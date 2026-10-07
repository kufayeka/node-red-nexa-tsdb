'use strict';
// Node-RED nodes of the historian core:
//   tsdb-config   a database (a folder): opened on deploy, closed (checkpointed) on redeploy / stop
//   tsdb-store    msg -> points:  topic + payload (+ timestamp), or an array of { tag, ts, value }, or { tag: value }
//   tsdb-query    msg.query (merged over the node's own query) -> msg.payload
// The core is pure: ts, tag, value (number, bool, string). A nested object is not split here (the asset layer does that).
// The engine runs in a worker thread (lib/worker.js): writes are batched to it, queries are answered by it.
const path = require('path');
const { openHistorian } = require('../lib/client');

module.exports = function (RED) {
    function toMs(x) {
        if (x === undefined || x === null || x === '') return Date.now();
        if (typeof x === 'number') return x;
        if (x instanceof Date) return x.getTime();
        const n = Number(x);
        return Number.isFinite(n) ? n : Date.parse(x);
    }
    const num = (v, d) => (v === '' || v === undefined || v === null || !Number.isFinite(Number(v)) ? d : Number(v));

    // ---- the database ------------------------------------------------------------------------------------
    function TsdbConfig(n) {
        RED.nodes.createNode(this, n);
        const node = this;
        node.name = n.name || 'historian';
        const base = (RED.settings && RED.settings.userDir) || process.cwd();
        node.dir = n.dir ? path.resolve(base, n.dir) : path.join(base, 'tsdb', node.name.replace(/[^\w.-]+/g, '_'));
        let rules = n.rules;
        if (typeof rules === 'string') { try { rules = JSON.parse(rules || '[]'); } catch (e) { node.error('storage rules: not JSON (' + e.message + ')'); rules = []; } }
        node.engine = openHistorian(node.dir, {
            rawDays: num(n.rawDays, 30), indexDays: num(n.indexDays, 365),
            walFlushMs: num(n.walFlushMs, 1000), checkpointMs: num(n.checkpointMs, 60000),
            rules: Array.isArray(rules) ? rules.filter((r) => r && r.pattern) : []
        });
        node.engine.onError = (e) => node.error('historian ' + node.dir + ': ' + e.message);
        node.engine.ready.then((m) => {
            node.log('historian open: ' + node.dir + ' (' + m.tags + ' tags' + (m.recovered ? ', ' + m.recovered + ' points recovered' : '') + ')');
            (m.warnings || []).forEach((w) => node.warn('storage rule ' + w));
        }, () => {});
        node.on('close', function (done) {
            // the batch, a checkpoint, the worker ends: a redeploy loses nothing
            node.engine.close().then(() => done(), (e) => { node.error('close: ' + e.message); done(); });
        });
    }
    RED.nodes.registerType('tsdb-config', TsdbConfig);

    // ---- store -------------------------------------------------------------------------------------------
    function TsdbStore(n) {
        RED.nodes.createNode(this, n);
        const node = this, db = RED.nodes.getNode(n.db);
        const prefix = n.prefix || '', deadband = num(n.deadband, 0), changesOnly = !!n.changesOnly;
        const last = new Map();
        let nested = 0, rejected = 0;
        const put = (tag, ts, value) => {
            if (value !== null && typeof value === 'object') { nested++; return; }
            if (typeof value !== 'number' && typeof value !== 'boolean' && typeof value !== 'string') return;
            const name = prefix + tag;
            // report by exception: a value is stored when it changes (past the deadband, for a number)
            let prevOf;
            if (changesOnly || deadband > 0) {
                const p = last.get(name);
                prevOf = p;
                if (p !== undefined && (typeof value === 'number' && typeof p === 'number' ? Math.abs(value - p) <= deadband : value === p)) return;
                last.set(name, value);
            }
            // the point is remembered as the last stored one only when the historian took it (overload / down: it is tried again)
            if (!db.engine.write(name, toMs(ts), value)) { rejected++; if (changesOnly || deadband > 0) { if (prevOf === undefined) last.delete(name); else last.set(name, prevOf); } }
        };
        // the database's counts (the worker reports them every second); late / wrong-type points are refused there
        const timer = setInterval(() => {
            const st = db && db.engine ? db.engine.stats : null;
            if (!st || (!st.points && !nested)) return;
            const refused = (st.late || 0) + (st.badType || 0) + (st.overload || 0);
            if (st.down) { node.status({ fill: 'red', shape: 'ring', text: 'restarting after an I/O error: ' + String(st.down).slice(0, 60) }); return; }
            const lr = st.lastRefused;
            node.status({ fill: refused ? 'yellow' : 'green', shape: 'dot', text: st.points + ' stored' + (st.overwritten ? ', ' + st.overwritten + ' replaced' : '') + (refused ? ', ' + refused + ' refused - last: ' + lr.tag + ' ' + lr.reason : '') + (nested ? ', ' + nested + ' nested skipped' : '') + (st.restarts ? ', restarted ' + st.restarts + 'x after I/O errors' : '') + (st.corruptChunks ? ', ' + st.corruptChunks + ' DAMAGED chunk(s): run verify' : '') });
        }, 2000);
        node.on('input', function (msg, send, done) {
            if (!db || !db.engine) { done(new Error('no historian (check the database node)')); return; }
            try {
                const p = msg.payload, before = rejected, t0 = Date.now();
                if (Array.isArray(p)) p.forEach((r) => { if (r && typeof r === 'object') put(r.tag !== undefined ? r.tag : r.topic, r.ts !== undefined ? r.ts : r.timestamp !== undefined ? r.timestamp : msg.timestamp, r.value); });
                else if (p !== null && typeof p === 'object' && !msg.topic) Object.keys(p).forEach((k) => put(k, msg.timestamp, p[k]));
                else if (msg.topic) put(msg.topic, msg.timestamp, p);
                else { done(new Error('a point needs msg.topic (the tag), or msg.payload as [{ tag, ts, value }] or { tag: value }')); return; }
                // overload / historian down: said on the message, not swallowed (the points are not stored)
                const lo = db.engine.lastOverload;
                if (rejected > before && lo && lo.at >= t0) { done(new Error((rejected - before) + ' point(s) not stored - ' + lo.reason)); return; }
                done();
            } catch (e) { done(e); }
        });
        node.on('close', () => clearInterval(timer));
    }
    RED.nodes.registerType('tsdb-store', TsdbStore);

    // ---- query -------------------------------------------------------------------------------------------
    function TsdbQuery(n) {
        RED.nodes.createNode(this, n);
        const node = this, db = RED.nodes.getNode(n.db);
        const list = (s) => String(s || '').split(',').map((x) => x.trim()).filter(Boolean);
        const own = { tags: list(n.tags), from: n.from || '-1h', to: n.to || 'now', mode: n.mode || 'm4', width: num(n.width, 1000),
            bucket: n.bucket || '1h', offset: n.offset || undefined, agg: list(n.agg).length ? list(n.agg) : ['avg'], fill: n.fill || 'none', format: n.format || 'series' };
        node.on('input', function (msg, send, done) {
            if (!db || !db.engine) { done(new Error('no historian (check the database node)')); return; }
            try {
                const q = Object.assign({}, own, msg.query && typeof msg.query === 'object' ? msg.query : {});
                if (!q.tags || (Array.isArray(q.tags) && !q.tags.length)) q.tags = msg.topic ? [msg.topic] : [];
                const t0 = Date.now();
                db.engine.query(q).then((r) => {
                    msg.payload = r;
                    msg.query = q;
                    node.status({ fill: 'green', shape: 'dot', text: Object.keys(r).length + ' tags, ' + (Date.now() - t0) + ' ms' });
                    send(msg);
                    done();
                }, (e) => { node.status({ fill: 'red', shape: 'ring', text: e.message }); done(e); });
            } catch (e) { node.status({ fill: 'red', shape: 'ring', text: e.message }); done(e); }
        });
    }
    RED.nodes.registerType('tsdb-query', TsdbQuery);

    // ---- admin: drop tags, delete a range, drop all, compact, list, stats ------------------------------------
    function TsdbAdmin(n) {
        RED.nodes.createNode(this, n);
        const node = this, db = RED.nodes.getNode(n.db);
        node.on('input', function (msg, send, done) {
            if (!db || !db.engine) { done(new Error('no historian (check the database node)')); return; }
            const req = msg.payload && typeof msg.payload === 'object' && !Array.isArray(msg.payload) ? msg.payload : { op: n.op || 'stats' };
            if (!req.op) req.op = n.op || 'stats';
            db.engine.admin(req).then((r) => {
                msg.payload = r;
                const what = Array.isArray(r) ? r.length + ' tags' : r.dryRun ? 'dry run: ' + (r.tags ? r.tags.length : 0) + ' tags, ' + (r.points || 0) + ' points'
                    : r.op === 'deleteRange' || r.op === 'dropTag' ? (r.tags.length + ' tags, ' + r.points + ' points deleted') : r.op;
                node.status({ fill: r.dryRun ? 'blue' : 'green', shape: 'dot', text: what });
                send(msg);
                done();
            }, (e) => { node.status({ fill: 'red', shape: 'ring', text: e.message.slice(0, 60) }); done(e); });
        });
    }
    RED.nodes.registerType('tsdb-admin', TsdbAdmin);
};
