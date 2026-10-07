'use strict';
// Node-RED nodes of the historian core:
//   tsdb-config   a database (a folder): opened on deploy, closed (checkpointed) on redeploy / stop
//   tsdb-store    msg -> points:  topic + payload (+ timestamp), or an array of { tag, ts, value }, or { tag: value }
//   tsdb-query    msg.query (merged over the node's own query) -> msg.payload
// The core is pure: ts, tag, value (number, bool, string). A nested object is not split here (the asset layer does that).
const path = require('path');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');

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
        try {
            node.engine = new Engine(node.dir, {
                rawDays: num(n.rawDays, 30), indexDays: num(n.indexDays, 365),
                walFlushMs: num(n.walFlushMs, 1000), checkpointMs: num(n.checkpointMs, 60000)
            }).open();
            node.retentionTimer = setInterval(() => { try { node.engine.retention(); } catch (e) { node.error('retention: ' + e.message); } }, 3600000);
            node.log('historian open: ' + node.dir + ' (' + node.engine.tags.filter(Boolean).length + ' tags)');
        } catch (e) {
            node.engine = null;
            node.error('cannot open the historian at ' + node.dir + ': ' + e.message);
        }
        node.on('close', function (done) {
            clearInterval(node.retentionTimer);
            try { if (node.engine) node.engine.close(); } catch (e) { node.error('close: ' + e.message); }
            done();
        });
    }
    RED.nodes.registerType('tsdb-config', TsdbConfig);

    // ---- store -------------------------------------------------------------------------------------------
    function TsdbStore(n) {
        RED.nodes.createNode(this, n);
        const node = this, db = RED.nodes.getNode(n.db);
        const prefix = n.prefix || '', deadband = num(n.deadband, 0), changesOnly = !!n.changesOnly;
        const last = new Map();
        let count = 0, refused = 0, nested = 0;
        const put = (tag, ts, value) => {
            if (value !== null && typeof value === 'object') { nested++; return; }
            if (typeof value !== 'number' && typeof value !== 'boolean' && typeof value !== 'string') return;
            const name = prefix + tag;
            // report by exception: a value is stored when it changes (past the deadband, for a number)
            if (changesOnly || deadband > 0) {
                const p = last.get(name);
                if (p !== undefined && (typeof value === 'number' && typeof p === 'number' ? Math.abs(value - p) <= deadband : value === p)) return;
                last.set(name, value);
            }
            if (db.engine.write(name, toMs(ts), value)) count++; else refused++;
        };
        const timer = setInterval(() => {
            node.status(count || refused || nested ? { fill: refused ? 'yellow' : 'green', shape: 'dot', text: count + ' stored' + (refused ? ', ' + refused + ' refused (late / type)' : '') + (nested ? ', ' + nested + ' nested skipped' : '') } : {});
        }, 2000);
        node.on('input', function (msg, send, done) {
            if (!db || !db.engine) { done(new Error('no historian (check the database node)')); return; }
            try {
                const p = msg.payload;
                if (Array.isArray(p)) p.forEach((r) => { if (r && typeof r === 'object') put(r.tag !== undefined ? r.tag : r.topic, r.ts !== undefined ? r.ts : r.timestamp !== undefined ? r.timestamp : msg.timestamp, r.value); });
                else if (p !== null && typeof p === 'object' && !msg.topic) Object.keys(p).forEach((k) => put(k, msg.timestamp, p[k]));
                else if (msg.topic) put(msg.topic, msg.timestamp, p);
                else { done(new Error('a point needs msg.topic (the tag), or msg.payload as [{ tag, ts, value }] or { tag: value }')); return; }
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
                msg.payload = Q.run(db.engine, q);
                msg.query = q;
                node.status({ fill: 'green', shape: 'dot', text: Object.keys(msg.payload).length + ' tags, ' + (Date.now() - t0) + ' ms' });
                send(msg);
                done();
            } catch (e) { node.status({ fill: 'red', shape: 'ring', text: e.message }); done(e); }
        });
    }
    RED.nodes.registerType('tsdb-query', TsdbQuery);
};
