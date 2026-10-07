'use strict';
// The engine in its own thread: compression, the WAL, checkpoints, recovery and queries never block Node-RED.
// Messages in:  { op: 'batch', defs: [[localId, name]], ids: Uint32Array, ts: Float64Array, vals: Float64Array,
//                 kinds: Uint8Array (0 number, 1 bool, 2 string), strs: [string per kind-2 point, in order], n }
//               { op: 'query', id, q, now }   { op: 'tags', id }   { op: 'checkpoint', id }   { op: 'admin', id, req }   { op: 'close', id }
// Messages out: { op: 'ready', tags } | { op: 'error', error } | { op: 'reply', id, result | error } | { op: 'stats', stats } | { op: 'ack', n }
//
// An I/O error while writing (disk full, a failed fsync ...) leaves an engine whose memory may be half way through an
// operation. The worker does what a crash does, which is the path that is tested hardest: it stops the engine without
// writing anything (abort), opens it again (recovery from the WAL), and carries on. Batches that arrive meanwhile wait
// (the client's backpressure refuses writes past maxInFlight), queries are answered with the reason. If opening fails
// (still no space) it retries with a growing delay and says so on every try.
const { parentPort, workerData } = require('worker_threads');
const { Engine } = require('./engine');
const Q = require('./query');
const admin = require('./admin');

const names = [];                 // the main thread's local id -> tag name
const queue = [];                 // batches that came while the engine was restarting (and the one it stopped in)
const own = { restarts: 0, lastError: null, down: null };
let engine = null, restarting = false, lastStats = {};

const post = (m) => parentPort.postMessage(m);
// a Node error code of the system (ENOSPC, EIO, EMFILE ...): not the historian's own refusals (ETSDB_CORRUPT, a bad request)
const isIO = (e) => !!e && typeof e.code === 'string' && /^E[A-Z0-9]+$/.test(e.code) && e.code !== 'ETSDB_CORRUPT';

function start() {
    const e = new Engine(workerData.dir, workerData.opts);
    e.onError = (err) => restart(err);             // an error in the engine's own timers (the WAL sync, the checkpoint)
    return e.open();
}

try {
    engine = start();
    post({ op: 'ready', tags: engine.tags.filter(Boolean).length, recovered: engine.stats.recovered, warnings: engine.ruleWarnings });
} catch (e) {
    post({ op: 'error', error: 'open: ' + e.message });
    process.exit(1);
}

function restart(err) {
    if (restarting) return;
    restarting = true;
    own.restarts++; own.lastError = { at: Date.now(), error: err.message }; own.down = err.message;
    post({ op: 'error', error: 'I/O error, restarting the historian (recovery from the WAL): ' + err.message });
    // what is written but not in the WAL yet (the buffer of the last 1 s) is taken out before the engine stops, and played
    // into the restarted one: an error we know about loses nothing (only a crash of the process loses that buffer)
    let pend = [];
    if (engine) { lastStats = Object.assign({ tags: engine.tags.length }, engine.stats); try { pend = engine.pending(); } catch (e) { /* nothing to save */ } try { engine.abort(); } catch (e) { /* closed already */ } }
    engine = null;
    let delay = 300;
    const attempt = () => {
        try {
            engine = start();
            restarting = false; own.down = null;
            if (pend.length) queue.unshift({ op: 'replay', points: pend });
            post({ op: 'recovered', tags: engine.tags.filter(Boolean).length, recovered: engine.stats.recovered, replayed: pend.length });
            while (engine && queue.length) handle(queue.shift());
        } catch (e) {
            engine = null;
            post({ op: 'error', error: 'the historian could not restart (' + e.message + '); trying again in ' + Math.round(delay / 1000 * 10) / 10 + ' s' });
            delay = Math.min(30000, delay * 2);
            setTimeout(attempt, delay);
        }
    };
    setTimeout(attempt, delay);
}

const snapshot = () => Object.assign({}, engine ? Object.assign({ tags: engine.tags.length }, engine.stats) : lastStats, { restarts: own.restarts, lastError: own.lastError, down: own.down });
const statsTimer = setInterval(() => post({ op: 'stats', stats: snapshot() }), workerData.statsMs || 1000);
const retentionTimer = setInterval(() => { if (engine) { try { engine.retention(); } catch (e) { if (isIO(e)) restart(e); else post({ op: 'error', error: 'retention: ' + e.message }); } } }, 3600000);

function handle(m) {
    try {
        if (m.op === 'replay') {
            for (let i = m.from || 0; i < m.points.length; i++) {
                const p = m.points[i];
                try { engine.replayPoint(p.name, p.ts, p.value); } catch (err) { m.from = i; queue.unshift(m); restart(err); return; }
            }
            return;
        }
        if (m.op === 'batch') {
            if (m.defs) { for (const [id, name] of m.defs) names[id] = name; m.defs = null; }
            let s = m.sFrom || 0;
            for (let i = m.from || 0; i < m.n; i++) {
                const k = m.kinds[i];
                const v = k === 0 ? m.vals[i] : k === 1 ? m.vals[i] !== 0 : m.strs[s++];
                try { engine.write(names[m.ids[i]], m.ts[i], v); }
                catch (err) {
                    // from this point on: the batch waits for the restarted engine (this point again: a same-time point replaces, an older one is counted)
                    m.from = i; m.sFrom = k === 2 ? s - 1 : s;
                    queue.unshift(m);
                    restart(err);
                    return;
                }
            }
            post({ op: 'ack', n: m.n });   // always: the main thread's backpressure must never stall
            return;
        }
        let result;
        if (m.op === 'query') result = Q.run(engine, m.q, m.now);
        else if (m.op === 'tags') result = engine.tagList();
        else if (m.op === 'checkpoint') { engine.checkpoint(); result = true; }
        else if (m.op === 'retention') { engine.retention(m.now); result = true; }
        else if (m.op === 'admin') result = admin.run(engine, m.req);
        else if (m.op === 'close') {
            clearInterval(statsTimer); clearInterval(retentionTimer);
            engine.close();
            post({ op: 'stats', stats: snapshot() });
            post({ op: 'reply', id: m.id, result: true });
            parentPort.close();
            return;
        }
        post({ op: 'reply', id: m.id, result });
    } catch (e) {
        if (m.id !== undefined) post({ op: 'reply', id: m.id, error: e.message });
        // a failed write operation (a checkpoint, a delete, a compact) may have stopped half way: restart, as for a write
        if (isIO(e) && ['checkpoint', 'admin', 'retention', 'close'].includes(m.op)) restart(e);
    }
}

parentPort.on('message', (m) => {
    if (engine) { handle(m); return; }
    if (m.op === 'batch') { queue.push(m); return; }                       // waits for the restarted engine
    if (m.op === 'close') { clearInterval(statsTimer); clearInterval(retentionTimer); post({ op: 'reply', id: m.id, result: true }); parentPort.close(); return; }
    if (m.id !== undefined) post({ op: 'reply', id: m.id, error: 'the historian is restarting after an I/O error (' + own.down + '): try again in a moment' });
});
