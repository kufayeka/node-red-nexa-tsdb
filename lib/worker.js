'use strict';
// The engine in its own thread: compression, the WAL, checkpoints, recovery and queries never block Node-RED.
// Messages in:  { op: 'batch', defs: [[localId, name]], ids: Uint32Array, ts: Float64Array, vals: Float64Array,
//                 kinds: Uint8Array (0 number, 1 bool, 2 string), strs: [string per kind-2 point, in order], n }
//               { op: 'query', id, q, now }   { op: 'tags', id }   { op: 'checkpoint', id }   { op: 'close', id }
// Messages out: { op: 'ready', tags } | { op: 'error', error } | { op: 'reply', id, result | error } | { op: 'stats', stats }
const { parentPort, workerData } = require('worker_threads');
const { Engine } = require('./engine');
const Q = require('./query');
const admin = require('./admin');

let engine;
try {
    engine = new Engine(workerData.dir, workerData.opts).open();
    parentPort.postMessage({ op: 'ready', tags: engine.tags.filter(Boolean).length, recovered: engine.stats.recovered, warnings: engine.ruleWarnings });
} catch (e) {
    parentPort.postMessage({ op: 'error', error: 'open: ' + e.message });
    process.exit(1);
}

const names = [];   // the main thread's local id -> tag name
const statsTimer = setInterval(() => parentPort.postMessage({ op: 'stats', stats: Object.assign({ tags: engine.tags.length }, engine.stats) }), workerData.statsMs || 1000);
const retentionTimer = setInterval(() => { try { engine.retention(); } catch (e) { parentPort.postMessage({ op: 'error', error: 'retention: ' + e.message }); } }, 3600000);

parentPort.on('message', (m) => {
    try {
        if (m.op === 'batch') {
            for (const [id, name] of m.defs) names[id] = name;
            let s = 0;
            for (let i = 0; i < m.n; i++) {
                const k = m.kinds[i];
                engine.write(names[m.ids[i]], m.ts[i], k === 0 ? m.vals[i] : k === 1 ? m.vals[i] !== 0 : m.strs[s++]);
            }
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
            parentPort.postMessage({ op: 'stats', stats: Object.assign({ tags: engine.tags.length }, engine.stats) });
            parentPort.postMessage({ op: 'reply', id: m.id, result: true });
            parentPort.close();
            return;
        }
        parentPort.postMessage({ op: 'reply', id: m.id, result });
    } catch (e) {
        parentPort.postMessage({ op: 'reply', id: m.id, error: e.message });
    }
});
