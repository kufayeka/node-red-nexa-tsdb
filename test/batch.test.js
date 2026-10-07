'use strict';
// Many queries in one call: the answers in order, each equal to the query run alone, a failing one does not stop the others, the size
// of a batch and of its answers is held, through the worker and the node.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');
const { openHistorian } = require('../lib/client');

let passed = 0;
async function ok(label, fn) { await fn(); passed++; console.log('✔ ' + label); }
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-bt-'));
const MIN = 60000, HOUR = 3600000, T0 = Date.UTC(2026, 0, 1);

(async () => {
    const d = tmp(), e = new Engine(d, { walSync: false, checkpointMs: 1e9, walFlushMs: 1e9, rawDays: 36500 }).open();
    for (let k = 0; k < 600; k++) { e.write('A', T0 + k * MIN, k); e.write('B', T0 + k * MIN, k % 7); e.write('S', T0 + k * MIN, k % 3 ? 'x' : 'y'); }
    const qs = [
        { tags: 'A', from: T0, to: T0 + 600 * MIN, mode: 'bucket', bucket: '1h', agg: ['avg', 'max', 'increase'] },
        { tags: 'B', from: T0, to: T0 + 600 * MIN, mode: 'range', agg: ['avg'] },
        { tags: 'S', from: T0, to: T0 + 600 * MIN, mode: 'range', agg: ['counts'] },
        { tags: 'A', mode: 'last', to: T0 + 700 * MIN },
        { tags: '*', from: T0, to: T0 + HOUR, mode: 'raw' }
    ];
    await ok('a batch answers in order, each equal to the query run alone', () => {
        const r = Q.runBatch(e, qs, T0 + 700 * MIN);
        assert.strictEqual(r.length, qs.length);
        qs.forEach((q, i) => { assert.strictEqual(r[i].ok, true); assert.deepStrictEqual(r[i].result, Q.run(e, q, T0 + 700 * MIN), 'query ' + i); });
    });
    await ok('a query that fails does not stop the others; its error is in its place', () => {
        const r = Q.runBatch(e, [qs[0], { tags: 'A', mode: 'nope' }, { tags: 'A', agg: ['bogus'], mode: 'bucket' }, qs[1]], T0 + 700 * MIN);
        assert.deepStrictEqual(r.map((x) => x.ok), [true, false, false, true]);
        assert.match(r[1].error, /unknown mode/); assert.match(r[2].error, /unknown aggregate/);
    });
    await ok('relative times use one `now` for the whole batch', () => {
        const r = Q.runBatch(e, [{ tags: 'A', from: '-2h', mode: 'range', agg: ['count'] }, { tags: 'A', from: '-2h', mode: 'range', agg: ['count'] }], T0 + 300 * MIN);
        assert.deepStrictEqual(r[0], r[1]);
        assert.strictEqual(r[0].result.A.count[0], 121);
    });
    await ok('a batch is at most 1000 queries and not an object; the answers of a batch are held to maxPoints', () => {
        assert.throws(() => Q.runBatch(e, new Array(1001).fill(qs[3])), /at most 1000/);
        assert.throws(() => Q.runBatch(e, { tags: 'A' }), /array/);
        const big = { tags: 'A', from: T0, to: T0 + 600 * MIN, mode: 'raw' };
        const r = Q.runBatch(e, [big, big, big], T0 + 700 * MIN, 1000);                       // 600 points each: the third is past 1 000
        assert.deepStrictEqual(r.map((x) => x.ok), [true, true, false]);
        assert.match(r[2].error, /past 1,000 points/);
        assert.deepStrictEqual(Q.runBatch(e, []), []);
    });
    e.close();

    await ok('through the worker: queryBatch, and the node (msg.query as an array)', async () => {
        const dir = tmp(), db = openHistorian(dir, { rawDays: 36500 }); await db.ready;
        for (let k = 0; k < 120; k++) db.write('W', T0 + k * MIN, k);
        const r = await db.queryBatch([{ tags: 'W', from: T0, to: T0 + 119 * MIN, mode: 'range', agg: ['count', 'last'] }, { tags: 'W', mode: 'nope' }]);
        assert.deepStrictEqual([r[0].ok, r[0].result.W.count[0], r[0].result.W.last[0], r[1].ok], [true, 120, 119, false]);
        await db.close();
        const types = {}, nodes = {};
        const RED = { settings: { userDir: tmp() }, nodes: { registerType: (n, f) => { types[n] = f; }, createNode: (node, n) => { node._h = {}; node.on = (ev, f) => { node._h[ev] = f; }; node.status = (s) => { node.lastStatus = s; }; node.error = () => {}; node.log = () => {}; node.warn = () => {}; nodes[n.id] = node; }, getNode: (id) => nodes[id] } };
        require('../nodes/tsdb')(RED);
        const cfg = {}; types['tsdb-config'].call(cfg, { id: 'c', name: 'b' }); await cfg.engine.ready;
        const now = Date.now() - 2 * HOUR;                              // recent: the node's database keeps raw points for 30 days
        for (let k = 0; k < 60; k++) cfg.engine.write('N', now + k * MIN, k);
        const qn = {}; types['tsdb-query'].call(qn, { id: 'q', db: 'c', mode: 'range', agg: 'count', from: String(now), to: String(now + 59 * MIN) });
        const msg = { query: [{ tags: 'N' }, { tags: 'N', agg: ['last'] }, { tags: 'N', mode: 'bogus' }] };
        const sent = await new Promise((res) => qn._h.input(msg, (m) => res(m), (err) => { if (err) res(err); }));
        assert.deepStrictEqual(sent.payload.map((x) => x.ok), [true, true, false]);
        assert.strictEqual(sent.payload[0].result.N.count[0], 60); assert.strictEqual(sent.payload[1].result.N.last[0], 59);
        assert.match(qn.lastStatus.text, /3 queries, 1 failed/);
        await new Promise((res) => cfg._h.close(res));
    });
    console.log('\n' + passed + ' passed\nALL OK');
})().catch((e) => { console.error(e); process.exit(1); });
