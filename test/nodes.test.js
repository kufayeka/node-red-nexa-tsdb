'use strict';
// The three nodes on a stand-in RED: the config node opens the folder, store takes the three message shapes, query
// answers msg.query merged over its own settings, a redeploy (close) checkpoints and a new config node reopens it.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

let passed = 0;
async function ok(label, fn) { await fn(); passed++; console.log('✔ ' + label); }

const userDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-nodes-'));
const types = {}, nodes = {};
const RED = {
    settings: { userDir },
    nodes: {
        registerType: (name, fn) => { types[name] = fn; },
        createNode: (node, n) => {
            node.id = n.id; node._h = {}; node.sent = []; node.errors = []; node.lastStatus = null;
            node.on = (ev, f) => { node._h[ev] = f; };
            node.status = (s) => { node.lastStatus = s; };
            node.error = (e) => node.errors.push(e); node.log = () => {};
            nodes[n.id] = node;
        },
        getNode: (id) => nodes[id]
    }
};
require('../nodes/tsdb')(RED);
const make = (type, n) => { const o = {}; types[type].call(o, Object.assign({ id: type + Math.random() }, n)); return o; };
const input = (node, msg) => new Promise((res) => node._h.input(msg, (m) => node.sent.push(m), (err) => res(err)));
const close = (node) => new Promise((res) => (node._h.close.length ? node._h.close(res) : (node._h.close(), res())));

(async () => {
    const T = Date.now() - 3600000;
    let db = make('tsdb-config', { id: 'db1', name: 'plant' });
    await ok('the config node opens <userDir>/tsdb/<name> (the engine in its worker)', async () => {
        assert.ok(db.engine);
        assert.strictEqual(db.dir, path.join(userDir, 'tsdb', 'plant'));
        await db.engine.ready;
    });
    const store = make('tsdb-store', { db: 'db1', prefix: 'L1.', changesOnly: true });
    await ok('store: topic + payload, an array of points, an object of tags; changes only; a nested object skipped', async () => {
        assert.strictEqual(await input(store, { topic: 'Speed', payload: 120, timestamp: T }), undefined);
        await input(store, { topic: 'Speed', payload: 120, timestamp: T + 1000 });          // the same value: not stored
        await input(store, { payload: [{ tag: 'Speed', ts: T + 2000, value: 125 }, { tag: 'Mode', ts: T + 2000, value: 'Auto' }] });
        await input(store, { payload: { Running: true, Info: { a: 1 } }, timestamp: T + 3000 });
        const err = await input(store, { payload: 5 });
        assert.ok(err && /topic/.test(err.message), 'no tag: an error');
        const tags = (await db.engine.tags()).map((t) => t.name).sort();
        assert.deepStrictEqual(tags, ['L1.Mode', 'L1.Running', 'L1.Speed']);
    });
    const query = make('tsdb-query', { db: 'db1', tags: 'L1.Speed', from: '-2h', mode: 'raw' });
    await ok('query: its own settings, msg.query over them, rows', async () => {
        await input(query, {});
        assert.deepStrictEqual(query.sent[0].payload['L1.Speed'].v, [120, 125]);
        await input(query, { query: { tags: ['L1.*'], mode: 'last', format: 'rows' } });
        const rows = query.sent[1].payload.map((r) => [r.tag, r.value]).sort();
        assert.deepStrictEqual(rows, [['L1.Mode', 'Auto'], ['L1.Running', true], ['L1.Speed', 125]]);
        const bad = await input(query, { query: { mode: 'nope' } });
        assert.ok(bad && /unknown mode/.test(bad.message));
    });
    await ok('a redeploy: close checkpoints; a new config node on the folder has every point', async () => {
        await close(db);
        db = make('tsdb-config', { id: 'db1', name: 'plant' });
        const q2 = make('tsdb-query', { db: 'db1', tags: 'L1.Speed', from: '-2h', mode: 'raw' });
        await input(q2, {});
        assert.deepStrictEqual(q2.sent[0].payload['L1.Speed'].v, [120, 125]);
        await close(db);
    });
    await ok('admin: a dry run, a drop, a broad pattern refused; storage rules from the config (a memory tag)', async () => {
        db = make('tsdb-config', { id: 'db1', name: 'plant' });
        await db.engine.ready;
        const adm = make('tsdb-admin', { db: 'db1', op: 'stats' });
        await input(adm, {});
        assert.strictEqual(adm.sent[0].payload.op, 'stats');
        await input(adm, { payload: { op: 'dropTag', tags: 'L1.M*', dryRun: true } });
        assert.deepStrictEqual(adm.sent[1].payload.tags, ['L1.Mode']);
        await input(adm, { payload: { op: 'dropTag', tags: 'L1.Mode' } });
        assert.ok(!(await db.engine.tags()).some((t) => t.name === 'L1.Mode'));
        const err = await input(adm, { payload: { op: 'dropAll' } });
        assert.ok(err && /DROP ALL/.test(err.message));
        const mdb = make('tsdb-config', { id: 'db2', name: 'mem', rules: '[{"pattern":"Vib.*","store":"memory","keep":"10s"}]' });
        await mdb.engine.ready;
        mdb.engine.write('Vib.X', Date.now(), 1);
        assert.deepStrictEqual((await mdb.engine.tags()).map((t) => t.store), ['memory']);
        await close(mdb);
        await close(db);
    });
    await close(store);
    fs.rmSync(userDir, { recursive: true, force: true });
    console.log(`\n${passed} passed\nALL OK`);
})().catch((e) => { console.error(e); process.exit(1); });
