'use strict';
// Short-term (RAM) patterns. A tag is on Disk unless a RAM pattern matches its name; the store is decided by the patterns of the RUN, not
// fixed when the tag was created:
//   - disk -> RAM: the disk data stays untouched, the RAM starts empty, a query answers from RAM only;
//   - RAM -> disk again: writes go on appending to the same disk data (it continues, in order) until a retention or a drop removes it;
//   - a RAM pattern is not a Disk/RAM choice per rule any more: { pattern, keep, max }; the disk retention is its own list `rules`;
//   - a `rules` entry of an older version with store: "memory" still works as a RAM pattern.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');
const { openHistorian } = require('../lib/client');

let passed = 0;
async function ok(label, fn) { await fn(); passed++; console.log('✔ ' + label); }
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-r-'));
const open = (d, o) => new Engine(d, Object.assign({ walSync: false, checkpointMs: 1e9, walFlushMs: 1e9 }, o || {})).open();
const raw = (e, tag, from) => { const r = Q.run(e, { tags: tag, from: from || '-30d', mode: 'raw' })[tag]; return r ? Array.from(r.v) : []; };
const NOW = Date.now(), MIN = 60000;
const stores = (e) => e.tagList().map((t) => t.name + ':' + t.store).join(',');

(async () => {
    await ok('disk -> RAM -> disk: the disk data stays, the RAM starts empty, the writes go on appending to the old data', () => {
        const d = tmp();
        let e = open(d);
        for (let i = 0; i < 10; i++) e.write('Press', NOW - 100 * MIN + i * MIN, i);                // run 1: on disk
        e.close();

        e = open(d, { ram: [{ pattern: 'Press', keep: '2h' }] });                                    // run 2: a RAM pattern now
        assert.strictEqual(stores(e), 'Press:memory', 'the tag is a RAM tag now');
        assert.deepStrictEqual(raw(e, 'Press'), [], 'RAM starts clean: the disk data is not read while it is a RAM tag');
        for (let i = 0; i < 5; i++) e.write('Press', NOW - 50 * MIN + i * MIN, 100 + i);
        assert.deepStrictEqual(raw(e, 'Press'), [100, 101, 102, 103, 104], 'a query reads the RAM only');
        e.close();
        const log = fs.readFileSync(path.join(d, 'tags.log'), 'utf8').trim().split('\n');
        assert.strictEqual(log.length, 1, 'the tag is still one line in tags.log (not two tags)');

        e = open(d);                                                                                  // run 3: no RAM pattern: back on disk
        assert.strictEqual(stores(e), 'Press:disk');
        assert.deepStrictEqual(raw(e, 'Press'), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 'the disk data of run 1 is there, the RAM of run 2 is gone');
        for (let i = 0; i < 3; i++) e.write('Press', NOW - 10 * MIN + i * MIN, 200 + i);
        assert.deepStrictEqual(raw(e, 'Press'), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 200, 201, 202], 'it continues the same series, in order');
        e.close();
        e = open(d);
        assert.deepStrictEqual(raw(e, 'Press'), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 200, 201, 202], 'and it is stored');
        e.close();
    });

    await ok('a RAM pattern is not a choice per rule: a tag that matches is in RAM at once, one that does not is on disk', () => {
        const d = tmp();
        const e = open(d, { ram: [{ pattern: 'Vib.*', keep: '1h', max: 100 }] });
        const t1 = NOW - 30000;
        for (let i = 0; i < 300; i++) { e.write('Vib.X', t1 + i * 100, i); e.write('Plain', t1 + i * 100, i); }
        assert.strictEqual(stores(e), 'Vib.X:memory,Plain:disk');
        assert.strictEqual(raw(e, 'Vib.X', '-1h').length, 100, 'capped at max');
        e.close();
        assert.ok(!/Vib/.test(fs.readFileSync(path.join(d, 'tags.log'), 'utf8')), 'a tag that never was on disk is not in tags.log');
    });

    await ok('a disk tag that moves to RAM answers the queries from RAM only (raw, last, bucket), and stats / tags say it is RAM', () => {
        const d = tmp();
        let e = open(d);
        for (let i = 0; i < 20; i++) e.write('A', NOW - 60 * MIN + i * MIN, i);
        e.close();
        e = open(d, { ram: [{ pattern: 'A', keep: '1h' }] });
        assert.deepStrictEqual(Q.run(e, { tags: 'A', mode: 'last' }).A.v, [], 'last: nothing yet (the RAM is empty)');
        e.write('A', NOW - 5 * MIN, 7);
        e.write('A', NOW - 4 * MIN, 8);
        assert.deepStrictEqual(Array.from(Q.run(e, { tags: 'A', mode: 'last' }).A.v), [8]);
        const b = Q.run(e, { tags: 'A', from: '-2h', mode: 'bucket', bucket: '1h', agg: ['count'] }).A;
        assert.strictEqual(Array.from(b.count || b.v || []).reduce((a, x) => a + (x || 0), 0), 2, 'buckets count the RAM points only');
        assert.deepStrictEqual(e.tagList().map((t) => [t.name, t.store]), [['A', 'memory']]);
        assert.strictEqual(require('../lib/admin').run(e, { op: 'stats' }).memoryTags, 1);
        e.close();
    });

    await ok('the disk data of a tag that is in RAM is still the admin\'s: dropTag removes it (and its RAM), the tag is new afterwards', () => {
        const d = tmp();
        let e = open(d);
        for (let i = 0; i < 10; i++) e.write('Gone', NOW - 100 * MIN + i * MIN, i);
        e.close();
        e = open(d, { ram: [{ pattern: 'Gone', keep: '1h' }] });
        e.write('Gone', NOW - MIN, 99);
        const dry = require('../lib/admin').run(e, { op: 'dropTag', tags: 'Gone', dryRun: true });
        assert.strictEqual(dry.points, 11, 'the disk points and the RAM point: ' + dry.points);
        require('../lib/admin').run(e, { op: 'dropTag', tags: 'Gone' });
        assert.deepStrictEqual(e.tagList(), []);
        e.close();
        e = open(d);
        assert.deepStrictEqual(raw(e, 'Gone'), [], 'the disk data is gone for good');
        e.close();
    });

    await ok('deleteRange on a RAM tag removes from its RAM ring; the disk data behind it, outside the range, is untouched', () => {
        const d = tmp();
        let e = open(d);
        for (let i = 0; i < 10; i++) e.write('D', NOW - 100 * MIN + i * MIN, i);
        e.close();
        e = open(d, { ram: [{ pattern: 'D', keep: '1h' }] });
        for (let i = 0; i < 6; i++) e.write('D', NOW - 30 * MIN + i * MIN, 50 + i);
        const r = require('../lib/admin').run(e, { op: 'deleteRange', tags: 'D', from: NOW - 29 * MIN - 1, to: NOW - 27 * MIN + 1 });
        assert.strictEqual(r.points, 3, 'three points of the RAM ring: ' + r.points);
        assert.deepStrictEqual(raw(e, 'D'), [50, 54, 55]);
        e.close();
        e = open(d);
        assert.deepStrictEqual(raw(e, 'D'), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 'the disk data was outside the range');
        e.close();
    });

    await ok('the disk retention is its own list: a RAM keep (seconds) never cuts the disk data of the same tag', () => {
        const d = tmp();
        let e = open(d);
        for (let i = 0; i < 10; i++) e.write('R', NOW - 100 * MIN + i * MIN, i);
        e.close();
        e = open(d, { ram: [{ pattern: 'R', keep: '10s' }], rules: [{ pattern: 'R', keep: '30d' }] });
        e.retention();
        e.close();
        e = open(d, { rules: [{ pattern: 'R', keep: '30d' }] });
        assert.deepStrictEqual(raw(e, 'R'), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 'the 10 s of the RAM did not touch the disk data');
        e.close();
    });

    await ok('a disk rule applies to the disk data after a run in RAM (keep follows the rules at every start)', () => {
        const d = tmp();
        let e = open(d);
        for (let i = 0; i < 5; i++) e.write('K', NOW - 3 * 86400000 + i * MIN, i);
        e.close();
        e = open(d, { ram: [{ pattern: 'K', keep: '1m' }] });
        e.close();
        e = open(d, { rules: [{ pattern: 'K', keep: '1d' }] });
        assert.deepStrictEqual(raw(e, 'K'), [], 'older than the new keep of 1 day: not returned');
        e.close();
    });

    await ok('an older flow (rules: [{ store: "memory" }]) works as before: it is a RAM pattern', () => {
        const d = tmp();
        const e = open(d, { rules: [{ pattern: 'Vib.*', store: 'memory', keep: '10s' }, { pattern: 'Debug.*', keep: '1h' }] });
        e.write('Vib.X', NOW - 1000, 1);
        e.write('Debug.Y', NOW - 1000, 2);
        assert.strictEqual(stores(e), 'Vib.X:memory,Debug.Y:disk');
        e.close();
    });

    await ok('warnings: a RAM keep under 100 ms and over 1 h, a disk keep under 1 h', () => {
        const d = tmp();
        const e = open(d, { ram: [{ pattern: 'a', keep: '10ms' }, { pattern: 'b', keep: '2h' }], rules: [{ pattern: 'c', keep: '10m' }] });
        const w = e.ruleWarnings.join('\n');
        assert.ok(/a: RAM keep 10ms is under 100 ms/.test(w) && /b: RAM for 2h/.test(w) && /c: Disk keep 10m is under 1 h/.test(w), w);
        e.close();
    });

    await ok('through the real historian (worker): disk -> RAM -> disk keeps the disk data, and the points go on in order', async () => {
        const d = tmp();
        const O = { walFlushMs: 50, checkpointMs: 1e9, rawDays: 36500, indexDays: 36500 };
        let h = openHistorian(d, O);
        await h.ready;
        for (let i = 0; i < 10; i++) h.write('Line', NOW - 100 * MIN + i * MIN, i);
        await h.close();

        h = openHistorian(d, Object.assign({ ram: [{ pattern: 'Line', keep: '2h' }] }, O));
        await h.ready;
        for (let i = 0; i < 3; i++) h.write('Line', NOW - 20 * MIN + i * MIN, 100 + i);
        const mid = await h.query({ tags: 'Line', from: '-30d', mode: 'raw' });
        assert.deepStrictEqual(Array.from(mid.Line.v), [100, 101, 102], 'RAM only');
        await h.close();

        h = openHistorian(d, O);
        await h.ready;
        h.write('Line', NOW - 5 * MIN, 300);
        const end = await h.query({ tags: 'Line', from: '-30d', mode: 'raw' });
        assert.deepStrictEqual(Array.from(end.Line.v), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 300], 'the old disk data, then the new point');
        await h.close();
    });

    console.log('\n' + passed + ' ramswitch tests passed');
})().catch((e) => { console.error(e); process.exit(1); });
