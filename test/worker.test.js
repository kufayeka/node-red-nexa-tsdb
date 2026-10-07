'use strict';
// The engine in a worker: points written from the main thread come back exact; the main thread's event loop stays free
// while the worker answers a heavy query (run the same query in the main thread to see the difference); a worker killed
// hard loses nothing that was in the WAL; a closed historian refuses.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { openHistorian } = require('../lib/client');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');

let passed = 0;
async function ok(label, fn) { await fn(); passed++; console.log('✔ ' + label); }
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-w-'));
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const DAY = 864e5;
const T0 = Math.floor((Date.now() - 3 * DAY) / 1000) * 1000;

// the worst stall of the main thread's event loop while fn runs (a 5 ms timer, how late it fires)
async function stall(fn) {
    let worst = 0, last = performance.now();
    const t = setInterval(() => { const now = performance.now(); worst = Math.max(worst, now - last - 5); last = now; }, 5);
    await wait(20);
    const r = await fn();
    await wait(20);
    clearInterval(t);
    return { worst, r };
}

(async () => {
    const dir = tmp();
    let db = openHistorian(dir, { walSync: false });
    await db.ready;

    await ok('numbers, bools and strings written from the main thread come back exact', async () => {
        for (let i = 0; i < 50000; i++) {
            const t = T0 + i * 100;
            db.write('Line1.Speed', t, Math.round((100 + Math.sin(i / 50) * 10) * 100) / 100);
            if (i % 10 === 0) db.write('Line1.Running', t, i % 70 < 50);
            if (i % 600 === 0) db.write('Line1.Mode', t, ['Auto', 'Manual', 'Setup'][(i / 600) % 3]);
        }
        const r = await db.query({ tags: 'Line1.*', from: T0, to: T0 + DAY, mode: 'raw' });
        assert.strictEqual(r['Line1.Speed'].t.length, 50000);
        assert.strictEqual(r['Line1.Speed'].v[123], Math.round((100 + Math.sin(123 / 50) * 10) * 100) / 100);
        assert.deepStrictEqual(r['Line1.Mode'].v.slice(0, 4), ['Auto', 'Manual', 'Setup', 'Auto']);
        assert.strictEqual(r['Line1.Running'].v[0], true);
        await wait(1200);
        assert.ok(db.stats.points >= 55000, 'the worker reports its counts: ' + db.stats.points);
    });

    await ok('a late point and a wrong type are refused in the worker and counted', async () => {
        db.write('Line1.Speed', T0, 1);
        db.write('Line1.Speed', T0 + DAY, 'text');
        assert.strictEqual(db.write('Line1.Speed', T0 + DAY, { a: 1 }), false, 'an object is not a value of the core');
        await db.checkpoint();
        await wait(1200);
        assert.deepStrictEqual([db.stats.late, db.stats.badType], [1, 1]);
    });

    await ok('the event loop stays free while the worker decodes 2 M points (the same query in the main thread stalls it)', async () => {
        for (let i = 0; i < 2000000; i++) db.write('Big', T0 + i * 100, Math.round(Math.sin(i / 1000) * 10000) / 100);
        await db.checkpoint();
        const q = { tags: 'Big', from: T0, to: T0 + 3 * DAY, mode: 'bucket', bucket: '10s', agg: ['avg', 'max'] };
        const inWorker = await stall(() => db.query(q));
        assert.ok(inWorker.r.Big.t.length > 10000);
        await db.close();
        const e = new Engine(dir, { walSync: false, checkpointMs: 1e9, walFlushMs: 1e9 }).open();
        const inMain = await stall(async () => Q.run(e, q));
        e.close();
        console.log('   worst main-thread stall: in the worker ' + inWorker.worst.toFixed(1) + ' ms, in the main thread ' + inMain.worst.toFixed(1) + ' ms');
        assert.ok(inWorker.worst < 50, 'the main thread is not held');
        assert.ok(inMain.worst > inWorker.worst * 3, 'the worker is what keeps it free');
        db = openHistorian(dir, { walSync: false });
        await db.ready;
    });

    await ok('a worker killed hard (no close) loses nothing that reached the WAL', async () => {
        const d = tmp(), h = openHistorian(d, { walSync: true, walFlushMs: 100 });
        await h.ready;
        for (let i = 0; i < 30000; i++) h.write('K', T0 + i * 100, i);
        h.flush();
        await wait(600);                                    // the batch in the worker, the WAL fsynced
        h.closed = true;                                    // no restart, no reply expected
        await h.worker.terminate();
        const h2 = openHistorian(d, { walSync: false });
        const info = await h2.ready;
        const r = await h2.query({ tags: 'K', from: T0, to: T0 + DAY, mode: 'raw' });
        assert.strictEqual(r.K.t.length, 30000, 'every point back');
        assert.ok(r.K.v.every((v, i) => v === i), 'once each, in order');
        console.log('   recovered ' + info.recovered + ' points from the WAL');
        await h2.close();
    });

    await ok('an I/O error in the worker: it restarts the engine (recovery from the WAL), batches wait, nothing acknowledged is lost, writes carry on', async () => {
        const d = tmp(), h = openHistorian(d, { walSync: true, walFlushMs: 50, batchMs: 10, workerEnv: { TSDB_TEST_FAULT: 'flushWal:6' } });
        await h.ready;
        const errors = []; h.onError = (e) => errors.push(e.message);
        const N = 6000;
        for (let k = 0; k < N; k++) { while (!h.write('E1', T0 + k * 1000, k)) await wait(5); if (k % 3 === 0) { while (!h.write('E2', T0 + k * 1000, 'state' + (k % 4))) await wait(5); } if (k % 400 === 0) await wait(30); }
        await wait(1500);
        assert.ok(errors.some((m) => /restarting the historian/.test(m)), 'the error was reported: ' + JSON.stringify(errors));
        assert.ok(h.stats.restarts >= 1 && h.recoveries >= 1, 'restarted and recovered: ' + h.stats.restarts + ' / ' + h.recoveries);
        await h.checkpoint();
        const r = await h.query({ tags: ['E1', 'E2'], from: T0, to: T0 + N * 1000, mode: 'raw' });
        assert.strictEqual(r.E1.t.length, N, 'every point of E1');
        assert.ok(r.E1.v.every((v, i) => v === i && r.E1.t[i] === T0 + i * 1000));
        assert.strictEqual(r.E2.t.length, N / 3, 'every point of E2');
        assert.ok(r.E2.v.every((v, i) => v === 'state' + ((i * 3) % 4)));
        const v = await h.admin({ op: 'verify' });
        assert.ok(v.ok, 'the database is clean: ' + JSON.stringify(v.problems.slice(0, 2)));
        await h.close();
    });

    await ok('closed: a write is refused, a query rejects; a bad query rejects with its reason', async () => {
        await assert.rejects(db.query({ tags: 'Big', mode: 'nope' }), /unknown mode/);
        await db.close();
        assert.strictEqual(db.write('X', Date.now(), 1), false);
        await assert.rejects(db.query({ tags: 'Big' }), /closed/);
    });

    fs.rmSync(dir, { recursive: true, force: true });
    console.log(`\n${passed} passed\nALL OK`);
})().catch((e) => { console.error(e); process.exit(1); });
