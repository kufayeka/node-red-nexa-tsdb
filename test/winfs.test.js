'use strict';
// Windows refuses to rename a file over one that is open (EPERM / EACCES / EBUSY), by our own handle or by another process' (the old
// worker still closing, an indexer, a virus scan). It broke a changed keep rule: the retention that runs at open rewrote an index file with
// its own handle still open on it, the open failed with EPERM, and the worker stopped for good (code 1).
//   - the rename tries again for a while, and gives up with the real error;
//   - a compaction that cannot rename is not an error of the data path: the open goes on, the file waits for the next pass;
//   - a worker that cannot open its folder yet (the old one still holds the LOCK) tries again, the points wait for it.
// Runs on every platform (on one that allows the rename the checks of the busy case are skipped, said in the label).
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const cp = require('child_process');
const fsx = require('../lib/fsx');
const { Engine, RECB, REC, F } = require('../lib/engine');
const Q = require('../lib/query');
const { openHistorian } = require('../lib/client');

const WIN = process.platform === 'win32';
let passed = 0;
async function ok(label, fn) { await fn(); passed++; console.log('✔ ' + label); }
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-w-'));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const HOUR = 3600000, T0 = Date.UTC(2024, 0, 1);
const tLastOf = (i) => T0 + i * HOUR + 59 * 60000;
function writeIdx(file, n) {
    const buf = Buffer.alloc(n * RECB), f = new Float64Array(buf.buffer, buf.byteOffset, n * REC);
    for (let i = 0; i < n; i++) { const o = i * REC; f[o + F.tFirst] = T0 + i * HOUR; f[o + F.tLast] = tLastOf(i); f[o + F.count] = 60; f[o + F.seg] = T0; f[o + F.off] = i; }
    fs.writeFileSync(file, buf);
}
// another process that holds `file` open for `ms` (resolves when it does)
function hold(file, ms) {
    const child = cp.spawn(process.execPath, ['-e', `const fs=require('fs');fs.openSync(${JSON.stringify(file)},'r');process.stdout.write('open');setTimeout(()=>process.exit(0),${ms});`], { stdio: ['ignore', 'pipe', 'ignore'] });
    const opened = new Promise((r) => child.stdout.once('data', r));
    const gone = new Promise((r) => child.once('exit', r));
    return opened.then(() => ({ child, gone }));
}
// a fs that answers EPERM / EBUSY / ... for the first `n` renames
const busyFs = (code, n) => { let calls = 0; return { calls: () => calls, renameSync() { if (++calls <= n) { const e = new Error(code + ': busy'); e.code = code; throw e; } } }; };

(async () => {
    await ok('renameSync tries again while the file is busy (EPERM, EACCES, EBUSY) and then goes through', async () => {
        for (const code of ['EPERM', 'EACCES', 'EBUSY']) {
            const f = busyFs(code, 4);
            fsx.renameSync('a', 'b', { fs: f, budgetMs: 3000 });
            assert.strictEqual(f.calls(), 5, code + ': 4 busy, the 5th goes through');
        }
    });

    await ok('renameSync gives up with the real error after its budget; any other error at once', async () => {
        const f = busyFs('EPERM', 1e9), t0 = Date.now();
        assert.throws(() => fsx.renameSync('a', 'b', { fs: f, budgetMs: 200 }), (e) => e.code === 'EPERM');
        const took = Date.now() - t0;
        assert.ok(took >= 150 && took < 1500, 'it waited about its budget: ' + took + ' ms');
        const g = { calls: 0, renameSync() { this.calls++; const e = new Error('nope'); e.code = 'ENOENT'; throw e; } };
        assert.throws(() => fsx.renameSync('a', 'b', { fs: g, budgetMs: 5000 }), (e) => e.code === 'ENOENT');
        assert.strictEqual(g.calls, 1, 'a missing file is not busy: no waiting');
    });

    await ok('a file another process holds open for a while (' + (WIN ? 'EPERM on this platform' : 'no refusal here: the rename just works') + '): the rename waits for it and succeeds', async () => {
        const d = tmp(), f = path.join(d, '0.r0');
        fs.writeFileSync(f, 'old'); fs.writeFileSync(f + '.tmp', 'new');
        const h = await hold(f, 400);
        fsx.renameSync(f + '.tmp', f, { budgetMs: 4000 });
        assert.strictEqual(fs.readFileSync(f, 'utf8'), 'new');
        await h.gone;
    });

    // the data of a tag: its real records after 12 000 hourly ones, the first 3 250 of them past `indexDays` (a quarter of the file: due)
    const O = { walSync: false, checkpointMs: 1e9, walFlushMs: 1e9, indexDays: 365, rawDays: 36500, renameBudgetMs: 300 };
    function dueIndex(d) {
        const now = tLastOf(12010), t0 = now - 3 * HOUR;
        const e = new Engine(d, O).open();
        for (let i = 0; i < 200; i++) e.write('T0', t0 + i * 1000, i);
        e.checkpoint(); e.close();
        const f = path.join(d, 'idx', '0.r0'), real = fs.readFileSync(f);
        writeIdx(f, 12000); fs.appendFileSync(f, real);
        return { f, now, t0, size: fs.statSync(f).size };
    }

    await ok('retention does not fail when it cannot rename: the file is as it was, no .tmp is left, the pass is counted; a later pass compacts it' + (WIN ? '' : ' (no refusal on this platform: it just compacts)'), async () => {
        const d = tmp(), { f, now, size } = dueIndex(d);
        const h = await hold(f, 2500);
        const e = new Engine(d, O).open();       // its own retention at open runs on today's clock: everything of this file is due
        assert.doesNotThrow(() => e.retention(now), 'a busy file is not an error of the engine');
        if (WIN) {
            assert.strictEqual(fs.statSync(f).size, size, 'the file is untouched');
            assert.ok(!fs.existsSync(f + '.tmp'), 'no .tmp left');
            assert.ok(e.stats.retentionBusy >= 2, 'the pass at open and this one say they could not: ' + e.stats.retentionBusy);
            assert.ok(e.opened, 'the engine is still open');
        }
        await h.gone;
        e.retention();
        assert.ok(fs.statSync(f).size < size, 'the next pass compacted it: ' + fs.statSync(f).size + ' < ' + size);
        assert.ok(!fs.existsSync(f + '.tmp'));
        e.close();
    });

    await ok('the open does not fail because the retention at open cannot rename (it was the crash: EPERM at open, the worker gone)', async () => {
        const d = tmp(), { f } = dueIndex(d);
        const h = await hold(f, 2500);
        let e = null;
        assert.doesNotThrow(() => { e = new Engine(d, O).open(); });
        assert.ok(e && e.opened, 'open');
        if (WIN) assert.ok(e.stats.retentionBusy >= 1, 'and it counted the pass it could not do');
        const r = Q.run(e, { tags: 'T0', from: 0, to: Date.now() + 1e9, mode: 'raw', limit: 1e6 }).T0;
        assert.ok(r && r.t.length === 200, 'the data is all there: ' + (r && r.t.length));
        e.close();
        await h.gone;
    });

    await ok('a worker whose first open fails with an I/O error tries again (it used to stop for good, code 1); the points written meanwhile wait and are stored', async () => {
        const d = tmp();
        const h = openHistorian(d, { walFlushMs: 50, checkpointMs: 1e9, rawDays: 36500, indexDays: 36500, workerEnv: { TSDB_TEST_FAULT: 'open:1' } });   // the 1st open throws an EIO
        const said = [];
        h.onError = (e) => said.push(e.message);
        let ready = false;
        h.ready.then(() => { ready = true; }, () => {});
        // until it says so (a worker starts in ~80-160 ms here: a fixed 100 ms wait failed now and then)
        for (let i = 0; i < 300 && !said.length; i++) await sleep(10);
        assert.ok(said.some((m) => /injected test fault/.test(m) && /again in/.test(m)), 'it said why and that it tries again: ' + JSON.stringify(said));
        assert.strictEqual(ready, false, 'not ready yet, and not dead');
        const ts = Date.now() - 2000;
        assert.ok(h.write('late', ts, 42), 'a point is taken while the historian is not open');
        const info = await Promise.race([h.ready, sleep(8000).then(() => { throw new Error('never opened'); })]);
        assert.ok(info.tags >= 0);
        const r = await h.query({ tags: 'late', from: ts - 1000, to: ts + 1000, mode: 'raw' });
        assert.deepStrictEqual(Array.from(r.late.v), [42], 'the point that came first is stored');
        await h.close();
    });

    await ok('a redeploy whose close fails with an I/O error: the old worker ENDS (it used to restart and fight the new database node for the LOCK every 30 s); the new one opens and has every point', async () => {
        const d = tmp();
        const a = openHistorian(d, { walFlushMs: 50, checkpointMs: 1e9, rawDays: 36500, indexDays: 36500, workerEnv: { TSDB_TEST_FAULT: 'checkpoint:1' } });   // the close's checkpoint throws an EIO
        const saidA = [];
        a.onError = (e) => saidA.push(e.message);
        await a.ready;
        const ts = Date.now() - 5000;
        for (let i = 0; i < 50; i++) a.write('R', ts + i * 10, i);
        await sleep(300);                                          // in the WAL
        await a.close();                                           // the redeploy: it resolves, it does not hang
        assert.ok(a.worker.threadId < 0, 'the old worker is gone');
        assert.ok(saidA.some((m) => /closed after an error/.test(m)), 'it said the close had an error: ' + JSON.stringify(saidA));
        const b = openHistorian(d, { walFlushMs: 50, checkpointMs: 1e9, rawDays: 36500, indexDays: 36500 });   // the new database node
        const saidB = [];
        b.onError = (e) => saidB.push(e.message);
        await b.ready;
        const r = await b.query({ tags: 'R', from: ts - 1000, to: ts + 1000, mode: 'raw', limit: 1000 });
        assert.strictEqual(r.R.v.length, 50, 'every point is there (recovered from the WAL)');
        await sleep(1500);
        assert.ok(!saidA.concat(saidB).some((m) => /could not restart|another engine/.test(m)), 'nobody fights for the LOCK: ' + JSON.stringify(saidA.concat(saidB)));
        await b.close();
    });

    await ok('a folder another engine holds is still refused at once with its message (two database nodes on one folder), not waited for', async () => {
        const d = tmp(), a = openHistorian(d, {});
        await a.ready;
        const b = openHistorian(d, {});
        b.onError = () => {};
        const t0 = Date.now();
        await assert.rejects(b.ready, /another engine/);
        assert.ok(Date.now() - t0 < 5000, 'it did not wait');
        await a.close();
    });

    await ok('the keep rule made SHORTER between two runs (the user\'s change, through the real historian): it opens (on Windows it died with EPERM at open) and answers only what is within keep', async () => {
        const d = tmp(), DAY = 86400000, now = Date.now();
        const O2 = { walFlushMs: 50, checkpointMs: 1e9, rawDays: 36500, indexDays: 36500 };
        let h = openHistorian(d, O2);
        await h.ready;
        for (let i = 0; i < 1200; i++) h.write('press1.kW', now - 5 * DAY + i * 360000, i);
        await h.checkpoint(); await h.close();
        h = openHistorian(d, Object.assign({ rules: [{ pattern: 'press*', store: 'disk', keep: '2d', raw: '2d' }] }, O2));
        await h.ready;
        const r = await h.query({ tags: 'press1.kW', from: now - 6 * DAY, to: now, mode: 'raw' });
        const t = Array.from(r['press1.kW'].t);
        assert.ok(t.length > 400 && t.length < 560, 'about two days of 10 points an hour: ' + t.length);
        assert.ok(t[0] >= now - 2 * DAY - 1000, 'nothing older than keep is answered');
        const st = await h.admin({ op: 'stats' }).catch(() => null);
        await h.close();
        assert.ok(st === null || typeof st === 'object');
    });

    console.log('\n' + passed + ' passed\nALL OK');
    process.exit(0);
})().catch((e) => { console.error(e); process.exit(1); });
