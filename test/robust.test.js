'use strict';
// Found by the production review: a point that bricked the database (ts in the first hour of 1970), a wrong clock that made every
// later point "late", two engines on one folder, NaN in an average, a range cut by retention without a word, the store node
// swallowing refused points.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');
const admin = require('../lib/admin');
const { openHistorian } = require('../lib/client');

let passed = 0;
async function ok(label, fn) { await fn(); passed++; console.log('✔ ' + label); }
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-r-'));
const open = (dir, o) => new Engine(dir, Object.assign({ walSync: false, checkpointMs: 1e9, walFlushMs: 1e9, rawDays: 36500, indexDays: 36500 }, o)).open();
const HOUR = 3600000;

(async () => {
    await ok('a point in the first hour of 1970 is stored, survives a reopen, verifies clean (it used to make the database unopenable)', () => {
        const d = tmp();
        let e = open(d);
        for (let i = 0; i < 10; i++) e.write('A', 1000 + i * 1000, i);
        e.write('A', 5 * HOUR + 1, 99);
        e.close();
        e = open(d);
        const r = Q.run(e, { tags: 'A', from: 0, to: 1e9, mode: 'raw' }).A;
        assert.deepStrictEqual(r.v, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 99]);
        assert.strictEqual(admin.run(e, { op: 'verify' }).ok, true);
        e.close();
    });

    await ok('ts 0, a negative or NaN time, and a time far ahead of the clock are refused with their reason; the tag keeps taking good points', () => {
        const e = open(tmp()), now = Date.now();
        assert.strictEqual(e.write('B', 0, 1), false);
        assert.strictEqual(e.write('B', -5, 1), false);
        assert.strictEqual(e.write('B', NaN, 1), false);
        assert.strictEqual(e.write('B', now, 1), true);
        assert.strictEqual(e.write('B', now + 10 * 365 * 86400000, 2), false, 'a wrong clock, 10 years ahead');
        assert.match(e.stats.lastRefused.reason, /ahead of the clock/);
        let taken = 0;
        for (let i = 1; i <= 100; i++) if (e.write('B', now + i * 1000, i)) taken++;
        assert.strictEqual(taken, 100, 'the later points are not "late"');
        e.close();
    });

    await ok('a zero-filled WAL tail (a power cut) does not put a point at time 0 in a tag', () => {
        const d = tmp(), e = open(d);
        e.write('Z', Date.now() - 1000, 1); e.flushWal();
        e._timers.forEach(clearInterval); fs.closeSync(e.walFd); e.segFds.forEach((s) => fs.closeSync(s.fd)); e._unlock();
        const wal = path.join(d, 'wal', fs.readdirSync(path.join(d, 'wal'))[0]);
        fs.appendFileSync(wal, Buffer.alloc(400));
        const e2 = open(d);
        const r = Q.run(e2, { tags: 'Z', from: 0, to: Date.now() + 1000, mode: 'raw' }).Z;
        assert.strictEqual(r.t.length, 1);
        e2.close();
    });

    await ok('NaN is refused (counted as a bad value), so an average is never off by it', () => {
        const e = open(tmp()), t0 = Math.floor(Date.now() / HOUR) * HOUR - 2 * HOUR;
        for (let i = 0; i < 2000; i++) e.write('N', t0 + i * 1000, i % 2 ? NaN : 10);
        const r = Q.run(e, { tags: 'N', from: t0, to: t0 + 2000000, mode: 'bucket', bucket: '1h', agg: ['avg', 'count'] }).N;
        assert.deepStrictEqual([r.avg[0], r.count[0]], [10, 1000]);
        assert.strictEqual(e.stats.badType, 1000);
        e.close();
    });

    await ok('one engine a folder: a second open is refused (ETSDB_LOCKED); close, abort and a failed open release it; a dead process\'s lock is taken over', () => {
        const d = tmp(), a = open(d);
        assert.throws(() => open(d), (e) => e.code === 'ETSDB_LOCKED');
        a.close();
        const b = open(d); b.abort();
        const c = open(d); c.close();
        // a lock whose process is gone (a pid that is not running) is stale
        fs.writeFileSync(path.join(d, 'LOCK'), '2147483646:dead');
        open(d).close();
        // a stale heartbeat (a pid reused by another process) is stale too
        fs.writeFileSync(path.join(d, 'LOCK'), process.pid + ':old');
        const old = new Date(Date.now() - 120000); fs.utimesSync(path.join(d, 'LOCK'), old, old);
        open(d).close();
        // an open that fails (a damaged database) releases the lock, so the worker's retries are not locked out by themselves
        fs.mkdirSync(path.join(d, 'dict.log'));                          // cannot be read as a file: open() throws after taking the lock
        assert.throws(() => open(d), (e) => e.code === 'EISDIR');
        assert.ok(!fs.existsSync(path.join(d, 'LOCK')), 'no lock left by the failed open');
    });

    await ok('two historians (workers) on one folder: the second fails to open, the first is untouched and keeps its lock', async () => {
        const d = tmp(), a = openHistorian(d, {}); await a.ready;
        const b = openHistorian(d, {}); b.onError = () => {};
        await assert.rejects(b.ready, /another engine/);
        await new Promise((r) => setTimeout(r, 200));
        assert.ok(fs.existsSync(path.join(d, 'LOCK')), 'the loser did not remove the winner\'s lock');
        a.write('A', Date.now(), 1); assert.strictEqual((await a.query({ tags: 'A', from: '-1m', mode: 'raw' })).A.v[0], 1);
        await a.close();
        assert.ok(!fs.existsSync(path.join(d, 'LOCK')));
    });

    await ok('a range that starts before retention says so (clippedFrom); inside retention it does not', () => {
        const e = open(tmp(), { rawDays: 2 }), now = Date.now();
        for (let i = 0; i < 100; i++) e.write('R', now - 1000 * (100 - i), i);
        const inside = Q.run(e, { tags: 'R', from: '-1h', mode: 'raw' }).R;
        assert.strictEqual(inside.clippedFrom, undefined);
        const past = Q.run(e, { tags: 'R', from: '-10d', mode: 'raw' }).R;
        assert.strictEqual(past.t.length, 100);
        assert.ok(Math.abs(past.clippedFrom - (now - 2 * 86400000)) < 5000, 'the answer starts at the raw keep');
        e.close();
    });

    // the store node: a point the historian refuses (overload) is an error on the message, and with "changes only" it is tried again
    {
        const types = {}, nodes = {};
        const RED = { settings: { userDir: tmp() }, nodes: { registerType: (n, f) => { types[n] = f; }, createNode: (node, n) => { node._h = {}; node.on = (ev, f) => { node._h[ev] = f; }; node.status = () => {}; node.error = () => {}; node.log = () => {}; node.warn = () => {}; nodes[n.id] = node; }, getNode: (id) => nodes[id] } };
        require('../nodes/tsdb')(RED);
        const cfg = {}; types['tsdb-config'].call(cfg, { id: 'c', name: 'r' }); await cfg.engine.ready;
        const store = {}; types['tsdb-store'].call(store, { id: 's', db: 'c', changesOnly: true });
        const input = (msg) => new Promise((res) => store._h.input(msg, () => {}, (err) => res(err)));
        await ok('store node: an overload is an error on the message, and the value is stored when the historian takes it again', async () => {
            const keep = cfg.engine.o.maxInFlight; cfg.engine.o.maxInFlight = 0;
            const err = await input({ topic: 'Speed', payload: 120, timestamp: Date.now() - 5000 });
            assert.ok(err && /not stored.*overload/.test(err.message), String(err));
            cfg.engine.o.maxInFlight = keep;
            assert.strictEqual(await input({ topic: 'Speed', payload: 120, timestamp: Date.now() - 4000 }), undefined);
            const r = await cfg.engine.query({ tags: 'Speed', from: '-1m', mode: 'raw' });
            assert.deepStrictEqual(r.Speed.v, [120], 'the same value, refused once, is not lost for "changes only"');
        });
        store._h.close();
        await new Promise((res) => cfg._h.close(res));
    }
    console.log('\n' + passed + ' passed\nALL OK');
})().catch((e) => { console.error(e); process.exit(1); });
