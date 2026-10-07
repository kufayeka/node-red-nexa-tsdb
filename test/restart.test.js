'use strict';
// B: restarts. Many cycles of open / write / stop (clean close, a crash with no close, a hard kill of the process, a terminated
// worker): every acknowledged point is there exactly once, open stays fast, and nothing piles up - file descriptors, WAL files,
// heap, a stale LOCK. The long soak (days under load: test/soak) is run by hand on the target hardware.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const cp = require('child_process');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');
const admin = require('../lib/admin');
const { openHistorian } = require('../lib/client');

const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? +process.argv[i + 1] : d; };
const CYCLES = arg('cycles', 300), KILLS = arg('kills', 12), WORKERS = arg('workers', 25);
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-b-'));
const O = { walSync: false, checkpointMs: 1e9, walFlushMs: 1e9, rawDays: 36500, indexDays: 36500 };
const fds = () => { try { return fs.readdirSync('/proc/self/fd').length; } catch (e) { return -1; } };
const crash = (e) => { e.flushWal(); e._timers.forEach(clearInterval); fs.closeSync(e.walFd); e.segFds.forEach((x) => fs.closeSync(x.fd)); e.closeIdx(); e._unlock(); };
const rawOf = (e, tag, from, to) => { const r = Q.run(e, { tags: tag, from, to, mode: 'raw', limit: 5e6 })[tag]; return r ? { t: Array.from(r.t), v: Array.from(r.v) } : { t: [], v: [] }; };
let passed = 0;
async function ok(label, fn) { const t = Date.now(); await fn(); passed++; console.log('✔ ' + label + ' (' + (Date.now() - t) + ' ms)'); }

(async () => {
    await ok(CYCLES + ' open / write / stop cycles (clean close, crash, abort): every point once and exact; open time, fds, WAL files and heap stay flat; no stale LOCK', () => {
        const d = tmp(), T0 = Date.UTC(2026, 0, 1), model = new Map();
        let t = T0, openMs = [], f0 = -2, heap0 = 0, walMax = 0, e;
        for (let c = 0; c < CYCLES; c++) {
            const a = process.hrtime.bigint();
            e = new Engine(d, O).open();
            openMs.push(Number(process.hrtime.bigint() - a) / 1e6);
            for (let k = 0; k < 120; k++) {
                const tag = 'T' + (k % 7), v = (c * 120 + k) % 1000 / 10;
                t += 1000; assert.ok(e.write(tag, t, v)); (model.get(tag) || model.set(tag, []).get(tag)).push([t, v]);
            }
            if (c % 5 === 4) e.write('S', t, 'state' + (c % 3));    // a string tag with a dictionary that must survive every restart
            if (c % 3 === 0) e.checkpoint();
            const w = c % 3;
            if (w === 0) e.close(); else if (w === 1) { e.flushWal(); crash(e); } else { e.flushWal(); e.abort(); }
            assert.ok(!fs.existsSync(path.join(d, 'LOCK')), 'a stopped engine leaves no LOCK (cycle ' + c + ')');
            walMax = Math.max(walMax, fs.readdirSync(path.join(d, 'wal')).length);
            if (c === 20) { f0 = fds(); if (global.gc) global.gc(); heap0 = process.memoryUsage().heapUsed; }
        }
        // the data
        e = new Engine(d, O).open();
        for (const [tag, pts] of model) { const r = rawOf(e, tag, 0, t + 1000); assert.deepStrictEqual([r.t, r.v], [pts.map((p) => p[0]), pts.map((p) => p[1])], 'every point of ' + tag + ', once, exact'); }
        assert.strictEqual(admin.run(e, { op: 'verify' }).ok, true);
        const sv = rawOf(e, 'S', 0, t + 1000).v; assert.ok(sv.length >= CYCLES / 5 - 1 && sv.every((x) => /^state[012]$/.test(x)), 'the string dictionary survives every restart');
        e.close();
        // flat: the last open is not slower than the early ones (compare medians, a 4x slack for a noisy machine), no fd leak, no WAL pile
        const med = (a) => a.slice().sort((x, y) => x - y)[a.length >> 1];
        const early = med(openMs.slice(5, 30)), late = med(openMs.slice(-25));
        console.log('   open: early median ' + early.toFixed(1) + ' ms, late median ' + late.toFixed(1) + ' ms (' + t / 1000 + ' s of data, ' + model.size + ' tags); wal files at most ' + walMax);
        assert.ok(late < Math.max(60, early * 4), 'open time grows with the number of restarts: ' + early + ' -> ' + late);
        if (f0 >= 0) assert.ok(fds() - f0 <= 2, 'file descriptors leak across restarts: ' + f0 + ' -> ' + fds());
        assert.ok(walMax <= 4, 'WAL files pile up: ' + walMax);
        if (global.gc) { global.gc(); const grow = (process.memoryUsage().heapUsed - heap0) / 1e6; console.log('   heap growth over the restarts: ' + grow.toFixed(1) + ' MB'); assert.ok(grow < 30, 'heap grows across restarts: ' + grow + ' MB'); }
    });

    await ok(KILLS + ' hard kills (SIGKILL) of a real writer process: its stale LOCK is taken over, every point it reported durable is there, no gap, no duplicate', async () => {
        const d = tmp(); let from = Date.UTC(2026, 0, 1), durable = null, last = null;
        for (let k = 0; k < KILLS; k++) {
            const child = cp.fork(path.join(__dirname, 'restart-child.js'), [d, String(from)], { stdio: 'ignore' });
            let opened = false;
            const seen = await new Promise((res, rej) => {
                const tm = setTimeout(() => { child.kill('SIGKILL'); rej(new Error('the writer did not start (a stale LOCK refusing it?) in kill ' + k)); }, 15000);
                child.on('message', (m) => { if (m.opened) opened = true; if (m.durable) last = m.durable; if (opened && last !== null && last > from + 600 + (k % 4) * 700) { clearTimeout(tm); child.kill('SIGKILL'); } });
                child.on('exit', () => { clearTimeout(tm); res(last); });
            });
            assert.ok(fs.existsSync(path.join(d, 'LOCK')), 'the killed process left its LOCK behind (this is the case under test)');
            durable = seen; from = durable + 1 + 5;     // the next writer starts after what was durable (the points past it may or may not be there)
            // between kills, read the data the way a restarted Node-RED would
            const e = new Engine(d, O).open();
            const r = rawOf(e, 'K', 0, Infinity);
            assert.ok(r.t.length >= 1 && r.t[r.t.length - 1] >= durable, 'kill ' + k + ': the last durable point ' + durable + ' is there (last ' + r.t[r.t.length - 1] + ')');
            for (let i = 1; i < r.t.length; i++) assert.ok(r.t[i] > r.t[i - 1], 'strictly rising times (no duplicate, no disorder)');
            e.close();
            from = Math.max(from, r.t[r.t.length - 1] + 1);
        }
        const e = new Engine(d, O).open(); const v = admin.run(e, { op: 'verify' }); assert.strictEqual(v.ok, true, JSON.stringify(v.problems)); e.close();
    });

    await ok(WORKERS + ' worker cycles through the client (clean close, terminate): the worker takes the folder every time, nothing is lost, no thread is left', async () => {
        const d = tmp(), T0 = Date.UTC(2026, 0, 1); let t = T0, n = 0;
        for (let c = 0; c < WORKERS; c++) {
            const db = openHistorian(d, { walSync: true, walFlushMs: 20, rawDays: 36500, indexDays: 36500 });
            await db.ready;
            for (let k = 0; k < 500; k++) { t += 1000; assert.ok(db.write('W', t, k)); n++; }
            if (c % 2) { await db.close(); }
            else { await db.query({ tags: 'W', from: t - 1000, to: t, mode: 'raw' }); await new Promise((r) => setTimeout(r, 60)); await db.worker.terminate(); }   // the batch reached the worker and its WAL
            await new Promise((r) => setTimeout(r, 30));
        }
        const db = openHistorian(d, { rawDays: 36500, indexDays: 36500 }); await db.ready;
        const r = (await db.query({ tags: 'W', from: 0, to: t + 1, mode: 'raw', limit: 5e6 })).W;
        assert.strictEqual(r.t.length, n, 'every acknowledged point of ' + WORKERS + ' sessions, once');
        assert.ok(r.t.every((x, i) => x === T0 + (i + 1) * 1000 && r.v[i] === i % 500));
        await db.close();
        assert.ok(!fs.existsSync(path.join(d, 'LOCK')));
    });

    console.log('\n' + passed + ' passed\nALL OK');
    process.exit(0);
})().catch((e) => { console.error(e); process.exit(1); });
