'use strict';
// Stress: many tags written without pause, queries hammering, the worker killed hard again and again, a delete and
// compactions under load. At the end every stored point is checked against what was written:
//   - no duplicate, time strictly rising, every value the one written for its time;
//   - a missing point only within the last moment before a kill (its batch / WAL not yet on disk) or in the deleted range.
//
//   node test/stress.js [--tags 2000] [--seconds 60] [--kills 10]
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { openHistorian } = require('../lib/client');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');

const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? +process.argv[i + 1] : d; };
const TAGS = arg('tags', 2000), SECONDS = arg('seconds', 60), KILLS = arg('kills', 10);
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-stress-'));
const OPTS = { walSync: true, walFlushMs: 100, checkpointMs: 2000, rawDays: 400, indexDays: 400 };
const LOSS_MS = 600;            // a kill may lose what was written in its last moment (the 50 ms batch, the 100 ms WAL sync)
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const name = (i) => 'S.T' + i;
const val = (i, s) => ((i * 31 + s * 7) % 1000) / 10;
const T0 = Math.floor((Date.now() - 6 * 3600000) / 1000) * 1000;
const tOf = (s) => T0 + s * 100;

(async () => {
    let db = openHistorian(dir, OPTS);
    await db.ready;
    const stepWall = [];            // when each step was handed over (wall clock)
    const kills = [];               // the wall clock of each kill
    let deleted = null, step = 0, queries = 0, queryErrors = 0, adminOps = 0, held = 0, running = true;
    const start = performance.now();

    // the querier: random queries without pause; a query that dies with a killed worker is fine, a wrong one is not
    const querier = (async () => {
        while (running) {
            const q = [
                { tags: name(Math.floor(Math.random() * TAGS)), from: T0, to: tOf(step), width: 400 },
                { tags: 'S.T1*', from: T0, to: tOf(step), mode: 'bucket', bucket: '1m', agg: ['avg', 'count'] },
                { tags: name(Math.floor(Math.random() * TAGS)), from: tOf(Math.max(0, step - 600)), to: tOf(step), mode: 'raw' },
                { tags: 'S.*', mode: 'last' }
            ][Math.floor(Math.random() * 4)];
            try {
                const r = await db.query(q);
                for (const s of Object.values(r)) for (let k = 1; k < s.t.length; k++) if (!(s.t[k] > s.t[k - 1]) && q.mode !== 'bucket') throw new Error('times not rising in a query: ' + JSON.stringify(q));
                queries++;
            } catch (e) { if (/worker|closed/.test(e.message)) queryErrors++; else throw e; }
            await wait(5);
        }
    })();

    // the writer, with the kills, the delete and the compactions on the way
    const killEvery = (SECONDS * 1000) / (KILLS + 1);
    let nextKill = killEvery, nextAdmin = 1500;
    while (performance.now() - start < SECONDS * 1000) {
        // a good writer: a point refused by the backpressure (or while the worker restarts) is written again
        for (let i = 0; i < TAGS; i++) while (!db.write(name(i), tOf(step), val(i, step))) { held++; await wait(2); }
        stepWall[step] = performance.now();
        step++;
        await new Promise(setImmediate);
        const now = performance.now() - start;
        if (now > nextKill && kills.length < KILLS) {
            nextKill += killEvery;
            kills.push(performance.now());
            db.closed = true;
            await db.worker.terminate();                      // a crash: no close, no checkpoint
            db = openHistorian(dir, OPTS);
            await db.ready;
        } else if (now > nextAdmin) {
            nextAdmin += 3000;
            try {
                if (!deleted && step > 3000) {
                    deleted = { tag: 'S.T7', from: tOf(step - 2500), to: tOf(step - 2000) };
                    await db.admin({ op: 'deleteRange', tags: deleted.tag, from: deleted.from, to: deleted.to });
                } else await db.admin({ op: adminOps % 2 ? 'compact' : 'diagnose' });
                adminOps++;
            } catch (e) { if (!/worker|closed/.test(e.message)) throw e; }
        }
    }
    running = false;
    await querier;
    await db.close();

    // the check, on a fresh engine
    const e = new Engine(dir, Object.assign({}, OPTS, { checkpointMs: 1e9, walFlushMs: 1e9 })).open();
    const lossy = (s) => kills.some((k) => stepWall[s] <= k && stepWall[s] > k - LOSS_MS);
    let points = 0, lost = 0, lostOutside = 0;
    const sample = Array.from({ length: Math.min(TAGS, 200) }, (_, k) => Math.floor((k * TAGS) / Math.min(TAGS, 200)));
    if (!sample.includes(7)) sample.push(7);
    for (const i of sample) {
        const r = Q.run(e, { tags: name(i), from: T0, to: tOf(step), mode: 'raw' })[name(i)];
        const got = new Map();
        for (let k = 0; k < r.t.length; k++) {
            if (k) assert.ok(r.t[k] > r.t[k - 1], name(i) + ': times strictly rising (no duplicate)');
            const s = (r.t[k] - T0) / 100;
            assert.ok(Number.isInteger(s) && s >= 0 && s < step, name(i) + ': a time that was written');
            assert.strictEqual(r.v[k], val(i, s), name(i) + ' step ' + s + ': its value');
            got.set(s, true);
        }
        points += r.t.length;
        for (let s = 0; s < step; s++) {
            if (got.has(s)) continue;
            const del = deleted && name(i) === deleted.tag && tOf(s) >= deleted.from && tOf(s) <= deleted.to;
            if (del) continue;
            lost++;
            if (!lossy(s)) lostOutside++;
        }
        if (deleted && name(i) === deleted.tag) assert.ok(!r.t.some((t) => t >= deleted.from && t <= deleted.to), 'the deleted range stays deleted');
    }
    e.close();
    const total = step * TAGS;
    console.log(`stress: ${TAGS} tags × ${step} steps = ${(total / 1e6).toFixed(1)} M points in ${SECONDS} s, ${kills.length} hard kills, ${queries} queries (${queryErrors} cut by a kill), ${adminOps} admin ops (a delete, compactions), the writer held back ${held} times`);
    console.log(`check:  ${sample.length} tags read back, ${points.toLocaleString()} points, every value right, no duplicate; ${lost} missing, all ${lostOutside ? 'NOT ' : ''}within ${LOSS_MS} ms before a kill`);
    assert.strictEqual(lostOutside, 0, 'nothing lost outside the moment before a kill');
    fs.rmSync(dir, { recursive: true, force: true });
    console.log('ALL OK');
})().catch((e) => { console.error(e); process.exit(1); });
