'use strict';
// The engine through its worker, as Node-RED uses it: write throughput of 9 000 tags at 100 ms, the main thread's
// worst stall while writing and while querying, the query times as a node sees them (the message round trip included).
//
//   node bench/worker-bench.js [--tags 9000] [--seconds 60] [--months 6]
const fs = require('fs');
const os = require('os');
const path = require('path');
const { openHistorian } = require('../lib/client');
// a chart: one bucket per pixel column, with the four values a line chart needs
const chart = (q, width) => Object.assign({ mode: 'bucket', bucket: Math.max(1, Math.ceil((q.to - q.from + 1) / width)), agg: ['first', 'min', 'max', 'last'] }, q);

const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? +process.argv[i + 1] : d; };
const TAGS = arg('tags', 9000), SECONDS = arg('seconds', 60), MONTHS = arg('months', 6);
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-wbench-'));
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const ms = (t) => t.toFixed(t < 10 ? 2 : 0) + ' ms';

let worst = 0, last = performance.now();
const probe = setInterval(() => { const now = performance.now(); worst = Math.max(worst, now - last - 5); last = now; }, 5);
const resetStall = () => { worst = 0; last = performance.now(); };

(async () => {
    const db = openHistorian(dir, { walSync: true, rawDays: 400, indexDays: 400, checkpointMs: 30000 });
    await db.ready;

    // 1. 9 000 tags × 100 ms, in real time order: each 100 ms of data written in one go (as a Sparkplug burst would)
    {
        const t0 = Date.now() - SECONDS * 1000 - 60000, names = Array.from({ length: TAGS }, (_, i) => 'Plant.Line' + (i % 20) + '.Tag' + i), x = new Float64Array(TAGS).fill(50);
        let seed = 3; const rnd = () => { seed = (seed * 16807) % 2147483647; return seed / 2147483647; };
        resetStall();
        const a = performance.now(), steps = SECONDS * 10;
        for (let p = 0; p < steps; p++) {
            const t = t0 + p * 100;
            for (let i = 0; i < TAGS; i++) { x[i] += (rnd() - 0.5) * 0.2; db.write(names[i], t, Math.round(x[i] * 100) / 100); }
            await new Promise(setImmediate);   // one 100 ms burst (all tags) per turn of the event loop, as a live flow delivers it
        }
        await db.checkpoint();
        const s = (performance.now() - a) / 1000, n = TAGS * steps;
        console.log(`write   ${TAGS} tags × ${steps} (100 ms) = ${(n / 1e6).toFixed(1)} M points in ${s.toFixed(1)} s = ${Math.round(n / s).toLocaleString()} points/s (real time needs ${(TAGS * 10).toLocaleString()}/s)`);
        console.log(`        main thread: the write calls themselves, worst stall ${ms(worst)} (the encoding and the disk are in the worker)`);
    }

    // 2. one tag, a long history at 1 s
    const end = Math.floor(Date.now() / 1000) * 1000 - 3600000, from = end - MONTHS * 30 * 864e5;
    {
        let x = 80, n = 0;
        for (let t = from; t <= end; t += 1000) { x += (Math.random() - 0.5) * 0.2; db.write('Plant.Long', t, Math.round(x * 100) / 100); if (++n % 200000 === 0) await new Promise(setImmediate); }
        await db.checkpoint();
        console.log(`write   1 tag × ${MONTHS} months at 1 s = ${(n / 1e6).toFixed(1)} M points`);
    }

    // 3. queries as a node sees them; the main thread's stall during each
    const time = async (label, q) => {
        const ts = [];
        let worstQ = 0, out;
        for (let i = 0; i < 5; i++) { resetStall(); const a = performance.now(); out = await db.query(q); ts.push(performance.now() - a); worstQ = Math.max(worstQ, worst); }
        ts.sort((a, b) => a - b);
        const pts = Object.values(out).reduce((s, x) => s + x.t.length, 0);
        console.log(`query   ${label.padEnd(44)} ${ms(ts[2]).padStart(8)}   main-thread stall ${ms(worstQ).padStart(7)}   → ${pts} points`);
    };
    await time(`chart ${MONTHS} months, 1 200 px`, chart({ tags: 'Plant.Long', from, to: end }, 1200));
    await time('chart last 7 days, 1 200 px', chart({ tags: 'Plant.Long', from: end - 7 * 864e5, to: end }, 1200));
    await time(`avg / max per hour, ${MONTHS} months`, { tags: 'Plant.Long', from, to: end, mode: 'bucket', bucket: '1h', agg: ['avg', 'max'] });
    await time('per 1 s, last 24 h (decodes 86 400 points)', { tags: 'Plant.Long', from: end - 864e5, to: end, mode: 'bucket', bucket: '1s', agg: ['avg'] });
    await time('raw, last 1 hour', { tags: 'Plant.Long', from: end - 3600000, to: end, mode: 'raw' });
    await time('a line: 450 tags, last 2 min, 600 px', chart({ tags: 'Plant.Line3.*', from: Date.now() - SECONDS * 1000 - 60000, to: Date.now() }, 600));

    clearInterval(probe);
    await db.close();
    fs.rmSync(dir, { recursive: true, force: true });
})().catch((e) => { console.error(e); process.exit(1); });
