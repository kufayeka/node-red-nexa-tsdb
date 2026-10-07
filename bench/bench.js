'use strict';
// Benchmark: write throughput over many tags, disk size, and query times over a long history.
//
//   node bench/bench.js                         9 000 tags × 600 points (100 ms) + 1 tag × 6 months at 1 s
//   node bench/bench.js --tags 9000 --points 600 --months 6 --period 1000 --dir <folder>
//
// The data is a random walk with 2 decimals (a PLC value). The folder is deleted at the end unless --keep.
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');

const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? process.argv[i + 1] : d; };
const TAGS = +arg('tags', 9000), POINTS = +arg('points', 600), MONTHS = +arg('months', 6), PERIOD = +arg('period', 1000);
const dir = arg('dir', fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-bench-')));
const keep = process.argv.includes('--keep');

const du = (d) => { let n = 0; for (const f of fs.readdirSync(d, { withFileTypes: true })) n += f.isDirectory() ? du(path.join(d, f.name)) : fs.statSync(path.join(d, f.name)).size; return n; };
const mb = (b) => (b / 1048576).toFixed(1) + ' MB';
const ms = (t) => t.toFixed(t < 10 ? 2 : 0) + ' ms';
let seed = 1;
const rnd = () => { seed = (seed * 16807) % 2147483647; return seed / 2147483647; };

const e = new Engine(dir, { walSync: true, checkpointMs: 1e9, walFlushMs: 1e9, rawDays: 400, indexDays: 400 }).open();

// 1. many tags at 100 ms
{
    const t0 = Date.now() - POINTS * 100 - 60000, names = Array.from({ length: TAGS }, (_, i) => 'Plant.Line' + (i % 20) + '.Tag' + i), x = new Float64Array(TAGS).fill(50);
    const start = process.hrtime.bigint();
    for (let p = 0; p < POINTS; p++) {
        const t = t0 + p * 100;
        for (let i = 0; i < TAGS; i++) { x[i] += (rnd() - 0.5) * 0.2; e.write(names[i], t, Math.round(x[i] * 100) / 100); }
        if (p % 10 === 9) e.flushWal();      // the WAL written and fsynced every second of data
    }
    e.checkpoint();
    const s = Number(process.hrtime.bigint() - start) / 1e9, n = TAGS * POINTS;
    console.log(`write  ${TAGS} tags × ${POINTS} points (100 ms) = ${(n / 1e6).toFixed(1)} M points in ${s.toFixed(1)} s = ${Math.round(n / s).toLocaleString()} points/s (real time needs ${(TAGS * 10).toLocaleString()}/s)`);
    console.log(`       ${(e.stats.chunkBytes / e.stats.chunkPoints).toFixed(2)} bytes/point in chunks, ${mb(du(dir))} on disk`);
}

// 2. one tag, a long history
const LONG = 'Plant.Long', end = Math.floor(Date.now() / 1000) * 1000 - 3600000, from = end - MONTHS * 30 * 864e5;
{
    let x = 80;
    const start = process.hrtime.bigint();
    let n = 0;
    for (let t = from; t <= end; t += PERIOD) { x += (rnd() - 0.5) * 0.2; e.write(LONG, t, Math.round(x * 100) / 100); n++; if (n % 500000 === 0) e.flushWal(); }
    e.checkpoint();
    const s = Number(process.hrtime.bigint() - start) / 1e9;
    console.log(`write  1 tag × ${MONTHS} months every ${PERIOD} ms = ${(n / 1e6).toFixed(1)} M points in ${s.toFixed(1)} s`);
}
e.close();

// 3. queries on a fresh open (the OS cache is warm: a cold disk adds its seek times)
const r = new Engine(dir, { rawDays: 400, indexDays: 400 }).open();
const time = (label, q, count) => {
    const runs = 5, ts = [];
    let out;
    for (let i = 0; i < runs; i++) { const a = process.hrtime.bigint(); out = Q.run(r, q, end + 3600000); ts.push(Number(process.hrtime.bigint() - a) / 1e6); }
    ts.sort((a, b) => a - b);
    console.log(`query  ${label.padEnd(46)} ${ms(ts[Math.floor(runs / 2)]).padStart(9)}  (best ${ms(ts[0])})  → ${count(out)}`);
};
const pts = (o) => Object.values(o).reduce((s, x) => s + x.t.length, 0) + ' points';
time(`chart ${MONTHS} months, 1 200 px (M4)`, { tags: LONG, from, to: end, width: 1200 }, pts);
time(`chart ${MONTHS} months, 4 000 px (M4)`, { tags: LONG, from, to: end, width: 4000 }, pts);
time('chart last 7 days, 1 200 px (M4)', { tags: LONG, from: end - 7 * 864e5, to: end, width: 1200 }, pts);
time('chart last 1 hour, 1 200 px (M4)', { tags: LONG, from: end - 3600000, to: end, width: 1200 }, pts);
time(`avg / min / max per hour, ${MONTHS} months`, { tags: LONG, from, to: end, mode: 'bucket', bucket: '1h', agg: ['avg', 'min', 'max'] }, pts);
time(`per 8 h shift (06:00), ${MONTHS} months`, { tags: LONG, from, to: end, mode: 'bucket', bucket: '8h', offset: '6h', agg: ['avg', 'min', 'max', 'count'] }, pts);
time('per 5 min, last 24 h', { tags: LONG, from: end - 864e5, to: end, mode: 'bucket', bucket: '5m', agg: ['avg', 'max'] }, pts);
time('raw, last 1 hour', { tags: LONG, from: end - 3600000, to: end, mode: 'raw' }, pts);
time('last value', { tags: LONG, mode: 'last', to: end }, pts);
time(`20 tags (a line) × 1 h of 100 ms, 1 200 px`, { tags: 'Plant.Line3.*', from: end - 3600000 * 2, to: end + 3600000, width: 1200 }, (o) => Object.keys(o).length + ' tags, ' + pts(o));
r.close();
console.log(`disk   ${mb(du(dir))} in ${dir}`);
if (!keep) fs.rmSync(dir, { recursive: true, force: true });
