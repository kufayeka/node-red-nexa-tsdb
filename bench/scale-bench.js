'use strict';
// The scale benchmark, through the worker as Node-RED uses it:
//   A. 100 000 tags: real time at 1 s (100 000 points/s) and the most it takes at 100 ms; checkpoints, memory, disk
//   B. a year: 1 tag every second (31.5 M points) and 100 tags every minute (52.6 M points); the queries
//   C. ten years: projected from the measured sizes (bytes a point, index records a tag)
//
//   node bench/scale-bench.js [--tags 100000] [--seconds 120] [--year 1]
const fs = require('fs');
const os = require('os');
const path = require('path');
const { openHistorian } = require('../lib/client');
// a chart: one bucket per pixel column, with the four values a line chart needs
const chart = (q, width) => Object.assign({ mode: 'bucket', bucket: Math.max(1, Math.ceil((q.to - q.from + 1) / width)), agg: ['first', 'min', 'max', 'last'] }, q);

const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? +process.argv[i + 1] : d; };
const TAGS = arg('tags', 100000), SECONDS = arg('seconds', 120), YEARS = arg('year', 1);
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-scale-'));
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const ms = (t) => (t < 10 ? t.toFixed(2) : t.toFixed(0)) + ' ms';
const mb = (b) => (b / 1048576).toFixed(0) + ' MB';
const du = (d) => { let n = 0, files = 0; for (const f of fs.readdirSync(d, { withFileTypes: true })) { if (f.isDirectory()) { const r = du(path.join(d, f.name)); n += r.n; files += r.files; } else { n += fs.statSync(path.join(d, f.name)).size; files++; } } return { n, files }; };
let seed = 9; const rnd = () => { seed = (seed * 16807) % 2147483647; return seed / 2147483647; };
const DAY = 864e5, YEAR = 365 * DAY;
const lines = [];
// the backpressure: a point refused as an overload is written again once the worker caught up
let db;
async function put(tag, t, v) { while (!db.write(tag, t, v)) await wait(5); }
const out = (s) => { console.log(s); lines.push(s); };

(async () => {
    db = openHistorian(dir, { walSync: true, rawDays: 4000, indexDays: 4000, checkpointMs: 60000 });
    await db.ready;
    const names = Array.from({ length: TAGS }, (_, i) => 'Plant.Area' + (i % 50) + '.Tag' + i), x = new Float64Array(TAGS).fill(50);

    // A1. 100 000 tags at 1 s, written as fast as it goes (each second of data = one burst of every tag)
    {
        const t0 = Math.floor((Date.now() - SECONDS * 1000 - 3 * 3600000) / 1000) * 1000;
        const a = performance.now();
        let worst = 0, last = performance.now();
        const probe = setInterval(() => { const n = performance.now(); worst = Math.max(worst, n - last - 5); last = n; }, 5);
        for (let s = 0; s < SECONDS; s++) {
            const t = t0 + s * 1000;
            for (let i = 0; i < TAGS; i++) { x[i] += (rnd() - 0.5) * 0.2; const v = Math.round(x[i] * 100) / 100; if (!db.write(names[i], t, v)) await put(names[i], t, v); }
            await new Promise(setImmediate);
        }
        const c0 = performance.now(); await db.checkpoint(); const cp = performance.now() - c0;
        clearInterval(probe);
        const sec = (performance.now() - a) / 1000, n = TAGS * SECONDS;
        out(`A1  ${TAGS.toLocaleString()} tags × ${SECONDS} s at 1 s = ${(n / 1e6).toFixed(1)} M points in ${sec.toFixed(1)} s → ${Math.round(n / sec).toLocaleString()} points/s (real time needs ${TAGS.toLocaleString()}/s: ${(n / sec / TAGS).toFixed(1)}× real time)`);
        out(`    a checkpoint of ${TAGS.toLocaleString()} open chunks: ${ms(cp)}; Node-RED thread held at most ${ms(worst)} per burst; process memory ${mb(process.memoryUsage().rss)}`);
        const d = du(dir);
        out(`    disk ${mb(d.n)} in ${d.files.toLocaleString()} files (${(d.n / n).toFixed(2)} bytes a point incl. WAL + index)`);
    }

    // A2. the same 100 000 tags at 100 ms: the most it takes
    {
        const SEC = Math.max(10, Math.round(SECONDS / 4)), t0 = Math.floor((Date.now() - 2 * 3600000) / 1000) * 1000;
        const a = performance.now();
        for (let s = 0; s < SEC * 10; s++) {
            const t = t0 + s * 100;
            for (let i = 0; i < TAGS; i++) { x[i] += (rnd() - 0.5) * 0.2; const v = Math.round(x[i] * 100) / 100; if (!db.write(names[i], t, v)) await put(names[i], t, v); }
            await new Promise(setImmediate);
        }
        await db.checkpoint();
        const sec = (performance.now() - a) / 1000, n = TAGS * SEC * 10;
        out(`A2  ${TAGS.toLocaleString()} tags at 100 ms, ${SEC} s of data = ${(n / 1e6).toFixed(0)} M points in ${sec.toFixed(1)} s → ${Math.round(n / sec).toLocaleString()} points/s (real time needs ${(TAGS * 10).toLocaleString()}/s: ${(n / sec / TAGS / 10).toFixed(2)}× real time)`);
    }
    await wait(1200);
    const st = db.stats;
    if (st.overload) out(`    (the writer was held back ${st.overload.toLocaleString()} times by the backpressure and wrote those points again)`);
    out(`    totals: ${st.points.toLocaleString()} points, ${st.chunks.toLocaleString()} chunks, ${(st.chunkBytes / st.chunkPoints).toFixed(2)} bytes a point in chunks, refused ${st.late + st.badType}`);

    // B. a year
    const end = Math.floor(Date.now() / 60000) * 60000 - 4 * 3600000, from = end - YEARS * YEAR;
    {
        const a = performance.now();
        let v = 80, n = 0;
        for (let t = from; t <= end; t += 1000) { v += (rnd() - 0.5) * 0.2; const w = Math.round(v * 100) / 100; if (!db.write('Year.Sec', t, w)) await put('Year.Sec', t, w); if (++n % 200000 === 0) await new Promise(setImmediate); }
        const vs = new Float64Array(100).fill(50);
        for (let t = from; t <= end; t += 60000) { for (let i = 0; i < 100; i++) { vs[i] += (rnd() - 0.5) * 0.5; const w = Math.round(vs[i] * 100) / 100; if (!db.write('Year.Min' + i, t, w)) await put('Year.Min' + i, t, w); n++; } if (n % 200000 < 100) await new Promise(setImmediate); }
        await db.checkpoint();
        out(`B   ${YEARS} year: 1 tag at 1 s + 100 tags at 1 min = ${(n / 1e6).toFixed(1)} M points written in ${((performance.now() - a) / 1000).toFixed(0)} s`);
    }
    const time = async (label, q) => {
        const ts = []; let r;
        for (let i = 0; i < 5; i++) { const a = performance.now(); r = await db.query(q); ts.push(performance.now() - a); }
        ts.sort((p, q2) => p - q2);
        const pts = Object.values(r).reduce((s, o) => s + o.t.length, 0);
        out(`    ${label.padEnd(52)} ${ms(ts[2]).padStart(9)}  → ${Object.keys(r).length} tag(s), ${pts.toLocaleString()} points`);
    };
    await time(`chart ${YEARS} y, 1 tag at 1 s, 1 200 px`, chart({ tags: 'Year.Sec', from, to: end }, 1200));
    await time(`chart ${YEARS} y, 1 tag at 1 s, 4 000 px`, chart({ tags: 'Year.Sec', from, to: end }, 4000));
    await time('chart 30 days, 1 tag at 1 s, 1 200 px', chart({ tags: 'Year.Sec', from: end - 30 * DAY, to: end }, 1200));
    await time(`per day, ${YEARS} y, 1 tag`, { tags: 'Year.Sec', from, to: end, mode: 'bucket', bucket: '1d', agg: ['avg', 'min', 'max'] });
    await time(`per hour, ${YEARS} y, 1 tag`, { tags: 'Year.Sec', from, to: end, mode: 'bucket', bucket: '1h', agg: ['avg', 'min', 'max'] });
    await time(`per 8 h shift from 06:00, ${YEARS} y, 1 tag`, { tags: 'Year.Sec', from, to: end, mode: 'bucket', bucket: '8h', offset: '6h', agg: ['avg', 'count'] });
    await time('raw, last hour, 1 tag at 1 s', { tags: 'Year.Sec', from: end - 3600000, to: end, mode: 'raw' });
    await time(`chart ${YEARS} y, 100 tags at 1 min, 600 px each`, chart({ tags: 'Year.Min*', from, to: end }, 600));
    await time(`per day, ${YEARS} y, 100 tags`, { tags: 'Year.Min*', from, to: end, mode: 'bucket', bucket: '1d', agg: ['avg', 'max'] });
    await time(`last value of ${TAGS.toLocaleString()} tags`, { tags: 'Plant.*', mode: 'last' });
    await time('chart 2 h of 2 000 tags at 100 ms, 300 px each', chart({ tags: 'Plant.Area7.*', from: Date.now() - 4 * 3600000, to: Date.now() }, 300));

    // C. ten years, from what was measured
    const d = du(dir);
    const idx = fs.readdirSync(path.join(dir, 'idx'));
    const r0 = idx.filter((f) => f.endsWith('.r0')).reduce((s, f) => s + fs.statSync(path.join(dir, 'idx', f)).size, 0);
    out(`C   disk now ${mb(d.n)} in ${d.files.toLocaleString()} files; per-chunk summaries ${mb(r0)}`);
    const bpp = st.chunkBytes / st.chunkPoints;
    const proj = (label, tags, periodS, changeShare) => {
        const pts = tags * (YEAR / 1000 / periodS) * changeShare;
        const raw = pts * bpp, r0y = pts / 1024 * 96, r1y = tags * 8760 * 96, r2y = tags * 365 * 96;
        out(`    ${label.padEnd(44)} raw ${(raw / 1e12).toFixed(2)} TB/y · chunk summaries ${(r0y / 1e9).toFixed(1)} GB/y · hour + day ${((r1y + r2y) / 1e9).toFixed(1)} GB/y → 10 y of summaries ${((r1y + r2y) * 10 / 1e9).toFixed(0)} GB`);
    };
    proj('100 000 tags, 1 s, every value stored', 100000, 1, 1);
    proj('100 000 tags, 1 s, 10 % change (RBE)', 100000, 1, 0.1);
    proj('9 000 tags, 100 ms, every value stored', 9000, 0.1, 1);
    proj('9 000 tags, 100 ms, 10 % change (RBE)', 9000, 0.1, 0.1);
    proj('1 000 tags, 1 s, every value stored', 1000, 1, 1);
    fs.writeFileSync(path.join(__dirname, '..', 'bench', 'scale-results.txt'), lines.join('\n') + '\n');
    await db.close();
    fs.rmSync(dir, { recursive: true, force: true });
})().catch((e) => { console.error(e); process.exit(1); });
