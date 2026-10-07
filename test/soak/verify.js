'use strict';
// Verifies a dataset made by gen.js: random ranges, random modes, every answer compared with the one the model
// recomputes (brute force over the same points). Nothing is stored to know the right answer.
//
//   node test/soak/verify.js --dir D:\tsdb-soak [--queries 300] [--seed 1] [--max-points 6000000] [--reopen 100] [--direct]
//   node test/soak/verify.js --dir D:\tsdb-soak --only 17 --seed 1        replay one query of a run (printed on a failure)
//
// A query is one of: raw (every point exact), bucket (count / sum / min / max / first / last per bucket, with shift
// offsets), m4 (what a chart draws: every returned point must be a real point, the first / last / overall min and max
// exact, the columns' own min / max exact in nearly all columns), last. The range is random over the whole dataset, a
// fifth of the time "a whole calendar month of a random year". Every --reopen queries the database is closed and
// opened again (and its open time reported). --direct reads through the engine in this process (no worker).
const fs = require('fs');
const path = require('path');
const { openHistorian } = require('../../lib/client');
const { Engine } = require('../../lib/engine');
const Q = require('../../lib/query');
const { Model, DAY } = require('./model');

const argv = process.argv.slice(2);
const args = {};
for (let i = 0; i < argv.length; i++) if (argv[i].startsWith('--')) { const k = argv[i].slice(2), n = argv[i + 1]; if (n === undefined || n.startsWith('--')) args[k] = true; else { args[k] = n; i++; } }
const dir = args.dir && path.resolve(String(args.dir));
if (!dir || !fs.existsSync(path.join(dir, 'meta.json'))) { console.error('usage: node test/soak/verify.js --dir <a folder made by gen.js> [--queries 300] [--seed 1]'); process.exit(2); }
if (!fs.existsSync(path.join(dir, 'done.json'))) console.warn('warning: done.json is missing: the generator did not finish; the end of the data may be missing.');
const meta = JSON.parse(fs.readFileSync(path.join(dir, 'meta.json'), 'utf8')), model = new Model(meta);
const QUERIES = +args.queries || 300, SEED = +args.seed || 1, MAXP = +args['max-points'] || 6000000, REOPEN = args.reopen === undefined ? 100 : +args.reopen;
const OPTS = { rawDays: 36500, indexDays: 36500 };
const fmt = (n) => Math.round(n).toLocaleString('en-US');
const iso = (t) => new Date(t).toISOString().slice(0, 19) + 'Z';

function rng(seed) { let a = seed >>> 0; return () => { a = (a + 0x6D2B79F5) >>> 0; let t = a; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; }; }

let db = null, eng = null;
async function open() {
    const a = performance.now();
    if (args.direct) { eng = new Engine(dir, Object.assign({ walSync: false }, OPTS)).open(); }
    else { db = openHistorian(dir, OPTS); await db.ready; }
    return performance.now() - a;
}
async function close() { if (args.direct) { eng.close(); eng = null; } else { await db.close(); db = null; } }
const run = (q) => (args.direct ? Promise.resolve(Q.run(eng, q)) : db.query(q));
const listTags = async () => (args.direct ? eng.tagList() : db.tags());

// ---- the expected answers ----------------------------------------------------------------------------------
const near = (a, b) => Math.abs(a - b) <= 1e-6 * Math.max(1, Math.abs(b));

function checkRaw(i, res, q) {
    const r = res[model.names[i]];
    if (!r) return 'the tag is missing from the answer';
    let n = 0, bad = null;
    model.each(i, q.from, q.to, (t, v) => { if (!bad && (r.t[n] !== t || r.v[n] !== v)) bad = `point ${n}: expected ${iso(t)} ${v}, got ${r.t[n] === undefined ? 'nothing' : iso(r.t[n]) + ' ' + r.v[n]}`; n++; });
    if (!bad && r.t.length !== n) bad = `${r.t.length} points, expected ${n}`;
    return bad;
}

function checkBucket(i, res, q) {
    const r = res[model.names[i]];
    if (!r) return 'the tag is missing from the answer';
    const size = Q.parseDuration(q.bucket), off = q.offset ? Q.parseDuration(q.offset) : 0, origin = Math.floor((q.from - off) / size) * size + off;
    const want = new Map(), numeric = model.kind[i] === 0;
    model.each(i, q.from, q.to, (t, v) => {
        const b = origin + Math.floor((t - origin) / size) * size;
        let a = want.get(b);
        if (!a) want.set(b, (a = { n: 0, sum: 0, min: Infinity, max: -Infinity, first: v, last: v }));
        a.n++; a.last = v;
        if (numeric) { a.sum += v; if (v < a.min) a.min = v; if (v > a.max) a.max = v; }
    });
    if (r.t.length !== want.size) return `${r.t.length} buckets, expected ${want.size}`;
    for (let b = 0; b < r.t.length; b++) {
        const w = want.get(r.t[b]);
        if (!w) return `a bucket at ${iso(r.t[b])} that has no points`;
        if (r.count[b] !== w.n) return `bucket ${iso(r.t[b])}: count ${r.count[b]}, expected ${w.n}`;
        if (r.first[b] !== w.first || r.last[b] !== w.last) return `bucket ${iso(r.t[b])}: first / last ${r.first[b]} / ${r.last[b]}, expected ${w.first} / ${w.last}`;
        if (numeric && (r.min[b] !== w.min || r.max[b] !== w.max || !near(r.sum[b], w.sum) || !near(r.avg[b], w.sum / w.n))) return `bucket ${iso(r.t[b])}: min / max / sum ${r.min[b]} / ${r.max[b]} / ${r.sum[b]}, expected ${w.min} / ${w.max} / ${w.sum}`;
    }
    return null;
}

// a column's own min / max may miss a point within one chunk of the column's edge (the chunk is placed by its four points):
// "explained" = the true extreme lies that close to an edge. m4 is exact by default (every column exact); exact: false is the fast form.
const CHUNK_SPAN = Math.min(3600000, 1024 * meta.period) + meta.period;

// returns { bad, exact, explained, columns }
function checkM4(i, res, q) {
    const r = res[model.names[i]];
    if (!r) return { bad: 'the tag is missing from the answer' };
    const k0 = (t) => Math.round((t - model.t0) / model.period);
    let first = null, last = null, gmin = Infinity, gmax = -Infinity;
    const size = (q.to - q.from + 1) / q.width, numeric = model.kind[i] === 0, cols = new Map();
    model.each(i, q.from, q.to, (t, v) => {
        if (!first) first = { t, v };
        last = { t, v };
        if (numeric) {
            if (v < gmin) gmin = v; if (v > gmax) gmax = v;
            const c = Math.floor((t - q.from) / size), edge = Math.min(t - (q.from + c * size), q.from + (c + 1) * size - t);
            let a = cols.get(c); if (!a) cols.set(c, (a = { min: Infinity, max: -Infinity, minEdge: 0, maxEdge: 0 }));
            if (v < a.min) { a.min = v; a.minEdge = edge; } else if (v === a.min && edge > a.minEdge) a.minEdge = edge;     // the occurrence farthest from an edge
            if (v > a.max) { a.max = v; a.maxEdge = edge; } else if (v === a.max && edge > a.maxEdge) a.maxEdge = edge;
        }
    });
    if (!first) return r.t.length ? { bad: `${r.t.length} points where the range has none` } : { bad: null, exact: 0, columns: 0 };
    if (!r.t.length) return { bad: 'no points where the range has some' };
    const got = new Map();
    for (let n = 0; n < r.t.length; n++) {
        const k = k0(r.t[n]);
        if (n && !(r.t[n] > r.t[n - 1])) return { bad: 'times not rising at point ' + n };
        if (!(model.present(i, k) && model.time(k) === r.t[n] && model.value(i, k) === r.v[n])) return { bad: `point ${n} (${iso(r.t[n])} ${r.v[n]}) is not a real point` };
        if (numeric) { const c = Math.floor((r.t[n] - q.from) / size); let a = got.get(c); if (!a) got.set(c, (a = { min: Infinity, max: -Infinity })); if (r.v[n] < a.min) a.min = r.v[n]; if (r.v[n] > a.max) a.max = r.v[n]; }
    }
    if (r.t[0] !== first.t || r.v[0] !== first.v) return { bad: `first point ${iso(r.t[0])} ${r.v[0]}, expected ${iso(first.t)} ${first.v}` };
    const n = r.t.length - 1;
    if (r.t[n] !== last.t || r.v[n] !== last.v) return { bad: `last point ${iso(r.t[n])} ${r.v[n]}, expected ${iso(last.t)} ${last.v}` };
    if (!numeric) return { bad: null, exact: 0, columns: 0 };
    let rmin = Infinity, rmax = -Infinity;
    for (const v of r.v) { if (v < rmin) rmin = v; if (v > rmax) rmax = v; }
    if (rmin !== gmin || rmax !== gmax) return { bad: `overall min / max ${rmin} / ${rmax}, expected ${gmin} / ${gmax}` };
    let exact = 0, explained = 0, unexplained = null;
    cols.forEach((w, c) => {
        const g = got.get(c);
        if (g && g.min === w.min && g.max === w.max) { exact++; return; }
        const minOk = g && g.min === w.min || w.minEdge <= CHUNK_SPAN, maxOk = g && g.max === w.max || w.maxEdge <= CHUNK_SPAN;
        if (minOk && maxOk && g && g.min >= w.min && g.max <= w.max) explained++;
        else if (!unexplained) unexplained = `column ${c} of ${q.width}: min / max ${g ? g.min + ' / ' + g.max : 'none'}, expected ${w.min} / ${w.max} (a true extreme ${Math.round(Math.min(w.minEdge, w.maxEdge) / 1000)} s from an edge: not explained by a straddling chunk)`;
    });
    if (unexplained) return { bad: unexplained };
    if (q.exact !== false && exact !== cols.size) return { bad: `${cols.size - exact} of ${cols.size} columns differ (m4 is exact by default)` };
    return { bad: null, exact, explained, columns: cols.size };
}

function checkLast(i, res, q) {
    const r = res[model.names[i]], w = model.lastBefore(i, q.to);
    if (!r) return 'the tag is missing from the answer';
    if (!w) return r.t.length ? 'a last point where there is none' : null;
    if (r.t[0] !== w.t || r.v[0] !== w.v) return `last ${r.t[0] === undefined ? 'nothing' : iso(r.t[0]) + ' ' + r.v[0]}, expected ${iso(w.t)} ${w.v}`;
    return null;
}

// ---- a random query ---------------------------------------------------------------------------------------------
function makeQuery(qi) {
    const rand = rng(SEED * 100003 + qi), pick = (a) => a[Math.floor(rand() * a.length)];
    const nt = 1 + Math.floor(rand() * 3), idx = [];
    while (idx.length < nt) { const i = Math.floor(rand() * meta.tags); if (!idx.includes(i)) idx.push(i); }
    const r = rand(), mode = r < 0.35 ? 'm4' : r < 0.7 ? 'bucket' : r < 0.9 ? 'raw' : 'last';
    const span = meta.steps * meta.period, cap = Math.min(span, Math.floor(MAXP / nt) * meta.period, mode === 'raw' ? 900000 * meta.period : Infinity);
    const minLen = Math.min(cap, Math.max(5 * meta.period, 300000));
    let from, to, jump = null;
    if (mode !== 'last' && rand() < 0.2 && mode !== 'raw') {
        // "a whole calendar month of a random year"
        const d = new Date(meta.t0 + rand() * span);
        from = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1); to = Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1) - 1;
        from = Math.max(from, meta.t0); if (to - from > cap) to = from + cap;
        jump = d.getUTCFullYear() + '-' + String(d.getUTCMonth() + 1).padStart(2, '0');
    } else {
        const len = Math.exp(Math.log(minLen) + rand() * (Math.log(cap) - Math.log(minLen)));
        from = meta.t0 + Math.floor(rand() * (span - len)); to = Math.min(meta.end - 1, from + Math.floor(len));
        from = Math.floor(from / 1000) * 1000;
    }
    const q = { tags: idx.map((i) => model.names[i]), from, to };
    if (mode === 'last') { q.mode = 'last'; q.to = to; delete q.from; }
    else if (mode === 'raw') { q.mode = 'raw'; q.limit = 1000000; }
    else if (mode === 'm4') { q.mode = 'm4'; q.width = pick([100, 300, 1200, 4000]); if (rand() < 0.3) q.exact = false; }
    else {
        const sizes = ['1m', '5m', '15m', '1h', '8h', '1d'].filter((s) => Q.parseDuration(s) >= meta.period && (to - from) / Q.parseDuration(s) < 400000);
        q.mode = 'bucket'; q.bucket = pick(sizes.length ? sizes : ['1d']); q.agg = ['avg', 'min', 'max', 'sum', 'count', 'first', 'last'];
        const off = pick(['', '', '6h', '17h', '23h', '30m']); if (off) q.offset = off;
    }
    return { q, idx, mode, jump };
}

const failures = [], timing = { m4: [], bucket: [], raw: [], last: [] };
let strictCols = 0, exactCols = 0, explainedCols = 0, allCols = 0, checkedPoints = 0, openMs = [];

(async () => {
    console.log(`dataset: ${meta.tags} tags every ${meta.period / 1000} s, ${meta.days} days (${iso(meta.t0)} .. ${iso(meta.end)}), ${fmt(meta.steps * meta.tags * 0.98 / 1e6)} M points`);
    openMs.push(await open());
    console.log(`opened in ${openMs[0].toFixed(0)} ms (${args.direct ? 'direct' : 'through the worker'})`);

    // every tag is there, with its last point
    const list = await listTags(), byName = new Map(list.map((t) => [t.name, t]));
    for (let i = 0; i < meta.tags; i++) {
        const t = byName.get(model.names[i]), w = model.lastBefore(i, meta.end);
        if (!t) failures.push({ qi: -1, what: 'tag ' + model.names[i] + ' is missing' });
        else if (t.type !== model.type(i)) failures.push({ qi: -1, what: `tag ${t.name} is ${t.type}, expected ${model.type(i)}` });
        else if (w && t.last !== w.t) failures.push({ qi: -1, what: `tag ${t.name}: last point ${t.last === null ? 'none' : iso(t.last)}, expected ${iso(w.t)}` });
    }
    console.log(failures.length ? `tags: ${failures.length} problem(s)` : `tags: all ${meta.tags} present with the right last point`);

    const only = args.only === undefined ? null : +args.only;
    for (let qi = only === null ? 0 : only; qi < (only === null ? QUERIES : only + 1); qi++) {
        if (REOPEN && qi && qi % REOPEN === 0) { await close(); openMs.push(await open()); }
        const { q, idx, mode, jump } = makeQuery(qi);
        const a = performance.now();
        let res = null, err = null;
        try { res = await run(q); } catch (e) { err = e; }
        const ms = performance.now() - a;
        timing[mode].push(ms);
        const desc = `${mode} ${idx.length} tag(s) ${iso(q.from || q.to)}${q.from ? ' .. ' + iso(q.to) : ''}${jump ? ' (month ' + jump + ')' : ''}${q.bucket ? ' bucket ' + q.bucket + (q.offset ? '+' + q.offset : '') : ''}${q.width ? ' width ' + q.width : ''}`;
        if (err) { failures.push({ qi, what: desc + ': ' + err.message }); continue; }
        for (const i of idx) {
            let bad = null;
            if (mode === 'raw') bad = checkRaw(i, res, q);
            else if (mode === 'bucket') bad = checkBucket(i, res, q);
            else if (mode === 'last') bad = checkLast(i, res, q);
            else { const m = checkM4(i, res, q); bad = m.bad; if (q.exact === false) { exactCols += m.exact || 0; explainedCols += m.explained || 0; allCols += m.columns || 0; } else strictCols += m.columns || 0; }
            if (bad) failures.push({ qi, what: `${desc} - ${model.names[i]}: ${bad}` });
        }
        if (only !== null) console.log(desc, ms.toFixed(0) + ' ms', failures.length ? 'FAILED' : 'ok');
        else if ((qi + 1) % 25 === 0) console.log(`${qi + 1} / ${QUERIES} queries, ${failures.length} failure(s)  (this process ${(process.memoryUsage().rss / 1048576).toFixed(0)} MB)`);
    }
    await close();

    const pct = (a, p) => { const s = a.slice().sort((x, y) => x - y); return s.length ? s[Math.min(s.length - 1, Math.floor(p * s.length))] : 0; };
    console.log('\nmode     queries     p50      p95      max');
    for (const m of ['m4', 'bucket', 'raw', 'last']) console.log(m.padEnd(8), String(timing[m].length).padStart(7), (pct(timing[m], 0.5).toFixed(0) + ' ms').padStart(9), (pct(timing[m], 0.95).toFixed(0) + ' ms').padStart(8), (Math.max(0, ...timing[m]).toFixed(0) + ' ms').padStart(8));
    console.log('\nm4 (exact by default): ' + strictCols + ' columns checked, every one exact; every returned point a real point; first / last / overall min and max exact.');
    if (allCols) console.log('m4 with exact: false (the fast form): columns exact ' + exactCols + ' of ' + allCols + ' (' + (100 * exactCols / allCols).toFixed(1) + ' %), ' + explainedCols + ' differ only by a point within one chunk of the column edge, 0 unexplained.');
    console.log(`open time: ${openMs.map((x) => x.toFixed(0)).join(', ')} ms`);
    if (failures.length) {
        console.log(`\nFAILED: ${failures.length}`);
        failures.slice(0, 15).forEach((f) => console.log(' - ' + (f.qi >= 0 ? `query ${f.qi} (replay: --seed ${SEED} --only ${f.qi}): ` : '') + f.what));
        process.exit(1);
    }
    console.log('\nALL OK: ' + QUERIES + ' random queries, every answer equal to the model');
    void checkedPoints; void DAY;
})().catch((e) => { console.error(e); process.exit(1); });
