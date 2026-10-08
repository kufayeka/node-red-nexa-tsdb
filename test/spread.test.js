'use strict';
// stddev, variance, median and percentiles: every answer equals a brute force over the points, through every level of the pyramid
// (raw points, chunk, hour and day summaries), checkpoints, reopens and crashes; large values close together (no loss of digits);
// sample and population; a percentile only where the raw points are kept; strings, empty buckets, errors.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');
const admin = require('../lib/admin');

const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? +process.argv[i + 1] : d; };
const SEED = arg('seed', 12), ROUNDS = arg('rounds', 10);
let s = SEED >>> 0;
const rnd = () => { s = (s + 0x6D2B79F5) >>> 0; let t = s; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
const pick = (a) => a[Math.floor(rnd() * a.length)];
const int = (a, b) => a + Math.floor(rnd() * (b - a + 1));
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-s-'));
const open = (dir, o) => new Engine(dir, Object.assign({ walSync: false, checkpointMs: 1e9, walFlushMs: 1e9, rawDays: 36500, indexDays: 36500 }, o)).open();
const crash = (e) => { e.flushWal(); e._timers.forEach(clearInterval); fs.closeSync(e.walFd); e.segFds.forEach((x) => fs.closeSync(x.fd)); e.closeIdx(); e._unlock(); };
const MIN = 60000, HOUR = 3600000, DAY = 86400000, T0 = Date.UTC(2026, 0, 1);
let passed = 0;
function ok(label, fn) { const t = Date.now(); try { fn(); } catch (e) { console.error('FAILED (seed ' + SEED + '): ' + label); throw e; } passed++; console.log('✔ ' + label + ' (' + (Date.now() - t) + ' ms)'); }
const near = (a, b, what, tol) => {
    if (a === null || b === null || a === undefined || b === undefined) return assert.strictEqual(a, b, what);
    assert.ok(Math.abs(a - b) <= (tol || 1e-9) * Math.max(1, Math.abs(a), Math.abs(b)), what + ': ' + a + ' vs ' + b);
};

// ---- the brute force: two passes over each bucket's values, a full sort for the percentiles ------------------------
function bruteVar(vs, population) {
    const n = vs.length, d = population ? n : n - 1;
    if (!(d > 0)) return null;
    const mean = vs.reduce((a, b) => a + b, 0) / n;
    return vs.reduce((a, v) => a + (v - mean) * (v - mean), 0) / d;
}
function brutePct(vs, p) {
    if (!vs.length) return null;
    const x = vs.slice().sort((a, b) => a - b), h = (x.length - 1) * p, lo = Math.floor(h);
    return lo + 1 < x.length ? x[lo] + (h - lo) * (x[lo + 1] - x[lo]) : x[lo];
}
function bruteBuckets(pts, from, to, size) {
    const origin = Math.floor(from / size) * size, m = new Map();
    for (const [t, v] of pts) {
        if (t < from || t > to) continue;
        const k = Math.floor((t - origin) / size) * size + origin;
        if (!m.has(k)) m.set(k, []);
        m.get(k).push(v);
    }
    return [...m.entries()].sort((a, b) => a[0] - b[0]);
}

// a noisy process around a large base: a sum of squares would lose the spread, m2 does not
function series(n, base) {
    const out = []; let t = T0, v = base;
    for (let i = 0; i < n; i++) {
        t += pick([1000, 10000, MIN, MIN, 5 * MIN, rnd() < 0.02 ? 3 * HOUR : MIN]);
        v += (rnd() - 0.5) * 2;
        out.push([t, rnd() < 0.05 ? Math.round(v) : v]);
    }
    return out;
}
function load(d, o, name, pts) {
    let e = open(d, o);
    const cp = int(150, 3000);
    pts.forEach(([t, v], w) => {
        assert.ok(e.write(name, t, v));
        if ((w + 1) % cp === 0) { const x = rnd(); if (x < 0.4) e.checkpoint(); else if (x < 0.7) { e.close(); e = open(d, o); } else { crash(e); e = open(d, o); } }
    });
    return e;
}

ok('stddev and variance per bucket (levels: raw, chunk, hour, day), sample and population, equal to a brute force, around 0 and around 1e9', () => {
    for (let round = 0; round < ROUNDS; round++) {
        const base = pick([0, 1000, 1e9]), d = tmp(), pts = series(int(200, 6000), base), o = { chunkPoints: pick([32, 128, 1024]), segmentMs: pick([HOUR, HOUR, DAY / 4]) };
        const e = load(d, o, 'V', pts), first = pts[0][0], last = pts[pts.length - 1][0];
        for (let k = 0; k < 8; k++) {
            const size = pick([MIN, 10 * MIN, HOUR, 6 * HOUR, DAY, 7 * DAY]), from = int(first - HOUR, last), to = Math.min(last + HOUR, from + pick([HOUR, 5 * HOUR, DAY, 6 * DAY, 30 * DAY]));
            const population = rnd() < 0.5;
            const r = Q.run(e, { tags: 'V', from, to, mode: 'bucket', bucket: size, agg: ['stddev', 'variance', 'count'], population }).V;
            const B = bruteBuckets(pts, from, to, size);
            assert.deepStrictEqual(r.t, B.map((b) => b[0]), 'the same buckets');
            B.forEach(([ts, vs], i) => {
                const v = bruteVar(vs, population);
                // relative to the spread, not to the values: 1e9 +- a few must still give the few
                near(r.variance[i], v, 'variance of bucket ' + new Date(ts).toISOString() + ' (' + vs.length + ' values)', 1e-6);
                near(r.stddev[i], v === null ? null : Math.sqrt(v), 'stddev of bucket ' + i, 1e-6);
            });
        }
        assert.strictEqual(admin.run(e, { op: 'verify' }).ok, true);
        e.close();
    }
});

ok('median and percentiles per bucket, equal to a brute force over the raw points', () => {
    for (let round = 0; round < ROUNDS; round++) {
        const d = tmp(), pts = series(int(200, 5000), pick([0, 50])), o = { chunkPoints: pick([32, 128, 1024]) };
        const e = load(d, o, 'P', pts), first = pts[0][0], last = pts[pts.length - 1][0];
        for (let k = 0; k < 6; k++) {
            const size = pick([MIN, 10 * MIN, HOUR, DAY]), from = int(first - HOUR, last), to = Math.min(last + HOUR, from + pick([HOUR, DAY, 6 * DAY]));
            const r = Q.run(e, { tags: 'P', from, to, mode: 'bucket', bucket: size, agg: ['median', 'p0', 'p25', 'p95', 'p99.9', 'p100', 'min', 'max'] }).P;
            const B = bruteBuckets(pts, from, to, size);
            assert.deepStrictEqual(r.t, B.map((b) => b[0]), 'the same buckets');
            B.forEach(([, vs], i) => {
                near(r.median[i], brutePct(vs, 0.5), 'median ' + i);
                near(r.p25[i], brutePct(vs, 0.25), 'p25 ' + i);
                near(r.p95[i], brutePct(vs, 0.95), 'p95 ' + i);
                near(r['p99.9'][i], brutePct(vs, 0.999), 'p99.9 ' + i);
                assert.ok(r.p0[i] === r.min[i], 'p0 is the min');           // === : -0 and 0 are one value
                assert.ok(r.p100[i] === r.max[i], 'p100 is the max');
            });
        }
        const day = Q.run(e, { tags: 'P', from: first, to: last, mode: 'range', agg: ['median'] }).P;
        near(day.median[0], brutePct(pts.map((p) => p[1]), 0.5), 'the median of the whole range');
        e.close();
    }
});

ok('known answers: 2 4 4 4 5 5 7 9 (population stddev 2, sample variance 32 / 7, median 4.5, p25 4, p90 7.6); one point; fill', () => {
    const d = tmp(), e = open(d);
    [2, 4, 4, 4, 5, 5, 7, 9].forEach((v, k) => e.write('K', T0 + k * MIN, v));
    e.write('One', T0, 42);
    const q = { from: T0, to: T0 + HOUR - 1, mode: 'range' };
    const pop = Q.run(e, Object.assign({ tags: 'K', agg: ['stddev', 'variance'], population: true }, q)).K;
    assert.deepStrictEqual([pop.stddev[0], pop.variance[0]], [2, 4]);
    const smp = Q.run(e, Object.assign({ tags: 'K', agg: ['variance', 'median', 'p25', 'p90'] }, q)).K;
    near(smp.variance[0], 32 / 7, 'sample variance');
    assert.deepStrictEqual([smp.median[0], smp.p25[0]], [4.5, 4]);
    near(smp.p90[0], 7.6, 'p90');
    // one point: its sample stddev is not known (null), its population stddev is 0, its median is itself
    const one = Q.run(e, Object.assign({ tags: 'One', agg: ['stddev', 'median'] }, q)).One;
    assert.deepStrictEqual([one.stddev[0], one.median[0]], [null, 42]);
    assert.strictEqual(Q.run(e, Object.assign({ tags: 'One', agg: ['stddev'], population: true }, q)).One.stddev[0], 0);
    // an empty bucket with fill null
    const f = Q.run(e, { tags: 'K', from: T0, to: T0 + 3 * HOUR - 1, mode: 'bucket', bucket: '1h', fill: 'null', agg: ['stddev', 'median'] }).K;
    assert.deepStrictEqual([f.stddev.length, f.stddev[1], f.median[2]], [3, null, null]);
    // rows
    const rows = Q.run(e, Object.assign({ tags: 'K', agg: ['median', 'stddev'], population: true, format: 'rows' }, q));
    assert.deepStrictEqual(rows, [{ tag: 'K', ts: T0, median: 4.5, stddev: 2 }]);
    e.close();
});

ok('a string tag: its stddev and percentiles are null; a bool: the share of true is its mean, its median 0 or 1', () => {
    const d = tmp(), e = open(d);
    ['Run', 'Stop', 'Run', 'Run'].forEach((v, k) => e.write('S', T0 + k * MIN, v));
    [true, true, false, true].forEach((v, k) => e.write('B', T0 + k * MIN, v));
    const q = { from: T0, to: T0 + HOUR - 1, mode: 'range', agg: ['stddev', 'variance', 'median', 'p95'] };
    const str = Q.run(e, Object.assign({ tags: 'S' }, q)).S;
    assert.deepStrictEqual([str.stddev[0], str.variance[0], str.median[0], str.p95[0]], [null, null, null, null]);
    const b = Q.run(e, Object.assign({ tags: 'B', population: true }, q)).B;
    assert.strictEqual(b.median[0], 1);
    near(b.variance[0], 0.75 * 0.25, 'the variance of a bool');
    e.close();
});

ok('stddev comes from the summaries after the raw points are gone; a percentile is null there and clippedFrom says where raw starts', () => {
    const d = tmp(), e = open(d, { rawDays: 1 });
    const vs = [];
    for (let t = T0; t < T0 + DAY; t += 10000) { const v = (t / 10000) % 37; vs.push(v); e.write('C', t, v); }
    e.close();
    const e2 = open(d, { rawDays: 1 });                     // the data is far past one day of raw
    const r = Q.run(e2, { tags: 'C', from: T0, to: T0 + DAY - 1, mode: 'range', agg: ['stddev', 'median', 'count'], population: true }).C;
    near(r.stddev[0], Math.sqrt(bruteVar(vs, true)), 'stddev from the day summary');
    assert.strictEqual(r.count[0], vs.length);
    assert.strictEqual(r.median[0], null, 'no raw points: no median');
    assert.ok(r.clippedFrom > T0, 'clippedFrom: the raw keep');
    const s2 = Q.run(e2, { tags: 'C', from: T0, to: T0 + DAY - 1, mode: 'range', agg: ['stddev'] }).C;
    assert.strictEqual(s2.clippedFrom, undefined, 'stddev alone is answered whole: no clippedFrom');
    e2.close();
});

ok('errors: an unknown percentile; past maxPoints a percentile is refused with its reason', () => {
    const d = tmp(), e = open(d);
    for (let k = 0; k < 100; k++) e.write('A', T0 + k * 1000, k);
    const q = (agg, x) => () => Q.run(e, Object.assign({ tags: 'A', from: T0, to: T0 + HOUR, mode: 'range', agg }, x));
    assert.throws(q(['p101']), /unknown aggregate "p101"/);
    assert.throws(q(['pct95']), /unknown aggregate "pct95".*median, p0 \.\.\. p100/);
    assert.throws(q(['median'], { maxPoints: 50 }), /more than 50 points in the range: a percentile reads every raw point/);
    assert.strictEqual(q(['median'], { maxPoints: 100 })().A.median[0], 49.5);
    e.close();
});

console.log('\n' + passed + ' passed');
