'use strict';
// Calendar buckets (day, week, month, quarter, year in a time zone), bucket: "auto", and the aggregates across edges that are not one
// size: increase, delta, integral, counts equal a brute force, and the buckets of a range add up to the range.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');
const cal = require('../lib/calendar');

const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? +process.argv[i + 1] : d; };
const SEED = arg('seed', 5), ROUNDS = arg('rounds', 8);
let s = SEED >>> 0;
const rnd = () => { s = (s + 0x6D2B79F5) >>> 0; let t = s; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
const pick = (a) => a[Math.floor(rnd() * a.length)];
const int = (a, b) => a + Math.floor(rnd() * (b - a + 1));
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-cal-'));
const open = (dir, o) => new Engine(dir, Object.assign({ walSync: false, checkpointMs: 1e9, walFlushMs: 1e9, rawDays: 36500, indexDays: 36500 }, o)).open();
const MIN = 60000, HOUR = 3600000, DAY = 86400000;
let passed = 0;
function ok(label, fn) { const t = Date.now(); try { fn(); } catch (e) { console.error('FAILED (seed ' + SEED + '): ' + label); throw e; } passed++; console.log('✔ ' + label + ' (' + (Date.now() - t) + ' ms)'); }
const iso = (t) => new Date(t).toISOString();
const near = (a, b, what) => assert.ok(Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(a), Math.abs(b)), what + ': ' + a + ' vs ' + b);

ok('the edges of months, weeks, days, quarters and years: Jakarta (+7), Kolkata (+5:30), New York across daylight saving, Monday / Sunday weeks', () => {
    const list = (u, a, b, tz, ws) => Array.from(cal.edges(u, a, b, tz, ws)).map(iso);
    assert.deepStrictEqual(list('month', cal.parseLocal('2026-01-15', 'Asia/Jakarta'), cal.parseLocal('2026-03-10', 'Asia/Jakarta'), 'Asia/Jakarta'),
        ['2025-12-31T17:00:00.000Z', '2026-01-31T17:00:00.000Z', '2026-02-28T17:00:00.000Z', '2026-03-31T17:00:00.000Z'], 'Jakarta months start at 17:00 UTC of the day before');
    assert.deepStrictEqual(list('day', Date.UTC(2026, 0, 1, 12), Date.UTC(2026, 0, 3, 1), 'Asia/Kolkata'),
        ['2025-12-31T18:30:00.000Z', '2026-01-01T18:30:00.000Z', '2026-01-02T18:30:00.000Z', '2026-01-03T18:30:00.000Z'], 'Kolkata days start at 18:30 UTC');
    assert.deepStrictEqual(list('day', cal.parseLocal('2026-03-07', 'America/New_York'), cal.parseLocal('2026-03-09T12:00', 'America/New_York'), 'America/New_York'),
        ['2026-03-07T05:00:00.000Z', '2026-03-08T05:00:00.000Z', '2026-03-09T04:00:00.000Z', '2026-03-10T04:00:00.000Z'], 'the day of the spring change has 23 hours');
    assert.deepStrictEqual(list('day', cal.parseLocal('2026-11-01', 'America/New_York'), cal.parseLocal('2026-11-01T12:00', 'America/New_York'), 'America/New_York'),
        ['2026-11-01T04:00:00.000Z', '2026-11-02T05:00:00.000Z'], 'the day of the autumn change has 25 hours');
    assert.deepStrictEqual(list('week', Date.UTC(2026, 1, 4), Date.UTC(2026, 1, 17), 'UTC', 'mon'), ['2026-02-02T00:00:00.000Z', '2026-02-09T00:00:00.000Z', '2026-02-16T00:00:00.000Z', '2026-02-23T00:00:00.000Z']);
    assert.deepStrictEqual(list('week', Date.UTC(2026, 1, 4), Date.UTC(2026, 1, 10), 'UTC', 'sun'), ['2026-02-01T00:00:00.000Z', '2026-02-08T00:00:00.000Z', '2026-02-15T00:00:00.000Z']);
    assert.deepStrictEqual(list('quarter', Date.UTC(2026, 4, 1), Date.UTC(2026, 7, 1), 'UTC'), ['2026-04-01T00:00:00.000Z', '2026-07-01T00:00:00.000Z', '2026-10-01T00:00:00.000Z']);
    assert.deepStrictEqual(list('year', Date.UTC(2025, 5, 1), Date.UTC(2026, 5, 1), 'UTC'), ['2025-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z', '2027-01-01T00:00:00.000Z']);
    assert.strictEqual(iso(cal.parseLocal('2026-01-01T06:30:15.5', 'Asia/Jakarta')), '2025-12-31T23:30:15.500Z');
    assert.throws(() => cal.edges('fortnight', 0, 1, 'UTC'), /not a calendar unit/);
    assert.throws(() => cal.edges('week', 0, 1, 'UTC', 'someday'), /weekStart/);
    assert.throws(() => cal.formatter('Mars/Olympus'), /unknown time zone/);
});

// ---- brute force over explicit edges ----------------------------------------------------------------------------
const step = (a, b) => (b >= a ? b - a : b);
function bruteEdges(pts, from, to, edges) {
    const nb = edges.length - 1, idx = (t) => { if (t < edges[0]) return -1; for (let k = 0; k < nb; k++) if (t < edges[k + 1]) return k; return nb; };
    const B = Array.from({ length: nb }, () => ({ n: 0, sum: 0, min: Infinity, max: -Infinity, inc: 0, iL: 0, iS: 0, cov: 0, first: NaN, last: NaN, base: NaN, edge: false }));
    for (let i = 0; i < pts.length; i++) {
        const [t, v] = pts[i];
        if (t < from || t > to) continue;
        const k = idx(t), b = B[k], prev = i ? pts[i - 1] : null;
        if (!b.n) { b.base = prev ? prev[1] : NaN; b.first = v; }
        b.n++; b.sum += v; b.min = Math.min(b.min, v); b.max = Math.max(b.max, v); b.last = v;
        if (prev) {
            b.inc += step(prev[1], v);
            const [ta, va] = prev, gap = t - ta;
            if (gap > 0) for (let j = Math.max(idx(ta), 0); j <= idx(t) && j < nb; j++) {
                const sa = Math.max(ta, edges[j]), se = Math.min(t, edges[j + 1]);
                if (!(se > sa)) continue;
                const vs = va + (v - va) * (sa - ta) / gap, ve = va + (v - va) * (se - ta) / gap;
                B[j].iL += (vs + ve) / 2 * (se - sa); B[j].iS += va * (se - sa); B[j].cov += se - sa; B[j].edge = true;
            }
        }
    }
    return B;
}
function meter(from, to, stepMs) {                                   // a kWh meter: rises, plateaus, resets now and then, a gap now and then
    const pts = []; let t = from, v = 1000;
    while (t < to) {
        t += rnd() < 0.002 ? int(2 * HOUR, 20 * HOUR) : int(Math.floor(stepMs / 2), stepMs * 2);
        const w = rnd(); if (w < 0.002) v = pick([0, 3]); else if (w > 0.3) v += int(0, 120) / 100;
        pts.push([t, Math.round(v * 100) / 100]);
    }
    return pts;
}

ok('increase, delta, integral and counts over calendar buckets (days, weeks, months, quarters in Jakarta, Kolkata, New York) equal a brute force', () => {
    for (let round = 0; round < ROUNDS; round++) {
        const tz = pick(['UTC', 'Asia/Jakarta', 'Asia/Kolkata', 'America/New_York', 'Europe/London']);
        const t0 = Date.UTC(2026, 0, 1) + int(0, 30) * DAY, t1 = t0 + int(20, 200) * DAY, pts = meter(t0, t1, pick([5 * MIN, 15 * MIN, HOUR]));
        const d = tmp(), e = open(d, { chunkPoints: pick([64, 512]) });
        pts.forEach((p) => e.write('M', p[0], p[1])); if (rnd() < 0.5) e.checkpoint();
        for (let k = 0; k < 6; k++) {
            const unit = pick(['day', 'week', 'month', 'quarter', 'year']), a = int(t0, t1 - DAY), b = Math.min(t1, a + int(3, 120) * DAY), ws = pick(['mon', 'sun']);
            const r = Q.run(e, { tags: 'M', from: a, to: b, mode: 'bucket', bucket: unit, tz, weekStart: ws, agg: ['increase', 'delta', 'integral', 'count', 'sum', 'min', 'max'], per: 'h' }).M;
            const edges = cal.edges(unit, a, b, tz, ws), B = bruteEdges(pts, a, b, edges);
            assert.strictEqual(r.bucket, unit); assert.strictEqual(r.tz, tz);
            let row = 0;
            B.forEach((bk, j) => {
                if (!bk.n && !bk.edge) return;
                assert.strictEqual(r.t[row], edges[j], 'bucket start ' + iso(edges[j]) + ' (' + unit + ' ' + tz + ')');
                if (bk.n) {
                    assert.strictEqual(r.count[row], bk.n); near(r.sum[row], bk.sum, 'sum'); assert.strictEqual(r.min[row], bk.min); assert.strictEqual(r.max[row], bk.max);
                    near(r.increase[row], bk.inc, 'increase ' + iso(edges[j]));
                    near(r.delta[row], bk.last - (bk.base === bk.base ? bk.base : bk.first), 'delta');
                } else { assert.strictEqual(r.count[row], 0); assert.strictEqual(r.increase[row], null); }
                near(r.integral[row], bk.iL / HOUR, 'integral ' + iso(edges[j]));
                row++;
            });
            assert.strictEqual(r.t.length, row);
        }
        e.close();
    }
});

ok('the buckets of a range add up to the range: the increase and the integral by months equal the one row of the whole range', () => {
    const t0 = Date.UTC(2026, 0, 1), t1 = Date.UTC(2026, 6, 1), pts = meter(t0, t1, 10 * MIN), d = tmp(), e = open(d);
    pts.forEach((p) => e.write('M', p[0], p[1])); e.checkpoint();
    for (const tz of ['UTC', 'Asia/Jakarta', 'America/New_York']) {
        const from = cal.parseLocal('2026-02-01', tz), to = cal.parseLocal('2026-06-01', tz) - 1;
        const months = Q.run(e, { tags: 'M', from, to, mode: 'bucket', bucket: 'month', tz, agg: ['increase', 'integral'], per: 'h' }).M;
        const whole = Q.run(e, { tags: 'M', from, to, mode: 'range', agg: ['increase', 'integral'], per: 'h' }).M;
        assert.strictEqual(months.t.length, 4, tz + ': February, March, April, May');
        near(months.increase.reduce((x, y) => x + y, 0), whole.increase[0], tz + ' increase');
        near(months.integral.reduce((x, y) => x + y, 0), whole.integral[0], tz + ' integral');
    }
    e.close();
});

ok('bucket "auto": 6 months gives months, a month weeks, a week days, a day hours (and the unit is in the answer)', () => {
    const t0 = Date.UTC(2026, 0, 1), t1 = Date.UTC(2026, 7, 1), d = tmp(), e = open(d);
    for (let t = t0; t < t1; t += 15 * MIN) e.write('K', t, (t - t0) / HOUR);
    const q = (from, to) => Q.run(e, { tags: 'K', from, to, mode: 'bucket', bucket: 'auto', tz: 'Asia/Jakarta', agg: ['increase', 'count'] }).K;
    const J = (x) => cal.parseLocal(x, 'Asia/Jakarta');
    const six = q(J('2026-01-01'), J('2026-07-01') - 1);
    assert.strictEqual(six.bucket, 'month'); assert.strictEqual(six.t.length, 6);
    assert.deepStrictEqual(six.t.map(iso).slice(0, 2), ['2025-12-31T17:00:00.000Z', '2026-01-31T17:00:00.000Z']);
    const one = q(J('2026-03-01'), J('2026-04-01') - 1);
    assert.strictEqual(one.bucket, 'week'); assert.strictEqual(one.t.length, 6, 'March 2026 touches six Monday weeks');
    const week = q(J('2026-03-02'), J('2026-03-09') - 1);
    assert.strictEqual(week.bucket, 'day'); assert.strictEqual(week.t.length, 7);
    const day = q(J('2026-03-02'), J('2026-03-03') - 1);
    assert.strictEqual(day.bucket, 'hour'); assert.strictEqual(day.t.length, 24);
    const year = q(J('2022-01-01'), J('2026-07-01') - 1);
    assert.strictEqual(year.bucket, 'year');
    // only what falls in [from, to] is counted: the data starts at 2026-01-01T00:00Z, the range ends at 2026-06-30T17:00Z (Jakarta 07/01 00:00)
    assert.strictEqual(six.count.reduce((x, y) => x + y, 0), (180 * 24 + 17) * 4);
    assert.strictEqual(Q.run(e, { tags: 'K', from: J('2026-03-02'), to: J('2026-03-09') - 1, mode: 'bucket', bucket: 'auto', minBuckets: 1, tz: 'Asia/Jakarta', agg: ['count'] }).K.bucket, 'week', 'minBuckets 1: one week is enough');
    e.close();
});

ok('times without a zone are read as the zone\'s clock; endExclusive; the first of next month as `to` does not make an extra month', () => {
    const d = tmp(), e = open(d);
    for (let t = Date.UTC(2025, 11, 1); t < Date.UTC(2026, 4, 1); t += HOUR) e.write('H', t, 1);
    const r = Q.run(e, { tags: 'H', from: '2026-01-01', to: '2026-04-01', endExclusive: true, mode: 'bucket', bucket: 'month', tz: 'Asia/Jakarta', agg: ['count'] }).H;
    assert.deepStrictEqual(r.t.map(iso), ['2025-12-31T17:00:00.000Z', '2026-01-31T17:00:00.000Z', '2026-02-28T17:00:00.000Z']);
    assert.deepStrictEqual(r.count, [31 * 24, 28 * 24, 31 * 24], 'the hours of January, February, March of Jakarta');
    const withZone = Q.run(e, { tags: 'H', from: '2026-01-01T00:00:00+07:00', to: '2026-01-02T00:00:00+07:00', endExclusive: true, mode: 'range', agg: ['count'] }).H;
    assert.strictEqual(withZone.count[0], 24, 'an offset in the text is exact');
    e.close();
});

ok('hostile bucket parameters: an unknown unit, a zone, weekStart, minBuckets and a bucket of 0 are refused with their reason', () => {
    const d = tmp(), e = open(d); for (let k = 0; k < 10; k++) e.write('A', Date.now() - k * MIN, k);
    const q = (x) => () => Q.run(e, Object.assign({ tags: 'A', from: '-1h', mode: 'bucket' }, x));
    assert.throws(q({ bucket: 'fortnight' }), /not a duration/);
    assert.throws(q({ bucket: 'month', tz: 'Mars/Olympus' }), /unknown time zone/);
    assert.throws(q({ bucket: 'week', weekStart: 'someday' }), /weekStart/);
    assert.throws(q({ bucket: 'auto', minBuckets: 0 }), /minBuckets/);
    assert.throws(q({ bucket: 0 }), /longer than 0/);
    assert.ok(q({ bucket: 'auto' })().A.t.length > 0);
    e.close();
});

console.log('\n' + passed + ' passed (seed ' + SEED + ')\nALL OK');
