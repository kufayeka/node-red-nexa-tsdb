'use strict';
// delta, increase, integral, twa, states: every answer equals a brute force over the points, through every level of the pyramid
// (raw points, chunk, hour and day summaries), checkpoints, reopens and crashes; counters that reset, plateaus, zeros, gaps;
// the counter policies (reset, tolerance, maxStep, ignoreZero, maxGap); strings and bools as states.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');
const admin = require('../lib/admin');

const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? +process.argv[i + 1] : d; };
const SEED = arg('seed', 77), ROUNDS = arg('rounds', 12);
let s = SEED >>> 0;
const rnd = () => { s = (s + 0x6D2B79F5) >>> 0; let t = s; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
const pick = (a) => a[Math.floor(rnd() * a.length)];
const int = (a, b) => a + Math.floor(rnd() * (b - a + 1));
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-u-'));
const open = (dir, o) => new Engine(dir, Object.assign({ walSync: false, checkpointMs: 1e9, walFlushMs: 1e9, rawDays: 36500, indexDays: 36500 }, o)).open();
const crash = (e) => { e.flushWal(); e._timers.forEach(clearInterval); fs.closeSync(e.walFd); e.segFds.forEach((x) => fs.closeSync(x.fd)); e.closeIdx(); e._unlock(); };
const MIN = 60000, HOUR = 3600000, DAY = 86400000, T0 = Date.UTC(2026, 0, 1);
let passed = 0;
function ok(label, fn) { const t = Date.now(); try { fn(); } catch (e) { console.error('FAILED (seed ' + SEED + '): ' + label); throw e; } passed++; console.log('✔ ' + label + ' (' + (Date.now() - t) + ' ms)'); }
const near = (a, b, what) => {
    if (a === null || b === null || a === undefined || b === undefined) return assert.strictEqual(a, b, what);
    assert.ok(Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(a), Math.abs(b)), what + ': ' + a + ' vs ' + b);
};

// ---- the brute force: the whole series, in memory ---------------------------------------------------------------
const stepOf = (pol, a, b) => {
    const d = b - a;
    if (d >= 0) return d > pol.maxStep ? 0 : d;
    if (-d <= pol.tolerance) return 0;
    return pol.reset === 'restart' && b <= pol.maxStep ? b : 0;
};
function brute(all, from, to, size, origin, o) {
    const pol = Object.assign({ reset: 'restart', tolerance: 0, maxStep: Infinity, ignoreZero: false, maxGap: Infinity }, o.policy || {});
    const pts = pol.ignoreZero ? all.filter((p) => p[1] !== 0) : all;
    const b0 = Math.floor((from - origin) / size), nb = Math.max(1, Math.floor((to - origin) / size) - b0 + 1);
    const B = Array.from({ length: nb }, () => ({ n: 0, inc: 0, iL: 0, iS: 0, cov: 0, last: NaN, first: NaN, base: NaN, counts: new Map(), dur: new Map(), ent: new Map(), changes: 0, edge: false }));
    const idx = (t) => Math.floor((t - origin) / size) - b0;
    for (let i = 0; i < pts.length; i++) {
        const [t, v] = pts[i];
        if (t < from || t > to) continue;
        const b = idx(t), bk = B[b], prev = i ? pts[i - 1] : null;
        if (!bk.n) bk.base = prev ? prev[1] : NaN;
        bk.n++; bk.last = v; if (bk.n === 1) bk.first = v;
        bk.counts.set(v, (bk.counts.get(v) || 0) + 1);
        if (prev) {
            bk.inc += stepOf(pol, prev[1], v);
            if (prev[1] !== v) { bk.ent.set(v, (bk.ent.get(v) || 0) + 1); bk.changes++; }
            const [ta, va] = prev, gap = t - ta;
            if (gap > 0 && gap <= pol.maxGap) {
                for (let k = Math.max(idx(ta), 0); k <= idx(t) && k < nb; k++) {
                    const e0 = origin + (k + b0) * size, e1 = e0 + size, sa = Math.max(ta, e0), se = Math.min(t, e1);
                    if (!(se > sa)) continue;
                    const vs = va + (v - va) * (sa - ta) / gap, ve = va + (v - va) * (se - ta) / gap;
                    B[k].iL += (vs + ve) / 2 * (se - sa); B[k].iS += va * (se - sa); B[k].cov += se - sa; B[k].dur.set(va, (B[k].dur.get(va) || 0) + se - sa); B[k].edge = true;
                }
            }
        }
    }
    return { B, origin: origin + b0 * size };
}

// ---- data -------------------------------------------------------------------------------------------------------
// a kWh meter: climbs, plateaus (no consumption), resets to 0 or to a small value, stretches with no data
function meter(n) {
    const pts = []; let t = T0 + int(0, HOUR), v = int(0, 5000) / 10;
    for (let i = 0; i < n; i++) {
        t += rnd() < 0.01 ? int(HOUR, 6 * HOUR) : int(1000, 90000);
        const w = rnd();
        if (w < 0.01) v = pick([0, 0, int(1, 30) / 10]);                // a reset
        else if (w < 0.3) v += 0;                                      // a plateau: the meter did not move
        else v += int(1, 200) / 100;
        pts.push([t, Math.round(v * 100) / 100]);
    }
    return pts;
}
// kW: fluctuates, often 0 (the machine is off)
function power(n) {
    const pts = []; let t = T0 + int(0, HOUR);
    for (let i = 0; i < n; i++) { t += rnd() < 0.01 ? int(HOUR, 4 * HOUR) : int(1000, 60000); pts.push([t, rnd() < 0.2 ? 0 : Math.round(rnd() * 5000) / 100]); }
    return pts;
}
// a state: held for a while, then another
function states(n, names) {
    const pts = []; let t = T0 + int(0, HOUR), cur = pick(names);
    for (let i = 0; i < n; i++) { t += rnd() < 0.01 ? int(HOUR, 3 * HOUR) : int(1000, 60000); if (rnd() < 0.08) cur = pick(names); pts.push([t, cur]); }
    return pts;
}

function load(d, o, series) {                                         // series: { name: pts }; written in time order, checkpoints / reopens / crashes between
    let e = open(d, o);
    const order = []; Object.keys(series).forEach((n) => series[n].forEach((p) => order.push([p[0], n, p[1]])));
    order.sort((a, b) => a[0] - b[0] || (a[1] < b[1] ? -1 : 1));
    const cp = int(150, 3000); let w = 0;
    for (const [t, n, v] of order) {
        assert.ok(e.write(n, t, v));
        if (++w % cp === 0) { const x = rnd(); if (x < 0.4) e.checkpoint(); else if (x < 0.7) { e.close(); e = open(d, o); } else { crash(e); e = open(d, o); } }
    }
    return e;
}

function compareNumeric(e, name, pts, from, to, size, aggs, qopt, bopt) {
    const origin = Math.floor(from / size) * size;
    const q = Object.assign({ tags: name, from, to, mode: 'bucket', bucket: size, agg: aggs }, qopt);
    const r = Q.run(e, q)[name];
    const { B, origin: o0 } = brute(pts, from, to, size, origin, bopt || {});
    const per = 3600000, method = (qopt && qopt.method) || 'linear', inner = qopt && qopt.anchor === 'inner', rev = qopt && qopt.reverse;
    let row = 0;
    for (let k = 0; k < B.length; k++) {
        const b = B[k];
        if (!b.n && !(b.edge && (aggs.includes('integral') || aggs.includes('twa')))) continue;     // a bucket inside a gap exists for the integral only
        assert.strictEqual(r.t[row], o0 + k * size, 'bucket time ' + k);
        const at = (a) => r[a][row];
        if (aggs.includes('delta')) {
            if (!b.n) assert.strictEqual(at('delta'), null, 'delta of a bucket with no points');
            else { const base = !inner && b.base === b.base ? b.base : b.first; near(at('delta'), (rev ? -1 : 1) * (b.last - base), 'delta bucket ' + k); }
        }
        if (aggs.includes('increase')) { if (!b.n) assert.strictEqual(at('increase'), null); else near(at('increase'), b.inc, 'increase bucket ' + k); }
        if (aggs.includes('integral')) near(at('integral'), (method === 'step' ? b.iS : b.iL) / per, 'integral bucket ' + k);
        if (aggs.includes('twa') && b.cov > 0) near(at('twa'), (method === 'step' ? b.iS : b.iL) / b.cov, 'twa bucket ' + k);
        row++;
    }
    assert.strictEqual(r.t.length, row, 'the same buckets');
}

ok('a kWh meter that resets: increase and delta per bucket (levels: raw, chunk, hour, day), equal to a brute force', () => {
    for (let round = 0; round < ROUNDS; round++) {
        const d = tmp(), pts = meter(int(200, 7000)), o = { chunkPoints: pick([32, 128, 1024]), segmentMs: pick([HOUR, HOUR, DAY / 4]) };
        const e = load(d, o, { M: pts }), first = pts[0][0], last = pts[pts.length - 1][0];
        for (let k = 0; k < 8; k++) {
            const size = pick([MIN, 10 * MIN, HOUR, 6 * HOUR, DAY]), from = int(first - HOUR, last), to = Math.min(last + HOUR, from + pick([HOUR, 5 * HOUR, DAY, 6 * DAY]));
            compareNumeric(e, 'M', pts, from, to, size, ['increase', 'delta', 'count'], {});
            compareNumeric(e, 'M', pts, from, to, size, ['delta'], { anchor: 'inner' });
            compareNumeric(e, 'M', pts, from, to, size, ['delta'], { reverse: true });
        }
        assert.strictEqual(admin.run(e, { op: 'verify' }).ok, true);
        e.close();
    }
});

ok('kW: integral (kWh with per "h"), linear and step, and the time-weighted average, equal to a brute force; a bucket inside a gap gets its share', () => {
    for (let round = 0; round < ROUNDS; round++) {
        const d = tmp(), pts = power(int(200, 7000)), o = { chunkPoints: pick([32, 128, 1024]), segmentMs: pick([HOUR, HOUR, DAY / 4]) };
        const e = load(d, o, { P: pts }), first = pts[0][0], last = pts[pts.length - 1][0];
        for (let k = 0; k < 8; k++) {
            const size = pick([MIN, 10 * MIN, HOUR, 6 * HOUR, DAY]), from = int(first - HOUR, last), to = Math.min(last + HOUR, from + pick([HOUR, 5 * HOUR, DAY, 6 * DAY]));
            compareNumeric(e, 'P', pts, from, to, size, ['integral', 'twa'], {});
            compareNumeric(e, 'P', pts, from, to, size, ['integral', 'twa'], { method: 'step' });
        }
        e.close();
    }
});

ok('the integral of a constant 10 kW over 3 hours is 30 kWh, however the range is cut into buckets', () => {
    const d = tmp(), e = open(d);
    for (let k = 0; k <= 180; k++) e.write('K', T0 + k * MIN, 10);
    for (const size of [MIN, 7 * MIN, 30 * MIN, HOUR, DAY]) {
        const r = Q.run(e, { tags: 'K', from: T0, to: T0 + 180 * MIN, mode: 'bucket', bucket: size, agg: ['integral'], per: 'h' }).K;
        near(r.integral.reduce((x, y) => x + y, 0), 30, 'sum of the buckets of ' + size / MIN + ' min');
    }
    const one = Q.run(e, { tags: 'K', from: T0, to: T0 + 180 * MIN, mode: 'range', agg: ['integral', 'twa', 'avg'] }).K;
    near(one.integral[0], 30, 'range'); near(one.twa[0], 10, 'twa'); assert.strictEqual(one.t.length, 1);
    e.close();
});

ok('mode range: the delta and the increase between two times, one row; the buckets of the same range add up to it', () => {
    const d = tmp(), pts = meter(5000), e = load(d, { chunkPoints: 128 }, { M: pts }), first = pts[0][0], last = pts[pts.length - 1][0];
    for (let k = 0; k < 20; k++) {
        const from = int(first, last - 2 * HOUR), to = int(from + HOUR, last);
        const one = Q.run(e, { tags: 'M', from, to, mode: 'range', agg: ['delta', 'increase'] }).M;
        if (!pts.some((p) => p[0] >= from && p[0] <= to)) { assert.strictEqual(one.t.length, 0, 'no point in the range: no row'); continue; }
        assert.strictEqual(one.t.length, 1); assert.strictEqual(one.t[0], from);
        const parts = Q.run(e, { tags: 'M', from, to, mode: 'bucket', bucket: pick([10 * MIN, HOUR]), agg: ['delta', 'increase'] }).M;
        near(parts.increase.reduce((x, y) => x + y, 0), one.increase[0], 'the increases of the buckets add up');
        const dsum = parts.delta.reduce((x, y) => x + y, 0), dd = one.delta[0];
        near(dsum, dd, 'the deltas of consecutive buckets add up to the delta of the whole range (bridged)');
    }
    e.close();
});

ok('a meter that resets: the increase counts the new count after the reset; a plateau is 0; a reset to 0 is not a negative', () => {
    const d = tmp(), e = open(d), v = [100, 100, 101, 105, 5, 5, 8, 0, 0, 2, 2];
    v.forEach((x, k) => e.write('R', T0 + k * MIN, x));
    const r = Q.run(e, { tags: 'R', from: T0, to: T0 + 10 * MIN, mode: 'range', agg: ['increase', 'delta', 'first', 'last'] }).R;
    // steps: 0 1 4 | reset: +5 | 0 3 | reset: +0 | 0 2 0
    assert.strictEqual(r.increase[0], 0 + 1 + 4 + 5 + 0 + 3 + 0 + 0 + 2 + 0);
    assert.strictEqual(r.delta[0], 2 - 100 + 0, 'delta is last - first: the value change, a reset makes it negative (use increase for consumption)');
    const ig = Q.run(e, { tags: 'R', from: T0, to: T0 + 10 * MIN, mode: 'range', agg: ['increase'], reset: 'ignore' }).R;
    assert.strictEqual(ig.increase[0], 0 + 1 + 4 + 0 + 0 + 3 + 0 + 0 + 2 + 0, 'reset: "ignore" drops the step of a reset');
    e.close();
});

ok('a meter that reads 0 for a moment and comes back: ignoreZero keeps it from being counted as consumption; maxStep and tolerance do the same for a jump and a jitter', () => {
    const run = (name, vals, extra) => {
        const d = tmp(), e = open(d); vals.forEach((x, k) => e.write(name, T0 + k * MIN, x));
        const r = Q.run(e, Object.assign({ tags: name, from: T0, to: T0 + vals.length * MIN, mode: 'range', agg: ['increase'] }, extra)).increase || Q.run(e, Object.assign({ tags: name, from: T0, to: T0 + vals.length * MIN, mode: 'range', agg: ['increase'] }, extra))[name].increase;
        e.close(); return Array.isArray(r) ? r[0] : r;
    };
    const val = (name, vals, extra) => { const d = tmp(), e = open(d); vals.forEach((x, k) => e.write(name, T0 + k * MIN, x)); const r = Q.run(e, Object.assign({ tags: name, from: T0, to: T0 + vals.length * MIN, mode: 'range', agg: ['increase'] }, extra))[name].increase[0]; e.close(); return r; };
    void run;
    const zero = [1000, 1001, 0, 1002];
    near(val('A', zero), 1 + 0 + 1002, 'as it is: the way back from the 0 is counted (the default cannot tell a reset from a glitch)');
    near(val('B', zero, { ignoreZero: true }), 1 + 1, 'ignoreZero: the 0 is a missing reading');
    const jitter = [100, 100.5, 100.49, 101];
    near(val('C', jitter), 0.5 + 100.49 + 0.51, 'a jitter down is a reset');
    near(val('D', jitter, { tolerance: 0.05 }), 0.5 + 0 + 0.51, 'a drop within the tolerance is noise');
    const jump = [10, 11, 5000, 5001];
    near(val('E', jump), 1 + 4989 + 1);
    near(val('F', jump, { maxStep: 100 }), 1 + 0 + 1, 'a step past maxStep is not counted');
});

ok('counter policies on random meters (reset, tolerance, maxStep, ignoreZero, maxGap) equal a brute force; they read the raw points', () => {
    for (let round = 0; round < Math.max(4, ROUNDS / 2); round++) {
        const d = tmp(), pts = meter(int(300, 4000)), e = load(d, { chunkPoints: pick([32, 256]) }, { M: pts }), first = pts[0][0], last = pts[pts.length - 1][0];
        const pol = { reset: pick(['restart', 'ignore']), tolerance: pick([0, 0.05, 1]), maxStep: pick([Infinity, 50, 5]), ignoreZero: pick([false, true]) };
        const from = int(first, last - HOUR), to = Math.min(last, from + pick([HOUR, 6 * HOUR, 3 * DAY])), size = pick([10 * MIN, HOUR, DAY]);
        compareNumeric(e, 'M', pts, from, to, size, ['increase'], pol, { policy: pol });
        const gap = { maxGap: pick([2 * MIN, 30 * MIN]) };
        const g = power(int(300, 3000)), e2 = load(tmp(), {}, { P: g });
        compareNumeric(e2, 'P', g, g[0][0], Math.min(g[g.length - 1][0], g[0][0] + 2 * DAY), pick([HOUR, 6 * HOUR]), ['integral'], { maxGap: gap.maxGap }, { policy: { maxGap: gap.maxGap } });
        e.close(); e2.close();
    }
});

ok('a state (string): occurrences, entries, duration and the histograms equal a brute force, over constant and mixed chunks', () => {
    const names = ['Run', 'Idle', 'Stop', 'Fault'];
    for (let round = 0; round < ROUNDS; round++) {
        const d = tmp(), pts = states(int(300, 6000), names), e = load(d, { chunkPoints: pick([16, 64, 256]), segmentMs: pick([HOUR, DAY / 4]) }, { S: pts }), first = pts[0][0], last = pts[pts.length - 1][0];
        for (let k = 0; k < 8; k++) {
            const size = pick([10 * MIN, HOUR, 6 * HOUR, DAY]), from = int(first - HOUR, last), to = Math.min(last + HOUR, from + pick([HOUR, 6 * HOUR, 3 * DAY])), origin = Math.floor(from / size) * size, st = pick(names);
            const r = Q.run(e, { tags: 'S', from, to, mode: 'bucket', bucket: size, agg: ['occurrences', 'entries', 'duration', 'changes', 'counts', 'durations'], value: st }).S;
            const ids = new Map(); const dict = e.byName.get('S').dict; names.forEach((n) => ids.set(dict.get(n), n));
            const num = pts.map((p) => [p[0], dict.get(p[1])]);
            const { B, origin: o0 } = brute(num, from, to, size, origin, {});
            const target = dict.get(st); let row = 0;
            for (let j = 0; j < B.length; j++) {
                const b = B[j]; if (!b.n && !b.edge) continue;
                assert.strictEqual(r.t[row], o0 + j * size);
                assert.strictEqual(r.occurrences[row], b.counts.get(target) || 0, 'occurrences ' + st);
                assert.strictEqual(r.entries[row], b.ent.get(target) || 0, 'entries ' + st);
                assert.strictEqual(r.duration[row], b.dur.get(target) || 0, 'duration ' + st);
                assert.strictEqual(r.changes[row], b.changes, 'changes');
                const want = {}; for (const [id, c] of b.counts) want[ids.get(id)] = c;
                assert.deepStrictEqual(r.counts[row], want, 'counts');
                const wd = {}; for (const [id, c] of b.dur) wd[ids.get(id)] = c;
                assert.deepStrictEqual(r.durations[row], wd, 'durations');
                row++;
            }
            assert.strictEqual(r.t.length, row);
        }
        e.close();
    }
});

ok('a bool: the time it was true (running hours) and how many times it went true', () => {
    const d = tmp(), e = open(d);
    // true for 10, 20, 30 minutes, with false in between
    const spans = [[0, true], [10, false], [20, true], [40, false], [50, true], [80, false]];
    spans.forEach(([m, v]) => e.write('B', T0 + m * MIN, v));
    const r = Q.run(e, { tags: 'B', from: T0, to: T0 + 80 * MIN, mode: 'range', agg: ['duration', 'entries', 'occurrences', 'sum'], value: true }).B;
    assert.strictEqual(r.duration[0], 60 * MIN, 'ran for 10 + 20 + 30 minutes');
    assert.strictEqual(r.entries[0], 2, 'went true twice after the first reading');
    assert.strictEqual(r.occurrences[0], 3, 'three readings of true');
    e.close();
});

ok('hostile parameters: an unknown aggregate, a missing value, a bad method / anchor / per / reset are refused with their reason', () => {
    const d = tmp(), e = open(d); for (let k = 0; k < 10; k++) { e.write('A', T0 + k * MIN, k); e.write('S', T0 + k * MIN, 'x' + (k % 2)); }
    const q = (x) => () => Q.run(e, Object.assign({ tags: 'A', from: T0, to: T0 + 10 * MIN, mode: 'bucket', bucket: '1h' }, x));
    assert.throws(q({ agg: ['avg', 'nope'] }), /unknown aggregate "nope"/);
    assert.throws(q({ agg: ['occurrences'] }), /needs a value/);
    assert.throws(q({ agg: ['integral'], method: 'cubic' }), /method/);
    assert.throws(q({ agg: ['delta'], anchor: 'middle' }), /anchor/);
    assert.throws(q({ agg: ['integral'], per: 'year' }), /per must be/);
    assert.throws(q({ agg: ['increase'], reset: 'maybe' }), /reset/);
    assert.throws(q({ agg: ['increase'], tolerance: -1 }), /tolerance/);
    const num = Q.run(e, { tags: 'S', from: T0, to: T0 + 10 * MIN, mode: 'range', agg: ['delta', 'increase', 'integral', 'avg'] }).S;
    assert.deepStrictEqual([num.delta[0], num.increase[0], num.integral[0], num.avg[0]], [null, null, null, null], 'a string has no delta, increase, integral or average');
    e.close();
});

ok('the first bucket starts from the point before the range, also when it is in an older chunk, hour or day summary (or the open chunk)', () => {
    const d = tmp(), pts = meter(6000), e = load(d, { chunkPoints: 64 }, { M: pts }), first = pts[0][0], last = pts[pts.length - 1][0];
    for (let k = 0; k < 60; k++) {
        const t = int(first, last), p = e.pointBefore(e.byName.get('M'), t);
        let want = null; for (const x of pts) if (x[0] < t) want = x; else break;
        assert.deepStrictEqual(p && [p.t, p.v], want, 'the newest point before ' + t);
    }
    assert.strictEqual(e.pointBefore(e.byName.get('M'), first), null);
    e.close();
});

console.log('\n' + passed + ' passed (seed ' + SEED + ')\nALL OK');
