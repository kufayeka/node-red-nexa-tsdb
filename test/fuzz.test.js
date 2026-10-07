'use strict';
// A: input and contract. Whatever is thrown at write() and query(), the engine answers or refuses with a reason (never an internal
// TypeError / RangeError, never a hang), stays consistent (verify clean, reopens), and every answer equals a brute-force model.
// Seeded: a failure prints its seed (node test/fuzz.test.js --seed N --rounds N).
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');
const admin = require('../lib/admin');

const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? +process.argv[i + 1] : d; };
const SEED = arg('seed', 1234), ROUNDS = arg('rounds', 40);
let s = SEED >>> 0;
const rnd = () => { s = (s + 0x6D2B79F5) >>> 0; let t = s; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
const pick = (a) => a[Math.floor(rnd() * a.length)];
const int = (a, b) => a + Math.floor(rnd() * (b - a + 1));
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-f-'));
const open = (dir, o) => new Engine(dir, Object.assign({ walSync: false, checkpointMs: 1e9, walFlushMs: 1e9, rawDays: 36500, indexDays: 36500 }, o)).open();
const HOUR = 3600000, DAY = 86400000;
let passed = 0;
function ok(label, fn) { const t = Date.now(); try { fn(); } catch (e) { console.error('FAILED (seed ' + SEED + '): ' + label); throw e; } passed++; console.log('✔ ' + label + ' (' + (Date.now() - t) + ' ms)'); }
// an internal error is a bug; a refusal with a reason is the contract
const internal = (e) => e instanceof TypeError || e instanceof RangeError || e instanceof ReferenceError || /undefined|is not a function|Invalid typed array|Invalid array length|Invalid time value|Cannot read prop/.test(String(e && e.message));
const crash = (e) => { e.flushWal(); e._timers.forEach(clearInterval); fs.closeSync(e.walFd); e.segFds.forEach((x) => fs.closeSync(x.fd)); e.closeIdx(); e._unlock(); };

ok('write() with hostile values never throws; the database reopens and verifies clean', () => {
    const d = tmp(); let e = open(d);
    const names = ['A', 'a', '', ' ', 'x'.repeat(5000), 'Ünï/çødé 温度 ☃', 'a.b.c', '*', 'a*b', '../../etc/passwd', 'with\nnewline', '{"id":1}', '__proto__', 'constructor', 'toString', String.fromCharCode(0)];
    const times = [0, 1, -1, 1.5, 1e12, 1.7e12, Date.now(), Date.now() + DAY * 2, Date.now() + 1e13, NaN, Infinity, -Infinity, '2026-01-01', null, undefined, {}, [], true, 1e300, 2 ** 53, 5e-324];
    const values = [0, -0, 1, -1, 1e308, -1e308, 5e-324, NaN, Infinity, -Infinity, 0.1, 1 / 3, true, false, '', 'x', 'a'.repeat(100000), '\u0000', '温度', null, undefined, {}, [], 12345678901234567890n];
    let accepted = 0, refused = 0;
    for (let i = 0; i < 20000; i++) {
        let r;
        try { r = e.write(pick(names), i % 3 ? pick(times) : Date.now() - 1000 + i, pick(values)); } catch (x) { assert.fail('write threw: ' + x.stack); }
        if (r) accepted++; else refused++;
    }
    assert.ok(accepted > 0 && refused > 0);
    const types = new Map(e.tagList().map((t) => [t.name, t.type]));
    e.checkpoint();
    const v = admin.run(e, { op: 'verify' }); assert.strictEqual(v.ok, true, JSON.stringify(v.problems));
    e.close(); e = open(d);
    assert.deepStrictEqual(new Map(e.tagList().map((t) => [t.name, t.type])), types, 'tags and types survive a reopen');
    assert.strictEqual(admin.run(e, { op: 'verify' }).ok, true);
    e.close();
});

ok('query() with hostile objects answers or refuses with a reason - never an internal error, never a hang', () => {
    const d = tmp(), e = open(d), t0 = Date.now() - HOUR;
    for (let i = 0; i < 3000; i++) { e.write('Num', t0 + i * 1000, i % 50); e.write('Str', t0 + i * 1000, 'm' + (i % 4)); e.write('Bool', t0 + i * 1000, i % 2 === 0); }
    e.checkpoint();
    const odd = [undefined, null, 0, -1, 1, 0.5, NaN, Infinity, -Infinity, 1e300, '', ' ', 'abc', '-1h', 'now', 'now-1h', '-9999y', '2026-13-45', '1h', '0s', '-5m', [], {}, [[]], true, false, 'Num', ['Num', 'Str'], '*', '*N*', ['', null, 5], '\\', '(', '[a-', 'a**b', 'range', 'raw', 'bucket', 'last', 'nope', 12, '1ms', '1y', 'month', 'week', 'day', 'auto', 'quarter', 'year', 'Asia/Jakarta', 'UTC', 'sun', Date.now(), t0, new Date(t0)];
    const keys = ['tags', 'from', 'to', 'mode', 'bucket', 'offset', 'agg', 'fill', 'format', 'limit', 'maxPoints', 'page', 'exact', 'anchor', 'reverse', 'method', 'per', 'reset', 'tolerance', 'maxStep', 'ignoreZero', 'maxGap', 'value'];
    const goodAggs = [['avg'], ['delta'], ['increase', 'delta'], ['integral', 'twa'], ['range', 'min', 'max'], ['occurrences', 'entries', 'duration'], ['counts', 'durations', 'changes'], ['avg', 'integral', 'increase', 'counts']];
    let answered = 0, refused = 0;
    for (let i = 0; i < 4000; i++) {
        const q = {}; for (const k of keys) if (rnd() < 0.55) q[k] = pick(odd);
        for (const k of ['tz', 'weekStart', 'minBuckets', 'endExclusive']) if (rnd() < 0.07) q[k] = pick(odd);
        if (rnd() < 0.2) { q.bucket = pick(['auto', 'month', 'week', 'day', 'hour', 'quarter', 'year', 'minute']); if (rnd() < 0.5) q.tz = pick(['Asia/Jakarta', 'America/New_York', 'UTC', 'Asia/Kolkata']); }
        if (rnd() < 0.7) q.tags = pick(['Num', 'Str', 'Bool', ['Num', 'Str', 'Bool'], 'N*', '*']);
        if (rnd() < 0.5) { q.agg = pick(goodAggs); if (rnd() < 0.7) q.mode = pick(['bucket', 'range']); if (rnd() < 0.6) q.value = pick(['m1', 'm9', true, false, 3, 'x']); }
        const t = Date.now(), show = () => JSON.stringify(q, (k, v) => (typeof v === 'number' && !Number.isFinite(v) ? String(v) : v));
        try { const r = Q.run(e, q); answered++; JSON.stringify(r); } catch (x) {
            refused++;
            if (internal(x)) assert.fail('internal error for ' + show() + ': ' + x.message);
        }
        assert.ok(Date.now() - t < 5000, 'a query must not hang: ' + show());
    }
    assert.ok(answered > 100 && refused > 100, answered + ' answered, ' + refused + ' refused');
    e.close();
});

// the model: every point kept in memory per tag; answers computed the slow way
function model(n, stepMax) {
    const pts = []; let t = Date.UTC(2026, 0, 1) + int(0, DAY);
    for (let i = 0; i < n; i++) { t += rnd() < 0.02 ? int(HOUR, 5 * HOUR) : int(1, stepMax); pts.push([t, Math.round(rnd() * 20000) / 100]); }
    return pts;
}
const inRange = (pts, a, b) => pts.filter(([t]) => t >= a && t <= b);

ok('random data, random checkpoints / reopens / crashes: raw and bucket (every basic aggregate) equal a brute-force model', () => {
    for (let round = 0; round < ROUNDS; round++) {
        const d = tmp(), tags = int(1, 4), data = {}, cp = int(200, 4000);
        const o = { chunkPoints: pick([64, 256, 1024]), segmentMs: pick([HOUR, HOUR, DAY / 4]) };
        let e = open(d, o);
        for (let k = 0; k < tags; k++) data['T' + k] = model(int(1, 6000), pick([50, 1000, 30000]));
        const order = []; Object.keys(data).forEach((n) => data[n].forEach((p) => order.push([p[0], n, p[1]])));
        order.sort((a, b) => a[0] - b[0] || (a[1] < b[1] ? -1 : 1));
        let wrote = 0;
        for (const [t, n, v] of order) {
            assert.ok(e.write(n, t, v), 'write ' + n + ' ' + t);
            if (++wrote % cp === 0) { const w = rnd(); if (w < 0.4) e.checkpoint(); else if (w < 0.7) { e.close(); e = open(d, o); } else { crash(e); e = open(d, o); } }
        }
        for (const name of Object.keys(data)) {
            const pts = data[name], first = pts[0][0], last = pts[pts.length - 1][0];
            for (let k = 0; k < 6; k++) {
                const a = rnd() < 0.3 ? first - int(0, HOUR) : int(first, last), b = Math.min(last + int(0, HOUR), a + pick([HOUR, 3 * HOUR, DAY, 9 * DAY])), want = inRange(pts, a, b);
                const raw = Q.run(e, { tags: name, from: a, to: b, mode: 'raw' })[name];
                const gotRaw = raw ? [Array.from(raw.t), Array.from(raw.v)] : [[], []];
                assert.deepStrictEqual(gotRaw, [want.map((p) => p[0]), want.map((p) => p[1])], 'raw ' + name + ' round ' + round);
                const size = pick([1000, 60000, HOUR, 6 * HOUR, DAY]), origin = Math.floor(a / size) * size;
                const bk = Q.run(e, { tags: name, from: a, to: b, mode: 'bucket', bucket: size + 'ms', agg: ['avg', 'min', 'max', 'sum', 'count', 'first', 'last'] })[name];
                const by = new Map(); for (const p of want) { const i = Math.floor((p[0] - origin) / size); (by.get(i) || by.set(i, []).get(i)).push(p); }
                const ids = [...by.keys()].sort((x, y) => x - y);
                assert.deepStrictEqual(bk.t, ids.map((i) => origin + i * size), 'bucket times ' + name + ' size ' + size + ' round ' + round);
                ids.forEach((id, j) => {
                    const g = by.get(id), vs = g.map((p) => p[1]), sum = vs.reduce((x, y) => x + y, 0);
                    assert.strictEqual(bk.count[j], g.length, 'count'); assert.strictEqual(bk.first[j], g[0][1], 'first'); assert.strictEqual(bk.last[j], g[g.length - 1][1], 'last');
                    assert.strictEqual(bk.min[j], Math.min(...vs), 'min'); assert.strictEqual(bk.max[j], Math.max(...vs), 'max');
                    assert.ok(Math.abs(bk.sum[j] - sum) <= 1e-6 * Math.max(1, Math.abs(sum)), 'sum'); assert.ok(Math.abs(bk.avg[j] - sum / g.length) <= 1e-6 * Math.max(1, Math.abs(sum / g.length)), 'avg');
                });
            }
            const lastQ = Q.run(e, { tags: name, mode: 'last' })[name];
            assert.deepStrictEqual([lastQ.t[0], lastQ.v[0]], [last, pts[pts.length - 1][1]], 'last');
        }
        assert.strictEqual(admin.run(e, { op: 'verify' }).ok, true, 'verify round ' + round);
        e.close();
    }
});

ok('clock jumps: a step back, a step forward, a duplicate burst - nothing accepted out of order, the answer equals the accepted points', () => {
    const d = tmp(), e = open(d), acc = [], now = Date.now(); let t = now - 6 * HOUR;
    for (let i = 0; i < 30000; i++) {
        const w = rnd();
        if (w < 0.01) t -= int(1000, 3 * HOUR);                 // the clock steps back
        else if (w < 0.02) t += int(HOUR, 5 * HOUR);            // forward (past the clock: refused)
        else if (w >= 0.1) t += int(1, 2000);                   // (0.02 - 0.1: the same time again, a replace)
        const v = i % 97, r = e.write('C', t, v), last = acc.length ? acc[acc.length - 1][0] : -Infinity;
        if (r) { assert.ok(t >= last, 'accepted out of order'); if (t === last) acc[acc.length - 1][1] = v; else acc.push([t, v]); }
        else assert.ok(t < last || t > now + DAY || (t === last), 'refused for no reason: ' + t + ' last ' + last);
        if (i % 7000 === 6999) e.checkpoint();
    }
    const raw = Q.run(e, { tags: 'C', from: 0, to: now + 10 * DAY, mode: 'raw' }).C;
    assert.deepStrictEqual([Array.from(raw.t), Array.from(raw.v)], [acc.map((p) => p[0]), acc.map((p) => p[1])]);
    e.close();
});

console.log('\n' + passed + ' passed (seed ' + SEED + ')\nALL OK');
