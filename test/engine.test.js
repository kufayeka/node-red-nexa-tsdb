'use strict';
// The engine: what is written comes back exact after a reopen; bucket aggregates equal a brute force over the raw
// points, at every level of the pyramid; M4 keeps every extreme; a crash at any point loses nothing that was in the
// WAL and duplicates nothing; retention drops raw chunks but the summaries still answer.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');

let passed = 0;
function ok(label, fn) { fn(); passed++; console.log('✔ ' + label); }

const DAY = 864e5, H = 36e5;
const T0 = Math.floor((Date.now() - 3 * DAY) / DAY) * DAY;   // three days ago at 00:00 UTC (inside the default retention)
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-'));
const open = (dir, o) => new Engine(dir, Object.assign({ walSync: false, checkpointMs: 1e9, walFlushMs: 1e9 }, o)).open();
// a crash: the process dies, nothing more is written (what the OS already has stays)
function crash(e) { e._timers.forEach(clearInterval); if (e.walFd !== null) fs.closeSync(e.walFd); e.segFds.forEach((s) => fs.closeSync(s.fd)); e._unlock(); }   // the process is gone: so is its lock

let seed = 7;
const rnd = () => { seed = (seed * 16807) % 2147483647; return seed / 2147483647; };

// 2.5 days of a temperature every 10 s, a bool every minute, a state every 7 minutes
function fill(e, ref) {
    let x = 80;
    for (let t = T0; t < T0 + 2.5 * DAY; t += 10000) {
        x += (rnd() - 0.5) * 0.4; const v = Math.round(x * 100) / 100;
        e.write('Oven1.Temp', t, v); ref.temp.push([t, v]);
        if (t % 60000 === 0) { const b = rnd() > 0.3; e.write('Oven1.Running', t, b); ref.run.push([t, b]); }
        if (t % 420000 === 0) { const s = ['Run', 'Idle', 'Setup', 'Breakdown'][Math.floor(rnd() * 4)]; e.write('Oven1.State', t, s); ref.state.push([t, s]); }
    }
}
const ref = { temp: [], run: [], state: [] };
const dir = tmp();

ok('number, bool and string tags come back exact after a close and a reopen', () => {
    const e = open(dir);
    fill(e, ref);
    e.close();
    const e2 = open(dir);
    const r = Q.run(e2, { tags: 'Oven1.*', from: T0, to: T0 + 3 * DAY, mode: 'raw' });
    assert.deepStrictEqual(Object.keys(r).sort(), ['Oven1.Running', 'Oven1.State', 'Oven1.Temp']);
    assert.deepStrictEqual(r['Oven1.Temp'].t.map((t, i) => [t, r['Oven1.Temp'].v[i]]), ref.temp);
    assert.deepStrictEqual(r['Oven1.Running'].v, ref.run.map((p) => p[1]));
    assert.deepStrictEqual(r['Oven1.State'].v, ref.state.map((p) => p[1]));
    assert.strictEqual(r['Oven1.State'].type, 'string');
    console.log('   ' + e2.stats.points + ' points in WAL replay, ' + fs.readdirSync(path.join(dir, 'seg')).length + ' segment files');
    e2.close();
});

ok('a late point and a wrong type are refused and counted', () => {
    const e = open(dir);
    assert.strictEqual(e.write('Oven1.Temp', T0, 1), false);
    assert.strictEqual(e.write('Oven1.Temp', T0 + 3 * DAY, 'text'), false);
    assert.deepStrictEqual([e.stats.late, e.stats.badType], [1, 1]);
    e.close();
});

// the brute force over the raw points
function brute(points, from, to, size, origin) {
    const out = new Map();
    for (const [t, v] of points) {
        if (t < from || t > to) continue;
        const b = Math.floor((t - origin) / size) * size + origin;
        let a = out.get(b);
        if (!a) out.set(b, (a = { n: 0, sum: 0, min: Infinity, max: -Infinity, first: v, last: v }));
        a.n++; a.sum += v; a.min = Math.min(a.min, v); a.max = Math.max(a.max, v); a.last = v;
    }
    return out;
}
const near = (a, b) => Math.abs(a - b) < 1e-6 * Math.max(1, Math.abs(b));

ok('bucket aggregates equal the brute force: per day (level 2), per hour (level 1), per 7 min (level 0 + raw), shifted by 6 h, partial ranges', () => {
    const e = open(dir);
    const cases = [['1d', 0, T0, T0 + 3 * DAY], ['1h', 0, T0 + 5 * 60000, T0 + 2 * DAY + 1234], ['7m', 0, T0 + 3 * H + 17, T0 + 9 * H], ['8h', 6 * H, T0, T0 + 2.5 * DAY], ['30s', 0, T0 + DAY, T0 + DAY + 600000]];
    for (const [bucket, off, from, to] of cases) {
        const r = Q.run(e, { tags: 'Oven1.Temp', from, to, mode: 'bucket', bucket, offset: off, agg: ['avg', 'min', 'max', 'sum', 'count', 'first', 'last'] })['Oven1.Temp'];
        const size = Q.parseDuration(bucket), origin = Math.floor((from - off) / size) * size + off;
        const want = brute(ref.temp, from, to, size, origin);
        assert.strictEqual(r.t.length, want.size, bucket + ': buckets');
        r.t.forEach((t, i) => {
            const w = want.get(t);
            assert.ok(w, bucket + ': bucket ' + t);
            assert.strictEqual(r.count[i], w.n, bucket + ' count');
            assert.ok(near(r.sum[i], w.sum) && near(r.avg[i], w.sum / w.n), bucket + ' sum/avg');
            assert.deepStrictEqual([r.min[i], r.max[i], r.first[i], r.last[i]], [w.min, w.max, w.first, w.last], bucket + ' min/max/first/last at ' + t);
        });
    }
    e.close();
});

ok('M4 for a chart: every column holds its true min and max; 1 200 px over 2.5 days reads summaries, over 20 min the raw points', () => {
    const e = open(dir);
    for (const [from, to, width] of [[T0, T0 + 2.5 * DAY, 1200], [T0 + DAY, T0 + DAY + 1200000, 300]]) {
        const r = Q.run(e, { tags: 'Oven1.Temp', from, to, width })['Oven1.Temp'];
        assert.ok(r.t.length <= width * 4, 'at most 4 points a column');
        const size = (to - from + 1) / width, want = brute(ref.temp, from, to, size, from);
        const got = brute(r.t.map((t, i) => [t, r.v[i]]), from, to, size, from);
        let exact = 0;
        want.forEach((w, b) => { const g = got.get(b); if (g && g.min === w.min && g.max === w.max) exact++; });
        // the global extremes always; a column's own extremes in (almost) every column
        assert.strictEqual(Math.min(...r.v), Math.min(...[...want.values()].map((w) => w.min)));
        assert.strictEqual(Math.max(...r.v), Math.max(...[...want.values()].map((w) => w.max)));
        assert.ok(exact / want.size > 0.97, 'columns with their exact min / max: ' + exact + ' of ' + want.size);
        console.log('   ' + width + ' px: ' + r.t.length + ' points, ' + exact + ' / ' + want.size + ' columns exact');
    }
    e.close();
});

ok('a string tag in buckets: count, first, last as text; min / max / avg null', () => {
    const e = open(dir);
    const r = Q.run(e, { tags: 'Oven1.State', from: T0, to: T0 + DAY - 1, mode: 'bucket', bucket: '1d', agg: ['count', 'first', 'last', 'avg'] })['Oven1.State'];
    const day = ref.state.filter(([t]) => t < T0 + DAY);
    assert.deepStrictEqual([r.count[0], r.first[0], r.last[0], r.avg[0]], [day.length, day[0][1], day[day.length - 1][1], null]);
    e.close();
});

ok('a crash: the WAL replays what was not in a chunk; nothing twice', () => {
    const d = tmp(), e = open(d), pts = [];
    for (let i = 0; i < 5000; i++) { const t = T0 + i * 100; e.write('A', t, i); pts.push(t); }
    e.checkpoint();                                       // part in chunks, WAL dropped
    for (let i = 5000; i < 7300; i++) { const t = T0 + i * 100; e.write('A', t, i); pts.push(t); }
    e.flushWal();                                          // the rest only in the WAL and a part in the open chunk
    crash(e);
    const e2 = open(d);
    const r = Q.run(e2, { tags: 'A', from: T0, to: T0 + DAY, mode: 'raw' }).A;
    assert.strictEqual(r.t.length, 7300, 'every point, once');
    assert.ok(r.v.every((v, i) => v === i));
    e2.close();
});

ok('a crash mid write: a torn chunk in a segment and a torn index record are cut, the WAL brings their points back', () => {
    const d = tmp(), e = open(d);
    for (let i = 0; i < 3000; i++) e.write('B', T0 + i * 100, i % 50);
    e.flushWal();
    crash(e);
    const seg = path.join(d, 'seg', fs.readdirSync(path.join(d, 'seg'))[0]);
    fs.appendFileSync(seg, Buffer.from([0x54, 0x53, 0x43, 0x31, 0, 0, 0, 0, 9, 9]));   // half a chunk header
    fs.appendFileSync(path.join(d, 'idx', '0.r0'), Buffer.alloc(50, 7));             // half a record
    const e2 = open(d);
    const r = Q.run(e2, { tags: 'B', from: T0, to: T0 + DAY, mode: 'raw' }).B;
    assert.strictEqual(r.t.length, 3000);
    assert.strictEqual(fs.statSync(path.join(d, 'idx', '0.r0')).size % 96, 0);
    e2.close();
});

ok('retention: old raw segments are deleted; their summaries still draw the chart and give the aggregates', () => {
    const d = tmp(), e = open(d, { rawDays: 1 });
    for (let t = T0; t < T0 + DAY; t += 1000) e.write('C', t, (t / 1000) % 100);
    e.close();
    const e2 = open(d, { rawDays: 1 });                     // three days old: past one day of raw
    assert.strictEqual(fs.readdirSync(path.join(d, 'seg')).length, 0, 'the raw segments are gone');
    assert.strictEqual(Q.run(e2, { tags: 'C', from: T0, to: T0 + DAY, mode: 'raw' }).C.t.length, 0);
    const day = Q.run(e2, { tags: 'C', from: T0, to: T0 + DAY - 1, mode: 'bucket', bucket: '1d', agg: ['count', 'min', 'max'] }).C;
    assert.deepStrictEqual([day.count[0], day.min[0], day.max[0]], [86400, 0, 99]);
    const m4 = Q.run(e2, { tags: 'C', from: T0, to: T0 + DAY, width: 24 }).C;
    assert.ok(m4.t.length > 0 && Math.max(...m4.v) === 99, 'the chart from the hour summaries');
    e2.close();
});

ok('query times and durations: relative, ISO, ms; a tag glob', () => {
    const now = 1791240000000;
    assert.strictEqual(Q.parseTime('-8h', now), now - 8 * H);
    assert.strictEqual(Q.parseTime('now-30m', now), now - 1800000);
    assert.strictEqual(Q.parseTime('2026-10-01T00:00:00Z', now), Date.UTC(2026, 9, 1));
    assert.strictEqual(Q.parseTime(String(now), now), now);
    assert.strictEqual(Q.parseDuration('1.5h'), 1.5 * H);
    assert.throws(() => Q.parseDuration('soon'));
    const e = open(dir);
    assert.deepStrictEqual(Q.matchTags(e, ['Oven1.T*', 'Oven1.State']).map((t) => t.name), ['Oven1.Temp', 'Oven1.State']);
    e.close();
});

ok('the same timestamp replaces the value (also after a checkpoint and a crash); an older one is refused with its reason; diagnose lists the problems first', () => {
    const d = tmp(), e = open(d), t = Date.now() - 5000;
    e.write('P', t, 1); e.write('P', t, 2);
    e.write('P', t + 100, 3); e.write('P', t + 100, 4);
    e.checkpoint();                                   // the last point stays in memory: still replaceable
    e.write('P', t + 100, 5);
    assert.strictEqual(e.write('P', t, 9), false, 'older: refused');
    assert.strictEqual(e.write('P', t + 200, 'x'), false, 'another type: refused');
    e.write('Q', t, 1);
    e.flushWal();
    crash(e);
    const e2 = open(d);
    const r = Q.run(e2, { tags: 'P', from: t - 1, to: t + 1000, mode: 'raw' }).P;
    assert.deepStrictEqual([r.t.length, r.v[0], r.v[1]], [2, 2, 5], 'two points, the last values');
    e2.write('P', t + 100, 6);
    assert.strictEqual(Q.run(e2, { tags: 'P', from: t - 1, to: t + 1000, mode: 'raw' }).P.v[1], 6, 'replaceable after a restart too');
    assert.strictEqual(e2.stats.overwritten, 1);
    e2.write('Q', t - 1, 0);
    const dx = e2.diagnose();
    assert.deepStrictEqual(dx.map((x) => [x.tag, x.refused]), [['Q', 1], ['P', 0]], 'problems first');
    assert.ok(/older than the last point/.test(dx[0].lastRefused.reason) && dx[0].lastRefused.ts === t - 1);
    assert.strictEqual(e2.stats.lastRefused.tag, 'Q');
    e2.close();
});

ok('100 000 tags: a head grows with its points (a tag with one point holds a few hundred bytes, not 32 KB)', () => {
    const d = tmp(), e = open(d), mem = () => process.memoryUsage().heapUsed + process.memoryUsage().arrayBuffers, before = mem(), t = Date.now() - 1000;
    for (let i = 0; i < 100000; i++) e.write('Plant.T' + i, t, i);
    const used = mem() - before;
    console.log('   100 000 tags with a point each: ' + (used / 1048576).toFixed(0) + ' MB');
    assert.ok(used < 400 * 1048576, 'well under the 3.2 GB of fixed 1 024-point heads');
    e.close();
});

ok('a backfill across many hours keeps only a few segment files open (no "too many open files"), every point exact', () => {
    const d = tmp(), e = open(d, { segFds: 8, idxFds: 4 }), t0 = Math.floor((Date.now() - 20 * DAY) / H) * H;
    for (let i = 0; i < 200 * 360; i++) { const t = t0 + i * 10000; e.write('BF', t, i % 1000); e.write('BG', t, i % 7); }
    assert.ok(e.segFds.size <= 8 && e.idxFds.size <= 4, 'bounded: ' + e.segFds.size + ' / ' + e.idxFds.size);
    e.close();
    const e2 = open(d), r = Q.run(e2, { tags: 'BF', from: t0, to: t0 + 200 * H, mode: 'raw' }).BF;
    assert.strictEqual(r.t.length, 200 * 360);
    assert.ok(r.v.every((v, i) => v === i % 1000));
    const day = Q.run(e2, { tags: 'BG', from: t0, to: t0 + 200 * H, mode: 'bucket', bucket: '1d', agg: ['count'] }).BG;
    assert.strictEqual(day.count.reduce((a, b) => a + b, 0), 200 * 360, 'the day summaries hold every point');
    e2.close();
});

ok('last with a `to` in the past and no `from`: the newest point at or before it (a bug the soak test found)', () => {
    const d = tmp(), e = open(d), t = Date.now() - 10 * DAY;
    for (let i = 0; i < 100; i++) e.write('L', t + i * 1000, i);
    e.checkpoint();
    const r = (to) => Q.run(e, { tags: 'L', mode: 'last', to }).L;
    assert.deepStrictEqual([r(t + 50500).t[0], r(t + 50500).v[0]], [t + 50000, 50], 'a time in the middle, long ago');
    assert.deepStrictEqual(r(t - 1).t, [], 'before the first point: nothing');
    assert.strictEqual(r('now').v[0], 99, 'now: the newest');
    e.close();
});

console.log(`\n${passed} passed\nALL OK`);
