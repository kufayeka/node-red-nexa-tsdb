'use strict';
// Storage rules (memory rings, per-tag keep) and the admin operations: drop tags (wildcards, dry run, the broad-pattern
// confirm), delete a range (aggregates still equal a brute force over what is left, after a reopen too), compact,
// drop all, and a crash in the middle of a committed delete.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');
const admin = require('../lib/admin');

let passed = 0;
function ok(label, fn) { fn(); passed++; console.log('✔ ' + label); }
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-a-'));
const open = (dir, o) => new Engine(dir, Object.assign({ walSync: false, checkpointMs: 1e9, walFlushMs: 1e9 }, o)).open();
function crash(e) { e._timers.forEach(clearInterval); if (e.walFd !== null) fs.closeSync(e.walFd); e.segFds.forEach((s) => fs.closeSync(s.fd)); }
const DAY = 864e5, H = 36e5, NOW = Date.now();
const T0 = Math.floor((NOW - 3 * DAY) / DAY) * DAY;

// what is left, bucketed by brute force
function brute(points, from, to, size, origin) {
    const out = new Map();
    for (const [t, v] of points) {
        if (t < from || t > to) continue;
        const b = Math.floor((t - origin) / size) * size + origin;
        let a = out.get(b);
        if (!a) out.set(b, (a = { n: 0, sum: 0, min: Infinity, max: -Infinity }));
        a.n++; a.sum += v; a.min = Math.min(a.min, v); a.max = Math.max(a.max, v);
    }
    return out;
}
function sameBuckets(e, tag, points, from, to, bucket, label) {
    const r = Q.run(e, { tags: tag, from, to, mode: 'bucket', bucket, agg: ['count', 'sum', 'min', 'max'] })[tag];
    const size = Q.parseDuration(bucket), want = brute(points, from, to, size, Math.floor(from / size) * size);
    assert.strictEqual(r ? r.t.length : 0, want.size, label + ': buckets');
    if (r) r.t.forEach((t, i) => {
        const w = want.get(t);
        assert.ok(w && r.count[i] === w.n && Math.abs(r.sum[i] - w.sum) < 1e-6 && r.min[i] === w.min && r.max[i] === w.max, label + ': bucket ' + new Date(t).toISOString() + ' ' + JSON.stringify([r.count[i], w && w.n]));
    });
}

ok('a memory tag: its last `keep` only, in RAM; nothing on disk; gone after a restart; `max` caps it', () => {
    const d = tmp(), rules = [{ pattern: 'Vib.*', store: 'memory', keep: '10s' }, { pattern: 'Fast', store: 'memory', keep: '1h', max: 100 }];
    let e = open(d, { rules });
    const t1 = NOW - 30000;
    for (let i = 0; i < 300; i++) { e.write('Vib.X', t1 + i * 100, i); e.write('Fast', t1 + i * 100, i); e.write('Disk', t1 + i * 100, i); }
    const r = Q.run(e, { tags: 'Vib.X', from: '-1h', mode: 'raw' }, t1 + 29900)['Vib.X'];
    assert.ok(r.t.length >= 95 && r.t.length <= 101 && r.v[r.v.length - 1] === 299, 'the last 10 s: ' + r.t.length);
    assert.strictEqual(Q.run(e, { tags: 'Fast', from: '-1h', mode: 'raw' }).Fast.t.length, 100, 'capped at max');
    assert.ok(Q.run(e, { tags: 'Vib.X', from: '-1h', width: 50 })['Vib.X'].t.length > 0, 'a chart of it');
    assert.deepStrictEqual(e.tagList().map((t) => [t.name, t.store]), [['Vib.X', 'memory'], ['Fast', 'memory'], ['Disk', 'disk']]);
    e.close();
    assert.ok(!/Vib/.test(fs.readFileSync(path.join(d, 'tags.log'), 'utf8')), 'not in tags.log');
    e = open(d, { rules });
    assert.deepStrictEqual(e.tagList().map((t) => t.name), ['Disk']);
    e.close();
});

ok('a disk tag with keep 1h: a query never returns older points; its files are cut at retention', () => {
    const d = tmp(), e = open(d, { rules: [{ pattern: 'Debug.*', keep: '1h' }] });
    for (let t = NOW - 3 * H; t < NOW; t += 1000) { e.write('Debug.A', t, 1); e.write('Keep.A', t, 1); }
    const n = (tag, mode) => Object.values(Q.run(e, { tags: tag, from: NOW - 4 * H, to: NOW, mode, bucket: '10m', agg: ['count'] }))[0];
    assert.ok(n('Debug.A', 'raw').t.length <= 3601 && n('Debug.A', 'raw').t.length >= 3599, 'raw: the last hour');
    assert.strictEqual(n('Debug.A', 'bucket').count.reduce((a, b) => a + b, 0), n('Debug.A', 'raw').t.length, 'buckets: the same hour');
    assert.strictEqual(n('Keep.A', 'raw').t.length, 3 * 3600, 'another tag keeps all');
    e.close();
});

ok('dropTag: a wildcard, a dry run first; the name can be used again (another type); still dropped after a reopen', () => {
    const d = tmp();
    let e = open(d);
    for (let i = 0; i < 2000; i++) { const t = T0 + i * 1000; e.write('Line1.A', t, i); e.write('Line1.B', t, i); e.write('Line2.A', t, i); }
    const dry = admin.run(e, { op: 'dropTag', tags: 'Line1.*', dryRun: true });
    assert.deepStrictEqual([dry.tags, dry.points, dry.dryRun], [['Line1.A', 'Line1.B'], 4000, true]);
    assert.strictEqual(e.tagList().length, 3, 'a dry run changes nothing');
    const r = admin.run(e, { op: 'dropTag', tags: 'Line1.*' });
    assert.deepStrictEqual(r.tags, ['Line1.A', 'Line1.B']);
    assert.deepStrictEqual(e.tagList().map((t) => t.name), ['Line2.A']);
    assert.ok(!fs.existsSync(path.join(d, 'idx', '0.r0')));
    assert.ok(e.write('Line1.A', T0, 'now a text tag'), 'the name again, another type, an older time');
    e.close();
    e = open(d);
    assert.deepStrictEqual(e.tagList().map((t) => [t.name, t.type]), [['Line2.A', 'number'], ['Line1.A', 'string']]);
    assert.strictEqual(Q.run(e, { tags: 'Line1.A', from: T0 - 1, to: T0 + DAY, mode: 'raw' })['Line1.A'].v[0], 'now a text tag');
    e.close();
});

ok('a broad pattern ("*" or more than 100 tags) needs confirm with its count; dropAll needs "DROP ALL"', () => {
    const d = tmp(), e = open(d);
    for (let i = 0; i < 120; i++) e.write('T' + i, T0, i);
    assert.throws(() => admin.run(e, { op: 'dropTag', tags: 'T*' }), /matches 120 tags: send confirm: 120/);
    assert.throws(() => admin.run(e, { op: 'dropTag', tags: '*', confirm: 119 }), /confirm: 120/);
    assert.strictEqual(admin.run(e, { op: 'dropTag', tags: '*', dryRun: true }).tags.length, 120, 'a dry run needs no confirm');
    assert.strictEqual(admin.run(e, { op: 'dropTag', tags: 'T*', confirm: 120 }).tags.length, 120);
    e.write('X', T0, 1);
    assert.throws(() => admin.run(e, { op: 'dropAll' }), /DROP ALL/);
    assert.deepStrictEqual(admin.run(e, { op: 'dropAll', confirm: 'DROP ALL' }), { op: 'dropAll', tags: 1 });
    assert.strictEqual(e.tagList().length, 0);
    assert.ok(e.write('X', T0, 5) && Q.run(e, { tags: 'X', from: T0, to: T0, mode: 'raw' }).X.v[0] === 5, 'it works again');
    e.close();
});

// 2 days of a tag every 10 s, then a range deleted
const D = tmp();
const pts = [];
{
    const e = open(D);
    let seed = 5; const rnd = () => { seed = (seed * 16807) % 2147483647; return seed / 2147483647; };
    for (let t = T0; t < T0 + 2 * DAY; t += 10000) { const v = Math.round(rnd() * 1000) / 10; e.write('Oven1.Temp', t, v); e.write('Oven2.Temp', t, v); pts.push([t, v]); }
    // and the last 2 hours up to now (the open hour / day buckets)
    for (let t = Math.floor((NOW - 2 * H) / 10000) * 10000; t < NOW; t += 10000) { const v = Math.round(rnd() * 1000) / 10; e.write('Oven1.Temp', t, v); e.write('Oven2.Temp', t, v); pts.push([t, v]); }
    e.close();
}

ok('deleteRange (a wildcard): the points in it are gone; hour and day aggregates equal a brute force over what is left', () => {
    const e = open(D);
    const from = T0 + DAY + 8 * H + 1234, to = T0 + DAY + 8.5 * H + 777;     // 08:00:01 – 08:30:00 of day 2, mid-chunk edges
    const dry = admin.run(e, { op: 'deleteRange', tags: 'Oven*', from, to, dryRun: true });
    const inRange = pts.filter(([t]) => t >= from && t <= to).length;
    assert.deepStrictEqual([dry.points, dry.tags.length], [2 * inRange, 2]);
    const r = admin.run(e, { op: 'deleteRange', tags: 'Oven*', from, to });
    assert.strictEqual(r.points, 2 * inRange);
    assert.ok(r.chunksRewritten >= 2, 'the edge chunks rewritten: ' + r.chunksRewritten);
    const left = pts.filter(([t]) => t < from || t > to);
    const raw = Q.run(e, { tags: 'Oven1.Temp', from: T0, to: NOW, mode: 'raw' })['Oven1.Temp'];
    assert.strictEqual(raw.t.length, left.length);
    assert.ok(!raw.t.some((t) => t >= from && t <= to));
    sameBuckets(e, 'Oven1.Temp', left, T0, T0 + 2 * DAY - 1, '1h', 'per hour');
    sameBuckets(e, 'Oven1.Temp', left, T0, T0 + 2 * DAY - 1, '1d', 'per day');
    e.close();
    const e2 = open(D);
    sameBuckets(e2, 'Oven2.Temp', left, T0, T0 + 2 * DAY - 1, '1d', 'per day after a reopen');
    sameBuckets(e2, 'Oven2.Temp', left, T0, T0 + 2 * DAY - 1, '1h', 'per hour after a reopen');
    e2.close();
});

ok('deleteRange in the open hour and day (in memory): their buckets follow too', () => {
    const e = open(D);
    const from = NOW - 30 * 60000, to = NOW - 10 * 60000;
    admin.run(e, { op: 'deleteRange', tags: 'Oven1.Temp', from, to });
    const left = pts.filter(([t]) => (t < from || t > to) && t >= NOW - 2 * H && !(t >= T0 + DAY + 8 * H + 1234 && t <= T0 + DAY + 8.5 * H + 777));
    sameBuckets(e, 'Oven1.Temp', left, NOW - 2 * H, NOW, '1h', 'the open hour');
    sameBuckets(e, 'Oven1.Temp', left, NOW - 2 * H, NOW, '1d', 'the open day');
    e.close();
});

ok('compact: the bytes of dropped tags and replaced chunks are reclaimed; every point still exact', () => {
    const e = open(D);
    const before = Q.run(e, { tags: 'Oven*', from: T0, to: NOW, mode: 'raw' });
    admin.run(e, { op: 'dropTag', tags: 'Oven2.Temp' });
    const r = admin.run(e, { op: 'compact' });
    assert.ok(r.bytesAfter < r.bytesBefore * 0.6, 'about half gone: ' + JSON.stringify(r));
    const after = Q.run(e, { tags: 'Oven*', from: T0, to: NOW, mode: 'raw' });
    assert.deepStrictEqual(after['Oven1.Temp'], before['Oven1.Temp']);
    e.close();
    const e2 = open(D);
    assert.deepStrictEqual(Q.run(e2, { tags: 'Oven1.Temp', from: T0, to: NOW, mode: 'raw' })['Oven1.Temp'], before['Oven1.Temp']);
    e2.close();
});

ok('a crash after the commit, before every rename: the start finishes the delete', () => {
    const d = tmp(), e = open(d), keep = [];
    for (let i = 0; i < 5000; i++) { const t = T0 + i * 1000; e.write('A', t, i); if (i <= 1000 || i > 2000) keep.push(t); }
    e.checkpoint();
    const real = fs.renameSync;
    let calls = 0;
    fs.renameSync = function (a, b) { if (++calls === 2) throw new Error('power cut'); return real.call(fs, a, b); };
    assert.throws(() => admin.run(e, { op: 'deleteRange', tags: 'A', from: T0 + 1000500, to: T0 + 2000500 }), /power cut/);
    fs.renameSync = real;
    crash(e);
    assert.ok(fs.readFileSync(path.join(d, 'ops.log'), 'utf8').includes('deleteRange'), 'the commit is there, not its done');
    const e2 = open(d);
    const r = Q.run(e2, { tags: 'A', from: T0, to: T0 + DAY, mode: 'raw' }).A;
    assert.deepStrictEqual(Array.from(r.t), keep);
    assert.ok(!fs.existsSync(path.join(d, 'ops.log')), 'the log is cleared');
    assert.ok(!fs.readdirSync(path.join(d, 'idx')).some((f) => f.endsWith('.tmp')), 'no tmp left');
    sameBuckets(e2, 'A', keep.map((t) => [t, (t - T0) / 1000]), T0, T0 + DAY - 1, '1h', 'its hours');
    e2.close();
});

ok('stats and tags', () => {
    const e = open(D);
    const s = admin.run(e, { op: 'stats' });
    assert.ok(s.tags === 1 && s.bytes > 0);
    assert.deepStrictEqual(admin.run(e, { op: 'tags', tags: 'Oven*' }).map((t) => t.name), ['Oven1.Temp']);
    assert.throws(() => admin.run(e, { op: 'nope' }), /unknown op/);
    e.close();
});

console.log(`\n${passed} passed\nALL OK`);
