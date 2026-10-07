'use strict';
// The historian never returns a wrong value without saying so: a cut answer is refused (or paged), a damaged chunk or index
// is an error with its place, a power cut's zero-filled / torn tails are cleaned on open, verify finds damage and repair
// makes the database answer again (without the damaged part, reported), data written before the checksums still reads.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Engine, F, REC, RECB, pad } = require('../lib/engine');
const Q = require('../lib/query');
const admin = require('../lib/admin');
const chunk = require('../lib/chunk');

let passed = 0;
function ok(label, fn) { fn(); passed++; console.log('✔ ' + label); }
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-i-'));
const open = (dir, o) => new Engine(dir, Object.assign({ walSync: false, checkpointMs: 1e9, walFlushMs: 1e9, rawDays: 36500, indexDays: 36500 }, o)).open();
const MIN = 60000, T0 = Date.UTC(2026, 0, 1);
const value = (k) => (k * 7) % 1000 / 10;
function fill(e, tag, n, from) { for (let k = from || 0; k < n; k++) e.write(tag, T0 + k * MIN, value(k)); }
const seg0 = (d) => path.join(d, 'seg', fs.readdirSync(path.join(d, 'seg')).sort()[0]);
const raisesCorrupt = (fn, re) => assert.throws(fn, (e) => e.code === 'ETSDB_CORRUPT' && (!re || re.test(e.message)));

ok('a raw answer past the limit is an error, never a cut result; page: true returns exact pages that add up to every row', () => {
    const d = tmp(), e = open(d); fill(e, 'A', 35000); e.checkpoint();
    const q = { tags: 'A', from: T0, to: T0 + 35000 * MIN, mode: 'raw' };
    assert.throws(() => Q.run(e, Object.assign({ limit: 10000 }, q)), /more than 10,000 points.*nothing is returned cut/i);
    assert.strictEqual(Q.run(e, Object.assign({ limit: 35000 }, q)).A.t.length, 35000, 'exactly the limit: whole');
    let from = T0, pages = 0;
    const t = [], v = [];
    for (;;) {
        const r = Q.run(e, Object.assign({}, q, { from, limit: 10000, page: true })).A;
        t.push(...r.t); v.push(...r.v); pages++;
        if (!r.more) break;
        assert.strictEqual(r.t.length, 10000); from = r.next;
    }
    assert.strictEqual(pages, 4);
    assert.strictEqual(t.length, 35000);
    assert.ok(t.every((x, k) => x === T0 + k * MIN && v[k] === value(k)), 'every row, once, exact');
    assert.throws(() => Q.run(e, Object.assign({ format: 'rows', limit: 10, page: true }, q)), /format: series/);
    assert.throws(() => Q.run(e, Object.assign({ maxPoints: 1000 }, q)), /maxPoints/);
    e.close();
});

ok('a flipped bit anywhere in a chunk is refused with its place (raw, a bucket under an hour, exact m4): 60 random flips', () => {
    const d = tmp(); let e = open(d); fill(e, 'A', 20000); e.close();
    const file = seg0(d), orig = fs.readFileSync(file);
    let seed = 11; const rnd = () => { seed = (seed * 16807) % 2147483647; return seed / 2147483647; };
    let seen = 0;
    for (let i = 0; i < 60; i++) {
        const b = Buffer.from(orig), pos = Math.floor(rnd() * b.length);
        b[pos] ^= 1 << Math.floor(rnd() * 8);
        fs.writeFileSync(file, b);
        const e2 = open(d, { checkpointMs: 1e9 });
        const range = { tags: 'A', from: T0, to: T0 + 20000 * MIN };
        let caught = 0;
        for (const q of [{ mode: 'raw' }, { mode: 'bucket', bucket: '10m', agg: ['sum'] }, { mode: 'm4', width: 4000 }]) {
            try { Q.run(e2, Object.assign({}, range, q)); } catch (x) { if (x.code === 'ETSDB_CORRUPT') caught++; else throw x; }
        }
        e2.close();
        // the flip is in a chunk's bytes (or a header that makes it unreadable): at least raw sees it
        assert.ok(caught >= 1, 'flip at byte ' + pos + ' went unnoticed');
        seen++;
    }
    fs.writeFileSync(file, orig);
    const e3 = open(d); assert.strictEqual(Q.run(e3, { tags: 'A', from: T0, to: T0 + 20000 * MIN, mode: 'raw' }).A.t.length, 20000, 'the original reads again'); e3.close();
    assert.strictEqual(seen, 60);
});

ok('data written before the checksums (TSC1 chunks, no trailer) still reads exactly; a damaged one is caught by its summary', () => {
    const d = tmp(); let e = open(d); fill(e, 'A', 3000); fill(e, 'B', 3000); e.close();
    // rewrite every chunk as TSC1: magic 1, no trailer; offsets in level 0 move
    const dirSeg = path.join(d, 'seg');
    const idx = {}, newOff = new Map();
    for (const f of fs.readdirSync(dirSeg)) {
        const b = fs.readFileSync(path.join(dirSeg, f)), parts = []; let off = 0, pos = 0;
        while (off < b.length) {
            const h = chunk.parse(b.subarray(off, off + 16)), body = b.subarray(off + 16, off + 16 + h.len), head = Buffer.from(b.subarray(off, off + 16));
            head.writeUInt32LE(chunk.MAGIC1, 0); parts.push(head, body); newOff.set(f + ':' + off, pos); pos += 16 + h.len; off += h.total;
        }
        fs.writeFileSync(path.join(dirSeg, f), Buffer.concat(parts));
    }
    for (const f of fs.readdirSync(path.join(d, 'idx')).filter((x) => x.endsWith('.r0'))) {
        const file = path.join(d, 'idx', f), recs = new Float64Array(fs.readFileSync(file).buffer.slice(0));
        for (let i = 0; i < recs.length; i += REC) recs[i + F.off] = newOff.get(pad(recs[i + F.seg]) + '.seg:' + recs[i + F.off]);
        fs.writeFileSync(file, Buffer.from(recs.buffer));
    }
    e = open(d);
    const r = Q.run(e, { tags: ['A', 'B'], from: T0, to: T0 + 3000 * MIN, mode: 'raw' });
    for (const k of ['A', 'B']) assert.ok(r[k].t.length === 3000 && r[k].t.every((x, i) => x === T0 + i * MIN && r[k].v[i] === value(i)), k + ' exact from TSC1 chunks');
    assert.strictEqual(admin.run(e, { op: 'verify' }).ok, true);
    e.close();
    // a damaged TSC1 chunk: no checksum, but its values no longer match its summary
    const file = seg0(d), b = fs.readFileSync(file); b[16 + 20] ^= 0x40; fs.writeFileSync(file, b);
    e = open(d);
    raisesCorrupt(() => Q.run(e, { tags: 'A', from: T0, to: T0 + 3000 * MIN, mode: 'raw' }), /summary|times|sense|not/);
    e.close();
});

ok('zero-filled and torn tails (what a power cut leaves) are cleaned when the database opens; the answers stay exact', () => {
    const d = tmp(); let e = open(d); fill(e, 'A', 6000); e.close();
    fs.appendFileSync(path.join(d, 'idx', '0.r0'), Buffer.alloc(RECB * 2 + 37));          // two zero records and a ragged bit
    fs.appendFileSync(path.join(d, 'idx', '0.r1'), Buffer.alloc(RECB));
    const segs = fs.readdirSync(path.join(d, 'seg')).sort(), last = path.join(d, 'seg', segs[segs.length - 1]);
    fs.appendFileSync(last, Buffer.alloc(300));                                            // a zero-filled tail of the last segment
    e = open(d);
    const r = Q.run(e, { tags: 'A', from: T0, to: T0 + 6000 * MIN, mode: 'raw' }).A;
    assert.ok(r.t.length === 6000 && r.v.every((v, i) => v === value(i)));
    const h = Q.run(e, { tags: 'A', from: T0, to: T0 + 6000 * MIN, mode: 'bucket', bucket: '1h', agg: ['count'] }).A;
    assert.strictEqual(h.count.reduce((a, b) => a + b, 0), 6000);
    assert.strictEqual(fs.statSync(path.join(d, 'idx', '0.r0')).size % RECB, 0);
    assert.strictEqual(admin.run(e, { op: 'verify' }).ok, true, 'the database is clean');
    e.close();
});

ok('bytes after the last chunk that are not zeros are left alone and counted, never cut away', () => {
    const d = tmp(); let e = open(d); fill(e, 'A', 3000); e.close();
    const segs = fs.readdirSync(path.join(d, 'seg')).sort(), last = path.join(d, 'seg', segs[segs.length - 1]), size = fs.statSync(last).size;
    fs.appendFileSync(last, Buffer.from('this is not a chunk, whoever wrote it'));
    e = open(d);
    assert.ok(e.stats.unreadableBytes > 0, 'counted');
    assert.ok(fs.statSync(last).size > size, 'not cut');
    assert.strictEqual(Q.run(e, { tags: 'A', from: T0, to: T0 + 3000 * MIN, mode: 'raw' }).A.t.length, 3000);
    e.close();
});

ok('an index out of order or not valid is an error with its file, not a wrong answer', () => {
    const d = tmp(); let e = open(d); fill(e, 'A', 8000); e.close();
    const f = path.join(d, 'idx', '0.r0'), b = fs.readFileSync(f), a = Buffer.from(b.subarray(0, RECB)), c = Buffer.from(b.subarray(RECB, 2 * RECB));
    c.copy(b, 0); a.copy(b, RECB);                                                         // records 0 and 1 swapped
    fs.writeFileSync(f, b);
    e = open(d);
    raisesCorrupt(() => Q.run(e, { tags: 'A', from: T0, to: T0 + 8000 * MIN, mode: 'raw' }), /index 0\.r0/);
    e.close();
});

ok('verify finds a damaged chunk, repair drops it from the index: every other point exact, the aggregates equal what is left, clean after a reopen', () => {
    const d = tmp(); let e = open(d); fill(e, 'A', 9000); fill(e, 'B', 9000); e.close();
    const file = seg0(d), b = fs.readFileSync(file);
    // damage the chunk of A in the first segment
    const probe = open(d); const rec = require('../lib/engine').readRecords(probe._idx(probe.byName.get('A'), 0)); probe.close();
    const first = { from: rec[F.tFirst], to: rec[F.tLast], count: rec[F.count], off: rec[F.off] };
    b[first.off + 30] ^= 0x01; fs.writeFileSync(file, b);
    e = open(d);
    const v = admin.run(e, { op: 'verify' });
    assert.strictEqual(v.ok, false); assert.strictEqual(v.damagedChunks, 1);
    assert.deepStrictEqual([v.problems[0].tag, v.problems[0].kind, v.problems[0].reason, v.problems[0].from, v.problems[0].to], ['A', 'chunk', 'checksum mismatch', first.from, first.to]);
    raisesCorrupt(() => admin.run(e, { op: 'compact' }), /verify/);
    const fixed = admin.run(e, { op: 'verify', repair: true });
    assert.strictEqual(fixed.repaired, true);
    assert.strictEqual(admin.run(e, { op: 'verify' }).ok, true, 'clean now');
    const check = (eng) => {
        const r = Q.run(eng, { tags: ['A', 'B'], from: T0, to: T0 + 9000 * MIN, mode: 'raw' });
        assert.strictEqual(r.B.t.length, 9000, 'the other tag is whole');
        const want = []; for (let k = 0; k < 9000; k++) { const t = T0 + k * MIN; if (t < first.from || t > first.to) want.push(k); }
        assert.deepStrictEqual(Array.from(r.A.t), want.map((k) => T0 + k * MIN));
        assert.ok(r.A.v.every((x, i) => x === value(want[i])));
        const hr = Q.run(eng, { tags: 'A', from: T0, to: T0 + 9000 * MIN, mode: 'bucket', bucket: '1d', agg: ['count', 'sum'] }).A;
        assert.strictEqual(hr.count.reduce((x, y) => x + y, 0), want.length);
        assert.ok(Math.abs(hr.sum.reduce((x, y) => x + y, 0) - want.reduce((s, k) => s + value(k), 0)) < 1e-6);
    };
    check(e); e.close();
    e = open(d); check(e); assert.strictEqual(admin.run(e, { op: 'verify' }).ok, true); e.close();
});

ok('verify finds a wrong hour / day summary and repair rebuilds it from the chunks', () => {
    const d = tmp(); let e = open(d); fill(e, 'A', 4 * 1440); e.close();
    const f = path.join(d, 'idx', '0.r2'), b = fs.readFileSync(f), x = new Float64Array(b.buffer.slice(b.byteOffset, b.byteOffset + b.length));
    x[F.sum] += 1000; fs.writeFileSync(f, Buffer.from(x.buffer));
    e = open(d);
    const v = admin.run(e, { op: 'verify' });
    assert.ok(!v.ok && v.summaryProblems >= 1 && v.problems[0].kind === 'summary');
    admin.run(e, { op: 'verify', repair: true });
    assert.strictEqual(admin.run(e, { op: 'verify' }).ok, true);
    const day = Q.run(e, { tags: 'A', from: T0, to: T0 + 4 * 1440 * MIN - 1, mode: 'bucket', bucket: '1d', agg: ['sum'] }).A.sum;
    const want = [0, 1, 2, 3].map((dd) => { let s = 0; for (let k = dd * 1440; k < (dd + 1) * 1440; k++) s += value(k); return s; });
    day.forEach((s, i) => assert.ok(Math.abs(s - want[i]) < 1e-6));
    e.close();
});

ok('a crash with a tag that stopped hours ago: its open hour and day buckets come back (found by the kill -9 soak run)', () => {
    const d = tmp(); let e = open(d);
    // tag A writes 10 minutes, then nothing; tag B keeps writing for 5 hours: the files' last segments are long after A's
    for (let k = 0; k < 10; k++) e.write('A', T0 + k * MIN, value(k));
    for (let k = 0; k < 5 * 60; k++) e.write('B', T0 + k * MIN, value(k));
    e.checkpoint(); e.flushWal();
    e.abort();                                                  // the crash: A's bucket was open in memory only
    e = open(d);
    const count = (tag, bucket) => Q.run(e, { tags: tag, from: T0, to: T0 + 6 * 3600000, mode: 'bucket', bucket, agg: ['count', 'sum'] })[tag];
    assert.deepStrictEqual([count('A', '1h').count, count('A', '1d').count], [[10], [10]], 'A: its hour and its day are there');
    assert.ok(Math.abs(count('A', '1h').sum[0] - Array.from({ length: 10 }, (_, k) => value(k)).reduce((x, y) => x + y, 0)) < 1e-9);
    assert.strictEqual(count('B', '1h').count.reduce((x, y) => x + y, 0), 300);
    const v = admin.run(e, { op: 'verify' });
    assert.ok(v.ok, 'verify: ' + JSON.stringify(v.problems.slice(0, 2)));
    // and A can carry on: a later point lands in a new hour, the old one is closed exactly once
    e.write('A', T0 + 5 * 3600000, 1); e.checkpoint();
    assert.strictEqual(admin.run(e, { op: 'verify' }).ok, true);
    assert.deepStrictEqual(Q.run(e, { tags: 'A', from: T0, to: T0 + 6 * 3600000, mode: 'bucket', bucket: '1h', agg: ['count'] }).A.count, [10, 1]);
    e.close();
});

console.log(`\n${passed} passed\nALL OK`);
