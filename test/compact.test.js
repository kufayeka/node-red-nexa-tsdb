'use strict';
// Index retention: a big index file is not rewritten for a day's worth of expired records (a year of 1 Hz summaries is 50 MB a tag:
// rewriting it daily was 52 GB of writes a day at 1000 tags, and held the worker for minutes). It is rewritten once a quarter of it
// is expired, streamed, fsynced, renamed; a pass can be given a time budget and carries on from where it stopped.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Engine, compactIdx, RECB, REC, F } = require('../lib/engine');
const Q = require('../lib/query');

let passed = 0;
function ok(label, fn) { fn(); passed++; console.log('✔ ' + label); }
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-c-'));
const HOUR = 3600000, DAY = 86400000, T0 = Date.UTC(2024, 0, 1);
// n records, one an hour: tFirst = T0 + i h, tLast = tFirst + 59 min; seg / off valid-looking
function writeIdx(file, n) {
    const buf = Buffer.alloc(n * RECB), f = new Float64Array(buf.buffer, buf.byteOffset, n * REC);
    for (let i = 0; i < n; i++) { const o = i * REC; f[o + F.tFirst] = T0 + i * HOUR; f[o + F.tLast] = T0 + i * HOUR + 59 * 60000; f[o + F.count] = 60; f[o + F.seg] = T0; f[o + F.off] = i; }
    fs.writeFileSync(file, buf); return buf;
}
const tLastOf = (i) => T0 + i * HOUR + 59 * 60000;

ok('a big file with a little expired is left alone (same bytes, same inode); at a quarter expired it is rewritten: exactly the records that were not expired', () => {
    const d = tmp(), f = path.join(d, '0.r0'), n = 20000, buf = writeIdx(f, n);   // 1.9 MB
    const ino = fs.statSync(f).ino;
    assert.strictEqual(compactIdx(f, tLastOf(1999)), false, '10% expired: not worth a rewrite');   // the first 1999 records are < cut
    assert.strictEqual(fs.statSync(f).ino, ino); assert.strictEqual(fs.readFileSync(f).length, buf.length);
    assert.strictEqual(compactIdx(f, tLastOf(5000)), true, '25% expired: rewritten');
    assert.deepStrictEqual(fs.readFileSync(f), buf.subarray(5000 * RECB), 'every record from the cut on, byte for byte');
    assert.ok(!fs.existsSync(f + '.tmp'));
    assert.strictEqual(compactIdx(f, tLastOf(5000)), false, 'nothing more to drop');
});

ok('the cut falls exactly: a record with tLast == cut stays, one before it goes; everything expired empties the file; a small file is cut at once', () => {
    const d = tmp(), f = path.join(d, '0.r0'), n = 20000, buf = writeIdx(f, n);
    compactIdx(f, tLastOf(7000));
    assert.strictEqual(fs.readFileSync(f).length, (n - 7000) * RECB, 'the record whose tLast equals the cut stays');
    compactIdx(f, tLastOf(7000) + 1);
    // 1 record of 13000 is < 25%: waits (a big file)
    assert.strictEqual(fs.readFileSync(f).length, (n - 7000) * RECB);
    compactIdx(f, tLastOf(n) + DAY);
    assert.strictEqual(fs.readFileSync(f).length, 0, 'all expired: an empty file');
    const g = path.join(d, '1.r1'); const sb = writeIdx(g, 100);
    assert.strictEqual(compactIdx(g, tLastOf(1)), true, 'a small file: at once');
    assert.deepStrictEqual(fs.readFileSync(g), sb.subarray(1 * RECB));
    assert.strictEqual(compactIdx(path.join(d, 'none.r0'), 1), false, 'no file');
    assert.strictEqual(compactIdx(g, -Infinity), false, 'no cut (keep for ever)');
});

ok('a crash in the middle (a half-written .tmp next to the intact file): the file is whole, the next pass overwrites the .tmp and finishes', () => {
    const d = tmp(), f = path.join(d, '0.r0'), buf = writeIdx(f, 20000);
    fs.writeFileSync(f + '.tmp', buf.subarray(6000 * RECB, 6100 * RECB));          // what a crash leaves: part of the new file
    assert.deepStrictEqual(fs.readFileSync(f), buf, 'the original is untouched until the rename');
    assert.strictEqual(compactIdx(f, tLastOf(6000)), true);
    assert.deepStrictEqual(fs.readFileSync(f), buf.subarray(6000 * RECB));
    assert.ok(!fs.existsSync(f + '.tmp'));
});

ok('a pass with a budget compacts at least one tag and goes on from there next time: every tag is done within a few passes, none twice, and the engine still answers', () => {
    const d = tmp(), now = tLastOf(12010), t0 = now - 3 * HOUR, TAGS = 12, o = { walSync: false, checkpointMs: 1e9, walFlushMs: 1e9, indexDays: 365, rawDays: 36500 };
    const e = new Engine(d, o).open();
    for (let k = 0; k < TAGS; k++) for (let i = 0; i < 200; i++) e.write('T' + k, t0 + i * 1000, i);
    e.checkpoint();
    // in front of each tag's real records: 12000 hourly records ending long before them, 3250 of which are past `now - 365 d`
    for (let k = 0; k < TAGS; k++) { const f = path.join(d, 'idx', k + '.r0'), real = fs.readFileSync(f); writeIdx(f, 12000); fs.appendFileSync(f, real); }
    const sizeOf = (k) => fs.statSync(path.join(d, 'idx', k + '.r0')).size, done = new Set();
    let passes = 0;
    for (let guard = 0; guard < 50 && done.size < TAGS; guard++, passes++) {
        const before = Array.from({ length: TAGS }, (_, k) => sizeOf(k));
        e.retention(now, 0);                                          // budget 0: the first tag due, then it stops
        const changed = before.map((b, k) => (sizeOf(k) !== b ? k : -1)).filter((k) => k >= 0);
        assert.ok(changed.length >= 1, 'a pass with no budget left still does one tag');
        changed.forEach((k) => { assert.ok(!done.has(k), 'a tag is compacted once'); done.add(k); });
    }
    assert.strictEqual(done.size, TAGS, 'every tag was reached');
    assert.ok(passes <= TAGS + 1, 'round robin: ' + passes + ' passes for ' + TAGS + ' tags');
    for (let k = 0; k < TAGS; k++) assert.ok(sizeOf(k) < 12000 * RECB, 'the expired part of tag ' + k + ' is gone');
    const r = Q.run(e, { tags: 'T0', from: t0 - 1, to: t0 + 1e6, mode: 'raw' }).T0;
    assert.strictEqual(r.t.length, 200, 'the real points are still answered');
    e.close();
});

console.log('\n' + passed + ' passed\nALL OK');
