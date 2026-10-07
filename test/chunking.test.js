'use strict';
// The timer's checkpoint ({ soft: true }) leaves a young small chunk open: a tag that writes once a minute used to get a one-point
// chunk (37 bytes + a 96-byte summary, 135 bytes for 8 bytes of data) at every checkpoint. Its points wait in the WAL, and the WAL
// files holding them are kept until the chunk is written. The clock is simulated (Date.now) so hours pass in milliseconds.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');
const admin = require('../lib/admin');

let passed = 0;
function ok(label, fn) { try { fn(); } finally { Date.now = realNow; } passed++; console.log('✔ ' + label); }
const realNow = Date.now.bind(Date);
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-k-'));
const O = { walSync: false, checkpointMs: 1e9, walFlushMs: 1e9, rawDays: 36500, indexDays: 36500 };
const MIN = 60000, HOUR = 3600000, T0 = Date.UTC(2026, 5, 1, 10, 0, 0);
const walFiles = (d) => fs.readdirSync(path.join(d, 'wal')).length;
const raw = (e, tag) => { const r = Q.run(e, { tags: tag, from: 0, to: Date.now() + 1e6, mode: 'raw' })[tag]; return r ? Array.from(r.t) : []; };
const crash = (e) => { e.flushWal(); e._timers.forEach(clearInterval); fs.closeSync(e.walFd); e.segFds.forEach((x) => fs.closeSync(x.fd)); e.closeIdx(); e._unlock(); };

ok('a tag writing once a minute: a chunk an hour, not one a checkpoint; every point exact; the WAL stays bounded', () => {
    const d = tmp(); let clock = T0; Date.now = () => clock;
    const e = new Engine(d, O).open(); let walMax = 0;
    for (let k = 0; k < 6 * 60; k++) {                              // 6 hours, one point a minute, a soft checkpoint every minute
        clock = T0 + k * MIN;
        e.write('Slow', clock, k);
        e.checkpoint({ soft: true });
        walMax = Math.max(walMax, walFiles(d));
    }
    assert.ok(e.stats.chunks <= 7, 'a chunk an hour (' + e.stats.chunks + ' chunks for 360 checkpoints)');
    assert.ok(walMax <= 63, 'the WAL files of the open hour are kept, no more: ' + walMax);
    assert.deepStrictEqual(raw(e, 'Slow'), Array.from({ length: 360 }, (_, k) => T0 + k * MIN));
    e.close();
});

ok('a crash with young chunks still open: the WAL gives every point back, once; verify is clean', () => {
    const d = tmp(); let clock = T0; Date.now = () => clock;
    let e = new Engine(d, O).open();
    for (let k = 0; k < 150; k++) {
        clock = T0 + k * MIN;
        for (const t of ['A', 'B', 'C']) e.write(t, clock, k * 10 + t.charCodeAt(0));
        e.checkpoint({ soft: true });
    }
    e.write('A', clock + 1, -1);                                    // the last point only in the WAL buffer
    crash(e);
    e = new Engine(d, O).open();
    for (const t of ['A', 'B', 'C']) {
        const r = Q.run(e, { tags: t, from: 0, to: clock + 1e6, mode: 'raw' })[t];
        assert.strictEqual(r.t.length, t === 'A' ? 151 : 150, t + ': every point, once');
        assert.deepStrictEqual(r.t.slice(0, 150), Array.from({ length: 150 }, (_, k) => T0 + k * MIN));
    }
    assert.strictEqual(admin.run(e, { op: 'verify' }).ok, true);
    e.close();
});

ok('a clean close (and an explicit checkpoint) writes every open chunk and leaves no WAL file with data', () => {
    const d = tmp(); let clock = T0; Date.now = () => clock;
    const e = new Engine(d, O).open();
    for (let k = 0; k < 30; k++) { clock = T0 + k * MIN; e.write('S', clock, k); e.checkpoint({ soft: true }); }
    assert.strictEqual(e.stats.chunks, 0, 'nothing cut yet: 30 points, young');
    e.checkpoint();                                                  // as called by an admin operation or the client
    assert.strictEqual(e.stats.chunks, 1);
    clock += MIN; e.write('S', clock, 99); e.checkpoint({ soft: true });
    e.close();
    assert.strictEqual(e.stats.chunks, 2, 'close writes the open chunk');
    const e2 = new Engine(d, O).open();
    assert.ok(e2.stats.recovered <= 1, 'after a clean close only the last point (kept in the WAL so it can be replaced) is replayed: ' + e2.stats.recovered);
    assert.strictEqual(raw(e2, 'S').length, 31);
    e2.close();
});

ok('a chunk is cut when it has chunkMinPoints, when its first point is older than maxChunkAgeMs, and when its segment is over', () => {
    const d = tmp(); let clock = T0; Date.now = () => clock;
    const e = new Engine(d, Object.assign({}, O, { chunkMinPoints: 100 })).open();
    for (let k = 0; k < 99; k++) { clock = T0 + k * 1000; e.write('N', clock, k); }
    e.checkpoint({ soft: true }); assert.strictEqual(e.stats.chunks, 0, '99 points < 100: held');
    clock += 1000; e.write('N', clock, 99); e.checkpoint({ soft: true }); assert.strictEqual(e.stats.chunks, 1, '100 points: cut');
    // age: one point, then 61 minutes later (still in the same hour? the segment is over at 11:00: both rules)
    clock = T0 + 2 * MIN; e.write('Age', clock, 1); clock = T0 + 30 * MIN; e.checkpoint({ soft: true });
    assert.strictEqual(e.stats.chunks, 1, 'a young single point is held');
    clock = T0 + HOUR + MIN; e.checkpoint({ soft: true });           // 11:01: its segment is over, but its only point is under an hour old:
    assert.strictEqual(e.stats.chunks, 1, 'the last point of a tag stays open for an hour (it can still be replaced)');
    clock = T0 + HOUR + 3 * MIN; e.checkpoint({ soft: true });       // 11:03: older than an hour, its segment is over: cut (and the tag N's one point)
    assert.strictEqual(e.stats.chunks, 3, 'its segment is over: cut');
    e.close();
});

ok('a fast tag (600 points a minute) is cut at every checkpoint: chunks stay large, the WAL files go at once', () => {
    const d = tmp(); let clock = T0; Date.now = () => clock;
    const e = new Engine(d, O).open();
    for (let m = 0; m < 10; m++) { for (let i = 0; i < 600; i++) { clock = T0 + m * MIN + i * 100; e.write('Fast', clock, i); } e.checkpoint({ soft: true }); }
    assert.ok(e.stats.chunks >= 10 && e.stats.chunks <= 12, e.stats.chunks + ' chunks for 10 checkpoints');
    assert.ok(walFiles(d) <= 2, 'WAL files: ' + walFiles(d));
    e.close();
});

ok('the old WAL files go once the held chunk is written (and not before)', () => {
    const d = tmp(); let clock = T0; Date.now = () => clock;
    const e = new Engine(d, O).open();
    for (let k = 0; k < 20; k++) { clock = T0 + k * MIN; e.write('W', clock, k); e.checkpoint({ soft: true }); }
    const held = walFiles(d); assert.ok(held >= 19, 'the files holding the open chunk are kept: ' + held);
    clock = T0 + HOUR + MIN; e.checkpoint({ soft: true });
    assert.ok(walFiles(d) <= 2, 'after the chunk was cut: ' + walFiles(d));
    assert.deepStrictEqual(raw(e, 'W'), Array.from({ length: 20 }, (_, k) => T0 + k * MIN));
    e.close();
});

console.log('\n' + passed + ' passed\nALL OK');
