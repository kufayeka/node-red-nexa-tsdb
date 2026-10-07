'use strict';
// The writer of test/fault.test.js: writes points, flushes, checkpoints, compacts, while the file system FAILS at random
// (disk full, I/O errors, a write that lands only in part, a rename that fails). On any error the engine is stopped as a
// crash does (abort) and opened again (recovery from the WAL), as the worker does. At the end the process kills itself.
//   node test/fault-child.js <dir> <seed> <startStep> <logfile> <tags> <failure probability>
// The log (written around the injection): "D <step>" after a flushWal that returned (everything written before it is durable),
// "F <tag> <step>" a write that threw (not acknowledged), "R <step>" an engine restart (what was written since the last flushWal is lost with the old
// engine's buffer: the next flushWal covers only what comes after), "E <message>".
const fs = require('fs');
const path = require('path');
const { dir, seed: seed0, start, log, tags: nTags, p } = (() => { const a = process.argv.slice(2); return { dir: a[0], seed: +a[1], start: +a[2], log: a[3], tags: +a[4], p: +a[5] }; })();
const real = {};
for (const k of ['writeSync', 'fsyncSync', 'fdatasyncSync', 'renameSync', 'unlinkSync', 'truncateSync', 'writeFileSync', 'appendFileSync']) real[k] = fs[k];
const logFd = fs.openSync(log, 'a');
const note = (s) => real.writeSync.call(fs, logFd, s + '\n');

let s = seed0 >>> 0;
const rnd = () => { s = (s + 0x6D2B79F5) >>> 0; let t = s; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
let inject = false;
const hit = () => inject && rnd() < p;
const fail = (code) => { const e = new Error('injected ' + code); e.code = code; throw e; };

fs.writeSync = function (fd, data, ...rest) {
    if (fd !== logFd && hit()) {
        // half of the failures: part of the data lands first (a disk that fills up in the middle of a write)
        if (rnd() < 0.5) {
            if (typeof data === 'string') real.writeSync.call(fs, fd, data.slice(0, Math.floor(rnd() * data.length)));
            else if (rest.length >= 2 && typeof rest[1] === 'number') real.writeSync.call(fs, fd, data, rest[0], Math.floor(rnd() * rest[1]), rest[2]);
        }
        fail('ENOSPC');
    }
    return real.writeSync.call(fs, fd, data, ...rest);
};
for (const k of ['fsyncSync', 'fdatasyncSync', 'renameSync', 'unlinkSync', 'truncateSync']) fs[k] = function (...a) { if (hit()) fail('EIO'); return real[k].apply(fs, a); };
fs.writeFileSync = function (f, ...a) { if (hit()) { if (rnd() < 0.5) real.writeFileSync.call(fs, f, String(a[0]).slice(0, 5)); fail('ENOSPC'); } return real.writeFileSync.call(fs, f, ...a); };

// FAULT_SOFT=1: the clock follows the steps (every point is "now") and the timer's checkpoint ({ soft: true }) runs every 60 steps, so
// young small chunks stay open with their points only in the WAL: the path of a slow tag, crashed and failed at random
const SOFT = process.env.FAULT_SOFT === '1';
const nowStep = { k: start };
if (SOFT) Date.now = () => Date.UTC(2026, 5, 1) + nowStep.k * 1000;
const { Engine } = require('../lib/engine');
const admin = require('../lib/admin');
const T0 = Date.UTC(2026, 5, 1), name = (i) => 'F.T' + i, val = (i, k) => ((i * 131 + k * 7) % 1000) / 10;
const OPTS = { walSync: true, checkpointMs: 1e9, walFlushMs: 1e9, rawDays: 36500, indexDays: 36500 };

let e = null, curStep = start;
function openEngine() {
    inject = false;                                   // a start is not made to fail here: the point of the test is the run
    e = new Engine(dir, OPTS).open();
    inject = true;
}
function restart(err) {
    note('E ' + err.message);
    let pend = [];
    try { pend = e.pending(); } catch (x) { /* nothing to save */ }
    try { e.abort(); } catch (x) { /* nothing to close */ }
    note('R ' + curStep);
    openEngine();
    // what was written but not in the WAL yet is played into the new engine (as the worker does); a failure here restarts again
    for (let i = 0; i < pend.length; i++) {
        try { e.replayPoint(pend[i].name, pend[i].ts, pend[i].value); } catch (err) { return restart(err); }
    }
}
const guard = (fn) => { try { return fn(); } catch (err) { restart(err); return undefined; } };

openEngine();
const steps = 3000 + Math.floor(rnd() * 6000);
for (let k = start; k < start + steps; k++) {
    curStep = k; nowStep.k = k;
    for (let i = 0; i < nTags; i++) {
        try { e.write(name(i), T0 + k * 1000, val(i, k)); } catch (err) {
            restart(err);                                                  // then the same point again, as the worker does
            try { e.write(name(i), T0 + k * 1000, val(i, k)); } catch (err2) { note('F ' + i + ' ' + k); restart(err2); }
        }
    }
    if (k % 100 === 0) { const ok = guard(() => { e.flushWal(); return true; }); if (ok) note('D ' + k); }
    if (SOFT ? k % 60 === 0 : k % 700 === 0) guard(() => e.checkpoint(SOFT ? { soft: true } : undefined));
    if (k % 2500 === 0 && k > start) guard(() => admin.run(e, { op: 'compact' }));
}
note('K ' + (start + steps - 1));
process.kill(process.pid, 'SIGKILL');
