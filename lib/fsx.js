'use strict';
// File operations that survive Windows' refusal to rename a file over one that is open (EPERM; also EACCES / EBUSY): by our own
// handle, or by another process' (the old worker still closing, an indexer, a virus scan). The handle is usually gone in a few
// milliseconds, so the rename is tried again, with growing pauses, for a budget; then it fails with the real error.
// A refusal that is not of that kind (a missing file, a full disk) is thrown at once.
const fs = require('fs');

const BUSY = new Set(['EPERM', 'EACCES', 'EBUSY']);
const pause = new Int32Array(new SharedArrayBuffer(4));
const sleep = (ms) => Atomics.wait(pause, 0, 0, ms);    // the workers are synchronous: this thread waits, nothing else is waiting on it

const isBusy = (e) => !!e && BUSY.has(e.code);

/** fs.renameSync that tries again while the file is busy: `opts.budgetMs` (default 3000), `opts.fs` (a stand-in, for tests). */
function renameSync(from, to, opts) {
    const o = opts || {}, f = o.fs || fs, budget = o.budgetMs === undefined ? 3000 : o.budgetMs, t0 = Date.now();
    let wait = 10;
    for (;;) {
        try { return f.renameSync(from, to); } catch (e) {
            const left = budget - (Date.now() - t0);
            if (!isBusy(e) || left <= 0) throw e;
            sleep(Math.min(wait, left));
            wait = Math.min(200, wait * 2);
        }
    }
}

module.exports = { renameSync, isBusy, BUSY };
