'use strict';
// A writer for test/restart.test.js: opens the folder (the LOCK of a killed predecessor must not stop it), writes points
// 1 per ms from a start time given on the command line, tells the parent how far it got, and is killed with SIGKILL.
const { Engine } = require('../lib/engine');
const dir = process.argv[2], from = +process.argv[3];
const e = new Engine(dir, { walFlushMs: 20, checkpointMs: 500, walSync: true, rawDays: 36500, indexDays: 36500 }).open();
process.send && process.send({ opened: true, tags: e.tags.filter(Boolean).length });
let i = 0;
setInterval(() => {
    for (let k = 0; k < 200; k++, i++) e.write('K', from + i, i);
    e.flushWal();                                      // a point written before this call is durable (walSync)
    process.send && process.send({ durable: from + i - 1 });
}, 5);
