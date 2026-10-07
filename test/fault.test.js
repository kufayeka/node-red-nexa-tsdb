'use strict';
// Fault injection: a writer whose file system fails at random (disk full, I/O errors, a write that lands in part, a failed
// rename) restarts its engine on every error and is finally killed (kill -9); this is repeated on the same database.
// After every round the database is opened and checked:
//   1. it opens;
//   2. every point it returns is the point that was written (no wrong value, times rising, no duplicate);
//   3. every point that a successful flushWal covered is there (durable means durable), except writes that threw;
//   4. the index agrees with the chunks: verify finds nothing, bucket counts equal the rows.
//   node test/fault.test.js [--rounds 10] [--tags 12] [--p 0.03]
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');
const admin = require('../lib/admin');

const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? +process.argv[i + 1] : d; };
const ROUNDS = arg('rounds', 10), TAGS = arg('tags', 12), P = arg('p', 0.03);
const T0 = Date.UTC(2026, 5, 1), name = (i) => 'F.T' + i, val = (i, k) => ((i * 131 + k * 7) % 1000) / 10;
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-fault-'));
const logFile = path.join(dir, '..', path.basename(dir) + '.log');
const OPTS = { walSync: false, checkpointMs: 1e9, walFlushMs: 1e9, rawDays: 36500, indexDays: 36500 };

(async () => {
    let next = 0, restarts = 0, errors = 0, failedWrites = 0;
    const durable = [];                       // [from, to] step ranges that a flushWal covered
    const failed = new Set();                 // "tag:step" of writes that threw
    for (let round = 0; round < ROUNDS; round++) {
        fs.writeFileSync(logFile, '');
        const code = await new Promise((r) => spawn(process.execPath, [path.join(__dirname, 'fault-child.js'), dir, String(1000 + round), String(next), logFile, String(TAGS), String(P)], { stdio: 'inherit' }).on('exit', (c, sig) => r(sig || c)));
        assert.ok(code === 'SIGKILL' || code === 9 || code === 1, 'the child ended by the kill, got ' + code);
        let lastD = next - 1, lastK = next - 1, cur = next;
        for (const line of fs.readFileSync(logFile, 'utf8').split(String.fromCharCode(10))) {
            const [t, a, b] = line.split(' ');
            // a flushWal that returned covers everything written since the last one, unless the engine was restarted in between
            // (its unflushed buffer is gone): then only what comes after the restart
            if (t === 'D') { if (+a >= cur) durable.push([cur, +a]); cur = +a + 1; lastD = +a; }
            else if (t === 'R') restarts++;                         // nothing is lost to a restart: the pending points are played into the new engine
            else if (t === 'F') { failed.add(a + ':' + b); failedWrites++; } else if (t === 'E') errors++; else if (t === 'K') lastK = +a;
        }
        const from = next;
        if (lastK < from) lastK = from + 99;     // killed before it said: the steps it started are unknown; the next round starts after the last D and a margin
        next = Math.max(lastK, lastD) + 1;

        // 1. it opens
        const e = new Engine(dir, OPTS).open();
        // 2. every point is the point that was written
        const present = [];
        for (let i = 0; i < TAGS; i++) {
            const r = Q.run(e, { tags: name(i), from: T0, to: T0 + next * 1000, mode: 'raw', limit: 5000000, maxPoints: 5000000 })[name(i)];
            const have = new Set();
            for (let n = 0; n < r.t.length; n++) {
                if (n) assert.ok(r.t[n] > r.t[n - 1], `round ${round} ${name(i)}: times not rising at ${n}`);
                const k = (r.t[n] - T0) / 1000;
                assert.ok(Number.isInteger(k) && k >= 0 && k < next, `round ${round} ${name(i)}: a time that was never written (${r.t[n]})`);
                assert.strictEqual(r.v[n], val(i, k), `round ${round} ${name(i)} step ${k}: a wrong value`);
                have.add(k);
            }
            present.push(have);
            // 3. every durable point is there
            for (const [a, b] of durable) for (let k = a; k <= b; k++) if (!failed.has(i + ':' + k)) assert.ok(have.has(k), `round ${round} ${name(i)}: step ${k} was durable (flushWal returned) and is gone`);
            // 4. the index agrees: the bucket counts add up to the rows
            const c = Q.run(e, { tags: name(i), from: T0, to: T0 + next * 1000, mode: 'bucket', bucket: '1h', agg: ['count'] })[name(i)];
            assert.strictEqual(c.count.reduce((x, y) => x + y, 0), r.t.length, `round ${round} ${name(i)}: the hour summaries count another number of points than the rows`);
        }
        const v = admin.run(e, { op: 'verify' });
        assert.ok(v.ok, `round ${round}: verify found damage after a crash: ${JSON.stringify(v.problems.slice(0, 3))}`);
        e.close();
        console.log(`round ${round + 1}/${ROUNDS}: steps to ${next - 1}, ${present[0].size} points a tag, engine restarts so far ${restarts}, injected errors ${errors}, writes that threw ${failedWrites}: all checks passed`);
    }
    fs.rmSync(dir, { recursive: true, force: true }); fs.rmSync(logFile, { force: true });
    console.log(`\n${ROUNDS} rounds, ${restarts} restarts after injected I/O errors, ${ROUNDS} kill -9\nALL OK`);
})().catch((e) => { console.error(e); console.error('the database is kept for a look: ' + dir); process.exit(1); });
