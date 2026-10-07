'use strict';
// Generates a historian of years of data into a folder, as fast as it goes, to be verified with verify.js.
//
//   node test/soak/gen.js --dir D:\tsdb-soak --preset small          100 tags every 1 min   (5 years: 263 M points, minutes)
//   node test/soak/gen.js --dir D:\tsdb-soak --preset medium         100 tags every 10 s    (5 years: 1.6 B points, an hour or so)
//   node test/soak/gen.js --dir D:\tsdb-soak --preset large          20 tags every 1 s      (5 years: 3.2 B points, hours)
//   options: --years 5 | --days N   --tags N   --period 1m|10s|1s   --kill-every 120   --resume   --force
//
// It never writes into a folder that already holds something (unless --resume of its own run). If it dies (Ctrl+C, a
// power cut, --kill-every), run the same command with --resume: it continues from where every tag stopped.
// --kill-every S: a supervisor kills the generator with SIGKILL every ~S seconds (random) and restarts it: the whole
// process dies mid write, again and again, and the dataset must still come out complete and exact.
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');
const { openHistorian } = require('../../lib/client');
const { parseDuration } = require('../../lib/query');
const { Model, DAY } = require('./model');

const argv = process.argv.slice(2);
const args = {};
for (let i = 0; i < argv.length; i++) if (argv[i].startsWith('--')) { const k = argv[i].slice(2), n = argv[i + 1]; if (n === undefined || n.startsWith('--')) args[k] = true; else { args[k] = n; i++; } }
const PRESETS = { small: { tags: 100, period: '1m' }, medium: { tags: 100, period: '10s' }, large: { tags: 20, period: '1s' } };
const OPTS = { rawDays: 36500, indexDays: 36500, walSync: true };       // the dataset is old: nothing may be retained away

const dir = args.dir && path.resolve(String(args.dir));
if (!dir) { console.error('usage: node test/soak/gen.js --dir <folder> [--preset small|medium|large] [--years 5] [--kill-every 120] [--resume]'); process.exit(2); }
const metaFile = path.join(dir, 'meta.json'), doneFile = path.join(dir, 'done.json');
const fmt = (n) => Math.round(n).toLocaleString('en-US');
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

function meta() {
    if (fs.existsSync(metaFile)) {
        if (!args.resume) { console.error(dir + ' already holds a dataset. --resume continues it; else choose another --dir.'); process.exit(2); }
        return JSON.parse(fs.readFileSync(metaFile, 'utf8'));
    }
    if (fs.existsSync(dir) && fs.readdirSync(dir).length) { console.error(dir + ' is not empty: this script only writes into an empty folder.'); process.exit(2); }
    const p = PRESETS[args.preset || 'small'];
    if (!p) { console.error('unknown preset ' + args.preset); process.exit(2); }
    const period = parseDuration(args.period || p.period), tags = Math.round(+args.tags || p.tags);
    const days = Math.round(args.days ? +args.days : (+args.years || 5) * 365);
    if (DAY % period !== 0) { console.error('the period must divide a day (1m, 10s, 1s, 5m ...)'); process.exit(2); }
    const endDay = Math.floor((Date.now() - DAY) / DAY) * DAY;          // yesterday 00:00 UTC (never the future)
    const m = { t0: endDay - days * DAY, end: endDay, period, tags, days, steps: days * DAY / period, created: new Date().toISOString() };
    // the disk: about 1.4 (dense) to 6 (slow data: measured 5.2 at 1 min) bytes a point, plus the WAL; refuse a run the disk cannot hold
    const points = m.steps * tags * 0.98, lo = points * 1.4, hi = points * 6;
    fs.mkdirSync(dir, { recursive: true });
    let free = Infinity;
    try { const s = fs.statfsSync(dir); free = s.bavail * s.bsize; } catch (e) { /* unknown: no check */ }
    console.log(`dataset: ${tags} tags every ${args.period || p.period} for ${days} days = ${fmt(points / 1e6)} M points; about ${(lo / 1e9).toFixed(1)} - ${(hi / 1e9).toFixed(1)} GB on disk; free here ${(free / 1e9).toFixed(0)} GB`);
    if (free < hi * 1.2 && !args.force) { console.error('not enough free disk for the upper estimate (use --force to try anyway)'); fs.rmdirSync(dir); process.exit(2); }
    fs.writeFileSync(metaFile, JSON.stringify(m, null, 1));
    return m;
}

// ---- the supervisor: kill -9 the generator again and again, restart it with --resume ------------------------
async function supervise() {
    meta();
    let kills = 0;
    const base = argv.filter((a, i) => !['--resume', '--kill-every', '--child'].includes(a) && !['--kill-every'].includes(argv[i - 1]));
    for (;;) {
        const child = spawn(process.execPath, [__filename, ...base, '--resume', '--child'], { stdio: 'inherit' });
        const life = (0.5 + Math.random()) * +args['kill-every'] * 1000;
        const timer = setTimeout(() => { kills++; console.log(`\n>>> kill #${kills}: SIGKILL after ${(life / 1000).toFixed(0)} s (the whole process, mid write)\n`); child.kill('SIGKILL'); }, life);
        const code = await new Promise((r) => child.on('exit', (c, sig) => r(sig ? 'killed' : c)));
        clearTimeout(timer);
        if (code === 0 && fs.existsSync(doneFile)) break;
        if (code !== 'killed') { console.error('the generator stopped by itself with code ' + code); process.exit(1); }
    }
    console.log(`finished after ${kills} hard kills. Now: node test/soak/verify.js --dir ${dir}`);
}

// ---- the generator ----------------------------------------------------------------------------------------------
async function generate() {
    const mt = meta(), model = new Model(mt), names = model.names, N = mt.tags;
    const db = openHistorian(dir, OPTS);
    await db.ready;
    const last = new Float64Array(N).fill(-Infinity);
    let k0 = 0;
    if (args.resume) {
        const list = await db.admin({ op: 'tags' });
        let seen = 0, min = Infinity;
        for (const t of list) { const i = model.index.get(t.name); if (i !== undefined && t.last !== null) { last[i] = t.last; seen++; min = Math.min(min, t.last); } }
        k0 = seen < N || !Number.isFinite(min) ? 0 : Math.max(0, Math.floor((min - mt.t0) / mt.period));
        console.log(`resume: ${seen} of ${N} tags have data, the oldest last point ${Number.isFinite(min) ? new Date(min).toISOString() : '-'}; continuing from step ${fmt(k0)} of ${fmt(mt.steps)}`);
    }
    const put = async (i, t, v) => { while (!db.write(names[i], t, v)) await wait(2); };   // a refused point (backpressure) is written again
    const started = performance.now();
    let written = 0, lastReport = started, sinceReport = 0;
    for (let k = k0; k < mt.steps; k++) {
        const t = model.time(k);
        for (let i = 0; i < N; i++) {
            if (t <= last[i] || !model.present(i, k)) continue;
            const v = model.value(i, k);
            if (!db.write(names[i], t, v)) await put(i, t, v);
            written++; sinceReport++;
        }
        if ((k & 127) === 0) await new Promise(setImmediate);
        if (k > k0 && (k * mt.period) % DAY === 0) await db.checkpoint();     // a simulated day: the open chunks to disk
        const now = performance.now();
        if (now - lastReport > 10000) {
            const rate = sinceReport / ((now - lastReport) / 1000), left = (mt.steps - k) * N * 0.98;
            console.log(`${(100 * k / mt.steps).toFixed(1).padStart(5)} %  ${new Date(t).toISOString().slice(0, 10)}  ${fmt(rate)} points/s  ${fmt(written / 1e6)} M written  eta ${left / rate > 90 ? (left / rate / 60).toFixed(0) + ' min' : (left / rate).toFixed(0) + ' s'}`);
            lastReport = now; sinceReport = 0;
        }
    }
    await db.checkpoint();
    await db.close();
    const sec = (performance.now() - started) / 1000;
    const du = (d) => fs.readdirSync(d, { withFileTypes: true }).reduce((s, f) => s + (f.isDirectory() ? du(path.join(d, f.name)) : fs.statSync(path.join(d, f.name)).size), 0);
    const bytes = du(dir);
    fs.writeFileSync(doneFile, JSON.stringify({ at: new Date().toISOString(), points: written, seconds: sec, bytes }, null, 1));
    console.log(`done: ${fmt(written)} points this run in ${(sec / 60).toFixed(1)} min, ${(bytes / 1e9).toFixed(2)} GB on disk. Now: node test/soak/verify.js --dir ${dir}`);
}

(args['kill-every'] && !args.child ? supervise() : generate()).catch((e) => { console.error(e); process.exit(1); });
void os;
