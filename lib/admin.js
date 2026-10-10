'use strict';
// Admin operations of the historian: drop tags, delete a time range, drop everything, compact, list, stats.
//
//   run(engine, { op: "dropTag",     tags: "Test.*" | [...], dryRun })
//   run(engine, { op: "deleteRange", tags, from, to, dryRun })      from / to: "-2h", ISO, ms
//   run(engine, { op: "dropAll",     confirm: "DROP ALL" })
//   run(engine, { op: "compact" })                                   rewrite segments without the dropped / replaced chunks
//   run(engine, { op: "tags", tags? })   run(engine, { op: "stats" })
// A pattern that is "*" or matches more than 100 tags needs confirm: <the number it matches> (a typo must not delete
// thousands of tags); dryRun: true answers what it would do and changes nothing.
//
// Crash safety: every file an operation changes is first written as <file>.tmp and fsynced; one line in ops.log
// commits the operation (its renames, its unlinks, its dropped tag ids); then the renames / unlinks run and a "done"
// line follows. On a start, a committed operation without its "done" is finished; tmp files without a commit are
// deleted (recoverOps, called by Engine.open before anything is read).
const fs = require('fs');
const fsx = require('./fsx');
const path = require('path');
const gorilla = require('./gorilla');
const { F, REC, RECB, DAY, pad, summarize, merge, readRecords, chunkProblem, sane, emptyAcc } = require('./engine');
const chunk = require('./chunk');
const Q = require('./query');

const BROAD = 100;

// ---- the commit protocol --------------------------------------------------------------------------------------
function fsyncFile(f) { const fd = fs.openSync(f, 'r+'); try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); } }
function appendLine(dir, file, obj) {
    const fd = fs.openSync(path.join(dir, file), 'a');
    try { fs.writeSync(fd, JSON.stringify(obj) + '\n'); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
}
function apply(dir, c) {
    for (const id of c.drops || []) appendLine(dir, 'tags.log', { drop: id });
    for (const [tmp, final] of c.renames || []) { const a = path.join(dir, tmp); if (fs.existsSync(a)) fsx.renameSync(a, path.join(dir, final)); }
    for (const f of c.unlinks || []) { try { fs.unlinkSync(path.join(dir, f)); } catch (e) { if (e.code !== 'ENOENT') throw e; } }
}
function commit(engine, op, renames, unlinks, drops) {
    const dir = engine.dir;
    engine.closeIdx();
    for (const [tmp] of renames) fsyncFile(path.join(dir, tmp));
    const seq = Date.now() + Math.random();
    const c = { seq, op, renames, unlinks, drops };
    appendLine(dir, 'ops.log', c);
    apply(dir, c);
    appendLine(dir, 'ops.log', { seq, done: true });
}

/** On open, before anything is read: finish committed operations, drop uncommitted tmp files. */
function recoverOps(dir) {
    const f = path.join(dir, 'ops.log');
    if (fs.existsSync(f)) {
        const lines = fs.readFileSync(f, 'utf8').split('\n').filter(Boolean).map((l) => { try { return JSON.parse(l); } catch (e) { return null; } }).filter(Boolean);
        const done = new Set(lines.filter((l) => l.done).map((l) => l.seq));
        for (const c of lines) if (!c.done && !done.has(c.seq)) apply(dir, c);
        fs.unlinkSync(f);
    }
    for (const sub of ['idx', 'seg']) {
        const d = path.join(dir, sub);
        if (fs.existsSync(d)) for (const n of fs.readdirSync(d)) if (n.endsWith('.tmp')) fs.unlinkSync(path.join(d, n));
    }
}

// ---- helpers --------------------------------------------------------------------------------------------------
const rel = (engine, tag, l) => path.join('idx', tag.id + '.r' + l);
const asList = (recs) => { const out = []; for (let i = 0; i < recs.length; i += REC) out.push(recs.slice(i, i + REC)); return out; };
function writeTmp(engine, relPath, list) {
    const buf = Buffer.alloc(list.length * RECB);
    list.forEach((r, i) => Buffer.from(r.buffer, r.byteOffset, RECB).copy(buf, i * RECB));
    fs.writeFileSync(path.join(engine.dir, relPath + '.tmp'), buf);
    return [relPath + '.tmp', relPath];
}
function mergeAll(list) { const a = emptyAcc(new Float64Array(REC + 1)); list.forEach((r) => merge(a, r)); return a; }
const bucketOf = (t, size) => Math.floor(t / size) * size;

// null: the segment file is gone (raw past its retention: normal). { bad: why }: the file is there and the chunk is not.
// else { n, len, ver, ok (the checksum holds), body, all (header .. trailer, a copy) }.
function readChunk(engine, seg, off) {
    const file = path.join(engine.dir, 'seg', pad(seg) + '.seg');
    let fd;
    try { fd = fs.openSync(file, 'r'); } catch (e) { if (e.code === 'ENOENT') return null; throw e; }
    try {
        const c = engine._chunkAt(fd, off);
        if (!c || c.torn) return { bad: 'no complete chunk at the index offset' };
        const all = Buffer.from(c.all);
        return { n: c.h.n, len: c.h.len, ver: c.h.ver, id: c.h.id, ok: c.ok, body: all.subarray(16, 16 + c.h.len), all };
    } finally { fs.closeSync(fd); }
}
const damaged = (tag, c) => Object.assign(new Error('corrupt chunk of "' + tag.name + '" (' + (c.bad || 'checksum mismatch') + '): run { op: "verify", repair: true } first'), { code: 'ETSDB_CORRUPT' });

function match(engine, req) {
    const pats = req.tags === undefined ? [] : Array.isArray(req.tags) ? req.tags : [req.tags];
    if (!pats.length) throw new Error('which tags? (tags: "Oven1.Temp", "Line1.*" or a list)');
    const tags = Q.matchTags(engine, pats);
    const broad = pats.includes('*') || tags.length > BROAD;
    if (broad && !req.dryRun && req.confirm !== tags.length) {
        throw new Error('"' + pats.join(', ') + '" matches ' + tags.length + ' tags: send confirm: ' + tags.length + ' to go ahead (or dryRun: true to see them)');
    }
    return tags;
}

// ---- drop tags ------------------------------------------------------------------------------------------------
function dropTags(engine, req) {
    const tags = match(engine, req);
    let points = 0;
    for (const tag of tags) {
        points += tag.n - tag.m0;
        if (!tag.mem) { const r0 = readRecords(engine._idx(tag, 0)); for (let i = 0; i < r0.length; i += REC) points += r0[i + F.count]; }
    }
    const out = { op: 'dropTag', tags: tags.map((t) => t.name), points, dryRun: !!req.dryRun };
    if (req.dryRun || !tags.length) return out;
    const disk = tags.filter((t) => !t.mem);
    if (disk.length) {
        const unlinks = [];
        disk.forEach((t) => { for (let l = 0; l < 3; l++) unlinks.push(rel(engine, t, l)); });
        commit(engine, 'dropTag', [], unlinks, disk.map((t) => t.id));
    }
    // their WAL records and chunks are now nobody's: replay skips them, compact / retention reclaims the bytes
    for (const tag of tags) { engine.tags[tag.id] = null; engine.byName.delete(tag.name); }
    return out;
}

// ---- delete a time range -------------------------------------------------------------------------------------
function deleteRange(engine, req) {
    const now = Date.now();
    const from = Q.parseTime(req.from, now), to = Q.parseTime(req.to, now);
    if (req.from === undefined || !(to >= from)) throw new Error('deleteRange needs from and to (to after from)');
    const tags = match(engine, req);
    if (!req.dryRun) engine.checkpoint();       // every point in a chunk first (the WAL then holds none of them)
    const out = { op: 'deleteRange', tags: [], points: 0, chunksRewritten: 0, summariesDropped: 0, from, to, dryRun: !!req.dryRun };
    const renames = [], after = [];
    for (const tag of tags) {
        let removed = 0;
        if (tag.mem || tag.n) {
            let w = tag.m0;
            for (let i = tag.m0; i < tag.n; i++) {
                const t = tag.ts[i];
                if (t >= from && t <= to) { removed++; continue; }
                if (!req.dryRun) { tag.ts[w] = t; tag.vs[w] = tag.vs[i]; }
                w++;
            }
            if (!req.dryRun) tag.n = w;
        }
        if (!tag.mem) {
            const plan = rangeOfTag(engine, tag, from, to, !!req.dryRun);
            removed = plan.removed;
            out.chunksRewritten += plan.rewritten;
            out.summariesDropped += plan.dropped;
            if (plan.renames.length) { renames.push(...plan.renames); after.push(plan); }
        }
        if (removed) { out.tags.push(tag.name); out.points += removed; if (!req.dryRun && tag.lastT >= from && tag.lastT <= to) tag.lastKnown = false; }
    }
    if (req.dryRun) return out;
    engine.segFds.forEach((s) => { if (s.dirty) { fs.fsyncSync(s.fd); s.dirty = false; } });   // the rewritten chunks
    if (renames.length) commit(engine, 'deleteRange', renames, [], []);
    for (const p of after) { p.tag.acc[1] = p.acc1; p.tag.acc[2] = p.acc2; }
    return out;
}

// one disk tag: its chunks in the range rewritten without those points, its summaries rebuilt (as tmp files)
function rangeOfTag(engine, tag, from, to, dry) {
    const seg = engine.o.segmentMs;
    const old0 = asList(readRecords(engine._idx(tag, 0)));
    const plan = { tag, removed: 0, rewritten: 0, dropped: 0, renames: [] };
    const new0 = [];
    let changed = false;
    const ts = engine._ts, vs = engine._vs;
    for (const r of old0) {
        if (r[F.tLast] < from || r[F.tFirst] > to) { new0.push(r); continue; }
        changed = true;
        if (r[F.tFirst] >= from && r[F.tLast] <= to) { plan.removed += r[F.count]; continue; }
        const c = readChunk(engine, r[F.seg], r[F.off]);
        if (!c) { plan.removed += r[F.count]; plan.dropped++; continue; }   // its raw is past retention: the summary goes whole
        if (c.bad || !c.ok || c.id !== tag.id) throw damaged(tag, c);
        gorilla.decode(c.body, c.n, ts, vs);
        let k = 0;
        for (let i = 0; i < c.n; i++) if (ts[i] < from || ts[i] > to) { ts[k] = ts[i]; vs[k] = vs[i]; k++; }
        plan.removed += c.n - k;
        if (k === c.n) { new0.push(r); continue; }
        if (!k) continue;
        plan.rewritten++;
        const nr = new Float64Array(REC);
        summarize(ts, vs, k, nr);
        nr[F.seg] = r[F.seg];
        if (!dry) {
            // the new chunk at the end of its segment (the old bytes stay until compact / retention)
            const ch = chunk.build(tag.id, k, gorilla.encode(ts, vs, k)), s = engine._segFd(r[F.seg]);
            nr[F.off] = s.size;
            fs.writeSync(s.fd, ch, 0, ch.length, s.size);
            s.size += ch.length; s.dirty = true;
        }
        new0.push(nr);
    }
    if (!changed || dry) return plan;
    plan.renames.push(writeTmp(engine, rel(engine, tag, 0), new0));

    // level 1 (hours) and level 2 (days) of the touched buckets: rebuilt from the level under them when that level
    // still holds the whole bucket (its counts add up); else the bucket's summary goes
    const rebuild = (level, size, oldUp, oldLow, newLow, acc, accLow) => {
        const lowIn = (list, b) => list.filter((r) => bucketOf(r[F.tFirst], size) === b);
        const sum = (list) => list.reduce((s, r) => s + r[F.count], 0);
        const out = [];
        let dropped = 0;
        for (const r of oldUp) {
            const b = bucketOf(r[F.tFirst], size);
            if (b + size - 1 < from || b > to) { out.push(r); continue; }
            if (r[F.count] !== sum(lowIn(oldLow, b))) { dropped++; continue; }
            const m = mergeAll(lowIn(newLow, b));
            if (m[F.count] > 0) out.push(m.slice(0, REC));
        }
        // the open bucket (in memory): the level under it, plus the open one under that (the day's open hour)
        let open = acc;
        if (acc[REC] === acc[REC] && acc[REC] + size - 1 >= from && acc[REC] <= to) {
            open = mergeAll(lowIn(newLow, acc[REC]));
            if (accLow && accLow[REC] === accLow[REC] && bucketOf(accLow[REC], size) === acc[REC] && accLow[F.count] > 0) merge(open, accLow);
            open[REC] = acc[REC];
            if (!(open[F.count] > 0)) open[REC] = NaN;
        }
        return { out, dropped, open };
    };
    const old1 = asList(readRecords(engine._idx(tag, 1))), old2 = asList(readRecords(engine._idx(tag, 2)));
    const h = rebuild(1, seg, old1, old0, new0, tag.acc[1], null);
    const d = rebuild(2, DAY, old2, old1, h.out, tag.acc[2], h.open);
    plan.dropped += h.dropped + d.dropped;
    plan.renames.push(writeTmp(engine, rel(engine, tag, 1), h.out), writeTmp(engine, rel(engine, tag, 2), d.out));
    plan.acc1 = h.open; plan.acc2 = d.open;
    return plan;
}

// ---- drop everything -------------------------------------------------------------------------------------------
function dropAll(engine, req) {
    if (req.confirm !== 'DROP ALL') throw new Error('dropAll deletes every tag and point: send confirm: "DROP ALL"');
    const tags = engine.tags.filter(Boolean).length;
    if (engine.walFd !== null) { fs.closeSync(engine.walFd); engine.walFd = null; }
    engine.walLen = 0;
    engine.segFds.forEach((s) => fs.closeSync(s.fd));
    engine.segFds.clear();
    engine.closeIdx();
    for (const d of ['wal', 'seg', 'idx']) fs.rmSync(path.join(engine.dir, d), { recursive: true, force: true });
    for (const f of ['tags.log', 'dict.log', 'ops.log']) fs.rmSync(path.join(engine.dir, f), { force: true });
    for (const d of ['wal', 'seg', 'idx']) fs.mkdirSync(path.join(engine.dir, d), { recursive: true });
    engine.tags = []; engine.byName = new Map();
    Object.keys(engine.stats).forEach((k) => (engine.stats[k] = 0));
    engine._openWal();
    return { op: 'dropAll', tags };
}

// ---- compact: segments rewritten with only the chunks an index points to ----------------------------------------
function compact(engine) {
    engine.checkpoint();     // closes the segments that are not being written
    const segs = engine._segFiles().filter((s) => !engine.segFds.has(s));
    const live = new Map();  // seg -> [{ tag, i, off }]
    const r0s = new Map();   // tag -> its records
    for (const tag of engine.tags) {
        if (!tag || tag.mem) continue;
        const list = asList(readRecords(engine._idx(tag, 0)));
        r0s.set(tag, list);
        list.forEach((r, i) => { if (segs.includes(r[F.seg])) { if (!live.has(r[F.seg])) live.set(r[F.seg], []); live.get(r[F.seg]).push({ tag, i, off: r[F.off] }); } });
    }
    const renames = [], unlinks = [], touched = new Set();
    let before = 0, afterBytes = 0, rewritten = 0;
    for (const seg of segs) {
        const name = path.join('seg', pad(seg) + '.seg'), size = fs.statSync(path.join(engine.dir, name)).size;
        before += size;
        const chunks = (live.get(seg) || []).sort((a, b) => a.off - b.off);
        if (!chunks.length) { unlinks.push(name); continue; }
        const parts = [];
        let pos = 0;
        for (const c of chunks) {
            const ch = readChunk(engine, seg, c.off);
            if (!ch) continue;
            if (ch.bad || !ch.ok) throw damaged(c.tag, ch);
            parts.push(ch.all);
            r0s.get(c.tag)[c.i][F.off] = pos;
            pos += ch.all.length;
            touched.add(c.tag);
        }
        afterBytes += pos;
        if (pos === size) continue;   // nothing to reclaim (offsets unchanged)
        rewritten++;
        fs.writeFileSync(path.join(engine.dir, name + '.tmp'), Buffer.concat(parts));
        renames.push([name + '.tmp', name]);
    }
    if (!renames.length && !unlinks.length) return { op: 'compact', segments: 0, removed: 0, bytesBefore: before, bytesAfter: before };
    for (const tag of touched) renames.push(writeTmp(engine, rel(engine, tag, 0), r0s.get(tag)));
    commit(engine, 'compact', renames, unlinks, []);
    return { op: 'compact', segments: rewritten, removed: unlinks.length, bytesBefore: before, bytesAfter: afterBytes };
}

// ---- verify / repair --------------------------------------------------------------------------------------------
// Every chunk the index points to is read: its checksum, then its points against the summary record; the index records
// are checked for order and sense; the hour and day summaries are recomputed from level 0 and compared. repair: the
// damaged chunks / records are dropped from the index (the bytes go at the next compact) and the hour and day summaries
// rebuilt from what is left: the database opens and answers again, without the damaged part (it is reported, never hidden).
function emptyBucket(start) { const a = emptyAcc(new Float64Array(REC + 1)); a[REC] = start; return a; }
function groupMerge(list, size) {
    const out = [];
    for (const r of list) {
        const start = bucketOf(r[F.tFirst], size);
        if (!out.length || out[out.length - 1][REC] !== start) out.push(emptyBucket(start));
        merge(out[out.length - 1], r);
    }
    return out;
}
const near = (a, b) => a === b || (a !== a && b !== b) || Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(b)) * 1e3;

function recordProblem(engine, tag, r, prev) {
    if (!sane(r, prev, true)) return { kind: 'index', reason: 'the index record is not valid or out of order' };
    const c = readChunk(engine, r[F.seg], r[F.off]);
    if (!c) return null;                                       // raw past its retention: the summary stands alone
    if (c.bad) return { kind: 'chunk', reason: c.bad };
    if (!c.ok) return { kind: 'chunk', reason: 'checksum mismatch' };
    if (c.id !== tag.id || c.n !== r[F.count]) return { kind: 'chunk', reason: 'not the chunk its index record describes' };
    gorilla.decode(c.body, c.n, engine._ts, engine._vs);
    const why = chunkProblem(engine._ts, engine._vs, c.n, r, c.ver, engine._chk || (engine._chk = new Float64Array(REC)));
    return why ? { kind: 'chunk', reason: why } : null;
}

function levelMismatches(engine, tag, good) {
    let n = 0;
    for (const [level, size] of [[1, engine.o.segmentMs], [2, DAY]]) {
        const want = groupMerge(good, size), stored = asList(readRecords(engine._idx(tag, level))), closed = want.slice(0, -1);
        if (stored.length !== closed.length) { n++; continue; }
        stored.forEach((s, i) => {
            const w = closed[i];
            if (s[F.tFirst] !== w[F.tFirst] || s[F.tLast] !== w[F.tLast] || s[F.count] !== w[F.count] || !near(s[F.sum], w[F.sum]) || !near(s[F.vMin], w[F.vMin]) || !near(s[F.vMax], w[F.vMax]) || s[F.vFirst] !== w[F.vFirst] || s[F.vLast] !== w[F.vLast]) n++;
        });
    }
    return n;
}

function verify(engine, req) {
    engine.checkpoint();
    const pats = req.tags === undefined ? ['*'] : Array.isArray(req.tags) ? req.tags : [req.tags];
    const tags = Q.matchTags(engine, pats).filter((t) => !t.mem);
    const out = { op: 'verify', tags: tags.length, chunks: 0, points: 0, damagedChunks: 0, indexProblems: 0, summaryProblems: 0, problems: [], repaired: false, ok: true };
    const fixes = [];
    for (const tag of tags) {
        const recs = asList(readRecords(engine._idx(tag, 0))), good = [];
        let prev, bad = 0;
        recs.forEach((r, i) => {
            const p = recordProblem(engine, tag, r, prev);
            if (p) {
                bad++;
                out[p.kind === 'index' ? 'indexProblems' : 'damagedChunks']++;
                if (out.problems.length < 100) out.problems.push({ tag: tag.name, kind: p.kind, record: i, from: Number.isFinite(r[F.tFirst]) ? r[F.tFirst] : null, to: Number.isFinite(r[F.tLast]) ? r[F.tLast] : null, reason: p.reason });
                return;
            }
            prev = r[F.tLast]; good.push(r); out.chunks++; out.points += r[F.count];
        });
        let mism = 0;
        if (!bad) { mism = levelMismatches(engine, tag, good); if (mism) { out.summaryProblems += mism; if (out.problems.length < 100) out.problems.push({ tag: tag.name, kind: 'summary', reason: mism + ' hour / day summary record(s) do not match the chunks' }); } }
        if ((bad || mism) && req.repair) fixes.push({ tag, good });
    }
    out.ok = !out.damagedChunks && !out.indexProblems && !out.summaryProblems;
    if (fixes.length && !req.dryRun) {
        for (const { tag, good } of fixes) {
            const hours = groupMerge(good, engine.o.segmentMs), days = groupMerge(good, DAY);
            commit(engine, 'repair', [writeTmp(engine, rel(engine, tag, 0), good), writeTmp(engine, rel(engine, tag, 1), hours.slice(0, -1)), writeTmp(engine, rel(engine, tag, 2), days.slice(0, -1))], [], []);
            tag.acc[1] = hours.length ? hours[hours.length - 1] : emptyBucket(NaN); tag.acc[2] = days.length ? days[days.length - 1] : emptyBucket(NaN);
            const head = tag.n > tag.m0 ? tag.ts[tag.n - 1] : -Infinity, lastRec = good.length ? good[good.length - 1][F.tLast] : -Infinity;
            tag.lastT = Math.max(head, lastRec); tag.lastKnown = false;
        }
        out.repaired = true;
    }
    return out;
}

// ---- list, stats ------------------------------------------------------------------------------------------------
function du(d) { let n = 0; if (!fs.existsSync(d)) return 0; for (const f of fs.readdirSync(d, { withFileTypes: true })) n += f.isDirectory() ? du(path.join(d, f.name)) : fs.statSync(path.join(d, f.name)).size; return n; }

function run(engine, req) {
    if (!req || typeof req !== 'object' || !req.op) throw new Error('an admin request is { op: "tags" | "stats" | "diagnose" | "dropTag" | "deleteRange" | "dropAll" | "compact" | "verify" | "backup" | "restore", ... }');
    if (engine._busy && !['tags', 'stats', 'diagnose'].includes(req.op)) throw new Error('a ' + engine._busy + ' is running: ' + req.op + ' waits until it is done');
    switch (req.op) {
        case 'tags': return req.tags ? Q.matchTags(engine, Array.isArray(req.tags) ? req.tags : [req.tags]).map((t) => engine.tagList().find((x) => x.id === t.id)) : engine.tagList();
        case 'stats': {
            const list = engine.tags.filter(Boolean);
            return Object.assign({ op: 'stats', tags: list.length, memoryTags: list.filter((t) => t.mem).length, bytes: du(engine.dir), dir: engine.dir }, engine.stats);
        }
        case 'diagnose': return engine.diagnose(req.tags ? (Array.isArray(req.tags) ? req.tags : [req.tags]) : null).filter((d) => !req.problems || d.refused > 0);
        case 'dropTag': case 'dropTags': return dropTags(engine, req);
        case 'deleteRange': return deleteRange(engine, req);
        case 'dropAll': return dropAll(engine, req);
        case 'compact': return compact(engine);
        case 'verify': return verify(engine, req);
        case 'backup': return require('./backup').backup(engine, req, () => engine.opened);   // a Promise
        case 'restore': throw new Error('restore swaps the database folder: send it through the historian (client.admin / the admin node) or use lib/backup.js restore with the database closed');
        default: throw new Error('unknown op: ' + req.op);
    }
}

module.exports = { run, recoverOps };
