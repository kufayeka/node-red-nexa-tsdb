'use strict';
// The historian core: ts, tag, value. Pure: no assets, no JSON, no events (those are the layers above).
//
// On disk (one folder per database):
//   tags.log        a line per tag: {"id","name","type"}                       (append only)
//   dict.log        a line per string of a string tag: [tagId, id, "text"]    (append only)
//   wal/<n>.wal     write-ahead log: tagId u32 · ts f64 · value f64 (20 bytes) (deleted after a checkpoint)
//   seg/<t>.seg     the chunks of one time segment (default 1 h): header 16 bytes + Gorilla bytes
//   idx/<id>.r0     a summary per chunk (≈ 1024 points) and where it is      ← pyramid level 0
//   idx/<id>.r1     a summary per segment (1 h)                                ← level 1
//   idx/<id>.r2     a summary per day                                          ← level 2
// A summary is 15 float64 (120 bytes, lib/layout.js): tFirst tLast vFirst vLast vMin tMin vMax tMax sum count seg off inc integL integS. It holds the
// first, last, min and max WITH their times, so a bucket's aggregates come out of the summaries exactly as from the raw
// points, for any range, reading about as many records as the answer has rows.
//
// Values are float64: a number as it is, a bool as 0 / 1, a string as its id in the tag's dictionary.
// Durability: a point is in the WAL (fsync every walFlushMs) before it is in a chunk; a checkpoint writes every open
// chunk, fsyncs the segments and only then deletes the WAL it covered. After a crash the index of the last two segments
// is checked against the segments, the summaries are rebuilt and the WAL is replayed (a point already in a chunk is
// skipped). The open chunks live in memory (up to chunkPoints points a tag).
//
// Storage rules (opts.rules, the first whose pattern matches a tag's name; * matches any text):
//   { pattern: "Vib.*", store: "memory", keep: "10s", max: 100000 }   a ring in RAM: never on disk, gone on a restart
//   { pattern: "Debug.*", keep: "1d", raw: "1d" }                      on disk: kept 1 day (raw and summaries)
//   default: on disk, raw kept rawDays, per-chunk summaries indexDays, hour / day summaries for ever
// A tag's store is set when it is created; its keep / raw follow the rules of each start. A query never returns what
// is past a tag's keep (or, from raw points, its raw); the files go at the next retention pass.
//
// ops.log: deletes and compaction are written as tmp files, then committed with one line, then renamed; a start
// after a crash finishes a committed one and drops an uncommitted one (lib/admin.js).
const fs = require('fs');
const path = require('path');
const gorilla = require('./gorilla');
const chunk = require('./chunk');
const { parseDuration, globRe } = require('./query');

const DAY = 86400000;
const { REC, RECB, F } = require('./layout');   // a summary record: 15 float64
const SEG_MAGIC = 0x31435354;                   // "TSC1"
const WALB = 20;
// test hook (test/worker.test.js): TSDB_TEST_FAULT=flushWal:3 makes the 3rd flushWal throw an EIO once
const FAULT = (() => { const m = /^(\w+):(\d+)$/.exec(process.env.TSDB_TEST_FAULT || ''); return m ? { op: m[1], n: +m[2], seen: 0 } : null; })();
function testFault(op) { if (FAULT && FAULT.op === op && ++FAULT.seen === FAULT.n) { const e = new Error('injected test fault'); e.code = 'EIO'; throw e; } }
const QFDS = 32;                // segment files one query keeps open
const RBUF = 4096;              // the first read of a chunk
const LEVELS = 3;
const LOCK_STALE_MS = 30000;    // a LOCK that has not beat for this long is stale
const ABANDONED = new Set();    // tokens of locks this thread's engines gave up but could not delete (the file system failed): its own leftovers

const DEFAULTS = {
    segmentMs: 3600000,         // a segment file (and the level 1 summary): 1 hour; a day must be a whole number of them
    chunkPoints: 1024,          // points in a chunk
    walFlushMs: 1000,           // the WAL is written and fsynced this often (the most a power cut loses)
    checkpointMs: 60000,        // open chunks are written this often (the WAL is then dropped)
    rawDays: 30,                // raw chunks (segments) kept
    indexDays: 365,             // level 0 summaries kept (the raw data's own summaries outlive it)
    walSync: true,
    rules: [],
    idxFds: 512,                // index files kept open for appends (an LRU): a file open costs ~1-2 ms on Windows
    segFds: 64,                 // segment files kept open (an LRU; a backfill touches thousands of hours)
    minTs: 1,                   // a point before this time is refused (ts 0: a device without a clock; a zero-filled WAL tail reads as ts 0)
    maxFutureMs: 86400000,      // a point more than this ahead of the clock is refused: one wrong clock must not make every later point 'late' (0: no limit)
    lock: true,                 // one engine a folder (a LOCK file: pid + heartbeat)
    chunkMinPoints: 256,        // the timer's checkpoint cuts a chunk only when the open one has this many points ...
    maxChunkAgeMs: 3600000      // ... or its first point is this old, or its segment is over (until then its points wait in the WAL)
};

const pad = (n) => String(n).padStart(15, '0');

class Engine {
    constructor(dir, opts) {
        this.dir = dir;
        this.o = Object.assign({}, DEFAULTS, opts || {});
        if (DAY % this.o.segmentMs !== 0) throw new Error('segmentMs must divide a day');
        this.tags = [];                 // by id
        this.byName = new Map();
        this.segFds = new Map();        // segment start -> { fd, size, dirty }
        this.idxFds = new Map();        // index file -> fd, oldest use first (an LRU of appends)
        this.walBuf = Buffer.alloc(1 << 20);
        this.walLen = 0;
        this.walFd = null;
        this.walSeq = 0;
        this.stats = { points: 0, overwritten: 0, late: 0, badType: 0, chunks: 0, chunkBytes: 0, chunkPoints: 0, recovered: 0, lastRefused: null, corruptChunks: 0, unreadableBytes: 0 };
        this._ts = new Float64Array(this.o.chunkPoints);
        this._vs = new Float64Array(this.o.chunkPoints);
        this._rec = Buffer.alloc(RECB);
        this._recF = new Float64Array(this._rec.buffer, this._rec.byteOffset, REC);
        // a closed bucket is written from its own buffer: _rec still holds the summary being rolled up
        this._out = Buffer.alloc(RECB);
        this._outF = new Float64Array(this._out.buffer, this._out.byteOffset, REC);
        this.opened = false;
        // a duration in seconds is a RAM rule's; on disk the least is one segment (1 h): a shorter one is raised (warned)
        this.ruleWarnings = [];
        this.rules = (this.o.rules || []).map((r) => {
            const mem = r.store === 'memory' || r.store === 'ram';
            const dur = (x, d) => (x === undefined || x === null || x === '' ? d : parseDuration(x));
            let keep = dur(r.keep, mem ? 60000 : Infinity), raw = dur(r.raw, undefined);
            const least = mem ? 100 : this.o.segmentMs;
            if (keep < least) { this.ruleWarnings.push((r.pattern || '*') + ': ' + (mem ? 'RAM' : 'Disk') + ' keep ' + r.keep + ' is under ' + (mem ? '100 ms' : '1 h (use RAM for seconds)') + '; ' + least + ' ms used'); keep = least; }
            if (!mem && raw !== undefined && raw < least) { this.ruleWarnings.push((r.pattern || '*') + ': Disk raw ' + r.raw + ' is under 1 h; 1 h used'); raw = least; }
            if (mem && keep > 3600000) this.ruleWarnings.push((r.pattern || '*') + ': RAM for ' + r.keep + ' - lost on a restart or a redeploy of the database node, and it uses memory (about 16 bytes a point)');
            return { re: globRe(r.pattern || '*'), store: mem ? 'memory' : 'disk', keep, raw, max: Math.max(100, Math.round(r.max || 1e6)) };
        });
    }

    // the rule of a tag name (the first match), else the defaults
    _rule(name) {
        for (const r of this.rules) if (r.re.test(name)) return r;
        return { store: 'disk', keep: Infinity, raw: undefined, max: 0 };
    }
    // a tag's keep (everything) and raw (raw points) in ms
    _applyRule(tag) {
        const r = this._rule(tag.name);
        tag.keep = r.keep;
        tag.rawKeep = tag.mem ? r.keep : Math.min(r.raw !== undefined ? r.raw : this.o.rawDays * DAY, r.keep);
        tag.max = r.max;
    }
    // the oldest time a query may return for a tag (raw: from raw points)
    _cut(tag, raw, now) {
        const t = now === undefined ? Date.now() : now, k = raw ? tag.rawKeep : tag.keep;
        return k === Infinity ? -Infinity : t - k;
    }

    // ---- open / close ----------------------------------------------------------------------------------------
    open() {
        for (const d of ['', 'wal', 'seg', 'idx']) fs.mkdirSync(path.join(this.dir, d), { recursive: true });
        this._lock();
        try { this._open(); } catch (e) { this._unlock(); throw e; }   // an open that fails must not keep the folder locked (the worker retries)
        return this;
    }
    _open() {
        require('./admin').recoverOps(this.dir);   // a delete / compact cut short: finished or dropped, before any read
        this._loadTags();
        this._loadDicts();
        this._recover();
        this._openWal();
        this.opened = true;
        // a timer's error goes to onError (the worker restarts the engine); without a handler it is thrown, as before
        const guard = (fn) => () => { try { fn(); } catch (e) { if (this.onError) this.onError(e); else throw e; } };
        this._timers = [
            setInterval(guard(() => { this._beat(); this.flushWal(); }), this.o.walFlushMs),
            setInterval(guard(() => this.checkpoint({ soft: true })), this.o.checkpointMs)
        ];
        this._timers.forEach((t) => t.unref && t.unref());
        this.retention();
    }

    // ---- one engine a folder -----------------------------------------------------------------------------------
    // LOCK holds "pid:token"; its mtime is a heartbeat (every WAL flush). A lock is stale when its process is gone or it has not
    // beat for LOCK_STALE_MS (a pid reused after a container restart). Two engines on one folder would both append to the
    // same WAL, segments and indexes: that is damage, so the second open is refused.
    _lock() {
        if (!this.o.lock) return;
        const f = path.join(this.dir, 'LOCK');
        this._lockToken = process.pid + ':' + Math.random().toString(36).slice(2);
        for (let tries = 0; tries < 3; tries++) {
            try {
                const fd = fs.openSync(f, 'wx');
                try { fs.writeSync(fd, this._lockToken); } finally { fs.closeSync(fd); }
                this._lockFile = f;
                return;
            } catch (e) {
                if (e.code !== 'EEXIST') throw e;
                let owner = '', age = Infinity;
                try { owner = fs.readFileSync(f, 'utf8'); age = Date.now() - fs.statSync(f).mtimeMs; } catch (e2) { continue; }   // gone meanwhile: try again
                const pid = parseInt(owner, 10);
                let alive = age < LOCK_STALE_MS && !ABANDONED.has(owner);
                if (alive && pid !== process.pid) { try { process.kill(pid, 0); } catch (e3) { alive = e3.code === 'EPERM'; } }
                if (alive) { const err = new Error('the database folder ' + this.dir + ' is open in another engine (LOCK held by process ' + pid + (pid === process.pid ? ', this one' : '') + '); two engines on one folder would damage it'); err.code = 'ETSDB_LOCKED'; throw err; }
                try { fs.unlinkSync(f); } catch (e4) { /* taken by another meanwhile */ }
            }
        }
        const err = new Error('could not take the LOCK of ' + this.dir); err.code = 'ETSDB_LOCKED'; throw err;
    }
    _beat() { if (this._lockFile) { const t = new Date(); try { fs.utimesSync(this._lockFile, t, t); } catch (e) { /* the next beat */ } } }
    _unlock() {
        if (!this._lockFile) return;
        // an engine stopped because of an I/O error may not be able to delete its LOCK either: the file is then marked as its own
        // leftover, so the engine that replaces it (the worker restarts at once) is not refused by it
        try { if (fs.readFileSync(this._lockFile, 'utf8') === this._lockToken) fs.unlinkSync(this._lockFile); } catch (e) { if (e.code !== 'ENOENT') ABANDONED.add(this._lockToken); }
        this._lockFile = null;
    }

    /** The points the WAL has not got yet (written, in the buffer): [{ name, ts, value }]. Taken before abort() so a restart loses none. */
    pending() {
        const out = [];
        for (let o = 0; o + WALB <= this.walLen; o += WALB) {
            const tag = this.tags[this.walBuf.readUInt32LE(o)];
            if (!tag) continue;
            const v = this.walBuf.readDoubleLE(o + 12);
            out.push({ name: tag.name, ts: this.walBuf.readDoubleLE(o + 4), value: tag.type === 'number' ? v : tag.type === 'bool' ? v !== 0 : tag.words[v] });
        }
        return out;
    }
    /** A point of pending(), into a restarted engine: what the WAL replay already restored is skipped, not counted as late. */
    replayPoint(name, ts, value) {
        const tag = this.byName.get(name);
        if (tag && tag.lastT >= ts && !(tag.lastT === ts && tag.n > tag.m0 && tag.ts[tag.n - 1] === ts)) return false;
        return this.write(name, ts, value);
    }

    /** Stop as a crash does: no checkpoint, no write; every file closed. For a historian that met an I/O error: the next open recovers from the WAL. */
    abort() {
        (this._timers || []).forEach(clearInterval);
        const quiet = (fd) => { try { fs.closeSync(fd); } catch (e) { /* gone */ } };
        if (this.walFd !== null) { quiet(this.walFd); this.walFd = null; }
        this.segFds.forEach((x) => quiet(x.fd)); this.segFds.clear();
        this.idxFds.forEach(quiet); this.idxFds.clear();
        this.opened = false;
        this._unlock();
    }

    close() {
        if (!this.opened) return;
        (this._timers || []).forEach(clearInterval);
        this.checkpoint();
        if (this.walFd !== null) { fs.closeSync(this.walFd); this.walFd = null; }
        this.segFds.forEach((s) => fs.closeSync(s.fd));
        this.segFds.clear();
        this.closeIdx();
        this.opened = false;
        this._unlock();
    }

    // ---- tags and dictionaries --------------------------------------------------------------------------------
    _loadTags() {
        const f = path.join(this.dir, 'tags.log');
        if (!fs.existsSync(f)) return;
        for (const line of fs.readFileSync(f, 'utf8').split('\n')) {
            if (!line.trim()) continue;
            let t;
            try { t = JSON.parse(line); } catch (e) { continue; }   // a torn last line
            if (t.drop !== undefined) { const d = this.tags[t.drop]; if (d) { this.byName.delete(d.name); this.tags[t.drop] = null; } continue; }
            this._addTag(t.id, t.name, t.type);
        }
    }
    _addTag(id, name, type, mem) {
        // a head starts small and doubles up to a chunk (100 000 tags must not hold 1 024 points each up front)
        const n = mem ? Math.min(1024, this._rule(name).max) : Math.min(16, this.o.chunkPoints);
        // m0: the first live point (a memory ring drops from the front); 0 for a disk tag
        const tag = { id, name, type, mem: !!mem, ts: new Float64Array(n), vs: new Float64Array(n), n: 0, m0: 0, seg: 0, lastT: -Infinity,
            acc: [null, new Float64Array(REC + 1), new Float64Array(REC + 1)], dict: null, words: null, span: 0,
            written: 0, overwritten: 0, late: 0, badType: 0, refused: null, lastAt: 0, lastV: NaN, lastKnown: false,
            w0: Infinity };    // w0: the WAL file that holds the oldest point of the open chunk (that file stays until the chunk is written)
        this._applyRule(tag);
        tag.acc[1][REC] = NaN; tag.acc[2][REC] = NaN;      // [REC] = the bucket start (NaN: empty)
        if (type === 'string') { tag.dict = new Map(); tag.words = []; }
        this.tags[id] = tag;
        this.byName.set(name, tag);
        return tag;
    }
    _appendDurable(file, text) {
        const fd = fs.openSync(path.join(this.dir, file), 'a');
        try { fs.writeSync(fd, text); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    }
    // new tags and new dictionary words: written with one fsync a file per batch, before the WAL records (and any chunk)
    // that use their ids (100 000 new tags must not cost 100 000 fsyncs)
    _flushMeta() {
        if (this._newTags) { this._appendDurable('tags.log', this._newTags); this._newTags = ''; }
        if (this._newWords) { this._appendDurable('dict.log', this._newWords); this._newWords = ''; }
    }
    _tag(name, type) {
        let tag = this.byName.get(name);
        if (tag) return tag;
        const mem = this._rule(name).store === 'memory';
        tag = this._addTag(this.tags.length, name, type, mem);
        if (!mem) this._newTags = (this._newTags || '') + JSON.stringify({ id: tag.id, name, type }) + '\n';
        return tag;
    }
    _loadDicts() {
        const f = path.join(this.dir, 'dict.log');
        if (!fs.existsSync(f)) return;
        for (const line of fs.readFileSync(f, 'utf8').split('\n')) {
            if (!line.trim()) continue;
            let r;
            try { r = JSON.parse(line); } catch (e) { continue; }
            const tag = this.tags[r[0]];
            if (tag && tag.dict) { tag.dict.set(r[2], r[1]); tag.words[r[1]] = r[2]; }
        }
    }
    _wordId(tag, s) {
        let id = tag.dict.get(s);
        if (id !== undefined) return id;
        id = tag.words.length;
        tag.dict.set(s, id); tag.words[id] = s;
        if (!tag.mem) this._newWords = (this._newWords || '') + JSON.stringify([tag.id, id, s]) + '\n';
        return id;
    }

    /** Per tag: what it received and what it refused (problems first), with the last refusal. */
    diagnose(patterns, now) {
        const t = now === undefined ? Date.now() : now;
        const list = (patterns ? require('./query').matchTags(this, patterns) : this.tags.filter(Boolean)).map((g) => ({
            tag: g.name, type: g.type, store: g.mem ? 'memory' : 'disk', written: g.written, overwritten: g.overwritten, late: g.late, badType: g.badType,
            refused: g.late + g.badType, lastRefused: g.refused, lastTs: g.lastT === -Infinity ? null : g.lastT,
            lastWriteAgoMs: g.lastAt ? t - g.lastAt : null, inMemory: g.n - g.m0
        }));
        return list.sort((a, b) => b.refused - a.refused || a.tag.localeCompare(b.tag));
    }

    tagList() {
        return this.tags.filter(Boolean).map((t) => ({ id: t.id, name: t.name, type: t.type, store: t.mem ? 'memory' : 'disk', keep: t.keep === Infinity ? null : t.keep, raw: t.rawKeep === Infinity ? null : t.rawKeep, last: t.lastT === -Infinity ? null : t.lastT }));
    }

    // ---- writing ---------------------------------------------------------------------------------------------
    /** A point. value: number | boolean | string (a tag's type is its first value's). false: rejected (late / type). */
    write(name, ts, value) {
        const type = typeof value === 'boolean' ? 'bool' : typeof value === 'string' ? 'string' : 'number';
        if (type === 'number' && typeof value !== 'number') { this.stats.badType++; return false; }
        const tag = this._tag(String(name), type);
        const t = Math.round(+ts);
        if (tag.type !== type) { this.stats.badType++; tag.badType++; this._refuse(tag, t, 'wrong type: a ' + type + ' to a ' + tag.type + ' tag', value); return false; }
        if (!Number.isFinite(t) || t < this.o.minTs) { this.stats.late++; tag.late++; this._refuse(tag, t, 'not a valid time: ' + ts, value); return false; }
        if (this.o.maxFutureMs && t > Date.now() + this.o.maxFutureMs) { this.stats.late++; tag.late++; this._refuse(tag, t, 'more than ' + Math.round(this.o.maxFutureMs / 3600000) + ' h ahead of the clock: ' + (t < 8.64e15 ? new Date(t).toISOString() : t), value); return false; }
        if (type === 'number' && value !== value) { this.stats.badType++; tag.badType++; this._refuse(tag, t, 'NaN is not a value (it would count in an average and add nothing)', value); return false; }
        if (t < tag.lastT) { this.stats.late++; tag.late++; this._refuse(tag, t, 'older than the last point (' + new Date(tag.lastT).toISOString() + ')', value); return false; }
        const v = type === 'number' ? value : type === 'bool' ? (value ? 1 : 0) : this._wordId(tag, value);
        tag.lastAt = Date.now();
        // the same time as the last point: its value is replaced (the last point stays in memory until a newer one)
        if (t === tag.lastT) {
            if (!(tag.n > tag.m0 && tag.ts[tag.n - 1] === t)) { this.stats.late++; tag.late++; this._refuse(tag, t, 'the same time as a point already in a chunk', value); return false; }
            tag.vs[tag.n - 1] = v; tag.lastV = v;
            if (!tag.mem) this._walPut(tag.id, t, v);
            this.stats.overwritten++; tag.overwritten++;
            return true;
        }
        tag.written++;
        if (tag.mem) { this._pushMem(tag, t, v); return true; }
        this._walPut(tag.id, t, v);
        this._push(tag, t, v);
        return true;
    }

    // a refused point: counted on its tag, the last one kept (tag + database) for the diagnostics
    _refuse(tag, t, reason, value) {
        const r = { tag: tag.name, ts: t, lastTs: tag.lastT === -Infinity ? null : tag.lastT, reason, value: typeof value === 'object' ? String(value) : value, at: Date.now() };
        tag.refused = r;
        this.stats.lastRefused = r;
    }

    // a memory tag: a ring of its last `keep` (at most `max` points); grows by doubling, compacts in place
    _pushMem(tag, t, v) {
        if (tag.n === tag.ts.length) {
            if (tag.m0 > 0) { tag.ts.copyWithin(0, tag.m0, tag.n); tag.vs.copyWithin(0, tag.m0, tag.n); tag.n -= tag.m0; tag.m0 = 0; }
            else if (tag.ts.length < tag.max) {
                const size = Math.min(tag.max, tag.ts.length * 2), ts = new Float64Array(size), vs = new Float64Array(size);
                ts.set(tag.ts); vs.set(tag.vs); tag.ts = ts; tag.vs = vs;
            } else { tag.ts.copyWithin(0, 1, tag.n); tag.vs.copyWithin(0, 1, tag.n); tag.n--; }
        }
        tag.ts[tag.n] = t; tag.vs[tag.n] = v; tag.n++;
        tag.lastT = t; tag.lastV = v; tag.lastKnown = true;
        const cut = t - tag.keep;
        while (tag.m0 < tag.n && tag.ts[tag.m0] < cut) tag.m0++;
        this.stats.points++;
    }

    _push(tag, t, v) {
        const seg = Math.floor(t / this.o.segmentMs) * this.o.segmentMs;
        if (tag.n && (seg !== tag.seg || tag.n === this.o.chunkPoints)) this._flushHead(tag);
        if (!tag.n) { tag.seg = seg; tag.w0 = this._replaySeq || this.walSeq; }
        if (tag.n === tag.ts.length) {
            const size = Math.min(this.o.chunkPoints, tag.ts.length * 2), ts = new Float64Array(size), vs = new Float64Array(size);
            ts.set(tag.ts.subarray(0, tag.n)); vs.set(tag.vs.subarray(0, tag.n)); tag.ts = ts; tag.vs = vs;
        }
        tag.ts[tag.n] = t; tag.vs[tag.n] = v; tag.n++;
        tag.lastT = t; tag.lastV = v; tag.lastKnown = true;
        this.stats.points++;
    }

    _walPut(id, t, v) {
        if (this.walLen + WALB > this.walBuf.length) this.flushWal();
        this.walBuf.writeUInt32LE(id, this.walLen);
        this.walBuf.writeDoubleLE(t, this.walLen + 4);
        this.walBuf.writeDoubleLE(v, this.walLen + 12);
        this.walLen += WALB;
    }

    flushWal() {
        testFault('flushWal');
        this._flushMeta();
        if (this.walFd === null || !this.walLen) return;
        fs.writeSync(this.walFd, this.walBuf, 0, this.walLen);
        if (this.o.walSync) fs.fdatasyncSync(this.walFd);
        this.walLen = 0;
    }

    _openWal() {
        const files = this._walFiles();
        this.walSeq = files.length ? files[files.length - 1] + 1 : 1;
        this.walFd = fs.openSync(path.join(this.dir, 'wal', pad(this.walSeq) + '.wal'), 'a');
    }
    _walFiles() { return fs.readdirSync(path.join(this.dir, 'wal')).filter((f) => f.endsWith('.wal')).map((f) => parseInt(f, 10)).sort((a, b) => a - b); }

    // an open chunk -> its segment file, its level 0 summary, the levels above
    _flushHead(tag, keepLast) {
        const n = keepLast ? tag.n - 1 : tag.n;
        if (n <= 0) return;
        if (this._newTags || this._newWords) this._flushMeta();   // a chunk never names a tag the log does not have
        const ch = chunk.build(tag.id, n, gorilla.encode(tag.ts, tag.vs, n));
        const s = this._segFd(tag.seg);
        const off = s.size;
        fs.writeSync(s.fd, ch, 0, ch.length, off);
        s.size += ch.length; s.dirty = true;
        const r = this._recF;
        summarize(tag.ts, tag.vs, n, r);
        r[F.seg] = tag.seg; r[F.off] = off;
        this._appendIdx(this._idx(tag, 0), this._rec);
        tag.span = r[F.tLast] - r[F.tFirst];
        this._roll(tag, r);
        this.stats.chunks++; this.stats.chunkBytes += ch.length; this.stats.chunkPoints += n;
        if (keepLast) { tag.ts[0] = tag.ts[n]; tag.vs[0] = tag.vs[n]; tag.n = 1; } else tag.n = 0;
    }

    // a level 0 summary into the open level 1 / 2 buckets; a bucket that ends is written
    _roll(tag, r) {
        for (let l = 1; l < LEVELS; l++) {
            const size = l === 1 ? this.o.segmentMs : DAY, acc = tag.acc[l], b = Math.floor(r[F.tFirst] / size) * size;
            if (acc[REC] === acc[REC] && acc[REC] !== b) this._closeBucket(tag, l);
            if (acc[REC] !== acc[REC]) { emptyAcc(acc); acc[REC] = b; }
            merge(acc, r);
        }
    }
    _closeBucket(tag, l) {
        const acc = tag.acc[l];
        this._outF.set(acc.subarray(0, REC));
        this._appendIdx(this._idx(tag, l), this._out);
        acc[REC] = NaN;
    }

    _segFd(seg) {
        let s = this.segFds.get(seg);
        if (s) { this.segFds.delete(seg); this.segFds.set(seg, s); return s; }   // the newest use last
        // past the limit the oldest use goes: fsynced first (a checkpoint cannot fsync a file it no longer holds)
        while (this.segFds.size >= this.o.segFds) {
            const [old, o] = this.segFds.entries().next().value;
            if (o.dirty) fs.fsyncSync(o.fd);
            fs.closeSync(o.fd);
            this.segFds.delete(old);
        }
        const f = path.join(this.dir, 'seg', pad(seg) + '.seg');
        const fd = fs.openSync(f, fs.existsSync(f) ? 'r+' : 'w+');
        s = { fd, size: fs.fstatSync(fd).size, dirty: false };
        this.segFds.set(seg, s);
        return s;
    }
    _idx(tag, l) { return path.join(this.dir, 'idx', tag.id + '.r' + l); }

    // an append to an index file through the LRU of open files
    _appendIdx(file, buf) {
        let fd = this.idxFds.get(file);
        if (fd === undefined) {
            if (this.idxFds.size >= this.o.idxFds) { const [old, ofd] = this.idxFds.entries().next().value; fs.closeSync(ofd); this.idxFds.delete(old); }
            fd = fs.openSync(file, 'a');
        } else this.idxFds.delete(file);
        this.idxFds.set(file, fd);
        fs.writeSync(fd, buf);
    }
    /** Close the index files kept open (before one is renamed / deleted / truncated: Windows refuses that on an open file). */
    closeIdx() { this.idxFds.forEach((fd) => fs.closeSync(fd)); this.idxFds.clear(); }

    /**
     * Write the open chunks, fsync, drop the WAL they covered.
     * Called as it is (close, an admin operation, a client's checkpoint) every open chunk is written. The timer's checkpoint
     * ({ soft: true }) leaves a young small chunk open: a tag that writes slowly would otherwise get a chunk of one point (37 bytes
     * and a 96-byte summary for 8 bytes of data) at every checkpoint. Its points are in the WAL, and the WAL files that hold them
     * are kept until the chunk is written (tag.w0): a crash replays them, as it replays any WAL.
     */
    checkpoint(opts) {
        if (!this.opened) return;
        const soft = !!(opts && opts.soft);
        this.flushWal();
        // the points so far are in the WAL files up to this one; new points go to a new file
        const covered = this.walSeq;
        if (this.walFd !== null) fs.closeSync(this.walFd);
        this.walSeq++;
        this.walFd = fs.openSync(path.join(this.dir, 'wal', pad(this.walSeq) + '.wal'), 'a');
        // a recent last point stays in memory (it may still be replaced) and goes to the new WAL; an old one is final
        const now = Date.now(), recent = now - this.o.segmentMs, curSeg = Math.floor(now / this.o.segmentMs) * this.o.segmentMs;
        let floor = Infinity;                                        // the oldest WAL file still holding a point that is in no chunk
        for (const tag of this.tags) {
            if (!tag || !tag.n || tag.mem) continue;
            if (soft && tag.seg >= curSeg && tag.n < this.o.chunkMinPoints && now - tag.ts[0] < this.o.maxChunkAgeMs) { floor = Math.min(floor, tag.w0); continue; }
            const keep = tag.ts[tag.n - 1] >= recent;
            this._flushHead(tag, keep);
            if (keep) { this._walPut(tag.id, tag.ts[0], tag.vs[0]); tag.w0 = this.walSeq; floor = Math.min(floor, tag.w0); } else tag.w0 = Infinity;
        }
        this.flushWal();
        const nowSeg = Math.floor(Date.now() / this.o.segmentMs) * this.o.segmentMs;
        this.segFds.forEach((s, seg) => {
            if (s.dirty) { fs.fsyncSync(s.fd); s.dirty = false; }
            if (seg < nowSeg - this.o.segmentMs) { fs.closeSync(s.fd); this.segFds.delete(seg); }
        });
        for (const n of this._walFiles()) if (n <= covered && n < floor) fs.unlinkSync(path.join(this.dir, 'wal', pad(n) + '.wal'));
    }

    // ---- recovery --------------------------------------------------------------------------------------------
    _segFiles() { return fs.readdirSync(path.join(this.dir, 'seg')).filter((f) => f.endsWith('.seg')).map((f) => parseInt(f, 10)).sort((a, b) => a - b); }

    _recover() {
        const segs = this._segFiles();
        const from = segs.length > 1 ? segs[segs.length - 2] : segs.length ? segs[0] : Infinity;
        // only the last two segments are checked against the index: stat only those (every file's stat is seconds of an open)
        const sizes = new Map(segs.filter((s) => s >= from).map((s) => [s, fs.statSync(path.join(this.dir, 'seg', pad(s) + '.seg')).size]));
        for (const tag of this.tags) {
            if (!tag) continue;
            // level 0: only the tail is looked at (the whole file is not read to open): no torn or zero-filled record
            // (what a power cut leaves), nothing out of order, nothing that points past what the last segments hold
            const f0 = this._idx(tag, 0);
            const tail = readTail(f0, 256), recs = tail.recs;
            let keep = recs.length / REC;
            while (keep > 0) {
                const r = recs.subarray((keep - 1) * REC, keep * REC), prev = keep > 1 ? recs[(keep - 2) * REC + F.tLast] : undefined;
                if (!sane(r, prev, true)) { keep--; continue; }
                if (r[F.seg] >= from && !this._chunkOk(r[F.seg], r[F.off], tag.id, sizes)) { keep--; continue; }
                break;
            }
            const total = tail.total - (recs.length / REC - keep);
            if (total !== tail.total || tail.ragged) truncateRecords(f0, total);
            tag._last0 = keep ? recs.subarray((keep - 1) * REC, keep * REC).slice() : null;
        }
        // chunks in the last two segments the index does not have yet
        for (const seg of segs.filter((s) => s >= from)) {
            const file = path.join(this.dir, 'seg', pad(seg) + '.seg'), fd = fs.openSync(file, 'r'), size = sizes.get(seg);
            let off = 0, cut = null;
            try {
                while (off + 16 <= size) {
                    const c = this._chunkAt(fd, off);
                    if (!c) break;                                            // not a chunk header: zeros or garbage (below)
                    if (c.torn) { cut = off; break; }                         // the file ends inside this chunk
                    if (!c.ok) {
                        if (off + c.h.total >= size) { cut = off; break; }    // the last chunk, its bytes not (all) on disk: torn
                        this.stats.corruptChunks++; off += c.h.total; continue;   // damaged in the middle: not indexed, left for verify
                    }
                    const tag = this.tags[c.h.id], last = tag && tag._last0;
                    if (tag && (!last || seg > last[F.seg] || (seg === last[F.seg] && off > last[F.off]))) {
                        gorilla.decode(c.body, c.h.n, this._ts, this._vs);
                        summarize(this._ts, this._vs, c.h.n, this._recF);
                        this._recF[F.seg] = seg; this._recF[F.off] = off;
                        this._appendIdx(this._idx(tag, 0), this._rec);
                        tag._last0 = this._recF.slice();
                        this.stats.recovered++;
                    }
                    off += c.h.total;
                }
            } finally { fs.closeSync(fd); }
            // a torn chunk at the end (its points are in the WAL) or a zero-filled tail (what a power cut leaves): cut.
            // Anything else after the last chunk is left alone and counted (never cut away what may be data).
            if (cut !== null) fs.truncateSync(file, cut);
            else if (off < size) {
                const z = Buffer.alloc(Math.min(4096, size - off)), f2 = fs.openSync(file, 'r');
                try { fs.readSync(f2, z, 0, z.length, off); } finally { fs.closeSync(f2); }
                if (z.every((b) => b === 0)) fs.truncateSync(file, off); else this.stats.unreadableBytes += size - off;
            }
        }
        // levels 1 / 2: rebuilt from `from` on; the open buckets in memory again
        for (const tag of this.tags) {
            if (!tag) continue;
            const last = tag._last0;
            tag.lastT = last ? last[F.tLast] : -Infinity;
            if (last) { tag.lastV = last[F.vLast]; tag.lastKnown = true; }
            if (last) tag.span = last[F.tLast] - last[F.tFirst];
            this._rebuildLevels(tag);
            delete tag._last0;
        }
        // the WAL: every point not in a chunk yet
        const walDir = path.join(this.dir, 'wal');
        for (const n of this._walFiles()) {
            const buf = fs.readFileSync(path.join(walDir, pad(n) + '.wal'));
            this._replaySeq = n;
            for (let o = 0; o + WALB <= buf.length; o += WALB) {
                const tag = this.tags[buf.readUInt32LE(o)];
                const t = buf.readDoubleLE(o + 4), v = buf.readDoubleLE(o + 12);
                if (!tag || !(t >= this.o.minTs)) continue;
                if (t > tag.lastT) { this._push(tag, t, v); this.stats.recovered++; }
                else if (t === tag.lastT && tag.n && tag.ts[tag.n - 1] === t) { tag.vs[tag.n - 1] = v; tag.lastV = v; }
            }
        }
        this._replaySeq = 0;
        // the replayed WAL files stay until the first checkpoint (it writes their points into chunks, then drops them)
    }
    // Levels 1 and 2 of a tag, from its own level 0, where the files stop. A bucket closes only when a later chunk of the tag arrives:
    // at a crash the bucket of its last chunk is open (in memory), and a crash between a chunk and the closing of the bucket before it
    // leaves that one missing too. So, for each level: the valid prefix of the file is kept (a zero-filled or out of order record, what a
    // power cut leaves, is dropped); the chunks after the last bucket in the file are grouped; every group but the last is appended, the
    // last is the open bucket again. (A tag that stopped writing hours ago is rebuilt like any other: this does not look at the segments.)
    _rebuildLevels(tag) {
        for (let l = 1; l < LEVELS; l++) {
            const f = this._idx(tag, l), size = l === 1 ? this.o.segmentMs : DAY;
            const tail = readTail(f, 512), recs = tail.recs, had = recs.length / REC;
            let keep = had;
            while (keep > 0) {
                const r = recs.subarray((keep - 1) * REC, keep * REC), prev = keep > 1 ? recs[(keep - 2) * REC + F.tLast] : undefined;
                if (!sane(r, prev, false)) { keep--; continue; }
                break;
            }
            const total = tail.total - (had - keep);
            if (total !== tail.total || tail.ragged) truncateRecords(f, total);
            const from0 = keep ? Math.floor(recs[(keep - 1) * REC + F.tFirst] / size) * size + size : -Infinity;
            const r0 = readRange(this._idx(tag, 0), from0, Infinity), groups = [];
            for (let i = 0; i < r0.length; i += REC) {
                const r = r0.subarray(i, i + REC);
                if (r[F.tFirst] < from0) continue;
                const start = Math.floor(r[F.tFirst] / size) * size;
                if (!groups.length || groups[groups.length - 1][REC] !== start) {
                    const a = emptyAcc(new Float64Array(REC + 1)); a[REC] = start;
                    groups.push(a);
                }
                merge(groups[groups.length - 1], r);
            }
            if (groups.length > 1) {
                const buf = Buffer.alloc((groups.length - 1) * RECB);
                for (let k = 0; k < groups.length - 1; k++) Buffer.from(groups[k].buffer, groups[k].byteOffset, RECB).copy(buf, k * RECB);
                fs.appendFileSync(f, buf);
            }
            if (groups.length) tag.acc[l] = groups[groups.length - 1];
        }
    }


    _chunkOk(seg, off, id, sizes) {
        const size = sizes.get(seg);
        if (size === undefined || off + 16 > size) return false;
        const fd = fs.openSync(path.join(this.dir, 'seg', pad(seg) + '.seg'), 'r'), head = Buffer.alloc(16);
        try { fs.readSync(fd, head, 0, 16, off); } finally { fs.closeSync(fd); }
        const h = chunk.parse(head);
        return !!h && h.id === id && off + h.total <= size;
    }

    // the chunk at an offset of an open segment, in one read when it is small: { h, all, body, ok } | { h, torn } | null (no header)
    _chunkAt(fd, off) {
        const buf = this._rb || (this._rb = Buffer.allocUnsafe(RBUF));
        const got = fs.readSync(fd, buf, 0, RBUF, off);
        if (got < 16) return null;
        const h = chunk.parse(buf);
        if (!h) return null;
        let all;
        if (h.total <= got) all = buf.subarray(0, h.total);
        else { all = Buffer.allocUnsafe(h.total); if (fs.readSync(fd, all, 0, h.total, off) < h.total) return { h, torn: true }; }
        return { h, all, body: all.subarray(16, 16 + h.len), ok: chunk.intact(all, h) };
    }

    // what a read says when stored data is damaged: loud, never a wrong value
    _corrupt(tag, seg, off, why) {
        this.stats.corruptChunks++;
        const e = new Error('corrupt data: tag "' + tag.name + '", segment ' + new Date(seg).toISOString() + ', offset ' + off + ': ' + why + ' (run { op: "verify" }; { op: "verify", repair: true } drops the damaged chunks)');
        e.code = 'ETSDB_CORRUPT';
        return e;
    }

    // ---- retention -------------------------------------------------------------------------------------------
    // budgetMs: the time this pass may spend compacting index files (the worker's hourly pass: a big database is done over several
    // passes, tag after tag, so writes and queries are never held for minutes); without it, everything due is done now
    retention(now, budgetMs) {
        const t = now === undefined ? Date.now() : now, started = Date.now();
        this.closeIdx();
        // a segment goes when it is past every disk tag's raw keep (the default rawDays when there is no tag yet)
        let longest = 0;
        for (const tag of this.tags) if (tag && !tag.mem) longest = Math.max(longest, tag.rawKeep);
        const rawCut = t - (longest || this.o.rawDays * DAY);
        for (const seg of this._segFiles()) {
            if (seg + this.o.segmentMs > rawCut || this.segFds.has(seg)) continue;
            fs.unlinkSync(path.join(this.dir, 'seg', pad(seg) + '.seg'));
        }
        const count = this.tags.length, first = count ? (this._retCursor || 0) % count : 0;
        this._retCursor = 0;
        for (let k = 0; k < count; k++) {
            const at = (first + k) % count, tag = this.tags[at];
            if (!tag || tag.mem) continue;
            if (budgetMs !== undefined && k > 0 && Date.now() - started > budgetMs) { this._retCursor = at; break; }   // the next pass goes on from here
            // per-chunk summaries: indexDays (never past keep); hour / day summaries: keep (for ever by default)
            compactIdx(this._idx(tag, 0), t - Math.min(tag.keep, this.o.indexDays * DAY));
            if (tag.keep !== Infinity) { compactIdx(this._idx(tag, 1), t - tag.keep); compactIdx(this._idx(tag, 2), t - tag.keep); }
        }
        // a memory tag: its ring past keep (a tag that stopped writing)
        for (const tag of this.tags) if (tag && tag.mem) { const cut = t - tag.keep; while (tag.m0 < tag.n && tag.ts[tag.m0] < cut) tag.m0++; }
    }

    // ---- reading ---------------------------------------------------------------------------------------------
    /** The raw points of a tag in [from, to] (chunks + the open one): { t: Float64Array, v: Float64Array }. */
    raw(tag, fromArg, to, limit) {
        const max = limit || 1e7, from = Math.max(fromArg, this._cut(tag, true));
        // the points go straight into growing Float64Arrays (a JS array of millions of numbers, then a copy, costs 3 - 4 times the memory)
        let cap = 4096, ts = new Float64Array(cap), vs = new Float64Array(cap), len = 0;
        const push = (t, v) => {
            if (len === cap) { cap *= 2; const a = new Float64Array(cap), b = new Float64Array(cap); a.set(ts); b.set(vs); ts = a; vs = b; }
            ts[len] = t; vs[len] = v; len++;
        };
        let n = 0;
        if (!tag.mem && from <= to) {
            const recs = readRange(this._idx(tag, 0), from, to), fds = new Map();
            try {
                for (let i = 0; i < recs.length && n < max; i += REC) n = this._decodeInto(tag, recs.subarray(i, i + REC), from, to, push, n, max, fds);
            } finally { fds.forEach((fd) => { if (fd !== null) fs.closeSync(fd); }); }
        }
        for (let i = tag.m0; i < tag.n && n < max; i++) { const t = tag.ts[i]; if (t >= from && t <= to) { push(t, tag.vs[i]); n++; } }
        return { t: ts.subarray(0, len), v: vs.subarray(0, len) };
    }

    _decodeInto(tag, r, from, to, fn, n, max, fds) {
        const seg = r[F.seg], off = r[F.off];
        let fd = fds ? fds.get(seg) : undefined;
        if (fd !== undefined) { if (fds && fd !== null) { fds.delete(seg); fds.set(seg, fd); } }   // the newest use last
        else {
            if (fds) {
                // a query that walks years touches thousands of hourly segments: only the last QFDS stay open
                let open = 0;
                fds.forEach((x) => { if (x !== null) open++; });
                while (open >= QFDS) { for (const [k, x] of fds) if (x !== null) { fs.closeSync(x); fds.delete(k); open--; break; } }
            }
            try { fd = fs.openSync(path.join(this.dir, 'seg', pad(seg) + '.seg'), 'r'); }
            catch (e) { if (e.code === 'ENOENT') { if (fds) fds.set(seg, null); return n; } throw e; }   // raw past its retention: the summaries remain
            if (fds) fds.set(seg, fd);
        }
        if (fd === null) return n;
        // the header and (nearly always) the whole chunk in one read: a chunk of 60 - 1 024 points is 100 bytes - 9 KB
        try {
            const c = this._chunkAt(fd, off);
            if (!c || c.torn) throw this._corrupt(tag, seg, off, 'no complete chunk where the index says');
            if (!c.ok) throw this._corrupt(tag, seg, off, 'checksum mismatch');
            const cnt = c.h.n;
            if (c.h.id !== tag.id || cnt !== r[F.count]) throw this._corrupt(tag, seg, off, 'not the chunk its index record describes');
            gorilla.decode(c.body, cnt, this._ts, this._vs);
            const bad = chunkProblem(this._ts, this._vs, cnt, r, c.h.ver, this._chk || (this._chk = new Float64Array(REC)));
            if (bad) throw this._corrupt(tag, seg, off, bad);
            for (let i = 0; i < cnt && n < max; i++) { const t = this._ts[i]; if (t >= from && t <= to) { fn(t, this._vs[i]); n++; } }
        } finally { if (!fds) fs.closeSync(fd); }
        return n;
    }

    /**
     * Buckets of [from, to] of `size` ms (aligned to origin): per bucket first / last / min / max with their times, sum,
     * count. A summary is used whole when it lies in one bucket; else the level under it is read for its span, down to
     * the raw points. ext: the aggregates that need the points in time order (lib/rollup.js).
     */
    buckets(tag, from, to, size, origin, ext) {
        const o = origin === undefined ? from : origin;
        const b0 = Math.floor((from - o) / size), nb = Math.max(1, Math.floor((to - o) / size) - b0 + 1);
        if (nb > 5e6) throw new Error('too many buckets: ' + nb);
        const acc = new Float64Array(nb * 10).fill(NaN);
        for (let b = 0; b < nb; b++) { acc[b * 10 + 8] = 0; acc[b * 10 + 9] = 0; }
        const idxOf = (t) => Math.floor((t - o) / size) - b0;
        // ext: the aggregates that need the points in time order (lib/rollup.js): delta, increase, integral, states
        const sa = ext ? new (require('./rollup').SeriesAgg)({ nb, origin: o, size, b0, flags: ext.flags, policy: ext.policy, target: ext.target }) : null;
        const addPoint0 = (t, v) => {
            const b = idxOf(t);
            if (b < 0 || b >= nb) return;
            const a = b * 10;
            if (!(acc[a + 9] > 0)) { acc[a] = t; acc[a + 1] = v; acc[a + 2] = t; acc[a + 3] = v; acc[a + 4] = t; acc[a + 5] = v; acc[a + 6] = t; acc[a + 7] = v; }
            else {
                if (t < acc[a]) { acc[a] = t; acc[a + 1] = v; }
                if (t > acc[a + 2]) { acc[a + 2] = t; acc[a + 3] = v; }
                if (v < acc[a + 5]) { acc[a + 4] = t; acc[a + 5] = v; }
                if (v > acc[a + 7]) { acc[a + 6] = t; acc[a + 7] = v; }
            }
            acc[a + 8] += v; acc[a + 9]++;
        };
        const addPoint = sa ? (t, v) => { addPoint0(t, v); sa.point(t, v); } : addPoint0;
        const addRec0 = (r) => {
            const b = idxOf(r[F.tFirst]), a = b * 10;
            if (b < 0 || b >= nb) return;
            if (!(acc[a + 9] > 0)) {
                acc[a] = r[F.tFirst]; acc[a + 1] = r[F.vFirst]; acc[a + 2] = r[F.tLast]; acc[a + 3] = r[F.vLast];
                acc[a + 4] = r[F.tMin]; acc[a + 5] = r[F.vMin]; acc[a + 6] = r[F.tMax]; acc[a + 7] = r[F.vMax];
            } else {
                if (r[F.tFirst] < acc[a]) { acc[a] = r[F.tFirst]; acc[a + 1] = r[F.vFirst]; }
                if (r[F.tLast] > acc[a + 2]) { acc[a + 2] = r[F.tLast]; acc[a + 3] = r[F.vLast]; }
                if (r[F.vMin] < acc[a + 5]) { acc[a + 4] = r[F.tMin]; acc[a + 5] = r[F.vMin]; }
                if (r[F.vMax] > acc[a + 7]) { acc[a + 6] = r[F.tMax]; acc[a + 7] = r[F.vMax]; }
            }
            acc[a + 8] += r[F.sum]; acc[a + 9] += r[F.count];
        };
        const addRec = sa ? (r) => { addRec0(r); sa.rec(r); } : addRec0;
        const span = tag.span || this.o.segmentMs / 10;
        const start = tag.mem || (sa && sa.p.custom) ? -1 : size >= DAY ? 2 : size >= this.o.segmentMs ? 1 : size >= span ? 0 : -1;
        // nothing past the tag's keep; raw points not past its raw keep (their summaries still answer)
        const lo = Math.max(from, this._cut(tag, false));
        // one query: each level's records for the whole range read once, each segment opened once
        const sink = { addPoint, addRec, fits: sa ? (r) => idxOf(r[F.tFirst]) === idxOf(r[F.tLast]) && sa.accepts(r) : (r) => idxOf(r[F.tFirst]) === idxOf(r[F.tLast]), levels: [], fds: new Map(), from: lo, to, rawCut: this._cut(tag, true) };
        // the bridge of the first bucket starts at the newest point before the range
        if (sa && lo <= to) {
            let sp = this.pointBefore(tag, lo);
            // ignoreZero: the zeros before the range are missing readings too (up to 200 of them are stepped over)
            for (let k = 0; sp && sa.p.ignoreZero && sp.v === 0 && k < 200; k++) sp = this.pointBefore(tag, sp.t);
            sa.seed(sp);
        }
        try { if (lo <= to) this._scan(tag, start, lo, to, sink); } finally { sink.fds.forEach((fd) => { if (fd !== null) fs.closeSync(fd); }); }
        return { acc, nb, start: o + b0 * size, size, sa };
    }

    /** The newest point strictly before t: { t, v } | null (not past the tag's keep). From the open chunk, else the summaries (a straddling chunk is decoded). */
    pointBefore(tag, t) {
        const cut = this._cut(tag, false);
        for (let i = tag.n - 1; i >= tag.m0; i--) if (tag.ts[i] < t) return tag.ts[i] >= cut ? { t: tag.ts[i], v: tag.vs[i] } : null;
        if (tag.mem) return null;
        for (let l = 0; l < LEVELS; l++) {
            const r = lastRecStartingBefore(this._idx(tag, l), t);
            if (!r) continue;
            if (r[F.tLast] < t) return r[F.tLast] >= cut ? { t: r[F.tLast], v: r[F.vLast] } : null;
            if (l === 0) {                                           // t falls inside this chunk: its last point before t
                const p = this.raw(tag, r[F.tFirst], t - 1);
                const n = p.t.length;
                if (n) return p.t[n - 1] >= cut ? { t: p.t[n - 1], v: p.v[n - 1] } : null;
            }
        }
        return null;
    }

    // a level's records meeting [from, to]: read once per query for the query's whole range, then cut in memory
    _levelRecs(tag, level, from, to, sink) {
        let all = sink.levels[level];
        if (!all) all = sink.levels[level] = readRange(this._idx(tag, level), sink.from, sink.to);
        const n = all.length / REC;
        let lo = 0, hi = n;
        while (lo < hi) { const m = (lo + hi) >> 1; if (all[m * REC + F.tLast] < from) lo = m + 1; else hi = m; }
        let lo2 = lo, hi2 = n;
        while (lo2 < hi2) { const m = (lo2 + hi2) >> 1; if (all[m * REC + F.tFirst] <= to) lo2 = m + 1; else hi2 = m; }
        return all.subarray(lo * REC, lo2 * REC);
    }

    _scan(tag, level, from, to, sink) {
        if (from > to) return;
        if (level < 0) {
            const lo = Math.max(from, sink.rawCut === undefined ? -Infinity : sink.rawCut);
            if (!tag.mem && lo <= to) {
                const recs = this._levelRecs(tag, 0, lo, to, sink);
                for (let i = 0; i < recs.length; i += REC) this._decodeInto(tag, recs.subarray(i, i + REC), lo, to, sink.addPoint, 0, Infinity, sink.fds);
            }
            for (let i = tag.m0; i < tag.n; i++) { const t = tag.ts[i]; if (t >= lo && t <= to) sink.addPoint(t, tag.vs[i]); }
            return;
        }
        const recs = this._levelRecs(tag, level, from, to, sink);
        const bsize = level === 2 ? DAY : level === 1 ? this.o.segmentMs : 0;
        let covered = from - 1;
        for (let i = 0; i < recs.length; i += REC) {
            const r = recs.subarray(i, i + REC);
            const s = bsize ? Math.floor(r[F.tFirst] / bsize) * bsize : r[F.tFirst], e = bsize ? s + bsize - 1 : r[F.tLast];
            // between two records there is no data (a record exists for every bucket / chunk that has some)
            // a summary that lies in one bucket is used whole; one that straddles a bucket edge is read at the level under it
            if (r[F.tFirst] >= from && r[F.tLast] <= to && sink.fits(r)) sink.addRec(r);
            else this._scan(tag, level - 1, Math.max(from, s), Math.min(to, e), sink);
            covered = Math.max(covered, e);
        }
        // past the last record of this level (its open bucket, the open chunk): the level under it
        if (covered < to) this._scan(tag, level - 1, Math.max(from, covered + 1), to, sink);
    }
}

// ---- summaries ---------------------------------------------------------------------------------------------
// an accumulator (REC + 1 floats, the last is a bucket's start) with nothing in it: no first / last / min / max, nothing summed
function emptyAcc(a) {
    a.fill(NaN, 0, REC);
    a[F.sum] = 0; a[F.count] = 0; a[F.seg] = 0; a[F.off] = 0; a[F.inc] = 0; a[F.integL] = 0; a[F.integS] = 0;
    return a;
}
// the step up of a counter from a to b: a drop is a restart from 0, so the new value is what was counted since
const step = (a, b) => (b >= a ? b - a : b);
function summarize(ts, vs, n, r) {
    r[F.tFirst] = ts[0]; r[F.vFirst] = vs[0]; r[F.tLast] = ts[n - 1]; r[F.vLast] = vs[n - 1];
    let mn = Infinity, mx = -Infinity, tmn = ts[0], tmx = ts[0], sum = 0, cnt = 0, inc = 0, iL = 0, iS = 0;
    for (let i = 0; i < n; i++) {
        const v = vs[i];
        if (i) { const a = vs[i - 1], dt = ts[i] - ts[i - 1]; inc += step(a, v); iL += (a + v) / 2 * dt; iS += a * dt; }
        if (v !== v) continue;
        if (v < mn) { mn = v; tmn = ts[i]; }
        if (v > mx) { mx = v; tmx = ts[i]; }
        sum += v; cnt++;
    }
    r[F.vMin] = cnt ? mn : NaN; r[F.tMin] = tmn; r[F.vMax] = cnt ? mx : NaN; r[F.tMax] = tmx;
    r[F.sum] = sum; r[F.count] = n; r[F.inc] = inc; r[F.integL] = iL; r[F.integS] = iS;
}
// r (later in time) into acc: what lies between the last point of acc and the first of r is added too
function merge(acc, r) {
    if (!(acc[F.count] > 0)) {
        for (let i = 0; i < 8; i++) acc[i] = r[i];
    } else {
        const a = acc[F.vLast], b = r[F.vFirst], dt = r[F.tFirst] - acc[F.tLast];
        acc[F.inc] += step(a, b); acc[F.integL] += (a + b) / 2 * dt; acc[F.integS] += a * dt;
        if (r[F.tFirst] < acc[F.tFirst]) { acc[F.tFirst] = r[F.tFirst]; acc[F.vFirst] = r[F.vFirst]; }
        if (r[F.tLast] > acc[F.tLast]) { acc[F.tLast] = r[F.tLast]; acc[F.vLast] = r[F.vLast]; }
        if (r[F.vMin] < acc[F.vMin] || acc[F.vMin] !== acc[F.vMin]) { acc[F.vMin] = r[F.vMin]; acc[F.tMin] = r[F.tMin]; }
        if (r[F.vMax] > acc[F.vMax] || acc[F.vMax] !== acc[F.vMax]) { acc[F.vMax] = r[F.vMax]; acc[F.tMax] = r[F.tMax]; }
    }
    acc[F.sum] += r[F.sum]; acc[F.count] += r[F.count];
    acc[F.inc] += r[F.inc]; acc[F.integL] += r[F.integL]; acc[F.integS] += r[F.integS];
}

// ---- index files: fixed records sorted by time ----------------------------------------------------------------
// The records past `cut` (tLast < cut) are dropped, but a big file is not rewritten for a day's worth: it is rewritten (streamed in
// blocks, never held whole in memory, fsynced, then renamed) only once the expired part is a quarter of it, so a year of 1 Hz
// summaries (50 MB a tag) is rewritten 4 times a year, not 365. Queries never return what is past a tag's keep, so the records that
// wait are never wrong, only not yet reclaimed. A small file (< 1 MB) is cut at once: it costs nothing.
const COMPACT_SMALL = 1 << 20, COMPACT_SHARE = 0.25, COMPACT_BLOCK = 4 << 20;
function compactIdx(f, cut) {
    if (!Number.isFinite(cut) || !fs.existsSync(f)) return false;
    const size = fs.statSync(f).size, n = Math.floor(size / RECB);
    if (!n) return false;
    const fd = fs.openSync(f, 'r');
    try {
        // the first record that is not expired (records are in time order): a binary search of 8-byte reads
        const probe = Buffer.alloc(8);
        let lo = 0, hi = n;
        while (lo < hi) { const mid = (lo + hi) >> 1; fs.readSync(fd, probe, 0, 8, mid * RECB + F.tLast * 8); if (probe.readDoubleLE(0) < cut) lo = mid + 1; else hi = mid; }
        if (lo === 0 || (size > COMPACT_SMALL && lo < n * COMPACT_SHARE)) return false;
        const tmp = f + '.tmp', out = fs.openSync(tmp, 'w'), buf = Buffer.allocUnsafe(COMPACT_BLOCK);
        try {
            for (let off = lo * RECB, end = n * RECB; off < end;) {
                const got = fs.readSync(fd, buf, 0, Math.min(buf.length, end - off), off);
                if (got <= 0) throw new Error('short read compacting ' + f);
                fs.writeSync(out, buf, 0, got); off += got;
            }
            fs.fsyncSync(out);
        } finally { fs.closeSync(out); }
        fs.renameSync(tmp, f);
        return true;
    } finally { fs.closeSync(fd); }
}
function readRecords(file) {
    if (!fs.existsSync(file)) return new Float64Array(0);
    const size = fs.statSync(file).size, n = Math.floor(size / RECB);
    const out = new Float64Array(n * REC);
    if (!n) return out;
    const fd = fs.openSync(file, 'r');
    try { fs.readSync(fd, new Uint8Array(out.buffer), 0, n * RECB, 0); } finally { fs.closeSync(fd); }
    return out;
}
function truncateRecords(file, n) { if (fs.existsSync(file)) fs.truncateSync(file, n * RECB); }

// a record that can be trusted: a count, times in order, (level 0) a place in a segment
function sane(r, prevTLast, level0) {
    return r[F.count] > 0 && Number.isFinite(r[F.count]) && Number.isFinite(r[F.tFirst]) && r[F.tLast] >= r[F.tFirst] && (prevTLast === undefined || r[F.tFirst] > prevTLast)
        && (!level0 || (r[F.seg] >= 0 && Number.isFinite(r[F.seg]) && r[F.off] >= 0 && Number.isFinite(r[F.off])));
}
// the records a query reads must be in order and valid: a damaged index is an error, not a wrong answer
function checkRecords(file, out) {
    const level0 = file.endsWith('.r0');
    let prev;
    for (let i = 0; i < out.length; i += REC) {
        if (!sane(out.subarray(i, i + REC), prev, level0)) {
            const e = new Error('corrupt index ' + path.basename(file) + ': record ' + (i / REC) + ' is not valid or out of order (run { op: "verify" }; { op: "verify", repair: true } rebuilds it from the chunks)');
            e.code = 'ETSDB_CORRUPT';
            throw e;
        }
        prev = out[i + F.tLast];
    }
}
// the last n records of an index file: { recs, total (records in the file), ragged (a partial record at the end) }
function readTail(file, n) {
    if (!fs.existsSync(file)) return { recs: new Float64Array(0), total: 0, ragged: false };
    const size = fs.statSync(file).size, total = Math.floor(size / RECB), take = Math.min(n, total), recs = new Float64Array(take * REC);
    if (take) { const fd = fs.openSync(file, 'r'); try { fs.readSync(fd, new Uint8Array(recs.buffer), 0, take * RECB, (total - take) * RECB); } finally { fs.closeSync(fd); } }
    return { recs, total, ragged: size % RECB !== 0 };
}
const same = (a, b) => a === b || (a !== a && b !== b);
// a decoded chunk against its summary record: null when they agree, else why not (this is the only check a TSC1 chunk has)
function chunkProblem(ts, vs, n, r, ver, scratch) {
    if (ts[0] !== r[F.tFirst] || ts[n - 1] !== r[F.tLast]) return 'its times do not match its summary';
    if (!same(vs[0], r[F.vFirst]) || !same(vs[n - 1], r[F.vLast])) return 'its first / last value does not match its summary';
    if (ver === 1) {
        summarize(ts, vs, n, scratch);
        if (!same(scratch[F.vMin], r[F.vMin]) || !same(scratch[F.vMax], r[F.vMax]) || Math.abs(scratch[F.sum] - r[F.sum]) > 1e-9 * Math.max(1, Math.abs(r[F.sum])) * n) return 'its values do not match its summary';
    }
    for (let i = 1; i < n; i++) if (!(ts[i] > ts[i - 1])) return 'its times are not rising';
    return null;
}

// the last record that starts before t (records never overlap in time), or null
function lastRecStartingBefore(file, t) {
    if (!fs.existsSync(file)) return null;
    const n = Math.floor(fs.statSync(file).size / RECB);
    if (!n) return null;
    const fd = fs.openSync(file, 'r');
    try {
        const probe = Buffer.alloc(8);
        let lo = 0, hi = n;
        while (lo < hi) { const mid = (lo + hi) >> 1; fs.readSync(fd, probe, 0, 8, mid * RECB + F.tFirst * 8); if (probe.readDoubleLE(0) < t) lo = mid + 1; else hi = mid; }
        if (lo === 0) return null;
        const out = new Float64Array(REC);
        fs.readSync(fd, new Uint8Array(out.buffer), 0, RECB, (lo - 1) * RECB);
        return out;
    } finally { fs.closeSync(fd); }
}

// the records whose [tFirst, tLast] meets [from, to] (binary search on tLast; records never overlap in time)
const _probe = new Float64Array(1), _probeB = new Uint8Array(_probe.buffer);
function readRange(file, from, to) {
    if (!fs.existsSync(file)) return new Float64Array(0);
    const n = Math.floor(fs.statSync(file).size / RECB);
    if (!n) return new Float64Array(0);
    const fd = fs.openSync(file, 'r');
    try {
        const at = (i, field) => { fs.readSync(fd, _probeB, 0, 8, i * RECB + field * 8); return _probe[0]; };
        let lo = 0, hi = n;
        while (lo < hi) { const mid = (lo + hi) >> 1; if (at(mid, F.tLast) < from) lo = mid + 1; else hi = mid; }
        let lo2 = lo, hi2 = n;
        while (lo2 < hi2) { const mid = (lo2 + hi2) >> 1; if (at(mid, F.tFirst) <= to) lo2 = mid + 1; else hi2 = mid; }
        const cnt = lo2 - lo, out = new Float64Array(cnt * REC);
        if (cnt) fs.readSync(fd, new Uint8Array(out.buffer), 0, cnt * RECB, lo * RECB);
        checkRecords(file, out);
        return out;
    } finally { fs.closeSync(fd); }
}

module.exports = { compactIdx, emptyAcc, Engine, DEFAULTS, F, REC, RECB, DAY, SEG_MAGIC, pad, summarize, merge, readRecords, readRange, chunkProblem, sane, readTail };
