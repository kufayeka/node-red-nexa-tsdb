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
// A summary is 12 float64 (96 bytes): tFirst tLast vFirst vLast vMin tMin vMax tMax sum count seg off. It holds the
// first, last, min and max WITH their times, so a chart's M4 (min / max / first / last per pixel column) comes out of
// the summaries exactly as from the raw points, for any range, reading about as many records as the chart has pixels.
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
const { parseDuration, globRe } = require('./query');

const DAY = 86400000;
const REC = 12, RECB = REC * 8;                 // a summary record: 12 float64
const F = { tFirst: 0, tLast: 1, vFirst: 2, vLast: 3, vMin: 4, tMin: 5, vMax: 6, tMax: 7, sum: 8, count: 9, seg: 10, off: 11 };
const SEG_MAGIC = 0x31435354;                   // "TSC1"
const WALB = 20;
const LEVELS = 3;

const DEFAULTS = {
    segmentMs: 3600000,         // a segment file (and the level 1 summary): 1 hour; a day must be a whole number of them
    chunkPoints: 1024,          // points in a chunk
    walFlushMs: 1000,           // the WAL is written and fsynced this often (the most a power cut loses)
    checkpointMs: 60000,        // open chunks are written this often (the WAL is then dropped)
    rawDays: 30,                // raw chunks (segments) kept
    indexDays: 365,             // level 0 summaries kept (the raw data's own summaries outlive it)
    walSync: true,
    rules: []
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
        this.walBuf = Buffer.alloc(1 << 20);
        this.walLen = 0;
        this.walFd = null;
        this.walSeq = 0;
        this.stats = { points: 0, late: 0, badType: 0, chunks: 0, chunkBytes: 0, chunkPoints: 0, recovered: 0 };
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
        require('./admin').recoverOps(this.dir);   // a delete / compact cut short: finished or dropped, before any read
        this._loadTags();
        this._loadDicts();
        this._recover();
        this._openWal();
        this.opened = true;
        this._timers = [
            setInterval(() => this.flushWal(), this.o.walFlushMs),
            setInterval(() => this.checkpoint(), this.o.checkpointMs)
        ];
        this._timers.forEach((t) => t.unref && t.unref());
        this.retention();
        return this;
    }

    close() {
        if (!this.opened) return;
        (this._timers || []).forEach(clearInterval);
        this.checkpoint();
        if (this.walFd !== null) { fs.closeSync(this.walFd); this.walFd = null; }
        this.segFds.forEach((s) => fs.closeSync(s.fd));
        this.segFds.clear();
        this.opened = false;
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
        const n = mem ? Math.min(1024, this._rule(name).max) : this.o.chunkPoints;   // a ring never starts past its max
        // m0: the first live point (a memory ring drops from the front); 0 for a disk tag
        const tag = { id, name, type, mem: !!mem, ts: new Float64Array(n), vs: new Float64Array(n), n: 0, m0: 0, seg: 0, lastT: -Infinity,
            acc: [null, new Float64Array(REC + 1), new Float64Array(REC + 1)], dict: null, words: null, span: 0 };
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
    _tag(name, type) {
        let tag = this.byName.get(name);
        if (tag) return tag;
        const mem = this._rule(name).store === 'memory';
        tag = this._addTag(this.tags.length, name, type, mem);
        if (!mem) this._appendDurable('tags.log', JSON.stringify({ id: tag.id, name, type }) + '\n');
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
        if (!tag.mem) this._appendDurable('dict.log', JSON.stringify([tag.id, id, s]) + '\n');
        return id;
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
        if (tag.type !== type) { this.stats.badType++; return false; }
        const t = Math.round(+ts);
        if (!Number.isFinite(t) || t <= tag.lastT) { this.stats.late++; return false; }
        const v = type === 'number' ? value : type === 'bool' ? (value ? 1 : 0) : this._wordId(tag, value);
        if (tag.mem) { this._pushMem(tag, t, v); return true; }
        this._walPut(tag.id, t, v);
        this._push(tag, t, v);
        return true;
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
        tag.lastT = t;
        const cut = t - tag.keep;
        while (tag.m0 < tag.n && tag.ts[tag.m0] < cut) tag.m0++;
        this.stats.points++;
    }

    _push(tag, t, v) {
        const seg = Math.floor(t / this.o.segmentMs) * this.o.segmentMs;
        if (tag.n && seg !== tag.seg) this._flushHead(tag);
        if (!tag.n) tag.seg = seg;
        tag.ts[tag.n] = t; tag.vs[tag.n] = v; tag.n++;
        tag.lastT = t;
        this.stats.points++;
        if (tag.n === this.o.chunkPoints) this._flushHead(tag);
    }

    _walPut(id, t, v) {
        if (this.walLen + WALB > this.walBuf.length) this.flushWal();
        this.walBuf.writeUInt32LE(id, this.walLen);
        this.walBuf.writeDoubleLE(t, this.walLen + 4);
        this.walBuf.writeDoubleLE(v, this.walLen + 12);
        this.walLen += WALB;
    }

    flushWal() {
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
    _flushHead(tag) {
        const n = tag.n;
        if (!n) return;
        const body = gorilla.encode(tag.ts, tag.vs, n);
        const s = this._segFd(tag.seg);
        const head = Buffer.alloc(16);
        head.writeUInt32LE(SEG_MAGIC, 0); head.writeUInt32LE(tag.id, 4); head.writeUInt32LE(n, 8); head.writeUInt32LE(body.length, 12);
        const off = s.size;
        fs.writeSync(s.fd, head, 0, 16, off);
        fs.writeSync(s.fd, body, 0, body.length, off + 16);
        s.size += 16 + body.length; s.dirty = true;
        const r = this._recF;
        summarize(tag.ts, tag.vs, n, r);
        r[F.seg] = tag.seg; r[F.off] = off;
        fs.appendFileSync(this._idx(tag, 0), this._rec);
        tag.span = r[F.tLast] - r[F.tFirst];
        this._roll(tag, r);
        this.stats.chunks++; this.stats.chunkBytes += 16 + body.length; this.stats.chunkPoints += n;
        tag.n = 0;
    }

    // a level 0 summary into the open level 1 / 2 buckets; a bucket that ends is written
    _roll(tag, r) {
        for (let l = 1; l < LEVELS; l++) {
            const size = l === 1 ? this.o.segmentMs : DAY, acc = tag.acc[l], b = Math.floor(r[F.tFirst] / size) * size;
            if (acc[REC] === acc[REC] && acc[REC] !== b) this._closeBucket(tag, l);
            if (acc[REC] !== acc[REC]) { acc.fill(NaN, 0, REC); acc[F.sum] = 0; acc[F.count] = 0; acc[F.seg] = 0; acc[F.off] = 0; acc[REC] = b; }
            merge(acc, r);
        }
    }
    _closeBucket(tag, l) {
        const acc = tag.acc[l];
        this._outF.set(acc.subarray(0, REC));
        fs.appendFileSync(this._idx(tag, l), this._out);
        acc[REC] = NaN;
    }

    _segFd(seg) {
        let s = this.segFds.get(seg);
        if (s) return s;
        const f = path.join(this.dir, 'seg', pad(seg) + '.seg');
        const fd = fs.openSync(f, fs.existsSync(f) ? 'r+' : 'w+');
        s = { fd, size: fs.fstatSync(fd).size, dirty: false };
        this.segFds.set(seg, s);
        return s;
    }
    _idx(tag, l) { return path.join(this.dir, 'idx', tag.id + '.r' + l); }

    /** Write every open chunk, fsync, drop the WAL it covered. */
    checkpoint() {
        if (!this.opened) return;
        this.flushWal();
        // the points so far are in the WAL files up to this one; new points go to a new file
        const covered = this.walSeq;
        if (this.walFd !== null) fs.closeSync(this.walFd);
        this.walSeq++;
        this.walFd = fs.openSync(path.join(this.dir, 'wal', pad(this.walSeq) + '.wal'), 'a');
        for (const tag of this.tags) if (tag && tag.n && !tag.mem) this._flushHead(tag);
        const now = Math.floor(Date.now() / this.o.segmentMs) * this.o.segmentMs;
        this.segFds.forEach((s, seg) => {
            if (s.dirty) { fs.fsyncSync(s.fd); s.dirty = false; }
            if (seg < now - this.o.segmentMs) { fs.closeSync(s.fd); this.segFds.delete(seg); }
        });
        for (const n of this._walFiles()) if (n <= covered) fs.unlinkSync(path.join(this.dir, 'wal', pad(n) + '.wal'));
    }

    // ---- recovery --------------------------------------------------------------------------------------------
    _segFiles() { return fs.readdirSync(path.join(this.dir, 'seg')).filter((f) => f.endsWith('.seg')).map((f) => parseInt(f, 10)).sort((a, b) => a - b); }

    _recover() {
        const segs = this._segFiles();
        const from = segs.length > 1 ? segs[segs.length - 2] : segs.length ? segs[0] : Infinity;
        const sizes = new Map(segs.map((s) => [s, fs.statSync(path.join(this.dir, 'seg', pad(s) + '.seg')).size]));
        for (const tag of this.tags) {
            if (!tag) continue;
            // level 0: no torn record, nothing that points past what the segments hold
            const f0 = this._idx(tag, 0);
            let recs = readRecords(f0);
            const had = recs.length / REC;
            let keep = had;
            while (keep > 0) {
                const r = recs.subarray((keep - 1) * REC, keep * REC);
                if (r[F.seg] < from) break;
                if (this._chunkOk(r[F.seg], r[F.off], tag.id, sizes)) break;
                keep--;
            }
            if (keep !== had || (fs.existsSync(f0) && fs.statSync(f0).size !== had * RECB)) truncateRecords(f0, keep);
            if (keep !== had) recs = recs.subarray(0, keep * REC);
            tag._last0 = keep ? recs.subarray((keep - 1) * REC, keep * REC).slice() : null;
        }
        // chunks in the last two segments the index does not have yet
        for (const seg of segs.filter((s) => s >= from)) {
            const fd = fs.openSync(path.join(this.dir, 'seg', pad(seg) + '.seg'), 'r'), size = sizes.get(seg), head = Buffer.alloc(16);
            let off = 0;
            try {
                while (off + 16 <= size) {
                    fs.readSync(fd, head, 0, 16, off);
                    if (head.readUInt32LE(0) !== SEG_MAGIC) break;
                    const id = head.readUInt32LE(4), n = head.readUInt32LE(8), len = head.readUInt32LE(12);
                    if (off + 16 + len > size) break;
                    const tag = this.tags[id], last = tag && tag._last0;
                    if (tag && (!last || seg > last[F.seg] || (seg === last[F.seg] && off > last[F.off]))) {
                        const body = Buffer.alloc(len);
                        fs.readSync(fd, body, 0, len, off + 16);
                        gorilla.decode(body, n, this._ts, this._vs);
                        summarize(this._ts, this._vs, n, this._recF);
                        this._recF[F.seg] = seg; this._recF[F.off] = off;
                        fs.appendFileSync(this._idx(tag, 0), this._rec);
                        tag._last0 = this._recF.slice();
                        this.stats.recovered++;
                    }
                    off += 16 + len;
                }
            } finally { fs.closeSync(fd); }
            // a torn chunk at the end: cut it (its points are in the WAL)
            if (off < size) fs.truncateSync(path.join(this.dir, 'seg', pad(seg) + '.seg'), off);
        }
        // levels 1 / 2: rebuilt from `from` on; the open buckets in memory again
        for (const tag of this.tags) {
            if (!tag) continue;
            const last = tag._last0;
            tag.lastT = last ? last[F.tLast] : -Infinity;
            if (last) tag.span = last[F.tLast] - last[F.tFirst];
            for (let l = 1; l < LEVELS; l++) {
                const f = this._idx(tag, l), recs = readRecords(f), size = l === 1 ? this.o.segmentMs : DAY;
                const had = recs.length / REC;
                let keep = had;
                while (keep > 0 && Math.floor(recs[(keep - 1) * REC + F.tFirst] / size) * size >= (l === 1 ? from : Math.floor(from / DAY) * DAY)) keep--;
                if (keep !== had || (fs.existsSync(f) && fs.statSync(f).size % RECB)) truncateRecords(f, keep);
            }
            // the day bucket open at `from`: its closed hours first
            const dayStart = Math.floor(from / DAY) * DAY;
            if (Number.isFinite(from)) {
                const hours = readRange(this._idx(tag, 1), dayStart, from - 1);
                for (let i = 0; i < hours.length; i += REC) {
                    const acc = tag.acc[2];
                    if (acc[REC] !== acc[REC]) { acc.fill(NaN, 0, REC); acc[F.sum] = 0; acc[F.count] = 0; acc[F.seg] = 0; acc[F.off] = 0; acc[REC] = dayStart; }
                    merge(acc, hours.subarray(i, i + REC));
                }
                const zero = readRange(this._idx(tag, 0), from, Infinity);
                for (let i = 0; i < zero.length; i += REC) this._rollRecovered(tag, zero.subarray(i, i + REC));
            }
            delete tag._last0;
        }
        // the WAL: every point not in a chunk yet
        const walDir = path.join(this.dir, 'wal');
        for (const n of this._walFiles()) {
            const buf = fs.readFileSync(path.join(walDir, pad(n) + '.wal'));
            for (let o = 0; o + WALB <= buf.length; o += WALB) {
                const tag = this.tags[buf.readUInt32LE(o)];
                const t = buf.readDoubleLE(o + 4), v = buf.readDoubleLE(o + 12);
                if (tag && t > tag.lastT) { this._push(tag, t, v); this.stats.recovered++; }
            }
        }
        if (this._walFiles().length) {
            // the replayed points into chunks now, then the old WAL goes
            this.opened = true;
            this.walSeq = this._walFiles().slice(-1)[0];
            this.walFd = null;
            for (const tag of this.tags) if (tag && tag.n) this._flushHead(tag);
            this.segFds.forEach((s) => { if (s.dirty) { fs.fsyncSync(s.fd); s.dirty = false; } });
            for (const n of this._walFiles()) fs.unlinkSync(path.join(walDir, pad(n) + '.wal'));
            this.opened = false;
        }
    }
    _rollRecovered(tag, r) { this._roll(tag, r); }

    _chunkOk(seg, off, id, sizes) {
        const size = sizes.get(seg);
        if (size === undefined || off + 16 > size) return false;
        const fd = fs.openSync(path.join(this.dir, 'seg', pad(seg) + '.seg'), 'r'), head = Buffer.alloc(16);
        try { fs.readSync(fd, head, 0, 16, off); } finally { fs.closeSync(fd); }
        return head.readUInt32LE(0) === SEG_MAGIC && head.readUInt32LE(4) === id && off + 16 + head.readUInt32LE(12) <= size;
    }

    // ---- retention -------------------------------------------------------------------------------------------
    retention(now) {
        const t = now === undefined ? Date.now() : now;
        // a segment goes when it is past every disk tag's raw keep (the default rawDays when there is no tag yet)
        let longest = 0;
        for (const tag of this.tags) if (tag && !tag.mem) longest = Math.max(longest, tag.rawKeep);
        const rawCut = t - (longest || this.o.rawDays * DAY);
        for (const seg of this._segFiles()) {
            if (seg + this.o.segmentMs > rawCut || this.segFds.has(seg)) continue;
            fs.unlinkSync(path.join(this.dir, 'seg', pad(seg) + '.seg'));
        }
        for (const tag of this.tags) {
            if (!tag || tag.mem) continue;
            // per-chunk summaries: indexDays (never past keep); hour / day summaries: keep (for ever by default)
            compactIdx(this._idx(tag, 0), t - Math.min(tag.keep, this.o.indexDays * DAY), t);
            if (tag.keep !== Infinity) { compactIdx(this._idx(tag, 1), t - tag.keep, t); compactIdx(this._idx(tag, 2), t - tag.keep, t); }
        }
        // a memory tag: its ring past keep (a tag that stopped writing)
        for (const tag of this.tags) if (tag && tag.mem) { const cut = t - tag.keep; while (tag.m0 < tag.n && tag.ts[tag.m0] < cut) tag.m0++; }
    }

    // ---- reading ---------------------------------------------------------------------------------------------
    /** The raw points of a tag in [from, to] (chunks + the open one): { t: Float64Array, v: Float64Array }. */
    raw(tag, fromArg, to, limit) {
        const max = limit || 1e7, outT = [], outV = [], from = Math.max(fromArg, this._cut(tag, true));
        let n = 0;
        if (!tag.mem && from <= to) {
            const recs = readRange(this._idx(tag, 0), from, to);
            for (let i = 0; i < recs.length && n < max; i += REC) n = this._decodeInto(tag, recs.subarray(i, i + REC), from, to, (t, v) => { outT.push(t); outV.push(v); }, n, max);
        }
        for (let i = tag.m0; i < tag.n && n < max; i++) { const t = tag.ts[i]; if (t >= from && t <= to) { outT.push(t); outV.push(tag.vs[i]); n++; } }
        return { t: Float64Array.from(outT), v: Float64Array.from(outV) };
    }

    _decodeInto(tag, r, from, to, fn, n, max, fds) {
        const seg = r[F.seg], off = r[F.off];
        let fd = fds && fds.get(seg);
        if (fd === undefined) {
            const file = path.join(this.dir, 'seg', pad(seg) + '.seg');
            if (!fs.existsSync(file)) { if (fds) fds.set(seg, null); return n; }   // raw past its retention: the summaries remain
            fd = fs.openSync(file, 'r');
            if (fds) fds.set(seg, fd);
        }
        if (fd === null) return n;
        const head = this._head || (this._head = Buffer.alloc(16));
        try {
            fs.readSync(fd, head, 0, 16, off);
            const cnt = head.readUInt32LE(8), len = head.readUInt32LE(12), body = Buffer.alloc(len);
            fs.readSync(fd, body, 0, len, off + 16);
            gorilla.decode(body, cnt, this._ts, this._vs);
            for (let i = 0; i < cnt && n < max; i++) { const t = this._ts[i]; if (t >= from && t <= to) { fn(t, this._vs[i]); n++; } }
        } finally { if (!fds) fs.closeSync(fd); }
        return n;
    }

    /**
     * Buckets of [from, to] of `size` ms (aligned to origin): per bucket first / last / min / max with their times, sum,
     * count. A summary is used whole when it lies in one bucket; else the level under it is read for its span, down to
     * the raw points. mode "m4": a level 0 summary is placed by its points' own times (near-exact, far fewer reads).
     */
    buckets(tag, from, to, size, origin, mode) {
        const o = origin === undefined ? from : origin;
        const b0 = Math.floor((from - o) / size), nb = Math.max(1, Math.floor((to - o) / size) - b0 + 1);
        if (nb > 5e6) throw new Error('too many buckets: ' + nb);
        const acc = new Float64Array(nb * 10).fill(NaN);
        for (let b = 0; b < nb; b++) { acc[b * 10 + 8] = 0; acc[b * 10 + 9] = 0; }
        const idxOf = (t) => Math.floor((t - o) / size) - b0;
        const addPoint = (t, v) => {
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
        const addRec = (r) => {
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
        // a level 0 summary for M4: its four points, each in the bucket of its own time (sum / count are not used by M4)
        const addRecPoints = (r) => { addPoint(r[F.tFirst], r[F.vFirst]); addPoint(r[F.tMin], r[F.vMin]); addPoint(r[F.tMax], r[F.vMax]); addPoint(r[F.tLast], r[F.vLast]); };
        const span = tag.span || this.o.segmentMs / 10;
        const start = tag.mem ? -1 : size >= DAY ? 2 : size >= this.o.segmentMs ? 1 : (mode === 'm4' ? size >= 4 * span : size >= span) ? 0 : -1;
        // nothing past the tag's keep; raw points not past its raw keep (their summaries still answer)
        const lo = Math.max(from, this._cut(tag, false));
        // one query: each level's records for the whole range read once, each segment opened once
        const sink = { addPoint, addRec, addRecPoints, fits: (r) => idxOf(r[F.tFirst]) === idxOf(r[F.tLast]), mode, levels: [], fds: new Map(), from: lo, to, rawCut: this._cut(tag, true) };
        try { if (lo <= to) this._scan(tag, start, lo, to, sink); } finally { sink.fds.forEach((fd) => { if (fd !== null) fs.closeSync(fd); }); }
        return { acc, nb, start: o + b0 * size, size };
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
            if (r[F.tFirst] >= from && r[F.tLast] <= to && (level === 0 && sink.mode === 'm4' ? true : sink.fits(r))) {
                if (level === 0 && sink.mode === 'm4') sink.addRecPoints(r); else sink.addRec(r);
            } else this._scan(tag, level - 1, Math.max(from, s), Math.min(to, e), sink);
            covered = Math.max(covered, e);
        }
        // past the last record of this level (its open bucket, the open chunk): the level under it
        if (covered < to) this._scan(tag, level - 1, Math.max(from, covered + 1), to, sink);
    }
}

// ---- summaries ---------------------------------------------------------------------------------------------
function summarize(ts, vs, n, r) {
    r[F.tFirst] = ts[0]; r[F.vFirst] = vs[0]; r[F.tLast] = ts[n - 1]; r[F.vLast] = vs[n - 1];
    let mn = Infinity, mx = -Infinity, tmn = ts[0], tmx = ts[0], sum = 0, cnt = 0;
    for (let i = 0; i < n; i++) {
        const v = vs[i];
        if (v !== v) continue;
        if (v < mn) { mn = v; tmn = ts[i]; }
        if (v > mx) { mx = v; tmx = ts[i]; }
        sum += v; cnt++;
    }
    r[F.vMin] = cnt ? mn : NaN; r[F.tMin] = tmn; r[F.vMax] = cnt ? mx : NaN; r[F.tMax] = tmx;
    r[F.sum] = sum; r[F.count] = n;
}
function merge(acc, r) {
    if (!(acc[F.count] > 0)) {
        for (let i = 0; i < 8; i++) acc[i] = r[i];
    } else {
        if (r[F.tFirst] < acc[F.tFirst]) { acc[F.tFirst] = r[F.tFirst]; acc[F.vFirst] = r[F.vFirst]; }
        if (r[F.tLast] > acc[F.tLast]) { acc[F.tLast] = r[F.tLast]; acc[F.vLast] = r[F.vLast]; }
        if (r[F.vMin] < acc[F.vMin] || acc[F.vMin] !== acc[F.vMin]) { acc[F.vMin] = r[F.vMin]; acc[F.tMin] = r[F.tMin]; }
        if (r[F.vMax] > acc[F.vMax] || acc[F.vMax] !== acc[F.vMax]) { acc[F.vMax] = r[F.vMax]; acc[F.tMax] = r[F.tMax]; }
    }
    acc[F.sum] += r[F.sum]; acc[F.count] += r[F.count];
}

// ---- index files: fixed records sorted by time ----------------------------------------------------------------
// the records past `cut` (tLast < cut) are dropped once they are worth a rewrite (a day, or a tenth of the keep)
function compactIdx(f, cut, now) {
    if (!Number.isFinite(cut) || !fs.existsSync(f)) return;
    const fd = fs.openSync(f, 'r'), first = Buffer.alloc(RECB);
    const got = fs.readSync(fd, first, 0, RECB, 0);
    fs.closeSync(fd);
    if (got < RECB || first.readDoubleLE(8) >= cut - Math.min(DAY, Math.max(1000, (now - cut) / 10))) return;
    const recs = readRecords(f);
    let i = 0;
    while (i < recs.length && recs[i + F.tLast] < cut) i += REC;
    const rest = Buffer.from(recs.buffer, recs.byteOffset + i * 8, (recs.length - i) * 8);
    fs.writeFileSync(f + '.tmp', rest);
    fs.renameSync(f + '.tmp', f);
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
        return out;
    } finally { fs.closeSync(fd); }
}

module.exports = { Engine, DEFAULTS, F, REC, RECB, DAY, SEG_MAGIC, pad, summarize, merge, readRecords, readRange };
