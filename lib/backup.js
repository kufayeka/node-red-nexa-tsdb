'use strict';
// Backup to one file, restore from one file.
//
// A backup is taken while the database runs. A checkpoint writes every open chunk; then, in the same synchronous step,
// every file is listed with its size (and the small ones, the logs and the WAL, are read whole). That list is a moment
// the engine can recover from: the state of a crash just after a checkpoint. The segment and index files are then copied
// up to those sizes, a slice at a time, while writes go on (what is appended after the moment is not in the backup).
// Retention, deletes, compact and repair, which rename or truncate files, are refused while a backup runs (engine._busy).
//
// The file is gzip of:  "NXTSDBK1"  u32 length + manifest JSON  { format, created, source, files: [[path, size]] }
//                       the bytes of every file, in the order of the manifest
//                       u32 length + trailer JSON { files, bytes }  "NXTSDBK1"
// gzip carries its own CRC32 over the whole content; a restore checks every size and the trailer, then opens the restored
// folder and verifies every chunk and summary before it is used. Memory (RAM) tags are not in a backup.
//
// Offline (Node-RED stopped, or a database that does not open):
//   node lib/backup.js restore <file> <empty folder>
//   node lib/backup.js backup <database folder> <file>      (the database must not be open elsewhere)
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { finished } = require('stream/promises');

const MAGIC = Buffer.from('NXTSDBK1'), FORMAT = 1, SLICE = 1 << 20;
const SMALL = (p) => p === 'tags.log' || p === 'dict.log' || p.startsWith('wal/');
// the only paths a backup holds: a restore never writes outside its folder
const SAFE = /^(tags\.log|dict\.log|(wal|seg|idx)\/[0-9]+\.(wal|seg|r[0-2]))$/;
const tick = () => new Promise((r) => setImmediate(r));
const stamp = (t) => new Date(t).toISOString().replace(/[-:]/g, '').replace('T', '-').slice(0, 15);
const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n, 0); return b; };
const json = (o) => { const b = Buffer.from(JSON.stringify(o)); return Buffer.concat([u32(b.length), b]); };

/** Where a backup goes: a relative `file` is under the folder that holds the database (<userDir>/tsdb in Node-RED). */
function backupPath(dir, file, now) {
    if (file !== undefined && (typeof file !== 'string' || !file)) throw new Error('file must be a path (got ' + JSON.stringify(file) + ')');
    const base = path.dirname(dir);
    const f = path.resolve(base, file || path.join('backups', path.basename(dir) + '-' + stamp(now) + '.tsdb.gz'));
    const rel = path.relative(dir, f);
    if (!rel.startsWith('..') && !path.isAbsolute(rel)) throw new Error('a backup cannot be written inside the database folder (' + f + ')');
    return f;
}

// the files of a database, with their sizes, at this moment
function listFiles(dir) {
    const out = [];
    for (const f of ['tags.log', 'dict.log']) { const p = path.join(dir, f); if (fs.existsSync(p)) out.push([f, fs.statSync(p).size]); }
    for (const sub of ['wal', 'seg', 'idx']) {
        const d = path.join(dir, sub);
        if (!fs.existsSync(d)) continue;
        for (const n of fs.readdirSync(d).sort()) { const p = sub + '/' + n; if (SAFE.test(p)) out.push([p, fs.statSync(path.join(d, n)).size]); }
    }
    return out;
}

function readUpTo(file, size) {
    const b = Buffer.alloc(size), fd = fs.openSync(file, 'r');
    try { let got = 0; while (got < size) { const n = fs.readSync(fd, b, got, size - got, got); if (!n) break; got += n; } if (got < size) throw new Error(file + ' is shorter than at the start of the backup'); } finally { fs.closeSync(fd); }
    return b;
}

/**
 * Write a backup of an open engine to one file: a Promise of { op, file, files, bytes, fileBytes, created, ms }.
 * still(): false when the engine was replaced or closed meanwhile (the worker restarted it after an I/O error): the backup stops.
 */
function backup(engine, req, still) {
    const t0 = Date.now(), dir = engine.dir, file = backupPath(dir, req && req.file, t0);
    if (engine._busy) throw new Error('a ' + engine._busy + ' is running: try again when it is done');
    if (engine.checkpoint) engine.checkpoint();
    // the moment: every file and its size, the small ones read now (a checkpoint may delete a WAL file later)
    const files = listFiles(dir), small = new Map();
    for (const [p, size] of files) if (SMALL(p)) small.set(p, readUpTo(path.join(dir, p), size));
    engine._busy = 'backup';
    return copy(engine, file, files, small, still, t0);
}
// the copy, a slice at a time (the checks and the moment above are synchronous: a refusal throws at once)
async function copy(engine, file, files, small, still, t0) {
    const dir = engine.dir;
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const part = file + '.part', gz = zlib.createGzip({ level: 1 }), out = fs.createWriteStream(part);
    gz.pipe(out);
    const put = (b) => (gz.write(b) ? null : new Promise((r) => gz.once('drain', r)));
    let bytes = 0;
    try {
        await put(Buffer.concat([MAGIC, json({ format: FORMAT, created: t0, source: path.basename(dir), files })]));
        for (const [p, size] of files) {
            if (small.has(p)) { await put(small.get(p)); bytes += size; continue; }
            const fd = fs.openSync(path.join(dir, p), 'r');
            try {
                for (let off = 0; off < size;) {
                    if (still && !still()) throw new Error('the historian restarted during the backup: try again');
                    const b = Buffer.allocUnsafe(Math.min(SLICE, size - off)), n = fs.readSync(fd, b, 0, b.length, off);
                    if (n !== b.length) throw new Error(p + ' is shorter than at the start of the backup');
                    const w = put(b);
                    if (w) await w;
                    off += n; bytes += n;
                    await tick();                                     // writes and queries go on between slices
                }
            } finally { fs.closeSync(fd); }
        }
        await put(Buffer.concat([json({ files: files.length, bytes }), MAGIC]));
        gz.end();
        await finished(out);
        const fd = fs.openSync(part, 'r+'); try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
        fs.renameSync(part, file);
    } catch (e) {
        gz.destroy(); out.destroy();
        try { fs.unlinkSync(part); } catch (x) { /* never written */ }
        throw e;
    } finally { engine._busy = null; }
    return { op: 'backup', file, files: files.length, bytes, fileBytes: fs.statSync(file).size, created: new Date(t0).toISOString(), ms: Date.now() - t0 };
}

/** Unpack a backup into a folder that does not exist yet (or is empty). Every path, size and the trailer are checked. */
async function extract(file, dest) {
    if (!fs.existsSync(file)) throw new Error('no backup file ' + file);
    if (fs.existsSync(dest) && fs.readdirSync(dest).length) throw new Error('the folder to restore into is not empty: ' + dest);
    for (const d of ['', 'wal', 'seg', 'idx']) fs.mkdirSync(path.join(dest, d), { recursive: true });
    const gun = fs.createReadStream(file).pipe(zlib.createGunzip());
    let pend = null, stage = 'magic', want = 8, manifest = null, trailer = null, i = -1, fd = null, left = 0, bytes = 0;
    const bad = (why) => new Error('not a valid backup file (' + why + '): ' + file);
    const nextFile = () => {
        if (fd !== null) { fs.fsyncSync(fd); fs.closeSync(fd); fd = null; }
        while (++i < manifest.files.length) {
            const [p, size] = manifest.files[i];
            fd = fs.openSync(path.join(dest, p), 'w');
            if (size > 0) { left = size; stage = 'file'; return; }
            fs.closeSync(fd); fd = null;
        }
        stage = 'tlen'; want = 4;
    };
    try {
        for await (const c of gun) {
            const b = pend ? Buffer.concat([pend, c]) : c;
            pend = null;
            let p = 0;
            while (p < b.length) {
                if (stage === 'file') {
                    const n = Math.min(left, b.length - p);
                    fs.writeSync(fd, b, p, n);
                    p += n; left -= n; bytes += n;
                    if (!left) nextFile();
                    continue;
                }
                if (stage === 'done') throw bad('bytes after its end');
                if (b.length - p < want) break;
                const part = b.subarray(p, p + want);
                p += want;
                if (stage === 'magic') { if (!part.equals(MAGIC)) throw bad('it does not start like one'); stage = 'mlen'; want = 4; }
                else if (stage === 'mlen' || stage === 'tlen') { want = part.readUInt32LE(0); if (want > 64 << 20) throw bad('a header too large'); stage = stage === 'mlen' ? 'manifest' : 'trailer'; }
                else if (stage === 'manifest') {
                    try { manifest = JSON.parse(part.toString()); } catch (e) { throw bad('its manifest is not JSON'); }
                    if (manifest.format !== FORMAT) throw bad('format ' + manifest.format + ', this version reads ' + FORMAT);
                    if (!Array.isArray(manifest.files) || !manifest.files.every((f) => Array.isArray(f) && SAFE.test(f[0]) && Number.isInteger(f[1]) && f[1] >= 0)) throw bad('a file it names is not one of a database');
                    nextFile();
                } else if (stage === 'trailer') {
                    try { trailer = JSON.parse(part.toString()); } catch (e) { throw bad('its trailer is not JSON'); }
                    stage = 'end'; want = 8;
                } else if (stage === 'end') { if (!part.equals(MAGIC)) throw bad('it does not end like one'); stage = 'done'; }
            }
            if (p < b.length) pend = b.subarray(p);
        }
    } catch (e) {
        if (fd !== null) fs.closeSync(fd);
        throw /not a valid backup/.test(e.message) ? e : bad(e.code === 'Z_BUF_ERROR' || e.code === 'Z_DATA_ERROR' ? 'cut short or damaged' : e.message);
    }
    if (stage !== 'done') throw bad('cut short');
    if (!trailer || trailer.files !== manifest.files.length || trailer.bytes !== bytes) throw bad('its trailer does not match its content');
    return { files: manifest.files.length, bytes, created: new Date(manifest.created).toISOString(), source: manifest.source };
}

/**
 * Unpack and check a backup next to the live folder, before anything is swapped: <dir>.restore-<time>, opened with the
 * engine's options and verified (every chunk, every summary). Returns { tmp, info }; the caller swaps it in (swapIn).
 */
async function prepare(file, dir, opts, now) {
    const { Engine } = require('./engine');
    const admin = require('./admin');
    const tmp = dir + '.restore-' + stamp(now);
    try {
        const info = await extract(file, tmp);
        const e = new Engine(tmp, Object.assign({}, opts, { checkpointMs: 1e9, walFlushMs: 1e9 })).open();
        try {
            const v = admin.run(e, { op: 'verify' });
            if (!v.ok) throw new Error('the backup does not verify: ' + v.problems.slice(0, 3).map((x) => x.reason || JSON.stringify(x)).join('; '));
            Object.assign(info, { chunks: v.chunks, points: v.points, tags: e.tags.filter(Boolean).length });
        } finally { e.close(); }
        return { tmp, info };
    } catch (e) { fs.rmSync(tmp, { recursive: true, force: true }); throw e; }
}

/** The restored folder in place of the live one (closed). The live one is kept as <dir>.before-restore-<time>. */
function swapIn(dir, tmp, now) {
    const before = dir + '.before-restore-' + stamp(now);
    fs.renameSync(dir, before);
    try { fs.renameSync(tmp, dir); } catch (e) { fs.renameSync(before, dir); throw e; }
    return before;
}
/** Undo swapIn (the restored folder did not open). */
function swapBack(dir, before) {
    fs.renameSync(dir, dir + '.restore-failed-' + stamp(Date.now()));
    fs.renameSync(before, dir);
}

function checkRestore(req) {
    if (!req || typeof req.file !== 'string' || !req.file) throw new Error('restore needs file: the backup to restore');
    if (req.confirm !== 'RESTORE') throw new Error('restore replaces the whole database with the backup: send confirm: "RESTORE"');
}

module.exports = { backup, extract, prepare, swapIn, swapBack, checkRestore, backupPath, listFiles };

if (require.main === module) {
    const [cmd, a, b] = process.argv.slice(2);
    const done = (r) => { console.log(JSON.stringify(r, null, 2)); };
    const fail = (e) => { console.error(e.message); process.exit(1); };
    if (cmd === 'restore' && a && b) {
        const dest = path.resolve(b);
        prepare(path.resolve(a), dest + '.tmp', {}, Date.now())
            .then(({ tmp, info }) => { if (fs.existsSync(dest)) { if (fs.readdirSync(dest).length) throw new Error('not empty: ' + dest); fs.rmdirSync(dest); } fs.renameSync(tmp, dest); done(Object.assign({ op: 'restore', dir: dest }, info)); })
            .catch(fail);
    } else if (cmd === 'backup' && a && b) {
        const { Engine } = require('./engine');
        const e = new Engine(path.resolve(a), { checkpointMs: 1e9, walFlushMs: 1e9 }).open();
        backup(e, { file: path.resolve(b) }).then((r) => { e.close(); done(r); }, (x) => { e.close(); fail(x); });
    } else {
        console.error('node lib/backup.js restore <file> <empty or new folder>\nnode lib/backup.js backup <database folder> <file>');
        process.exit(2);
    }
}
