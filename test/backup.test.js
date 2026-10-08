'use strict';
// Backup to one file while the database runs, restore from that file: the restored database answers exactly what the
// live one answered at the backup; writes go on during a backup; a damaged, cut or hostile file is refused and the live
// database stays as it was; the offline restore (node lib/backup.js restore) gives the same database.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const zlib = require('zlib');
const { execFileSync } = require('child_process');
const { openHistorian } = require('../lib/client');
const { Engine } = require('../lib/engine');
const Q = require('../lib/query');
const admin = require('../lib/admin');

let passed = 0;
async function ok(label, fn) { await fn(); passed++; console.log('✔ ' + label); }
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'tsdb-b-'));
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const NOW = Date.now(), T0 = Math.floor((NOW - 3 * 3600000) / 1000) * 1000;
const ALL = { tags: '*', from: T0, to: NOW + 3600000, mode: 'raw' };
const plain = (r) => { const o = {}; for (const [k, s] of Object.entries(r)) o[k] = { t: Array.from(s.t), v: Array.from(s.v) }; return o; };
const fill = (w, from, to) => { for (let t = from; t < to; t += 1000) { const i = (t - T0) / 1000; w('Line1.Power', t, Math.round(Math.sin(i / 300) * 5000) / 100); if (i % 10 === 0) w('Line1.Running', t, i % 700 < 500); if (i % 60 === 0) w('Line1.State', t, ['run', 'idle', 'fault'][i % 3]); } };

(async () => {
    const root = tmp(), dir = path.join(root, 'plant');
    let db = openHistorian(dir, { walSync: false });
    await db.ready;
    let file, atBackup;

    await ok('backup while writing: one file, the writes go on (none refused), the moment is every point written before the call', async () => {
        fill((n, t, v) => db.write(n, t, v), T0, NOW - 60000);
        await db.checkpoint();
        fill((n, t, v) => db.write(n, t, v), NOW - 60000, NOW - 1000);         // points still in open chunks and the WAL
        atBackup = plain(await db.query(ALL));
        let writing = true, sent = 0, refused = 0;
        const writer = (async () => { let t = NOW; while (writing) { for (let k = 0; k < 200; k++) { if (db.write('Live.Counter', t, sent)) sent++; else refused++; t += 10; } await wait(1); } })();
        const r = await db.admin({ op: 'backup' });
        writing = false; await writer;
        file = r.file;
        assert.ok(fs.existsSync(file) && r.fileBytes > 0 && r.files > 3, JSON.stringify(r));
        assert.strictEqual(path.dirname(file), path.join(root, 'backups'), 'by default next to the database, in backups/');
        assert.ok(!fs.existsSync(file + '.part'));
        assert.strictEqual(refused, 0, 'no write refused during the backup');
        assert.ok(sent > 0);
        const live = plain(await db.query(Object.assign({}, ALL, { tags: 'Live.Counter', from: NOW, to: NOW + 3600000 })))['Live.Counter'];
        assert.strictEqual(live.t.length, sent, 'every point written during the backup is in the live database');
    });

    await ok('restore needs confirm: "RESTORE" and a file; nothing changes without them', async () => {
        await assert.rejects(db.admin({ op: 'restore', file }), /confirm: "RESTORE"/);
        await assert.rejects(db.admin({ op: 'restore', confirm: 'RESTORE' }), /needs file/);
        await assert.rejects(db.admin({ op: 'restore', file: path.join(root, 'nope.tsdb.gz'), confirm: 'RESTORE' }), /no backup file/);
    });

    await ok('restore: the database answers exactly what it answered at the backup; the previous folder is kept; writes go on and survive a restart', async () => {
        fill((n, t, v) => db.write(n, t, v), NOW - 1000, NOW + 30000);
        db.write('After.Backup', NOW, 1);
        const r = await db.admin({ op: 'restore', file, confirm: 'RESTORE' });
        assert.ok(r.previous && fs.existsSync(r.previous), 'the live folder before the restore is kept: ' + r.previous);
        assert.ok(r.points > 0 && r.chunks > 0 && r.tags >= 3, JSON.stringify(r));
        const now = plain(await db.query(ALL));
        for (const tag of ['Line1.Power', 'Line1.Running', 'Line1.State']) assert.deepStrictEqual(now[tag], atBackup[tag], tag + ' as at the backup');
        assert.ok(!now['After.Backup'], 'a tag made after the backup is not there');
        assert.deepStrictEqual((await db.query({ tags: 'Line1.State', mode: 'last' }))['Line1.State'].v.length, 1, 'string tags answer (their dictionary came back)');
        db.write('Line1.Power', NOW + 60000, 42);
        await db.close();
        db = openHistorian(dir, { walSync: false });
        await db.ready;
        const p = plain(await db.query(Object.assign({}, ALL, { tags: 'Line1.Power' })))['Line1.Power'];
        assert.strictEqual(p.t[p.t.length - 1], NOW + 60000);
        assert.strictEqual(p.v[p.v.length - 1], 42);
        const v = await db.admin({ op: 'verify' });
        assert.ok(v.ok, JSON.stringify(v.problems));
    });

    await ok('a cut, a damaged or a hostile file is refused, and the live database stays as it was', async () => {
        const before = plain(await db.query(ALL));
        const buf = fs.readFileSync(file);
        const cut = path.join(root, 'cut.tsdb.gz'), flip = path.join(root, 'flip.tsdb.gz'), evil = path.join(root, 'evil.tsdb.gz'), junk = path.join(root, 'junk.tsdb.gz');
        fs.writeFileSync(cut, buf.subarray(0, Math.floor(buf.length / 2)));
        const f = Buffer.from(buf); f[Math.floor(f.length / 2)] ^= 0x40; fs.writeFileSync(flip, f);
        const manifest = Buffer.from(JSON.stringify({ format: 1, created: NOW, source: 'x', files: [['../../evil.txt', 3]] })), len = Buffer.alloc(4);
        len.writeUInt32LE(manifest.length, 0);
        fs.writeFileSync(evil, zlib.gzipSync(Buffer.concat([Buffer.from('NXTSDBK1'), len, manifest, Buffer.from('bad')])));
        fs.writeFileSync(junk, zlib.gzipSync(Buffer.from('hello, this is not a backup')));
        for (const [bad, why] of [[cut, /cut short/], [flip, /not a valid backup/], [evil, /not one of a database/], [junk, /does not start like one/]]) {
            await assert.rejects(db.admin({ op: 'restore', file: bad, confirm: 'RESTORE' }), why);
            assert.deepStrictEqual(plain(await db.query(ALL)), before, path.basename(bad) + ': the live database is unchanged');
        }
        assert.ok(!fs.existsSync(path.join(root, 'evil.txt')) && !fs.existsSync(path.join(os.tmpdir(), 'evil.txt')), 'nothing written outside the folder');
        assert.ok(!fs.readdirSync(root).some((n) => n.includes('.restore-')), 'no half restored folder left');
    });

    await ok('during a backup the operations that rename or delete files wait (refused with the reason); stats and tags answer', async () => {
        const d = tmp(), e = new Engine(d, { walSync: false, checkpointMs: 1e9, walFlushMs: 1e9 }).open();
        fill((n, t, v) => e.write(n, t, v), T0, NOW - 1000);
        const p = admin.run(e, { op: 'backup', file: path.join(d + '-bk', 'x.tsdb.gz') });
        assert.throws(() => admin.run(e, { op: 'compact' }), /a backup is running/);
        assert.throws(() => admin.run(e, { op: 'deleteRange', tags: 'Line1.Power', from: T0, to: T0 + 1000 }), /a backup is running/);
        assert.ok(admin.run(e, { op: 'stats' }).tags === 3);
        const r = await p;
        assert.ok(fs.existsSync(r.file));
        admin.run(e, { op: 'compact' });   // free again
        assert.throws(() => admin.run(e, { op: 'backup', file: path.join(d, 'inside.tsdb.gz') }), /inside the database folder/);
        e.close();
    });

    await ok('offline: node lib/backup.js restore <file> <folder> gives the same database (for a database that does not open)', async () => {
        const out = path.join(tmp(), 'restored');
        const r = JSON.parse(execFileSync(process.execPath, [path.join(__dirname, '..', 'lib', 'backup.js'), 'restore', file, out]).toString());
        assert.strictEqual(r.dir, out);
        const e = new Engine(out, { walSync: false, checkpointMs: 1e9, walFlushMs: 1e9 }).open();
        const got = plain(Q.run(e, ALL));
        for (const tag of ['Line1.Power', 'Line1.Running', 'Line1.State']) assert.deepStrictEqual(got[tag], atBackup[tag], tag);
        e.close();
        assert.throws(() => execFileSync(process.execPath, [path.join(__dirname, '..', 'lib', 'backup.js'), 'restore', file, out], { stdio: 'pipe' }), /not empty/);
    });

    await db.close();
    console.log(`\n${passed} passed\nALL OK`);
})().catch((e) => { console.error(e); process.exit(1); });
