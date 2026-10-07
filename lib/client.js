'use strict';
// The main-thread side of the worker: write() only puts a point in a batch (typed arrays, no object per point); the
// batch goes to the worker every batchMs or when full (its buffers transferred, not copied). A tag name crosses once;
// after that only its number. query() / tags() / checkpoint() / close() are Promises.
//
//   const db = openHistorian(dir, opts);       db.ready (a Promise)
//   db.write(tag, ts, value)                    value: number | boolean | string; false: not a value this core keeps
//   await db.query({ tags, from, to, mode, ... })
//   db.stats  (from the worker, every second): points, late, badType, chunks, ...; overload (refused here)
//
// Backpressure: the points sent and not yet written by the worker are counted; past maxInFlight (default 2 M,
// about 40 MB) write() refuses a point (stats.overload, the last one in stats.lastRefused) instead of letting
// a backlog grow until the process runs out of memory.
//   await db.close()                            the batch, a checkpoint, the worker ends
const path = require('path');
const { Worker } = require('worker_threads');

const CAP = 16384;

class Historian {
    constructor(dir, opts) {
        const o = Object.assign({ batchMs: 50, maxInFlight: 2000000 }, opts || {});
        this.inFlight = 0;
        this.overload = 0;
        this.o = o;
        this.ids = new Map();       // tag name -> local id
        this.defs = [];             // names not yet sent
        this.stats = { points: 0, late: 0, badType: 0, queued: 0 };
        this.pending = new Map();
        this.seq = 0;
        this.closed = false;
        this.error = null;
        this._fresh();
        this.worker = new Worker(path.join(__dirname, 'worker.js'), { workerData: { dir, opts: o, statsMs: o.statsMs }, env: Object.assign({}, process.env, o.workerEnv || {}) });
        this.ready = new Promise((resolve, reject) => { this._ready = resolve; this._fail = reject; });
        this.ready.catch(() => {});
        this.worker.on('message', (m) => this._onMessage(m));
        this.worker.on('error', (e) => this._down(e));
        // the worker gone (a crash, a terminate, a close): every request still waiting is refused, never left hanging
        this.worker.on('exit', (code) => {
            const e = new Error('the historian worker stopped (code ' + code + ')');
            this.pending.forEach((p) => p.reject(e));
            this.pending.clear();
            // a worker that ended without closing (terminated, crashed) cannot release its folder's LOCK: its process (this one) is alive
            const l = this.info && this.info.lock;
            if (l) { try { if (require('fs').readFileSync(l.file, 'utf8') === l.token) require('fs').unlinkSync(l.file); } catch (x) { /* gone */ } }
            if (!this.closed) this._down(e);
        });
        this.timer = setInterval(() => this.flush(), o.batchMs);
        if (this.timer.unref) this.timer.unref();
    }

    _fresh() {
        this.b = { ids: new Uint32Array(CAP), ts: new Float64Array(CAP), vals: new Float64Array(CAP), kinds: new Uint8Array(CAP), strs: [], n: 0 };
    }

    write(tag, ts, value) {
        if (this.closed) return false;
        const k = typeof value === 'number' ? 0 : typeof value === 'boolean' ? 1 : typeof value === 'string' ? 2 : -1;
        if (k < 0) return false;
        if (this.error) {
            this.overload++;
            this.lastOverload = { tag, ts, lastTs: null, reason: 'historian down: ' + this.error.message, value, at: Date.now() };
            return false;
        }
        if (this.inFlight + this.b.n >= this.o.maxInFlight) {
            this.overload++;
            this.lastOverload = { tag, ts, lastTs: null, reason: 'overload: the historian is ' + this.inFlight.toLocaleString() + ' points behind (writes faster than it stores)', value, at: Date.now() };
            return false;
        }
        let id = this.ids.get(tag);
        if (id === undefined) { id = this.ids.size; this.ids.set(tag, id); this.defs.push([id, tag]); }
        const b = this.b, i = b.n;
        b.ids[i] = id; b.ts[i] = ts; b.kinds[i] = k;
        if (k === 0) b.vals[i] = value; else if (k === 1) b.vals[i] = value ? 1 : 0; else { b.vals[i] = 0; b.strs.push(value); }
        b.n++;
        this.stats.queued++;
        if (b.n === CAP) this.flush();
        return true;
    }

    flush() {
        const b = this.b;
        if (!b.n && !this.defs.length) return;
        this.inFlight += b.n;
        this.worker.postMessage({ op: 'batch', defs: this.defs, ids: b.ids, ts: b.ts, vals: b.vals, kinds: b.kinds, strs: b.strs, n: b.n }, [b.ids.buffer, b.ts.buffer, b.vals.buffer, b.kinds.buffer]);
        this.defs = [];
        this._fresh();
    }

    _call(op, extra) {
        if (this.error) return Promise.reject(this.error);
        if (this.closed && op !== 'close') return Promise.reject(new Error('the historian is closed'));
        this.flush();   // a query sees every point written before it
        const id = ++this.seq;
        return new Promise((resolve, reject) => { this.pending.set(id, { resolve, reject }); this.worker.postMessage(Object.assign({ op, id }, extra)); });
    }
    query(q, now) { return this._call('query', { q, now }); }
    tags() { return this._call('tags'); }
    checkpoint() { return this._call('checkpoint'); }
    retention(now) { return this._call('retention', { now }); }
    /** { op: "tags" | "stats" | "dropTag" | "deleteRange" | "dropAll" | "compact", ... } (lib/admin.js) */
    admin(req) { return this._call('admin', { req }); }

    async close() {
        if (this.closed) return;
        clearInterval(this.timer);
        if (this.error) { this.closed = true; return; }
        const done = this._call('close');
        this.closed = true;
        await done;
        await new Promise((r) => { if (this.worker.threadId < 0) r(); else this.worker.once('exit', r); });
    }

    _onMessage(m) {
        if (m.op === 'ready') { this.info = m; this._ready(m); return; }
        if (m.op === 'ack') { this.inFlight -= m.n; return; }
        if (m.op === 'recovered') { this.recoveries = (this.recoveries || 0) + 1; return; }
        if (m.op === 'stats') {
            Object.assign(this.stats, m.stats, { overload: this.overload, inFlight: this.inFlight });
            if (this.lastOverload && (!this.stats.lastRefused || this.lastOverload.at > this.stats.lastRefused.at)) this.stats.lastRefused = this.lastOverload;
            return;
        }
        if (m.op === 'error') { const e = new Error(m.error); if (!this.info) this._fail(e); if (this.onError) this.onError(e); return; }
        if (m.op === 'reply') {
            const p = this.pending.get(m.id);
            if (!p) return;
            this.pending.delete(m.id);
            if (m.error) p.reject(new Error(m.error)); else p.resolve(m.result);
        }
    }

    _down(e) {
        if (this.error) return;
        this.error = e;
        clearInterval(this.timer);
        this.inFlight = 0;
        this.stats.lastRefused = { tag: null, ts: null, lastTs: null, reason: 'historian down: ' + e.message, at: Date.now() };
        this._fail(e);
        this.pending.forEach((p) => p.reject(e));
        this.pending.clear();
        if (this.onError) this.onError(e);
    }
}

function openHistorian(dir, opts) { return new Historian(dir, opts); }

module.exports = { openHistorian, Historian };
