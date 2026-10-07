'use strict';
// The aggregates that need the points in time order, not only a bucket's first / last / min / max / sum / count:
//
//   delta      the change of the value across a bucket: its last value minus the last value before it (the bridge from the
//              previous bucket is the bucket's own, so the deltas of consecutive buckets add up to the delta of the whole range)
//   increase   a counter's increase: the sum of the steps up; a drop is a restart from 0 (the new value is the step), so a meter
//              that resets and counts again is counted right
//   integral   the area under the value over time (kW -> kWh with per: "h"); the points joined by lines (linear) or each value held
//              until the next point (step); an interval that crosses a bucket's edge is split there
//   twa        the time-weighted average: the integral over the time it covers
//   states     occurrences / entries / duration / counts / durations of a state (a string, a bool, a code)
//
// A bucket is fed by the engine's scan in time order with what it has at hand: a whole summary where it lies in one bucket, else the
// points under it. Each element is the first point, the last point and what lies inside it (inc, integrals); between two elements
// the bridge is computed here. A summary of an older format (its inc / integrals NaN) is not used whole: the points under it are.
const { F } = require('./layout');

const UNITS = { ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000 };

// the policy a summary's stored `inc` was made with; a query that asks for another cannot use it and reads the points
const DEFAULT_POLICY = { reset: 'restart', tolerance: 0, maxStep: Infinity, ignoreZero: false, maxGap: Infinity };

function policyOf(o) {
    const p = Object.assign({}, DEFAULT_POLICY);
    for (const k of Object.keys(o || {})) if (o[k] !== undefined && o[k] !== null) p[k] = o[k];
    if (p.reset !== 'restart' && p.reset !== 'ignore') throw new Error('counter.reset must be "restart" or "ignore"');
    for (const k of ['tolerance', 'maxStep', 'maxGap']) if (!(typeof p[k] === 'number' && p[k] >= 0)) throw new Error('counter.' + k + ' must be a number of 0 or more');
    p.custom = p.reset !== DEFAULT_POLICY.reset || p.tolerance !== 0 || p.maxStep !== Infinity || !!p.ignoreZero || p.maxGap !== Infinity;
    return p;
}

class SeriesAgg {
    /**
     * @param o { nb, origin, size, b0, flags: { delta, inc, integral, state }, policy, target (a state's stored value, or undefined) }
     */
    constructor(o) {
        this.nb = o.nb; this.org = o.origin; this.size = o.size; this.b0 = o.b0;
        this.f = o.flags; this.p = o.policy; this.target = o.target;
        const n = this.nb, mk = (v) => new Float64Array(n).fill(v);
        this.seen = new Uint8Array(n);                              // a bucket got its first element
        if (this.f.delta) { this.dBase = mk(NaN); this.dLast = mk(NaN); }
        if (this.f.inc) this.inc = mk(0);
        if (this.f.integral) { this.iL = mk(0); this.iS = mk(0); this.cov = mk(0); }
        if (this.f.state) { this.counts = new Array(n).fill(null); this.dur = new Array(n).fill(null); this.ent = new Array(n).fill(null); this.changes = mk(0); }
        this.pt = NaN; this.pv = NaN;                               // the end of the element before
        this.needsSummaryFields = !!(this.f.inc || this.f.integral);
        this.needsPoints = !!this.f.state;
    }
    idxOf(t) { return Math.floor((t - this.org) / this.size) - this.b0; }

    /** The newest point before the range: { t, v } | null. The first bucket's bridge starts there. */
    seed(pt) { if (pt && !(this.p.ignoreZero && pt.v === 0)) { this.pt = pt.t; this.pv = pt.v; } }

    /** May this summary be taken whole (it lies in one bucket)? A constant one for states; one with its fields for increase / integral. */
    accepts(r) {
        if (this.needsPoints && r[F.vMin] !== r[F.vMax]) return false;
        if (this.p.custom) return false;                            // another policy than the stored one: the points
        if (this.needsSummaryFields && !(r[F.inc] === r[F.inc] && r[F.integL] === r[F.integL] && r[F.integS] === r[F.integS])) return false;
        return true;
    }

    point(t, v) {
        if (this.p.ignoreZero && v === 0) return;
        this._el(t, v, t, v, 1, null);
    }
    rec(r) { this._el(r[F.tFirst], r[F.vFirst], r[F.tLast], r[F.vLast], r[F.count], r); }

    _el(tf, vf, tl, vl, count, r) {
        const b = this.idxOf(tf);
        const inRange = b >= 0 && b < this.nb;
        const has = this.pt === this.pt;                            // an element before this one
        if (has) this._bridge(inRange ? b : -1, this.pt, this.pv, tf, vf);
        if (inRange) {
            if (!this.seen[b]) {
                this.seen[b] = 1;
                if (this.dBase) this.dBase[b] = has ? this.pv : NaN;
            }
            if (this.dLast) this.dLast[b] = vl;
            if (r) {
                if (this.inc) this.inc[b] += r[F.inc];
                if (this.iL) { this.iL[b] += r[F.integL]; this.iS[b] += r[F.integS]; this.cov[b] += tl - tf; }
                if (this.counts) {                                  // a constant summary: all its points are one state
                    this._add(this.counts, b, vf, count);
                    this._add(this.dur, b, vf, tl - tf);
                }
            } else if (this.counts) this._add(this.counts, b, vf, 1);
        }
        this.pt = tl; this.pv = vl;
    }

    _add(arr, b, key, x) { let m = arr[b]; if (!m) m = arr[b] = new Map(); m.set(key, (m.get(key) || 0) + x); }

    // the interval from the end of the element before to the start of this one: (ta, va) -> (tb, vb); b: the bucket of tb (-1 outside)
    _bridge(b, ta, va, tb, vb) {
        if (this.inc && b >= 0) {
            const d = vb - va;
            let s;
            if (d >= 0) s = d > this.p.maxStep ? 0 : d;
            else if (-d <= this.p.tolerance) s = 0;
            else s = this.p.reset === 'restart' && vb <= this.p.maxStep ? vb : 0;
            this.inc[b] += s;
        }
        if (this.counts && b >= 0 && vb !== va) { this._add(this.ent, b, vb, 1); this.changes[b]++; }
        const gap = tb - ta;
        if ((this.iL || this.dur) && gap > 0 && gap <= this.p.maxGap) this._split(ta, va, tb, vb);
    }

    // the interval cut at the edges of the buckets it crosses: the lines are cut at their value there, a held value is held
    _split(ta, va, tb, vb) {
        const gap = tb - ta, first = Math.floor((ta - this.org) / this.size) - this.b0, last = Math.floor((tb - this.org) / this.size) - this.b0;
        for (let k = Math.max(first, 0); k <= last && k < this.nb; k++) {
            const edge0 = this.org + (k + this.b0) * this.size, edge1 = edge0 + this.size;
            const s = Math.max(ta, edge0), e = Math.min(tb, edge1);
            if (!(e > s)) continue;
            if (this.iL) {
                const vs = va + (vb - va) * (s - ta) / gap, ve = va + (vb - va) * (e - ta) / gap;
                this.iL[k] += (vs + ve) / 2 * (e - s); this.iS[k] += va * (e - s); this.cov[k] += e - s;
            }
            if (this.dur) this._add(this.dur, k, va, e - s);
        }
    }

    /** Has this bucket something to say, though it holds no point (an interval that crosses it, a state held through it)? */
    has(b) { return !!((this.cov && this.cov[b] > 0) || (this.dur && this.dur[b]) || this.seen[b]); }

    // ---- the answers --------------------------------------------------------------------------------------------
    /** delta of a bucket: last - the last value before it (anchor "start", the default) or last - first inside it ("inner") */
    delta(b, acc, inner, reverse) {
        if (!this.seen[b]) return null;
        const a = b * 10;
        const base = !inner && this.dBase[b] === this.dBase[b] ? this.dBase[b] : acc[a + 1];
        const d = this.dLast[b] - base;
        return reverse ? -d : d;
    }
    increase(b) { return this.seen[b] ? this.inc[b] : null; }
    integral(b, method, per) {
        if (!this.seen[b] && !(this.cov[b] > 0)) return null;
        return (method === 'step' ? this.iS[b] : this.iL[b]) / per;
    }
    twa(b, method, acc) {
        if (this.cov[b] > 0) return (method === 'step' ? this.iS[b] : this.iL[b]) / this.cov[b];
        return this.seen[b] ? acc[b * 10 + 8] / acc[b * 10 + 9] : null;       // one point: its own value
    }
}

module.exports = { SeriesAgg, policyOf, DEFAULT_POLICY, UNITS };
