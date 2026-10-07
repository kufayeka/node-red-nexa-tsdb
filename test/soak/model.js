'use strict';
// The data of the soak test. Every value is a PURE function of (tag, step): the generator writes it, the verifier
// recomputes any range it asks the historian for, so nothing has to be stored to know the right answer.
//
//   tags: 80 % numbers (2 decimals: a slow wave + a daily wave + noise + a rare spike of +60), 10 % booleans
//         (change about every 10 min), 10 % strings (a state: Run / Idle / Setup / Breakdown, about every 30 min)
//   gaps: about 2 % of tag-days lose 1 - 6 hours (an outage), so "no data" ranges are part of the test
const DAY = 864e5, TWO_PI = Math.PI * 2, STATES = ['Run', 'Idle', 'Setup', 'Breakdown'];

function mix(a, b) {
    let h = Math.imul(a ^ 0x9e3779b9, 0x85ebca6b) ^ Math.imul((b + 0x7f4a7c15) | 0, 0xc2b2ae35);
    h ^= h >>> 15; h = Math.imul(h, 0x2c1b3c6d); h ^= h >>> 12; h = Math.imul(h, 0x297a2d39); h ^= h >>> 15;
    return h >>> 0;
}

class Model {
    /** meta: { t0 (day aligned ms), period (ms, divides a day), tags, steps } */
    constructor(meta) {
        this.t0 = meta.t0; this.period = meta.period; this.tags = meta.tags; this.steps = meta.steps;
        this.names = []; this.kind = []; this.par = [];
        for (let i = 0; i < this.tags; i++) {
            const kind = i % 10 === 8 ? 1 : i % 10 === 9 ? 2 : 0;       // 0 number, 1 bool, 2 string
            this.kind.push(kind);
            this.names.push('Plant.Area' + (i % 5) + '.' + ['Num', 'Bool', 'State'][kind] + i);
            this.par.push({ base: 20 + (mix(i, 1) % 800) / 10, a1: 1 + (mix(i, 2) % 100) / 10, p1: (6 + (mix(i, 3) % 40)) * 3600000, a2: 0.5 + (mix(i, 5) % 30) / 10, noise: 0.3 + (mix(i, 4) % 10) / 10 });
        }
        this.index = new Map(this.names.map((n, i) => [n, i]));
    }
    time(k) { return this.t0 + k * this.period; }
    type(i) { return ['number', 'bool', 'string'][this.kind[i]]; }

    // false inside an outage of this tag
    present(i, k) {
        const tt = k * this.period, d = Math.floor(tt / DAY), h = mix(i, d + 1000000);
        if (h % 50 !== 0) return true;
        const start = (h >>> 8) % (20 * 3600000), len = 3600000 + ((h >>> 4) % (5 * 3600000)), within = tt - d * DAY;
        return !(within >= start && within < start + len);
    }

    value(i, k) {
        const kind = this.kind[i], tt = k * this.period;
        if (kind === 1) return (mix(i, Math.floor(tt / 600000)) & 1) === 1;
        if (kind === 2) return STATES[mix(i, Math.floor(tt / 1800000)) % 4];
        const p = this.par[i];
        let x = p.base + p.a1 * Math.sin(TWO_PI * (tt % p.p1) / p.p1) + p.a2 * Math.sin(TWO_PI * (tt % DAY) / DAY) + (((mix(i, k) % 2001) - 1000) / 1000) * p.noise;
        if (mix(i, k + 7777777) % 20011 === 0) x += 60;
        return Math.round(x * 100) / 100;
    }

    /** fn(t, value) for every point of tag i with from <= t <= to, in time order */
    each(i, from, to, fn) {
        const k0 = Math.max(0, Math.ceil((from - this.t0) / this.period)), k1 = Math.min(this.steps - 1, Math.floor((to - this.t0) / this.period));
        for (let k = k0; k <= k1; k++) if (this.present(i, k)) fn(this.t0 + k * this.period, this.value(i, k));
    }
    /** the newest point at or before `to`: { t, v } or null */
    lastBefore(i, to) {
        for (let k = Math.min(this.steps - 1, Math.floor((to - this.t0) / this.period)); k >= 0; k--) if (this.present(i, k)) return { t: this.t0 + k * this.period, v: this.value(i, k) };
        return null;
    }
}

module.exports = { Model, mix, DAY };
