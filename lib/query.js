'use strict';
// The query object (msg.query, the node's form; a fluent JS builder comes later and builds the same object):
//
//   { tags: "Oven1.Temp" | ["Oven*.Temp", ...],   a * matches any text
//     from: "-8h" | "now-8h" | "2026-10-01T06:00" | ms,   to: "now" (default) | ...
//     mode: "m4" (default) | "raw" | "bucket" | "last",
//     width: 1000,                         m4: the chart's pixel columns (about 4 points each, the extremes kept)
//     bucket: "1h", offset: "6h",          bucket: its size, its alignment (a shift starting at 06:00)
//     agg: ["avg", "min", "max", "sum", "count", "first", "last"],
//     fill: "none" | "null" | "previous",  bucket: what an empty bucket gives
//     limit: 1000000,                      raw: at most this many points a tag
//     format: "series" (default) | "rows" }
//
// series: { "<tag>": { type, t: [...], v: [...] } }   (bucket: t and one array per agg; last: t, v)
// rows:   [{ tag, ts, value }]                         (bucket: [{ tag, ts, avg, max, ... }])
// A string tag's values come back as text; its min / max / avg / sum are null.

const UNITS = { ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000, w: 604800000, mo: 2592000000, y: 31536000000 };

function parseDuration(x) {
    if (typeof x === 'number') return x;
    const m = /^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w|mo|y)\s*$/.exec(String(x || ''));
    if (!m) throw new Error('not a duration: ' + x + ' (e.g. 500ms, 30s, 5m, 1h, 7d)');
    return Number(m[1]) * UNITS[m[2]];
}

function parseTime(x, now) {
    if (x === undefined || x === null || x === '' || x === 'now') return now;
    if (typeof x === 'number') return x;
    if (x instanceof Date) return x.getTime();
    const s = String(x).trim();
    const rel = /^(?:now)?\s*([+-])\s*(.+)$/.exec(s);
    if (rel && !/^\d{4}-/.test(s)) return now + (rel[1] === '-' ? -1 : 1) * parseDuration(rel[2]);
    if (/^\d+$/.test(s)) return Number(s);
    const t = Date.parse(s);
    if (!Number.isFinite(t)) throw new Error('not a time: ' + x);
    return t;
}

// a tag pattern: * matches any text
function globRe(pat) { return new RegExp('^' + String(pat).split('*').map((p) => p.replace(/[.+?^${}()|[\]\\]/g, '\\$&')).join('.*') + '$'); }

function matchTags(engine, tags) {
    const list = Array.isArray(tags) ? tags : [tags];
    const out = [];
    for (const pat of list) {
        if (typeof pat !== 'string' || !pat) continue;
        if (!pat.includes('*')) { const t = engine.byName.get(pat); if (t && !out.includes(t)) out.push(t); continue; }
        const re = globRe(pat);
        engine.tags.forEach((t) => { if (t && re.test(t.name) && !out.includes(t)) out.push(t); });
    }
    return out;
}

const AGGS = ['avg', 'min', 'max', 'sum', 'count', 'first', 'last'];

function run(engine, q, nowArg) {
    if (!q || typeof q !== 'object') throw new Error('a query is an object: { tags, from, to, mode, ... }');
    const now = nowArg === undefined ? Date.now() : nowArg;
    const from = parseTime(q.from === undefined ? '-1h' : q.from, now), to = parseTime(q.to, now);
    if (!(to >= from)) throw new Error('to is before from');
    const tags = matchTags(engine, q.tags);
    const mode = q.mode || 'm4', rows = q.format === 'rows';
    const out = rows ? [] : {};
    for (const tag of tags) {
        const word = tag.type === 'string' ? (v) => (v === v && v !== null ? tag.words[v] : null) : tag.type === 'bool' ? (v) => (v === v && v !== null ? v === 1 : null) : (v) => (v === v ? v : null);
        if (mode === 'raw' || mode === 'last') {
            let t, v;
            if (mode === 'last') {
                // the newest point at or before `to`: the open chunk, else the last chunk that starts before it
                const r = engine.raw(tag, Math.max(from, to - 7 * UNITS.d), to);
                const n = r.t.length;
                t = n ? [r.t[n - 1]] : []; v = n ? [word(r.v[n - 1])] : [];
            } else {
                const r = engine.raw(tag, from, to, q.limit || 1000000);
                t = Array.from(r.t); v = Array.from(r.v, word);
            }
            if (rows) t.forEach((ts, i) => out.push({ tag: tag.name, ts, value: v[i] }));
            else out[tag.name] = { type: tag.type, t, v };
            continue;
        }
        if (mode === 'm4') {
            const width = Math.max(1, Math.min(100000, Math.round(q.width || 1000)));
            const size = Math.max(1, (to - from + 1) / width);
            const b = engine.buckets(tag, from, to, size, from, 'm4');
            const t = [], v = [];
            for (let i = 0; i < b.nb; i++) {
                const a = i * 10;
                if (!(b.acc[a + 9] > 0)) continue;
                // first, min, max, last in time order, each once
                const pts = [[b.acc[a], b.acc[a + 1]], [b.acc[a + 4], b.acc[a + 5]], [b.acc[a + 6], b.acc[a + 7]], [b.acc[a + 2], b.acc[a + 3]]].sort((x, y) => x[0] - y[0]);
                let last = NaN;
                for (const [pt, pv] of pts) { if (pt === last) continue; last = pt; t.push(pt); v.push(word(pv)); }
            }
            if (rows) t.forEach((ts, i) => out.push({ tag: tag.name, ts, value: v[i] }));
            else out[tag.name] = { type: tag.type, t, v };
            continue;
        }
        if (mode === 'bucket') {
            const size = parseDuration(q.bucket || '1h'), off = q.offset ? parseDuration(q.offset) : 0;
            const origin = Math.floor((from - off) / size) * size + off;
            const b = engine.buckets(tag, from, to, size, origin, 'bucket');
            const aggs = (Array.isArray(q.agg) ? q.agg : [q.agg || 'avg']).filter((x) => AGGS.includes(x));
            const numeric = tag.type !== 'string';
            const col = { t: [] };
            aggs.forEach((k) => (col[k] = []));
            let prev = null;
            for (let i = 0; i < b.nb; i++) {
                const a = i * 10, n = b.acc[a + 9], ts = b.start + i * b.size;
                if (!(n > 0)) {
                    if (!q.fill || q.fill === 'none') continue;
                    col.t.push(ts);
                    aggs.forEach((k) => col[k].push(q.fill === 'previous' && prev ? prev[k] : k === 'count' ? 0 : null));
                    continue;
                }
                const val = {
                    avg: numeric ? b.acc[a + 8] / n : null, min: numeric ? word(b.acc[a + 5]) : null, max: numeric ? word(b.acc[a + 7]) : null,
                    sum: numeric ? b.acc[a + 8] : null, count: n, first: word(b.acc[a + 1]), last: word(b.acc[a + 3])
                };
                col.t.push(ts);
                aggs.forEach((k) => col[k].push(val[k]));
                prev = val;
            }
            if (rows) col.t.forEach((ts, i) => { const r = { tag: tag.name, ts }; aggs.forEach((k) => (r[k] = col[k][i])); out.push(r); });
            else out[tag.name] = Object.assign({ type: tag.type }, col);
            continue;
        }
        throw new Error('unknown mode: ' + mode);
    }
    return out;
}

module.exports = { run, parseTime, parseDuration, matchTags, globRe };
