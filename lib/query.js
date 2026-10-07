'use strict';
// The query object (msg.query, the node's form; a fluent JS builder comes later and builds the same object):
//
//   { tags: "Oven1.Temp" | ["Oven*.Temp", ...],   a * matches any text
//     from: "-8h" | "now-8h" | "2026-10-01T06:00" | ms,   to: "now" (default) | ...
//     mode: "m4" (default) | "raw" | "bucket" | "last",
//     width: 1000,                         m4: the chart's pixel columns (about 4 points each, the extremes kept)
//     exact: false,                        m4: the fast form (default is exact: every column's own min / max; a chunk straddling a column
//                                          edge is decoded, +0.1 - 0.3 s over 5 years). Fast: such a chunk is placed by its four points,
//                                          a column's min / max can then miss a point within one chunk of its edge
//     bucket: "1h", offset: "6h",          bucket: its size, its alignment (a shift starting at 06:00)
//     agg: ["avg", "min", "max", "sum", "count", "first", "last"],
//     fill: "none" | "null" | "previous",  bucket: what an empty bucket gives
//     limit: 1000000,                      raw: at most this many points a tag
//     maxPoints: 5000000,                  the most points in the whole answer (more: refused with the reason)
//     page: true,                          raw: an answer past the limit is returned as a page, { t, v, more: true, next: <ts> }; ask the
//                                          next page with from: next (same to). Without page, such an answer is an error, never a cut result
//     format: "series" (default) | "rows" }
//
// series: { "<tag>": { type, t: [...], v: [...] } }   (bucket: t and one array per agg; last: t, v)
// rows:   [{ tag, ts, value }]                         (bucket: [{ tag, ts, avg, max, ... }])
// clippedFrom: present when `from` is before what retention keeps (raw: rawDays / raw; else keep): the answer starts there.
// A string tag's values come back as text; its min / max / avg / sum are null.

const UNITS_PER = { ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000 };
const { policyOf } = require('./rollup');
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

const AGGS = ['avg', 'min', 'max', 'sum', 'count', 'first', 'last', 'range', 'delta', 'increase', 'integral', 'twa', 'occurrences', 'entries', 'duration', 'changes', 'counts', 'durations'];
const NEEDS_VALUE = ['occurrences', 'entries', 'duration'];
const STATE_AGGS = ['occurrences', 'entries', 'duration', 'changes', 'counts', 'durations'];

function parseAggs(agg) {
    const list = Array.isArray(agg) ? agg : [agg || 'avg'];
    for (const k of list) if (!AGGS.includes(k)) throw new Error('unknown aggregate "' + (typeof k === 'object' ? JSON.stringify(k) : k) + '" (known: ' + AGGS.join(', ') + ')');
    return list;
}

// What the aggregates that need the points in time order ask of the engine, and how their answers are read out of it (lib/rollup.js).
// null when the query has none of them.
function seriesSpec(q, tag, aggs) {
    const has = (names) => aggs.some((k) => names.includes(k));
    const flags = { delta: has(['delta']), inc: has(['increase']), integral: has(['integral', 'twa']), state: has(STATE_AGGS) };
    if (!flags.delta && !flags.inc && !flags.integral && !flags.state) return null;
    if (q.anchor !== undefined && q.anchor !== 'start' && q.anchor !== 'inner') throw new Error('anchor must be "start" or "inner"');
    if (q.method !== undefined && q.method !== 'linear' && q.method !== 'step') throw new Error('method must be "linear" or "step"');
    const per = q.per === undefined ? 'h' : q.per;
    if (!UNITS_PER[per]) throw new Error('per must be one of ' + Object.keys(UNITS_PER).join(', '));
    const policy = policyOf({ reset: q.reset, tolerance: q.tolerance, maxStep: q.maxStep, ignoreZero: q.ignoreZero, maxGap: q.maxGap === undefined ? undefined : parseDuration(q.maxGap) });
    for (const k of NEEDS_VALUE) if (aggs.includes(k) && q.value === undefined) throw new Error(k + ' needs a value: the state to count (value: "Running")');
    // a state is stored as a number: a string tag's dictionary id, a bool's 0 / 1, a number as it is
    let target;
    if (q.value !== undefined) {
        if (tag.type === 'string') { const id = tag.dict.get(String(q.value)); target = id === undefined ? NaN : id; }
        else if (tag.type === 'bool') target = q.value === true || q.value === 'true' || q.value === 1 || q.value === '1' ? 1 : q.value === false || q.value === 'false' || q.value === 0 || q.value === '0' ? 0 : NaN;
        else target = Number(q.value);
    }
    const numeric = tag.type !== 'string', inner = q.anchor === 'inner', reverse = !!q.reverse, method = q.method || 'linear', div = UNITS_PER[per];
    const stateKey = (k) => (tag.type === 'string' ? tag.words[k] : tag.type === 'bool' ? k === 1 : k);
    const hist = (m, scale) => { if (!m) return {}; const o = {}; for (const [k, x] of m) o[String(stateKey(k))] = scale ? x / scale : x; return o; };
    return {
        ext: { flags, policy, target },
        values(sa, i, acc, isNumeric) {
            const v = {};
            if (flags.delta) v.delta = isNumeric ? sa.delta(i, acc, inner, reverse) : null;
            if (flags.inc) v.increase = isNumeric ? sa.increase(i) : null;
            if (flags.integral) { v.integral = isNumeric ? sa.integral(i, method, div) : null; v.twa = isNumeric ? sa.twa(i, method, acc) : null; }
            if (flags.state) {
                const get = (arr) => (arr[i] && target === target ? arr[i].get(target) || 0 : 0);
                v.occurrences = get(sa.counts); v.entries = get(sa.ent); v.duration = get(sa.dur); v.changes = sa.changes[i];
                v.counts = hist(sa.counts[i]); v.durations = hist(sa.dur[i]);
            }
            return v;
        }
    };
}

function run(engine, q, nowArg) {
    if (!q || typeof q !== 'object') throw new Error('a query is an object: { tags, from, to, mode, ... }');
    const now = nowArg === undefined ? Date.now() : nowArg;
    // `last` needs no from (the newest point at or before `to`, however old); every other mode defaults to the last hour
    const from = parseTime(q.from === undefined ? (q.mode === 'last' ? 0 : '-1h') : q.from, now), to = parseTime(q.to, now);
    if (!(to >= from)) throw new Error('to is before from');
    const tags = matchTags(engine, q.tags);
    const mode = q.mode || 'm4', rows = q.format === 'rows';
    const aggs = mode === 'bucket' || mode === 'range' ? parseAggs(q.agg) : [];
    const rawOnly = new Set();                      // tags whose answer came from raw points only (a counter policy of its own)
    for (const k of ['limit', 'maxPoints', 'width']) if (q[k] !== undefined && q[k] !== null && !(typeof q[k] === 'number' && q[k] >= 1 && Number.isFinite(q[k]))) throw new Error(k + ' must be a number of 1 or more (got ' + (typeof q[k] === 'object' ? JSON.stringify(q[k]) : q[k]) + ')');
    const out = rows ? [] : {};
    // an answer past maxPoints (all tags together) is refused with its reason, never built until memory runs out
    const cap = q.maxPoints || 5000000;
    let total = 0;
    const tooBig = (n) => new Error('the answer would be about ' + n.toLocaleString('en-US') + ' points (more than maxPoints ' + cap.toLocaleString('en-US') + '): narrow the range or the tags, use mode m4 (a chart) or bucket (an aggregate), or raise maxPoints');
    const account = (n) => { total += n; if (total > cap) throw tooBig(total); };
    for (const tag of tags) {
        const word = tag.type === 'string' ? (v) => (v === v && v !== null ? tag.words[v] : null) : tag.type === 'bool' ? (v) => (v === v && v !== null ? v === 1 : null) : (v) => (v === v ? v : null);
        if (mode === 'raw' || mode === 'last') {
            let t, v, more = false, next = null;
            if (mode === 'last') {
                // the newest point at or before `to`, however old (from does not limit it unless given):
                // a tag's last point is in memory (no file read); else its chunks, a week back at a time
                const cut = q.from === undefined ? -Infinity : from;
                if (tag.lastKnown && tag.lastT <= to && tag.lastT >= cut && tag.lastT >= engine._cut(tag, false)) {
                    t = [tag.lastT]; v = [word(tag.lastV)];
                } else {
                    let r = { t: [] }, hi = to;
                    for (let k = 0; k < 520 && !r.t.length && hi >= cut && hi > engine._cut(tag, false); k++, hi -= 7 * UNITS.d) r = engine.raw(tag, Math.max(cut, hi - 7 * UNITS.d), hi);
                    const n = r.t.length;
                    t = n ? [r.t[n - 1]] : []; v = n ? [word(r.v[n - 1])] : [];
                }
            } else {
                // one point more than the limit is read: that is how a cut answer is known (and never returned as if whole)
                const limit = Math.min(q.limit || 1000000, cap - total), r = engine.raw(tag, from, to, limit + 1);
                let n = r.t.length;
                if (n > limit) {
                    if (limit < (q.limit || 1000000)) throw tooBig(total + n);
                    if (!q.page) throw new Error('"' + tag.name + '" has more than ' + limit.toLocaleString('en-US') + ' points in the range (the first ' + limit.toLocaleString('en-US') + ' end at ' + new Date(r.t[limit - 1]).toISOString() + '). Nothing is returned cut: use { page: true } and ask on from: the answer\'s next, or raise limit (the whole answer is held to maxPoints), or use mode m4 / bucket.');
                    if (rows) throw new Error('paging needs format: series (the answer carries more / next)');
                    more = true; next = r.t[limit]; n = limit;
                }
                t = Array.from(r.t.subarray(0, n)); v = Array.from(r.v.subarray(0, n), word);
            }
            account(t.length);
            if (rows) t.forEach((ts, i) => out.push({ tag: tag.name, ts, value: v[i] }));
            else out[tag.name] = q.page && mode === 'raw' ? { type: tag.type, t, v, more, next } : { type: tag.type, t, v };
            continue;
        }
        if (mode === 'm4') {
            const width = Math.max(1, Math.min(100000, Math.round(q.width || 1000)));
            const size = Math.max(1, (to - from + 1) / width);
            const b = engine.buckets(tag, from, to, size, from, 'm4', q.exact !== false);
            const t = [], v = [];
            for (let i = 0; i < b.nb; i++) {
                const a = i * 10;
                if (!(b.acc[a + 9] > 0)) continue;
                // first, min, max, last in time order, each once
                const pts = [[b.acc[a], b.acc[a + 1]], [b.acc[a + 4], b.acc[a + 5]], [b.acc[a + 6], b.acc[a + 7]], [b.acc[a + 2], b.acc[a + 3]]].sort((x, y) => x[0] - y[0]);
                let last = NaN;
                for (const [pt, pv] of pts) { if (pt === last) continue; last = pt; t.push(pt); v.push(word(pv)); }
            }
            account(t.length);
            if (rows) t.forEach((ts, i) => out.push({ tag: tag.name, ts, value: v[i] }));
            else out[tag.name] = { type: tag.type, t, v };
            continue;
        }
        if (mode === 'bucket' || mode === 'range') {
            // range: the whole of [from, to] as one bucket
            const size = mode === 'range' ? to - from + 1 : parseDuration(q.bucket || '1h');
            const off = mode === 'range' ? ((from % size) + size) % size : q.offset ? parseDuration(q.offset) : 0;
            const origin = Math.floor((from - off) / size) * size + off;
            const expected = Math.floor((to - origin) / size) - Math.floor((from - origin) / size) + 1;
            if (total + expected > cap) throw tooBig(total + expected);
            const spec = seriesSpec(q, tag, aggs);
            const b = engine.buckets(tag, from, to, size, origin, 'bucket', undefined, spec && spec.ext);
            if (spec && spec.ext.policy.custom) rawOnly.add(tag.name);
            const sa = b.sa, numeric = tag.type !== 'string';
            const col = { t: [] };
            aggs.forEach((k) => (col[k] = []));
            let prev = null;
            for (let i = 0; i < b.nb; i++) {
                const a = i * 10, n = b.acc[a + 9], ts = b.start + i * b.size;
                if (!(n > 0) && !(sa && sa.has(i))) {
                    if (!q.fill || q.fill === 'none') continue;
                    col.t.push(ts);
                    aggs.forEach((k) => col[k].push(q.fill === 'previous' && prev ? prev[k] : k === 'count' ? 0 : null));
                    continue;
                }
                const val = n > 0 ? {
                    avg: numeric ? b.acc[a + 8] / n : null, min: numeric ? word(b.acc[a + 5]) : null, max: numeric ? word(b.acc[a + 7]) : null,
                    sum: numeric ? b.acc[a + 8] : null, count: n, first: word(b.acc[a + 1]), last: word(b.acc[a + 3]),
                    range: numeric ? b.acc[a + 7] - b.acc[a + 5] : null
                } : { avg: null, min: null, max: null, sum: null, count: 0, first: null, last: null, range: null };
                if (spec) Object.assign(val, spec.values(sa, i, b.acc, numeric));
                col.t.push(ts);
                aggs.forEach((k) => col[k].push(val[k]));
                prev = val;
            }
            account(col.t.length);
            if (rows) col.t.forEach((ts, i) => { const r = { tag: tag.name, ts }; aggs.forEach((k) => (r[k] = col[k][i])); out.push(r); });
            else out[tag.name] = Object.assign({ type: tag.type }, col);
            continue;
        }
        throw new Error('unknown mode: ' + mode);
    }
    // a range that starts before what retention keeps is answered from what is kept, and says so: `clippedFrom` is the oldest time
    // that could be answered (raw: the tag's raw keep; the other modes: its keep). Never a silently shorter answer.
    if (!rows) for (const tag of tags) {
        const cut = engine._cut(tag, mode === 'raw' || rawOnly.has(tag.name)), o = out[tag.name];
        if (o && Number.isFinite(cut) && from < cut) o.clippedFrom = cut;
    }
    return out;
}

module.exports = { run, parseTime, parseDuration, matchTags, globRe };
