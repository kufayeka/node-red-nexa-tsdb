'use strict';
// The query object (msg.query, the node's form; a fluent JS builder comes later and builds the same object):
//
//   { tags: "Oven1.Temp" | ["Oven*.Temp", ...],   a * matches any text
//     from: "-8h" | "now-8h" | "2026-10-01T06:00" | ms,   to: "now" (default) | ...
//     mode: "bucket" (default) | "range" | "raw" | "last",
//     bucket: "1h", offset: "6h",          bucket: its size, its alignment (a shift starting at 06:00); range: the whole [from, to] as one bucket
//     agg: ["avg", "min", "max", "sum", "count", "first", "last", "range", "delta", "increase", "integral", "twa",
//           "occurrences", "entries", "duration", "changes", "counts", "durations",
//           "stddev", "variance", "median", "p95", "p99.9" ...]   (see the README)
//     population: true,                    stddev / variance of the values as the whole population (divide by n); default a sample (n - 1)
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

// tz: a date or a date and time without a zone ("2026-01-31", "2026-01-31T06:00") is read as that zone's clock; with an offset or Z it is exact
function parseTime(x, now, tz) {
    if (x === undefined || x === null || x === '' || x === 'now') return now;
    if (typeof x === 'number') return x;
    if (x instanceof Date) return x.getTime();
    const s = String(x).trim();
    const rel = /^(?:now)?\s*([+-])\s*(.+)$/.exec(s);
    if (rel && !/^\d{4}-/.test(s)) return now + (rel[1] === '-' ? -1 : 1) * parseDuration(rel[2]);
    if (/^\d+$/.test(s)) return Number(s);
    if (tz) { const l = require('./calendar').parseLocal(s, tz); if (l !== null) return l; }
    const t = Date.parse(s);
    if (!Number.isFinite(t)) throw new Error('not a time: ' + x);
    return t;
}

// a tag pattern: * matches any text
function globRe(pat) { return new RegExp('^' + String(pat).split('*').map((p) => p.replace(/[.+?^${}()|[\]\\]/g, '\\$&')).join('.*') + '$'); }

// the tags a pattern list names. active: the store each is read from NOW (a disk tag that a RAM pattern matches is read from its RAM twin);
// without it the tags as they are on disk (what the admin operations act on)
function matchTags(engine, tags, active) {
    const list = Array.isArray(tags) ? tags : [tags];
    const out = [];
    const add = (t) => { if (active) t = engine.active(t); if (!out.includes(t)) out.push(t); };
    for (const pat of list) {
        if (typeof pat !== 'string' || !pat) continue;
        if (!pat.includes('*')) { const t = engine.byName.get(pat); if (t) add(t); continue; }
        const re = globRe(pat);
        engine.tags.forEach((t) => { if (t && re.test(t.name)) add(t); });
    }
    return out;
}

const AGGS = ['avg', 'min', 'max', 'sum', 'count', 'first', 'last', 'range', 'delta', 'increase', 'integral', 'twa', 'occurrences', 'entries', 'duration', 'changes', 'counts', 'durations'];
const NEEDS_VALUE = ['occurrences', 'entries', 'duration'];
const STATE_AGGS = ['occurrences', 'entries', 'duration', 'changes', 'counts', 'durations'];

// stddev / variance come out of the summaries (each keeps its m2); a percentile needs the raw points, so it is answered only where they are kept (rawDays)
const SPREAD_AGGS = ['stddev', 'variance'];
// a percentile: "median" (p50) or "p" and a number from 0 to 100 ("p95", "p99.9"); its fraction, or null when k is not one
function pctOf(k) {
    if (k === 'median') return 0.5;
    const m = typeof k === 'string' ? /^p(\d{1,3}(?:\.\d+)?)$/.exec(k) : null;
    return m && Number(m[1]) <= 100 ? Number(m[1]) / 100 : null;
}

function parseAggs(agg) {
    const list = Array.isArray(agg) ? agg : [agg || 'avg'];
    for (const k of list) if (!AGGS.includes(k) && !SPREAD_AGGS.includes(k) && pctOf(k) === null) throw new Error('unknown aggregate "' + (typeof k === 'object' ? JSON.stringify(k) : k) + '" (known: ' + AGGS.concat(SPREAD_AGGS).join(', ') + ', median, p0 ... p100)');
    return list;
}

// the value at fraction p of sorted values (linear between the two closest ranks, as numpy and Excel's PERCENTILE.INC)
function quantile(sorted, p) {
    const n = sorted.length;
    if (!n) return null;
    const h = (n - 1) * p, lo = Math.floor(h);
    return lo + 1 < n ? sorted[lo] + (h - lo) * (sorted[lo + 1] - sorted[lo]) : sorted[lo];
}

// the percentiles of each bucket, from the raw points: [{ <agg>: value }] by bucket (null: a bucket with no raw point)
function percentiles(engine, tag, from, to, b, pcts, cap) {
    const r = engine.raw(tag, from, to, cap + 1);
    if (r.t.length > cap) throw new Error('"' + tag.name + '" has more than ' + cap.toLocaleString('en-US') + ' points in the range: a percentile reads every raw point (they are not in the summaries); narrow the range or raise maxPoints');
    const out = new Array(b.nb).fill(null);
    // the points are in time order and a bucket is a span of time: each bucket is one run of them
    for (let i = 0; i < r.t.length;) {
        const k = b.idxOf(r.t[i]);
        let j = i + 1;
        while (j < r.t.length && b.idxOf(r.t[j]) === k) j++;
        if (k >= 0 && k < b.nb) {
            const vs = r.v.slice(i, j).filter((v) => v === v).sort();
            if (vs.length) { const o = {}; for (const [name, p] of pcts) o[name] = quantile(vs, p); out[k] = o; }
        }
        i = j;
    }
    return out;
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

// The buckets of a query: fixed (a size and where it starts) or calendar (edges: days, weeks, months ... of a time zone).
//   bucket: "1h" | 90000 | "5m"          a fixed size, aligned to UTC (offset shifts the start)
//   bucket: "day" | "week" | "month" | "quarter" | "year"    calendar buckets of `tz` (a month is not 30 days, a day not always 24 hours); weekStart: "mon" (default) | "sun" | "sat" ...
//   bucket: "hour" | "minute" | "second"                      fixed, aligned to the clock of `tz`
//   bucket: "auto"                       the coarsest unit that gives at least minBuckets (default 4) buckets over the range: year, month, week, day, hour, minute, second
const MAX_CALENDAR_BUCKETS = 100000;       // calendar edges cost a time-zone lookup each
const UNIT_MS = { year: 365.25 * 86400000, quarter: 91.3125 * 86400000, month: 30.4375 * 86400000, week: 7 * 86400000, day: 86400000, hour: 3600000, minute: 60000, second: 1000 };
function bucketSpec(q, mode, from, to, tz) {
    const cal = require('./calendar');
    if (mode === 'range') { const size = to - from + 1; return { size, origin: from }; }      // the whole range as one bucket
    let b = q.bucket === undefined || q.bucket === null || q.bucket === '' ? '1h' : q.bucket;
    let unit = null;
    if (b === 'auto') {
        const minB = q.minBuckets === undefined ? 4 : q.minBuckets;
        if (!(typeof minB === 'number' && minB >= 1)) throw new Error('minBuckets must be a number of 1 or more');
        const len = to - from + 1;
        unit = 'second';
        for (const u of ['year', 'month', 'week', 'day', 'hour', 'minute']) if (len / UNIT_MS[u] >= minB) { unit = u; break; }
        b = unit;
    }
    if (typeof b === 'string' && UNIT_MS[b] !== undefined && !/^\d/.test(b)) {
        unit = b;
        if (['year', 'quarter', 'month', 'week', 'day'].includes(b)) {
            const approx = (to - from + 1) / UNIT_MS[b];
            if (approx > MAX_CALENDAR_BUCKETS) throw new Error('too many buckets: about ' + Math.round(approx).toLocaleString('en-US') + ' (' + b + ' buckets, at most ' + MAX_CALENDAR_BUCKETS.toLocaleString('en-US') + '): use a longer unit or a shorter range');
            return { edges: cal.edges(b, from, to, tz || 'UTC', q.weekStart, MAX_CALENDAR_BUCKETS + 2), unit };
        }
        const size = UNIT_MS[b], off = ((cal.tzOffset(from, tz || 'UTC') % size) + size) % size;
        return { size, origin: Math.floor((from - off) / size) * size + off, unit };
    }
    const size = parseDuration(b), off = q.offset ? parseDuration(q.offset) : 0;
    if (!(size > 0)) throw new Error('bucket must be longer than 0 (e.g. 1h, 15m, day, week, month, auto)');
    return { size, origin: Math.floor((from - off) / size) * size + off };
}

function run(engine, q, nowArg) {
    if (!q || typeof q !== 'object') throw new Error('a query is an object: { tags, from, to, mode, ... }');
    const now = nowArg === undefined ? Date.now() : nowArg;
    // `last` needs no from (the newest point at or before `to`, however old); every other mode defaults to the last hour
    const tz = q.tz === undefined || q.tz === null || q.tz === '' ? undefined : String(q.tz);
    if (tz) require('./calendar').formatter(tz);                    // an unknown zone is refused at once
    const from = parseTime(q.from === undefined ? (q.mode === 'last' ? 0 : '-1h') : q.from, now, tz);
    let to = parseTime(q.to, now, tz);
    if (q.endExclusive) to -= 1;                                     // `to` is the first instant NOT wanted (the first of next month)
    if (!(to >= from)) throw new Error('to is before from');
    const tags = matchTags(engine, q.tags, true);
    const mode = q.mode || 'bucket', rows = q.format === 'rows';
    const aggs = mode === 'bucket' || mode === 'range' ? parseAggs(q.agg) : [];
    const rawOnly = new Set();                      // tags whose answer came from raw points only (a counter policy of its own)
    for (const k of ['limit', 'maxPoints']) if (q[k] !== undefined && q[k] !== null && !(typeof q[k] === 'number' && q[k] >= 1 && Number.isFinite(q[k]))) throw new Error(k + ' must be a number of 1 or more (got ' + (typeof q[k] === 'object' ? JSON.stringify(q[k]) : q[k]) + ')');
    const bs = mode === 'bucket' || mode === 'range' ? bucketSpec(q, mode, from, to, tz) : null;
    const out = rows ? [] : {};
    // an answer past maxPoints (all tags together) is refused with its reason, never built until memory runs out
    const cap = q.maxPoints || 5000000;
    let total = 0;
    const tooBig = (n) => new Error('the answer would be about ' + n.toLocaleString('en-US') + ' points (more than maxPoints ' + cap.toLocaleString('en-US') + '): narrow the range or the tags, use mode bucket (an aggregate per time bucket), or raise maxPoints');
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
                    if (!q.page) throw new Error('"' + tag.name + '" has more than ' + limit.toLocaleString('en-US') + ' points in the range (the first ' + limit.toLocaleString('en-US') + ' end at ' + new Date(r.t[limit - 1]).toISOString() + '). Nothing is returned cut: use { page: true } and ask on from: the answer\'s next, or raise limit (the whole answer is held to maxPoints), or use mode bucket.');
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
        if (mode === 'bucket' || mode === 'range') {
            const expected = bs.edges ? bs.edges.length - 1 : Math.floor((to - bs.origin) / bs.size) - Math.floor((from - bs.origin) / bs.size) + 1;
            if (total + expected > cap) throw tooBig(total + expected);
            const spec = seriesSpec(q, tag, aggs);
            const numeric = tag.type !== 'string', spread = aggs.some((k) => SPREAD_AGGS.includes(k));
            const b = engine.buckets(tag, from, to, bs.size, bs.origin, spec && spec.ext, bs.edges, spread && numeric);
            if (spec && spec.ext.policy.custom) rawOnly.add(tag.name);
            const pcts = aggs.filter((k) => pctOf(k) !== null).map((k) => [k, pctOf(k)]);
            const pv = pcts.length && numeric ? percentiles(engine, tag, from, to, b, pcts, cap) : null;
            if (pcts.length) rawOnly.add(tag.name);
            const sa = b.sa;
            const col = { t: [] };
            aggs.forEach((k) => (col[k] = []));
            let prev = null;
            for (let i = 0; i < b.nb; i++) {
                const a = i * 10, n = b.acc[a + 9], ts = b.edgeAt(i);
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
                if (spread) {
                    const d = q.population ? n : n - 1, variance = b.m2 && d > 0 ? b.m2[i] / d : null;
                    val.variance = variance; val.stddev = variance === null ? null : Math.sqrt(variance);
                }
                for (const [k] of pcts) val[k] = pv && pv[i] ? pv[i][k] : null;
                col.t.push(ts);
                aggs.forEach((k) => col[k].push(val[k]));
                prev = val;
            }
            account(col.t.length);
            if (rows) col.t.forEach((ts, i) => { const r = { tag: tag.name, ts }; aggs.forEach((k) => (r[k] = col[k][i])); out.push(r); });
            else out[tag.name] = Object.assign({ type: tag.type }, bs.unit ? { bucket: bs.unit, tz: tz || 'UTC' } : {}, col);
            continue;
        }
        throw new Error('unknown mode: ' + mode + ' (bucket, range, raw, last)');
    }
    // a range that starts before what retention keeps is answered from what is kept, and says so: `clippedFrom` is the oldest time
    // that could be answered (raw: the tag's raw keep; the other modes: its keep). Never a silently shorter answer.
    if (!rows) for (const tag of tags) {
        const cut = engine._cut(tag, mode === 'raw' || rawOnly.has(tag.name)), o = out[tag.name];
        if (o && Number.isFinite(cut) && from < cut) o.clippedFrom = cut;
    }
    return out;
}

const MAX_BATCH = 1000;
/**
 * Many queries in one call: [{ ok: true, result } | { ok: false, error, code }], in the order given. A query that fails does not stop the
 * others. They share one `now` and see the same data (the worker writes nothing while it runs the batch). The points of all the answers
 * together are held to `maxPoints` (5 000 000): the queries past it get an error. A batch saves the calls, not the work: each query costs what
 * it costs alone (about 0.05 ms for a few days of hourly buckets); several aggregates of one tag are cheaper as one query with a longer `agg`.
 */
function runBatch(engine, queries, nowArg, maxPoints) {
    if (!Array.isArray(queries)) throw new Error('a batch is an array of queries');
    if (queries.length > MAX_BATCH) throw new Error('a batch is at most ' + MAX_BATCH + ' queries (got ' + queries.length + ')');
    const now = nowArg === undefined ? Date.now() : nowArg, cap = maxPoints || 5000000;
    let held = 0;
    const points = (r) => (Array.isArray(r) ? r.length : Object.values(r).reduce((n, x) => n + (x && x.t ? x.t.length : 0), 0));
    return queries.map((q) => {
        if (held > cap) return { ok: false, error: 'the answers of this batch are past ' + cap.toLocaleString('en-US') + ' points: ask the rest in another batch' };
        try { const result = run(engine, q, now); held += points(result); return { ok: true, result }; } catch (e) { return { ok: false, error: e.message, code: e.code }; }
    });
}

module.exports = { runBatch, run, parseTime, parseDuration, matchTags, globRe, quantile };
