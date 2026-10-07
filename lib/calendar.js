'use strict';
// Calendar buckets in a time zone: the start of a local day, week, month, quarter or year, as epoch ms, and the list of edges that
// covers a range. A month is not 30 days and a day is not always 24 hours (daylight saving), so these buckets are not a fixed size:
// the engine takes their edges as a list (lib/engine.js buckets()).
//
// Local time is computed with Intl (the zone database of Node), never by hand: tzOffset(t, tz) is what the zone's clock differs from
// UTC at the instant t; a local wall-clock time is turned into an instant with two refinements, which is right for every zone, also
// across a daylight saving change (a local time that does not exist is taken a moment later, one that exists twice the first time).

const fmts = new Map();
function formatter(tz) {
    let f = fmts.get(tz);
    if (!f) {
        try { f = new Intl.DateTimeFormat('en-US', { timeZone: tz, hourCycle: 'h23', year: 'numeric', month: 'numeric', day: 'numeric', hour: 'numeric', minute: 'numeric', second: 'numeric' }); }
        catch (e) { throw new Error('unknown time zone "' + tz + '" (an IANA name, for example Asia/Jakarta, or UTC)'); }
        fmts.set(tz, f);
    }
    return f;
}

/** The zone's clock minus UTC at the instant t, in ms (Asia/Jakarta: 25 200 000). */
function tzOffset(t, tz) {
    if (tz === 'UTC') return 0;
    const parts = formatter(tz).formatToParts(new Date(Math.floor(t / 1000) * 1000)), v = {};
    for (const p of parts) if (p.type !== 'literal') v[p.type] = +p.value;
    return Date.UTC(v.year, v.month - 1, v.day, v.hour % 24, v.minute, v.second) - Math.floor(t / 1000) * 1000;
}

/** The local wall-clock time y-mo-d h:mi:s of a zone, as the instant it is (mo 0 - 11; a day past the month's end rolls over). */
function localToUtc(y, mo, d, h, mi, s, tz) {
    const asUtc = Date.UTC(y, mo, d, h || 0, mi || 0, s || 0);
    if (tz === 'UTC') return asUtc;
    let t = asUtc - tzOffset(asUtc, tz);
    const o2 = tzOffset(t, tz);
    if (asUtc - o2 !== t) t = asUtc - o2;
    return t;
}

/** The local calendar fields of an instant: { y, mo (0 - 11), d, h, mi, s, dow (0 Sunday) }. */
function localFields(t, tz) {
    const u = new Date(t + tzOffset(t, tz));
    return { y: u.getUTCFullYear(), mo: u.getUTCMonth(), d: u.getUTCDate(), h: u.getUTCHours(), mi: u.getUTCMinutes(), s: u.getUTCSeconds(), dow: u.getUTCDay() };
}

const UNITS = ['year', 'quarter', 'month', 'week', 'day'];
const WEEK_START = { mon: 1, sun: 0, sat: 6, tue: 2, wed: 3, thu: 4, fri: 5 };

// the local start (as y / mo / d) of the unit that holds the instant, and the same one a unit later
function unitStart(unit, f, weekStart) {
    if (unit === 'year') return [f.y, 0, 1];
    if (unit === 'quarter') return [f.y, Math.floor(f.mo / 3) * 3, 1];
    if (unit === 'month') return [f.y, f.mo, 1];
    if (unit === 'week') { const back = (f.dow - weekStart + 7) % 7; return [f.y, f.mo, f.d - back]; }
    return [f.y, f.mo, f.d];
}
function unitNext(unit, [y, mo, d]) {
    if (unit === 'year') return [y + 1, mo, d];
    if (unit === 'quarter') return [y, mo + 3, d];
    if (unit === 'month') return [y, mo + 1, d];
    if (unit === 'week') return [y, mo, d + 7];
    return [y, mo, d + 1];
}

/**
 * The edges (epoch ms, rising) of the calendar buckets that cover [from, to]: the first is the start of the unit that holds `from`, the
 * last is the first edge after `to`. Bucket k is [edges[k], edges[k + 1]). unit: day | week | month | quarter | year.
 */
function edges(unit, from, to, tz, weekStart, limit) {
    if (!UNITS.includes(unit)) throw new Error('not a calendar unit: ' + unit);
    const ws = WEEK_START[weekStart === undefined ? 'mon' : weekStart];
    if (ws === undefined) throw new Error('weekStart must be one of ' + Object.keys(WEEK_START).join(', '));
    const out = [];
    let cur = unitStart(unit, localFields(from, tz), ws);
    for (;;) {
        const t = localToUtc(cur[0], cur[1], cur[2], 0, 0, 0, tz);
        if (out.length && !(t > out[out.length - 1])) { cur = unitNext(unit, cur); continue; }   // a zone that skips a local midnight
        out.push(t);
        if (t > to) break;
        if (limit && out.length > limit) throw new Error('too many buckets: more than ' + limit);
        cur = unitNext(unit, cur);
    }
    return Float64Array.from(out);
}

/** The instant of an ISO text without a zone ("2026-01-31", "2026-01-31T06:00") read as the zone's clock; null if it has none to read. */
function parseLocal(s, tz) {
    const m = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?)?$/.exec(s);
    if (!m) return null;
    const ms = m[7] ? Math.round(+('0.' + m[7]) * 1000) : 0;
    return localToUtc(+m[1], +m[2] - 1, +m[3], +(m[4] || 0), +(m[5] || 0), +(m[6] || 0), tz) + ms;
}

module.exports = { tzOffset, localToUtc, localFields, edges, parseLocal, UNITS, WEEK_START, formatter };
