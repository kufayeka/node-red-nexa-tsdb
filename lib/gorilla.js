'use strict';
// Gorilla compression (Pelkonen et al., VLDB 2015) of one chunk: timestamps as delta-of-delta, values as the XOR with the
// previous value. Regular samples (every 100 ms) cost ~1 bit per timestamp; a slowly moving value a few bits.
//
//   encode(ts, vals, n) -> Buffer          ts: integer ms (Float64Array), vals: Float64Array
//   decode(buf, n, outTs, outVals)         fills the two arrays (no allocation per point)
//
// Timestamps are integer milliseconds (the engine rounds them). A value is any float64, NaN and ±Infinity included.
//
// The values have two codecs, the smaller wins per chunk (the first byte says which):
//   0      XOR with the previous value (Gorilla): any float64;
//   1 + k  every value has k decimals (k = 0 .. 6), as PLC data mostly has: the integers v·10^k as deltas. A decimal like
//          80.15 has no short binary form, so its XOR is ~45 noisy bits; as an integer its delta is a few bits.

// a bit writer over one reused scratch buffer (grown when a chunk needs more)
let scratch = new Uint8Array(1 << 16);
let pos = 0, bit = 0;   // byte position, bits already used in it (0 .. 7)

function ensure(bytes) {
    if (pos + bytes + 16 < scratch.length) return;
    const next = new Uint8Array(scratch.length * 2);
    next.set(scratch.subarray(0, pos + 1));
    scratch = next;
}
// write the low n bits (n <= 32) of v, most significant first
function put(v, n) {
    while (n > 0) {
        const room = 8 - bit, take = n < room ? n : room;
        const part = (n > 31 ? v >>> (n - take) : (v >>> (n - take))) & ((1 << take) - 1);
        scratch[pos] |= part << (room - take);
        bit += take; n -= take;
        if (bit === 8) { bit = 0; pos++; scratch[pos] = 0; }
    }
}

// float64 <-> two uint32 (little endian: [lo, hi])
const f64 = new Float64Array(1), u32 = new Uint32Array(f64.buffer);

function clz64(hi, lo) { return hi ? Math.clz32(hi) : 32 + Math.clz32(lo); }
function ctz32(x) { return x ? 31 - Math.clz32(x & -x) : 32; }
function ctz64(hi, lo) { return lo ? ctz32(lo) : 32 + ctz32(hi); }

// write the m bits of (hi, lo) >> t (m + t <= 64)
function putBits64(hi, lo, t, m) {
    let h, l;
    if (t === 0) { h = hi; l = lo; }
    else if (t < 32) { l = ((lo >>> t) | (hi << (32 - t))) >>> 0; h = hi >>> t; }
    else { l = hi >>> (t - 32); h = 0; }
    if (m > 32) { put(h, m - 32); put(l, 32); } else put(l, m);
}

// the decimals k (0 .. 6) every value has exactly, or -1 (NaN, ±Infinity, -0, too large, or more decimals)
const POW = [1, 10, 100, 1000, 10000, 100000, 1000000];
function decimals(vals, n) {
    let k = 0;
    for (let i = 0; i < n; i++) {
        const v = vals[i];
        if (!Number.isFinite(v) || Object.is(v, -0)) return -1;
        while (k <= 6) { const s = v * POW[k]; if (Math.abs(s) < 4503599627370496 && Math.round(s) / POW[k] === v) break; k++; }
        if (k > 6) return -1;
    }
    return k;
}

// a bucketed signed integer (a timestamp's delta of delta, or a scaled value's delta)
function putInt(x) {
    if (x === 0) put(0, 1);
    else if (x >= -63 && x <= 64) { put(0b10, 2); put(x + 63, 7); }
    else if (x >= -255 && x <= 256) { put(0b110, 3); put(x + 255, 9); }
    else if (x >= -2047 && x <= 2048) { put(0b1110, 4); put(x + 2047, 12); }
    else if (x >= -2147483648 && x <= 2147483647) { put(0b11110, 5); put(x | 0, 32); }
    else { put(0b11111, 5); f64[0] = x; put(u32[1], 32); put(u32[0], 32); }
}

function encode(ts, vals, n) {
    const k = decimals(vals, n);
    const xor = encodeWith(ts, vals, n, -1);
    if (k < 0) return xor;
    const dec = encodeWith(ts, vals, n, k);
    return dec.length < xor.length ? dec : xor;
}

function encodeWith(ts, vals, n, k) {
    pos = 0; bit = 0; scratch[0] = 0;
    ensure(n * 20 + 32);
    put(k + 1, 8);
    if (k >= 0) {
        // the scaled integers as deltas
        f64[0] = ts[0]; put(u32[1], 32); put(u32[0], 32);
        let prevI = Math.round(vals[0] * POW[k]);
        f64[0] = prevI; put(u32[1], 32); put(u32[0], 32);
        let prevT = ts[0], prevD = 0;
        for (let i = 1; i < n; i++) {
            ensure(24);
            const d = ts[i] - prevT; putInt(d - prevD); prevT = ts[i]; prevD = d;
            const iv = Math.round(vals[i] * POW[k]); putInt(iv - prevI); prevI = iv;
        }
        return Buffer.from(scratch.subarray(0, pos + (bit ? 1 : 0)));
    }
    // the first point in full
    f64[0] = ts[0]; put(u32[1], 32); put(u32[0], 32);
    f64[0] = vals[0]; let ph = u32[1], pl = u32[0]; put(ph, 32); put(pl, 32);
    let prevT = ts[0], prevD = 0, lead = -1, trail = 0;
    for (let i = 1; i < n; i++) {
        ensure(24);
        // the timestamp: delta of delta, in buckets
        const d = ts[i] - prevT;
        putInt(d - prevD);
        prevT = ts[i]; prevD = d;
        // the value: XOR with the previous one
        f64[0] = vals[i];
        const h = u32[1], l = u32[0], xh = (h ^ ph) >>> 0, xl = (l ^ pl) >>> 0;
        ph = h; pl = l;
        if (xh === 0 && xl === 0) { put(0, 1); continue; }
        put(1, 1);
        let lz = clz64(xh, xl);
        const tz = ctz64(xh, xl);
        if (lz > 31) lz = 31;
        if (lead >= 0 && lz >= lead && tz >= trail) {
            put(0, 1);
            putBits64(xh, xl, trail, 64 - lead - trail);
        } else {
            const m = 64 - lz - tz;
            put(1, 1); put(lz, 5); put(m & 63, 6);   // 64 meaningful bits are written as 0
            putBits64(xh, xl, tz, m);
            lead = lz; trail = tz;
        }
    }
    const len = pos + (bit ? 1 : 0);
    return Buffer.from(scratch.subarray(0, len));
}

// ---- the reader -----------------------------------------------------------------------------------------
let rb = null, rpos = 0, rbit = 0;
function get(n) {
    let v = 0;
    while (n > 0) {
        const room = 8 - rbit, take = n < room ? n : room;
        const part = (rb[rpos] >>> (room - take)) & ((1 << take) - 1);
        v = n > 31 || take === 32 ? v * (1 << take) + part : ((v << take) | part) >>> 0;
        rbit += take; n -= take;
        if (rbit === 8) { rbit = 0; rpos++; }
    }
    return v >>> 0;
}
function getBit() { const v = (rb[rpos] >>> (7 - rbit)) & 1; if (++rbit === 8) { rbit = 0; rpos++; } return v; }

function getInt() {
    if (!getBit()) return 0;
    if (!getBit()) return get(7) - 63;
    if (!getBit()) return get(9) - 255;
    if (!getBit()) return get(12) - 2047;
    if (!getBit()) return get(32) | 0;
    u32[1] = get(32); u32[0] = get(32); return f64[0];
}

function decode(buf, n, outTs, outVals) {
    rb = buf; rpos = 0; rbit = 0;
    const k = get(8) - 1;
    if (k >= 0) {
        u32[1] = get(32); u32[0] = get(32); let t = f64[0];
        u32[1] = get(32); u32[0] = get(32); let iv = f64[0];
        const p = POW[k];
        outTs[0] = t; outVals[0] = iv / p;
        let d = 0;
        for (let i = 1; i < n; i++) { d += getInt(); t += d; outTs[i] = t; iv += getInt(); outVals[i] = iv / p; }
        rb = null;
        return;
    }
    u32[1] = get(32); u32[0] = get(32); let t = f64[0];
    u32[1] = get(32); u32[0] = get(32); let ph = u32[1], pl = u32[0];
    outTs[0] = t; outVals[0] = f64[0];
    let d = 0, lead = 0, trail = 0;
    for (let i = 1; i < n; i++) {
        d += getInt(); t += d;
        outTs[i] = t;
        if (getBit()) {
            if (getBit()) { lead = get(5); let m = get(6); if (m === 0) m = 64; trail = 64 - lead - m; }
            const m = 64 - lead - trail;
            // read m bits, shift left by trail into (xh, xl)
            let h = 0, l = 0;
            if (m > 32) { h = get(m - 32); l = get(32); } else l = get(m);
            let xh, xl;
            if (trail === 0) { xh = h; xl = l; }
            else if (trail < 32) { xh = ((h << trail) | (l >>> (32 - trail))) >>> 0; xl = (l << trail) >>> 0; }
            else { xh = (l << (trail - 32)) >>> 0; xl = 0; }
            ph = (ph ^ xh) >>> 0; pl = (pl ^ xl) >>> 0;
        }
        u32[1] = ph; u32[0] = pl;
        outVals[i] = f64[0];
    }
    rb = null;
}

module.exports = { encode, decode };
