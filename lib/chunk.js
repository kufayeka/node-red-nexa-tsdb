'use strict';
// A chunk on disk: a header of 16 bytes (magic, tag id, point count, body length), the Gorilla body, and (TSC2) a trailer
// of 4 bytes: the CRC32 of the header and the body. Any flipped bit in a chunk fails the check. TSC1 chunks (written
// before the checksum) have no trailer: they are still read, and checked against their summary instead (lib/engine.js).
const MAGIC1 = 0x31435354, MAGIC2 = 0x32435354, HEAD = 16, MAXLEN = 1 << 24, MAXN = 1 << 20;

const TABLE = new Int32Array(256);
for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xEDB88320 ^ (c >>> 1) : c >>> 1; TABLE[n] = c; }

function crc32(buf, start, end) {
    let c = -1;
    for (let i = start; i < end; i++) c = TABLE[(c ^ buf[i]) & 255] ^ (c >>> 8);
    return (c ^ -1) >>> 0;
}

/** A whole chunk (header, body, CRC) as one buffer: one write. */
function build(tagId, n, body) {
    const b = Buffer.allocUnsafe(HEAD + body.length + 4);
    b.writeUInt32LE(MAGIC2, 0); b.writeUInt32LE(tagId, 4); b.writeUInt32LE(n, 8); b.writeUInt32LE(body.length, 12);
    body.copy(b, HEAD);
    b.writeUInt32LE(crc32(b, 0, HEAD + body.length), HEAD + body.length);
    return b;
}

/** The header at buf[0 .. 16): { ver, id, n, len, total (the bytes the chunk takes on disk) }, or null when it is not a chunk header. */
function parse(buf) {
    const m = buf.readUInt32LE(0);
    if (m !== MAGIC1 && m !== MAGIC2) return null;
    const n = buf.readUInt32LE(8), len = buf.readUInt32LE(12), ver = m === MAGIC2 ? 2 : 1;
    if (!(n >= 1 && n <= MAXN) || len > MAXLEN) return null;
    return { ver, id: buf.readUInt32LE(4), n, len, total: HEAD + len + (ver === 2 ? 4 : 0) };
}

/** The whole chunk is in buf (header .. trailer): true when its checksum holds (a TSC1 chunk has none: true). */
function intact(buf, h) {
    return h.ver === 1 || crc32(buf, 0, HEAD + h.len) === buf.readUInt32LE(HEAD + h.len);
}

module.exports = { MAGIC1, MAGIC2, HEAD, crc32, build, parse, intact };
