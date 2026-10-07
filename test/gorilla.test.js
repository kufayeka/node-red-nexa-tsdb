'use strict';
// Gorilla round trips: every value and timestamp comes back bit for bit; the sizes it reaches on factory-like data.
const assert = require('assert');
const { encode, decode } = require('../lib/gorilla');

let passed = 0;
function ok(label, fn) { fn(); passed++; console.log('✔ ' + label); }

function roundTrip(ts, vals) {
    const n = ts.length, buf = encode(Float64Array.from(ts), Float64Array.from(vals), n);
    const t2 = new Float64Array(n), v2 = new Float64Array(n);
    decode(buf, n, t2, v2);
    for (let i = 0; i < n; i++) {
        assert.strictEqual(t2[i], ts[i], 'ts at ' + i);
        assert.ok(Object.is(v2[i], vals[i]) || (Number.isNaN(v2[i]) && Number.isNaN(vals[i])), 'value at ' + i + ': ' + v2[i] + ' vs ' + vals[i]);
    }
    return buf.length;
}

let seed = 42;
const rnd = () => { seed = (seed * 16807) % 2147483647; return seed / 2147483647; };
const T0 = 1791240000000;

ok('regular 100 ms, a random walk: exact, and small', () => {
    const ts = [], v = [];
    let x = 80;
    for (let i = 0; i < 1024; i++) { ts.push(T0 + i * 100); x += (rnd() - 0.5) * 0.2; v.push(Math.round(x * 100) / 100); }
    const bytes = roundTrip(ts, v);
    console.log('   1024 points: ' + bytes + ' bytes = ' + (bytes / 1024).toFixed(2) + ' bytes/point');
    assert.ok(bytes / 1024 < 8, 'well under 16 bytes a point');
});

ok('a constant (a state, a setpoint): about 2 bits a point', () => {
    const ts = [], v = [];
    for (let i = 0; i < 1024; i++) { ts.push(T0 + i * 100); v.push(3); }
    const bytes = roundTrip(ts, v);
    console.log('   1024 points: ' + bytes + ' bytes');
    assert.ok(bytes < 300);
});

ok('NaN, ±Infinity, -0, tiny and huge numbers, integers as counters', () => {
    const v = [1, NaN, Infinity, -Infinity, -0, 0, 5e-324, 1.7976931348623157e308, -1e-300, 123456789012, 123456789013, 0.1 + 0.2];
    roundTrip(v.map((_, i) => T0 + i * 100), v);
});

ok('irregular timestamps: jitter, gaps of seconds, an hour, a year, back to 1 ms', () => {
    const ts = [T0], v = [1];
    const steps = [100, 101, 99, 100, 5000, 100, 3600000, 1, 1, 365 * 864e5, 100, 2, 300000, 7];
    steps.forEach((s, i) => { ts.push(ts[ts.length - 1] + s); v.push(i * 1.5); });
    roundTrip(ts, v);
});

ok('one point and two points', () => {
    roundTrip([T0], [42]);
    roundTrip([T0, T0 + 1], [1, 2]);
});

ok('random values (the worst case) still round trip', () => {
    const ts = [], v = [];
    for (let i = 0; i < 2000; i++) { ts.push(T0 + i * 100 + Math.floor(rnd() * 5)); v.push((rnd() - 0.5) * 1e6); }
    const bytes = roundTrip(ts, v);
    console.log('   2000 random points: ' + (bytes / 2000).toFixed(2) + ' bytes/point');
});

ok('two chunks encoded back to back do not leak state', () => {
    const a = roundTrip([T0, T0 + 100, T0 + 200], [1.5, 2.5, 3.5]);
    const b = roundTrip([T0, T0 + 100, T0 + 200], [1.5, 2.5, 3.5]);
    assert.strictEqual(a, b);
});

console.log(`\n${passed} passed\nALL OK`);
