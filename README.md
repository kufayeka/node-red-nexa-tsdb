# Nexa TSDB (`@kufayeka/node-red-tsdb-engine`)

Nexa TSDB is a time-series database that runs inside Node-RED. You send it measurements (a name, a time and a value), it stores them on the local disk, and you ask it questions later: *what was the temperature last week, how much energy did this line use yesterday hour by hour, how many hours was this machine running this month, how many times did it go into Fault.*

It is a **historian for the edge**: one process, no database server to install, no separate service to keep alive. It is the storage core of the Kufayeka Nexa Asset Framework.

This page is the manual. If you are new, read [What it is](#what-it-is), [What it can store](#what-it-can-store) and [Quick start](#quick-start) first, then [Asking questions](#asking-questions-queries-and-aggregates) and [Recipes](#recipes).

## Contents

1. [What it is](#what-it-is)
2. [What it can store](#what-it-can-store)
3. [Quick start](#quick-start)
4. [Nodes](#nodes)
5. [Writing data](#writing-data)
6. [Asking questions: queries and aggregates](#asking-questions-queries-and-aggregates)
7. [Many queries in one call (batch)](#many-queries-in-one-call-batch)
8. [Recipes](#recipes)
9. [Storage and retention](#storage-and-retention)
10. [Reliability](#reliability)
11. [Administration](#administration)
12. [Capacity planning](#capacity-planning)
13. [Testing and benchmarks](#testing-and-benchmarks)
14. [Limits](#limits)

## What it is

A *time series* is a list of values over time: `182.4` at 10:00:00, `182.9` at 10:00:01, and so on. A *tag* is the name of one series, for example `Oven1.Temp` or `Line2.Motor.Running`. A *point* is one `(tag, time, value)`.

What Nexa TSDB does:

- **Stores points compactly.** A full 1 024-point chunk of a process value with two decimals takes about 1.2 bytes per point; real installations see 2 to 5 bytes per point including the index (see [Capacity planning](#capacity-planning)).
- **Answers range questions fast.** Every chunk, hour and day carries a small summary (first, last, min, max, sum, count, and the increase and the area under the curve). A chart over a year, or the hourly consumption of a month, is answered from the summaries in milliseconds instead of reading every point.
- **Does not block Node-RED.** The engine runs in its own thread. Writing, compressing, checkpoints and queries never hold the Node-RED event loop.
- **Survives crashes.** A write-ahead log, checksums on every chunk and recovery on start. A power cut loses at most the last second of data (configurable).
- **Stays pure.** It stores `tag, time, value`. Splitting a JSON object into tags, assets and events belong to the layers above (`node-red-asset-engine`, `node-red-event-engine`).

Requirements: Node.js 20 or later, Node-RED 4 or later.

## What it can store

Every point is a **tag name**, a **time** and **one value**.

### Values

| Type | What you send | Typical use | Notes |
|---|---|---|---|
| **number** | `182.4`, `-3`, `0`, `1e6` | temperature, pressure, speed, power in kW, a kWh counter, a production count, an alarm code | Any finite double. `NaN` is refused. |
| **boolean** | `true` / `false` | running, door open, alarm active, valve position | Stored as 1 / 0. The average of a boolean is the share of points that were true; `duration` gives the running time. |
| **string** | `"Running"`, `"Fault"`, `"Recipe-42"` | machine state, mode, recipe name, operator, batch id | Each tag keeps a dictionary of its distinct texts, so a state that repeats a million times costs a few bits. Keep the number of distinct texts small (a state, a mode); free text or a unique id per point grows the dictionary without bound. |

The type of a tag is fixed by its **first** point. Sending a different type later is refused and counted.

### Names and times

- A **tag name** is any text. Dots are only a convention (`Plant.Line1.Oven.Temp`). Queries can use `*` as a wildcard (`Line1.*.Speed`).
- A **time** is milliseconds since 1970 (what `Date.now()` returns). Sub-millisecond precision is rounded.
- **One value per tag per millisecond.** A second point with the *same* time replaces the value of the first (counted as `overwritten`).
- Points must arrive in time order *per tag*. A point older than the tag's last is refused (`late`). Different tags are independent.

### What is not stored

Objects, arrays, `null`, `undefined`, `NaN`, binary blobs. A nested JSON object has to be split into one tag per field before it gets here; the store node skips nested objects and counts them. High-rate waveforms (vibration at kHz) are not what a tag historian is for.

### Examples

| Data | Tag | Type | Values |
|---|---|---|---|
| Oven temperature, every second | `Oven1.Temp` | number | `182.4`, `182.9`, … |
| Energy meter reading: rises, resets now and then | `Meter1.kWh` | number | `1204.5`, `1204.6`, …, `0.0`, `0.1`, … |
| Power draw | `Meter1.kW` | number | `12.4`, `0`, `15.8`, … |
| Machine state | `Line2.State` | string | `"Running"`, `"Idle"`, `"Fault"` |
| Motor running | `Line2.Motor.Run` | boolean | `true`, `false` |
| Alarm code | `Line2.Alarm` | number | `0`, `0`, `17`, `0` |

## Quick start

In Node-RED, add a **tsdb-config** node (the database), a **tsdb-store** node to write and a **tsdb-query** node to read.

A store node accepts any of these messages:

```js
{ topic: "Oven1.Temp", payload: 182.4 }                                  // time = now
{ topic: "Oven1.Temp", payload: 182.4, timestamp: 1767225600000 }        // time given
{ payload: { "Oven1.Temp": 182.4, "Oven1.Door": false } }                // several tags at once
{ payload: [{ tag: "Oven1.Temp", ts: 1767225600000, value: 182.4 }] }    // a batch
```

From JavaScript, through the worker client:

```js
const { openHistorian } = require('@kufayeka/node-red-tsdb-engine/lib/client');

const db = openHistorian('/var/lib/tsdb/plant', { rawDays: 30 });
await db.ready;

db.write('Oven1.Temp', Date.now(), 182.4);                 // false if the point is not accepted

const r = await db.query({ tags: 'Oven1.Temp', from: '-8h', mode: 'bucket', bucket: '1m', agg: ['avg', 'max'] });
await db.close();                                          // flushes, checkpoints, ends the worker
```

`write()` only appends to a batch of typed arrays. Every 50 ms the batch goes to the worker with its buffers transferred, not copied.

## Nodes

| Node | Purpose |
|---|---|
| **tsdb-config** | One database: a folder (default `<userDir>/tsdb/<name>`), raw retention, summary retention, WAL flush and checkpoint intervals, [storage rules](#storage-rules-disk-or-ram). Opened on deploy, checkpointed and closed on redeploy. |
| **tsdb-store** | Writes points (the message shapes above). Options: tag prefix; *changes only* (a value equal to the previous one is not stored). Otherwise every value is stored as it arrives. If the historian is overloaded or down the message ends with an error; it is never dropped silently. |
| **tsdb-query** | The node's settings are a query; `msg.query` overrides any field (use it for options the form does not show, such as `value`, `per`, `reset`). The result is `msg.payload`. |
| **tsdb-admin** | `msg.payload = { op, ... }`: list tags, stats, diagnose, delete, compact, verify. See [Administration](#administration). |

One folder can be opened by one engine only. Two config nodes with the same name, or two Node-RED instances, pointing at the same folder are refused (`ETSDB_LOCKED`).

## Writing data

### What is accepted and what is refused

A refused point is counted per tag, the last refusal is kept with its reason, and the store node's status shows it.

| Case | Result |
|---|---|
| Same time as the tag's last point | **Replaces** the value (`overwritten`). The last point stays in memory while it is recent, so it can still be replaced after a restart. |
| Older than the tag's last point | Refused (`late`). There is no backfill. |
| Different type from the tag's first value | Refused (`badType`). |
| Time before `minTs` (default 1 ms), negative, or not a number | Refused. Time 0 usually means a device with no clock. |
| Time more than `maxFutureMs` (default 1 day) ahead of the clock | Refused. One wrong clock must not make every later point of the tag "late". |
| `NaN` | Refused (`badType`). |
| Past `maxInFlight` points (default 2 million) sent but not yet stored | Refused as `overload`, so a backlog cannot grow until the process runs out of memory. The writer should wait and try again. |
| Worker down | Refused at once (`historian down: ...`); requests already waiting are rejected. Nothing hangs. |

A point before 1970-01-01T01:00 is valid and stored like any other.

### Settings

| Setting | Default | Meaning |
|---|---|---|
| `rawDays` | 30 | Raw points are kept this long. The summaries outlive them. |
| `indexDays` | 365 | Per-chunk summaries are kept this long. |
| `walFlushMs` | 1000 | The WAL is written and fsynced this often. A power cut loses at most this much. |
| `checkpointMs` | 60000 | How often the timer's checkpoint runs. |
| `chunkMinPoints` | 256 | The timer's checkpoint cuts a chunk only when the open one has this many points, or is older than `maxChunkAgeMs`, or its segment is over. |
| `maxChunkAgeMs` | 3600000 | The longest the first point of an open chunk waits before the chunk is cut. |
| `walSync` | true | fsync the WAL on every flush. |
| `minTs` / `maxFutureMs` | 1 / 86 400 000 | The time window a point must fall in. |
| `maxInFlight` | 2 000 000 | Backpressure limit (client side). |
| `batchMs` | 50 | How often the client sends its batch to the worker. |
| `rules` | none | [Storage rules](#storage-rules-disk-or-ram). |

## Asking questions: queries and aggregates

A query is one object (the **tsdb-query** node's form builds the same object; `msg.query` overrides it):

```js
{ tags: ["Oven1.Temp", "Line1.*.Speed"],   // * matches any text
  from: "-8h", to: "now",                  // relative ("now-30m"), ISO text, or epoch ms
  mode: "bucket",                          // bucket (default) | range | raw | last
  bucket: "1h", offset: "6h",              // bucket: a size, a calendar unit (day, week, month ...) or "auto"
  tz: "Asia/Jakarta",                      // the time zone of calendar buckets and of dates without a zone
  agg: ["avg", "max", "increase"],         // see the table below
  fill: "none",                            // bucket: none | null | previous (what an empty bucket gives)
  format: "series" }                       // series | rows
```

### Modes

| Mode | Returns | Use |
|---|---|---|
| `bucket` | One row per time bucket with the aggregates you ask for. | Hourly or daily numbers, reports, KPI. |
| `range` | **One row for the whole `[from, to]`**: the same aggregates over the range. | "How much between these two times?" |
| `raw` | The stored points. | Export. A cut answer is an error; use `page: true` for pages. |
| `last` | The newest point at or before `to`, however old. | Current value. |

### Buckets

The `bucket` of a `bucket` query is one of three kinds:

| `bucket` | Meaning |
|---|---|
| a size: `"15m"`, `"1h"`, `"1d"`, `900000` | Fixed buckets of that length, aligned to UTC (`"1d"` starts at 00:00 UTC). `offset` moves the start: `offset: "6h"` makes shifts start at 06:00 UTC. `"1w"` and `"1mo"` are 7 and 30 days, not calendar weeks and months. |
| a calendar unit: `"day"`, `"week"`, `"month"`, `"quarter"`, `"year"` | **Calendar buckets of the time zone `tz`**: a month is the real month (28 to 31 days), a day starts at local midnight (23 or 25 hours when daylight saving changes), a week starts on Monday (`weekStart: "sun"` or another day to change). Use these for reports: *the consumption of each month*. |
| `"hour"`, `"minute"`, `"second"` | Fixed buckets aligned to the clock of `tz` (matters for zones with a half-hour offset such as `Asia/Kolkata`). |
| `"auto"` | The coarsest unit that still gives at least `minBuckets` (default 4) buckets over the range: **a range of 6 months gives months, a month gives weeks, a week gives days, a day gives hours**, then minutes and seconds. For a dashboard that has a date range picker. The unit it chose is in the answer (`bucket: "month"`). `minBuckets: 1` accepts one bucket (a one-week range then gives a week). |

**Time zone.** `tz` is an IANA name (`Asia/Jakarta`, `America/New_York`, `UTC`; the default is UTC). It sets the calendar buckets, and it is how a date without a zone is read: with `tz: "Asia/Jakarta"`, `from: "2026-01-01"` is 2026-01-01 00:00 in Jakarta (2025-12-31T17:00Z). A text with an offset or `Z` (`"2026-01-01T00:00:00+07:00"`) is exact whatever `tz` says.

**The end of the range.** `to` is inclusive. For "the first six months", `to: "2026-07-01"` would also take a point at exactly 00:00 on July 1 and make a seventh bucket. Say `endExclusive: true`: `to` is then the first instant *not* wanted. The first and last bucket are cut to `[from, to]`: if the range does not start on a boundary, its first bucket holds only what is inside the range.

**Calendar buckets across edges.** The integral and the increase are split exactly at the edges (the line between two readings is cut where it crosses midnight or the end of the month), so the buckets of a range always add up to the whole range. The limit is 100 000 calendar buckets in one query.

### Aggregates

Give them in `agg`. An unknown name is an error that lists the known ones.

**Basic**

| Aggregate | Meaning | Types |
|---|---|---|
| `avg` `min` `max` `sum` | The usual, over the points in the bucket. | number, boolean |
| `count` | Number of points. | all |
| `first` `last` | The first and last value in the bucket. | all |
| `range` | `max − min`. | number |

**Change and counters** (a meter that only goes up, and sometimes resets)

| Aggregate | Meaning |
|---|---|
| `delta` | How much the value changed across the bucket: its last value minus the last value *before* the bucket. Because each bucket starts where the previous one ended, the deltas of consecutive buckets add up exactly to the delta of the whole range. Options: `anchor`, `reverse`. |
| `increase` | How much a counter **counted**. It adds up every step up. A drop in the value is taken as a **reset**: the counter started again from 0, so the new value is what it counted since. A plateau (the same value again) counts 0. This is the aggregate for energy consumption from a kWh meter. Options: `reset`, `tolerance`, `maxStep`, `ignoreZero`. |

**Area under the curve** (a value that fluctuates: kW, flow, speed)

| Aggregate | Meaning |
|---|---|
| `integral` | The area under the value over time. With kW and `per: "h"` this is **kWh**. Options: `method`, `per`, `maxGap`. |
| `twa` | The time-weighted average: the integral divided by the time it covers. Unlike `avg`, a value held for an hour counts more than one that lasted a second. |

**States** (strings, booleans, codes: "how many times was it Running")

| Aggregate | Meaning | Needs `value` |
|---|---|---|
| `occurrences` | How many points equal `value`. | yes |
| `entries` | How many times it *changed into* `value`. | yes |
| `duration` | How long it was in `value`, in milliseconds. For a boolean, `value: true` gives the running time. | yes |
| `changes` | How many times it changed to anything else. | no |
| `counts` | For every state, its number of points: `{ "Running": 120, "Idle": 12 }`. | no |
| `durations` | For every state, its time in ms. | no |

### Options for the aggregates

| Option | Applies to | Meaning |
|---|---|---|
| `anchor` | `delta` | `"start"` (default): last value minus the value before the bucket. `"inner"`: last minus first inside the bucket only. |
| `reverse` | `delta` | `true` returns the opposite sign (first − last): for a value that counts down, such as a tank level or a stock. |
| `reset` | `increase` | `"restart"` (default): a drop is a reset and the new value is counted. `"ignore"`: the step of a reset is not counted (the consumption is unknown). |
| `tolerance` | `increase` | A drop smaller than this is noise, not a reset (counts 0). Default 0. |
| `maxStep` | `increase` | A single step bigger than this is a glitch and is not counted. Default none. |
| `ignoreZero` | `increase`, `delta` | A reading of exactly 0 is a missing reading and is skipped. For a meter that now and then reports 0 and then comes back. |
| `method` | `integral`, `twa` | `"linear"` (default): the points are joined by straight lines. `"step"`: each value is held until the next point. |
| `per` | `integral` | The time unit of the result: `ms`, `s`, `m`, `h` (default), `d`. kW with `per: "h"` is kWh. |
| `maxGap` | `integral`, `duration` | An interval between two points longer than this (for example `"10m"`) is a gap in the data and is not counted. Default: no limit, so a gap is bridged. |
| `value` | `occurrences`, `entries`, `duration` | The state to look for. |

### How the aggregates are computed

- **A step between two readings belongs to the bucket of the later reading.** If the meter was read at 09:59:30 and 10:00:30, the consumption between them is in the 10:00 bucket for `increase` and `delta`. This keeps the sum of the buckets equal to the answer for the whole range.
- **The integral is split exactly at bucket edges.** The line between two points is cut where it crosses an hour, and each part goes to its own hour. A bucket that lies entirely inside a gap gets its share too (unless `maxGap` is set). The time after the last point of the range, up to `to`, is not counted, because there is nothing to join it to.
- **Speed.** With the default options these aggregates are answered from the hour, day and chunk summaries, like `avg`. Setting `tolerance`, `maxStep`, `ignoreZero`, `reset: "ignore"` or `maxGap` forces reading the raw points: exact, but limited to the raw retention (`rawDays`), and the answer says `clippedFrom` when the range goes further back. State aggregates read the points under any chunk that holds more than one state; a chunk that holds a single state is counted from its summary.
- **The first bucket starts from the point before the range**, so the consumption of the first hour includes the step from the last reading before it.
- A string tag has no `avg`, `delta`, `increase`, `integral` or `twa` (they are `null`); the state aggregates work on every type.

### Output

`series` is `{ "<tag>": { type, t: [...], <agg>: [...], ... } }`; `rows` is `[{ tag, ts, <agg>... }]`. `raw` and `last` return `t` and `v`. A bucket with no points is skipped, or filled with `fill`.

### Rules the query follows

- **An answer is never silently cut.** A `raw` query with more points than `limit` (default 1 000 000) is an error that says so. With `page: true` it returns `{ t, v, more, next }`, and the next page is `from: next`. An answer larger than `maxPoints` (default 5 000 000) is refused with its reason.
- **A range older than retention says so.** The answer starts at the oldest kept time and the tag's series carries `clippedFrom`.
- **Bad parameters are refused with a reason**, never an internal error.

## Many queries in one call (batch)

A dashboard with twenty panels can send all its queries at once. From JavaScript:

```js
const r = await db.queryBatch([
  { tags: "Meter1.kWh", from: "-1d", mode: "bucket", bucket: "1h", agg: ["increase"] },
  { tags: "Oven1.Temp", from: "-1h", mode: "bucket", bucket: "1m", agg: ["avg", "max"] },
  { tags: "Line1.State", from: "-1d", mode: "range", agg: ["counts"] }
]);
// [ { ok: true, result: { ... } }, { ok: true, result: { ... } }, { ok: true, result: { ... } } ]
```

In the **tsdb-query** node, put an array in `msg.query`: each item is a query (over the node's own settings, so a shared `tz` can be set once), and `msg.payload` is the array of answers in the same order.

- The answers come in the order of the queries: `{ ok: true, result }`, or `{ ok: false, error }` for one that failed. A failing query does not stop the others.
- All of them use the same `now` (so `-1h` means the same hour in every query) and see the same data, since the worker writes nothing while it answers the batch.
- At most 1 000 queries in a batch. The points of all the answers together are held to 5 000 000; the queries past that get an error and can be asked in another batch.

**What a batch saves.** The calls, not the work: a query costs what it costs alone. Measured, 100 queries of hourly buckets over three days: 16 ms one by one with `await`, 9.4 ms started together with `Promise.all`, 8.3 ms as one batch (about 0.08 ms each, nearly all of it the query itself). The gains that matter are elsewhere: ask several aggregates of one tag in one query (`agg: ["avg", "max", "increase"]`, one pass over the data), and several tags in one query (`tags: ["A", "B"]` or a `*` pattern).

## Recipes

Every recipe shows the data, the query and the answer the engine gave. Times are UTC, on 2026-10-06; `t` is the start of the bucket in epoch milliseconds (`1791273600000` is 08:00).

### Energy per hour from a kWh meter that resets

The meter is read every 20 minutes. At 10:20 it was reset and counts again from 0.

| Time | 08:00 | 08:20 | 08:40 | 09:00 | 09:20 | 09:40 | 10:00 | 10:20 | 10:40 | 11:00 |
|---|---|---|---|---|---|---|---|---|---|---|
| `Meter1.kWh` | 100.0 | 100.5 | 101.0 | 101.6 | 102.1 | 102.7 | 103.2 | **0.4** | 1.0 | 1.6 |

```js
{ tags: "Meter1.kWh", from: 1791273600000, to: 1791287940000,     // 08:00 to 11:59
  mode: "bucket", bucket: "1h", agg: ["increase"] }
```

```js
{ "Meter1.kWh": {
    type: "number",
    t:        [1791273600000, 1791277200000, 1791280800000, 1791284400000],   // 08:00  09:00  10:00  11:00
    increase: [1.0,           1.7,           1.5,           0.6]
} }
```

How to read it: 08:00 consumed 1.0 (100.0 to 101.0). The 09:00 hour includes the step 101.0 to 101.6 that happened between 08:40 and 09:00. The 10:00 hour holds the reset: 103.2 to 0.4 is not a negative, the meter restarted, so what it counted since (0.4) is the step; the hour is 0.5 + 0.4 + 0.6 = 1.5. (The engine returns floating point values such as `1.7000000000000028`; round when you display.)

### Between two dates: one row

```js
{ tags: "Meter1.kWh", from: 1791273600000, to: 1791287940000, mode: "range", agg: ["increase", "delta"] }
```

```js
{ "Meter1.kWh": { type: "number", t: [1791273600000], increase: [4.8], delta: [-98.4] } }
```

`increase` is what was consumed: 4.8 kWh (the four hours above added). `delta` is last minus first, 1.6 − 100.0: negative because of the reset, which is why consumption is `increase` and `delta` is for a value that does not reset. `reverse: true` flips the sign of `delta`, for a level that counts down.

### The meter reads 0 for a moment

A communication loss makes the meter read 0 in the middle of a run, then it comes back:

| Time | 08:00 | 08:20 | 08:40 | 09:00 | 09:20 |
|---|---|---|---|---|---|
| `Meter2.kWh` | 100.0 | 100.5 | **0** | 101.0 | 101.5 |

```js
{ tags: "Meter2.kWh", from: 1791273600000, to: 1791280740000, mode: "range", agg: ["increase"] }
// { "Meter2.kWh": { type: "number", t: [1791273600000], increase: [102.0] } }        a false jump of 101.0
```

The 0 looks like a reset and the way back looks like 101 kWh used. Tell it that a 0 is a missing reading:

```js
{ tags: "Meter2.kWh", from: 1791273600000, to: 1791280740000, mode: "range", agg: ["increase"], ignoreZero: true }
// { "Meter2.kWh": { type: "number", t: [1791273600000], increase: [1.5] } }          0.5 + 0.5 + 0.5
```

`maxStep: 500` also drops any single step bigger than 500 kWh, and `tolerance: 0.05` ignores a jitter of a few hundredths downwards. A real reset to 0 is still handled: the zero is skipped, and the next reading is lower than the one before it, which is a restart. These options read the raw points.

### Energy from a power signal (kW to kWh)

| Time | 08:00 | 08:30 | 09:00 | 09:30 | 10:00 | 10:30 | 11:00 |
|---|---|---|---|---|---|---|---|
| `Meter1.kW` | 10 | 10 | 20 | 20 | 10 | 0 | 0 |

```js
{ tags: "Meter1.kW", from: 1791273600000, to: 1791284400000,      // 08:00 to 11:00
  mode: "bucket", bucket: "1h", agg: ["integral", "twa", "max"], per: "h" }
```

```js
{ "Meter1.kW": {
    type: "number",
    t:        [1791273600000, 1791277200000, 1791280800000, 1791284400000],   // 08:00  09:00  10:00  11:00
    integral: [12.5,          17.5,          2.5,           0],               // kWh
    twa:      [12.5,          17.5,          2.5,           0],               // kW, weighted by time
    max:      [10,            20,            10,            0]
} }
```

08:00 to 09:00: 10 kW for half an hour is 5 kWh, then the line from 10 to 20 kW is 15 kW on average for half an hour, 7.5 kWh: 12.5 kWh. `method: "step"` would hold each reading until the next one instead of joining them with a line (for a value that is logged when it changes). `maxGap: "10m"` leaves out an interval longer than that, so a logger that was offline is not filled in.

### How many times was the machine in a state

`Line1.State` is read every 10 minutes: Running, Running, Idle, Idle, Fault, Fault, Fault, Running, Running (08:00 to 09:20).

```js
{ tags: "Line1.State", from: 1791273600000, to: 1791277200000,    // 08:00 to 09:00
  mode: "range", agg: ["occurrences", "entries", "duration", "counts"], value: "Fault" }
```

```js
{ "Line1.State": {
    type: "string",
    t:           [1791273600000],
    occurrences: [3],                                  // readings that said Fault
    entries:     [1],                                  // times it went into Fault
    duration:    [1200000],                            // ms in Fault: 20 minutes
    counts:      [{ Running: 2, Idle: 2, Fault: 3 }]   // every state at once
} }
```

A fault that lasts 100 readings is 100 `occurrences` but one `entry`. Use `mode: "bucket"` with `bucket: "day"` for one row per day.

### Running hours of a motor

`Line2.Motor.Run` (boolean): true at 08:00, false at 08:20, true at 08:30, false at 09:00, false at 09:10.

```js
{ tags: "Line2.Motor.Run", from: 1791273600000, to: 1791277800000,   // 08:00 to 09:10
  mode: "range", agg: ["duration", "entries"], value: true }
```

```js
{ "Line2.Motor.Run": { type: "bool", t: [1791273600000], duration: [3000000], entries: [1] } }
```

3 000 000 ms: 20 + 30 minutes running. Divide by 3 600 000 for hours; with `bucket: "day"` it is the running hours per day.

### One query for a date range picker

```js
{ tags: "Meter3.kWh", from: "2026-01-01", to: "2026-07-01", endExclusive: true,
  mode: "bucket", bucket: "auto", tz: "Asia/Jakarta", agg: ["increase"] }
```

Here `Meter3.kWh` rises by 1 every hour. The answer for six months is six calendar months of Jakarta:

```js
{ type: "number", bucket: "month", tz: "Asia/Jakarta",
  t:        [1767200400000, 1769878800000, 1772298000000, 1774976400000, 1777568400000, 1780246800000],
            // 2025-12-31T17:00Z = 2026-01-01 00:00 in Jakarta, then Feb 1, Mar 1, Apr 1, May 1, Jun 1
  increase: [744, 672, 744, 720, 744, 720] }       // the hours of January ... June
```

The same query with other ranges:

| Range | `bucket` chosen | Answer |
|---|---|---|
| `2026-03-01` to `2026-04-01` (a month) | `"week"` | `increase: [24, 168, 168, 168, 168, 48]`: six Monday weeks, the first and last cut to the range |
| `2026-03-02` to `2026-03-09` (a week) | `"day"` | `increase: [24, 24, 24, 24, 24, 24, 24]` |
| `2026-03-02` to `2026-03-03` (a day) | `"hour"` | 24 values, each `1` |

The same query on a power signal, `tags: "Meter1.kW", agg: ["integral"], per: "h"`, gives kWh per bucket from kW. Both are answered from the summaries: about 10 ms for six months of a tag at 1 Hz.

### A line chart

```js
// 1 200 columns over 8 hours: one bucket per column (8 h / 1 200 = 24 s), with the four values a line needs
{ tags: "Oven1.Temp", from: "-8h", mode: "bucket", bucket: "24s", agg: ["first", "min", "max", "last"] }
```

Draw the four values of each bucket in time order and the line looks exactly like the line through all the points, whatever the range: the extremes of every column are kept. Use `(to - from) / width` as the bucket size.

## Storage and retention

### Layout

```
tags.log        one line per tag (append only)
dict.log        one line per distinct string of a string tag
wal/*.wal       write-ahead log: tag id u32, ts f64, value f64
seg/*.seg       the chunks of each 1-hour segment (Gorilla, with CRC32)
idx/<id>.r0     one summary per chunk, and where the chunk is
idx/<id>.r1     one summary per hour
idx/<id>.r2     one summary per day
LOCK            held by the engine that has the folder open
```

A summary record is 15 float64 (120 bytes): first, last, min, max (each with its time), sum, count, where the chunk is, the counter increase and the two integrals. The on-disk format may change between 0.x versions without a migration.

### Retention

| Data | Kept |
|---|---|
| Raw points (segments) | `rawDays`, or the tag's rule. Whole segment files are deleted. |
| Per-chunk summaries (`.r0`) | `indexDays` |
| Hour and day summaries (`.r1`, `.r2`) | For ever, unless the tag's rule has a `keep` |

Old periods therefore still answer charts and aggregates (including consumption and energy) from the summaries after their raw points are gone. Aggregates with a custom counter policy, and state aggregates over mixed chunks, need raw points.

Index files are trimmed lazily: a file is rewritten only when a quarter of it has expired (streamed in blocks, fsynced, renamed), and the worker's hourly pass has a 1 second budget and continues with the next tags on its next pass. Expired records that are waiting are never returned.

### Storage rules: Disk or RAM

In the database node, each rule is a row. The first rule whose pattern matches a tag name decides (`*` matches any text). A tag that no rule matches is stored on disk with the default retention.

| Store | `keep` | Notes |
|---|---|---|
| **Disk** | 1 hour or more; empty means for ever. `raw` sets how long raw points are kept (summaries outlive them). | A query never returns anything older than `keep`. A disk `keep` under 1 hour is raised to 1 hour, with a warning. |
| **RAM** | Any duration down to seconds (`10s`). `max` limits the number of points. | A ring buffer in memory: **nothing is written to disk** (no SSD or SD wear) and **it is lost on restart or redeploy** of the database node. About 16 bytes per point. The editor warns on every RAM rule. |

A tag's store (disk or RAM) is fixed when the tag is created; its `keep` follows the rules at every start.

## Reliability

**Write path.** A point goes to the WAL first. Every `walFlushMs` the WAL is written and fsynced. Every `checkpointMs` a checkpoint runs: chunks that are large enough, old enough or at the end of their segment are written to the segments and fsynced, and only then are the WAL files they covered deleted. A small young chunk stays open in memory, and the WAL files that hold its points are kept until the chunk is written (up to `maxChunkAgeMs`, so up to an hour of a slow tag's points sit in the WAL). Close, `{ op: "checkpoint" }` and the admin operations write every open chunk.

**Recovery on start.** The last two segments are checked against the index (a torn chunk or record is cut), hour and day summaries are rebuilt from there, and the WAL is replayed (a point already stored is skipped). A power cut or crash loses at most the last `walFlushMs`. Start-up replays the WAL files that were kept, so it reads at most about an hour of points. A point still in the client's 50 ms batch is lost if the whole process dies.

**Checksums.** Every chunk has a CRC32 over its header and body. Every read verifies it and compares the decoded points with the chunk's summary.

**Damage is reported, not hidden.** Corrupt data is an error that names its place (`ETSDB_CORRUPT`: tag, segment, offset). A flipped bit anywhere in a chunk was caught in all 60 random flips of the test. Index files must be valid and in time order before a query uses them.

**Verify and repair.**
- `{ op: "verify" }` reads every chunk and recomputes the hour and day summaries. It returns `{ ok, chunks, points, damagedChunks, indexProblems, summaryProblems, problems }`. A 15.5-million-point database takes about 4 seconds.
- `{ op: "verify", repair: true }` drops damaged chunks from the index and rebuilds the summaries. The database answers again without the damaged part, which is reported. `dryRun: true` only reports.

**Power-cut leftovers.** A zero-filled or torn tail of an index file or of the last segment is cut when the database opens. Bytes after the last chunk that are not zeros are left alone and counted (`unreadableBytes`).

**I/O errors** (disk full, failed fsync, partial write) are handled like a crash. The worker stops the engine without writing, opens it again (recovery from the WAL), replays the points that were written but not yet in the WAL, and carries on. Batches wait meanwhile, and the client refuses writes past `maxInFlight`. Queries get the reason. If the engine cannot open (still no space), it retries with a growing delay and reports each attempt.

**One engine per folder.** A `LOCK` file holds the process id and is touched on every WAL flush. A second open is refused. A lock whose process is gone, or that has not been touched for 30 seconds, is taken over.

## Administration

```js
{ op: "tags" }                                                            // list tags
{ op: "stats" }                                                           // totals and the last refusal
{ op: "diagnose", tags: "Line1.*", problems: true }                       // per tag, problems first
{ op: "dropTag", tags: "Test.*" }                                         // tags, their data and summaries; the name can be reused
{ op: "deleteRange", tags: ["Oven*"], from: "-2h", to: "-1h" }            // points in a range; hour and day summaries rebuilt
{ op: "deleteRange", tags: "Oven*", from: "-2h", to: "-1h", dryRun: true }   // what it would do; nothing changes
{ op: "dropAll", confirm: "DROP ALL" }
{ op: "compact" }                                                         // reclaim bytes of dropped tags and replaced chunks
{ op: "verify", repair: false }                                           // see Reliability
```

- `diagnose` returns per tag `{ tag, type, store, written, overwritten, late, badType, refused, lastRefused: { ts, lastTs, reason, value, at }, lastTs, lastWriteAgoMs, inMemory }`.
- A pattern that is `*`, or that matches more than 100 tags, runs only with `confirm: <the number of tags it matches>`.
- Deletes are crash safe: the changed files are written aside and fsynced, one line in `ops.log` commits them, then they are swapped in. A start after a power cut finishes a committed delete and drops an uncommitted one.

## Capacity planning

These figures are measured or computed from measured sizes. They are estimates for planning, not guarantees.

**Disk per point.** The cost is the chunk (about 1.2 bytes per 2-decimal value in a full chunk, plus 20 bytes of header) and its 120-byte summary in the index. Measured on noisy 2-decimal data with the default settings (before the summary grew from 96 to 120 bytes; add about 25% to the index part):

| Writing rate per tag | Bytes per point | Per tag per year (index included) |
|---|---|---|
| 1 per second | about 2.1 (chunks 1.8, index 0.4) | about 66 MB |
| 1 per 5 seconds | about 2.6 | about 16 MB |
| 1 per minute | about 5.2 (chunks 1.9, index 3.3) | about 2.7 MB |

A tag that writes slowly makes few, small chunks (about one an hour), so it costs more per point than a fast one but little per year. The per-chunk index is the part to watch: about 12 to 15 MB per tag per year at 1 Hz, kept for `indexDays` (365 by default). Hour summaries add about 1 MB per tag per year and day summaries about 45 KB, kept for ever unless a rule sets `keep`.

**Example: 1 000 tags at 1 Hz, default settings.** Raw points for 30 days about 5 GB; per-chunk index for 365 days about 15 GB; hour and day summaries +1 GB per year. About 21 GB in year 1 and about 30 GB in year 10.

**Settings that reduce it.** `indexDays: 60` cuts the per-chunk index about six times. A larger `chunkMinPoints` (for example 1024) makes chunks bigger still, at the price of more points waiting in the WAL. Set a `keep` in the storage rules for tags that do not need to be stored for ever. Use *changes only* in the store node for tags that hold the same value for long stretches.

**Hardware.** Use an SSD or a disk with power-loss protection. The engine fsyncs every second. An SD card is not recommended for long-running installs.

**How many tags.** On one writer the practical envelope is about 20 000 tags at 1 s, or 10 000 tags at 100 ms. A single plant rarely logs more than a few tens of thousands of tags; for several plants run one instance per plant.

## Testing and benchmarks

### Tests

```
npm test            # all suites below
npm run fuzz        # the fuzz suite, long, with a random seed
npm run fault       # a writer whose file system fails at random, 30 rounds, with kill -9
npm run fault:soft  # the same, with young chunks held open in the WAL
npm run stress      # a writer under random kill -9
npm run bench       # the engine alone
npm run soak:gen && npm run soak:verify   # 5 years of data, random queries against a model
```

| Suite | Covers |
|---|---|
| `gorilla.test.js` | Compression round trips |
| `engine.test.js` | The engine against brute force at every summary level, crash recovery, retention |
| `worker.test.js` | Results through the worker, a free event loop, a hard kill, I/O errors |
| `admin.test.js` | Delete, compact, storage rules, a crash in the middle of a delete |
| `integrity.test.js` | Bit flips, torn and zero tails, verify and repair, paging |
| `robust.test.js` | Time 0 and 1970, wrong clocks, NaN, the folder lock, `clippedFrom`, the store node under overload |
| `chunking.test.js` | Young chunks held in the WAL, cut by size, age and segment end; a crash with them open |
| `compact.test.js` | Lazy index trimming, a crash in the middle, the retention time budget |
| `batch.test.js` | Batches: order, one failing query among others, one `now`, the size limits, through the worker and the node |
| `calendar.test.js` | Day, week, month, quarter and year edges in Jakarta, Kolkata, New York (daylight saving) and London; increase, delta, integral and counts over those buckets against a brute force; the buckets of a range add up to the range; `auto`; dates read in a zone |
| `rollup.test.js` | `delta`, `increase`, `integral`, `twa`, state aggregates and `range` against a brute force over random meters with resets, plateaus, zeros and gaps, through raw, chunk, hour and day levels, checkpoints, reopens and crashes; every counter option; hostile parameters |
| `fuzz.test.js` | Hostile input to `write()` and `query()`; raw and bucket against a model through random checkpoints, reopens and crashes |
| `restart.test.js` | 300 open / write / stop cycles, 12 `SIGKILL`s of a real writer, 25 worker cycles; open time, file handles, WAL files and heap stay flat |
| `fault.test.js` | A file system that fails at random, with `kill -9`, checked after every round (30 rounds: 775 restarts after injected errors; 30 rounds with young chunks: 1 915) |
| `nodes.test.js` | The Node-RED nodes on a stand-in runtime |

The Gorilla codec was also fuzzed with 20 000 random chunks of every kind of value (NaN, infinities, extremes, decimals, large time steps): no mismatch.

### Benchmarks

Measured on a Linux virtual machine, Node 22, warm OS cache, data written through the engine with a checkpoint every simulated minute. Times are the best of three.

**Reading, 1 tag at 1 Hz for one year (31.5 million points)**

| Query | Time |
|---|---|
| A chart over the whole year: buckets of first / min / max / last, 1 200 columns / 4 000 columns | 49 ms / 126 ms |
| A chart over 30 days / 24 hours / 1 hour, 1 200 columns | 21 ms / 6 ms / 1 ms |
| Bucket 1 hour over the year (8 760 rows), avg / min / max | 22 ms |
| Bucket 1 day over the year | 11 ms |
| Bucket 15 min over 30 days / 1 min over 24 hours | 48 ms / 6 ms |
| Raw, last hour / last 24 hours (86 400 points) | 1 ms / 14 ms |
| Last value | under 1 ms |

**Reading, 1 000 tags for 3 days at 5 s (51.8 million points)**

| Query | Time |
|---|---|
| Buckets of first / min / max / last of one tag over 3 days, 1 200 columns | 21 ms |
| Raw, 24 hours of one tag | 7 ms |
| 1 000 tags, last hour, 600 buckets each | 555 ms |
| Last value of all 1 000 tags | 1 ms |

**Reading, 1 tag at 1 per minute for 5 years (2.6 million points)**

| Query | Time |
|---|---|
| Bucket 1 hour over 5 years (43 800 rows) | 22 ms |
| Bucket 15 min over 4 years (140 160 rows, decodes raw chunks) | 360 ms |
| Bucket 1 min over 7 days | 2 ms |

**Counters and energy aggregates** (1 tag at 1 Hz, 5 days, 432 000 points)

| Query | Time |
|---|---|
| `increase` and `delta` per day / per hour (from summaries, no chunk decoded) | 5 ms / 2 ms |
| `integral` per hour | 2 ms |
| `increase` per 10 minutes (decodes the 600 chunks that straddle a bucket edge) | 52 ms |

**Energy over calendar buckets** (a kWh counter that resets every 30 days, and the kW it was made from; 1 Hz, 6.5 months, 34 million points; `tz: "Asia/Jakarta"`, `bucket: "auto"`)

| Query | Time |
|---|---|
| `increase` of the counter over 6 months: 6 calendar months | 13 ms |
| `integral` (kWh) of the kW over the same 6 months | 6 ms |
| Over one month: weeks / one week: days / one day: hours | under 1 ms each |
| `increase` and `delta` between two dates (`mode: "range"`, 3 months) | 4 ms |

The per-month kWh from the counter (`increase`) and from the power signal (`integral`) agreed to the unit in this run.

**Writing**

| | |
|---|---|
| Engine alone, 2 000 tags × 3 000 points at 100 ms, WAL fsync | about 1.8 million points/s (real time needs 20 000) |
| Size on disk, dense data, full chunks | 1.24 bytes per point |
| Through the worker, 9 000 tags every 100 ms (author's Windows laptop) | 316 000 points/s; Node-RED's thread held at most 47 ms per 9 000-point burst |
| A 3-million-point raw query while 20 000 points/s are written | 3.4 s, no write refused |
| Index trimming, 3 years of daily passes on a 1 Hz tag | 8 rewrites instead of 1 095; 0.4 GB written instead of 55 GB |

**Disk per point with young chunks held in the WAL** (checkpoint every simulated minute)

| | Before | After |
|---|---|---|
| 1 per minute, 1 tag, 1 year | 134.7 B/point | 5.2 B/point |
| 1 per 5 s, 200 tags, 7 days | 13.5 B/point | 2.6 B/point |
| 1 per second, 50 tags, 3 days | 3.9 B/point | 2.1 B/point |

**Scale and stress** (author's earlier runs on a Windows laptop, Node 24; not re-measured)

| | |
|---|---|
| 10 000 tags at 1 s | 18 times real time, 200 MB |
| 10 000 tags at 100 ms | 2.7 times real time |
| Stress: 2 000 tags, 4.6 M points in 60 s, 10 hard kills, a delete and compactions under load | every value correct, no duplicates; lost only what was in the last 600 ms before each kill |

**5-year soak** (`test/soak/`): 100 tags at 1 per minute for 1 825 days (262 million points), 1.34 GB. 300 random queries (random tags and ranges; raw, bucket, last) compared with a model: 0 differences. p50 / p95 per mode: bucket 5 ms / 2.1 s, raw 12 ms / 1.3 s, last 26 / 85 ms (Windows laptop, earlier version).

## Limits

- **No backfill.** Points older than a tag's newest point are refused, so store-and-forward from a reconnecting device is not supported.
- **One worker per database.** Under heavy writes, queries queue behind them, and a very large query holds the worker (it is refused past `maxPoints`). Checkpoint and retention also run on this worker. A "one writer, several readers" design is planned.
- **No online backup or replication.** Copy the folder while the database is stopped, or from a snapshot, after `{ op: "checkpoint" }`.
- **Power loss on real hardware is not tested.** It is simulated with torn and zero-filled tails and `kill -9`. A disk that lies about fsync can still lose data.
- **No multi-day run has been done.** The longest runs are minutes; memory and file handle use stayed flat in them. A multi-day soak on the target hardware should come before relying on it as the only copy of the data.
- **Aggregates.** Standard deviation and percentiles are not available (they cannot be answered from summaries). Fixed buckets given as a size (`"1d"`) are UTC; use the calendar units with `tz` for local days, weeks and months.
- **The on-disk format of 0.x versions may change without a migration.** A database made by an older development build has to be recreated.
- Planned: an index journal for 100 000 tags, one writer plus several readers, time-budgeted queries, a fluent query builder.

## License

Apache-2.0
