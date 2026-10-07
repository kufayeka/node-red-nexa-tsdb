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
7. [Recipes](#recipes)
8. [Storage and retention](#storage-and-retention)
9. [Reliability](#reliability)
10. [Administration](#administration)
11. [Capacity planning](#capacity-planning)
12. [Testing and benchmarks](#testing-and-benchmarks)
13. [Limits](#limits)

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

const r = await db.query({ tags: 'Oven1.Temp', from: '-8h', width: 1200 });
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
  mode: "bucket",                          // m4 | bucket | range | raw | last
  bucket: "1h", offset: "6h",              // bucket: its size, and where it starts
  agg: ["avg", "max", "increase"],         // see the table below
  fill: "none",                            // bucket: none | null | previous (what an empty bucket gives)
  format: "series" }                       // series | rows
```

### Modes

| Mode | Returns | Use |
|---|---|---|
| `m4` | For each pixel column (`width`), its first, min, max and last point. | A line chart: the shape is exact whatever the range. |
| `bucket` | One row per time bucket with the aggregates you ask for. | Hourly or daily numbers, reports, KPI. |
| `range` | **One row for the whole `[from, to]`**: the same aggregates over the range. | "How much between these two times?" |
| `raw` | The stored points. | Export. A cut answer is an error; use `page: true` for pages. |
| `last` | The newest point at or before `to`, however old. | Current value. |

Buckets are aligned to UTC: `bucket: "1d"` starts at 00:00 UTC. `offset` moves the start: `offset: "6h"` makes the buckets start at 06:00 UTC (a shift), and for a day that starts at 00:00 local time in UTC+7 use `offset: "17h"`.

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

`series` is `{ "<tag>": { type, t: [...], <agg>: [...], ... } }`; `rows` is `[{ tag, ts, <agg>... }]`. `m4` and `raw` return `t` and `v`. A bucket with no points is skipped, or filled with `fill`.

### Rules the query follows

- **An answer is never silently cut.** A `raw` query with more points than `limit` (default 1 000 000) is an error that says so. With `page: true` it returns `{ t, v, more, next }`, and the next page is `from: next`. An answer larger than `maxPoints` (default 5 000 000) is refused with its reason.
- **A range older than retention says so.** The answer starts at the oldest kept time and the tag's series carries `clippedFrom`.
- **Bad parameters are refused with a reason**, never an internal error.

### M4 and `exact`

By default (`exact: true`) every column's own min and max are exact: a chunk that straddles a column edge is decoded and its points are placed one by one. `exact: false` is faster: such a chunk is placed by its four points, so a column's min or max can miss a point within one chunk of its edge. The returned points are always real points, and the overall min, max, first and last are always exact.

## Recipes

**Energy used per hour from a kWh meter that resets**

```js
{ tags: "Meter1.kWh", from: "2026-10-06T00:00:00Z", to: "2026-10-07T00:00:00Z",
  mode: "bucket", bucket: "1h", agg: ["increase"] }
```

The meter climbs, then resets to 0 and climbs again: `increase` counts the climbing and takes the reset as a restart, so the hours that contain a reset are right. A plateau (the meter did not move) is 0.

**Energy between two times**

```js
{ tags: "Meter1.kWh", from: "2026-10-01T00:00:00Z", to: "2026-11-01T00:00:00Z",
  mode: "range", agg: ["increase", "delta"] }
```

One row. `increase` is what was consumed. `delta` is the last value minus the value before; it is negative if there was a reset, which is why `increase` is the one for consumption and `delta` is for a value that does not reset. `reverse: true` flips the sign of `delta`, for a level that counts down.

**The meter reads 0 now and then**

A reading of `0` in the middle of a run (a communication loss) looks like a reset followed by a huge jump, and `increase` would count the jump as consumption. Tell it the zeros are missing readings:

```js
{ tags: "Meter1.kWh", from: "-1d", mode: "bucket", bucket: "1h", agg: ["increase"], ignoreZero: true }
```

Add `maxStep: 500` to also drop any single step bigger than 500 kWh, and `tolerance: 0.05` to ignore a jitter of a few hundredths downwards. A real reset to 0 is still handled: the zero is skipped, and the next reading after the reset is lower than the one before it, which is a restart. These options read the raw points.

**Energy from a power signal (kW to kWh)**

```js
{ tags: "Meter1.kW", from: "-30d", mode: "bucket", bucket: "1d", agg: ["integral", "twa", "max"], per: "h" }
```

`integral` with `per: "h"` is kWh per day. `twa` is the average power weighted by time. Use `method: "step"` when each reading means "this much until the next reading" (a value logged on change); the default joins the points with lines (a signal sampled regularly). Add `maxGap: "10m"` if the logger can go offline and you do not want the gap filled.

**How many times was the machine in a state**

```js
{ tags: "Line1.State", from: "-7d", mode: "range",
  agg: ["occurrences", "entries", "duration", "counts"], value: "Fault" }
```

`occurrences`: how many readings said `Fault`. `entries`: how many times the machine *went into* Fault (a fault that lasts 100 readings is one entry). `duration`: how long it was in Fault, in ms. `counts`: all states at once. Use `mode: "bucket"` with `bucket: "1d"` for one row per day.

**Running hours of a motor**

```js
{ tags: "Line2.Motor.Run", from: "-30d", mode: "bucket", bucket: "1d", agg: ["duration"], value: true }
```

Divide the milliseconds by 3 600 000 for hours.

**A chart**

```js
{ tags: "Oven1.Temp", from: "-8h", mode: "m4", width: 1200 }
```

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
| `rollup.test.js` | `delta`, `increase`, `integral`, `twa`, state aggregates and `range` against a brute force over random meters with resets, plateaus, zeros and gaps, through raw, chunk, hour and day levels, checkpoints, reopens and crashes; every counter option; hostile parameters |
| `fuzz.test.js` | Hostile input to `write()` and `query()`; raw, bucket and m4 against a model through random checkpoints, reopens and crashes |
| `restart.test.js` | 300 open / write / stop cycles, 12 `SIGKILL`s of a real writer, 25 worker cycles; open time, file handles, WAL files and heap stay flat |
| `fault.test.js` | A file system that fails at random, with `kill -9`, checked after every round (30 rounds: 775 restarts after injected errors; 30 rounds with young chunks: 1 915) |
| `nodes.test.js` | The Node-RED nodes on a stand-in runtime |

The Gorilla codec was also fuzzed with 20 000 random chunks of every kind of value (NaN, infinities, extremes, decimals, large time steps): no mismatch.

### Benchmarks

Measured on a Linux virtual machine, Node 22, warm OS cache, data written through the engine with a checkpoint every simulated minute. Times are the best of three.

**Reading, 1 tag at 1 Hz for one year (31.5 million points)**

| Query | Time |
|---|---|
| Chart over the whole year, 1 200 px (exact M4) | 96 ms |
| Chart over 30 days / 24 hours | 22 ms / 8 ms |
| Bucket 1 hour over the year (8 761 rows) | 61 ms |
| Bucket 1 day over the year | 55 ms |
| Bucket 15 min over 30 days / 1 min over 24 hours | 23 ms / 8 ms |
| Raw, last hour / last 24 hours (86 400 points) / last 7 days (605 000 points) | 1 ms / 16 ms / 111 ms |
| Last value | under 1 ms |

**Reading, 1 000 tags for 3 days at 5 s (51.8 million points)**

| Query | Time |
|---|---|
| Chart of one tag over 3 days | 21 ms |
| Raw, 24 hours of one tag | 7 ms |
| 1 000 tags, last hour, 600 px each | 555 ms |
| Last value of all 1 000 tags | 1 ms |

**Reading, 1 tag at 1 per minute for 5 years (2.6 million points)**

| Query | Time |
|---|---|
| Chart over 5 years | 32 ms |
| Bucket 1 hour over 5 years (43 800 rows) | 22 ms |
| Bucket 15 min over 4 years (140 160 rows, decodes raw chunks) | 360 ms |
| Bucket 1 min over 7 days | 2 ms |

**Counters and energy aggregates** (1 tag at 1 Hz, 5 days, 432 000 points)

| Query | Time |
|---|---|
| `increase` and `delta` per day / per hour (from summaries, no chunk decoded) | 5 ms / 2 ms |
| `integral` per hour | 2 ms |
| `increase` per 10 minutes (decodes the 600 chunks that straddle a bucket edge) | 52 ms |

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

**5-year soak** (`test/soak/`): 100 tags at 1 per minute for 1 825 days (262 million points), 1.34 GB. 300 random queries (random tags and ranges; raw, bucket, m4, last) compared with a model: 0 differences. p50 / p95 per mode: m4 11 / 277 ms, bucket 5 ms / 2.1 s, raw 12 ms / 1.3 s, last 26 / 85 ms (Windows laptop, earlier version).

## Limits

- **No backfill.** Points older than a tag's newest point are refused, so store-and-forward from a reconnecting device is not supported.
- **One worker per database.** Under heavy writes, queries queue behind them, and a very large query holds the worker (it is refused past `maxPoints`). Checkpoint and retention also run on this worker. A "one writer, several readers" design is planned.
- **No online backup or replication.** Copy the folder while the database is stopped, or from a snapshot, after `{ op: "checkpoint" }`.
- **Power loss on real hardware is not tested.** It is simulated with torn and zero-filled tails and `kill -9`. A disk that lies about fsync can still lose data.
- **No multi-day run has been done.** The longest runs are minutes; memory and file handle use stayed flat in them. A multi-day soak on the target hardware should come before relying on it as the only copy of the data.
- **Aggregates and time zones.** Buckets align to UTC; use `offset` for a local start. Calendar months and daylight saving are not built in. Standard deviation and percentiles are not available.
- **The on-disk format of 0.x versions may change without a migration.** A database made by an older development build has to be recreated.
- Planned: an index journal for 100 000 tags, one writer plus several readers, time-budgeted queries, a fluent query builder.

## License

Apache-2.0
