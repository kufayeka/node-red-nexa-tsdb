# @kufayeka/node-red-tsdb-engine

The historian core of the Kufayeka Nexa Asset Framework: **ts, tag, value**, stored on the edge with no database server, and fast to read for any range.

- **Values:** numbers, booleans (0 / 1), strings (a dictionary per tag: a state, a recipe, a mode). Every value is a float64 on disk; no object per point in the write path.
- **Pure:** no assets, no JSON, no events. A nested object is split into tags by the asset layer above (`node-red-asset-engine`); event frames come from `node-red-event-engine`.
- **Compression:** Gorilla (delta-of-delta timestamps, XOR values) plus a decimal codec: when a chunk's values all have k decimals (PLC data), they are stored as integers ×10^k as deltas. ~1.2 bytes a point for a 2-decimal process value, ~2 bits for a constant or a state.
- **Summary pyramid (M4 / OM3):** per chunk (~1 024 points), per segment (1 h) and per day: first, last, min, max **with their times**, sum, count. A chart's M4 (the extremes of each pixel column) and the bucket aggregates are answered from the summaries, reading about as many records as the answer has rows, whatever the range.

## In a worker thread

The engine runs in its own thread (`lib/worker.js`); Node-RED talks to it through `lib/client.js`:

- `write(tag, ts, value)` only puts the point in a batch of typed arrays (no object per point); every 50 ms the batch is sent to the worker with its buffers **transferred**, not copied. A tag name crosses once, then only its number.
- `query()`, `tags()`, `checkpoint()`, `close()` are Promises. Compression, the WAL, checkpoints, recovery and queries never run on Node-RED's event loop.
- A late or wrong-type point is refused in the worker; its counts come back in `stats` every second (the store node's status).
- A redeploy closes the database: the batch, a checkpoint, then the worker ends.

```js
const { openHistorian } = require('@kufayeka/node-red-tsdb-engine/lib/client');
const db = openHistorian(dir, { rawDays: 30 });
await db.ready;
db.write('Oven1.Temp', Date.now(), 182.4);
const r = await db.query({ tags: 'Oven1.Temp', from: '-8h', width: 1200 });
```

## Nodes

| Node | Does |
|---|---|
| **tsdb-config** | A database: a folder (default `<userDir>/tsdb/<name>`), raw retention, summary retention, WAL sync, checkpoint. |
| **tsdb-store** | `msg.topic` + `msg.payload` (+ `msg.timestamp`), or `msg.payload` = `[{ tag, ts, value }]`, or `{ tag: value }`. Tag prefix, changes only, deadband. |
| **tsdb-query** | The node's settings are a query; `msg.query` overrides any field. Output in `msg.payload`. |
| **tsdb-admin** | `msg.payload` = `{ op: "dropTag" \| "deleteRange" \| "dropAll" \| "compact" \| "tags" \| "stats", ... }`. |

## Storage rules: Disk or RAM, per tag pattern

In the database node, a row per rule; the first whose pattern matches a tag decides (`*` = any text). A tag no rule matches is on Disk, kept for ever (raw points `Raw kept` days).

| Store | keep | |
|---|---|---|
| **Disk** | 1 hour or more (empty = for ever); `raw`: how long its raw points are (its summaries outlive them) | A query never returns what is past `keep`. A disk keep under 1 h is raised to 1 h (warned): use RAM for seconds. |
| **RAM** | any, down to seconds (`10s`); `max`: points at most | A ring in memory: **never on disk** (no SSD / SD wear), **lost on a restart or a redeploy of the database node**. The editor warns on every RAM rule, and again on a long one (about 16 bytes a point: 1 tag at 100 ms for 24 h is about 14 MB). |

A tag's store is set when it is created; its keep follows the rules of each start.

## Writing: the same time replaces, an older one is refused, a backlog is refused

- A point with **the same timestamp** as a tag's last point **replaces** its value (counted as `overwritten`). A tag's last point stays in memory (and in the WAL across a checkpoint) while it is recent, so it can still be replaced after a restart.
- A point **older** than the tag's last one, or of **another type**, is refused (`late`, `badType`).
- **Backpressure:** the points sent to the worker and not yet stored are counted; past `maxInFlight` (2 M, about 40 MB) a write is refused as an `overload` instead of a backlog growing until the process runs out of memory. A writer that gets `false` waits and writes again.
- If the worker is down, a write is refused at once (`historian down: ...`) and every request still waiting is rejected; nothing hangs.

## Diagnostics

`{ op: "diagnose", tags?: "Line1.*", problems?: true }` → per tag, problems first:
`{ tag, type, store, written, overwritten, late, badType, refused, lastRefused: { ts, lastTs, reason, value, at }, lastTs, lastWriteAgoMs, inMemory }`.
`{ op: "stats" }` has the totals and the database's last refusal; the store node's status shows it (`... refused - last: Oven1.Temp older than the last point (...)`).

## Deleting

```js
{ op: "dropTag",     tags: "Test.*" }                                   // tags, their data, their summaries; the name can be used again
{ op: "deleteRange", tags: ["Oven*"], from: "-2h", to: "-1h" }          // the points in a range; hour / day summaries rebuilt
{ op: "dropAll",     confirm: "DROP ALL" }
{ op: "compact" }                                                        // reclaims the bytes of dropped tags and replaced chunks
{ op: "deleteRange", tags: "Oven*", from: "-2h", to: "-1h", dryRun: true }   // what it would do; nothing changes
```

- A pattern that is `*` or matches more than 100 tags runs only with `confirm: <the number of tags it matches>`.
- **Crash safe:** the files a delete changes are written aside and fsynced, one line in `ops.log` commits them, then they are swapped in; a start after a power cut finishes a committed delete and drops an uncommitted one.
- deleteRange rewrites only the chunks the range touches (the old bytes stay until `compact` or retention). A range whose raw points are past retention loses those summaries whole (`summariesDropped`).

## The query

```js
{ tags: ["Oven1.Temp", "Line1.*.Speed"],   // * matches any text
  from: "-8h", to: "now",                  // or ISO, or ms; "now-30m"
  mode: "m4",                              // m4 | bucket | raw | last
  width: 1200,                             // m4: pixel columns
  bucket: "8h", offset: "6h",              // bucket: size and alignment (shifts from 06:00)
  agg: ["avg", "min", "max", "sum", "count", "first", "last"],
  fill: "none",                            // none | null | previous
  limit: 1000000,                          // raw
  format: "series" }                       // series { tag: { type, t: [], v: [] } } | rows [{ tag, ts, value }]
```

A string tag returns its text; its min / max / avg / sum are null. M4 keeps every column's true min and max (the tests check 1 200 / 1 200 columns exact).

## On disk

```
tags.log      a line per tag (append only)        dict.log   a line per string of a string tag
wal/*.wal     the write-ahead log (tag u32, ts f64, value f64)
seg/*.seg     the chunks of each 1 h segment (Gorilla)
idx/<id>.r0   a summary per chunk + where it is    .r1 per hour    .r2 per day
```

**Durability:** a point is in the WAL (fsync every `walFlushMs`, default 1 s) before it is anywhere else. A checkpoint (default every 60 s) writes the open chunks, fsyncs the segments, then drops the WAL it covered. On start, the index of the last two segments is checked against them (a torn chunk or record is cut), the hour / day summaries are rebuilt from there and the WAL is replayed (a point already stored is skipped). A power cut loses at most the last `walFlushMs`.

**Retention:** raw segments older than *Raw kept* are deleted; their summaries remain, so charts and aggregates of old periods still work. Per-chunk summaries are compacted after *Summaries* days.

## Benchmark (Windows laptop, Node 24, warm OS cache)

Through the worker, as Node-RED uses it (`node bench/worker-bench.js`):

| | |
|---|---|
| write 9 000 tags × 100 ms, a burst per event-loop turn | **316 000 points/s** (real time needs 90 000); Node-RED's thread held at most **47 ms** per 9 000-point burst |
| chart, 6 months, 1 200 px | **9 ms**, Node-RED's thread held 9 ms |
| avg / max per hour, 6 months | 10 ms |
| per 1 s, last 24 h (86 400 buckets) | 46 ms |
| a line: 450 tags, 2 min, 600 px | 265 ms, Node-RED's thread held 13 ms |
| a query that decodes 2 M points (test) | Node-RED's thread held **12 ms** (240 ms when run on it) |

The engine alone (`npm run bench`):

| | |
|---|---|
| write 9 000 tags × 100 ms (WAL fsync every second) | **321 000 points/s** (real time needs 90 000) |
| size | **1.26 bytes / point** |
| chart, 6 months (15.6 M points), 1 200 px / 4 000 px | **13 ms** / **13 ms** |
| avg / min / max per hour, 6 months | 12 ms |
| per 8 h shift from 06:00, 6 months | 5 ms |
| chart, last 7 days, 1 200 px | 111 ms |
| raw, last hour / last value | 3 ms / 1 ms |

## Scale and stress (`bench/scale-bench.js`, `npm run stress`; Windows laptop, Node 24)

| | |
|---|---|
| 10 000 tags at 1 s | **18× real time**, Node-RED's thread held ≤ 17 ms per burst, 200 MB |
| 10 000 tags at 100 ms | **2.7× real time**, 1.36 bytes a point, none lost |
| a year of 1 tag at 1 s + 100 tags at 1 min (84 M points) | written in 119 s |
| chart 1 year, 1 200 / 4 000 px | **21 / 23 ms** |
| per day / per hour / per 8 h shift, 1 year | 6.5 / 15 / 5.5 ms |
| chart 1 year of 100 tags, 600 px each | 498 ms |
| last value of 10 000 tags | 32 ms |
| stress: 2 000 tags, 4.6 M points in 60 s, **10 hard kills**, a delete and compactions under load | every value right, **no duplicate**, lost only what was in the last 600 ms before each kill |

**100 000 tags:** the memory holds (a tag's head grows with its points: ~164 MB for 100 000 tags), but writing at 1 s is **0.7× real time on Windows**: a tag's index is its own files, and a checkpoint touching 100 000 of them spends ~170 s opening files (~1.7 ms each on Windows; Linux opens are ~10-50× faster). The envelope of this version: **about 20 000 tags at 1 s or 10 000 at 100 ms** on one writer. Past that: an index journal (one append stream, merged into the per-tag files in bulk), planned.

**Ten years:** the hour and day summaries of a tag are 0.9 MB a year (10 000 tags: 9 GB a year; 100 000 tags: 88 GB), and a 10-year chart reads ~3 650 day summaries per tag (a few ms). Raw data at 100 ms without report-by-exception is ~0.5 TB a year per 1 000 tags: keep raw days to weeks and let the summaries carry the years, and store changes (deadband) where the process allows.

## Limits of this MVP (next steps)

- **Late data** (older than a tag's newest point) is refused and counted; backfill comes later.
- **One worker per database**: under a flood of writes, queries queue behind them (a stress run completes 20 queries a minute while it writes 4.6 M points). One writer + N readers is planned.
- **100 000 tags** need the index journal above.
- A point still in the 50 ms batch (not yet in the worker's WAL) is lost if the whole process dies; a worker that dies alone loses only what was not in its WAL.
- **No fluent JS builder yet** (`tsdb.query("Oven1.Temp").last("8h")…`): it will build the same query object.
- Planned: time-budgeted queries, event-aware retention, KPIs at ingest (state durations, counters), quality codes in NaN payloads, blobs, a binary transport to Nexa charts.

## Tests

```
npm test        # Gorilla round trips; the engine against brute force (every pyramid level), crash recovery, retention;
                # the worker (exact through it, the event loop kept free, a hard kill recovered);
                # storage rules and admin (aggregates after a delete equal a brute force, a crash mid delete); the nodes
npm run bench   # the engine alone: --tags 9000 --points 600 --months 6 --period 1000 --keep
node bench/worker-bench.js   # through the worker: --tags 9000 --seconds 60 --months 6
```
