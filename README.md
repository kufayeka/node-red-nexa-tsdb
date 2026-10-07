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

## Refused at the door, and one engine a folder

- A time before `minTs` (1 ms: 0 is a device with no clock, and a zero-filled WAL tail reads as 0), not a number, or more than `maxFutureMs` (1 day) ahead of the clock is **refused** with its reason: one wrong clock must not make every later point of a tag "late". A point in 1970 (after 0) is stored like any other.
- **NaN is refused** (counted as `badType`): it would count in an average and add nothing to it.
- **A folder has one engine:** a `LOCK` file (pid + a heartbeat on every WAL flush) refuses a second open (`ETSDB_LOCKED`): two config nodes with the same name, or two Node-RED instances, on one folder. A lock whose process is gone, or that has not beat for 30 s, is taken over.
- A range that starts before retention (`rawDays`, or a rule's `keep` / `raw`) is answered from what is kept, and the answer says so: `clippedFrom` on the tag's series.
- Disk use is about **2 - 4 bytes a point** for 2-decimal process data written at 1 Hz with the default 60 s checkpoint (a chunk per tag per checkpoint is ~60 points, and its summary is 96 bytes): the 1.2 bytes above is a full 1 024-point chunk. A longer `checkpointMs` makes bigger chunks (and a longer WAL replay after a crash).

- **Index retention is lazy and spread out:** an index file is rewritten only once a quarter of it is expired (streamed, fsynced, renamed), not daily, and the worker's hourly pass has a 1 s budget and carries on tag after tag. A year of 1 Hz summaries is 50 MB a tag; rewriting it daily was 55 GB written a year a tag, now about 0.13 GB. The records that wait are never returned (a query stops at a tag's `keep`); they are only not yet reclaimed.

## Diagnostics

`{ op: "diagnose", tags?: "Line1.*", problems?: true }` → per tag, problems first:
`{ tag, type, store, written, overwritten, late, badType, refused, lastRefused: { ts, lastTs, reason, value, at }, lastTs, lastWriteAgoMs, inMemory }`.
`{ op: "stats" }` has the totals and the database's last refusal; the store node's status shows it (`... refused - last: Oven1.Temp older than the last point (...)`).

## Integrity: never a wrong value without saying so

- **A cut answer is never returned as whole.** A `raw` query with more points than `limit` (1 000 000) is an **error** that says so; `page: true` returns exact pages `{ t, v, more, next }` and the next page is `from: next` (a full-range export in pages, checked: every row, once, exact).
- **Every chunk has a checksum** (CRC32 over its header and body, format TSC2); every read checks it, and checks the decoded points against the chunk's summary record. Chunks written before the checksums (TSC1) still read, and are checked against their summary only.
- **Damaged data is an error that names its place** (`corrupt data: tag ..., segment ..., offset ...`, code `ETSDB_CORRUPT`); a flipped bit anywhere in a chunk was caught in all 60 random flips of the test. The index files (level 0, 1, 2) must be valid and in time order for a query to use them.
- **`{ op: "verify" }`** reads every chunk and recomputes the hour and day summaries: `{ ok, chunks, points, damagedChunks, indexProblems, summaryProblems, problems }` (a 15.5 M-point database in 4 s). **`{ op: "verify", repair: true }`** drops the damaged chunks from the index and rebuilds the summaries: the database answers again, without the damaged part, which is reported (the bytes go at the next `compact`; `compact` refuses to run over a damaged chunk). `dryRun: true` only reports.
- **What a power cut leaves is cleaned on open:** a zero-filled or torn tail of an index file or of the last segment is cut; bytes after the last chunk that are *not* zeros are left alone and counted (`unreadableBytes`), never cut away.
- **An I/O error (disk full, a failed fsync, a write that lands in part) is handled as a crash is:** the worker stops the engine without writing anything, opens it again (recovery from the WAL), plays into it the points that were written but not yet in the WAL, and carries on. Batches wait meanwhile (the client refuses writes past `maxInFlight`, so a long outage cannot grow memory); queries are answered with the reason; if it cannot open (still no space) it retries with a growing delay and reports each try. The store node's status shows `restarting after an I/O error` and `DAMAGED chunk(s): run verify`.

`test/fuzz.test.js` throws hostile input at `write()` and `query()` (never an internal error, never a hang) and checks raw, bucket and m4 answers against a brute-force model through random checkpoints, reopens and crashes (`npm run fuzz` runs it long with a random seed); `test/restart.test.js` runs 300 open / write / stop cycles, 12 `SIGKILL`s of a real writer process (its stale `LOCK` is taken over) and 25 worker cycles, and checks that open time, file descriptors, WAL files and heap stay flat.

Proven by `npm test` (`test/integrity.test.js`: bit flips, TSC1 data, torn and zero tails, verify / repair, paging; `test/fault.test.js`: a writer whose file system fails at random for 30 rounds, 765 restarts after injected errors, 30 `kill -9`, checked after every round: nothing wrong, nothing durable lost, the index agrees with the chunks) and by the soak test below.

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
  limit: 1000000,                          // raw: at most this many points a tag
  maxPoints: 5000000,                      // the whole answer: more is refused with the reason (never built until memory runs out)
  exact: true,                             // m4 (default true): see below
  format: "series" }                       // series { tag: { type, t: [], v: [] } } | rows [{ tag, ts, value }]
```

A string tag returns its text; its min / max / avg / sum are null. 
**M4 and `exact`:** per pixel column M4 returns its first, last, min and max. By default (`exact: true`) every column's own min and max are exact: a chunk that straddles a column edge is decoded and its points placed one by one (checked on 90 804 random columns of a 5-year dataset: all exact). `exact: false` is the faster form: such a chunk is placed by its four points, so a column's min / max can miss a point within one chunk (at most an hour) of its edge (97.7 % of columns exact in the soak test, the rest differ only that way; the returned points are always real points and the overall min / max / first / last always exact). Cost of exact over 5 years: +0.14 s at 1 200 px, +0.33 s at 4 000 px.

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

## Soak test: 5 years, 262 million points, random ranges (`test/soak/`, `npm run soak:gen` / `soak:verify`)

100 tags every 1 min for 1 825 days, written in 9.3 min, **1.34 GB** (5.2 bytes a point: a slow tag is one small chunk an hour, so it costs more per point than a dense one). 300 random queries (random tags, random ranges over the whole 5 years, a fifth of them a whole calendar month of a random year; raw, bucket with shift offsets and edges off the hour, m4, last), each compared with the model's recomputation: **0 differences**; the process memory stayed flat. Through the engine in one process:

| mode | p50 | p95 | max |
|---|---|---|---|
| m4 (chart) | 11 ms | 277 ms | 555 ms |
| bucket | 5 ms | 2.1 s | 10 s |
| raw | 12 ms | 1.3 s | 2.4 s |
| last | 26 ms | 85 ms | 110 ms |

Opening the 5-year database: about 1 s. **What is slow, and why:** a bucket finer than an hour (or on a half-hour offset) over years has to decode raw chunks, and a slow tag keeps one small chunk per hourly file: 73 % of the time is `open` / `read` / `close` (one tag, 1 year, 15-min buckets: 0.8 s; 4 years: 2.8 s; 3 tags, 4 years: ~10 s). Anything answered from the summaries (hour, shift, day, week, month aggregates; charts) stays in milliseconds. The structural fix (day files for slow data, decoupled from the hourly summaries) is not done.

Found by running it, and fixed: `EMFILE` (a query kept every hourly segment it touched open: now an LRU of 32), `last` with a past `to`, a 3-4 s open (a `stat` of all 43 800 segments: now only the last two), a 14 s raw query (an open + close per chunk), M4 columns 42 - 82 % exact (now exact by default). On a machine whose pagefile is full (here: `explorer.exe` held 14.6 GB of commit) allocations fail whatever the engine does; a result larger than `maxPoints` is refused with its reason.

## Production readiness (what is proven, what is not)

**Proven (with the tests above):**
- Exact rows and aggregates: random tags, random ranges over 5 years (262 M points) in raw, bucket, M4 (exact) and last, against a model that recomputes the answer: no difference; random ranges are what a chart, a report and an export do.
- A crash anywhere (`kill -9` of the whole process, repeated: 11 - 12 times in a soak run; I/O errors injected into every write, fsync and rename; torn and zero-filled tails) loses nothing that was flushed (WAL every `walFlushMs`, 1 s by default: a crash loses at most that), duplicates nothing, and leaves the index and the chunks in agreement (`verify` clean).
- Damage is detected, named, and can be repaired without taking the rest down.

**Not proven / not built (decide before relying on it as the only copy of the data):**
- **Late and out-of-order data is refused** (counted, with the last example in the diagnostics): no backfill, so store-and-forward from a device that reconnects, or a clock that steps back, loses those points. Live data stamped now is fine.
- **A real power cut** (a disk that lies about fsync, a cache that is lost) is simulated by torn and zero-filled tails and by kill -9, not tested on hardware. A UPS and a disk with power-loss protection are still the right answer for the data that must not be lost.
- **No online backup / replication.** Copy the folder while the database is stopped (or from a snapshot); `{ op: "checkpoint" }` first.
- **One worker per database:** under a flood of writes, queries queue behind them; a very large query holds the worker (it is refused past `maxPoints`).
- **Not run for days:** the longest runs are minutes; memory and file handle use stayed flat in them (and handles are bounded by design), but a multi-day soak is the next test.
- **Only Windows / x64 measured** (Node 24). A Linux ARM edge box is likely faster at file opens and slower at CPU; not measured.
- Reporting helpers are not built yet: time zone, calendar months, `increase` / `integral` aggregates (a counter's difference, kW -> kWh).

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
