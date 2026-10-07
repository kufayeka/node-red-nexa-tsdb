# @kufayeka/node-red-tsdb-engine

A time-series store (historian) for Node-RED. It keeps `(timestamp, tag, value)` on the local disk with no database server, and answers range queries, chart queries and aggregates quickly. It is the storage core of the Kufayeka Nexa Asset Framework.

- **Values:** numbers, booleans and strings (a string tag keeps a dictionary of its distinct texts: a state, a mode, a recipe).
- **Compression:** Gorilla (delta-of-delta timestamps, XOR values), plus a decimal codec for process data with a fixed number of decimals. About 1.2 bytes per point in a full chunk; see [Capacity](#capacity) for what to expect in practice.
- **Fast ranges:** every chunk, hour and day keeps a summary (first, last, min, max with their times, sum, count). Charts (M4) and bucket aggregates are answered from the summaries, so the cost follows the size of the answer, not the length of the range.
- **Runs off the main thread:** the engine lives in a worker thread, so compression, the write-ahead log, checkpoints and queries never block Node-RED's event loop.
- **Built to survive crashes:** write-ahead log, per-chunk checksums, recovery on start, and a verify / repair operation.
- **Scope:** only `ts, tag, value`. Splitting nested objects into tags and event frames belong to the layers above (`node-red-asset-engine`, `node-red-event-engine`).

Requires Node.js 20 or later and Node-RED 4 or later.

## Contents

1. [Quick start](#quick-start)
2. [Nodes](#nodes)
3. [Writing data](#writing-data)
4. [Querying](#querying)
5. [Storage and retention](#storage-and-retention)
6. [Reliability](#reliability)
7. [Administration](#administration)
8. [Capacity](#capacity)
9. [Performance](#performance)
10. [Limits](#limits)
11. [Tests](#tests)

## Quick start

In Node-RED, add a **tsdb-config** node (the database), then a **tsdb-store** node to write and a **tsdb-query** node to read.

From JavaScript, through the worker client:

```js
const { openHistorian } = require('@kufayeka/node-red-tsdb-engine/lib/client');

const db = openHistorian('/var/lib/tsdb/plant', { rawDays: 30 });
await db.ready;

db.write('Oven1.Temp', Date.now(), 182.4);                 // returns false if the point is not accepted

const r = await db.query({ tags: 'Oven1.Temp', from: '-8h', width: 1200 });
await db.close();                                          // flushes the batch, checkpoints, ends the worker
```

`write()` only appends to a batch of typed arrays. Every 50 ms the batch is handed to the worker with its buffers transferred, not copied. A tag name crosses the thread boundary once; after that only its number does.

## Nodes

| Node | Purpose |
|---|---|
| **tsdb-config** | One database: a folder (default `<userDir>/tsdb/<name>`), raw retention, summary retention, WAL flush interval, checkpoint interval and the storage rules. Opened on deploy, checkpointed and closed on redeploy or stop. |
| **tsdb-store** | Writes points. Accepts `msg.topic` + `msg.payload` (+ `msg.timestamp`), or `msg.payload = [{ tag, ts, value }]`, or `msg.payload = { tag: value }`. Options: tag prefix, and *changes only* (a value equal to the previous one is skipped). Everything else is stored as it arrives. If the historian is overloaded or down, the message ends with an error instead of being dropped silently. |
| **tsdb-query** | The node's settings form a query; `msg.query` overrides any field. The result is in `msg.payload`. |
| **tsdb-admin** | `msg.payload = { op, ... }` with `op` one of `tags`, `stats`, `diagnose`, `dropTag`, `deleteRange`, `dropAll`, `compact`, `verify`. See [Administration](#administration). |

One folder can be opened by one engine only. Two config nodes with the same name, or two Node-RED instances, pointing at the same folder are refused (`ETSDB_LOCKED`).

## Writing data

### What is accepted and what is refused

A refused point is counted per tag, the last refusal is kept with its reason, and the store node's status shows it.

| Case | Result |
|---|---|
| Same timestamp as the tag's last point | **Replaces** the value (`overwritten`). The last point stays in memory while it is recent, so it can still be replaced after a restart. |
| Older than the tag's last point | Refused (`late`). There is no backfill. |
| Different type from the tag's first value | Refused (`badType`). A tag's type is fixed by its first point. |
| Time before `minTs` (default 1 ms), negative, or not a number | Refused. Time 0 usually means a device with no clock. |
| Time more than `maxFutureMs` (default 1 day) ahead of the clock | Refused. One wrong clock must not make every later point of the tag "late". |
| `NaN` | Refused (`badType`). It would be counted in an average without adding to it. |
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
| `chunkMinPoints` | 256 | The timer's checkpoint cuts a chunk only when the open one has this many points, or is older than `maxChunkAgeMs`, or its segment is over. Smaller chunks cost 96 bytes of index each, so a tag that writes slowly would otherwise get a chunk per checkpoint. |
| `maxChunkAgeMs` | 3600000 | The longest the first point of an open chunk waits before the chunk is cut. |
| `walSync` | true | fsync the WAL on every flush. |
| `minTs` / `maxFutureMs` | 1 / 86 400 000 | The time window a point must fall in (see above). |
| `maxInFlight` | 2 000 000 | Backpressure limit (client side). |
| `batchMs` | 50 | How often the client sends its batch to the worker. |
| `rules` | none | [Storage rules](#storage-rules-disk-or-ram). |

## Querying

```js
{ tags: ["Oven1.Temp", "Line1.*.Speed"],   // * matches any text
  from: "-8h", to: "now",                  // relative ("now-30m"), ISO text, or epoch ms
  mode: "m4",                              // m4 | bucket | raw | last
  width: 1200,                             // m4: pixel columns
  bucket: "8h", offset: "6h",              // bucket: size, and alignment (shifts start to 06:00)
  agg: ["avg", "min", "max", "sum", "count", "first", "last"],
  fill: "none",                            // bucket: none | null | previous
  limit: 1000000,                          // raw: most points per tag
  maxPoints: 5000000,                      // most points in the whole answer
  page: false,                             // raw: return pages (see below)
  exact: true,                             // m4: see below
  format: "series" }                       // series | rows
```

| Mode | Returns |
|---|---|
| `m4` | For each pixel column, its first, min, max and last point. The right data for a line chart: the shape is exact whatever the range. |
| `bucket` | One row per time bucket with the chosen aggregates. Buckets are aligned to UTC; use `offset` for another start (for example a shift from 06:00). |
| `raw` | The stored points. |
| `last` | The newest point at or before `to`, however old (`from` only limits it if you give one). |

Output formats: `series` is `{ "<tag>": { type, t: [...], v: [...] } }`; `rows` is `[{ tag, ts, value }]`. A bucket result has `t` and one array per aggregate. A string tag returns its text, and its min / max / avg / sum are `null`.

Rules the query follows:

- **An answer is never silently cut.** A `raw` query with more points than `limit` is an error that says so. With `page: true` it returns `{ t, v, more, next }`, and the next page is `from: next`. An answer larger than `maxPoints` is refused with its reason.
- **A range older than retention says so.** If `from` is before what is kept, the answer starts at the oldest kept time and the tag's series carries `clippedFrom` (raw: the raw keep; other modes: the tag's `keep`).
- **Bad parameters are refused with a reason**, never an internal error: `limit`, `maxPoints` and `width` must be numbers of 1 or more.

**M4 and `exact`.** By default (`exact: true`) every column's own min and max are exact: a chunk that straddles a column edge is decoded and its points are placed one by one. `exact: false` is faster: such a chunk is placed by its four points, so a column's min or max can miss a point within one chunk of its edge. The returned points are always real points, and the overall min, max, first and last are always exact. Measured on a 5-year dataset, `exact` costs +0.14 s at 1 200 px and +0.33 s at 4 000 px.

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

### Retention

| Data | Kept |
|---|---|
| Raw points (segments) | `rawDays`, or the tag's rule. Whole segment files are deleted. |
| Per-chunk summaries (`.r0`) | `indexDays` |
| Hour and day summaries (`.r1`, `.r2`) | For ever, unless the tag's rule has a `keep` |

Old periods therefore still answer charts and aggregates from the summaries after their raw points are gone.

Index files are trimmed lazily: a file is rewritten only when a quarter of it has expired (streamed in blocks, fsynced, renamed), and the worker's hourly pass has a 1 second budget and continues with the next tags on its next pass. Expired records that are waiting are never returned; a query stops at the tag's `keep`.

### Storage rules: Disk or RAM

In the database node, each rule is a row. The first rule whose pattern matches a tag name decides (`*` matches any text). A tag that no rule matches is stored on disk with the default retention.

| Store | `keep` | Notes |
|---|---|---|
| **Disk** | 1 hour or more; empty means for ever. `raw` sets how long raw points are kept (summaries outlive them). | A query never returns anything older than `keep`. A disk `keep` under 1 hour is raised to 1 hour, with a warning. |
| **RAM** | Any duration down to seconds (`10s`). `max` limits the number of points. | A ring buffer in memory: **nothing is written to disk** (no SSD or SD wear) and **it is lost on restart or redeploy** of the database node. About 16 bytes per point (one tag at 100 ms for 24 h is about 14 MB). The editor warns on every RAM rule. |

A tag's store (disk or RAM) is fixed when the tag is created; its `keep` follows the rules at every start.

## Reliability

**Write path.** A point goes to the WAL first. Every `walFlushMs` the WAL is written and fsynced. Every `checkpointMs` a checkpoint runs: chunks that are large enough, old enough or at the end of their segment are written to the segments and fsynced, and only then are the WAL files they covered deleted. A small young chunk stays open in memory, and the WAL files that hold its points are kept until the chunk is written (up to `maxChunkAgeMs`, so up to an hour of a slow tag's points sit in the WAL). Close, `{ op: "checkpoint" }` and the admin operations write every open chunk.

**Recovery on start.** The last two segments are checked against the index (a torn chunk or record is cut), hour and day summaries are rebuilt from there, and the WAL is replayed (a point already stored is skipped). A power cut or crash loses at most the last `walFlushMs`. Start-up replays the WAL files that were kept, so it reads at most about an hour of points. A point still in the client's 50 ms batch is lost if the whole process dies.

**Checksums.** Every chunk has a CRC32 over its header and body (format TSC2). Every read verifies it and compares the decoded points with the chunk's summary. Chunks written before checksums (TSC1) are still readable and are checked against their summary only.

**Damage is reported, not hidden.** Corrupt data is an error that names its place (`ETSDB_CORRUPT`: tag, segment, offset). A flipped bit anywhere in a chunk was caught in all 60 random flips of the test. Index files must be valid and in time order before a query uses them.

**Verify and repair.**
- `{ op: "verify" }` reads every chunk and recomputes the hour and day summaries. It returns `{ ok, chunks, points, damagedChunks, indexProblems, summaryProblems, problems }`. A 15.5-million-point database takes about 4 seconds.
- `{ op: "verify", repair: true }` drops damaged chunks from the index and rebuilds the summaries. The database answers again without the damaged part, which is reported. The bytes are reclaimed at the next `compact`, which refuses to run over a damaged chunk.
- `dryRun: true` only reports.

**Power-cut leftovers.** A zero-filled or torn tail of an index file or of the last segment is cut when the database opens. Bytes after the last chunk that are not zeros are left alone and counted (`unreadableBytes`).

**I/O errors** (disk full, failed fsync, partial write) are handled like a crash. The worker stops the engine without writing, opens it again (recovery from the WAL), replays the points that were written but not yet in the WAL, and carries on. Batches wait meanwhile, and the client refuses writes past `maxInFlight`, so a long outage cannot grow memory. Queries get the reason. If the engine cannot open (still no space), it retries with a growing delay and reports each attempt.

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
- `deleteRange` rewrites only the chunks the range touches. A range whose raw points are already past retention loses those summaries whole (`summariesDropped`).

## Capacity

These figures are measured or computed from measured sizes. They are estimates for planning, not guarantees.

**Disk per point.** The cost is the chunk (about 1.2 bytes per 2-decimal value in a full chunk, plus 20 bytes of header) and its 96-byte summary in the index. Measured on noisy 2-decimal data with the default settings:

| Writing rate per tag | Bytes per point | Per tag per year (index included) |
|---|---|---|
| 1 per second | about 2.1 (chunks 1.8, index 0.4) | about 66 MB |
| 1 per 5 seconds | about 2.6 | about 16 MB |
| 1 per minute | about 5.2 (chunks 1.9, index 3.3) | about 2.7 MB |

A tag that writes slowly makes few, small chunks (about one an hour), so it costs more per point than a fast one but little per year. The per-chunk index is the part to watch: about 12 MB per tag per year at 1 Hz, kept for `indexDays` (365 by default). Hour summaries add about 0.84 MB per tag per year and day summaries about 35 KB, kept for ever unless a rule sets `keep`.

**Example: 1 000 tags at 1 Hz, default settings.**

| Item | Size |
|---|---|
| Raw points, 30 days | about 5 GB (steady) |
| Per-chunk index, 365 days | about 12 GB (steady) |
| Hour and day summaries | +0.9 GB per year |
| Total | about 18 GB in year 1, about 26 GB in year 10 |

**Settings that reduce it.** `indexDays: 60` cuts the per-chunk index about six times, and the index files are trimmed less often. A larger `chunkMinPoints` (for example 1024) makes chunks bigger still, at the price of more points waiting in the WAL. With `indexDays: 60`, 1 000 tags at 1 Hz need roughly 8 GB. Set a `keep` in the storage rules for tags that do not need to be stored for ever.

**Hardware.** Use an SSD or a disk with power-loss protection. The engine fsyncs every second. An SD card is not recommended for long-running installs.

## Performance

Measured on a Windows laptop, Node 24, warm OS cache. Linux and ARM are not measured. The numbers are from an earlier version of this code and have not been re-measured since the recent changes.

**Through the worker, as Node-RED uses it** (`node bench/worker-bench.js`):

| | |
|---|---|
| Write 9 000 tags every 100 ms | 316 000 points/s (real time needs 90 000); Node-RED's thread held at most 47 ms per 9 000-point burst |
| Chart, 6 months, 1 200 px | 9 ms |
| Avg / max per hour, 6 months | 10 ms |
| Per second, last 24 h (86 400 buckets) | 46 ms |
| 450 tags, 2 minutes, 600 px | 265 ms (Node-RED's thread held 13 ms) |

**Engine alone** (`npm run bench`): 321 000 points/s write; 1.26 bytes per point on dense data; chart over 6 months (15.6 M points) 13 ms; raw last hour 3 ms; last value 1 ms.

**Scale** (`bench/scale-bench.js`, `npm run stress`):

| | |
|---|---|
| 10 000 tags at 1 s | 18 times real time, 200 MB |
| 10 000 tags at 100 ms | 2.7 times real time |
| Chart over 1 year | about 21 ms |
| Last value of 10 000 tags | 32 ms |
| Stress: 2 000 tags, 4.6 M points in 60 s, 10 hard kills, a delete and compactions under load | every value correct, no duplicates; lost only what was in the last 600 ms before each kill |

On one writer the practical envelope is about **20 000 tags at 1 s, or 10 000 tags at 100 ms**. At 100 000 tags memory holds, but a checkpoint spends minutes opening index files (about 1.7 ms per open on Windows, much less on Linux).

**5-year soak** (`test/soak/`, `npm run soak:gen` then `npm run soak:verify`): 100 tags at 1 per minute for 1 825 days (262 million points), 1.34 GB. 300 random queries (random tags and ranges, including whole calendar months; raw, bucket, m4, last) compared with a model: 0 differences.

| Mode | p50 | p95 | max |
|---|---|---|---|
| m4 | 11 ms | 277 ms | 555 ms |
| bucket | 5 ms | 2.1 s | 10 s |
| raw | 12 ms | 1.3 s | 2.4 s |
| last | 26 ms | 85 ms | 110 ms |

Opening the 5-year database takes about 1 second. Slow cases are buckets finer than an hour over years: they decode raw chunks, and a slow tag has one small chunk per hourly file, so most of the time goes to file opens. Anything answered from the summaries (hour, shift, day, week or month aggregates, and charts) stays in milliseconds.

## Limits

- **No backfill.** Points older than a tag's newest point are refused, so store-and-forward from a reconnecting device is not supported.
- **One worker per database.** Under heavy writes, queries queue behind them, and a very large query holds the worker (it is refused past `maxPoints`). Index maintenance (checkpoint, retention) also runs on this worker.
- **No online backup or replication.** Copy the folder while the database is stopped, or from a snapshot, after `{ op: "checkpoint" }`.
- **Power loss on real hardware is not tested.** It is simulated with torn and zero-filled tails and `kill -9`. A disk that lies about fsync can still lose data.
- **No multi-day run has been done.** The longest runs are minutes; memory and file handle use stayed flat in them. A multi-day soak on the target hardware should come before relying on it as the only copy of the data.
- **Tags:** no practical limit on their number up to the envelope above; the string dictionary of a tag grows with its distinct values, so avoid free-text or unique-id strings.
- **Aggregates in UTC.** Calendar months, time zones and `increase` / `integral` (counter difference, kW to kWh) are not built in.
- Planned: an index journal for 100 000 tags, one writer plus several readers, time-budgeted queries, a fluent query builder.

## Tests

```
npm test            # all suites below (about 20 seconds)
npm run fuzz        # the fuzz suite, long, with a random seed
npm run bench       # the engine alone
npm run stress      # a writer under random kill -9
npm run fault       # a writer whose file system fails at random, 30 rounds
npm run soak:gen && npm run soak:verify   # 5 years of data, random queries against a model
```

| Suite | Covers |
|---|---|
| `gorilla.test.js` | Compression round trips |
| `engine.test.js` | The engine against brute force at every summary level, crash recovery, retention |
| `worker.test.js` | Results through the worker, a free event loop, a hard kill, I/O errors |
| `admin.test.js` | Delete, compact, storage rules, a crash in the middle of a delete |
| `integrity.test.js` | Bit flips, old-format data, torn and zero tails, verify and repair, paging |
| `robust.test.js` | Time 0 and 1970, wrong clocks, NaN, the folder lock, `clippedFrom`, the store node under overload |
| `compact.test.js` | Lazy index trimming, a crash in the middle, the retention time budget |
| `fuzz.test.js` | Hostile input to `write()` and `query()`; raw, bucket and m4 against a model through random checkpoints, reopens and crashes |
| `restart.test.js` | 300 open / write / stop cycles, 12 `SIGKILL`s of a real writer, 25 worker cycles; open time, file handles, WAL files and heap stay flat |
| `fault.test.js` | A file system that fails at random, with `kill -9`, checked after every round |
| `nodes.test.js` | The Node-RED nodes on a stand-in runtime |

## License

Apache-2.0
