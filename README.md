# @kufayeka/node-red-tsdb-engine

The historian core of the Kufayeka Nexa Asset Framework: **ts, tag, value**, stored on the edge with no database server, and fast to read for any range.

- **Values:** numbers, booleans (0 / 1), strings (a dictionary per tag: a state, a recipe, a mode). Every value is a float64 on disk; no object per point in the write path.
- **Pure:** no assets, no JSON, no events. A nested object is split into tags by the asset layer above (`node-red-asset-engine`); event frames come from `node-red-event-engine`.
- **Compression:** Gorilla (delta-of-delta timestamps, XOR values) plus a decimal codec: when a chunk's values all have k decimals (PLC data), they are stored as integers ×10^k as deltas. ~1.2 bytes a point for a 2-decimal process value, ~2 bits for a constant or a state.
- **Summary pyramid (M4 / OM3):** per chunk (~1 024 points), per segment (1 h) and per day: first, last, min, max **with their times**, sum, count. A chart's M4 (the extremes of each pixel column) and the bucket aggregates are answered from the summaries, reading about as many records as the answer has rows, whatever the range.

## Nodes

| Node | Does |
|---|---|
| **tsdb-config** | A database: a folder (default `<userDir>/tsdb/<name>`), raw retention, summary retention, WAL sync, checkpoint. |
| **tsdb-store** | `msg.topic` + `msg.payload` (+ `msg.timestamp`), or `msg.payload` = `[{ tag, ts, value }]`, or `{ tag: value }`. Tag prefix, changes only, deadband. |
| **tsdb-query** | The node's settings are a query; `msg.query` overrides any field. Output in `msg.payload`. |

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

## Benchmark (`npm run bench`, Windows laptop, Node 24, warm OS cache)

| | |
|---|---|
| write 9 000 tags × 100 ms (WAL fsync every second) | **321 000 points/s** (real time needs 90 000) |
| size | **1.26 bytes / point** |
| chart, 6 months (15.6 M points), 1 200 px / 4 000 px | **13 ms** / **13 ms** |
| avg / min / max per hour, 6 months | 12 ms |
| per 8 h shift from 06:00, 6 months | 5 ms |
| chart, last 7 days, 1 200 px | 111 ms |
| raw, last hour / last value | 3 ms / 1 ms |

## Limits of this MVP (next steps)

- **Late data** (older than a tag's newest point) is refused and counted; backfill comes later.
- **Synchronous I/O in the Node-RED process**; the engine moves to a worker thread next (its API is already message-shaped).
- **No fluent JS builder yet** (`tsdb.query("Oven1.Temp").last("8h")…`): it will build the same query object.
- Planned: time-budgeted queries, event-aware retention, KPIs at ingest (state durations, counters), quality codes in NaN payloads, blobs, a binary transport to Nexa charts.

## Tests

```
npm test        # Gorilla round trips, the engine against brute force (every pyramid level), crash recovery, retention, the nodes
npm run bench   # --tags 9000 --points 600 --months 6 --period 1000 --keep
```
