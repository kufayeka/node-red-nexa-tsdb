# Soak test: years of data, random ranges, kills

`gen.js` writes a historian of years of data into a folder; `verify.js` then asks it random questions and compares every
answer with the one it **recomputes** (every value is a pure function of tag and time, see `model.js`: nothing is stored
to know the right answer).

```
# 1. generate (an EMPTY folder; it refuses a folder that holds something, and a disk that is too small)
node test/soak/gen.js --dir D:\tsdb-soak --preset small       # 100 tags every 1 min, 5 years  = 263 M points
node test/soak/gen.js --dir D:\tsdb-soak --preset medium      # 100 tags every 10 s,  5 years  = 1.6 B points
node test/soak/gen.js --dir D:\tsdb-soak --preset large       # 20 tags every 1 s,    5 years  = 3.2 B points
#    options: --years 5 | --days N   --tags N   --period 1m|10s|1s   --force (ignore the disk estimate)

# 2. the durability test: the generator is killed (SIGKILL, the whole process, mid write) every ~120 s and restarted
node test/soak/gen.js --dir D:\tsdb-soak --preset medium --kill-every 120
#    Ctrl+C or a power cut does the same: run the same command with --resume and it continues

# 3. verify
node test/soak/verify.js --dir D:\tsdb-soak --queries 500 --seed 1
node test/soak/verify.js --dir D:\tsdb-soak --seed 1 --only 17      # replay one query (printed on a failure)
#    --direct: through the engine in this process (no worker)   --reopen 100: close and open every 100 queries
#    --max-points 6000000: the most points one query is checked against (the brute force is the cost)
```

The generator prints its rate and an ETA every 10 s and the disk it expects (about 1.4 - 4 bytes a point plus the WAL) up
front. The data has number tags (2 decimals, a slow wave, a daily wave, noise, a rare +60 spike), boolean tags and string
(state) tags, and about 2 % of tag-days lose 1 - 6 hours (gaps).

**What a query is checked for** (a random tag set, a random range over the whole dataset, a fifth of them "a whole calendar
month of a random year"):

| mode | must be |
|---|---|
| `raw` | every point exact, nothing missing, nothing extra |
| `bucket` | per bucket: count, sum, min, max, first, last, avg exact (random size 1 min .. 1 day, shift offsets, edges that are not on the hour) |
| `m4` | every returned point is a real point; the first, the last and the overall min / max exact; the columns' own min / max exact in nearly all columns (reported as a percentage, 90 % required) |
| `last` | the newest point at or before the time |

It prints p50 / p95 / max per mode and the time to open the database (what a restart costs). Exit code 1 and the failing
queries (with `--seed N --only i` to replay) when anything differs.

Smoke run (30 tags every 10 s for 40 days, killed twice): 300 queries, no difference, 97.8 % of the M4 columns exact.
