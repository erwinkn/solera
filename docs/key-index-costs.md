# Key index: cost analysis

Companion to `object-store-state.md` §6. What the engine-owned key index
costs on S3, per operation and per month, from 1K to 100M keys per index.

Every number comes from `bench/cost_model/` (`model.py` holds the
assumptions and formulas; `tables.py` and `scenarios.py` print the tables
below). They are **estimates**: the K0 prototype measures the assumptions
marked below and the model is re-run with the measured values.

## Summary

- **Up to ~1M keys per index, the key index costs nothing measurable.** An
  index that size is at most ~25 MB and is read in a handful of requests;
  a commit costs about what the attempt costs anyway.
- **Storage never matters:** $0.06 a month at 100M keys.
- **At high frequency, the attempt itself is the main cost**, not the
  index: an attempt every 10 seconds costs ~$6.70 a month in spec, result,
  log and journal writes.
- **Scattered writes into a very large index are the one pattern that
  costs noticeably more.** Per-file Bloom filters keep them in check: 100M
  keys with 1,000 random changes every 10 s costs ~$14 a month on a cold
  worker (~$116 without filters), ~$8 with index files cached on local
  disk, and ~$1 if the same changes are committed every 10 minutes.
- **CPU, not requests, is what needs native code:** at 10M–100M keys,
  sorting and comparing take seconds natively and minutes in pure Python.

## Assumptions

| Parameter | Value | Measured in K0 |
|---|---|---|
| Entry size | ~40 B raw (24 B key, 16 B version), ~20 B compressed | yes |
| Filters | per file, a Bloom filter of keys and one of `(key, version)` pairs, 14 bits per item each (~3.5 B per entry); 0.2% false positives per check | yes |
| Block | 64 KB raw, ~1,640 entries, ~32 KB compressed | |
| Compacted file cap | 64 MB | |
| Level structure | level 0: up to 8 delta files; deeper levels 10× apart (about 4 levels at 100M keys); upper levels ≈ 11% of the bottom level | |
| Read strategy | per level, one of: stream the whole level in 16 MB range reads; read each file's tail (footer, block index, filters) and then the touched blocks; or the same with filters, reading blocks only for keys the filters can't clear. Consecutive blocks are one range read; files are selected by key range. The **cheapest option that fits a 2 s latency budget** wins, else the fastest. | yes (crossover) |
| Requests in parallel | 64 | |
| Request latency | 30 ms | yes, on real S3 |
| Throughput to S3 | 500 MB/s aggregate | yes, on real S3 |
| CPU (merge, compare, scan) | native 30M entries/s; pure Python 1.5M entries/s | yes |
| Compaction write amplification | ~5× per level (each entry rewritten ~5–20× over its life) | yes |
| Attempt overhead | 4 PUT + 2 GET: spec, result, log chunk, joined log; plus 1 journal PUT per commit (upper bound — flushes are shared when several commits land within a second) | |
| Prices | S3 Standard list prices: PUT $5 per million, GET $0.40 per million, storage $0.023/GB-month; DELETE and same-region transfer free. Cloudflare R2 is ~10% cheaper per request and $0.015/GB, with free egress. | check before relying |

"Cold" means a worker with no cache — every attempt in a fresh process,
as with ECS or Kubernetes placements. "Warm cache" means file tails
(footers, block indexes, filters) and the small upper levels are cached,
but not bottom-level blocks.

## How the filters work

A commit needs to know, for each written `(key, version)`, whether it is a
real change. Every index file carries two Bloom filters: one of its keys,
one of its `(key, version)` pairs. A Bloom filter answers "definitely not
present" or "maybe present".

- A pair that is **definitely absent** from every file that could hold the
  key is a real change: the key's current `(key, version)` is always
  present in some file, so this can't be it. No block is read.
- A pair that is **maybe present** — an unchanged rewrite, or a false
  positive — gets the exact block lookup.
- The key filter tells new keys from existing ones for the key count, and
  lets exact lookups skip files that don't hold the key.

The **key count** becomes "exact after each compaction, and within the
filters' false-positive rate between compactions": a new key that a key
filter wrongly reports as present is counted as an update until the next
compaction recounts.

## Per operation

Columns are the number of keys in the index.

### Index size and storage

| | 1K | 10K | 100K | 1M | 10M | 100M |
|---|---|---|---|---|---|---|
| index size (incl. filters) | 0.03 MB | 0.26 MB | 2.55 MB | 25.50 MB | 255.00 MB | 2.5 GB |
| storage / month | $0.0000006 | $0.0000059 | $0.0000587 | $0.00059 | $0.00587 | $0.059 |

### Initial load (first commit, empty index)

Nothing to compare against; the sorted file goes straight to the bottom
level, so there is no compaction.

| | 1K | 10K | 100K | 1M | 10M | 100M |
|---|---|---|---|---|---|---|
| requests | 6 PUT | 6 PUT | 6 PUT | 7 PUT | 17 PUT | 125 PUT |
| cost | $0.0000308 | $0.0000308 | $0.0000308 | $0.0000358 | $0.0000858 | $0.00063 |
| upload time | 0 ms | 0 ms | 4 ms | 40 ms | 400 ms | 4.0 s |
| sort, native | 0 ms | 1 ms | 14 ms | 166 ms | 1.9 s | 22.1 s |
| sort, pure Python | 2 ms | 22 ms | 277 ms | 3.3 s | 38.8 s | 7 min |

### Incremental commit: 100 random keys changed

| | 1K | 10K | 100K | 1M | 10M | 100M |
|---|---|---|---|---|---|---|
| GETs, cold, exact lookups only | 8 | 8 | 8 | 9 | 20 | 150 |
| GETs, cold, with filters | 8 | 8 | 8 | 9 | 12 | 42 |
| GETs, warm cache | 2 | 2 | 2 | 2 | 3 | 3 |
| cost, cold | $0.0000332 | $0.0000332 | $0.0000332 | $0.0000336 | $0.0000348 | $0.0000468 |
| lookup wall time, cold | 30 ms | 30 ms | 34 ms | 74 ms | 145 ms | 814 ms |
| compaction I/O (amortized) | 0.01 MB | 0.01 MB | 0.01 MB | 0.02 MB | 0.03 MB | 0.04 MB |

### Incremental commit: 1K random keys changed

| | 1K | 10K | 100K | 1M | 10M | 100M |
|---|---|---|---|---|---|---|
| GETs, cold, exact lookups only | 8 | 8 | 8 | 9 | 20 | 1,042 |
| GETs, cold, with filters | 8 | 8 | 8 | 9 | 17 | 56 |
| GETs, warm cache | 2 | 2 | 2 | 2 | 8 | 10 |
| cost, cold | $0.0000332 | $0.0000332 | $0.0000332 | $0.0000337 | $0.0000369 | $0.0000525 |
| lookup wall time, cold | 30 ms | 30 ms | 34 ms | 74 ms | 145 ms | 815 ms |
| compaction I/O (amortized) | 0.06 MB | 0.06 MB | 0.10 MB | 0.20 MB | 0.30 MB | 0.40 MB |

### Incremental commit: 1K random keys written, half of them unchanged

Unchanged rewrites are exactly the keys the filters can't clear, so they
need the exact lookup — this is where filtering unchanged rows costs
something.

| | 1K | 10K | 100K | 1M | 10M | 100M |
|---|---|---|---|---|---|---|
| GETs, cold, exact lookups only | 8 | 8 | 8 | 9 | 20 | 1,042 |
| GETs, cold, with filters | 8 | 8 | 8 | 9 | 20 | 552 |
| GETs, warm cache | 2 | 2 | 2 | 2 | 14 | 504 |
| cost, cold | $0.0000332 | $0.0000332 | $0.0000332 | $0.0000337 | $0.0000381 | $0.00025 |
| lookup wall time, cold | 30 ms | 30 ms | 34 ms | 74 ms | 474 ms | 1.4 s |
| compaction I/O (amortized) | 0.06 MB | 0.06 MB | 0.10 MB | 0.20 MB | 0.30 MB | 0.40 MB |

### Incremental commit: 1K clustered keys changed

Keys that sort near each other — a date or partition prefix, sequential
ids. Filters don't matter here: the changed keys share a few blocks.

| | 1K | 10K | 100K | 1M | 10M | 100M |
|---|---|---|---|---|---|---|
| GETs, cold, exact lookups only | 8 | 8 | 8 | 9 | 11 | 12 |
| GETs, cold, with filters | 8 | 8 | 8 | 9 | 11 | 12 |
| GETs, warm cache | 2 | 2 | 2 | 2 | 3 | 3 |
| cost, cold | $0.0000332 | $0.0000332 | $0.0000332 | $0.0000337 | $0.0000345 | $0.0000349 |
| lookup wall time, cold | 30 ms | 30 ms | 34 ms | 74 ms | 75 ms | 31 ms |
| compaction I/O (amortized) | 0.06 MB | 0.06 MB | 0.10 MB | 0.20 MB | 0.30 MB | 0.40 MB |

### Incremental commit: 100K random keys changed

At 100M, streaming the whole level takes ~140 reads but ~4.4 s, over the
2 s budget, so the reader takes the filtered path: more reads (still
$0.0004 per commit) but 1.6 s.

| | 1K | 10K | 100K | 1M | 10M | 100M |
|---|---|---|---|---|---|---|
| GETs, cold, exact lookups only | 8 | 8 | 9 | 10 | 22 | 142 |
| GETs, cold, with filters | 8 | 8 | 9 | 10 | 22 | 845 |
| GETs, warm cache | 2 | 2 | 3 | 3 | 16 | 797 |
| cost, cold | $0.0000332 | $0.0000334 | $0.0000364 | $0.0000400 | $0.0000477 | $0.00038 |
| lookup wall time, cold | 30 ms | 31 ms | 38 ms | 78 ms | 478 ms | 1.6 s |
| compaction I/O (amortized) | 0.06 MB | 0.60 MB | 10.00 MB | 20.00 MB | 30.00 MB | 40.00 MB |

### Full replacement (bare return of every row, 1% changed)

A full replacement must compare every existing key, so it reads the whole
index; that is inherent, and cheap in requests.

| | 1K | 10K | 100K | 1M | 10M | 100M |
|---|---|---|---|---|---|---|
| GETs | 4 | 4 | 4 | 5 | 17 | 152 |
| cost | $0.0000316 | $0.0000316 | $0.0000316 | $0.0000320 | $0.0000368 | $0.0000958 |
| read time | 30 ms | 30 ms | 34 ms | 70 ms | 430 ms | 4.1 s |
| compare, native | 0 ms | 0 ms | 3 ms | 33 ms | 333 ms | 3.3 s |
| compare, pure Python | 1 ms | 7 ms | 67 ms | 667 ms | 6.7 s | 66.7 s |

### Full delivery to a consumer

A consumer re-reads everything (new consumer, version bump, `full` run),
one attempt per page of `batch_size` keys. Only the key index side is
counted here; loading the rows is the store's cost.

Pages of 10K keys:

| | 1K | 10K | 100K | 1M | 10M | 100M |
|---|---|---|---|---|---|---|
| pages / attempts | 1 | 1 | 10 | 100 | 1,000 | 10,000 |
| GETs | 7 | 7 | 70 | 800 | 12,000 | 120,000 |
| cost (index + attempt overhead) | $0.0000278 | $0.0000278 | $0.00028 | $0.00282 | $0.030 | $0.298 |

Pages of 100K keys:

| | 1K | 10K | 100K | 1M | 10M | 100M |
|---|---|---|---|---|---|---|
| pages / attempts | 1 | 1 | 1 | 10 | 100 | 1,000 |
| GETs | 7 | 7 | 7 | 80 | 1,200 | 12,000 |
| cost (index + attempt overhead) | $0.0000278 | $0.0000278 | $0.0000278 | $0.00028 | $0.00298 | $0.030 |

Most of this is per-attempt overhead, so page size matters more than
index size — and even the worst case is $0.30 per full re-read of 100M
keys.

## Per month, for representative workloads

"Key index" is what the index adds; "attempt + journal" is what running
the attempts costs regardless. Filters on, cold workers.

| Workload | Commits / month | Key index | Attempt + journal overhead | Index storage | **Total / month** | Time per commit | |
|---|---|---|---|---|---|---|---|
| A. Reference table, 1K keys, full replace hourly (1% changed) | 720 | $0.0042 | $0.02 | <$0.0001 | $0.02 | 30 ms |  |
| B. SharePoint inventory, 100K keys, 100 random changes every 10 s | 259,200 | $1.92 | $6.69 | <$0.0001 | $8.61 | 34 ms |  |
| C. Event table, 10M keys, 10K clustered changes every minute | 43,200 | $0.41 | $1.11 | $0.0059 | $1.53 | 75 ms |  |
| D. Large dimension, 100M keys, 1M random changes daily | 30 | $0.04 | $0.0008 | $0.06 | $0.10 | 4.6 s | + one full delivery to a new consumer (100K pages) |
| E. Worst case, 100M keys, 1K random changes every 10 s | 259,200 | $6.92 | $6.69 | $0.06 | $13.67 | 815 ms |  |
| F. Big full replacement, 100M keys daily (1% changed) | 30 | $0.0021 | $0.0008 | $0.06 | $0.06 | 4.1 s |  |

In B, most of the key index's $1.92 is the one delta file written per
commit ($1.30); the lookups themselves are a few requests.

### The scattered-writes case

In E, each commit checks 1,000 keys scattered over a 2.5 GB index.
Without filters that is ~1,000 small reads per commit, 270M a month.

| Variant | Commits / month | Key index | Attempt + journal overhead | **Total / month** | Time per commit |
|---|---|---|---|---|---|
| E0. exact lookups only, cold worker | 259,200 | $109.14 | $6.69 | $115.89 | 1.0 s |
| E. with filters, cold worker | 259,200 | $6.92 | $6.69 | $13.67 | 815 ms |
| E1. with filters + index files cached on local disk | 259,200 | $1.33 | $6.69 | $8.07 | 50 ms |
| E2. same changes committed every 10 min (60K per commit), cold | 4,320 | $0.96 | $0.11 | $1.13 | 1.4 s |

- **E, filters:** each commit reads every file's tail — footer, block
  index and filters, ~390 MB in ~55 reads — and then only the handful of
  blocks for keys the filters can't clear.
- **E1, local disk cache:** index files never change once written, so a
  cached copy is never stale and needs no invalidation. The `Local`
  placement shares one bounded cache across attempts (`Project(key_cache=…)`);
  after the first commit, only new delta files are downloaded.
- **E2, batching:** most workloads with 100M keys don't need 10-second
  freshness.

## What this means for the design

1. **No change to the model.** The index is essentially free up to ~1M
   keys per index and stays cheap beyond that.
2. **Filters in every index file from the start**, with the cost-aware
   read strategy.
3. **`Project(key_cache=…)`**, on by default for the `Local` placement
   when the engine's machine has a writable directory, with a size cap.
4. **Native code is needed for large indexes**, for CPU rather than
   request cost: 100M-key sorts and full comparisons take seconds natively
   and minutes in pure Python. Pure Python remains fine up to ~1M keys per
   operation.
5. **Attempt overhead is worth trimming** since it dominates at high
   frequency: no log object when nothing was logged, and a single write
   instead of chunk + join when the log fits in one chunk. That takes a
   typical attempt from 4 PUT to 2–3.

## What K0 must confirm

- Entry size and compression ratio on realistic keys (the 20 B/entry
  assumption scales every size and read above).
- Filter size and false-positive rate as built.
- Native and pure-Python throughput for sort, merge, compare and scan.
- The read-strategy crossovers, and how many range reads in parallel a
  worker actually sustains.
- Request latency and throughput on real S3 — the one thing a local
  S3-compatible server cannot tell us.
