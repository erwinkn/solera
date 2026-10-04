# Key index: cost analysis

Companion to `object-store-state.md` §6. What the engine-owned key index
costs on S3, per operation and per month, from 1K to 100M keys per index.

**Status: these costs are the leveled index's.** The index is now spans
tiling commit time, merged under the policy of `key-index-design.md`;
levels, compaction and the delta log are gone. The per-commit read and
write costs below still describe the read strategy, which spans kept; the
compaction rows and the level structure do not. The span index's costs
are in `key-index-design.md` — replayed on metadata (`bench/keys/spans.py`,
`retention.py`) and measured on prototype files (`v4bench.py`,
`catchup.py`, `layouts.py`, on branch
`bb/key-index-design-first-principles-thr_xvgqnrw2kr` at `42c4b69`).
They are re-measured over the implementation next, and this document
then follows. The leveled benches cited here (`bench.py`, `warm.py`,
`bulk.py`, `amplification.py`) run at commit `0d09fc4`.

The per-operation and per-month tables come from `bench/cost_model/`
(`model.py` holds the assumptions and formulas; `tables.py` and
`scenarios.py` print them). They are **estimates**, left at the model's
assumptions; the measured values are in the assumptions table's last
column and under **Measured**, which compares the two.

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
  keys with 1,000 random changes every 10 s costs ~$13.50 a month measured
  on a cold worker in steady state (~$116 modelled without filters), ~$8
  with index files cached on local disk, and ~$1 if the same changes are
  committed every 10 minutes.
- **Bulk operations are bounded in memory, not in keys:** a full
  replacement of 100M keys takes ~28 s and 0.8 GB beyond the data the
  worker holds (Arrow rows); a compaction rewriting a 100M-key level 27 s
  and 0.5 GB. All per-key work is native.

## Assumptions

| Parameter | Value (model) | Measured (`bench/keys/results.md`) |
|---|---|---|
| Entry size | ~40 B raw (24 B key, 16 B version), ~20 B compressed | format v3 (`versions.md`), a generation per entry and no content version: 8–9 B with filters on random ids; 34 B on UUIDs; 6 B on sequential ids. A source's 16-byte versions add ~18 B (27 B on random ids) |
| Filters | per file, a Bloom filter of keys and one of `(key, version)` pairs, 14 bits per item each (~3.5 B per entry); 0.2% false positives per check (the model, before format v3) | two filters, keys and tombstones: 1.75 B per entry; 0.35% false positives per check |
| Block | 64 KB raw, ~1,640 entries, ~32 KB compressed | ~2,400 entries, ~60 KB compressed on random ids |
| Compacted file cap | 64 MB | files split at ~95 MB compressed |
| Level structure | level 0: up to 8 delta files; deeper levels 10× apart (about 4 levels at 100M keys); upper levels ≈ 11% of the bottom level | 3 levels at 100M; level 0 merges in itself until it holds a tenth of level 1 |
| Read strategy | per level, one of: stream the whole level in 16 MB range reads; read each file's tail (footer, block index, filters) and then the touched blocks; or the same with filters, reading blocks only for keys the filters can't clear. Consecutive blocks are one range read; files are selected by key range. The **cheapest option that fits a 2 s latency budget** wins, else the fastest. | levels over 32 MB always start with their tails; once the filters say which keys need a block, the planner picks per level between those blocks and the rest of each file (crossover tables) |
| Requests in parallel | 64 | |
| Request latency | 30 ms | not yet on real S3 |
| Throughput to S3 | 500 MB/s aggregate | not yet on real S3 |
| CPU (merge, compare, scan) | native 30M entries/s; pure Python 1.5M entries/s | native, one core: decode 4.6M, encode 1.3M entries/s; streaming on 8 cores: write 6M, replace ~4M, compact ~4M entries/s; filters ~0.4M keys/s including Python |
| Compaction write amplification | ~5× per level (each entry rewritten ~5–20× over its life) | 16× / 22× / 31× over an entry's life at 1M / 10M / 100M keys |
| Attempt overhead | 4 PUT + 2 GET: spec, result, log chunk, joined log; plus 1 journal PUT per commit (upper bound — flushes are shared when several commits land within a second) | |
| Prices | S3 Standard list prices: PUT $5 per million, GET $0.40 per million, storage $0.023/GB-month; DELETE and same-region transfer free. Cloudflare R2 is ~10% cheaper per request and $0.015/GB, with free egress. | check before relying |

"Cold" means a worker with no cache — every attempt in a fresh process,
as with ECS or Kubernetes placements. "Warm cache" means file tails
(footers, block indexes, filters) and the small upper levels are cached,
but not bottom-level blocks.

## How the filters work

Every key written is a change (`versions.md`), so a commit needs the
index only to tell new keys from existing ones — the count — and to find
a source key's version to compare. Every index file carries two Bloom
filters (since format v4, one: its keys). A Bloom filter answers
"definitely not present" or "maybe present".

- A key **no key filter** of any file that could hold it matches is new.
  No block is read.
- Anything else gets the exact block lookup, which also lets lookups skip
  files that don't hold the key.

Writes are exact (`key-index-design.md`), so the **key count** is exact.
The figures below were measured before that, when a key filter match with
no tombstone filter match counted as an update with no block read; a cold
write of existing keys now reads their blocks (~1.25 per key).

## Measured

The K0 prototype and its follow-ups ran every operation below against an
S3-compatible server (MinIO) with 30 ms injected per request, 80 MB/s per
connection and 64 requests in parallel; full numbers in
`bench/keys/results.md`. The request counts are exact — they are what S3
would bill; wall times are only as good as the latency model. "Fresh" is
an index built straight into its bottom level; "steady" has its upper
levels filled the way steady-state random writes leave them.

| At 100M keys | Model | Measured, fresh | Measured, steady |
|---|---|---|---|
| Index size, incl. filters | 2.5 GB (23.5 B/entry) | 2.7 GB (27.2 B/entry; filters 3.5 B) | 3.4 GB |
| 1K random keys changed, cold | 56 GETs, 0.8 s | **34 GETs, 0.5 s**, 351 MB read | **48 GETs, 0.8 s**, 445 MB read |
| 1K written, half unchanged | 552 GETs | 542 GETs, 1.0 s | 291 GETs, 1.1 s |
| 1K clustered keys changed | 12 GETs | 1 GET, 0.2 s | |
| 1K new keys inserted | — | 29 GETs, 0.5 s | |
| 100K random keys changed | 845 GETs, 1.6 s | 376 GETs, 1.3 s | |
| 1K random, engine cache warm | 10 GETs | 0 GETs, 57 ms, the delta uploaded | |
| Full-pass batch of 10K keys | 12 GETs | 2 GETs, 0.3 MB, 0.1 s | 14 GETs, 1.4 MB, 0.1 s |
| Full scan (recount), 100K-key pages | ~150 GETs (§6) | 1,077 GETs, 122 s | 3,029 GETs, 242 s |

Scenario E (1K random changes every 10 s into 100M keys, cold) is about
**$11.50 a month** on a fresh index and **$13.54 in steady state** — 50
GETs and a PUT per commit, plus compaction's 1.2 GETs and 0.14 PUTs per
commit (`bench/keys/amplification.py`) — against $13.67 modelled. Before
level 0 merged in itself and the planner read the filters first, the
steady state cost ~$14.40–14.60 at 1.5 s per commit; it now takes 0.8 s.

Where the model was wrong:

- **Entries were bigger than assumed** while they carried a content
  version: 27–29 B with filters on random 12-digit ids, 51 B on UUIDs,
  24 B on sequential ids. Format v3 (`versions.md`) drops the content version and
  the pair filter: 8–9 B on random ids, 34 B on UUIDs, 6 B on sequential
  ids, with ~18 B more for a source's 16-byte versions. Sizes and
  transfer scale accordingly; request counts barely move, except a cold
  source commit with versions, which reads every touched key's entry.
- **Filters false-positive 0.35% of the time**, not 0.2%: keeping an
  item's 10 bits inside one 512-bit block costs ~1.8× over independent bits.
- **Compaction rewrote far more than assumed** while every level-0 merge
  went into level 1: with random keys every level-1 file overlaps every
  delta, so 0.23 MB of deltas rewrote up to 67 MB, 125–159× over an
  entry's life. Level 0 now merges in itself until it holds a tenth of
  level 1: 16×, 22× and 31× at 1M, 10M and 100M keys, near the model's
  ~5–20×.
- **A recount is ~370 reads at 100M keys**, not ~150: it streams every
  level in 8 MB segments (2.4 GB, 14 s); paging it 100K keys at a time
  took ~1,100.
- **CPU is slower than assumed.** On one core, native encoding runs at
  about 1.3M entries/s (zlib level 1 is most of it) and decoding at about
  4.6M/s; the model assumed 30M/s. Blocks now decode and compress on every
  core, so the streaming operations run at ~4M entries/s on 8 cores: a
  full replacement of 100M keys takes ~28 s (sorting them ~5 s of it) and
  0.8 GB beyond the written rows, against ~45 GB before it streamed.
  Request costs are unaffected — the work runs on the worker.

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

### Full pass to a consumer

A consumer re-reads everything (new consumer, version bump, `full` run),
one attempt per batch of `batch_size` keys. Only the key index side is
counted here; loading the rows is the store's cost.

Batches of 10K keys:

| | 1K | 10K | 100K | 1M | 10M | 100M |
|---|---|---|---|---|---|---|
| batches / attempts | 1 | 1 | 10 | 100 | 1,000 | 10,000 |
| GETs | 7 | 7 | 70 | 800 | 12,000 | 120,000 |
| cost (index + attempt overhead) | $0.0000278 | $0.0000278 | $0.00028 | $0.00282 | $0.030 | $0.298 |

Batches of 100K keys:

| | 1K | 10K | 100K | 1M | 10M | 100M |
|---|---|---|---|---|---|---|
| batches / attempts | 1 | 1 | 1 | 10 | 100 | 1,000 |
| GETs | 7 | 7 | 7 | 80 | 1,200 | 12,000 |
| cost (index + attempt overhead) | $0.0000278 | $0.0000278 | $0.0000278 | $0.00028 | $0.00298 | $0.030 |

Most of this is per-attempt overhead, so batch size matters more than
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
| D. Large dimension, 100M keys, 1M random changes daily | 30 | $0.04 | $0.0008 | $0.06 | $0.10 | 4.6 s | + one full pass to a new consumer (batches of 100K keys) |
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
| E1. with filters, resolved by the engine's cache | 259,200 | $1.33 | $6.69 | $8.07 | 113 ms |
| E2. same changes committed every 10 min (60K per commit), cold | 4,320 | $0.96 | $0.11 | $1.13 | 1.4 s |

- **E, filters:** each commit reads every file's tail — footer, block
  index and filters, ~390 MB in ~55 reads — and then only the handful of
  blocks for keys the filters can't clear.
- **E1, the engine's cache:** index files never change once written, so a
  cached copy is never stale and needs no invalidation. The engine keeps
  them on its local disk and answers a small write from them
  (`resolved-commits.md`); after its first fill, only new delta files are
  downloaded, and those it mostly wrote itself. Measured end to end at
  100M keys, the delta uploaded: 113 ms (`bench/keys/results.md`).
  Workers keep no cache of their own.
- **E2, batching:** most workloads with 100M keys don't need 10-second
  freshness.

## What this means for the design

1. **No change to the model.** The index is essentially free up to ~1M
   keys per index and stays cheap beyond that.
2. **Filters in every index file from the start**, with the cost-aware
   read strategy.
3. **The engine's cache of index files** (`resolved-commits.md`), on by
   default when its machine has a writable directory, with a disk budget;
   workers read the store.
4. **Native code, streaming, for large indexes**, for CPU and memory
   rather than request cost: 100M-key sorts and full comparisons take
   seconds, and hold a permutation of the written rows and a few buffers,
   never the index.
5. **Attempt overhead is worth trimming** since it dominates at high
   frequency: no log object when nothing was logged, and a single write
   instead of chunk + join when the log fits in one chunk. That takes a
   typical attempt from 4 PUT to 2–3.

## What remains to measure

Everything above ran against MinIO with injected latency and bandwidth.
Request counts and bytes carry over to S3 exactly; wall times need one run
on real S3 from a worker in the bucket's region (`object-store-state.md`
§13).
