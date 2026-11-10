# The change index at 10M and 100M keys, against the format benchmark (SOLE-6)

The shipped reader (`native/index`) meets the bar set by the format benchmark's "ours" prototype (T44, `exp/layer-format`) on all four workloads at both scales. Every result was checked against a brute-force fold of the commit stream, 124 results per scale, with 0 mismatches.

- **Cold time.** Queries that take more than 0.15 s run at 0.63–1.12× T44's time. The runs above 1.0× are either folds, which are CPU-bound and run on a slower machine than T44 did (below), or queries under 0.25 s, where single runs vary by ±10%.
- **GETs.** Queries make the same number of GETs as T44 or fewer: scans and folds at 100M make 13–25% fewer. Merges, and folds at 10M, make more, as explained below.
- **Bytes read.** Within 5% of T44, except a few first-10K pages and 1%-of-keys diffs, which read up to 0.6 MB more or up to 17 MB less (below).
- **Peak memory.** Lower everywhere except the merge of the 4 newest layers (up to 1.15×, 16.6 MB) and the 1-day diffs over 1% of keys (1.1 MB at most).

## Method

`change-index-bench` (`src/main.rs`) replays T44's run: the same workload generators, seed (7), 30 days × 24 commits a day on top of a load commit, the same layer schedule (`src/sched.rs`, the size rule), and the same query set. The layers the schedule leaves are written directly with the crate's `Writer` (the encoder `write_commit` and `merge` use). The measured merges go through `ChangeIndex::merge`, and queries go through `ChangeIndex` with its default `StoreOptions`.

- **cold**: a fresh `Store` (empty memory cache) over a simulated object store (`src/sim.rs`) that charges every GET 30 ms of latency plus transfer at 80 MB/s, as T44 did. Times are wall time; GETs and MB are what the store served.
- **warm**: the same query on another fresh `Store` with no simulated latency, over the same local files, which the OS has already cached. The memory cache starts empty, so *warm CPU* (the query's thread CPU time) covers reading, checking and decoding every block the query needs.
- **peak MB**: the counting allocator's high-water mark during the query, minus what was allocated before it started.
- **fold ✓**: the result equals the brute-force fold of the commit stream at the queried commits.
- The T44 numbers are its final runs (`t44/*.jsonl`, the "ours" rows of `exp/layer-format` `results/final-*`).

Run: `./run.sh NAME workload=W keys=N` writes `results/NAME.jsonl`. `python3 report.py results/*-10m.jsonl` prints a table below. Run the 100M configurations one at a time: process peak RSS is 6.1–10.7 GB, mostly the bench's oracle and generator. The tables below are from commit dd164084.

## What differs from T44, and why

- **Scans and folds make fewer GETs at 100M.** For example, a scan at head on daily-scattered makes 157 GETs where T44 made 202. Batches double up to `max_get` (16 MiB), the largest GET, and end where it would. Listed-key reads count the bytes their GETs fetch, gaps included, and keep at most 16 MiB ahead per layer.
- **Merges make more GETs, and folds make more at 10M.** A merge reads each input layer's commits section in its own GET, at the same time as the entries walk; T44 read it afterwards. That costs one GET per input layer. Holding the commit sections during the walk also raises a small merge's peak memory to at most 1.15× T44's (16.6 MB).
- **Gets binary-search the listed keys inside each decoded block**, so they skip the rows nobody asked for. For 10K scattered gets (the scale target's batch), warm CPU before and after this change:

  | workload | 10M before | 10M after | 100M before | 100M after |
  |---|---|---|---|---|
  | daily-clustered | 1.23 | 0.74 | 1.81 | 1.01 |
  | daily-scattered | 1.47 | 0.75 | 4.52 | 2.47 |
  | hot | 1.14 | 0.57 | 2.89 | 1.61 |
  | rewrite | 1.92 | 1.16 | 3.67 | 2.19 |

  ("Before" is commit 07e16ddf, CPU seconds.) At 100M, what remains is decoding about one block per listed key per layer.
- **Warm CPU is up to 30% higher than T44's on whole-layer reads and folds.** This is the machine, not the code. T44 ran on a different machine; T44's own bench rerun on this machine (rewrite, 10M, twice each, interleaved) gives:

  | query | index CPU s | T44 ours CPU s |
  |---|---|---|
  | diff, paused 30 days, all | 1.62, 1.78 | 2.13, 2.24 |
  | scan at head, all | 2.92, 3.41 | 3.45, 3.44 |
  | fold every layer into the base (cold) | 4.06, 4.73 | 5.04, 4.71 |

  The same machine difference explains the two folds that run slower cold: hot at 10M (1.10×) and rewrite at 100M (1.07×). A fold is CPU-bound.
- **Lower peak memory.** Bulk reads (scans, folds, merges) above `StoreOptions.cache_batch` (8 MiB) are decoded straight from the response and never enter the memory cache.
- **Bytes on rewrite.** "diff, paused 30 days, first 10K" reads 0.6 MB more at 100M (4.2 vs 3.6 MB). The metadata is larger (3.76 vs 2.68 MB for the whole layer set) because segments carry checksums and first keys, and this query reads more of it than it reads blocks. At 10M the same query reads 1.5 MB less, and "diff, 1 day, first 10K" at 100M reads 17 MB less (121 vs 138 MB).
- Single runs of queries under 0.25 s vary by ±10%, so ratios of 1.02–1.27 on them are noise.

## 10M keys

#### daily-clustered, 10M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.14 s · 4 · 0.8 MB | 0.14 s · 4 · 0.8 MB | 1.01 · 1.00 · 1.00 | 0.01 · 0.01 | 2 · 5 | ✓ |
| diff, 1 day, first 10K | 0.07 s · 4 · 0.5 MB | 0.07 s · 4 · 0.5 MB | 0.99 · 1.00 · 1.00 | 0.00 · 0.00 | 2 · 4 | ✓ |
| diff, 1 day, all | 0.17 s · 8 · 1.7 MB | 0.18 s · 8 · 1.7 MB | 0.94 · 1.00 · 1.00 | 0.03 · 0.01 | 3 · 4 | ✓ |
| diff, 1 day, 1% of keys | 0.06 s · 4 · 0.0 MB | 0.06 s · 4 · 0.0 MB | 1.00 · 1.00 · 1.00 | 0.00 · 0.00 | 0 · 0 | ✓ |
| diff, paused 30 days, first 10K | 0.07 s · 12 · 1.7 MB | 0.08 s · 12 · 1.6 MB | 0.92 · 1.00 · 1.01 | 0.00 · 0.01 | 6 · 17 | ✓ |
| diff, paused 30 days, all | 0.83 s · 28 · 29.4 MB | 0.92 s · 28 · 29.4 MB | 0.89 · 1.00 · 1.00 | 0.44 · 0.36 | 36 · 84 | ✓ |
| diff, paused 30 days, 1% of keys | 0.14 s · 15 · 1.6 MB | 0.16 s · 15 · 1.6 MB | 0.86 · 1.00 · 1.01 | 0.03 · 0.04 | 4 · 8 | ✓ |
| scan at head, all | 2.26 s · 42 · 150.1 MB | 2.80 s · 45 · 150.1 MB | 0.81 · 0.93 · 1.00 | 1.74 · 1.44 | 78 · 285 | ✓ |
| scan at head, 1% of keys | 0.17 s · 19 · 3.0 MB | 0.18 s · 19 · 3.0 MB | 0.92 · 1.00 · 1.02 | 0.04 · 0.03 | 6 · 11 | ✓ |
| scan a day ago, 1% of keys | 0.17 s · 17 · 3.0 MB | 0.20 s · 17 · 3.0 MB | 0.82 · 1.00 · 1.02 | 0.07 · 0.04 | 6 · 11 | ✓ |
| 30 scattered gets | 0.11 s · 74 · 2.1 MB | – | – | – | 4 · – | ✓ |
| 100 scattered gets | 0.16 s · 165 · 4.0 MB | – | – | – | 6 · – | ✓ |
| 1K scattered gets | 0.45 s · 705 · 32.4 MB | 0.59 s · 705 · 32.4 MB | 0.76 · 1.00 · 1.00 | 0.12 · 0.19 | 27 · 156 | ✓ |
| 10K scattered gets (a batch at the scale target) | 1.25 s · 35 · 148.5 MB | – | – | – | 65 · – | ✓ |
| merge the 4 newest layers | 0.23 s, read 3.2 MB, wrote 3.2 MB | 0.23 s, read 3.2 MB, wrote 3.2 MB | 1.01 · 1.29 · 1.00 | 0.12 · 0.07 | 8 · 12 | ✓ |
| fold every layer into the base | 3.26 s, read 150.2 MB, wrote 105.8 MB | 3.52 s, read 150.1 MB, wrote 105.7 MB | 0.93 · 1.09 · 1.00 | 2.85 · 2.34 | 111 · 285 | ✓ |

Stored: 150.1 MB (11.31 B/entry), metadata 0.24 MB, 7 layers; T44 ours 150.1 MB (11.31 B/entry), metadata 0.17 MB.
Process peak RSS (workload, oracle and bench included): 787 MB.

#### daily-scattered, 10M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.07 s · 2 · 0.1 MB | 0.06 s · 2 · 0.1 MB | 1.02 · 1.00 · 1.00 | 0.00 · 0.00 | 0 · 0 | ✓ |
| diff, 1 day, first 10K | 0.07 s · 6 · 0.6 MB | 0.07 s · 6 · 0.6 MB | 0.96 · 1.00 · 1.00 | 0.00 · 0.01 | 1 · 3 | ✓ |
| diff, 1 day, all | 0.17 s · 10 · 2.5 MB | 0.16 s · 10 · 2.5 MB | 1.07 · 1.00 · 1.00 | 0.03 · 0.02 | 4 · 5 | ✓ |
| diff, 1 day, 1% of keys | 0.06 s · 6 · 0.1 MB | 0.06 s · 6 · 0.1 MB | 1.00 · 1.00 · 1.00 | 0.00 · 0.00 | 1 · 0 | ✓ |
| diff, paused 30 days, first 10K | 0.07 s · 12 · 1.5 MB | 0.07 s · 12 · 1.5 MB | 0.96 · 1.00 · 1.01 | 0.00 · 0.01 | 3 · 8 | ✓ |
| diff, paused 30 days, all | 1.06 s · 30 · 50.7 MB | 1.27 s · 31 · 50.7 MB | 0.83 · 0.97 · 1.00 | 0.56 · 0.47 | 52 · 123 | ✓ |
| diff, paused 30 days, 1% of keys | 0.11 s · 13 · 0.7 MB | 0.10 s · 13 · 0.7 MB | 1.03 · 1.00 · 1.01 | 0.01 · 0.01 | 2 · 3 | ✓ |
| scan at head, all | 2.63 s · 44 · 171.6 MB | 3.01 s · 48 · 171.6 MB | 0.87 · 0.92 · 1.00 | 2.02 · 1.73 | 95 · 333 | ✓ |
| scan at head, 1% of keys | 0.16 s · 17 · 2.1 MB | 0.17 s · 17 · 2.0 MB | 0.91 · 1.00 · 1.03 | 0.02 · 0.03 | 5 · 6 | ✓ |
| scan a day ago, 1% of keys | 0.16 s · 13 · 2.0 MB | 0.16 s · 13 · 2.0 MB | 1.01 · 1.00 · 1.04 | 0.03 · 0.02 | 4 · 6 | ✓ |
| 30 scattered gets | 0.15 s · 104 · 5.7 MB | – | – | – | 7 · – | ✓ |
| 100 scattered gets | 0.25 s · 274 · 11.2 MB | – | – | – | 13 · – | ✓ |
| 1K scattered gets | 0.49 s · 670 · 64.9 MB | 0.79 s · 670 · 64.8 MB | 0.63 · 1.00 · 1.00 | 0.22 · 0.42 | 58 · 257 | ✓ |
| 10K scattered gets (a batch at the scale target) | 1.47 s · 37 · 170.2 MB | – | – | – | 81 · – | ✓ |
| merge the 4 newest layers | 0.20 s, read 3.8 MB, wrote 3.8 MB | 0.21 s, read 3.8 MB, wrote 3.8 MB | 0.94 · 1.29 · 1.00 | 0.10 · 0.07 | 10 · 8 | ✓ |
| fold every layer into the base | 3.83 s, read 171.7 MB, wrote 128.2 MB | 3.83 s, read 171.6 MB, wrote 128.2 MB | 1.00 · 1.06 · 1.00 | 3.34 · 2.68 | 131 · 333 | ✓ |

Stored: 171.7 MB (12.96 B/entry), metadata 0.25 MB, 7 layers; T44 ours 171.6 MB (12.95 B/entry), metadata 0.18 MB.
Process peak RSS (workload, oracle and bench included): 838 MB.

#### hot, 10M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.08 s · 2 · 0.0 MB | 0.06 s · 2 · 0.0 MB | 1.27 · 1.00 · 1.00 | 0.00 · 0.00 | 0 · 0 | ✓ |
| diff, 1 day, first 10K | 0.11 s · 7 · 0.6 MB | 0.10 s · 7 · 0.6 MB | 1.01 · 1.00 · 1.00 | 0.01 · 0.01 | 1 · 10 | ✓ |
| diff, 1 day, all | 0.11 s · 7 · 0.6 MB | 0.10 s · 7 · 0.6 MB | 1.03 · 1.00 · 1.00 | 0.01 · 0.01 | 1 · 10 | ✓ |
| diff, 1 day, 1% of keys | 0.07 s · 6 · 0.0 MB | 0.06 s · 6 · 0.0 MB | 1.19 · 1.00 · 1.00 | 0.00 · 0.00 | 0 · 1 | ✓ |
| diff, paused 30 days, first 10K | 0.27 s · 31 · 8.0 MB | 0.31 s · 31 · 8.0 MB | 0.86 · 1.00 · 1.00 | 0.15 · 0.11 | 10 · 79 | ✓ |
| diff, paused 30 days, all | 0.28 s · 31 · 8.0 MB | 0.28 s · 31 · 8.0 MB | 0.99 · 1.00 · 1.00 | 0.14 · 0.11 | 10 · 80 | ✓ |
| diff, paused 30 days, 1% of keys | 0.07 s · 18 · 0.2 MB | 0.07 s · 18 · 0.2 MB | 1.05 · 1.00 · 1.05 | 0.00 · 0.00 | 2 · 4 | ✓ |
| scan at head, all | 2.62 s · 45 · 128.7 MB | 2.92 s · 48 · 128.7 MB | 0.90 · 0.94 · 1.00 | 1.75 · 1.51 | 54 · 296 | ✓ |
| scan at head, 1% of keys | 0.16 s · 22 · 1.6 MB | 0.16 s · 22 · 1.6 MB | 0.96 · 1.00 · 1.04 | 0.02 · 0.03 | 4 · 8 | ✓ |
| scan a day ago, 1% of keys | 0.16 s · 18 · 1.6 MB | 0.17 s · 18 · 1.5 MB | 0.92 · 1.00 · 1.04 | 0.02 · 0.03 | 4 · 7 | ✓ |
| 30 scattered gets | 0.14 s · 92 · 5.5 MB | – | – | – | 7 · – | ✓ |
| 100 scattered gets | 0.17 s · 119 · 9.7 MB | – | – | – | 12 · – | ✓ |
| 1K scattered gets | 0.46 s · 587 · 31.5 MB | 0.71 s · 587 · 31.5 MB | 0.66 · 1.00 · 1.00 | 0.24 · 0.31 | 27 · 275 | ✓ |
| 10K scattered gets (a batch at the scale target) | 1.46 s · 43 · 127.0 MB | – | – | – | 43 · – | ✓ |
| merge the 4 newest layers | 0.15 s, read 0.9 MB, wrote 0.8 MB | 0.12 s, read 0.9 MB, wrote 0.8 MB | 1.18 · 1.40 · 1.01 | 0.04 · 0.03 | 3 · 15 | ✓ |
| fold every layer into the base | 4.06 s, read 128.7 MB, wrote 120.8 MB | 3.68 s, read 128.7 MB, wrote 120.7 MB | 1.10 · 1.15 · 1.00 | 3.52 · 2.29 | 90 · 296 | ✓ |

Stored: 128.7 MB (10.70 B/entry), metadata 0.20 MB, 10 layers; T44 ours 128.7 MB (10.69 B/entry), metadata 0.15 MB.
Process peak RSS (workload, oracle and bench included): 723 MB.

#### rewrite, 10M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.16 s · 4 · 1.0 MB | 0.14 s · 4 · 1.0 MB | 1.12 · 1.00 · 1.00 | 0.01 · 0.01 | 2 · 5 | ✓ |
| diff, 1 day, first 10K | 0.22 s · 17 · 7.2 MB | 0.29 s · 17 · 7.2 MB | 0.76 · 1.00 · 1.00 | 0.04 · 0.04 | 13 · 29 | ✓ |
| diff, 1 day, all | 0.98 s · 27 · 26.1 MB | 1.15 s · 27 · 26.1 MB | 0.85 · 1.00 · 1.00 | 0.40 · 0.32 | 30 · 74 | ✓ |
| diff, 1 day, 1% of keys | 0.00 s · 0 · 0.0 MB | 0.00 s · 0 · 0.0 MB | – · – · – | 0.00 · 0.00 | 0 · 0 | ✓ |
| diff, paused 30 days, first 10K | 0.08 s · 16 · 2.4 MB | 0.08 s · 19 · 3.9 MB | 0.99 · 0.84 · 0.61 | 0.00 · 0.01 | 10 · 19 | ✓ |
| diff, paused 30 days, all | 3.27 s · 51 · 112.7 MB | 3.69 s · 51 · 112.7 MB | 0.89 · 1.00 · 1.00 | 1.91 · 1.29 | 71 · 148 | ✓ |
| diff, paused 30 days, 1% of keys | 0.14 s · 6 · 1.3 MB | 0.15 s · 6 · 1.2 MB | 0.97 · 1.00 · 1.02 | 0.02 · 0.01 | 2 · 5 | ✓ |
| diff across the rewrite, first 10K | 0.07 s · 16 · 2.4 MB | 0.08 s · 20 · 4.4 MB | 0.95 · 0.80 · 0.53 | 0.00 · 0.01 | 10 · 19 | ✓ |
| diff across the rewrite, all | 3.03 s · 51 · 112.7 MB | 3.60 s · 51 · 112.7 MB | 0.84 · 1.00 · 1.00 | 1.76 · 1.27 | 71 · 152 | ✓ |
| scan at head, all | 4.06 s · 65 · 233.7 MB | 4.73 s · 68 · 233.5 MB | 0.86 · 0.96 · 1.00 | 3.04 · 2.53 | 113 · 364 | ✓ |
| scan at head, 1% of keys | 0.15 s · 10 · 2.6 MB | 0.17 s · 10 · 2.6 MB | 0.92 · 1.00 · 1.03 | 0.03 · 0.02 | 5 · 8 | ✓ |
| scan a day ago, 1% of keys | 0.16 s · 10 · 2.6 MB | 0.18 s · 10 · 2.6 MB | 0.88 · 1.00 · 1.03 | 0.03 · 0.04 | 5 · 8 | ✓ |
| 30 scattered gets | 0.11 s · 102 · 1.8 MB | – | – | – | 5 · – | ✓ |
| 100 scattered gets | 0.22 s · 285 · 5.4 MB | – | – | – | 8 · – | ✓ |
| 1K scattered gets | 0.75 s · 1070 · 56.6 MB | 0.99 s · 1070 · 56.5 MB | 0.75 · 1.00 · 1.00 | 0.18 · 0.37 | 47 · 246 | ✓ |
| 10K scattered gets (a batch at the scale target) | 1.69 s · 46 · 231.7 MB | – | – | – | 151 · – | ✓ |
| merge the 4 newest layers | 0.60 s, read 10.0 MB, wrote 10.0 MB | 0.71 s, read 10.0 MB, wrote 10.0 MB | 0.85 · 1.20 · 1.00 | 0.35 · 0.27 | 16 · 21 | ✓ |
| fold every layer into the base | 4.51 s, read 233.7 MB, wrote 61.3 MB | 5.18 s, read 233.5 MB, wrote 61.3 MB | 0.87 · 1.09 · 1.00 | 4.09 · 3.08 | 148 · 364 | ✓ |

Stored: 233.7 MB (11.27 B/entry), metadata 0.38 MB, 9 layers; T44 ours 233.5 MB (11.26 B/entry), metadata 0.27 MB.
Process peak RSS (workload, oracle and bench included): 1,138 MB.

## 100M keys

#### daily-clustered, 100M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.11 s · 3 · 0.5 MB | 0.10 s · 3 · 0.5 MB | 1.03 · 1.00 · 1.00 | 0.01 · 0.00 | 1 · 2 | ✓ |
| diff, 1 day, first 10K | 0.07 s · 12 · 1.6 MB | 0.08 s · 12 · 1.6 MB | 0.96 · 1.00 · 1.01 | 0.00 · 0.01 | 5 · 14 | ✓ |
| diff, 1 day, all | 0.62 s · 26 · 15.4 MB | 0.70 s · 26 · 15.3 MB | 0.88 · 1.00 · 1.00 | 0.22 · 0.16 | 19 · 29 | ✓ |
| diff, 1 day, 1% of keys | 0.06 s · 8 · 0.1 MB | 0.06 s · 8 · 0.1 MB | 1.02 · 1.00 · 1.12 | 0.00 · 0.00 | 1 · 1 | ✓ |
| diff, paused 30 days, first 10K | 0.09 s · 24 · 3.8 MB | 0.11 s · 24 · 3.6 MB | 0.78 · 1.00 · 1.04 | 0.01 · 0.03 | 14 · 30 | ✓ |
| diff, paused 30 days, all | 5.67 s · 80 · 290.4 MB | 6.50 s · 85 · 290.2 MB | 0.87 · 0.94 · 1.00 | 5.04 · 3.82 | 141 · 589 | ✓ |
| diff, paused 30 days, 1% of keys | 0.46 s · 25 · 9.4 MB | 0.46 s · 25 · 9.2 MB | 1.01 · 1.00 · 1.02 | 0.12 · 0.11 | 17 · 38 | ✓ |
| scan at head, all | 23.09 s · 159 · 1498.5 MB | 26.04 s · 199 · 1497.8 MB | 0.89 · 0.80 · 1.00 | 18.23 · 16.78 | 192 · 841 | ✓ |
| scan at head, 1% of keys | 0.57 s · 33 · 21.7 MB | 0.73 s · 33 · 21.5 MB | 0.78 · 1.00 · 1.01 | 0.24 · 0.24 | 30 · 55 | ✓ |
| scan a day ago, 1% of keys | 0.56 s · 27 · 21.6 MB | 0.72 s · 27 · 21.4 MB | 0.78 · 1.00 · 1.01 | 0.24 · 0.23 | 29 · 54 | ✓ |
| 30 scattered gets | 0.17 s · 123 · 7.3 MB | – | – | – | 9 · – | ✓ |
| 100 scattered gets | 0.28 s · 230 · 11.7 MB | – | – | – | 13 · – | ✓ |
| 1K scattered gets | 0.87 s · 1257 · 36.9 MB | 1.03 s · 1257 · 36.2 MB | 0.84 · 1.00 · 1.02 | 0.17 · 0.32 | 39 · 203 | ✓ |
| 10K scattered gets (a batch at the scale target) | 3.58 s · 6731 · 316.0 MB | – | – | – | 68 · – | ✓ |
| merge the 4 newest layers | 0.32 s, read 5.8 MB, wrote 5.8 MB | 0.36 s, read 5.8 MB, wrote 5.8 MB | 0.88 · 1.27 · 1.00 | 0.19 · 0.12 | 14 · 25 | ✓ |
| fold every layer into the base | 29.13 s, read 1498.5 MB, wrote 1047.8 MB | 34.40 s, read 1497.8 MB, wrote 1047.4 MB | 0.85 · 0.86 · 1.00 | 28.59 · 25.36 | 237 · 842 | ✓ |

Stored: 1,498.5 MB (11.29 B/entry), metadata 2.33 MB, 13 layers; T44 ours 1,497.8 MB (11.28 B/entry), metadata 1.67 MB.
Process peak RSS (workload, oracle and bench included): 6,648 MB.

#### daily-scattered, 100M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.11 s · 3 · 0.7 MB | 0.11 s · 3 · 0.7 MB | 1.06 · 1.00 · 1.00 | 0.01 · 0.00 | 1 · 3 | ✓ |
| diff, 1 day, first 10K | 0.08 s · 12 · 1.7 MB | 0.09 s · 12 · 1.7 MB | 0.91 · 1.00 · 1.01 | 0.01 · 0.02 | 6 · 9 | ✓ |
| diff, 1 day, all | 0.89 s · 31 · 45.9 MB | 1.12 s · 31 · 45.9 MB | 0.79 · 1.00 · 1.00 | 0.52 · 0.39 | 49 · 103 | ✓ |
| diff, 1 day, 1% of keys | 0.10 s · 13 · 0.6 MB | 0.10 s · 13 · 0.6 MB | 0.99 · 1.00 · 1.03 | 0.01 · 0.01 | 2 · 3 | ✓ |
| diff, paused 30 days, first 10K | 0.09 s · 18 · 3.2 MB | 0.09 s · 18 · 3.0 MB | 0.99 · 1.00 · 1.05 | 0.01 · 0.01 | 12 · 14 | ✓ |
| diff, paused 30 days, all | 6.10 s · 77 · 506.0 MB | 7.95 s · 88 · 505.8 MB | 0.77 · 0.88 · 1.00 | 5.62 · 5.14 | 167 · 450 | ✓ |
| diff, paused 30 days, 1% of keys | 0.25 s · 25 · 5.9 MB | 0.26 s · 25 · 5.7 MB | 0.94 · 1.00 · 1.03 | 0.06 · 0.05 | 11 · 12 | ✓ |
| scan at head, all | 20.73 s · 157 · 1715.6 MB | 27.28 s · 202 · 1715.0 MB | 0.76 · 0.78 · 1.00 | 19.82 · 18.14 | 219 · 680 | ✓ |
| scan at head, 1% of keys | 0.47 s · 33 · 18.0 MB | 0.66 s · 33 · 17.9 MB | 0.72 · 1.00 · 1.01 | 0.23 · 0.18 | 26 · 42 | ✓ |
| scan a day ago, 1% of keys | 0.47 s · 23 · 17.8 MB | 0.64 s · 23 · 17.6 MB | 0.74 · 1.00 · 1.01 | 0.21 · 0.16 | 25 · 41 | ✓ |
| 30 scattered gets | 0.23 s · 247 · 8.8 MB | – | – | – | 10 · – | ✓ |
| 100 scattered gets | 0.50 s · 588 · 20.9 MB | – | – | – | 21 · – | ✓ |
| 1K scattered gets | 1.63 s · 2346 · 134.8 MB | 2.16 s · 2346 · 134.1 MB | 0.75 · 1.00 · 1.00 | 0.43 · 0.95 | 133 · 504 | ✓ |
| 10K scattered gets (a batch at the scale target) | 3.61 s · 6211 · 670.9 MB | – | – | – | 163 · – | ✓ |
| merge the 4 newest layers | 0.26 s, read 7.0 MB, wrote 7.0 MB | 0.29 s, read 6.9 MB, wrote 7.0 MB | 0.87 · 1.25 · 1.00 | 0.16 · 0.11 | 17 · 14 | ✓ |
| fold every layer into the base | 33.02 s, read 1715.6 MB, wrote 1282.6 MB | 37.33 s, read 1715.0 MB, wrote 1282.1 MB | 0.88 · 0.83 · 1.00 | 32.72 · 28.39 | 257 · 680 | ✓ |

Stored: 1,715.6 MB (12.95 B/entry), metadata 2.46 MB, 10 layers; T44 ours 1,714.9 MB (12.95 B/entry), metadata 1.81 MB.
Process peak RSS (workload, oracle and bench included): 6,627 MB.

#### hot, 100M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.10 s · 3 · 0.3 MB | 0.10 s · 3 · 0.3 MB | 0.98 · 1.00 · 1.00 | 0.00 · 0.00 | 1 · 2 | ✓ |
| diff, 1 day, first 10K | 0.08 s · 12 · 1.6 MB | 0.08 s · 12 · 1.6 MB | 0.96 · 1.00 · 1.00 | 0.02 · 0.01 | 5 · 18 | ✓ |
| diff, 1 day, all | 0.17 s · 22 · 4.9 MB | 0.20 s · 22 · 4.8 MB | 0.86 · 1.00 · 1.00 | 0.08 · 0.06 | 6 · 23 | ✓ |
| diff, 1 day, 1% of keys | 0.07 s · 12 · 0.1 MB | 0.06 s · 12 · 0.1 MB | 1.03 · 1.00 · 1.00 | 0.00 · 0.00 | 1 · 1 | ✓ |
| diff, paused 30 days, first 10K | 0.45 s · 48 · 39.4 MB | 0.60 s · 49 · 41.4 MB | 0.74 · 0.98 · 0.95 | 0.23 · 0.31 | 57 · 182 | ✓ |
| diff, paused 30 days, all | 1.36 s · 61 · 83.4 MB | 1.80 s · 62 · 83.4 MB | 0.75 · 0.98 · 1.00 | 1.10 · 1.07 | 87 · 611 | ✓ |
| diff, paused 30 days, 1% of keys | 0.10 s · 25 · 1.1 MB | 0.11 s · 25 · 1.1 MB | 0.97 · 1.00 · 1.05 | 0.01 · 0.01 | 4 · 14 | ✓ |
| scan at head, all | 18.23 s · 140 · 1291.5 MB | 23.77 s · 176 · 1291.0 MB | 0.77 · 0.80 · 1.00 | 15.40 · 14.47 | 138 · 863 | ✓ |
| scan at head, 1% of keys | 0.46 s · 33 · 13.2 MB | 0.61 s · 33 · 13.2 MB | 0.75 · 1.00 · 1.00 | 0.15 · 0.15 | 20 · 40 | ✓ |
| scan a day ago, 1% of keys | 0.47 s · 23 · 13.1 MB | 0.58 s · 23 · 13.1 MB | 0.80 · 1.00 · 1.00 | 0.15 · 0.14 | 19 · 40 | ✓ |
| 30 scattered gets | 0.24 s · 210 · 9.6 MB | – | – | – | 9 · – | ✓ |
| 100 scattered gets | 0.39 s · 343 · 25.4 MB | – | – | – | 23 · – | ✓ |
| 1K scattered gets | 1.14 s · 1134 · 86.0 MB | 1.25 s · 1134 · 85.4 MB | 0.91 · 1.00 · 1.01 | 0.46 · 0.75 | 77 · 787 | ✓ |
| 10K scattered gets (a batch at the scale target) | 3.01 s · 5468 · 324.3 MB | – | – | – | 112 · – | ✓ |
| merge the 4 newest layers | 0.18 s, read 2.6 MB, wrote 1.7 MB | 0.17 s, read 2.6 MB, wrote 1.7 MB | 1.05 · 1.29 · 1.00 | 0.11 · 0.06 | 6 · 15 | ✓ |
| fold every layer into the base | 26.81 s, read 1291.5 MB, wrote 1208.4 MB | 34.06 s, read 1291.0 MB, wrote 1207.9 MB | 0.79 · 0.87 · 1.00 | 26.43 · 24.93 | 185 · 863 | ✓ |

Stored: 1,291.5 MB (10.73 B/entry), metadata 1.99 MB, 13 layers; T44 ours 1,291.0 MB (10.73 B/entry), metadata 1.45 MB.
Process peak RSS (workload, oracle and bench included): 6,149 MB.

#### rewrite, 100M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.39 s · 7 · 10.0 MB | 0.46 s · 7 · 10.0 MB | 0.85 · 1.00 · 1.00 | 0.12 · 0.08 | 13 · 36 | ✓ |
| diff, 1 day, first 10K | 2.12 s · 25 · 121.0 MB | 2.79 s · 30 · 137.7 MB | 0.76 · 0.83 · 0.88 | 1.41 · 1.41 | 51 · 300 | ✓ |
| diff, 1 day, all | 6.39 s · 53 · 351.2 MB | 7.72 s · 62 · 351.1 MB | 0.83 · 0.85 · 1.00 | 4.76 · 3.67 | 62 · 300 | ✓ |
| diff, 1 day, 1% of keys | 0.00 s · 0 · 0.0 MB | 0.00 s · 0 · 0.0 MB | – · – · – | 0.00 · 0.00 | 0 · 0 | ✓ |
| diff, paused 30 days, first 10K | 0.09 s · 16 · 4.2 MB | 0.09 s · 16 · 3.6 MB | 0.97 · 1.00 · 1.16 | 0.00 · 0.02 | 19 · 22 | ✓ |
| diff, paused 30 days, all | 18.89 s · 118 · 1127.8 MB | 22.62 s · 149 · 1127.2 MB | 0.84 · 0.79 · 1.00 | 15.65 · 12.43 | 118 · 491 | ✓ |
| diff, paused 30 days, 1% of keys | 0.41 s · 11 · 12.2 MB | 0.51 s · 11 · 11.9 MB | 0.80 · 1.00 · 1.02 | 0.15 · 0.10 | 18 · 38 | ✓ |
| diff across the rewrite, first 10K | 0.09 s · 16 · 4.2 MB | 0.09 s · 16 · 3.6 MB | 0.95 · 1.00 · 1.16 | 0.00 · 0.02 | 19 · 22 | ✓ |
| diff across the rewrite, all | 19.20 s · 118 · 1127.8 MB | 22.64 s · 149 · 1127.2 MB | 0.85 · 0.79 · 1.00 | 17.00 · 12.45 | 118 · 495 | ✓ |
| scan at head, all | 31.24 s · 198 · 2337.4 MB | 31.74 s · 263 · 2336.3 MB | 0.98 · 0.75 · 1.00 | 28.03 · 24.08 | 170 · 706 | ✓ |
| scan at head, 1% of keys | 0.55 s · 19 · 24.3 MB | 0.65 s · 19 · 24.0 MB | 0.84 · 1.00 · 1.01 | 0.34 · 0.19 | 32 · 60 | ✓ |
| scan a day ago, 1% of keys | 0.56 s · 19 · 24.3 MB | 0.66 s · 19 · 24.0 MB | 0.85 · 1.00 · 1.01 | 0.32 · 0.18 | 32 · 60 | ✓ |
| 30 scattered gets | 0.15 s · 108 · 5.0 MB | – | – | – | 11 · – | ✓ |
| 100 scattered gets | 0.28 s · 307 · 8.2 MB | – | – | – | 15 · – | ✓ |
| 1K scattered gets | 1.48 s · 2360 · 59.9 MB | 1.69 s · 2360 · 58.8 MB | 0.87 · 1.00 · 1.02 | 0.23 · 0.45 | 61 · 288 | ✓ |
| 10K scattered gets (a batch at the scale target) | 5.39 s · 10202 · 576.2 MB | – | – | – | 118 · – | ✓ |
| merge the 4 newest layers | 2.44 s, read 70.2 MB, wrote 70.2 MB | 2.85 s, read 70.2 MB, wrote 70.2 MB | 0.86 · 1.10 · 1.00 | 2.00 · 1.35 | 85 · 148 | ✓ |
| fold every layer into the base | 40.38 s, read 2337.4 MB, wrote 612.8 MB | 37.91 s, read 2336.3 MB, wrote 612.3 MB | 1.07 · 0.79 · 1.00 | 39.70 · 30.51 | 214 · 707 | ✓ |

Stored: 2,337.4 MB (11.27 B/entry), metadata 3.76 MB, 9 layers; T44 ours 2,336.3 MB (11.27 B/entry), metadata 2.68 MB.
Process peak RSS (workload, oracle and bench included): 10,680 MB.
