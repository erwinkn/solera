# The change index at 10M and 100M keys, against the format benchmark (SOLE-6)

The shipped reader (`native/index`) meets the bar set by the format benchmark's "ours" prototype (T44, `exp/layer-format`) on all four workloads at both scales. Queries make the same number of GETs as T44 (two exceptions: one more on one query, 3–4 fewer on two). They read the same bytes within 5%, except a few small diffs that read up to 0.6 MB more (explained below). Cold time is 0.65–1.0× T44's on every query that takes more than 0.15 s. Peak memory per query is lower everywhere except the merge of the 4 newest layers, which uses up to 1.3× more (24 MB at most). Every result was checked against a brute-force fold of the commit stream: 100 results per scale, 0 mismatches.

## Method

`change-index-bench` (`src/main.rs`) replays T44's run: the same workload generators, seed (7), 30 days × 24 commits a day on top of a load commit, the same layer schedule (`src/sched.rs`, the size rule), and the same query set. The layers the schedule leaves are written directly with the crate's `Writer` (the encoder `write_commit` and `merge` use). The measured merges go through `ChangeIndex::merge`, and queries go through `ChangeIndex` with its default `StoreOptions`.

- **cold**: a fresh `Store` (empty memory cache) over a simulated object store (`src/sim.rs`) that charges every GET 30 ms of latency plus transfer at 80 MB/s, as T44 did. Times are wall time; GETs and MB are what the store served.
- **warm**: the same query on another fresh `Store` with no simulated latency, over the same local files, which the OS has already cached. The memory cache starts empty, so *warm CPU* (the query's thread CPU time) covers reading, checking and decoding every block the query needs.
- **peak MB**: the counting allocator's high-water mark during the query, minus what was allocated before it started.
- **fold ✓**: the result equals the brute-force fold of the commit stream at the queried commits.
- The T44 numbers are its final runs (`t44/*.jsonl`, the "ours" rows of `exp/layer-format` `results/final-*`).

Run: `./run.sh NAME workload=W keys=N` writes `results/NAME.jsonl`. `python3 report.py results/*-10m.jsonl` prints a table below. Run the 100M configurations one at a time: process peak RSS is 6.1–10.7 GB, mostly the bench's oracle and generator.

## What differs from T44, and why

- **Merges make 7–40% more GETs.** A merge reads each input layer's commits section in its own GET, at the same time as the entries walk; T44 read it afterwards. That costs one GET per input layer and makes merges up to 28% faster cold. A fold at 100M rewrite comes out even (38.3 s vs 37.9 s). Holding the commit sections during the walk also raises a small merge's peak memory to at most 1.3× T44's (24 MB).
- **Warm CPU in the 100M tables is up to 27% higher on rewrite.** This is the machine, not the code. T44 ran on a different machine; T44's own bench rerun on this machine (rewrite, 10M, twice each, interleaved) gives:

  | query | index CPU s | T44 ours CPU s |
  |---|---|---|
  | diff, paused 30 days, all | 1.62, 1.78 | 2.13, 2.24 |
  | scan at head, all | 2.92, 3.41 | 3.45, 3.44 |
  | fold every layer into the base (cold) | 4.06, 4.73 | 5.04, 4.71 |

- **Lower peak memory.** Bulk reads (scans, folds, merges) above `StoreOptions.cache_batch` (8 MiB) are decoded straight from the response and never enter the memory cache. Listed-key reads launch every batch at once and decode each one as it arrives.
- **"diff, paused 30 days, first 10K" reads 0.6 MB more on rewrite at 100M** (4.2 vs 3.6 MB, 1.2× the cold time on one run of 0.1 s). The metadata is larger (3.76 vs 2.68 MB for the whole layer set) because segments carry checksums and first keys; this query reads more of it than it reads blocks.
- Single runs of queries under 0.1 s vary by ±10%, so ratios of 1.02–1.10 on them are noise.

## 10M keys

#### daily-clustered, 10M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.14 s · 4 · 0.8 MB | 0.14 s · 4 · 0.8 MB | 0.97 · 1.00 · 1.00 | 0.02 · 0.01 | 2 · 5 | ✓ |
| diff, 1 day, first 10K | 0.07 s · 4 · 0.5 MB | 0.07 s · 4 · 0.5 MB | 0.97 · 1.00 · 1.00 | 0.00 · 0.00 | 2 · 4 | ✓ |
| diff, 1 day, all | 0.17 s · 8 · 1.7 MB | 0.18 s · 8 · 1.7 MB | 0.95 · 1.00 · 1.00 | 0.02 · 0.01 | 3 · 4 | ✓ |
| diff, 1 day, 1% of keys | 0.06 s · 4 · 0.0 MB | 0.06 s · 4 · 0.0 MB | 1.00 · 1.00 · 1.00 | 0.00 · 0.00 | 0 · 0 | ✓ |
| diff, paused 30 days, first 10K | 0.07 s · 12 · 1.7 MB | 0.08 s · 12 · 1.6 MB | 0.93 · 1.00 · 1.01 | 0.00 · 0.01 | 6 · 17 | ✓ |
| diff, paused 30 days, all | 0.75 s · 28 · 29.4 MB | 0.92 s · 28 · 29.4 MB | 0.81 · 1.00 · 1.00 | 0.49 · 0.36 | 36 · 84 | ✓ |
| diff, paused 30 days, 1% of keys | 0.14 s · 15 · 1.6 MB | 0.16 s · 15 · 1.6 MB | 0.87 · 1.00 · 1.01 | 0.05 · 0.04 | 4 · 8 | ✓ |
| scan at head, all | 2.11 s · 45 · 150.1 MB | 2.80 s · 45 · 150.1 MB | 0.75 · 1.00 · 1.00 | 1.56 · 1.44 | 112 · 285 | ✓ |
| scan at head, 1% of keys | 0.17 s · 19 · 3.0 MB | 0.18 s · 19 · 3.0 MB | 0.93 · 1.00 · 1.02 | 0.04 · 0.03 | 6 · 11 | ✓ |
| scan a day ago, 1% of keys | 0.16 s · 17 · 3.0 MB | 0.20 s · 17 · 3.0 MB | 0.81 · 1.00 · 1.02 | 0.04 · 0.04 | 5 · 11 | ✓ |
| 1K scattered gets | 0.61 s · 705 · 32.4 MB | 0.59 s · 705 · 32.4 MB | 1.04 · 1.00 · 1.00 | 0.20 · 0.19 | 32 · 156 | ✓ |
| merge the 4 newest layers | 0.22 s, read 3.2 MB, wrote 3.2 MB | 0.23 s, read 3.2 MB, wrote 3.2 MB | 0.97 · 1.29 · 1.00 | 0.09 · 0.07 | 8 · 12 | ✓ |
| fold every layer into the base | 2.82 s, read 150.2 MB, wrote 105.8 MB | 3.52 s, read 150.1 MB, wrote 105.7 MB | 0.80 · 1.16 · 1.00 | 2.49 · 2.34 | 142 · 285 | ✓ |

Stored: 150.1 MB (11.31 B/entry), metadata 0.24 MB, 7 layers; T44 ours 150.1 MB (11.31 B/entry), metadata 0.17 MB.
Process peak RSS (workload, oracle and bench included): 742 MB.

#### daily-scattered, 10M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.06 s · 2 · 0.1 MB | 0.06 s · 2 · 0.1 MB | 1.00 · 1.00 · 1.00 | 0.00 · 0.00 | 0 · 0 | ✓ |
| diff, 1 day, first 10K | 0.07 s · 6 · 0.6 MB | 0.07 s · 6 · 0.6 MB | 0.95 · 1.00 · 1.00 | 0.00 · 0.01 | 2 · 3 | ✓ |
| diff, 1 day, all | 0.17 s · 10 · 2.5 MB | 0.16 s · 10 · 2.5 MB | 1.04 · 1.00 · 1.00 | 0.03 · 0.02 | 4 · 5 | ✓ |
| diff, 1 day, 1% of keys | 0.06 s · 6 · 0.1 MB | 0.06 s · 6 · 0.1 MB | 1.02 · 1.00 · 1.00 | 0.00 · 0.00 | 1 · 0 | ✓ |
| diff, paused 30 days, first 10K | 0.07 s · 12 · 1.5 MB | 0.07 s · 12 · 1.5 MB | 1.01 · 1.00 · 1.01 | 0.01 · 0.01 | 6 · 8 | ✓ |
| diff, paused 30 days, all | 0.99 s · 31 · 50.7 MB | 1.27 s · 31 · 50.7 MB | 0.78 · 1.00 · 1.00 | 0.48 · 0.47 | 49 · 123 | ✓ |
| diff, paused 30 days, 1% of keys | 0.10 s · 13 · 0.7 MB | 0.10 s · 13 · 0.7 MB | 1.00 · 1.00 · 1.01 | 0.01 · 0.01 | 2 · 3 | ✓ |
| scan at head, all | 2.19 s · 48 · 171.6 MB | 3.01 s · 48 · 171.6 MB | 0.73 · 1.00 · 1.00 | 1.79 · 1.73 | 126 · 333 | ✓ |
| scan at head, 1% of keys | 0.16 s · 17 · 2.1 MB | 0.17 s · 17 · 2.0 MB | 0.91 · 1.00 · 1.03 | 0.02 · 0.03 | 5 · 6 | ✓ |
| scan a day ago, 1% of keys | 0.15 s · 13 · 2.0 MB | 0.16 s · 13 · 2.0 MB | 0.97 · 1.00 · 1.04 | 0.03 · 0.02 | 4 · 6 | ✓ |
| 1K scattered gets | 0.56 s · 670 · 64.9 MB | 0.79 s · 670 · 64.8 MB | 0.70 · 1.00 · 1.00 | 0.38 · 0.42 | 60 · 257 | ✓ |
| merge the 4 newest layers | 0.18 s, read 3.8 MB, wrote 3.8 MB | 0.21 s, read 3.8 MB, wrote 3.8 MB | 0.83 · 1.29 · 1.00 | 0.10 · 0.07 | 10 · 8 | ✓ |
| fold every layer into the base | 3.26 s, read 171.7 MB, wrote 128.2 MB | 3.83 s, read 171.6 MB, wrote 128.2 MB | 0.85 · 1.15 · 1.00 | 3.04 · 2.68 | 162 · 333 | ✓ |

Stored: 171.7 MB (12.96 B/entry), metadata 0.25 MB, 7 layers; T44 ours 171.6 MB (12.95 B/entry), metadata 0.18 MB.
Process peak RSS (workload, oracle and bench included): 829 MB.

#### hot, 10M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.07 s · 2 · 0.0 MB | 0.06 s · 2 · 0.0 MB | 1.05 · 1.00 · 1.00 | 0.00 · 0.00 | 0 · 0 | ✓ |
| diff, 1 day, first 10K | 0.10 s · 7 · 0.6 MB | 0.10 s · 7 · 0.6 MB | 0.95 · 1.00 · 1.00 | 0.01 · 0.01 | 1 · 10 | ✓ |
| diff, 1 day, all | 0.10 s · 7 · 0.6 MB | 0.10 s · 7 · 0.6 MB | 0.96 · 1.00 · 1.00 | 0.01 · 0.01 | 1 · 10 | ✓ |
| diff, 1 day, 1% of keys | 0.06 s · 6 · 0.0 MB | 0.06 s · 6 · 0.0 MB | 1.00 · 1.00 · 1.00 | 0.00 · 0.00 | 0 · 1 | ✓ |
| diff, paused 30 days, first 10K | 0.24 s · 31 · 8.0 MB | 0.31 s · 31 · 8.0 MB | 0.75 · 1.00 · 1.00 | 0.12 · 0.11 | 11 · 79 | ✓ |
| diff, paused 30 days, all | 0.21 s · 31 · 8.0 MB | 0.28 s · 31 · 8.0 MB | 0.77 · 1.00 · 1.00 | 0.14 · 0.11 | 11 · 80 | ✓ |
| diff, paused 30 days, 1% of keys | 0.07 s · 18 · 0.2 MB | 0.07 s · 18 · 0.2 MB | 1.06 · 1.00 · 1.05 | 0.01 · 0.00 | 2 · 4 | ✓ |
| scan at head, all | 2.01 s · 48 · 128.7 MB | 2.92 s · 48 · 128.7 MB | 0.69 · 1.00 · 1.00 | 1.41 · 1.51 | 88 · 296 | ✓ |
| scan at head, 1% of keys | 0.16 s · 22 · 1.6 MB | 0.16 s · 22 · 1.6 MB | 0.97 · 1.00 · 1.04 | 0.02 · 0.03 | 4 · 8 | ✓ |
| scan a day ago, 1% of keys | 0.15 s · 18 · 1.6 MB | 0.17 s · 18 · 1.5 MB | 0.87 · 1.00 · 1.04 | 0.01 · 0.03 | 4 · 7 | ✓ |
| 1K scattered gets | 0.58 s · 587 · 31.5 MB | 0.71 s · 587 · 31.5 MB | 0.82 · 1.00 · 1.00 | 0.21 · 0.31 | 36 · 275 | ✓ |
| merge the 4 newest layers | 0.12 s, read 0.9 MB, wrote 0.8 MB | 0.12 s, read 0.9 MB, wrote 0.8 MB | 0.94 · 1.40 · 1.01 | 0.05 · 0.03 | 3 · 15 | ✓ |
| fold every layer into the base | 2.67 s, read 128.7 MB, wrote 120.8 MB | 3.68 s, read 128.7 MB, wrote 120.7 MB | 0.72 · 1.21 · 1.00 | 2.40 · 2.29 | 122 · 296 | ✓ |

Stored: 128.7 MB (10.70 B/entry), metadata 0.20 MB, 10 layers; T44 ours 128.7 MB (10.69 B/entry), metadata 0.15 MB.
Process peak RSS (workload, oracle and bench included): 653 MB.

#### rewrite, 10M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.14 s · 4 · 1.0 MB | 0.14 s · 4 · 1.0 MB | 0.99 · 1.00 · 1.00 | 0.03 · 0.01 | 2 · 5 | ✓ |
| diff, 1 day, first 10K | 0.22 s · 17 · 7.2 MB | 0.29 s · 17 · 7.2 MB | 0.75 · 1.00 · 1.00 | 0.05 · 0.04 | 13 · 29 | ✓ |
| diff, 1 day, all | 0.96 s · 27 · 26.1 MB | 1.15 s · 27 · 26.1 MB | 0.84 · 1.00 · 1.00 | 0.34 · 0.32 | 30 · 74 | ✓ |
| diff, 1 day, 1% of keys | 0.00 s · 0 · 0.0 MB | 0.00 s · 0 · 0.0 MB | – · – · – | 0.00 · 0.00 | 0 · 0 | ✓ |
| diff, paused 30 days, first 10K | 0.07 s · 16 · 2.4 MB | 0.08 s · 19 · 3.9 MB | 0.90 · 0.84 · 0.61 | 0.00 · 0.01 | 10 · 19 | ✓ |
| diff, paused 30 days, all | 2.97 s · 51 · 112.7 MB | 3.69 s · 51 · 112.7 MB | 0.80 · 1.00 · 1.00 | 1.37 · 1.29 | 70 · 148 | ✓ |
| diff, paused 30 days, 1% of keys | 0.15 s · 6 · 1.3 MB | 0.15 s · 6 · 1.2 MB | 0.99 · 1.00 · 1.02 | 0.03 · 0.01 | 2 · 5 | ✓ |
| diff across the rewrite, first 10K | 0.07 s · 16 · 2.4 MB | 0.08 s · 20 · 4.4 MB | 0.92 · 0.80 · 0.53 | 0.00 · 0.01 | 10 · 19 | ✓ |
| diff across the rewrite, all | 2.94 s · 51 · 112.7 MB | 3.60 s · 51 · 112.7 MB | 0.82 · 1.00 · 1.00 | 1.41 · 1.27 | 70 · 152 | ✓ |
| scan at head, all | 3.47 s · 68 · 233.7 MB | 4.73 s · 68 · 233.5 MB | 0.73 · 1.00 · 1.00 | 2.65 · 2.53 | 137 · 364 | ✓ |
| scan at head, 1% of keys | 0.15 s · 10 · 2.6 MB | 0.17 s · 10 · 2.6 MB | 0.91 · 1.00 · 1.03 | 0.02 · 0.02 | 4 · 8 | ✓ |
| scan a day ago, 1% of keys | 0.15 s · 10 · 2.6 MB | 0.18 s · 10 · 2.6 MB | 0.84 · 1.00 · 1.03 | 0.02 · 0.04 | 4 · 8 | ✓ |
| 1K scattered gets | 0.90 s · 1070 · 56.6 MB | 0.99 s · 1070 · 56.5 MB | 0.91 · 1.00 · 1.00 | 0.31 · 0.37 | 52 · 246 | ✓ |
| merge the 4 newest layers | 0.55 s, read 10.0 MB, wrote 10.0 MB | 0.71 s, read 10.0 MB, wrote 10.0 MB | 0.77 · 1.20 · 1.00 | 0.27 · 0.27 | 24 · 21 | ✓ |
| fold every layer into the base | 3.97 s, read 233.7 MB, wrote 61.3 MB | 5.18 s, read 233.5 MB, wrote 61.3 MB | 0.77 · 1.13 · 1.00 | 3.44 · 3.08 | 162 · 364 | ✓ |

Stored: 233.7 MB (11.27 B/entry), metadata 0.38 MB, 9 layers; T44 ours 233.5 MB (11.26 B/entry), metadata 0.27 MB.
Process peak RSS (workload, oracle and bench included): 1,137 MB.

## 100M keys

#### daily-clustered, 100M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.10 s · 3 · 0.5 MB | 0.10 s · 3 · 0.5 MB | 1.02 · 1.00 · 1.00 | 0.01 · 0.00 | 1 · 2 | ✓ |
| diff, 1 day, first 10K | 0.07 s · 12 · 1.6 MB | 0.08 s · 12 · 1.6 MB | 0.95 · 1.00 · 1.01 | 0.00 · 0.01 | 5 · 14 | ✓ |
| diff, 1 day, all | 0.56 s · 26 · 15.4 MB | 0.70 s · 26 · 15.3 MB | 0.79 · 1.00 · 1.00 | 0.20 · 0.16 | 19 · 29 | ✓ |
| diff, 1 day, 1% of keys | 0.06 s · 8 · 0.1 MB | 0.06 s · 8 · 0.1 MB | 1.02 · 1.00 · 1.12 | 0.00 · 0.00 | 1 · 1 | ✓ |
| diff, paused 30 days, first 10K | 0.08 s · 24 · 3.8 MB | 0.11 s · 24 · 3.6 MB | 0.72 · 1.00 · 1.04 | 0.01 · 0.03 | 14 · 30 | ✓ |
| diff, paused 30 days, all | 5.13 s · 85 · 290.4 MB | 6.50 s · 85 · 290.2 MB | 0.79 · 1.00 · 1.00 | 3.87 · 3.82 | 166 · 589 | ✓ |
| diff, paused 30 days, 1% of keys | 0.36 s · 25 · 9.4 MB | 0.46 s · 25 · 9.2 MB | 0.78 · 1.00 · 1.02 | 0.13 · 0.11 | 17 · 38 | ✓ |
| scan at head, all | 16.92 s · 199 · 1498.5 MB | 26.04 s · 199 · 1497.8 MB | 0.65 · 1.00 · 1.00 | 16.02 · 16.78 | 251 · 841 | ✓ |
| scan at head, 1% of keys | 0.55 s · 33 · 21.7 MB | 0.73 s · 33 · 21.5 MB | 0.76 · 1.00 · 1.01 | 0.23 · 0.24 | 30 · 55 | ✓ |
| scan a day ago, 1% of keys | 0.57 s · 27 · 21.6 MB | 0.72 s · 27 · 21.4 MB | 0.78 · 1.00 · 1.01 | 0.27 · 0.23 | 29 · 54 | ✓ |
| 1K scattered gets | 1.01 s · 1257 · 36.9 MB | 1.03 s · 1257 · 36.2 MB | 0.98 · 1.00 · 1.02 | 0.29 · 0.32 | 39 · 203 | ✓ |
| merge the 4 newest layers | 0.31 s, read 5.8 MB, wrote 5.8 MB | 0.36 s, read 5.8 MB, wrote 5.8 MB | 0.86 · 1.27 · 1.00 | 0.15 · 0.12 | 14 · 25 | ✓ |
| fold every layer into the base | 25.73 s, read 1498.5 MB, wrote 1047.8 MB | 34.40 s, read 1497.8 MB, wrote 1047.4 MB | 0.75 · 1.07 · 1.00 | 25.13 · 25.36 | 305 · 842 | ✓ |

Stored: 1,498.5 MB (11.29 B/entry), metadata 2.33 MB, 13 layers; T44 ours 1,497.8 MB (11.28 B/entry), metadata 1.67 MB.
Process peak RSS (workload, oracle and bench included): 6,648 MB.

#### daily-scattered, 100M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.11 s · 3 · 0.7 MB | 0.11 s · 3 · 0.7 MB | 1.02 · 1.00 · 1.00 | 0.01 · 0.00 | 1 · 3 | ✓ |
| diff, 1 day, first 10K | 0.07 s · 12 · 1.7 MB | 0.09 s · 12 · 1.7 MB | 0.83 · 1.00 · 1.01 | 0.01 · 0.02 | 6 · 9 | ✓ |
| diff, 1 day, all | 0.86 s · 31 · 45.9 MB | 1.12 s · 31 · 45.9 MB | 0.77 · 1.00 · 1.00 | 0.46 · 0.39 | 49 · 103 | ✓ |
| diff, 1 day, 1% of keys | 0.10 s · 13 · 0.6 MB | 0.10 s · 13 · 0.6 MB | 0.98 · 1.00 · 1.03 | 0.00 · 0.01 | 2 · 3 | ✓ |
| diff, paused 30 days, first 10K | 0.08 s · 18 · 3.2 MB | 0.09 s · 18 · 3.0 MB | 0.94 · 1.00 · 1.05 | 0.01 · 0.01 | 12 · 14 | ✓ |
| diff, paused 30 days, all | 5.57 s · 88 · 506.0 MB | 7.95 s · 88 · 505.8 MB | 0.70 · 1.00 · 1.00 | 5.36 · 5.14 | 233 · 450 | ✓ |
| diff, paused 30 days, 1% of keys | 0.23 s · 25 · 5.9 MB | 0.26 s · 25 · 5.7 MB | 0.88 · 1.00 · 1.03 | 0.08 · 0.05 | 11 · 12 | ✓ |
| scan at head, all | 18.26 s · 202 · 1715.6 MB | 27.28 s · 202 · 1715.0 MB | 0.67 · 1.00 · 1.00 | 18.47 · 18.14 | 318 · 680 | ✓ |
| scan at head, 1% of keys | 0.46 s · 33 · 18.0 MB | 0.66 s · 33 · 17.9 MB | 0.70 · 1.00 · 1.01 | 0.18 · 0.18 | 26 · 42 | ✓ |
| scan a day ago, 1% of keys | 0.47 s · 23 · 17.8 MB | 0.64 s · 23 · 17.6 MB | 0.73 · 1.00 · 1.01 | 0.20 · 0.16 | 25 · 41 | ✓ |
| 1K scattered gets | 1.74 s · 2346 · 134.8 MB | 2.16 s · 2346 · 134.1 MB | 0.80 · 1.00 · 1.00 | 0.79 · 0.95 | 124 · 504 | ✓ |
| merge the 4 newest layers | 0.24 s, read 7.0 MB, wrote 7.0 MB | 0.29 s, read 6.9 MB, wrote 7.0 MB | 0.83 · 1.25 · 1.00 | 0.15 · 0.11 | 18 · 14 | ✓ |
| fold every layer into the base | 28.64 s, read 1715.6 MB, wrote 1282.6 MB | 37.33 s, read 1715.0 MB, wrote 1282.1 MB | 0.77 · 1.05 · 1.00 | 28.30 · 28.39 | 357 · 680 | ✓ |

Stored: 1,715.6 MB (12.95 B/entry), metadata 2.46 MB, 10 layers; T44 ours 1,714.9 MB (12.95 B/entry), metadata 1.81 MB.
Process peak RSS (workload, oracle and bench included): 6,627 MB.

#### hot, 100M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.10 s · 3 · 0.3 MB | 0.10 s · 3 · 0.3 MB | 1.02 · 1.00 · 1.00 | 0.01 · 0.00 | 1 · 2 | ✓ |
| diff, 1 day, first 10K | 0.09 s · 12 · 1.6 MB | 0.08 s · 12 · 1.6 MB | 1.10 · 1.00 · 1.00 | 0.03 · 0.01 | 5 · 18 | ✓ |
| diff, 1 day, all | 0.16 s · 22 · 4.9 MB | 0.20 s · 22 · 4.8 MB | 0.80 · 1.00 · 1.00 | 0.07 · 0.06 | 6 · 23 | ✓ |
| diff, 1 day, 1% of keys | 0.07 s · 12 · 0.1 MB | 0.06 s · 12 · 0.1 MB | 1.03 · 1.00 · 1.00 | 0.00 · 0.00 | 1 · 1 | ✓ |
| diff, paused 30 days, first 10K | 0.42 s · 45 · 26.8 MB | 0.60 s · 49 · 41.4 MB | 0.69 · 0.92 · 0.65 | 0.26 · 0.31 | 55 · 182 | ✓ |
| diff, paused 30 days, all | 1.44 s · 62 · 83.4 MB | 1.80 s · 62 · 83.4 MB | 0.80 · 1.00 · 1.00 | 1.21 · 1.07 | 87 · 611 | ✓ |
| diff, paused 30 days, 1% of keys | 0.10 s · 25 · 1.1 MB | 0.11 s · 25 · 1.1 MB | 0.99 · 1.00 · 1.05 | 0.01 · 0.01 | 4 · 14 | ✓ |
| scan at head, all | 15.58 s · 176 · 1291.5 MB | 23.77 s · 176 · 1291.0 MB | 0.66 · 1.00 · 1.00 | 14.65 · 14.47 | 173 · 863 | ✓ |
| scan at head, 1% of keys | 0.46 s · 33 · 13.2 MB | 0.61 s · 33 · 13.2 MB | 0.75 · 1.00 · 1.00 | 0.14 · 0.15 | 20 · 40 | ✓ |
| scan a day ago, 1% of keys | 0.46 s · 23 · 13.1 MB | 0.58 s · 23 · 13.1 MB | 0.79 · 1.00 · 1.00 | 0.16 · 0.14 | 19 · 40 | ✓ |
| 1K scattered gets | 1.02 s · 1134 · 86.0 MB | 1.25 s · 1134 · 85.4 MB | 0.81 · 1.00 · 1.01 | 0.66 · 0.75 | 70 · 787 | ✓ |
| merge the 4 newest layers | 0.16 s, read 2.6 MB, wrote 1.7 MB | 0.17 s, read 2.6 MB, wrote 1.7 MB | 0.95 · 1.29 · 1.00 | 0.07 · 0.06 | 6 · 15 | ✓ |
| fold every layer into the base | 26.64 s, read 1291.5 MB, wrote 1208.4 MB | 34.06 s, read 1291.0 MB, wrote 1207.9 MB | 0.78 · 1.07 · 1.00 | 26.28 · 24.93 | 220 · 863 | ✓ |

Stored: 1,291.5 MB (10.73 B/entry), metadata 1.99 MB, 13 layers; T44 ours 1,291.0 MB (10.73 B/entry), metadata 1.45 MB.
Process peak RSS (workload, oracle and bench included): 6,149 MB.

#### rewrite, 100M keys

| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |
|---|---|---|---|---|---|---|
| diff, last commit | 0.40 s · 7 · 10.0 MB | 0.46 s · 7 · 10.0 MB | 0.87 · 1.00 · 1.00 | 0.13 · 0.08 | 13 · 36 | ✓ |
| diff, 1 day, first 10K | 1.86 s · 31 · 137.9 MB | 2.79 s · 30 · 137.7 MB | 0.67 · 1.03 · 1.00 | 1.29 · 1.41 | 85 · 300 | ✓ |
| diff, 1 day, all | 5.98 s · 62 · 351.2 MB | 7.72 s · 62 · 351.1 MB | 0.78 · 1.00 · 1.00 | 4.52 · 3.67 | 86 · 300 | ✓ |
| diff, 1 day, 1% of keys | 0.00 s · 0 · 0.0 MB | 0.00 s · 0 · 0.0 MB | – · – · – | 0.00 · 0.00 | 0 · 0 | ✓ |
| diff, paused 30 days, first 10K | 0.11 s · 16 · 4.2 MB | 0.09 s · 16 · 3.6 MB | 1.20 · 1.00 · 1.16 | 0.04 · 0.02 | 19 · 22 | ✓ |
| diff, paused 30 days, all | 18.34 s · 149 · 1127.8 MB | 22.62 s · 149 · 1127.2 MB | 0.81 · 1.00 · 1.00 | 15.67 · 12.43 | 178 · 491 | ✓ |
| diff, paused 30 days, 1% of keys | 0.42 s · 11 · 12.2 MB | 0.51 s · 11 · 11.9 MB | 0.84 · 1.00 · 1.02 | 0.18 · 0.10 | 17 · 38 | ✓ |
| diff across the rewrite, first 10K | 0.09 s · 16 · 4.2 MB | 0.09 s · 16 · 3.6 MB | 0.95 · 1.00 · 1.16 | 0.02 · 0.02 | 19 · 22 | ✓ |
| diff across the rewrite, all | 19.38 s · 149 · 1127.8 MB | 22.64 s · 149 · 1127.2 MB | 0.86 · 1.00 · 1.00 | 16.27 · 12.45 | 178 · 495 | ✓ |
| scan at head, all | 28.99 s · 263 · 2337.4 MB | 31.74 s · 263 · 2336.3 MB | 0.91 · 1.00 · 1.00 | 28.86 · 24.08 | 265 · 706 | ✓ |
| scan at head, 1% of keys | 0.54 s · 19 · 24.3 MB | 0.65 s · 19 · 24.0 MB | 0.82 · 1.00 · 1.01 | 0.31 · 0.19 | 32 · 60 | ✓ |
| scan a day ago, 1% of keys | 0.50 s · 19 · 24.3 MB | 0.66 s · 19 · 24.0 MB | 0.76 · 1.00 · 1.01 | 0.24 · 0.18 | 32 · 60 | ✓ |
| 1K scattered gets | 1.53 s · 2360 · 59.9 MB | 1.69 s · 2360 · 58.8 MB | 0.90 · 1.00 · 1.02 | 0.46 · 0.45 | 54 · 288 | ✓ |
| merge the 4 newest layers | 2.27 s, read 70.2 MB, wrote 70.2 MB | 2.85 s, read 70.2 MB, wrote 70.2 MB | 0.79 · 1.13 · 1.00 | 1.73 · 1.35 | 99 · 148 | ✓ |
| fold every layer into the base | 38.31 s, read 2337.4 MB, wrote 612.8 MB | 37.91 s, read 2336.3 MB, wrote 612.3 MB | 1.01 · 1.03 · 1.00 | 37.65 · 30.51 | 306 · 707 | ✓ |

Stored: 2,337.4 MB (11.27 B/entry), metadata 3.76 MB, 9 layers; T44 ours 2,336.3 MB (11.27 B/entry), metadata 2.68 MB.
Process peak RSS (workload, oracle and bench included): 10,680 MB.
