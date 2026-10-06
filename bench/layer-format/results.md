# T44: the layer file format for MVCC entries — our blocks vs Parquet

Spike on `exp/layer-format` (never merged). The target design's change index
(the brief's §4, decision D4) stores MVCC entries — key, commit, new version,
replaced version — sorted by key then commit, newest first, in immutable
layers under the 4× size rule. Fable proposed Parquet layers, so that DuckDB
reads them directly; D183 asks for the format to be decided by measurement
against our own block format. Everything below comes from
`bench/layer-format/all.sh`, and every one of the 1,264 measurements matched
a brute-force fold of the commit stream.

## Recommendation

**Parquet**, read through our own planner (as in this bench, not arrow-rs's
stock async reader), with 128K-row groups, 32 KiB / 4,096-row pages, zstd-1,
the key column DELTA_BYTE_ARRAY (prefix-coded) and the integer columns
DELTA_BINARY_PACKED.

Performance does not decide it: on the index's own reads the two formats sit
within about ±10% of each other, each winning where its layout helps. What
decides it is everything around the reads:

- **For Parquet.** DuckDB reads the layers as they are: history questions
  run in 2–160 ms (a first query 0.96 s), against 0.35–0.56 s for ours
  through an Arrow export, which first takes 1.5 s to decode the 10M-key
  layers, with no pushdown (table below). That is the brief's "nothing stored twice" (D3/D4) for free.
  Parquet also stores 10–12% less, and it is 5–12% faster on wide reads
  (full diffs, scans, merges, folds), where columnar integers decode faster.
- **Against Parquet.** 2–3× the GETs at 128K-row groups (each column's pages
  are separate ranges); 16K-row groups bring that down to ours (below) at
  1.45× the metadata. 1K scattered gets are 1.7× slower and read 2.4× the
  bytes: a point read fetches 4 columns' pages, ours one block. Peak memory
  is 1.1–2× ours. Its metadata is 5.5× ours (10 MB vs 1.8 MB per 100M-key
  output), and the engine keeps it warm to plan with. The worker's native
  module grows by 5.7 MB stripped, and its clean build by 34 s (arrow-rs is
  there already: +10 crates).
- **The reader is ours either way.** The Parquet numbers here need the
  planner this bench built on arrow-rs: footer first, then only the page
  indexes of the row groups a read touches; read units of a few pages, not
  whole row groups; a decode batch sized to the rows selected; a small
  coalescing gap. Each was worth 2–5× in bytes read or memory on some query
  (§ The Parquet reader's choices). `pq.rs` is 442 lines with its writer;
  `ours.rs`, the whole format, is 513.

If DuckDB over the layers were dropped from the design, I would recommend
ours instead: the same speed, a half to a third of the GETs, half the memory on point
reads, a fifth of the metadata, and no new dependency.

## The numbers at 100M keys

Cold: one reader with nothing cached, the simulated store's latency (30 ms
a request, 80 MB/s per connection, 64 in flight), cells `time · GETs · MB
read`. Warm: the same read from a local copy, CPU seconds. Peak: heap above
the reader's start (a counting allocator), MB.

#### daily-scattered

| query | ours, cold | Parquet, cold | Parquet / ours (time · GETs · MB) | warm CPU s, ours · Parquet | peak MB, ours · Parquet |
|---|---|---|---|---|---|
| diff, last commit | 0.11 s · 3 · 0.7 MB | 0.11 s · 4 · 0.7 MB | 1.03 · 1.3 · 1.03 | 0.00 · 0.01 | 3 · 3 |
| diff, 1 day, first 10K (one batch) | 0.09 s · 12 · 1.7 MB | 0.08 s · 38 · 4.9 MB | 0.89 · 3.2 · 2.87 | 0.02 · 0.01 | 9 · 12 |
| diff, 1 day, all | 1.12 s · 31 · 45.9 MB | 1.00 s · 90 · 45.2 MB | 0.89 · 2.9 · 0.98 | 0.39 · 0.35 | 103 · 104 |
| diff, 1 day, 1% of keys | 0.10 s · 13 · 0.6 MB | 0.08 s · 26 · 1.0 MB | 0.75 · 2.0 · 1.70 | 0.01 · 0.01 | 3 · 4 |
| diff, paused 30 days, first 10K | 0.09 s · 18 · 3.0 MB | 0.15 s · 44 · 6.5 MB | 1.72 · 2.4 · 2.14 | 0.01 · 0.03 | 14 · 19 |
| diff, paused 30 days, all | 7.95 s · 88 · 505.8 MB | 7.55 s · 253 · 474.2 MB | 0.95 · 2.9 · 0.94 | 5.14 · 4.43 | 450 · 538 |
| scan at head, 1% of keys | 0.66 s · 33 · 17.9 MB | 0.56 s · 103 · 17.7 MB | 0.85 · 3.1 · 0.99 | 0.18 · 0.17 | 42 · 62 |
| scan a day ago, 1% of keys | 0.64 s · 23 · 17.6 MB | 0.59 s · 82 · 17.2 MB | 0.93 · 3.6 · 0.98 | 0.16 · 0.15 | 41 · 60 |
| scan at head, all | 27.28 s · 202 · 1715.0 MB | 25.49 s · 475 · 1533.4 MB | 0.93 · 2.4 · 0.89 | 18.14 · 16.17 | 680 · 867 |
| 1K scattered gets | 2.16 s · 2346 · 134.1 MB | 3.69 s · 4070 · 319.4 MB | 1.70 · 1.7 · 2.38 | 0.95 · 1.46 | 504 · 989 |
| merge the 4 newest layers | 0.29 s · 16 · 6.9 MB | 0.30 s · 32 · 7.0 MB | 1.02 · 2.0 · 1.01 | 0.11 · 0.12 | 14 · 21 |
| fold every layer into the base | 37.33 s · 202 · 1715.0 MB | 36.00 s · 475 · 1533.4 MB | 0.96 · 2.4 · 0.89 | 28.39 · 27.41 | 680 · 870 |

Stored: ours 1,715 MB (12.95 B/entry), Parquet 1,529 MB (11.55 B/entry): 0.89×. Metadata: ours 1.8 MB, Parquet 10.1 MB. 10 layers; merges wrote 4.16 entries per entry committed.

#### daily-clustered

| query | ours, cold | Parquet, cold | Parquet / ours (time · GETs · MB) | warm CPU s, ours · Parquet | peak MB, ours · Parquet |
|---|---|---|---|---|---|
| diff, last commit | 0.10 s · 3 · 0.5 MB | 0.10 s · 5 · 0.4 MB | 1.01 · 1.7 · 0.93 | 0.00 · 0.01 | 2 · 4 |
| diff, 1 day, first 10K (one batch) | 0.08 s · 12 · 1.6 MB | 0.08 s · 27 · 2.8 MB | 1.06 · 2.2 · 1.72 | 0.01 · 0.02 | 14 · 21 |
| diff, 1 day, all | 0.70 s · 26 · 15.3 MB | 0.61 s · 47 · 14.2 MB | 0.88 · 1.8 · 0.93 | 0.16 · 0.14 | 29 · 48 |
| diff, 1 day, 1% of keys | 0.06 s · 8 · 0.1 MB | 0.06 s · 12 · 0.3 MB | 1.02 · 1.5 · 3.75 | 0.00 · 0.00 | 1 · 2 |
| diff, paused 30 days, first 10K | 0.11 s · 24 · 3.6 MB | 0.16 s · 59 · 9.7 MB | 1.39 · 2.5 · 2.66 | 0.03 · 0.03 | 30 · 46 |
| diff, paused 30 days, all | 6.50 s · 85 · 290.2 MB | 5.95 s · 166 · 271.6 MB | 0.92 · 2.0 · 0.94 | 3.82 · 3.40 | 589 · 690 |
| scan at head, 1% of keys | 0.73 s · 33 · 21.5 MB | 0.71 s · 69 · 20.6 MB | 0.97 · 2.1 · 0.96 | 0.24 · 0.22 | 55 · 77 |
| scan a day ago, 1% of keys | 0.72 s · 27 · 21.4 MB | 0.65 s · 60 · 20.4 MB | 0.90 · 2.2 · 0.95 | 0.23 · 0.21 | 54 · 76 |
| scan at head, all | 26.04 s · 199 · 1497.8 MB | 23.79 s · 388 · 1314.6 MB | 0.91 · 1.9 · 0.88 | 16.78 · 14.91 | 841 · 1009 |
| 1K scattered gets | 1.03 s · 1257 · 36.2 MB | 2.25 s · 3179 · 100.4 MB | 2.17 · 2.5 · 2.77 | 0.32 · 0.56 | 203 · 524 |
| merge the 4 newest layers | 0.36 s · 15 · 5.8 MB | 0.36 s · 28 · 5.3 MB | 1.01 · 1.9 · 0.92 | 0.12 · 0.14 | 25 · 41 |
| fold every layer into the base | 34.40 s · 199 · 1497.8 MB | 33.30 s · 388 · 1314.6 MB | 0.97 · 1.9 · 0.88 | 25.36 · 25.08 | 842 · 1020 |

Stored: ours 1,498 MB (11.28 B/entry), Parquet 1,312 MB (9.88 B/entry): 0.88×. Metadata: ours 1.7 MB, Parquet 9.8 MB. 13 layers; merges wrote 3.95 entries per entry committed.

#### hot

| query | ours, cold | Parquet, cold | Parquet / ours (time · GETs · MB) | warm CPU s, ours · Parquet | peak MB, ours · Parquet |
|---|---|---|---|---|---|
| diff, last commit | 0.10 s · 3 · 0.3 MB | 0.10 s · 4 · 0.5 MB | 1.02 · 1.3 · 1.37 | 0.00 · 0.01 | 2 · 2 |
| diff, 1 day, first 10K (one batch) | 0.08 s · 12 · 1.6 MB | 0.09 s · 22 · 2.1 MB | 1.01 · 1.8 · 1.32 | 0.01 · 0.02 | 18 · 15 |
| diff, 1 day, all | 0.20 s · 22 · 4.8 MB | 0.19 s · 42 · 7.0 MB | 0.95 · 1.9 · 1.44 | 0.06 · 0.05 | 23 · 28 |
| diff, 1 day, 1% of keys | 0.06 s · 12 · 0.1 MB | 0.06 s · 26 · 0.4 MB | 1.02 · 2.2 · 3.08 | 0.00 · 0.00 | 1 · 2 |
| diff, paused 30 days, first 10K | 0.60 s · 49 · 41.4 MB | 0.68 s · 126 · 55.2 MB | 1.12 · 2.6 · 1.33 | 0.31 · 0.25 | 182 · 239 |
| diff, paused 30 days, all | 1.80 s · 62 · 83.4 MB | 1.62 s · 153 · 115.9 MB | 0.90 · 2.5 · 1.39 | 1.07 · 0.72 | 611 · 582 |
| scan at head, 1% of keys | 0.61 s · 33 · 13.2 MB | 0.59 s · 88 · 14.0 MB | 0.95 · 2.7 · 1.06 | 0.15 · 0.13 | 40 · 59 |
| scan a day ago, 1% of keys | 0.58 s · 23 · 13.1 MB | 0.55 s · 67 · 13.7 MB | 0.93 · 2.9 · 1.05 | 0.14 · 0.13 | 40 · 58 |
| scan at head, all | 23.77 s · 176 · 1291.0 MB | 21.68 s · 375 · 1159.0 MB | 0.91 · 2.1 · 0.90 | 14.47 · 12.58 | 863 · 916 |
| 1K scattered gets | 1.25 s · 1134 · 85.4 MB | 2.31 s · 2672 · 179.9 MB | 1.84 · 2.4 · 2.11 | 0.75 · 0.72 | 787 · 1192 |
| merge the 4 newest layers | 0.17 s · 14 · 2.6 MB | 0.20 s · 26 · 3.8 MB | 1.20 · 1.9 · 1.44 | 0.06 · 0.07 | 15 · 20 |
| fold every layer into the base | 34.06 s · 176 · 1291.0 MB | 31.25 s · 375 · 1159.0 MB | 0.92 · 2.1 · 0.90 | 24.93 · 22.81 | 863 · 927 |

Stored: ours 1,291 MB (10.73 B/entry), Parquet 1,154 MB (9.59 B/entry): 0.89×. Metadata: ours 1.4 MB, Parquet 8.6 MB. 13 layers; merges wrote 4.08 entries per entry committed.

#### rewrite

| query | ours, cold | Parquet, cold | Parquet / ours (time · GETs · MB) | warm CPU s, ours · Parquet | peak MB, ours · Parquet |
|---|---|---|---|---|---|
| diff, last commit | 0.46 s · 7 · 10.0 MB | 0.44 s · 17 · 9.2 MB | 0.95 · 2.4 · 0.91 | 0.08 · 0.07 | 36 · 51 |
| diff, 1 day, first 10K (one batch) | 2.79 s · 30 · 137.7 MB | 2.70 s · 58 · 133.7 MB | 0.97 · 1.9 · 0.97 | 1.41 · 1.09 | 300 · 413 |
| diff, 1 day, all | 7.72 s · 62 · 351.1 MB | 7.12 s · 121 · 319.4 MB | 0.92 · 2.0 · 0.91 | 3.67 · 3.23 | 300 · 413 |
| diff, 1 day, 1% of keys | 0.00 s · 0 · 0.0 MB | 0.00 s · 0 · 0.0 MB | 0.00 · 0.0 · 0.00 | 0.00 · 0.00 | 0 · 0 |
| diff, paused 30 days, first 10K | 0.09 s · 16 · 3.6 MB | 0.15 s · 28 · 10.8 MB | 1.64 · 1.8 · 3.02 | 0.02 · 0.03 | 22 · 38 |
| diff, paused 30 days, all | 22.62 s · 149 · 1127.2 MB | 20.93 s · 270 · 1043.2 MB | 0.93 · 1.8 · 0.93 | 12.43 · 11.04 | 491 · 621 |
| diff across the rewrite, first 10K | 0.09 s · 16 · 3.6 MB | 0.16 s · 28 · 10.8 MB | 1.77 · 1.8 · 3.02 | 0.02 · 0.03 | 22 · 38 |
| diff across the rewrite, all | 22.64 s · 149 · 1127.2 MB | 20.79 s · 270 · 1043.2 MB | 0.92 · 1.8 · 0.93 | 12.45 · 11.02 | 495 · 621 |
| scan at head, 1% of keys | 0.65 s · 19 · 24.0 MB | 0.57 s · 66 · 22.0 MB | 0.88 · 3.5 · 0.92 | 0.19 · 0.16 | 60 · 93 |
| scan a day ago, 1% of keys | 0.66 s · 19 · 24.0 MB | 0.51 s · 66 · 22.0 MB | 0.77 · 3.5 · 0.92 | 0.18 · 0.16 | 60 · 93 |
| scan at head, all | 31.74 s · 263 · 2336.3 MB | 28.28 s · 492 · 2102.4 MB | 0.89 · 1.9 · 0.90 | 24.08 · 21.04 | 706 · 927 |
| 1K scattered gets | 1.69 s · 2360 · 58.8 MB | 3.35 s · 4946 · 196.7 MB | 1.98 · 2.1 · 3.35 | 0.45 · 0.86 | 288 · 716 |
| merge the 4 newest layers | 2.85 s · 31 · 70.2 MB | 2.66 s · 74 · 64.0 MB | 0.93 · 2.4 · 0.91 | 1.35 · 1.37 | 148 · 210 |
| fold every layer into the base | 37.91 s · 263 · 2336.3 MB | 37.23 s · 492 · 2102.4 MB | 0.98 · 1.9 · 0.90 | 30.51 · 30.96 | 707 · 928 |

Stored: ours 2,336 MB (11.27 B/entry), Parquet 2,099 MB (10.12 B/entry): 0.90×. Metadata: ours 2.7 MB, Parquet 15.8 MB. 9 layers; merges wrote 2.55 entries per entry committed.

Every measurement above matches the fold: True.

The same tables at 10M keys, warm runs included, and per-query results
counts, are in `results-tables.md`.

## The read policy, and Parquet's row groups (100M, daily-scattered, cold)

Both formats go through one planner, so the I/O policy is shared: ranges
closer than 64 KiB are fetched as one GET, metadata under 1 MiB in one GET
(else a small top level first), units of a block or 4 Parquet pages, batches
that double from 256 KiB to 32 MiB with one in flight ahead. A
request-frugal policy (1 MiB gaps, metadata whole, whole row groups) trades
bytes for requests, and helps both formats on wide reads; it is what an
object_store-based reader does by default. Cells are `time · GETs · MB read
· peak MB`.

| query | ours | Parquet, 128K-row groups | Parquet, 16K-row groups | ours, request-frugal | Parquet, request-frugal |
|---|---|---|---|---|---|
| diff-1c | 0.11 s · 3 · 0.7 MB · 3 MB pk | 0.11 s · 4 · 0.7 MB · 3 MB pk | 0.11 s · 4 · 0.6 MB · 3 MB pk | 0.10 s · 3 · 0.7 MB · 3 MB pk | 0.08 s · 2 · 0.6 MB · 5 MB pk |
| diff-1d-first10k | 0.09 s · 12 · 1.7 MB · 9 MB pk | 0.08 s · 38 · 4.9 MB · 12 MB pk | 0.10 s · 22 · 4.3 MB · 12 MB pk | 0.09 s · 15 · 3.0 MB · 10 MB pk | 0.15 s · 13 · 9.8 MB · 49 MB pk |
| diff-1d | 1.12 s · 31 · 45.9 MB · 103 MB pk | 1.00 s · 90 · 45.2 MB · 104 MB pk | 1.07 s · 41 · 44.7 MB · 104 MB pk | 1.17 s · 31 · 45.9 MB · 101 MB pk | 0.97 s · 23 · 44.1 MB · 114 MB pk |
| diff-1d-range1pct | 0.10 s · 13 · 0.6 MB · 3 MB pk | 0.08 s · 26 · 1.0 MB · 4 MB pk | 0.08 s · 15 · 1.4 MB · 5 MB pk | 0.10 s · 13 · 0.6 MB · 3 MB pk | 0.09 s · 12 · 5.3 MB · 6 MB pk |
| diff-paused-first10k | 0.09 s · 18 · 3.0 MB · 14 MB pk | 0.15 s · 44 · 6.5 MB · 19 MB pk | 0.15 s · 32 · 9.5 MB · 26 MB pk | 0.08 s · 18 · 3.0 MB · 14 MB pk | 0.16 s · 20 · 18.6 MB · 83 MB pk |
| diff-paused | 7.95 s · 88 · 505.8 MB · 450 MB pk | 7.55 s · 253 · 474.2 MB · 538 MB pk | 7.54 s · 97 · 475.2 MB · 539 MB pk | 7.92 s · 88 · 505.8 MB · 444 MB pk | 6.77 s · 77 · 471.8 MB · 517 MB pk |
| scan-head-range1pct | 0.66 s · 33 · 17.9 MB · 42 MB pk | 0.56 s · 103 · 17.7 MB · 62 MB pk | 0.60 s · 40 · 22.4 MB · 76 MB pk | 0.64 s · 32 · 19.1 MB · 42 MB pk | 0.62 s · 26 · 34.6 MB · 90 MB pk |
| scan-head | 27.28 s · 202 · 1715.0 MB · 680 MB pk | 25.49 s · 475 · 1533.4 MB · 867 MB pk | 25.90 s · 173 · 1541.2 MB · 866 MB pk | 27.34 s · 201 · 1715.0 MB · 680 MB pk | 23.75 s · 174 · 1529.4 MB · 798 MB pk |
| get-1k | 2.16 s · 2346 · 134.1 MB · 504 MB pk | 3.69 s · 4070 · 319.4 MB · 989 MB pk | 2.87 s · 2145 · 364.3 MB · 1041 MB pk | 1.44 s · 493 · 734.1 MB · 818 MB pk | 2.19 s · 193 · 1217.4 MB · 1756 MB pk |

Parquet, 128K-row groups: stored 1,529 MB, metadata 10.1 MB (the base's 7.4 MB).
Parquet, 16K-row groups: stored 1,535 MB, metadata 14.6 MB (the base's 10.8 MB).

- 16K-row groups put a unit's four column chunks next to each other, so they
  coalesce: Parquet's GETs fall to ours, its latency is unchanged, and its
  metadata grows 45%. It is the setting to use where request rates matter.
- The request-frugal policy is 7–15% faster on the widest reads and costs
  2–9× the bytes on narrow ones; Parquet's 1K gets read 1.2 GB.

## The Parquet reader's choices (10M keys, daily-scattered, cold)

Each choice undone alone; cells `time · GETs · MB read · peak MB`.

| Parquet reader | diff-1d-first10k | diff-paused-first10k | diff-1d-range1pct | scan-head-range1pct | scan-head | get-1k |
|---|---|---|---|---|---|---|
| final | 0.07 s · 10 · 0.9 MB · 4 pk | 0.07 s · 28 · 2.8 MB · 9 pk | 0.06 s · 12 · 0.2 MB · 1 pk | 0.17 s · 43 · 2.9 MB · 13 pk | 2.80 s · 143 · 153.9 MB · 447 pk | 0.86 s · 368 · 112.1 MB · 533 pk |
| whole row groups as units | 0.10 s · 6 · 2.4 MB · 18 pk | 0.14 s · 12 · 7.7 MB · 46 pk | 0.06 s · 12 · 0.2 MB · 1 pk | 0.10 s · 30 · 2.9 MB · 16 pk | 2.58 s · 35 · 152.6 MB · 364 pk | 0.86 s · 368 · 112.1 MB · 526 pk |
| decode batch = row group | 0.08 s · 10 · 0.9 MB · 16 pk | 0.09 s · 35 · 4.0 MB · 46 pk | 0.06 s · 12 · 0.2 MB · 5 pk | 0.17 s · 43 · 2.9 MB · 50 pk | 3.12 s · 143 · 153.9 MB · 1806 pk | 0.91 s · 368 · 112.1 MB · 2249 pk |
| coalescing gap 256 KiB | 0.07 s · 9 · 1.9 MB · 5 pk | 0.09 s · 21 · 4.8 MB · 11 pk | 0.07 s · 8 · 0.7 MB · 1 pk | 0.17 s · 29 · 5.1 MB · 13 pk | 2.90 s · 68 · 165.0 MB · 450 pk | 0.82 s · 28 · 149.3 MB · 570 pk |
| coalescing gap 1 MiB | 0.08 s · 7 · 2.9 MB · 5 pk | 0.10 s · 15 · 9.4 MB · 13 pk | 0.07 s · 6 · 1.5 MB · 2 pk | 0.19 s · 17 · 10.4 MB · 14 pk | 3.00 s · 43 · 178.9 MB · 450 pk | 0.84 s · 22 · 151.7 MB · 572 pk |

- Whole row groups as read units: a first page (planning one batch) reads
  2.7× the bytes and, behind a paused consumer, takes twice as long; a full
  scan is 8% faster in a quarter of the GETs. A planner that grows its units
  as a read goes on would get both.
- A decode batch the size of the row group: the reader allocates for rows it
  never decodes, 4× the peak memory.
- Coalescing gaps of 256 KiB or 1 MiB (object_store's default): a narrow read
  spans the four column chunks between its pages, 2–4× the bytes for fewer
  GETs.
- Reading all metadata in one GET, at 100M keys: 7.4 MB of the base's page
  indexes for every cold read; with the other two, the request-frugal
  column of the table above (a 1-day, 1% diff reads 5.3 MB, not 1.0).

## DuckDB (10M keys, daily-scattered, local files, 4 threads)

Parquet layers are read natively; ours through an Arrow IPC export, which
stands in for a table function in the native module (decoding to Arrow is
what such a function would do). The export took 1.5 s and
1,019 MB uncompressed, for 172 MB of layers.

| question | Parquet, `read_parquet` (first · again) | ours, Arrow scan (first · again) | same answer |
|---|---|---|---|
| one key's history | 0.956 · 0.157 s | 0.486 · 0.454 s | yes |
| entries in the last day's commits | 0.002 · 0.002 s | 0.353 · 0.563 s | yes |
| entries by commit, the whole log | 0.046 · 0.042 s | 0.570 · 0.559 s | yes |
| a key prefix (1/10 of the keys) | 0.148 · 0.148 s | 0.677 · 0.490 s | yes |

DuckDB prunes Parquet row groups by their statistics (a day's window skips
the base by its commit range) and projects columns. Ours gets neither
without a real extension.

## Dependencies (the worker's native module, `native/`)

```
base: clean release build 20 s (8 jobs, as maturin builds it: pyo3/extension-module), libsolera_native.so 3161040 bytes (stripped: 2565592), 43 crates
parquet: clean release build 54 s (8 jobs, as maturin builds it: pyo3/extension-module), libsolera_native.so 9923680 bytes (stripped: 8263136), 53 crates
pyarrow (Python route): 152 MB installed; import pyarrow.parquet: 0.08 s
duckdb (server only today): 59 MB installed
```

## Stored bytes and merges

| workload (100M) | ours | Parquet | Parquet / ours | merges: entries written per entry committed |
|---|---|---|---|---|
| daily-scattered | 1,715 MB, 12.95 B/entry | 1,529 MB, 11.55 B/entry | 0.89 | 4.16 |
| daily-clustered | 1,498 MB, 11.28 B/entry | 1,312 MB, 9.88 B/entry | 0.88 | 3.95 |
| hot | 1,291 MB, 10.73 B/entry | 1,154 MB, 9.59 B/entry | 0.89 | 4.08 |
| rewrite | 2,336 MB, 11.27 B/entry | 2,099 MB, 10.12 B/entry | 0.90 | 2.55 |

Merges take the same time in both formats (within 2–8%, the fold of 2.3 GB
into a 0.6 GB base in 37–38 s, single-threaded); Parquet reads and writes
10% fewer bytes. The schedule — the brief's rule, run on entry counts so
that both formats get the same layers — wrote 2.5–4.2 entries per entry
committed over 30 days, under the brief's estimate of about 5.

## Method

- **Entries.** `key` (53 bytes, `site-0042/part-17/obj-000042170123-9f3c1a2e.json`:
  in id order, with a hash part as real object names carry), `commit`,
  `new` and `replaced` versions (u64 generations, one per commit, as an
  asset's attempt writes them; absent for a removal or an add).
- **Ours.** Rows prefix-coded against the previous key, varints, a flag
  byte; zstd-1 per 32 KiB block, cut only between keys; a block index in
  segments of 128 blocks under a top level of segment first keys. A
  column-by-section layout inside blocks stored 5% *more* (zstd matches a
  commit with its version in row order) and was dropped.
- **Parquet.** arrow-rs 60: the same four columns, sorted, page statistics
  and the page index, sorting columns declared. The writer and the 32 KiB
  page size were chosen by the 10M sweeps in `results/sweep*` (the first
  sweep's reads predate the planner's fixes; only its stored bytes are
  used).
- **Layers.** One schedule for both: a layer per commit; four adjacent layers
  merged, newest first, when the largest is at most 30× the smallest and
  the result keeps the size rule (4× the newer layers, plus 1 MiB); past 32
  layers the newest pair that fits; folds into the base past a horizon. A
  paused consumer holds the load commit, so the base stays there and a diff
  from it is answerable.
- **Workloads.** 30 days of 24 commits after a load of 1,000 commits:
  1% of the keys a day, scattered (random keys) or clustered (contiguous
  runs, adds at the end of the key space); the same volume on a hot set of
  0.05% of the keys, a third of its writes reverting the key; and 7 days of
  1% then a full rewrite in key order, 1M keys a commit.
- **Reads.** A k-way merge of the layers a query needs, per the brief's §4:
  `diff(C1, C2)` from the entries in the window (oldest's replaced, newest's
  new); `scan(C)` and `get(C, keys)` from the newest entry at or before C.
  Single-threaded decode for both formats.
- **The check.** The workload's commit stream gives every key's version
  history (an oracle independent of layers and formats). Each query's
  output — every key, with its two versions — is folded into an
  order-sensitive 64-bit digest, and so is the oracle's answer; they must
  be equal. Merges are checked by reading their output back. 1,264
  measurements, 0 mismatches.
- **Not modelled.** Clears; combining entries below the retention horizon
  (merges keep every entry); string versions (a source's word); Bloom
  filters; parallel decode; a real object store's latency tail.

## Reproduce

`bench/layer-format/all.sh` (~2 h on the 96-core machine, at most two runs
at once, each in a systemd scope capped at 12 cores and 110 GB): the 10M and
100M finals, the variants, the ablations, the DuckDB comparison and the
dependency cost, into `results/`. `run.sh NAME ARGS` runs one configuration;
`report.py`, `headline.py`, `variants.py` and `ablations.py` turn the JSON
lines into these tables. The numbers above were made by its pieces, run one
by one with the same arguments; the script as a whole was smoke-run at 1M
keys (`RESULTS=… SCALES=1000000 VKEYS=1000000 AKEYS=1000000`: 556
measurements, 0 mismatches).
