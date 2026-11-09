# T44 ranges: range records and run-length versions in our block format

Spike on `exp/layer-format-ranges` (never merged), extending `results.md`.

## Verdict

**Run-length versions: no.** They make every workload's layers 2–7% larger. The one exception is the hot workload's change layers (−31%), and its base outweighs that saving. zstd already finds the repeats in row order, and in the base and the scattered workloads the replaced versions almost never repeat.

**Range records: not now.** Keep one entry per key. Revisit them as a generalization of the existing `clear` flag (§4 rule 4) if real outputs show large range-partitioned rewrites dominating the change log. They do what they promise:

- **Where they help: rewrite.** A full rewrite of 100M keys becomes 101 records instead of 100M entries. Stored bytes fall to 0.57× (2,336 → 1,324 MB). A scan at head (36 → 24 s), a fold into the base (44 → 33 s), 1K gets (1.8 → 1.3 s) and the first page of a 1-day diff (3.1 → 0.14 s) all get faster.
- **Where they help a little: daily-clustered** (contiguous updates and removals). 95% of the change entries become 1,440 records, but stored bytes only fall to 0.81×, because the base dominates.
- **Where they do nothing: daily-scattered and hot.** No run of 16 neighbouring keys is ever rewritten together, so no records are found and the layers are byte-identical to ours.

Three things stop me recommending them:

1. **The saving is temporary.** Records shrink only the change log above the base. Once the retention horizon passes, the fold writes one entry per key anyway, and the base is the same size with or without records (612 MB after the 100M rewrite in every variant). The bench keeps the base pinned at the load commit, because a paused consumer holds it, which shows the best case.
2. **They break the read bound.** Today, a reader starting at any commit reads about 5× the bytes that changed since (the size rule). A record doesn't carry the versions it replaced, so a diff whose window contains one must also read the older layers and the base over the record's key range, after a second round trip to learn the ranges. Measured: the last-commit diff on rewrite at 100M goes from 0.47 s · 7 GETs · 10 MB to 0.67 s · 13 GETs · 13 MB. The paused-consumer diff on clustered at 10M goes from 28 to 354 GETs and 29 to 44 MB. Diffs that span the whole rewrite read about the same bytes either way (1.1 GB of entries vs 1.3 GB of base), and they are 3–5% faster with records.
3. **The complexity reaches past the format.** Every reader path has to resolve records against older history. Merges can no longer check the "entries chain" rule (§4 rule 2) across a record without reading below. Cleanup, which learns from `replaced` which files a commit made obsolete, would have to resolve the replaced versions from older layers too. And records only work when every rewritten key gets the same new version (an asset attempt's generation), never for per-key source versions.

## What a range record is (today vs. with ranges)

**Today.** A full run that rewrites keys `site-0012/…/obj-000012000000…` through `…obj-000012999999…` at commit 1250 writes one entry per key: 1M entries, about 11 MB. Each entry has the shape `(key, 1250, new = g1250, replaced = <that key's old version>)`.

**With ranges.** The same commit writes one record of about 100 bytes: *"every key present in [obj-000012000000, obj-000012999999] just before commit 1250 was rewritten at 1250 to version g1250"*. Adds inside the range stay ordinary entries. A range removal is the same record with no new version. The engine can find records while it builds a commit's entries, because it already knows from its index which keys are present (D184).

**Reading it.** A scan or get already reads every layer down to the base, so it applies the record to each present key it finds, and nothing extra is read. A diff (C1, C2] whose window contains a record first reads the window's layer indexes, where the records sit. It then reads the older layers and the base, but only over the records' key ranges, to learn which keys existed and their versions at C1.

**Merging.** Merges keep records as they are. That is simpler, and it is correct because a merged layer's records still refer to the history below it, which every reader of that layer reads over the range anyway. Expanding records would mean reading the base at every merge. A fold into the base expands records, since it reads every layer anyway and the base must hold one entry per key.

## Method

This extends the T44 bench on branch `exp/layer-format-ranges` (forked from `exp/layer-format` at ed19381). It uses the same workloads, seed, layer schedule (on logical entry counts, so every variant gets the same layers), simulated store (30 ms per request, 80 MB/s) and queries. The "ours" numbers reproduce T44's to the byte.

- **ours+ranges.** Records sit in the layer's index top level, so opening a layer reads no extra bytes. A run needs at least 16 keys. Records are found by replaying each commit against the state before it.
- **ours+rle.** Keys stay row by row, prefix-coded. The flag, commit, new and replaced columns are each stored as (value, run length) pairs, or as plain varints where that is smaller, and the block is then zstd-compressed as before. Blocks are cut at the same rows as ours.
- **Check.** Every query and merge is compared with the brute-force fold of the commit stream, the same check as before. Merges that keep records are checked by spelling out each record against the fold. Result: 632 measurements, 0 mismatches. A deliberately corrupted resolver fails 9 of the 13 rewrite queries. The other 4 don't use replaced versions or return nothing.
- **Scale.** 10M keys covers all four workloads and all four variants. 100M keys covers rewrite and daily-scattered, one variant per process (peak 9.7 GB), because this machine has 4 cores and a 12.6 GiB memory cap. I didn't run daily-clustered or hot at 100M.
- **Reproduce.** `bench/layer-format/ranges.sh` (header has the 100M line), then `python3 ranges_report.py results/ranges-*.jsonl`. All tables are in `results/ranges-tables.md`.
- **Code added (bench).** About 660 lines: about 100 for records in the index and multi-range block lookup, about 170 for run-length blocks, about 180 in the reader (two-phase diff, per-key resolution, records through merges), and about 100 in the driver (finding records, checking merges).

## Stored bytes

| workload | ours | ours+ranges | ours+rle | ours+ranges+rle | records (entries they replace) |
|---|---|---|---|---|---|
| rewrite, 100M | 2,336 MB (11.27 B/entry) | 1,324 MB (6.38) — 0.57× | 2,464 MB — 1.05× | 1,398 MB — 0.60× | 101 (99.8M) |
| daily-scattered, 100M | 1,715 MB (12.95) | 1,715 MB — 1.00× | 1,828 MB — 1.07× | 1,828 MB — 1.07× | 0 |
| rewrite, 10M | 233.5 MB (11.26) | 132.3 MB — 0.57× | 246.6 MB — 1.06× | 139.8 MB — 0.60× | 101 (9.98M) |
| daily-clustered, 10M | 150.1 MB (11.31) | 121.7 MB — 0.81× | 155.7 MB — 1.04× | 126.6 MB — 0.84× | 1,440 (3.12M) |
| daily-scattered, 10M | 171.6 MB (12.95) | 171.6 MB — 1.00× | 182.4 MB — 1.06× | 182.4 MB — 1.06× | 0 |
| hot, 10M | 128.7 MB (10.69) | 128.7 MB — 1.00× | 131.2 MB — 1.02× | 131.2 MB — 1.02× | 0 |

B/entry divides by the same logical entry count for every variant. At 10M, the change log above the base goes from 112.7 to 11.5 MB on rewrite, from 29.4 to 1.0 MB on clustered, and is unchanged on scattered (50.7 MB) and hot (8.0 MB). With run-length coding the change log is +7%, +2%, +10% and −31% respectively, and the base is +4–5%.

## Queries, rewrite, 100M keys (cold: time · GETs · MB read)

| query | ours | ours+ranges | warm CPU s, ours · ranges |
|---|---|---|---|
| diff, last commit | 0.47 s · 7 · 10.0 MB | 0.67 s · 13 · 13.3 MB | 0.08 · 0.13 |
| diff, 1 day, first 10K | 3.07 s · 30 · 137.7 MB | 0.14 s · 10 · 1.0 MB | 1.52 · 0.01 |
| diff, 1 day, all | 8.59 s · 62 · 351.1 MB | 6.55 s · 46 · 314.7 MB | 4.20 · 3.30 |
| diff, paused 30 days, first 10K | 0.11 s · 16 · 3.6 MB | 0.20 s · 12 · 1.9 MB | 0.03 · 0.04 |
| diff, paused 30 days, all | 25.91 s · 149 · 1127.2 MB | 25.12 s · 137 · 1323.9 MB | 14.80 · 14.27 |
| diff, paused, 1% of keys | 0.54 s · 11 · 11.9 MB | 0.65 s · 13 · 13.4 MB | 0.11 · 0.18 |
| diff across the rewrite, all | 25.52 s · 149 · 1127.2 MB | 24.47 s · 137 · 1323.9 MB | 14.78 · 15.11 |
| diff across the rewrite, first 10K | 0.10 s · 16 · 3.6 MB | 0.17 s · 12 · 1.9 MB | 0.03 · 0.04 |
| scan at head, all | 35.77 s · 263 · 2336.3 MB | 24.37 s · 137 · 1323.9 MB | 27.57 · 13.87 |
| scan at head, 1% of keys | 0.65 s · 19 · 24.0 MB | 0.67 s · 13 · 13.4 MB | 0.22 · 0.16 |
| scan a day ago, 1% of keys | 0.70 s · 19 · 24.0 MB | 0.61 s · 13 · 13.4 MB | 0.20 · 0.15 |
| 1K scattered gets | 1.78 s · 2360 · 58.8 MB | 1.26 s · 1478 · 45.2 MB | 0.48 · 0.39 |
| merge the 4 newest layers | 3.33 s, read 70.2 MB, wrote 70.2 MB | 0.03 s, read 0.0 MB, wrote 0.0 MB | 1.83 · 0.00 |
| fold every layer into the base | 44.05 s, read 2336 MB → 612 MB | 32.64 s, read 1324 MB → 612 MB | 36.41 · 23.51 |

## Queries, daily-clustered, 10M keys: what records cost a diff

| query | ours | ours+ranges |
|---|---|---|
| diff, last commit | 0.15 s · 4 · 0.8 MB | 0.10 s · 10 · 0.3 MB |
| diff, 1 day, first 10K | 0.07 s · 5 · 1.1 MB | 0.14 s · 23 · 1.0 MB |
| diff, 1 day, all | 0.19 s · 8 · 1.7 MB | 0.21 s · 36 · 2.0 MB |
| diff, paused 30 days, all | 1.06 s · 28 · 29.4 MB | 1.17 s · 354 · 44.1 MB |
| scan at head, all | 3.08 s · 45 · 150.1 MB | 2.95 s · 30 · 121.7 MB |
| 1K scattered gets | 0.69 s · 705 · 32.4 MB | 0.55 s · 576 · 23.6 MB |
| merge the 4 newest layers | 0.29 s · 3.2 MB written | 0.07 s · 0.1 MB written |

The 1,440 records cover about 2,000 small key ranges spread over the key space. A diff across them has to fetch a base block and a block from each older layer for each range, which is where the 12× GETs come from.

## Queries, daily-scattered, 100M keys

There are no records, so ours+ranges reads exactly what ours reads (same GETs and bytes on every query). Times are within run-to-run noise: a scan at head takes 29.7 vs 30.2 s, and the paused diff 8.4 vs 9.0 s. The full table is in `results/ranges-tables.md`.
