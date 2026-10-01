# K0 benchmark results

Local MinIO with injected S3-like latency; reproduce with `uv run python bench/keys/bench.py --sizes 1e6,1e7,1e8 --latency 0.03` (a MinIO at 127.0.0.1:9100, see the script). Machine: 8 cores, native extension, 2026-09-23.
### Key index benchmark (native, 30 ms per request, 80 MB/s per connection, 64 in parallel)

| Keys | Build | Index size | Per entry (incl. filters) | Filters per entry | Files | Levels |
|---|---|---|---|---|---|---|
| 1,000,000 | 1.3 s | 29.0 MB | 29.0 B | 3.51 B | 1 | 1 |
| 10,000,000 | 11.6 s | 281.4 MB | 28.1 B | 3.51 B | 3 | 2 |
| 100,000,000 | 113.9 s | 2,719.6 MB | 27.2 B | 3.51 B | 29 | 3 |

| Operation | 1,000,000 keys | 10,000,000 keys | 100,000,000 keys |
|---|---|---|---|
| 100 random keys changed | 342 ms · 2 GET 0 PUT · 29.0 MB | 239 ms · 3 GET 0 PUT · 35.1 MB | 434 ms · 28 GET 0 PUT · 338.5 MB |
| 1K random keys changed | 469 ms · 2 GET 0 PUT · 29.0 MB | 280 ms · 5 GET 0 PUT · 35.2 MB | 474 ms · 34 GET 0 PUT · 351.2 MB |
| 1K random keys, half unchanged | 461 ms · 2 GET 0 PUT · 29.0 MB | 773 ms · 453 GET 0 PUT · 66.0 MB | 843 ms · 542 GET 0 PUT · 383.3 MB |
| 1K clustered keys changed | 307 ms · 2 GET 0 PUT · 29.0 MB | 259 ms · 2 GET 0 PUT · 12.5 MB | 219 ms · 1 GET 0 PUT · 12.4 MB |
| 1K new keys inserted | 475 ms · 2 GET 0 PUT · 29.0 MB | 247 ms · 3 GET 0 PUT · 35.1 MB | 495 ms · 29 GET 0 PUT · 350.9 MB |
| 100K random keys changed | 868 ms · 2 GET 0 PUT · 29.0 MB | 2.6 s · 17 GET 0 PUT · 281.4 MB | 1.3 s · 376 GET 0 PUT · 372.7 MB |
| 1K random keys changed, disk cache warm | 163 ms · 0 GET 0 PUT · 0.0 MB | 28 ms · 0 GET 0 PUT · 0.0 MB | 227 ms · 0 GET 0 PUT · 0.0 MB |
| full-delivery page of 10K keys | 80 ms · 2 GET 0 PUT · 0.3 MB | 81 ms · 2 GET 0 PUT · 0.3 MB | 83 ms · 2 GET 0 PUT · 0.3 MB |
| commit: 1K random changes + delta write | 485 ms · 2 GET 1 PUT · 29.0 MB | 321 ms · 6 GET 1 PUT · 35.3 MB | 733 ms · 36 GET 1 PUT · 351.3 MB |
| compaction: 8 delta files into level 1 | 1.4 s · 10 GET 1 PUT · 29.3 MB | 92 ms · 8 GET 1 PUT · 0.3 MB | 91 ms · 8 GET 1 PUT · 0.3 MB |
| full replacement, 1% changed | 805 ms · 2 GET 0 PUT · 29.0 MB | 5.9 s · 17 GET 0 PUT · 281.4 MB | — |

## Follow-up: the rest of §13 (2026-09-30)

K0 above left part of `object-store-state.md` §13 unmeasured: a full scan,
the filters' false-positive rate, block size, level fanout, the
read-strategy crossovers, and bulk operations at 100M. Its indexes were
built straight into their bottom level, so levels 1–2 were empty, and at
10M and 100M its compaction merged eight deltas into an empty level 1
(0.3 MB). Same setup here: 8 cores, local MinIO with 30 ms injected per
request and 80 MB/s per connection, 64 requests in parallel, native unless
marked. The base rows re-ran unchanged: the same request counts and bytes
as K0, wall times within 25% (32% for the CPU-bound warm-cache rows).
Nothing here ran on real S3.

    uv run python bench/keys/bench.py --s3 http://solera:solera-bench-secret@127.0.0.1:9100/solera-test --sizes 1e8
    uv run python bench/keys/params.py
    uv run python bench/keys/cpu.py 1000000

`bench.py` gained four suites. `scan` is the full scan a recount does, cold
and through a warm disk cache. `load` is an initial load of every key, in
row order, through `KeyIndex.changes` and `write`. `crossover` forces each
read strategy against the planner's pick. `steady` fills the index the way
steady-state writes leave it — levels 1 and 2 at 95% of their targets with
newer versions of random keys, seven deltas in level 0 — then commits an
eighth delta and runs one compaction of each kind, pushing a level down the
way the planner does once it is over its target. Each size ran in its own
process; "Peak RSS" is that process's.

### Format (`params.py`, 1M entries, no I/O)

| Keys | Versions | Raw | Prefix-encoded | Blocks (zlib) | Compression | Filters | Index | **Total per entry** | Entries per block | Block, compressed |
|---|---|---|---|---|---|---|---|---|---|---|
| random ids `cust-%013d` (bench) | 16 random bytes (bench, model) | 34.0 B | 26.8 B | 25.5 B | 1.33× | 3.50 B | 0.010 B | **29.0 B** | 2,439 | 60.8 KiB |
| random ids `cust-%013d` (bench) | SHA-256 hex (row digest, the default) | 82.0 B | 74.8 B | 65.0 B | 1.26× | 3.50 B | 0.026 B | **68.5 B** | 876 | 55.6 KiB |
| UUIDs | 16 random bytes (bench, model) | 52.0 B | 51.7 B | 47.8 B | 1.09× | 3.50 B | 0.035 B | **51.3 B** | 1,267 | 59.2 KiB |
| UUIDs | SHA-256 hex (row digest, the default) | 100.0 B | 99.7 B | 87.9 B | 1.14× | 3.50 B | 0.067 B | **91.4 B** | 658 | 56.5 KiB |
| sequential ids `order-%012d` | 16 random bytes (bench, model) | 34.0 B | 21.1 B | 20.2 B | 1.68× | 3.50 B | 0.006 B | **23.7 B** | 3,096 | 61.2 KiB |
| sequential ids `order-%012d` | SHA-256 hex (row digest, the default) | 82.0 B | 69.1 B | 59.6 B | 1.38× | 3.50 B | 0.019 B | **63.1 B** | 949 | 55.2 KiB |
| sequential ids `order-%012d` | short revision `%d` | 23.9 B | 11.0 B | 7.2 B | 3.33× | 3.50 B | 0.003 B | **10.7 B** | 5,952 | 41.7 KiB |
| paths `site-…/file-i` (cpu.py) | 16 random bytes (bench, model) | 44.9 B | 38.6 B | 31.8 B | 1.41× | 3.50 B | 0.018 B | **35.3 B** | 1,698 | 52.7 KiB |
| paths `site-…/file-i` (cpu.py) | SHA-256 hex (row digest, the default) | 92.9 B | 86.6 B | 71.9 B | 1.29× | 3.50 B | 0.039 B | **75.5 B** | 757 | 53.2 KiB |

The model's ~20 B per entry assumed 2× compression. Random ids with random
versions compress 1.3×, and most of that is prefix compression rather than
zlib (26.8 B → 25.5 B). The harness's default version is a SHA-256 hex
digest — 64 bytes, not 16 — which makes an entry 68.5 B.

| Bits per item | k | Filter bytes per entry (3 filters) | Key filter FP | Pair filter FP (changed version) | Theory, standard | Theory, 512-bit blocked |
|---|---|---|---|---|---|---|
| 8 | 6 | 2.00 B | 2.511% | 2.500% | 2.158% | 2.342% |
| 10 | 7 | 2.50 B | 1.138% | 1.121% | 0.819% | 0.957% |
| 12 | 8 | 3.00 B | 0.569% | 0.569% | 0.314% | 0.407% |
| 14 (default) | 10 | 3.50 B | 0.347% | 0.354% | 0.120% | 0.187% |
| 16 | 11 | 4.00 B | 0.228% | 0.233% | 0.046% | 0.086% |
| 20 | 14 | 5.00 B | 0.145% | 0.145% | 0.007% | 0.022% |

Filters take 3.5 B per entry as modelled, but they false-positive 0.35% of
the time, not 0.2% (the model) or 0.1% (§6): double hashing inside one
512-bit block, `(a + i·b) mod 512`, costs ~1.8× over independent bit
positions.

| Versions | Block (raw) | Total per entry | Blocks per entry | Index part | Entries per block | Block, compressed | Encode | Decode one block |
|---|---|---|---|---|---|---|---|---|
| 16 random bytes | 4 KiB | 29.9 B | 26.3 B | 141 KB | 153 | 3.9 KiB | 2.47 M/s | 0.03 ms |
| 16 random bytes | 16 KiB | 29.3 B | 25.8 B | 37 KB | 611 | 15.4 KiB | 3.05 M/s | 0.12 ms |
| 16 random bytes | 32 KiB | 29.1 B | 25.6 B | 19 KB | 1,221 | 30.5 KiB | 3.27 M/s | 0.23 ms |
| 16 random bytes | 64 KiB (default) | 29.0 B | 25.5 B | 10 KB | 2,439 | 60.8 KiB | 2.74 M/s | 0.45 ms |
| 16 random bytes | 128 KiB | 29.0 B | 25.5 B | 5 KB | 4,878 | 121.3 KiB | 2.67 M/s | 0.90 ms |
| 16 random bytes | 256 KiB | 29.0 B | 25.5 B | 3 KB | 9,709 | 241.3 KiB | 2.52 M/s | 1.80 ms |
| SHA-256 hex | 4 KiB | 75.8 B | 71.9 B | 384 KB | 55 | 3.9 KiB | 1.41 M/s | 0.02 ms |
| SHA-256 hex | 16 KiB | 71.8 B | 68.2 B | 102 KB | 219 | 14.6 KiB | 1.51 M/s | 0.08 ms |
| SHA-256 hex | 32 KiB | 69.7 B | 66.2 B | 52 KB | 438 | 28.3 KiB | 1.55 M/s | 0.16 ms |
| SHA-256 hex | 64 KiB (default) | 68.5 B | 65.0 B | 26 KB | 876 | 55.6 KiB | 1.33 M/s | 0.34 ms |
| SHA-256 hex | 128 KiB | 68.0 B | 64.5 B | 13 KB | 1,751 | 110.3 KiB | 1.26 M/s | 0.67 ms |
| SHA-256 hex | 256 KiB | 67.7 B | 64.2 B | 7 KB | 3,497 | 219.3 KiB | 1.17 M/s | 1.40 ms |

Block size barely matters between 32 and 256 KiB. A 64 KiB block is ~60 KiB
compressed (the model assumed 32 KB): under a millisecond of transfer per
exact lookup.

### CPU (`cpu.py 1000000`: 1M path-like keys, 16-byte versions)

| | Native | Pure Python |
|---|---|---|
| encode (blocks + filters) | 1.25M entries/s | 0.14M entries/s |
| decode every block | 4.57M entries/s | 1.14M entries/s |
| pair filter check | 3.69M entries/s | 0.51M entries/s |
| sort | 1.49M entries/s | 0.58M entries/s |
| merge two files into one | 1.52M entries/s | 0.12M entries/s |
| file size | 35.3 MB | 32.6 MB |

The native writer's files are 8% larger: zlib-rs at level 1 compresses less
than CPython's zlib at level 1. Without compression a file is 19% larger and
encodes 40% faster (`cargo run --release --example bench`).

### Operations (`bench.py`)

#### Key index benchmark (native, 30 ms per request, 80 MB/s per connection, 64 in parallel)

| Keys | Build | Build rate | Index size | Per entry (incl. filters) | Filters per entry | Files | Levels | Peak RSS |
|---|---|---|---|---|---|---|---|---|
| 1,000,000 | 1.3 s | 0.75 M keys/s, 22 MB/s | 29.0 MB | 29.0 B | 3.51 B | 1 | 1 | 0.8 GB |
| 10,000,000 | 12.1 s | 0.82 M keys/s, 23 MB/s | 281.4 MB | 28.1 B | 3.51 B | 3 | 2 | 5.0 GB |
| 100,000,000 | 115.7 s | 0.86 M keys/s, 24 MB/s | 2,719.6 MB | 27.2 B | 3.51 B | 29 | 3 | 5.9 GB |

| Operation | 1,000,000 keys | 10,000,000 keys | 100,000,000 keys |
|---|---|---|---|
| 100 random keys changed | 354 ms · 2 GET 0 PUT · 29.0 MB | 251 ms · 3 GET 0 PUT · 35.1 MB | 436 ms · 28 GET 0 PUT · 338.5 MB |
| 1K random keys changed | 470 ms · 2 GET 0 PUT · 29.0 MB | 295 ms · 5 GET 0 PUT · 35.2 MB | 507 ms · 34 GET 0 PUT · 351.2 MB |
| 1K random keys, half unchanged | 465 ms · 2 GET 0 PUT · 29.0 MB | 766 ms · 453 GET 0 PUT · 66.0 MB | 938 ms · 542 GET 0 PUT · 383.3 MB |
| 1K clustered keys changed | 308 ms · 2 GET 0 PUT · 29.0 MB | 253 ms · 2 GET 0 PUT · 12.5 MB | 224 ms · 1 GET 0 PUT · 12.4 MB |
| 1K new keys inserted | 471 ms · 2 GET 0 PUT · 29.0 MB | 259 ms · 3 GET 0 PUT · 35.1 MB | 434 ms · 29 GET 0 PUT · 350.9 MB |
| 100K random keys changed | 836 ms · 2 GET 0 PUT · 29.0 MB | 2.5 s · 17 GET 0 PUT · 281.4 MB | 1.3 s · 376 GET 0 PUT · 372.7 MB |
| 1K random keys changed, disk cache warm | 156 ms · 0 GET 0 PUT · 0.0 MB | 34 ms · 0 GET 0 PUT · 0.0 MB | 299 ms · 0 GET 0 PUT · 0.0 MB |
| full-delivery page of 10K keys | 80 ms · 2 GET 0 PUT · 0.3 MB | 80 ms · 2 GET 0 PUT · 0.3 MB | 83 ms · 2 GET 0 PUT · 0.3 MB |
| commit: 1K random changes + delta write | 491 ms · 2 GET 1 PUT · 29.0 MB | 312 ms · 6 GET 1 PUT · 35.3 MB | 560 ms · 36 GET 1 PUT · 351.3 MB |
| compaction: 8 delta files into level 1 | 1.4 s · 10 GET 1 PUT · 29.3 MB ↑29.0 MB | 91 ms · 8 GET 1 PUT · 0.3 MB ↑0.2 MB | 90 ms · 8 GET 1 PUT · 0.3 MB ↑0.2 MB |
| full replacement, 1% changed | 728 ms · 2 GET 0 PUT · 29.0 MB | 6.9 s · 17 GET 0 PUT · 281.4 MB | — |
| initial load: every key, unsorted, changes + write | 1.8 s · 0 GET 1 PUT · 0.0 MB ↑28.9 MB | 22.5 s · 0 GET 3 PUT · 0.0 MB ↑279.9 MB | — |
| full scan (recount), 100K-key pages | 1.4 s · 12 GET 0 PUT · 26.1 MB | 14.1 s · 105 GET 0 PUT · 252.6 MB | 118.8 s · 1077 GET 0 PUT · 2433.3 MB |
| steady: 1K random keys changed | 766 ms · 9 GET 0 PUT · 29.2 MB | 1.2 s · 17 GET 0 PUT · 100.4 MB | 1.5 s · 65 GET 0 PUT · 501.5 MB |
| steady: 1K random keys, half unchanged | 762 ms · 9 GET 0 PUT · 29.2 MB | 1.5 s · 364 GET 0 PUT · 124.1 MB | 2.3 s · 857 GET 0 PUT · 552.9 MB |
| steady: full-delivery page of 10K keys | 334 ms · 16 GET 0 PUT · 0.5 MB | 359 ms · 18 GET 0 PUT · 0.8 MB | 421 ms · 20 GET 0 PUT · 1.1 MB |
| steady: full scan (recount), 100K-key pages | 4.3 s · 96 GET 0 PUT · 28.3 MB | 52.7 s · 929 GET 0 PUT · 528.4 MB | 615.5 s · 10518 GET 0 PUT · 7789.6 MB |
| steady: commit: 1K random changes + delta write | 821 ms · 9 GET 1 PUT · 29.2 MB | 1.2 s · 15 GET 1 PUT · 100.3 MB | 1.5 s · 58 GET 1 PUT · 501.1 MB |
| steady: compaction: 8 delta files + level 1 into level 1 | 1.4 s · 10 GET 1 PUT · 29.3 MB ↑29.0 MB | 2.7 s · 12 GET 1 PUT · 65.2 MB ↑65.1 MB | 2.8 s · 12 GET 1 PUT · 67.3 MB ↑67.3 MB |
| full scan (recount), disk cache warm | 399 ms · 0 GET 0 PUT · 0.0 MB | 4.0 s · 0 GET 0 PUT · 0.0 MB | 41.2 s · 0 GET 0 PUT · 0.0 MB |
| steady: compaction: a level-1 file into level 2 | — | 11.9 s · 21 GET 3 PUT · 346.5 MB ↑281.4 MB | 28.7 s · 45 GET 8 PUT · 716.8 MB ↑698.7 MB |
| steady: compaction: a level-2 file into level 3 | — | — | 5.3 s · 9 GET 2 PUT · 140.4 MB ↑126.0 MB |

Steady-state shape (level: files, MB), before and after the compactions:

| Keys | Before | After |
|---|---|---|
| 1,000,000 | L0: 7, 0.2 MB · L1: 1, 29.0 MB | L1: 1, 29.0 MB |
| 10,000,000 | L0: 7, 0.2 MB · L1: 1, 64.9 MB · L2: 3, 281.4 MB | L2: 3, 281.4 MB |
| 100,000,000 | L0: 7, 0.2 MB · L1: 1, 67.0 MB · L2: 7, 649.6 MB · L3: 29, 2,719.6 MB | L2: 7, 684.4 MB · L3: 29, 2,719.6 MB |

Read strategy, forced each way (cold; wall · GETs · MB read):

| Keys | Written keys | Unchanged | Whole levels | Filters | Planner picks | Planner's estimate, whole / filters |
|---|---|---|---|---|---|---|
| 1,000,000 | 1,000 | 0% | 476 ms · 2 GET 0 PUT · 29.0 MB | 119 ms · 5 GET 0 PUT · 3.8 MB | whole: 441 ms | 88 ms / 37 ms |
| 1,000,000 | 10,000 | 0% | 482 ms · 2 GET 0 PUT · 29.0 MB | 179 ms · 33 GET 0 PUT · 5.6 MB | whole: 494 ms | 88 ms / 67 ms |
| 1,000,000 | 100,000 | 0% | 652 ms · 2 GET 0 PUT · 29.0 MB | 552 ms · 110 GET 0 PUT · 17.9 MB | whole: 654 ms | 88 ms / 487 ms |
| 1,000,000 | 1,000 | 50% | 449 ms · 2 GET 0 PUT · 29.0 MB | 316 ms · 79 GET 0 PUT · 22.3 MB | whole: 453 ms | 88 ms / 37 ms |
| 1,000,000 | 10,000 | 50% | 464 ms · 2 GET 0 PUT · 29.0 MB | 556 ms · 3 GET 0 PUT · 29.0 MB | whole: 487 ms | 88 ms / 67 ms |
| 1,000,000 | 100,000 | 50% | 664 ms · 2 GET 0 PUT · 29.0 MB | 839 ms · 3 GET 0 PUT · 29.0 MB | whole: 732 ms | 88 ms / 487 ms |
| 10,000,000 | 1,000 | 0% | 913 ms · 17 GET 0 PUT · 281.4 MB | 287 ms · 8 GET 0 PUT · 35.4 MB | filtered: 270 ms | 593 ms / 100 ms |
| 10,000,000 | 10,000 | 0% | 2.0 s · 17 GET 0 PUT · 281.4 MB | 335 ms · 31 GET 0 PUT · 36.8 MB | whole: 2.0 s | 593 ms / 130 ms |
| 10,000,000 | 100,000 | 0% | 2.4 s · 17 GET 0 PUT · 281.4 MB | 858 ms · 311 GET 0 PUT · 57.0 MB | whole: 2.3 s | 593 ms / 550 ms |
| 10,000,000 | 1,000,000 | 0% | 5.7 s · 17 GET 0 PUT · 281.4 MB | 7.1 s · 975 GET 0 PUT · 178.5 MB | whole: 5.5 s | 593 ms / 4.8 s |
| 10,000,000 | 1,000 | 50% | 896 ms · 17 GET 0 PUT · 281.4 MB | 682 ms · 411 GET 0 PUT · 63.6 MB | filtered: 703 ms | 593 ms / 100 ms |
| 10,000,000 | 10,000 | 50% | 2.1 s · 17 GET 0 PUT · 281.4 MB | 2.0 s · 789 GET 0 PUT · 212.0 MB | whole: 2.1 s | 593 ms / 130 ms |
| 10,000,000 | 100,000 | 50% | 2.5 s · 17 GET 0 PUT · 281.4 MB | 2.6 s · 20 GET 0 PUT · 281.4 MB | whole: 2.4 s | 593 ms / 550 ms |
| 100,000,000 | 1,000 | 0% | 2.9 s · 170 GET 0 PUT · 2719.6 MB | 531 ms · 30 GET 0 PUT · 350.9 MB | filtered: 479 ms | 5.5 s / 732 ms |
| 100,000,000 | 10,000 | 0% | 5.7 s · 170 GET 0 PUT · 2719.6 MB | 538 ms · 63 GET 0 PUT · 353.0 MB | filtered: 542 ms | 5.5 s / 792 ms |
| 100,000,000 | 100,000 | 0% | 17.0 s · 170 GET 0 PUT · 2719.6 MB | 1.1 s · 372 GET 0 PUT · 372.5 MB | filtered: 1.2 s | 5.5 s / 1.2 s |
| 100,000,000 | 1,000,000 | 0% | 22.2 s · 170 GET 0 PUT · 2719.6 MB | 9.6 s · 3080 GET 0 PUT · 558.2 MB | filtered: 10.1 s | 5.5 s / 5.4 s |
| 100,000,000 | 1,000 | 50% | 2.3 s · 170 GET 0 PUT · 2719.6 MB | 889 ms · 515 GET 0 PUT · 381.6 MB | filtered: 1.1 s | 5.5 s / 732 ms |
| 100,000,000 | 10,000 | 50% | 5.7 s · 170 GET 0 PUT · 2719.6 MB | 5.0 s · 4167 GET 0 PUT · 645.6 MB | filtered: 5.0 s | 5.5 s / 792 ms |
| 100,000,000 | 100,000 | 50% | 17.1 s · 170 GET 0 PUT · 2719.6 MB | 17.0 s · 7395 GET 0 PUT · 2105.9 MB | filtered: 17.1 s | 5.5 s / 1.2 s |

#### Key index benchmark (python, 30 ms per request, 80 MB/s per connection, 64 in parallel)

| Keys | Build | Build rate | Index size | Per entry (incl. filters) | Filters per entry | Files | Levels | Peak RSS |
|---|---|---|---|---|---|---|---|---|
| 1,000,000 | 7.3 s | 0.14 M keys/s, 4 MB/s | 27.1 MB | 27.1 B | 3.51 B | 1 | 1 | 0.7 GB |

| Operation | 1,000,000 keys |
|---|---|
| 100 random keys changed | 503 ms · 2 GET 0 PUT · 27.1 MB |
| 1K random keys changed | 1.2 s · 2 GET 0 PUT · 27.1 MB |
| 1K random keys, half unchanged | 1.1 s · 2 GET 0 PUT · 27.1 MB |
| 1K clustered keys changed | 321 ms · 2 GET 0 PUT · 27.1 MB |
| 1K new keys inserted | 1.2 s · 2 GET 0 PUT · 27.1 MB |
| 100K random keys changed | 1.6 s · 2 GET 0 PUT · 27.1 MB |
| 1K random keys changed, disk cache warm | 841 ms · 0 GET 0 PUT · 0.0 MB |
| full-delivery page of 10K keys | 89 ms · 2 GET 0 PUT · 0.3 MB |
| commit: 1K random changes + delta write | 1.2 s · 2 GET 1 PUT · 27.1 MB |
| compaction: 8 delta files into level 1 | 8.3 s · 10 GET 1 PUT · 27.3 MB ↑27.1 MB |
| full replacement, 1% changed | 1.8 s · 2 GET 0 PUT · 27.1 MB |
| initial load: every key, unsorted, changes + write | 8.5 s · 0 GET 1 PUT · 0.0 MB ↑26.9 MB |
| full scan (recount), 100K-key pages | 2.2 s · 12 GET 0 PUT · 24.1 MB |
| steady: 1K random keys changed | 1.5 s · 9 GET 0 PUT · 27.3 MB |
| steady: 1K random keys, half unchanged | 1.4 s · 9 GET 0 PUT · 27.3 MB |
| steady: full-delivery page of 10K keys | 371 ms · 16 GET 0 PUT · 0.5 MB |
| steady: full scan (recount), 100K-key pages | 5.4 s · 96 GET 0 PUT · 26.1 MB |
| steady: commit: 1K random changes + delta write | 1.5 s · 9 GET 1 PUT · 27.3 MB |
| steady: compaction: 8 delta files + level 1 into level 1 | 8.8 s · 10 GET 1 PUT · 27.3 MB ↑27.1 MB |

Steady-state shape (level: files, MB), before and after the compactions:

| Keys | Before | After |
|---|---|---|
| 1,000,000 | L0: 7, 0.2 MB · L1: 1, 27.1 MB | L1: 1, 27.1 MB |

Read strategy, forced each way (cold; wall · GETs · MB read):

| Keys | Written keys | Unchanged | Whole levels | Filters | Planner picks | Planner's estimate, whole / filters |
|---|---|---|---|---|---|---|
| 1,000,000 | 1,000 | 0% | 1.1 s · 2 GET 0 PUT · 27.1 MB | 163 ms · 5 GET 0 PUT · 3.7 MB | whole: 1.1 s | 84 ms / 37 ms |
| 1,000,000 | 10,000 | 0% | 1.3 s · 2 GET 0 PUT · 27.1 MB | 274 ms · 33 GET 0 PUT · 5.5 MB | whole: 1.2 s | 84 ms / 67 ms |
| 1,000,000 | 100,000 | 0% | 1.6 s · 2 GET 0 PUT · 27.1 MB | 1.8 s · 110 GET 0 PUT · 16.8 MB | whole: 1.6 s | 84 ms / 487 ms |
| 1,000,000 | 1,000 | 50% | 1.1 s · 2 GET 0 PUT · 27.1 MB | 866 ms · 79 GET 0 PUT · 20.9 MB | whole: 1.1 s | 84 ms / 37 ms |
| 1,000,000 | 10,000 | 50% | 1.2 s · 2 GET 0 PUT · 27.1 MB | 1.4 s · 3 GET 0 PUT · 27.1 MB | whole: 1.3 s | 84 ms / 67 ms |
| 1,000,000 | 100,000 | 50% | 1.5 s · 2 GET 0 PUT · 27.1 MB | 2.2 s · 3 GET 0 PUT · 27.1 MB | whole: 1.5 s | 84 ms / 487 ms |

A full replacement and an initial load hold every key in memory: 4.3 GB of
peak RSS at 10M, so ~45 GB at 100M, more than this machine has. At 10M a
profile shows only per-entry work, so both extrapolate linearly: a full
replacement of 100M keys takes ~60 s (at 10M: `replace_diff` 3.3 s, sort
0.8 s, Python 0.8 s, reads ~1 s), and an initial load ~225 s (at 10M: the
Python loops in `KeyIndex._replace` and `_split` 14 s, encoding 4.5 s,
sorting and diffing 1.5 s).

## Fixes to the follow-up's findings (2026-09-30, later)

The follow-up above found five things to fix in the index itself; they are
fixed, and this section measures before and after on the same machine and
setup (8 cores, local MinIO, 30 ms per request, 80 MB/s per connection, 64
in parallel, native). "Before" at 1M and 10M is a rerun of the previous
commit today, within 10% of the follow-up's figures; at 100M it is the
follow-up's own run.

    uv run python bench/keys/bench.py --s3 http://solera:solera-bench-secret@127.0.0.1:9100/solera-test --sizes 1e6,1e7
    uv run python bench/keys/bench.py --s3 http://solera:solera-bench-secret@127.0.0.1:9100/solera-test --sizes 1e8
    uv run python bench/keys/amplification.py --sizes 1e6,1e7,1e8 --commits 100000
    uv run python bench/keys/params.py

What changed:

1. **The read planner decides after reading the tails.** Levels over 32 MB
   start with their files' tails; the filters say which keys need an exact
   read, and only then does the planner pick, per level, between their
   blocks and the rest of each file whole — fewest requests within the 2 s
   budget, else fastest. Its estimate gained CPU (decoding at 4.5M
   entries/s native, 1M/s Python; filter checks at 0.4M/0.12M keys/s,
   measured through `KeyIndex._filter`) and an 80 MB/s per-request cap.
   Exact reads go only to files whose key filter matched.
2. **Level 0 merges in itself until it is a tenth of level 1**, then into
   level 1.
3. **A recount applies to the state it pinned**, commits since adding their
   `added − removed` (`IndexRecounted`; `count_exact` became `inexact`, a
   count of filter-based commits since the last recount).
4. **No sequential awaits**: level-0 files and small levels are fetched at
   once, lookups in every filtered level run at once, a scan fetches all its
   files' blocks at once, keeps each file's last page of blocks for the
   next, and reads files under ~2.4 MB (one request's latency worth of
   transfer) whole, once.
5. **Row digests are 16 raw bytes** (BLAKE2b of the row's encoding) from
   `key_map` to the index and back; a declared revision is its text.

### Before and after

Steady state (the `steady` suite) differs in one way: level 0 now holds,
besides six deltas, a merged file of on average half the size at which it
joins level 1 (1.7 / 3.6 / 3.9 MB), where before it held seven deltas.

| Operation | 1M before → after | 10M before → after | 100M before → after |
|---|---|---|---|
| steady: 1K random keys changed | 792 ms · 9 GET → **544 ms · 9 GET** | 1.2 s · 17 GET → **500 ms · 13 GET** | 1.5 s · 65 GET → **765 ms · 48 GET** |
| steady: 1K random keys, half unchanged | 771 ms · 9 GET → **565 ms · 9 GET** | 1.7 s · 364 GET → **1.0 s · 32 GET** | 2.3 s · 857 GET → **1.1 s · 291 GET** |
| steady: commit, 1K random changes + delta write | 811 ms · 9 GET → **571 ms · 9 GET** | 1.3 s · 15 GET → **543 ms · 12 GET** | 1.5 s · 58 GET → **817 ms · 50 GET** |
| steady: full-delivery page of 10K keys | 331 ms · 16 GET → **108 ms · 9 GET** | 392 ms · 18 GET → **105 ms · 12 GET** | 421 ms · 20 GET → **102 ms · 14 GET** |
| steady: full scan (recount), 100K-key pages | 4.4 s · 96 GET → **1.7 s · 19 GET** | 53.2 s · 929 GET → **21.6 s · 219 GET** | 615.5 s · 10,518 GET → **241.7 s · 3,029 GET** |
| steady: level 0 compacted | 1.4 s, ↑29.0 MB for 0.23 MB of deltas | 2.8 s, ↑65.1 MB | 2.8 s, ↑67.3 MB |
| → now, 8 files merged in level 0 | **176 ms, ↑1.7 MB** | **307 ms, ↑3.5 MB** | **272 ms, ↑3.7 MB** |
| → now, level 0 at a tenth of level 1, into level 1 | **1.4 s, ↑29.0 MB** | **2.9 s, ↑68.5 MB** | **3.1 s, ↑71.8 MB** |
| 1K random keys, half unchanged (fresh index) | 490 ms · 2 GET → 477 ms · 2 GET | 773 ms · 453 GET → **950 ms · 20 GET** | 938 ms · 542 GET → 1.0 s · 542 GET |
| 100K random keys changed (fresh index) | 851 ms · 2 GET → 883 ms · 2 GET | 2.6 s · 17 GET → **1.4 s · 20 GET** | 1.3 s · 376 GET → 1.3 s · 376 GET |
| entry size, random ids, row digest (`params.py`) | 68.5 B → **29.0 B** | | |

Rows not listed make the same requests as before and moved within
run-to-run noise (the largest, the 100M base commit at 560 → 720 ms, ran
at 733 ms in K0 on the same code).

- **Planner.** Half-unchanged writes at 10M read the level whole after its
  tails: 20 GETs instead of 453, 180 ms slower, still within the budget —
  fewer requests is what the policy asks for. At 100M the rest of the level
  is 2.7 GB, over the budget, so the blocks stay (542 GETs). In the steady
  state the filters' per-file matches cut the exact reads: 857 → 291 GETs.
  One case got slower: a million keys written into 10M (crossover table)
  now pays the tails and a million filter checks before reading the level
  whole, 8.4 s against 7.0 s reading it whole straight away; the planner
  cannot know that without the filters.
- **Sequential awaits.** Seven level-0 reads are now one round trip; a
  steady page costs 100 ms instead of 330–420; a steady full scan reads 3–5×
  fewer times.
- **Recount reads.** A full scan of a freshly compacted 100M index is 1,077
  GETs (1,000 pages plus 29 index parts plus a few), not the ~150 §6 said;
  with the upper levels full, 3,029.

### Compaction write amplification (`amplification.py`)

The real planner replayed over 100,000 commits of 1K random keys, on file
metadata only (a merge keeps each key's newest entry; files split as
`merge_files` splits them); measured over the second half, once the levels
filled. "ratio R" merges level 0 into level 1 at 1/R of its size instead of
1/10.

| Keys | Policy | Written per byte committed | Level-0 files read per commit | Level-0 MB read per commit | Compaction GETs / PUTs per commit |
|---|---|---|---|---|---|
| 1,000,000 | before | 125.0 | 3.5 | 0.10 | 1.25 / 0.125 |
| 1,000,000 | **now** | **16.3** | 4.0 | 1.48 | 1.15 / 0.141 |
| 1,000,000 | ratio 3 | 27.5 | 4.0 | 5.18 | 1.14 / 0.142 |
| 1,000,000 | ratio 30 | 29.8 | 3.9 | 0.50 | 1.17 / 0.139 |
| 10,000,000 | before | 155.2 | 3.5 | 0.10 | 1.33 / 0.126 |
| 10,000,000 | **now** | **22.3** | 3.9 | 1.80 | 1.16 / 0.142 |
| 10,000,000 | ratio 3 | 36.8 | 4.0 | 6.14 | 1.16 / 0.143 |
| 10,000,000 | ratio 30 | 33.3 | 3.9 | 0.63 | 1.18 / 0.139 |
| 100,000,000 | before | 159.2 | 3.5 | 0.10 | 1.34 / 0.129 |
| 100,000,000 | **now** | **31.0** | 3.9 | 1.81 | 1.18 / 0.144 |
| 100,000,000 | ratio 3 | 42.0 | 4.0 | 5.37 | 1.17 / 0.146 |
| 100,000,000 | ratio 30 | 42.5 | 3.8 | 0.59 | 1.19 / 0.142 |

Why a tenth: level 0 rewrites itself every seven commits, costing about
x / (2 · 7d) per byte for a merged file of up to x and deltas of d; the
merge into level 1 costs L1 / x. The sum is smallest at x = √(14 · d ·
L1), ≈ L1 / 12 for 1K-key deltas (d = 29 KB) and L1 = 64 MB. Both
neighbours measure worse. The price is level 0 read whole on every commit:
1.8 MB instead of 0.1 MB, about 20 ms at 80 MB/s. Before, the ~280× the
follow-up saw was the step at a full level 1; averaged over level 1
filling up it is 125–159×.

### Scenario E, steady state

1K random changes every 10 s into 100M keys, cold, 259,200 commits a month.
Per commit: the steady commit's requests plus compaction's (from the
simulation), plus the attempt and journal overhead ($6.69 a month) and
storage ($0.06):

| | Commit | Compaction per commit | Key index / month | **Total / month** | Time per commit |
|---|---|---|---|---|---|
| before | 58 GET, 1 PUT | 1.34 GET, 0.129 PUT | $7.62 | **$14.37** | 1.5 s |
| now | 50 GET, 1 PUT | 1.18 GET, 0.144 PUT | $6.79 | **$13.54** | 817 ms |

(The follow-up estimated ~$14.60 for "before", amortizing compaction from
its one measured compaction of each kind rather than a long run.)

### Format (`params.py`, 1M entries, row digests now 16 bytes)

| Keys | Versions | Blocks (zlib) | Filters | **Total per entry** | Entries per block |
|---|---|---|---|---|---|
| random ids `cust-%013d` (bench) | row digest | 25.5 B | 3.50 B | **29.0 B** (68.5 B with SHA-256 hex) | 2,439 |
| UUIDs | row digest | 47.8 B | 3.50 B | **51.3 B** (91.4 B) | 1,267 |
| sequential ids `order-%012d` | row digest | 20.2 B | 3.50 B | **23.7 B** (63.1 B) | 3,096 |
| sequential ids `order-%012d` | short revision `%d` | 7.2 B | 3.50 B | **10.7 B** | 5,952 |
| paths `site-…/file-i` (cpu.py) | row digest | 31.8 B | 3.50 B | **35.3 B** (75.5 B) | 1,698 |

Filters are unchanged: 3.5 B per entry, 0.35% false positives per check.

### Full results after the fixes
#### Key index benchmark (native, 30 ms per request, 80 MB/s per connection, 64 in parallel)

| Keys | Build | Build rate | Index size | Per entry (incl. filters) | Filters per entry | Files | Levels | Peak RSS |
|---|---|---|---|---|---|---|---|---|
| 1,000,000 | 1.3 s | 0.75 M keys/s, 22 MB/s | 29.0 MB | 29.0 B | 3.51 B | 1 | 1 | 0.9 GB |
| 10,000,000 | 13.3 s | 0.75 M keys/s, 21 MB/s | 281.4 MB | 28.1 B | 3.51 B | 3 | 2 | 5.0 GB |
| 100,000,000 | 118.0 s | 0.85 M keys/s, 23 MB/s | 2,719.6 MB | 27.2 B | 3.51 B | 29 | 3 | 5.9 GB |

| Operation | 1,000,000 keys | 10,000,000 keys | 100,000,000 keys |
|---|---|---|---|
| 100 random keys changed | 361 ms · 2 GET 0 PUT · 29.0 MB | 251 ms · 3 GET 0 PUT · 35.1 MB | 434 ms · 28 GET 0 PUT · 338.5 MB |
| 1K random keys changed | 507 ms · 2 GET 0 PUT · 29.0 MB | 298 ms · 5 GET 0 PUT · 35.2 MB | 509 ms · 34 GET 0 PUT · 351.2 MB |
| 1K random keys, half unchanged | 477 ms · 2 GET 0 PUT · 29.0 MB | 950 ms · 20 GET 0 PUT · 281.4 MB | 1.0 s · 542 GET 0 PUT · 383.3 MB |
| 1K clustered keys changed | 304 ms · 2 GET 0 PUT · 29.0 MB | 261 ms · 2 GET 0 PUT · 12.5 MB | 229 ms · 1 GET 0 PUT · 12.4 MB |
| 1K new keys inserted | 474 ms · 2 GET 0 PUT · 29.0 MB | 250 ms · 3 GET 0 PUT · 35.1 MB | 458 ms · 29 GET 0 PUT · 350.9 MB |
| 100K random keys changed | 883 ms · 2 GET 0 PUT · 29.0 MB | 1.4 s · 20 GET 0 PUT · 281.4 MB | 1.3 s · 376 GET 0 PUT · 372.7 MB |
| 1K random keys changed, disk cache warm | 164 ms · 0 GET 0 PUT · 0.0 MB | 28 ms · 0 GET 0 PUT · 0.0 MB | 311 ms · 0 GET 0 PUT · 0.0 MB |
| full-delivery page of 10K keys | 84 ms · 2 GET 0 PUT · 0.3 MB | 82 ms · 2 GET 0 PUT · 0.3 MB | 97 ms · 2 GET 0 PUT · 0.3 MB |
| commit: 1K random changes + delta write | 512 ms · 2 GET 1 PUT · 29.0 MB | 332 ms · 6 GET 1 PUT · 35.3 MB | 720 ms · 36 GET 1 PUT · 351.3 MB |
| compaction: 8 delta files | 91 ms · 8 GET 1 PUT · 0.3 MB ↑0.2 MB | 97 ms · 8 GET 1 PUT · 0.3 MB ↑0.2 MB | 87 ms · 8 GET 1 PUT · 0.3 MB ↑0.2 MB |
| full replacement, 1% changed | 762 ms · 2 GET 0 PUT · 29.0 MB | 6.1 s · 17 GET 0 PUT · 281.4 MB | — |
| initial load: every key, unsorted, changes + write | 1.9 s · 0 GET 1 PUT · 0.0 MB ↑28.9 MB | 23.9 s · 0 GET 3 PUT · 0.0 MB ↑279.9 MB | — |
| full scan (recount), 100K-key pages | 1.5 s · 12 GET 0 PUT · 25.5 MB | 14.7 s · 105 GET 0 PUT · 246.4 MB | 122.0 s · 1077 GET 0 PUT · 2369.6 MB |
| full scan (recount), disk cache warm | 698 ms · 0 GET 0 PUT · 0.0 MB | 7.2 s · 0 GET 0 PUT · 0.0 MB | 42.6 s · 0 GET 0 PUT · 0.0 MB |
| steady: 1K random keys changed | 544 ms · 9 GET 0 PUT · 30.7 MB | 500 ms · 13 GET 0 PUT · 46.8 MB | 765 ms · 48 GET 0 PUT · 445.4 MB |
| steady: 1K random keys, half unchanged | 565 ms · 9 GET 0 PUT · 30.7 MB | 1.0 s · 32 GET 0 PUT · 349.9 MB | 1.1 s · 291 GET 0 PUT · 519.3 MB |
| steady: full-delivery page of 10K keys | 108 ms · 9 GET 0 PUT · 2.0 MB | 105 ms · 12 GET 0 PUT · 1.1 MB | 102 ms · 14 GET 0 PUT · 1.4 MB |
| steady: full scan (recount), 100K-key pages | 1.7 s · 19 GET 0 PUT · 27.2 MB | 21.6 s · 219 GET 0 PUT · 306.6 MB | 241.7 s · 3029 GET 0 PUT · 2999.4 MB |
| steady: commit: 1K random changes + delta write | 571 ms · 9 GET 1 PUT · 30.7 MB | 543 ms · 12 GET 1 PUT · 46.7 MB | 817 ms · 50 GET 1 PUT · 445.6 MB |
| steady: compaction: level 0, 8 files | 176 ms · 8 GET 1 PUT · 1.7 MB ↑1.7 MB | 307 ms · 8 GET 1 PUT · 3.7 MB ↑3.5 MB | 272 ms · 8 GET 1 PUT · 3.9 MB ↑3.7 MB |
| steady: compaction: level 0, a tenth of level 1, into level 1 | 1.4 s · 4 GET 1 PUT · 32.2 MB ↑29.0 MB | 2.9 s · 6 GET 1 PUT · 71.9 MB ↑68.5 MB | 3.1 s · 6 GET 1 PUT · 74.5 MB ↑71.8 MB |
| steady: compaction: a level-1 file into level 2 | — | 12.4 s · 22 GET 3 PUT · 349.9 MB ↑281.4 MB | 27.8 s · 45 GET 8 PUT · 721.3 MB ↑702.1 MB |
| steady: compaction: a level-2 file into level 3 | — | — | 5.7 s · 10 GET 2 PUT · 143.8 MB ↑126.0 MB |

Steady-state shape (level: files, MB), before and after the compactions:

| Keys | Before | After |
|---|---|---|
| 1,000,000 | L0: 7, 1.7 MB · L1: 1, 29.0 MB | L1: 1, 29.0 MB |
| 10,000,000 | L0: 7, 3.6 MB · L1: 1, 64.9 MB · L2: 3, 281.4 MB | L2: 3, 281.4 MB |
| 100,000,000 | L0: 7, 3.9 MB · L1: 1, 67.0 MB · L2: 7, 649.6 MB · L3: 29, 2,719.6 MB | L2: 7, 684.3 MB · L3: 29, 2,719.6 MB |

Read strategy, forced each way (cold; wall · GETs · MB read):

| Keys | Written keys | Unchanged | Whole levels | Tails, then blocks | Tails, then rest | Planner picks | Planner's estimate, blocks / rest |
|---|---|---|---|---|---|---|---|
| 1,000,000 | 1,000 | 0% | 437 ms · 2 GET 0 PUT · 29.0 MB | 128 ms · 5 GET 0 PUT · 3.8 MB | 376 ms · 3 GET 0 PUT · 29.0 MB | whole: 457 ms · 2 GET 0 PUT · 29.0 MB | — |
| 1,000,000 | 10,000 | 0% | 589 ms · 2 GET 0 PUT · 29.0 MB | 241 ms · 33 GET 0 PUT · 5.6 MB | 439 ms · 3 GET 0 PUT · 29.0 MB | whole: 513 ms · 2 GET 0 PUT · 29.0 MB | — |
| 1,000,000 | 100,000 | 0% | 792 ms · 2 GET 0 PUT · 29.0 MB | 678 ms · 110 GET 0 PUT · 17.9 MB | 812 ms · 3 GET 0 PUT · 29.0 MB | whole: 747 ms · 2 GET 0 PUT · 29.0 MB | — |
| 1,000,000 | 1,000 | 50% | 451 ms · 2 GET 0 PUT · 29.0 MB | 330 ms · 79 GET 0 PUT · 22.3 MB | 509 ms · 3 GET 0 PUT · 29.0 MB | whole: 455 ms · 2 GET 0 PUT · 29.0 MB | — |
| 1,000,000 | 10,000 | 50% | 493 ms · 2 GET 0 PUT · 29.0 MB | 576 ms · 3 GET 0 PUT · 29.0 MB | 612 ms · 3 GET 0 PUT · 29.0 MB | whole: 481 ms · 2 GET 0 PUT · 29.0 MB | — |
| 1,000,000 | 100,000 | 50% | 749 ms · 2 GET 0 PUT · 29.0 MB | 1.0 s · 3 GET 0 PUT · 29.0 MB | 1.0 s · 3 GET 0 PUT · 29.0 MB | whole: 741 ms · 2 GET 0 PUT · 29.0 MB | — |
| 10,000,000 | 1,000 | 0% | 812 ms · 17 GET 0 PUT · 281.4 MB | 277 ms · 8 GET 0 PUT · 35.4 MB | 640 ms · 20 GET 0 PUT · 281.4 MB | tails, then blocks: 283 ms · 8 GET 0 PUT · 35.4 MB | 221 ms / 713 ms |
| 10,000,000 | 10,000 | 0% | 2.0 s · 17 GET 0 PUT · 281.4 MB | 328 ms · 31 GET 0 PUT · 36.8 MB | 661 ms · 20 GET 0 PUT · 281.4 MB | tails, then rest: 683 ms · 20 GET 0 PUT · 281.4 MB | 259 ms / 748 ms |
| 10,000,000 | 100,000 | 0% | 2.4 s · 17 GET 0 PUT · 281.4 MB | 960 ms · 311 GET 0 PUT · 57.0 MB | 1.2 s · 20 GET 0 PUT · 281.4 MB | tails, then rest: 1.1 s · 20 GET 0 PUT · 281.4 MB | 827 ms / 1.2 s |
| 10,000,000 | 1,000,000 | 0% | 7.0 s · 17 GET 0 PUT · 281.4 MB | 8.4 s · 975 GET 0 PUT · 178.5 MB | 9.0 s · 20 GET 0 PUT · 281.4 MB | tails, then rest: 8.4 s · 20 GET 0 PUT · 281.4 MB | 4.7 s / 4.5 s |
| 10,000,000 | 1,000 | 50% | 815 ms · 17 GET 0 PUT · 281.4 MB | 682 ms · 411 GET 0 PUT · 63.6 MB | 852 ms · 20 GET 0 PUT · 281.4 MB | tails, then rest: 844 ms · 20 GET 0 PUT · 281.4 MB | 712 ms / 968 ms |
| 10,000,000 | 10,000 | 50% | 2.0 s · 17 GET 0 PUT · 281.4 MB | 2.0 s · 789 GET 0 PUT · 212.0 MB | 2.0 s · 20 GET 0 PUT · 281.4 MB | tails, then rest: 1.9 s · 20 GET 0 PUT · 281.4 MB | 2.6 s / 2.3 s |
| 10,000,000 | 100,000 | 50% | 2.5 s · 17 GET 0 PUT · 281.4 MB | 2.7 s · 20 GET 0 PUT · 281.4 MB | 2.8 s · 20 GET 0 PUT · 281.4 MB | tails, then blocks: 2.8 s · 20 GET 0 PUT · 281.4 MB | 3.2 s / 3.2 s |
| 100,000,000 | 1,000 | 0% | 2.8 s · 170 GET 0 PUT · 2719.6 MB | 567 ms · 30 GET 0 PUT · 350.9 MB | 823 ms · 34 GET 0 PUT · 434.5 MB | tails, then blocks: 527 ms · 30 GET 0 PUT · 350.9 MB | 766 ms / 975 ms |
| 100,000,000 | 10,000 | 0% | 6.3 s · 170 GET 0 PUT · 2719.6 MB | 580 ms · 63 GET 0 PUT · 353.0 MB | 1.5 s · 121 GET 0 PUT · 1883.0 MB | tails, then blocks: 525 ms · 63 GET 0 PUT · 353.0 MB | 811 ms / 3.9 s |
| 100,000,000 | 100,000 | 0% | 17.7 s · 170 GET 0 PUT · 2719.6 MB | 1.2 s · 372 GET 0 PUT · 372.5 MB | 2.5 s · 171 GET 0 PUT · 2719.6 MB | tails, then blocks: 1.1 s · 372 GET 0 PUT · 372.5 MB | 1.4 s / 6.0 s |
| 100,000,000 | 1,000,000 | 0% | 22.2 s · 170 GET 0 PUT · 2719.6 MB | 8.9 s · 3080 GET 0 PUT · 558.2 MB | 8.6 s · 171 GET 0 PUT · 2719.6 MB | tails, then blocks: 9.3 s · 3080 GET 0 PUT · 558.2 MB | 7.0 s / 10.0 s |
| 100,000,000 | 1,000 | 50% | 2.5 s · 170 GET 0 PUT · 2719.6 MB | 901 ms · 515 GET 0 PUT · 381.6 MB | 2.3 s · 171 GET 0 PUT · 2719.6 MB | tails, then blocks: 929 ms · 515 GET 0 PUT · 381.6 MB | 1.3 s / 5.9 s |
| 100,000,000 | 10,000 | 50% | 5.9 s · 170 GET 0 PUT · 2719.6 MB | 7.3 s · 4167 GET 0 PUT · 645.6 MB | 5.3 s · 171 GET 0 PUT · 2719.6 MB | tails, then blocks: 5.1 s · 4167 GET 0 PUT · 645.6 MB | 6.1 s / 8.3 s |
| 100,000,000 | 100,000 | 50% | 17.5 s · 170 GET 0 PUT · 2719.6 MB | 17.4 s · 7395 GET 0 PUT · 2105.9 MB | 14.5 s · 171 GET 0 PUT · 2719.6 MB | tails, then rest: 16.0 s · 171 GET 0 PUT · 2719.6 MB | 24.4 s / 22.3 s |

## Streaming bulk operations (2026-10-01)

Finding 6 of the follow-up above: a 100M-key full replacement needed ~45 GB,
because every key crossed into the index as Python objects, and every block
of every level was loaded at once. Full replacement is now a streaming
merge-join in Rust, and compaction and recount stream too
(`object-store-state.md` §6). Same machine (8 cores, 31 GB), local MinIO
with 30 ms per request and 80 MB/s per connection, 64 requests in
parallel. Nothing here ran on real S3.

    cargo run --release --example sort -- 1e8 ids        # in native/; also uuids, paths
    uv run python bench/keys/bulk.py --s3 http://solera:solera-bench-secret@127.0.0.1:9100/solera-test --sizes 1e6,1e7,1e8

### Sorting the written keys (`native/examples/sort.rs`)

Keys in one packed buffer with `u32` offsets, as in an Arrow string column,
in random order. Time, and the heap each strategy adds at its peak per key
(a counting allocator), on top of the keys. Every strategy returns the same
`u32` permutation; the pair strategies compact their pairs into it in place.
Pairs hold the 8 bytes after the prefix every key shares (5 bytes for the
ids and paths) and break ties through the keys.

random ids `cust-%013d`:

| Strategy | 1M | 10M | 100M |
|---|---|---|---|
| bare `u32` permutation | 240 ms · 4.0 B | 5.5 s · 4.0 B | 96.2 s · 4.0 B |
| bare permutation, every core | 80 ms · 4.1 B | 1.1 s · 4.0 B | 17.7 s · 4.0 B |
| (8-byte prefix, `u32`) pairs | 70 ms · 12.1 B | 790 ms · 12.0 B | 45.0 s · 12.0 B |
| pairs, every core | 30 ms · 12.1 B | 210 ms · 12.0 B | 6.2 s · 12.0 B |
| pairs, LSD radix + tie fix-up | 40 ms · 24.1 B | 470 ms · 24.0 B | 41.9 s · 24.0 B |
| pairs, one in-place MSD pass, buckets on every core | 80 ms · 12.1 B | 430 ms · 12.0 B | 44.2 s · 12.0 B |
| **buckets of a permutation, each as pairs, every core (chosen)** | 90 ms · 8.8 B | 210 ms · 5.7 B | 4.9 s · 4.6 B |

UUIDs:

| Strategy | 1M | 10M | 100M |
|---|---|---|---|
| bare `u32` permutation | 270 ms · 4.0 B | 6.4 s · 4.0 B | 95.5 s · 4.0 B |
| bare permutation, every core | 110 ms · 4.1 B | 1.2 s · 4.0 B | 18.1 s · 4.0 B |
| (8-byte prefix, `u32`) pairs | 70 ms · 12.1 B | 640 ms · 12.0 B | 7.5 s · 12.0 B |
| pairs, every core | 70 ms · 12.1 B | 170 ms · 12.0 B | 1.5 s · 12.0 B |
| pairs, LSD radix + tie fix-up | 60 ms · 24.1 B | 350 ms · 24.0 B | 3.8 s · 24.0 B |
| pairs, one in-place MSD pass, buckets on every core | 40 ms · 12.1 B | 260 ms · 12.0 B | 2.5 s · 12.0 B |
| **buckets of a permutation, each as pairs, every core (chosen)** | 30 ms · 8.8 B | 190 ms · 5.7 B | 1.5 s · 4.4 B |

paths `site-%05d/file-%09d`:

| Strategy | 1M | 10M | 100M |
|---|---|---|---|
| bare `u32` permutation | 270 ms · 4.0 B | 6.0 s · 4.0 B | 92.2 s · 4.0 B |
| bare permutation, every core | 90 ms · 4.1 B | 1.2 s · 4.0 B | 19.4 s · 4.0 B |
| (8-byte prefix, `u32`) pairs | 220 ms · 12.1 B | 3.9 s · 12.0 B | 60.3 s · 12.0 B |
| pairs, every core | 60 ms · 12.1 B | 590 ms · 12.0 B | 9.1 s · 12.0 B |
| pairs, LSD radix + tie fix-up | 140 ms · 24.1 B | 2.6 s · 24.0 B | 37.1 s · 24.0 B |
| pairs, one in-place MSD pass, buckets on every core | 260 ms · 12.1 B | 3.9 s · 12.0 B | 59.6 s · 12.0 B |
| **buckets of a permutation, each as pairs, every core (chosen)** | 40 ms · 9.4 B | 930 ms · 5.7 B | 6.1 s · 4.4 B |

A bare permutation is the least memory but compares through the offsets,
missing cache on every comparison: 18–19 s at 100M on every core. Prefix
pairs settle most comparisons on their own, but cost 12 B per key, and
collapse when keys share more than their common prefix — ids whose next 8
bytes are mostly the same digits, paths with `/file-` in the middle. The
chosen strategy scatters a permutation into 65,536 buckets by the two bytes
after the common prefix, sorts each bucket as pairs on every core, then
re-buckets ties by the next 8 bytes (`sort_deep`); a bucket over 1/64 of
the rows splits again by its next two bytes instead of becoming pairs. It
is fastest or close at every size and shape, and peaks at the permutation
plus the pairs of the buckets in flight: 4.4–4.6 B per key at 100M (the
fixed 65,536-bucket histograms show at 1M).

An earlier round of this benchmark freed one small allocation per key just
before timing; glibc's cleanup of them on the next large allocation was
billed to whichever strategy ran first, inflating it up to 3×. Keys are now
generated in two large buffers.

### Bulk operations (`bulk.py`)

Each operation ran in a process of its own after its input existed, and
reports the peak resident memory it added on top of that input (the
kernel's peak counter reset first) — what a worker needs beyond the data it
already holds. The index: every key in level 1, plus a delta in level 0
changing 1% of versions; the replacement writes every key back at its first
version, so the 1% change again. Keys `cust-%013d` with random gaps,
versions the MD5 of the key. `list` input is Python `bytes` keys with a
version function in Python (MD5: about a row digest's cost); `arrow` is a
pyarrow Table read in place. Rows arrive shuffled unless sorted.

Before: the code as of the follow-up above (`KeyIndex.changes(..., replace=True)`
and `write`; compaction through whole files), same script. It cannot run at
100M on this machine.

| Operation | 1M keys, before | 1M keys, after | 10M keys, before | 10M keys, after | 100M keys, after |
|---|---|---|---|---|---|
| initial load, `list` | 3.5 s · **0.18 GB** | 1.7 s · **0.11 GB** | 40.8 s · **1.13 GB** | 11.4 s · **0.57 GB** | 136.5 s · **3.35 GB** |
| initial load, `arrow` | — | 874 ms · **0.09 GB** | — | 4.2 s · **0.36 GB** | 37.6 s · **0.81 GB** |
| initial load, `arrow`, sorted | — | 835 ms · **0.08 GB** | — | 3.6 s · **0.32 GB** | 30.1 s · **0.37 GB** |
| full replacement, 1% changed, `list` | 1.8 s · **0.32 GB** | 1.1 s · **0.11 GB** | 20.1 s · **2.86 GB** | 10.2 s · **0.44 GB** | 139.3 s · **3.35 GB** |
| full replacement, 1% changed, `arrow` | — | 562 ms · **0.05 GB** | — | 2.8 s · **0.18 GB** | 27.6 s · **0.83 GB** |
| compaction: delta + all of level 1 into level 1 | 1.5 s · **0.23 GB** | 1.1 s · **0.12 GB** | 12.9 s · **1.13 GB** | 3.8 s · **0.45 GB** | 27.1 s · **0.54 GB** |
| recount | 1.4 s · **0.06 GB** | 315 ms · **0.05 GB** | 13.8 s · **0.09 GB** | 1.7 s · **0.14 GB** | 14.1 s · **0.20 GB** |

Requests (after): a replacement, compaction or recount reads the index in
8 MB segments — 6, 40 and 371 GETs (26 MB, 251 MB, 2.4 GB) at 1M, 10M and
100M; the recount was 1,077 GETs and 118.8 s at 100M in the follow-up. The
delta of a 1% replacement is one PUT at every size (27 MB at 100M); a load
or a compaction writes a file per ~66 MB (41 at 100M).

Where the memory goes at 100M:

- **Sorting**: the permutation, 0.4 GB, plus a few buckets' pairs while it
  sorts — the difference between shuffled and sorted Arrow input (0.81 and
  0.37 GB).
- **Python keys**: packed once into one buffer, 26 B per key here (18 B of
  key, an 8-byte offset): 2.6 GB of the `list` cases' 3.35 GB. Arrow keys
  stay in place.
- **Buffers**, whatever the size: the file being written (up to 64 MB of
  blocks plus 32 B of filter hashes per entry, ~75 MB), two files
  uploading, and per level three segments read ahead plus 32 decoded blocks
  — ~0.35 GB, the whole of a sorted load and most of a compaction or recount.

The `list` cases' time is the version function: 100M calls of Python MD5 run
one window of rows at a time under the GIL, about 1 µs each. Arrow versions
— a revision column or a row digest — are read or computed on every core.
The old figure of ~45 GB for a 100M-key replacement also counted the worker
building `key_map` (a dict of every key and its digest) and the lists it
passed in; the worker now hands its rows to `key_rows` directly.

## One row digest for Python and Arrow (2026-10-01)

Versions are now the canonical digest of `docs/row-digest.md`: one grammar,
in Rust, fed from Python values and from Arrow arrays, every key the group
of rows that carry it. Before, a Python row was BLAKE2b-128 of its JSON — or
of its pickle, for anything JSON cannot hold, a `datetime` included —
computed by a Python function the native join called a row at a time;
an Arrow row was XXH3-128 of a different encoding.

    uv run python bench/keys/digest.py --sizes 1e6,1e7

Each case in a process of its own: its rows built first, then — measured —
`key_rows` and an initial load into an empty index on local disk, so every
version is computed once. Peak memory is above the rows. Rows: a key, an
integer, a float, a string, a list of two strings and a timestamp; `flat`
leaves the timestamp out (JSON values only, the old digest's fast path).
Same machine, before and after back to back.

| Rows | 1M, before | 1M, after | 10M, before | 10M, after |
|---|---|---|---|---|
| `list[dict]` | 7.3 s · 0.10 GB | **3.7 s** · 0.10 GB | 71.3 s · 0.51 GB | **35.8 s** · 0.51 GB |
| `list[dict]`, flat | 6.9 s · 0.10 GB | **2.1 s** · 0.10 GB | 67.8 s · 0.51 GB | **24.0 s** · 0.51 GB |
| Arrow table | 0.6 s · 0.11 GB | 0.7 s · 0.10 GB | 4.8 s · 0.32 GB | 5.5 s · 0.32 GB |

Python rows digest 2–3× faster: the walk is native (still under the GIL,
one window of rows at a time), with no JSON round trip and no pickle. A
flat row costs ~0.9 µs and a `datetime` field ~1 µs more (its attributes
are read through Python). Arrow rows cost 14% more than before: records
are framed and their fields sorted per row. Memory is the same.
