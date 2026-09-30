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
