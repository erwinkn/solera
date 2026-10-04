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
| full-pass batch of 10K keys | 80 ms · 2 GET 0 PUT · 0.3 MB | 81 ms · 2 GET 0 PUT · 0.3 MB | 83 ms · 2 GET 0 PUT · 0.3 MB |
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
| full-pass batch of 10K keys | 80 ms · 2 GET 0 PUT · 0.3 MB | 80 ms · 2 GET 0 PUT · 0.3 MB | 83 ms · 2 GET 0 PUT · 0.3 MB |
| commit: 1K random changes + delta write | 491 ms · 2 GET 1 PUT · 29.0 MB | 312 ms · 6 GET 1 PUT · 35.3 MB | 560 ms · 36 GET 1 PUT · 351.3 MB |
| compaction: 8 delta files into level 1 | 1.4 s · 10 GET 1 PUT · 29.3 MB ↑29.0 MB | 91 ms · 8 GET 1 PUT · 0.3 MB ↑0.2 MB | 90 ms · 8 GET 1 PUT · 0.3 MB ↑0.2 MB |
| full replacement, 1% changed | 728 ms · 2 GET 0 PUT · 29.0 MB | 6.9 s · 17 GET 0 PUT · 281.4 MB | — |
| initial load: every key, unsorted, changes + write | 1.8 s · 0 GET 1 PUT · 0.0 MB ↑28.9 MB | 22.5 s · 0 GET 3 PUT · 0.0 MB ↑279.9 MB | — |
| full scan (recount), 100K-key pages | 1.4 s · 12 GET 0 PUT · 26.1 MB | 14.1 s · 105 GET 0 PUT · 252.6 MB | 118.8 s · 1077 GET 0 PUT · 2433.3 MB |
| steady: 1K random keys changed | 766 ms · 9 GET 0 PUT · 29.2 MB | 1.2 s · 17 GET 0 PUT · 100.4 MB | 1.5 s · 65 GET 0 PUT · 501.5 MB |
| steady: 1K random keys, half unchanged | 762 ms · 9 GET 0 PUT · 29.2 MB | 1.5 s · 364 GET 0 PUT · 124.1 MB | 2.3 s · 857 GET 0 PUT · 552.9 MB |
| steady: full-pass batch of 10K keys | 334 ms · 16 GET 0 PUT · 0.5 MB | 359 ms · 18 GET 0 PUT · 0.8 MB | 421 ms · 20 GET 0 PUT · 1.1 MB |
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
| full-pass batch of 10K keys | 89 ms · 2 GET 0 PUT · 0.3 MB |
| commit: 1K random changes + delta write | 1.2 s · 2 GET 1 PUT · 27.1 MB |
| compaction: 8 delta files into level 1 | 8.3 s · 10 GET 1 PUT · 27.3 MB ↑27.1 MB |
| full replacement, 1% changed | 1.8 s · 2 GET 0 PUT · 27.1 MB |
| initial load: every key, unsorted, changes + write | 8.5 s · 0 GET 1 PUT · 0.0 MB ↑26.9 MB |
| full scan (recount), 100K-key pages | 2.2 s · 12 GET 0 PUT · 24.1 MB |
| steady: 1K random keys changed | 1.5 s · 9 GET 0 PUT · 27.3 MB |
| steady: 1K random keys, half unchanged | 1.4 s · 9 GET 0 PUT · 27.3 MB |
| steady: full-pass batch of 10K keys | 371 ms · 16 GET 0 PUT · 0.5 MB |
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
| steady: full-pass batch of 10K keys | 331 ms · 16 GET → **108 ms · 9 GET** | 392 ms · 18 GET → **105 ms · 12 GET** | 421 ms · 20 GET → **102 ms · 14 GET** |
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
| full-pass batch of 10K keys | 84 ms · 2 GET 0 PUT · 0.3 MB | 82 ms · 2 GET 0 PUT · 0.3 MB | 97 ms · 2 GET 0 PUT · 0.3 MB |
| commit: 1K random changes + delta write | 512 ms · 2 GET 1 PUT · 29.0 MB | 332 ms · 6 GET 1 PUT · 35.3 MB | 720 ms · 36 GET 1 PUT · 351.3 MB |
| compaction: 8 delta files | 91 ms · 8 GET 1 PUT · 0.3 MB ↑0.2 MB | 97 ms · 8 GET 1 PUT · 0.3 MB ↑0.2 MB | 87 ms · 8 GET 1 PUT · 0.3 MB ↑0.2 MB |
| full replacement, 1% changed | 762 ms · 2 GET 0 PUT · 29.0 MB | 6.1 s · 17 GET 0 PUT · 281.4 MB | — |
| initial load: every key, unsorted, changes + write | 1.9 s · 0 GET 1 PUT · 0.0 MB ↑28.9 MB | 23.9 s · 0 GET 3 PUT · 0.0 MB ↑279.9 MB | — |
| full scan (recount), 100K-key pages | 1.5 s · 12 GET 0 PUT · 25.5 MB | 14.7 s · 105 GET 0 PUT · 246.4 MB | 122.0 s · 1077 GET 0 PUT · 2369.6 MB |
| full scan (recount), disk cache warm | 698 ms · 0 GET 0 PUT · 0.0 MB | 7.2 s · 0 GET 0 PUT · 0.0 MB | 42.6 s · 0 GET 0 PUT · 0.0 MB |
| steady: 1K random keys changed | 544 ms · 9 GET 0 PUT · 30.7 MB | 500 ms · 13 GET 0 PUT · 46.8 MB | 765 ms · 48 GET 0 PUT · 445.4 MB |
| steady: 1K random keys, half unchanged | 565 ms · 9 GET 0 PUT · 30.7 MB | 1.0 s · 32 GET 0 PUT · 349.9 MB | 1.1 s · 291 GET 0 PUT · 519.3 MB |
| steady: full-pass batch of 10K keys | 108 ms · 9 GET 0 PUT · 2.0 MB | 105 ms · 12 GET 0 PUT · 1.1 MB | 102 ms · 14 GET 0 PUT · 1.4 MB |
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

    uv run python bench/keys/bulk.py --s3 http://solera:solera-bench-secret@127.0.0.1:9100/solera-test --sizes 1e6,1e7,1e8

### Sorting the written keys

`native/examples/sort.rs` at `f198382`; only the chosen strategy stayed in
`native/src/sort.rs` (`cargo run --release --example sort -- 1e8 ids` at
that commit reruns the comparison).

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

## The cold resolver: sparse or streamed (2026-10-02)

`docs/resolved-commits.md` §6 replaced the read planner with two readers
and two switch rules: the sparse reader (tails, filters, then blocks of the
keys the filters cannot clear) for small patches, and a streaming
merge-join of the patch with every level (`Job.patch`, native) for dense
ones. Every route below writes its delta, so the requests are the whole
cost of resolving. Same machine and setup as above: 8 cores, local MinIO,
30 ms per request, 80 MB/s per connection, 64 in parallel, native.

    uv run python bench/keys/bench.py --s3 http://solera:solera-bench-secret@127.0.0.1:9100/solera-test --sizes 1e6 --suites crossover,steady
    uv run python bench/keys/bench.py --s3 http://solera:solera-bench-secret@127.0.0.1:9100/solera-test --sizes 1e7,1e8 --suites crossover,steady

"Index": fresh is one bottom level; steady has its upper levels filled as
steady-state writes leave them (§ the steady suite above). CPU is the
process's, all threads; peak RSS is what the operation added. `resolve`
picks with the final thresholds (`stream_density` 2%, `stream_reads` 16).
The 10M and 100M rows are a second run, made with those defaults; the
first run, which set them from the forced columns, picked with 5% and 4.

| Keys | Index | Patch | Unchanged | Sparse | Stream | `resolve` picks |
|---|---|---|---|---|---|---|
| 1,000,000 | fresh | 1,000 | 0% | 490 ms · 2 GET · 29.2 MB · 222 ms · 0.09 GB | 353 ms · 5 GET · 25.7 MB · 438 ms · 0.02 GB | sparse: 432 ms |
| 1,000,000 | fresh | 1,000 | 50% | 452 ms · 2 GET · 29.2 MB · 154 ms · 0.00 GB | 333 ms · 5 GET · 25.7 MB · 340 ms · 0.01 GB | sparse: 427 ms |
| 1,000,000 | fresh | 1,000 | 100% | 425 ms · 2 GET · 29.2 MB · 168 ms · 0.00 GB | 257 ms · 5 GET · 25.7 MB · 273 ms · 0.00 GB | sparse: 399 ms |
| 1,000,000 | fresh | 10,000 | 0% | 467 ms · 2 GET · 29.2 MB · 177 ms · 0.02 GB | 390 ms · 5 GET · 25.7 MB · 397 ms · 0.01 GB | sparse: 488 ms |
| 1,000,000 | fresh | 10,000 | 50% | 463 ms · 2 GET · 29.2 MB · 187 ms · 0.02 GB | 316 ms · 5 GET · 25.7 MB · 227 ms · 0.01 GB | sparse: 484 ms |
| 1,000,000 | fresh | 10,000 | 100% | 457 ms · 2 GET · 29.2 MB · 210 ms · 0.01 GB | 300 ms · 5 GET · 25.7 MB · 426 ms · 0.00 GB | sparse: 450 ms |
| 1,000,000 | fresh | 100,000 | 0% | 818 ms · 2 GET · 29.2 MB · 536 ms · 0.02 GB | 553 ms · 5 GET · 25.7 MB · 562 ms · 0.02 GB | stream: 535 ms |
| 1,000,000 | fresh | 100,000 | 50% | 783 ms · 2 GET · 29.2 MB · 477 ms · 0.01 GB | 444 ms · 5 GET · 25.7 MB · 378 ms · 0.00 GB | stream: 464 ms |
| 1,000,000 | fresh | 100,000 | 100% | 615 ms · 2 GET · 29.2 MB · 359 ms · 0.00 GB | 367 ms · 5 GET · 25.7 MB · 283 ms · 0.01 GB | stream: 372 ms |
| 1,000,000 | steady | 1,000 | 0% | 434 ms · 9 GET · 31.1 MB · 158 ms · 0.01 GB | 426 ms · 12 GET · 27.6 MB · 399 ms · 0.01 GB | sparse: 423 ms |
| 1,000,000 | steady | 1,000 | 50% | 428 ms · 9 GET · 31.1 MB · 148 ms · 0.00 GB | 428 ms · 12 GET · 27.6 MB · 454 ms · 0.00 GB | sparse: 429 ms |
| 1,000,000 | steady | 1,000 | 100% | 391 ms · 9 GET · 31.1 MB · 131 ms · 0.00 GB | 445 ms · 12 GET · 27.6 MB · 511 ms · 0.00 GB | sparse: 393 ms |
| 1,000,000 | steady | 10,000 | 0% | 544 ms · 9 GET · 31.1 MB · 246 ms · 0.01 GB | 507 ms · 12 GET · 27.6 MB · 514 ms · 0.00 GB | sparse: 549 ms |
| 1,000,000 | steady | 10,000 | 50% | 503 ms · 9 GET · 31.1 MB · 211 ms · 0.00 GB | 467 ms · 12 GET · 27.6 MB · 518 ms · 0.00 GB | sparse: 534 ms |
| 1,000,000 | steady | 10,000 | 100% | 461 ms · 9 GET · 31.1 MB · 207 ms · 0.00 GB | 445 ms · 12 GET · 27.6 MB · 509 ms · 0.00 GB | sparse: 473 ms |
| 1,000,000 | steady | 100,000 | 0% | 1.0 s · 9 GET · 31.1 MB · 684 ms · 0.03 GB | 535 ms · 12 GET · 27.6 MB · 431 ms · 0.02 GB | stream: 611 ms |
| 1,000,000 | steady | 100,000 | 50% | 1.0 s · 9 GET · 31.1 MB · 729 ms · 0.01 GB | 558 ms · 12 GET · 27.6 MB · 570 ms · 0.00 GB | stream: 622 ms |
| 1,000,000 | steady | 100,000 | 100% | 859 ms · 9 GET · 31.1 MB · 604 ms · 0.00 GB | 496 ms · 12 GET · 27.6 MB · 586 ms · 0.00 GB | stream: 469 ms |
| 10,000,000 | fresh | 1,000 | 0% | 311 ms · 8 GET · 35.4 MB · 72 ms · 0.04 GB | 1.5 s · 34 GET · 248.8 MB · 3.4 s · 0.08 GB | sparse: 296 ms |
| 10,000,000 | fresh | 1,000 | 50% | 716 ms · 429 GET · 64.1 MB · 365 ms · 0.01 GB | 1.4 s · 34 GET · 248.8 MB · 3.4 s · 0.02 GB | sparse: 674 ms |
| 10,000,000 | fresh | 1,000 | 100% | 963 ms · 694 GET · 89.2 MB · 606 ms · 0.01 GB | 1.4 s · 34 GET · 248.8 MB · 3.0 s · 0.01 GB | stream: 1.6 s |
| 10,000,000 | fresh | 10,000 | 0% | 379 ms · 33 GET · 36.9 MB · 120 ms · 0.03 GB | 1.5 s · 34 GET · 248.8 MB · 2.6 s · 0.00 GB | sparse: 375 ms |
| 10,000,000 | fresh | 10,000 | 50% | 2.0 s · 831 GET · 211.3 MB · 1.2 s · 0.04 GB | 1.4 s · 34 GET · 248.8 MB · 2.5 s · 0.02 GB | stream: 1.7 s |
| 10,000,000 | fresh | 10,000 | 100% | 1.5 s · 353 GET · 260.5 MB · 1.4 s · 0.05 GB | 1.5 s · 34 GET · 248.8 MB · 2.6 s · 0.01 GB | stream: 1.7 s |
| 10,000,000 | fresh | 100,000 | 0% | 947 ms · 269 GET · 52.5 MB · 634 ms · 0.00 GB | 1.5 s · 34 GET · 248.8 MB · 2.6 s · 0.02 GB | sparse: 912 ms |
| 10,000,000 | fresh | 100,000 | 50% | 2.2 s · 20 GET · 283.8 MB · 1.9 s · 0.37 GB | 1.5 s · 34 GET · 248.8 MB · 2.6 s · 0.00 GB | stream: 2.0 s |
| 10,000,000 | fresh | 100,000 | 100% | 2.3 s · 20 GET · 283.8 MB · 1.9 s · 0.24 GB | 1.5 s · 34 GET · 248.8 MB · 3.0 s · 0.00 GB | stream: 2.0 s |
| 10,000,000 | fresh | 1,000,000 | 0% | 7.7 s · 999 GET · 178.1 MB · 7.1 s · 0.22 GB | 2.8 s · 34 GET · 248.8 MB · 3.5 s · 0.18 GB | stream: 2.9 s |
| 10,000,000 | fresh | 1,000,000 | 50% | 8.5 s · 20 GET · 283.8 MB · 8.0 s · 0.38 GB | 2.8 s · 34 GET · 248.8 MB · 4.1 s · 0.09 GB | stream: 2.8 s |
| 10,000,000 | fresh | 1,000,000 | 100% | 8.8 s · 20 GET · 283.8 MB · 8.4 s · 0.44 GB | 2.2 s · 34 GET · 248.8 MB · 3.8 s · 0.00 GB | stream: 2.3 s |
| 10,000,000 | steady | 1,000 | 0% | 399 ms · 15 GET · 46.9 MB · 73 ms · 0.00 GB | 2.3 s · 49 GET · 309.5 MB · 4.1 s · 0.01 GB | sparse: 414 ms |
| 10,000,000 | steady | 1,000 | 50% | 673 ms · 253 GET · 62.0 MB · 283 ms · 0.00 GB | 2.5 s · 49 GET · 309.5 MB · 4.3 s · 0.02 GB | sparse: 684 ms |
| 10,000,000 | steady | 1,000 | 100% | 831 ms · 458 GET · 77.5 MB · 366 ms · 0.00 GB | 2.3 s · 49 GET · 309.5 MB · 3.8 s · 0.01 GB | sparse: 861 ms |
| 10,000,000 | steady | 10,000 | 0% | 468 ms · 33 GET · 48.0 MB · 118 ms · 0.00 GB | 2.4 s · 49 GET · 309.5 MB · 3.9 s · 0.00 GB | sparse: 506 ms |
| 10,000,000 | steady | 10,000 | 50% | 1.7 s · 1201 GET · 170.3 MB · 1.1 s · 0.00 GB | 2.3 s · 49 GET · 309.5 MB · 4.7 s · 0.00 GB | stream: 3.1 s |
| 10,000,000 | steady | 10,000 | 100% | 2.0 s · 1188 GET · 244.3 MB · 1.5 s · 0.03 GB | 2.4 s · 49 GET · 309.5 MB · 4.4 s · 0.00 GB | stream: 3.0 s |
| 10,000,000 | steady | 100,000 | 0% | 1.3 s · 202 GET · 58.7 MB · 843 ms · 0.00 GB | 2.8 s · 49 GET · 309.5 MB · 4.2 s · 0.02 GB | sparse: 1.3 s |
| 10,000,000 | steady | 100,000 | 50% | 2.9 s · 49 GET · 350.7 MB · 2.4 s · 0.25 GB | 2.6 s · 49 GET · 309.5 MB · 4.3 s · 0.04 GB | stream: 3.3 s |
| 10,000,000 | steady | 100,000 | 100% | 2.8 s · 32 GET · 352.4 MB · 2.4 s · 0.29 GB | 2.6 s · 49 GET · 309.5 MB · 4.4 s · 0.00 GB | stream: 3.3 s |
| 100,000,000 | fresh | 1,000 | 0% | 461 ms · 30 GET · 350.9 MB · 332 ms · 0.26 GB | 12.5 s · 341 GET · 2394.0 MB · 17.6 s · 0.02 GB | sparse: 489 ms |
| 100,000,000 | fresh | 1,000 | 50% | 896 ms · 534 GET · 381.8 MB · 619 ms · 0.32 GB | 12.5 s · 341 GET · 2394.0 MB · 18.7 s · 0.02 GB | sparse: 911 ms |
| 100,000,000 | fresh | 1,000 | 100% | 1.3 s · 1001 GET · 411.0 MB · 984 ms · 0.45 GB | 12.6 s · 341 GET · 2394.0 MB · 18.1 s · 0.02 GB | sparse: 1.6 s |
| 100,000,000 | fresh | 10,000 | 0% | 657 ms · 77 GET · 353.8 MB · 350 ms · 0.31 GB | 15.4 s · 341 GET · 2394.0 MB · 23.3 s · 0.00 GB | sparse: 550 ms |
| 100,000,000 | fresh | 10,000 | 50% | 4.1 s · 4145 GET · 635.0 MB · 3.1 s · 0.28 GB | 12.5 s · 341 GET · 2394.0 MB · 23.1 s · 0.00 GB | sparse: 4.1 s |
| 100,000,000 | fresh | 10,000 | 100% | 6.8 s · 6811 GET · 886.9 MB · 5.1 s · 0.01 GB | 12.5 s · 341 GET · 2394.0 MB · 22.5 s · 0.01 GB | stream: 12.9 s |
| 100,000,000 | fresh | 100,000 | 0% | 1.2 s · 372 GET · 371.9 MB · 948 ms · 0.20 GB | 12.5 s · 341 GET · 2394.0 MB · 23.7 s · 0.00 GB | sparse: 1.1 s |
| 100,000,000 | fresh | 100,000 | 50% | 13.5 s · 7918 GET · 2089.9 MB · 12.1 s · 0.04 GB | 12.7 s · 341 GET · 2394.0 MB · 23.8 s · 0.00 GB | stream: 13.1 s |
| 100,000,000 | fresh | 100,000 | 100% | 13.1 s · 2734 GET · 2568.1 MB · 12.9 s · 0.20 GB | 12.8 s · 341 GET · 2394.0 MB · 21.9 s · 0.00 GB | stream: 13.1 s |
| 100,000,000 | fresh | 1,000,000 | 0% | 10.0 s · 3093 GET · 555.1 MB · 9.3 s · 0.18 GB | 14.4 s · 341 GET · 2394.0 MB · 31.1 s · 0.09 GB | sparse: 9.7 s |
| 100,000,000 | fresh | 1,000,000 | 50% | 19.1 s · 199 GET · 2744.0 MB · 19.7 s · 1.75 GB | 13.8 s · 341 GET · 2394.0 MB · 24.7 s · 0.00 GB | stream: 18.6 s |
| 100,000,000 | fresh | 1,000,000 | 100% | 19.6 s · 199 GET · 2744.0 MB · 20.0 s · 1.67 GB | 13.7 s · 341 GET · 2394.0 MB · 22.8 s · 0.00 GB | stream: 18.7 s |
| 100,000,000 | steady | 1,000 | 0% | 618 ms · 46 GET · 444.6 MB · 475 ms · 0.32 GB | 23.3 s · 436 GET · 3025.2 MB · 46.7 s · 0.04 GB | sparse: 588 ms |
| 100,000,000 | steady | 1,000 | 50% | 782 ms · 281 GET · 458.9 MB · 594 ms · 0.32 GB | 22.8 s · 436 GET · 3025.2 MB · 44.3 s · 0.00 GB | sparse: 842 ms |
| 100,000,000 | steady | 1,000 | 100% | 1.1 s · 532 GET · 474.1 MB · 827 ms · 0.28 GB | 22.7 s · 436 GET · 3025.2 MB · 44.6 s · 0.00 GB | sparse: 955 ms |
| 100,000,000 | steady | 10,000 | 0% | 732 ms · 83 GET · 446.9 MB · 594 ms · 0.30 GB | 21.5 s · 436 GET · 3025.2 MB · 39.3 s · 0.01 GB | sparse: 718 ms |
| 100,000,000 | steady | 10,000 | 50% | 2.9 s · 2305 GET · 588.6 MB · 1.9 s · 0.30 GB | 21.2 s · 436 GET · 3025.2 MB · 39.2 s · 0.04 GB | sparse: 2.7 s |
| 100,000,000 | steady | 10,000 | 100% | 4.2 s · 4197 GET · 721.6 MB · 3.1 s · 0.36 GB | 21.3 s · 436 GET · 3025.2 MB · 39.4 s · 0.02 GB | sparse: 4.4 s |
| 100,000,000 | steady | 100,000 | 0% | 1.4 s · 274 GET · 458.5 MB · 1.2 s · 0.36 GB | 21.3 s · 436 GET · 3025.2 MB · 38.2 s · 0.05 GB | sparse: 1.5 s |
| 100,000,000 | steady | 100,000 | 50% | 13.8 s · 11878 GET · 1630.8 MB · 11.0 s · 0.42 GB | 22.6 s · 436 GET · 3025.2 MB · 39.3 s · 0.06 GB | stream: 22.6 s |
| 100,000,000 | steady | 100,000 | 100% | 16.5 s · 11381 GET · 2401.5 MB · 15.2 s · 0.00 GB | 23.6 s · 436 GET · 3025.2 MB · 47.0 s · 0.00 GB | stream: 25.1 s |

How the thresholds follow:

- **Density.** The sparse reader's cost grows with the written keys —
  ~9 µs of CPU each in Python, past the filters — and the stream's with the
  index's entries, about 7M a second at 10M–100M. They meet near 2% of the
  entries: 1M keys into 100M (1%) take 10.0 s sparse and 14.4 s streamed,
  1M into 10M (10%) 7.7 s sparse and 2.8 s streamed.
- **Exact reads after the filters.** Wall times meet where the reads
  number about 24 per streamed segment: 100M steady, 100K keys half
  unchanged, 11,878 reads in 13.8 s against 436 segments in 22.6 s; 10M
  fresh, 10K half unchanged, 831 reads in 2.0 s against 34 segments in
  1.4 s. At 16 per segment the stream takes the close calls: it issues
  1/16 of the requests or fewer, and its memory does not grow with the
  patch (the sparse reader added 1.75 GB for 1M keys half unchanged at
  100M, the stream nothing measurable).
- **Small indexes.** At 1M keys the sparse reader reads the one level
  whole in two 16 MB requests, and streaming's 8 MB segments are faster
  by 100–150 ms at every size; the thresholds keep small patches on the
  sparse reader there anyway (2 GETs instead of 5).

Against the design's figures: a 1K-key patch at 100M in steady state, cold,
was projected at ~50 GETs and measured 46 GETs, 445 MB, 0.68 s (the delta
PUT included). A streamed patch of the steady 100M index was projected at
~430 GETs and ~3.4 GB; it reads 436 GETs and 3.0 GB in 21–25 s, the fresh
one 341 GETs and 2.4 GB in 12–15 s, on all eight cores (19–47 CPU-seconds).

How the final picks fare, over the 60 rows: `resolve` took 198.5 s in
all, against 157.6 s had every row taken its faster route, and 22,124
GETs against 6,895 had every row taken its cheaper one — the thresholds
trade some of each. Six picks are over 1.5× (and 0.3 s) slower than the
faster route, all of them streams the request rule chose, and their time
includes the tails and filter checks read before switching (0.3–0.8 s at
10M): 10M steady, 10K keys half unchanged, streams in 3.1 s where the
sparse reader takes 1.7 s with 1,201 GETs instead of 60.

**Compaction garbage** (`key-index-format.md` § Garbage files): with
`garbage=True` a compaction also writes the entries it dropped. In the
test workloads (`tests/sdk/test_keys_index.py`) every object a key held and
lost is named — by the delta that superseded it, when its old entry was
read, or by the compaction that dropped it — and no live one ever is, under
all five read routes.

## The engine's warm resolver (2026-10-02)

`docs/resolved-commits.md` §4–§5, milestone 4: the engine keeps index
files on local disk in a local form (blocks decompressed and re-blocked at
8 KiB, a restart point every 16 entries, a CRC on the directory and on each
block) and answers a worker's patch from it with no requests. Same machine
and setup as above; MinIO with 30 ms and 80 MB/s injected for the workers
and the fill.

    uv run python bench/keys/warm.py --s3 http://solera:solera-bench-secret@127.0.0.1:9100/solera-test --sizes 1e6,1e7,1e8

Each index in steady state (upper levels filled, seven deltas in level 0);
patches of random keys, half rewritten unchanged. Every path ends at the
same line, the delta uploaded (one PUT, 30 ms injected): the cold and warm
workers resolve and upload; the engine path builds the worker's run and
request, runs the resolver's whole answer — framing, validation, the
lookups, the delta — and uploads; in brackets, its resolve alone. HTTP is
not in it (below). "SSD only" ran after dropping the page cache. The warm
worker reads through a disk cache holding every file. (An earlier version
of this table stopped the engine's clock at the resolve, before the
upload: the milestone 4 review's finding 12.)

| Keys | Patch | Cold worker | Warm worker | Engine, page cache | Engine, SSD only |
|---|---|---|---|---|---|
| 1,000,000 | 1,000 keys, half unchanged | 419 ms · 9 GET · 29.6 MB · CPU 137 ms | 152 ms · 0 GET · 0.0 MB · CPU 116 ms | 53 ms · 0 GET · 0.0 MB · CPU 18 ms (16 ms) | 86 ms · 0 GET · 0.0 MB · CPU 18 ms (47 ms) |
| 1,000,000 | 10,000 keys, half unchanged | 511 ms · 9 GET · 29.6 MB · CPU 207 ms | 217 ms · 0 GET · 0.0 MB · CPU 174 ms | 192 ms · 0 GET · 0.3 MB · CPU 156 ms (143 ms) | 220 ms · 0 GET · 0.3 MB · CPU 179 ms (168 ms) |
| 1,000,000 | 100,000 keys, half unchanged | 580 ms · 12 GET · 26.1 MB · CPU 594 ms | 356 ms · 0 GET · 0.0 MB · CPU 534 ms | 278 ms · 0 GET · 3.0 MB · CPU 273 ms (147 ms) | 279 ms · 0 GET · 3.0 MB · CPU 276 ms (149 ms) |
| 10,000,000 | 1,000 keys, half unchanged | 852 ms · 524 GET · 77.9 MB · CPU 444 ms | 249 ms · 0 GET · 0.0 MB · CPU 208 ms | 57 ms · 0 GET · 0.0 MB · CPU 20 ms (20 ms) | 215 ms · 0 GET · 0.0 MB · CPU 46 ms (176 ms) |
| 10,000,000 | 10,000 keys, half unchanged | 2.6 s · 60 GET · 349.3 MB · CPU 4.5 s | 2.5 s · 0 GET · 0.0 MB · CPU 5.2 s | 414 ms · 0 GET · 0.3 MB · CPU 180 ms (363 ms) | 457 ms · 0 GET · 0.3 MB · CPU 186 ms (407 ms) |
| 10,000,000 | 100,000 keys, half unchanged | 3.7 s · 60 GET · 349.3 MB · CPU 5.8 s | 3.2 s · 0 GET · 0.0 MB · CPU 5.4 s | 1.8 s · 0 GET · 3.0 MB · CPU 1.7 s (1.7 s) | 1.8 s · 0 GET · 3.0 MB · CPU 1.8 s (1.7 s) |
| 100,000,000 | 1,000 keys, half unchanged | 1.1 s · 649 GET · 478.0 MB · CPU 908 ms | 438 ms · 0 GET · 0.0 MB · CPU 399 ms | 113 ms · 0 GET · 0.0 MB · CPU 48 ms (76 ms) | 451 ms · 0 GET · 0.0 MB · CPU 60 ms (409 ms) |
| 100,000,000 | 10,000 keys, half unchanged | 5.3 s · 5154 GET · 790.5 MB · CPU 4.5 s | 3.4 s · 0 GET · 0.0 MB · CPU 2.1 s | 1.8 s · 0 GET · 0.3 MB · CPU 368 ms (1.8 s) | 1.8 s · 0 GET · 0.3 MB · CPU 408 ms (1.8 s) |
| 100,000,000 | 100,000 keys, half unchanged | 23.2 s · 480 GET · 3462.3 MB · CPU 47.4 s | 25.5 s · 0 GET · 0.0 MB · CPU 52.0 s | 4.4 s · 0 GET · 3.0 MB · CPU 2.1 s (4.2 s) | 3.7 s · 0 GET · 3.0 MB · CPU 1.9 s (3.6 s) |

| Keys | Engine fill: time · GETs · MB read · CPU | Local files on disk |
|---|---|---|
| 1,000,000 | 476 ms · 9 GET · 30 MB · CPU 259 ms | 0.03 GB |
| 10,000,000 | 2.0 s · 28 GET · 349 MB · CPU 2.6 s | 0.35 GB |
| 100,000,000 | 18.2 s · 221 GET · 3461 MB · CPU 25.8 s | 3.42 GB |

- **1K keys** — the scenario-E write — take 53–57 ms end to end on the
  engine at 1M and 10M keys (16–20 ms of it the resolve), against 152–249
  ms for a warm worker and 419–852 ms (9–524 GETs) cold. At 100M: 113 ms
  with the local files in the page cache (76 ms resolving), 451 ms when the
  ~3,000 small reads go to the disk — then about a warm worker's 438 ms,
  still with none of the cold worker's 649 GETs.
- **The local block size decides that wait.** With the source's 64 KiB
  blocks decompressed whole (~100 KB), the 1K-key resolve at 100M took
  438 ms and was slower than the warm worker.
- **Point lookups or a merge.** A lookup costs ~2.5 µs, a merge ~120 ns per
  entry of the snapshot; the engine merges only past one lookup per 16
  entries. Merging at 128 entries per lookup made the 100K-key resolve at
  100M take 15.3 s (all 125M entries decoded) against 4.4 s by lookups.
- **The fill** reads each file once — 221 GETs and 3.5 GB at 100M — and
  writes 3.4 GB of local files: about the compressed size, not the ~5 GB
  the design estimated, since the local form keeps prefix sharing. It takes
  18.2 s since at most two whole files are fetched or built at once (the
  review's finding 2 bounds that memory); unbounded, it took 5.9 s.
- **HTTP** adds ~0.75 ms median (2.5 ms p95) for a 40 KB request and a
  27 KB response on localhost (uvicorn, httpx).

Requests per commit once warm, as `tests/server/test_keys.py` checks it end
to end: the worker reads no index file and makes one PUT (its delta), the
engine installs that delta from the bytes it returned (no GET), and a
consumer's page comes inline in its spec (no GET): scenario E's projected
15.5 GET-equivalents a commit, compaction included.

## Without a worker cache (2026-10-02)

Decision D6 removed the workers' disk cache of index files (`key_cache`,
`DiskCache`): the engine's cache answers small writes, and workers read
the store. The engine's path does not touch a worker's cache, so this
re-run of `warm.py` checks that it did not move, and puts numbers on what
the cold paths cost now. Same command as above. The host was shared and
loaded (load average ~20 on 8 cores, 16 of 31 GB used by other
processes), so only part of the local files stayed in the page cache.

| Keys | Patch | Cold worker | Engine, page cache | Engine, SSD only |
|---|---|---|---|---|
| 1,000,000 | 1,000 keys, half unchanged | 494 ms · 9 GET · 29.6 MB · CPU 133 ms | 62 ms · 0 GET · 0.0 MB · CPU 10 ms (10 ms) | 100 ms · 0 GET · 0.0 MB · CPU 14 ms (50 ms) |
| 1,000,000 | 10,000 keys, half unchanged | 601 ms · 9 GET · 29.6 MB · CPU 181 ms | 231 ms · 0 GET · 0.3 MB · CPU 138 ms (164 ms) | 197 ms · 0 GET · 0.3 MB · CPU 140 ms (142 ms) |
| 1,000,000 | 100,000 keys, half unchanged | 541 ms · 12 GET · 26.1 MB · CPU 309 ms | 341 ms · 0 GET · 3.0 MB · CPU 264 ms (180 ms) | 527 ms · 0 GET · 3.0 MB · CPU 246 ms (312 ms) |
| 10,000,000 | 1,000 keys, half unchanged | 1.0 s · 524 GET · 77.9 MB · CPU 358 ms | 61 ms · 0 GET · 0.0 MB · CPU 18 ms (16 ms) | 299 ms · 0 GET · 0.0 MB · CPU 29 ms (217 ms) |
| 10,000,000 | 10,000 keys, half unchanged | 4.0 s · 60 GET · 349.3 MB · CPU 3.2 s | 446 ms · 0 GET · 0.3 MB · CPU 148 ms (390 ms) | 531 ms · 0 GET · 0.3 MB · CPU 138 ms (417 ms) |
| 10,000,000 | 100,000 keys, half unchanged | 4.0 s · 60 GET · 349.3 MB · CPU 4.1 s | 2.2 s · 0 GET · 3.0 MB · CPU 1.7 s (2.0 s) | 2.1 s · 0 GET · 3.0 MB · CPU 1.8 s (1.9 s) |
| 100,000,000 | 1,000 keys, half unchanged | 1.4 s · 649 GET · 478.0 MB · CPU 771 ms | 149 ms · 0 GET · 0.0 MB · CPU 30 ms (97 ms) | 541 ms · 0 GET · 0.0 MB · CPU 69 ms (452 ms) |
| 100,000,000 | 10,000 keys, half unchanged | 7.2 s · 5154 GET · 790.5 MB · CPU 3.6 s | 3.2 s · 0 GET · 0.3 MB · CPU 369 ms (3.1 s) | 3.1 s · 0 GET · 0.3 MB · CPU 350 ms (3.0 s) |
| 100,000,000 | 100,000 keys, half unchanged | 34.4 s · 480 GET · 3462.3 MB · CPU 31.2 s | 5.6 s · 0 GET · 3.0 MB · CPU 1.4 s (5.5 s) | 6.1 s · 0 GET · 3.0 MB · CPU 1.4 s (5.9 s) |

- **The commit path did not move.** A 1K-key write through a warm engine
  takes 61–62 ms at 1M and 10M (53–57 ms before) and 149 ms at 100M (113
  ms), with no GETs; its CPU is the same or lower (10–30 ms, from 18–48).
  The wall-time differences at 10M–100M are disk reads: the CPU column
  matches the earlier run, the page cache did not hold the 3.4 GB of
  local files on this loaded host, and the 10K-key rows at 100M wait on
  reads in both columns (3.2 s and 3.1 s).
- **What a write the engine does not answer costs** — declined (cold,
  busy, too big, unreachable), over `resolve_max_keys`, or with the
  engine's cache off: a cold worker's resolve. 1K keys: 0.5–1.4 s and
  9–649 GETs (78–478 MB) at 1M–100M, where a worker's warm disk cache took
  152–438 ms and no GETs. 100K keys at 100M: 23–34 s and 480 GETs either
  way; the cache saved requests, not the CPU, which dominates.
- **Other cold readers**, from the follow-up's tables above: a
  full-pass batch of 10K keys, 80–110 ms and 2–14 GETs; `Each`'s and
  failure indexes' lookups, a sparse read like the 1K-key rows.
- **The engine's recount and compaction** read the cache's local copies
  when it holds the index warm (one warm copy serves every engine
  reader), the store otherwise. `warm.py --recount --patches ""` at 100M,
  the same steady index, the counts equal:

  | Keys | Recount from the store | Recount over the cache's local copies |
  |---|---|---|
  | 1,000,000 | 493 ms · 12 GET · 26.1 MB · CPU 244 ms | 235 ms · 0 GET · 0.0 MB · CPU 107 ms |
  | 100,000,000 | 26.0 s · 436 GET · 3021.5 MB · CPU 32.0 s | 15.9 s · 0 GET · 0.0 MB · CPU 14.8 s |

  No requests, and half the CPU: local blocks are stored decompressed.
  (The follow-up's 241.7 s and 3,029 GETs above were a differently shaped
  steady index, before the streaming recount's later changes.)

## Engine-served reads (2026-10-02)

docs/resolved-commits.md §7: at `start`, the engine answers an
attempt's input reads from its cache's local copies, and the worker reads
no index file to find its pages. `warm.py --reads` puts a consumer behind
the steady index by 20 commits of 5K keys (a 100K-entry change window)
and measures each read cold — the worker paging the store, 30 ms and
80 MB/s injected — against the engine's answer end to end: the engine
records the read over its local files, the reply is serialized to JSON
and parsed, and the worker's same call is answered from it.

    uv run python bench/keys/warm.py --s3 http://solera:solera-bench-secret@127.0.0.1:9100/solera-test --sizes 1e7,1e8 --reads --patches ""

| Keys | Read | Cold worker | Engine-served (reply MB) |
|---|---|---|---|
| 1,000,000 | full pass: first batch, 10K keys | 150 ms · 29 GET · 5.7 MB · CPU 92 ms | 24 ms · 0 GET · 0.4 MB · CPU 18 ms |
| 1,000,000 | full pass: a batch of 100K keys, mid-index | 230 ms · 29 GET · 8.0 MB · CPU 130 ms | 139 ms · 0 GET · 3.9 MB · CPU 125 ms |
| 1,000,000 | change window of 20 commits (100K entries): a page of 50K | 119 ms · 20 GET · 5.0 MB · CPU 84 ms | 77 ms · 0 GET · 2.0 MB · CPU 80 ms |
| 10,000,000 | full pass: first batch, 10K keys | 206 ms · 31 GET · 4.5 MB · CPU 94 ms | 20 ms · 0 GET · 0.4 MB · CPU 16 ms |
| 10,000,000 | full pass: a batch of 100K keys, mid-index | 295 ms · 33 GET · 11.4 MB · CPU 217 ms | 121 ms · 0 GET · 3.8 MB · CPU 134 ms |
| 10,000,000 | change window of 20 commits (100K entries): a page of 50K | 126 ms · 20 GET · 3.6 MB · CPU 86 ms | 110 ms · 0 GET · 2.0 MB · CPU 110 ms |
| 100,000,000 | full pass: first batch, 10K keys | 189 ms · 33 GET · 4.8 MB · CPU 127 ms | 30 ms · 0 GET · 0.4 MB · CPU 28 ms |
| 100,000,000 | full pass: a batch of 100K keys, mid-index | 441 ms · 37 GET · 16.4 MB · CPU 424 ms | 118 ms · 0 GET · 3.7 MB · CPU 131 ms |
| 100,000,000 | change window of 20 commits (100K entries): a page of 50K | 135 ms · 20 GET · 3.6 MB · CPU 79 ms | 65 ms · 0 GET · 2.0 MB · CPU 67 ms |

- **No GETs.** A page costs the worker no index reads; the cold worker's
  20–37 are a page's index parts and blocks, fetched in parallel.
- **A full-pass batch** of 10K keys takes 20–30 ms instead of 150–206
  ms at every size; a page of 100K keys 118–139 ms instead of 230–441 ms,
  most of it encoding the reply (3.7–3.9 MB of `.kx` in base64 JSON) and
  decoding it.
- **A change window's page** gains least, 65–110 ms against 119–135 ms:
  its 20 delta files are small, so the cold worker's reads are few and
  parallel, and the reply's 2 MB costs about what they do. Its GETs go to
  zero all the same.

## A Railway bucket, measured from Railway (2026-10-02)

Railway Buckets (S3-compatible, served by Tigris at `t3.storageapi.dev`),
probed and benchmarked from a temporary service in the bucket's region
(bucket `sjc`, service `us-west2`, 8 vCPU, 24 GB), nothing injected. Both
were deleted afterwards. Requests are free on Railway (storage
$0.015/GB-month, API operations and egress free), so a GET count no
longer costs money: it costs latency only.

**Conditional writes.** `bench/railway/probe.py`: `If-None-Match: *` —
the second PUT is 412 and the first write stays, on CompleteMultipartUpload
too; `If-Match` with the current ETag succeeds and a stale one is 412;
24 clients racing one create: exactly one winner, 20 rounds of 20;
read-after-write (new and overwrite), list-after-write and
list-after-delete all consistent; ranges and multipart work; obstore's
create and update modes behave as against S3. The store conformance kit,
journal fencing, filestore and the private-store worker test pass against
the bucket (85 passed; the Postgres cases skipped).

**The network** (`bench/railway/net.py`, through obstore, 200 requests
each, the second of two runs):

| Request | p50 | p95 | p99 |
|---|---|---|---|
| GET 1 KB | 6.2 ms | 12.1 ms | 23.1 ms |
| GET range of 100 B | 6.5 ms | 16.1 ms | 28.2 ms |
| HEAD | 5.8 ms | 15.2 ms | 57.1 ms |
| PUT 1 KB | 12.4 ms | 31.8 ms | 242.7 ms |
| PUT 1 KB, create-if-absent | 12.3 ms | 22.3 ms | 36.3 ms |
| LIST, one page | 16.8 ms | 29.0 ms | 47.2 ms |

| Throughput | |
|---|---|
| one connection, 64 KiB range reads of a 256 MiB object | p50 18.9 ms each (3 MB/s) |
| one connection, 1 MiB reads | p50 25.4 ms (41 MB/s) |
| one connection, 8 MiB reads | p50 57.7 ms (145 MB/s) |
| one connection, 64 MiB reads | p50 638 ms (105 MB/s) |
| one GET of 256 MiB | 158 MB/s |
| one PUT of 64 MiB | 252 MB/s |
| 8 / 32 parallel 8 MiB reads | 252 / 490 MB/s |
| 64 parallel 4 MiB / 16 MiB reads | 534 / 1,512 MB/s |

Against the injected assumptions (30 ms a request, 80 MB/s a
connection): a small request is ~5× faster (6 ms p50; a range of a large
object ~19 ms), a connection is ~1.3–2× faster (105–158 MB/s), and 64
parallel reads reach 0.5–1.5 GB/s. The tables below are therefore what the
key index costs there; the earlier sections' injected numbers are the
slower bound.

**The key index from Railway** (`bench/railway/bench.sh`: `bench.py` and
`warm.py --reads --recount`, `--latency 0 --bandwidth 0`). Every path to
the delta uploaded; in brackets, the engine's resolve alone. ("SSD only"
needs `sudo` to drop the page cache, which the container has not.)

| Keys | Write, half unchanged | Cold worker | Engine |
|---|---|---|---|
| 1,000,000 | 1K keys | 283 ms · 29 GET · 35 MB | 46 ms · 0 GET (13 ms) |
| 1,000,000 | 10K keys | 765 ms · 29 GET | 253 ms · 0 GET (148 ms) |
| 1,000,000 | 100K keys | 893 ms · 32 GET | 265 ms · 0 GET (170 ms) |
| 10,000,000 | 1K keys | 1.1 s · 349 GET · 68 MB | 57 ms · 0 GET (20 ms) |
| 10,000,000 | 10K keys | 2.3 s · 100 GET | 169 ms · 0 GET (123 ms) |
| 10,000,000 | 100K keys | 6.4 s · 100 GET | 1.6 s · 0 GET (1.5 s) |
| 100,000,000 | 1K keys | 1.3 s · 426 GET · 467 MB | 70 ms · 0 GET (33 ms) |
| 100,000,000 | 10K keys | 3.8 s · 3,387 GET | 257 ms · 0 GET (193 ms) |
| 100,000,000 | 100K keys | 29.4 s · 520 GET | 1.8 s · 0 GET (1.6 s) |

| Keys | Read | Cold worker | Engine-served |
|---|---|---|---|
| 1,000,000 | full pass, first 10K-key batch | 200 ms · 29 GET | 25 ms · 0 GET |
| 1,000,000 | 100K-key page, mid-index | 160 ms · 29 GET | 89 ms · 0 GET |
| 1,000,000 | 50K page of a 20-commit window | 111 ms · 20 GET | 56 ms · 0 GET |
| 10,000,000 | full pass, first 10K-key batch | 360 ms · 31 GET | 21 ms · 0 GET |
| 10,000,000 | 100K-key page, mid-index | 534 ms · 33 GET | 111 ms · 0 GET |
| 10,000,000 | 50K page of a 20-commit window | 120 ms · 20 GET | 64 ms · 0 GET |
| 100,000,000 | full pass, first 10K-key batch | 357 ms · 33 GET | 25 ms · 0 GET |
| 100,000,000 | 100K-key page, mid-index | 1.0 s · 37 GET | 151 ms · 0 GET |
| 100,000,000 | 50K page of a 20-commit window | 129 ms · 20 GET | 101 ms · 0 GET |

| Keys | Engine fill | Recount from the store | Recount over local copies |
|---|---|---|---|
| 1,000,000 | 491 ms · 29 GET · 35 MB | 391 ms · 32 GET | 123 ms · 0 GET |
| 10,000,000 | 1.9 s · 48 GET · 352 MB | 1.9 s · 69 GET | 1.5 s · 0 GET |
| 100,000,000 | 18.2 s · 241 GET · 3.5 GB | 21.8 s · 456 GET | 16.1 s · 0 GET |

`bench.py`, selected rows (the full tables are in the run's logs; their
shape matches the follow-up's above):

| Operation | 1,000,000 | 10,000,000 | 100,000,000 |
|---|---|---|---|
| build (initial index) | 1.6 s | 11.6 s | 136 s, 4.6 GB peak RSS |
| 1K random keys changed | 206 ms · 2 GET | 206 ms · 5 GET | 977 ms · 34 GET |
| steady: 1K random keys, half unchanged | 253 ms · 9 GET | 1.0 s · 229 GET | 5.5 s · 308 GET |
| steady: full-pass batch of 10K keys | 67 ms · 9 GET | 210 ms · 14 GET | 350 ms · 18 GET |
| steady: commit, 1K changes + delta write | 384 ms · 9 GET · 1 PUT | 348 ms · 12 GET · 1 PUT | 964 ms · 52 GET · 1 PUT |
| steady: full scan (recount) | 173 ms · 12 GET | 1.8 s · 49 GET | 17.6 s · 436 GET |
| steady: compaction into the deepest level | — | 2.0 s · 11 GET | 4.8 s · 49 GET |

- **The engine's numbers move little** — they read no requests: a 1K-key
  write at 100M is 70 ms here, 113–149 ms on the VM, the difference
  the upload's PUT (12 ms against 30 ms injected) and the machine.
- **A cold worker is faster here, but not by the requests alone.** At 1M
  and 10M a 1K-key write is 283 ms and 1.1 s against 494 ms and 1.0 s on
  the VM; at 100M 1.3 s against 1.4 s: its 426 GETs are parallel, so the
  6 ms request matters less than the 467 MB they carry and the CPU that
  decodes them. The 100K-key write at 100M is 29 s either way — CPU.
- **Requests are free here**, so GET counts cost latency only: the
  engine's advantage on Railway is time (an order of magnitude on writes
  and on full-pass batchs) and load on the bucket, not money. A change
  window's page gains least (101 ms against 129 ms at 100M): its delta
  files are small and read in parallel.

## Python rows: the native walk against tuned pure Python (2026-10-02)

Is the native digest of `list[dict]` held back by FFI? The 2.1–3.6 µs a row
above compared the old path (JSON or pickle, a Python hash callback) with the
new encoder, never with Python written for speed.

    uv run python bench/keys/pyrows.py --sizes 1e4,1e6,1e7
    uv run python bench/keys/pyrows.py --sizes 1e4,1e6,1e7 --native <old build>/_native.abi3.so --cases native,arrow

Rows shaped like an operational table: twelve columns — a string key, four
strings (two from small vocabularies), three integers, three floats, a
UTC-aware `datetime` — about 10% of fields None, keys shuffled. Each case
runs in its own process with its rows built first. Peak memory is what the
step adds on top of the rows, which take ~0.9 GB per million.
**Digest** is every row's `row(r)` in list order: `row_digests`, or the
baseline. **Versions** is each key's version, sorted by key, as a patch
reads them: `Rows.records(...).entries()`, or the baseline sorting and
grouping.

- **Pure Python, tuned**: `docs/row-digest.md` for these value types. It is
  one flat loop with locals bound, each key set's sorted field plan made
  once, values dispatched on their exact type, one `join` and one
  `xxhash.xxh3_128_intdigest` a row. It is byte-identical to the
  extension, checked on every run.
- **Native, before** and **after**: the extension at `3ef5c4d`, and with
  this section's changes.
- **Arrow**: `pyarrow.Table.from_pylist`, then `Rows.arrow`, conversion
  timed.

| µs a row · peak MB | 10K | 1M | 10M |
|---|---|---|---|
| Digest: pure Python | 3.48 · 2 | 3.60 · 169 | 3.55 · 1,682 |
| Digest: native, before | 2.42 · 1 | 2.58 · 33 | 2.42 · 321 |
| Digest: native, after | **0.47** · 1 | **0.54** · 32 | **0.50** · 320 |
| Versions: pure Python | 4.43 · 5 | 5.90 · 287 | 6.33 · 2,730 |
| Versions: native, before | 2.86 · 7 | 4.36 · 159 | 4.56 · 1,553 |
| Versions: native, after | **0.78** · 6 | **0.83** · 181 | **0.83** · 1,796 |
| Versions: Arrow, before | 1.61 · 104 | 3.25 · 399 | 2.87 · 2,940 |
| Versions: Arrow, after | 1.54 · 97 | 3.26 · 392 | 2.57 · 2,937 |

Before, the native walk was only 1.5× faster than pure Python. It is now
~7× faster on digests and ~7.5× on versions, and digesting the dicts beats
converting them to Arrow by 3–4×.

**Where the time went** (`perf`, 1M rows, the old build's digest step):
71% reading Python objects (attribute lookups, dict lookups, refcounting
calls, decoding UTF-8), 24% the walker's own glue (pair vectors, copies,
dispatch), 3% encoding, 1% hashing. By column, from a row holding only its
key:

| Added to a row | Before | After | After, without abi3 |
|---|---|---|---|
| the key alone (dict, framing, hash) | 0.17 µs | 0.05 µs | 0.05 µs |
| four strings | 0.25 µs | 0.09 µs | — |
| three integers | 0.34 µs | 0.08 µs | — |
| three floats | 0.23 µs | 0.05 µs | — |
| one aware `datetime` | 1.07 µs | 0.16 µs | 0.05 µs |
| the whole row | 2.36 µs | 0.48 µs | 0.34 µs |

So the boundary itself was not the problem. These were:

- **Reading a `datetime`** cost 45% of the row: about fourteen attribute
  reads (`year` … `microsecond`, `tzinfo`, `utcoffset()`, then the
  offset's `days`/`seconds`/`microseconds`). Each one built its attribute
  name as a new Python string (decode, hash, lookup) and a new int. Now an
  exact `datetime` is one subtraction from the epoch (aware or naive), then
  three reads of the `timedelta`, with interned names, and the
  `timezone.utc` singleton needs no `utcoffset()` call.
- **The generic walk.** Each dict was collected into a vector of owned
  pairs (two refcount *calls* per field under the stable ABI). Its field
  names were sorted per row, the skip list allocated per row, integers
  read through pyo3's 128-bit slow path and formatted into a `String`, and
  the record copied into a fresh buffer to hash. Now a dict row is read with
  `PyDict_Next` (borrowed references), and its fields are written in an
  order sorted once per key set. The key set is recognized by the very
  `str` objects, then by name. Exact `str`, `int`, `float`, `bool` and
  `None` are encoded without running Python code. Anything else is held
  and encoded once the dict is done with, so a value's own code (a
  `tzinfo`) cannot pull the row out from under the walk.
- **Key order.** `Rows.records` used to digest rows lazily, a window at a
  time in *key* order. With shuffled keys that order is random in memory,
  and reading a row missed the cache at nearly every object. The versions
  step cost 2.4 µs a row on top of its digests. Now the rows are digested
  in list order in the same pass that reads their keys, and the 16-byte
  digests are handed out in key order. The cost is the digests held for the
  whole write (+0.24 GB at 10M rows, against ~9 GB of rows).
- **Hashing.** XXH3-128 of a ~200-byte record takes 19 ns. A streaming
  state fed the record's ~24 pieces took 79 ns, against 89 ns to copy the
  pieces into one buffer and hash it. So the record is now written after
  the 2-byte `row` prefix and hashed where it lies: no copy, no streaming.

Arrow rows now skip the per-row sort too: their columns are already in
name order, so each field is written directly. `from_pylist` alone takes
~2.5 µs a row. For Python rows, Arrow pays off only when the data
already is Arrow.

**What is left** is the stable ABI (`abi3-py312`: one wheel for every
CPython from 3.12). Built per version instead, the `datetime` is read from
its struct (0.16 → 0.05 µs) and the row costs 0.34 µs, not 0.48. That
would mean a wheel per Python version, so it is left as a choice.

## Versions are generations: format v3 (2026-10-02)

`docs/versions.md`: an entry is `(key, generation, deleted, payload?)` —
no content version, no pair filter, and an optional payload (a source's
version, a failure record). Earlier sections measured 16-byte row
digests on every entry; `digest.py` and `pyrows.py`, which measured
digesting, are deleted with the digest. `bench.py --payload N` gives every
entry an N-byte payload: without one (the default) the index is a derived
output's, where every write is a change, so the unchanged-share rows only
run with one. "Before" is `dbf1070` (16-byte digests), same machine and
latency model (30 ms per request, 80 MB/s per connection, 64 in parallel).


#### 1,000,000 keys

| | Before (16 B digests) | After, no payload (derived) | After, 16 B versions (source) |
|---|---|---|---|
| build_s | 1.3 s | 0.8 s | 1.3 s |
| bytes | 29.2 MB | 8.8 MB | 26.8 MB |
| b_per_entry | 29.2 B | 8.8 B | 26.8 B |
| filter_b_per_entry | 3.51 B | 1.75 B | 1.76 B |
| 100 random keys changed | 335 ms · 2G · 29.2 MB | 191 ms · 1G · 8.8 MB | 335 ms · 2G · 26.8 MB |
| 1K random keys changed | 456 ms · 2G · 29.2 MB | 228 ms · 1G · 8.8 MB | 422 ms · 2G · 26.8 MB |
| 1K random keys, half unchanged | 432 ms · 2G · 29.2 MB | — | 383 ms · 2G · 26.8 MB |
| 1K clustered keys changed | 312 ms · 2G · 29.2 MB | 164 ms · 1G · 8.8 MB | 271 ms · 2G · 26.8 MB |
| 1K new keys inserted | 454 ms · 2G · 29.2 MB | 246 ms · 1G · 8.8 MB | 398 ms · 2G · 26.8 MB |
| 100K random keys changed | 717 ms · 2G · 29.2 MB | 468 ms · 1G · 8.8 MB | 767 ms · 2G · 26.8 MB |
| full-pass batch of 10K keys | 79 ms · 2G · 0.3 MB | 74 ms · 2G · 0.1 MB | 83 ms · 2G · 0.3 MB |
| commit: 1K random changes + delta write | 528 ms · 2G · 29.2 MB | 274 ms · 1G · 8.8 MB | 468 ms · 2G · 26.8 MB |
| compaction: 8 delta files | 92 ms · 8G · 0.4 MB | 85 ms · 8G · 0.1 MB | 80 ms · 8G · 0.2 MB |
| full replacement, 1% changed | 701 ms · 5G · 25.7 MB | 682 ms · 2G · 7.1 MB | 664 ms · 4G · 25.1 MB |
| initial load: every key, unsorted | 1.7 s · 0G · 0.0 MB | 480 ms · 0G · 0.0 MB | 1.6 s · 0G · 0.0 MB |
| full scan (recount), 100K-key pages | 293 ms · 5G · 25.7 MB | 226 ms · 2G · 7.1 MB | 327 ms · 4G · 25.1 MB |
| steady: 1K random keys changed | 452 ms · 9G · 31.1 MB | 335 ms · 8G · 9.4 MB | 466 ms · 9G · 28.4 MB |
| steady: 1K random keys, half unchanged | 491 ms · 9G · 31.1 MB | — | 494 ms · 9G · 28.4 MB |
| steady: full-pass batch of 10K keys | 108 ms · 9G · 2.1 MB | 83 ms · 9G · 0.7 MB | 105 ms · 9G · 1.9 MB |
| steady: full scan (recount), 100K-key pages | 357 ms · 12G · 27.6 MB | 299 ms · 9G · 7.6 MB | 389 ms · 11G · 26.6 MB |
| steady: commit: 1K random changes + delta write | 518 ms · 9G · 31.1 MB | 361 ms · 8G · 9.4 MB | 512 ms · 9G · 28.4 MB |
| steady: compaction: level 0, 8 files | 153 ms · 8G · 1.9 MB | 118 ms · 8G · 0.6 MB | 154 ms · 8G · 1.6 MB |
| steady: compaction: level 0, a tenth of level 1, into level 1 | 1.1 s · 7G · 28.9 MB | 585 ms · 4G · 8.1 MB | 1.0 s · 6G · 28.0 MB |

#### 10,000,000 keys

| | Before (16 B digests) | After, no payload (derived) | After, 16 B versions (source) |
|---|---|---|---|
| build_s | 10.8 s | 7.3 s | 10.7 s |
| bytes | 283.8 MB | 79.6 MB | 257.6 MB |
| b_per_entry | 28.4 B | 8.0 B | 25.8 B |
| filter_b_per_entry | 3.51 B | 1.75 B | 1.76 B |
| 100 random keys changed | 232 ms · 3G · 35.1 MB | 135 ms · 3G · 17.5 MB | 274 ms · 97G · 23.5 MB |
| 1K random keys changed | 285 ms · 5G · 35.2 MB | 134 ms · 3G · 17.5 MB | 802 ms · 728G · 70.2 MB |
| 1K random keys, half unchanged | 689 ms · 450G · 65.2 MB | — | 822 ms · 713G · 69.6 MB |
| 1K clustered keys changed | 252 ms · 2G · 12.5 MB | 126 ms · 1G · 6.2 MB | 158 ms · 2G · 6.3 MB |
| 1K new keys inserted | 229 ms · 3G · 35.1 MB | 133 ms · 3G · 17.5 MB | 172 ms · 4G · 17.7 MB |
| 100K random keys changed | 890 ms · 346G · 58.4 MB | 462 ms · 3G · 17.5 MB | 2.0 s · 20G · 257.6 MB |
| full-pass batch of 10K keys | 87 ms · 4G · 0.7 MB | 78 ms · 4G · 0.2 MB | 87 ms · 4G · 0.7 MB |
| commit: 1K random changes + delta write | 305 ms · 6G · 35.3 MB | 167 ms · 3G · 17.5 MB | 888 ms · 711G · 69.5 MB |
| compaction: 8 delta files | 91 ms · 8G · 0.3 MB | 80 ms · 8G · 0.1 MB | 95 ms · 8G · 0.2 MB |
| full replacement, 1% changed | 4.6 s · 34G · 248.8 MB | 3.7 s · 12G · 62.1 MB | 4.6 s · 34G · 240.1 MB |
| initial load: every key, unsorted | 14.7 s · 0G · 0.0 MB | 4.8 s · 0G · 0.0 MB | 16.6 s · 0G · 0.0 MB |
| full scan (recount), 100K-key pages | 1.3 s · 34G · 248.8 MB | 662 ms · 12G · 62.1 MB | 1.3 s · 34G · 240.1 MB |
| steady: 1K random keys changed | 487 ms · 11G · 46.7 MB | 183 ms · 7G · 1.9 MB | 812 ms · 404G · 50.8 MB |
| steady: 1K random keys, half unchanged | 773 ms · 229G · 60.2 MB | — | 790 ms · 390G · 49.7 MB |
| steady: full-pass batch of 10K keys | 104 ms · 14G · 1.6 MB | 116 ms · 15G · 2.3 MB | 93 ms · 14G · 1.4 MB |
| steady: full scan (recount), 100K-key pages | 2.3 s · 49G · 309.5 MB | 2.6 s · 28G · 114.5 MB | 2.2 s · 50G · 304.5 MB |
| steady: commit: 1K random changes + delta write | 559 ms · 12G · 46.7 MB | 204 ms · 7G · 1.9 MB | 753 ms · 407G · 50.7 MB |
| steady: compaction: level 0, 8 files | 261 ms · 8G · 3.7 MB | 196 ms · 8G · 2.0 MB | 237 ms · 8G · 3.7 MB |
| steady: compaction: level 0, a tenth of level 1, into level 1 | 2.2 s · 10G · 64.0 MB | 2.8 s · 11G · 54.2 MB | 2.1 s · 11G · 67.8 MB |
| steady: compaction: a level-1 file into level 2 | 2.1 s · 11G · 74.4 MB | 3.1 s · 20G · 113.1 MB | 2.0 s · 11G · 72.2 MB |

- **A derived output's index is 3.3× smaller** (8.0–8.8 B per entry against
  28.4–29.2 B) and everything that reads it whole moves with it: an initial
  load of 10M keys 14.7 s → 4.8 s, a recount 1.3 s → 0.66 s, a full
  replacement reads 62 MB instead of 249 MB.
- **A patch of a derived output reads less and never more**: the filters
  decide every key with no block read (1K random keys at 10M: 3 GETs
  against 5). Rows rewritten unchanged used to need exact reads — 450
  GETs at 10M for half of 1K keys — and are now simply changes.
- **A cold source commit with versions pays for the pair filter's
  removal.** Only the entry says whether a version moved, so each touched
  key's block is read: 1K random keys at 10M, 728 GETs and 802 ms against
  5 GETs and 285 ms, and 100K keys stream the index (2.0 s against 0.9 s).
  `versions.md` §3 accepted this. Source commits are prepared by the
  engine, which answers them from its warm cache whenever it holds the
  index (`resolved-commits.md` §4), so the cold path is the exception.
- With 16-byte versions, entries are 26–27 B: the payload costs what the
  digest did, less the second filter (1.75 B per entry saved).
- The steady state's level-1 push at 10M grew (2.1 s → 3.1 s): the filled
  upper levels hold ~3× more entries in the same bytes, so a pushed file
  overlaps more of the level below. Bytes per operation are the target
  sizes', as before.

`params.py` (1M entries, total per entry with filters): random ids 11.4 B
without a payload, 29.7 B with 16 random bytes; UUIDs 33.7 B / 52.4 B;
sequential ids 5.9 B / 24.0 B, 11.8 B with short versions. Two filters
are 1.75 B per entry at 14 bits per item, 0.35% false positives measured.
`cpu.py 1000000` (path-like keys): native encode 2.38M entries/s, decode
5.79M/s, merge 5.73M/s without payloads (1.85M, 3.81M and 3.80M with 16
bytes).

## F16's cost: level 0 merged into an empty level 1 (2026-10-03)

Before F16's fix, level 0 compacting into an empty level 1 was a relabel:
no bytes read or written. It is now a merge (level-0 files overlap). This
compares that merge with the same compaction into a level 1 holding every
key. Level 0: 8 files of `n` changed keys each, drawn from `10n`; level 1
empty, or all `10n` keys. Local file:// store, no injected latency (wall
time is CPU and local disk), Apple M5 Max, native, zlib level 1. Bytes are
everything through `ObjectIO`; peak RSS is the compaction's own (a fresh
process, measured above its resident memory just before); wall time the
best of three.

    uv run python bench/keys/f16.py --sizes 1e3,1e4,1e5

| Keys per level-0 file | Level 1 | Wall | Read | Written | Peak RSS | Out entries |
|---|---|---|---|---|---|---|
| 1,000 | empty | 5 ms | 37 kB | 24 kB | 4.4 MB | 5,662 |
| 1,000 | all 10,000 keys | 6 ms | 56 kB | 37 kB | 5.3 MB | 10,000 |
| 10,000 | empty | 12 ms | 0.3 MB | 0.2 MB | 13.9 MB | 57,106 |
| 10,000 | all 100,000 keys | 20 ms | 0.5 MB | 0.4 MB | 24.5 MB | 100,000 |
| 100,000 | empty | 75 ms | 3.4 MB | 2.3 MB | 96.8 MB | 569,612 |
| 100,000 | all 1,000,000 keys | 141 ms | 5.2 MB | 3.7 MB | 164.6 MB | 1,000,000 |

The merge into an empty level 1 costs about half the merge into a full
one, at every size: it reads level 0's files and writes the merged file
(overlapping keys once). On S3 that adds a round trip of reads and one of
writes, and a few MB, to a compaction that was free. It happens only when
level 1 is empty: an index's first compaction, or one after a replacement
by nothing. Peak memory is about 170 bytes per output entry in both cases.

## One tiling of spans (2026-10-04)

`docs/key-index-design.md`: the index as key-sorted spans tiling commit
time, merged only between observer boundaries. Apple M5 Max.

**The policy replayed** (`tiling.py`, density model as `amplification.py`;
commits of 1K keys, 90% updates, 5% removes, 5% adds; second half
measured; 4-way size classes, merge into the oldest span at a quarter of
it; consumers read every `period` commits). `amplification.py --policies
now --commits 40000` on the same commits: 16.3× at 1M, 29.8× at 100M.

    uv run python bench/keys/tiling.py --sizes 1e6,1e8 --commits 40000,200000 --policies 4:1000

| Keys | Consumers (periods) | Written per entry committed | Spans, mean (max) | Entries per live key, mean (max) | Catch-up of the slowest, per read |
|---|---|---|---|---|---|
| 1,000,000 | 1 | 7.1 | 6.2 (11) | 1.13 (1.25) | 1 span, 1K entries |
| 1,000,000 | 1, 360 | 8.9 | 4.9 (9) | 1.16 (1.31) | 6 spans, 312K |
| 1,000,000 | 1, 360, 8,640 | 9.0 | 9.9 (15) | 2.15 (2.54) | 5 spans, 1,237K |
| 1,000,000 | ten, 1 to 8,640 | 10.0 | 12.0 (16) | 3.43 (4.31) | 12 spans, 2,992K |
| 1,000,000 | 1, 60,480 | 7.2 | 7.9 (14) | 2.87 (4.11) | 10 spans, 2,696K |
| 100,000,000 | 1 | 10.3 | 11.3 (21) | 1.12 (1.25) | 1 span, 1K |
| 100,000,000 | 1, 360 | 15.9 | 13.2 (22) | 1.13 (1.25) | 8 spans, 360K |
| 100,000,000 | 1, 360, 8,640 | 17.2 | 14.8 (24) | 1.17 (1.33) | 5 spans, 8,297K |
| 100,000,000 | ten, 1 to 8,640 | 16.8 | 15.1 (20) | 1.16 (1.29) | 12 spans, 8,490K |
| 100,000,000 | 1, 60,480 | 10.4 | 10.3 (20) | 1.24 (1.49) | 14 spans, 49,449K |

With a storage cap of 2× the live keys (`--cap-ratio 2`): at 1M, ten
consumers lose 9 positions over 20,000 commits and hold 2.42 (3.00)
entries per live key; a weekly consumer, 1.80 (3.09); with a cap of 1,
1.13 (2.05). Nothing at 100M reaches the cap. Commits of 10K keys at 100M
(20,000 commits): 8.6×, 10.5× and 13.1× for 1, (1, 360, 8,640) and ten
consumers. Rejected policies: capping spans per group by merging the
smallest neighbours wrote 540–1,300× at 100M; size classes without the
"older than a bigger neighbour" rule left 176 spans on average (354 max)
behind an hourly consumer at 100M.

**Cold reads** (`tiling_reads.py`: real files, `KeyIndex.lookup` exact and
`KeyIndex.page`, local store with 30 ms per request and 80 MB/s per
connection, 64 in parallel). "Leveled" is the presence study's steady
state; "tiling R" is the same base plus 4-way size classes, three spans
each (R spans).

    uv run python bench/keys/tiling_reads.py --sizes 1e6,1e8 --dir /tmp/tiling

| Keys | Layout | Files | Entries per live key | Size | Tails | 1K lookups, exact | 100K-key page |
|---|---|---|---|---|---|---|---|
| 1,000,000 | leveled | 8 | 1.01 | 9 MB | 2 MB | 8 GETs, 9 MB, 0.19 s | 9 GETs, 1 MB, 0.09 s |
| 1,000,000 | tiling 13 | 13 | 1.25 | 11 MB | 2 MB | 13 GETs, 11 MB, 0.20 s | 14 GETs, 3 MB, 0.10 s |
| 1,000,000 | tiling 25 | 25 | 1.51 | 14 MB | 3 MB | 25 GETs, 14 MB, 0.21 s | 26 GETs, 6 MB, 0.12 s |
| 100,000,000 | leveled | 53 | 2.00 | 1,416 MB | 350 MB | 1,851 GETs, 425 MB, 2.74 s | 19 GETs, 4 MB, 0.15 s |
| 100,000,000 | tiling 22 | 56 | 1.16 | 845 MB | 204 MB | 1,117 GETs, 285 MB, 1.52 s | 37 GETs, 13 MB, 0.13 s |
| 100,000,000 | tiling 43 | 83 | 1.33 | 985 MB | 233 MB | 1,163 GETs, 343 MB, 1.72 s | 70 GETs, 25 MB, 0.16 s |

The 100M run took 308 s (44 s of it the build) and 5.2 GB peak RSS.

## Spans after review: churn, real catch-up, matched layouts, retention (2026-10-05)

`docs/key-index-design.md`, revised: merges may cross endpoints, keeping
the version each endpoint sees. Apple M5 Max, ≤ 3 cores.

**Observer churn** (`spans.py`, replayed; full tables in the doc):
`versions` writes 7.3–9.5× (1M) and 12.3–14.4× (100M) in every scenario
(1 to 1,000 daily readers, an endpoint at every commit, a stalled full
pass, 60 staggered hourly readers, temporary-key churn, a 1M-key commit),
with 6–22 spans on average and 30 at most; `blocked` (merges never cross
an endpoint) has 543–546 spans at 100 daily readers and 183–187 for 60
staggered hourly ones. Budgeted upkeep (two lanes, 2M entries per commit
each) moves the peak by at most 4 spans; one lane let 70–117 spans pile up
behind a 100M base merge.

    uv run python bench/keys/spans.py --sizes 1e6,1e8

**Catch-up over real spans** (`catchup.py`; 12,000 commits of 1K keys;
paged 100K keys at a time; classes checked against the per-commit merge;
30 ms per request, 80 MB/s per connection, 64 in parallel):

| Keys | Behind | Spans | Aligned (fanout 8) | Packed, one merge |
|---|---|---|---|---|
| 1M | 100 | 4 GETs, 0.9 MB, 0.06 s | 9 GETs, 1.0 MB, 0.06 s | 1 GET, 1.0 MB, 0.08 s |
| 1M | 360 | 9 GETs, 3.2 MB, 0.15 s | 10 GETs, 3.4 MB, 0.15 s | 1 GET, 3.7 MB, 0.23 s |
| 1M | 10,000 | 13 spans, 27 GETs, 13.6 MB, 1.22 s, +21 MB RSS | 159 GETs, 41.2 MB, 2.24 s | 7 GETs, 101 MB, 11.7 s, +494 MB RSS |
| 100M | 100 | 4 GETs, 0.9 MB, 0.06 s | 9 GETs, 0.9 MB, 0.07 s | 1 GET, 1.0 MB, 0.08 s |
| 100M | 360 | 9 GETs, 3.3 MB, 0.18 s | 10 GETs, 3.4 MB, 0.17 s | 1 GET, 3.5 MB, 0.24 s |
| 100M | 10,000 | 16 spans, 259 GETs, 65.3 MB, 7.69 s, +20 MB RSS | 965 GETs, 68.0 MB, 8.08 s | 6 GETs, 97.1 MB, 12.9 s, +419 MB RSS |

One commit behind is 1 GET, 10 KB, 30 ms in every layout. Building the
spans wrote 6.2× (1M) and 8.7× (100M) the committed entries past the
oldest consumer; the aligned blocks 3.5× and 4.4×.

    uv run python bench/keys/catchup.py --sizes 1e6,1e8 --commits 12000

**Retention** (`retention.py`, replayed, 100M): current spans 1,141 MB
mean (1,278 peak); kept by attempt pins 4 MB (1,279); cleanup deltas
0.1 MB; unpublished or abandoned merge outputs 6 MB (1,000); data objects
kept by pins 170K (585K). The peaks are one slow attempt pinned across a
base merge and one abandoned base merge.

    uv run python bench/keys/retention.py --sizes 1e6,1e8

**Matched layouts** (`layouts.py`, measured; one trace applied to today's
leveled index by its real planner and to the spans by the policy):

| | 1M: leveled | 1M: spans | 100M: leveled | 100M: spans |
|---|---|---|---|---|
| written per entry committed | 17.2 | 8.0 | 50.3 | 13.0 |
| runs · files · entries per live key | 3 · 3 · 1.03 | 3 · 3 · 1.10 | 5 · 11 · 1.01 | 9 · 14 · 1.02 |
| lookup 1K exact, cold | 3 GETs, 10 MB, 0.21 s | 3 GETs, 11 MB, 0.29 s | 907 GETs, 200 MB, 1.13 s | 901 GETs, 207 MB, 1.20 s |
| append 1K exact, cold | 3 GETs, 0.24 s | 3 GETs, 0.31 s | 58 GETs, 409 MB, 5.86 s | 65 GETs, 417 MB, 7.65 s |
| page 100K, cold | 4 GETs, 0.9 MB, 0.10 s | 4 GETs, 2.0 MB, 0.14 s | 9 GETs, 1.6 MB, 0.09 s | 13 GETs, 4.8 MB, 0.11 s |
| lookup 1K / page 100K, warm | 0.01 / 0.01 s | 0.01 / 0.02 s | 0.01 / 0.01 s | 0.01 / 0.01 s |

20,000 commits at 1M, 30,000 at 100M (8 min, 3.9 GB RSS). Lookups cost the
same on matched layouts; the first version's 1,117-vs-1,851 GETs compared
against a leveled layout with 2.0 entries per live key and is withdrawn.

    RAYON_NUM_THREADS=2 uv run python bench/keys/layouts.py --sizes 1e6,1e8 --commits 30000
