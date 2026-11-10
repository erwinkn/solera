# Change index I/O boundary: Rust object_store (A) vs Python obstore (B)

Measured 2026-11-10 on a 4-vCPU x86_64 cloud machine (15 GB RAM), Python 3.12, obstore 0.11.1 (Solera's pin),
object_store 0.14.2, pyo3 0.29 (abi3-py312), tokio 1.x. Throwaway spike, never merged.

## Recommendation

**A: I/O inside Rust.** For the common queries (q1 diff, q2 scan) the two are indistinguishable (wall within noise,
process CPU within 3%). They separate on the request-heavy query (q3, 2,410 GETs per query): at 50 concurrent
queries B costs **+12 to +16 s of process CPU per 120,500 GETs (about 100-120 µs per GET)**, creates **127,600 Python
awaitables vs 100**, burns **1.6-4.1 s of event-loop CPU vs 0.1-0.6 s**, and stalls the event loop for up to
**0.24-0.76 s vs 15-55 ms**. Wall time for q3 is 3-48% worse with B (largest when decode is cheap and the store is
local: 14.3 s vs 7.4 s). A also makes an in-process single-flight cache trivial: 50 concurrent reads of one 10 MB
range become 1 GET / 10 MB instead of 50 GETs / 500 MB. The price is build cost: release clean build 30 s → 92 s,
stripped .so 2.5 MB → 10.3 MB (obstore's own .so, already installed, is 15 MB).

## Method

- `ext/`: a pyo3 extension. `start_a` runs a whole plan on a shared 4-worker tokio runtime with
  `object_store::parse_url_opts` (file:// and s3://) and calls back once (`loop.call_soon_threadsafe`), so the engine
  awaits one future per query. `run_a` is the same, blocking with the GIL released (`py.detach`), for
  `asyncio.to_thread`. `QueryB.step(bufs)` takes obstore's `Bytes` zero-copy through the buffer protocol, burns
  decode CPU with the GIL released and returns the next round's ranges. Optional single-flight block cache for A:
  `HashMap<(path, range), Shared<future>>`.
- `driver.py`: one configuration per fresh process. A awaits one future per query; Athread uses `asyncio.to_thread`
  (default executor: 8 threads); B fetches each range with `obstore.get_range_async` and calls `QueryB.step` through
  `asyncio.to_thread` once per round. Both cap in-flight GETs at 64 per query (A: `buffer_unordered(64)`, B: 64 worker
  coroutines per round) and issue exactly the same GETs (no range coalescing in either).
- Decode stand-in: a calibrated CPU loop (LCG, about 930 iterations/µs) plus one XOR pass over every fetched byte,
  **10 ms CPU per MB fetched** (100 MB/s, the brief's figure) for the main tables, and **1 ms/MB** (≈1 GB/s) for the
  cheap-decode q3 table. A decodes on tokio's blocking pool; B on the asyncio default executor.
- Stores: `file` = local files (page cache warm); `s3` = RustFS 1.0.4 in Docker (host network); `s3lat` = the same
  through toxiproxy with a 30 ms latency toxic on the request direction, so both variants pay the same 30 ms per GET.
  MinIO is no longer pullable (Docker Hub/quay deny, dl.min.io returns 410), hence RustFS.
- Layer files: random bytes, 2000, 800, 400, 200, 100, 50, 20, 10, 10, 10 MB (+ a 10 MB `sf.bin`).
- Shapes (random offsets per query, seeded, identical for A and B):
  q1 diff 1 day first 10K: rounds of 4×32 KB (tails), 4×100 KB, 4×300 KB = 12 GETs, 1.73 MB.
  q2 scan at head 1%: 3×64 KB, 10×200 KB, 20×790 KB = 33 GETs, 18.0 MB.
  q3 1K scattered gets: 10×64 KB, 100×64 KB, 2,300×55 KB = 2,410 GETs, 133.5 MB.
  sf: one 10 MB GET of the same object.
- Measured: wall; p50/p95 per query; process CPU (getrusage); event-loop thread CPU (`time.thread_time` on the loop
  thread); worst lag of a task sleeping 10 ms in a loop; crossings into our extension; Python awaitables created
  (Tasks via a counting task factory + obstore futures + executor futures + A's completion future); peak RSS.
  Each query is preceded by a warm-up q1 in the same process (connection pools, runtimes).
- Every configuration ran 3 times (repeats interleaved across configurations, one at a time); tables show medians.
- Reproduce: `run_matrix.py out.jsonl` then `summarize.py out.jsonl` (see `setup_stores.py` for the bucket and proxy).

## Results (medians of 3)

### One query alone (decode 10 ms/MB)

| shape | store | variant | runs | wall ms | CPU s | loop CPU ms | max lag ms | crossings | awaitables |
|---|---|---|---|---|---|---|---|---|---|
| q1 | file | A | 3 | 25 | 0.02 | 0.5 | 0 | 1 | 2 |
| q1 | file | B | 3 | 24 | 0.02 | 1.9 | 1 | 4 | 28 |
| q1 | s3 | A | 3 | 27 | 0.02 | 0.6 | 1 | 1 | 2 |
| q1 | s3 | B | 3 | 26 | 0.03 | 1.8 | 1 | 4 | 28 |
| q1 | s3lat | A | 3 | 121 | 0.02 | 1.7 | 1 | 1 | 2 |
| q1 | s3lat | B | 3 | 123 | 0.03 | 3.4 | 2 | 4 | 28 |
| q2 | file | A | 3 | 260 | 0.23 | 2.9 | 1 | 1 | 2 |
| q2 | file | B | 3 | 197 | 0.21 | 6.1 | 2 | 4 | 70 |
| q2 | s3 | A | 3 | 218 | 0.22 | 3.0 | 3 | 1 | 2 |
| q2 | s3 | B | 3 | 213 | 0.23 | 6.9 | 2 | 4 | 70 |
| q2 | s3lat | A | 3 | 318 | 0.23 | 3.9 | 4 | 1 | 2 |
| q2 | s3lat | B | 3 | 311 | 0.23 | 7.4 | 5 | 4 | 70 |
| q3 | file | A | 3 | 1564 | 1.68 | 17.9 | 2 | 1 | 2 |
| q3 | file | B | 3 | 1675 | 1.94 | 53.8 | 9 | 4 | 2552 |
| q3 | s3 | A | 3 | 1974 | 1.75 | 23.5 | 2 | 1 | 2 |
| q3 | s3 | B | 3 | 2102 | 2.03 | 74.4 | 15 | 4 | 2552 |
| q3 | s3lat | A | 3 | 2673 | 1.76 | 28.5 | 7 | 1 | 2 |
| q3 | s3lat | B | 3 | 2747 | 2.17 | 152.5 | 8 | 4 | 2552 |

### 50 concurrent queries (decode 10 ms/MB)

| shape | store | variant | runs | wall s | p50 s | p95 s | CPU s | loop CPU s | max lag ms | crossings | awaitables | GETs | MB | peak RSS MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| q1 | file | A | 3 | 0.32 | 0.31 | 0.31 | 1.0 | 0.01 | 28 | 50 | 100 | 600 | 86 | 71 |
| q1 | file | B | 3 | 0.34 | 0.27 | 0.34 | 1.1 | 0.03 | 25 | 200 | 1400 | 600 | 86 | 98 |
| q1 | s3 | A | 3 | 0.45 | 0.40 | 0.45 | 1.1 | 0.01 | 40 | 50 | 100 | 600 | 86 | 126 |
| q1 | s3 | B | 3 | 0.50 | 0.43 | 0.49 | 1.1 | 0.03 | 27 | 200 | 1400 | 600 | 86 | 129 |
| q1 | s3lat | A | 3 | 0.54 | 0.52 | 0.54 | 1.1 | 0.01 | 22 | 50 | 100 | 600 | 86 | 122 |
| q1 | s3lat | B | 3 | 0.66 | 0.59 | 0.66 | 1.2 | 0.04 | 38 | 200 | 1400 | 600 | 86 | 133 |
| q2 | file | A | 3 | 2.72 | 2.60 | 2.72 | 10.1 | 0.03 | 45 | 50 | 100 | 1650 | 900 | 710 |
| q2 | file | B | 3 | 2.89 | 1.93 | 2.82 | 10.4 | 0.10 | 96 | 200 | 3500 | 1650 | 900 | 815 |
| q2 | s3 | A | 3 | 3.39 | 2.98 | 3.36 | 10.3 | 0.04 | 20 | 50 | 100 | 1650 | 900 | 944 |
| q2 | s3 | B | 3 | 3.58 | 2.63 | 3.55 | 10.7 | 0.12 | 60 | 200 | 3500 | 1650 | 900 | 1043 |
| q2 | s3lat | A | 3 | 4.01 | 3.71 | 4.00 | 10.4 | 0.04 | 38 | 50 | 100 | 1650 | 900 | 972 |
| q2 | s3lat | Athread | 3 | 3.62 | 2.00 | 3.50 | 10.2 | 0.03 | 19 | 50 | 100 | 1650 | 900 | 243 |
| q2 | s3lat | B | 3 | 4.00 | 3.04 | 3.95 | 10.7 | 0.13 | 75 | 200 | 3500 | 1650 | 900 | 1108 |
| q3 | file | A | 3 | 24.09 | 22.98 | 23.54 | 85.5 | 0.24 | 35 | 50 | 100 | 120500 | 6677 | 6127 |
| q3 | file | B | 3 | 32.53 | 24.87 | 31.52 | 101.3 | 2.40 | 762 | 200 | 127600 | 120500 | 6677 | 6283 |
| q3 | s3 | A | 3 | 51.80 | 50.92 | 51.75 | 87.6 | 0.58 | 39 | 50 | 100 | 120500 | 6677 | 6776 |
| q3 | s3 | B | 3 | 54.36 | 46.55 | 54.28 | 98.9 | 2.34 | 549 | 200 | 127600 | 120500 | 6677 | 6725 |
| q3 | s3lat | A | 3 | 65.37 | 63.90 | 65.24 | 87.7 | 0.64 | 55 | 50 | 100 | 120500 | 6677 | 6727 |
| q3 | s3lat | Athread | 3 | 53.68 | 28.88 | 53.53 | 84.5 | 0.49 | 51 | 50 | 100 | 120500 | 6677 | 1210 |
| q3 | s3lat | B | 3 | 70.99 | 62.09 | 70.36 | 100.3 | 4.07 | 244 | 200 | 127600 | 120500 | 6677 | 6930 |

### 50 concurrent q3, cheap decode (1 ms/MB)

| shape | store | variant | runs | wall s | p50 s | p95 s | CPU s | loop CPU s | max lag ms | crossings | awaitables | GETs | MB | peak RSS MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| q3 | file | A | 3 | 7.39 | 7.16 | 7.23 | 23.4 | 0.09 | 15 | 50 | 100 | 120500 | 6677 | 5166 |
| q3 | file | B | 3 | 14.28 | 13.47 | 14.17 | 35.7 | 1.56 | 666 | 200 | 127600 | 120500 | 6677 | 6374 |
| q3 | s3 | A | 3 | 30.99 | 30.69 | 30.95 | 24.6 | 0.35 | 45 | 50 | 100 | 120500 | 6677 | 6714 |
| q3 | s3 | B | 3 | 36.95 | 36.07 | 36.91 | 39.1 | 2.04 | 673 | 200 | 127600 | 120500 | 6677 | 6818 |
| q3 | s3lat | A | 3 | 46.88 | 46.27 | 46.86 | 27.0 | 0.49 | 39 | 50 | 100 | 120500 | 6677 | 6559 |
| q3 | s3lat | B | 3 | 53.96 | 52.94 | 53.91 | 39.5 | 3.96 | 383 | 200 | 127600 | 120500 | 6677 | 6838 |

### Single flight: 50 concurrent queries reading the same 10 MB object

| shape | store | variant | runs | wall s | p50 s | p95 s | CPU s | loop CPU s | max lag ms | crossings | awaitables | GETs | MB | peak RSS MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| sf | s3 | A | 3 | 1.67 | 1.58 | 1.66 | 5.6 | 0.02 | 26 | 50 | 100 | 50 | 500 | 504 |
| sf | s3 | Acache | 3 | 1.39 | 1.30 | 1.38 | 5.1 | 0.01 | 14 | 50 | 100 | 1 | 10 | 45 |
| sf | s3 | B | 3 | 1.63 | 1.00 | 1.59 | 5.6 | 0.03 | 101 | 100 | 200 | 50 | 500 | 523 |
| sf | s3lat | A | 3 | 1.75 | 1.69 | 1.75 | 5.6 | 0.02 | 22 | 50 | 100 | 50 | 500 | 511 |
| sf | s3lat | Acache | 3 | 1.42 | 1.34 | 1.40 | 5.1 | 0.01 | 16 | 50 | 100 | 1 | 10 | 44 |
| sf | s3lat | B | 3 | 1.74 | 1.17 | 1.71 | 5.5 | 0.05 | 118 | 100 | 200 | 50 | 500 | 518 |


### Dependency cost (native/ copy, `cargo build --release --features pyo3/extension-module`, clean, 2 runs each)

| | clean release build | .so | stripped .so | crates in tree |
|---|---|---|---|---|
| native/ today | 30.0 / 30.3 s | 3.2 MB | 2.6 MB | 52 |
| + object_store 0.14 (aws, default rustls/aws-lc-rs) + tokio rt-multi-thread + one function using them | 90.0 / 95.3 s | 12.3 MB | 10.3 MB | 207 |

Crates downloaded beforehand (not in the time). The "after" copy calls `parse_url_opts` + `get_range` on a runtime so the
linker keeps the code.

## Reading the numbers

- **Per-GET cost of B.** q3 at 1 ms/MB: process CPU 35.7 vs 23.4 s (file), 39.1 vs 24.6 s (s3), 39.5 vs 27.0 s (s3lat)
  for 120,500 GETs: 100-120 µs extra CPU per GET, of which 13-33 µs lands on the event-loop thread. At 10 ms/MB decode
  the same gap (+11 to +16 s) is hidden under 67 s of decode, but loop lag remains 0.24-0.76 s.
- **q1/q2** (12 and 33 GETs per query): B's extra 1,300-3,400 awaitables per 50 queries cost nothing visible; wall,
  CPU and RSS are within run-to-run noise (q2 50× s3lat walls: A 3.80-4.12 s, B 3.91-4.09 s).
- **S3 rows are bounded by the S3 server**, which shares the 4 vCPUs (q3 50× on s3 at 1 ms/MB: 31 s wall with 25 s of
  process CPU). Real S3 would not eat our CPU, so the file rows show the boundary's own cost more cleanly; the s3 rows
  understate the gap.
- **Concurrency control matters more than the boundary for latency and memory.** Athread (to_thread, so at most 8
  queries in flight) beat unbounded A on q3 s3lat: wall 53.7 vs 65.4 s, p50 28.9 vs 63.9 s, peak RSS 1.2 GB vs 6.7 GB.
  Running all 50 q3 at once holds 50 × 134 MB of blocks. Whichever variant wins, the engine should admit a bounded
  number of heavy queries (or bound bytes in flight).
- **p50 is lower for B at equal p95** in several rows: B's decode runs on an 8-thread executor (FIFO), A's on an
  unbounded blocking pool (fair sharing), so B finishes some queries early and the last at the same time. It is a
  scheduling artifact, not a boundary cost.
- **Single flight.** Acache: 1 GET and 10 MB from the store instead of 50 GETs and 500 MB, RSS 45 MB vs 505 MB; wall
  improves only 1.67 → 1.39 s because each query still decodes its own 10 MB (5 s of CPU). Sharing decoded blocks would
  cut that too (not measured). B could do single-flight in Python with a dict of futures, but every waiter is still a
  Python awaitable.

## Caveats and what was trimmed

- Noise: q3 walls vary up to ±15% between repeats (e.g. A s3lat 62.1-71.4 s, B s3lat 59.2-75.0 s), so the q3 wall gap
  on S3 is suggestive, not proven; the CPU, loop-CPU, lag and awaitable gaps are consistent in every repeat.
- RustFS instead of MinIO; first attempt hit RustFS's 1024 open-files limit (HTTP 500 under 1,000 connections),
  fixed with `--ulimit nofile`; results are from the rerun only.
- Decode is a CPU stand-in, not real zstd/varint decoding; A decodes on an unbounded blocking pool, B on 8 threads.
- B uses one `get_range_async` per GET as specified. `obstore.get_ranges_async` (one awaitable per file, coalescing
  nearby ranges) would cut B's awaitables but also changes the GET count; not measured.
- Trimmed: Athread only for q2/q3 on s3lat; cheap decode only for q3; single flight only on s3/s3lat; no A-to_thread
  with a larger executor; build cost measured with 2 runs, not 3.
