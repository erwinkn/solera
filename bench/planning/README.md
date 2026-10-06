# T45: does engine-side planning scale?

**Short answer.** Yes, once the planner streams; today's planner does not.
On a 4-vCPU engine with 100M keys and 50 tasks planning 10,000-key batches
at once, a plan takes 2–3.5 s, the engine plans 114–250k keys a second in
all, and the event loop stalls at most 0.3 s. The limit is the event loop:
it is 85–90% busy, with per-key Python classification as the cost. Move
planning to a process pool, or at least a thread. No worker fallback is
needed. Retries are the outlier, and need an index of what is due.

Everything here comes from one script, `bench/planning/plan.py`, on branch
`exp/engine-planning` (never merged):

    python bench/planning/plan.py all     # builds what is missing, runs the matrix, writes results.md

`results.md` holds every run. `results.json` holds the raw measures,
including fill times, garbage-collection pauses and cache sizes.

## What was measured

- **The index.** The upstream index is stamped layers, as `main` builds
  them. It has N keys (10M and 100M), each 41 bytes and random-looking, so
  it compresses like real keys rather than a counter. On top of the base:
  30 days of churn at 1% of the keys a day (5% removes, the rest updates).
  That is 29 daily commits, then the last day as 96 commits, then one
  commit of 100 keys. Today's merge rule runs after each commit, with the
  cut at the oldest reader, 30 days back. Results: 10M keys is 97 MiB in
  10 layers; 100M keys is 932 MiB in 19 layers.
- **The planner.** `owed.batch` over `LayerIndex`, with the engine's
  `LayerCache` shared by every plan, one fresh `LayerIndex` per plan (as
  `Observing._upstream` makes them), all on one event loop, as
  `_observe` runs today. The thread pool is sized as on a 4-vCPU machine
  (8 threads), and each run is capped at 4 CPUs and 16 GB.
- **Scenarios.**
  - Consumers behind by seconds (1 commit), a day (97 commits) and 30 days
    (the base).
  - A full run (an empty base, so every key is owed).
  - A `keys="all"` run.
  - Per-key retries. `Observing._retry`'s walk stands in: pages of the
    outcome index, 1% of keys due and scattered, up to `WALK × batch`
    records walked.
  - Each scenario with 10 and 50 concurrent tasks, each walking 3 batches
    from its own place in the key space.
- **Measures.**
  - Per-plan latency (p50/p95) and wall time.
  - Process CPU, and the event loop thread's own CPU.
  - Event-loop lag: how late a 10 ms sleeper wakes, which is the tick's
    view.
  - Garbage-collector pauses and RSS.
  - Cache disk, disk hits and object-store GETs.
  - Cold start: an S3-like 30 ms per GET, the page cache dropped, and the
    engine's cache either empty (as after a restart, which wipes it) or
    filled.

## Finding 1: today's planner costs the key space per batch, not the batch

There are three places where a batch's cost grew with N rather than the
batch size:

- **`owed.candidates`** collected a whole segment's classes before yielding
  the first, to interleave its points. A full run's batch, or one for a
  consumer far behind, therefore classed every key in the segment. At 1M
  keys, a single full-run batch took **43 s**: 993k keys classified to
  deliver 10k.
- **`_with_unchanged`** (`keys="all"`) collected the whole owed stream, then
  read each unchanged key with two Δ calls: **97 s** a batch at 1M with 10
  tasks.
- **`LayerIndex._bound`** built a Python list of every remaining block of
  every layer for each 1,000-key Δ page, on the event loop. That is
  100k blocks per page at 100M.

Commit `819b512` makes all three stream. Points are merged in as the stream
passes them; the "all" scan is merged with the candidates and reuses their
old observations; the bound is computed lazily. The planner tests (owed,
staleness including its reference property test, selection, observed
histories, layers, delta) pass unchanged. The table below compares the two,
with 10 tasks:

| per 10k-key batch, p50 | 1M as is (10 tasks) | 1M streaming | 10M as is (1 task, alone) | 10M streaming (10 tasks) |
|---|---|---|---|---|
| a day behind | 0.15 s | 0.16 s | 0.79 s (10 tasks) | 0.23 s |
| 30 days behind | 6.6 s | 0.54 s | 131 s | 0.74 s |
| full run | 34 s | 0.66 s | 706 s | 0.92 s |
| `keys="all"` | 97 s | 0.63 s | 24.5 s | 0.79 s |

(The 10M `keys="all"` run as is was cheaper than its full run, because its
owed stream is only the last day's changes. It still read every unchanged
key twice.)

Under the planner as it is, a full run over 100M keys is about 10,000
batches, and each batch classes about 100M keys. **This must be fixed
whatever the redesign:** a batch's planning cost must be bounded by the
batch.

## Finding 2: with the planner streaming, the limit is the event loop

The table is 100M keys, warm cache, 4 vCPUs, with Δ pages of 1,000 keys (as
today). Throughput is the keys planned per wall-clock second, for the whole
engine.

| scenario | 10 tasks p50 | 50 tasks p50 / p95 | throughput (50 tasks) | CPU used | loop thread busy | worst loop stall |
|---|---|---|---|---|---|---|
| seconds behind | 4 ms | 16 / 18 ms | (tiny batches) | – | – | 1 ms |
| a day behind | 0.38 s | 1.9 / 2.1 s | 250k keys/s | 2.0 cores | 89% | 55 ms |
| 30 days behind | 0.59 s | 2.7 / 3.8 s | 164k keys/s | 3.1 cores | 88% | 75 ms |
| full run | 0.65 s | 3.2 / 6.6 s | 114k keys/s | 2.9 cores | 84% | 290 ms |
| `keys="all"` | 0.69 s | 3.4 / 3.9 s | 144k keys/s | 3.5 cores | 83% | 83 ms |
| per-key retries | 13.5 s | 67 / 70 s | 7k due keys/s | 3.8 cores | 56% | 270 ms |

At 10M keys the numbers are close (results.md). For a batch's cost, the
upstream's size matters less than the number of layers a page reads.

- **CPU per key planned (30 days behind, 100M).** About 5 µs runs on the
  event loop: `Diff` and `Owe` objects, merged async generators, the
  pattern matcher. About 13 µs runs in the pool: the native scans and disk
  reads. A 10k-key batch is about 0.2 CPU-seconds, so a 4-vCPU engine tops
  out near 15–20 batch plans a second.
- **The event loop is the bottleneck.** Its thread is 82–90% busy while 50
  tasks plan. The native work in threads is not the limit.
- **The tick stays responsive, but only just.** The worst stall is under
  0.3 s against the 0.5 s eval interval, and the loop has little time left
  for the API, commits and dispatch.
- **Δ page size trades native CPU against stalls.** Each 1,000-key page
  decodes whole blocks from every layer, and small layers' blocks span
  wide key ranges, so most of what is decoded is discarded. With
  10,000-key pages, native CPU drops 2–4x (10M, 30 days behind: 47 →
  11 CPU-s). But the loop then stalls up to 1.1 s, because when 50 tasks'
  reads finish together, each task's continuation classes 10k keys in one
  step.

### Planning off the loop (Δ pages of 10,000, 50 tasks)

| where | 10M 30 days p50 | 10M full p50 | 100M 30 days p50 | 100M full p50 | engine loop worst stall |
|---|---|---|---|---|---|
| the engine's loop | 2.06 s | 2.01 s | 2.67 s | 2.85 s | 0.25–1.1 s |
| a planning thread | 2.10 s | 2.08 s | 2.61 s | 2.89 s | 0.09–0.12 s |
| 4 planning processes | 0.87 s | 1.27 s | 1.57 s | 3.15 s | 0.06–0.18 s |

- **A thread** keeps the engine's loop free at no throughput cost. It is
  bound by the GIL like the loop was, so planning gets no faster.
- **Processes** plan 1.7–2.4x faster within the same 4 CPUs for consumers
  behind (the GIL no longer serializes them), and scale with cores. A
  full run at 100M gained nothing (3.15 s against 2.85 s): without a
  shared memory tier, each process fetched and decoded the 10 MB base
  index object itself. Planning here read the
  layer files locally, which stands in for a shared warm disk cache: the
  layer files are immutable, so processes can share the cache's files
  read-only.

## Finding 3: cache, cold start, memory

- **Cache disk.** A warm index is its main parts: about 10 bytes a key, or
  932 MiB at 100M (with 30 days of churn included). The default 16 GiB
  holds about 17 such upstreams.
  - Filling one took 1.1 s locally (29 GETs). From S3 at about
    100 MB/s it would take about 10 s.
  - Warm, the remaining misses are the `.lix` index objects: `fill` keeps
    only `.lay` files on disk, and a plan holds index objects in memory.
- **Duplicate index fetches.** `_prepare` has no single-flight: 50 plans
  starting together each fetch the same index objects. At 100M that is the
  10 MB base `.lix`, so about 500 MB of GETs and 50 decodes for one
  object (results.md, 100M full: 900 GETs, 626 MB).
- **Cold start after a restart.** Planning does not fill the disk cache; only
  the resolver (on a worker's resolve request), commits and merges do. A
  restarted engine plans from the object store until resolver traffic
  happens to warm that index. Measured with 30 ms per GET and the page
  cache dropped:
  - Plans are 1.6–1.8x slower than warm: at 100M with 10 tasks, 30 days
    behind is p50 1.05 s against 0.59 s; a full run is 1.19 s against
    0.65 s.
  - That costs about 23 GETs and 15–19 MB per 10k-key batch. Windowed
    range reads keep it modest, but on S3 it is real money at scale.
- **Memory.** RSS was 0.25–2.3 GB at 100M. Each `LayerIndex` keeps its own
  64 MiB block cache, so 50 concurrent plans could hold up to 3.2 GB.
  Garbage-collector pauses stayed under 0.1 s.

## Finding 4: retries are the expensive plan

A retry batch walks the outcome index from the pass's place until
`batch_size` keys are due, or `WALK × batch_size` (1M) records are walked.
With 1% of keys due and scattered, every batch walks the full million:
5.4 CPU-seconds a batch at 100M, a p50 of 13.5 s with 10 tasks and 67 s
with 50. The walk's cost is bounded by `WALK`, not by N; it is large
because it decodes and tests a million stored outcomes to find 10,000. (The
bench uses the upstream index as the outcome index, a worst case: a real
outcome index holds only failing keys.)

## Recommendation

1. **Land the streaming planner** (`819b512`, or its equivalent in the MVCC
   planner). Without it, engine planning does not scale at all: full,
   `keys="all"` and far-behind batches cost the whole key space.
2. **Plan in a process pool beside the engine, sharing the disk cache.** The
   pool is sized to the engine's cores minus one. Layer files are immutable,
   so the processes can read the cache's files directly; each keeps its own
   memory tier; the engine keeps the journal and the cache's bookkeeping.
   This takes planning load off the event loop entirely, and on a 4-vCPU
   machine it plans 1.7–2.4x faster for consumers behind. That needs index
   objects shared too (on disk), or full runs gain nothing.
   - **A planning thread is the cheap first step:** a dozen lines, the loop
     free (stalls ≤ 0.12 s), but no faster.
   - **A worker fallback for big batches is not needed:** a plan's cost is
     bounded by its batch (about 0.2 CPU-seconds per 10k keys), and
     `keys=` lists go `batch_size` at a time.
3. **The limit, stated.** On the loop as today, one 4-vCPU engine plans
   about 114–250k keys a second, roughly 11–25 concurrent 10k-key plans a
   second, with the loop near saturation. Beyond that, planning latency
   grows linearly with concurrency: 50 tasks give 2–3.5 s plans, so 500
   would give 20–35 s. With processes, the limit becomes the cores.
4. **Smaller fixes.** Each of these is independent and cheap:
   - single-flight index-object fetches in `_prepare`;
   - keep `.lix` files on the engine's disk;
   - fill an index on its first plan after a restart, not only on resolver
     traffic;
   - one block cache shared across plans instead of 64 MiB each;
   - 10,000-key Δ pages, once planning is off the loop.
5. **Retries.** Let a retry plan read only what is due: an outcome index
   keyed or bucketed by due time, or a small due-set kept beside it.
   Today's walk is the most expensive plan by 10–20x.

## What would differ under MVCC entries

The target design's change index is Fable's §4 / T43: entries carry
`replaced` and `new`, `diff(C1, C2)` emits both versions, `scan(C)` serves
full and "all" runs, and the file format is decided by D183's benchmark.

- **Same work, same bound.** Planning is still a merge of the layers that
  overlap a window, restricted to a key range, then per-key
  classification. The costs measured here carry over, including the need to
  stream: a batch must never cost the key space.
- **Fewer reads per key for lists.** `get` and `diff` return the old version
  in the entry. Today, a key in a `keys=` list or a point needs a second Δ
  read to decode its old version (`_decoded`, two native calls per key).
  That goes away.
- **Bigger entries.** Two versions per entry, Fable's ~16 bytes a change,
  means somewhat more bytes to decode per key. Native decode is about
  13 µs/key today with 1,000-key pages, mostly decode amplification, and
  the page size dominates entry size.
- **Classification is the real lever.** The engine-side bottleneck is
  Python per-key classification (about 5 µs/key on the loop), whatever the
  layer format. Classifying in native or vectorized code would cut it
  about 10x. A Parquet format read through Arrow fits that, if the
  comparison runs on arrays rather than per-key Python objects.
- **Cache size.** A Parquet base of 100M keys is estimated at 1.2–1.6 GB,
  against 0.93 GB here. Expect about 1.5x the cache disk.

## Caveats

- **A local store stands in for S3.** It has 30 ms injected per GET for
  cold runs and no bandwidth limit. The machine's page cache (251 GB)
  makes warm disk reads memory-speed; the cold runs drop it for our files
  (`posix_fadvise`).
- **The CPU quota sets the engine size.** One process is capped at 4 CPUs
  to stand for a 4-vCPU engine. The machine has 96 cores, so nothing else
  competed.
- **Key distribution.** Keys are uniformly random within the key space,
  and so is churn. Clustered churn (by site, by day) would make Δ pages
  denser and plans cheaper.
- **Python version.** The venv runs CPython 3.14.4 (the server's
  migration). The CPU numbers would differ slightly on 3.12 and 3.13.
