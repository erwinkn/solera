# Engine-resolved commits — design

Status: **target design**, not built (decision D4). It replaces the
object-store `.ask` protocol of the earlier proposal, which two reviews
rejected; it keeps that proposal's idea, a warm engine cache. It changes
how a keyed write learns what it changed (`object-store-state.md` §6,
"Compute a delta"), how a downstream attempt receives small pending
windows, and what the worker does when no warm reader is available. It
goes to one review with `lifecycle.md` (the worker → engine HTTP channel)
and `per-key-processing.md` (new readers of the cache, §8).

## 1. Why

A keyed write must know, for each key it writes, whether the key is new,
changed or unchanged: the index records only real changes, the store writes
only what must be written, and downstream consumers see only real changes.
Today the worker answers that against the index on S3, and a worker is a
cold reader.

Measured at 100M keys in steady state (`bench/keys/results.md`, MinIO with
30 ms injected per request and 80 MB/s per connection):

| 1K random keys, cold worker | GETs | Bytes read | Wall |
|---|---|---|---|
| lookup | 48 | 445 MB | 765 ms |
| lookup + delta PUT | 50 + 1 PUT | 446 MB | 817 ms |
| lookup, worker disk cache warm | 0 | 0 | 311 ms (decoding blocks) |

Most of the 445 MB is the tail of every file — its filters — fetched again
on every commit to answer a question about 1,000 keys. The engine, by
contrast, sees every index file come into existence: it writes compaction
outputs and commits every delta. Index files are immutable, so a copy it
keeps is never stale. A warm engine can answer the same question with no
GETs.

## 2. The design in one paragraph

The worker sorts what it intends to write and sends it to the engine with
`POST /resolve`. The engine answers from its warm cache with the exact
delta and counts, computed purely from the snapshot the attempt pinned; it
persists nothing and writes no object. The worker uploads the delta as its
own delta file, takes the fence and applies today's repair rules, exactly
as if it had computed the delta itself. If the engine cannot be reached,
declines or is too slow, the worker resolves locally: a sparse exact
reader for small patches, a streaming merge-join for replacements and
dense patches. The read planner is deleted. The same cache inlines small
pending windows into downstream specs and serves the per-key readers.

Correctness never depends on the resolver: its answer is a pure function
of the pinned snapshot and the request, the worker could compute the same
delta, and every durable decision — delta file, fence, result, journal —
is made exactly where it is made today.

## 3. Two questions per keyed output

The earlier proposal let "the delta is empty" mean "write nothing". That
is wrong after a dead attempt (review finding 1):

```
committed           a=1
attempt 1           writes a=2 to the store, dies before committing    → unsettled intent {a}
attempt 2 returns   a=1
delta vs the index  empty: the index still says a=1
the store           still holds a=2 — it must be overwritten
```

So the worker asks two separate questions, and the resolver answers only
the first:

1. **What changes in the index?** The delta: entries whose version differs
   from the pinned snapshot, deletions of live keys the write removes, and
   exact `added` / `removed`. This is what the resolver computes, and what
   becomes the batch's delta file.
2. **What must the store write?** The delta, plus repair. Today's rules,
   unchanged and owned by the worker:

| Write | Unsettled intents | Store writes |
|---|---|---|
| Patch | none | the delta's upserts and removes |
| Patch | some | first `_repair`: keys dead attempts meant to change and this patch does not mention are read back from the store and folded into the run *before* it is resolved. Then the delta, plus the patch's own keys that an intent touched (`own ∩ intended`), even when unchanged in the index |
| Replacement | none | the delta's upserts and removes; the whole scope if the delta's key list was not collected |
| Replacement | some | the whole scope. `_repair` does not run: it would keep stray rows the replacement means to remove |
| `Sql` | — | the statement; the store reports its key map afterwards, resolved as a replacement |

An output is **unchanged** — no store write, no new batch, head kept —
only when the delta is empty, the output exists, and no intent is
unsettled. An empty delta with unsettled intents is still a commit: no
delta file, but the repair writes and the settlement that clears the
intents.

## 4. The resolver protocol

`POST /api/attempts/{attempt}/resolve` on the worker → engine HTTPS channel
of `lifecycle.md`, authenticated as that channel authenticates the attempt
(its token is bound to the attempt and invocation). One request per
attempt that has something to resolve, covering every output the worker
wants resolved; outputs it resolves itself (big writes, `Sql`, unkeyed)
are simply absent.

**Request.** A binary body: a `u32` header length, a JSON header, then one
payload per output.

```json
{"attempt": "01J9…", "invocation": "k3f…", "outputs": [
  {"name": "orders", "scope": "", "batch": 12,
   "base": {"prefix": "keys/orders/_/", "head_batch": 11},
   "kind": "patch", "keys": 1000, "offset": 0, "size": 41250, "digest": "9e07…"}
]}
```

- The payload is the sorted run as a `.kx` file (`key-index-format.md`):
  `(key, version, deleted)` with `deleted` for removes. One format, one
  parser, the CRCs included.
- `base` binds the request to the pinned snapshot: the index (its prefix,
  which survives renames) and the head's batch as pinned in the spec.
  `batch` is the batch the delta will carry.
- `digest` is the payload's XXH3-128, for deduplication.

**Response.** Same framing: a header, then each resolved output's delta.

```json
{"outputs": [
  {"name": "orders", "result": "delta", "added": 212, "removed": 3, "entries": 640,
   "offset": 0, "size": 26810},
  {"name": "events", "result": "empty"},
  {"name": "sessions", "result": "declined", "reason": "cold"}
]}
```

- `delta`: the payload is the delta as a complete `.kx` file, ready to
  upload under the worker's own name `{batch:012d}-{attempt}`. The worker
  checks its CRCs and footer, uploads the bytes, and builds `DeltaFiles`
  from them with the header's `added` and `removed`, `exact: true`. A
  response is capped at `resolve_max_bytes` (16 MB), so it is one file —
  below the 64 MB split.
- `empty`: the write changes nothing in the index. No file; `DeltaFiles([],
  0, 0, exact=True)`. §3 decides whether that means "unchanged".
- `declined`: the worker resolves this output locally (§6), at once.
  Reasons: `cold` (the snapshot is not fully in the cache), `busy` (queue
  full), `too_big` (over the key, byte or replacement limits), `stale`
  (`base` does not match the index the engine holds for that scope),
  `not_live` (the attempt is not running, or does not hold that scope),
  `corrupt` (a local copy and its S3 object both failed validation).
- Overload of the whole engine is `503` with no body; transport errors and
  timeouts count as declines.

**Exact counts.** The engine resolves against full entries, never through
filters, so `added` and `removed` are exact, and an engine-resolved commit
never increments `inexact`.

**Which snapshot.** While an attempt is live it holds its scope lock: no
other commit can change that index, and compaction and recounts change its
files and count fields but not its content. The engine therefore resolves
against the file set it currently holds for that scope, after checking
that the attempt still holds the lock and that `base.head_batch` is the
head's batch; the content is the pinned snapshot's even if compaction
swapped files since prepare. It pins that file set for the duration of
the resolve (§5).

**Idempotent by construction.** The answer is a pure function of the
snapshot content and the payload, and the engine writes nothing, so a
retried or duplicated request — after a dropped connection, an engine
restart, a second invocation of the same attempt — gets the same answer or
a decline. Concurrent requests with the same `(attempt, output, digest)`
share one computation. Requests that differ (two invocations writing
different rows) are both answered; the fence decides which invocation
writes, as today. There is no request to freeze, no response to recover
after a restart, and no engine-written file to clean up — review findings
2, 3 and 4 have nothing left to apply to.

**Mixed attempts.** Each output is independent: one may be resolved by the
engine, one declined and resolved by a streaming merge-join, one written
by `Sql`, and the attempt commits them together. The worker uses exactly
one answer per output, chosen before it uploads that output's delta.

**Waiting.** The worker waits at most `min(resolve_timeout, remaining
deadline)` — `resolve_timeout` is 5 s, a few times the slowest warm resolve
we expect — and then resolves locally. A late response is dropped
unread. Nothing the engine did needs undoing: it wrote nothing.

**Limits** (all to be set from the benchmark of §10, starting values):

| Limit | Start | Applies to |
|---|---|---|
| `resolve_max_keys` | 100K run entries | patches |
| `resolve_max_bytes` | 16 MB | request and response |
| `resolve_max_entries` | 2M physical entries in the snapshot | replacements: the engine merges the run with every entry |
| `resolve_queue_bytes` | 64 MB of queued payloads | admission; beyond it, `busy` |
| `resolve_concurrency` | 2 | resolves in flight, on their own threads |

The worker sends a patch only when it is under `resolve_max_keys` and a
replacement only when the pinned count plus its run is under
`resolve_max_entries`; the engine checks again.

**Failure cases.**

| Case | Outcome |
|---|---|
| Engine unreachable, restarting, or slow | Local resolve after the timeout; same delta, possibly an inexact count (§6) |
| Worker dies after the response, before the fence | Nothing to clean: the engine wrote nothing; an uploaded delta is the worker's own and is discarded at attempt end as today |
| Worker dies after the fence | Unsettled intents and repair, unchanged |
| Attempt canceled while a resolve runs | The resolve checks liveness when it starts and between chunks, and stops; its pins are released |
| Compaction commits during a resolve | The resolve keeps the file set it pinned; GC waits for the pin |
| Two invocations of one attempt | Both may be answered; one takes the fence |
| Local copy corrupt | Dropped, refetched from S3 and rebuilt; the request is declined `cold` meanwhile |

## 5. The engine cache

One cache serves every reader on the engine: resolves, inlined windows,
compaction (which reads what it just wrote), recounts, and the per-key
readers (§8). It runs on the maintenance threads, never on the engine's
event loop.

**Unit: an immutable file.** Entries are keyed by object path; a path is
never reused, so an entry is never stale and never invalidated, only
evicted. An index is "warm" when every file of the snapshot is present.

**Local form.** On disk, each file becomes `{name}.kxl`: its blocks
decompressed, with a restart point every 16 entries (offset of a full,
unshared key), and a header:

```
magic "KXL1" · source size · source footer CRC · blocks · per block: local offset, length, entries, CRC32C
```

A lookup binary-searches the block index (in RAM), then the block's restart
points, then scans at most 16 entries: microseconds, where decoding a
compressed 64 KB block costs ~0.45 ms. The S3 format does not change.

**Integrity** (review finding 6). The source block's CRC covers compressed
bytes and says nothing about the decompressed form, so the local form
carries its own: CRC32C over each block's entries *and* its restart table.
A block is verified each time it is read from disk (~10 µs for 128 KB);
local files are written to a temporary name and renamed into place, so a
crash leaves no half-written file under a real name. A mismatch drops the
local file, refetches the source, validates it against its own CRCs and
rebuilds; if the source fails too, the read fails with `corrupt`.

**Budgets, separate:**

- `cache_disk` (default 16 GB): local files. A 100M-key index in steady
  state holds ~125M physical entries across its levels — not 100M — at
  ~40 B decompressed: ~5 GB.
- `cache_ram` (default 512 MB): block indexes of every local file (~0.01 B
  per entry: ~1.3 MB at 100M), plus an LRU of hot decompressed blocks, plus
  the inline summaries of §7 and §8. Filters are not cached: a warm reader
  never needs them.

**Pins.** A reader pins the file set it reads; eviction skips pinned
files. A reader that fetches from S3 is also a reader pin in the garbage
order of `object-store-state.md` (pins and deletions by event position),
so compaction cannot delete a file under a fill.

**Fills, deduplicated.** One fill per path at a time; a second reader
awaits the first. A fill streams the file in 8 MB segments, as the
streaming merge-join does, and builds the local form as it goes. A resolve
never waits on a fill: if its snapshot is not warm, it declines `cold` and
queues fills for the missing files (if admission allows, below). The
worker's local resolve and the engine's fill then run side by side once,
and the next commit is warm.

**Write-through.** The engine never reads what it produced or saw:

- compaction outputs are installed from the bytes the engine has in hand;
- a resolve keeps the delta bytes it returned as a **candidate**, keyed by
  `(attempt, output, size, CRC)`: present in the byte cache, invisible as
  index state. When the attempt commits, the committed delta's size and CRC
  match the candidate (the worker uploaded those exact bytes), and the
  candidate becomes that file's local form without a GET. Candidates of
  attempts that end without committing are dropped;
- deltas the engine did not produce (a local resolve, a source commit
  resolved elsewhere) are fetched once at commit, on the maintenance
  thread — they are small.

**Admission and eviction** (review finding 10). An LRU over files thrashes
on the simplest bad case:

```
budget 6 GB; index A 5 GB, index B 5 GB; commits alternate A, B, A, B…
LRU: every commit evicts the other index and refills its own — 5 GB of GETs per commit
```

So the cache admits by index and evicts by file, with hysteresis:

- An index is **admitted** when its snapshot fits beside the indexes
  **active** within `cache_window` (default 15 minutes) — those that served
  a reader in that window. Otherwise it stays cold: its writers resolve
  locally, with no thrash.
- Eviction takes files of inactive indexes first (least recently used),
  then superseded files (inputs of a compaction, still pinned by no one),
  and never files of an active index.
- In the example, A is admitted and B declined; B's workers pay the cold
  path until A has been idle for the window. Two indexes that both fit are
  both warm.

Block-granular eviction — keeping only the touched part of a big index —
helps only clustered workloads and is deferred until a benchmark shows one.

## 6. The cold path, without the planner

When the engine cannot answer, the worker resolves alone. Two readers,
chosen by one rule each; `_plan_reads`, `_read_options`, `Cost`, the
estimate and the CPU-rate options are deleted.

**Sparse exact reader**, for small patches. Today's filtered path minus
the planner:

1. read every level small enough (≤ 32 MB) and every level-0 file whole,
   all at once, and resolve newest first;
2. for the larger levels, read the tails of the files whose key range
   covers a written key, all at once;
3. classify each key with the filters: absent (no key filter matched),
   changed (no pair or tombstone filter matched: live at another version),
   or maybe;
4. read the blocks of the maybe keys, only in files whose key filter
   matched, all levels at once; each key takes its newest entry.

The delta is exact. The count is exact unless a pair filter cleared a key
behind a key-filter false positive (0.35% per check), which increments
`inexact` as today. Dropping the pair-filter shortcut would make the count
exact on this path too, at ~1.25 block reads per changed key instead of
~0.01: ~1,250 more GETs for 1K keys at 100M, ~$0.0005 per commit. That is
the price of removing recounts altogether; this design keeps them (§11).

**Streaming merge-join**, for replacements and dense patches: the native
job that full replacement, compaction and recount already use, extended to
patches — it streams every level in 8 MB segments, merges them with the
sorted run, and emits the delta as it goes. At 100M keys it reads 2.4 GB in
371 GETs; memory is the buffers, whatever the size.

**The crossover, measured.** A patch goes to the sparse reader, then
switches at most once:

- **Before reading:** a patch denser than `stream_density` — run entries
  per physical entry in the snapshot — streams directly. Every block will
  be decoded anyway, and the tails and filter checks are wasted work.
- **After the filters:** if the maybe keys' block reads exceed
  `stream_reads` × the streaming read count (the snapshot's bytes / 8 MB),
  stream instead of reading blocks. The tails already read are not
  repeated: streaming reads index parts and data segments.

Both constants come from one grid, run cold and warm:

| Variable | Values |
|---|---|
| index size | 1M, 10M, 100M; fresh and steady state |
| patch size | 1K, 10K, 100K, 1M keys |
| share rewritten unchanged | 0%, 50%, 100% |
| path | sparse, streaming, forced each way |
| report | GETs, bytes, wall, CPU seconds, peak memory |

What we already have suggests where they land, but does not set them:
1M keys into 10M (10% density) took 7.0 s reading the level whole and
8.4 s through the filters; 100K keys at 100M with half unchanged took
17.4 s and 7,395 GETs through the filters, against 371 GETs and ~14 s
streaming.

## 7. Inlined downstream changes

When the engine prepares an attempt whose `Incremental` edge reads a keyed
upstream, it can put the first page of the pending window in the spec
instead of making the worker read deltas:

```json
"changes": {"from": 56, "to": 57, "after": null, "limit": 10000, "full": false,
            "inline": {"upserted": {"alpha-file-2": "3f9c…"}, "deleted": ["alpha-file-7"],
                       "next": null}}
```

The rules (review recommendation 7):

- **The pinned window, nothing newer.** The page merges exactly the delta
  files of batches `from…to` in the spec's pinned log, newest batch
  winning per key. A batch 58 committed after prepare is not in it, even
  though the engine holds it.
- **The same paging as `pending`.** Keys `> after`, at most `limit`, in key
  order, tombstones kept as deletions. `next` is the continuation cursor
  (`null` when the window is exhausted); the worker reports `delivered` as
  for a paged window, and the next attempt continues from `next` — inlined
  or not.
- **A byte cap.** At most `inline_max_bytes` (1 MB) of serialized page; a
  page that reaches it ends early with `next` set.
- **Never waiting in prepare, on S3 or on disk.** Prepare reads only RAM:
  at commit, on the maintenance thread, the engine keeps for each committed
  delta that is small (≤ `inline_max` entries, 10K) its sorted entries in
  RAM, within the `cache_ram` budget, until the consumers' watermarks pass
  the batch. A window whose batches all have their summaries is merged in
  RAM at prepare (a k-way merge of a few small sorted lists); a window
  missing any summary — after a restart, or with a big batch — goes out as
  today, and the worker pages it from the index.

## 8. Readers for per-key processing

`per-key-processing.md` adds three readers. Each is a maintenance-thread
computation from the cache, published to the engine loop as a small
record; none runs on the scheduling path, and none adds cached content
beyond failure indexes (themselves ordinary indexes).

| Reader | When | Computes | If the input is not cached |
|---|---|---|---|
| Pattern match counts | per committed delta, per consuming edge with `include`/`exclude` | matching keys per (edge, batch), and the keys themselves when ≤ `inline_max` (they become that edge's inline summary, §7) | unknown: the window is launched, and the worker filters |
| Failure index summary | after each commit of a failure delta | `due` (earliest `next_at`) and `epoch_min`, exact, by streaming the failure index's local form; failure indexes are small unless something fails systemically | lower bounds, as the per-key doc says; the worker that finishes a retry pass reports the exact values |
| Due-retry page | when `due` passes, and after each failure commit | the next page of due entries (`next_at ≤ now`, `> retry.after`), held in RAM for prepare to inline | the spec pins the failure index and the worker pages through it |

The same "never wait in prepare" rule applies: prepare uses these records
if they are there.

## 9. Costs

Prices: GET $0.40 and PUT $5.00 per million, so **a PUT is 12.5 GETs**;
storage $0.023/GB-month. Scenario E: 1K random changes every 10 s into
100M keys, 259,200 commits a month, steady state, per commit:

| | Today (measured) | Resolver, warm (projected) |
|---|---|---|
| Worker index GETs | 50 | 0 |
| Worker index PUTs | 1 (delta) | 1 (delta) |
| Engine index GETs | 0 | 0 (write-through) |
| Compaction (simulated, `amplification.py`) | 1.18 GET + 0.144 PUT | same |
| GET-equivalents | 50 + 12.5 + 1.18 + 1.8 = **65.5** | 12.5 + 1.18 + 1.8 = **15.5** |
| Key index / month | $6.79 | **$1.60** |
| Attempt + journal overhead / month | $6.69 | $6.69 (the lifecycle doc's subject) |
| Total / month, with storage | $13.54 | **$8.35** |

The resolver adds no object requests; HTTP traffic within the region is
free. Two costs it adds elsewhere:

- **Cold fills.** One streaming read of a snapshot when an index is
  admitted: ~430 GETs and ~3.4 GB at 100M in steady state — what seven of
  today's commits cost in requests — once per admission, not per commit.
- **The cold path when declined.** Unchanged from today's figures: ~50
  GETs per 1K-key commit at 100M, ~371 GETs for a streaming patch.

**Latency and CPU, kept apart.** For a warm 1K-key resolve at 100M:

| | Today (measured) | Resolver (estimate, to measure) |
|---|---|---|
| Network | ~2 rounds of 30 ms, ~445 MB at 80 MB/s per request | 1 HTTPS round trip in region (~1–5 ms) with a ~40 KB request |
| CPU | filter checks and block decoding inside the 765 ms | ~2.8 lookups per key × a few µs ≈ 10–20 ms on the engine |
| Delta upload | 1 PUT, ~30 ms | same |
| Time to delta | 765 ms | ~20–30 ms, then the PUT |

The CPU figure is an estimate from the local form's design, not a
measurement; the benchmark below measures time to delta, engine CPU per
resolve and engine RSS, separately.

## 10. Tests and benchmarks

- Engine-resolved and locally resolved deltas are byte-identical, with
  equal counts, over random patches, removes, replacements, repairs and
  compactions between prepare and resolve (property test against
  `_python.py`).
- Repair: the `a=1 → a=2 → a=1` sequence of §3 rewrites the store with an
  empty delta; a replacement with unsettled intents rewrites the scope;
  "unchanged" only without intents.
- Protocol: duplicate and concurrent requests share one computation; a
  decline of each reason falls back locally; a timeout falls back and the
  late response is dropped; `stale` after a head move; `not_live` after
  cancel; mixed attempts (engine, local, `Sql`) commit together.
- Cache: the alternating-indexes case stays warm for one and cold for the
  other without refills; a corrupted local block is refetched; a
  corrupted source fails `corrupt`; a candidate delta is installed without
  a GET at commit; fills are deduplicated; pinned files survive eviction
  and GC.
- Inlining: inlined pages equal `pending` pages for the same pinned window,
  including a later batch that must not leak in, tombstones, the byte cap
  and the continuation cursor.
- Benchmarks: the crossover grid of §6; warm resolves at 1K/10K/100K keys
  into 1M/10M/100M — time to delta, engine CPU, RSS, cache disk; Scenario
  E end to end, counting every request.

## 11. What changes in the code

- `solera/keys`: delete `_plan_reads`, `_read_options`, `Cost`,
  `_estimate` and the rate options; add the patch variant of the streaming
  job and the two switch rules; keep filters and `inexact` for the sparse
  reader.
- `native/`: the local form (build, verify, lookup) and a resolve job over
  it; the in-RAM window merge.
- `solera_worker`: build the run after `_repair`; ask, wait, fall back;
  upload the returned delta; the store-write selection of §3 unchanged.
- `solera_server`: the `/resolve` route on the lifecycle channel; the
  cache, its threads, admission and fills; candidates; inline summaries
  and the per-key readers; `inline` in prepare.
- Docs: §6 "Compute a delta" and "Deliver pending deltas" in
  `object-store-state.md`; `key-index-costs.md`.

## 12. Not in this design

- **The canonical row digest** of the earlier proposal (one encoding for
  Python and Arrow rows) is independent of resolution. It is being built
  with `Rows` grouping by the native work in flight, and belongs in its own
  spec next to that code (`row-digest.md`), together with the group
  production `per-key-processing.md` §6 defines; that doc's references to
  the grammar should move there. The review's requirements stand for it: a versioned byte grammar
  with framing for rows and structs (the Arrow struct framing is fixed
  already), nanosecond timestamps, dates distinct from timestamps, decimal
  normalization, missing versus null fields, no automatic pickle fallback,
  and golden vectors across Python and Arrow. `per-key-processing.md`
  should point there for its group production.
- **An object-store channel** for resolves. Workers can always reach the
  engine over HTTPS; a worker that cannot simply resolves locally.

## 13. Where this departs from the reviews

- **No frozen request per attempt** (finding 4 asked for one, with
  conflicting retries rejected). The resolver persists nothing and decides
  nothing, so two different requests for one attempt are harmless: each
  gets a correct answer for its own rows, and the fence picks the writer.
- **Resolve against the current file set, not the pinned manifest**
  (finding 6 asked for the manifest). Under the scope lock they have the
  same content, and the current files are the warm ones; `base` and the
  lock check make a moved head a `stale` decline rather than a wrong
  answer.
- **Filters and recounts stay** (recommendation 9 removes them if every
  path is exact). The sparse reader's pair-filter shortcut is what keeps
  a declined or over-budget index at ~50 GETs per small commit instead of
  ~1,300, and indexes over the cache budget take that path on every
  commit.

## 14. Open questions

1. **Limits and constants**: `resolve_max_keys`, `resolve_max_entries`,
   `stream_density`, `stream_reads`, `resolve_timeout` — set from the
   benchmark, or derived from the snapshot (e.g. relative to its block
   count)?
2. **Admission policy**: is "fits beside the indexes active in the last
   15 minutes" enough, or do we need priorities (an index whose writers
   commit every 10 s over one written hourly)?
3. **Engine capacity**: at what commit rate does the single engine's
   resolve CPU or disk become the bottleneck, and is the planned
   `engine_executor` (`object-store-state.md` §6) the place to offload
   resolves as well as compaction — with the cache moving along?
4. **Source commits** resolve in the engine process already; should they
   use the warm cache directly (exact counts) and decline to today's
   in-process cold path like a worker would?
5. **`.kx` as the response format** spares the worker an encode, but ties
   the uploaded bytes to the engine's encoder. Should the worker re-encode
   instead, so its delta files never depend on the engine's build?
6. **Coordination with `lifecycle.md`**: route, authentication, and the
   invocation token are that doc's; this one assumes a request/response
   route on the attempt's channel with a body up to 16 MB.
