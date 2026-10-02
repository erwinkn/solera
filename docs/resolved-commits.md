# Engine-resolved commits

Status: **built** (decision D4); §14 lists where the code departs from the
text. How a keyed write learns what it changed (`object-store-state.md` §6,
"Compute a delta"): the engine answers from a warm cache of the key index,
and the worker resolves locally when it cannot. Also how a downstream
attempt receives small pending windows inline.

It depends on two other designs, and says where:

- `lifecycle.md` — the worker → engine HTTPS channel (§5), the `.worker`
  claim that admits one invocation (§4), and the store kinds `immutable`,
  `fenced` and `overwrite` (§9.6), which decide the repair rules of §3.
- `per-key-processing.md` — the failure index, whose one v1 reader here
  (inlined retry pages) follows that doc's eligibility predicate and
  transition table (§8). Its own semantics (rescoping, cancellation, retry
  pacing, sensors) belong to that doc.
- `key-index-format.md` — entries with a locator, and deltas with
  predecessors (the `.kx` bump of the row-digest work).

## 1. Why

A keyed write must know, for each key it writes, whether the key is new,
changed or unchanged: the index records only real changes, the store writes
only what must be written, and downstream consumers see only real changes.
On its own, a worker answers that against the index on S3, and a worker is
usually a cold reader.

Measured at 100M keys in steady state (`bench/keys/results.md`, MinIO with
30 ms injected per request and 80 MB/s per connection — not real S3):

| 1K random keys | GETs | Bytes read | Wall |
|---|---|---|---|
| cold worker: lookup | 48 | 445 MB | 765 ms |
| cold worker: lookup + delta PUT | 50 + 1 PUT | 446 MB | 817 ms |
| warm worker (a disk cache holding every file, since removed): lookup | 0 | 0 | 311 ms, decoding compressed blocks |

Most of the 445 MB is the tail of every file — its filters — fetched again
on every commit to answer a question about 1,000 keys. A warm worker
removes the requests but still decodes a 64 KB block per lookup, and only
`Local` placements and workers with a volume can be warm. The engine sees
every index file come into existence: it writes compaction outputs and
commits every delta. Index files are immutable, so a copy it keeps is
never stale. The question this design must answer by measurement is how
much a warm engine beats both rows above, not only the first. It does
(§14), and workers now keep no cache of index files: the engine's is the
one.

## 2. The design in one paragraph

The worker sorts what it intends to write and sends it to the engine with
`POST …/resolve`. The engine answers from its warm cache with the exact
delta and counts, computed purely from the snapshot the attempt pinned; it
persists nothing and writes no object. The worker uploads the delta as its
own delta file and follows its store's write rules — repair, gate, fencing
— exactly as if it had computed the delta itself. If the engine cannot be
reached, declines or is too slow, the worker resolves locally: a sparse
reader for small patches, a streaming merge-join for replacements and
dense patches. There is no read planner. The same cache inlines small
pending windows into downstream specs and serves the per-key readers.

Correctness never depends on the resolver: its answer is a pure function
of the pinned snapshot and the request, the worker can compute the same
delta, and every durable decision — delta file, gate, result, journal — is
made exactly where it is made without it.

## 3. Two questions per keyed output

A delta that is empty does not mean the store has nothing to write:

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
   `added` / `removed`. This is what the resolver computes, and what
   becomes the batch's delta file.
2. **What must the store write?** Decided by the worker, by the store's
   kind (`lifecycle.md` §9.6).

**`immutable` stores** (FileStore, S3Store) write each key at its version
under a name carrying the writer's generation, `{key}/{version}.{generation}`
(`lifecycle.md` §9.8), and never overwrite. A dead attempt leaves only
unreferenced objects, so there are no unsettled intents and no repair: the
store writes the delta's upserts, and the commit records the delta. The
generation is the delta entry's **locator**; superseded objects are
collected through their predecessors' locators (§6).

**`fenced` and `overwrite` stores** keep intents and repair, in this
order — the order matters for `fenced` stores, and is today's for
`overwrite`:

```
1. acquire       fenced: the store's generation for (output, scope), before reading anything
                 (an older writer's open transaction finishes first; its later ones are refused)
2. repair read   named intents: keys dead attempts meant to change and this patch does
                 not mention, read back from the store and folded into the run;
                 an unknown-writes intent: the store's whole key map, reconciled (below)
3. resolve       the repaired run — by the engine or locally
4. upload        the delta file, create-only
5. gate          the attempt's `.writing` gate with its intents (both kinds keep it, lifecycle §9.6)
6. write         under the store's checks: fenced, every transaction checks the generation
```

Repair reads before acquisition — what `_store_outputs` does today — would
read `k=1` while an older transaction is about to commit `k=2`, and the new
commit would then clear the intents with the index still saying `k=1`.
Acquisition is a new store phase (`Store.acquire(scope)`, a no-op for
other kinds); the `generation` is the lifecycle's.

What those stores write:

| Write | Unsettled intents | Store writes |
|---|---|---|
| Patch | none | the delta's upserts and removes |
| Patch | named | the delta, plus the patch's own keys an intent touched (`own ∩ intended`), even when unchanged in the index; step 2 already folded in the intents' other keys |
| Patch | unknown writes | step 2 reads the store's whole key map and reconciles it (below); then as for named intents, with every key of the scope as `intended` |
| Replacement | none | the delta's upserts and removes; the whole scope if the delta's key list was not collected |
| Replacement | named or unknown | the whole scope, overwritten. Step 2 does not run: it would keep stray rows the replacement means to remove |
| `Sql` | — | the statement; the store reports its rows afterwards, resolved as a replacement |

**Unknown writes.** A `Sql` write's intent names no keys: the statement
can change any of them, and its key map is known only once it reports. If
its worker dies after the gate and before reporting, the next attempt
cannot read back "the intended keys":

```
committed           a=1, b absent
Sql attempt         deletes a, inserts b=1, commits its transaction, dies before reporting
next attempt        a patch of c
```

So after acquisition (step 1), a **patch** against an unknown-writes
intent reads the store's full key map for the scope — the same extraction
a `Sql` write reports with — and reconciles it with the pinned index:
every key whose store version differs from the index's, that the store
holds and the index does not, or that the index holds live and the store
does not (here `a` deleted and `b` inserted), joins the run as a repair
entry before resolution. Only then can the commit clear the intent. A
**replacement** needs no read: it overwrites the whole scope, as with any
unsettled intent.

An output is **unchanged** — no store write, no new batch, head kept —
only when the delta is empty, the output exists, and no intent is
unsettled. An empty delta with unsettled intents is still a commit: no
delta file, but the repair writes and the settlement that clears the
intents.

## 4. The resolver protocol

**Route.** One project-scoped binary route on the lifecycle channel:

```
POST /api/projects/{p}/attempts/{a}/resolve
Content-Type: application/vnd.solera.resolve; version=1
Authorization: Bearer {attempt token}
```

The token, invocation rule and retries are `lifecycle.md`'s (§4, §5.2,
§5.3); the framing, limits and validation below are this doc's, and
`lifecycle.md` §5.1 points here. One request per attempt that has something to
resolve, covering every output the worker wants resolved; outputs it
resolves itself (big writes, `Sql`, unkeyed, failure deltas, §8) are
absent.

**Framing.** `u8` protocol version · `u32` header length · JSON header ·
payloads, back to back in output order: each output's `offset` is where
the one before ended, and together they fill the body, so no byte is two
outputs'. A version the engine does not speak gets `415`, and the worker
resolves locally; a frame whose bounds do not hold gets `400`.

**Request.**

```json
{"invocation": "k3f…", "outputs": [
  {"name": "orders", "scope": "", "kind": "patch", "batch": 12, "generation": 184467,
   "base": {"prefix": "keys/orders/_/", "head_batch": 11},
   "keys": 1000, "offset": 0, "size": 41250, "digest": "9e07…"}
]}
```

- The payload is the sorted run as a `.kx` file (`key-index-format.md`):
  `(key, version, locator, deleted)`, `deleted` for removes. Every written
  entry's locator is `generation`, the attempt's (`lifecycle.md` §9.7), so
  an immutable store's name for it is known. One format, one parser, the
  CRCs included.
- `kind` is `patch` or `replace`: the same run means different deltas
  (`{a: 1}` against `{a: 1, b: 1}` is empty as a patch and deletes `b` as
  a replacement).
- `base` names the snapshot the worker resolved against: the index (its
  prefix, which survives renames) and the head's batch, as pinned in its
  spec. `batch` is the batch the delta will carry.
- `digest` is the payload's XXH3-128.

**Validation.** The engine trusts nothing in the header it can check
against the attempt's preparation, which it holds in memory for every
live attempt (and rebuilds from `.spec` on adoption):

- the invocation is the one the engine admitted (`lifecycle.md` §4: the
  `start`ed one, or after a restart the one in `.worker`); any other gets
  `409` and resolves nothing — it should not be running;
- the attempt is live and holds the scope lock for `(name, scope)`;
- `name`, `scope`, `batch`, `generation` and `base.prefix` are the ones
  prepared for that output, the run's locators are that generation, and
  `base.head_batch` is the head's batch now;
- `kind` is allowed for the output (a `replace` of an output whose write
  can only be a patch is refused);
- the payload is what the header says, from its own bytes: its digest is
  `digest`, and decoded once — footer, index and block CRCs, each block's
  entries against its index entry and the file's against its footer, keys
  strictly increasing, every length checked before it is used — it holds
  `keys` entries. The limits below apply to what it holds, decoding stops
  at them (`too_big`), and the resolve reads the decoded run, never the
  payload again.

A mismatch is a per-output decline (`stale`, `not_live`, `invalid`), never
a guess.

**Deduplication.** Concurrent and repeated requests share one computation
when their whole semantic input matches:

```
(attempt, invocation, name, scope, kind, batch, generation, base.prefix, base.head_batch, digest)
```

The answer is a pure function of that tuple and the snapshot content,
and the engine writes nothing, so a retry after a dropped connection or an
engine restart gets the same answer or a decline. There is no request to
freeze, no response to recover and no engine-written file to clean up.

**Response.** Same framing: a header, then each resolved output's delta.

```json
{"outputs": [
  {"name": "orders", "result": "delta", "added": 212, "removed": 3, "entries": 640,
   "file": {"size": 26810, "digest": "51aa…"}, "offset": 0},
  {"name": "events", "result": "empty"},
  {"name": "sessions", "result": "declined", "reason": "cold"}
]}
```

- `delta`: the payload is the delta as a complete `.kx` file, ready to
  upload under the worker's own name `{batch:012d}-{attempt}`. Its entries
  are `(key, version, locator, deleted)`, and every changed or deleted key
  that had a live entry carries that entry's **predecessor** `(version,
  locator)` — always, since the engine reads full entries; for an
  immutable output, the names to collect once the delta commits (§6). The worker
  validates it — footer, index and block CRCs, filter CRC, its digest —
  and decodes it once for its store selection (§3); it uploads the bytes
  unchanged and builds `DeltaFiles` with the header's `added` and
  `removed`, `exact: true`, and the file's `digest` (§5, candidates). A
  response is capped at `resolve_max_bytes` (16 MB), so it is one file,
  below the 64 MB split. The `.kx` format version travels in the file's
  footer as today; the worker refuses a version it cannot read and
  resolves locally.
- `empty`: the write changes nothing in the index. No file;
  `DeltaFiles([], 0, 0, exact=True)`. §3 decides whether that means
  "unchanged".
- `declined`: the worker resolves this output locally (§6), at once.
  Reasons: `cold` (the snapshot is not warm), `busy` (queue full),
  `too_big` (over the key, byte or replacement limits), `stale` (the head
  moved), `not_live`, `invalid` (see validation), `corrupt` (a local copy
  and its S3 object both failed validation).
- Overload of the whole engine is `503` with no body; transport errors and
  timeouts count as declines.

**Exact counts.** The engine resolves against full entries, never through
filters, so `added` and `removed` are exact and an engine-resolved commit
never increments `inexact`.

**Which snapshot.** While an attempt is live it holds its scope lock: no
other commit can change that index, and compaction and recounts change its
files and count fields but not its content. The engine therefore resolves
against the file set it currently holds for that scope, once validation
passed, and pins that file set before any asynchronous work (§5); the
content is the pinned snapshot's even if compaction swapped files since
prepare.

**Mixed attempts.** Each output is independent: one may be resolved by
the engine, one declined and resolved by a streaming merge-join, one
written by `Sql`, and the attempt commits them together. The worker uses
exactly one answer per output, chosen before it uploads that output's
delta.

**Waiting.** The worker waits at most `min(resolve_timeout, remaining
deadline)` — `resolve_timeout` is 5 s — and then resolves locally. A late
response is dropped unread; the engine wrote nothing.

**Limits.** Absolute caps, with starting values; the measured thresholds
are §6's:

| Limit | Start | Applies to |
|---|---|---|
| `resolve_max_keys` | 100K run entries | patches |
| `resolve_max_bytes` | 16 MB | request and response bodies |
| `resolve_max_decoded` | 64 MB | a run decoded: decompressed blocks, and its keys and versions |
| `resolve_max_entries` | 2M physical entries in the snapshot | replacements: the engine merges the run with every entry |
| `resolve_queue_bytes` | 64 MB of queued payloads | admission; beyond it, `busy` at once |
| `resolve_concurrency` | 2 | resolves in flight, on their own threads, apart from compaction's |

The worker sends a patch only under `resolve_max_keys` and a replacement
only when the pinned count plus its run is under `resolve_max_entries`;
the engine checks again.

**Other resolver targets.**

- **Source commits** (`commit_source`) run in the engine: they call the
  same warm resolver in-process, with the same local fallback, and keep
  today's optimistic head check before recording.
- **Sensor ticks** (`lifecycle.md` §11) post a small key map in this
  framing to their tick route, and the engine resolves it in-process as it
  does for source commits; a map over `sensor_map_max` is resolved on the
  sensor host, which commits a reference to its delta file. Neither uses
  this route or an attempt's validation.
- **Failure indexes** are not resolve targets: failure deltas are resolved
  by the worker, with exact lookups of prior records
  (`per-key-processing.md` §9).

**Failure cases.**

| Case | Outcome |
|---|---|
| Engine unreachable, restarting, or slow | Local resolve after the timeout; same delta, possibly an inexact count (§6) |
| Worker dies after the response, before the gate | Nothing to clean on the engine; the uploaded delta is the worker's and is discarded at attempt end as today |
| Worker dies after the gate | Unsettled intents and repair (overwrite, fenced); nothing for immutable stores |
| Attempt canceled while a resolve runs | By the cancel record (`lifecycle.md` §2.2): a draining attempt still resolves; once the record is `forced`, or the attempt ended, a resolve checks that when it starts and between chunks, and stops, releasing its pins |
| Compaction commits during a resolve | The resolve keeps the file set it pinned; garbage collection waits for the pin |
| A second invocation of the attempt | `409`: the engine admits one invocation (lifecycle §4) |
| Local copy corrupt | Dropped, refetched from S3 and rebuilt; the request is declined `cold` meanwhile |

## 5. The engine cache

One cache serves every reader on the engine: resolves, inlined windows,
compaction (which reads what it just wrote), recounts, and the per-key
readers (§8). It runs on maintenance threads, never on the engine's event
loop.

**Unit: an immutable file.** Entries are keyed by object path; a path is
never reused, so an entry is never stale, only evicted. An index is
**warm** when every file of its snapshot is present.

**Identity of a file.** `FileInfo` gains `digest`: the XXH3-128 of the
file's bytes, computed by whoever writes it (the worker for deltas, the
engine for compaction outputs) and recorded with the file in the commit.
The cache checks a fill against it, and candidates (below) are matched by
it.

**Local form.** On disk, each file becomes `{name}.kxl` (byte layout in
`native/src/local.rs`):

```
header     magic "KXL1" · format version · header length · source size · source digest · source path
           · blocks · entries
directory  per block: first key, last key, local offset, entries length, restart count, entries,
           CRC-32 of the block's entries and restart table
           CRC-32 of header and directory together
blocks     per block: entries as in a `.kx` block, uncompressed (key, version, locator, deleted,
           predecessor if any), then its restart table (offsets of full keys, every 16 entries)
```

A lookup binary-searches the directory (held in RAM), then the block's
restart points, then scans at most 16 entries — where a cold or warm
worker decodes a compressed 64 KB block (~0.45 ms). The S3 format does not
change.

**Integrity.** No byte of the local form is used unverified:

- **The directory** — boundaries, offsets, lengths, counts and the source
  identity — is covered by its own CRC-32, checked whenever the file is
  opened: at fill, and on every reopen after an engine restart. A
  corrupted first key would otherwise steer a lookup to a valid block of
  the wrong range, and a live key would read as absent.
- **A block** — its entries and restart table — is checked each time it
  is read from disk (~10 µs for 128 KB). A lookup also checks the key
  against the block's first and last key from the directory.
- **Atomic publication**: a local file is written under a temporary name
  and renamed, so a crash leaves no half-written file under a real name;
  temporary files are deleted when the cache opens.

A failure drops the local file, refetches the source, validates it against
its own CRCs and its `digest`, and rebuilds; if the source fails too, the
read fails with `corrupt`.

**Budgets, separate:**

- `cache_disk` (default 16 GB): local files, candidates, temporary files
  and reservations. A 100M-key index in steady state holds ~125M physical
  entries across its levels — not 100M — at ~40 B decompressed: ~5 GB
  (estimated from the steady-state level sizes).
- memory: directories of every local file (~0.01 B per entry: ~1.3 MB
  at 100M) and the summaries of §7 (256 MB). Blocks are read through the
  OS page cache — no block LRU of our own; §9 measures both warmths.
  Filters are not cached: a warm reader never needs them.

**Pins.** A reader pins the file set it reads; eviction skips pinned
files. A reader that fetches from S3 is also a reader pin in the garbage
order of `object-store-state.md` (pins and deletions by event position),
so compaction cannot delete a file under a fill.

**Reservations.** Every operation that adds bytes reserves them first,
against `cache_disk`:

| Operation | Reserves | Released |
|---|---|---|
| fill of a file | its local size: what it built to before, else estimated from its compressed data | when installed, or when the fill fails or is canceled (its temporary file deleted) |
| compaction | its outputs' estimated size, while its inputs stay pinned by readers | when the outputs are installed and the inputs unpinned and evicted |
| candidate | its delta's decompressed size (below) | when installed, dropped or evicted |

A reservation that cannot be met evicts first (below). If eviction cannot
make room, the operation runs without the cache: a fill is not started; a
compaction installs nothing, and its index is **demoted** — marked cold,
its files evictable like an inactive index's — so its writers decline
`cold` until it is admitted again.

**Fills, deduplicated.** One fill per path at a time; a second reader
awaits the first. A fill streams the file in 8 MB segments, as the
streaming merge-join does, and builds the local form as it goes. A resolve
never waits on a fill: if its snapshot is not warm, it declines `cold` and
queues fills for the missing files, if the index is admitted. The worker's
local resolve and the engine's fill then run side by side once, and the
next commit is warm.

**Write-through.** The engine does not read back what it produced or saw:

- compaction outputs are installed from the bytes the engine has in hand;
- a resolve keeps the delta it returned as a **candidate**, keyed by
  `(path the worker will use, size, digest)`: present on disk, invisible as
  index state. The worker uploads exactly those bytes and its result's
  `DeltaFiles` carries the same `digest`, so when the attempt commits the
  candidate becomes that file's local form without a GET. Candidates of
  attempts that end without committing are dropped;
- deltas the engine did not produce (a local resolve, a delta built
  elsewhere) are fetched once at commit, on a maintenance thread — they
  are small.

Candidates have their own sub-budget (`cache_candidates`, 1 GB of
`cache_disk`): many attempts can finish resolving and then spend minutes
writing their stores. Over budget, the oldest candidate is evicted; an
evicted candidate costs one GET at commit, nothing else.

**Admission and eviction.** An LRU over files thrashes on the simplest bad
case:

```
budget 6 GB; index A 5 GB, index B 5 GB; commits alternate A, B, A, B…
LRU: every commit evicts the other index and refills its own — 5 GB of GETs per commit
```

So the cache admits by index and evicts by file, with hysteresis:

- An index is **admitted** when its snapshot, plus a reservation for one
  compaction's overlap (its largest level's size), fits beside the indexes
  **active** within `cache_window` (15 minutes) — those that served a
  reader in that window. Otherwise it stays cold: its writers resolve
  locally, without thrash.
- Eviction order: candidates past their budget, then files of inactive or
  demoted indexes (least recently used), then superseded files (inputs of
  a finished compaction, unpinned). Never pinned files, never files of an
  active index.
- An admitted index whose new snapshot no longer fits after a compaction
  or growth is demoted, as above, and re-admitted under the same rule.
  Admission counts a file at the size its local form built to once one
  was built, so a file shown not to fit is not fetched again until the
  budget, the other indexes' use or the snapshot changes.
- In the example, A is admitted and B declined; B's writers pay the cold
  path until A has been idle for the window. Two indexes that both fit are
  both warm.

Block-granular eviction — keeping only the touched part of a big index —
helps only clustered workloads and is deferred until a benchmark shows
one; so are priorities between indexes.

## 6. The cold path, without the planner

When the engine cannot answer, the worker resolves alone. Two readers,
chosen by one rule each; `_plan_reads`, `_read_options`, `Cost`, the
estimate and the CPU-rate options are deleted, and no other cost model
replaces them.

**Sparse reader**, for small patches — today's filtered path minus the
planner:

1. read every level small enough (≤ 32 MB) and every level-0 file whole,
   all at once, and resolve newest first;
2. for the larger levels, read the tails of the files whose key range
   covers a written key, all at once;
3. classify each key with the filters: absent (no key filter matched),
   changed (no pair or tombstone filter matched: live at another version),
   or maybe;
4. read the blocks of the maybe keys, only in files whose key filter
   matched, all levels at once; each key takes its newest entry.

**Its contract:** the delta is exact; the count is exact unless a pair
filter cleared a key behind a key-filter false positive (0.35% per check),
which increments `inexact` as today. Nothing that decides correctness —
scheduling, skipping, "unchanged" — reads the count. Dropping the
pair-filter shortcut would make the count exact here too, at ~1.25 block
reads per changed key instead of ~0.01: ~1,250 more GETs for 1K keys at
100M, ~$0.0005 per commit. Indexes over the cache budget take this path on
every commit, so the design keeps filters and recounts (§12).

**Predecessors of immutable outputs.** An immutable store collects a
superseded object by name, `{key}/{version}.{locator}`, so someone must
learn each changed or deleted key's previous `(version, locator)`. The
filter shortcut cannot: for `k → v2` it knows that `k` holds some other
version, not whether that was `k/v1.17` or `k/v1.93`. The design uses two
triggers, each with one rule:

- **At resolution, whenever the old entry was read.** The delta names
  the predecessor of every changed or deleted key whose resolution read
  its old entry: always on the engine, always in the streaming merge-join,
  and for the sparse reader's maybe keys. The commit's data-garbage entry
  collects those names once no reader pins them (`lifecycle.md` §9.8).
- **At compaction, for the rest.** A key the pair filter cleared has no
  named predecessor, but its old entry is still in the index, shadowed.
  Every merge that drops a shadowed entry — or the entry under a
  tombstone at the bottom level — emits that entry's `(key, version,
  locator)` as data garbage at the compaction's event position, under the
  same reader-pin rule. Compaction emits every entry it drops, named
  before or not: names are never reused, so discarding a name twice is a
  no-op (`discard` ignores missing names), and no "already collected" bit
  has to survive compaction.

The alternatives, priced at 100M keys, 1K random changes per commit:

| Choice | Cold sparse path | Collection |
|---|---|---|
| exact predecessors only, at resolution | the pair filter no longer decides: ~1.25 block reads per changed key, ~1,300 GETs instead of ~50, ~$0.0005 per commit | prompt |
| deferred only, at compaction | ~50 GETs | every superseded object waits until its new entry merges over the old one — for random keys mostly at the bottom level, so ~20% of keys (the upper levels' share in steady state) keep a second object, hours to days |
| **both (chosen)** | ~50 GETs | prompt wherever the old entry was read (warm and streaming: every key); deferred only for keys the cold path's filters cleared |

An index over the cache budget takes the cold path on every commit, so
the first row would cost scenario E ~$130 a month; the second gives up
prompt collection that the warm path gets for free. Outputs on `fenced`
and `overwrite` stores carry locators too but collect nothing by them.

**Streaming merge-join**, for replacements and dense patches: the native
job that full replacement, compaction and recount already use, extended to
patches — it streams every level in 8 MB segments, merges them with the
sorted run, and emits the delta as it goes. Measured for a recount of a
fresh 100M index (one level): 2.4 GB in 371 GETs, 14.1 s. A steady-state
snapshot (~3.4 GB over three levels) and a patch merge are not measured
yet; the patch merge adds a lookup per run key to the recount's work.

The sparse reader, this join and the engine's cache (§5) decide each key
by one rule (`native/src/delta.rs`): what the index holds — absent, live
at a known version and locator, or live at another version as the filters
said — against an upsert or a remove.

**The crossover.** A patch goes to the sparse reader, and switches at
most once, by two thresholds expressed in the quantities that decide it —
physical snapshot size and distinct block reads:

- **Before reading:** a patch whose run is more than `stream_density` of
  the snapshot's physical entries streams directly: nearly every block
  will be decoded anyway, so the tails and filter checks are wasted.
- **After the filters:** if the maybe keys need more distinct block reads
  than `stream_reads` × the streaming read count (snapshot bytes / 8 MB),
  stream instead. The tails already read are not repeated: streaming reads
  index parts and data segments.

Both constants come from one grid (`bench/keys/bench.py --suites
crossover,steady`, `bench/keys/results.md`): indexes of 1M, 10M and 100M
keys, fresh and in steady state; patches of 1K–1M keys with 0%, 50% and
100% rewritten unchanged; each route forced. The objective is wall time,
with requests tipping close calls toward streaming, which issues an order
of magnitude fewer:

- **`stream_density` = 2%.** Under it the sparse reader wins: 1M keys
  into 100M (1%) take 10 s against 14 s streamed. Over it streaming does:
  1M keys into 10M (10%) take 2.8 s streamed, 7.7 s sparse — the sparse
  reader's cost per written key is mostly CPU (~9 µs in Python), the
  stream's per index entry.
- **`stream_reads` = 16.** The two routes take the same time where the
  exact reads number about 24 per streamed segment (100M steady, 100K
  keys half unchanged: 11,878 reads in 13.8 s, against 436 segments in
  22.6 s; 10M fresh, 10K: 831 reads in 2.0 s, 34 segments in 1.4 s);
  16 gives streaming the close calls, saving the requests.

## 7. Inlined downstream changes

When the engine prepares an attempt whose `Incremental` edge reads a keyed
upstream, it can put the first page of the pending window in the spec
instead of making the worker read deltas:

```json
"changes": {"from": 56, "to": 57, "after": null, "limit": 10000, "full": false,
            "inline": {"upserted": {"alpha-file-2": ["3f9c…", 184467]}, "deleted": ["alpha-file-7"],
                       "next": null}}
```

An upserted key carries its `(version, locator)`, as a paged window's
does: the worker hands them to the load as `Keys({key: (revision,
locator)})` (`lifecycle.md` §9.8), and an immutable store computes every
name without a LIST.

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
  page that reaches it ends early with `next` set. If the first entry
  alone exceeds the cap, the window is not inlined at all — an empty page
  with a cursor would not advance.
- **Bounded work in prepare.** Prepare reads only RAM. At commit, on a
  maintenance thread, the engine keeps for each committed delta of at most
  `inline_max` entries (10K) its sorted entries as a **summary** — native
  sorted runs, accounted at the bytes they hold — within `cache_ram`,
  preferably until the consumers' watermarks pass the batch; the budget
  evicts the oldest first. A window is inlined only when every batch in it
  has a summary and it spans at most `inline_max_batches` (64) summaries:
  then prepare merges them from the cursor, newest batch winning, and
  stops at the page (a binary search per summary, then a k-way merge of
  `limit` entries), so its work is the page's, not the window's. Anything
  else — after a restart, a big batch, a lagging consumer with thousands
  of batches — goes out as today, and the worker pages the window.

## 8. Readers for per-key processing

`per-key-processing.md` is authoritative for everything about failures:
the entry format, the **transition table** (interrupted keys follow the
cancel record's `reason`, `lifecycle.md` §2.2), the **one
eligibility predicate** `eligible(entry, now, epoch, forced)`, the
outcome counts moved by transitions, the conservative minima maintained
from each commit and made exact by completed retry passes, and retry-pass
identity. This doc implements none of that differently; it calls the same
SDK predicate.

In v1 the cache has **one** per-key reader: the **inlined retry page**.
When a scope has retries and its failure index is warm, a maintenance
thread selects the next page of entries after the pass position that
satisfy `eligible`, at most `inline_max`, with their prior records, and
holds it for prepare, under §7's caps:

- it carries its **input identity** — (failure index prefix, its head
  batch, the pass identity of the per-key doc, the pass position
  `retry.after`) — and is discarded rather than published if any of them
  moved while it was computed, and ignored by prepare unless they all
  match the scope's current ones;
- prepare never waits for it: without a matching page, `.spec` pins the
  failure index and the worker pages it with the same predicate.

Failure deltas are not resolved here: the worker resolves them locally,
with exact lookups of prior records (per-key doc §9), and an inlined page
already carries the prior records of its keys.

Not in v1, and not to be built from this doc: engine-side pattern skip
hints (pattern match counts per committed delta), and coalesced
recomputation of failure minima from the cache. The per-key doc records
both as later optimizations; if they come, they follow the same identity
rule.

## 9. Costs

**Requests.** GET $0.40 and PUT $5.00 per million, so a PUT is 12.5 GETs.
Scenario E: 1K random changes every 10 s into 100M keys, 259,200 commits a
month, steady state, per commit:

| | Today (measured) | Resolver, warm (projected) |
|---|---|---|
| Worker index GETs | 50 | 0 |
| Worker index PUTs | 1 (delta) | 1 (delta) |
| Engine index GETs | 0 | 0 (write-through; an evicted candidate costs 1) |
| Compaction (simulated, `amplification.py`) | 1.18 GET + 0.144 PUT | same |
| GET-equivalents | **65.5** | **15.5** |
| Key index requests / month | $6.79 | **$1.60** |

That is $5.18 a month saved on scenario E's requests, before the engine's
added disk, CPU and network. Earlier drafts added today's $6.69 of attempt
and journal overhead to both columns; that figure is the lifecycle doc's
to re-estimate, not this one's.

**Elsewhere:**

- **Cold fills.** One streaming read of a snapshot per admission: ~430
  GETs and ~3.4 GB at 100M in steady state (projected from the snapshot
  size; the measured streaming read is the fresh index's 371 GETs and
  2.4 GB).
- **The cold path when declined.** Today's figures: ~50 GETs per 1K-key
  commit at 100M, immutable outputs included, since predecessors the
  filters skip are collected at compaction (§6); a streaming patch reads
  the snapshot.
- **Locators and predecessors.** A varint generation per entry, and a
  predecessor `(version, locator)` per changed key in deltas until
  compaction drops it; the bytes per entry are the format work's to
  measure (`lifecycle.md` §15). Discards are DELETEs, free on S3, batched
  by 1,000; keys collected at commit are discarded a second time, as a
  no-op, when compaction drops their old entry.
- **Network.** Requests and responses are ~40 KB and ~27 KB per 1K-key
  commit. Within one availability zone that is free; across zones EC2
  charges per GB in each direction (about $0.01/GB each way at today's
  prices): ~17 GB a month for scenario E, ~$0.35. Placing workers in the
  engine's zone avoids it.

**Latency and CPU, kept apart.** For a 1K-key lookup at 100M — every
figure in the last column is a projection to measure:

| | Cold worker (measured) | Warm worker (measured) | Resolver (projected) |
|---|---|---|---|
| Network | 2 rounds of 30 ms, ~445 MB at 80 MB/s per request | none | 1 HTTPS round trip in region (~1–5 ms), ~67 KB |
| CPU | filter checks and block decoding, inside 765 ms | decoding a compressed block per lookup, inside 311 ms | ~2.8 lookups per key × a few µs ≈ 10–20 ms, if the touched blocks are in the page cache |
| Time to delta | 765 ms | 311 ms | ~20–30 ms + the round trip |
| then | 1 PUT, ~30 ms | same | same |

"Warm" has two levels on the engine, measured separately: blocks in the OS
page cache (the projection above), and blocks only on local SSD, where
~2,800 random reads of ~128 KB each cost what the device's random-read
rate allows — likely tens of milliseconds on NVMe, more on network disks.
A 512 MB RAM budget does not make a 5 GB index resident; the benchmark
reports both.

## 10. Tests and benchmarks

- **Equivalence.** Engine-resolved and locally resolved deltas decode to
  the same entries (compressed bytes may differ); engine counts equal an
  oracle's exact counts; the cold path's counts satisfy its contract
  (`inexact` set exactly when a pair filter decided, and `added − removed`
  off by at most the keys it decided). Over random patches, removes,
  replacements, repairs and compactions between prepare and resolve.
- **Repair.** The `a=1 → a=2 → a=1` sequence rewrites the store with an
  empty delta; a replacement with unsettled intents rewrites the scope;
  "unchanged" only without intents; for a fenced store, an older writer's
  pending `k=2` lands before the repair read; immutable stores skip repair.
- **Protocol.** Requests differing only in `kind` are not deduplicated;
  identical ones are; a header naming another scope, batch or prefix is
  declined; a second invocation gets `409`; each decline reason falls back
  locally; a timeout falls back and the late response is dropped; an
  unknown protocol version gets `415`; mixed attempts commit together.
- **Cache.** The alternating-indexes case stays warm for one and cold for
  the other without refills; a corrupted block and a corrupted directory
  (a shifted first key) are both detected and refetched, including on
  reopen after a restart; a corrupted source fails `corrupt`; a candidate
  is installed without a GET at commit, and an evicted one costs one GET;
  fills are deduplicated; failed and canceled fills release their
  reservations; a compaction that cannot reserve demotes its index;
  pinned files survive eviction and garbage collection.
- **Locators.** Resolver deltas, cold deltas, inlined pages and `Keys`
  carry `(version, locator)`; every superseded object of an immutable
  output is discarded — by its commit when the old entry was read, by the
  compaction that drops it otherwise, including keys the pair filter
  cleared and keys deleted at the bottom level — and none while a reader
  pins it; a second discard is a no-op.
- **Unknown writes.** A dead `Sql` writer that deleted `a` and inserted
  `b`: the next patch acquires, reads the store's key map, reconciles both
  keys and only then clears the intent; a replacement overwrites instead.
- **Retry pages.** An inlined retry page computed against an older failure
  head or pass position is discarded; prepare without a matching page
  pins the failure index instead of waiting; canceled keys are never
  selected by themselves (the per-key predicate).
- **Inlining.** Inlined pages equal `pending` pages for the same pinned
  window, including a later batch that must not leak in, tombstones, the
  byte cap, an oversized first entry (no inline), the summary cap, and
  the continuation cursor.
- **Benchmarks.** The crossover grid of §6; warm resolves at 1K/10K/100K
  keys into 1M/10M/100M against the cold-worker and warm-worker baselines
  — time to delta end to end, engine CPU, RSS, cache disk, with the page
  cache dropped and not; scenario E end to end, counting every request.

## 11. Not in this design

- **The canonical row digest** of the earlier proposal is independent of
  resolution and belongs in its own spec next to its code
  (`row-digest.md`), with the group production `per-key-processing.md`
  defines. The first review's requirements stand for it: a versioned byte
  grammar with framing for rows and structs, nanosecond timestamps, dates
  distinct from timestamps, decimal normalization, missing versus null
  fields, no automatic pickle fallback, and golden vectors across Python,
  Arrow and SQL.
- **An object-store channel** for resolves. Workers can always reach the
  engine over HTTPS; a worker that cannot simply resolves locally.
- **Sensor publication, rescoping, cancellation and retry pacing** —
  `per-key-processing.md` and `lifecycle.md`.
- **Later, not v1:** pattern skip hints and coalesced recomputation of
  failure minima (§8).

## 12. Where this departs from the reviews

The follow-up review agrees with all three.

- **No frozen request per attempt.** The resolver persists nothing and
  decides nothing, so a retried request needs no stored answer: it is
  recomputed, or deduplicated while in flight.
- **Resolve against the current file set, not the pinned manifest.**
  Under the scope lock they have the same content, and the current files
  are the warm ones; validation and pinning before any asynchronous work
  make a moved head a `stale` decline rather than a wrong answer.
- **Filters and recounts stay.** The sparse reader's pair-filter shortcut
  keeps a declined or over-budget index at ~50 GETs per small commit
  instead of ~1,300, and approximate counts never decide correctness.

## 13. Open questions

1. **Thresholds.** `stream_density` and `stream_reads` are measured (§6);
   `resolve_max_keys`, `resolve_max_entries` and `resolve_timeout` still
   come from the warm grid; the absolute caps stay regardless.
2. **Engine capacity.** No rate threshold is credible before the local
   form is measured. Resolves, retry pages and compaction have separate
   threads; if offloading becomes necessary, the cache and all its readers
   move together into `engine_executor` (`object-store-state.md` §6),
   rather than a second cache-owning service.
3. **Page cache vs SSD.** Whether a 100M-key index needs its hot blocks in
   RAM (a bigger `cache_ram`) to stay well ahead of a cold worker, or SSD
   reads suffice.
4. **With `lifecycle.md`:** settled there — the route (§5.1),
   `Store.acquire` (§9.7), and failure deltas staying out of the gate's
   intents (§9.6); sensors (§11 there) need no attempt validation. Its
   §9.8 collection gains the compaction trigger of §6 here.

## 14. As built

Milestones 3 and 4. Where the code differs from the text above, it says so
here.

| Piece | Where |
|---|---|
| Sparse reader, streaming patch, the two switches, `exact`, `get` | `KeyIndex.resolve` / `changes` / `lookup` (`solera/keys/index.py`); `Job.patch` (`native/src/jobs.rs`) |
| Compaction garbage | `KeyIndex.compact(garbage=True)`; `.kg` files (`key-index-format.md` § Garbage files) |
| Repair by store kind, unknown `Sql` writes | `_store_outputs` and `_reconcile` (`solera_worker/worker.py`); acquisition is the lifecycle's |
| Local form, lookups and merges over it | `native/src/local.rs`: `build_local`, `LocalFile`, `Snapshot` |
| The cache | `EngineCache` (`solera/keys/cache.py`) |
| Framing, validation, deduplication, declines | `Resolver`, `request`, `answers` (`solera/keys/resolver.py`) |
| The route | `POST /api/projects/{p}/attempts/{a}/resolve` (`api.py`), `Engine.attempt_resolve` (`attempts.py`), the channels' `resolve` |
| The thread, write-through, summaries, inline pages, source commits | `KeyService` (`solera_server/keyservice.py`); `Engine._cache_commit`, `_resolve_source`, `_incremental_plan` |

Differences:

- **Checksums are CRC-32**, as in `.kx` files, not CRC32C.
- **No block LRU of our own**: blocks are read through the OS page cache,
  and a resolve keeps, per run, the block it read last: its keys come
  sorted, so it never needs an earlier one.
- **The engine reads a patch by point lookups** while they number under 32
  per block of the snapshot (a lookup reads one block, ~10 µs from the page
  cache; a merge decodes every entry, ~0.4 ms per block), else by a merge.
- **Where the cache lives**: `Engine(resolve_cache=...)`: by default beside
  `file://` state (`{its directory}/.key-cache/{its name}`) or in a
  temporary directory for remote state, with a 16 GB disk budget; `None` or
  `False` turns the resolver off (every request is answered `503`; workers
  resolve themselves). A directory that cannot be made turns it off too,
  with a warning. It is the only cache of index files: workers read the
  store.
- **Admission happens on the first decline**: a resolve against a snapshot
  not warm declines `cold` and queues a fill of the index, which admits it.
  An empty index is warm: the first write of an output is answered.
- **After a restart** the cache keeps the local files whose directory checks
  out, their indexes inactive until a reader asks; temporary files and
  anything that fails its check are deleted.
- **Local blocks are 8 KiB**, re-blocked from the source's 64 KiB: a lookup
  reads one, and at 100M keys the read, not the CPU, is most of a warm
  resolve (`bench/keys/results.md`, "The engine's warm resolver").

Measured against §9's projections (results.md), every path to the delta
uploaded: a 1K-key write takes 53–57 ms through the engine at 1M–10M keys
(16–20 ms of it resolving), about the projection plus the upload; at 100M,
113 ms with the local files in the page cache and 451 ms from the disk —
then what a worker's disk cache gave (438 ms), with no GETs (open question
3: the hot blocks need to stay in RAM). A write the engine does not answer
pays a cold worker's resolve (results.md, "Without a worker cache"). The fill of a 100M-key steady snapshot reads
221 GETs and 3.5 GB, in 18.2 s with at most two whole files in memory at
once; its local files take 3.4 GB, not ~5 GB. HTTP adds under a
millisecond.
