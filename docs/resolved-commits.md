# Engine-resolved commits

Status: **built** (decision D4); §14 lists where the code departs from the
text. How a keyed write learns what it changed (`object-store-state.md` §6,
"Compute a delta"): the engine answers from a warm cache of the key index,
and the worker resolves locally when it cannot.

It depends on two other designs, and says where:

- `lifecycle.md` — the worker → engine HTTPS channel (§5), the control
  file whose `owned` admits one worker (§2.4, §4), and the store kinds `immutable`
  and `fenced` (§9.6, `stores.md`), which decide the repair rules of §3.
- `key-index-design.md` and `key-index-format.md` — the stamped-layer
  index the delta is resolved against, and the delta's bytes.

## 1. Why

A keyed write must know, for each key it writes, whether the key is new,
changed or unchanged: the index records only real changes, the store writes
only what must be written, and downstream consumers see only real changes.
On its own, a worker answers that against the index on S3, and a worker is
usually a cold reader.

Measured at 100M keys in steady state on the leveled index that came before
layers (`bench/keys/results.md`, MinIO with 30 ms injected per request and
80 MB/s per connection — not real S3); the stamped-layer index's cold
lookups are in `key-index-design.md` § Measured:

| 1K random keys | GETs | Bytes read | Wall |
|---|---|---|---|
| cold worker: lookup | 48 | 445 MB | 765 ms |
| cold worker: lookup + delta PUT | 50 + 1 PUT | 446 MB | 817 ms |
| warm worker (a disk cache holding every file, since removed): lookup | 0 | 0 | 311 ms, decoding compressed blocks |

Most of the 445 MB is the tail of every file — its filters — fetched again
on every commit to answer a question about 1,000 keys. A warm worker
removes the requests but still decodes a 64 KB block per lookup, and only
`Local` placements and workers with a volume can be warm. The engine sees
every index file come into existence: it writes merge outputs and
commits every delta. Index files are immutable, so a copy it keeps is
never stale, and the engine's cache of them (§5) answers a warm resolve
with local reads. Workers keep no cache of index files: the engine's is
the one.

## 2. The design in one paragraph

The worker sorts what it intends to write and sends it to the engine with
`POST …/resolve`. The engine answers from its warm cache with the exact
delta and counts, computed purely from the snapshot the attempt pinned; it
persists nothing and writes no object. The worker uploads the delta as its
own and follows its store's write rules — repair, gate, fencing — exactly
as if it had computed the delta itself. If the engine cannot be reached,
declines or is too slow, the worker resolves locally: sparsely for small
patches, by a streamed join for replacements and large patches. A worker's
input batches come in its spec, planned by the engine (`observed-set.md`):
the only index reads left to a worker are this resolve and a whole load's
pages of an immutable store's keys.

Correctness never depends on the resolver: its answer is a pure function
of the pinned snapshot and the request, the worker can compute the same
delta, and every durable decision — delta files, gate, result, journal —
is made exactly where it is made without it.

## 3. Two questions per keyed output

A delta that is empty does not mean the store has nothing to write:

```
committed           a=1
attempt 1           writes a=2 to the store, dies before committing    → repair intent {a}
attempt 2 returns   a=1
delta vs the index  empty: the index still says a=1
the store           still holds a=2 — it must be overwritten
```

So the worker asks two separate questions, and the resolver answers only
the first:

1. **What changes in the index?** The delta: entries whose version differs
   from the pinned snapshot, deletions of live keys the write removes, and
   `added` / `removed`. This is what the resolver computes, and what
   becomes the commit's delta.
2. **What must the store write?** Decided by the worker, by the store's
   kind (`lifecycle.md` §9.6).

**`immutable` stores** (FileStore, S3Store) write each key under a name
carrying the writer's generation, `{key}/{generation}` (`lifecycle.md`
§9.8), and never overwrite. A dead attempt leaves only unreferenced
objects, so there are no repair intents and no repair: the store writes
the delta's upserts, and the commit records the delta. The generation is
the commit's own (`versions.md`); superseded objects are collected through
the replaced generations their deltas name, by the partition's cleanup
cursor (`lifecycle.md` §9.8).

**`fenced` stores** keep intents and repair, in this order:

```
1. acquire       fenced: the store's generation for (output, partition), before reading anything
                 (an older writer's open transaction finishes first; its later ones are refused)
2. repair read   named intents: keys dead attempts meant to change and this patch does
                 not mention, read back from the store and folded into the run;
                 an unknown-writes intent: the store's whole key map, reconciled (below)
3. resolve       the repaired run — by the engine or locally
4. upload        the delta's files, create-only
5. gate          the control file swapped to `writing`, with its intents (lifecycle §2.4, §9.6)
6. write         under the store's checks: fenced, every transaction checks the generation
```

Repair reads before acquisition — what `_store_outputs` did before — would
read `k=1` while an older transaction is about to commit `k=2`, and the new
commit would then clear the intents with the index still saying `k=1`.
Acquisition is a new store phase (`Store.acquire(context)`, a no-op for
other kinds); the `generation` is the lifecycle's.

What those stores write:

| Write | Repair intents | Store writes |
|---|---|---|
| Patch | none | the delta's upserts and removes |
| Patch | named | the delta, plus the patch's own keys an intent touched (`own ∩ intended`), even when unchanged in the index; step 2 already folded in the intents' other keys |
| Patch | unknown writes | step 2 reads the store's whole key map and reconciles it (below); then as for named intents, with every key of the partition as `intended` |
| Replacement | none | the delta's upserts and removes; the whole partition if the delta's key list was not collected |
| Replacement | named or unknown | the whole partition, overwritten. Step 2 does not run: it would keep stray rows the replacement means to remove |
| Opaque (`Opaque`: Postgres's `Sql`) | — | read by the store itself, a query materialized; the store reports its keys afterwards, resolved as a replacement |

**Unknown writes.** An opaque write's intent names no keys: a query
can change any of them, and its key map is known only once it reports. If
its worker dies after the gate and before reporting, the next attempt
cannot read back "the intended keys":

```
committed           a=1, b absent
opaque attempt      deletes a, inserts b=1, commits its transaction, dies before reporting
next attempt        a patch of c
```

So after acquisition (step 1), a **patch** against an unknown-writes
intent reads the store's full key map for the partition — the same extraction
an opaque write reports with — and reconciles it with the pinned index:
every key whose store version differs from the index's, that the store
holds and the index does not, or that the index holds live and the store
does not (here `a` deleted and `b` inserted), joins the run as a repair
entry before resolution. Only then can the commit clear the intent. A
**replacement** needs no read: it overwrites the whole partition, as with any
repair intent.

An output is **unchanged** — no store write, no new commit, head kept —
only when the delta is empty, the output exists, and no intent is
owing a repair. An empty delta with repair intents is still a commit: no
delta file, but the repair writes and the settlement that clears the
intents.

## 4. The resolver protocol

**Route.** One project-scoped binary route on the lifecycle channel:

```
POST /api/projects/{p}/attempts/{a}/resolve
Content-Type: application/vnd.solera.resolve; version=2
Authorization: Bearer {attempt token}
```

The token, worker rule and retries are `lifecycle.md`'s (§4, §5.2,
§5.3); the framing, limits and validation below are this doc's, and
`lifecycle.md` §5.1 points here. One request per attempt that has something to
resolve, covering every output the worker wants resolved; outputs it
resolves itself (big writes, opaque writes, unkeyed, outcome deltas, §8) are
absent.

**Framing.** `u8` protocol version · `u32` header length · JSON header ·
payloads, back to back in output order: each output's `offset` is where
the one before ended, and together they fill the body, so no byte is two
outputs'. A version the engine does not speak gets `415`, and the worker
resolves locally; a frame whose bounds do not hold gets `400`.

**Request.**

```json
{"worker": "k3f…", "outputs": [
  {"name": "orders", "partition": "", "kind": "patch", "commit_number": 12, "generation": 184467,
   "base": {"prefix": "keys/orders/_/", "head_commit": 11},
   "keys": 1000, "offset": 0, "size": 41250, "digest": "9e07…"}
]}
```

- The payload is the sorted run (`SortedEntries.encode`) as one delta file
  (`key-index-format.md`): each key updated with its payload (a source
  key's version), or removed. Every written key takes `generation`, the
  attempt's (`lifecycle.md` §9.7), from the header, so an immutable store's
  name for it is known. One format, one parser, the CRCs included.
- `kind` is `patch` or `replace`: the same run means different deltas
  (`{a: 1}` against `{a: 1, b: 1}` is empty as a patch and deletes `b` as
  a replacement).
- `base` names the snapshot the worker resolved against: the index (its
  prefix, which survives renames) and the head's commit number, as pinned in its
  spec. `commit_number` is the commit the delta will carry.
- `digest` is the payload's XXH3-128.

**Validation.** The engine trusts nothing in the header it can check
against the attempt's preparation, which it holds in memory for every
live attempt (and rebuilds from `.spec` on adoption):

- the worker is the one the engine admitted (`lifecycle.md` §4: the
  `start`ed one, or after a restart the owner its control file names); any other gets
  `409` and resolves nothing — it should not be running;
- the attempt is live and holds the claim for `(name, partition)`;
- `name`, `partition`, `commit_number`, `generation` and `base.prefix` are the ones
  prepared for that output, and `base.head_commit` is the head's commit
  number now;
- `kind` is allowed for the output (a `replace` of an output whose write
  can only be a patch is refused);
- the payload is what the header says, from its own bytes: its digest is
  `digest`, and decoded once — every block's CRC, raw length and format,
  keys strictly increasing, every length checked before it is used — it
  holds `keys` entries. The limits below apply to what it holds, decoding
  stops at them (`too_big`), and the resolve reads the decoded run, never
  the payload again.

A mismatch is a per-output decline (`stale`, `not_live`, `invalid`), never
a guess.

**Deduplication.** Concurrent and repeated requests share one computation
when their whole semantic input matches:

```
(attempt, worker, name, partition, kind, commit_number, generation, base.prefix, base.head_commit, digest, keys)
```

The answer is a pure function of that tuple and the snapshot content,
and the engine writes nothing, so a retry after a dropped connection or an
engine restart gets the same answer or a decline. There is no request to
freeze, no response to recover and no engine-written file to clean up.

**Response.** Same framing: a header, then each resolved output's delta.

```json
{"outputs": [
  {"name": "orders", "result": "delta", "added": 212, "removed": 3, "entries": 640,
   "file": {"size": 26810, "digest": "51aa…", "first": "6b31", "last": "6b39"}, "offset": 0},
  {"name": "events", "result": "empty"},
  {"name": "sessions", "result": "declined", "reason": "cold"}
]}
```

- `delta`: the payload is the delta as one file, ready to upload under the
  worker's own name `{commit_number:012d}-{attempt}-0.lay`. On an
  immutable store's output, each updated or removed entry names the
  generation it replaced (`key-index-format.md`): what the partition's
  cleanup deletes. The worker checks the digest, uploads the bytes
  unchanged, and commits it as `DeltaFiles` from the header (`delta_files`:
  `added`, `removed`, its `entries` and first and last keys, in hex). An
  answer is at most 256 KiB, one file with no index object; a larger delta
  is declined `too_big` and resolved by the worker.
- `empty`: the write changes nothing in the index. No file. §3 decides
  whether that means "unchanged".
- `declined`: the worker resolves this output locally (§6), at once.
  Reasons: `cold` (the snapshot is not warm), `busy` (queue full),
  `too_big` (over the key, byte or replacement limits, or an answer over
  256 KiB), `stale` (the head moved), `not_live`, `invalid` (see
  validation), `corrupt` (the answer failed its digest at the worker).
- Overload of the whole engine is `503` with no body; transport errors and
  timeouts count as declines.

**Exact counts.** Every resolver reads each written key's entry at the
head, so `added` and `removed` are exact (`key-index-design.md` § Writing).

**Which snapshot.** While an attempt is live it holds its claim: no
other commit can change that index, and a merge changes its files but
not its content. The engine therefore resolves
against the layers it currently holds for that partition, once validation
passed, and holds their files before any asynchronous work (§5); the
content is the pinned snapshot's even if a merge swapped layers since
prepare.

**Mixed attempts.** Each output is independent: one may be resolved by
the engine, one declined and resolved by a streamed join, one
written opaquely (`Sql`), and the attempt commits them together. The worker uses
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
| `resolve_max_decoded` | 64 MB | a run decoded: its decompressed blocks, its keys and versions — one budget |
| `resolve_max_entries` | 2M entries in the snapshot | replacements: the engine joins the run with every entry |
| answered delta | 256 KiB (`layers.SMALL`) | a larger delta is declined `too_big`: it would need an index object |
| `resolve_queue_bytes` | 64 MB of queued payloads | admission; beyond it, `busy` at once |
| `resolve_concurrency` | 2 | resolves in flight, on their own threads, apart from merges' |

The worker sends a patch only under `resolve_max_keys` and a replacement
only when the pinned count plus its run is under `resolve_max_entries`;
the engine checks again.

**Other resolver targets.**

- **Source commits** (`commit_source`) run in the engine: they call the
  same warm resolver in-process, with the same local fallback, and keep
  the optimistic head check before recording.
- **Sensor ticks** (`lifecycle.md` §11) post a small key map in this
  framing to their tick route, and the engine resolves it in-process as it
  does for source commits; a map over `sensor_map_max` is resolved on the
  sensor worker, which commits a reference to its delta. Neither uses
  this route or an attempt's validation.
- **Stored outcomes** are not resolve targets: outcome deltas are resolved
  by the worker, with exact lookups of prior records
  (`per-key-processing.md` §9).

**Failure cases.**

| Case | Outcome |
|---|---|
| Engine unreachable, restarting, or slow | Local resolve after the timeout; the same delta (§6) |
| Worker dies after the response, before the gate | Nothing to clean on the engine; the uploaded delta is an orphan the engine collects (`key-index-design.md` § Lifecycles) |
| Worker dies after the gate | Repair intents and repair (fenced); nothing for immutable stores |
| Attempt canceled while a resolve runs | By the cancel record (`lifecycle.md` §2.2): a draining attempt still resolves; once the record is `forced`, or the attempt ended, a resolve checks that when it starts and between chunks, and stops, releasing its pins |
| A merge publishes during a resolve | The resolve keeps the files it holds; collection waits for the pin |
| A second worker of the attempt | `409`: the engine admits one worker (lifecycle §4) |
| Local copy corrupt | Its index's copies dropped and filled again from the store; the request is declined `cold` meanwhile |

## 5. The engine cache

One cache serves every reader on the engine (`LayerCache`,
`key-index-design.md` § The engine's cache): resolves, source commits,
merges, the engine's own Δ and scans when it plans batches, and key
listings. It runs on the key service's thread and merges' threads, never
on the engine's event loop.

- **In memory:** index objects and small parts (read whole), least
  recently used out, bounded in bytes.
- **On disk:** raw copies of layer files, read by positioned reads and
  checked block by block (CRC) as they decompress. Installed from the
  engine's own writes (a merge's outputs as it uploads them, a resolved
  delta at its commit), and filled per index; evicted by bytes, least
  recently used first; a file a read holds open is not evicted.

**Unit: an immutable file.** Entries are keyed by object path; a path is
never reused, so an entry is never stale, only evicted. An index is
**warm** when every file of its layers' main parts is on disk.

**Resolves read warm only.** A cold index is declined at once (`cold`), and
a fill of it starts in the background, holding a reader pin until its
fetches end; the worker resolves this time, the next request finds it
warm. A copy that fails its check mid-resolve has its index's copies
dropped and filled again; the request is declined `cold`.

**Candidates.** A delta the resolver answered is kept in memory under the
path the worker will upload it to; when the commit installs that path
with that size, the cache installs the candidate without a GET. An
attempt that ends without committing drops its candidates.

**Admission.** One rule for every resolve: `resolve_queue_bytes` of
queued payloads and `resolve_concurrency` turns, both held until the work
— native threads included — has ended; past the queue, `busy` at once.

## 6. The cold path

When the engine cannot answer, the worker resolves alone, against the
index pinned in its spec (`key-index-design.md` § Writing a commit's
delta):

- **Sparse** (`LayerIndex.write_patch`): the blocks its keys fall in, per
  part sought or streamed by a time model in requests and bytes, held in
  memory while it resolves. Taken when they hold at most 64 MiB.
- **Streamed** (`write_patch` past that, `write_replace`): a join of the
  sorted writes against every layer's main part, 8 MiB per input at a
  time. A replacement removes every present key it omits; a reconcile
  streams the store's listing with the run's writes as an overlay.

Both apply the one rule per key (`native/src/delta.rs`, `layers::DeltaWriter`):
what the head holds — absent, or present at a generation and payload —
against an upsert (with its payload, if any) or a remove. So the delta and
the count are exact, and on an immutable store's outputs every updated or
removed key names the generation it replaced. Merges need no cleanup of
their own: every version a merge drops was named by the delta that
replaced it.

## 7. Engine-served reads

Removed (§11). The engine plans every input batch from Δ over the layers
and hands it in the attempt's spec (`observed-set.md`); a per-key batch's
prior stored outcomes and its retry walk come in the spec too (§8). A
worker's remaining index reads are its own resolve (§6) and a whole load
of an immutable store's keys, paged from the pinned index.

## 8. Readers for per-key processing

`per-key-processing.md` is authoritative for everything about failures:
the entry format, the **transition table** (interrupted keys follow the
cancel record's `reason`, `lifecycle.md` §2.2), the **one eligibility
predicate** `eligible(entry, now, deploy, forced)`, the outcome counts
moved by transitions, the conservative minima maintained from each commit
and made exact by completed retry passes, and retry-pass identity. This
doc implements none of that differently; it calls the same SDK predicate.

The engine reads a per-key batch's stored outcomes when it plans it
(`Observing._failed`): the prior records of its keys, by a lookup of the
outcome index at its head, and — when retries may be due — the retry
batch, a walk of the stored outcomes from the pass's place keeping those
`eligible` says are due. Both come in the spec. Failure deltas are not
resolved here: the worker resolves them locally against the outcome index
its spec pins.

Not in v1, and not to be built from this doc: engine-side pattern skip
hints (pattern match counts per committed delta), and coalesced
recomputation of failure minima from the cache.

## 9. Costs

The request costs measured for this design (scenario E: 1K random changes
every 10 s into 100M keys) were the leveled index's, and are kept in
`key-index-costs.md`; the stamped-layer index's are in
`key-index-design.md` § Measured. What carries over:

- **A warm resolve reads no object**: the delta is computed from local
  copies, and only the worker's PUT of the delta remains. A cold resolve
  at 1M keys measured 0.50 s and 17 GETs before the engine's cache.
- **Cold fills.** One read of an index's main parts per admission into the
  cache: at 1M keys ~16 MB.
- **Replaced generations.** A varint per updated or removed key, as its
  distance from the commit's generation: one byte, often. Cleanups are
  DELETEs, free on S3, batched.
- **Network.** Requests and responses are tens of KB per 1K-key commit.
  Within one availability zone that is free; across zones EC2 charges per
  GB each way. Placing workers in the engine's zone avoids it.

## 10. Tests and benchmarks

- **Equivalence.** The engine's delta is the worker's own, byte for byte,
  replaced generations included (`test_keys_layers.py`); every resolver's
  delta agrees with the per-commit fold over random histories with merges.
- **Repair.** The `a=1 → a=2 → a=1` sequence rewrites the store with an
  empty delta; a replacement with repair intents rewrites the partition;
  "unchanged" only without intents; for a fenced store, an older writer's
  pending `k=2` lands before the repair read; immutable stores skip repair.
- **Protocol.** Requests differing only in `kind` are not deduplicated;
  identical ones are; a header naming another partition, commit number or prefix is
  declined; a second worker gets `409`; each decline reason falls back
  locally; a timeout falls back and the late response is dropped; an
  unknown protocol version gets `415`; mixed attempts commit together; any
  body is framed exactly or `Malformed` (a property).
- **Cache.** A warm index reads no object; a cold one is declined and
  filled; a candidate is installed without a GET at commit; a corrupt copy
  is dropped and filled again; two indexes sharing the cache never read
  each other's files.
- **Unknown writes.** A dead opaque writer that deleted `a` and inserted
  `b`: the next patch acquires, reads the store's key map, reconciles both
  keys and only then clears the intent; a replacement overwrites instead.

## 11. Not in this design

- **A canonical row digest.** Built for a while, then removed:
  nothing hashes user data (`versions.md`).
- **An object-store channel** for resolves. Workers can always reach the
  engine over HTTPS; a worker that cannot simply resolves locally.
- **Sensor publication, rescoping, cancellation and retry pacing** —
  `per-key-processing.md` and `lifecycle.md`.
- **Later, not v1:** pattern skip hints and coalesced recomputation of
  failure minima (§8).
- **Engine-served input reads.** Built for a while: the engine answered an
  attempt's index reads at `start` from its cache. Removed with the
  observed set (D153, step 5): the engine plans every batch from Δ and
  hands it in the spec, so a worker reads no index for its inputs.

## 12. Three choices

- **No frozen request per attempt.** The resolver persists nothing and
  decides nothing, so a retried request needs no stored answer: it is
  recomputed, or deduplicated while in flight.
- **Resolve against the current layers, not the pinned manifest.** Under
  the claim they have the same content, and the current files are the
  warm ones; validation and holding the files before any asynchronous
  work make a moved head a `stale` decline rather than a wrong answer.
- **Answers are one small file.** A delta over 256 KiB is declined: the
  answer stays one file with no index object, and a big write resolves on
  the worker, which streams.

## 13. Open questions

1. **Thresholds.** `resolve_max_keys`, `resolve_max_entries` and
   `resolve_timeout` come from the leveled index's warm grid; to measure
   again on layers.
2. **Engine capacity.** Resolves and merges have separate threads; if
   offloading becomes necessary, the cache and all its readers move
   together into `engine_executor` (`object-store-state.md` §6), rather
   than a second cache-owning service.

## 14. As built

| Piece | Where |
|---|---|
| The delta, sparse and streamed; the one rule per key | `LayerIndex.resolve`, `write_patch`, `write_replace`, `compute` (`solera/keys/layers.py`); `layers::resolve`, `LayerJoin`, `DeltaWriter` (`native/src/layers.rs`) |
| Repair by store kind, unknown opaque writes | `_store_outputs` and `_reconcile` (`solera_worker/worker.py`); acquisition is the lifecycle's |
| The cache | `LayerCache` (`solera/keys/layer_cache.py`) |
| Framing, validation, deduplication, declines | `Resolver`, `request`, `answers`, `delta_files` (`solera/keys/resolver.py`) |
| The route | `POST /api/projects/{p}/attempts/{a}/resolve` (`api.py`), `Engine.attempt_resolve` (`attempts.py`), the channels' `resolve` |
| The thread, write-through, source commits | `KeyService` (`solera_server/keyservice.py`); `Engine._cache_commit`, `_resolve_source` |

Differences:

- **Checksums are CRC-32** of each compressed block, not CRC32C.
- **No block cache of the engine's own beyond the disk copies**: blocks
  are read through the OS page cache by positioned reads.
