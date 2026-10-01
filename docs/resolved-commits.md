# Engine-resolved commits — proposal

Status: **proposed**, not built. It changes how a keyed write learns what
it changed (`object-store-state.md` §6, "Compute a delta"), how a downstream
attempt receives its pending changes, and how row versions are digested.

## 1. Why

A keyed write must tell, for each key it writes, whether it is new,
changed or unchanged: the store writes only changed rows, and downstream
`Incremental` consumers see only real changes. Today the **worker**
answers that question against the index on S3, and a worker is a cold
reader: it starts with nothing and throws everything away when it exits.

For `k` random keys over an index of `N` entries, a cold reader can only
pay in requests (one block read per key, ~`k` GETs) or in bytes (read the
files whole, ~`N` bytes). Every structure in a file — blocks, filters,
block index — has this property, because random keys have no locality.
The read planner (`KeyIndex._plan_reads`) exists only to pick a point on
that curve.

Measured at 100M keys in steady state (`bench/keys/results.md`), a 1K-key
patch costs ~50 GETs and ~0.8 s, and most of it is fetching the tail of
every file — about 350 MB of filters, re-read on every commit to answer a
question about 1,000 keys.

A **warm reader** has no such curve. Index files are immutable, so a cached
copy is never stale; and the engine already sees every new file — it
writes compaction outputs and commits every delta. A warm engine answers
the same 1K-key question with no GETs and milliseconds of CPU.

## 2. The design in one paragraph

Small writes ask the engine. The worker computes its sorted run of
`(key, version, deleted)` — what it intends to write — and puts it as one
object; the engine resolves the run against its warm copy of the index and
writes the batch's delta file; the worker reads the delta and writes only
the changed rows to the store. Big writes — replacements of big scopes and
patches above a threshold — stay on the worker as the streaming merge-join
it already does for replacements, which reads every file anyway in a few
large GETs. The read planner is deleted. The engine's warm copy also
serves downstream: small pending windows are inlined into the downstream
attempt's spec.

## 3. Who resolves what

At prepare, for each keyed output the attempt writes, the engine puts in
the spec:

```json
"outputs": {"orders": {"exists": true, "batch": 12, "index": {"…": "…"},
                       "resolve": {"max_keys": 100000}}}
```

The worker computes `n` = keys written + keys removed (after `_repair`, §8
of the state doc), plus the index's live count for a replacement:

| Write | `n ≤ max_keys` | `n > max_keys` |
|---|---|---|
| Patch | ask the engine | streaming merge-join on the worker (new: the patch variant of `KeyIndex.replace`) |
| Replacement | ask the engine | streaming merge-join on the worker (today's path) |
| `Sql` | the store reports its keys after writing (today's path) | same |

`resolve` is absent when the engine will not answer — `max_keys = 0` by
configuration, or the engine cannot hold the index (§6). The worker then
uses the merge-join whatever `n` is.

## 4. The ask, step by step

```
worker   compute the run: sorted (key, version, deleted) per keyed output
worker   PUT  runs/{run}/{attempt}.ask                 header + one .kx run per output
engine   sees the ask (§5)
engine   resolve each run against the warm index (§6), newest entry wins
engine   PUT  keys/{output}/{scope}/{batch}-{attempt}.kx   per output, possibly empty
worker   sees the delta of its last output (§5) → GET each delta
worker   create-if-absent {attempt}.writing  {"state": "writing", "intents": {…delta files…}}
worker   write only the delta's keys to the store; result as today
engine   settle: commit the deltas as today
```

- **The ask** is one object per attempt: a small JSON header
  `{"outputs": [{"name", "batch", "replace", "offset", "size"}]}` followed
  by one `.kx` file per keyed output (the format of
  `key-index-format.md`, so the engine reads it with the same code).
- **The delta** has the deterministic name the worker's own delta has today
  and is written in output order, so the existence of the last output's
  delta means all are there. An empty delta is an empty `.kx` file; a
  delta that is empty on an output that exists means "unchanged", exactly
  as today, and the engine deletes the file at settle.
- **Counts are exact.** The engine resolves against full data, never
  through filters, so `added` and `removed` are exact and `inexact` does
  not grow.
- **Repair is unchanged.** `_intended` and `_repair` run on the worker
  before it builds the run; the delta files remain the fence's intents.

## 5. Seeing each other — through the object store

With only the object store between them (the `objects` channel of the
worker-communication design), both sides poll:

- the engine GETs `{attempt}.ask` every `ask_poll` (0.5 s) for each
  in-flight attempt whose spec has a `resolve` output, from launch until
  the ask appears or the attempt ends;
- the worker GETs its last delta every 0.25 s after asking, up to
  `resolve_timeout` (30 s). Past it, the worker falls back to the
  merge-join and names its delta `{batch}-{attempt}-w` so a late engine
  delta cannot collide with it; the engine deletes whichever delta the
  result does not list when the attempt ends.

Cost of the polling: 2 GETs/s per waiting attempt. A 10 s attempt pays ~20
GETs for its engine poll — fewer than the ~50 it saves at 100M keys, and
priced at a tenth of a PUT each. A 1-hour attempt pays ~7,200 GETs
(~$0.003). Latency: ~0.5 s on average for each side to notice.

**With an HTTP channel** (the worker-communication design), the run goes up
as the body of `POST /api/attempts/{id}/resolve` and the response says the
deltas are written; there is no ask object and no polling, and the
round-trip is a few milliseconds plus the resolve. The object-store path
stays as the fallback, so a lost request costs only latency.

## 6. The engine's warm copy

- **What it holds, per index:** block indexes and filters in RAM (~4 B per
  entry; ~400 MB at 100M keys) and blocks on local disk. On disk, blocks
  are stored decompressed, with restart points every 16 entries computed
  on load, so a lookup is a binary search over the block index, then over
  the restart points, then a scan of ≤16 entries: a few microseconds.
  The S3 format does not change; the local form is the engine's.
- **How it stays warm:** write-through. The engine puts every file it
  writes — engine-resolved deltas, compaction outputs — into the cache as
  it uploads it, and fetches every file a worker commits (a big write's
  deltas) when it commits it, whole, in one GET. In steady state the
  engine never reads what it already wrote.
- **Cold start** (after a restart, or an evicted index): the first ask for
  an index reads its files whole — a few large GETs — and every later
  commit reuses them. There is no planner: a whole read is never wasted
  once there is a next commit.
- **Budget:** one LRU over indexes, `engine_key_cache = KeyCache(max_size=…)`.
  An index larger than the budget gets no `resolve` in its specs.
- **Off the event loop:** resolves run on the maintenance thread with its
  own loop, at most `resolve_concurrency` at a time. The engine loop never
  waits on one.
- **Determinism:** the scope lock — one attempt per (asset, scope) from
  launch to settlement — means the index content cannot change between
  prepare and resolve (compaction changes files, not content). A resolve
  repeated after an engine restart writes the same bytes.

Expected cost, 100M keys, a 1K-key patch, warm:

|  | Today | Engine-resolved (objects channel) | With HTTP channel |
|---|---|---|---|
| Worker GETs | ~50 (tails ~350 MB + blocks) | 1 per output (the delta) + polls | 1 per output |
| Engine GETs | 0 | 1 (the ask) + polls | 0 |
| Index PUTs | 1 (delta) | 2 (ask, delta) | 1 (delta) |
| Time to know the delta | ~0.8 s | ~1 s, mostly polling | ~10 ms |
| Count | approximate when filters decided | exact | exact |

## 7. Downstream: inlined changes

When the engine prepares an attempt with an `Incremental` edge on a keyed
upstream, and the pending window's entries are in its warm copy and number
at most `inline_max` (10K keys, ~1 MB of JSON), it inlines them:

```json
"changes": {"from": 56, "to": 57, "after": null, "full": false, "limit": 10000,
            "inline": {"upserted": {"alpha-file-2": "3f9c…"}, "deleted": []}}
```

The worker skips the index read and asks the store for those keys. The
window obeys `limit` and `after` exactly as a paged one, and `delivered`
is reported the same way. The engine inlines only from its cache — it
never waits on S3 to prepare — and a window it cannot inline goes out as
today.

So one warm copy serves three readers: the upstream commit's lookup, the
downstream change delivery, and compaction.

## 8. One row digest

Today a row's version depends on how the producer returned it: Python rows
are BLAKE2b-128 of their JSON (or pickle) encoding, Arrow rows XXH3-128 of
Arrow's row encoding. An asset that switches from returning dicts to
returning a DataFrame re-delivers every key downstream once.

Proposal: one canonical row encoding, implemented once in Rust and fed
from either side.

- A row is its columns sorted by name; each value is a type tag and a
  canonical payload: null; bool; integer (as i64, else as a decimal
  string); float (f64 bits, `-0.0` and NaN normalized); string (UTF-8);
  bytes; date / timestamp (UTC microseconds) ; list (length + elements);
  struct / dict (sorted keys + values).
- Python rows are walked into that encoding in Rust; Arrow batches are
  encoded column-wise into the same bytes. The digest is XXH3-128 of it.
- A value with no canonical form (an arbitrary Python object) falls back
  to its pickle bytes under its own tag; such rows can never come from
  Arrow, so nothing is lost.
- A declared `revision` column is used verbatim, as today.

The digest of the same logical row is then the same however it arrives,
and the per-row Python call that makes list input slow (~1 µs per row
under the GIL) goes away.

## 9. What changes in the code

- `solera/keys`: delete `_plan_reads`, `_read_options`, `_filter`,
  `_resolve` and the planning constants; add the patch variant of the
  streaming merge-join; add the engine's local form (decompressed blocks,
  restart points) and lookup in `native/`.
- `solera_worker/worker.py`: `_store_outputs` builds the run and asks, or
  merge-joins; the fallback after `resolve_timeout`; inlined changes in
  `_resolve_inputs`.
- `solera_server`: the ask poll in `_watch` (or beside it); resolves on the
  maintenance thread; write-through cache; `resolve` and `inline` in
  `_prepare`; the `-w` delta cleanup at attempt end.
- `native/`: the canonical row encoding for Python and Arrow input.
- Docs: §6 "Compute a delta" and "Deliver pending deltas", §8 (the ask),
  `key-index-costs.md`.

## 10. Failure cases

| Case | Outcome |
|---|---|
| Worker dies after asking, before the fence | The attempt fails as today; the engine's delta is deleted at attempt end like any uncommitted delta. |
| Cancel while the worker waits for its delta | The engine writes the abort fence; the worker's heartbeat sees it and stops. A delta written meanwhile is garbage at attempt end. The engine skips asks of attempts it aborted. |
| Engine restarts with an ask pending | Adoption resumes the ask poll; the resolve is deterministic (§6), so redoing it is harmless. |
| Engine never answers | The worker falls back after `resolve_timeout`; correctness never depends on the engine answering. |
| Engine cache corrupt or evicted mid-resolve | Blocks carry CRCs, checked on load; a bad or missing file is fetched again from S3. |
| Worker dies after the fence | Unsettled intents and repair, unchanged. |

## 11. Tests

- Engine-resolved and worker-resolved commits produce byte-identical
  deltas and identical counts, over random patches, removes, replacements
  and repairs (property test against `_python.py`).
- Ask → cancel, ask → worker death, ask → engine restart, ask → timeout
  fallback, a late engine delta after a fallback.
- Warm-cache steady state: zero engine GETs per commit after the first.
- Inlined windows deliver exactly what paged windows deliver.
- The same rows as dicts and as Arrow produce the same digests.
- Bench: 1K, 10K, 100K-key patches into 1M/10M/100M, warm and cold, both
  channels: GETs, PUTs, time to delta, engine CPU and RSS.

## 12. Open questions for review

1. Is polling the right object-store signal, or should the engine find
   asks some other way (piggyback on `.beat`, a LIST of an `asks/` prefix,
   bucket notifications)?
2. `max_keys = 100K`: should the limit be in keys, in expected engine CPU,
   or relative to the index's block count (past roughly one key per block
   the merge-join reads no more than point lookups would)?
3. Engine memory and disk across many indexes — is an LRU over whole
   indexes right, or should it cache at file or block granularity?
4. The engine now does per-commit work for every small keyed write. Is the
   single engine a throughput risk at many commits per second, and should
   resolves be shardable or offloadable like compaction (`engine_executor`)?
5. Should the worker write the delta instead (the engine answering only a
   per-key verdict), keeping the engine read-only on the commit path?
