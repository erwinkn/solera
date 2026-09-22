# Storage redesign: deltas, watermarks, and a bounded control plane

Status: approved design, awaiting implementation. Supersedes §6 of
`architecture.md` and amends §1, §2, §4, §5, §8, §9; the implementer updates
those sections as part of the work so `architecture.md` stays normative.

## 0. Why

A Phase 7 build of the demo ran for ~40 hours on `file://` state and filled
a 296 GB disk. Reproduced on `main` (commit `613720a`): growth is
quadratic in commits and the engine slows in step with it. Three
independent causes, all still present:

| Cause | Where | Growth |
|---|---|---|
| `JsonStore` rewrites the **entire** append history as a new object on every commit | `packages/sdk/src/cursus/stores.py:198-250` | O(N²) bytes — the ~160 GB |
| The full `key → revision` map is re-staged as a new `keys/{sha}.json` on every commit, for every store | `apps/worker/src/cursus_worker/worker.py:278-285`, `apps/server/src/cursus_server/state.py:784` | O(N²) bytes |
| One `keystate/…` row per consumed key, never deleted, **full prefix scan on every plan**; SlateDB WAL never garbage-collected; per-attempt `specs/results/logs` never reclaimed | `state.py:98-110`, `engine.py:668-687`, `storage.py` | O(N) rows, O(N) work per plan, O(commits) WAL objects |

An append output's key map has one entry per batch forever, so the second
and third causes hit Postgres-backed outputs exactly as hard as JSON ones.
This is an engine-model problem; the store is only the first symptom.

## 1. Model changes (SDK surface)

### 1.1 Outputs declare incrementality; the engine learns two booleans

```python
Output("site_events", incremental=True)                       # batch mode
Output("site_files",  incremental=True, key="file_id")        # keyed mode
Output("site_files",  incremental=True, key="file_id", revision="version")
```

- `incremental=True` — the output produces a **delta per commit** and may be
  consumed by `Incremental()` edges.
- `key=` implies `incremental=True`; declaring `key` without it is a
  registration error. `revision=` names the column that supplies the
  per-key revision; absent, the row digest is used (unchanged).
- `Output(mode="append")` is **removed** from the public surface. Whether
  rows are appended or upserted is a store concern negotiated through
  output metadata (e.g. Postgres `primary_key=`). The engine never sees it.
- `PartitionSet` stays a value output. Its element list travels in
  `ref.meta["partitions"]`, written by the harness, read by
  `Engine._scopes`. It no longer uses the key channel.

The manifest records `{"incremental": bool, "key": str | None}` per output.
Stores veto declarations they cannot honor through `can_store` (existing
mechanism; `BlobStore` refuses `incremental`).

### 1.2 `Incremental()` replaces `ByKey()`

```python
inputs={"site_files": Incremental(batch_size=2)}
```

- Registration error if the upstream output is not `incremental`.
- Consumer-side behaviour is identical in both upstream modes: advance a
  watermark. What a delta *contains* is decided by the upstream:

| Upstream | Delta per batch | `ctx.changes[name]` |
|---|---|---|
| `incremental=True` | every row written in the batch | `rows`, `deleted == []` |
| `incremental=True, key=` | rows whose revision changed + removed keys | `rows`, `deleted` |

- `Changes` gains `batches: range` and `full: bool`. `upserted` is kept as
  an alias of the delivered key list for keyed upstreams.
- `AllPartitions` + `Incremental` do **not** compose in this pass (§6).

### 1.3 Run mode: `incremental | full`

`Automation(mode=)` and the run API accept `incremental` (default) or
`full`; `recompute` is removed. `full` resets every `Incremental` edge's
watermark and withholds `prior` and cursor from the producer, as
`recompute` does today. An edge that is not `Incremental` always receives
the full head regardless of run mode.

Per-run override `run.keys[output]`: `"full"` resets that edge's
watermark; `{"keys": [...]}` is a one-off keyed load, legal only on a
keyed upstream, and does not move the watermark.

## 2. Deltas and watermarks (engine)

### 2.1 The delta log

Every commit of an incremental output produces one **delta object**:

```
deltas/{output}/{scope}/{batch:012d}.json
{"batch": N, "rows": <count>, "upserted": {key: revision, ...}, "deleted": [key, ...]}
```

- `batch` is assigned by the engine (`prior.batch + 1`, or `0`) and handed
  to the store in `Scope`. For batch-mode outputs `upserted` is
  `{str(i): digest(row)}` positional, or omitted if the store reports only a
  count — the consumer loads by batch range, not by key.
- **The store computes the delta.** It owns the prior rows, so "did this
  key's revision change" is a store-local comparison (Postgres:
  `WHERE prior.rev IS DISTINCT FROM new.rev`; JsonStore: dict compare on
  the prior batch's payload). `Written` becomes:

  ```python
  @dataclass(frozen=True)
  class Written:
      ref: Ref
      delta: Delta | None = None      # None for non-incremental outputs
  ```

- The harness writes the delta object and records
  `ref.meta["delta"] = {"object": path, "batch": N, "rows": n}`.
  `ref.meta["keys"]` and `keys/{sha}.json` are **deleted**.
- Unchanged content produces an empty delta and the same version; nothing
  downstream fires (rule preserved).

### 2.2 Watermarks

```
watermark/{asset}/{edge}/{scope} -> {"batch": int, "offset": int, "fingerprint": str}
```

Replaces `keystate/…` entirely. Planning an `Incremental` edge:

1. Read the upstream head; `head_batch = ref.meta["delta"]["batch"]`.
2. Read the watermark. If absent, or `fingerprint` differs from the
   current interpretation fingerprint, treat as `(batch=-1, offset=0)` and
   set `full=True`.
3. Pending = union of `deltas/…/{w.batch+1 .. head_batch}`, last writer
   wins per key, removes cancel upserts. For batch-mode upstreams this is
   just the batch range.
4. Sort deterministically; take `batch_size` items from `offset`; `more`
   if any remain.
5. Commit advances the watermark to the last delivered `(batch, offset)`
   with the current fingerprint. `more` re-queues as today.

Cost is O(delta), never O(keys). Delta retention must cover the slowest
live consumer; a watermark older than the retained log resets to
`full=True` (logged as a warning).

### 2.3 External sources

Sources fed through `POST …/sources/{name}/commit` have no store. The
engine keeps the current `key → revision` map for those only, computes the
delta against the supplied `keys=` / `upsert=` / `remove=`, and writes the
same delta object. `keys=` with a full map is O(map) by the caller's
choice.

## 3. Store contract

```python
class Store(Protocol):
    version: str
    ref_type: type[Ref]
    def can_load(self, t, selection) -> bool: ...
    def can_store(self, t, output) -> bool: ...
    async def store(self, write, prior: Ref | None, scope: Scope) -> Written: ...
    async def load(self, ref: Ref, t, selection: Keys | Batches | None) -> Any: ...
```

- `Scope` gains `batch: int` (engine-assigned) and loses `prior_keys`.
- `Batches(lo, hi)` is a new selection type: load rows of batches in
  `[lo, hi]`. `Keys` is unchanged.
- **`JsonStore`** — one object per batch: `data/{output}/{scope}/{batch}.json`.
  The ref lists `batches: [first, last]`. Full load of an append output
  concatenates; full load of a keyed output folds batches (last writer
  wins, removes apply). To bound fold cost the store may write a snapshot
  object every `K` batches (`snapshot_every`, default 64) and fold only the
  tail; the ref records the last snapshot batch. Value outputs are
  unchanged (one object per version).
- **`PostgresStore`** — receives `scope.batch` instead of deriving it from
  `prior_keys`. Computes `Delta` in the upsert via `RETURNING` /
  `IS DISTINCT FROM`. Append vs upsert is chosen from output metadata
  (`primary_key` present → upsert; absent → append with `__batch/__seq`),
  not from `mode`.
- **`BlobStore`** — refuses `incremental`.

## 4. Control plane: trimmed state and an in-memory engine

The engine is single-writer (SlateDB fencing). The in-memory model becomes
canonical; SlateDB is written through for durability and **read only at
open**.

### 4.1 Durable prefixes (SlateDB)

| Prefix | Record | Notes |
|---|---|---|
| `manifest/{rev}`, `sys/*` | project | delete superseded revisions on register |
| `head/{output}/{scope}` | `{ref, commit, at, complete, asset, version}` | unchanged |
| `cursor/{asset}/{scope}` | producer cursor | unchanged |
| `watermark/{asset}/{edge}/{scope}` | §2.2 | replaces `keystate/` |
| `commit/{seq:020d}` | lineage record | **monotonic sequence key** (was uuid; listing order was random) |
| `automation_state/{name}` | `{enabled, last_at, last_run, last_revision, commit_watermark}` | declaration stays in the manifest; `pending[]` removed — OnChange automations consume the commit log from `commit_watermark` |
| `run/{id}` | unchanged minus nothing | |
| `task/{run}/{asset}:{scope}` | drop `result`, `error` (live in `commit` / `attempt`) | |
| `attempt/{task}/{gen}` | drop `lease_until` | |
| `lock/{asset}/{scope}` | `{attempt, generation}` | **no `lease_until`** — leases are in memory (§4.3) |
| `worker/{id}` | unchanged | |

**Removed:** `keystate/`, `keys/` objects, `queue/`, `pending/`,
`runindex/`, `active/`, `pool/` (a pool attempt is a task with
`placement.kind == "Pool"` and status `claimable`; its spec is already in
the object store), `scope/` (rebuilt in memory from the last terminal task
per scope; may be persisted as a cache but is not truth).

### 4.2 Memory-only, rebuilt at open

Ready queue (by `ready_at`), pending-per-scope, run listing order, active
wait loops, **dependents index + unfinished-deps counter per task** (makes
`advance_run` O(1) per completion instead of O(tasks in run)), automation
pending work (commits since `commit_watermark`), scope outcomes, lease
expiries.

Open = scan `run/`, `task/`, `attempt/` (non-terminal only), `lock/`,
`automation_state/`, `head/`, `watermark/`, `worker/`. Bounded by in-flight
work plus project shape, not history. Completed runs are evicted from the
working set after their terminal transition; the API reads them from
SlateDB.

### 4.3 Leases in memory

Claim writes `lock/` durably. Renewals (engine wait loop, pool worker
heartbeat) update memory only. `owned()` and the sweeps read memory. On
open every lock is treated as expired: attempts are marked `expired`, tasks
re-queued — the same path `sweep_scope_leases` takes today. This removes
two durable puts per 20 s per in-flight attempt.

### 4.4 SlateDB settings and GC

In `storage.py`:

- `flush_interval`: `"1s"` (was `100ms`). Caps WAL objects at 1/s at any
  load.
- `l0_sst_size_bytes`: `1 MiB` (was 8). Bounds restart replay to ~1000 WAL
  objects.
- Run the garbage collector: a background task calling
  `Admin.run_gc_once(GarbageCollectorOptions(...))` every 5 minutes with
  `min_age_ms = 300_000` for `wal_options`, `manifest_options`,
  `compacted_options`, `compactions_options`. Expose `cursus gc` as a CLI
  subcommand for one-shot use. `AdminBuilder(path, object_store)` — same
  `native` store the writer uses.

## 5. Retention

Per-asset policy, project default, engine-enforced:

```python
@asset(..., retention=Retention(days=7, runs=1000))
Project(..., retention=Retention(days=30))
```

A periodic sweep (same cadence as GC) deletes, per asset:

- attempts, and their `specs/`, `results/`, `logs/` objects, older than
  the policy **and** beyond the run count;
- `deltas/` and `data/` batch objects older than the policy **and** below
  the minimum live consumer watermark **and** not referenced by any live
  head or by a retained commit;
- `commit/` records outside the policy.

Never delete anything referenced by a head. Never delete a delta a live
watermark still needs — bump the consumer to `full` instead, and log it.

## 6. Non-goals for this pass

`ParquetStore` / DuckDB; splitting server and worker packages;
`AllPartitions` + `Incremental` composition; changing placements or the
harness protocol beyond the delta object; multi-writer engines.

## 7. Implementation phases and gates

Each phase lands green on `uv run pytest -q` and `pnpm -C apps/console test`
before the next starts. Keep commits per phase.

**Phase A — stop the bleeding (small, independent).**
`JsonStore` one-object-per-batch (§3); `flush_interval` 1 s and
`l0_sst_size_bytes` 1 MiB; SlateDB GC background task + `cursus gc`.
*Gate:* a soak test (`tests/test_soak.py`) runs the demo project in-process
with a fake clock for ≥ 500 batches per site and asserts `objects/data`
byte growth is linear in batches (fit slope; reject if the second
difference is positive beyond noise) and that `metadata/wal` object count
stays bounded after GC.

**Phase B — deltas and watermarks (the model).**
SDK: `Output(incremental, key, revision)`, `Incremental()`, `Changes`,
`Batches`, `Written.delta`, `Scope.batch`, `mode="append"` removed,
`recompute` → `full`. Stores compute deltas. Harness writes `deltas/…`,
drops `keys/`. Engine: watermarks replace `keystate/`; `_bykey_diff` →
`_incremental_plan`. PartitionSet via `ref.meta["partitions"]`. External
source commits per §2.3. Update `demo.py`, `example/brimstone.py`,
`architecture.md` §1/§2/§4/§5/§6/§8, README.
*Gate:* existing suites pass with renamed surface; the soak test asserts
`keys/` no longer exists and `deltas/` grows linearly; a new test proves a
keyed upstream rewriting identical rows delivers an empty delta and
`skipped` downstream; a test proves `full` resets a watermark; a test
proves a fingerprint change forces `full=True`.

**Phase C — control plane.**
Trimmed prefixes (§4.1), in-memory model with rebuild-at-open (§4.2),
leases in memory (§4.3), `commit/{seq}`, `automation_state` +
`commit_watermark`, dependents index. Remove `pool/`, `active/`, `queue/`,
`pending/`, `runindex/`, `scope/` (or mark cache).
*Gate:* restart test — run the demo, kill the engine mid-flight (including
with a pool attempt claimed), reopen, assert every in-flight task is
re-queued exactly once and no head or watermark is lost; a test that a
10 000-task run completes with `advance_run` doing O(1) reads per task
(count `tx.get` calls via a counting `Tx`); API listing of commits is
newest-first.

**Phase D — retention.**
`Retention` on assets and project; the sweep (§5); `cursus retention
sweep` CLI.
*Gate:* soak test extended: with `Retention(runs=50)` the number of
attempt objects per asset is bounded at 50 × outputs after 500 batches
while every head still loads and every live consumer still plans without
`full=True`.

## 8. Measured reference points

From the investigation, useful as sanity checks:

- Demo on `main`, `file://`, 15 min: `data/site_events` 0.37 MB → 5.19 MB
  while uptime grew 2.6× (quadratic). Extrapolates to 200 GB at ~37 h,
  matching the incident.
- SlateDB with `storage.py` settings: one WAL object per durable commit,
  4940 objects after 4940 commits, none reclaimed; one `run_gc_once` pass
  → 845.
- DuckDB keyed delta over 1M rows: 0.14 s (reference for the eventual
  `ParquetStore`; out of scope here).
