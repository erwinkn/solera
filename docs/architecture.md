# cursus — architecture

cursus is an asset-first orchestrator. An **asset** is a function that produces
**outputs**. A **store** writes and reads them. A **commit** records which
outputs changed. Everything else — partitions, incrementality, automations,
placement — is a small amount of engine state around those three things.

This document is normative and supersedes every earlier design note.
`example/brimstone.py` is the reference example. Sections: 1 Model · 2 Assets
· 3 Refs · 4 Stores · 5 Inputs · 6 Incrementality · 7 Partitions · 8 Runs ·
9 Automations · 10 Execution · 11 Registration · 12 Later and non-goals.

## 1. Model

Three rules:

1. **The engine is control-plane only.** It records, for each `(output,
   partition)`, the committed ref and its metadata, plus cursors, per-edge
   key state and automation state. It never moves, parses or interprets
   payloads. The one value-derived thing it reads is the **key map**
   (§6), which it defines.
2. **Stores own data semantics.** Where an output lives, how a write
   applies, how a load materializes, how writes stay correct under replay.
   Core defines no write semantics: a bare return value is a full
   replacement and every store implements it; the store reports which keys
   the write left behind (§4). `Patch` is an SDK type for partial writes
   that stores opt into, so producers spell them the same way everywhere.
3. **Consumption semantics live on edges.** Whole, by key, or all
   partitions is a property of `inputs=`. What an output *exposes* (its
   declared key) is the output's; how it is consumed is the edge's.

Corollary: **`changed` means "the committed ref's `version` changed"** and
nothing else fires downstream work. A poll that produces identical content
yields an identical version, no changed ref, and wakes nothing.

| Term | Meaning |
|---|---|
| **asset** | A function plus declaration: `outputs`, `inputs`, `partitions`, `executor`, `automations`. An asset with no outputs is a **job**. |
| **output** | A named slot on a store. The thing with heads. |
| **scope** | One partition key of an asset, or `""` when unpartitioned. |
| **head** | The committed ref of `(output, scope)`. |
| **ref** | A self-contained pointer into a store, with a version (§3). |
| **commit** | The atomic transaction installing an attempt's result: heads, lineage, cursor, key state. |
| **run** | A request to materialize targets. It plans **tasks**, one per `(asset, scope)`. |
| **attempt** | One execution of a task. |
| **cursor** | Per-scope JSON state the producer sets and receives back (§6). |
| **key map** | The complete `key → revision` map of `(output, scope)` at a commit: one object, staged by the harness, read by the engine (§6). |
| **placement** | Where an attempt runs: a typed request built from a project-level environment (§10). |

Three processes:

| Process | Runs | Holds |
|---|---|---|
| **server** | engine + API. No user code. | SlateDB state, object store credentials |
| **harness** | user code: producers, stores, resources. Launched per attempt by a placement. | store/resource secrets via `env:`, object store access via the environment's auth |
| **console** | UI over the API. | nothing |

## 2. Assets

```python
@asset(
    outputs=Output("qaqc_samples", store="postgres", schema="qaqc",
                   primary_key=["sample_id"], partition_column="site"),
    partitions=sites,                 # an asset producing a PartitionSet (§7)
    inputs={"qaqc_files": ByKey()},   # an incremental edge (§5)
    automations=AutoRefresh(),        # OnChange over inputs + deps (§9)
)
async def qaqc_samples(ctx, qaqc_files: pd.DataFrame, sharepoint): ...
```

| Arg | Meaning | Default |
|---|---|---|
| `outputs` | `Output` or tuple of them. Empty tuple = job. | one `Output(fn.__name__)` on the default store |
| `inputs` | edge declarations (§5) | every non-resource parameter binds the same-named output |
| `deps` | unbound inputs (§5) | `()` |
| `partitions` | partition declaration (§7) | unpartitioned |
| `executor` | placement (§10) | `Local()()` |
| `retries` / `timeout` | `Retry(n, delay, backoff)` / seconds | `Retry(3)` / `3600` |
| `version` | opaque string; bump to invalidate incremental state (§6) | `"1"` |
| `on_version_change` | `"fail"` (attempts fail until an operator recomputes) or `"recompute"` (the next attempt of each scope is a recompute) | `"fail"` |
| `automations` | attached automations (§9) | `()` |

Resources (`Project(resources={...})`) bind by parameter name. `ctx` is
reserved and optional.

**`Output(name=None, store=None, key=None, revision=None, mode=None,
**config)`** declares a slot: registry key (defaults to the function name
when the asset has one output), store (default `JsonStore`), and
store-specific config validated by `can_store` at registration.

| Arg | Meaning |
|---|---|
| `key` | Column identifying what was materialized. Declared once, here; consumers never name columns. Independent of `primary_key` (storage identity). |
| `revision` | Column that changes when a key's content changes. Absent: `revision = H(row)`. |
| `mode="append"` | A keyed output whose keys are engine-numbered **batches**: each `Patch` is one new key, a load is the snapshot of every batch at the pinned version (§3), and a `ByKey` consumer receives only new batches. Cannot declare its own `key`. |
| `**config` | Store-specific: `schema`, `primary_key`, `columns`, `indexes`, `partition_column`, … |

**`PartitionSet(name=None)`** is an `Output` whose value *is* a list of
partition keys: default store, each element is its own key, revision =
presence. It is how an asset produces a partition set (§7); it is distinct
from `key=`, which is about incremental consumption. Under `sources=` a `PartitionSet(name)` is fed
from outside through the commit API (§5).

**Return values.** A single-output asset returns the bare value. Otherwise
`Result(outputs={...}, cursor=...)`. Omitted outputs keep their prior head
and do not fire `changed` (error if an omitted output has no head). `cursor`
present sets it; absent leaves it.

**Jobs.** `@job(...)` is `@asset(outputs=())`. A job has inputs, partitions,
a cursor, a placement and automations exactly like an asset; its commit
records lineage and cursor and fires nothing.

**`ctx`:**

| Field | Meaning |
|---|---|
| `partition` / `partitions` | canonical key string / dict view by dimension (§7) |
| `partition_window` | `(start, end)` for time dimensions |
| `cursor` | committed cursor, `None` on first run or recompute |
| `changes[input]` | `Changes(upserted: list[str], deleted: list[str])` for `ByKey` edges |
| `run_id`, `config` | the run and its config |
| `execution` | resolved placement `{kind, cpu, memory: bytes, gpu}` |
| `log(message, **fields)` | structured log line |
| `load(ref, T)` | load any ref through its store |

## 3. Refs

```python
@dataclass(frozen=True)
class Ref:
    output: str       # output name — lineage
    store: str        # store registry key
    handle: Any       # store-defined coordinates, JSON
    version: str      # deterministic change token
    partition: str    # scope this ref is the head for
    meta: dict        # engine-defined: {"keys": {"object", "count"}, "external": bool}
```

- **JSON only.** Refs live in state, specs and results. Handles are dicts of
  primitives. Typed subclasses (`TableRef`, `BlobRef`, `JsonRef`) are
  conveniences over the same wire form: `TableRef.table`,
  `TableRef.where`, `TableRef.sql()`.
- **Self-contained.** A historical ref resolves without current store
  config: everything `load` needs (table, partition slice, key and revision
  columns, newest batch) is in the handle. Credentials never appear; `env:`
  indirection only.
- **By reference.** Annotating an input with a `Ref` subclass hands the
  producer the pinned ref instead of loading it. With `Sql` this is the
  in-database path: take `TableRef`s in, return a statement over
  `ref.table` / `ref.where`, never load a row into the harness. Every store names its ref
  type; the registration check is `can_load(TableRef, None)`.
- **Partition slices.** A partitioned output on a shared table names its
  slice in the handle (`where: {site: "Richmond"}`), written when the output
  declares `partition_column`. Two partitions never share a handle.

### Version

`version` is a deterministic function of `(prior.version, write)`, computed
by the store before the write:

| Write | Version |
|---|---|
| bare value | `H(payload)` — the store defines `H` per accepted type |
| `Patch` | `H(prior.version ‖ H(op))` |
| no-op (zero rows, zero keys) | `prior` unchanged |

Replay-stable without hashing tables. Accepted imprecision: a recompute or
converging incremental writes may give different versions for identical
content (over-eager, never wrong).

### Mutable-store contract

Content-addressed stores get pin→load→commit atomicity for free. A store
with mutable handles must provide it:

**Replace slots** keep a version marker per `(output, partition)`.
`store()` refuses when the live marker ≠ `prior.version` (first write:
marker absent; recompute skips the check). `load(ref)` refuses when the live
marker ≠ `ref.version`: a consumer never reads content that is not its
pinned version; the attempt fails and retries. Inherent consequence: a
consumer slower than its producer's interval cannot load a mutable
replace slot. Use an append output, a content-addressed store, or a coarser
schedule.

**Keyed outputs** on a mutable store apply partial writes under the same
marker check, then delete rows whose key is absent from the resulting map:
rows left by an attempt that never committed are absent from
`scope.prior_keys` by construction. An append output is the case where
every key is a batch number, `prior_keys` gives the next one, and batches
are never rewritten: the handle carries the newest batch, `load(ref)`
returns batches `<= ref`'s, a true snapshot at the pinned version, always
readable even after later writes. `TableRef.where` carries the same
filter for raw SQL. Recompute (`prior=None`) truncates the partition. A
sink that cannot delete makes writes idempotent on `(scope, batch)`
instead.

Residual: between a fenced write and its successor's commit, a raw-SQL
reader that ignores the filter can see an uncommitted batch.

## 4. Stores

```python
class Store(Protocol):
    def can_load(self, t: type, selection: type[Keys] | None) -> bool: ...
    def can_store(self, t: type | None, output: Output) -> bool: ...
    async def store(self, write: Any, prior: Ref | None, scope: Scope) -> Written: ...
    async def load(self, ref: Ref, t: type, selection: Keys | None) -> Any: ...

Scope   = (output: Output, partition: str, prior_keys: Mapping[str, str] | None)
Written = (ref: Ref, keys: Mapping[str, str] | None)
Keys    = (revisions: Mapping[str, str])
```

| Method | Contract |
|---|---|
| `can_load(t, selection)` | Registration. Can you produce `t`, filtered by `Keys` when `selection` is given? `can_load(R, None)` for a `Ref` subclass `R` means "are your refs `R`". |
| `can_store(t, output)` | Registration. Can you take values of type `t` for this `Output` declaration, and extract its declared key from them? `t` is `None` when the producer is unannotated. |
| `store(write, prior, scope)` | Apply the write; return the new ref (version per §3) and, when the output declares a key, the scope's **complete** `key → revision` map, merged with `scope.prior_keys` for partial writes. Duplicate keys are a write error. For `partition_column` outputs, stamp the column with `scope.partition` and reject rows that disagree. |
| `load(ref, t, selection)` | Materialize `t`; under `Keys`, only the selected keys, at their pinned revisions where the data model allows. Refs with `meta.external` were not written by the store and skip the marker check. |

### Writes

A bare value is replace. `Patch` is the SDK's partial write, accepted by
`JsonStore` and `PostgresStore` (`can_store`) and by any store that opts
in; it mirrors the commit API's `upsert`/`remove` (§5). The store, not
the engine, turns it into the scope's complete key map:

| Write | Semantics |
|---|---|
| `Patch(rows, remove=())` | after the write, the scope's rows for the keys present in `rows` (read from the declared `key` column) are exactly these; keys in `remove` are gone; every other key is untouched. On a `mode="append"` output the rows are one new batch and `remove` is not allowed. |
| `Sql(stmt)` | PostgresStore only. The output *is* the table `{schema}.{table}` (`table` defaults to the output name, `schema` to `public`): a `SELECT` is materialized into it as a replace; any other statement runs verbatim and must leave that table in place. Returns an ordinary `TableRef`, loadable downstream. Version `H(prior.version ‖ H(stmt))`. |

### Shipped stores

| Store | Accepts | Ref |
|---|---|---|
| `JsonStore` (default, built in) | JSON values: scalars, lists, dicts, and `list[dict]` rows for the row ops | `JsonRef` (sha256 object) |
| `BlobStore` | `bytes`, `Path` | `BlobRef` |
| `PostgresStore` | `DataFrame`, `GeoDataFrame`, `list[dict]`; `Sql` | `TableRef` |

Secrets travel via `env:` indirection in store and resource config, resolved
in the harness. The manifest records each store's name and `Store.version`
(a class attribute, default `"1"`), not its construction config: a DSN or a
grants list is deployment, not definition. Bump `version` when write, load,
key extraction or the version recipe changes; it bumps the project revision
and the interpretation fingerprint (§6) of every asset that reads or writes
through the store. A store must keep resolving handles written by its
earlier versions.

## 5. Inputs

```python
inputs={
    "qaqc_files":  ByKey(batch_size=100),       # incremental edge, same-named output
    "site_health": AllPartitions(),             # collapse upstream-only dimensions
    "feed":        "station_feed",              # rename, same as In("station_feed")
    "x":           ByKey("some_output"),        # rename + incremental
    "matrix":      In(meta={"owner": "lab"}),   # whole value, with edge metadata
}
deps=["usgs_3dep_tiles"]                        # pinned, watched, not loaded
```

Every value is an `In` or a `str` (sugar for `In(output)`). `In(output=None,
*, meta=None)` is the edge base class: `output` defaults to the parameter
name, `meta` is free-form JSON recorded on the edge in the manifest. The
engine knows exactly three edge kinds; user subclasses are rejected.

| Value | Meaning |
|---|---|
| `In(output=None, meta=None)` | whole value (or ref) of the output at its pinned head |
| `ByKey(output=None, batch_size=100, meta=None)` | receive only keys whose revision changed since this consumer last processed them (§6) |
| `AllPartitions(output=None, meta=None)` | receive every partition of the upstream dimensions this asset lacks (§7) |

**By value or by reference.** The annotation decides. `T` loads through the
upstream store (`store.load(ref, T, selection)`); a `Ref` subclass hands
over the pinned ref. Under `AllPartitions`, `dict[str, T]` loads per
partition and `dict[str, TableRef]` hands over refs. A `ByKey` edge cannot
be ref-annotated.

**The parameter is the selection.** Under `ByKey` the value arrives
filtered to the upserted keys; `ctx.changes[name].deleted` carries what a
selection cannot.

**`deps=`** are unbound inputs: planned, pinned into lineage, part of the
interpretation fingerprint (§6), watched by `AutoRefresh`, bound to no
parameter.

### Sources

`Source(name, store=None, key=None, **handle)` declares an output with no
producer. With a store it loads like any input; without one it is a lineage
pointer. At registration it gets a synthesized head `{output, store,
handle: {name, **handle}, version: digest(handle), meta.external: true}`,
so pinning and fingerprinting are uniform.

The **commit API** advances a source without moving data:

```python
client.commit("pmpt_project_matrix", version="2026-09-18T21:57Z")        # unkeyed: new revision
client.commit("sharepoint_files", keys={"f1": "v3", "f2": "v1"})              # keyed: full map
client.commit("sharepoint_files", upsert={"f1": "v4"}, remove=["f0"])         # keyed: patch
client.commit("uploads", upsert=["u-91"], remove=["u-12"])                    # PartitionSet: patch the set
# POST /api/projects/{p}/sources/{name}/commit
```

For a keyed source the server stages the complete map as the key map
(a patch is merged into the current one) and derives `version` from it, so
an identical map is not a change. `upsert` inserts a key or replaces its
revision; `remove` deletes it.

A keyed source, or a `PartitionSet` listed under `sources=`, is consumable
`ByKey` and usable as a partition set (§7) exactly like a keyed output, so a
system that *pushes* can feed the graph directly. Systems that must be *polled* belong in the graph: a cursor asset
(§6) is the platform-native sensor and needs no service outside cursus.

## 6. Incrementality

Two independent mechanisms.

**Cursor.** Per-scope JSON state the producer sets via `Result(cursor=…)`
and receives as `ctx.cursor` next time. Committed atomically with the
outputs, so a rejected commit re-asks the same question. `graph_delta`
stores the Graph delta token.

**`ByKey`.** The upstream output declares its key (§2). Its store returns
the scope's complete `key → revision` map on every write (§4); the harness
stages it as one object, `keys/{sha256}.json`, and records `{"object",
"count"}` in `ref.meta["keys"]`. The engine diffs that map against the consumer's
per-edge key state and hands the harness `Keys(upserted)` plus `deleted`.
Work is batched by `batch_size`: each batch commits with its key state;
`more` re-queues the task; `scope_complete := not more` on the head.

The **interpretation fingerprint** `H(version, store versions of the
asset's input and output stores, run config, refs of non-ByKey inputs and
deps)` is stored per processed key. A key is reprocessed when its revision
or the fingerprint changes, so a `version` bump or a change to any whole
input reprocesses every key. Code changes alone do not: the code hash
bumps the project revision, not the fingerprint.

A head written before the key was declared has no key map: "no keys
known"; the consumer's state starts empty and the next write upserts
everything. A `version` mismatch between committed and declared makes an
incremental attempt fail non-retryably ("recompute required"), or, with
`on_version_change="recompute"`, turns the next attempt of each scope into
a recompute.

## 7. Partitions

A partition declaration is one **dimension** or a dict of named dimensions:

```python
partitions=sites                                   # one dimension
partitions={"site": sites, "day": TimePartitions(start="2024-01-01", every="1d")}
```

| Dimension | Keys | State |
|---|---|---|
| `StaticPartitions([...])` | fixed list | none |
| `TimePartitions(start, every, *, end=None, end_offset=None, timezone="UTC", format=None)` | half-open windows `[start, start+every)` aligned in `timezone`; `every` is a duration (`"15m"`, `"1h"`, `"1d"`, `"1w"`) or a cron expression for calendar slices; the set ends at the newest complete window unless `end`/`end_offset` (a duration) say otherwise; `format` is the key's strftime, defaulted from `every`, required for cron | none |
| a `PartitionSet`, or any keyed output or source | the current key map of that output | its head |

There is no separate partition-set mechanism. A dynamic set is an asset
producing a `PartitionSet` (with its own inputs, automations and placement); an
external set is a `PartitionSet` under `sources=`, patched through the commit
API. Assets bound to a key set pin its ref in lineage. A set change
is not an `OnChange` event: new keys surface through `partitions="missing"`, retired
keys through fan-out exclusion.

`ctx.partition` is the canonical string (`"Richmond"`, or
`"day=2024-01-01,site=Richmond"` sorted by dimension name);
`ctx.partitions` is the dict view.

**Projection rule.** An edge maps partitions by dimension identity (same
declaration, or the same key-set output):

- dimensions on both sides: same key;
- dimensions only on the consumer: broadcast (every consumer key reads the
  same upstream scope);
- dimensions only on the upstream: must be collapsed with
  `AllPartitions()`, which yields `dict[key, T]` over those dimensions,
  resolved to **keys with committed heads at pin time**, never a barrier on
  missing keys.

`ByKey` requires no upstream-only dimensions (a broadcast `ByKey` diffs the
whole map per consumer key).

**Fan-out.** A run's `partitions` (§8) selects keys from the current set:
`"latest"`, `"missing"`, `"all"` or a list. `OnChange` automations default
to the projection rule from the changed upstream scope.
**Retired** keys (absent from the current set) leave fan-out and
`AllPartitions`; their heads and cursors persist read-only.

## 8. Runs

A run is `{targets, partitions, mode, upstream, config, keys}`:

| Field | Meaning |
|---|---|
| `targets` | assets (or outputs) to materialize |
| `partitions` | `[k…]` · `"all"` (current key set) · `"missing"` (no complete head) · `"latest"` (newest window of each time dimension, every key of the others) · default `"latest"` |
| `mode` | `incremental` (default) or `recompute` |
| `upstream` | also plan the upstream closure; default false: **targets only, inputs pinned to current heads**, so a rebuild never re-polls an external system |
| `config` | JSON passed as `ctx.config` |
| `keys` | per-edge override `{"qaqc_files": {"keys": [...]} \| "full"}`: explicit keys merge into key state; `full` treats every key as upserted and state-minus-map as deleted |

**Modes.** `incremental`: the store gets `prior` = head, the cursor is
kept, `ByKey` edges get the diff. `recompute`: no prior, no cursor, key
state cleared, every key upserted; the store makes the output equal to
exactly this write. `keys=full` is not recompute: prior is kept.

**Attempts.** A task is claimed under a per-scope lock with a lease and a
generation. Inputs are resolved to heads when the attempt starts and
validated at commit; the commit also checks the output heads are unchanged
since the claim. A stale generation cannot commit (**fencing**). Outcomes:

| Outcome | Meaning |
|---|---|
| `succeeded` | committed |
| `skipped` | every `ByKey` edge diff was empty and heads are complete: no harness launched, nothing changes |
| `failed` | retryable → `retries=` applies with backoff; non-retryable (store conflict, revision mismatch, recompute required) → task fails |
| `canceled` | run canceled or lease lost; a successor may already own the scope |

A commit installs heads, `input_refs`, the cursor, per-edge key state and a
`changed` list, and pends `OnChange` automations in the same transaction.

## 9. Automations

```python
Automation(name=None, targets=None, trigger=..., enabled=True,
           partitions=None, mode="incremental", upstream=False, config=None, keys=None)
AutoRefresh()                     # Automation(trigger=OnChange()) over inputs + deps
```

A trigger says **when**; the automation says **what run** to submit, in
the run's own vocabulary (§8): `partitions`, `mode`, `upstream`, `config`,
`keys` are passed through unchanged.

| Arg | Standalone (`Project(automations=[...])`) | Attached (`automations=` on an asset) |
|---|---|---|
| `name` | required; key for toggles | derived `{asset}.{trigger}.{index}` |
| `targets` | required: assets, singleton or list | the asset |
| `trigger` | required | required |
| `partitions` | `"latest"` · `"missing"` · `"all"` · `[k…]`; default `"latest"` for `Every`/`Cron`, the projection of the changed scope for `OnChange` | same |
| `enabled` | default `True` | default `True` |

| Trigger | Fires |
|---|---|
| `Every(seconds)` | on an interval, with a floor; a tick is skipped for any scope still running |
| `Cron(expr, timezone="UTC")` | on schedule; same skip rule |
| `OnChange(*outputs)` | when a listed output's head changes; no args = every input and dep of the target. May not name an output of the target itself |

`partitions="missing"` on a schedule is how new keys of a partition set
and failed first runs get picked up without an operator:
`Automation(trigger=Every(60), partitions="missing")`.

Automation runs plan targets only, pinned to current heads; every pinned
input must have a head (sources synthesize theirs). Toggles are keyed by
name; renaming an asset rekeys its attached automations.

## 10. Execution

### Placement

```python
ecs = AWSECS(cluster="lab", region="us-east-1")      # environment, project level
pool = Pool("ingest")

@asset(executor=ecs(cpu=4, memory="30GB"))           # placement, per asset
```

An **environment** is a project-level object describing where attempts can
run and how to reach it; calling it returns a **placement**, the typed
per-asset request. Each kind defines its own placement signature (`AWSECS`:
`cpu`, `memory`, `gpu`, `image`; `Modal`: `gpu`; `Local`: none), so a bad
option fails at construction. The manifest records `{kind, environment,
placement}`; the server rebuilds the placement from a registry of kinds
(built-ins, plus classes declared with `Project(executors=[MyKind])`, which
the server must be able to import since `launch` runs in the engine).
Placements are not part of the interpretation fingerprint. `retries=` and
`timeout=` are engine policy. `ctx.execution` is the placement's serialized
form.

### Executor protocol

A placement is lifecycle only: start the harness somewhere, report when it
stopped. It never reads a spec or a result, and it carries all of its own
configuration, so the methods take none.

```python
Stage     = (attempt: str, objects: str)   # objects = object-store URL incl. namespace
RunHandle = (id: str, meta: Mapping)       # JSON, durable across engine restarts
Exit      = (code: int | None, reason: str | None, meta: Mapping)

class Placement(Protocol):
    async def launch(self, stage: Stage) -> RunHandle: ...
    async def wait(self, run: RunHandle, timeout: float) -> Exit | None: ...
    async def cancel(self, run: RunHandle) -> None: ...
    max_concurrent: int | None                 # per environment
```

| Method | Contract |
|---|---|
| `launch` | Start the harness, handing it the two `stage` strings (container override, argv, function argument); the harness reaches `objects` with the environment's own auth. The spec is already at `specs/{attempt}.json`. Raising = attempt failed, retryable. |
| `wait` | Block at most `timeout`; `None` while running, else `Exit`. Idempotent, safe after termination; a vanished run is `Exit(None, "lost")`. |
| `cancel` | Best-effort, idempotent, never raises for a finished run. |

Object keys are conventional under `objects`: `specs/{attempt}.json`,
`results/{attempt}.json`, `logs/{attempt}/{seq}.jsonl`.

**Engine loop**, per attempt:

```python
await objects.put(f"specs/{attempt}.json", spec)
run = await placement.launch(Stage(attempt, objects_url))
deadline = now() + timeout
while (exit := await placement.wait(run, lease_interval)) is None:
    if now() > deadline:
        await placement.cancel(run); await placement.wait(run, grace)
        return fail("timeout", retryable=True)
    try:
        await attempts.renew(attempt)            # raises LostOwnership when fenced or canceled
    except LostOwnership:
        await placement.cancel(run); await placement.wait(run, grace)
        return
result = await objects.get(f"results/{attempt}.json")
if result is None:
    return fail(f"harness exited without a result: {exit}", retryable=True)
commit_or_fail(result)
```

On restart, `active/` attempts with a handle are resumed at `wait`, not
abandoned. `Local` handles carry `{pid, started_at}` and treat a mismatch
as lost. The engine counts in-flight attempts per environment against
`max_concurrent`.

### Worker protocol

Structure lives in the manifest, state lives in the spec, effects live in
the result. The spec carries only what the harness cannot recompute from
code.

```json
{
  "attempt":   "t1/3",
  "revision":  "9f3c…",
  "asset":     "qaqc_samples",
  "partition": "Richmond",
  "run":       {"id": "r7", "config": {}},
  "cursor":    "token-41",
  "prior":     {"qaqc_samples": Ref},
  "inputs": {
    "qaqc_files":      {"ref": Ref, "changes": {"upserted": {"f1": "v3"}, "deleted": ["f0"]}},
    "site_health":     {"refs": {"Richmond": Ref, "Perth": Ref}},
    "psa_samples":     {"ref": Ref},
    "usgs_3dep_tiles": {"ref": Ref}
  }
}
```

- `inputs` holds every pin by input name, including `deps`; the manifest
  says which bind parameters. `upserted` is the `Keys` selection and
  `ctx.changes[...].upserted` at once.
- `prior` is the committed head per output; the prior key map is
  located by `prior.meta["keys"]`. Recompute is expressed by withholding
  `prior` and `cursor`; there is no `mode` field.
- Store names, output config, annotations, placement and time windows are
  derived from the manifest and the key.

```json
{"attempt": "t1/3", "status": "succeeded", "outputs": {"qaqc_samples": Ref}, "cursor": "token-42"}
{"attempt": "t1/3", "status": "failed",
 "error": {"type": "ValueError", "message": "…", "traceback": "…", "retryable": true}}
```

The result is the attempt's commit request: refs for returned outputs
(omitted = keep prior), `cursor` if set, or an error. `retryable=false` for
store conflicts, revision mismatch and recompute-required. No result means
the harness died. The engine validates the attempt id, that every ref names
a known output and this scope, and that a keyed output carries its
key map, then commits against its own record of the pins.

**Harness** (`python -m cursus_worker run --objects URL --attempt ID`; the
project entrypoint comes from the environment): fetch spec → refuse on
revision mismatch (a failed result, not a crash) → resolve `env:` → load
inputs per annotation → build `ctx` → run the producer → `store()` each
returned output, stage key maps → write the result last, in one PUT.
Logs stream to chunked objects throughout. `manifest` mode runs through
`Local` only, at server start.

### Built-ins

| Kind | `launch` | handle | `wait` | `cancel` |
|---|---|---|---|---|
| `Local()()` | subprocess with an explicit env allow-list | `{pid, started_at}` | polls the process | `SIGTERM`, then `SIGKILL` |
| `AWSECS(cluster, region)(cpu, memory, gpu, image)` | `run_task` with container overrides carrying the stage | `{task_arn}` | describes until `STOPPED`; `Exit.meta.log_url` | `stop_task` |
| `Modal(app)(gpu)` | spawns the harness function | `{call_id}` | polls the call | cancels it |
| `K8sJob(cluster, namespace)(cpu, memory, image)` | creates a job | `{job}` | watches conditions | deletes the job |
| `Pool(name)(cpu, memory, gpu)` | publishes the stage as a claimable task | `{task}` | result appeared, `complete` called, or claim lease expired | marks the task canceled |

`Pool` is the pull path. Workers are external processes:
`POST /api/workers/register` `{pool, cpu, memory, gpu}`;
`POST /api/tasks/claim` returns `{task, stage, lease_seconds}` for a task
whose placement fits; `POST /api/tasks/{id}/renew`; `POST
/api/tasks/{id}/complete`. Expired claims are swept in the eval loop.

## 11. Registration

```python
project = Project(
    assets=[...], sources=[...],
    stores={"postgres": PostgresStore(dsn="env:DATABASE_URL")},
    executors=[...], resources={...}, automations=[...],
)
# or Project.from_package("brimstone.assets", ...)
```

The manifest records assets (`outputs` with `{name, store, key, revision,
mode, config}`, `inputs`, `deps`, `partitions`, `placement`, `retries`,
`timeout`, `version`, code hash, load types via `typing.get_type_hints`),
sources, automations, store names with their `Store.version`, executor
names, and the project revision.

Registration errors:

- an `inputs=` value is not a `str` or one of `In`, `ByKey`, `AllPartitions`,
  or names an unknown output;
- a partition edge violates the projection rule (§7); a `ByKey` edge has
  upstream-only dimensions or is ref-annotated;
- a `ByKey` edge's upstream output declares no key, or its store fails
  `can_load(T, Keys)`;
- a keyed output is `mode="append"`, or its store fails `can_store(T,
  output)`;
- a store-bound input is unannotated, or its store fails `can_load(T,
  selection)`;
- an output's config or return annotation fails `can_store`;
- a partitioned output on a shared-table store lacks `partition_column`;
- `partitions=` names an output with no key;
- `Automation()` has no trigger; a standalone automation has no name or
  targets; automation names collide;
- an `OnChange` names an output of its own target;
- a placement's kind is not registered.

A bare `DataFrame` to a `primary_key` output is replace, not a `Patch`.

## 12. Later and non-goals

**Later** (specified when a workload demands it): `route=` broadcast
optimization for `ByKey`; checks and conditions (`when=`); `OnRunStatus`;
trigger composition; chunked key maps past ~1M keys; console data preview.

**Non-goals:** cycles (DAG only, including self-triggers; loop inside a
producer); dynamic topology (the manifest is static per
revision; key sets cover data-driven cardinality); continuous operators
(bounded runs, micro-batch streams); cross-project edges (`Source` + commit
API is the bridge); imperative per-run control flow.
