# solera — architecture

solera is an asset-first orchestrator. An **asset** is a function that produces
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
    outputs=Output(
        "qaqc_samples", store="postgres", schema="qaqc", primary_key=["sample_id"], partition_column="site"
    ),
    partitions=sites,  # an asset producing a PartitionSet (§7)
    inputs={"qaqc_files": Incremental()},  # an incremental edge (§5)
    automations=AutoRefresh(),  # OnChange over inputs + deps (§9)
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
| `on_version_change` | `"fail"` (attempts fail until an operator runs `full`) or `"full"` (the next attempt of each scope is a full run) | `"fail"` |
| `automations` | attached automations (§9) | `()` |

Resources (`Project(resources={...})`) bind by parameter name. `ctx` is
reserved and optional.

**`Output(name=None, store=None, key=None, revision=None, incremental=None,
migrations=(), **config)`** declares a slot: registry key (defaults to the function name
when the asset has one output), store (default `JsonStore`), and
store-specific config validated by `can_store` at registration.

| Arg | Meaning |
|---|---|
| `key` | Column identifying what was materialized. Declared once, here; consumers never name columns. Independent of `primary_key` (storage identity). |
| `revision` | Column that changes when a key's content changes. Absent: `revision = H(row)`. |
| `incremental` | The output commits in engine-numbered batches: a keyed output's changes land in its key index (object-store-state.md §6), an unkeyed one's batches in its store; `Incremental()` consumers read what arrived after their watermark. `key=` implies it. Default false — a value output is one object per version. |
| `migrations` | Ordered `Migration(name, payload)` list owned by this output. The store applies pending ones before its first write to the output in an attempt (§4). Payload type is store-defined (`can_store`). The applied set travels in the handle (§3) and the declared list is in the fingerprint (§6). |
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
| `cursor` | committed cursor, `None` on first run or `full` |
| `changes[input]` | `Changes(rows, deleted, batches, full, upserted)` for `Incremental` edges (§6) |
| `run_id`, `config` | the run and its config |
| `execution` | resolved placement `{kind, cpu, memory: bytes, gpu}` |
| `log(message, **fields)` | structured log line |
| `load(ref, T)` | load any ref through its store |

## 3. Refs

```python
@dataclass(frozen=True)
class Ref:
    output: str  # output name — lineage
    store: str  # store registry key
    handle: Any  # store-defined coordinates, JSON
    version: str  # deterministic change token
    partition: str  # scope this ref is the head for
    meta: dict  # engine-defined: {"keys": {"object", "count"}, "external": bool}
```

- **JSON only.** Refs live in state, specs and results. Handles are dicts of
  primitives. Typed subclasses (`TableRef`, `BlobRef`, `JsonRef`) are
  conveniences over the same wire form: `TableRef.table`,
  `TableRef.where`, `TableRef.sql()`.
- **Self-contained.** A historical ref resolves without current store
  config: everything `load` needs (table, partition slice, key and revision
  columns, newest batch, the last applied migration as `schema`) is in the
  handle. Credentials never appear; `env:`
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

Replay-stable without hashing tables. Accepted imprecision: a `full` run or
converging incremental writes may give different versions for identical
content (over-eager, never wrong).

### Mutable-store contract

Content-addressed stores get pin→load→commit atomicity for free. A store
with mutable handles must provide it:

**Replace slots** keep a version marker per `(output, partition)`.
`store()` refuses when the live marker ≠ `prior.version` (first write:
marker absent; a `full` run skips the check). `load(ref)` refuses when the live
marker ≠ `ref.version`: a consumer never reads content that is not its
pinned version; the attempt fails and retries. Inherent consequence: a
consumer slower than its producer's interval cannot load a mutable
replace slot. Use an incremental output, a content-addressed store, or a
coarser schedule.

**Keyed outputs** on a mutable store apply partial writes under the same
marker check; a write with no prior (a first write or a `full` run) replaces
the slice. Stores keep no key maps: the engine's key index does (§6). An
unkeyed incremental output is
the case where data is a sequence of engine-numbered batches — `scope.batch`
gives the next one and batches are never rewritten: `load(ref)` returns the
batches `ref` was committed at, a true snapshot at the pinned version, always
readable even after later writes. `TableRef.where` carries the same
filter for raw SQL. A `full` run (`prior=None`) truncates the partition. A
sink that cannot delete makes writes idempotent on `(scope, batch)`
instead.

Residual: between a fenced write and its successor's commit, a raw-SQL
reader that ignores the filter can see an uncommitted batch.

## 4. Stores

```python
class Store(Protocol):
    def can_load(self, t: type, selection: type | None) -> bool: ...
    def can_store(self, t: type | None, output: Output) -> bool: ...
    async def store(self, write: Any, prior: Ref | None, scope: Scope) -> Written: ...
    async def load(self, ref: Ref, t: type, selection: Keys | Batches | None) -> Any: ...
    async def migrate(self, output: Output, migrations: Sequence[Migration]) -> list[str]: ...  # optional

Scope   = (output: Output, partition: str, batch: int | None, baseline: Ref | None)
Written = (ref: Ref, delta: Delta | None)
Delta   = (batch: int, rows: int, upserted: Mapping | None, deleted: tuple, reset: bool)
Keys    = (revisions: Mapping[str, str])
Batches = (lo: int, hi: int)  # load rows of batches in [lo, hi]
```

| Method | Contract |
|---|---|
| `can_load(t, selection)` | Registration. Can you produce `t`, filtered by `Keys` when `selection` is given? `can_load(R, None)` for a `Ref` subclass `R` means "are your refs `R`". |
| `can_store(t, output)` | Registration. Can you take values of type `t` for this `Output` declaration, and extract its declared key from them? `t` is `None` when the producer is unannotated. |
| `store(write, prior, scope)` | Apply the write; return the new ref (version per §3) and, for an incremental output, the `Delta` this write produced — `scope.batch` is the engine-assigned batch number and `scope.baseline` the committed head to diff against (`prior` is withheld on a `full` run, `baseline` is not). Identical content returns the prior ref with no delta. Duplicate keys are a write error. For `partition_column` outputs, stamp the column with `scope.partition` and reject rows that disagree. |
| `load(ref, t, selection)` | Materialize `t`; under `Keys`, only the selected keys, at their pinned revisions where the data model allows; under `Batches`, only batches in the range. Refs with `meta.external` were not written by the store and skip the marker check. |
| `migrate(output, migrations)` | Optional. Apply, in declared order, every migration not yet in the store's own ledger for this output; return the applied names. Must be safe under concurrent attempts of one output (partitions share tables): take a store-level lock and re-read the ledger inside it. Where the backend is transactional, a migration and its ledger row commit together. A store without `migrate` rejects `migrations=` at registration. |

### Migrations

`Migration(name, payload)` is schema, not data: DDL for a table store, a
callable over its prefix for a blob store; JsonStore has nothing to
migrate and rejects the argument. The ledger of applied names lives next
to the data (`solera_migrations(output, name, at)` in Postgres), never in
engine state, so the store is the only source of truth about its own
shape. The harness calls `migrate` before the first `store()` to an output
in an attempt, so a write can never precede its own migration; `solera
migrate [OUTPUT…]` applies eagerly through a `Local` harness for deploys
that should fail fast. A migration that needs data from other outputs is
an asset with inputs, not a migration.

### Writes

A bare value is replace. `Patch` is the SDK's partial write, accepted by
`JsonStore` and `PostgresStore` (`can_store`) and by any store that opts
in; it mirrors the commit API's `upsert`/`remove` (§5). The store, not
the engine, computes the resulting delta:

| Write | Semantics |
|---|---|
| `Patch(rows, remove=())` | after the write, the scope's rows for the keys present in `rows` (read from the declared `key` column) are exactly these; keys in `remove` are gone; every other key is untouched. On an unkeyed incremental output the rows are one new batch and `remove` is not allowed. |
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
inputs = {
    "qaqc_files": Incremental(batch_size=100),  # incremental edge, same-named output
    "site_health": AllPartitions(),  # collapse upstream-only dimensions
    "feed": "station_feed",  # rename, same as In("station_feed")
    "x": Incremental("some_output"),  # rename + incremental
    "matrix": In(meta={"owner": "lab"}),  # whole value, with edge metadata
}
deps = ["usgs_3dep_tiles"]  # pinned, watched, not loaded
```

Every value is an `In` or a `str` (sugar for `In(output)`). `In(output=None,
*, meta=None)` is the edge base class: `output` defaults to the parameter
name, `meta` is free-form JSON recorded on the edge in the manifest. The
engine knows exactly three edge kinds; user subclasses are rejected.

| Value | Meaning |
|---|---|
| `In(output=None, meta=None)` | whole value (or ref) of the output at its pinned head |
| `Incremental(output=None, batch_size=100, meta=None)` | receive only what changed since this consumer's watermark — upserted/deleted keys on a keyed upstream, new batches on an unkeyed one (§6) |
| `AllPartitions(output=None, meta=None)` | receive every partition of the upstream dimensions this asset lacks (§7) |

**By value or by reference.** The annotation decides. `T` loads through the
upstream store (`store.load(ref, T, selection)`); a `Ref` subclass hands
over the pinned ref. Under `AllPartitions`, `dict[str, T]` loads per
partition and `dict[str, TableRef]` hands over refs. An `Incremental` edge cannot
be ref-annotated.

**The parameter is the selection.** Under `Incremental` the value arrives
filtered to the delivered keys or batches; `ctx.changes[name]` carries the
rest — `deleted` keys, the `batches` range, `full` on a reset delivery.

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
client.commit("pmpt_project_matrix", version="2026-09-18T21:57Z")  # unkeyed: new revision
client.commit("sharepoint_files", keys={"f1": "v3", "f2": "v1"})  # keyed: full map
client.commit("sharepoint_files", upsert={"f1": "v4"}, remove=["f0"])  # keyed: patch
client.commit("uploads", upsert=["u-91"], remove=["u-12"])  # PartitionSet: patch the set
# POST /api/projects/{p}/sources/{name}/commit
```

For a keyed source the server applies the commit as one delta batch against
the current key map and derives `version` from the result, so an identical
map is not a change. `upsert` inserts a key or replaces its
revision; `remove` deletes it.

A keyed source, or a `PartitionSet` listed under `sources=`, is consumable
via `Incremental` and usable as a partition set (§7) exactly like a keyed
output, so a system that *pushes* can feed the graph directly. Systems that must be *polled* belong in the graph: a cursor asset
(§6) is the platform-native sensor and needs no service outside solera.

## 6. Incrementality

Two independent mechanisms.

**Cursor.** Per-scope JSON state the producer sets via `Result(cursor=…)`
and receives as `ctx.cursor` next time. Committed atomically with the
outputs, so a rejected commit re-asks the same question. `graph_delta`
stores the Graph delta token.

**`Incremental`.** Every commit that changes an incremental output gets
the next batch number (`head.batch`). A keyed output (or keyed source) has
a **key index** — an engine-owned log-structured merge tree of `(key,
version)` files (object-store-state.md §6): the harness compares each write
with it, skips the store entirely when nothing changed, and otherwise writes
the changed entries as the batch's delta file. An unkeyed output's batches
are its store's; `head.base` is the first batch after its last reset. The
engine keeps a per-edge **watermark** `{batch, until?, after, full,
fingerprint, output, up}` — the consumer's position. For a keyed upstream
the spec pins the index and a window — the delta log from `batch` to the
head, or the whole index for a full delivery — and the harness reads one
page of it (`batch_size` keys), loads those keys with `Keys(…)`, and reports
where the page ended (`after`); for an unkeyed one the engine plans a
`Batches(lo, hi)` range. Each page commits with its watermark update;
`more` re-queues the task; `complete := not more` on the head.

The **interpretation fingerprint** `H(version, store versions of the
asset's input and output stores, migration names of the asset's outputs,
run config, refs of non-incremental inputs and deps)` is stored on the
watermark. A fingerprint mismatch — a `version` bump, a new migration, or a
change to any whole input — forces `full=True` on the edge: the delivery
resets to the whole head. Code changes alone do not: the code hash
bumps the project revision, not the fingerprint.

A head written before the output was incremental has no delta log: "no keys
known"; the consumer's watermark starts empty and the next write upserts
everything. A `version` mismatch between committed and declared makes an
incremental attempt fail non-retryably, or, with
`on_version_change="full"`, turns the next attempt of each scope into
a full run.

## 7. Partitions

A partition declaration is one **dimension** or a dict of named dimensions:

```python
partitions = sites  # one dimension
partitions = {"site": sites, "day": TimePartitions(start="2024-01-01", every="1d")}
```

| Dimension | Keys | State |
|---|---|---|
| `StaticPartitions([...])` | fixed list | none |
| `TimePartitions(start, every, *, end=None, end_offset=None, timezone="UTC", format=None)` | half-open windows `[start, start+every)` aligned in `timezone`; `every` is a duration (`"15m"`, `"1h"`, `"1d"`, `"1w"`) or a cron expression for calendar slices; the set ends at the newest complete window unless `end`/`end_offset` (a duration) say otherwise; `format` is the key's strftime, defaulted from `every`, required for cron | none |
| a `PartitionSet`, or any keyed output or source | `meta.partitions` on the head ref (a partition set), else the fold of its delta log | its head |

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

`Incremental` requires no upstream-only dimensions (a broadcast `Incremental`
diffs the same delta log per consumer key).

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
| `mode` | `incremental` (default) or `full` |
| `upstream` | also plan the upstream closure; default false: **targets only, inputs pinned to current heads**, so a rebuild never re-polls an external system |
| `config` | JSON passed as `ctx.config` |
| `keys` | per-edge override `{"qaqc_files": {"keys": [...]} \| "full"}`: explicit keys are delivered as that edge's selection; `full` resets the edge — the whole head as a reset delivery |

**Modes.** `incremental`: the store gets `prior` = head, the cursor is
kept, `Incremental` edges get the watermark diff. `full`: no prior, no
cursor, every incremental edge resets to the whole head and its watermark
lands past the head batch; the store makes the output equal to
exactly this write. `keys=full` resets one edge only: `prior` is kept.

**Attempts.** A task is claimed under a per-scope lock with a lease and a
generation. Inputs are resolved to heads when the attempt starts and
validated at commit; the commit also checks the output heads are unchanged
since the claim. A stale generation cannot commit (**fencing**). Outcomes:

| Outcome | Meaning |
|---|---|
| `succeeded` | committed |
| `skipped` | every `Incremental` edge was already at its head (empty diff) and heads are complete: no harness launched, nothing changes |
| `failed` | retryable → `retries=` applies with backoff; non-retryable (store conflict, revision mismatch, version-mismatch without `on_version_change="full"`) → task fails |
| `canceled` | run canceled or lease lost; a successor may already own the scope |

A commit installs heads, `input_refs`, the cursor, per-edge watermarks and a
`changed` list, and pends `OnChange` automations in the same transaction.
Every terminal task outcome also records `{last_outcome, last_attempt, at}`
on the `(asset, scope)` record, and queued or running tasks are indexed per
scope. Views such as the partition grid read those two things; nothing
scans task history.

**Retention.** `@asset(retention=Retention(days=…, runs=…))` bounds an
asset's history; `Project(retention=…)` sets the default and
`Retention(forever=True)` opts out of it (object-store-state.md §11). Current
state — heads, key indexes, cursors, watermarks — never depends on runs and
never expires. Every `retention_interval` (60 s) the engine deletes finished
runs (`runs/{run}/`: the run record, attempt files and logs) that every asset
they ran has let go of; only runs in progress are protected. Data expires in
the harness: an attempt of an asset with a finite policy carries the horizon
in its spec, and before writing an output the harness calls
`store.expire(head, before)` with the committed head, which stays loadable.
`solera runs delete RUN` and `solera runs prune [--before] [--asset] [--keep]
[--dry-run]` (and `DELETE /runs/{run}`, `POST /runs:prune`) delete runs by hand.
Postgres has no `expire`: bound an append-only table with a scheduled job
that deletes old rows (object-store-state.md §11); a keyed table removes
keys through its own asset, so its key index and consumers see it.

## 9. Automations

```python
Automation(
    name=None,
    targets=None,
    trigger=...,
    enabled=True,
    partitions=None,
    mode="incremental",
    upstream=False,
    config=None,
    keys=None,
)
AutoRefresh()  # Automation(trigger=OnChange()) over inputs + deps
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
| `OnDeploy()` | once per new project revision (§11), for the latest revision only: the automation records the revision it last fired for, so a restart on the same revision is silent and back-to-back deploys fire once. Default `partitions="latest"` |

`partitions="missing"` on a schedule is how new keys of a partition set
and failed first runs get picked up without an operator:
`Automation(trigger=Every(60), partitions="missing")`. `OnDeploy()` on an
asset whose outputs declare migrations applies them as part of the deploy;
on a job it is a post-deploy hook.

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
| `launch` | Start the harness, handing it the three `stage` strings — `attempt`, `run`, `objects` — (container override, argv, function argument); the harness reaches `objects` with the environment's own auth. The spec is already in the attempt file. Raising = attempt failed, retryable. |
| `wait` | Block at most `timeout`; `None` while running, else `Exit`. Idempotent, safe after termination; a vanished run is `Exit(None, "lost")`. |
| `cancel` | Best-effort, idempotent, never raises for a finished run. |

Object keys are conventional under `objects`: the attempt file
`runs/{run}/{attempt}.json` — the spec, then spec + result + log index —
and its gzip log `runs/{run}/{attempt}.log` (`.log.{n}` chunks while it runs);
see object-store-state.md §8.

**Engine loop**, per attempt:

```python
await objects.create(f"runs/{run_id}/{attempt}.json", {"spec": spec})
run = await placement.launch(Stage(attempt, run_id, objects_url))
deadline = now() + timeout
while (exit := await placement.wait(run, lease_interval)) is None:
    if now() > deadline:
        await placement.cancel(run)
        await placement.wait(run, grace)
        return fail("timeout", retryable=True)
    try:
        await attempts.renew(attempt)  # raises LostOwnership when fenced or canceled
    except LostOwnership:
        await placement.cancel(run)
        await placement.wait(run, grace)
        return
result = (await objects.get(f"runs/{run_id}/{attempt}.json")).get("result")
if result is None:
    return fail(f"harness exited without a result: {exit}", retryable=True)
commit_or_fail(result)
```

On restart every lease is expired: in-flight attempts are fenced and their
tasks requeue and relaunch from scratch — nothing resumes at `wait`. The
engine counts in-flight attempts per environment against `max_concurrent`.

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
  "outputs":   {"qaqc_samples": {"exists": true, "head": Ref, "batch": 7, "index": KeyIndex}},
  "retention": {"before": 1790000000.0},
  "inputs": {
    "qaqc_files":      {"ref": Ref, "index": KeyIndex,
                        "changes": {"from": 12, "to": 14, "after": null, "full": false, "limit": 100}},
    "site_events":     {"ref": Ref, "changes": {"batches": [4, 6], "full": false}},
    "site_health":     {"refs": {"Richmond": Ref, "Perth": Ref}},
    "psa_samples":     {"ref": Ref},
    "usgs_3dep_tiles": {"ref": Ref}
  }
}
```

- `inputs` holds every pin by input name, including `deps`; the manifest
  says which bind parameters. `changes` is what to deliver — for a keyed
  upstream a window of its pinned key index (the delta log `from`–`to`, or
  the whole index when `full`), read `limit` keys at a time from `after`; for
  an unkeyed one the `[lo, hi]` `Batches` range; a run's `keys=` override
  names its keys outright. `full` marks a reset delivery.
- `prior` is the committed head per output; `outputs` pins each output's
  committed head, its engine-assigned batch number and, when keyed, its key
  index. A `full` run is expressed by withholding `prior` and `cursor`
  (`outputs` stays); there is no `mode` field. `retention.before` is the
  horizon for `store.expire` (§8 Retention).
- Store names, output config, annotations, placement and time windows are
  derived from the manifest and the key.

```json
{"status": "succeeded",
 "outputs": {"qaqc_samples": {"ref": Ref, "keys": {"files": [FileInfo], "added": 1, "removed": 0, "exact": true}}},
 "delivered": {"qaqc_files": {"after": null, "upserted": ["f1"], "deleted": ["f0"]}},
 "cursor": "token-42"}
{"status": "failed",
 "error": {"type": "ValueError", "message": "…", "traceback": "…", "retryable": true}}
```

The result is the attempt's commit request: per returned output its ref
(or `unchanged`), a keyed output's delta files (`keys`) and a partition
set's `elements`; per keyed Incremental input the page it `delivered`;
`cursor` if set; or an error. `retryable=false` for store conflicts,
revision mismatch and version-mismatch without a full run. No result means
the harness died. The engine validates the attempt id, that every ref names
a known output and this scope, and that a keyed output reports its delta,
then commits against its own record of the pins. Inputs that moved since
they were pinned do not void the commit: the attempt delivered the window
it was given.

**Harness** (`python -m solera_worker run --objects URL --attempt ID`; the
project entrypoint comes from the environment): fetch spec → refuse on
revision mismatch (a failed result, not a crash) → resolve `env:` → load
inputs per annotation (keyed Incremental edges through the upstream key
index) → build `ctx` → run the producer → for each returned output, compare
the write with its key index; `store()` it unless nothing changed and write
the delta file → write the result last, in one PUT.
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
    assets=[...],
    sources=[...],
    stores={"postgres": PostgresStore(dsn="env:DATABASE_URL")},
    executors=[...],
    resources={...},
    automations=[...],
)
# or Project.from_package("brimstone.assets", ...)
```

The manifest records assets (`outputs` with `{name, store, key, revision,
incremental, migrations, config}`, `inputs`, `deps`, `partitions`, `placement`, `retries`,
`timeout`, `version`, code hash, load types via `typing.get_type_hints`),
sources, automations, store names with their `Store.version`, executor
names, and the project revision.

Registration errors:

- an `inputs=` value is not a `str` or one of `In`, `Incremental`, `AllPartitions`,
  or names an unknown output;
- a partition edge violates the projection rule (§7); an `Incremental` edge has
  upstream-only dimensions or is ref-annotated;
- an `Incremental` edge's upstream output is not incremental, or its store fails
  `can_load(T, selection)`;
- an output's store fails `can_store(T, output)`;
- a store-bound input is unannotated, or its store fails `can_load(T,
  selection)`;
- an output's config or return annotation fails `can_store`;
- a partitioned output on a shared-table store lacks `partition_column`;
- `partitions=` names an output with no key;
- `Automation()` has no trigger; a standalone automation has no name or
  targets; automation names collide;
- an `OnChange` names an output of its own target;
- an output declares `migrations=` on a store without `migrate`, a
  migration name repeats, or a payload fails `can_store`;
- a placement's kind is not registered.

A bare `DataFrame` to a `primary_key` output is replace, not a `Patch`.

## 12. Later and non-goals

**Later** (specified when a workload demands it): `route=` broadcast
optimization for `Incremental`; checks and conditions (`when=`); `OnRunStatus`;
trigger composition; delta-log compaction for very long histories; console data preview.

**Non-goals:** cycles (DAG only, including self-triggers; loop inside a
producer); dynamic topology (the manifest is static per
revision; key sets cover data-driven cardinality); continuous operators
(bounded runs, micro-batch streams); cross-project edges (`Source` + commit
API is the bridge); imperative per-run control flow.
