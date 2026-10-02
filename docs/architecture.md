# solera — architecture

solera is an asset-first orchestrator. An **asset** is a function that produces
**outputs**. A **store** writes and reads them. A **commit** records which
outputs changed. Everything else — partitions, incrementality, automations,
placement — is a small amount of engine state around those three things.

This document is normative for the model; the designs it points to are
normative for their parts: `object-store-state.md` (engine state, the key
index, attempt files, run history, retention), `lifecycle.md` (attempts,
the worker channel, store kinds, sensors), `per-key-processing.md` (`Each`,
error classes, build identity), `resolved-commits.md` (the engine's
resolver), `key-index-format.md` and `row-digest.md` (the byte formats).
`example/brimstone.py` is the reference example. Sections: 1 Model · 2 Assets
· 3 Refs · 4 Stores · 5 Inputs · 6 Incrementality · 7 Partitions · 8 Runs ·
9 Automations · 10 Execution · 11 Registration · 12 Later and non-goals.

## 1. Model

Three rules:

1. **The engine is control-plane only.** It records, for each `(output,
   partition)`, the committed ref and its metadata, plus cursors, per-edge
   watermarks and automation state. It never moves, parses or interprets
   payloads. The one value-derived thing it keeps is each keyed output's
   **key index** (§6): `(key, version)` entries in a format it defines,
   computed by the harness.
2. **Stores own data semantics.** Where an output lives, how a write
   applies, how a load materializes, how writes stay correct under replay.
   Core defines no write semantics: a bare return value is a full
   replacement and every store implements it; the harness works out which
   keys the write changed, against the key index, and tells the store (§4). `Patch` is an SDK type for partial writes
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
| **commit** | The atomic transaction installing an attempt's result: heads, lineage, cursor, watermarks, key index deltas. |
| **run** | A request to materialize targets. It plans **tasks**, one per `(asset, scope)`. |
| **attempt** | One execution of a task. |
| **cursor** | Per-scope JSON state the producer sets and receives back (§6). |
| **key index** | The `(key, version)` entries of a keyed output's `(output, scope)`: an engine-owned log-structured index, one delta file per commit that changes it (§6, object-store-state.md §6). |
| **placement** | Where an attempt runs: a typed request built from a named, project-level executor (§10). |

Three processes:

| Process | Runs | Holds |
|---|---|---|
| **server** | engine + API. No user code. | its state, in the object store (object-store-state.md); object store credentials |
| **harness** | user code: producers, stores, resources. Launched per attempt by a placement. | store/resource secrets via `env:`, object store access via the environment's auth |
| **console** | UI over the API. | nothing |

One distribution, split by extras: `solera` is the SDK and the harness
(the worker), `solera[server]` adds the engine, API and run history. The
harness's import graph holds none of the server's libraries, nor pandas or
pyarrow unless a value is of their type.

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
when the asset has one output), store (default: the project's `default_store`, a `FileStore`), and
store-specific config validated by `can_store` at registration.

| Arg | Meaning |
|---|---|
| `keyed` | The output is a `dict[str, Any]`: its keys are the keys, its values the content. Excludes `key` and `revision`. |
| `key` | Column identifying what was materialized. Declared once, here; consumers never name columns. A key holds every row that carries it — one, or the many rows parsed from one file. Independent of `primary_key` (storage identity). |
| `revision` | Column that changes when a key's content changes; a key's rows must share it. Absent: a 16-byte digest of the key's rows (`row-digest.md`). |
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
  primitives. Typed subclasses (`TableRef`, `ObjectRef`) are
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

### What a read sees

A run pins the versions it reads; whether a load returns exactly those
depends on the store's kind (`lifecycle.md` §9.6). There is no setting:
it is what each kind can promise.

- **Snapshot: FileStore and S3Store** (`immutable`). Every object is
  written once under a name no other attempt uses, so a load returns
  exactly the pinned version: a keyed load names its objects from the
  pinned index's `(version, locator)`, a value its head's object, a batch
  range the committed object of each batch. A newer commit does not
  change what a pinned reader sees; superseded objects stay until no
  reader pin predates them.
- **Current data: PostgresStore and every `fenced` store.** One copy per
  row, changed in place: a load returns the rows as they are now. A consumer pinned to
  version 12 that loads after version 13 committed reads version 13's
  rows, so one run can see different outputs at different moments. An
  `Incremental` edge still delivers the keys of its pinned window; a row
  changed since is read in its newer form (and delivered again with the
  window that changed it: a harmless repeat), and a row deleted since may
  be missing. Writers are safe all the same: a fenced store refuses an
  older attempt's writes (`stores.md`).

**Keyed outputs.** Stores keep no key maps: the engine's key index does
(§6). The harness has a keyed write read once — by its store's `prepare`,
or the default — and hands the store a `KeyedWrite`: the keys it changes,
each to the version the index will hold, and the keys it removes, so a
store reads and touches only those; with no prior (a first write or a
`full` run) the write is the slice's whole content. An
unkeyed incremental output is a sequence of engine-numbered batches —
`scope.batch` gives the next one, and a `full` run (`prior=None`) starts
the partition over. A sink that cannot delete makes writes idempotent on
`(scope, batch)` instead.

## 4. Stores

```python
class Store(Protocol):
    def can_load(self, t: type, selection: type | None) -> bool: ...
    def can_store(self, t: type | None, output: Output) -> bool: ...
    async def store(self, write: Any, prior: Ref | None, scope: Scope) -> Written: ...
    async def load(self, ref: Ref, t: type, selection: Keys | Batches | None) -> Any: ...
    async def migrate(self, output: Output, migrations: Sequence[Migration]) -> list[str]: ...  # optional

Scope   = (output: Output, partition: str, batch: int | None, attempt: str | None, aliases: tuple,
           generation: int | None, invocation: str | None)
Prepared   = (output, rows: Rows, take: Callable, patch: bool, removes)  # a keyed write, read once
KeyedWrite = (prepared: Prepared, upserts: Mapping[str, bytes] | DeltaKeys | None,
              removes: frozenset[str], whole: bool, value: Any)  # what a keyed output's store gets
Written = (ref: Ref, keys: Iterable | None)   # keys: only for Sql writes the harness never sees as rows
Keys    = (revisions: Mapping[str, bytes])  # revisions as the key index holds them
Batches = (lo: int, hi: int)  # load rows of batches in [lo, hi]
```

| Method | Contract |
|---|---|
| `can_load(t, selection)` | Registration. Can you produce `t`, filtered by `Keys` when `selection` is given? `can_load(R, None)` for a `Ref` subclass `R` means "are your refs `R`". |
| `can_store(t, output)` | Registration. Can you take values of type `t` for this `Output` declaration, and extract its declared key from them? `t` is `None` when the producer is unannotated. |
| `store(write, prior, scope)` | Apply the write; return the new ref (version per §3). `scope.batch` is the engine-assigned batch number. A keyed output's `write` is a `KeyedWrite`, which a store reads four ways: `whole` (clear the scope first), `removes`, `pages()` — the keys to write a page at a time, each with its version and group, only that page taken from the write (`iter_pages()` for a store writing on a thread of its own) — and `version(prior)`; `value` is what the producer returned. `prior` is withheld on a `full` run. Duplicate keys are a write error. For `partition_column` outputs, stamp the column with `scope.partition` and reject rows that disagree. |
| `load(ref, t, selection)` | Materialize `t` from what the store holds now; under `Keys`, only the selected keys; under `Batches`, only batches in the range. |
| `migrate(output, migrations)` | Optional. Apply, in declared order, every migration not yet in the store's own ledger for this output; return the applied names. Must be safe under concurrent attempts of one output (partitions share tables): take a store-level lock and re-read the ledger inside it. Where the backend is transactional, a migration and its ledger row commit together. A store without `migrate` rejects `migrations=` at registration. |

Every store declares `writes`: `"immutable"`, implementing `discard(scope,
prior, items)`, or `"fenced"`, implementing `acquire(scope)` — how a writer
the engine gave up on is kept from writing over a newer one. `stores.md`
is the contract, with its invariants, recipes per backend and the
scenarios `solera.testing.stores` checks. Optional: `prepare(write, output) -> Prepared` — how a
keyed write of the types the store takes is read: its rows, natively, and
how to take the rows it persists. The framework knows plain Python only
(the default, `solera.stores.prepare`); DataFrames and Arrow are a store's
to read (`solera.stores.frames`), and its `can_store` says what it takes; `stamped(output)` and `scan(ref, output, skip)` —
the columns the store adds to every row, left out of their digests, and
how a `Sql` write's rows are read back (row-digest.md); `shared_table`
— one table for every partition, so a partitioned output needs a
`partition_column` (§3).

### Migrations

`Migration(name, payload)` is schema, not data: DDL for a table store, a
callable over its prefix for an object store. FileStore and S3Store have
no `migrate` and reject the argument. The ledger of applied names lives next
to the data (`solera_migrations(output, name, at)` in Postgres), never in
engine state, so the store is the only source of truth about its own
shape. The harness calls `migrate` before the first `store()` to an output
in an attempt, so a write can never precede its own migration; `solera
migrate [OUTPUT…]` applies eagerly through a `Local` harness for deploys
that should fail fast. A migration that needs data from other outputs is
an asset with inputs, not a migration.

### Writes

A bare value is replace. `Patch` is the SDK's partial write, accepted by
`FileStore`, `S3Store` and `PostgresStore` (`can_store`) and by any store that opts
in; it mirrors the commit API's `upsert`/`remove` (§5). The harness, not
the store, works out what changed, against the key index (§6):

| Write | Semantics |
|---|---|
| `Patch(rows, remove=())` | after the write, the scope's rows for the keys present in `rows` (read from the declared `key` column) are exactly these; keys in `remove` are gone; every other key is untouched. On an unkeyed incremental output the rows are one new batch and `remove` is not allowed. By key, `Patch({key: rows})`: each key's group, its key column stamped by the store; a key given no rows does not exist, and the patch removes it (per-key-processing.md §6). |
| `Sql(stmt)` | PostgresStore only. The output *is* the table `{schema}.{table}` (`table` defaults to the output name, `schema` to `public`): a `SELECT` is materialized into it as a replace; any other statement runs verbatim and must leave that table in place. Returns an ordinary `TableRef`, loadable downstream. Version `H(prior.version ‖ H(stmt))`. |

### Shipped stores

| Store | Accepts | Ref |
|---|---|---|
| `FileStore(path=None)` (default, built in) | anything: JSON when it round-trips, pickle otherwise; keyed rows as plain Python, DataFrames or Arrow. One file per value, partition, key or batch under `.solera/data` next to the project file (or `$SOLERA_DATA`) | `ObjectRef` |
| `S3Store(url, **options)` | the same, in a bucket | `ObjectRef` |
| `PostgresStore` | `list[dict]`, `DataFrame`, `GeoDataFrame`, Arrow; `Sql` | `TableRef` |

Declare PostgresStore's columns (`Output(..., columns={...})`, changed by
migrations). Undeclared, a table a write creates is typed from that
write — a DataFrame's or Arrow table's schema (pyarrow's inference when it
is installed, else pandas' dtypes), else the kind of every value not null
in a column; two kinds, or only nulls, want `columns=`; what it inferred
is logged once — and one a `Sql` SELECT creates by the SELECT's own
types. It stores what was hashed: a value its column would read back as
another type — `42` into a text column — is a write error. Its transactions run on a thread,
off the worker's event loop.

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
    "qaqc_files": Incremental(page_size=100),  # incremental edge, same-named output
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
engine knows exactly three edge kinds; user subclasses are rejected (`Each`
is an `Incremental` edge to the engine, with a failure index).


| Value | Meaning |
|---|---|
| `In(output=None, meta=None)` | whole value (or ref) of the output at its pinned head |
| `Incremental(output=None, page_size=100, meta=None, *, include=None, exclude=None)` | receive only what changed since this consumer's watermark — upserted/deleted keys on a keyed upstream, new batches on an unkeyed one (§6). On a keyed upstream, `include`/`exclude` globs (or `Regex`) select keys by name; pages are formed from the keys they take — read ahead past the others — so no page is empty, a delivery they take nothing from is `skipped` without calling the producer, and a change of patterns cuts over: pending changes finish under the old ones, membership is diffed against the index at the cutover (pinned until the diff ends), then deltas continue under the new (per-key-processing.md §11) |
| `Each(output=None, *, page_size=100, concurrency=16, meta=None)` | an `Incremental` edge on a keyed upstream whose producer is written for **one key**: the parameter is that key's value (a rows upstream: its group), `ctx.key` its key. The worker calls it for every changed key of a page, `concurrency` at a time, stores the keys that succeeded as one `Patch({key: value})` per output, and keeps the ones that raised in the asset's failure index, retried by their error class; deleted keys lose their rows without a call. One per asset, its other inputs whole, every output keyed. per-key-processing.md §5–§10 |
| `AllPartitions(output=None, meta=None)` | receive every partition of the upstream dimensions this asset lacks (§7) |

**By value or by reference.** The annotation decides. `T` loads through the
upstream store (`store.load(ref, T, selection)`); a `Ref` subclass hands
over the pinned ref. Under `AllPartitions`, `dict[str, T]` loads per
partition and `dict[str, TableRef]` hands over refs. An `Incremental` edge cannot
be ref-annotated.

**The parameter is the selection.** Under `Incremental` the value arrives
filtered to the delivered keys or batches; `ctx.changes[name]` carries the
rest. A delivery comes in **pages** of `page_size` keys (or upstream
batches); "batch" always means an upstream commit. Top-level fields
describe the delivery: `upserted` and `deleted` keys, `full` on every page
of a full delivery (the whole head, after a reset), `page` — the page's
0-based index, exact — `pages`, how many pages the delivery was planned to
take when it started (exact without patterns and with an exact key count,
else an estimate), `first` (`page == 0`), and `final`, set when the
delivery has run out, never inferred from `pages`. Pages hold only keys the
edge's patterns take, read ahead past the others, so `final` is always on
a real page. `upstream` carries facts about the upstream: its `output`,
and for a batch-mode upstream the range of `batches` the page covers. The
page plan is kept on the edge's watermark while the delivery continues, for
keyed and unkeyed upstreams, delta windows and full deliveries alike. A
consumer that rebuilds starts over when `full and first` — never on `full`
alone, or each page would erase the ones before it.

**`deps=`** are unbound inputs: planned, pinned into lineage, part of the
interpretation fingerprint (§6), watched by `AutoRefresh`, bound to no
parameter. A dep across upstream-only dimensions pins the heads that
exist, like `AllPartitions` (§7).

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
the source's key index and derives `version` from the result, so an identical
map is not a change. `upsert` inserts a key or replaces its
revision; `remove` deletes it.

A keyed source, or a `PartitionSet` listed under `sources=`, is consumable
via `Incremental` and usable as a partition set (§7) exactly like a keyed
output, so a system that *pushes* can feed the graph directly.

A system that must be *polled* gets a **sensor** (`lifecycle.md` §11): a
check run every interval on a long-lived sensor host, which may commit to
the sources it declares and request runs, all or nothing, and keeps a
cursor. Ticks that find nothing record nothing.

```python
@sensor(every=60, commits=["uploads"], executor=Pool("sensors"))  # default: the engine's own host
def new_uploads(ctx, s3: S3Client) -> Tick | None:
    page = s3.list_since(ctx.cursor)
    return Tick(cursor=page.token, commits=[Commit("uploads", upsert={o.key: o.etag for o in page.objects})],
                runs=[RunRequest(["ingest"])])

class Landing(Source):  # an observable source: sugar for a sensor `landing.observe`
    def observe(self, ctx, s3: S3Client) -> dict[str, str]:
        return {o.key: o.etag for o in s3.list(self.handle["bucket"])}

Project(sources=[Landing("landing", key="id", observe=Every(300), bucket="in")], sensors=[new_uploads])
```

`observe()` returns a version (`str`), a full key map, `Observed(upsert,
remove, cursor)`, or `None`. A cursor asset (§6) still suits a poll whose
result is itself data.

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
engine keeps a per-edge **watermark** — the consumer's position: `next`,
the first upstream batch not yet delivered, and while a delivery is under
way, `delivery` `{mode, from, to, at, page, pages}`: `full` or `delta`, its
boundary, and its position (the last key delivered, or the next batch),
all decided when it starts and kept until its last page. For a keyed
upstream the spec pins the index and a window — the delta log from `next`
to the head, or the whole index for a full delivery — and the harness reads
one page of it (`page_size` keys), loads those keys with `Keys(…)`, and
reports where the page ended (`after`); for an unkeyed one the engine plans
a `Batches(lo, hi)` range. Each page's commit advances the watermark by
what it delivered (`delivery.advance`); `more` re-queues the task. Whether the delivery drained is the scope's
(`drained := not more` on its progress), not its outputs': a last page may
write none of them, and the scope is complete all the same. A scope is
**complete** when each of its outputs has a head and its delivery drained —
a job, once a run of it succeeded. Selection (`"missing"`), `AllPartitions`
and the console all ask that one question.

The **interpretation fingerprint** `H(version, store versions of the
asset's input and output stores, migration names of the asset's outputs,
run config, refs of non-incremental inputs and deps)` is stored on the
watermark. A fingerprint mismatch — a `version` bump, a new migration, or a
change to any whole input — forces `full=True` on the edge: the delivery
resets to the whole head. Code changes alone do not: the build identity
(§11) bumps the project revision, not the fingerprint.

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

A **dep** across upstream-only dimensions needs no `AllPartitions`: it
collapses the same way, pinning every head that exists and agrees with the
consumer's shared keys.

```python
@asset(partitions={"day": daily, "site": "sites"})
def readings(ctx): ...

@asset(partitions={"day": daily}, deps=["readings"])
def report(ctx): ...
```

`report` for `2026-10-01` pins whichever `readings` of that day have
committed heads — say Richmond and Oslo, while Paris has none yet — and
runs. It does not wait for Paris. To wait for every site, submit with
`upstream=True`: the run builds every `readings` scope of the day, and
`report` starts only once they all succeed (a failure blocks it). Only an
upstream build lists the domain, refused past `MAX_SCOPES`.
`skip_missing_inputs` (§9) waits only for the first: a fan-in that finds no
upstream head at all — no `readings` for the day, or for `AllPartitions` no
complete one — counts as missing, so `report` is skipped until one exists.
Without the flag it runs over nothing.

`Incremental` requires no upstream-only dimensions (a broadcast `Incremental`
diffs the same delta log per consumer key).

**Fan-out.** A run's `partitions` (§8) selects keys from the current set:
`"latest"`, `"missing"`, `"all"` or a list. `OnChange` automations default
to the projection rule from the changed upstream scope.
**Retired** keys (absent from the current set) leave fan-out and every
fan-in — `AllPartitions`, a collapsing dep, the missing-input check; their
heads and cursors persist read-only.

## 8. Runs

A run is `{targets, partitions, mode, upstream, config, keys}`:

| Field | Meaning |
|---|---|
| `targets` | assets (or outputs) to materialize |
| `partitions` | `[k…]` · `"all"` (current key set) · `"missing"` (not complete, §6) · `"latest"` (newest window of each time dimension, every key of the others) · default `"latest"`. Explicit keys are checked part by part, so they never depend on the size of the domain; any other selection is counted before it is listed — `"latest"` across every non-time dimension — and past 100,000 scopes the request is refused, never truncated. So is a run past 100,000 tasks once `upstream=True` adds its upstream work |
| `mode` | `incremental` (default) or `full` |
| `upstream` | also plan the upstream closure; default false: **targets only, inputs pinned to current heads**, so a rebuild never re-polls an external system |
| `config` | JSON passed as `ctx.config` |
| `keys` | per-edge override `{"qaqc_files": {"keys": [...]} \| "full"}`: explicit keys are delivered as that edge's selection; `full` resets the edge — the whole head as a reset delivery |

**Modes.** `incremental`: the store gets `prior` = head, the cursor is
kept, `Incremental` edges get the watermark diff. `full`: no prior, no
cursor, every incremental edge resets to the whole head and its watermark
lands past the head batch; the store makes the output equal to
exactly this write. `keys=full` resets one edge only: `prior` is kept.

**Attempts.** A task is claimed under a per-scope lock, so one attempt at a
time writes an (asset, scope). Inputs are resolved to heads when the attempt
starts. Once launched (`AttemptLaunched`), the claim is durable: a
restarted engine adopts the attempt rather than launching it again. Before
its first store write the harness takes the attempt's write fence; the
engine takes the same fence before it cancels, times out or fails the
attempt, so exactly one side wins (object-store-state.md §8). Outcomes:

| Outcome | Meaning |
|---|---|
| `succeeded` | committed |
| `skipped` | every `Incremental` edge was already at its head (empty diff) and the scope is complete: no harness launched, nothing changes |
| `failed` | retryable → `retries=` applies with backoff; non-retryable (revision mismatch, version-mismatch without `on_version_change="full"`) → task fails |
| `canceled` | run canceled before the attempt began writing; an attempt already writing is committed instead |

**Errors in user code** are classified by the class they subclass
(`solera.Rejected`, `Failed`, `Transient`, `Abort`), or by
`Project(errors={ExceptionType: Class})` for types users cannot subclass;
the first match along the error's method resolution order wins, and
anything else is `Failed` (per-key-processing.md §8). For an attempt:
`Rejected` fails the task without retries; `Failed` and `Abort` follow
`retries=`; `Transient(retry_after=, retry_for=)` is retried after
`retry_after`, else one minute doubling to six hours, past `retries=`,
until `retry_for` (24 h by default) has passed since its first failure.

A commit installs heads, `input_refs`, the cursor, per-edge watermarks and a
`changed` list, and pends `OnChange` automations in the same transaction.
Every terminal task outcome also records `{last_outcome, last_attempt, at}`
on the `(asset, scope)` record, and queued or running tasks are indexed per
scope. Views such as the partition grid read those two things; nothing
scans task history.

**History.** Everything that finishes — runs, tasks, attempts, output
versions with their metadata, and the input versions each was built from —
lands in the run history: Parquet tables under `history/`, queried with an
embedded DuckDB (object-store-state.md §7). It serves run listings with
filters, facets and a time histogram, operations stats (p50/p95 duration
and queue wait, failure rates, compute hours per executor), an asset's
version timeline, and lineage in both directions. Runs carry tags
(`solera run --tag env=prod`, `"tags"` in the API, `Automation(tags=…)`);
assets carry tags too (`@asset(tags=…)`); an attempt records per-version
metadata with `ctx.metadata(rows=…, auc=…)` or `Result(metadata=…)`. Each
run also keeps a timeline — `run_events`: submitted, held and why, claimed,
launched, booted, loaded, computing, `ctx.mark("joined")`, stored,
committed… — from which each attempt's phases (provisioning, loading,
computing, writing, …) and each task's wait are computed; pauses and
engine outages don't count as wait.

**Retention.** `@asset(retention=Retention(days=…, runs=…))` bounds an
asset's history; `Project(retention=…)` sets the default and
`Retention(forever=True)` opts out of it (object-store-state.md §11). Current
state — heads, key indexes, cursors, watermarks — never depends on runs and
never expires. Every `retention_interval` (60 s) the engine deletes finished
runs — their attempt files and logs under `runs/{run}/`, and their history
rows — that every asset they ran has let go of; only runs in progress are protected. Data never
expires: stores hold current content only.
`solera runs delete RUN` and `solera runs prune [--before] [--asset] [--keep]
[--dry-run]` (and `DELETE /runs/{run}`, `POST /runs:prune`) delete runs by hand.
Bound an append-only output (FileStore batches, a Postgres event table) with a scheduled job
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
    tags=None,
    skip_missing_inputs=False,
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
| `partitions` | `"latest"` · `"missing"` · `"all"` · `[k…]`; default `"latest"` for `Every`/`Cron`, the projection of the changed scope for `OnChange` — a source has no dimensions, so its change reaches every scope of the target (bounded like `"all"`). Named partitions run as named, whatever changed. A firing is one run over every target, so a target that reads another waits for it. A change stays pending — never consumed — while a scope it is owed is claimed or queued in any run (that work would not see it, and the firing could not order after it), and while a target reading the change through `AllPartitions` cannot see it yet: a delivery under way is read once it completes | same |
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
input must have a head (sources synthesize theirs). With
`skip_missing_inputs=True`, a scope reading an input that was never written
(and that the run doesn't build, with `upstream=True`) is left out rather
than run to fail, and a tick left with nothing is skipped: a weekly digest
over an index nobody has built yet waits for it. A fan-in (§7) is missing
when no upstream head agrees with the scope at all; one is enough to run. Toggles are keyed by
name; renaming an asset rekeys its attached automations.

## 10. Execution

### Placement

```python
etl = AWSECS("etl", cluster="lab", region="us-east-1")   # executor, project level
ingest = Pool("ingest")

@asset(executor=etl(cpu=4, memory="30GB"))                # placement, per asset
```

An **executor** is a named, project-level environment: where attempts can
run and how to reach it. The name is what the engine, the console and the
history call it — `etl`, not `AWSECS(lab/us-east-1)` — and one name means
one kind and environment: declaring `etl` twice, differently, fails
registration. `Local()` is always `local`; a `Pool`'s name is its pool's.
Calling an executor returns a **placement**, the typed per-asset request.
Each kind defines its own placement signature (`AWSECS`: `cpu`, `memory`,
`gpu`, `image`; `Modal`: `gpu`; `Local`: none), so a bad option fails at
construction. The manifest records each placement as `{executor, kind,
environment, placement}` and the executors as `{name: {kind,
environment}}`; the server rebuilds placements from a registry of kinds
(built-ins, plus custom kinds whose executors are declared with
`Project(executors=[MyKind("name", ...)])`, which the server must be able
to import since `launch` runs in the engine). Each launched attempt records
where it ran and what it asked for (object-store-state.md §7).
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
    resume: Callable[[Stage], Awaitable[RunHandle]]   # optional: `launch`, where it is idempotent
    max_concurrent: int | None                 # per executor
    provision_seconds: float | None            # optional: the engine's default, None for no deadline
```

| Method | Contract |
|---|---|
| `launch` | Start the harness, handing it the three `stage` strings — `attempt`, `run`, `objects` — (container override, argv, function argument); the harness reaches `objects` with the environment's own auth. The spec is already in `.spec`. Where the provider allows it, the run is named after the attempt, so launching twice starts it once. Raising = attempt failed, retryable. The handle is recorded (`AttemptPlaced`). |
| `resume` | Optional, for an attempt adopted without a handle: find its run, or start it if the launch never happened — `launch` itself, when that is idempotent. |
| `wait` | Block at most `timeout`; `None` while running, else `Exit`. Idempotent, safe after termination; a run the provider says is gone is `Exit(None, "lost")`. Raises when it cannot tell — an API error, a run not shown yet: the engine keeps the handle and asks again. |
| `cancel` | Best-effort, idempotent, never raises for a finished run. |

Object keys are conventional under `objects`: `runs/{run}/{attempt}.spec`,
the claim `.worker`, the gate `.writing`, log chunks `.log.{n:06d}` and the
result `.result`; see object-store-state.md §8 and `lifecycle.md`.

**Engine loop**, per attempt:

```python
await objects.create(f"runs/{run_id}/{attempt}.spec", spec)
record(AttemptLaunched(...))                  # from here on, a restart adopts it
await durable()                               # never launch what a restart wouldn't adopt
run = await placement.launch(Stage(attempt, run_id, objects_url))
record(AttemptPlaced(attempt, run), lazy=True)   # a restart follows it through this handle
while not finished:                           # the worker's `finished`, the provider's exit,
    ...                                       # or the worker silent: settle
    # reports (channel, else `.worker`) are evidence, never permission
    if canceled or past_timeout or not reported and past_provisioning:
        cancel = latch(requested, reason)     # answered to the worker's next beat
        if not reported or past(cancel_grace):
            writes = await take_gate(attempt, "aborted")   # none, or uncertain if `writing`
            await placement.cancel(run)
            return fail(reason, writes)
result = await objects.get(f"runs/{run_id}/{attempt}.result")
if result is None:
    return fail(f"the worker exited without a result: {exit}", retryable=True)
commit_or_fail(result)
```

A launched attempt survives an engine restart: the new engine adopts it,
following its recorded placement handle, else the one `resume` finds,
else its worker's reports — object-store-state.md §8. An attempt that ends
with its gate `writing` leaves its keyed outputs **unsettled**: the next
attempt reads the keys it meant to change back from the store and folds
what landed into its own commit. The engine counts in-flight attempts per
executor against `max_concurrent`.

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
  (`outputs` stays); there is no `mode` field. An output left unsettled
  by a dead attempt carries its `unsettled` intents in `outputs`.
- Store names, output config, annotations, placement and time windows are
  derived from the manifest and the key.

```json
{"status": "succeeded",
 "outputs": {"qaqc_samples": {"ref": Ref, "keys": {"files": [FileInfo], "added": 1, "removed": 0, "exact": true}}},
 "delivered": {"qaqc_files": {"after": null, "upserted": ["f1"], "deleted": ["f0"]}},
 "cursor": "token-42"}
{"status": "failed",
 "error": {"type": "Throttled", "message": "…", "traceback": "…", "retryable": true,
           "class": "transient", "retry_after": 30.0, "retry_for": 7200.0}}
```

The result is the attempt's commit request: per returned output its ref
(or `unchanged`), a keyed output's delta files (`keys`) and a partition
set's `elements`; per keyed Incremental input the page it `delivered`;
`cursor` if set; or an error. `retryable=false` for revision mismatch and version-mismatch without a full run. No result means
the harness died. The engine validates the attempt id, that every ref names
a known output and this scope, and that a keyed output reports its delta,
then commits against its own record of the pins. Inputs that moved since
they were pinned do not void the commit: the attempt delivered the window
it was given.

**Harness** (`python -m solera_worker run --objects URL --attempt ID`; the
project entrypoint comes from the environment): fetch `.spec` → claim
`.worker` (a loser writes nothing and waits for the owner's result) →
`start` on the channel → refuse on revision mismatch (a failed result, not
a crash) → resolve `env:` → load inputs per annotation (keyed Incremental
edges through the upstream key index) → build `ctx` → run the producer →
compare each keyed output with its key index and write the delta file (an
output where nothing changed is not stored) → take the gate, if anything
is to be written (write nothing if the engine holds it) → `store()` each
output → seal the result once into `.result`, retried as it is: a failed
upload never changes the outcome. Throughout, a thread beats every 10 s,
logs go live and as chunks, and a requested cancel stops the work before
the gate. `manifest` mode runs through `Local` only, at server start.

### Built-ins

| Kind | `launch` | handle | `wait` | `cancel` |
|---|---|---|---|---|
| `Local()()` | subprocess with an explicit env allow-list | `{launch, pid, started_at, ticks, host}` | polls its own child; an adopted pid only if host and /proc start time match, else can't tell | `SIGTERM`, then `SIGKILL`, only to that same process |
| `AWSECS(name, cluster, region)(cpu, memory, gpu, image)` | `run_task` with container overrides carrying the stage, `clientToken` = attempt | `{task_arn}` | describes until `STOPPED`; `Exit.meta.log_url`; a task not shown: can't tell | `stop_task` |
| `Modal(name, app)(gpu)` | spawns the harness function | `{call_id}` | polls the call: its return, raise or timeout is an exit; Modal's client and service errors: can't tell | cancels it |
| `K8sJob(name, cluster, namespace)(cpu, memory, image)` | creates the job `solera-{attempt}` (lowercased); an existing one is its own | `{job}` | watches conditions; deleted: lost | deletes the job |
| `Pool(name)(cpu, memory, gpu)` | nothing: the launched attempt is discoverable | none | — (the worker's reports) | — (a cancel before the claim ends it) |

`Pool` is the pull path (`lifecycle.md` §10). Workers are external
processes (`solera worker pool NAME`): they long-poll `GET
/api/projects/{p}/pools/{pool}/work` with their capacity and get launched
attempts that fit and have not started, oldest first; they race for each
attempt's claim by creating its `.worker`, and the winner runs it like any
attempt. No registration, no leases: a pool worker's liveness is its
attempt's heartbeat. A claim whose worker never reports is ended, classified
from its gate, and retried under a new attempt id.

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
`timeout`, `version`, load types via `typing.get_type_hints`),
sources, automations, store names with their `Store.version`, executors
by name, the **build identity**, and the project revision — a digest of
all of it. The build identity says which code this is
(per-key-processing.md §13): `Project(build=…)` or `$SOLERA_BUILD` when
given (a CI commit, an image digest); else, in a git work tree, `HEAD`
plus the content of every path that differs from it, untracked files
included and ignored ones not; else the content of the Python files under
the project's directory. A commit and a dirty flag are recorded for
display only. Where the manifest is built and where workers import the
project must agree on it — an image without `.git` should set
`SOLERA_BUILD` (the `Dockerfile` takes it as a build arg, or from Railway's
`RAILWAY_GIT_COMMIT_SHA`). A worker or sensor host whose revision differs
because it was computed by another method (git against a file hash) makes
the engine log a warning that says so. The engine counts the revisions it serves: the **revision
epoch**.

Registering a project reconciles the work outstanding under the last one: a
task not yet launched of a renamed asset carries on under its new name; one
of an asset that is gone is canceled, with why, and its run rolls up. A
launched attempt settles under the contract it was launched with.

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
trigger composition; delta-log compaction for very long histories; data preview, a read-only SQL
page and a DuckDB store (object-store-state.md §13).

**Non-goals:** cycles (DAG only, including self-triggers; loop inside a
producer); dynamic topology (the manifest is static per
revision; key sets cover data-driven cardinality); continuous operators
(bounded runs, micro-batch streams); cross-project edges (`Source` + commit
API is the bridge); imperative per-run control flow.
