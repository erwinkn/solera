# Glossary: Solera's domain language

Status: **normative for names.** One concept, one name; one name, one
concept. Every concept here earns its place through an edge case it
resolves or a user convenience; the ones that do not are listed in
"Redundant concepts", with what replaces them. Code, API, CLI, console
and docs follow this file; §"Rename plan" is how they get there.

It describes the target vocabulary: it assumes `versions.md` is built
(a key's version is the generation that wrote it; no content hashing), and
the renames below applied. Where a name is still in the code, its entry
says what it was (*Was:*).

The running example is the demo project (`python/solera_server/demo.py`):
`sites` is a partition set, `site_files` a keyed output partitioned by
site, `file_index` reads it incrementally, `uploads` is a source.

## The model in one picture

```
project ── one deploy at a time (numbered)
  ├─ asset ──── outputs (none: a job) ── each on a store
  │    ├─ inputs ── each reads an upstream output (whole, incremental, Each, AllPartitions, dep)
  │    └─ partitions ── from dimensions (static, time, partition set)
  ├─ source ─── an output fed from outside (source commit, sensor)
  ├─ executor ─ where attempts run (an asset asks for a placement on it)
  ├─ automation ── trigger → firing → run
  └─ sensor ──── tick → source commits, runs

run ─▶ task (asset partition) ─▶ attempt (one generation) ─▶ invocation(s)
attempt ─ commit ─▶ head (output partition) ─▶ ref ─▶ the data, in the store
                      └─ keyed: key index, one delta per batch
incremental input ─ watermark ─▶ delivery (full | delta | diff) ─▶ pages
```

Relations, in words:

- A **project** declares assets, sources, stores, executors, resources,
  automations and sensors. A **deploy** is one version of it being served.
- An **asset** has outputs and inputs and is partitioned by dimensions.
  An asset with no output is a **job**. A **source** is an output no asset
  writes.
- A **run** has tasks, one per asset partition; a **task** has attempts; an
  **attempt** has one generation and one or more invocations, one of which
  owns it.
- An attempt's **commit** installs a new **head** for each output partition
  it changed; a head holds a **ref**, which points into the store.
- A keyed output partition has a **key index**; each commit that changes it
  adds a **delta**, numbered by **batch**.
- An **incremental input** keeps a **watermark** per partition; a
  **delivery** reads the upstream in **pages**.

## Authoring

**project** `Project`. Everything one deploy declares: assets, sources,
stores, executors, resources, automations, sensors.
*Not:* a namespace (where a project's state lives).

**asset** `@asset`. A function plus its declaration: outputs, inputs,
partitions, executor, automations, retries, version.
*Not:* an output; one asset may write several.
*Example:* `site_feed` writes `site_events` and `site_files`.

**job** `@job`. An asset with no outputs. It has inputs, partitions, a
cursor and automations; its commit records lineage and cursor, and wakes
nothing. *Example:* `weekly_digest`. *Not:* the native extension's
streaming `Job` (renamed, below).

**output** `Output`. A named slot on a store, written by one asset. It has
one head per partition. *Not:* the asset; the ref (one head's pointer).
*Example:* `Output("files", key="file_id")`.

**source** `Source`. An output no asset writes, fed from outside by
source commits or a sensor. With a store it loads like any output; without
one it is a pointer for lineage. An **observable source** is a source with
`observe()`, sugar for a sensor `{name}.observe` that commits to it.
*Was:* external output (`meta.external`). *Example:* `uploads`.

**store** `Store`. Writes and reads outputs; owns how data is laid out.
Each store is **immutable** (writes only names no other attempt uses;
implements `discard`) or **fenced** (refuses an older attempt's writes;
implements `acquire`). Shipped: `FileStore`, `S3Store` (immutable),
`PostgresStore` (fenced). *Not:* the object store holding engine state.

**ref** `Ref`, `TableRef`, `ObjectRef`. A store's self-contained JSON
pointer to one output partition's content: everything a load needs, no
credentials. Carries the generation that wrote it. *Not:* the head, which
holds the ref plus the engine's facts about it.
*Example:* `TableRef(table="ops.site_status", where={"site": "alpha"})`.

**key**. The unit of a keyed output: a string naming every row that
carries it (one or many), or one value (`keyed=True`). Consumers receive
keys, failures are per key, the key index lists them.
*Not:* a partition. *Was:* element (a partition set's keys).
*Example:* `site_files` keyed by `file_id`; `alpha-file-2` is a key.

**partition**. One slice of an asset along its dimensions, named by a
string; `""` when the asset is unpartitioned. An **asset partition**
(asset × partition) is what a task runs, a claim holds, and a cursor,
watermark and outcome belong to. An **output partition** (output ×
partition) is what a head, a key index, discards and repairs belong to.
*Not:* a key. *Was:* scope, slice, partition key, asset scope, output
scope. *Example:* `alpha`; `day=2026-09-01,site=alpha` for two
dimensions (sorted by dimension name).

**dimension**. One axis of partitions: `StaticPartitions` (a fixed list),
`TimePartitions` (windows of a duration or cron), or a **partition set**.
*Example:* `site_digest` has dimensions `site` and `day`.

**partition set** `PartitionSet`. A keyed output (or source) whose keys
are the partitions of a dimension. Produced by an asset, or fed as a
source. Listing a partition again changes nothing.
*Was:* key set, set dimension's elements. *Example:* `sites`.

**window**. A time partition's `[start, end)`: `ctx.partition_window`.
Only time partitions have windows. *Was:* also "delta window" and "change
window", which are deliveries.

**input** `In`, `Incremental`, `Each`, `AllPartitions`, `deps=`. How an
asset reads one **upstream** output. Every input has a kind:

- **whole** (`In`, or a plain `str`): the head's whole value, or its ref.
- **incremental** (`Incremental`): what changed since its watermark, in
  pages; keyed upstreams by key, unkeyed ones by batch.
- **Each** (`Each`): incremental, and the asset is written for one key:
  one call per changed key, failures kept per key.
- **AllPartitions** (`AllPartitions`): every partition of the upstream's
  extra dimensions, as `dict[partition, value]`.
- **dep** (`deps=`): an input bound to no parameter: planned, pinned,
  watched by `AutoRefresh`, never loaded.

*Not:* the upstream output itself. *Was:* edge (`EdgeDecl`, `/edges`,
`--keys EDGE=`). *Example:* `file_index` has an incremental input on
`site_files`, `page_size=2`.

**patterns** `include=`, `exclude=`. Globs (or `Regex`) on an
incremental or Each input selecting upstream keys by name. A change of
patterns is a cutover (below). *Example:* `exclude={"drafts": "*-file-2"}`.

**resource**. A named object (a client, a connection) bound to a
producer or sensor parameter of the same name. Never an input.

**cursor**. JSON state a producer sets (`Result(cursor=…)`) and gets back
as `ctx.cursor`, per asset partition; a sensor keeps one too. Committed
with the outputs, so a refused commit asks the same question again.
*Not:* a watermark (the engine's position on an input).
*Example:* `site_feed` stores the feed's position.

**write**. What a producer returns for an output. Three forms:
a **replacement** (a bare value: the output partition becomes exactly
this), a **patch** (`Patch(rows, remove=…)`: these keys change, the rest
stay), or **`Sql`** (a query PostgresStore materializes). Omitting an
output from `Result` writes nothing.

**reset**. A write that starts an output partition over: its first
write, or a full run's. The store keeps nothing of the prior content.
*Not:* a replacement, which compares against the prior keys.
*Was:* `KeyedWrite.whole`.

**version** `@asset(version=)`, `Store.version`. A code version, bumped
by hand when the code's meaning changes. It is part of the interpretation
fingerprint: a bump makes every incremental input deliver in full.
*Not:* generation (a write counter), revision (a source's word on
content), the project's deploy. *Example:* `file_index` declares
`version="2"`.

**migration** `Migration(name, payload)`. A schema change owned by an
output, applied by its store before the first write of an attempt, and
recorded in the store's own ledger.

**error class** `Rejected`, `Failed`, `Transient`, `Abort`. How a user
error is retried: `Rejected` never, `Failed` and `Abort` by `retries=`,
`Transient` on its own backoff until `retry_for`. Classified by subclass
or by `Project(errors=…)`.

## Scheduling

**run**. A request to bring targets up to date: `targets`, `partitions`
(a selection), `mode`, `upstream`, `config`, `keys`, `tags`. It plans one
task per asset partition. *Not:* a placement's process (*was* `RunHandle`,
now a handle).

**selection**. Which partitions a run covers: `latest`, `missing`, `all`
or a list; and, per input, `keys=`: explicit keys delivered as that
input's page, or `full`. *Example:* `solera run file_index --keys
'site_files=alpha-file-0'`.

**full run** `mode="full"`. A run whose writes are resets: no cursor,
and every incremental input delivers its upstream in full.
*Not:* a full delivery of one input (`keys={"x": "full"}`).

**upstream**. The output an input reads. A run with `upstream=True` also
runs what its targets read; without it, inputs are pinned to current
heads, so a rebuild never re-polls an external system.

**complete**. An asset partition is complete when each of its outputs
has a head and its last delivery drained; a job's, once a run of it
succeeded. **Missing** = not complete. A **retired** partition has left
its set: it leaves fan-out and fan-in, and its heads stay read-only.
*Example:* `partitions="missing"` plans every incomplete partition.

**projection**. How an input maps partitions: shared dimensions match;
the consumer's extra dimensions **broadcast** (every consumer partition
reads the same upstream partition); the upstream's extra dimensions must
**fan in** (`AllPartitions`, or a dep), reading every partition with a
head at pin time, never waiting for missing ones.

**automation** `Automation`. Submits a run, in the run's own vocabulary,
whenever its trigger fires. **Attached** to an asset (named
`{asset}.{trigger}.{n}`) or **standalone** (`Project(automations=…)`).
`AutoRefresh()` is `Automation(trigger=OnChange())`.
*Example:* `site_feed.every.0`.

**trigger** `Every`, `Cron`, `OnChange`, `OnDeploy`. When an automation
fires. *Not:* a run's origin (*was* the history's `trigger` column).

**firing**. One occasion an automation's trigger fired: it submits one
run over all its targets, or is skipped. *Was:* tick (architecture §9).

**sensor** `@sensor`, `Sensor`. A user check run every interval on a
sensor host; it may commit to the sources it declares and request runs,
all or nothing, and keeps a cursor.

**tick** `Tick`. One evaluation of a sensor and what it found: a cursor,
source commits, run requests. Outcomes: `skipped`, `advanced`,
`committed`, `requested`, `refused`, `failed`. *Not:* an attempt (no spec,
no run) or a firing. *Was:* observation.

**source commit** `solera commit`, `Commit`, `POST …/sources/{name}/commit`.
A new head for a source: a revision (unkeyed), a full key map, or a
patch (`upsert`, `remove`). Recorded as a run with no tasks.
*Example:* `solera commit uploads --upsert '["u-1"]'`.

**deploy**. One version of the project being served: the digest of its
manifest and build, numbered in the order the namespace served them
(the **deploy number**). `OnDeploy` fires once per new deploy; a failed
key gets one try per deploy. *Was:* project revision, revision epoch,
epoch.

**build**. The identity of the project's code: `Project(build=…)`,
`$SOLERA_BUILD`, else the git tree's content. Part of the deploy.

## Execution

**task**. One asset partition within a run. Its status: `waiting`,
`queued`, `running`, then `succeeded`, `skipped`, `failed`, `blocked` or
`canceled`. *Example:* task `file_index:alpha`.

**attempt**. One execution of a task, with its own id, generation, spec
and result. Outcome: `succeeded`, `failed`, `skipped`, `canceled`.
Retries of a task are new attempts.

**claim**. An attempt's exclusive hold on its asset partition, from
dispatch to settlement: one attempt at a time writes an asset partition.
Its event position is the attempt's generation. *Was:* scope lock, lock.
*Not:* an invocation taking the attempt (below).

**invocation**. One process running an attempt. A placement may start
two; the first to create `.worker` **takes** the attempt and is its
**owner**; the others write nothing. *Was:* invocation token; "claim" for
the `.worker` object.

**executor** `Local`, `Pool`, `AWSECS`, `K8sJob`, `Modal`. A named,
project-level place attempts run: a kind and its configuration. Its kind
launches, waits for and cancels attempts. *Was:* `Environment` (the
class). *Example:* `AWSECS("etl", cluster="lab")`.

**placement**. One asset's request on an executor: the executor plus
options (`cpu`, `memory`, `gpu`, `image`). Calling an executor makes one:
`@asset(executor=etl(cpu=4))`; an executor alone is a placement with no
options. `ctx.placement` is the resolved one. *Was:* `ctx.execution`.

**pool** `Pool(name)`. An executor whose attempts wait until a **pool
worker** (`solera worker pool NAME`) pulls one whose needs fit its
capacity.

**worker**. The process that runs one attempt: reads the spec, takes the
attempt, computes, takes the gate, writes, seals the result. A pool
worker is a long-lived worker pulling from a pool. *Was:* harness.

**sensor host**. A long-lived process with the project loaded that runs
sensor ticks: the engine's own by default, or `solera worker sensors
NAME` for a pool. *Not:* a worker (ticks are not attempts).

**spec** `{attempt}.spec`. The immutable description of an attempt:
what to run, its pinned inputs, its generation, its deploy.

**result**. What an attempt produced: per output its ref and delta, the
pages it delivered, its cursor, or an error; sealed once into
`{attempt}.result`. `Result(outputs=…, cursor=…)` is the producer's side
of the same thing.

**heartbeat**. A worker's report every 10 s: evidence that it lives,
never permission to write.

**cancel**. Two phases: **requested** (stop starting work, drain) then
**forced** (the engine takes the gate and ends the attempt). Reasons:
`user`, `timeout`, `provisioning`.

**settle**. The engine's decision on an ended attempt: commit its result,
or fail it. The last phase of an attempt (`settling`).

**handle**. An executor's identifier for the process it started,
recorded (`AttemptPlaced`) so a restarted engine can follow it.
*Was:* `RunHandle`, the protocol's `run` parameter.

## Storage and the key index

**namespace**. One isolated engine state under an object-store prefix.
One engine writes it at a time.

**engine**. The control-plane process: plans runs, settles attempts,
runs no user code. A new engine on a namespace fences the previous one.
*Was:* writer (`WriterStarted`, the segment's `writer` id).

**journal**. The engine's state as an append-only log of events in
object-store segments, with periodic checkpoints. State is the fold of the
events. *Example:* `AttemptFinished`, `SourceCommitted`.

**event position**. How many events the engine has applied: the engine's
clock, the same in every engine that replays the journal. Generations and
pins are event positions. *Was:* `applied` (code), `seq` in places.

**head**. An output partition's latest commit, as the engine records it:
its ref, the run, attempt and generation that wrote it, its batch and key
count, and whether it is complete.
*Not:* the ref alone. *Example:* the head of `site_files`/`alpha`.

**commit**. The atomic install of an attempt's result, or of a source
commit: new heads, key-index deltas, cursor, watermarks, failure deltas,
all in one journal event. Identified by `(run, attempt)`. History keeps
one row per output partition it changed. *Was:* materialization.

**generation**. Solera's write counter: the event position of the
attempt's claim (or of a source commit). Every write of a key carries
one; a later attempt on an asset partition has a larger one; it is never
reused. Lineage records it; immutable stores name objects by it; fenced
stores compare it. *Not:* batch, version, revision.
*Example:* `site_files/alpha/f-1/184467.json` was written by generation
184467.

**batch**. The number of an incremental output partition's commit:
0, 1, 2… per output partition, assigned by the engine. A retry reuses
its predecessor's batch, so an append-only sink stays idempotent on
`(partition, batch)`. An unkeyed output's batch is the rows it appended;
a keyed output's is its delta. *Not:* a page (a unit of delivery) or a
generation (never reused). *Example:* `site_events` batch 42.

**revision**. What a source says about a key's content (an etag, a
ctag, an `updated_at`), or about an unkeyed source's content (a string).
Optional; only compared: same revision, same content, so the key is
unchanged. Never shown as a version.
*Example:* `observe()` returns `{"a.csv": "c7"}`; `c7` is a revision.

**key index**. The engine's index of an output partition's keys: per
key its generation, whether it was removed (a **tombstone**), and its
revision for source keys. A log-structured merge tree of `.kx` files on
the object store, compacted and recounted in the background. *Was:* pair
filter, locator (removed by `versions.md`).

**delta**. The keys one commit changed: upserted and removed, one delta
file per batch. The **delta log** is the deltas from the lowest
consumer's watermark to the head. *Example:* batch 57 of
`site_files`/`alpha`: `alpha-file-1` upserted.

**fence**. A newer writer's mark that refuses an older writer's
writes. A fenced store keeps one per output partition, by generation
(`Store.acquire`, `solera.fencing`); the journal keeps one per namespace,
by engine. *Not:* the gate.

## Delivery

**watermark**. An incremental input's position, per asset partition:
the first upstream batch not yet delivered (`next`), the delivery under
way if any, the patterns it delivers under, and a cutover or reconcile it
owes. *Not:* a cursor (the user's state).

**delivery**. One pass over an upstream, fixed when it starts: **full**
(the whole head), **delta** (the batches since the watermark), or
**diff** (a cutover's membership change). It is read in pages, over one or
more attempts. A delivery that ends behind the head goes on to it.
*Was:* window, delta window, change window, pending window.

**page**. The unit of delivery to a consumer: up to `page_size` keys, or
whole upstream batches. One page per attempt and commit. Each page knows
its index (`page`, 0-based, exact), the planned count (`pages`, possibly
an estimate), `first` and `final`. *Not:* a batch (a unit of upstream
commits). *Example:* `file_index` reads four files per site in two pages
of `page_size=2`.

**interpretation fingerprint**. The digest of what an asset's reading of
its inputs depends on: its version, its stores' versions, its outputs'
migrations, run config, and its whole inputs and deps by generation. When
it changes, every incremental input delivers in full.

**cutover**. A change of an input's patterns, done in three steps:
deltas up to the cutover batch finish under the old patterns, then a diff
delivery adds and removes the keys whose match changed, then deltas
continue under the new ones. *Was:* rescope, pattern transition.

**reconcile**. After a full delivery to an Each asset, a pass over its
outputs that removes keys the upstream no longer names. *Was:* cleanup
(the delivery's `cleanup` flag).

**drained**. Whether an asset partition's last delivery ran out. Part of
complete; not a user-facing concept.

## Lifecycle and safety

**gate** `{attempt}.writing`. The one-time decision between an attempt's
worker and the engine: the worker creates it `writing` before its first
store write, or the engine creates it `aborted` (or `closed`) when it ends
the attempt first. Whoever creates it wins. Only attempts with outputs on
fenced stores take one. *Not:* a fence. *Was:* write fence (README,
architecture §8).

**intents**. The keys a gated attempt means to change, recorded in its
gate. What a repair reads back.

**landed**. Whether an ended attempt's writes can still land: `none`,
`complete` or `uncertain`. From the worker's result, else from the gate.
*Was:* write-completion evidence, the result's `writes` field.

**repair**. What the next attempt on an output partition does after a
writer died past its gate: acquire the fence, ask the store which intended
keys are present, and commit them at its own generation. An output
partition **owes a repair** until then. *Was:* unsettled.

**pin**. An event position a reader holds: it reads the state as of that
position, and nothing let go of after it is deleted while it holds. An
attempt pins at its claim (its generation); paged deliveries, cutovers,
ticks and the engine's own reads pin too. Internal: lineage shows what
was read, not what was pinned. *Was:* reader floor (the oldest pin).

**garbage**. Engine files (index, history) nothing references any more.
The engine deletes them once no pin predates them.

**discard** `Store.discard`. A store object superseded or abandoned in an
immutable store, deleted by a worker through `store.discard` once no pin
predates it. Pending, or **stuck** after three failed tries.
*Was:* data garbage. *Example:* `solera discards site_files alpha`.

**retention** `Retention(days=…, runs=…)`. How long run history is kept,
per asset. Current state and data never expire.

## History and lineage

**run history**. Parquet tables queried by DuckDB: `runs`, `tasks`,
`attempts`, `commits`, `lineage`, `key_outcomes`, `ticks`, `run_events`.
Rows are written as things finish.

**lineage**. For each commit, the generation of every input partition it
read, flagged `uncommitted` (a write no commit settled) or `mixed` (two
reads saw two generations). *Example:* `file_index`/`alpha` at g140 read
`site_files`/`alpha` at g120.

**timeline** `run_events`. Everything that happened to a run, in order:
the engine's decisions and the worker's reports (`claimed`, `launched`,
`booted`, `computing`, `stored`, `committed`…). An attempt's **phases**
(`preparing` … `settling`) and a task's **wait** are read off it.

**failure index**. An Each asset's key index of the keys that did not
succeed, per partition, each with its retry record. A **retry pass** walks
it in pages; `solera keys retry` makes a **forced retry**.
*Example:* `GET /assets/file_checks/failures`.

**key outcome**. What one Each call came to: `ok`, `removed`,
`unmatched`, or a failure (`rejected`, `failed`, `retrying`, `canceled`,
`timed_out`).

**metadata**. Facts an attempt records about what it commits
(`ctx.metadata(rows=…)`), per output. **tags** label runs and assets.

**status / outcome**. A run's or task's **status** says where it is,
live or done; an **outcome** says how a finished thing ended (an attempt,
a key, a tick, an asset partition's last task).

## Words that are not domain terms

Use them in their plain sense only, never as a name for a concept above:
*chunk* (a bounded piece of a transfer: log chunks, a store write's
chunks), *snapshot* (a database's), *kind*, *slot*, *digest* (any hash),
*format version* (a byte format's), *token* (an auth secret only),
*materialize* (say run), *domain* (a fence row's key, internal),
*observation*, *harness*, *scope*, *edge* (graph drawing only).

## Redundant concepts

Each line: what goes, what replaces it, and why it does not earn a name.

| Goes | Replaced by | Why |
|---|---|---|
| scope, slice, partition key, key set | **partition** | One string names one partition everywhere; "scope" had to be explained every time |
| edge | **input** | `inputs=` already names it; a dep is an input bound to no parameter |
| materialization | **commit** (history), **head** (current) | A materialization was a commit's row for one output |
| element | **key** of a partition set | A partition set is a keyed output |
| harness | **worker** | The same process; `solera_worker` |
| project revision, epoch | **deploy**, **deploy number** | "revision" now means a source's word on content |
| writer (engine instance) | **engine** | "writer" also meant an attempt writing |
| write fence (prose) | **gate** | The gate is attempt-level; a fence is store- or journal-level |
| scope lock, lock | **claim** | The lock is the claim, indexed |
| `.worker` "claim" | an invocation **takes** the attempt | "claim" stays engine-side |
| rescope, pattern transition | **cutover** | One procedure, three names |
| cleanup (delivery flag) | **reconcile** | One procedure, two names |
| unsettled, settled (outputs) | **repair**, owes a repair | "settle" stays the engine's decision on an attempt |
| data garbage | **discard** | The store's verb names its entries |
| window (delta), change window | **delta delivery** | "window" stays a time partition's |
| observation | **tick** | |
| tick (automation) | **firing** | "tick" stays a sensor's |
| `RunHandle` | **handle** | "run" stays Solera's |
| `KeyedWrite.whole` | **reset** | `whole` was true exactly on resets |
| `Changes.deleted`, `delivered.deleted` | **removed** | `Patch`, `Commit` and `Observed` say remove |
| `Ref.version`, locator, token, row digests | **generation**, **revision** | `versions.md` |
| pinned vs read, in lineage | **what was read** | The pin is internal (Erwin) |
| `Head.elements` | `Head.partitions` | What a partition set's head lists |

**Kept, though they look redundant:**

- **batch and generation.** Both number writes, but a batch is dense per
  output partition and a retry reuses it (append-only sinks stay
  idempotent on `(partition, batch)`, and `Batches(lo, hi)` ranges need no
  listing); a generation is global and never reused (an abandoned
  attempt's objects can go at once; fences compare it).
- **head and ref.** The ref is the store's (users receive it); the head is
  the engine's (who wrote it, its batch, key count, completeness).
- **executor and placement.** One executor serves many assets with
  different `cpu`/`memory`; a name means one kind and configuration.
- **cursor and watermark.** The user's state versus the engine's position.
- **gate and fence.** The gate decides once, worker versus engine, for one
  attempt; a fence orders attempts (or engines) by generation.
- **garbage and discard.** The engine deletes its own files; only a worker
  can reach a store's objects.

**Candidates to merge** (shape changes, not renames; not in the plan):

- **`progress` into the partition's outcome record.** `Model.outcomes` and
  `Model.progress` are both per asset partition (last outcome; `drained`).
  One record `{outcome, run, attempt, at, drained}` is one concept,
  "the asset partition's last result", read by `complete`.

## Rename plan

Order matters: phase 1 frees names that phase 2 reuses. Each row is one
codemod: rename `from` to `to` in the listed places, word-bounded. "User"
marks what a user or store author sees (SDK, API, CLI, console, docs).
Persisted names change too (journal events, checkpoint fields, history
columns, manifest): as in `versions.md`, there is no deployment to
migrate, so existing namespaces start fresh.

### Phase 0: owned by `versions.md` (in flight)

Listed so nothing falls between the two. Applied by the `versions.md`
build, in its vocabulary (generation, revision; no token).

| From | To | Where | User |
|---|---|---|---|
| `Ref.version` | `Ref.generation` | `sdk.py`, stores, console `Ref`, API `OutputHead.version` | yes |
| index entry `version` + `locator` | `generation` | `native/`, `solera/keys/`, `key-index-format.md` | no |
| per-key token | `revision` | `versions.md` prose, `.kx` entry, `SourceCommitted` | no |
| `Output(revision=)` | deleted | `sdk.py`, manifest `outputs[*].revision`, console `OutputDecl.revision` | yes |
| `Keys(revisions=)` | `Keys(generations=)` | `stores/__init__.py`, stores, `stores.md` | yes |
| `ctx.revision` (Each) | `ctx.generation` | `worker.py`, `each.py`, docs | yes |
| failure record `revision` | `generation` | `failures.py`, console `FailureKey.revision` | yes |
| `key_outcomes.revision`; explain `upstream_revision`, `outputs[*].revision` | `generation`, `upstream_generation` | `history.py`, `views.py`, console `KeyOutcome`, `Explain`; the "Revision" columns and "at revision" prose in `routes/asset-keys.tsx` and `routes/run.tsx:637` (key outcomes) | yes |
| head "version" in the console | generation | `routes/asset.tsx` ("The committed version of each output", `head.ref.version`), `routes/asset-history.tsx` (`m.version`) | yes |
| `materializations.version`, `lineage.version`, `input_version`, `attempts.outputs` values | generations | `history.py`, console | yes |
| lineage `pinned_version`, `pinned_generation`, `input_generation` vs `read` | one `generation` (what was read) | `history.py`, `attempts.py`, console `LineageNode` | yes |

### Phase 1: free the names

| # | From | To | Where | User |
|---|---|---|---|---|
| 1.1 | store protocol class `Scope` | `WriteContext` | `solera/stores/__init__.py`, `__init__.py` exports, `fencing.py`, `stores/files.py`, `solera_postgres`, `testing/stores.py`, `worker.py` (`_Out.scope`), `keys/index.py`, tests, `stores.md`, `architecture.md` §4, `object-store-state.md` §9 | yes (store authors) |
| 1.2 | parameter `scope` of `store()`, `discard()`, `acquire()`, `fence()` | `context` | same files | yes |
| 1.3 | project `revision` (manifest, spec, `ProjectRegistered.revision`, `Model.revision`, diagnostics, sensor host) | `deploy` | `sdk.py` (`_build`), `engine.py`, `model.py`, `attempts.py`, `sensors.py`, `worker.py`, `solera_worker/sensors.py`, `selftest.py`, `build.py` (`method_note`), console `Manifest.revision`, `Diagnostics.revision`, `SensorHost.revision`, the hosts table's "Revision" (`routes/sensors.tsx:152`) | yes |
| 1.4 | `Model.epoch` | `deploy_number` | `model.py`, `engine.py`, `views.py`, `planning.py` | no |
| 1.5 | failure record `epoch`, `epoch_min`, retry pass `epoch`, `epoch_acc` | `deploy`, `deploy_min`, `deploy`, `deploy_acc` | `failures.py`, `engine.py`, `each.py`, `views.py`, console `FailureScope.epoch_min`, `Failures.epoch` | yes (API) |
| 1.6 | automation state `last_revision` | `last_deploy` | `model.py`, `engine.py` (`automation_view`), console `Automation.last_revision` | yes (API) |
| 1.7 | `RunHandle`, protocol param `run` of `wait`/`cancel` | `Handle`, `handle` | `placements/*.py`, `architecture.md` §10 | yes (executor authors) |
| 1.8 | journal `writer` (segment field, `State.writer`, `Model.writer`), `WriterStarted` | `engine`, `EngineStarted` | `journal.py`, `state.py`, `model.py`, `object-store-state.md` §3–§5, §10, tests/sim | no |
| 1.9 | `EngineRestarted` | `EngineOutage` | `engine.py`, `model.py` | no |
| 1.10 | native `SortedRun`, merge input "run" | `SortedEntries`, "stream" | `native/src/run.rs` (file → `entries.rs`), `lib.rs`, `jobs.rs`, `solera/keys/*`, `bench/` | no |
| 1.11 | native `Job` | `Merge` | `native/src/lib.rs`, `jobs.rs`, `solera/keys/jobs.py` | no |
| 1.12 | native `Pages`, `Rows.pages`, `KeyedWrite.pages()`, `iter_pages()` | `Chunks`, `chunks()`, `iter_chunks()` | `native/src/lib.rs`, `stores/__init__.py`, stores, `stores.md`, `architecture.md` §4 | yes (store authors) |

### Phase 2: bulk renames

| # | From | To | Where | User |
|---|---|---|---|---|
| 2.1 | `scope` (the partition string: variables, params, dict keys, fields) | `partition` | all of `python/`, `tests/`, `apps/console/src` (types, routes, `queries.ts`), docs. Where a function already has a `partition` local it is the same value: merge them | yes |
| 2.2 | `scopes` | `partitions` | API `AssetDetail.scopes`, `Failures.scopes`, views, console | yes |
| 2.3 | `up_scope`, watermark `up`, `input_scope` | `upstream_partition`, `upstream_partition`, `input_partition` | `delivery.py`, `model.py`, `engine.py`, `planning.py`, `upkeep.py`, `views.py`, `history.py`, console `Explain`, `EdgeScope` | yes (API) |
| 2.4 | `MAX_SCOPES`, `scope_statuses`, `scope_discards`, `ScopeOutcome`, `EdgeScope` | `MAX_PARTITIONS`, `partition_statuses`, `partition_discards`, `PartitionOutcome`, `InputPartition` | `planning.py`, `views.py`, `attempts.py`, console types | yes (console) |
| 2.5 | CLI `solera scopes discards OUTPUT [SCOPE]`; `POST …/scopes:clear-discards` | `solera discards OUTPUT [PARTITION]`; `POST …/discards:clear` | `cli.py`, `api.py`, console `mutations.ts`, README, `lifecycle.md` §9.8 | yes |
| 2.6 | `edge`, `edges` (an asset's input) | `input`, `inputs` | `GET …/assets/{name}/edges` → `/inputs`; `asset_edges`; explain param `edge`; `planning.Edge` → `Input`; console `EdgeDecl` → `InputDecl`, `Edge` → `Input`, `EdgeState` → `InputState`, route `asset-edges.tsx` → `asset-inputs.tsx`; CLI help `EDGE=` → `INPUT=`; prose "per-edge" | yes |
| 2.7 | history table `materializations`; `history.materialization()`, `.materializations()` | `commits`; `commit_row()`, `.commits()` | `history.py`, `model.py`, `api.py`, console `Materialization` → `Commit` | yes (API) |
| 2.8 | "Materialize" (verb, UI and CLI help) | "Run" | `cli.py` (`run` help), console `features/materialize.tsx` (→ `run-dialog.tsx`), labels "Materialize", "Materialize upstream first", "Materialize this partition" | yes |
| 2.9 | console "Versions" (asset history) | "Commits" | `routes/asset-history.tsx` ("Versions", "No versions yet", "A job makes no versions") | yes |
| 2.10 | `Head.elements`, result `elements`, `Planner.elements`, sentinel `"<elements>"` | `partitions`, `partitions`, `set_partitions`, `"<partitions>"` | `planning.py`, `views.py`, `engine.py`, `sensors.py`, `worker.py`, `sdk.py`, `stores/*`, `native/src/rows.rs`, console `Head.elements` | yes (API) |
| 2.11 | `unsettled` (model, spec outputs, `AttemptFinished.unsettled`, API, console); commit `settled` | `repairs`; `AttemptFinished.intents`; `repaired` | `model.py`, `engine.py`, `attempts.py`, `views.py`, `worker.py`, console `AssetStatus.unsettled`, `AssetDetail.unsettled` | yes (API) |
| 2.12 | `GET /holds`, `holds_view`, console `Holds` | `GET /repairs` and `GET /discards`, two views, two types | `api.py`, `views.py`, console `queries.ts`, `health.tsx`, `overview.tsx` | yes |
| 2.13 | watermark `rescope {old, new, cutover, snapshot, pin}`; `EdgeState "rescope"` | `cutover {old, new, batch, snapshot, pin}`; `"cutover"` | `delivery.py`, `engine.py`, `model.py`, `views.py`, `each.py`, console `Watermark`, `lib/status.ts` | yes (API) |
| 2.14 | delivery `cleanup` flag | `reconcile` | `delivery.py`, `engine.py`, console `Watermark.delivery.cleanup` | yes (API) |
| 2.15 | watermark `pass` | `reset_by` | `delivery.py`, `engine.py`, console `Watermark.pass` | no |
| 2.16 | `Changes.deleted`; result `delivered[*].deleted` | `removed` | `sdk.py`, `worker.py`, `each.py`, docs, demo, example | yes |
| 2.17 | `KeyedWrite.whole` | `KeyedWrite.reset` | `stores/__init__.py`, stores, `stores.md` | yes (store authors) |
| 2.18 | result `writes` (none/complete/uncertain) | `landed` | `lifecycle.py`, `worker.py`, `attempts.py`, `engine.py`, console `AttemptResult.writes` | no |
| 2.19 | `AttemptLaunched.pin`, `launched.pin`, claim `pin` | `generation` | `model.py`, `engine.py`, `attempts.py` | no |
| 2.20 | held reason `lock` | `claim` | `engine.py`, console timeline | yes (console) |
| 2.21 | class `Environment`; executor record `environment`; placement record key `placement` (its options) | `Executor`; `config`; `options` | `executors.py`, `sdk.py`, `placements/*`, console `Placement`, `Manifest.executors`, `Executor.environment` | yes |
| 2.22 | `ctx.execution` | `ctx.placement` | `worker.py`, `architecture.md` §2, example | yes |
| 2.23 | package `solera_server/placements/` | `solera_server/executors/` | imports, docs | no |
| 2.24 | "harness" | "worker" | prose and comments: `docs/`, README, `sdk.py`, stores, `placements/*`, tests | yes (docs) |
| 2.25 | Ref `meta.external` | `meta.source` | `sdk.py` (`Source.head`), `engine.py`, `api.py`, `attempts.py`, console `lib/store.ts`, `graph.tsx` | yes (API) |
| 2.26 | automation view `last_at` | `last_fired` | `engine.py` (`automation_view`), console `Automation.last_at` | yes (API) |
| 2.27 | run detail attempt `status` | `outcome` | `engine.py` (`run_detail`), console `Attempt.status` | yes (API) |
| 2.28 | `Model.retired` (runs) | `deleted` | `model.py`, `upkeep.py`, `object-store-state.md` §5, §11 | no |
| 2.29 | reader pin `domains`; `KeyService.hold`/`release` | `prefixes`; `pin`/`unpin` | `model.py`, `keyservice.py`, `engine.py` | no |
| 2.30 | `Commit(version=)`, `solera commit --version`, `observe() -> str` "a version" | `Commit(revision=)`, `--revision`, "a revision" | `sdk.py`, `cli.py`, `api.py` (commit body), `engine.py`, README, `architecture.md` §5, `per-key-processing.md` §12 | yes |

### Phase 3: prose only

Docs, README, docstrings and comments, after phases 1–2:

- "scope" → partition; "asset scope" → asset partition; "output scope" →
  output partition; "slice" → partition (`TableRef` docstring, `stores.md`).
- "delta window", "change window", "pending window", "paged delta window"
  → delta delivery; "window" stays for time partitions.
- "observation" → tick; "a tick is skipped" (automations,
  `architecture.md` §9) → a firing is skipped.
- "write fence" (README, `architecture.md` §8, `object-store-state.md`
  §0) → gate where it means `.writing`; "the `{attempt}.writing` fence"
  (`per-key-processing.md` intro) → gate.
- "scope lock" → claim; "the claim `.worker`" → the `.worker` object, which
  the invocation that takes the attempt creates.
- "key set", "key-set output" → partition set; "lists the domain" →
  lists every partition.
- "invocation token" → invocation id.
- "feed token", "delta token" (demo, `per-key-processing.md` §4) → the
  feed's cursor.
- "materialize", "materialization" → run, commit.
- `per-key-processing.md`: `solera retry` → `solera keys retry`.
- `object-store-state.md` §4: add the events the table lacks:
  `TasksHeld`, `EngineOutage`, `KeysRetryRequested`, `DiscardsDone`,
  `DiscardsCleared`, `SensorAdvanced`.

### Phase 4: if Erwin agrees (see below)

| From | To | Where | User |
|---|---|---|---|
| `Changes`, `ctx.changes[input]`, spec input `changes` | `Page`, `ctx.page[input]`, `page`; the index field `page` → `index` | `sdk.py`, `__init__.py`, `worker.py`, `each.py`, docs, demo, example, tests | yes |
| history `runs.trigger` (manual, automation, sensor, commit), facet, filter | `origin` | `history.py`, `api.py`, console `RunRow.trigger`, `Facets`, `routes/runs.tsx` | yes |

## Open questions for Erwin

1. **`Changes` → `Page`.** The SDK's name for a page is a third name for
   one concept. Renaming it means the page's index field cannot also be
   `page` (`ctx.page["files"].page`), so it becomes `index`, which changes
   one name you already decided. Recommended: rename.
2. **"deploy" for the project revision.** "revision" now belongs to
   sources, so the project's needs another word. "deploy" matches
   `OnDeploy` and "one try per deploy". Recommended.
3. **`Store.version` is a version.** The decided rule says *version* is
   only an asset's code version; a store's is the same thing (a
   hand-bumped code version in the fingerprint). Recommended: widen the
   rule rather than rename it.
4. **Batch on keyed outputs.** The decided rule says a batch is an
   upstream commit of an append output; keyed outputs number their deltas
   by batch too, and watermarks count in batches for both. Recommended:
   widen the rule to every incremental output.
5. **Fresh namespaces.** Renamed persisted fields (events, checkpoint,
   history columns, manifest) make existing state unreadable. Assumed
   acceptable, as for `versions.md`.
