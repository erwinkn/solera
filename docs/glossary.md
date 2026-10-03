# Glossary: Solera's domain language

Status: **normative for names.** One concept, one name; one name, one
concept. Every concept here earns its place through an edge case it
resolves or a user convenience; the ones that do not are listed in
"Redundant concepts", with what replaces them. Code, API, CLI, console
and docs follow this file.

It describes what is built. An entry's *Was:* line names what it replaced.
Changes still to build live in the design docs and the queue, and get
their entries when they land.

The running example is the demo project (`python/solera_server/demo.py`):
`sites` is a dynamic partitions output, `site_files` a keyed output
partitioned by site, `file_index` reads it incrementally, `uploads` is a
source.

## The model in one picture

```
project ── one deploy at a time (numbered)
  ├─ asset ──── outputs (none: a job) ── each on a store
  │    ├─ inputs ── each reads an upstream output (whole, incremental, dep)
  │    └─ partitions ── from dimensions (static, time, dynamic)
  ├─ source ─── an output fed from outside (commit, sensor)
  ├─ executor ─ where attempts run (an asset asks for a placement on it)
  ├─ automation ── fires → run
  └─ sensor ──── tick → commits, runs

run ─▶ task (asset partition) ─▶ attempt (one generation, one batch) ─▶ worker
attempt ─ commit ─▶ head (output partition) ─▶ ref ─▶ the data, in the store
                      └─ keyed: key index, one delta per commit number
incremental input ─ bookmark ─▶ pass (full | delta | diff) ─▶ batches
```

Relations, in words:

- A **project** declares assets, sources, stores, executors, resources,
  automations and sensors. A **deploy** is one version of it being served.
- An **asset** has outputs and inputs and is partitioned by dimensions.
  An asset with no output is a **job**. A **source** is an output no asset
  writes.
- A **run** has tasks, one per asset partition; a **task** has attempts,
  one per batch and per retry; an **attempt** has one generation and
  normally one worker.
- A **commit** installs a new **head** for each output partition it
  changed; a head holds a **ref**, which points into the store.
- A keyed output partition has a **key index**; each commit that changes
  it adds a **delta**, numbered by its **commit number**.
- An **incremental input** keeps a **bookmark** per partition; a **pass**
  reads the upstream in **batches**, one per attempt.

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
nothing. *Example:* `weekly_digest`.

**output** `Output`. A named slot on a store, written by one asset. It has
one head per partition. What the producer returns for it is its new
content, which the store writes: the engine never looks inside it. The
one value the engine reads is a `Patch`. Omitting an output from `Result`
writes nothing. *Not:* the asset; the ref (one head's pointer).
*Example:* `Output("files", key="file_id")`.

**Patch** `Patch(rows, remove=…)`. A value for a keyed output that changes
only the keys it names: `rows` are upserted, `remove` are removed, every
other key stays. A bare value instead replaces the output partition: keys
it lacks are removed. *Why a name:* a producer that sees four changed
files out of a million writes four keys, not a million.
*Example:* `Patch({"alpha-file-2": rows}, remove=["alpha-file-0"])`.

**source** `Source`. An output no asset writes, fed from outside by
commits or a sensor. With a store it loads like any output; without one
it is a pointer for lineage. An **observable source** is a source with
`observe()`, sugar for a sensor `{name}.observe` that commits to it.
*Was:* external output (`meta.external`). *Example:* `uploads`.

**store** `Store`. Writes and reads outputs, and owns how data is laid
out. Each store is **immutable** (writes only names no other attempt
uses; implements `cleanup`) or **fenced** (refuses an older attempt's
writes; implements `acquire`). Shipped: `FileStore`, `S3Store`
(immutable), `PostgresStore` (fenced). *Not:* the object store holding
engine state.

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
bookmark and outcome belong to. An **output partition** (output ×
partition) is what a head, a key index, cleanups and repairs belong to.
A partition exists while its dimension lists it: one removed from its
dynamic partitions (or outside its time range) leaves fan-out and
fan-in, and its heads are kept, unread.
*Not:* a key. *Was:* scope, slice, partition key, asset scope, output
scope; "retired" for a removed one. *Example:* `alpha`;
`day=2026-09-01,site=alpha` for two dimensions (sorted by dimension name).

**dimension**. One axis of partitions: `StaticPartitions` (a fixed list),
`TimePartitions` (windows of a duration or cron), or dynamic partitions.
*Example:* `site_digest` has dimensions `site` and `day`.

**dynamic partitions** `DynamicPartitions`. A keyed output (or source)
whose keys are the partitions of a dimension. Produced by an asset, or fed
as a source. Listing a partition again changes nothing; removing its key
removes the partition. *Was:* partition set, key set, set dimension's
elements. *Example:* `sites`.

**window**. A time partition's `[start, end)`: `ctx.partition_window`.
Only time partitions have windows. *Was:* also "delta window" and "change
window", which are passes.

**input** `In`, `Incremental`, `Each`, `AllPartitions`, `deps=`. How an
asset reads one **upstream** output:

- **whole** (`In`, or a plain `str`): the head's whole value, or its ref.
- **incremental** (`Incremental`): what changed since its bookmark, in
  batches; keyed upstreams by key, unkeyed ones by commit.
- **`Each`**: an incremental input over a keyed upstream whose asset is
  written for one key: one call per changed key (`ctx.key`),
  `concurrency` at once, failures kept per key. *Why:* a failure on one
  file must not block the other 999.
- **`AllPartitions`**: a whole input over the upstream's partitions that
  the consumer lacks dimensions for, as `dict[partition, value]`
  (§projection). *Why:* `weekly_digest` reads every site's index.
- **dep** (`deps=`): an input bound to no parameter: planned, pinned,
  watched by `OnChange`, never loaded.

*Not:* the upstream output itself. *Was:* edge (`EdgeDecl`, `/edges`,
`--keys EDGE=`). *Example:* `file_index` reads
`Incremental("site_files", batch_size=2)`.

**patterns** `include=`, `exclude=`. Globs (or `Regex`) on an incremental
input selecting upstream keys by name. *Example:*
`exclude={"drafts": "*-file-2"}`.

**resource**. A named object (a client, a connection) bound to a
producer or sensor parameter of the same name. Never an input.

**cursor**. JSON state a producer sets (`Result(cursor=…)`) and gets back
as `ctx.cursor`, per asset partition; a sensor keeps one too. Committed
with the outputs, so a refused commit asks the same question again.
*Not:* a bookmark (the engine's record of what an input has read).
*Example:* `site_feed` stores the feed's position.

**version**. What tells whether something changed, read by its subject:

- an **asset's version** `@asset(version=)` and a **store's version**
  `Store.version`: a code version, bumped by hand when the code's meaning
  changes. Part of the fingerprint.
- a **key's version**: the generation that last wrote it. A source key may
  carry the source's own version instead (an etag, an `updated_at`), only
  ever compared: same version, same content, so the commit leaves the key
  unchanged. An unkeyed source's version is one string.

*Not:* the deploy (the project's). *Was:* revision (a source's word on
content), token. *Example:* `file_index` declares `version="2"`;
`observe()` returns `{"a.csv": "c7"}`, and `c7` is `a.csv`'s version.

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
task per asset partition, in dependency order: the **run graph**.

**selection**. Which partitions a run covers: `latest`, `missing` (not
materialized), `all` or a list; and, per input, `keys=`: explicit keys read
as that input's batch, or `full`. *Example:* `solera run file_index --keys
'site_files=alpha-file-0'`.

**full run** `mode="full"`. The opposite of an incremental run: it
materializes all the data of each partition again. Its writes **reset**
each output partition (the store keeps nothing of the prior content), no
cursor is passed, and every incremental input reads its upstream in a
full pass. *Why reset is a word:* an unkeyed output appends one commit at
a time; only a reset tells the store to drop the earlier ones. A deploy
that removes an output or moves it to another store resets it too, as a
whole: the output under that name is a new one, its heads, index and the
bookmarks on it go at that deploy, and an attempt launched before commits
nothing (object-store-state.md §2).
*Not:* a full pass of one input (`keys={"x": "full"}`), which rewrites
nothing by itself. *Was:* `KeyedWrite.whole`.

**upstream**. The output an input reads. A run with `upstream=True` also
runs what its targets read; without it, inputs are pinned to current
heads, so a rebuild never re-polls an external system.

**materialized**. An asset partition is materialized when each of its
outputs has a head and it has caught up with its incremental inputs (no
pass under way); a job's, once a run of it succeeded. *Why caught up
counts:* `file_index`/`alpha` reads four files in two batches; after the
first commit it has a head but indexes half the files, and a fan-in must
not read it yet. *Was:* complete (missing = not complete), drained.

**projection**. How an input maps partitions: shared dimensions match
(`alpha` reads `alpha`); the consumer's extra dimensions **broadcast**
(every day of `site_digest`/`alpha` reads `site_files`/`alpha`); the
upstream's extra dimensions **fan in**: the input receives
`dict[partition, value]` over the upstream partitions materialized at pin
time, never waiting for the others (`weekly_digest` reads
`{"alpha": …, "beta": …}` of `file_index`). Only `AllPartitions` and deps
fan in; another input over an upstream with extra dimensions is refused at
registration. An incremental input never fans in.

**automation** `Automation(trigger=…)`. A rule that submits a run when its
**trigger** fires: `Every`, `Cron`, `OnChange` (`AutoRefresh()`: on a
change of any input) or `OnDeploy`. The rest is the run, in the run's own
vocabulary (`targets`, `partitions`, `mode`, `upstream`, `config`,
`keys`, `tags`), plus `enabled` and `skip_missing_inputs`; a bare trigger
stands for an automation with the defaults. **Attached** to an asset
(named `{asset}.{kind}.{n}`) or **standalone** (`Project(automations=…)`).
Each time it fires it submits one run, or skips with a reason (nothing to
do, inputs missing), which the history records with the time it fired.
*Example:* `site_feed.every.0` is `Every(3600)` on `site_feed`.

**sensor** `@sensor`, `Sensor`. A user check run every interval on its
executor (`Local`, the default, or a `Pool`); it may commit to the
sources it declares and request runs, all or nothing, and keeps a cursor.

**tick** `Tick`. One evaluation of a sensor and what it found: a cursor,
commits, run requests. Outcomes: `skipped`, `advanced`, `committed`,
`requested`, `refused`, `failed`. *Not:* an attempt (no spec, no run).
*Was:* observation.

**deploy**. One version of the project being served: the digest of its
manifest and its code, numbered in the order the namespace served them
(the **deploy number**). The code is named by `Project(build=…)` or
`$SOLERA_BUILD` (a CI build id), else by the git tree's content. `OnDeploy`
fires once per new deploy; a failed key gets one try per deploy; a worker
refuses to run code of another deploy. *Was:* project revision, revision
epoch, epoch; build (now just how a deploy names its code).

## Execution

**task**. One asset partition in one run: the run graph's node. It waits
for its upstream tasks, holds the retry budget, and lasts across every
attempt its partition needs: one per batch of a pass, plus retries.
Status: `waiting`, `queued`, `running`, then `succeeded`, `skipped`,
`failed`, `blocked` or `canceled`. *Why a name:* run R targets
`file_index` for `alpha` and `beta`; `alpha`'s pass takes two batches and
its first attempt fails once: task `file_index:alpha` has three attempts
and one status, and `weekly_digest`'s task waits on it.
*Not:* a planned run: a run plans many tasks; one batch or one key is an
attempt or a call within one.

**attempt**. One execution for a task: one generation, one batch, at most
one commit. Its own id, spec and result. Outcome: `succeeded`, `failed`,
`skipped`, `canceled`.

**claim**. The engine's rule that one attempt at a time runs an asset
partition. Before preparing an attempt, the engine claims its asset
partition; the claim lasts until the attempt settles. A task whose
partition is claimed by another run's attempt waits (held: `claim`). The
event counter at the claim is the attempt's generation. *Example:* the
hourly run and a manual run both target `file_index:alpha`; the second
waits for the first's attempt to commit, then reads what it wrote.
*Was:* scope lock, lock.

**worker**. A process that runs user code: one attempt (reads the spec,
computes, writes, seals the result), or, long-lived, a `Pool`'s attempts
or an executor's sensor ticks (`solera worker pool NAME`, `solera worker
sensors NAME`). A placement may start two workers for one attempt (a
Kubernetes Job's second pod): the first to swap the control file from
`open` to `owned` **owns** the attempt; the other waits for it to end and
writes nothing.
*Was:* harness; invocation (one worker of an attempt), invocation token
(worker id); sensor host.

**executor** `Local`, `Pool`, `AWSECS`, `K8sJob`, `Modal`. A named,
project-level place attempts (and sensor ticks) run: a kind and its
configuration. Its kind launches, waits for and cancels workers; a
`Pool`'s workers are started by you and pull attempts whose needs fit
their capacity. *Was:* `Environment` (the class). *Example:*
`AWSECS("etl", cluster="lab")`.

**placement**. One asset's request on an executor: the executor plus
options (`cpu`, `memory`, `gpu`, `image`). Calling an executor makes one:
`@asset(executor=etl(cpu=4))`; an executor alone is a placement with no
options. `ctx.placement` is the resolved one. *Was:* `ctx.execution`.

**attempt spec** `{attempt}.spec`. The immutable description of an
attempt: what to run, its pinned inputs, its batch, its generation, its
deploy. Written before launch; what a worker reads first.

**control file** `{attempt}.control`. The one object that decides an
attempt's life between the worker and the engine: who owns it, whether
it began writing (the gate) and with which intents, its sealed result,
or the engine's end. The engine creates it `open` before the launch;
after that it is only swapped (`If-Match`): `open` → `owned` → `writing`
→ `sealed` by the owner, or → `ended` by the engine, and exactly one of a
seal and an end lands. A worker that finds it gone stops and never
creates it (`lifecycle.md` §2.4). *Was:* `{attempt}.worker` (ownership),
`{attempt}.writing` (the gate) and `{attempt}.result`, until K18.

**attempt handle**. An executor's identifier for the worker it started
(an ECS task ARN, a pod name), recorded so a restarted engine can follow
or cancel it. *Was:* `RunHandle`, the protocol's `run` parameter.

**result**. What an attempt produced: per output its ref and delta, the
batches it read, its cursor, or an error; sealed once into the control
file. `Result(outputs=…, cursor=…)` is the producer's side
of the same thing.

**step**. One moment in a run's life, recorded on its run timeline.
The engine's: `submitted`, `ready`, `claimed`, `launched`, then
`committed`, `failed` or `canceled` (the engine **settles** the attempt:
commits its result or fails it). The worker's: `booted`, `imported`,
`loaded`, `computing`, `computed`, `writing`, `stored`, `finished`. A
**phase** is the time from one step to the next (`provisioning` is
`launched` → `booted`). *Example:* `file_index:alpha` attempt 2 was
`launched` at 10:00:03, `booted` at 10:00:41: 38 s provisioning.

**heartbeat**. A worker's report every 10 s: its steps since the last,
its usage. Evidence that it lives, never permission to write.

**cancel**. Two phases: **requested** (stop starting work; what finished
still commits) then **forced** (the engine swaps the control file to `ended`).
Reasons: `user`, `timeout`, `provisioning`.

## State, commits and the key index

**namespace**. One isolated engine state under an object-store prefix.
One engine writes it at a time.

**engine**. The control-plane process: plans runs, settles attempts,
runs no user code. A new engine on a namespace fences the previous one by
rewriting the journal under its **engine id**, random per process
(`object-store-state.md` §10). *Was:* writer (`WriterStarted`, the
segment's `writer` id).

**journal**. The engine's state as a log of events: one object,
`control/journal.json`, holding the engine id of its writer, the
checkpoint it extends and the events since. Every flush rewrites it with
`If-Match`. State is the fold of the events. *Was:* numbered segments
(`journal/{seq}.json`), until K18; the segment number `seq` went with
them.

**event**. One entry of the journal: one decision of the engine, applied
whole. *Example:* `RunSubmitted`, `AttemptLaunched`, `AttemptFinished`
(which carries the attempt's commit), `SourceCommitted`. *Not:* a step
(a run's moment, for people; several steps can share one event).

**event counter**. How many events the engine has applied: its clock, the
same in every engine that replays the journal. Generations and pins are
values of it. *Was:* event position, `applied` (code).

**commit**. The atomic install of new heads, all in one event: an
attempt's result (heads, key-index deltas, cursor, bookmarks, failed keys),
or a source's new content from outside (`solera commit`, `POST
…/sources/{name}/commit`, a tick): a version (unkeyed), a full key map,
or a patch (`upsert`, `remove`). History keeps one row per output
partition it changed. *Was:* materialization, source commit.
*Example:* `solera commit uploads --upsert '["u-1"]'`.

**head**. The latest committed state of something. An output partition's
head records its ref, the run, attempt and generation that wrote it, its
commit number and key count; a key's head is its latest version; an
asset partition's are its outputs'. *Not:* the ref alone. *Example:* the
head of `site_files`/`alpha`.

**generation**. Solera's write counter: the event counter at the
attempt's claim (or at a source's commit). Every write of a key carries
one; a later attempt on an asset partition has a larger one; it is never
reused. Lineage records it; immutable stores name objects by it; fenced
stores compare it. *Example:* `site_files/alpha/f-1/184467.json` was
written by generation 184467.

**commit number**. The n-th commit of an incremental output partition:
0, 1, 2… with no gaps, since a failed attempt's number goes to its retry.
The delta log and bookmarks count in it. *Why, beside the generation:*
`site_events` appends rows tagged with their commit number; an attempt
writes half of commit 42 and dies; its retry writes commit 42 again and
the store keeps only the retry's, so a reader of commits 40–42 never sees
the dead attempt's half. A generation, new on every retry, would leave them
between two committed ones. And "commits 40–42" is a range with no
listing. *Was:* batch (output side). *Example:* commit 57 of
`site_files`/`alpha` upserted `alpha-file-1`.

**key index**. The engine's index of an output partition's keys: per
key its generation, whether it was removed (a **tombstone**), and its
version for source keys. A log-structured merge tree of `.kx` files on
the object store, compacted and recounted in the background. *Was:* pair
filter, locator (removed by `versions.md`).

**delta**. The keys one commit changed: upserted and removed, one delta
file per commit number. The **delta log** is the deltas from the furthest
bookmark behind to the head.

**fence**. A newer writer's mark that refuses an older writer's
writes. A fenced store keeps one per output partition, by generation
(`Store.acquire`, `solera.fencing`); the journal's engine id is the
namespace's. *Not:* the gate.

## Reading inputs

**bookmark**. What an incremental input has read of its upstream, per
asset partition: the first commit not yet read (`next`), the pass under
way if any, and the patterns it reads under. The engine sends the input
what lies past it. *Not:* a cursor (the user's state). *Was:* watermark.
*Example:* `file_index`/`alpha`'s bookmark on `site_files` is at commit
56: commits 56 onwards are next.

**pass**. One read of an upstream, fixed when it starts, done in batches
over one or more attempts:

- **full**: the whole head (first read, full run, fingerprint change,
  `keys=full`);
- **delta**: the commits since the bookmark, removed keys included: an
  `each` asset drops them from its outputs;
- **diff**: after a pattern change, the keys whose match changed.

A pass that ends behind the head goes on to it.
*Was:* delivery, window, delta window, change window, pending window.

**asset change**. A deploy that adds an asset (new, or added back),
renames it, changes its declaration (version, deps, inputs and patterns,
outputs and their stores' versions) or resets it. Its automations decide
what follows: `OnChange` fires once for every partition whose inputs have
heads, a schedule waits for its next time, no automation runs nothing.
*Why a word:* "the deploy that changed it" is what decides a rebuild and
what `stale` is measured against, whichever of four things changed.
*Example:* `items` moved to another store, so its `OnChange` builds it in
the new one at once (object-store-state.md §2).

**batch** `ctx.batch[input]`. What one attempt reads from an incremental
input: up to `batch_size` keys, or up to `batch_size` upstream commits.
It may be one upstream commit, part of one, or several. One batch per
attempt and commit. Never empty, but for one case: a full pass whose
input takes no key reaches a plain producer as one empty batch, since
starting over must happen (an `Each` asset's cleanup does it instead). It knows its `index` in the pass (0-based, exact),
the planned `count` (possibly an estimate), `first`, `final`, `full`, and
its `upserted` and `removed` keys. *Example:* `file_index` reads four
files per site in two batches of `batch_size=2`. *Was:* page (`Changes`,
`ctx.batch`, `page_size`, `page`, `pages`).

**stale**. A partition materialized, but caught up before its asset's
last asset change: built by the old declaration, so due a rebuild. A
partition status beside `materialized`, `missing`, `failed`, `running`,
`removed`. *Edge case:* an asset with no automation, renamed or with a new
version, keeps its old content until someone runs it: `stale` says so,
where `materialized` would hide it. A run that catches it up clears it,
a skip included. *Not:* `missing` (no head at all).

**fingerprint**. The digest of what an asset's incremental reads depend
on: its version, its stores' versions, its outputs' migrations, the run's
config, and the versions of its whole inputs and deps. When it changes,
every incremental input reads its upstream in a full pass. *Was:*
interpretation fingerprint.

**pattern change**. What happens when an input's patterns change: commits
up to the change finish under the old patterns, then a diff pass adds and
removes the keys whose match changed, then reading continues under the
new ones. *Example:* adding `exclude="*-file-2"` removes `alpha-file-2`'s
outputs from `file_checks` without a full pass. *Was:* cutover, rescope,
pattern transition.

**reconcile**. The end of a full pass to an `each` asset: remove the
output keys the upstream no longer has. *Why:* a full pass lists what
exists, not what went away. `site_files` does a full run whose content is
`{K1, K3}`; `file_checks` reads K1 and K3, but still holds K2; reconcile
removes it. A delta pass needs none: Patch `remove=["K2"]` upstream
arrives as a removed key, and `file_checks` writes `Patch({"K1": …},
remove=["K2"])`. *Was:* cleanup (the pass's flag).

## Safety

**gate**. The one-time race between an attempt's worker and the engine
over whether it may write, held in the control file: the worker swaps it
to `writing` (with its **intents**: the keys it is about to write) before
its first store write; the engine swaps it to `ended` when it ends the
attempt first. Both swap the same version, so whoever comes first wins,
for good. *Not a lock:* nothing waits on it, nothing releases
it, and it concerns one attempt; ordering attempts is the fence's job.
*Example:* a 10-minute timeout fires as the worker finishes computing: if
the engine's `ended` lands first, the worker writes nothing and the
retry starts clean; if the worker's `writing` lands first, the writes may
be half done, and the next attempt repairs its intents. Only attempts with
outputs on fenced stores take one. *Was:* write fence (prose); the
create-only object `{attempt}.writing`, until K18.

**repair**. What the next attempt on an output partition does after a
worker died past its gate: acquire the fence, ask the store which intended
keys are present, and commit them at its own generation. An output
partition **owes a repair** until then. *Example:* g150 meant to write
three keys and died after two; g160 finds two in Postgres and commits
them. *Was:* unsettled.

**pin**. A reader's hold on the state as of one event counter value:
nothing let go of after that value is deleted until the reader is done.
*Example:* an attempt pinned at 184467 loads `site_files`/`alpha`'s key
index while compaction replaces its `.kx` files; the old files stay until
the attempt settles. Attempts pin at their claim; multi-batch passes,
pattern changes, ticks and the engine's own reads pin too. Internal:
lineage shows what was read, not what was pinned. *Was:* reader floor (the
oldest pin).

**cleanup** `Store.cleanup`. Deleting what nothing references any more,
once no pin predates it: the engine deletes its own files (index,
history); a worker deletes a store's superseded or abandoned objects
through `store.cleanup` (immutable stores). A store cleanup is pending, or
**stuck** after three failed tries. *Was:* garbage, discard, data garbage.
*Example:* `solera cleanups site_files alpha`.

**retention** `Retention(days=…, runs=…)`. How long run history is kept,
per asset. Current state and data never expire.

## History and lineage

**run history**. Parquet tables queried by DuckDB: `runs`, `tasks`,
`attempts`, `commits`, `lineage`, `key_outcomes`, `ticks`,
`run_timeline`. Rows are written as things finish.

**run timeline** `run_timeline`. A run's steps in order, the engine's and
the worker's. A task's **wait** and an attempt's phases are read off it.
*Was:* timeline, `run_events`.

**lineage**. What each output was made from. An output is identified by
up to `(output, partition, key, generation)` (the key for keyed outputs);
lineage maps it to the identifiers of what it read. Recorded per output
partition commit, and per key for `each` assets. *Example:*
`file_index`/`alpha` at g140 read `site_files`/`alpha` at g120.

**uncommitted read**. A read that saw a write no commit installed: a
fenced store reading current rows saw a dead attempt's half. Lineage
flags it and names the writer. *Not:* a repair's commit, which makes such
keys committed.

**failed keys**. An `each` asset's record, per partition, of the keys
whose last call failed, each with its retry record. A **retry pass** walks
them in batches; `solera keys retry` makes a **forced retry**.
*Was:* failure index. *Example:* `GET /assets/file_checks/failed-keys`.

**key outcome**. What one `each` call came to: `ok`, `removed`,
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
*materialize* (what a run does; the state is **materialized**, the act a
commit), *domain* (a fence row's key, internal), *write* (the act; the
value is the output's), *trigger*, *firing*, *delivery*, *page*,
*watermark*, *drained*, *complete*, *retired*, *revision*,
*invocation*, *host*, *observation*, *harness*, *scope*, *edge* (graph
drawing only).

## Redundant concepts

Each line: what goes, what replaces it, and why it does not earn a name.

| Goes | Replaced by | Why |
|---|---|---|
| scope, slice, partition key, key set | **partition** | One string names one partition everywhere |
| partition set | **dynamic partitions** | The common name for a dimension whose partitions come from data |
| retired (partition) | a **removed** partition | A rule (it leaves fan-out and fan-in, its heads stay), not a state worth a name |
| complete, missing, drained | **materialized** (and not) | `missing` stays only as the selection's name for "not materialized"; drained is "caught up", part of the definition |
| edge | **input** | `inputs=` already names it; a dep is an input bound to no parameter |
| materialization | **commit** (history), **head** (current) | A materialization was a commit's row for one output |
| source commit | **commit** | Same install, other origin |
| element | **key** of dynamic partitions | Dynamic partitions are a keyed output |
| harness, invocation, sensor host, pool worker (as a concept) | **worker** | All processes that run user code; an attempt may have two |
| build | **deploy** | The code half of a deploy's identity; `build=` stays as its option |
| project revision, epoch | **deploy**, **deploy number** | |
| revision, token (a source's word on content) | **version** | Its subject says which: an asset's, a store's, a key's |
| writer (engine instance) | **engine** | "writer" also meant an attempt writing |
| event position, `applied` | **event counter** | |
| write fence (prose) | **gate** | The gate is attempt-level; a fence is store- or journal-level |
| scope lock, lock | **claim** | |
| batch (output side) | **commit number** | Frees "batch" for what an attempt reads |
| page, `Changes` | **batch** | |
| watermark | **bookmark** | It records how far an input has read |
| delivery, window (delta), change window | **pass** | |
| interpretation fingerprint | **fingerprint** | It is the asset's; no other fingerprint is a domain term |
| rescope, pattern transition, cutover | **pattern change** | One procedure, four names |
| cleanup (pass flag) | **reconcile** | |
| unsettled, settled (outputs) | **repair**, owes a repair | "settle" stays the engine's decision on an attempt |
| landed, `writes`, `uncertain` | **`write`**: `none`, `writing`, `complete` | Model change 5 |
| garbage, discard, data garbage | **cleanup** | One rule (delete once no pin predates it), two places |
| mixed (lineage) | two reads | Model change 6 |
| failure index | **failed keys** | |
| timeline, `run_events` | **run timeline** | |
| spec, handle, `RunHandle` | **attempt spec**, **attempt handle** | Both belong to one attempt, not to a run (open question 1) |
| observation | **tick** | |
| `Changes.deleted`, `delivered.deleted` | **removed** | `Patch`, `Commit` and `Observed` say remove |
| `Ref.version`, locator, row digests | **generation** | `versions.md` |
| pinned vs read, in lineage | **what was read** | The pin is internal |

**Kept, though they look redundant:**

- **commit number and generation.** Both number writes; the commit number
  is dense per output partition and reused by a retry (commit number,
  above), the generation is global and never reused (an abandoned
  attempt's objects can go at once; fences compare it).
- **task and attempt.** A task is a partition's place in a run (waits,
  retry budget, status); an attempt is one execution, one batch.
- **event and step.** An event is the engine's (replayed, exact); a step is
  for people (the worker's steps travel in heartbeats and are folded into
  one event at the end).
- **head and ref.** The ref is the store's (users receive it); the head is
  the engine's (who wrote it, its commit number, key count).
- **executor and placement.** One executor serves many assets with
  different `cpu`/`memory`; a name means one kind and configuration.
- **cursor and bookmark.** The user's state versus the engine's record.
- **gate and fence.** The gate decides once, worker versus engine, for one
  attempt; a fence orders attempts (or engines) by generation.
- **claim and fence.** The claim keeps two attempts of one partition from
  running at once, inside the engine; the fence keeps a stale one, still
  running somewhere, from writing.

## Open questions for Erwin

1. **"Run spec" and "run handle".** Both belong to one attempt (the spec
   is `{attempt}.spec`; the handle names one attempt's worker), and a run
   has many attempts, so "run spec" would give "run" a second meaning.
   This glossary says **attempt spec** and **attempt handle**. If you want
   "run" to mean one execution, that is the task question below, not a
   rename of two words.
2. **Task stays.** It is not a planned run: it is one asset partition's
   place in one run, across all its batches and retries (entry above).
   Removing it would mean either calling every attempt a run (then the
   user's request needs a new name) or saying "the run's asset partition"
   everywhere. Recommended: keep.
3. **`missing` as a selection value.** It names "not materialized" in
   `partitions="missing"`; `"unmaterialized"` is the strict alternative.
   Recommended: keep `missing`.
4. **Fresh namespaces.** Renamed persisted fields (events, checkpoint,
   history columns, manifest) make existing state unreadable. Assumed
   acceptable, as for `versions.md`.
