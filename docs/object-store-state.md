# Object-store state: data model

Status: **implemented** (K0, J1–J5, H1), except where a section says otherwise. Replaces the SlateDB persistence and the
retention design in `storage-redesign.md` §4–§5. Keeps `Incremental`
edges, per-batch deltas and watermarks from PR #7, and the in-memory
engine model from PR #8.

The bet: the engine runs on an object store alone — no database, no other
infrastructure.

## 0. Constraints

- **Primitives:** `GET` (including range reads), `PUT`, `PUT` create-only,
  `LIST` (lexicographic, with or without a delimiter), `DELETE`. No
  compare-and-swap: obstore's `file://` backend does not implement it
  (verified on 0.11.1), so nothing may depend on it.
- **One writer per namespace.** The engine's in-memory state is the source
  of truth; storage is written to, and read only when a writer starts.
- **The engine owns keys; stores own rows.** Every key → version index,
  for assets and external sources alike, is engine-defined (§6). Stores
  never compute deltas or keep key maps, so user-written stores need no
  key logic.
- **The engine never reads, lists or deletes output data** except through
  the store contract (§9).
- **Nothing on a hot path grows with history.** Every in-memory structure
  is bounded by the project's shape, in-flight work, or a retention policy.

## 1. Storage kinds

Everything lives under `{root}/{namespace}/`.

| Kind | Path | Written by | Mutability | Deleted when |
|---|---|---|---|---|
| Journal segment | `control/journal/{seq:020d}.json` | engine | create-only | the checkpoint before the newest covers it |
| Checkpoint | `control/checkpoints/{seq:020d}.json` | engine | create-only | two newer checkpoints exist |
| Key index file | `keys/{output}/{scope}/{name}.kx` | harness (delta files), compaction | create-only | no longer in the index and no consumer needs it (§6) |
| History file | `history/{table}/{ulid}.parquet` | engine | create-only | merged into a bigger file, or rewritten without deleted runs (§7) |
| Attempt file | `runs/{run}/{attempt}.json` | engine creates it with the spec; the harness overwrites it with spec + result + log index | two writes, one writer each | with its run |
| Write fence | `runs/{run}/{attempt}.writing` | the harness before it writes, or the engine before it ends the attempt — whichever is first | create-only | with its run |
| Heartbeat | `runs/{run}/{attempt}.beat` | harness, every 30 s | overwritten | with its run |
| Engine heartbeat | `engine/alive.json` | engine, every 30 s while runs are live | overwritten | never (one object) |
| Attempt log | `runs/{run}/{attempt}.log` (chunks `{attempt}.log.{n:06d}` while running) | harness | chunks write-once; joined at the end | with its run |
| Output data | store-defined (FileStore: `{output}/{partition}/{key}.json` under `.solera/data`, §9) | the store, inside the harness | overwritten in place | when the output no longer holds it (§9); never expired |

**Growth.** `control/` is bounded: at most two checkpoints plus the
journal since the older one, and a checkpoint is written whenever that
journal reaches the size of the last checkpoint (§10) — so `control/`
stays under about three times the engine's state size. `keys/` is bounded
by live keys plus unconsumed deltas. What grows over time is `runs/` and
`history/`, both bounded by retention, and user data, which holds only
current content.

```
{root}/{namespace}/
  control/
    journal/00000000000000001001.json        ← a writer's fence segment
    journal/00000000000000001002.json
    …
    checkpoints/00000000000000000990.json    ← previous (kept for recovery)
    checkpoints/00000000000000001040.json    ← newest
  keys/
    site_files/alpha/000000000057.kx         ← delta file of batch 57
    site_files/alpha/c01J8ZE2….kx            ← compacted file
    uploads/_/000000000003.kx                ← external source
  history/
    runs/01J9A2….parquet                     ← merged: 4,000 runs
    runs/01J9C7….parquet                     ← one flush
    tasks/…  attempts/…  materializations/…  lineage/…
  runs/
    01J8ZB3K…/01J8ZB3M….json                 ← attempt: spec, then spec + result
    01J8ZB3K…/01J8ZB3M….log                  ← gzip blocks
    01J8ZB3K…/01J8ZB3M….writing              ← write fence
    01J8ZB3K…/01J8ZB3M….beat                 ← heartbeat
```

Output data lives wherever its store puts it: FileStore under
`.solera/data` next to the project file, S3Store in its own bucket.

## 2. Identifiers

| Id | Form | Notes |
|---|---|---|
| run | `{ulid}` | sorts by time and embeds its creation time; carries no names, so renames never orphan history |
| task | `{asset}:{scope}` | unique within its run |
| attempt | `{ulid}` | globally unique; names delta files and attempt files |
| commit | `(run, attempt)` | no separate commit id or record |
| batch | integer per (output, scope) | engine-assigned, starts at 0 |
| seq | integer per namespace | journal position |
| writer | the `seq` of that writer's fence segment | no separate epoch object |

**Renames.** `@asset(aliases=["old_name"])`. On registration the engine
moves everything held under an alias to the current name: heads, cursors,
watermarks, key indexes, outcomes, automation state (attached automations
are named after their asset), retention lists. Outputs named after the
asset follow. Aliases are also passed to stores, so one that derives a
physical name from the output (a Postgres table) can rename it.

## 3. Journal segment

One object per flush. There is one way to change state:
`State.record(*events)` applies the events to the model and buffers them,
synchronously — the engine never waits on storage. A background flusher
writes what is buffered as one segment once the oldest event has waited
1 s or 1 MB is buffered.

Only what acts on the outside world on the strength of an event waits for
it to be written, with `await state.durable()`, which flushes at once
rather than after the interval:

- an attempt is started only once its `AttemptLaunched` is durable, so a
  crash never leaves a running attempt no restart would adopt (§8);
- files are deleted only once the events that let go of them are durable,
  so a replay never references them again;
- an API call that recorded anything (a run submission, a source commit,
  a cancel, a claim) is answered only once that is durable — a `503` if
  this writer was replaced meanwhile.

A segment is sealed before it is written: a failed or interrupted write is
retried with the very same bytes, so a retry that finds its segment
already there recognizes it as its own rather than another writer's.

Everything else the storage does runs on its own loop, off the engine's:
the history lake flushes and merges (§7), and `Upkeep` truncates delta
logs, compacts and recounts key indexes (§6), deletes garbage and applies
retention (§11).

```json
{
  "seq": 1042,
  "writer": 1001,
  "at": 1790074866.1,
  "events": [
    {"type": "AttemptFinished", "run": "01J8ZC7Q…", "task": "site_feed:alpha",
     "attempt": "01J8ZC7R…", "outcome": "succeeded", "started_at": 1790074865.2, "finished_at": 1790074866.0,
     "commit": {
       "heads": {"site_events": {"…": "Head, §5"}, "site_files": {"…": "Head, §5"}},
       "keys": {"site_files": {"batch": 57, "added": 0, "removed": 0, "exact": true,
                               "files": [{"name": "000000000057-01J8ZC7R…", "level": 0, "entries": 2,
                                          "min": "alpha-file-1", "max": "alpha-file-3", "…": "…"}]}},
       "cursor": "5921",
       "watermarks": {}
     }},
    {"type": "AutomationFired", "name": "site_feed.every.0", "at": 1790074866.1, "run": "01J8ZC7S…"}
  ]
}
```

## 4. Events

State is `fold(apply, events)`. Queueing, dependency unblocking and run
status are derived inside `apply`; they are not events.

| Event | Fields | Effect |
|---|---|---|
| `WriterStarted` | — | first event of every writer; its segment's `seq` becomes the writer id |
| `ProjectRegistered` | `revision`, `manifest` | replaces the manifest; applies aliases; reconciles automation state |
| `RunSubmitted` | `run` (id, request, tasks) | adds an active run |
| `RunControlled` | `run`, `action` (`cancel` \| `pause` \| `resume`) | |
| `AttemptLaunched` | `run`, `task`, `attempt`, `started_at`, `at`, `execution`, `prepared`, `pool?` | the attempt file exists and a placement is about to start it: its claim and scope lock become durable (§8) |
| `AttemptClaimed` | `attempt`, `worker`, `at` | a pool worker took a launched attempt; no other worker is offered it |
| `AttemptFinished` | `run`, `task`, `attempt`, `outcome` (`succeeded` \| `failed` \| `skipped` \| `canceled`), `started_at`, `finished_at`, `error?`, `retryable?`, `commit?`, `unsettled?` | records the attempt; on commit, installs heads, cursor, watermarks, and each keyed output's new delta file; `unsettled` keeps the intents of a writer that died (§8) |
| `SourceCommitted` | `source`, `head`, `keys?`, `at`, `run?` | installs a source head and its delta file; a commit that changed something records `run` in the history (§7) |
| `IndexCompacted` | `output`, `scope`, `added` [file], `removed` [name], `recount?`, `at` | swaps compacted files into a key index; a recount replaces its count |
| `IndexTruncated` | `output`, `scope`, `below`, `at` | drops delta log entries below `below` |
| `GarbageDeleted` | `paths` | forgets index files that were deleted |
| `AutomationChanged` | `name`, `enabled` | |
| `AutomationFired` | `name`, `at`, `run` | clears its pending set |
| `RunArchived` | `run`, `at` | drops a finished run from memory; its rows join the pending history (§7) |
| `RunReopened` | `run`, `at` | a retry brings a finished run back; its history rows are dropped until it ends again |
| `HistoryFlushed` | `files` {table: file}, `upto` {table: row seq} | installs one Parquet file per table and drops the rows it holds |
| `HistoryCompacted` | `changes` [{`table`, `removed` [path], `added?` file}], `at` | swaps merged or purged files in; the removed ones become garbage |
| `RunsDeleted` | `runs`, `at` | drops their pending rows and hides them in files that may hold them |

An attempt that has not launched yet — it is still pinning inputs and
writing its spec — is **not** in the journal: after a restart its task is
simply dispatched again. Once `AttemptLaunched` is written, the attempt
outlives the engine: the next engine adopts it, waits for it and commits
its result (§8).

## 5. State (in memory) — also the checkpoint's content

A checkpoint is this structure serialized as of `seq`. Nested maps rather
than joined string keys, because partition keys may contain `/`.

```
State
  seq, writer, revision, manifest
  heads        {output: {scope: Head}}             # assets and external sources
  indexes      {output: {scope: KeyIndex}}         # keyed outputs and keyed sources (§6)
  cursors      {asset: {scope: json}}
  watermarks   {asset: {edge: {scope: Watermark}}}
  outcomes     {asset: {scope: Outcome}}           # last terminal result per scope
  automations  {name: AutomationState}
  runs         {run: Run}                          # active, or finished and not yet archived
  history      {files: {table: [File]}, rows: {table: [[seq, row], …]}, seq, imported}   # §7
  unsettled    {output: {scope: [Intent, …]}}      # keyed outputs a dead writer may have half-written (§8)
  garbage      [[path, at], …]                     # index and history files nothing references any more
```

| Type | Fields | Bounded by |
|---|---|---|
| `Head` | `ref` (from the store), `run`, `attempt` (may point at a deleted run), `batch` (incremental outputs: the last batch that changed it, −1 before any), `base` (unkeyed incremental outputs: the first batch after the last reset), `count` (keyed: live keys), `elements?` (partition sets and set dimensions), `complete`, `version` (declared asset version), `asset`, `at` | outputs × partitions |
| `KeyIndex` | `prefix` (where its files live — kept across renames), `count`, `count_exact`, `files` [{`name`, `level`, `min`, `max`, `entries`, `size`, `tail`, `index`}], `log` [[`batch`, [file]], …] — see §6 | a few dozen files per index |
| `Watermark` | `batch` (first batch not fully delivered; during a full drain, the head's batch + 1 when the drain began, so changes made while draining arrive afterwards as deltas), `until` (the last batch of a delta window being delivered in pages), `after` (last key delivered inside the window or the full drain), `full` (a full drain is in progress), `fingerprint`, `output` and `up` (the upstream index it reads) | edges × partitions |
| `Outcome` | `outcome`, `run`, `attempt`, `at` | assets × partitions |
| `AutomationState` | `enabled`, `last_fired`, `last_run`, `last_revision`, `pending` (set of `[asset, scope]` for OnChange) | automations × partitions |
| `Run` | `id`, `request` {targets, partitions, mode, config, keys, automation, tags}, `status`, `paused`, `created_at`, `events` (how many it has recorded), `tasks` {task: `Task`} | in-flight work |
| `Task` | `status`, `deps`, `ready_at` (now, or a retry's due time), `wait` (seconds counted so far), `queued_at` (when the wait clock last started; null while stopped), `held?` [reason, name] (why the dispatcher last passed it over), `max_attempts`, `attempts` [`Attempt`], `launched?` {`attempt`, `started_at`, `at`, `execution`, `prepared`, `pool?`, `worker?`, `claimed_at?`} | |
| `File` | `path`, `rows`, `bytes`, `at` [lo, hi] (time column), `runs` [first, last], `deleted?` [run] (hidden until rewritten), `deleted_at?` | files per table: ~log(rows) after merging |
| `Intent` | `added`, `removed`, `exact`, `files` (the dead attempt's delta files), `run`, `attempt` | writers that died mid-write, until the next commit of that output |
| `Attempt` | `id`, `outcome`, `started_at`, `finished_at`, the seconds of each phase it reached, `cpu_seconds?`, `peak_memory?`, `error?`, `outputs?` {output: ref} | |

**Derived, rebuilt at start:** the claims and scope locks of launched
attempts (from `Task.launched`), the pool queue, the ready queue and the
dependents index.

**Memory only:** the claim of an attempt still preparing (a restart
dispatches its task again), pool leases, registered workers (they
re-register on their next heartbeat), a cache of key index blocks, and a
local copy of the history files (§7).

Example (abridged):

```json
{
  "seq": 1040, "writer": 1001, "revision": "c0ffee…", "manifest": {"…": "…"},
  "heads": {"site_files": {"alpha": {
    "ref": {"output": "site_files", "store": "default", "partition": "alpha", "version": "8f35…",
            "handle": {"mode": "keyed", "path": "site_files/alpha", "key": "path"}},
    "run": "01J8ZC7Q…", "attempt": "01J8ZC7R…",
    "batch": 57, "count": 4, "complete": true, "version": "1", "at": 1790074866.0}}},
  "indexes": {"site_files": {"alpha": {
    "prefix": "keys/site_files/alpha/", "count": 4, "count_exact": true,
    "files": [{"name": "c01J8ZE2…-0000", "level": 1, "min": "alpha-file-0", "max": "alpha-file-3", "entries": 4, "size": 212, "…": "…"},
              {"name": "000000000057-01J8ZC7R…", "level": 0, "min": "alpha-file-1", "max": "alpha-file-3", "entries": 2, "size": 140, "…": "…"}],
    "log": [[56, [{"name": "000000000056-01J8ZB…", "…": "…"}]], [57, [{"name": "000000000057-01J8ZC7R…", "…": "…"}]]]}}},
  "cursors": {"site_feed": {"alpha": "5921"}},
  "watermarks": {"file_index": {"site_files": {"alpha": {"batch": 56, "after": null, "full": false, "fingerprint": "8d46…",
                                                         "output": "site_files", "up": "alpha"}}}},
  "outcomes": {"file_index": {"alpha": {"outcome": "succeeded", "run": "01J8ZB3K…", "attempt": "01J8ZB3M…", "at": 1790074800.0}}},
  "automations": {"site_feed.every.0": {"enabled": true, "last_fired": 1790074866.1,
                  "last_run": "01J8ZC7S…", "last_revision": "c0ffee…", "pending": []}},
  "history": {"files": {"runs": [{"path": "history/runs/01J9A2….parquet", "rows": 4000, "at": [1790…, 1790…],
                                  "keys": ["01J9A2…", "01J9B7…"], "bytes": 81233}]},
              "rows": {"runs": [[311, ["01J9C8…", 1790074866.1, "…"]]]}, "seq": 311},
  "runs": {"01J8ZC7S…": {"…": "Run"}}
}
```

## 6. Key index

One per keyed output (or keyed external source) and partition. The format
and operations live in an SDK library (`solera.keys`) used by the harness,
by compaction, and by the server for key listings. The engine itself only
holds each index's `KeyIndex` record.

**Structure: a log-structured merge tree of sorted files.** Every file
holds `(key, version, deleted)` entries sorted by key.

- **Level 0** holds delta files, one per commit, named by batch. Their key
  ranges overlap.
- **Levels 1+** hold compacted files with non-overlapping key ranges, each
  up to ~64 MB, each level ~10× the previous.
- **Newest wins:** a key's current version is its entry in the newest file
  containing it; a `deleted` entry hides older ones.
- **`log`** lists the delta files by batch, from the lowest consumer
  watermark to the head. A delta file stays readable while it is in the
  log, even after compaction has merged it out of the levels.

**File layout.** `[data blocks][filters][block index][footer]`, byte
format in `key-index-format.md`. Data blocks hold ~64 KB of entries
before compression. Everything after the data blocks — filters, block
index, footer — is the file's **tail**; the index and footer alone are
its **index part**. The `KeyIndex` record stores each file's size and both
lengths, so a reader fetches exactly what it needs with one range read:
the tail when it checks filters, the index part when it scans, the whole
file when it is small — then range-reads only the blocks it needs.

**Filters.** Each file carries three blocked Bloom filters (14 bits per
item, ~0.1% false positives): its keys, its live `(key, version)` pairs,
and its deleted keys. A written `(key, version)` that no pair filter and
no tombstone filter matches, across every file whose key range could hold
the key, is a real change of a live key and needs no block read: the
key's current `(key, version)` is always present in some file. A key no
key filter matches is new. Everything else — unchanged rewrites, keys
that may be deleted, false positives — gets an exact lookup.

**Key count.** `KeyIndex.count` is exact while every commit's reads are
exact, which is always the case for indexes small enough to read whole.
When a commit relies on the filters, a new key that some key filter
falsely matches is counted as an update, so the count drifts low by about
the filters' false-positive rate on inserted keys, and `count_exact`
turns false. Compaction then schedules a **recount** — a full scan of the
index's block indexes and blocks, about 150 reads at 100M keys — at most
once per `recount_interval` (default 1 hour), which makes it exact again.
Deltas themselves are always exact; only the count is approximate.

**Read strategy.** Per level, the reader chooses between streaming the
whole level, reading the touched blocks, and reading the file tails and
then only the blocks the filters could not clear. Consecutive blocks are
one range read; files are selected by key range. The cheapest option in
requests that fits a latency budget (default 2 s) wins, else the fastest.
Levels small enough (≤ 32 MB) are always read whole.

**Operations.**

| Operation | Who | How |
|---|---|---|
| Compute a delta | harness, at write time | Extract `(key, version)` from the written rows (declared `revision` column, else row digest). Check them against the index **as pinned in the spec**, with the filters and the read strategy above. Keep entries whose version changed, plus `deleted` entries for removed keys that may exist. A full replacement also compares against every existing key, which is inherent. Write the result as the batch's delta file. |
| Commit | engine | Add the delta file to level 0 and to `log`; `count += added − removed`. The scope lock — one attempt per (asset, scope) from launch to settlement — guarantees the index didn't change underneath. |
| Deliver pending deltas | harness, for an `Incremental` edge | Read the `log` files from the watermark to the head; chunk by `batch_size` in key order; ask the upstream store for those rows with `Keys(…)`. |
| Full delivery | harness | Page through the merged view of all levels from `after`, `batch_size` keys at a time, and ask the store for them with `Keys(…)`. Per level, only the files covering the page are opened, and only their index parts are read. |
| Compaction | the engine's machine by default (§6, *Engine work*) | When level 0 exceeds ~8 files, merge it with the overlapping level-1 files into new level-1 files, cascading down; commit with `IndexCompacted`. |
| Truncate the log | engine | Drop `log` entries below the lowest consumer watermark and below every window an in-flight attempt was given (`IndexTruncated`); an output with no `Incremental` consumers keeps none. A consumer whose window the log no longer holds gets a full delivery. |
| Delete files | engine | A file in neither `files` nor `log` joins `garbage`, and is deleted once every attempt that could have pinned it has finished (`GarbageDeleted`). A delta file of an attempt that never committed is deleted when the attempt ends, unless it is an unsettled intent (§8). |

Writes that never pass through the harness as rows — `Sql` materialized
inside Postgres — are the one case where the store must report the written
`(key, version)` pairs; only stores supporting such writes need to.

External sources use the same index. An API commit becomes a delta file;
for very large commits the client builds the file itself and commits a
reference to it.

**Engine work.** Compaction is the one heavy computation the engine
itself starts. It runs locally on the engine's machine, on a worker thread
with its own event loop, at most `maintenance_concurrency` at a time. A
project-level setting to offload it to an executor is planned, not built:

```python
Project(..., engine_executor=etl(cpu=2, memory="8GB"))   # not built yet; local today
```

**Local disk cache.** Index files never change once written, so a cached
copy is never stale and needs no invalidation. Attempts on the `Local`
placement share one bounded disk cache of index files, keyed by file
name, evicting least recently used:

```python
Project(..., key_cache=KeyCache(max_size="8GB", path=None))   # the default
Project(..., key_cache=None)                                   # disabled
```

It is on by default whenever the engine's machine has a writable data
directory; `path=None` means a directory next to the engine's `file://`
state, or a temporary directory for `s3://` state. It is what keeps
frequent scattered writes into very large indexes cheap; see
`key-index-costs.md`. Remote placements start cold unless given a volume.

**Implementation.** The file format is ours (no Parquet): a small Rust
extension (PyO3, abi3 wheels, computation only — encoding, decoding,
merging, lookups over fetched bytes), with I/O through obstore, and a
pure-Python implementation of the same format as the reference and
fallback.

## 7. Run history — `history/{table}/*.parquet`

What ran and what it made, kept as Parquet files on the object store and
queried with DuckDB, embedded in the engine. It answers questions like
"failed runs of `site_feed` tagged `env=prod` last week", "how long does
`file_index` take at p95, and how long does it wait for a slot", or "which
input versions built this version of `revenue`".

**Tables.** One row per thing that finished, and one per moment of a run:

| Table | One row per | Notable columns |
|---|---|---|
| `runs` | finished run or source commit | `status` (`succeeded`, `failed`, `canceled`, `skipped`), `trigger` (`manual`, `automation`, `commit`), `automation`, `by`, `source`, `targets`, `assets`, `committed`, `tags` (map), `task_count`, `failed_count`, `error`, `config` and `keys` (JSON, as submitted) |
| `tasks` | task of a finished run | `asset`, `scope`, `status`, `started_at`, `finished_at`, `attempts`, `duration`, `wait` (seconds it could have run but didn't), `deps`, `max_attempts`, `retry_delay`, `retry_backoff`, `retried`, `executor` (of its last attempt) |
| `attempts` | attempt | `task`, `n`, `outcome`, `started_at`, `finished_at`, `duration`, `preparing`, `provisioning`, `importing`, `loading`, `computing`, `writing`, `settling` (seconds per phase, below), `peak_memory` (bytes; only in a process of its own), `cpu_seconds`, `error`, `executor`, `cpu`, `memory`, `gpu` (requested; all null if it never launched), `options` (map: its other placement options, e.g. `image`), `outputs` (map: output → version committed) |
| `run_events` | moment of a run | `n` (its order in the run), `at`, `type`, `task` and `attempt` (null for the run's own events), `by`, `name`, `reason`, `until`, `rows` — the timeline, below |
| `materializations` | output version a commit installed | `output`, `scope`, `version`, `run`, `attempt`, `at`, `batch`, `added`, `removed`, `added_keys`, `removed_keys` (a source commit's keys, up to 1,000), `rows`, `metadata` (JSON) |
| `lineage` | input version an output version was read from | `output`, `scope`, `version`, `input`, `input_scope`, `input_version`, `param` |

A run where every task was skipped — it launched nothing and wrote
nothing — is recorded with status `skipped`. Listings hide skipped runs
unless the filter asks for that status, so a 10-second poller that
usually finds nothing new does not bury the runs that did something.

A source commit that changed something is a `runs` row with trigger
`commit`, plus the `materializations` row of the version it made, which
lists the keys it changed (up to 1,000; past that, only counted). The
source's head points at it (`head.run`).

**No run documents.** The tables are the record: a finished run's detail
— and the run a retry reopens — is rebuilt from its `runs`, `tasks` and
`attempts` rows (a source commit's from its `runs` and `materializations`
rows), and reads the same as it did while the run was live. Only what has
no shape of its own stays JSON: a run's `config` and `keys`, and output
metadata.

**The timeline — `run_events`.** Everything that happened to a run, in
order: the engine's decisions and what the worker reports. The summary
columns of the other tables are computed from it. For example, one
attempt of `revenue`:

```
+0.00  run          submitted        by erwin
+0.00  revenue      ready
+0.02  revenue      held             engine · the engine is at its concurrency
+4.10  revenue #1   claimed
+4.35  revenue #1   launched         etl
+38.9  revenue #1   booted           ip-10-0-3-17          ← by the worker
+40.2  revenue #1   imported
+41.0  revenue #1   loaded           orders · 1,204 rows
+41.1  revenue #1   computing
+47.5  revenue #1   mark             joined                ← ctx.mark("joined")
+52.3  revenue #1   computed
+52.3  revenue #1   writing
+53.9  revenue #1   stored           revenue · 88 rows
+54.0  revenue #1   finished
+54.1  revenue #1   committed
+54.1  revenue      succeeded
+54.1  run          succeeded
```

- **Run events:** `submitted` (`by` the user, or `engine` with `name` the
  automation), `paused`, `resumed`, `canceled`, `retried` (`by`),
  `outage` (the engine was down from `at` to `until`), the run's outcome,
  and a source commit's `committed` (`name` the source).
- **Task events:** `ready`, `retry_scheduled` (`until` its due time),
  `held` (`reason` `lock`, `engine` or `executor`, `name` the attempt
  holding the lock or the full executor — recorded when the reason
  changes, not every tick), `canceled`, and its outcome.
- **Attempt events:** from the engine, `claimed`, `launched` (`name` the
  executor), `taken` (by a pool worker, `name`); from the worker (`by`
  `worker`), `booted` (`name` the host), `imported`, `loaded` and `stored`
  (per input and output, with `rows` when the value has a length),
  `computing`, `mark` (each `ctx.mark(name)`), `computed`, `writing`,
  `finished`; and how it ended, from the engine: `committed`, `skipped`,
  `failed` (`reason` `launch`, `engine`, `conflict`, or none when the
  asset raised), `aborted` (`canceled` or `timeout`), or `lost` (the
  worker exited without a result: `reason` the exit, e.g. `exit code 137`).

The worker keeps its events and sends them with every heartbeat and in
its result, so even a lost attempt keeps what it did up to its last beat.
Its clock is not the engine's: its times are kept within the attempt —
not before its launch, not after its end — and in order.

**Phases** are read off the timeline: from one milestone to the next one
reached — `preparing` (claimed → launched), `provisioning` (launched →
booted), `importing` (booted → imported), `loading` (imported →
computing), `computing` (→ computed), `writing` (computed → finished),
`settling` (finished → the engine's end). A missing milestone folds its
phase into the one before: an attempt lost while computing has
`computing` up to its end, and no `writing`. **Wait** is the time a task
was ready but not running: from `ready` to `claimed`, minus pauses of its
run and engine outages. An engine writes `engine/alive.json` every 30 s
while it has live runs; the next engine to start reads it, and records an
`outage` for each live run from then to now.

Run events are appended as they happen, not when the run finishes: a live
run's timeline is queryable, and a reopened run keeps its events and
numbers the new ones after them.

**Where rows come from.** Rows are born inside `apply`, from the events
that finish things: `RunArchived` yields a run's `runs`, `tasks` and
`attempts` rows; `AttemptFinished` with a commit yields a
`materializations` row per changed output, plus `lineage` rows from the
input versions pinned in the attempt's spec; `SourceCommitted` yields a
`runs` row and a `materializations` row. From there, they are the **lake's**
business.

**The lake** (`lake.py`) is the storage layer under the history: a set of
append-only tables, each a list of Parquet files plus a buffer of rows not
yet written. Nothing above it knows how rows are buffered, flushed, merged
or rewritten; the history only declares its tables (columns, the time
column files are sorted by, the key rows are deleted by), appends rows, and
asks questions in SQL. Its durable part, `State.history`, holds the files
and the buffered rows, each `[seq, values]` with values in the table's
column order, so the checkpoint doesn't repeat column names on every row.

**Flushing.** A flush writes each table's buffered rows as one Parquet
file (zstd, sorted by the table's time column), and `HistoryFlushed`
installs them. It happens once 2,000 rows are buffered or the oldest has
waited a minute, so the checkpoint never carries more than a minute or so
of history.

**Merging.** The lake keeps each file's row count, time range and key
range. Files are grouped in size tiers — up to 1,000 rows, then ×4 per
tier — and four neighbours of one tier are merged into one, up to 1M rows
per file, on a worker thread (`HistoryCompacted`). The file count stays
logarithmic in the history's size: a year of a run per minute (~0.5M runs)
is a few dozen `runs` files.

**Deleting runs.** `RunsDeleted` drops the runs' buffered rows at once and
adds the runs to the `hidden` list of every file whose key range covers
them; queries filter them out (`WHERE run NOT IN (…)`). A file is
rewritten without them after an hour, or as soon as it hides 1,000 runs,
and the old file becomes garbage. A flush or merge that raced a deletion
is discarded rather than installed, so a deleted run never comes back.

**Queries.** A query runs on a worker thread, with one view per table
over three parts:

- the Parquet files, skipped when their time or key range cannot match
  (a query for last week's runs never opens last year's files);
- the buffered rows, mirrored in an in-memory DuckDB table that each query
  brings up to date — dropping what was flushed, adding what was appended —
  so a query copies only the rows that arrived since the last one. The
  query reads the mirror in a transaction of its own: rows that change
  while it runs are not seen, as in any database snapshot;
- for `runs`, `tasks` and `attempts`, the runs still in progress, so a run
  appears in listings the moment it is submitted.

History files never change once written. With `file://` state, DuckDB
reads them in place. With other stores, they are copied once to a local
cache, then read from there.

**Tags and metadata.** Runs carry string tags, from any of three places:

```python
solera run revenue --tag env=prod --tag ticket=OPS-42     # CLI
POST /api/projects/{p}/runs  {"targets": ["revenue"], "tags": {"env": "prod"}}
Automation(trigger=Every(3600), tags={"team": "growth"})   # its runs carry them
```

Assets declare tags too (`@asset(tags={"team": "growth"})`), and a run
filter can keep runs that ran any asset with a given tag. An attempt
records facts about the versions it writes, to chart across versions:

```python
@asset
def orders(ctx):
    rows = fetch()
    ctx.metadata(rows=len(rows), source="shop")           # its only output
    return rows

@asset(outputs=[Output("model"), Output("report")])
def train(ctx):
    return Result(..., metadata={"model": {"auc": 0.91}})  # per output
```

Metadata is a JSON object per output version, at most 64 KB (larger is
dropped, with a warning). `rows` is filled in without asking: the live key
count of a keyed output, else the length of a returned list.

**Reading it.**

| Question | API | CLI |
|---|---|---|
| runs, filtered, newest first | `GET /runs?status=failed&asset=site_feed&tag=env=prod&q=timeout&since=…&limit=50` → `{runs, next, total}` | `solera runs --status failed --asset site_feed --tag env=prod -q timeout` |
| the next page | `GET /runs?…&before={next}` | `solera runs … --before {next}` |
| page 3 of 50, not shifted by newer runs | `GET /runs?…&anchor={newest id of page 1}&offset=100&limit=50` | |
| value counts per filter field | `GET /runs:facets?…` → `{status: [{value, count}], asset, tag, trigger, automation, by, source}` | |
| runs over time, per status | `GET /runs:histogram?…&bars=60` → `{bucket, since, until, bars: [{t, counts}]}` | |
| finished tasks | `GET /tasks?asset=&status=&run=&since=&before=` | |
| p50/p95 duration and wait, failure counts, compute hours, per asset and per executor | `GET /stats?since=&asset=&scope=` | |
| an asset's versions and their metadata | `GET /assets/{name}/history?output=&scope=&before=` | |
| what a version was built from, or what was built from it | `GET /outputs/{name}/lineage?scope=&version=&direction=upstream\|downstream&depth=5` | |

Filter fields combine with AND; repeating one field (`status=failed&status=canceled`)
matches any of its values. A facet counts its values with every *other*
field applied, so the console can show "12 failed, 340 succeeded" while
failed is selected. Histogram buckets are the smallest of 1 min, 5 min,
15 min, 1 h, 3 h, 6 h, 12 h, 1 d, 7 d, 30 d that fit the span in the
requested number of bars. `next` is a cursor: the last run id of the page.

## 8. Attempt files — `runs/{run}/{attempt}.json` and `.log`

**The attempt file** is written twice, by one writer each time: the
engine creates it with `spec` before launching; the harness reads it and,
when it finishes, overwrites it with `spec` + `result` + `log`. Attempt
ids are unique, so no two harnesses ever write the same file. The engine
reads it after the harness exits; a file without `result` means the
harness died.

`spec` is everything the harness needs and the lineage record of what the
attempt read, including the key indexes as pinned (the `KeyIndex` records
of the outputs it writes and the incremental inputs it reads).

```json
{
  "spec": {
    "attempt": "01J8ZB3M…", "run": "01J8ZB3K…", "revision": "c0ffee…",
    "asset": "file_index", "scope": "alpha", "config": {}, "execution": {"kind": "Local"},
    "inputs": {"site_files": {"ref": {"…": "…"}, "index": {"…": "KeyIndex: levels + log[56..57]"},
               "changes": {"from": 56, "to": 57, "after": null, "full": false, "limit": 2}}},
    "prior": {"file_index": {"…": "ref"}},
    "outputs": {"file_index": {"exists": true, "batch": 12, "index": {"…": "KeyIndex: levels only"}}},
    "cursor": null
  },
  "result": {
    "status": "succeeded",
    "outputs": {"file_index": {"ref": {"…": "…"},
                "keys": {"added": 0, "removed": 0, "exact": true, "files": [{"name": "000000000012-01J8ZB3M…", "…": "…"}]}}},
    "delivered": {"site_files": {"after": null, "upserted": ["alpha-file-2"], "deleted": []}},
    "cursor": null,
    "error": null
  },
  "log": {"blocks": [[0, 412, 1790074791.2], [3911, 388, 1790074793.2]], "lines": 800, "bytes": 7702, "truncated": false}
}
```

**Launch and adoption.** The engine writes the attempt file, then
`AttemptLaunched`, then starts the placement. From that event on, the
attempt's claim and its scope lock are durable: an engine that restarts
does not launch the task again, it adopts the attempt — waits for it and
settles it (commits its result, or fails it) as the first engine would
have.

**Heartbeat.** The engine follows an attempt through its placement handle
(a process, an ECS task). After a restart it may have none, so the harness
also rewrites `{attempt}.beat` every `heartbeat_seconds` (30 s), from a
thread so a producer that blocks its event loop still beats, and marks it
done when its result is written. Each beat carries the attempt's timeline
so far (§7). Three missed beats and the worker is dead:
the engine fails the attempt, and the task is retried.

**Write fence — `{attempt}.writing`.** Stores overwrite in place (§9), so
a dead attempt must never write over a live one. One create-only object
decides it:

- The harness creates it — `{"state": "writing", "intents": {…}}`, listing
  the delta files of the keys it is about to change — before its first
  store write. If it already exists, the engine got there first: the
  harness writes nothing and exits.
- The engine creates it — `{"state": "aborted"}` — before it cancels, times
  out or fails a launched attempt. If it already exists, the harness is
  writing: the engine waits for it and commits its result, even on a
  canceled run, since its data has landed.

For example, a run is canceled while `file_index:alpha` computes. The
engine takes the fence first; the harness, done computing, finds it taken
and exits without touching the store. Had the harness taken it first, the
engine would have waited, and the commit would stand.

**Unsettled outputs.** A harness that dies after taking the fence may have
written part of its keyed outputs. The engine fails the attempt with the
fence's intents (`AttemptFinished.unsettled`) and keeps their delta files.
The next attempt on that scope reads the intended keys back from the store
and folds what landed into its own delta — keys it writes itself end as it
says either way — and its commit settles the output, releasing the intent
files. Unkeyed outputs need no repair: the next attempt writes the same
value or batch again. The console shows unsettled outputs.

**The attempt log** is gzip-compressed JSON lines, one line per
`ctx.log(message, level="info", **fields)` call: `{"at", "level", "message", "fields"}`.

- While running, the harness flushes every 2 s or 256 KB, whichever comes
  first. Each flush writes one gzip block as `{attempt}.log.{n}`; the
  console tails a running attempt by reading new chunks.
- When the attempt ends, the harness joins the chunks into
  `{attempt}.log` and deletes them. Concatenated gzip blocks are a valid
  gzip file, so nothing is recompressed and standard tools read it.
- `log.blocks` lists `[byte offset, lines, first timestamp]` per block, so
  the console fetches "the last 200 lines" or a given page with a range
  read of just those blocks.
- Each attempt's log is capped (default 100 MB compressed). Past the cap
  the harness writes a truncation marker, stops shipping, and sets
  `log.truncated`.

## 9. Store contract

```python
class Store(Protocol):
    def can_store(self, t, output) -> bool                  # checked at registration
    def can_load(self, t, selection) -> bool
    async def store(self, write, prior, scope) -> Written   # Written(ref, keys?)
    async def load(self, ref, t, selection) -> Any          # selection: None | Keys | Batches
```

- `Scope` carries the engine-assigned `batch`, the `attempt` id and the
  output's `aliases`. For a keyed output it also says which keys the write
  changes against the key index: `upserts` to write, `removes` to delete.
  Both are `None` when there is no prior (a first write or a `full` run):
  the store then writes everything and deletes whatever else it holds.
  A patch that changes 3 keys of 100,000 reaches the store as 3 upserts.
- `Written.keys` is only for writes the harness never sees as rows
  (§6); for everything else the harness computes keys itself.
- **Reads are not pinned.** A ref names where content lives, and a load
  reads what is there now. A consumer pinned to version 12 that loads after
  version 13 committed gets version 13's content. Keeping one copy is what
  lets data need no expiry; the write fence (§8) is what keeps a dead
  attempt from writing over a live one.
- **Nothing expires.** A store holds the current content of each output,
  nothing older; the engine never deletes data.

**FileStore**, the default, writes what an asset returns as files under
`.solera/data` next to the project file (or `FileStore(path)`, or
`$SOLERA_DATA`) — one object per value, partition, key or batch:

```
rollup.json                       a value
site_status/alpha.json            a value, partition alpha
uploads/u-7.json                  a keyed output: one object per key
site_files/alpha/f-1.json         keyed and partitioned
site_events/alpha/000000000042.json
                                  an unkeyed incremental output: one object per batch
```

Content is JSON when it round-trips exactly, pickle (`.pkl`) otherwise.
Keyed outputs are declared `Output(keyed=True)` and return
`dict[str, Any]`, or rows with `key="id"`. A removed key's object is
deleted. Batches of an unkeyed incremental output accumulate until a
`full` run starts the output over. **S3Store(url)** is the same layout in a
bucket; `Project(default_store=S3Store("s3://…"))` makes it the default.

**PostgresStore** keeps one table per output, shared by its partitions,
and rewrites the rows a write covers.

## 10. Lifecycles

**Writer start.** `LIST control/checkpoints/` → `GET` the newest →
`LIST control/journal/` after its `seq` → `GET` and apply each segment →
create `journal/{seq+1}` with `[WriterStarted]`. If that create fails,
another writer appended: `GET` it, apply it, retry at the next `seq`.
Then adopt every launched attempt (§8); tasks that were preparing are
dispatched again.

**Fencing.** Every segment write is create-only at `seq+1`. A writer
whose create collides reads the colliding segment: if it has another
writer id, it has been replaced and shuts down. Segments a replaced
writer managed to write before the new fence were acknowledged and are
replayed by the new writer, so no acknowledged work is lost.

**Checkpoint.** Written when the journal bytes since the last checkpoint
exceed `max(256 KB, size of the last checkpoint)`, and on clean shutdown.
Write cost stays proportional to the journal volume; restart replays at
most about one checkpoint's worth of journal.

**Journal cleanup.** Immediately after writing a checkpoint, delete every
checkpoint older than the previous one, and every journal segment at or
below the previous checkpoint's `seq`. The previous checkpoint and the
journal after it are kept so a newest checkpoint that turns out
unreadable can be recovered from.

**Run lifecycle.** submit → `RunSubmitted` · attempts → `AttemptFinished`
· terminal → `RunArchived` (its rows join the pending history) · flush →
`HistoryFlushed` · merge → `HistoryCompacted` · retention →
`DELETE runs/{run}/` and `RunsDeleted`.

**Attempt lifecycle.** claim (memory) → pin and write the spec →
`AttemptLaunched` → [pool: `AttemptClaimed`] → the harness takes the
fence, writes, and writes its result → the engine commits it
(`AttemptFinished`). A cancel or timeout takes the fence first and ends the
attempt with `AttemptFinished` (`canceled`, or `failed` and retryable).

## 11. Retention

Policies are set per asset, with a project default:

```python
@asset(retention=Retention(days=7))
@asset(retention=Retention(runs=90))           # the 90 newest runs that committed to it
Project(retention=Retention(days=30))          # default, including source commits
Retention(forever=True)
```

**Current state never depends on runs.** Heads, cursors, watermarks and
key indexes stand on their own; a head keeps its `run` and `attempt`
references even after that run is deleted ("produced 45 days ago, run
expired"). The only runs that cannot be deleted are active ones.
Retention applies to runs only: output data holds no history (§9), so
there is nothing else to expire.

**Runs.** An asset's **horizon** is `now − days`, or the creation time of
the `runs`-th newest run that committed to it; with both, the earlier
(whichever keeps more). Every `retention_interval` (60 s) the engine asks
the history (§7) for those times and for the finished runs older than the
latest horizon, and deletes each run older than the horizon of every asset
it ran: its directory — `DELETE runs/{run}/` — and its history rows
(`RunsDeleted`). A run of an asset that keeps everything is kept; a source
commit or a run with no tasks follows the project default. Nothing about
retention is held in memory.

For example, with `site_feed` at `Retention(days=1)` and `file_index` at
`Retention(runs=90)`: a two-day-old run of only `site_feed` goes; a
two-day-old run of both stays while it is among the 90 newest that
committed to `file_index`.

**Bounding data.** Stores keep only current content, so a keyed output is
bounded by its live keys, and a value by its size. What grows is an
append-only output — FileStore batches, a Postgres event table. Bound it
with a scheduled job, or a periodic `full` run:

```python
@job(deps=["site_events"], automations=Automation(trigger=Every(3600)))
def trim_site_events(db: Database):
    db.execute("DELETE FROM site_events WHERE received_at < now() - interval '7 days'")
```

Only for unkeyed outputs: a keyed output's rows are tracked by its key
index, so it removes keys through its own asset (`Patch(remove=[…])`),
which tells every consumer.

**Manual deletion** goes through the same path and skips only active runs:

```
solera runs delete <run>
solera runs prune [--before DATE] [--asset NAME] [--keep N] [--dry-run]
DELETE /api/projects/{p}/runs/{run}
POST   /api/projects/{p}/runs:prune   {"before", "asset", "keep", "dry_run"}
```

## 12. Removed from earlier drafts

| Removed | Replaced by |
|---|---|
| `epochs/` claim objects | writer id = `seq` of its fence segment |
| sharded, content-addressed checkpoints | one checkpoint object, written when the journal tail exceeds its size |
| `spill/`, `deltas/`, deltas inside attempt results | delta files in the output's key index (§6) |
| key maps held by stores (`solera_keys`, JsonStore folds) and by the engine for sources | one engine-defined key index for every keyed output and source |
| `Page` in the store contract | full delivery pages through the key index and loads with `Keys` |
| separate commit ids and records | commit = `(run, attempt)`; lineage inputs are in the attempt's `spec` |
| separate `spec.json`, `result.json`, per-flush log files | one attempt file; one gzip log per attempt, chunked only while running |
| persisted locks, queue, workers | locks and queue derived from runs and launches; workers re-register |
| claim events for attempts still preparing | a restart dispatches them again; launches are journaled (`AttemptLaunched`) |
| `ref.meta.delta` / `.keys` / `.partitions` | engine fields on `Head`: `batch`, `base`, `count`, `elements` |
| watermark `offset` and `after_key` | one `after` field, plus `full` and `until` |
| refusing a commit whose inputs moved after pinning | the commit stands: it delivered what it pinned |
| automation commit watermark | a pending set of `(asset, scope)` |
| per-automation retention, automation names in run ids | per-asset retention; run id = ULID |
| `runs/{run}/run.json`, the in-memory recent-runs list and run-id listing, `State.retention` | the run history: Parquet tables queried with DuckDB (§7) |
| skipped runs kept only in memory | recorded with status `skipped`, hidden by default |
| pinned runs | nothing but active runs is protected; current state is independent of runs |
| HTTP-only attempt I/O for pool workers | one uniform attempt-file channel |
| JsonStore, BlobStore, per-attempt data objects | FileStore / S3Store: one object per value, partition, key or batch, overwritten in place |
| `store.expire`, data retention | stores hold current content only; retention covers runs |
| per-partition version markers in stores, `StoreConflict`, `StaleRead` | the write fence (§8); reads are not pinned (§9) |
| attempt leases and their sweeps; a restart re-queues running tasks | durable launches: the next engine adopts them (§8) |

## 13. Open questions

1. **Key index parameters**, measured by the prototype against the
   assumptions in `key-index-costs.md`: entry size and compression ratio,
   filter size and false-positive rate, block size, level fanout, the
   read-strategy crossovers, and native vs pure-Python throughput. Targets
   at 1M, 10M and 100M keys: write throughput, a 1K random-key delta
   (requests, bytes, wall time with S3-like latency), a bulk merge, one
   compaction, and a full scan. Run against a local S3-compatible server
   with injected latency; validate once on real S3.

## 14. Implementation plan

Built on PR #8's branch.

| Phase | Deliverable | Gate |
|---|---|---|
| K0 key index | format, pure-Python + Rust implementations, index operations, benchmark report | §13.1 numbers; each implementation reads the other's files and decodes identical content |
| J1 journal | `solera_server/journal.py`: flush, fencing, checkpoint, replay, cleanup | crash between `PUT` and ack; replaced-writer test; replaying the full journal equals checkpoint + tail; passes on `file://` and S3 |
| J2 model | events + `apply`; engine and API on `State`; SlateDB removed | full suite; restart tests; a counting object store asserts 0 GETs on plan and API paths |
| J3 key index | `solera.keys` in the harness, delta files, local compaction + `engine_executor`, `key_cache`, full delivery through the index, sources on the index, aliases | 10k-batch soak: object operations per commit stay flat; `Keys`-only store contract |
| J4 runs and retention | `runs/` layout, archive, per-asset retention, CLI and API | soak with `Retention(days=1)` on a 10-second poller keeps `runs/` bounded |
| H1 run history | Parquet history tables, flush, merge, deletion, `run.json` import; filters, facets, histogram, stats, asset history, lineage; tags and metadata; console runs explorer, asset history and lineage | soak: request count per run stays flat, retention keeps the history bounded |
| J5 fence and stores | durable launches, adoption, heartbeats, write fence, unsettled outputs; FileStore / S3Store; no expiry | a worker killed mid-write is repaired by the next attempt; a restarted engine adopts a running attempt |

## 15. Future ideas

Not built; recorded because DuckDB in the engine makes them cheap.

- **Data preview.** DuckDB reads Parquet, CSV and JSON from S3 or local
  files directly, so the console could show the first rows of an output
  version, its column types, and summary statistics (`SUMMARIZE`) without
  a store-specific reader.
- **A read-only SQL page, and `solera sql`.** Arbitrary queries over the
  history tables, for questions the built-in views do not answer. It must
  be sandboxed: a connection holding only the history views, with
  `enable_external_access=false` so a query cannot read other files or
  URLs, plus memory and time limits.
- **A DuckDB store.** A `Store` that keeps outputs as DuckDB or Parquet
  tables, for assets that are naturally tabular and want SQL between
  steps.
