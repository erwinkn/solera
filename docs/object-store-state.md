# Object-store state: data model

Status: **implemented** (K0, J1–J4), except where a section says otherwise. Replaces the SlateDB persistence and the
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
| Run record | `runs/{run}/run.json` | engine | written once, when the run ends | retention (§11) |
| Attempt file | `runs/{run}/{attempt}.json` | engine creates it with the spec; the harness overwrites it with spec + result + log index | two writes, one writer each | with its run |
| Attempt log | `runs/{run}/{attempt}.log` (chunks `{attempt}.log.{n:06d}` while running) | harness | chunks write-once; joined at the end | with its run |
| Output data | store-defined (JsonStore: `data/{output}/{scope}/…`, named after the writing attempt) | the store, inside the harness | store-defined | `store.expire` (§9) |

**Growth.** `control/` is bounded: at most two checkpoints plus the
journal since the older one, and a checkpoint is written whenever that
journal reaches the size of the last checkpoint (§10) — so `control/`
stays under about three times the engine's state size. `keys/` is bounded
by live keys plus unconsumed deltas. What grows over time is `runs/`,
bounded by retention, and user data.

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
  runs/
    01J8ZB3K…/run.json
    01J8ZB3K…/01J8ZB3M….json                 ← attempt: spec, then spec + result
    01J8ZB3K…/01J8ZB3M….log                  ← gzip blocks
  data/…                                     ← JsonStore / BlobStore
```

## 2. Identifiers

| Id | Form | Notes |
|---|---|---|
| run | `{ulid}` | sorts by time and embeds its creation time; carries no names, so renames never orphan history |
| task | `{asset}:{scope}` | unique within its run |
| attempt | `{ulid}` | globally unique; names delta files and data objects, so a zombie attempt can never overwrite a committed one |
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

One object per flush. The engine flushes when events are pending and
either 1 s has passed or 1 MB is buffered. Anything that must be
acknowledged as durable (a commit, a run submission, a source commit)
waits for the flush that contains it.

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
| `AttemptFinished` | `run`, `task`, `attempt`, `outcome` (`succeeded` \| `failed` \| `skipped` \| `expired`), `started_at`, `finished_at`, `error?`, `retryable?`, `commit?` | records the attempt; on commit, installs heads, cursor, watermarks, and each keyed output's new delta file |
| `SourceCommitted` | `source`, `head`, `keys?`, `at` | installs a source head and its delta file |
| `IndexCompacted` | `output`, `scope`, `added` [file], `removed` [name], `recount?`, `at` | swaps compacted files into a key index; a recount replaces its count |
| `IndexTruncated` | `output`, `scope`, `below`, `at` | drops delta log entries below `below` |
| `GarbageDeleted` | `paths` | forgets index files that were deleted |
| `AutomationChanged` | `name`, `enabled` | |
| `AutomationFired` | `name`, `at`, `run` | clears its pending set |
| `RunArchived` | `run` | drops a finished run from memory once `run.json` is written |

A claim is **not** an event: attempt ids are unique, so after a restart a
harness that was running reports into an attempt the engine no longer
knows, and its result is rejected. The task is simply re-queued.

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
  retention    {asset: [run, …]}                   # finite policies only, oldest first (§11)
  runs         {run: Run}                          # active, or finished and not yet archived
  garbage      [[path, at], …]                     # index files no index references any more
```

| Type | Fields | Bounded by |
|---|---|---|
| `Head` | `ref` (from the store), `run`, `attempt` (may point at a deleted run), `batch` (incremental outputs: the last batch that changed it, −1 before any), `base` (unkeyed incremental outputs: the first batch after the last reset), `count` (keyed: live keys), `elements?` (partition sets and set dimensions), `complete`, `version` (declared asset version), `asset`, `at` | outputs × partitions |
| `KeyIndex` | `prefix` (where its files live — kept across renames), `count`, `count_exact`, `files` [{`name`, `level`, `min`, `max`, `entries`, `size`, `tail`, `index`}], `log` [[`batch`, [file]], …] — see §6 | a few dozen files per index |
| `Watermark` | `batch` (first batch not fully delivered; during a full drain, the head's batch + 1 when the drain began, so changes made while draining arrive afterwards as deltas), `until` (the last batch of a delta window being delivered in pages), `after` (last key delivered inside the window or the full drain), `full` (a full drain is in progress), `fingerprint`, `output` and `up` (the upstream index it reads) | edges × partitions |
| `Outcome` | `outcome`, `run`, `attempt`, `at` | assets × partitions |
| `AutomationState` | `enabled`, `last_fired`, `last_run`, `last_revision`, `pending` (set of `[asset, scope]` for OnChange) | automations × partitions |
| `Run` | `id`, `request` {targets, partitions, mode, config, keys, automation}, `status`, `paused`, `created_at`, `tasks` {task: `Task`} | in-flight work |
| `Task` | `status`, `deps`, `ready_at`, `max_attempts`, `attempts` [`Attempt`] | |
| `Attempt` | `id`, `outcome`, `started_at`, `finished_at`, `error?`, `outputs?` {output: ref} | |

**Memory only, rebuilt at start:** leases and scope locks (every lock is
treated as expired on restart), the ready queue, the dependents index,
registered workers (they re-register on their next heartbeat), a cache of
key index blocks, and the recent-runs list for the console.

Example (abridged):

```json
{
  "seq": 1040, "writer": 1001, "revision": "c0ffee…", "manifest": {"…": "…"},
  "heads": {"site_files": {"alpha": {
    "ref": {"output": "site_files", "store": "json", "partition": "alpha", "version": "8f35…",
            "handle": {"mode": "keyed", "prefix": "data/site_files/alpha/", "batches": [0, 57]}},
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
  "retention": {"site_feed": ["01J8Y…", "01J8Z…"]},
  "runs": {"01J8ZC7S…": {"…": "Run"}}
}
```

## 6. Key index

One per keyed output (or keyed external source) and partition. The format
and operations live in an SDK library (`cursus.keys`) used by the harness,
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
| Commit | engine | Add the delta file to level 0 and to `log`; `count += added − removed`. The existing commit check — head unchanged since the attempt was claimed — guarantees the index didn't change underneath. |
| Deliver pending deltas | harness, for an `Incremental` edge | Read the `log` files from the watermark to the head; chunk by `batch_size` in key order; ask the upstream store for those rows with `Keys(…)`. |
| Full delivery | harness | Page through the merged view of all levels from `after`, `batch_size` keys at a time, and ask the store for them with `Keys(…)`. Per level, only the files covering the page are opened, and only their index parts are read. |
| Compaction | the engine's machine by default (§6, *Engine work*) | When level 0 exceeds ~8 files, merge it with the overlapping level-1 files into new level-1 files, cascading down; commit with `IndexCompacted`. |
| Truncate the log | engine | Drop `log` entries below the lowest consumer watermark and below every window an in-flight attempt was given (`IndexTruncated`); an output with no `Incremental` consumers keeps none. A consumer whose window the log no longer holds gets a full delivery. |
| Delete files | engine | A file in neither `files` nor `log` joins `garbage`, and is deleted once every attempt that could have pinned it has finished (`GarbageDeleted`). A delta file of an attempt that never committed is deleted when the attempt ends. |

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
Project(..., engine_executor=ecs(cpu=2, memory="8GB"))   # not built yet; local today
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

## 7. Run record — `runs/{run}/run.json`

The finished `Run` from §5, written once when the run reaches a terminal
status, before `RunArchived`. If the engine crashes in between, the run is
still in the checkpoint and gets archived again (the write is idempotent).

```json
{
  "id": "01J8ZB3K…",
  "request": {"targets": ["file_index"], "partitions": "all", "mode": "incremental",
              "config": {}, "keys": null, "automation": null},
  "status": "succeeded", "created_at": 1790074790.0, "finished_at": 1790074800.0,
  "tasks": {
    "file_index:alpha": {"status": "succeeded", "attempts": [
      {"id": "01J8ZB3M…", "outcome": "succeeded", "started_at": 1790074791.0, "finished_at": 1790074800.0,
       "outputs": {"file_index": {"output": "file_index", "store": "json", "version": "…", "handle": {"…": "…"}}}}]},
    "file_index:bravo": {"status": "skipped", "attempts": [{"id": "01J8ZB3N…", "outcome": "skipped"}]}
  }
}
```

A source commit that changes something is recorded as a run with no
tasks. Its `run.json` holds only what nothing else says:

```json
{"id": "01J9QX…", "source": "uploads", "by": "sharepoint-webhook", "batch": 12,
 "upserted": ["u-7"], "deleted": []}
```

The time is the id's (a ULID). `by` is whatever the caller passed, else
the channel (`api`, `cli`). `upserted` and `deleted` list the changed keys,
or count them past 1,000. An unkeyed source records `version` instead. The
source's head points at the run (`head.run`). Runs submitted by hand carry
the same `by` in their request; automation runs name their `automation`.

A run where every task was skipped launched nothing and wrote nothing, so
it is **not archived**; it only appears in the console's in-memory recent
list.

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
    async def store(self, write, prior, scope) -> Written   # Written(ref, keys?)
    async def load(self, ref, t, selection) -> Any           # selection: None | Keys | Batches
    async def expire(self, head, before) -> None             # delete what no version written after `before` needs
```

- `Scope` carries the engine-assigned `batch`, the `attempt` id and the
  output's `aliases`. Stores put the attempt id in object names, so no
  attempt overwrites another's objects. JsonStore writes
  `data/{output}/{scope}/{batch // 1000}/b{batch}-{attempt}.json` (and
  `s…` snapshots, `v{attempt}.json` for values); when attempts wrote the
  same batch, the newest attempt's object is the committed one — a later
  attempt only gets that batch number while it is still uncommitted.
- `Written.keys` is only for writes the harness never sees as rows
  (§6); for everything else the harness computes keys itself.
- `expire` is optional; it is how data retention reaches the store (§11).
  The harness calls it with the committed head before writing that output.
  JsonStore dates each object by the attempt id in its name (a ULID) and
  deletes what no version written after `before` needs: older values, keyed
  batches below the snapshot the oldest retained version folds from, event
  log batches written before `before` (an event log's content is the batches
  it retains), and objects of attempts that never committed. Postgres keeps
  no old versions and has no `expire`. The head always stays loadable.

## 10. Lifecycles

**Writer start.** `LIST control/checkpoints/` → `GET` the newest →
`LIST control/journal/` after its `seq` → `GET` and apply each segment →
create `journal/{seq+1}` with `[WriterStarted]`. If that create fails,
another writer appended: `GET` it, apply it, retry at the next `seq`.
Then re-queue every task that was running.

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
· terminal → write `run.json` → `RunArchived`.

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

**Run records.** For each asset with a `runs` policy, the engine keeps the
ids of the newest `runs` runs in which it succeeded (`State.retention`).
An asset's **horizon** is `now − days`, or the time of the `runs`-th newest
of those runs; with both, the earlier (whichever keeps more). Every
`retention_interval` (60 s) the engine deletes — `DELETE runs/{run}/` —
each finished run older than the horizon of every asset it ran. A run of
an asset that keeps everything is kept. The engine lists `runs/` once when
it starts and keeps that list in memory.

**Data versions.** An attempt of an asset with a finite policy carries
its horizon in its spec (`retention.before`); before writing an output, the
harness calls `store.expire(head, before)` with the committed head. Every
version written after the horizon still loads; the head always does,
whatever becomes of the attempt's own commit.

**Table stores don't version.** Retention reaches data through
`store.expire`, and a store without one (Postgres) keeps its tables as they
are. To bound an append-only table, schedule a job:

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
cursus runs delete <run>
cursus runs prune [--before DATE] [--asset NAME] [--keep N] [--dry-run]
DELETE /api/projects/{p}/runs/{run}
POST   /api/projects/{p}/runs:prune   {"before", "asset", "keep", "dry_run"}
```

## 12. Removed from earlier drafts

| Removed | Replaced by |
|---|---|
| `epochs/` claim objects | writer id = `seq` of its fence segment |
| sharded, content-addressed checkpoints | one checkpoint object, written when the journal tail exceeds its size |
| `spill/`, `deltas/`, deltas inside attempt results | delta files in the output's key index (§6) |
| key maps held by stores (`cursus_keys`, JsonStore folds) and by the engine for sources | one engine-defined key index for every keyed output and source |
| `Page` in the store contract | full delivery pages through the key index and loads with `Keys` |
| separate commit ids and records | commit = `(run, attempt)`; lineage inputs are in the attempt's `spec` |
| separate `spec.json`, `result.json`, per-flush log files | one attempt file; one gzip log per attempt, chunked only while running |
| persisted locks, leases, queue, workers | memory only; rebuilt or re-registered |
| claim events | unique attempt ids make unknown attempts harmless |
| `ref.meta.delta` / `.keys` / `.partitions` | engine fields on `Head`: `batch`, `base`, `count`, `elements` |
| watermark `offset` and `after_key` | one `after` field, plus `full` and `until` |
| refusing a commit whose inputs moved after pinning | the commit stands: it delivered what it pinned |
| automation commit watermark | a pending set of `(asset, scope)` |
| per-automation retention, automation names in run ids | per-asset retention; run id = ULID |
| pinned runs | nothing but active runs is protected; current state is independent of runs |
| HTTP-only attempt I/O for pool workers | one uniform attempt-file channel |

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
| J1 journal | `cursus_server/journal.py`: flush, fencing, checkpoint, replay, cleanup | crash between `PUT` and ack; replaced-writer test; replaying the full journal equals checkpoint + tail; passes on `file://` and S3 |
| J2 model | events + `apply`; engine and API on `State`; SlateDB removed | full suite; restart tests; a counting object store asserts 0 GETs on plan and API paths |
| J3 key index | `cursus.keys` in the harness, delta files, local compaction + `engine_executor`, `key_cache`, full delivery through the index, sources on the index, aliases | 10k-batch soak: object operations per commit stay flat; `Keys`-only store contract |
| J4 runs and retention | `runs/` layout, archive, per-asset retention, `store.expire`, CLI and API | soak with `Retention(days=1)` on a 10-second poller stays bounded; a consumer added after expiry receives the full head |
