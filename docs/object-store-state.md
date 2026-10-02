# Object-store state: data model

Status: **built**, except where a section says otherwise; the measured
key index is in `bench/keys/results.md`. The engine's state, the key index
of every keyed output, attempt files, run history and retention.

The bet: the engine runs on an object store alone — no database, no other
infrastructure.

## 0. Constraints

- **Primitives:** `GET` (including range reads), `PUT`, `PUT` create-only,
  `LIST` (lexicographic, with or without a delimiter), `DELETE`. No
  compare-and-swap: obstore's `file://` backend does not implement it
  (verified on 0.11.1), so nothing may depend on it.
- **A create can land unheard.** The object is written, the response is
  lost, and the retry finds it there. (So two writers must never seal the
  same bytes for one name: a writer's fence carries a random nonce.) So every create-only write that
  decides something — a journal segment, a spec, a delta file, a write
  fence — reads back an object in its way: holding exactly the bytes being
  written, it is the writer's own earlier try, and the write succeeded
  (`solera.objects.create`). Only different bytes are another writer's.
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
| Journal segment | `control/journal/{seq:020d}.json` | engine | create-only | the checkpoint before the newest covers it; a writer's fence segment never |
| Checkpoint | `control/checkpoints/{seq:020d}.json` | engine | create-only | two newer checkpoints exist |
| Key index file | `keys/{output}/{scope}/{name}.kx` | harness (delta files), compaction | create-only | no longer in the index and no consumer needs it (§6) |
| History file | `history/{table}/{ulid}.parquet` | engine | create-only | merged into a bigger file, or rewritten without deleted runs (§7) |
| Spec | `runs/{run}/{attempt}.spec` | engine, before `AttemptLaunched` | create-only, immutable | with its run |
| Claim | `runs/{run}/{attempt}.worker` | the invocation that claims the attempt; then its reports while its channel fails | created once, then overwritten by its owner only | with its run |
| Result | `runs/{run}/{attempt}.result` | the claim's owner, once | create-only, immutable, sealed bytes | with its run |
| Gate | `runs/{run}/{attempt}.writing` | the worker about to write, or the engine ending the attempt — whichever is first | create-only | `gate_days` (30) after its run (§8) |
| Engine heartbeat | `engine/alive.json` | engine, every 30 s while runs are live | overwritten | never (one object) |
| Attempt log | chunks `runs/{run}/{attempt}.log.{n:06d}`, every 30 s or 1 MB; the end inside the result | worker | create-only, never joined | with its run |
| Output data | store-defined (FileStore: `{output}/{partition}/{key}/{version}.{generation}.json` under `.solera/data`, §9) | the store, inside the harness | FileStore / S3Store: created once, never overwritten; others: store-defined | FileStore / S3Store: superseded or abandoned objects, by the scope's next attempt once no reader pin predates them (§9); never expired |

**Growth.** `control/` is bounded: at most two checkpoints plus the
journal since the older one — and one fence segment per writer that ever
started —, and a checkpoint is written whenever that
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
    01J8ZB3K…/01J8ZB3M….spec                 ← what to run, immutable
    01J8ZB3K…/01J8ZB3M….worker               ← the claim
    01J8ZB3K…/01J8ZB3M….writing              ← the gate
    01J8ZB3K…/01J8ZB3M….log.000000           ← log chunks
    01J8ZB3K…/01J8ZB3M….result               ← the outcome, immutable
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
| `WriterStarted` | `writer`, `nonce` | first event of every writer; its segment's `seq` becomes the writer id; `nonce` is random, so no two writers' fences have the same bytes |
| `ProjectRegistered` | `revision`, `manifest` | replaces the manifest; applies aliases; reconciles automation state |
| `RunSubmitted` | `run` (id, request, tasks) | adds an active run |
| `RunControlled` | `run`, `action` (`cancel` \| `pause` \| `resume`) | |
| `AttemptLaunched` | `run`, `task`, `attempt`, `started_at`, `pin`, `at`, `execution`, `prepared`, `pool?` | the attempt file exists and a placement is about to start it: its claim and scope lock become durable (§8) |
| `AttemptPlaced` | `attempt`, `handle` | the placement started it: where it runs, for whichever engine follows it (§8) |
| `AttemptFinished` | `run`, `task`, `attempt`, `outcome` (`succeeded` \| `failed` \| `skipped` \| `canceled`), `started_at`, `finished_at`, `error?`, `retryable?`, `commit?`, `unsettled?`, `writes?` | records the attempt; on commit, installs heads, cursor, watermarks, and each keyed output's new delta file; `unsettled` keeps the intents of a writer that died (§8) |
| `SourceCommitted` | `source`, `head`, `keys?`, `at`, `run?` | installs a source head and its delta file; a commit that changed something records `run` in the history (§7) |
| `IndexCompacted` | `output`, `scope`, `added` [file], `removed` [name], `at` | swaps compacted files into a key index |
| `IndexRecounted` | `output`, `scope`, `live`, `pinned_count`, `pinned_inexact` | a recount found `live` keys where the state it scanned said `pinned_count`: the count becomes `live` plus what commits since added, and `inexact` drops by `pinned_inexact` (§6) |
| `IndexTruncated` | `output`, `scope`, `below`, `at` | drops delta log entries below `below` |
| `GarbageDeleted` | `paths` | forgets index files that were deleted |
| `AutomationChanged` | `name`, `enabled` | |
| `AutomationFired` | `name`, `at`, `run` | clears its pending set |
| `RunArchived` | `run`, `at` | drops a finished run from memory; its rows join the pending history (§7) |
| `HistoryFlushed` | `files` {table: file}, `upto` {table: row seq} | installs one Parquet file per table and drops the rows it holds |
| `HistoryCompacted` | `changes` [{`table`, `removed` [path], `added?` file}], `at` | swaps merged or purged files in; the removed ones become garbage |
| `RunsDeleted` | `runs`, `files`, `at` | retires runs for good: drops their pending rows, hides them in files that may hold them, and queues the directories of `files` (runs that launched anything) for deletion (§11) |
| `RunsPurged` | `runs` | their directories are deleted |

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
  seq, writer, applied, revision, manifest         # applied: events applied so far, the model's clock
  heads        {output: {scope: Head}}             # assets and external sources
  indexes      {output: {scope: KeyIndex}}         # keyed outputs and keyed sources (§6)
  cursors      {asset: {scope: json}}
  watermarks   {asset: {edge: {scope: Watermark}}}
  outcomes     {asset: {scope: Outcome}}           # last terminal result per scope
  automations  {name: AutomationState}
  runs         {run: Run}                          # active, or finished and not yet archived
  history      {files: {table: [File]}, rows: {table: [[seq, row], …]}, seq, imported}   # §7
  unsettled    {output: {scope: [Intent, …]}}      # keyed outputs a dead writer may have half-written (§8)
  failures     {asset: {scope: Failures}}          # an Each asset's failing keys (per-key-processing.md §9)
  garbage      [[path, n], …]                      # index and history files nothing references since event n
  retired      [run, …]                            # deleted runs whose directories are still to delete (§11)
```

| Type | Fields | Bounded by |
|---|---|---|
| `Head` | `ref` (from the store), `run`, `attempt` (may point at a deleted run), `batch` (incremental outputs: the last batch that changed it, −1 before any), `base` (unkeyed incremental outputs: the first batch after the last reset), `count` (keyed: live keys), `elements?` (partition sets and set dimensions), `complete`, `version` (declared asset version), `asset`, `at` | outputs × partitions |
| `Failures` | `batch` (the failure index's last batch), `counts` {outcome: keys}, `due` and `epoch_min` (lower bounds), `retry?` {`pass`, `epoch`, `forced_pos`, `after`, `due_acc`, `epoch_acc`}, `passes`, `done_forced`, `last` (`changes` or `retry`), `forced` {class: position} — its index is `indexes["@asset"][scope]` (per-key-processing.md §9) | Each assets × partitions |
| `KeyIndex` | `prefix` (where its files live — kept across renames), `count`, `inexact` (commits since the last recount whose count came from filters; the count is exact at 0), `files` [{`name`, `level`, `min`, `max`, `entries`, `size`, `tail`, `index`}], `log` [[`batch`, [file]], …] — see §6 | a few dozen files per index |
| `Watermark` | `batch` (first batch not fully delivered; during a full drain, the head's batch + 1 when the drain began, so changes made while draining arrive afterwards as deltas), `until` (the last batch of a delta window being delivered in pages), `after` (last key delivered inside the window or the full drain), `full` (a full drain is in progress), `fingerprint`, `output` and `up` (the upstream index it reads) | edges × partitions |
| `Outcome` | `outcome`, `run`, `attempt`, `at` | assets × partitions |
| `AutomationState` | `enabled`, `last_fired`, `last_run`, `last_revision`, `pending` (set of `[asset, scope]` for OnChange) | automations × partitions |
| `Run` | `id`, `request` {targets, partitions, mode, config, keys, automation, tags}, `status`, `paused`, `created_at`, `events` (how many it has recorded), `tasks` {task: `Task`} | in-flight work |
| `Task` | `status`, `deps`, `ready_at` (now, or a retry's due time), `wait` (seconds counted so far), `queued_at` (when the wait clock last started; null while stopped), `held?` [reason, name] (why the dispatcher last passed it over), `max_attempts`, `attempts` [`Attempt`], `launched?` {`attempt`, `started_at`, `pin` (`applied` when it was claimed), `at`, `execution`, `prepared`, `handle?`, `pool?`, `worker?`, `claimed_at?`} | |
| `File` | `path`, `rows`, `bytes`, `at` [lo, hi] (time column), `runs` [first, last], `deleted?` [run] (hidden until rewritten), `deleted_at?` | files per table: ~log(rows) after merging |
| `Intent` | `added`, `removed`, `exact`, `files` (the dead attempt's delta files), `run`, `attempt` | writers that died mid-write, until the next commit of that output |
| `Attempt` | `id`, `outcome`, `started_at`, `finished_at`, the seconds of each phase it reached, `cpu_seconds?`, `peak_memory?`, `error?`, `outputs?` {output: ref} | |

**Derived, rebuilt at start:** the claims and scope locks of launched
attempts (from `Task.launched`), the pool queue, the ready queue and the
dependents index.

**Memory only:** the claim of an attempt still preparing (a restart
dispatches its task again), what each launched attempt's worker reported
(rebuilt from `.worker` after a restart), a cache of key index blocks, and
a local copy of the history files (§7).

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
    "prefix": "keys/site_files/alpha/", "count": 4, "inexact": 0,
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
holds `(key, version, deleted, locator)` entries sorted by key — the
locator is the generation that wrote the key's object (`lifecycle.md`
§9.8) — and a delta's entries also the predecessor `(version, locator)`
of the key they change or delete, which compaction drops.

- **Level 0** holds delta files, one per commit, named by batch, and files
  merged from them. Their key ranges overlap; a file is as recent as the
  newest batch it holds, which leads its name.
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
item, 3.5 B per entry, 0.35% false positives measured — keeping an
item's bits in one 512-bit block costs ~1.8× over independent bits): its
keys, its live `(key, version)` pairs, and its deleted keys. A written `(key, version)` that no pair filter and
no tombstone filter matches, across every file whose key range could hold
the key, is a real change of a live key and needs no block read: the
key's current `(key, version)` is always present in some file. A key no
key filter matches is new. Everything else — unchanged rewrites, keys
that may be deleted, false positives — gets an exact lookup.

**Key count.** `KeyIndex.count` is exact while every commit's reads are
exact, which is always the case for indexes small enough to read whole.
When a commit relies on the filters, a new key that some key filter
falsely matches is counted as an update, so the count drifts low by about
the filters' false-positive rate on inserted keys; `inexact` counts such
commits. While it is non-zero, the engine schedules a **recount** — one
streaming pass over every level, 8 MB of blocks per read: ~370 reads and
14 s at 100M keys, a few more with the upper levels full — at most once per
`recount_interval` (default 1 hour). The recount is exact
for the state it pinned, and commits landing while it runs keep their
`added − removed` on top of it (`IndexRecounted`), so a busy index gets
its exact count back unless one of those commits was itself inexact.
Deltas themselves are always exact; only the count is approximate.

**Engine-resolved commits** (`resolved-commits.md`). A small write is first
offered to the engine over the attempt's channel: it answers from a cache
of the index files on its local disk — exact, no requests — or declines,
and the worker resolves the write itself as below. The same cache keeps
small deltas' entries in memory, so a consumer's next page of a pending
window can come inline in its spec.

**Read strategy** (`resolved-commits.md` §6). A patch whose run holds
more than 2% of the index's physical entries streams the whole index —
every level in 8 MB segments, merged with the sorted run. Otherwise the
sparse reader: newest first, levels up to 32 MB — two range reads, no
more than a tail and a block — are read whole, all at once; from the
first larger level on, it fetches the tails of the files that could hold
the written keys and runs the filters, and reads blocks only for the keys
they cannot clear, in the files whose key filter matched, all levels at
once, each key taking its newest entry. If those block reads would number
more than 16 per streamed segment, it streams instead. An `exact` read
(failure indexes) takes no "changed" verdict from a pair filter.

**Full replacement.** A bare return of every row must compare every live
key, so it reads the whole index — as a stream, never whole. The written
content stays where the worker holds it: an Arrow key column is read in
place (anything with `__arrow_c_stream__`; a pandas DataFrame goes through
DuckDB to get there), Python keys are packed once into one buffer (~28 B
per key). One O(n) pass finds whether they arrive sorted; if not, a
permutation sorts them — bucketed by the two key bytes after the prefix
they all share, each bucket sorted as 12-byte (prefix, row) pairs on every
core, ~4.5 B per key at the peak (bench/keys/results.md, "Sorting the written keys"). A merge-join
then walks the sorted keys and the index's newest-wins view together, each
level fed a segment of 8 MB of consecutive blocks at a time, a few ahead.
A key's version — every key is the group of rows that carry it — is
computed when the join reaches it and compared once: the text of the
declared revision column, which the key's rows share, else the digest of
its rows in the canonical grammar of `row-digest.md`, the same whether
they arrive as Python values or Arrow data (Python rows a window at a time
under the GIL, Arrow rows on every core). New keys
and changed versions go straight into the current delta file, live keys
not written into `deleted` entries, and each file goes to the store as it
fills. Memory is the permutation, a few segments per level and a file or
two in flight: at 100M keys, 0.8 GB on top of the data for shuffled Arrow
rows (0.4 GB sorted; 28 s), 3.4 GB for Python rows, whose keys are packed
(`bench/keys/results.md`).

A `Sql` write's store reports its rows already sorted by key, a chunk at
a time through a server-side cursor — PostgresStore reads the key and the
revision column, or every column without one — and the harness versions
them as it versions any rows, so nothing is sorted or held.

**Operations.**

| Operation | Who | How |
|---|---|---|
| Compute a delta | harness, at write time | Extract `(key, version)` from the written rows with the store's `key_rows` (the declared `revision` column's text, else the 16-byte digest of the key's rows, `row-digest.md`), against the index **as pinned in the spec**. A patch is checked with the filters and the read strategy above: keep entries whose version changed, plus `deleted` entries for removed keys that may exist. A full replacement is the streaming merge-join above. Either way the result is the batch's delta files, split at ~64 MB. |
| Commit | engine | Add the delta file to level 0 and to `log`; `count += added − removed`, and `inexact += 1` if the count change came from filters. The scope lock — one attempt per (asset, scope) from launch to settlement — guarantees the index didn't change underneath. |
| Deliver pending deltas | harness, for an `Incremental` edge | Read the `log` files from the watermark to the head; chunk by `batch_size` in key order; ask the upstream store for those rows with `Keys(…)`. |
| Full delivery | harness | Page through the merged view of all levels from `after`, `batch_size` keys at a time, and ask the store for them with `Keys(…)`. Per level, only the files covering the page are opened, and only their index parts are read — or the whole file, once, when it is small (below one request's latency worth of transfer, ~2.4 MB). A multi-page scan keeps each file's last fetched blocks for the next page, so it reads every block once. |
| Compaction | the engine's machine by default (§6, *Engine work*) | Once level 0 holds ~8 files, merge them into one level-0 file — or, once level 0 holds a tenth of level 1's bytes, into level 1 with the level-1 files it overlaps (all of them, for random keys). A level over its target pushes one file down, merging it with the files it overlaps there. A merge streams, a few segments per input and one output file at a time. Commit with `IndexCompacted`. Each merge into a level rewrites about ten times the bytes it brings: ~20–30× over an entry's life with random keys (`bench/keys/amplification.py`). |
| Truncate the log | engine | Drop `log` entries below the lowest consumer watermark and below every window an in-flight attempt was given (`IndexTruncated`); an output with no `Incremental` consumers keeps none. A consumer whose window the log no longer holds gets a full delivery. |
| Delete files | engine | A file in neither `files` nor `log` joins `garbage`, and is deleted once every attempt that could have pinned it has finished (`GarbageDeleted`): every attempt claimed before the event that let go of it. Both are positions in event order (`applied`), the same in every engine that replays the journal — never wall clocks, which two engines may disagree on. A delta file of an attempt that never committed is deleted when the attempt ends, unless it is an unsettled intent (§8). |

Writes that never pass through the harness as rows — `Sql` materialized
inside Postgres — are the one case where the store must report the written
`(key, version)` pairs, sorted (above); only stores supporting such writes
need to.

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

**Implementation.** The file format is ours (no Parquet). The per-key
work — encoding, decoding, sorting, merging, lookups over fetched bytes —
is Rust, in the `solera._native` extension (PyO3, abi3) that the `solera`
distribution builds and both the engine and the worker require; I/O goes
through obstore. A full replacement, a compaction and a recount run as
streaming jobs (`native/src/jobs.rs`) that ask for the file segments they
need and hand back the files they write; Python only chooses files, fetches
bytes and parses tails (`solera/keys/jobs.py`). Blocks decode and compress
on every core. A pure-Python implementation of the format is kept as the
tests' reference.

## 7. Run history — `history/{table}/*.parquet`

What ran and what it made, kept as Parquet files on the object store and
queried with DuckDB, embedded in the engine. It answers questions like
"failed runs of `site_feed` tagged `env=prod` last week", "how long does
`file_index` take at p95, and how long does it wait for a slot", or "which
input versions built this version of `revenue`".

**Tables.** One row per thing that finished, and one per moment of a run:

| Table | One row per | Notable columns |
|---|---|---|
| `runs` | finished run or source commit | `status` (`succeeded`, `failed`, `canceled`, `skipped`), `trigger` (`manual`, `automation`, `sensor`, `commit`), `automation`, `by`, `retry_of` (the run a retry ran again), `source`, `targets`, `assets`, `committed`, `tags` (map), `task_count`, `failed_count`, `error`, `config` and `keys` (JSON, as submitted) |
| `tasks` | task of a finished run | `asset`, `scope`, `status`, `started_at`, `finished_at`, `attempts`, `duration`, `wait` (seconds it could have run but didn't), `deps`, `max_attempts`, `retry_delay`, `retry_backoff`, `executor` (of its last attempt) |
| `attempts` | attempt | `task`, `n`, `outcome`, `started_at`, `finished_at`, `duration`, `preparing`, `provisioning`, `importing`, `loading`, `computing`, `writing`, `settling` (seconds per phase, below), `peak_memory` (bytes; only in a process of its own), `cpu_seconds`, `error`, `executor`, `cpu`, `memory`, `gpu` (requested; all null if it never launched), `options` (map: its other placement options, e.g. `image`), `outputs` (map: output → version committed), `keys` (map: an `Each` attempt's keys by outcome) |
| `run_events` | moment of a run | `n` (its order in the run), `at`, `type`, `task` and `attempt` (null for the run's own events), `by`, `name`, `reason`, `until`, `rows` — the timeline, below |
| `materializations` | output version a commit installed | `output`, `scope`, `version`, `run`, `attempt`, `at`, `batch`, `added`, `removed`, `added_keys`, `removed_keys` (a source commit's keys, up to 1,000), `rows`, `metadata` (JSON) |
| `lineage` | input version an output version was read from | `output`, `scope`, `version`, `input`, `input_scope`, `input_version`, `param` |
| `key_outcomes` | key an `Each` attempt processed | `run`, `attempt` (`attempts.id`), `asset`, `scope`, `key`, `revision`, `outcome` (`ok`, `removed`, `unmatched`, `rejected`, `failed`, `retrying`, `canceled`, `timed_out`), `error`, `duration`, `at` — per-key-processing.md §10 |

A run where every task was skipped — it launched nothing and wrote
nothing — is recorded with status `skipped`. Listings hide skipped runs
unless the filter asks for that status, so a 10-second poller that
usually finds nothing new does not bury the runs that did something.

A source commit that changed something is a `runs` row with trigger
`commit`, plus the `materializations` row of the version it made, which
lists the keys it changed (up to 1,000; past that, only counted). The
source's head points at it (`head.run`).

**No run documents.** The tables are the record: a finished run's detail
— is rebuilt from its `runs`, `tasks` and
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
run's timeline is queryable.

**A finished run never changes.** Retrying it (`POST runs/{run}/retry`)
submits a new run of its failed and canceled tasks' scopes, and of those
it blocked, with `retry_of` naming it; a run in progress cannot be
retried. Retries inside a running task are its attempts.

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
| every asset at a glance: scopes by status, newest outcome, failing keys, held and unsettled scopes | `GET /assets:status` → `{assets: {name: {partitions, partitioned, last, failures, held, unsettled, updated_at}}}` | |
| an `Each` asset's failing keys, and each scope's failure record | `GET /assets/{name}/failures?scope=&outcome=&after=&limit=100` → `{scopes, keys, epoch, now, next}` | |
| what an `Each` asset's keys came to, newest first | `GET /assets/{name}/key-outcomes?scope=&key=&q=&outcome=&run=&before=&limit=100` → `{outcomes, next}` | |
| why a key is, or is not, in an asset's output (per-key-processing.md §10) | `GET /assets/{name}/explain?key=&scope=&edge=` → `{verdict, patterns, failure, last, last_ok, …}` | |
| an asset's input edges, with every scope's watermark, lag and state | `GET /assets/{name}/edges` | |
| scopes held for an uncertain writer, unsettled outputs, stuck discards (lifecycle.md §9.8, §9.9) | `GET /holds` | `solera scopes release`, `solera scopes discards` |

Filter fields combine with AND; repeating one field (`status=failed&status=canceled`)
matches any of its values. A facet counts its values with every *other*
field applied, so the console can show "12 failed, 340 succeeded" while
failed is selected. Histogram buckets are the smallest of 1 min, 5 min,
15 min, 1 h, 3 h, 6 h, 12 h, 1 d, 7 d, 30 d that fit the span in the
requested number of bars. `next` is a cursor: the last run id of the page.

## 8. Attempt objects — `runs/{run}/{attempt}.*`

The protocol is `lifecycle.md`'s, which this section summarizes as built;
the records the engine and the worker share are `solera/lifecycle.py`.

**Spec, claim, result.** The engine writes `{attempt}.spec` — immutable,
before `AttemptLaunched`. A worker reads it and claims the attempt by
creating `{attempt}.worker` with a random invocation token; the first
create wins. An invocation that loses writes nothing and, unless it is a
pool worker, waits for the owner's result before exiting, so its exit is
never taken for the attempt's. The owner seals its outcome once into
`{attempt}.result`, create-only, retried with the same bytes: its existence
means the worker is done. A worker that cannot publish exits without a
result, as if it had died.

`spec` is everything the worker needs and the lineage record of what the
attempt read, including the key indexes as pinned, plus the engine's URL,
the attempt's token and its generation (the claim's event position).

```json
{
  "attempt": "01J8ZB3M…", "run": {"id": "01J8ZB3K…", "config": {}}, "revision": "c0ffee…",
  "project": "brimstone", "asset": "file_index", "partition": "alpha", "execution": {"kind": "Local"},
  "inputs": {"site_files": {"ref": {"…": "…"}, "index": {"…": "KeyIndex: levels + log[56..57]"},
             "changes": {"from": 56, "to": 57, "after": null, "full": false, "limit": 2}}},
  "prior": {"file_index": {"…": "ref"}},
  "outputs": {"file_index": {"exists": true, "batch": 12, "index": {"…": "KeyIndex: levels only"}}},
  "heartbeat": 10, "engine": "https://solera.example.com", "token": "…", "generation": 184467
}
```

```json
{
  "invocation": "k3v9q2", "status": "succeeded", "writes": "complete",
  "outputs": {"file_index": {"ref": {"…": "…"},
              "keys": {"added": 0, "removed": 0, "exact": true, "files": [{"name": "000000000012-01J8ZB3M…", "…": "…"}]}}},
  "delivered": {"site_files": {"after": null, "upserted": ["alpha-file-2"], "deleted": []}},
  "events": [{"type": "booted", "at": 1790074791.2}, "…"], "usage": {"cpu_seconds": 0.4},
  "log": {"chunks": [[0, 412, 1790074791.2]], "tail": "H4sI…", "lines": 800, "bytes": 3911, "truncated": false}
}
```

**Launch and adoption.** The engine writes the spec, then
`AttemptLaunched`, durable, then starts the placement and records its
handle as `AttemptPlaced` — lazily, riding the next journal segment. From
`AttemptLaunched` on, the attempt's claim and its scope lock are durable:
an engine that restarts adopts it — follows its handle, or finds it again
by name (ECS `clientToken`, the Kubernetes job `solera-{attempt}`), or
follows its worker's reports — and settles it as the first engine would
have. A handle is never given up: a provider that errs, or does not show
the run, cannot tell for now.

**Reports: evidence, never permission.** The worker reaches the engine
over its channel (HTTPS, `lifecycle.md` §5): `start` once, a beat every 10
s, live log lines with their offsets, `finished`. Each answer carries the
cancel record, if any. After two failed beats — or with no engine to
reach, as under the CLI — it reports by overwriting `.worker` every two
beats instead, and reads its gate each time: an `aborted` or `closed` gate
stops it. The engine reads `.worker` only while the channel is quiet. A
worker is silent after three beats without a report over the channel, or
six through `.worker`; a provider's exit ends an attempt unless its owner
still reports over the channel (then it was a duplicate's). None of this
decides whether the attempt's writes may still land: that is the gate's.

**Provisioning and deadlines.** Until its first report — `start`, a beat,
or its claim seen — a worker is provisioning, under a deadline of its own
(`provision_seconds`, 10 min; none for a pool). Its `timeout` runs from the
first report. The engine times attempts on its own monotonic clock; an
adopted attempt still provisioning keeps what the launching engine's clock
says is left of its allowance, between three heartbeats and all of it,
and one already running gets its whole timeout again.

**Cancel, in two phases.** A user cancel, a timeout or the provisioning
deadline latches a cancel record `{phase, reason, since}` (user over
timeout over provisioning). A worker that has reported gets `requested`:
it stops starting work and drains within `cancel_grace` (60 s), publishing
what finished — a plain asset, `canceled` with nothing written, unless it
had already taken its gate, in which case it completes its writes and the
commit stands. Its result carries the record it acted on. After the grace,
or at once for a worker that never reported, the record becomes `forced`:
the engine takes the gate and ends the attempt; a late result is refused.

**The gate — `{attempt}.writing`.** A worker about to write creates it,
`{"state": "writing", "invocation", "intents"}`, listing the delta files
of the keys it will change; finding one already there — `aborted` or
`closed` — it writes nothing. The engine ending an attempt without a
result creates it `aborted`, and what it finds is the attempt's
write-completion evidence: winning means `none`; finding `writing` means
`uncertain`. A result says its own: `none` (no store call), `complete`
(every store call returned) or `uncertain` (one raised, or was
abandoned). An attempt that ends with nothing written and no gate gets one
`closed`.

**Gates outlive their runs.** A worker that read its spec, paused, and
resumes after its run was deleted must still find its gate: retention
deletes a run's objects except its gates, notes them under
`control/gates/{day}/`, and deletes them `gate_days` (30) later.

**Unsettled outputs.** An attempt that ends with its gate `writing` may
have written part of its keyed outputs. The engine fails it with the
gate's intents (`AttemptFinished.unsettled`) and keeps their delta files.
The next attempt on that scope reads the intended keys back from the store
and folds what landed into its own delta, and its commit settles the
output. Unkeyed outputs need no repair: the next attempt writes the same
value or batch again.

**The attempt log** is gzip-compressed JSON lines, one per `ctx.log(…)`:
`{"at", "level", "message", "fields"}`. Lines go live to the engine within
a second (the console tails a running attempt from there), and durably as
create-only chunks `{attempt}.log.{n:06d}` every 30 s or 1 MB, never
joined. At the end, lines not yet in a chunk travel inside the result
(`log.tail`, gzipped, under 64 KB), else as one last chunk: a short
attempt writes no log object at all. `log.chunks` lists `[n, lines, first
timestamp]`, so the last 200 lines are the tail and the last few chunks.
Past 100 MB compressed a truncation marker is written and shipping stops.

## 9. Store contract

```python
class Store(Protocol):
    def can_store(self, t, output) -> bool                  # checked at registration
    def can_load(self, t, selection) -> bool
    async def store(self, write, prior, scope) -> Written   # Written(ref, keys?)
    async def load(self, ref, t, selection) -> Any          # selection: None | Keys | Batches
    writes: str = "overwrite"                               # or "immutable", "fenced" (lifecycle.md §9.6)
```

- `Scope` carries the engine-assigned `batch`, the `attempt` id, its
  `generation` and `invocation` (`lifecycle.md` §9.7–9.8) and the
  output's `aliases`. For a keyed output it also says which keys the write
  changes against the key index: `upserts` to write, `removes` to delete.
  Both are `None` when there is no prior (a first write or a `full` run):
  the store then writes everything and deletes whatever else it holds.
  A patch that changes 3 keys of 100,000 reaches the store as 3 upserts.
- `Written.keys` is only for writes the harness never sees as rows
  (§6); for everything else the harness computes keys itself.
- **Reads are pinned by immutable stores only.** FileStore and S3Store
  never overwrite, so a load reads the version its consumer pinned; a
  superseded object lingers until no reader pin predates it. A store that
  overwrites (PostgresStore, user stores) holds one copy: a consumer pinned
  to version 12 that loads after version 13 committed gets version 13's
  content.
- **Nothing expires.** A store holds the current content of each output,
  plus, for an immutable store, what pinned readers still need.

**FileStore**, the default, writes what an asset returns as files under
`.solera/data` next to the project file (or `FileStore(path)`, or
`$SOLERA_DATA`) — one object per value, partition, key or batch:

```
rollup@184467.json                a value, by generation 184467
site_status/alpha@184467.json     a value, partition alpha
uploads/u-7/9c41e0d2….184467.json a keyed output: one object per key and version
site_files/alpha/f-1/9c41….json   keyed and partitioned
site_events/alpha/000000000042.184467.json
                                  an unkeyed incremental output: one object per batch
```

Every name carries the **generation** of the attempt that wrote it (its
claim's event position), so no two attempts write one name and objects
are created once, never overwritten (`lifecycle.md` §9.8). A keyed load
names its objects from the index's `(version, locator)`; a whole keyed
read is paged from the pinned index. Content is JSON when it round-trips
exactly, pickle (`.pkl`) otherwise. Keyed outputs are declared
`Output(keyed=True)` and return `dict[str, Any]`, or rows with
`key="id"`. A superseded or removed key's object, a value's previous
object and an abandoned attempt's objects are deleted by the scope's next
attempt (`store.discard`) once no reader pin predates them. Batches of an
unkeyed incremental output accumulate until a `full` run starts the
output over; a batch's committed object is its highest generation. **S3Store(url)** is the same layout in a
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

The slot a replaced writer collides in is always its successor's fence,
so fence segments are kept for good. Were cleanup to delete one, say
writer 1001 fenced by 1042 that has since checkpointed past 1042, then
1001's next create at 1042 would succeed: its events acknowledged,
replayed by no one. Each checkpoint lists the fences (`fences`), and
cleanup skips them.

**Checkpoint.** Written when the journal bytes since the last checkpoint
exceed `max(256 KB, size of the last checkpoint)`, and on clean shutdown.
Write cost stays proportional to the journal volume; restart replays at
most about one checkpoint's worth of journal.

**Journal cleanup.** Immediately after writing a checkpoint, delete every
checkpoint older than the previous one, and every journal segment at or
below the previous checkpoint's `seq` except fence segments. The previous checkpoint and the
journal after it are kept so a newest checkpoint that turns out
unreadable can be recovered from.

**Run lifecycle.** submit → `RunSubmitted` · attempts → `AttemptFinished`
· terminal → `RunArchived` (its rows join the pending history) · flush →
`HistoryFlushed` · merge → `HistoryCompacted` · retention →
`RunsDeleted`, durable → `DELETE runs/{run}/` → `RunsPurged`.

**Attempt lifecycle.** claim (memory) → pin and write the spec →
`AttemptLaunched` → launch → `AttemptPlaced` → the worker claims
(`.worker`), starts, takes the gate, writes, and seals its result → the
engine commits it (`AttemptFinished`). A cancel or timeout is requested,
drained, then forced: the engine takes the gate and ends the attempt with
`AttemptFinished` (`canceled`, or `failed` and retryable) (§8).

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
it ran: its history rows and its directory. A run of an asset that keeps
everything is kept; a source commit or a run with no tasks follows the
project default. Nothing about retention is held in memory.

**Retire, then delete.** Deleting a run is irreversible, so it is recorded
first: `RunsDeleted` hides the run from the history for good and lists it
in `retired`; once that is durable, `DELETE runs/{run}/`, then
`RunsPurged`. An engine replaced meanwhile cannot make `RunsDeleted`
durable, so it deletes nothing; an engine that stops between the two picks
the deletion up again from `retired`. Retirement skips runs that are
active, and a finished run never becomes active again.

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

## 12. Open questions

1. **The key index on real S3.** Its parameters and costs are measured at
   1M, 10M and 100M keys against MinIO with 30 ms injected per request and
   80 MB/s per connection (`bench/keys/results.md`, compared with the model
   in `key-index-costs.md` → Measured). Request counts and bytes carry over
   to S3 exactly; wall times rest on the injected latency and bandwidth,
   which nobody has checked against S3. One run of
   `bench/keys/bench.py --s3 s3://bucket/prefix --latency 0 --bandwidth 0`
   from a worker in the bucket's region settles it.

## 13. Future ideas

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
