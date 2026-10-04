# Object-store state: data model

Status: **built**, except where a section says otherwise; the measured
key index is in `bench/keys/results.md`. The engine's state, the key index
of every keyed output, attempt files, run history and retention.

The bet: the engine runs on an object store alone — no database, no other
infrastructure.

## 0. Constraints

- **Primitives:** `GET` (including range reads), `PUT`, `PUT` create-only
  (`If-None-Match: *`), `PUT` if-match (`If-Match` on an ETag: a
  compare-and-swap), `LIST` (lexicographic, with or without a delimiter),
  `DELETE`. The store holding the engine's state must support `If-Match`:
  S3 (since November 2024), GCS, R2, MinIO and Railway's buckets do
  (`bench/keys/results.md`, "A Railway bucket"), and so does obstore's
  `MemoryStore`. obstore's `file://` backend does not (0.11.1), so
  `solera.objects` implements it there with a lock (below). Output
  stores (§9) need none of this.
- **A create can land unheard.** The object is written, the response is
  lost, and the retry finds it there. So every create-only write that
  decides something — a spec, a delta file, a write fence — reads back an
  object in its way: holding exactly the bytes being written, it is the
  writer's own earlier try, and the write succeeded
  (`solera.objects.create`). Only different bytes are another writer's.
- **So can a swap.** `solera.objects` has two calls for objects that
  are overwritten:
  - `read(store, path) -> (data, etag)`, or `None` if there is no object.
  - `swap(store, path, data, etag) -> etag` writes `data` only if the
    object's ETag is still `etag` (`None`: only if there is no object),
    and returns the new ETag.

  A swap that is refused (412; or 409, which S3 answers to one of two
  concurrent conditional writes), or whose answer is lost, `read`s the
  object to find out what happened:
  - it holds exactly `data`: the write landed, and `swap` returns that
    ETag;
  - its ETag is still `etag`: nothing landed, and `swap` writes again;
  - anything else: `swap` raises `Conflict`, because another writer's
    write is there.

  So no two writes may produce the same bytes for one object. Every body
  written by `swap` names its writer, and never repeats (the journal's:
  §10).
- **`file://` has no `If-Match`, so `swap` takes a lock.** It takes
  `fcntl.flock` on the object's directory (no file of its own) and
  compares the file's SHA-256, which serves as the ETag there, with
  `etag`. Then it writes a temporary file,
  fsyncs it, `os.replace`s it over the object, and unlocks. The kernel
  drops the lock when its process dies, so a crash leaves nothing to clean
  up. A lock file created with `O_EXCL` instead would outlive a crash, and
  breaking it would need a timeout. This works on one machine only, not
  over NFS: `file://` is for local development.
- **One writer per namespace.** The engine's in-memory state is the source
  of truth; storage is written to, and read only when a writer starts.
- **The engine owns keys; stores own rows.** Every key → generation index,
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
| Journal | `control/journal.json` | engine | swapped (`If-Match`) on every flush | never (one object) |
| Checkpoint | `control/checkpoints/{engine}-{n:06d}.json` | engine | create-only | once the journal has moved to a newer one (§10) |
| Key index file | `keys/{output}/{partition}/{name}.kx` | worker (delta files), compaction | create-only | no longer in the index and no consumer needs it (§6) |
| History file | `history/{table}/{ulid}.parquet` | engine | create-only | merged into a bigger file, or rewritten without deleted runs (§7) |
| Spec | `runs/{run}/{attempt}.spec` | engine, before `AttemptLaunched` | create-only, immutable | with its run |
| Control file | `runs/{run}/{attempt}.control` | the engine creates it `open` before the launch; then the owner (`owned`, `writing`, `sealed` with its result) or the engine (`ended`) | created once, then only swapped (`If-Match`) | with its run |
| Beat | `runs/{run}/{attempt}.beat` | the owner, while its channel fails | overwritten | with its run |
| Engine heartbeat | `engine/alive.json` | engine, every 30 s while runs are live | overwritten | never (one object) |
| Attempt log | chunks `runs/{run}/{attempt}.log.{n:06d}`, every 30 s or 1 MB; the end inside the result | worker | create-only, never joined | with its run |
| Output data | store-defined (FileStore: `{output}/{partition}/{key}/{generation}.json` under `.solera/data`, §9) | the store, inside the worker | FileStore / S3Store: created once, never overwritten; others: store-defined | FileStore / S3Store: superseded or abandoned objects, by the partition's next attempt once no reader pin predates them (§9); never expired |

**Growth.** `control/` is bounded: the journal, at most
`max(64 KB, a sixteenth of the checkpoint)`, plus the checkpoint it names
(§10). A checkpoint left over by an engine that crashed or was fenced
before its cleanup goes with the next engine's cleanup. So `control/`
stays a little over the engine's state size. `keys/` is bounded
by live keys plus unconsumed deltas. What grows over time is `runs/` and
`history/`, both bounded by retention, and user data, which holds only
current content.

```
{root}/{namespace}/
  control/
    journal.json                             ← the engine id, the checkpoint, the events since
    checkpoints/7f3a9c0e5b21d846-000012.json ← the checkpoint the journal names
  keys/
    site_files/alpha/000000000057.kx         ← delta file of commit 57
    site_files/alpha/c01J8ZE2….kx            ← compacted file
    uploads/_/000000000003.kx                ← external source
  history/
    runs/01J9A2….parquet                     ← merged: 4,000 runs
    runs/01J9C7….parquet                     ← one flush
    tasks/…  attempts/…  commits/…  lineage/…
  runs/
    01J8ZB3K…/01J8ZB3M….spec                 ← what to run, immutable
    01J8ZB3K…/01J8ZB3M….control              ← the owner, the gate, the sealed result
    01J8ZB3K…/01J8ZB3M….log.000000           ← log chunks
```

Output data lives wherever its store puts it: FileStore under
`.solera/data` next to the project file, S3Store in its own bucket.

## 2. Identifiers

| Id | Form | Notes |
|---|---|---|
| run | `{ulid}` | sorts by time and embeds its creation time; carries no names, so renames never orphan history |
| task | `{asset}:{partition}` | unique within its run |
| attempt | `{ulid}` | globally unique; names delta files and attempt files |
| commit | `(run, attempt)` | no separate commit id or record |
| commit number | integer per (output, partition) | engine-assigned, starts at 0 |
| engine id | 16 random hex characters, per process | names the journal's writer (§10); no two processes share one |

**Renames.** `@asset(aliases=["old_name"])`. On registration the engine
moves everything held under an alias to the current name: its partition
records (cursor, outcome, completeness, positions, failing keys — one
record per partition, moved whole), heads, key indexes, automation state
(attached automations are named after their asset), retention lists. Outputs named after the
asset follow. Stores never rename anything: the committed head's ref
says where the content is (a FileStore directory, a Postgres table), so
a renamed output keeps its storage, and every ref to it stays readable;
the declaration names the storage only of a first write.

**Removals and moves reset.** A deploy that removes an asset, or removes
an output or declares it on another store than before (no alias carrying
it over), resets it: what comes back under that name, or what the new
store holds, is a new one (K10). At that deploy, not later:

- its heads, key indexes (their files go to collection) and repair intents
  go;
- every position that reads it goes, and every position of the asset that
  writes it: its consumers re-read it from scratch, and its asset reads
  each of its inputs in a full pass. A `keys=` run of a partition that lost
  positions this way reads that full pass too, to its last batch, before
  it succeeds;
- a removed asset loses its partition records — cursor, positions, failed
  keys — a job's too, which has no output (F21).

An attempt launched before the reset commits nothing of it: one of a
reset asset, or that writes a reset output or reads one incrementally, is
refused at commit, as a stale head is (`reset_at` in the model, the deploy
number of the last reset, by output and by asset, against the one
the attempt launched under), and its task tries again under the new one. So nothing waits for an attempt in
flight. History keeps the old records, and pending cleanups stay, still
owed. Moving away and back with nothing written in between is two
resets all the same. Which store holds an output is therefore not in the
fingerprint, only its store's version.

What an earlier life left in a store is never read as the new one's, even
before cleanup removes it: reads are bounded by the head, and generations
only grow. FileStore keeps a name's objects in one place, so life 3 of
`log` writes commits 0–2 where life 1 wrote 0–9; a read lists only the
head's commits `first..last` and takes each one's highest generation —
life 3's — and a keyed read names only the objects its fresh key index
holds. Findings this closes: F12 (`copy` renamed to `mirror` and back kept
`mirror`'s first life), F13 (a key removed after a move stayed
downstream), F17 (moved away and back, an output lost keys), F19 (removed
while its attempt ran and added back, an asset resumed its first life), F21 (the same, for a job).

**An asset change leaves the asset due.** A deploy that adds an asset
(new, or added back), renames it, changes its declaration (its version,
deps, inputs and their patterns, outputs and their stores' versions) or
resets it is an **asset change** (`changed_at` in the model, by asset).
Its automations then decide, each by its own criterion:

- `OnChange` owes one firing, as for a change of the asset's own, per
  current partition whose inputs have heads (for a
  fan-in, a materialized upstream partition): built at once, not when its
  upstream next changes (F22). The engine works this out once, right after
  recording the deploy (`FiringsOwed`); no tick checks it again, so a run
  of it that fails is not submitted on every tick (F20). After a rename the
  firing is a skip: the state carried over is caught up.
- A schedule fires at its next time — a new one too, counted from when it
  was declared, never at once;
  `OnDeploy` and sensors keep their criteria.
- With no automation nothing runs. The partition shows `stale` —
  materialized, but caught up (`caught_up_at`) before its asset's last
  change — until a run catches it up: the console and CLI show it, for a
  run by hand.

## 3. Journal

One object, `control/journal.json`, rewritten whole by every flush: the
engine id of its writer, the name of the checkpoint it extends, and
every event since that checkpoint (§10). There is one way to change state:
`State.record(*events)` applies the events to the model and buffers them,
synchronously — the engine never waits on storage. A background flusher
appends what is buffered to the journal once the oldest event has waited
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

A flush is sealed before it is written: a failed or interrupted write is
retried with the very same bytes, so a retry that finds them in place
knows its write landed (`swap`, §0).

Everything else the storage does runs on its own loop, off the engine's:
the history lake flushes and merges (§7), and `Upkeep` truncates delta
logs, compacts key indexes (§6), deletes garbage and applies
retention (§11).

```json
{
  "checkpoint": "7f3a9c0e5b21d846-000012",
  "engine": "7f3a9c0e5b21d846",
  "events": [
    "…every event since checkpoint 000012, then:",
    {"type": "AttemptFinished", "run": "01J8ZC7Q…", "task": "site_feed:alpha",
     "attempt": "01J8ZC7R…", "outcome": "succeeded", "started_at": 1790074865.2, "finished_at": 1790074866.0,
     "commit": {
       "heads": {"site_events": {"…": "Head, §5"}, "site_files": {"…": "Head, §5"}},
       "keys": {"site_files": {"commit_number": 57, "added": 0, "removed": 0, "exact": true,
                               "files": [{"name": "000000000057-01J8ZC7R…", "level": 0, "entries": 2,
                                          "min": "alpha-file-1", "max": "alpha-file-3", "…": "…"}]}},
       "cursor": "5921",
       "positions": {}
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
| `ProjectRegistered` | `deploy`, `manifest` | replaces the manifest; applies aliases; resets removed assets and removed or moved outputs (§2); reconciles automation state |
| `FiringsOwed` | `owed` {automation: [[asset, partition]]} | what the deploy's asset changes leave each `OnChange` automation owing, once (§2): appended to its pending changes |
| `RunSubmitted` | `run` (id, request, tasks) | adds an active run |
| `RunControlled` | `run`, `action` (`cancel` \| `pause` \| `resume`) | |
| `AttemptLaunched` | `run`, `task`, `attempt`, `started_at`, `pin`, `at`, `execution`, `prepared`, `pool?` | the attempt file exists and a placement is about to start it: its claim becomes durable (§8) |
| `AttemptPlaced` | `attempt`, `handle` | the placement started it: where it runs, for whichever engine follows it (§8) |
| `AttemptFinished` | `run`, `task`, `attempt`, `outcome` (`succeeded` \| `failed` \| `skipped` \| `canceled`), `started_at`, `finished_at`, `error?`, `retryable?`, `commit?`, `owing a repair?`, `writes?` | records the attempt; on commit, installs heads, the partition's record (cursor, positions, completeness), and each keyed output's new delta file; `repairs` keeps the intents of a writer that died (§8) |
| `SourceCommitted` | `source`, `head`, `keys?`, `at`, `run?` | installs a source head and its delta file; a commit that changed something records `run` in the history (§7) |
| `IndexCompacted` | `output`, `partition`, `added` [file], `removed` [name], `at` | swaps compacted files into a key index |
| `IndexTruncated` | `output`, `partition`, `below`, `at` | drops delta log entries below `below` |
| `FilesCleanedUp` | `paths` | forgets index files that were deleted |
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

A checkpoint is this structure serialized as of the journal's last flush. Nested maps rather
than joined string keys, because partition keys may contain `/`.

```
State
  event_counter, deploy, deploy_number, manifest   # event_counter: events applied so far, the model's clock
  heads        {output: {partition: Head}}             # assets and external sources
  indexes      {output: {partition: KeyIndex}}         # keyed outputs and keyed sources (§6)
  partitions   {asset: {partition: PartitionRecord}}   # each asset partition's committed lifecycle
  automations  {name: AutomationState}
  runs         {run: Run}                          # active, or finished and not yet archived
  history      {files: {table: [File]}, rows: {table: [[seq, row], …]}, seq, imported}   # §7
  repairs    {output: {partition: [Intent, …]}}      # keyed outputs a dead writer may have half-written (§8)
  garbage      [[path, n], …]                      # index and history files nothing references since event n
  retired      [run, …]                            # deleted runs whose directories are still to delete (§11)
```

| Type | Fields | Bounded by |
|---|---|---|
| `Head` | `ref` (from the store), `run`, `attempt` (may point at a deleted run), `commit_number` (incremental outputs: the last commit that changed it, −1 before any), `base` (the first commit after the last reset of an unkeyed incremental output), `count` (keyed: live keys), `partitions?` (dynamic partitions: the partitions it lists), `version` (declared asset version), `asset`, `at`, `n?` (a source's: the event counter of its commit) | outputs × partitions |
| `PartitionRecord` | `cursor?` (json), `last?` (`Outcome`: its last terminal result), `caught_up?` (whether its last commit finished the pass it was on — the partition's completeness, whatever its outputs wrote), `caught_up_at?` (the event counter of the commit that last caught it up: before its asset's `changed_at`, it is `stale`), `seen?` {input: versions} (the head generations of the whole and dep inputs it last caught up to: moved since, it is `stale`), `positions?` {input: `Position`}, `failures?` (`Failures`: a per-key asset's failing keys, per-key-processing.md §9). Registration moves it whole under a rename, drops the positions of inputs the project no longer declares, and those a reset takes (§2). | assets × partitions |
| `Failures` | `commit_number` (the record's last commit), `counts` {outcome: keys}, `due` and `deploy_min` (lower bounds), `retry?` {`pass`, `deploy`, `forced_at`, `after`, `due_acc`, `deploy_acc`}, `passes`, `done_forced`, `last` (`changes` or `retry`), `forced` {class: position} — its index is `indexes["@asset"][partition]` (per-key-processing.md §9) | Each assets × partitions |
| `KeyIndex` | `prefix` (where its files live — kept across renames), `count` (exact: writes are exact), `files` [{`name`, `level`, `min`, `max`, `entries`, `size`, `tail`, `index`}], `log` [[`batch`, [file]], …] — see §6 | a few dozen files per index |
| `Position` | `kind` (`keys` or `commits`), `next` (the first upstream commit not yet delivered), `began` (when its last full pass began, kept after it ends: a whole or dep input that moved since makes one due), `pass` (one under way: its `mode` — `full`, `delta`, or a pattern change's `diff` — its boundary `from`..`to`, its place `at` — the last key delivered, or the next batch — its `page` of `pages`, a delta pass's reader `pin`; a full keyed pass's `from` is the head's commit number + 1 when it began, so changes made meanwhile arrive afterwards as deltas), `fingerprint`, `output` and `up` (the upstream index it reads), and per-key `patterns`, `pattern change`, `reconcile`, and `ahead`, the read-ahead: `[commit, run, attempt]` per `keys=` run since the last pass, capped (`positions-from-reads.md`; `python/solera_server/positions.py`) | inputs × partitions |
| `Outcome` | `outcome`, `run`, `attempt`, `at` | assets × partitions |
| `AutomationState` | `enabled`, `last_fired`, `last_run`, `last_revision`, `pending` (set of `[asset, partition]` for OnChange) | automations × partitions |
| `Run` | `id`, `request` {targets, partitions, mode, config, keys, automation, tags}, `status`, `paused`, `created_at`, `events` (how many it has recorded), `tasks` {task: `Task`} | in-flight work |
| `Task` | `status`, `deps`, `ready_at` (now, or a retry's due time), `wait` (seconds counted so far), `queued_at` (when the wait clock last started; null while stopped), `held?` [reason, name] (why the dispatcher last passed it over), `max_attempts`, what its ended attempts add up to — `tries`, `outcomes` {outcome: count}, `duration`, `first_at`, `last_at`, `last` (the latest attempt's `id`, `outcome`, `error?`, `outputs?`), `error?`, `executor?` — never a list of them (each one's row is in the history as it ends), `launched?` {`attempt`, `started_at`, `pin` (`applied` when it was claimed), `at`, `execution`, `prepared`, `handle?`, `pool?`, `worker?`, `claimed_at?`} | |
| `File` | `path`, `rows`, `bytes`, `at` [lo, hi] (time column), `runs` [first, last], `deleted?` [run] (hidden until rewritten), `deleted_at?` | files per table: ~log(rows) after merging |
| `Intent` | `added`, `removed`, `exact`, `files` (the dead attempt's delta files), `run`, `attempt` | writers that died mid-write, until the next commit of that output |

**Derived, rebuilt at start:** the claims of launched
attempts (from `Task.launched`), the pool queue, the ready queue and the
dependents index.

**Memory only:** the claim of an attempt still preparing (a restart
dispatches its task again), what each launched attempt's worker reported
(rebuilt from the control file and `.beat` after a restart), a cache of
key index blocks, and
a local copy of the history files (§7).

Example (abridged):

```json
{
  "event_counter": 48211, "deploy": "c0ffee…", "manifest": {"…": "…"},
  "heads": {"site_files": {"alpha": {
    "ref": {"output": "site_files", "store": "default", "partition": "alpha", "generation": 184467,
            "handle": {"mode": "keyed", "path": "site_files/alpha", "key": "path"}},
    "run": "01J8ZC7Q…", "attempt": "01J8ZC7R…",
    "commit_number": 57, "count": 4, "complete": true, "version": "1", "at": 1790074866.0}}},
  "indexes": {"site_files": {"alpha": {
    "prefix": "keys/site_files/alpha/", "count": 4,
    "files": [{"name": "c01J8ZE2…-0000", "level": 1, "min": "alpha-file-0", "max": "alpha-file-3", "entries": 4, "size": 212, "…": "…"},
              {"name": "000000000057-01J8ZC7R…", "level": 0, "min": "alpha-file-1", "max": "alpha-file-3", "entries": 2, "size": 140, "…": "…"}],
    "log": [[56, [{"name": "000000000056-01J8ZB…", "…": "…"}]], [57, [{"name": "000000000057-01J8ZC7R…", "…": "…"}]]]}}},
  "partitions": {"site_feed": {"alpha": {"cursor": "5921", "drained": true}},
             "file_index": {"alpha": {
               "last": {"outcome": "succeeded", "run": "01J8ZB3K…", "attempt": "01J8ZB3M…", "at": 1790074800.0},
               "drained": true,
               "positions": {"site_files": {"kind": "keys", "next": 56, "fingerprint": "8d46…",
                                             "output": "site_files", "up": "alpha"}}}}},
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
and operations live in an SDK library (`solera.keys`) used by the worker,
by compaction, and by the server for key listings. The engine itself only
holds each index's `KeyIndex` record.

**Structure: a log-structured merge tree of sorted files.** Every file
holds `(key, generation, deleted, payload?)` entries sorted by key — the
generation that last wrote the key, its version, and what an immutable
store names its object by (`versions.md`, `lifecycle.md` §9.8); the
optional payload is a source key's version or a failed keys's record —
and a delta's entries also the predecessor generation of the key they
change or delete, which compaction drops.

- **Level 0** holds delta files, one per commit, named by commit number, and files
  merged from them. Their key ranges overlap; a file is as recent as the
  newest commit it holds, which leads its name.
- **Levels 1+** hold compacted files with non-overlapping key ranges, each
  up to ~64 MB, each level ~10× the previous.
- **Newest wins:** a key's current entry is its entry in the newest file
  containing it; a `deleted` entry hides older ones.
- **`log`** lists the delta files by commit number, from the lowest consumer
  position to the head. A delta file stays readable while it is in the
  log, even after compaction has merged it out of the levels.

**File layout.** `[data blocks][filters][block index][footer]`, byte
format in `key-index-format.md`. Data blocks hold ~64 KB of entries
before compression. Everything after the data blocks — filters, block
index, footer — is the file's **tail**; the index and footer alone are
its **index part**. The `KeyIndex` record stores each file's size and both
lengths, so a reader fetches exactly what it needs with one range read:
the tail when it checks filters, the index part when it scans, the whole
file when it is small — then range-reads only the blocks it needs.

**Filters.** Each file carries one blocked Bloom filter of its keys (14
bits per item, 1.75 B per entry, 0.35% false positives measured — keeping
an item's bits in one 512-bit block costs ~1.8× over independent bits). A
key no key filter matches, across every file whose key range could hold
it, is new and needs no block read; every other key gets an exact lookup.

**Exact writes, exact count.** Every delta entry names its key's
predecessor (the generation it replaced) when the key was live, so a
consumer can tell added from updated and an immutable store's cleanup
knows every replaced object; `KeyIndex.count` is exact (`count += added −
removed` per commit). Writes read the block of every key a filter holds
for that (docs/key-index-design.md, which measured the cost: nothing on a
warm engine, ~1K block reads per 1K random updates on a cold 100M-key
index).

**Engine-resolved commits** (`resolved-commits.md`). A small write is first
offered to the engine over the attempt's channel: it answers from a cache
of the index files on its local disk — exact, no requests — or declines,
and the worker resolves the write itself as below. The same cache answers
an attempt's input reads — its batches of a full pass or of a
delta — with its `start` reply, so a consumer reads no index file when
the index is warm.

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
(failed keys) reads every live key's entry, counting nothing from the
filters.

**Full replacement.** A bare return of every row must compare every live
key, so it reads the whole index — as a stream, never whole. The written
content stays where the worker holds it: an Arrow key column is read in
place (anything with `__arrow_c_stream__`), Python keys — of rows, or of a
pandas DataFrame's key column — are packed once into one buffer (~28 B
per key). One O(n) pass finds whether they arrive sorted; if not, a
permutation sorts them — bucketed by the two key bytes after the prefix
they all share, each bucket sorted as 12-byte (prefix, row) pairs on every
core, ~4.5 B per key at the peak (bench/keys/results.md, "Sorting the written keys"). A merge-join
then walks the sorted keys and the index's newest-wins view together, each
level fed a segment of 8 MB of consecutive blocks at a time, a few ahead.
Every key written goes straight into the current delta file at the
writer's generation — a source key whose version equals its entry's
excepted — live keys not written into `deleted` entries, and each file
goes to the store as it fills. Only keys are read: nothing of a row's
other columns reaches the index. Memory is the permutation, a few segments per level and a file or
two in flight: at 100M keys, 0.8 GB on top of the data for shuffled Arrow
rows (0.4 GB sorted; 28 s), 3.4 GB for Python rows, whose keys are packed
(`bench/keys/results.md`).

An opaque write's store (Postgres, for its `Sql`) reports the keys it holds once it wrote, already
sorted, a chunk at a time through a server-side cursor (`keys(ref,
None)`), and the replacement streams them, so nothing is sorted or held.

**Operations.**

| Operation | Who | How |
|---|---|---|
| Compute a delta | worker, at write time | Read the write's keys once (`Prepared`, `per-key-processing.md` §7), against the index **as pinned in the spec**. A patch is checked with the filters and the read strategy above: every key it writes, at the attempt's generation, plus `deleted` entries for removed keys that may exist. A full replacement is the streaming merge-join above. Either way the result is the batch's delta files, split at ~64 MB. |
| Commit | engine | Add the delta file to level 0 and to `log`; `count += added − removed`. The claim — one attempt per (asset, partition) from launch to settlement — guarantees the index didn't change underneath. |
| Deliver pending deltas | worker, for an `Incremental` input | Read the `log` files from the position to the head; chunk by `batch_size` in key order; ask the upstream store for those rows with `Keys(…)`. |
| Full pass | worker | Page through the merged view of all levels from `after`, `batch_size` keys at a time, and ask the store for them with `Keys(…)`. Per level, only the files covering the page are opened, and only their index parts are read — or the whole file, once, when it is small (below one request's latency worth of transfer, ~2.4 MB). A multi-page scan keeps each file's last fetched blocks for the next page, so it reads every block once. |
| Compaction | the engine's machine by default (§6, *Engine work*) | Once level 0 holds ~8 files, merge them into one level-0 file — or, once level 0 holds a tenth of level 1's bytes, into level 1 with the level-1 files it overlaps (all of them, for random keys). A level over its target pushes one file down, merging it with the files it overlaps there. A merge streams, a few segments per input and one output file at a time. Commit with `IndexCompacted`. Each merge into a level rewrites about ten times the bytes it brings: ~20–30× over an entry's life with random keys (`bench/keys/amplification.py`). |
| Truncate the log | engine | Drop `log` entries below the lowest consumer position and below every delta an in-flight attempt was given (`IndexTruncated`); an output with no `Incremental` consumers keeps none. A consumer whose delta the log no longer holds gets a full pass. |
| Delete files | engine | A file in neither `files` nor `log` joins `garbage`, and is deleted once every attempt that could have pinned it has finished (`FilesCleanedUp`): every attempt claimed before the event that let go of it. Both are positions in event order (`applied`), the same in every engine that replays the journal — never wall clocks, which two engines may disagree on. A delta file of an attempt that never committed is deleted when the attempt ends, unless it is an repair intent (§8). |

Writes that never pass through the worker as rows — opaque writes
(`Opaque`), such as Postgres's `Sql` — are the one case where the store must report the keys it holds once it
wrote, sorted (above); only stores supporting such writes need to.

External sources use the same index. An API commit becomes a delta file;
for very large commits the client builds the file itself and commits a
reference to it.

**Engine work.** Compaction is the one heavy computation the engine
itself starts. It runs locally on the engine's machine, on a worker thread
with its own event loop, at most `maintenance_concurrency` at a time. It
reads the engine's cache's local copies when the cache holds the index
warm, the store otherwise. A
project-level setting to offload it to an executor is planned, not built:

```python
Project(..., engine_executor=etl(cpu=2, memory="8GB"))   # not built yet; local today
```

**Caching.** Index files never change once written, so a cached copy is
never stale. The one cache of them is the engine's (`resolved-commits.md`
§5): it answers small writes from local copies before a worker reads
anything, and answers attempts' input reads at `start`. Workers keep
no cache: what they read — a write the engine declines or that is too big
for it, a full pass's batches, per-key incremental's lookups — comes from the store
(the costs are in `bench/keys/results.md`, "Without a worker cache").

**Implementation.** The file format is ours (no Parquet). The per-key
work — encoding, decoding, sorting, merging, lookups over fetched bytes —
is Rust, in the `solera._native` extension (PyO3, abi3) that the `solera`
distribution builds and both the engine and the worker require; I/O goes
through obstore. A full replacement and a compaction run as
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
| `tasks` | task of a finished run | `asset`, `partition`, `status`, `started_at`, `finished_at`, `attempts`, `duration`, `wait` (seconds it could have run but didn't), `deps`, `max_attempts`, `retry_delay`, `retry_backoff`, `executor` (of its last attempt) |
| `attempts` | attempt | `task`, `n`, `generation` (the one its writes carried), `outcome`, `started_at`, `finished_at`, `duration`, `preparing`, `provisioning`, `importing`, `loading`, `computing`, `writing`, `settling` (seconds per phase, below), `peak_memory` (bytes; only in a process of its own), `cpu_seconds`, `error`, `executor`, `cpu`, `memory`, `gpu` (requested; all null if it never launched), `options` (map: its other placement options, e.g. `image`), `outputs` (the outputs it committed, each at its `generation`), `keys` (map: a per-key attempt's keys by outcome) |
| `run_timeline` | moment of a run | `n` (its order in the run), `at`, `type`, `task` and `attempt` (null for the run's own events), `by`, `name`, `reason`, `until`, `rows` — the timeline, below |
| `commits` | output version a commit installed | `output`, `partition`, `generation` (its version: the writing attempt's, or a source commit's), `run`, `attempt`, `at`, `batch`, `added`, `removed`, `added_keys`, `removed_keys` (a source commit's keys, up to 1,000), `rows`, `metadata` (JSON; an unkeyed source commit's `version`) |
| `lineage` | input version an output version was read from, and what a current read saw (stores.md, "What a read sees") | `output`, `partition`, `generation`, `input`, `input_scope`, `input_generation` (what was pinned: the head, or a fixed pass's generation), `param`, `read_generation` (what a read of current rows saw; null for a snapshot store's read, which is the pin, and for an external source's, which is its tick — `versions.md` §6) |
| `key_outcomes` | key a per-key attempt processed | `run`, `attempt` (`attempts.id`), `asset`, `partition`, `key`, `generation` (the upstream key's it processed), `outcome` (`ok`, `removed`, `unmatched`, `rejected`, `failed`, `retrying`, `canceled`, `timed_out`), `error`, `duration`, `at` — per-key-processing.md §10 |

A run where every task was skipped — it launched nothing and wrote
nothing — is recorded with status `skipped`. Listings hide skipped runs
unless the filter asks for that status, so a 10-second poller that
usually finds nothing new does not bury the runs that did something.

A source commit that changed something is a `runs` row with trigger
`commit`, plus the `commits` row of the version it made, which
lists the keys it changed (up to 1,000; past that, only counted). The
source's head points at it (`head.run`).

**No run documents.** The tables are the record: a finished run's detail
— is rebuilt from its `runs`, `tasks` and
`attempts` rows (a source commit's from its `runs` and `commits`
rows), and reads the same as it did while the run was live. Only what has
no shape of its own stays JSON: a run's `config` and `keys`, and output
metadata.

**The timeline — `run_timeline`.** Everything that happened to a run, in
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
submits a new run of its failed and canceled tasks' partitions, and of those
it blocked, with `retry_of` naming it; a run in progress cannot be
retried. Retries inside a running task are its attempts.

**Where rows come from.** Rows are born inside `apply`, from the events
that finish things: `AttemptFinished` yields the attempt's `attempts` row
(a task working through a backlog holds counts, not one summary per batch);
`RunArchived` yields a run's `runs` and `tasks` rows; `AttemptFinished`
with a commit also yields a
`commits` row per changed output, plus `lineage` rows from the
input versions pinned in the attempt's spec; `SourceCommitted` yields a
`runs` row and a `commits` row. From there, they are the **lake's**
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
is cleaned up rather than installed, so a deleted run never comes back.

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
Every(3600, tags={"team": "growth"})   # an automation: its runs carry them
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
| p50/p95 duration and wait, failure counts, compute hours, per asset and per executor | `GET /stats?since=&asset=&partition=` | |
| an asset's versions and their metadata | `GET /assets/{name}/history?output=&partition=&before=` | |
| what a version was built from, or what was built from it | `GET /outputs/{name}/lineage?partition=&generation=&direction=upstream\|downstream&depth=5` | each edge's `from` is what was read: its `generation` and the writer's `run`, `attempt` and `at`. Flag: `uncommitted` (`{attempt, run}`: a write no attempt committed). `detail` keeps the pin (`pinned_generation`) for debugging |
| every asset at a glance: partitions by status, newest outcome, failing keys, partitions owing a repair | `GET /assets:status` → `{assets: {name: {partitions, partitioned, last, failures, owing a repair, updated_at}}}` | |
| a per-key asset's failing keys, and each partition's failure record | `GET /assets/{name}/failed-keys?partition=&outcome=&after=&limit=100` → `{partitions, keys, deploy, now, next}` | |
| a partition's staleness reasons and its stale keys (`positions-from-reads.md`) | `GET /assets/{name}/stale-keys?partition=&after=` | `solera stale ASSET [PARTITION]` |
| what a per-key asset's keys came to, newest first | `GET /assets/{name}/key-outcomes?partition=&key=&q=&outcome=&run=&before=&limit=100` → `{outcomes, next}` | |
| why a key is, or is not, in an asset's output (per-key-processing.md §10) | `GET /assets/{name}/explain?key=&partition=&input=` → `{verdict, patterns, failure, last, last_ok, …}` | |
| an asset's input inputs, with every partition's position, lag and state | `GET /assets/{name}/inputs` | |
| outputs owing a repair, stuck cleanups (lifecycle.md §9.6, §9.8) | `GET /repairs`, `GET /cleanups` | `solera cleanups` |

Filter fields combine with AND; repeating one field (`status=failed&status=canceled`)
matches any of its values. A facet counts its values with every *other*
field applied, so the console can show "12 failed, 340 succeeded" while
failed is selected. Histogram buckets are the smallest of 1 min, 5 min,
15 min, 1 h, 3 h, 6 h, 12 h, 1 d, 7 d, 30 d that fit the span in the
requested number of bars. `next` is a cursor: the last run id of the page.

## 8. Attempt objects — `runs/{run}/{attempt}.*`

The protocol is `lifecycle.md`'s, which this section summarizes; the
records the engine and the worker share are `solera/lifecycle.py`.

**Spec and control file.** The engine writes `{attempt}.spec`, immutable,
then creates `{attempt}.control` `open`, both before `AttemptLaunched`.
Every later change to the control file is a swap (`If-Match`, §0), and
each body names its writer. A worker reads the spec and takes the
attempt by swapping the control file from `open` to `owned`, with a
random worker id; the first swap wins. A worker that loses writes nothing
and, unless it is a pool worker, waits for the owner's end before exiting,
so its exit is never taken for the attempt's. The owner seals its outcome
once, swapping the file to `sealed` with the result; a refused retry that
reads its own body back landed. A worker that cannot publish exits
without a result, as if it had died. A worker that finds the file `ended`
or gone stops, and never creates it (`lifecycle.md` §2.4).

`spec` is everything the worker needs and the lineage record of what the
attempt read, including the key indexes as pinned, plus the engine's URL,
the attempt's token and its generation (the claim's event counter).

```json
{
  "attempt": "01J8ZB3M…", "run": {"id": "01J8ZB3K…", "config": {}}, "deploy": "c0ffee…",
  "project": "brimstone", "asset": "file_index", "partition": "alpha", "execution": {"kind": "Local"},
  "inputs": {"site_files": {"ref": {"…": "…"}, "index": {"…": "KeyIndex: levels + log[56..57]"},
             "changes": {"from": 56, "to": 57, "after": null, "full": false, "limit": 2}}},
  "outputs": {"file_index": {"before": {"…": "ref"}, "reset": false, "contract": {"…": "store, writes, key"},
                             "commit_number": 12, "index": {"…": "KeyIndex: levels only"}}},
  "heartbeat": 10, "engine": "https://solera.example.com", "token": "…", "generation": 184467
}
```

```json
{
  "worker": "k3v9q2", "status": "succeeded", "writes": "complete",
  "outputs": {"file_index": {"ref": {"…": "…"},
              "keys": {"added": 0, "removed": 0, "exact": true, "files": [{"name": "000000000012-01J8ZB3M…", "…": "…"}]}}},
  "delivered": {"site_files": {"after": null, "upserted": ["alpha-file-2"], "deleted": []}},
  "events": [{"type": "booted", "at": 1790074791.2}, "…"], "usage": {"cpu_seconds": 0.4},
  "log": {"chunks": [[0, 412, 1790074791.2]], "tail": "H4sI…", "lines": 800, "bytes": 3911, "truncated": false}
}
```

**Launch and adoption.** The engine writes the spec, then
`AttemptLaunched`, durable, then starts the placement and records its
handle as `AttemptPlaced` — lazily, riding the next flush. From
`AttemptLaunched` on, the attempt's claim is durable:
an engine that restarts adopts it — follows its handle, or finds it again
by name (ECS `clientToken`, the Kubernetes job `solera-{attempt}`), or
follows its worker's reports — and settles it as the first engine would
have. A handle is never given up: a provider that errs, or does not show
the run, cannot tell for now.

**Reports: evidence, never permission.** The worker reaches the engine
over its channel (HTTPS, `lifecycle.md` §5): `start` once, a beat every 10
s, live log lines with their offsets, `finished`. Each answer carries the
cancel record, if any. After two failed beats — or with no engine to
reach, as under the CLI — it reports by overwriting `.beat` every two
beats instead, and reads its control file each time: `ended` stops it.
The engine reads `.beat` only while the channel is quiet. A worker is
silent after three beats without a report over the channel, or six
through `.beat`; a provider's exit ends an attempt unless its owner
still reports over the channel (then it was a duplicate's). None of this
decides whether the attempt's writes may still land: that is the control
file's.

**Provisioning and deadlines.** Until its first report — `start`, a beat,
or `owned` seen — a worker is provisioning, under a deadline of its own
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
the engine swaps the control file to `ended`; a result sealed before
that stands, and a later one is refused (its swap fails).

**The gate: the control file's `writing`.** A worker about to write to a
`fenced` store swaps the file from `owned` to `writing`, with its
intents: the delta files of the keys it will change. Refused, with `ended`
or no file there, it writes nothing. The engine ending an attempt without
a result swaps the file to `ended`, and what it swapped from is the
attempt's write-completion evidence: from `open` or `owned`, `none`;
from `writing`, `writing`, with the intents. A result says its own:
`none` (no store call), `complete` (every store call returned) or
`writing` (one raised, or was abandoned).

**Nothing outlives its run.** Retention deletes a run's directory whole.
A worker that read its spec, paused, and resumes after that finds its
control file gone, so its swap is refused and it stops: it never creates
the file, so it never takes a gate the engine did not leave open.

**Outputs owing a repair.** An attempt that ends with its control file
`writing` may have written part of its keyed outputs. The engine fails it
with the intents (`AttemptFinished.intents`) and keeps their delta files.
The next attempt on that partition reads the intended keys back from the store
and folds what landed into its own delta, and its commit settles the
output. Unkeyed outputs need no repair: the next attempt writes the same
value or commit again.

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
    async def store(self, write, prior, context) -> Written   # Written(ref, keys?)
    async def load(self, ref, t, selection) -> Any          # selection: None | Keys | Commits
    writes: str                                             # "immutable" or "fenced" (stores.md)
```

- `WriteContext` carries the engine-assigned `commit_number`, whether the write is a
  `reset`, the `attempt` id, its
  `generation` and `worker_id` (`lifecycle.md` §9.7–9.8). A keyed output's write reaches the store as a
  `KeyedWrite`: read once (`prepared`), and what it changes against the
  key index — `upserts` to write, `removes` to delete — or `whole`, a first write or a reset (a `full`
  run): the store then writes everything and deletes whatever else
  it holds. A patch that changes 3 keys of 100,000 reaches the store as 3
  upserts, and the store reads only their groups.
- `Written.keys` is only for writes the worker never sees as rows
  (§6); for everything else the worker computes keys itself.
- **What a read sees depends on the store's kind** (`architecture.md` §3,
  `lifecycle.md` §9.6). FileStore and S3Store (`immutable`) never
  overwrite, so a load reads exactly the generation its consumer pinned; a
  superseded object lingers until no reader pin predates it. PostgresStore
  and every `fenced` store hold one copy: a load reads the
  current rows, so a consumer pinned to generation 12 that loads after
  generation 13 committed gets generation 13's content (and lineage says
  13), and a changed row may be delivered twice. `Store.load` makes no promise beyond its kind's.
- **Nothing expires.** A store holds the current content of each output,
  plus, for an immutable store, what pinned readers still need.

**FileStore**, the default, writes what an asset returns as files under
`.solera/data` next to the project file (or `FileStore(path)`, or
`$SOLERA_DATA`) — one object per value, partition, key or commit:

```
rollup@184467.json                a value, by generation 184467
site_status/alpha@184467.json     a value, partition alpha
uploads/u-7/184467.pkl            a keyed output: one object per key and generation
site_files/alpha/f-1/184467.json  keyed and partitioned
site_events/alpha/000000000042/184467.json
                                  an unkeyed incremental output: one object per commit
```

Every name carries the **generation** of the attempt that wrote it (its
claim's event counter), so no two attempts write one name and objects
are created once, never overwritten (`lifecycle.md` §9.8). A keyed load
names its objects from the index's generations; a whole keyed
read is paged from the pinned index. Content is JSON when it round-trips
exactly, pickle (`.pkl`) otherwise. Keyed outputs are declared
`Output(keyed=True)` and return `dict[str, Any]`, or rows with
`key="id"`. A superseded or removed key's object, a value's previous
object and an abandoned attempt's objects are deleted by the partition's next
attempt (`store.cleanup`) once no reader pin predates them. Commits of an
unkeyed incremental output accumulate until a `full` run starts the
output over; a commit's object is its highest generation. **S3Store(url)** is the same layout in a
bucket; `Project(default_store=S3Store("s3://…"))` makes it the default.

**PostgresStore** keeps one table per output, shared by its partitions,
and rewrites the rows a write covers.

## 10. Lifecycles

*The journal is one object, `control/journal.json`, swapped with
`If-Match` (§0) on every write. It holds three things: the engine id of
the engine that writes it, the name of the checkpoint it extends, and
every event since that checkpoint. The journal spec checks the rules
below (`verification.md`, "Formal model: the journal object"), and
`journal-object.md` says why it replaced the numbered segments and what
it costs.*

**Engine start.**

1. `read` the journal. With no journal there, the state is empty.
2. `GET` the checkpoint the journal names, and apply the journal's
   events on top of it. If the checkpoint is gone, a running engine has
   moved the journal to a newer one and cleaned up since: go back to 1.
3. Fence: `swap` in the same journal under this process's own engine
   id, a new random one, on the ETag step 1 read (`None` if there was no
   journal). A `Conflict` means another engine wrote in between: go back
   to 1.

Then adopt every launched attempt (§8). Tasks that were preparing are
dispatched again. A read-only open (tools that inspect a namespace a
server may be writing) stops after step 2.

**Fencing.** Every write an engine makes is a `swap` on the ETag of its
own last write. Its bodies never repeat: each one names its engine id,
and each write either adds events or names a new checkpoint. So once
another engine has written, the old engine's next `swap` raises
`Conflict`. It stops, failing every `durable()` waiter (a `503`).
Everything the old engine wrote before that was in the journal the new
engine read, so no acknowledged event is lost. For example:

1. Engine A serves. The journal is `{a7f3, cp a7f3-000004, [e1, e2]}`,
   with ETag `X`.
2. Engine B starts: it reads the journal (`X`), loads `a7f3-000004`,
   applies `e1` and `e2`, and swaps in `{91c2, cp a7f3-000004, [e1, e2]}`
   on `X`.
3. A flushes `e3`, swapping on `X`: `Conflict`, since B's body is there.
   A stops. `e3` was never acknowledged, and its API call gets a `503`.

The engine id is what makes the fence work. Without it, B's fence would
write A's bytes again, so the ETag would stay `X`, and A's next write would
still succeed.

**Flush.** A flush swaps in the whole journal: the engine id, the
checkpoint, and every event since it, with the new ones appended. Events
stay encoded in memory, so a flush only joins bytes. A flush whose answer
is lost is resolved by `swap` (§0): its own bytes are there, so it
landed.

**Checkpoint.** A checkpoint is due once the journal's events reach
`max(64 KB, a sixteenth of the last checkpoint's size)`, and on a clean
shutdown. It takes these steps, under the flusher's lock:

1. `LIST control/checkpoints/`.
2. `PUT` the state, as of the last flush, under a fresh name:
   `checkpoints/{engine}-{n:06d}.json`, where `n` counts this engine's
   checkpoints.
3. `GET` it back and compare its bytes with what was written. The
   encoding is deterministic and came from a valid state, so the same
   bytes are a checkpoint that parses. If the GET fails or the bytes
   differ, leave the journal as it is: the next due point tries again, and
   cleanup deletes the bad checkpoint with the rest.
4. Move the journal: swap in `{engine, the new checkpoint, no events}`.
   A `Conflict` means the engine was fenced: it stops and deletes nothing.
   Any other error leaves the move pending: the next flush writes its very
   same body before anything else, and `swap` takes its own bytes for a
   move that landed unheard.
5. Once the move has landed, `DELETE` every checkpoint that step 1 listed.

Besides the engine id, three rules make this safe. Break any one and
the journal spec finds the failure: an opener that gives up for the
first, a lost acknowledged event for the other two.

- **An opener whose checkpoint is gone reads the journal again.**
  Cleanup only deletes checkpoints the journal no longer names, so the
  journal has moved past it.
- **No checkpoint is named before it is read back.** Only one checkpoint
  is kept, so the journal must never name one that does not read back as
  written.
- **Cleanup deletes only what it listed before its move.** A checkpoint
  exists before the journal names it, and listing after the move would
  catch it in that window. For example: A moves to `a7f3-000005`. B fences
  A, appends, and writes `91c2-000001`, not yet named. A then LISTs,
  deletes everything but its own `a7f3-000005`, and so deletes B's
  checkpoint. B's move then names a checkpoint that is gone. With the
  rule, this cannot happen. If A's move lands, nobody fenced A before it,
  so when A listed, no newer engine's checkpoint existed yet.

**Sizes.** A flush writes half the threshold on average: 32 KB at the
minimum. With a 10 MB state it writes up to 640 KB, about 1.3 ms of upload
at Railway's 250 MB/s, next to a 12 ms PUT. A checkpoint follows at least
a sixteenth of its own size in journal, so checkpoints come at most 16
times as often as under the segments' rule. The measurements are in
`journal-object.md`.

**A failed event ends the process.** Events are encoded and checked before
any is applied; should a reducer still raise half-way, the model is no
longer the fold of the journal. The engine then takes no checkpoint,
refuses every later event, writes what it recorded before (the failed
batch never reached the journal), logs why, and exits with code 70: the
platform restarts it, and the replay recovers exactly the journal's state.

**Run lifecycle.** submit → `RunSubmitted` · attempts → `AttemptFinished`
· terminal → `RunArchived` (its rows join the pending history) · flush →
`HistoryFlushed` · merge → `HistoryCompacted` · retention →
`RunsDeleted`, durable → `DELETE runs/{run}/` → `RunsPurged`.

**Attempt lifecycle.** claim (memory) → pin, write the spec and the
control file →
`AttemptLaunched` → launch → `AttemptPlaced` → the worker takes the
control file (`owned`), starts, takes the gate (`writing`), writes, and
seals its result → the engine commits it (`AttemptFinished`). A cancel or
timeout is requested, drained, then forced: the engine swaps the control
file to `ended` and ends the attempt with
`AttemptFinished` (`canceled`, or `failed` and retryable) (§8).

## 11. Retention

Policies are set per asset, with a project default:

```python
@asset(retention=Retention(days=7))
@asset(retention=Retention(runs=90))           # the 90 newest runs that committed to it
Project(retention=Retention(days=30))          # default, including source commits
Retention(forever=True)
```

**Current state never depends on runs.** Heads, cursors, positions and
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
append-only output — FileStore's commits, a Postgres event table. Bound it
with a scheduled job, or a periodic `full` run:

```python
@job(deps=["site_events"], automations=Every(3600))
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
