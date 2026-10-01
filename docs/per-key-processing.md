# Per-key processing — proposal

Status: **proposed**, not built. It adds an `Each` edge (an asset written
for one key, run over every changed key), keys that hold many rows,
per-key outcomes with user-classified errors, key patterns on edges, and
observable sources. It builds on the engine cache, the HTTP resolver,
inlined changes and the canonical row digest of `resolved-commits.md`
(being rewritten), and on the worker → engine HTTP channel and attempt
objects of `lifecycle.md` (being written): `{attempt}.spec`, the
`{attempt}.worker` invocation claim, `{attempt}.result`, and the
`{attempt}.writing` fence.

## 1. Why

The first real project, Brimstone's SharePoint ingestion, has ~56 assets
of one shape: a file changed in some folder → parse it → upsert its rows.
Each asset hand-rolls the same loop:

```python
async def icp(ctx, files: list[dict], sharepoint: SharePointClient):
    async def one(f):
        try:
            return parse_icp(await sharepoint.read(f["item_id"]), f["path"])
        except Exception as e:
            ctx.log.warning(f"skipping {f['path']}: {e}")   # swallowed: the file is never retried
    frames = await asyncio.gather(*(one(f) for f in files))
    return Patch(pd.concat([f for f in frames if f is not None]), remove=ctx.changes["files"].deleted)
```

Three things in Solera force that shape:

1. **One key, one row.** A keyed write rejects a second row with the same
   key ("duplicate key in write"), so a file that yields 40 samples cannot
   be the key of its rows. `example/brimstone.py`'s `qaqc_samples` returns
   exactly this and cannot run today.
2. **No per-key failure.** One bad file fails the page; the watermark stays
   where it was, and every retry hits the same file first — a poison pill.
   The only escape is to swallow the error, and then nothing ever retries
   the file or shows that it failed.
3. **No filters on edges.** Each asset wants one folder and a few name
   patterns. Today the poller filters for its consumers, which couples it
   to every one of them.

## 2. The design in one paragraph

An asset declares how to process **one key**; `Each` runs it over every
changed key of a keyed upstream, `concurrency` at a time, `batch_size` keys
per attempt, and hands each output's store one `Patch({key: value})`.
Every key is a group: it holds all the rows that carry it, one or many,
and its version is a digest of the multiset of those rows. Errors raised for one key are classified by the
Solera exception the user's error subclasses — `Rejected`, `Failed`,
`Transient`, `Abort` — and a key that did not succeed lands in the edge's
**failure index**, a key index of its own, so it is visible, retried on a
bounded schedule, and never blocks the keys behind it. `include` and
`exclude` patterns on an edge select keys by name; the engine evaluates
them from its cache to skip work, the worker evaluates them for
correctness. A `Source` subclass with `observe()` is polled by an
automation and committed like the commit API. Throughout, the engine sees
keys and revisions only.

## 3. What the engine sees

| Piece | Lives in | Knows |
|---|---|---|
| Key indexes, watermarks, the failure index, key patterns | engine | key strings, versions (opaque bytes), outcome classes |
| The per-key loop, concurrency, error classification | worker | a page of keys; each key's value is opaque |
| Splitting a page into per-key values; turning a write into `(key, version)` rows; stamping the key column; replacing a key's rows | store | its own types |
| SharePoint, samples, what counts as unprocessable | user code | everything else |

The keyed dictionary stays the one shape the core understands: `Each`
hands the user one entry of a `dict[key, value]` and hands the store a
`dict[key, value]` back. Everything about rows — how many a key has, how
they are compared, which column carries the key — goes through stores
(§7).

## 4. Example: SharePoint, end to end

```
Graph delta feed ──► sharepoint_events ──► sharepoint_files ──► Each consumers: icp, xrf, bet, …
                     append-only log,      keyed by path,        one call per changed file,
                     cursor = delta token  revision = ctag        selected by key patterns
```

```python
@asset(
    outputs=Output("sharepoint_events", store="postgres", schema="sharepoint",
                   incremental=True, partition_column="site"),
    partitions=sites,
    automations=Automation(trigger=Every(30)),
)
def sharepoint_events(ctx, graph: GraphClient):
    """The entry point: Graph delta events as they come, never a listing."""
    events, token = graph.delta(site=ctx.partition, since=ctx.cursor)
    return Result(outputs={"sharepoint_events": Patch(events)}, cursor=token)


@asset(
    outputs=Output("sharepoint_files", store="postgres", schema="sharepoint",
                   key="path", revision="ctag", partition_column="site"),
    partitions=sites,
    inputs={"events": Incremental("sharepoint_events")},
    automations=AutoRefresh(),
)
def sharepoint_files(ctx, events: pd.DataFrame):
    """Fold events into the current state of each file. A folder move or
    delete expands to its files through this output's own index."""
    upsert, remove = fold(events, under=lambda prefix: ctx.keys("sharepoint_files", prefix=prefix))
    return Patch(upsert, remove=remove)


@asset(
    outputs=(
        Output("icp_samples", store="postgres", schema="analytical",
               key="path", primary_key=["path", "sample_id"]),
        Output("icp_raw_data", store="postgres", schema="analytical", key="path"),
    ),
    partitions=sites,
    inputs={"file": Each("sharepoint_files",
                         include="ICP/Results/**/*.csv",
                         exclude={"archive": "**/archive/**", "templates": "**/*template*"},
                         batch_size=100, concurrency=16)},
    automations=AutoRefresh(),
)
async def icp(ctx, file: dict, sharepoint: SharePointClient) -> Result:
    raw = await sharepoint.read(file["item_id"])     # a network error is Failed, or Transient if mapped
    if not raw:
        raise Unprocessable("empty file")              # Brimstone's: class Unprocessable(solera.Rejected)
    samples, values = parse_icp(raw)
    return Result(outputs={"icp_samples": samples, "icp_raw_data": values})
```

Why a files table between the log and the consumers — what a consumer
sees in each case:

| Between two runs | Reading `sharepoint_events` | Reading `sharepoint_files` |
|---|---|---|
| a file saved 5× | 5 events to collapse | 1 change, the latest ctag |
| a file created, then deleted | 2 events that cancel out | nothing |
| a folder moved | 1 folder event, 0 file events | N removes, N upserts |

The fold is written once, in `sharepoint_files`, instead of in every
consumer. Two assets rather than one with two outputs keep the log the
record: `solera run sharepoint_files --full` rebuilds the files from it.

**Key = path, not item id.** Patterns can only see keys (§11), so the key
carries what consumers select on. Moves come out right: a file moved out
of `ICP/Results/` is a remove for `icp`, a file moved in is an upsert. A
rename reprocesses the file, which the rows need anyway since they carry
its path. `item_id` stays a column.

## 5. `Each`

```python
Each(output=None, *, include=None, exclude=None, batch_size=100, concurrency=16, meta=None)
```

- **The upstream must be keyed** — a keyed output or keyed source; keys
  are the unit of outcome. Partition rules are `Incremental`'s.
- **The parameter is one key's value**, loaded through the upstream store
  as part of a page: the worker asks `store.load(ref, dict[str, T],
  Keys(page))` and the store splits the page by key. `ctx.key` and
  `ctx.revision` name the key and its upstream version.
- **`batch_size`** is keys per attempt, which is keys per commit: it bounds
  memory and how much work a crash throws away. **`concurrency`** is keys
  in flight within an attempt: a semaphore for an `async` function, a pool
  of threads for a plain one.
- **Every output of the asset is keyed by the input's key** — `key=`
  (the rows the call returns, any number) or `keyed=True` (one value).
  Unkeyed outputs are rejected at registration. The call returns a value
  (one output) or `Result(outputs=…)`. An output the call does not return
  holds nothing for that key.
- **Deleted keys never call the function**: their rows are removed.
- **Other inputs are whole and shared**: loaded once per attempt, given to
  every call. A change to one resets the edge, as today: every key is
  processed again. Joins with slowly changing tables belong downstream
  (§14).
- **One `Each` per asset**, and no cursor (`Result(cursor=)` is rejected):
  the edge is the asset's iteration.
- **To the engine, `Each` is an `Incremental` edge** (`"each": {…}` in the
  manifest) with a failure index (§9). The engine still knows three edge
  kinds; the per-key call is the worker's.

One attempt, four changed files and one deleted:

```
page: a b c d (changed), e (deleted)
  a → rows                               ok
  b → rows                               ok
  c → Unprocessable("header row 2 …")    rejected: keeps its previous rows
  d → TimeoutError                       failed (unclassified)
  e                                      removed
store.store(Patch({a: …, b: …}, remove=[e]))      one write per output
commit: delta files · watermark → batch 42 · failure index: +c, +d
```

**Cancel and timeout keep finished keys.** The engine sends the cancel to
the worker over the HTTP channel; the worker:

1. stops starting keys, and cancels the calls in flight (an `async` call
   is cancelled; a thread is abandoned and its result ignored);
2. writes the keys that finished — one store write per output, one commit,
   as for a whole page;
3. records every key it did not finish in the failure index as
   **interrupted**, due immediately, its `tries` unchanged.

Finished keys need not be a key-order prefix of the page — with
`concurrency=16`, `a c d` may finish while `b` is still reading. So the
watermark does not stop at the first unfinished key: it moves past the
whole page, exactly as on success, and the holes are carried by the
failure index instead. The next run takes the interrupted keys as a retry
page (§9) at their current upstream revision.

```
page: a b c d e(deleted)   cancel while b, d are in flight
store.store(Patch({a: …, c: …}, remove=[e]))
commit: watermark → past e · failure index: +b interrupted (due now), +d interrupted (due now)
```

If the worker had already taken the fence when the cancel arrived,
today's rule stands: the write completes and commits. If the worker does
not answer within the grace, the engine takes the fence itself as today:
nothing commits and the page is delivered again — only a dead or
unreachable worker loses finished work. The attempt ends `canceled` (or
`timed out`) with the outputs it committed.

## 6. Every key is a group

```python
Output("icp_samples", store="postgres", key="path", primary_key=["path", "sample_id"])
```

There is no separate declaration for "many rows per key": a store
extracts the distinct keys of whatever it is given, and each key holds
every row that carries it. `key="path"` over a DataFrame or a list of
dicts means one key per distinct path; a key with a single row is a group
of one. Writes keep the semantics §4 of the architecture doc already
states for `Patch` — "the scope's rows for the keys present … are exactly
these" — and the duplicate-key error goes away: two rows with the same
key are that key's group.

So a batch asset can return its rows flat, and `Each` hands the store the
same thing by key:

```python
Patch(pd.concat(frames), remove=deleted)                                 # rows carry `path`
Patch({"ICP/Results/run-17.csv": df_17, "ICP/Results/run-18.csv": df_18},
      remove=["ICP/Results/old.csv"])                                    # by key; the store stamps `path`
```

PostgresStore runs either as one transaction per output:

```sql
DELETE FROM analytical.icp_samples WHERE path = ANY($upserts ∪ $removes);
INSERT INTO analytical.icp_samples … ;     -- the rows of upserted keys
```

For the by-key form the store stamps the key column into each row, as it
stamps `partition_column`, and rejects a row whose own value disagrees.
Uniqueness inside a group is the store's business (`primary_key`).
`example/brimstone.py`'s `qaqc_samples`, keyed by `file_id`, becomes valid
as written. An index has one entry per key: 3,000 files of 200,000 rows
are 3,000 entries.

**The version of a key** is one rule, the group production of the
canonical digest grammar (`resolved-commits.md`), so Python and Arrow
input agree and the definition is versioned with the grammar:

```
group(rows) = XXH3-128( grammar header ‖ "G" ‖ varint(n) ‖ sorted(row(r₁) … row(rₙ)) )
row(r)      = the canonical 16-byte row digest of r without the key column
```

| Case | Version |
|---|---|
| the same rows in another order | the same: a group is a multiset, as a table is |
| a row duplicated | different: a multiset counts |
| the key column present or not in the returned rows | the same: it is excluded |
| a key with one row | `group([row])` — the same rule, no special case |
| a key given no rows (`Patch({k: []})`) | `group([])`, a live key with empty content — not a remove |
| a declared `revision=` column | its value, which every row of the group must share; else a write error |
| a `keyed=True` output | the canonical digest of its one value (it holds values, not rows) |

An empty group keeps "processed, nothing in it" apart from "gone": the key
stays live in the index, and downstream consumers see an upsert with no
rows. Moving one-row keys from a row digest to `group([row])` changes
every existing version once, together with the grammar's own version.

## 7. Key extraction belongs to stores, and stays columnar

A write's `(key, version)` rows feed the key index. Today the worker
extracts them with `solera.stores.key_rows` for replacements — native
`Rows` built from Arrow in place or from packed Python keys, never a
per-key Python object, which is what keeps 100M keys under 1 GB
(`key-index-costs.md`) — and with `key_map`, a `dict[str, bytes]`, for
patches. Stores own data shape, so the extraction becomes a store hook
that returns the native form:

```python
class Store(Protocol):
    ...
    def key_rows(self, write: Any, output: Output) -> Rows: ...   # optional
```

| Store | `key_rows` |
|---|---|
| default (absent) | `solera.stores.key_rows`: `list[dict]`, `dict`, DataFrame (through DuckDB), anything with `__arrow_c_stream__` |
| PostgresStore | the default for flat rows; for `Patch({key: frames})`, the concatenated Arrow table it is about to insert, through `Rows.arrow(table, key, revision)` |
| a custom store for a custom type | builds `Rows.arrow(…)` from its own columnar form, or `Rows.objects(…)` |
| `Sql` writes | unchanged: after writing, the store reports sorted `(key, version)` chunks; a key may repeat, and its per-row versions fold into one |

- `Rows` is the only currency: an opaque native handle of keys and
  versions, sorted and grouped natively. Neither the worker nor the
  engine sees a row.
- **Grouping is native**: `Rows` sorts by key with the existing
  permutation, digests rows in parallel, and folds each run of equal keys
  into `group(…)`; for the by-key form, keys with no rows come as a
  separate packed list. The native work lands with the digest grammar.
- **Patches move off `key_map`** onto the same `Rows` (removes as a packed
  key list). That is also what the HTTP resolver needs — the worker's
  sorted run of `(key, version, deleted)` — so one path serves
  replacement, patch, resolve and merge-join.
- **The only per-key Python objects are an `Each` page's**: a
  `dict[key, value]` of at most `batch_size` entries.

## 8. Errors

Solera ships four exception classes. Users subclass them to say how their
own errors behave:

```python
class Unprocessable(solera.Rejected): ...       # Brimstone's
class Throttled(solera.Transient): ...          # raise Throttled(retry_after=30, retry_for="2h")
class GraphAuthExpired(solera.Abort): ...

project = Project(..., errors={httpx.TimeoutException: Transient, msal.AuthError: Abort})
```

`errors=` classifies exceptions that users cannot subclass; the first
matching entry in method resolution order wins.

`Transient(message, *, retry_after=None, retry_for=None)`: `retry_after`
is the earliest next try (seconds or a duration string), `retry_for` how
long the key keeps retrying, counted from its first failure, before it
becomes failed. A subclass sets its default as a class attribute; the
base default is 24 h:

```python
class Throttled(solera.Transient):
    retry_for = "2h"
```

| Raised in the per-key call | Means | The key becomes | Retried automatically |
|---|---|---|---|
| `Rejected` | the input is bad, and that is expected: an empty file, a template | rejected; keeps its previous output | when its input revision changes |
| `Failed`, **and any unclassified exception** | unexpected: a bug, a format nobody handles | failed; keeps its previous output | when its input revision changes, and once per new project revision (§13) |
| `Transient` | will probably work later: throttling, timeouts | retrying | with backoff, 1 min doubling to 6 h, honouring `retry_after`; after `retry_for` (default 24 h), failed |
| `Abort` | the attempt itself is broken: credentials, the database | unchanged | the whole attempt, per `retries=`; nothing commits |

Every class can also be retried on demand: `solera retry icp --failed`
(`--rejected`, `--all`, `--partition`), or a button in the asset's Keys
view. A `version=` bump reprocesses every key regardless.

Rejected and failed differ in more than colour: a failed key gets another
try when the code changes, because the bug may be fixed; a rejected key
does not, because the file is the problem. Failed is red and alertable;
rejected is expected noise. Every automatic retry is bounded — none loops.

**Outside the per-key call** — loading the page, writing to a store — an
exception fails the attempt as today. On a non-`Each` asset the classes
apply to the attempt: `Rejected` fails the task without retries,
`Transient` is retried after `retry_after` or the backoff, `Failed` and
`Abort` follow `retries=` (today's behaviour).

**Stale** means a key's output does not reflect its current input
revision. A key is stale exactly when it is in the failure index:

| `a.csv` | Outcome | The table holds | State |
|---|---|---|---|
| ctag 7 | ok | ctag 7's 42 rows | current |
| ctag 8 | rejected — a half-edited save | still ctag 7's 42 rows | stale |
| ctag 9 | ok | ctag 9's rows | current |

Keeping the previous rows is deliberate: a bad save should not delete
data, and the stale mark makes it visible.

**Runs stay `succeeded`** with per-key counts on the attempt
(`{ok: 3, rejected: 1, failed: 1, removed: 1}`). Failing belongs to a key,
not to a run: once a later run fixes `c`, the earlier run should not stay
red.

## 9. The failure index

A key that did not succeed must be remembered until it does, and a
systemic failure — a bug that throws on every file of a 1M-key full
delivery — must not put 1M entries into engine state or the checkpoint.
So the failing set is not a map in state: it is a **key index** per
`Each` asset and scope, in the format and machinery of every other index
(`object-store-state.md` §6), under `keys/@{asset}/{scope}/`.

**An entry** is `key → version`, the version packing what a retry needs:

```
outcome u8 · tries u8 · epoch varint · since varint · next_at varint · until varint · revision (len, bytes) · message (len, ≤ 200 bytes)
```

`outcome` is rejected, failed, retrying or interrupted (§5, cancel);
`revision` is the upstream version that failed; `epoch` the project
revision number it last failed under (§13); `since` when the key first
failed; `next_at` when a retrying or interrupted key is due; `until`
when a retrying key turns failed (`since + retry_for`). A key that succeeds, is
removed upstream, or leaves the edge's patterns gets a tombstone. The
error message is kept with the entry, so retention of the history (§10)
never orphans a failing key's explanation; a systemic failure repeats one
message, which block compression absorbs.

**Who writes it.** The worker, like every delta: it resolves the page's
outcomes (at most `batch_size` entries) against the failure index through
the HTTP resolver, which persists nothing, and uploads the resulting
delta file next to its output deltas. It is engine metadata, not store
data: it is not one of the fence's intents, and an attempt that never
commits leaves it as garbage. The engine commits it with the watermark,
so a page's outputs, position and failures land together.

**What state holds** per asset and scope — constant size, whatever the
failure count:

```
Failures  index: KeyIndex   counts: {rejected, failed, retrying}
          due: earliest next_at   epoch_min: lowest epoch among failed keys
          forced: {classes, at}?   (an operator's `solera retry`)
```

The engine keeps `due` and `epoch_min` exact from its cached copy of the
index, on the maintenance thread after each commit. When the index is not
in the cache or is past the inline limit, they are lower bounds, and the
worker that finishes a retry pass reports the exact values for what
remains.

**When retries come due.** An `Each` asset normally runs when its upstream
changes. A retrying key needs its own clock, or a quiet afternoon would
never retry it:

```
10:00  c raises Throttled(retry_after=60)       → c retrying, next_at 10:01
10:01  no new events; icp(site=oakland) has due ≤ now
       → the engine starts a run of that asset and scope; its page is [c]
       → c ok → tombstone; due moves to the next key, or none
```

A scope is due when `due ≤ now`, when `epoch_min` is below the current
epoch (failed keys get one try per deploy), or when `forced` is set. Only
automated assets are started by the clock; an asset run by hand picks up
due keys on its next run.

**Every due key is retried**; the only question is pacing. Due retries
form **pages of their own**, up to `batch_size` keys: a retry pass is a
drain over the failure index with its own position in the watermark
(`retry: {after}`), selecting due entries.

- If the engine holds the index in its cache and the due keys number at
  most `inline_max`, it **inlines** them into `.spec` with their
  revisions, as it inlines downstream changes (`resolved-commits.md`).
- Otherwise `.spec` pins the failure index and the worker pages through
  it, skipping entries that are not due.

When both retries and new changes are pending, the scope **alternates**: a
retry page, then a change page, and so on — the watermark records which
kind went last. Neither starves and there is no fraction to tune: a retry
storm of 1M failed keys after a deploy halves the pace of new files
instead of stopping them, and a busy day of new files still drains the
retries. When only one kind is pending, every page is that kind.

A due key whose upstream has changed since it failed is skipped by the
retry page — the worker compares the entry's `revision` with the pinned
upstream index — and arrives with the change window instead, so it is
processed once, at its new revision.

**Bounds.** State is constant per scope. A systemic failure of 1M keys is a
1M-entry index on the object store, compacted like any other, and its
recovery — a deploy that fixes the bug, or `solera retry --failed` — is a
paged drain on workers. Nothing about it runs on the engine's scheduling
path.

## 10. History and the Keys view

`key_outcomes` is a new history table, one row per key an attempt
processed:

| Column | |
|---|---|
| `run`, `attempt` | `attempt` holds `attempts.id`, as `materializations.attempt` and `lineage.attempt` do |
| `asset`, `scope`, `key` | |
| `revision` | the upstream version it processed |
| `outcome` | `ok`, `rejected`, `failed`, `retrying`, `removed` |
| `error` | class and message |
| `duration` | seconds in the call |
| `at` | |

The rows travel in the attempt's `.result` (at most `batch_size` of them) and
the engine appends them at settlement. They are filed by `run`, so
retention drops them with their run; the failure index is state and never
expires. The table counts no rows: how many rows a key produced is the
store's knowledge.

Live, over the HTTP channel: a started/finished event per key, so the
console shows a page's progress, and `ctx.log` inside the call tags each
line with the key. The final log keeps the tags.

The asset's **Keys** view lists the failure index (rejected, failed,
retrying, with messages and due times) and searches `key_outcomes`.
`solera explain icp "ICP/Results/run-17.csv"` answers the most common
question — why is my file not in the table:

```
ok, from ctag 9 at 10:31 (run 01J9…)
rejected at ctag 7: header row 2 has no "Sample ID"
excluded by "archive" (**/archive/**)
not matched: outside include "ICP/Results/**/*.csv"
```

Exclusions are explained from the manifest's patterns at the time of the
question, not stored per key.

## 11. Key patterns on edges

```python
Each("sharepoint_files", include="ICP/Results/**/*.csv", exclude={"archive": "**/archive/**"})
Incremental("sharepoint_files", include=["XRF/**/*.csv", "XRF Data/**/*.csv"])
```

Globs over key strings — `**` crosses `/`, `*` does not — or
`Regex("…")`. `include` is a pattern or a list; `exclude` a dict of named
patterns (named, so `explain` can say which rule) or a list. They apply to
`Each` and `Incremental` alike, and compile to one native matcher shared
by engine and worker.

**The worker is authoritative.** It filters every page it reads, so
correctness never depends on what the engine knew. **The engine evaluates
patterns only to skip work**, and only from what it already holds:

1. **Pruning.** Each include pattern has a literal prefix
   (`ICP/Results/`). A delta file whose `[min, max]` key range misses every
   include prefix cannot match: no read.
2. **At commit, from the cache.** When the engine commits a delta, it holds
   the file — it wrote or fetched it for the engine cache. On the
   maintenance thread it matches the delta's keys against each consuming
   edge's patterns and keeps, per edge and batch, the count of matching
   keys (and the keys themselves when they fit `inline_max`). That is
   per-delta work, proportional to the commit, never to the index.
3. **At prepare, from those counts.** A window whose batches all matched
   nothing advances the watermark with no attempt (the existing `skipped`
   outcome). A window whose matches are known and few is inlined. Anything
   unknown — after a restart, or a delta never cached — is launched, and
   the worker filters it.

The engine never waits on S3 to decide, and never scans an index. A
consumer that cares about one site matches nothing in the others: those
scopes are skipped at commit time.

**A pattern change is a key-set diff, not a reset.** The watermark records
the patterns it was delivered under. When the manifest changes them, the
edge enters a **rescope drain**: the worker pages through the upstream
index at the pinned head, as a full delivery does, limited to the key
ranges the old and new include prefixes cover, and delivers only the keys
whose match changed:

| Key | Delivered as |
|---|---|
| matched before, not now (a new `exclude`) | removed: its rows go, its failure entry too |
| matched now, not before (a widened `include`) | upserted, at its current version |
| matched both times | nothing |

Changes committed while the drain runs arrive afterwards as deltas, under
the new patterns — the full drain's rule. Adding an `archive` exclusion
removes the archived keys and processes nothing else. Patterns are not
part of the interpretation fingerprint.

**Matching is by key segments, not substrings.** Monolith's
`"old" in name.lower()` (`icp.py:83`) drops `Gold_ore.csv` and
`latest.csv`; `**/*old*` would too. Brimstone's helpers should compile
word rules to segment-anchored patterns (`**/old/**`, `**/old *`,
`**/* old.*`).

## 12. Observable sources

A `Source` today is an output with no producer, advanced from outside
through the commit API. An observable source adds `observe()`, which an
automation calls; its result is committed exactly as a commit-API call:

```python
class DatasmartTable(Source):
    """A table another system writes; Solera watches its version."""
    def __init__(self, name):
        super().__init__(name, store="postgres", schema="datasmart", observe=Every(300))

    def observe(self, ctx, db: Datasmart) -> str:                     # unkeyed: a version
        return db.scalar(f"SELECT max(updated_at)::text FROM datasmart.{self.name}")


class Landing(Source):
    def observe(self, ctx, s3: S3Client):                              # keyed: the full map
        return {o.key: o.etag for o in s3.list(self.handle["bucket"], self.handle["prefix"])}


class Feed(Source):
    def observe(self, ctx, api: FeedClient) -> Observed:              # keyed, big: a cursor
        page = api.changes(since=ctx.cursor)
        return Observed(upsert=page.upserts, remove=page.removes, cursor=page.token)
```

| `observe()` returns | Committed as |
|---|---|
| `str` | `commit(name, version=…)` |
| `dict[str, str]`, or Arrow data with key and revision columns (read through `Rows.arrow`) | `commit(name, keys=…)`: the full map |
| `Observed(upsert, remove, cursor)` | `commit(name, upsert=…, remove=…)`, with the cursor in the same commit |

An identical result is not a change and wakes nothing. Subclasses share
one `observe` across a family of sources; a `@source` decorator is sugar
for one-offs. Use a cursor once the full map is large: comparing a 10M-key
map every 5 minutes is a full replacement every 5 minutes.

**Where it runs.** The trigger is the engine's — the `Every(300)` clock
holds no user code, and the server runs none (architecture §1). The
`observe()` body is user code with secrets, so it runs on a placement
like any task: `Local` by default (a subprocess on the engine's machine,
a second or two of imports per tick), or a `Pool` for tight intervals,
whose long-lived workers keep the project imported — the shape of
Dagster's code server.

**An unchanged observation is cheap.** Under the HTTP lifecycle, an
observation needs none of an attempt's durable objects until it changes
something. How observations are launched and claimed is `lifecycle.md`'s;
what this proposal needs from it is the left column:

| | Unchanged | Changed |
|---|---|---|
| `.spec` | none: it rides the launch or the claim over HTTP | same |
| `.worker` | none on `Local`; a Pool claim creates it — at most one small PUT, unless `lifecycle.md` lets observation claims stay in memory | same |
| `.result` | none: an HTTP report; for a keyed map, the HTTP resolver answers "no delta" from the engine cache | the delta file (keyed), uploaded by the worker as for any commit |
| `.writing` fence | none: an observation writes no store data | none |
| Journal | nothing | the source commit, with its cursor |
| Logs | live only | the final log |
| History | its `runs` and `attempts` rows, outcome `skipped`, kind `observe` | as any run |
| Restart | an observation in flight may vanish; the next tick reruns it | the commit is durable once journaled |

So a skipped observation costs one launch or claim, one HTTP report and a
few history rows riding the shared history flush — no object writes of its
own. Skipped observation runs are hidden from the runs list by default and
kept a day (`Retention` for kind `observe`, outcome `skipped`); an
observation that changed its source is a normal run with lineage and
normal retention. That is the pattern elsewhere: Dagster keeps sensor
ticks out of runs and purges skipped ones, Airflow's triggerer runs cheap
checks in one long-lived process, Temporal polls inside long activities.
Solera keeps observations as runs for uniformity — logs, placement,
cancel, history — and makes the empty ones nearly free instead.

## 13. Project revision from a build identity

The project revision is today a digest of the manifest, which includes a
`code_hash` per asset: a hash of the asset's whole source file. That is
both noisy and blind — a cosmetic edit in `icp.py` makes a new revision,
a real fix in a helper `parsers.py` does not, so `OnDeploy()` misses
helper-only deploys. Nothing invalidates on it; invalidation is the
explicit `version=` and the interpretation fingerprint.

Proposal: drop `code_hash` from the manifest; the revision is
`H(manifest, build)`, where `build` is `SOLERA_BUILD` if set, else the git
commit of the project (with a dirty flag), else a hash of the files of the
project's package. The engine numbers revisions as it serves them — the
**epoch** — which the failure index uses to give failed keys one try per
deploy without rewriting any entry: a failed key is due when its `epoch`
is below the current one.

## 14. Conventions for users (Brimstone's, not Solera's)

- **Per-key functions are pure: one file → its rows.** Logic across files
  moves downstream: `xrf_incremental`'s `drop_duplicates` only dedupes
  files that land in the same page. So do joins with slowly changing
  tables: `bet` takes `sample_id_crosswalk` as a whole input, so each
  crosswalk change would reprocess every BET file. `bet_raw` per file, then
  a SQL asset joins the crosswalk, recomputed in Postgres in seconds.
- **`SharePointFiles(site, folder, suffix, exclude=Exclude(folders=…, words=…))`**
  is a helper that returns an `Each` with compiled patterns (§11).
- **`class Unprocessable(solera.Rejected)`**, raised for files that are
  bad on purpose.
- **Sample tracing** is a downstream asset Brimstone builds from output
  metadata: `Output(…, meta={"samples": {"original": "original_sample_id",
  "normalized": "sample_id"}})` (a new free-form `meta=` on `Output`,
  mirroring `In(meta=)`), and a factory that finds every such output and
  builds one `Incremental` asset maintaining `trace.observations`. Solera
  sees only metadata.

## 15. What the newer design changes in this proposal

**Simpler.**

- `Each` pages are small writes — `batch_size` keys — so their commits
  always take the HTTP resolver: exact counts, no index reads on the
  worker, and the worker uploads its own deltas, failure delta included.
- Pattern evaluation, inlined retries and "is this observation a change"
  are all answered from the one warm engine cache, which `resolved-commits`
  builds anyway; this proposal adds readers, not a cache.
- The failure index is an ordinary key index: format, compaction, cache,
  delta naming, garbage collection — none new.
- Indexes count keys, not rows: a file of 40 samples is one entry.
- Per-key progress and key-tagged logs are live events on the HTTP
  channel, not objects.

**Harder.**

- The canonical digest grammar must define the group production, which
  is now every key's version, and be versioned (§6).
- `Rows` must group natively, and patches must move onto `Rows` (§7).
- The watermark gains two positions: the rescope drain (§11) and the retry
  pass (§9).
- Observations want a launch path with no `.spec` or `.result` (§12),
  which only the HTTP lifecycle makes possible.
- Cancel commits a partial page (§5): the worker, not the engine, takes
  the fence on cancel while it answers.

**Obsolete**, from the earlier draft of this proposal.

- `Store.keys(write, output) -> dict[str, str]`: replaced by
  `Store.key_rows → Rows` (§7). A dict per write is the Python-object path
  that cost ~45 GB at 100M keys.
- "The engine reads small delta files to filter": replaced by evaluation
  at commit from the cache (§11).
- A failing set as a map in engine state: replaced by the failure index
  (§9).
- Retrying failed keys when an asset's code hash changes: replaced by the
  epoch (§13).
- A `rows` count per key in `key_outcomes`: dropped.
- Filters in the poller (`example/brimstone.py`'s `is_qaqc_workbook`):
  replaced by patterns on the consumer's edge.
- `grouped=True` and the duplicate-key error: every key is a group (§6).

## 16. Interactions with work in flight

| Work | What this proposal needs from it |
|---|---|
| Key index (`object-store-state.md` §6) | No format change. A new kind of index (`keys/@{asset}/{scope}/`, the failure index) compacted like the others; `Rows` groups every key (§6), with `Store.key_rows` and the digest grammar — the native thread's current work; patches build `Rows`. |
| Engine cache (`resolved-commits.md`, being rewritten) | New readers on the maintenance thread: pattern counts at commit, failure-index `due`, inlined retry keys. No new cached content beyond failure indexes. |
| HTTP resolver (`resolved-commits.md`) | Each pages and failure deltas are small resolves; the worker uploads both. Inlined windows are filtered before the `inline_max` check; observations resolve to learn "unchanged". The grammar gains the group production. |
| Attempt lifecycle (`lifecycle.md`, being written) | Observation launches without `.spec` or `.result`, and ideally without `.worker`; live per-key events and key-tagged logs; per-key outcomes in `.result`; cancel delivered to the worker, which commits finished keys before the engine would take the fence (§5). |

## 17. What changes in the code

- `python/solera/sdk.py`: `Each`; `include`/`exclude` on `Incremental`;
  `Output(meta=…)`; `Rejected`, `Failed`, `Transient(retry_after,
  retry_for)`, `Abort`, `Project(errors=…)`; `Source.observe`, `Observed`;
  `ctx.key`, `ctx.revision`, `ctx.keys(output, prefix=)`; the revision from
  a build identity; `code_hash` removed.
- `python/solera/stores.py`: `Patch({key: value})`; the `key_rows` hook;
  no duplicate-key error.
- `python/solera_postgres`: group writes, `key_rows` for by-key patches,
  keyed loads as `dict[str, T]`.
- `python/solera_worker/worker.py`: the per-key loop, classification,
  outcomes, the failure delta, retry and rescope pages, partial commits on
  cancel, observations.
- `python/solera_server/engine.py`: the `Failures` record, the due clock,
  epochs, pattern counts at commit, rescope and retry positions on the
  watermark, alternation of retry and change pages, cancel sent to the
  worker, observation runs.
- `python/solera_server/history.py`: `key_outcomes`; per-key counts on
  `attempts`.
- `native/`: the group digest and grouping in `Rows` (in progress), the
  pattern matcher.
- `example/brimstone.py`: the §4 shape; `qaqc_samples` becomes an
  `Each`.
- Docs: architecture §2 (outputs), §4 (writes, store hook), §5 (edges,
  sources), §6 (incrementality), §8 (runs and errors), §9 (observations),
  §11 (revision); `object-store-state.md` §5–7.

## 18. Tests

- `Each` delivers exactly what an equivalent batch asset returning
  `Patch({key: …})` writes, over random pages, deletes and failures.
- Group versions: invariant under row order and key-column presence,
  sensitive to duplicates; flat rows and the by-key form give the same
  versions; Python and Arrow input give the same digests; empty groups are
  live keys.
- Each error class in and out of the per-key call; `errors=` mapping;
  `Transient` turning failed after its `retry_for`; failed keys retried
  once per epoch and never more.
- Cancel: finished keys commit whatever their order; unfinished keys are
  interrupted and retried by the next run; a fence already taken
  completes; an unresponsive worker loses the page to the engine's
  fence.
- Failure index: a 1M-key systemic failure leaves state constant; retry
  passes inline when small and page when big; retry and change pages
  alternate when both are pending; a due key changed upstream is
  processed once; due bounds exact after a pass.
- Patterns: the worker's and engine's matchers agree; pruning never drops a
  match; a cold engine launches and the worker filters; a rescope drain
  delivers exactly the symmetric difference.
- Observations: unchanged ticks write no objects; a changed tick commits
  once; a restart mid-observation drops it harmlessly.

## 19. Open questions

1. When a few keys at a time come due while new changes keep arriving,
   alternation runs small retry attempts between full change pages. Each
   costs a whole attempt; acceptable as is, or should a retry page wait
   until it is full or its oldest key has waited long enough?
