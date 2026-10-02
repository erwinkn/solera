# Per-key processing

Status: **§5–§11 and §13 built** (error classes, build identity, `Each`,
groups by key, the failure index, retry passes, forced retries, the drain on
cancel, `key_outcomes`, key patterns and the rescope cutover); the engine's
match-count hints (§11) and summary recomputation (§9) are deferred, and
`solera explain` (§10) is not built; §12 follows `lifecycle.md` §11
(sensors). §20 records where the build departs from this
text. It adds an `Each` edge (an asset written
for one key, run over every changed key), keys that hold many rows,
per-key outcomes with user-classified errors, key patterns on edges, and
observable sources. It builds on the engine cache, the HTTP resolver,
inlined changes of `resolved-commits.md`, the canonical row digest of
`row-digest.md`, and on the worker → engine HTTP channel and attempt
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
`exclude` patterns on an edge select keys by name; the worker evaluates
them on every page it reads. A `Source` subclass with `observe()` is
polled by a sensor and committed like the commit API. Throughout, the engine sees
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
  how much work a crash throws away. **`concurrency`** is keys in flight
  within an attempt: a semaphore for an `async` function, a pool of
  threads for a plain one.
- **Neither bounds row memory.** Both count keys: a page of 100 keys holds
  whatever rows those keys produce, and one 2 GB workbook is still one
  key. A per-key function that can produce huge groups needs a smaller
  `batch_size` and `concurrency`, chosen by its author; Solera does not
  measure rows.
- **Every output of the asset is keyed by the input's key** — `key=`
  (the rows the call returns, any number) or `keyed=True` (one value).
  Unkeyed outputs are rejected at registration. The call returns a value
  (one output) or `Result(outputs=…)`. An output the call does not return,
  or returns as `None`, is left as it is for that key: no change. Removal
  is explicit — `Patch(None, remove=[ctx.key])` takes the key out of that
  output.
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

**Cancel and timeout keep finished keys.** They follow the attempt's
two-phase cancel (`lifecycle.md` §7); for a per-key page the phases are:

1. **Cancel requested** — the cancel record (`lifecycle.md` §2.2) reaches
   the worker with phase `requested`. The worker stops starting keys and cancels the calls in flight (an `async` call is
   cancelled; a thread is abandoned and its result ignored). Then, within
   `cancel_grace` (60 s by default, per asset), it **drains**: it writes
   the keys that finished — one store write per output, as for a whole
   page — records the keys it did not finish as **interrupted** in the
   failure delta by the record's `reason` (table below), and publishes
   all of it as one `.result` with `status: canceled`, carrying the record
   as §2.2 says. The engine commits outputs, failure delta and watermark
   as one journal decision.
2. **Forced abort,** after `cancel_grace` without a result: the record's
   phase becomes `forced` and the attempt ends as `lifecycle.md` §7
   describes. Nothing of the page commits and it is delivered again;
   whether its writes may still land is the attempt's write-completion
   evidence (`lifecycle.md` §2.3), and repair follows from it. Only a
   worker that cannot drain in time loses finished work.

Finished keys need not be a key-order prefix of the page — with
`concurrency=16`, `a c d` may finish while `b` is still reading. So the
watermark does not stop at the first unfinished key: it moves past the
whole page, exactly as on success, and the holes are carried by the
failure index instead.

```
page: a b c d e(deleted)   cancel requested while b, d are in flight
store.store(Patch({a: …, c: …}, remove=[e]))
.result status: canceled → one commit: watermark past e · failure index +b, +d interrupted
```

What happens to the holes depends on the record's `reason`:

| `reason` | Recorded as | Runs again |
|---|---|---|
| `user` | `canceled`, not due | only when a later run is requested for it: `solera retry` (or a forced retry), or a new change of the key upstream. A run the user stopped does not resume itself. |
| `timeout` | `timed out`, `tries + 1`, due after the retry backoff (§8) | by the retry clock (§9); once `tries` passes the asset's `retries=`, the key becomes `failed`, so a key that always outlives the timeout stops cycling |

The attempt ends `canceled` or `timed out`, with the outputs it
committed; an attempt timeout stays retryable within `retries=`, as
today.

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
canonical digest grammar (`row-digest.md`), so Python and Arrow
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
| a declared `revision=` column | its value, verbatim, which every row of the group must share; else a write error |
| a declared `revision=` column and no rows | `group([])`: there is no value to take, so an empty group always has the empty-group version |
| a `keyed=True` output | the canonical digest of its one value (it holds values, not rows) |

An empty group keeps "processed, nothing in it" apart from "gone": the key
stays live in the index, and downstream consumers see an upsert with no
rows. Loading it by key returns it: `store.load(ref, dict[str, T],
Keys([k]))` gives `{k: <empty T>}` — an empty DataFrame with the table's
columns, `[]` for rows. A flat load (`T` alone) has no way to show an
empty group; a consumer that must tell "empty" from "absent" loads by
key, as `Each` does.

The byte grammar — row digests, the group production, its header and
version — is specified next to the native code in `row-digest.md` (being
written with `Rows` grouping); this section states only what stores and
the engine rely on, and the two must stay in agreement. Moving one-row keys from a row digest to `group([row])` changes
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
| a custom store for a custom type | builds `Rows.arrow(…)` from its own columnar form, or `Rows.records(…)` |
| `Sql` writes | after writing, the store reports the content sorted by key, by one of two paths (below) |

**`Sql` writes have two paths**, because the rows never pass through the
worker (built; the `md5` per-row versions are gone):

| Output declares | The store reports | Versions |
|---|---|---|
| `revision="col"` | `SELECT key, col … ORDER BY key` through a server-side cursor | native code checks that every row of a key carries the same value, else a write error; the version is that value's text (`row-digest.md` § Revisions) — the same rule as rows from Python or Arrow |
| no `revision` | the written rows themselves, typed, in key order (`SELECT * … ORDER BY key` through a cursor; PostgresStore hands over chunks of row mappings, since pyarrow is not a runtime dependency — Arrow chunks are accepted too) | the same canonical row digests and group production as any other write, computed natively |

The first path is the cheap one: one short value per row. The second
reads back every written row, which a large `Sql` replacement pays in
transfer; declaring a revision column avoids it.

- `Rows` is the only currency: an opaque native handle of keys and
  versions, sorted and grouped natively. Neither the worker nor the
  engine sees a row.
- **Grouping is native**: `Rows` sorts by key with the existing
  permutation, digests rows in parallel, and folds each run of equal keys
  into `group(…)`; for the by-key form, keys with no rows come as a
  separate packed list. Built: `Rows.records`, `Rows.arrow`, `Rows.values`
  (`keyed=True`), `Rows.pairs` and `Rows.keys`, grouping natively, and
  `Store.key_rows`; not yet the by-key `Patch({key: frames})` form or its
  empty groups.
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
revision. That has two causes: an upstream change not yet processed — in
the pending window, or committed after it — and a failure. The first is
transient and shows as the edge's lag; the second is what the failure
index records, with a reason:

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

**This section is authoritative** for the failure record, the transition
table, the eligibility predicate, the retry-pass state and forced-request
identity. `resolved-commits.md` and `lifecycle.md` refer here, and the
SDK holds one implementation that the engine, the worker and the engine
cache's inline reader all call.

**An entry** is `key → version`, the version packing what a retry needs:

```
outcome u8 · tries varint · epoch varint · forced varint · since varint · last varint · next_at varint
       · until varint · revision (len, bytes) · message (len, ≤ 200 bytes)
```

| Field | |
|---|---|
| `outcome` | rejected, failed, retrying, canceled, timed out (the last two are interrupted keys, §5) |
| `tries` | calls at this `revision`; a varint, since a record can outlive any fixed width |
| `epoch` | the project revision number (§13) the last try ran under, copied from the spec |
| `forced` | the position of the latest forced request the last try ran under (below), copied from the spec; 0 if none |
| `since` | first failure at this `revision` |
| `last` | time of the last try: display only |
| `next_at` | when a retrying or timed-out key is due: scheduling only |
| `until` | when a retrying key turns failed: `since + retry_for` |
| `revision` | the upstream version that failed |
| `message` | class and message of the last error |

A key that succeeds, is removed upstream, or leaves the edge's patterns
gets a tombstone. The message lives with the entry, so retention of the
history (§10) never orphans a failing key's explanation; a systemic
failure repeats one message, which block compression absorbs.

**Who writes it: the worker, resolved locally.** Failure deltas never go
to the HTTP resolver. A page touches at most `batch_size` keys of the
failure index, and the worker needs their *prior records*, not just
whether they changed: tries, `since` and `until` carry over. So it does
exact point lookups of the touched keys in the pinned failure index (a
small index is read whole; a large one costs at most a block per touched
key, and retry pages arrive with their prior records inlined), applies
the transitions below, and uploads the delta next to its output deltas.
It is engine metadata, not store data: it is not one of the fence's
intents, and an attempt that never commits leaves it as garbage.

| Prior → outcome | New record |
|---|---|
| none → ok | nothing |
| any → ok, removed, or unmatched | tombstone |
| none, or a record at another `revision` → not ok | a fresh record: `tries = 1`, `since = last = now` |
| retrying → `Transient` again | `tries + 1`, `since` and `until` kept, `next_at` by backoff or `retry_after`; failed once `now ≥ until` |
| failed or rejected → the same class again | `tries + 1`, `last` updated |
| any → interrupted by a cancel | `canceled`, `tries` unchanged, no `next_at` |
| any → interrupted by a timeout | `timed out`, `tries + 1`, `next_at` by backoff; `failed` once `tries` passes `retries=` |
| any → another class | the new class, `tries + 1`, `since` kept; `until` set when it becomes retrying |

Every record a try writes takes `epoch` and `forced` from the page's
spec — engine-assigned positions, never the worker's clock — so whether a
key has had its deploy retry or its forced retry is decided causally.
`last`, `next_at` and `until` are worker times; a skewed clock shifts when
a key is retried, never whether it is.

**Counts move by explicit transitions.** Changing an entry from failed to
retrying adds and removes no key, so the index's own key count says
nothing about outcomes. The worker's result carries, per outcome, the
change its transitions made (`{failed: −1, retrying: +1}`); the engine
applies them in the same commit as the output deltas, the failure delta
and the watermark, so a page's outputs, position and failures land
together, and the counts are exact because every prior was read exactly.
Scheduling never depends on the index's approximate cardinality.

**What state holds** per asset and scope — constant size, whatever the
failure count:

```
Failures  index: KeyIndex
          counts: {rejected, failed, retrying, canceled, timed_out}   exact (transitions)
          due_min                                                 ≤ every retrying or timed-out next_at
          epoch_min                                               ≤ every failed entry's epoch
          forced: {class: position}                               latest forced request per class
```

**One eligibility predicate**, in the SDK, used by the engine to decide
that a scope has retries and by the worker to select them:

```python
def eligible(entry, now, epoch, forced) -> bool:
    return (
        (entry.outcome in (RETRYING, TIMED_OUT) and entry.next_at <= now)
        or (entry.outcome == FAILED and entry.epoch < epoch)                  # one try per deploy
        or entry.forced < forced.get(entry.outcome, 0)                        # an operator's retry
    )
```

**Forced requests are positions, not times.** `solera retry icp --failed`
(or `--rejected`, `--canceled`, `--all`) is journaled as an event; its
journal position is the request's identity, and `forced[class]` keeps the
latest position per class — constant size, and a request for one class
never cancels a pending one for another. A pass runs under the forced
positions as they were when it started (`forced_pos`, their maximum), and
every record it writes carries `forced = forced_pos`. A key has satisfied
a request exactly when its record's `forced` is at least that request's
position.

Each clause retires itself: a retried key's `next_at` moves on, its
`epoch` becomes the pass's, its `forced` the pass's position. None loops.
A `canceled` key matches no clause but the forced one: only a request, or
a new change of the key, brings it back.

**Minima are conservative, and exact at pass completion.** `due_min` and
`epoch_min` are lower bounds. Every commit — change page or retry page —
lowers them from the records it wrote (`min(due_min, next_at)` over
retrying and timed-out records, `min(epoch_min, epoch)` over failed ones),
which is O(page). Removing the record that held a minimum leaves the bound
too low: a scope may start a retry pass that finds nothing, which is safe.
Nothing rescans the index per commit. Exact values come from the retry
pass, which accumulates them as it walks (below), and replace the bounds
when it completes. A coalesced recomputation from the engine cache is a
later optimization, not v1.

**When retries come due.** An `Each` asset normally runs when its upstream
changes. A retrying key needs its own clock, or a quiet afternoon would
never retry it:

```
10:00  c raises Throttled(retry_after=60)       → c retrying, next_at 10:01; due_min = 10:01
10:01  no new events; icp(site=oakland) has due_min ≤ now
       → the engine starts a run of that asset and scope with a retry page [c]
       → c ok → tombstone; the pass completes, due_min = its due_acc (none)
```

A scope has retries when `due_min ≤ now`, `epoch_min < epoch`, or a
forced request is newer than the `forced_pos` of the last completed pass. Only automated
assets are started by the clock; an asset run by hand picks up due keys
on its next run.

**Every eligible key is retried**; the only question is pacing. Retries
form **pages of their own**, up to `batch_size` keys, in a **retry pass**:
a walk over the failure index in key order, with its position in the
watermark:

```
retry: {pass: 7, epoch: 12, forced_pos: 4031, after: "ICP/Results/run-17.csv",
        due_acc: 10:42, epoch_acc: 11}
```

| Field | |
|---|---|
| `pass`, `epoch`, `forced_pos` | the pass's identity: its number and the predicate inputs it runs under |
| `after` | the last key the pass has walked |
| `due_acc`, `epoch_acc` | minima over the **resulting records** of every key at or before `after`: `next_at` over retrying and timed-out records, `epoch` over failed ones |

- **Each retry page** walks the index from `after` until it has
  `batch_size` eligible keys or reaches the end. Its commit — atomic with
  the outputs, failure delta and watermark — advances `after` and folds
  into the accumulators every record in the walked range *as it is after
  the page's transitions*, eligible or not. The worker computes that from
  what it read; an inlined page carries the same range minima, computed
  by the same function.
- **Each change page** commits records too; the engine folds the ones at
  or before `after` into the accumulators (records past `after` will be
  walked). A change can only lower an accumulator or leave a stale
  lower value behind — conservative either way.
- **A restart** — `epoch` or `forced_pos` changes mid-pass, from a deploy
  or a new `solera retry` — starts a new pass from the first key, with
  empty accumulators, so no key before `after` is skipped. The global
  bounds stay as they were until a pass completes.
- **Completion** is the page that reaches the end of the index: `due_min`
  and `epoch_min` become `due_acc` and `epoch_acc`, folded with that
  page's own records, in the same commit. If anything is still eligible —
  it became due behind the walk — the next pass starts.
- If the engine holds the index in its cache and the next eligible keys
  number at most `inline_max`, it **inlines** them, with their prior
  records, into `.spec`; otherwise `.spec` pins the failure index and the
  worker pages through it with the same predicate.

When both retries and new changes are pending, the scope **alternates**: a
retry page, then a change page — the watermark records which kind went
last. Neither starves and there is no fraction to tune: a retry storm of
1M failed keys after a deploy halves the pace of new files instead of
stopping them. When only one kind is pending, every page is that kind.

A retry-eligible key whose upstream has changed since it failed is skipped
by the retry page — the worker compares the entry's `revision` with the
pinned upstream index — and arrives with the change window instead, so it
is processed once, at its new revision.

**Bounds.** State is constant per scope. A systemic failure of 1M keys is a
1M-entry index on the object store, compacted like any other. Each page
does O(`batch_size`) lookups and O(`batch_size`) bound updates; nothing
rescans the index per commit. Its recovery — a deploy that fixes the bug,
or `solera retry --failed` — is a paged pass on workers, off the engine's
scheduling path.

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

**The worker filters.** It filters every page it reads, so correctness
never depends on what the engine knew. In v1 the engine does not evaluate
patterns: a window with changes launches an attempt, and a page whose
keys all fall outside the patterns ends `skipped` after advancing the
watermark.

*Later: engine-side skip hints.* An optimization once wasted launches are
measured; the engine would evaluate patterns only to skip work, and only
from what it already holds:

1. **Pruning.** Each include pattern has a literal prefix
   (`ICP/Results/`). A delta file whose `[min, max]` key range misses every
   include prefix cannot match: no read.
2. **At commit, from the cache.** When the engine commits a delta, it holds
   the file — it wrote or fetched it for the engine cache. On the
   maintenance thread it matches the delta's keys against each consuming
   edge's patterns and keeps, per edge and batch, the count of matching
   keys (and the keys themselves when they fit `inline_max`). That is
   per-delta work, proportional to the commit, never to the index. Each
   count is tagged with the fingerprint of the patterns it was computed
   under, and is used only while the edge delivers under those patterns:
   a zero counted for old patterns never lets the engine skip a batch the
   new ones might match.
3. **At prepare, from those counts.** A window whose batches all matched
   nothing advances the watermark with no attempt (the existing `skipped`
   outcome). A window whose matches are known and few is inlined. Anything
   unknown — after a restart, or a delta never cached — is launched, and
   the worker filters it.

With the hints, the engine never waits on S3 to decide and never scans an
index, and a consumer that cares about one site would skip the others at
commit time instead of launching for them.

**A pattern change is a key-set diff, not a reset** — taken in three
steps around a **cutover batch**, so that no pending change is judged by
the wrong patterns. Take an `archive` exclusion deployed while the edge
has unconsumed deltas:

```
watermark at batch 40, head at 45; the new manifest adds exclude "archive"
batch 43 deleted archive/a.csv, which still has rows downstream
```

1. **Cut over.** The engine fixes `c` = the upstream head when it serves
   the new patterns (45) and records the transition on the watermark:
   `rescope: {from: old, to: new, cutover: 45, snapshot: <files at 45>}`.
2. **Finish under the old patterns.** Deltas up to `c` are delivered
   under the patterns they were committed for: batch 43's deletion of
   `archive/a.csv` matched before, so its rows are removed. Without this
   step, the new exclusion would hide the deletion and the rows would
   survive forever.
3. **Diff against the snapshot at `c`.** The worker pages through the
   upstream index *as of batch 45* — the snapshot recorded in step 1,
   pinned and protected from garbage collection for the whole drain, not
   re-pinned to the current head on each page as a full delivery is —
   limited to the key ranges the old and new include prefixes cover, and
   delivers only the keys whose match changed:

   | Key at `c` | Delivered as |
   |---|---|
   | matched before, not now (the new `exclude`) | removed: its rows go, its failure entry too |
   | matched now, not before (a widened `include`) | upserted, at its version in the snapshot |
   | matched both times, or neither | nothing |

4. **Continue under the new patterns** from batch `c + 1`. Changes
   committed during steps 2 and 3 wait for this step.

Adding an `archive` exclusion removes the archived keys and processes
nothing else. **Pattern changes are serialized:** a manifest that changes
the patterns again while a transition runs does not interrupt it; when
the transition ends, if the served patterns differ from its `to`, the
next transition starts with its own cutover. The watermark holds at most
one transition. Patterns are not part of the interpretation fingerprint.

**Matching is by key segments, not substrings.** Monolith's
`"old" in name.lower()` (`icp.py:83`) drops `Gold_ore.csv` and
`latest.csv`; `**/*old*` would too. Brimstone's helpers should compile
word rules to segment-anchored patterns (`**/old/**`, `**/old *`,
`**/* old.*`).

## 12. Observable sources

A `Source` today is an output with no producer, advanced from outside
through the commit API. An **observable source** adds `observe()`, which
Solera calls on a schedule and commits like a commit-API call. It is sugar
for a **sensor** (`lifecycle.md` §11): `Source(name, observe=Every(300))`
declares a sensor `{name}.observe` whose body calls `observe()` and returns
a `Commit` to its own source.

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

| `observe()` returns | The sensor's `Tick` |
|---|---|
| `str` | `Commit(name, version=…)` |
| `dict[str, str]`, or Arrow data with key and revision columns | `Commit(name, keys=…)`: the full map |
| `Observed(upsert, remove, cursor)` | `Commit(name, upsert=…, remove=…)` and `cursor=` |
| `None` | nothing |

Subclasses share one `observe` across a family of sources; a `@source`
decorator is sugar for one-offs. A sensor that does more than advance its
own source — commits to several sources, requests runs — is written with
`@sensor` directly; `observe()` covers the common case.

**Where it runs.** In a **sensor host**: a long-lived process with the
project loaded, so a tick costs a function call, not a process start and
an import. By default the engine keeps one host subprocess beside it;
`observe=Every(300), executor=Pool("sensors")` runs the tick on a remote
host instead, for sources the engine's machine cannot reach. The engine
holds the clock and no user code: a host long-polls for due ticks, runs
the body, and posts the outcome. A tick is not an attempt — no `.spec`,
`.worker`, `.result`, fence or run of its own.

**How a tick lands.** Observable sources follow the lifecycle's
source-head identity and snapshot contract for sensors (`lifecycle.md`
§11): the engine dispatches each tick with an opaque, monotonic identity
of the source's head — for unkeyed sources as for keyed ones, so a string
version committed by an API client in between is caught too — plus, when
the host may resolve a big map, the pinned index manifest, whose reader
pin lasts until the tick ends. `observe()` declares its one commit target,
its own source, so the dispatcher knows which head to send. The posted
outcome is applied in one step, through the commit API's own checks:

- **Head-checked.** If the source's head identity moved since dispatch —
  an API client, another sensor — the tick is refused and the next one
  observes again. A stale full map must never undo a newer commit.
- **"Unchanged" means nothing durable changed**: no new version, no key
  change, **and no new cursor**. A cursored feed often returns an empty
  page with a newer token; saving that token is what moves the feed on.

| Tick | Records | Wakes consumers |
|---|---|---|
| changed (version or keys) | the source commit, and the cursor if it moved, in one journal record | yes |
| cursor only | `SensorAdvanced {sensor, cursor}`: no delta, no new version | no |
| unchanged | nothing | no |
| raised | nothing; the cursor stays; a `failed` tick row | no |

**Key maps.** A full map up to `sensor_map_max` (1M keys) is posted as a
sorted run and resolved by the engine in-process against the source's
index, as API commits are. A bigger one is resolved **on the host** — the
streaming merge-join against the pinned manifest the tick carries — which
uploads the delta file and posts a reference; the engine installs it only
if the head identity is unchanged. A refused or repeated post and its file
follow the lifecycle's retry-safe sensor rules. Past a few million
keys, use a cursor and `Observed(upsert, remove)`: comparing a 10M-key map
every five minutes is a full replacement every five minutes.

**What it costs.** An unchanged tick is a long-poll answer, a function
call on a warm host, an HTTP post, and a row in the `ticks` history table
(buffered, written with the next history flush, kept a day). No object
writes, no journal entry, no run. A tick that committed is recorded as a
source commit — a run with no tasks, as API commits are — and its tick row
is kept as long as that run. After an engine restart no tick is in
flight; each sensor is due again at its next interval, from its durable
cursor.

This is the pattern elsewhere: Dagster evaluates sensors in a code server
and keeps ticks out of runs, Airflow's triggerer runs cheap checks in one
long-lived process, Temporal polls inside long activities.

## 13. Project revision from a build identity

The project revision is today a digest of the manifest, which includes a
`code_hash` per asset: a hash of the asset's whole source file. That is
both noisy and blind — a cosmetic edit in `icp.py` makes a new revision,
a real fix in a helper `parsers.py` does not, so `OnDeploy()` misses
helper-only deploys. Nothing invalidates on it; invalidation is the
explicit `version=` and the interpretation fingerprint.

Proposal: drop `code_hash` from the manifest; the revision is
`H(manifest, build)`, where `build` must identify the code exactly:

- `SOLERA_BUILD` when set — an immutable identifier from CI or the image
  (a commit SHA of a clean checkout, an image digest);
- otherwise a content hash of the project's working tree: every file under
  the project root that git does not ignore, tracked or untracked, or
  every file under the project package when it is not in a git
  repository.

A git commit with a dirty flag is not an identity: edit a helper without
committing, deploy, fix it again, deploy — both builds are "abc123,
dirty", and the failed keys never get their retry under the fix. The
commit and the dirty flag are recorded for display only. The engine numbers revisions as it serves them — the
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

- `Each` pages are small writes — `batch_size` keys — so their output
  deltas take the HTTP resolver whenever the engine has the output's index
  admitted to its cache (exact counts, no index reads on the worker), and
  the cold path otherwise. Failure deltas are always resolved by the
  worker itself (§9). The worker uploads both.
- Inlined retries and, later, pattern hints are answered from the one
  warm engine cache, which `resolved-commits` builds anyway; this proposal
  adds readers, not a cache.
- Observable sources are sugar for sensors (`lifecycle.md` §11): no
  launch path, result route or retention class of their own.
- The failure index is an ordinary key index: format, compaction, cache,
  delta naming, garbage collection — none new.
- Indexes count keys, not rows: a file of 40 samples is one entry.
- Per-key progress and key-tagged logs are live events on the HTTP
  channel, not objects.

**Harder.**

- The canonical digest grammar must define the group production, which
  is now every key's version, and be versioned (§6) — done, `row-digest.md`.
- `Rows` must group natively, and patches must move onto `Rows` (§7).
- The watermark gains two positions: the rescope drain (§11) and the retry
  pass (§9).
- Cancel commits a partial page (§5): it needs the lifecycle's two-phase
  cancel, with a drain before any forced abort.

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
- Observations as attempts or runs, `skipped` observation runs and their
  retention class: replaced by sensor ticks (§12).
- `grouped=True` and the duplicate-key error: every key is a group (§6).

## 16. Interactions with work in flight

| Work | What this proposal needs from it |
|---|---|
| Key index (`object-store-state.md` §6) | No format change. A new kind of index (`keys/@{asset}/{scope}/`, the failure index) compacted like the others; `Rows` groups every key (§6), with `Store.key_rows` and the digest grammar — the native thread's current work; patches build `Rows`. |
| Engine cache (`resolved-commits.md`, being rewritten) | New readers: inlined retry keys in v1; pattern counts at commit and failure-summary recomputation later. No new cached content beyond failure indexes. |
| HTTP resolver (`resolved-commits.md`) | Each pages' output deltas are small resolves when the index is admitted; failure deltas are resolved locally, not by the resolver (§9 here is authoritative for the record, transitions, eligibility, pass state and forced-request identity; the resolver's inline reader calls the same SDK functions, and its v1 has no pattern hints or summary recomputation); the worker uploads both. Inlined windows are filtered before the `inline_max` check. A sensor's full key map is resolved in-process (small) or on the host (big), not through an attempt's resolve. The grammar gains the group production. |
| Attempt lifecycle (`lifecycle.md`) | The cancel record (§2.2) and write-completion evidence (§2.3), authoritative there; the two-phase cancel of §7, which §5 follows; live per-key events and key-tagged logs; per-key outcomes in `.result`. Sensors (§11) carry observable sources: `Source.observe` declares one. |

## 17. What changes in the code

- `python/solera/sdk.py`: `Each`; `include`/`exclude` on `Incremental`;
  `Output(meta=…)`; `Rejected`, `Failed`, `Transient(retry_after,
  retry_for)`, `Abort`, `Project(errors=…)`; `Source.observe` as a sensor, `Observed`;
  `ctx.key`, `ctx.revision`, `ctx.keys(output, prefix=)`; the revision from
  a build identity; `code_hash` removed.
- `python/solera/stores.py`: `Patch({key: value})`; the `key_rows` hook;
  no duplicate-key error.
- `python/solera_postgres`: group writes, `key_rows` for by-key patches,
  keyed loads as `dict[str, T]`.
- `python/solera_worker/worker.py`: the per-key loop, classification,
  outcomes, the failure delta, retry and rescope pages, partial commits on
  cancel.
- `python/solera_server/engine.py`: the `Failures` record, the due clock,
  epochs, rescope and retry positions on the
  watermark, alternation of retry and change pages, the cancel drain.
- `python/solera_server/history.py`: `key_outcomes`; per-key counts on
  `attempts`.
- `native/`: the group digest and grouping in `Rows` (in progress), the
  pattern matcher.
- `example/brimstone.py`: the §4 shape; `qaqc_samples` becomes an
  `Each`.
- Docs: architecture §2 (outputs), §4 (writes, store hook), §5 (edges,
  sources), §6 (incrementality), §8 (runs and errors), §9 (observable sources as sensors),
  §11 (revision); `object-store-state.md` §5–7.

## 18. Tests

- `Each` delivers exactly what an equivalent batch asset returning
  `Patch({key: …})` writes, over random pages, deletes and failures.
- Group versions: invariant under row order and key-column presence,
  sensitive to duplicates; flat rows and the by-key form give the same
  versions; Python and Arrow input give the same digests; empty groups are
  live keys, load as `{k: <empty T>}`, and keep the empty-group version
  under `revision=`; both `Sql` paths give the versions a Python or Arrow
  write of the same rows gives, and a group with mixed revisions is a
  write error.
- Each error class in and out of the per-key call; `errors=` mapping;
  `Transient` turning failed after its `retry_for`; failed keys retried
  once per epoch and never more.
- Cancel: the failure delta follows the `reason` of the record the worker
  sealed with; a drain within `cancel_grace` commits finished keys whatever
  their order, the interrupted holes and the watermark as one decision;
  canceled keys never come due by themselves; timed-out keys count a try
  and end `failed` past `retries=`; a forced abort commits nothing, or
  ends with the evidence `lifecycle.md` §2.3 assigns; a late drain result is
  refused.
- Failure index: a 1M-key systemic failure leaves state constant and no
  commit rescans the index; every row of the transition table, with exact
  outcome counts; the engine's and worker's eligibility agree; a deploy or
  a forced retry mid-pass restarts the pass and misses no key; a worker
  clock minutes off neither skips nor repeats a forced retry; a retry
  request for one class leaves a pending one for another intact; pass
  accumulators over several pages give the exact minima at completion,
  with change pages committed in between; retry
  passes inline when small and page when big; retry and change pages
  alternate when both are pending; a due key changed upstream is
  processed once.
- Patterns: a page with no matching key ends `skipped` with its
  watermark advanced; a rescope with
  pending deletions of newly excluded keys removes their rows; the drain
  delivers exactly the symmetric difference at the cutover snapshot,
  whatever is committed meanwhile; a second pattern change waits for the
  first.
- Build identity: an uncommitted edit, deployed twice with different
  content, gives two revisions.
- Observable sources: each `observe()` return shape becomes the right
  `Tick`; unchanged ticks write nothing durable; a cursor-only tick
  records `SensorAdvanced` without a new version or a wake-up; a tick whose
  head identity is no longer current is refused — including an unkeyed
  source whose version an API client changed — whether resolved by the
  engine or on the host; a restart mid-tick drops it harmlessly.

## 19. Open questions

1. When a few keys at a time come due while new changes keep arriving,
   alternation runs small retry attempts between full change pages. Each
   costs a whole attempt; acceptable as is, or should a retry page wait
   until it is full or its oldest key has waited long enough?

## 20. As built

Where the implementation (`solera/errors.py`, `solera/build.py`,
`solera/failures.py`, `solera_worker/each.py`, the engine's `_each_plan` and
`_each_commit`) departs from or adds to the text above:

- **The value of a key** is what the upstream store hands out for it under
  `dict[str, T]`: for a rows upstream, its group — `file: list[dict]`, one
  row for a file inventory; for `keyed=True`, its value.
- **An output a call omits, or returns as `None`,** is not changed for
  that key (decision D7): its previous content stays. Removing the key from
  an output is explicit, `Patch(None, remove=[ctx.key])`; a `Patch` that
  writes rows or removes another key fails the call. `[]` is an empty
  group: a live key.
- **An output with nothing to write is left out of the commit;** an `Each`
  page whose keys all failed makes no head yet, and an `Each` asset skips
  without heads when nothing is pending.
- **Transient errors at the attempt level** count `retry_for` from the
  task's first transient failure (`transient_since` on the task), past
  `retries=`.
- **A change page that leaves keys due at once** continues its run with a
  retry page; a completed retry pass never continues its run by itself, so
  `retry_after=0` costs one retry per run, not a loop.
- **A full delivery** (a reset, or a `full` run) defers retries until it is
  drained: it reprocesses every key anyway.
- **A retry page walks at most 100 × `batch_size` records** before it ends,
  so a long stretch of keys that are not due spans several pages.
- **Retry pages are not inlined**, though change pages are
  (`resolved-commits.md` §7–§8): the worker pages through the failure index. Transitions read priors with an
  exact `get` of the touched keys, and the failure delta is resolved locally
  (`KeyIndex.resolve(exact=True)`).
- **Record times are the worker's clock**; eligibility compares them with
  the engine's `now` in the spec. Skew moves when a key retries, never
  whether (§9).
- **Forced retries**: `solera keys retry ASSET [--failed|--rejected|
  --canceled|--retrying|--timed-out|--all] [--partition P]`, or
  `POST /api/projects/{p}/assets/{name}/keys:retry`, records
  `KeysRetryRequested` and submits a run for the scopes concerned at once;
  only scopes with a failure record take the request.
- **The retry clock** starts a run for a scope with keys due when the asset
  has any enabled automation and the scope is idle.
- **`explain`** is an API, not yet a CLI: `GET /assets/{name}/explain?key=&scope=`
  answers with a `verdict` (`ok`, `failing`, `excluded`, `not_matched`,
  `pending`, `removed`, `absent`) and its evidence — the key's failure
  record, its newest `key_outcomes` rows, the patterns the edge delivers
  under and the rule that excluded it (`Engine.explain`). The Keys view
  reads `GET /assets/{name}/failures` and `GET /assets/{name}/key-outcomes`.
- **The build identity outside git** hashes the project directory's Python
  files only (data written next to a project would otherwise change it).
- **PostgresStore** loads an empty group as an empty DataFrame with the
  table's columns; `can_load(dict[str, T], Keys)` holds when `can_load(T, Keys)` does.
- **Patterns** (`solera/patterns.py`) are evaluated by Python's `re`, by
  the worker only — on every page it reads: windows, inlined pages
  (`resolved-commits.md` §7), full deliveries, `keys=` overrides and
  retry pages (a due key the edge no longer takes is `unmatched`). A page
  they take nothing from does not call the producer and ends `skipped`
  once its window is done.
- **The rescope cutover** lives on the watermark: `patterns` (what it
  delivers under) and, during a transition, `rescope` {`from`, `to`,
  `cutover`, `snapshot` (the upstream index as of the cutover), `pin`,
  `after`}. The diff pages through the whole snapshot (`batch_size` keys
  read per page), not only the key ranges the patterns' prefixes cover.
  A newer pattern change waits for the transition to end, then cuts over
  again. Retries wait for a transition, as for a full delivery. A window
  the log no longer covers falls back to a full delivery under the new
  patterns, which ends the transition.
- **The snapshot pin** joins collection's pins: index-file garbage
  (`Upkeep.collect`) and immutable data discards both wait for the oldest
  rescope pin as for a live claim. A retry pass needs none: each retry page
  reads the failure index and the upstream as they are at its own prepare,
  and the accumulators absorb what changes between pages.
- **After the v1 review** (thr_9ezn6cyar5):
  - Every key of a page gets an outcome before its watermark moves past
    it — a key a cancel reaches while it waits for a concurrency slot is
    interrupted like one in flight. Interrupted keys become canceled or
    timed out by the cancel record the result is sealed with, decided
    after the store writes; the result carries that record.
  - A full delivery of an `Each` edge keeps patch semantics: a key that
    fails keeps its last good output. If the asset held keys when the
    delivery began (or the delta log was lost mid-rescope), the delivery
    ends with a **cleanup** (`reconcile` on the watermark): the outputs'
    and failure index's keys, a page at a time, against the current
    upstream and patterns; those it no longer has are removed. Retries
    wait for it.
  - A reset begins a **pass** (`pass` on the watermark: the run that began
    it); a `full` run's later attempts resume it instead of starting over.
  - A scope's failure record keeps the configuration it last ran under;
    the retry clock, `solera keys retry` and the API submit retries under
    it (`Engine.submit_retries`).
  - A forced request newer than the pass in progress, or than the last
    one done, makes the run that sees it continue with a pass.
  - `keys=` overrides are filtered by the edge's patterns.
  - Renaming an asset (`aliases=`) moves its failure record and its
    `@asset` index.
  - The manifest records the error policy (`errors`: raised, class,
    `retry_for`), so changing it changes the revision; the build identity
    hashes submodules and nested work trees that differ from `HEAD`.
  - Deadlines are computed from the exact time, then rounded up; the
    backoff's exponent saturates.
