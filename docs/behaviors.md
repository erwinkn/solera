# Behaviours: what Solera does, whatever its design

Status: **the contract.** Each entry is something a user, a producer or an
API client can observe. A design must keep every entry; a change to one is
a change to the contract, and says so. Every entry is executable:
`tests/contract/` has its tests, named by its id (`test_inc_4_…` checks
INC-4) and driven only through what a user reaches.

## How to read it

Every entry has an id (area prefix, stable), a rule in its title, and a
concrete example. Entries describe behaviour only, never how it is built.

**The running example:**

```
feed (a keyed source: keys k1, k2, … each at a version)
  └─▶ items   keyed, incremental over feed, key "id"
        ├─▶ tally     plain incremental over items; keeps a count: before + len(added) − len(removed)
        ├─▶ copy      plain incremental over items; a keyed copy of it
        └─▶ checks    per-key (each=True) over items
              └─▶ filtered   per-key over checks, include=["k1"]
factor (an unkeyed source; tally and checks read it whole)
sites (dynamic partitions) ─▶ site_files (keyed, one partition per site)
readings (partitioned by day × site) ─▶ report (by day; fans in over sites)
```

`k1@3` is key `k1` at version 3. A key's version is the generation of the
write that last wrote it, or, for a source, the version its commit gave.

**Words** (D155, D140): a **run** asks for targets; it has one **task**
per asset partition. A task works through what it owes in **batches** of
up to `batch_size` keys, each committed once. An **attempt** is one
execution of a batch; a retry is another attempt of the same batch. Each
key a batch delivers has a **class**: `added`, `updated`, `removed`, or
`unchanged`. What a consumer partition **has processed** is, per key of a
keyed incremental input, the upstream version it last processed (or that
it processed the key's removal); what it **owes** is every key where
that differs from upstream now.

Areas: [INC](#inc--incremental-delivery-and-classification) ·
[RUN](#run--runs-tasks-batches-attempts-retries-cancel) ·
[SEL](#sel--keys-lists-all-full-runs-batchreset) ·
[CHG](#chg--definition-pattern-and-context-changes) ·
[RST](#rst--upstream-resets-removals-and-moves) ·
[SRC](#src--sources-and-served-versions) ·
[KEY](#key--per-key-assets-and-key-outcomes) ·
[STA](#sta--staleness-completeness-and-statuses) ·
[PAR](#par--partitions-fan-in-broadcast-dynamic-partitions) ·
[AUT](#aut--automations) ·
[HIS](#his--history-and-lineage) ·
[CLN](#cln--cleanup-and-retention) ·
[ENG](#eng--engine-crashes-restarts-zombies-and-fencing) ·
[DEP](#dep--deploys-and-reload) ·
[STO](#sto--stores) ·
[Open questions](#contradictions-gaps-and-open-questions) ·
[Coverage](#coverage-summary) ·
[Appendix](#appendix-entries-to-todays-mechanisms)

## INC — incremental delivery and classification

**INC-1. A first run delivers every upstream key as added.**
`tally` has never run; `items` holds k1…k5; `batch_size=2`. The run makes
three batches, `added` = [k1, k2], [k3, k4], [k5]; nothing is updated or
removed; the count ends at 5.

**INC-2. A key's class is the net difference between what the partition processed and upstream now.**
`added`: it did not hold the key, upstream has it. `updated`: both have
it, at different versions. `removed`: it held the key, upstream no longer
has it (or no longer offers it, INC-7, CHG-4). `tally` processed k1@1,
k2@1; then k1 → @2, k3 added, k2 removed: the next batch has
`added=[k3]`, `updated=[k1]`, `removed=[k2]`, and `rows` holds k3 and k1.

**INC-3. Changes that cancel out between two runs deliver nothing.**
k4 is added and removed again before `copy` runs: `copy` gets nothing
about k4 and is not stale. A key removed and added back is `updated`.

**INC-4. An update reverted to the processed version is not delivered.**
`feed`'s k1 goes @1 → @2 → @1 before `items` runs: `items` is stale while
k1 is at @2, fresh again once it is back at @1, and its next run delivers
nothing for k1. What is owed compares versions, so equal ends mean nothing
changed, and no run is spent on it (STA-14).

**INC-5. A total kept from the classes alone stays exact.**
`tally` = what it held + `len(added)` − `len(removed)`. After any history
of commits, `keys=` runs, cancels and failures, once a default run has
nothing left to owe, the count equals the number of keys upstream offers.

**INC-6. Every change is delivered once, even one made during a run.**
`keys=(k1)` processes k1@1; then k1 → @2 (or k3 is added, or k1 removed);
two default runs follow. The change arrives exactly once, in the first.

**INC-7. A delivered removal is final.**
Once `tally` was given k5's removal, k5 coming back, even at the version
it once had, is an `added`. `feed` removes k1; `keys=(k1)` processes the
removal; `feed` restores k1@1: the next run owes k1 as added.

**INC-8. Each batch reads upstream as it is when that batch starts; nothing goes back in time.**
A run over k1, k2 with `batch_size=1`: batch 1 reads k1; k2 changes; batch
2 delivers k2's new version. If `keys=(k1)` delivered k1@2 before the
run's batch reached k1, that batch delivers nothing for k1, never k1@1. On
a store that writes in place, this rests on the version check per
partition in every write transaction (STO-4), not on a fence taken before
reads.

**INC-9. A change made behind a run's batches is owed by the next run.**
Batch 1 commits k1; k1 changes while batch 2 runs: the run ends, the
partition is stale, the next run delivers k1 as updated. A key removed
during a run, before its batch, is delivered once, as whatever it is when
its batch reads.

**INC-10. What a producer receives for an incremental input.**
`ctx.batch[input]`: `added`, `updated`, `removed`, `unchanged` (keys
loaded only because the run named them, SEL-1), `rows` (of added, updated
and unchanged; removed keys have none, and `rows` is the parameter),
`index` (0-based in the run), `count` (batches planned, an estimate),
`first`, `final` (no batch of this run follows; never inferred from
`count`), `reset` (the first batch of a full run, SEL-9), `upstream` (its
output). A run of 3 batches
gives `index` 0, 1, 2 and `final` only on the last.

**INC-11. `ctx.load()` returns the asset's own output as materialized; `None` before its first commit.**
`tally`'s second batch reads `{"rows": 2}` committed by its first, and adds
to it.
In a full run too: batch 0 says `reset`, and `ctx.load()` still returns
what was materialized before it (SEL-9).

**INC-12. A task walks what it owes in key order, `batch_size` keys a commit.**
Seven owed keys, `batch_size=3`: three batches, three commits. Default
`batch_size` is 10,000 for every incremental input.

**INC-14. Writing is changing; writing nothing wakes nothing.**
`items` returns an empty `Patch`: no commit, no `OnChange`. It writes the
same rows again: a new version, and consumers process them again
(accepted: over-eager, never wrong).

**INC-15. An upstream's full run never makes a keyed downstream remove a key and add it back; downstream keeps its progress.**
`copy` holds k1–k4 and runs full while `items` holds k1, k2, k4, k5, with
`batch_size=2`; `archive` reads `copy`. The full run's first commit is a
clear, and `copy`'s commits keep their numbering, so `archive` is not
reset. While `copy` is incomplete `archive` runs no batch over it
(STA-13). Once `copy` completes, `archive` gets one difference: k1, k2, k4
updated, k3 removed, k5 added; never k4 removed and then added back. A
difference whose window holds a clear is two scans, `copy` as `archive`
last read it and `copy` now, never a replay of each commit.

## RUN — runs, tasks, batches, attempts, retries, cancel

**RUN-1. A run has a task per asset partition; a task commits batch by batch; an attempt runs one batch.**
`tally` owes 25,000 keys, `batch_size=10_000`: one task, three batches.
Batch 2's first attempt dies; attempt 2 of batch 2 runs it again.

**RUN-2. One attempt at a time per asset partition, across runs.**
The hourly run and a manual run both target `copy`: the second task waits
("held: claim (copy)") and runs after the first settles.

**RUN-3. `@asset(concurrency=N)` caps partitions running at once.**
`report` has `concurrency=2` and three partitions due: `a` and `b` run,
`c` waits ("held: concurrency (report)") and runs when one ends.

**RUN-4. A waiting task says why.**
One of: `claim`, `concurrency`, `merges` (an output it writes is far
behind on upkeep), `engine` (the engine's slots), `executor` (its limit),
`invalid` (its placement cannot be built), `upstream incomplete` (an
upstream it reads is mid full run, STA-13).

**RUN-5. A task resumes from its last committed batch, across attempts and engine restarts; a new run starts from the beginning.**
Batches 1 and 2 commit; batch 3's attempt dies, or the engine restarts:
the task continues after batch 2's last key. When the run ends, its
progress goes; the next run compares again and walks from the first key,
cheaply, since only changes are owed.

**RUN-6. A task never relaunches forever.**
Two full runs of `copy` with a `feed` commit between: the second finishes
in a bounded number of attempts.

**RUN-7. A failed attempt is retried by its error's class.**
`retries=Retry(3)` by default, with backoff. `Rejected` fails the task at
once; `Failed` and `Abort` use `retries=`; `Transient` retries after
`retry_after` (else 1 min doubling to 6 h), past `retries=`, until
`retry_for` (24 h) since its first failure. A worker that dies without a
result is a retryable failure.

**RUN-8. A failed or abandoned attempt changes nothing a consumer processed.**
Batch 2 of `tally` fails: its keys stay owed and `tally`'s count is what
batch 1 left.

**RUN-9. A cancel is asked first, then forced.**
The worker hears the cancel within a beat (10 s), stops starting work and
has `cancel_grace` (60 s, one engine-wide setting) to publish what
finished. A plain asset that had not begun writing ends `canceled` with
nothing; one already writing finishes and its commit stands. Past the
grace the attempt is forced and nothing more of it is accepted.

**RUN-10. A cancelled run keeps what it committed and does not resume itself.**
`tally`'s run commits batches 1 and 2 and is cancelled: their keys stay
processed; the partition is stale; the next run owes only the rest plus
what changed since.

**RUN-11. An attempt's timeout counts from its worker's first report.**
A 20-minute image pull does not eat a 30-second timeout. Past the
timeout the attempt is cancelled (reason `timeout`) and retried within
`retries=`. Before any report, the executor's provisioning deadline
applies (`provision=`: 10 min for a container executor, none for a
`Pool` unless set) and the attempt fails retryably.

**RUN-12. Retrying a finished run is a new run; the old one stays as it ended.**
Run R had a failed and a canceled task; "retry" submits a new run with
`retry_of: R` for exactly that work; R still reads as it ended.

**RUN-13. A task's outcome is `succeeded`, `skipped`, `failed` or `canceled`.**
`skipped`: nothing owed and the partition complete, so no worker
launches. A per-key run whose keys partly failed still `succeeded`, with
per-key counts (`{ok: 3, rejected: 1, failed: 1, removed: 1}`).

**RUN-14. The console shows the run first, and batches and attempts only when they say something.**
A task with one batch and one attempt shows just the run. A retry adds
"attempt 2". Several batches add "batch 2 of 3" and a progress bar. No
user-facing text says "attempt spec" or "attempt handle".

**RUN-15. A run targets its assets only unless asked for upstream.**
`upstream=False` (default) pins current heads and never re-runs an
upstream; `upstream=True` builds the closure first. A selection past
100,000 partitions or tasks is refused, never truncated.

## SEL — `keys=` lists, `"all"`, full runs, `batch.reset`

**SEL-1. `keys=[…]` loads exactly the named keys and never touches another.**
`copy` holds k1, k2, k3; k1 and k2 changed upstream. `keys=(k1)` delivers
k1 as updated; k2 and k3 are untouched, and k2 is still owed. A named key
already processed as it is comes as `unchanged`.

**SEL-2. A named key upstream lacks is a removal if held, else nothing.**
`keys=(k9)` where k9 never existed: nothing delivered, nothing recorded.
`keys=(k1)` after `feed` removed k1: k1 delivered as removed.

**SEL-3. What a `keys=` run delivered is not delivered again.**
`keys=(k1)` delivers k1's update; the next default run delivers k2's and
not k1's.

**SEL-4. A `keys=` run never clears what it did not process.**
After `keys=(k1)`, `copy` stays stale for k2 until a run processes k2.

**SEL-5. Named keys go through the input's patterns.**
`checks` takes `include=["k1"]`; `keys=(k1, k2)` loads k1 only; a held k2
is delivered as removed.

**SEL-6. A long `keys=` list goes `batch_size` keys a commit, `concurrency` at once.**
Ten named keys, `batch_size=4`: three batches, three commits; the run
ends with the ten processed.

**SEL-7. `keys={input: "all"}` loads every key under the patterns without starting over.**
Owed keys in their class, the rest `unchanged`; nothing is reset.
`keys={"rates": "all"}`.

**SEL-8. A full run owes every upstream key under the patterns; a plain consumer starts over.**
`mode="full"`, or an automatic full run (CHG-1, RST-1). A full run is the
run's mode, never a `keys=` value (Q3: `keys={input: "full"}` is refused).
`tally`'s first batch starts its count from zero; its cursor is `None`;
the run continues batch by batch across attempts.

**SEL-9. A full run's first batch says `batch.reset`; `ctx.load()` still returns what is materialized.**
`tally` sees `batch.reset == True` on batch 0 and ignores `{"rows": 7}`
from `ctx.load()`; later batches build on what batch 0 committed.
Per-key producers never see `reset`.

**SEL-10. A per-key consumer's full run keeps its outputs readable and removes what upstream lacks.**
`checks` holds k1, k2, x9; upstream has k1, k2, k3. The full run calls k1
and k2 (updated), k3 (added) and drops x9 (removed). A key that fails
keeps its last good output. Cancelled midway, the rest stays owed and
readable.

**SEL-11. A full run reaches its producer even when it owes no key.**
`copy` (plain) over an upstream now empty, or with patterns taking
nothing: called once with an empty batch, first and final, and its
output becomes empty; its first commit is a clear even though it writes
no key, and the run is complete at once. A per-key consumer is not
called; its held keys are removed.

**SEL-12. A due full run may be carried out by several runs, `keys=` ones included.**
`copy`'s version is bumped; `keys=(k1)` is the full run's first batch:
its commit clears `copy`, which now holds k1 alone and owes k2, k3; its
readers are held (STA-13) until a default run finishes it.

## CHG — definition, pattern and context changes

**CHG-1. A definition change makes the partition stale, and its next run a full run.**
The definition covers the asset's `version`, its input bindings, its
outputs' migrations, its stores' versions, and the run's config; not its
stores' config (D202): rotating a password or moving a DSN changes
nothing, and a store whose data really changed bumps its version. `tally`
`version="1"` → `"2"`: stale ("definition changed"); the next default run
starts over (SEL-8).

**CHG-2. A code change alone is a new deploy, not a definition change.**
Editing a helper module serves a new deploy: nothing goes stale; failed
per-key keys get one more try (KEY-3).

**CHG-3. A batch planned before a definition change or an upstream reset does not commit.**
`copy`'s attempt is launched as version 1; version 2 is deployed before
it settles: it commits nothing, and its keys stay owed under version 2.

**CHG-4. A pattern change is an input change: the next run owes the difference in membership.**
`tally` has `include=["k1"]`; a deploy widens it to `"k*"`: stale ("input
changed"); the next run adds k2, k3 and does not process k1 again.
Narrowing to `"z*"` owes the removal of every held key. If `tally` has
an `OnChange` automation, either deploy fires it, because it makes
something owed (AUT-4).

**CHG-5. A first `include` removes what it leaves out.**
No include over `keep/a`, `drop/b`; a deploy adds `include="keep/*"`:
`drop/b` is owed a removal.

**CHG-6. A key added upstream while the patterns exclude it is owed nothing.**
Patterns narrow from `keep/*, drop/*` to `keep/*`; `drop/b` is then added
upstream: nothing.

**CHG-7. Widening to a key that never existed owes nothing.**
`include=["k1"]` → `["k1", "k9"]`, no k9 upstream: not stale.

**CHG-8. A `keys=` run around a pattern change is counted once.**
Widen `k1` → `k*`, then `keys=(k2)`, then a default run: k2 is added once;
the count ends at 3 for k1, k2, k3.

**CHG-9. A change to a key a consumer excludes makes nothing stale there.**
`items` commits only `x1`, which `copy` excludes: `copy` stays fresh.
`filtered` takes k1 of `checks`; k2 changes: `filtered` stays fresh, and
k1 changing makes it stale.

**CHG-10. A moved whole or dep input owes every key processed under its old version.**
`checks` processed k1, k2 under `factor=w1`; `factor` → `w2`: both owed
updates, reason "input changed" (not "definition changed").
`keys=(k1)` processes k1 under `w2`: k2 still owed. `factor` back to `w1`:
now k1 is owed and k2 is not.

**CHG-11. A whole input written again with the same content is a change.**
`settings` is rewritten identically: a new version; its readers owe a
rerun.

**CHG-12. Patterns match keys by path segments.**
`**` crosses `/`, `*` does not; `Regex(…)` too, in Rust's `regex` dialect
against the whole key (no lookaround or backreferences: refused when
declared). `exclude="k1*"` is one
pattern, not its characters; `**/*old*` matches `Gold_ore.csv`, so word
rules want segment-anchored patterns.

**CHG-13. A whole input that moves mid-run makes the keys processed before it moved owed again.**
`factor` moves from w1 to w2 while `checks`' run is between its first and
second batch: later batches read w2, and after the run the keys of the
first batch are owed again, reason "input changed".

## RST — upstream resets, removals and moves

**RST-1. Moving an output to another store makes it a new output.**
`items` moves from FileStore to Postgres: its head and keys start empty;
its first write is whole; every consumer owes a full run (stale "input
changed"); its own inputs are read in full. Nothing is copied; the old
store's data is cleaned up (CLN-6).

**RST-2. Moving away and back is two resets.**
`items` moves to Postgres and back with nothing written between: it is
reset all the same, and keeps no key from before.

**RST-3. An asset removed and declared again starts a new life.**
`copy` removed, then declared again: no cursor, no processed keys, no
failing keys from before. An attempt of the first life, still running,
commits nothing into the second. Same for a job.

**RST-6. A removed asset's queued work is cancelled, saying why.**
A deploy drops `doomed`: its queued tasks are cancelled with the reason,
its runs end, and a launched attempt is not retried.

**RST-7. An upstream reset drops a per-key consumer's failing keys for that input.**
`items` moves; `checks` had k1 failing: k1's failure goes at the deploy
(it failed against keys that are no longer the input's).

**RST-8. A removed consumer stops holding back its upstream's history, except through its retained runs.**
`gone` and `keep` read `feed`; `gone` is removed: its progress holds
nothing from then on, and its runs still in history keep the commits
their batches name until those runs expire.

## SRC — sources and served versions

**SRC-1. A source commit changes only keys whose version moved.**
`feed` holds a.csv@c7, b.csv@c3; `commit(keys={a.csv: c7, b.csv: c4,
d.csv: c1})`: b.csv updated, d.csv added, a.csv unchanged. A full map
removes keys it omits; `upsert`/`remove` patch. A key given no version is
always a change. An unkeyed source given its current version again makes
no commit.

**SRC-2. Each source commit that changes something is a run with no tasks.**
It says who committed and what changed, and expires with the project's
default retention.

**SRC-3. Sources are read live, and every load says which version it served.**
A keyed loader returns `{key: Loaded(row, version=…)}` (or one table,
SRC-13); a key it leaves out was served absent. `ctx.batch[input].served` shows each key's served
version. A keyed loader that omits versions fails the load.

**SRC-4. A batch's classes follow what the source served, before the producer runs.**
The commit says k2@2; the source serves k2@3: classed from @3. Restored
to @2 by a commit: owed as updated. Served absent: removed if held, else
nothing. Served at the version already processed: nothing.

**SRC-5. A key the source lost without a commit is processed as absent; its return is delivered.**
`feed` commits k1@3; before `items` reads it the outside loses k1:
`items` processes k1 as absent. `feed` restores k1@3: `items` is owed k1
(added). A commit that removes k1 owes nothing more.

**SRC-6. A keyed source loaded as data must be able to say what it served.**
No loader, and a store that cannot serve it (no `version_column`, no
`path`): refused at registration. Read only as a `Ref`, it needs neither.

**SRC-7. Built-in stores serve sources at their own versions.**
FileStore/S3Store: each object under `path` at its etag (local: inode,
mtime, size; `hash=True` hashes content). Postgres: each row at its
`version_column`. An unkeyed function may return a bare value (caveat: a
revert between commit and read can leave a consumer one revision ahead).

**SRC-8. Bumping a loader's `version=` owes every key as updated.**
`@source(version="1")` → `"2"`: every consumer owes every key.

**SRC-9. `Source(copy=True)` copies each changed key into a Solera store at commit.**

**SRC-10. A sensor tick is all or nothing.**
A tick's source commits, requested runs and cursor land together. No
change and no new cursor: nothing recorded. A new cursor alone advances
without waking anyone. A tick is refused whole only if a key it reports
changed since it observed it: a commit by another client, or a
consumer's load (SRC-12), to other keys does not refuse it. A raising tick
keeps its cursor. A retried post applies once; a tick's runs are
submitted once.

**SRC-11. An observable source commits on its schedule.**
`Source("landing", observe=Every(300))`: `observe()` returning a version,
a full map, or `Observed(upsert, remove, cursor)` becomes that source's
commit.

**SRC-12. A load that finds a source at another version than its log records it in the log.**
`feed`'s k1 was committed @2; the outside moves it to @3 before any commit
says so; `items` loads k1@3: `feed`'s log now records k1@3, unless the log
changed k1 since the batch's head (it then knows better). Two consumers of
a source that changes on every read may trigger each other, bounded by
the engine's pace (ENG-10).

**SRC-13. A keyed loader may serve one Arrow table.**
`Loaded(table, version="etag")`: the key column is the one `key=` names,
each row's version its `etag`. Before, `{k: Loaded(row, version=etag)}`
per key; after, one table, of which only the two columns are read. The
consumer gets the same batch as from the dict form: the rows of the keys
asked, in key order, without `etag`, as its parameter asks (a list, a
DataFrame, a `pa.Table`). A key with no row was served absent. A table
that cannot say what it served fails the load: a key twice, a null key or
version, no key or version column, a version not a string.

## KEY — per-key assets and key outcomes

**KEY-1. A per-key asset is called once per owed key; removed keys lose their rows without a call.**
`checks` over k1, k2 (changed) and k3 (removed): two calls; one store
write per output holds k1's and k2's rows and removes k3's.

**KEY-2. `batch_size` is keys per commit; `concurrency` is keys at once.**
Defaults 10,000 and 64. `@asset(concurrency=4)` with per-key
`concurrency=64` is at most 4 × 64 calls. A cancel interrupts keys still
waiting for a slot like keys in flight.

**KEY-3. A key's error class decides its fate; one bad key never blocks the others.**
`Rejected` (an empty file): keeps its previous output, retried when its
input changes. `Failed` (any unclassified error): the same, plus one try
per new deploy. `Transient`: retried on backoff (1 min doubling to 6 h,
`retry_after` honoured) until `retry_for` (24 h), then failed. `Abort`:
the whole attempt fails, nothing commits. If a failing key's input goes
back to the version the partition held before the failure, the failure is
moot: its outcome goes, and it is not retried.

**KEY-4. Returning `None` changes nothing for that key; removing is explicit.**
A call returns `None` for `icp_raw_data`: its previous rows stay.
`Patch(None, remove=[ctx.key])` removes it. A key given no rows does not
exist.

**KEY-5. Every key of a per-key partition has a latest outcome; a failing key stays owed.**
`rejected`, `failed`, `retrying`, `canceled`, `timed_out` (stored, with
tries, since, message); `ok` (processed, with the version it processed,
or null once upstream replaced it); `unmatched` (upstream has it, the
patterns leave it out); `removed` (upstream had it and lacks it now); or
none. Each row also says whether the key is owed: a.csv ok at v5 with
upstream at v6 is `ok`, owed. A failing key is owed: the partition still
holds what it held before the failure, so its row says `owed: true` until
it succeeds. A key that never succeeded and is removed upstream owes
nothing, and its outcome goes (KEY-12). `GET /assets/{name}/outcomes`
pages them, filterable by outcome.

**KEY-6. Failed keys come back on their own, bounded.**
An automated per-key asset with keys due starts a run on its own when
the partition is idle; one run by hand picks them up on its next run.
No input head (a move reset it): the clock waits rather than looping.

**KEY-7. A forced retry calls each matching key once.**
`solera keys retry checks --rejected` calls every rejected key once, even
if its source went 1 → 2 → 1 in between.

**KEY-8. Retries and new changes alternate; a key that changed upstream comes with its change.**
1M keys failing after a deploy halve the pace of new files instead of
stopping them. A due key whose upstream changed is processed once, at
its new version.

**KEY-9. A cancelled per-key batch commits its finished keys.**
Cancel while b and d run: a, c, e(removed) commit; b, d stay owed, so the
partition is stale and the next run of it, whatever starts it, processes
them. Reason `user`: they show `canceled`, and nothing starts a run for
them alone. Reason `timeout`: `timed_out`, a try counted, due after
backoff, `failed` past `retries=`. A user cancel during a timeout's drain
makes them `canceled`.

**KEY-10. A full run reprocesses the failing keys without a reset.**
`checks` v1 leaves k1 failing; v2 makes every key due, k1 included: k1 is
called again, and its failure goes when it succeeds. Nothing is cleared at
the start, so a key that fails again keeps its failure and its last good
output. A full run asked for by hand does the same (KEY-13).

**KEY-11. "Why is this key not there?" has an answer.**
`GET /assets/checks/explain?key=k2`: `ok`, `failing`, `excluded` (naming
the rule), `not_matched`, `pending`, `removed` or `absent`, with
evidence.

**KEY-12. A key that never succeeded and is removed upstream owes nothing.**
`items` adds k6; `checks` fails on it at its first try; `items` removes
k6: `checks` is not stale for k6, no run delivers its removal, and k6's
outcome goes.

**KEY-13. A per-key asset's full run resets nothing and never empties its output.**
`checks` holds k1, k2, x9, and `items` holds k1, k2, k3. A full run calls
k1, k2 (updated) and k3 (added) and removes x9; every held key stays
readable throughout, and the partition stays complete. A full run asked
for by hand, with no definition change, makes every key due again (through
a rebuild counter on each input); canceled midway, the next run resumes
where it stopped instead of starting over.

## STA — staleness, completeness and statuses

**STA-1. A partition's status is one of six.**
In order of precedence: `removed` (no longer in the partition set),
`running`, `failed` (its last outcome), `stale`, `materialized`
(complete), `missing`. A built partition that is stale shows `stale` even
if incomplete.

**STA-2. A stale partition says every reason that holds.**
`input changed` (an input owes something), `upstream stale` (a partition
it reads is stale), `definition changed`. `copy`'s version bumped while
`items` is stale: both, in a stable order.

**STA-3. Stale means exactly: a default run would load something.**
Derived from progress and upstream; one comparison answers both. Owed
failing keys whose retry is not due yet do not make a partition stale
(STA-15).

**STA-4. Staleness runs down the lineage.**
`feed` commits; `items` has not rerun: `items` is "input changed";
`copy`, `tally`, `checks` are "upstream stale". Once `items` reruns they
become "input changed"; once they rerun, all are fresh.

**STA-5. Roll-ups use "any".**
An asset is stale if any partition is; a per-key partition if any key is.

**STA-6. Stale keys are exact for per-key assets; other keyed outputs' keys go stale together.**
`checks`: only the keys owed, each with its own reason naming its input.
`copy` (keyed, not per-key): a change to k1 makes k1, k2, k3 stale. An
unkeyed output has no keys.

**STA-7. A key neither side holds is never stale.**
`fchecks` processed k1 only; `feed` removes k2, which `fchecks` never
held: fresh.

**STA-8. An unkeyed partition stays stale until it reruns.**
`count` is built; `items` changes: `count` is stale through any number of
ticks until a run of it.

**STA-9. Never built is `missing`, not stale.**

**STA-10. Complete: no key upstream that the partition has not processed since it last started over.**
| History | Complete? |
|---|---|
| never run | no |
| a first run finished | yes |
| an incremental run cancelled midway | yes (and stale) |
| a full run cancelled midway | no |
| a per-key full run cancelled midway | yes |
| `keys=` on a never-run partition | no, unless it named every key upstream |
| a full run whose unprocessed rest was deleted upstream | yes |
| empty upstream, or patterns taking nothing | yes, at once |
| two inputs, one walked to the end | no |

Complete partitions are what `partitions="missing"` skips and fan-ins read.

**STA-11. Completeness is live; history logs only whether a commit was its run's last batch.**
A three-batch run's commit rows say `final` false, false, true; a
cancelled run logs no `true`.

**STA-12. Freshness is exact whenever asked; there is no `pending` status.**
Whether a partition is stale is kept exact at every upstream commit and
every batch commit (one witness due key per input, §6 of the design); the number
of owed keys is counted when asked and cached until the upstream or the
progress moves. A status asked right after a prefixless pattern change on
100M keys is exact: never guessed, never `pending`. Where a gate holds an
input (STA-13), its reason is `upstream incomplete`, with no number.

**STA-13. A reader of an incomplete upstream runs no batch until the upstream's full run completes.**
`copy` runs full in two batches; between them, `archive` (keyed),
`summary` (reading `copy` whole) and every other reader are held: a run
of theirs waits, saying `upstream incomplete` (RUN-4), and starts once
`copy` completes. On a table, a held reader's batch that was already in
flight when the full run began holds back every key it loaded at another
version than it planned; those keys stay owed. A full run canceled or
failed midway raises an alarm, and its readers wait until a run of the
producer completes it.

**STA-14. No false alarms: a partition is stale only if something is due; changes that cancel out start nothing.**
`feed`'s k1 goes v1 → v2 → v1: `items` is stale while k1 is at v2 and
fresh again at v1; a run asked for after the revert delivers nothing. A
key added and removed again between two runs is never owed.

**STA-15. Failing keys not yet due are shown, not stale.**
`checks` has k2 failing, its retry not due yet, and owes k3 and k5: it is
stale for k3 and k5 only, and shows one failing key. Once k3 and k5 are
processed, `checks` is fresh with k2 still failing, until k2's retry
comes due or its input changes.

## PAR — partitions, fan-in, broadcast, dynamic partitions

**PAR-1. Partition keys are canonical strings; an unpartitioned asset has one, never shown.**
`"Richmond"`, or `"day=2024-01-01,site=Richmond"` sorted by dimension.
Time windows are half-open and aligned in their timezone.

**PAR-2. Dimensions project: shared → same key, consumer-only → broadcast, upstream-only → fan-in.**
`report` (day) over `readings` (day × site) gets `dict[site, T]`. A
consumer-only dimension reads the same upstream partition for every
key. An incremental input may not fan in.

**PAR-3. A fan-in reads the complete heads that exist; it never waits.**
`report` for 2026-10-01 runs over Richmond and Oslo while Paris has no
head. To wait for all, `upstream=True`. With `skip_missing_inputs`, a
fan-in with no head at all is skipped.

**PAR-4. `all_partitions=True` reads every upstream partition, shared dimensions too.**
`site_report` for alpha compares alpha with every other site.

**PAR-5. Dynamic partitions: re-listing changes nothing; new elements surface through `"missing"`.**
`sites` re-lists every site each cron run: no consumer wakes. A new site
is picked up by `Every(60, partitions="missing")`.

**PAR-6. A removed partition leaves fan-out and fan-in; its head stays readable for `cleanup_after`.**
`sites` drops `west`: no run targets it, no fan-in reads it, its status is
`removed`, and its last head can still be read for `cleanup_after` (a
week by default); then its data is cleaned up (CLN-6). Back within the
window, it picks up where it left off.

**PAR-7. Partition selections: `"latest"`, `"missing"`, `"all"`, a list.**
Counted before listed; past 100,000 refused. An explicit list never
enumerates the domain.

**PAR-8. A source change reaches every partition of its consumer.**
`rates`, an unkeyed source (no dimensions), changes: every partition of
`site_report`, which reads it, owes a rerun.

**PAR-9. A sensor's requested runs see its own commits.**
A tick replaces `sites` `[old]` with `[new]` and asks for all partitions:
the run targets `new`.

## AUT — automations

**AUT-1. `OnChange` fires when an input's head changes, as one ordered run.**
An automation targets `copy` and `summary` (which reads `copy`); `items`
commits: one run, `summary` after `copy`, partitions projected from the
change.

**AUT-2. A change waits while the work it is owed is already queued or running.**
`copy` is running when `items` commits again: the firing waits and runs
after, so no change is consumed by work that could not see it.

**AUT-3. A schedule waits for its next time, even the first.**
`Cron("0 6 * * *")` declared at 14:00 first fires tomorrow at 06:00. A
tick is skipped for a partition running or queued.

**AUT-4. An asset change leaves its automations to decide; with none, it shows stale.**
Adding, re-adding, or changing an asset's definition: its
`OnChange` owes one firing per partition whose inputs have heads; a
schedule waits for its next time; with no automation, the asset shows
stale for a run by hand. A deploy that changes an input's patterns owes
the firing when the change makes something owed (CHG-4), and none when it
owes nothing (CHG-7: widening to a key that never existed).

**AUT-5. `OnDeploy` fires once per new deploy.**
Restarting on the same deploy is silent; two deploys before a tick fire
once, for the latest.

**AUT-6. A failing partition's changes back off.**
`site_status` fails the same way on every `site_events` commit: its
firings wait 60 s doubling to 1 h, reset by a success, so it stays
visibly failed instead of failing 7,500 times.

**AUT-7. With nothing manual, automated outputs converge on their sources.**
After quiet, every automated output equals what its sources imply; no
change stuck, none silently consumed.

## HIS — history and lineage

**HIS-1. Everything that finishes is in history, and reads the same once archived.**
Runs, tasks, attempts, output versions with metadata, and the input
versions each was built from; filters, facets, a timeline per run, where
each attempt ran.

**HIS-2. A task's wait counts only the time it could have run.**
A paused run or an engine outage is not wait.

**HIS-3. Lineage records the versions actually read: the planned ones, plus the deviations.**
FileStore: the planned version. Postgres: the planned version, or the one
loaded where it differed (HIS-8). A source read live: the version it
served. `ctx.load` is not lineage.

**HIS-4. A commit row names the batch that made it.**
`batch: {index, count}` and `final`; null for an unbatched task.

**HIS-5. While a run is in history, everything about it is answerable exactly, per key.**
Run R's batch 2 processed k1@7 and k2@3; a month later, R still retained:
"which version of k1 did batch 2 read, and what was its class?" →
k1@7, updated. Upstream key history is kept back to the oldest commit a
retained run or live reader refers to. A key outcome's `removed`, and its
processed version, stay known as long as their run.

**HIS-6. Per-key history keeps failures, a duration histogram per batch, and each batch's 10 slowest keys.**
Failures are logged per call, newest first. Ok calls are not logged one
by one: a batch's ok keys come from lineage (HIS-5). All of it expires
with its run.

**HIS-7. A batch's input is recomputed from its record and checked against its digest.**
Asked what batch 2 of run R received, the engine recomputes the key list
from the batch's record. The digest covers the planned keys, each with
its class and version, so a mismatch is an error naming the batch, never
a wrong list.

**HIS-8. Lineage lists the keys loaded at another version than planned, with the version loaded.**
`tally` planned k2@2 and loaded k2@5 (STO-3): its batch's lineage says k2
was planned at 2 and loaded at 5.

## CLN — cleanup and retention

**CLN-1. Runs expire by policy; current state never does.**
`Retention(days=7)`, `Retention(runs=90)`, `forever=True`, a project
default. A run goes once it is past the horizon of every asset it ran;
active runs never. Heads, keys, cursors and what was processed stand on
their own; data never expires.

**CLN-2. Runs can be deleted and pruned by hand; active ones cannot.**

**CLN-3. A deleted run is gone for good, and its late worker writes nothing.**
On a store that writes in place this rests on the version check (STO-4).

**CLN-4. Superseded versions go once no attempt in flight may read them.**
`items` rewrites k1 (FileStore, k1@g5 → k1@g9): k1@g5's object is deleted
once no live attempt planned at a head before g9. A lagging consumer holds
nothing back, since it plans at the head when it runs. A burst of commits
makes few cleanup tasks.

**CLN-5. What an abandoned attempt wrote is cleaned up: late on a store that names objects by version, by the next attempt on a table.**
An attempt dies after writing an object to FileStore: the object is
deleted after the asset's timeout plus the cancel grace. On a table, the
next attempt's repair deletes the rows whose key the log lacks, and the
producer rewrites the others (ENG-8).

**CLN-6. A removed or moved output's old data goes after `cleanup_after`.**
A week by default on every store (an undo window); an output may
override it. Added back within the week, its new data stays. An attempt
in flight from before the removal holds the cleanup back. A removed
dynamic partition's data follows the same window (PAR-6). A custom store
the project no longer declares leaves the cleanup stuck, saying so.

**CLN-7. Cleanup runs in the background; a run is settled before its cleanup is.**

**CLN-8. A cleanup that keeps failing is stuck and shown, not retried forever.**

**CLN-9. A store root belongs to one namespace.**
Staging's engine writing `s3://acme-solera/data`, owned by prod, is
refused naming both. `solera adopt-store` moves it, refused while prod is
live.

**CLN-10. Sensor tick rows last a day; a tick that committed lasts as long as its run.**
A row lasts at least a day: rows written together go together, once the
newest of them is a day old.

**CLN-11. A paused consumer never loses its place to retention.**
`tally` paused 40 days: when it runs, it owes k2 (updated), k3 (removed),
k4 (added), not a full run, because the cut never passes a consumer's
progress. Its upstream's history is kept for it meanwhile, watched by an
alarm (CLN-13).

**CLN-12. A replaced data file goes once no live attempt planned before its replacement.**
`items` replaces k1@1 with k1@4 at c4 while an attempt of `checks`
planned at c3 is live: k1@1's file stays, and the attempt can load it.
When that attempt settles, the file goes.

**CLN-13. An output whose history outgrows its live keys raises an alarm naming what holds it; an operator can release a consumer's place.**
`copy`, run by hand once a quarter, holds `items`' history while `items`
is rewritten daily: past a set multiple of `items`' live keys, an alarm
names `copy` and its place. Releasing that place lets the history go, and
`copy` then owes a full run.

## ENG — engine crashes, restarts, zombies and fencing

**ENG-1. A launched attempt survives an engine restart, and commits once.**
The engine restarts while `copy`'s attempt computes: the new engine
adopts it (no relaunch) and commits its result.

**ENG-2. An attempt not yet durably launched leaves no trace.**
The engine is replaced before the launch is durable: no worker was told,
nothing was written, the next engine dispatches the task again.

**ENG-3. Workers keep working through an engine outage.**
A pool worker finishes while the engine is down; the restarted engine
settles its result.

**ENG-4. A new engine fences the old one; nothing acknowledged is lost.**
A rolling deploy: B takes the namespace while A still runs. A stops acting
at once; every event A acknowledged survives; A never deletes B's files.

**ENG-5. State is rebuilt from what was recorded alone.**
A replay of the recorded events equals the live engine.

**ENG-6. A worker started twice runs once.**
Kubernetes starts two pods for one attempt: the first to own it runs; the
other writes nothing, waits for the end, and its exit does not end the
attempt.

**ENG-7. A worker the engine gave up on cannot change what a newer one committed.**
A worker paused for a minute wakes after its attempt was ended and retried:
on Postgres its write transaction is refused by the version check
(STO-4); on FileStore its writes land under names nothing reads.
Heartbeats are evidence, never permission.

**ENG-8. A dead writer's rows on a table are repaired by the next attempt.**
`orders`' attempt (version 6) dies mid-write on Postgres. The next attempt
opens a write transaction, even an empty one, lists the rows at version 6,
deletes those whose key the log lacks, and rewrites the others. No repair
clock, and readers never wait: a consumer that loads a dead row meanwhile
raises an anomaly (ENG-17).

**ENG-9. Bad input fails one attempt or request, never the engine.**
A malformed control file or result fails its attempt, retryably. A run
config holding an integer past 64 bits is a 400. A malformed body is a
422.

**ENG-10. No hot loops.**
Between external inputs the engine's ticks, events and requests stay
within `burst + rate × elapsed`. No wake floor: each loop is fixed where
it is.

**ENG-11. A restart may stretch an attempt's timeout by one, never cut it short.**

**ENG-12. A sensor tick in flight at a restart is forgotten; the sensor is due again from its cursor.**

**ENG-13. Writes wait while an output's upkeep is far behind.**
Past the bound, attempts writing that output are held ("held: merges"),
and a source commit to it is refused, retryable.

**ENG-14. Upkeep that keeps failing backs off and alarms; it never stops for good.**
An output's merges fail three times: it waits 10 minutes, doubling,
alarmed, and tries again; an operator can clear the wait; merges cut short
by the engine's own stop count for nothing.

**ENG-15. `SIGTERM` stops `solera serve` cleanly, exit 0.**

**ENG-16. A namespace from an older state format is refused, naming the fix.**

**ENG-17. A loaded version that neither its output's log nor a live attempt explains is an anomaly, never a change.**
`tally` loads k2@6 from `items` on Postgres, written by attempt A, which
died before committing: the engine raises an alarm naming `items`, k2 and
A; `tally`'s k2 is not due until `items` has redone it; `items`' log
never records version 6. A version of an attempt still live is not an
anomaly: the key settles when that attempt commits.

**ENG-18. An asset never builds on a dead write to its own output.**
`tally` on Postgres holds 4; attempt A reads 4, writes 5 and dies before
its commit. The retry's `ctx.load()` would return 5, which is not
`tally`'s last commit: `tally` owes a full run instead of adding to 5.

## DEP — deploys and reload

**DEP-1. A deploy is the manifest plus the code the project runs.**
Editing `helpers.py` (committed or not) is a new deploy; a note or data
file beside the project is not, and neither is a commit of unchanged code:
the commit is shown, not digested. `SOLERA_BUILD` wins when set. The engine
numbers deploys as it serves them.

**DEP-2. A local serve reloads on code changes; a broken edit keeps the current deploy.**
On by default for `--insecure` loopback serves, `--reload` elsewhere.
Saves are debounced; attempts in flight finish under their own deploy.

**DEP-3. An attempt finishes under the deploy it was launched with.**
A new deploy (or a local reload) neither stops nor fails attempts in
flight. A worker whose code is another deploy than its attempt's fails
it, not retryably, saying why.

**DEP-4. A sensor host on old code takes no ticks and restarts on the new.**

**DEP-5. Migrations apply before an output's first write; a failed one is not retried.**
`solera migrate` applies eagerly and is idempotent; each schema's table
gets its own.

**DEP-6. A secret written into a built-in store's config is refused at registration.**
`PostgresStore("postgresql://u:pw@…")` is refused, naming `env:NAME`.

## STO — stores

**STO-1. A store has one contract: a write says what it wrote, a load says what it loaded.**
No store kinds. A write returns the keys it wrote and removed; a load
returns the keys and versions it actually loaded, absent included. A store
that writes in place keeps one version check per partition inside each
write transaction (STO-4). Registration refuses a store missing a method.

**STO-2. A load says the versions it read; a store that names objects by version loads exactly the planned ones.**
g5 writes {a:1}, a reader plans it; g9 writes {a:2}: the reader still
loads a:1, and says it loaded g5.

**STO-3. A key loaded at another version than planned is processed as loaded; nothing replans.**
`items` is on Postgres; `tally`'s batch plans k1@4 and k2@2, and `items`
commits k2 → 5 before the load. The batch gets k2 as updated at 5, its
lineage records the deviation (HIS-8), and once it commits k2 is not owed
again.

**STO-4. On a store that writes in place, a stale writer changes nothing.**
Each write transaction first raises the partition's version check to its
attempt's version, or is refused whole if it is already higher. g5
writes; g9 writes; g5 writes again: refused. A newer writer waits for an
older one's open transaction, which can then never write again. Nothing is
acquired before reads.

**STO-5. A table never holds a row its log does not have.**
Once no attempt is live on a partition of a store that writes in place,
it holds exactly the keys its output lists, at the versions its log
records. A dead attempt's rows are repaired by the next attempt (ENG-8).

**STO-6. Writes mean the same on every store.**
A replacement is the whole content; a patch changes only its keys; a key
with zero rows does not exist; many rows of one key are its group; the
same attempt writing again is one write.

**STO-7. A key is stored as itself, or the write fails.**
A Postgres `numeric` key column would read `1.0` back as `1`: the write
fails. A `bytes` key fails the write rather than vanishing.

**STO-8. A program reading a table outside Solera never sees an ordinary output's full run half done.**
`copy` is on Postgres and holds k1, k2, k3, k4; `items` then removes k3
and adds k5, and `copy` runs full in batches of 2. Until the run's last
batch commits, a query outside Solera reads k1 to k4 at their old
versions; from the switch that follows it on, k1, k2, k4 and k5 at the
new ones; never a mix. Solera's own readers read the build as it grows,
under the gate and the hold-back (STA-13). A Solera asset that takes
`copy` as a `TableRef` and queries it itself reads what outsiders read,
so the gate holds it until the switch. If publishing fails (a
constraint only the switch can check, such as a unique index across
partitions), outsiders keep the old rows and the full run is reported
failed to publish: its batches all committed, and Solera's own readers
already read its result. An output's first run is a full run of an
empty output, and its first batch says `batch.reset` (SEL-9). The one
exception (Erwin, 2026-11-11): a full run into a partition holding no
rows has no old rows to keep, so it writes in place. Outsiders may see it
fill, and a dead first run's partial rows stay visible until the next
run, which finds rows and builds beside. A full run released after a cancel (STA-13) is published as
it stands, once no attempt it interrupted is left unrepaired (at the
asset's next run if the cancel interrupted one). A
per-key asset's full run (SEL-10) keeps every key readable and changes
them a batch at a time, outside Solera as inside.
