# Positions from what was read (design note, K43, K45–K47)

Status: **approved** by Erwin (K43, amended by K45 and its read-ahead
entries, K46, K47, and semantic change d).

**Built:** one record shape for every asset, `each=True` included: a
position plus read-ahead entries, no per-key payloads (K47); `keys=` on
any keyed incremental input through the read-ahead, capped at 10,000
entries per partition (K45); a full pass that may complete across runs,
`keys=` runs included; staleness on demand, transitive, as three
predicates (K46); whole and dep inputs caught up to in the partition
record (`seen`), not in the fingerprint (d); exact presence at the
position (K44): batches class each key added, updated or removed, net,
and "behind" counts only those, from the span key index's `changes()`,
the read-ahead classed by what each `keys=` run delivered.

**Not built:** a position *derived* from what attempts report they read —
positions are still moved by the plan an attempt was given, its
`selection` kind included, and `caught_up` is still set by the commit
path.
It supersedes K36–K42 and K40's wording.

## Units

- An **output unit** is the grain at which an asset stores and versions
  data: a **key** within a partition if the output is keyed, else the
  **partition**. An unpartitioned asset is one partition (`""` in code,
  as today); the console and the CLI never show it or ask for it.
- An **input unit**, per input, is what one output unit can be traced
  to: **one upstream key** if the asset is `each=True`, else the
  **upstream partition(s)** the input reads.

**Dependency.** With `each=True`, output key `k` depends on input key `k`
and on nothing else of that input. Otherwise every output unit of a
partition depends on the whole input unit: a keyed output that is not
`each` has all its keys depend on the input together, since nothing says
which upstream keys made which output key.

*Example.* `file_index` (`each=True`) reads `site_files`: its key
`alpha-file-2` depends on `site_files`' key `alpha-file-2`. `tally`
(keyed, not `each`) reads `site_files/alpha`: every key of `tally/alpha`
depends on all of `site_files/alpha`.

## The rule

The engine keeps a **record of what was seen** at input-unit grain, and
every successful attempt updates the records of the units it committed
from what it **actually read**. The run spec does not matter (default,
`partitions=`, `keys=`, a retry pass).

An output unit is **stale** iff an input unit it depends on changed
after the unit was written (K39: a change under the input's patterns; a
key the patterns exclude never counts), or its asset changed since.
Roll-ups use "any": a partition is stale if any of its keys is, an asset
if any of its partitions is.

The **position is derived** from the records, as the index planning uses
for incremental reads: not a second source of truth. *As built*, the
plan an attempt was given still moves it (`positions.advance`); what the
attempt reported (`delivered`) feeds only the read-ahead's covers and the
per-key outcomes.

## Where each record lives

| Input unit | Record | Where | Exists today? |
|---|---|---|---|
| upstream key (`each=True`) | nothing of its own: derived from the position and the read-ahead (K47) | — | — |
| upstream partition, incremental input | the position: `next`, the pass under way, the patterns | partition record | yes, but moved by the plan (below) |
| upstream partition, incremental input, keys read ahead | the read-ahead: `[commit, run, attempt, …]` per `keys=` run since the last pass (K45), the attempts of its pages | the position, capped at 10,000 entries | yes |
| upstream partition, whole or dep input | the head generation read (a digest of the refs, for a fan-in) | partition record (`seen`) | yes, since semantic change (d) |
| the declaration | the asset change it last caught up to | partition record | yes: `caught_up_at`, against `changed_at` |

**The read-ahead** (K45 and its amendment). A `keys=` run of a plain
incremental input is delivered the delta past `next`, filtered to the keys
it names and to what the read-ahead lacks, as of one upstream commit, the
head its attempt pinned; its attempts' specs and sealed results already
say what it read and delivered (it goes `batch_size` keys an attempt). So
the position records only `[commit, run, attempt, …]` (the run locates
them), and stores no per-key version. The record is snapshot
`next` plus that list:

- The next pass reads the delta past `next`. A changed key is skipped if
  an entry read it at or after its last change; one that changed again
  since is delivered. Planning reads the listed specs only when the list
  is not empty, together, into one map of key to the latest upstream
  generation an entry read it at (an output partition's generations rise
  with its commit numbers).
- Once a pass moves `next`, the entries read before it collapse into the
  snapshot: after a default run the list is empty. A `keys=` run that
  leaves nothing past `next` uncovered collapses it at once: `next`
  moves to the head it read, and the list empties.
- The cap counts entries, `keys=` runs since the last pass, not keys:
  10,000 per partition, past which a `keys=` run is refused with "run the
  partition first". One run may name any number of keys.
- Retention keeps a run whose attempt an entry names, until it collapses.
  A run deleted by hand loses its entry: its keys are delivered again.
- **A full pass due runs across runs** (Erwin's correction). After an
  asset change or a reset (no position, a fingerprint change, a log that
  no longer holds the delta), a full pass is due, and any mix of runs may
  complete it. It reads one snapshot, the upstream as of its start — a
  reserved endpoint of the key index (D93) — so every delivery in it, by
  any run, is truly added, and what changes after it reaches the next
  delta (A19 R1, R2). Its first delivery starts over, whether a `keys=` run or a
  default run: its first batch is full and first, so the consumer
  rebuilds (a count restarts from 0). Later `keys=` runs continue the same
  pass with their named keys, recorded like any read-ahead entry, with the
  pass as their base instead of a snapshot. A default run delivers only
  the keys the pass has not delivered and finishes it, never starting over
  again, so nothing earlier `keys=` runs wrote is dropped. Once every key
  of the snapshot under the patterns has been delivered within the pass,
  whichever run delivered the last piece, the record collapses to the
  snapshot, and the next delta brings what changed since — a delivered
  key's removal included (A19 R3). Nothing is delivered twice within a
  pass, and K44's added, updated and removed are relative to its start:
  everything is added. *Example:* `copy` holds k1, k2, k3, and its version
  is bumped. keys=(k1) starts over with k1; keys=(k2, k3) continues, and
  `copy` is fresh. Or, after keys=(k1), a default run delivers k2 and k3
  and finishes.
- **A batch planned before an asset change does not commit** (Positions.tla,
  approved): its commit is refused, as for a reset, when the declaration
  the fingerprint takes (version, store versions, migrations) changed since
  its claim. What it built is the old definition's, so it cannot finish the
  full pass the change makes due; the run goes on with a fresh attempt. A
  rename changes no definition: an attempt in flight commits under the new
  name.
- **A pattern change under way** decides membership first: a `keys=` run
  meanwhile merges the keys it names, and is recorded like any read-ahead
  entry (D93, A19 R4): the delta under the old patterns classes against
  it, and the membership diff skips a key it delivered at the key's
  version, so a count stays exact
  (`test_a_selection_during_a_pattern_change_is_counted_once`). An entry
  lists only the keys its run delivered, so a key the old patterns
  excluded is still delivered by the diff, and each key once
  (`test_a_pattern_change_after_a_keys_run_delivers_each_key_once`). A
  batch planned under the old patterns commits a position that names them,
  so the next plan finds the change and diffs.

**One record for every asset** (K47). `each=True` keeps the same record:
its `keys=` runs are read-ahead entries, counted by the cap, and its stale
keys are derived from it key by key (below). Positions.tla found the
snapshot plus read-ahead exactly as precise as per-key records in a 1:1
chain, so the key index stores no consumer payload. A retry pass (an
`each` asset's failed keys) whose last batch finds nothing past the
snapshot undelivered, read ahead or read by that batch, collapses the
record as a default run does. As built it seldom has to: a `keys=` run
that leaves nothing uncovered collapses the record itself, so entries stay
only while some changed key is undelivered, which a retry pass, reading
failed keys at their failed version, does not read.

A full pass records the claim generation that `began` it. Entries
recorded before a start-over stay in the record, but count toward the
pass only from `began` on: a start-over owes every key again, while an
older entry still says what a key was read at, which keeps a key read
under the old definition from looking never delivered. An entry also
stops counting once a whole or dep input it read commits again (it read
the old one). Within a full pass, a `keys=` run skips a named key the
pass already delivered at its current version, read ahead or walked:
nothing twice. For `each=True`, it covers (and collapses the record)
only if the output and its failed keys hold no key the pass's reconcile
would remove, except keys the run names: it removes those itself (R2).

## Runs target only what input units allow

- `partitions=` always: a partition is an output unit or holds them.
- `keys=` on any keyed incremental input, `each=True` included, through
  the read-ahead above (K45 replaced K43's refusal; K47, one record).

## How runs update records, and positions follow

Position at 56, upstream head at 60. Commits 56–60 touched `k1`, `k2`,
`k3`.

- **`each=True`, `keys=("k1","k2")`.** The same record as any asset: the
  run is delivered both as of 60, and the position becomes snapshot 56
  plus `[60, run, attempt]`; `k3` is stale, `k1` and `k2` are not. A later
  `keys=("k3",)` leaves nothing past 56 undelivered, and the record
  collapses to snapshot 61.
- **Not `each`, `partitions=("P1",)`.** A partition rerun reads its input
  unit whole, so P1's records update as any default run's do, and P1 is
  current. Today already works this way.
- **Not `each`, `keys=("k1",)`.** The run is delivered `k1` as of 60 and
  commits; the position becomes snapshot 56 plus `[60, run, attempt]`. The
  next default run delivers `k2` and `k3`, skipping `k1` (unless it changed
  again after 60), and collapses the list. Had the run named `k1`, `k2` and
  `k3`, nothing would be left past 56, and the record would collapse at
  once to snapshot 61. A named key that did not change past 56 is not
  delivered at all.
- **A default run** reads `from..to` in full, and the position moves to
  `to + 1`, as today.

## Staleness at every level

Staleness is **transitive and says why** (K46). A stale status carries
one or more reasons:

- **input changed**: an input unit it depends on changed since it read
  it; rerunning it helps;
- **upstream stale**: an upstream it depends on is itself stale, to any
  depth; rerun the upstream first, or run with `upstream=True`;
- **definition changed**: its asset changed (version, configuration,
  patterns, rename, added or reset) since it was written.

It is computed on demand, never stored as a flag: a project's statuses are
one walk up the lineage, memoized, each partition's direct checks or any
partition it reads being stale (`python/solera_server/staleness.py`).
*Example:* feed → items → copy. feed commits and items has not rerun:
items is stale (input changed), and so is copy (upstream stale), though
items has not moved. Automations are unchanged: `OnChange` fires on real
commits, and copy's input changes once items reruns.

Each reason is its own predicate, a function of the records, and the
reported reasons are exactly those that hold (Erwin's ruling); nothing is
kept of earlier causes, and each clears on its own condition:

- `definition_changed`: the asset changed since the partition last caught
  up. A pass under the new definition clears it. For an `each=True`
  asset, by key: a key it holds written before the change, or one its
  patterns no longer take, whose removal the pattern change owes (A19 R9).
- `input_changed`: an upstream reset replaced its input's content since it
  caught up (the model records when it dropped the position); or an
  incremental input has a key its patterns take changed past `next`, not
  read ahead at or after its change; or a whole or dep input is at another
  version than the one recorded at the last catch-up (`seen`), or none is
  recorded yet (d). That last makes a full pass due — the next default run
  is one, `keys=` runs continue it — and every key stays stale until it
  completes: the record keeps one version per partition, not per key. A missing
  position, or a pass under way, counts only when the asset's own change
  did not make that pass due: a full pass due only to an asset change is
  `definition changed` alone, until commits land past its base. A pass
  that reads the new upstream clears it.
- `upstream_stale`: a partition it reads is itself stale. Along an
  `each=True` chain, only through a stale upstream key its patterns take:
  a key depends on its own upstream key and nothing else (A19 R10).

So an upstream reset followed by the asset's own change reports both.
A key's last read is its read-ahead entry's, else the snapshot's, and the
latest read wins: a key read ahead and then removed upstream is behind,
whatever a net delta past the snapshot says (Positions.tla): it is classed
against what its `keys=` run delivered, live, so it is removed. Past the
snapshot alone, the net delta decides: a key added and removed past `next`
does not count, nor, at a versioned source, one updated and reverted.

What can be stale is what was built since its outputs' last reset: a
head, or a commit (a `keys=` run that took no key included). Never built,
or reset since, it is missing.

- **key** (`each=True`), from the one record: with a snapshot, a key the
  patterns take changed past `next`, read by no entry at or after its
  change (removed: only if the output holds it); with a full pass due or
  under way (no position, an upstream reset no pass has delivered yet,
  its definition changed, a whole or dep input moved), every key under
  the patterns the pass has not delivered at its current version, and
  every key the output holds the upstream has not; and along an each
  chain, the keys whose upstream key is stale. Its reasons are its
  keys': `definition changed` while the output holds a key, present
  upstream, whose generation (its writer's claim) is before the change;
  `input changed` while a key is stale as if the definition had not
  changed: a pass the change made due reads as the snapshot it left.
- **partition**: the reasons above (`each=True` alike). A key
  under the patterns changed, appeared or disappeared upstream past the
  position (K39); or a whole or dep head differs from its record; or
  `caught_up_at` is before `changed_at`. A keyed output that is not
  `each` has its keys share the partition's answer: all its keys are
  stale, or none. An unkeyed output has no keys (`tracked: false`).
  `GET /assets/{name}/stale-keys?partition=&after=`, `solera stale ASSET
  [PARTITION]`.
- **asset**: one of its partitions is stale.

No other state is needed. A default run's dry plan is the same
comparison: the work it would do is exactly the stale part.

## What it replaces or deletes

- **Gone:** the partition record's `reset` flag, and the clause that
  promoted a `keys=` run (both on the parked `held/keys-rule` commit,
  which this replaces); K36's "a keys= run never moves a position": a
  `keys=` run adds a read-ahead entry, or collapses the record when it
  covers (K45).
- **Gone** (K47): the key index's per-key consumer payloads and the
  held/each-payload records: an `each=True` input keeps the same record.
- **Not yet changed:** a position is moved by the spec's plan
  (`engine.py`, `commit_attempt`: `advance(plan, after)`), the
  `selection` kind included. The design has it become what the attempt
  reports it read (`delivered`: the keys, and the commits through which
  it read).
- **Gone** (semantic change d): the fingerprint's `refs` part. Each whole
  or dep input's version is the partition record's `seen`, so a change
  is a comparison that makes a full pass due, reason *input changed*, not
  a digest mismatch, reason *definition changed*. The fingerprint keeps
  the declaration and the run's config. What it still guards after K43:
  the run config alone. A declaration change already makes its full pass
  due through `changed_at` (positions point 3 refuses a commit across
  one), so the fingerprint's declaration part only repeats that; a run
  with another `config=` is the one interpretation change no other record
  holds.
- **To derive** (not built): `caught_up` (no unit stale) and
  `caught_up_at` (the declaration seen), from the records instead of set
  by the commit path.
- **Kept, but no longer consulted:** the history's lineage table. It
  stays the durable "what was each version built from", for users.
- **Tests:** `keys=` runs on plain incremental assets stay (K45 replaced
  K43's refusal); the staleness machine holds them, and every default or
  full run, to the keys the reference says it writes.

## Risks

- **Reset rule.** No record means everything is stale, which is correct.
  A selection then covers the partition only by naming every key: the
  bounded check above.
- **Pattern changes.** A pattern change is an asset change, so every unit
  is stale until it reads again under the new patterns. The position's
  diff pass keeps its old/new split.
- **Retry passes.** An `each` key that failed was read, so its entry, or
  the pass it failed in, covers it. Its failure record keeps it due:
  failing, not stale.
- **Repairs after a writer died.** A dead attempt committed nothing, so
  its reads update no record. A repair moves no record. Unchanged.
