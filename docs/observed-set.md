# The observed set: incremental reads from one observation record (draft)

Status: **draft**, revised after review A27 ("build with listed
changes"), W36's model of it (`ObservedSet.tla`: three counterexamples),
Erwin's removal of the pass (a run keeps its cursor in memory, and each
batch records what it observed at its own head), and his naming calls
(D140). Docs only; nothing is built. Names (D133): the **observed set** is what a consumer partition
has processed, key → upstream version; its stored form is the
**observation record** — a **base**, explicit **points** and compressed
**ranges**, with disjoint overwrites and prefix membership queries —
which decodes to it. The earlier encodings it replaces are on this
branch's history (`fb56cbe`, `bdf8962`, and the pass-based `89ac81b`).

## The principle

A consumer partition's **observed set** `S`, per keyed incremental input, is
key → the **observation** it processed: whether the key was present, its
version and payload as served, the whole and dep versions it was
processed under (its **context**), and the upstream's **life**. `S` is
independent of what upstream holds now.

What the partition owes is decided **key by key, once**: for each
**candidate** key, from its old effective state, `decode(R)(k)`, to its
new one, the upstream now under the current patterns and context:

| Old (`decode`) | New (upstream now, current patterns, current context) | Owed |
|---|---|---|
| absent | present | an add |
| present | absent, or no longer taken | a removal |
| present | present, another version, or another context | an update |
| same | same | nothing |

Net changes and membership queries never class anything themselves: they
only collect candidates, deduplicated, and the table above classes each
one (A27 R4). The owed set is the staleness, and exactly what a default
run loads, as `added`, `updated` and `removed`.

**`keys=` only chooses what a run loads**, per input: an explicit list of
keys, `"all"` (everything under the patterns, `keys={"rates": "all"}`), or
— the default — what is owed. It never clears an observation: the
observation record changes only through what runs commit.

**A delivered removal is final.** Once a removal is delivered — a pattern
narrowing's included — the record says the key is no longer held, so a
later re-inclusion is owed an add. The record keeps no history: that is
in lineage and run history.

**A pattern change is an input change**: patterns define the effective
input, so new patterns make staleness say *input changed*, not
*definition changed*. It stores nothing: it only changes the "new"
side.

**The invariant.** After every commit, `decode(R) = S`. Every override is
written by a committed batch, and every fold leaves the decoded value
unchanged.

## The observation record

The observation record `R` is three layers, the first that holds `k`
deciding:

1. **Points**: `k → observation`, explicit — presence, version, payload,
   the patterns it was read under, context, life. Written where an
   observation of `k` is not what a range or the base would decode: keys
   a run loaded by an explicit `keys=` list; a current-only store serving a row other than the
   version at the batch's head, or none; and a key that changed under a
   range as the range folds (below). A point pins nothing (A27 R11).
2. **Ranges**: disjoint key ranges `[lo, hi] → (H, patterns, context,
   life)`: every key in it as upstream had it at head `H`, if those
   patterns take it, else absent. A batch's commit writes one: the key
   range it covered, **observed at** the head it read at. Compressed: one
   range stands for every key of it.
3. **The base**: `(P, patterns, context, life)`: every key as at endpoint
   `P`, if those patterns take it, else absent. One per partition, in one
   of two forms (below): a **commit** `P`, or a commit `C` with a
   **before-image** of what changed since `P`.

Patterns are stored normalised: no `include` is the universal include,
never an empty list (A27 R3). Contexts are stored once per partition, in
a small table that layers refer to by id.

**Overwrite.** A batch's commit overwrites exactly the keys it covered: a
range write `(a, c] @ H` splits every range it overlaps — their parts
outside it stay — and drops the points inside, which the batch classed
again at `H`; then the batch's own points go on top. One
`AttemptFinished` carries the batch's outputs and its overwrite,
atomically (A27 R8).

**Fold**, at each batch commit, after the overwrite:

- **An older range relabels to `H`.** Its keys that changed between its
  own head and `H` — found with `changes` restricted to its key range —
  first become points at the version it observed; nothing left under it
  then changed, so it decodes the same at `H`. Points are therefore
  bounded by the keys that changed during the run.
- Adjacent ranges at the same head, patterns, context and life merge.
  When one range spans every key, it **becomes the base**.
- **An override drops** when the value decoded *with it removed* equals
  its own. So `P = k1@1`, a range `k1@2`, and a newer selection `k1@1`:
  removing the point would reveal the range's `@2`, so it stays.

A completed default run thus ends as a base at its last head, plus points
for the keys that changed behind its batches — which the next run owes.

**The commit check.** A batch commits only if, at its commit, its
input's life and its asset's definition are still the ones it was
planned under: an upstream reset or a definition change in between
refuses it, and it writes nothing. Today's engine has both checks (the
life, and a batch planned before an asset change); the observed set keeps
them.

**Outcomes.** A failed or abandoned attempt writes nothing: its keys stay
owed. A per-key batch's failed keys are processed — observed at the version
they failed at, and retried by their failure record (A19 R8), not owed. A
drained, canceled per-key batch commits its finished keys as points; its
interrupted keys are not observed, so they stay owed.

## A run

A run computes what is owed by comparing upstream now, under the current
patterns and context, with `decode(R)`. It processes the owed keys in key
order, `batch_size` keys a commit. Its **cursor lives in the run's memory
only**: nothing about the run is stored but what its batches commit.

**Each batch reads at its own head `H`**, pinned for that batch only by
the claim's existing reader pin (A27 R6). The claim reserves `H` itself
as well as the rows, so a retention cut never passes the head of a batch
in flight, and no batch commits a range `@ H` that nothing can decode
(W36's model, at `67b85eb`). It covers the key range from the previous
batch's last key to its own, `(prev, c]` — from the first key for the
first batch, to the last for the final one. At `H` it classes
every candidate in that range: the run's owed keys there, plus whatever
changed in the range since the run compared. So its range write
`(prev, c] @ H` holds for every key in it, owed or not. This rule is
load-bearing: with it off, W36's model breaks A19 R2's history (a key
added twice). It loads the keys it owes, calls the producer, and commits
its outputs, the range and its points.

A batch's `index`, `first` and `final` are relative to its run; there
is no `full`. A run given `"all"` covers the same key ranges and loads
every key in them; one given an explicit list loads those keys and writes
points.

A cancelled or failed run leaves its committed batches' ranges and
points. The next run compares again: keys under those ranges decode at
their heads, so the unchanged ones are not owed. No plan, cursor, pass or
scan pin is stored, and none can be left stranded.

## What a producer sees

`ctx.batch[input]` carries, all relative to the run:

- `added`, `updated`, `removed`: the owed classes of the principle's table;
- `unchanged`: keys loaded only because the run asked (`"all"` or a list)
  that the consumer already observed at the same version; empty in a
  default run;
- `rows`: the rows of `added`, `updated` and `unchanged` — the object the
  parameter receives; removed keys have no rows;
- `index` (0-based, in the run), `count` (the batches the run planned, an
  estimate), `first` (`index == 0`) and `final` (no batch of this run
  follows); there is no `full`;
- `upstream`: facts about the upstream.

A consumer that keeps a total moves it by the changes, starting from what
it holds. `ctx.load()` returns `None` before its first commit, and in a
full run until that run's first commit, so it starts from zero then;
`unchanged` keys are already counted:

```python
before = await ctx.load()
count = before["count"] if before is not None else 0
return {"count": count + len(batch.added) - len(batch.removed)}
```

## Two forms of base: a commit, or a commit with a before-image

A base is normally a **commit**: `P`, decoded against the upstream's
history — so the key index keeps `P` as an endpoint. When retention is
about to cut past `P`, at a cut `C`, the base takes its second form: the
commit `C` with a **before-image** — a file of only the keys changed in
`(P, C]` (under `P`'s patterns), each with its presence and version or
payload at `P`. A key added in `(P, C]` is in it as absent; one removed,
as present at its old version. Upkeep writes it once, from the index
while `P` is still readable (index metadata, no store code), and one
journal event installs it before the cut: `P` is released, `C` becomes
the base's endpoint. Points stay as they are. A range older than the cut
folds to `C` as at a batch commit: the keys changed in its range become
points, and it relabels. The before-image is the base's fold, kept as a
file because the base's changed keys can be many.

Decode reads the before-image for its keys and the key view at `C` for
everything else — the same answer `P` gave. Classify is unchanged.
Candidates: the before-image's keys, `changes(C, now)`, and a membership
query if the patterns differ from the base's. If retention later cuts
past `C` too, the next before-image adds the keys changed in `(C, C′]`,
keeping each existing key's entry at `P`. When a run's ranges become the
base, the file is deleted.

Its size is proportional to the keys changed in `(P, C]`, not to the
index: a reader that lags while a few keys churn holds a few entries
(~27 B each). A full file — the whole observed set — is only the degenerate
case where nearly every key changed.

The index stays reader-agnostic: retention asks for every base and range
older than its cut to be folded first, and keeps no per-reader snapshot.
The reader owns its before-image through its observation record.

## Observations: what was served (A27 R2)

An observation is what the store served, taken from the row itself: a
current-only store returns, with each row, its version and payload, and
says which named keys it has none for. Never a separate lookup of the
head index — a source can revert between the load and the lookup. A store
that cannot say what it served cannot back an incremental input on
current rows; its reads fail instead of guessing.

The batch's classes are then decided from the observations, before the
producer is called: planned as an update, served absent — a removal if
the key was held, nothing if not; served a version equal to what was
observed — nothing. So an aggregate's count follows what it was actually
given (a tally of 2 cannot be left holding one member). A served row
other than the version at the batch's head becomes a point.

## Context and lives (A27 R1, R7)

**Context.** Every layer carries the whole and dep versions it was
processed under. A key's old context is its layer's; one differing from
now owes an update. So a `keys=(k1)` run after `factor` moved to `w2`
writes `k1` under `w2` — fresh — while `k2`, decoded from a base under
`w1`, is owed; if `factor` returns to `w1`, it reverses. When the base's
context differs from now, every key the base decodes present is a
candidate, enumerated by a scan of the base even with no keyed change. A
batch records the context it read under, and its range carries it.

**Lives.** Each layer names the upstream index's **life**: its output
home and reset count, never a name or a commit number, which repeat
across lives; a rename keeps it, with the spill and pins. An upstream
reset (removed and declared again, moved to another store) starts a new
life and deletes the old index, so the old layers cannot be decoded.
Staleness says *input changed*, and the next run is a full run (below).

**A full run.** A definition change or an upstream reset stores no flag:
the partition's definition differs from its asset's, or `R`'s layers name
an old life, and the engine makes the next run a full run — the same as
`mode="full"`. Every upstream key under the patterns is owed.

- A **plain consumer**'s first commit resets its output (the store keeps
  nothing prior) and `R`, to an empty base in the current life; until
  then `ctx.load()` returns `None`, so the producer starts from zero.
- A **per-key consumer** compares against its own output index: its base
  becomes that index, where a key it holds decodes present at no upstream
  version. So every upstream key is owed, and an output key the upstream
  lacks is an owed removal — also after a cancel midway, since the keys
  past the last batch still decode from the output index. Its outputs
  stay readable while it runs.

## Candidates, queries and their cost

For a decode-equal state with unchanged patterns and context, the
candidates are complete:

- `changes(P, now)`'s keys, outside the ranges and points;
- for each range, `changes(H, now)` within it — which also finds a key
  absent at `H` and restored since, absent at `P` too (A27 R9);
- every point's key.

Outside them a key's old and new states are the base's, unchanged. Each
is classed once. A base with a before-image adds the before-image's keys
to `changes(C, now)`'s. A batch takes the same candidates within its key
range, at its head.

**When the patterns changed**, the keys whose membership may differ are
added: those a changed pattern can match. After normalisation, each glob
has a literal prefix (the characters before its first wildcard; a regex
only a proved common prefix), which bounds it to one key range: scanned
in each layer's index state and at now, clipped to that layer's range,
in the index's byte order with the exact prefix key included (`after` and
`until` are exclusive). A pattern with no prefix — the universal include
appearing or going, `**/archive/**`, `*template*`, an unanchored regex —
bounds nothing: its query is the whole range, a full compare.

**When the context changed**, the base (and any range under the old
context) is scanned for its present keys, as above.

**Costs.** The usual comparison is today's delta read plus point lookups.
A changed prefix pattern costs the keys under the prefix in each view. A
full compare reads the upstream at `P` and now, merged with the
overrides: two key views, streamed (bounded memory, not bounded reads);
at 100M keys several GB, run when a run plans. Endpoints retained: `P`
and each range's `H`. The fold relabels older ranges to the newest head
at every batch commit, so a run in progress holds two: `P` and its latest
`H`; points add none. Each range is one `changes` call over its key range;
points are one key-list call.

**Freshness is exact or pending** (A27 R10). Staleness is the comparison,
computed and cached per observation record revision and upstream head; where a
full compare is due and not yet made, the partition reports **pending**,
never `stale` or fresh on a guess: widening `include` to a key that
never existed changes no debt. Shared, definition and retry debt are
explicit, an empty first run included. The stale reasons are *input
changed* (keys owed, new patterns, a shared input moved, an upstream
reset) and *definition changed*. An `each` chain
intersects the upstream's owed keys with the consumer's patterns, key by
key, and rolls up with "any".

## Bounds and spill (A27 R5, R11)

Points and ranges live in the partition's record up to a bound (1,000
overrides, say). Past it they **spill** to an overrides index under the
consumer's index prefix, committed with the batch: each entry the whole
override — kind, presence, version, payload, the patterns and context it
was observed under (by id), life. Absence is a live, tagged payload, not
an index tombstone, which lookups skip and merges drop; an index deletion
only removes an override. Nothing is refused. Folding streams. The worst
case is explicit: `M` `keys=` keys left behind by a moving source are `M`
points, `M` values, and no retained endpoint.

## Worked examples

`feed` → `items` (keyed) → `tally`, a count `before + len(added) −
len(removed)`, and `checks`, per-key over `items`. `k1@3` is key `k1` at
generation 3; `⊥` an empty base.

**`keys=` before a default run.** `R = ⊥`. `keys=(k1)` at head `H1`:
point `k1@1`. `k1` updated to `@4`. Candidates: everything since `⊥`, and
`k1`: `k1` is owed an update (`@4` against `@1`), `k2` an add. The run's
one batch reads at `H`, delivers both, and writes `(−∞, +∞) @ H`, which
becomes the base `(H)`; the point is dropped with the rest of the
overwrite.

**Widening.** Base `(P, include=k1)`; the deploy widens to `k*`. The
membership query over prefix `k` finds `k2`: decoded absent, present and
taken now: an add. After `keys=(k2)`, a point `k2@1` under `k*`: decoded
present at the same version — nothing, not added twice. `keys=(k9)`, a
key that never existed: decoded absent, absent now — nothing, and no
point.

**Narrowing.** Base `(P, include=k*)`, narrowed to `k1`: the query over
`k` finds `k2`, decoded present, no longer taken: a removal.

**Removed, then included again.** `k5` is observed at `v1`. The patterns
narrow to exclude it: decoded present, no longer taken — a removal, which
a run delivers; its batch's range, under the narrowed patterns, decodes
`k5` absent. The patterns widen again with `k5` still at `v1`: decoded
absent, present and taken now — an add, though `v1` is what the consumer
once held. Had the patterns widened back before the removal was
delivered, `k5` would decode present at `v1`: nothing.

**A first include (A27 R3).** No `include` (universal) over `keep/a`,
`drop/b`; the deploy adds `include=keep/*`. The universal include going is
a prefixless change: the whole range is queried, and `drop/b` is owed a
removal. Querying only `keep/` would have missed it.

**An add during a narrowing (A27 R4).** The base's patterns took `drop/*`,
which had no key; `drop/b` is added upstream while the current patterns
exclude `drop/*`. `drop/b` is a candidate (a change since `P`): decoded
absent, excluded now: nothing. Classing the change under the old patterns
would have owed an add of an excluded key.

**A definition change.** The definition — the digest of the asset's
version, input bindings, store version and config — differs, and staleness
says *definition changed*. The next run is a full run: `tally`'s first
commit resets its count, `ctx.load()` having returned `None`; `checks`
compares against its output index, and its leftover outputs are owed
removals.

**A run in two batches.** From `⊥`, `batch_size=1` over `k1`, `k2`. Batch
1 reads at `H1`, delivers `k1`, and writes `(−∞, k1] @ H1`. `k2` changes
at `H2`. Batch 2 reads at `H2`, delivers `k2` at its new version, and
writes `(k1, +∞) @ H2`. Fold: `changes(H1, H2)` within `(−∞, k1]` is
empty, so that range relabels to `H2`; the two merge and span every key:
the base is `(H2)`. Had `k1` changed too, it would keep its `H1` version
as a point, owed by the next run.

**A cancelled run.** Batches 1 and 2 committed `(−∞, k1] @ H1` and
`(k1, k2] @ H2` (the first relabelled to `H2` at the second's commit, the
two merged); the run is cancelled before batch 3. The next run compares
again: keys up to `k2` decode at `H2`, so only what changed there since
is owed, and the rest is owed from the base. Nothing about the cancelled
run is left to resume or to release.

**A selection during a run.** A run's batch for `k1` has not come yet;
`k1` is updated to `@2`, and `keys=(k1)` delivers `@2`: a point `k1@2`.
When the run's batch covering `k1` reads at its head (`@2` or later), it
classes `k1` from the point: equal at `@2`, nothing. It cannot send the
consumer back to `@1`: every batch reads at its own head, never at an
older one.

**A shared input moves (A27 R1).** Base under `factor=w1`; `factor`
becomes `w2`. Every key the base decodes present is owed an update.
`keys=(k1)` writes `k1` under `w2`: fresh, `k2` still owed. `factor`
back to `w1`: `k1` owed, `k2` fresh.

**The current-only revert (A27 R2).** A batch reads at a head where `k2`
is `@2`; the store serves the row `@3`, saying so: the class is decided
from `@3`, and a point `k2@3` is written. Restored to `@2`: `@3` against
`@2`, an update. A row served ahead of every commit is a point all the
same: no commit has to describe it.

**The same-version re-arrival.** The store has no row for a planned `k2`:
for a plain batch, a removal if `k2` was held; for a per-key one, its
outputs go. Point `k2 absent`. `k2` returns at the same version: decoded
absent, present now: an add.

**A reader paused 40 days, a 30-day window.** `tally`'s base is commit
`P`, day 0, holding `k1@1 k2@1 k3@1`. `k0` is added just after, at `P′`;
a run that day delivers it in its first batch, writing `(−∞, k0] @ P′`,
and is cancelled. Its automation is then disabled, and the index keeps 30
days of history. Upstream: `k2` updated on day 10, `k3` removed on day
20, `k4` added on day 35. On day 29, retention's next cut `C` would pass
`P` and `P′`. Upkeep folds the range — nothing in `(−∞, k0]` changed, so
it relabels to `C` — and writes the base's before-image of what changed in
`(P, C]` outside it: `k2@1`, `k3@1` (present), two entries, not the whole
set. One event installs both; the base is `C` with the before-image, and
`P` and `P′` are released. On day 40 `tally` runs. Candidates: the
before-image's `k2` and `k3`, and `changes(C, now)`'s `k4`. Classed as
always: `k2` `@1` against `@7`, an update; `k3` present against absent, a
removal; `k4` absent at `C` against present, an add. `k0`, in the range
at `C`, and `k1`, in the base, are unchanged: nothing. The run completes:
its ranges become the base at its last head, and the file is deleted.
Without the fold and the before-image, day 40 would find `P` gone and owe
a full run.

**An upstream reset (A27 R7).** `items` is moved to another store; its
old index goes. `tally`'s layers name the old life: it owes a full run,
not a decode it can no longer make. A batch in flight across the reset is
refused by the commit check.

**A takeover mid-run.** Batches 1 and 2 committed their overwrites;
batch 3's attempt dies, writing none. The next engine decodes `R` to `S`;
its next run owes batch 3's keys and whatever changed since, and needs
nothing of the old run's cursor.

**Per-key versus plain.** One observation record for both. For `checks`,
failed keys are observed, retried by their failure records. For `tally`, `S`
is the only record of what the count holds: `added` is never a key it
holds, `removed` always one.

## Each past finding is a decode mismatch

| Finding | Why it cannot recur |
|---|---|
| A19 R1, R2: a key added twice | it decodes present: an update or nothing |
| A19 R3: a delivered removal forgotten | the removed key decodes present until a run removes it |
| A19 R4, A26 N2, N3: pattern-time selections | a selection writes points under the patterns it read under; folds are decode-equal; candidates are classed once |
| A19 R5, A26 N4, F38: served rows | observations come from the row; classes follow them before the callback |
| A19 R7, F37: shared inputs | context per layer; scanned when it moves |
| A19 R9, R10, F35: staleness | one comparison, exact or pending; `each` chains intersect key by key |
| F41: a position past a removal it never delivered (`items` over a current-only `feed` going `{…, k11}` → `{k11}` → `{k1, k3}`: read through commit 2, still holding `k11`, fresh) | nothing moves past an observation: `k11` stays in `S` as served until a batch delivers its removal, and the next comparison classes it from that decoded state against upstream now — an owed removal |
| A26 N1: the pass's rows | impossible: there is no pass; each batch reads at its own head, pinned by its claim |
| A26 N5: the cap | there is none: overrides spill |
| A27 R3, R4, R8, R9 | normalised patterns; candidates classed once; disjoint overwrite and decode-equal fold; per-range changes |
| `ObservedSet.tla` 1 (P1, W36): a retention cut past an active pass's `T` (a pass at `T = 1` commits `k1`; `k2` changes at 2; the cut at 2 folds the base and ranges, and the pass's next batch, `k2`, needs the key view at 1, gone) | impossible: there is no pass; the next batch reads at its own head. Its remainder, a cut past the head of a batch in flight, is closed by the claim reserving that head |
| `ObservedSet.tla` 2 (P2, W36): a pass sends a key back (the pass is at `T = 1`; `k1` is updated to `@2`; `keys=(k1)` delivers `@2`; the pass reaches `k1` and would deliver `@1`) | impossible: there is no pass; the batch covering `k1` reads at its own head and finds the point equal |
| `ObservedSet.tla` 3 (P1, W36): a batch committing across a change it was not planned under (an upstream reset, a definition change) | the commit check: the life and definition it was planned under |

A19 R6 (`keys=` bounded by `batch_size` and `concurrency`) and R8 (a
forced retry) are kept as they are, outside the observation record.

## Words retired

Each goes from the docs and the glossary when the observed set is built
(D140):

- **position** — *was* how far an input had read; now the observation record.
- **read-ahead** — *was* what `keys=` runs read past the position; now points.
- **pass** (full, delta, diff) — *was* a frozen walk of the upstream; now a
  run's batches over what it loads, each at its own head.
- **retry pass** — *was* a walk of the failed keys; now a run loads the keys
  its failure records make due.
- **pattern change** (as a process) — *was* a cut-over, a delta under the
  old patterns, and a membership diff; now an input change, and the
  comparison under the new patterns.
- **reconcile** — *was* a per-key cleanup after a full pass; now owed
  removals, against the output index in a full run.
- **rebuild**, **start-over** — *was* a flag owing a restart; now a full
  run, which a definition change or an upstream reset makes due.
- **seen** — *was* the whole and dep versions a partition caught up to; now
  each layer's context.
- **caught_up** — *was* a flag set by the commit path; now nothing owed.
- **fingerprint** — *was* its name; now the **definition**.
- **input unit** — *was* a unit of an input's record; gone. **Output unit**
  stays.
- **`Batch.full`** — *was* a full pass's flag; gone: `index`, `first` and
  `final` are relative to the run.

## What it replaces

| Today | With the observed set |
|---|---|
| Position: `next`, `pass` (`from`, `at`, `batch`, `pin`), `fingerprint`, `began`, `seen` | The base and ranges, each observed at a head; no pass, cursor or scan pin is stored; the `definition` (was `fingerprint`) stays: a change resets `R` |
| K45 read-ahead (`ahead`), its cap, paged selections' shared entries | Points, spilling past a bound |
| D93: snapshot passes; selections classed via `lower=` | Each batch reads at its own head, pinned by its claim; selections write points; `decode` replaces `lower=` |
| D100: classes from the index, rows from the store; rowless deliveries | Classes from what was served, decided before the callback |
| The pattern change: old patterns' delta, membership diff, old/new split | Normalised patterns per layer; prefix membership queries; one classification |
| `caught_up`, `caught_up_at`, `_dep_restart` | Derived: nothing owed; context per layer |
| Staleness: three predicates; K38's filter-then-confirm | One comparison, exact or pending |

Deleted: `_selection` and its branches, the pass and its pin, `held_at`,
`walked`, `read_from`, `pattern_change` and the `diff` mode,
`_read_ahead`, `_read_ahead_of`, `changes(lower=)`, `READ_AHEAD_FULL` and
the cap, `_dep_restart`, `caught_up`, most of `staleness.py` and
`positions.py`, the per-key reconcile, `Batch.full`, D100's rowless
deliveries and `gone_since`'s early removal. Kept: the key index and
`changes()`, endpoint reservation and the claim's reader pin, the failure
index and retries, D111's bounds; an unkeyed upstream's observation is
the one commit it last read.

## Testing

The staleness reference keeps `S` literally: a dict, key → observation,
updated by each step as the model says. A property test runs random
histories — upstream commits and resets, current-only rows served ahead,
behind or not at all, `keys=` and default runs, cancels and failures,
pattern changes (first include, prefixless globs) and definition and
shared-input changes, retention cuts, takeovers, renames — and after
**every commit** checks that `R` decodes to the dict, and that staleness
and a default run's load equal the dict's comparison with the upstream.
The A19, A26 and A27 histories are named examples, and so are
`ObservedSet.tla`'s three counterexamples and F41 (W38's replay in
`tests/sim/test_replays.py`, a strict xfail in the old model). Before
relying on the bounds: measure a large spill and a pattern change at
100M.

## What gets harder

- **Stores of current rows must say what they served**, per row; one that
  cannot fails its reads for incremental consumers instead of guessing.
- **A full compare at 100M** — a prefixless pattern change, a moved shared
  input, a truncated log — reads two whole key views; until a run makes
  it, the partition is pending.
- **An upstream reset** is a full run for its consumers.
- **Moving to it** resets every consumer once: an empty base, and a full
  first run.

## What we build

**Records** (in the consumer partition's record, per keyed input):

```text
Context   = {id, whole_and_dep_versions}
Layer     = {endpoint, patterns (normalised), context: id, life}
Base      = Layer                                   # endpoint P: a commit
          | Layer + {before_image}                  # endpoint C; the keys changed in (P, C], as at P
          | {output_index, life}                    # a per-key full run: held keys, at no upstream version
Range     = Layer + {lo, hi}                        # endpoint H, the head observed at; disjoint, sorted
Point     = {key, present, version, payload, patterns, context: id, life}
ObservationRecord = {base, ranges[], points{}, contexts[], spill: index state | None}
# beside it, per partition: the definition its observations were made under
```

**Decode** — the observed set, from an observation record (the old
effective state of `k`):

```text
decode(R, k):
    if k in R.points (or its spill):   return R.points[k] as an observation
    layer = the range holding k, else R.base
    if layer.output_index:             return HELD if k in layer.output_index else ABSENT
    if not layer.patterns.take(k):     return ABSENT
    if layer.before_image and k in layer.before_image:
        entry = layer.before_image[k]                      # as at P
        entry = entry if entry.present else None           # a key added since P: absent then
    else:
        entry = index(layer.life).lookup(k, at=layer.endpoint)
    return ABSENT if entry is None else (entry.version, entry.payload, layer.context)
```

**Classify** (one candidate, once):

```text
classify(R, k, now):                  # now: head, current patterns, current context
    old = decode(R, k)
    new = ABSENT if not now.patterns.take(k) else index.lookup(k, at=now.head)
    if old is ABSENT and new is ABSENT:         return NOTHING
    if old is ABSENT:                           return ADDED
    if new is ABSENT:                           return REMOVED
    if old is HELD:                             return UPDATED   # at no upstream version
    if differs(old.version, old.payload, new):  return UPDATED   # the net rule
    if old.context != now.context:              return UPDATED
    return NOTHING

full_run_due(R, now) = R.definition != now.definition or R.base.life != now.life
owed(R, now) = {k: c for k in candidates(R, now) if (c := classify(R, k, now)) != NOTHING}
# in a full run: every upstream key under the patterns, plus a per-key base's held keys
# candidates: changes(endpoint, now), plus a before-image's keys; plus per-range changes,
# point keys, membership and context scans
```

**A run** — the owed keys in key order, `batch_size` a commit, its cursor
in memory:

```text
run(R):
    todo = sorted(owed(R, now))                    # by default; keys= may give a list, or "all"
    prev = FIRST
    while prev is not LAST:
        keys = the next batch_size keys of todo after prev
        c = keys[-1] if todo goes on past it else LAST
        H = the head now, pinned by the batch's claim
        owed_here = {k: classify(R, k, at H) for k in candidates(R, H) within (prev, c]}
        load the keys owed_here owes, plus any other key the run asked for (unchanged)
        reclassify each from what was served
        call the producer
        if not may_commit(batch, R, now):
            return                                 # refused: its keys stay owed
        commit outputs, the range (prev, c] @ H, a point per key served otherwise
        fold(R, H)
        prev = c

may_commit(batch, R, now):
    return batch.life == now.life and batch.definition == now.definition

fold(R, H):                                        # at each batch commit
    for range in R.ranges with range.endpoint < H:
        for k in changes(range.endpoint, H, within=[range.lo, range.hi]).keys:
            if k not in R.points: R.points[k] = decode(R, k)   # as observed, before relabelling
        range.endpoint = H
    merge adjacent ranges with equal (endpoint, patterns, context, life)
    if a range spans every key: R.base, R.ranges = that range, []
    drop each override that decodes the same without it
```
