# The seen-set: incremental reads from one encoded state (draft)

Status: **draft** for Erwin's model. Docs only; nothing is built. If the
model is not confirmed, this note is dropped. The **interval map** (next
section) is the proposed encoding; position plus exceptions, which the
rest of the note describes, is kept as the comparison. The seen-set, the
invariant and the computation are the same for both.

## Proposed encoding: the interval map

A consumer partition's **seen-set**, per keyed incremental input, is key →
the upstream version it processed, as it was served (below, "The
seen-set"). The interval map encodes it as a piecewise map over key space: each key range → the
**commit it was observed at**, and the **patterns it was observed under**.
No interval carries a version or an "absent": both are derived from its
commit. Decoded, an interval `[lo, hi] @ (c, π)` means "each key in
`[lo, hi]` as upstream had it after commit `c − 1` — its version, or
absent — if `π` takes it; else absent".

| State | Intervals |
|---|---|
| A partition caught up | one: `(−∞, +∞) @ P` |
| A full pass part done | two: `(…, c] @ T` (its snapshot), the rest `@ P` |
| A `keys=` run, or a current-only read | a point `[k, k] @` the commit the read reflected: the head at which `k` had the served version, or was gone |
| After a default run that leaves nothing owed | one again: everything re-based to the head it reached |

**The diff, per interval.** `changes(c, now)` restricted to its key range
gives each key's net change since it was seen: an add, an update, a
removal, or nothing — classed under the interval's patterns `π`. Where the
current patterns `π′` differ from `π`, a **membership query** adds the
keys whose membership changed: one newly included is owed an add unless
the interval saw it (it did not, `π` excluding it), one newly excluded
that the interval saw is owed a removal. Then the run re-bases the
interval: `@ (now, π′)`. The union over intervals is the staleness, and
exactly a default run's load.

**Exactness, on the same examples.** `⊥` is the empty commit: it decodes
to nothing.

- *`keys=` before a default run.* `(−∞, +∞) @ ⊥`; `keys=(k1)` at head
  `H1` splits it: `(…, k1) @ ⊥`, `[k1, k1] @ H1`, `(k1, …) @ ⊥`. `k1` is
  updated at `H2`. The default run: `changes(⊥, now)` on the outer two
  gives `k2` an add; `changes(H1, now)` on `[k1, k1]` gives `k1` an update.
  Everything re-bases: `(−∞, +∞) @ now`.
- *A widening.* `(−∞, +∞) @ (P, include=k1)`, widened to `k*`. The
  membership query over `k*` but not `k1` finds `k2` upstream and unseen:
  an add. After `keys=(k2)` at `H` the map holds `[k2, k2] @ (H, k*)`,
  under the new patterns already: no membership change there, and
  `changes(H, now)` says nothing — not added twice. `keys=(k9)`, a key
  that never existed, leaves `[k9, k9] @ H`, which decodes to absent:
  nothing owed (or the run records nothing at all).
- *A narrowing.* `@ (P, k*)` narrowed to `k1`: the membership query over
  `k*` but not `k1` finds `k2`, present at `P`: a removal.
- *A definition change.* The map resets to `(−∞, +∞) @ ⊥`: everything
  upstream is owed an add, and the consumer starts over.
- *A full pass part done.* `(…, c] @ T`, the rest `@ P`: keys up to `c`
  are seen as at `T`; an update after `T` is in `changes(T, now)`, owed
  once.
- *The current-only revert.* A batch at `T` names `k2@2`; the store serves
  `@3`, which the head `H` holds: `[k2, k2] @ H`. Restored to `@2` (a new
  generation, or `v2`'s payload): `changes(H, now)` on `k2` says updated.
  A store ahead of its index (a row no commit has named yet) is recorded
  at the head too, and the commit that catches the index up arrives as an
  update: one redundant rewrite, never a missed one. A store behind its
  index that still holds an older row is the one case no commit
  describes (as for exceptions).
- *The same-version re-arrival.* `k2` gone when a per-key batch loads it:
  `[k2, k2] @ H`, `H` the head where it was gone. It returns:
  `changes(H, now)` says added.
- *A takeover.* Each batch's intervals commit with its outputs; an
  attempt that dies records nothing, and the map still decodes to `S`.

**Membership queries for globs.** A key's membership can only change
under a pattern that changed: one added to, or removed from, `include` or
`exclude`. So the keys to look at are those matching one of those
patterns. Each glob has a **literal prefix**, the characters before its
first wildcard (`ICP/Results/**/*.csv` → `ICP/Results/`), which bounds the
keys it can match to one key range. The query scans that range, clipped
to the interval: in the index at `c` for what was seen under `π`, and at
`now` for what `π′` takes. A prefix pattern costs the keys under its
prefix, not the index. A glob with no literal prefix (`**/archive/**`,
`*template*`), or an unanchored regex, bounds nothing: its query is the
interval's whole range — a full compare of that interval, which is what
position plus exceptions does for every pattern change.

**Size and folding.** One interval normally; one more for a pass under
way; one point per key a `keys=` run named, or a current-only read found
other than its batch's commit said. Adjacent intervals with the same
`(c, π)` merge. A point re-bases to a later commit `c′` when its key did
not change in between (`changes(c, c′, keys=…)`, batched), so a default
run that completes folds every point to the head it reached, and the map
is one interval again — but for current-only reads made during that very
run. Past a bound (1,000 intervals, say) the points spill to a small
index, key → commit, under the consumer's index prefix, committed with
the run; nothing is refused. A `keys=` run's points can share one commit
(the head it pinned), so its keys spill as one list.

**Can the index serve it?** `changes(first, last)` already takes a key
range (`after`, `until`) and a key list (`keys=`): it reads the spans
overlapping `[first, last]` and, in each, only the blocks covering the
range — a prefix's worth, not the index. Two limits:

- `first` must be a reserved endpoint (or 0). Every distinct commit in the
  map stays reserved while an interval names it; that is what retains
  versions, so points are re-based, and a run's points share its head, to
  keep the distinct commits few.
- One call takes one `first`. Intervals at different commits are
  separate calls, grouped by commit: a call per distinct commit, each a
  few seeks per span. A per-key `first` in one call would need the index
  to take it — `lower=` is close, but it carries a delivered presence the
  map derives from the commit instead.

**Against position plus exceptions.** The interval map stores no
version and no absent, so a point is a key and a commit; a pass and a
point are the same shape; and a pattern change costs a prefix's scan,
not a full compare. Its price: every commit it names must stay a reserved
endpoint, where an exception carries its version and pins nothing. The
seen-set, the invariant, the computation and the testing are the same.

## The seen-set, and the comparison encoding: position plus exceptions

Each consumer partition has, per keyed incremental input, a **seen-set**
`S`: key → the upstream version it processed (the version it was
**served**, which on a current-only store may differ from the one the
index named), whatever the upstream holds now. Nothing stores `S` key by
key. It is kept as an **encoding** `E`:

- a **position** `P`: an endpoint of the upstream's key index, with the
  patterns, and the whole and dep versions, in force when it was taken;
- **exceptions**: where `S` differs from what `P` decodes to. A **point**
  exception is `key → version`, or `key → absent`; a **range** exception
  is "the keys in `(…, c]` are as upstream had them at endpoint `T`" — a
  pass part done (below).

**Decode.** `decode(E)(k)` is the first that applies: a point exception
for `k`; a range exception whose range holds `k` (`k` as of `T`, under the
range's patterns); else `k` as of `P` if `P`'s patterns take it, else
absent.

**The invariant.** After every commit, `decode(E) = S`. An exception
folds into `P` only when `P` already decodes `k` to its value. Every
endpoint the encoding names (`P`, each range's `T`) is reserved in the
key index while it names it.

**The computation.** What a partition owes is the difference between the
upstream now, under its current patterns, and `decode(E)`:

| Upstream now | `decode(E)` | Owed |
|---|---|---|
| has `k` | lacks `k` | an add |
| has `k` at `v2` | has `k` at `v1` | an update |
| lacks `k`, or its patterns no longer take `k` | has `k` | a removal |
| has `k` at `v` | has `k` at `v` | nothing |

That difference is the staleness, and exactly what a default run loads,
as `added`, `updated` and `removed`. A `keys=` run loads the named keys of
it. Its candidates are `changes(P, head)`'s keys and the exceptions' keys:
a key in neither is as `P` decodes it and has not changed since, so it
owes nothing. A **full compare** — the upstream now merged with `decode(E)`
in key order — is needed only when the patterns, or the definition, differ
from those recorded with `P`.

**A run updates `E`.** Each batch sets `S` for the keys it processed: to
the version it was served, or absent. Its commit records that as
exceptions (point ones for scattered keys; one range exception for a pass
in key order), with its outputs, in one `AttemptFinished`. A run that
leaves nothing owed moves `P` to the head it reached, with today's
patterns and whole and dep versions, and folds every exception the new
`P` implies; those it does not imply stay.

## Worked examples

`feed` → `items` (keyed) → `tally`, a count `before + len(added) −
len(removed)`, and `checks`, per-key over `items`. `k1@3` is key `k1` at
generation 3. `⊥` is the empty position: it decodes to nothing.

**`keys=` before a default run.** `items` holds `k1@1 k2@1`; `tally` has
`E = (⊥)`. `keys=(k1)`: owed an add, processed, `S = {k1@1}`, encoded as
`(⊥, k1→@1)`. `k1` is updated to `@4`. The default run's candidates are
everything changed since `⊥` and `k1`: `k1` owes an update (`@4` against
`@1`), `k2` an add. Processed, `S = {k1@4, k2@1}`, which the head decodes
to: `E = (P=head)`, the exception folded. (Today: K45's read-ahead.)

**A pattern widening.** `E = (P, include=k1)`, decoding to `{k1@1}`. The
deploy widens to `k*`: the patterns differ from `P`'s, so a full compare.
`k2@1` is upstream, taken now, and decodes to absent: an add. A
`keys=(k2)` first instead leaves `(P, include=k1; k2→@1)`, and the
compare then finds nothing. A `keys=(k9)`, a key that never existed,
processes nothing and records nothing.

**A pattern narrowing.** `E = (P, include=k*)` decodes to `{k1@1, k2@1}`;
the deploy narrows to `k1`. Full compare: `k2` decodes to `@1` and is no
longer taken: a removal. The run removes it, and `E = (head, include=k1)`.

**A definition change.** The asset's version is bumped: `S` is what the
old definition processed, which the new one has not, so `E` resets to
`(⊥)` and everything upstream is owed an add; the first batch is `full`
and `first`, and the consumer starts over. A per-key asset keeps its
outputs through it; what they hold that upstream lacks is found in its
output index and removed after the pass, as today.

**A full pass, part done.** From `(⊥)`, a default run plans a pass at
endpoint `T` over `k1 k2 k3`; batch 1 commits `k1`: `E = (⊥; (…, k1] at
T)`. `k2` is updated meanwhile. Batch 2 continues from `k1`, still at
`T`: `k2` at `T`'s version, `E = (⊥; (…, k2] at T)`. The run ends with
`(⊥; (…, k3] at T)` = `(P=T)`. The update after `T` is a candidate of
`changes(T, head)`: owed once, by the next run. (Today: D93's snapshot,
whose pin is this range's reserved `T`.)

**A current-only revert.** `items` lives in a current-only store. A batch
at `T` names `k2@2`, and the store already serves `@3`: the worker reads
the served version from the head index when it loads the row, and the
commit records `k2→@3`. The source restores `k2@2` (a new generation, or
`v2`'s payload). The candidates include `k2` (an exception): `@3` against
the upstream's `@2`, an update, processed again. (Today it nets to
nothing and leaves `v3`'s data: A26 N4.)

**A same-version re-arrival.** A per-key batch at `T` finds no row for
`k2`, removed since: its outputs go, and the commit records `k2→absent`
(`T` decodes `k2` live). `k2` returns at the same version. The candidate
`k2` decodes to absent and is upstream: an add, and `checks` rebuilds it.
(Today it nets to nothing and `k2` stays missing: A26 N4.)

**A takeover mid-run.** Batches 1 and 2 committed, each with its
exceptions; batch 3's attempt dies uncommitted, recording nothing. The
next engine decodes `E`, which is `S` exactly, and finds batch 3's keys
still owed.

**Per-key versus plain.** One encoding for both. For `checks`, `S` is per
key the version its output was built from, and a key that failed counts
as seen at the version it failed at: its retry is its failure record's,
not an owed update. For `tally`, `S` is the only record of which upstream
keys its count holds — and that is what makes the count exact: `added` is
never a key it holds, `removed` always one.

## Keeping exceptions bounded

Point exceptions come from `keys=` runs (as many as they name), from a
current-only store serving another version than the index named (as many
as change while being read), and from keys found gone. Range exceptions
come from a pass under way: one each, since a pass goes in key order. A
default run that completes folds every exception its new `P` implies, so
they last only until the partition next catches up.

Up to a bound (1,000 point exceptions, say) they live in the partition's
record. Past it they **spill** to an exceptions index: the key index
format, key → version or a tombstone for absent, under the consumer's
index prefix, written as a delta with the run's commit and installed in
the same `AttemptFinished`. Nothing is refused: today's 10,000-run cap
goes. Reading them costs what reading a small index costs; a fold
rewrites it, and an empty one is dropped.

## Each past finding is a decode mismatch

Every one of them was a delivery classed against a history — a position,
a read-ahead entry, a pass — that no longer said what the consumer had
seen. Under the invariant the class is always `upstream now` against
`decode(E) = S`:

- **A key already held is never added** (A19 R1, R2, R4; A26 N2 repeated
  selections and the partial full pass): `decode(E)` holds it, so it is an
  update or nothing.
- **A key never held is never removed** (A26 N2's `k9`; the "never held"
  traces): `decode(E)` lacks it.
- **A removal is never lost** (A19 R3): the removed key stays in
  `decode(E)` until a run processes the removal.
- **A served version is what decodes** (A19 R5, A26 N4, F38): a newer
  row, or none, is recorded as an exception, so the next comparison sees
  the difference instead of netting it out.
- **A pattern change keeps what was delivered** (A26 N3): exceptions fold
  only when `P` implies them, so no transition can collapse one early.
- **A pass's endpoint is reserved because the encoding names it** (A26
  N1): the reservation follows `T`, whoever started the pass.
- **Nothing is capped** (A26 N5): exceptions spill.
- **Staleness and loading cannot disagree** (F35, A19 R9, R10): they are
  the same difference. Transitive staleness for an each chain intersects
  the upstream's owed keys with the consumer's patterns, key by key.
- **Whole and dep versions** (F37, A19 R7): they are recorded with `P`; at
  others, every seen key owes an update, and no pass continues under the
  old ones.

## What it replaces

| Today | With the seen-set |
|---|---|
| Position: `next`, `pass` (`from`, `at`, `batch`, `pin`), `fingerprint`, `seen` | `P` with its patterns and whole and dep versions; a pass is a range exception; `fingerprint` stays: a change resets `E` |
| K45 read-ahead (`ahead`), its cap, paged selections' shared entries | Point exceptions, spilling past a bound; no cap |
| D93: a full pass reads a pinned snapshot; selections recorded and classed via `lower=` | A pass is a range exception at its `T`; a selection's keys are point exceptions; `decode` replaces `lower=` |
| D100: classes follow the index, rows the store; rowless deliveries | The class is the comparison with `decode(E)`; a served version that differs is an exception, so no key is delivered rowless |
| The pattern change: old patterns' delta, a membership diff over a pinned snapshot | Patterns recorded with `P`; a full compare when they differ |
| `caught_up`, `caught_up_at`, `began`, `seen` on the position | Derived: nothing owed; `P`'s whole and dep versions |
| Staleness: three predicates over position, read-ahead and pass; K38's filter-then-confirm for `each` | One difference, for both kinds and every level |

Deleted from the code: `_selection` and its branches, `held_at`,
`walked`, `read_from`, `pattern_change` and the `diff` mode,
`_read_ahead` and `_read_ahead_of`, `changes(lower=)`, `READ_AHEAD_FULL`
and the cap, `_dep_restart`, `caught_up`, most of `staleness.py` and of
`positions.py`. Kept: the key index and `changes()`, endpoint
reservation, the failure index and per-key retries, per-key reconcile
against the output index, positions by commit for unkeyed upstreams (no
keys, no seen-set).

## Testing

The staleness reference keeps a literal seen-set: a dict, key → version,
updated by each step exactly as the model says. A property test runs
random histories — upstream commits and resets, current-only rows that
run ahead or vanish, `keys=` and default runs, pattern and definition
changes, takeovers — and after every step checks that the engine's `E`
decodes to that dict, and that its staleness and a default run's load
equal the dict's comparison with the upstream. The A19 and A26 histories
stay as named examples.

## What gets harder

- **Decoding at `P` and `T`** needs their states readable: the key index
  must keep every endpoint the encoding names, as it does today for
  positions and passes; a long-lived exception pins nothing extra.
- **Served versions on a current-only store** are read from the head
  index when a row is loaded. A store behind its index that still holds
  an older row would record a version newer than the row: the one gap
  left (a missing row is already F33's `SourceBehind`).
- **A full compare at 100M keys** reads the upstream index at `P` and now,
  merged with the exceptions: seconds, and run when a run plans; until
  then, staleness after a pattern or definition change is reported
  without its keys.
- **Moving to it** resets every consumer's encoding once: `(⊥)`, and a
  full first run.
