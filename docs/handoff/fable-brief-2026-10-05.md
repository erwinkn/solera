# Incremental data flow tracking: architecture brief

Oct 5, 2026 · @Erwin

## 1. Summary

The system stores two logs of facts and derives everything else from them.

- **Output log**, per output partition: every commit, as one entry per changed key: `(key, commit, new version, replaced version)`.
- **Input log**, per consumer input: one small record per batch, saying "took key range (lo, hi\] at upstream commit H, under these conditions". It points into the output log and does not copy the keys a batch took.

&#91;embedded content: architecture · 2 logs, 2 derived structures, 1 query\]

Facts flow down. The change index and progress are rebuilt from the logs, and every question is one diff: from where a consumer stands to the head, or between the two commits a batch record names.

Three derived structures make the logs fast to use.

| Structure | What it is | Typical size (estimates) |
| --- | --- | --- |
| Change index | The output log sorted by key, in a few immutable layers | About 16 bytes per change |
| Progress | The input log folded into "key range → upstream commit" | 1 to 10 entries per input |
| Failing-key index | Keys of a per-key asset whose last attempt failed | One entry per failing key |

**The idea that keeps it small.** A batch reads one upstream commit H and walks keys in order. After it commits, every key in the range it covered is in sync with H, including the keys it did not touch: they were not owed, so they already matched H. A consumer's position is therefore a handful of key ranges, each tagged with a commit, and not one version per key.

**One query does most of the work.** `diff(output, C1, C2, key range)` returns the keys whose state differs between two commits. Staleness and planning call it with C2 set to the head. The lineage of a past batch calls it with that batch's own two commits. Planning and history run the same function over the same files, so history reproduces exactly what a batch received.

What this buys, at the scale in the requirements:

- **Fan-out is free.** Fifty consumers share one change index. Each adds a few hundred bytes of progress.
- **The input log stays tiny.** A 10,000-batch full run over 100M keys writes about 2.5 MB of batch records, against about 1.6 GB if keys were copied (estimates).
- **The hot path does no I/O.** Staleness for an edge is a comparison of numbers held in memory.
- **No tracking of running readers.** A time-based grace period replaces it (section 10).

Sections 2 to 6 are the core. Sections 7 to 11 are variations on it. Sections 12 to 15 cover operations. Sections 16 to 19 are review material: edge cases, the decisions I would like your opinion on, verification, and a map back to Q1 to Q10.

## 2. Vocabulary

These sixteen terms carry the whole brief. Each is used in one sense only.

| Term | Meaning |
| --- | --- |
| Output | One output partition: a versioned map from key to version. It has a stable id. |
| Version | An opaque token, compared only for equality. A generation id for produced outputs; the source's own version string for sources. |
| Commit | A numbered, atomic change to one output. Numbers start at 1 and are never reused. |
| State at c | The output's map after commit c. Written S(c). |
| Head | The output's latest commit. |
| Entry | One key's change in one commit: `(key, commit, new, replaced)`. |
| Edge | One keyed input of one consumer partition, reading one upstream output. |
| Conditions | What a key was processed under: the pattern set, the context, and the definition. |
| Observation | What a consumer holds for one key: nothing; or a version, with the context and definition it was processed under; or "held", meaning something at an unknown version. |
| Progress | The map from key to observation for one edge. Stored as pieces and points. |
| Piece | A key range with one value: `at(H, conditions)`, `empty`, or `held`. |
| Point | One key with an explicit observation, overriding its piece. |
| Owed | A key whose upstream state now, under the conditions now, differs from its observation. |
| Batch record | The input-log entry for one committed batch on one edge. |
| Grace period T | How long a replaced file is kept before it is deleted. |
| Horizon | The commit above which the change index keeps every entry. |

Notation used in examples: `a@2` is key a at version 2. `c4` is commit 4. A range `(lo, hi]` excludes lo and includes hi. `−` in a version column means absent.

## 3. The output log

Each output has an append-only log of commits. A commit is a small header plus one entry per changed key.

### Entries

| Field | Meaning |
| --- | --- |
| `key` | The key that changed |
| `commit` | The number of the commit that changed it |
| `new` | Its version after the commit, or absent if the commit removed it |
| `replaced` | Its version just before the commit, or absent if the commit added it |

The kind of change follows from the two versions: added (nothing replaced), updated (both present), removed (nothing new).

The commit header holds the commit number, its parent, a timestamp, who produced it (run, batch and attempt, or a source report), counts by kind, the live key count afterwards, the smallest and largest key touched, and a `clear` flag.

### Four rules

1. **Numbers never restart.** Commits are numbered 1, 2, 3 and are never reused. An output that is deleted and recreated gets a new id and a new log.
2. **Entries chain.** For one key, each entry's `replaced` equals the previous entry's `new`. Every merge checks this for free.
3. **One writer at a time.** A commit names its parent. The engine rejects it if the head has moved; the writer rebuilds and retries.
4. **A commit may be a `clear`.** It means "everything present before this commit is removed, then this commit's entries apply". Its own entries still carry the true replaced version. A key it removed without naming restarts its chain: that key's next entry replaces nothing. Only full runs use a clear (section 8). It is one flag, not 100 million removal entries.

### Why entries carry `replaced` (decision D2)

- **A diff needs only the entries inside its window.** Whether a key was added or updated is known without reading older data.
- **Cleanup knows which file each commit replaced.**
- **A key that went 1 → 2 → 1 is recognised as unchanged.**

The cost is that the writer must know the version each key it writes replaces. Often it already does, and otherwise it reads its own index.

| Case | Where the replaced version comes from | Cost (estimates, for a 10,000-key batch) |
| --- | --- | --- |
| A key the plan classes as added, on a key-preserving asset | The plan: progress says the consumer holds nothing | None |
| A `held` piece | The planner's scan of the consumer's own output | None |
| A store that returns it from the write, such as a table | The store | None |
| A full run or backfill of any other asset | The index slice covering 10,000 adjacent keys | About 160 KB |
| An incremental batch on a key-preserving asset, at 1% churn | The index slice covering the batch's range, about 1M keys | About 16 MB, 0.2 to 0.5 s |
| A re-keying asset whose output keys are scattered | One small block of the index per key, at worst | Up to 10,000 ranged GETs: about $0.004, and a few seconds in parallel |

Even the worst case costs less than the 10,000 PUTs that write the keys. It relies on small row groups in the base.

**The store adapter may supply it.** Returning the replaced version from a write is an optional capability of a store adapter. A table can do it in the same statement or transaction. A store of immutable files cannot do it cheaply: the file path contains the version, so the index is the only place that knows which version is current.

### The running example

Output `scans/alpha` receives four commits. The rest of the brief reuses them.

| Commit | Entries, as key: replaced → new |
| --- | --- |
| c1 | a: − → 1, b: − → 1, c: − → 1 |
| c2 | b: 1 → 2, d: − → 1 |
| c3 | c: 1 → −, e: − → 1 |
| c4 | a: 1 → 2 |

The resulting states:

| State | a | b | c | d | e |
| --- | --- | --- | --- | --- | --- |
| S(c1) | 1 | 1 | 1 | − | − |
| S(c2) | 1 | 2 | 1 | 1 | − |
| S(c3) | 1 | 2 | − | 1 | 1 |
| S(c4) | 2 | 2 | − | 1 | 1 |

### Sources

A source's log records what the orchestrator believes the source holds. It is fed by reports: a loader's listing, an explicit "these keys changed", or a consumer that read a key and found a different version (section 10).

Every report says what was observed, and the source head when the observation began. Whoever makes the report turns it into a commit against the current head, with one guard: for any key the log has changed since the observation began, the log wins and the report is ignored. Rule 3 covers the race at append. If the head has moved, the commit is rejected and rebuilt, and the guard is applied again.

## 4. The change index

The change index is the output log itself, sorted by key and stored as a few immutable layers on object storage. Nothing is stored twice (decision D3).

### Layers

- A layer covers a contiguous run of commits and holds their entries, sorted by key and then by commit, newest first.
- The newest layers are single commits. The worker writes a commit's entries as a layer file during the attempt, and the file is never rewritten.
- Background merging combines adjacent layers into larger ones.
- The oldest layer is the **base**: one entry per key present at the base commit.
- The list of layers (file, commit run, key range, size) lives in the engine's journaled state. Nothing is ever found by LIST.

```
commits   1 ............ 900 | 901 ..... 990 | 991 .. 998 | 999 | 1000
layers    base at c900       | merged        | merged     | one | one    <- head
```

### The queries

| Query | Returns | Used by |
| --- | --- | --- |
| `diff(C1, C2, range)` | Keys whose state differs between C1 and C2, with both versions | Planning, staleness, lineage |
| `scan(C, range)` | Every key present at C, with its version | New consumers, "all" and full runs, condition changes |
| `get(C, keys)` | The state at C of the listed keys | Key-list runs, points |
| `history(key)` | Every retained entry for one key | Key trace |

All take an optional "first N after key k" and a pattern filter. C1 and C2 can be any retained commits, not only the head.

### How diff works

1. Check that C1 and C2 are retained (see merging, below). If one is not, refuse, and name the nearest retained commits.
2. Pick the layers whose commit run overlaps (C1, C2\].
3. In each, read only the blocks that overlap the key range.
4. Merge them by key. For each key, keep the entries with C1 < commit ≤ C2.
5. The oldest of those gives the state at C1 (its `replaced`). The newest gives the state at C2 (its `new`).
6. Emit the key if the two states differ.

**Example: `diff(c1, c4)` on the running example.**

| Key | Entries in (c1, c4\] | At c1 | At c4 | Result |
| --- | --- | --- | --- | --- |
| a | c4 | 1 | 2 | updated |
| b | c2 | 1 | 2 | updated |
| c | c3 | 1 | − | removed |
| d | c2 | − | 1 | added |
| e | c3 | − | 1 | added |

**Example: a key that comes and goes.** Suppose c5 adds f and c6 removes it. `diff(c4, c6)` finds two entries for f. The oldest replaced nothing and the newest left nothing, so the states are equal and f is not emitted. A consumer sitting at c4 never hears of f.

### How scan works, and what a `clear` changes

`scan(C, range)` merges the base with the layers up to C and keeps, per key, the newest entry at or before C. If a `clear` lies at or before C, entries older than the latest such clear are ignored.

**A diff window that contains a `clear`.** Entries before the clear do not say what the clear removed. So the diff becomes a merge of two scans, `scan(C1)` and `scan(C2)`. That costs a read of every key in the range, which is unavoidable: everything changed. The engine keeps each output's list of clear commits, so a planner always knows whether its window holds one.

**Example.** c1 adds a, b and c at version 1. c2 is a clear that re-adds a at version 2. `scan(c2)` returns only a@2. `diff(c1, c2)` returns a updated, b removed and c removed.

### Merging (decision D5)

**The size rule.** A merged layer may hold at most 4 times the bytes of all newer layers combined, plus 1 MiB.

**What it guarantees.** A reader starting at any commit reads at most about 5 times the bytes that changed since, plus 1 MiB. The oldest layer it must open is at most 4 times the newer ones, and the newer ones hold only changes made after its start.

**When merges run.** Merge a run of four adjacent layers, newest first, when the largest is at most about 30 times the smallest and the result stays within the size rule. As a backstop, when an output has more than about 32 layers, merge the newest adjacent pair that fits the size rule, whatever their sizes. In a small simulation the first trigger alone rewrote about 5 bytes per byte committed and never held more than 16 layers over 3,000 commits. With a tighter size tolerance and no backstop, layers piled up into the thousands.

Folding old layers into the base is a separate merge, exempt from the size rule. It runs when the layers ready to fold reach about a quarter of the base's size. All these numbers are starting points for the benchmarks of section 18. The GET counts in the cost table assume the layer count stays in the tens.

**What a merge keeps.** Above the horizon, every entry. Below it, only what is needed to answer at the commits someone still refers to, called preserved commits (section 14). The horizon itself counts as one. Precisely: two consecutive entries of a key, at commits e1 < e2, are combined into one unless some preserved commit p satisfies e1 ≤ p < e2. The combined entry takes `replaced` from the older and `new` and `commit` from the newer. A combined entry whose `new` equals its `replaced` is dropped.

**A merged layer records where it is exact.** Each layer below the horizon lists the preserved commits it was merged for. A diff or scan may use it at those commits only. The engine holds these lists with the layer list, so a query at any other commit is refused before anything is read.

**Clears are boundaries.** A clear commit and the commit before it always count as preserved. So nothing is combined across a clear, and a clear's own entries are never dropped. Once the base moves to a commit at or after the clear, the keys the clear removed are simply not in it.

**Example.** Key b has entries at c2 (1 → 2), c5 (2 → 3), c7 (3 → 4) and c12 (4 → 5). The horizon is c10. A slow consumer still sits at c4, so c4 is preserved.

- The base moves to c4 and holds b@2. The entry at c2 folds into it.
- The entries at c5 and c7 combine into one: c7 (2 → 4). No preserved commit p satisfies 5 ≤ p < 7.
- The entry at c12 is above the horizon and stays as it is.

`diff(c4, head)` still answers exactly: b went from 2 to 5.

**A commit that is not retained.** The merged layer covering c5 to c10 is exact at c10 only. A question about c6 is refused: "not retained at c6; the nearest retained commits are c4 and c10". It is never answered from the combined entry, which would give the wrong version.

### File format (decision D4)

Layers are Parquet files sorted by key and commit, with small row groups so a key range reads only a few of them. The same files are readable by DuckDB, so history queries need no second copy. A commit with only a few dozen entries rides inline in its journal event and is written to a file by the first merge, saving a PUT and a GET.

### Costs (estimates)

| Case | Cost |
| --- | --- |
| Planning for a consumer a few commits behind | 0 to 2 ranged GETs |
| Planning 10,000 keys for a consumer one day behind (100M keys, 1% daily churn) | 3 to 6 ranged GETs of about 150 KB; 100 to 300 ms |
| Planning one batch of a full or "all" run | A base slice plus a slice per layer: 5 to 10 GETs |
| Base for 100M keys | 1.2 to 1.6 GB |
| 30 days of 1% daily churn | About 30M entries, 0.5 to 1 GB |
| One full rewrite of 100M keys | About 1.5 GB of entries, kept while inside the retention window |
| Bytes rewritten by merging, per byte committed | About 5, in a small simulation |

## 5. Progress: pieces and points

Progress says, for every key of an edge, what the consumer holds. It is a few pieces plus a few points, kept in the engine's memory and rebuilt by replaying the input log.

### Pieces

A piece is a key range with one of three values.

| Value | What it says about every key in the range |
| --- | --- |
| `at(H, conditions)` | The consumer holds the key as upstream had it at commit H, if the conditions' patterns match it. Otherwise it holds nothing. |
| `empty` | The consumer holds nothing. |
| `held` | The consumer holds whatever its own output contains. Which upstream version produced it is unknown (section 8). |

Pieces never overlap and together cover the whole key space. A new consumer has one piece: everything `empty`.

### Points

A point is one key with an explicit observation. It overrides the piece the key falls in. The observation is one of three things: nothing; a version, together with the context and definition it was processed under; or `held`. Points come from three places only:

- a key of a per-key asset that failed, which keeps what the consumer held before (section 9);
- a key processed through an explicit key list, ahead of its piece (section 8);
- a key read at a version other than the planned one (section 10).

Points carry explicit versions, not commit numbers (decision D7). One stuck key therefore never forces the index to keep old history.

### Decoding, and what "owed" means

To decode a key, use its point if it has one, otherwise its piece. A key is owed when the decoded observation differs from upstream now, under the conditions now.

| Consumer holds | Upstream now | Result |
| --- | --- | --- |
| Nothing | Present and matched | Owed: added |
| Version v | Same version, same context and definition | Not owed |
| Version v | Another version, or the context or definition changed | Owed: updated |
| Something at an unknown version (`held`) | Present and matched | Owed: updated |
| Version v, or `held` | Absent, or no longer matched by the patterns | Owed: removed |
| Nothing | Absent or unmatched | Not owed |

### What a batch commit writes

A batch that planned at head H and covered keys (lo, hi\] writes one piece: `(lo, hi] → at(H, conditions now)`.

Inside that range it then leaves a point for exactly those keys whose observation differs from what the new piece says about them. In practice these are failed keys, keys held back from this batch, and keys read at another version. Every other point in the range is removed.

The comparison is on the whole observation, not the version alone. A key that fails during a context change keeps a point even though its version did not change, because the consumer still holds it under the old context.

**Why this is correct.** A key in the range that the batch neither processed nor held back was not owed at H. "Not owed" means its observation already equalled what the piece now says. So the claim "everything here is as at H" is true for it as well.

### Worked example

Consumer `report/alpha` reads `scans/alpha` with batches of 2 keys.

| Step | Head | What the batch plans | Progress afterwards |
| --- | --- | --- | --- |
| Start |  |  | (−∞, +∞) empty |
| Run 1, batch 1 | c1 | A scan finds a, b, c. Takes a and b, both added. | (−∞, b\] at c1 · (b, +∞) empty |
| c2 lands |  |  |  |
| Run 1, batch 2 | c2 | A scan of (b, +∞) finds c and d, both added. Nothing lies beyond, so the range runs to +∞. | (−∞, b\] at c1 · (b, +∞) at c2 |
| c3 and c4 land |  |  |  |
| Run 2, batch 1 | c4 | `diff(c1, c4)` over (−∞, b\] gives a and b, both updated. | (−∞, b\] at c4 · (b, +∞) at c2 |
| Run 2 is canceled |  |  |  |

&#91;embedded content: progress as a step line · 5 keys, 4 commits, 2 pieces\]

Read it column by column. For keys a and b the consumer stands at the head, so nothing is owed. For c, d and e it stands at c2, and the two changes above that line are what the next batch takes.

Three things to notice.

- **After run 1, b is owed and nothing had to record that.** Key b changed at c2, under a range the run had already passed. The piece `(−∞, b] at c1` says so by itself: `diff(c1, c2)` over it returns b.
- **After the cancel, exactly the right keys are owed.** `diff(c2, c4)` over (b, +∞) returns c removed and e added.
- **The consumer is complete, only stale.** No piece is `empty`. Had run 2 been a full run canceled after its first batch, the second piece would be `(b, +∞) empty`, and d and e would be upstream keys never processed: incomplete.

### How pieces stay few (decision D6)

Progress changes in two ways only: a batch commit, and an explicit reset (section 8). Nothing tidies it in the background. Pieces stay few anyway.

- A run sweeps the whole key space, so its batches overwrite every older piece.
- Adjacent pieces with the same value merge.
- When little is owed, one batch covers a wide range. Three owed keys among 100M make one batch and one piece.
- The worst case is a 10,000-batch run racing upstream commits: up to 10,000 pieces, about 1 MB. The next run collapses them even if it finds nothing owed, because an empty batch still writes its range.

### Where progress lives

Pieces and points live in the engine's memory and are rebuilt by replaying batch commits and resets from the last checkpoint. When one edge has more than a few thousand points, they move to a sorted file on object storage, and the engine keeps only their count and key bounds.

## 6. A batch from start to finish

The worker plans a batch from immutable files, and the engine commits it as one journal event.

### The eight steps

1. **The engine issues a spec and stamps the attempt's start.** The spec holds the upstream head H and its layer list, the conditions now, the cursor key, the batch size N, the run mode, the plan time, the retry flags and the failing-key index files, and the pieces and points from the cursor onwards with their sequence number. Building it needs no I/O.
2. **The worker plans.** It walks the pieces after the cursor in key order and collects candidates until it has N. The range ends at the last key taken, or at +∞ if the candidates ran out; the planner peeks one key ahead to know.
3. **The worker reads** the added and updated keys at their versions at H (section 10).
4. **The worker runs the user function** with the keys and their classes.
5. **The worker writes the output:** the data files, then the output commit's entries as a layer file. It takes each `replaced` version from the plan, the store or the output's own index (section 3).
6. **The worker asks the engine to commit.** It sends the range, the counts by class, a digest of the keys taken, the exceptions (described below), and the locations of its layer file and outcomes file.
7. **The engine validates and appends one journal event.** That event is the output commit, the progress change, the batch record, and any source reports from the batch's reads, all at once.
8. **The engine applies the event in memory:** a new head for the output, a new piece for the edge, updated counters. The event is exported to the history tables later.

The plan is a pure function of the spec and immutable files, so the engine never handles key lists (decision D8). A 100M-key run never passes through its memory.

**A batch whose output does not change makes no output commit.** It still commits its progress. This keeps an empty batch from looking like a change to consumers downstream.

### Where candidates come from

| What lies under the cursor | Candidates | Class |
| --- | --- | --- |
| A piece `at(P)`, same conditions as now | `diff(P, H, range)` | From the two observations |
| A piece `at(P)`, other conditions | `scan(H, range)` merged with `diff(P, H, range)` | Section 7 |
| An `empty` piece | `scan(H, range)` | All added |
| A `held` piece | `scan(H, range)` merged with a scan of the consumer's own output | In both: updated. Upstream only: added. Own output only: removed. |
| A point | `get(H, key)`, compared with the point | From the two observations |
| A failing key that is not owed | `get(H, key)` | Unchanged, if the key should still be what it failed on. Otherwise the failure is moot and its entry is dropped (section 9). |

### What the engine checks at commit

| Check | If it fails |
| --- | --- |
| The attempt is alive and started less than T ago, taking the smallest T among the outputs it reads | Rejected. The batch is retried as a new attempt. |
| The edge's progress still has the sequence number the plan used | Rejected and replanned. This cannot fail while one task at a time runs per consumer partition; it is the safety net. |
| The asset's definition is the one the batch was planned under | Rejected. The run ends, and staleness reports "definition changed". |
| The output's head is the commit's parent | Rejected. The worker rebuilds the entries and retries. |

### Failure, retry and cancel

- **A batch that fails appended nothing, so nothing changed.** Its keys stay owed. The files it wrote are orphans, deleted when the attempt is closed.
- **A retry is a new attempt with a new spec at the new head** (decision D9). It may contain different keys from the first attempt. No head is held across retries, so the grace period only has to cover one attempt.
- **A cancel stops the engine issuing specs.** Committed batches stay committed.

### The batch record

This is the input-log entry. One is written per committed batch per edge.

| Field | Example: run 2, batch 1 of section 5 |
| --- | --- |
| Run, batch, attempt, mode | Run 2, batch 1, attempt 1, incremental |
| Edge | `scans/alpha` → `report/alpha` |
| Range | (−∞, b\] |
| Head | c4 |
| Conditions | Patterns p1, context x1, definition d1 |
| Replaced slice | (−∞, b\] at c1, with no points |
| Plan time and retry flags | 10:00:00, no forced retries |
| Exceptions | None |
| Counts | Updated 2 |
| Digest | Hash of \[(a, 2, updated), (b, 2, updated)\] |
| Planner version | 1 |
| Output | Head before: c2. Commit made: c3. |

The record is about 200 bytes plus the replaced slice, which is usually one piece.

**Exceptions** are the keys read at another version than planned (section 10), each with the version read. They are few, and they are facts.

**Failures are facts too, kept in their own file.** A batch with failing keys writes a small outcomes file, named in its journal event (section 9). Which keys were held back is not recorded. It is recomputed from the failing-key index as it stood at this batch, the plan time and the retry flags.

**A key-list run also records its key list.** The list is the user's input and cannot be derived. It is the one case where a batch record holds planned keys, at most N of them. Its replaced slice names the pieces and points those keys fell in, although the batch replaces only points.

**A reset is its own row in the input log.** The first batch of a full run follows it, and that batch's replaced slice is the freshly reset piece.

**Why it carries the replaced slice (decision D10).** The record is then self-contained. This batch's exact input is the diff from the replaced slice to the head, over the range. No earlier record has to be replayed, so retention can delete any run without breaking another.

**Why it carries a digest.** A lineage query recomputes the key list. If the hash differs from the stored one, the system reports an error and not a wrong answer.

### Several keyed inputs

The batch covers the same key range on every edge. Candidates from all edges are merged in key order. N counts distinct keys, so a batch never ends between two inputs that share a key. Each edge gets its own piece at its own head and its own batch record.

## 7. Conditions: patterns, context and definition

A change of patterns, context or definition writes nothing. Each piece remembers the conditions it was processed under, and a difference from the conditions now is what makes keys owed (decision D11).

| Part | What it is | When it differs from now |
| --- | --- | --- |
| Patterns | An id for the edge's include and exclude globs | Keys matched by exactly one of the two pattern sets are owed |
| Context | The head commits of the consumer's whole and watched inputs, read when a batch starts | Every present key is owed, as updated |
| Definition | The asset's declared version and config | Every present key is owed; ordinary assets rebuild with a full run |

Pattern sets are kept in a small registry by id, so old pieces and old batch records stay readable.

### Planning over a piece with other conditions

A diff is not enough here, because a key can be owed without having changed upstream. The planner scans the state at H and merges it with the diff. For each key in either:

- **Held before** is the key's state at the piece's commit, if the old patterns match it. That state is the diff's "before" when the key is in the diff, and its state at H otherwise.
- **Wanted now** is the key's state at H, if the patterns now match it.
- The two are compared with the table in section 5. A context or definition difference counts as "updated".

The staleness reason comes from the same comparison: it names whichever part differs.

Points are compared the same way. A point carries the context and definition it was written under, so a key-list result from before a context change is owed again after it.

### Example: patterns change

Upstream holds `a.csv@1`, `b.csv@1` and `c.xlsx@1`. The consumer processed it under patterns p1 = `*.csv`. The user changes the patterns to p2 = `b.csv` and `*.xlsx`.

| Key | Held, under p1 | Wanted, under p2 | Result |
| --- | --- | --- | --- |
| `a.csv` | 1 | Nothing: no longer matched | Removed. A per-key asset records the outcome "unmatched". |
| `b.csv` | 1 | 1 | Not owed |
| `c.xlsx` | Nothing | 1 | Added |

If the run is canceled midway, the pieces on the left carry p2 and the pieces on the right still carry p1. The next run picks up from that without any extra state.

Only key prefixes where the two pattern sets can disagree need scanning. Adding `archive/**` scans that prefix alone.

### Example: context changes

The consumer reads `rates` as a whole input, and its pieces are stamped with context `rates at c7`. Then `rates` commits c8. Every present key is now owed as updated, with the reason "context changed". Each batch of the next run reads the context when it starts and stamps it on its piece.

If `rates` moves again to c9 during that run, later batches stamp c9 and the earlier ranges are owed again afterwards. The consumer converges once `rates` holds still for one run. Fixing the context for a whole run is the alternative; section 17 weighs it.

### Example: definition changes

The definition goes from d1 to d2, so every present key is owed.

- **A key-preserving asset**, such as a per-key asset, replaces each key's output as the run reaches it.
- **An ordinary asset** does a full run: its first batch clears the output (section 8).

### Cost

A condition change costs a scan of the affected key range. That is inherent: those keys must be examined, and usually reprocessed.

## 8. Run modes

The four run modes differ only in which keys the planner takes and in what the first batch writes. The three that sweep the key space use the commit rule of section 5 unchanged. A key-list run writes points only.

| Mode | Keys the planner takes | What a commit writes | After a cancel |
| --- | --- | --- | --- |
| Incremental | Owed keys, and failing keys due for a retry, in key order | A piece for the range, plus points | The rest stays owed |
| All | Every owed key, and every other present key classed "unchanged" | The same | The rest is as it was |
| Key list | Exactly the listed keys, classed against what the consumer holds | Points only, no piece | The processed keys are done |
| Full | Everything, after a reset written by the first batch | The reset, then a piece per batch | The unreached part is `empty` or `held` |

### Key list: an example

Progress is one piece, everything at c2. The head is c4. The user runs keys a and e.

- Key a: the consumer holds 1, upstream now has 2, so it is updated. Key e: the consumer holds nothing, upstream has 1, so it is added.
- The commit writes two points, a → 2 and e → 1. The piece stays at c2.
- Afterwards `diff(c2, c4)` still returns a, c and e. The points for a and e equal upstream now, so only c is owed, as removed.
- The next incremental run covers the range, moves the piece to its own head, and drops both points.

A listed key that is not owed is classed "unchanged" and needs no point. A listed key inside an `empty` or `held` piece always gets a point, because its piece says something else.

### Resets

A reset sets an edge's whole progress to `empty` or to `held`, and clears its points and failure entries. It is the only change to progress other than a batch commit. It is an event in the journal and a row in the input log. A full run's first batch carries one, and the engine can issue one by itself when progress can no longer be trusted (see the end of this section).

### Full run of an ordinary asset

An ordinary asset's output keys are not tied to its input keys, so the output must be rebuilt from nothing.

- **The first batch's commit is a `clear`** on the consumer's own output. The same event resets every incremental input, keyed or unkeyed, to nothing consumed. It then writes the batch's own piece.
- **Later batches are ordinary.** Planning over `empty` pieces is a scan, and every key is added.
- **If the first batch fails, nothing happened.** The old output is intact.
- **If the run is canceled after batch 3,** the output holds what three batches produced, and the rest of the key space is `empty`. The consumer is incomplete.

### Full run of a key-preserving asset

A key-preserving asset writes each output key from the input key of the same name, and has exactly one keyed input. Per-key assets are the main case. The first batch resets progress to `held` and clears nothing.

**Example.** The consumer's output holds a, b and c. Upstream now holds a, b and d.

| Key | In own output | Upstream | Result |
| --- | --- | --- | --- |
| a | Yes | Present | Updated |
| b | Yes | Present | Updated |
| c | Yes | Absent | Removed |
| d | No | Present | Added |

The output never empties, and downstream sees updates and not a removal followed by re-adds. After a cancel, the unreached part stays `held`, so all its keys remain owed.

A key that fails during such a run keeps a point whose value is `held`: the consumer still holds its old result, at an unknown version. It stays owed as updated, or as removed if upstream drops it.

### Why the output's reset is a `clear` in the same log (decision D12)

- **Downstream consumers keep their progress.** They see one large diff across the clear. There is no new incarnation and no cascade of full runs.
- **Versions are never reused,** so a rebuilt key can never look unchanged by accident.

The cost is that the output is partial while the rebuild runs. A downstream batch planned in that window would process removals for keys not yet rebuilt. So "upstream incomplete" is a staleness reason, and by default it blocks downstream work (section 12). The engine checks it before issuing every batch spec, so a downstream run already in flight pauses too.

### When progress can no longer be trusted (decision D13)

This happens when an upstream output is replaced by a new one with a new id, when state is migrated from elsewhere, or when someone repairs a store by hand.

- **A key-preserving asset is reset to `held`.** Its next run reconciles its output against upstream, key by key.
- **An ordinary asset cannot use `held`,** because its output keys say nothing about its input keys. Its progress is left untouched and the consumer is flagged "full run required", exactly as after a definition change. Nothing reads the old progress again: only a full run is accepted, and its first batch resets everything.

## 9. Per-key assets: failures, retries and outcomes

Progress records success only. A key that fails keeps what the consumer held before, as a point, so it stays owed. A separate sparse index holds the failure and its retry schedule (decision D14).

### What a failing key leaves behind

Take run 2, batch 1 from section 5: it covers a and b at c4. Suppose b fails.

| Structure | After the commit |
| --- | --- |
| Piece | (−∞, b\] at c4, exactly as before |
| Point | b → version 1, with the context and definition it was processed under. The consumer still holds its result from version 1. |
| Outcomes file | b failed, with the error |
| Failing-key index | b → failed, 1 try, failed on b@2, next retry time, error excerpt |

Key a is not blocked: the piece moved on. Key b is owed: the consumer holds 1 and upstream has 2.

### The failing-key index

There is one per consumer partition. It is the current set of failing keys. Each entry holds the outcome kind (rejected, failed, retrying, canceled, timed out), the number of tries, when it started failing, the next retry time, the deadline, what the key failed on, and an error excerpt. "What it failed on" is the observation the batch tried to reach: a version with its context and definition, or a removal.

It is a small change index of its own. Each batch with failures or recoveries writes an outcomes file of the entries it adds and removes, named in its journal event. Background merges compact the files, and versions are kept as long as the run that wrote them. So the index as it stood at any retained batch can be read back, which lineage needs.

The engine keeps only a summary, the count per kind and the next due time, so a tick never scans the index.

### When a failing key is taken into a batch

| Situation | Taken? |
| --- | --- |
| The key is owed, and what it should be now differs from what it failed on: another version, another context or definition, or no longer wanted | Yes: it is new input |
| Its next retry time has passed | Yes |
| The run asks to retry failures | Yes |
| Otherwise | No. It is held back, and keeps its point if it has one. |

A deploy makes every failing key due at once. The plan time and the retry flags are part of the spec, so the choice can be recomputed later from the index as it stood.

**A failing key need not be owed.** In an "all" run, a key classed "unchanged" can fail. The consumer still holds a good result for that version, so nothing is owed and no point is needed. The entry stays, and the key is retried as "unchanged" when due.

### When the entry and the point go away

- **The key succeeds.** Both are removed.
- **Upstream removes the key, or the patterns stop matching it.** It is processed as removed or unmatched, and both are removed.
- **The failure becomes moot.** What the key should be is no longer what it failed on, and the key is not owed. The usual cause is upstream returning to the version the consumer holds. The next batch covering the key drops both.
- **A reset.** It clears both.

### The current outcome of any key

Outcomes are derived, in this order. Most keys need nothing stored.

| Check | Outcome |
| --- | --- |
| The failing-key index has the key | That entry's kind |
| The key is owed and has no entry | Pending |
| The consumer holds the version upstream has | ok |
| Upstream no longer has the key | Removed |
| Upstream has the key but the patterns do not match it | Unmatched |

### Why progress records success only

The alternative is to record a failed key as "seen at version 2" and let the failing-key index alone drive retries. Recording success only is more exact in three cases.

- **Retries are classified correctly.** A key whose first attempt failed is retried as added, because progress says the consumer holds nothing.
- **Removals are correct.** If a key that never succeeded vanishes upstream, nothing is owed. The consumer is not asked to remove what it never produced.
- **Progress always describes the output.** For a rejected key the output still holds the result of version 1, and progress says 1.

The cost is two small records per failing key, a point and an index entry, where the alternative has one.

### Mass failure

If a bug fails every key, points and entries approach the key count. There are two defences. Both structures spill to sorted files on object storage. And a circuit breaker can fail the whole batch when more than a set share of its keys fail: a failed batch records nothing, so one fact replaces 10,000.

## 10. Reading without pins

Nothing tracks which batch is reading what. A fixed grace period keeps replaced files alive long enough, and stores that cannot keep old versions report what they served (decisions D15 and D16).

### The grace period

Two rules replace all reader tracking.

1. A replaced file is deleted no sooner than T after the commit that replaced it. The same holds for layer files replaced by a merge.
2. The engine rejects a commit from an attempt that started more than T ago.

**Why this is enough.** The engine stamps an attempt's start when it issues the spec, which carries the head H and the layer list. A file in S(H), or a layer on that list, can only be replaced after that moment, so it survives until at least the start plus T. The attempt can only commit before the start plus T. So every attempt that commits has read exactly S(H). Only the engine's clock is involved.

**Example, with T = 6 hours.**

| Time | Event |
| --- | --- |
| 10:00 | An attempt starts, plans at c4, and needs a@2 |
| 10:05 | c5 replaces a@2 with a@3. The file for a@2 may be deleted from 16:05. |
| 10:20 | The attempt reads a@2, which is still there |
| 10:30 | The attempt commits |

A worker that hangs until 16:10 finds its commit rejected, because it started more than 6 hours ago. Its retry plans at the new head.

**T is set per output:** the longest attempt timeout among its consumers, plus a margin. An attempt that reads several outputs is checked against the smallest of their T values, which by construction is at least its own timeout. The price is storage for T's worth of replaced files. An output rewritten in full keeps two copies for T.

**Two conditions the argument relies on.**

- **Object names are never reused.** A deleted file can then only be missing, never wrong.
- **Readers that are not attempts read layer files only.** A lineage query or a listing of owed keys is not bound by T. If a merge removes a layer under one, it fails on a missing file and retries with the current layer list.

### Stores that move on

Some stores hold only the current version: a folder of files, an API, a Postgres table. A batch cannot read them "as of H". The rule for these: a read returns the content together with its version, and the batch records any difference from the plan.

| Planned at H | The read returned | What happens |
| --- | --- | --- |
| b@2, to update | b@2 | Nothing special |
| b@2, to update | b@3 | Processed as b@3. Point b → 3. The source log gets a commit b: 2 → 3. |
| b@2, to update | Nothing: b is gone | Processed as removed. Point b → absent. The source log gets b: 2 → absent. |
| e@1, to add | Nothing: e is gone | Dropped from the batch. Point e → absent. The source log gets e: 1 → absent. |

**Why the read is reported back to the source log.** Without it, the log still says b@2 while the consumer holds 3. Key b would look owed forever, and every run would read it again. With it, the log says 3, the consumer holds 3, and other consumers learn of the change.

**Reports travel with the batch.** They are part of the batch's journal event, so the point and the source commit appear together or not at all.

**A late report never overwrites newer knowledge.** This is the guard of section 3: a report is ignored for any key the source log has changed since H. The worker checks this when it builds the report's commit, against the source head it names as the parent. If that head has moved when the engine appends, the engine rejects the commit and the worker checks again. A dropped report leaves the point standing alone; the key is then owed, and the next run settles it. Comparing versions would not be enough: a store that went 2 → 3 → 2 would fool it.

**What the store adapter must provide.** Content and version must come from one read. An S3 GET returns the body with its ETag. A folder needs the version checked again after the read, or computed from the bytes read. A table needs a row version in the same query.

### What is lost for these stores

A batch may see half of an upstream change. Suppose x is renamed to y after planning. The plan holds x; the read finds x gone and processes a removal; y is not in the plan and arrives one batch later. The output briefly shows neither.

This is harmless for per-key assets. It matters for functions that combine the keys of a batch. The batch record lists the keys that were read at another version, so lineage stays exact either way.

## 11. Other input kinds and partition mappings

The other input kinds are the keyed design with the key dimension removed.

| Input kind | Progress | What a batch takes | Lineage |
| --- | --- | --- | --- |
| Keyed incremental | Pieces and points | Owed keys | A diff over the batch's range |
| Unkeyed incremental | One number: "consumed through commit c" | The commits after c, up to a cap | The commit interval (c, H\] |
| Whole | None of its own; its head is part of the context | The whole upstream at H | The head that was read |
| Watched | None of its own; its head is part of the context | Nothing | The head that was seen |

**Unkeyed incremental.** The upstream's commits carry no keys, only appended data. The edge's progress is a single piece over a key space with one point in it. The edge is stale when the head is past c. A reset returns it to "nothing consumed", and the next run takes every commit whose data the upstream still holds.

**Only whole or watched inputs.** Such a consumer is stale when the context now differs from the context in its last batch record.

### Partition mappings

An edge is one pair of upstream partition and consumer partition, with its own progress.

- **One-to-one:** one edge.
- **Broadcast:** one upstream partition feeds many consumer partitions. Each has its own edge and progress, and all share the upstream's single change index.
- **Fan-in:** one consumer partition reads many upstream partitions, with one edge each. A batch covers the same key range on all of them. The same key arriving from two upstream partitions is two inputs, told apart by edge.

## 12. The hot path: staleness, completeness and lag

The per-second evaluation reads a small summary per edge that changes only when something happens. It does no I/O and does not grow with history.

### What the engine keeps

| Per edge | Size |
| --- | --- |
| Pieces and points; each point has a cached flag: owed, not owed, or unknown | Usually under 1 KB |
| The conditions now | Three ids |
| Pending counters: commits and changes since the edge's oldest piece, and the time of the oldest | Three numbers |
| Failing-key summary: count per kind, next due time | A few numbers |

Per output it keeps the head, the live key count, the list of clear commits, and the headers of the last thousand or so commits.

### What updates the summary

| Event | Update |
| --- | --- |
| An upstream commit | For each edge reading it: one more pending commit, and the commit's change count added to pending changes. A point's flag is refreshed if the engine holds the commit's entries, and set to unknown otherwise. |
| A batch commit | The edge's pieces and points change. When a run finishes its sweep, the counters are recomputed from the headers of commits after the run's oldest head. If that head is older than the headers kept, a background query over `commits` supplies the figure. |
| Patterns, context or definition change | The conditions now change |
| A reset | The edge's progress becomes `empty` or `held` |

### Q1: is anything owed, and why

An edge is clean when every piece is at the head under the conditions now, and no point is flagged owed or unknown. Otherwise the reason is read off the summary.

| Reason | Test | Starts a run? |
| --- | --- | --- |
| Input changed | A piece is behind the head and pending changes are above zero | Yes |
| Patterns, context or definition changed | A piece's conditions differ from now in that part | Yes |
| Never processed | A piece is `empty` and holds upstream keys | Yes |
| To reconcile | A piece is `held` | Yes |
| Full run required | The consumer is flagged: an ordinary asset after a definition change, or after its upstream was lost | Yes, as a full run |
| Failures due | The failing-key summary's next due time has passed. A deploy makes every failure due. | Yes |
| Failures waiting | Failing keys exist, and none is due | No |
| Upstream stale or incomplete | The producer of the upstream has a reason above; this spreads along the graph | By policy, checked before every batch |

**"Input changed" can be a false alarm.** The changes may cancel out, as in 1 → 2 → 1, or fall outside the patterns. The next run plans, finds nothing, and commits an empty batch that moves the pieces to the head. An empty batch makes no output commit, so the false alarm does not travel downstream. It costs one planning read.

**Which keys are owed, with reasons,** is the planner run without a limit. It is computed on demand, for a screen or an API call, and costs a read proportional to the answer.

### Q3: completeness

A consumer is incomplete when an `empty` piece's range holds an upstream key that the patterns match and that has no point. The check is a scan that skips keys with points and stops at the first key without one. It is cached against the edge's sequence number and the upstream head, and recomputed off the hot path when either changes. It is never stored as a fact.

Note the distinction this draws. A key added upstream after a piece's commit makes the consumer stale. Only a range the consumer has never processed makes it incomplete.

### Q5: lag

| Figure | Source | Exact? |
| --- | --- | --- |
| Commits behind | Pending commits | Exact |
| Keys owed because the input changed | Pending changes | An upper bound: a key changed twice counts twice. A `clear` adds the live key count before it. |
| Keys owed because conditions changed, or a piece is `empty` or `held` | The upstream's live key count, plus the consumer's own for a `held` piece | An upper bound |
| Keys failing | The failing-key summary | Exact |
| Age of the oldest owed change | Time of the oldest pending commit | An upper bound |
| Exact count of keys owed | The planner in count mode | Exact; costs a read proportional to the changes |

The display says what it knows: "at most 37 keys owed across 3 commits, oldest 14 minutes ago". The engine keeps the entries of recent small commits in a bounded cache, so for an edge only a few commits behind the figures are exact.

### Cost

A tick over 100,000 edges reads cached flags and takes milliseconds (estimate). Each upstream commit costs work proportional to its fan-out.

## 13. History: lineage, key trace and metrics

Per-key history is computed from the two logs when asked. Only facts that cannot be recomputed are stored per key (decision D17).

### The tables

All are Parquet on object storage, queried with DuckDB. Rows are exported from the journal by position, so each event lands exactly once.

| Table | One row per | Holds |
| --- | --- | --- |
| `runs` | Run | Mode, trigger, times, status |
| `attempts` | Attempt of a batch | Head, planned range, status, error |
| `batch_inputs` | Committed batch and edge | The batch record of section 6. This is the input log. |
| `commits` | Output commit | The commit header of section 3 |
| `key_outcomes` | Key with a non-ok outcome in a batch | Key, version, kind, error, duration |
| Change index layers | Entry | The output log's entries, in the same files the planner reads |

### Q6: batch lineage

**Input side.** Run the planner again with what the batch's record holds: its mode, range, head, conditions and replaced slice. For an incremental batch that is `diff(piece's commit, record's head)` over each replaced piece. Then apply the record's exceptions, and check the result against the digest.

A replaced slice that was `held` needs the consumer's own output as it stood just before the batch. The record stores that output's head for this purpose. Held-back keys are recomputed from the failing-key index as it stood at the batch.

**Output side.** Read the entries of the batch's output commit.

**Example: run 2, batch 1.** The record says range (−∞, b\], head c4, replaced slice (−∞, b\] at c1. `diff(c1, c4)` over that range returns a: 1 → 2 and b: 1 → 2, both updated. The hash matches. The output side is the entries of `report/alpha` c3.

The cost is that of planning the batch once: a few GETs. It needs c1 and c4 to be answerable in the index, and they are, because a retained batch record keeps the commits it names preserved (section 14).

### Q7: key trace

A trace alternates two lookups.

1. `history(key)` on an output gives the commits that changed the key. Each commit header names the batch that produced it.
2. For each consumer of that output, find the batch records whose range holds the key. A batch received the key if the key is in that batch's lineage, computed for the one key.
3. Follow each such batch to its output commit, and repeat from step 1 on the consumer's output. A key-preserving consumer continues with the same key. Any other continues at commit level: "the output commit that consumed it".

**Example: trace b from `scans/alpha`.**

| Step | Lookup | Finds |
| --- | --- | --- |
| 1 | `history(b)` on `scans/alpha` | c1: added at version 1. c2: 1 → 2. |
| 2 | Batch records of `report/alpha` whose range holds b | Run 1 batch 1 (head c1, from `empty`). Run 2 batch 1 (head c4, from c1). |
| 3 | What each received | Run 1 batch 1 received b@1 as added. Run 2 batch 1 received b@2 as updated. |
| 4 | Their output commits | `report/alpha` c1 and c3. The trace continues there. |

The outcome at each hop is the key's row in `key_outcomes` if it has one, and ok otherwise. Each hop costs a few GETs; one edge's batch table is megabytes.

### Q8: flow metrics

Metrics come from numbers stored at commit time. No query reads keys.

| Metric | From |
| --- | --- |
| Keys per hour on an edge, by class | The counts in `batch_inputs` |
| How often full runs happen | The mode in `runs` |
| Reuse per batch, such as "touched 4 of 1,204 keys" | The batch's count, over the upstream's live key count at the batch's head |

### What is stored per key

- **Stored:** failures, with kind, error and duration. They cannot be recomputed.
- **Not stored:** the ok keys of each batch. Lineage derives them.
- **A choice for you:** duration per ok key is a fact that cannot be recomputed. Storing it costs 100M rows per full run. The default here is a duration histogram per batch.

### Honest answers when information is gone

| Question | Answer |
| --- | --- |
| The data of an old version | "b was at version 1 then; that file has since been deleted" |
| A state between two preserved commits below the horizon | "2 at c4, 4 at c10; versions in between are not retained" |
| A batch from a pruned run | "run not retained" |
| A recomputed key list that does not match its digest | An error naming the batch; never a silently wrong list |

## 14. Retention and cleanup

Three clocks decide what is kept, each for its own reason.

| Clock | Length | What it keeps | Why |
| --- | --- | --- | --- |
| Grace period T | Hours | Replaced data files, and layer files replaced by merges | A running attempt may still read them |
| Run retention | 30 to 90 days, set per asset | History rows, and every index entry above the horizon | A retained run must be answerable exactly |
| Live progress | As long as a piece exists | The index's state at that piece's commit | A consumer must be able to diff from where it stands |

### Cleanup of replaced files (Q9)

Each entry names the version it replaced. For stores where a commit writes new files, a cleanup cursor per output walks the commits in order. For each commit older than T it deletes the files of the replaced versions, then moves on. Deletes are idempotent.

**Example.** Commit c4 (a: 1 → 2) lands at 10:05 and T is 6 hours. From 16:05 the cursor deletes the file of a@1. The index still knows a was at version 1 from c1 to c3; only the data is gone.

A `clear` commit replaces every key present just before it. The cursor finds them with a scan at the previous commit.

### The horizon and preserved commits (decision D18)

- **The horizon** of an output is its head as it was when the oldest retained run that touches it began. That run may belong to its producer or to any of its consumers, so the longest retention among them decides. Every entry above the horizon stays, and the head of every retained batch lies above it.
- **Below the horizon,** only the states at preserved commits must stay answerable. A commit is preserved while a live piece sits at it, or a retained batch record names it. A clear and the commit before it are preserved too, until the base and the cleanup cursor have both passed them.
- **Between two neighbouring preserved commits,** merges combine a key's entries into one (section 4). Below the lowest preserved commit, everything folds into the base.

**Example.** An output is at commit 50,000. Run retention is 30 days, and the head 30 days ago was 41,000, so that is the horizon. Consumer A's pieces sit near 50,000. Consumer B was paused four months ago at 12,000.

| Commits | What the index keeps |
| --- | --- |
| Above 41,000 | Every entry |
| 12,000 to 41,000 | One combined entry per key that changed |
| Up to 12,000 | The base: one entry per key present at 12,000 |

B costs at most one entry per key that changed in those months, never more than the key count. When B runs again, its batch records name 12,000 as their starting commit, so 12,000 stays preserved until those records are pruned.

**Tracking preserved commits.** A merge reads the preserved set at one journal position. Live pieces come from the engine's state at that position. Commits named by retained batch records come from a DuckDB query over `batch_inputs`, which must be exported at least up to that position before the merge starts. A batch committed later can only replace a piece that was live at that position, so its commit is already counted.

### Pruning (Q10)

History tables are partitioned by the day a run started, so pruning drops whole files. The horizon then advances, merges combine entries or fold them into the base, and the files they replace are deleted after T. Pattern sets and commit headers go when nothing refers to them.

### A simpler first version

A single cut can stand in for the horizon and preserved commits: the oldest commit anything refers to, with every entry kept above it. The file format is the same and only the merge rule differs. Its weakness is that one paused consumer makes the index keep every entry since it stopped.

### What grows with what

| Structure | Grows with |
| --- | --- |
| Change index above the horizon | Changes made during the retention window. An asset's own full run adds one entry per key. |
| Change index below the horizon | At worst, the key count times the number of preserved commits |
| Input log | Batches committed during the retention window |
| Progress | Pieces and points that exist now |
| Engine memory | Live things only: outputs, edges, running attempts |

## 15. Physical layout, engine state and crash safety

Everything durable is an immutable object on object storage. The engine's journal is the only thing that says which objects count.

### Layout

```
journal/<segment>                      engine events, appended with create-if-absent
checkpoints/<n>                        snapshots of the engine's state
attempts/<attempt id>/manifest         the paths this attempt intends to write
attempts/<attempt id>/entries.parquet  the commit's layer file, used in place after commit
outputs/<output id>/layers/<file>      merged layers and the base
edges/<edge id>/points/<file>          points, only when too many for memory
consumers/<partition>/failing/<file>   the failing-key index
history/<table>/<day>/<file>           the history tables
```

Nothing is renamed or moved at commit. A committed attempt's layer file stays where the worker wrote it, and the journal event points at it.

### What the engine holds in memory

| Per | State |
| --- | --- |
| Output | Head, layer list, clear commits, live key count, recent commit headers, cleanup cursor |
| Edge | Pieces, points or a reference to them, counters, sequence number |
| Consumer partition | Conditions now, failing-key summary, the running task |
| Running attempt | Start time and spec |

All of it is bounded by what exists now. Nothing grows with the number of past commits or runs.

### The main journal events

| Event | Carries |
| --- | --- |
| Batch commit | The output commit header and its layer file. Per edge: the batch record. Also any reset, any source reports, and the outcomes file. |
| Reset | The edge, and the value: `empty` or `held` |
| Source report | A commit header and its entries, when a loader reports outside a batch |
| Merge | Layers removed and layers added |
| Attempt started or closed | The spec, or the outcome |
| Conditions changed | The new ids |
| Cleanup advanced | The cursor |
| History exported | The journal position reached |

### Crash cases

| What crashes, and when | What happens |
| --- | --- |
| A worker, before asking to commit | Nothing was appended. The attempt times out and the files in its manifest are deleted. The keys stay owed. |
| A worker, after asking but before hearing back | It asks again with the same attempt id. The engine answers from the journal: committed, or not. |
| The engine, before appending | Nothing happened. The worker's retry is handled as new. |
| The engine, after appending but before replying | Recovery replays the journal and the commit stands. The worker's retry is told "committed". |
| The engine, during a merge | The merge event was never appended. The half-written layers are orphans and are deleted. |
| The engine, during a history export | The export resumes from the last recorded journal position. File names derive from positions, so nothing is written twice. |
| The engine restarts in the middle of a run | The run continues. Its position, the last committed batch and its end key, is rebuilt from its commit events. |

### The one hard case: outputs that are not on object storage

A Postgres output cannot be committed in the same step as a journal append. The adapter must stage its rows under the attempt id, let the journal append decide, and then publish. Recovery completes whichever half is missing: staged rows with no journal event are discarded, and a journal event whose rows are still staged is published. This is the part of the design that depends most on each store, and section 17 lists it as open.

## 16. Edge cases and how each resolves

Every case below resolves through the mechanisms already described, with no rule of its own.

### Upstream changes

| Case | How it resolves |
| --- | --- |
| A key changes under a range the run has already passed | The piece keeps its older commit, so the key is owed after the run. Nothing is recorded for it. |
| A key changes and changes back, 1 → 2 → 1 | The diff sees equal ends and the key is not owed. Staleness may raise a false alarm, which an empty batch clears. |
| A key is added and removed before the consumer runs | The consumer never sees it. |
| Upstream commits faster than a run can finish | Each run sweeps the key space once and ends. Changes behind its cursor wait for the next run. |
| One commit changes 100M keys | The worker writes the entries as layer files. The journal event carries a reference. Consumers plan in batches as usual. |

### Runs

| Case | How it resolves |
| --- | --- |
| An incremental run is canceled | Pieces behind the cursor are at new commits, the rest at old ones. The consumer is stale, not incomplete. |
| A full run is canceled | The unreached range is `empty` or `held`. The consumer is incomplete, or everything there is still owed. |
| The first batch of a full run fails | The clear and the reset were part of that commit, so they never happened. |
| Two runs touch the same consumer partition | The second commit fails the sequence-number check and is replanned. |
| An attempt runs longer than T | Its commit is rejected. The retry plans at the new head. |
| A batch finds nothing owed | It commits an empty batch for its range, with no output commit. Pieces move to the head and merge. |
| An "all" run meets a key that upstream removed | The key is owed, so it is taken as removed along with the present keys. |
| Two inputs of a fan-in batch share a key | The batch never ends between them, because N counts distinct keys. |

### Conditions

| Case | How it resolves |
| --- | --- |
| Patterns change during a run | Later batches plan under the new patterns. Earlier pieces keep the old ones and are owed where the two differ. |
| The context moves during a run | Earlier ranges are owed again afterwards. The consumer converges once the context holds still for one run. |
| The definition changes during a run | The next commit is rejected and the run ends. Everything is owed under the new definition. |
| A key-list result predates a context change | Its point carries the old context, so the key is owed again. |
| A key fails while only the context changed | Its version is unchanged, but its point keeps the old context, so it stays owed. |

### Per-key assets

| Case | How it resolves |
| --- | --- |
| A key fails forever | Its point holds an explicit version, so the index keeps no history for it. Other keys proceed. |
| A failed key's upstream returns to the version the consumer holds | The failure is moot. The next batch covering the key drops the point and the failure entry. |
| A key that never succeeded is removed upstream | Nothing is owed, because the consumer holds nothing. The failure entry is dropped. |
| A key classed "unchanged" fails in an "all" run | Nothing is owed and no point is left. The failure entry stays, and the key is retried when due. |
| A key fails during a full run of a key-preserving asset | Its point is `held`: the old result stands, at an unknown version. The key stays owed. |
| Every key fails | The circuit breaker fails the batch as one fact. Without it, points and entries spill to files. |

### Reads and sources

| Case | How it resolves |
| --- | --- |
| A source serves a newer version than its log shows | The batch processes what it read, leaves a point, and reports the version to the source log. |
| A key vanishes between planning and reading | It is processed as removed, or dropped if it was to be added. A point records "absent". |
| Two consumers report different versions of one key | The first report is applied. The second finds an entry after its plan head and is dropped. That consumer's point then disagrees with the log, so it is owed the key. |
| A store goes 2 → 3 → 2 around a batch's read of 3 | The report of 3 is dropped, because the log has entries after the plan head. The key is owed, and the next run reads 2. |
| A rename in a store that keeps no old versions | One batch sees the removal, the next sees the addition. The output is briefly missing the key. |

### Resets and identity

| Case | How it resolves |
| --- | --- |
| An upstream full run is in progress | Downstream shows "upstream incomplete" and by default does not run. |
| Downstream after an upstream full run | One diff across the clear, done as two scans. Rebuilt keys have new versions and are updated. Keys not rebuilt are removed. |
| An upstream output is deleted and recreated | It has a new id, and the engine resets the edge. A key-preserving asset goes to `held`. Any other is flagged "full run required". |
| A consumer was paused for months | Its commit stays preserved. The cost is at most one entry per key that changed. |
| A consumer paused since before a clear | The engine's list of clear commits tells the planner its window holds one. The diff is done as two scans. |
| A new consumer joins an old output | It starts with one `empty` piece and scans the current state. It needs no history. |

### Retention and infrastructure

| Case | How it resolves |
| --- | --- |
| Lineage is asked for a batch whose starting commit is below the horizon | The commit is preserved, because the retained record names it. |
| A merge replaces a layer a worker is reading | The old layer is not deleted before T. |
| A recomputed key list does not match its digest | The query returns an error naming the batch. |
| A worker writes files and dies | Closing the attempt deletes the files in its manifest. |

## 17. Decisions and trade-offs for your review

These eighteen decisions define the design. Each row names what was chosen, what it was chosen over, why, and what it costs. The last column is yours.

| Decision | Instead of | Why | What it costs | Your call |
| --- | --- | --- | --- | --- |
| **D1.** Two logs; the input log refers to the output log | Copying each batch's keys into the input log | About 2.5 MB against 1.6 GB per full run. Planning and history share one function. | History is recomputed, and depends on the index staying exact at the commits records name | Agree |
| **D2.** Entries carry the replaced version | Storing only the new version | Diffs need only their own window. Cleanup has its list. "Unchanged" is exact. | The writer must know what each key it writes replaces. The plan or the store often says; otherwise it is an index read, at worst one small GET per key |  |
| **D3.** One change index for the hot path and history | A compact head-only index plus a separate archive | Nothing is stored twice. Planner and history cannot disagree. | A reader far behind reads every version of a key that changed often, not only the latest |  |
| **D4.** Layers are Parquet files | A custom block format | DuckDB reads the same files. Standard tooling. | A heavier reader in workers; less control over block layout | Change |
| **D5.** Merging by a size ratio | Fixed fan-in tiers, or levelled compaction | A stated read bound from any starting commit | About 5 bytes rewritten per byte committed, in a small simulation | Discuss |
| **D6.** Pieces keep their own commits; only batch commits and resets change progress | Moving older pieces to the newest head at each commit and recording the changed keys as points | A commit needs no index read. Progress is a pure replay of the input log. | Up to one piece per batch until the next run |  |
| **D7.** Points carry explicit versions | Points that refer to a commit | A stuck key preserves no history | Checking a point needs a lookup of the key's state now |  |
| **D8.** Workers plan; the engine validates | The engine planning | Engine time and memory do not depend on key counts | The planner must be deterministic, and its version recorded with each batch |  |
| **D9.** Each attempt plans at its own head | Holding one head across all attempts of a batch | T covers one attempt. No state between attempts. | A retry may see different keys from the failed attempt |  |
| **D10.** Batch records carry the replaced slice and a digest | Rebuilding a batch's starting state by replaying earlier records | Each record stands alone, so any run can be pruned | A few more bytes per record |  |
| **D11.** Conditions are compared, never rewritten; context is read per batch | Rewriting progress on a change; fixing context for a whole run | Nothing to write on a change. Safe under cancel. | A context that keeps moving makes earlier ranges owed again |  |
| **D12.** A reset is a `clear` commit in the same log | A new incarnation with fresh numbering; or building aside and swapping | Downstream keeps its progress. No cascade of full runs. | The output is partial during a rebuild. A diff across a clear costs two scans. |  |
| **D13.** `held` is the fallback when progress is unknown | Always resetting | Key-preserving assets rebuild without emptying their output | A third piece value and one more planner branch |  |
| **D14.** Progress records success only | Recording a failed key as seen | Exact retry classes and removals. Progress always describes the output. | Two records per failing key |  |
| **D15.** A grace period, with no reader tracking | Tracking the head each running reader holds | No registry and no stale holds. One constant per output. | Storage for T's worth of replaced files. Attempts are capped at T. |  |
| **D16.** Stores that move on record what was read | Refusing such stores, or failing the batch on any mismatch | Works with any store and corrects itself | No consistent view across keys for those stores |  |
| **D17.** Per-key history is computed; only failures are stored per key | Storing every key of every batch | Storage follows changes, not batch sizes | No duration per ok key by default |  |
| **D18.** A horizon plus preserved commits; lineage is kept exactly as long as its run | A single cut; or a shorter clock for lineage | Paused consumers are cheap. No run is "retained but not exact". | A more involved merge rule. It can follow a single-cut first version. |  |

### Open questions

These are choices the design leaves to you.

- [ ] **Rejected keys (D14).** A rejected key stays owed and held back, and does not count as processed. Is that the meaning you want?
- [ ] **Key-preserving assets (D13).** The design assumes they have exactly one keyed input. Is that true of your per-key assets?
- [ ] **Incomplete upstream.** Should it block downstream runs by default, or only warn?
- [ ] **Context (D11).** Read per batch, or fixed for a whole run? Per run needs the grace period to cover the run.
- [ ] **Circuit breaker.** What share of failing keys fails the whole batch, and is it on by default?
- [ ] **Duration per ok key (D17).** Is it a promised fact? If so it must be stored, at 100M rows per full run.
- [ ] **The default T.** And do long reads of whole inputs need their own?
- [ ] **Unkeyed outputs.** How long do they keep their appended data? A consumer's full run can only re-read what is still there.
- [ ] **Retention staging (D18).** Start with the single cut, or build preserved commits from the start?
- [ ] **Outputs not on object storage.** Is "stage, then publish" an acceptable requirement on store adapters?
- [ ] **Full runs of ordinary assets (D12).** Building aside and swapping would avoid a partial output. It needs double storage for a while and changes what Q3 means. Worth considering?

## 18. Verification plan

The design is small enough to check against a brute-force model, and its core has been checked that way once already.

### Done for this brief

A reference model of about 300 lines of Python implements the design's own structures and algorithms: entries, diff and scan computed from entries alone, clears, the merge rule, pieces and points, conditions, `held`, all four run modes, per-key failures, and reads that return another version. Beside it sits a brute-force oracle: the full state after every commit, and an explicit per-key record of what the consumer holds.

A random test ran 3,000 trials, about 121,000 checked steps. After every step it asserted three things.

1. Decoding progress equals the oracle's per-key record.
2. The planner's candidates, built from diffs and scans, equal the owed keys found by brute force, with the same classes, in incremental and in "all" mode.
3. A scan computed from entries equals the true state at every commit, clears included.

Once per trial it asserted two more.

4. A diff computed from entries equals the difference of the two true states, for every pair of commits.
5. After the merge rule is applied for a random set of preserved commits, every scan at a preserved commit and every diff between two of them is unchanged.

All passed. The model also prints the examples of sections 4, 5, 7, 8 and 9. It does not cover the grace period, crash recovery, fan-in, the choice of horizon, layer files, or the engine's counters.

A separate adversarial review of the draft found gaps around clears, conditions on points, `held` and "all" mode. They are now specified in this brief and covered by the model. The file is attached to the conversation as `model.py`, as a seed for the tests below.

### Property tests for the implementation

Use a tiny universe: 5 to 8 keys, 3 versions, 2 outputs, 2 consumers. Reverts and collisions then happen constantly. Generate random interleavings of:

- commits, reverts and clears;
- batch start, commit, failure and cancel, with per-key failures;
- reads that return another version or nothing;
- pattern, context and definition changes, in all four run modes;
- merges, pruning and cleanup;
- an engine crash and recovery at any point.

After each step, check these invariants against the model.

| Invariant | Protects |
| --- | --- |
| Decoding equals the model's per-key record | Progress |
| Owed keys and classes equal the model's | Staleness and planning |
| Completeness equals the model's | Q3 |
| Each retained batch's recomputed input equals what the model recorded, and its digest | Lineage |
| Merging never changes a diff between preserved commits | Retention |
| Cleanup never deletes a file that an attempt still able to commit could read | The grace period |
| State after recovery equals state before the crash | Crash safety |
| Entries chain for every key | The output log |

### Model checking of the protocols

A small TLA+ or P model should cover interleavings, which property tests reach poorly.

- A worker writes files and asks to commit while the engine appends, with a crash or a retry at every point.
- An attempt starts, a commit replaces its files, deletion comes due, and the attempt's commit is accepted or rejected.
- A merge swaps layers while workers read them.
- The journal is exported to history exactly once.
- A store that is not object storage stages, and the journal decides.

A deterministic simulation with an object store that injects latency, errors and duplicate requests covers the rest. Create-if-absent should be tested on each provider, not assumed.

### Benchmarks

| Benchmark | What to watch |
| --- | --- |
| Planning at a lag of 1 second, 1 day and 30 days, on 100M keys | GETs and time, against the estimates in section 4 |
| A 100% rewrite, end to end | Bytes rewritten per byte committed; layer count |
| 50 small commits a minute | PUTs per commit on the inline path |
| A tick over 100,000 edges | Milliseconds |
| 50 consumers at mixed lags on one output | No cost per consumer beyond counters |
| A full run racing upstream commits | Piece count, and its collapse on the next run |
| One million failing keys | Point spill and planning cost |
| A pattern change on 100M keys | Scan cost |
| Lineage of a 60-day-old batch; a five-hop key trace | GETs and seconds |
| Recovery from a checkpoint | Seconds |

## 19. Requirements map

Each of the ten questions is answered by one mechanism.

| Question | Answered by | Section |
| --- | --- | --- |
| Q1 Staleness | The per-edge summary: pieces against the head and the conditions now. The reason is the part that differs. The per-key list is the planner without a limit. | 12, 7 |
| Q2 Planning | Diffs and scans over the pieces after the cursor, first N, classified | 6 |
| Q3 Completeness | An `empty` piece that holds an upstream key | 12, 5 |
| Q4 Key outcomes | The failing-key index; everything else is derived from progress and upstream | 9 |
| Q5 Lag | Pending counters as bounds; the planner in count mode for exact figures | 12 |
| Q6 Batch lineage | The batch record plus a diff, checked by its digest; the output commit's entries | 13, 6 |
| Q7 Key trace | `history(key)`, alternating with the batch records whose range holds the key | 13 |
| Q8 Flow metrics | Counts stored in batch records and commit headers | 13 |
| Q9 Cleanup | The replaced version in each entry; files deleted after T | 14, 10 |
| Q10 Retention | History pruned by day; the horizon advances; merges combine and fold | 14 |

### The principles

| Principle | How the design meets it |
| --- | --- |
| Store facts; derive the rest | The facts are entries, batch records, failures and source reports. Progress, counters, outcomes and lineage are all derived. |
| Exactness where promised; honesty where information is lost | Digests on lineage, preserved commits, and explicit "not retained" answers |
| Grouping over individuals | Pieces stand for ranges, a `clear` for a whole output, a batch record for all its keys; only failures are per key |
| Simplicity | One query, one commit rule, one grace constant per output, and the same planner for work and for history |
