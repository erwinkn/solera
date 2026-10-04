# A25 independent review of the two-view design

## Review scope

Reviewed the Claude phase 1 contributions on `exp/key-index-two-views`: commits `25bddc7`, `4bda5b3`, and `dcfc069`. The exact reviewed revision is **dcfc0692be17731c8586a48e44fa251e6fa0828d**. Primary files are `docs/key-index-two-views-design.md`, `bench/keys/views/handoff.md`, the structural replay, glob prototype, and codec experiment with their committed outputs. Source locations below refer to that revision.

The review workspace is `/home/exedev/.bb/plugins/environment-git-worktree/host-data/worktrees/thr_545hz78wrc-1/data-orchestrator`, at HEAD **1c6f9ef18167fbe1eb27948bac00386ecfaf09fb**. I read the experiment through immutable Git objects without switching branches or copying source into the checkout. During review, the shared experiment ref advanced to **07c67c743e065eae2ba7fdda256e275dbb4da8f7**, adding a main merge and native prototype work. The reviewed design, glob helper and replay are unchanged there; the new native prototype is outside this review. The workspace remained clean.

Read D109-D116, A17's `review/a17/handoff.md` at `810c537a07cdb965cada15f2c0130ca6c9b18545`, A19's `/tmp/solera-a19-review/handoff.md`, and relevant existing index, lifecycle, pattern and position contracts. No source, dependency, install or branch changes were made. Review artifacts live in `/tmp/solera-a25-review`.

## Verdict

**Sound with listed fixes. Needs attention before accepting the design.** The aligned net-change tree and base-plus-cover read model are viable. The submitted rules have concrete failures in glob pruning, read-ahead ordering and file identity across index lives. The proposed bounded-retention extension loses active pass endpoints, while the floor-only version retains unbounded history. Two replay assumptions also understate the proposed design's costs.

This is a phase 1 verdict, not a recommendation to switch from spans. D116's switch rule needs phase 2 measurements on the corrected design. A finite stalled-reader trace cannot establish its required storage bound.

## Findings

### R1 [P1] Make interval pruning accept matches between variable-length bounds

Location: `bench/keys/views/globs.py:130` and `:138`; design `docs/key-index-two-views-design.md:381`.

A commit writes keys `aa`, `b`, and `ca`. A block or candidate interval covers `aa` through `ca`, and a reader asks for glob `?`. The committed `intersects(tokens('?'), 'aa', 'ca')` returns false although `b` matches. The proposed reader skips a block containing a result. The walk does not accept a completed match while it is below a longer upper bound, and its generic-character representative can stay tight on a boundary instead of exploring an interior character. Even the singleton match `a` is rejected for bounds `a` and `ab`. **Fix:** make end-of-string an accepting option whenever it satisfies both bounds, and distinguish lower-bound, upper-bound and strictly interior character transitions. Check pruning against the real matcher on short variable-length keys, not only the fixed-shape benchmark datasets. Two executable counterexamples are in `probes.log`.

### R2 [P1] Preserve Solera's terminal double-star semantics

Location: `bench/keys/views/globs.py:32-37`.

A block contains `tenant/a/x` through `tenant/a/z`; the reader's include is `tenant/**`. Solera's matcher includes `tenant/a/y`, but the prototype tokenizes `**` as two single stars, neither of which can cross `/`. Its matcher and interval predicate both reject the key/block. The measurement checks use that same incorrect matcher, so their assertions cannot catch this mismatch. **Fix:** add the unrestricted `**` transition used by `python/solera/patterns.py:glob_regex`, and validate against Solera's matcher or its independent reference. `**/` alone is not the complete glob grammar. This is independently reproduced for both `tenant/**` and `**`.

### R3 [P1] Suppress read-ahead newer than a resumed pass's endpoint

Location: `docs/key-index-two-views-design.md:181-186`.

A consumer pauses a delta pass ending at `N=4`, before reaching key `d`. Commit 5 adds `d`, and a `keys=[d]` selection delivers it at `r=5`. The old pass then resumes. K at 4 correctly reports `d` absent, but the table's live-to-absent row emits a removal, undoing the newer selection. The inverse history also fails: a selection removes `d` at 5 while K at 4 still has its older live version; the removed-to-live row emits an addition. The existing contract permits selections during an incomplete pass and suppresses any change already read at a newer generation. **Fix:** skip read-ahead keys whose delivered observation is at or after `N`, before classing presence against K at N. Preserve this ordering when absence no longer has a retained tombstone generation. Include both histories and multi-page cursor behavior in the oracle tests. The probes exercise the written table, not an implemented two-view engine.

### R4 [P1] Put index life in deterministic object identities and publication checks

Location: `docs/key-index-two-views-design.md:240`, `:485-488`, and replacement state at `:570`.

Engine epoch 7 builds `keys/feed/_/t1-000000000000-e7.kx` for commits 0-3. The old index is retired, and its collector passes the journal barrier but pauses before DELETE. The same engine resets/recreates `feed`, whose commit numbers restart, and publishes its different new-life pack for commits 0-3 under the same name. The old collector resumes and deletes the live pack. An old in-flight PUT completing after the new upload can overwrite it as well. Determinism only holds for the same children; reset changes those children without changing the epoch. The current product prefix is based on output/partition and may survive a rename, while the existing `IndexState.life` and input-identity publication checks prevent cross-life publication. The proposed state replacement omits life. **Fix:** retain a durable index-life identity in names, manifests and build/publication records; never reuse a name a retired build or deletion can still target. Specify reset and rename behavior, and retain exact input/base-watermark validation before publication. A journal barrier alone does not make a reusable name permanently dead. The probe demonstrates the namespace/deletion collision under the written naming rule, not a live-store race.

### R5 [P1] Finish a bounded retention policy that preserves active upper endpoints

Location: `docs/key-index-two-views-design.md:617-623`; floor/pin rules at `:204-209` and `:340-345`.

A full pass stalls at commit 2,000 while the writer repeatedly updates the same fixed set of keys. The floor remains pinned, so packs alone retain a new copy of every commit forever, even when high-level nodes collapse to one entry per key. The proposed cure is also incomplete: let the oldest reader start at 4 with a paused pass ending at 7, another reader start at 16, and K's pinned base be at 3. Compaction of 4-15 into one net node discards the boundary at 8. Two histories that add `k=v1` or `k=v3` before 7 and both update it to `v2` at 9 produce the same compacted node, but require different K-at-7 and changes(4,7) results. Keeping the subtree to answer that old endpoint restores the retention problem. **Fix:** choose and specify one bounded policy before using the simplicity/storage claims. Position-based summaries must preserve all active query starts and ends, base watermarks, pins and in-flight readers, with a durable transition/deletion protocol. Alternatively define a physical budget and durable expiry/cancel-restart policy, including stalled full-pass pins. Measuring the floor-only structure for 12,000 commits does not resolve this choice. The note acknowledges unbounded retention, but its proposed extension is not yet a correct bounded design.

### R6 [P2] Keep upload-attempt accounting mandatory across takeovers

Location: `docs/key-index-two-views-design.md:571` and `:592-594`.

An engine uploads a completed level-2 node for commits 0-15 and crashes before its built-through record is durable. Each successor repeats that upload, potentially under a new epoch. After any number of restarts the same logical node still has not advanced the published level, but uploaded bytes and charges have grown with every attempt. Having a fixed number of levels does not bound this work. The reuse table deletes `MERGE_ATTEMPTS` per input set, and the A17 checklist makes a durable counter conditional while claiming T's writes remain bounded without it. **Fix:** carry D109 forward unconditionally for both node builds and base merges: reserve and flush attempts before upload, key them by life and durable input/build identity, and preserve exhaustion and alarms across takeover. Count failed and abandoned bytes in costs. Rejecting unproductive span rewrites disappears; crash accounting does not.

### R7 [P2] Remove alignment bias from the key-view run count

Location: `bench/keys/views/model.py:103-107`; reported result at `docs/key-index-two-views-design.md:321-323`.

At 100M keys, b=4 and r=4, the base cycle has 65,536 commits. Sampling it every 32 commits fixes the low digits of the head position, systematically missing populated low levels. The committed model reports 10.5 runs on average and 19 maximum. Enumerating all 65,536 heads with the same `Shape.chain` gives **13.000015 on average and 25 maximum**, with the maximum at relative head 65,534. A cold writer or reader at that ordinary point must open the omitted runs. **Fix:** enumerate the inexpensive full cycle, derive digit counts analytically, or use unbiased sampling with separately proved maxima. Count pack sections as merge inputs separately from object requests. Regenerate the tables before they influence the fanout comparison or memory/GET expectations. These are counts in the submitted analytical model, not measured reader latency.

### R8 [P2] Include the base watermark in physical T storage

Location: `bench/keys/views/model.py:119-121`; design `:225-235` and `:504-509`.

With 100M live keys, a base at commit 65,535 and the head at 131,070, the next level-8 node is one commit from completion. A daily reader is at 122,431, but the actual floor is still 65,536 because K's base needs all later commits. The retention rule keeps every complete descendant above that floor, not just the reader's last day or K's current cover. Applying the submitted density model to those complete nodes gives **431.4M retained entries**, excluding the final unpacked deltas, versus the day-only table's **51.6M**. At the midpoint the count is already 225.9M. **Fix:** compute physical storage over the base cycle using the minimum of consumer positions and all active/current base watermarks, plus base copies, unpacked deltas, cleanup holds and orphan outputs. Distinguish logical read-chain entries from retained objects. If the intent is to delete off-chain descendants, specify how arbitrary new readers and pinned endpoints remain readable. The sentence that no consumers means T holds only K's chain contradicts the stated floor deletion rule.

## Fix queue

1. Correct interval pruning and the full glob grammar; check against the independent matcher.
2. Specify read-ahead behavior for `r >= N`, with both live and removed selections during a paused pass.
3. Preserve life and immutable build identity through naming, publication and collection; add paused PUT/DELETE reset and rename histories.
4. Choose a bounded retention policy and enumerate every boundary it protects. Validate paused pass ends before measuring it.
5. Preserve durable pre-upload attempt accounting and admission for every index writer.
6. Correct run sampling and physical storage accounting, then regenerate the design tables.
7. Run phase 2 on the corrected design and matched span configurations under D116. Include cold isolated readers, an actual stalled pin, churn, uneven/1M-key commits, sparse/empty commits, crashes and takeovers.

## Prior reviews walked as a checklist

| A17 | Assessment of this design |
|---|---|
| R1 zombie orphan deletion | Durable barrier plus monotone floor is a reasonable rule within one immutable life. R4 shows why life-scoped non-reused paths remain necessary. Current-epoch in-flight builds must remain protected too. |
| R2 page boundary through one key | A merged node/base holds one row per key, removing the within-run version-chain case. Packs still contain up to four sections with the same key; the reader must merge all section inputs before advancing its cursor. Reuse the streaming reader, not the older windowed reader mentioned at line 331. |
| R3 cached first-file selection | Disjoint key ranges per merged run remove the historical duplicate-file case. Verify pack section handling and newest-run order in phase 2. |
| R4 positionless selection | Index endpoint transfer can disappear, but attempt pins and engine-side delivery accounting remain. |
| R5 covering retry landing | A floor retaining arbitrary starts solves this while it remains in force. R5 in this review shows the bounded extension needs additional protected boundaries. |
| R6 all-writer admission | Correctly retained as an obligation; no actual backlog threshold or enforcement exists in phase 1. Include source and failure indexes. |
| R7 rejected rewrites | The discretionary no-op rewrite is gone. Failed publication after uploads still needs accounting. |
| R8 restart budget | Not discharged. See R6 above and D109. |
| R9 maximum generation bound | Removing generation clipping avoids that exact sentinel bug. Commit/epoch encoding still needs explicit bounds. |
| R10 hot-key memory | Four-way net merges avoid collecting an arbitrary number of versions inside one node. Pack sections, read-cover fan-in, large payloads and backlog admission still set memory use. The 1M-key commit needs a streaming measurement. |

| A19 | Assessment of this design |
|---|---|
| R1-R2 moving full pass | A pinned K manifest can give the required stable pass, provided retention preserves it. |
| R3 removal owed | K at N can establish absence, but the read-ahead ordering defect in R3 above must be fixed. Completion must still preserve/deliver the debt in the engine. |
| R4 pattern selections | K at the split plus recorded selections is sufficient in principle. A selected delivery newer than a paused split also needs the ordering rule. |
| R5 current-only early removal | D100 can keep classes tied to the pinned index while rows follow the store. No new permission to reclassify a missing old row is implied. |
| R6 selection bound | Engine work under D111/D115; selections must paginate and honor concurrency. The index does not implement this. |
| R7 definition/input binding | Engine identity/restart policy, not an index guarantee. |
| R8 retry after revert | Keeping the live after-generation is correct. The engine must still avoid assuming a newer generation guarantees a delivered net delta. |
| R9-R10 staleness | Early-stop T reads help, but each-key membership/roll-up logic remains in the engine. Pruning must not introduce the false negatives in R1/R2. |

## From-scratch choices, reuse and simplicity

The codec experiment compares zlib, zstd, lz4 and no compression; it also compares row/column layouts and three block sizes. That is real evidence for reconsidering v4's internal choices. Fanout is compared at 2/4/8/16, and separate K structures are compared against the shared tree. I found no need to reject the basic container or row layout merely because they already exist. Fix the cost model before treating its selected fanout/base level as established.

Several qualifications matter for the phase 1 acceptance criterion that every choice has a measured alternative:

- The 64 MiB base-file ceiling is carried over without a file-size sweep or a quantitative alternative. The no-base-filter argument assumes whole-filter reads; a from-scratch comparison should at least cost selective reads of the already-blocked filter. Those are incomplete justifications, not demonstrated reasons to choose a different layout.
- Prefix/glob pruning needs the correctness repairs above before its block-read measurements are usable. The prose also overstates the table: trigram filters slightly beat interval pruning in a few listed cases, and `**/report-0004?.*` at 16 KiB reads about 1.86 times the blocks with matches. Those observations do not by themselves justify storing filters.
- The row-layout justification should stand on decode cost and the key distribution; "not worth a second layout" is not an independent reason to preserve the old one. The committed measurements do offer an independent argument for a single row layout, so this is a wording/decision-rationale qualification rather than a blocker.

Counting the named structural concepts explicitly: the proposed design has a commit delta, a pack, a merged net node, an aligned cover, a base watermark, a retention floor and a pinned K manifest, **seven**, before adding its bounded-retention mechanism. Spans has a commit delta, versioned span/segments, reserved endpoints, a base and pinned manifests, **five** at this grouping level. This is a vocabulary count, not a complexity score. The two-view version removes substantial version-fold and merge-policy code, while adding cover construction, packs, progress/epoch/skip metadata and a retention policy that is not finished.

Both designs still have **two merge/build lanes plus collection**. Two views calls them tree builds and base merges; it does not eliminate background work. Its extra stalled-reader compaction must be scheduled in one of those lanes or accounted as another. The shared obligations also remain: exact predecessors; complete uploads before publication; durable publication before deleting inputs; life/epoch fencing; atomic pin acquisition; protection of active builds; durable attempt accounting; and admission under backlog. The title-level "no versions" claim applies to merged runs. A pack can physically contain the same key in four sections.

The simplification is promising, especially in each merge's per-key state. It is not yet established for the whole lifecycle. Prefer it under D116 if corrected measurements and the final retention design meet that rule; do not require a new 1.5-times speedup merely because the obsolete proposal at design lines 675-679 says so.

## Verification and limits

- [Probes](/tmp/solera-a25-review/probes.py) load the exact committed `globs.py`, `model.py` and product pattern matcher into memory. [Output](/tmp/solera-a25-review/probes.log) records four false-negative block-pruning cases and the corrected model counts. Runtime was about 2.4 seconds, one Python process.
- **122,472 algebra checks passed** over all 2,187 seven-commit one-key histories using absent/a/b states, all subranges and both leaf/grouped covers. These cover empty commits, net additions/removals/updates, equal-payload reverts, presence and live generations/payloads. They support the ordinary algebra, not the lifecycle or complete file reader.
- Read-ahead, index-life naming and position-compaction examples are bounded models of explicit design rules. No native two-view implementation, real S3 operation or scheduler interleaving was executed for them.
- Deleted-generation identity is deliberately excluded from the algebra pass. Dropping an absent-to-absent node can hide a later tombstone generation: remove at 1, add at 4, remove at 5 can leave the cover's removal naming 1 after node 4-7 is dropped. Phase 2 must define whether deleted generations are observable and either preserve them or normalize absence consistently in the oracle/API. Do not label this probe as an exact check of every returned generation.
- Codec source and committed results were inspected; codec timings were not rerun. No 1M/100M real-file benchmark, full suite, native build, Postgres matrix, Mac gate, object-store crash test, memory stress run or price re-verification was performed. D116's accepted price profile is used only as context; no new dollar estimate is claimed.
- The experiment contributes only documentation and benchmark code at this revision. Product work on main and any subsequent prototype commits are outside this review.

## Constraints and preferences

Read-only A25. Keep implementation and final retention-policy choices with the coordinator. Use D116 for the later decision; D111/D115 establish 10,000-key commit batching with separate per-key concurrency, even though the comparison trace deliberately uses 1K-key commits. Keep that distinction explicit when interpreting workload results.

## Human reviewer callouts

- **This change introduces a new dependency:** the isolated codec benchmark uses zstd and lz4 libraries; no product dependency or installation was changed by this review.
- **This change introduces backwards-incompatible public schema/API/contract changes:** the design proposes v5 files and replacement index metadata. Neither is product code at this phase.
- **This change includes irreversible or destructive operations:** the proposed collector deletes packs, nodes and bases. R4/R5 require correction before implementing those rules.

## Bounded handoff

A25 review is complete at `dcfc0692be17731c8586a48e44fa251e6fa0828d`. Coordinator action: route R1-R8 to W53, settle the bounded-retention design, then request the corrected phase 2 evidence and targeted lifecycle/reader regressions. The early coordinator message covered glob and read-ahead blockers; this is the canonical complete result.

No source or install changes, commits, pushes, pending commands or background work. No fixes were delegated by this reviewer. All probes are synchronous and finished. Workspace and verification revisions differ intentionally because the experiment was reviewed via Git objects. Review choices and alternatives are in [review-choices.md](/tmp/solera-a25-review/review-choices.md), recorded separately through Initiative as D117 and D118.
