# A17 independent review of Solera's key index

## Review scope

Reviewed the Claude integration contributions in steps 1–2 and the net rule: `8bc7432`, `0d09fc4`, `b49bbef..78a7c5e`, and `eef4f33`. The source checked is **eef4f335f64ec0565c47f05d3159c917cac9cda0**, in `/home/exedev/.bb/plugins/environment-git-worktree/host-data/worktrees/thr_di2vgmzppa-1/data-orchestrator`. The checkout stayed clean. No source, dependencies, branches or install state were changed.

During review, the shared `origin/main` ref advanced to `c8db0c3e30d71bdc27a509bf78f69eccd0690434`. That commit adds the TLA model and verification documentation only; the product code reviewed remains eef4f33. I did not review the new TLA model.

Read the approved design and D65, D69–D76, and A15's report. Reviewed native exact writes, v4 encoding, spans, streaming and local reads; Python page selection, merge policy and index transitions; upkeep, publication, collection, source admission and engine endpoint reservations. The cost document's measurements were excluded as requested.

## Verdict

**Don't trust as-is. Needs attention.** Three P1 findings affect stored data, reads or exact writes. Seven P2 findings affect supported selections, endpoint preservation or stated resource bounds. The core presence/payload algebra looks sound in the focused checks, but its surrounding readers and lifecycle do not yet uphold the design.

## Findings

### R1 — [P1] Fence orphan deletion after listing

Location: `python/solera_server/upkeep.py:262`, especially deletion at line 274.

Engine A starts `collect_orphans` and stalls while listing `keys/`. Engine B opens the same CAS-backed store, takes ownership, merges `[0,0]` and `[1,1]`, records `IndexMerged`, and flushes it durably. A resumes listing and sees B's new output. Its stale `named` and `running` sets contain neither that file nor B's merge, so it deletes a file B's current index references. A has not necessarily detected its fencing: an idle journal has no pending CAS write. This was reproduced using the actual `Journal`, `Model`, `State` and `Upkeep` on `MemoryStore`; the published file exists before A resumes and is gone afterward. **Fix:** put a successful authority/CAS barrier after the candidate listing and before deletion, with protection for all current references and publishing reservations. A durable GC intent is one option. Merely calling `durable()` with no newly recorded event is not an ownership check.

### R2 — [P1] Complete a key before advancing a cold page's block window

Location: `python/solera/keys/index.py:817`, with the analogous file boundary at line 802 and cursor handling at line 838.

Keep 20 versions of `k` at live endpoints and merge them into five small blocks. With `limit=1`, `_window` fetches two blocks and chooses `k` itself as the exclusive bound. No key can be returned. `page(None,1)` and `changes_page(1,19,None,1)` return empty results and `cursor=None`, although lookup and `changes(keys=[k])` return generation 20. Starting after `j` returns the same `j` cursor forever. **Fix:** fetch or stream enough of the boundary key to finish it, across files too, and guarantee that a nonterminal page advances. Do not solve this by collecting arbitrarily many versions in memory; R10 applies.

### R3 — [P1] Select the first file containing a key in cached lookups

Location: `native/src/local.rs:501`; the new repeated-key support in `block_of` at line 378 fixes block selection but leaves file selection unchanged.

A key's retained versions can cross files in one span. `Snapshot::get` chooses the last file whose minimum is `<= key`, which selects older versions when several files begin with that key. With 20 versions split into three files, cold lookup returns generation 20 and cached lookup returns generation 4. A cached resolve writing generation 21 then records predecessor 4 instead of 20, breaking exact cleanup; payload-bearing writes can also be suppressed against the wrong prior payload. **Fix:** find the earliest file that can contain the key's newest entry, including a preceding file whose maximum equals the key, and use the same first-version rule at file, block and restart boundaries.

### R4 — [P2] Allow selections that intentionally have no position

Location: `python/solera_server/positions.py:176`.

A fresh position with old patterns can receive a `keys=` request under changed patterns. `Engine._selection` deliberately returns `kind=selection, position=None` in that case: the selection does not advance the pattern-change drain. The new `reads` function includes every selection and dereferences `position['next']`, raising `TypeError: 'NoneType' object is not subscriptable` during preparation, before the worker runs. Reproduced by calling the actual `_selection` path and then `reads`. **Fix:** skip endpoint transfer for plans that cannot advance a position, while preserving the attempt's ordinary manifest pin.

### R5 — [P2] Reserve a covering retry's landing endpoint

Location: `python/solera_server/positions.py:174`; producer at `engine.py:1549`, transfer at `engine.py:1756`.

A per-key retry may have `kind=held` and `head=2`, and report that it covers all pending read-ahead. `commit_attempt` then advances its position to 3. `reads` excludes held plans, so while the retry runs the upstream can commit 3 and 4 and merge `[1,4]` without retaining endpoint 3. In the reproduction the only reserved endpoint is 1; after that merge `generation(3)` is `None` and `covers(3,4)` is false. The retry lands at a boundary the index discarded, forcing a full restart. Repetition under active writers defeats the intended bounded catch-up. **Fix:** reserve the landing point for every plan that may advance, including covering retries, and keep that reservation until transfer or failure.

### R6 — [P2] Apply backpressure to every index writer

Location: `python/solera_server/engine.py:608`.

The 64-span check examines an asset task's declared outputs only. `commit_source` bypasses dispatch, and the per-key failure index `@asset` is not a declared output. With maintenance unavailable, 70 calls to `commit_source('feed', upsert=['k'])` all succeed and leave 70 spans despite the configured threshold of 64. There is no later admission check to stop continued growth. A failing merge that permanently stops upkeep makes this an ordinary operational case. **Fix:** gate source commits and failure-index writes as well as ordinary outputs, using one admission rule with visible retry/held behavior. D71's 64-span proxy is a recorded deviation; even that proxy is not enforced for these paths.

### R7 — [P2] Stop re-uploading rejected single-span rewrites

Location: `python/solera_server/upkeep.py:173`; eligibility at `python/solera/keys/index.py:1279`, upload before rejection at line 1129.

Create a base of 1,000 entries and a tail span with two segments containing 100 different keys each. Retire the boundary between those segments. The policy counts the newer segment's 100 entries as dead, even though no key has another version and all 200 survive. The rewrite uploads all 200, rejects its output for dropping nothing, and deletes it. `_merge` clears the attempt count and caches only the entire index object's identity. Each subsequent one-key commit creates a new index object, making the same unchanged tail eligible again. Four consecutive cycles reproduced an output PUT each with no publication. **Fix:** count these attempts, remember an unsuccessful rewrite by its actual input files and endpoint set across unrelated commits, and do not upload an unguarded rewrite without establishing enough discardable versions. The proof charges uploads, not just published merges.

### R8 — [P2] Keep the merge-attempt budget across engine lifetimes

Location: `python/solera_server/upkeep.py:65` and line 125.

The three-attempt limit and stopped set are process memory. Fail the same input set three times, restart or take over, and it gets three more attempts. The focused check showed 3, then 6 attempts of identical inputs. Repeated crashes after upload have the same accounting hole and may never leave a visible persistent alarm. This contradicts the design's R=3 bound over failed and abandoned uploads. **Fix:** durably reserve/count attempts before starting work, keyed by index life and exact input identities. Preserve exhaustion over takeover, and keep old-life failures from stopping a new life at the same output name. Otherwise explicitly revise the resource guarantee and its proof assumptions; no recorded D71–D73 deviation covers this weakening.

### R9 — [P2] Distinguish an exclusive u64 maximum from no bound

Location: `native/src/spans.rs:268`; caller at `python/solera/keys/index.py:858`.

Commit 0 writes `k` at `u64::MAX-1`, commit 1 writes it at `u64::MAX`, and a merge keeps endpoint 1. A lookup/page at endpoint 1 must return the former version. `older` treats the valid exclusive bound `u64::MAX` as unbounded, returning the latter. `changes(0,0)` likewise reports the version from commit 1. All three were reproduced. **Fix:** represent no bound separately, such as `Option<u64>`, through the Python/native APIs and streaming readers. Keep the largest generation readable at the head without including it below an explicit equal bound.

### R10 — [P2] Stream a hot key's versions within the memory budget

Location: `native/src/spans.rs:120`, `native/src/jobs.rs:283`, and `native/src/local.rs:662`.

The fan-in limit does not bound versions of one key. With 1,000 reserved endpoints and one 1 MiB source-version payload at each, a single span can legally hold roughly 1 GiB for one key. `Groups::next_group` copies every payload into a `Vec<Version>` before classification or the page byte limit; `SpanMerge` accumulates the same group and `retain` clones its retained contents. A page of one key or one merge can therefore exceed the promised block/page budget by an arbitrary amount, even with one span. This is a source-level allocation proof; I did not run a GiB stress test on the shared server. **Fix:** process versions incrementally, retaining only the state needed for the requested endpoints/predecessor, and emit merge output as it becomes decidable. Enforce any remaining memory/retention limit before allocation. A 32- or 64-span cap alone cannot supply that bound.

## Fix queue

1. R1: make orphan deletion safe across CAS takeover; reproduce with a published and a still-publishing successor file.
2. R2 and R3: repair cold windows and warm file selection, then compare page/lookup/resolve across every key/block/file boundary.
3. R4 and R5: derive reservations from what a plan may transfer, including null-position selections and covering retries.
4. R6: enforce admission for sources and failure indexes.
5. R7 and R8: account every attempted upload and prevent rejected rewrites from repeating against unchanged inputs.
6. R9: separate explicit maximum generation from the head sentinel.
7. R10: stream versions of a single hot key; verify peak memory with a small isolated process and a low test budget.

## Verification and evidence

Bounded reproductions are in this directory:

- [reproduce.py](reproduce.py): R2, R4 and R9. Outputs include an empty terminal page for a live key, a repeating nonterminal cursor, the null-position exception, and the wrong maximum generation.
- [lifecycle.py](lifecycle.py): R1, R3 and R6. Uses the actual CAS journal lifecycle on `MemoryStore`; also shows cached predecessor 4 instead of 20 and 70 accepted source spans.
- [bounds.py](bounds.py): R5 and R8. Shows lost endpoint 3 and the same merge inputs receiving six attempts across two Upkeep lifetimes.
- [rejected_rewrites.py](rejected_rewrites.py): R7. Four uploads of the unchanged 200-entry tail, all rejected.
- [net.rs](net.rs): exact eef4f33 `Version`, `retain`, `change`, and `older` function bodies, extracted without modification. Compiled with `rustc` into this artifact directory. **49 checks passed** for absent/live endpoints, equal/different/empty payloads, predecessor-carried payloads, retained versions and payload-free outputs.

**Runtime qualification:** the reviewed checkout has no built extension. Python probes used its exact eef4f33 Python source and the existing binary at `/home/exedev/.bb/plugins/environment-git-worktree/host-data/worktrees/thr_9kc32wdrrt-1/data-orchestrator/python/solera/_native.abi3.so`, from a checkout reporting `76b27c1b6da22909a4746f62b5d904f49aafc539`. Binary SHA-256: `825aa27c30ae60c09311028d990772aa66212509cf29a0c9c81ccf31dba8fc2f`. The donor had live Python edits in sdk.py, each.py and worker.py; none was imported. Native reproduction paths are unchanged by eef4f33's net additions, confirmed by source inspection. I did not rebuild or attest the donor binary's provenance. Thus these are **bounded reproductions backed by source inspection**, not an exact-eef4f33 extension gate. The new net algebra was checked separately from exact source; its complete encode/decode pipeline was not executed here.

Run the Python artifacts with `PYTHONDONTWRITEBYTECODE=1` and the existing interpreter `/home/exedev/.bb/plugins/environment-git-worktree/host-data/worktrees/thr_9kc32wdrrt-1/data-orchestrator/.venv/bin/python`. `reproduce.py` preloads only the native binary and sets imports to the reviewed checkout. `lifecycle.py` creates disposable local cache files; stores and journals are in memory.

**Existing focused tests: 8 passed in 2.86 seconds**, with bytecode and pytest cache writes disabled. Selected tests: native A12-1 and A12-2; native fold seed 0; publication rejecting another life/files; simple orphan protection; three failures stopping upkeep; ordinary-output backpressure; full-pass fallback when a consumer lacks a boundary. These use the same qualified runtime above. They do not cover the reported counterexamples.

**Inherited gates, not rerun:** A15 records macOS at `78a7c5e5f2a2d6d29df6aa1f4d3028a837136294`: ruff, cargo fmt, cargo release tests, and pytest with Postgres, 1,141 passed / 53 skipped / 5 xfailed; Rust 12 passed. Linux container at the same revision: 1,143 passed / 51 skipped / 5 xfailed. These substantiate steps 1–2's reported gate, not the later net commit. No full suite, benchmark, Postgres, macOS or container gate ran during A17.

Test gaps: the Lean/vector tests supply complete native runs and use small generation values; they miss Python `_window`, cached file selection and the maximum-generation alias. Cache tests merge without retaining endpoints, so they do not create these duplicate file ranges. The orphan test has one engine and a static `_busy` set. The retry and admission tests stop at one engine lifetime and ordinary outputs. The `/proc/self/status` cache-memory regression is Linux-only, correctly explaining why both platform gates matter; neither it nor the bounded block tests constrain accumulated versions of one key.

## Choices, alternatives and constraints

Kept all implementation fixes with the coordinator, as required. Review notes and probes live outside the repository. D77 records the fixed source scope and reuse of A15's gates; the later A17 decision records the runtime qualification. No installation, full suite or stress benchmark was necessary to make the findings concrete.

For orphan collection, rechecking one engine's in-memory references alone does not establish authority after takeover. For repeated-key paging, fetching an unlimited group would fix correctness while worsening R10. For retry accounting, treating restart as a fresh budget is a possible revised policy, but it abandons the stated lifetime write bound and requires an explicit decision. These alternatives are called out so the fixes preserve the approved guarantees.

The fallback from a lost lower endpoint starts a full pass and the existing regression passes. It does not make a missing reservation acceptable: R5 can force repeated full passes. The life/input publication checks and two non-overlapping merge lanes have no additional demonstrated correctness defect in this review. Their protection does not cover orphan deletion by an obsolete owner.

The 32-span figure is not a hard online maximum in this implementation. D71 intentionally permits ordinary writers to reach 64; force-merging begins above 32. Source/failure admission and per-key version accumulation invalidate even the weaker global bound. Physical retention budget defaults remain an open design decision; I did not treat them as an implemented limit.

## Human reviewer callouts

- **This change introduces backwards-incompatible public schema/API/contract changes:** v4 files and span-based index state replace the earlier format/state; the design specifies an empty namespace precondition.
- **This change includes irreversible or destructive operations:** upkeep deletes superseded and orphan merge objects. R1 must be fixed before trusting this collector with durable data.

## Bounded handoff

No repository dirty files, pending commands or background jobs. This review is complete; source is unchanged. The coordinator should route R1–R10 to the implementation owners and request focused regressions, then both full platform gates on the resulting revision. Step 3 still needs measurements on corrected real code, the cost-document update, and consumer/staleness adoption of the `changes` classes/read-ahead contract as tracked by the coordinator. The net rule and removal of `Merge.compact` already landed in eef4f33. Re-review the corrected lifecycle and readers before accepting the quantitative bounds.
