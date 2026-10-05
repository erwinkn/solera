# Behaviours: what Solera does, whatever its design

Status: **the contract.** Each entry is something a user, a producer or an
API client can observe, decided by Erwin or settled by a finding. Any
design of history, observations and commits must keep every entry; one
that changes an entry changes the contract, and says so. Entries become
scenario tests, property tests against a reference model, and a
behavioural TLA+ spec (T40, T41).

Checked against `origin/main` at `8fceb63` ("object-store-state.md says
what the state holds now"); open questions resolved by T42. Test paths are under `tests/`; `sim` invariants are
`tests/sim/machine.py`; TLA+ specs are `spec/tla/`.

## How to read it

Every entry has an id (area prefix, stable), a rule in its title, a
concrete example, its sources (*From*), and what checks it today
(*Covered*), or **none**. Where a decision superseded another, only the
successor is stated, and the chain is noted (`D175 → D176 → D177`).
Entries describe behaviour only: how it is built today is the appendix's
business. Where sources disagree, or the code does not do what was
decided yet, the entry says so and the last section lists it.

**The running example** (`tests/server/histories.py`, the simulation):

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
[RST](#rst--upstream-resets-removals-moves-and-renames) ·
[SRC](#src--sources-and-served-versions) ·
[KEY](#key--per-key-assets-and-key-outcomes) ·
[STA](#sta--staleness-completeness-and-statuses) ·
[PAR](#par--partitions-fan-in-broadcast-dynamic-partitions) ·
[AUT](#aut--automations) ·
[HIS](#his--history-and-lineage) ·
[CLN](#cln--cleanup-and-retention) ·
[ENG](#eng--engine-crashes-restarts-zombies-and-fencing) ·
[DEP](#dep--deploys-and-reload) ·
[STO](#sto--fenced-and-immutable-stores) ·
[Open questions](#contradictions-gaps-and-open-questions) ·
[Coverage](#coverage-summary) ·
[Appendix](#appendix-entries-to-todays-mechanisms)

## INC — incremental delivery and classification

**INC-1. A first run delivers every upstream key as added.**
`tally` has never run; `items` holds k1…k5; `batch_size=2`. The run makes
three batches, `added` = [k1, k2], [k3, k4], [k5]; nothing is updated or
removed; the count ends at 5.
*From* D44, D139; F44. *Covered* `server/test_changes.py::test_a_first_run_delivers_every_key_as_added`; `ObservedSet.tla` CountExact.

**INC-2. A key's class is the net difference between what the partition processed and upstream now.**
`added`: it did not hold the key, upstream has it. `updated`: both have
it, at different versions. `removed`: it held the key, upstream no longer
has it (or no longer offers it, INC-7, CHG-4). `tally` processed k1@1,
k2@1; then k1 → @2, k3 added, k2 removed: the next batch has
`added=[k3]`, `updated=[k1]`, `removed=[k2]`, and `rows` holds k3 and k1.
*From* D44, D126, D139 (chain: D42 → D126). *Covered* `server/test_engine.py::test_incremental_filters_input_and_changes`; `server/test_owed.py::test_runs_keep_the_record_equal_to_the_observed_set`; `ObservedSet.tla` OwedExact.

**INC-3. Changes that cancel out between two runs deliver nothing.**
k4 is added and removed again before `copy` runs: `copy` gets nothing
about k4 and is not stale. A key removed and added back is `updated`.
*From* D44, D126. *Covered* `server/test_staleness.py::test_a_key_added_and_removed_past_the_read_changes_nothing`.

**INC-4. An update reverted to the processed version is still delivered, as updated.**
`feed`'s k1 goes @1 → @2 → @1 before `items` runs: `items` gets k1 as
`updated`. Accepted cost: upstream history keeps no old versions, so a
key changed since it was processed is known only to have changed.
*From* D156; review A31 R2 (`observed-set.md`, "What the index can say"); Q4. *Covered* `server/test_staleness.py::test_a_key_updated_and_reverted_reads_as_updated`.

**INC-5. A total kept from the classes alone stays exact.**
`tally` = what it held + `len(added)` − `len(removed)`. After any history
of commits, `keys=` runs, cancels and failures, once a default run has
nothing left to owe, the count equals the number of keys upstream offers.
*From* D44, D126, D139. *Covered* `server/test_changes.py::test_a_total_kept_from_the_changes_stays_exact`; `server/test_staleness.py::test_a_count_kept_from_its_batches_stays_exact_through_a_keys_run`; every `server/test_observed_histories.py` test (asserts the count); `ObservedSet.tla` CountExact, TallyExact.

**INC-6. Every change is delivered once, even one made during a run.**
`keys=(k1)` processes k1@1; then k1 → @2 (or k3 is added, or k1 removed);
two default runs follow. The change arrives exactly once, in the first.
*From* D126, D139; review A19 R1, R2. *Covered* `server/test_observed_histories.py::test_a19_r1_r2_a_change_during_a_run_is_counted_once` (update, add, remove); `server/test_changes.py::test_a_change_during_a_full_pass_is_counted_once`; `Positions.tla` DeliveredOnce (positions-era model).

**INC-7. A delivered removal is final.**
Once `tally` was given k5's removal, k5 coming back, even at the version
it once had, is an `added`. `feed` removes k1; `keys=(k1)` processes the
removal; `feed` restores k1@1: the next run owes k1 as added.
*From* D126, D133; review A19 R3, A27 R9. *Covered* `server/test_observed_histories.py::test_a27_r9_a_removal_restored_at_its_version_is_owed_its_add`; `server/test_observed_histories.py::test_a19_r3_a_delivered_removal_is_not_forgotten`.

**INC-8. Each batch reads upstream as it is when that batch starts; nothing goes back in time.**
A run over k1, k2 with `batch_size=1`: batch 1 reads k1; k2 changes; batch
2 delivers k2's new version. If `keys=(k1)` delivered k1@2 before the
run's batch reached k1, that batch delivers nothing for k1, never k1@1.
*From* D139, D154; `ObservedSet.tla` counterexample 2 (W36). *Covered* `server/test_observed_histories.py::test_a26_n1_a_run_reads_rows_after_the_upstream_moved_on`; `server/test_versions.py::test_each_batch_says_the_generation_it_read`.

**INC-9. A change made behind a run's batches is owed by the next run.**
Batch 1 commits k1; k1 changes while batch 2 runs: the run ends, the
partition is stale, the next run delivers k1 as updated. A key removed
during a run, before its batch, is delivered once, as whatever it is when
its batch reads.
*From* D139. *Covered* `server/test_changes.py::test_a_key_removed_during_a_full_pass_is_counted_out_once`; `server/test_observed_histories.py::test_a19_r5_an_early_removal_from_a_current_only_source_is_delivered_once`.

**INC-10. What a producer receives for an incremental input.**
`ctx.batch[input]`: `added`, `updated`, `removed`, `unchanged` (keys
loaded only because the run named them, SEL-1), `rows` (of added, updated
and unchanged; removed keys have none, and `rows` is the parameter),
`index` (0-based in the run), `count` (batches planned, an estimate),
`first`, `final` (no batch of this run follows; never inferred from
`count`), `reset` (the first batch of a full run, SEL-9), `upstream` (its
output; for an unkeyed upstream the commit range). A run of 3 batches
gives `index` 0, 1, 2 and `final` only on the last.
*From* D44, D140 (4), D157. *Covered* `server/test_engine.py::test_a_batch_says_where_it_sits_in_its_pass`; `server/test_engine.py::test_the_batch_plan_is_an_estimate_but_final_is_not`. `reset`: `server/test_selection.py::test_sel_9_a_full_runs_first_batch_says_reset_and_load_returns_what_is_materialized`.

**INC-11. `ctx.load()` returns the asset's own output as materialized; `None` before its first commit.**
`tally`'s second batch reads `{"rows": 2}` committed by its first, and adds
to it.
In a full run too: batch 0 says `reset`, and `ctx.load()` still returns
what was materialized before it (SEL-9).
*From* D44, D166 (chain: D141 → D166); Q1. *Covered* `server/test_selection.py::test_inc_11_ctx_load_returns_the_output_as_materialized`; the full-run case `server/test_selection.py::test_sel_9_a_full_runs_first_batch_says_reset_and_load_returns_what_is_materialized`; exercised by `tally` in every `server/test_observed_histories.py` test.

**INC-12. A task walks what it owes in key order, `batch_size` keys a commit.**
Seven owed keys, `batch_size=3`: three batches, three commits. Default
`batch_size` is 10,000 for every incremental input.
*From* D111, D115, D155. *Covered* `server/test_each.py::test_concurrency_and_batches`; `server/test_owed.py::test_a_batch_takes_its_size_of_owed_keys_and_covers_up_to_the_last`; `server/test_engine.py::test_incremental_batching_and_more`.

**INC-13. An unkeyed incremental upstream delivers the commits after the last one read; its reset is delivered as a start-over.**
`events` (unkeyed, append) commits 0–9; `digest` read through 6; its next
batch gets commits 7–9 (`upstream.commits`). If `events` is run `full`,
`digest` starts over rather than receiving the reset as a delta.
*From* F8. *Covered* `server/test_sim_found.py::test_an_unkeyed_upstream_reset_right_after_a_pass_is_delivered_in_full`; `server/test_sim_found.py::test_an_earlier_lifes_objects_are_never_read`.

**INC-14. Writing is changing; writing nothing wakes nothing.**
`items` returns an empty `Patch`: no commit, no `OnChange`. It writes the
same rows again: a new version, and consumers process them again
(accepted: over-eager, never wrong).
*From* `versions.md` §1 (Erwin). *Covered* `server/test_engine.py::test_a_poll_that_writes_nothing_wakes_nothing`; `server/test_keys.py::test_every_write_is_a_change_and_an_empty_patch_none`; `server/test_model.py::test_content_written_again_is_a_change`; `server/test_matrix.py::test_a_value_written_again_is_a_new_version_its_readers_reread`.

## RUN — runs, tasks, batches, attempts, retries, cancel

**RUN-1. A run has a task per asset partition; a task commits batch by batch; an attempt runs one batch.**
`tally` owes 25,000 keys, `batch_size=10_000`: one task, three batches.
Batch 2's first attempt dies; attempt 2 of batch 2 runs it again.
*From* D155. *Covered* `server/test_engine.py::test_incremental_batching_and_more`; `server/test_complete.py::test_each_commit_row_names_the_batch_that_made_it`.

**RUN-2. One attempt at a time per asset partition, across runs.**
The hourly run and a manual run both target `copy`: the second task waits
("held: claim (copy)") and runs after the first settles.
*From* `lifecycle.md` §3.1; F34, F39. *Covered* `server/test_engine.py::test_fencing_concurrent_claim`; sim `one_attempt_per_partition`; `Execution.tla` OneAttemptPerPartition.

**RUN-3. `@asset(concurrency=N)` caps partitions running at once.**
`report` has `concurrency=2` and three partitions due: `a` and `b` run,
`c` waits ("held: concurrency (report)") and runs when one ends.
*From* D78, D111. *Covered* `server/test_engine.py::test_an_assets_concurrency_caps_its_partitions_running_at_once`.

**RUN-4. A waiting task says why.**
One of: `claim`, `concurrency`, `merges` (an output it writes is far
behind on upkeep), `engine` (the engine's slots), `executor` (its limit),
`invalid` (its placement cannot be built).
*From* `lifecycle.md` §3.1; D71. *Covered* `server/test_history.py::test_a_task_held_back_says_why`; `server/test_keys.py::test_writes_wait_while_an_outputs_merges_are_far_behind`.

**RUN-5. A task resumes from its last committed batch, across attempts and engine restarts; a new run starts from the beginning.**
Batches 1 and 2 commit; batch 3's attempt dies, or the engine restarts:
the task continues after batch 2's last key. When the run ends, its
progress goes; the next run compares again and walks from the first key,
cheaply, since only changes are owed.
*From* D154, D155. *Covered* `server/test_engine.py::test_a_full_override_resumes_its_pass_batch_by_batch`; `server/test_each_review.py::test_a_full_run_reads_every_key_once_in_batches`; `sim/test_replays.py::test_f42_a_full_run_finishes_its_pass`.

**RUN-6. A task never relaunches forever.**
Two full runs of `copy` with a `feed` commit between: the second finishes
in a bounded number of attempts.
*From* F42; D60, D81. *Covered* `sim/test_replays.py::test_f42_a_full_run_finishes_its_pass`; sim `attempts_are_bounded` (100 attempts per task).

**RUN-7. A failed attempt is retried by its error's class.**
`retries=Retry(3)` by default, with backoff. `Rejected` fails the task at
once; `Failed` and `Abort` use `retries=`; `Transient` retries after
`retry_after` (else 1 min doubling to 6 h), past `retries=`, until
`retry_for` (24 h) since its first failure. A worker that dies without a
result is a retryable failure.
*From* `per-key-processing.md` §8. *Covered* `server/test_errors.py::test_rejected_fails_without_retries`; `server/test_errors.py::test_failed_and_abort_follow_retries`; `server/test_errors.py::test_transient_retries_past_retries_within_its_budget`; `server/test_errors.py::test_transient_gives_up_after_retry_for`; `server/test_engine.py::test_retry_with_backoff_and_nonretryable`; `worker/test_worker.py::test_killed_harness_retries`.

**RUN-8. A failed or abandoned attempt changes nothing a consumer processed.**
Batch 2 of `tally` fails: its keys stay owed and `tally`'s count is what
batch 1 left.
*From* D139 (`observed-set.md`, "Outcomes"). *Covered* `server/test_model.py::test_failed_precondition_changes_nothing`; `server/test_model.py::test_an_aborted_attempt_can_no_longer_commit`.

**RUN-9. A cancel is asked first, then forced.**
The worker hears the cancel within a beat (10 s), stops starting work and
has `cancel_grace` (60 s, one engine-wide setting) to publish what
finished. A plain asset that had not begun writing ends `canceled` with
nothing; one already writing finishes and its commit stands. Past the
grace the attempt is forced and nothing more of it is accepted.
*From* D135; `lifecycle.md` §7. *Covered* `server/test_lifecycle.py::test_a_requested_cancel_drains_into_a_canceled_result`; `server/test_fence.py::test_a_cancel_waits_for_a_worker_that_is_writing`; `server/test_fence.py::test_a_drain_that_outlives_its_grace_is_forced_and_still_writing`; `server/test_fence.py::test_a_cancel_aborts_a_worker_that_is_not_writing`; `server/test_engine.py::test_cancel_run`.

**RUN-10. A cancelled run keeps what it committed and does not resume itself.**
`tally`'s run commits batches 1 and 2 and is cancelled: their keys stay
processed; the partition is stale; the next run owes only the rest plus
what changed since.
*From* D139, D154. *Covered* `server/test_complete.py::test_an_incremental_run_canceled_midway_is_complete_and_stale`; `server/test_each.py::test_a_user_cancel_commits_finished_keys_and_leaves_the_rest_owed`.

**RUN-11. An attempt's timeout counts from its worker's first report.**
A 20-minute image pull does not eat a 30-second timeout. Past the
timeout the attempt is cancelled (reason `timeout`) and retried within
`retries=`. Before any report, the executor's provisioning deadline
applies (`provision=`: 10 min for a container executor, none for a
`Pool` unless set) and the attempt fails retryably.
*From* `lifecycle.md` §8. *Covered* `server/test_fence.py::test_the_timeout_runs_from_the_first_report`; `server/test_engine.py::test_timeout_fails_retryably`; `server/test_lifecycle.py::test_a_timeout_drain_is_retryable`; `server/test_fence.py::test_a_worker_that_never_reports_is_given_up_on`.

**RUN-12. Retrying a finished run is a new run; the old one stays as it ended.**
Run R had a failed and a canceled task; "retry" submits a new run with
`retry_of: R` for exactly that work; R still reads as it ended.
*From* `architecture.md` §8. *Covered* `server/test_history.py::test_a_retry_is_a_new_run_and_the_old_one_stays_as_it_ended`; `server/test_history.py::test_a_retry_asks_for_the_work_it_selects`.

**RUN-13. A task's outcome is `succeeded`, `skipped`, `failed` or `canceled`.**
`skipped`: nothing owed and the partition complete, so no worker
launches. A per-key run whose keys partly failed still `succeeded`, with
per-key counts (`{ok: 3, rejected: 1, failed: 1, removed: 1}`).
*From* `architecture.md` §8; `per-key-processing.md` §8. *Covered* `server/test_retention.py::test_quiet_runs_are_recorded_as_skipped`; `server/test_each.py::test_failures_are_recorded_and_never_block`.

**RUN-14. The console shows the run first, and batches and attempts only when they say something.**
A task with one batch and one attempt shows just the run. A retry adds
"attempt 2". Several batches add "batch 2 of 3" and a progress bar. No
user-facing text says "attempt spec" or "attempt handle".
*From* D173, D170. *Covered* none in pytest (console e2e `apps/console/tests/console.spec.ts` not checked here).

**RUN-15. A run targets its assets only unless asked for upstream.**
`upstream=False` (default) pins current heads and never re-runs an
upstream; `upstream=True` builds the closure first. A selection past
100,000 partitions or tasks is refused, never truncated.
*From* `architecture.md` §8. *Covered* `server/test_engine.py::test_upstream_false_never_replans`; `server/test_planning.py::test_latest_and_changes_are_counted_before_they_are_listed`.

## SEL — `keys=` lists, `"all"`, full runs, `batch.reset`

**SEL-1. `keys=[…]` loads exactly the named keys and never touches another.**
`copy` holds k1, k2, k3; k1 and k2 changed upstream. `keys=(k1)` delivers
k1 as updated; k2 and k3 are untouched, and k2 is still owed. A named key
already processed as it is comes as `unchanged`.
*From* D140 (3), D126 (chain: D36 → D41 → D42 → D126 → D140). *Covered* `server/test_engine.py::test_run_keys_override`; `server/test_owed.py::test_a_named_key_held_as_it_is_is_unchanged`; `server/test_staleness.py::test_a_keys_run_never_touches_a_key_it_does_not_name`; `server/test_staleness.py::test_a_keys_run_makes_each_named_key_match_its_upstream`.

**SEL-2. A named key upstream lacks is a removal if held, else nothing.**
`keys=(k9)` where k9 never existed: nothing delivered, nothing recorded.
`keys=(k1)` after `feed` removed k1: k1 delivered as removed.
*From* D126. *Covered* `server/test_observed_histories.py::test_a26_n2_selections_around_a_pattern_change` (`absent key`); `server/test_observed_histories.py::test_a27_r9_a_removal_restored_at_its_version_is_owed_its_add`.

**SEL-3. What a `keys=` run delivered is not delivered again.**
`keys=(k1)` delivers k1's update; the next default run delivers k2's and
not k1's.
*From* D126, D140 (3). *Covered* `server/test_staleness.py::test_a_keys_run_on_an_incremental_asset_delivers_each_change_once`; `server/test_observed_histories.py::test_a19_r4_a_selection_under_new_patterns_is_counted_once`.

**SEL-4. A `keys=` run never clears what it did not process.**
After `keys=(k1)`, `copy` stays stale for k2 until a run processes k2.
*From* D140 (3) (chain: D36 → D140). *Covered* `server/test_observed_histories.py::test_a19_r3_a_delivered_removal_is_not_forgotten`; `server/test_staleness.py::test_a_key_gone_upstream_stays_stale_through_a_keys_run_that_does_not_name_it`.

**SEL-5. Named keys go through the input's patterns.**
`checks` takes `include=["k1"]`; `keys=(k1, k2)` loads k1 only; a held k2
is delivered as removed.
*From* `per-key-processing.md` §16. *Covered* `server/test_each_review.py::test_7_a_keys_override_obeys_the_patterns`.

**SEL-6. A long `keys=` list goes `batch_size` keys a commit, `concurrency` at once.**
Ten named keys, `batch_size=4`: three batches, three commits; the run
ends with the ten processed.
*From* D111 (chain: D93 (3) → D111); review A19 R6. *Covered* `server/test_observed_histories.py::test_a19_r6_keys_runs_go_batch_size_keys_a_commit`; `server/test_each.py::test_a_keys_selection_runs_batch_size_keys_at_a_time`.

**SEL-7. `keys={input: "all"}` loads every key under the patterns without starting over.**
Owed keys in their class, the rest `unchanged`; nothing is reset.
`keys={"rates": "all"}`.
*From* D140 (3); Q3. *Covered* `server/test_selection.py::test_sel_7_keys_all_loads_every_key_without_starting_over`; `server/test_engine.py::test_run_keys_override`.

**SEL-8. A full run owes every upstream key under the patterns; a plain consumer starts over.**
`mode="full"`, or an automatic full run (CHG-1, RST-1). A full run is the
run's mode, never a `keys=` value (Q3: `keys={input: "full"}` is refused).
`tally`'s first batch starts its count from zero; its cursor is `None`;
the run continues batch by batch across attempts.
*From* D141, D166, D140. *Covered* `server/test_selection.py::test_sel_8_a_full_run_owes_every_key_and_a_plain_consumer_starts_over`; `server/test_selection.py::test_sel_8_keys_full_is_no_longer_a_full_run`; `server/test_engine.py::test_a_full_run_starts_its_record_over`; `server/test_engine.py::test_result_cursor_and_omitted_output`; `server/test_each_review.py::test_a_full_run_reads_every_key_once_in_batches`.

**SEL-9. A full run's first batch says `batch.reset`; `ctx.load()` still returns what is materialized.**
`tally` sees `batch.reset == True` on batch 0 and ignores `{"rows": 7}`
from `ctx.load()`; later batches build on what batch 0 committed.
Per-key producers never see `reset`.
*From* D166 (chain: D141 → D166; D140 (4) drops `Batch.full`); Q1. *Covered* `server/test_selection.py::test_sel_9_a_full_runs_first_batch_says_reset_and_load_returns_what_is_materialized`.

**SEL-10. A per-key consumer's full run keeps its outputs readable and removes what upstream lacks.**
`checks` holds k1, k2, x9; upstream has k1, k2, k3. The full run calls k1
and k2 (updated), k3 (added) and drops x9 (removed). A key that fails
keeps its last good output. Cancelled midway, the rest stays owed and
readable.
*From* D141, D166. *Covered* `server/test_owed.py::test_a_held_base_owes_each_held_key_an_update_or_a_removal`; `server/test_each_review.py::test_2_a_full_run_removes_what_upstream_no_longer_has`; `server/test_each_review.py::test_2_a_full_run_keeps_a_failing_keys_last_good_output`; `server/test_complete.py::test_a_per_key_full_run_canceled_midway_is_complete`.

**SEL-11. A full run reaches its producer even when it owes no key.**
`copy` (plain) over an upstream now empty, or with patterns taking
nothing: called once with an empty batch, first and final, and its
output becomes empty. A per-key consumer is not called; its held keys
are removed.
*From* F10. *Covered* `server/test_sim_found.py::test_a_full_pass_that_takes_no_key_still_starts_over`; `server/test_sim_found.py::test_a_full_pass_over_an_empty_upstream_reaches_its_producer`; `server/test_sim_found.py::test_an_each_full_pass_that_takes_no_key_drops_its_keys`.

**SEL-12. A due full run may be carried out by several runs, `keys=` ones included.**
`copy`'s version is bumped; `keys=(k1)` is the full run's first batch:
`copy` now holds k1 alone and owes k2, k3; a default run finishes it.
*From* D140, D141. *Covered* `server/test_staleness.py::test_a_full_run_after_an_asset_change_may_take_several_runs`; `server/test_staleness.py::test_a_full_run_spread_over_keys_runs_delivers_each_key_it_names`.

## CHG — definition, pattern and context changes

**CHG-1. A definition change makes the partition stale, and its next run a full run.**
The definition covers the asset's `version`, its input bindings, its
outputs' migrations, its stores' versions, and the run's config. `tally`
`version="1"` → `"2"`: stale ("definition changed"); the next default run
starts over (SEL-8).
*From* D140 (1), D93 (4), D34. *Covered* `server/test_engine.py::test_a_version_bump_starts_the_next_run_over`; `server/test_engine.py::test_config_change_reprocesses_everything`; `server/test_engine.py::test_migration_changes_fingerprint_and_marks_handle`; `server/test_each.py::test_binding_a_whole_input_to_another_head_rebuilds_every_key`. Whether store config is in it: Q11.

**CHG-2. A code change alone is a new deploy, not a definition change.**
Editing a helper module serves a new deploy: nothing goes stale; failed
per-key keys get one more try (KEY-3).
*From* D174; `per-key-processing.md` §13. *Covered* `sdk/test_build.py::test_the_build_is_the_code_the_project_runs`; `server/test_each.py::test_a_failed_key_gets_one_try_per_deploy`.

**CHG-3. A batch planned before a definition change or an upstream reset does not commit.**
`copy`'s attempt is launched as version 1; version 2 is deployed before
it settles: it commits nothing, and its keys stay owed under version 2.
*From* D139 (the commit check); `ObservedSet.tla` counterexample 3. *Covered* `server/test_fence.py::test_an_attempt_launched_before_a_version_bump_does_not_commit`; `server/test_sim_found.py::test_an_attempt_launched_before_its_output_moved_commits_nothing`.

**CHG-4. A pattern change is an input change: the next run owes the difference in membership.**
`tally` has `include=["k1"]`; a deploy widens it to `"k*"`: stale ("input
changed"); the next run adds k2, k3 and does not process k1 again.
Narrowing to `"z*"` owes the removal of every held key.
*From* D140 (2), D126 (amends D34's "patterns are an asset change"). *Covered* `server/test_each.py::test_a_pattern_change_cuts_over`; `server/test_observed_histories.py::test_a19_r9_excluding_every_held_key_owes_their_removal`; `server/test_staleness.py::test_a_pattern_change_that_excludes_every_held_key_leaves_it_stale`.

**CHG-5. A first `include` removes what it leaves out.**
No include over `keep/a`, `drop/b`; a deploy adds `include="keep/*"`:
`drop/b` is owed a removal.
*From* D126; review A27 R3. *Covered* `server/test_observed_histories.py::test_a27_r3_a_first_include_owes_the_removal_of_what_it_leaves_out`.

**CHG-6. A key added upstream while the patterns exclude it is owed nothing.**
Patterns narrow from `keep/*, drop/*` to `keep/*`; `drop/b` is then added
upstream: nothing.
*From* D126; review A27 R4. *Covered* `server/test_observed_histories.py::test_a27_r4_an_add_during_a_narrowing_is_owed_nothing`.

**CHG-7. Widening to a key that never existed owes nothing.**
`include=["k1"]` → `["k1", "k9"]`, no k9 upstream: not stale.
*From* D126; review A27 R10. *Covered* `server/test_observed_histories.py::test_a27_r10_widening_to_a_key_that_never_existed_changes_no_debt`.

**CHG-8. A `keys=` run around a pattern change is counted once.**
Widen `k1` → `k*`, then `keys=(k2)`, then a default run: k2 is added once;
the count ends at 3 for k1, k2, k3.
*From* D126; review A19 R4, A26 N2, N3. *Covered* `server/test_observed_histories.py::test_a26_n2_selections_around_a_pattern_change` (repeated, absent key, partial); `server/test_observed_histories.py::test_a26_n3_a_selection_before_an_old_pattern_delta_is_kept`; `server/test_changes.py::test_a_selection_during_a_pattern_change_is_counted_once`.

**CHG-9. A change to a key a consumer excludes makes nothing stale there.**
`items` commits only `x1`, which `copy` excludes: `copy` stays fresh.
`filtered` takes k1 of `checks`; k2 changes: `filtered` stays fresh, and
k1 changing makes it stale.
*From* D39; review A19 R10. *Covered* `server/test_staleness.py::test_a_commit_of_excluded_keys_alone_leaves_their_consumers_fresh`; `server/test_observed_histories.py::test_a19_r10_an_excluded_stale_upstream_key_does_not_propagate`; `server/test_staleness.py::test_an_each_consumer_is_not_upstream_stale_for_a_key_it_excludes`.

**CHG-10. A moved whole or dep input owes every key processed under its old version.**
`checks` processed k1, k2 under `factor=w1`; `factor` → `w2`: both owed
updates, reason "input changed" (not "definition changed").
`keys=(k1)` processes k1 under `w2`: k2 still owed. `factor` back to `w1`:
now k1 is owed and k2 is not.
*From* D40, D126; review A19 R7, A27 R1. *Covered* `server/test_observed_histories.py::test_a19_r7_a_shared_input_move_owes_every_key_its_context`; `server/test_staleness.py::test_a_shared_input_change_makes_every_key_stale`; `server/test_staleness.py::test_a_dep_change_is_an_input_change_owing_every_key`.

**CHG-11. A whole input written again with the same content is a change.**
`settings` is rewritten identically: a new version; its readers owe a
rerun.
*From* `versions.md` §7. *Covered* `server/test_matrix.py::test_a_value_written_again_is_a_new_version_its_readers_reread`.

**CHG-12. Patterns match keys by path segments.**
`**` crosses `/`, `*` does not; `Regex(…)` too. `exclude="k1*"` is one
pattern, not its characters; `**/*old*` matches `Gold_ore.csv`, so word
rules want segment-anchored patterns.
*From* F1; `per-key-processing.md` §11. *Covered* `sdk/test_patterns.py::test_globs`; `sdk/test_patterns.py::test_one_exclude_pattern_is_a_pattern_not_its_characters`; `sdk/test_patterns.py::test_a_key_is_taken_when_an_include_and_no_exclude_matches`.

## RST — upstream resets, removals, moves and renames

**RST-1. Moving an output to another store makes it a new output.**
`items` moves from FileStore to Postgres: its head and keys start empty;
its first write is whole; every consumer owes a full run (stale "input
changed"); its own inputs are read in full. Nothing is copied; the old
store's data is cleaned up (CLN-6).
*From* D10. *Covered* `server/test_observed_histories.py::test_a27_r7_an_upstream_reset_owes_a_full_run`; `server/test_sim_found.py::test_a_key_a_moved_output_dropped_leaves_its_consumer`; `sim/test_replays.py::test_f9_a_key_dropped_by_a_moved_output_leaves_its_consumers`; `sim/test_replays.py::test_f13_a_key_removed_after_a_store_move_leaves_its_consumers`.

**RST-2. Moving away and back is two resets.**
`items` moves to Postgres and back with nothing written between: it is
reset all the same, and keeps no key from before.
*From* D10; F17. *Covered* `server/test_sim_found.py::test_a_move_and_back_with_no_write_between_resets`; `sim/test_replays.py::test_f17_an_output_moved_away_and_back_keeps_its_keys`.

**RST-3. An asset removed and declared again starts a new life.**
`copy` removed, then declared again: no cursor, no processed keys, no
failing keys from before. An attempt of the first life, still running,
commits nothing into the second. Same for a job.
*From* F12, F19, F21. *Covered* `server/test_sim_found.py::test_a_name_removed_and_added_back_starts_over`; `server/test_sim_found.py::test_an_attempt_of_a_removed_and_readded_asset_stays_in_its_life`; `server/test_sim_found.py::test_a_job_added_back_does_not_take_its_first_lifes_commit`; sim `a_life_is_its_own`.

**RST-4. A rename with `aliases=` keeps everything.**
`copy` → `mirror` (`aliases=["copy"]`): heads, keys, cursor, what each
input processed, failing keys, owed repairs and cleanups, automation
state all follow; consumers continue incrementally. Data stays where it
is: `mirror`'s new partition b is written under `copy/`.
*From* D85 (1). *Covered* `server/test_keys.py::test_renamed_asset_keeps_its_state`; `server/test_model.py::test_a_rename_moves_a_scopes_record_whole`; `server/test_each_review.py::test_8_a_renamed_asset_keeps_its_failures`; `server/test_lifecycle.py::test_a_renamed_asset_keeps_what_its_scope_owes`; `server/test_lifecycle.py::test_a_renamed_outputs_new_partitions_go_where_its_old_ones_are`; `server/test_lineage_reads.py::test_a_renamed_postgres_output_stays_readable`.

**RST-5. An attempt launched before a rename settles under the new name.**
`copy`'s attempt runs; `copy` is renamed `mirror`: the attempt commits
into `mirror` and its run ends. Renamed onto an earlier life's name, the
earlier life's attempt still settles.
*From* F5, F34. *Covered* `server/test_sim_found.py::test_an_attempt_launched_before_a_rename_settles`; `server/test_fence.py::test_a_rename_moves_a_launched_attempt_with_its_scope`; `sim/test_replays.py::test_f34_a_rename_onto_an_earlier_lifes_name_keeps_its_attempt_settleable`.

**RST-6. A removed asset's queued work is cancelled, saying why.**
A deploy drops `doomed`: its queued tasks are cancelled with the reason,
its runs end, and a launched attempt is not retried.
*From* F4. *Covered* `server/test_matrix.py::test_a_removed_asset_takes_its_queued_work_with_it`; `server/test_sim_found.py::test_a_removed_assets_last_attempt_ends_its_run`; `server/test_fence.py::test_a_removed_assets_launched_attempt_is_not_retried`.

**RST-7. An upstream reset drops a per-key consumer's failing keys for that input.**
`items` moves; `checks` had k1 failing: k1's failure goes at the deploy
(it failed against keys that are no longer the input's).
*From* `per-key-processing.md` §9. *Covered* `server/test_each.py::test_a_reset_of_the_input_drops_the_stored_outcomes`.

**RST-8. A removed consumer stops holding back its upstream's history.**
`gone` and `keep` read `feed`; `gone` is removed: `feed`'s history is
kept for `keep` alone.
*From* `object-store-state.md` §2. *Covered* `server/test_matrix.py::test_a_removed_consumer_lets_go_of_its_upstreams_log`.

## SRC — sources and served versions

**SRC-1. A source commit changes only keys whose version moved.**
`feed` holds a.csv@c7, b.csv@c3; `commit(keys={a.csv: c7, b.csv: c4,
d.csv: c1})`: b.csv updated, d.csv added, a.csv unchanged. A full map
removes keys it omits; `upsert`/`remove` patch. A key given no version is
always a change. An unkeyed source given its current version again makes
no commit.
*From* `versions.md` §2 (Erwin). *Covered* `server/test_keys.py::test_keyed_source_commits_go_through_the_index`; `server/test_api.py::test_source_commit_endpoints`; `server/test_planning.py::test_unkeyed_source_commits_over_http`.

**SRC-2. Each source commit that changes something is a run with no tasks.**
It says who committed and what changed, and expires with the project's
default retention.
*From* `object-store-state.md` §11. *Covered* `server/test_retention.py::test_source_commits_are_recorded_as_runs`.

**SRC-3. Sources are read live, and every load says which version it served.**
A keyed loader returns `{key: Loaded(row, version=…)}`; a key it leaves
out was served absent. `ctx.batch[input].served` shows each key's served
version. A keyed loader that omits versions fails the load.
*From* D146, D147. *Covered* `server/test_sources.py::test_a_function_source_serves_rows_with_their_versions`; `server/test_sources.py::test_a_key_the_loader_leaves_out_was_observed_absent`; `server/test_sources.py::test_a_keyed_loader_must_say_what_it_served`.

**SRC-4. A batch's classes follow what the source served, before the producer runs.**
The commit says k2@2; the source serves k2@3: classed from @3. Restored
to @2 by a commit: owed as updated. Served absent: removed if held, else
nothing. Served at the version already processed: nothing.
*From* D147, D126; review A27 R2, A26 N4. *Covered* `server/test_observed_histories.py::test_a26_n4_a_row_served_ahead_of_its_commit_is_a_point`; `server/test_observed_histories.py::test_a26_n4_a_key_served_absent_then_back_is_owed_its_add`.

**SRC-5. A key the source lost without a commit is processed as absent; its return is delivered.**
`feed` commits k1@3; before `items` reads it the outside loses k1:
`items` processes k1 as absent. `feed` restores k1@3: `items` is owed k1
(added). A commit that removes k1 owes nothing more.
*From* D147, D126 (chain: D56 → D70 → D93 → D100 → D126/D147); F33, F38, F41. *Covered* `server/test_source_behind.py::test_a_key_the_source_lost_is_observed_absent`; `server/test_source_behind.py::test_the_next_commit_restoring_the_key_delivers_it`; `server/test_source_behind.py::test_the_next_commit_removing_the_key_owes_nothing`; `server/test_source_behind.py::test_a_per_key_batch_observes_it_alike`; `server/test_sim_found.py::test_a_key_a_current_read_missed_reaches_its_consumer_once_restored`; `sim/test_replays.py::test_f41_a_consumer_that_read_a_removal_does_not_keep_the_key`. D56 (fail, retryable) is retired by D147 (Q5).

**SRC-6. A keyed source loaded as data must be able to say what it served.**
No loader, and a store that cannot serve it (no `version_column`, no
`path`): refused at registration. Read only as a `Ref`, it needs neither.
*From* D147. *Covered* `server/test_sources.py::test_registration_refuses_a_keyed_source_loaded_without_versions`.

**SRC-7. Built-in stores serve sources at their own versions.**
FileStore/S3Store: each object under `path` at its etag (local: inode,
mtime, size; `hash=True` hashes content). Postgres: each row at its
`version_column`. An unkeyed function may return a bare value (caveat: a
revert between commit and read can leave a consumer one revision ahead).
*From* D147. *Covered* `server/test_sources.py::test_a_file_source_serves_objects_at_the_stores_version`; `server/test_sources.py::test_an_unkeyed_file_source_serves_the_objects_bytes`; `server/test_sources.py::test_a_table_source_serves_rows_at_their_version_column`; `server/test_sources.py::test_an_unkeyed_function_source_serves_its_value`.

**SRC-8. Bumping a loader's `version=` owes every key as updated.**
`@source(version="1")` → `"2"`: every consumer owes every key.
*From* D147. *Covered* none found (Q9).

**SRC-9. `Source(copy=True)` copies each changed key into a Solera store at commit.**
*From* D146, D147. *Covered* none: not built (Q9).

**SRC-10. A sensor tick is all or nothing.**
A tick's source commits, requested runs and cursor land together. No
change and no new cursor: nothing recorded. A new cursor alone advances
without waking anyone. A tick that saw a source another client moved
since is refused whole. A raising tick keeps its cursor. A retried post
applies once; a tick's runs are submitted once.
*From* `lifecycle.md` §11. *Covered* `server/test_sensors.py::test_a_tick_commits_and_requests_runs_in_one_record`; `server/test_sensors.py::test_nothing_changed_records_nothing_and_a_cursor_alone_only_advances`; `server/test_sensors.py::test_a_tick_that_saw_a_source_since_moved_is_refused_whole`; `server/test_sensors.py::test_a_raising_body_fails_its_tick_and_keeps_the_cursor`; `server/test_sensors.py::test_a_retried_post_is_answered_again_and_applied_once`; sim `a_ticks_runs_are_submitted_once`.

**SRC-11. An observable source commits on its schedule.**
`Source("landing", observe=Every(300))`: `observe()` returning a version,
a full map, or `Observed(upsert, remove, cursor)` becomes that source's
commit.
*From* `per-key-processing.md` §12. *Covered* `server/test_sensors.py::test_observable_sources_commit_on_their_schedule`.

## KEY — per-key assets and key outcomes

**KEY-1. A per-key asset is called once per owed key; removed keys lose their rows without a call.**
`checks` over k1, k2 (changed) and k3 (removed): two calls; one store
write per output holds k1's and k2's rows and removes k3's.
*From* `per-key-processing.md` §5. *Covered* `server/test_each.py::test_one_call_per_key_many_rows_one_write`; `server/test_each.py::test_removed_keys_lose_their_rows`.

**KEY-2. `batch_size` is keys per commit; `concurrency` is keys at once.**
Defaults 10,000 and 64. `@asset(concurrency=4)` with per-key
`concurrency=64` is at most 4 × 64 calls. A cancel interrupts keys still
waiting for a slot like keys in flight.
*From* D111 (chain: D76 → D78 → D80 → D111). *Covered* `server/test_each.py::test_concurrency_and_batches`; `server/test_each_review.py::test_1_a_cancel_interrupts_keys_still_waiting_for_a_slot`.

**KEY-3. A key's error class decides its fate; one bad key never blocks the others.**
`Rejected` (an empty file): keeps its previous output, retried when its
input changes. `Failed` (any unclassified error): the same, plus one try
per new deploy. `Transient`: retried on backoff (1 min doubling to 6 h,
`retry_after` honoured) until `retry_for` (24 h), then failed. `Abort`:
the whole attempt fails, nothing commits.
*From* `per-key-processing.md` §8. *Covered* `server/test_each.py::test_failures_are_recorded_and_never_block`; `server/test_each.py::test_transient_key_retried_when_due`; `server/test_each.py::test_a_failed_key_gets_one_try_per_deploy`; `server/test_each.py::test_abort_fails_the_attempt_and_commits_nothing`; `sdk/test_key_outcomes.py::test_transition_table`; `sdk/test_key_outcomes.py::test_transient_backoff_and_budget`.

**KEY-4. Returning `None` changes nothing for that key; removing is explicit.**
A call returns `None` for `icp_raw_data`: its previous rows stay.
`Patch(None, remove=[ctx.key])` removes it. A key given no rows does not
exist.
*From* `per-key-processing.md` §16. *Covered* `server/test_each.py::test_none_is_no_change_and_removal_is_explicit`; `server/test_keys.py::test_a_key_given_no_rows_does_not_exist`.

**KEY-5. Every key of a per-key partition has a latest outcome.**
`rejected`, `failed`, `retrying`, `canceled`, `timed_out` (stored, with
tries, since, message); `ok` (processed, with the version it processed,
or null once upstream replaced it); `unmatched` (upstream has it, the
patterns leave it out); `removed` (upstream had it and lacks it now); or
none. Each row also says whether the key is owed: a.csv ok at v5 with
upstream at v6 is `ok`, owed. `GET /assets/{name}/outcomes` pages them,
filterable by outcome.
*From* D179. *Covered* `server/test_outcomes.py::test_derived_outcomes_follow_the_observation_record`; `server/test_outcomes.py::test_a_listing_pages_in_key_order`; `server/test_console_api.py::test_stored_outcomes_list_page_and_filter`; sim `stored_counts_sum_to_the_outcome_index`.

**KEY-6. Failed keys come back on their own, bounded.**
An automated per-key asset with keys due starts a run on its own when
the partition is idle; one run by hand picks them up on its next run.
No input head (a move reset it): the clock waits rather than looping.
*From* `per-key-processing.md` §9; F20. *Covered* `server/test_each.py::test_the_retry_clock_runs_automated_assets`; `server/test_sim_found.py::test_the_retry_clock_waits_for_an_input_with_no_head`.

**KEY-7. A forced retry calls each matching key once.**
`solera keys retry checks --rejected` calls every rejected key once, even
if its source went 1 → 2 → 1 in between.
*From* `per-key-processing.md` §16. *Covered* `server/test_each.py::test_forced_retry_takes_each_key_once`; `server/test_each.py::test_a_forced_retry_runs_after_its_source_reverts`; `server/test_each_review.py::test_6_a_forced_retry_during_the_last_retry_page_is_taken`.

**KEY-8. Retries and new changes alternate; a key that changed upstream comes with its change.**
1M keys failing after a deploy halve the pace of new files instead of
stopping them. A due key whose upstream changed is processed once, at
its new version.
*From* `per-key-processing.md` §9; F31. *Covered* `server/test_each.py::test_a_retry_pass_spans_batches_and_accumulates_its_bounds`; `server/test_each_passes.py::test_a_run_finishes_the_pass_it_began_before_it_ends`.

**KEY-9. A cancelled per-key batch commits its finished keys.**
Cancel while b and d run: a, c, e(removed) commit; b, d stay owed, so the
partition is stale and the next run of it, whatever starts it, processes
them. Reason `user`: they show `canceled`, and nothing starts a run for
them alone. Reason `timeout`: `timed_out`, a try counted, due after
backoff, `failed` past `retries=`. A user cancel during a timeout's drain
makes them `canceled`.
*From* `per-key-processing.md` §5; D139; Q15 (Erwin: cancelled keys need no machinery of their own). *Covered* `server/test_each.py::test_a_user_cancel_commits_finished_keys_and_leaves_the_rest_owed`; `server/test_each.py::test_a_timeout_drain_counts_a_try_and_comes_due`; `server/test_each_review.py::test_5_a_user_cancel_during_a_timeout_drain_makes_its_keys_canceled`.

**KEY-10. A full run starts the failing keys over.**
`checks` v1 leaves k1 failing; v2 makes a full run due: k1's failure goes
with the start-over.
*From* `per-key-processing.md` §9. *Covered* `server/test_each.py::test_a_start_over_clears_the_stored_outcomes`.

**KEY-11. "Why is this key not there?" has an answer.**
`GET /assets/checks/explain?key=k2`: `ok`, `failing`, `excluded` (naming
the rule), `not_matched`, `pending`, `removed` or `absent`, with
evidence.
*From* `per-key-processing.md` §10, §16. *Covered* `server/test_console_api.py::test_explain_says_why_a_key_is_or_is_not_there`; `server/test_console_api.py::test_explain_a_key_restored_after_its_removal_is_pending`.

## STA — staleness, completeness and statuses

**STA-1. A partition's status is one of six.**
In order of precedence: `removed` (no longer in the partition set),
`running`, `failed` (its last outcome), `stale`, `materialized`
(complete), `missing`. A built partition that is stale shows `stale` even
if incomplete.
*From* D177, D38. *Covered* `server/test_api.py::test_partitions_read_scope_records_not_task_history`; `server/test_api.py::test_failed_scope_reports_complete_after_success`; `server/test_console_api.py::test_assets_status_rolls_up_every_asset`. No `pending` status for now (STA-12).

**STA-2. A stale partition says every reason that holds.**
`input changed` (an input owes something), `upstream stale` (a partition
it reads is stale), `definition changed`. `copy`'s version bumped while
`items` is stale: both, in a stable order.
*From* D35, D140 (2). *Covered* `server/test_stale_reasons.py::test_a_partition_may_be_stale_for_several_reasons`; `server/test_staleness.py::test_a_stale_status_carries_every_reason_that_holds`; `server/test_staleness.py::test_a_reset_upstream_and_a_new_version_give_both_reasons`.

**STA-3. Stale means exactly: a default run would load something.**
Derived, never stored; one comparison answers both.
*From* D35, D42, D126. *Covered* `server/test_staleness.py::test_staleness_matches_the_reference_over_any_history` (property test); `ObservedSet.tla` StaleExact, OwedExact.

**STA-4. Staleness runs down the lineage.**
`feed` commits; `items` has not rerun: `items` is "input changed";
`copy`, `tally`, `checks` are "upstream stale". Once `items` reruns they
become "input changed"; once they rerun, all are fresh.
*From* `staleness.py` (K46; no ledger entry). *Covered* `server/test_stale_reasons.py::test_staleness_runs_down_the_lineage`; `server/test_staleness.py::test_staleness_is_transitive_down_a_chain`.

**STA-5. Roll-ups use "any".**
An asset is stale if any partition is; a per-key partition if any key is.
*From* D38, D40. *Covered* `server/test_console_api.py::test_assets_status_rolls_up_every_asset`; `server/test_console_api.py::test_a_domain_too_big_to_list_still_rolls_up`.

**STA-6. Stale keys are exact for per-key assets; other keyed outputs' keys go stale together.**
`checks`: only the keys owed, each with its own reason naming its input.
`copy` (keyed, not per-key): a change to k1 makes k1, k2, k3 stale. An
unkeyed output has no keys.
*From* D40, D43, D157. *Covered* `server/test_stale_reasons.py::test_stale_keys_follow_the_partition_and_say_why`; `server/test_staleness.py::test_a_stale_keys_reasons_are_its_own_and_name_their_input`; `server/test_staleness.py::test_a_non_each_keyed_outputs_keys_go_stale_together`.

**STA-7. A key neither side holds is never stale.**
`fchecks` processed k1 only; `feed` removes k2, which `fchecks` never
held: fresh.
*From* F35; D40. *Covered* `server/test_staleness.py::test_a_key_neither_side_holds_leaves_an_each_partition_fresh`; `server/test_staleness.py::test_a_key_neither_side_holds_is_never_stale`; `server/test_staleness.py::test_a_key_a_keys_run_never_held_leaves_fchecks_fresh_when_it_goes`.

**STA-8. An unkeyed partition stays stale until it reruns.**
`count` is built; `items` changes: `count` is stale through any number of
ticks until a run of it.
*From* D40. *Covered* `server/test_staleness.py::test_an_unkeyed_partition_stays_stale_until_it_reruns`.

**STA-9. Never built is `missing`, not stale.**
*From* D35, D177. *Covered* `server/test_complete.py::test_never_run_is_not_complete_and_a_first_run_is`.

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
*From* D177 (chain: D175 → D176 → D177). *Covered* `server/test_complete.py::test_never_run_is_not_complete_and_a_first_run_is`; `server/test_complete.py::test_an_incremental_run_canceled_midway_is_complete_and_stale`; `server/test_complete.py::test_a_full_run_canceled_midway_is_not_complete`; `server/test_complete.py::test_a_per_key_full_run_canceled_midway_is_complete`; `server/test_complete.py::test_a_keys_run_on_a_never_run_partition_is_not_complete`; `server/test_complete.py::test_a_keys_run_naming_every_upstream_key_is_complete`; `server/test_complete.py::test_a_partial_full_run_whose_rest_is_deleted_upstream_is_complete`; `server/test_complete.py::test_an_empty_upstream_or_patterns_taking_no_key_complete_at_once`; `server/test_complete.py::test_two_keyed_inputs_are_complete_together`; `server/test_complete.py::test_deletions_keep_it_complete`.

**STA-11. Completeness is live; history logs only whether a commit was its run's last batch.**
A three-batch run's commit rows say `final` false, false, true; a
cancelled run logs no `true`.
*From* D177. *Covered* `server/test_complete.py::test_each_commit_logs_whether_it_was_its_runs_last_batch`.

**STA-12. Freshness is computed exactly when asked; there is no `pending` status.**
After a prefixless pattern change on 100M keys, asking for the status runs
the full compare; it is never guessed and never shown as `pending`. A
`pending` status waits for a background staleness cache, which does not
exist yet.
*From* coordinator, rebuild step 5 (Q7; D157's `pending` deferred); review A27 R10. *Covered* `server/test_staleness.py::test_staleness_matches_the_reference_over_any_history`; `ObservedSet.tla` StaleExact.

## PAR — partitions, fan-in, broadcast, dynamic partitions

**PAR-1. Partition keys are canonical strings; an unpartitioned asset has one, never shown.**
`"Richmond"`, or `"day=2024-01-01,site=Richmond"` sorted by dimension.
Time windows are half-open and aligned in their timezone.
*From* D43. *Covered* `sdk/test_partitions.py::test_canonical_key_encoding`; `sdk/test_partitions.py::test_daily_keys`; `sdk/test_partitions.py::test_timezone_alignment`.

**PAR-2. Dimensions project: shared → same key, consumer-only → broadcast, upstream-only → fan-in.**
`report` (day) over `readings` (day × site) gets `dict[site, T]`. A
consumer-only dimension reads the same upstream partition for every
key. An incremental input may not fan in.
*From* `architecture.md` §7. *Covered* `server/test_engine.py::test_two_dimension_broadcast_and_collapse`; `server/test_engine.py::test_all_partitions_values`.

**PAR-3. A fan-in reads the complete heads that exist; it never waits.**
`report` for 2026-10-01 runs over Richmond and Oslo while Paris has no
head. To wait for all, `upstream=True`. With `skip_missing_inputs`, a
fan-in with no head at all is skipped.
*From* `architecture.md` §7, §9. *Covered* `server/test_planning.py::test_a_fan_in_reads_the_heads_that_exist`; `server/test_planning.py::test_an_empty_fan_in_is_missing`; `server/test_engine.py::test_an_automation_can_skip_until_its_inputs_are_written`; `server/test_matrix.py::test_a_change_is_kept_until_its_delivery_completes`.

**PAR-4. `all_partitions=True` reads every upstream partition, shared dimensions too.**
`site_report` for alpha compares alpha with every other site.
*From* `architecture.md` §5. *Covered* `server/test_matrix.py::test_all_partitions_reads_every_partition_the_shared_dimensions_too`; `server/test_planning.py::test_a_dep_reads_every_partition_with_all_partitions`.

**PAR-5. Dynamic partitions: re-listing changes nothing; new elements surface through `"missing"`.**
`sites` re-lists every site each cron run: no consumer wakes. A new site
is picked up by `Every(60, partitions="missing")`.
*From* `versions.md` §2; D161. *Covered* `server/test_engine.py::test_external_partition_set_via_commit`; `server/test_engine.py::test_missing_on_schedule_picks_up_new_keys`; `server/test_keys.py::test_partition_set_elements_ride_on_the_head`.

**PAR-6. A removed partition leaves fan-out and fan-in; its head stays readable.**
`sites` drops `west`: no run targets it, no fan-in reads it, its status is
`removed`, its last head can still be read.
*From* `architecture.md` §7. *Covered* `server/test_engine.py::test_retired_keys_leave_fanout`; `server/test_planning.py::test_a_fan_in_reads_only_current_partitions`. Its data's cleanup: Q10.

**PAR-7. Partition selections: `"latest"`, `"missing"`, `"all"`, a list.**
Counted before listed; past 100,000 refused. An explicit list never
enumerates the domain.
*From* `architecture.md` §8; D161. *Covered* `server/test_engine.py::test_partition_selections`; `server/test_planning.py::test_one_explicit_scope_never_enumerates_the_domain`; `server/test_matrix.py::test_an_explicit_selection_is_linear`.

**PAR-8. A source change reaches every partition of its consumer.**
`rates`, an unkeyed source (no dimensions), changes: every partition of
`site_report`, which reads it, owes a rerun.
*From* `architecture.md` §9. *Covered* `server/test_planning.py::test_a_source_change_fans_out_over_a_partitioned_consumer`.

**PAR-9. A sensor's requested runs see its own commits.**
A tick replaces `sites` `[old]` with `[new]` and asks for all partitions:
the run targets `new`.
*From* `lifecycle.md` §11. *Covered* `server/test_sensors.py::test_requested_runs_see_the_ticks_own_commits`.

## AUT — automations

**AUT-1. `OnChange` fires when an input's head changes, as one ordered run.**
An automation targets `copy` and `summary` (which reads `copy`); `items`
commits: one run, `summary` after `copy`, partitions projected from the
change.
*From* `architecture.md` §9. *Covered* `server/test_engine.py::test_onchange_fans_out_by_projection`; `server/test_planning.py::test_an_onchange_firing_is_one_run_in_order`.

**AUT-2. A change waits while the work it is owed is already queued or running.**
`copy` is running when `items` commits again: the firing waits and runs
after, so no change is consumed by work that could not see it.
*From* `architecture.md` §9. *Covered* `server/test_matrix.py::test_a_change_waits_for_work_already_queued`; `server/test_planning.py::test_a_change_during_a_run_is_kept_for_after_it`.

**AUT-3. A schedule waits for its next time, even the first.**
`Cron("0 6 * * *")` declared at 14:00 first fires tomorrow at 06:00. A
tick is skipped for a partition running or queued.
*From* D35 (1). *Covered* `server/test_engine.py::test_every_and_cron_fire`; `server/test_engine.py::test_every_skips_active_scope`; `server/test_engine.py::test_every_skips_queued_scope`.

**AUT-4. An asset change leaves its automations to decide; with none, it shows stale.**
Adding, re-adding, renaming, or changing an asset's definition: its
`OnChange` owes one firing per partition whose inputs have heads; a
schedule waits for its next time; with no automation, the asset shows
stale for a run by hand.
*From* D34 (pattern part amended by D140 (2), Q6), D33; F22. *Covered* `server/test_sim_found.py::test_an_onchange_asset_added_back_is_built`; `server/test_sim_found.py::test_an_asset_change_is_built_by_its_automation_or_marked_stale`; `server/test_sim_found.py::test_a_reset_output_is_due_for_a_rebuild`.

**AUT-5. `OnDeploy` fires once per new deploy.**
Restarting on the same deploy is silent; two deploys before a tick fire
once, for the latest.
*From* `architecture.md` §9. *Covered* `server/test_engine.py::test_ondeploy_fires_once_per_revision`; `server/test_engine.py::test_ondeploy_silent_on_restart_same_revision`; `server/test_engine.py::test_ondeploy_two_registrations_fire_latest_once`.

**AUT-6. A failing partition's changes back off.**
`site_status` fails the same way on every `site_events` commit: its
firings wait 60 s doubling to 1 h, reset by a success, so it stays
visibly failed instead of failing 7,500 times.
*From* D167; F43. *Covered* `server/test_engine.py::test_a_failing_partition_backs_off_its_changes`.

**AUT-7. With nothing manual, automated outputs converge on their sources.**
After quiet, every automated output equals what its sources imply; no
change stuck, none silently consumed.
*From* D60; `verification.md` ("Automations converge"). *Covered* sim `_converge` (Automations converge, A catch-up run converges); `Execution.tla` Quiesces, RunsEndCaughtUp.

## HIS — history and lineage

**HIS-1. Everything that finishes is in history, and reads the same once archived.**
Runs, tasks, attempts, output versions with metadata, and the input
versions each was built from; filters, facets, a timeline per run, where
each attempt ran.
*From* `architecture.md` §8. *Covered* `server/test_history.py::test_versions_carry_metadata_and_lineage`; `server/test_history.py::test_a_run_reads_the_same_once_archived`; `server/test_history.py::test_runs_filter_facets_and_pages`; `server/test_history.py::test_a_run_has_one_timeline`; `server/test_history.py::test_an_attempt_records_where_it_ran`.

**HIS-2. A task's wait counts only the time it could have run.**
A paused run or an engine outage is not wait.
*From* `architecture.md` §8. *Covered* `server/test_history.py::test_a_task_waits_only_while_it_could_run`.

**HIS-3. Lineage records the version actually read.**
FileStore: the pinned version. Postgres: the version whose write the read
saw, `uncommitted` if no commit made it (a dead writer's). A source read
live: the version of the commit the attempt was pinned to. `ctx.load` is
not lineage.
*From* `versions.md` §6. *Covered* `server/test_lineage_reads.py::test_lineage_says_what_a_current_read_saw`; `server/test_lineage_reads.py::test_an_external_tables_lineage_is_its_observation`; sim `reads_say_what_they_read`.

**HIS-4. A commit row names the batch that made it.**
`batch: {index, count}` and `final`; null for an unbatched task.
*From* D177, D173. *Covered* `server/test_complete.py::test_each_commit_row_names_the_batch_that_made_it`.

**HIS-5. While a run is in history, everything about it is answerable exactly, per key.**
Run R's batch 2 processed k1@7 and k2@3; a month later, R still retained:
"which version of k1 did batch 2 read, and what was its class?" →
k1@7, updated. Upstream key history is kept back to the oldest commit a
retained run or live reader refers to. A key outcome's `removed`, and its
processed version, stay known as long as their run.
*From* D180. *Covered* none: not built (T39; Q8).

**HIS-6. Per-key outcomes are logged per call, newest first, and expire with their run.**
*From* `per-key-processing.md` §10; D179. *Covered* `server/test_console_api.py::test_key_outcomes_page_newest_first`.

## CLN — cleanup and retention

**CLN-1. Runs expire by policy; current state never does.**
`Retention(days=7)`, `Retention(runs=90)`, `forever=True`, a project
default. A run goes once it is past the horizon of every asset it ran;
active runs never. Heads, keys, cursors and what was processed stand on
their own; data never expires.
*From* `object-store-state.md` §11. *Covered* `server/test_retention.py::test_keep_the_newest_runs`; `server/test_retention.py::test_runs_kept_forever_do_not_crowd_out_expired_ones`; `test_soak.py::test_soak_with_retention`.

**CLN-2. Runs can be deleted and pruned by hand; active ones cannot.**
*From* `object-store-state.md` §11. *Covered* `server/test_api.py::test_delete_and_prune_runs`; `server/test_retention.py::test_pruning_a_skipped_run_deletes_what_its_attempt_wrote`.

**CLN-3. A deleted run is gone for good, and its late worker writes nothing.**
*From* `object-store-state.md` §11. *Covered* `server/test_retention.py::test_a_run_retires_for_good_before_its_files_go`; `server/test_retention.py::test_a_deleted_run_takes_its_control_files_and_a_late_worker_writes_nothing`.

**CLN-4. Superseded versions go once nothing needs them.**
`items` rewrites k1 (FileStore, k1@g5 → k1@g9): k1@g5's object is deleted
once nothing that may still read it is in flight; a lagging consumer can
hold it back. A burst of commits makes few cleanup tasks.
*From* D168 (chain: D162 → D165 → D168), D144 (2). *Covered* `server/test_cleanup_cursor.py::test_superseded_generations_go_once_no_pin_needs_them`; `server/test_cleanup_cursor.py::test_an_observation_holds_the_cursor_back`; `server/test_cleanup_cursor.py::test_a_removed_keys_object_goes_and_a_later_re_add_stays`; `server/test_cleanup_cursor.py::test_a_burst_of_commits_makes_few_cleanup_tasks`; `server/test_collection.py::test_a_reader_pin_holds_collection_back`; sim `nothing_read_after_collection`.

**CLN-5. What an abandoned attempt wrote is cleaned up, late.**
An attempt dies after writing an object: it is deleted after the asset's
timeout plus the cancel grace.
*From* `lifecycle.md` §9.8. *Covered* `server/test_collection.py::test_an_abandoned_attempts_objects_go`; `server/test_key_orphans.py::test_an_abandoned_attempts_delta_is_collected_by_the_engine`.

**CLN-6. A removed, moved, or renamed-without-alias output's old data goes after `cleanup_after`.**
A week by default on every store (an undo window); an output may
override it. Added back within the week, its new data stays. A reader
pinned before the removal holds the cleanup back. A custom store the
project no longer declares leaves the cleanup stuck, saying so.
*From* D145, D96, D144 (2) (chain: D25 → D145). *Covered* `server/test_retirement.py::test_a_removed_outputs_files_go`; `server/test_retirement.py::test_a_moved_outputs_old_files_go_and_its_new_ones_stay`; `server/test_retirement.py::test_the_grace_period_is_the_stores_unless_the_output_says`; `server/test_retirement.py::test_a_store_that_says_nothing_keeps_a_removed_output_a_week`; `server/test_retirement.py::test_an_output_added_back_within_the_grace_period_keeps_its_new_data`; `server/test_retirement.py::test_a_cleanup_waits_for_a_reader_pinned_before_the_reset`; `server/test_retirement.py::test_a_custom_store_the_project_no_longer_declares_leaves_the_cleanup_stuck`.

**CLN-7. Cleanup runs in the background; a run is settled before its cleanup is.**
*From* D144 (2). *Covered* `server/test_collection.py::test_a_run_is_settled_before_its_cleanup_has_run`.

**CLN-8. A cleanup that keeps failing is stuck and shown, not retried forever.**
*From* `lifecycle.md` §9.8. *Covered* `server/test_collection.py::test_a_cleanup_task_that_keeps_failing_is_stuck_and_shown`.

**CLN-9. A store root belongs to one namespace.**
Staging's engine writing `s3://acme-solera/data`, owned by prod, is
refused naming both. `solera adopt-store` moves it, refused while prod is
live.
*From* D167; F43. *Covered* `server/test_engine.py::test_a_data_root_another_namespace_owns_is_refused`; `server/test_cli.py::test_adopt_store_moves_a_root_once_its_namespace_is_retired`; `test_demo_e2e.py::test_the_demo_keeps_its_data_beside_its_state`.

**CLN-10. Sensor tick rows last a day; a tick that committed lasts as long as its run.**
*From* `per-key-processing.md` §12. *Covered* `server/test_sensors.py::test_tick_rows_are_written_with_the_history_and_expire_after_a_day`.

**CLN-11. A paused consumer never loses its place to retention.**
`tally` paused 40 days under a 30-day window: when it runs, it owes k2
(updated), k3 (removed), k4 (added), not a full run.
*From* D123, D126 (`observed-set.md`, "A reader paused 40 days"). *Covered* none: not built; today such a consumer gets a full run (`server/test_keys.py::test_a_consumer_below_the_cut_starts_over`) (Q12).

## ENG — engine crashes, restarts, zombies and fencing

**ENG-1. A launched attempt survives an engine restart, and commits once.**
The engine restarts while `copy`'s attempt computes: the new engine
adopts it (no relaunch) and commits its result.
*From* `lifecycle.md` §12. *Covered* `server/test_fence.py::test_a_restarted_engine_adopts_and_commits_a_launched_attempt`; `server/test_model.py::test_restart_keeps_launched_claims`; `server/test_fence.py::test_an_attempt_whose_launch_was_cut_short_is_resumed`; sim `one_end_per_attempt`.

**ENG-2. An attempt not yet durably launched leaves no trace.**
The engine is replaced before the launch is durable: no worker was told,
nothing was written, the next engine dispatches the task again.
*From* F26. *Covered* `server/test_control_file.py::test_an_engine_fenced_before_its_launch_is_durable_tells_no_worker`; `server/test_sim_found.py::test_a_pool_attempt_is_offered_only_once_its_launch_is_durable`; `Attempt.tla` NoOrphanWrite.

**ENG-3. Workers keep working through an engine outage.**
A pool worker finishes while the engine is down; the restarted engine
settles its result.
*From* `lifecycle.md` §1. *Covered* `server/test_placements.py::test_a_pool_attempt_that_ended_while_the_engine_was_down_settles`.

**ENG-4. A new engine fences the old one; nothing acknowledged is lost.**
A rolling deploy: B takes the namespace while A still runs. A stops acting
at once; every event A acknowledged survives; A never deletes B's files.
*From* D18, D86 (chain: D5 → D9 → D18); F7, F14, F15, F40. *Covered* `server/test_journal.py::test_a_new_engine_fences_the_old_one`; `server/test_fence.py::test_a_fenced_engine_halts_at_once_not_at_its_next_tick`; `server/test_fence.py::test_a_replaced_engine_stops_acting`; `server/test_sim_found.py::test_a_slow_new_engine_never_opens_without_acknowledged_events`; `server/test_keys.py::test_f40_a_zombies_orphan_collector_spares_the_serving_engines_layers`; `JournalObject.tla` NoAckedLoss, OneWriter.

**ENG-5. State is rebuilt from what was recorded alone.**
A replay of the recorded events equals the live engine.
*From* `object-store-state.md` §3. *Covered* `server/test_model.py::test_the_journal_alone_reproduces_the_live_model`; `server/test_journal.py::test_replay_restores_state`; sim `_converge` (the journal alone rebuilds the state).

**ENG-6. A worker started twice runs once.**
Kubernetes starts two pods for one attempt: the first to own it runs; the
other writes nothing, waits for the end, and its exit does not end the
attempt.
*From* `lifecycle.md` §4. *Covered* `server/test_lifecycle.py::test_a_duplicate_invocation_waits_for_the_owner_and_writes_nothing`; `server/test_lifecycle.py::test_a_duplicates_exit_does_not_end_the_owners_attempt`; `server/test_placements.py::test_pool_workers_race_for_a_claim`.

**ENG-7. A worker the engine gave up on cannot change what a newer one committed.**
A worker paused for a minute wakes after its attempt was ended and retried:
on Postgres its writes are refused; on FileStore they land under names
nothing reads. Heartbeats are evidence, never permission.
*From* `lifecycle.md` §6, §9.4. *Covered* `server/test_fence.py::test_an_aborted_worker_writes_nothing`; `server/test_control_file.py::test_a_worker_takes_the_gate_before_its_first_write`; sim `fenced_writes_hold_their_gate`; `Attempt.tla` NoWriteAfterNone, WritesInOrder, OneOutcome.

**ENG-8. A dead writer on a fenced store is repaired, on its own, within a budget.**
`orders`'s attempt dies mid-write on Postgres: the retry runs at once and
repairs (a key that landed counts as changed; one that did not, nothing).
Out of retries, the repair clock runs the partition up to 3 times, 60 s
apart doubling; then it stays "stuck" in `/repairs` until a run. Readers
of that partition wait for the repair.
*From* `lifecycle.md` §9.5; `versions.md` §5. *Covered* `server/test_fence.py::test_a_worker_that_dies_writing_leaves_its_output_unsettled_and_the_retry_repairs_it`; `server/test_fence.py::test_the_repair_clock_repairs_a_dead_writers_partition_on_its_own`; `server/test_fence.py::test_a_repair_that_always_fails_stops_after_its_budget`; `server/test_lifecycle.py::test_a_fenced_store_runs_its_retry_at_once`; `server/test_versions.py::test_a_repair_keeps_a_dead_writers_key_it_finds`; `server/test_versions.py::test_a_repair_drops_a_dead_writers_key_it_does_not_find`; `server/test_keys.py::test_a_patch_reconciles_what_a_dead_sql_writer_left`.

**ENG-9. Bad input fails one attempt or request, never the engine.**
A malformed control file or result fails its attempt, retryably. A run
config holding an integer past 64 bits is a 400. A malformed body is a
422.
*From* F27, F28, F30, F32. *Covered* `server/test_control_file.py::test_a_malformed_control_file_fails_its_attempt_and_is_ended`; `server/test_control_file.py::test_an_open_control_file_naming_a_worker_fails_its_attempt`; `server/test_fence.py::test_a_malformed_worker_result_is_settled_without_its_bad_parts`; `server/test_api.py::test_a_value_no_checkpoint_can_hold_is_a_400`; `server/test_api_bodies.py::test_a_malformed_body_on_a_raw_route_is_refused`.

**ENG-10. No hot loops.**
Between external inputs the engine's ticks, events and requests stay
within `burst + rate × elapsed`. No wake floor: each loop is fixed where
it is.
*From* D60, D81; F20. *Covered* sim "No hot loop" (`PACE`, `tests/sim/world.py`).

**ENG-11. A restart may stretch an attempt's timeout by one, never cut it short.**
*From* `lifecycle.md` §8. *Covered* `server/test_fence.py::test_an_adopted_deadline_trusts_the_launching_clock_within_bounds`.

**ENG-12. A sensor tick in flight at a restart is forgotten; the sensor is due again from its cursor.**
*From* `lifecycle.md` §11.5. *Covered* `server/test_sensors.py::test_late_and_pre_restart_ticks_get_409`.

**ENG-13. Writes wait while an output's upkeep is far behind.**
Past the bound, attempts writing that output are held ("held: merges"),
and a source commit to it is refused, retryable.
*From* D64, D71. *Covered* `server/test_keys.py::test_writes_wait_while_an_outputs_merges_are_far_behind`; `server/test_keys.py::test_every_index_writer_waits_while_merges_are_far_behind`; `server/test_keys.py::test_writers_held_by_backpressure_go_on_once_a_merge_publishes`.

**ENG-14. Upkeep that keeps failing backs off and alarms; it never stops for good.**
An output's merges fail three times: it waits 10 minutes, doubling,
alarmed, and tries again; an operator can clear the wait; merges cut short
by the engine's own stop count for nothing.
*From* D181 (Q13). *Covered* `server/test_keys.py::test_a_merge_that_keeps_failing_backs_off_and_resumes`; `server/test_keys.py::test_an_operator_clear_merges_at_once`; `server/test_keys.py::test_merges_the_engine_stops_count_for_nothing`.

**ENG-15. `SIGTERM` stops `solera serve` cleanly, exit 0.**
*From* commit `4275ca0` (no ledger entry). *Covered* `server/test_serve.py::test_a_sigterm_stops_it_cleanly`.

**ENG-16. A namespace from an older state format is refused, naming the fix.**
*From* D169. *Covered* `server/test_journal.py::test_a_namespace_of_another_state_format_is_refused`.

## DEP — deploys and reload

**DEP-1. A deploy is the manifest plus the code the project runs.**
Editing `helpers.py` (committed or not) is a new deploy; a note or data
file beside the project is not. `SOLERA_BUILD` wins when set. The engine
numbers deploys as it serves them.
*From* D174. *Covered* `sdk/test_build.py::test_the_build_is_the_code_the_project_runs`; `sdk/test_build.py::test_explicit_build_wins`; `sdk/test_build.py::test_the_deploy_number_counts_served_deploys`; `sdk/test_build.py::test_the_error_policy_changes_the_revision`.

**DEP-2. A local serve reloads on code changes; a broken edit keeps the current deploy.**
On by default for `--insecure` loopback serves, `--reload` elsewhere.
Saves are debounced; attempts in flight finish under their own deploy.
*From* D174. *Covered* `server/test_reloading.py::test_a_code_change_is_served_and_a_broken_one_is_not`; `server/test_reloading.py::test_redeploy_serves_another_deploy_in_place`.

**DEP-3. An attempt finishes under the deploy it was launched with.**
A new deploy (or a local reload) neither stops nor fails attempts in
flight. A worker whose code is another deploy than its attempt's fails
it, not retryably, saying why.
*From* D174; Q16; `architecture.md` §8. *Covered* `worker/test_worker.py::test_revision_mismatch_writes_failed_result`; `sdk/test_build.py::test_the_engine_warns_when_a_worker_computed_its_revision_another_way`.

**DEP-4. A sensor host on old code takes no ticks and restarts on the new.**
*From* `lifecycle.md` §11. *Covered* `server/test_sensors.py::test_a_host_on_another_revision_gets_no_ticks`; `server/test_sensors.py::test_a_host_on_old_code_waits_then_starts_afresh`.

**DEP-5. Migrations apply before an output's first write; a failed one is not retried.**
`solera migrate` applies eagerly and is idempotent; each schema's table
gets its own.
*From* `architecture.md` §4; F18. *Covered* `worker/test_worker.py::test_migrate_runs_before_first_write`; `worker/test_worker.py::test_failed_migration_is_not_retryable`; `server/test_cli.py::test_migrate_command_applies_and_is_idempotent`; `server/test_engine.py::test_an_unchanged_keyed_write_still_applies_its_migrations`.

**DEP-6. A secret written into a built-in store's config is refused at registration.**
`PostgresStore("postgresql://u:pw@…")` is refused, naming `env:NAME`.
*From* D96. *Covered* `server/test_retirement.py::test_a_built_in_store_with_a_secret_written_in_the_open_is_refused`.

## STO — fenced and immutable stores

**STO-1. A store is immutable or fenced; nothing else.**
Registration refuses another kind, or a store missing its kind's methods.
*From* `lifecycle.md` §9.4. *Covered* `sdk/test_store_conformance.py::test_shipped_stores_conform`; `sdk/test_store_conformance.py::test_the_kit_catches_a_store_that_forgets_its_fence`.

**STO-2. An immutable store's read returns exactly the pinned version.**
g5 writes {a:1}, a reader pins it; g9 writes {a:2}: the reader still
reads a:1.
*From* `stores.md`. *Covered* `sdk/test_store_conformance.py::test_shipped_stores_conform` ("a pinned read returns its version"); sim `committed_keys_are_readable`.

**STO-3. A batch on a fenced store is classed at the write its read saw.**
A batch planned at g12 reads Postgres after g13 committed: it is classed
at g13, so its classes and rows agree key by key, and lineage records 13.
A store that moved on since planning makes the batch replan, not fail.
*From* D144 (1); Q14; `stores.md`, "What a read sees". *Covered* `server/test_fenced_reads.py::test_a_commit_installed_between_plan_and_read_replans_once` (replans once, runs at the new head); `server/test_fenced_reads.py::test_a_long_upstream_write_holds_its_consumers_until_it_commits` (waits, no failed attempt); `server/test_fenced_reads.py::test_a_store_that_keeps_moving_fails_after_its_replan_window`; the generation read, `server/test_lineage_reads.py::test_lineage_says_what_a_current_read_saw`, sim `reads_say_what_they_read`.

**STO-4. On a fenced store a stale writer changes nothing.**
g5 writes; g9 acquires and writes; g5 writes again: refused. One
generation admits one worker. A newer writer waits for an older one's
open transaction.
*From* `stores.md` (invariants 6–8). *Covered* `sdk/test_store_conformance.py::test_shipped_stores_conform`; `sdk/test_store_machine.py::test_a_store_conforms_under_random_sequences`.

**STO-5. A fenced partition at rest holds exactly its keys.**
No attempt holding it and no repair owed: the table holds exactly the
keys the output lists, as written by its head.
*From* `verification.md`. *Covered* sim `a_fenced_scope_at_rest_holds_its_index_keys`.

**STO-6. Writes mean the same on every store.**
A replacement is the whole content; a patch changes only its keys; a key
with zero rows does not exist; many rows of one key are its group; the
same attempt writing again is one write.
*From* `stores.md` (invariants 1–2); `per-key-processing.md` §6. *Covered* `sdk/test_store_conformance.py::test_shipped_stores_conform`; `server/test_keys.py::test_a_key_given_no_rows_does_not_exist`; `server/test_each.py::test_one_call_per_key_many_rows_one_write`.

**STO-7. A key is stored as itself, or the write fails.**
A Postgres `numeric` key column would read `1.0` back as `1`: the write
fails. A `bytes` key fails the write rather than vanishing.
*From* `versions.md` §4. *Covered* `server/test_keys.py::test_a_byte_valued_key_fails_the_write_instead_of_vanishing`.

## Contradictions, gaps and open questions

Numbers are stable: entries refer to them.

### Open, for Erwin

6. **Do pattern changes still trigger the asset-change rule?** D34 lists
   patterns among asset changes (an `OnChange` firing owed at the deploy);
   D140 (2) makes them an input change. Does narrowing `include` still owe
   a firing, or only make the partition stale? AUT-4, CHG-4.
10. **A removed dynamic partition's data.** `architecture.md` §7 keeps its
    heads read-only; `stores.md` lists `cleanup(o, partition=p)` "for a
    removed dynamic partition". Is it ever cleaned up, and after what
    grace? PAR-6.
11. **Is store config part of the definition?** `observed-set.md` lists
    "store version and config"; `architecture.md` §4 says config is
    deployment, not definition (D96 puts built-in store config in the
    manifest for cleanup only). Changing a DSN: stale or not? CHG-1.
12. **A paused consumer and retention.** `observed-set.md` decides a
    before-image so a consumer paused past the retention window owes only
    what changed (CLN-11); not built. Today: upstream history is kept for
    the oldest reader, so retention is unbounded by a paused consumer, and
    one below what is kept gets a full run. Which is the contract: a
    bounded window with exact catch-up, or unbounded history?
13. **Upkeep that keeps failing.** D181 (an agent decision, awaiting
    review) replaced A17 R8's "stop merging for the index's life" with a
    back-off; built in `a6ece2a`. Confirm it. ENG-14.
17. **Decisions awaiting review:** D178 (internal: keys read from the
    attempt spec, built in `ea2ec14`; dynamic partition list derived,
    built in `75539a9`; unkeyed lives) and D181.
    D76 and D162 were marked not okay and are superseded (D78, D168).

### Gaps: decided, not built

8. **D180 is not built.** Key-level lineage per batch and history back to
   the oldest retained run are T39. Today a key outcome's `removed` and
   processed version reach back only as far as upstream history happens to
   be kept. HIS-5.
9. **Sources: `copy=True` and loader version bumps** (D146, D147) have no
   implementation or test found. SRC-8, SRC-9.
18. **D173's console disclosure** is covered only by the console's
    Playwright tests, not checked against these entries. RUN-14.

### Resolved (T42)

1. **`Batch.full` vs `batch.reset`, `ctx.load()` in a full run.** D166
   wins: the first batch of a full run says `batch.reset`, and `ctx.load()`
   always returns what's materialized; built (rebuild step 6). INC-10,
   INC-11, SEL-9.
2. **Stale docs.** `architecture.md` §1–§10, `versions.md` §6–§8 and
   `per-key-processing.md` now state the contract and point here
   (`observed-set.md`, the key-index docs, `glossary.md` and
   `object-store-state.md` are held for the redesign).
3. **`keys=` forms.** `keys=` takes a list, `"all"`, or the default (what
   is owed); a full run is the run's `mode="full"`, not a `keys` value
   (Erwin, in the rebuild decisions); built (rebuild step 6). SEL-7, SEL-8.
4. **A reverted update** is delivered as updated (D156); `architecture.md`
   §5 is fixed. INC-4.
5. **A key missing from a source** is processed as absent: D147 retires
   D56; `versions.md` §6 is fixed. SRC-5.
7. **`pending`.** No `pending` partition status until a background
   staleness cache exists; statuses are computed exactly on demand
   (coordinator, rebuild step 5). STA-12.
14. **Fenced reads and classes.** D144 (1) stands: classes and rows agree
   key by key, and a store that moved on makes the batch replan, not fail;
   built (rebuild step 4b). STO-3.
15. **Cancelled per-key keys** stay owed, and any next run processes them:
   intended (Erwin: cancelled keys need no machinery). KEY-9.
16. **A deploy mismatch.** D174 wins: attempts in flight finish under their
   own deploy; a worker running other code fails its attempt, not
   retryably. DEP-3.

## Coverage summary

"Covered" means at least one test, sim invariant or TLA+ property cited
exists on `8fceb63`; "partly" entries cite a test of today's behaviour
where the decided one is not built.

| Area | Entries | Covered | Partly | None |
|---|---|---|---|---|
| INC incremental delivery | 14 | 14 | 0 | 0 |
| RUN runs, batches, attempts | 15 | 14 | 0 | 1 (RUN-14) |
| SEL keys=, all, full, reset | 12 | 12 | 0 | 0 |
| CHG definition, patterns, context | 12 | 12 | 0 | 0 |
| RST resets, moves, renames | 8 | 8 | 0 | 0 |
| SRC sources | 11 | 9 | 0 | 2 (SRC-8, SRC-9) |
| KEY per-key, key outcomes | 11 | 11 | 0 | 0 |
| STA staleness, completeness | 12 | 12 | 0 | 0 |
| PAR partitions | 9 | 9 | 0 | 0 |
| AUT automations | 7 | 7 | 0 | 0 |
| HIS history, lineage | 6 | 5 | 0 | 1 (HIS-5) |
| CLN cleanup, retention | 11 | 10 | 0 | 1 (CLN-11) |
| ENG crashes, fencing | 16 | 16 | 0 | 0 |
| DEP deploys, reload | 6 | 6 | 0 | 0 |
| STO stores | 7 | 7 | 0 | 0 |
| **Total** | **157** | **152** | **0** | **5** |

## Appendix: entries to today's mechanisms

How `8fceb63` implements each area, for anyone running a new design
through the contract. Nothing above depends on it.

| Area | Mechanism today |
|---|---|
| INC, SEL, CHG, STA-3 | Per consumer partition and keyed input, an **observation record** (`solera_server/observed.py`): a base (a commit, or empty), ranges observed at a head, and points; it decodes to "what was processed". Owed keys are one comparison of decode with upstream now (`owed.py`), over the key index's Δ(P, H) and head scans. A batch writes a range `(prev, c] @ H` and points, then folds. Patterns and the whole/dep versions ("context") are stored per layer. |
| INC-8, RUN-5 | Each batch pins its head on its **claim** (a reader pin); a task's **progress** (last batch's index and end key) is run state, recovered from its commits. |
| SEL-8, CHG-1, RST-1 | A full run is due when the partition's stored `definition` differs from the asset's, or the record's layers name an old **life** of the upstream index; the first commit resets the record to an empty base (a per-key consumer: its own output index). |
| CHG-3 | The commit check: an attempt's planned life and definition against the model's at `AttemptFinished`; `reset_at` per output and asset. |
| SRC | Loaders return `Loaded(row, version=)`; the worker passes `served`; classes are corrected from served versions before the producer. |
| KEY | The **outcome index**, a key index per per-key asset partition holding non-ok outcomes; `ok`, `unmatched`, `removed` derived from the observation record and the upstream index (`outcomes.py`). |
| STA | `staleness.py` (reasons, transitive walk), `planning.complete` / `observed.gaps` (complete: an existence check in the empty base's gaps). |
| HIS | Parquet tables under `history/`, DuckDB queries (`history.py`, `lake.py`); lineage from each result's `read` generations. |
| CLN | A per-output-partition **cleanup cursor** walking deltas' replaced generations (D168); cleanup tasks; collection gated by reader pins and the oldest observation; whole-output cleanup entries from the deploy diff. |
| ENG | The **journal head** (one object, compare-and-swap) with a fencing epoch per engine; the attempt **control file** (`open → owned → writing → sealed | ended`); heartbeats over HTTP; the repair clock. |
| STO | `writes = "immutable"` (names carry the generation) or `"fenced"` (`acquire`, a fence row with `written`); `reads()` for current-row stores. |
| Key index | Stamped layers with minimal deltas (T33, D156); merges, a cut at the oldest reader, writer backpressure past 64 layers. |
