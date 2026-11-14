"""What the contract's tests check, entry by entry (docs/behaviors.md).

A test checks the entry its name starts with: `test_inc_4_…` is INC-4's.
Every entry has at least one test, or a reason in `PENDING`."""

PENDING: dict[str, str] = {
    "RUN-14": (
        "a rule of the console's display (D173): the HTTP API serves every batch and attempt, and"
        " which of them the console shows is checked by apps/console's Playwright tests"
    ),
    "ENG-2": "its window, an attempt's spec written but its launch not yet durable, lies inside one engine "
    "step: no request, restart or takeover a user makes can stop the engine there without hooking its flush",
    "HIS-7": "no route serves a batch's input recomputed from its record, and a digest mismatch needs a "
    "batch record altered behind the engine, which no user can do: per-batch lineage is B10's",
    "HIS-8": "no route serves a batch's per-key lineage (each key planned at one version and loaded at "
    "another): B10 adds it; STO-3's tests check that such a key is processed as loaded",
    "RST-8": "what a removed consumer's retained runs keep is only observable as their batches' per-key "
    "lineage (HIS-5's question, asked of a removed asset), which no route serves: B10, with B9's cut",
    "ENG-16": "needs a namespace in an older state format, and the contract has none to open: making one "
    "means writing the engine's journal by hand, which no user does and the harness may not",
}


# The entries — or single tests, by name — that a later unit builds: skipped with the
# unit's name until it lands. A test whose run the engine refuses (`harness.NOT_YET`)
# skips on its own; these are the ones that would fail instead, or assert something a
# refused run never reaches.
LATER: dict[str, str] = {
    **dict.fromkeys(
        [
            "test_key_2_a_cancel_interrupts_keys_still_waiting_for_a_slot",
            "test_key_9_a_user_cancel_commits_finished_keys_and_leaves_the_rest_owed",
            "test_key_9_a_timeout_counts_a_try_comes_due_after_backoff_and_fails_past_retries",
            "test_key_9_a_user_cancel_during_a_timeout_drain_makes_its_keys_canceled",
        ],
        "B6: a canceled per-key batch does not commit its finished keys yet",
    ),
    "test_key_4_a_key_given_no_rows_does_not_exist": "B6: no route serves a key's outcome yet",
    "test_sta_5_a_domain_too_big_to_list_still_rolls_up": "B6: no route explains a key yet",
    **dict.fromkeys(["CLN", "RST-8", "PAR-6", "ENG-13", "ENG-14"], "B9: retention and upkeep"),
    **dict.fromkeys(["HIS-3", "HIS-5", "HIS-7", "HIS-8"], "B10: lineage"),
}
