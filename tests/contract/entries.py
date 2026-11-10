"""What the contract's tests check, entry by entry (docs/behaviors.md).

A test checks the entry its name starts with: `test_inc_4_…` is INC-4's.
Every entry has at least one test, or a reason in `PENDING`.

`TARGET` lists the entries whose expected behaviour today's engine lacks:
those the new tracking core changes or adds (docs/tracking-design.md §16,
and the full-run and staleness verdicts), each with the decision. Their
tests assert the target, so today's path skips them; an entry whose rule
the core keeps, and which today's engine already passes, is not listed."""

TARGET: dict[str, str] = {
    # changed
    "INC-4": "D187",
    "KEY-3": "D191",
    "KEY-5": "D191",
    "KEY-10": "D194",
    "STA-3": "D191",
    "STA-12": "D183, D197",
    "STO-1": "D184, D196",
    "STO-2": "D184",
    "STO-3": "D184",
    "STO-4": "D196",
    "STO-5": "D196",
    "ENG-7": "D196",
    "ENG-8": "D196",
    "CLN-3": "D196",
    "CLN-5": "D196",
    "SRC-10": "D193",
    "HIS-3": "D184",
    "HIS-5": "D180",
    "HIS-6": "D195",
    "CLN-4": "D183",
    "CLN-6": "D183, D201",
    "CLN-11": "D192",
    "SEL-12": "the full-run verdict",
    "RUN-4": "D197",
    "PAR-6": "D201",
    # new
    "INC-15": "P7, the full-run verdict",
    "STA-13": "P12, the full-run verdict",
    "STA-14": "D183",
    "STA-15": "D191",
    "KEY-12": "D191",
    "CLN-12": "D183",
    "CLN-13": "D192",
    "CLN-14": "D198",
    "SRC-12": "D193",
    "ENG-17": "D196",
    "ENG-18": "D196",
}

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
