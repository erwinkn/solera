"""What the contract's tests check, entry by entry (docs/behaviors.md).

A test checks the entry its name starts with: `test_inc_4_…` is INC-4's.
Every entry has at least one test, or a reason in `PENDING`.

`CHANGED` lists the entries whose expected behaviour the new core changes
(docs/tracking-design.md §16), each with the decision that changes it. Their
tests assert the target, so today's path skips them."""

CHANGED: dict[str, str] = {
    "INC-4": "D187",
    "INC-8": "D196",
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
    "RST-8": "D180",
    "CLN-4": "D183",
    "CLN-6": "D183, D201",
    "CLN-11": "D192",
    "SEL-11": "§10",
    "SEL-12": "§10",
    "RUN-4": "D197",
    "AUT-4": "D200",
    "CHG-4": "D200",
    "CHG-1": "D202",
    "PAR-6": "D201",
}

PENDING: dict[str, str] = {
    **{
        entry: f"changed by docs/tracking-design.md §16 ({decision}); its test, written for the target, is next"
        for entry, decision in CHANGED.items()
    },
    "RUN-14": (
        "a rule of the console's display (D173): the HTTP API serves every batch and attempt, and"
        " which of them the console shows is checked by apps/console's Playwright tests"
    ),
    "ENG-2": "its window, an attempt's spec written but its launch not yet durable, lies inside one engine "
    "step: no request, restart or takeover a user makes can stop the engine there without hooking its flush",
    "ENG-16": "needs a namespace in an older state format, and the contract has none to open: making one "
    "means writing the engine's journal by hand, which no user does and the harness may not",
}
