"""Histories that pin each rule of the tracking model: every one passes with
all of its form's rules on and breaks an invariant with its own rule off
(test_tracking.py). They were shrunk from random histories by

    uv run python -m tests.tracking staleness --calibrate --histories 6000
    uv run python -m tests.tracking fullrun --calibrate --histories 2000

except one written by hand: archive's batch in flight while copy's full run
clears a table. Keys are (model, form, store kind, rule)."""

CALIBRATIONS = {
    ("staleness", "witness", "file", "rule1"): [("up", (("k2", 3),), False)],
    ("staleness", "witness", "file", "rule2"): [
        ("up", (("k5", 1),), False),
        ("up", (("k1", 2), ("k5", None)), False),
    ],
    ("staleness", "witness", "file", "rule3_recheck"): [
        ("up", (("k1", None), ("k5", 1)), False),
        ("plan", 1),
        ("up", (("k2", 3),), False),
        ("commit", ("k1", "k2"), ()),
    ],
    ("staleness", "witness", "file", "rule3_range"): [
        ("up", (("k1", 1), ("k4", None)), False),
        ("plan", 2),
        ("pat", 2),
        ("commit", (), ("k3",)),
    ],
    ("staleness", "witness", "file", "rule4"): [("up", (("k1", 1), ("k4", None)), False), ("pat", 2)],
    ("staleness", "witness", "file", "rule5"): [
        ("up", (("k3", 2), ("k4", 2)), False),
        ("full", 3),
        ("commit", ("k4",), ()),
        ("retry", "k4", False),
    ],
    ("staleness", "witness", "file", "rule7"): [("up", (("k1", 1),), True)],
    ("staleness", "witness", "file", "rule8"): [("clear", (("k1", 1), ("k3", None)), False)],
    ("staleness", "witness", "file", "reset_probe"): [
        ("up", (("k1", 3), ("k3", 2), ("k4", 1)), False),
        ("plan", 3),
        ("commit", ("k1",), ()),
        ("full", 2),
        ("commit", (), ()),
    ],
    ("staleness", "witness", "file", "held_probe"): [
        ("up", (("k4", 1),), False),
        ("full", 3),
        ("commit", ("k1",), ()),
        ("held",),
    ],
    ("staleness", "witness", "file", "catch_up_first"): [
        ("up", (("k1", 1), ("k3", 3), ("k4", 3)), True),
        ("plan", 2),
        ("commit", ("k1",), ()),
    ],
    ("staleness", "witness", "file", "rearm"): [
        ("up", (("k4", 3), ("k5", None)), False),
        ("full", 3),
        ("commit", ("k4",), ("k5",)),
        ("retry", "k4", True),
        ("restart",),
    ],
    ("fullrun", "clear", "file", "reset"): [
        ("items", (("k1", True), ("k4", False))),
        ("items", (("k2", True), ("k4", True))),
        ("keys", "tally", ("k4",)),
        ("full", "tally"),
    ],
    ("fullrun", "clear", "file", "clear"): [
        ("items", (("k1", True), ("k4", False))),
        ("items", (("k2", True), ("k4", True))),
        ("step", "copy", 2),
        ("define", "copy"),
    ],
    ("fullrun", "clear", "file", "two_scans"): [
        ("define", "copy"),
        ("step", "copy", 2),
        ("items", (("k1", True), ("k4", True))),
        ("step", "copy", 1),
        ("define", "copy"),
        ("items", (("k3", True),)),
        ("items", (("k1", True),)),
        ("step", "archive", 2),
    ],
    ("fullrun", "clear", "file", "gate"): [
        ("items", (("k2", True), ("k3", True))),
        ("define", "copy"),
        ("step", "copy", 2),
        ("full", "copy"),
        ("step", "archive", 2),
        ("items", (("k1", True),)),
    ],
    ("fullrun", "clear", "file", "gate_accumulating"): [
        ("items", (("k1", True), ("k4", False))),
        ("items", (("k2", True), ("k4", True))),
        ("full", "tally"),
    ],
    ("fullrun", "clear", "file", "gate_whole"): [
        ("items", (("k2", True), ("k4", True))),
        ("step", "copy", 1),
        ("summary",),
    ],
    ("fullrun", "clear", "table", "hold_back"): [
        ("items", (("k1", True), ("k2", True), ("k3", True), ("k4", True))),
        ("step", "copy", 4),
        ("step", "archive", 4),
        ("feed", (("k3", True), ("k4", True))),
        ("step", "copy", 2),
        ("plan", "archive", 2),
        ("full", "copy"),
        ("step", "copy", 2),
        ("commit", "archive"),
    ],
    ("fullrun", "clear", "file", "counter"): [
        ("items", (("k3", True),)),
        ("step", "checks", 2),
        ("full", "checks"),
    ],
}

# report plans over tally, whose definition then changes; tally's full run
# clears its table and counts k1 alone, and report's batch, planned before,
# loads that count. The gate does not cover a batch already in flight: the
# hold (rule hold_back) does.
REPORT_IN_FLIGHT = [
    ("items", (("k1", True), ("k3", True), ("k5", True))),
    ("step", "tally", 3),
    ("items", (("k5", False),)),
    ("step", "tally", 2),
    ("plan", "report", 2),
    ("define", "tally"),
    ("step", "tally", 1),
    ("commit", "report"),
]
