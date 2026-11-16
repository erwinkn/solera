"""Histories that pin each rule of the tracking model: every one passes with
all of its model's rules on and breaks an invariant with its own rule off
(test_tracking.py). They were shrunk from random histories by

    uv run python -m tests.tracking staleness --calibrate --histories 6000
    uv run python -m tests.tracking fullrun --calibrate --histories 2000

except one written by hand: archive's batch in flight while copy's full run
clears a table. Keys are (model, store kind, rule)."""

CALIBRATIONS = {
    ("staleness", "file", "hold_back"): [
        ("up", (("k1", 1),)),
        ("plan", 2),
        ("commit", (), ()),
        ("up", (("k1", 2),)),
        ("plan", 2),
        ("clear", ()),
        ("commit", (), ("k1",)),
    ],
    ("fullrun", "file", "reset"): [
        ("step", "tally", 1),
        ("items", (("k1", True), ("k4", False))),
        ("items", (("k2", True), ("k4", True))),
        ("keys", "tally", ("k4",)),
        ("full", "tally"),
    ],
    ("fullrun", "file", "clear"): [
        ("items", (("k1", True), ("k4", False))),
        ("items", (("k2", True), ("k4", True))),
        ("step", "copy", 2),
        ("define", "copy"),
    ],
    ("fullrun", "file", "two_scans"): [
        ("define", "copy"),
        ("step", "copy", 2),
        ("items", (("k1", True), ("k4", True))),
        ("step", "copy", 1),
        ("define", "copy"),
        ("items", (("k3", True),)),
        ("items", (("k1", True),)),
        ("step", "archive", 2),
    ],
    ("fullrun", "file", "gate"): [
        ("items", (("k2", True), ("k3", True))),
        ("define", "copy"),
        ("step", "copy", 2),
        ("full", "copy"),
        ("step", "archive", 2),
        ("items", (("k1", True),)),
    ],
    ("fullrun", "file", "gate_accumulating"): [
        ("items", (("k1", True), ("k4", False))),
        ("items", (("k2", True), ("k4", True))),
        ("full", "tally"),
    ],
    ("fullrun", "file", "gate_whole"): [
        ("items", (("k2", True), ("k4", True))),
        ("step", "copy", 1),
        ("summary",),
    ],
    ("fullrun", "table", "hold_back"): [
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
    ("fullrun", "file", "counter"): [
        ("items", (("k3", True),)),
        ("step", "checks", 2),
        ("full", "checks"),
    ],
    # Stores that write in place, with late and dead writers (review F1, recs 1 to 5).
    ("fullrun", "table", "fence"): [("items", (("k1", True),)), ("plan", "copy", 2), ("restart",)],
    ("fullrun", "table", "open_txn"): [
        ("step", "copy", 1),
        ("items", (("k1", True),)),
        ("plan", "copy", 1),
        ("restart",),
        ("items", (("k1", False),)),
        ("late", "copy"),
    ],
    ("fullrun", "table", "repair"): [
        ("step", "copy", 1),
        ("items", (("k1", True),)),
        ("plan", "copy", 1),
        ("write", "copy"),
        ("giveup", "copy"),
        ("items", (("k1", False),)),
    ],
    ("fullrun", "table", "repair_removals"): [
        ("items", (("k1", True), ("k2", True), ("k3", True))),
        ("step", "copy", 3),
        ("items", (("k3", False),)),
        ("plan", "copy", 1),
        ("write", "copy"),
        ("giveup", "copy"),
        ("items", (("k1", True),)),
        ("step", "copy", 1),
    ],
    ("fullrun", "table", "repair_at_once"): [
        ("step", "copy", 1),
        ("items", (("k1", True),)),
        ("plan", "copy", 1),
        ("write", "copy"),
        ("giveup", "copy"),
        ("items", (("k1", False),)),
    ],
    ("fullrun", "table", "repair_handover"): [
        ("items", (("k1", True), ("k3", True))),
        ("step", "copy", 2),
        ("keys", "copy", ("k2", "k3")),
        ("write", "copy"),
        ("items", (("k1", False), ("k3", False))),
        ("giveup", "copy"),
        ("plan", "copy", 1),
        ("write", "copy"),
        ("items", (("k1", True), ("k2", True))),
        ("restart",),
    ],
    # The cut and exact cleanup (§13), and lineage (§12), on file stores.
    ("fullrun", "file", "cut_records"): [
        ("step", "copy", 1),
        ("items", (("k2", True), ("k3", True))),
    ],
    ("fullrun", "file", "cut_pieces"): [
        ("step", "tally", 1),
        ("retire",),
        ("items", (("k1", False), ("k3", True))),
        ("merge", "items"),
    ],
    ("fullrun", "file", "cut_cursor"): [("items", (("k1", False), ("k3", True)))],
    ("fullrun", "file", "cut_attempts"): [
        ("plan", "checks", 1),
        ("items", (("k1", True),)),
        ("merge", "items"),
    ],
    ("fullrun", "file", "cleanup_held"): [
        ("define", "copy"),
        ("define", "copy"),
        ("items", (("k2", True),)),
        ("step", "copy", 2),
        ("full", "copy"),
        ("plan", "archive", 2),
        ("step", "copy", 2),
        ("cleanup", "copy"),
    ],
    ("fullrun", "table", "load_check"): [
        ("step", "tally", 1),
        ("items", (("k2", True), ("k5", True))),
        ("keys", "tally", ("k5",)),
        ("items", (("k1", True),)),
        ("write", "tally"),
        ("restart",),
    ],
}

# archive loads k1 as a dead attempt of copy left it: a version no commit
# recorded. It holds k1 back (rule hold_back, F1 rec 4) until copy redoes it.
DEAD_ROW = [
    ("items", (("k1", True),)),
    ("step", "copy", 1),
    ("plan", "archive", 1),
    ("items", (("k1", True),)),
    ("plan", "copy", 1),
    ("write", "copy"),
    ("giveup", "copy"),
    ("commit", "archive"),
]

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

# A reader's batch loads while tally (report) or copy (archive) is mid rebuild
# and commits after the rebuild completes: the gate no longer holds it, so the
# hold must also cover a clear since the batch was planned (review, Fable 5.2).
REPORT_LOADS_MID_REBUILD = [
    ("items", (("k1", True), ("k3", True))),
    ("step", "tally", 2),
    ("step", "report", 2),
    ("items", (("k5", True),)),
    ("step", "tally", 2),
    ("plan", "report", 2),
    ("define", "tally"),
    ("step", "tally", 1),
    ("load", "report"),
    ("step", "tally", 2),
    ("step", "tally", 2),
    ("commit", "report"),
]
ARCHIVE_LOADS_MID_REBUILD = [
    ("items", (("k1", True), ("k2", True), ("k3", True))),
    ("step", "copy", 3),
    ("step", "archive", 3),
    ("feed", (("k3", True),)),
    ("step", "copy", 3),
    ("plan", "archive", 2),
    ("full", "copy"),
    ("step", "copy", 1),
    ("load", "archive"),
    ("step", "copy", 2),
    ("commit", "archive"),
    ("step", "archive", 2),
]
