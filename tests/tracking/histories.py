"""Histories that pin each rule of the tracking model: every one passes with
all of its form's rules on and breaks an invariant with its own rule off
(test_tracking.py). They were shrunk from random histories by

    uv run python -m tests.tracking staleness --calibrate --histories 6000
    uv run python -m tests.tracking fullrun --calibrate --histories 2000

except two written by hand: copy completing its rebuild with no batch (items
removes the keys it had not reached yet), and archive's batch in flight while
copy's full run clears a table. Keys are (model, form, store kind, rule)."""

CALIBRATIONS = {
    ("staleness", "counts", "file", "propagate"): [("up", (("k2", 3),), False)],
    ("staleness", "counts", "file", "since"): [
        ("plan", 3),
        ("commit", ("k3", "x1"), ("k3", "k5")),
        ("up", (("k2", 1),), False),
        ("up", (("k2", None), ("x1", 1)), False),
    ],
    ("staleness", "counts", "file", "clear"): [("clear", (("k1", 1), ("k3", None)), False)],
    ("staleness", "counts", "file", "split"): [
        ("up", (("x1", 3),), False),
        ("up", (("k1", None), ("k5", 1)), False),
        ("plan", 1),
        ("commit", ("k1", "k2"), ()),
    ],
    ("staleness", "counts", "file", "new_piece"): [
        ("up", (("x1", 3),), False),
        ("up", (("k1", None), ("k5", 1)), False),
        ("plan", 1),
        ("up", (("k2", 3),), False),
        ("commit", ("k1", "k2"), ()),
    ],
    ("staleness", "counts", "file", "new_piece_conditions"): [
        ("up", (("k1", 3), ("k3", 3)), False),
        ("plan", 1),
        ("ctx",),
        ("commit", ("x1",), ()),
    ],
    ("staleness", "counts", "file", "split_recount"): [
        ("up", (("k5", 2),), False),
        ("up", (("k4", 1), ("x1", 2)), False),
        ("plan", 2),
        ("pat", 2),
        ("commit", (), ()),
    ],
    ("staleness", "counts", "file", "conditions"): [
        ("up", (("k1", 3), ("k3", 3), ("x1", 3)), False),
        ("plan", 1),
        ("ctx",),
        ("commit", ("k5",), ()),
        ("pat", 2),
    ],
    ("staleness", "counts", "file", "reset"): [("up", (("k1", 2), ("k3", 3)), False), ("held",)],
    ("staleness", "counts", "file", "keys"): [
        ("up", (("k2", 3), ("k4", 3)), False),
        ("keys", ("k2",)),
        ("commit", ("k5",), ()),
    ],
    ("staleness", "counts", "file", "replay"): [
        ("up", (("k1", 2), ("k3", 3)), False),
        ("ctx",),
        ("up", (("x1", 3),), False),
        ("restart",),
    ],
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
    ("fullrun", "floor", "file", "reset"): [
        ("items", (("k1", True), ("k4", False))),
        ("items", (("k2", True), ("k4", True))),
        ("step", "copy", 2),
        ("define", "copy"),
    ],
    ("fullrun", "floor", "file", "floor_set"): [
        ("items", (("k1", True), ("k4", False))),
        ("items", (("k2", True), ("k4", True))),
        ("step", "copy", 2),
        ("define", "copy"),
    ],
    ("fullrun", "floor", "file", "load_floor"): [
        ("items", (("k1", True), ("k2", True))),
        ("step", "tally", 2),
        ("define", "tally"),
        ("step", "tally", 1),
    ],
    ("fullrun", "floor", "file", "leftovers"): [
        ("items", (("k2", True), ("k4", True))),
        ("step", "copy", 2),
        ("define", "copy"),
    ],
    ("fullrun", "floor", "file", "complete_alone"): [
        ("items", (("k1", True), ("k2", True), ("k3", True))),
        ("step", "copy", 3),
        ("full", "copy"),
        ("step", "copy", 1),
        ("items", (("k2", False), ("k3", False))),
    ],
    ("fullrun", "floor", "file", "floor_moves_up"): [
        ("full", "copy"),
        ("items", (("k1", True), ("k4", False))),
        ("items", (("k2", True), ("k4", True))),
        ("step", "copy", 2),
        ("define", "copy"),
    ],
    ("fullrun", "floor", "file", "gate_accumulating"): [
        ("items", (("k1", True), ("k4", False))),
        ("items", (("k2", True), ("k4", True))),
        ("full", "tally"),
    ],
    ("fullrun", "floor", "file", "gate_whole"): [
        ("items", (("k2", True), ("k4", True))),
        ("step", "copy", 1),
        ("summary",),
    ],
    ("fullrun", "floor", "table", "hold_back"): [
        ("items", (("k1", True), ("k2", False))),
        ("step", "tally", 2),
        ("items", (("k2", True), ("k4", True))),
        ("define", "tally"),
        ("plan", "report", 1),
    ],
    ("fullrun", "floor", "file", "counter"): [
        ("items", (("k3", True),)),
        ("step", "checks", 2),
        ("full", "checks"),
    ],
}

# report plans over tally, whose definition then changes; tally's full run
# rewrites the group rows of a table in place, and report's batch, planned
# before, loads a count the rebuild has only half built. Both forms need the
# hold (rule hold_back): the gate does not cover a batch already in flight.
REPORT_IN_FLIGHT = CALIBRATIONS[("fullrun", "floor", "table", "hold_back")]
