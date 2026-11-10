"""The tracking model (docs/verification.md, "The tracking model"): the decided
forms hold over random histories, every rule is needed,
and the model tells the design's own examples the way the design does."""

from __future__ import annotations

import os
import random

import pytest

from . import fullrun, oracle, staleness
from .core import ABSENT, NEG, POS, Log, within
from .histories import ARCHIVE_LOADS_MID_REBUILD, CALIBRATIONS, REPORT_IN_FLIGHT, REPORT_LOADS_MID_REBUILD

HISTORIES = int(os.environ.get("SOLERA_TRACKING_HISTORIES", "200"))
SLOW = 5000


def _staleness(form: str, histories: int) -> None:
    for seed in range(histories):
        events = staleness.history(random.Random(seed), 40)
        r = staleness.run(events, form)
        assert r is None, f"seed {seed}, step {r[0]} {events[r[0]]}: {r[1]}"


def _fullrun(form: str, kind: str, histories: int) -> None:
    for seed in range(histories):
        events = fullrun.history(random.Random(seed), 60)
        r = fullrun.run(events, form, kind)
        assert r is None, f"seed {seed}, step {r[0]}: {r[1]}"


@pytest.mark.parametrize("form", staleness.RULES)
def test_a_staleness_form_answers_exactly(form):
    _staleness(form, HISTORIES)


@pytest.mark.parametrize("kind", ["file", "table"])
@pytest.mark.parametrize("form", fullrun.RULES)
def test_a_full_run_form_ends_where_it_should_and_misleads_no_reader(form, kind):
    _fullrun(form, kind, HISTORIES)


@pytest.mark.slow
@pytest.mark.parametrize("form", staleness.RULES)
def test_a_staleness_form_answers_exactly_long(form):
    _staleness(form, SLOW)


@pytest.mark.slow
@pytest.mark.parametrize("kind", ["file", "table"])
@pytest.mark.parametrize("form", fullrun.RULES)
def test_a_full_run_form_holds_long(form, kind):
    _fullrun(form, kind, SLOW)


def _run(key: tuple, off: frozenset[str]):
    model, form, kind, _ = key
    events = CALIBRATIONS[key]
    return staleness.run(events, form, off) if model == "staleness" else fullrun.run(events, form, kind, off)


@pytest.mark.parametrize("key", list(CALIBRATIONS), ids=lambda k: "-".join(k))
def test_each_rule_is_needed(key):
    assert _run(key, frozenset()) is None
    assert _run(key, frozenset({key[3]})) is not None


def test_every_rule_has_its_history():
    rules = {("staleness", f, r) for f, rs in staleness.RULES.items() for r in rs}
    rules |= {("fullrun", f, r) for f, rs in fullrun.RULES.items() for r in rs}
    assert {(m, f, r) for m, f, _, r in CALIBRATIONS} == rules


@pytest.mark.parametrize("form", fullrun.RULES)
def test_a_reader_of_an_accumulating_table_holds_back_what_its_batch_in_flight_finds_half_built(form):
    assert fullrun.run(REPORT_IN_FLIGHT, form, "table") is None
    r = fullrun.run(REPORT_IN_FLIGHT, form, "table", frozenset({"hold_back"}))
    assert r is not None and "unfinished rebuild" in r[1]


# -- the design's examples ---------------------------------------------------------
TALLY = [  # §3's story: tally reads items with batch_size=2
    ("up", (("k1", 1), ("k2", 1), ("k3", 1)), False),
    ("plan", 2),
    ("commit", (), ()),
    ("up", (("k2", 2), ("k4", 2)), False),
    ("plan", 2),
    ("commit", (), ()),
    ("up", (("k3", None), ("k5", 3)), False),
    ("up", (("k1", 4),), False),
    ("plan", 2),
    ("commit", (), ()),
    ("cancel",),
]


def test_tallys_witness_follows_the_amendment():
    witness = oracle.replay_staleness(TALLY)
    assert witness.prog.pieces[0][2][1] == 4 and witness.prog.pieces[1][2][1] == 2
    assert witness.form.state == ("stale", "k3")  # the amendment: "probe finds k3, removed at c3"
    assert oracle.due(witness) == ["k3", "k5"]


def test_a_revert_raises_no_alarm():
    events = [
        ("up", (("k1", 1),), False),
        ("plan", 2),
        ("commit", (), ()),
        ("up", (("k1", 2),), False),
        ("up", (("k1", 1),), False),
    ]
    for form in staleness.RULES:
        w = oracle.replay_staleness(events, form)
        assert w.form.answer() == ("fresh",) and oracle.due(w) == []


def test_archive_sees_a_full_run_of_copy_the_way_the_design_tells_it():
    """amendment-full-run.md's worked example, under the decided form (a clear): copy holds k1..k4, its definition
    changes, items has k1, k2, k4, k5 (k3 removed), batch_size=2."""
    start = [
        ("items", (("k1", True), ("k2", True), ("k3", True), ("k4", True))),
        ("step", "copy", 4),
        ("step", "archive", 4),
        ("items", (("k3", False), ("k5", True))),
        ("full", "copy"),
        ("step", "copy", 2),
    ]
    w = oracle.replay_fullrun(start)
    assert sorted(w.copy.output.rows) == ["k1", "k2"]  # the clear emptied copy
    assert w.archive.held()  # and the gate holds archive
    w.apply(("step", "copy", 2))
    assert sorted(w.copy.output.rows) == ["k1", "k2", "k4", "k5"]
    assert w.copy.complete() and w.copy.rebuild is None
    assert oracle.next_plan(w, "archive", 9)[0] == ["k1", "k2", "k3", "k4", "k5"]


def test_the_log_answers_from_entries_what_its_states_say():
    """get, scan and diff, computed from entries, equal the true states at every
    pair of commits and over ranges, across clears and reverts."""
    keys = ("k1", "k2", "k3", "k4")
    ranges = [(NEG, POS), (NEG, "k2"), ("k1", "k3"), ("k3", POS)]
    for seed in range(150):
        rnd = random.Random(seed)
        log = Log()
        for _ in range(16):
            changes = {k: rnd.choice((ABSENT, 1, 2, 3)) for k in rnd.sample(keys, rnd.randint(1, 3))}
            log.commit(changes, clear=rnd.random() < 0.15)
        for c in range(log.head + 1):
            now = log.states[c]
            assert {k: log.get(c, k) for k in keys} == {k: now.get(k) for k in keys}, (seed, c)
            for lo, hi in ranges:
                assert log.scan(c, lo, hi) == {k: v for k, v in now.items() if within(k, lo, hi)}, (seed, c)
                for c1 in range(c + 1):
                    then = log.states[c1]
                    want = {
                        k: (then.get(k), now.get(k))
                        for k in set(then) | set(now)
                        if within(k, lo, hi) and then.get(k) != now.get(k)
                    }
                    assert log.diff(c1, c, lo, hi) == want, (seed, c1, c, lo, hi)


def test_a_pattern_change_owes_only_the_keys_that_change_membership():
    w = oracle.replay_staleness(
        [("up", (("k2", 1), ("k3", 1), ("x1", 1)), False), ("plan", 6), ("commit", (), ()), ("pat", 2)]
    )
    assert oracle.due(w) == ["k3"]


def test_a_feed_removal_does_not_excuse_churn():
    events = list(CALIBRATIONS[("fullrun", "clear", "table", "hold_back")])
    events.insert(-1, ("feed", (("k3", False), ("k4", False))))
    assert fullrun.run(events, "clear", "table") is None
    assert fullrun.run(events, "clear", "table", frozenset({"hold_back"})) is not None


def test_the_oracle_refuses_keys_outside_the_models_key_space():
    with pytest.raises(ValueError, match="key space"):
        oracle.replay_staleness([("up", (("order-123", 1),), False)])
    with pytest.raises(ValueError, match="key space"):
        oracle.replay_fullrun([("keys", "copy", ("k9",))])
    for kind in ("up", "clear"):
        oracle.replay_staleness([(kind, (("k1", "v1"),), False)])


ALPHABET = [
    ("up", (("k1", 1),), False),
    ("up", (("k1", 2),), False),
    ("up", (("k1", None),), False),
    ("up", (("k2", 1),), False),
    ("up", (("k2", None),), False),
    ("up", (("x1", 1),), False),
    ("up", (("x1", None),), False),
    ("up", (("k1", 1),), True),
    ("up", (("k1", 2),), True),
    ("up", (("k1", None),), True),
    ("up", (("x1", None),), True),
    ("up", (("k2", 1),), True),
    ("clear", (("k1", 3),), False),
    ("clear", (), True),
    ("complete",),
    ("plan", 1),
    ("plan", 2),
    ("full", 1),
    ("keys", ("k1",)),
    ("keys", ("k2",)),
    ("commit", (), ()),
    ("commit", ("k1",), ()),
    ("commit", ("k2",), ()),
    ("commit", (), ("k1",)),
    ("commit", (), ("k2",)),
    ("cancel",),
    ("pat", 0),
    ("pat", 1),
    ("pat", 2),
    ("ctx",),
    ("defn",),
    ("held",),
    ("retry", "k1", True),
    ("retry", "k1", False),
    ("restart",),
]


def _exhaust(depth: int) -> int:
    """Every staleness history up to depth over a small alphabet; an event that
    does not apply ends its branch, since every longer history through it is
    one already checked."""
    seen = 0

    def dfs(prefix: list[tuple]) -> None:
        nonlocal seen
        w = staleness.World("witness")
        for ev in prefix[:-1]:
            w.apply(ev)
        before = len(w.trace)
        w.apply(prefix[-1])
        if len(w.trace) == before:
            return
        seen += 1
        bad = w.check()
        assert bad is None, f"{prefix}: {bad}"
        if len(prefix) < depth:
            for ev in ALPHABET:
                dfs([*prefix, ev])

    for ev in ALPHABET:
        dfs([ev])
    return seen


def test_every_staleness_history_of_four_events_answers_exactly():
    assert _exhaust(4) > 100_000


def test_a_change_of_conditions_or_a_reset_catches_up_first():
    for events in (
        [("pat", 1), ("up", (("x1", 1),), False), ("up", (("x1", None),), True), ("pat", 0)],
        [
            ("up", (("k1", 1),), False),
            ("plan", 2),
            ("commit", ("k1",), ()),
            ("up", (("k1", None),), True),
            ("held",),
        ],
    ):
        oracle.replay_staleness(events)


def test_a_failing_keys_outcome_goes_when_upstream_reverts():
    w = oracle.replay_staleness(
        [
            ("up", (("k1", 1),), False),
            ("plan", 2),
            ("commit", (), ()),
            ("up", (("k1", 2),), False),
            ("plan", 2),
            ("commit", ("k1",), ()),
            ("up", (("k1", 1),), False),
            ("up", (("k1", 2),), False),
        ]
    )
    assert oracle.due(w) == ["k1"]


def test_the_oracle_answers_nothing_due_for_a_held_reader():
    w = oracle.replay_fullrun(
        [("items", (("k1", True), ("k2", True), ("k3", True))), ("step", "copy", 3), ("full", "copy")]
    )
    assert oracle.due(w, "copy") == ["k1", "k2", "k3"] == oracle.next_plan(w, "copy", 3)[0]
    w.apply(("step", "copy", 1))
    assert (
        oracle.held(w, "archive")
        and oracle.due(w, "archive") == []
        and oracle.next_plan(w, "archive") == ([], POS)
    )
    s = oracle.replay_staleness([("up", (("k1", 1),), False), ("clear", (("k2", 2),), False)])
    assert oracle.held(s) and oracle.due(s) == []


@pytest.mark.parametrize(
    "events", [REPORT_LOADS_MID_REBUILD, ARCHIVE_LOADS_MID_REBUILD], ids=["report", "archive"]
)
def test_a_reader_that_loaded_mid_rebuild_holds_back_after_it_completes(events):
    assert fullrun.run(events, "clear", "table") is None
    assert fullrun.run(events, "clear", "table", frozenset({"hold_back"})) is not None


def test_a_key_list_counts_as_processed_on_every_input():
    w = oracle.replay_fullrun(
        [("items", (("k1", True),)), ("keys", "copy", ("k1",)), ("commit", "copy"), ("feed", (("k1", True),))]
    )
    assert w.copy.complete() and not oracle.held(w, "archive")


def test_a_failing_key_waits_on_the_version_it_loaded():
    w = oracle.replay_staleness(
        [("up", (("k1", 1),), False), ("plan", 2), ("up", (("k1", 2),), False), ("commit", ("k1",), ("k1",))]
    )
    assert oracle.due(w) == []
    w.apply(("up", (("k1", 3),), False))
    assert oracle.due(w) == ["k1"]


def test_a_failing_key_the_patterns_drop_loses_its_outcome():
    w = oracle.replay_staleness(
        [("up", (("k1", 1),), False), ("plan", 2), ("commit", ("k1",), ()), ("pat", 2), ("pat", 0)]
    )
    assert oracle.due(w) == ["k1"]


def test_a_removal_loaded_before_a_rebuild_is_not_the_rebuilds():
    events = [
        ("items", (("k1", True), ("k2", True))),
        ("step", "copy", 2),
        ("step", "archive", 2),
        ("items", (("k1", False),)),
        ("step", "copy", 2),
        ("plan", "archive", 2),
        ("load", "archive"),
        ("items", (("k1", True),)),
        ("step", "copy", 2),
        ("full", "copy"),
        ("keys", "copy", ("k2",)),
        ("commit", "copy"),
        ("commit", "archive"),
    ]
    assert fullrun.run(events, "clear", "table") is None


def test_an_empty_key_list_processes_nothing():
    w = oracle.replay_fullrun([("items", (("k1", True),)), ("keys", "copy", ()), ("commit", "copy")])
    assert oracle.holds(w, "copy") == {} and oracle.due(w, "copy") == ["k1"]
