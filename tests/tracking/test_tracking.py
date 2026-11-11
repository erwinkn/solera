"""The tracking model (docs/verification.md, "The tracking model"): the decided
forms hold over random histories, every rule is needed,
and the model tells the design's own examples the way the design does."""

from __future__ import annotations

import os
import random

import pytest

from . import fullrun, oracle, staleness
from .core import ABSENT, NEG, POS, Log, within
from .histories import (
    ARCHIVE_LOADS_MID_REBUILD,
    CALIBRATIONS,
    DEAD_ROW,
    REPORT_IN_FLIGHT,
    REPORT_LOADS_MID_REBUILD,
)

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


def test_a_reader_holds_back_a_row_no_commit_recorded():
    assert fullrun.run(DEAD_ROW, "clear", "table") is None
    r = fullrun.run(DEAD_ROW, "clear", "table", frozenset({"hold_back"}))
    assert r is not None and "no commit" in r[1]


def test_an_empty_key_list_processes_nothing():
    w = oracle.replay_fullrun([("items", (("k1", True),)), ("keys", "copy", ()), ("commit", "copy")])
    assert oracle.holds(w, "copy") == {} and oracle.due(w, "copy") == ["k1"]


@pytest.mark.parametrize("rule", ["open_txn", "repair"])
def test_a_dead_writers_row_outlives_the_log_without_its_rule(rule):
    events = CALIBRATIONS[("fullrun", "clear", "table", rule)]
    assert fullrun.run(events, "clear", "table") is None
    r = fullrun.run(events, "clear", "table", frozenset({rule}))
    assert r is not None and r[1] == "at rest copy's log says {}, its store {k1@2}"


def test_lineage_needs_the_keys_a_key_list_names(monkeypatch):
    events = [("items", (("k1", True),)), ("step", "copy", 1), ("keys", "copy", ("k1",)), ("commit", "copy")]
    assert fullrun.run(events, "clear", "file") is None
    record = fullrun.World.record

    def unlisted(self, rec):
        rec.listed = {}
        record(self, rec)

    monkeypatch.setattr(fullrun.World, "record", unlisted)
    r = fullrun.run(events, "clear", "file")
    assert r is not None and "lineage" in r[1]


def test_cleanup_never_deletes_a_current_file(monkeypatch):
    events = [("items", (("k1", True),)), ("step", "copy", 1), ("cleanup", "copy")]
    assert fullrun.run(events, "clear", "file") is None

    def everything(self):
        self.output.deleted |= set(self.output.objects)

    monkeypatch.setattr(fullrun.Ordinary, "cleanup", everything)
    r = fullrun.run(events, "clear", "file")
    assert r is not None and "deleted" in r[1]


def test_lineage_needs_the_keys_a_repair_redid(monkeypatch):
    events = [
        ("items", (("k1", True),)),
        ("step", "copy", 1),
        ("keys", "copy", ("k1",)),
        ("write", "copy"),
        ("giveup", "copy"),
        ("step", "copy", 1),
    ]
    w = oracle.replay_fullrun(events, kind="table")
    assert w.records[-2].edge == "copy<0" and w.records[-2].digest == [("k1", 1, "unchanged")]
    record = fullrun.World.record

    def unlisted(self, rec):
        rec.listed = {}
        record(self, rec)

    monkeypatch.setattr(fullrun.World, "record", unlisted)
    r = fullrun.run(events, "clear", "table")
    assert r is not None and "lineage" in r[1]


@pytest.mark.parametrize(
    "events",
    [
        [
            ("items", (("k1", True),)),
            ("step", "copy", 1),
            ("step", "archive", 1),
            ("items", (("k1", True),)),
            ("step", "copy", 1),
            ("plan", "archive", 1),
            ("items", (("k1", False),)),
            ("plan", "copy", 1),
            ("write", "copy"),
            ("giveup", "copy"),
            ("commit", "archive"),
            ("items", (("k1", True),)),
        ],
    ],
    ids=["a dead removal"],
)
def test_a_reader_never_processes_what_no_commit_recorded(events):
    assert fullrun.run(events, "clear", "table") is None
    r = fullrun.run(events, "clear", "table", frozenset({"hold_back"}))
    assert r is not None and "no commit" in r[1]


def test_solera_never_reads_a_clear_the_journal_has_not_recorded():
    """Before D211 this history needed the hold-back: summary read copy's
    uncommitted clear in place. Now the clear's write opens a build that no
    run in the log names, so Solera and outsiders both read the old rows."""
    events = [("items", (("k1", True),)), ("step", "copy", 1), ("switch", "copy", True), ("full", "copy")]
    events += [("keys", "copy", ()), ("write", "copy"), ("summary",)]
    for off in (frozenset(), frozenset({"hold_back"})):
        assert fullrun.run(events, "clear", "table", off) is None
    w = oracle.replay_fullrun(events, kind="table")
    assert _versions(w.copy.output.rows) == _versions(w.copy.output.outside()) == {"k1": 1}


# -- a table's full run builds beside the published rows (D211) ---------------------
K4 = (("k1", True), ("k2", True), ("k3", True), ("k4", True))


def _versions(rows: dict) -> dict:
    return {k: r.version for k, r in sorted(rows.items())}


def test_outsiders_read_copys_full_run_all_old_then_all_new():
    """The brief's example: copy holds k1..k4; items then removes k3 and adds k5;
    copy runs full in batches of 2. Solera reads the build as it grows;
    a program outside Solera reads k1..k4 at their old versions until the
    store switches, then k1, k2, k4, k5."""
    w = oracle.replay_fullrun(
        [
            ("items", K4),
            ("step", "copy", 4),
            ("switch", "copy", True),
            ("step", "archive", 4),
            ("items", (("k3", False), ("k5", True))),
            ("full", "copy"),
            ("step", "copy", 2),
        ],
        kind="table",
    )
    out = w.copy.output
    old = {"k1": 1, "k2": 1, "k3": 1, "k4": 1}
    assert _versions(out.rows) == {"k1": 3, "k2": 3} and _versions(out.outside()) == old
    assert w.archive.held()
    w.apply(("step", "copy", 2))
    assert _versions(out.rows) == {"k1": 3, "k2": 3, "k4": 4, "k5": 4} and _versions(out.outside()) == old
    assert w.ended == {"copy": 3}  # the run ended in the log; the switch is owed
    w.apply(("switch", "copy", True))
    assert _versions(out.outside()) == {"k1": 3, "k2": 3, "k4": 4, "k5": 4}
    assert out.builds == {} and w.ended == {} and w.check() is None


def test_a_first_run_into_an_empty_partition_fills_in_place():
    """An output's first run is a full run of an empty output, and a partition
    holding no rows has no old rows to keep (Erwin, 2026-11-11): the run
    writes in place, outsiders see it fill, and its switch publishes
    nothing. The next full run finds rows and builds beside."""
    events = [("items", K4), ("step", "copy", 2)]
    w = oracle.replay_fullrun(events, kind="table")
    out = w.copy.output
    assert sorted(out.outside()) == ["k1", "k2"] and not out.builds and out.filling == 1
    events += [("step", "copy", 2), ("switch", "copy", True)]
    w = oracle.replay_fullrun(events, kind="table")
    out = w.copy.output
    assert sorted(out.outside()) == ["k1", "k2", "k3", "k4"] and out.filling is None
    assert out.in_place == {1} and not out.switched
    events += [("full", "copy"), ("step", "copy", 2)]
    w = oracle.replay_fullrun(events, kind="table")
    out = w.copy.output
    assert len(out.builds) == 1 and {r.version for r in out.outside().values()} == {1, 2}
    assert fullrun.run(events, "clear", "table") is None


def test_outsiders_may_see_only_the_filling_runs_rows():
    """Sol R8-2, Astra A10: while a partition fills, outsiders may see the
    rows of the run filling it, and of writes outside a full run, nothing
    else. A dead first run's rows stay; the next full run builds beside,
    and its build's rows leaked into the table are caught."""
    for removed in (True, False):  # Astra R1-2: a key the dead run and the build both hold
        events = [
            ("items", (("k1", True), ("k2", True), ("k3", True))),
            ("plan", "copy", 1),
            ("write", "copy"),
            ("restart",),
            *([("items", (("k1", False),))] if removed else []),
            ("plan", "copy", 1),
            ("write", "copy"),
        ]
        w = oracle.replay_fullrun(events, kind="table")
        out = w.copy.output
        assert out.filling == 1 and len(out.builds) == 1 and w.outsiders(w.copy) is None
        out.published.update(out.builds[max(out.builds)])
        assert "outsiders read copy as" in (w.outsiders(w.copy) or "")


def test_a_new_full_runs_clear_is_not_the_filling_runs_write():
    """Astra A10 (repros/model_reset_scope.py): a new full run's first write
    still names the run filling the partition (its ctx), but it is the new
    run's own: it builds beside, and its rows leaked into the table are
    caught. Only the filling run's clear and the writes that continue it
    are excused."""
    events = [
        ("items", (("k1", True), ("k2", True), ("k3", True))),
        ("step", "copy", 1),
        ("full", "copy"),
        ("plan", "copy", 2),
        ("write", "copy"),
    ]
    w = oracle.replay_fullrun(events, kind="table")
    out, s = w.copy.output, w.copy.pending
    assert s.version == 2 and s.clear and s.ctx == (1, None)
    assert out.filling == 1 and out.loose == {} and list(out.builds) == [2]
    assert w.outsiders(w.copy) is None
    out.published.update(out.builds[2])
    assert "outsiders read copy as" in w.outsiders(w.copy)


def test_a_release_publishes_the_partial_output():
    """D205's release ends the full run in the log (FullRunDone): outsiders and
    downstream alike then read the partial output, and the rest fills in place."""
    events = [
        ("items", K4),
        ("step", "copy", 4),
        ("switch", "copy", True),
        ("full", "copy"),
        ("step", "copy", 2),
        ("cancel", "copy"),
        ("release", "copy"),
        ("switch", "copy", True),
    ]
    w = oracle.replay_fullrun(events, kind="table")
    assert sorted(w.copy.output.outside()) == ["k1", "k2"] and not w.archive.held()
    assert fullrun.run(events, "clear", "table") is None


def test_a_switch_a_newer_full_run_overtook_is_skipped():
    """The engine crashed before copy's switch; a newer full run then
    journaled its clear, which drops the older run's debt. The late switch
    publishes nothing: outsiders go from the old rows straight to the newer
    run's result."""
    events = [
        ("items", (("k1", True), ("k2", True))),
        ("step", "copy", 2),
        ("switch", "copy", True),
        ("full", "copy"),
        ("step", "copy", 2),
        ("full", "copy"),
        ("step", "copy", 1),
        ("switch", "copy", True),
    ]
    w = oracle.replay_fullrun(events, kind="table")
    out = w.copy.output
    assert _versions(out.outside()) == {"k1": 1, "k2": 1} and w.ended == {}
    w.apply(("step", "copy", 1))
    w.apply(("switch", "copy", True))
    assert _versions(out.outside()) == {"k1": 3, "k2": 4} and out.switched == {3: 1}
    assert out.superseded == {2} and out.in_place == {1}  # the first run filled the empty partition
    assert fullrun.run(events, "clear", "table") is None


@pytest.mark.parametrize(
    "rule, check",
    [
        ("swap", "outsiders read"),  # (i)
        ("first_full", "outsiders read"),  # (i)
        ("solera_build", "log says"),  # (ii)
        ("switch_commit", "log says"),  # (ii): the build a run left unowed is read by no one
        ("switch_alone", "log says"),
        ("switch_release", "log says"),
        ("switch_owed", "log says"),
        ("notice_match", "log says"),
        ("build_keep", "outsiders read"),  # (i)
        ("switch_quiet", "outsiders read"),
        ("switch_overtaken", "outsiders read"),
        ("byref_wait", "by reference"),
        ("switch_result", "log says"),
        ("reset_replay", "times"),  # (iii)
        ("switch_cleanup", "of an older build"),  # (iv)
    ],
)
def test_each_check_on_outside_readers_bites(rule, check):
    events = CALIBRATIONS[("fullrun", "clear", "table", rule)]
    r = fullrun.run(events, "clear", "table", frozenset({rule}))
    assert r is not None and check in r[1]


def test_a_canceled_full_run_asked_for_by_hand_leaves_nothing_in_the_way():
    """Its first write opened a build and died; the person cancels, so the next
    run is ordinary. Its write lands in the user's table, where the log's state
    is, and drops the build no run in the log names (Sol review of #37, 1)."""
    events = [("items", (("k1", True), ("k2", True))), ("step", "copy", 2), ("switch", "copy", True)]
    events += [("full", "copy"), ("plan", "copy", 1), ("write", "copy"), ("cancel", "copy")]
    events += [("items", (("k1", True),)), ("step", "copy", 1)]
    w = oracle.replay_fullrun(events, kind="table")
    assert not w.copy.full_due and w.copy.output.builds == {}
    assert _versions(w.copy.output.outside()) == {"k1": 3, "k2": 1} == _versions(w.copy.output.rows)
    assert fullrun.run(events, "clear", "table") is None


def test_a_reset_replayed_after_its_switch_writes_in_place():
    """A one-batch full run is published, then its write transaction is
    delivered again at the same version: it rewrites the same rows in the
    user's table instead of opening a build no run names, which would only
    go at the next write (Sol review, 2). Neither Solera nor outsiders would
    read that build: writes and loads follow the log's runs."""
    events = [("items", (("k1", True),)), ("step", "copy", 1), ("switch", "copy", True), ("replay", "copy")]
    w = oracle.replay_fullrun(events, kind="table")
    assert w.copy.output.builds == {} and _versions(w.copy.output.outside()) == {"k1": 1}
    assert fullrun.run(events, "clear", "table") is None
    r = fullrun.run(events, "clear", "table", frozenset({"reset_replay"}))
    assert r is not None and "that no run in the log names" in r[1]


def test_a_replayed_write_carries_what_its_repair_added():
    """A store call delivered twice sends the same payload: the retry that
    repaired k3 writes k3 again on replay, so the commit that follows records
    both keys it holds (Sol review 3 of #37, 1)."""
    events = [("step", "copy", 2), ("items", (("k2", True), ("k3", True)))]
    events += [("plan", "copy", 2), ("write", "copy"), ("giveup", "copy")]
    events += [("plan", "copy", 1), ("write", "copy"), ("replay", "copy"), ("commit", "copy")]
    w = oracle.replay_fullrun(events, kind="table")
    assert w.copy.output.log.states[-1] == {"k2": 3, "k3": 3} == _versions(w.copy.output.rows)
    assert fullrun.run(events, "clear", "table") is None


def test_an_input_taken_by_reference_waits_for_the_switch():
    """STO-8's example, read by an asset that queries copy's table itself: the
    run has ended in the log, but until the switch the table holds the old
    rows, so the gate holds the reader; after the switch it reads the new
    ones (Fable review of #37, H1)."""
    k4 = (("k1", True), ("k2", True), ("k3", True), ("k4", True))
    events = [("items", k4), ("step", "copy", 4), ("switch", "copy", True)]
    events += [
        ("items", (("k3", False), ("k5", True))),
        ("full", "copy"),
        ("step", "copy", 2),
        ("step", "copy", 2),
    ]
    events += [("summary",)]
    w = oracle.replay_fullrun(events, kind="table")
    assert w.copy.complete() and w.ended == {"copy": 2} and w.held["by reference"] == 1
    assert fullrun.run(events, "clear", "table") is None
    r = fullrun.run(events, "clear", "table", frozenset({"byref_wait"}))
    assert r is not None and "by reference" in r[1]
    events += [("switch", "copy", True), ("summary",)]
    w = oracle.replay_fullrun(events, kind="table")
    new = {"k1": 2, "k2": 2, "k4": 3, "k5": 3}
    assert w.held["by reference"] == 1 and _versions(w.copy.output.outside()) == new
    assert fullrun.run(events, "clear", "table") is None


def test_a_late_switch_call_keeps_the_debt():
    """The engine calls the switch once the run ended and the partition is
    quiet, but the call reaches the store after a newer batch wrote into the
    build: the store does nothing and says not published, so the engine keeps
    the debt and calls again (Fable review 2 of #37, M1)."""
    k4 = (("k1", True), ("k2", True), ("k3", True), ("k4", True))
    events = [("items", k4), ("step", "copy", 4), ("switch", "copy", True)]
    events += [
        ("items", (("k3", False), ("k5", True))),
        ("full", "copy"),
        ("step", "copy", 2),
        ("step", "copy", 2),
    ]
    events += [("switch", "copy", "delayed"), ("items", (("k1", True),)), ("step", "copy", 1), ("notice",)]
    w = oracle.replay_fullrun(events, kind="table")
    assert w.ended == {"copy": 2} and w.copy.output.switched[2] == 0
    assert fullrun.run(events, "clear", "table") is None
    r = fullrun.run(events, "clear", "table", frozenset({"switch_result"}))
    assert r is not None and "copy's log says" in r[1]


def test_a_write_context_never_names_two_full_runs():
    """The run in progress and the run owed a switch are never both set: a
    newer run's clear drops the older debt, so B8's write context carries one
    field (Fable review of #37, low 6)."""
    for seed in range(300):
        w = fullrun.World("clear", "table")
        for ev in fullrun.history(random.Random(seed), 60):
            w.apply(ev)
            assert None in w.copy.runs() and None in w.tally.runs()


def test_a_late_notice_of_an_older_switch_keeps_the_newer_one_owed():
    """The hook for run A returns, run B ends, then FullRunNoticed(A) is
    journaled: B's switch stays owed (Sol review, 6)."""
    events = [("items", (("k1", True),)), ("step", "copy", 1), ("switch", "copy", "late")]
    events += [("full", "copy"), ("step", "copy", 1), ("notice",)]
    w = oracle.replay_fullrun(events, kind="table")
    assert w.ended == {"copy": 2}
    assert fullrun.run(events, "clear", "table") is None
    r = fullrun.run(events, "clear", "table", frozenset({"notice_match"}))
    assert r is not None and "copy's log says" in r[1]


def test_outsiders_are_checked_at_once_not_only_at_rest():
    """Without the build, the uncommitted first write of a full run over rows
    shows outsiders rows no commit recorded, and the check after that very
    step says so (Sol review, 4)."""
    w = fullrun.World("clear", "table", frozenset({"swap"}))
    events = [("items", (("k4", True), ("k5", True))), ("step", "tally", 2), ("switch", "tally", True)]
    for ev in [*events, ("full", "tally"), ("plan", "tally", 1), ("write", "tally")]:
        assert w.check() is None
        w.apply(ev)
    assert "outsiders read tally" in (w.check() or "")


def test_an_unrelated_step_does_not_let_a_first_run_show():
    """A step before the first run must not count the output as having run
    (Sol review, 5)."""
    events = [("summary",), *CALIBRATIONS[("fullrun", "clear", "table", "first_full")]]
    r = fullrun.run(events, "clear", "table", frozenset({"first_full"}))
    assert r is not None and "outsiders read" in r[1]


def test_every_write_keeps_the_builds_the_log_names():
    """One deletion rule for every write: keep the builds the context names
    (the run in progress and the run owed a switch) and the reset's own new
    build; drop the others (Sol review 2 of #37, 3)."""
    out = fullrun.Output("copy", "table")
    out.published = {
        "k0": fullrun.Row(0, ("z", None, 0), None)
    }  # the partition holds a row: runs build beside
    out.write(1, {"k1": ("a", None, 0)}, True, 1)  # run 1 opens build 1
    out.write(2, {"k1": ("b", None, 0)}, True, 2, (None, 1))  # run 1 ended, owed; run 2 opens build 2
    out.write(3, {"k1": ("c", None, 0)}, True, 3, (2, 1))  # run 2 in progress; a reset opens build 3
    assert sorted(out.builds) == [1, 2, 3]
    out.write(4, {"k2": ("d", None, 0)}, False, 3, (3, 1))  # run 3's clear journaled: build 2 goes
    assert sorted(out.builds) == [1, 3] and sorted(out.builds[3]) == ["k1", "k2"]


@pytest.mark.parametrize(
    "end", [("items", (("k2", False),)), ("release", "copy")], ids=["done with no batch", "released"]
)
def test_a_switch_waits_until_no_dead_write_is_left_in_its_build(end):
    """copy's attempt writes into the build of its second full run (the
    partition holds rows) and is given up on; the run then ends without
    it. The switch waits until the next attempt repaired the build, so
    outsiders never read the dead row (Sol review 2 of #37, 1)."""
    events = [("items", (("k1", True), ("k2", True))), ("step", "copy", 2), ("switch", "copy", True)]
    events += [("full", "copy"), ("step", "copy", 1), ("plan", "copy", 1)]
    events += [("write", "copy"), ("giveup", "copy"), end, ("switch", "copy", True)]
    w = oracle.replay_fullrun(events, kind="table")
    assert w.ended == {"copy": 2} and w.copy.output.switched == {}
    assert fullrun.run(events, "clear", "table") is None
    r = fullrun.run(events, "clear", "table", frozenset({"switch_quiet"}))
    assert r is not None and "outsiders read" in r[1]


def test_a_first_run_done_in_one_batch_shows_whole_even_without_its_build():
    """Negative control for first_full: a first run that completes in one
    batch never shows outsiders a partial table, so its ablation passes here
    (Sol review 2 of #37, 2)."""
    events = [("items", (("k2", True), ("k3", True))), ("step", "copy", 2)]
    assert fullrun.run(events, "clear", "table", frozenset({"first_full"})) is None
