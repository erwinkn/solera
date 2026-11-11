"""Retention and cleanup through the bindings (B9): the cut and what holds
it, the alarm, cleanup's steps and what they release, merges, orphan layer
files, retiring an output. Each is one call on a Tracking."""

import pytest
from solera._native import (
    ChangeIndex,
    Tracking,
    encode_changes,
    retain_alarms,
    retain_cuts,
    retain_event_retire,
    retain_event_track,
    retain_merge,
    retain_orphans,
    retain_record_pins,
    retain_released,
    retain_step,
)


def commit(t: Tracking, upserts, removes=(), *, version: int) -> dict:
    built = t.build_commit(
        b"items",
        encode_changes(list(upserts), removes=list(removes)),
        version=version,
        path=f"items/{version}",
    )
    t.apply(built["event"])
    return built


def released(t: Tracking, step: bytes, limit: int) -> list:
    out, after = [], None
    while True:
        page, after = retain_released(t, step, after=after, limit=limit)
        out += page
        if after is None:
            return out


@pytest.fixture
def t(tmp_path) -> Tracking:
    """items, cleaned from the start: a, b, c at version 10 (commit 1), then
    a at 20, b at 30, and c removed (commits 2 to 4)."""
    t = Tracking()
    t.attach(b"items", ChangeIndex(f"file://{tmp_path}"))
    t.apply(retain_event_track(b"items", 0))
    commit(t, ["a", "b", "c"], version=10)
    commit(t, ["a"], version=20)
    commit(t, ["b"], version=30)
    commit(t, [], ["c"], version=40)
    return t


def test_cleanup_releases_every_version_its_commits_replaced_a_page_at_a_time_then_moves_its_cursor(t):
    assert retain_cuts(t) == {b"items": (0, "cursor", None)}
    start, end, event = retain_step(t, b"items")
    assert (start, end) == (0, 4)
    assert released(t, event, limit=2) == [("a", 10), ("b", 10), ("c", 10)]
    _, _, to_2 = retain_step(t, b"items", [(b"items", 2, "attempt", b"a-1")])
    assert released(t, to_2, limit=10) == [("a", 10)]
    with pytest.raises(ValueError, match="at least one key"):
        retain_released(t, event, limit=0)
    t.apply(event)
    assert retain_cuts(t) == {b"items": (4, "head", None)}
    assert retain_step(t, b"items") is None
    with pytest.raises(ValueError, match="its cursor is Some"):
        retain_released(t, event)
    with pytest.raises(ValueError, match="its cursor is Some"):
        t.apply(event)
    with pytest.raises(ValueError, match="not a cleanup step"):
        retain_released(t, retain_event_track(b"items", 4))


def test_a_live_attempt_holds_cleanup_and_the_cut_at_the_head_it_planned_at(t):
    attempt = [(b"items", 2, "attempt", b"a-1")]
    start, end, event = retain_step(t, b"items", attempt)
    assert (start, end) == (0, 2)
    t.apply(event)
    assert retain_cuts(t, attempt) == {b"items": (2, "cursor", None)}
    assert retain_cuts(t, [(b"items", 1, "attempt", b"a-0")]) == {b"items": (1, "attempt", b"a-0")}
    with pytest.raises(ValueError, match="run.*attempt"):
        retain_cuts(t, [(b"items", 1, "piece", b"e")])


def test_a_merge_folds_within_the_cut_and_the_files_it_replaced_become_orphans(t):
    t.apply(retain_step(t, b"items")[2])
    merged = retain_merge(t, b"items", "items/m1")
    assert merged is not None and merged["layer"]["floor"] == 4 and merged["cut"] == 4
    assert merged["replaced"] == [f"items/{v}" for v in (10, 20, 30, 40)]
    t.apply(merged["event"])
    listed = [*merged["replaced"], "items/m1", "items/stray", "items/building"]
    assert retain_orphans(t, listed, ["items/building"]) == [*merged["replaced"], "items/stray"]
    with pytest.raises(ValueError, match="no longer adjacent"):
        t.apply(merged["event"])


def test_a_merge_to_a_path_the_list_holds_is_refused_before_it_writes(t, tmp_path):
    t.apply(retain_step(t, b"items")[2])
    before = (tmp_path / "items" / "10").read_bytes()
    with pytest.raises(ValueError, match="written twice"):
        retain_merge(t, b"items", "items/10")
    assert (tmp_path / "items" / "10").read_bytes() == before


def test_a_merge_is_an_event_of_its_own(t):
    t.apply(retain_step(t, b"items")[2])
    merged = retain_merge(t, b"items", "items/m1")
    with pytest.raises(ValueError, match="an event of its own"):
        t.apply(Tracking.join([merged["event"], Tracking.event_edge_open(b"e", b"items", True)]))
    t.apply(merged["event"])


def test_a_merge_folds_within_the_cut_of_the_state_it_reads_and_the_engine_pins(t):
    merged = retain_merge(t, b"items", "items/m1")
    assert merged is not None and merged["cut"] == 0 and merged["layer"]["floor"] is None
    t.apply(retain_step(t, b"items")[2])
    attempt = [(b"items", 2, "attempt", b"a-1")]
    merged = retain_merge(t, b"items", "items/m2", attempt)
    assert merged is not None and merged["cut"] == 2


def test_the_alarm_names_the_run_whose_records_hold_the_cut(t):
    t.apply(retain_step(t, b"items")[2])
    run = [(b"items", 1, "run", b"monthly")]
    (alarm,) = retain_alarms(t, multiple=1, pins=run)
    assert alarm["output"] == b"items" and alarm["live"] == 2
    assert alarm["cut"] == (1, "run", b"monthly") and alarm["above"] == 3
    assert retain_alarms(t, multiple=2, pins=run) == []
    assert retain_alarms(t, multiple=0) == []


def test_a_consumer_holds_the_cut_at_its_piece_until_its_place_is_released(t):
    t.apply(Tracking.event_edge_open(b"e", b"items", True))
    plan = t.plan({"inputs": [{"edge": b"e", "conditions": {}}], "limit": 10})["plan"]
    event, records, _ = t.progress_ops(plan, [([], [])])
    t.apply(event)
    assert retain_record_pins(records[0], b"tally", b"r-7") == [(b"items", 4, "run", b"r-7")]
    commit(t, ["a"], version=50)
    t.apply(retain_step(t, b"items")[2])
    assert retain_cuts(t) == {b"items": (4, "piece", b"e")}
    t.apply(Tracking.event_reset(b"e", True))
    assert retain_cuts(t) == {b"items": (5, "head", None)}


def test_an_output_retires_once_nothing_reads_it_and_then_every_file_is_an_orphan(t):
    t.apply(Tracking.event_edge_open(b"e", b"items", True))
    with pytest.raises(ValueError, match="still read by edge e"):
        t.apply(retain_event_retire(b"items"))
    t.apply(Tracking.event_edge_close(b"e"))
    t.apply(retain_event_retire(b"items"))
    assert retain_cuts(t) == {}
    assert retain_orphans(t, ["items/10"], []) == ["items/10"]
