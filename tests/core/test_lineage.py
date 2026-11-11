"""Lineage through the binding (B10, design §12): what a batch received,
recomputed from its record; a key's trace; flow; the honest answers."""

import pytest
from solera._native import ChangeIndex, Tracking, lineage_flow, lineage_input, lineage_trace

HOUR = 3_600_000


def commit(t: Tracking, ix: ChangeIndex, layers: list, entries: list, live: int) -> None:
    c = len(layers) + 1
    layers.append(ix.write_commit(f"items/{c}", c, entries, live_before=live))
    t._set_output(b"items", layers, c, live)


def batch(t: Tracking, report=([], [])) -> bytes:
    """Plans every owed key of edge e and commits it: its batch record."""
    plan = t.plan({"inputs": [{"edge": b"e", "conditions": {}}], "limit": 10})["plan"]
    event, records, _ = t.progress_ops(plan, [report])
    t.apply(event)
    return records[0]


@pytest.fixture
def tally(tmp_path):
    """items: k1, k2 at c1; tally's run 1 reads both; then k1 1 → 4 at c2."""
    ix = ChangeIndex(f"file://{tmp_path}")
    t = Tracking()
    t.attach(b"items", ix)
    layers: list = []
    commit(t, ix, layers, [("k1", 1, None), ("k2", 1, None)], 0)
    t.apply(Tracking.event_edge_open(b"e", b"items", True))
    first = batch(t)
    commit(t, ix, layers, [("k1", 4, 1)], 2)
    return t, ix, layers, first


def keys(answer: dict) -> list:
    assert answer["state"] == "found"
    return [
        (r["key"], r["class"], r["held"], r["planned"], r["loaded"] if r["deviated"] else "-")
        for r in answer["keys"]
    ]


def test_a_batch_received_what_its_record_recomputes_to(tally):
    t, _, _, first = tally
    assert keys(lineage_input(t, first)) == [("k1", "added", None, 1, "-"), ("k2", "added", None, 1, "-")]
    assert keys(lineage_input(t, batch(t))) == [("k1", "updated", 1, 4, "-")]


def test_a_deviation_names_the_version_loaded(tally):
    t, ix, layers, _ = tally
    plan = t.plan({"inputs": [{"edge": b"e", "conditions": {}}], "limit": 10})["plan"]
    commit(t, ix, layers, [("k1", 5, 4)], 2)
    event, records, _ = t.progress_ops(plan, [([], [("k1", 5)])])
    t.apply(event)
    got = lineage_input(t, records[0])
    assert keys(got) == [("k1", "updated", 1, 4, 5)]
    assert got["keys"][0]["reason"] == "deviation"


def test_a_failed_key_names_the_version_it_loaded(tally):
    t, ix, layers, _ = tally
    plan = t.plan({"inputs": [{"edge": b"e", "conditions": {}}], "limit": 10})["plan"]
    commit(t, ix, layers, [("k1", 5, 4)], 2)
    event, records, _ = t.progress_ops(plan, [(["k1"], [("k1", 5)])])
    t.apply(event)
    got = lineage_input(t, records[0], cleaned=3)
    assert keys(got) == [("k1", "updated", 1, 4, 5)]
    assert got["keys"][0]["reason"] == "failed"
    assert not got["keys"][0]["deleted"]  # c3 released k1@4, but the batch read k1@5


def test_a_version_cleanup_released_is_deleted(tally):
    t, _, _, first = tally
    rows = {r["key"]: r["deleted"] for r in lineage_input(t, first, cleaned=2)["keys"]}
    assert rows == {"k1": True, "k2": False}  # k1@1 was replaced at c2
    assert not any(r["deleted"] for r in lineage_input(t, first, cleaned=1)["keys"])


def test_a_version_written_again_is_not_deleted(tally):
    t, ix, layers, first = tally
    commit(t, ix, layers, [("k1", 1, 4)], 2)  # c3: k1 is at 1 again
    assert not lineage_input(t, first, cleaned=3)["keys"][0]["deleted"]
    commit(t, ix, layers, [("k1", 5, 1)], 2)  # c4 replaces it again
    assert not lineage_input(t, first, cleaned=3)["keys"][0]["deleted"]
    assert lineage_input(t, first, cleaned=4)["keys"][0]["deleted"]


def test_a_record_its_input_no_longer_recomputes_to_is_an_error(tally, tmp_path):
    _, _, _, first = tally
    ix = ChangeIndex(f"file://{tmp_path}/other")
    t = Tracking()
    t.attach(b"items", ix)
    commit(t, ix, [], [("k1", 1, None)], 0)  # items answers with another history
    with pytest.raises(ValueError, match="record says 2 keys"):
        lineage_input(t, first)
    with pytest.raises(ValueError, match="record says 2 keys"):
        lineage_trace(t, b"items", "k1", [(first, None)])


def test_a_record_past_its_output_head_is_an_error(tally, tmp_path):
    _, _, _, first = tally
    t = Tracking()
    t.attach(b"items", ChangeIndex(f"file://{tmp_path}/empty"))
    with pytest.raises(ValueError, match="whose index ends at 0"):
        lineage_input(t, first)


def test_a_key_trace_alternates_history_and_the_batches_that_held_it(tally):
    t, _, _, first = tally
    second = batch(t)
    got = lineage_trace(t, b"items", "k1", [(first, None), (second, None)])
    assert got["floor"] == 0 and got["at_floor"] is None
    assert got["changes"] == [(2, 4, 1), (1, 1, None)]
    assert [b["key"]["planned"] for b in got["batches"]] == [1, 4]
    assert [b["key"]["class"] for b in got["batches"]] == ["added", "updated"]
    assert lineage_trace(t, b"items", "k2", [(second, None)])["batches"] == [None]


def test_a_batch_whose_history_was_cut_is_not_retained(tally):
    t, ix, layers, first = tally
    second = batch(t)  # replaced progress at c1
    commit(t, ix, layers, [("k2", 3, 1)], 2)
    third = batch(t)  # replaced progress at c2
    base = ix.merge(layers, 0, 2, 2, "items/base")  # c1 and c2 fold into a base at c2
    t._set_output(b"items", [base, layers[2]], 3, 2)
    for rec in (first, second):
        assert lineage_input(t, rec) == {"state": "not_retained", "output": b"items", "commit": 1}
    assert keys(lineage_input(t, third)) == [("k2", "updated", 1, 3, "-")]
    got = lineage_trace(t, b"items", "k1", [(first, None), (third, None)])
    assert got["floor"] == 2 and got["at_floor"] == 4 and got["changes"] == []
    assert got["batches"] == [{"state": "not_retained", "output": b"items", "commit": 1}, None]


def test_flow_counts_keys_by_class_per_window(tally):
    t, _, _, first = tally
    second = batch(t)
    assert lineage_flow([(10, first), (HOUR + 5, second)], HOUR) == [
        {"start_ms": 0, "batches": 1, "full_runs": 0, "added": 2, "updated": 0, "removed": 0, "unchanged": 0},
        {
            "start_ms": HOUR,
            "batches": 1,
            "full_runs": 0,
            "added": 0,
            "updated": 1,
            "removed": 0,
            "unchanged": 0,
        },
    ]
