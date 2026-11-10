"""Events on the tracking core's state: refused whole, never half applied."""

import pytest
from solera._native import ChangeIndex, Tracking


def items(tmp_path, n: int) -> Tracking:
    """A state whose output `items` has `n` keys, `k0` to `k{n-1}`, written
    one per commit."""
    ix = ChangeIndex(f"file://{tmp_path}")
    t = Tracking()
    t.attach(b"items", ix)
    layers = []
    for c in range(1, n + 1):
        layers.append(ix.write_commit(f"items/{c}", c, [(f"k{c - 1}", c, None)], live_before=c - 1))
        t._set_output(b"items", layers, c, c)
    return t


def test_an_event_that_changes_one_edge_twice_is_refused_and_leaves_the_state_as_it_was():
    t = Tracking()
    t.apply(Tracking.event_edge_open(b"e", b"items", True))
    before = t.snapshot()
    both = Tracking.join([Tracking.event_edge_close(b"e"), Tracking.event_reset(b"e", False)])
    with pytest.raises(ValueError, match="two parts of one event change edge e"):
        t.apply(both)
    assert t.snapshot() == before
    t.apply(Tracking.event_edge_close(b"e"))


def test_a_stale_unkeyed_commit_is_refused_and_never_moves_progress_back(tmp_path):
    t = items(tmp_path, 1)
    t.apply(Tracking.event_edge_open(b"u", b"items", False))
    old = t.plan({"unkeyed": [b"u"], "limit": 1})["plan"]
    stale = t.progress_ops(old, [])[0]
    t2 = items(tmp_path / "two", 2)
    t2.apply(Tracking.event_edge_open(b"u", b"items", False))
    t2.apply(t2.progress_ops(t2.plan({"unkeyed": [b"u"], "limit": 1})["plan"], [])[0])
    assert t2.plan({"unkeyed": [b"u"], "limit": 1})["unkeyed"][0]["through"] == 2
    with pytest.raises(ValueError, match="moved since the batch was planned"):
        t2.apply(stale)
    with pytest.raises(ValueError, match="moved since the batch was planned"):
        t2.progress_ops(old, [])
    assert t2.plan({"unkeyed": [b"u"], "limit": 1})["unkeyed"][0]["through"] == 2


def test_a_batch_limit_past_the_counter_is_refused(tmp_path):
    t = items(tmp_path, 3)
    t.apply(Tracking.event_edge_open(b"e", b"items", True))
    with pytest.raises(ValueError, match="limit is too large"):
        t.plan({"inputs": [{"edge": b"e", "conditions": {}}], "limit": 2**64 - 1})
    p = t.plan({"inputs": [{"edge": b"e", "conditions": {}}], "limit": 2**63})
    assert Tracking.plan_keys(p["plan"], 0)[0] == ["k0", "k1", "k2"] and p["hi"] is None


def test_a_page_of_owed_keys_holds_at_least_one_key():
    t = Tracking()
    t.apply(Tracking.event_edge_open(b"e", b"items", True))
    with pytest.raises(ValueError, match="at least one key"):
        t.owed(b"e", {}, limit=0)


def upstream(tmp_path) -> tuple[Tracking, ChangeIndex, list]:
    """items at c1 holds k1 and k2; the keyed edge e reads it."""
    ix = ChangeIndex(f"file://{tmp_path}")
    t = Tracking()
    t.attach(b"items", ix)
    layers = [ix.write_commit("items/1", 1, [("k1", 1, None), ("k2", 1, None)], live_before=0)]
    t._set_output(b"items", layers, 1, 2)
    t.apply(Tracking.event_edge_open(b"e", b"items", True))
    return t, ix, layers


def plan_all(t: Tracking) -> bytes:
    return t.plan({"inputs": [{"edge": b"e", "conditions": {}}], "limit": 10})["plan"]


def test_a_key_loaded_after_a_clear_since_the_plan_is_held_back_even_once_the_full_run_is_done(tmp_path):
    t, ix, layers = upstream(tmp_path)
    plan = plan_all(t)
    layers.append(ix.write_commit("items/2", 2, [("k1", 2, None)], live_before=2, clear=True))
    t._set_output(b"items", layers, 2, 1, 2)
    t.apply(Tracking.event_full_run_done(b"items"))
    assert t.full_run(b"items") is None
    event, _, held_back = t.progress_ops(plan, [(["k1"], [("k1", 2)])])
    assert held_back == [["k1"]]  # held back before its failure counts
    t.apply(event)
    assert t.owed(b"e", {}) == ([("k1", "added"), ("k2", "removed")], None)


def test_a_key_loaded_mid_full_run_is_held_back_without_a_clear_since_the_plan(tmp_path):
    ix = ChangeIndex(f"file://{tmp_path}")
    t = Tracking()
    t.attach(b"items", ix)
    layers = [ix.write_commit("items/1", 1, [("k1", 1, None), ("k2", 1, None)], live_before=0, clear=True)]
    t._set_output(b"items", layers, 1, 2, 1)
    t.apply(Tracking.event_edge_open(b"e", b"items", True))
    plan = plan_all(t)  # a reader the gate does not hold
    layers.append(ix.write_commit("items/2", 2, [("k1", 2, 1)], live_before=2))
    t._set_output(b"items", layers, 2, 2, 1)
    assert t.progress_ops(plan, [([], [("k1", 2)])])[2] == [["k1"]]


def test_a_key_loaded_at_another_version_with_no_full_run_is_a_deviation(tmp_path):
    t, ix, layers = upstream(tmp_path)
    plan = plan_all(t)
    layers.append(ix.write_commit("items/2", 2, [("k1", 2, 1)], live_before=2))
    t._set_output(b"items", layers, 2, 2)
    event, _, held_back = t.progress_ops(plan, [([], [("k1", 2)])])
    assert held_back == [[]]
    t.apply(event)
    assert t.owed(b"e", {}) == ([], None)


def test_a_commit_is_refused_when_a_full_run_starts_or_ends_after_it_was_built(tmp_path):
    t, ix, layers = upstream(tmp_path)
    plan = plan_all(t)
    layers.append(ix.write_commit("items/2", 2, [("k1", 2, 1)], live_before=2))
    t._set_output(b"items", layers, 2, 2)
    event = t.progress_ops(plan, [([], [("k1", 2)])])[0]  # k1 is a deviation
    layers.append(ix.write_commit("items/3", 3, [("k1", 2, None)], live_before=2, clear=True))
    t._set_output(b"items", layers, 3, 1, 3)
    with pytest.raises(ValueError, match="full run moved"):
        t.apply(event)
    held = t.progress_ops(plan, [([], [("k1", 2)])])
    assert held[2] == [["k1"]]
    unmoved = t.progress_ops(plan, [([], [])])[0]
    t.apply(Tracking.event_full_run_done(b"items"))
    t.apply(unmoved)  # loaded as planned: the full run's state did not decide anything


def test_a_commit_held_mid_full_run_is_refused_once_the_full_run_has_ended(tmp_path):
    ix = ChangeIndex(f"file://{tmp_path}")
    t = Tracking()
    t.attach(b"items", ix)
    layers = [ix.write_commit("items/1", 1, [("k1", 1, None)], live_before=0, clear=True)]
    t._set_output(b"items", layers, 1, 1, 1)
    t.apply(Tracking.event_edge_open(b"e", b"items", True))
    plan = plan_all(t)
    layers.append(ix.write_commit("items/2", 2, [("k1", 2, 1)], live_before=1))
    t._set_output(b"items", layers, 2, 1, 1)
    event, _, held_back = t.progress_ops(plan, [([], [("k1", 2)])])
    assert held_back == [["k1"]]
    t.apply(Tracking.event_full_run_done(b"items"))
    with pytest.raises(ValueError, match="full run moved"):
        t.apply(event)
