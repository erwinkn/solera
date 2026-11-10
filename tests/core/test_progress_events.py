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
    stale, _ = t.progress_ops(old, [])
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
