"""Events on the tracking core's state: refused whole, never half applied."""

import pytest
from solera._native import ChangeIndex, Tracking, encode_changes


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


def test_a_commit_is_refused_when_a_full_run_starts_after_it_was_built(tmp_path):
    t, ix, layers = upstream(tmp_path)
    plan = plan_all(t)
    layers.append(ix.write_commit("items/2", 2, [("k1", 2, 1)], live_before=2))
    t._set_output(b"items", layers, 2, 2)
    event = t.progress_ops(plan, [([], [("k1", 2)])])[0]  # k1 is a deviation
    layers.append(ix.write_commit("items/3", 3, [("k1", 2, None)], live_before=2, clear=True))
    t._set_output(b"items", layers, 3, 1, 3)
    with pytest.raises(ValueError, match="became unsettled"):
        t.apply(event)
    held = t.progress_ops(plan, [([], [("k1", 2)])])
    assert held[2] == [["k1"]]
    unmoved = t.progress_ops(plan, [([], [])])[0]
    t.apply(Tracking.event_full_run_done(b"items"))
    t.apply(unmoved)  # loaded as planned: the full run's state did not decide anything


def test_a_commit_held_mid_full_run_applies_once_the_full_run_has_ended(tmp_path):
    """Holding back is always safe: the key stays owed and the next batch
    takes it, so the end of the full run does not refuse the commit."""
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
    t.apply(event)
    assert t.owed(b"e", {}) == ([("k1", "added")], None)


def test_a_key_held_back_on_one_input_is_held_back_on_every_input(tmp_path):
    """k's output is one value computed from both inputs: a load of k that a
    full run of a makes unsafe holds k back on b too, so neither commits it."""
    ix = ChangeIndex(f"file://{tmp_path}")
    t = Tracking()
    logs = {}
    for out in (b"a", b"b"):
        t.attach(out, ix)
        logs[out] = [ix.write_commit(f"{out.decode()}/1", 1, [("k", 1, None), ("m", 1, None)], live_before=0)]
        t._set_output(out, logs[out], 1, 2)
        t.apply(Tracking.event_edge_open(b"r/" + out, out, True))
    inputs = [{"edge": b"r/a", "conditions": {}}, {"edge": b"r/b", "conditions": {}}]
    plan = t.plan({"inputs": inputs, "limit": 10})["plan"]
    logs[b"a"].append(ix.write_commit("a/2", 2, [("k", 2, None)], live_before=2, clear=True))
    t._set_output(b"a", logs[b"a"], 2, 1, 2)
    event, _, held_back = t.progress_ops(plan, [([], [("k", 2)]), ([], [])])
    assert held_back == [["k"], ["k"]]
    t.apply(event)
    assert t.owed(b"r/b", {}) == ([("k", "added")], None)


def test_a_commit_is_refused_once_its_edge_reads_another_output(tmp_path):
    """Whether a load is held back is decided on the output the plan read: a
    commit for an edge renamed to read another output since is refused."""
    t, ix, layers = upstream(tmp_path)
    t.attach(b"other", ix)
    other = [ix.write_commit("other/1", 1, [("k1", 1, None)], live_before=0, clear=True)]
    t._set_output(b"other", other, 1, 1, 1)
    plan = plan_all(t)
    layers.append(ix.write_commit("items/2", 2, [("k1", 2, 1)], live_before=2))
    t._set_output(b"items", layers, 2, 2)
    event = t.progress_ops(plan, [([], [("k1", 2)])])[0]
    t.apply(Tracking.event_edge_rename(b"e", b"e", b"other"))
    with pytest.raises(ValueError, match="reads another output"):
        t.apply(event)
    with pytest.raises(ValueError, match="reads another output"):
        t.progress_ops(plan, [([], [("k1", 2)])])


def test_a_commit_is_refused_once_a_fold_passes_the_head_it_planned_at(tmp_path):
    """A fold through a commit after the plan's head drops the clears it
    covered, so the commit can no longer tell whether its loads are safe."""
    t, ix, layers = upstream(tmp_path)
    plan = plan_all(t)
    layers.append(ix.write_commit("items/2", 2, [("k1", 2, None)], live_before=2, clear=True))
    layers.append(ix.write_commit("items/3", 3, [("k2", 3, None)], live_before=1))
    t._set_output(b"items", layers, 3, 2)
    assert t.progress_ops(plan, [([], [("k1", 2)])])[2] == [["k1"]]
    t._set_output(b"items", [ix.merge(layers, 0, 3, 3, "items/m")], 3, 2)
    with pytest.raises(ValueError, match="no longer reach back"):
        t.progress_ops(plan, [([], [("k1", 2)])])


def test_a_fenced_commit_is_refused_once_a_fold_passes_the_head_it_planned_at(tmp_path):
    t, ix, layers = upstream(tmp_path)
    plan = plan_all(t)
    layers.append(ix.write_commit("items/2", 2, [("k1", 2, 1)], live_before=2))
    t._set_output(b"items", layers, 2, 2)
    event = t.progress_ops(plan, [([], [("k1", 2)])])[0]
    t._set_output(b"items", [ix.merge(layers, 0, 2, 2, "items/m")], 2, 2)
    with pytest.raises(ValueError, match="no longer reach back"):
        t.apply(event)


def test_a_fenced_commit_cannot_share_an_event_with_a_commit_to_the_output_it_reads(tmp_path):
    """The fence reads the input's output, so no other part of its event may
    change it: the event's outcome would depend on the order of its parts."""
    ix = ChangeIndex(f"file://{tmp_path}")
    t = Tracking()
    t.attach(b"items", ix)
    t.apply(t.build_commit(b"items", encode_changes(["k1", "k2"], []), version=1, path="items/1")["event"])
    t.apply(Tracking.event_edge_open(b"e", b"items", True))
    plan = plan_all(t)
    t.apply(t.build_commit(b"items", encode_changes(["k1"], []), version=2, path="items/2")["event"])
    event, _, held_back = t.progress_ops(plan, [([], [("k1", 2)])])
    assert held_back == [[]]
    clear = t.build_commit(b"items", encode_changes(["k1"], []), version=3, path="items/3", mode="clear")
    with pytest.raises(ValueError, match="two parts of one event change output items"):
        t.apply(Tracking.join([clear["event"], event]))


def test_a_report_naming_a_key_the_batch_did_not_take_is_refused(tmp_path):
    t, _, _ = upstream(tmp_path)
    plan = plan_all(t)
    with pytest.raises(ValueError, match="did not take"):
        t.progress_ops(plan, [(["zz"], [])])
    with pytest.raises(ValueError, match="did not take"):
        t.progress_ops(plan, [([], [("k0", 1)])])
    t.apply(t.progress_ops(plan, [(["k2"], [("k1", 1)])])[0])


def test_a_key_another_input_took_is_held_back_when_loaded_mid_full_run(tmp_path):
    """k is owed on b only, but the worker loads a's k too, from the middle of
    a's full run: no version was planned for it on a, so the load counts as
    moved and k is held back on b."""
    ix = ChangeIndex(f"file://{tmp_path}")
    t = Tracking()
    logs = {}
    for out in (b"a", b"b"):
        t.attach(out, ix)
        logs[out] = [ix.write_commit(f"{out.decode()}/1", 1, [("k", 1, None)], live_before=0)]
        t._set_output(out, logs[out], 1, 1)
        t.apply(Tracking.event_edge_open(b"r/" + out, out, True))
    inputs = [{"edge": b"r/a", "conditions": {}}, {"edge": b"r/b", "conditions": {}}]
    t.apply(t.progress_ops(t.plan({"inputs": inputs, "limit": 10})["plan"], [([], []), ([], [])])[0])
    logs[b"b"].append(ix.write_commit("b/2", 2, [("k", 2, 1)], live_before=1))
    t._set_output(b"b", logs[b"b"], 2, 1)
    plan = t.plan({"inputs": inputs, "limit": 10})["plan"]
    assert Tracking.plan_keys(plan, 0)[0] == [] and Tracking.plan_keys(plan, 1)[0] == ["k"]
    logs[b"a"].append(ix.write_commit("a/2", 2, [("m", 2, None)], live_before=1, clear=True))
    t._set_output(b"a", logs[b"a"], 2, 1, 2)
    event, _, held_back = t.progress_ops(plan, [([], [("k", None)]), ([], [])])
    assert held_back == [[], ["k"]]
    t.apply(event)
    assert t.owed(b"r/b", {}) == ([("k", "updated")], None)
