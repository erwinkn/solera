"""Per-key outcomes through the core's bindings (B6): a batch commits its
failing keys' records in its own event, a catch-up drops the failures gone
moot, and an event built from a state that moved since says so."""

import itertools

import pytest
from solera._native import ChangeIndex, OutcomesMovedError, Tracking, UnsettledError

REQUEST = {"inputs": [{"edge": b"e", "conditions": {}}], "limit": 10}


class World:
    """`items`, a keyed output written commit by commit, and `e`, a keyed
    edge over it, with its record outputs attached."""

    def __init__(self, tmp_path):
        self.ix = ChangeIndex(f"file://{tmp_path}")
        self.t = Tracking()
        self.layers, self.held, self.paths = [], {}, itertools.count(1)
        for id_ in (b"items", *Tracking.outcome_outputs(b"e")):
            self.t.attach(id_, self.ix)
        self.t.apply(Tracking.event_edge_open(b"e", b"items", True))

    def commit(self, **versions) -> None:
        """Commit each key at its version (None: removed)."""

        c = len(self.layers) + 1
        rows = [(k, v, self.held.get(k)) for k, v in versions.items()]
        live = len(self.held)
        self.layers.append(self.ix.write_commit(f"items/{c}", c, rows, live_before=live))
        for k, v in versions.items():
            if v is None:
                self.held.pop(k, None)
            else:
                self.held[k] = v
        self.t._set_output(b"items", self.layers, c, len(self.held))

    def clock(self, now: float = 1000.0) -> dict:
        n = next(self.paths)
        return {
            "now": now,
            "deploy": 1,
            "retries": 0,
            "time_ms": int(now * 1000),
            "failures_path": f"f/{n}",
            "due_path": f"d/{n}",
        }

    def run(self, reported=(), **clock):
        """Plan the edge and commit the batch, the keys of `reported`
        (`(key, kind, message, retry_after, retry_for)`) failing: what
        `progress_ops` returns, applied."""

        plan = self.t.plan(REQUEST)["plan"]
        failed = sorted(r[0] for r in reported)
        outcomes = {"input": 0, "reported": sorted(reported), "conditions": {}, **self.clock(**clock)}
        out = self.t.progress_ops(plan, [(failed, [])], outcomes=outcomes)
        self.t.apply(out[0])
        return out

    def catch_up(self):
        return self.t.outcome_catch_up(b"e", {}, **self.clock())


def test_a_failing_key_keeps_a_record_in_its_batchs_own_event_and_waits_for_its_input(tmp_path):
    w = World(tmp_path)
    w.commit(a=1, b=1)
    w.run()
    w.commit(a=2, b=2)
    event, records, held_back, stored = w.run([("a", "failed", "ValueError: bad row", None, None)])
    assert stored == {"a": "failed"} and held_back == [[]] and len(records) == 1
    assert w.t.outcome_counts(b"e") == {
        "rejected": 0,
        "failed": 1,
        "retrying": 0,
        "canceled": 0,
        "timed_out": 0,
    }
    rows, after = w.t.outcome_records(b"e")
    assert after is None and [k for k, _ in rows] == ["a"]
    record = rows[0][1]
    assert (record["outcome"], record["tries"], record["message"], record["tried"]) == (
        "failed",
        1,
        "ValueError: bad row",
        2,
    )
    assert record["next_at"] is None and record["until"] is None
    # Its input has not changed since it failed: nothing is owed, b is done.
    assert w.t.owed(b"e", {})[0] == []
    assert w.t.outcome_records(b"e", keys=["b", "a"])[0] == rows
    assert w.t.outcome_records(b"e", outcomes=["rejected"]) == ([], None)
    with pytest.raises(ValueError, match="no stored outcome"):
        w.t.outcome_records(b"e", outcomes=["ok"])


def test_progress_ops_says_what_each_reported_key_became(tmp_path):
    """A transient error past its `retry_for` is failed; one within it is
    retrying, due after its `retry_after`."""

    w = World(tmp_path)
    w.commit(a=1, b=1, c=1)
    *_, stored = w.run(
        [
            ("a", "transient", "Busy", 30.0, 3600.0),
            ("b", "transient", "Busy", None, 0.0),
            ("c", "rejected", "Empty", None, None),
        ],
        now=1000.0,
    )
    assert stored == {"a": "retrying", "b": "failed", "c": "rejected"}
    rows = dict(w.t.outcome_records(b"e")[0])
    assert (rows["a"]["next_at"], rows["a"]["until"]) == (1030, 4600)
    assert len(w.t.outcome_records(b"e", limit=2)[0]) == 2
    assert w.t.outcome_records(b"e", limit=2)[1] == "b"
    assert [k for k, _ in w.t.outcome_records(b"e", after="b")[0]] == ["c"]


def test_a_failure_whose_input_goes_back_is_dropped_by_the_catch_up(tmp_path):
    """KEY-3: a fails at 2 over the 1 the edge holds; its input goes back to
    1: the batch's commit asks for a catch-up first, which drops a's record."""

    w = World(tmp_path)
    w.commit(a=1)
    w.run()
    w.commit(a=2)
    w.run([("a", "failed", "ValueError: x", None, None)])
    assert w.catch_up() is None  # nothing new upstream
    w.commit(a=1)
    plan = w.t.plan(REQUEST)["plan"]
    outcomes = {"input": 0, "reported": [], "conditions": {}, **w.clock()}
    with pytest.raises(OutcomesMovedError, match="catch up first"):
        w.t.progress_ops(plan, [([], [])], outcomes=outcomes)
    w.t.apply(w.catch_up())
    assert w.t.outcome_records(b"e") == ([], None)
    assert not any(w.t.outcome_counts(b"e").values())
    assert w.t.owed(b"e", {})[0] == []


def test_a_key_that_never_succeeded_and_is_removed_upstream_owes_nothing(tmp_path):
    """KEY-12: a fails at its first try, then upstream removes it."""

    w = World(tmp_path)
    w.commit(a=1)
    w.run([("a", "failed", "ValueError: x", None, None)])
    w.commit(a=None)
    w.t.apply(w.catch_up())
    assert w.t.outcome_records(b"e") == ([], None)
    assert w.t.owed(b"e", {})[0] == []


def test_an_event_built_from_a_state_that_moved_since_is_refused_as_moved(tmp_path):
    """Two catch-ups built from one state: the second, checked after the
    first applies, is refused with `OutcomesMovedError`, and the state is
    unchanged."""

    w = World(tmp_path)
    w.commit(a=1, b=1)
    w.run()
    w.commit(a=2, b=2)
    w.run([("a", "failed", "x", None, None), ("b", "failed", "x", None, None)])
    w.commit(a=1)
    first, second = w.catch_up(), w.catch_up()
    w.t.apply(first)
    before = w.t.snapshot()
    with pytest.raises(OutcomesMovedError):
        w.t.check(second)
    with pytest.raises(ValueError):
        w.t.apply(second)  # `apply` refuses it too, as a plain refusal
    assert w.t.snapshot() == before


def test_a_batch_whose_records_moved_since_it_was_planned_is_planned_again(tmp_path):
    """b fails; a plan takes b again once its input moves; a catch-up drops
    b's failure (its input went back) before the batch commits: the batch's
    commit is refused with `UnsettledError`, and is planned again."""

    w = World(tmp_path)
    w.commit(b=1)
    w.run()
    w.commit(b=2)
    w.run([("b", "failed", "x", None, None)])
    w.commit(b=3)
    plan = w.t.plan(REQUEST)["plan"]
    w.commit(b=1)
    w.t.apply(w.catch_up())
    outcomes = {"input": 0, "reported": [], "conditions": {}, **w.clock()}
    with pytest.raises(UnsettledError, match="moved since the batch was planned"):
        w.t.progress_ops(plan, [([], [])], outcomes=outcomes)


def test_an_edges_record_outputs_are_named_as_the_core_names_them():
    assert Tracking.outcome_outputs(b"e") == (b"\x00fe", b"\x00de")
