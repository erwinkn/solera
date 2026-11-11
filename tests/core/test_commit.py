"""Output commits through the bindings (B4): one call builds and writes a
commit from the changed keys, the event it returns installs it."""

import random

import pytest
from solera._native import ChangeIndex, FormatError, Tracking, encode_changes

from tests.tracking.core import ABSENT, Log

GEN = object()


@pytest.fixture
def index(tmp_path):
    return ChangeIndex(f"file://{tmp_path}")


class Core:
    """A Tracking with the layer lists the test needs to query the index."""

    def __init__(self, index):
        self.t = Tracking()
        self.index = index
        self.layers = {}

    def commit(self, output, changes, **kw):
        self.t.attach(output, self.index)
        built = self.t.build_commit(output, changes, **kw)
        if built is not None:
            self.t.apply(built["event"])
            self.layers.setdefault(output, []).append(built["layer"])
        return built

    def state(self, output):
        layers = self.layers[output]
        rows, _ = self.index.scan(layers, layers[-1]["hi"])
        return {k: v for k, _, v, _ in rows}

    def entries(self, output, commit):
        rows, _ = self.index.diff(self.layers[output], commit - 1, commit)
        return [(k, new, old) for k, _, new, old in rows]


def test_a_batch_commit_records_what_each_key_replaced(index):
    core = Core(index)
    first = core.commit(b"items", encode_changes(["a", "b", "c"]), version=10, path="items/1", collect=10)
    assert first["commit"] == 1 and first["live"] == 3 and first["added"] == 3
    assert first["upserted_keys"] == ["a", "b", "c"] and first["removed_keys"] == []
    second = core.commit(
        b"items", encode_changes(["b", "d"], removes=["c", "zz"]), version=11, path="items/2"
    )
    assert (second["added"], second["updated"], second["removed"], second["live"]) == (1, 1, 1, 3)
    assert second["upserted_keys"] is None
    assert core.entries(b"items", 2) == [("b", 11, 10), ("c", None, 10), ("d", 11, None)]
    assert core.state(b"items") == {"a": 10, "b": 11, "d": 11}


def test_a_commit_that_changes_nothing_writes_nothing(index, tmp_path):
    core = Core(index)
    core.commit(b"src", encode_changes({"a": "e1"}), version=1, path="src/1")
    before = core.t.snapshot()
    assert core.commit(b"src", encode_changes({"a": "e1"}, removes=["b"]), version=2, path="src/2") is None
    assert core.t.snapshot() == before
    assert not (tmp_path / "src" / "2").exists()


def test_a_full_listing_removes_what_it_does_not_name(index):
    core = Core(index)
    core.commit(b"src", encode_changes({"a": "e1", "b": "e1", "c": None}), version=5, path="src/1")
    listed = core.commit(
        b"src", encode_changes({"a": "e1", "b": "e2", "d": ""}), version=6, path="src/2", mode="replace"
    )
    assert core.entries(b"src", 2) == [("b", "e2", "e1"), ("c", None, 5), ("d", "", None)]
    assert listed["live"] == 3


def test_a_load_report_yields_to_a_later_commit(index):
    core = Core(index)
    core.commit(b"src", encode_changes({"a": "e1", "b": "e1"}), version=1, path="src/1")
    core.commit(b"src", encode_changes({"b": "e2"}), version=2, path="src/2")
    report = core.commit(
        b"src", encode_changes({"a": "e9", "b": "e9"}), version=3, path="src/3", mode="report", at=1
    )
    assert report["commit"] == 3
    assert core.entries(b"src", 3) == [("a", "e9", "e1")]


def test_a_commit_built_at_an_old_head_is_refused(index):
    core = Core(index)
    core.t.attach(b"o", index)
    a = core.t.build_commit(b"o", encode_changes(["k"]), version=1, path="o/a")
    b = core.t.build_commit(b"o", encode_changes(["k"]), version=2, path="o/b")
    with pytest.raises(ValueError, match="two parts of one event"):
        core.t.apply(Tracking.join([a["event"], b["event"]]))
    core.t.apply(a["event"])
    with pytest.raises(ValueError, match="built before o had a life: it is in life 1 now"):
        core.t.apply(b["event"])
    restored = Tracking.restore(core.t.snapshot())
    assert restored.snapshot() == core.t.snapshot()


def test_bad_input_is_refused(index):
    t = Tracking()
    with pytest.raises(ValueError, match="no change index attached"):
        t.build_commit(b"o", encode_changes(["a"]), version=1, path="o/1")
    t.attach(b"o", index)
    with pytest.raises(FormatError):
        t.build_commit(b"o", b"not changes", version=1, path="o/1")
    with pytest.raises(ValueError):
        encode_changes(["a"], removes=["a"])
    with pytest.raises(ValueError):
        encode_changes({"a": "v" * (1 << 20)})
    with pytest.raises(ValueError):
        t.build_commit(b"o", encode_changes(["a"]), version=1, path="o/1", mode="report")


def test_commits_match_the_model_over_random_histories(tmp_path):
    """Each commit's changes and the state after it equal the model's
    (tests/tracking/core.py `Log.commit`) over random patches, full listings,
    clears, reverts and removals; a commit the model finds empty writes
    nothing, except a clear."""

    for seed in range(200):
        rng = random.Random(seed)
        core, log = Core(ChangeIndex(f"file://{tmp_path}/{seed}")), Log()
        for step in range(1, 13):
            keys = rng.sample("abcdefgh", rng.randint(0, 6))
            versions = {k: rng.choice(["e1", "e2", "", GEN, ABSENT]) for k in keys}
            mode = rng.choice(["patch", "patch", "replace", "clear"])
            if mode == "replace":
                versions = {k: v for k, v in versions.items() if v is not ABSENT}
            changes = encode_changes(
                {k: None if v is GEN else v for k, v in versions.items() if v is not ABSENT},
                removes=[k for k, v in versions.items() if v is ABSENT],
            )
            named = {k: step if v is GEN else v for k, v in versions.items()}
            if mode == "replace":
                named |= {k: ABSENT for k in log.states[-1] if k not in named}
            expected = log.commit(named, clear=mode == "clear")
            built = core.commit(b"o", changes, version=step, path=f"o/{seed}/{step}", mode=mode)
            if not expected and mode != "clear":
                log.entries.pop(), log.states.pop()
                assert built is None, (seed, step)
                continue
            before, after = log.states[-2], log.states[-1]
            changed = [(k, after.get(k), before.get(k)) for k in sorted(before.keys() | after.keys())]
            assert built["commit"] == log.head and built["live"] == len(after), (seed, step)
            assert core.entries(b"o", log.head) == [c for c in changed if c[1] != c[2]], (seed, step)
            assert core.state(b"o") == after, (seed, step)
            if mode == "clear":
                assert core.t.full_run(b"o") == log.head, (seed, step)


def test_a_full_run_clears_then_ends_with_its_last_commit_or_without_one(index):
    core = Core(index)
    core.commit(b"o", encode_changes({"a": "e1", "b": "e1", "c": "e1"}), version=1, path="o/1")
    assert core.t.full_run(b"o") is None
    with pytest.raises(ValueError, match="not in a full run"):
        core.t.apply(Tracking.event_full_run_done(b"o"))
    first = core.commit(
        b"o", encode_changes({"b": "e2", "d": None}, removes=["zz"]), version=2, path="o/2", mode="clear"
    )
    assert (first["added"], first["updated"], first["removed"], first["live"]) == (2, 0, 3, 2)
    assert first["layer"]["clears"] == [2] and core.t.full_run(b"o") == 2
    assert core.state(b"o") == {"b": "e2", "d": 2}
    assert core.entries(b"o", 2) == [("a", None, "e1"), ("b", "e2", "e1"), ("c", None, "e1"), ("d", 2, None)]
    last = core.commit(b"o", encode_changes({"e": "e1"}), version=3, path="o/3", ends_full_run=True)
    assert last["live"] == 3 and core.t.full_run(b"o") is None
    with pytest.raises(ValueError, match="not in one"):
        core.commit(b"o", encode_changes({"f": "e1"}), version=4, path="o/4", ends_full_run=True)
    empty = core.commit(b"o", encode_changes([]), version=5, path="o/5", mode="clear")
    assert empty["live"] == 0 and core.state(b"o") == {} and core.t.full_run(b"o") == 4
    assert core.commit(b"o", encode_changes([]), version=6, path="o/6", ends_full_run=True) is None
    core.t.apply(Tracking.event_full_run_done(b"o"))
    assert core.t.full_run(b"o") is None
    assert Tracking.restore(core.t.snapshot()).snapshot() == core.t.snapshot()
