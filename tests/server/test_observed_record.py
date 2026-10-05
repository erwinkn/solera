"""The observation record (solera_server.observed): random histories of
range writes under patterns and contexts, points, and folds made as the
engine makes them — a range's keys that changed since its head made points
first, then the range relabelled — against the literal observed set, a
plain dict, after every operation (docs/observed-set.md, "The invariant")."""

import random

import pytest
from solera.patterns import Matcher
from solera_server import observed

KEYS = [f"k{i}" for i in range(12)]
PATTERNS = [
    None,
    {"include": [{"glob": "k1*"}]},
    {"include": [{"glob": "k*"}], "exclude": [["odd", {"glob": "k*[13579]"}]]},
]


class World:
    def __init__(self, rng: random.Random):
        self.rng = rng
        self.commits: list[dict[str, int]] = []  # upstream after each commit: key -> version
        self.rec = observed.record("life-1")
        self.truth: dict[str, tuple] = {}  # S: key -> (version, context versions)

    def commit(self) -> None:
        state = dict(self.commits[-1]) if self.commits else {}
        for k in self.rng.sample(KEYS, self.rng.randrange(1, 5)):
            if self.rng.random() < 0.25:
                state.pop(k, None)
            else:
                state[k] = len(self.commits) + 1
        self.commits.append(state)

    def at(self, endpoint: int, key: str):
        return self.commits[endpoint].get(key)

    def label(self, endpoint, patterns, context):
        return observed.layer(self.rec, endpoint, patterns, context, "life-1")

    def ops(self, ops) -> None:
        observed.apply(self.rec, ops)

    def write_range(self) -> None:
        lo, hi = sorted(self.rng.sample([None, *KEYS, None], 2), key=lambda k: (k is not None, k or ""))
        if lo is not None and hi is not None and lo >= hi:
            return
        if hi is None and lo is None:
            pass
        h = len(self.commits) - 1
        patterns, context = self.rng.choice(PATTERNS), {"factor": self.rng.choice(["w1", "w2"])}
        label = self.label(h, patterns, context)
        self.ops([{"op": "range", "lo": lo, "hi": hi, "label": label}])
        take = Matcher(patterns)
        for k in KEYS:
            if observed._in(k, lo, hi):
                v = self.commits[h].get(k)
                if take(k) and v is not None:
                    self.truth[k] = (v, context)
                else:
                    self.truth.pop(k, None)

    def write_point(self) -> None:
        k = self.rng.choice(KEYS)
        context = {"factor": self.rng.choice(["w1", "w2"])}
        label = self.label(len(self.commits) - 1, None, context)
        if self.rng.random() < 0.3:
            self.ops([{"op": "point", "key": k, "present": False, "label": label}])
            self.truth.pop(k, None)
        else:
            version = f"served-{self.rng.randrange(100)}"
            self.ops([{"op": "point", "key": k, "present": True, "version": version, "label": label}])
            self.truth[k] = (version, context)

    def fold(self) -> None:
        """As the engine folds: each older range's keys that changed between its
        head and now become points at their observed version; then it relabels."""

        h = len(self.commits) - 1
        ops = []
        for r in self.rec["ranges"]:
            if r["endpoint"] >= h:
                continue
            for k in KEYS:
                if observed._in(k, r["lo"], r["hi"]) and k not in self.rec["points"]:
                    if self.commits[r["endpoint"]].get(k) != self.commits[h].get(k):
                        was = observed.decode(self.rec, k, self.at)
                        label = {"patterns": r["patterns"], "context": r["context"], "life": r["life"]}
                        if was is None:
                            ops.append({"op": "point", "key": k, "present": False, "label": label})
                        else:
                            ops.append(
                                {"op": "point", "key": k, "present": True, "version": was[0], "label": label}
                            )
            ops.append({"op": "relabel", "lo": r["lo"], "hi": r["hi"], "endpoint": h})
        self.ops(ops)

    def check(self) -> None:
        for k in KEYS:
            assert observed.decode(self.rec, k, self.at) == self.truth.get(k), k
        bounds = [(r["lo"], r["hi"]) for r in self.rec["ranges"]]
        for (_, a_hi), (b_lo, _) in zip(bounds, bounds[1:], strict=False):
            assert a_hi is not None and b_lo is not None and a_hi <= b_lo, "ranges stay disjoint and sorted"


@pytest.mark.parametrize("seed", range(40))
def test_the_record_decodes_to_the_observed_set(seed):
    w = World(random.Random(seed))
    w.commit()
    for _ in range(60):
        step = w.rng.random()
        if step < 0.3:
            w.commit()
        elif step < 0.6:
            w.write_range()
        elif step < 0.8:
            w.write_point()
        else:
            w.fold()
        w.check()


def test_a_range_spanning_every_key_becomes_the_base():
    rec = observed.record("life-1")
    label = observed.layer(rec, 3, None, {"factor": "w1"}, "life-1")
    observed.apply(rec, [{"op": "range", "lo": None, "hi": "k5", "label": label}])
    observed.apply(rec, [{"op": "range", "lo": "k5", "hi": None, "label": label}])
    assert rec["ranges"] == [] and rec["base"]["endpoint"] == 3 and len(rec["contexts"]) == 1


def test_an_older_selection_over_a_newer_range_is_kept():
    """A27 R8: base k1@1, a range k1@2, a newer selection back at @1: the
    point stays, or decode would reveal the range's @2."""

    rec = observed.record("life-1")
    commits = [{"k1": 1}, {"k1": 2}]
    old = observed.layer(rec, 0, None, {}, "life-1")
    new = observed.layer(rec, 1, None, {}, "life-1")
    observed.apply(rec, [{"op": "range", "lo": None, "hi": None, "label": old}])
    observed.apply(rec, [{"op": "range", "lo": None, "hi": None, "label": new}])
    observed.apply(rec, [{"op": "point", "key": "k1", "present": True, "version": 1, "label": new}])
    assert observed.decode(rec, "k1", lambda e, k: commits[e].get(k)) == (1, {})


def test_the_empty_base_decides_only_in_a_records_gaps():
    """`gaps`: where the empty base decodes keys — none once the base is a
    commit or a per-key consumer's held keys, or the ranges tile the key
    space under any labels, a mid-run context move included; points never
    open one. An unkeyed input's record has none."""

    rec = observed.record("life-1")
    assert observed.gaps(rec) == [(None, None)]  # a first or full run not yet walked
    w1 = observed.layer(rec, 3, None, {"factor": "w1"}, "life-1")
    w2 = observed.layer(rec, 4, None, {"factor": "w2"}, "life-1")
    observed.apply(rec, [{"op": "range", "lo": None, "hi": "k5", "label": w1}])
    observed.apply(rec, [{"op": "point", "key": "k9", "present": True, "version": 1, "label": w2}])
    assert observed.gaps(rec) == [("k5", None)]  # past k5, k9's point aside
    observed.apply(rec, [{"op": "range", "lo": "k5", "hi": None, "label": w2}])
    assert observed.gaps(rec) == [] and len(rec["ranges"]) == 2  # two contexts, every key covered
    observed.apply(rec, [{"op": "range", "lo": None, "hi": None, "label": w2}])
    assert observed.gaps(rec) == [] and rec["ranges"] == []  # one range spanning all: the base
    assert observed.gaps(observed.record("life-1", held=True)) == []  # a per-key full run's base
    assert observed.gaps({"upstream": ["log", ""], "commit": 0, "base": 0}) == []
