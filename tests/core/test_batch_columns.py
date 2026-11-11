"""The worker's columnar batch (`PlannedKeys`, `Served`) against the per-key
rules it replaces, over random histories of a source output whose keys carry
version words: decoding a plan gives `Tracking.plan_keys`'s keys, classes and
versions, whether the keys carry words or generations, and `observe` gives the classes and the keys served otherwise than
planned that the per-key rule gives, for any answer a loader can serve."""

import random

import pytest
from solera._native import ChangeIndex, PlannedKeys, Served, Tracking, encode_changes
from solera.sdk import Loaded
from solera_worker.batch import KeyVersions

KEYS = [f"k{i}" for i in range(12)]
CLASSES = ("added", "updated", "removed", "unchanged")


def observe(keys, classes, versions, served):
    """The rule as the worker held it per key before SOLE-24."""
    out = {c: [] for c in CLASSES}
    loaded = []
    for key, cls, version in zip(keys, classes, versions, strict=True):
        if served is not None and cls != "removed":
            got = served.get(key)
            if got is None:
                loaded.append([key, None])
                cls = None if cls == "added" else "removed"
            elif isinstance(version, str) and got != version:
                loaded.append([key, got])
                cls = "updated" if cls == "unchanged" else cls
        if cls is not None:
            out[cls].append(key)
    return out, loaded


def answer(rnd, keys, words):
    """A loader's answer to `keys`: each served at its word, at another, absent
    or left out."""
    out = {}
    for k in keys:
        r = rnd.random()
        if r < 0.6 and k in words:
            out[k] = Loaded({"id": k}, version=words[k])
        elif r < 0.75:
            out[k] = Loaded([{"id": k}, {"id": k}], version=f"w{rnd.randint(0, 3)}")
        elif r < 0.85:
            out[k] = None
    return out


@pytest.mark.parametrize("seed", range(40))
def test_columns_follow_the_per_key_rule(tmp_path, seed):
    rnd = random.Random(seed)
    ix = ChangeIndex(f"file://{tmp_path}")
    t = Tracking()
    t.attach(b"src", ix)
    t.apply(Tracking.event_edge_open(b"e", b"src", True))
    words, head, worded = {}, 0, seed % 2 == 0
    for _ in range(8):
        ups = {k: f"w{rnd.randint(0, 3)}" for k in rnd.sample(KEYS, rnd.randint(0, 5))}
        rem = [k for k in rnd.sample(KEYS, rnd.randint(0, 2)) if k in words and k not in ups]
        changes = encode_changes(ups if worded else sorted(ups), rem)
        built = t.build_commit(b"src", changes, version=head + 1, path=f"src/{head + 1}")
        if built is not None:
            head += 1
            t.apply(built["event"])
            words.update(ups)
            for k in rem:
                words.pop(k, None)
        req = {"inputs": [{"edge": b"e", "conditions": {}}], "limit": rnd.randint(1, 8)}
        mode = rnd.choice(["incremental", "incremental", "all", "keys"])
        if mode == "all":
            req["mode"] = "all"
        elif mode == "keys":
            req.update(mode="keys", keys=sorted(rnd.sample(KEYS, rnd.randint(0, 6))))
        p = t.plan(req)["plan"]
        keys, classes, versions = Tracking.plan_keys(p, 0)
        planned = PlannedKeys.decode(p, 0)
        assert len(planned) == len(keys)
        load = {k: v for k, c, v in zip(keys, classes, versions, strict=True) if c != "removed"}
        view = KeyVersions(planned)
        assert list(view) == list(load) and dict(view.items()) == load and view.sorted() == sorted(load)
        served = None
        if rnd.random() < 0.8:
            served = Served()
            for _ in range(rnd.randint(1, 2)):
                rows, bad = served.read(answer(rnd, sorted(load), words), sorted(load), Loaded)
                assert bad is None
        got, loaded = planned.observe(served)
        want, want_loaded = observe(keys, classes, versions, None if served is None else dict(served.items()))
        assert {c: list(got.keys(c)) for c in CLASSES} == want
        assert loaded == want_loaded
        assert got.any() == any(want.values())
        t.apply(t.progress_ops(p, [([], [tuple(x) for x in loaded])])[0])
