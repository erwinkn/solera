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


def _rows(value):
    return value if isinstance(value, list) else [value]


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
        served, model = None, None
        if rnd.random() < 0.8:
            served, model = Served(), {}
            for _ in range(rnd.randint(1, 3)):
                asked = sorted(rnd.sample(sorted(load), rnd.randint(0, len(load))))
                given = answer(rnd, asked, words)
                rows, bad = served.read(given, asked, Loaded)
                assert bad is None
                assert rows == [r for k in asked if given.get(k) for r in _rows(given[k].value)]
                model.update({k: None if given.get(k) is None else given[k].version for k in asked})
            assert served.items() == sorted(model.items(), key=lambda kv: kv[0].encode())
        got, loaded = planned.observe(served)
        want, want_loaded = observe(keys, classes, versions, model)
        assert {c: list(got.keys(c)) for c in CLASSES} == want
        assert loaded == want_loaded
        assert got.any() == any(want.values())
        t.apply(t.progress_ops(p, [([], [tuple(x) for x in loaded])])[0])


def test_a_whole_answer_is_read_in_its_order_and_grouped_on_request():
    answer = {
        "b": Loaded([{"id": "b"}, {"id": "b2"}], version="1"),
        "a": Loaded({"id": "a"}, version=2),
        "c": None,
    }
    served = Served()
    assert served.read(answer, None, Loaded) == ([{"id": "b"}, {"id": "b2"}, {"id": "a"}], None)
    assert served.items() == [("a", "2"), ("b", "1"), ("c", None)]
    grouped, bad = Served().read(answer, ["a", "z"], Loaded, grouped=True)
    assert grouped == {"a": [{"id": "a"}]} and bad is None


def test_a_bad_answer_names_its_key_and_leaves_it_as_it_was_served():
    served = Served()
    served.read({"a": Loaded(1, version="1")}, ["a"], Loaded)
    rows, bad = served.read({"a": Loaded(2, version=None), None: 3}, ["a"], Loaded)
    assert bad == ("a",) and served.get("a") == "1"
    assert Served().read({None: object()}, None, Loaded)[1] == (None,)


def test_a_mapping_answer_is_read_through_its_get():
    class Answer(dict):
        def get(self, key, default=None):
            return Loaded({"id": key}, version="mine")

    served = Served()
    assert served.read(Answer(), ["k"], Loaded) == ([{"id": "k"}], None)
    assert served["k"] == "mine" and served.get("x", 0) == 0
    with pytest.raises(KeyError):
        served["x"]


def test_loads_out_of_key_order_keep_each_keys_last_serving():
    served = Served()
    served.read({"m": Loaded(0, version="1"), "z": Loaded(0, version="1")}, None, Loaded)
    served.read({"a": Loaded(0, version="2"), "m": None, "q": Loaded(0, version="2")}, None, Loaded)
    served.read({"z": Loaded(0, version="3")}, None, Loaded)
    assert served.items() == [("a", "2"), ("m", None), ("q", "2"), ("z", "3")]
    assert served.versions() == {"a": "2", "q": "2", "z": "3"}
    assert "a" in served and "m" not in served and "2" not in served
    twice = Served()
    twice.read({1: Loaded(0, version="x"), "1": Loaded(0, version="y")}, None, Loaded)
    assert twice.items() == [("1", "y")]


def test_a_list_subclass_is_read_through_its_iterator():
    class Visible(list):
        def __iter__(self):
            return iter([r for r in super().__iter__() if r != "hidden"])

    rows, _ = Served().read({"k": Loaded(Visible(["hidden", "seen"]), version="1")}, ["k"], Loaded)
    assert rows == ["seen"]


def test_a_batch_read_from_columns_equals_one_built_whole():
    import copy
    import pickle

    from solera.sdk import Batch

    p = PlannedKeys()
    classes, loaded = p.observe(None)
    lazy = Batch._of_columns(classes, None, rows=[1], index=2, count=3)
    whole = Batch(rows=[1], index=2, count=3)
    assert lazy == whole and repr(lazy) == repr(whole) and loaded == []
    assert copy.deepcopy(lazy) == whole and pickle.loads(pickle.dumps(lazy)) == whole
    held = []
    looped = Batch(rows=held)
    held.append(looped)
    copied = copy.deepcopy(looped)
    assert copied.rows[0] is copied
    match lazy:
        case Batch(rows, added):
            assert (rows, added) == ([1], ())
    with pytest.raises(AttributeError):
        lazy.rows = []
