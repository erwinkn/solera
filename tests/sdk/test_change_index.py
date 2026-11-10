"""The change index's Python bindings (`solera._native.ChangeIndex`, the
`native/index` crate): random histories written commit by commit, merged and
folded at random cuts, each query compared with a fold of the history; the
index's globs against `solera.patterns`; and an S3-compatible server when
SOLERA_TEST_S3 is set. The crate's own property tests (`cargo test -p
solera-index`) cover the format and the reader in depth."""

import os
import random
import time
import uuid
from urllib.parse import unquote, urlsplit

import pytest
from hypothesis import given
from hypothesis import strategies as st
from solera._native import ChainError, ChangeIndex, FormatError, index_pattern_matches
from solera.patterns import Matcher

S3_URL = os.getenv("SOLERA_TEST_S3")


def history(rng: random.Random):
    keys = sorted(
        {
            f"{rng.choice(['a', 'a/b', 'ab', 'é', 'z/z'])}/{rng.randrange(40)}"
            for _ in range(rng.randrange(1, 40))
        }
    )
    state: dict[str, object] = {}
    commits, states = [None], [{}]
    for c in range(1, rng.randrange(2, 25)):
        changes = []
        for k in keys:
            if rng.random() > 0.3:
                continue
            cur = state.get(k)
            new = None if cur is not None and rng.random() < 0.3 else rng.choice([1, 2, c * 10, "x", "y", ""])
            if new == cur or (new is None and cur is None):
                continue
            changes.append((k, new, cur))
            if new is None:
                del state[k]
            else:
                state[k] = new
        commits.append(changes)
        states.append(dict(state))
    return keys, commits, states


def build(ix: ChangeIndex, rng: random.Random, commits, states):
    layers, floor, n = [], 0, 0
    for c in range(1, len(commits)):
        layers.append(ix.write_commit(f"c{c}", c, commits[c], producer=b"p", live_before=len(states[c - 1])))
        if rng.random() < 0.5:
            cut = rng.randint(floor, c)
            pick = ChangeIndex.pick_merge(layers, cut)
            if pick is None:
                a = rng.randrange(len(layers))
                pick = (a, rng.randint(a + 1, len(layers)))
            n += 1
            merged = ix.merge(layers, pick[0], pick[1], cut, f"m{n}")
            if pick[0] == 0:
                floor = merged["floor"] or 0
            layers[pick[0] : pick[1]] = [merged]
    return layers, floor


@pytest.mark.parametrize("seed", range(40))
def test_queries_on_merged_layers_match_the_fold(tmp_path, seed):
    rng = random.Random(seed)
    keys, commits, states = history(rng)
    ix = ChangeIndex(
        f"file://{tmp_path}", memory_bytes=rng.choice([0, 1 << 20]), block_bytes=rng.choice([40, 4096])
    )
    layers, floor = build(ix, rng, commits, states)
    head = len(commits) - 1

    def last_set(k, c):
        return max(x for x in range(1, c + 1) if any(e[0] == k for e in commits[x]))

    for _ in range(10):
        c1 = rng.randint(floor, head)
        c2 = rng.randint(c1, head)
        lo = rng.choice([None, *keys])
        hi = rng.choice([None, *keys])
        limit = rng.choice([None, 1, 3])
        include = rng.choice([None, ["a/**"], ["**/1*"], ["é/*"]])
        m = Matcher({"include": [{"glob": g} for g in include]} if include else None)

        def takes(k, lo=lo, hi=hi, m=m):
            return (lo is None or k > lo) and (hi is None or k <= hi) and m(k)

        def page(rows, limit=limit):
            if limit is not None and len(rows) >= limit:
                return rows[:limit], rows[limit - 1][0]
            return rows, None

        want = [
            (
                k,
                max(c for c in range(c1 + 1, c2 + 1) if any(e[0] == k for e in commits[c])),
                states[c2].get(k),
                states[c1].get(k),
            )
            for k in keys
            if takes(k) and states[c1].get(k) != states[c2].get(k)
        ]
        rows, nxt = ix.diff(layers, c1, c2, lo=lo, hi=hi, limit=limit, include=include)
        assert (rows, nxt) == page(want), (seed, c1, c2, lo, hi, limit, include)
        assert ix.count_diff(layers, c1, c2, lo=lo, hi=hi, limit=limit, include=include) == len(rows)

        want = [(k, last_set(k, c2), states[c2][k]) for k in keys if takes(k) and k in states[c2]]
        rows, nxt = ix.scan(layers, c2, lo=lo, hi=hi, limit=limit, include=include)
        assert ([r[:3] for r in rows], nxt) == page(want)
        assert ix.count_scan(layers, c2, lo=lo, hi=hi, limit=limit, include=include) == len(rows)

        w1 = rng.randint(floor + 1, head + 1)
        w2 = rng.randint(w1, head + 1)
        want = [
            (k, [(c, e[1], e[2]) for c in range(min(w2, head), w1 - 1, -1) for e in commits[c] if e[0] == k])
            for k in keys
            if takes(k)
        ]
        rows, nxt = ix.history(layers, commits=(w1, w2), lo=lo, hi=hi, limit=limit, include=include)
        assert (rows, nxt) == page([w for w in want if w[1]])

        ask = rng.sample(keys, min(len(keys), 5)) + ["nothing"]
        want = [
            (k, last_set(k, c1), states[c1][k]) for k in keys if k in ask and takes(k) and k in states[c1]
        ]
        rows, nxt = ix.get(layers, c1, ask, lo=lo, hi=hi, limit=limit, include=include)
        assert ([r[:3] for r in rows], nxt) == page(want)

    rows, _ = ix.history(layers)
    for k, entries in rows:
        above = [(c, e[1], e[2]) for c in range(head, floor, -1) for e in commits[c] if e[0] == k]
        state = [(last_set(k, floor), states[floor][k], None)] if floor and k in states[floor] else []
        assert entries == above + state
    with pytest.raises(ValueError):
        ix.scan(layers, head + 1)


def test_a_merge_refuses_entries_that_do_not_chain(tmp_path):
    ix = ChangeIndex(f"file://{tmp_path}")
    l1 = ix.write_commit("c1", 1, [("k", 1, None)])
    l2 = ix.write_commit("c2", 2, [("k", 3, 2)], live_before=1)
    with pytest.raises(ChainError):
        ix.merge([l1, l2], 0, 2, 0, "m")


def test_a_damaged_layer_is_a_format_error(tmp_path):
    ix = ChangeIndex(f"file://{tmp_path}")
    layer = ix.write_commit("c1", 1, [(f"k{i:04}", i, None) for i in range(2000)])
    path = tmp_path / "c1"
    data = bytearray(path.read_bytes())
    data[100] ^= 0xFF
    path.write_bytes(bytes(data))
    with pytest.raises(FormatError):
        ChangeIndex(f"file://{tmp_path}").scan([layer], 1)


def test_bad_input_is_refused(tmp_path):
    ix = ChangeIndex(f"file://{tmp_path}")
    with pytest.raises(ValueError):
        ix.write_commit("c1", 1, [("k", 1, None), ("k", 2, None)])
    with pytest.raises(ValueError):
        ix.write_commit("c1", 1, [("k", 1, 1)])
    with pytest.raises(TypeError):
        ix.write_commit("c1", 1, [("k", 1.5, None)])
    layer = ix.write_commit("c1", 1, [("k", 1, None)])
    with pytest.raises(ValueError):
        ix.diff([{**layer, "lo": 2, "hi": 2}], 1, 2)
    with pytest.raises(FormatError):
        ix.scan([{**layer, "entries": 5}], 1)


segments = st.sampled_from(["a", "b", "/", ".", "é", "€", "😀", "x/y"])
glob_parts = st.sampled_from(["a", "b", "/", ".", "é", "€", "😀", "*", "**", "**/", "?"])


@given(st.lists(segments, max_size=6).map("".join), st.lists(glob_parts, min_size=1, max_size=5).map("".join))
def test_the_index_reads_globs_as_solera_patterns_do(key, glob):
    assert not index_pattern_matches("€", ["*??"]) and not index_pattern_matches("😀", ["**???"])
    assert index_pattern_matches(key, [glob]) == Matcher({"include": [{"glob": glob}]})(key)
    assert index_pattern_matches(key, [], [glob]) == Matcher({"exclude": [["x", {"glob": glob}]]})(key)


@pytest.mark.s3
def test_s3_round_trip(tmp_path):
    if not S3_URL:
        pytest.skip("set SOLERA_TEST_S3 to run against an S3-compatible server")
    u = urlsplit(S3_URL)
    options = {
        "aws_endpoint": f"{u.scheme}://{u.netloc.rpartition('@')[2]}",
        "aws_access_key_id": unquote(u.username),
        "aws_secret_access_key": unquote(u.password),
        "aws_region": "us-east-1",
        "aws_allow_http": "true",
    }
    url = f"s3://{u.path.strip('/')}/index-test-{uuid.uuid4().hex}"
    ix = ChangeIndex(url, options, block_bytes=256)
    rng = random.Random(3)
    keys, commits, states = history(rng)
    layers, floor = build(ix, rng, commits, states)
    head = len(commits) - 1
    rows_head, _ = ix.scan(layers, head)
    assert [(k, v) for k, _, v, _ in rows_head] == sorted(states[head].items())
    rows, _ = ix.diff(layers, floor, head)
    assert {k for k, *_ in rows} == {k for k in keys if states[floor].get(k) != states[head].get(k)}
    assert ix.stats()["gets"] > 0

    cached = ChangeIndex(url, options, disk_dir=str(tmp_path))
    assert cached.scan(layers, head)[0] == rows_head
    reopened = ChangeIndex(url, options, disk_dir=str(tmp_path))
    for _ in range(100):
        if reopened.scan(layers, head)[0] == rows_head and reopened.stats()["disk_reads"] > 0:
            break
        time.sleep(0.05)
    assert reopened.stats()["disk_reads"] > 0
