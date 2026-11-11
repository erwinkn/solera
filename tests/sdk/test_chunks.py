"""`KeyedWrite.chunks`: a write's keys and rows read as columns, a `Chunk` at a
time, for every form a producer returns (rows in or out of key order, keys
holding several rows, a by-key mapping, a `keyed=True` dict, a DataFrame,
Arrow), against the groups a key-at-a-time reading gives."""

import random

import pyarrow as pa
import pytest
from solera.sdk import KEYS, Output
from solera.stores import KeyedWrite, StoreError, prepare
from solera.stores.frames import prepare as prepare_frames


def expected(rows, column):
    """Each key with its rows in the write's order, keys by their UTF-8 bytes."""
    groups = {}
    for r in rows:
        groups.setdefault(str(r[column]), []).append(r)
    return [(k, groups[k]) for k in sorted(groups, key=lambda k: k.encode())]


def random_rows(rnd):
    keys = [
        rnd.choice(["a", "b", "é", "z", "10", "9", "k\u00e9", "x"]) + str(i)
        for i in range(rnd.randint(1, 12))
    ]
    rows = [{"id": rnd.choice(keys), "n": i} for i in range(rnd.randint(0, 40))]
    if rnd.random() < 0.3:
        rows.sort(key=lambda r: r["id"].encode())
    return rows


def read(write, size):
    chunks = list(write.iter_chunks(size))
    assert all(0 < len(c) <= size for c in chunks)
    return chunks


@pytest.mark.parametrize("seed", range(60))
def test_chunks_give_each_key_its_rows(seed):
    rnd = random.Random(seed)
    rows = random_rows(rnd)
    out = Output("t", key="id")
    size = rnd.randint(1, 5)
    want = expected(rows, "id")
    for prepared in (prepare(rows, out), prepare_frames(pa.Table.from_pylist(rows), out) if rows else None):
        if prepared is None:
            continue
        chunks = read(KeyedWrite(prepared, reset=True), size)
        assert [g for c in chunks for g in c.groups()] == want
        assert [k for c in chunks for k in c.keys] == [k for k, _ in want]
        assert [r for c in chunks for r in c.rows] == [r for _, g in want for r in g]
        for c in chunks:
            assert prepared.take(c.indices) == c.rows
    if want:
        picked = sorted(rnd.sample([k for k, _ in want], rnd.randint(1, len(want))))
        chunks = read(KeyedWrite(prepare(rows, out), upserts=set(picked)), size)
        assert [g for c in chunks for g in c.groups()] == [g for g in want if g[0] in picked]


def test_a_dataframe_and_a_keyed_dict_are_chunked_by_key():
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame({"id": ["b", "a", "b"], "n": [1, 2, 3]})
    chunks = read(KeyedWrite(prepare_frames(frame, Output("t", key="id")), reset=True), 1)
    assert [g for c in chunks for g in c.groups()] == [
        ("a", [{"id": "a", "n": 2}]),
        ("b", [{"id": "b", "n": 1}, {"id": "b", "n": 3}]),
    ]
    keyed = prepare({"y": 1, "x": [2]}, Output("t", key=KEYS))
    assert [g for c in read(KeyedWrite(keyed, reset=True), 5) for g in c.groups()] == [("x", [2]), ("y", 1)]


def test_a_key_the_write_does_not_hold_is_refused():
    write = KeyedWrite(prepare([{"id": "a"}], Output("t", key="id")), upserts={"a", "b"})
    with pytest.raises(StoreError, match="'b', which the write does not hold"):
        read(write, 5)


def test_a_row_that_is_not_a_mapping_is_refused():
    from solera.stores import WriteError

    with pytest.raises(WriteError, match="a row is a list, not a mapping"):
        prepare([{"id": "a"}, ["b"]], Output("t", key="id"))
