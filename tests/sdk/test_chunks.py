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
        prepared = prepare(rows, out)
        chunks = read(KeyedWrite(prepared, upserts=set(picked)), size)
        assert [g for c in chunks for g in c.groups()] == [g for g in want if g[0] in picked]
        for c in chunks:
            assert prepared.take(c.indices) == c.rows


def test_a_selection_whose_rows_are_a_run_is_a_range_and_an_empty_one_is_empty():
    from solera.stores import Chunk

    prepared = prepare([{"id": k} for k in "abcd"], Output("t", key="id"))
    assert prepared.rows.span(["b", "c"]).rows() == range(1, 3)
    assert list(prepared.rows.span(["a", "c"]).rows()) == [0, 2]
    empty = Chunk(prepared, prepared.rows.span([]))
    assert (len(empty), empty.keys, empty.rows, empty.groups()) == (0, [], [], [])


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


def test_arrow_maps_are_read_as_dicts_at_any_depth():
    m = pa.map_(pa.string(), pa.int64())
    table = pa.table(
        {
            "id": ["a", "b"],
            "m": pa.array([[("x", 1)], [("y", 2)]], m),
            "s": pa.array([{"inner": [("z", 3)]}, {"inner": []}], pa.struct([("inner", m)])),
        }
    )
    [chunk] = read(KeyedWrite(prepare_frames(table, Output("t", key="id")), reset=True), 5)
    assert chunk.rows == [
        {"id": "a", "m": {"x": 1}, "s": {"inner": {"z": 3}}},
        {"id": "b", "m": {"y": 2}, "s": {"inner": {}}},
    ]


class _MapExtension(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.map_(pa.string(), pa.int64()), "solera.test.map")

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


def test_an_extension_type_over_a_map_is_read_as_dicts():
    storage = pa.array([[("x", 1)]], pa.map_(pa.string(), pa.int64()))
    table = pa.table({"id": ["a"], "e": pa.ExtensionArray.from_storage(_MapExtension(), storage)})
    [chunk] = read(KeyedWrite(prepare_frames(table, Output("t", key="id")), reset=True), 5)
    assert chunk.rows == [{"id": "a", "e": {"x": 1}}]


@pytest.mark.xfail(
    strict=True,
    raises=pa.ArrowNotImplementedError,
    reason="as on main: Arrow has no take for string views; a store write of one out of key order fails (SOLE-24 R3)",
)
def test_view_columns_are_read_in_key_order():
    table = pa.table(
        {
            "id": pa.array(["b", "a"], pa.string_view()),
            "p": pa.array([["x"], ["y"]], pa.list_view(pa.string_view())),
        }
    )
    [chunk] = read(KeyedWrite(prepare_frames(table, Output("t", key="id")), reset=True), 5)
    assert sorted(chunk.rows, key=lambda r: r["id"]) == [{"id": "a", "p": ["y"]}, {"id": "b", "p": ["x"]}]
