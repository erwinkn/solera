"""The native streaming jobs — full replacement, compaction, recount — and
the written content they read, against plain-Python expectations and the
`_python` reference."""

import random

import duckdb
import pyarrow as pa
import pytest
from solera import _native
from solera.keys import _python

from .keys_driver import drive

OPTS = {"block_size": 512}


def content(files):
    """Every entry of consecutive files: [(key, version, deleted)]."""

    return [e for f in files for e in _python.iter_file(f)]


def index(entries, max_file_bytes=4096):
    """Entries (key -> (version, deleted)) as one run of small files."""

    ks = sorted(entries)
    return _native.write_files(
        ks,
        [entries[k][0] for k in ks],
        bytes(entries[k][1] for k in ks),
        max_file_bytes=max_file_bytes,
        **OPTS,
    )


def replace(rows, runs, max_file_bytes=4096, collect=10**6, stream=(), fold=False):
    job = _native.Job.replace(
        rows, len(runs), max_file_bytes=max_file_bytes, collect=collect, fold=fold, **OPTS
    )
    files = drive(job, runs, stream)
    return job, files


def scenario(seed=0, n=3000):
    """An index of two runs — an old level and a newer delta — and a new content."""

    rng = random.Random(seed)
    keys = [b"k-%06d" % rng.randrange(10**6) for _ in range(n)]
    old = {k: (b"v1-" + k, 0) for k in keys}
    newer = {k: (b"v2-" + k, 0) for k in rng.sample(keys, n // 10)}
    for k in rng.sample(keys, n // 20):
        newer[k] = (b"", 1)  # deleted since
    live = {k: v for k, (v, d) in {**old, **newer}.items() if not d}
    written = {}
    for k in rng.sample(sorted(live), len(live) * 3 // 4):
        written[k] = live[k] if rng.random() < 0.8 else b"v3-" + k
    for k in rng.sample(keys, n // 20):
        if k not in live:
            written[k] = b"v4-" + k  # deleted keys written again
    for i in range(n // 10):
        written[b"new-%05d" % i] = b"v5"
    runs = [index(newer), index(old)]
    return live, written, runs


def expected(live, written):
    out = []
    for k in sorted(set(live) | set(written)):
        if k not in written:
            out.append((k, b"", 1))
        elif live.get(k) != written[k]:
            out.append((k, written[k], 0))
    return out


@pytest.mark.parametrize("presorted", [False, True])
def test_replace_objects(presorted):
    live, written, runs = scenario()
    items = sorted(written.items())
    if not presorted:
        random.Random(1).shuffle(items)
    rows = _native.Rows.pairs(items)
    assert rows.presorted == presorted and len(rows) == len(written)
    job, files = replace(rows, runs)
    want = expected(live, written)
    assert content(files) == want
    assert len(files) > 1  # cut as they fill
    assert job.added == sum(1 for k in written if k not in live)
    assert job.removed == sum(1 for k in live if k not in written)
    assert job.changed == sum(1 for k in written if k in live and live[k] != written[k])
    upserts, removes = job.collected()
    assert upserts == [k for k, _, d in want if not d] and removes == [k for k, _, d in want if d]


def test_replace_initial_load_and_nothing_changed():
    written = {b"%05d" % i: b"v" for i in range(2000)}
    rows = _native.Rows.keys(list(written), b"v")
    _, files = replace(rows, [])
    assert content(files) == [(k, b"v", 0) for k in sorted(written)]
    job, files = replace(_native.Rows.keys(list(written), b"v"), [files])
    assert files == [] and (job.added, job.removed, job.changed) == (0, 0, 0)


def test_collected_stops_at_its_limit():
    live, written, runs = scenario()
    job, _ = replace(_native.Rows.pairs(list(written.items())), runs, collect=10)
    assert job.collected() is None


def test_records_keys_groups_and_errors():
    rows = [{"id": 3, "v": "a"}, {"id": "x\udcff", "v": "b"}]
    job, files = replace(_native.Rows.records(rows, "id", "v"), [])
    assert content(files) == [(b"3", b"a", 0), (b"x\xff", b"b", 0)]  # str(), surrogateescape
    # A key repeats: its rows are one group, whatever their order.
    grouped = [{"id": 2, "n": 1}, {"id": 1, "n": 0}, {"id": "2", "n": 2}]
    _, files = replace(_native.Rows.records(grouped, "id"), [])
    assert content(files) == [
        (b"1", _native.group_digest([grouped[1]], "id"), 0),
        (b"2", _native.group_digest([grouped[2], grouped[0]], "id"), 0),
    ]
    with pytest.raises(ValueError, match="different revisions"):
        replace(_native.Rows.records([{"id": 1, "v": "a"}, {"id": 1, "v": "b"}], "id", "v"), [])
    with pytest.raises(KeyError):
        _native.Rows.records([{"id": 1}, {}], "id")
    with pytest.raises(ValueError, match="revision field"):
        replace(_native.Rows.records([{"id": 1}], "id", "v"), [])
    with pytest.raises(ValueError, match="cannot digest a value of type set"):
        replace(_native.Rows.records([{"id": 1, "s": {1}}], "id"), [])


def test_replace_arrow_in_place():
    live, written, runs = scenario()
    items = list(written.items())
    random.Random(2).shuffle(items)
    table = pa.table({"k": [k.decode() for k, _ in items], "rev": [v for _, v in items]})
    _, files = replace(_native.Rows.arrow(table, "k", "rev"), runs)
    assert content(files) == expected(live, written)
    # Multiple chunks, string views, integer keys.
    chunks = pa.Table.from_batches(table.to_batches(max_chunksize=97))
    viewed = chunks.cast(pa.schema([("k", pa.string_view()), ("rev", pa.binary())]))
    _, again = replace(_native.Rows.arrow(viewed, "k", "rev"), runs)
    assert content(again) == content(files)
    ints = pa.table({"id": pa.array([3, 1, 2], pa.int32()), "rev": [7, 8, 9]})
    _, files = replace(_native.Rows.arrow(ints, "id", "rev"), [])
    assert content(files) == [(b"1", b"8", 0), (b"2", b"9", 0), (b"3", b"7", 0)]


def test_row_digests():
    t = pa.table(
        {
            "id": ["a", "b", "c"],
            "n": pa.array([1, 2, None], pa.int64()),
            "tags": [["x"], [], None],
            "at": pa.array([0, 1_000, 2_000], pa.timestamp("ms", tz="UTC")),
        }
    )

    def digests(table):
        _, files = replace(_native.Rows.arrow(table, "id"), [])
        return {k: v for k, v, _ in content(files)}

    base = digests(t)
    assert len(set(base.values())) == 3 and all(len(v) == 16 for v in base.values())
    # Physical layout and column order don't change a digest; values do.
    same = pa.table(
        {
            "at": pa.array([0, 1_000_000, 2_000_000], pa.timestamp("us", tz="UTC")),
            "tags": pa.array([["x"], [], None], pa.large_list(pa.large_string())),
            "n": pa.array([1, 2, None], pa.int8()),
            "id": pa.array(["a", "b", "c"], pa.string_view()),
        }
    )
    assert digests(same) == base
    changed = digests(t.set_column(1, "n", pa.array([1, 3, None], pa.int64())))
    assert changed[b"a"] == base[b"a"] and changed[b"b"] != base[b"b"]
    ree = pa.RunEndEncodedArray.from_arrays(pa.array([1], pa.int32()), pa.array(["x"]))
    with pytest.raises(ValueError, match="cannot digest"):
        replace(_native.Rows.arrow(pa.table({"id": ["a"], "u": ree}), "id"), [])


def test_struct_digests_are_framed():
    # Moving `c` into `a` changes the row; without a field count the bytes would not.
    flat = pa.table({"id": ["x"], "a": [{"b": 1}], "c": [2]})
    nested = pa.table({"id": ["x"], "a": [{"b": 1, "c": 2}]})
    (_, v1, _), (_, v2, _) = (content(replace(_native.Rows.arrow(t, "id"), [])[1])[0] for t in (flat, nested))
    assert v1 != v2


def test_duckdb_relation_is_arrow():
    rel = duckdb.sql("select 'k' || lpad(range::varchar, 6, '0') as k, range as n from range(5000)")
    rows = _native.Rows.arrow(rel, "k")
    assert rows.presorted and len(rows) == 5000


def test_replace_sorted_stream():
    live, written, runs = scenario()
    items = sorted(written.items())
    chunks = [items[i : i + 700] for i in range(0, len(items), 700)]
    chunks[1] = pa.table({"key": [k for k, _ in chunks[1]], "version": [v for _, v in chunks[1]]})
    _, files = replace(None, runs, stream=chunks)
    assert content(files) == expected(live, written)
    with pytest.raises(ValueError, match="sorted"):
        replace(None, runs, stream=[items[5:10], items[:5]])


def test_streamed_rows_fold_into_groups():
    # A store reporting one version per row: a key's rows may span chunks.
    rows = [(b"a", b"1" * 16), (b"b", b"2" * 16), (b"b", b"3" * 16), (b"b", b"4" * 16), (b"c", b"5" * 16)]
    _, files = replace(None, [], stream=[rows[:2], rows[2:3], rows[3:]], fold=True)
    reordered = [rows[0], rows[3], rows[1], rows[2], rows[4]]  # b's rows in another order and chunking
    _, again = replace(None, [], stream=[reordered[:3], reordered[3:]], fold=True)
    assert [k for k, _, _ in content(files)] == [b"a", b"b", b"c"]
    assert content(files) == content(again)
    _, fewer = replace(None, [], stream=[rows[:3] + rows[4:]], fold=True)
    assert content(fewer)[1] != content(files)[1]  # a group counts its rows
    # Without folding, a repeated key must repeat its version.
    _, files = replace(None, [], stream=[[(b"a", b"x"), (b"a", b"x")]])
    assert content(files) == [(b"a", b"x", 0)]
    with pytest.raises(ValueError, match="different revisions"):
        replace(None, [], stream=[[(b"a", b"x")], [(b"a", b"y")]])


def test_compact_many_files():
    rng = random.Random(3)
    levels = []
    for lv in range(4):
        levels.append(
            {b"k%05d" % rng.randrange(20000): (b"v%d" % lv, int(rng.random() < 0.1)) for _ in range(3000)}
        )
    runs = [index(e, max_file_bytes=2048) for e in levels]
    assert sum(len(r) for r in runs) > 20
    want = {}
    for e in reversed(levels):
        want.update(e)
    for drop in (False, True):
        job = _native.Job.compact(len(runs), drop_deleted=drop, max_file_bytes=4096, **OPTS)
        files = drive(job, runs, per=2)
        got = content(files)
        assert got == [(k, v, d) for k, (v, d) in sorted(want.items()) if not (drop and d)]
    count = _native.Job.count(len(runs))
    drive(count, runs)
    assert count.live == sum(1 for _, d in want.values() if not d)


def test_key_rows_shapes():
    """A keyed write's content, whatever its shape, with one version for the same rows."""

    import pandas as pd
    from solera.sdk import KEYS, Output
    from solera.stores import WriteError, key_rows

    rows = [{"id": 2, "v": "b"}, {"id": 1, "v": "a"}]
    declared = Output("t", key="id", revision="v")
    for value in (rows, pd.DataFrame(rows), pa.Table.from_pylist(rows)):
        _, files = replace(key_rows(value, declared), [])
        assert content(files) == [(b"1", b"a", 0), (b"2", b"b", 0)]
    digested = Output("d", key="id")
    want = [
        (b"1", _native.group_digest([rows[1]], "id"), 0),
        (b"2", _native.group_digest([rows[0]], "id"), 0),
    ]
    for value in (rows, pd.DataFrame(rows), pa.Table.from_pylist(rows)):
        assert content(replace(key_rows(value, digested), [])[1]) == want
    _, files = replace(key_rows({"x": [1]}, Output("k", key=KEYS)), [])
    assert content(files) == [(b"x", _native.value_digest([1]), 0)]
    _, files = replace(key_rows(rows + rows, declared), [])  # a key's rows agree on their revision
    assert content(files) == [(b"1", b"a", 0), (b"2", b"b", 0)]
    with pytest.raises(WriteError, match="key column"):
        key_rows([{"x": 1}], declared)
