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


def replace(rows, runs, max_file_bytes=4096, collect=10**6, stream=()):
    job = _native.Job.replace(rows, len(runs), max_file_bytes=max_file_bytes, collect=collect, **OPTS)
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
    rows = _native.Rows.objects(items, 0, lambda kv: kv[1])
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
    rows = _native.Rows.objects(list(written), None, b"v")
    _, files = replace(rows, [])
    assert content(files) == [(k, b"v", 0) for k in sorted(written)]
    job, files = replace(_native.Rows.objects(list(written), None, b"v"), [files])
    assert files == [] and (job.added, job.removed, job.changed) == (0, 0, 0)


def test_collected_stops_at_its_limit():
    live, written, runs = scenario()
    job, _ = replace(_native.Rows.objects(list(written.items()), 0, lambda kv: kv[1]), runs, collect=10)
    assert job.collected() is None


def test_objects_keys_and_errors():
    rows = [{"id": 3, "v": "a"}, {"id": "x\udcff", "v": "b"}]
    job, files = replace(_native.Rows.objects(rows, "id", lambda r: r["v"].encode()), [])
    assert content(files) == [(b"3", b"a", 0), (b"x\xff", b"b", 0)]  # str(), surrogateescape
    with pytest.raises(ValueError, match="duplicate key"):
        _native.Rows.objects([{"id": 1}, {"id": "1"}], "id", b"")  # sorted
    with pytest.raises(ValueError, match="duplicate key"):
        replace(_native.Rows.objects([{"id": 2}, {"id": 1}, {"id": "2"}], "id", b""), [])
    with pytest.raises(KeyError):
        _native.Rows.objects([{"id": 1}, {}], "id", b"")

    def boom(row):
        raise LookupError("no revision")

    with pytest.raises(LookupError, match="no revision"):
        replace(_native.Rows.objects([1, 2], None, boom), [])


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
    with pytest.raises(ValueError, match="cannot digest"):
        _native.Rows.arrow(pa.table({"id": ["a"], "u": pa.array([[1]], pa.list_view(pa.int64()))}), "id")


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
    """A keyed write's content, whatever its shape, as `key_map` defines it."""

    import pandas as pd
    from solera.sdk import KEYS, Output
    from solera.stores import WriteError, key_rows, revision

    rows = [{"id": 2, "v": "b"}, {"id": 1, "v": "a"}]
    declared = Output("t", key="id", revision="v")
    for value in (rows, pd.DataFrame(rows), pa.Table.from_pylist(rows)):
        _, files = replace(key_rows(declared, value), [])
        assert content(files) == [(b"1", b"a", 0), (b"2", b"b", 0)]
    _, files = replace(key_rows(Output("d", key="id"), rows), [])
    assert content(files) == [(b"1", revision(rows[1]), 0), (b"2", revision(rows[0]), 0)]
    _, files = replace(key_rows(Output("k", key=KEYS), {"x": [1]}), [])
    assert content(files) == [(b"x", revision([1]), 0)]
    with pytest.raises(WriteError, match='duplicate key "1"'):
        key_rows(declared, sorted(rows + rows, key=lambda r: r["id"]))  # sorted: found at once
    with pytest.raises(ValueError, match='duplicate key "1"'):
        replace(key_rows(declared, rows + rows), [])  # else as the join reaches it
    with pytest.raises(WriteError, match="key column"):
        key_rows(declared, [{"x": 1}])
    with pytest.raises(WriteError, match="revision field"):
        replace(key_rows(declared, [{"id": 1}]), [])
