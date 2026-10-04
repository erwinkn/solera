"""The native streaming jobs — full replacement, compaction — and
the written content they read, against plain-Python expectations and the
`_python` reference."""

import random

import duckdb
import pyarrow as pa
import pytest
from solera import _native

from . import keys_reference as _python
from .keys_driver import drive

OPTS = {"block_size": 512}
G = 77  # the writing generation


def content(files):
    """Every entry of consecutive files: [(key, generation, deleted, payload)]."""

    return [e[:4] for f in files for e in _python.iter_file(f)]


def index(entries, max_file_bytes=4096):
    """Entries (key -> (generation, deleted, payload)) as one run of small files."""

    ks = sorted(entries)
    return _native.write_files(
        ks,
        [entries[k][0] for k in ks],
        bytes(entries[k][1] for k in ks),
        payloads=[entries[k][2] for k in ks],
        max_file_bytes=max_file_bytes,
        **OPTS,
    )


def replace(rows, runs, max_file_bytes=4096, collect=10**6, stream=(), key=None):
    job = _native.Merge.replace(
        rows, len(runs), max_file_bytes=max_file_bytes, collect=collect, key=key, generation=G, **OPTS
    )
    files = drive(job, runs, stream)
    return job, files


def scenario(seed=0, n=3000):
    """An index of two runs — an old level and a newer delta — of a source
    whose keys carry versions, and its next tick: most keys at the
    version they have, some at another, some written again with none,
    deleted keys back, new keys."""

    rng = random.Random(seed)
    keys = [b"k-%06d" % rng.randrange(10**6) for _ in range(n)]
    old = {k: (1, 0, b"r1-" + k) for k in keys}
    newer = {k: (2, 0, b"r2-" + k) for k in rng.sample(keys, n // 10)}
    for k in rng.sample(keys, n // 20):
        newer[k] = (2, 1, None)  # deleted since
    live = {k: e for k, e in {**old, **newer}.items() if not e[1]}
    written = {}
    for k in rng.sample(sorted(live), len(live) * 3 // 4):
        roll = rng.random()
        written[k] = live[k][2] if roll < 0.7 else None if roll < 0.8 else b"r3-" + k
    for k in rng.sample(keys, n // 20):
        if k not in live:
            written[k] = b"r4-" + k  # deleted keys written again
    for i in range(n // 10):
        written[b"new-%05d" % i] = b"r5"
    runs = [index(newer), index(old)]
    return live, written, runs


def expected(live, written):
    """docs/versions.md §2: a key is unchanged only at its stored version."""

    out = []
    for k in sorted(set(live) | set(written)):
        if k not in written:
            out.append((k, G, 1, None))
        elif written[k] is None or k not in live or live[k][2] != written[k]:
            out.append((k, G, 0, written[k]))
    return out


@pytest.mark.parametrize("presorted", [False, True])
def test_replace_a_sources_observation(presorted):
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
    assert job.changed == sum(1 for k, _, d, _ in want if not d and k in live)
    upserts, removes = job.collected()
    assert upserts == [k for k, _, d, _ in want if not d] and removes == [k for k, _, d, _ in want if d]


def test_every_write_of_a_key_without_a_version_is_a_change():
    """docs/versions.md §1: an attempt's write carries no version, so every
    key it writes is changed — at its generation — whatever the index held."""

    first = {b"%05d" % i: (5, 0, None) for i in range(2000)}
    job, files = replace(_native.Rows.keys(list(first)), [index(first)])
    assert content(files) == [(k, G, 0, None) for k in sorted(first)]
    assert (job.added, job.removed, job.changed) == (0, 0, 2000)


def test_a_set_relisted_changes_nothing():
    """A dynamic partitions's elements carry an empty version: listing them again
    is no change (docs/versions.md §2)."""

    written = [b"%05d" % i for i in range(2000)]
    _, files = replace(_native.Rows.keys(written, b""), [])
    assert content(files) == [(k, G, 0, b"") for k in sorted(written)]
    job, files = replace(_native.Rows.keys(written, b""), [files])
    assert files == [] and (job.added, job.removed, job.changed) == (0, 0, 0)


def test_collected_stops_at_its_limit():
    live, written, runs = scenario()
    job, _ = replace(_native.Rows.pairs(list(written.items())), runs, collect=10)
    assert job.collected() is None


def test_records_read_only_their_keys():
    rows = [{"id": 3, "v": {1}}, {"id": "x\udcff", "v": object()}]  # values are never read
    _, files = replace(_native.Rows.records(rows, "id"), [])
    assert content(files) == [(b"3", G, 0, None), (b"x\xff", G, 0, None)]  # str(), surrogateescape
    # A key repeats: its rows are one key, whatever their order.
    grouped = [{"id": 2, "n": 1}, {"id": 1, "n": 0}, {"id": "2", "n": 2}]
    rows = _native.Rows.records(grouped, "id")
    assert rows.entries() == ([b"1", b"2"], [None, None])
    assert rows.find(["2", "1"]) == ([0, 2, 1], [2, 3])
    with pytest.raises(KeyError):
        _native.Rows.records([{"id": 1}, {}], "id")
    with pytest.raises(ValueError, match="str or an int"):
        _native.Rows.records([{"id": 1.5}], "id")
    with pytest.raises(ValueError, match="two versions"):
        replace(_native.Rows.pairs([(b"a", b"x"), (b"a", b"y")]), [])


def test_replace_arrow_in_place():
    keys = [b"k-%05d" % i for i in range(3000)]
    random.Random(2).shuffle(keys)
    table = pa.table({"k": [k.decode() for k in keys], "v": [object.__name__] * len(keys)})
    _, files = replace(_native.Rows.arrow(table, "k"), [])
    assert content(files) == [(k, G, 0, None) for k in sorted(keys)]
    # Multiple chunks, string views.
    chunks = pa.Table.from_batches(table.to_batches(max_chunksize=97))
    viewed = chunks.cast(pa.schema([("k", pa.string_view()), ("v", pa.string())]))
    _, again = replace(_native.Rows.arrow(viewed, "k"), [])
    assert content(again) == content(files)
    for t in (pa.int8(), pa.int32(), pa.uint64()):  # integer keys: their decimal text
        ints = pa.table({"id": pa.array([3, 1, 2], t)})
        _, files = replace(_native.Rows.arrow(ints, "id"), [])
        assert [e[0] for e in content(files)] == [b"1", b"2", b"3"]
    with pytest.raises(ValueError, match="strings or integers"):
        _native.Rows.arrow(pa.table({"id": [1.5]}), "id")


def test_duckdb_relation_is_arrow():
    rel = duckdb.sql("select 'k' || lpad(range::varchar, 6, '0') as k, range as n from range(5000)")
    rows = _native.Rows.arrow(rel, "k")
    assert rows.presorted and len(rows) == 5000


def test_replace_sorted_stream_of_keys():
    """A store reporting the keys it wrote (a `Sql` write): sorted chunks of
    keys — a list, or Arrow data whose first column holds them."""

    old = {b"k%04d" % i: (3, 0, None) for i in range(0, 3000, 2)}
    keys = sorted(b"k%04d" % i for i in range(0, 3000, 3))
    chunks = [keys[i : i + 300] for i in range(0, len(keys), 300)]
    chunks[1] = pa.table({"key": chunks[1]})
    job, files = replace(None, [index(old)], stream=chunks)
    want = sorted([(k, G, 0, None) for k in keys] + [(k, G, 1, None) for k in old if k not in set(keys)])
    assert content(files) == want
    assert (job.added, job.removed) == (len(set(keys) - set(old)), len(set(old) - set(keys)))
    with pytest.raises(ValueError, match="sorted"):
        replace(None, [], stream=[keys[5:10], keys[:5]])


def test_streamed_rows_group_by_their_key():
    rows = [{"k": "a", "n": 1}, {"k": "b", "n": 2}, {"k": "b", "n": 3}, {"k": "b", "n": 4}, {"k": "c"}]
    _, files = replace(None, [], stream=[rows[:2], rows[2:3], rows[3:]], key="k")
    assert content(files) == [(k, G, 0, None) for k in (b"a", b"b", b"c")]
    table = pa.Table.from_pylist(rows)
    _, arrow = replace(None, [], stream=[table.slice(0, 3), table.slice(3)], key="k")
    assert content(arrow) == content(files)


def test_a_span_merge_of_many_files():
    rng = random.Random(3)
    levels = []
    for lv in range(4):
        levels.append(
            {
                b"k%05d" % rng.randrange(20000): (
                    4 - lv,  # the first run is the newest span
                    int(rng.random() < 0.1),
                    b"p%d" % lv if lv % 2 else None,
                )
                for _ in range(3000)
            }
        )
    runs = [index(e, max_file_bytes=2048) for e in levels]
    assert sum(len(r) for r in runs) > 20
    want = {}
    for e in reversed(levels):
        want.update(e)
    for drop in (False, True):  # into the base: deleted keys go
        job = _native.Merge.spans(len(runs), endpoints=[], base=drop, max_file_bytes=4096, **OPTS)
        files = drive(job, runs, per=2)
        got = content(files)
        assert got == [(k, g, d, p) for k, (g, d, p) in sorted(want.items()) if not (drop and d)]


def test_key_rows_shapes():
    """A keyed write's keys, whatever its shape."""

    import pandas as pd
    from solera.sdk import KEYS, Output
    from solera.stores import WriteError, frames

    def key_rows(value, output):  # as a store taking DataFrames and Arrow reads them
        return frames.prepare(value, output).rows

    rows = [{"id": 2, "v": "b"}, {"id": 1, "v": "a"}]
    declared = Output("t", key="id")
    for value in (rows, rows + rows, pd.DataFrame(rows), pa.Table.from_pylist(rows)):
        _, files = replace(key_rows(value, declared), [])
        assert content(files) == [(b"1", G, 0, None), (b"2", G, 0, None)]
    _, files = replace(key_rows({"x": [1]}, Output("k", key=KEYS)), [])
    assert content(files) == [(b"x", G, 0, None)]
    with pytest.raises(WriteError, match="key column"):
        key_rows([{"x": 1}], declared)


def test_one_join_whatever_the_content_comes_as():
    """Review round 2, finding 5: one merge-join serves rows and sorted runs;
    a replacement and a patch differ only in what becomes of the index keys
    the content leaves out."""

    live, written, runs = scenario(seed=5)
    items = sorted(written.items())
    by_rows, files = replace(_native.Rows.pairs(items), runs)
    run = _native.SortedEntries.of([k for k, _ in items], [v for _, v in items])
    by_run = _native.Merge.patch(
        run, len(runs), replace=True, max_file_bytes=4096, collect=10**6, generation=G, **OPTS
    )
    assert content(drive(by_run, runs, ())) == content(files) == expected(live, written)
    assert (by_run.added, by_run.removed, by_run.changed) == (by_rows.added, by_rows.removed, by_rows.changed)
    assert by_run.collected() == by_rows.collected()
    patch = _native.Merge.patch(run, len(runs), max_file_bytes=4096, generation=G, **OPTS)
    assert content(drive(patch, runs, ())) == [e for e in expected(live, written) if not e[2]]
    assert patch.removed == 0


def test_arrow_columns_named_twice_are_refused():
    """Review round 5: a column named twice is refused where Arrow data comes
    in, before any column is chosen: else the key reads one and a store
    keeps the other."""

    table = pa.Table.from_arrays([pa.array(["a"]), pa.array(["b"])], names=["id", "id"])
    with pytest.raises(ValueError, match="appears twice"):
        _native.Rows.arrow(table, "id")
    job = _native.Merge.replace(None, 0, key="id", **OPTS)
    with pytest.raises(ValueError, match="appears twice"):
        job.feed_rows(table)
