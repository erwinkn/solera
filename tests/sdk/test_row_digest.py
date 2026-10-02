"""The row digest grammar (docs/row-digest.md): golden vectors, and the same
logical rows digesting alike from Python values and from Arrow data."""

import datetime as dt
import decimal
import math
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pytest
from solera import _native

D = decimal.Decimal


def arrow(table: pa.Table) -> dict[bytes, bytes]:
    """Each key's version, from Arrow data keyed by `id`."""

    keys, versions = _native.Rows.arrow(table, "id").entries()
    return dict(zip(keys, versions, strict=True))


def python(rows: list[dict]) -> dict[bytes, bytes]:
    keys, versions = _native.Rows.records(rows, "id").entries()
    return dict(zip(keys, versions, strict=True))


def same(column: pa.Array, values: list) -> None:
    """A one-column table and the same values as Python rows digest alike."""

    ids = [f"r{i}" for i in range(len(values))]
    table = pa.table({"id": ids, "x": column})
    assert arrow(table) == python([{"id": i, "x": v} for i, v in zip(ids, values, strict=True)])


# -- golden bytes --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,encoded",
    [
        (None, b"n"),
        (True, b"o\x01"),
        (0, b"i\x010"),
        (-12, b"i\x03-12"),
        (2**70, b"i\x161180591620717411303424"),
        (1.5, b"f" + (0x3FF8000000000000).to_bytes(8, "little")),
        (-0.0, b"f" + bytes(8)),
        (math.nan, b"f" + (0x7FF8000000000000).to_bytes(8, "little")),
        (D("1.20"), b"e\x0212\x01"),  # 12 · 10^-1, the exponent zigzagged
        (D("-0.00"), b"e\x010\x00"),
        (D("1E+3"), b"e\x011\x06"),
        ("é", b"s\x02\xc3\xa9"),
        (b"\x00", b"b\x01\x00"),
        (dt.date(1970, 1, 2), b"D" + (1).to_bytes(8, "little")),
        (dt.time(0, 0, 1), b"h" + (10**9).to_bytes(8, "little")),
        (dt.datetime(1970, 1, 1, 0, 0, 0, 1), b"t" + (1000).to_bytes(16, "little")),
        (dt.datetime(1970, 1, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=1))), b"T" + bytes(16)),
        (dt.timedelta(microseconds=-1), b"u" + (-1000).to_bytes(16, "little", signed=True)),
        ([1, None], b"l\x02i\x011n"),
        ({"b": 1, "a": None}, b"r\x01\x01bi\x011"),
        ({2: "x", 1: "y"}, b"m\x02i\x011s\x01yi\x012s\x01x"),
    ],
)
def test_encodings(value, encoded):
    assert _native.encode(value) == encoded


def test_digests_are_pinned():
    # Any change here changes every version in every index: bump the grammar version.
    row = {"id": "k", "n": 1, "s": "x", "at": dt.datetime(2026, 1, 1), "tags": ["a"]}
    assert _native.DIGEST_VERSION == 1
    assert _native.row_digest(row, "id").hex() == "e374e4ff321e8685f3ffc3a2c50fca36"
    assert _native.group_digest([row], "id").hex() == "540c629d9d908e7efd1675cadd04cb94"
    assert _native.value_digest([1, "a"]).hex() == "626d0beaca5a4a2bf4a99e70a91277ad"
    # The productions, from the spec alone: XXH3-128, little-endian, over version and tag.
    import xxhash

    def h(b):
        return xxhash.xxh3_128_intdigest(b).to_bytes(16, "little")

    record = _native.encode({k: v for k, v in row.items() if k != "id"})
    assert _native.row_digest(row, "id") == h(b"\x01R" + record)
    assert _native.group_digest([row], "id") == h(b"\x01G\x01" + h(b"\x01R" + record))
    assert _native.value_digest([1, "a"]) == h(b"\x01V" + _native.encode([1, "a"]))


# -- Python and Arrow agree ----------------------------------------------------------------


def test_integers_one_representation():
    for t in (pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint32(), pa.uint64()):
        same(pa.array([0, 1, 127], t), [0, 1, 127])
    same(pa.array([2**64 - 1], pa.uint64()), [2**64 - 1])
    same(pa.array([-(2**63)], pa.int64()), [-(2**63)])
    # Booleans are not integers, from either side.
    assert _native.encode(True) != _native.encode(1)
    assert python([{"id": "a", "x": True}]) != python([{"id": "a", "x": 1}])
    same(pa.array([True, False]), [True, False])


def test_floats():
    same(pa.array([1.5, -0.0, math.nan], pa.float64()), [1.5, 0.0, math.nan])
    same(pa.array([1.5, 0.25], pa.float32()), [1.5, 0.25])
    assert _native.encode(1.0) != _native.encode(1)


def test_decimals_normalized():
    same(pa.array([D("1.20"), D("-3.00"), D("0")], pa.decimal128(10, 2)), [D("1.2"), D("-3"), D("0.000")])
    same(
        pa.array([D("12345678901234567890123456789012345678.9")], pa.decimal256(50, 1)),
        [D("12345678901234567890123456789012345678.90")],
    )
    with pytest.raises(ValueError, match="NaN or infinite Decimal"):
        _native.encode(D("NaN"))


def test_temporal():
    ns = pd.Timestamp("2026-03-01 12:00:00.000000123")
    same(pa.array([ns.value], pa.timestamp("ns")), [ns])
    same(pa.array([1_000_000], pa.timestamp("us")), [dt.datetime(1970, 1, 1, 0, 0, 1)])
    same(pa.array([1], pa.timestamp("s")), [dt.datetime(1970, 1, 1, 0, 0, 1)])
    # An instant is the same in any zone; naive is a different value.
    paris = dt.datetime(2026, 6, 1, 14, tzinfo=ZoneInfo("Europe/Paris"))
    instant = int(paris.timestamp()) * 10**6
    same(pa.array([instant], pa.timestamp("us", tz="UTC")), [paris])
    same(pa.array([instant], pa.timestamp("us", tz="Asia/Tokyo")), [paris])
    assert _native.encode(paris) != _native.encode(paris.replace(tzinfo=None))
    # Nanoseconds are kept: 1001 ns and 1999 ns differ.
    assert arrow(pa.table({"id": ["a"], "x": pa.array([1001], pa.timestamp("ns"))})) != arrow(
        pa.table({"id": ["a"], "x": pa.array([1999], pa.timestamp("ns"))})
    )
    # Dates are not midnight timestamps.
    same(pa.array([20000], pa.date32()), [dt.date(2024, 10, 4)])
    same(pa.array([20000 * 86_400_000], pa.date64()), [dt.date(2024, 10, 4)])
    assert _native.encode(dt.date(2024, 10, 4)) != _native.encode(dt.datetime(2024, 10, 4))
    same(pa.array([3_600_000_000], pa.time64("us")), [dt.time(1)])
    same(pa.array([1500], pa.duration("ms")), [dt.timedelta(seconds=1.5)])
    same(pa.array([pd.Timedelta(5).value], pa.duration("ns")), [pd.Timedelta(5)])
    with pytest.raises(ValueError, match="timezone"):
        _native.encode(dt.time(1, tzinfo=dt.UTC))


def test_nested_records_maps_and_lists():
    same(pa.array([[1, None], []], pa.list_(pa.int64())), [[1, None], []])
    same(pa.array([["a"]], pa.large_list(pa.large_string())), [("a",)])
    same(
        pa.array([{"b": 1, "a": "x"}], pa.struct([("a", pa.string()), ("b", pa.int32())])),
        [{"a": "x", "b": 1}],
    )
    # A string-keyed map is a record; other maps keep their keys typed.
    same(pa.array([[("b", 1), ("a", 2)]], pa.map_(pa.string(), pa.int64())), [{"a": 2, "b": 1}])
    same(pa.array([[(2, "x"), (1, "y")]], pa.map_(pa.int64(), pa.string())), [{1: "y", 2: "x"}])
    # Moving a field into a struct changes the row.
    flat = pa.table({"id": ["k"], "a": [{"b": 1}], "c": [2]})
    nested = pa.table({"id": ["k"], "a": [{"b": 1, "c": 2}]})
    assert arrow(flat) != arrow(nested)
    assert arrow(flat) == python([{"id": "k", "a": {"b": 1}, "c": 2}])
    with pytest.raises(ValueError, match="appears twice"):
        arrow(
            pa.table({"id": ["k"], "m": pa.array([[("a", 1), ("a", 2)]], pa.map_(pa.string(), pa.int64()))})
        )


def test_missing_is_null():
    rows = [{"id": "a", "x": 1}, {"id": "b", "x": None, "y": None}, {"id": "c"}]
    table = pa.Table.from_pylist(rows)
    assert python(rows) == arrow(table)
    assert python(rows)[b"b"] == python(rows)[b"c"]
    assert python([{"id": "a", "x": math.nan}]) != python([{"id": "a"}])  # NaN is a value
    # pandas marks missing floats with NaN; a DataFrame reaches Arrow through DuckDB, which reads them as nulls.
    from solera.sdk import Output
    from solera.stores import frames

    frame = pd.DataFrame([{"id": "a", "x": math.nan, "y": 1.0}, {"id": "b", "x": 2.0, "y": None}])
    keys, versions = frames.prepare(frame, Output("t", key="id")).rows.entries()
    assert dict(zip(keys, versions, strict=True)) == python([{"id": "a", "y": 1.0}, {"id": "b", "x": 2.0}])


def test_groups():
    rows = [{"id": "k", "n": 1}, {"id": "k", "n": 2}, {"id": "j", "n": 1}]
    assert python(rows) == python(rows[::-1]) == arrow(pa.Table.from_pylist(rows[::-1]))
    assert python(rows + rows[:1])[b"k"] != python(rows)[b"k"]  # a duplicate counts
    assert python([{"n": 1}, {"id": "k", "n": 2}][1:])[b"k"] == _native.group_digest(
        [{"n": 2}]
    )  # no key column
    one = python([{"id": "k", "n": 1}])[b"k"]
    assert one == _native.group_digest([{"id": "k", "n": 1}], "id") != _native.row_digest({"n": 1})


def test_revisions_render_alike():
    cases = [
        (pa.array([True]), True, b"true"),
        (pa.array([7], pa.uint16()), 7, b"7"),
        (pa.array([1.5]), 1.5, b"1.5"),
        (pa.array([D("1.20")], pa.decimal128(5, 2)), D("1.2"), b"1.2"),
        (pa.array([dt.date(2026, 1, 31)]), dt.date(2026, 1, 31), b"2026-01-31"),
        (
            pa.array([dt.datetime(2026, 1, 31, 8, 30, 0, 500000)], pa.timestamp("us")),
            dt.datetime(2026, 1, 31, 8, 30, 0, 500000),
            b"2026-01-31T08:30:00.5",
        ),
        (pa.array(["v1"]), "v1", b"v1"),
    ]
    for column, value, text in cases:
        keys, versions = _native.Rows.arrow(pa.table({"id": ["a"], "rev": column}), "id", "rev").entries()
        assert versions == [text]
        assert _native.Rows.records([{"id": "a", "rev": value}], "id", "rev").entries()[1] == [text]
        assert _native.revision_text(value) == text
    with pytest.raises(ValueError, match="cannot be null"):
        _native.revision_text(None)


def test_keyed_values_and_no_fallback():
    for v in (1, "x", [1, {"a": None}], {"k": [1.5]}):
        assert _native.Rows.values([("k", v)]).entries()[1] == [_native.value_digest(v)]
    for v in ({1, 2}, object(), D("Infinity")):
        with pytest.raises(ValueError, match="revision="):
            _native.value_digest(v)


def test_keys_are_str_or_int_everywhere():
    """One key rule for native extraction, a store's reading and removals: a
    `str`, or an `int` as its decimal text. Anything else — bytes included —
    is refused before anything is written."""

    from solera.sdk import Output
    from solera.stores import Patch, WriteError, key_rows, key_text, prepare

    assert python([{"id": 7}]).keys() == {b"7"} and key_text(7) == "7"
    out = Output("t", key="id")
    for bad in (b"b", True, 1.5, None):
        with pytest.raises(ValueError, match="a key must be a str or an int"):
            _native.Rows.records([{"id": bad}], "id")
        with pytest.raises(WriteError, match="a key must be a str or an int"):
            key_rows([{"id": bad}], out)
        with pytest.raises(WriteError, match="a key must be a str or an int"):
            prepare(Patch([], remove=[bad]), out)
    with pytest.raises(ValueError, match="strings or integers"):
        _native.Rows.arrow(pa.table({"id": pa.array([b"b"], pa.binary())}), "id")


def test_numpy_scalars_convert_or_fail_without_crashing():
    np = pytest.importorskip("numpy")
    assert _native.encode(np.int32(5)) == _native.encode(5)
    assert _native.encode(np.uint64(2**64 - 1)) == _native.encode(2**64 - 1)
    assert _native.encode(np.float32(1.5)) == _native.encode(1.5)
    assert _native.encode(np.bool_(True)) == _native.encode(True)
    for bad in (np.complex128(1j), np.datetime64("2026-01-01")):
        with pytest.raises(ValueError, match="revision="):
            _native.encode(bad)
    # Extended precision once recursed forever and killed the process: run it apart.
    import subprocess
    import sys

    code = (
        "import numpy as np\nfrom solera import _native\n"
        "try:\n    _native.encode(np.longdouble('1.25'))\nexcept ValueError as e:\n    print('refused', e)\n"
    )
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert done.returncode == 0, (done.returncode, done.stderr[-500:])
    if np.finfo(np.longdouble).bits > 64:  # extended precision: no exact float64 to take
        assert done.stdout.startswith("refused")
    else:  # longdouble is float64 here (Apple Silicon, MSVC): it encodes as one
        assert done.stdout == "" and _native.encode(np.longdouble("1.25")) == _native.encode(1.25)


def test_memoryview_is_bytes():
    data = b"abcdef"
    assert _native.encode(memoryview(data)) == _native.encode(data)
    assert _native.encode(memoryview(data)[1:4]) == _native.encode(b"bcd")
    assert _native.encode(memoryview(data)[::2]) == _native.encode(b"ace")  # not contiguous: logical order
    same(pa.array([data], pa.binary()), [memoryview(data)])


def test_duplicate_names_are_errors_even_when_null():
    for values in ([None, 1], [1, None], [None, None]):
        maps = pa.array([[("x", values[0]), ("x", values[1])]], pa.map_(pa.string(), pa.int64()))
        with pytest.raises(ValueError, match="appears twice"):
            arrow(pa.table({"id": ["k"], "m": maps}))
        struct = pa.StructArray.from_arrays(
            [pa.array([values[0]], pa.int64()), pa.array([values[1]], pa.int64())], names=["x", "x"]
        )
        with pytest.raises(ValueError, match="appears twice"):
            arrow(pa.table({"id": ["k"], "s": struct}))


# -- the dict fast path ----------------------------------------------------------------------


class Moment(dt.datetime):
    """A datetime subclass: read through its attributes, as before the fast path."""


def general(row: dict) -> bytes:
    """`row(r)` through the general walk: a mapping that is not a dict, a
    datetime that is not exactly one."""

    from types import MappingProxyType

    def slow(v):
        if type(v) is dt.datetime:
            fields = (v.year, v.month, v.day, v.hour, v.minute, v.second, v.microsecond)
            return Moment(*fields, v.tzinfo, fold=v.fold)
        return v

    return _native.row_digest(MappingProxyType({k: slow(v) for k, v in row.items()}), "id")


def test_the_dict_fast_path_digests_as_the_general_walk():
    class NoOffset(dt.tzinfo):
        def utcoffset(self, _):
            return None

    new_york = ZoneInfo("America/New_York")
    values = [
        None, True, False, 0, -1, 2**63 - 1, -(2**63), 2**63, -(2**64), 10**40,
        0.0, -0.0, math.nan, math.inf, 1e300, "", "é", "x" * 200, b"raw", [1, None], {"a": None, "b": 2},
        dt.datetime(2026, 3, 1, 12, 30, 5, 7), dt.datetime.min, dt.datetime.max,
        dt.datetime(2026, 3, 1, 12, tzinfo=dt.UTC),
        dt.datetime(2026, 3, 1, 12, tzinfo=dt.timezone(dt.timedelta(hours=2))),
        dt.datetime(2021, 11, 7, 1, 30, tzinfo=new_york),
        dt.datetime(2021, 11, 7, 1, 30, fold=1, tzinfo=new_york),
        dt.datetime(1, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=-5))),
        dt.datetime(2026, 3, 1, tzinfo=NoOffset()),
        dt.date(2026, 3, 1), dt.timedelta(days=-3, microseconds=5), D("1.50"),
    ]  # fmt: skip
    rows = [{"id": i, "v": v, "w": 1} for i, v in enumerate(values)]
    # Shapes that change from row to row, names in another order, equal names as other objects.
    rows += [
        {"id": 100, "w": 1, "v": 2},
        {"id": 101, "a": 1},
        {"id": 102},
        {"".join(["i", "d"]): 103, "".join("v"): 1, "w": None},
        {"id": 104, "v": 1, "w": 1},
    ]
    assert _native.row_digests(rows, "id") == b"".join(general(r) for r in rows)
    assert _native.row_digests(rows) == b"".join(_native.row_digest(r) for r in rows)
    for bad in ({"id": 1, 2: "x"}, {"id": 1, "\ud800": 1}, {"id": 1, "v": "\ud800"}, {"id": 1, "v": {1}}):
        with pytest.raises(ValueError):
            _native.row_digests([{"id": 0, "v": 1}, bad], "id")


def test_a_row_changed_while_it_is_read_is_no_crash():
    """A value's own code (a tzinfo's `utcoffset`) may change the row it is
    in: the walk holds what it has yet to encode."""

    rows = []

    class Clears(dt.tzinfo):
        def utcoffset(self, _):
            for row in rows:
                row.clear()
            return dt.timedelta(0)

    at = dt.datetime(2026, 1, 1, tzinfo=Clears())
    rows += [{"a": at, "b": "x" * 50, "c": [1, 2], "id": i} for i in range(3)]
    expected = _native.row_digest({"a": dt.datetime(2026, 1, 1, tzinfo=dt.UTC), "b": "x" * 50, "c": [1, 2]})
    assert _native.row_digests(rows[:1], "id") == expected


# -- what is hashed is what is stored ----------------------------------------------------------


def _stored_again(value, out):
    """Each key's version as prepared, and as its groups — what a store
    persists — prepare again."""

    from solera.stores.frames import prepare

    prepared = prepare(value, out)
    versions = dict(prepared.entries())
    groups = prepared.groups(sorted(versions))
    again = dict(prepare([row for group in groups for row in group], out).entries())
    return versions, again


def test_a_dataframe_is_stored_as_it_is_hashed():
    """A DataFrame's missing values — NaN, NaT, None, NA — are null where it
    is hashed and where it is stored; read through pandas alone, it digests
    as the same rows given as mappings would."""

    from solera.sdk import Output
    from solera.stores.frames import prepare

    out = Output("t", key="id")
    frame = pd.DataFrame(
        {
            "id": ["a", "b", "c"],
            "x": [1.5, math.nan, 2.0],
            "at": pd.to_datetime(
                ["2026-01-01 00:00:00.000000001", None, "2026-01-03 00:00:00.000000000"], utc=True
            ),
            "s": ["p", None, math.nan],
            "n": pd.array([1, None, 3], dtype="Int64"),
            "d": [D("1.20"), None, D("3")],
        }
    )
    versions, again = _stored_again(frame, out)
    assert versions == again
    rows = [
        {"id": "a", "x": 1.5, "at": frame["at"][0], "s": "p", "n": 1, "d": D("1.20")},
        {"id": "b"},
        {"id": "c", "x": 2.0, "at": frame["at"][2], "n": 3, "d": D("3")},
    ]
    assert dict(prepare(rows, out).entries()) == versions
    # Timestamps with no nanoseconds are read as `datetime`s: the same instants.
    local = pd.DataFrame(
        {"id": ["a", "b"], "t": pd.to_datetime(["2026-01-01 10:00", None]).tz_localize("Europe/Paris")}
    )
    versions, again = _stored_again(local, out)
    assert versions == again == dict(prepare([{"id": "a", "t": local["t"][0]}, {"id": "b"}], out).entries())


def test_arrow_is_stored_as_it_is_hashed():
    """Arrow values become Python values that digest alike: a map a dict, an
    interval, nanoseconds, decimals, structs and lists as they were."""

    from solera.sdk import Output

    out = Output("t", key="id")
    table = pa.table(
        {
            "id": ["a", "b"],
            "m": pa.array([[("x", 1)], None], pa.map_(pa.string(), pa.int64())),
            "iv": pa.array([pa.MonthDayNano([1, 2, 3]), None], pa.month_day_nano_interval()),
            "at": pa.array([1_000_000_001, None], pa.timestamp("ns", tz="UTC")),
            "d": pa.array([D("1.20"), D("-3")], pa.decimal128(5, 2)),
            "st": pa.array([{"p": 1, "q": None}, None], pa.struct([("p", pa.int64()), ("q", pa.string())])),
            "l": pa.array([[1, None], []], pa.list_(pa.int64())),
        }
    )
    versions, again = _stored_again(table, out)
    assert versions == again


async def test_a_dataframe_read_back_from_a_store_digests_as_written(tmp_path):
    """The round trip through FileStore: written, read back, prepared again."""

    from solera.keys.index import key_str
    from solera.sdk import Output
    from solera.stores import FileStore, Keys
    from solera.stores.frames import prepare

    from tests.conftest import scope

    out = Output("t", key="id")
    frame = pd.DataFrame({"id": ["a", "a", "b"], "x": [1.0, math.nan, 2.5]})
    store = FileStore(tmp_path)
    written = await store.store(frame, None, scope(out, generation=1))
    versions = dict(prepare(frame, out).entries())
    read = await store.load(written.ref, None, Keys({k: (v, 1) for k, v in versions.items()}))
    assert {key_str(k.encode()): v for k, v in prepare(read, out).entries()} == versions


def test_the_core_reads_plain_python_only():
    """The default `prepare` knows no DataFrame or Arrow table: a store that
    takes them reads them itself (`solera.stores.frames`)."""

    from solera.sdk import Output
    from solera.stores import WriteError, frames, prepare

    out = Output("t", key="id")
    for value in (pd.DataFrame({"id": ["a"]}), pa.table({"id": ["a"]})):
        with pytest.raises(WriteError, match="reads plain rows"):
            prepare(value, out)
        assert dict(frames.prepare(value, out).entries()) == dict(prepare([{"id": "a"}], out).entries())
