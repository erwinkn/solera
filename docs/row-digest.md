# Row digests (version 1)

How a keyed output's content becomes the version its key index holds
(`object-store-state.md` §6). One grammar, implemented once in Rust
(`native/src/digest.rs`) and fed from Python values and from Arrow arrays
alike: the same logical rows get the same version however they arrive.
`tests/sdk/test_row_digest.py` holds golden vectors for both sides.

## What a key's version is

Every key is a **group**: the rows of a write that carry it. A row is
usually alone in its group; a key whose rows come from one parsed file
holds them all. A key with no rows does not exist: a patch giving a key
none removes it. The version of a key is:

| Output | Version |
|---|---|
| rows, `key="col"` | `group(rows)`: the multiset of the key's rows, each without its key column |
| rows, `key="col", revision="rev"` | the text of the rows' `rev` value (§ Revisions), which every row of the group must share |
| `keyed=True` | `value(v)` of the key's value |
| a partition set | `1` |

```
digest(x)    = XXH3-128(x), seed 0, as 16 bytes, little-endian
row(r)       = digest(0x01 ‖ "R" ‖ record(r without the key column and the store's own columns))
group(rows)  = digest(0x01 ‖ "G" ‖ varint(n) ‖ row(r₁) ‖ … ‖ row(rₙ))   — the n row digests sorted bytewise
value(v)     = digest(0x01 ‖ "V" ‖ enc(v))
```

`0x01` is the grammar version; a change to anything below bumps it, and
every key's version changes once.

**Keys.** A key is a `str`, as its UTF-8 bytes, or an `int` (not a `bool`)
as its decimal text — in Arrow, a string or integer column. Native
extraction, store grouping (`solera.stores.key_text`) and removals follow
this one rule; any other key — `bytes`, a float, None — fails the write
before anything is written.

**A store's own columns.** A column the store adds to every row itself —
PostgresStore's `partition_column` — is no part of the row: a row digests
the same whether it is written with or without it, or read back with it
(`Store.stamped`, passed as `exclude`).

So a group is order-free (rows in another order: the same version), counts
duplicates (a row written twice: a different version), and ignores whether
the rows carry their key column. A single row's version is `group([r])`.

## Values

`enc(v)` is a tag byte and a payload. All fixed-width integers are
little-endian; `varint` is unsigned LEB128; `len` is a varint byte count.

| Tag | Logical type | Payload | From Python | From Arrow |
|---|---|---|---|---|
| `n` | null | — | `None`, `pandas.NA`, `pandas.NaT` | a null slot, `Null` |
| `o` | boolean | 1 byte, 0 or 1 | `bool`, `numpy.bool_` (checked before `int`) | `Boolean` |
| `i` | integer | `len`, ASCII decimal: no leading zeros, `-` only when negative | `int` of any size, `numpy` integers | every signed and unsigned width |
| `f` | float | f64 bits, 8 bytes; `-0.0` as `0.0`, every NaN as `0x7FF8000000000000` | `float`, `numpy` floats of at most 64 bits (extended precision is refused) | `Float16`, `Float32`, `Float64` (widened exactly) |
| `e` | decimal | `len`, ASCII decimal unscaled value; zigzag varint exponent | `decimal.Decimal` (finite) | `Decimal128`, `Decimal256` |
| `s` | string | `len`, UTF-8 | `str` | `Utf8`, `LargeUtf8`, `Utf8View` |
| `b` | bytes | `len`, bytes | `bytes`, `bytearray`, `memoryview` (its bytes in logical C order, whatever its shape or strides) | `Binary`, `LargeBinary`, `BinaryView`, `FixedSizeBinary` |
| `D` | date | days since 1970-01-01, i64 | `datetime.date` | `Date32`; `Date64` (rounded down to whole days) |
| `h` | time of day | nanoseconds since midnight, i64 | `datetime.time` without a timezone | `Time32`, `Time64` |
| `t` | timestamp, naive | nanoseconds since 1970-01-01T00:00, wall clock, i128 | `datetime.datetime` without a timezone; `pandas.Timestamp` with its nanoseconds | `Timestamp` without a timezone, any unit |
| `T` | timestamp, an instant | nanoseconds since 1970-01-01T00:00Z, i128 | `datetime.datetime` with a timezone | `Timestamp` with a timezone, any unit |
| `u` | duration | nanoseconds, i128 | `datetime.timedelta`; `pandas.Timedelta` with its nanoseconds | `Duration`, any unit |
| `v` | calendar interval | months i32, days i32, nanoseconds i64 | — | `Interval`, any unit |
| `l` | list | `varint(n)`, then each element's `enc` (nulls kept) | `list`, `tuple` | `List`, `LargeList`, `FixedSizeList`, `ListView`, `LargeListView` |
| `r` | record | `varint(n)`, then per field `len`, name UTF-8, `enc(value)` — fields sorted bytewise by name, null fields left out | a `Mapping` with `str` keys | `Struct`; `Map` with string keys |
| `m` | map | `varint(n)`, then per entry `enc(key)`, `enc(value)` — sorted bytewise by `enc(key)`, null values left out | a `Mapping` with other keys | `Map` with other keys |

A row is a record: `record(r)` is `enc` of the row as an `r` value.

Rules where representations could disagree:

- **Equal values encode equally.** `5` is `i 1 "5"` whether it is a Python
  `int`, an `Int8` or a `UInt64`; `Decimal("1.20")` and a `Decimal128(10, 2)`
  holding 120 are both `e 2 "12" (-1)`, trailing zeros stripped (zero is
  `"0"` with exponent 0); a timestamp in seconds and in nanoseconds of the
  same instant are equal. Distinct logical types never are: `1`, `1.0`,
  `Decimal(1)` and `True` are four different values.
- **Missing is null.** A record field that is null and one that is absent
  are the same, so a `list[dict]` whose rows omit a field and an Arrow table
  that pads it with nulls digest alike. Nulls inside a list are kept. NaN is
  a float, not a null — but pandas marks missing values with NaN (and NaT,
  None, NA), and a DataFrame is read column by column through pandas with
  every missing value null: where it is hashed and where it is stored.
- **Timezones.** An aware timestamp is an instant: `12:00+02:00` equals
  `10:00Z`, and the zone's name is not part of it. A naive timestamp is a
  wall-clock reading and never equals an aware one. A `datetime.time` with a
  timezone is an error.
- **Precision.** Timestamps and durations keep nanoseconds: Python's
  microseconds and pandas' nanoseconds are exact, and no Arrow unit loses
  anything.
- **Records versus maps.** A string-keyed mapping is a record whether it is
  a Python `dict`, an Arrow `Struct` or an Arrow `Map<Utf8, …>`. Duplicate
  names or keys — a struct with two fields of one name, a map entry
  repeated — and null map keys are errors, checked before null fields are
  left out: `[("x", None), ("x", 1)]` is an error, not `{"x": 1}`.
- **Not supported** — sets, NaN and infinite decimals, NumPy scalars other
  than booleans, integers and floats of at most 64 bits, any other object: an
  error asking for an explicit `revision=` on the output. There is no
  fallback encoding: a value either has a canonical form or the write
  fails.

## Revisions

A declared `revision=` column's value becomes the version's text,
rendered the same from Python and Arrow:

| Value | Text |
|---|---|
| string, bytes | as they are |
| boolean | `true`, `false` |
| integer | decimal |
| float | shortest round-trip form, Rust's `{:?}` (`1.0`, `0.1`, `1e300`); `-0.0` as `0.0`; `NaN`, `inf`, `-inf` |
| decimal | plain decimal, normalized (`1.2`, `1000`) |
| date | `YYYY-MM-DD` |
| time | `HH:MM:SS`, then `.` and up to nine digits when not whole |
| timestamp | `YYYY-MM-DDTHH:MM:SS[.fffffffff]`, and `Z` for an instant (in UTC) |
| duration | nanoseconds, decimal, and `ns` |
| interval | `{months}m{days}d{nanoseconds}ns` |

A null revision, or a list, record or map, is an error; so are two rows of
one key with different revisions.

## Where versions come from

The harness reads a keyed write once as native `Rows`
(`solera.stores.prepare`, `per-key-processing.md` §7): `Rows.records` for
Python rows, `Rows.columns` for a pandas DataFrame (column by column,
through pandas alone), `Rows.arrow` for Arrow data, `Rows.values` for
`keyed=True`, `Rows.keys` for partition sets; a store reading types of its
own reads them itself (`Store.prepare`), and one adding columns leaves
them out (`Store.stamped`). What a store persists is taken from the same
reading — a DataFrame's missing values None, an Arrow map a dict — so it
digests as it was hashed. Keys are sorted
natively, each run of equal keys is folded into one version, and no
per-key Python object is made. A patch's keys, versions and removes
become one `SortedRun`, native through every resolver — the sparse
reader, the streaming patch, the engine's cache — and encoded only to
cross the wire. The store's version (`Rows.digest()`, every key and
version) and the groups it writes (`Rows.find`) read the same `Rows`.

A `Sql` write's rows never pass through the worker: after writing, its
store reads them back sorted by key — the key and the revision column, or
every column without one (the partition column aside) — a chunk at a time,
and the harness versions them as any rows, natively. PostgresStore's
values arrive as psycopg's Python types, so a row written by `Sql` and the
same row returned from Python digest alike. The same read-back (`scan`)
serves a patch that must take in what a dead `Sql` writer left: the slice
streams back a chunk at a time, the patch's rows merged in, into the
native replacement — memory is a chunk and the patch, not the slice.

## Golden vectors

`tests/sdk/test_row_digest.py` pins them, and recomputes the productions
from this spec with an independent XXH3 implementation:

| Value | `enc(v)` |
|---|---|
| `-12` (any integer type) | `69 03 2D 31 32` — `i`, 3, `-12` |
| `Decimal("1.20")`, `Decimal128(10, 2)` 120 | `65 02 31 32 01` — `e`, 2, `12`, zigzag(-1) |
| `1.5` | `66 00 00 00 00 00 00 F8 3F` |
| `datetime(1970, 1, 1, 0, 0, 0, 1)` | `74`, then 1000 as 16 bytes |
| `datetime(1970, 1, 1, 1, tzinfo=+01:00)` | `54`, then 16 zero bytes — the instant 1970-01-01T00:00Z |
| `[1, None]` | `6C 02 69 01 31 6E` |
| `{"b": 1, "a": None}` | `72 01 01 62 69 01 31` — the null field is left out |
| `{2: "x", 1: "y"}` | `6D 02 69 01 31 73 01 79 69 01 32 73 01 78` |

| Production | Of | Hex |
|---|---|---|
| `row` | `{"id": "k", "n": 1, "s": "x", "at": datetime(2026, 1, 1), "tags": ["a"]}`, key `id` | `e374e4ff321e8685f3ffc3a2c50fca36` |
| `group` | that row alone | `540c629d9d908e7efd1675cadd04cb94` |
| `value` | `[1, "a"]` | `626d0beaca5a4a2bf4a99e70a91277ad` |

Beyond these, the test checks Python against Arrow for every rule above:
integers across widths and signs up to `2⁶⁴ − 1`, booleans apart from
integers, floats with `-0.0` and NaN, decimals with trailing zeros and
256-bit decimals, nanosecond, microsecond and second timestamps, instants
across zones, naive against aware, dates against timestamps, times and
durations, lists, structs and string-keyed maps as records, typed maps,
missing against null fields (and a DataFrame's missing values), groups
in two orders and with a duplicate, revisions rendered from both sides,
and no fallback for sets, objects or infinite decimals.
