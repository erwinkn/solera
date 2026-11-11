"""DataFrames and Arrow data, for a store that takes them (docs/stores.md).

The core reads plain Python only (`solera.stores.prepare`). A store taking
pandas DataFrames or Arrow data (anything with `__arrow_c_stream__`)
reads them here: `prepare` for its `Store.prepare`, `can_store` in its
`can_store`, `rows` and `materialize` where it writes or loads them. A
DataFrame is read a column at a time through pandas alone, every missing
value — NaN, NaT, None, NA — None, where it is stored; Arrow is read in place and stored with maps as dicts. pandas and
pyarrow are imported only for a value of their own type. Of a keyed
write, only the key column is read for the key index (docs/versions.md).

`KINDS` are the value kinds a store types the columns of a table it
creates by: `kind_of` a Python value, `frame_kinds` and `arrow_kinds` of
whole columns, from pyarrow's inferred schema when it is installed, else
pandas' dtypes.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Mapping
from typing import Any

from ..sdk import KEYS, Output
from . import WriteError, key_text, take_list
from . import prepare as _prepare

KINDS = (
    "boolean",
    "integer",
    "float",
    "decimal",
    "text",
    "instant",
    "timestamp",
    "date",
    "time",
    "interval",
    "bytes",
)


def is_frame(value: Any) -> bool:
    """A pandas (or geopandas) DataFrame, told by its type's name and module."""

    t = type(value)
    return t.__name__ in ("DataFrame", "GeoDataFrame") and t.__module__.split(".")[0] in (
        "pandas",
        "geopandas",
    )


def is_frame_type(t: Any) -> bool:
    return (
        isinstance(t, type)
        and t.__name__ in ("DataFrame", "GeoDataFrame")
        and t.__module__.split(".")[0] in ("pandas", "geopandas")
    )


def can_store(t: Any) -> bool:
    """Whether a producer's return annotation is a DataFrame or Arrow table."""

    return is_frame_type(t) or (isinstance(t, type) and t.__module__.split(".")[0] == "pyarrow")


def prepare(write: Any, output: Output):
    """`solera.stores.prepare`, reading DataFrames and Arrow data too."""

    return _prepare(write, output, read)


def read(content: Any, output: Output):
    """`(rows, take, empty, kinds)` of a rows output's DataFrame, Arrow data,
    or by-key mapping holding DataFrames; None for plain Python."""

    from ..keys import Rows

    if output.key in (None, KEYS):
        return None
    empty: list[str] = []
    by_key = isinstance(content, Mapping) and any(is_frame(g) for g in content.values())
    if by_key:
        content, empty = _by_key(content, output)
    if is_frame(content):
        names, columns = _frame(content, output.name)
        return (
            Rows.columns(names, columns, output.key),
            _column_taker(names, columns),
            empty,
            frame_kinds(content),
        )
    if hasattr(content, "__arrow_c_stream__"):
        if type(content).__module__.startswith("pyarrow") and type(content).__name__ != "Table":
            import pyarrow as pa  # a stream reads once: hold it

            content = pa.table(content)
        return Rows.arrow(content, output.key), _arrow_taker(content), empty, arrow_kinds(content)
    if by_key:  # DataFrames among lists of rows: rows, every one
        return Rows.records(content, output.key), functools.partial(take_list, content), empty, None
    return None


def _by_key(content: Mapping, output: Output):
    """A by-key write holding DataFrames: one DataFrame, each group's key
    stamped, when every group is one; else rows. Keys given no rows."""

    column, frames, rows, empty = output.key, [], [], []
    for key, group in content.items():
        if not isinstance(key, str):
            raise WriteError(f"{output.name}: a by-key write takes str keys, got {type(key).__name__}")
        found = group if is_frame(group) else None
        if found is not None:
            if column in found.columns and (found[column].astype(str) != key).any():
                raise WriteError(f"{output.name}: rows of {key!r} carry another {column!r}")
            if len(found):
                frames.append(found.assign(**{column: key}))
            else:
                empty.append(key)
            continue
        group_rows = rows_of(group, output.name)
        for row in group_rows:
            if column in row and key_text(row[column]) != key:
                raise WriteError(f"{output.name}: a row of {key!r} carries {column}={row[column]!r}")
            row[column] = key
        rows.extend(group_rows)
        if not group_rows:
            empty.append(key)
    if frames and not rows:
        import pandas as pd

        return pd.concat(frames, ignore_index=True), empty
    return rows + [r for f in frames for r in rows_of(f, output.name)], empty


def rows_of(value: Any, name: str) -> list[dict]:
    """Rows as dicts — a list of mappings, a DataFrame (its missing values
    None), Arrow data (maps as dicts)."""

    if value is None:
        return []
    if is_frame(value):
        names, columns = _frame(value, name)
        return _column_taker(names, columns)(None)
    if hasattr(value, "__arrow_c_stream__"):
        return _arrow_taker(value)(None)
    if isinstance(value, list) and all(isinstance(r, Mapping) for r in value):
        return [dict(r) for r in value]
    raise WriteError(f"{name}: expected rows (list[dict], a DataFrame or Arrow), got {type(value).__name__}")


def is_table(value: Any) -> bool:
    """A pyarrow Table, told by its type's name and module."""

    t = type(value)
    return t.__name__ == "Table" and t.__module__.split(".")[0] == "pyarrow"


def is_table_type(t: Any) -> bool:
    return isinstance(t, type) and t.__name__ == "Table" and t.__module__.split(".")[0] == "pyarrow"


def materialize(items: Any, t: Any, columns: list[str] | None = None):
    """Loaded rows as `t`: a DataFrame or an Arrow table when asked for one,
    else a list of rows. `items` is a list of rows, or an Arrow table read
    in place (a keyed source's, `served`). `columns` name an empty list's."""

    table = is_table(items)
    if is_table_type(t):
        if table:
            return items
        import pyarrow as pa

        if not items and columns:
            return pa.table({c: pa.nulls(0) for c in columns})
        return pa.Table.from_pylist(items)
    if is_frame_type(t):
        import pandas as pd

        if table:  # the DataFrame its rows make, built by column: the dict form's, dtypes included
            maps = {"maps_as_pydicts": "strict"} if any(_has_map(c) for c in items.schema.types) else {}
            return pd.DataFrame(
                {n: c.to_pylist(**maps) for n, c in zip(items.column_names, items.columns, strict=True)}
            )

        return pd.DataFrame(items, columns=columns if not items and columns else None)
    return _arrow_taker(items)(None) if table else items


def served(table: Any, take: bytes | None, version: str, t: Any, names: list | None = None):
    """A keyed source's rows served as one Arrow table (`Loaded(table,
    version=column)`) as `t`, without the version column: the rows `take`
    names (row numbers, u32; None: every row, as it is), in its order. With
    `names`, the key each row is, `t` is each key's own: `{key: T}`, each a
    group of one row, as a dict answer's are."""

    import pyarrow as pa

    table = table.drop_columns([version])
    rows = (
        pa.array([], pa.uint32())
        if take is None
        else pa.Array.from_buffers(pa.uint32(), len(take) // 4, [None, pa.py_buffer(take)])
    )
    try:  # every table, so whether it loads never hangs on the keys asked
        taken = table.take(rows)
    except pa.ArrowNotImplementedError as e:
        raise ValueError(f"Arrow cannot select rows of this table, so it cannot serve keys: {e}") from None
    if take is not None:
        table = taken
    if names is None:
        return materialize(table, t)
    return {k: materialize([row], t) for k, row in zip(names, materialize(table, list), strict=True)}


def _frame(frame: Any, name: str) -> tuple[list[str], list[list]]:
    """A DataFrame's columns as Python values, each missing one None."""

    if not frame.columns.is_unique:
        raise WriteError(f"{name}: a DataFrame's column names must be unique")
    names, columns = [], []
    for column, series in frame.items():
        if not isinstance(column, str):
            raise WriteError(f"{name}: column names must be strings, got {column!r}")
        missing = series.isna().to_numpy()
        values = _column_values(series, missing)
        if missing.any():
            for i in missing.nonzero()[0].tolist():
                values[i] = None
        names.append(column)
        columns.append(values)
    return names, columns


def _column_values(series: Any, missing: Any) -> list:
    """A column's values as Python objects. Timestamps with no nanoseconds
    become `datetime`s, the same instants as pandas' `Timestamp`s, made
    and read faster."""

    if series.dtype.kind == "M" and not (series.dt.nanosecond.to_numpy()[~missing] != 0).any():
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)  # an array now, a Series in pandas 3
            return list(series.dt.to_pydatetime())
    return series.tolist()


def _column_taker(names: list[str], columns: list[list]) -> Callable:
    n = len(columns[0]) if columns else 0

    def take(rows):
        return [
            {name: column[r] for name, column in zip(names, columns, strict=True)}
            for r in (range(n) if rows is None else rows)
        ]

    return take


def _arrow_taker(data: Any) -> Callable:
    """Rows of Arrow data as Python values: maps as dicts. pyarrow is imported only here, for its own data, or
    when another library's has rows to give."""

    table = None

    def take(rows):
        nonlocal table
        import pyarrow as pa

        if table is None:
            table = data if isinstance(data, pa.Table) else pa.table(data)
        if rows is None:
            taken = table
        elif isinstance(rows, range) and rows.step == 1:
            taken = table.slice(rows.start, len(rows))
        elif isinstance(rows, memoryview):
            taken = table.take(pa.Array.from_buffers(pa.uint32(), len(rows), [None, pa.py_buffer(rows)]))
        else:
            taken = table.take(list(rows))
        if any(_has_map(t) for t in table.schema.types):  # a third the speed: only where a map is
            return taken.to_pylist(maps_as_pydicts="strict")
        return taken.to_pylist()

    return take


def _has_map(t: Any) -> bool:
    """Whether an Arrow type holds a map, at any depth."""

    import pyarrow as pa

    if pa.types.is_map(t):
        return True
    if isinstance(t, pa.BaseExtensionType):
        return _has_map(t.storage_type)
    if pa.types.is_dictionary(t):
        return _has_map(t.value_type)
    return any(_has_map(t.field(i).type) for i in range(t.num_fields))


# -- kinds --------------------------------------------------------------------------------


def kind_of(value: Any) -> str | None:
    """A Python value's kind; None for one no column kind holds."""

    import datetime as dt
    from decimal import Decimal

    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, Decimal):
        return "decimal"
    if isinstance(value, str):
        return "text"
    if isinstance(value, dt.datetime):
        return "timestamp" if value.utcoffset() is None else "instant"
    if isinstance(value, dt.date):
        return "date"
    if isinstance(value, dt.time):
        return "time"
    if isinstance(value, dt.timedelta):
        return "interval"
    if isinstance(value, bytes | bytearray | memoryview):
        return "bytes"
    if type(value).__module__ == "numpy" and hasattr(value, "dtype"):
        return {"b": "boolean", "i": "integer", "u": "integer", "f": "float"}.get(value.dtype.kind)
    return None


def frame_kinds(frame: Any) -> dict[str, str | None]:
    """A DataFrame's column kinds: from pyarrow's inferred schema when it is
    installed and infers one, else from pandas' dtypes; None where neither
    says (an object column: its values do)."""

    try:
        import pyarrow as pa
    except ImportError:
        pa = None
    if pa is not None:
        try:
            schema = pa.Schema.from_pandas(frame, preserve_index=False)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            schema = None
        if schema is not None:
            return {f.name: _arrow_kind(f.type) for f in schema}
    return {str(c): _dtype_kind(frame[c].dtype) for c in frame.columns}


def arrow_kinds(data: Any) -> dict[str, str | None] | None:
    schema = getattr(data, "schema", None)
    if schema is None or not type(data).__module__.startswith("pyarrow"):
        return None
    return {f.name: _arrow_kind(f.type) for f in schema}


def _arrow_kind(t: Any) -> str | None:
    import pyarrow as pa

    types = pa.types
    if types.is_boolean(t):
        return "boolean"
    if types.is_integer(t):
        return "integer"
    if types.is_floating(t):
        return "float"
    if types.is_decimal(t):
        return "decimal"
    if types.is_string(t) or types.is_large_string(t):
        return "text"
    if types.is_timestamp(t):
        return "instant" if t.tz is not None else "timestamp"
    if types.is_date(t):
        return "date"
    if types.is_time(t):
        return "time"
    if types.is_duration(t):
        return "interval"
    if types.is_binary(t) or types.is_large_binary(t) or types.is_fixed_size_binary(t):
        return "bytes"
    return None


def _dtype_kind(dtype: Any) -> str | None:
    kind = getattr(dtype, "kind", None)
    if kind == "M":
        return "instant" if getattr(dtype, "tz", None) is not None else "timestamp"
    return {"b": "boolean", "i": "integer", "u": "integer", "f": "float", "m": "interval"}.get(kind)
