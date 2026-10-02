"""Which scopes a request selects (§7, §8): a pure function of an asset's
dimensions, the selection, the time and what the heads say — no engine
state, no I/O, nothing awaited.

A selection is answered without enumerating the partition domain unless it
asks for the whole of it: one explicit scope checks each of its parts'
membership, `latest` builds only the latest time window, and only `all` and
`missing` list every combination — up to `MAX_SCOPES`, past which they are an
error rather than a silent truncation.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from functools import partial
from itertools import product

from solera.sdk import MAX_PARTITION_KEYS, TimePartitions, canonical_partition, split_partition

MAX_SCOPES = 100_000  # scopes a request may enumerate


def time_partitions(dim: dict) -> TimePartitions:
    return TimePartitions(
        dim["start"],
        dim["every"],
        end=dim.get("end"),
        end_offset=dim.get("end_offset"),
        timezone=dim.get("timezone") or "UTC",
        format=dim.get("format"),
    )


def canonical(dims: dict, key: str) -> str:
    if len(dims) == 1:
        return str(key)
    return canonical_partition(dims, split_partition(dims, key))


def dim_keys(dim: dict, now: dt.datetime, elements: Callable[[str], list[str] | None]) -> list[str]:
    """One dimension's current keys, enumerated."""

    if dim["kind"] == "static":
        return [str(k) for k in dim["keys"]]
    if dim["kind"] == "time":
        return time_partitions(dim).keys(now, MAX_PARTITION_KEYS)
    return sorted(elements(dim["output"]) or ())


def _size(dim: dict, now: dt.datetime, elements) -> int:
    if dim["kind"] == "static":
        return len(dim["keys"])
    if dim["kind"] == "time":
        return time_partitions(dim).count(now, MAX_SCOPES)
    return len(elements(dim["output"]) or ())


def size(dims: dict, now: dt.datetime, elements) -> int:
    """How many scopes `dims` spans, counted per dimension — never enumerated.
    A cron time dimension is counted only to just past `MAX_SCOPES`."""

    total = 1
    for dim in dims.values():
        total *= _size(dim, now, elements)
    return total


def enumerate_scopes(dims: dict, now: dt.datetime, elements, *, what: str = "") -> list[str]:
    """Every scope of `dims`, in dimension order — refused past `MAX_SCOPES`."""

    if not dims:
        return [""]
    total = size(dims, now, elements)
    if total > MAX_SCOPES:
        raise ValueError(
            f"{what or 'the selection'} spans {total} partitions, more than {MAX_SCOPES}: "
            "select partitions explicitly, or the latest"
        )
    keys = [dim_keys(dim, now, elements) for dim in dims.values()]
    return [canonical_partition(dims, dict(zip(dims, combo, strict=True))) for combo in product(*keys)]


def membership(dims: dict, now: dt.datetime, elements) -> Callable[[str], bool]:
    """Whether a scope is one `enumerate_scopes` would list: canonical, each
    of its parts a member of its dimension — checked part by part, never by
    enumeration. Each dimension's members are read once, for any number of
    scopes."""

    if not dims:
        return lambda scope: scope == ""
    checks = {}
    for name, dim in dims.items():
        if dim["kind"] == "time":
            checks[name] = partial(time_partitions(dim).contains, as_of=now)
        elif dim["kind"] == "static":
            checks[name] = {str(k) for k in dim["keys"]}.__contains__
        else:
            checks[name] = set(elements(dim["output"]) or ()).__contains__

    def member(scope: str) -> bool:
        try:
            parts = split_partition(dims, scope)
        except ValueError:
            return False
        return canonical_partition(dims, parts) == scope and all(
            check(parts[name]) for name, check in checks.items()
        )

    return member


def select_scopes(
    dims: dict,
    selection,
    *,
    now: dt.datetime,
    elements: Callable[[str], list[str] | None],
    missing: Callable[[str], bool],
    what: str = "",
) -> list[str]:
    """The scopes `selection` names: `"all"` (or None), `"latest"`,
    `"missing"`, or explicit keys. `elements(output)` gives a set
    dimension's members; `missing(scope)` whether a scope lacks a complete
    head."""

    if selection is None or selection == "all":
        return enumerate_scopes(dims, now, elements, what=what)
    if selection == "missing":
        return [s for s in enumerate_scopes(dims, now, elements, what=what) if missing(s)]
    if selection == "latest":
        if not dims:
            return [""]
        chosen = []
        for dim in dims.values():
            if dim["kind"] == "time":
                latest = time_partitions(dim).latest(now)
                chosen.append([latest] if latest else [])
            else:
                chosen.append(dim_keys(dim, now, elements))
        return [canonical_partition(dims, dict(zip(dims, combo, strict=True))) for combo in product(*chosen)]
    wanted = list(dict.fromkeys(selection or ()))
    if not dims:
        return [""] if "" in wanted else []
    member, out = membership(dims, now, elements), []
    for key in wanted:
        try:
            scope = canonical(dims, key)
        except (ValueError, KeyError):
            continue
        if scope not in out and member(scope):
            out.append(scope)
    return out
