"""Staleness on the new tracking core (build plan B5): whether a keyed
incremental input owes a partition anything, answered on demand by a plan of
one key from the start of its edge (SOLE-14's stale index replaces it, B7).
The plan reads the edge's progress and the upstream's layers as an attempt's
would, records nothing, and runs as a heavy call."""

from __future__ import annotations

from solera.patterns import Matcher

from . import Core
from .prepare import conditions

COVER_PAGE = 10_000  # keys a completeness check reads at a time


async def owes(core: Core, asset: str, partition: str, input, planner, inputs) -> bool:
    """Whether `input` owes the partition a key: under its patterns and the
    conditions it would be processed under now. An edge never opened, or
    opened on an upstream's earlier life, owes whatever upstream holds."""

    m = core.m
    upstream = m.core_outputs.get((input.output, input.partition))
    found = m.core_output(input.output, input.partition)
    if upstream is None or found is None:
        return False
    edge = m.core_edges.get((asset, partition, input.param))
    if edge is None or edge[1] != upstream:
        return found["live"] > 0
    record = m.partition(asset, partition)
    base = {
        "context": core.engine._context(planner, inputs),
        "definition": record.get("definition") or "",
    }
    core.attach(upstream)
    request = {
        "mode": "incremental",
        "reset": False,
        "after": None,
        "limit": 1,
        "inputs": [{"edge": edge[0].encode(), "conditions": conditions(core.engine, input, base)}],
    }
    summary = await core.run(core.tracking.plan, request)
    return bool(summary["inputs"][0]["keys"])


async def taken_after(core: Core, input, empty: str) -> bool:
    """Whether upstream holds a key the input's patterns take past `empty`
    (`""`: anywhere): one the partition has not processed since it last
    started over, so it is not complete (STA-10, D176). Pages until the
    first such key."""

    found = core.m.core_output(input.output, input.partition)
    if found is None:
        return False
    core.attach(found["id"])
    taken, after = Matcher(input.spec.get("patterns")), empty or None
    while True:
        rows, after = await core.run(
            core.index.scan, found["layers"], found["head"], after=after, limit=COVER_PAGE
        )
        if any(taken(row[0]) for row in rows):
            return True
        if after is None:
            return False
