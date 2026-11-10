"""Staleness on the new tracking core (build plan B5): whether a keyed
incremental input owes a partition anything, answered on demand by a plan of
one key from the start of its edge (SOLE-14's stale index replaces it, B7).
The plan reads the edge's progress and the upstream's layers as an attempt's
would, records nothing, and runs as a heavy call."""

from __future__ import annotations

from . import Core
from .prepare import conditions


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


async def taken_after(core: Core, input, empty: str, edge: str | None = None) -> bool:
    """Whether upstream holds a key the input's patterns take past `empty`
    (`""`: anywhere): one the partition has not processed since it last
    started over, so it is not complete (STA-10, D176); with `edge`, a key
    a key list processed there does not count. One native scan, stopping at
    the first such key."""

    id_ = core.m.core_outputs.get((input.output, input.partition))
    if id_ is None:
        return False
    core.attach(id_)
    return await core.run(
        core.tracking.taken,
        id_.encode(),
        input.spec.get("patterns"),
        empty or None,
        edge.encode() if edge is not None else None,
    )
