"""Staleness on the new tracking core (build plan B5): whether a keyed
incremental input owes a partition anything, answered on demand by a plan of
one key from the start of its edge (SOLE-14's stale index replaces it, B7).
The plan reads the edge's progress and the upstream's layers as an attempt's
would, records nothing, and runs as a heavy call."""

from __future__ import annotations

from ..model import edge_id, output_id
from . import Core, Generation, Redeployed
from .prepare import conditions


async def owes(core: Core, asset: str, partition: str, input, planner, inputs) -> bool:
    """Whether `input` owes the partition a key: under its patterns and the
    conditions it would be processed under now. An edge the partition has not
    read owes whatever upstream holds."""

    m = core.m
    upstream = output_id(input.output, input.partition)
    found = m.core_output(input.output, input.partition)
    if found is None:
        return False
    edge = edge_id(asset, partition, input.param)
    if not (m.edge(asset, partition, input.param) or {}).get("read"):
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
        "inputs": [{"edge": edge, "conditions": conditions(core.engine, input, base)}],
    }
    generation = Generation(m)
    try:
        with generation:
            summary = await core.run(core.tracking.plan, request)
        generation.check()
    except Redeployed:  # a deploy while it planned: not known, so owed
        return True
    return bool(summary["inputs"][0]["keys"])


async def taken_after(core: Core, input, empty: str, edge: bytes | None = None) -> bool:
    """Whether upstream holds a key the input's patterns take past `empty`
    (`""`: anywhere): one the partition has not processed since it last
    started over, so it is not complete (STA-10, D176); with `edge`, a key
    a key list processed there does not count. One native scan, stopping at
    the first such key."""

    id_ = output_id(input.output, input.partition)
    core.attach(id_)
    return await core.run(core.tracking.taken, id_, input.spec.get("patterns"), empty or None, edge)
