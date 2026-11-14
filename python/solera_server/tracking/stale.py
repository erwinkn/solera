"""Staleness on the new tracking core (build plan B5): whether an
incremental input owes a partition anything. A keyed one is answered on
demand by a plan of one key from the start of its edge (SOLE-14's stale index
replaces it, B7); a log's by its head and what the edge consumed.
The plan reads the edge's progress and the upstream's layers as an attempt's
would, records nothing, and runs as a heavy call."""

from __future__ import annotations

from solera._native import Tracking

from ..model import edge_id, is_log, output_id
from . import Core, Generation, Redeployed, outcomes
from .prepare import conditions


async def owes(core: Core, asset: str, partition: str, input, planner, inputs) -> bool:
    """Whether `input` owes the partition a key: under its patterns and the
    conditions it would be processed under now. An edge the partition has not
    read owes whatever upstream holds. A per-key edge's failing key is owed
    once its input changes, or once its retry clock reaches it (STA-3,
    STA-15): a default run would take it."""

    m = core.m
    upstream = output_id(input.output, input.partition)
    found = m.core_output(input.output, input.partition)
    if found is None:
        return False
    edge = edge_id(asset, partition, input.param)
    held = m.edge(asset, partition, input.param) or {}
    if not held.get("read"):
        return found["live"] > 0
    if is_log(m.manifest["outputs"].get(input.output)):  # the commits after the last it read
        return "log" not in held or found["head"] > held["log"]["through"]
    core.attach(upstream, *Tracking.outcome_outputs(edge))  # a per-key edge's records, where it has any
    request = {
        "mode": "incremental",
        "reset": False,
        "after": None,
        "limit": 1,
        "inputs": [
            {"edge": edge, "conditions": conditions_now(core, asset, partition, input, planner, inputs)}
        ],
    }
    generation = Generation(m)
    try:
        with generation:
            summary = await core.run(core.tracking.plan, request)
            owed = bool(summary["inputs"][0]["keys"]) or await outcomes.due_now(core, edge)
        generation.check()
    except Redeployed:  # a deploy while it planned: not known, so owed
        return True
    return owed


def conditions_now(core: Core, asset: str, partition: str, input, planner, inputs) -> dict:
    """The conditions `input`'s keys would be processed under now."""

    definition = core.m.partition(asset, partition).get("definition") or ""
    base = {"context": core.engine._context(planner, inputs), "definition": definition}
    return conditions(input.spec, base)


async def owed_keys(
    core: Core, asset: str, partition: str, input, planner, inputs, after: str | None, limit: int
) -> tuple[list[str], str | None] | None:
    """A page of the keys `input` owes the partition, in key order, and where
    the next page starts: the planner's answer, with no batch made. None
    where the core cannot say: the input has no edge yet, or a deploy came
    in while it read. A failing key due by its retry clock is owed too, but
    not listed here (`outcomes.due_keys`)."""

    edge = edge_id(asset, partition, input.param)
    if core.tracking.edge(edge) is None:
        return None
    core.attach(output_id(input.output, input.partition), *Tracking.outcome_outputs(edge))
    conds, generation = conditions_now(core, asset, partition, input, planner, inputs), Generation(core.m)
    try:
        with generation:
            rows, next_ = await core.run(core.tracking.owed, edge, conds, after=after, limit=limit)
        generation.check()
    except Redeployed:
        return None
    return [key for key, _ in rows], next_


async def taken_after(core: Core, input, empty: str, edge: bytes | None = None) -> bool:
    """Whether upstream holds a key the input's patterns take past `empty`
    (`""`: anywhere): one the partition has not processed since it last
    started over, so it is not complete (STA-10, D176); with `edge`, a key
    a key list processed there does not count. One native scan, stopping at
    the first such key."""

    id_ = output_id(input.output, input.partition)
    core.attach(id_)
    return await core.run(core.tracking.taken, id_, input.spec.get("patterns"), empty or None, edge)
