"""Incomplete upstreams on the new tracking core (docs/tracking-design.md
§10, D205: the clear at the first batch, plus the gate).

A producer partition is incomplete while a keyed output of it is mid full
run (its first commit cleared it), or, once it has committed, while an
input's walk from empty is not done (`Model.core_empty`): a keyed input's
empty piece still holds a key it has not processed, or an unkeyed input's
commits are not all read (a first or full run not walked to the end:
`Engine._complete_known`). Every reader of an incomplete output waits, keyed
or whole: its task is held, saying "upstream incomplete" and naming it
(STA-13, RUN-4). A full run ends with the batch that ends its walk, or as
soon as its producer is complete (a key list that took the rest, or the rest
deleted upstream): `settle` records that. A run that stops midway leaves
its outputs incomplete; an alarm names each one with no run of its producer
under way, and `proceed` lets readers go on over the partial output."""

from __future__ import annotations

import contextlib

from solera._native import Tracking

from .. import planning
from ..state import Conflict
from . import Core, b64

INCOMPLETE = "upstream incomplete"


def running(core: Core) -> set[tuple[str, str]]:
    """The keyed outputs mid full run, as `(output, partition)`."""

    tracking = core.tracking
    return {key for key, id_ in core.m.core_outputs.items() if tracking.full_run(id_.encode()) is not None}


def incomplete(engine) -> set[tuple[str, str]]:
    """The outputs whose producer partition is incomplete, but those an
    operator let readers proceed over."""

    m, assets = engine.m, engine.manifest["assets"]
    out = running(engine.core)
    for asset, partition in {(a, p) for a, p, _ in m.core_empty}:
        if asset in assets and not engine._complete_known(asset, partition):
            out |= {
                (o["name"], partition) for o in assets[asset]["outputs"] if (o["name"], partition) in m.heads
            }
    owners = engine.manifest["outputs"]
    return {(o, p) for o, p in out if ((owners.get(o) or {}).get("asset"), p) not in m.proceeded}


def gate(planner, task: dict, incomplete: set) -> list | None:
    """Why a task waits for an upstream, if it does: the first output it
    reads, incrementally or whole, that is `incomplete`."""

    try:
        inputs = planner.inputs(task["asset"], task["partition"])
    except planning.UpstreamOnly:  # its preparation refuses it, saying why
        return None
    for input in inputs:
        if input.kind == "dep":
            continue
        partitions = planner.fan_in(input, materialized=False) if input.fan_in else (input.partition,)
        if any((input.output, p) in incomplete for p in partitions):
            return [INCOMPLETE, input.output]
    return None


def stopped(engine) -> list[dict]:
    """The incomplete outputs with no run of their producer under way to
    complete them: what an operator must see."""

    m, out = engine.m, []
    for output, partition in sorted(incomplete(engine)):
        asset = (engine.manifest["outputs"].get(output) or {}).get("asset")
        if (asset, partition) in m.claimed_partitions or m.pending.get((asset, partition)):
            continue
        out.append({"output": output, "partition": partition, "asset": asset})
    return out


async def settle(engine) -> None:
    """End the full run of every keyed output whose producer is complete
    with no batch to end it (D177's completeness, as the tick knows it): a
    key list took the rest, or the rest was deleted upstream."""

    core = engine.core
    for output, partition in sorted(running(core)):
        asset = (engine.manifest["outputs"].get(output) or {}).get("asset")
        if asset is None or not engine._complete_known(asset, partition):
            continue
        id_ = core.m.core_outputs[(output, partition)]
        async with core.lock(id_):  # a batch building its last commit meanwhile ends the run itself
            if core.tracking.full_run(id_.encode()) is None or not engine._complete_known(asset, partition):
                continue
            event = Tracking.event_full_run_done(id_.encode())
            core.check(event)
            engine.state.record({"type": "Core", "event": b64(event), "outputs": [], "edges": []})


async def proceed(engine, asset: str, partition: str, by: str) -> dict:
    """An operator's `solera proceed`: the readers of a partition whose run
    stopped midway go on over its partial output (D205). Its keyed outputs'
    full runs end (`FullRunDone`), and the gate lets every reader of its
    outputs through until the partition starts over."""

    core, outputs = engine.core, sorted(o["name"] for o in engine.manifest["assets"][asset]["outputs"])
    ids = [i for o in outputs if (i := core.m.core_outputs.get((o, partition))) is not None]
    async with contextlib.AsyncExitStack() as locks:
        for id_ in ids:  # in output-name order, as a batch's commit takes them
            await locks.enter_async_context(core.lock(id_))
        stuck = [s["output"] for s in stopped(engine) if (s["asset"], s["partition"]) == (asset, partition)]
        if not stuck:
            raise Conflict(
                f"{asset} {partition!r}: no incomplete output, or a run of it is under way", retryable=False
            )
        ending = [
            Tracking.event_full_run_done(i.encode())
            for i in ids
            if core.tracking.full_run(i.encode()) is not None
        ]
        event = Tracking.join(ending) if ending else None
        core.check(event)
        engine.state.record(
            {
                "type": "Proceeded",
                "asset": asset,
                "partition": partition,
                "by": by,
                "at": engine.clock(),
                "event": b64(event) if event is not None else None,
            }
        )
    return {"asset": asset, "partition": partition, "outputs": stuck}
