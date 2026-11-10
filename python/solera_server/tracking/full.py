"""Partial content on the new tracking core (docs/tracking-design.md §10,
D205: the clear at the first batch, plus the gate).

One fact decides it: an output commit is partial when its producer
partition still had a walk from empty pending after it applied (a first run,
a full run, a start-over not walked to the end). `Model.partial_spans`
holds those commits per output life, written only by its producer's
commits, by `settle` (`Settled`) and by an operator's `proceed`
(`Proceeded`); `Model.partial` answers for one commit. A span opens only at
a new commit and only shrinks after the fact, so whether a given head is
partial can only go from yes to no.

A batch reads no partial commit, keyed or whole (STA-13, RUN-4): its task is
held at dispatch while an upstream's head is partial (advisory), its
preparation checks the heads it planned on, and its commit checks them again
in the step that records it (`held`). A run that stops midway leaves its
outputs' heads partial; an alarm names each one with no run of its producer
under way, and `proceed` lets readers go on over the partial output."""

from __future__ import annotations

import contextlib
import logging

from solera._native import Tracking

from .. import planning
from ..state import Conflict
from . import b64

INCOMPLETE = "upstream incomplete"

log = logging.getLogger(__name__)


def running(core) -> set[tuple[str, str]]:
    """The keyed outputs the core holds mid full run (its mark), as
    `(output, partition)`."""

    tracking = core.tracking
    return {key for key, id_ in core.m.core_outputs.items() if tracking.full_run(id_.encode()) is not None}


def waiting(engine) -> set[tuple[str, str]]:
    """Every output partition whose head is partial: what the dispatch pass
    and the alarm read."""

    m = engine.m
    owner = {("@life", id_): key for key, id_ in m.core_outputs.items()}
    out = set()
    for key in m.partial_spans:
        output, partition = owner.get(key, key)
        if output != "@life" and m.partial(output, partition, m.axis_head(output, partition)):
            out.add((output, partition))
    return out


def gate(planner, task: dict, waits) -> list | None:
    """Why a task waits for an upstream at dispatch, if it does: the first
    output partition it reads, incrementally or whole, that `waits(output,
    partition)`. Advisory: what a batch reads is checked on the heads it
    planned (`held`)."""

    if task["asset"] not in planner.manifest["assets"]:  # the engine's own task (a cleanup): it reads nothing
        return None
    try:
        for input in planner.inputs(task["asset"], task["partition"]):
            if input.kind == "dep":
                continue
            partitions = planner.fan_in(input, materialized=False) if input.fan_in else (input.partition,)
            if any(waits(input.output, p) for p in partitions):
                return [INCOMPLETE, input.output]
            if input.fan_in and input.kind == "in":
                for output in dynamic_sets(input):
                    if waits(output, ""):
                        return [INCOMPLETE, output]
    except (
        Exception
    ):  # its preparation refuses it alone, saying why (UpstreamOnly, a partition that no longer fits)
        return None
    return None


def dynamic_sets(input) -> list[str]:
    """The set outputs a fan-in's membership comes from: its dynamic dimensions'."""

    return sorted({dim["output"] for dim in (input.dims or {}).values() if dim.get("kind") == "dynamic"})


def held(m, heads) -> list | None:
    """Why a batch that read `heads` (`[[output, partition, commit, life], ...]`)
    must not commit, if it must not: the first it read that is partial. A
    head of a life that is no longer the output's was free when planned, and
    stays so: that life gets no commit, and its spans only shrank."""

    for output, partition, at, *life in heads:
        if life and life[0] != m.life(output, partition):
            continue
        if m.partial(output, partition, at):
            return [INCOMPLETE, output]
    return None


def stopped(engine) -> list[dict]:
    """The partial outputs with no run of their producer under way to
    complete them: what an operator must see."""

    m, out = engine.m, []
    for output, partition in sorted(waiting(engine)):
        asset = (engine.manifest["outputs"].get(output) or {}).get("asset")
        if asset is None or (asset, partition) in m.claimed_partitions or m.pending.get((asset, partition)):
            continue
        out.append({"output": output, "partition": partition, "asset": asset})
    return out


def settles(engine, asset: str, partition: str) -> bool:
    """Whether a partition with open spans is complete with no batch to say
    so: complete as known (D177), made under today's definition, and reading
    no partial head of an upstream, proceeded or not."""

    m = engine.m
    if asset not in engine.manifest["assets"] or not engine._complete_known(asset, partition):
        return False
    record = m.partition(asset, partition)
    if record.get("definition") != engine._definition(asset, {"config": record.get("config")}):
        return False
    try:
        inputs = engine.planner().inputs(asset, partition)
    except planning.UpstreamOnly:
        return False
    return not any(
        m.partial(i.output, i.partition, m.axis_head(i.output, i.partition), proceeded=True)
        for i in inputs
        if i.kind == "incremental"
    )


async def settle(engine) -> None:
    """Close the open spans of every partition `settles` finds complete
    (`Settled`), then end the core's full-run mark of every output with no
    open span (`FullRunDone`): the mark follows the spans, at most a tick
    late, and a late end only holds back more."""

    m, core = engine.m, engine.core
    owner = {("@life", id_): key for key, id_ in m.core_outputs.items()}
    partitions = set()
    for key, spans in m.partial_spans.items():
        output, partition = owner.get(key, key)
        if output != "@life" and spans[-1][1] is None:
            if (asset := (engine.manifest["outputs"].get(output) or {}).get("asset")) is not None:
                partitions.add((asset, partition))
    for asset, partition in sorted(partitions):
        try:
            if settles(engine, asset, partition):
                engine.state.record({"type": "Settled", "asset": asset, "partition": partition})
        except Exception:  # one partition that no longer plans (a dimension changed) stops no other
            log.exception("settling %s %r", asset, partition)
    for (output, partition), id_ in sorted(m.core_outputs.items()):
        if core.tracking.full_run(id_.encode()) is None:
            continue
        spans = m.partial_spans.get(("@life", id_)) or ()
        if spans and spans[-1][1] is None:
            continue
        async with core.lock(id_):
            if (
                core.m.core_outputs.get((output, partition)) != id_
                or core.tracking.full_run(id_.encode()) is None
            ):
                continue
            spans = m.partial_spans.get(("@life", id_)) or ()
            if spans and spans[-1][1] is None:
                continue
            event = Tracking.event_full_run_done(id_.encode())
            core.check(event)
            engine.state.record({"type": "Core", "event": b64(event), "outputs": [], "edges": []})


async def proceed(engine, asset: str, partition: str, by: str) -> dict:
    """An operator's `solera proceed`: the readers of a partition whose run
    stopped midway go on over its partial output (D205). Its open spans are
    marked proceeded, until the partition starts over, and its keyed
    outputs' full runs end (`FullRunDone`)."""

    core = engine.core

    def lives() -> list[str]:
        names = [o["name"] for o in (engine.manifest["assets"].get(asset) or {}).get("outputs") or ()]
        return sorted(i for o in names if (i := core.m.core_outputs.get((o, partition))) is not None)

    while True:
        ids = lives()
        async with contextlib.AsyncExitStack() as locks:
            for id_ in ids:  # in life-id order, as a batch's commit takes them
                await locks.enter_async_context(core.lock(id_))
            if lives() != ids:  # a deploy changed its lives while it waited: lock the current ones
                continue
            stuck = [
                s["output"] for s in stopped(engine) if (s["asset"], s["partition"]) == (asset, partition)
            ]
            if not stuck:
                raise Conflict(
                    f"{asset} {partition!r}: no incomplete output, or a run of it is under way",
                    retryable=False,
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
