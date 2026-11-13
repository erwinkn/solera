"""Partial content on the new tracking core (docs/tracking-design.md §10,
D205: the clear at the first batch, plus the gate).

One fact decides it: an output commit is partial when its producer
partition still had a walk from empty pending after it applied (a first run,
a full run, a start-over not walked to the end). `Model.partial_spans`
holds those commits per output life, written only by its producer's
commits and by `settle` (`Settled`); `Model.partial` answers for one commit.
A span opens only at a new commit and only shrinks after the fact, so
whether a given head is partial goes from yes to no.

A batch reads no partial commit, keyed or whole (STA-13, RUN-4): its task is
held at dispatch while an upstream's head is partial (advisory), its
preparation checks the heads it planned on, and its commit checks them again
in the step that records it (`held`). A run that stops midway leaves its
outputs' heads partial; an alarm names each one with no run of its producer
under way, and its readers wait until a run of the producer completes it."""

from __future__ import annotations

import logging

import orjson
from solera._native import Tracking

from .. import planning
from ..model import output_id
from . import Generation

INCOMPLETE = "upstream incomplete"

log = logging.getLogger(__name__)


def running(core) -> set[tuple[str, str]]:
    """The keyed outputs the core holds mid full run (its mark), as
    `(output, partition)`."""

    tracking = core.tracking
    return {tuple(orjson.loads(id_)) for id_ in tracking.outputs() if tracking.full_run(id_) is not None}


def waiting(engine) -> set[tuple[str, str]]:
    """Every output partition whose head is partial: what the dispatch pass
    and the alarm read."""

    m = engine.m
    return {key for key in m.partial_spans if m.partial(*key, m.axis_head(*key))}


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
    no partial head of an upstream."""

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
        m.partial(i.output, i.partition, m.axis_head(i.output, i.partition))
        for i in inputs
        if i.kind == "incremental"
    )


async def settle(engine) -> None:
    """Close the open spans of every partition `settles` finds complete
    (`Settled`), then end the core's full-run mark of every output with no
    open span (`FullRunDone`): the mark follows the spans, at most a tick
    late, and a late end only holds back more."""

    m, core = engine.m, engine.core
    partitions = set()
    for (output, partition), spans in m.partial_spans.items():
        if spans[-1][1] is None:
            if (asset := (engine.manifest["outputs"].get(output) or {}).get("asset")) is not None:
                partitions.add((asset, partition))
    for asset, partition in sorted(partitions):
        try:
            if settles(engine, asset, partition):
                engine.state.record({"type": "Settled", "asset": asset, "partition": partition})
        except Exception:  # one partition that no longer plans (a dimension changed) stops no other
            log.exception("settling %s %r", asset, partition)
    generation = Generation(m)

    def open_span(key: tuple[str, str]) -> bool:
        spans = m.partial_spans.get(key) or ()
        return bool(spans) and spans[-1][1] is None

    for key in sorted(running(core)):
        if open_span(key):
            continue
        id_ = output_id(*key)
        async with core.lock(id_):
            if generation.moved:  # a deploy while it waited: the next tick goes on
                return
            if core.tracking.full_run(id_) is not None and not open_span(key):
                core.record(Tracking.event_full_run_done(id_))
