"""Full runs on the new tracking core (the full-run verdict, "Final rules,
as merged": the clear, plus the gate). A full run's first commit clears its
outputs (`commit.full_run`), and they stay incomplete until the run's last
batch ends it. Meanwhile every reader of an incomplete output waits: a task
reading one, keyed or whole, is held, saying "upstream incomplete" and
naming it (STA-13, RUN-4). A full run that stops midway, canceled or
failed, leaves its outputs incomplete until a run of their producer walks
the rest: an alarm names each one with no such run under way."""

from __future__ import annotations

from .. import planning
from . import Core

INCOMPLETE = "upstream incomplete"


def incomplete(core: Core) -> set[tuple[str, str]]:
    """The outputs mid full run, as `(output, partition)`."""

    tracking = core.tracking
    return {key for key, id_ in core.m.core_outputs.items() if tracking.full_run(id_.encode()) is not None}


def gate(planner, task: dict, incomplete: set) -> list | None:
    """Why a task waits for an upstream's full run, if it does: the first
    output it reads, incrementally or whole, that is `incomplete`."""

    if not incomplete:
        return None
    try:
        inputs = planner.inputs(task["asset"], task["partition"])
    except planning.UpstreamOnly:  # its preparation refuses it, saying why
        return None
    for input in inputs:
        if input.kind == "dep":
            continue
        if input.fan_in:
            held = any(o == input.output for o, _ in incomplete)
        else:
            held = (input.output, input.partition) in incomplete
        if held:
            return [INCOMPLETE, input.output]
    return None


def stopped(core: Core) -> list[dict]:
    """The outputs a full run left incomplete with no run of their producer
    under way to complete them: what an operator must see."""

    m, out = core.m, []
    for output, partition in sorted(incomplete(core)):
        asset = (m.manifest["outputs"].get(output) or {}).get("asset")
        if asset is not None and (
            (asset, partition) in m.claimed_partitions or m.pending.get((asset, partition))
        ):
            continue
        out.append({"output": output, "partition": partition, "asset": asset})
    return out
