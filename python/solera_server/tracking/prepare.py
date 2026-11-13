"""Planning a batch on the new tracking core (docs/tracking-design.md §7):
the incremental half of preparing an attempt, which `Engine._prepare` does
on today's path.

The engine's shared half (`Engine._pin_inputs`) pins every input that is not
incremental. Here every keyed incremental input's edge is opened if it is
new (one `Core` event, recorded before anything is awaited), and every keyed
input is planned by one `Tracking.plan` call, off the event loop: from the
task's progress key, the next `batch_size` keys owed on any of them, in key
order, with their classes, each input at its own head. The plan's bytes go
into the attempt's spec untouched (`spec["plan"]`); the engine keeps them in
memory for the commit, which reads them back from the spec after a restart.
"""

from __future__ import annotations

import math

from solera._native import Tracking

from ..model import edge_id, output_id
from . import WALK, Core, Generation, Redeployed, canonical
from .commit import HeldBack
from .full import dynamic_sets, held


def conditions(engine, input, base: dict) -> dict:
    """What a keyed input's keys are processed under (§9): its patterns, the
    context (the heads of the whole and dep inputs), the definition, and its
    rebuild counter (a per-key asset's, B6)."""

    return {
        "patterns": input.spec.get("patterns"),
        "context": canonical(base["context"]),
        "definition": base["definition"].encode(),
        "rebuild": 0,
    }


def open_edges(core: Core, task: dict, keyed: list) -> dict:
    """Each incremental input's edge, opening those the core does not hold:
    recorded as one event, at once. Opening an edge names its upstream. An
    edge that reads another output than the input does now is closed and
    opened again: what it held is of an output it no longer reads."""

    events, out = [], {}
    for input in keyed:
        upstream = output_id(input.output, input.partition)
        edge = out[input.param] = edge_id(task["asset"], task["partition"], input.param)
        found = core.tracking.edge(edge)
        if found is not None and found["input"] == upstream:
            continue
        if found is not None:
            events.append(Tracking.event_edge_close(edge))
        events.append(Tracking.event_edge_open(edge, upstream, True))
    if events:
        core.record(Tracking.join(events))
    return out


async def prepare(core: Core, task: dict, run: dict, attempt: str | None) -> dict:
    """`_prepare` under one deploy (`Generation`): a deploy while it planned
    starts it again, so what it read by name is of the deploy it returns
    under."""

    while True:
        generation = Generation(core.m)
        try:
            with generation:
                prepared = await _prepare(core, task, run, attempt)
            generation.check()
            return prepared
        except Redeployed:
            if not generation.moved:
                raise
            continue


async def _prepare(core: Core, task: dict, run: dict, attempt: str | None) -> dict:
    """An attempt's preparation on the new path: `Engine._prepare`'s result,
    with the plan's bytes under `plan` (kept out of the journal), and under
    `gate` every upstream head it reads, `[output, partition, commit, life]`:
    a whole input's as pinned, an incremental input's as planned. A partial
    one sends it back to wait (`HeldBack`); its commit checks them again
    (`full.held`)."""

    engine, m = core.engine, core.m
    base = engine._pin_inputs(task, run)
    heads = []
    for input in base["inputs"]:  # in the step that pinned them: the heads the pins name
        if input.kind == "in":
            parts = base["planner"].fan_in(input, materialized=True) if input.fan_in else (input.partition,)
            heads += [[input.output, p, m.axis_head(input.output, p), m.life(input.output, p)] for p in parts]
            if input.fan_in:  # which partitions it reads: each dynamic dimension's set output says
                heads += [[s, "", m.axis_head(s, ""), m.life(s, "")] for s in dynamic_sets(input)]
    if (why := held(m, heads)) is not None:
        raise HeldBack(f"{why[0]}: {why[1]}")
    if base["incremental"]:  # a full run's first batch, or a definition the partition was not built under
        base["full"] = engine._full_run(task, run) is not None
    full = base["full"]
    pins = base["pins"]
    keyed = base["incremental"]
    refuse(engine, task, run, keyed)
    if any(restarted(core, task, i) for i in keyed):  # an upstream's new life is read in full, from the start
        full = base["full"] = True
    progress = {} if full else task.get("progress") or {}
    plans, all_empty, plan_bytes = {}, True, None
    mine = progress.get(WALK)
    if keyed:
        edges = open_edges(core, task, keyed)
        core.attach(*(output_id(i.output, i.partition) for i in keyed))
        after = (mine or {}).get("key")
        mode, named = run_mode(run, keyed)
        request = {
            "mode": mode,
            "keys": named,
            "reset": full,
            "after": after,
            "limit": min(int(i.spec["batch_size"]) for i in keyed),
            "inputs": [{"edge": edges[i.param], "conditions": conditions(engine, i, base)} for i in keyed],
        }
        summary = await core.run(core.tracking.plan, request)
        plan_bytes = summary["plan"]
        hi = summary["hi"]
        index = 0 if mine is None else int(mine["batch"]) + 1
        for n, input in enumerate(keyed):
            found = summary["inputs"][n]
            heads.append(
                [input.output, input.partition, found["head"], m.life(input.output, input.partition)]
            )
            # The walk ends where the plan says (`hi` None). The worker is told each input's
            # own end too: it owes nothing past the batch (a full run: holds nothing past it),
            # though the others' go on.
            final = hi is None
            own = final
            if not final and len(keyed) > 1 and mode == "incremental":
                own = not await _more(
                    core, input, edges[input.param], conditions(engine, input, base), hi, full
                )
            upstream = core.m.core_output(input.output, input.partition)
            size = int(input.spec["batch_size"])
            live = (upstream or {}).get("live", 0)
            count = index + 1 if own else max(index + 2, math.ceil(live / size))
            pins[input.param] = {
                "ref": engine._pin_at(input.output, input.partition),
                "core": n,
                "batch": {"index": index, "count": count, "final": own, "reset": full},
            }
            plans[input.param] = {
                "kind": "core",
                "core": n,
                "mode": mode,
                "output": input.output,
                "upstream_partition": input.partition,
                "head": found["head"],
                "index": index,
                "count": count,
                "after": after,
                "end": hi,
                "final": final,
                "full": full,
                "classes": found["counts"],
            }
            all_empty = all_empty and not found["keys"]
    if (why := held(m, heads)) is not None:  # a head it planned on is partial
        raise HeldBack(f"{why[0]}: {why[1]}")
    more = any(not (p["final"] or p.get("done")) for p in plans.values())
    skip = bool(base["incremental"]) and all_empty and not more and not full
    prepared = engine._prepared(task, run, attempt, base, plans, more, skip, None)
    prepared["gate"] = heads
    if plan_bytes is not None:
        prepared["plan"] = plan_bytes
        if attempt is not None:
            core.plans[attempt] = plan_bytes
    return prepared


def restarted(core: Core, task: dict, input) -> bool:
    """Whether an incremental input must start over. A task's walk goes on
    unless a deploy since its last commit reset either side of the edge (its
    asset, one of its outputs, the upstream), whatever another run committed
    meanwhile; a rebind changes the definition, which starts it over too
    (`_full_run`). A task with no walk yet starts over if the partition has
    not read its edge, yet it has committed (a job, with no output, has only
    its record) or one of the asset's outputs still holds content to clear."""

    m, asset, partition = core.m, task["asset"], task["partition"]
    outputs = [o["name"] for o in (m.manifest["assets"].get(asset) or {}).get("outputs") or ()]
    if (mine := (task.get("progress") or {}).get(WALK)) is not None:
        sides = [("asset", asset), ("output", input.output), *(("output", name) for name in outputs)]
        return any(m.reset_at.get(side, 0) > mine["deploy"] for side in sides)
    if (m.edge(asset, partition, input.param) or {}).get("read"):
        return False
    return "definition" in m.partition(asset, partition) or any(
        (name, partition) in m.heads for name in outputs
    )


def refuse(engine, task: dict, run: dict, keyed: list) -> None:
    """What the new path does not run yet, refused for good with why: per-key
    assets (B6) and stores that write in place (B8)."""

    from ..engine import NonRetryable

    asset = engine.manifest["assets"][task["asset"]]
    if any(i.spec.get("each") is not None for i in keyed):
        raise NonRetryable("per-key assets do not run on the new tracking core yet (build plan B6)")
    for output in asset["outputs"]:
        if (
            output.get("key") is not None
            and engine.manifest["stores"][output["store"]]["writes"] != "immutable"
        ):
            raise NonRetryable(
                f"output {output['name']!r}: keyed outputs on stores that write in place do not run on "
                "the new tracking core yet (build plan B8)"
            )


async def _more(core: Core, input, edge: bytes, conds: dict, hi: str, full: bool) -> bool:
    """Whether one keyed input's walk goes on past `hi`: it owes a key past
    it, or in a full run, upstream holds one its patterns take."""

    if full:
        from .stale import taken_after

        return await taken_after(core, input, hi)
    rows, _ = await core.run(core.tracking.owed, edge, conds, after=hi, limit=1)
    return bool(rows)


def run_mode(run: dict, keyed: list) -> tuple[str, list[str]]:
    """What a run asks of its keyed inputs (`keys=`, per upstream output): every
    key, the rest unchanged ("all"); the keys listed ("keys"); or what is
    owed. A batch's keys are one set across its keyed inputs, so the lists
    of several are one list, and "all" on any is "all" on every one."""

    asked = [(run.get("keys") or {}).get(i.output) for i in keyed]
    if any(a == "all" for a in asked):
        return "all", []
    if any(isinstance(a, list) for a in asked):
        return "keys", sorted({k for a in asked if isinstance(a, list) for k in a})
    return "incremental", []
