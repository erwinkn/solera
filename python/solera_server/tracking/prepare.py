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
Unkeyed incremental inputs are planned as today's path plans them, by
commit numbers.
"""

from __future__ import annotations

import math

from solera._native import Tracking

from . import WALK, Core, b64, canonical
from .commit import HeldBack
from .full import held


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
    """Each keyed input's edge, opening those that are new and minting the
    ids of upstream outputs never committed: recorded as one event, at once.
    An edge whose upstream is another output's life than the one it read is
    closed and opened again: what it held is of an output that is gone."""

    m, asset, partition = core.m, task["asset"], task["partition"]
    minted, edges, events, out = [], [], [], {}
    for input in keyed:
        upstream = core.output_id(input.output, input.partition, minted)
        found = m.core_edges.get((asset, partition, input.param))
        if found is not None and found[1] == upstream:
            out[input.param] = found[0]
            continue
        if found is not None:
            events.append(Tracking.event_edge_close(found[0].encode()))
        edge = core.edge_id()
        events.append(Tracking.event_edge_open(edge.encode(), upstream.encode(), True))
        edges.append([asset, partition, input.param, [edge, upstream]])
        out[input.param] = edge
    if events or minted:
        event = Tracking.join(events) if events else None
        core.check(event, minted)
        core.engine.state.record(
            {"type": "Core", "event": b64(event) if event else None, "outputs": minted, "edges": edges}
        )
    return out


async def prepare(core: Core, task: dict, run: dict, attempt: str | None) -> dict:
    """An attempt's preparation on the new path: `Engine._prepare`'s result,
    with the plan's bytes under `plan` (kept out of the journal), and under
    `gate` every upstream head it reads, `[output, partition, commit]`: a
    whole input's as pinned, a keyed input's as planned, an unkeyed input's
    planned end. A partial one sends it back to wait (`HeldBack`); its commit
    checks them again (`full.held`)."""

    engine, m = core.engine, core.m
    deploy = m.deploy_number
    base = engine._pin_inputs(task, run)
    heads = []
    for input in base["inputs"]:  # in the step that pinned them: the heads the pins name
        if input.kind == "in":
            parts = base["planner"].fan_in(input, materialized=True) if input.fan_in else (input.partition,)
            heads += [[input.output, p, m.axis_head(input.output, p)] for p in parts]
    if (why := held(m, heads)) is not None:
        raise HeldBack(f"{why[0]}: {why[1]}")
    # With incremental inputs, a run starts over as today's path decides (`_full_run`): a full
    # run's first batch, a definition other than the partition was built under, an input reset.
    if base["incremental"]:
        base["full"] = engine._full_run(task, run, base["incremental"]) is not None
    full = base["full"]
    pins = base["pins"]
    keyed = [i for i in base["incremental"] if engine._keyed(i)]
    unkeyed = [i for i in base["incremental"] if not engine._keyed(i)]
    refuse(engine, task, run, keyed)
    if any(restarted(core, task, i) for i in keyed):  # an upstream's new life is read in full, from the start
        full = base["full"] = True
    progress = {} if full else task.get("progress") or {}
    plans, all_empty, plan_bytes = {}, True, None
    for input in unkeyed:  # by commit numbers, as today
        mine = progress.get(input.param)
        record = (engine.m.partition(task["asset"], task["partition"]).get("observed") or {}).get(input.param)
        o = engine._commits(input, record, mine, full)
        pin, plan, empty = engine._commits_pin(input, engine._pin_at(input.output, input.partition), o)
        pins[input.param], plans[input.param] = pin, plan
        if not plan.get("done"):
            heads.append([input.output, input.partition, plan["end"]])
        all_empty = all_empty and empty
    mine = progress.get(WALK)
    if keyed and mine is not None and mine["key"] is None:  # walked to the end in this task
        # While unkeyed inputs go on, each keyed one is delivered empty and final, its edge left be.
        index = int(mine["batch"]) + 1
        for n, input in enumerate(keyed):
            pins[input.param] = {
                "ref": engine._pin_at(input.output, input.partition),
                "core": n,
                "batch": {"index": index, "count": index + 1, "final": True, "reset": False},
            }
            plans[input.param] = {"kind": "core", "output": input.output, "final": True, "done": True}
    elif keyed:
        edges = open_edges(core, task, keyed)
        core.attach(*(core.m.core_edges[(task["asset"], task["partition"], i.param)][1] for i in keyed))
        after = (mine or {}).get("key")
        mode, named = run_mode(run, keyed)
        request = {
            "mode": mode,
            "keys": named,
            "reset": full,
            "after": after,
            "limit": min(int(i.spec["batch_size"]) for i in keyed),
            "inputs": [
                {"edge": edges[i.param].encode(), "conditions": conditions(engine, i, base)} for i in keyed
            ],
        }
        summary = await core.run(core.tracking.plan, request)
        plan_bytes = summary["plan"]
        hi = summary["hi"]
        index = 0 if mine is None else int(mine["batch"]) + 1
        for n, input in enumerate(keyed):
            found = summary["inputs"][n]
            heads.append([input.output, input.partition, found["head"]])
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
    if engine.m.deploy_number != deploy:  # redeployed while it planned: prepared under one deploy only
        return await prepare(core, task, run, attempt)
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
    """Whether a keyed input must start over: what its committed progress
    reads (`Model.core_read`, which only a commit writes) is an upstream life
    that is gone (a reset: the output removed, or moved to another store),
    or nothing though the partition has committed (its edge was closed and
    opened again by deploys since)."""

    m, asset, partition = core.m, task["asset"], task["partition"]
    read = m.core_read.get((asset, partition, input.param))
    if read is None:
        return "definition" in m.partition(asset, partition)
    return read != m.core_outputs.get((input.output, input.partition))


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


async def _more(core: Core, input, edge: str, conds: dict, hi: str, full: bool) -> bool:
    """Whether one keyed input's walk goes on past `hi`: it owes a key past
    it, or in a full run, upstream holds one its patterns take."""

    if full:
        from .stale import taken_after

        return await taken_after(core, input, hi)
    rows, _ = await core.run(core.tracking.owed, edge.encode(), conds, after=hi, limit=1)
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
