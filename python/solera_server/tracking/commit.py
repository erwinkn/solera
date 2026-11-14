"""Committing a batch on the new tracking core (docs/tracking-design.md §7,
steps 6 and 7): the tracking half of `Engine.commit_attempt`.

The engine builds every commit (D207). For each keyed output the worker
wrote, `build_commit` joins the keys the write changed with the output's own
index at its head and writes the commit's layer, off the event loop and
under the output's lock, so an output's commits are built one at a time. Then
`progress_ops` turns the plan and the worker's load report into each keyed
input's new piece, points and batch record. Everything joins into one core
event, which `Engine.commit_attempt` checks and records in one synchronous
step, inside the same locks. The event names layers and counts, never a list
of keys.

A per-key batch also commits its keys' outcomes (B6): the keys that did not
succeed are its input's failed keys, and `progress_ops` builds their records
into the same event, from the edge's state as it reads it. Should the state
move before the event is recorded (an upstream commit, a catch-up), its
progress and outcomes are built again (`rebuild`), its outputs' commits kept.
"""

from __future__ import annotations

import contextlib
import tempfile
from dataclasses import dataclass, field

import obstore
from obstore.exceptions import NotFoundError
from solera._native import OutcomesMovedError, Tracking, UnsettledError

from ..model import edge_id
from ..state import Conflict
from . import Core, Generation, b64, canonical, outcomes, unb64
from .loaded import rebuilding

DYNAMIC_KEYS = 1_000_000  # a dynamic partitions output's elements, read from its commit's keys up to this


@dataclass
class Built:
    """What a batch commits on the core, built and not yet recorded."""

    event: bytes | None = None
    records: list = field(default_factory=list)  # [input, base64 of a batch record]
    counts: dict = field(default_factory=dict)  # output -> {commit, live, added, updated, removed}
    elements: dict = field(default_factory=dict)  # a dynamic partitions output -> its elements after
    progress: dict | None = None  # the run's walk over the keyed inputs: {batch, key}
    walked: list = field(default_factory=list)  # the inputs whose edges the batch wrote
    partition: str = ""
    generation: Generation | None = None  # the deploy it was built under
    locks: contextlib.AsyncExitStack = field(default_factory=contextlib.AsyncExitStack)
    # A per-key batch's failing keys, each with the stored outcome the core made of it.
    stored: dict = field(default_factory=dict)
    commits: list = field(default_factory=list)  # its outputs' commit events, which a rebuild keeps
    # What its progress is built from, again if the state moves: (plan, reports, producer, commit, each, params).
    again: tuple | None = None
    rebuilds: int = 0

    def payload(self) -> dict | None:
        """The commit's `core` field, as the model applies it."""

        if self.event is None:
            return None
        return {
            "event": b64(self.event),
            "records": self.records,
            "counts": self.counts,
            "walked": self.walked,
        }


class HeldBack(Conflict):
    """The batch would hold back a key: it commits nothing and is planned
    again (the `replanned` outcome, no try spent). Which output keys a held
    key's load made is the asset's code's to know, so no part of the batch
    is published; the gate keeps it waiting while its upstream is
    incomplete, so it is planned again once, not in a loop."""

    replan = True


def held(ops: tuple, reports: list) -> None:
    """Refuse a batch `progress_ops` holds back a key of that its worker
    processed (§7 step 4). A key the worker itself held back, at load, was
    never given to the asset: the batch commits, and the key stays owed."""

    spared = {key for report in reports for key in report[2]}
    if keys := sorted({k for keys in ops[2] for k in keys} - spared):
        raise HeldBack(f"a clear came in on an input since the batch planned: {len(keys)} keys held back")


async def plan_of(core: Core, task: dict, attempt: str, prepared: dict) -> bytes | None:
    """The attempt's plan: kept in memory since it was prepared here, else
    read back from its spec (an engine that adopted it after a restart)."""

    if not any((p or {}).get("kind") in ("core", "log") for p in (prepared.get("plans") or {}).values()):
        return None
    if (plan := core.plans.get(attempt)) is not None:
        return plan
    spec = await core.engine.state.attempt_spec(task["run"], attempt)
    if spec is None or spec.get("plan") is None:
        raise Conflict(f"attempt {attempt}: its spec and the plan in it are gone")
    return unb64(spec["plan"])


@contextlib.asynccontextmanager
async def changes_of(core: Core, name: str, entry: dict):
    """A write's changed keys: in the result, or in the file the worker put
    beside its output when they were too many for it (N13), streamed to a
    local file whose path `build_commit` maps, so the engine never holds a
    large write's keys."""

    if entry.get("changes") is not None:
        yield unb64(entry["changes"])
        return
    path = entry.get("changes_path")
    if path is None:
        raise Conflict(f"keyed output {name}: the result carries no changed keys", retryable=False)
    try:
        found = await obstore.get_async(core.engine.state.objects, path)
    except (NotFoundError, FileNotFoundError):
        raise Conflict(f"keyed output {name}: its changed keys at {path} are gone") from None
    with tempfile.NamedTemporaryFile() as f:
        async for chunk in found.stream():
            f.write(chunk)
        f.flush()
        yield f.name


async def build(core: Core, task: dict, attempt: str, prepared: dict, result: dict, outputs: dict) -> Built:
    """Build a batch's commit: each keyed output's commit and the keyed
    inputs' progress, joined into one event. Returns holding the locks of the
    outputs it built, for the caller to record under them and then release
    (`Built.locks`)."""

    engine, m = core.engine, core.m
    partition = task["partition"]
    built = Built(partition=partition, generation=Generation(m, "its commit was built"))
    events = []
    try:
        with built.generation:  # an error under a newer deploy: planned again
            claim = m.claimed(attempt)
            generation = int((claim or {}).get("generation") or 0)
            producer = canonical({"run": task["run"], "attempt": attempt})
            clear = full_run(prepared)
            plan = await plan_of(core, task, attempt, prepared)
            keyed, logs, reports, ops, foreseen, each = [], [], [], None, None, None
            keyed_outputs = [
                (name, info, outputs.get(name) or {"unchanged": True})
                for name, info in sorted((prepared.get("outputs") or {}).items())
                if info.get("contract", {}).get("key") is not None
            ]
            if plan is not None:
                keyed = sorted(
                    ((p, plan) for p, plan in prepared["plans"].items() if "core" in (plan or {})),
                    key=lambda item: item[1]["core"],
                )
                logs = [p for p, plan in prepared["plans"].items() if (plan or {}).get("kind") == "log"]
                delivered = result.get("delivered") or {}
                failed = result.get("outcomes") or {}  # a per-key batch's keys that did not succeed
                reports = [
                    (
                        [row[0] for row in failed.get(p) or ()],
                        sorted(map(tuple, (delivered.get(p) or {}).get("loaded") or ())),
                        sorted((delivered.get(p) or {}).get("held") or ()),
                    )
                    for p, _ in keyed
                ]
                each = per_key(core, task, attempt, prepared, keyed, failed)
                # The batch records name the output commit, built after: its number is foreseen,
                # so the batch costs one call (4 ms per 10k keys), not two.
                foreseen = next(
                    (
                        (m.core_output(n, partition) or {"head": 0})["head"] + 1
                        for n, _, e in keyed_outputs
                        if not e.get("unchanged")
                    ),
                    None,
                )
                ops = await progress(core, plan, reports, producer, foreseen, each)
            # Every output lock the batch needs, in the order of the lives' ids, as any other taker of
            # several does: an id is never renamed. Its names are read now, so under the deploy it
            # planned under, or a life would be minted for a name the deploy took away.
            built.generation.check()
            ids = {
                name: core.named(name, partition)
                for name, info in sorted((prepared.get("outputs") or {}).items())
                if info["contract"].get("incremental")
                and not (outputs.get(name) or {"unchanged": True}).get("unchanged")
            }
            for id_ in sorted(ids.values()):
                await built.locks.enter_async_context(core.lock(id_))
            for name, info in sorted((prepared.get("outputs") or {}).items()):
                if "log" in info and name in ids:  # the layer its worker wrote, as the head's next commit
                    event, found = log_commit(core, name, ids[name], info["log"], outputs[name], clear)
                    events.append(event)
                    built.counts[name] = found
            first = None
            for name, info, entry in keyed_outputs:
                if name not in ids:
                    continue
                id_ = ids[name]
                core.attach(id_)
                out = m.core_output(name, partition)
                dynamic = bool(info.get("dynamic"))
                async with changes_of(core, name, entry) as changes:
                    found = await core.run(
                        core.tracking.build_commit,
                        id_,
                        changes,
                        version=generation,
                        path=core.path(out["life"], out["head"] + 1, attempt),
                        time_ms=int(engine.clock() * 1000),
                        producer=producer,
                        mode="clear" if clear else "replace" if entry.get("whole") else "patch",
                        collect=DYNAMIC_KEYS if dynamic else 0,
                    )
                if found is None:
                    continue
                events.append(found["event"])
                first = found["commit"] if first is None else first
                built.counts[name] = {k: found[k] for k in ("commit", "live", "added", "updated", "removed")}
                if dynamic:
                    built.elements[name] = await elements(core, name, partition, out, found)
            built.commits = list(events)
            if plan is not None:
                if first != foreseen:  # an output built nothing, or moved meanwhile
                    # a clear may have come in while the outputs were built
                    ops = await progress(core, plan, reports, producer, first, each)
                built.again = (plan, reports, producer, first, each, [p for p, _ in keyed])
                take(built, ops)
                some = keyed[0][1] if keyed else prepared["plans"][logs[0]]
                built.progress = {
                    "batch": some["index"],
                    "key": some.get("end"),  # where the keyed inputs' walk goes on, None once it ended
                    "deploy": m.deploy_number,  # a reset since starts the walk over (`prepare.restarted`)
                }
                # Keys held back under a rebuild are this run's to take once the rebuild is done:
                # its walk goes on, from the first key, over what is still owed.
                if any(
                    report[2] and rebuilding(core, plan["output"], plan["upstream_partition"], plan["head"])
                    for (_, plan), report in zip(keyed, reports, strict=True)
                ):
                    built.progress.update(key="", held=True)
                built.walked = [p for p, _ in keyed] + logs
            elif events:
                built.event = Tracking.join(events)
            return built
    except BaseException:
        await built.locks.aclose()
        raise


def per_key(core: Core, task: dict, attempt: str, prepared: dict, keyed: list, failed: dict) -> dict | None:
    """A per-key batch's outcomes as `progress` commits them: its per-key
    input's edge and place in the plan, the keys that did not succeed as the
    worker reported them, and the conditions the batch was planned under
    (`prepare.conditions`, from what the attempt was prepared under)."""

    from .prepare import conditions  # prepare imports this module

    inputs = core.m.manifest["assets"][task["asset"]]["inputs"]
    found = next(((p, plan) for p, plan in keyed if (inputs.get(p) or {}).get("each") is not None), None)
    if found is None:
        return None
    param, plan = found
    base = {"context": prepared.get("context"), "definition": prepared.get("definition") or ""}
    return {
        "edge": edge_id(task["asset"], task["partition"], param),
        "asset": task["asset"],
        "by": attempt,
        "input": plan["core"],
        "reported": [tuple(row) for row in failed.get(param) or ()],
        "conditions": conditions(inputs[param], base),
    }


async def progress(
    core: Core, plan: bytes, reports: list, producer: bytes, output_commit, each: dict | None = None
) -> tuple:
    """What committing the plan writes to its inputs' edges (`progress_ops`),
    refusing a batch that holds a key back, or whose log started over since
    it was planned: it read rows the log no longer holds. With `each` (a
    per-key batch, `per_key`), its keys' outcomes too, once its edge has
    caught up; refused when the edge's records moved since it was planned."""

    for _ in range(outcomes.TRIES):
        more = {}
        if each is not None:
            await outcomes.catch_up(core, each["edge"], each["conditions"], each["asset"])
            more["outcomes"] = {
                "input": each["input"],
                "reported": each["reported"],
                "conditions": each["conditions"],
                **outcomes.clock(core, each["edge"], each["asset"], each["by"]),
            }
        try:
            ops = await core.run(
                core.tracking.progress_ops,
                plan,
                reports,
                producer=producer,
                output_commit=output_commit,
                **more,
            )
        except OutcomesMovedError:  # the edge moved since it caught up: again
            continue
        except UnsettledError as error:  # a log started over, or the edge's records moved, since it planned
            raise HeldBack(f"planned again: {error}") from None
        held(ops, reports)
        return ops
    raise HeldBack("the per-key records kept moving while the batch's commit was built")


def take(built: Built, ops: tuple) -> None:
    """What `progress_ops` built becomes the batch's: its event, with the
    outputs' commits after it, its records and its keys' stored outcomes."""

    params = built.again[5]
    built.event = Tracking.join([ops[0], *built.commits])
    built.records = [[p, b64(r)] for p, r in zip(params, ops[1], strict=True)]
    built.stored = ops[3] if len(ops) > 3 else {}


def moved(built: Built, error: Exception) -> bool:
    """Whether a refusal of `built` is of outcomes the state moved under
    since they were built: then they are built again (`rebuild`), a few
    times at most."""

    return (
        isinstance(error.__cause__, OutcomesMovedError)
        and built.again is not None
        and built.rebuilds < outcomes.TRIES
    )


async def rebuild(core: Core, built: Built) -> None:
    """Build a per-key batch's progress and outcomes again, from the state
    as it is now, its outputs' commits kept."""

    built.rebuilds += 1
    plan, reports, producer, commit, each, _ = built.again
    take(built, await progress(core, plan, reports, producer, commit, each))


def log_commit(core: Core, name: str, id_: bytes, planned: dict, entry: dict, walk: bool) -> tuple:
    """The event that installs the layer a worker wrote for a log, and what
    it leaves (`{"commit", "live"}`): the layer the engine named at prepare,
    as the commit after the head. A full run of one commit ends with it; a
    walk's ends when the walk does (`full.settle`)."""

    layer = entry.get("layer")
    if layer is None or layer["path"] != planned["path"] or bool(layer["clears"]) != planned["clear"]:
        raise Conflict(f"output {name}: the result's layer is not the one this attempt was to write")
    try:
        return core.tracking.log_commit(id_, layer, ends_full_run=planned["clear"] and not walk)
    except ValueError as error:
        raise Conflict(f"output {name}: {error}") from error


async def elements(core: Core, name: str, partition: str, out: dict, found: dict) -> list[str]:
    """A dynamic partitions output's elements after a commit: the head's,
    changed by the commit's keys; past `DYNAMIC_KEYS` of them, its keys at
    the new head, read from its index."""

    head = core.m.heads.get((name, partition)) or {}
    if found["upserted_keys"] is not None:
        keys = set(head.get("partitions") or ())
        keys |= set(found["upserted_keys"])
        keys -= set(found["removed_keys"])
        return sorted(keys)
    layers, after, keys = [*out["layers"], found["layer"]], None, []
    while True:
        rows, after = await core.run(core.index.scan, layers, found["commit"], after=after, limit=100_000)
        keys += [row[0] for row in rows]
        if after is None:
            return keys


def full_run(prepared: dict) -> bool:
    """Whether a batch's commit starts its outputs' full run (the clear,
    plus the gate): a full run's first batch of an incremental asset clears,
    its outputs start over, and their readers wait until the run completes
    (`full`). The core's mark of the run ends once its spans close
    (`full.settle`)."""

    plans = [p for p in (prepared.get("plans") or {}).values() if p]
    return bool(prepared.get("full")) and bool(plans)


def check(core: Core, built: Built) -> None:
    """The commit check of the core (§7): the event applies to the state as it
    is now, or nothing is recorded. Run in the synchronous step that records.
    An input that cleared after the batch's progress was built would have held
    keys back: its fence refuses the batch, which is planned again."""

    built.generation.check()  # a deploy since it was built: its names may be another output's
    if built.event is None:
        return
    try:
        core.check(built.event)
    except Conflict as error:
        if isinstance(error.__cause__, UnsettledError):  # the core's fence refusal
            raise HeldBack(str(error)) from None
        raise
