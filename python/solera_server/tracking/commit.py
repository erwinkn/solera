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
"""

from __future__ import annotations

import contextlib
import tempfile
from dataclasses import dataclass, field

import obstore
from obstore.exceptions import NotFoundError
from solera._native import Tracking, UnsettledError

from ..state import Conflict
from . import Core, Generation, b64, canonical, unb64

DYNAMIC_KEYS = 1_000_000  # a dynamic partitions output's elements, read from its commit's keys up to this


@dataclass
class Built:
    """What a batch commits on the core, built and not yet recorded."""

    event: bytes | None = None
    records: list = field(default_factory=list)  # [input, base64 of a batch record]
    counts: dict = field(default_factory=dict)  # output -> {id, commit, live, added, updated, removed}
    elements: dict = field(default_factory=dict)  # a dynamic partitions output -> its elements after
    progress: dict | None = None  # the run's walk over the keyed inputs: {batch, key}
    walk: dict | None = None  # the batch's range of it: {inputs, after, hi, reset}
    partition: str = ""
    generation: Generation | None = None  # the deploy it was built under
    locks: contextlib.AsyncExitStack = field(default_factory=contextlib.AsyncExitStack)

    def payload(self) -> dict | None:
        """The commit's `core` field, as the model applies it."""

        if self.event is None:
            return None
        return {
            "event": b64(self.event),
            "records": self.records,
            "counts": self.counts,
            "walk": self.walk,
        }


class HeldBack(Conflict):
    """The batch would hold back a key: it commits nothing and is planned
    again (the `replanned` outcome, no try spent). Which output keys a held
    key's load made is the asset's code's to know, so no part of the batch
    is published; the gate keeps it waiting while its upstream is
    incomplete, so it is planned again once, not in a loop."""

    replan = True


def held(ops: tuple) -> None:
    """Refuse a batch `progress_ops` holds keys back in (§7 step 4)."""

    if keys := sorted({k for keys in ops[2] for k in keys}):
        raise HeldBack(f"a clear came in on an input since the batch planned: {len(keys)} keys held back")


async def plan_of(core: Core, task: dict, attempt: str, prepared: dict) -> bytes | None:
    """The attempt's plan: kept in memory since it was prepared here, else
    read back from its spec (an engine that adopted it after a restart)."""

    if not any(
        (p or {}).get("kind") == "core" and not p.get("done") for p in (prepared.get("plans") or {}).values()
    ):
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
            keyed, reports, ops, foreseen = [], [], None, None
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
                delivered = result.get("delivered") or {}
                reports = [
                    ([], sorted(map(tuple, (delivered.get(p) or {}).get("loaded") or ()))) for p, _ in keyed
                ]
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
                ops = await core.run(
                    core.tracking.progress_ops, plan, reports, producer=producer, output_commit=foreseen
                )
                held(ops)
            # Every output lock the batch needs, in the order of the lives' ids, as any other taker of
            # several does: an id is never renamed. Its names are read now, so under the deploy it
            # planned under, or a life would be minted for a name the deploy took away.
            built.generation.check()
            ids = {
                name: core.named(name, partition)
                for name, _, entry in keyed_outputs
                if not entry.get("unchanged")
            }
            for id_ in sorted(ids.values()):
                await built.locks.enter_async_context(core.lock(id_))
            first = None
            for name, info, entry in keyed_outputs:
                if name not in ids:
                    continue
                id_ = ids[name]
                core.attach(id_)
                out = m.core_output(name, partition) or {"head": 0, "layers": []}
                dynamic = bool(info.get("dynamic"))
                async with changes_of(core, name, entry) as changes:
                    found = await core.run(
                        core.tracking.build_commit,
                        id_.encode(),
                        changes,
                        version=generation,
                        path=core.path(id_, out["head"] + 1, attempt),
                        time_ms=int(engine.clock() * 1000),
                        producer=producer,
                        mode="clear" if clear else "replace" if entry.get("whole") else "patch",
                        collect=DYNAMIC_KEYS if dynamic else 0,
                    )
                if found is None:
                    continue
                events.append(found["event"])
                first = found["commit"] if first is None else first
                built.counts[name] = {
                    "id": id_,
                    **{k: found[k] for k in ("commit", "live", "added", "updated", "removed")},
                }
                if dynamic:
                    built.elements[name] = await elements(core, name, partition, out, found)
            if plan is not None:
                if first != foreseen:  # an output built nothing, or moved meanwhile
                    ops = await core.run(
                        core.tracking.progress_ops, plan, reports, producer=producer, output_commit=first
                    )
                    held(ops)  # a clear may have come in while the outputs were built
                progress, records, _ = ops
                events.insert(0, progress)
                built.records = [[p, b64(r)] for (p, _), r in zip(keyed, records, strict=True)]
                some = keyed[0][1]
                ended = some["end"] is None  # the plan's walk, not one input's own end
                built.progress = {
                    "batch": some["index"],
                    "key": None if ended else some["end"],
                    "edges": {
                        p: plan["edge"] for p, plan in keyed
                    },  # the walk's own: a closed one starts it over
                }
                built.walk = {
                    "inputs": [p for p, _ in keyed],
                    "after": some["after"],
                    "hi": None if ended else some["end"],
                    "reset": bool(some["full"]),
                    "keys": some.get("mode") == "keys",
                }
            if events:
                built.event = Tracking.join(events)
            return built
    except BaseException:
        await built.locks.aclose()
        raise


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
    try:
        core.check(built.event)
    except Conflict as error:
        if isinstance(error.__cause__, UnsettledError):  # the core's fence refusal
            raise HeldBack(str(error)) from None
        raise
