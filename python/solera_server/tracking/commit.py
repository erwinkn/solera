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
from solera._native import Tracking

from ..state import Conflict
from . import Core, b64, canonical, unb64

DYNAMIC_KEYS = 1_000_000  # a dynamic partitions output's elements, read from its commit's keys up to this


@dataclass
class Built:
    """What a batch commits on the core, built and not yet recorded."""

    event: bytes | None = None
    minted: list = field(default_factory=list)  # [output, partition, id]: outputs at their first commit
    records: list = field(default_factory=list)  # [input, base64 of a batch record]
    counts: dict = field(default_factory=dict)  # output -> {id, commit, live, added, updated, removed}
    elements: dict = field(default_factory=dict)  # a dynamic partitions output -> its elements after
    progress: dict | None = None  # the run's walk over the keyed inputs: {batch, key}
    walk: dict | None = None  # the batch's range of it: {inputs, after, hi, reset}
    locks: contextlib.AsyncExitStack = field(default_factory=contextlib.AsyncExitStack)

    def payload(self) -> dict | None:
        """The commit's `core` field, as the model applies it."""

        if self.event is None and not self.minted:
            return None
        return {
            "event": b64(self.event) if self.event is not None else None,
            "outputs": self.minted,
            "records": self.records,
            "counts": self.counts,
            "walk": self.walk,
        }


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
    built = Built()
    events = []
    try:
        claim = m.claimed(attempt)
        generation = int((claim or {}).get("generation") or 0)
        producer = canonical({"run": task["run"], "attempt": attempt})
        first = None
        for name, entry in sorted(outputs.items()):
            info = (prepared.get("outputs") or {}).get(name) or {}
            if info.get("contract", {}).get("key") is None or entry.get("unchanged"):
                continue
            id_ = core.output_id(name, partition, built.minted)
            await built.locks.enter_async_context(core.lock(id_))
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
                    mode="replace" if entry.get("whole") else "patch",
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
        plan = await plan_of(core, task, attempt, prepared)
        if plan is not None:
            keyed = sorted(
                ((p, plan) for p, plan in prepared["plans"].items() if "core" in (plan or {})),
                key=lambda item: item[1]["core"],
            )
            delivered = result.get("delivered") or {}
            reports = [
                ([], sorted(map(tuple, (delivered.get(p) or {}).get("loaded") or ()))) for p, _ in keyed
            ]
            progress, records = await core.run(
                core.tracking.progress_ops, plan, reports, producer=producer, output_commit=first
            )
            events.insert(0, progress)
            built.records = [[p, b64(r)] for (p, _), r in zip(keyed, records, strict=True)]
            some, ended = keyed[0][1], all(p["final"] for _, p in keyed)
            built.progress = {"batch": some["index"], "key": None if ended else some["end"]}
            built.walk = {
                "inputs": [p for p, _ in keyed],
                "after": some["after"],
                "hi": None if ended else some["end"],
                "reset": bool(some["full"]),
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


def unkeyed(prepared: dict) -> dict:
    """The preparation as today's path commits its unkeyed incremental
    inputs, by commit numbers: without the keyed inputs the core commits."""

    plans = {
        p: plan for p, plan in (prepared.get("plans") or {}).items() if (plan or {}).get("kind") == "commits"
    }
    return {**prepared, "plans": plans}


def check(core: Core, built: Built) -> None:
    """The commit check of the core (§7): the event applies to the state as it
    is now, or nothing is recorded. Run in the synchronous step that records."""

    core.check(built.event, built.minted)
