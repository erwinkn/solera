"""What must hold, checked against the system as it is.

`Journal` taps every segment that lands in the object store, so the
history of decisions is known even after checkpoints delete segments.
`Checks` reads the engine's state and the stores the way a reader would
and raises `Violation` with what it found."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field

from solera import lifecycle
from solera.keys.index import KeyIndex, key_str
from solera.keys.io import ObjectIO
from solera.sdk import Ref
from solera.stores import Keys


class Violation(AssertionError):
    """An invariant the system broke."""


@dataclass
class Journal:
    """Every journal segment that landed, by seq: events in order."""

    segments: dict[int, dict] = field(default_factory=dict)
    finished: dict[str, list[tuple[int, dict]]] = field(default_factory=lambda: defaultdict(list))
    launched: dict[str, dict] = field(default_factory=dict)  # attempt -> AttemptLaunched
    problems: list[str] = field(default_factory=list)
    twice: list[tuple] = field(
        default_factory=list
    )  # (seq, path, when): segments that landed again, other bytes
    now: object = None  # the world's clock
    applied_commits: set = field(default_factory=set)  # attempts some engine committed in memory
    gates: dict[str, tuple] = field(default_factory=dict)  # attempt -> (state, worker id, landed at)

    def landed(self, path: str, data: bytes) -> None:
        if path.endswith(lifecycle.GATE):
            gate = json.loads(data)
            attempt = path.rsplit("/", 1)[-1].removesuffix(lifecycle.GATE)
            self.gates.setdefault(attempt, (gate["state"], gate.get("worker_id"), self.now()))
            return
        if "/control/journal/" not in path:
            return
        body = json.loads(data)
        seq = body["seq"]
        if seq in self.segments:
            if self.segments[seq] != body:
                self.twice.append((seq, path, self.now()))
            return
        self.segments[seq] = body
        for event in body["events"]:
            if event["type"] == "AttemptFinished":
                self.finished[event["attempt"]].append((seq, event))
            elif event["type"] == "AttemptLaunched":
                self.launched[event["attempt"]] = event

    def overwritten(self, deleted: dict, now: float) -> list[int]:
        """Segments that landed twice with different bytes, the second still
        there a minute later. (An opener whose fence lands in a hole cleanup
        left deletes it at once and opens again: transient, by design.)"""

        return [seq for seq, path, at in self.twice if path not in deleted and now - at > 60.0]

    def recorded(self, events) -> None:
        """Events an engine applied (durable or not yet): what its model knows."""

        for event in events:
            if (
                event["type"] == "AttemptFinished"
                and event.get("outcome") == "succeeded"
                and event.get("commit")
            ):
                self.applied_commits.add(event["attempt"])
            elif event["type"] == "AttemptLaunched":
                self.launched.setdefault(event["attempt"], event)

    def events(self):
        for seq in sorted(self.segments):
            for event in self.segments[seq]["events"]:
                yield seq, event

    def two_attempts_at_once(self, same=lambda partition: partition) -> str | None:
        """The first durable launch of an attempt on an asset partition
        another launched attempt still holds (`same` names a partition the
        way a rename keeps it), or None."""

        holder: dict[str, str] = {}
        for seq, event in self.events():
            if event["type"] not in ("AttemptLaunched", "AttemptFinished"):
                continue
            partition = same(event["task"].split("/", 1)[1])
            if event["type"] == "AttemptFinished":
                if holder.get(partition) == event["attempt"]:
                    del holder[partition]
            elif holder.get(partition, event["attempt"]) != event["attempt"]:
                return f"{event['attempt']} launched on {partition} at seq {seq}, held by {holder[partition]}"
            else:
                holder[partition] = event["attempt"]
        return None

    def generation(self, attempt: str) -> int | None:
        launched = self.launched.get(attempt)
        return None if launched is None else int(launched.get("pin", -1))

    def committed_generations(self) -> set[int]:
        out = set()
        attempts = set(self.applied_commits)
        for attempt, ends in self.finished.items():
            if any(e["outcome"] == "succeeded" and e.get("commit") for _, e in ends):
                attempts.add(attempt)
        for attempt in attempts:
            if (g := self.generation(attempt)) is not None:
                out.add(g)
        return out


async def index_entries(state, output: str, scope: str) -> dict[str, tuple[int, bytes | None]]:
    """Every live entry of an output scope's key index: key -> (generation, payload)."""

    index = KeyIndex(ObjectIO(state.objects), None, state.model.index(output, scope).pinned())
    entries, after = {}, None
    while True:
        keys, generations, payloads, after = await index.page(after, 100_000)
        entries.update(zip(map(key_str, keys), zip(generations, payloads, strict=True), strict=True))
        if after is None:
            return entries


async def keyed_content(
    engine, project, output: str, scope: str = "", *, whole=False, column: str | None = "v"
) -> dict[str, str | None]:
    """A keyed output's content as a reader of its head gets it: every key
    its index holds, loaded through its store at the generation the index
    holds (docs/versions.md) — no key missing, none read twice, none the
    index does not list. `whole` also checks a store that reads current
    rows holds no row the index does not list (only true when no writer is
    unsettled). Returns each key's `column`, None without one."""

    m = engine.state.model
    head = m.heads.get((output, scope))
    if head is None:
        return {}
    entries = await index_entries(engine.state, output, scope)
    store_name = head["ref"]["store"]
    store = project.stores[store_name]
    ref = Ref.from_json(head["ref"])
    try:
        rows = await store.load(ref, list[dict], Keys({k: g for k, (g, _) in entries.items()}))
    except Exception as error:
        raise Violation(
            f"{output}[{scope!r}]: its committed keys cannot be read: {type(error).__name__}: {error}"
        ) from error
    got: dict[str, str | None] = {}
    for row in rows:
        k = str(row["id"])
        if k in got:
            raise Violation(f"{output}[{scope!r}]: key {k} read twice")
        got[k] = str(row[column]) if column is not None else None
    if set(got) != set(entries):
        missing = sorted(set(entries) - set(got))
        extra = sorted(set(got) - set(entries))
        raise Violation(
            f"{output}[{scope!r}] ({store_name}): the store does not hold what the index says: "
            f"missing {missing[:5]}, extra {extra[:5]}"
        )
    if whole and getattr(store, "writes", None) == "fenced":
        rows = await store.load(ref, list[dict], None)
        extra = sorted({str(r["id"]) for r in rows} - set(entries))
        if extra or len(rows) != len(entries):
            raise Violation(f"{output}[{scope!r}] ({store_name}): rows nobody committed remain: {extra[:5]}")
    return got


async def value_content(engine, project, output: str, scope: str = ""):
    m = engine.state.model
    head = m.heads.get((output, scope))
    if head is None:
        return None
    ref = Ref.from_json(head["ref"])
    store = project.stores[head["ref"]["store"]]
    try:
        return await store.load(ref, object, None)
    except Exception as error:
        raise Violation(
            f"{output}[{scope!r}]: its head cannot be read: {type(error).__name__}: {error}"
        ) from error


async def commit_rows(engine, project, output: str, scope: str = "") -> list:
    head = engine.state.model.heads.get((output, scope))
    if head is None:
        return []
    ref = Ref.from_json(head["ref"])
    store = project.stores[head["ref"]["store"]]
    try:
        return await store.load(ref, list[dict], None)
    except Exception as error:
        raise Violation(
            f"{output}[{scope!r}]: its batches cannot be read: {type(error).__name__}: {error}"
        ) from error
