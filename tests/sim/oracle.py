"""What must hold, checked against the system as it is.

`Journal` taps every segment that lands in the object store, so the
history of decisions is known even after checkpoints delete segments.
`Checks` reads the engine's state and the stores the way a reader would
and raises `Violation` with what it found."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field

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
    applied_commits: set = field(default_factory=set)  # attempts some engine committed in memory

    def landed(self, path: str, data: bytes) -> None:
        if "/control/journal/" not in path:
            return
        body = json.loads(data)
        seq = body["seq"]
        if seq in self.segments:
            if self.segments[seq] != body:
                self.problems.append(f"segment {seq} landed twice with different bytes")
            return
        self.segments[seq] = body
        for event in body["events"]:
            if event["type"] == "AttemptFinished":
                self.finished[event["attempt"]].append((seq, event))
            elif event["type"] == "AttemptLaunched":
                self.launched[event["attempt"]] = event

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


async def index_entries(state, output: str, scope: str) -> dict[str, tuple[bytes, int]]:
    """Every live entry of an output scope's key index: key -> (version, locator)."""

    index = KeyIndex(ObjectIO(state.objects), None, state.model.index(output, scope).pinned())
    entries, after = {}, None
    while True:
        keys, versions, locators, after = await index.page(after, 100_000)
        entries.update({key_str(k): (v, loc) for k, v, loc in zip(keys, versions, locators, strict=True)})
        if after is None:
            return entries


def text(version) -> str:
    return version.decode() if isinstance(version, bytes) else str(version)


async def keyed_content(engine, project, output: str, scope: str = "", *, whole=False) -> dict[str, str]:
    """A keyed output's content as a reader of its head gets it: every key
    its index holds, loaded through its store, each row's `v` checked
    against the version the index holds. `whole` also checks a store that
    reads current rows holds no row the index does not list (only true
    when no writer is unsettled)."""

    m = engine.state.model
    head = m.heads.get((output, scope))
    if head is None:
        return {}
    entries = await index_entries(engine.state, output, scope)
    store_name = head["ref"]["store"]
    store = project.stores[store_name]
    ref = Ref(**{k: head["ref"][k] for k in ("output", "store", "handle", "version", "partition", "meta")})
    try:
        rows = await store.load(ref, list[dict], Keys(entries))
    except Exception as error:
        raise Violation(
            f"{output}[{scope!r}]: its committed keys cannot be read: {type(error).__name__}: {error}"
        ) from error
    column = (engine.manifest["outputs"].get(output) or {}).get("revision") or "v"
    got: dict[str, str] = {}
    for row in rows:
        k = str(row["id"])
        if k in got:
            raise Violation(f"{output}[{scope!r}]: key {k} read twice")
        got[k] = str(row[column])
    want = {k: text(v) for k, (v, _) in entries.items()}
    if got != want:
        missing = sorted(set(want) - set(got))
        wrong = sorted(k for k in set(want) & set(got) if want[k] != got[k])
        extra = sorted(set(got) - set(want))
        raise Violation(
            f"{output}[{scope!r}] ({store_name}): the store does not hold what the index says: "
            f"missing {missing[:5]}, wrong {[(k, want[k], got[k]) for k in wrong[:5]]}, extra {extra[:5]}"
        )
    if whole and getattr(store, "writes", None) == "fenced":
        rows = await store.load(ref, list[dict], None)
        extra = sorted({str(r["id"]) for r in rows} - set(want))
        if extra or len(rows) != len(want):
            raise Violation(f"{output}[{scope!r}] ({store_name}): rows nobody committed remain: {extra[:5]}")
    return want


async def value_content(engine, project, output: str, scope: str = ""):
    m = engine.state.model
    head = m.heads.get((output, scope))
    if head is None:
        return None
    ref = Ref(**{k: head["ref"][k] for k in ("output", "store", "handle", "version", "partition", "meta")})
    store = project.stores[head["ref"]["store"]]
    try:
        return await store.load(ref, object, None)
    except Exception as error:
        raise Violation(
            f"{output}[{scope!r}]: its head cannot be read: {type(error).__name__}: {error}"
        ) from error


async def batch_rows(engine, project, output: str, scope: str = "") -> list:
    head = engine.state.model.heads.get((output, scope))
    if head is None:
        return []
    ref = Ref(**{k: head["ref"][k] for k in ("output", "store", "handle", "version", "partition", "meta")})
    store = project.stores[head["ref"]["store"]]
    try:
        return await store.load(ref, list[dict], None)
    except Exception as error:
        raise Violation(
            f"{output}[{scope!r}]: its batches cannot be read: {type(error).__name__}: {error}"
        ) from error
