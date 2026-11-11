"""The engine on the new tracking core (docs/tracking-design.md; build plan B5).

`Core` is what an engine on the new path holds beside its model: the change
index every output's layers live in, the ids the core knows outputs and
edges by, and the cap on heavy core calls. The core's state itself is the
model's (`Model.core`), changed only by the events the model applies, so a
replay of the journal rebuilds it.

Every call into the core is one crossing (rule 2). A plan and a commit's
build read the state and do I/O: they run on a thread (`run`), at most
`HEAVY` at once, and the event loop never waits on them. The check of an
event, its record and the model's `apply` are one synchronous step, so a
refused event never reaches the journal and a recorded one always applies
(rule 3).

Modules: `prepare` plans a batch, `commit` commits one, `sources` commits a
source, `stale` answers staleness on demand. `engine-seams.md` in the crew's
drive lists the hooks later units add.
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
from base64 import b64decode, b64encode

import orjson
from solera import lifecycle
from solera.change_index import open_index
from solera.threads import in_thread

from ..state import Conflict, LostOwnership

HEAVY = 4  # plans and commit builds running at once: the engine's cores (N9, D208)
# The keyed inputs' walk in a task's progress, beside unkeyed inputs': never a parameter's name.
WALK = "(core)"

log = logging.getLogger(__name__)


def b64(data: bytes) -> str:
    return b64encode(data).decode()


def unb64(text: str) -> bytes:
    return b64decode(text)


def canonical(value) -> bytes:
    """A value's bytes as the core compares them: JSON, keys sorted."""

    return orjson.dumps(value, option=orjson.OPT_SORT_KEYS)


def output_pin(core: Core, output: str, partition: str, layers: dict) -> dict | None:
    """A keyed output as a worker reads it whole: the change index it is in,
    and its id and head there (None: never committed). Its layers at that
    head, from which the worker names the output's objects, go in `layers`
    by (id, head), for the attempt's sidecar: the spec never lists them."""

    found = core.m.core_output(output, partition)
    if found is None:
        return None
    layers.setdefault((found["id"], found["head"]), found["layers"])
    return {"url": core.engine.state.objects_url, "id": found["id"], "head": found["head"]}


class Redeployed(Conflict):
    """A deploy landed while an operation awaited: what it read by name (a
    name's life, an edge, an axis, a store) may be another thing's now, so
    nothing it decided is acted on. Each operation says what that means for
    it, starting over or "not known" (`Generation`); a batch is planned
    again, no try spent."""

    replan = True


class Generation:
    """The deploy an operation on the new path runs under, the one owner of
    "which deploy this decision was made under" (pr2-design.md, the deploy
    generation rule). Every operation that reads deploy-dependent state
    across an await takes one when it starts: `check` raises `Redeployed`
    once a deploy has landed, at the step that acts on what it read, and in
    its block an error raised under a newer deploy is `Redeployed` too (a
    native call on an edge or a life the deploy closed), but for a lost
    claim, which is no one's to plan again."""

    def __init__(self, m, what: str = "it awaited") -> None:
        self.m, self.deploy, self.what = m, m.deploy_number, what

    @property
    def moved(self) -> bool:
        return self.m.deploy_number != self.deploy

    def check(self) -> None:
        if self.moved:
            raise Redeployed(f"a deploy came in while {self.what}")

    def __enter__(self) -> Generation:
        return self

    def __exit__(self, kind, error, _tb) -> None:
        if isinstance(error, Exception) and not getattr(error, "replan", False) and self.moved:
            if not isinstance(error, LostOwnership):
                raise Redeployed(f"a deploy came in while {self.what}") from error


class Core:
    """The new path's runtime beside an engine (`Engine.core`)."""

    def __init__(self, engine, cache: str | None = None):
        self.engine = engine
        url = engine.state.objects_url
        try:
            self.index = open_index(url, os.path.join(cache, "core") if cache else None)
        except ValueError as error:  # its disk cache is another's (an engine this one took over from)
            log.warning("the change index runs without a disk cache: %s", error)
            self.index = open_index(url)
        self.heavy = asyncio.Semaphore(HEAVY)
        self.locks: dict[str, asyncio.Lock] = {}  # output id -> held while a commit of it is built
        self.plans: dict[str, bytes] = {}  # attempt -> its plan, while it runs here (else its sidecar's)
        self._attached: tuple[int, set[str]] = (0, set())
        # Ids minted by this process: with the event counter, unique in the namespace. An id
        # minted and never recorded names nothing; one recorded moved the counter on.
        self._minted = 0

    @property
    def m(self):
        return self.engine.m

    @property
    def tracking(self):
        return self.m.core

    async def run(self, fn, /, *args, **kwargs):
        """A heavy core call (a plan, a commit's build) on a thread, `HEAVY` at
        most at once. A cancelled caller waits for the thread (`in_thread`):
        what it holds, the call holds."""

        async with self.heavy:
            return await in_thread(fn, *args, **kwargs)

    def attach(self, *ids: str) -> None:
        """The outputs a call reads or writes, attached to the change index:
        runtime configuration, not state, so every engine attaches again."""

        tracking = self.tracking
        if self._attached[0] != id(tracking):
            self._attached = (id(tracking), set())
        seen = self._attached[1]
        for id_ in ids:
            if id_ not in seen:
                tracking.attach(id_.encode(), self.index)
                seen.add(id_)

    def lock(self, id_: str) -> asyncio.Lock:
        """Held while a commit of output `id_` is built and recorded: an
        output's commits are built one at a time, in commit order."""

        return self.locks.setdefault(id_, asyncio.Lock())

    def output_id(self, output: str, partition: str, minted: list) -> str:
        """The core's id of an output's current life. A new one is minted and
        added to `minted`, for the event that first names it to record;
        `check` refuses it if another event named one first."""

        id_ = self.m.core_outputs.get((output, partition))
        if id_ is not None:
            return id_
        if ((self.m.manifest or {}).get("outputs", {}).get(output) or {}).get("key") is None:
            # A life is the current deploy's keyed output's alone (`Generation` keeps names current).
            raise Conflict(f"output {output!r} is no keyed output of this deploy: no life is minted for it")
        for o, p, i in minted:
            if (o, p) == (output, partition):
                return i
        self._minted += 1
        id_ = f"o{self.m.event_counter}.{self._minted}"
        minted.append([output, partition, id_])
        return id_

    def named(self, output: str, partition: str) -> str:
        """The id of an output's current life, minted and recorded at once
        if it has none: an id a commit is built under is never another
        event's to name first (a consumer opening an edge on it)."""

        minted: list = []
        id_ = self.output_id(output, partition, minted)
        if minted:
            self.engine.state.record({"type": "Core", "event": None, "outputs": minted, "edges": []})
        return id_

    def edge_id(self) -> str:
        """A fresh edge id, minted as output ids are."""

        self._minted += 1
        return f"e{self.m.event_counter}.{self._minted}"

    @staticmethod
    def path(id_: str, commit: int, by) -> str:
        """Where a commit's layer goes: under its output's id, named by its
        commit, who made it, and this build alone. Two builds never share an
        object: an engine that lost its namespace may still be uploading one
        while its successor builds the same commit again."""

        return f"core/{id_}/{commit:012d}-{by}-{secrets.token_hex(8)}.lay"

    def spec(self, prepared: dict, base: str) -> tuple[dict, bytes | None]:
        """What the new path adds to an attempt's spec, whose objects are at
        `base`: the field that sends its worker down the new path, and the
        name of its sidecar. With the sidecar's bytes, to write before the
        spec: the plan, and the layers of the keyed outputs it reads (ERW-30).
        So a spec's size is neither its batch's nor its outputs' history's."""

        out = {"tracking": "core"}
        plan, layers = prepared.get("plan"), prepared.get("layers") or {}
        if plan is None and not layers:
            return out, None
        out["sidecar"] = f"{base}{lifecycle.SIDECAR}"
        return out, lifecycle.sidecar(plan, [[id_, head, found] for (id_, head), found in layers.items()])

    def check(self, event: bytes | None, minted=()) -> None:
        """Refuse an event the core would not apply, applying nothing; and ids
        minted for outputs another event has named since."""

        for output, partition, id_ in minted:
            if self.m.core_outputs.get((output, partition), id_) != id_:
                raise Conflict(f"output {output!r} was first committed elsewhere meanwhile; retry")
        if event is None:
            return
        try:
            self.tracking.check(event)
        except ValueError as error:
            raise Conflict(f"the tracking core refused the commit: {error}") from error

    def check_payload(self, payload: dict | None) -> None:
        """`check`, for a commit's `core` field as the model will apply it."""

        if payload is not None:
            event = payload.get("event")
            self.check(unb64(event) if event is not None else None, payload.get("outputs") or ())
