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
import os
from base64 import b64decode, b64encode

import orjson
from solera.change_index import open_index
from solera.threads import in_thread

from ..state import Conflict

HEAVY = 4  # plans and commit builds running at once: the engine's cores (N9, D208)


def b64(data: bytes) -> str:
    return b64encode(data).decode()


def unb64(text: str) -> bytes:
    return b64decode(text)


def canonical(value) -> bytes:
    """A value's bytes as the core compares them: JSON, keys sorted."""

    return orjson.dumps(value, option=orjson.OPT_SORT_KEYS)


def output_pin(core: Core, output: str, partition: str) -> dict | None:
    """A keyed output as a worker reads it whole: the change index it is in,
    and its layers and head there, from which it names the output's objects
    (None: never committed)."""

    found = core.m.core_output(output, partition)
    if found is None:
        return None
    return {"url": core.engine.state.objects_url, "layers": found["layers"], "head": found["head"]}


class Core:
    """The new path's runtime beside an engine (`Engine.core`)."""

    def __init__(self, engine, cache: str | None = None):
        self.engine = engine
        self.index = open_index(engine.state.objects_url, os.path.join(cache, "core") if cache else None)
        self.heavy = asyncio.Semaphore(HEAVY)
        self.locks: dict[str, asyncio.Lock] = {}  # output id -> held while a commit of it is built
        self.plans: dict[str, bytes] = {}  # attempt -> its plan, while it runs here (else its spec's)
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
        for o, p, i in minted:
            if (o, p) == (output, partition):
                return i
        self._minted += 1
        id_ = f"o{self.m.event_counter}.{self._minted}"
        minted.append([output, partition, id_])
        return id_

    def edge_id(self) -> str:
        """A fresh edge id, minted as output ids are."""

        self._minted += 1
        return f"e{self.m.event_counter}.{self._minted}"

    @staticmethod
    def path(id_: str, commit: int, by) -> str:
        """Where a commit's layer goes: under its output's id, named by its
        commit and who made it, so a commit built again lands on the same
        object."""

        return f"core/{id_}/{commit:012d}-{by}.lay"

    def spec(self, prepared: dict) -> dict:
        """What the new path adds to an attempt's spec: the field that sends
        its worker down the new path, and the plan's bytes."""

        out = {"tracking": "core"}
        if prepared.get("plan") is not None:
            out["plan"] = b64(prepared["plan"])
        return out

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
            raise Conflict(f"the tracking core refused the commit: {error}") from None

    def check_payload(self, payload: dict | None) -> None:
        """`check`, for a commit's `core` field as the model will apply it."""

        if payload is not None:
            event = payload.get("event")
            self.check(unb64(event) if event is not None else None, payload.get("outputs") or ())
