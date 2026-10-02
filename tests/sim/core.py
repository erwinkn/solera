"""The deterministic substrate of the simulation: an event loop on virtual
time, seeded ids, and an object store whose every request goes through one
seeded fault plan.

Determinism comes from three rules:

- **Time is virtual.** `SimLoop.time()` is a counter that jumps to the next
  timer whenever nothing is ready, so a 60 s timeout costs no wall time and
  fires at exactly the same point every run. `time.time()` reads it too.
- **Nothing runs beside the loop.** Object requests are answered
  synchronously (obstore's blocking calls against a real `file://` store)
  and work handed to a thread runs to completion while the loop waits, so
  the loop's FIFO order is the only order there is.
- **Randomness is seeded.** ULIDs, invocation tokens and writer nonces come
  from the example's RNG; faults and delays from the fault plan's.

A request the engine or a worker makes can fail before it reaches the store
(a 5xx), land and lose its answer, or be delayed; an actor the simulation
killed (a crashed engine, a dead worker) can make no request at all.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import os
import random
import secrets
import selectors
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import obstore
from obstore.exceptions import GenericError, NotFoundError

EPOCH = 1_790_000_000.0  # virtual 0 is 2026-09-21: wall time is EPOCH + loop time


class Deadlock(RuntimeError):
    """The loop has nothing ready and nothing scheduled: what it waits for can never come."""


class Killed(BaseException):
    """Raised into an actor the simulation killed. A `BaseException`, so no
    `except Exception` in product code survives it — as no code survives a
    SIGKILL."""


# Who is acting: ("engine", n) or ("worker", attempt, invocation n), or None
# for the simulation itself. Tasks inherit it from whoever created them.
actor: contextvars.ContextVar[tuple | None] = contextvars.ContextVar("sim_actor", default=None)


class _VirtualSelector:
    """A selector that never blocks: when nothing is ready it moves the
    loop's clock to the next timer instead of sleeping until it."""

    def __init__(self):
        self.real = selectors.DefaultSelector()
        self.loop: SimLoop | None = None

    def select(self, timeout=None):
        events = self.real.select(0)
        if events or timeout == 0:
            return events
        if timeout is None:
            raise Deadlock("the loop has nothing ready and no timer: what it awaits can never come")
        self.loop._now += timeout
        return []

    def __getattr__(self, name):
        return getattr(self.real, name)


class SimLoop(asyncio.SelectorEventLoop):
    def __init__(self):
        selector = _VirtualSelector()
        super().__init__(selector)
        selector.loop = self
        self._now = 0.0
        self.threaded = 0  # executor calls made: each ran to its end while the loop waited

    def time(self) -> float:
        return self._now

    def run_in_executor(self, executor, func, *args):
        """Run `func` to its end on a thread of its own while the loop waits:
        deterministic, and a function that runs a loop of its own (upkeep's
        compactions) still can. A function marked `__sim_async__` (a channel
        call that needs this very loop) is awaited here instead."""

        target = func.args[0] if isinstance(func, functools.partial) and func.args else func
        if hasattr(target, "__sim_async__"):
            rest = func.args[1:] if target is not func else args
            return asyncio.ensure_future(target.__sim_async__(*rest, **getattr(func, "keywords", {})))
        future = self.create_future()
        box: dict = {}

        def call():
            try:
                box["value"] = func(*args)
            except BaseException as error:  # noqa: BLE001 — handed back to the loop as is
                box["error"] = error

        import threading

        thread = threading.Thread(target=call, name="sim-executor", daemon=True)
        thread.start()
        thread.join()
        self.threaded += 1
        if "error" in box:
            future.set_exception(box["error"])
        else:
            future.set_result(box.get("value"))
        return future

    def advance(self, seconds: float) -> None:
        """Run everything due in the next `seconds` of virtual time."""

        self.run_until_complete(asyncio.sleep(seconds))


# -- seeded identity ---------------------------------------------------------------------


class _SeededOs:
    """What `solera.ids` reads of `os`: urandom, from the example's RNG."""

    def __init__(self, rng: random.Random):
        self.rng = rng

    def urandom(self, n: int) -> bytes:
        return self.rng.randbytes(n)

    def __getattr__(self, name):
        return getattr(os, name)


class Determinism:
    """Patches that make one example replay exactly: wall time from the
    loop, ids and tokens from a seed. Active only while the loop runs, so
    Hypothesis keeps its own clocks."""

    def __init__(self, loop: SimLoop, seed: int):
        self.loop = loop
        self.rng = random.Random(seed)
        self._saved: list[tuple] = []
        self._started = False

    def __enter__(self):
        import solera.ids as ids

        rng = self.rng
        if not self._started:  # a fresh example: no id carries over from the last one
            ids._last[:] = [-1, 0]
            self._started = True

        def token_hex(n: int = 32) -> str:
            return rng.randbytes(n).hex()

        patches = [
            (time, "time", lambda: EPOCH + self.loop._now),
            (ids, "os", _SeededOs(rng)),
            (secrets, "token_hex", token_hex),
        ]
        for owner, name, value in patches:
            self._saved.append((owner, name, getattr(owner, name)))
            setattr(owner, name, value)
        return self

    def __exit__(self, *exc):
        while self._saved:
            owner, name, value = self._saved.pop()
            setattr(owner, name, value)


# -- the object store ----------------------------------------------------------------------


@dataclass
class FaultPlan:
    """How often requests fail, per request: `error` before reaching the
    store (a 5xx), `lost` after landing (the answer never came), and how
    long they may be `delay`ed (virtual seconds, uniform up to the bound)."""

    error: float = 0.0
    lost: float = 0.0
    delay: float = 0.0
    rng: random.Random = field(default_factory=lambda: random.Random(0))
    enabled: bool = True

    def decide(self) -> tuple[str | None, float]:
        if not self.enabled:
            return None, 0.0
        roll = self.rng.random()
        fate = "error" if roll < self.error else "lost" if roll < self.error + self.lost else None
        delay = self.rng.random() * self.delay if self.delay else 0.0
        return fate, delay


class _Got:
    """A `GetResult` read eagerly: its bytes, synchronously or not."""

    def __init__(self, data: bytes, meta):
        self._data, self.meta = data, meta

    def bytes(self) -> bytes:
        return self._data

    async def bytes_async(self) -> bytes:
        return self._data


class _Listing:
    """A listing collected at once, iterable either way, in batches."""

    def __init__(self, batches):
        self.batches = batches

    def __iter__(self):
        return iter(self.batches)

    def __aiter__(self):
        async def gen():
            for batch in self.batches:
                yield batch

        return gen()

    def collect(self):
        return [m for b in self.batches for m in b]

    async def collect_async(self):
        return self.collect()


@dataclass
class Op:
    """One request, as the simulation saw it."""

    at: float
    kind: str  # put | create | get | range | list | delete
    path: str  # the store's root joined with the request's path
    who: tuple | None
    fate: str | None
    found: bool = True
    gone: tuple | None = None  # (when, by whom) the path was deleted before this request


class Objects:
    """Every obstore request of the process, answered synchronously through
    the real store, with the fault plan applied to the simulation loop's
    requests. `hook(who, kind, path, when)` runs before (`when="before"`)
    and after each one: the world uses it to kill or pause an actor at a
    lifecycle step. `log` keeps the requests that matter to invariants."""

    def __init__(self, loop: SimLoop, plan: FaultPlan):
        self.loop, self.plan = loop, plan
        self.dead: set[tuple] = set()  # actors that can make no request any more
        self.hook: Callable | None = None
        self.tap: Callable | None = None  # (full path, bytes) of every write that landed
        self.log: list[Op] = []
        self.deleted: dict[str, tuple] = {}  # full path -> (when, by whom) it was deleted
        self.requests = 0
        self._saved: list[tuple] = []
        self._real = {
            name: getattr(obstore, name)
            for name in (
                "put_async",
                "get_async",
                "get_range_async",
                "delete_async",
                "list",
                "put",
                "get",
                "delete",
            )
        }

    # -- install ----------------------------------------------------------------------

    def __enter__(self):
        patched = {
            "put_async": self.put_async,
            "get_async": self.get_async,
            "get_range_async": self.get_range_async,
            "delete_async": self.delete_async,
            "list": self.list,
            "put": self.put,
            "get": self.get,
        }
        for name, fn in patched.items():
            self._saved.append((name, getattr(obstore, name)))
            setattr(obstore, name, fn)
        return self

    def __exit__(self, *exc):
        while self._saved:
            name, fn = self._saved.pop()
            setattr(obstore, name, fn)

    # -- helpers ----------------------------------------------------------------------

    @staticmethod
    def full(store, path: str) -> str:
        root = str(getattr(store, "prefix", "") or "")
        return f"{root.rstrip('/')}/{path}" if root else path

    def _on_loop(self) -> bool:
        try:
            return asyncio.get_running_loop() is self.loop
        except RuntimeError:
            return False

    async def _request(self, kind: str, store, path: str, do: Callable, data: bytes | None = None):
        """One request: the actor's hooks, the plan's fate, the real call."""

        who = actor.get()
        full = self.full(store, path)
        on_loop = self._on_loop()
        if who is not None and who in self.dead:
            raise Killed(f"{who} is dead")
        fate, delay = self.plan.decide() if on_loop and who is not None else (None, 0.0)
        if on_loop and self.hook is not None:
            await self.hook(who, kind, full, "before")
        if delay:
            await asyncio.sleep(delay)
            if who is not None and who in self.dead:
                raise Killed(f"{who} is dead")
        self.requests += 1
        op = Op(self.loop._now, kind, full, who, fate, gone=self.deleted.get(full))
        if kind in ("create", "put", "delete") or op.gone is not None:
            self.log.append(op)
        if fate == "error":
            raise GenericError(f"injected: 503 Slow Down ({kind} {path})")
        try:
            value = do()
        except (NotFoundError, FileNotFoundError):
            op.found = False
            if kind == "delete":
                self.deleted[full] = (self.loop._now, who)
            raise
        if kind == "delete":
            self.deleted[full] = (self.loop._now, who)
        elif kind in ("create", "put"):
            self.deleted.pop(full, None)
            if self.tap is not None:
                self.tap(full, data)
        if on_loop and self.hook is not None:
            await self.hook(who, kind, full, "after")
        if fate == "lost":
            raise GenericError(f"injected: connection reset after the request landed ({kind} {path})")
        return value

    # -- the obstore surface ------------------------------------------------------------

    async def put_async(self, store, path, data, *, mode="overwrite", use_multipart=None, **kw):
        kind = "create" if mode == "create" else "put"
        body = bytes(data) if not isinstance(data, bytes) else data
        real = self._real["put"]
        return await self._request(kind, store, path, lambda: real(store, path, body, mode=mode), body)

    async def get_async(self, store, path, **kw):
        real = self._real["get"]

        def do():
            got = real(store, path)
            return _Got(bytes(got.bytes()), got.meta)

        return await self._request("get", store, path, do)

    async def get_range_async(self, store, path, *, start, end=None, length=None):
        def do():
            got = self._real["get"](store, path)
            data = bytes(got.bytes())
            stop = end if end is not None else start + length
            return data[start:stop]

        return await self._request("range", store, path, do)

    async def delete_async(self, store, paths):
        many = [paths] if isinstance(paths, str) else list(paths)
        real = self._real["delete"]

        def do():
            missing = []
            for p in many:  # every path is tried, as obstore does; a missing one raises after
                try:
                    real(store, p)
                except (NotFoundError, FileNotFoundError):
                    missing.append(p)
            for p in many[1:]:
                full = self.full(store, p)
                self.deleted[full] = (self.loop._now, actor.get())
                self.log.append(Op(self.loop._now, "delete", full, actor.get(), None))
            if missing:
                raise FileNotFoundError(f"No such file or directory: {missing[0]}")

        return await self._request("delete", store, many[0] if many else "", do)

    def list(self, store, prefix=None, *, offset=None, **kw):
        real = self._real["list"]
        who = actor.get()
        if who is not None and who in self.dead:
            raise Killed(f"{who} is dead")
        fate, _ = self.plan.decide() if self._on_loop() and who is not None else (None, 0.0)
        self.requests += 1
        if fate == "error":
            raise GenericError(f"injected: 503 Slow Down (list {prefix})")
        args = {"prefix": prefix} if prefix is not None else {}
        if offset is not None:
            args["offset"] = offset
        batches = [list(b) for b in real(store, **args)]
        return _Listing(batches)

    def put(self, store, path, data, **kw):
        return self._real["put"](store, path, data, **kw)

    def get(self, store, path, **kw):
        got = self._real["get"](store, path)
        return _Got(bytes(got.bytes()), got.meta)
