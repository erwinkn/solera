"""The simulated deployment: engines that start, crash, restart and take
over from each other; workers that run the real worker in process and
can die, pause or run twice at any step of an attempt's lifecycle; a
sensor worker; and the clients that call the API — all on one `SimLoop`.

Every actor runs in a context naming it (`core.actor`), so the object
store knows who asks, the world can kill an actor's every task at once,
and a killed actor can make no request afterwards."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import json
import logging
import os
import random
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from solera import lifecycle
from solera.lifecycle import Ended

from .core import EPOCH, Determinism, FaultPlan, Killed, Objects, SimLoop, actor

log = logging.getLogger("sim")


class Unreachable(ConnectionError):
    """No engine answers (none is up, or this worker's network is cut)."""


# -- worker fates ---------------------------------------------------------------------------

POINTS = ("claim", "start", "delta", "gate", "store", "result", "finished")


@dataclass(frozen=True)
class Fate:
    """What befalls the next worker launched: `die` or `pause` (for
    `seconds`) `before` or `after` its first request at `point`; `mute`
    (its channel never reaches the engine); `twice` (a second worker
    starts `seconds` later)."""

    kind: str
    point: str = "claim"
    when: str = "before"
    seconds: float = 0.0

    def __str__(self):
        if self.kind == "mute":
            return "mute"
        if self.kind == "twice":
            return f"twice+{self.seconds:g}s"
        tail = f" {self.seconds:g}s" if self.kind == "pause" else ""
        return f"{self.kind} {self.when} {self.point}{tail}"


CONTROL_POINTS = {lifecycle.OWNED: "claim", lifecycle.WRITING: "gate", lifecycle.SEALED: "result"}


def _in_creation_order(tasks) -> list[asyncio.Task]:
    """`asyncio.all_tasks()` is a set, ordered by the tasks' addresses: cancelling
    in its order resumed them in an order that changed with whatever the process
    had allocated before (an earlier example, an import, the GC's timing), so one
    program could run two ways. Tasks are named `Task-<n>` in creation order."""

    def key(task):
        name = task.get_name()
        _, _, n = name.rpartition("-")
        return (0, int(n), "") if n.isdigit() else (1, 0, name)

    return sorted(tasks, key=key)


def point_of(full: str, kind: str, data_root: str, data: bytes | None = None) -> str | None:
    """Which lifecycle step a worker's request is (`POINTS`), if any: its
    control file's swaps own the attempt, take the gate and seal the result."""

    if full.endswith(lifecycle.CONTROL):
        return CONTROL_POINTS.get(json.loads(data)["state"]) if kind == "swap" and data else None
    if kind not in ("create", "put"):
        return None
    if "/keys/" in full and full.endswith(".kx"):
        return "delta"
    if full.startswith(data_root):
        return "store"
    return None


# A window's budget per kind of engine activity: (burst, rate per virtual second).
# Calibrated on ordinary runs and F20 (docs/verification.md, "Calibration").
PACE = {"ticks": (200, 10.0), "events": (500, 5.0), "requests": (500, 5.0)}


class Pace:
    """The engines' pace between external inputs (Erwin's D60 ruling: no
    wake floor, so hot loops are the simulation's to catch). Every input — a
    step, a client or worker request, an engine start — opens a window; in
    it, each kind of engine activity (ticks, journal events, store requests)
    may reach `burst + rate × the window's virtual seconds`, no more. A hot
    loop at one virtual instant spends the burst; a slower one, the rate.
    Without `bounds` it only measures: each window's length and counts."""

    KINDS = ("ticks", "events", "requests")

    def __init__(self, now: Callable[[], float], bounds: dict | None):
        self.now, self.bounds = now, bounds  # kind -> (burst, rate per virtual second)
        self.since = 0.0
        self.counts = dict.fromkeys(self.KINDS, 0)
        self.windows: list[tuple[float, dict]] = []  # measuring: (seconds, counts) of each closed window
        self.over: str | None = None  # the first window over its bound
        self.again = 0  # past it: the count at which the loop is stopped once more

    def input(self) -> None:
        if self.bounds is None and any(self.counts.values()):
            self.windows.append((self.now() - self.since, dict(self.counts)))
        self.since, self.counts = self.now(), dict.fromkeys(self.KINDS, 0)

    def bump(self, kind: str, n: int = 1) -> bool:
        """Count `n`; True the first time a window goes over its bound."""

        self.counts[kind] += n
        if self.bounds is None:
            return False
        if self.over is not None:  # the loop still spins (a shutdown runs it): stop it at each doubling
            if self.counts[kind] <= self.again:
                return False
            self.again = 2 * self.counts[kind]
            return True
        burst, rate = self.bounds[kind]
        elapsed = self.now() - self.since
        if self.counts[kind] <= burst + rate * elapsed:
            return False
        self.again = 2 * self.counts[kind]
        self.over = (
            f"a hot loop: {self.counts[kind]} engine {kind} in {elapsed:g} virtual seconds with no input "
            f"(bound {burst} + {rate}/s)"
        )
        return True


@dataclass
class Worker:
    who: tuple
    attempt: str
    run: str
    task: asyncio.Task
    fate: Fate | None
    fired: bool = False  # the fate has struck
    paused_until: float = 0.0


@dataclass
class EngineSlot:
    n: int
    ctx: contextvars.Context
    state: object = None
    engine: object = None
    project: object = None
    dead: bool = False
    stopping: bool = False  # a clean stop has begun


# -- the channel ----------------------------------------------------------------------------


class SimChannel:
    """A worker's channel to whichever engine serves now, as the HTTPS name
    reaches it: each call runs as the engine's own request (so a worker's
    death does not cancel it) and is answered once what it recorded is
    durable, as the API's middleware does."""

    def __init__(self, world: World, attempt: str, who: tuple, muted: bool):
        self.world, self.attempt, self.who, self.muted = world, attempt, who, muted

    async def _call(self, name: str, body):
        if self.muted:
            raise Unreachable("this worker cannot reach the engine")
        return await self.world.request(lambda e: getattr(e, f"attempt_{name}")(self.attempt, body))

    async def start(self, body):
        await self.world.channel_point(self.who, "start")
        return await self._call("start", body)

    async def abeat(self, body):
        return await self._call("beat", body)

    def beat(self, body):  # only ever awaited through `asyncio.to_thread`: see SimLoop.run_in_executor
        raise RuntimeError("SimChannel.beat is awaited, never called")

    beat.__sim_async__ = "abeat"

    async def logs(self, body):
        return await self._call("logs", body)

    async def finished(self, body):
        await self.world.channel_point(self.who, "finished")
        return await self._call("finished", body)

    async def cleaned_up(self, body):
        return await self._call("cleaned_up", body)

    async def resolve(self, body):
        return await self._call("resolve", body)

    def close(self):
        pass


def sim_reporter_class():
    """The worker's `Reporter`, beating from a task instead of a thread:
    the same reports, fallbacks and cancel handling, on virtual time. A
    paused worker does not beat."""

    import obstore
    from obstore.exceptions import NotFoundError
    from solera_worker import reporting

    class SimReporter(reporting.Reporter):
        world: World | None = None

        def start(self):
            self._who = actor.get()
            self._task = asyncio.get_running_loop().create_task(self._arun())

        async def _arun(self):
            loop = asyncio.get_running_loop()
            while not self.ended:
                world = SimReporter.world
                worker = world.workers.get(self._who) if world is not None else None
                while worker is not None and loop.time() < worker.paused_until:
                    await asyncio.sleep(worker.paused_until - loop.time())
                self.seq += 1
                report = {"worker_id": self.worker_id, "seq": self.seq, **self.timeline.report()}
                if self.channel is not None:
                    try:
                        self.latch((await self.channel.abeat(report)).get("cancel"))
                        self.failures = 0
                    except Ended:
                        self.ended = True
                        self.on_ended()
                        return
                    except Exception:
                        self.failures += 1
                now = loop.time()
                if (self.channel is None or self.failures >= reporting.FALLBACK_AFTER) and (
                    now - self._fallback_at >= 2 * self.interval
                ):
                    self._fallback_at = now
                    with contextlib.suppress(Exception):
                        path = f"{self.base}{lifecycle.BEAT}"
                        await obstore.put_async(self.objects, path, json.dumps(report).encode())
                        try:
                            got = await obstore.get_async(self.objects, f"{self.base}{lifecycle.CONTROL}")
                            control = json.loads(bytes(await got.bytes_async()))
                        except (NotFoundError, FileNotFoundError):
                            control = {"state": lifecycle.ENDED}  # retention took it
                        if control["state"] == lifecycle.ENDED:
                            reason = self.cancel.reason if self.cancel else "user"
                            self.latch({"phase": "forced", "reason": reason, "since": 0})
                await asyncio.sleep(self.interval)

        async def stop(self):
            self._task.cancel()
            with contextlib.suppress(BaseException):
                await self._task

    return SimReporter


def sim_key_service_class():
    """The engine's key cache on the simulation loop rather than a thread
    of its own: the same cache and resolver, called in loop order."""

    from solera.keys.cache import EngineCache
    from solera.keys.io import ObjectIO
    from solera.keys.resolver import Resolver
    from solera_server.keyservice import KeyService

    class SimKeyService(KeyService):
        loop_of: SimLoop | None = None
        world: World | None = None

        def start(self):
            if self.loop is not None or self._stopped:
                return
            self.io = ObjectIO(self.objects)
            budget = SimKeyService.world.cache_budget if SimKeyService.world is not None else None
            disk, candidates = budget or (self.disk, self.candidates)
            self.cache = EngineCache(self.root, disk=disk, candidates=candidates, window=self.window)
            self.resolver = Resolver(self.cache, self.io, self.options, self.limits, pins=self)
            self.loop = SimKeyService.loop_of

        async def stop(self):
            self._stopped = True
            if self.loop is None:
                return
            self.loop = None
            await self._close_tasks()

        def _open(self, state):  # from a compaction's thread, while the loop waits for it
            if not self._running():
                return None
            return self.cache.open(state)

    return SimKeyService


# -- placement ------------------------------------------------------------------------------


class SimPlacement:
    """`Local`, in process: each launch starts the real worker as a task
    of its own actor; handles survive engines, as a process outlives the
    engine that started it."""

    max_concurrent = None

    def __init__(self, ctx, world: World):
        self.ctx, self.world = ctx, world

    async def launch(self, stage: dict) -> dict:
        self.world.launch(stage)
        return {"id": stage["attempt"]}

    async def wait(self, handle: dict, timeout: float) -> dict | None:
        workers = self.world.by_attempt.get(handle["id"])
        if not workers:
            return {"code": None, "reason": "lost", "meta": {}}
        task = workers[0].task  # the process the launch started
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if not done:
            return None
        if task.cancelled():
            return {"code": 137, "reason": "killed", "meta": {}}
        error = task.exception()
        if isinstance(error, Killed):
            return {"code": 137, "reason": "killed", "meta": {}}
        if error is not None:
            return {"code": 1, "reason": f"{type(error).__name__}: {error}", "meta": {}}
        return {"code": task.result(), "reason": None, "meta": {}}

    async def cancel(self, handle: dict) -> None:
        for worker in self.world.by_attempt.get(handle["id"], ()):
            await self.world.kill(worker.who)


# -- the world ------------------------------------------------------------------------------


@dataclass
class Clients:
    """What the clients were told: acknowledged requests only."""

    commits: list = field(default_factory=list)
    errors: int = 0


class World:
    """One simulated deployment over a fresh namespace under `root`."""

    def __init__(self, root: Path, seed: int, *, flush_interval=1.0, min_checkpoint=4096, key_options=None):
        self.root = root
        # Ready callbacks interleaved from the run's seed (SOLERA_SIM_ORDER=fifo: asyncio's order).
        order = None if os.environ.get("SOLERA_SIM_ORDER") == "fifo" else random.Random(seed ^ 0x0BDE5)
        self.loop = SimLoop(order)
        self.rng = random.Random(seed)
        self.plan = FaultPlan(rng=random.Random(seed ^ 0x5EED))
        self.objects = Objects(self.loop, self.plan)
        self.objects.hook = self._hook
        self.determinism = Determinism(self.loop, seed)
        self.url = (root / "state").as_uri()
        self.data_root = str((root / "data").resolve())
        self.flush_interval, self.min_checkpoint = flush_interval, min_checkpoint
        self.key_options = key_options
        self.slots: list[EngineSlot] = []
        self.slot: EngineSlot | None = None  # the engine clients and workers reach
        self.workers: dict[tuple, Worker] = {}
        self.by_attempt: dict[str, list[Worker]] = {}
        self.fates: deque[Fate] = deque()
        # The serving engine's next launch (a pool attempt's, with `pool`) stalls `seconds`
        # at `point`, then goes on or, with `crash`, the engine crashes there:
        # (point, seconds, pool, crash) or None.
        self.launch_fate: tuple[str, float, bool, bool] | None = None
        self.pace = Pace(self.now, None if os.environ.get("SOLERA_SIM_PACE") == "measure" else PACE)
        self.project_now: Callable | None = None  # the project the platform restarts an engine on
        self.restarting: asyncio.Task | None = None  # that restart, while it runs
        self.calm = False  # converging: no fate strikes any more, one already stalled included
        self.observing = False  # an invariant reads: no fate strikes meanwhile
        self._launching: dict[int, str] = {}  # engine -> how far its doomed launch has come
        self.exits: list[tuple[int, int]] = []  # (engine, code): a State that broke
        self.launched = 0
        self.steps = 0
        self._saved: list = []
        self.on_record: Callable | None = None  # events an engine applied, as it applies them
        self.pg = None  # a postgres.Ledger, when the project writes to Postgres
        self.pool_hosts = 1  # how many pool hosts poll (`start_pool_hosts`)
        self.cache_budget: tuple[int, int] | None = None  # (disk, candidates) of engines started from now on

    # -- running ------------------------------------------------------------------------

    def _install(self):
        from solera_server import engine as engine_mod
        from solera_server.state import State
        from solera_worker import worker as worker_mod

        reporter = sim_reporter_class()
        reporter.world = self
        keys = sim_key_service_class()
        keys.loop_of, keys.world = self.loop, self
        world = self

        def exit_(code):
            world._broken(code)

        record = State.record

        def recording(state, *events, lazy=False):
            record(state, *events, lazy=lazy)
            world._paced("events", len(events))
            if world.on_record is not None:
                world.on_record(events)

        tick = engine_mod.Engine.tick

        async def ticking(engine):
            world._paced("ticks")
            return await tick(engine)

        patches = []
        if self.pg is not None:
            from . import postgres

            patches += postgres.patches(self.pg, actor.get, self.now)
        patches += [
            (worker_mod, "Reporter", reporter),
            (engine_mod, "KeyService", keys),
            (State, "_exit", staticmethod(exit_)),
            (State, "record", recording),
            (engine_mod.Engine, "tick", ticking),
        ]
        for owner, name, value in patches:
            self._saved.append((owner, name, getattr(owner, name)))
            setattr(owner, name, value)

    def _uninstall(self):
        while self._saved:
            owner, name, value = self._saved.pop()
            setattr(owner, name, value)

    def run(self, coro, *, timeout: float | None = None):
        """Run `coro` (as the simulation itself) to its end on virtual time."""

        self.pace.input()  # a step: a new window
        self._install()
        quiet = [logging.getLogger(n) for n in ("solera_server", "solera_worker", "asyncio")]
        levels = [q.level for q in quiet]
        for q in quiet:  # engines that fail on purpose say so loudly; the trace has what matters
            q.setLevel(logging.CRITICAL + 1)
        try:
            with self.determinism, self.objects:
                if timeout is not None:
                    coro = asyncio.wait_for(coro, timeout)
                return self.loop.run_until_complete(coro)
        except RuntimeError:
            if self.pace.over is None:
                raise
            from .oracle import Violation

            raise Violation(self.pace.over) from None
        finally:
            for q, level in zip(quiet, levels, strict=True):
                q.setLevel(level)
            self._uninstall()

    def advance(self, seconds: float) -> None:
        self.run(asyncio.sleep(seconds))

    def now(self) -> float:
        return self.loop._now

    def wall(self) -> float:
        return EPOCH + self.loop._now

    # -- actors -------------------------------------------------------------------------

    def tasks_of(self, who: tuple) -> list[asyncio.Task]:
        return [
            t
            for t in _in_creation_order(asyncio.all_tasks(self.loop))
            if t.get_context().get(actor) == who and not t.done()
        ]

    async def kill(self, who: tuple) -> None:
        """SIGKILL: the actor makes no request from now on, and its tasks end.
        An actor killing itself (a fate striking) does not wait for its own
        tasks: they may be waiting for the very task that strikes."""

        self.objects.dead.add(who)
        tasks = self.tasks_of(who)
        me = asyncio.current_task()
        for task in tasks:
            if task is not me:
                task.cancel()
        if me in tasks:
            return
        await asyncio.gather(*tasks, return_exceptions=True)

    def _spawn(self, who: tuple, coro) -> asyncio.Task:
        ctx = contextvars.copy_context()
        ctx.run(actor.set, who)
        return ctx.run(self.loop.create_task, coro)

    def _broken(self, code: int) -> None:
        who = actor.get()
        n = who[1] if who and who[0] == "engine" else -1
        self.exits.append((n, code))
        if who is not None:
            self.objects.dead.add(who)

    # -- engines ------------------------------------------------------------------------

    @property
    def engine(self):
        return self.slot.engine if self.slot is not None and not self.slot.dead else None

    def live_slots(self) -> list[EngineSlot]:
        return [s for s in self.slots if not s.dead and s.engine is not None]

    async def start_engine(self, project) -> EngineSlot:
        """A new engine process: open the namespace (fencing whoever wrote
        before), register `project`, start. Clients and workers reach it
        from here on; an older live engine is now a zombie."""

        self.pace.input()

        from solera_server.engine import Engine
        from solera_server.state import State

        n = len(self.slots)
        slot = EngineSlot(n, contextvars.copy_context())
        slot.ctx.run(actor.set, ("engine", n))
        slot.project = project
        self.slots.append(slot)

        async def boot():
            state = await State.open(
                self.url,
                "sim",
                clock=self.wall,
                flush_interval=self.flush_interval,
                min_checkpoint=self.min_checkpoint,
            )
            slot.state = state
            kw = {"key_options": self.key_options} if self.key_options is not None else {}
            engine = Engine(
                state,
                project.manifest,
                placements={"Local": lambda spec, c: SimPlacement(c, self)},
                project=project.manifest["name"],
                clock=self.wall,
                resolve_cache=str(self.root / "key-cache"),
                **kw,
            )
            slot.engine = engine
            await engine.initialize()
            await state.durable()
            await engine.start()

        task = slot.ctx.run(self.loop.create_task, boot())
        try:
            await task
        except BaseException:
            await self.kill(("engine", n))
            slot.dead = True
            raise
        self.slot = slot
        return slot

    async def crash(self, slot: EngineSlot | None = None) -> None:
        """The engine process dies at once: nothing buffered is written."""

        slot = slot or self.slot
        if slot is None or slot.dead:
            return
        slot.dead = True
        await self.kill(("engine", slot.n))

    async def stop(self, slot: EngineSlot | None = None) -> None:
        """A clean shutdown: the engine stops, its journal flushed and checkpointed."""

        slot = slot or self.slot
        if slot is None or slot.dead:
            return
        slot.stopping = True  # what it still writes while it stops is no launch the fate takes

        async def down():
            await slot.engine.stop()
            await slot.state.close()

        task = slot.ctx.run(self.loop.create_task, down())
        with contextlib.suppress(Exception):
            await task
        await self.crash(slot)  # whatever is left of the process

    async def request(self, call: Callable, slot: EngineSlot | None = None):
        """An API request to the serving engine, as its middleware runs it:
        the handler, then — if it recorded anything — durability, else 503."""

        from solera_server.state import Unavailable

        self.pace.input()

        slot = slot or self.slot
        if slot is None or slot.dead or slot.engine is None:
            raise Unreachable("no engine is serving")
        engine = slot.engine

        async def handle():
            state = engine.state
            recorded = state.recorded
            value = await call(engine)
            if state.recorded != recorded:
                await state.durable()
            return value

        task = slot.ctx.run(self.loop.create_task, handle())
        try:
            return await asyncio.shield(task)
        except Unavailable:
            raise
        except Killed as error:
            raise Unreachable("the engine died while answering") from error
        except asyncio.CancelledError:
            if task.cancelled() and not asyncio.current_task().cancelling():
                raise Unreachable("the engine died while answering") from None
            raise

    # -- workers ------------------------------------------------------------------------

    def launch(
        self, stage: dict, *, fate: Fate | None = None, twin: bool = False, pool: bool = False
    ) -> Worker:
        """A worker for `stage`: launched by a placement, or (`pool`) one a
        pool host took from discovery."""

        from solera_worker.worker import run_attempt

        attempt = stage["attempt"]
        if not twin:
            fate = self.fates.popleft() if self.fates else None
        self.launched += 1
        who = ("worker", attempt, self.launched)
        project = self.slot.project if self.slot is not None else None
        channel = SimChannel(self, attempt, who, muted=fate is not None and fate.kind == "mute")
        coro = run_attempt(stage["objects"], attempt, project, run=stage["run"], channel=channel, pool=pool)
        task = self._spawn(who, coro)
        worker = Worker(who, attempt, stage["run"], task, fate if not twin else None)
        self.workers[who] = worker
        self.by_attempt.setdefault(attempt, []).append(worker)
        if fate is not None and fate.kind == "twice" and not twin:

            async def second():
                await asyncio.sleep(fate.seconds)
                self.launch(stage, twin=True)

            self._spawn(None, second())
        return worker

    def start_pool_hosts(self, pool: str, count: int, every: float = 10.0) -> None:
        """`count` hosts of `pool` (`solera worker pool`): each polls the
        serving engine for work every `every` seconds while it is one of the
        first `pool_hosts`, and starts a worker for each attempt offered.
        Discovery is a hint: hosts offered the same attempt race for its claim."""

        async def host(n: int):
            mine: dict[str, Worker] = {}  # the worker this host started for each attempt
            while True:
                await asyncio.sleep(every)
                if n >= self.pool_hosts:
                    continue
                try:
                    stages = await self.request(lambda e: e.pool_work(pool, {"cpu": 4.0}, f"host-{n}", 0.0))
                except (Exception, Killed):
                    continue
                for stage in stages:  # still offered: never claimed, or its worker died first
                    worker = mine.get(stage["attempt"])
                    if worker is None or worker.task.done():
                        twin = stage["attempt"] in self.by_attempt
                        mine[stage["attempt"]] = self.launch(stage, twin=twin, pool=True)

        for n in range(count):
            self._spawn(("pool", n), host(n))

    async def _strike(self, worker: Worker) -> None:
        fate = worker.fate
        worker.fired = True
        if fate.kind == "die":
            await self.kill(worker.who)
            raise Killed(f"{worker.who} died at {fate}")
        if fate.kind == "pause":
            worker.paused_until = self.loop._now + fate.seconds
            await asyncio.sleep(fate.seconds)

    def _paced(self, kind: str, n: int = 1) -> None:
        if self.pace.bump(kind, n):
            self.loop.stop()  # the step ends here, with `pace.over`: the loop may never yield

    async def _hook(self, who, kind: str, full: str, when: str, data: bytes | None = None) -> None:
        if who is not None and who[0] == "engine" and when == "before":
            self._paced("requests")
        if who is not None and who[0] == "engine" and when == "before" and self.launch_fate is not None:
            await self._launch_point(who, kind, full, data)
        if who is None or who[0] != "worker":
            return
        worker = self.workers.get(who)
        if worker is None or worker.fate is None or worker.fired:
            return
        fate = worker.fate
        if (
            fate.kind in ("die", "pause")
            and fate.when == when
            and point_of(full, kind, self.data_root, data) == fate.point
        ):
            await self._strike(worker)

    async def _launch_point(self, who: tuple, kind: str, full: str, data: bytes | None) -> None:
        """A launch writes its spec, then its control file, then records
        `AttemptLaunched` and waits for the journal flush that makes it
        durable. The engine stalls at the chosen one of the three (pool
        hosts and workers go on meanwhile), then goes on or crashes there."""

        point, seconds, pool, crash = self.launch_fate
        n, reached = who[1], self._launching.get(who[1])
        if self.slot is None or self.slot.n != n or self.slot.stopping:  # the serving engine's, running
            return
        if kind == "create" and full.endswith(".spec"):
            if pool and b'"Pool"' not in (data or b""):
                return
            here = "spec"
        elif reached == "spec" and kind == "create" and full.endswith(".control"):
            here = "control"
        elif reached == "control" and kind == "swap" and full.endswith("journal.json"):
            here = "flush"
        else:
            return
        self._launching[n] = here
        if here != point or self.observing:  # observed: it waits for a launch in a step
            return
        self.launch_fate, self._launching = None, {}
        await asyncio.sleep(seconds)
        if not crash or self.calm or self.observing or self.slots[n].stopping:
            return  # it goes on: stalled only, or converging, observed, stopping
        serving = self.slot is self.slots[n]  # not replaced while it stalled
        await self.crash(self.slots[n])
        if serving and self.project_now is not None:  # the platform starts another at once
            self.restarting = self._spawn(None, self._restart(self.project_now()))
        raise Killed(f"{who} crashed mid-launch, at its {point}")

    async def _restart(self, project) -> None:
        with contextlib.suppress(Exception, Killed):  # a start that fails is retried at the next step
            await self.start_engine(project)

    async def channel_point(self, who: tuple, point: str) -> None:
        worker = self.workers.get(who)
        if worker is None or worker.fate is None or worker.fired:
            return
        fate = worker.fate
        if fate.kind in ("die", "pause") and fate.point == point:
            await self._strike(worker)

    def cache_trouble(self, kind: str) -> list[str]:
        """Something outside the engine at its key cache's files, under it:
        `wipe` deletes them (a temporary-files cleaner), `corrupt` flips a
        byte in the middle of each (bit rot). Returns the files touched."""

        root = self.root / "key-cache"
        touched = []
        for path in sorted(root.glob("*")) if root.exists() else ():
            if kind == "wipe":
                path.unlink(missing_ok=True)
            else:
                with open(path, "r+b") as f:
                    f.seek(path.stat().st_size // 2)
                    byte = f.read(1)
                    f.seek(-len(byte), 1)
                    f.write(bytes([byte[0] ^ 0xFF]) if byte else b"")
            touched.append(path.name)
        return touched

    def live_workers(self) -> list[Worker]:
        return [w for w in self.workers.values() if not w.task.done()]

    # -- teardown -----------------------------------------------------------------------

    def close(self) -> None:
        async def down():
            for slot in self.slots:
                if not slot.dead:
                    await self.crash(slot)
            for worker in list(self.workers.values()):
                if not worker.task.done():
                    await self.kill(worker.who)
            rest = [
                t for t in _in_creation_order(asyncio.all_tasks(self.loop)) if t is not asyncio.current_task()
            ]
            for t in rest:
                t.cancel()
            await asyncio.gather(*rest, return_exceptions=True)

        with contextlib.suppress(BaseException):
            self.run(down())
        self.loop.close()
