"""Attempts on a placement the test plays (docs/lifecycle.md): `Remote`
launches and waits, the test writes the worker's objects — its control
file swaps (`own`, `as_worker`, `finish_as_worker`) — and reads them back
(`fence`). `Gated` and `LiveStore` are fenced stores whose gates and
writes the tests watch."""

import asyncio

from solera import lifecycle
from solera.executors import Executor
from solera.objects import swap
from solera.sdk import Project, Ref, asset
from solera.stores import FileStore, KeyedWrite, Keys, Patch, Written
from solera_server.engine import Engine
from solera_server.executors.inline import InlinePlacement


class Fake(Executor):
    kind = "Fake"


class Gated(FileStore):
    """FileStore's layout, declared `fenced`: its attempts take a gate and
    intents, so tests can watch them (docs/lifecycle.md §9.6)."""

    writes = "fenced"

    async def acquire(self, context, prior=None):
        pass

    def keys(self, ref, among=None):
        raise AssertionError("no test of Gated repairs a slice")


class Remote:
    """A placement whose worker the test plays by writing its objects."""

    max_concurrent = None
    launches: list[str] = []

    def __init__(self, ctx):
        self.ctx = ctx

    async def launch(self, stage):
        self.launches.append(stage["attempt"])
        return {"id": stage["attempt"], "run": stage["run"]}

    async def wait(self, handle, timeout):
        if await self.ctx.state.attempt_finished(handle["run"], handle["id"]):
            return {"code": 0, "reason": None, "meta": {}}  # the worker exits once it wrote its result
        await asyncio.sleep(min(timeout, 0.05))
        return None

    async def cancel(self, handle):
        return None


def engine_for(state, project, placement="remote", worker=None, **kw):
    if placement == "remote":
        placements = {"Fake": lambda s, c: (worker or Remote)(c)}
    else:
        placements = {"Local": lambda s, c: InlinePlacement(c, project)}
    kw.setdefault("heartbeat_seconds", 0.3)
    return Engine(state, project.manifest, placements=placements, clock=state.clock, eval_interval=0.02, **kw)


async def until(engine, done, timeout=60.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not done():
        assert asyncio.get_running_loop().time() < deadline, "timed out"
        await engine.tick()
        await asyncio.sleep(0.02)


async def as_worker(state, run_id, attempt, to, worker_id="w", **fields):
    """Swap the attempt's control file to `to`, as worker `worker_id` would
    (docs/lifecycle.md §2.4). Raises `Conflict` where a worker is refused."""

    found = await lifecycle.read_control(state.objects, run_id, attempt)
    body = lifecycle.control(to, worker_id=worker_id, **fields)
    path = f"{state.attempt_path(run_id, attempt)}{lifecycle.CONTROL}"
    await swap(state.objects, path, body, found[1] if found is not None else None)


async def own(state, run_id, attempt, worker_id="w"):
    """A worker's first write: the attempt's control file, `open` → `owned`."""

    await as_worker(state, run_id, attempt, lifecycle.OWNED, worker_id)


async def finish_as_worker(state, run_id, attempt, output, worker_id="w", **extra):
    """What a worker does (docs/lifecycle.md §3): own the attempt, take the
    gate, write, then seal its result (with `extra` fields)."""

    found = await lifecycle.read_control(state.objects, run_id, attempt)
    if found[0]["state"] == lifecycle.OPEN:
        await own(state, run_id, attempt, worker_id)
    await as_worker(state, run_id, attempt, lifecycle.WRITING, worker_id, intents={})
    ref = {
        "output": output,
        "store": "default",
        "handle": {"path": "x"},
        "version": "v1",
        "partition": "",
    }
    result = {
        "worker_id": worker_id,
        "status": "succeeded",
        "write": "complete",
        "outputs": {output: {"ref": {**ref, "meta": {}}}},
        **extra,
    }
    await as_worker(state, run_id, attempt, lifecycle.SEALED, worker_id, result=result)


async def fence(state, run_id, attempt):
    """The attempt's control file as `(state, write evidence)`, or `None`."""

    found = await lifecycle.read_control(state.objects, run_id, attempt)
    if found is None:
        return None
    body = found[0]
    return body["state"], body.get("write", (body.get("result") or {}).get("write"))


async def launched(engine, targets):
    Remote.launches.clear()
    run = await engine.submit(targets)
    await until(engine, lambda: Remote.launches)
    attempt = Remote.launches[0]
    await until(engine, lambda: engine.m.claimed(attempt) and engine.m.claimed(attempt).get("launched"))
    return run, attempt


@asset(executor=Fake("fake")())
def remote():
    return [{"ok": True}]


REMOTE = Project(assets=[remote], executors=[Fake("fake")], default_store=Gated())


class LiveStore(FileStore):
    """A row store read in place, like a database table: what a dead attempt
    wrote is visible. `die` makes the next write land its first n rows, then
    kills the worker."""

    writes = "fenced"

    def __init__(self):
        super().__init__()
        self.rows: dict[str, dict] = {}
        self.die: int | None = None

    async def acquire(self, context, prior=None):
        pass

    def can_load(self, t, selection):
        return True

    async def store(self, write, prior, context):
        if isinstance(write, KeyedWrite):  # written as the producer returned it
            write = write.value
        patch = isinstance(write, Patch)
        rows = write.rows if patch else write
        if hasattr(rows, "to_pylist"):  # Arrow
            rows = rows.to_pylist()
        if not patch:
            self.rows.clear()
        for n, row in enumerate(rows):
            if self.die is not None and n == self.die:
                self.die = None
                raise asyncio.CancelledError  # the worker dies mid-write
            self.rows[row["id"]] = dict(row)
        for key in write.remove if patch else ():
            self.rows.pop(key, None)
        return Written(Ref(context.output.name, "live", {}, context.partition))

    def keys(self, ref, among=None):
        """The keys it holds, sorted by their bytes: what a repair asks."""

        yield sorted((k for k in self.rows if among is None or k in among), key=str.encode)

    async def load(self, ref, t, selection):
        if isinstance(selection, Keys):
            return [dict(r) for k, r in self.rows.items() if k in selection.generations]
        return [dict(r) for r in self.rows.values()]
