"""The contract's harness: one Solera namespace, driven from where a user
stands (docs/behaviors.md). A test declares a project with the SDK, deploys
it, and then does only what a user can: call the engine's HTTP API, commit
sources, run, cancel, restart or replace the engine, let time pass, and read
outputs through their stores. It never reads the engine's state.

`Solera(root, path)` serves the namespace on one tracking path: `"today"`,
the engine as it is, or `"core"`, the new core (docs/tracking-design.md
§15). `engine_options` is the one place that tells them apart."""

from __future__ import annotations

import asyncio
import secrets
import time
from pathlib import Path

import httpx
import pytest
from solera.sdk import Loaded, Project, Ref
from solera.stores import FileStore, Keys, Patch

PATHS = ("today", "core")


def engine_options(path: str) -> dict:
    """The `Engine` arguments that select a tracking path. B5 replaces the
    skip with the constructor argument it adds (build plan, rule 4)."""

    if path == "today":
        return {}
    pytest.skip("the new core's path is not built yet (build plan B5)")


class Refused(AssertionError):
    """A request the API refused: its status and detail."""

    def __init__(self, status: int, detail):
        super().__init__(f"{status}: {detail}")
        self.status, self.detail = status, detail


class Solera:
    """A namespace under `root`, served by one engine at a time."""

    def __init__(self, root: Path, path: str = "today"):
        self.root, self.path = Path(root), path
        self.offset = 0.0  # seconds `advance` moved the clock past the wall
        self.state = self.engine = self.project = self.client = None
        self.engine_kw: dict = {}

    def clock(self) -> float:
        return time.time() + self.offset

    def advance(self, seconds: float) -> None:
        """Time passes: every clock the namespace reads moves on."""

        self.offset += seconds

    # -- serving ---------------------------------------------------------------

    async def deploy(self, project: Project, **engine_kw) -> None:
        """Serve `project`: the first deploy, or a new one in place of the
        running engine. Attempts in flight keep running; the next engine
        adopts them. `engine_kw` are serve settings (`cancel_grace=`, …)."""

        if self.state is None:
            await self._open_state()
        await self._stop_engine()
        self.project, self.engine_kw = project, engine_kw
        await self._serve()

    async def restart(self) -> None:
        """The engine process restarts on the same deploy and state."""

        await self._stop_engine()
        await self.state.close()
        await self._open_state()
        await self._serve()

    async def _open_state(self) -> None:
        from solera_server.state import State

        self.state = await State.open(
            (self.root / "state").as_uri(), "contract", clock=self.clock, flush_interval=0.001
        )

    async def _serve(self) -> None:
        from solera_server.api import create_app
        from solera_server.engine import Engine
        from solera_server.executors.inline import InlinePlacement

        project, kw = self.project, dict(self.engine_kw)
        kw.setdefault("eval_interval", 0.05)
        kw.setdefault("clock", self.clock)
        kw.setdefault("placements", {"Local": lambda s, c: InlinePlacement(c, project)})
        self.engine = Engine(self.state, project.manifest, **kw, **engine_options(self.path))
        await self.engine.initialize()
        token = secrets.token_hex(16)
        app = create_app(engine=self.engine, token=token)
        app.state.engine = self.engine
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://solera",
            headers={"Authorization": f"Bearer {token}"},
        )

    async def _stop_engine(self) -> None:
        if self.client is not None:
            await self.client.aclose()
            self.client = None
        if self.engine is not None:
            await self.engine.stop()
            self.engine = None

    async def close(self) -> None:
        await self._stop_engine()
        if self.state is not None:
            await self.state.close()
            self.state = None

    # -- the API ---------------------------------------------------------------

    async def request(self, method: str, path: str, *, ok: bool = True, **kw):
        """`method` on `path`, relative to the project's routes unless it
        starts with `/api/`. A refusal raises `Refused`; `ok=False` returns
        the response as it came instead."""

        url = path if path.startswith("/api/") else f"/api/projects/{self.project.manifest['name']}{path}"
        response = await self.client.request(method, url, **kw)
        if not ok:
            return response
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail")
            except ValueError:
                detail = response.text
            raise Refused(response.status_code, detail)
        return response.json() if response.content else None

    async def get(self, path: str, **params):
        return await self.request("GET", path, params=params)

    async def post(self, path: str, body=None, **kw):
        return await self.request("POST", path, json=body, **kw)

    # -- runs ------------------------------------------------------------------

    async def submit(self, targets, **fields) -> dict:
        """Ask for a run (`POST /runs`): `partitions=`, `mode=`, `keys=`,
        `upstream=`, `config=`, `tags=`, `by=`."""

        targets = [targets] if isinstance(targets, str) else list(targets)
        return await self.post("/runs", {"targets": targets, **fields})

    async def wait(self, run, timeout: float = 30) -> dict:
        """Time passes until `run` settles: its detail, as `GET /runs/{id}`
        gives it. The engine works meanwhile, as a served one does."""

        run_id = run if isinstance(run, str) else run["id"]
        await self.engine.run_until(run_id, timeout)
        return await self.get(f"/runs/{run_id}")

    async def run(self, targets, *, expect: str | None = "succeeded", timeout: float = 30, **fields) -> dict:
        """Ask for a run and wait for it to settle as `expect` (None: any)."""

        detail = await self.wait(await self.submit(targets, **fields), timeout)
        if expect is not None:
            assert detail["request"]["status"] == expect, detail["request"]
        return detail

    async def settle(self, seconds: float = 0.5) -> None:
        """The engine works for `seconds` of wall time with nothing asked:
        automations, retries and cleanups do what they would."""

        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            await self.engine.tick()
            await asyncio.sleep(0.02)

    async def cancel(self, run_id: str) -> dict:
        return await self.post(f"/runs/{run_id}/cancel")

    async def runs(self, **filters) -> list[dict]:
        return (await self.get("/runs", **filters))["runs"]

    # -- sources ---------------------------------------------------------------

    async def commit_source(self, name: str, **body) -> dict:
        """A source commit (`POST /sources/{name}/commit`): `version=`,
        `keys=`, `upsert=`, `remove=`."""

        return await self.post(f"/sources/{name}/commit", body)

    # -- what a user reads -----------------------------------------------------

    async def stale(self, asset: str, partition: str = "") -> list:
        """Why the partition is stale: its reasons, empty when it is fresh."""

        return (await self.get(f"/assets/{asset}/stale-keys", partition=partition))["reasons"]

    async def stale_keys(self, asset: str, partition: str = "") -> dict[str, list]:
        """Every stale key of the partition, with the reasons that are its own."""

        out, after = {}, None
        while True:
            params = {"partition": partition} | ({"after": after} if after is not None else {})
            page = await self.get(f"/assets/{asset}/stale-keys", **params)
            out.update((row["key"], row["reasons"]) for row in page["keys"])
            if (after := page.get("next")) is None:
                return out

    async def status(self, asset: str, partition: str | None = None) -> str:
        """A partition's status (STA-1); unpartitioned, the asset's one."""

        if partition is not None:
            rows = (await self.get(f"/partitions/{asset}"))["partitions"]
            return next(r["status"] for r in rows if r["partition"] == partition)
        rollup = (await self.get("/assets:status"))["assets"][asset]["partitions"]
        return next(s for s, n in rollup.items() if s != "total" and n)

    async def keys(self, output: str, partition: str = "") -> dict:
        """An output's live keys, each at its version; empty before a commit."""

        out, after = {}, None
        while True:
            params = {"partition": partition} | ({"after": after} if after is not None else {})
            response = await self.request("GET", f"/outputs/{output}/keys", params=params, ok=False)
            if response.status_code == 404:
                return {}
            page = response.json()
            out.update(page["keys"])
            if (after := page.get("next")) is None:
                return out

    async def head(self, output: str, partition: str = "") -> dict | None:
        heads = (await self.get(f"/outputs/{output}/heads"))["heads"]
        return next((h for h in heads if h["partition"] == partition), None)

    async def read(self, output: str, partition: str = "", typ=None):
        """An output's content at its head, read through its store as a
        downstream user would: a keyed one as its rows at the versions its
        keys list. None before its first commit."""

        head = await self.head(output, partition)
        if head is None:
            return None
        ref, store = Ref.from_json(head["ref"]), self.project.stores[head["ref"]["store"]]
        if self.project.manifest["outputs"][output]["key"] is None:
            return await store.load(ref, typ or object, None)
        return await store.load(ref, typ or list[dict], Keys(await self.keys(output, partition)))

    async def rows(self, output: str, partition: str = "", column: str = "v") -> dict:
        """A keyed output as `{key: row[column]}`."""

        key = self.project.manifest["outputs"][output]["key"]
        return {str(r[key]): r.get(column) for r in await self.read(output, partition) or []}

    # -- upkeep, waiting, a sensor host ------------------------------------------

    async def upkeep(self) -> None:
        """Storage upkeep does one pass, as a served engine's does every
        second: retention among it."""

        await self.engine.upkeep.tick()

    async def until(self, condition, timeout: float = 10) -> None:
        """The engine works, as in `settle`, until `condition()` (awaited when
        it returns an awaitable) holds; fails past `timeout` seconds."""

        import inspect

        deadline = time.monotonic() + timeout
        while True:
            held = condition()
            if inspect.isawaitable(held):
                held = await held
            if held:
                return
            assert time.monotonic() < deadline, f"still not so after {timeout} s"
            await self.settle(0.05)

    async def quiet(self, timeout: float = 10) -> None:
        """The engine works until no run is queued or running."""

        async def idle():
            return not await self.runs(status=["queued", "running"])

        await self.until(idle, timeout)

    async def sensors(self, ticks: int, executor: str = "local") -> None:
        """A sensor host of `executor` runs `ticks` ticks as a sensor worker
        does: it asks for due ones, runs each body, and posts its outcome."""

        from solera_worker.channel import SensorChannel
        from solera_worker.sensors import run_sensor_host

        client = self.client

        class Transport:
            async def asend(self, method, path, **kw):
                return await client.request(method, path, **kw)

        channel = SensorChannel(Transport(), self.project.manifest["name"])
        await run_sensor_host(channel, self.project, executor, max_ticks=ticks, host="contract")

    # -- key outcomes and commits ------------------------------------------------

    async def retry_keys(self, asset: str, *classes: str, partition: str | None = None) -> list[dict]:
        """`solera keys retry` (`POST /assets/{name}/keys:retry`): the runs it
        started, each settled, as `GET /runs/{id}` gives them."""

        before = {r["id"] for r in await self.runs(limit=500)}
        await self.post(f"/assets/{asset}/keys:retry", {"classes": list(classes), "partition": partition})
        started = [r["id"] for r in await self.runs(limit=500) if r["id"] not in before]
        return [await self.wait(run_id) for run_id in started]

    async def outcomes(self, asset: str, *outcomes: str, partition: str | None = None) -> dict[str, dict]:
        """A per-key asset's key outcomes (`GET /assets/{name}/outcomes`),
        every page, by key: of `outcomes` only, if named."""

        out, after = {}, None
        while True:
            params = {"outcome": list(outcomes)}
            params |= {"partition": partition} if partition is not None else {}
            params |= {"after": after} if after is not None else {}
            page = await self.get(f"/assets/{asset}/outcomes", **params)
            out.update((row["key"], row) for row in page["keys"])
            if (after := page.get("next")) is None:
                return out

    async def explain(self, asset: str, key: str, **params) -> dict:
        """Why `key` is or is not in `asset`'s output (`GET /assets/{name}/explain`)."""

        return await self.get(f"/assets/{asset}/explain", key=key, **params)

    async def commits(self, asset: str, run_id: str | None = None) -> list[dict]:
        """The commits history logs for `asset`'s outputs, oldest first: those
        of `run_id` only, if named."""

        rows = (await self.get(f"/assets/{asset}/history", limit=5000))["commits"]
        return sorted((r for r in rows if run_id in (None, r["run"])), key=lambda r: r["generation"])

    # -- workers that boot slowly or die, outages --------------------------------

    def workers(self, *, boot: float | None = 0.0) -> dict:
        """A serve setting, `deploy(project, placements=solera.workers(…))`:
        the `Local` executor's workers, each running the real worker but
        booting `boot` seconds after its launch, as an image pull or a queue
        makes it; with None it never boots. `kill_workers` kills them."""

        from solera_server.executors.inline import InlinePlacement

        running = self.__dict__.setdefault("running_workers", set())

        class Booting(InlinePlacement):
            def _work(self, stage):
                return self._boot(stage)

            async def _boot(self, stage):
                running.add(asyncio.current_task())
                try:
                    await (asyncio.Event().wait() if boot is None else asyncio.sleep(boot))
                    return await InlinePlacement._work(self, stage)
                finally:
                    running.discard(asyncio.current_task())

        return {"Local": lambda s, c: Booting(c, self.project)}

    async def kill_workers(self) -> int:
        """Every worker `workers` launched that is running dies at once, as a
        killed process does: it publishes no result. How many died."""

        killed = list(self.__dict__.get("running_workers", ()))
        for task in killed:
            task.cancel()
        await asyncio.gather(*killed, return_exceptions=True)
        return len(killed)

    async def outage(self, seconds: float) -> None:
        """The engine is down for `seconds`, then served again on the same
        deploy and state."""

        await self._stop_engine()
        await self.state.close()
        self.advance(seconds)
        await self._open_state()
        await self._serve()

    # -- pool workers, sensor hosts and reloads a test plays ---------------------

    def _worker_transport(self):
        """The engine's routes as a worker in this process reaches them."""

        from solera_server.api import create_app
        from solera_worker.channel import LocalTransport

        token = self.client.headers["Authorization"].removeprefix("Bearer ")
        app = create_app(engine=self.engine, token=token)
        app.state.engine = self.engine
        return LocalTransport(app, token)

    async def work(self, pool: str, project: Project, timeout: float = 10) -> int:
        """A pool worker whose code is `project` (its deploy, which need not
        be the served one) takes the oldest attempt waiting on `pool` and
        runs it, as `solera worker pool` does in each attempt's process:
        its exit code."""

        from solera_worker.channel import AttemptChannel
        from solera_worker.worker import run_attempt

        deadline = time.monotonic() + timeout
        while not (stages := (await self.get(f"/pools/{pool}/work", wait=0, host="contract"))["work"]):
            assert time.monotonic() < deadline, f"no attempt waits on pool {pool!r}"
            await self.settle(0.02)
        stage = stages[0]
        channel = AttemptChannel(self._worker_transport(), self.project.manifest["name"], stage["attempt"])
        return await run_attempt(
            stage["objects"], stage["attempt"], project, run=stage["run"], channel=channel, pool=True
        )

    async def sensor_host(self, project: Project, executor: str = "local", **kw) -> int:
        """A sensor host whose code is `project` serves `executor`'s sensors
        until it stops, as `solera worker sensors` does: its exit code.
        `kw` are the host's settings (`stale_wait=`, `max_ticks=`)."""

        from solera_worker.channel import SensorChannel
        from solera_worker.sensors import run_sensor_host

        channel = SensorChannel(self._worker_transport(), self.project.manifest["name"])
        try:
            return await run_sensor_host(channel, project, executor, host="contract", **kw)
        finally:
            await channel.close()

    def reloading(self, entry: str, *, poll: float = 0.05, quiet: float = 0.3):
        """`solera serve --reload` of the project at `entry` (`path.py:name`),
        as `async with`: it is deployed, then served again in place each
        time the code it runs changes, until the block ends."""

        import contextlib

        from solera_server.reloading import reload
        from solera_worker.worker import load_project

        @contextlib.asynccontextmanager
        async def served():
            await self.deploy(project := load_project(entry))
            watcher = asyncio.create_task(reload(self.engine, entry, project.files(), poll=poll, quiet=quiet))
            try:
                yield project
            finally:
                watcher.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await watcher

        return served()

    # -- takeovers, workers and processes a test plays ---------------------------
    # Serving, outages and takeovers, and the workers a test plays itself.

    async def start(self) -> None:
        """The engine serves on its own from here, as `solera serve` runs it:
        its loop, upkeep and history work in the background until it stops."""

        await self.engine.start()

    async def down(self) -> None:
        """The engine process goes away (an outage, a crash): workers keep
        running; `restart()` serves the namespace again."""

        await self._stop_engine()

    def successor(self) -> Solera:
        """Another engine process for this namespace, on the same clock (a
        rolling deploy's new one): its `deploy()` takes the namespace over
        while this one still runs. The caller closes it."""

        other = Solera(self.root, self.path)
        other.clock = self.clock
        return other

    async def offered(self, pool: str, timeout: float = 30, **capacity) -> list[dict]:
        """What a pool worker polling `pool` is offered (`GET /pools/{pool}/work`),
        asked until something is: a launch is offered once it is durable."""

        deadline = time.monotonic() + timeout
        while True:
            work = (await self.get(f"/pools/{pool}/work", wait=0, host="contract", **capacity))["work"]
            if work or time.monotonic() > deadline:
                return work
            await self.engine.tick()
            await asyncio.sleep(0.02)

    async def worker(self, stage: dict, project: Project | None = None, *, pool: bool = True) -> int:
        """A worker runs the attempt `stage` names, as `solera_worker` runs one
        (`pool`: a pool host's; else one started for that attempt alone), on
        the attempt's routes while an engine serves and through the object
        store alone while none does: its exit code."""

        from solera import lifecycle
        from solera_server.api import local_transport
        from solera_worker.channel import AttemptChannel
        from solera_worker.worker import run_attempt

        attempt, channel = stage["attempt"], None
        if self.engine is not None:
            token = lifecycle.token(self.engine.secret, attempt)
            channel = AttemptChannel(
                local_transport(self.engine, token), self.project.manifest["name"], attempt
            )
        return await run_attempt(
            stage["objects"],
            attempt,
            project or self.project,
            run=stage["run"],
            channel=channel,
            pool=pool,
            loser_poll=0.05,  # a duplicate looks for the owner's end this often
        )

    async def tamper(self, run: str, attempt: str, change) -> None:
        """A worker gone wrong: the control file it writes for `attempt` of
        `run` becomes `change(body)`, its body as it stands (None when there
        is none), raw bytes."""

        import json

        import obstore
        from obstore.exceptions import NotFoundError
        from solera import lifecycle

        store = obstore.store.from_url(self.state.objects_url)
        path = f"{lifecycle.base(run, attempt)}{lifecycle.CONTROL}"
        try:
            body = json.loads(bytes(await (await obstore.get_async(store, path)).bytes_async()))
        except NotFoundError:
            body = None
        await obstore.put_async(store, path, change(body))

    async def sensor_ticks(self, executor: str = "local", wait: float = 0) -> list[dict]:
        """A sensor host's ask for ticks (`GET /sensors/next`): the ticks due
        on `executor`'s hosts, each with its sensor's cursor, waiting up to
        `wait` seconds for one."""

        deploy = (await self.get("/manifest"))["deploy"]
        params = {"executor": executor, "deploy": deploy, "host": "contract", "slots": 8, "wait": wait}
        return (await self.get("/sensors/next", **params))["ticks"]

    async def sensor_post(self, tick: dict, outcome) -> dict:
        """A sensor host posts what `tick` found (a `Tick`)."""

        return await self.post(f"/sensors/{tick['sensor']}/ticks/{tick['tick']}", outcome.to_json())

    async def serve_process(self, **env):
        """`solera serve` as its own process, as an operator starts it, on a
        state beside this namespace's (`env` adds to its environment): the
        process, once it listens."""

        import os
        import socket
        import subprocess
        import sys

        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        argv = ["--state-url", (self.root / "served").as_uri(), "serve", "--insecure", "--port", str(port)]
        process = subprocess.Popen(
            [sys.executable, "-c", "from solera_server.cli import main; main()", *argv],
            env={
                **os.environ,
                "SOLERA_SELFTEST": "0",
                "SOLERA_DATA_URL": (self.root / "data").as_uri(),
                **env,
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        deadline = time.monotonic() + 120
        while True:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", port)) == 0:
                    return process
            if process.poll() is not None or time.monotonic() > deadline:
                process.kill()
                raise AssertionError(f"serve never listened: {process.communicate()[1][-2000:]}")
            await asyncio.sleep(0.1)

    async def cli(self, *argv: str):
        """The `solera` CLI on this namespace, as an operator runs it: its exit
        code, stdout and stderr."""

        import os
        import subprocess
        import sys

        command = [sys.executable, "-c", "from solera_server.cli import main; main()"]
        command += ["--state-url", (self.root / "state").as_uri(), "--namespace", "contract", *argv]
        done = await asyncio.to_thread(
            subprocess.run, command, env={**os.environ}, capture_output=True, text=True, timeout=120
        )
        return done.returncode, done.stdout, done.stderr

    def refuse_upkeep(self, output: str, refused: bool = True) -> None:
        """The namespace's object store refuses (or takes again) the writes
        of `output`'s key index upkeep: a disk or bucket gone read-only there."""

        import os

        root = self.root / "state" / "contract" / "keys" / output
        for path in [root, *(p for p in root.rglob("*") if p.is_dir())]:
            os.chmod(path, 0o555 if refused else 0o755)

    # -- PERKEY+HISTORY (target) ---------------------------------------------------

    async def key_log(self, asset: str, **params) -> list[dict]:
        """What a per-key asset's keys came to, call by call, newest first
        (`GET /assets/{name}/outcomes/history`), every page: of `params` only
        (`run=`, `key=`, `outcome=`), if named."""

        out, before = [], None
        while True:
            page = await self.get(
                f"/assets/{asset}/outcomes/history", **params, **({"before": before} if before else {})
            )
            out += page["outcomes"]
            if (before := page["next"]) is None:
                return out

    # -- FULLRUN+STALENESS (target) ----------------------------------------------

    async def alarms(self) -> dict:
        """What the engine's diagnostics (`GET /api/diagnostics`) raise, past
        what names the engine itself: where an operator looks for what needs
        a hand (stuck cleanups today; a full run left incomplete, STA-13)."""

        names = ("backend", "state", "objects", "namespace", "project", "deploy", "inflight", "active_runs")
        return {k: v for k, v in (await self.get("/api/diagnostics")).items() if k not in names and v}

    async def load(self, output: str, head: dict, keys: dict):
        """A keyed output's content at `head` (as `head()` gave it), read
        through its store at `keys`, as a reader pinned there would: None
        once its data is gone."""

        from obstore.exceptions import NotFoundError
        from solera.stores import StoreError

        ref, store = Ref.from_json(head["ref"]), self.project.stores[head["ref"]["store"]]
        try:
            return await store.load(ref, list[dict], Keys(keys))
        except (FileNotFoundError, NotFoundError, StoreError):
            return None


# -- the outside world, as a user's project sees it ---------------------------


class Outside:
    """What lives outside Solera: `feed`, a keyed source's current rows
    (key -> value, the value doubling as the version it is served at), and
    `knob`, an unkeyed source's current value."""

    def __init__(self):
        self.feed: dict[str, str] = {}
        self.knob = "w1"


class SourceStore(FileStore):
    """A user's store serving a keyed source live: every key the outside
    holds, at its current value as its version (docs/versions.md §6)."""

    def __init__(self, path, outside: Outside):
        super().__init__(path)
        self.outside = outside

    def serve(self, source, keys, ctx):
        current = self.outside.feed
        return {
            k: Loaded({"id": k, "v": current[k]}, version=current[k]) for k in keys or current if k in current
        }


def rebuild(changes, rows: list[dict]):
    """A keyed consumer's write for one batch: a full run starts its output
    over on its first batch; every other batch patches it."""

    if changes.reset:
        return rows
    return Patch(rows, remove=list(changes.removed))
