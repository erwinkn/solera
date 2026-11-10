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

    # -- ENG+CLN+STO ---------------------------------------------------------------
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
