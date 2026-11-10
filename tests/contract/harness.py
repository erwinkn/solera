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
