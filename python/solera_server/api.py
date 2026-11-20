"""The solera API (§5, §8-§10): project-scoped reads and run control under
`/api/projects/{p}`, and the worker channel (docs/lifecycle.md §5): attempt
routes authenticated by the attempt's own token, pool discovery by a pool
token. Everything else takes a caller with a role (`auth.py`, docs/access.md)."""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
import re
import secrets
import signal
import traceback
from contextlib import asynccontextmanager
from pathlib import Path

import solera_worker
from fastapi import FastAPI, Header, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from solera import Project, lifecycle
from solera.access import Identity, OpenID
from solera.lifecycle import Ended
from solera_worker.worker import described, load_project

from . import auth, openid, reloading
from .engine import Conflict, Engine
from .executors.local import read_manifest
from .history import TERMINAL_RUN, RunFilter
from .journal import Fenced
from .sensors import HOST_TOKEN
from .state import LostOwnership, State, Unavailable, open_store
from .tracking import full as core_full

log = logging.getLogger(__name__)

ATTEMPT_ROUTE = re.compile(r"^/api/projects/[^/]+/attempts/([^/]+)/(start|beat|logs|loaded|finished)$")
POOL_ROUTE = re.compile(r"^/api/projects/[^/]+/pools/[^/]+/work$")
SENSOR_ROUTE = re.compile(r"^/api/projects/[^/]+/sensors/(next|[^/]+/ticks/[^/]+)$")

ADMIN = Identity("admin", "admin")  # whoever holds SOLERA_API_TOKEN
LOCAL = Identity("local", "admin")  # anyone, under --insecure (loopback only)


async def _end_when_stopped(app, runtime) -> None:
    """An engine that writes no more ends its serve: answering 503 for ever
    would keep its port and its place from whoever serves the namespace now.
    Replaced (fenced), the process exits 0 — its successor owns everything,
    and a supervisor must not start it again against it; stopped for any
    other reason, 1."""

    await runtime.state.ended.wait()
    stopped = runtime.state.journal.stopped
    app.state.exit_code = 0 if isinstance(stopped, Fenced) else 1
    log.error("the engine writes no more (%s): the serve stops", stopped)
    signal.raise_signal(signal.SIGTERM)  # uvicorn's own graceful stop


class RunInput(BaseModel):
    targets: list[str] = Field(min_length=1, max_length=100)
    partitions: str | list[str] = "latest"
    mode: str = "incremental"
    upstream: bool = False
    config: dict = Field(default_factory=dict)
    keys: dict[str, list[str] | str] | None = None  # per input: keys, or "all"
    by: str | None = Field(default=None, max_length=200)
    tags: dict[str, str] = Field(default_factory=dict)


class PruneInput(BaseModel):
    before: float | None = None
    asset: str | None = None
    keep: int | None = Field(default=None, ge=1)
    dry_run: bool = False


class KeysRetryInput(BaseModel):
    classes: list[str] = Field(default_factory=lambda: ["failed"], max_length=10)
    partition: str | None = None
    by: str | None = Field(default=None, max_length=200)


class CleanupsClearInput(BaseModel):
    output: str
    partition: str = ""
    by: str | None = Field(default=None, max_length=200)


class SourceCommitInput(BaseModel):
    version: str | None = None
    keys: dict | list | None = None
    upsert: dict | list | None = None
    by: str | None = Field(default=None, max_length=200)
    remove: list[str] | None = Field(default=None, max_length=100000)


async def _import(project: str, state_url: str, namespace: str) -> tuple[Project, dict, list[str]]:
    """Import the project into this process, as `solera serve PROJECT` asks:
    it, its manifest and its source files. Code that fails to load is
    recorded first, before this server takes the namespace over: the
    server it was to replace still runs, and shows why (reloading.py)."""

    try:
        loaded = load_project(project)
        return loaded, *read_manifest(described(loaded))
    except Exception as error:
        try:
            await reloading.failed(open_store(state_url, namespace)[0], _traceback(error))
        except Exception:
            log.exception("the project did not load, and that could not be recorded")
        raise


LOADER = (str(Path(__file__).parent), str(Path(solera_worker.__file__).parent))


def _traceback(error: BaseException) -> str:
    """`error`'s traceback from the project's own code on: the frames of
    the import that reached it (this package's, the worker's, importlib's)
    say nothing about what to fix."""

    tb = error.__traceback__
    while tb is not None and tb.tb_frame.f_code.co_filename.startswith((*LOADER, "<frozen")):
        tb = tb.tb_next
    return "".join(traceback.format_exception(type(error), error, tb or error.__traceback__))


def asked_by(request: Request, given: str | None) -> str | None:
    """Who asked (a run's `by`, a commit's): a caller the project's sign-in
    named is that name, whatever the body says, so nobody signs with another's.
    The admin token and --insecure name nobody, so the body's word stands."""

    signed_in = request.state.signed_in
    return signed_in.name if signed_in is not None else given


def create_app(
    *,
    state_url=None,
    namespace=None,
    project=None,
    token=None,
    signin: auth.SignIn | None = None,
    insecure=False,
    engine=None,
    engine_url=None,
    reload=False,
):
    state_url = state_url or os.getenv("SOLERA_STATE_URL", Path(".solera").resolve().as_uri())
    namespace = namespace or os.getenv("SOLERA_NAMESPACE", "default")
    project = project or os.getenv("SOLERA_PROJECT", "solera_server.demo:project")
    token = token or os.getenv("SOLERA_API_TOKEN")
    pool_token = os.getenv("SOLERA_POOL_TOKEN")
    engine_url = engine_url or os.getenv("SOLERA_ENGINE_URL")

    @asynccontextmanager
    async def lifespan(app):
        owned = engine is None
        runtime = engine
        if owned:
            loaded, manifest, files = await _import(project, state_url, namespace)
            # The project's sign-in (`Project(sign_in=...)`), unless the caller
            # gave one. A reload keeps the one imported here; a new one takes a
            # restart.
            if signin is None:
                app.state.signin = loaded.sign_in
            if isinstance(app.state.signin, OpenID):
                app.state.signin = openid.Flow(app.state.signin)
        if not token and not app.state.signin and not insecure:
            raise ValueError(
                "Set SOLERA_API_TOKEN, give the project a sign_in, or explicitly enable insecure local development"
            )
        if owned:
            state = await State.open(state_url, namespace)
            runtime = Engine(
                state,
                manifest,
                project=project,
                concurrency=int(os.getenv("SOLERA_CONCURRENCY", "4")),
                engine_url=engine_url,
            )
            try:
                await runtime.initialize()
            except BaseException:
                await state.close()
                raise
            await reloading.loaded(state.objects)
        app.state.engine = runtime
        await runtime.start()
        if owned and reload:  # a local serve: each new deploy served as its code changes
            runtime.tasks.spawn(reloading.reload(runtime, project, files), key="reload")
        ending = asyncio.create_task(_end_when_stopped(app, runtime)) if owned else None
        try:
            yield
        finally:
            if ending is not None:
                ending.cancel()
            await runtime.stop()
            if owned:
                await runtime.state.close()

    app = FastAPI(
        title="Data Orchestrator", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None
    )
    app.state.signin = signin

    web = Path(__file__).parent / "web"

    def channel(request: Request, presented: str) -> bool | None:
        """For a worker channel's route, whether the request carries that
        channel's own credential; None for any other route."""

        path = request.url.path
        if (match := ATTEMPT_ROUTE.match(path)) is not None:
            secret = request.app.state.engine.secret
            return secret is not None and lifecycle.valid(secret, match.group(1), presented)
        if POOL_ROUTE.match(path):  # without a pool token, the admin token alone
            return bool(pool_token) and hmac.compare_digest(presented.encode(), pool_token.encode())
        if SENSOR_ROUTE.match(path):  # sensor workers: the pool token, or the local host's own
            if pool_token and hmac.compare_digest(presented.encode(), pool_token.encode()):
                return True
            secret = request.app.state.engine.secret
            return secret is not None and lifecycle.valid(secret, HOST_TOKEN, presented)
        return None

    def unauthenticated(request: Request) -> JSONResponse:
        body = {"detail": "Authentication required"}
        if isinstance(request.app.state.signin, openid.Flow):  # where a browser goes to sign in
            body["sign_in"] = openid.PATHS["sign_in"]
        return JSONResponse(body, status_code=401)

    async def check(request: Request) -> JSONResponse | None:
        """The one check every /api/ request passes; a refusal, or None to go on.
        A worker channel's route takes its own credential (or the admin token).
        Every other route takes a caller with the role its method needs
        (`auth.needs`), recognised in order by the admin token, the project's
        sign-in, and --insecure; it is left on `request.state` for the routes,
        as `identity`, and as `signed_in` too when the sign-in named it."""

        presented = request.headers.get("Authorization", "").removeprefix("Bearer ")
        admin = bool(token) and hmac.compare_digest(presented.encode(), token.encode())
        worker = channel(request, presented)
        if worker is not None:
            # Workers are not callers: the sign-in is never asked here, so it cannot open these.
            return None if worker or admin or insecure else unauthenticated(request)
        signed_in = None
        signin = request.app.state.signin
        if not admin and signin is not None:
            try:
                signed_in = await auth.sign_in(signin, request)
            except Exception as error:
                # Fail closed. A 500, not a 401: the caller may well be who they say,
                # and a 401 would send a browser round its sign-in again for nothing.
                # The log gets the error's type and where it was raised, never its
                # message: a parser's message quotes its input, here a cookie or a token.
                log.error(
                    "the project's sign-in failed on %s %s: %s\n%s",
                    request.method,
                    request.url.path,
                    type(error).__name__,
                    "".join(traceback.format_tb(error.__traceback__)),
                )
                return JSONResponse({"detail": "The sign-in failed; see the server's log"}, status_code=500)
        identity = ADMIN if admin else signed_in or (LOCAL if insecure else None)
        if identity is None:
            return unauthenticated(request)
        need = auth.needs(request.method)
        if need != "read" and auth.cross_site(request):
            return JSONResponse({"detail": "A change must come from the console's own page"}, status_code=403)
        if not auth.allows(identity, need):
            return JSONResponse(
                {"detail": f"This needs the {need} role; {identity.name} has {identity.role}."},
                status_code=403,
            )
        request.state.identity = identity
        request.state.signed_in = signed_in
        return None

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        if request.url.path.startswith("/api/") and (refused := await check(request)) is not None:
            return refused
        state = request.app.state.engine.state
        recorded = state.recorded
        response = await call_next(request)
        if request.url.path.startswith("/api/") and state.recorded != recorded:
            # A request that changed state is answered once the change is durable.
            try:
                await state.durable()
            except Unavailable as error:
                response = JSONResponse({"detail": str(error)}, status_code=503)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        # The console runs no inline script (its theme is applied by a file, not a
        # snippet); inline styles stay allowed for the style attributes it sets.
        # Everything remains same-origin: no external scripts, styles, or frames.
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
            "connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'"
        )
        response.headers["Cache-Control"] = "no-store" if request.url.path.startswith("/api/") else "no-cache"
        return response

    @app.exception_handler(Conflict)
    async def conflict(request, error):
        return JSONResponse({"detail": str(error)}, status_code=409)

    @app.exception_handler(ValueError)
    async def invalid(request, error):
        return JSONResponse({"detail": str(error)}, status_code=400)

    @app.exception_handler(KeyError)
    async def missing(request, error):
        return JSONResponse({"detail": "Resource not found"}, status_code=404)

    @app.exception_handler(Unavailable)
    async def unavailable(request, error):
        return JSONResponse({"detail": str(error)}, status_code=503)

    @app.exception_handler(LostOwnership)
    async def fenced(request, error):
        return JSONResponse({"detail": str(error)}, status_code=409)

    @app.exception_handler(Ended)
    async def ended(request, error):
        return JSONResponse({"detail": error.reason}, status_code=409)

    def engine_of(request: Request) -> Engine:
        return request.app.state.engine

    async def project_engine(request: Request, p: str) -> Engine:
        runtime = engine_of(request)
        if p != runtime.manifest["name"]:
            raise KeyError(p)
        return runtime

    async def asset_engine(request: Request, p: str, name: str) -> Engine:
        runtime = await project_engine(request, p)
        if name not in runtime.manifest["assets"]:
            raise KeyError(name)
        return runtime

    @app.get("/healthz")
    async def health(request: Request):
        # A liveness probe: 503 only when a restart is the fix, the journal stopped or the state
        # broken. Something failing in the background (a merge, an automation) is
        # degraded: a restart would fix none of it, and its error is in /api/diagnostics only,
        # since this route is unauthenticated.
        runtime = engine_of(request)
        if runtime.state.poisoned:
            return JSONResponse({"status": "unavailable", "backend": "object-store"}, status_code=503)
        return {"status": "degraded" if runtime.failing else "ok", "backend": "object-store"}

    @app.get("/api/me")
    async def me(request: Request):
        identity = request.state.identity
        return {"name": identity.name, "role": identity.role}

    @app.get("/api/diagnostics")
    async def diagnostics(request: Request):
        runtime = engine_of(request)
        return {
            "backend": "object-store",
            "state": runtime.state.url,
            "objects": runtime.state.objects_url,
            "namespace": runtime.state.namespace,
            "project": runtime.manifest["name"],
            "deploy": runtime.manifest["deploy"],
            "deploy_number": runtime.m.deploy_number,
            "inflight": len(runtime.watchers),
            "active_runs": sum(1 for r in runtime.m.runs.values() if r["status"] not in TERMINAL_RUN),
            "postgres": bool(os.environ.get("DATABASE_URL")),
            "last_error": "; ".join(f"{name}: {error}" for name, error in runtime.failing.items()) or None,
            # the latest code that did not load, a reload's or a new server's: this one serves the code before it
            "load_error": await reloading.load_error(runtime.state.objects),
            # a run left midway: its readers wait until a run of its producer completes it
            "incomplete_full_runs": core_full.stopped(runtime),
            # a batch loaded, from a table, a version or an absence nothing explains: it held the
            # key back, which stays owed until its producer writes it again
            "anomalies": [found for keys in runtime.anomalies.values() for found in keys.values()],
            # history rows left out (the latest 100), the rest written: one a flush could not encode,
            # or a worker's malformed key outcome
            "skipped_history_rows": runtime.history.lake.skipped,
            # cleanup whose task ran out of tries: see and clear with `solera cleanups`
            "stuck_cleanups": [
                {"output": output, "partition": partition, "id": e["id"]}
                for (output, partition), entries in runtime.m.cleanups.items()
                for e in entries
                if e.get("stuck")
            ]
            + [
                {"output": output, "partition": partition, "id": f"delta:{d['n']}"}
                for (output, partition), queue in runtime.m.cleaning.items()
                for d in queue
                if d.get("stuck")
            ],
        }

    # -- project reads ---------------------------------------------------------

    @app.get("/api/projects/{p}/manifest")
    async def manifest(p: str, request: Request):
        runtime = await project_engine(request, p)
        return runtime.manifest

    @app.get("/api/projects/{p}/assets")
    async def assets(p: str, request: Request):
        runtime = await project_engine(request, p)
        return {"assets": await runtime.catalog()}

    @app.get("/api/projects/{p}/assets:status")
    async def asset_statuses(p: str, request: Request):
        runtime = await project_engine(request, p)
        return {"assets": await runtime.asset_statuses()}

    @app.get("/api/projects/{p}/assets/{name}")
    async def asset_detail(p: str, name: str, request: Request):
        runtime = await asset_engine(request, p, name)
        detail = await runtime.asset_detail(name)
        detail["automations"] = [
            runtime.automation_view(a) for a in runtime.m.automations.values() if name in a.get("targets", [])
        ]
        return detail

    @app.get("/api/projects/{p}/assets/{name}/stale-keys")
    async def asset_stale_keys(
        p: str,
        name: str,
        request: Request,
        partition: str = "",
        after: str | None = None,
        limit: int = Query(default=1000, ge=1, le=10000),
    ):
        """A page of a keyed asset's stale keys in `partition` (unpartitioned:
        none), and why (docs/observed-set.md): `{tracked, keys, next,
        reasons}`, `keys` as `[{key, reasons}]`. A key's reasons are only
        what is specific to it, each naming its input: `{kind: "input
        changed", input}` (that input owes the key) and `{kind: "upstream
        stale", input}` (its upstream key is stale, along an each chain). A
        partition-wide reason — its definition changed, a stale upstream
        every key depends on, a keyed output that is not `each` — is on the
        partition's `reasons` alone, never repeated per key: a key it covers
        lists none."""

        runtime = await asset_engine(request, p, name)
        return await runtime.stale_keys(name, partition, after=after, limit=limit)

    @app.get("/api/projects/{p}/assets/{name}/outcomes")
    async def asset_outcomes(
        p: str,
        name: str,
        request: Request,
        partition: str | None = None,
        key: str | None = None,
        after: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ):
        """A per-key asset's keys with their latest outcome (KEY-5): `{key,
        partition, outcome, owed, version, ...}` each, a failing key's record
        too (tries, since, last, next_at, message). `?outcome=failed&outcome=ok`
        keeps those; `?key=` that key's rows alone; `?after=` a page's `next`."""

        runtime = await asset_engine(request, p, name)
        outcomes = [v for v in request.query_params.getlist("outcome") if v]
        return await runtime.latest_outcomes(
            name, partition, key=key, outcomes=outcomes, after=after, limit=limit
        )

    @app.get("/api/projects/{p}/assets/{name}/explain")
    async def asset_explain(
        p: str, name: str, request: Request, key: str, partition: str = "", input: str | None = None
    ):
        """Why `key` is, or is not, in the asset's output (KEY-11): `ok`,
        `failing`, `excluded`, `not_matched`, `pending`, `removed` or
        `absent`, with the evidence."""

        runtime = await asset_engine(request, p, name)
        return await runtime.explain(name, key, partition, input)

    @app.get("/api/projects/{p}/assets/{name}/outcomes/history")
    async def asset_outcome_history(
        p: str,
        name: str,
        request: Request,
        partition: str | None = None,
        key: str | None = None,
        q: str | None = None,
        run: str | None = None,
        before: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ):
        """The log of what a per-key asset's keys came to (§10), run by run,
        newest first; repeat `outcome` to match any of several."""

        runtime = await asset_engine(request, p, name)
        outcomes = [v for v in request.query_params.getlist("outcome") if v]
        page = await runtime.history.key_outcomes(
            name, partition=partition, key=key, q=q, outcomes=outcomes, run=run, before=before, limit=limit
        )
        return {"asset": name, **page}

    @app.get("/api/projects/{p}/outputs/{name}/heads")
    async def output_heads(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        if name not in runtime.manifest["outputs"]:
            raise KeyError(name)
        out = []
        planner = runtime.planner()
        owner = runtime.manifest["outputs"][name].get("asset")
        for partition, head in runtime.m.heads_of(name):
            cursor = owner is not None and runtime.m.partition(owner, partition).get("cursor") is not None
            out.append(
                {
                    "partition": partition,
                    "ref": head["ref"],
                    "version": head.get("version"),
                    "key_count": runtime.m.key_count(name, partition),
                    "commit_number": head.get("commit_number"),
                    "materialized": planner.head_materialized(name, partition),
                    "cursor": cursor,
                    "at": head["at"],
                    "commit": runtime.head_view(name, partition, head)["commit"],
                    "cleanups": runtime.partition_cleanups(name, partition),
                }
            )
        return {"output": name, "heads": out}

    @app.get("/api/projects/{p}/outputs/{name}/keys")
    async def output_keys(
        p: str,
        name: str,
        request: Request,
        partition: str = Query(default=""),
        after: str | None = Query(default=None),
        offset: int = Query(default=0, ge=0, le=100000),
        limit: int = Query(default=1000, ge=1, le=100000),
    ):
        runtime = await project_engine(request, p)
        page = await runtime.list_keys(name, partition, after=after, offset=offset, limit=limit)
        return {"output": name, "partition": partition, **page}

    @app.get("/api/projects/{p}/partitions/{name}")
    async def partitions(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        try:
            asset = runtime.planner().asset_of(name)
        except ValueError:
            raise KeyError(name) from None
        if not runtime.planner().dims(asset):
            raise ValueError(f"{asset} is unpartitioned")
        return {"asset": asset, "partitions": (await runtime.partition_statuses([asset]))[asset]}

    # -- runs -------------------------------------------------------------------

    @app.post("/api/projects/{p}/runs", status_code=201)
    async def submit_run(
        p: str, body: RunInput, request: Request, idempotency_key: str | None = Header(default=None)
    ):
        runtime = await project_engine(request, p)
        fields = body.model_dump()
        fields["by"] = asked_by(request, fields["by"]) or "api"
        run = await runtime.submit(**fields, command_id=idempotency_key)
        return run or {"status": "skipped-active"}

    def run_filter(request: Request) -> RunFilter:
        """A run filter from the query string: `?status=failed&status=canceled
        &asset=feed&tag=team=growth&q=timeout&since=…`. Repeat a field to
        match any of its values."""

        query = request.query_params
        f = RunFilter(q=query.get("q") or None)
        for name in ("status", "asset", "asset_tag", "origin", "automation", "by", "source", "tag"):
            setattr(f, name, [v for v in query.getlist(name) if v])
        for name in ("since", "until"):
            if query.get(name):
                setattr(f, name, float(query[name]))
        return f

    @app.get("/api/projects/{p}/runs")
    async def runs(
        p: str,
        request: Request,
        before: str | None = Query(default=None),
        anchor: str | None = Query(default=None),
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=50, ge=1, le=500),
    ):
        runtime = await project_engine(request, p)
        return await runtime.list_runs(
            run_filter(request), before=before, anchor=anchor, offset=offset, limit=limit
        )

    @app.get("/api/projects/{p}/runs:facets")
    async def run_facets(p: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.history.facets(run_filter(request))

    @app.get("/api/projects/{p}/runs:histogram")
    async def run_histogram(p: str, request: Request, bars: int = Query(default=60, ge=1, le=500)):
        runtime = await project_engine(request, p)
        return await runtime.history.histogram(run_filter(request), bars=bars)

    @app.get("/api/projects/{p}/tasks")
    async def tasks(
        p: str,
        request: Request,
        asset: str | None = None,
        partition: str | None = None,
        run: str | None = None,
        since: float | None = None,
        until: float | None = None,
        before: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ):
        runtime = await project_engine(request, p)
        status = [v for v in request.query_params.getlist("status") if v]
        return await runtime.history.tasks(
            asset=asset,
            partition=partition,
            status=status,
            run=run,
            since=since,
            until=until,
            before=before,
            limit=limit,
        )

    @app.get("/api/projects/{p}/tasks:histogram")
    async def task_histogram(
        p: str,
        request: Request,
        since: float,
        bucket: float = Query(gt=0),
        until: float | None = None,
        asset: str | None = None,
    ):
        runtime = await project_engine(request, p)
        return await runtime.history.task_histogram(since=since, until=until, bucket=bucket, asset=asset)

    @app.get("/api/projects/{p}/stats")
    async def stats(
        p: str,
        request: Request,
        since: float | None = None,
        until: float | None = None,
        asset: str | None = None,
        partition: str | None = None,
    ):
        runtime = await project_engine(request, p)
        return await runtime.history.stats(since=since, until=until, asset=asset, partition=partition)

    @app.get("/api/projects/{p}/assets/{name}/history")
    async def asset_history(
        p: str,
        name: str,
        request: Request,
        output: str | None = None,
        partition: str | None = None,
        before: str | None = None,
        limit: int = Query(default=200, ge=1, le=5000),
    ):
        runtime = await project_engine(request, p)
        info = runtime.manifest["assets"].get(name)
        if info is None:
            raise KeyError(name)
        names = [o["name"] for o in info["outputs"]]
        if output is not None:
            if output not in names:
                raise KeyError(output)
            names = [output]
        page = await runtime.history.commits(outputs=names, partition=partition, before=before, limit=limit)
        return {"asset": name, **page}

    @app.get("/api/projects/{p}/outputs/{name}/lineage")
    async def output_lineage(
        p: str,
        name: str,
        request: Request,
        partition: str = "",
        generation: int | None = None,
        direction: str = Query(default="upstream", pattern="^(upstream|downstream)$"),
        depth: int = Query(default=5, ge=1, le=50),
    ):
        runtime = await project_engine(request, p)
        if generation is None:
            head = runtime.m.heads.get((name, partition))
            if head is None:
                raise KeyError(name)
            generation = head["ref"].get("generation")
        return await runtime.history.lineage(
            name, partition, generation, downstream=direction == "downstream", depth=depth
        )

    @app.get("/api/projects/{p}/runs/{run_id}")
    async def run_detail(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.run_detail(run_id)

    @app.get("/api/projects/{p}/runs/{run_id}/events")
    async def run_timeline(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.history.events(run_id)

    @app.post("/api/projects/{p}/runs/{run_id}/cancel")
    async def cancel_run(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.cancel(run_id, by=asked_by(request, None))

    @app.post("/api/projects/{p}/runs/{run_id}/retry")
    async def retry_run(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.retry(run_id, by=asked_by(request, None))

    @app.post("/api/projects/{p}/runs/{run_id}/pause")
    async def pause_run(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.pause(run_id, by=asked_by(request, None))

    @app.post("/api/projects/{p}/runs/{run_id}/resume")
    async def resume_run(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.pause(run_id, False, by=asked_by(request, None))

    # -- attempts ----------------------------------------------------------------

    @app.delete("/api/projects/{p}/runs/{run_id}")
    async def delete_run(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        await runtime.delete_run(run_id)
        return {"deleted": [run_id]}

    @app.post("/api/projects/{p}/runs:prune")
    async def prune_runs(p: str, body: PruneInput, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.prune(before=body.before, asset=body.asset, keep=body.keep, dry_run=body.dry_run)

    # -- attempts (§8) --------------------------------------------------------------

    @app.get("/api/projects/{p}/runs/{run_id}/attempts/{attempt}/logs")
    async def attempt_logs(
        p: str, run_id: str, attempt: str, request: Request, tail: int | None = Query(None, ge=1)
    ):
        runtime = await project_engine(request, p)
        live = runtime.attempt_lines(attempt)
        if live is not None:  # still running: the lines it sent live
            lines = live[-tail:] if tail else live
            return Response("".join(lines).encode(), media_type="application/x-ndjson")
        data = await runtime.state.attempt_log(run_id, attempt, tail)
        return Response(data, media_type="application/x-ndjson")

    @app.get("/api/projects/{p}/runs/{run_id}/attempts/{attempt}/spec")
    async def attempt_spec(p: str, run_id: str, attempt: str, request: Request):
        runtime = await project_engine(request, p)
        spec = await runtime.state.attempt_spec(run_id, attempt)
        if spec is None:
            raise KeyError(f"{run_id}/{attempt}")
        return {k: v for k, v in spec.items() if k != "token"}

    @app.get("/api/projects/{p}/runs/{run_id}/attempts/{attempt}/result")
    async def attempt_result(p: str, run_id: str, attempt: str, request: Request):
        runtime = await project_engine(request, p)
        result = await runtime.state.attempt_result(run_id, attempt)
        if result is None:
            raise KeyError(f"{run_id}/{attempt}: no result yet")
        return result

    @app.get("/api/projects/{p}/cleanups")
    async def cleanups(p: str, request: Request):
        runtime = await project_engine(request, p)
        return {"cleanups": runtime.cleanups_view()}

    @app.post("/api/projects/{p}/assets/{name}/keys:retry")
    async def retry_keys(p: str, name: str, body: KeysRetryInput, request: Request):
        """KEY-7: call a per-key asset's failing keys of `classes` once more
        each (`all`: every stored outcome), now: `{asset, classes,
        partitions, runs}`, the runs it started."""

        runtime = await asset_engine(request, p, name)
        return await runtime.retry_keys(
            name, body.classes, body.partition, asked_by(request, body.by) or "api"
        )

    @app.post("/api/projects/{p}/cleanups:clear")
    async def clear_cleanups(p: str, body: CleanupsClearInput, request: Request):
        runtime = await project_engine(request, p)
        return runtime.clear_cleanups(body.output, body.partition, asked_by(request, body.by) or "api")

    # -- the worker channel (docs/lifecycle.md §5) ----------------------------------------

    @app.post("/api/projects/{p}/attempts/{attempt}/start")
    async def attempt_start(p: str, attempt: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.attempt_start(attempt, await request.json())

    @app.post("/api/projects/{p}/attempts/{attempt}/beat")
    async def attempt_beat(p: str, attempt: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.attempt_beat(attempt, await request.json())

    @app.post("/api/projects/{p}/attempts/{attempt}/logs")
    async def attempt_live_logs(p: str, attempt: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.attempt_logs(attempt, await request.json())

    @app.post("/api/projects/{p}/attempts/{attempt}/loaded")
    async def attempt_loaded(p: str, attempt: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.attempt_loaded(attempt, await request.json())

    @app.post("/api/projects/{p}/attempts/{attempt}/finished", status_code=204)
    async def attempt_finished(p: str, attempt: str, request: Request):
        runtime = await project_engine(request, p)
        await runtime.attempt_finished(attempt, await request.json())
        return Response(status_code=204)

    @app.get("/api/projects/{p}/pools/{pool}/work")
    async def pool_work(p: str, pool: str, request: Request, wait: float = Query(30, ge=0, le=30)):
        runtime = await project_engine(request, p)
        query = request.query_params
        capacity = {k: float(query[k]) for k in ("cpu", "memory", "gpu") if k in query}
        host = query.get("host") or (request.client.host if request.client else "worker")
        return {"work": await runtime.pool_work(pool, capacity, host, wait)}

    # -- sensors (docs/lifecycle.md §11) ---------------------------------------------

    @app.get("/api/projects/{p}/sensors")
    async def sensors(p: str, request: Request):
        runtime = await project_engine(request, p)
        return {"sensors": runtime.sensor_views(), "workers": list(runtime.sensor_hosts.values())}

    @app.get("/api/projects/{p}/sensors/next")
    async def sensors_next(
        p: str,
        request: Request,
        executor: str,
        deploy: str,
        slots: int = Query(4, ge=0, le=64),
        wait: float = Query(30, ge=0, le=30),
        build: str | None = None,
    ):
        runtime = await project_engine(request, p)
        host = request.query_params.get("host") or (request.client.host if request.client else "host")
        return await runtime.sensor_next(executor, deploy, host, slots, wait, build)

    @app.post("/api/projects/{p}/sensors/{sensor}/ticks/{tick}")
    async def sensor_tick(p: str, sensor: str, tick: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.sensor_post(sensor, tick, await request.json())

    @app.get("/api/projects/{p}/sensors/{sensor}/ticks")
    async def sensor_ticks(p: str, sensor: str, request: Request, limit: int = Query(100, ge=1, le=1000)):
        runtime = await project_engine(request, p)
        if sensor not in runtime.manifest.get("sensors", {}):
            raise KeyError(sensor)
        return {"ticks": await runtime.history.ticks(sensor, limit)}

    # -- automations -------------------------------------------------------------

    @app.get("/api/projects/{p}/automations")
    async def automations(p: str, request: Request):
        runtime = await project_engine(request, p)
        return {"automations": [runtime.automation_view(a) for a in runtime.m.automations.values()]}

    @app.post("/api/projects/{p}/automations/{name}/enable")
    async def automation_enable(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        return runtime.automation_view(await runtime.set_automation(name, True))

    @app.post("/api/projects/{p}/automations/{name}/disable")
    async def automation_disable(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        return runtime.automation_view(await runtime.set_automation(name, False))

    @app.post("/api/projects/{p}/automations/{name}/run-now", status_code=202)
    async def automation_run_now(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        return runtime.automation_view(await runtime.run_automation(name, by=asked_by(request, None)))

    # -- sources -------------------------------------------------------------------

    @app.post("/api/projects/{p}/sources/{name}/commit")
    async def source_commit(p: str, name: str, body: SourceCommitInput, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.commit_source(
            name,
            version=body.version,
            keys=body.keys,
            upsert=body.upsert,
            remove=body.remove,
            by=asked_by(request, body.by) or "api",
        )

    # -- executors + workers --------------------------------------------------------

    @app.get("/api/projects/{p}/executors")
    async def executors(p: str, request: Request):
        runtime = await project_engine(request, p)

        def limit(name, executor):
            placement = runtime.registry.build({"executor": name, **executor, "options": {}})
            return getattr(placement, "max_concurrent", None)

        return {
            "executors": [
                {
                    "name": name,
                    **executor,
                    "max_concurrent": limit(name, executor),
                    "in_flight": runtime.executor_inflight.get(name, 0),
                }
                for name, executor in runtime.manifest["executors"].items()
            ]
        }

    @app.get("/api/projects/{p}/workers")
    async def workers(p: str, request: Request):
        runtime = await project_engine(request, p)
        return {"workers": sorted(runtime.pollers.values(), key=lambda w: w["id"])}

    # -- console -----------------------------------------------------------------------
    # The console builds with base=/static/: its bundle is web/… served under
    # /static/…, and every other path is a client route (/runs/…, and names
    # with dots, like /sensors/landing.observe). A missing bundle file is a
    # 404, never the shell, so a page from an older build fails loudly.

    # -- signing in through a provider (`OpenID`, openid.py) ---------------------------

    def flow(request: Request) -> openid.Flow:
        if not isinstance(request.app.state.signin, openid.Flow):
            raise KeyError(request.url.path)  # a 404: this project signs in some other way
        return request.app.state.signin

    @app.get(openid.PATHS["sign_in"])
    async def sign_in(request: Request):
        return await flow(request).sign_in(request)

    @app.get(openid.PATHS["callback"])
    async def callback(request: Request):
        return await flow(request).callback(request)

    @app.get(openid.PATHS["sign_out"])
    async def sign_out(request: Request):
        return await flow(request).sign_out(request)

    @app.get("/")
    async def index():
        return FileResponse(web / "index.html")

    @app.get("/{path:path}")
    async def console(path: str):
        if path.startswith("api/"):
            raise KeyError(path)
        bundle = path.removeprefix("static/")
        target = (web / bundle).resolve()
        if target.is_file() and web.resolve() in target.parents:
            return FileResponse(target)
        if path.startswith("static/") or ("/" not in path and "." in path):
            raise KeyError(path)  # a file asked for by name (/favicon.ico), not a route
        return FileResponse(web / "index.html")

    return app


def local_transport(engine: Engine, token: str):
    """A transport to `engine`'s own routes in this process, for workers it runs
    itself (`solera_worker.channel.LocalTransport`): the app is the one a
    served engine runs, with an admin token nobody holds, so a worker gets in
    only with its own token, as over HTTPS."""

    from solera_worker.channel import LocalTransport

    if engine.local_app is None:  # held by the engine, so the two go together
        # Its lifespan never runs, so it loads no sign-in: this app serves workers alone.
        engine.local_app = create_app(engine=engine, token=secrets.token_hex(16))
        engine.local_app.state.engine = engine
    return LocalTransport(engine.local_app, token)
