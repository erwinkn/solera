"""The solera API (§5, §8-§10): project-scoped reads and run control under
`/api/projects/{p}`, and the worker channel (docs/lifecycle.md §5): attempt
routes authenticated by the attempt's own token, pool discovery by a pool
token. Everything else takes the admin token."""

from __future__ import annotations

import hmac
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from solera import lifecycle
from solera.lifecycle import Ended

from .engine import Conflict, Engine
from .history import TERMINAL_RUN, RunFilter
from .placements.local import load_manifest
from .sensors import HOST_TOKEN
from .state import LostOwnership, State, Unavailable

ATTEMPT_ROUTE = re.compile(r"^/api/projects/[^/]+/attempts/([^/]+)/(start|beat|logs|resolve|finished)$")
POOL_ROUTE = re.compile(r"^/api/projects/[^/]+/pools/[^/]+/work$")
SENSOR_ROUTE = re.compile(r"^/api/projects/[^/]+/sensors/(next|[^/]+/ticks/[^/]+)$")


class RunInput(BaseModel):
    targets: list[str] = Field(min_length=1, max_length=100)
    partitions: str | list[str] = "latest"
    mode: str = "incremental"
    upstream: bool = False
    config: dict = Field(default_factory=dict)
    keys: dict | None = None
    by: str | None = Field(default=None, max_length=200)
    tags: dict[str, str] = Field(default_factory=dict)


class PruneInput(BaseModel):
    before: float | None = None
    asset: str | None = None
    keep: int | None = Field(default=None, ge=1)
    dry_run: bool = False


class SourceCommitInput(BaseModel):
    version: str | None = None
    keys: dict | list | None = None
    upsert: dict | list | None = None
    by: str | None = Field(default=None, max_length=200)
    remove: list[str] | None = Field(default=None, max_length=100000)


def create_app(
    *, state_url=None, namespace=None, project=None, token=None, insecure=False, engine=None, engine_url=None
):
    state_url = state_url or os.getenv("SOLERA_STATE_URL", Path(".solera").resolve().as_uri())
    namespace = namespace or os.getenv("SOLERA_NAMESPACE", "default")
    project = project or os.getenv("SOLERA_PROJECT", "solera_server.demo:project")
    token = token or os.getenv("SOLERA_API_TOKEN")
    pool_token = os.getenv("SOLERA_POOL_TOKEN")
    engine_url = engine_url or os.getenv("SOLERA_ENGINE_URL")

    @asynccontextmanager
    async def lifespan(app):
        if not token and not insecure:
            raise ValueError("Set SOLERA_API_TOKEN, or explicitly enable insecure local development")
        owned = engine is None
        runtime = engine
        if owned:
            manifest = await load_manifest(project)
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
        app.state.engine = runtime
        await runtime.start()
        try:
            yield
        finally:
            await runtime.stop()
            if owned:
                await runtime.state.close()

    app = FastAPI(
        title="Data Orchestrator", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None
    )

    web = Path(__file__).parent / "web"

    def allowed(request: Request) -> bool:
        presented = request.headers.get("Authorization", "").removeprefix("Bearer ")
        if hmac.compare_digest(presented.encode(), token.encode()):
            return True
        path = request.url.path
        if (match := ATTEMPT_ROUTE.match(path)) is not None:
            secret = request.app.state.engine.secret
            return secret is not None and lifecycle.valid(secret, match.group(1), presented)
        if POOL_ROUTE.match(path) and pool_token:
            return hmac.compare_digest(presented.encode(), pool_token.encode())
        if SENSOR_ROUTE.match(path):  # sensor hosts: the pool token, or the local host's own
            if pool_token and hmac.compare_digest(presented.encode(), pool_token.encode()):
                return True
            secret = request.app.state.engine.secret
            return secret is not None and lifecycle.valid(secret, HOST_TOKEN, presented)
        return False

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        if request.url.path.startswith("/api/") and token and not allowed(request):
            return JSONResponse({"detail": "Authentication required"}, status_code=401)
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
        runtime = engine_of(request)
        healthy = not runtime.state.poisoned and not runtime.failing
        return JSONResponse(
            {"status": "ok" if healthy else "unavailable", "backend": "object-store"},
            status_code=200 if healthy else 503,
        )

    @app.get("/api/diagnostics")
    async def diagnostics(request: Request):
        runtime = engine_of(request)
        return {
            "backend": "object-store",
            "state": runtime.state.url,
            "objects": runtime.state.objects_url,
            "namespace": runtime.state.namespace,
            "project": runtime.manifest["name"],
            "revision": runtime.manifest["revision"],
            "inflight": len(runtime.inflight),
            "active_runs": sum(1 for r in runtime.m.runs.values() if r["status"] not in TERMINAL_RUN),
            "postgres": bool(os.environ.get("DATABASE_URL")),
            "last_error": runtime.failing,
            # data garbage whose names could not be read: see and clear with `solera scopes discards`
            "stuck_discards": [
                {"output": output, "scope": scope, "id": e["id"]}
                for (output, scope), entries in runtime.m.discards.items()
                for e in entries
                if e.get("stuck")
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

    @app.get("/api/projects/{p}/assets/{name}/edges")
    async def asset_edges(p: str, name: str, request: Request):
        runtime = await asset_engine(request, p, name)
        return await runtime.asset_edges(name)

    @app.get("/api/projects/{p}/assets/{name}/failures")
    async def asset_failures(
        p: str,
        name: str,
        request: Request,
        scope: str | None = None,
        after: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ):
        """An Each asset's failing keys (docs/per-key-processing.md §9):
        `?outcome=rejected&outcome=failed` keeps those classes."""

        runtime = await asset_engine(request, p, name)
        outcomes = [v for v in request.query_params.getlist("outcome") if v]
        return await runtime.key_failures(name, scope, outcomes=outcomes, after=after, limit=limit)

    @app.get("/api/projects/{p}/assets/{name}/key-outcomes")
    async def asset_key_outcomes(
        p: str,
        name: str,
        request: Request,
        scope: str | None = None,
        key: str | None = None,
        q: str | None = None,
        run: str | None = None,
        before: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ):
        """What an Each asset's keys came to (§10), newest first; repeat
        `outcome` to match any of several."""

        runtime = await asset_engine(request, p, name)
        outcomes = [v for v in request.query_params.getlist("outcome") if v]
        page = await runtime.history.key_outcomes(
            name, scope=scope, key=key, q=q, outcomes=outcomes, run=run, before=before, limit=limit
        )
        return {"asset": name, **page}

    @app.get("/api/projects/{p}/assets/{name}/explain")
    async def asset_explain(
        p: str, name: str, request: Request, key: str, scope: str = "", edge: str | None = None
    ):
        runtime = await asset_engine(request, p, name)
        return await runtime.explain(name, key, scope, edge)

    @app.get("/api/projects/{p}/outputs/{name}/heads")
    async def output_heads(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        if name not in runtime.manifest["outputs"]:
            raise KeyError(name)
        out = []
        for scope, head in runtime.m.heads_of(name):
            owner = head.get("asset")
            cursor = owner is not None and runtime.m.cursors.get((owner, scope)) is not None
            out.append(
                {
                    "scope": scope,
                    "ref": head["ref"],
                    "version": head.get("version"),
                    "key_count": head.get("count"),
                    "batch": head.get("batch"),
                    "complete": head["complete"],
                    "cursor": cursor,
                    "at": head["at"],
                    "commit": runtime.head_view(head)["commit"],
                    "discards": runtime.scope_discards(name, scope),
                }
            )
        return {"output": name, "heads": out}

    @app.get("/api/projects/{p}/outputs/{name}/keys")
    async def output_keys(
        p: str,
        name: str,
        request: Request,
        scope: str = Query(default=""),
        after: str | None = Query(default=None),
        offset: int = Query(default=0, ge=0, le=100000),
        limit: int = Query(default=1000, ge=1, le=100000),
    ):
        runtime = await project_engine(request, p)
        page = await runtime.list_keys(name, scope, after=after, offset=offset, limit=limit)
        return {"output": name, "scope": scope, **page}

    @app.get("/api/projects/{p}/partitions/{name}")
    async def partitions(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        try:
            asset = runtime._asset_of(name)
        except ValueError:
            raise KeyError(name) from None
        if not runtime._dims(asset):
            raise ValueError(f"{asset} is unpartitioned")
        return {"asset": asset, "partitions": (await runtime.scope_statuses([asset]))[asset]}

    # -- runs -------------------------------------------------------------------

    @app.post("/api/projects/{p}/runs", status_code=201)
    async def submit_run(
        p: str, body: RunInput, request: Request, idempotency_key: str | None = Header(default=None)
    ):
        runtime = await project_engine(request, p)
        fields = body.model_dump()
        fields["by"] = fields["by"] or "api"
        run = await runtime.submit(**fields, command_id=idempotency_key)
        return run or {"status": "skipped-active"}

    def run_filter(request: Request) -> RunFilter:
        """A run filter from the query string: `?status=failed&status=canceled
        &asset=feed&tag=team=growth&q=timeout&since=…`. Repeat a field to
        match any of its values."""

        query = request.query_params
        f = RunFilter(q=query.get("q") or None)
        for name in ("status", "asset", "asset_tag", "trigger", "automation", "by", "source", "tag"):
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
        scope: str | None = None,
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
            scope=scope,
            status=status,
            run=run,
            since=since,
            until=until,
            before=before,
            limit=limit,
        )

    @app.get("/api/projects/{p}/stats")
    async def stats(
        p: str,
        request: Request,
        since: float | None = None,
        until: float | None = None,
        asset: str | None = None,
        scope: str | None = None,
    ):
        runtime = await project_engine(request, p)
        return await runtime.history.stats(since=since, until=until, asset=asset, scope=scope)

    @app.get("/api/projects/{p}/assets/{name}/history")
    async def asset_history(
        p: str,
        name: str,
        request: Request,
        output: str | None = None,
        scope: str | None = None,
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
        page = await runtime.history.materializations(outputs=names, scope=scope, before=before, limit=limit)
        return {"asset": name, **page}

    @app.get("/api/projects/{p}/outputs/{name}/lineage")
    async def output_lineage(
        p: str,
        name: str,
        request: Request,
        scope: str = "",
        version: str | None = None,
        direction: str = Query(default="upstream", pattern="^(upstream|downstream)$"),
        depth: int = Query(default=5, ge=1, le=50),
    ):
        runtime = await project_engine(request, p)
        if version is None:
            head = runtime.m.heads.get((name, scope))
            if head is None:
                raise KeyError(name)
            version = head["ref"].get("version")
        return await runtime.history.lineage(
            name, scope, version, downstream=direction == "downstream", depth=depth
        )

    @app.get("/api/projects/{p}/runs/{run_id}")
    async def run_detail(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.run_detail(run_id)

    @app.get("/api/projects/{p}/runs/{run_id}/events")
    async def run_events(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.history.events(run_id)

    @app.post("/api/projects/{p}/runs/{run_id}/cancel")
    async def cancel_run(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.cancel(run_id)

    @app.post("/api/projects/{p}/runs/{run_id}/retry")
    async def retry_run(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.retry(run_id)

    @app.post("/api/projects/{p}/runs/{run_id}/pause")
    async def pause_run(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.pause(run_id)

    @app.post("/api/projects/{p}/runs/{run_id}/resume")
    async def resume_run(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.pause(run_id, False)

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

    @app.get("/api/projects/{p}/holds")
    async def holds(p: str, request: Request):
        runtime = await project_engine(request, p)
        return runtime.holds_view()

    @app.post("/api/projects/{p}/scopes:clear-discards")
    async def clear_discards(p: str, request: Request):
        runtime = await project_engine(request, p)
        body = await request.json()
        return runtime.clear_discards(body["output"], body.get("scope", ""), body.get("by") or "api")

    @app.post("/api/projects/{p}/scopes:release")
    async def release_scope(p: str, request: Request):
        runtime = await project_engine(request, p)
        body = await request.json()
        return runtime.release_scope(body["asset"], body.get("scope", ""), body.get("by") or "api")

    @app.post("/api/projects/{p}/assets/{name}/keys:retry")
    async def retry_keys(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        body = await request.json()
        found = runtime.retry_keys(
            name, body.get("classes") or ["failed"], body.get("scope"), body.get("by") or "api"
        )
        if found["scopes"]:
            await runtime.submit_retries(name, found["scopes"], body.get("by") or "api")
        return found

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

    @app.post("/api/projects/{p}/attempts/{attempt}/resolve")
    async def attempt_resolve(p: str, attempt: str, request: Request):
        """A small write's delta from the engine's cache (docs/resolved-commits.md §4):
        binary, versioned framing both ways."""

        from solera.keys.resolver import CONTENT_TYPE, MAX_BODY, Malformed, UnsupportedVersion

        runtime = await project_engine(request, p)
        received = bytearray()
        async for chunk in request.stream():  # never more than a request may be
            received += chunk
            if len(received) > MAX_BODY:
                return JSONResponse({"detail": f"a body over {MAX_BODY} bytes"}, status_code=413)
        try:
            body = await runtime.attempt_resolve(attempt, bytes(received))
        except UnsupportedVersion as e:
            return JSONResponse({"detail": str(e)}, status_code=415)
        except Malformed as e:
            return JSONResponse({"detail": str(e)}, status_code=400)
        if body is None:
            return Response(status_code=503)
        return Response(content=body, media_type=CONTENT_TYPE)

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
        return {"sensors": runtime.sensor_views(), "hosts": list(runtime.sensor_hosts.values())}

    @app.get("/api/projects/{p}/sensors/next")
    async def sensors_next(
        p: str,
        request: Request,
        executor: str,
        revision: str,
        slots: int = Query(4, ge=0, le=64),
        wait: float = Query(30, ge=0, le=30),
        build: str | None = None,
    ):
        runtime = await project_engine(request, p)
        host = request.query_params.get("host") or (request.client.host if request.client else "host")
        return await runtime.sensor_next(executor, revision, host, slots, wait, build)

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
        return runtime.automation_view(await runtime.run_automation(name))

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
            by=body.by or "api",
        )

    # -- executors + workers --------------------------------------------------------

    @app.get("/api/projects/{p}/executors")
    async def executors(p: str, request: Request):
        runtime = await project_engine(request, p)

        def limit(name, executor):
            placement = runtime.registry.build({"executor": name, **executor, "placement": {}})
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
