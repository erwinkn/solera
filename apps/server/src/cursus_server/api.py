"""The cursus API (§5, §8-§10): project-scoped reads and run control under
`/api/projects/{p}`, the worker pull path under `/api`, token auth."""

from __future__ import annotations

import hmac
import json
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from .engine import Conflict, Engine
from .placements.local import load_manifest
from .state import LostOwnership, State
from .storage import SlateState, Unavailable


class RunInput(BaseModel):
    targets: list[str] = Field(min_length=1, max_length=100)
    partitions: str | list[str] = "latest"
    mode: str = "incremental"
    upstream: bool = False
    config: dict = Field(default_factory=dict)
    keys: dict | None = None


class SourceCommitInput(BaseModel):
    version: str | None = None
    keys: dict | list | None = None
    upsert: dict | list | None = None
    remove: list[str] = Field(default_factory=list, max_length=100000)


def create_app(*, state_url=None, namespace=None, project=None, token=None, insecure=False, engine=None):
    state_url = state_url or os.getenv("CURSUS_STATE_URL", Path(".cursus").resolve().as_uri())
    namespace = namespace or os.getenv("CURSUS_NAMESPACE", "default")
    project = project or os.getenv("CURSUS_PROJECT", "cursus_server.demo:project")
    token = token or os.getenv("CURSUS_API_TOKEN")

    @asynccontextmanager
    async def lifespan(app):
        if not token and not insecure:
            raise ValueError("Set CURSUS_API_TOKEN, or explicitly enable insecure local development")
        owned = engine is None
        runtime = engine
        if owned:
            manifest = await load_manifest(project)
            slate = await SlateState.open(state_url, namespace)
            state = State(slate)
            runtime = Engine(
                state,
                manifest,
                project=project,
                concurrency=int(os.getenv("CURSUS_CONCURRENCY", "4")),
            )
            try:
                await runtime.initialize()
            except BaseException:
                await slate.close()
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

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        if request.url.path.startswith("/api/") and token:
            authorization = request.headers.get("Authorization", "")
            if not hmac.compare_digest(authorization.encode(), f"Bearer {token}".encode()):
                return JSONResponse({"detail": "Authentication required"}, status_code=401)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        # The prerendered console shell relies on framework-injected inline
        # hydration scripts, so inline script/style execution stays allowed.
        # Everything remains same-origin: no external scripts, styles, or frames.
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "
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

    def engine_of(request: Request) -> Engine:
        return request.app.state.engine

    async def project_engine(request: Request, p: str) -> Engine:
        runtime = engine_of(request)
        if p != runtime.manifest["name"]:
            raise KeyError(p)
        return runtime

    @app.get("/healthz")
    async def health(request: Request):
        runtime = engine_of(request)
        healthy = not runtime.state.poisoned and not runtime.last_error
        return JSONResponse(
            {"status": "ok" if healthy else "unavailable", "backend": "slatedb"},
            status_code=200 if healthy else 503,
        )

    @app.get("/api/diagnostics")
    async def diagnostics(request: Request):
        runtime = engine_of(request)
        return {
            "backend": "slatedb",
            "state": runtime.state.url,
            "objects": runtime.state.objects_url,
            "namespace": runtime.state.namespace,
            "project": runtime.manifest["name"],
            "revision": runtime.manifest["revision"],
            "inflight": len(runtime.inflight),
            "postgres": bool(os.environ.get("DATABASE_URL")),
            "last_error": runtime.last_error,
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

    @app.get("/api/projects/{p}/assets/{name}")
    async def asset_detail(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        if name not in runtime.manifest["assets"]:
            raise KeyError(name)
        detail = await runtime.asset_detail(name)
        detail["automations"] = [
            a for _, a in await runtime.state.scan("automation/") if name in a.get("targets", [])
        ]
        return detail

    @app.get("/api/projects/{p}/outputs/{name}/heads")
    async def output_heads(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        if name not in runtime.manifest["outputs"]:
            raise KeyError(name)
        out = []
        async with runtime.state.transaction() as tx:
            for scope, head in await tx.heads(name):
                owner = head.get("asset")
                cursor = owner is not None and (await tx.cursor(owner, scope)) is not None
                out.append(
                    {
                        "scope": scope,
                        "ref": head["ref"],
                        "version": head.get("version"),
                        "key_count": (head["ref"].get("meta") or {}).get("keys", {}).get("count"),
                        "complete": head["complete"],
                        "cursor": cursor,
                        "at": head["at"],
                        "commit": head["commit"],
                    }
                )
        return {"output": name, "heads": out}

    @app.get("/api/projects/{p}/outputs/{name}/keys")
    async def output_keys(
        p: str,
        name: str,
        request: Request,
        scope: str = Query(default=""),
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=1000, ge=1, le=100000),
    ):
        runtime = await project_engine(request, p)
        async with runtime.state.transaction() as tx:
            head = await tx.head(name, scope)
        if head is None:
            raise KeyError(f"{name}/{scope}")
        keys = await runtime.state.fetch_key_map(head["ref"].get("meta", {}).get("keys")) or {}
        items = sorted(keys.items())
        return {
            "output": name,
            "scope": scope,
            "total": len(items),
            "keys": dict(items[offset : offset + limit]),
        }

    @app.get("/api/projects/{p}/partitions/{name}")
    async def partitions(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        try:
            asset = runtime._asset_of(name)
        except ValueError:
            raise KeyError(name) from None
        dims = runtime._dims(asset)
        if not dims:
            raise ValueError(f"{asset} is unpartitioned")
        async with runtime.state.transaction() as tx:
            keys = await runtime._dim_keys(tx, dims)
            from itertools import product

            from cursus.sdk import canonical_partition

            current = {
                canonical_partition(dims, dict(zip(dims, combo, strict=True))) for combo in product(*keys)
            }
            outputs = runtime.manifest["assets"][asset]["outputs"]
            scopes = {}
            for output in outputs:
                for scope, head in await tx.heads(output["name"]):
                    scopes[scope] = head
            # Per-scope outcomes + the pending index — never a task/ scan (§8).
            outcomes = await tx.scope_outcomes(asset)
            running = await tx.pending_scopes(asset)
            out = []
            for scope in sorted(current | set(scopes) | set(outcomes)):
                head = scopes.get(scope)
                record = outcomes.get(scope) or {}
                status = (
                    "retired"
                    if scope not in current
                    else "complete"
                    if head and head["complete"]
                    else "running"
                    if scope in running
                    else "failed"
                    if record.get("last_outcome") in {"failed", "canceled", "blocked"}
                    else "missing"
                )
                out.append(
                    {
                        "scope": scope,
                        "status": status,
                        "last_outcome": record.get("last_outcome"),
                        "last_attempt": record.get("last_attempt"),
                    }
                )
        return {"asset": asset, "partitions": out}

    # -- runs -------------------------------------------------------------------

    @app.post("/api/projects/{p}/runs", status_code=201)
    async def submit_run(
        p: str, body: RunInput, request: Request, idempotency_key: str | None = Header(default=None)
    ):
        runtime = await project_engine(request, p)
        run = await runtime.submit(**body.model_dump(), command_id=idempotency_key)
        return run or {"status": "skipped-active"}

    @app.get("/api/projects/{p}/runs")
    async def runs(p: str, request: Request, limit: int = Query(default=50, ge=1, le=500)):
        runtime = await project_engine(request, p)
        return {"runs": await runtime.list_runs(limit)}

    @app.get("/api/projects/{p}/runs/{run_id}")
    async def run_detail(p: str, run_id: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.run_detail(run_id)

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

    @app.get("/api/projects/{p}/attempts/{attempt_id:path}/logs")
    async def attempt_logs(p: str, attempt_id: str, request: Request):
        runtime = await project_engine(request, p)
        prefix = f"logs/{attempt_id}/"

        async def stream():
            for key in await runtime.state.list_objects(prefix):
                data = await runtime.state.get_object(key)
                if data:
                    yield data

        return StreamingResponse(stream(), media_type="application/x-ndjson")

    @app.get("/api/projects/{p}/attempts/{attempt_id:path}/spec")
    async def attempt_spec(p: str, attempt_id: str, request: Request):
        runtime = await project_engine(request, p)
        data = await runtime.state.get_object(f"specs/{attempt_id}.json")
        if data is None:
            raise KeyError(attempt_id)
        return json.loads(data)

    @app.get("/api/projects/{p}/attempts/{attempt_id:path}/result")
    async def attempt_result(p: str, attempt_id: str, request: Request):
        runtime = await project_engine(request, p)
        data = await runtime.state.get_object(f"results/{attempt_id}.json")
        if data is None:
            raise KeyError(attempt_id)
        return json.loads(data)

    # -- automations -------------------------------------------------------------

    @app.get("/api/projects/{p}/automations")
    async def automations(p: str, request: Request):
        runtime = await project_engine(request, p)
        return {"automations": [a for _, a in await runtime.state.scan("automation/")]}

    @app.post("/api/projects/{p}/automations/{name}/enable")
    async def automation_enable(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.set_automation(name, True)

    @app.post("/api/projects/{p}/automations/{name}/disable")
    async def automation_disable(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.set_automation(name, False)

    @app.post("/api/projects/{p}/automations/{name}/run-now", status_code=202)
    async def automation_run_now(p: str, name: str, request: Request):
        runtime = await project_engine(request, p)
        return await runtime.run_automation(name)

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
        )

    # -- environments + workers -----------------------------------------------------

    @app.get("/api/projects/{p}/environments")
    async def environments(p: str, request: Request):
        runtime = await project_engine(request, p)
        seen = {}
        for info in runtime.manifest["assets"].values():
            spec = info["placement"]
            key = runtime.registry.env_key(spec)
            placement = runtime.registry.build(spec)
            seen[key] = {
                "key": key,
                "kind": spec["kind"],
                "environment": spec["environment"],
                "max_concurrent": getattr(placement, "max_concurrent", None),
                "in_flight": runtime.env_inflight.get(key, 0),
            }
        return {"environments": list(seen.values())}

    @app.get("/api/projects/{p}/workers")
    async def workers(p: str, request: Request):
        runtime = await project_engine(request, p)
        async with runtime.state.transaction() as tx:
            claimed = {}
            for _, task in await tx.pool_tasks():
                if task["status"] == "claimed" and task["claimed_by"]:
                    claimed[task["claimed_by"]] = task["attempt"]
        return {
            "workers": [{**w, "task": claimed.get(w["id"])} for _, w in await runtime.state.list_workers()]
        }

    # -- worker pull path (§10) -------------------------------------------------------

    @app.post("/api/workers/register", status_code=201)
    async def register_worker(request: Request):
        body = await request.json()
        state = engine_of(request).state
        worker_id = uuid.uuid4().hex
        await state.register_worker(worker_id, body.get("pools") or [], body.get("capacity") or {})
        return {"worker": worker_id}

    @app.post("/api/tasks/claim")
    async def claim_task(request: Request):
        body = await request.json()
        state = engine_of(request).state
        worker = await state.get_worker(body["worker"])
        if worker is None:
            raise KeyError(body["worker"])
        task = await state.claim_pool_task(
            body["worker"],
            worker["pools"],
            body.get("capacity") or {},
            lease_seconds=float(body.get("lease_seconds") or 30),
        )
        if task is None:
            return Response(status_code=204)
        return {
            "task": task["attempt"],
            "stage": {"attempt": task["attempt"], "objects": state.objects_url},
            "lease_seconds": 30,
        }

    @app.post("/api/tasks/{task_id:path}/renew")
    async def renew_task(task_id: str, request: Request):
        body = await request.json()
        await engine_of(request).state.heartbeat_pool_task(body["worker"], task_id, lease_seconds=30)
        return {"ok": True}

    @app.post("/api/tasks/{task_id:path}/complete")
    async def complete_task(task_id: str, request: Request):
        body = await request.json()
        state = engine_of(request).state
        task = await state.get_pool_task(task_id)
        if task is None or task["claimed_by"] != body["worker"]:
            raise Conflict(f"pool task {task_id} is not claimed by {body['worker']}")
        await state.release_pool_task(body["worker"], task_id)
        return {"ok": True}

    # -- console -----------------------------------------------------------------------
    # The SPA builds with base=/static/, so client routes like /static/assets
    # collide with the bundle path: serve real files, fall back to index.html.

    @app.get("/")
    async def index():
        return FileResponse(web / "index.html")

    @app.get("/{path:path}")
    async def console(path: str):
        if path.startswith("api/"):
            raise KeyError(path)
        # The SPA builds with base=/static/: bundle files live under
        # web/assets/…, while client routes (/static/assets, /static/runs) are
        # pathnames that happen to share the prefix — files win, then the SPA.
        bundle = path[7:] if path.startswith("static/") else path
        target = (web / bundle).resolve()
        if target.is_file() and web.resolve() in target.parents:
            return FileResponse(target)
        if "." in Path(path).name:
            raise KeyError(path)
        return FileResponse(web / "index.html")

    return app
