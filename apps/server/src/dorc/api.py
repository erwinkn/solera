from __future__ import annotations

import hmac
import os
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .engine import Conflict, Engine
from .placements.local import load_manifest
from .state import State
from .storage import SlateState, Unavailable


class RunInput(BaseModel):
    targets: list[str] = Field(min_length=1, max_length=100)
    partitions: list[str] = Field(default_factory=list, max_length=1000)
    mode: str = "incremental"
    config: dict = Field(default_factory=dict)


class AutomationInput(BaseModel):
    enabled: bool


def create_app(*, state_url=None, namespace=None, project=None, token=None, insecure=False, engine=None):
    state_url = state_url or os.getenv("DORC_STATE_URL", Path(".dorc").resolve().as_uri())
    namespace = namespace or os.getenv("DORC_NAMESPACE", "default")
    project = project or os.getenv("DORC_PROJECT", "dorc.demo:project")
    token = token or os.getenv("DORC_API_TOKEN")

    @asynccontextmanager
    async def lifespan(app):
        if not token and not insecure:
            raise ValueError("Set DORC_API_TOKEN, or explicitly enable insecure local development")
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
                concurrency=int(os.getenv("DORC_CONCURRENCY", "4")),
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

    @app.get("/healthz")
    async def health(request: Request):
        runtime = request.app.state.engine
        healthy = not runtime.state.poisoned and not runtime.last_error
        return JSONResponse(
            {"status": "ok" if healthy else "unavailable", "backend": "slatedb"},
            status_code=200 if healthy else 503,
        )

    @app.get("/api/state")
    async def state(request: Request):
        runtime = request.app.state.engine
        catalog = await runtime.catalog()
        runs = await runtime.list_runs()
        automations = [a for _, a in await runtime.state.scan("automation/")]
        return {
            "assets": catalog,
            "runs": runs,
            "automations": automations,
            "storage": {
                "engine": "SlateDB",
                "scheme": urlsplit(runtime.state.url).scheme,
                "namespace": runtime.state.namespace,
                "sequence": runtime.state.last_sequence,
                "experimental": True,
            },
            "revision": runtime.manifest["revision"],
        }

    @app.post("/api/runs", status_code=201)
    async def submit(body: RunInput, request: Request, idempotency_key: str | None = Header(default=None)):
        return await request.app.state.engine.submit(**body.model_dump(), command_id=idempotency_key)

    @app.get("/api/runs/{run_id}")
    async def run(run_id: str, request: Request):
        return await request.app.state.engine.run_detail(run_id)

    @app.post("/api/runs/{run_id}/cancel")
    async def cancel(run_id: str, request: Request):
        return await request.app.state.engine.cancel(run_id)

    @app.post("/api/runs/{run_id}/retry")
    async def retry(run_id: str, request: Request):
        return await request.app.state.engine.retry(run_id)

    @app.post("/api/runs/{run_id}/pause")
    async def pause(run_id: str, request: Request):
        return await request.app.state.engine.pause(run_id)

    @app.post("/api/runs/{run_id}/resume")
    async def resume(run_id: str, request: Request):
        return await request.app.state.engine.pause(run_id, False)

    @app.get("/api/assets/{name}")
    async def asset_detail(name: str, request: Request, partition: str = Query(default="", max_length=10)):
        return await request.app.state.engine.asset_detail(name, partition)

    @app.post("/api/automations/{name}")
    async def automation(name: str, body: AutomationInput, request: Request):
        return await request.app.state.engine.set_automation(name, body.enabled)

    @app.post("/api/automations/{name}/run", status_code=202)
    async def run_automation(name: str, request: Request):
        return await request.app.state.engine.run_automation(name)

    app.mount("/static", StaticFiles(directory=web), name="static")

    @app.get("/")
    async def index():
        return FileResponse(web / "index.html")

    @app.get("/{path:path}")
    async def console(path: str):
        # Client-side routes (assets, runs, automations, storage) share the shell.
        target = web / path
        if path and ".." not in path and target.is_file():
            return FileResponse(target)
        if path.startswith("api/") or "." in Path(path).name:
            raise KeyError(path)
        return FileResponse(web / "index.html")

    return app
