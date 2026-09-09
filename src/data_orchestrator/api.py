from __future__ import annotations

import asyncio
import os
import secrets
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from .database import Database
from .sdk import DailyPartitions
from .service import Service
from .worker import Worker


class RunBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    targets: list[str] = Field(min_length=1, max_length=100)
    partitions: list[str] = Field(default_factory=list, max_length=366)
    mode: Literal["incremental", "recompute", "fill_missing"] = "incremental"
    include_upstream: bool = True
    idempotency_key: str | None = Field(default=None, max_length=256)


class BackfillBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    asset: str
    start: str
    end: str
    mode: Literal["incremental", "recompute", "fill_missing"] = "fill_missing"
    include_upstream: bool = True


class EnabledBody(BaseModel):
    enabled: bool


class PausedBody(BaseModel):
    paused: bool


def create_app(
    database: Database | None = None,
    *,
    with_worker: bool = False,
    concurrency: int = 4,
    api_token: str | None = None,
) -> FastAPI:
    db = database or Database()
    service = Service(db)
    token = api_token if api_token is not None else os.getenv("DORC_API_TOKEN", "")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        stop = threading.Event()
        thread = (
            threading.Thread(
                target=Worker(db, concurrency=concurrency).run, args=(stop,), daemon=True
            )
            if with_worker
            else None
        )
        if thread:
            thread.start()
        yield
        if thread:
            stop.set()
            await asyncio.to_thread(thread.join, 10)

    app = FastAPI(
        title="Data Orchestrator",
        version="0.1.0a1",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url="/api/openapi.json",
    )

    @app.middleware("http")
    async def security(request: Request, call_next: Any):
        if request.url.path.startswith("/api/"):
            supplied = request.headers.get("authorization", "")
            if token and not secrets.compare_digest(supplied, f"Bearer {token}"):
                return JSONResponse({"detail": "Authentication required"}, status_code=401)
            if request.method not in {"GET", "HEAD", "OPTIONS"}:
                origin = request.headers.get("origin")
                expected = f"{request.url.scheme}://{request.headers.get('host')}"
                if origin and origin != expected:
                    return JSONResponse(
                        {"detail": "Cross-origin writes are not permitted"}, status_code=403
                    )
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; style-src 'self' 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'"
        )
        return response

    @app.exception_handler(ValueError)
    async def invalid(request: Request, error: ValueError):
        return JSONResponse({"detail": str(error)}, status_code=422)

    @app.exception_handler(KeyError)
    async def missing(request: Request, error: KeyError):
        return JSONResponse({"detail": "Resource not found"}, status_code=404)

    @app.get("/healthz")
    def health():
        with db.connect() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok", "version": "0.1.0a1"}

    @app.get("/api/catalog")
    def catalog():
        return service.catalog()

    @app.get("/api/assets/{key}")
    def asset(key: str, partition: str | None = None):
        return service.asset(key, partition)

    @app.get("/api/runs")
    def runs(
        limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0), backfills: bool = False
    ):
        return service.runs(limit=limit, offset=offset, backfills=backfills)

    @app.post("/api/runs", status_code=202)
    def submit(body: RunBody):
        return {"id": db.submit(**body.model_dump())}

    @app.post("/api/backfills", status_code=202)
    def backfill(body: BackfillBody):
        catalog = service.catalog()
        asset = next((a for a in catalog["assets"] if a["key"] == body.asset), None)
        if not asset or not asset["partitions"]:
            raise ValueError("Backfills require a partitioned target")
        partitions = DailyPartitions(**asset["partitions"]).keys(body.start, body.end)
        return {
            "id": db.submit(
                [body.asset],
                partitions=partitions,
                mode=body.mode,
                include_upstream=body.include_upstream,
                reason="backfill",
            )
        }

    @app.get("/api/runs/{request_id}")
    def run(request_id: UUID):
        return service.run(str(request_id))

    @app.post("/api/runs/{request_id}/cancel")
    def cancel(request_id: UUID):
        service.cancel(str(request_id))
        return {"ok": True}

    @app.post("/api/runs/{request_id}/pause")
    def pause(request_id: UUID, body: PausedBody):
        service.pause(str(request_id), body.paused)
        return {"ok": True}

    @app.post("/api/runs/{request_id}/repair", status_code=202)
    def repair(request_id: UUID):
        return {"id": service.repair(str(request_id))}

    @app.get("/api/automations")
    def automations():
        return service.automations()

    @app.post("/api/automations/{name}")
    def enable(name: str, body: EnabledBody):
        service.set_automation(name, body.enabled)
        return {"ok": True}

    @app.post("/api/automations/{name}/run", status_code=202)
    def run_automation(name: str):
        automation = next((a for a in service.automations() if a["name"] == name), None)
        if not automation:
            raise HTTPException(404, "Automation not found")
        return {
            "id": db.submit(
                automation["definition"]["targets"],
                include_upstream=automation["definition"]["trigger"]["kind"] != "commit",
                reason=f"manual:{name}",
            )
        }

    web = Path(__file__).with_name("web")
    if web.is_dir():
        app.mount("/", StaticFiles(directory=web, html=True), name="ui")
    else:

        @app.get("/")
        def ui_unbuilt():
            return JSONResponse(
                {"detail": "Build the UI: cd ui && npm ci && npm run build"}, status_code=503
            )

    return app
