import asyncio
import sys

import httpx
import pytest

from data_orchestrator import Project, asset
from data_orchestrator.api import create_app
from data_orchestrator.execution import LocalSubprocess


async def test_authentication_validation_and_live_api(make_engine):
    @asset
    def demo():
        return [{"value": 7}]

    engine = await make_engine(Project([demo]))
    app = create_app(engine=engine, token="secret-token")
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get("/")).status_code == 200
            assert (await client.get("/api/state")).status_code == 401
            assert (await client.get("/openapi.json")).status_code == 404
            client.headers["Authorization"] = "Bearer secret-token"
            assert (await client.get("/api/state")).json()["storage"]["engine"] == "SlateDB"
            assert (await client.post("/api/runs", json={"targets": []})).status_code == 422
            assert (await client.post("/api/runs", json={"targets": ["unknown"]})).status_code == 400
            response = await client.post(
                "/api/runs", json={"targets": ["demo"]}, headers={"Idempotency-Key": "api-run"}
            )
            assert response.status_code == 201
            run_id = response.json()["id"]
            repeat = await client.post(
                "/api/runs", json={"targets": ["demo"]}, headers={"Idempotency-Key": "api-run"}
            )
            assert repeat.json()["id"] == run_id
            for _ in range(100):
                detail = (await client.get("/api/runs/" + run_id)).json()
                if detail["request"]["status"] == "succeeded":
                    break
                await asyncio.sleep(0.03)
            assert detail["request"]["status"] == "succeeded"
            asset_response = await client.get("/api/assets/demo")
            assert asset_response.json()["preview"] == [{"value": 7}]
            assert "no-store" in asset_response.headers["cache-control"]
            assert "script-src 'self'" in asset_response.headers["content-security-policy"]
            engine.state.poisoned = True
            assert (await client.get("/healthz")).status_code == 503
            engine.state.poisoned = False


async def test_api_requires_explicit_authentication_choice():
    app = create_app()
    with pytest.raises(ValueError, match="DORC_API_TOKEN"):
        async with app.router.lifespan_context(app):
            pass


async def test_subprocess_rejects_wrong_revision():
    backend = LocalSubprocess("data_orchestrator.demo:project")
    with pytest.raises(RuntimeError, match="Code revision changed"):
        await backend.execute({"revision": "wrong", "producer": "source_files", "inputs": {}, "context": {}})


async def test_public_insecure_cli_is_rejected():
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "data_orchestrator.cli",
        "serve",
        "--host",
        "0.0.0.0",
        "--insecure",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await process.communicate()
    assert process.returncode == 2
    assert b"limited to loopback" in stderr
