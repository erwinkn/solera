"""Code that does not load (reloading.py): a `solera serve` whose project
fails to import records why before it takes the namespace over, so the
server it was to replace keeps running and shows it; code that loads
clears it."""

import httpx
import pytest
from solera_server import reloading
from solera_server.api import create_app
from solera_server.state import open_store

GOOD = """
from solera import Project, asset


@asset
def numbers():
    return [1]


project = Project(assets=[numbers])
"""

BROKEN = GOOD.replace("Project(assets=[numbers])", "Project(assets=[numbers, numbers])")


def app(tmp_path, source: str, name: str):
    (tmp_path / f"{name}.py").write_text(source)
    state = (tmp_path / "state").as_uri()
    return create_app(state_url=state, namespace="test", project=str(tmp_path / f"{name}.py"), insecure=True)


async def test_a_server_whose_project_does_not_load_leaves_the_running_one_serving_and_showing_why(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SOLERA_DATA_URL", (tmp_path / "data").as_uri())
    running = app(tmp_path, GOOD, "good")
    async with running.router.lifespan_context(running):
        broken = app(tmp_path, BROKEN, "broken")
        with pytest.raises(Exception, match="Duplicate asset: numbers"):
            async with broken.router.lifespan_context(broken):
                pass
        transport = httpx.ASGITransport(app=running)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            diagnostics = (await client.get("/api/diagnostics")).json()
            error = diagnostics["load_error"]["error"]
            assert error.startswith("Traceback") and "Duplicate asset: numbers" in error
            assert "solera_server" not in error.splitlines()[1]  # from the project's own code on
            assert diagnostics["load_error"]["at"] > 0
            run = await client.post("/api/projects/default/runs", json={"targets": ["numbers"]})
            assert run.status_code == 201, run.text  # still the writer: the failed server took nothing over

    fixed = app(tmp_path, GOOD, "fixed")
    async with fixed.router.lifespan_context(fixed):
        assert await reloading.load_error(open_store((tmp_path / "state").as_uri(), "test")[0]) is None
