"""A local serve's reload (solera_server.reloading): each new deploy served as
the project's code changes; a burst of saves one deploy; an edit that fails
to load the current deploy kept, the error shown, until a fix."""

import asyncio

from solera_server import reloading
from solera_server.executors.local import load_manifest

PROJECT = """
from helpers import f

from solera.sdk import Project, asset


@asset
def numbers():
    return [f()]


project = Project(assets=[numbers])
"""


class Served:
    """An engine as the reload sees it: what it serves, and what fails."""

    def __init__(self, manifest):
        self.manifest, self.failing, self.deploys = manifest, {}, []

    def redeploy(self, manifest) -> bool:
        if manifest["deploy"] == self.manifest["deploy"]:
            return False
        self.manifest = manifest
        self.deploys.append(manifest["deploy"])
        return True


async def until(predicate, timeout=60.0):
    for _ in range(int(timeout / 0.05)):
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("never")


async def test_a_code_change_is_served_and_a_broken_one_is_not(tmp_path, monkeypatch):
    monkeypatch.delenv("SOLERA_BUILD", raising=False)
    (tmp_path / "project.py").write_text(PROJECT)
    (tmp_path / "helpers.py").write_text("def f(): return 1\n")
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    entry = f"{tmp_path / 'project.py'}:project"
    manifest, files = await load_manifest(entry, watch=True)
    assert sorted(files) == [str(tmp_path / "helpers.py"), str(tmp_path / "project.py")]
    served = Served(manifest)
    task = asyncio.create_task(reloading.reload(served, entry, files, poll=0.05, quiet=0.3))
    try:
        (tmp_path / "notes.md").write_text("beside it: nothing")
        for n in (2, 3, 4):  # a burst of saves: one deploy
            (tmp_path / "helpers.py").write_text(f"def f(): return {n}\n")
            await asyncio.sleep(0.05)
        await until(lambda: served.deploys)
        await asyncio.sleep(0.5)
        assert len(served.deploys) == 1
        (tmp_path / "helpers.py").write_text("def f(: return\n")  # half saved
        await until(lambda: "reload" in served.failing)
        assert "still serving the previous deploy" in served.failing["reload"] and len(served.deploys) == 1
        (tmp_path / "helpers.py").write_text("def f(): return 5\n")
        await until(lambda: len(served.deploys) == 2)
        assert "reload" not in served.failing
    finally:
        task.cancel()


async def test_redeploy_serves_another_deploy_in_place(state, monkeypatch):
    from solera.sdk import Project, asset

    from .engines import make_engine

    monkeypatch.setenv("SOLERA_BUILD", "one")

    @asset
    def numbers():
        return [1]

    engine = make_engine(state, Project(assets=[numbers]))
    await engine.initialize()
    monkeypatch.setenv("SOLERA_BUILD", "two")
    later = Project(assets=[numbers]).manifest
    assert not engine.redeploy(dict(engine.manifest))
    assert engine.redeploy(later) and state.model.deploy == later["deploy"]
    assert engine.upkeep.manifest is later
