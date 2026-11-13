"""A deploy while a skip's commit is built replans the attempt, as a batch's commit does:
no try is spent, so a run with no retries still succeeds (review round 3, Fable)."""

import pytest
from solera.sdk import Incremental, Output, Project, Retry, asset
from solera.stores import FileStore

from tests.contract.harness import Solera, rebuild


@pytest.fixture
async def solera(tmp_path):
    s = Solera(tmp_path)
    yield s
    await s.close()


async def test_a_deploy_while_a_skip_commits_spends_no_try(solera, monkeypatch):
    rows = {"a": 1, "b": 1}

    def project(extra=False):
        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in rows.items()]

        @asset(
            inputs={"files": Incremental(batch_size=10)}, outputs=Output("copy", key="id"), retries=Retry(n=0)
        )
        async def copy(ctx, files: list):
            return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

        assets = [files, copy]
        if extra:

            @asset
            def unrelated():
                return 1

            assets.append(unrelated)
        return Project(assets=assets, default_store=FileStore(solera.root / "data"))

    await solera.deploy(project())
    await solera.run("files")
    await solera.run("copy")
    engine = solera.engine
    new, fired = project(extra=True), []
    run = engine.core.run

    async def interleaved(fn, *args, **kwargs):
        found = await run(fn, *args, **kwargs)
        if fn == engine.core.tracking.progress_ops and not fired:
            fired.append(True)
            engine.redeploy(new.manifest)
            solera.project = new
        return found

    monkeypatch.setattr(engine.core, "run", interleaved)
    detail = await solera.run("copy", expect=None)  # nothing owed: a skip
    tries = [(a["outcome"], a.get("error")) for a in sum(detail["attempts"].values(), [])]
    assert fired, tries
    assert all(o in ("replanned", "skipped") for o, _ in tries), tries
    assert detail["request"]["status"] == "succeeded", (detail["request"]["status"], tries)
