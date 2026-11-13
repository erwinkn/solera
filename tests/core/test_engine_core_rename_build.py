"""A rename that lands while a batch's progress is built (the first await of commit.build,
before the outputs' ids are read) makes `Core.named` mint a new life under the old name:
`commit.named` then sees the name map to the id it built, and the batch commits its keys to
an orphan life while its edge records them as processed (round 2, Fable; R2's fix covers a
rename after the ids are read only)."""

import pytest
from solera.sdk import In, Incremental, Output, Project, Retry, asset
from solera.stores import FileStore
from solera_server.executors.inline import InlinePlacement

from tests.contract.harness import Solera, rebuild


@pytest.fixture
async def solera(tmp_path):
    s = Solera(tmp_path)
    yield s
    await s.close()


async def test_a_rename_while_a_batchs_progress_is_built_loses_no_key(solera, monkeypatch):
    rows = {"a": 1}

    def project(name, aliases=()):
        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in rows.items()]

        async def body(ctx, files: list):
            return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

        body.__name__ = body.__qualname__ = name
        made = asset(
            inputs={"files": Incremental(batch_size=10)},
            outputs=Output(key="id"),
            aliases=list(aliases),
            retries=Retry(n=0),
        )(body)

        @asset(inputs={"up": In(name)})
        def report(up: list):
            return sorted(r["id"] for r in up)

        return Project(assets=[files, made, report], default_store=FileStore(solera.root / "data"))

    await solera.deploy(project("old"))
    await solera.run("files")
    await solera.run("old")
    rows["b"] = 1
    await solera.run("files")
    engine = solera.engine
    run, work = engine.core.run, InlinePlacement._work
    new, renamed = project("new", ["old"]), []

    async def interleaved(fn, *args, **kwargs):
        found = await run(fn, *args, **kwargs)
        if fn == engine.core.tracking.progress_ops and not renamed:
            renamed.append(True)
            engine.redeploy(new.manifest)
            solera.project = new
        return found

    def current_worker(self, stage):
        if engine.manifest["deploy"] == new.manifest["deploy"]:
            self.project = new
        return work(self, stage)

    monkeypatch.setattr(engine.core, "run", interleaved)
    monkeypatch.setattr(InlinePlacement, "_work", current_worker)
    detail = await solera.run("old", expect=None)
    outcomes = [a["outcome"] for a in sum(detail["attempts"].values(), [])]
    assert renamed
    m = engine.m
    state = {
        "outcomes": outcomes,
        "core ids": {k: v for k, v in m.core_outputs.items() if k[0] in ("old", "new")},
        "head": m.heads["new", ""].get("commit_number"),
        "core head": m.core_output("new", "")["head"],
        "new holds": sorted(await solera.rows("new")),
        "stale": await solera.stale("new"),
    }
    await solera.run("new", expect=None)  # anything owed is taken now
    await solera.run("report")
    state["after a plain run, new holds"] = sorted(await solera.rows("new"))
    state["report read"] = await solera.read("report")
    assert ("old", "") not in m.core_outputs, f"a life was named under the old name: {state}"
    assert state["after a plain run, new holds"] == ["a", "b"], state
