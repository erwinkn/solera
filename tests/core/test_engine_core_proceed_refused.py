"""An operator's proceed is refused on the new tracking core until ERW-42 (review round 4: the
fact "this partition holds content read at a proceeded head" has no owner yet). No `Proceeded`
is journaled and no span is ever proceeded, so a reader of a partial head holds until a run of
its producer completes it."""

import asyncio

import pytest
from solera.sdk import In, Incremental, Output, Project, Retry, asset
from solera.stores import FileStore
from solera_server.tracking import full

from tests.contract.harness import Solera, rebuild


@pytest.fixture
async def solera(tmp_path):
    s = Solera(tmp_path, "core")
    yield s
    await s.close()


async def test_proceed_is_refused_and_readers_hold_until_the_run_completes(solera):
    hold, waiting, seen, types = asyncio.Event(), [False], [], []
    hold.set()

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k} for k in "abcd"]

    @asset(inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"), retries=Retry(n=0))
    async def copy(ctx, files: list):
        if ctx.batch["files"].index > 0:
            waiting[0] = True
            await hold.wait()
        return rebuild(ctx.batch["files"], files)

    @asset(inputs={"copy": In()}, retries=Retry(n=0))
    def report(copy: list):
        seen.append(sorted(r["id"] for r in copy))
        return len(copy)

    await solera.deploy(
        Project(assets=[files, copy, report], default_store=FileStore(solera.root / "data")),
        cancel_grace=0.2,
        heartbeat_seconds=0.1,
    )
    eng = solera.engine
    apply = eng.m.apply

    def spy(e):
        types.append(e.get("type"))
        return apply(e)

    eng.m.apply = spy
    await solera.run("files")
    hold.clear()
    stopped = await solera.submit("copy", mode="full")
    await solera.until(lambda: waiting[0])
    await solera.cancel(stopped["id"])
    await solera.wait(stopped)
    hold.set()
    assert full.stopped(eng) == [{"output": "copy", "partition": "", "asset": "copy"}]
    with pytest.raises(full.ProceedNotYet, match="ERW-42"):
        await eng.proceed("copy", "", "test")
    response = await solera.request("POST", "/assets/copy/proceed", ok=False, json={})
    assert response.status_code == 409 and "ERW-42" in response.text, response.text
    reader = await solera.submit("report")
    with pytest.raises(TimeoutError):
        await solera.wait(reader, 1.5)
    assert not seen, seen
    await solera.run("copy")  # the run completes copy: report goes on and reads it whole
    await solera.wait(reader)
    assert seen == [["a", "b", "c", "d"]], seen
    assert "Proceeded" not in types, types
    assert not any(flag for spans in eng.m.partial_spans.values() for _, _, flag in spans), (
        eng.m.partial_spans
    )
