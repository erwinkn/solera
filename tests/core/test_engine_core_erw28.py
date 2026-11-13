"""Two incremental runs of one reader at once (ERW-28, both paths): each task walks from its own
progress, so their batches interleave and the reader is delivered a page or a key twice.
Strict xfails until ERW-28 gives the partition's walk one cursor; the random tests' oracle
exemptions for it are marked ERW-28 too."""

import asyncio

import pytest
from solera.sdk import Incremental, Output, Project, Retry, asset
from solera.stores import FileStore

from tests.contract.harness import Solera, rebuild

ERW28 = "ERW-28: two runs of one reader walk from their own progress and read a batch twice"


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path)
    yield namespace
    await namespace.close()


@pytest.mark.parametrize(
    "modes",
    [
        ("full", "full"),
        ("full", "incremental"),
        pytest.param(("incremental", "incremental"), marks=pytest.mark.xfail(strict=True, reason=ERW28)),
    ],
)
async def test_two_runs_of_a_keyed_reader(solera, modes):
    rows, seen = {k: 1 for k in "abcdef"}, []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    @asset(inputs={"files": Incremental(batch_size=2)}, outputs=Output("copy", key="id"), retries=Retry(n=0))
    async def copy(ctx, files: list):
        b = ctx.batch["files"]
        seen.append((ctx.run_id, b.index, b.reset, sorted(r["id"] for r in files)))
        await asyncio.sleep(0.2)
        return rebuild(b, [{"id": r["id"], "v": r["v"]} for r in files])

    await solera.deploy(Project(assets=[files, copy], default_store=FileStore(solera.root / "data")))
    await solera.run("files")
    if modes[1] == "incremental" and modes[0] == "incremental":
        await solera.run("copy")
        rows.update({k: 2 for k in "abcdef"})
        await solera.run("files")
    one = await solera.submit("copy", mode=modes[0])
    two = await solera.submit("copy", mode=modes[1])
    await solera.wait(one, 30)
    await solera.wait(two, 30)
    keys = [k for _, _, _, ks in seen for k in ks]
    print("SEEN", [(r[-4:], i, z, ks) for r, i, z, ks in seen])
    assert sorted(keys) == sorted(set(keys)) or modes[0] == "full", seen
