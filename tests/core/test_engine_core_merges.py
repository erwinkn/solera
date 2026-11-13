"""Merges on the engine (`tracking.merge`): an output's layer list stays
short however many commits it holds, its readers get what they got before,
and the files a merge replaced are deleted once nothing reads them."""

import asyncio
import os

import pytest
from solera.sdk import Incremental, Output, Project, asset
from solera.stores import FileStore, Patch
from solera_server.model import output_id
from solera_server.tracking import merge

from tests.contract.harness import Solera


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path)
    yield namespace
    await namespace.close()


async def merged(solera) -> None:
    """Tick until no merge is due or running, then collect the garbage."""

    engine = solera.engine
    while True:
        await engine.tick()
        job = engine.upkeep.jobs.get(merge.JOB)
        if job is None:
            break
        await asyncio.wait([job])
    await engine.upkeep.collect()


def layers(solera, output: str) -> list[dict]:
    return solera.engine.m.core.output(output_id(output, ""))["layers"]


def layer_files(solera) -> set[str]:
    root = solera.root / "state" if (solera.root / "state").exists() else solera.root
    return {
        os.path.relpath(os.path.join(folder, name), start)
        for start in [next(p for p, _, _ in os.walk(root) if os.path.isdir(os.path.join(p, "core")))]
        for folder, _, names in os.walk(os.path.join(start, "core"))
        for name in names
    }


async def test_a_log_of_many_commits_is_packed_into_a_few_layers(solera):
    """Twelve commits make twelve layers; merges leave a few, holding the
    same rows in the same order, and a reader paused at commit 5 still gets
    commits 6 to 12 and nothing else."""

    n, seen = [0], []

    @asset(outputs=Output("events", incremental=True))
    def events():
        n[0] += 1
        return Patch([{"n": n[0]}])

    @asset(inputs={"events": Incremental()})
    def digest(ctx, events: list):
        seen.append([e["n"] for e in events])
        return len(events)

    await solera.deploy(Project(assets=[events, digest], default_store=FileStore(solera.root / "data")))
    for _ in range(5):
        await solera.run("events")
    await solera.run("digest")
    for _ in range(7):
        await solera.run("events")
    await merged(solera)
    held = layers(solera, "events")
    assert len(held) < 6 and (held[0]["lo"], held[-1]["hi"]) == (1, 12)
    assert layer_files(solera) >= {layer["path"] for layer in held}
    assert sum(path.split("/")[1] == held[0]["path"].split("/")[1] for path in layer_files(solera)) == len(
        held
    )
    assert [row["n"] for row in await solera.read("events")] == list(range(1, 13))
    await solera.run("digest")
    assert seen == [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10, 11, 12]]


async def test_a_keyed_output_s_history_survives_its_merges(solera):
    """`files` commits twelve patches. `copy` read it at commit 4: after the
    merges it gets exactly the keys changed since, one of them removed."""

    turn, seen = [0], []

    @asset(outputs=Output("files", key="id"))
    def files():
        t = turn[0]
        return Patch([{"id": f"k{t}", "v": t}], remove=["k1"] if t == 8 else [])

    @asset(inputs={"files": Incremental()})
    def copy(ctx, files: list):
        b = ctx.batch["files"]
        seen.append((sorted(b.added), sorted(b.removed)))
        return len(files)

    await solera.deploy(Project(assets=[files, copy], default_store=FileStore(solera.root / "data")))
    for turn[0] in range(4):
        await solera.run("files")
    await solera.run("copy")
    for turn[0] in range(4, 12):
        await solera.run("files")
    await merged(solera)
    assert len(layers(solera, "files")) < 6
    assert sorted(await solera.keys("files")) == sorted(f"k{t}" for t in range(12) if t != 1)
    await solera.run("copy")
    assert seen == [
        (["k0", "k1", "k2", "k3"], []),
        ([f"k{t}" for t in ("10", "11", 4, 5, 6, 7, 8, 9)], ["k1"]),
    ]


async def test_replaced_layers_outlive_whoever_was_reading_them(solera):
    """A merge lets go of the layers it replaced, and they are deleted only
    once no reader pinned before it is left: an engine read under way, or an
    attempt that reads the output, which pins the output's layer files."""

    @asset(outputs=Output("events", incremental=True))
    def events():
        return Patch([{"n": 1}])

    await solera.deploy(Project(assets=[events], default_store=FileStore(solera.root / "data")))
    for _ in range(8):
        await solera.run("events")
    engine = solera.engine
    before = {layer["path"] for layer in layers(solera, "events")}
    with engine.m.reading():
        await merged(solera)
        assert len(layers(solera, "events")) < 8
        assert layer_files(solera) >= before, "a read that began before the merge may still open them"
    await engine.upkeep.collect()
    assert not layer_files(solera) & (before - {layer["path"] for layer in layers(solera, "events")})
    assert engine.m.garbage == []
    ref = engine.m.heads[("events", "")]["ref"]
    life = engine.m.core.output(output_id("events", ""))["life"]
    assert f"core/{life}/" in engine._prefixes({"events": {"ref": ref}}, {}, {"partition": ""})
