"""An attempt's sidecar on the new tracking core (ERW-30): its spec names
the plan and the layers of the keyed outputs it reads, never carries them.
The engine writes them in one object beside the spec, the worker reads it
once, an engine that adopts the attempt reads its plan back from it, and it
goes once the attempt's end is durable."""

import json

import pytest
from solera import lifecycle
from solera.executors import Pool
from solera.sdk import In, Incremental, Output, Project, Retry, Source, asset
from solera.stores import FileStore

from tests.contract.harness import Outside, Solera, SourceStore, rebuild


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path, "core")
    yield namespace
    await namespace.close()


def _project(root, outside: Outside, seen: list, executor=None) -> Project:
    """`items`, keyed and incremental over `feed`, loads its own output
    whole; `total` reads `items` whole: both name their objects from the
    output's layers."""

    @asset(
        inputs={"feed": Incremental(batch_size=100)},
        outputs=Output("items", key="id"),
        retries=Retry(n=0),
        **({"executor": executor} if executor is not None else {}),
    )
    async def items(ctx, feed: list):
        held = await ctx.load()
        seen.append(sorted(r["id"] for r in held or ()))
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(inputs={"items": In()}, outputs=Output("total"))
    def total(items: list):
        return {"ids": sorted(r["id"] for r in items)}

    @asset(outputs=Output("plain"))
    def plain():
        return {"n": 1}

    return Project(
        assets=[items, total, plain],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(root / "ext", outside)},
        default_store=FileStore(root / "data"),
    )


async def _feed(solera, outside: Outside, upserts: dict) -> None:
    outside.feed.update(upserts)
    await solera.commit_source("feed", upsert=upserts)


def _written(solera) -> dict[str, bytes]:
    """Every object the engine creates from now on, by key."""

    written, create = {}, solera.state.create_object

    async def spy(key, value):
        written[key] = value
        return await create(key, value)

    solera.state.create_object = spy
    return written


async def _no_sidecar(solera) -> bool:
    return not any(p.endswith(lifecycle.SIDECAR) for p in await solera.state.list_objects("runs/"))


async def test_a_spec_names_its_plan_and_layers_and_the_worker_reads_them_from_the_sidecar(solera, tmp_path):
    outside, seen = Outside(), []
    await solera.deploy(_project(tmp_path, outside, seen))
    first = {f"k{n:03d}": "1" for n in range(30)}
    await _feed(solera, outside, first)
    await solera.run("items")
    written = _written(solera)
    await _feed(solera, outside, {"k100": "2"})
    await solera.run("items")
    await solera.run("total")

    specs = [json.loads(v) for k, v in written.items() if k.endswith(lifecycle.SPEC)]
    by_asset = {spec["asset"]: spec for spec in specs}
    for spec in specs:
        assert spec["tracking"] == "core" and "plan" not in spec
        assert '"layers"' not in json.dumps(spec), spec  # an output's layers: its id and head alone
    items, total = by_asset["items"], by_asset["total"]
    plan, layers = lifecycle.read_sidecar(written[items["sidecar"]])
    assert plan is not None and b"k100" in plan and "k100" not in json.dumps(items)
    pin = items["outputs"]["items"]["core"]
    assert set(pin) == {"url", "id", "head"} and layers[(pin["id"], pin["head"])]
    plan, layers = lifecycle.read_sidecar(written[total["sidecar"]])
    pin = total["inputs"]["items"]["index"]["core"]
    assert plan is None and list(layers) == [(pin["id"], pin["head"])]
    # The worker resolved both: the second batch loaded what the first wrote, `total` every key.
    assert seen == [[], sorted(first)]
    assert await solera.read("total") == {"ids": sorted([*first, "k100"])}
    await solera.until(lambda: _no_sidecar(solera))


async def test_an_attempt_that_reads_no_keyed_output_writes_no_sidecar(solera, tmp_path):
    await solera.deploy(_project(tmp_path, Outside(), []))
    written = _written(solera)
    await solera.run("plain")
    [spec] = [json.loads(v) for k, v in written.items() if k.endswith(lifecycle.SPEC)]
    assert spec["tracking"] == "core" and "sidecar" not in spec
    assert not any(k.endswith(lifecycle.SIDECAR) for k in written)


async def test_an_engine_restarted_mid_attempt_commits_it_from_its_sidecar(solera, tmp_path):
    outside, seen = Outside(), []
    await solera.deploy(_project(tmp_path, outside, seen, Pool("gpu")()))
    await _feed(solera, outside, {"k1": "1", "k2": "1"})
    run = await solera.submit("items")
    [stage] = await solera.offered("gpu")
    assert await solera.worker(stage) == 0
    await solera.wait(run)
    await _feed(solera, outside, {"k3": "1"})
    run = await solera.submit("items")
    [stage] = await solera.offered("gpu")
    assert stage["attempt"] in solera.engine.core.plans
    await solera.down()
    assert await solera.worker(stage) == 0  # through the object store alone: its sidecar included
    await solera.restart()
    assert solera.engine.core.plans == {}  # the plan is the sidecar's alone now
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert seen == [[], ["k1", "k2"]]
    assert await solera.rows("items") == {"k1": "1", "k2": "1", "k3": "1"}
    await solera.until(lambda: _no_sidecar(solera))
