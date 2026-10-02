"""Lineage of a store that reads the current rows (docs/stores.md, "What a
read sees"): an attempt's reads of PostgresStore see one moment, and say
which generation's write they saw. Lineage records the pin's generation and
the one read: exact when they agree; when a newer write landed, the
generation read and, for a page of keys, the versions read. Skips unless
SOLERA_TEST_DATABASE_URL points at a scratch database."""

import os
import uuid

import pytest
from solera.sdk import In, Incremental, Output, Project, Ref, asset
from solera.stores import Scope, prepare_for
from solera_server.engine import Engine
from solera_server.placements.inline import InlinePlacement
from solera_server.state import State

pytestmark = pytest.mark.postgres

DSN = os.environ.get("SOLERA_TEST_DATABASE_URL")


class Clock:
    def __init__(self):
        self.now = 1_800_000_000.0

    def __call__(self):
        return self.now


@pytest.fixture
async def state(tmp_path):
    opened = await State.open(tmp_path.as_uri(), "test", clock=Clock(), flush_interval=0.001)
    yield opened
    await opened.close()


async def run(engine, targets, **kw):
    engine.clock.now += 60
    detail = await engine.run_until((await engine.submit(targets, **kw))["id"], 60)
    assert detail["request"]["status"] == "succeeded", detail
    return detail


async def edges(engine, state, output):
    version = state.model.heads[(output, "")]["ref"]["version"]
    return {e["param"]: e["read"] for e in (await engine.history.lineage(output, "", version))["edges"]}


async def test_lineage_says_what_a_current_read_saw(state):
    if not DSN:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    from solera_postgres import PostgresStore

    store = PostgresStore(DSN)
    name = f"sites_{uuid.uuid4().hex[:8]}"
    out = Output(name, key="id", store="postgres", columns={"id": "text", "v": "text"})
    content = {"rows": [{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]}

    @asset(outputs=out)
    def sites():
        return content["rows"]

    @asset(inputs={"sites": In(name)})
    def report(sites: list[dict]):
        return {"seen": sorted(r["v"] for r in sites)}

    @asset(inputs={"sites": Incremental(name)})
    def changes(sites: list[dict]):
        return {"seen": sorted(r["v"] for r in sites)}

    project = Project(assets=[sites, report, changes], stores={"postgres": store})
    engine = Engine(
        state,
        project.manifest,
        placements={"Local": lambda s, c: InlinePlacement(c, project)},
        clock=state.clock,
        eval_interval=0.01,
    )
    await engine.initialize()

    # Read as pinned: exact, at the generation that wrote the head.
    await run(engine, ["report", "changes"], upstream=True)
    pinned = state.model.heads[(name, "")]["generation"]
    assert (await edges(engine, state, "report")) == {"sites": {"exact": True, "generation": pinned}}

    # sites commits b=2; then a newer writer's b=3 lands before the readers read,
    # uncommitted (a retry under way, or one that died after its write).
    content["rows"] = [{"id": "a", "v": "1"}, {"id": "b", "v": "2"}]
    await run(engine, ["sites"])
    pinned = state.model.heads[(name, "")]["generation"]
    newer = pinned + 1_000
    head = state.model.heads[(name, "")]["ref"]
    landed = [{"id": "a", "v": "1"}, {"id": "b", "v": "3"}]
    await store.store(
        landed,
        Ref.from_json(head),
        Scope(output=out, partition="", attempt="x", generation=newer, invocation="x"),
    )
    await run(engine, ["report", "changes"])

    # A whole read: the generation it saw, which committed nothing yet; no key list.
    assert (await edges(engine, state, "report")) == {
        "sites": {"exact": False, "pinned_generation": pinned, "generation": newer, "version": None}
    }
    # A page of keys (b changed): the versions it read, as the store versions rows.
    read = (await edges(engine, state, "changes"))["sites"]
    assert read["generation"] == newer and read["pinned_generation"] == pinned
    versions = dict(prepare_for(store, [{"id": "b", "v": "3"}], out).entries())
    assert read["keys"] == {"b": versions["b"].hex()}
