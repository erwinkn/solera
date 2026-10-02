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
    return {e["param"]: e for e in (await engine.history.lineage(output, "", version))["edges"]}


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
    head = state.model.heads[(name, "")]
    edge = (await edges(engine, state, "report"))["sites"]
    assert edge["from"] == {
        "output": name,
        "scope": "",
        "version": head["ref"]["version"],
        "generation": head["generation"],
        "run": head["run"],
        "attempt": head["attempt"],
        "at": head["at"],
    }
    assert "uncommitted" not in edge and "mixed" not in edge
    assert edge["detail"] == {
        "pinned_version": head["ref"]["version"],
        "pinned_generation": head["generation"],
    }

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

    # A whole read of a write no attempt committed: flagged, no version; the pin in `detail`.
    edge = (await edges(engine, state, "report"))["sites"]
    assert edge["from"]["generation"] == newer and edge["from"]["version"] is None
    assert edge["uncommitted"] == {"attempt": None, "run": None}  # written outside the engine
    assert edge["detail"]["pinned_generation"] == pinned
    # A page of keys (b changed): the versions it read, as the store versions rows.
    edge = (await edges(engine, state, "changes"))["sites"]
    assert edge["from"]["generation"] == newer and "uncommitted" in edge
    versions = dict(prepare_for(store, [{"id": "b", "v": "3"}], out).entries())
    assert edge["from"]["keys"] == {"b": versions["b"].hex()}

    # The writer commits after all: the edge then names the version it committed, and who.
    from solera_server.history import attempt_row

    summary = {"id": "late", "outcome": "succeeded", "started_at": 1.0, "finished_at": 2.0}
    summary |= {"outputs": {name: "v-late"}, "generation": newer}
    task = {"id": "t-late", "asset": "sites", "scope": ""}
    state.model._record("attempts", attempt_row("r-late", task, summary, 1))
    edge = (await edges(engine, state, "report"))["sites"]
    assert "uncommitted" not in edge
    assert {k: edge["from"][k] for k in ("version", "generation", "run", "attempt", "at")} == {
        "version": "v-late",
        "generation": newer,
        "run": "r-late",
        "attempt": "late",
        "at": 2.0,
    }


async def test_a_renamed_postgres_output_stays_readable(state):
    """Review round 5 #1: an asset renamed with `aliases=` that writes the
    same rows commits nothing new, and its head — the old table — must
    still load; a changed write lands in that table too. The engine pins
    the head; the store keeps the table the head names."""

    if not DSN:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    from solera_postgres import PostgresStore

    store = PostgresStore(DSN)
    schema = f"s_{uuid.uuid4().hex[:8]}"
    rows = {"v": [{"id": "a", "v": "1"}]}

    def make(project_assets):
        project = Project(assets=project_assets, stores={"postgres": store})
        return project, Engine(
            state,
            project.manifest,
            placements={"Local": lambda s, c: InlinePlacement(c, project)},
            clock=state.clock,
            eval_interval=0.01,
        )

    def old():
        return rows["v"]

    def new():
        return rows["v"]

    decl = dict(key="id", store="postgres", schema=schema, columns={"id": "text", "v": "text"})
    _, engine = make([asset(outputs=Output(**decl))(old)])
    await engine.initialize()
    await run(engine, ["old"])
    _, engine = make([asset(outputs=Output(**decl), aliases=["old"])(new)])
    await engine.initialize()
    await run(engine, ["new"])  # the same rows: the head stays
    head = Ref.from_json(state.model.heads[("new", "")]["ref"])
    assert await store.load(head, list[dict], None) == [{"id": "a", "v": "1"}]
    rows["v"] = [{"id": "a", "v": "2"}]
    await run(engine, ["new"])
    head = Ref.from_json(state.model.heads[("new", "")]["ref"])
    assert head.table == f'"{schema}"."old"'
    assert await store.load(head, list[dict], None) == [{"id": "a", "v": "2"}]
