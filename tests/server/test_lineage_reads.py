"""Lineage of a store that reads the current rows (docs/stores.md, "What a
read sees"; docs/versions.md §6): an attempt's reads of PostgresStore see
one moment, and say which generation's write they saw. Lineage records the
pin's generation and the one read: exact when they agree; when a newer
write landed, the generation read, flagged `uncommitted` until a commit
installs it. Skips unless SOLERA_TEST_DATABASE_URL points at a scratch
database."""

import os
import uuid

import pytest
from solera.sdk import In, Incremental, Output, Project, Ref, Source, asset
from solera.stores import WriteContext
from solera_server.engine import Engine
from solera_server.executors.inline import InlinePlacement
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
    generation = state.model.heads[(output, "")]["ref"]["generation"]
    return {e["param"]: e for e in (await engine.history.lineage(output, "", generation))["edges"]}


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
        "partition": "",
        "generation": head["ref"]["generation"],
        "run": head["run"],
        "attempt": head["attempt"],
        "at": head["at"],
    }
    assert "uncommitted" not in edge
    assert edge["detail"] == {"pinned_generation": head["ref"]["generation"]}

    # sites commits b=2; then a newer writer's b=3 lands before the readers read,
    # uncommitted (a retry under way, or one that died after its write).
    content["rows"] = [{"id": "a", "v": "1"}, {"id": "b", "v": "2"}]
    await run(engine, ["sites"])
    pinned = state.model.heads[(name, "")]["ref"]["generation"]
    newer = pinned + 1_000
    head = state.model.heads[(name, "")]["ref"]
    landed = [{"id": "a", "v": "1"}, {"id": "b", "v": "3"}]
    await store.store(
        landed,
        Ref.from_json(head),
        WriteContext(output=out, partition="", attempt="x", generation=newer, worker_id="x"),
    )
    await run(engine, ["report", "changes"])

    # A read of a write no commit installed: flagged; the pin in `detail`.
    for param in ("report", "changes"):  # a whole read, and a batch of keys
        edge = (await edges(engine, state, param))["sites"]
        assert edge["from"]["generation"] == newer
        assert edge["uncommitted"] == {"attempt": None, "run": None}  # written outside the engine
        assert edge["detail"]["pinned_generation"] == pinned

    # Its attempt is known: the edge names it, still uncommitted.
    from solera_server.history import attempt_row, commit_row

    summary = {"id": "late", "outcome": "failed", "started_at": 1.0, "finished_at": 2.0, "generation": newer}
    state.model._record(
        "attempts", attempt_row("r-late", {"id": "t", "asset": "sites", "partition": ""}, summary, 1)
    )
    edge = (await edges(engine, state, "report"))["sites"]
    assert edge["uncommitted"] == {"attempt": "late", "run": "r-late"}

    # A commit installs that generation (a repair's, say): the edge then names who made it.
    late = {"ref": {**head, "generation": newer}, "run": "r-repair", "attempt": "repair", "at": 3.0}
    state.model._record("commits", commit_row(name, "sites", "", late))
    edge = (await edges(engine, state, "report"))["sites"]
    assert "uncommitted" not in edge
    assert {k: edge["from"][k] for k in ("generation", "run", "attempt", "at")} == {
        "generation": newer,
        "run": "r-repair",
        "attempt": "repair",
        "at": 3.0,
    }


async def test_an_external_tables_lineage_is_its_observation(state):
    """docs/versions.md §6, review finding 4: a source's table is written
    outside Solera — no fence says which write a read saw. Lineage records
    the generation of the tick the read came from, and flags
    nothing: an external source is read as it is now, so what a reader
    loads may be newer than the tick."""

    if not DSN:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    from solera_postgres import PostgresStore

    store = PostgresStore(DSN)
    table = f"ext_{uuid.uuid4().hex[:8]}"
    with store._connect() as conn:
        conn.execute(f'CREATE TABLE "{table}" (id text, v text)')
        conn.execute(f"INSERT INTO \"{table}\" VALUES ('a', '1')")

    @asset(inputs={"ext": In("ext")})
    def report(ext: list[dict]):
        return {"seen": sorted(r["v"] for r in ext)}

    source = Source("ext", store="postgres", table=f'"public"."{table}"', where={})
    project = Project(assets=[report], sources=[source], stores={"postgres": store})
    engine = Engine(
        state,
        project.manifest,
        placements={"Local": lambda s, c: InlinePlacement(c, project)},
        clock=state.clock,
        eval_interval=0.01,
    )
    await engine.initialize()
    await engine.commit_source("ext", version="t1")
    observed = state.model.heads[("ext", "")]["ref"]["generation"]
    with store._connect() as conn:  # the external writer moves on before the reader reads
        conn.execute(f"UPDATE \"{table}\" SET v = '2'")
    await run(engine, ["report"])
    edge = (await edges(engine, state, "report"))["ext"]
    assert edge["from"]["generation"] == observed and "uncommitted" not in edge


async def test_a_renamed_postgres_output_stays_readable(state):
    """Review round 5 #1: an asset renamed with `aliases=` writes into the
    table its head names — the old one — and its head still loads. The
    engine pins the head; the store keeps the table the head names."""

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
    await run(engine, ["new"])  # the same rows, written again: into the old table
    head = Ref.from_json(state.model.heads[("new", "")]["ref"])
    assert await store.load(head, list[dict], None) == [{"id": "a", "v": "1"}]
    rows["v"] = [{"id": "a", "v": "2"}]
    await run(engine, ["new"])
    head = Ref.from_json(state.model.heads[("new", "")]["ref"])
    assert head.table == f'"{schema}"."old"'
    assert await store.load(head, list[dict], None) == [{"id": "a", "v": "2"}]
