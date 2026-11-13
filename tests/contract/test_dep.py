"""DEP: deploys and reload (docs/behaviors.md)."""

import asyncio
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Callable

import pytest
from solera.build import identity
from solera.executors import Pool
from solera.sdk import (
    Commit,
    Migration,
    Output,
    Project,
    RegistrationError,
    Retry,
    Source,
    Tick,
    asset,
    sensor,
)
from solera.stores import FileStore, Patch

from tests.contract.example import Example
from tests.contract.harness import Solera


def held(ex: Example) -> tuple[asyncio.Event, asyncio.Event]:
    """`tally`'s next batch waits, once it has read its inputs, until
    released: (entered, release)."""

    entered, release = asyncio.Event(), asyncio.Event()

    async def hold():
        entered.set()
        await release.wait()

    ex.during = hold
    return entered, release


# -- DEP-1: the build identity, the SDK alone ------------------------------------------


def git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


PROJECT = """
from helpers import f

from solera.sdk import Project, asset


@asset
def numbers():
    return [f()]


project = Project(assets=[numbers])
"""


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A project in a git work tree: `project.py` builds it, `helpers.py`
    is what its asset calls; `other.py` and `notes.md` are beside it."""

    monkeypatch.delenv("SOLERA_BUILD", raising=False)
    repo = tmp_path / "repo"
    (repo / "pkg").mkdir(parents=True)
    git(repo, "init", "-q")
    git(repo, "config", "user.email", "t@example.com")
    git(repo, "config", "user.name", "t")
    (repo / "pkg" / "project.py").write_text(PROJECT)
    (repo / "pkg" / "helpers.py").write_text("def f(): return 1\n")
    (repo / "pkg" / "other.py").write_text("x = 1\n")
    (repo / "pkg" / "notes.md").write_text("# notes\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "one")
    return repo


def build(pkg) -> dict:
    """The build identity a fresh process importing the project computes, as a worker does."""

    script = "import json, sys; sys.path.insert(0, sys.argv[1]); import project; "
    script += "print(json.dumps(project.project.manifest['build']))"
    out = subprocess.run([sys.executable, "-c", script, str(pkg)], capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


def test_dep_1_an_explicit_build_wins(repo, monkeypatch):
    assert identity(str(repo / "pkg"), "img@sha256:ab") == {"id": "img@sha256:ab", "source": "explicit"}
    monkeypatch.setenv("SOLERA_BUILD", "ci-42")
    assert identity(str(repo / "pkg"))["id"] == "ci-42"


def test_dep_1_the_build_is_the_code_the_project_runs(repo):
    """The modules its objects reach — `project.py`, and `helpers.py`, which
    its asset calls — committed or not; the commit only for display. An
    edit beside them it does not run (a note, a module it never imports,
    data) changes nothing, so a local serve keeps its deploy."""

    pkg = repo / "pkg"
    clean = build(pkg)
    assert clean["source"] == "modules" and len(clean["commit"]) == 40
    (pkg / "notes.md").write_text("# other notes\n")
    (pkg / "other.py").write_text("x = 2\n")
    (pkg / "data.json").write_text("{}")
    assert build(pkg)["id"] == clean["id"]
    (pkg / "helpers.py").write_text("def f(): return 2\n")
    first = build(pkg)
    (pkg / "helpers.py").write_text("def f(): return 3\n")
    second = build(pkg)
    assert len({clean["id"], first["id"], second["id"]}) == 3  # two uncommitted fixes, two builds
    (pkg / "helpers.py").write_text("def f(): return 1\n")
    assert build(pkg)["id"] == clean["id"]


def test_dep_1_outside_git_the_modules_are_the_build(tmp_path, monkeypatch):
    monkeypatch.delenv("SOLERA_BUILD", raising=False)
    pkg = tmp_path / "proj"
    pkg.mkdir()
    (pkg / "project.py").write_text(PROJECT)
    (pkg / "helpers.py").write_text("def f(): return 1\n")
    first = build(pkg)
    assert first["source"] == "modules" and "commit" not in first
    (pkg / "helpers.py").write_text("def f(): return 2\n")
    assert build(pkg)["id"] != first["id"]


POOLED = """
from solera.executors import Pool
from solera.sdk import Project, asset
from solera.stores import FileStore


@asset(executor=Pool("workers")())
def made():
    return {{"v": 1}}


project = Project(assets=[made], default_store=FileStore({data!r}))
"""


def imported(path) -> Project:
    """The project a fresh import of `path` builds, as a worker's process does."""

    import importlib.util

    spec = importlib.util.spec_from_file_location(f"pooled_{uuid.uuid4().hex}", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.project


async def test_dep_1_a_commit_of_the_same_code_is_the_same_deploy(solera, repo):
    """The commit is shown, not digested: an attempt launched before a commit
    of unchanged code, and one launched after it, both run on a worker that
    imported the project after it."""

    pkg = repo / "pkg"
    (pkg / "pooled.py").write_text(POOLED.format(data=str(solera.root / "data")))
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "two")
    served = imported(pkg / "pooled.py")
    await solera.deploy(served)
    first, stages = await solera.submit("made"), []

    async def launched():
        stages.extend((await solera.get("/pools/workers/work", wait=0, host="contract"))["work"])
        return stages

    await solera.until(launched)
    git(repo, "commit", "--allow-empty", "-qm", "three")
    after = imported(pkg / "pooled.py")
    assert after.manifest["build"]["commit"] != served.manifest["build"]["commit"]
    assert after.manifest["deploy"] == served.manifest["deploy"]
    assert await solera.worker(stages[0], after) == 0
    assert (await solera.wait(first))["request"]["status"] == "succeeded"
    second = await solera.submit("made")
    assert await solera.work("workers", imported(pkg / "pooled.py")) == 0
    assert (await solera.wait(second))["request"]["status"] == "succeeded"


def test_dep_1_the_error_policy_changes_the_deploy(monkeypatch):
    """Review 9: classifying an error differently is a different project."""

    from solera import Failed, Rejected, Transient

    monkeypatch.setenv("SOLERA_BUILD", "same")

    @asset
    def a():
        return 1

    class Slow(Transient):
        retry_for = "2h"

    class Slower(Transient):
        retry_for = "3h"

    revisions = {
        Project(assets=[a], errors=mapping).manifest["deploy"]
        for mapping in (
            {ValueError: Failed},
            {ValueError: Rejected},
            {ValueError: Slow},
            {ValueError: Slower},
            {},
        )
    }
    assert len(revisions) == 5


async def test_dep_1_the_engine_numbers_deploys_as_it_serves_them(solera, monkeypatch):
    ex = Example(solera)
    numbers = []
    for build_id in ("r1", "r1", "r2", "r1"):  # the second r1 serves no new deploy
        monkeypatch.setenv("SOLERA_BUILD", build_id)
        await ex.deploy()
        numbers.append((await solera.get("/assets/checks/outcomes"))["deploy"])
    await solera.restart()
    numbers.append((await solera.get("/assets/checks/outcomes"))["deploy"])
    assert numbers == [1, 1, 2, 3, 3]


# -- DEP-2: a local serve's reload ------------------------------------------------------

RELOADED = """
from dep2_helpers import f

from solera.sdk import Incremental, Output, Project, asset


@asset(outputs=Output("files", key="id"))
def files():
    return [{"id": "a"}]


@asset(inputs={"file": Incremental("files", each=True)}, outputs=Output("parsed", key="id"))
def parsed(ctx, file: list):
    return [{"id": ctx.key, "v": f()}]


project = Project(assets=[files, parsed])
"""


async def test_dep_2_a_local_serve_reloads_on_code_changes_and_keeps_its_deploy_on_a_broken_edit(
    solera, tmp_path, monkeypatch
):
    monkeypatch.delenv("SOLERA_BUILD", raising=False)
    code = tmp_path / "code"
    code.mkdir()
    (code / "reloaded.py").write_text(RELOADED)
    helpers = code / "dep2_helpers.py"
    helpers.write_text("def f(): return 1\n")

    async def served() -> tuple[int, str, str | None]:
        """The deploy's number, the deploy served, and what fails. The number
        first: a reload serves its deploy before it numbers it."""

        number = (await solera.get("/assets/parsed/outcomes"))["deploy"]
        diagnostics = await solera.get("/api/diagnostics")
        return number, diagnostics["deploy"], diagnostics["last_error"]

    async def until(predicate):
        """What `served()` says once `predicate` holds of it."""

        seen = []

        async def holds():
            seen.append(await served())
            return predicate(seen[-1])

        await solera.until(holds)
        return seen[-1]

    async with solera.reloading(f"{code / 'reloaded.py'}:project"):
        first = await served()
        (code / "notes.md").write_text("beside it: nothing")
        for n in (2, 3, 4):  # a burst of saves: one deploy
            helpers.write_text(f"def f(): return {n}\n")
            await asyncio.sleep(0.05)
        second = await until(lambda now: now[0] > first[0])
        assert second[0] == first[0] + 1 and second[2] is None
        helpers.write_text("def f(: return\n")  # half saved
        broken = await until(lambda now: now[2] is not None)
        assert broken[:2] == second[:2]  # the burst made one deploy, and the broken edit none
        assert "previous deploy" in broken[2]
        assert (await solera.client.get("/healthz")).status_code == 503
        helpers.write_text("def f(): return 5\n")
        third = await until(lambda now: now[0] > second[0])
        assert third[1] != second[1] and third[2] is None


# -- DEP-3: an attempt and its deploy ---------------------------------------------------


async def test_dep_3_an_attempt_finishes_under_the_deploy_it_was_launched_with(solera, monkeypatch):
    monkeypatch.setenv("SOLERA_BUILD", "one")
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    entered, release = held(ex)
    run = await solera.submit("tally")
    await solera.until(entered.is_set)
    monkeypatch.setenv("SOLERA_BUILD", "two")
    await ex.deploy()  # a new deploy neither stops nor fails it
    release.set()
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    [[attempt]] = detail["attempts"].values()
    assert attempt["outcome"] == "succeeded"
    assert (await solera.head("tally"))["commit"] == f"{run['id']}/{attempt['id']}"
    assert await ex.count() == 2 and await ex.stale() == []


def pooled(root, version: str = "1", build: str | None = None) -> Project:
    @asset(executor=Pool("workers")(), retries=Retry(2, delay=0), version=version)
    def made():
        return {"v": 1}

    return Project(assets=[made], default_store=FileStore(root / "data"), build=build)


async def test_dep_3_a_worker_on_another_deploy_fails_the_attempt_not_retryably(solera):
    await solera.deploy(pooled(solera.root))
    run = await solera.submit("made")
    await solera.work("workers", pooled(solera.root, version="2"))  # code of another deploy
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "failed"
    [[attempt]] = detail["attempts"].values()  # not retried, though `retries=` allows two more
    assert "deploy mismatch" in attempt["error"]
    result = await solera.get(f"/runs/{run['id']}/attempts/{attempt['id']}/result")
    assert result["status"] == "failed" and result["error"]["retryable"] is False
    assert await solera.head("made") is None


async def test_dep_3_a_worker_that_computed_its_deploy_another_way_is_told_why(solera, monkeypatch):
    monkeypatch.delenv("SOLERA_BUILD", raising=False)
    await solera.deploy(pooled(solera.root, build="served"))
    run = await solera.submit("made")
    await solera.work("workers", pooled(solera.root))  # its build, from its modules
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "failed"
    [[attempt]] = detail["attempts"].values()
    assert "SOLERA_BUILD" in attempt["error"]


# -- DEP-4: sensor hosts ------------------------------------------------------------


def watching(build: str) -> Project:
    @asset(deps=["feed"])
    def ingest() -> int:
        return 1

    @sensor(every=60, commits=["feed"])
    def watch(ctx) -> Tick | None:
        return Tick(commits=[Commit("feed", version="v7")])

    return Project(assets=[ingest], sources=[Source("feed")], sensors=[watch], build=build)


async def test_dep_4_a_sensor_host_on_old_code_takes_no_ticks_and_restarts_on_the_new(solera):
    await solera.deploy(watching("new"))
    assert await asyncio.wait_for(solera.sensor_host(watching("old"), stale_wait=0.05), 5) == 0
    assert (await solera.get("/sensors/watch/ticks"))["ticks"] == []
    assert (await solera.head("feed") or {}).get("version") is None
    # Its process starts again, with the code as it is now: it takes the ticks.
    assert await asyncio.wait_for(solera.sensor_host(watching("new"), max_ticks=1), 5) == 0
    assert (await solera.head("feed"))["version"] == "v7"


# -- DEP-5: migrations ---------------------------------------------------------------


class Migrating(FileStore):
    """A store that applies each migration once, keeping its own ledger,
    noting in `calls` each migration it applies and each write."""

    def __init__(self, root, calls: list):
        super().__init__(root)
        self.calls, self.ledger = calls, set()

    def can_store(self, t, output):
        return t is Callable or super().can_store(t, output)

    async def migrate(self, output, migrations, context=None, prior=None):
        for m in migrations:
            if (output.name, m.name) not in self.ledger:
                m.payload(None, "")
                self.ledger.add((output.name, m.name))
                self.calls.append(f"migrate {output.name} {m.name}")
        return [m.name for m in migrations]

    async def store(self, write, prior, context):
        self.calls.append("store")
        return await super().store(write, prior, context)


async def test_dep_5_migrations_apply_before_an_outputs_first_write(solera):
    calls = []

    @asset(
        outputs=Output("migrated", store="mig", migrations=[Migration("m1", lambda objects, prefix: None)])
    )
    def producer():
        return {"v": 1}

    await solera.deploy(Project(assets=[producer], stores={"mig": Migrating(solera.root / "mig", calls)}))
    await solera.run("producer")
    assert calls == ["migrate migrated m1", "store"]
    await solera.run("producer")
    assert calls == ["migrate migrated m1", "store", "store"]  # applied once
    assert await solera.read("migrated") == {"v": 1}


async def test_dep_5_a_failed_migration_is_not_retried(solera):
    calls = []

    @asset(
        outputs=Output("migrated", store="mig", migrations=[Migration("m1", lambda objects, prefix: 1 / 0)])
    )
    def producer():
        return {"v": 1}

    await solera.deploy(Project(assets=[producer], stores={"mig": Migrating(solera.root / "mig", calls)}))
    detail = await solera.run("producer", expect="failed")
    [[attempt]] = detail["attempts"].values()
    assert "migration failed" in attempt["error"]
    result = await solera.get(f"/runs/{detail['request']['id']}/attempts/{attempt['id']}/result")
    assert result["error"]["retryable"] is False
    assert calls == [] and await solera.head("migrated") is None


async def test_dep_5_an_unchanged_keyed_write_still_applies_its_migrations(solera):
    """Schema is not data: a migration added to a keyed output that a run
    writes nothing to is applied all the same; its content stays."""

    calls, rows = [], {"v": [{"id": "a", "v": 1}]}

    def project(migrations):
        @asset(outputs=Output("files", key="id", store="mig", migrations=migrations))
        def files():
            return rows["v"]

        return Project(assets=[files], stores={"mig": Migrating(solera.root / "mig", calls)})

    await solera.deploy(project([]))
    await solera.run("files")
    keys = await solera.keys("files")
    rows["v"] = Patch([])  # nothing written
    await solera.deploy(project([Migration("m1", lambda objects, prefix: None)]))
    await solera.run("files")
    assert [c for c in calls if c.startswith("migrate")] == ["migrate files m1"]
    assert await solera.keys("files") == keys and await solera.rows("files") == {"a": 1}


MIGRATED = """
import json
import os

from solera.sdk import Migration, Output, Project, asset
from solera.stores import FileStore

ROOT = {root!r}
LEDGER = os.path.join(ROOT, "ledger.json")


def note(what):
    with open(os.path.join(ROOT, "calls"), "a") as f:
        f.write(what + "\\n")


class Ledgered(FileStore):
    \"\"\"Applies each migration once, noting it in a ledger of its own.\"\"\"

    async def migrate(self, output, migrations, context=None, prior=None):
        applied = json.load(open(LEDGER)) if os.path.exists(LEDGER) else []
        for m in migrations:
            if m.name not in applied:
                m.payload()
                applied.append(m.name)
        json.dump(applied, open(LEDGER, "w"))
        return applied


@asset(outputs=Output("docs", store="ledgered", migrations=[Migration("seed", lambda: note("seed"))]))
def docs():
    note("docs")
    return b"x"


project = Project(assets=[docs], stores={{"ledgered": Ledgered(os.path.join(ROOT, "data"))}}, name="migdemo")
"""


async def test_dep_5_solera_migrate_applies_eagerly_and_is_idempotent(solera, tmp_path):
    """`solera migrate` applies what the project declares before any
    producer runs; run again, it applies nothing again."""

    project = tmp_path / "migdemo.py"
    project.write_text(MIGRATED.format(root=str(tmp_path)))
    for _ in range(2):
        code, _, err = await solera.cli("migrate", "--project", str(project))
        assert code == 0, err
        assert (tmp_path / "calls").read_text() == "seed\n"  # applied once, and no producer ran


@pytest.mark.postgres
async def test_dep_5_each_schemas_table_gets_its_own_migrations(solera, tmp_path):
    """A staging and a production namespace write `orders` into schemas of
    their own: each gets the migration applied, once."""

    if not os.environ.get("SOLERA_TEST_DATABASE_URL"):
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    import psycopg
    from solera_postgres import PostgresStore

    applied, schemas = [], [f"s_{uuid.uuid4().hex[:8]}" for _ in range(2)]

    def project(schema):
        def note(cursor):
            applied.append(schema)

        output = Output(
            "orders",
            key="id",
            store="pg",
            schema=schema,
            columns={"id": "text"},
            migrations=[Migration("m1", note)],
        )

        @asset(outputs=output)
        def orders():
            return [{"id": "a"}]

        return Project(assets=[orders], stores={"pg": PostgresStore("env:SOLERA_TEST_DATABASE_URL")})

    staging = Solera(tmp_path / "staging")
    try:
        for namespace, schema in zip((solera, staging), schemas, strict=True):
            await namespace.deploy(project(schema))
            await namespace.run("orders")
            await namespace.run("orders")
            assert await namespace.rows("orders", column="id") == {"a": "a"}
        assert applied == schemas  # once for each schema's table
    finally:
        await staging.close()
        with psycopg.connect(os.environ["SOLERA_TEST_DATABASE_URL"], autocommit=True) as conn:
            for schema in schemas:
                conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


# -- DEP-6 ----------------------------------------------------------------------------


def test_dep_6_a_secret_written_into_a_built_in_stores_config_is_refused():
    """K25 (b): a built-in store's config enters the manifest, so registration
    refuses a literal secret there, naming the field and the env:NAME form;
    a DSN without a password (local development) passes."""

    from solera.stores import S3Store
    from solera_postgres import PostgresStore

    @asset
    def one():
        return 1

    def register(store):
        return Project(assets=[one], default_store=store).manifest

    with pytest.raises(RegistrationError, match="dsn='env:NAME'"):
        register(PostgresStore("postgresql://solera:hunter2@db:5432/solera"))
    with pytest.raises(RegistrationError, match="dsn='env:NAME'"):
        register(PostgresStore("host=db dbname=solera password=hunter2"))
    with pytest.raises(RegistrationError, match="secret_access_key='env:NAME'"):
        register(S3Store("s3://bucket/data", secret_access_key="hunter2"))
    for fine in (
        PostgresStore("postgresql://solera@localhost/solera"),
        PostgresStore("env:SOLERA_DSN"),
        S3Store("s3://bucket/data", secret_access_key="env:AWS_SECRET"),
    ):
        assert register(fine)["stores"]["default"]["built_in"]["class"] == type(fine).__name__
