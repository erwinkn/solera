"""The reference project (`example/brimstone.py`) run end to end on
Postgres, its SharePoint ingestion as monolith would run it: the site list,
each site's Graph delta feed into an event log and a file inventory, one
call per changed workbook into a table, and the tables built from them.
The outside world (Graph, SharePoint, the mailer) is faked; everything
else is the project as written. Skips unless SOLERA_TEST_DATABASE_URL points
at a Postgres server."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from tests.contract.harness import Solera
from tests.test_demo_e2e import own_database  # noqa: F401  (a fixture)

pytestmark = pytest.mark.postgres

BRIMSTONE = Path(__file__).parents[1] / "example" / "brimstone.py"


class Graph:
    """Microsoft Graph: the sites, and each site's delta feed, an append-only
    list of file events whose cursor is how many the caller has seen."""

    def __init__(self):
        self.feeds: dict[str, list[dict]] = {"alpha": [], "bravo": []}

    def list_sites(self) -> list[str]:
        return sorted(self.feeds)

    def delta(self, site: str, since: str | None) -> tuple[list[dict], str]:
        feed = self.feeds[site]
        return feed[int(since or 0) :], str(len(feed))

    def touch(self, site: str, file_id: str, version: int, *, deleted: bool = False, path: str | None = None):
        self.feeds[site].append(
            {
                "file_id": file_id,
                "version": version,
                "path": path or f"/QAQC/{file_id}.xlsx",
                "status": "deleted" if deleted else "modified",
                "deleted": deleted,
            }
        )


class SharePoint:
    """File contents by id: a workbook is its samples, as JSON; empty bytes
    are an empty workbook."""

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.reads: list[str] = []

    async def read(self, file_id: str) -> bytes:
        self.reads.append(file_id)
        return self.files[file_id]

    def write(self, file_id: str, samples: list[dict]) -> None:
        self.files[file_id] = json.dumps(samples).encode()


class Mailer:
    def __init__(self):
        self.sent: list[tuple] = []

    def send(self, to: str, subject: str, body: str) -> None:
        self.sent.append((to, subject, body))


@pytest.fixture
def brimstone(own_database, monkeypatch):  # noqa: F811
    """The project, loaded with its database at the test's own, its tables
    as monolith has them before Solera writes, and its clients faked."""

    import pandas as pd
    import psycopg

    monkeypatch.setenv("DATABASE_URL", own_database)
    monkeypatch.setenv("ARTIFACT_STORE", "s3://unused/artifacts")
    spec = importlib.util.spec_from_file_location("brimstone", BRIMSTONE)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "brimstone", module)
    spec.loader.exec_module(module)
    module.is_qaqc_workbook = lambda event: event["path"].endswith(".xlsx")
    module.read_qaqc_excel = lambda raw: pd.DataFrame(json.loads(raw))
    module.site_region = lambda site: "east" if site == "alpha" else "west"
    with psycopg.connect(own_database, autocommit=True) as conn:
        conn.execute("CREATE ROLE qgis") if not conn.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = 'qgis'"
        ).fetchone() else None
        conn.execute("CREATE ROLE felt") if not conn.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = 'felt'"
        ).fetchone() else None
        # The samples table exists before Solera: its migrations alter it.
        conn.execute("CREATE SCHEMA qaqc")
        conn.execute(
            "CREATE TABLE qaqc.qaqc_samples (file_id text, sample_id text, value double precision, "
            "measured timestamp, site text, _version bigint, PRIMARY KEY (file_id, sample_id, site))"
        )
    graph, sharepoint, mailer = Graph(), SharePoint(), Mailer()

    def query(sql: str, **params) -> list[dict]:
        with psycopg.connect(own_database, row_factory=psycopg.rows.dict_row) as conn:
            return conn.execute(sql, params).fetchall()

    module.project.resources.update(graph=graph, sharepoint=sharepoint, mailer=mailer, query=query)
    return module.project, graph, sharepoint, mailer, query


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path)
    yield namespace
    await namespace.close()


def sample(sample_id: str, value: float) -> dict:
    return {"sample_id": sample_id, "value": value, "measured": "2026-11-13T09:30:00", "analyst": "ek"}


async def test_sharepoint_ingestion_runs_end_to_end(solera, brimstone):
    project, graph, sharepoint, mailer, query = brimstone
    await solera.deploy(project)

    # The site list, then each site's feed: two workbooks at alpha, one at bravo, and a
    # file that is no QAQC workbook.
    await solera.run("sites")
    sharepoint.write("f1", [sample("s1", 1.0), sample("s2", 2.0)])
    sharepoint.write("f2", [sample("s3", 3.0)])
    sharepoint.write("f3", [sample("s4", 4.0)])
    graph.touch("alpha", "f1", 1)
    graph.touch("alpha", "f2", 1)
    graph.touch("alpha", "notes", 1, path="/notes.txt")
    graph.touch("bravo", "f3", 1)
    await solera.run("graph_delta", partitions="all")
    events = query("SELECT site, file_id FROM sharepoint.change_events ORDER BY site, file_id")
    assert [(e["site"], e["file_id"]) for e in events] == [
        ("alpha", "f1"),
        ("alpha", "f2"),
        ("alpha", "notes"),
        ("bravo", "f3"),
    ]

    # One call per workbook, each site's samples in its rows of the table.
    await solera.run("qaqc_samples", partitions="all")
    assert sorted(sharepoint.reads) == ["f1", "f2", "f3"]
    rows = query("SELECT site, file_id, sample_id, value FROM qaqc.qaqc_samples ORDER BY sample_id")
    assert [(r["site"], r["file_id"], r["sample_id"]) for r in rows] == [
        ("alpha", "f1", "s1"),
        ("alpha", "f1", "s2"),
        ("alpha", "f2", "s3"),
        ("bravo", "f3", "s4"),
    ]
    assert query(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'qaqc_samples' AND column_name = 'analyst'"
    )

    # A workbook changes and another is deleted: only those two keys move.
    sharepoint.reads.clear()
    sharepoint.write("f1", [sample("s1", 10.0)])
    graph.touch("alpha", "f1", 2)
    graph.touch("alpha", "f2", 2, deleted=True)
    await solera.run("graph_delta", partitions="all")
    await solera.run("qaqc_samples", partitions="all")
    assert sharepoint.reads == ["f1"]
    rows = query("SELECT file_id, sample_id, value FROM qaqc.qaqc_samples ORDER BY sample_id")
    assert [(r["file_id"], r["sample_id"], r["value"]) for r in rows] == [
        ("f1", "s1", 10.0),
        ("f3", "s4", 4.0),
    ]

    # The tables built from them: in the database, across sites, by region.
    await solera.run(["site_health", "fleet_dashboard", "region_rollup"], partitions="all")
    health = query("SELECT site, status, n FROM ops.site_health ORDER BY site, status")
    assert [(h["site"], h["status"], h["n"]) for h in health] == [
        ("alpha", "deleted", 1),
        ("alpha", "modified", 4),
        ("bravo", "modified", 1),
    ]
    assert {r["site"] for r in query("SELECT site FROM ops.fleet_dashboard")} == {"alpha", "bravo"}
    rollup = query("SELECT region, sample_id FROM ops.region_rollup ORDER BY region")
    assert [(r["region"], r["sample_id"]) for r in rollup] == [("east", "s1"), ("west", "s4")]

    # The job reads the dashboard and mails it.
    await solera.run("weekly_report")
    assert len(mailer.sent) == 1 and "alpha" in mailer.sent[0][2]

    # Nothing changed outside: nothing is stale, and a run has nothing to do.
    for name in ("qaqc_samples", "site_health", "fleet_dashboard", "region_rollup"):
        assert await solera.stale(name) == [], name


async def test_first_migrations_of_many_partitions_at_once_do_not_collide(own_database):  # noqa: F811
    """Eight partitions of one table run their first migration at the same
    moment, in a database that has no migrations ledger yet: each applies or
    finds it applied, and none fails making the ledger."""

    import asyncio

    import psycopg
    from solera.sdk import Migration, Output
    from solera.stores import WriteContext
    from solera_postgres import PostgresStore

    with psycopg.connect(own_database, autocommit=True) as conn:
        conn.execute("CREATE TABLE public.samples (id text, site text, _version bigint)")
    out = Output("samples", store="postgres", key="id", partition_column="site")
    migration = Migration("0001_note", "ALTER TABLE public.samples ADD COLUMN IF NOT EXISTS note text")

    async def migrate(n: int) -> list[str]:
        context = WriteContext(out, f"site-{n}", attempt=f"a{n}", generation=n + 1, worker_id=f"w{n}")
        return await PostgresStore(own_database).migrate(out, [migration], context)

    assert await asyncio.gather(*(migrate(n) for n in range(8))) == [["0001_note"]] * 8
