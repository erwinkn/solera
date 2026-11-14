"""The first slice of monolith's SharePoint ingestion, as it would run on
Solera beside Dagster. Monolith keeps a cache of each site's files in
Postgres (a files table, a folders table, written by its own sync service).
Here a view joins them into one row per file with its path, a source
observes that view, and a per-key asset parses the workbooks one folder
holds into a table of its own, keyed by path.

Everything a real slice does to the cache is played: a file added, edited,
deleted, a folder renamed, and a sheet that cannot be parsed. Skips unless SOLERA_TEST_DATABASE_URL points at a
Postgres server."""

import pytest
from solera.errors import Rejected
from solera.sdk import Every, Incremental, Output, Project, Source, asset
from solera.stores import FileStore
from solera_postgres import PostgresStore

from tests.contract.harness import Solera
from tests.test_demo_e2e import own_database  # noqa: F401  (a fixture)

pytestmark = pytest.mark.postgres

CACHE = """
CREATE SCHEMA sharepoint;
CREATE TABLE sharepoint.analytical_data_folders (id text PRIMARY KEY, path text NOT NULL);
CREATE TABLE sharepoint.analytical_data_files (
    id text PRIMARY KEY, name text NOT NULL, url text, last_modified timestamptz NOT NULL,
    deleted boolean NOT NULL DEFAULT false, parent_id text NOT NULL);
CREATE VIEW sharepoint.analytical_data_paths AS
    SELECT f.id, d.path || '/' || f.name AS path, f.url, f.last_modified
    FROM sharepoint.analytical_data_files f
    JOIN sharepoint.analytical_data_folders d ON d.id = f.parent_id
    WHERE NOT f.deleted;
"""


class Unprocessable(Rejected):
    """A sheet that is bad on purpose: kept as a rejected key until it changes."""


class FileCache(Source):
    """The file cache as a keyed source: each file's path, at the time it
    was last modified. The whole map each time, so a folder rename shows as
    its files' old paths gone and their new ones added."""

    def observe(self, ctx, query):
        rows = query("SELECT path, last_modified FROM sharepoint.analytical_data_paths")
        return {r["path"]: str(r["last_modified"]) for r in rows}


@pytest.fixture
def slice_(own_database, monkeypatch):  # noqa: F811
    import psycopg

    monkeypatch.setenv("DATABASE_URL", own_database)
    with psycopg.connect(own_database, autocommit=True) as conn:
        conn.execute(CACHE)
    sheets: dict[str, list[dict] | None] = {}  # file id -> its rows, None for a sheet that cannot be read
    parsed: list[str] = []

    def query(sql: str, *params) -> list[dict]:
        with psycopg.connect(own_database, autocommit=True, row_factory=psycopg.rows.dict_row) as conn:
            found = conn.execute(sql, params)
            return found.fetchall() if found.description else []

    @asset(
        inputs={
            "sheet": Incremental(
                "analytical_data_files", include="Alumina Density/**/*Data Sheet*.xlsx", each=True
            )
        },
        outputs=Output(
            "sga_alumina_density",
            store="postgres",
            schema="sga_solera",
            key="path",
            columns={
                "path": "text",
                "file_id": "text",
                "file_url": "text",
                "sample": "text",
                "density": "float",
            },
        ),
    )
    def sga_alumina_density(ctx, sheet: list[dict]):
        """One data sheet: its samples, each stamped with the file it came
        from. `sheet` is the key's rows in the source: here one, the file's."""

        [file] = sheet
        parsed.append(file["id"])
        rows = sheets[file["id"]]
        if rows is None:
            raise Unprocessable("not a data sheet")
        return [{"file_id": file["id"], "file_url": file["url"], **row} for row in rows]

    project = Project(
        assets=[sga_alumina_density],
        sources=[
            FileCache(
                "analytical_data_files",
                store="postgres",
                key="path",
                table="sharepoint.analytical_data_paths",
                version_column="last_modified",
                observe=Every(1),  # monolith's sync service polls every five minutes
            )
        ],
        stores={"postgres": PostgresStore("env:DATABASE_URL")},
        resources={"query": query},
        default_store=FileStore(None),
    )
    return project, query, sheets, parsed


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path)
    yield namespace
    await namespace.close()


def put(query, sheets, file_id: str, folder: str, name: str, at: str, rows) -> None:
    """ms-sync sees a file, new or edited."""

    query(
        "INSERT INTO sharepoint.analytical_data_files (id, name, url, last_modified, parent_id) "
        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET last_modified = EXCLUDED.last_modified, "
        "name = EXCLUDED.name, deleted = false",
        file_id,
        name,
        f"https://sharepoint.example/{file_id}",
        at,
        folder,
    )
    sheets[file_id] = rows


async def synced(solera) -> None:
    """The source observes the cache, then the asset runs over what changed."""

    await solera.sensors(1)
    await solera.run("sga_alumina_density")


async def test_the_first_slice_follows_the_file_cache(solera, slice_):
    project, query, sheets, parsed = slice_
    project.default_store = FileStore(solera.root / "data")
    await solera.deploy(project)

    def table() -> list[tuple]:
        rows = query(
            "SELECT path, file_id, sample, density FROM sga_solera.sga_alumina_density ORDER BY path, sample"
        )
        return [(r["path"], r["file_id"], r["sample"], r["density"]) for r in rows]

    query(
        "INSERT INTO sharepoint.analytical_data_folders VALUES ('d1', 'Alumina Density/2026'), ('d2', 'Other')"
    )
    put(
        query,
        sheets,
        "f1",
        "d1",
        "Alumina Density Data Sheet A.xlsx",
        "2026-11-01T10:00:00Z",
        [{"sample": "s1", "density": 3.9}],
    )
    put(
        query,
        sheets,
        "f2",
        "d1",
        "Alumina Density Data Sheet B.xlsx",
        "2026-11-01T10:00:00Z",
        [{"sample": "s2", "density": 3.8}],
    )
    put(
        query, sheets, "f3", "d1", "notes.xlsx", "2026-11-01T10:00:00Z", []
    )  # not a data sheet: its name says so
    put(
        query, sheets, "f4", "d2", "Alumina Density Data Sheet C.xlsx", "2026-11-01T10:00:00Z", []
    )  # another folder
    await synced(solera)
    assert sorted(parsed) == ["f1", "f2"]
    a, b = (
        "Alumina Density/2026/Alumina Density Data Sheet A.xlsx",
        "Alumina Density/2026/Alumina Density Data Sheet B.xlsx",
    )
    assert table() == [(a, "f1", "s1", 3.9), (b, "f2", "s2", 3.8)]

    # An edit and a deletion: only those two files move.
    parsed.clear()
    put(
        query,
        sheets,
        "f1",
        "d1",
        "Alumina Density Data Sheet A.xlsx",
        "2026-11-02T08:00:00Z",
        [{"sample": "s1", "density": 4.0}, {"sample": "s9", "density": 4.1}],
    )
    query("UPDATE sharepoint.analytical_data_files SET deleted = true WHERE id = 'f2'")
    await synced(solera)
    assert parsed == ["f1"]
    assert table() == [(a, "f1", "s1", 4.0), (a, "f1", "s9", 4.1)]

    # A folder rename: the file's old path goes, its new one is parsed.
    parsed.clear()
    query(
        "UPDATE sharepoint.analytical_data_folders SET path = 'Alumina Density/Archive 2026' WHERE id = 'd1'"
    )
    await synced(solera)
    moved = "Alumina Density/Archive 2026/Alumina Density Data Sheet A.xlsx"
    assert parsed == ["f1"]
    assert table() == [(moved, "f1", "s1", 4.0), (moved, "f1", "s9", 4.1)]

    # A sheet that cannot be parsed is recorded, and the others commit all the same.
    parsed.clear()
    put(query, sheets, "f5", "d1", "Alumina Density Data Sheet E.xlsx", "2026-11-03T08:00:00Z", None)
    put(
        query,
        sheets,
        "f6",
        "d1",
        "Alumina Density Data Sheet F.xlsx",
        "2026-11-03T08:00:00Z",
        [{"sample": "s6", "density": 3.7}],
    )
    await synced(solera)
    bad, good = (f"Alumina Density/Archive 2026/Alumina Density Data Sheet {x}.xlsx" for x in "EF")
    assert sorted(parsed) == ["f5", "f6"]
    assert table() == [(moved, "f1", "s1", 4.0), (moved, "f1", "s9", 4.1), (good, "f6", "s6", 3.7)]
    failing = await solera.outcomes("sga_alumina_density", "rejected", "failed")
    assert {k: (r["outcome"], r["message"]) for k, r in failing.items()} == {
        bad: ("rejected", "Unprocessable: not a data sheet")
    }

    # The run history logs the failure too. The file's version is a timestamp, not a number.
    [logged] = await solera.key_log("sga_alumina_density")
    assert (logged["key"], logged["outcome"], logged["error"]) == (
        bad,
        "rejected",
        "Unprocessable: not a data sheet",
    )

    # Nothing changed: nothing is stale, and nothing is parsed, the bad sheet included.
    parsed.clear()
    await solera.sensors(1)
    assert await solera.stale("sga_alumina_density") == []
    await solera.run("sga_alumina_density")
    assert parsed == []

    # The bad sheet is fixed: it alone is parsed, and its failure goes.
    put(
        query,
        sheets,
        "f5",
        "d1",
        "Alumina Density Data Sheet E.xlsx",
        "2026-11-04T08:00:00Z",
        [{"sample": "s5", "density": 3.6}],
    )
    await synced(solera)
    assert parsed == ["f5"]
    assert await solera.outcomes("sga_alumina_density", "rejected", "failed") == {}
    assert (bad, "f5", "s5", 3.6) in table()
