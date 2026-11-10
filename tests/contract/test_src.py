"""SRC: sources and served versions (docs/behaviors.md)."""

import os
import uuid

import pytest
from solera.sdk import (
    Every,
    In,
    Incremental,
    Loaded,
    Observed,
    Output,
    Project,
    Ref,
    RegistrationError,
    Retention,
    Retry,
    Source,
    asset,
    source,
)
from solera.stores import FileStore

from tests.contract.example import Example
from tests.contract.harness import Outside, Refused, SourceStore, rebuild


def keyed(root, outside: Outside, got: list) -> Project:
    """`feed`, a keyed source served live from `outside`; `ingest` records
    each batch's classes; `knob`, an unkeyed source."""

    @asset(inputs={"feed": Incremental()})
    def ingest(ctx, feed: list):
        b = ctx.batch["feed"]
        got.append((sorted(b.added), sorted(b.updated), sorted(b.removed)))
        return len(feed)

    return Project(
        assets=[ingest],
        sources=[Source("feed", key="id", store="ext"), Source("knob")],
        stores={"ext": SourceStore(root / "ext", outside)},
        default_store=FileStore(root / "data"),
    )


async def test_src_1_a_commit_changes_only_the_keys_whose_version_moved(solera):
    outside, got = Outside(), []
    await solera.deploy(keyed(solera.root, outside, got))
    outside.feed.update({"a.csv": "c7", "b.csv": "c3"})
    assert (await solera.commit_source("feed", keys={"a.csv": "c7", "b.csv": "c3"}))["changed"]
    await solera.run("ingest")
    outside.feed.update({"b.csv": "c4", "d.csv": "c1"})
    commit = await solera.commit_source("feed", keys={"a.csv": "c7", "b.csv": "c4", "d.csv": "c1"})
    request = (await solera.get(f"/runs/{commit['run']}"))["request"]
    assert (request["upserted"], request["deleted"]) == (["b.csv", "d.csv"], [])
    await solera.run("ingest")
    assert got[-1] == (["d.csv"], ["b.csv"], [])  # a.csv, at the version it held, unchanged

    del outside.feed["a.csv"]  # a full map removes what it omits
    commit = await solera.commit_source("feed", keys={"b.csv": "c4", "d.csv": "c1"})
    assert (await solera.get(f"/runs/{commit['run']}"))["request"]["deleted"] == ["a.csv"]
    await solera.run("ingest")
    assert got[-1] == ([], [], ["a.csv"])
    assert set(await solera.keys("feed")) == {"b.csv", "d.csv"}

    outside.feed.update({"e.csv": "c1"})  # upsert and remove patch, leaving the rest
    outside.feed.pop("b.csv")
    await solera.commit_source("feed", upsert={"e.csv": "c1"}, remove=["b.csv"])
    await solera.run("ingest")
    assert got[-1] == (["e.csv"], [], ["b.csv"])
    assert set(await solera.keys("feed")) == {"d.csv", "e.csv"}
    assert not (await solera.commit_source("feed", upsert={"e.csv": "c1"}))["changed"]


async def test_src_1_a_key_given_no_version_is_always_a_change(solera):
    outside, got = Outside(), []
    await solera.deploy(keyed(solera.root, outside, got))
    outside.feed.update({"a": "1"})
    for _ in range(2):
        commit = await solera.commit_source("feed", upsert={"a": None})
        assert commit["changed"]
        assert (await solera.get(f"/runs/{commit['run']}"))["request"]["upserted"] == ["a"]


async def test_src_1_an_unkeyed_source_at_its_current_version_makes_no_commit(solera):
    await solera.deploy(keyed(solera.root, Outside(), []))
    assert (await solera.commit_source("knob", version="v2"))["changed"]
    before = await solera.runs()
    assert not (await solera.commit_source("knob", version="v2"))["changed"]
    assert await solera.runs() == before
    assert (await solera.head("knob"))["version"] == "v2"
    assert (await solera.commit_source("knob", version="v3"))["changed"]
    assert (await solera.head("knob"))["version"] == "v3"


async def test_src_1_the_commit_route_takes_a_full_map_or_a_patch(solera):
    """POST sources/{name}/commit advances the source; an unknown source is 404."""

    @asset(inputs={"uploads": In()})
    def refs(uploads: Ref):
        return []

    await solera.deploy(Project(assets=[refs], sources=[Source("uploads", key="id")]))
    committed = await solera.commit_source("uploads", keys={"u-1": "v1"})
    assert committed["ref"]["generation"]
    await solera.commit_source("uploads", upsert={"u-2": "v1"}, remove=["u-1"])
    assert set(await solera.keys("uploads")) == {"u-2"}
    with pytest.raises(Refused) as missing:
        await solera.commit_source("ghost", version="1")
    assert missing.value.status == 404


async def test_src_2_each_commit_that_changes_something_is_a_run_with_no_tasks(solera):
    @source(key="id")
    async def uploads(keys, ctx):
        return {k: Loaded({"id": k}, version="1") for k in keys}

    @asset(inputs={"uploads": Incremental()})
    def ingest(uploads: list):
        return []

    await solera.deploy(Project(assets=[ingest], sources=[uploads], retention=Retention(days=1)))
    first = await solera.commit_source("uploads", keys={"u-6": "1", "u-7": "1"}, by="sharepoint-webhook")
    assert not (await solera.commit_source("uploads", upsert={"u-7": "1"}))["changed"]  # no change, no run
    assert [r["id"] for r in await solera.runs()] == [first["run"]]
    solera.advance(60)
    second = await solera.commit_source("uploads", keys={"u-7": "2"}, by="api")
    view = (await solera.get(f"/runs/{second['run']}"))["request"]
    assert view["source"] == "uploads" and view["by"] == "api"
    assert (view["upserted"], view["deleted"]) == (["u-7"], ["u-6"])
    view = (await solera.get(f"/runs/{first['run']}"))["request"]
    assert view["by"] == "sharepoint-webhook" and view["status"] == "succeeded" and view["tasks"] == []
    assert view["created_at"] == pytest.approx(solera.clock() - 60, abs=1)
    assert [r["id"] for r in await solera.runs()] == [second["run"], first["run"]]
    solera.advance(2 * 86400)
    await solera.upkeep()
    assert await solera.runs() == []  # expired under the project's default
    with pytest.raises(Refused) as gone:
        await solera.get(f"/runs/{first['run']}")
    assert gone.value.status == 404


async def test_src_3_a_keyed_loader_serves_rows_with_their_versions(solera):
    """Keyed: called with the batch's keys. The consumer gets the rows, and
    `served` each key's version; a key the loader leaves out was served absent."""

    outside = {"a": "1", "b": "2"}
    seen, asked = [], []

    @source(key="id")
    async def files(keys, ctx):
        asked.append(sorted(keys))
        return {
            k: Loaded({"id": k, "v": outside[k]}, version=f"etag-{outside[k]}") for k in keys if k in outside
        }

    @asset(inputs={"files": Incremental()}, outputs=Output("copy", key="id"))
    def copy(ctx, files: list):
        b = ctx.batch["files"]
        seen.append(
            (sorted(r["id"] for r in files), {k: v for k, v in b.served.items() if v}, sorted(b.removed))
        )
        return rebuild(b, files)

    await solera.deploy(Project(assets=[copy], sources=[files], default_store=FileStore(solera.root / "out")))
    await solera.commit_source("files", upsert={"a": "1", "b": "2"})
    await solera.run("copy")
    assert asked == [["a", "b"]]
    assert seen == [(["a", "b"], {"a": "etag-1", "b": "etag-2"}, [])]
    del outside["b"]
    outside["a"] = "3"
    await solera.commit_source("files", upsert={"a": "3", "b": "3"})
    await solera.run("copy")
    assert asked[-1] == ["a", "b"]
    assert seen[-1] == (["a"], {"a": "etag-3"}, ["b"])  # b, left out, was served absent
    assert await solera.rows("copy") == {"a": "3"}


async def test_src_3_a_keyed_loader_that_omits_versions_fails_the_load(solera):
    @source(key="id")
    async def files(keys, ctx):
        return {k: {"id": k} for k in keys}  # rows, but no versions

    @asset(inputs={"files": Incremental()})
    def count(files: list):
        return len(files)

    await solera.deploy(Project(assets=[count], sources=[files]))
    await solera.commit_source("files", upsert={"a": "1"})
    detail = await solera.run("count", expect="failed")
    [[attempt]] = detail["attempts"].values()
    assert "Loaded(row, version=" in attempt["error"]


async def test_src_4_a_row_served_ahead_of_its_commit_is_classed_from_what_was_served(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.feed({"k2": "2"}, items=False)
    ex.outside.feed["k2"] = "3"  # ahead of every commit
    [batch] = await ex.run("items")
    assert batch.classes == ([], ["k2"], [])
    assert await solera.rows("items") == {"k1": "1", "k2": "3"}
    ex.outside.feed["k2"] = "2"  # restored to the committed version by a commit: owed as updated
    await solera.commit_source("feed", upsert={"k2": "2"})
    assert await solera.stale("items") == ["input changed"]
    [batch] = await ex.run("items")
    assert batch.classes == ([], ["k2"], [])
    assert await solera.rows("items") == {"k1": "1", "k2": "2"}
    assert await solera.stale("items") == []


async def test_src_4_served_at_the_version_already_processed_is_nothing(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.feed({"k2": "2"}, items=False)
    ex.outside.feed["k2"] = "3"
    await ex.run("items")
    await solera.commit_source("feed", upsert={"k2": "3"})  # the commit catches up with what was served
    assert await solera.stale("items") == []


async def test_src_4_served_absent_is_removed_if_held_else_nothing(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    del ex.outside.feed["k2"]  # the outside loses k2; the commit says k2@2 and adds k3, never served
    await solera.commit_source("feed", upsert={"k2": "2", "k3": "1"})
    [batch] = await ex.run("items")
    assert batch.classes == ([], [], ["k2"])
    assert await solera.rows("items") == {"k1": "1"}


def behind(root, outside: Outside) -> Project:
    """`items` (whole batches) and `checks` (per key), each over `feed`."""

    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id"), retries=Retry(n=0))
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(
        inputs={"row": Incremental("feed", each=True)}, outputs=Output("checks", key="id"), retries=Retry(n=0)
    )
    async def checks(ctx, row: list):
        return [{"id": ctx.key, "v": row[0]["v"]}]

    return Project(
        assets=[items, checks],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(root / "ext", outside)},
        default_store=FileStore(root / "data"),
    )


async def lose_k1(solera, asset_name: str) -> Outside:
    """`feed` commits k1@3 and k2@1, then the outside loses k1 without a
    commit, before `asset_name` reads it."""

    outside = Outside()
    await solera.deploy(behind(solera.root, outside))
    outside.feed.update({"k1": "3", "k2": "1"})
    await solera.commit_source("feed", upsert={"k1": "3", "k2": "1"})
    del outside.feed["k1"]
    await solera.run(asset_name)
    return outside


@pytest.mark.parametrize("asset_name", ["items", "checks"])
async def test_src_5_a_key_lost_without_a_commit_is_processed_as_absent(solera, asset_name):
    await lose_k1(solera, asset_name)
    assert await solera.rows(asset_name) == {"k2": "1"}
    assert await solera.stale(asset_name) == [
        "input changed"
    ]  # feed still lists k1: owed until it says otherwise


@pytest.mark.parametrize("asset_name", ["items", "checks"])
async def test_src_5_its_return_is_delivered(solera, asset_name):
    outside = await lose_k1(solera, asset_name)
    outside.feed["k1"] = "3"  # restored, at the version its commit named
    await solera.commit_source("feed", upsert={"k1": "3"})
    await solera.run(asset_name)
    assert await solera.rows(asset_name) == {"k1": "3", "k2": "1"}
    assert await solera.stale(asset_name) == []


@pytest.mark.parametrize("asset_name", ["items", "checks"])
async def test_src_5_a_commit_removing_it_owes_nothing_more(solera, asset_name):
    await lose_k1(solera, asset_name)
    await solera.commit_source("feed", remove=["k1"])
    assert await solera.stale(asset_name) == []
    await solera.run(asset_name, expect=None)
    assert await solera.rows(asset_name) == {"k2": "1"}


async def test_src_5_items_learns_of_the_return_as_added(solera):
    ex = await Example(solera).start()
    ex.outside.feed.update({"k1": "3", "k2": "1"})
    await solera.commit_source("feed", upsert={"k1": "3", "k2": "1"})
    del ex.outside.feed["k1"]
    [batch] = await ex.run("items")
    assert batch.classes == (["k2"], [], [])
    ex.outside.feed["k1"] = "3"
    await solera.commit_source("feed", keys={"k1": "3", "k2": "1"})  # the same version: no change to feed
    [batch] = await ex.run("items")
    assert batch.classes == (["k1"], [], [])
    assert await solera.rows("items") == {"k1": "3", "k2": "1"}


def test_src_6_a_keyed_source_loaded_as_data_must_be_able_to_say_what_it_served():
    """No loader and a store that cannot serve it: refused at registration.
    Read only as a `Ref`, it loads nothing, so needs neither."""

    @asset(inputs={"uploads": Incremental()})
    def ingest(uploads: list):
        return []

    with pytest.raises(RegistrationError, match="which version"):
        Project(assets=[ingest], sources=[Source("uploads", key="id")])

    @asset(inputs={"uploads": In()})
    def refs(uploads: Ref):
        return []

    Project(assets=[refs], sources=[Source("uploads", key="id")])


async def test_src_7_a_file_source_serves_objects_at_the_stores_version(solera):
    """A store-backed source: objects under `path`, each served as `{key: name,
    "content": bytes}` at the store's word for it — on local disk its stamp
    (inode, mtime, size); with `hash=True` a hash of its content."""

    data = solera.root / "data"
    (data / "incoming").mkdir(parents=True)
    (data / "incoming" / "a.txt").write_text("one")
    seen = {}

    @asset(inputs={"docs": Incremental()})
    def sizes(ctx, docs: list):
        seen["docs"] = ({r["name"]: r["content"] for r in docs}, dict(ctx.batch["docs"].served))
        return {r["name"]: len(r["content"]) for r in docs}

    @asset(inputs={"hashed": Incremental()})
    def hashes(ctx, hashed: list):
        seen["hashed"] = dict(ctx.batch["hashed"].served)
        return len(hashed)

    project = Project(
        assets=[sizes, hashes],
        sources=[
            Source("docs", key="name", path="incoming"),
            Source("hashed", key="name", path="incoming", hash=True),
        ],
        default_store=FileStore(data),
    )
    await solera.deploy(project)
    for name in ("docs", "hashed"):
        await solera.commit_source(name, upsert={"a.txt": "v1"})
    await solera.run(["sizes", "hashes"])
    (content, served), hashed = seen["docs"], seen["hashed"]
    assert content == {"a.txt": b"one"} and served["a.txt"]  # the file's stamp
    assert hashed == {"a.txt": "sha256:7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"}


async def test_src_7_an_unkeyed_file_source_serves_the_objects_bytes(solera):
    data = solera.root / "data"
    data.mkdir()
    (data / "rates.json").write_text('{"eur": 1.1}')
    got = []

    @asset(inputs={"rates": In()})
    def priced(rates: bytes):
        got.append(rates)
        return 1

    await solera.deploy(
        Project(assets=[priced], sources=[Source("rates", path="rates.json")], default_store=FileStore(data))
    )
    await solera.commit_source("rates", version="v1")
    await solera.run("priced")
    assert got == [b'{"eur": 1.1}']


@pytest.mark.postgres
async def test_src_7_a_table_source_serves_rows_at_their_version_column(solera):
    dsn = os.environ.get("SOLERA_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    import psycopg
    from solera_postgres import PostgresStore

    table = f"orders_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"CREATE TABLE public.{table} (id text PRIMARY KEY, v int, updated_at bigint)")
        conn.execute(f"INSERT INTO public.{table} VALUES ('a', 1, 100), ('b', 2, 200)")
    seen = []

    @asset(inputs={"orders": Incremental()})
    def total(ctx, orders: list):
        seen.append(dict(ctx.batch["orders"].served))
        return sum(r["v"] for r in orders)

    pg = PostgresStore("env:SOLERA_TEST_DATABASE_URL")
    try:
        with pytest.raises(RegistrationError, match="which version"):  # keyed, and nothing names the version
            Project(
                assets=[total],
                sources=[Source("orders", store="pg", key="id", table=f"public.{table}")],
                stores={"pg": pg},
            )
        source = Source("orders", store="pg", key="id", table=f"public.{table}", version_column="updated_at")
        await solera.deploy(
            Project(
                assets=[total],
                sources=[source],
                stores={"pg": pg},
                default_store=FileStore(solera.root / "out"),
            )
        )
        await solera.commit_source("orders", upsert={"a": "100", "b": "200"})
        await solera.run("total")
        assert seen == [{"a": "100", "b": "200"}]
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(f"DROP TABLE public.{table}")


@pytest.mark.parametrize("bare", [False, True])
async def test_src_7_an_unkeyed_function_source_serves_its_value(solera, bare):
    """With its version, or a bare value."""

    got = []

    @source
    async def rates(keys, ctx):
        assert keys is None
        return {"eur": 1.1} if bare else Loaded({"eur": 1.1}, version="2026-10-04")

    @asset(inputs={"rates": In()})
    def priced(rates: dict):
        got.append(rates)
        return rates["eur"] * 2

    await solera.deploy(Project(assets=[priced], sources=[rates]))
    await solera.commit_source("rates", version="v1")
    await solera.run("priced")
    assert got == [{"eur": 1.1}]
    assert await solera.read("priced") == 2.2


@pytest.mark.xfail(strict=True, reason="not built: behaviors.md gap 9")
async def test_src_8_bumping_a_loaders_version_owes_every_key_as_updated(solera):
    seen = []

    def project(version: str) -> Project:
        @source(key="id", version=version)
        async def feed(keys, ctx):
            return {k: Loaded({"id": k}, version="1") for k in keys}

        @asset(inputs={"feed": Incremental()})
        def ingest(ctx, feed: list):
            b = ctx.batch["feed"]
            seen.append((sorted(b.added), sorted(b.updated), sorted(b.removed)))
            return len(feed)

        return Project(assets=[ingest], sources=[feed])

    await solera.deploy(project("1"))
    await solera.commit_source("feed", upsert={"a": "1", "b": "1"})
    await solera.run("ingest")
    assert seen == [(["a", "b"], [], [])]
    await solera.deploy(project("2"))
    assert await solera.stale("ingest") != []
    await solera.run("ingest")
    assert seen[-1] == ([], ["a", "b"], [])


@pytest.mark.xfail(strict=True, reason="not built: behaviors.md gap 9")
async def test_src_9_a_copied_source_keeps_each_changed_key_as_committed(solera):
    """`copy=True`: at commit, each changed key is copied into a Solera store;
    consumers read the copy, whatever the outside holds since."""

    outside, got = Outside(), []

    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id"))
    def items(ctx, feed: list):
        got.append(sorted((r["id"], r["v"]) for r in feed))
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    project = Project(
        assets=[items],
        sources=[Source("feed", key="id", store="ext", copy=True)],
        stores={"ext": SourceStore(solera.root / "ext", outside)},
        default_store=FileStore(solera.root / "data"),
    )
    await solera.deploy(project)
    outside.feed.update({"k1": "1", "k2": "1"})
    await solera.commit_source("feed", upsert={"k1": "1", "k2": "1"})
    outside.feed.update({"k1": "2"})  # moved on outside, no commit
    del outside.feed["k2"]
    await solera.run("items")
    assert got == [[("k1", "1"), ("k2", "1")]]
    assert await solera.rows("items") == {"k1": "1", "k2": "1"}


async def test_src_11_an_observable_source_commits_on_its_schedule(solera):
    """`Source(observe=Every(…))`: a version, a full map, or a patch with a
    cursor, each its source's commit."""

    pages = [Observed(upsert={"x": "1", "y": "1"}, cursor="p1"), Observed(remove=["x"], cursor="p2")]
    cursors = []

    class Versioned(Source):
        def observe(self, ctx):
            return "2026-10-02"

    class Landing(Source):
        def observe(self, ctx, bucket):
            return dict(bucket)

    class Feed(Source):
        def observe(self, ctx):
            cursors.append(ctx.cursor)
            return pages.pop(0) if pages else None

    project = Project(
        sources=[
            Versioned("table", observe=Every(1)),
            Landing("landing", key="id", observe=Every(1)),
            Feed("events", key="id", observe=Every(1)),
        ],
        resources={"bucket": {"a": "e1", "b": "e2"}},
    )
    await solera.deploy(project)
    await solera.sensors(3)  # each due at once
    assert (await solera.head("table"))["version"] == "2026-10-02"
    assert sorted(await solera.keys("landing")) == ["a", "b"]
    assert sorted(await solera.keys("events")) == ["x", "y"]
    runs = await solera.runs()
    assert sorted(r["source"] for r in runs) == ["events", "landing", "table"]
    await solera.sensors(3)  # a second on
    assert sorted(await solera.keys("events")) == ["y"]
    assert cursors == [None, "p1"]
    assert len(await solera.runs()) == 4  # the same version and map again commit nothing
