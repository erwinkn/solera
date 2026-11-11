"""SRC: sources and served versions (docs/behaviors.md)."""

import os
import uuid

import pytest
from solera.sdk import (
    Commit,
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
    RunRequest,
    Source,
    Tick,
    asset,
    source,
)
from solera.stores import FileStore

from tests.contract.example import Example
from tests.contract.harness import Outside, Refused, SourceStore, rebuild
from tests.contract.test_eng import sensed_project


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
    await solera.start()  # served as `solera serve` runs it, upkeep included

    async def expired():
        response = await solera.request("GET", f"/runs/{first['run']}", ok=False)
        return response.status_code == 404

    await solera.until(expired)  # expired under the project's default
    assert await solera.runs() == []


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
    (data / "incoming" / "b.txt").write_text("two")
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
    await solera.commit_source("docs", upsert={"a.txt": "v1", "b.txt": "v1"})
    await solera.commit_source("hashed", upsert={"a.txt": "v1"})
    await solera.run(["sizes", "hashes"])
    (content, served), hashed = seen["docs"], seen["hashed"]
    assert content == {"a.txt": b"one", "b.txt": b"two"} and served["a.txt"] and served["b.txt"]
    assert served["a.txt"] != served["b.txt"]  # each file's own stamp
    assert hashed == {"a.txt": "sha256:7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"}

    (data / "incoming" / "a.txt").write_text("three")  # changed outside Solera; no commit says so
    await solera.run("sizes", keys={"docs": ["a.txt", "b.txt"]})
    content, again = seen["docs"]
    assert content == {"a.txt": b"three", "b.txt": b"two"}
    assert again["a.txt"] != served["a.txt"]  # the file moved: so did its version
    assert again["b.txt"] == served["b.txt"]  # untouched: the same version


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


# -- target (PR 2) --


# -- SRC-10 (target) --------------------------------------------------------------------


async def test_src_10_a_commit_to_other_keys_while_a_tick_runs_does_not_refuse_it(solera):
    """Another client commits z while a tick that saw a and z runs: the tick
    reports a alone, so its commit, run and cursor land together."""

    await solera.deploy(sensed_project())
    await solera.commit_source("uploads", upsert={"a": "1", "z": "1"})
    [tick] = await solera.sensor_ticks()
    await solera.commit_source("uploads", upsert={"z": "2"})
    before = await solera.keys("uploads")
    outcome = Tick(cursor="c1", commits=[Commit("uploads", upsert={"a": "2"})], runs=[RunRequest("ingest")])
    await solera.sensor_post(tick, outcome)
    after = await solera.keys("uploads")
    assert after["a"] != before["a"] and after["z"] == before["z"]
    assert not (await solera.commit_source("uploads", upsert={"a": "2", "z": "2"}))["changed"]
    assert [r["targets"] for r in await solera.runs() if "ingest" in r["targets"]] == [["ingest"]]
    [row] = (await solera.get("/sensors/watch/ticks"))["ticks"]
    assert row["tick"] == tick["tick"] and row["outcome"] == "requested"
    [tick] = await solera.sensor_ticks(wait=5)
    assert tick["cursor"] == "c1"


async def test_src_10_a_commit_to_a_key_the_tick_reports_refuses_it_whole(solera):
    await solera.deploy(sensed_project())
    await solera.commit_source("uploads", upsert={"a": "1", "z": "1"})
    [tick] = await solera.sensor_ticks()
    await solera.commit_source("uploads", upsert={"a": "3"})
    before = await solera.keys("uploads")
    outcome = Tick(cursor="c1", commits=[Commit("uploads", upsert={"a": "2"})], runs=[RunRequest("ingest")])
    with pytest.raises(Refused) as refused:
        await solera.sensor_post(tick, outcome)
    assert refused.value.status == 409
    assert await solera.keys("uploads") == before
    assert not (await solera.commit_source("uploads", upsert={"a": "3"}))["changed"]
    assert not [r for r in await solera.runs() if "ingest" in r["targets"]]
    [tick] = await solera.sensor_ticks(wait=5)
    assert tick["cursor"] is None


# -- SRC-12 (target) --------------------------------------------------------------------


async def test_src_12_a_load_that_finds_a_source_ahead_of_its_log_records_it(solera):
    """feed's k1 was committed at 2; the outside moves it to 3 before any
    commit says so, and items loads k1@3: feed's log now says k1@3 (a
    commit saying so changes nothing), and items owes nothing."""

    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.feed({"k1": "2"}, items=False)
    ex.outside.feed["k1"] = "3"
    [batch] = await ex.run("items")
    assert batch.classes == ([], ["k1"], []) and await solera.rows("items") == {"k1": "3", "k2": "1"}
    assert await solera.stale("items") == []
    assert await ex.run("items") == []
    assert not (await solera.commit_source("feed", upsert={"k1": "3"}))["changed"]


async def test_src_12_a_log_that_moved_since_the_batchs_head_keeps_its_version(solera):
    """As above, but a client commits k1@4 while items' batch runs: the log
    knows better and keeps 4, and items owes k1."""

    outside, steps = Outside(), []

    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id"))
    async def items(ctx, feed: list):
        if steps:
            await steps.pop(0)()
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    await solera.deploy(
        Project(
            assets=[items],
            sources=[Source("feed", key="id", store="ext")],
            stores={"ext": SourceStore(solera.root / "ext", outside)},
            default_store=FileStore(solera.root / "data"),
        )
    )

    async def commit(**upserts):
        outside.feed.update(upserts)
        await solera.commit_source("feed", upsert=upserts)

    async def later():
        await commit(k1="4")

    await commit(k1="1")
    await solera.run("items")
    await commit(k1="2")
    outside.feed["k1"] = "3"
    steps.append(later)
    await solera.run("items")
    assert await solera.rows("items") == {"k1": "3"} and await solera.stale("items") == ["input changed"]
    assert not (await solera.commit_source("feed", upsert={"k1": "4"}))["changed"]
    await solera.run("items")
    assert await solera.rows("items") == {"k1": "4"} and await solera.stale("items") == []


# -- SRC-13: a keyed loader may serve one Arrow table ------------------------------------------


def arrow_rows(rows: list[dict]):
    """`rows`, each with its `etag`, as one Arrow table: its columns even when empty."""

    import pyarrow as pa

    schema = pa.schema([("id", pa.string()), ("v", pa.int64()), ("etag", pa.string())])
    return pa.Table.from_pylist(rows, schema=schema)


async def test_src_13_a_keyed_loader_may_serve_one_arrow_table(solera):
    """`Loaded(table, version="etag")`: the consumer gets the rows of the keys
    asked, in key order and without the version column, whatever the table's
    order and whatever else it holds; `served` each key's version from its
    column. A key with no row was served absent."""

    outside = {"a": 1, "b": 2, "z": 9}  # z is never asked
    seen = []

    @source(key="id")
    async def files(keys, ctx):
        rows = [{"id": k, "v": v, "etag": f"etag-{v}"} for k, v in outside.items()]
        return Loaded(arrow_rows(rows[::-1]), version="etag")

    @asset(inputs={"files": Incremental()}, outputs=Output("copy", key="id"))
    def copy(ctx, files: list):
        b = ctx.batch["files"]
        seen.append((files, {k: v for k, v in b.served.items() if v}, sorted(b.removed)))
        return rebuild(b, files)

    await solera.deploy(Project(assets=[copy], sources=[files], default_store=FileStore(solera.root / "out")))
    await solera.commit_source("files", upsert={"a": "1", "b": "2"})
    await solera.run("copy")
    assert seen == [([{"id": "a", "v": 1}, {"id": "b", "v": 2}], {"a": "etag-1", "b": "etag-2"}, [])]
    del outside["b"]
    outside["a"] = 3
    await solera.commit_source("files", upsert={"a": "3", "b": "3"})
    await solera.run("copy")
    assert seen[-1] == ([{"id": "a", "v": 3}], {"a": "etag-3"}, ["b"])  # b has no row: served absent
    assert await solera.rows("copy") == {"a": 3}


@pytest.mark.parametrize("form", ["table", "dict"])
async def test_src_13_the_rows_come_as_the_parameter_asks(solera, form):
    """The same rows, served as a table or a dict, to a list, a DataFrame and
    an Arrow table."""

    import pandas as pd
    import pyarrow as pa

    got = {}

    @source(key="id")
    async def files(keys, ctx):
        if form == "dict":
            return {k: Loaded({"id": k, "v": int(k[1:])}, version="e") for k in keys}
        return Loaded(arrow_rows([{"id": k, "v": int(k[1:]), "etag": "e"} for k in keys]), version="etag")

    @asset(inputs={"files": Incremental()})
    def as_list(files: list):
        got["list"] = files

    @asset(inputs={"files": Incremental()})
    def as_frame(files: pd.DataFrame):
        got["frame"] = files.to_dict("records")

    @asset(inputs={"files": Incremental()})
    def as_table(files: pa.Table):
        got["table"] = (files.schema.names, files.to_pylist())

    await solera.deploy(Project(assets=[as_list, as_frame, as_table], sources=[files]))
    await solera.commit_source("files", upsert={"k1": "1", "k2": "2"})
    await solera.run(["as_list", "as_frame", "as_table"])
    rows = [{"id": "k1", "v": 1}, {"id": "k2", "v": 2}]
    assert got == {"list": rows, "frame": rows, "table": (["id", "v"], rows)}


async def test_src_13_a_per_key_consumer_gets_each_keys_row(solera):
    got = {}

    @source(key="id")
    async def files(keys, ctx):
        return Loaded(arrow_rows([{"id": k, "v": int(k[1:]), "etag": "e"} for k in keys]), version="etag")

    @asset(inputs={"row": Incremental("files", each=True)}, outputs=Output("each", key="id"))
    def each(ctx, row: list):
        got[ctx.key] = row
        return row

    await solera.deploy(Project(assets=[each], sources=[files]))
    await solera.commit_source("files", upsert={"k1": "1", "k2": "2"})
    await solera.run("each")
    assert got == {"k1": [{"id": "k1", "v": 1}], "k2": [{"id": "k2", "v": 2}]}


@pytest.mark.parametrize(
    ("columns", "error"),
    [
        ({"id": ["a", "a"], "etag": ["1", "2"]}, 'key "a" has two rows'),
        ({"id": ["a", None], "etag": ["1", "2"]}, 'column "id" has nulls'),
        ({"etag": ["1"]}, 'no column "id"'),
        ({"id": ["a"]}, 'no column "etag"'),
        ({"id": ["a"], "etag": [1]}, 'version column "etag" must hold strings, not Int64'),
        ({"id": ["a", "b"], "etag": ["1", None]}, 'column "etag" has nulls'),
    ],
    ids=[
        "duplicate key",
        "null key",
        "no key column",
        "no version column",
        "version not a string",
        "null version",
    ],
)
async def test_src_13_a_table_that_cannot_say_what_it_served_fails_the_load(solera, columns, error):
    import pyarrow as pa

    @source(key="id")
    async def files(keys, ctx):
        return Loaded(pa.table(columns), version="etag")

    @asset(inputs={"files": Incremental()})
    def count(files: list):
        return len(files)

    await solera.deploy(Project(assets=[count], sources=[files]))
    await solera.commit_source("files", upsert={"a": "1"})
    detail = await solera.run("count", expect="failed")
    [[attempt]] = detail["attempts"].values()
    assert f"source 'files': {error}" in attempt["error"]


@pytest.mark.parametrize("seed", range(4))
async def test_src_13_the_table_and_dict_forms_serve_alike(solera, seed):
    """Random histories, the same for two sources: `d` answers a dict of
    `Loaded` rows, `t` one Arrow table, shuffled and holding keys not asked.
    Commits name versions behind, at and ahead of what is served, and keys
    the outside has lost; some runs are full. Each consumer's loads ask the
    same keys, and its batches get the same classes, served versions and
    rows; the outputs end equal."""

    import random

    rng = random.Random(seed)
    universe = [f"k{i:02}" for i in range(14)]
    outside: dict[str, int] = {}
    asked = {"d": [], "t": []}
    seen = {"d": [], "t": []}

    @source(key="id")
    async def d(keys, ctx):
        asked["d"].append(list(keys))
        return {k: Loaded({"id": k, "v": v}, version=f"e{v}") for k, v in outside.items()}

    @source(key="id")
    async def t(keys, ctx):
        asked["t"].append(list(keys))
        rows = [{"id": k, "v": v, "etag": f"e{v}"} for k, v in outside.items()]
        random.Random(repr(keys)).shuffle(rows)
        return Loaded(arrow_rows(rows), version="etag")

    def record(b, rows):
        served = dict(b.served.items())
        return (b.index, b.reset, b.added, b.updated, b.removed, b.unchanged, served, rows)

    @asset(inputs={"d": Incremental(batch_size=4)}, outputs=Output("out_d", key="id"))
    def use_d(ctx, d: list):
        seen["d"].append(record(ctx.batch["d"], d))
        return rebuild(ctx.batch["d"], d)

    @asset(inputs={"t": Incremental(batch_size=4)}, outputs=Output("out_t", key="id"))
    def use_t(ctx, t: list):
        seen["t"].append(record(ctx.batch["t"], t))
        return rebuild(ctx.batch["t"], t)

    await solera.deploy(
        Project(assets=[use_d, use_t], sources=[d, t], default_store=FileStore(solera.root / "out"))
    )
    for _ in range(8):
        for k in rng.sample(universe, 5):  # the outside moves: a key added or moved, or lost
            if rng.random() < 0.3:
                outside.pop(k, None)
            else:
                outside[k] = outside.get(k, 0) + 1
        upsert = {k: f"e{outside.get(k, 0) + rng.choice([-1, 0, 0, 1])}" for k in rng.sample(universe, 6)}
        remove = [k for k in rng.sample(universe, 2) if k not in upsert]
        for name in ("d", "t"):
            await solera.commit_source(name, upsert=upsert, remove=remove)
        mode = {"mode": "full"} if rng.random() < 0.15 else {}
        await solera.run("use_d", **mode)
        await solera.run("use_t", **mode)
    assert asked["t"] == asked["d"]
    assert seen["t"] == seen["d"]
    assert any(b[2] or b[3] for b in seen["d"]) and any(b[4] for b in seen["d"])
    assert await solera.rows("out_t") == await solera.rows("out_d")
