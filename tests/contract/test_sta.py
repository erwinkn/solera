"""STA: staleness, completeness and statuses (docs/behaviors.md)."""

import asyncio

import pytest
from solera.sdk import DynamicPartitions, Incremental, Output, Project, Retry, Source, StaticPartitions, asset
from solera.stores import FileStore

from tests.contract.example import Example
from tests.contract.harness import Outside, Refused, SourceStore, rebuild
from tests.contract.test_key import console

INPUT, UPSTREAM, DEFINITION = "input changed", "upstream stale", "definition changed"


async def rollup(solera, name: str) -> dict:
    """An asset's roll-up, as the console's graph reads it (`GET /assets:status`)."""

    return (await solera.get("/assets:status"))["assets"][name]


async def partition_statuses(solera, name: str) -> dict[str, str]:
    return {p["partition"]: p["status"] for p in (await solera.get(f"/partitions/{name}"))["partitions"]}


# -- a lineage: feed -> items -> copy, summary -------------------------------------


def lineage(root, outside: Outside, copy_v: str = "1") -> Project:
    """feed (a keyed source) -> items -> copy, both keyed incremental; and
    `summary`, reading `items` whole."""

    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id"))
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(inputs={"items": Incremental()}, outputs=Output("copy", key="id"), version=copy_v)
    def copy(ctx, items: list):
        return rebuild(ctx.batch["items"], [{"id": r["id"], "v": r["v"]} for r in items])

    @asset(inputs={"items": "items"})
    def summary(items: list):
        return {"n": len(items)}

    return Project(
        assets=[items, copy, summary],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(root / "ext", outside)},
        default_store=FileStore(root / "out"),
    )


async def built_lineage(solera, **kw) -> Outside:
    outside = Outside()
    await solera.deploy(lineage(solera.root, outside, **kw))
    outside.feed.update(k1="1", k2="1")
    await solera.commit_source("feed", upsert={"k1": "1", "k2": "1"})
    await solera.run(["copy", "summary"], upstream=True)
    return outside


# -- a wider one: feed -> items -> checks, copy, count; feed -> fchecks --------------


def wide(root, outside: Outside, *, items_store="a", checks_v="1", copy_v="1", count_v="1") -> Project:
    """`feed` (a keyed source) -> `items` (a keyed copy, run by hand) ->
    `checks` (each=True, excluding `x*`, with the unkeyed source `knob` a
    dep), `copy` (keyed, incremental, excluding `x*`) and `count`
    (unkeyed); `fchecks`, each=True over `feed`. No automations."""

    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id", store=items_store))
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(
        inputs={"item": Incremental("items", batch_size=2, exclude=["x*"], each=True)},
        deps=["knob"],
        outputs=Output("checks", key="id"),
        version=checks_v,
    )
    async def checks(ctx, item: list):
        return [{"v": f"{item[0]['v']}.{checks_v}"}]

    @asset(inputs={"items": Incremental(exclude=["x*"])}, outputs=Output("copy", key="id"), version=copy_v)
    def copy(ctx, items: list):
        return rebuild(ctx.batch["items"], [{"id": r["id"], "v": r["v"]} for r in items])

    @asset(inputs={"items": Incremental()}, outputs=Output("count"), version=count_v)
    def count(ctx, items: list):
        return {"rows": len(items)}

    @asset(inputs={"row": Incremental("feed", each=True)}, outputs=Output("fchecks", key="id"))
    async def fchecks(ctx, row: list):
        return [{"v": row[0]["v"]}]

    return Project(
        assets=[items, checks, copy, count, fchecks],
        sources=[Source("feed", key="id", store="ext"), Source("knob")],
        stores={
            "ext": SourceStore(root / "ext", outside),
            "a": FileStore(root / "a"),
            "b": FileStore(root / "b"),
        },
        default_store=FileStore(root / "default"),
    )


async def built_wide(solera, keys: dict, **decl) -> Outside:
    """`knob` at version 0, `items` holding `keys`, and `checks`, `copy`
    and `count` built from it."""

    outside = Outside()
    await solera.deploy(wide(solera.root, outside, **decl))
    outside.feed.update(keys)
    await solera.commit_source("knob", version="0")
    await solera.commit_source("feed", upsert=dict(keys))
    await solera.run("items")
    await solera.run(["checks", "copy", "count"])
    return outside


async def change(solera, outside: Outside, upserts=(), removes=()) -> None:
    outside.feed.update(dict.fromkeys(upserts, "2"))
    for k in removes:
        outside.feed.pop(k, None)
    await solera.commit_source("feed", upsert=dict.fromkeys(sorted(upserts), "2"), remove=sorted(removes))
    await solera.run("items")


async def stale(solera, name: str) -> bool:
    """Whether the partition's status is `stale`; and the asset's roll-up agrees."""

    stale = await solera.status(name) == "stale"
    assert (await rollup(solera, name))["stale"] is stale
    return stale


# -- STA-1 ---------------------------------------------------------------------


async def test_sta_1_a_partitions_status_is_one_of_six(solera):
    """`copy` by site: removed (no longer a site), running, failed, stale,
    materialized and missing; built from some keys alone, it is stale, not
    `missing`, though incomplete."""

    sites, content = DynamicPartitions("sites"), {}

    @asset(outputs=Output("files", key="id"), partitions={"site": sites})
    def files(ctx):
        return [{"id": k, "v": v} for k, v in content[ctx.partition].items()]

    @asset(
        inputs={"files": Incremental()},
        outputs=Output("copy", key="id"),
        partitions={"site": sites},
        retries=Retry(n=0),
    )
    def copy(ctx, files: list):
        if ctx.config.get("fail"):
            raise RuntimeError("boom")
        return rebuild(ctx.batch["files"], files)

    await solera.deploy(Project(assets=[files, copy], sources=[sites], default_store=FileStore(solera.root)))
    names = ["built", "changed", "partial", "failing", "running", "never", "gone"]
    content.update((n, {"k1": 1, "k2": 1}) for n in names)
    await solera.commit_source("sites", upsert=names)
    await solera.run("files", partitions=names)
    await solera.run("copy", partitions=["built", "changed", "gone"])
    content["changed"]["k1"] = 2
    await solera.run("files", partitions=["changed"])
    await solera.run("copy", partitions=["partial"], keys={"files": ["k1"]})
    await solera.run("copy", partitions=["failing"], config={"fail": True}, expect="failed")
    await solera.commit_source("sites", remove=["gone"])
    await solera.submit("copy", partitions=["running"])
    assert await partition_statuses(solera, "copy") == {
        "gone": "removed",
        "running": "running",
        "failing": "failed",
        "changed": "stale",
        "partial": "stale",
        "built": "materialized",
        "never": "missing",
    }
    assert not (await solera.head("copy", "partial"))["materialized"]  # incomplete, and shown stale


async def test_sta_1_a_failed_partition_reports_materialized_after_a_success(solera):
    days = StaticPartitions(["2026-09-18", "2026-09-19"])

    @asset(partitions={"days": days}, retries=Retry(n=0))
    def flaky(ctx):
        if ctx.config.get("fail"):
            raise RuntimeError("boom")
        return {"day": ctx.partition}

    await solera.deploy(Project(assets=[flaky], default_store=FileStore(solera.root)))
    await solera.run("flaky", partitions=["2026-09-19"], config={"fail": True}, expect=None)
    assert (await partition_statuses(solera, "flaky"))["2026-09-19"] == "failed"

    await solera.run("flaky", partitions=["2026-09-19"])
    parts = (await solera.get("/partitions/flaky"))["partitions"]
    done = next(p for p in parts if p["partition"] == "2026-09-19")
    assert done["status"] == "materialized" and done["last_outcome"] == "succeeded"


# -- STA-2 ---------------------------------------------------------------------


async def test_sta_2_a_partition_may_be_stale_for_several_reasons(solera):
    """copy's version is bumped while items is stale: both reasons, in a
    stable order. Rerunning items leaves copy's input changed and its
    definition; a default run of copy clears both."""

    outside = await built_lineage(solera)
    await solera.deploy(lineage(solera.root, outside, copy_v="2"))
    outside.feed["k2"] = "2"
    await solera.commit_source("feed", upsert={"k2": "2"})
    assert await solera.stale("copy") == [UPSTREAM, DEFINITION]
    await solera.run("items")
    assert await solera.stale("copy") == [INPUT, DEFINITION]
    await solera.run("copy")
    assert await solera.stale("copy") == []


async def test_sta_2_a_stale_status_carries_every_reason_that_holds(solera):
    """`copy`'s version is bumped and `feed` commits, `items` not yet
    rerun: `copy` is stale for its definition and its upstream at once."""

    outside = await built_wide(solera, {"k1": "1"})
    await solera.deploy(wide(solera.root, outside, copy_v="2"))
    outside.feed["k1"] = "2"
    await solera.commit_source("feed", upsert={"k1": "2"})
    assert await stale(solera, "copy")
    assert set(await solera.stale("copy")) == {DEFINITION, UPSTREAM}


async def test_sta_2_a_reset_upstream_and_a_new_version_give_both_reasons(solera):
    """`items` reset (moved, its content replaced) and then `count`'s
    version bumped: `count` is stale for both; one full run under the new
    version, reading the new `items`, clears both."""

    outside = await built_wide(solera, {"k1": "1"})
    await solera.deploy(wide(solera.root, outside, items_store="b"))  # items reset
    await solera.run("items")
    await solera.deploy(wide(solera.root, outside, items_store="b", count_v="2"))
    assert set(await solera.stale("count")) == {INPUT, DEFINITION}
    await solera.run("count")
    assert await solera.stale("count") == []


# -- STA-4 ---------------------------------------------------------------------


async def test_sta_4_staleness_runs_down_the_lineage(solera):
    """feed commits: items' input changed; copy and summary read an items
    that has not moved, yet are stale because items is. Once items reruns,
    their own inputs changed; once they rerun, all are fresh."""

    outside = await built_lineage(solera)
    assert [await solera.stale(n) for n in ("items", "copy", "summary")] == [[], [], []]
    outside.feed["k1"] = "2"
    await solera.commit_source("feed", upsert={"k1": "2"})
    assert await solera.stale("items") == [INPUT]
    assert await solera.stale("copy") == [UPSTREAM]
    assert await solera.stale("summary") == [UPSTREAM]
    assert (await rollup(solera, "copy"))["stale"] is True
    await solera.run("items")
    assert await solera.stale("items") == []
    assert await solera.stale("copy") == [INPUT]
    assert await solera.stale("summary") == [INPUT], "a whole input at another version"
    await solera.run(["copy", "summary"])
    assert [await solera.stale(n) for n in ("items", "copy", "summary")] == [[], [], []]
    assert (await rollup(solera, "copy"))["stale"] is False


async def test_sta_4_staleness_is_transitive_down_a_chain(solera):
    """Three levels: `feed` commits k2 and `items` has not rerun. `items`
    is stale ("input changed"); `copy`, `count` and every key of
    `checks` are stale ("upstream stale"), though `items` has not moved.
    Once `items` runs, they are stale for their own input instead."""

    outside = await built_wide(solera, {"k1": "1", "k2": "1"})
    outside.feed["k2"] = "2"
    await solera.commit_source("feed", upsert={"k2": "2"})
    assert await solera.stale("items") == [INPUT]
    for name in ("checks", "copy", "count"):
        assert await solera.stale(name) == [UPSTREAM], name
        assert await stale(solera, name), name
    assert set(await solera.stale_keys("checks")) == {"k1", "k2"}
    await solera.run("items")
    assert not await stale(solera, "items")
    for name in ("checks", "copy", "count"):
        assert await solera.stale(name) == [INPUT], name
    assert set(await solera.stale_keys("checks")) == {"k2"}


async def test_sta_4_the_running_example_goes_fresh_level_by_level(solera):
    """`feed` commits; `items` is "input changed", `copy`, `tally` and
    `checks` "upstream stale". Once `items` reruns they are "input
    changed"; once they rerun, all are fresh."""

    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await solera.run(["tally", "copy", "checks"])
    await ex.feed({"k1": "2"}, items=False)
    assert await solera.stale("items") == [INPUT]
    for name in ("copy", "tally", "checks"):
        assert await solera.stale(name) == [UPSTREAM], name
    await solera.run("items")
    for name in ("copy", "tally", "checks"):
        assert await solera.stale(name) == [INPUT], name
    await solera.run(["tally", "copy", "checks"])
    for name in ("items", "copy", "tally", "checks", "filtered"):
        assert await solera.stale(name) == [], name


# -- STA-5 ---------------------------------------------------------------------


async def test_sta_5_assets_status_rolls_up_every_asset(solera):
    await console(solera)
    await solera.run("parse", upstream=True)
    await solera.run("parted", partitions="all")
    await solera.run("consume", partitions=["x"])  # its dep, `files`, ran above
    await solera.run("notify")

    status = (await solera.get("/assets:status"))["assets"]
    assert set(status) == {"files", "parse", "parted", "consume", "notify"}
    parse = status["parse"]
    assert parse["partitions"] == {
        "total": 1,
        "materialized": 1,
        "stale": 0,
        "missing": 0,
        "failed": 0,
        "running": 0,
        "removed": 0,
    }
    assert parse["partitioned"] is False and parse["stored_counts"] == {"rejected": 1, "failed": 1}
    assert parse["last"]["outcome"] == "succeeded" and parse["last"]["attempt"].count("/") == 1
    assert parse["repairs"] == 0 and parse["updated_at"]
    assert status["files"]["stored_counts"] is None  # no per-key input
    consume = status["consume"]
    assert consume["partitioned"] and consume["partitions"]["total"] == 2
    assert (consume["partitions"]["materialized"], consume["partitions"]["missing"]) == (1, 1)
    # A job has no head: it is complete once it succeeded.
    assert status["notify"]["partitions"]["materialized"] == 1 and status["notify"]["updated_at"] is None

    # The rollup is not an asset name, and /partitions reads the same statuses.
    response = await solera.request("GET", "/assets/assets:status", ok=False)
    assert response.status_code == 404
    assert await partition_statuses(solera, "consume") == {"x": "materialized", "y": "missing"}


async def test_sta_5_a_domain_too_big_to_list_still_rolls_up(solera):
    grid = {d: StaticPartitions([f"{d}{i}" for i in range(1000)]) for d in "ab"}  # 1,000,000 partitions

    @asset(outputs=Output("rows", keyed=True), partitions=grid)
    def rows(ctx):
        return {"k1": {"text": "1"}, "k2": {"text": "2"}}

    @asset(
        inputs={"row": Incremental("rows", each=True)}, outputs=Output("cells", key="path"), partitions=grid
    )
    def cells(row: dict):
        return [{"value": int(row["text"])}]

    await solera.deploy(Project(assets=[rows, cells], name="grid", default_store=FileStore(solera.root)))
    partition = "a=a7,b=b9"
    await solera.run("rows", partitions=[partition])
    await solera.run("cells", partitions=[partition])

    status = (await solera.get("/assets:status"))["assets"]
    for name in ("rows", "cells"):
        assert status[name]["partitions"] == {
            "total": 1_000_000,
            "materialized": 1,
            "stale": 0,
            "missing": 999_999,
            "failed": 0,
            "running": 0,
            "removed": 0,
        }
    assert (await solera.explain("cells", "k1", partition=partition))["verdict"] == "ok"
    other = "a=a8,b=b9"  # a current partition, never run
    assert (await solera.explain("cells", "k1", partition=other))["verdict"] == "absent"
    with pytest.raises(Refused) as refused:
        await solera.explain("cells", "k1", partition="a=a7,b=b1000")
    assert refused.value.status == 404
    # Listing every partition stays bounded: refused, as an `all` run would be.
    with pytest.raises(Refused) as refused:
        await solera.get("/partitions/rows")
    assert refused.value.status == 400


async def test_sta_5_a_per_key_partition_is_stale_if_any_key_is(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1", "k3": "1"})
    await solera.run("checks")
    await ex.feed({"k2": "2"})
    assert set(await solera.stale_keys("checks")) == {"k2"}
    assert await stale(solera, "checks")


# -- STA-6 ---------------------------------------------------------------------


async def test_sta_6_stale_keys_follow_the_partition_and_say_why(solera):
    """A keyed output that is not `each` has all its keys stale or none, a
    partition-wide reason no key repeats; an unkeyed one has no keys."""

    outside = await built_lineage(solera)
    assert await solera.get("/assets/copy/stale-keys") == {
        "tracked": True,
        "keys": [],
        "next": None,
        "reasons": [],
    }
    outside.feed["k1"] = "2"
    await solera.commit_source("feed", upsert={"k1": "2"})
    page = await solera.get("/assets/copy/stale-keys")
    assert page["keys"] == [{"key": "k1", "reasons": []}, {"key": "k2", "reasons": []}]
    assert page["reasons"] == [UPSTREAM]
    assert (await solera.get("/assets/summary/stale-keys"))["tracked"] is False
    assert await solera.get("/assets/copy/stale-keys", limit=1) == {
        "tracked": True,
        "keys": [{"key": "k1", "reasons": []}],
        "next": "k1",
        "reasons": [UPSTREAM],
    }


async def test_sta_6_a_stale_keys_reasons_are_its_own_and_name_their_input(solera):
    """`checks` (each=True over `items`, its input `item`). k1 changes: the
    key `item` owes says so, naming its input. `checks`' definition
    changes: every key is stale, but the partition-wide reason is on the
    partition alone; k2, stale only for it, lists none, and k1 keeps its own."""

    outside = await built_wide(solera, {"k1": "1", "k2": "1"})
    await change(solera, outside, upserts=["k1"])
    page = await solera.get("/assets/checks/stale-keys")
    own = {"kind": INPUT, "input": "item", "upstream_partition": ""}
    assert page["keys"] == [{"key": "k1", "reasons": [own]}]
    assert page["reasons"] == [INPUT]
    await solera.deploy(wide(solera.root, outside, checks_v="2"))
    page = await solera.get("/assets/checks/stale-keys")
    assert page["keys"] == [{"key": "k1", "reasons": [own]}, {"key": "k2", "reasons": []}]
    assert page["reasons"] == [INPUT, DEFINITION]


async def test_sta_6_a_non_each_keyed_outputs_keys_go_stale_together(solera):
    """Every key of `copy` depends on the whole of `items` under its
    patterns, so a change of `k1` alone makes `k1`, `k2` and `k3` stale;
    a default run clears them all."""

    outside = await built_wide(solera, {"k1": "1", "k2": "1", "k3": "1"})
    assert await solera.stale_keys("copy") == {}
    outside.feed["k1"] = "2"
    await solera.commit_source("feed", upsert={"k1": "2"})
    await solera.run("items")
    assert set(await solera.stale_keys("copy")) == {"k1", "k2", "k3"}
    assert await stale(solera, "copy")
    await solera.run("copy")
    assert await solera.stale_keys("copy") == {}
    assert not await stale(solera, "copy")


# -- STA-7 ---------------------------------------------------------------------


async def test_sta_7_a_key_neither_side_holds_leaves_an_each_partition_fresh(solera):
    """`fchecks` (each=True over `feed`) runs for k1 alone; `feed` then
    removes k2, which `fchecks` never held: nothing is stale, keys or
    partition."""

    outside = await built_wide(solera, {"k1": "1", "k2": "1"})
    await solera.run("fchecks", keys={"feed": ["k1"]})
    outside.feed.pop("k2")
    await solera.commit_source("feed", remove=["k2"])
    assert await solera.stale_keys("fchecks") == {}
    assert not await stale(solera, "fchecks"), "stale, with no stale key"


async def test_sta_7_a_key_added_and_removed_again_leaves_it_fresh(solera):
    """`fchecks` runs: fresh. `feed` adds k3: `fchecks` lacks it, so k3 is
    stale. `feed` removes k3 again: neither side holds it."""

    outside = await built_wide(solera, {"k1": "1", "k2": "1"})
    await solera.run("fchecks")
    assert not await stale(solera, "fchecks")
    outside.feed["k3"] = "1"
    await solera.commit_source("feed", upsert={"k3": "1"})
    assert set(await solera.stale_keys("fchecks")) == {"k3"}
    outside.feed.pop("k3")
    await solera.commit_source("feed", remove=["k3"])
    assert await solera.stale_keys("fchecks") == {}
    assert not await stale(solera, "fchecks")


async def test_sta_7_a_key_a_keys_run_never_held_leaves_it_fresh_when_it_goes(solera):
    """`fchecks` runs for k1 alone; k2 is missing, so it is stale. `feed`
    removes k2: neither side holds it."""

    outside = await built_wide(solera, {"k1": "1", "k2": "1"})
    await solera.run("fchecks", keys={"feed": ["k1"]})
    assert set(await solera.stale_keys("fchecks")) == {"k2"}
    assert await solera.stale("fchecks") == [INPUT]
    outside.feed.pop("k2")
    await solera.commit_source("feed", remove=["k2"])
    assert await solera.stale_keys("fchecks") == {}
    assert not await stale(solera, "fchecks")


# -- STA-8 ---------------------------------------------------------------------


async def test_sta_8_an_unkeyed_partition_stays_stale_until_it_reruns(solera):
    """`count` (unkeyed) built, then `items` changes: `count` is stale,
    through any time the engine works, until a run of it."""

    outside = await built_wide(solera, {"k1": "1"})
    assert not await stale(solera, "count")
    outside.feed["k2"] = "1"
    await solera.commit_source("feed", upsert={"k2": "1"})
    await solera.run("items")
    solera.advance(86_400)
    await solera.settle(0.3)
    assert await stale(solera, "count"), "an upstream changed since its catch-up"
    await solera.run("count")
    assert not await stale(solera, "count")


# -- STA-9 ---------------------------------------------------------------------


async def test_sta_9_never_built_is_missing_not_stale(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1"})
    await ex.feed({"k1": "2", "k2": "1"})  # upstream moves on: still never built
    for name in ("tally", "copy", "checks"):
        assert await solera.status(name) == "missing", name
        assert await solera.stale(name) == [], name
        assert (await rollup(solera, name))["stale"] is False, name
    assert await solera.stale_keys("checks") == {}


# -- STA-10 --------------------------------------------------------------------


class Files:
    """`files` (keyed, `rows` its content) -> `copy`, plain or per key, a
    key a batch. While `hold` is clear, every batch of `copy` past the
    first (per key: every key but the first) waits for it, so a run can be
    cancelled midway."""

    def __init__(self, solera, rows: dict, *, each: bool = False, include=None, version: str = "1"):
        self.solera, self.rows, self.each, self.include, self.version = solera, rows, each, include, version
        self.hold = asyncio.Event()
        self.hold.set()
        self.waiting = False  # a batch is held

    async def deploy(self, **changes) -> "Files":
        for name, value in changes.items():
            setattr(self, name, value)
        await self.solera.deploy(self.project(), cancel_grace=0.2, heartbeat_seconds=0.1)
        return self

    async def held(self) -> None:
        self.waiting = True
        await self.hold.wait()
        self.waiting = False

    def project(self) -> Project:
        files_, rows = self, self.rows

        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in rows.items()]

        if self.each:

            @asset(
                inputs={"files": Incremental(batch_size=1, include=self.include, each=True)},
                outputs=Output("copy", key="id"),
                version=self.version,
            )
            async def copy(ctx, files: list):
                if ctx.key != min(rows):
                    await files_.held()
                return [{"v": files[0]["v"]}]

        else:

            @asset(
                inputs={"files": Incremental(batch_size=1, include=self.include)},
                outputs=Output("copy", key="id"),
                version=self.version,
            )
            async def copy(ctx, files: list):
                if ctx.batch["files"].index > 0:
                    await files_.held()
                return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

        return Project(assets=[files, copy], default_store=FileStore(self.solera.root / "data"))

    async def cancel_midway(self, **fields) -> dict:
        """A run of `copy`, cancelled once its first batch committed."""

        self.hold.clear()
        run = await self.solera.submit("copy", **fields)
        await self.solera.until(lambda: self.waiting)
        await self.solera.cancel(run["id"])
        detail = await self.solera.wait(run)
        self.hold.set()
        assert detail["request"]["status"] == "canceled"
        return detail

    async def complete(self) -> bool:
        """Whether `copy` is complete: its head says it is `materialized`."""

        head = await self.solera.head("copy")
        return head is not None and head["materialized"]


async def test_sta_10_never_run_is_not_complete_and_a_first_run_is(solera):
    f = await Files(solera, {"a": 1, "b": 1}).deploy()
    await solera.run("files")
    assert not await f.complete() and await solera.status("copy") == "missing"
    await solera.run("copy")
    assert await f.complete() and await solera.status("copy") == "materialized"


async def test_sta_10_an_incremental_run_cancelled_midway_is_complete_and_stale(solera):
    rows = {"a": 1, "b": 1, "c": 1}
    f = await Files(solera, rows).deploy()
    await solera.run("copy", upstream=True)
    rows.update(a=2, b=2, c=2)
    await solera.run("files")
    await f.cancel_midway()
    assert await f.complete()  # what it last started over from is still a commit: complete, just stale
    assert await solera.stale("copy") == [INPUT]


async def test_sta_10_a_full_run_cancelled_midway_is_not_complete(solera):
    f = await Files(solera, {"a": 1, "b": 1, "c": 1}).deploy()
    await solera.run("copy", upstream=True)
    await f.cancel_midway(mode="full")
    assert not await f.complete()  # it started over, and has not got to the end


async def test_sta_10_a_per_key_full_run_cancelled_midway_is_complete(solera):
    """A per-key consumer's full run compares against what it holds: it is
    complete throughout, its outputs readable while it runs."""

    f = await Files(solera, {"a": 1, "b": 1, "c": 1}, each=True).deploy()
    await solera.run("copy", upstream=True)
    await f.deploy(version="2")  # a full run is due
    await f.cancel_midway()
    assert await f.complete()


async def test_sta_10_a_keys_run_on_a_never_run_partition_is_not_complete(solera):
    f = await Files(solera, {"a": 1, "b": 1}).deploy()
    await solera.run("files")
    await solera.run("copy", keys={"files": ["a"]})
    assert not await f.complete()
    await solera.run("copy")
    assert await f.complete()


async def test_sta_10_a_keys_run_naming_every_upstream_key_is_complete(solera):
    f = await Files(solera, {"a": 1, "b": 1}).deploy()
    await solera.run("files")
    await solera.run("copy", keys={"files": ["a", "b", "z"]})
    assert await f.complete() and await solera.stale("copy") == []


async def test_sta_10_a_partial_full_run_whose_rest_is_deleted_upstream_is_complete(solera):
    rows = {"a": 1, "b": 1, "c": 1}
    f = await Files(solera, rows).deploy()
    await solera.run("copy", upstream=True)
    await f.cancel_midway(mode="full")
    assert not await f.complete()  # b and c, upstream, not processed since it started over
    del rows["b"], rows["c"]
    await solera.run("files")
    assert await f.complete()  # what is left of the walk holds nothing upstream


async def test_sta_10_an_empty_upstream_or_patterns_taking_no_key_complete_at_once(solera):
    f = await Files(solera, {}).deploy()
    await solera.run("copy", upstream=True)
    assert await f.complete()
    f.rows["a"] = 1
    await f.deploy(include=["z*"])
    await solera.run("copy", upstream=True)
    assert await f.complete()


async def test_sta_10_deletions_keep_it_complete(solera):
    rows = {"a": 1, "b": 1}
    f = await Files(solera, rows).deploy()
    await solera.run("copy", upstream=True)
    del rows["a"]
    await solera.run("copy", upstream=True)
    assert await f.complete()


async def test_sta_10_two_inputs_are_complete_together(solera):
    """`both` reads `left` (one key) and `right` (two), a key a batch: its
    first run, cancelled once `left` is walked to the end and `right` is
    not, is not complete; a run to the end is."""

    hold, held = asyncio.Event(), []

    @asset(outputs=Output("left", key="id"))
    def left():
        return [{"id": "a"}]

    @asset(outputs=Output("right", key="id"))
    def right():
        return [{"id": "b"}, {"id": "c"}]

    @asset(
        inputs={"lefts": Incremental("left", batch_size=1), "rights": Incremental("right", batch_size=1)},
        outputs=Output("both", key="id"),
    )
    async def both(ctx, lefts: list, rights: list):
        if ctx.batch["rights"].index > 0 and not hold.is_set():
            held.append(ctx.batch["lefts"].final)
            await hold.wait()
        return [*lefts, *rights]

    project = Project(assets=[left, right, both], default_store=FileStore(solera.root))
    await solera.deploy(project, cancel_grace=0.2, heartbeat_seconds=0.1)
    await solera.run(["left", "right"])
    run = await solera.submit("both")
    await solera.until(lambda: held)
    assert held == [True]  # `left` is walked to the end, `right` is not
    await solera.cancel(run["id"])
    await solera.wait(run)
    hold.set()
    assert not (await solera.head("both"))["materialized"]
    await solera.run("both")
    assert (await solera.head("both"))["materialized"]


async def test_sta_10_partitions_missing_takes_the_incomplete_partitions_alone(solera):
    """`partitions="missing"` skips the complete partitions: a built one,
    and one an empty upstream completes at once; it takes the never-run one
    and the one a `keys=` run left incomplete."""

    days, content, seen = StaticPartitions(["built", "empty", "partial", "never"]), {}, []

    @asset(outputs=Output("files", key="id"), partitions={"day": days})
    def files(ctx):
        return [{"id": k} for k in content[ctx.partition]]

    @asset(inputs={"files": Incremental()}, outputs=Output("copy", key="id"), partitions={"day": days})
    def copy(ctx, files: list):
        seen.append(ctx.partition)
        return rebuild(ctx.batch["files"], files)

    content.update(built=["a"], empty=[], partial=["a", "b"], never=["a"])
    await solera.deploy(Project(assets=[files, copy], default_store=FileStore(solera.root)))
    await solera.run("files", partitions="all")
    await solera.run("copy", partitions=["built", "empty"])
    await solera.run("copy", partitions=["partial"], keys={"files": ["a"]})
    seen.clear()
    await solera.run("copy", partitions="missing")
    assert sorted(seen) == ["never", "partial"]


# -- STA-11 --------------------------------------------------------------------


async def test_sta_11_each_commit_logs_whether_it_was_its_runs_last_batch(solera):
    """History's commit rows say `final` (as `ctx.batch.final`), not
    completeness, which is live: a run of three batches logs false, false,
    true; a cancelled run logs no final commit."""

    f = await Files(solera, {"a": 1, "b": 1, "c": 1}).deploy()
    detail = await solera.run("copy", upstream=True)
    rows = await solera.commits("copy", detail["request"]["id"])
    assert [r["final"] for r in rows] == [False, False, True]
    detail = await f.cancel_midway(mode="full")
    rows = await solera.commits("copy", detail["request"]["id"])
    assert rows and not any(r["final"] for r in rows)
