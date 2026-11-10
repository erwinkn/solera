"""STA: staleness, completeness and statuses (docs/behaviors.md)."""

import asyncio

import pytest
from solera import Transient
from solera.sdk import DynamicPartitions, Incremental, Output, Project, Retry, Source, StaticPartitions, asset
from solera.stores import FileStore

from tests.contract.example import Batch, Example
from tests.contract.harness import Outside, Refused, SourceStore, rebuild
from tests.contract.test_key import STORED, Checks, busy_project, console

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


# -- target (PR 2) --
# Full runs take form (a): the first commit clears the output, commit numbers
# go on, and every reader of an incomplete upstream is held until the run
# completes (docs/tracking-design.md §10, the full-run verdict). Staleness is
# exact whenever asked (§6, the staleness verdict).

INCOMPLETE = "upstream incomplete"


class Rebuild:
    """`feed` (a keyed source) -> `items` -> `copy` (keyed, `batch_size=2`)
    -> `archive` (keyed, incremental over `copy`) and `summary` (reading
    `copy` whole). Each producer records what it received; `pause(i)`
    holds copy's next batch `i`, once begun, until released, and
    `failing` names the batches of copy that fail, once each. Used as
    `async with`: leaving it releases every pause, whatever the test did."""

    def __init__(self, solera):
        self.solera, self.outside = solera, Outside()
        self.seen: dict[str, list[Batch]] = {}
        self.summaries: list[list[str]] = []
        self.pauses: dict[int, tuple[asyncio.Event, asyncio.Event]] = {}
        self.failing: set[int] = set()
        self.releases: list[asyncio.Event] = []
        self.release = asyncio.Event()

    async def __aenter__(self) -> "Rebuild":
        return self

    async def __aexit__(self, *exc) -> None:
        for release in self.releases:
            release.set()

    def pause(self, index: int = 1) -> tuple[asyncio.Event, asyncio.Event]:
        begun, release = asyncio.Event(), asyncio.Event()
        self.pauses[index] = (begun, release)
        self.releases.append(release)
        return begun, release

    def project(
        self, store=None, decl: dict | None = None, archive_executor=None, version: str = "1"
    ) -> Project:
        ex = self

        @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id"))
        def items(ctx, feed: list):
            return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

        @asset(
            inputs={"items": Incremental(batch_size=2)},
            outputs=Output("copy", key="id", store="copies" if store else None, **(decl or {})),
            version=version,
            retries=Retry(0),
        )
        async def copy(ctx, items: list):
            batch = ctx.batch["items"]
            if batch.index in ex.failing:
                ex.failing.discard(batch.index)
                raise RuntimeError("copy fails midway")
            ex.seen.setdefault("copy", []).append(Batch.of(batch, items))
            if batch.index in ex.pauses:
                begun, release = ex.pauses.pop(batch.index)
                begun.set()
                await release.wait()
            return rebuild(batch, [{"id": r["id"], "v": r["v"]} for r in items])

        @asset(inputs={"copy": Incremental()}, outputs=Output("archive", key="id"), executor=archive_executor)
        def archive(ctx, copy: list):
            ex.seen.setdefault("archive", []).append(Batch.of(ctx.batch["copy"], copy))
            return rebuild(ctx.batch["copy"], [{"id": r["id"], "v": r["v"]} for r in copy])

        @asset(inputs={"copy": "copy"})
        def summary(copy: list):
            ex.summaries.append(sorted(r["id"] for r in copy))
            return {"n": len(copy)}

        stores = {"ext": SourceStore(self.solera.root / "ext", self.outside)}
        return Project(
            assets=[items, copy, archive, summary],
            sources=[Source("feed", key="id", store="ext")],
            stores=stores | ({"copies": store} if store else {}),
            default_store=FileStore(self.solera.root / "out"),
        )

    async def start(self, keys=("k1", "k2", "k3", "k4"), **decl) -> "Rebuild":
        """Served, with copy and summary built from `keys` at 1, and archive
        too unless it runs on a pool (`decl`: `project`'s)."""

        await self.deploy(**decl)
        await self.feed(dict.fromkeys(keys, "1"))
        await self.solera.run(["copy", "summary"])
        if decl.get("archive_executor") is None:
            await self.solera.run("archive")
        return self

    async def deploy(self, **decl) -> None:
        await self.solera.deploy(self.project(**decl), cancel_grace=0.2, heartbeat_seconds=0.2)

    async def feed(self, upserts: dict | None = None, removes=()) -> None:
        """`feed` changes outside and is committed; items follows."""

        self.outside.feed.update(upserts or {})
        for k in removes:
            self.outside.feed.pop(k, None)
        await self.solera.commit_source("feed", upsert=dict(upserts or {}), remove=list(removes))
        await self.solera.run("items")

    async def midway(self, **fields) -> dict:
        """A full run of copy over k1, k2, k4, k5 (feed drops k3 and adds k5
        first), two batches, its second held once begun: the run."""

        await self.feed({"k5": "1"}, removes=["k3"])
        begun, self.release = self.pause(1)
        run = await self.solera.submit("copy", mode="full", **fields)
        await self.solera.until(begun.is_set)
        return run

    async def finish(self, run: dict) -> dict:
        """The held batch goes on: the full run, settled."""

        self.release.set()
        return await self.solera.wait(run)

    async def failed_midway(self) -> dict:
        """A full run of copy over k1, k2, k4, k5, as `midway`, whose second
        batch fails after the first committed: the run, settled."""

        await self.feed({"k5": "1"}, removes=["k3"])
        self.failing.add(1)
        return await self.solera.run("copy", mode="full", expect="failed")


async def task_of(solera, run: dict, name: str) -> dict:
    """`name`'s task in the run, as `GET /runs/{id}` shows it."""

    [task] = [t for t in (await solera.get(f"/runs/{run['id']}"))["tasks"] if t["asset"] == name]
    return task


async def decided(solera, run: dict, name: str) -> dict:
    """`name`'s task in the run once the engine has decided on it: held by
    a gate, or taken (no longer waiting)."""

    async def done() -> bool:
        task = await task_of(solera, run, name)
        return (task.get("held") or [None])[0] == INCOMPLETE or task["status"] not in ("queued", "claimable")

    await solera.until(done)
    return await task_of(solera, run, name)


# -- STA-3 (target) ---------------------------------------------------------------------


async def test_sta_3_a_failing_key_whose_retry_is_not_due_does_not_make_it_stale(solera):
    """`checks` (per key) is told to come back in a minute for a: a is
    owed, but a default run would load nothing until its retry is due, so
    the partition is not stale. Once due, it is, and a run takes a."""

    tries = []
    await solera.deploy(busy_project(solera.root, tries))
    await solera.run("checks", upstream=True)
    assert (await solera.outcomes("checks"))["a"]["outcome"] == "retrying"
    assert await solera.stale("checks") == [] and await solera.status("checks") != "stale"
    await solera.run("checks")
    assert tries == ["a"]  # a default run loaded nothing
    solera.advance(61)
    assert await solera.stale("checks") != [] and await solera.status("checks") == "stale"
    await solera.run("checks")
    assert tries == ["a", "a"] and await solera.stale("checks") == []


# -- STA-12 (target) --------------------------------------------------------------------


async def test_sta_12_a_status_right_after_a_prefixless_pattern_change_is_exact(solera):
    """`copy` takes *1, then *2 (patterns with no literal prefix): asked at
    once, it is stale for exactly the keys the change owes, never pending."""

    ex = await Example(solera).start(include=["*1"])
    await ex.feed({f"k{i}": "1" for i in range(1, 5)})
    await ex.run("copy")
    await ex.deploy(include=["*2"])
    assert await solera.status("copy") == "stale" and await solera.stale("copy") == [INPUT]
    [batch] = await ex.run("copy")
    assert batch.classes == (["k2"], [], ["k1"])
    assert await solera.status("copy") == "materialized" and await solera.stale("copy") == []


async def test_sta_12_an_input_a_gate_holds_says_upstream_incomplete_with_no_number(solera):
    async with Rebuild(solera) as r:
        await r.start()
        run = await r.midway()
        reasons = await solera.stale("archive")
        assert INCOMPLETE in reasons and INPUT not in reasons, reasons
        assert await solera.stale_keys("archive") == {}  # no number while held
        await r.finish(run)
        assert await solera.stale("archive") == [INPUT]
        assert set(await solera.stale_keys("archive")) == {"k1", "k2", "k3", "k4", "k5"}  # counted once open


# -- STA-13 (target) --------------------------------------------------------------------


async def test_sta_13_every_reader_of_an_incomplete_upstream_waits_until_its_full_run_completes(solera):
    """Between copy's two batches, archive (keyed) and summary (whole) are
    held: their runs wait, saying upstream incomplete, and start once copy
    completes, reading it whole and complete."""

    async with Rebuild(solera) as r:
        await r.start()
        run = await r.midway()
        archived, summarized = len(r.seen["archive"]), len(r.summaries)
        readers = [await solera.submit(name) for name in ("archive", "summary")]
        for reader, name in zip(readers, ("archive", "summary"), strict=True):
            task = await decided(solera, reader, name)
            assert task["status"] == "queued" and task.get("held") == [INCOMPLETE, "copy"], task
        assert len(r.seen["archive"]) == archived and len(r.summaries) == summarized
        assert (await r.finish(run))["request"]["status"] == "succeeded"
        for reader in readers:
            assert (await solera.wait(reader))["request"]["status"] == "succeeded"
        [batch] = r.seen["archive"][archived:]
        assert batch.classes == (["k5"], ["k1", "k2", "k4"], ["k3"]) and not batch.reset
        assert r.summaries[summarized:] == [["k1", "k2", "k4", "k5"]]
        assert await solera.rows("archive") == await solera.rows("copy")


async def held_for_good(solera, r: Rebuild) -> dict:
    """copy is left incomplete: a run of archive is held, saying so, and an
    alarm names copy. Running copy again completes it, the alarm goes, and
    archive's run goes on: its detail, settled."""

    assert not (await solera.head("copy"))["materialized"]
    reader = await solera.submit("archive")
    archived = len(r.seen["archive"])
    task = await decided(solera, reader, "archive")
    assert task["status"] == "queued" and task.get("held") == [INCOMPLETE, "copy"], task
    assert len(r.seen["archive"]) == archived
    assert await solera.alarmed("copy"), await solera.alarms()
    await solera.run("copy")
    detail = await solera.wait(reader)
    assert (
        await solera.rows("archive")
        == await solera.rows("copy")
        == {"k1": "1", "k2": "1", "k4": "1", "k5": "1"}
    )
    assert not await solera.alarmed("copy"), await solera.alarms()
    return detail


async def test_sta_13_a_full_run_canceled_midway_holds_its_readers_and_raises_an_alarm(solera):
    """Nothing resumes a canceled run: copy stays incomplete, archive stays
    held, and an alarm names copy until copy runs again."""

    async with Rebuild(solera) as r:
        await r.start()
        run = await r.midway()
        await solera.cancel(run["id"])
        assert (await solera.wait(run))["request"]["status"] == "canceled"
        r.release.set()
        assert (await held_for_good(solera, r))["request"]["status"] == "succeeded"


async def test_sta_13_a_full_run_failed_midway_holds_its_readers_and_raises_an_alarm(solera):
    """copy's full run fails in its second batch, after its first committed
    (the clear, with k1 and k2): copy stays incomplete, archive stays held,
    and an alarm names copy until copy runs again."""

    async with Rebuild(solera) as r:
        await r.start()
        await r.failed_midway()
        assert await solera.rows("copy") == {"k1": "1", "k2": "1"}
        assert (await held_for_good(solera, r))["request"]["status"] == "succeeded"


@pytest.mark.postgres
async def test_sta_13_on_a_table_a_batch_in_flight_holds_back_keys_it_loaded_at_another_version(solera):
    """archive's batch, planned with k3 and k4 updated, loads only once
    copy's full run has cleared its table and written k1 and k2: it finds k3
    and k4 absent, at another version than planned, and holds them back.
    archive keeps their rows and is held while copy is incomplete; once copy
    completes, and before archive runs again, it still owes them."""

    import os
    import uuid

    from solera.executors import Pool

    dsn = os.environ.get("SOLERA_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    import psycopg
    from solera_postgres import PostgresStore

    schema = f"contract_{uuid.uuid4().hex[:10]}"
    decl = {"schema": schema, "columns": {"id": "text", "v": "text"}}
    store = PostgresStore("env:SOLERA_TEST_DATABASE_URL")
    try:
        async with Rebuild(solera) as r:
            await r.start(store=store, decl=decl, archive_executor=Pool("p")())
            archive = await solera.submit("archive")
            await solera.work("p", r.project(store, decl, Pool("p")()))
            await solera.wait(archive)
            await r.feed({"k3": "2", "k4": "2"})
            await solera.run("copy")
            archive = await solera.submit("archive")
            assert await solera.offered("p")  # launched: k3 and k4 planned as updated
            run = await r.midway()  # copy's table: k1 and k2 alone
            await solera.work("p", r.project(store, decl, Pool("p")()))
            assert await solera.rows("archive") == {"k1": "1", "k2": "1", "k3": "1", "k4": "1"}
            assert INCOMPLETE in await solera.stale("archive")
            await r.finish(run)
            assert {"k3", "k4"} <= set(await solera.stale_keys("archive"))  # archive has not run again
            await solera.work("p", r.project(store, decl, Pool("p")()))
            assert (await solera.wait(archive))["request"]["status"] == "succeeded"
            assert await solera.rows("archive") == await solera.rows("copy")
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


# -- STA-14 (target) --------------------------------------------------------------------


async def test_sta_14_a_change_reverted_before_a_run_leaves_it_fresh_and_starts_nothing(solera):
    """feed's k1 goes 1 -> 2 -> 1: items is stale while k1 is at 2 and
    materialized again at 1; a run asked for then has nothing to do."""

    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.feed({"k1": "2"}, items=False)
    assert await solera.status("items") == "stale"
    await ex.feed({"k1": "1"}, items=False)
    assert await solera.status("items") == "materialized" and await solera.stale("items") == []
    detail = await solera.run("items")
    assert not any(detail["attempts"].values()) and ex.seen["items"][-1].added == ["k1", "k2"]


async def test_sta_14_a_key_added_and_removed_between_two_runs_is_never_owed(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1"})
    await ex.feed({"k9": "1"}, items=False)
    assert await solera.status("items") == "stale"
    await ex.feed(removes=["k9"], items=False)
    assert await solera.status("items") == "materialized" and await solera.stale("items") == []
    assert await ex.run("items") == []


# -- STA-15 (target) ---------------------------------------------------------------


async def test_sta_15_a_failing_key_not_yet_due_is_shown_not_stale(solera):
    """k2 fails, its retry a minute away, and feed adds k3 and k5: `checks` is
    stale for k3 and k5 alone and shows one failing key. Once they are
    processed it is fresh, k2 still failing, until k2's retry comes due."""

    c = await Checks(solera).deploy()
    await c.feed(k1="1", k2="1")
    await c.run()
    c.failing["k2"] = Transient("busy", retry_after=60)
    await c.feed(k2="2")
    await c.run()
    await c.feed(k3="1", k5="1")
    assert set(await solera.stale_keys("checks")) == {"k3", "k5"}
    assert list(await solera.outcomes("checks", *STORED)) == ["k2"]
    assert await c.run() == ["k3", "k5"]
    assert await solera.stale("checks") == [] and await solera.stale_keys("checks") == {}
    assert list(await solera.outcomes("checks", *STORED)) == ["k2"]
    solera.advance(61)
    assert set(await solera.stale_keys("checks")) == {"k2"}
    assert await solera.stale("checks") != []


async def test_sta_15_a_failing_keys_new_input_makes_it_stale_before_its_retry_is_due(solera):
    c = await Checks(solera).deploy()
    await c.feed(k1="1", k2="1")
    await c.run()
    c.failing["k2"] = Transient("busy", retry_after=3600)
    await c.feed(k2="2")
    await c.run()
    assert await solera.stale_keys("checks") == {}
    await c.feed(k2="3")
    assert set(await solera.stale_keys("checks")) == {"k2"}
