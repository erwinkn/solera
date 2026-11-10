"""RST: upstream resets, removals, moves and renames (docs/behaviors.md)."""

import asyncio
import os
import re
import uuid

import pytest
from solera.sdk import Incremental, Output, Project, Result, Retry, StaticPartitions, asset, job
from solera.stores import FileStore

from tests.contract.example import Example

ENDED = {"succeeded", "failed", "canceled"}


class Gate:
    """A point a producer waits at until the test releases it."""

    def __init__(self):
        self.entered, self.released = asyncio.Event(), asyncio.Event()

    async def wait(self):
        self.entered.set()
        await self.released.wait()


def held_commit(detail: dict) -> str:
    """The commit a run's first attempt would make."""

    [attempts] = detail["attempts"].values()
    return f"{detail['request']['id']}/{attempts[0]['id']}"


def parsing(root, name: str | None = "parse", aliases=(), fail=("a",), calls=None) -> Project:
    """`files` (a, b) → `name`, per key, writing `rows`; the keys in `fail` raise.
    No `name`: `files` alone."""

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a"}, {"id": "b"}]

    def body(ctx, file: list):
        calls.append(ctx.key)
        if ctx.key in fail:
            raise ValueError("bug")
        return [{"id": ctx.key}]

    body.__name__ = name or "parse"
    parse = asset(
        inputs={"file": Incremental("files", each=True)},
        outputs=Output("rows", key="id"),
        aliases=list(aliases),
    )(body)
    return Project(assets=[files, parse] if name else [files], default_store=FileStore(root / "data"))


async def cancelled_as_removed(solera, run: dict) -> list[str]:
    """The assets whose tasks in `run` were cancelled with an error that
    names them and says they are gone from the project, whatever its words."""

    removed = re.compile(r"removed|no longer|not in the project|gone", re.I)
    return sorted(
        t["asset"]
        for t in (await solera.get("/tasks", run=run["id"]))["tasks"]
        if t["status"] == "canceled" and t["asset"] in (t["error"] or "") and removed.search(t["error"] or "")
    )


async def failing(solera, name: str) -> dict:
    """A per-key asset's keys, each with its outcome."""

    return {k["key"]: k["outcome"] for k in (await solera.get(f"/assets/{name}/outcomes"))["keys"]}


# -- RST-1, RST-2: moves ------------------------------------------------------------


async def test_rst_1_moving_an_output_to_another_store_makes_it_a_new_output(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.run("tally")
    await ex.run("copy")
    await ex.deploy(items_store="other")
    assert await solera.head("items") is None and await solera.keys("items") == {}  # nothing copied
    [first] = await ex.run("items")
    assert first.added == ["k1", "k2"]  # its own inputs, read in full
    assert await solera.rows("items") == {"k1": "1", "k2": "1"}
    for name in ("tally", "copy"):
        assert await solera.stale(name) == ["input changed"], name
    [batch] = await ex.run("tally")
    assert batch.reset and batch.added == ["k1", "k2"]  # a full run
    [batch] = await ex.run("copy")
    assert batch.reset and batch.rows == ["k1", "k2"]
    assert await ex.count() == 2 and await ex.stale() == [] and await solera.stale("copy") == []


async def test_rst_1_a_key_the_moved_outputs_first_write_leaves_out_leaves_its_consumers(solera):
    ex = await Example(solera).start()
    await ex.feed({"k10": "1", "k11": "1"})
    await ex.run("copy")
    await solera.run("checks")
    await ex.deploy(items_store="other")
    await ex.feed({"k3": "1"}, removes=["k11"])  # the first write in the new store: k10, k3
    await ex.run("copy")
    await solera.run("checks")
    assert await solera.rows("copy") == {"k10": "1", "k3": "1"}
    assert sorted(await solera.rows("checks")) == ["k10", "k3"]


async def test_rst_1_a_key_removed_after_a_move_leaves_its_consumers(solera):
    ex = await Example(solera).start()
    await ex.feed({"k11": "1"})
    await ex.run("copy")
    await solera.run("checks")
    await ex.deploy(items_store="other")
    await ex.feed({"k3": "1"})
    await ex.run("copy")
    await ex.feed(removes=["k11"])
    await ex.run("copy")
    await solera.run("checks")
    assert await solera.rows("copy") == {"k3": "1"}
    assert sorted(await solera.rows("checks")) == ["k3"]


async def test_rst_2_moving_away_and_back_is_two_resets(solera):
    ex = await Example(solera).start()
    await ex.feed({"a": "1", "b": "1"})
    await ex.run("copy")
    await ex.deploy(items_store="other")
    await ex.deploy()  # and back, nothing written between
    assert await solera.head("items") is None and await solera.keys("items") == {}
    await ex.feed(removes=["b"])  # the new `items` holds no b
    [batch] = await ex.run("copy")
    assert batch.reset and batch.rows == ["a"]
    assert await solera.rows("copy") == {"a": "1"}


# -- RST-3: a name removed and declared again ---------------------------------------


async def test_rst_3_an_asset_removed_and_declared_again_starts_a_new_life(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.run("copy")
    await ex.deploy(copy_name="mirror")  # `copy` removed: no alias carries it over
    await ex.deploy()  # `copy` declared again
    assert await solera.head("copy") is None and await solera.keys("copy") == {}
    [batch] = await ex.run("copy")
    assert batch.added == ["k1", "k2"]  # nothing processed before
    assert await solera.rows("copy") == {"k1": "1", "k2": "1"}


async def test_rst_3_a_per_key_asset_declared_again_has_no_failing_keys_from_before(solera):
    calls: list = []
    await solera.deploy(parsing(solera.root, calls=calls))
    await solera.run("parse", upstream=True, expect=None)
    assert await failing(solera, "parse") == {"a": "failed", "b": "ok"}
    await solera.deploy(parsing(solera.root, None))  # `parse` removed
    await solera.deploy(parsing(solera.root, calls=calls, fail=()))
    assert "failed" not in (await failing(solera, "parse")).values()
    await solera.run("parse")
    assert sorted(calls) == ["a", "a", "b", "b"]  # every key, again
    assert await solera.rows("rows", column="id") == {"a": "a", "b": "b"}


@pytest.mark.parametrize("ends", ["commits", "fails"])
@pytest.mark.parametrize("when", ["before", "while waiting"])
async def test_rst_3_an_attempt_of_the_first_life_commits_nothing_into_the_second(solera, ends, when):
    """Launched before the removal, it ends after the re-add: before the new
    life's first run, or while that run waits."""

    gate = Gate()

    def project(life):
        @asset(outputs=Output("items", key="id"))
        def items():
            return [{"id": "a"}, {"id": "b"}]

        @asset(inputs={"items": Incremental()}, outputs=Output("copy", key="id"), retries=Retry(3, delay=0))
        async def copy(items: list):
            if life == "1":
                await gate.wait()
                if ends == "fails":
                    raise ValueError("the first life fails")
            return [{"id": r["id"], "life": life} for r in items]

        declared = [items, copy] if life else [items]
        return Project(assets=declared, default_store=FileStore(solera.root / "data"))

    await solera.deploy(project("1"))
    await solera.run("items")
    old = await solera.submit("copy")
    await solera.until(gate.entered.is_set)
    await solera.deploy(project(None))
    await solera.deploy(project("2"))
    new = await solera.submit("copy") if when == "while waiting" else None
    gate.released.set()
    detail = await solera.wait(old)
    assert detail["request"]["status"] in ENDED
    head = await solera.head("copy")
    assert head is None or head["commit"] != held_commit(detail)
    assert set((await solera.rows("copy", column="life")).values()) <= {"2"}
    if new is not None:
        await solera.wait(new)
    await solera.run("copy")
    assert await solera.rows("copy", column="life") == {"a": "2", "b": "2"}


async def test_rst_3_a_job_declared_again_does_not_take_its_first_lifes_commit(solera):
    gate, cursors = Gate(), []

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a"}, {"id": "b"}]

    def project(life):
        if life is None:
            return Project(assets=[items], default_store=FileStore(solera.root / "data"))

        @job(inputs={"items": Incremental()}, version=life)
        async def seen(ctx, items: list):
            if life == "1":
                await gate.wait()
            else:
                cursors.append(ctx.cursor)
            return Result(outputs={}, cursor={"life": life})

        return Project(assets=[items, seen], default_store=FileStore(solera.root / "data"))

    await solera.deploy(project("1"))
    await solera.run("items")
    old = await solera.submit("seen")
    await solera.until(gate.entered.is_set)
    await solera.deploy(project(None))
    await solera.deploy(project("2"))
    gate.released.set()
    await solera.wait(old)
    assert (await solera.get("/assets/seen"))["cursor"] in (None, {"life": "2"})
    await solera.run("seen", mode="full")
    assert cursors and all(c in (None, {"life": "2"}) for c in cursors), cursors


# -- RST-4, RST-5: renames ----------------------------------------------------------


async def test_rst_4_a_rename_with_aliases_keeps_its_heads_keys_and_progress(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.run("copy")
    await ex.feed({"k1": "2"})
    await ex.run("copy")
    head, keys = await solera.head("copy"), await solera.keys("copy")
    await ex.deploy(copy_name="mirror", aliases=["copy"])
    renamed = await solera.head("mirror")
    assert renamed["commit"] == head["commit"] and await solera.keys("mirror") == keys
    assert (await solera.request("GET", "/outputs/copy/heads", ok=False)).status_code == 404
    assert await solera.rows("mirror") == {"k1": "2", "k2": "1"}
    assert await solera.stale("mirror") == []
    await ex.feed({"k2": "2"})
    [batch] = await ex.run("mirror")
    assert not batch.reset and batch.classes == ([], ["k2"], [])  # it continues incrementally
    assert await solera.rows("mirror") == {"k1": "2", "k2": "2"}


async def test_rst_4_a_renamed_per_key_asset_keeps_its_failing_and_processed_keys(solera):
    calls: list = []
    await solera.deploy(parsing(solera.root, calls=calls))
    await solera.run("parse", upstream=True, expect=None)
    await solera.deploy(parsing(solera.root, "parsed", aliases=["parse"], fail=(), calls=calls))
    assert await failing(solera, "parsed") == {"a": "failed", "b": "ok"}
    await solera.run("parsed")
    assert sorted(calls) == ["a", "a", "b"]  # b was processed: only the failing key runs again
    assert await failing(solera, "parsed") == {"a": "ok", "b": "ok"}


async def test_rst_4_a_renamed_job_keeps_its_cursor(solera):
    cursors, rows = [], [{"id": "a"}]

    def project(name, aliases=()):
        @asset(outputs=Output("items", key="id"))
        def items():
            return rows

        async def body(ctx, items: list):
            cursors.append(ctx.cursor)
            return Result(outputs={}, cursor={"runs": (ctx.cursor or {"runs": 0})["runs"] + 1})

        body.__name__ = name
        counted = job(inputs={"items": Incremental()}, aliases=list(aliases))(body)
        return Project(assets=[items, counted], default_store=FileStore(solera.root / "data"))

    await solera.deploy(project("count"))
    await solera.run("count", upstream=True)
    await solera.deploy(project("tally", aliases=["count"]))
    rows.append({"id": "b"})
    await solera.run("tally", upstream=True)
    assert cursors == [None, {"runs": 1}]


async def test_rst_4_a_renamed_outputs_data_stays_where_it_is(solera):
    def project(name, sites, aliases=()):
        def body(ctx):
            return {"site": ctx.partition, "by": name}

        body.__name__ = name
        made = asset(partitions={"site": StaticPartitions(sites)}, aliases=list(aliases))(body)
        return Project(assets=[made], default_store=FileStore(solera.root / "data"))

    await solera.deploy(project("copy", ["a"]))
    await solera.run("copy", partitions=["a"])
    await solera.deploy(project("mirror", ["a", "b"], aliases=["copy"]))
    await solera.run("mirror", partitions=["b"])
    assert await solera.read("mirror", "a") == {"site": "a", "by": "copy"}
    assert await solera.read("mirror", "b") == {"site": "b", "by": "mirror"}
    assert not (solera.root / "data" / "mirror").exists()  # b is written where a is


@pytest.mark.postgres
async def test_rst_4_a_renamed_postgres_output_stays_in_its_table(solera):
    dsn = os.environ.get("SOLERA_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    import psycopg
    from solera_postgres import PostgresStore

    schema, rows = f"s_{uuid.uuid4().hex[:8]}", {"v": [{"id": "a", "v": "1"}]}

    def project(name, aliases=()):
        def body():
            return rows["v"]

        body.__name__ = name
        output = Output(key="id", store="pg", schema=schema, columns={"id": "text", "v": "text"})
        made = asset(outputs=output, aliases=list(aliases))(body)
        return Project(assets=[made], stores={"pg": PostgresStore("env:SOLERA_TEST_DATABASE_URL")})

    await solera.deploy(project("old"))
    await solera.run("old")
    await solera.deploy(project("new", aliases=["old"]))
    assert await solera.rows("new") == {"a": "1"}
    await solera.run("new")  # the same rows, written again
    rows["v"] = [{"id": "a", "v": "2"}]
    await solera.run("new")
    assert await solera.rows("new") == {"a": "2"}
    with psycopg.connect(dsn, autocommit=True) as conn:
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (schema,)
        ).fetchall()
        assert ("new",) not in tables and ("old",) in tables
        conn.execute(f'DROP SCHEMA "{schema}" CASCADE')


def renaming(root, name: str, gate: Gate | None, aliases=()) -> Project:
    """`files` (a) → `name`, an unkeyed count saying who wrote it; it waits at
    `gate` when given one."""

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a"}]

    async def body(files: list):
        if gate is not None and not gate.released.is_set():
            await gate.wait()
        return {"n": len(files), "by": name}

    body.__name__ = name
    made = asset(inputs={"files": Incremental()}, aliases=list(aliases))(body)
    return Project(assets=[files, made], default_store=FileStore(root / "data"))


async def test_rst_5_an_attempt_launched_before_a_rename_settles_under_the_new_name(solera):
    gate, later = Gate(), Gate()
    await solera.deploy(renaming(solera.root, "old", None))
    await solera.run("old", upstream=True)
    await solera.deploy(renaming(solera.root, "old", gate))
    run = await solera.submit("old", mode="full")
    await solera.until(gate.entered.is_set)
    await solera.deploy(renaming(solera.root, "new", later, aliases=["old"]))
    again = await solera.submit("new", mode="full")
    await solera.settle(0.3)
    assert not later.entered.is_set()  # one writer: the next attempt waits for the one in flight
    gate.released.set()
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert (await solera.head("new"))["commit"] == held_commit(detail)  # committed into `new`
    assert await solera.read("new") == {"n": 1, "by": "old"}
    await solera.until(later.entered.is_set)
    later.released.set()
    assert (await solera.wait(again))["request"]["status"] == "succeeded"
    assert await solera.read("new") == {"n": 1, "by": "new"}


async def test_rst_5_a_rename_onto_an_earlier_lifes_name_keeps_its_attempt_settleable(solera):
    """`copy` renamed `mirror`, whose attempt runs; `copy` comes back
    without an alias (`mirror` removed), and its attempt runs; `copy` is
    renamed `mirror` again. Both attempts settle, both runs end."""

    first, second = Gate(), Gate()
    await solera.deploy(renaming(solera.root, "copy", None))
    await solera.run("copy", upstream=True)
    await solera.deploy(renaming(solera.root, "mirror", first, aliases=["copy"]))
    runs = [await solera.submit("mirror", mode="full")]
    await solera.until(first.entered.is_set)
    await solera.deploy(renaming(solera.root, "copy", second))
    runs.append(await solera.submit("copy", mode="full"))
    await solera.until(second.entered.is_set)
    await solera.deploy(renaming(solera.root, "mirror", None, aliases=["copy"]))
    first.released.set()
    second.released.set()
    for run in runs:
        assert (await solera.wait(run))["request"]["status"] in ENDED
    await solera.run("mirror", mode="full")
    assert await solera.read("mirror") == {"n": 1, "by": "mirror"}


# -- RST-6, RST-7 --------------------------------------------------------------------


async def test_rst_6_a_removed_assets_queued_work_is_cancelled_saying_why(solera):
    @asset
    def doomed():
        return [1]

    @asset(inputs={"doomed": "doomed"})
    def after(doomed: list):
        return doomed

    @asset
    def old_name():
        return [2]

    @asset(aliases=["old_name"])
    def new_name():
        return [2]

    @asset
    def other():
        return [3]

    store = FileStore(solera.root / "data")
    await solera.deploy(Project(assets=[doomed, after, old_name], default_store=store))
    gone = await solera.submit("after", upstream=True)
    renamed = await solera.submit("old_name")
    for run in (gone, renamed):
        await solera.post(f"/runs/{run['id']}/pause")
    await solera.deploy(Project(assets=[new_name, other], default_store=store))
    assert await cancelled_as_removed(solera, gone) == ["after", "doomed"]
    assert (await solera.get(f"/runs/{gone['id']}"))["request"]["status"] == "failed"
    await solera.run("other")
    await solera.post(f"/runs/{renamed['id']}/resume")
    detail = await solera.wait(renamed)
    assert {t["asset"]: t["status"] for t in detail["tasks"]} == {"new_name": "succeeded"}


async def test_rst_6_a_removed_assets_last_attempt_ends_its_run(solera):
    gate = Gate()

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a"}, {"id": "b"}]

    @asset(inputs={"files": Incremental(batch_size=1)})
    async def batches(files: list):
        await gate.wait()
        return []

    store = FileStore(solera.root / "data")
    await solera.deploy(Project(assets=[files, batches], default_store=store))
    await solera.run("files")
    run = await solera.submit("batches")
    await solera.until(gate.entered.is_set)
    await solera.deploy(Project(assets=[files], default_store=store))  # `batches` is gone
    gate.released.set()
    assert (await solera.wait(run, timeout=10))["request"]["status"] in ENDED


async def test_rst_6_a_removed_assets_launched_attempt_is_not_retried(solera):
    gate, tries = Gate(), []

    @asset(retries=Retry(1, delay=0))
    async def doomed():
        tries.append(1)
        await gate.wait()
        raise ValueError("boom")

    @asset
    def other():
        return [1]

    store = FileStore(solera.root / "data")
    await solera.deploy(Project(assets=[doomed], default_store=store))
    run = await solera.submit("doomed")
    await solera.until(gate.entered.is_set)
    await solera.deploy(Project(assets=[other], default_store=store))
    gate.released.set()
    assert (await solera.wait(run))["request"]["status"] == "failed"
    assert await cancelled_as_removed(solera, run) == ["doomed"]
    assert tries == [1]


async def test_rst_7_an_upstream_reset_drops_a_per_key_consumers_failing_keys(solera):
    ex = await Example(solera).start()
    ex.failing["k1"] = ValueError("bug")
    await ex.feed({"k1": "1", "k2": "1"})
    await solera.run("checks", expect=None)
    assert await failing(solera, "checks") == {"k1": "failed", "k2": "ok"}
    await ex.deploy(items_store="other")  # `items` moves: `checks`' input is reset
    assert "failed" not in (await failing(solera, "checks")).values()  # at the deploy, before any run
