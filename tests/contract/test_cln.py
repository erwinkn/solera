"""CLN: cleanup and retention (docs/behaviors.md)."""

import asyncio
import datetime as dt
import importlib.util
import shutil

import pytest
from solera.executors import Pool
from solera.sdk import (
    DynamicPartitions,
    Incremental,
    Output,
    Project,
    Ref,
    Retention,
    Retry,
    RunRequest,
    Tick,
    asset,
)
from solera.stores import FileStore, Keys, Patch

from tests.contract import test_sto as sto
from tests.contract.harness import Refused, Solera, rebuild
from tests.contract.test_eng import sensed_project
from tests.contract.test_sto import (
    Files,
    cleanup_pass,
    in_table,
    items_on,
    killed_after_writing,
    rows,
    served,
)

DAY = 86400


async def until(solera, done, seconds: float = 10):
    """The engine serves on until `done()` holds."""

    for _ in range(int(seconds / 0.05)):
        if await done():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("never")


async def test_cln_1_runs_expire_by_policy_and_current_state_never_does(solera):
    n, rows, seen = [0], {"k1": "1", "k2": "1"}, []

    @asset(outputs=Output("kept"), retention=Retention(runs=2))
    def kept():
        n[0] += 1
        return {"n": n[0]}

    @asset(outputs=Output("forever"), retention=Retention(forever=True))
    def forever():
        return {"n": 1}

    @asset(outputs=Output("items", key="id"))
    def items():
        return Patch([{"id": k, "v": v} for k, v in rows.items()])

    @asset(inputs={"items": Incremental()}, outputs=Output("tally"))
    def tally(ctx, items: list):
        batch = ctx.batch["items"]
        seen.append((batch.reset, sorted(batch.added)))
        return {"rows": len(items)}

    @asset(outputs=Output("trained"), executor=Pool("gpu")())
    def trained():
        return {"w": 1}

    project = Project(assets=[kept, forever, items, tally, trained], retention=Retention(days=1))
    await solera.deploy(project)
    runs = []
    for _ in range(4):
        solera.advance(60)
        runs.append((await solera.run("kept"))["request"]["id"])
    keep = [*runs[-2:], (await solera.run("forever"))["request"]["id"]]
    gone = [
        *runs[:2],
        (await solera.run("items"))["request"]["id"],
        (await solera.run("tally"))["request"]["id"],
    ]
    keep.append((await solera.submit("trained"))["id"])  # active: waits on a pool no worker polls
    solera.advance(2 * DAY)
    await solera.start()

    async def expired():
        return {r["id"] for r in await solera.runs()} == set(keep)

    await until(solera, expired)
    for run in gone:
        assert (await solera.request("GET", f"/runs/{run}", ok=False)).status_code == 404
    assert await solera.read("kept") == {"n": 4}  # heads stand, and data never expires
    assert await solera.rows("items") == {"k1": "1", "k2": "1"}
    assert (await solera.head("tally"))["ref"] is not None
    rows["k3"] = "1"
    await solera.run("items")
    await solera.run("tally")
    assert [added for _, added in seen] == [["k1", "k2"], ["k3"]] and not seen[-1][0]  # it keeps its place


async def test_cln_2_runs_are_deleted_and_pruned_by_hand_and_active_ones_are_not(solera):
    @asset(outputs=Output("feed"))
    def feed():
        return {"n": 1}

    @asset(outputs=Output("trained"), executor=Pool("gpu")())
    def trained():
        return {"w": 1}

    await solera.deploy(Project(assets=[feed, trained]))
    finished = [(await solera.run("feed", mode="full"))["request"]["id"] for _ in range(3)]
    await solera.settle(0.1)
    assert await solera.request("DELETE", f"/runs/{finished[0]}") == {"deleted": [finished[0]]}
    assert (await solera.request("GET", f"/runs/{finished[0]}", ok=False)).status_code == 404
    assert await solera.get(f"/runs/{finished[0]}/events") == []
    dry = await solera.post("/runs:prune", {"asset": "feed", "keep": 1, "dry_run": True})
    assert dry == {"deleted": [finished[1]], "dry_run": True}
    assert (await solera.get(f"/runs/{finished[1]}"))["request"]["id"] == finished[1]
    assert (await solera.post("/runs:prune", {"asset": "feed", "keep": 1}))["deleted"] == [finished[1]]
    assert (await solera.request("GET", f"/runs/{finished[1]}", ok=False)).status_code == 404
    assert (await solera.get(f"/runs/{finished[2]}"))["request"]["status"] == "succeeded"
    active = await solera.submit("trained")
    with pytest.raises(Refused) as refused:
        await solera.request("DELETE", f"/runs/{active['id']}")
    assert refused.value.status == 409
    pruned = await solera.post("/runs:prune", {"before": solera.clock() + 3600})
    assert finished[2] in pruned["deleted"] and active["id"] not in pruned["deleted"]
    assert (await solera.get(f"/runs/{active['id']}"))["request"]["status"] != "succeeded"
    assert await solera.read("feed") == {"n": 1}


class HeldCleanup(FileStore):
    """A user's FileStore whose deletes wait to be let go, or fail."""

    def __init__(self, path):
        super().__init__(path)
        self.asked, self.done = 0, 0
        self.go = asyncio.Event()
        self.refuse = False

    async def cleanup(self, *args, **kw):
        self.asked += 1
        if self.refuse:
            raise OSError("the bucket refuses deletes")
        await self.go.wait()
        await super().cleanup(*args, **kw)
        self.done += 1


def scores(store) -> Project:
    value = {"n": 0}

    @asset(outputs=Output("scores"))
    def scores():
        value["n"] += 1
        return {"n": value["n"]}

    return Project(assets=[scores], default_store=store)


async def test_cln_7_a_run_is_settled_before_its_cleanup_is(solera):
    store = HeldCleanup(solera.root / "data")
    await solera.deploy(scores(store))
    await solera.run("scores")
    await solera.run("scores")  # supersedes the first: its cleanup is due
    await solera.settle(0.3)
    assert store.asked and not store.done  # cleaning in the background
    store.go.set()

    async def cleaned():
        await solera.settle(0.05)
        return store.done

    await until(solera, cleaned)
    assert await solera.read("scores") == {"n": 2}


async def test_cln_8_a_cleanup_that_keeps_failing_is_stuck_and_shown_not_retried_forever(solera):
    store = HeldCleanup(solera.root / "data")
    store.refuse = True
    await solera.deploy(scores(store))
    await solera.run("scores")
    await solera.run("scores")  # supersedes the first: its cleanup fails, and is retried

    async def stuck():
        solera.advance(1000)  # retries back off
        await solera.settle(0.05)
        return (await solera.get("/cleanups"))["cleanups"]

    await until(solera, stuck)
    [view] = (await solera.get("/cleanups"))["cleanups"]
    assert view["output"] == "scores" and view["pending"] == 0 and view["stuck"]
    assert [s["output"] for s in (await solera.get("/api/diagnostics"))["stuck_cleanups"]] == ["scores"]
    tried = store.asked
    assert 1 < tried
    for _ in range(5):
        solera.advance(DAY)
        await solera.settle(0.05)
    assert store.asked == tried  # it waits for an operator
    cleared = await solera.post("/cleanups:clear", {"output": "scores", "partition": ""})
    assert cleared["cleared"] == [s["id"] for s in view["stuck"]]
    assert (await solera.get("/cleanups"))["cleanups"] == []
    assert await solera.read("scores") == {"n": 2}


SHARED = """
from solera.sdk import Output, Project, asset
from solera.stores import FileStore


@asset(outputs=Output("files", key="id"))
def files():
    return [{"id": "a", "v": 1}]


project = Project(assets=[files], default_store=FileStore(PATH))
"""


async def test_cln_9_a_store_root_belongs_to_one_namespace(solera, tmp_path):
    shared = tmp_path / "shared"
    module = tmp_path / "shared_project.py"
    module.write_text(SHARED.replace("PATH", repr(str(shared))))
    spec = importlib.util.spec_from_file_location("shared_project", module)
    spec.loader.exec_module(loaded := importlib.util.module_from_spec(spec))
    project = loaded.project
    staging = Solera(tmp_path / "staging")
    try:
        await staging.deploy(project)
        await staging.run("files")  # staging claims the root
        await solera.deploy(project)
        detail = await solera.run("files", expect="failed")
        [task] = detail["tasks"]
        owner = (tmp_path / "staging" / "state").resolve().as_uri()
        assert f"belongs to namespace contract (at {owner})" in task["error"]
        assert "this engine serves namespace contract (at" in task["error"]

        code, _, err = await solera.cli("adopt-store", "--project", str(module), "default")
        assert code != 0 and "which is live" in err  # refused while staging is live
    finally:
        await staging.close()
    shutil.rmtree(tmp_path / "staging")  # staging is retired
    code, out, err = await solera.cli("adopt-store", "--project", str(module), "default")
    assert code == 0, err
    assert "now belongs to namespace contract" in out
    await solera.run("files")
    assert await solera.rows("files") == {"a": 1}


async def test_cln_10_sensor_ticks_last_a_day_and_a_tick_that_asked_for_a_run_lasts_with_it(solera):
    await solera.deploy(sensed_project())
    [tick] = await solera.sensor_ticks()
    await solera.sensor_post(tick, Tick(cursor="a", runs=[RunRequest("ingest")]))
    [run] = await solera.runs()
    [row] = (await solera.get("/sensors/watch/ticks"))["ticks"]
    assert row["tick"] == tick["tick"] and row["runs"] == [run["id"]]
    solera.advance(DAY - 60)
    await solera.start()
    await asyncio.sleep(1.2)  # upkeep looks
    assert len((await solera.get("/sensors/watch/ticks"))["ticks"]) == 1  # under a day old
    solera.advance(120)

    async def expired():
        return (await solera.get("/sensors/watch/ticks"))["ticks"] == []

    await until(solera, expired)
    detail = await solera.get(f"/runs/{run['id']}")  # the run it asked for keeps it
    assert detail["request"]["tags"] == {"sensor": "watch", "tick": tick["tick"]}


# -- target (PR 2) --

table = sto.table  # the fixture: a Postgres store a test makes misbehave


async def gone(solera, output: str, head: dict, keys: dict | None = None, timeout: float = 5) -> None:
    """The engine serves on, its clock a minute further at each look, until
    `output`'s content at `head` (and `keys`) is gone from its store."""

    async def cleaned():
        solera.advance(60)
        return await solera.at(output, head, keys) is None

    await solera.until(cleaned, timeout)


# -- CLN-3 ------------------------------------------------------------------------


async def deleted_with_a_late_write(solera, store) -> tuple[str, asyncio.Event]:
    """`items` writes a=1. Its next run's attempt sends a=2, b=2, and its
    worker is killed before the write lands: the run fails, and is deleted.
    `items`' next run commits a=3. The deleted run's id, and an event that
    lets the late write land."""

    writes = [rows("a=1"), rows("a=2", "b=2"), rows("a=3")]
    await served(solera, items_on(store, writes, retries=Retry(0)))
    await solera.run("items")
    store.late = late = asyncio.Event()
    run = await solera.submit("items")
    await solera.until(store.sent.is_set)
    assert await solera.kill_workers() == 1
    assert (await solera.wait(run))["request"]["status"] == "failed"
    assert await solera.request("DELETE", f"/runs/{run['id']}") == {"deleted": [run["id"]]}
    await solera.run("items")
    return run["id"], late


async def gone_for_good(solera, run_id: str) -> None:
    assert (await solera.request("GET", f"/runs/{run_id}", ok=False)).status_code == 404
    assert run_id not in {r["id"] for r in await solera.runs()}
    assert run_id not in {c["run"] for c in await solera.commits("items")}


@pytest.mark.postgres
async def test_cln_3_a_deleted_runs_late_write_to_a_table_changes_nothing(solera, table):
    deleted, late = await deleted_with_a_late_write(solera, table)
    late.set()
    await table.landing
    [refused] = table.outcomes
    assert isinstance(refused, Exception), refused
    assert in_table(table) == {"a": ["3"]} and await solera.rows("items") == {"a": "3"}
    await gone_for_good(solera, deleted)


async def test_cln_3_a_deleted_runs_late_write_to_files_is_never_read(solera):
    files = Files(solera.root / "files")
    deleted, late = await deleted_with_a_late_write(solera, files)
    late.set()
    await files.landing
    assert files.outcomes == ["written"]
    assert await solera.rows("items") == {"a": "3"}
    await solera.run("copy")
    assert await solera.rows("copy") == {"a": "3"}
    await gone_for_good(solera, deleted)


# -- CLN-4 ------------------------------------------------------------------------


async def test_cln_4_a_lagging_consumer_holds_no_superseded_version_back(solera):
    """`items` rewrites k1 twice while `copy`, which read k1@1, lags: once
    a cleanup pass has run, k1@1 is gone, since no attempt in flight may
    read it; `copy` plans at the head when it runs."""

    received = []
    await solera.deploy(
        items_on(Files(solera.root / "files"), [rows(f"k1={n}") for n in (1, 2, 3)], received)
    )
    await solera.run("items")
    await solera.run("copy")
    first = await solera.head("items"), await solera.keys("items")
    assert await solera.at("items", *first) == rows("k1=1")
    await solera.run("items")
    await solera.run("items")
    await solera.start()
    await cleanup_pass(solera)
    assert await solera.at("items", *first) is None
    await solera.run("copy")
    assert received[-1] == (([], ["k1"], []), {"k1": "3"})


async def test_cln_4_a_burst_of_commits_makes_few_cleanup_tasks(solera):
    n = 12
    await solera.deploy(items_on(Files(solera.root / "files"), [rows(f"k1={i}") for i in range(n)]))
    await solera.run("items")
    first = await solera.head("items"), await solera.keys("items")
    for _ in range(n - 1):
        await solera.run("items")
    await solera.start()
    await gone(solera, "items", *first)
    await solera.quiet()
    tasks = await solera.runs(origin=["cleanup"], limit=500)
    assert 1 <= len(tasks) <= n // 4, len(tasks)


# -- CLN-5 ------------------------------------------------------------------------


async def test_cln_5_what_an_abandoned_attempt_wrote_to_files_goes_late(solera):
    """An attempt dies once its objects landed. They stay while a worker that
    outlived it could still write, then go: after the asset's timeout plus
    the cancel grace."""

    files = Files(solera.root / "files")
    writes = [rows("a=1", "b=1"), rows("a=2", "c=1"), rows("a=2", "c=1"), rows("a=2", "c=1")]
    project = items_on(files, writes, retries=Retry(1, delay=0), timeout=1)
    await solera.deploy(project, placements=solera.workers(), heartbeat_seconds=0.2, cancel_grace=0.1)
    await solera.run("items")
    files.hang_after_write()
    run = await solera.submit("items")
    await killed_after_writing(solera, files)
    dead = {"a": files.versions[-1], "c": files.versions[-1]}
    assert (await solera.wait(run))["request"]["status"] == "succeeded"
    head = await solera.head("items")
    assert await solera.at("items", head, dead) is not None  # a late writer might still add to it
    solera.advance(2)  # the asset's timeout and the cancel grace pass
    await solera.run("items")
    await solera.start()
    await gone(solera, "items", head, dead)
    assert await solera.rows("items") == {"a": "2", "c": "1"}


@pytest.mark.postgres
async def test_cln_5_what_an_abandoned_attempt_wrote_to_a_table_goes_with_the_next_attempt(solera, table):
    """An attempt writes c and a → 2 to a table and dies, out of retries. No
    cleanup by version takes its rows, late or not; the next attempt deletes
    c, the key the log lacks, and its producer rewrites a."""

    writes = [rows("a=1", "b=1"), Patch(rows("c=1", "a=2")), Patch(rows("a=3"))]
    project = items_on(table, writes, retries=Retry(0), timeout=1)
    await solera.deploy(project, placements=solera.workers(), heartbeat_seconds=0.2, cancel_grace=0.1)
    await solera.run("items")
    table.hang_after_write()
    run = await solera.submit("items")
    await killed_after_writing(solera, table)
    assert (await solera.wait(run))["request"]["status"] == "failed"
    solera.advance(2)  # past the asset's timeout and the cancel grace
    await solera.start()
    await cleanup_pass(solera)
    assert in_table(table) == {"a": ["2"], "b": ["1"], "c": ["1"]}
    await solera.run("items")
    assert in_table(table) == {"a": ["3"], "b": ["1"]}
    assert await solera.rows("items") == {"a": "3", "b": "1"}


# -- CLN-6 ------------------------------------------------------------------------


def removable(name: str, days: float | None = None):
    """An asset writing `name`, an output nothing reads; once removed, its data
    waits `days` (its store's week unless said)."""

    def write():
        return rows(f"{name}=1")

    write.__name__ = name
    after = {} if days is None else {"cleanup_after": dt.timedelta(days=days)}
    return asset(outputs=Output(name, key="id", **after))(write)


async def listed(solera, output: str) -> bool:
    """Whether `output`'s old data waits for its cleanup (`GET /cleanups`)."""

    return any(c["output"] == output for c in (await solera.get("/cleanups"))["cleanups"])


async def retired(solera, output: str) -> bool:
    """Whether `output`, listed for cleanup since its removal, comes to be
    cleaned up while the engine works on, its clock a minute further at each
    look: listed no more. A barrier before what a test asserts next."""

    async def done():
        solera.advance(60)
        return not await listed(solera, output)

    try:
        await solera.until(done)
    except AssertionError:
        return False
    return True


def orders_project(store, value: str | None, *others) -> Project:
    """`orders` on `store` writes o1 at `value`, None: removed, beside
    `others`."""

    @asset(outputs=Output("orders", key="id", store="s"))
    def orders():
        return rows(f"o1={value}")

    return Project(assets=[*others] if value is None else [orders, *others], stores={"s": store})


async def test_cln_6_a_removed_outputs_data_goes_after_a_week_and_one_added_back_keeps_its_new_data(solera):
    """`orders` and `brief` (a day's window) are removed. Three days on,
    `brief`'s data is gone and `orders`' stays; `orders` is added back
    and writes; past the week, its old data goes and its new data stays."""

    files = FileStore(solera.root / "files")
    await solera.deploy(orders_project(files, "1", removable("brief", days=1)))
    await solera.run("orders")
    await solera.run("brief")
    old = await solera.head("orders"), await solera.keys("orders")
    await solera.deploy(orders_project(files, None))
    await solera.start()
    await solera.until(lambda: listed(solera, "brief"))
    solera.advance(3 * DAY)
    assert await retired(solera, "brief")
    assert await solera.at("orders", *old) == rows("o1=1")  # within its week
    await solera.deploy(orders_project(files, "2"))  # added back
    await solera.run("orders")
    await solera.start()
    solera.advance(5 * DAY)
    assert await retired(solera, "orders")
    assert await solera.at("orders", *old) is None
    assert await solera.rows("orders") == {"o1": "2"}


async def test_cln_6_an_attempt_in_flight_from_before_the_move_holds_the_cleanup_back(solera):
    """`orders` moves from store a to store b, and `weekly` is removed,
    while `copy`'s attempt, planned before, has not loaded `orders` yet.
    Past the week `weekly`'s data is gone and `orders`' old data stays,
    until that attempt settles."""

    a, held, go = Files(solera.root / "a"), asyncio.Event(), asyncio.Event()

    def project(store: str, value: str, *others) -> Project:
        @asset(outputs=Output("orders", key="id", store=store))
        def orders():
            return rows(f"o1={value}")

        @asset(inputs={"orders": Incremental()}, outputs=Output("copy", key="id"))
        def copy(ctx, orders: list):
            return orders

        return Project(assets=[orders, copy, *others], stores={"a": a, "b": FileStore(solera.root / "b")})

    async def hold():
        held.set()
        await go.wait()

    await solera.deploy(project("a", "1", removable("weekly")))
    await solera.run("orders")
    await solera.run("weekly")
    old = await solera.head("orders"), await solera.keys("orders")
    a.before_load = hold
    run = await solera.submit("copy")
    try:
        await solera.until(held.is_set)
        await solera.deploy(project("b", "2"))  # moved: a new output; the old data waits a week
        await solera.start()
        await solera.until(lambda: listed(solera, "weekly"))
        solera.advance(8 * DAY)
        assert await retired(solera, "weekly"), "held by an attempt that does not read it"
        assert await solera.at("orders", *old) == rows("o1=1")
    finally:
        go.set()
    await solera.wait(run)
    assert await retired(solera, "orders")
    assert await solera.at("orders", *old) is None


async def test_cln_6_a_removed_dynamic_partitions_data_goes_after_a_week(solera):
    """`sites` drops west as `brief` (a day's window) and `weekly` are
    removed: `site_files[west]` is readable once `brief`'s data went, and
    gone once `weekly`'s went; east's stays."""

    listed_sites = ["east", "west"]

    @asset(outputs=DynamicPartitions("sites"))
    def sites():
        return list(listed_sites)

    @asset(partitions={"site": "sites"}, outputs=Output("site_files", key="id"))
    def site_files(ctx):
        return rows(f"{ctx.partition}=1")

    await solera.deploy(Project(assets=[sites, site_files, removable("brief", days=1), removable("weekly")]))
    await solera.run(["sites", "brief", "weekly"])
    await solera.run("site_files", partitions="all")
    west = await solera.head("site_files", "west"), await solera.keys("site_files", "west")
    listed_sites.remove("west")
    await solera.deploy(Project(assets=[sites, site_files]))
    await solera.run("sites")
    await solera.start()
    await solera.until(lambda: listed(solera, "weekly"))
    solera.advance(3 * DAY)
    assert await retired(solera, "brief")
    assert await solera.at("site_files", *west) == rows("west=1")  # within its week
    solera.advance(5 * DAY)
    assert await retired(solera, "weekly")
    assert await solera.at("site_files", *west) is None
    assert await solera.rows("site_files", "east") == {"east": "1"}


async def test_cln_6_a_custom_store_the_project_no_longer_declares_leaves_the_cleanup_stuck(solera):
    class Mine(FileStore):
        """A store of the project's own: nothing rebuilds it from the manifest."""

    mine = Mine(solera.root / "mine")

    @asset(outputs=Output("orders", key="id", store="mine"))
    def orders():
        return rows("o1=1")

    @asset
    def other():
        return 1

    await solera.deploy(Project(assets=[orders, other], stores={"mine": mine}))
    await solera.run("orders")
    old = await solera.head("orders"), await solera.keys("orders")
    await solera.deploy(Project(assets=[other]))
    await solera.start()
    solera.advance(8 * DAY)

    async def stuck():
        return [c for c in (await solera.get("/cleanups"))["cleanups"] if c.get("stuck")]

    await solera.until(stuck)
    [row] = await stuck()
    assert (row["output"], row["store"]) == ("orders", "mine")
    assert await mine.load(Ref.from_json(old[0]["ref"]), list[dict], Keys(old[1])) == rows("o1=1")


# -- CLN-11 -----------------------------------------------------------------------


def paused_project(received: list, writes: list, **project) -> Project:
    """`items` writes the next of `writes`; `copy` reads it incrementally,
    recording each batch: `(reset, classes, {key: value})`."""

    @asset(outputs=Output("items", key="id"))
    def items():
        return writes.pop(0)

    @asset(inputs={"items": Incremental()}, outputs=Output("copy", key="id"))
    def copy(ctx, items: list):
        batch = ctx.batch["items"]
        classes = (sorted(batch.added), sorted(batch.updated), sorted(batch.removed))
        received.append((batch.reset, classes, {r["id"]: r["v"] for r in items}))
        return rebuild(batch, items)

    return Project(assets=[items, copy], **project)


async def expired(solera, run_id: str) -> None:
    """A barrier: retention has deleted `run_id`."""

    async def gone_run():
        return (await solera.request("GET", f"/runs/{run_id}", ok=False)).status_code == 404

    await solera.until(gone_run)


async def test_cln_11_a_paused_consumer_never_loses_its_place_to_retention(solera):
    """`copy` is paused 40 days while `items`, whose runs are kept a week,
    commits daily: once retention has run, `copy` owes k2 (updated), k3
    (removed), k4 (added), not a full run."""

    received = []
    writes = [rows("k1=0", "k2=0", "k3=0"), Patch(rows("k2=1", "k4=1"), remove=["k3"])]
    writes += [Patch(rows(f"k2={day}")) for day in range(2, 41)]
    await solera.deploy(paused_project(received, writes, retention=Retention(days=7)))
    first = (await solera.run("items"))["request"]["id"]
    await solera.run("copy")
    for _ in range(40):
        solera.advance(DAY)
        await solera.run("items")
        await solera.upkeep()
    await solera.start()
    await expired(solera, first)
    await solera.run("copy")
    assert received[-1] == (False, (["k4"], ["k2"], ["k3"]), {"k2": "40", "k4": "1"})
    assert await solera.rows("copy") == {"k1": "0", "k2": "40", "k4": "1"}


# -- CLN-12 -----------------------------------------------------------------------


async def test_cln_12_a_replaced_data_file_goes_once_no_live_attempt_planned_before_its_replacement(solera):
    """`items` replaces k1@1 with k1@4 while `copy`'s attempt, planned
    before, is live: past a cleanup pass k1@1 stays, and the attempt loads
    it; once that attempt settles, the next pass deletes k1@1."""

    files, received = Files(solera.root / "files"), []
    planned, go = asyncio.Event(), asyncio.Event()
    await solera.deploy(items_on(files, [rows("k1=1"), rows("k1=4")], received))
    await solera.run("items")
    first = await solera.head("items"), await solera.keys("items")

    async def replaced():  # copy planned k1@1; items replaces it before copy loads
        planned.set()
        await go.wait()

    files.before_load = replaced
    run = await solera.submit("copy")
    try:
        await solera.until(planned.is_set)
        await solera.run("items")
        await solera.start()
        await cleanup_pass(solera)
        assert await solera.at("items", *first) == rows("k1=1")  # a live attempt planned before c4
    finally:
        go.set()
    assert (await solera.wait(run))["request"]["status"] == "succeeded"
    assert received[0][1] == {"k1": "1"}  # it loaded k1@1
    await cleanup_pass(solera)
    assert await solera.at("items", *first) is None


# -- CLN-13 -----------------------------------------------------------------------


async def test_cln_13_an_output_whose_history_outgrows_its_live_keys_raises_an_alarm_naming_what_holds_it(
    solera,
):
    """`copy` ran once and holds its place while `items`, two keys, is
    rewritten 40 times: once retention and merges have run, an alarm names
    `items` and `copy`."""

    received, writes = [], [rows(f"k1={n}", f"k2={n}") for n in range(41)]
    await solera.deploy(paused_project(received, writes, retention=Retention(days=1)))
    first = (await solera.run("items"))["request"]["id"]
    await solera.run("copy")
    for _ in range(40):
        solera.advance(3600)
        await solera.run("items")
    layers = (await solera.get("/outputs/items/merges"))["layers"]
    solera.advance(2 * DAY)
    await solera.start()
    await expired(solera, first)

    async def merged():
        return (await solera.get("/outputs/items/merges"))["layers"] < layers

    await solera.until(merged)
    assert await solera.alarmed("items", "copy")


# -- CLN-14 -----------------------------------------------------------------------


async def test_cln_14_an_append_only_output_keeps_everything_by_default(solera):
    """`events` appends daily and keeps every commit, whatever its runs'
    retention: `digest`, paused 40 days, gets the 40 days' events once
    retention has run, not a full run."""

    day, received = [0], []

    @asset(outputs=Output("events", incremental=True))
    def events():
        return Patch([{"day": day[0]}])

    @asset(inputs={"events": Incremental()})
    def digest(ctx, events: list):
        received.append((ctx.batch["events"].reset, [e["day"] for e in events]))
        return len(events)

    await solera.deploy(Project(assets=[events, digest], retention=Retention(days=7)))
    first = (await solera.run("events"))["request"]["id"]
    await solera.run("digest")
    for day[0] in range(1, 41):
        solera.advance(DAY)
        await solera.run("events")
        await solera.upkeep()
    await solera.start()
    await expired(solera, first)
    await solera.run("digest")
    assert received == [(False, [0]), (False, list(range(1, 41)))]
