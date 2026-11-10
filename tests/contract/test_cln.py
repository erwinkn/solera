"""CLN: cleanup and retention (docs/behaviors.md)."""

import asyncio
import importlib.util
import shutil

import pytest
from solera.executors import Pool
from solera.sdk import Commit, Incremental, Output, Project, Retention, RunRequest, Tick, asset
from solera.stores import FileStore, Patch

from tests.contract.harness import Refused, Solera
from tests.contract.test_eng import sensed_project

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
    staging = Solera(tmp_path / "staging", solera.path)
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


async def past_a_day(solera, tick: dict) -> None:
    """Served for a day and more after `tick`: its row stands under a day old
    and is gone after."""

    [row] = (await solera.get("/sensors/watch/ticks"))["ticks"]
    assert row["tick"] == tick["tick"]
    solera.advance(DAY - 60)
    await solera.start()
    await asyncio.sleep(1.2)  # upkeep looks
    assert len((await solera.get("/sensors/watch/ticks"))["ticks"]) == 1  # under a day old
    solera.advance(120)

    async def expired():
        return (await solera.get("/sensors/watch/ticks"))["ticks"] == []

    await until(solera, expired)


async def test_cln_10_sensor_tick_rows_last_a_day(solera):
    await solera.deploy(sensed_project())
    [tick] = await solera.sensor_ticks()
    await solera.sensor_post(tick, Tick(cursor="a"))
    await past_a_day(solera, tick)


async def test_cln_10_a_tick_that_committed_lasts_as_long_as_its_run(solera):
    await solera.deploy(sensed_project())
    [tick] = await solera.sensor_ticks()
    await solera.sensor_post(tick, Tick(cursor="a", commits=[Commit("uploads", upsert={"a": "1"})]))
    [run] = await solera.runs()
    await past_a_day(solera, tick)
    detail = await solera.get(f"/runs/{run['id']}")  # its commit's run keeps it: what changed, and who
    assert detail["request"]["status"] == "succeeded"
    assert detail["request"]["by"] == "sensor watch"
    [listed] = await solera.runs()
    assert listed["source"] == "uploads" and listed["tags"] == {"sensor": "watch", "tick": tick["tick"]}
    assert (await solera.keys("uploads")).keys() == {"a"}


async def test_cln_10_a_tick_that_asked_for_a_run_lasts_with_it(solera):
    await solera.deploy(sensed_project())
    [tick] = await solera.sensor_ticks()
    await solera.sensor_post(tick, Tick(cursor="a", runs=[RunRequest("ingest")]))
    [run] = await solera.runs()
    [row] = (await solera.get("/sensors/watch/ticks"))["ticks"]
    assert row["runs"] == [run["id"]]
    await past_a_day(solera, tick)
    detail = await solera.get(f"/runs/{run['id']}")
    assert detail["request"]["tags"] == {"sensor": "watch", "tick": tick["tick"]}
