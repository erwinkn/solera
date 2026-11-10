"""RUN: runs, tasks, batches, attempts, retries, cancel (docs/behaviors.md)."""

import asyncio
import itertools
import time

import pytest
from solera import Abort, Failed, Rejected, Transient
from solera.sdk import In, Incremental, Output, Project, RegistrationError, Retry, StaticPartitions, asset
from solera.stores import FileStore

from tests.contract.example import Example
from tests.contract.harness import Refused
from tests.contract.test_sta import Rebuild, decided


class Unprocessable(Rejected):
    pass


class Throttled(Transient):
    retry_for = "2h"


def inside(ex: Example, step, batch: int = 0) -> None:
    """`step` runs inside tally's `batch`-th batch from now (0: the next)."""

    if batch == 0:
        ex.during = step
        return

    async def later():
        inside(ex, step, batch - 1)

    ex.during = later


def hold(ex: Example, batch: int = 0) -> tuple[asyncio.Event, asyncio.Event]:
    """tally's `batch`-th batch from now waits, once begun, until released."""

    begun, release = asyncio.Event(), asyncio.Event()

    async def step():
        begun.set()
        await release.wait()

    inside(ex, step, batch)
    return begun, release


def attempts(detail: dict, asset_name: str | None = None) -> list[dict]:
    """The attempts of the run's task (of `asset_name`), in order."""

    [task] = [t for t in detail["tasks"] if asset_name in (None, t["asset"])]
    return detail["attempts"].get(task["id"], [])


async def detail_of(solera, run) -> dict:
    return await solera.get(f"/runs/{run['id']}")


async def failed(solera, run, n: int = 1) -> bool:
    """`n` attempts of the run's task have failed."""

    return sum(a["outcome"] == "failed" for a in attempts(await detail_of(solera, run))) >= n


async def served(ex: Example, **decl) -> Example:
    """The example served on workers the test can kill, beating every 0.2 s:
    a killed one is missed, and a cancel heard, within a second."""

    await ex.solera.deploy(ex.project(**decl), placements=ex.solera.workers(), heartbeat_seconds=0.2)
    await ex.factor("w1")
    return ex


# -- RUN-1 ------------------------------------------------------------------------


async def test_run_1_a_task_commits_batch_by_batch_and_an_attempt_runs_one_batch(solera):
    ex = await Example(solera).start(batch_size=2)
    await ex.feed({f"k{i}": "1" for i in range(1, 6)})

    async def dies():
        raise ValueError("the worker's batch dies")

    inside(ex, dies, batch=1)
    run = await solera.submit("tally")

    await solera.until(lambda: failed(solera, run))
    solera.advance(60)
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    [task] = detail["tasks"]
    assert (task["asset"], task["partition"], task["status"]) == ("tally", "", "succeeded")
    ran = attempts(detail)
    assert [(a["outcome"], a["batch"]["index"], a["batch"]["final"]) for a in ran] == [
        ("succeeded", 0, False),
        ("failed", 1, False),
        ("succeeded", 1, False),
        ("succeeded", 2, True),
    ]
    seen = ex.seen["tally"]
    assert [b.added for b in seen] == [["k1", "k2"], ["k3", "k4"], ["k3", "k4"], ["k5"]]
    assert [(b.index, b.first, b.final) for b in seen] == [
        (0, True, False),
        (1, False, False),
        (1, False, False),
        (2, False, True),
    ]
    commits = (await solera.get("/assets/tally/history"))["commits"]
    assert sorted(c["batch"]["index"] for c in commits if c["run"] == run["id"]) == [0, 1, 2]
    assert await ex.count() == 5


async def test_run_1_a_run_has_a_task_per_asset_partition(solera):
    @asset(partitions=StaticPartitions(["a", "b", "c"]))
    def sites(ctx):
        return [{"site": ctx.partition}]

    @asset(partitions=StaticPartitions(["a", "b", "c"]), inputs={"sites": In()})
    def reports(ctx, sites: list):
        return sites

    await solera.deploy(Project(assets=[sites, reports]))
    detail = await solera.run("reports", partitions=["a", "c"], upstream=True)
    assert sorted((t["asset"], t["partition"]) for t in detail["tasks"]) == [
        ("reports", "a"),
        ("reports", "c"),
        ("sites", "a"),
        ("sites", "c"),
    ]
    assert all(len(detail["attempts"][t["id"]]) == 1 for t in detail["tasks"])


# -- RUN-2 ------------------------------------------------------------------------


async def test_run_2_one_attempt_at_a_time_per_partition_across_runs(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1"})
    begun, release = hold(ex)
    first = await solera.submit("tally")
    await solera.until(begun.is_set)
    await ex.feed({"k2": "1"})
    second = await solera.submit("tally")
    await solera.settle(0.3)
    [waiting] = (await detail_of(solera, second))["tasks"]
    assert waiting["status"] == "queued" and waiting["held"][0] == "claim"
    assert attempts(await detail_of(solera, second)) == []
    release.set()
    one, two = await solera.wait(first), await solera.wait(second)
    assert one["request"]["status"] == two["request"]["status"] == "succeeded"
    [before], [after] = attempts(one), attempts(two)
    assert after["started_at"] >= before["finished_at"]
    assert [b.added for b in ex.seen["tally"]] == [["k1"], ["k2"]]
    assert await ex.count() == 2


# -- RUN-3 ------------------------------------------------------------------------


async def test_run_3_concurrency_caps_partitions_running_at_once(solera):
    release, running, peak = asyncio.Event(), set(), [0]

    @asset(partitions={"site": StaticPartitions(["a", "b", "c"])}, concurrency=2, retries=Retry(n=0))
    async def report(ctx):
        running.add(ctx.partition)
        peak[0] = max(peak[0], len(running))
        await release.wait()
        running.discard(ctx.partition)
        return [1]

    await solera.deploy(Project(assets=[report]))
    run = await solera.submit("report", partitions="all")

    await solera.until(lambda: len(running) == 2)
    await solera.settle(0.3)  # it does not take a third, however long it works
    assert len(running) == 2
    tasks = (await detail_of(solera, run))["tasks"]
    [waiting] = [t for t in tasks if t["status"] == "queued"]
    assert waiting["held"] == ["concurrency", "report"]
    release.set()
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded" and peak[0] == 2

    with pytest.raises(RegistrationError, match="positive number of partitions"):
        asset(concurrency=0)(lambda: [1])


# -- RUN-5 ------------------------------------------------------------------------


async def test_run_5_a_task_resumes_after_its_last_committed_batch_when_its_attempt_dies(solera):
    ex = await served(Example(solera), batch_size=2)
    await ex.feed({f"k{i}": "1" for i in range(1, 6)})
    begun, _ = hold(ex, batch=2)
    run = await solera.submit("tally")
    await solera.until(begun.is_set)
    assert await solera.kill_workers() == 1
    await solera.until(lambda: failed(solera, run))
    solera.advance(60)
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert [(a["outcome"], a["batch"]["index"]) for a in attempts(detail)] == [
        ("succeeded", 0),
        ("succeeded", 1),
        ("failed", 2),
        ("succeeded", 2),
    ]
    assert [b.added for b in ex.seen["tally"]] == [["k1", "k2"], ["k3", "k4"], ["k5"], ["k5"]]
    assert await ex.count() == 5


async def test_run_5_a_task_resumes_after_its_last_committed_batch_across_engine_restarts(solera):
    ex = await Example(solera).start(batch_size=2)
    await ex.feed({f"k{i}": "1" for i in range(1, 6)})
    begun, release = hold(ex, batch=1)
    run = await solera.submit("tally")
    await solera.until(begun.is_set)
    await solera.restart()
    release.set()
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert [b.added for b in ex.seen["tally"]] == [["k1", "k2"], ["k3", "k4"], ["k5"]]
    assert [a["batch"]["index"] for a in attempts(detail)] == [0, 1, 2]
    assert await ex.count() == 5


async def test_run_5_a_new_run_starts_from_the_first_key(solera):
    ex = await Example(solera).start(batch_size=2)
    await ex.feed({f"k{i}": "1" for i in range(1, 6)})
    await ex.run("tally")
    await ex.feed({"k1": "2", "k5": "2"})
    [batch] = await ex.run("tally")
    assert batch.index == 0 and batch.updated == ["k1", "k5"] and not batch.added


async def test_run_5_a_full_run_walks_on_batch_by_batch_and_the_next_starts_over(solera):
    """Engine review round 2 #2: a full run's later batches walk on from its
    progress — the first batch is not served again, nor started over."""
    seen = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": 1} for k in "abc"]

    @asset(inputs={"files": Incremental(batch_size=2)})
    def consumer(ctx, files: list):
        changes = ctx.batch["files"]
        seen.append((sorted(r["id"] for r in files), changes.reset, changes.index))
        return []

    await solera.deploy(Project(assets=[files, consumer]))
    await solera.run("consumer", upstream=True)
    for _ in range(2):  # a second full run starts over on its own
        seen.clear()
        await solera.run("consumer", mode="full", timeout=10)
        assert seen == [(["a", "b"], True, 0), (["c"], False, 1)]


# -- RUN-6 ------------------------------------------------------------------------


async def test_run_6_a_full_run_finishes_in_a_bounded_number_of_attempts(solera):
    ex = await Example(solera).start(batch_size=2)
    await ex.feed({f"k{i}": "1" for i in range(1, 6)})
    await ex.run("tally", mode="full")
    begun, release = hold(ex)
    run = await solera.submit("tally", mode="full")
    await solera.until(begun.is_set)
    await ex.feed({"k0": "1", "k9": "1"})  # feed moves while the pass is under way
    release.set()
    detail = await solera.wait(run, timeout=10)
    assert detail["request"]["status"] == "succeeded"
    ran = attempts(detail)
    assert [a["outcome"] for a in ran] == ["succeeded"] * len(ran)
    assert [a["batch"]["index"] for a in ran] == list(range(len(ran))) and len(ran) <= 4


# -- RUN-7 ------------------------------------------------------------------------


async def test_run_7_rejected_fails_at_once(solera):
    calls = {"n": 0}

    @asset(retries=Retry(3, delay=0.01))
    def bad():
        calls["n"] += 1
        raise Unprocessable("empty file")

    await solera.deploy(Project(assets=[bad]))
    detail = await solera.run("bad", expect="failed")
    assert calls["n"] == 1
    assert "Unprocessable: empty file" in detail["attempts"][detail["tasks"][0]["id"]][0]["error"]


async def test_run_7_failed_and_abort_follow_retries(solera):
    calls = {"failed": 0, "abort": 0}

    @asset(retries=Retry(1, delay=0.01))
    def unclassified():
        calls["failed"] += 1
        raise ValueError("bug")

    @asset(retries=Retry(1, delay=0.01))
    def aborts():
        calls["abort"] += 1
        raise Abort("credentials")

    await solera.deploy(Project(assets=[unclassified, aborts]))
    await solera.run("unclassified", expect="failed")
    await solera.run("aborts", expect="failed")
    assert calls == {"failed": 2, "abort": 2}


async def test_run_7_retries_back_off_and_a_retry_that_succeeds_ends_the_task(solera):
    """§8: retryable failures re-queue with Retry backoff; non-retryable
    failures end the task."""
    calls = {"n": 0}

    @asset(retries=Retry(2, delay=0.01))
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("transient")
        return ["ok"]

    await solera.deploy(Project(assets=[flaky]))
    detail = await solera.run("flaky")
    assert calls["n"] == 3
    assert [a["outcome"] for a in attempts(detail)] == ["failed", "failed", "succeeded"]


async def backs_off(solera, run, calls: list, delays, start: int = 1) -> None:
    """After each failure the next attempt waits its delay: none comes with
    half of it left, one comes once it has passed. The clock is the wall's
    plus what was advanced, so only a pause of half a delay could cross."""

    for n, delay in enumerate(delays, start=start):
        await solera.until(lambda n=n: failed(solera, run, n))
        solera.advance(delay / 2)
        await solera.settle(0.1)
        assert len(calls) == n, f"retried before its backoff of {delay} s"
        solera.advance(delay / 2)
        await solera.until(lambda n=n: len(calls) > n)


async def test_run_7_by_default_a_failure_is_retried_three_times_backing_off(solera):
    calls = []

    @asset
    def flaky():
        calls.append(1)
        raise Failed("bug")

    await solera.deploy(Project(assets=[flaky]))
    run = await solera.submit("flaky")
    for n in range(1, 4):  # Retry(3): three retries, each after its backoff
        await solera.until(lambda n=n: failed(solera, run, n))
        solera.advance(60)
        await solera.until(lambda n=n: len(calls) > n)
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "failed" and len(calls) == 4
    ran = attempts(detail)
    assert [a["outcome"] for a in ran] == ["failed"] * 4
    # Each retry came a second at least after the failure before it.
    assert all(b["started_at"] >= a["finished_at"] + 1 for a, b in itertools.pairwise(ran))


async def test_run_7_retries_back_off_doubling(solera):
    calls = []

    @asset(retries=Retry(3, delay=600))
    def flaky():
        calls.append(1)
        raise Failed("bug")

    await solera.deploy(Project(assets=[flaky]))
    run = await solera.submit("flaky")
    await backs_off(solera, run, calls, (600, 1200, 2400))
    assert (await solera.wait(run))["request"]["status"] == "failed" and len(calls) == 4


async def test_run_7_transient_retries_past_retries_within_its_budget(solera):
    """A Transient error is retried after its `retry_after` even with no
    retries left, until `retry_for` has passed since the first one."""

    calls = {"n": 0}

    @asset(retries=Retry(0))
    def throttled():
        calls["n"] += 1
        if calls["n"] < 4:
            raise Throttled("429", retry_after=0.01)
        return ["ok"]

    await solera.deploy(Project(assets=[throttled]))
    await solera.run("throttled")
    assert calls["n"] == 4


async def test_run_7_transient_without_retry_after_waits_a_minute_doubling(solera):
    calls = []

    @asset(retries=Retry(0))
    def throttled():
        calls.append(1)
        if len(calls) < 4:
            raise Transient("429")
        return ["ok"]

    await solera.deploy(Project(assets=[throttled]))
    run = await solera.submit("throttled")
    await backs_off(solera, run, calls, (60, 120, 240))
    assert (await solera.wait(run))["request"]["status"] == "succeeded"


async def test_run_7_transient_backoff_doubles_up_to_6_hours(solera):
    calls = []

    @asset(retries=Retry(0))
    def throttled():
        calls.append(1)
        raise Transient("429")

    await solera.deploy(Project(assets=[throttled]))
    run = await solera.submit("throttled")
    for n in range(1, 10):  # a minute, doubling: the ninth retry waits 4 h 16 min
        await solera.until(lambda n=n: failed(solera, run, n))
        solera.advance(60 * 2 ** (n - 1))
        await solera.until(lambda n=n: len(calls) > n)
    # Then 6 h each time, not 8.5 h then 17 h: at 14.5 h and 20.5 h, within retry_for's day.
    await backs_off(solera, run, calls, (6 * 3600, 6 * 3600), start=10)
    assert len(calls) == 12


async def test_run_7_transient_gives_up_after_retry_for(solera):
    calls = {"n": 0}

    @asset(retries=Retry(0))
    def flapping():
        calls["n"] += 1
        raise Transient("down", retry_after=0.05, retry_for=0.2)

    await solera.deploy(Project(assets=[flapping], errors={}))
    await solera.run("flapping", expect="failed")
    assert 2 <= calls["n"] <= 6


async def test_run_7_transient_retries_for_a_day_by_default(solera):
    calls = []

    @asset(retries=Retry(0))
    def flapping():
        calls.append(1)
        raise Transient("down", retry_after="6h")

    await solera.deploy(Project(assets=[flapping]))
    run = await solera.submit("flapping")
    for n in range(1, 5):  # retried at 6, 12, 18 and 24 hours, past retries=
        await solera.until(lambda n=n: failed(solera, run, n))
        assert (await detail_of(solera, run))["request"]["status"] == "running"
        solera.advance(6 * 3600)
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "failed" and len(calls) == 5  # the last past retry_for


async def test_run_7_a_worker_that_dies_without_a_result_is_retried(solera):
    started, calls = asyncio.Event(), []

    @asset
    async def slow():
        calls.append(1)
        if len(calls) == 1:
            started.set()
            await asyncio.sleep(30)  # killed here
        return [{"ok": True}]

    await solera.deploy(Project(assets=[slow]), placements=solera.workers(), heartbeat_seconds=0.2)
    run = await solera.submit("slow")
    await solera.until(started.is_set)
    assert await solera.kill_workers() == 1
    await solera.until(lambda: failed(solera, run))
    solera.advance(60)
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert [a["outcome"] for a in attempts(detail)] == ["failed", "succeeded"]
    assert await solera.read("slow") == [{"ok": True}]


# -- RUN-8 ------------------------------------------------------------------------


async def test_run_8_a_failed_batch_changes_nothing_its_consumer_processed(solera):
    ex = await Example(solera).start(batch_size=2)
    await ex.feed({f"k{i}": "1" for i in range(1, 5)})

    async def rejects():
        raise Unprocessable("batch 2 is bad")

    inside(ex, rejects, batch=1)
    await solera.run("tally", expect="failed")
    assert await ex.count() == 2  # what batch 1 left
    assert await ex.stale() != []
    [batch] = await ex.run("tally")  # batch 2's keys are still owed, and only they
    assert batch.added == ["k3", "k4"] and not batch.updated and not batch.removed
    assert await ex.count() == 4


async def test_run_8_an_abandoned_attempt_changes_nothing_its_consumer_processed(solera):
    ex = await served(Example(solera), batch_size=2)
    await ex.feed({f"k{i}": "1" for i in range(1, 5)})
    begun, _ = hold(ex, batch=1)
    run = await solera.submit("tally")
    await solera.until(begun.is_set)
    await solera.kill_workers()
    assert await ex.count() == 2
    await solera.until(lambda: failed(solera, run))
    solera.advance(60)
    await solera.wait(run)
    first, second, again = ex.seen["tally"]
    assert again.classes == second.classes == (["k3", "k4"], [], [])
    assert await ex.count() == 4


# -- RUN-9 ------------------------------------------------------------------------


class Holding(FileStore):
    """A user's store whose writes, once begun, wait for `release`."""

    def __init__(self, root):
        super().__init__(root)
        self.begun, self.release = asyncio.Event(), asyncio.Event()

    async def store(self, write, prior, context):
        self.begun.set()
        await self.release.wait()
        return await super().store(write, prior, context)


def writer(store: FileStore) -> Project:
    @asset(retries=Retry(0))
    def report():
        return {"v": 1}

    return Project(assets=[report], default_store=store)


async def test_run_9_a_cancel_stops_an_asset_that_has_not_begun_writing(solera):
    started, finished = asyncio.Event(), []

    @asset(retries=Retry(0))
    async def slow():
        started.set()
        await asyncio.sleep(30)
        finished.append(1)
        return {"v": 1}

    await solera.deploy(Project(assets=[slow]), heartbeat_seconds=0.2, cancel_grace=30)
    run = await solera.submit("slow")
    await solera.until(started.is_set)
    asked = time.monotonic()
    await solera.cancel(run["id"])
    detail = await solera.wait(run)
    assert time.monotonic() - asked < 2  # heard within a beat, long before the grace
    assert detail["request"]["status"] == "canceled"
    assert [a["outcome"] for a in attempts(detail)] == ["canceled"] and not finished
    assert await solera.head("slow") is None


async def test_run_9_an_asset_already_writing_finishes_and_its_commit_stands(solera, tmp_path):
    store = Holding(tmp_path / "data")
    await solera.deploy(writer(store), heartbeat_seconds=0.2, cancel_grace=30)
    run = await solera.submit("report")
    await solera.until(store.begun.is_set)
    await solera.cancel(run["id"])
    await solera.settle(0.5)  # the worker heard it within a beat: it drains, not forced
    assert [a["outcome"] for a in attempts(await detail_of(solera, run))] == ["running"]
    store.release.set()
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "canceled"
    assert [a["outcome"] for a in attempts(detail)] == ["succeeded"]
    assert await solera.read("report") == {"v": 1}


async def test_run_9_past_its_grace_a_cancel_is_forced_and_nothing_more_is_accepted(solera, tmp_path):
    store = Holding(tmp_path / "data")
    await solera.deploy(writer(store), heartbeat_seconds=0.2, cancel_grace=0.3)
    run = await solera.submit("report")
    await solera.until(store.begun.is_set)
    await solera.cancel(run["id"])
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "canceled"
    assert [a["outcome"] for a in attempts(detail)] == ["canceled"]
    store.release.set()
    await solera.settle(0.3)
    assert await solera.head("report") is None


# -- RUN-10 -----------------------------------------------------------------------


async def test_run_10_a_cancelled_run_keeps_what_it_committed_and_does_not_resume(solera):
    ex = await served(Example(solera), batch_size=2)
    await ex.feed({f"k{i}": "1" for i in range(1, 7)})
    begun, _ = hold(ex, batch=2)
    run = await solera.submit("tally")
    await solera.until(begun.is_set)
    await solera.cancel(run["id"])
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "canceled"
    assert await ex.count() == 4  # batches 1 and 2 stand
    assert await ex.stale() != []
    await solera.settle(0.5)  # it does not resume itself
    assert len(attempts(await detail_of(solera, run))) == 3
    assert [r["id"] for r in await solera.runs() if "tally" in r["targets"]] == [run["id"]]
    await ex.feed({"k1": "2"})
    batches = await ex.run("tally")  # the rest, and what changed since
    assert sorted(k for b in batches for k in b.added) == ["k5", "k6"]
    assert [k for b in batches for k in b.updated] == ["k1"]
    assert not any(b.removed for b in batches)
    assert await ex.count() == 6


# -- RUN-11 -----------------------------------------------------------------------


async def test_run_11_provisioning_does_not_count_against_the_timeout(solera):
    @asset(timeout=0.3, retries=Retry(0))
    def brief():
        return [{"ok": True}]

    await solera.deploy(Project(assets=[brief]), placements=solera.workers(boot=0.6), heartbeat_seconds=0.2)
    detail = await solera.run("brief")
    assert [a["outcome"] for a in attempts(detail)] == ["succeeded"]


async def test_run_11_past_its_timeout_an_attempt_is_cancelled_and_retried(solera):
    calls = []

    @asset(timeout=0.3, retries=Retry(1, delay=0))
    async def late():
        calls.append(1)
        if len(calls) == 1:
            await asyncio.sleep(30)
        return [{"ok": True}]

    await solera.deploy(Project(assets=[late]), heartbeat_seconds=0.2, cancel_grace=0.2)
    detail = await solera.run("late")
    first, second = attempts(detail)
    assert first["outcome"] == "failed" and "timeout" in first["error"]
    assert second["outcome"] == "succeeded"


async def test_run_11_a_worker_that_never_reports_fails_retryably_at_its_provisioning_deadline(solera):
    @asset(retries=Retry(1, delay=0))
    def never():
        return []

    await solera.deploy(
        Project(assets=[never]),
        placements=solera.workers(boot=None),
        heartbeat_seconds=0.2,
        provision_seconds=0.3,
    )
    detail = await solera.run("never", expect="failed")
    ran = attempts(detail)
    assert [a["outcome"] for a in ran] == ["failed", "failed"]  # retryable: retried within retries=


# -- RUN-12 -----------------------------------------------------------------------


def shop() -> Project:
    @asset
    def orders():
        return [{"id": i} for i in range(3)]

    @asset(inputs={"orders": In()}, retries=Retry(n=0))
    def revenue(orders: list, ctx):
        if ctx.config.get("fail"):
            raise RuntimeError("upstream timeout")
        return {"revenue": len(orders)}

    @asset(inputs={"revenue": In()})
    def summary(revenue: dict):
        return revenue

    return Project(assets=[orders, revenue, summary])


async def test_run_12_retrying_a_finished_run_is_a_new_run_and_the_old_one_stays_as_it_ended(solera):
    await solera.deploy(shop())
    failed = await solera.run(
        "summary", expect="failed", upstream=True, config={"fail": True}, tags={"env": "prod"}
    )
    run_id = failed["request"]["id"]
    owed = {(t["asset"], t["partition"]) for t in failed["tasks"] if t["status"] != "succeeded"}
    assert owed == {("revenue", ""), ("summary", "")}
    retried = await solera.post(f"/runs/{run_id}/retry")
    assert retried["id"] != run_id
    with pytest.raises(Refused) as refused:
        await solera.post(f"/runs/{retried['id']}/retry")
    assert refused.value.status == 409  # not finished
    again = await solera.wait(retried)
    assert {(t["asset"], t["partition"]) for t in again["tasks"]} == owed
    assert again["request"]["config"] == {"fail": True} and again["request"]["tags"] == {"env": "prod"}
    assert await solera.get(f"/runs/{run_id}") == failed
    [row] = await solera.runs(limit=1)
    assert row["id"] == retried["id"] and row["retry_of"] == run_id
    ok = await solera.run("orders")
    with pytest.raises(Refused) as refused:
        await solera.post(f"/runs/{ok['request']['id']}/retry")
    assert refused.value.status == 409  # nothing to retry


async def test_run_12_a_retry_asks_for_exactly_the_partitions_that_did_not_succeed(solera):
    @asset(partitions=StaticPartitions(["east", "west"]), retries=Retry(n=0))
    def site(ctx):
        if ctx.partition == "west" and ctx.config.get("fail"):
            raise RuntimeError("west is down")
        return [{"site": ctx.partition}]

    await solera.deploy(Project(assets=[site]))
    failed = await solera.run("site", expect="failed", partitions="all", config={"fail": True})
    retried = await solera.post(f"/runs/{failed['request']['id']}/retry")
    detail = await solera.wait(retried)
    assert detail["request"]["partitions"] == {"site": ["west"]}
    assert [t["partition"] for t in detail["tasks"]] == ["west"]


# -- RUN-13 -----------------------------------------------------------------------


async def test_run_13_a_tasks_outcome_is_succeeded_skipped_failed_or_canceled(solera):
    ex = await served(Example(solera))
    await ex.feed({"k1": "1"})
    ran = await solera.run("tally")
    quiet = await solera.run("tally", expect=None)  # nothing owed
    [task] = quiet["tasks"]
    assert task["status"] == "skipped" and len(ex.seen["tally"]) == 1  # its producer never ran
    assert all(a["outcome"] == "skipped" for a in attempts(quiet))
    assert [r["id"] for r in await solera.runs(status="skipped")] == [quiet["request"]["id"]]

    async def rejects():
        raise Unprocessable("bad")

    await ex.feed({"k2": "1"})
    inside(ex, rejects)
    failed = await solera.run("tally", expect="failed")
    begun, _ = hold(ex)
    run = await solera.submit("tally")
    await solera.until(begun.is_set)
    await solera.cancel(run["id"])
    canceled = await solera.wait(run)
    assert [d["tasks"][0]["status"] for d in (ran, quiet, failed, canceled)] == [
        "succeeded",
        "skipped",
        "failed",
        "canceled",
    ]


async def test_run_13_a_per_key_run_whose_keys_partly_failed_succeeded_with_counts(solera):
    ex = await Example(solera).start()
    await ex.feed({f"k{i}": "1" for i in range(1, 6)})
    await solera.run("checks")
    ex.failing.update({"k2": Unprocessable("bad row"), "k3": ValueError("bug")})
    await ex.feed({"k1": "2", "k2": "2", "k3": "2"}, removes=["k4"])
    detail = await solera.run("checks")
    [attempt] = attempts(detail)
    assert attempt["keys"] == {"ok": 1, "rejected": 1, "failed": 1, "removed": 1}


# -- RUN-15 -----------------------------------------------------------------------


async def test_run_15_upstream_false_pins_current_heads_and_never_reruns_an_upstream(solera):
    """§8: upstream=False plans targets only; inputs pin current heads and a
    producer is never re-run."""
    calls = {"n": 0}

    @asset(outputs=Output("feed", key="id"))
    def producer():
        calls["n"] += 1
        return [{"id": "a", "v": calls["n"]}]

    @asset(inputs={"feed": Incremental()})
    def consumer(feed: list):
        return [{"n": len(feed)}]

    await solera.deploy(Project(assets=[producer, consumer]))
    await solera.run("producer")
    detail = await solera.run("consumer", upstream=False)
    assert calls["n"] == 1  # producer never re-polled
    assert [t["asset"] for t in detail["tasks"]] == ["consumer"]


async def test_run_15_upstream_true_builds_the_closure_first(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1"}, items=False)
    detail = await solera.run("tally", upstream=True)
    assert sorted(t["asset"] for t in detail["tasks"]) == ["items", "tally"]
    [items] = attempts(detail, "items")
    [tally] = attempts(detail, "tally")
    assert tally["started_at"] >= items["finished_at"]
    assert [b.added for b in ex.seen["tally"]] == [["k1"]]


async def test_run_15_a_selection_past_100000_partitions_is_refused_not_truncated(solera):
    big = StaticPartitions([f"k{i}" for i in range(400)])

    @asset(partitions={"a": big, "b": big})
    def grid(ctx):
        return []

    await solera.deploy(Project(assets=[grid]))
    with pytest.raises(Refused) as refused:
        await solera.submit("grid", partitions="all")
    assert refused.value.status == 400 and "100000" in refused.value.detail
    assert await solera.runs() == []


# -- target (PR 2) --


# -- RUN-4 (target) ---------------------------------------------------------------------


async def test_run_4_a_task_held_by_an_upstream_mid_full_run_says_upstream_incomplete(solera):
    async with Rebuild(solera) as r:
        await r.start()
        run = await r.midway()
        reader = await solera.submit("archive")
        task = await decided(solera, reader, "archive")
        assert task["status"] == "queued" and task.get("held") == ["upstream incomplete", "copy"], task
        events = await solera.get(f"/runs/{reader['id']}/events")
        assert [(e["reason"], e["name"]) for e in events if e["type"] == "held"] == [
            ("upstream incomplete", "copy")
        ]
        await r.finish(run)
        assert (await solera.wait(reader))["request"]["status"] == "succeeded"
