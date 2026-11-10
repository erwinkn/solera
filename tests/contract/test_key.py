"""KEY: per-key assets and key outcomes (docs/behaviors.md)."""

import asyncio
import copy
import json

import pytest
from solera import Rejected, Transient
from solera.sdk import Incremental, OnChange, Output, Project, Retry, Source, StaticPartitions, asset, job
from solera.stores import FileStore, Patch

from tests.contract.example import Example
from tests.contract.harness import Outside, Refused, SourceStore


class Unprocessable(Rejected):
    pass


STORED = ("rejected", "failed", "retrying", "canceled", "timed_out")  # the outcomes a key keeps


def files_project(root, content: dict, fn, *, written=None, build=None, **input) -> Project:
    """`files`, a keyed asset writing what changed in `content` since it last
    ran (a `Patch`), and `parse`, `fn` over each of its keys, writing
    `samples` keyed by `path`. Deploys of one `content` pass one `written`."""

    written = {} if written is None else written

    @asset(outputs=Output("files", keyed=True))
    def files():
        changed = {k: v for k, v in content.items() if written.get(k) != v}
        gone = [k for k in written if k not in content]
        written.clear()
        written.update(copy.deepcopy(content))
        return Patch(changed, remove=gone)

    parse = asset(
        fn, inputs={"file": Incremental("files", **input, each=True)}, outputs=Output("samples", key="path")
    )
    return Project(assets=[files, parse], default_store=FileStore(root / "data"), build=build)


def keyed_project(root, content: dict, fn, *, store=None, **decl) -> Project:
    """`files`, a keyed asset holding `content` as it is, and `parse`, `fn`
    over each of its keys, writing `rows` keyed by `path` (to `store`, if
    given); `decl` is the rest of `parse`'s declaration."""

    @asset(outputs=Output("files", keyed=True))
    def files():
        return dict(content)

    parse = asset(
        fn,
        inputs={"file": Incremental("files", each=True)},
        outputs=Output("rows", key="path", store="slow" if store else None),
        **decl,
    )
    stores = {"slow": store} if store else {}
    return Project(assets=[files, parse], stores=stores, default_store=FileStore(root / "data"))


def fed_project(root, outside: Outside, fn, *, retries=None, **input) -> Project:
    """`checks`, `fn` over each key of `feed`, a keyed source the outside
    serves, writing `checks` keyed by `id`."""

    checks = asset(
        fn,
        inputs={"row": Incremental("feed", each=True, **input)},
        outputs=Output("checks", key="id"),
        **({"retries": retries} if retries is not None else {}),
    )
    return Project(
        assets=[checks],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(root / "ext", outside)},
        default_store=FileStore(root / "data"),
    )


async def feed(solera, outside: Outside, **upserts) -> None:
    outside.feed.update(upserts)
    await solera.commit_source("feed", upsert=upserts)


def attempts_of(detail: dict, name: str) -> list[dict]:
    task = next(t for t in detail["tasks"] if t["asset"] == name)
    return detail["attempts"][task["id"]]


# -- KEY-1 ---------------------------------------------------------------------


async def test_key_1_a_per_key_asset_is_called_once_per_owed_key(solera):
    """`checks` over k1, k2 (changed) and k3 (removed), k4 unchanged: two
    calls, and one write holds k1's and k2's rows and removes k3's."""

    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1", "k3": "1", "k4": "1"})
    await ex.run("checks")
    assert sorted(ex.calls.pop("checks")) == ["k1", "k2", "k3", "k4"]
    await ex.feed({"k1": "2", "k2": "2"}, removes=["k3"])
    detail = await solera.run("checks")
    assert sorted(ex.calls["checks"]) == ["k1", "k2"]
    assert await solera.rows("checks") == {"k1": "2", "k2": "2", "k4": "1"}
    assert len(await solera.commits("checks", detail["request"]["id"])) == 1


async def test_key_1_removed_keys_lose_their_rows_without_a_call(solera):
    content, calls = {"a": {"text": "1"}, "b": {"text": "2"}}, []

    def parse(ctx, file: dict):
        calls.append(ctx.key)
        return [{"value": int(file["text"])}]

    await solera.deploy(files_project(solera.root, content, parse))
    await solera.run("parse", upstream=True)
    del content["a"]
    calls.clear()
    await solera.run("parse", upstream=True)
    assert set(await solera.rows("samples", column="value")) == {"b"}
    assert calls == []


# -- KEY-2 ---------------------------------------------------------------------


def test_key_2_by_default_10000_keys_a_commit_and_64_at_once():
    file = Incremental("files", each=True)
    assert (file.batch_size, file.concurrency) == (10_000, 64)


async def test_key_2_batch_size_is_keys_per_commit_and_concurrency_keys_at_once(solera):
    """Seven keys in batches of three make three commits, never more than
    two calls at once."""

    live = {"now": 0, "max": 0}

    async def parse(ctx, file: dict):
        live["now"] += 1
        live["max"] = max(live["max"], live["now"])
        await asyncio.sleep(0.02)
        live["now"] -= 1
        return [{"value": file["n"]}]

    content = {f"k{i}": {"n": i} for i in range(7)}
    await solera.deploy(files_project(solera.root, content, parse, batch_size=3, concurrency=2))
    detail = await solera.run("parse", upstream=True)
    assert live["max"] == 2
    assert [a["keys"] for a in attempts_of(detail, "parse")] == [{"ok": 3}, {"ok": 3}, {"ok": 1}]
    assert len(await solera.commits("parse", detail["request"]["id"])) == 3
    assert len(await solera.rows("samples")) == 7


async def test_key_2_the_default_concurrency_runs_64_keys_at_once(solera):
    live = {"now": 0, "max": 0}

    async def parse(ctx, file: dict):
        live["now"] += 1
        live["max"] = max(live["max"], live["now"])
        await asyncio.sleep(0.05)
        live["now"] -= 1
        return [{"value": file["n"]}]

    await solera.deploy(files_project(solera.root, {f"k{i:03}": {"n": i} for i in range(100)}, parse))
    detail = await solera.run("parse", upstream=True)
    assert live["max"] == 64
    assert [a["keys"] for a in attempts_of(detail, "parse")] == [{"ok": 100}]


async def test_key_2_the_assets_concurrency_caps_partitions_each_at_its_keys_concurrency(solera):
    """`@asset(concurrency=2)` with per-key `concurrency=2`: at most 2 x 2
    calls at once, at most 2 in any partition."""

    days = StaticPartitions(["d1", "d2", "d3"])
    live: dict[str, int] = {}
    peak = {"all": 0, "one": 0}

    @asset(outputs=Output("files", keyed=True), partitions={"day": days})
    def files(ctx):
        return {f"{ctx.partition}-{i}": i for i in range(3)}

    @asset(
        inputs={"file": Incremental("files", each=True, concurrency=2)},
        outputs=Output("rows", key="path"),
        partitions={"day": days},
        concurrency=2,
    )
    async def parse(ctx, file: int):
        live[ctx.partition] = live.get(ctx.partition, 0) + 1
        peak["all"] = max(peak["all"], sum(live.values()))
        peak["one"] = max(peak["one"], live[ctx.partition])
        await asyncio.sleep(0.05)
        live[ctx.partition] -= 1
        return [{"n": file}]

    await solera.deploy(Project(assets=[files, parse], default_store=FileStore(solera.root / "data")))
    await solera.run("files", partitions="all")
    await solera.run("parse", partitions="all")
    assert 2 < peak["all"] <= 4 and peak["one"] == 2


async def test_key_2_a_cancel_interrupts_keys_still_waiting_for_a_slot(solera):
    """With concurrency=1, b runs and hangs while c and d wait for a slot. A
    cancel interrupts the call in flight and the keys never started alike:
    all three canceled, and retried together."""

    started, release = asyncio.Event(), asyncio.Event()

    @asset(outputs=Output("files", keyed=True))
    def files():
        return {"a": 1, "b": 2, "c": 3, "d": 4}

    @asset(
        inputs={"file": Incremental("files", concurrency=1, each=True)}, outputs=Output("rows", key="path")
    )
    async def parse(ctx, file: int):
        if ctx.key != "a":
            started.set()
            await release.wait()
        return [{"n": file}]

    project = Project(assets=[files, parse], default_store=FileStore(solera.root / "data"))
    await solera.deploy(project, cancel_grace=30, heartbeat_seconds=0.2)
    await solera.run("files")
    run = await solera.submit("parse")
    await solera.until(started.is_set)
    await solera.cancel(run["id"])
    [attempt] = attempts_of(await solera.wait(run), "parse")
    assert attempt["keys"] == {"ok": 1, "canceled": 3}  # c and d too, never started
    canceled = await solera.outcomes("parse", "canceled")
    assert {k: r["outcome"] for k, r in canceled.items()} == dict.fromkeys("bcd", "canceled")
    release.set()
    await solera.retry_keys("parse", "canceled")
    assert set(await solera.rows("rows", column="n")) == set("abcd")


# -- KEY-4 ---------------------------------------------------------------------


async def test_key_4_returning_none_changes_nothing_and_removing_is_explicit(solera):
    """A call returning None keeps the key's previous rows;
    `Patch(None, remove=[ctx.key])` removes them; a Patch for any other key
    fails the call."""

    content = {"a": {"v": 1}, "b": {"v": 1}, "c": {"v": 1}}
    mode = dict.fromkeys(content, "rows")

    def parse(ctx, file: dict):
        if mode[ctx.key] == "none":
            return None
        if mode[ctx.key] == "remove":
            return Patch(None, remove=[ctx.key])
        if mode[ctx.key] == "other":
            return Patch(None, remove=["a"])
        return [{"v": file["v"]}]

    await solera.deploy(files_project(solera.root, content, parse))
    await solera.run("parse", upstream=True)
    assert await solera.rows("samples") == {"a": 1, "b": 1, "c": 1}
    for key in content:
        content[key] = {"v": 2}
    mode.update(a="none", b="remove", c="other")
    await solera.run("parse", upstream=True)
    assert await solera.rows("samples") == {"a": 1, "c": 1}  # a unchanged, b removed, c kept
    assert (await solera.outcomes("parse"))["c"]["outcome"] == "failed"


async def test_key_4_a_key_given_no_rows_does_not_exist(solera):
    content = {"a": {"n": [1]}, "b": {"n": []}}

    def parse(ctx, file: dict):
        return [{"v": n} for n in file["n"]]

    await solera.deploy(files_project(solera.root, content, parse))
    await solera.run("parse", upstream=True)
    assert sorted(await solera.keys("samples")) == ["a"]
    assert (await solera.outcomes("parse"))["b"]["outcome"] == "ok"  # processed: it has no rows
    content["b"] = {"n": [2]}
    await solera.run("parse", upstream=True)
    assert sorted(await solera.keys("samples")) == ["a", "b"]
    content["b"] = {"n": []}
    await solera.run("parse", upstream=True)
    assert sorted(await solera.keys("samples")) == ["a"]
    assert await solera.rows("samples") == {"a": 1}


# -- KEY-6 ---------------------------------------------------------------------


def busy_project(root, tries: list, *, automations=()) -> Project:
    """`items` (keyed, one key `a`) -> `checks`, per key, whose first call
    is told to come back in a minute."""

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a", "v": 1}]

    @asset(
        inputs={"item": Incremental("items", each=True)},
        outputs=Output("checks", key="id"),
        automations=automations,
    )
    def checks(ctx, item: list):
        tries.append(ctx.key)
        if len(tries) == 1:
            raise Transient("busy", retry_after=60)
        return [{"v": item[0]["v"]}]

    return Project(assets=[items, checks], default_store=FileStore(root / "data"))


async def test_key_6_an_automated_per_key_asset_retries_its_due_keys_on_its_own(solera):
    tries = []
    await solera.deploy(busy_project(solera.root, tries, automations=OnChange()))
    await solera.run("checks", upstream=True)
    assert (await solera.outcomes("checks"))["a"]["outcome"] == "retrying"
    await solera.settle(0.3)
    assert tries == ["a"]  # not due yet: nothing comes back early
    solera.advance(61)

    async def healed():
        return (await solera.outcomes("checks"))["a"]["outcome"] == "ok"

    await solera.until(healed)
    assert tries == ["a", "a"]
    assert await solera.rows("checks") == {"a": 1}


async def test_key_6_a_run_by_hand_picks_its_due_keys_up_on_its_next_run(solera):
    tries = []
    await solera.deploy(busy_project(solera.root, tries))
    await solera.run("checks", upstream=True)
    before = len(await solera.runs())
    solera.advance(61)
    await solera.settle(0.3)
    assert tries == ["a"] and len(await solera.runs()) == before  # nothing automated: nothing starts
    await solera.run("checks")
    assert tries == ["a", "a"] and (await solera.outcomes("checks"))["a"]["outcome"] == "ok"


async def test_key_6_with_no_input_head_the_retry_clock_waits(solera):
    """F20: `checks` (automated) owes a forced retry when a move leaves
    `items` with no head. Hours pass: no run starts, tick after tick."""

    def project(store):
        @asset(outputs=Output("items", key="id", store=store))
        def items():
            return [{"id": "a"}]

        @asset(
            inputs={"item": Incremental("items", each=True)},
            outputs=Output("checks", key="id"),
            automations=OnChange(),
        )
        def checks(ctx, item: list):
            raise RuntimeError("bad")

        root = solera.root
        return Project(
            assets=[items, checks],
            stores={"other": FileStore(root / "other")},
            default_store=FileStore(root / "data"),
        )

    await solera.deploy(project(None))
    await solera.run("checks", upstream=True)
    await solera.post("/assets/checks/keys:retry", {"classes": ["failed"]})
    await solera.deploy(project("other"))  # `items` moves: no head until it writes
    assert await solera.head("items") is None
    await solera.settle(0.2)
    before = len(await solera.runs())
    for _ in range(3):
        solera.advance(3600)
        await solera.settle(0.2)
    assert len(await solera.runs()) == before


# -- KEY-7 ---------------------------------------------------------------------


async def test_key_7_a_forced_retry_calls_each_matching_key_once(solera):
    tries = {"n": 0}

    def parse(file: dict):
        tries["n"] += 1
        if tries["n"] < 3:
            raise Unprocessable("bad")
        return [{"value": 1}]

    await solera.deploy(files_project(solera.root, {"a": {"text": "1"}}, parse))
    await solera.run("parse", upstream=True)
    with pytest.raises(Refused):
        await solera.retry_keys("files", "failed")
    with pytest.raises(Refused):
        await solera.retry_keys("parse", "broken")
    await solera.retry_keys("parse", "failed")  # no rejected key is retried for this
    assert tries["n"] == 1
    await solera.retry_keys("parse", "rejected")
    assert tries["n"] == 2  # once per request, rejected again
    await solera.run("parse")
    assert tries["n"] == 2
    await solera.retry_keys("parse", "all")
    assert tries["n"] == 3
    assert await solera.outcomes("parse", *STORED) == {}


async def test_key_7_a_forced_retry_runs_after_its_source_reverts(solera):
    """`checks` rejects k1 at version 1; the source goes to 2 and back to 1
    (the net change: nothing). A forced retry of rejected keys still calls it."""

    outside, calls, failing = Outside(), [], [True]

    async def checks(ctx, row: list):
        calls.append(ctx.key)
        if failing[0]:
            raise Rejected("reject once")
        return [{"v": row[0]["v"]}]

    await solera.deploy(fed_project(solera.root, outside, checks))
    await feed(solera, outside, k1="1")
    await solera.run("checks")
    failing[0] = False
    for v in ("2", "1"):
        await feed(solera, outside, k1=v)
    calls.clear()
    await solera.retry_keys("checks", "rejected")
    assert calls == ["k1"]
    assert await solera.outcomes("checks", *STORED) == {}
    assert await solera.rows("checks") == {"k1": "1"}


# -- KEY-8 ---------------------------------------------------------------------


async def batches_of(solera, detail: dict, name: str) -> list[list[str]]:
    """What each batch of `name`'s task in a run was called on, in order:
    the lines its producer logged (`ctx.log`), read back attempt by attempt
    (`GET /runs/{id}/attempts/{attempt}/logs`); a batch that called nothing
    is left out."""

    out = []
    for attempt in attempts_of(detail, name):
        path = f"/runs/{detail['request']['id']}/attempts/{attempt['id']}/logs"
        lines = (await solera.request("GET", path, ok=False)).text.splitlines()
        if called := sorted(json.loads(line)["message"] for line in lines if line):
            out.append(called)
    return out


async def test_key_8_retries_and_new_changes_alternate(solera):
    """Four keys fail; a deploy fixes the bug as four other keys arrive. In
    batches of two, retries and new changes take turns: new keys keep
    flowing while retries are owed. Every key is called once. (A failed key
    that also changed upstream: the next test.)"""

    content, written = {f"r{i}": {"n": 1} for i in range(4)}, {}
    broken = {"on": True}

    def parse(ctx, file: dict):
        ctx.log(f"{ctx.key}@{file['n']}")
        if broken["on"]:
            raise ValueError("bug")
        return [{"n": file["n"]}]

    await solera.deploy(files_project(solera.root, content, parse, written=written, batch_size=2))
    await solera.run("parse", upstream=True)
    assert set(await solera.outcomes("parse", "failed")) == set(content)
    broken["on"] = False
    content.update({f"n{i}": {"n": 1} for i in range(4)})
    fixed = files_project(solera.root, content, parse, written=written, batch_size=2, build="fixed")
    await solera.deploy(fixed)
    await solera.run("files")
    batches = await batches_of(solera, await solera.run("parse"), "parse")

    kinds = [{call[0] for call in batch} for batch in batches]  # "r": retried, "n": new
    assert kinds.count({"r"}) == 2 and kinds.count({"n"}) == 2, batches
    assert all(a != b for a, b in zip(kinds, kinds[1:], strict=False)), batches  # they take turns
    calls = [call for batch in batches for call in batch]
    assert sorted(calls) == sorted(f"{k}@1" for k in content)  # each once
    assert await solera.outcomes("parse", *STORED) == {}
    assert await solera.rows("samples", column="n") == {k: v["n"] for k, v in content.items()}


async def test_key_8_a_run_finishes_every_pass_it_began(solera):
    """`checks` (batches of 2) has a failed key; feed commits k1, k2, k3 and
    a forced retry runs: its batches alternate between the new changes and
    the retry, and it delivers every key before it ends."""

    outside, broken = Outside(), {"bad"}

    async def checks(ctx, row: list):
        if ctx.key in broken:
            raise RuntimeError("down")
        return [{"v": row[0]["v"]}]

    await solera.deploy(fed_project(solera.root, outside, checks, retries=Retry(0), batch_size=2))
    await feed(solera, outside, a="1", bad="1")
    await solera.run("checks")
    assert sorted(await solera.keys("checks")) == ["a"]  # `bad` failed

    broken.clear()
    await feed(solera, outside, k1="1", k2="1", k3="1")
    [detail] = await solera.retry_keys("checks", "failed")
    assert detail["request"]["status"] == "succeeded"
    assert sorted(await solera.keys("checks")) == ["a", "bad", "k1", "k2", "k3"]
    assert (await solera.head("checks"))["materialized"] and await solera.stale("checks") == []


async def test_key_8_a_due_key_that_changed_upstream_is_processed_once_at_its_new_version(solera):
    outside, calls, busy = Outside(), [], {"k1"}

    async def checks(ctx, row: list):
        calls.append(ctx.key)
        if ctx.key in busy:
            raise Transient("busy", retry_after=60)
        return [{"v": row[0]["v"]}]

    await solera.deploy(fed_project(solera.root, outside, checks))
    await feed(solera, outside, k1="1")
    await solera.run("checks")
    busy.clear()
    await feed(solera, outside, k1="2")
    solera.advance(61)  # due, and owed for its change
    calls.clear()
    await solera.run("checks")
    assert calls == ["k1"]
    assert await solera.rows("checks") == {"k1": "2"}


# -- KEY-9 ---------------------------------------------------------------------


async def test_key_9_a_user_cancel_commits_finished_keys_and_leaves_the_rest_owed(solera):
    """Cancel while b and d run: a, c and e (removed) commit; b and d stay
    owed and show `canceled`. Nothing starts a run for them alone; the next
    run processes them."""

    content = dict.fromkeys("abcde", 1)
    hang, started, finished, release = set(), set(), set(), asyncio.Event()

    async def parse(ctx, file: int):
        if ctx.key in hang:
            started.add(ctx.key)
            await release.wait()
        finished.add(ctx.key)
        return [{"n": file}]

    await solera.deploy(keyed_project(solera.root, content, parse), cancel_grace=30, heartbeat_seconds=0.2)
    await solera.run("parse", upstream=True)
    content.update(dict.fromkeys("abcd", 2))
    del content["e"]
    hang.update("bd")
    finished.clear()
    await solera.run("files")
    run = await solera.submit("parse")
    await solera.until(lambda: started == {"b", "d"} and finished >= {"a", "c"})
    await solera.cancel(run["id"])
    assert (await solera.wait(run))["request"]["status"] == "canceled"
    assert await solera.rows("rows", column="n") == {"a": 2, "b": 1, "c": 2, "d": 1}
    assert set(await solera.outcomes("parse", "canceled")) == {"b", "d"}
    assert set(await solera.stale_keys("parse")) == {"b", "d"}
    before = len(await solera.runs())
    solera.advance(86_400)
    await solera.settle(0.3)
    assert len(await solera.runs()) == before
    hang.clear()
    release.set()
    await solera.run("parse")
    assert await solera.rows("rows", column="n") == {"a": 2, "b": 2, "c": 2, "d": 2}
    assert await solera.outcomes("parse", *STORED) == {}


async def test_key_9_a_timeout_counts_a_try_comes_due_after_backoff_and_fails_past_retries(solera):
    async def parse(ctx, file: int):
        if ctx.key == "b":
            await asyncio.sleep(30)
        return [{"n": file}]

    project = keyed_project(solera.root, {"a": 1, "b": 2}, parse, timeout=0.3, retries=Retry(1, delay=0))
    await solera.deploy(project, heartbeat_seconds=0.1)
    await solera.run("files")
    detail = await solera.run("parse", expect=None)
    attempt, *rest = attempts_of(detail, "parse")
    assert attempt["outcome"] == "failed" and attempt["keys"] == {"ok": 1, "timed_out": 1}
    assert [a["outcome"] for a in rest] == ["skipped"]  # the retry finds nothing due yet
    assert set(await solera.rows("rows", column="n")) == {"a"}
    b = (await solera.outcomes("parse"))["b"]
    assert (b["outcome"], b["tries"], b["eligible"]) == ("timed_out", 1, False)
    assert b["next_at"] >= b["last"] + 60
    solera.advance(61)
    assert (await solera.outcomes("parse"))["b"]["eligible"]
    await solera.run("parse", expect=None)
    b = (await solera.outcomes("parse"))["b"]
    assert b["outcome"] == "failed" and b["tries"] > 1  # past retries=1


async def test_key_9_a_user_cancel_during_a_timeout_drain_makes_its_keys_canceled(solera):
    stored, release = asyncio.Event(), asyncio.Event()

    class SlowStore(FileStore):
        async def store(self, write, prior, partition):
            stored.set()
            await release.wait()
            return await super().store(write, prior, partition)

    async def parse(ctx, file: int):
        if ctx.key == "b":
            await asyncio.sleep(60)
        return [{"n": file}]

    project = keyed_project(
        solera.root, {"a": 1, "b": 2}, parse, store=SlowStore(solera.root / "slow"), timeout=0.3
    )
    await solera.deploy(project, cancel_grace=30, heartbeat_seconds=0.1)
    await solera.run("files")
    run = await solera.submit("parse")
    await solera.until(stored.is_set)  # the timeout drain is storing `a`
    await solera.cancel(run["id"])
    await solera.settle(0.5)  # the cancel reaches the worker with its next beat
    release.set()
    await solera.wait(run)
    b = (await solera.outcomes("parse"))["b"]
    assert (b["outcome"], b["tries"], b["next_at"]) == ("canceled", 0, None)


# -- KEY-11 --------------------------------------------------------------------

CONTENT = {
    "a.csv": {"text": "1"},
    "bad.csv": {"text": "reject"},
    "bug.csv": {"text": "bug"},
    "draft-1.csv": {"text": "2"},
    "notes.txt": {"text": "3"},
}


def console_project(root, content: dict, parts: dict) -> Project:
    """`files` -> `parse` (per key, `*.csv` but drafts), `parted` -> `consume`
    (by day, `files` a dep), and `notify`, a job."""

    @asset(outputs=Output("files", keyed=True))
    def files():
        return dict(content)

    @asset(
        inputs={"file": Incremental("files", include="*.csv", exclude={"drafts": "draft-*"}, each=True)},
        outputs=Output("samples", key="path"),
    )
    def parse(file: dict):
        if file["text"] == "reject":
            raise Unprocessable("empty file")
        if file["text"] == "bug":
            raise ValueError("unexpected header")
        return [{"value": int(file["text"])}]

    days = StaticPartitions(["x", "y"])

    @asset(outputs=Output("parts", keyed=True), partitions={"day": days})
    def parted(ctx):
        return dict(parts[ctx.partition])

    @asset(inputs={"parts": Incremental(batch_size=1)}, partitions={"day": days}, deps=["files"])
    def consume(parts: dict):
        return {"n": len(parts)}

    @job
    def notify(ctx):
        ctx.log("done")

    return Project(
        assets=[files, parse, parted, consume, notify], name="console", default_store=FileStore(root / "data")
    )


async def console(solera) -> dict:
    """The console's project, served: its `files`' content, to change."""

    content = {k: dict(v) for k, v in CONTENT.items()}
    parts = {"x": {"k1": 1, "k2": 2}, "y": {"k1": 1}}
    await solera.deploy(console_project(solera.root, content, parts))
    return content


async def test_key_11_explain_says_why_a_key_is_or_is_not_there(solera):
    content = await console(solera)
    await solera.run("parse", upstream=True)

    ok = await solera.explain("parse", "a.csv")
    assert (ok["verdict"], ok["input"], ok["upstream"], ok["upstream_partition"]) == (
        "ok",
        "file",
        "files",
        "",
    )
    assert ok["outputs"] == {
        "samples": {"present": True, "generation": ok["outputs"]["samples"]["generation"]}
    }
    assert ok["last"]["outcome"] == "ok" and ok["last_ok"] == ok["last"]
    assert ok["last_ok"]["generation"] == ok["upstream_generation"]
    assert ok["patterns"]["included"] and ok["patterns"]["excluded_by"] is None
    assert ok["patterns"]["pending"] is None and ok["outcome"] is None

    failing = await solera.explain("parse", "bad.csv")
    assert failing["verdict"] == "failing" and failing["outcome"]["outcome"] == "rejected"
    assert failing["last"]["outcome"] == "rejected" and failing["last_ok"] is None
    excluded = await solera.explain("parse", "draft-1.csv")
    assert excluded["verdict"] == "excluded" and excluded["patterns"]["excluded_by"] == "drafts"
    assert (await solera.explain("parse", "notes.txt"))["verdict"] == "not_matched"
    assert (await solera.explain("parse", "ghost.csv"))["verdict"] == "absent"

    # The upstream moves on: a changed key is pending, a deleted one removed.
    content["a.csv"] = {"text": "7"}
    del content["bug.csv"]
    content["new.csv"] = {"text": "8"}
    await solera.run("files")
    pending = await solera.explain("parse", "a.csv")
    assert pending["verdict"] == "pending"
    assert pending["last_ok"]["generation"] != pending["upstream_generation"]
    assert (await solera.explain("parse", "new.csv"))["verdict"] == "pending"
    assert (await solera.explain("parse", "bug.csv"))["verdict"] == "failing"  # its record outlives the key
    await solera.run("parse")
    assert (await solera.explain("parse", "bug.csv"))["verdict"] == "removed"
    assert (await solera.explain("parse", "a.csv"))["verdict"] == "ok"

    with pytest.raises(Refused) as refused:
        await solera.explain("files", "a.csv")
    assert refused.value.status == 400
    with pytest.raises(Refused) as refused:
        await solera.explain("parse", "a.csv", partition="x")
    assert refused.value.status == 404
    await solera.run("parted", partitions=["y"])  # a plain Incremental input never delivered
    consume = await solera.explain("consume", "k1", partition="y")
    assert consume["verdict"] == "pending"
    assert consume["last"] is None and consume["outputs"] == {}


async def test_key_11_explain_a_key_restored_after_its_removal_is_pending(solera):
    content = await console(solera)
    await solera.run("parse", upstream=True)
    original = content.pop("a.csv")
    await solera.run("parse", upstream=True)  # the removal is delivered: no row holds it
    content["a.csv"] = original  # back as it was: written again, at a generation of its own
    await solera.run("files")

    restored = await solera.explain("parse", "a.csv")
    assert restored["last"]["outcome"] == "removed"
    assert restored["last_ok"]["generation"] != restored["upstream_generation"]  # a new write of it
    assert restored["outputs"]["samples"]["present"] is False
    assert restored["verdict"] == "pending"
    await solera.run("parse")
    delivered = await solera.explain("parse", "a.csv")
    assert delivered["verdict"] == "ok" and delivered["outputs"]["samples"]["present"] is True
