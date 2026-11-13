"""INC: incremental delivery and classification (docs/behaviors.md)."""

import pytest
from solera.sdk import Incremental, OnChange, Output, Project, Source, asset
from solera.stores import FileStore, Patch

from tests.contract.example import Batch, Example
from tests.contract.harness import Outside, SourceStore, rebuild
from tests.contract.test_sta import Rebuild, decided


def holds(batches: list[Batch], held=()) -> set:
    """What a consumer holds after `batches`, from their classes alone. Each
    batch must agree with it: `added` a key it lacks, `updated` and `removed`
    one it holds; a reset empties it."""

    held = set(held)
    for b in batches:
        if b.reset:
            held.clear()
        assert not set(b.added) & held, f"added again: {b.classes}, holding {sorted(held)}"
        assert set(b.updated) | set(b.removed) <= held, f"never held: {b.classes}, holding {sorted(held)}"
        held = (held | set(b.added)) - set(b.removed)
    return held


def statuses(detail: dict) -> dict:
    return {t["asset"]: t["status"] for t in detail["tasks"]}


def attempts_of(detail: dict, name: str) -> list[dict]:
    [task] = [t for t in detail["tasks"] if t["asset"] == name]
    return detail["attempts"][task["id"]]


def commits_of(detail: dict, name: str) -> int:
    return sum(1 for a in attempts_of(detail, name) if a.get("commit"))


# -- INC-1 ----------------------------------------------------------------------------


async def test_inc_1_a_first_run_delivers_every_upstream_key_as_added(solera):
    ex = await Example(solera).start(batch_size=2)
    await ex.feed({f"k{i}": "1" for i in range(1, 6)})
    batches = await ex.run("tally")
    assert [b.added for b in batches] == [["k1", "k2"], ["k3", "k4"], ["k5"]]
    assert not any(b.updated or b.removed for b in batches)
    assert await ex.count() == 5


@pytest.mark.parametrize("each", [False, True], ids=["plain", "per-key"])
async def test_inc_1_a_first_run_delivers_every_key_as_added_plain_or_per_key(solera, each):
    """A consumer that has processed nothing is owed every upstream key as
    an add: a plain tally over five keys, in batches of two, counts five,
    and a per-key consumer's batches say added too, none updated."""

    calls = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": f"k{i}", "v": 1} for i in range(5)]

    if each:

        @asset(inputs={"files": Incremental(batch_size=2, each=True)}, outputs=Output("out", key="id"))
        def out(ctx, files: list):
            calls.append(ctx.key)
            return [{"v": files[0]["v"]}]

    else:

        @asset(inputs={"files": Incremental(batch_size=2)}, outputs=Output("out"))
        async def out(ctx, files: list):
            batch = ctx.batch["files"]
            assert not batch.updated and not batch.removed
            calls.extend(batch.added)
            return {"count": ((await ctx.load()) or {"count": 0})["count"] + len(batch.added)}

    await solera.deploy(Project(assets=[files, out], default_store=FileStore(solera.root / "data")))
    detail = await solera.run("out", upstream=True)
    assert sorted(calls) == [f"k{i}" for i in range(5)]
    batches = [a["batch"] for a in attempts_of(detail, "out")]
    assert [(b["added"], b["updated"]) for b in batches] == [(2, 0), (2, 0), (1, 0)]
    if not each:
        assert (await solera.read("out"))["count"] == 5


# -- INC-2 ----------------------------------------------------------------------------


async def test_inc_2_a_keys_class_is_the_net_difference_with_upstream_now(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.run("tally")
    await ex.feed({"k1": "2", "k3": "1"}, removes=["k2"])
    [batch] = await ex.run("tally")
    assert batch.classes == (["k3"], ["k1"], ["k2"])
    assert batch.rows == ["k1", "k3"]
    assert await ex.count() == 2


async def test_inc_2_the_parameter_holds_the_changed_rows_and_the_batch_says_which(solera):
    """Under Incremental the parameter arrives filtered to the keys added or
    updated, and `ctx.batch` says which, and what was removed."""

    seen = {}
    content = {"rows": [{"id": "a", "v": 1}, {"id": "b", "v": 1}, {"id": "c", "v": 1}]}

    @asset(outputs=Output("files", key="id"))
    def files():
        return content["rows"]

    @asset(inputs={"files": Incremental()})
    def consumer(ctx, files: list):
        seen["rows"] = list(files)
        seen["added"] = list(ctx.batch["files"].added)
        seen["updated"] = list(ctx.batch["files"].updated)
        seen["deleted"] = list(ctx.batch["files"].removed)
        return [{"n": len(files)}]

    await solera.deploy(Project(assets=[files, consumer], default_store=FileStore(solera.root / "data")))
    await solera.run("consumer", upstream=True)
    assert seen["added"] == ["a", "b", "c"]  # a first run adds everything
    assert {r["id"] for r in seen["rows"]} == {"a", "b", "c"}

    # Nothing written since: skipped, nothing loaded.
    seen.clear()
    detail = await solera.run("consumer")
    assert statuses(detail)["consumer"] == "skipped"
    assert seen == {}

    # One key written again: only that key, as updated.
    content["rows"] = Patch([{"id": "b", "v": 2}])
    await solera.run("consumer", upstream=True)
    assert seen["updated"] == ["b"] and seen["added"] == []
    assert [r["id"] for r in seen["rows"]] == ["b"]

    # A removal arrives through ctx.batch.
    seen.clear()
    content["rows"] = Patch([], remove=["b"])
    await solera.run("consumer", upstream=True)
    assert seen["deleted"] == ["b"] and seen["added"] == seen["updated"] == []


# -- INC-3 ----------------------------------------------------------------------------


async def test_inc_3_a_key_added_and_removed_between_two_runs_delivers_nothing(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await solera.run(["tally", "copy", "checks"])
    await ex.feed({"k4": "1"})
    await ex.feed(removes=["k4"])
    for name in ("tally", "copy", "checks"):
        assert await solera.stale(name) == [], name
    assert await solera.stale_keys("checks") == {}
    ex.calls.clear()
    for name in ("tally", "copy", "checks"):
        assert await ex.run(name) == [], name
    assert ex.calls == {}
    assert await ex.count() == 2


async def test_inc_3_a_key_removed_and_added_back_is_updated(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1", "k3": "1"})
    await ex.run("copy")
    await ex.feed(removes=["k3"])
    await ex.feed({"k3": "1"})
    [batch] = await ex.run("copy")
    assert batch.classes == ([], ["k3"], [])
    assert await solera.rows("copy") == {"k1": "1", "k2": "1", "k3": "1"}


# -- INC-5 ----------------------------------------------------------------------------


async def test_inc_5_a_count_kept_from_its_batches_stays_exact_through_a_keys_run(solera):
    """`tally` = what it held + added - removed. k4 added and k1 removed;
    keys=(k4) delivers k4 as added; the next default run delivers k1's
    removal alone, never k4 again: the tally equals the upstream's count."""

    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1", "k3": "1"})
    await ex.run("tally")
    assert await ex.count() == 3
    await ex.feed({"k4": "2"}, removes=["k1"])
    await ex.run("tally", keys={"items": ["k4"]})
    assert await ex.count() == 4  # k1's removal not read yet
    await ex.run("tally")
    assert await ex.count() == 3 == len(ex.outside.feed)


async def test_inc_5_a_count_kept_from_the_classes_alone_stays_exact_through_any_history(solera):
    """Commits, `keys=` runs, a failed batch and a canceled run: once a
    default run owes nothing, the count is the number of keys upstream offers."""

    ex = await Example(solera).start(batch_size=2)
    await ex.feed({f"k{i}": "1" for i in range(1, 7)})
    await ex.run("tally")
    await ex.feed({"k1": "2", "k7": "1"}, removes=["k2"])
    await ex.run("tally", keys={"items": ["k1", "k2", "k9"]})
    await ex.feed({"k8": "1", "k2": "3"}, removes=["k3", "k4"])

    failed = []

    async def fail():
        ex.seen["tally"].pop()  # a failed batch delivered nothing
        failed.append(True)
        raise RuntimeError("tally's batch fails")

    ex.during = fail
    run = await solera.submit("tally")
    await solera.until(lambda: failed)
    solera.advance(60)  # past the retry's delay
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert "failed" in [a["outcome"] for a in attempts_of(detail, "tally")]
    await ex.feed({"k5": "2"}, removes=["k6"])
    run = await solera.submit("tally")

    async def cancel():
        await solera.cancel(run["id"])

    ex.during = cancel
    assert (await solera.wait(run))["request"]["status"] == "canceled"
    await ex.feed({"k3": "4"}, removes=["k7"])
    await ex.run("tally")
    assert await ex.run("tally") == []
    assert await ex.stale() == []
    assert holds(ex.seen["tally"]) == set(ex.outside.feed)
    assert await ex.count() == len(ex.outside.feed)


# -- INC-6 ----------------------------------------------------------------------------

CHANGES = {"update": ({"k1": "2"}, []), "add": ({"k3": "1"}, []), "remove": ({}, ["k1"])}


@pytest.mark.parametrize("change", CHANGES)
async def test_inc_6_a_change_after_a_keys_run_is_delivered_once(solera, change):
    """`keys=(k1)` processes k1; then k1 changes (or k3 is added, or k1
    removed); two default runs follow. The change arrives once, in the first."""

    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.run("tally", keys={"items": ["k1"]})
    upserts, removes = CHANGES[change]
    await ex.feed(upserts, removes=removes)
    delivered = {
        "update": (["k2"], ["k1"], []),
        "add": (["k2", "k3"], [], []),
        "remove": (["k2"], [], ["k1"]),
    }
    assert [b.classes for b in await ex.run("tally")] == [delivered[change]]
    assert await ex.run("tally") == []
    assert holds(ex.seen["tally"]) == set(ex.outside.feed)
    assert await ex.count() == len(ex.outside.feed)
    assert await ex.stale() == []


@pytest.mark.parametrize("change", CHANGES)
async def test_inc_6_a_change_made_during_a_run_is_delivered_once(solera, change):
    """Batch 0 of a run processes k1; while batch 1 runs, k1 changes (or k3
    is added, or k1 removed). The change arrives once, by the next run."""

    ex = await Example(solera).start(batch_size=1)
    await ex.feed({"k1": "1", "k2": "1"})

    async def at_batch_1():
        upserts, removes = CHANGES[change]
        await ex.feed(upserts, removes=removes)

    async def at_batch_0():
        ex.during = at_batch_1

    ex.during = at_batch_0
    assert [b.classes for b in await ex.run("tally")] == [(["k1"], [], []), (["k2"], [], [])]
    delivered = {"update": ([], ["k1"], []), "add": (["k3"], [], []), "remove": ([], [], ["k1"])}
    assert [b.classes for b in await ex.run("tally")] == [delivered[change]]
    assert await ex.run("tally") == []
    assert holds(ex.seen["tally"]) == set(ex.outside.feed)
    assert await ex.count() == len(ex.outside.feed)


# -- INC-7 ----------------------------------------------------------------------------


async def test_inc_7_a_removal_restored_at_its_version_is_owed_its_add(solera):
    """`items` reads `feed` at the source's versions. `feed` removes k1;
    `keys=(k1)` processes the removal; `feed` restores k1 at the version it
    once had: the next run owes k1 as added."""

    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.feed(removes=["k1"], items=False)
    [batch] = await ex.run("items", keys={"feed": ["k1"]})
    assert batch.classes == ([], [], ["k1"])
    await ex.feed({"k1": "1"}, items=False)
    assert await solera.stale("items") != []
    [batch] = await ex.run("items")
    assert batch.classes == (["k1"], [], [])
    assert await solera.rows("items") == {"k1": "1", "k2": "1"}


async def test_inc_7_a_delivered_removal_is_never_delivered_again(solera):
    """Once `tally` was given k5's removal, nothing owes it again, and k5
    coming back is an add: the count follows."""

    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k5": "1"})
    await ex.run("tally")
    await ex.feed(removes=["k5"])
    [batch] = await ex.run("tally", keys={"items": ["k5"]})
    assert batch.classes == ([], [], ["k5"])
    assert await ex.stale() == []
    assert await ex.run("tally") == []
    await ex.feed({"k5": "1"})
    [batch] = await ex.run("tally")
    assert batch.classes == (["k5"], [], [])
    assert await ex.count() == 2


# -- INC-9 ----------------------------------------------------------------------------


async def test_inc_9_a_change_behind_a_runs_batches_is_owed_by_the_next_run(solera):
    """Batch 0 commits k1; k1 changes while batch 1 runs: the run ends, the
    partition is stale, and the next run delivers k1 as updated."""

    ex = await Example(solera).start(batch_size=1)
    await ex.feed({"k1": "1", "k2": "1"})

    async def change_k1():
        await ex.feed({"k1": "2"})

    async def at_batch_0():
        ex.during = change_k1

    ex.during = at_batch_0
    assert [b.classes for b in await ex.run("tally")] == [(["k1"], [], []), (["k2"], [], [])]
    assert await ex.stale() != []
    [batch] = await ex.run("tally")
    assert batch.classes == ([], ["k1"], [])
    assert await ex.stale() == []
    assert await ex.count() == 2


async def test_inc_9_a_key_removed_during_a_run_before_its_batch_is_delivered_once(solera):
    """k1 and k2, a key a batch, never read; while k1's batch runs the
    source removes k2. k2's batch reads after that commit and finds k2
    absent and never held: nothing to deliver. The count is 1, the
    source's, and the next run owes nothing."""

    outside, seen, removing, counts = Outside(), [], {}, []

    @asset(inputs={"feed": Incremental(batch_size=1)}, outputs=Output("tally"))
    async def tally(ctx, feed: list):
        batch = ctx.batch["feed"]
        seen.append(Batch.of(batch, feed))
        before = ((await ctx.load()) or {"rows": 0})["rows"]
        if removing.pop("now", False):
            outside.feed.pop("k2")
            await solera.commit_source("feed", remove=["k2"])
        counts.append(before + len(batch.added) - len(batch.removed))
        return {"rows": counts[-1]}

    await solera.deploy(
        Project(
            assets=[tally],
            sources=[Source("feed", key="id", store="ext")],
            stores={"ext": SourceStore(solera.root / "ext", outside)},
            default_store=FileStore(solera.root / "default"),
        )
    )
    outside.feed.update({"k1": "1", "k2": "1"})
    await solera.commit_source("feed", upsert=dict(outside.feed))
    removing["now"] = True
    await solera.run("tally")
    await solera.run("tally")
    assert holds(seen) == {"k1"}
    assert counts == [1], [b.classes for b in seen]


# -- INC-10 ---------------------------------------------------------------------------


def _paged(root, content: dict, pages: list, include=None) -> Project:
    """`files` (keyed: `content["v"]`, its rows or a Patch) -> `consumer`, which
    records every field of each batch it gets."""

    @asset(outputs=Output("files", key="id"))
    def files():
        return content["v"]

    @asset(inputs={"files": Incremental(batch_size=3, include=include)}, outputs=Output("consumer"))
    def consumer(ctx, files: list):
        b = ctx.batch["files"]
        pages.append(
            {
                "at": (b.index, b.first, b.final, b.reset),
                "count": b.count,
                "classes": (sorted(b.added), sorted(b.updated), sorted(b.removed), sorted(b.unchanged)),
                "rows": sorted(r["id"] for r in files),
                "rows_are_the_parameter": b.rows is files,
                "upstream": (b.upstream.output, b.upstream.commits),
            }
        )
        return {"n": len(files)}

    return Project(assets=[files, consumer], default_store=FileStore(root / "data"))


async def test_inc_10_a_batch_says_where_it_sits_in_its_run(solera):
    """Seven keys in batches of three: `index` 0, 1, 2, `first` on batch 0,
    `final` only on the last, `reset` on none; `count` is an estimate, so
    only its type is checked. A delta of four keys: two batches, `final`
    on the second."""

    pages, content = [], {"v": [{"id": f"k{i}", "v": 1} for i in range(7)]}
    await solera.deploy(_paged(solera.root, content, pages))
    await solera.run("consumer", upstream=True)
    assert [p["at"] for p in pages] == [
        (0, True, False, False),
        (1, False, False, False),
        (2, False, True, False),
    ]
    assert all(isinstance(p["count"], int) and p["count"] >= 1 for p in pages)
    assert [p["rows"] for p in pages] == [["k0", "k1", "k2"], ["k3", "k4", "k5"], ["k6"]]
    assert all(p["upstream"] == ("files", None) for p in pages)

    pages.clear()
    content["v"] = Patch([{"id": k, "v": 2} for k in ("k0", "k2", "k4", "k6")])
    await solera.run("consumer", upstream=True)
    assert [p["at"] for p in pages] == [(0, True, False, False), (1, False, True, False)]
    assert [p["rows"] for p in pages] == [["k0", "k2", "k4"], ["k6"]]


async def test_inc_10_final_is_on_the_last_real_batch_whatever_the_plan(solera):
    """Patterns take keys after the plan is made: the run takes the batches
    it takes, none is empty, and `final` is on the last real one."""

    pages = []
    content = {"v": [{"id": f"k{i}", "v": 1} for i in range(7)]}
    await solera.deploy(_paged(solera.root, content, pages, include=["k0", "k1", "k2", "k3"]))
    await solera.run("consumer", upstream=True)
    assert [p["rows"] for p in pages] == [["k0", "k1", "k2"], ["k3"]]
    assert [p["at"] for p in pages] == [(0, True, False, False), (1, False, True, False)]


async def test_inc_10_a_batch_says_each_keys_class_and_rows_come_for_all_but_the_removed(solera):
    """k1 updated, k2 removed, k4 added and k3 named as it is: each in its
    class; `rows`, the parameter itself, holds k1, k3 and k4."""

    pages = []
    content = {"v": [{"id": f"k{i}", "v": 1} for i in (1, 2, 3)]}
    await solera.deploy(_paged(solera.root, content, pages))
    await solera.run("consumer", upstream=True)
    content["v"] = Patch([{"id": "k1", "v": 2}, {"id": "k4", "v": 1}], remove=["k2"])
    await solera.run("files")
    pages.clear()
    await solera.run("consumer", keys={"files": ["k1", "k2", "k3", "k4"]})
    classes = [sorted(sum((p["classes"][i] for p in pages), [])) for i in range(4)]
    assert classes == [["k4"], ["k1"], ["k2"], ["k3"]]
    assert sorted(sum((p["rows"] for p in pages), [])) == ["k1", "k3", "k4"]
    assert all(p["rows_are_the_parameter"] for p in pages)
    assert pages[-1]["at"][2] and not any(p["at"][3] for p in pages)


# -- INC-11 ---------------------------------------------------------------------------


async def test_inc_11_ctx_load_returns_the_output_as_materialized(solera):
    ex = await Example(solera).start(batch_size=2)
    await ex.feed({"k1": "1", "k2": "1", "k3": "1"})
    batches = await ex.run("tally")
    assert [b.held for b in batches] == [None, {"rows": 2}]  # None before its first commit
    await ex.feed({"k4": "1"})
    [batch] = await ex.run("tally")
    assert batch.held == {"rows": 3}
    assert await solera.read("tally") == {"rows": 4}


# -- INC-12 ---------------------------------------------------------------------------


async def test_inc_12_a_task_walks_what_it_owes_in_key_order_batch_size_keys_a_commit(solera):
    """Seven owed keys, `batch_size=3`: three batches in key order, three commits."""

    ex = await Example(solera).start(batch_size=3)
    await ex.feed({f"k{i}": "1" for i in (7, 3, 1, 6, 2, 5, 4)})
    run = await solera.submit("copy")
    detail = await solera.wait(run)
    assert [b.rows for b in ex.seen["copy"]] == [["k1", "k2", "k3"], ["k4", "k5", "k6"], ["k7"]]
    assert commits_of(detail, "copy") == 3
    assert await solera.rows("copy") == {f"k{i}": "1" for i in range(1, 8)}

    await ex.feed({k: "2" for k in ("k7", "k5", "k2", "k6")})
    detail = await solera.wait(await solera.submit("copy"))
    assert [b.rows for b in ex.seen["copy"][3:]] == [["k2", "k5", "k6"], ["k7"]]  # only what it owes
    assert commits_of(detail, "copy") == 2


def test_inc_12_the_default_batch_size_is_ten_thousand_keys():
    assert Incremental().batch_size == 10_000
    assert Incremental(each=True).batch_size == 10_000


# -- INC-14 ---------------------------------------------------------------------------


async def _written(solera, content: dict) -> list:
    """`feed` writes `content["v"]`; `consumer` runs on every change of it."""

    fired = []

    @asset(outputs=Output("feed", key="id"))
    def feed():
        return content["v"]

    @asset(inputs={"feed": Incremental()}, automations=OnChange())
    def consumer(ctx, feed: list):
        fired.append((sorted(ctx.batch["feed"].added), sorted(ctx.batch["feed"].updated)))
        return []

    await solera.deploy(Project(assets=[feed, consumer], default_store=FileStore(solera.root / "data")))
    await solera.run("feed")
    await solera.until(lambda: fired)
    await solera.quiet()
    return fired


async def test_inc_14_writing_nothing_commits_nothing_and_wakes_nothing(solera):
    content = {"v": [{"id": "a", "v": 1}]}
    fired = await _written(solera, content)
    assert fired == [(["a"], [])]
    head, keys, runs = await solera.head("feed"), await solera.keys("feed"), len(await solera.runs())
    content["v"] = Patch([])
    await solera.run("feed")
    await solera.settle(0.3)
    after = await solera.head("feed")
    assert (after["commit_number"], after["ref"]) == (head["commit_number"], head["ref"])
    assert await solera.keys("feed") == keys
    assert len(await solera.runs()) == runs + 1  # the poll's own run, and no automation's
    assert fired == [(["a"], [])]
    assert await solera.stale("consumer") == []


async def test_inc_14_writing_the_same_rows_again_is_a_change_its_consumers_process(solera):
    content = {"v": [{"id": "a", "v": 1}, {"id": "b", "v": 1}]}
    fired = await _written(solera, content)
    keys = await solera.keys("feed")
    await solera.run("feed")
    await solera.until(lambda: len(fired) == 2)
    await solera.quiet()
    await solera.settle(0.3)
    again = await solera.keys("feed")
    assert set(again) == set(keys) == {"a", "b"}
    assert all(again[k] != keys[k] for k in keys)  # a new version of each
    assert await solera.rows("feed") == {"a": 1, "b": 1}
    assert fired == [(["a", "b"], []), ([], ["a", "b"])]


# -- target (PR 2) --


# -- INC-4 (target) ---------------------------------------------------------------------


async def test_inc_4_an_update_reverted_to_the_processed_version_is_not_delivered(solera):
    """feed's k1 goes 1 -> 2 -> 1 before items runs, and k2 goes 1 -> 2: items
    is stale while k1 is at 2 and fresh once it is back; its run delivers
    k2 alone."""

    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.feed({"k1": "2"}, items=False)
    assert await solera.stale("items") == ["input changed"]
    await ex.feed({"k1": "1"}, items=False)
    assert await solera.stale("items") == []
    await ex.feed({"k2": "2"}, items=False)
    [batch] = await ex.run("items")
    assert (batch.classes, batch.rows) == (([], ["k2"], []), ["k2"])
    assert await solera.rows("items") == {"k1": "1", "k2": "2"}


# -- INC-8 (target) ---------------------------------------------------------------------


def stepped(root, outside: Outside, got: list, steps: list) -> Project:
    """feed -> items -> `copy`, a key a batch, recording each batch's classes
    and the values its rows carry; `steps` run inside its next batches, one
    each, in order."""

    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id"))
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(inputs={"items": Incremental(batch_size=1)}, outputs=Output("copy", key="id"))
    async def copy(ctx, items: list):
        batch = ctx.batch["items"]
        got.append(
            (
                sorted(batch.added),
                sorted(batch.updated),
                sorted(batch.removed),
                {r["id"]: r["v"] for r in items},
            )
        )
        if steps:
            await steps.pop(0)()
        return rebuild(batch, [{"id": r["id"], "v": r["v"]} for r in items])

    return Project(
        assets=[items, copy],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(root / "ext", outside)},
        default_store=FileStore(root / "out"),
    )


async def test_inc_8_each_batch_reads_upstream_as_it_is_when_it_starts(solera):
    """A run over k1, k2, a key a batch: while batch 1 has k1, k2 changes;
    batch 2 delivers k2's new version."""

    outside, got, steps = Outside(), [], []
    await solera.deploy(stepped(solera.root, outside, got, steps))

    async def change(**upserts):
        outside.feed.update(upserts)
        await solera.commit_source("feed", upsert=upserts)
        await solera.run("items")

    await change(k1="1", k2="1")
    steps.append(lambda: change(k2="2"))
    await solera.run("copy")
    assert got == [(["k1"], [], [], {"k1": "1"}), (["k2"], [], [], {"k2": "2"})]
    assert await solera.rows("copy") == {"k1": "1", "k2": "2"} and await solera.stale("copy") == []


async def test_inc_8_a_batch_never_delivers_a_key_older_than_one_already_delivered(solera):
    """k1, k2, k3 go to 2 and a run of copy begins, a key a batch. Inside its
    first batch k2 goes to 3 and `keys=(k2)` is asked for. However the two
    runs interleave, k2 is delivered once, at 3: never at 2 after 3."""

    outside, got, steps = Outside(), [], []
    await solera.deploy(stepped(solera.root, outside, got, steps))

    async def change(**upserts):
        outside.feed.update(upserts)
        await solera.commit_source("feed", upsert=upserts)
        await solera.run("items")

    await change(k1="1", k2="1", k3="1")
    await solera.run("copy")
    await change(k1="2", k2="2", k3="2")
    keyed = []

    async def inside():
        await change(k2="3")
        keyed.append(await solera.submit("copy", keys={"items": ["k2"]}))

    steps.append(inside)
    del got[:]
    await solera.run("copy")
    await solera.wait(keyed[0])
    delivered = [rows.get("k2") for added, updated, removed, rows in got if "k2" in added + updated + removed]
    assert delivered == ["3"], got
    assert await solera.rows("copy") == {"k1": "2", "k2": "3", "k3": "2"}
    assert await solera.stale("copy") == []


# -- INC-15 (target) --------------------------------------------------------------------


async def test_inc_15_an_upstream_full_run_never_makes_a_keyed_reader_remove_and_add_back(solera):
    """copy holds k1-k4 and runs full while items holds k1, k2, k4, k5, two
    keys a batch. Its first commit is a clear and its numbering goes on;
    archive runs no batch until copy completes, then gets one difference,
    not reset: k1, k2, k4 updated, k3 removed, k5 added."""

    async with Rebuild(solera) as r:
        await r.start()
        number = (await solera.head("copy"))["commit_number"]
        run = await r.midway()
        assert await solera.rows("copy") == {"k1": "1", "k2": "1"}  # the clear, and batch 1's keys
        reader = await solera.submit("archive")
        task = await decided(solera, reader, "archive")
        assert task["status"] == "queued" and len(r.seen["archive"]) == 1, r.seen["archive"][1:]
        await r.finish(run)
        assert (await solera.wait(reader))["request"]["status"] == "succeeded"
        [batch] = r.seen["archive"][1:]
        assert not batch.reset and batch.classes == (["k5"], ["k1", "k2", "k4"], ["k3"])
        assert (await solera.head("copy"))["commit_number"] == number + 2
        assert await solera.rows("archive") == {"k1": "1", "k2": "1", "k4": "1", "k5": "1"}
