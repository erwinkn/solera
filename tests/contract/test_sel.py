"""SEL: `keys=` lists, `"all"`, full runs, `batch.reset` (docs/behaviors.md)."""

import asyncio
import copy

import pytest
from solera.sdk import Incremental, Output, Project, Result, Source, asset
from solera.stores import FileStore, Patch

from tests.contract.example import Example
from tests.contract.harness import Outside, Refused, SourceStore
from tests.contract.test_sta import INCOMPLETE, Rebuild, task_of


def files_project(root, content: dict, fn, version: str = "1", **input) -> Project:
    """`files`, a producer that writes what changed in `content` since it last
    ran (a `Patch`), and `parse`, the per-key `fn` over each of its keys,
    writing `samples`."""

    written = {}

    @asset(outputs=Output("files", keyed=True))
    def files():
        changed = {k: v for k, v in content.items() if written.get(k) != v}
        gone = [k for k in written if k not in content]
        written.clear()
        written.update(copy.deepcopy(content))
        return Patch(changed, remove=gone)

    parse = asset(
        fn,
        inputs={"file": Incremental("files", **input, each=True)},
        outputs=Output("samples", key="path"),
        version=version,
    )
    return Project(assets=[files, parse], default_store=FileStore(root / "data"))


async def samples(solera) -> dict:
    return await solera.read("samples", typ=dict[str, list])


def attempts_of(detail: dict, name: str) -> list[dict]:
    [task] = [t for t in detail["tasks"] if t["asset"] == name]
    return detail["attempts"][task["id"]]


async def built(solera, keys=("k1", "k2", "k3"), **decl) -> Example:
    """The running example with `items` holding `keys` at "1", and `tally`,
    `copy` and `checks` built from it."""

    ex = await Example(solera).start(**decl)
    await ex.feed(dict.fromkeys(keys, "1"))
    await solera.run(["tally", "copy", "checks"])
    return ex


# -- SEL-1 ----------------------------------------------------------------------------


async def test_sel_1_a_keys_run_loads_exactly_the_named_keys_and_never_touches_another(solera):
    """`copy` holds k1, k2, k3; k1 and k2 change. `keys=(k1)` delivers k1 as
    updated; k2 and k3 keep their rows and versions, and k2 is still owed.
    A named key already processed as it is comes as `unchanged`."""

    ex = await built(solera)
    before = await solera.keys("copy")
    await ex.feed({"k1": "2", "k2": "2"})
    [batch] = await ex.run("copy", keys={"items": ["k1"]})
    assert (batch.classes, batch.unchanged, batch.rows) == (([], ["k1"], []), [], ["k1"])
    assert await solera.rows("copy") == {"k1": "2", "k2": "1", "k3": "1"}
    after = await solera.keys("copy")
    assert {k: after[k] for k in ("k2", "k3")} == {k: before[k] for k in ("k2", "k3")}
    assert await solera.stale("copy") != []

    [batch] = await ex.run("copy", keys={"items": ["k3"]})
    assert (batch.classes, batch.unchanged, batch.rows) == (([], [], []), ["k3"], ["k3"])
    assert await solera.stale("copy") != []
    [batch] = await ex.run("copy")
    assert batch.classes == ([], ["k2"], [])
    assert await solera.rows("copy") == {"k1": "2", "k2": "2", "k3": "1"}


async def test_sel_1_a_keys_list_loads_the_keys_it_names_and_all_every_key(solera):
    """A `keys=` list loads the keys it names: b, unchanged, as it is;
    `"all"` loads every key again."""

    seen = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a", "v": 1}, {"id": "b", "v": 1}]

    @asset(inputs={"files": Incremental()})
    def consumer(ctx, files: list):
        seen.append(sorted(r["id"] for r in files))
        return []

    await solera.deploy(Project(assets=[files, consumer], default_store=FileStore(solera.root / "data")))
    await solera.run("consumer", upstream=True)
    await solera.run("consumer", keys={"files": ["b"]})
    assert seen == [["a", "b"], ["b"]]
    await solera.run("consumer", keys={"files": "all"})
    assert seen[-1] == ["a", "b"]


# -- SEL-2 ----------------------------------------------------------------------------


async def test_sel_2_a_named_key_upstream_never_had_delivers_nothing(solera):
    ex = await built(solera)
    keys = await solera.keys("copy")
    assert await ex.run("copy", keys={"items": ["k9"]}) == []
    assert await solera.keys("copy") == keys
    assert await solera.stale("copy") == []
    assert await ex.run("copy") == []
    await ex.feed({"k9": "1"})
    [batch] = await ex.run("copy")
    assert batch.classes == (["k9"], [], [])  # nothing was recorded of it


async def test_sel_2_a_named_key_upstream_removed_is_delivered_as_removed(solera):
    ex = await built(solera)
    await ex.feed(removes=["k1"])
    [batch] = await ex.run("copy", keys={"items": ["k1"]})
    assert (batch.classes, batch.rows) == (([], [], ["k1"]), [])
    assert await solera.rows("copy") == {"k2": "1", "k3": "1"}
    assert await solera.stale("copy") == []


# -- SEL-3 ----------------------------------------------------------------------------


@pytest.mark.parametrize("then", ["default", "keys"])
async def test_sel_3_what_a_keys_run_delivered_is_not_delivered_again(solera, then):
    """`copy` built from k1, k2, k3; k1 and k2 change. `keys=(k1)` delivers k1
    alone. Then a default run delivers k2 alone, never k1 again; or
    `keys=(k2)` delivers the rest: `copy` is fresh either way."""

    ex = await built(solera)
    await ex.feed({"k1": "2", "k2": "2"})
    [batch] = await ex.run("copy", keys={"items": ["k1"]})
    assert batch.classes == ([], ["k1"], [])
    [batch] = await ex.run("copy", keys=None if then == "default" else {"items": ["k2"]})
    assert batch.classes == ([], ["k2"], [])
    assert await solera.stale("copy") == []
    assert await ex.run("copy") == []
    assert await solera.rows("copy") == {"k1": "2", "k2": "2", "k3": "1"}


# -- SEL-4 ----------------------------------------------------------------------------


async def test_sel_4_a_keys_run_leaves_owed_what_it_did_not_process(solera):
    ex = await built(solera)
    await ex.feed({"k1": "2", "k2": "2"})
    await ex.run("copy", keys={"items": ["k1"]})
    await ex.run("copy", keys={"items": ["k3"]})
    assert await solera.stale("copy") != []
    await solera.run("checks", keys={"items": ["k1", "k3"]})
    assert set(await solera.stale_keys("checks")) == {"k2"}
    [batch] = await ex.run("copy")
    assert batch.classes == ([], ["k2"], [])
    await solera.run("checks")
    assert await solera.stale("copy") == [] and await solera.stale_keys("checks") == {}


async def test_sel_4_a_key_gone_upstream_stays_owed_through_a_keys_run_that_does_not_name_it(solera):
    """`feed` removes k2 and `items` follows. `keys=(k1, x1, k3)` on `checks`
    leaves k2, held but gone upstream, stale; a default run drops it."""

    ex = await built(solera)
    await ex.feed({"k1": "2"}, removes=["k2"])
    assert set(await solera.stale_keys("checks")) == {"k1", "k2"}
    ex.calls.clear()
    await solera.run("checks", keys={"items": ["k1", "x1", "k3"]})
    assert sorted(ex.calls["checks"]) == ["k1", "k3"]
    assert set(await solera.stale_keys("checks")) == {"k2"}
    assert set(await solera.rows("checks")) == {"k1", "k2", "k3"}
    await solera.run("checks")
    assert await solera.stale_keys("checks") == {} and await solera.stale("checks") == []
    assert set(await solera.rows("checks")) == {"k1", "k3"}


# -- SEL-5 ----------------------------------------------------------------------------


async def test_sel_5_named_keys_go_through_the_inputs_patterns(solera):
    """`copy` and `checks` now take `include=["k1"]`; `keys=(k1, k2)` loads k1
    only, and k2, held, is delivered as removed."""

    ex = await built(solera)
    await ex.deploy(include=["k1"])
    [batch] = await ex.run("copy", keys={"items": ["k1", "k2"]})
    assert (batch.classes, batch.unchanged, batch.rows) == (([], [], ["k2"]), ["k1"], ["k1"])
    assert set(await solera.rows("copy")) == {"k1", "k3"}  # k3 was not named
    ex.calls.clear()
    await solera.run("checks", keys={"items": ["k1", "k2"]})
    assert ex.calls["checks"] == ["k1"]
    assert set(await solera.rows("checks")) == {"k1", "k3"}


async def test_sel_5_a_keys_override_obeys_the_patterns(solera):
    seen = []

    def parse(ctx, file: dict):
        seen.append(ctx.key)
        return [{"n": file["n"]}]

    content = {"a/1": {"n": 1}, "excluded/2": {"n": 2}}
    await solera.deploy(files_project(solera.root, content, parse, include="a/**"))
    await solera.run("parse", upstream=True)
    content.update({"a/1": {"n": 3}, "excluded/2": {"n": 4}})  # both change
    await solera.run("files")
    seen.clear()
    await solera.run("parse", keys={"files": ["excluded/2", "a/1"]})
    assert seen == ["a/1"] and set(await samples(solera)) == {"a/1"}


# -- SEL-6 ----------------------------------------------------------------------------


async def test_sel_6_a_long_keys_list_goes_batch_size_keys_a_commit(solera):
    """Ten named keys, `batch_size=4`: three batches, three commits; the run
    ends with the ten processed."""

    ex = await Example(solera).start(batch_size=4)
    keys = [f"k{i:02}" for i in range(12)]
    await ex.feed(dict.fromkeys(keys, "1"))
    detail = await solera.run("copy", keys={"items": keys[:10]})
    assert [b.rows for b in ex.seen["copy"]] == [keys[0:4], keys[4:8], keys[8:10]]
    assert sum(1 for a in attempts_of(detail, "copy") if a.get("commit")) == 3
    assert sorted(await solera.rows("copy")) == keys[:10]


async def test_sel_6_a_keys_list_runs_batch_size_keys_at_a_time_concurrency_at_once(solera):
    """Five named keys in batches of two make three attempts, and
    `concurrency=1` runs one call at a time."""

    outside, live = Outside(), {"now": 0, "max": 0}

    @asset(
        inputs={"row": Incremental("feed", each=True, batch_size=2, concurrency=1)},
        outputs=Output("checks", key="id"),
    )
    async def checks(ctx, row: list):
        live["now"] += 1
        live["max"] = max(live["max"], live["now"])
        await asyncio.sleep(0.01)
        live["now"] -= 1
        return [{"v": row[0]["v"]}]

    await solera.deploy(
        Project(
            assets=[checks],
            sources=[Source("feed", key="id", store="ext")],
            stores={"ext": SourceStore(solera.root / "ext", outside)},
            default_store=FileStore(solera.root / "default"),
        )
    )
    outside.feed.update({f"k{i}": "1" for i in range(5)})
    await solera.commit_source("feed", upsert=dict(outside.feed))
    detail = await solera.run("checks", keys={"feed": list(outside.feed)})
    assert live["max"] == 1 and sum(len(a) for a in detail["attempts"].values()) == 3, live


# -- SEL-7 ----------------------------------------------------------------------------


async def test_sel_7_keys_all_loads_every_key_without_starting_over(solera):
    ex = await built(solera)
    await ex.feed({"k1": "2"})
    [b] = await ex.run("tally", keys={"items": "all"})
    assert not b.reset and b.updated == ["k1"] and b.unchanged == ["k2", "k3"] and not b.added
    assert b.rows == ["k1", "k2", "k3"] and b.held == {"rows": 3}  # owed in its class, the rest unchanged
    assert await ex.count() == 3 and await ex.stale() == []


# -- SEL-8 ----------------------------------------------------------------------------


async def test_sel_8_a_full_run_owes_every_key_and_a_plain_consumer_starts_over(solera):
    ex = await built(solera, batch_size=2)
    batches = await ex.run("tally", mode="full")
    assert [(b.index, b.reset, b.added) for b in batches] == [(0, True, ["k1", "k2"]), (1, False, ["k3"])]
    assert batches[1].held == {"rows": 2}  # batch by batch: the second builds on the first, from zero
    assert await ex.count() == 3


async def test_sel_8_keys_full_is_refused_a_full_run_is_the_runs_mode(solera):
    await built(solera)
    with pytest.raises(Refused) as refused:
        await solera.submit("tally", keys={"items": "full"})
    assert "mode='full'" in str(refused.value.detail)


async def test_sel_8_a_definition_change_makes_the_next_run_a_full_run(solera):
    ex = await built(solera, batch_size=2)
    await ex.deploy(batch_size=2, version="2")
    batches = await ex.run("tally")
    assert [(b.reset, b.added) for b in batches] == [(True, ["k1", "k2"]), (False, ["k3"])]
    assert await ex.count() == 3


async def test_sel_8_a_full_run_goes_on_batch_by_batch_across_attempts(solera):
    """Batch 1 of a full run fails once and is retried: the retry does not
    start over again, and builds on what batch 0 committed."""

    ex = await built(solera, batch_size=2)

    failed = []

    async def fail():
        ex.seen["tally"].pop()  # a failed batch delivered nothing
        failed.append(True)
        raise RuntimeError("batch 1 fails once")

    async def at_batch_0():
        ex.during = fail

    ex.during = at_batch_0
    run = await solera.submit("tally", mode="full")
    while not failed:
        await solera.settle(0.05)
    solera.advance(60)  # past the retry's delay
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert [a["outcome"] for a in attempts_of(detail, "tally")] == ["succeeded", "failed", "succeeded"]
    batches = ex.seen["tally"][-2:]
    assert [(b.reset, b.added, b.held) for b in batches] == [
        (True, ["k1", "k2"], {"rows": 3}),
        (False, ["k3"], {"rows": 2}),
    ]
    assert await ex.count() == 3


async def test_sel_8_a_full_runs_producer_starts_with_no_cursor(solera):
    cursors = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a", "v": 1}, {"id": "b", "v": 1}]

    @asset(inputs={"files": Incremental(batch_size=1)}, outputs=Output("log"))
    def log(ctx, files: list):
        cursors.append((ctx.batch["files"].reset, ctx.cursor))
        return Result(outputs={"log": {"n": len(files)}}, cursor=f"c{len(cursors)}")

    await solera.deploy(Project(assets=[files, log], default_store=FileStore(solera.root / "data")))
    await solera.run("log", upstream=True)
    assert cursors == [(False, None), (False, "c1")]
    await solera.run("log", mode="full")
    assert cursors[2:] == [(True, None), (False, "c3")]


# -- SEL-9 ----------------------------------------------------------------------------


async def test_sel_9_a_full_runs_first_batch_says_reset_and_load_returns_what_is_materialized(solera):
    ex = await built(solera, batch_size=2)
    first, second = await ex.run("tally", mode="full")
    assert first.reset and first.held == {"rows": 3}  # what is materialized, which it ignores
    assert not second.reset and second.held == {"rows": 2}  # what batch 0 committed


async def test_sel_9_a_per_key_producer_never_sees_reset(solera):
    resets = []

    def parse(ctx, file: dict):
        resets.extend(getattr(b, "reset", False) for b in ctx.batch.values())
        return [{"n": file["n"]}]

    await solera.deploy(files_project(solera.root, {"a": {"n": 1}, "b": {"n": 2}}, parse, batch_size=1))
    await solera.run("parse", upstream=True)
    await solera.run("parse", mode="full")
    assert not any(resets)
    assert {k: [r["n"] for r in v] for k, v in (await samples(solera)).items()} == {"a": [1], "b": [2]}


# -- SEL-10 ---------------------------------------------------------------------------


async def test_sel_10_a_per_key_full_run_calls_every_upstream_key_and_removes_what_upstream_lacks(solera):
    """`checks` holds k1, k2, x9; upstream now has k1, k2, k3. The full run
    calls k1, k2 and k3 and drops x9."""

    ex = await built(solera, keys=("k1", "k2", "x9"))
    await ex.feed({"k3": "1"}, removes=["x9"])
    ex.calls.clear()
    await solera.run("checks", mode="full")
    assert sorted(ex.calls["checks"]) == ["k1", "k2", "k3"]
    assert await solera.rows("checks") == {"k1": "1", "k2": "1", "k3": "1"}
    assert await solera.stale_keys("checks") == {} and await solera.stale("checks") == []


async def test_sel_10_a_full_run_keeps_a_failing_keys_last_good_output(solera):
    broken = {"b": False}

    def parse(ctx, file: dict):
        if broken.get(ctx.key):
            raise ValueError("broken now")
        return [{"n": file["n"]}]

    await solera.deploy(files_project(solera.root, {"a": {"n": 1}, "b": {"n": 2}}, parse))
    await solera.run("parse", upstream=True)
    broken["b"] = True
    detail = await solera.run("parse", mode="full")
    assert {t["asset"]: t["status"] for t in detail["tasks"]}["parse"] == "succeeded"
    assert {k: [r["n"] for r in v] for k, v in (await samples(solera)).items()} == {"a": [1], "b": [2]}


async def test_sel_10_a_per_key_full_run_canceled_midway_leaves_the_rest_owed_and_readable(solera):
    """`parse`'s version moves to 2: its next run is a full run. Canceled
    while b runs, a key a batch: every key's output stays readable, a and b
    at the new version, c at the old one, and c is still owed."""

    content, running, version, calls = {k: {"n": 1} for k in "abc"}, {}, {"v": "1"}, []

    def project():
        async def parse(ctx, file: dict):
            calls.append(ctx.key)
            if ctx.key == "b" and "run" in running:
                await solera.cancel(running.pop("run"))
            return [{"n": file["n"], "version": version["v"]}]

        return files_project(solera.root, content, parse, version["v"], batch_size=1, concurrency=1)

    await solera.deploy(project())
    await solera.run("parse", upstream=True)
    version["v"] = "2"
    await solera.deploy(project())
    assert await solera.stale("parse") != []
    run = running["run"] = (await solera.submit("parse"))["id"]
    assert (await solera.wait(run))["request"]["status"] == "canceled"
    got = {k: [r["version"] for r in v] for k, v in (await samples(solera)).items()}
    assert got == {"a": ["2"], "b": ["2"], "c": ["1"]}
    assert await solera.stale("parse") != [] or "c" in await solera.stale_keys("parse")
    calls.clear()
    await solera.run("parse")
    assert "c" in calls
    got = {k: [r["version"] for r in v] for k, v in (await samples(solera)).items()}
    assert got == {"a": ["2"], "b": ["2"], "c": ["2"]}
    assert await solera.stale("parse") == [] and await solera.stale_keys("parse") == {}


# -- target (PR 2) --


# -- SEL-11 (target) --------------------------------------------------------------------


async def test_sel_11_a_full_run_over_an_empty_upstream_reaches_its_producer(solera):
    """items is now empty: copy's full run calls it once, an empty batch,
    first and final; copy becomes empty and is complete at once."""

    ex = await built(solera)
    await ex.feed(removes=["k1", "k2", "k3"])
    [batch] = await ex.run("copy", mode="full")
    assert (batch.first, batch.final, batch.reset, batch.rows) == (True, True, True, [])
    assert await solera.rows("copy") == {} and (await solera.head("copy"))["materialized"]
    assert await solera.stale("copy") == []


async def test_sel_11_a_full_run_whose_patterns_take_nothing_reaches_its_producer(solera):
    """copy and checks now take `z*` alone. copy's full run is one empty
    batch and empties it; checks is called for no key, and loses every key
    it held."""

    ex = await built(solera)
    await ex.deploy(include=["z*"])
    [batch] = await ex.run("copy", mode="full")
    assert (batch.first, batch.final, batch.reset, batch.rows, batch.classes) == (
        True,
        True,
        True,
        [],
        ([], [], []),
    )
    assert await solera.rows("copy") == {} and (await solera.head("copy"))["materialized"]
    ex.calls.clear()
    await solera.run("checks", mode="full")
    assert ex.calls.get("checks", []) == [] and await solera.rows("checks") == {}
    assert await solera.stale("checks") == [] and await solera.stale("copy") == []


# -- SEL-12 (target) --------------------------------------------------------------------


async def test_sel_12_a_keys_run_may_be_a_full_runs_first_batch_and_a_default_run_finishes_it(solera):
    """copy's version is bumped; `keys=(k1)` is the full run's first batch:
    its commit clears copy, which holds k1 alone and owes k2 and k3. archive
    is held until a default run finishes copy, then gets one difference."""

    r = await Rebuild(solera).start(keys=("k1", "k2", "k3"))
    await r.deploy(version="2")
    await solera.run("copy", keys={"items": ["k1"]})
    [batch] = r.seen["copy"][-1:]
    assert (batch.reset, batch.rows) == (True, ["k1"])
    assert await solera.rows("copy") == {"k1": "1"} and not (await solera.head("copy"))["materialized"]
    assert await solera.stale("copy") != []
    reader = await solera.submit("archive")
    await solera.settle(0.5)
    task = await task_of(solera, reader, "archive")
    assert task["status"] == "queued" and task.get("held") == [INCOMPLETE, "copy"], task
    batches = len(r.seen["copy"])
    await solera.run("copy")
    assert [(b.reset, b.added) for b in r.seen["copy"][batches:]] == [(False, ["k2", "k3"])]
    assert (await solera.head("copy"))["materialized"] and await solera.stale("copy") == []
    assert (await solera.wait(reader))["request"]["status"] == "succeeded"
    assert r.seen["archive"][-1].classes == ([], ["k1", "k2", "k3"], [])
    assert await solera.rows("archive") == await solera.rows("copy") == {"k1": "1", "k2": "1", "k3": "1"}
