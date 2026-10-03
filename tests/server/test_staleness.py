"""Staleness at every level (K43, K45, K46; tests/staleness.py states the rules):
the engine's stale keys, partition and asset statuses against a reference
model, over random histories; worked examples and calibrations; and R2 on
every built-in store. Strict xfails until W22 builds the rules
(`staleness.LANDED`)."""

import asyncio

import pytest
from hypothesis import HealthCheck, Phase, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, precondition, rule
from solera.sdk import Each, Incremental, Output, Project, Source, asset
from solera.stores import FileStore
from solera_server.state import State

from tests import staleness
from tests.sim.oracle import index_entries, keyed_content
from tests.sim.project import External, SourceStore, rebuild

from .engines import drive, make_engine

KEYS = ["k1", "k2", "k3", "x1"]  # `x*`: what `checks` and `copy` exclude


def pending(why: str):
    """A strict xfail for what the build (8666748, 3c4ade0) does not have yet."""

    return pytest.mark.xfail(
        strict=True, raises=(staleness.NotBuilt, AssertionError, pytest.fail.Exception), reason=why
    )


per_key = pending("each=True staleness from the one record, position + read-ahead (K47): W22's next step")
net_delta = pending("the net delta: over-reports until K44's range scan (W22)")
k44 = pending("K44: added/updated/removed and ctx.load(): not built")


def taken(key: str) -> bool:
    return not key.startswith("x")


def project(
    root,
    outside: External,
    *,
    items_store="a",
    checks_store="a",
    checks_v="1",
    copy_v="1",
    count_v="1",
):
    """`feed` (a keyed source) -> `items` (a keyed copy, run by hand) ->
    `checks` (each=True, excluding `x*`, with the unkeyed source `knob` a
    dep), `copy` (keyed, incremental, excluding `x*`) and `count`
    (unkeyed). No automations: staleness stays until someone runs."""

    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id", store=items_store))
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(
        inputs={"item": Each("items", batch_size=2, exclude=["x*"])},
        deps=["knob"],
        outputs=Output("checks", key="id", store=checks_store),
        version=checks_v,
    )
    async def checks(ctx, item: list):
        return [{"v": f"{item[0]['v']}.{checks_v}"}]

    @asset(
        inputs={"items": Incremental(exclude=["x*"])},
        outputs=Output("copy", key="id"),
        version=copy_v,
    )
    def copy(ctx, items: list):
        changes = ctx.batch["items"]
        outside.delivered |= {r["id"] for r in items} | set(changes.removed)  # what reached it
        outside.started_over |= changes.full and changes.first
        return rebuild(changes, [{"id": r["id"], "v": r["v"]} for r in items])

    @asset(inputs={"items": Incremental()}, outputs=Output("count"), version=count_v)
    def count(ctx, items: list):
        return {"rows": len(items)}

    @asset(inputs={"items": Incremental()}, outputs=Output("tally"))
    def tally(ctx, items: list):
        """K44's example: the count of `items`, kept from what each batch
        added and removed (a full pass's first batch starts over)."""

        changes = ctx.batch["items"]
        before = 0 if changes.full and changes.first else (ctx.load() or {"rows": 0})["rows"]
        return {"rows": before + len(changes.added) - len(changes.removed)}

    @asset(inputs={"row": Each("feed")}, outputs=Output("fchecks", key="id"))
    async def fchecks(ctx, row: list):
        return [{"v": row[0]["v"]}]

    outside.delivered = getattr(outside, "delivered", set())
    outside.started_over = getattr(outside, "started_over", False)
    return Project(
        assets=[items, checks, copy, count, tally, fchecks],
        sources=[Source("feed", key="id", store="ext"), Source("knob")],
        stores={
            "ext": SourceStore(root / "ext", outside),
            "a": FileStore(root / "a"),
            "b": FileStore(root / "b"),
        },
        default_store=FileStore(root / "default"),
    )


async def boot(engine, outside: External, keys: dict[str, str]):
    """`knob` at version 0, and `items` holding `keys`."""

    outside.feed.update(keys)
    await engine.commit_source("knob", version="0")
    await engine.commit_source("feed", upsert=dict(keys))  # versioned: a revert is no change
    await drive(engine, await engine.submit(["items"]))


class Staleness(RuleBasedStateMachine):
    """Random histories of feed commits (excluded keys too), runs of `items`
    (so its consumers are stale upstream between the two), its resets,
    shared-input changes, `checks`' own resets, asset changes, `keys=` runs
    and default runs. Every run of `copy` is delivered what the reference
    says, starting over when it says; after every step the engine's
    statuses, reasons and stale keys are the reference's."""

    def __init__(self):
        super().__init__()
        self.loop = asyncio.new_event_loop()
        self.ref = staleness.Reference(takes=taken)
        self.outside = External()
        self.decl = {"items_store": "a", "checks_store": "a", "checks_v": "1", "copy_v": "1", "count_v": "1"}
        self.serial = 0

    def _run(self, coro):
        return self.loop.run_until_complete(coro)

    @initialize()
    def start(self):
        import tempfile
        from pathlib import Path

        self.tmp = tempfile.TemporaryDirectory(prefix="staleness-")
        self.root = Path(self.tmp.name)
        self.state = self._run(State.open((self.root / "state").as_uri(), "test", flush_interval=0.001))
        self.engine = None
        self._deploy()
        self._run(boot(self.engine, self.outside, {"k1": "0", "k2": "0"}))
        self.ref.change_knob()
        self.ref.commit({"k1": "0", "k2": "0"}, set())

    def _deploy(self):
        async def go():
            if self.engine is not None:
                await self.engine.stop()
            self.project = project(self.root, self.outside, **self.decl)
            self.engine = make_engine(self.state, self.project)
            await self.engine.initialize()

        self._run(go())

    def _submit(self, assets, **kw):
        async def go():
            detail = await drive(self.engine, await self.engine.submit(assets, upstream=False, **kw))
            assert detail["request"]["status"] == "succeeded", detail["request"]

        self._run(go())

    # -- history ----------------------------------------------------------------------

    @rule(
        upserts=st.dictionaries(st.sampled_from(KEYS), st.sampled_from(["1", "2"])),
        removes=st.sets(st.sampled_from(KEYS), max_size=2),
    )
    def commit_feed(self, upserts, removes):
        """Two versions a key moves between: updates revert, removals come
        back, and the net delta decides what changed."""

        removes -= set(upserts)
        self.outside.feed.update(upserts)
        for k in removes:
            self.outside.feed.pop(k, None)
        self._run(self.engine.commit_source("feed", upsert=dict(upserts), remove=sorted(removes)))
        self.ref.commit_feed(dict(upserts), removes)

    @rule(keys=st.one_of(st.none(), st.sets(st.sampled_from(KEYS), min_size=1)))
    def run_fchecks(self, keys):
        self._submit(["fchecks"], keys=keys and {"feed": {"keys": sorted(keys)}})
        self.ref.run_fchecks(keys)

    @rule()
    def run_items(self):
        self._submit(["items"])
        self.ref.run_items()

    @rule()
    def change_knob(self):
        self.serial += 1
        self._run(self.engine.commit_source("knob", version=str(self.serial)))
        self.ref.change_knob()

    @rule()
    def reset_upstream(self):
        self.decl["items_store"] = "b" if self.decl["items_store"] == "a" else "a"
        self._deploy()
        self._submit(["items"])
        self.ref.reset_upstream()

    @rule()
    def reset_checks(self):
        self.decl["checks_store"] = "b" if self.decl["checks_store"] == "a" else "a"
        self._deploy()
        self.ref.reset_checks()

    @rule(name=st.sampled_from(["checks", "copy", "count"]))
    def change_asset(self, name):
        key = f"{name}_v"
        self.decl[key] = str(int(self.decl[key]) + 1)
        self._deploy()
        self.ref.change_asset(name)

    @rule(keys=st.sets(st.sampled_from(KEYS), min_size=1))
    def run_keys(self, keys):
        self._submit(["checks"], keys={"items": {"keys": sorted(keys)}})
        self.ref.run_keys(keys)

    @precondition(lambda self: self.ref.others["copy"].built)
    @rule(keys=st.sets(st.sampled_from(KEYS), min_size=1))
    def run_keys_on_copy(self, keys):
        """K45: copy (plain incremental) is delivered the named keys' changes
        its record lacks, and nothing else."""

        self._delivered_as(self.ref.run_keys(keys, "copy"), ["copy"], keys={"items": {"keys": sorted(keys)}})

    @rule(name=st.sampled_from(["checks", "copy", "count"]))
    def run_default(self, name):
        if name == "copy":
            self._delivered_as(self.ref.run_default("copy"), ["copy"])
        else:
            self._submit([name])
            self.ref.run_default(name)

    def _delivered_as(self, want, assets, **kw):
        """Run, and check what reached `copy`: nothing twice, a start-over
        only where a full pass begins."""

        self.outside.delivered.clear()
        self.outside.started_over = False
        self._submit(assets, **kw)
        delivered, start_over = want
        assert self.outside.delivered == delivered, (
            f"copy was delivered {self.outside.delivered}, not {delivered}"
        )
        assert self.outside.started_over == start_over, f"copy started over: {self.outside.started_over}"

    # -- what the engine says ----------------------------------------------------------

    @invariant()
    def statuses_match_the_reference(self):
        if getattr(self, "engine", None) is None:
            return

        async def check():
            e, ref = self.engine, self.ref
            if ref.checks.built:
                got = await staleness.stale_keys(e, "checks")
                assert got == ref.stale_keys(), f"checks' stale keys {got}, expected {ref.stale_keys()}"
            got = await staleness.stale_keys(e, "copy")
            assert got == ref.stale_keys_of("copy"), f"copy's stale keys {got}: all its keys or none"
            assert await staleness.stale_keys(e, "count") is None, "count has no keys"
            if ref.fchecks.built:
                got = await staleness.stale_keys(e, "fchecks")
                assert got == ref.fchecks_stale_keys(), (
                    f"fchecks' stale keys {got}, not {ref.fchecks_stale_keys()}"
                )
            for name in ("items", "fchecks", "checks", "copy", "count"):
                want = ref.stale(name)
                assert await staleness.partition_stale(e, name) == want, f"{name}: partition stale != {want}"
                assert await staleness.asset_stale(e, name) == want, f"{name}: asset stale != {want}"
                why = await staleness.stale_reasons(e, name)
                assert why == ref.reasons(name), f"{name}: stale for {why}, not {ref.reasons(name)}"

        self._run(check())

    def teardown(self):
        async def close():
            if self.engine is not None:
                await self.engine.stop()
            await self.state.close()

        if getattr(self, "state", None) is not None:
            self._run(close())
            self.tmp.cleanup()
        self.loop.close()


Staleness.TestCase.settings = settings(
    max_examples=25,
    stateful_step_count=12,
    deadline=None,
    suppress_health_check=list(HealthCheck),
    phases=list(Phase) if staleness.LANDED else [Phase.explicit, Phase.generate],
)


@per_key
def test_staleness_matches_the_reference_over_any_history():
    Staleness.TestCase().runTest()


# -- worked examples and calibrations ---------------------------------------------------


async def _quiet_rounds(engine, n=20):
    for _ in range(n):
        await engine.tick()
        await asyncio.sleep(0.01)


async def _built(state, tmp_path, keys, **decl):
    outside = External()
    engine = make_engine(state, project(tmp_path, outside, **decl))
    await engine.initialize()
    await boot(engine, outside, keys)
    await drive(engine, await engine.submit(["checks", "copy", "count"]))
    return engine, outside


@per_key
async def test_keys_runs_after_an_upstream_reset_merge_and_together_catch_up(state, tmp_path):
    """The coordinator's example (R2, R4): `items` is reset; `checks` holds
    k1, k2, k3. keys=(k1, k2) updates those two, leaves k3 untouched and
    `checks` stale with stale keys {k3}; keys=(k3) then leaves it fresh, and
    its next default run writes nothing."""

    engine, outside = await _built(state, tmp_path, {"k1": "1", "k2": "1", "k3": "1"})
    before = await index_entries(state, "checks", "")
    await engine.stop()

    engine = make_engine(state, project(tmp_path, outside, items_store="b"))  # items reset
    await engine.initialize()
    await drive(engine, await engine.submit(["items"]))
    await drive(engine, await engine.submit(["checks"], keys={"items": {"keys": ["k1", "k2"]}}))
    after = await index_entries(state, "checks", "")
    assert set(after) == {"k1", "k2", "k3"}, "a keys= run after a reset is not a reset write (R2)"
    assert after["k3"] == before["k3"], "k3, not named, is untouched (R2)"
    assert after["k1"][0] > before["k1"][0] and after["k2"][0] > before["k2"][0]
    assert await staleness.stale_keys(engine, "checks") == {"k3"}
    assert await staleness.partition_stale(engine, "checks") and await staleness.asset_stale(engine, "checks")

    await drive(engine, await engine.submit(["checks"], keys={"items": {"keys": ["k3"]}}))
    assert await staleness.stale_keys(engine, "checks") == set()
    assert not await staleness.partition_stale(engine, "checks"), "keys= runs covering every stale key"
    settled = await index_entries(state, "checks", "")
    await drive(engine, await engine.submit(["checks"]))
    assert await index_entries(state, "checks", "") == settled, "the next default run writes nothing"


@per_key
async def test_a_reset_output_holds_only_what_keys_runs_wrote_until_a_default_run(state, tmp_path):
    """R6: `checks` itself reset (moved) starts empty; keys=(k1) leaves k1
    alone in it, stale keys {k2, k3} (missing); a default run converges."""

    engine, outside = await _built(state, tmp_path, {"k1": "1", "k2": "1", "k3": "1"})
    await engine.stop()
    p = project(tmp_path, outside, checks_store="b")
    engine = make_engine(state, p)
    await engine.initialize()
    await drive(engine, await engine.submit(["checks"], keys={"items": {"keys": ["k1"]}}))
    assert set(await keyed_content(engine, p, "checks", column=None)) == {"k1"}
    assert await staleness.stale_keys(engine, "checks") == {"k2", "k3"}
    await drive(engine, await engine.submit(["checks"]))
    assert set(await keyed_content(engine, p, "checks", column=None)) == {"k1", "k2", "k3"}
    assert not await staleness.partition_stale(engine, "checks")


async def test_an_unkeyed_partition_stays_stale_until_it_reruns(state, tmp_path):
    """K38's calibration: `count` (unkeyed) built, then `items` changes:
    `count` is stale, through any number of ticks, until a run of it."""

    engine, outside = await _built(state, tmp_path, {"k1": "1"})
    assert not await staleness.partition_stale(engine, "count")
    outside.feed["k2"] = "1"
    await engine.commit_source("feed", upsert=["k2"])
    await drive(engine, await engine.submit(["items"]))
    await _quiet_rounds(engine)
    assert await staleness.partition_stale(engine, "count"), "an upstream changed since its catch-up"
    assert await staleness.asset_stale(engine, "count")
    await drive(engine, await engine.submit(["count"]))
    assert not await staleness.partition_stale(engine, "count")
    assert not await staleness.asset_stale(engine, "count")


@per_key
async def test_a_commit_of_excluded_keys_alone_leaves_their_consumers_fresh(state, tmp_path):
    """K39's calibration: `items` commits only `x1`, which `checks` and `copy`
    exclude: neither is stale (the plain "position behind" rule says both
    are). Then a commit of `k1`: both are, and `count`, which takes every
    key, was already."""

    engine, outside = await _built(state, tmp_path, {"k1": "1", "k2": "1"})
    outside.feed["x1"] = "1"
    await engine.commit_source("feed", upsert=["x1"])
    await drive(engine, await engine.submit(["items"]))
    assert await staleness.stale_keys(engine, "checks") == set()
    assert not await staleness.partition_stale(engine, "checks"), "only an excluded key changed"
    assert not await staleness.partition_stale(engine, "copy"), "only an excluded key changed"
    assert await staleness.partition_stale(engine, "count")
    outside.feed["k1"] = "2"
    await engine.commit_source("feed", upsert=["k1"])
    await drive(engine, await engine.submit(["items"]))
    assert await staleness.stale_keys(engine, "checks") == {"k1"}
    assert await staleness.partition_stale(engine, "copy")


@per_key
async def test_a_shared_input_change_makes_every_key_stale(state, tmp_path):
    """`knob`, a dep every key of `checks` shares, changes: every key is
    stale, though no upstream key changed; keys= runs covering them all
    leave `checks` fresh."""

    engine, outside = await _built(state, tmp_path, {"k1": "1", "k2": "1"})
    await engine.commit_source("knob", version="1")
    assert await staleness.stale_keys(engine, "checks") == {"k1", "k2"}
    assert await staleness.partition_stale(engine, "checks") and await staleness.asset_stale(engine, "checks")
    await drive(engine, await engine.submit(["checks"], keys={"items": {"keys": ["k1", "k2"]}}))
    assert not await staleness.partition_stale(engine, "checks")


async def _change(engine, outside, upserts=(), removes=()):
    outside.feed.update(dict.fromkeys(upserts, "2"))
    for k in removes:
        outside.feed.pop(k, None)
    await engine.commit_source("feed", upsert=sorted(upserts), remove=sorted(removes))
    await drive(engine, await engine.submit(["items"]))


@pytest.mark.parametrize("then", ["default", "keys"])
async def test_a_keys_run_on_an_incremental_asset_delivers_each_change_once(state, tmp_path, then):
    """K45: `copy` (plain incremental) built from k1, k2, k3; k1 and k2
    change. keys=(k1) delivers k1 alone, and `copy` stays stale (k2). Then
    a default run delivers k2 alone, never k1 again; or keys=(k2) covers the
    rest, and the partition's record collapses: `copy` is fresh either way."""

    engine, outside = await _built(state, tmp_path, {"k1": "1", "k2": "1", "k3": "1"})
    await _change(engine, outside, upserts=["k1", "k2"])
    outside.delivered.clear()
    await drive(engine, await engine.submit(["copy"], keys={"items": {"keys": ["k1"]}}))
    assert outside.delivered == {"k1"}
    assert await staleness.partition_stale(engine, "copy"), "k2 is not read yet"
    outside.delivered.clear()
    keys = None if then == "default" else {"items": {"keys": ["k2"]}}
    await drive(engine, await engine.submit(["copy"], keys=keys))
    assert outside.delivered == {"k2"}, "k1 was delivered twice"
    assert not await staleness.partition_stale(engine, "copy")
    outside.delivered.clear()
    await drive(engine, await engine.submit(["copy"]))
    assert outside.delivered == set(), "the next default run has nothing new"


async def test_keys_runs_past_the_read_ahead_cap_are_refused_until_a_default_run(state, tmp_path):
    """K45's bound, at a cap of 2: a plain incremental partition takes two
    keys= runs, of any number of keys; a third is refused ("run the
    partition first") and nothing is submitted; a default run collapses the
    record, and keys= runs are taken again."""

    outside = External()
    engine = staleness.engine_with_read_ahead_cap(state, project(tmp_path, outside), cap=2)
    await engine.initialize()
    await boot(engine, outside, {"k1": "1"})
    await drive(engine, await engine.submit(["checks", "copy", "count"]))
    many = {"items": {"keys": [f"k{i:05d}" for i in range(20_000)]}}  # no cap on a run's keys
    for _ in range(2):
        await drive(engine, await engine.submit(["copy"], keys=many))
    runs = len(state.model.runs)
    with pytest.raises(ValueError, match="run the partition first"):
        await engine.submit(["copy"], keys={"items": {"keys": ["k1"]}})
    assert len(state.model.runs) == runs
    await drive(engine, await engine.submit(["copy"]))
    await drive(engine, await engine.submit(["copy"], keys={"items": {"keys": ["k1"]}}))


@per_key
async def test_keys_runs_on_an_each_asset_count_toward_the_cap_too(state, tmp_path):
    """K47: an each=True asset keeps the same record, so its keys= runs are
    read-ahead entries, and the cap (2 here) counts the ones that leave
    something uncovered: k1, k2, k3 change; keys=(k1), keys=(k2) leave k3,
    and a third keys= run is refused until a default run collapses the
    record."""

    outside = External()
    engine = staleness.engine_with_read_ahead_cap(state, project(tmp_path, outside), cap=2)
    await engine.initialize()
    await boot(engine, outside, {"k1": "1", "k2": "1", "k3": "1"})
    await drive(engine, await engine.submit(["checks"]))
    await _change(engine, outside, upserts=["k1", "k2", "k3"])
    for key in ("k1", "k2"):
        await drive(engine, await engine.submit(["checks"], keys={"items": {"keys": [key]}}))
    with pytest.raises(ValueError, match="run the partition first"):
        await engine.submit(["checks"], keys={"items": {"keys": ["k3"]}})
    await drive(engine, await engine.submit(["checks"]))
    await drive(engine, await engine.submit(["checks"], keys={"items": {"keys": ["k1"]}}))


@k44
async def test_a_count_kept_from_its_batches_stays_exact_through_a_keys_run(state, tmp_path):
    """K44's example through a keys= run (K45): `tally` = what it held +
    added - removed. k4 added and k1 removed; keys=(k4) delivers k4 as added;
    the next default run delivers k1's removal alone, never k4 again: the
    tally equals `items`' count after each run."""

    engine, outside = await _built(state, tmp_path, {"k1": "1", "k2": "1", "k3": "1"})
    p = project(tmp_path, outside)

    async def tally(keys=None):
        from tests.sim.oracle import value_content

        detail = await drive(engine, await engine.submit(["tally"], keys=keys))
        assert detail["request"]["status"] == "succeeded", detail["request"]
        return (await value_content(engine, p, "tally"))["rows"]

    assert await tally() == 3
    await _change(engine, outside, upserts=["k4"], removes=["k1"])
    assert await tally({"items": {"keys": ["k4"]}}) == 4  # k1's removal not read yet
    assert await tally() == 3 == len(outside.feed)


async def test_a_non_each_keyed_outputs_keys_go_stale_together(state, tmp_path):
    """K43: every key of `copy` depends on the whole of `items` under its
    patterns, so a change of `k1` alone makes `k1`, `k2` and `k3` stale; a
    default run clears them all."""

    engine, outside = await _built(state, tmp_path, {"k1": "1", "k2": "1", "k3": "1"})
    assert await staleness.stale_keys(engine, "copy") == set()
    outside.feed["k1"] = "2"
    await engine.commit_source("feed", upsert=["k1"])
    await drive(engine, await engine.submit(["items"]))
    assert await staleness.stale_keys(engine, "copy") == {"k1", "k2", "k3"}
    assert await staleness.partition_stale(engine, "copy") and await staleness.asset_stale(engine, "copy")
    await drive(engine, await engine.submit(["copy"]))
    assert await staleness.stale_keys(engine, "copy") == set()
    assert not await staleness.partition_stale(engine, "copy")


@pytest.mark.parametrize("then", ["keys", "default"])
async def test_a_full_pass_after_an_asset_change_may_take_several_runs(state, tmp_path, then):
    """The full pass (Erwin's correction to K45): `copy` holds k1, k2, k3;
    its version is bumped. keys=(k1) starts the pass over with k1: `copy`
    holds k1 alone, stale ("definition changed"). Then keys=(k2, k3)
    continues it, or a default run delivers k2 and k3 and finishes it,
    without starting over: either way `copy` holds all three, fresh."""

    engine, outside = await _built(state, tmp_path, {"k1": "1", "k2": "1", "k3": "1"})
    await engine.stop()
    p = project(tmp_path, outside, copy_v="2")
    engine = make_engine(state, p)
    await engine.initialize()

    async def run(keys=None):
        outside.delivered.clear()
        outside.started_over = False
        await drive(engine, await engine.submit(["copy"], keys=keys and {"items": {"keys": keys}}))
        return outside.delivered, outside.started_over

    assert await run(["k1"]) == ({"k1"}, True)
    assert set(await keyed_content(engine, p, "copy", column=None)) == {"k1"}
    assert await staleness.stale_reasons(engine, "copy") == {staleness.DEFINITION}
    assert await run(["k2", "k3"] if then == "keys" else None) == ({"k2", "k3"}, False)
    assert set(await keyed_content(engine, p, "copy", column=None)) == {"k1", "k2", "k3"}
    assert not await staleness.partition_stale(engine, "copy")
    assert await run() == (set(), False)


@per_key
async def test_staleness_is_transitive_down_a_chain(state, tmp_path):
    """K46, three levels: `feed` commits k2 and `items` has not rerun.
    `items` is stale ("input changed"); `copy`, `count` and every key of
    `checks` are stale ("upstream stale"), though `items` has not moved.
    Once `items` runs, they are stale for their own input instead."""

    engine, outside = await _built(state, tmp_path, {"k1": "1", "k2": "1"})
    outside.feed["k2"] = "2"
    await engine.commit_source("feed", upsert=["k2"])
    assert await staleness.stale_reasons(engine, "items") == {staleness.INPUT}
    for name in ("checks", "copy", "count"):
        assert await staleness.stale_reasons(engine, name) == {staleness.UPSTREAM}, name
        assert await staleness.asset_stale(engine, name), name
    assert await staleness.stale_keys(engine, "checks") == {"k1", "k2"}
    await drive(engine, await engine.submit(["items"]))
    assert not await staleness.partition_stale(engine, "items")
    for name in ("checks", "copy", "count"):
        assert await staleness.stale_reasons(engine, name) == {staleness.INPUT}, name
    assert await staleness.stale_keys(engine, "checks") == {"k2"}


async def test_a_stale_status_carries_every_reason_that_holds(state, tmp_path):
    """K46: `copy`'s version is bumped and `feed` commits, `items` not yet
    rerun: `copy` is stale for its definition and its upstream at once."""

    engine, outside = await _built(state, tmp_path, {"k1": "1"})
    await engine.stop()
    engine = make_engine(state, project(tmp_path, outside, copy_v="2"))
    await engine.initialize()
    outside.feed["k1"] = "2"
    await engine.commit_source("feed", upsert=["k1"])
    assert await staleness.stale_reasons(engine, "copy") == {staleness.DEFINITION, staleness.UPSTREAM}


@net_delta
async def test_a_key_added_and_removed_past_the_read_changes_nothing(state, tmp_path):
    """The net delta, through `items`: k4 is added and removed again past
    what `copy`, `count` and `checks` read. None of them is stale, and a
    default run of `copy` delivers nothing."""

    engine, outside = await _built(state, tmp_path, {"k1": "1", "k2": "1"})
    for change in ({"upsert": {"k4": "1"}}, {"remove": ["k4"]}):
        outside.feed.update(change.get("upsert", {}))
        outside.feed.pop("k4", None) if "remove" in change else None
        await engine.commit_source("feed", **change)
        await drive(engine, await engine.submit(["items"]))
    for name in ("checks", "copy", "count"):
        assert not await staleness.partition_stale(engine, name), name
    assert await staleness.stale_keys(engine, "checks") == set()
    outside.delivered.clear()
    await drive(engine, await engine.submit(["copy"]))
    assert outside.delivered == set()


@net_delta
async def test_a_key_updated_and_reverted_changes_nothing(state, tmp_path):
    """The net delta, at a versioned source: `feed`'s k1 goes from version 1
    to 2 and back to 1 before anyone reads it. Neither `items` (plain
    incremental) nor `fchecks` (each=True) is stale, and `items`' next run
    writes nothing."""

    engine, outside = await _built(state, tmp_path, {"k1": "1", "k2": "1"})
    await drive(engine, await engine.submit(["fchecks"]))
    for version in ("2", "1"):
        outside.feed["k1"] = version
        await engine.commit_source("feed", upsert={"k1": version})
    assert not await staleness.partition_stale(engine, "items")
    assert not await staleness.partition_stale(engine, "fchecks")
    assert await staleness.stale_keys(engine, "fchecks") == set()
    before = await index_entries(state, "items", "")
    await drive(engine, await engine.submit(["items"]))
    assert await index_entries(state, "items", "") == before


async def test_a_reset_upstream_and_a_new_version_give_both_reasons(state, tmp_path):
    """The ruling: `items` reset (its content replaced) and then `count`'s
    version bumped: `count` is stale for both, ["input changed",
    "definition changed"]; one full pass under the new version, reading the
    new `items`, clears both."""

    engine, outside = await _built(state, tmp_path, {"k1": "1"})
    await engine.stop()
    engine = make_engine(state, project(tmp_path, outside, items_store="b"))  # items reset
    await engine.initialize()
    await drive(engine, await engine.submit(["items"]))
    await engine.stop()
    engine = make_engine(state, project(tmp_path, outside, items_store="b", count_v="2"))
    await engine.initialize()
    assert await staleness.stale_reasons(engine, "count") == {staleness.INPUT, staleness.DEFINITION}
    await drive(engine, await engine.submit(["count"]))
    assert await staleness.stale_reasons(engine, "count") == set()


def test_the_reference_reads_the_worked_examples():
    """The reference itself on the examples above: what the tests hold the
    engine to."""

    DEF, IN, UP = staleness.DEFINITION, staleness.INPUT, staleness.UPSTREAM
    ref = staleness.Reference(takes=taken)
    ref.change_knob()
    ref.commit({"k1", "k2", "k3"}, set())
    for name in ("checks", "copy", "count"):
        ref.run_default(name)
    ref.reset_upstream()
    assert ref.stale_keys() == {"k1", "k2", "k3"}
    ref.run_keys({"k1", "k2"})
    assert ref.stale_keys() == {"k3"} and ref.stale("checks")
    ref.run_keys({"k3"})
    assert ref.stale_keys() == set() and not ref.stale("checks")
    ref.commit({"k4"}, set())
    assert ref.stale_keys() == {"k4"}  # missing counts
    ref.commit(set(), {"k1"})
    assert ref.stale_keys() == {"k1", "k4"}  # so does a key the upstream removed
    ref.change_asset("checks")
    ref.run_keys({"k1", "k4"})
    assert ref.stale_keys() == {"k2", "k3"} and ref.reasons("checks") == {DEF}  # written before the change
    ref.reset_checks()
    assert not ref.stale("checks")  # no head: missing, not stale
    ref.run_keys({"k2"})
    assert ref.stale_keys() == {"k3", "k4"}

    ref = staleness.Reference(takes=taken)  # K39
    ref.change_knob()
    ref.commit({"k1"}, set())
    for name in ("checks", "copy", "count"):
        ref.run_default(name)
    ref.commit({"x1"}, set())
    assert not ref.stale("checks") and not ref.stale("copy") and ref.stale("count")
    ref.commit({"k1"}, set())
    assert ref.stale("checks") and ref.stale("copy")

    ref.run_default("checks")  # a shared input
    ref.change_knob()
    assert ref.stale_keys() == {"k1"} and ref.reasons("checks") == {IN}
    assert ref.stale_keys_of("copy") == {"k1"}  # copy holds k1 alone: k1 changed
    assert ref.run_default("copy") == ({"k1"}, False)
    assert not ref.stale("copy") and ref.stale_keys_of("copy") == set()

    ref.commit({"k2", "k3"}, set())  # K45: a keys= run, then the rest
    assert ref.run_keys({"k2"}, "copy") == ({"k2"}, False) and ref.stale("copy")
    assert ref.run_default("copy") == ({"k3"}, False)  # never k2 again
    ref.commit({"k2", "k3"}, set())
    assert ref.run_keys({"k2", "x1"}, "copy") == ({"k2"}, False)
    assert len(ref.others["copy"].entries) == 1  # k3 still uncovered
    assert ref.run_keys({"k3"}, "copy") == ({"k3"}, False) and not ref.stale("copy")  # covered
    assert ref.others["copy"].entries == []  # nothing left uncovered: collapsed
    assert ref.run_default("copy") == (set(), False)
    ref.commit({"k2"}, set())  # an entry covers a key only up to the commit it read at
    assert ref.run_keys({"k2"}, "copy")[0] == {"k2"}
    ref.commit({"k2"}, set())
    assert ref.pending("copy") == {"k2"} and ref.run_default("copy") == ({"k2"}, False)

    ref.change_asset("copy")  # a full pass over several runs: the coordinator's example
    assert ref.reasons("copy") == {DEF}
    assert ref.run_keys({"k1"}, "copy") == ({"k1"}, True) and ref.others["copy"].keys == {"k1"}
    assert ref.run_keys({"k2", "k3"}, "copy") == ({"k2", "k3"}, False) and not ref.stale("copy")
    ref.change_asset("copy")
    ref.run_keys({"k1"}, "copy")
    assert ref.run_default("copy") == ({"k2", "k3"}, False) and not ref.stale("copy")
    ref.reset_upstream()  # a reset owes a pass too, for the input
    assert ref.reasons("copy") == {IN} and ref.run_default("copy") == ({"k1", "k2", "k3"}, True)

    ref.run_default("count")
    ref.commit_feed({"k1"}, set())  # K46: transitive, with reasons
    assert ref.reasons("items") == {IN} and ref.reasons("copy") == {UP} and ref.reasons("count") == {UP}
    assert ref.stale_keys() == {"k1", "k2", "k3"}  # every key of checks, upstream stale
    ref.change_asset("copy")
    assert ref.reasons("copy") == {DEF, UP}
    ref.run_items()  # items moved after copy's pass began: its input changed too
    assert ref.reasons("copy") == {DEF, IN} and ref.reasons("count") == {IN}

    net = staleness.Reference(takes=taken)  # the net delta
    net.change_knob()
    net.commit({"k1": "1", "k2": "1"}, set())
    for name in ("checks", "copy", "count"):
        net.run_default(name)
    net.run_fchecks()
    net.commit({"k4": "1"}, set())
    net.commit(set(), {"k4"})  # added and removed past the reads
    assert not any(net.stale(n) for n in ("checks", "copy", "count"))
    assert net.run_default("copy") == (set(), False)
    net.commit_feed({"k1": "2"}, set())
    net.commit_feed({"k1": "1"}, set())  # updated and reverted
    assert not net.stale("items") and not net.stale("fchecks")
    net.commit_feed({"k1": "2"}, set())
    assert net.reasons("items") == {IN} and net.fchecks_stale_keys() == {"k1"}

    both = staleness.Reference(takes=taken)  # the ruling: every reason that holds
    both.commit({"k1": "1"}, set())
    both.run_default("count")
    both.reset_upstream()
    both.change_asset("count")
    assert both.reasons("count") == {IN, DEF}
    both.run_default("count")  # one pass under the new version, reading the new upstream
    assert both.reasons("count") == set()
    both.change_asset("count")  # due only to the asset change
    assert both.reasons("count") == {DEF}

    capped = staleness.Reference(takes=taken, cap=2)  # the cap counts keys= runs that leave something
    capped.change_knob()
    capped.commit({"k1": "1", "k2": "1", "k3": "1"}, set())
    for name in ("checks", "copy"):
        capped.run_default(name)
    capped.commit({"k1": "2", "k2": "2", "k3": "2"}, set())
    assert capped.run_keys({"k1"}, "copy") == ({"k1"}, False)
    assert capped.run_keys({"k2"}, "copy") == ({"k2"}, False)
    assert capped.run_keys({"k3"}, "copy") == "refused"
    capped.run_default("copy")
    assert capped.run_keys({"k1"}, "copy") == (set(), False)
    capped.run_keys({"k1"})  # K47: each=True keeps the same record, capped alike
    capped.run_keys({"k2"})
    assert capped.run_keys({"k3"}) == "refused"
    capped.run_default("checks")
    assert capped.run_keys({"k1"}) is None


# -- the keyed merge on every built-in store (R2) ----------------------------------------


def _store(kind: str, root):
    """A built-in store of `kind`, and how to drop what it made."""

    import os
    import uuid

    if kind == "file":
        return FileStore(root / "x"), {}, lambda: None
    if kind == "postgres":
        dsn = os.environ.get("SOLERA_TEST_DATABASE_URL")
        if not dsn:
            pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
        import psycopg
        from solera_postgres import PostgresStore

        schema = f"staleness_{uuid.uuid4().hex[:10]}"

        def drop():
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

        return PostgresStore(dsn), {"schema": schema, "columns": {"id": "text", "v": "text"}}, drop
    from tests.sdk.test_store_conformance import s3_harness

    return s3_harness(root).store, {}, lambda: None


def merge_project(root, outside, store, decl, *, items_store="a"):
    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id", store=items_store))
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(
        inputs={"item": Each("items", batch_size=2)}, outputs=Output("checks", key="id", store="x", **decl)
    )
    async def checks(ctx, item: list):
        return [{"v": item[0]["v"]}]

    return Project(
        assets=[items, checks],
        sources=[Source("feed", key="id", store="ext")],
        stores={
            "ext": SourceStore(root / "ext", outside),
            "a": FileStore(root / "a"),
            "b": FileStore(root / "b"),
            "x": store,
        },
        default_store=FileStore(root / "default"),
    )


MERGE_KEYS = [f"k{i}" for i in range(6)]
merges = st.fixed_dictionaries(
    {
        "start": st.sets(st.sampled_from(MERGE_KEYS), min_size=1),
        "upserts": st.sets(st.sampled_from(MERGE_KEYS)),
        "removes": st.sets(st.sampled_from(MERGE_KEYS)),
        "named": st.sets(st.sampled_from(MERGE_KEYS), min_size=1),
    }
)


async def _keys_run(kind, tmp_path, reset, case):
    """`checks` built from `start`; the feed changes; `items` maybe reset;
    a keys= run of `named`. Returns `checks`' index and content before
    and after the keys= run, and the feed."""

    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory(dir=tmp_path) as d:
        root = Path(d)
        store, decl, drop = _store(kind, root)
        outside = External()
        state = await State.open((root / "state").as_uri(), "test", flush_interval=0.001)
        engine = None
        try:
            outside.feed.update(dict.fromkeys(case["start"], "1"))
            p = merge_project(root, outside, store, decl)
            engine = make_engine(state, p)
            await engine.initialize()
            await engine.commit_source("feed", upsert=sorted(case["start"]))
            await drive(engine, await engine.submit(["checks"], upstream=True))
            removes = case["removes"] - case["upserts"]
            outside.feed.update(dict.fromkeys(case["upserts"], "2"))
            for k in removes:
                outside.feed.pop(k, None)
            await engine.commit_source("feed", upsert=sorted(case["upserts"]), remove=sorted(removes))
            if reset:
                await engine.stop()
                p = merge_project(root, outside, store, decl, items_store="b")
                engine = make_engine(state, p)
                await engine.initialize()
            await drive(engine, await engine.submit(["items"]))
            before = await index_entries(state, "checks", ""), await keyed_content(engine, p, "checks")
            run = await engine.submit(["checks"], keys={"items": {"keys": sorted(case["named"])}})
            assert (await drive(engine, run))["request"]["status"] == "succeeded"
            after = await index_entries(state, "checks", ""), await keyed_content(engine, p, "checks")
            return before, after, dict(outside.feed)
        finally:
            if engine is not None:
                await engine.stop()
            await state.close()
            drop()


STORES = ["file", "postgres", "s3"]
# Each example runs an engine: few of them, and while the rules are not built, no
# shrinking of the failure the strict xfail expects.
MERGE_SETTINGS = settings(
    max_examples=6,
    deadline=None,
    suppress_health_check=list(HealthCheck),
    phases=list(Phase) if staleness.LANDED else [Phase.explicit, Phase.generate],
)


@pytest.mark.parametrize("reset", [False, True], ids=["", "after-reset"])
@pytest.mark.parametrize("kind", STORES)
def test_a_keys_run_never_touches_a_key_it_does_not_name(kind, reset, tmp_path):
    """R2, on every built-in store: a keys= run's write is a merge. Every
    key it does not name keeps its entry (generation and payload) and its
    value, and no key appears or goes but those named."""

    from hypothesis import example, given

    @MERGE_SETTINGS
    @given(case=merges)
    @example(case={"start": {"k0", "k1"}, "upserts": set(), "removes": set(), "named": {"k0"}})
    def check(case):
        (index0, content0), (index1, content1), _ = asyncio.run(_keys_run(kind, tmp_path, reset, case))
        for k in set(index0) | set(index1):
            if k in case["named"]:
                continue
            assert index1.get(k) == index0.get(k), f"{k}, not named: entry {index0.get(k)} -> {index1.get(k)}"
            assert content1.get(k) == content0.get(k), (
                f"{k}, not named: {content0.get(k)} -> {content1.get(k)}"
            )

    check()


@pytest.mark.parametrize("kind", STORES)
def test_a_keys_run_makes_each_named_key_match_its_upstream(kind, tmp_path):
    """R2's other half: each named key the upstream has is written at its
    upstream value; each it no longer has is removed; one neither has stays
    absent."""

    from hypothesis import example, given

    @MERGE_SETTINGS
    @given(case=merges, reset=st.booleans())
    @example(case={"start": {"k0"}, "upserts": set(), "removes": {"k0"}, "named": {"k0"}}, reset=False)
    def check(case, reset):
        _, (_, content), feed = asyncio.run(_keys_run(kind, tmp_path, reset, case))
        for k in case["named"]:
            assert content.get(k) == feed.get(k), f"{k}, named: {content.get(k)}, its upstream {feed.get(k)}"

    check()
