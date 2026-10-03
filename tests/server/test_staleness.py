"""Staleness at every level (K36–K38; tests/staleness.py states the rules):
the engine's stale keys, partition and asset statuses against a reference
model, over random histories; and the coordinator's worked example. Strict
xfails until W22 builds the rules (`staleness.LANDED`)."""

import asyncio

import pytest
from hypothesis import HealthCheck, Phase, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule
from solera.sdk import Each, Incremental, Output, Project, Source, asset
from solera.stores import FileStore
from solera_server.state import State

from tests import staleness
from tests.sim.oracle import index_entries, keyed_content
from tests.sim.project import External, SourceStore, rebuild

from .engines import drive, make_engine

KEYS = ["k1", "k2", "k3", "k4"]
pending = pytest.mark.xfail(
    not staleness.LANDED,
    strict=True,
    raises=(staleness.NotBuilt, AssertionError),
    reason="K36–K38: not built",
)


def project(root, outside: External, *, items_store="a", checks_store="a", checks_v="1", count_v="1"):
    """`feed` (a keyed source) -> `items` (a keyed copy, run by hand) ->
    `checks` (per key: an `Each`) and `count` (unkeyed). No automations:
    staleness stays until someone runs."""

    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id", store=items_store))
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(
        inputs={"item": Each("items", batch_size=2)},
        outputs=Output("checks", key="id", store=checks_store),
        version=checks_v,
    )
    async def checks(ctx, item: list):
        return [{"v": f"{item[0]['v']}.{checks_v}"}]

    @asset(inputs={"items": Incremental()}, outputs=Output("count"), version=count_v)
    def count(ctx, items: list):
        return {"rows": len(items)}

    return Project(
        assets=[items, checks, count],
        sources=[Source("feed", key="id", store="ext")],
        stores={
            "ext": SourceStore(root / "ext", outside),
            "a": FileStore(root / "a"),
            "b": FileStore(root / "b"),
        },
        default_store=FileStore(root / "default"),
    )


class Staleness(RuleBasedStateMachine):
    """Random histories of upstream changes, upstream resets, the per-key
    output's own resets, asset changes, `keys=` runs and default runs.
    After every step the engine says what the reference says."""

    def __init__(self):
        super().__init__()
        self.loop = asyncio.new_event_loop()
        self.ref = staleness.Reference()
        self.outside = External()
        self.decl = {"items_store": "a", "checks_store": "a", "checks_v": "1", "count_v": "1"}
        self.serial = 0

    def _run(self, coro):
        return self.loop.run_until_complete(coro)

    @initialize()
    def boot(self):
        import tempfile
        from pathlib import Path

        self.tmp = tempfile.TemporaryDirectory(prefix="staleness-")
        self.root = Path(self.tmp.name)
        self.state = self._run(State.open((self.root / "state").as_uri(), "test", flush_interval=0.001))
        self.engine = None
        self._deploy()
        self.commit({"k1", "k2"}, set())  # `items` has a head from here on

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

    @rule(upserts=st.sets(st.sampled_from(KEYS)), removes=st.sets(st.sampled_from(KEYS), max_size=2))
    def commit(self, upserts, removes):
        removes -= upserts
        self.serial += 1
        for k in upserts:
            self.outside.feed[k] = str(self.serial)
        for k in removes:
            self.outside.feed.pop(k, None)
        self._run(self.engine.commit_source("feed", upsert=sorted(upserts), remove=sorted(removes)))
        self._submit(["items"])
        self.ref.commit(upserts, {k for k in removes if k in self.ref.up.versions})

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
        self.ref.reset_per_key()

    @rule(which=st.sampled_from(["checks", "count"]))
    def change_asset(self, which):
        key = f"{which}_v"
        self.decl[key] = str(int(self.decl[key]) + 1)
        self._deploy()
        self.ref.change_asset("per_key" if which == "checks" else "unkeyed")

    @rule(keys=st.sets(st.sampled_from(KEYS), min_size=1))
    def run_keys(self, keys):
        self._submit(["checks"], keys={"items": {"keys": sorted(keys)}})
        self.ref.run_keys(keys)

    @rule(which=st.sampled_from(["checks", "count"]))
    def run_default(self, which):
        self._submit([which])
        self.ref.run_default("per_key" if which == "checks" else "unkeyed")

    # -- what the engine says ----------------------------------------------------------

    @invariant()
    def statuses_match_the_reference(self):
        if getattr(self, "engine", None) is None:
            return

        async def check():
            e, ref = self.engine, self.ref
            if ref.per_key.built:
                got = await staleness.stale_keys(e, "checks")
                assert got == ref.stale_keys(), f"checks' stale keys {got}, expected {ref.stale_keys()}"
            for name, want in (("checks", ref.per_key_stale()), ("count", ref.unkeyed_stale())):
                assert await staleness.partition_stale(e, name) == want, f"{name}: partition stale != {want}"
                assert await staleness.asset_stale(e, name) == want, f"{name}: asset stale != {want}"

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


@pending
def test_staleness_matches_the_reference_over_any_history():
    Staleness.TestCase().runTest()


# -- worked examples ------------------------------------------------------------------


async def _quiet_rounds(engine, n=20):
    for _ in range(n):
        await engine.tick()
        await asyncio.sleep(0.01)


@pending
async def test_keys_runs_after_an_upstream_reset_merge_and_together_catch_up(state, tmp_path):
    """The coordinator's example (R2, R4): `items` is reset; `checks` holds
    k1, k2, k3. keys=(k1, k2) updates those two, leaves k3 untouched and
    `checks` stale with stale keys {k3}; keys=(k3) then catches it up, and
    its next default run reads nothing."""

    outside = External()
    outside.feed.update(k1="1", k2="1", k3="1")
    engine = make_engine(state, project(tmp_path, outside))
    await engine.initialize()
    await engine.commit_source("feed", upsert=["k1", "k2", "k3"])
    await drive(engine, await engine.submit(["checks"], upstream=True))
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
    assert not await staleness.partition_stale(engine, "checks"), (
        "keys= runs covering every key catch up (R4)"
    )
    caught_up = state.model.partition("checks", "")
    assert caught_up.get("caught_up") and caught_up.get("caught_up_at")
    settled = await index_entries(state, "checks", "")
    await drive(engine, await engine.submit(["checks"]))
    assert await index_entries(state, "checks", "") == settled, "the next default run reads nothing new"


@pending
async def test_a_reset_output_holds_only_what_keys_runs_wrote_until_a_default_run(state, tmp_path):
    """R6: `checks` itself reset (moved) starts empty; keys=(k1) leaves k1
    alone in it, stale keys {k2, k3} (missing); a default run converges."""

    outside = External()
    outside.feed.update(k1="1", k2="1", k3="1")
    engine = make_engine(state, project(tmp_path, outside))
    await engine.initialize()
    await engine.commit_source("feed", upsert=["k1", "k2", "k3"])
    await drive(engine, await engine.submit(["checks"], upstream=True))
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


@pending
async def test_an_unkeyed_partition_stays_stale_until_it_reruns(state, tmp_path):
    """K38's calibration: `count` (unkeyed) built, then `items` changes:
    `count` is stale, through any number of ticks, until a run of it."""

    outside = External()
    outside.feed.update(k1="1")
    engine = make_engine(state, project(tmp_path, outside))
    await engine.initialize()
    await engine.commit_source("feed", upsert=["k1"])
    await drive(engine, await engine.submit(["count"], upstream=True))
    assert not await staleness.partition_stale(engine, "count")
    outside.feed["k2"] = "1"
    await engine.commit_source("feed", upsert=["k2"])
    await drive(engine, await engine.submit(["items"]))
    await _quiet_rounds(engine)
    assert await staleness.partition_stale(engine, "count"), "an upstream changed past its position"
    assert await staleness.asset_stale(engine, "count")
    await drive(engine, await engine.submit(["count"]))
    assert not await staleness.partition_stale(engine, "count")
    assert not await staleness.asset_stale(engine, "count")


def test_the_reference_reads_the_coordinators_example():
    """The reference itself, on the worked example: it is what the tests
    above hold the engine to."""

    ref = staleness.Reference()
    ref.commit({"k1", "k2", "k3"}, set())
    ref.run_default("per_key")
    ref.reset_upstream()
    assert ref.stale_keys() == {"k1", "k2", "k3"}
    ref.run_keys({"k1", "k2"})
    assert ref.stale_keys() == {"k3"} and ref.per_key.position is None  # R5
    ref.run_keys({"k3"})
    assert ref.stale_keys() == set() and ref.per_key.position == ref.up_changed  # R4
    ref.commit({"k4"}, set())
    assert ref.stale_keys() == {"k4"}  # missing counts
    ref.commit(set(), {"k1"})
    assert ref.stale_keys() == {"k1", "k4"}  # so does a key the upstream removed
    ref.change_asset("per_key")
    ref.run_keys({"k1", "k4"})
    assert ref.stale_keys() == {"k2", "k3"}  # written before the change
    ref.reset_per_key()
    assert not ref.per_key_stale()  # no head: missing, not stale
    ref.run_keys({"k2"})
    assert ref.stale_keys() == {"k3", "k4"}


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


def _no_reset_merge(kind):
    """Without a reset R2 already holds; after one, it waits for K36."""

    return [
        pytest.param(kind, False, id=f"{kind}"),
        pytest.param(kind, True, id=f"{kind}-after-reset", marks=pending),
    ]


@pytest.mark.parametrize("kind,reset", [p for k in STORES for p in _no_reset_merge(k)])
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


@pending
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
