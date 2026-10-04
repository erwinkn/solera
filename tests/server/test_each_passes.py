"""A per-key asset's passes and its retry passes share its runs (per-key
processing §9): they alternate, and a run never ends with either left half
way (F31)."""

from solera.sdk import Incremental, Output, Project, Retry, Source, asset
from solera.stores import FileStore

from tests.sim.project import External, SourceStore

from .engines import drive, make_engine


async def test_a_run_finishes_the_pass_it_began_before_it_ends(state, tmp_path):
    """F31, from W38's trace: `checks` (Each over `feed`, batches of 2) has a
    failed key. feed commits k1, k2, k3, and a forced retry run of checks
    goes: its batches alternate between the delta pass and the retry pass.
    A retry batch that finds nothing used to end the run with the delta
    pass at batch 1 of 2, so k3 never reached checks. The run delivers
    every key before it ends."""

    outside, broken = External(), {"bad"}

    @asset(
        inputs={"item": Incremental("feed", batch_size=2, each=True)},
        outputs=Output("checks", key="id"),
        retries=Retry(0),
    )
    async def checks(ctx, item: list):
        if ctx.key in broken:
            raise RuntimeError("down")
        return [{"v": item[0]["v"]}]

    project = Project(
        assets=[checks],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(tmp_path / "ext", outside)},
        default_store=FileStore(tmp_path / "out"),
    )
    engine = make_engine(state, project)
    await engine.initialize()
    outside.feed.update(bad="1", a="1")
    await engine.commit_source("feed", upsert=["a", "bad"])
    await drive(engine, await engine.submit(["checks"]))
    assert sorted((await engine.list_keys("checks"))["keys"]) == ["a"]  # `bad` failed

    broken.clear()
    outside.feed.update(k1="1", k2="1", k3="1")
    await engine.commit_source("feed", upsert=["k1", "k2", "k3"])
    found = engine.retry_keys("checks", ["failed"], by="test")
    [run] = await engine.submit_retries("checks", found["partitions"], "test")
    detail = await drive(engine, run)
    assert detail["request"]["status"] == "succeeded"
    assert sorted((await engine.list_keys("checks"))["keys"]) == ["a", "bad", "k1", "k2", "k3"]
    assert engine.m.partition("checks", "")["caught_up"] is True
    assert "pass" not in engine.m.position("checks", "item", "")


async def test_a_retry_pass_that_leaves_nothing_uncovered_collapses_the_record():
    """K47: a retry pass's last batch says whether every key changed past the
    snapshot has been delivered — read ahead at or after its change, or read
    by this batch (a removal by removing it) — so the record collapses as
    after a default run; anything else left, it does not."""

    from dataclasses import replace

    from obstore.store import MemoryStore
    from solera import keys as K
    from solera.keys.index import FileInfo, IndexState
    from solera.keys.io import ObjectIO
    from solera.patterns import Matcher
    from solera_worker.each import _retry_covers

    io, state = ObjectIO(MemoryStore()), IndexState(prefix="idx/")
    for c, (keys, generation, deleted) in enumerate(
        [([b"a", b"b"], 10, b"\0\0"), ([b"c"], 11, b"\x01")], start=1
    ):
        data = K.encode_file(keys, [generation] * len(keys), deleted)
        await io.write(f"idx/{c:012d}-log.kx", data)
        state = replace(state, log=state.log + ((c, (FileInfo.describe(f"{c:012d}-log", 0, data),)),))
    cover = {"from": 1, "to": 2, "index": state.to_json(), "ahead": {"a": 10}}
    taken = Matcher(None)
    assert await _retry_covers(cover, taken, {"b": 10}, {"c"}, io)  # a ahead, b retried, c removed
    assert not await _retry_covers(cover, taken, {"b": 10}, set(), io)  # c's removal not delivered
    assert not await _retry_covers(cover, taken, {"b": 9}, {"c"}, io)  # b read before its change
    assert await _retry_covers(
        cover, Matcher({"exclude": [["exclude[0]", {"glob": "b"}]], "include": []}), {}, {"c"}, io
    )  # b not taken
