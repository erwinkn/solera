"""The read-ahead of a plain incremental input (K45, docs/positions-from-reads.md):
a keys= run is delivered the named keys' changes past its position's
snapshot, as of one upstream commit, and the position records
`[commit, run, attempt]`; the next pass skips a key one of them read at or
after its last change, so nothing is delivered twice. Once nothing past
the snapshot is left, the record collapses to a new snapshot. A full pass
due (an asset change, a reset) runs the same way across keys= and default
runs: the first delivery starts it over, the rest continue it."""

import pytest
from solera.sdk import Incremental, Output, Project, Retention, Source, asset
from solera.stores import FileStore

from tests.sim.project import External, SourceStore, rebuild

from .engines import drive, make_engine


def project(root, outside, seen, retention=None, version="1"):
    @asset(
        inputs={"feed": Incremental()},
        outputs=Output("copy", key="id"),
        retention=retention,
        version=version,
    )
    def copy(ctx, feed: list):
        seen.append((sorted(ctx.batch["feed"].upserted), sorted(ctx.batch["feed"].removed)))
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    return Project(
        assets=[copy],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(root / "ext", outside)},
        default_store=FileStore(root / "out"),
    )


async def built(state, tmp_path, retention=None, **kw):
    outside, seen = External(), []
    engine = make_engine(state, project(tmp_path, outside, seen, retention), **kw)
    await engine.initialize()

    async def commit(**changes):
        for k, v in changes.items():
            if v is None:
                outside.feed.pop(k, None)
            else:
                outside.feed[k] = v
        up = sorted(k for k, v in changes.items() if v is not None)
        await engine.commit_source("feed", upsert=up, remove=sorted(set(changes) - set(up)))

    await commit(k1="1", k2="1", k3="1")
    await drive(engine, await engine.submit(["copy"]))
    seen.clear()
    return engine, commit, seen


def ahead(engine):
    return (engine.m.position("copy", "feed", "") or {}).get("ahead", [])


async def test_keys_read_ahead_are_not_delivered_again(state, tmp_path):
    """k1 and k2 change; keys=(k1) reads k1 and is recorded; the next pass
    delivers k2 alone, and the read-ahead collapses."""

    engine, commit, seen = await built(state, tmp_path)
    await commit(k1="2", k2="2")
    await drive(engine, await engine.submit(["copy"], keys={"feed": {"keys": ["k1"]}}))
    assert seen == [(["k1"], [])] and len(ahead(engine)) == 1
    await drive(engine, await engine.submit(["copy"]))
    assert seen[1:] == [(["k2"], [])], "k1 was read ahead at its last change"
    assert ahead(engine) == [] and engine.m.position("copy", "feed", "")["next"] == 2


async def test_a_key_changed_after_its_keys_run_is_delivered(state, tmp_path):
    """keys=(k1) reads k1; k1 changes again: the next pass delivers it."""

    engine, commit, seen = await built(state, tmp_path)
    await commit(k1="2")
    await drive(engine, await engine.submit(["copy"], keys={"feed": {"keys": ["k1"]}}))
    await commit(k1="3")
    await drive(engine, await engine.submit(["copy"]))
    assert seen == [(["k1"], []), (["k1"], [])]


async def test_a_keys_run_removes_a_named_key_the_upstream_has_not(state, tmp_path):
    """R2: a named key the upstream removed is removed. It was all that
    changed, so the record collapses at once, and the next pass has nothing."""

    engine, commit, seen = await built(state, tmp_path)
    await commit(k3=None)
    await drive(engine, await engine.submit(["copy"], keys={"feed": {"keys": ["k3"]}}))
    assert seen == [([], ["k3"])] and ahead(engine) == []
    assert engine.m.position("copy", "feed", "")["next"] == 2  # the new snapshot
    await drive(engine, await engine.submit(["copy"]))
    assert seen[1:] == []


async def test_a_keys_run_is_delivered_only_changes_past_the_snapshot(state, tmp_path):
    """K45: keys=(k1, k2) when only k1 changed delivers k1, not the
    unchanged k2; a second keys=(k1) is delivered nothing."""

    engine, commit, seen = await built(state, tmp_path)
    await commit(k1="2", k3="2")
    for _ in range(2):
        await drive(engine, await engine.submit(["copy"], keys={"feed": {"keys": ["k1", "k2"]}}))
    assert [s for s in seen if s != ([], [])] == [(["k1"], [])]
    assert len(ahead(engine)) == 2  # k3 is left: no collapse


async def test_the_read_ahead_is_capped_in_runs(state, tmp_path):
    """The cap counts keys= runs since the partition last ran, not keys:
    past it a keys= run is refused until a default run collapses it."""

    engine, commit, _ = await built(state, tmp_path, read_ahead_cap=2)
    await commit(k1="2", k2="2", k3="2")
    for keys in (["k1", "k2", "x1", "x2"], ["k1"]):  # one run may name any number of keys; k3 is left
        await drive(engine, await engine.submit(["copy"], keys={"feed": {"keys": keys}}))
    assert len(ahead(engine)) == 2
    with pytest.raises(ValueError, match="run the partition first"):
        await engine.submit(["copy"], keys={"feed": {"keys": ["k2"]}})
    await drive(engine, await engine.submit(["copy"]))
    await drive(engine, await engine.submit(["copy"], keys={"feed": {"keys": ["k2"]}}))


async def test_retention_keeps_a_run_a_read_ahead_names(state, tmp_path):
    """The keys of an entry are in its attempt's spec: retention keeps the
    run until the entry collapses, however its policy reads."""

    engine, commit, _ = await built(state, tmp_path, Retention(runs=1), retention_interval=0)
    await commit(k1="2", k2="2", k3="2")
    first = await engine.submit(["copy"], keys={"feed": {"keys": ["k1"]}})
    await drive(engine, first)
    await drive(engine, await engine.submit(["copy"], keys={"feed": {"keys": ["k2"]}}))
    await engine.upkeep.tick()  # `first` is past the newest run, but read ahead
    assert await state.list_objects(f"runs/{first['id']}/")
    await drive(engine, await engine.submit(["copy"]))  # collapses the read-ahead
    assert ahead(engine) == []
    await engine.upkeep.tick()
    assert not await state.list_objects(f"runs/{first['id']}/")


@pytest.mark.parametrize("then", ["keys", "default"])
async def test_a_full_pass_due_runs_across_keys_runs(state, tmp_path, then):
    """Erwin's correction: `copy` holds k1, k2, k3; its version is bumped.
    keys=(k1) starts the full pass over with k1 alone. Then keys=(k2, k3)
    continues it and `copy` is fresh, or a default run delivers k2 and k3
    and finishes it; either way nothing is delivered twice, nor dropped."""

    outside, seen = External(), []
    engine = make_engine(state, project(tmp_path, outside, seen))
    await engine.initialize()
    outside.feed.update(k1="1", k2="1", k3="1")
    await engine.commit_source("feed", upsert=["k1", "k2", "k3"])
    await drive(engine, await engine.submit(["copy"]))
    await engine.stop()
    engine = make_engine(state, project(tmp_path, outside, seen, version="2"))
    await engine.initialize()
    seen.clear()
    await drive(engine, await engine.submit(["copy"], keys={"feed": {"keys": ["k1"]}}))
    assert seen == [(["k1"], [])]
    assert sorted((await engine.list_keys("copy"))["keys"]) == ["k1"], "the first delivery starts over"
    if then == "keys":
        await drive(engine, await engine.submit(["copy"], keys={"feed": {"keys": ["k2", "k3"]}}))
    else:
        await drive(engine, await engine.submit(["copy"]))
    assert seen[1:] == [(["k2", "k3"], [])], "the pass continues with what it has not delivered"
    assert sorted((await engine.list_keys("copy"))["keys"]) == ["k1", "k2", "k3"]
    position = engine.m.position("copy", "feed", "")
    assert "pass" not in position and "ahead" not in position, "the record collapsed to a snapshot"
    assert engine.m.partition("copy", "")["caught_up"] is True


async def test_a_key_read_ahead_then_removed_upstream_is_behind(state, tmp_path):
    """Positions.tla, point 1: k4 is added past the snapshot and read ahead
    by keys=(k4); then the upstream removes it. Absent at both ends, the net
    delta past the snapshot omits it, but its read-ahead entry read it: it is
    behind, and the next pass delivers its removal."""

    engine, commit, seen = await built(state, tmp_path)
    await commit(k4="1", k1="2")
    await drive(engine, await engine.submit(["copy"], keys={"feed": {"keys": ["k4"]}}))
    assert seen == [(["k4"], [])] and len(ahead(engine)) == 1
    await commit(k4=None)
    assert await engine.stale_reasons("copy", "") == ["input changed"]
    await drive(engine, await engine.submit(["copy"]))
    assert seen[1:] == [(["k1"], ["k4"])]
    assert "k4" not in (await engine.list_keys("copy"))["keys"]


async def test_the_latest_read_of_a_key_wins(state, tmp_path):
    """Positions.tla, point 2: keys=(k1) reads k1, the upstream removes it,
    keys=(k1) reads its removal. The latest read wins (a removal is no
    version, not the lowest): the next pass delivers k2 alone."""

    engine, commit, seen = await built(state, tmp_path)
    await commit(k1="2", k2="2")
    await drive(engine, await engine.submit(["copy"], keys={"feed": {"keys": ["k1"]}}))
    await commit(k1=None)
    await drive(engine, await engine.submit(["copy"], keys={"feed": {"keys": ["k1"]}}))
    assert seen == [(["k1"], []), ([], ["k1"])]
    await drive(engine, await engine.submit(["copy"]))
    assert seen[2:] == [(["k2"], [])]
    assert await engine.stale_reasons("copy", "") == []
