"""The observed set's histories (docs/observed-set.md, "Each past finding is
a decode mismatch"): A19, A26 and A27's, each step followed by `verify()` —
the engine's decode of every observation record equals the literal observed
set, as the index can know it — and its count and staleness at the end."""

import pytest

from .histories import World


async def world(tmp_path, **deploy) -> World:
    return await World().start(tmp_path, **deploy)


# -- A19 -------------------------------------------------------------------------------


@pytest.mark.parametrize("change", ["update", "add", "remove"])
async def test_a19_r1_r2_a_change_during_a_run_is_counted_once(tmp_path, change):
    """keys=(k1) first; the upstream then changes; two default runs. Every
    change arrives once: decode follows what each batch gave."""

    w = await world(tmp_path)
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.run(keys={"items": {"keys": ["k1"]}})
        await w.verify()
        await w.feed(
            **{"update": {"upserts": {"k1": "2"}}, "add": {"upserts": {"k3": "1"}}}.get(
                change, {"removes": ["k1"]}
            )
        )
        await w.run()
        await w.verify()
        await w.run()
        await w.verify()
        assert await w.count() == len(w.outside.feed) and await w.stale() == []
    finally:
        await w.close()


async def test_a19_r3_a_delivered_removal_is_not_forgotten(tmp_path):
    w = await world(tmp_path)
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.run(keys={"items": {"keys": ["k1"]}})
        await w.feed(removes=["k1"])
        await w.run(keys={"items": {"keys": ["k2"]}})
        await w.verify()
        assert await w.stale() == ["input changed"]  # k1's removal is owed
        await w.run()
        await w.verify()
        assert await w.count() == 1 and await w.stale() == []
    finally:
        await w.close()


async def test_a19_r4_a_selection_under_new_patterns_is_counted_once(tmp_path):
    w = await world(tmp_path, include=["k1"])
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.run()
        await w.deploy(include=["k*"])
        await w.run(keys={"items": {"keys": ["k2"]}})
        await w.verify()
        await w.run()
        await w.verify()
        assert await w.count() == 2 and await w.stale() == []
    finally:
        await w.close()


async def test_a19_r5_an_early_removal_from_a_current_only_source_is_delivered_once(tmp_path):
    """A batch a key: k1 and k2 updated; during k1's batch the source removes
    k2 and commits it. k2's batch is served no row: classed from that
    observation before the producer (a removal), and nothing again later."""

    w = await world(tmp_path, batch_size=1)
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.run()
        await w.feed({"k1": "2", "k2": "2"})

        async def remove_k2():
            w.outside.feed.pop("k2")
            await w.engine.commit_source("feed", remove=["k2"])

        w.during = remove_k2
        await w.run()
        await w.verify()
        await w.run("items")
        await w.run()
        await w.verify()
        assert await w.count() == 1 and await w.stale() == []
    finally:
        await w.close()


async def test_a19_r6_keys_runs_go_batch_size_keys_a_commit(tmp_path):
    """keys= of many keys pages by batch_size: a commit each, every one
    verified; the run ends with the named keys observed."""

    w = await world(tmp_path, batch_size=4)
    try:
        await w.feed({f"k{i:02d}": "1" for i in range(20)})
        named = [f"k{i:02d}" for i in range(0, 20, 2)]
        detail = await w.run(keys={"items": {"keys": named}})
        assert len([t for t in detail["attempts"].values() for a in t]) >= 3  # 10 keys, 4 a batch
        await w.verify()
        assert set(w.observed.of("tally", "", "items")) == set(named)
    finally:
        await w.close()


async def test_a19_r7_a_shared_input_move_owes_every_key_its_context(tmp_path):
    """A27 R1 too: `factor` moves w1 -> w2; keys=(k1) processes k1 under w2;
    k2, under w1, is still owed; `factor` back to w1 reverses which."""

    w = await world(tmp_path)
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.run()
        await w.factor("w2")
        assert await w.stale() == ["input changed"]
        await w.run(keys={"items": {"keys": ["k1"]}})
        await w.verify()
        assert w.observed.of("tally", "", "items")["k1"][1] == {"factor": "w2"}
        assert await w.stale() == ["input changed"]  # k2 under w1
        await w.factor("w1")
        assert await w.stale() == ["input changed"]  # k1 now
        await w.run()
        await w.verify()
        assert await w.stale() == []
    finally:
        await w.close()


async def test_a19_r8_a_failed_key_is_observed_and_retried_by_its_failure_record(tmp_path):
    w = await world(tmp_path)
    try:
        w.outside.flaky["k2"] = "down"
        await w.feed({"k1": "1", "k2": "1"})
        await w.run("checks")
        await w.verify()
        w.outside.flaky.clear()
        await w.run("checks")  # its failure record retries it
        await w.verify()
    finally:
        await w.close()


async def test_a19_r9_excluding_every_held_key_owes_their_removal(tmp_path):
    w = await world(tmp_path)
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.run()
        await w.deploy(include=["z*"])
        assert await w.stale() == ["input changed"]
        await w.run()
        await w.verify()
        assert await w.count() == 0 and await w.stale() == []
    finally:
        await w.close()


async def test_a19_r10_an_excluded_stale_upstream_key_does_not_propagate(tmp_path):
    """`filtered` takes k1 of `checks`: k2 changing upstream leaves it fresh."""

    w = await world(tmp_path)
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.run("checks")
        await w.run("filtered")
        await w.feed({"k2": "2"})
        await w.verify()
        assert await w.stale("filtered") == []
        await w.feed({"k1": "2"})
        assert await w.stale("filtered") != []  # through a stale k1 of checks
    finally:
        await w.close()


# -- A26 -------------------------------------------------------------------------------


async def test_a26_n1_a_run_reads_rows_after_the_upstream_moved_on(tmp_path):
    """No pass holds a snapshot: each batch reads at its own head, its rows
    pinned by its claim, so an upstream change between batches never leaves
    a batch naming rows that are gone."""

    w = await world(tmp_path, batch_size=1)
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.run(keys={"items": {"keys": ["k1"]}})
        await w.feed({"k2": "2"})
        await w.run()
        await w.verify()
        assert await w.count() == 2 and await w.stale() == []
    finally:
        await w.close()


@pytest.mark.parametrize("history", ["repeated", "absent key", "partial"])
async def test_a26_n2_selections_around_a_pattern_change(tmp_path, history):
    w = await world(tmp_path, include=["k1"] if history != "partial" else ["k1", "k2"])
    try:
        await w.feed({"k1": "1", "k2": "1", "k3": "1"})
        if history == "partial":
            await w.run(keys={"items": {"keys": ["k1"]}})
        else:
            await w.run()
        await w.deploy(include=["k*"])
        named = {"repeated": ["k2"], "absent key": ["k9"], "partial": ["k2"]}[history]
        for _ in range(2 if history == "repeated" else 1):
            await w.run(keys={"items": {"keys": named}})
            await w.verify()
        await w.run()
        await w.verify()
        assert await w.count() == 3 and await w.stale() == []
    finally:
        await w.close()


async def test_a26_n3_a_selection_before_an_old_pattern_delta_is_kept(tmp_path):
    w = await world(tmp_path, include=["k1"])
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.run()
        await w.feed({"k1": "2"})
        await w.deploy(include=["k*"])
        await w.run(keys={"items": {"keys": ["k2"]}})
        await w.verify()
        await w.run()
        await w.verify()
        assert await w.count() == 2 and await w.stale() == []
    finally:
        await w.close()


async def test_a26_n4_a_key_served_absent_then_back_is_owed_its_add(tmp_path):
    """The store has no row for k2 when `items` reads it (it went without a
    commit): observed absent. It comes back at the same version: decoded
    absent, present now — an add, not 'fresh'."""

    w = await world(tmp_path)
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.feed({"k1": "2", "k2": "2"}, items=False)
        w.outside.feed.pop("k2")  # gone from the store, no commit says so
        await w.run("items")
        await w.verify()
        w.outside.feed["k2"] = "2"  # back, at the version its commit named
        assert await w.stale("items") == ["input changed"]
        await w.run("items")
        await w.verify()
        assert await w.stale("items") == []
    finally:
        await w.close()


async def test_a26_n4_a_row_served_ahead_of_its_commit_is_a_point(tmp_path):
    """A27 R2 too: the batch names k2@2; the store serves @3, saying so. The
    class follows @3, a point records it; restored to @2, it is owed."""

    w = await world(tmp_path)
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.feed({"k2": "2"}, items=False)
        w.outside.feed["k2"] = "3"  # ahead of every commit
        await w.run("items")
        await w.verify()
        assert w.observed.of("items", "", "feed")["k2"][0] == "3"
        w.outside.feed["k2"] = "2"
        await w.engine.commit_source("feed", upsert={"k2": "2"})
        assert await w.stale("items") == ["input changed"]
        await w.run("items")
        await w.verify()
    finally:
        await w.close()


# -- A27 -------------------------------------------------------------------------------


async def test_a27_r3_a_first_include_owes_the_removal_of_what_it_leaves_out(tmp_path):
    w = await world(tmp_path)
    try:
        await w.feed({"keep/a": "1", "drop/b": "1"})
        await w.run()
        await w.deploy(include=["keep/*"])
        assert await w.stale() == ["input changed"]
        await w.run()
        await w.verify()
        assert await w.count() == 1
    finally:
        await w.close()


async def test_a27_r4_an_add_during_a_narrowing_is_owed_nothing(tmp_path):
    w = await world(tmp_path, include=["keep/*", "drop/*"])
    try:
        await w.feed({"keep/a": "1"})
        await w.run()
        await w.deploy(include=["keep/*"])
        await w.feed({"drop/b": "1"})
        await w.run()
        await w.verify()
        assert await w.count() == 1 and await w.stale() == []
    finally:
        await w.close()


async def test_a27_r7_an_upstream_reset_owes_a_full_run(tmp_path):
    """`items` moves to another store: a new life, its old index gone.
    `tally`'s observations name the old life: it owes a full run, not a
    decode it cannot make; after it, decode is the new life's."""

    w = await world(tmp_path)
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.run()
        await w.deploy(items_store="other")
        await w.run("items")
        assert await w.stale() == ["input changed"]
        await w.run()
        await w.verify()
        assert await w.count() == 2 and await w.stale() == []
    finally:
        await w.close()


async def test_a27_r8_an_older_selection_over_a_newer_range_stays_a_point(tmp_path):
    w = await world(tmp_path)
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.run(keys={"items": {"keys": ["k1"]}})  # k1@1, a point
        await w.feed({"k1": "2"})
        await w.run()  # a range over k1@2
        await w.verify()
        await w.feed({"k1": "1"})
        await w.run(keys={"items": {"keys": ["k1"]}})  # back at @1: it must not fold into the base
        await w.verify()
        assert await w.stale() == []
    finally:
        await w.close()


async def test_a27_r9_a_removal_restored_at_its_version_is_owed_its_add(tmp_path):
    w = await world(tmp_path)
    try:
        await w.feed({"k1": "1", "k2": "1"})
        await w.run()
        await w.feed(removes=["k1"])
        await w.run(keys={"items": {"keys": ["k1"]}})  # k1 removed: observed absent
        await w.verify()
        await w.feed({"k1": "1"})  # restored at its old version, other changes netting out
        assert await w.stale() == ["input changed"]
        await w.run()
        await w.verify()
        assert await w.count() == 2
    finally:
        await w.close()


async def test_a27_r10_widening_to_a_key_that_never_existed_changes_no_debt(tmp_path):
    w = await world(tmp_path, include=["k1"])
    try:
        await w.feed({"k1": "1"})
        await w.run()
        await w.deploy(include=["k1", "k9"])
        assert await w.stale() == []
        await w.verify()
    finally:
        await w.close()
