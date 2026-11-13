"""A dead writer on a store that writes in place (D196): the engine keeps
its version, and the output's next attempt asks the store which keys hold
it, deletes those the log lacks, and the engine forgets the version once
that attempt commits. Postgres itself is exercised by the contract's STO-5
and ENG-8, which CI does not run; here the store is one of files that says
it writes in place and answers `written_at` as a table would."""

from dataclasses import replace

import pytest
from solera.sdk import Incremental, Output, Project, Retry, asset
from solera.stores import FileStore, KeyedWrite, Patch

from tests.contract.harness import Solera
from tests.contract.test_sto import Faults, items_on, killed_after_writing, rows, served


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path)
    yield namespace
    await namespace.close()


class Table(Faults, FileStore):
    """Files that claim a table's contract: `left` is what a dead writer's
    version holds, `asked` the versions the engine asked about, `removed`
    the keys a repair deleted."""

    writes = "fenced"

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.left, self.asked, self.removed = [], [], []
        self.holds = {}  # key -> the version a read says it found it at, whatever was planned
        self.value_at = None  # the version a read of a single value says it found

    async def read(self, ref, t, selection):
        found = await super().read(ref, t, selection)
        if found.versions is None:
            return found if self.value_at is None else replace(found, version=self.value_at)
        return replace(found, versions={**found.versions, **self.holds}) if self.holds else found

    async def written_at(self, versions, prior, context):
        self.asked.append((list(versions), context.generation))
        return [sorted(self.left)]

    def keys(self, ref, among=None):
        yield []

    async def store(self, write, prior, context):
        if isinstance(write, KeyedWrite) and write.removes and not len(write.prepared.rows):
            self.removed += sorted(write.removes)
            return await FileStore.store(self, write, prior, context)
        return await super().store(write, prior, context)


async def test_the_next_attempt_deletes_what_a_dead_writer_left_and_the_log_lacks(solera):
    """`items` holds a and b. An attempt writes c and a, and its worker is
    killed before the commit. The retry is told the dead version, asks the
    store for its keys (a and c) and deletes c, which the log lacks; a,
    which the log has, is the producer's to write again."""

    table = Table(solera.root / "table")
    writes = [rows("a=1", "b=1"), Patch(rows("c=1", "a=2")), Patch(rows("a=2"))]
    await served(solera, items_on(table, writes, retries=Retry(1, delay=0)))
    await solera.run("items")
    table.hang_after_write()
    table.left = ["a", "c"]
    run = await solera.submit("items")
    await killed_after_writing(solera, table)
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    dead, retry = table.versions[1], table.versions[2]
    assert table.asked == [([dead], retry)]
    assert table.removed == ["c"]
    assert solera.engine.m.repairs == {}
    assert sorted(await solera.keys("items")) == ["a", "b"]


async def test_a_failure_before_any_write_leaves_nothing_to_repair(solera):
    """An attempt that fails in the asset's own code never began writing:
    the engine keeps no version, and the next attempt asks the store nothing."""

    table = Table(solera.root / "table")
    project = items_on(table, [rows("a=1")], retries=Retry(0))
    fail = [True]
    produce = project.assets["items"].fn

    def items():
        if fail[0]:
            raise RuntimeError("no rows today")
        return produce()

    project.assets["items"].fn = items
    await solera.deploy(project)
    await solera.run("items", expect="failed")
    assert solera.engine.m.repairs == {}
    fail[0] = False
    await solera.run("items")
    assert table.asked == [] and sorted(await solera.keys("items")) == ["a"]


async def test_a_version_nothing_explains_is_held_back_and_raises_an_alarm(solera):
    """`copy` reads `items` from a table that says it holds k2 at version
    999: no commit of `items` and no live attempt wrote that. `copy`'s batch
    processes k1 and holds k2 back; an alarm names `items` and k2; k2 stays
    owed, and is processed once the table holds what the log lists."""

    table, received = Table(solera.root / "table"), []
    await solera.deploy(items_on(table, [rows("k1=1", "k2=1")], received))
    await solera.run("items")
    table.holds = {"k2": 999}
    await solera.run("copy")
    assert [sorted(seen) for _, seen in received] == [["k1"]]
    assert await solera.alarmed("items", "k2", "999")
    assert await solera.stale("copy") != []
    table.holds = {}
    await solera.run("copy")
    assert [sorted(seen) for _, seen in received] == [["k1"], ["k2"]]
    assert await solera.stale("copy") == []


async def test_an_asset_never_builds_on_a_write_to_its_own_output_no_commit_recorded(solera):
    """`tally` keeps a count in a table and adds to it through `ctx.load()`.
    The table says it holds a version `tally` never committed (a writer
    died after writing): the attempt that reads it stops before the asset
    adds to it, and `tally` runs in full instead, from nothing."""

    table, seen, feed = Table(solera.root / "table"), [], ["k1", "k2"]

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": key} for key in feed]

    @asset(inputs={"items": Incremental()}, outputs=Output("tally", store="s"))
    async def tally(ctx, items: list):
        batch, held = ctx.batch["items"], await ctx.load()
        before = 0 if batch.reset or held is None else held["n"]
        seen.append((batch.reset, sorted(batch.added), before))
        return {"n": before + len(batch.added)}

    await solera.deploy(Project(assets=[items, tally], stores={"s": table}))
    await solera.run("items")
    await solera.run("tally")
    feed.append("k3")
    await solera.run("items")
    table.value_at = 999
    detail = await solera.run("tally")
    [task] = detail["tasks"]
    assert [a["outcome"] for a in detail["attempts"][task["id"]]] == ["replanned", "succeeded"]
    assert seen == [(False, ["k1", "k2"], 0), (True, ["k1", "k2", "k3"], 0)]
    table.value_at = None
    assert (await solera.read("tally"))["n"] == 3
    feed.append("k4")
    await solera.run("items")
    await solera.run("tally")
    assert seen[-1] == (False, ["k4"], 3), "an ordinary batch again: the full run was owed once"
