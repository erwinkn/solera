"""A dead writer on a store that writes in place (D196): the engine keeps
its version, and the output's next attempt asks the store which keys hold
it, deletes those the log lacks, and the engine forgets the version once
that attempt commits. Postgres itself is exercised by the contract's STO-5
and ENG-8, which CI does not run; here the store is one of files that says
it writes in place and answers `written_at` as a table would."""

import pytest
from solera.sdk import Retry
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
