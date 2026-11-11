"""Review round 4 (Fable F1): a whole fan-in over a proceeded partition. Strict xfails until ERW-42 lets an operator proceed on the new tracking core
with that fact owned; they are its tests."""

import asyncio

import pytest
from solera.sdk import In, Incremental, Output, Project, StaticPartitions, asset
from solera.stores import FileStore
from solera_server.tracking import full

from tests.contract.harness import Solera, rebuild

ERW42 = "ERW-42: an operator's proceed is refused on the new tracking core until 'read at a proceeded head' has an owner"
pytestmark = pytest.mark.xfail(strict=True, reason=ERW42)

try:
    from solera.sdk import canonical_partition
except ImportError:
    from solera_server.planning import canonical_partition


async def test_pcF_a_fan_in_over_a_proceeded_partition_stays_partial(tmp_path):
    solera = Solera(tmp_path, "core")
    rows = {"a": 1, "b": 1, "c": 1}
    hold, waiting, seen, last = asyncio.Event(), [False], [], []
    hold.set()
    site = {"site": StaticPartitions(["east", "west"])}

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    @asset(partitions=site, inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"))
    async def copy(ctx, files: list):
        if ctx.batch["files"].index > 0 and not hold.is_set():
            waiting[0] = True
            await hold.wait()
            waiting[0] = False
        return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

    @asset(inputs={"copy": In(all_partitions=True)})
    def total(copy: dict[str, list]):
        seen.append({k: sorted(r["id"] for r in v) for k, v in copy.items()})
        return sum(len(v) for v in copy.values())

    @asset(inputs={"total": In()})
    def final(total: int):
        last.append(total)
        return total

    events = []
    try:
        project = Project(assets=[files, copy, total, final], default_store=FileStore(solera.root / "data"))
        await solera.deploy(project, cancel_grace=0.2, heartbeat_seconds=0.1)
        east = canonical_partition(["site"], {"site": "east"})
        west = canonical_partition(["site"], {"site": "west"})
        record = solera.state.record

        def hooked(*es, **kw):
            for e in es:
                if e.get("type") in ("Settled", "Proceeded"):
                    events.append((e["type"], e["asset"]))
                if e.get("type") == "AttemptFinished" and e.get("commit"):
                    t = solera.engine.m.task(e["task"]) or {}
                    events.append(("commit", t.get("asset"), bool(e["commit"].get("over_proceeded"))))
            return record(*es, **kw)

        solera.state.record = hooked
        await solera.run("files")
        await solera.run("copy", partitions=[east, west])
        await solera.run("total")
        await solera.run("final")
        assert last == [6]
        hold.clear()
        stopped = await solera.submit("copy", partitions=[west], mode="full")
        await solera.until(lambda: waiting[0])
        await solera.cancel(stopped["id"])
        await solera.wait(stopped)
        hold.set()
        await solera.engine.proceed("copy", west, "test")
        del events[:]
        await solera.run("total")  # reads east whole and west's proceeded head {a}: 4
        await solera.settle(0.5)
        reader = await solera.submit("final")
        try:
            await solera.wait(reader, 3)
        except TimeoutError:
            await solera.cancel(reader["id"])
        state = {
            "total read": seen[-1],
            "events": events,
            "waiting": sorted(full.waiting(solera.engine)),
            "final read": last[1:],
        }
        assert last == [6], (
            f"final read total, summed over copy's partial partition, with no proceed on total: {state}"
        )
    finally:
        hold.set()
        await solera.close()
