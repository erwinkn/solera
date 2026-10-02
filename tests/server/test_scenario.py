"""One short run across every boundary that rewrites state: journal
flushes and checkpoints, history flushes, key-index compaction, log
truncation behind a consumer, garbage collection and run retirement. Then
the journal alone reproduces the live model, every delivery was exact, and
`keys/` holds what is referenced. (The long growth soaks: `--slow`.)"""

import json
import random
from pathlib import Path

from solera.keys.index import Options
from solera.sdk import Incremental, Output, Project, Retention, asset
from solera.stores import Patch
from solera_server.engine import Engine
from solera_server.history import History
from solera_server.placements.inline import InlinePlacement
from solera_server.state import State


class Clock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now


async def test_every_boundary_once(tmp_path):
    rng = random.Random(3)
    truth: dict[str, int] = {}
    seen: dict[str, int] = {}
    pending = {"rows": [], "remove": []}

    @asset(outputs=Output("items", key="id"))
    def items():
        return Patch(pending["rows"], remove=pending["remove"])

    @asset(inputs={"items": Incremental(page_size=5)})  # a full read spans pages: wipe on its first
    def mirror(ctx, items: list):
        changes = ctx.changes["items"]
        if changes.full and changes.first:
            seen.clear()
        seen.update({row["id"]: row["v"] for row in items})
        for key in changes.deleted:
            seen.pop(key, None)
        return [{"n": len(items)}]

    project = Project(assets=[items, mirror], retention=Retention(runs=3))
    clock = Clock()
    url = tmp_path.as_uri()
    state = await State.open(url, "test", clock=clock, flush_interval=0.001, min_checkpoint=4096)
    engine = Engine(
        state,
        project.manifest,
        placements={"Local": lambda s, c: InlinePlacement(c, project)},
        clock=clock,
        key_options=Options(l0_max_files=2),
        retention_interval=0,
        history=History(state, clock=clock, flush_rows=10),
    )
    await engine.initialize()
    for step in range(16):
        rows, remove = {}, set()
        for _ in range(rng.randint(1, 5)):
            key = f"k{rng.randint(0, 12):02d}"
            if truth and rng.random() < 0.3:
                remove.add(gone := rng.choice(sorted(truth)))
                rows.pop(gone, None)
            elif key not in remove:
                rows[key] = rng.randint(0, 3)
        pending["rows"], pending["remove"] = [{"id": k, "v": v} for k, v in rows.items()], sorted(remove)
        assert (await engine.run_until((await engine.submit(["items"]))["id"], 20))["request"][
            "status"
        ] == "succeeded"
        for key in remove:
            truth.pop(key, None)
        truth.update(rows)
        if step % 4 == 3:
            await engine.run_until((await engine.submit(["mirror"]))["id"], 20)
            assert seen == truth
        clock.now += 60
        await engine.tick()  # archive what finished
        await engine.upkeep.tick()  # truncate, compact, collect, retire
        for job in list(engine.upkeep.jobs.values()):
            await job
        await engine.history.lake.tick()

    m = state.model
    assert m.indexes[("items", "")].depth >= 1  # compacted
    assert any(f["rows"] for f in m.history.files.get("runs", ()))  # history flushed
    kept = await engine.list_runs(None, limit=1000)
    assert kept["total"] <= 2 * 3 + 2  # of 20: retired behind `runs=3` per asset
    assert state.journal._checkpoints and state.journal._checkpoints[-1] > 1  # checkpointed on the way
    index = m.indexes[("items", "")]
    root = Path(state.objects_url.removeprefix("file://"))
    on_disk = {str(p.relative_to(root)) for p in (root / index.prefix).glob("*.kx")}
    assert on_disk <= {index.path(n) for n in index.referenced()} | m.discard_reads() | {
        g[0] for g in m.garbage
    }
    await state.durable()
    again = await State.open(url, "test", clock=clock, writer=False)
    live, replayed = json.loads(json.dumps(m.snapshot())), json.loads(json.dumps(again.model.snapshot()))
    live.pop("writer"), replayed.pop("writer")
    assert replayed == live
    await engine.stop()
    await state.close()
