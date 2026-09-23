"""Soak: the demo project runs in-process on `file://` state with a fake clock
for >= 500 batches per site, and nothing may grow faster than the work does.

- `data/` grows linearly in committed batches (one object per batch, no
  rewrites of history);
- `control/` — the journal and checkpoints — stays bounded however many runs
  happen: at most two checkpoints, and the journal since the older one;
- finished runs leave memory and land under `runs/`;
- the delta log holds one object per incremental batch, and `keys/` never
  appears.

The real engine, JsonStore and LocalStore all run in-process; `time.time` is
patched to the fake clock (advanced 6 s per run) so the demo's five-second
feed tick produces a new batch every iteration without wall-clock sleeps.
"""

import importlib
import json
import os
import time
from pathlib import Path

from cursus.sdk import Ref
from cursus_server.engine import Engine
from cursus_server.placements.inline import InlinePlacement
from cursus_server.state import State

BATCHES = int(os.getenv("CURSUS_SOAK_BATCHES", "500"))
SAMPLE_EVERY = max(1, BATCHES // 10)


def _bytes(root: Path, prefix: str) -> int:
    base = root / prefix
    return sum(p.stat().st_size for p in base.rglob("*") if p.is_file()) if base.is_dir() else 0


def _count(root: Path, prefix: str) -> int:
    base = root / prefix
    return sum(1 for p in base.rglob("*") if p.is_file()) if base.is_dir() else 0


async def test_soak(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    clock = [1_700_000_000.0]
    monkeypatch.setattr(time, "time", lambda: clock[0])

    import cursus_server.demo as demo

    importlib.reload(demo)  # DATABASE=False regardless of earlier imports
    project = demo.project

    state = await State.open(
        (tmp_path / "state").as_uri(), "soak", clock=lambda: clock[0], flush_interval=0.001
    )
    engine = Engine(
        state,
        project.manifest,
        placements={"Local": lambda env, opt, ctx: InlinePlacement(ctx, project)},
        clock=lambda: clock[0],
    )
    await engine.initialize()
    for name in list(state.model.automations):
        await engine.set_automation(name, False)
    root = tmp_path / "state" / "soak"

    # Saturate the site partition set (one new site per run, caps at 4).
    for _ in range(4):
        run = await engine.submit(["sites"])
        detail = await engine.run_until(run["id"], timeout=1e9)
        assert detail["request"]["status"] == "succeeded"
    assert len(state.model.heads[("sites", "")]["ref"]["meta"]["partitions"]) == 4

    samples = []  # (committed runs, data bytes, control bytes)
    checkpoints_peak = runs_in_memory_peak = 0
    submitted = 4
    for i in range(BATCHES):
        clock[0] += 6  # past the five-second feed tick → one new batch per site
        run = await engine.submit(["site_feed"], partitions="all")
        detail = await engine.run_until(run["id"], timeout=1e9)
        submitted += 1
        assert detail["request"]["status"] == "succeeded", detail["request"]["id"]
        if (i + 1) % 100 == 50:
            # Keep a live incremental consumer: file_index drains site_files
            # per site and advances its watermarks.
            run = await engine.submit(["file_index"], partitions="all")
            detail = await engine.run_until(run["id"], timeout=1e9)
            submitted += 1
            assert detail["request"]["status"] == "succeeded", detail["request"]["id"]
        checkpoints_peak = max(checkpoints_peak, _count(root, "control/checkpoints"))
        runs_in_memory_peak = max(runs_in_memory_peak, len(state.model.runs))
        if (i + 1) % SAMPLE_EVERY == 0 or i == BATCHES - 1:
            samples.append((i + 1, _bytes(root, "data"), _bytes(root, "control")))

    runs, sizes, control = [s[0] for s in samples], [s[1] for s in samples], [s[2] for s in samples]
    early_rate = (sizes[1] - sizes[0]) / (runs[1] - runs[0])
    late_rate = (sizes[-1] - sizes[-2]) / (runs[-1] - runs[-2])
    print(
        f"\nsoak: {BATCHES} runs x 4 sites, data {sizes[0]} -> {sizes[-1]} B "
        f"(early {early_rate:.0f} -> late {late_rate:.0f} B/run), control {control[0]} -> {control[-1]} B "
        f"(max {max(control)}), checkpoints peak {checkpoints_peak}, runs in memory peak {runs_in_memory_peak}"
    )
    assert sizes[-1] > sizes[0], "the soak must actually write data"
    assert late_rate <= max(2.0 * early_rate, early_rate + 16384), (
        f"data growth is superlinear ({early_rate:.0f} -> {late_rate:.0f} B/run) — "
        "batch writes must not rewrite history"
    )
    # control/ is bounded by the state's size, not by how many runs happened.
    assert checkpoints_peak <= 2
    assert max(control[len(control) // 2 :]) <= 2 * max(control[: len(control) // 2]) + 256 * 1024, control
    # Finished runs leave memory for runs/.
    assert runs_in_memory_peak <= 2
    await engine.tick()
    assert len(await state.archived_ids()) == submitted

    # The delta log: one object per committed batch, and no keys/ prefix.
    assert _count(root, "keys") == 0, "the keys/ prefix must not exist (§2.1)"
    assert _count(root, "deltas") >= BATCHES, "one delta object per committed batch"
    delta_meta = state.model.heads[("site_events", "alpha")]["ref"]["meta"].get("delta")
    assert delta_meta and delta_meta["object"].startswith("deltas/site_events/")
    assert delta_meta["batch"] == BATCHES - 1

    # A live consumer still plans incrementally, not as a full re-read.
    run = await engine.submit(["file_index"], partitions="all")
    detail = await engine.run_until(run["id"], timeout=1e9)
    assert detail["request"]["status"] == "succeeded", detail["request"]["id"]
    planned = 0
    for task in detail["tasks"]:
        for attempt in detail["attempts"].get(task["id"], []):
            raw = await state.get_object(f"specs/{attempt['id']}.json")
            if raw is None:
                continue
            planned += 1
            assert json.loads(raw)["inputs"]["site_files"]["changes"]["full"] is False
    assert planned, "file_index planned no incremental attempts"

    # Every head loads through its store, including after a restart.
    await state.close()
    state = await State.open((tmp_path / "state").as_uri(), "soak", clock=lambda: clock[0])
    for store in project.stores.values():
        bind = getattr(store, "bind_objects", None)
        if bind is not None:
            bind(state.objects)
    for head in state.model.heads.values():
        ref = Ref.from_json(head["ref"])
        handle = ref.handle or {}
        if "object" not in handle and "batches" not in handle:
            continue  # lineage-only source heads carry no object payload
        await project.stores[ref.store].load(ref, None, None)
    await state.close()
