"""§7 Phase A gate: the demo project runs in-process on `file://` state with a
fake clock for >= 500 batches per site. `objects/data` byte growth must be
linear in committed batches (one object per batch — no O(N^2) rewrites) and
`metadata/wal` object count must stay bounded across periodic GC.

The real engine, JsonStore and LocalStore all run in-process; `time.time` is
patched to the fake clock (advanced 6 s per run) so the demo's five-second
feed tick produces a new batch every iteration without wall-clock sleeps.
"""

import importlib
import json
import os
import time
from pathlib import Path

from cursus_server.engine import Engine
from cursus_server.placements.inline import InlinePlacement
from cursus_server.state import State
from cursus_server.storage import SlateState

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

    # 500 runs x ~30 commits: fast flush interval keeps wall time reasonable;
    # the WAL bound comes from the flush+GC pass, not the interval.
    slate = await SlateState.open((tmp_path / "state").as_uri(), "soak", flush_interval="10ms")
    state = State(slate, clock=lambda: clock[0])
    engine = Engine(
        state,
        # Phase D gate (§7): the demo carries Retention(runs=50).
        {**project.manifest, "retention": {"days": None, "runs": 50}},
        placements={"Local": lambda env, opt, ctx: InlinePlacement(ctx, project)},
        clock=lambda: clock[0],
    )
    await engine.initialize()
    for name in [a["name"] for a in project.manifest["automations"].values()]:
        await engine.set_automation(name, False)

    objects_root = tmp_path / "state" / "soak" / "objects"
    metadata_root = tmp_path / "state" / "soak" / "metadata"

    # Saturate the site partition set (one new site per run, caps at 4).
    for _ in range(4):
        run = await engine.submit(["sites"])
        detail = await engine.run_until(run["id"], timeout=1e9)
        assert detail["request"]["status"] == "succeeded"
    async with state.transaction() as tx:
        head = await tx.head("sites", "")
    keys = head["ref"]["meta"]["partitions"]
    assert len(keys) == 4, keys

    samples = []  # (committed runs, data bytes)
    wal_peak = 0
    bumped = []  # retention must never prune under a live watermark
    for i in range(BATCHES):
        clock[0] += 6  # past the five-second feed tick → one new batch per site
        run = await engine.submit(["site_feed"], partitions="all")
        detail = await engine.run_until(run["id"], timeout=1e9)
        assert detail["request"]["status"] == "succeeded", detail["request"]["id"]
        if (i + 1) % 10 == 0:
            gc = getattr(slate, "gc_once", None)
            if gc is not None:
                await gc(min_age_ms=0)
            wal_peak = max(wal_peak, _count(metadata_root, "wal"))
            bumped += (await engine.retention_sweep())["bumped"]
        if (i + 1) % 100 == 50:
            # Keep a live incremental consumer: file_index drains site_files
            # per site and advances its watermarks between sweeps.
            run = await engine.submit(["file_index"], partitions="all")
            detail = await engine.run_until(run["id"], timeout=1e9)
            assert detail["request"]["status"] == "succeeded", detail["request"]["id"]
        if (i + 1) % SAMPLE_EVERY == 0 or i == BATCHES - 1:
            samples.append((i + 1, _bytes(objects_root, "data")))

    # One committed site_events batch per site per run.
    runs, sizes = [s[0] for s in samples], [s[1] for s in samples]
    n = len(samples)
    sx, sy = sum(runs), sum(sizes)
    sxx = sum(x * x for x in runs)
    sxy = sum(x * y for x, y in samples)
    slope = (n * sxy - sx * sy) / (n * sxx - sx * sx)
    growth = sizes[-1] - sizes[0]
    # Second difference as a marginal rate: constant for linear growth, ~6x
    # higher at the tail for the old all-history-rewrite object. Per-interval
    # steps (keyed snapshots every `snapshot_every` batches) are absorbed by
    # comparing whole-interval rates.
    early_rate = (sizes[1] - sizes[0]) / (runs[1] - runs[0])
    late_rate = (sizes[-1] - sizes[-2]) / (runs[-1] - runs[-2])
    print(
        f"\nsoak: {BATCHES} runs x 4 sites, data {sizes[0]} -> {sizes[-1]} B, "
        f"slope {slope:.1f} B/run, early {early_rate:.0f} -> late {late_rate:.0f} B/run, "
        f"wal peak {wal_peak}"
    )
    assert slope > 0 and growth > 0, "the soak must actually write data"
    assert late_rate <= max(2.0 * early_rate, early_rate + 16384), (
        f"data growth is superlinear ({early_rate:.0f} -> {late_rate:.0f} B/run) — "
        "batch writes must not rewrite history"
    )
    assert wal_peak <= 64, f"wal object count unbounded after GC: {wal_peak}"

    # Phase B gate (§7): the harness writes deltas/, never keys/, and the
    # delta log grows one small object per committed incremental batch.
    delta_objects = _count(objects_root, "deltas")
    key_objects = _count(objects_root, "keys")
    print(f"soak: deltas/ {delta_objects} objects, keys/ {key_objects}")
    assert key_objects == 0, "the keys/ prefix must not exist (§2.1)"
    assert delta_objects >= BATCHES, "one delta object per committed batch"

    # An incremental head records its latest delta in meta.delta (§2.1).
    async with state.transaction() as tx:
        head = await tx.head("site_events", "alpha")
    delta_meta = head["ref"]["meta"].get("delta")
    assert delta_meta and delta_meta["object"].startswith("deltas/site_events/")
    assert delta_meta["batch"] == BATCHES - 1

    # Phase D gate (§7): Retention(runs=50) bounds attempt records per
    # (asset, scope) — site_feed ran BATCHES times per site yet keeps only the
    # newest 50 — while heads still load and live consumers plan incrementally.
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
            spec = json.loads(raw)
            assert spec["inputs"]["site_files"]["changes"]["full"] is False
    assert planned, "file_index planned no incremental attempts"

    sweep = await engine.retention_sweep()
    bumped += sweep["bumped"]
    assert bumped == [], f"live watermarks destroyed by the sweep: {bumped}"
    async with state.transaction() as tx:
        attempts = await tx.scan("attempt/")
        heads = await tx.all_heads()
    per_scope = {}
    for _key, rec in attempts:
        _, _, rest = rec["task"].partition("/")
        per_scope[rest] = per_scope.get(rest, 0) + 1
    assert per_scope["site_feed:alpha"] == 50, per_scope
    assert max(per_scope.values()) <= 50, per_scope

    # Every live head still loads through its store.
    from cursus.sdk import Ref

    for store in project.stores.values():
        bind = getattr(store, "bind_objects", None)
        if bind is not None:
            bind(state.objects)
    for _key, rec in heads:
        ref = Ref.from_json(rec["ref"])
        handle = ref.handle or {}
        if "object" not in handle and "batches" not in handle:
            continue  # lineage-only source heads carry no object payload
        await project.stores[ref.store].load(ref, None, None)
