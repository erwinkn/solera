"""Soak: the demo project runs in-process on `file://` state with a fake clock
for >= 500 commits per site, and nothing may grow faster than the work does.

- the demo's data (FileStore, under `$SOLERA_DATA`) grows linearly in
  commits: one object per commit of events, and per changed key;
- `control/` — the journal and checkpoints — stays bounded however many runs
  happen: at most two checkpoints, and the journal since the older one;
- finished runs leave memory and land under `runs/`;
- `keys/` — the key indexes — stays bounded by live keys plus the delta log
  a consumer still needs: compaction folds delta files together, the log is
  truncated behind the consumer's position, and unreferenced files are
  deleted.

The real engine, FileStore and LocalStore all run in-process; `time.time` is
patched to the fake clock (advanced 6 s per run) so the demo's five-second
feed tick produces a new batch every iteration without wall-clock sleeps.
"""

import asyncio
import importlib
import os
import time
from collections import Counter
from pathlib import Path

import obstore
import pytest
from solera.ids import ulid_time
from solera.sdk import Ref
from solera_server.engine import Engine
from solera_server.executors.inline import InlinePlacement
from solera_server.state import State

from tests.conftest import whole

pytestmark = (
    pytest.mark.slow
)  # minutes: `pytest --slow`; tests/server/test_scenario.py crosses the same boundaries quickly

BATCHES = int(os.getenv("SOLERA_SOAK_BATCHES", "500"))
SAMPLE_EVERY = max(1, BATCHES // 10)


def _bytes(root: Path, prefix: str) -> int:
    base = root / prefix
    return sum(p.stat().st_size for p in base.rglob("*") if p.is_file()) if base.is_dir() else 0


def _count(root: Path, prefix: str) -> int:
    base = root / prefix
    return sum(1 for p in base.rglob("*") if p.is_file()) if base.is_dir() else 0


def _count_calls(monkeypatch) -> Counter:
    """Count every object store request by kind, whoever makes it."""

    calls = Counter()
    for name in (
        "get_async",
        "get_range_async",
        "put_async",
        "delete_async",
        "list",
        "list_with_delimiter_async",
    ):
        original = getattr(obstore, name)

        def counted(*args, _original=original, _name=name, **kwargs):
            calls[_name] += 1
            return _original(*args, **kwargs)

        monkeypatch.setattr(obstore, name, counted)
    return calls


async def test_soak(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    calls = _count_calls(monkeypatch)
    clock = [1_700_000_000.0]
    monkeypatch.setattr(time, "time", lambda: clock[0])

    import solera_server.demo as demo

    importlib.reload(demo)  # DATABASE=False regardless of earlier imports
    project = demo.project

    state = await State.open(
        (tmp_path / "state").as_uri(), "soak", clock=lambda: clock[0], flush_interval=0.001
    )
    engine = Engine(
        state,
        project.manifest,
        placements={"Local": lambda spec, ctx: InlinePlacement(ctx, project)},
        clock=lambda: clock[0],
    )
    await engine.initialize()
    for name in list(state.model.automations):
        await engine.set_automation(name, False)
    root = tmp_path / "state" / "soak"

    # Saturate the site dynamic partitions (one new site per run, caps at 4).
    for _ in range(4):
        run = await engine.submit(["sites"])
        detail = await engine.run_until(run["id"], timeout=1e9)
        assert detail["request"]["status"] == "succeeded"
    assert len(state.model.heads[("sites", "")]["partitions"]) == 4

    samples = []  # (committed runs, data bytes, control bytes, keys bytes, object requests)
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
            # per site and advances its positions.
            run = await engine.submit(["file_index"], partitions="all")
            detail = await engine.run_until(run["id"], timeout=1e9)
            submitted += 1
            assert detail["request"]["status"] == "succeeded", detail["request"]["id"]
        await engine.upkeep.tick()  # what their own loops do every second
        await engine.history.lake.tick()
        # A flush acknowledges its events before it writes a due checkpoint and
        # cleans up; wait for that, so the sample sees the steady state.
        await state.journal.flush()
        checkpoints_peak = max(checkpoints_peak, _count(root, "control/checkpoints"))
        runs_in_memory_peak = max(runs_in_memory_peak, len(state.model.runs))
        if (i + 1) % SAMPLE_EVERY == 0 or i == BATCHES - 1:
            samples.append(
                (
                    i + 1,
                    _bytes(tmp_path, "data"),
                    _bytes(root, "control"),
                    _bytes(root, "keys"),
                    calls.total(),
                )
            )

    runs, sizes = [s[0] for s in samples], [s[1] for s in samples]
    control, keys = [s[2] for s in samples], [s[3] for s in samples]
    requests = [(b[4] - a[4]) / (b[0] - a[0]) for a, b in zip(samples, samples[1:], strict=False)]
    early_rate = (sizes[1] - sizes[0]) / (runs[1] - runs[0])
    late_rate = (sizes[-1] - sizes[-2]) / (runs[-1] - runs[-2])
    print(
        f"\nsoak: {BATCHES} runs x 4 sites, data {sizes[0]} -> {sizes[-1]} B "
        f"(early {early_rate:.0f} -> late {late_rate:.0f} B/run), control {control[0]} -> {control[-1]} B "
        f"(max {max(control)}), keys {keys[0]} -> {keys[-1]} B (max {max(keys)}), "
        f"requests/run {requests[0]:.0f} -> {requests[-1]:.0f} (max {max(requests):.0f}), "
        f"checkpoints peak {checkpoints_peak}, runs in memory peak {runs_in_memory_peak}"
    )
    assert sizes[-1] > sizes[0], "the soak must actually write data"
    assert late_rate <= max(2.0 * early_rate, early_rate + 16384), (
        f"data growth is superlinear ({early_rate:.0f} -> {late_rate:.0f} B/run) — "
        "appends must not rewrite history"
    )
    # Object requests per run stay flat: nothing reads or rewrites history.
    assert max(requests[len(requests) // 2 :]) <= 1.25 * max(requests[: len(requests) // 2]) + 5, requests
    # control/ is bounded by the state's size, not by how many runs happened.
    assert checkpoints_peak <= 2
    assert max(control[len(control) // 2 :]) <= 2 * max(control[: len(control) // 2]) + 256 * 1024, control
    # Finished runs leave memory for runs/.
    assert runs_in_memory_peak <= 2
    await engine.tick()
    assert len(await history_ids(engine)) == submitted

    # keys/ is bounded by live keys and the log the consumer still needs.
    assert max(keys[len(keys) // 2 :]) <= 2 * max(keys[: len(keys) // 2]) + 64 * 1024, keys
    await asyncio.gather(*engine.upkeep.jobs.values())
    await engine.upkeep.tick()  # the last garbage goes
    for (output, partition), index in state.model.indexes.items():
        assert len(index.spans) <= engine.key_options.fan_in, (output, partition, len(index.spans))
        referenced = {index.path(n) for n in index.referenced()}
        referenced |= {
            p for p in state.model.cleanup_reads() if p.startswith(index.prefix)
        }  # pending cleanups
        on_disk = {str(p.relative_to(root)) for p in (root / index.prefix).glob("*.kx")}
        assert on_disk == referenced, (output, partition, sorted(on_disk - referenced)[:5])
    assert _count(root, "deltas") == 0, "delta files live in the key index (§6)"
    assert state.model.heads[("site_events", "alpha")]["commit_number"] == BATCHES - 1
    # The index agrees with what the store holds.
    for site in ("alpha", "bravo"):
        ref = Ref.from_json(state.model.heads[("site_files", site)]["ref"])
        rows = await project.stores[ref.store].load(ref, None, await whole(state, "site_files", site))
        listed = await engine.list_keys("site_files", site)
        assert listed["total"] == len(rows) == state.model.heads[("site_files", site)]["count"]
        assert sorted(listed["keys"]) == sorted(r["file_id"] for r in rows)

    # A live consumer still plans incrementally, not as a full re-read.
    run = await engine.submit(["file_index"], partitions="all")
    detail = await engine.run_until(run["id"], timeout=1e9)
    assert detail["request"]["status"] == "succeeded", detail["request"]["id"]
    planned = 0
    for task in detail["tasks"]:
        for attempt in detail["attempts"].get(task["id"], []):
            spec = await state.attempt_spec(detail["request"]["id"], attempt["id"])
            if spec is None:
                continue
            planned += 1
            assert spec["inputs"]["site_files"]["batch"]["full"] is False
    assert planned, "file_index planned no incremental attempts"

    # Every head loads through its store, including after a restart.
    await state.close()
    state = await State.open((tmp_path / "state").as_uri(), "soak", clock=lambda: clock[0])
    for head in state.model.heads.values():
        ref = Ref.from_json(head["ref"])
        if "path" not in (ref.handle or {}):
            continue  # lineage-only source heads carry no object payload
        keyed = ref.handle.get("mode") == "keyed"
        await project.stores[ref.store].load(
            ref, None, await whole(state, ref.output, ref.partition) if keyed else None
        )
    await state.close()


async def history_ids(engine) -> list[str]:
    """Every finished run the history holds."""

    def work(con):
        return [r[0] for r in con.execute("SELECT id FROM runs").fetchall()]

    return await engine.history.query(work, ("runs",), live=False)


async def test_soak_with_retention(tmp_path, monkeypatch):
    """J4 gate (docs/object-store-state.md §11): the demo under
    `Retention(days=1)`, its 10-second poller driven by a fake clock that
    steps ten minutes a run, so a day passes every 144 runs. Once the first
    day has gone by, the history stops growing: every run past the
    horizon is deleted — while each head still loads, and a consumer added
    afterwards gets everything. Data in stores never expires."""

    monkeypatch.delenv("DATABASE_URL", raising=False)
    clock = [1_700_000_000.0]
    monkeypatch.setattr(time, "time", lambda: clock[0])

    import solera_server.demo as demo
    from solera.sdk import Incremental, Project, Retention, asset

    importlib.reload(demo)
    base = demo.project
    project = Project(
        assets=list(base.assets.values()),
        sources=list(base.sources.values()),
        stores=base.stores,
        executors=base.executors,
        resources=base.resources,
        automations=base.automations,
        retention=Retention(days=1),
        name=base.name,
    )
    state = await State.open(
        (tmp_path / "state").as_uri(), "retained", clock=lambda: clock[0], flush_interval=0.001
    )
    engine = Engine(
        state,
        project.manifest,
        placements={"Local": lambda spec, ctx: InlinePlacement(ctx, project)},
        clock=lambda: clock[0],
        retention_interval=0,
    )
    await engine.initialize()
    for name in list(state.model.automations):
        await engine.set_automation(name, False)
    for _ in range(4):
        await engine.run_until((await engine.submit(["sites"]))["id"], timeout=1e9)

    runs_total = max(300, BATCHES // 2)
    samples = []  # (run, runs in the history)
    for i in range(runs_total):
        clock[0] += 600
        detail = await engine.run_until(
            (await engine.submit(["site_feed"], partitions="all"))["id"], timeout=1e9
        )
        assert detail["request"]["status"] == "succeeded", detail["request"]["id"]
        if i % 20 == 10:
            run = await engine.submit(["file_index"], partitions="all")
            assert (await engine.run_until(run["id"], timeout=1e9))["request"]["status"] == "succeeded"
        await engine.upkeep.tick()
        await engine.history.lake.tick()
        if (i + 1) % 25 == 0:
            samples.append((i + 1, len(await history_ids(engine))))

    print(f"\nretention soak: {runs_total} runs, samples (run, runs kept): {samples}")
    steady = [s for s in samples if s[0] >= 200]  # well past the first day (144 runs)
    assert steady, "the soak must run past the retention horizon"
    first, peak = steady[0][1], max(s[1] for s in steady)
    assert peak <= 1.2 * first + 50, [s[1] for s in steady]
    # Runs older than a day are gone; the newest day's are all there.
    oldest = min(ulid_time(r) for r in await history_ids(engine))
    assert oldest >= clock[0] - 86400 - 1200

    # Every head still loads.
    for head in state.model.heads.values():
        ref = Ref.from_json(head["ref"])
        if "path" in (ref.handle or {}):
            keyed = ref.handle.get("mode") == "keyed"
            selection = await whole(state, ref.output, ref.partition) if keyed else None
            await project.stores[ref.store].load(ref, None, selection)

    # A consumer added after a day of run expiry receives the full head.
    seen = {}

    @asset(partitions={"site": demo.sites}, inputs={"site_files": Incremental(batch_size=100)})
    def late_reader(ctx, site_files: list):
        seen[ctx.partition] = (ctx.batch["site_files"].full, sorted(r["file_id"] for r in site_files))
        return []

    later = Project(
        assets=[*project.assets.values(), late_reader],
        sources=list(project.sources.values()),
        stores=project.stores,
        executors=project.executors,
        resources=project.resources,
        retention=Retention(days=1),
        name=project.name,
    )
    engine = Engine(
        state,
        later.manifest,
        placements={"Local": lambda spec, ctx: InlinePlacement(ctx, later)},
        clock=lambda: clock[0],
    )
    await engine.initialize()
    detail = await engine.run_until(
        (await engine.submit(["late_reader"], partitions="all"))["id"], timeout=1e9
    )
    assert detail["request"]["status"] == "succeeded"
    for site in ("alpha", "bravo", "charlie", "delta"):
        full, keys = seen[site]
        listed = await engine.list_keys("site_files", site)
        assert full and keys == sorted(listed["keys"])
    await state.close()
