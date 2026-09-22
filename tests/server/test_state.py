"""Phase 2 — the server state layer (architecture §1, §6, §8, §9, §10)."""

import uuid

import pytest
from cursus_server.state import Conflict, LostOwnership, State, Tx
from cursus_server.storage import SlateState

LEASE = 60.0


def manifest():
    return {
        "project": "test",
        "revision": "rev1",
        "assets": {
            "poll": {"version": "1", "outputs": [{"name": "events"}]},
            "rollup": {"version": "1", "outputs": [{"name": "summary"}]},
            "rollup2": {"version": "1", "outputs": [{"name": "summary"}]},
        },
        "automations": {
            "rollup.onchange.0": {
                "name": "rollup.onchange.0",
                "targets": ["rollup"],
                "trigger": {"kind": "onchange", "outputs": ["events"]},
                "watched": ["events"],
                "enabled": True,
                "partitions": None,
                "mode": "incremental",
                "upstream": False,
                "config": None,
                "keys": None,
            }
        },
    }


def ref(output, version, scope="", store="json"):
    return {
        "output": output,
        "store": store,
        "handle": {"object": f"{output}/{version}.json"},
        "version": version,
        "partition": scope,
        "meta": {},
    }


@pytest.fixture
def clock():
    return [1000.0]


@pytest.fixture
async def state(tmp_path, clock):
    opened = State(await SlateState.open(tmp_path.as_uri(), "test"), clock=lambda: clock[0])
    await opened.initialize(manifest(), "rev1")
    yield opened
    await opened.close()


async def make_task(state, asset="poll", scope="", run_id=None):
    async with state.transaction() as tx:
        run_id = run_id or uuid.uuid4().hex
        run = await tx.run(run_id) or {
            "id": run_id,
            "status": "running",
            "tasks": [],
            "created_at": state.clock(),
            "updated_at": state.clock(),
        }
        task = {
            "id": f"{run_id}/{asset}:{scope}",
            "run": run_id,
            "asset": asset,
            "scope": scope,
            "status": "queued",
            "deps": [],
            "generation": 0,
            "attempt_count": 0,
            "max_attempts": 1,
            "ready_at": state.clock(),
        }
        run["tasks"].append(task["id"])
        await tx.put_run(run)
        await tx.put_task(task)
        await tx.put_pending(task)
        await tx.enqueue(task["id"], task["ready_at"])
        await tx.set_run_stats(run_id, {"left": len(run["tasks"]), "bad": 0})
        return task


async def claim(state, task, lease=LEASE):
    async with state.transaction() as tx:
        task = await tx.task(task["id"])
        await state.claim(tx, task, lease)
        task["status"] = "running"
        await tx.put_task(task)
        return f"{task['id']}/{task['generation']}"


def prepared(inputs=None, baseline=None, **kw):
    return {
        "inputs": inputs or {},
        "baseline": baseline or {},
        "watermark_updates": {},
        "scope_complete": True,
        **kw,
    }


async def test_commit_installs_everything_atomically(state):
    """§8: a commit installs heads, the commit record, cursor and edge
    watermarks in one transaction; OnChange automations consume the record
    from the commit log via their watermark (§4.3)."""
    upstream = await make_task(state, asset="rollup")
    await state.commit_attempt(
        manifest(),
        await claim(state, upstream),
        prepared(),
        {"outputs": {"summary": ref("summary", "v0")}},
    )
    task = await make_task(state)
    attempt = await claim(state, task)
    record = await state.commit_attempt(
        manifest(),
        attempt,
        prepared(
            inputs={"upstream": {"ref": ref("summary", "v0")}},
            watermark_updates={"upstream": {"batch": 3, "offset": 0, "fingerprint": "fp"}},
        ),
        {"outputs": {"events": ref("events", "v1")}, "cursor": {"seen": 3}},
    )
    async with state.transaction() as tx:
        head = await tx.head("events", "")
        assert head["ref"]["version"] == "v1" and head["commit"] == record["id"]
        assert head["complete"] is True
        assert await tx.cursor("poll", "") == {"seen": 3}
        assert await tx.watermark("poll", "upstream", "") == {
            "batch": 3,
            "offset": 0,
            "fingerprint": "fp",
        }
        commit = await tx.commit_record(record["id"])
        assert commit["changed"] == ["events"]
        assert commit["input_refs"]["upstream"]["ref"]["version"] == "v0"
        assert (await tx.attempt(task["id"], 1))["status"] == "succeeded"
        auto = await tx.automation("rollup.onchange.0")
        assert "pending" not in auto
        events, high = await state.automation_events(tx, auto)
        # The automation watches `events`; the unrelated `summary` commit is
        # in the log but not an event for it.
        assert events == [
            {"commit": record["id"], "asset": "poll", "scope": "", "outputs": ["events"]}
        ]
        assert high == int(record["id"])
        assert (await tx.task(task["id"]))["status"] == "succeeded"


async def test_commits_list_newest_first(state):
    """§4.1: commit/{seq:020d} ids are monotonic and listings are newest-first."""
    ids = []
    for i in range(3):
        task = await make_task(state, run_id=f"run-{i}")
        record = await state.commit_attempt(
            manifest(),
            await claim(state, task),
            prepared(),
            {"outputs": {"events": ref("events", f"v{i}")}},
        )
        ids.append(record["id"])
    assert ids == sorted(ids)  # monotonic sequence keys
    async with state.transaction() as tx:
        listed = await tx.commits()
        assert [k.split("/", 1)[1] for k, _ in listed] == list(reversed(ids))
        assert [r["id"] for _, r in listed] == list(reversed(ids))


async def test_commit_failure_leaves_nothing(state, monkeypatch):
    """§8: a failure mid-transaction commits nothing — no head, no cursor,
    no watermark, no pending automation."""

    async def boom(self, *args):
        raise RuntimeError("injected")

    monkeypatch.setattr(Tx, "put_cursor", boom)
    task = await make_task(state)
    attempt = await claim(state, task)
    with pytest.raises(RuntimeError):
        await state.commit_attempt(
            manifest(),
            attempt,
            prepared(watermark_updates={"e": {"batch": 1, "offset": 0, "fingerprint": "f"}}),
            {"outputs": {"events": ref("events", "v1")}, "cursor": 1},
        )
    async with state.transaction() as tx:
        assert await tx.head("events", "") is None
        assert await tx.watermark("poll", "e", "") is None
        assert await tx.cursor("poll", "") is None
        auto = await tx.automation("rollup.onchange.0")
        assert (await state.automation_events(tx, auto))[0] == []
        assert (await tx.task(task["id"]))["status"] == "running"  # still owns the scope
        assert await tx.commits() == []


async def test_identical_content_is_not_a_change(state):
    """§1/§8: `changed` compares committed ref versions only — a re-commit of
    the same version produces no change and wakes no automation."""
    first = await make_task(state)
    await state.commit_attempt(
        manifest(), await claim(state, first), prepared(), {"outputs": {"events": ref("events", "v1")}}
    )
    async with state.transaction() as tx:
        auto = await tx.automation("rollup.onchange.0")
        _, high = await state.automation_events(tx, auto)
        auto["commit_watermark"] = high
        await tx.put_automation(auto["name"], auto)
    second = await make_task(state)
    record = await state.commit_attempt(
        manifest(),
        await claim(state, second),
        prepared(baseline={"events": (await _head(state, "events", ""))}),
        {"outputs": {"events": ref("events", "v1")}},
    )
    assert record["changed"] == []
    async with state.transaction() as tx:
        auto = await tx.automation("rollup.onchange.0")
        assert (await state.automation_events(tx, auto))[0] == []


async def _head(state, output, scope):
    async with state.transaction() as tx:
        return await tx.head(output, scope)


async def test_stale_generation_cannot_commit(state, clock):
    """§8: once a scope is reclaimed under a new generation, the fenced
    attempt's commit fails with LostOwnership."""
    task = await make_task(state)
    attempt1 = await claim(state, task, lease=1)
    clock[0] += 2
    await claim(state, task)  # second claim takes the expired scope
    with pytest.raises(LostOwnership):
        await state.commit_attempt(
            manifest(), attempt1, prepared(), {"outputs": {"events": ref("events", "v1")}}
        )


async def test_lease_expiry_lets_second_claimant_take_scope(state, clock):
    """§8/§10: lease expiry frees the scope for another run's task; the first
    attempt is fenced off."""
    task1 = await make_task(state, run_id="run-a")
    attempt1 = await claim(state, task1, lease=1)
    task2 = await make_task(state, run_id="run-b")
    clock[0] += 2
    attempt2 = await claim(state, task2)
    assert attempt2 != attempt1
    async with state.transaction() as tx:
        old = await tx.attempt(task1["id"], 1)
        assert old["status"] == "expired"
        assert (await tx.task(task1["id"]))["status"] == "queued"  # retryable
    with pytest.raises(LostOwnership):
        await state.commit_attempt(
            manifest(), attempt1, prepared(), {"outputs": {"events": ref("events", "v1")}}
        )
    await state.commit_attempt(manifest(), attempt2, prepared(), {"outputs": {"events": ref("events", "v2")}})
    assert (await _head(state, "events", ""))["ref"]["version"] == "v2"


async def test_commit_after_pinned_input_moved_is_refused(state):
    """§8: commit refuses when an input head moved after the attempt pinned it."""
    producer = await make_task(state)
    await state.commit_attempt(
        manifest(),
        await claim(state, producer),
        prepared(),
        {"outputs": {"events": ref("events", "v1")}},
    )
    consumer = await make_task(state, asset="rollup")
    consumer_attempt = await claim(state, consumer)
    pins = prepared(inputs={"events": {"ref": ref("events", "v1")}}, baseline={"summary": None})
    producer2 = await make_task(state)
    await state.commit_attempt(
        manifest(),
        await claim(state, producer2),
        prepared(baseline={"events": await _head(state, "events", "")}),
        {"outputs": {"events": ref("events", "v2")}},
    )
    with pytest.raises(Conflict, match="moved after pinning"):
        await state.commit_attempt(
            manifest(), consumer_attempt, pins, {"outputs": {"summary": ref("summary", "s1")}}
        )


async def test_commit_after_output_head_moved_is_refused(state):
    """§8: commit refuses when an output head changed since the claim's baseline."""
    consumer = await make_task(state, asset="rollup")
    attempt = await claim(state, consumer)
    other = await make_task(state, asset="rollup2")
    await state.commit_attempt(
        manifest(),
        await claim(state, other),
        prepared(),
        {"outputs": {"summary": ref("summary", "s1")}},
    )
    with pytest.raises(Conflict, match="head changed"):
        await state.commit_attempt(
            manifest(),
            attempt,
            prepared(baseline={"summary": None}),
            {"outputs": {"summary": ref("summary", "s2")}},
        )


async def test_omitted_output_without_head_errors(state):
    """§2/§8: an omitted output keeps its prior head — error when none exists."""
    task = await make_task(state)
    attempt = await claim(state, task)
    with pytest.raises(Conflict, match="omitted output"):
        await state.commit_attempt(manifest(), attempt, prepared(), {"outputs": {}})


async def test_delta_log_round_trip(state):
    """§2.1/§2.2: delta objects land under deltas/ and fold into the live key
    map — pure diffs, removes cancel upserts, last writer wins."""
    from cursus.stores import delta_path

    await state.put_object(
        delta_path("events", "", 0),
        b'{"batch": 0, "rows": 2, "upserted": {"a": "r1", "b": "r2"}}',
    )
    await state.put_object(
        delta_path("events", "", 1),
        b'{"batch": 1, "rows": 2, "upserted": {"a": "r3"}, "deleted": ["b"]}',
    )
    assert (await state.delta("events", "", 1))["batch"] == 1
    assert await state.delta("events", "", 9) is None
    assert await state.delta_key_map("events", "", 1) == {"a": "r3"}
    # A replace write's delta: `reset` marks the supersede; every key it
    # dropped is in `deleted`, so the forward fold stays correct.
    await state.put_object(
        delta_path("events", "", 2),
        b'{"batch": 2, "rows": 2, "upserted": {"c": "r1"}, "deleted": ["a"], "reset": true}',
    )
    assert await state.delta_key_map("events", "", 2) == {"c": "r1"}


async def test_restart_requeues_inflight_attempts(tmp_path, clock):
    """§4.3: on open every lock is expired — the attempt is fenced and the
    task requeued exactly once; no durable queue/lease survives."""
    url = tmp_path.as_uri()
    first = State(await SlateState.open(url, "test"), clock=lambda: clock[0])
    await first.initialize(manifest(), "rev1")
    task = await make_task(first)
    attempt = await claim(first, task)
    await first.close()

    reopened = State(await SlateState.open(url, "test"), clock=lambda: clock[0])
    async with reopened.transaction() as tx:
        assert await tx.lock(task["asset"], task["scope"]) is None
        assert (await tx.attempt(task["id"], 1))["status"] == "expired"
        task = await tx.task(task["id"])
        assert task["status"] == "queued"
        queued = await tx.queued()
        assert [tid for tid, _ in queued].count(task["id"]) == 1  # exactly once
    await reopened.close()


async def test_restart_requeues_pool_claim(tmp_path, clock):
    """§4.3/§10: a pool-claimed attempt is memory — after a restart the claim
    is gone and the task requeues; the old claim can never renew or commit."""
    url = tmp_path.as_uri()
    first = State(await SlateState.open(url, "test"), clock=lambda: clock[0])
    await first.initialize(manifest(), "rev1")
    task = await make_task(first)
    attempt = await claim(first, task)
    spec = {
        "execution": {"kind": "Pool", "environment": {"name": "ingest"}, "placement": {}}
    }
    await first.stage_pool_task({**task, "generation": 1}, prepared(), spec)
    claimed = await first.claim_pool_task(
        "w1", ["ingest"], {"cpu": 1, "memory": 10**9, "gpu": None}, lease_seconds=60
    )
    assert claimed["attempt"] == attempt
    await first.close()

    reopened = State(await SlateState.open(url, "test"), clock=lambda: clock[0])
    async with reopened.transaction() as tx:
        assert await tx.pool_task(attempt) is None  # memory-only claim
        assert (await tx.attempt(task["id"], 1))["status"] == "expired"
        assert (await tx.task(task["id"]))["status"] == "queued"
    with pytest.raises(LostOwnership):
        await reopened.heartbeat_pool_task("w1", attempt, 60)
    async with reopened.transaction() as tx:
        with pytest.raises(LostOwnership):
            await reopened.renew(tx, attempt, 60)
    await reopened.close()


async def test_pool_claim_lease_and_expiry(state, clock):
    """§10: pool tasks are claimable with a lease; expiry returns them to
    queued for another worker."""
    task = await make_task(state)
    attempt = await claim(state, task)
    spec = {
        "execution": {
            "kind": "Pool",
            "environment": {"name": "ingest"},
            "placement": {"cpu": 2},
        }
    }
    await state.stage_pool_task({**task, "generation": 1}, prepared(), spec)
    big = {"cpu": 4, "memory": 10**9, "gpu": None}
    small = {"cpu": 1, "memory": 10**9, "gpu": None}
    # a worker that doesn't fit the task's needs never sees it
    assert await state.claim_pool_task("w0", ["ingest"], small, lease_seconds=1) is None
    claimed = await state.claim_pool_task("w1", ["ingest"], big, lease_seconds=1)
    assert claimed["attempt"] == attempt and claimed["claimed_by"] == "w1"
    assert await state.claim_pool_task("w2", ["ingest"], big, lease_seconds=1) is None
    assert await state.claim_pool_task("w3", ["other"], big, lease_seconds=1) is None

    clock[0] += 2
    await state.sweep_pool_leases()
    reclaimed = await state.claim_pool_task("w2", ["ingest"], big, lease_seconds=60)
    assert reclaimed["claimed_by"] == "w2"
    with pytest.raises(LostOwnership):
        await state.heartbeat_pool_task("w1", attempt, 60)
    await state.release_pool_task("w2", attempt)
    assert await state.claim_pool_task("w1", ["ingest"], big, 60) is None


async def test_advance_run_is_constant_read_per_completion(state, monkeypatch):
    """§4.2: finishing one task of a 10,000-task run reads a bounded number
    of durable records — dependents index + counters, never a task/ scan."""
    from cursus_server import storage

    run_id = "big-run"
    async with state.transaction() as tx:
        run = {
            "id": run_id,
            "status": "running",
            "tasks": [],
            "created_at": state.clock(),
            "updated_at": state.clock(),
        }
        for i in range(10_000):
            task = {
                "id": f"{run_id}/poll:{i}",
                "run": run_id,
                "asset": "poll",
                "scope": str(i),
                "status": "queued",
                "deps": [],
                "generation": 0,
                "attempt_count": 0,
                "max_attempts": 1,
                "ready_at": state.clock(),
            }
            run["tasks"].append(task["id"])
            await tx.put_task(task)
            await tx.put_pending(task)
            await tx.enqueue(task["id"], task["ready_at"])
        await tx.set_run_stats(run_id, {"left": len(run["tasks"]), "bad": 0})
        await tx.put_run(run)

    reads = {"get": 0, "scan": 0}
    orig_get = storage.Transaction.get
    orig_scan = storage.Transaction.scan

    async def counted_get(self, *args, **kw):
        reads["get"] += 1
        return await orig_get(self, *args, **kw)

    async def counted_scan(self, *args, **kw):
        reads["scan"] += 1
        return await orig_scan(self, *args, **kw)

    monkeypatch.setattr(storage.Transaction, "get", counted_get)
    monkeypatch.setattr(storage.Transaction, "scan", counted_scan)

    async with state.transaction() as tx:
        task = await tx.task(f"{run_id}/poll:0")
        await state.claim(tx, task, LEASE)
        task["status"] = "running"
        await tx.put_task(task)
    await state.commit_attempt(
        manifest(),
        f"{run_id}/poll:0/1",
        prepared(),
        {"outputs": {"events": ref("events", "v1", scope="0")}},
    )
    async with state.transaction() as tx:
        assert (await tx.task(f"{run_id}/poll:0"))["status"] == "succeeded"
        assert (await tx.run(run_id))["status"] == "running"  # 9,999 left
    assert reads["scan"] == 0
    assert reads["get"] < 30


async def test_automation_state_survives_reregistration(state):
    """§9/§4.3: toggles and the commit watermark survive a manifest reload; a
    changed trigger restarts consumption at the log's current end."""
    task = await make_task(state)
    await state.commit_attempt(
        manifest(), await claim(state, task), prepared(), {"outputs": {"events": ref("events", "v1")}}
    )
    async with state.transaction() as tx:
        auto = await tx.automation("rollup.onchange.0")
        assert auto["enabled"] is True and "pending" not in auto
        auto["enabled"] = False
        auto["commit_watermark"] = 1  # consumed the first commit
        await tx.put_automation(auto["name"], auto)
    await state.initialize(manifest(), "rev1")
    async with state.transaction() as tx:
        auto = await tx.automation("rollup.onchange.0")
        assert auto["enabled"] is False and auto["commit_watermark"] == 1
    changed = manifest()
    changed["automations"]["rollup.onchange.0"]["trigger"] = {"kind": "every", "seconds": 60}
    await state.initialize(changed, "rev2")
    async with state.transaction() as tx:
        auto = await tx.automation("rollup.onchange.0")
        assert auto["enabled"] is False
        assert auto["commit_watermark"] == state._commit_seq
