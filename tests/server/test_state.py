"""Phase 2 — the server state layer (architecture §1, §6, §8, §9, §10)."""

import uuid

import pytest
from dorc.state import Conflict, LostOwnership, State, Tx
from dorc.storage import SlateState

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
        await tx.index_run(run)
        await tx.put_task(task)
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
        "key_updates": {},
        "scope_complete": True,
        **kw,
    }


async def test_commit_installs_everything_atomically(state):
    """§8: a commit installs heads, the commit record, cursor, key state and
    pending OnChange automations in one transaction."""
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
            key_updates={"upstream": {"upserted": {"k1": {"r": "a", "f": "fp"}}}},
        ),
        {"outputs": {"events": ref("events", "v1")}, "cursor": {"seen": 3}},
    )
    async with state.transaction() as tx:
        head = await tx.head("events", "")
        assert head["ref"]["version"] == "v1" and head["commit"] == record["id"]
        assert head["complete"] is True
        assert await tx.cursor("poll", "") == {"seen": 3}
        assert await tx.key_state("poll", "upstream", "") == {"k1": {"r": "a", "f": "fp"}}
        commit = await tx.commit_record(record["id"])
        assert commit["changed"] == ["events"]
        assert commit["input_refs"]["upstream"]["ref"]["version"] == "v0"
        assert (await tx.attempt(task["id"], 1))["status"] == "succeeded"
        auto = await tx.automation("rollup.onchange.0")
        assert auto["pending"] == [
            {"commit": record["id"], "asset": "poll", "scope": "", "outputs": ["events"]}
        ]
        assert (await tx.task(task["id"]))["status"] == "succeeded"


async def test_commit_failure_leaves_nothing(state, monkeypatch):
    """§8: a failure mid-transaction commits nothing — no head, no cursor,
    no key state, no pending automation."""

    async def boom(self, *args):
        raise RuntimeError("injected")

    monkeypatch.setattr(Tx, "put_cursor", boom)
    task = await make_task(state)
    attempt = await claim(state, task)
    with pytest.raises(RuntimeError):
        await state.commit_attempt(
            manifest(),
            attempt,
            prepared(key_updates={"e": {"upserted": {"k": {"r": "a", "f": "f"}}}}),
            {"outputs": {"events": ref("events", "v1")}, "cursor": 1},
        )
    async with state.transaction() as tx:
        assert await tx.head("events", "") is None
        assert await tx.key_state("poll", "e", "") == {}
        assert await tx.cursor("poll", "") is None
        assert (await tx.automation("rollup.onchange.0"))["pending"] == []
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
        auto["pending"] = []
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
        assert (await tx.automation("rollup.onchange.0"))["pending"] == []


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


async def test_key_map_round_trip(state):
    """§6: key maps are hash-addressed objects; staging is idempotent and the
    map round-trips with its key count in meta."""
    keys = {"a": "rev-1", "b": "rev-2"}
    info = await state.stage_key_map(keys)
    assert info["object"].startswith("keys/") and info["object"].endswith(".json")
    assert info["count"] == 2
    again = await state.stage_key_map(keys)
    assert again["object"] == info["object"]  # content-addressed: same body, same key
    assert await state.fetch_key_map(info) == keys
    assert await state.fetch_key_map({"object": "keys/missing.json"}) is None


async def test_restart_recovers_active_attempts(tmp_path, clock):
    """§8/§10: after reopening the state, in-flight attempts are still found
    with their handles so the placement loop can resume at wait."""
    url = tmp_path.as_uri()
    first = State(await SlateState.open(url, "test"), clock=lambda: clock[0])
    await first.initialize(manifest(), "rev1")
    task = await make_task(first)
    attempt = await claim(first, task)
    async with first.transaction() as tx:
        await tx.put_active(attempt, {"attempt": attempt, "handle": {"pid": 1234}})
    await first.close()

    reopened = State(await SlateState.open(url, "test"), clock=lambda: clock[0])
    async with reopened.transaction() as tx:
        actives = await tx.actives()
        assert [v for _, v in actives] == [{"attempt": attempt, "handle": {"pid": 1234}}]
        assert (await tx.attempt(task["id"], 1))["status"] == "claimed"
        assert (await tx.lock(task["asset"], task["scope"]))["attempt"] == attempt
    await reopened.close()


async def test_pool_claim_lease_and_expiry(state, clock):
    """§10: pool tasks are claimable with a lease; expiry returns them to
    queued for another worker."""
    task = await make_task(state)
    attempt = await claim(state, task)
    spec = {"kind": "Pool", "environment": {}, "placement": {"pool": "ingest"}}
    await state.stage_pool_task({**task, "generation": 1}, prepared(), spec)
    claimed = await state.claim_pool_task("w1", ["ingest"], lease_seconds=1)
    assert claimed["attempt"] == attempt and claimed["claimed_by"] == "w1"
    assert await state.claim_pool_task("w2", ["ingest"], lease_seconds=1) is None
    assert await state.claim_pool_task("w3", ["other"], lease_seconds=1) is None

    clock[0] += 2
    await state.sweep_pool_leases()
    reclaimed = await state.claim_pool_task("w2", ["ingest"], lease_seconds=60)
    assert reclaimed["claimed_by"] == "w2"
    with pytest.raises(LostOwnership):
        await state.heartbeat_pool_task("w1", attempt, 60)
    await state.release_pool_task("w2", attempt)
    assert await state.claim_pool_task("w1", ["ingest"], 60) is None


async def test_automation_state_survives_reregistration(state):
    """§9: toggles and pending events survive a manifest reload; a changed
    trigger resets them."""
    async with state.transaction() as tx:
        auto = await tx.automation("rollup.onchange.0")
        assert auto["enabled"] is True
        auto["enabled"] = False
        auto["pending"] = [{"commit": "c1"}]
        await tx.put_automation(auto["name"], auto)
    await state.initialize(manifest(), "rev1")
    async with state.transaction() as tx:
        auto = await tx.automation("rollup.onchange.0")
        assert auto["enabled"] is False and auto["pending"] == [{"commit": "c1"}]
    changed = manifest()
    changed["automations"]["rollup.onchange.0"]["trigger"] = {"kind": "every", "seconds": 60}
    await state.initialize(changed, "rev2")
    async with state.transaction() as tx:
        auto = await tx.automation("rollup.onchange.0")
        assert auto["enabled"] is False and auto["pending"] == []
