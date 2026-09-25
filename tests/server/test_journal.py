"""The journal (docs/object-store-state.md §3, §10): replay, checkpoints,
cleanup, and fencing between writers — on the local filesystem, in memory,
and on an S3-compatible server when CURSUS_TEST_S3 is set."""

import asyncio
import json
import os
import random
import uuid

import obstore
import pytest
from cursus_server.journal import Fenced, Journal, JournalCorrupt
from obstore.store import LocalStore, MemoryStore


class Counter:
    """A toy state: counts per key, plus which writers started."""

    def __init__(self):
        self.counts, self.writers = {}, []

    def restore(self, snap):
        self.counts = dict(snap["counts"]) if snap else {}
        self.writers = list(snap["writers"]) if snap else []

    def apply(self, event):
        if event["type"] == "WriterStarted":
            self.writers.append(event.get("writer"))
        elif event["type"] == "Add":
            self.counts[event["key"]] = self.counts.get(event["key"], 0) + event["n"]

    def snapshot(self):
        return {"counts": dict(self.counts), "writers": list(self.writers)}


# An S3-compatible server to also run against, e.g. http://user:secret@127.0.0.1:9100/bucket
S3_URL = os.getenv("CURSUS_TEST_S3")


@pytest.fixture(params=["local", "memory", "s3"])
def store(request, tmp_path):
    if request.param == "local":
        return LocalStore(str(tmp_path), mkdir=True)
    if request.param == "memory":
        return MemoryStore()
    if not S3_URL:
        pytest.skip("set CURSUS_TEST_S3 to run against an S3-compatible server")
    from urllib.parse import urlsplit

    from obstore.store import S3Store

    u = urlsplit(S3_URL)
    bucket = u.path.strip("/")
    return S3Store(
        bucket,
        prefix=f"journal-test-{uuid.uuid4().hex}",
        endpoint=f"{u.scheme}://{u.hostname}:{u.port}",
        access_key_id=u.username,
        secret_access_key=u.password,
        region="us-east-1",
        client_options={"allow_http": True},
    )


async def open_journal(store, state=None, **kw):
    state = state or Counter()
    j = Journal(store, "control", **{"flush_interval": 0.01, **kw})
    result = await j.open(state.restore, state.apply, state.snapshot)
    return j, state, result


async def add(j, state, key, n=1):
    event = {"type": "Add", "key": key, "n": n}
    state.apply(event)
    await j.durable(event)


def names(store, kind):
    return sorted(
        m["path"].rsplit("/", 1)[-1] for batch in obstore.list(store, f"control/{kind}/") for m in batch
    )


async def test_replay_restores_state(store):
    j, state, result = await open_journal(store)
    assert result.seq == 1 and result.checkpoint is None
    for i in range(20):
        await add(j, state, f"k{i % 3}", i)
    await j.close(checkpoint=False)
    j2, again, result = await open_journal(store)
    assert again.counts == state.counts
    assert again.writers == [1, result.seq]
    await j2.close()


async def test_events_group_into_segments(store):
    j, state, _ = await open_journal(store, flush_interval=0.2)
    events = [{"type": "Add", "key": "a", "n": 1} for _ in range(50)]
    for e in events:
        state.apply(e)
    await asyncio.gather(*(j.append(e) for e in events))
    await j.close(checkpoint=False)
    # The fence, then one segment holding all 50 events.
    assert len(names(store, "journal")) == 2


async def test_checkpoints_match_full_replay_and_cleanup_keeps_two(store):
    rng = random.Random(3)
    j, state, _ = await open_journal(store, min_checkpoint=200)
    for step in range(200):
        await add(j, state, f"k{rng.randrange(10)}", rng.randrange(100))
        if step % 17 == 0:
            await j.flush()
    await j.close(checkpoint=False)
    checkpoints = names(store, "checkpoints")
    assert len(checkpoints) == 2  # the newest and the previous
    oldest_kept = int(checkpoints[0][:-5])
    assert all(int(s[:-5]) > oldest_kept for s in names(store, "journal"))
    _, again, result = await open_journal(store)
    assert result.checkpoint == int(checkpoints[-1][:-5])
    assert again.counts == state.counts


async def test_a_new_writer_fences_the_old_one(store):
    a, sa, _ = await open_journal(store)
    await add(a, sa, "x", 5)
    b, sb, result = await open_journal(store)
    assert sb.counts == {"x": 5} and result.seq == 3
    with pytest.raises(Fenced):
        await add(a, sa, "x", 1)  # a's next segment collides with b's fence
    assert a.fenced
    with pytest.raises(Fenced):
        a.append({"type": "Add", "key": "x", "n": 1})
    await add(b, sb, "y", 2)
    await b.close()
    _, sc, _ = await open_journal(store)
    assert sc.counts == {"x": 5, "y": 2}  # a's rejected event is not there


async def test_fence_converges_past_a_segment_it_did_not_list(store, monkeypatch):
    """A writer whose listing missed the newest segment collides on fencing,
    applies that segment, and fences one further."""

    a, sa, _ = await open_journal(store)
    await add(a, sa, "x", 1)
    j = Journal(store, "control", flush_interval=0.01)
    real = j._list

    async def stale(kind, after=None):
        seqs = await real(kind, after)
        return seqs[:-1] if kind == "journal" and seqs else seqs

    monkeypatch.setattr(j, "_list", stale)
    state = Counter()
    result = await j.open(state.restore, state.apply, state.snapshot)
    assert state.counts == {"x": 1}  # the hidden segment was applied on collision
    assert result.seq == 3
    await j.close()


async def test_an_unconfirmed_write_is_recognized_on_retry(store, monkeypatch):
    j, state, _ = await open_journal(store)
    real = obstore.put_async
    calls = {"n": 0}

    async def lossy(store_, path, data, **kw):
        await real(store_, path, data, **kw)
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("the response was lost")

    monkeypatch.setattr(obstore, "put_async", lossy)
    await add(j, state, "x", 7)  # retried, found identical: success, not fenced
    assert not j.fenced
    monkeypatch.setattr(obstore, "put_async", real)
    await j.close()
    _, again, _ = await open_journal(store)
    assert again.counts == {"x": 7}


async def test_an_unreadable_newest_checkpoint_falls_back(store):
    j, state, _ = await open_journal(store, min_checkpoint=50)
    for i in range(60):
        await add(j, state, f"k{i % 4}", i)
    await j.close()
    newest = names(store, "checkpoints")[-1]
    await obstore.put_async(store, f"control/checkpoints/{newest}", b"not json")
    _, again, result = await open_journal(store)
    assert result.checkpoint == int(names(store, "checkpoints")[-2][:-5])
    assert again.counts == state.counts


async def test_a_gap_in_the_journal_is_an_error(store):
    j, state, _ = await open_journal(store)
    for i in range(3):
        await add(j, state, "x", i)
    await j.close(checkpoint=False)
    await obstore.delete_async(store, ["control/journal/00000000000000000002.json"])
    with pytest.raises(JournalCorrupt):
        await open_journal(store)


async def test_close_writes_a_final_checkpoint(store):
    j, state, _ = await open_journal(store, min_checkpoint=1 << 30)
    await add(j, state, "x", 3)
    await j.close()
    [only] = names(store, "checkpoints")
    body = json.loads(bytes(obstore.get(store, f"control/checkpoints/{only}").bytes()))
    assert body["state"]["counts"] == {"x": 3}
    _, again, result = await open_journal(store)
    assert result.replayed == 0 and again.counts == {"x": 3}
