"""The journal (docs/object-store-state.md §3, §10): replay, checkpoints,
cleanup, and fencing between writers — on the local filesystem, in memory,
and on an S3-compatible server when SOLERA_TEST_S3 is set."""

import asyncio
import json
import os
import random
import uuid

import obstore
import pytest
from obstore.store import LocalStore, MemoryStore
from solera_server.journal import Fenced, Journal, JournalCorrupt, encode


class Counter:
    """A toy state: counts per key, plus which writers started."""

    def __init__(self):
        self.counts, self.writers = {}, []

    def restore(self, snap):
        self.counts = dict(snap["counts"]) if snap else {}
        self.writers = list(snap["writers"]) if snap else []

    def apply(self, event):
        if event["type"] == "EngineStarted":
            self.writers.append(event.get("engine"))
        elif event["type"] == "Add":
            self.counts[event["key"]] = self.counts.get(event["key"], 0) + event["n"]

    def snapshot(self):
        return {"counts": dict(self.counts), "writers": list(self.writers)}


# An S3-compatible server to also run against, e.g. http://user:secret@127.0.0.1:9100/bucket
S3_URL = os.getenv("SOLERA_TEST_S3")


@pytest.fixture(params=["local", "memory", "s3"])
def store(request, tmp_path):
    if request.param == "local":
        return LocalStore(str(tmp_path), mkdir=True)
    if request.param == "memory":
        return MemoryStore()
    if not S3_URL:
        pytest.skip("set SOLERA_TEST_S3 to run against an S3-compatible server")
    from urllib.parse import unquote, urlsplit

    from obstore.store import S3Store

    u = urlsplit(S3_URL)
    bucket = u.path.strip("/")
    return S3Store(
        bucket,
        prefix=f"journal-test-{uuid.uuid4().hex}",
        endpoint=f"{u.scheme}://{u.netloc.rpartition('@')[2]}",
        access_key_id=unquote(u.username),
        secret_access_key=unquote(u.password),
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
    j.append(encode(event))
    await j.durable()


def names(store, kind):
    return sorted(
        m["path"].rsplit("/", 1)[-1]
        for commit_number in obstore.list(store, f"control/{kind}/")
        for m in commit_number
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
    j.append(*map(encode, events))
    await asyncio.sleep(0.3)  # the flush interval passes
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
    kept = [int(s[:-5]) for s in names(store, "journal")]
    assert [s for s in kept if s <= oldest_kept] == [1]  # but the writer's fence
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
        a.append(encode({"type": "Add", "key": "x", "n": 1}))
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


async def test_a_replaced_writer_stays_fenced_after_cleanup(store):
    """The new writer's fence outlives cleanup: however far it has moved on,
    the old writer's next segment collides with that fence and nothing it
    appends is acknowledged."""

    a, sa, _ = await open_journal(store)
    await add(a, sa, "x", 1)
    b, sb, result = await open_journal(store, min_checkpoint=50)
    for i in range(40):  # checkpoints come and go, and so does the journal under them
        await add(b, sb, "y", i)
    assert len(names(store, "checkpoints")) == 2
    assert f"{result.seq:020d}.json" in names(store, "journal")  # b's fence is kept
    with pytest.raises(Fenced):
        await add(a, sa, "x", 100)
    await b.close()
    _, again, _ = await open_journal(store)
    assert again.counts == sb.counts and "x" in again.counts and again.counts["x"] == 1


async def test_two_writers_never_take_the_same_fence(store):
    """Two writers starting at the same seq at the same instant would seal
    identical fence segments, and each would take the other's for its own
    unconfirmed write. Each writer's fence carries a nonce of its own, so
    the second collides, and the first is fenced at its next write."""

    a, sa, first = await open_journal(store, clock=lambda: 1790000000.0)
    b = Journal(store, "control", flush_interval=0.01, clock=lambda: 1790000000.0)
    real = b._list

    async def raced(kind, after=None):  # b listed before a's fence landed
        return [] if kind == "journal" else await real(kind, after)

    b._list = raced
    sb = Counter()
    second = await b.open(sb.restore, sb.apply, sb.snapshot)
    b._list = real
    assert (first.seq, second.seq) == (1, 2)
    with pytest.raises(Fenced):
        await add(a, sa, "x", 1)
    await add(b, sb, "y", 1)
    await b.close()
    _, again, _ = await open_journal(store)
    assert again.counts == {"y": 1}


async def test_an_opener_whose_segments_were_cleaned_up_opens_again(tmp_path):
    """Simulation finding F7: a new writer loads a checkpoint, and before it
    lists the segments after it, the old writer appends, checkpoints and
    cleans those segments up. The gap is not corruption: the new writer
    opens again from the newer checkpoint, with every acknowledged event,
    and fences the old one."""

    store = LocalStore(str(tmp_path), mkdir=True)
    a, sa, _ = await open_journal(store, min_checkpoint=50)
    await add(a, sa, "x")
    b, sb = Journal(store, "control", flush_interval=0.01), Counter()
    replay, loaded, go = b._replay, asyncio.Event(), asyncio.Event()

    async def slow(apply):  # loaded its checkpoint; slow to list what follows
        if not loaded.is_set():
            loaded.set()
            await go.wait()
        return await replay(apply)

    b._replay = slow
    opening = asyncio.create_task(b.open(sb.restore, sb.apply, sb.snapshot))
    await loaded.wait()
    for _ in range(12):
        await add(a, sa, "x")
    assert names(store, "journal")[0] != "00000000000000000002.json"  # cleaned up past it
    go.set()
    await opening
    assert sb.counts["x"] == 13
    with pytest.raises(Fenced):
        await add(a, sa, "x")
    await b.close()
