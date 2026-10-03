"""The journal (docs/object-store-state.md §0, §10): one object swapped with
`If-Match`, and checkpoints — replay, fencing between engines, the
checkpoint's order (list, write, read back, move, delete) — on the local
filesystem, in memory, and on an S3-compatible server when SOLERA_TEST_S3 is
set."""

import json
import os
import uuid

import obstore
import orjson
import pytest
from obstore.store import LocalStore, MemoryStore
from solera import objects
from solera_server import journal as journal_module
from solera_server.journal import Fenced, Journal, encode


class Counter:
    """A toy state: counts per key."""

    def __init__(self):
        self.counts = {}

    def restore(self, snap):
        self.counts = dict(snap["counts"]) if snap else {}

    def apply(self, event):
        if event["type"] == "Add":
            self.counts[event["key"]] = self.counts.get(event["key"], 0) + event["n"]

    def snapshot(self):
        return {"counts": dict(self.counts)}


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
    return S3Store(
        u.path.strip("/"),
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


def record(j, state, key, n=1):
    event = {"type": "Add", "key": key, "n": n}
    state.apply(event)
    j.append(encode(event))


async def journal_of(store) -> dict:
    got = await objects.read(store, "control/journal.json")
    return orjson.loads(got[0])


def checkpoints(store) -> list[str]:
    return sorted(
        m["path"].rsplit("/", 1)[1] for b in obstore.list(store, prefix="control/checkpoints/") for m in b
    )


async def test_replay_restores_state(store):
    j, s, result = await open_journal(store)
    assert result.checkpoint is None and result.replayed == 0
    for key in "abca":
        record(j, s, key)
    await j.durable()
    await j.close(checkpoint=False)
    j2, s2, result = await open_journal(store)
    assert s2.counts == {"a": 2, "b": 1, "c": 1} and result.replayed == 4
    await j2.close()


async def test_a_checkpoint_moves_the_journal_and_keeps_one(store):
    """Once the events reach the threshold, the state is written, read back
    and named by the journal, whose events start over; the checkpoint before
    goes. A replay from it equals the full fold."""

    j, s, _ = await open_journal(store, min_checkpoint=200)
    for i in range(40):
        record(j, s, f"k{i % 7}")
        await j.flush()
    assert len(checkpoints(store)) == 1
    body = await journal_of(store)
    assert body["checkpoint"] == j.checkpoint and len(body["events"]) < 40
    await j.close(checkpoint=False)
    j2, s2, result = await open_journal(store)
    assert s2.counts == s.counts and result.checkpoint == j.checkpoint
    await j2.close()


async def test_close_takes_a_final_checkpoint(store):
    j, s, _ = await open_journal(store)
    record(j, s, "a", 3)
    await j.close()
    body = await journal_of(store)
    assert body["events"] == [] and checkpoints(store) == [f"{body['checkpoint']}.json"]
    j2, s2, result = await open_journal(store)
    assert s2.counts == {"a": 3} and result.replayed == 0
    await j2.close()


async def test_a_new_engine_fences_the_old_one(store):
    """B's fence swaps in A's journal under its own id: A's next swap meets
    B's body and fails, and nothing A had acknowledged is lost."""

    a, sa, _ = await open_journal(store)
    record(a, sa, "x", 5)
    await a.durable()
    b, sb, result = await open_journal(store)
    assert sb.counts == {"x": 5} and result.engine != a.engine
    record(a, sa, "x", 1)
    with pytest.raises(Fenced):
        await a.durable()
    assert a.fenced
    with pytest.raises(Fenced):
        record(a, sa, "y")
    record(b, sb, "z")
    await b.durable()
    await b.close(checkpoint=False)
    _, sc, _ = await open_journal(store)
    assert sc.counts == {"x": 5, "z": 1}


async def test_a_read_only_open_does_not_fence(store):
    a, sa, _ = await open_journal(store)
    record(a, sa, "x")
    await a.durable()
    r = Journal(store, "control")
    reader = Counter()
    result = await r.open(reader.restore, reader.apply, reader.snapshot, writer=False)
    assert reader.counts == {"x": 1} and result.engine is None and r.fenced
    record(a, sa, "x")
    await a.durable()  # still the writer
    await a.close()


async def test_a_lost_answer_is_settled_by_reading_back_its_own_bytes(store, monkeypatch):
    """A flush lands but its answer is lost: `swap` reads back exactly the
    sealed bytes, so the flush succeeded — no conflict, no fence."""

    j, s, _ = await open_journal(store)
    real, lost = objects._conditional_put, []

    async def landed_unheard(st, path, data, etag):
        tag = await real(st, path, data, etag)
        if not lost:
            lost.append(path)
            raise ConnectionError("reset after the request landed")
        return tag

    monkeypatch.setattr(objects, "_conditional_put", landed_unheard)
    record(j, s, "a")
    await j.durable()
    assert lost and not j.fenced
    record(j, s, "b")
    await j.durable()
    await j.close(checkpoint=False)
    _, s2, _ = await open_journal(store)
    assert s2.counts == {"a": 1, "b": 1}


async def test_an_opener_whose_checkpoint_is_gone_reads_the_journal_again(store, monkeypatch):
    """B reads the journal naming checkpoint 1; A checkpoints again and
    deletes it before B loads it; B finds it gone and reads the journal
    again, now naming checkpoint 2."""

    a, sa, _ = await open_journal(store, min_checkpoint=1)
    record(a, sa, "x")
    await a.flush()  # checkpoint 1
    real_read, moved = journal_module.read, []

    async def read_then_a_moves(st, path):
        found = await real_read(st, path)
        if not moved:
            moved.append(1)
            record(a, sa, "y")
            await a.flush()  # checkpoint 2, and checkpoint 1 deleted
        return found

    monkeypatch.setattr(journal_module, "read", read_then_a_moves)
    b, sb, result = await open_journal(store)
    assert moved and sb.counts == {"x": 1, "y": 1} and result.checkpoint == a.checkpoint
    await b.close(checkpoint=False)


async def test_cleanup_deletes_only_what_it_listed_before_its_move(store, monkeypatch):
    """The journal spec's case (NoAckedLoss): A lists the checkpoints, then B
    fences A, appends and writes a checkpoint of its own; A's move fails, so A
    deletes nothing — B's checkpoint survives, and with it every event B
    acknowledged."""

    a, sa, _ = await open_journal(store, min_checkpoint=1 << 30)
    record(a, sa, "x")
    await a.durable()
    real_list, others = obstore.list, {}

    def list_then_b_takes_over(st, prefix=None, **kw):
        listed = list(real_list(st, prefix=prefix, **kw))
        others["listed"] = listed
        return listed

    monkeypatch.setattr(obstore, "list", list_then_b_takes_over)
    b, sb, _ = await open_journal(store, min_checkpoint=1)
    record(b, sb, "y")
    await b.flush()  # B's own checkpoint
    b_checkpoint = b.checkpoint
    snap = journal_module._dumps({"at": 0, "engine": a.engine, "state": sa.snapshot()})
    await a._take_checkpoint(snap)  # listed, wrote, read back; its move meets B's body
    assert a.fenced and f"{b_checkpoint}.json" in checkpoints(store)
    await b.close(checkpoint=False)
    _, sc, _ = await open_journal(store)
    assert sc.counts == {"x": 1, "y": 1}


async def test_no_checkpoint_is_named_before_it_reads_back(store, monkeypatch):
    """A checkpoint that does not read back is never named: the journal stays
    as it is, and the next due point tries again."""

    j, s, _ = await open_journal(store, min_checkpoint=1)
    real_get, broken = obstore.get_async, []

    async def garbled_once(st, path, **kw):
        got = await real_get(st, path, **kw)
        if "/checkpoints/" in path and not broken:
            broken.append(path)

            class Garbled:
                meta = got.meta

                async def bytes_async(self):
                    return b"{not json"

            return Garbled()
        return got

    monkeypatch.setattr(obstore, "get_async", garbled_once)
    record(j, s, "a")
    await j.flush()
    assert broken and (await journal_of(store))["checkpoint"] is None
    record(j, s, "b")
    await j.flush()  # due again: written, read back, named
    assert (await journal_of(store))["checkpoint"] == j.checkpoint is not None
    await j.close(checkpoint=False)
    _, s2, _ = await open_journal(store)
    assert s2.counts == {"a": 1, "b": 1}


async def test_the_encoding_is_deterministic():
    """The same state encodes to the same bytes, keys sorted: `swap` reads a
    lost answer back by bytes."""

    state = {"b": [1, 2.5, None, "x"], "a": {"z": True, "y": {"k": -(2**62)}}}
    assert journal_module._dumps(state) == journal_module._dumps(json.loads(json.dumps(state)))
    assert journal_module._dumps(state).startswith(b'{"a":{"y"')


@pytest.mark.parametrize("landed", [True, False])
async def test_a_checkpoints_move_that_errors_is_written_again(store, monkeypatch, landed):
    """F24: the swap that moves the journal to a new checkpoint fails with an
    error that is no conflict — its answer lost after it landed, or not
    landed at all. The move stays pending and is written again, the very
    same body, before anything else: a lone engine is not fenced, and no
    event is lost."""

    j, s, _ = await open_journal(store, min_checkpoint=1)
    real, failed = journal_module.swap, []

    async def move_fails_once(st, path, data, etag):
        if b'"events":[]' in data and not failed:
            failed.append(1)
            if landed:
                await real(st, path, data, etag)
            raise ConnectionError("the move's answer was lost")
        return await real(st, path, data, etag)

    monkeypatch.setattr(journal_module, "swap", move_fails_once)
    record(j, s, "a")
    with pytest.raises(ConnectionError):
        await j.flush()  # the event lands; its checkpoint's move fails
    assert failed and not j.fenced
    record(j, s, "b")
    await j.flush()  # the move first, again, then the event
    assert not j.fenced and (await journal_of(store))["checkpoint"] == j.checkpoint is not None
    await j.close(checkpoint=False)
    _, s2, _ = await open_journal(store)
    assert s2.counts == {"a": 1, "b": 1}
