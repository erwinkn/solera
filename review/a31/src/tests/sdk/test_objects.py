"""`solera.objects` (docs/object-store-state.md §0): `read` and `swap`, a
compare-and-swap with `If-Match`, settled by reading back when refused or
unanswered — on the local filesystem (a lock), in memory, and on an
S3-compatible server when SOLERA_TEST_S3 is set."""

import asyncio
import os
import uuid

import obstore
import pytest
from obstore.exceptions import GenericError, PreconditionError
from obstore.store import LocalStore, MemoryStore
from solera import objects
from solera.objects import Conflict, read, swap

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
        prefix=f"objects-test-{uuid.uuid4().hex}",
        endpoint=f"{u.scheme}://{u.netloc.rpartition('@')[2]}",
        access_key_id=unquote(u.username),
        secret_access_key=unquote(u.password),
        region="us-east-1",
        client_options={"allow_http": True},
    )


async def test_a_swap_replaces_only_the_version_it_read(store):
    """`swap(etag=None)` creates; a swap from the ETag read replaces; one
    from an older ETag, or a create over an object, is another writer's
    loss: `Conflict`."""

    assert await read(store, "head") is None
    first = await swap(store, "head", b"w1-1", None)
    assert await read(store, "head") == (b"w1-1", first)
    second = await swap(store, "head", b"w1-2", first)
    assert await read(store, "head") == (b"w1-2", second) and second != first
    with pytest.raises(Conflict):
        await swap(store, "head", b"w2-1", first)
    with pytest.raises(Conflict):
        await swap(store, "head", b"w2-1", None)
    assert (await read(store, "head"))[0] == b"w1-2"


async def test_of_concurrent_swaps_from_one_version_exactly_one_wins(store):
    """Ten writers swap from the same ETag, each with a body of its own:
    one lands, nine get `Conflict`, and the object holds the winner's."""

    etag = await swap(store, "head", b"base", None)

    async def one(i):
        try:
            return await swap(store, "head", f"writer-{i}".encode(), etag)
        except Conflict:
            return None

    won = [i for i, got in enumerate(await asyncio.gather(*(one(i) for i in range(10)))) if got is not None]
    assert len(won) == 1
    assert (await read(store, "head"))[0] == f"writer-{won[0]}".encode()


async def test_a_lost_answer_is_settled_by_reading_back_its_own_body(monkeypatch):
    """The write lands but its answer is lost: reading back finds exactly
    the body being written, so the swap succeeded, and says with which ETag."""

    store, real = MemoryStore(), obstore.put_async
    etag = await swap(store, "head", b"w1-1", None)

    async def landed_unheard(*args, **kwargs):
        await real(*args, **kwargs)
        raise GenericError("connection reset after the request landed")

    monkeypatch.setattr(objects.obstore, "put_async", landed_unheard)
    new = await swap(store, "head", b"w1-2", etag)
    assert await read(store, "head") == (b"w1-2", new)


async def test_a_refusal_that_changed_nothing_writes_again(monkeypatch):
    """S3 answers 409 to one of two concurrent conditional writes: refused,
    yet the object still reads at the ETag the swap had. Nothing landed, so
    the swap writes again."""

    store, real, calls = MemoryStore(), obstore.put_async, []
    etag = await swap(store, "head", b"w1-1", None)

    async def refused_once(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise PreconditionError("409 ConditionalRequestConflict")
        return await real(*args, **kwargs)

    monkeypatch.setattr(objects.obstore, "put_async", refused_once)
    new = await swap(store, "head", b"w1-2", etag)
    assert len(calls) == 2 and await read(store, "head") == (b"w1-2", new)


async def test_on_a_file_the_etag_is_the_digest_and_the_lock_is_dropped(tmp_path):
    """`file://` has no `If-Match`: the ETag is the content's SHA-256, a swap
    holds an `flock` on the directory only while it runs and leaves no file
    of its own, and an earlier try of the same body reads as landed. A swap
    from a version of a file that is gone creates nothing."""

    import hashlib

    store = LocalStore(str(tmp_path), mkdir=True)
    etag = await swap(store, "control/head", b"w1-1", None)
    assert etag == hashlib.sha256(b"w1-1").hexdigest()
    assert await swap(store, "control/head", b"w1-1", None) == etag  # its own earlier try
    import fcntl

    lock = os.open(tmp_path / "control", os.O_RDONLY)  # not held after the swap
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.close(lock)
    assert sorted(p.name for p in (tmp_path / "control").iterdir()) == ["head"]
    with pytest.raises(Conflict):
        await swap(store, "gone/head", b"w2-1", etag)
    assert not (tmp_path / "gone").exists()
