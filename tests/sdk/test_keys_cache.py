"""The engine cache and the resolver's core (docs/resolved-commits.md §4–§5):
engine-resolved deltas against the cold readers', the declines, deduplication,
admission, integrity, candidates and reservations."""

import asyncio
import os
import random
import struct
import zlib

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from obstore.store import MemoryStore
from solera import _native
from solera.keys import FOOTER_SIZE, SortedEntries
from solera.keys.cache import Corrupt, EngineCache
from solera.keys.index import FileInfo, IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO
from solera.keys.resolver import Ask, Limits, Malformed, Prepared, Resolver, answers, frame, request, unframe

from . import keys_reference as _python

OPTS = Options(
    block_size=512,
    max_file_bytes=16 * 1024,
    l0_max_files=3,
    l0_max_bytes=1 << 30,
    level_base=6 * 1024,
    fanout=3,
)


def key(i):
    return f"k{i:06d}".encode()


async def built_index(io, n=3000, commits=12, seed=1):
    """An index over several levels, as random commits and compactions leave it."""

    rng = random.Random(seed)
    state = IndexState(prefix="keys/out/_/")
    for b in range(commits):
        ks = sorted({key(rng.randrange(n)) for _ in range(n if b == 0 else 300)})
        rm = sorted({key(rng.randrange(n)) for _ in range(30)} - set(ks)) if b else []
        files, _ = await KeyIndex(io, None, state, OPTS).resolve(
            SortedEntries.of(ks, [rng.randbytes(8) for _ in ks], rm),
            commit_number=b,
            attempt=f"w{b}",
            generation=b + 1,
        )
        state = state.committed(b, files, keep_log=False)
        while (out := await KeyIndex(io, None, state, OPTS).compact()) is not None:
            state = state.compacted(*out[:2])
    return state


def prepared(state, commit_number=99, generation=100, replace=True):
    return Prepared(
        partition="",
        commit_number=commit_number,
        generation=generation,
        index=state,
        head_commit=98,
        replace=replace,
    )


def ask(state, keys, versions, removes=(), kind="patch", commit_number=99, generation=100):
    run = SortedEntries.of(list(keys), list(versions), list(removes))
    return Ask("out", "", kind, commit_number, generation, state.prefix, 98, run)


async def engine_answer(resolver, state, a, worker_id="inv", p=None, live=True):
    body = await resolver.resolve(
        "att", request(worker_id, [a]), lambda name: p or prepared(state), lambda: live
    )
    return answers(body)["out"]


def decoded(files):
    return [e for f in files for e in _python.iter_file(f)]


@pytest.fixture
def io():
    return ObjectIO(MemoryStore())


async def test_engine_and_cold_resolves_agree(io, tmp_path):
    state = await built_index(io)
    assert state.depth >= 2 and len(state.level(0)) >= 1
    cache = EngineCache(str(tmp_path))
    resolver = Resolver(cache, io, OPTS)
    rng = random.Random(5)
    first = await engine_answer(resolver, state, ask(state, [key(1)], [b"x"]))
    assert first[0] == {"name": "out", "result": "declined", "reason": "cold"}
    await asyncio.gather(*resolver._fills)
    assert cache.warm(state)
    page = await KeyIndex(io, None, state, OPTS).page(None, 10**6)
    current = dict(zip(page[0], page[2], strict=True))  # each key's version
    for step in range(30):
        kind = "replace" if step % 5 == 4 else "patch"
        ks = sorted({key(rng.randrange(3500)) for _ in range(rng.choice([1, 20, 400]))})
        vs = [current.get(k, b"new") if rng.random() < 0.5 else rng.randbytes(8) for k in ks]
        rm = [] if kind == "replace" else sorted({key(rng.randrange(3500)) for _ in range(10)} - set(ks))
        answer, delta = await engine_answer(resolver, state, ask(state, ks, vs, rm, kind))
        idx = KeyIndex(io, None, state, OPTS)
        if kind == "replace":
            files, _ = await idx.replace(
                _native.Rows.pairs(list(zip(ks, vs, strict=True))), 99, f"c{step}", generation=100
            )
        else:
            files, _ = await idx.resolve(
                SortedEntries.of(ks, vs, rm), commit_number=99, attempt=f"c{step}", generation=100, exact=True
            )
        cold = decoded([await io.read_whole(state.path(f.name), f.size) for f in files.files])
        if answer["result"] == "empty":
            assert cold == [] and files.added == files.removed == 0
        else:
            assert answer["result"] == "delta"
            assert decoded([delta]) == cold  # entries, generations, payloads and predecessors
            assert (answer["added"], answer["removed"]) == (files.added, files.removed)


async def test_declines(io, tmp_path):
    state = await built_index(io, commits=3)
    cache = EngineCache(str(tmp_path))
    resolver = Resolver(cache, io, OPTS, Limits(max_keys=50, max_entries=10))
    assert await cache.fill(io, state)
    a = ask(state, [key(1)], [b"x"])
    reason = lambda ans: ans[0].get("reason")  # noqa: E731
    assert reason(await engine_answer(resolver, state, a, live=False)) == "not_live"
    assert (
        reason(await engine_answer(resolver, state, ask(state, [key(1)], [b"x"], commit_number=7)))
        == "invalid"
    )
    assert (
        reason(await engine_answer(resolver, state, ask(state, [key(1)], [b"x"], generation=7))) == "invalid"
    )
    no_replace = prepared(state, replace=False)
    replace = ask(state, [key(1)], [b"x"], kind="replace")
    assert reason(await engine_answer(resolver, state, replace, p=no_replace)) == "invalid"
    moved = Prepared("", 99, 100, state, 97, True)
    assert reason(await engine_answer(resolver, state, a, p=moved)) == "stale"
    many = [key(i) for i in range(60)]
    assert reason(await engine_answer(resolver, state, ask(state, many, [b"x"] * 60))) == "too_big"
    assert reason(await engine_answer(resolver, state, replace)) == "too_big"  # 3000 entries > 10
    busy = Resolver(cache, io, OPTS, Limits(queue_bytes=10))
    assert reason(await engine_answer(busy, state, a)) == "busy"
    with pytest.raises(ValueError):
        await resolver.resolve(
            "att", b"\x09" + request("inv", [a])[1:], lambda n: prepared(state), lambda: True
        )


async def test_identical_requests_share_one_computation(io, tmp_path, monkeypatch):
    state = await built_index(io, commits=3)
    cache = EngineCache(str(tmp_path))
    await cache.fill(io, state)
    resolver = Resolver(cache, io, OPTS)
    calls = []
    real = resolver._compute

    async def counting(*args):
        calls.append(args[1])
        await asyncio.sleep(0.01)
        return await real(*args)

    monkeypatch.setattr(resolver, "_compute", counting)
    a = ask(state, [key(1), key(2)], [b"x", b"y"])
    r = ask(state, [key(1), key(2)], [b"x", b"y"], kind="replace")  # the same run: another answer
    got = await asyncio.gather(*(engine_answer(resolver, state, x) for x in (a, a, a, r)))
    assert calls == ["patch", "replace"]
    assert got[0] == got[1] == got[2] and got[3][0]["removed"] > 0


async def test_alternating_indexes_keep_one_warm(io, tmp_path):
    a = await built_index(io, seed=1)
    b = IndexState(prefix="keys/other/_/")
    b = await built_index_at(io, b)
    probe = EngineCache(str(tmp_path / "probe"))
    cache = EngineCache(str(tmp_path / "cache"), disk=max(probe.need(a), probe.need(b)), candidates=0)
    assert await cache.fill(io, a)
    gets = io.metrics.gets
    for _ in range(5):
        assert not await cache.fill(io, b)  # not admitted: no refill, no eviction of a
        assert cache.warm(a)
    assert io.metrics.gets == gets


async def built_index_at(io, state):
    rng = random.Random(2)
    ks = [key(i) for i in range(2500)]
    files, _ = await KeyIndex(io, None, state, OPTS).replace(
        _native.Rows.pairs([(k, rng.randbytes(8)) for k in ks]), 0, "w", generation=1
    )
    return state.committed(0, files, keep_log=False)


def tail_of(local: bytes) -> int:
    """Where a local file's tail — its identity and directory — starts (its footer says)."""

    return struct.unpack_from("<Q", local, len(local) - 20)[0]


async def test_corruption_is_refetched(io, tmp_path):
    state = await built_index(io, commits=2)
    cache = EngineCache(str(tmp_path))
    resolver = Resolver(cache, io, OPTS)
    await cache.fill(io, state)
    path = state.path(state.files[0].name)
    local = cache.files[path].local
    data = bytearray(open(local, "rb").read())
    data[10] ^= 0xFF  # a block
    open(local, "wb").write(bytes(data))
    a = ask(state, [key(i) for i in range(0, 3000, 7)], [b"x"] * 429)
    assert (await engine_answer(resolver, state, a))[0]["reason"] == "cold"
    await asyncio.gather(*resolver._fills)
    assert (await engine_answer(resolver, state, a))[0]["result"] == "delta"
    # A corrupted directory is refused on open, as after a restart.
    data = bytearray(open(cache.files[path].local, "rb").read())
    data[tail_of(data) + 40] ^= 0xFF
    bad = os.path.join(str(tmp_path), "bad.kxl")
    open(bad, "wb").write(bytes(data))
    with pytest.raises(ValueError):
        _native.LocalFile(bad)


async def test_a_candidate_becomes_the_committed_file_without_a_get(io, tmp_path):
    state = await built_index(io, commits=2)
    cache = EngineCache(str(tmp_path))
    cache.admit(state)
    await cache.fill(io, state)
    resolver = Resolver(cache, io, OPTS)
    answer, delta = await engine_answer(resolver, state, ask(state, [key(1), key(5000)], [b"x", b"y"]))
    assert answer["result"] == "delta"
    name = f"{99:012d}-att.0000"
    await io.write(state.path(name), delta)  # the worker uploads the bytes it was given
    from solera.keys.index import FileInfo

    f = FileInfo.describe(name, 0, delta)
    assert f.digest == answer["file"]["digest"]
    gets = io.metrics.gets
    assert await cache.committed(state.prefix, f, state.path(name))
    assert io.metrics.gets == gets and state.path(name) in cache.files
    # Evicted, the candidate costs a GET at the next fill instead.
    cache.offer(state.path("other"), delta)
    cache.candidates.clear()
    assert not await cache.committed(state.prefix, f, state.path("other"))


async def test_a_fill_that_cannot_reserve_demotes(io, tmp_path):
    state = await built_index(io, commits=2)
    cache = EngineCache(str(tmp_path), disk=10_000, candidates=0)
    cache.disk = cache.need(state)
    assert cache.admit(state)
    cache.disk = 10_000  # then the disk shrinks: nothing fits
    assert not await cache.fill(io, state)
    assert cache.reserved == 0 and not cache.indexes[state.prefix].admitted


async def test_a_restart_keeps_what_checks_out(io, tmp_path):
    state = await built_index(io, commits=2)
    cache = EngineCache(str(tmp_path))
    assert await cache.fill(io, state)
    path = state.path(state.files[0].name)
    data = bytearray(open(cache.files[path].local, "rb").read())
    data[tail_of(data) + 40] ^= 0xFF  # one directory goes bad while the engine is down
    open(cache.files[path].local, "wb").write(bytes(data))
    open(os.path.join(str(tmp_path), "x.kxl.1.tmp"), "wb").write(b"half")
    again = EngineCache(str(tmp_path))
    assert path not in again.files and len(again.files) == len(state.files) - 1
    assert sorted(os.listdir(str(tmp_path))) == sorted(
        os.path.basename(f.local) for f in again.files.values()
    )
    gets = io.metrics.gets
    assert await again.fill(io, state)  # only the bad one is fetched again
    assert io.metrics.gets == gets + 1


# -- the milestone 4 review's findings ---------------------------------------------------------


async def test_only_the_named_object_is_installed(io, tmp_path):
    """Review 1: bytes are installed or summarized only if they are the file
    their `FileInfo` names — size and digest — and a copy kept across a
    restart counts only for the object it was built from."""

    good = _native.encode_file([b"a"], [1], b"\x00", payloads=[b"good"])
    evil = _native.encode_file([b"a"], [1], b"\x00", payloads=[b"evil"])
    f = FileInfo.describe("000000000001-x.0000", 0, good)
    state = IndexState(prefix="keys/out/_/", files=(f,))
    cache = EngineCache(str(tmp_path))
    assert cache.admit(state)
    with pytest.raises(Corrupt):
        await cache.install(state.prefix, f, state.path(f.name), evil)
    assert not cache.files
    await io.write(state.path(f.name), evil)  # what the store holds is not what the commit names
    with pytest.raises(Corrupt):
        await cache.fill(io, state)
    assert not cache.warm(state)
    assert await cache.install(state.prefix, f, state.path(f.name), good) and cache.warm(state)
    other = FileInfo.describe(f.name, 0, evil)  # the same name, another object
    again = EngineCache(str(tmp_path))
    assert again.warm(state) and not again.warm(IndexState(prefix=state.prefix, files=(other,)))
    assert not again.files  # and the copy that is not it goes


async def test_committed_deltas_are_verified_before_install(io, tmp_path):
    from solera_server.keyservice import KeyService

    good = _native.encode_file([b"a"], [1], b"\x00", payloads=[b"good"])
    evil = _native.encode_file([b"a"], [1], b"\x00", payloads=[b"evil"])
    f = FileInfo.describe("000000000001-x.0000", 0, good)
    service = KeyService(io.store, str(tmp_path))
    service.start()
    try:
        path = f"keys/out/_/{f.name}.kx"
        await io.write(path, evil)
        state = IndexState(prefix="keys/out/_/")
        await asyncio.wrap_future(service._submit(asyncio.sleep(0)))
        service.cache.admit(state)
        service.committed("keys/out/_/", lambda n: f"keys/out/_/{n}.kx", [f], 0)
        await asyncio.sleep(0.2)
        assert path not in service.cache.files  # not the file the commit names
        assert service.floor() == float("inf")  # its reader pin went with it
    finally:
        await service.stop()


async def test_the_disk_budget_holds_against_the_real_size(io, tmp_path):
    """Review 2: 10,000 keys with one long repeated revision compress to almost
    nothing; their local form is many times the estimate. The build is not
    written if the room for its real size cannot be had."""

    keys = [key(i) for i in range(10_000)]
    data = _native.encode_file(keys, [0] * len(keys), bytes(len(keys)), payloads=[b"r" * 256] * len(keys))
    f = FileInfo.describe("c1-0000", 1, data)
    state = IndexState(prefix="keys/out/_/", files=(f,))
    await io.write(state.path(f.name), data)
    cache = EngineCache(str(tmp_path), candidates=0)
    cache.disk = cache.need(state)
    assert cache.admit(state)
    assert not await cache.fill(io, state)
    assert cache.used <= cache.disk and cache.reserved == 0 and not os.listdir(str(tmp_path))
    roomy = EngineCache(str(tmp_path / "roomy"), candidates=0)
    assert await roomy.fill(io, state) and roomy.used <= roomy.disk


def forged(a, *, keys=None, payload=None, digest=None):
    """A request for `a` whose header or payload says what the caller likes."""

    body = request("inv", [a])
    header, _ = unframe(body)
    payload = payload if payload is not None else a.run.encode()
    o = header["outputs"][0]
    o.update(size=len(payload), digest=digest or _native.content_digest(payload))
    if keys is not None:
        o["keys"] = keys
    return frame(header, [payload])


async def test_requests_are_checked_against_their_bytes(io, tmp_path):
    """Review 3: the digest that deduplicates and the count that limits are
    the payload's own, and a frame whose bounds do not hold is refused."""

    state = await built_index(io, commits=2)
    cache = EngineCache(str(tmp_path))
    await cache.fill(io, state)
    resolver = Resolver(cache, io, OPTS, Limits(max_keys=1))
    first, second = ask(state, [key(1)], [b"first"]), ask(state, [key(1)], [b"second"])
    lying = forged(second, digest=_native.content_digest(first.run.encode()))
    got = await asyncio.gather(
        *(
            resolver.resolve("att", b, lambda n: prepared(state), lambda: True)
            for b in (request("inv", [first]), lying)
        )
    )
    assert answers(got[0])["out"][0]["result"] == "delta"
    assert answers(got[1])["out"][0]["reason"] == "invalid"  # its digest is not its bytes'
    two = ask(state, [key(1), key(2)], [b"x", b"y"])
    claims_none = forged(two, keys=0)  # claims nothing, carries two
    out = await resolver.resolve("att", claims_none, lambda n: prepared(state), lambda: True)
    assert answers(out)["out"][0]["reason"] in ("invalid", "too_big")
    for bad in (
        b"\x01\xff\xff\x00\x00{}",  # a header past the end
        frame({"outputs": [{"name": "out", "offset": 0, "size": 10**6}]}, [b"x"]),  # a payload too
        frame(
            {"outputs": [{"name": "out", "offset": 0, "size": 1}, {"name": "out", "offset": 0, "size": 1}]},
            [b"x"],
        ),
    ):
        with pytest.raises(Malformed):
            await resolver.resolve("att", bad, lambda n: prepared(state), lambda: True)


def patched(data, *, block=None, entries=None):
    """`data`, an uncompressed file of one block, with that block's bytes or
    its footer's entry count replaced and every checksum made to match: what
    a forger sends."""

    data = bytearray(data)
    foot = len(data) - FOOTER_SIZE
    at, length = struct.unpack_from("<Q", data, foot + 28)[0], struct.unpack_from("<I", data, foot + 36)[0]
    if block is not None:
        [(_, off, size, _, crc)] = _python.parse_index(bytes(data), len(data))["blocks"]
        assert len(block) == size
        data[off : off + size] = block
        i = data.index(struct.pack("<I", crc), at, at + length)
        data[i : i + 4] = struct.pack("<I", zlib.crc32(block))
        struct.pack_into("<I", data, foot + 40, zlib.crc32(bytes(data[at : at + length])))
    if entries is not None:
        struct.pack_into("<Q", data, foot + 8, entries)
    return bytes(data)


async def test_limits_hold_against_what_a_run_holds(io, tmp_path):
    """Round 2, finding 1: a run is decoded once, every fact checked, before
    a limit is applied to it — not its footer's word — and no output's
    payload is another's."""

    state = await built_index(io, commits=2)
    cache = EngineCache(str(tmp_path))
    await cache.fill(io, state)
    resolver = Resolver(cache, io, OPTS, Limits(max_keys=1))

    async def reason(body):
        out = await resolver.resolve("att", body, lambda n: prepared(state), lambda: True)
        return answers(out)["out"][0].get("reason")

    # Two entries behind a footer that says one: never a delta of two.
    two = ask(state, [key(1), key(2)], [b"x", b"y"])
    assert await reason(forged(two, keys=1, payload=patched(two.run.encode(codec=0), entries=1))) == "invalid"
    # A length that overflows when added: a decline, not a Rust panic.
    one = ask(state, [b"k" * 20], [b"v"])
    data = one.run.encode(codec=0)
    size = _python.parse_index(data, len(data))["blocks"][0][2]
    overflow = b"\x00" + b"\xff" * 9 + b"\x01" + b"\x00" * (size - 11)
    assert await reason(forged(one, payload=patched(data, block=overflow))) == "invalid"
    # One payload behind 32 outputs: refused before anything is copied.
    header, payloads = unframe(request("inv", [one]))
    header["outputs"] = [{**header["outputs"][0], "name": f"o{i}"} for i in range(32)]
    with pytest.raises(Malformed):
        await resolver.resolve(
            "att", frame(header, [bytes(payloads)]), lambda n: prepared(state), lambda: True
        )


async def test_the_queue_is_taken_before_scheduling(io, tmp_path, monkeypatch):
    """Review 5: a burst of distinct requests holds no more than the queue's
    bytes at once, and a source commit's resolve is held to the same limit."""

    state = await built_index(io, commits=2)
    cache = EngineCache(str(tmp_path))
    await cache.fill(io, state)
    asks = [ask(state, [key(i)], [b"v%d" % i]) for i in range(10)]
    one = max(len(a.run.encode()) for a in asks)
    resolver = Resolver(cache, io, OPTS, Limits(queue_bytes=2 * one))
    real, peak = resolver._compute, []

    async def slow(*args):
        peak.append(resolver._queued)
        await asyncio.sleep(0.05)
        return await real(*args)

    monkeypatch.setattr(resolver, "_compute", slow)
    got = await asyncio.gather(*(engine_answer(resolver, state, a) for a in asks))
    reasons = [g[0].get("reason") for g in got]
    assert reasons.count("busy") == 8 and max(peak) <= 2 * one and resolver._queued == 0
    resolver._queued = 2 * one  # full: a source commit's resolve waits its turn too
    answer, _ = await resolver.compute(prepared(state), "patch", asks[0].run, "keys/out/_/x.kx")
    assert answer["reason"] == "busy"


async def test_a_canceled_fill_keeps_its_room_until_the_build_ends(io, tmp_path, monkeypatch):
    """Review 6: the build runs on its thread whatever happens to the fill;
    its reservation and its temporary file are its own until it ends, and
    nothing is published for a fill that was canceled."""

    import threading

    from solera.keys import cache as cache_module

    state = await built_index(io, commits=1)
    cache = EngineCache(str(tmp_path))
    started, go = threading.Event(), threading.Event()
    real = cache_module._native.build_local

    def paused(*args):
        started.set()
        go.wait(5)
        return real(*args)

    monkeypatch.setattr(cache_module._native, "build_local", paused)
    fill = asyncio.ensure_future(cache.fill(io, state))
    await asyncio.to_thread(started.wait, 5)
    files = list(cache._fills.values())  # one per file: the builds behind the fill
    fill.cancel()
    await asyncio.sleep(0.05)
    assert cache.reserved > 0  # still the builder's
    go.set()
    with pytest.raises(asyncio.CancelledError):
        await fill
    await asyncio.gather(*files, return_exceptions=True)
    assert cache.reserved == 0
    assert sorted(os.listdir(str(tmp_path))) == sorted(
        os.path.basename(f.local) for f in cache.files.values()
    )


async def test_a_reader_does_not_evict_what_compaction_wrote(io, tmp_path):
    """Review 7: a compaction's output, installed before the compaction is
    published, stays while readers still have the snapshot it replaces open; the
    inputs go once the compaction is published and no reader holds them."""

    state = await built_index(io, commits=3)
    cache = EngineCache(str(tmp_path))
    await cache.fill(io, state)
    idx = KeyIndex(io, None, state, OPTS)
    plan = (state.level(0) + state.level(1), 1) if state.level(0) else (state.level(1), 2)
    written = []
    idx.on_write = lambda path, f, data: written.append((path, f, data))
    added, removed, _ = await idx.compact(plan)
    for path, f, data in written:
        assert await cache.install(state.prefix, f, path, data)
    with cache.open(state):
        assert all(path in cache.files for path, _, _ in written)
        cache.retire([state.path(n) for n in removed])
        assert all(state.path(n) in cache.files for n in removed)  # still read
    assert not any(state.path(n) in cache.files for n in removed)  # gone with the last reader
    assert cache.warm(state.compacted(added, removed))


async def test_a_background_fill_is_a_reader_pin(io, tmp_path, monkeypatch):
    """Review 8: a fill started by a cold decline holds the event counter it read
    the index at until its fetches are done."""

    from solera_server.keyservice import KeyService

    state = await built_index(io, commits=1)
    service = KeyService(io.store, str(tmp_path))
    service.start()
    try:
        gate = asyncio.Event()
        real = EngineCache.fill

        async def held(self, io_, st):
            await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(gate.wait(), asyncio_loop))
            return await real(self, io_, st)

        asyncio_loop = asyncio.get_running_loop()
        monkeypatch.setattr(EngineCache, "fill", held)
        p = Prepared("", 99, 100, state, 98, True, at=7)
        body = request("inv", [ask(state, [key(1)], [b"x"])])
        out = await service.resolve("att", body, lambda n: p, lambda: True, 7)
        assert answers(out)["out"][0]["reason"] == "cold"
        assert service.floor() == 7  # the fill still reads
        gate.set()
        for _ in range(100):
            if service.floor() == float("inf"):
                break
            await asyncio.sleep(0.02)
        assert service.floor() == float("inf")
    finally:
        await service.stop()


async def test_a_cache_that_cannot_start_says_so(tmp_path):
    """Round 2, finding 3: the thread's setup error reaches the caller at
    once — not a wait forever — and the service declines from then on."""

    from solera_server.keyservice import KeyService

    blocker = tmp_path / "file"
    blocker.write_text("not a directory")
    service = KeyService(None, str(blocker / "cache"))
    with pytest.raises(OSError):
        await asyncio.wait_for(asyncio.to_thread(service.start), 5)
    assert service.loop is None and not service._running()
    assert await service.resolve("att", b"", lambda n: None, lambda: True, 0) is None


async def test_admission_remembers_what_a_file_built_to(io, tmp_path):
    """Round 2, finding 8: a file whose local form proved too big for the
    disk is not fetched and built again at the next fill; more room, and it
    is."""

    keys = [key(i) for i in range(10_000)]
    data = _native.encode_file(keys, [0] * len(keys), bytes(len(keys)), payloads=[b"r" * 256] * len(keys))
    f = FileInfo.describe("c1-0000", 1, data)
    state = IndexState(prefix="keys/out/_/", files=(f,))
    await io.write(state.path(f.name), data)
    cache = EngineCache(str(tmp_path), candidates=0)
    cache.disk = 2 * cache.need(state)
    gets = []
    for _ in range(4):
        assert not await cache.fill(io, state)
        gets.append(io.metrics.gets)
    assert gets == [1, 1, 1, 1]  # built once, then known not to fit
    assert cache.need(state) > 2 * EngineCache(str(tmp_path / "fresh")).need(state)  # what it reached
    cache.disk = 2**26  # room now
    assert await cache.fill(io, state) and io.metrics.gets == 2


async def test_maintenance_reads_the_engine_caches_copies(io, tmp_path):
    """One warm copy serves every engine reader: a recount and a
    compaction over the cache's local files read nothing from the store and
    agree with the store's; a copy short of one file reads the store."""

    from solera_server.keyservice import KeyService

    state = await built_index(io, commits=6)
    service = KeyService(io.store, str(tmp_path))
    service.start()
    try:
        assert await asyncio.wrap_future(service._submit(service.cache.fill(service.io, state)))
        with service.open(state) as local_files:
            assert local_files is not None
            cold = await KeyIndex(io, None, state, OPTS).recount()
            gets = io.metrics.gets
            held = ObjectIO(io.store, metrics=io.metrics, local=local_files.handles)
            idx = KeyIndex(held, None, state, OPTS)
            assert await idx.recount() == cold and idx.local_reads and io.metrics.gets == gets
            plan = (state.level(0) + state.level(1), 1) if state.level(0) else (state.level(1), 2)
            local = KeyIndex(held, None, state, OPTS)
            added, _, _ = await local.compact(plan, garbage=True)
            assert local.local_reads and io.metrics.gets == gets
            stored, _, _ = await KeyIndex(io, None, state, OPTS).compact(plan, garbage=True)

            async def read(files):
                return decoded([await io.read_whole(state.path(f.name), f.size) for f in files])

            assert await read(added) == await read(stored)
            partial = ObjectIO(io.store, local=dict(list(local_files.handles.items())[1:]))
            idx = KeyIndex(partial, None, state, OPTS)
            assert await idx.recount() == cold and not idx.local_reads
    finally:
        await service.stop()


# -- engine-served reads (docs/resolved-commits.md §7) ------------------------------------------


async def logged_index(io, seed=3):
    """An index over several levels whose log holds every batch, deletions included."""

    rng = random.Random(seed)
    state = IndexState(prefix="keys/out/_/")
    for b in range(10):
        ks = sorted({key(rng.randrange(2000)) for _ in range(2000 if b == 0 else 200)})
        rm = sorted({key(rng.randrange(2000)) for _ in range(40)} - set(ks)) if b else []
        files, _ = await KeyIndex(io, None, state, OPTS).resolve(
            SortedEntries.of(ks, [rng.randbytes(8) for _ in ks], rm),
            commit_number=b,
            attempt=f"w{b}",
            generation=b + 1,
        )
        state = state.committed(b, files, keep_log=True)
        while (out := await KeyIndex(io, None, state, OPTS).compact()) is not None:
            state = state.compacted(*out[:2])
    return state


async def test_local_reads_are_the_stores(io, tmp_path):
    """Pages, delta passes and lookups over the cache's local copies equal
    the store's, from any cursor."""

    from solera.keys.reads import Cold, Reads

    state = await logged_index(io)
    assert state.depth >= 1 and len(state.log) == 10
    cache = EngineCache(str(tmp_path))
    assert await cache.fill(io, state)
    opened = cache.open_present(state)
    assert len(opened.handles) == len(state.referenced())  # the logged deltas too
    local = ObjectIO(None, local=opened.handles)
    rng = random.Random(4)

    async def walk(read, after, limit):
        """Every entry from `after` on, a page at a time: a page may end early
        with a cursor (the store's does at a file it has not read), never skip."""

        out = []
        while True:
            *entries, after = await read(after, limit)
            assert len(entries[0]) <= limit
            out += list(zip(*entries, strict=True))
            if after is None:
                return out

    for _ in range(12):
        after = rng.choice([None, key(rng.randrange(2100))])
        limit = rng.choice([7, 100, 5000])
        lo = rng.randrange(10)
        hi = rng.randrange(lo, 10)
        cold, warm = KeyIndex(io, None, state, OPTS), KeyIndex(local, None, state, OPTS)
        assert await walk(warm.page, after, limit) == await walk(cold.page, after, limit)
        assert await walk(
            lambda a, n, w=warm, lo=lo, hi=hi: w.pending(lo, hi, a, n), after, limit
        ) == await walk(lambda a, n, c=cold, lo=lo, hi=hi: c.pending(lo, hi, a, n), after, limit)
        probe = [key(rng.randrange(2100)) for _ in range(50)]
        assert await warm.lookup(probe) == await cold.lookup(probe)
    # Recording reads only local copies: one it does not hold is `Cold`.
    gone = state.path(state.files[0].name)  # a file a page reads, not one only the log holds
    held = {p: h for p, h in opened.handles.items() if p != gone}
    partial = ObjectIO(None, local=held, served=Reads(recording=True))
    with pytest.raises(Cold):
        await KeyIndex(partial, None, state, OPTS).page(None, 10)
    opened.close()


async def test_a_record_answers_its_calls_and_nothing_else(io, tmp_path):
    """The engine records, the worker answers from the record: the same
    results with no GET, for the same pinned index and arguments only; a
    record stops at its bounds."""

    from solera.keys.reads import Full, Reads

    state = await logged_index(io)
    cache = EngineCache(str(tmp_path))
    assert await cache.fill(io, state)
    opened = cache.open_present(state)
    reads = Reads(recording=True, max_entries=10**6, max_bytes=2**24)
    engine = KeyIndex(ObjectIO(None, local=opened.handles, served=reads), None, state, OPTS)
    page = await engine.page(None, 300)
    window = await engine.pending(3, 9, None, 300)
    found = await engine.lookup([key(i) for i in range(0, 2100, 9)])
    import json

    served = Reads.from_json(json.loads(json.dumps(reads.to_json())))
    worker_io = ObjectIO(io.store, metrics=io.metrics, served=served)
    gets = io.metrics.gets
    worker = KeyIndex(worker_io, None, state, OPTS)
    assert await worker.page(None, 300) == page
    assert await worker.pending(3, 9, None, 300) == window
    assert await worker.lookup([key(i) for i in range(0, 2100, 9)]) == found
    assert io.metrics.gets == gets
    await worker.page(page[3], 300)  # not recorded: the store
    assert io.metrics.gets > gets
    other = state.slice(3, 9)  # another snapshot: never answered from this one's record
    gets = io.metrics.gets
    assert await KeyIndex(worker_io, None, other, OPTS).pending(3, 9, None, 300) == window
    assert io.metrics.gets > gets
    small = Reads(recording=True, max_entries=100, max_bytes=2**24)
    with pytest.raises(Full):
        await KeyIndex(ObjectIO(None, local=opened.handles, served=small), None, state, OPTS).page(None, 300)
    assert len(small) == 0
    opened.close()


# -- review round 2 ---------------------------------------------------------------------------


async def test_a_build_stops_at_the_room_it_holds(io, tmp_path):
    """Round 2, finding 2: a local file is written as it is built, into the
    room reserved for it; one whose local form outgrows the disk stops there
    — 10K keys with a 4 KiB revision expand to ~39 MB — and its size, at
    least what it reached, is what the next admission counts: no refetch."""

    keys = [key(i) for i in range(10_000)]
    data = _native.encode_file(keys, [0] * len(keys), bytes(len(keys)), payloads=[b"r" * 4096] * len(keys))
    f = FileInfo.describe("c1-0000", 1, data)
    state = IndexState(prefix="keys/out/_/", files=(f,))
    await io.write(state.path(f.name), data)
    cache = EngineCache(str(tmp_path), candidates=0)
    cache.disk = 2 * cache.need(state)
    assert not await cache.fill(io, state)
    assert cache.reserved == 0 and cache.used <= cache.disk and not os.listdir(str(tmp_path))
    gets = io.metrics.gets
    assert not await cache.fill(io, state) and io.metrics.gets == gets
    with pytest.raises(_native.LimitError):
        _native.build_local(data, "x", bytes(16), str(tmp_path / "capped"), 2**20)
    assert os.path.getsize(tmp_path / "capped") <= 2**20


async def test_long_paths_make_short_local_names(io, tmp_path):
    """Round 2, finding 3: a local file is named by a hash of its object's
    path, so a long partition fills like any other."""

    state = IndexState(prefix=f"keys/out/{'s' * 180}/")
    files, _ = await KeyIndex(io, None, state, OPTS).resolve(
        SortedEntries.of([key(i) for i in range(100)], [b"v"] * 100), commit_number=0, attempt="0" * 26
    )
    state = state.committed(0, files, keep_log=False)
    cache = EngineCache(str(tmp_path))
    assert await cache.fill(io, state)
    assert {len(name) for name in os.listdir(str(tmp_path))} == {len("0" * 32 + ".kxl")}
    assert EngineCache(str(tmp_path)).warm(state)  # and found again after a restart


async def test_stopping_waits_for_a_build_in_flight(io, tmp_path, monkeypatch):
    """Round 2, finding 4: `stop` cancels the service's operations and waits
    for each — a fill's build runs on its thread to the end, its room and
    temporary file its own until then."""

    import threading

    from solera.keys import cache as cache_module
    from solera_server.keyservice import KeyService

    state = await built_index(io, commits=1)
    started, go = threading.Event(), threading.Event()
    real = cache_module._native.build_local

    def paused(*args):
        started.set()
        go.wait(5)
        return real(*args)

    monkeypatch.setattr(cache_module._native, "build_local", paused)
    service = KeyService(io.store, str(tmp_path))
    service.start()
    cache = service.cache
    service._submit(cache.fill(service.io, state))
    await asyncio.to_thread(started.wait, 5)
    stopping = asyncio.ensure_future(service.stop())
    await asyncio.sleep(0.1)
    assert not stopping.done() and cache.reserved > 0  # the build still owns its room
    go.set()
    await asyncio.wait_for(stopping, 10)
    assert cache.reserved == 0
    assert not [n for n in os.listdir(str(tmp_path)) if n.endswith(".tmp")]


# -- review round 3 ---------------------------------------------------------------------------


async def test_installs_waiting_are_bounded_in_bytes(io, tmp_path, monkeypatch):
    """Round 3: files waiting to be installed hold their bytes; past the
    queue's bound one is skipped and its index demoted, not kept waiting."""

    import threading

    from solera.keys import cache as cache_module
    from solera_server import keyservice

    monkeypatch.setattr(keyservice, "INSTALL_QUEUE", 4 * 2**20)
    go = threading.Event()
    real = cache_module._native.build_local
    monkeypatch.setattr(cache_module._native, "build_local", lambda *a: (go.wait(5), real(*a))[1])
    state = IndexState(prefix="keys/out/_/")
    service = keyservice.KeyService(io.store, str(tmp_path), disk=2**31)
    service.start()
    try:
        await asyncio.wrap_future(service._submit(asyncio.sleep(0)))
        service.cache.admit(state)
        rng = random.Random(2)
        for i in range(30):
            ks = [key(j) for j in range(2000)]
            data = _native.encode_file(
                ks, [0] * len(ks), bytes(len(ks)), payloads=[rng.randbytes(500) for _ in ks], codec=0
            )
            f = FileInfo.describe(f"c{i:04d}-0000", 1, data)
            service.installed(state.prefix, f, state.path(f.name), data)
            assert service._installing <= keyservice.INSTALL_QUEUE
        await asyncio.wrap_future(service._submit(asyncio.sleep(0)))
        assert not service.cache.indexes[state.prefix].admitted  # skipped ones demoted it
        go.set()
        for _ in range(200):
            if not service._installing:
                break
            await asyncio.sleep(0.02)
        assert service._installing == 0
    finally:
        go.set()
        await service.stop()


def test_admission_finds_the_active_indexes_once(tmp_path):
    """Rounds 3 and 5: admission counts the active indexes' bytes from their
    own totals — never a walk of every cached file — and gets them right."""

    cache = EngineCache(str(tmp_path), disk=50 * 4096 + 2**30 + 8192, candidates=2**30)
    for i in range(50):
        cache._add(_file_stub(f"keys/out/p{i}/f.kx", f"keys/out/p{i}/"))
        cache.indexes[f"keys/out/p{i}/"].admitted = True
        cache.indexes[f"keys/out/p{i}/"].used = cache.clock()

    class Unwalkable(dict):
        def values(self):
            raise AssertionError("admission walked the files")

        items = __iter__ = values

    cache.files = Unwalkable(cache.files)
    assert cache.admit(IndexState(prefix="keys/out/new/"))  # 50 × 4 KiB beside it: fits
    cache.disk = 50 * 4096 + 2**30 - 1
    assert not cache.admit(IndexState(prefix="keys/out/newer/"))  # one byte short


def _file_stub(path, prefix):
    from solera.keys.cache import _File

    return _File(path, path, 4096, prefix, None, 0.0, 4096, "")


async def test_a_local_read_keeps_its_index_active_and_admitted(io, tmp_path):
    """Round 3: reading local copies — a resolve's open files or a start read's —
    marks the index active, and admits one recovered after a restart."""

    state = await built_index(io, commits=1)
    now = [0.0]
    cache = EngineCache(str(tmp_path), window=10, clock=lambda: now[0])
    assert await cache.fill(io, state)
    now[0] = 11.0
    assert state.prefix not in cache._active(now[0])
    with cache.open_present(state):
        pass
    assert state.prefix in cache._active(now[0])
    again = EngineCache(str(tmp_path), window=10, clock=lambda: now[0])
    assert not again.indexes[state.prefix].admitted  # recovered: kept, not admitted
    with again.open_present(state):
        assert again.indexes[state.prefix].admitted


async def test_a_corrupt_copy_recovers_through_start_reads(io, tmp_path):
    """Round 3: a start read that meets a corrupt local copy drops it and
    fills it again, as a cold index; the next attempt is answered."""

    from solera_server.keyservice import KeyService

    state = await built_index(io, commits=1)
    spec = {"inputs": {"x": {"index": state.to_json(), "batch": {"full": True, "after": None, "limit": 50}}}}
    service = KeyService(io.store, str(tmp_path))
    service.start()
    try:
        await asyncio.wrap_future(service._submit(service.cache.fill(service.io, state)))
        assert await service.reads(spec, 0) is not None
        local = service.cache.files[state.path(state.files[0].name)].local
        data = bytearray(open(local, "rb").read())
        data[10] ^= 0xFF
        open(local, "wb").write(bytes(data))
        assert await service.reads(spec, 0) is None
        for _ in range(200):
            if (record := await service.reads(spec, 0)) is not None:
                break
            await asyncio.sleep(0.02)
        assert record is not None  # refetched, warm again
    finally:
        await service.stop()


def test_a_record_refuses_entries_before_encoding(monkeypatch):
    """Round 3: a result past the entry allowance is refused before any of it
    is encoded."""

    from solera.keys import reads as reads_module

    encoded = []
    real = reads_module.encode_file
    monkeypatch.setattr(reads_module, "encode_file", lambda *a, **k: (encoded.append(1), real(*a, **k))[1])
    reads = reads_module.Reads(recording=True, max_entries=10, max_bytes=2**20)
    keys = [key(i) for i in range(11)]
    with pytest.raises(reads_module.Full):
        reads.record("i", "page", (None, 11), (keys, [0] * 11, [None] * 11, None))
    assert not encoded


# -- review round 4 ---------------------------------------------------------------------------


async def test_cancelled_work_keeps_what_it_holds_until_its_thread_ends(io, tmp_path, monkeypatch):
    """Round 4: cancelling a resolve — once, or again and again — leaves its
    semaphore, queued bytes and open files held while its native thread computes;
    with concurrency 1, one thread computes at a time."""

    import threading

    from solera.keys import resolver as resolver_module

    state = await built_index(io, commits=2)
    cache = EngineCache(str(tmp_path))
    await cache.fill(io, state)
    resolver = Resolver(cache, io, OPTS, Limits(concurrency=1))
    go, running, peak = threading.Event(), [0], [0]
    lock = threading.Lock()
    real = resolver_module._native.Snapshot

    class Gated:
        def __init__(self, runs):
            self.inner = real(runs)

        def resolve(self, *args, **kwargs):
            with lock:
                running[0] += 1
                peak[0] = max(peak[0], running[0])
            go.wait(5)
            try:
                return self.inner.resolve(*args, **kwargs)
            finally:
                with lock:
                    running[0] -= 1

    monkeypatch.setattr(resolver_module._native, "Snapshot", Gated)
    runs = [SortedEntries.of([key(i)], [b"v%d" % i]) for i in range(3)]
    tasks = [
        asyncio.ensure_future(resolver.compute(prepared(state), "patch", r, "keys/out/_/x.kx")) for r in runs
    ]
    await asyncio.sleep(0.1)
    for _ in range(3):  # cancelled, and cancelled again
        for t in tasks:
            t.cancel()
        await asyncio.sleep(0.05)
    assert running[0] == 1 and resolver._queued > 0  # the one computing still holds its room
    assert any(f.readers for f in cache.files.values())  # and its open files
    go.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    assert peak[0] == 1 and resolver._queued == 0
    assert not any(f.readers for f in cache.files.values())


@pytest.mark.skipif(not os.path.exists("/proc/self/status"), reason="reads its peak memory from /proc")
def test_a_build_never_expands_a_blocks_keys_at_once(tmp_path):
    """Round 4: keys sharing a long prefix are rebuilt one at a time as the
    local file is written; 4,000 keys of a 32 KiB prefix (17 KB compressed)
    stop at a 1 MiB ceiling without expanding to ~130 MiB first. Measured in
    a process that never held the keys."""

    import subprocess
    import sys

    keys = [b"p" * 32768 + b"%06d" % i for i in range(4000)]
    (tmp_path / "source.kx").write_bytes(
        _native.encode_file(keys, [0] * len(keys), bytes(len(keys)), payloads=[b"v"] * len(keys))
    )
    del keys
    # The peak of this address space (VmHWM): a child's ru_maxrss carries its parent's.
    script = f"""
from solera import _native
def peak():
    return int(next(x for x in open("/proc/self/status") if x.startswith("VmHWM")).split()[1])
data = open({str(tmp_path / "source.kx")!r}, "rb").read()
before = peak()
try:
    _native.build_local(data, "x", bytes(16), {str(tmp_path / "capped")!r}, 2**20)
except _native.LimitError:
    pass
print((peak() - before) // 1024)
"""
    out = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
    grown_mib = int(out.stdout)
    assert grown_mib < 16, grown_mib


async def test_a_page_reads_one_entry_past_itself(io, tmp_path):
    """Round 4: a full page asks for what it lacks and one more, so its
    record holds the page and the one entry that says another follows —
    not a second page."""

    from solera.keys.reads import Reads
    from solera_worker import each

    state = await built_index(io, commits=1)
    cache = EngineCache(str(tmp_path))
    assert await cache.fill(io, state)
    with cache.open_present(state) as opened:
        reads = Reads(recording=True, max_entries=10**6, max_bytes=2**24)
        spec_pin = {"index": state.to_json(), "batch": {"full": True, "after": None, "limit": 100}}
        read = await each.read_batch(spec_pin, ObjectIO(None, local=opened.handles, served=reads))
    assert len(read.upserted) == 100 and read.after is not None
    assert reads.entries == 101


async def test_a_fan_in_whole_read_is_read_ahead_member_by_member(io, tmp_path):
    """Round 4: an AllPartitions input loaded whole pins each member's index;
    the start read pages every one of them, as the worker's whole read will."""

    from solera.keys.reads import Reads
    from solera_server.keyservice import KeyService

    a = await built_index(io, commits=1, seed=1)
    b = await built_index_at(io, IndexState(prefix="keys/other/_/"))
    service = KeyService(io.store, str(tmp_path))
    service.start()
    try:
        for st in (a, b):
            assert await asyncio.wrap_future(service._submit(service.cache.fill(service.io, st)))
        pin = {"refs": {"x": {}, "y": {}}, "load": "data", "indexes": {"x": a.to_json(), "y": b.to_json()}}
        record = Reads.from_json(await service.reads({"inputs": {"members": pin}}, 0))
        assert len(record) == 2  # a page of each member's locators
        as_ref = {"refs": {"x": {}, "y": {}}, "load": "ref"}
        assert await service.reads({"inputs": {"members": as_ref}}, 0) is None
    finally:
        await service.stop()


# -- review round 5 ---------------------------------------------------------------------------


async def test_a_cancel_as_the_work_ends_is_not_lost(monkeypatch):
    """Round 5: a caller cancelled in the turn its thread's work completes
    still sees the cancellation."""

    import time

    from solera.keys import threads

    real = asyncio.to_thread
    owner: list[asyncio.Task] = []

    async def finishing(fn, *args, **kwargs):
        out = await real(fn, *args, **kwargs)
        owner[0].cancel()  # as the work completes, before its caller resumes
        return out

    monkeypatch.setattr(threads.asyncio, "to_thread", finishing)
    owner.append(asyncio.ensure_future(threads.in_thread(time.sleep, 0.01)))
    with pytest.raises(asyncio.CancelledError):
        await owner[0]


async def test_a_closed_streaming_reader_owns_no_fetch(io):
    """Round 5: closing a run's reader cancels and awaits every fetch it
    started, the one waiting for room in its queue too."""

    from solera.keys import jobs

    gate = asyncio.Event()

    class Slow(ObjectIO):
        async def read(self, path, start, end, size):
            await gate.wait()
            return await super().read(path, start, end, size)

    state = await built_index(io, commits=1)
    files = [f for level in state.newest_first() for f in level] * 4  # more segments than the queue holds
    reader = jobs._Run(Slow(io.store), state.path, files)
    await asyncio.sleep(0.05)
    fetches = len(reader.tasks) - 1  # beside its producer
    assert reader.queue.full() and fetches > reader.queue.qsize()  # one waits for room
    await reader.close()
    assert len(reader.tasks) == 0, "its producer and every fetch ended"


async def test_cache_totals_are_kept_not_summed(io, tmp_path):
    """Round 5: what the cache holds is counted as files and candidates come
    and go — a small commit does not sum the whole cache — and the counts
    stay what a full sum gives."""

    state = await built_index(io, commits=3)
    cache = EngineCache(str(tmp_path))
    assert await cache.fill(io, state)

    def recount():
        files = sum(f.size for f in cache.files.values())
        per_index = {p: sum(cache.files[q].size for q in ix.files) for p, ix in cache.indexes.items()}
        return files + sum(c.size for c in cache.candidates.values()) + cache.reserved, per_index

    rng = random.Random(3)
    for i in range(40):
        op = rng.random()
        if op < 0.4:
            cache.offer(f"keys/out/_/cand{i}.kx", rng.randbytes(rng.randrange(1, 5000)))
        elif op < 0.6 and cache.candidates:
            cache.candidates.pop(next(iter(cache.candidates)))
        elif op < 0.8 and cache.files:
            cache.corrupt(rng.choice(list(cache.files)))
        else:
            await cache.fill(io, state)
        total, per_index = recount()
        assert cache.used == total
        assert all(cache.indexes[p].bytes == b for p, b in per_index.items())
    cache.candidates.clear()
    assert cache.used == recount()[0]


async def test_a_page_the_record_cannot_keep_is_never_read(io, tmp_path, monkeypatch):
    """Round 5: a call whose page could not fit what is left of the record
    is refused before anything is scanned or decoded."""

    from solera.keys import index as index_module
    from solera.keys.reads import Full, Reads

    scanned = []
    real = index_module._scan_local

    async def spy(*args):
        scanned.append(args)
        return await real(*args)

    monkeypatch.setattr(index_module, "_scan_local", spy)
    state = await built_index(io, commits=1)
    cache = EngineCache(str(tmp_path))
    assert await cache.fill(io, state)
    with cache.open_present(state) as opened:
        reads = Reads(recording=True, max_entries=1000, max_bytes=2**24)
        idx = KeyIndex(ObjectIO(None, local=opened.handles, served=reads), None, state, OPTS)
        with pytest.raises(Full):
            await idx.page(None, 1100)
        assert not scanned
        small = Reads(recording=True, max_entries=10**6, max_bytes=2**24, max_decoded=100)
        with pytest.raises(Full):  # the decoded ceiling: stopped in the scan, not after it
            await KeyIndex(ObjectIO(None, local=opened.handles, served=small), None, state, OPTS).page(
                None, 500
            )


async def test_start_reads_are_admitted_and_hold_their_room(io, tmp_path, monkeypatch):
    """Round 5: start reads are admitted as resolves are — the resolver's
    concurrency and queue — and one that timed out keeps its place until
    its native work ends."""

    import threading

    from solera.keys import index as index_module
    from solera.keys.threads import in_thread
    from solera_server import keyservice

    monkeypatch.setattr(keyservice, "READS_TIMEOUT", 0.2)
    go, lock, running, peak = threading.Event(), threading.Lock(), [0], [0]
    real = index_module._scan_local

    async def gated(*args):
        def wait():
            with lock:
                running[0] += 1
                peak[0] = max(peak[0], running[0])
            go.wait(5)
            with lock:
                running[0] -= 1

        await in_thread(wait)
        return await real(*args)

    monkeypatch.setattr(index_module, "_scan_local", gated)
    state = await built_index(io, commits=1)
    service = keyservice.KeyService(io.store, str(tmp_path))
    service.start()
    try:
        assert await asyncio.wrap_future(service._submit(service.cache.fill(service.io, state)))
        spec = {
            "inputs": {"x": {"index": state.to_json(), "batch": {"full": True, "after": None, "limit": 50}}}
        }
        outs = await asyncio.gather(*(service.reads(spec, 0) for _ in range(16)))
        assert outs == [None] * 16  # timed out, or turned away
        resolver = service.resolver
        assert peak[0] <= resolver.limits.concurrency  # the resolver's gate: one rule for the cache
        assert 0 < resolver._queued <= resolver.limits.queue_bytes  # still theirs: the threads run
        go.set()
        for _ in range(250):
            if not resolver._queued:
                break
            await asyncio.sleep(0.02)
        assert resolver._queued == 0
    finally:
        go.set()
        await service.stop()


_step = st.one_of(
    st.tuples(st.just("commit"), st.integers(0, 2), st.integers(1, 300)),
    st.tuples(st.just("resolve"), st.integers(0, 2), st.integers(1, 60)),
    st.tuples(st.just("fills"), st.just(0), st.just(0)),
    st.tuples(st.sampled_from(["wipe", "corrupt", "restart"]), st.just(0), st.just(0)),
)


@settings(max_examples=40, deadline=None, suppress_health_check=list(HealthCheck))
@given(
    disk=st.sampled_from([4_000, 40_000, 400_000, 16 * 2**30]),
    steps=st.lists(_step, max_size=25),
    seed=st.integers(0, 99),
)
def test_under_any_budget_and_any_trouble_the_engine_resolves_as_a_cold_reader(
    tmp_path_factory, disk, steps, seed
):
    """Three indexes grow by commits while one engine cache, its disk budget
    from a few files' worth to plenty, serves resolves; its files are
    deleted (a temporary-files cleaner) or corrupted under it, or the cache
    restarts on its directory. Every answer it gives is the cold reader's,
    and it never holds more than its budget."""

    root = str(tmp_path_factory.mktemp("cache"))

    async def run():
        rng = random.Random(seed)
        io = ObjectIO(MemoryStore())
        states = [IndexState(prefix=f"keys/out{i}/_/") for i in range(3)]
        cache = EngineCache(root, disk=disk, candidates=disk // 4)
        resolver = Resolver(cache, io, OPTS)
        commit = 0
        for op, i, n in steps:
            if op == "commit":
                ks = sorted({key(rng.randrange(2000)) for _ in range(n)})
                rm = sorted({key(rng.randrange(2000)) for _ in range(n // 10)} - set(ks))
                files, _ = await KeyIndex(io, None, states[i], OPTS).resolve(
                    SortedEntries.of(ks, [rng.randbytes(4) for _ in ks], rm),
                    commit_number=commit,
                    attempt=f"w{commit}",
                    generation=commit + 1,
                )
                states[i] = states[i].committed(commit, files, keep_log=False)
                while (out := await KeyIndex(io, None, states[i], OPTS).compact()) is not None:
                    states[i] = states[i].compacted(*out[:2])
                commit += 1
            elif op == "resolve" and states[i].files:
                ks = sorted({key(rng.randrange(2000)) for _ in range(n)})
                rm = sorted({key(rng.randrange(2000)) for _ in range(n // 5)} - set(ks))
                vs = [rng.randbytes(4) for _ in ks]
                a = Ask("out", "", "patch", 99, 100, states[i].prefix, 98, SortedEntries.of(ks, vs, rm))
                p = prepared(states[i])
                for _ in range(2):  # declined cold: the fill runs, and the worker asks again
                    body = await resolver.resolve("att", request("inv", [a]), lambda _, p=p: p, lambda: True)
                    answer, delta = answers(body)["out"]
                    if answer.get("reason") != "cold":
                        break
                    await asyncio.gather(*resolver._fills, return_exceptions=True)
                if answer["result"] in ("delta", "empty"):
                    files, _ = await KeyIndex(io, None, states[i], OPTS).resolve(
                        SortedEntries.of(ks, vs, rm),
                        commit_number=99,
                        attempt=f"cold{rng.getrandbits(32)}",
                        generation=100,
                        exact=True,
                    )
                    cold = decoded([await io.read_whole(states[i].path(f.name), f.size) for f in files.files])
                    assert (decoded([delta]) if delta else []) == cold
            elif op == "fills":
                await asyncio.gather(*resolver._fills, return_exceptions=True)
            elif op in ("wipe", "corrupt"):
                for name in os.listdir(root):
                    path = os.path.join(root, name)
                    if op == "wipe":
                        os.unlink(path)
                    elif os.path.getsize(path):
                        with open(path, "r+b") as f:
                            f.seek(os.path.getsize(path) // 2)
                            byte = f.read(1)
                            f.seek(-1, 1)
                            f.write(bytes([byte[0] ^ 0xFF]))
            elif op == "restart":
                await asyncio.gather(*resolver._fills, return_exceptions=True)
                cache = EngineCache(root, disk=disk, candidates=disk // 4)
                resolver = Resolver(cache, io, OPTS)
            assert cache.used <= cache.disk, (cache.used, cache.disk)
        await asyncio.gather(*resolver._fills, return_exceptions=True)

    asyncio.run(run())


async def test_pending_pages_stream_what_pending_pages(io):
    """`pending_pages` reads the commits' files once, through one merge, and
    yields every entry `pending` gives a page at a time, from any cursor, in
    pages of any size: deletions, payloads and the newest generation alike."""

    state = await logged_index(io, seed=5)
    rng = random.Random(6)
    for _ in range(12):
        after = rng.choice([None, key(rng.randrange(2100))])
        limit = rng.choice([1, 7, 100, 5000])
        lo = rng.randrange(10)
        hi = rng.randrange(lo, 10)
        index = KeyIndex(io, None, state, OPTS)
        paged, cursor = [], after
        while True:
            *entries, cursor = await index.pending(lo, hi, cursor, limit)
            paged += list(zip(*entries, strict=True))
            if cursor is None:
                break
        streamed = []
        async for page in KeyIndex(io, None, state, OPTS).pending_pages(lo, hi, after, limit):
            assert len(page[0]) <= limit
            streamed += list(zip(*page, strict=True))
        assert streamed == paged
