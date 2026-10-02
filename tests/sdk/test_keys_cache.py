"""The engine cache and the resolver's core (docs/resolved-commits.md §4–§5):
engine-resolved deltas against the cold readers', the declines, deduplication,
admission, integrity, candidates and reservations."""

import asyncio
import os
import random
import struct
import zlib

import pytest
from obstore.store import MemoryStore
from solera import _native
from solera.keys import FOOTER_SIZE, SortedRun, _python
from solera.keys.cache import Corrupt, EngineCache
from solera.keys.index import FileInfo, IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO
from solera.keys.resolver import Ask, Limits, Malformed, Prepared, Resolver, answers, frame, request, unframe

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
            SortedRun.of(ks, [rng.randbytes(8) for _ in ks], rm), batch=b, attempt=f"w{b}", generation=b + 1
        )
        state = state.committed(b, files, keep_log=False)
        while (out := await KeyIndex(io, None, state, OPTS).compact()) is not None:
            state = state.compacted(*out[:2])
    return state


def prepared(state, batch=99, generation=100, replace=True):
    return Prepared(scope="", batch=batch, generation=generation, index=state, head_batch=98, replace=replace)


def ask(state, keys, versions, removes=(), kind="patch", batch=99, generation=100):
    run = SortedRun.of(list(keys), list(versions), list(removes))
    return Ask("out", "", kind, batch, generation, state.prefix, 98, run)


async def engine_answer(resolver, state, a, invocation="inv", p=None, live=True):
    body = await resolver.resolve(
        "att", request(invocation, [a]), lambda name: p or prepared(state), lambda: live
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
    current = dict(zip(*(await KeyIndex(io, None, state, OPTS).page(None, 10**6))[:2], strict=False))
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
                SortedRun.of(ks, vs, rm), batch=99, attempt=f"c{step}", generation=100, exact=True
            )
        cold = decoded([await io.read_whole(state.path(f.name), f.size) for f in files.files])
        if answer["result"] == "empty":
            assert cold == [] and files.added == files.removed == 0
        else:
            assert answer["result"] == "delta"
            assert decoded([delta]) == cold  # entries, locators and predecessors
            assert (answer["added"], answer["removed"]) == (files.added, files.removed)


async def test_declines(io, tmp_path):
    state = await built_index(io, commits=3)
    cache = EngineCache(str(tmp_path))
    resolver = Resolver(cache, io, OPTS, Limits(max_keys=50, max_entries=10))
    assert await cache.fill(io, state)
    a = ask(state, [key(1)], [b"x"])
    reason = lambda ans: ans[0].get("reason")  # noqa: E731
    assert reason(await engine_answer(resolver, state, a, live=False)) == "not_live"
    assert reason(await engine_answer(resolver, state, ask(state, [key(1)], [b"x"], batch=7))) == "invalid"
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

    good = _native.encode_file([b"a"], [b"good"], b"\x00")
    evil = _native.encode_file([b"a"], [b"evil"], b"\x00")
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


async def test_committed_deltas_are_verified_before_summaries(io, tmp_path):
    from solera_server.keyservice import KeyService

    good = _native.encode_file([b"a"], [b"good"], b"\x00")
    evil = _native.encode_file([b"a"], [b"evil"], b"\x00")
    f = FileInfo.describe("000000000001-x.0000", 0, good)
    service = KeyService(io.store, str(tmp_path))
    service.start()
    try:
        path = f"keys/out/_/{f.name}.kx"
        await io.write(path, evil)
        service.committed("keys/out/_/", lambda n: f"keys/out/_/{n}.kx", 1, [f], True, 0)
        await asyncio.sleep(0.2)
        assert service.inline("keys/out/_/", 1, 1, None, 10) is None  # no summary of the wrong bytes
        assert service.floor() == float("inf")  # its reader pin went with it
    finally:
        await service.stop()


async def test_the_disk_budget_holds_against_the_real_size(io, tmp_path):
    """Review 2: 10,000 keys with one long repeated revision compress to almost
    nothing; their local form is many times the estimate. The build is not
    written if the room for its real size cannot be had."""

    keys = [key(i) for i in range(10_000)]
    data = _native.encode_file(keys, [b"r" * 256] * len(keys), bytes(len(keys)))
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
    published, stays while readers still pin the snapshot it replaces; the
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
    with cache.pin(state):
        assert all(path in cache.files for path, _, _ in written)
        cache.retire([state.path(n) for n in removed])
        assert all(state.path(n) in cache.files for n in removed)  # still read
    assert not any(state.path(n) in cache.files for n in removed)  # gone with the last reader
    assert cache.warm(state.compacted(added, removed))


async def test_a_background_fill_is_a_reader_pin(io, tmp_path, monkeypatch):
    """Review 8: a fill started by a cold decline holds the position it read
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
        p = Prepared("", 99, 100, state, 98, True, position=7)
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


def test_inline_pages_are_capped_as_serialized(tmp_path):
    """Review 11: the cap counts the page as the spec serializes it — JSON
    escapes included — and the cursor."""

    import json

    from solera_server import keyservice

    service = keyservice.KeyService(None, str(tmp_path))
    keys = sorted((f"ключ-{i:05d}-" + "€" * 40).encode() for i in range(5000))
    service._summarize("p/", 1, [_native.encode_file(keys, [b"v" * 16] * len(keys), bytes(len(keys)))])
    page = service.inline("p/", 1, 1, None, 5000)
    assert page is not None and page["next"] is not None
    assert len(json.dumps(page)) <= keyservice.INLINE_BYTES


# -- the keys review, round 2 -------------------------------------------------------------------


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


def test_summaries_hold_runs_and_merge_only_the_page(tmp_path):
    """Round 2, finding 7: a summary is its delta's sorted runs, accounted at
    the bytes they hold, and replacing one gives back its size; a page
    merges from its cursor, newest batch winning, deletions kept."""

    from solera_server.keyservice import KeyService

    service = KeyService(None, str(tmp_path))
    rng = random.Random(9)
    model: dict[bytes, tuple] = {}  # key -> (version, deleted, locator), newest batch winning
    for b in range(4):
        ks = sorted({key(rng.randrange(400)) for _ in range(150)})
        dels = bytes(rng.random() < 0.2 for _ in ks)
        vs = [b"" if d else rng.randbytes(16) for d in dels]
        service._summarize("p/", b, [_native.encode_file(ks, vs, dels, locators=[b] * len(ks))])
        model.update((k, (v, d, b)) for k, v, d in zip(ks, vs, dels, strict=True))
    held = service._summary_size
    assert held == sum(r.nbytes for runs, _ in service.summaries.values() for r in runs)
    service._summarize("p/", 9, [_native.encode_file(sorted(model), [b"v"] * len(model), bytes(len(model)))])
    service._summarize("p/", 9, [_native.encode_file([b"x"], [b"v"], b"\x00")])  # batch 9 again
    assert service._summary_size == held + service.summaries[("p/", 9)][1]
    order = sorted(model)
    after = order[100].decode()
    page = service.inline("p/", 0, 3, after, 50)
    want = order[101:151]
    assert page["next"] == want[-1].decode()
    assert page["deleted"] == [k.decode() for k in want if model[k][1]]
    assert page["upserted"] == {k.decode(): [model[k][0].hex(), model[k][2]] for k in want if not model[k][1]}


async def test_admission_remembers_what_a_file_built_to(io, tmp_path):
    """Round 2, finding 8: a file whose local form proved too big for the
    disk is not fetched and built again at the next fill; more room, and it
    is."""

    keys = [key(i) for i in range(10_000)]
    data = _native.encode_file(keys, [b"r" * 256] * len(keys), bytes(len(keys)))
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
        pin = await asyncio.to_thread(service.pinned, state)
        assert pin is not None
        cold = await KeyIndex(io, None, state, OPTS).recount()
        gets = io.metrics.gets
        held = ObjectIO(io.store, metrics=io.metrics, local=pin.handles)
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
        partial = ObjectIO(io.store, local=dict(list(pin.handles.items())[1:]))
        idx = KeyIndex(partial, None, state, OPTS)
        assert await idx.recount() == cold and not idx.local_reads
        service.unpin(pin)
    finally:
        await service.stop()


# -- engine-served reads (docs/resolved-commits.md §7.1) ------------------------------------------


async def logged_index(io, seed=3):
    """An index over several levels whose log holds every batch, deletions included."""

    rng = random.Random(seed)
    state = IndexState(prefix="keys/out/_/")
    for b in range(10):
        ks = sorted({key(rng.randrange(2000)) for _ in range(2000 if b == 0 else 200)})
        rm = sorted({key(rng.randrange(2000)) for _ in range(40)} - set(ks)) if b else []
        files, _ = await KeyIndex(io, None, state, OPTS).resolve(
            SortedRun.of(ks, [rng.randbytes(8) for _ in ks], rm), batch=b, attempt=f"w{b}", generation=b + 1
        )
        state = state.committed(b, files, keep_log=True)
        while (out := await KeyIndex(io, None, state, OPTS).compact()) is not None:
            state = state.compacted(*out[:2])
    return state


async def test_local_reads_are_the_stores(io, tmp_path):
    """Pages, pending windows and lookups over the cache's local copies equal
    the store's, from any cursor."""

    from solera.keys.reads import Cold, Reads

    state = await logged_index(io)
    assert state.depth >= 1 and len(state.log) == 10
    cache = EngineCache(str(tmp_path))
    assert await cache.fill(io, state)
    pin = cache.held(state)
    assert len(pin.handles) == len(state.referenced())  # the logged deltas too
    local = ObjectIO(None, local=pin.handles)
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
    partial = ObjectIO(None, local=dict(list(pin.handles.items())[1:]), served=Reads(recording=True))
    with pytest.raises(Cold):
        await KeyIndex(partial, None, state, OPTS).page(None, 10)
    pin.__exit__(None, None, None)


async def test_a_record_answers_its_calls_and_nothing_else(io, tmp_path):
    """The engine records, the worker answers from the record: the same
    results with no GET, for the same pinned index and arguments only; a
    record stops at its bounds."""

    from solera.keys.reads import Full, Reads

    state = await logged_index(io)
    cache = EngineCache(str(tmp_path))
    assert await cache.fill(io, state)
    pin = cache.held(state)
    reads = Reads(recording=True, max_entries=10**6, max_bytes=2**24)
    engine = KeyIndex(ObjectIO(None, local=pin.handles, served=reads), None, state, OPTS)
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
    other = state.pinned(3, 9)  # another snapshot: never answered from this one's record
    gets = io.metrics.gets
    assert await KeyIndex(worker_io, None, other, OPTS).pending(3, 9, None, 300) == window
    assert io.metrics.gets > gets
    small = Reads(recording=True, max_entries=100, max_bytes=2**24)
    with pytest.raises(Full):
        await KeyIndex(ObjectIO(None, local=pin.handles, served=small), None, state, OPTS).page(None, 300)
    assert len(small) == 0
    pin.__exit__(None, None, None)


# -- review round 2 ---------------------------------------------------------------------------


async def test_a_build_stops_at_the_room_it_holds(io, tmp_path):
    """Round 2, finding 2: a local file is written as it is built, into the
    room reserved for it; one whose local form outgrows the disk stops there
    — 10K keys with a 4 KiB revision expand to ~39 MB — and its size, at
    least what it reached, is what the next admission counts: no refetch."""

    keys = [key(i) for i in range(10_000)]
    data = _native.encode_file(keys, [b"r" * 4096] * len(keys), bytes(len(keys)))
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
    path, so a long scope fills like any other."""

    state = IndexState(prefix=f"keys/out/{'s' * 180}/")
    files, _ = await KeyIndex(io, None, state, OPTS).resolve(
        SortedRun.of([key(i) for i in range(100)], [b"v"] * 100), batch=0, attempt="0" * 26
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
            data = _native.encode_file(ks, [rng.randbytes(500) for _ in ks], bytes(len(ks)), codec=0)
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


def test_admission_finds_the_active_indexes_once(tmp_path, monkeypatch):
    """Round 3: admission computes the active set once, not once per file."""

    cache = EngineCache(str(tmp_path))
    calls = []
    real = cache._active
    monkeypatch.setattr(cache, "_active", lambda now: (calls.append(now), real(now))[1])
    for i in range(50):
        cache._add(_file_stub(f"keys/out/p{i}/f.kx", f"keys/out/p{i}/"))
        cache.indexes[f"keys/out/p{i}/"].admitted = True
    calls.clear()
    cache.admit(IndexState(prefix="keys/out/new/"))
    assert len(calls) == 1


def _file_stub(path, prefix):
    from solera.keys.cache import _File

    return _File(path, path, 4096, prefix, None, 0.0, 4096, "")


async def test_a_local_read_keeps_its_index_active_and_admitted(io, tmp_path):
    """Round 3: reading local copies — a resolve's pin or a start read's —
    marks the index active, and admits one recovered after a restart."""

    state = await built_index(io, commits=1)
    now = [0.0]
    cache = EngineCache(str(tmp_path), window=10, clock=lambda: now[0])
    assert await cache.fill(io, state)
    now[0] = 11.0
    assert state.prefix not in cache._active(now[0])
    with cache.held(state):
        pass
    assert state.prefix in cache._active(now[0])
    again = EngineCache(str(tmp_path), window=10, clock=lambda: now[0])
    assert not again.indexes[state.prefix].admitted  # recovered: kept, not admitted
    with again.held(state):
        assert again.indexes[state.prefix].admitted


async def test_a_corrupt_copy_recovers_through_start_reads(io, tmp_path):
    """Round 3: a start read that meets a corrupt local copy drops it and
    fills it again, as a cold index; the next attempt is answered."""

    from solera_server.keyservice import KeyService

    state = await built_index(io, commits=1)
    spec = {
        "inputs": {"x": {"index": state.to_json(), "changes": {"full": True, "after": None, "limit": 50}}}
    }
    service = KeyService(io.store, str(tmp_path))
    service.start()
    try:
        await asyncio.wrap_future(service._submit(service.cache.fill(service.io, state)))
        assert await service.reads(spec, set(), 0) is not None
        local = service.cache.files[state.path(state.files[0].name)].local
        data = bytearray(open(local, "rb").read())
        data[10] ^= 0xFF
        open(local, "wb").write(bytes(data))
        assert await service.reads(spec, set(), 0) is None
        for _ in range(200):
            if (record := await service.reads(spec, set(), 0)) is not None:
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
        reads.record("i", "page", (None, 11), (keys, [b"v"] * 11, [0] * 11, None))
    assert not encoded
