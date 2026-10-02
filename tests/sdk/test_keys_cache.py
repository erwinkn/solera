"""The engine cache and the resolver's core (docs/resolved-commits.md §4–§5):
engine-resolved deltas against the cold readers', the declines, deduplication,
admission, integrity, candidates and reservations."""

import asyncio
import os
import random

import pytest
from obstore.store import MemoryStore
from solera import _native
from solera.keys import _python
from solera.keys.cache import EngineCache
from solera.keys.index import IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO
from solera.keys.resolver import Ask, Limits, Prepared, Resolver, answers, request

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


def run_file(keys, versions, removes=()):
    entries = sorted(
        [(k, v, 0) for k, v in zip(keys, versions, strict=True)] + [(k, b"", 1) for k in removes]
    )
    return _native.encode_file([e[0] for e in entries], [e[1] for e in entries], bytes(e[2] for e in entries))


async def built_index(io, n=3000, commits=12, seed=1):
    """An index over several levels, as random commits and compactions leave it."""

    rng = random.Random(seed)
    state = IndexState(prefix="keys/out/_/")
    for b in range(commits):
        ks = sorted({key(rng.randrange(n)) for _ in range(n if b == 0 else 300)})
        rm = sorted({key(rng.randrange(n)) for _ in range(30)} - set(ks)) if b else []
        files, _ = await KeyIndex(io, None, state, OPTS).resolve(
            ks, [rng.randbytes(8) for _ in ks], rm, batch=b, attempt=f"w{b}", generation=b + 1
        )
        state = state.committed(b, files, keep_log=False)
        while (out := await KeyIndex(io, None, state, OPTS).compact()) is not None:
            state = state.compacted(*out[:2])
    return state


def prepared(state, batch=99, generation=100, replace=True):
    return Prepared(scope="", batch=batch, generation=generation, index=state, head_batch=98, replace=replace)


def ask(state, keys, versions, removes=(), kind="patch", batch=99, generation=100):
    return Ask(
        "out", "", kind, batch, generation, state.prefix, 98, run_file(keys, versions, removes), len(keys)
    )


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
            files, _ = await idx.resolve(ks, vs, rm, batch=99, attempt=f"c{step}", generation=100, exact=True)
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


async def test_corruption_is_refetched(io, tmp_path):
    state = await built_index(io, commits=2)
    cache = EngineCache(str(tmp_path))
    resolver = Resolver(cache, io, OPTS)
    await cache.fill(io, state)
    path = state.path(state.files[0].name)
    local = cache.files[path].local
    data = bytearray(open(local, "rb").read())
    data[-5] ^= 0xFF  # a block
    open(local, "wb").write(bytes(data))
    a = ask(state, [key(i) for i in range(0, 3000, 7)], [b"x"] * 429)
    assert (await engine_answer(resolver, state, a))[0]["reason"] == "cold"
    await asyncio.gather(*resolver._fills)
    assert (await engine_answer(resolver, state, a))[0]["result"] == "delta"
    # A corrupted directory is refused on open, as after a restart.
    data = bytearray(open(cache.files[path].local, "rb").read())
    data[40] ^= 0xFF
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
    data[40] ^= 0xFF  # one directory goes bad while the engine is down
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
