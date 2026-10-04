"""The layer prototype (`layers.py`, `native/src/layers.rs`) against the
per-commit fold, and its lifecycle (A17's checklist where it applies).

    uv run pytest bench/keys/fp/test_layers.py -q

Sizes are shrunk (blocks of 128 raw bytes, parts over 512 bytes indexed,
tiers from 64 bytes) so that merges, multi-block parts, indexes, straddled
layers, the base's graveyard and other layers' side parts (keys added and
removed inside) all occur within a few hundred commits.
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
from pathlib import Path

import obstore
import pytest
from obstore.store import MemoryStore
from solera.keys.io import ObjectIO

sys.path.insert(0, str(Path(__file__).parent))
import layers as L  # noqa: E402


@pytest.fixture(autouse=True)
def small(monkeypatch):
    monkeypatch.setattr(L, "BLOCK", 128)
    monkeypatch.setattr(L, "SMALL", 512)
    monkeypatch.setattr(L, "B0", 64)
    monkeypatch.setattr(L, "Z", 256)
    monkeypatch.setattr(L, "FILE_LIMIT", 2048)


def run(c):
    return asyncio.run(c)


def key(i: int) -> bytes:
    return b"k%04d" % i


class Fold:
    def __init__(self):
        self.states: list[dict[bytes, int]] = []  # after each commit: key -> generation

    def at(self, c: int) -> dict[bytes, int]:
        return self.states[c]


async def history(rng: random.Random, commits: int, readers: int, window: int = 0):
    io = ObjectIO(MemoryStore())
    ix = L.Layers(io, "keys/t/", window=window)
    fold = Fold()
    base = sorted(key(i) for i in rng.sample(range(400), 120))
    import pyarrow as pa

    await ix.load_base([pa.array(base, pa.binary())], 1)
    fold.states.append(dict.fromkeys(base, 1))
    positions = [0] * readers  # observed at
    for c in range(1, commits + 1):
        state = dict(fold.states[-1])
        live = sorted(state)
        big = rng.random() < 0.05  # a large commit: its delta gets an index (and read by blocks)
        ups = {key(rng.randrange(400)) for _ in range(rng.randint(100, 200) if big else rng.randint(0, 12))}
        rms = set(rng.sample(live, min(len(live), rng.randint(0, 4)))) - ups
        rms |= {key(rng.randrange(400)) for _ in range(rng.randint(0, 1))} - ups  # removes of absent keys: nothing
        g = c + 1
        await ix.commit(c, g, sorted(ups), sorted(rms))
        for k in ups:
            state[k] = g
        for k in rms:
            state.pop(k, None)
        fold.states.append(state)
        for i in range(readers):
            if rng.random() < 0.1:
                positions[i] = c  # this reader read through c
        await ix.upkeep(min(positions))
        yield ix, fold, c, positions


async def check_reads(ix: L.Layers, fold: Fold, head: int, positions: list[int], rng: random.Random) -> int:
    reader = L.Reader(ix.io, L.State.from_json(ix.s.to_json()))
    now = fold.at(head)
    checks = 0
    for p in sorted(set(positions) | {ix.s.cut, head - 1, None} - {-1}, key=lambda x: -1 if x is None else x):
        if p is not None and (p < ix.s.cut or p < 0):
            continue
        then = {} if p is None else fold.at(p)
        want = {k: (k in then, k in now, now.get(k)) for k in set(then) | set(now) if then.get(k) != now.get(k)}
        got, after = {}, None
        while True:
            keys, ap, ah, st, _, cur = await reader.page(p, after, rng.randint(1, 9))
            for k, a, h, s in zip(keys, ap, ah, st, strict=True):
                assert k not in got
                got[k] = (bool(a), bool(h), s if h else None)
            if cur is None:
                break
            after = cur
        assert got == want, (p, head)
        checks += 1
    probe = sorted({key(rng.randrange(400)) for _ in range(20)})
    found = await reader.lookup(probe)
    for k, f in zip(probe, found, strict=True):
        live = f is not None and f[0]
        assert live == (k in now) and (not live or f[1] == now[k]), k
    return checks


def test_layers_against_the_fold():
    async def go():
        rng = random.Random(57)
        total = 0
        for _ in range(int(os.environ.get("LAYERS_SOAK", "6"))):
            async for ix, fold, c, positions in history(rng, 160, 3):
                if c % 7 == 0:
                    total += await check_reads(ix, fold, c, positions, rng)
        return total

    assert run(go()) > 300


def test_merges_happen_and_side_parts_hold_what_only_straddlers_need():
    async def go():
        rng = random.Random(3)
        seen = {"graveyard": False, "side above the base": False, "index": False, "straddle": False}
        async for ix, fold, c, positions in history(rng, 200, 2):
            base = ix.s.layers[0]
            seen["graveyard"] |= base.side is not None and base.side.entries > 0
            seen["side above the base"] |= any(x.side is not None and x.side.entries > 0 for x in ix.s.layers[1:])
            seen["index"] |= any(x.main.index for x in ix.s.layers)
            seen["straddle"] |= any(x.a <= min(positions) < x.b for x in ix.s.layers if x.stamp is None)
            seen["indexed delta"] = seen.get("indexed delta", False) or any(x.stamp is not None and x.main.index for x in ix.s.layers)
        assert all(seen.values()), seen
        assert ix.written["base"].merges and ix.written["tier"].merges

    run(go())


def test_a_p_below_the_cut_fails_loudly():
    async def go():
        rng = random.Random(5)
        async for ix, fold, c, positions in history(rng, 60, 1):
            pass
        assert ix.s.cut > 0
        with pytest.raises(L.CutError):
            await L.Reader(ix.io, ix.s).page(ix.s.cut - 1, None, 10)

    run(go())


# -- lifecycle ------------------------------------------------------------------------------


async def two_layers(io, epoch=1):
    import pyarrow as pa

    ix = L.Layers(io, "keys/t/", epoch=epoch)
    await ix.load_base([pa.array([key(i) for i in range(50)], pa.binary())], 1)
    for c in range(1, 5):
        await ix.commit(c, c + 1, [key(c)], [])
    return ix


def test_publication_refuses_replaced_inputs_and_another_life():
    async def go():
        io = ObjectIO(MemoryStore())
        ix = await two_layers(io)
        ins = ix.s.layers[1:3]
        out = await ix.merge(1, 2)
        assert out is not None
        # A second merge of the same inputs (a retry, a zombie's) is refused.
        stale = L.Layer("L1-2", 1, 2, L.Part([]))
        assert not ix.publish(ins, stale)
        # A merge from another life is refused.
        assert not ix.publish(ix.s.layers[1:2], stale, life="l1")

    run(go())


def test_pinned_files_outlive_the_merge_that_replaced_them():
    async def go():
        io = ObjectIO(MemoryStore())
        ix = await two_layers(io)
        pinned = ix.pin("batch")
        old = [p for x in pinned.layers for p in x.paths()]
        await ix.merge(0, len(ix.s.layers))
        assert await ix.collect() == []  # the pin predates the publication
        for p in old:
            await obstore.head_async(io.store, p)
        reader = L.Reader(io, pinned)  # the batch still reads its manifest
        keys, *_ = await reader.page(None, None, 100)
        assert len(keys) == 50 + 0  # keys 1..4 were updates of existing keys
        ix.unpin("batch")
        dead = await ix.collect()
        assert set(dead) >= {p for p in old if p not in ix.referenced()}
        for p in dead:
            with pytest.raises(FileNotFoundError):
                await obstore.head_async(io.store, p)

    run(go())


def test_a_fenced_engine_deletes_nothing_and_spares_a_newer_epoch():
    async def go():
        io = ObjectIO(MemoryStore())
        zombie = await two_layers(io, epoch=1)
        # Engine 2 takes over (epoch 2) from the durable state and merges.
        live = L.Layers(io, "keys/t/", epoch=2, state=L.State.from_json(zombie.s.to_json()))
        await live.merge(1, 3)
        # The zombie, unaware, merges the base with everything into its own
        # model: its garbage is files the durable state still names.
        named = live.referenced()
        await zombie.merge(0, len(zombie.s.layers))
        assert set(zombie.deletable()) & named

        async def fenced():
            raise RuntimeError("fenced: the journal swap failed")

        # Its collector's barrier fails, so it deletes nothing.
        with pytest.raises(RuntimeError):
            await zombie.collect(barrier=fenced)
        for p in named:
            await obstore.head_async(io.store, p)
        # Its orphan collector, judging by epochs, spares the newer engine's
        # outputs, which its model does not name; it may delete its own
        # unpublished leftovers.
        listing = [o["path"] for o in obstore.list(io.store, prefix="keys/t/").collect()]
        dead = await zombie.collect_orphans(listing)
        newer = [p for x in live.s.layers for p in x.paths() if "-e2-" in p]
        assert newer and not set(newer) & set(dead)
        assert all("-e1-" in p for p in dead)
        for p in newer:
            await obstore.head_async(io.store, p)

    run(go())


def test_a_failing_merge_stops_after_three_attempts():
    async def go():
        io = ObjectIO(MemoryStore())
        ix = await two_layers(io)
        calls = 0

        async def failing(lo, count):
            nonlocal calls
            calls += 1
            ins = ix.s.layers[lo : lo + count]
            k = ix._key(ins)
            ix.s.attempts[k] = ix.s.attempts.get(k, 0) + 1
            raise RuntimeError("upload failed")

        ix.merge = failing
        for _ in range(5):
            try:
                await ix.upkeep(0)
            except RuntimeError:
                pass
        # The counts are in the state: a successor engine sees them too.
        successor = L.Layers(io, "keys/t/", epoch=2, state=L.State.from_json(ix.s.to_json()))
        assert calls == 3
        assert successor.s.stopped == [ix._key(ix.s.layers[1:5])]

    run(go())


def test_globs_match_soleras_matcher():
    import re

    from solera import _native
    from solera.patterns import glob_regex

    rng = random.Random(25)
    alphabet = ["a", "b", "/"]
    parts = ["a", "b", "/", "*", "?", "**", "**/"]
    for _ in range(600):
        g = "".join(rng.choice(parts) for _ in range(rng.randint(1, 5)))
        keys = sorted({"".join(rng.choice(alphabet) for _ in range(rng.randint(0, 6))) for _ in range(40)})
        want = [bool(re.fullmatch(glob_regex(g), k)) for k in keys]
        got = list(_native.glob_match(g.encode(), [k.encode() for k in keys]))
        assert [bool(x) for x in got] == want, g
    # A25's cases: a short match between longer bounds, and a terminal **.
    assert list(_native.glob_match(b"?", [b"aa", b"b", b"ca"])) == [0, 1, 0]
    assert list(_native.glob_match(b"tenant/**", [b"tenant/a/y"])) == [1]


def test_full_scan_with_a_pattern():
    async def go():
        rng = random.Random(9)
        async for ix, fold, c, positions in history(rng, 120, 1):
            pass
        now = fold.at(c)
        r = L.Reader(ix.io, L.State.from_json(ix.s.to_json()))
        for pat, prefix in ((b"*1*", None), (b"k00*", b"k00"), (b"*template*", None)):
            want = sum(1 for k in now if _native_match(pat, k))
            got = await r.scan_all(pat, prefix=prefix, slice_bytes=256)
            assert got["matches"] == want, (pat, got, want)

    run(go())


def _native_match(pat: bytes, key: bytes) -> bool:
    import re

    from solera.patterns import glob_regex

    return bool(re.fullmatch(glob_regex(pat.decode()), key.decode()))
