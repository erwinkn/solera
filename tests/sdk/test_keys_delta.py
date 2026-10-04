"""Δ(P, H, keys) over today's index (solera.keys.delta): every key whose
state differs between P and H, with its presence at both and its version at
H — checked key by key against a per-commit fold: named keys, and ranges
read a page at a time, with and without a pattern filter, from −∞ and from
a commit, to the head and to a pinned head, across merges that keep the
endpoints asked for."""

import random

import pytest
from solera.keys.delta import Diff, delta

from .key_history import History


async def walk(index, p, h, first, take=None) -> list[Diff]:
    """A range read a page at a time, `first` differences a page."""

    got, after = [], None
    while True:
        page = await delta(index, p, h, after=after, first=first, take=take)
        assert len(page.diffs) <= first
        got += page.diffs
        if page.cursor is None:
            return got
        after = page.cursor


def k(i: int) -> str:
    return f"k{i:03d}"


@pytest.mark.parametrize("seed", range(6))
async def test_delta_agrees_with_the_fold(seed):
    rng = random.Random(seed)
    h = History()
    for _ in range(12):
        live = list(h.states.get(h.commit_number - 1, {}))
        upserts = [k(rng.randrange(60)) for _ in range(rng.randrange(1, 25))]
        removes = rng.sample(live, min(len(live), rng.randrange(0, 6)))
        await h.commit(upserts, removes)
    head, idx = h.commit_number - 1, h.index()
    take = (lambda key: key.endswith(("1", "5"))) if seed % 2 else None
    for p in [None, *range(head)]:
        for at in sorted({head, rng.randrange(0 if p is None else p + 1, head + 1)}):
            pinned = None if at == head else at
            assert [
                d for d in (await delta(idx, p, pinned, keys=[k(i) for i in range(60)], take=take)).diffs
            ] == [d for d in h.expected(p, at, [k(i) for i in range(60)], take)], (p, at)
            assert await walk(idx, p, pinned, rng.choice([1, 3, 1000]), take) == h.expected(
                p, at, take=take
            ), (p, at)


async def test_delta_reads_across_merges_that_keep_its_endpoints():
    h = History()
    rng = random.Random(7)
    for _ in range(10):
        await h.commit([k(rng.randrange(40)) for _ in range(8)], [k(rng.randrange(40)) for _ in range(2)])
    p = 4
    await h.merge_all({p + 1})  # a reader observed at commit 4 keeps commit 5 an endpoint
    assert len(h.state.spans) < 10
    head = h.commit_number - 1
    assert await walk(h.index(), p, None, 7) == h.expected(p, head)
    assert await walk(h.index(), None, None, 7) == h.expected(None, head)


async def test_an_empty_delta():
    h = History()
    await h.commit([k(1)])
    assert (await delta(h.index(), 0, None, keys=[k(1)])).diffs == []  # P is the head: nothing differs
    assert (await delta(h.index(), None, None, keys=[k(2)])).diffs == []  # absent at both ends
