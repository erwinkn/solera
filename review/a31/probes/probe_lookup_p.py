"""Query form 3 with a P: Δ(P, H, sorted keys) through Reader.lookup(keys, p).
History 1: k in the base; a reader observes at commit 5 (k present); commit 7
removes k; the base absorbs everything with the cut at 5 (k goes to the
graveyard, the base's side part). History 2: k added at 2, observed at 3,
removed at 4, layers [1, 6] merged (k absent at both ends: the side part).
In both, page(P) reports k removed; lookup(keys, p) reports it unchanged."""
from common import *

async def h1():
    io = MemIO(); ix = L.Layers(io, "t/")
    await load(ix, [key(i) for i in range(40)])
    for c in range(1, 11):
        ups = [key(100 + c)]  # fresh keys, to grow the layers
        rms = [key(0)] if c == 7 else []
        await ix.commit(c, c + 1, ups, rms)
    ix.set_cut(5, ix.s.head)
    out = await ix.merge(0, len(ix.s.layers))
    assert out is not None and len(ix.s.layers) == 1 and ix.s.layers[0].side is not None
    r = L.Reader(io, ix.s)
    keys, ap, ah, *_ = await r.page(5, None, 1000)
    page_says = {k: (bool(a), bool(h)) for k, a, h in zip(keys, ap, ah)}
    look = await r.lookup([key(0)], p=5)
    print("H1 base graveyard: page(5) says k0 ->", page_says.get(key(0)), "; lookup([k0], p=5) ->", look[0])
    return page_says.get(key(0)) == (True, False) and look[0] is None

async def h2():
    io = MemIO(); ix = L.Layers(io, "t/")
    await load(ix, [key(i) for i in range(40)])
    k = key(500)
    for c in range(1, 7):
        ups = [key(100 + c)] + ([k] if c == 2 else [])
        rms = [k] if c == 4 else []
        await ix.commit(c, c + 1, ups, rms)
    ix.set_cut(0, ix.s.head)
    out = await ix.merge(1, 6)  # [1, 6] above the base; k absent at both ends -> side
    assert out is not None and out.side is not None and out.side.entries >= 1
    r = L.Reader(io, ix.s)
    keys, ap, ah, *_ = await r.page(3, None, 1000)
    page_says = {kk: (bool(a), bool(h)) for kk, a, h in zip(keys, ap, ah)}
    look = await r.lookup([k], p=3)
    print("H2 straddler side part: page(3) says k ->", page_says.get(k), "; lookup([k], p=3) ->", look[0])
    return page_says.get(k) == (True, False) and look[0] is None

if __name__ == "__main__":
    a, b = run(h1()), run(h2())
    print("REPRODUCED" if a and b else "not reproduced", "(lookup with P misses removals held in side parts)")
