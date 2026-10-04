"""Δ(P, H) for EVERY P from the cut to the head (not only the readers'
positions), paged with random limits, plus Δ(−∞, H), against the per-commit
fold, on the df9ac17 kernels built for this review. Two workloads: the test's
own mix (indexed deltas included) and a churn mix (fresh keys added and
removed repeatedly). Also checks query form 3 (lookup with P) and reports
how often it disagrees with the fold."""
import os
from common import *

async def history(rng, commits, readers, churn=False, space=400):
    io = MemIO(); ix = L.Layers(io, "t/")
    base = sorted(key(i) for i in rng.sample(range(space), 120))
    await load(ix, base)
    states = [dict.fromkeys(base, 1)]
    positions = [0] * readers
    for c in range(1, commits + 1):
        st = dict(states[-1]); live = sorted(st)
        if churn:
            fresh = {key(space + c * 10 + j) for j in range(rng.randint(2, 5))}   # new temporary keys
            ups = fresh | {key(rng.randrange(space)) for _ in range(rng.randint(0, 4))}
            # remove some recently added temporaries and some old keys
            temps = [k for k in live if int(k[1:]) >= space]
            rms = set(rng.sample(temps, min(len(temps), rng.randint(1, 4)))) | set(rng.sample(live, min(len(live), rng.randint(0, 2))))
            rms -= ups
        else:
            big = rng.random() < 0.05
            ups = {key(rng.randrange(space)) for _ in range(rng.randint(100, 200) if big else rng.randint(0, 12))}
            rms = set(rng.sample(live, min(len(live), rng.randint(0, 4)))) - ups
            rms |= {key(rng.randrange(space)) for _ in range(rng.randint(0, 1))} - ups
        g = c + 1
        await ix.commit(c, g, sorted(ups), sorted(rms))
        for k in ups: st[k] = g
        for k in rms: st.pop(k, None)
        states.append(st)
        for i in range(readers):
            if rng.random() < 0.1: positions[i] = c
        await ix.upkeep(min(positions))
        yield ix, states, c, positions

async def check_all(ix, states, head, rng, stats):
    r = L.Reader(ix.io, L.State.from_json(ix.s.to_json()))
    now = states[head]
    for p in [None] + list(range(max(ix.s.cut, 0), head + 1)):
        then = {} if p is None else states[p]
        want = {k: (k in then, k in now, now.get(k)) for k in set(then) | set(now) if then.get(k) != now.get(k)}
        got, after = {}, None
        while True:
            keys, ap, ah, stm, _, cur = await r.page(p, after, rng.randint(1, 9))
            for k, a, h, s in zip(keys, ap, ah, stm):
                assert k not in got, ("duplicate", k, p, head)
                got[k] = (bool(a), bool(h), s if h else None)
            if cur is None: break
            after = cur
        if got != want:
            stats["page_mismatch"] += 1
            if stats["page_mismatch"] <= 3:
                print("PAGE MISMATCH", p, head, {k: (want.get(k), got.get(k)) for k in set(want) ^ set(got) or [k for k in want if want[k] != got.get(k)]})
        stats["page_checks"] += 1
        if p is not None:
            # query form 3: lookup with P, over the keys that changed (want) plus some unchanged
            probe = sorted(set(want) | {key(rng.randrange(600)) for _ in range(10)})
            found = await r.lookup(probe, p=p)
            for k, f in zip(probe, found):
                changed = k in want
                says_changed = f is not None
                if changed != says_changed or (changed and f is not None and (bool(f[0]) != want[k][1])):
                    stats["lookup_p_mismatch"] += 1
                    stats["lookup_p_examples"].add((want.get(k), None if f is None else bool(f[0])))
                stats["lookup_p_checks"] += 1
    stats["straddled"] += any(x.a <= (ix.s.cut if ix.s.cut >= 0 else 0) < x.b and x.stamp is None for x in ix.s.layers)

def main():
    hist = int(os.environ.get("A31_HIST", "6"))
    for churn in (False, True):
        rng = random.Random(31 if churn else 58)
        stats = dict(page_checks=0, page_mismatch=0, lookup_p_checks=0, lookup_p_mismatch=0, lookup_p_examples=set(), straddled=0)
        async def go():
            for _ in range(hist):
                async for ix, states, c, positions in history(rng, 120, 3, churn=churn):
                    if c % 6 == 0:
                        await check_all(ix, states, c, rng, stats)
            return ix
        ix = run(go())
        side = sum(1 for x in ix.s.layers if x.side and x.side.entries)
        print(f"{'churn' if churn else 'base '} workload: page Δ checks {stats['page_checks']} (every P from cut to head), mismatches {stats['page_mismatch']}; "
              f"lookup-with-P checks {stats['lookup_p_checks']}, mismatches {stats['lookup_p_mismatch']} {sorted(stats['lookup_p_examples'], key=str)}; "
              f"final layers {len(ix.s.layers)}, with side parts {side}, cut {ix.s.cut}")

if __name__ == "__main__":
    main()
