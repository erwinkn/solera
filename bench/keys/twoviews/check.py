"""Two views over one log (docs/key-index-two-views.md): an exhaustive-ish check
of the time view's algebra, its retention rule, the read-ahead rule over the key
view, and the digest that detects a view drifting from the log.

    python3 bench/keys/twoviews/check.py [--histories 3000] [--seed 1]

Small random histories (6 keys, up to 24 commits, payload-bearing or derived),
compared with the per-commit fold:

- `changes(P, N)` from the time view (aligned nodes of fanout b = 2, 3 or 4, up
  to level L, absent-to-absent entries pruned) equals the fold's net classes and
  delivered generations, for every 0 <= P <= N <= head;
- with endpoints born at head + 1 and retiring at random, the nodes the
  retention rule keeps (each endpoint's ascending and descending chains, the
  top-level nodes between) answer every query an endpoint can make, at every
  head, and a node it drops is never needed again;
- a read-ahead key classed against the key view at N matches the fold;
- every node satisfies the digest identity (sum over its entries of h(after) -
  h(before) equals D(b) - D(a - 1), D the live-state digest the writer keeps per
  commit), and a corrupted node (an entry dropped, a before or after state
  altered) breaks it.
"""

from __future__ import annotations

import argparse
import hashlib
import random
from dataclasses import dataclass

MASK = (1 << 64) - 1
ABSENT = (False, 0, None)  # (live, generation, payload)


def h(key: str, state) -> int:
    live, gen, payload = state
    if not live:
        return 0
    d = hashlib.blake2b(f"{key}|{gen}|{payload}".encode(), digest_size=8).digest()
    return int.from_bytes(d, "little")


@dataclass
class History:
    states: list[dict]  # state after commit c: key -> (live, gen, payload)
    deltas: list[dict]  # commit c: key -> (before, after)
    payloads: bool  # a source's versions (net rule on payloads) or a derived output
    digests: list[int]  # D(c), kept by the writer from its exact deltas


def history(rng: random.Random, keys: int, commits: int) -> History:
    payloads = rng.random() < 0.5
    names = [f"k{i}" for i in range(keys)]
    state = {k: ABSENT for k in names}
    states, deltas, digests = [], [], []
    digest = 0
    for c in range(commits):
        delta = {}
        for k in rng.sample(names, rng.randint(0, keys)):
            live = state[k][0]
            if live and rng.random() < 0.35:
                after = ABSENT
            elif not live and rng.random() < 0.3:
                continue  # removing an absent key writes nothing
            else:
                after = (True, 10 * c + 1, rng.choice("xy") if payloads else None)
            delta[k] = (state[k], after)
            digest = (digest + h(k, after) - h(k, state[k])) & MASK
            state[k] = after
        states.append(dict(state))
        deltas.append(delta)
        digests.append(digest)
    return History(states, deltas, payloads, digests)


# -- the time view ---------------------------------------------------------------------


def combine(parts: list[dict]) -> dict:
    """Adjacent net-change nodes, oldest first: before from the oldest, after from the
    newest; absent-to-absent entries pruned (state is continuous across nodes, so
    the next node's before already says absent)."""

    out: dict = {}
    for p in parts:
        for k, (before, after) in p.items():
            out[k] = (out[k][0] if k in out else before, after)
    return {k: v for k, v in out.items() if v[0][0] or v[1][0]}


def node(hist: History, b: int, level: int, i: int) -> dict:
    size = b**level
    return combine([hist.deltas[c] for c in range(i * size, (i + 1) * size)])


def klass(before, after, payloads: bool) -> str | None:
    if not before[0] and after[0]:
        return "added"
    if before[0] and not after[0]:
        return "removed"
    if before[0] and after[0]:
        return None if payloads and before[2] == after[2] else "updated"
    return None


def deliver(net: dict, payloads: bool) -> dict:
    out = {}
    for k, (before, after) in net.items():
        c = klass(before, after, payloads)
        if c:
            out[k] = (c, after[1] if after[0] else None)
    return out


def truth(hist: History, p: int, n: int) -> dict:
    s0 = hist.states[p - 1] if p > 0 else {k: ABSENT for k in hist.states[0]}
    s1 = hist.states[n]
    return deliver({k: (s0[k], s1[k]) for k in s1 if s0[k] != s1[k]}, hist.payloads)


def cover(x: int, y: int, b: int, top: int, avail) -> list[tuple[int, int]] | None:
    """[x, y] as maximal aligned nodes (level <= top), larger first, using only
    nodes `avail` accepts; None if some commit has no node."""

    out = []
    while x <= y:
        for j in range(top, -1, -1):
            s = b**j
            if x % s == 0 and x + s - 1 <= y and avail(j, x // s):
                out.append((j, x // s))
                x += s
                break
        else:
            return None
    return out


def asc(e: int, head: int, b: int, top: int) -> set:
    """The nodes a reader at e climbs through: from e, the largest aligned node
    starting there, up to level `top`, while complete by `head`."""

    out, x = set(), e
    while True:
        j = 0
        while j < top and x % b ** (j + 1) == 0:
            j += 1
        if x + b**j - 1 > head:
            # past the head: the climb continues only through complete smaller nodes
            while j > 0 and x + b**j - 1 > head:
                j -= 1
            if x + b**j - 1 > head:
                return out
        out.add((j, x // b**j))
        x += b**j


def desc(z: int, b: int, top: int) -> set:
    """The nodes a range ending at z - 1 descends through: from z leftwards, the
    largest aligned node ending at z - 1, up to level `top`."""

    out = set()
    while z > 0:
        j = 0
        while j < top and z % b ** (j + 1) == 0:
            j += 1
        out.add((j, (z - b**j) // b**j))
        z -= b**j
    return out


def kept(endpoints: set[int], head: int, b: int, top: int) -> set:
    """The retention rule: every endpoint's ascending chain to the head and
    descending chain, the head's descending chain (the frontier), and the
    top-level nodes from the oldest endpoint on."""

    if not endpoints:
        return set()
    lo = min(endpoints)
    out = desc(head + 1, b, top)
    for e in endpoints:
        out |= asc(e, head, b, top) | desc(e, b, top)
    s = b**top
    out |= {(top, i) for i in range(lo // s, (head + 1) // s) if i * s >= lo}
    # Only nodes inside [oldest endpoint, head] are ever read.
    return {(j, i) for j, i in out if i * b**j >= lo and (i + 1) * b**j - 1 <= head}


# -- the key view, and read-ahead -------------------------------------------------------


def read_ahead(hist: History, r: int, n: int, key: str) -> tuple | None:
    """A keys= run delivered `key` as of commit r (its sealed result: the state at
    r). Classed against the key view at N, which keeps no tombstones: a key live
    at N whose generation is no newer than commit r's is skipped; anything else
    is (delivered state, state at N)."""

    delivered = hist.states[r][key]
    at_n = hist.states[n][key]  # the key view at N: the fold, pinned or at the head
    if at_n[0] and at_n[1] <= 10 * r + 1:
        return None
    c = klass(delivered, at_n, hist.payloads)
    return (c, at_n[1] if at_n[0] else None) if c else None


def read_ahead_truth(hist: History, r: int, n: int, key: str) -> tuple | None:
    changed = any(key in hist.deltas[c] for c in range(r + 1, n + 1))
    if not changed:
        return None
    c = klass(hist.states[r][key], hist.states[n][key], hist.payloads)
    return (c, hist.states[n][key][1] if hist.states[n][key][0] else None) if c else None


# -- the digest -------------------------------------------------------------------------


def node_digest(net: dict) -> int:
    return sum(h(k, after) - h(k, before) for k, (before, after) in net.items()) & MASK


def check(histories: int, seed: int) -> dict:
    rng = random.Random(seed)
    n = {"changes": 0, "retention": 0, "dropped_unneeded": 0, "read_ahead": 0, "digest": 0}
    caught = missed = 0
    for _ in range(histories):
        commits = rng.randint(1, 24)
        hist = history(rng, 6, commits)
        b, top = rng.choice([2, 3, 4]), rng.randint(1, 4)
        built = {}

        def get(j, i, hist=hist, b=b, built=built):
            if (j, i) not in built:
                built[j, i] = node(hist, b, j, i)
            return built[j, i]

        # changes(P, N) from every complete node, for every range.
        for p in range(commits):
            for q in range(p, commits):
                parts = cover(p, q, b, top, lambda j, i: True)
                got = deliver(combine([get(j, i) for j, i in parts]), hist.payloads)
                assert got == truth(hist, p, q), (p, q, b, top)
                n["changes"] += 1
        # The digest identity, and corruption detected.
        for (j, i), net in list(built.items()):
            lo, hi = i * b**j, (i + 1) * b**j - 1
            want = (hist.digests[hi] - (hist.digests[lo - 1] if lo > 0 else 0)) & MASK
            assert node_digest(net) == want
            n["digest"] += 1
            if net:
                bad = dict(net)
                k = rng.choice(sorted(bad))
                kind = rng.choice(["drop", "before", "after"])
                if kind == "drop":
                    del bad[k]
                elif kind == "before":
                    bf, af = bad[k]
                    bad[k] = ((not bf[0], bf[1] or 1, bf[2]), af)
                else:
                    bf, af = bad[k]
                    bad[k] = (bf, (af[0], af[1] + 1, af[2]) if af[0] else (True, 1, None))
                if node_digest(bad) != want:
                    caught += 1
                else:
                    missed += 1
        # Endpoints born at head + 1, retiring at random; the kept nodes answer
        # every query, at every head, and nothing dropped is needed later.
        endpoints: set[int] = {0}
        dropped: set = set()
        for head in range(commits):
            if rng.random() < 0.4:
                endpoints.add(head + 1)
            if len(endpoints) > 1 and rng.random() < 0.25:
                endpoints.discard(rng.choice(sorted(endpoints)))
            live = {e for e in endpoints if e <= head}
            keep = kept(live, head, b, top)
            complete = {(j, i) for (j, i) in built if (i + 1) * b**j - 1 <= head}
            ends = sorted(live) + [head + 1]
            for x in range(len(ends)):
                for y in range(x + 1, len(ends)):
                    parts = cover(ends[x], ends[y] - 1, b, top, lambda j, i, keep=keep: (j, i) in keep)
                    assert parts is not None, (ends[x], ends[y] - 1, b, top, sorted(keep))
                    assert not set(parts) & dropped
                    got = deliver(combine([get(j, i) for j, i in parts]), hist.payloads)
                    assert got == truth(hist, ends[x], ends[y] - 1)
                    n["retention"] += 1
            if live:
                lo = min(live)
                gone = {(j, i) for (j, i) in complete if i * b**j >= lo} - keep
                dropped |= gone
                n["dropped_unneeded"] += len(gone)
        # Read-ahead against the key view.
        for r in range(commits):
            for q in range(r, commits):
                for k in hist.states[0]:
                    assert read_ahead(hist, r, q, k) == read_ahead_truth(hist, r, q, k)
                    n["read_ahead"] += 1
    n["corruptions_caught"], n["corruptions_missed"] = caught, missed
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--histories", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()
    for k, v in check(args.histories, args.seed).items():
        print(f"{k}: {v:,}")


if __name__ == "__main__":
    main()
