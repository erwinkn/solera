"""Stamped runs against the per-commit fold: random histories, random merges
of adjacent runs under a moving cut, and every Δ(P, H) with P >= cut checked
key by key.

    python3 bench/keys/fp/check.py [histories]

A run covers commits [a, b] and holds, per key: whether it is present at b,
the commit of its last change in [a, b], its payload, and its flips (the
commits in [a, b] where it was added or removed) newer than the cut. A delta
is the run [c, c]. A merge keeps each key's newest state and unites flips,
dropping those at or below the cut; with the oldest run among its inputs, it
drops a key absent at its end with no flips left.

Δ(P, H) over the runs that overlap (P, H] (H is the head here: the manifest
a batch pins), for every key with an entry newer than P:

- state at H: the newest entry;
- presence at P: presence at H, flipped once per flip newer than P;
- omitted when absent at both ends.

The fold replays the commits themselves. Checked as well: Δ(-inf, H), the
cursor walk 'after c, first N', and that every flip a run keeps is an add
or a remove of its key inside the run's range.
"""

from __future__ import annotations

import random
import sys
from dataclasses import dataclass

KEYS = [f"k{i:02d}" for i in range(12)]


@dataclass
class Entry:
    present: bool
    last: int
    payload: int | None
    flips: list[int]


@dataclass
class Run:
    a: int
    b: int
    entries: dict[str, Entry]


def merge(runs: list[Run], cut: int, bottom: bool) -> Run:
    out: dict[str, Entry] = {}
    for r in runs:  # oldest first
        for k, e in r.entries.items():
            prev = out.get(k)
            flips = (prev.flips if prev else []) + e.flips
            out[k] = Entry(e.present, e.last, e.payload, flips)
    for k in list(out):
        e = out[k]
        e.flips = sorted(f for f in e.flips if f > cut)
        if bottom and not e.present and not e.flips:
            del out[k]
    return Run(runs[0].a, runs[-1].b, out)


def delta(runs: list[Run], p: int | None, h: int) -> dict[str, tuple[bool, bool, int | None, int | None]]:
    """Δ(P, H): key -> (present at P, present at H, last change at H, payload at H)."""
    over = [r for r in runs if r.b > (p if p is not None else -1) and r.a <= h]
    assert all(r.b <= h for r in over), "a pinned manifest ends at H"
    newest: dict[str, Entry] = {}
    flips: dict[str, int] = {}
    for r in over:  # oldest first, so later entries win
        for k, e in r.entries.items():
            if p is not None and e.last <= p:
                continue
            newest[k] = e
            flips[k] = flips.get(k, 0) + sum(1 for f in e.flips if p is None or f > p)
    out = {}
    for k, e in newest.items():
        at_h = e.present
        at_p = False if p is None else at_h ^ (flips[k] % 2 == 1)
        if not at_p and not at_h:
            continue
        out[k] = (at_p, at_h, e.last if at_h else None, e.payload if at_h else None)
    return out


def fold(history: list[dict[str, int | None]], upto: int) -> dict[str, tuple[int, int]]:
    """State after commit `upto`: key -> (last change, payload)."""
    state: dict[str, tuple[int, int]] = {}
    for c in range(upto + 1):
        for k, v in history[c].items():
            if v is None:
                state.pop(k, None)
            else:
                state[k] = (c, v)
    return state


def expected(history, p: int | None, h: int):
    at_h = fold(history, h)
    if p is None:
        return {k: (False, True, c, v) for k, (c, v) in at_h.items()}
    at_p = fold(history, p)
    out = {}
    for k in KEYS:
        a, b = at_p.get(k), at_h.get(k)
        if a is None and b is None:
            continue
        if a is not None and b is not None and a[0] == b[0]:
            continue  # unchanged: same last change
        out[k] = (a is not None, b is not None, b[0] if b else None, b[1] if b else None)
    return out


def walk(runs, p, h, n: int) -> dict:
    """The cursor walk: 'after c, first N' until done, a merge between pages."""
    got: dict = {}
    c = ""
    while True:
        page = {k: v for k, v in sorted(delta(runs, p, h).items()) if k > c}
        keys = sorted(page)[:n]
        got.update({k: page[k] for k in keys})
        if len(keys) < n:
            return got
        c = keys[-1]


def one(rng: random.Random) -> int:
    commits = rng.randint(3, 40)
    window = rng.randint(1, 20)
    history: list[dict[str, int | None]] = []
    present: set[str] = set()
    runs: list[Run] = []
    checks = 0
    for c in range(commits):
        change: dict[str, int | None] = {}
        for k in rng.sample(KEYS, rng.randint(0, 5)):
            if k in present and rng.random() < 0.35:
                change[k] = None
            else:
                change[k] = rng.choice([1, 2, 3])  # payloads repeat: reverts happen
        entries = {}
        for k, v in change.items():
            was = k in present
            if v is None:
                present.discard(k)
                entries[k] = Entry(False, c, None, [c])
            else:
                present.add(k)
                entries[k] = Entry(True, c, v, [] if was else [c])
        history.append(change)
        runs.append(Run(c, c, entries))
        cut = max(-1, c - window)
        # Random merges of adjacent runs, the cut applied.
        for _ in range(rng.randint(0, 2)):
            if len(runs) < 2:
                break
            i = rng.randrange(len(runs) - 1)
            j = rng.randint(i + 2, min(len(runs), i + 4))
            runs[i:j] = [merge(runs[i:j], cut, bottom=(i == 0))]
        # Every reader position the index promises: P in [cut, c), and P = -inf.
        for p in [None] + list(range(max(0, cut), c)):
            got = delta(runs, p, c)
            want = expected(history, p, c)
            # Payload reverts read as updates (the redundant-update fallback);
            # the fold's 'unchanged' test is by last change, which matches.
            assert got == want, (p, c, got, want)
            assert walk(runs, p, c, rng.randint(1, 4)) == want
            checks += 1
        # Flips: each is a presence change of its key inside its run's range,
        # so a key holds at most one per commit that added or removed it.
        for r in runs:
            for k, e in r.entries.items():
                for f in e.flips:
                    assert r.a <= f <= r.b and k in history[f]
                    assert (history[f][k] is None) == (k in fold(history, f - 1)), (k, f)
    return checks


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
    rng = random.Random(57)
    total = 0
    for _ in range(n):
        total += one(rng)
    print(f"{n} histories, {total} (P, H) reads checked key by key against the fold: ok")


if __name__ == "__main__":
    main()
