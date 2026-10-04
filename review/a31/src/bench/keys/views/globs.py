"""W53 phase 1: how much of the key view a non-prefix glob must read.

Keys are sorted into blocks. A page under a pattern reads only blocks that
can hold a match. Three ways to tell:

- scan: every block (today's rule for non-prefix patterns: filter the stream);
- skip: from the block index alone. Every key of a block shares the common
  prefix of its first and last keys; if no string with that prefix can match
  the glob, the block is skipped. Free: no extra bytes stored. With only first
  keys in the index (format v4), the prefix is that of this block's first key
  and the next block's, a little shorter.
- trigrams: a per-block filter of the block's 3-grams (exact sets here, so a
  lower bound on what a Bloom filter would read), and what it would store at
  10 bits per distinct 3-gram.

Globs follow Solera's patterns: `*` and `?` stay within a path segment, `**/`
takes whole directories or none (`tests/sdk/test_patterns.py`).

Usage: python3 bench/keys/views/globs.py
"""

from __future__ import annotations

import os
import random
import re

from solera.patterns import glob_regex


def tokens(glob: str):
    """Solera's glob grammar (`solera.patterns.glob_regex`): `**/` (whole
    directories or none), `**` (anything, `/` included), `*` and `?` (within a
    segment), literals."""

    out, i = [], 0
    while i < len(glob):
        if glob.startswith("**/", i):
            out.append(("dirs",))
            i += 3
        elif glob.startswith("**", i):
            out.append(("any",))
            i += 2
        elif glob[i] == "*":
            out.append(("star",))
            i += 1
        elif glob[i] == "?":
            out.append(("one",))
            i += 1
        else:
            out.append(("lit", glob[i]))
            i += 1
    return out


def regex(glob: str) -> re.Pattern:
    """Solera's own matcher: the oracle."""

    return re.compile(glob_regex(glob), re.S)


# The automaton: a state is (token index, inside) where `inside` matters only
# for `dirs`: 1 once it has read part of a directory not yet closed by "/".


def closure(toks, states: set) -> set:
    """States reachable without reading: `*`, `**` and `**/` may match nothing."""

    todo, seen = list(states), set(states)
    while todo:
        s, inside = todo.pop()
        if s < len(toks) and inside == 0 and toks[s][0] in ("star", "any", "dirs") and (s + 1, 0) not in seen:
            seen.add((s + 1, 0))
            todo.append((s + 1, 0))
    return seen


def step(toks, states: set, ch: str) -> set:
    nxt = set()
    for s, inside in states:
        if s >= len(toks):
            continue
        kind = toks[s][0]
        if (kind == "lit" and toks[s][1] == ch) or (kind == "one" and ch != "/"):
            nxt.add((s + 1, 0))
        elif kind == "star" and ch != "/":
            nxt.add((s, 0))
        elif kind == "any":
            nxt.add((s, 0))
        elif kind == "dirs":  # (?:.*/)?: any characters, closed by a "/"
            nxt.add((s, 1))
            if ch == "/":
                nxt.add((s, 0))
    return closure(toks, nxt)


def start(toks) -> set:
    return closure(toks, {(0, 0)})


def accepts(toks, states: set) -> bool:
    return (len(toks), 0) in states


def alive(toks, prefix: str) -> bool:
    """Whether some string starting with `prefix` can match (every live
    state can be completed)."""

    states = start(toks)
    for ch in prefix:
        states = step(toks, states, ch)
        if not states:
            return False
    return True


def intersects(toks, lo: str, hi: str | None) -> bool:
    """Whether some match lies in [lo, hi] (hi None: no upper bound): a walk
    down the strings between the bounds, tight on either, that accepts a
    complete match wherever it lies inside both, and stops as soon as it is
    free of both (any live state can then be completed). What a skip-scan
    reader tests per block from the block index alone."""

    lits = {t[1] for t in toks if t[0] == "lit"} | {"/"}
    top = 0x10FFFF

    def walk(i: int, states: set, tlo: bool, thi: bool) -> bool:
        if not states:
            return False
        # The string read so far, if it ends here: >= lo unless a proper
        # prefix of lo; <= hi always (a prefix of hi, or already below it).
        if accepts(toks, states) and not (tlo and i < len(lo)):
            return True
        if tlo and i >= len(lo):
            tlo = False  # any longer string is past lo
        if not tlo and not thi:
            return True
        if thi and i >= len(hi):
            return False  # equal to hi: anything longer is past it
        low = ord(lo[i]) if tlo else 0
        high = ord(hi[i]) if thi else top
        cands = {low, high} | {ord(c) for c in lits if low <= ord(c) <= high}
        # One character strictly inside both bounds that is no literal: it
        # frees the walk of both bounds at once.
        inner = next((x for x in range(low + 1, min(high, low + 400)) if chr(x) not in lits), None)
        if inner is not None:
            cands.add(inner)
        for x in sorted(cands):
            if low <= x <= high and walk(
                i + 1, step(toks, states, chr(x)), tlo and x == low, thi and x == high
            ):
                return True
        return False

    return walk(0, start(toks), True, hi is not None)


def lcp(a: str, b: str) -> str:
    n = 0
    for x, y in zip(a, b):
        if x != y:
            break
        n += 1
    return a[:n]


def paths(n: int, rng: random.Random) -> list[str]:
    keys = set()
    while len(keys) < n:
        t, s = rng.randrange(8), rng.randrange(250)
        y, m, d = 2021 + rng.randrange(5), 1 + rng.randrange(12), 1 + rng.randrange(28)
        f, ext = rng.randrange(5000), rng.choice(["csv", "xlsx", "pdf", "json"])
        keys.add(f"tenant-{t:02}/site-{s:04}/{y}-{m:02}-{d:02}/report-{f:05}.{ext}")
    return sorted(keys)


def ids(n: int, rng: random.Random) -> list[str]:
    keys = set()
    while len(keys) < n:
        keys.add(f"{rng.randrange(10**12):012d}")
    return sorted(keys)


def grams(key: str) -> set[str]:
    return {key[i : i + 3] for i in range(len(key) - 2)}


def needed(glob: str) -> set[str] | None:
    """3-grams every match must contain: those of the glob's literal runs."""

    runs, cur = [], ""
    for t in tokens(glob):
        if t[0] == "lit":
            cur += t[1]
        else:
            runs.append(cur)
            cur = ""
    runs.append(cur)
    out = set()
    for r in runs:
        out |= grams(r)
    return out


def measure(name: str, keys: list[str], per_block: int, globs: list[str]):
    blocks = [keys[i : i + per_block] for i in range(0, len(keys), per_block)]
    firsts = [b[0] for b in blocks]
    block_grams = [set().union(*(grams(k) for k in b)) for b in blocks]
    gram_bytes = sum(len(g) for g in block_grams) * 10 / 8
    key_bytes = sum(len(k) for k in keys)
    print(f"\n{name}: {len(keys):,} keys, {len(blocks):,} blocks of {per_block}; trigram filters "
          f"{gram_bytes / len(keys):.1f} B/key ({100 * gram_bytes / key_bytes:.0f}% of the raw keys)")
    print("glob\tmatches\tblocks holding a match\tcommon prefix (first+last)\tinterval (first+last)\t"
          "interval (first keys only)\ttrigrams")
    for g in globs:
        rx, toks, want = regex(g), tokens(g), needed(g)
        hit = [any(rx.fullmatch(k) for k in b) for b in blocks]
        matches = sum(1 for k in keys if rx.fullmatch(k))
        prefix = [alive(toks, lcp(b[0], b[-1])) for b in blocks]
        both = [intersects(toks, b[0], b[-1]) for b in blocks]
        # First keys only: a block holds keys in [its first, the next block's first).
        firsts_only = [intersects(toks, firsts[i], firsts[i + 1] if i + 1 < len(blocks) else None)
                       for i in range(len(blocks))]
        tri = [want <= block_grams[i] for i in range(len(blocks))]
        for test in (prefix, both, firsts_only):
            assert all(s for s, h in zip(test, hit, strict=True) if h)  # never skips a match
        pct = lambda xs: f"{100 * sum(xs) / len(xs):.1f}%"  # noqa: E731
        print(f"{g}\t{matches:,}\t{pct(hit)}\t{pct(prefix)}\t{pct(both)}\t{pct(firsts_only)}\t{pct(tri)}")


def main():
    rng = random.Random(int(os.environ.get("SEED", "1")))
    p = paths(1_000_000, rng)
    pglobs = [
        "tenant-03/**/*",  # a prefix
        "tenant-*/site-0042/**/*",  # one site in every tenant
        "tenant-03/site-*/2024-03-*/*",  # a month, one tenant
        "*/*/2024-03-*/*",  # a month, everywhere
        "**/report-0004?.*",  # a rare file name, anywhere
        "**/*.pdf",  # a suffix
    ]
    # 64 KiB blocks hold ~1,600 path entries, 16 KiB ~400 (codec/results.tsv).
    for per in (1_600, 400):
        measure("paths", p, per, pglobs)
    i = ids(1_000_000, rng)
    for per in (2_700, 680):
        measure("12-digit ids", i, per, ["0042*", "*4242*", "*99"])


if __name__ == "__main__":
    main()
