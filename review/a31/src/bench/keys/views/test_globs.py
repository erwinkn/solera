"""The skip-scan never skips a block holding a match (A25 R1, R2).

    uv run pytest bench/keys/views/test_globs.py -q

Random short keys over a small alphabet (so slashes, prefixes and
variable lengths collide often), random globs in Solera's full grammar,
blocks of consecutive keys; the oracle is Solera's own matcher
(`solera.patterns.glob_regex`).
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from globs import intersects, regex, tokens  # noqa: E402

ALPHABET = "ab/c"
PIECES = ["a", "b", "/", "c", "?", "*", "**", "**/", "ab", "/a"]


def test_a25_counterexamples():
    assert intersects(tokens("?"), "aa", "ca")  # "b" lies between
    assert intersects(tokens("?"), "a", "ab")  # "a" itself
    assert intersects(tokens("tenant/**"), "tenant/a/x", "tenant/a/z")
    assert intersects(tokens("**"), "x/y", "x/z")


@pytest.mark.parametrize("seed", range(40))
def test_never_skips_a_match(seed):
    rng = random.Random(seed)
    for _ in range(60):
        glob = "".join(rng.choice(PIECES) for _ in range(rng.randint(1, 4)))
        rx, toks = regex(glob), tokens(glob)
        keys = sorted({"".join(rng.choice(ALPHABET) for _ in range(rng.randint(0, 5))) for _ in range(40)})
        per = rng.randint(1, 5)
        blocks = [keys[i : i + per] for i in range(0, len(keys), per)]
        for i, block in enumerate(blocks):
            if not any(rx.fullmatch(k) for k in block):
                continue
            # first and last keys, and first keys only (up to the next block's first)
            assert intersects(toks, block[0], block[-1]), (glob, block)
            nxt = blocks[i + 1][0] if i + 1 < len(blocks) else None
            assert intersects(toks, block[0], nxt), (glob, block, nxt)
