"""Seed corpora for the fuzz targets: real files the writers make, so the
fuzzer starts from the format rather than rediscovering it.

    uv run python native/fuzz/seeds.py   # writes native/fuzz/corpus/kx-file
"""

import random
from pathlib import Path

from solera import _native

root = Path(__file__).parent / "corpus"
rng = random.Random(7)


def kx(n: int, **kw) -> bytes:
    keys = sorted({f"site-{rng.randrange(10**6):06d}/f{rng.randrange(100)}".encode() for _ in range(n)})
    gens = [rng.randrange(1, 1 << 40) for _ in keys]
    deleted = bytes(rng.random() < 0.2 for _ in keys)
    payloads = [rng.randbytes(rng.randrange(6)) if rng.random() < 0.5 and not d else None for d in deleted]
    return _native.encode_file(keys, gens, deleted, payloads=payloads, **kw)


(root / "kx-file").mkdir(parents=True, exist_ok=True)
for i, (n, kw) in enumerate(
    [
        (0, {}),
        (1, {}),
        (3, {"codec": 0}),
        (20, {"block_size": 64, "codec": 0}),
        (20, {"block_size": 64}),
        (200, {}),
    ]
):
    (root / "kx-file" / f"seed{i}").write_bytes(kx(n, **kw))
print("seeds written under", root)
