"""Seed corpora for the fuzz targets: real files the writers make, so the
fuzzer starts from the format rather than rediscovering it (each block's
CRC must hold before its entries are parsed).

    uv run python native/fuzz/seeds.py   # writes native/fuzz/corpus/lay-file
"""

import asyncio
import random
from pathlib import Path

from obstore.store import MemoryStore
from solera.keys import SortedEntries
from solera.keys.io import ObjectIO
from solera.keys.layers import LayerIndex, LayerState

root = Path(__file__).parent / "corpus"
rng = random.Random(7)


async def files() -> list[bytes]:
    io, state, out = ObjectIO(MemoryStore()), LayerState(prefix="k/", life="1"), []
    for c, n in enumerate([1, 3, 20, 200]):
        keys = sorted({f"site-{rng.randrange(10**6):06d}/f{rng.randrange(100)}".encode() for _ in range(n)})
        run = SortedEntries.of(keys, [rng.randbytes(rng.randrange(6)) if rng.random() < 0.5 else None for _ in keys])
        delta, _ = await LayerIndex(io, state).write_patch(run, name=f"{c:012d}-s", generation=c + 1, replaced=True)
        state = state.committed(c, delta)
        out += [await io.read_whole(state.path(f.name), f.size) for f in delta.part.files]
    ids, layer = await LayerIndex(io, state).merge(0, len(state.layers), epoch=1)
    out += [await io.read_whole(state.path(n), 1 << 30) for n in layer.names() if n.endswith(".lay")]
    out.append(SortedEntries.of([b"a", b"b"], [b"1", None], [b"c"]).encode(block_size=256))
    return out


(root / "lay-file").mkdir(parents=True, exist_ok=True)
for i, data in enumerate(asyncio.run(files())):
    (root / "lay-file" / f"seed{i}").write_bytes(data)
print("seeds written under", root)
