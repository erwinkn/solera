"""A finished layers build with one base merge forced at its end: the shape
after any base merge. A 12,000-commit trace at 100M keys never reaches one
(the base absorbs the layers above it once they hold a quarter of it,
~25,000 commits in), so the build shows a first cycle only. As W53's
`bench/keys/views/rebase.py` does for two views.

    uv run python bench/keys/fp/rebase.py SRC_BUILD_DIR DST_BUILD_DIR

The base absorbs the layers just above it as far as the reader bound allows
(the trigger's quarter ignored); the cut is the build's. The new directory
shares the store and the fold's arrays (symlinks) and has its own
`layers.json`; the source build still reads its own files.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

from obstore.store import LocalStore
from solera.keys.io import ObjectIO

sys.path.insert(0, str(Path(__file__).parent))
import layers as L  # noqa: E402


async def main(src: Path, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for f in src.iterdir():
        if f.name not in ("layers.json", "reads.json", "built.json") and not (dst / f.name).exists():
            os.symlink(f.resolve(), dst / f.name)
    io = ObjectIO(LocalStore(str(src / "store")))
    ix = L.Layers(io, "", state=L.State.from_json(json.loads((src / "layers.json").read_text())), epoch=2)
    before = len(ix.s.layers)
    ls = ix.s.layers
    reach = max((j for j in range(2, len(ls) + 1) if ix._allowed(ls[:j], ls[j:])), default=None)
    t = time.perf_counter()
    if reach is not None:
        await ix.merge(0, reach)
    (dst / "layers.json").write_text(json.dumps(ix.s.to_json()))
    built = json.loads((src / "built.json").read_text())
    w = ix.written["base"]
    built["rebased"] = {
        "from": str(src),
        "absorbed": (reach or 1) - 1,
        "layers_before": before,
        "layers_after": len(ix.s.layers),
        "written_entries": w.entries,
        "written_bytes": w.bytes,
        "s": time.perf_counter() - t,
    }
    (dst / "built.json").write_text(json.dumps(built))
    print(json.dumps(built["rebased"]))


if __name__ == "__main__":
    asyncio.run(main(Path(sys.argv[1]), Path(sys.argv[2])))
