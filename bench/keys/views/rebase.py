"""A finished two-views build with one base merge forced at its end: the
key view as it is after any base merge, its chain starting at an aligned
commit (a 12,000-commit trace at 100M keys never reaches one, so K's chain
still starts at commit 1 and opens ~40 runs).

    uv run python bench/keys/views/rebase.py SRC_BUILD_DIR DST_BUILD_DIR

The base absorbs T's cover of [w + 1, e], e the end of the newest built
node of the highest level present; T is unchanged, so every catch-up reads
the same files. The new directory shares the store and the fold's arrays
(symlinks) and has its own `views.json`.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

from obstore.store import LocalStore

sys.path.insert(0, str(Path(__file__).parent))
from twoviews import PackIO, TwoViews  # noqa: E402
from viewbench import options  # noqa: E402


async def main(src: Path, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for f in src.iterdir():
        if f.name not in ("views.json", "reads.json", "built.json") and not (dst / f.name).exists():
            os.symlink(f.resolve(), dst / f.name)
    v = TwoViews.from_json(PackIO(LocalStore(str(src / "store"))), options(src), json.loads((src / "views.json").read_text()))
    before = len(v.k_runs())
    top = max(j for j, _ in v.nodes)
    e = max(s + v.b**top - 1 for j, s in v.nodes if j == top)
    t = time.perf_counter()
    chain = v.runs(v.w + 1, e)
    files = await v._merge(chain + [v.base], f"b{e:012d}-forced", base=True, opts=v.base_o)
    v.base, v.w = files, e  # the old base stays in the store: the source build still reads it
    (dst / "views.json").write_text(json.dumps(v.to_json()))
    built = json.loads((src / "built.json").read_text())
    built["rebased"] = {"from": str(src), "w": e, "k_runs_before": before, "k_runs_after": len(v.k_runs()), "s": time.perf_counter() - t}
    (dst / "built.json").write_text(json.dumps(built))
    print(json.dumps(built["rebased"]))


if __name__ == "__main__":
    asyncio.run(main(Path(sys.argv[1]), Path(sys.argv[2])))
