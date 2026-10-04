"""Re-encode a finished span build's files in another format: the same
entries and structure, only the container (codec, block size) rewritten.

    uv run python bench/keys/views/reencode.py SRC_BUILD_DIR DST_BUILD_DIR --codec zstd --block-size 16384

Each span is merged alone with its own segment starts as live endpoints:
the merge keeps exactly the versions those endpoints see, which is every
version the span holds, with each key's oldest predecessor; the base keeps
its live keys. The output must hold as many entries as the input (checked).
The new build directory shares the store and the fold's arrays with the
source (symlinks) and has its own `state.json` and `options.json`, so
`viewbench.py --reads DST` reads the re-encoded files.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import replace
from pathlib import Path

from obstore.store import LocalStore
from solera.keys.index import IndexState, KeyIndex, Span
from solera.keys.io import ObjectIO

sys.path.insert(0, str(Path(__file__).parent))
from viewbench import CODECS, options  # noqa: E402


async def main(src: Path, dst: Path, codec: str, block: int) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for f in src.iterdir():
        if f.name not in ("state.json", "options.json", "reads.json") and not (dst / f.name).exists():
            os.symlink(f.resolve(), dst / f.name)
    (dst / "options.json").write_text(json.dumps({"codec": codec, "block_size": block}))
    state = IndexState.from_json(json.loads((src / "state.json").read_text()))
    io = ObjectIO(LocalStore(str(src / "store")))
    opts = replace(options(src), codec=CODECS[codec], block_size=block)
    spans, t0, before, after = [], time.perf_counter(), 0, 0
    for i, s in enumerate(state.spans):
        ends = {c for c, _ in s.starts[1:]}
        idx = KeyIndex(io, None, state, opts)
        job, ins, ends_, gens, runs = idx._merge_job((i, 1), ends)
        name = f"r{codec}{block >> 10}k-{s.a:012d}-{s.b:012d}"
        files = await idx._run(job, runs, lambda n, name=name: f"{name}.{n:04d}")
        got, want = sum(job.segments), s.entries
        if got != want:
            raise SystemExit(f"span {s.a}..{s.b}: {got} entries re-encoded, {want} held")
        spans.append(Span(s.a, s.b, s.starts, tuple(files), tuple(job.segments)))
        before += s.size
        after += sum(f.size for f in files)
    out = replace(state, spans=tuple(spans))
    (dst / "state.json").write_text(json.dumps(out.to_json()))
    built = json.loads((src / "built.json").read_text())
    built["reencoded"] = {"from": str(src), "codec": codec, "block_size": block, "mb_before": before / 1e6, "mb_after": after / 1e6}
    (dst / "built.json").unlink(missing_ok=True)
    (dst / "built.json").write_text(json.dumps(built))
    print(json.dumps({"spans": len(spans), "mb_before": before / 1e6, "mb_after": after / 1e6, "s": time.perf_counter() - t0}))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("dst")
    ap.add_argument("--codec", choices=sorted(CODECS), default="zstd")
    ap.add_argument("--block-size", type=int, default=16384)
    a = ap.parse_args()
    asyncio.run(main(Path(a.src), Path(a.dst), a.codec, a.block_size))
