"""One line per run: a few queries cold (s/GETs/MB) and warm CPU, for sweeps."""
import json, sys
for f in sys.argv[1:]:
    rs = [json.loads(l) for l in open(f)]
    for fmt in ("ours", "parquet"):
        st = [r for r in rs if r["query"] == "stored" and r["fmt"] == fmt][0]
        def g(q, mode, k):
            return next(r[k] for r in rs if r["fmt"] == fmt and r["query"] == q and r.get("mode") == mode)
        cells = [f'{q}={g(q,"cold","wall_s"):.2f}s/{g(q,"cold","gets")}G/{g(q,"cold","mb"):.1f}MB/{g(q,"warm","cpu_s"):.2f}cpu'
                 for q in ["diff-1d", "diff-paused-first10k", "diff-paused", "scan-head-range1pct", "scan-head", "get-1k"]]
        print(f.split("/")[-1][:-6], fmt, f'B/e={st["bytes_per_entry"]:.2f} meta={st["meta_mb"]:.2f}MB', *cells, sep="  ")
