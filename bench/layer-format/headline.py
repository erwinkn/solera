"""T44: the headline table — every workload at one scale, the queries that
decide, cold (s · GETs · MB) and warm CPU, ours vs Parquet, with the ratio.

    python3 headline.py results/final-*-100m.jsonl  ->  Markdown on stdout
"""

import json
import sys
from collections import defaultdict

rows = [json.loads(line) for f in sys.argv[1:] for line in open(f) if line.strip()]
by = defaultdict(dict)
stored = defaultdict(dict)
for r in rows:
    if r["query"] == "stored":
        stored[r["workload"]][r["fmt"]] = r
    else:
        by[(r["workload"], r["query"], r["mode"])][r["fmt"]] = r

QUERIES = [
    ("diff-1c", "diff, last commit"),
    ("diff-1d-first10k", "diff, 1 day, first 10K (one batch)"),
    ("diff-1d", "diff, 1 day, all"),
    ("diff-1d-range1pct", "diff, 1 day, 1% of keys"),
    ("diff-paused-first10k", "diff, paused 30 days, first 10K"),
    ("diff-paused", "diff, paused 30 days, all"),
    ("diff-across-rewrite-first10k", "diff across the rewrite, first 10K"),
    ("diff-across-rewrite", "diff across the rewrite, all"),
    ("scan-head-range1pct", "scan at head, 1% of keys"),
    ("scan-1d-range1pct", "scan a day ago, 1% of keys"),
    ("scan-head", "scan at head, all"),
    ("get-1k", "1K scattered gets"),
    ("merge-newest4", "merge the 4 newest layers"),
    ("fold-into-base", "fold every layer into the base"),
]
workloads = sorted({w for w, _, _ in by}, key=["daily-scattered", "daily-clustered", "hot", "rewrite"].index)
ok = all(r.get("ok", True) for r in rows)
for w in workloads:
    print(f"\n#### {w}\n")
    print("| query | ours, cold | Parquet, cold | Parquet / ours (time · GETs · MB) | warm CPU s, ours · Parquet | peak MB, ours · Parquet |")
    print("|---|---|---|---|---|---|")
    for q, label in QUERIES:
        c, wm = by.get((w, q, "cold")), by.get((w, q, "warm"))
        if not c:
            continue
        o, p = c["ours"], c["parquet"]
        cell = lambda r: f'{r["wall_s"]:.2f} s · {r["gets"]} · {r["mb"]:.1f} MB'
        ratio = f'{p["wall_s"] / max(0.001, o["wall_s"]):.2f} · {p["gets"] / max(1, o["gets"]):.1f} · {p["mb"] / max(0.01, o["mb"]):.2f}'
        cpu = f'{wm["ours"]["cpu_s"]:.2f} · {wm["parquet"]["cpu_s"]:.2f}' if wm else f'{o["cpu_s"]:.2f} · {p["cpu_s"]:.2f}'
        print(f'| {label} | {cell(o)} | {cell(p)} | {ratio} | {cpu} | {o["peak_mb"]:.0f} · {p["peak_mb"]:.0f} |')
    s = stored[w]
    print(f'\nStored: ours {s["ours"]["stored_mb"]:,.0f} MB ({s["ours"]["bytes_per_entry"]:.2f} B/entry), '
          f'Parquet {s["parquet"]["stored_mb"]:,.0f} MB ({s["parquet"]["bytes_per_entry"]:.2f} B/entry): '
          f'{s["parquet"]["stored_mb"] / s["ours"]["stored_mb"]:.2f}×. Metadata: ours {s["ours"]["meta_mb"]:.1f} MB, '
          f'Parquet {s["parquet"]["meta_mb"]:.1f} MB. {s["ours"]["layers"]} layers; merges wrote '
          f'{s["ours"]["merge_amp"]:.2f} entries per entry committed.')
print(f"\nEvery measurement above matches the fold: {ok}.")
