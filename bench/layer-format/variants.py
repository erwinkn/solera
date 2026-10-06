"""T44: the I/O-policy and row-group variants at 100M (daily-scattered), cold.

    python3 variants.py  ->  Markdown on stdout
"""

import json

def load(f):
    return [json.loads(line) for line in open(f)]

final = load("results/final-daily-scattered-100m.jsonl")
rg = load("results/variant-pq-rg16k-100m.jsonl")
un = load("results/variant-untuned-100m.jsonl")

def g(rs, fmt, q):
    return next(r for r in rs if r["fmt"] == fmt and r["query"] == q and r.get("mode") == "cold")

cols = [("ours", final, "ours"), ("Parquet, 128K-row groups", final, "parquet"), ("Parquet, 16K-row groups", rg, "parquet"),
        ("ours, request-frugal", un, "ours"), ("Parquet, request-frugal", un, "parquet")]
print("| query | " + " | ".join(c[0] for c in cols) + " |")
print("|---" * (len(cols) + 1) + "|")
for q in ["diff-1c", "diff-1d-first10k", "diff-1d", "diff-1d-range1pct", "diff-paused-first10k", "diff-paused",
          "scan-head-range1pct", "scan-head", "get-1k"]:
    cells = []
    for _, rs, fmt in cols:
        r = g(rs, fmt, q)
        assert r["ok"]
        cells.append(f'{r["wall_s"]:.2f} s · {r["gets"]} · {r["mb"]:.1f} MB · {r["peak_mb"]:.0f} MB pk')
    print(f"| {q} | " + " | ".join(cells) + " |")
for name, rs in [("128K-row groups", final), ("16K-row groups", rg)]:
    s = next(r for r in rs if r["query"] == "stored" and r["fmt"] == "parquet")
    print(f'\nParquet, {name}: stored {s["stored_mb"]:,.0f} MB, metadata {s["meta_mb"]:.1f} MB (the base\'s {s["base_meta_mb"]:.1f} MB).', end="")
print()
