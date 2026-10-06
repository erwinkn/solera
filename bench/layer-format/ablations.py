"""T44: what each of the Parquet reader's choices is worth: the final
setting against each undone alone (10M keys, daily-scattered, cold).

    python3 ablations.py  ->  Markdown on stdout
"""

import json

def load(f):
    return [json.loads(line) for line in open(f)]

runs = [
    ("final", "results/final-daily-scattered-10m.jsonl"),
    ("whole row groups as units", "results/ablation-units-10m.jsonl"),
    ("decode batch = row group", "results/ablation-batch-10m.jsonl"),
    ("coalescing gap 256 KiB", "results/ablation-coalesce256k-10m.jsonl"),
    ("coalescing gap 1 MiB", "results/ablation-coalesce1m-10m.jsonl"),
]
qs = ["diff-1d-first10k", "diff-paused-first10k", "diff-1d-range1pct", "scan-head-range1pct", "scan-head", "get-1k"]
print("| Parquet reader | " + " | ".join(qs) + " |")
print("|---" * (len(qs) + 1) + "|")
for name, f in runs:
    rs = load(f)
    cells = []
    for q in qs:
        r = next(r for r in rs if r["fmt"] == "parquet" and r["query"] == q and r.get("mode") == "cold")
        assert r["ok"]
        cells.append(f'{r["wall_s"]:.2f} s · {r["gets"]} · {r["mb"]:.1f} MB · {r["peak_mb"]:.0f} pk')
    print(f"| {name} | " + " | ".join(cells) + " |")
