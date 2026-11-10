"""Median of the repeats per configuration, as Markdown tables."""

import json
import statistics
import sys
from collections import defaultdict

rows = [json.loads(line) for line in open(sys.argv[1])]
for r in rows:
    r["wall_ms"] = r["wall_s"] * 1000
    r["loop_ms"] = r["loop_cpu_s"] * 1000
groups = defaultdict(list)
for r in rows:
    groups[(r["variant"], r["store"], r["shape"], r["n"], r["decode_us"])].append(r)


def med(key, k):
    return statistics.median(r[k] for r in groups[key])


def table(title, keys, cols):
    print(f"\n### {title}\n")
    print("| shape | store | variant | runs | " + " | ".join(c[0] for c in cols) + " |")
    print("|" + "---|" * (4 + len(cols)))
    for key in keys:
        if key not in groups:
            continue
        v, store, shape, n, d = key
        cells = [fmt.format(med(key, k)) for _, k, fmt in cols]
        print(f"| {shape} | {store} | {v} | {len(groups[key])} | " + " | ".join(cells) + " |")


SHAPES = ["q1", "q2", "q3"]
STORES = ["file", "s3", "s3lat"]
full = [
    ("wall s", "wall_s", "{:.2f}"),
    ("p50 s", "p50_s", "{:.2f}"),
    ("p95 s", "p95_s", "{:.2f}"),
    ("CPU s", "cpu_s", "{:.1f}"),
    ("loop CPU s", "loop_cpu_s", "{:.2f}"),
    ("max lag ms", "max_lag_ms", "{:.0f}"),
    ("crossings", "crossings", "{:.0f}"),
    ("awaitables", "awaitables", "{:.0f}"),
    ("GETs", "gets", "{:.0f}"),
    ("MB", "mb", "{:.0f}"),
    ("peak RSS MB", "rss_mb", "{:.0f}"),
]
D = 10000.0
table("One query alone (decode 10 ms/MB)", [(v, s, q, 1, D) for q in SHAPES for s in STORES for v in ("A", "B")], [("wall ms", "wall_ms", "{:.0f}"), ("CPU s", "cpu_s", "{:.2f}"), ("loop CPU ms", "loop_ms", "{:.1f}")] + full[5:8])
table("50 concurrent queries (decode 10 ms/MB)", [(v, s, q, 50, D) for q in SHAPES for s in STORES for v in ("A", "Athread", "B")], full)
table("50 concurrent q3, cheap decode (1 ms/MB)", [(v, s, "q3", 50, 1000.0) for s in STORES for v in ("A", "B")], full)
table("Single flight: 50 concurrent queries reading the same 10 MB object", [(v, s, "sf", 50, D) for s in ("s3", "s3lat") for v in ("A", "Acache", "B")], full)
