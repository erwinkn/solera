"""Tables from the change-index bench's JSON lines, beside the format
benchmark's "ours" prototype (T44, `t44/`: its final runs, "ours" rows only).

    python3 report.py results/*.jsonl   ->  Markdown on stdout
"""

import json
import sys
from collections import defaultdict
from pathlib import Path

here = Path(__file__).parent
rows = [json.loads(line) for f in sys.argv[1:] for line in open(f) if line.strip()]
ref = [json.loads(line) for f in sorted((here / "t44").glob("*.jsonl")) for line in open(f) if line.strip()]
t44 = {(r["workload"], r["keys"], r["query"], r.get("mode")): r for r in ref}

QUERIES = [
    ("diff-1c", "diff, last commit"),
    ("diff-1d-first10k", "diff, 1 day, first 10K"),
    ("diff-1d", "diff, 1 day, all"),
    ("diff-1d-range1pct", "diff, 1 day, 1% of keys"),
    ("diff-paused-first10k", "diff, paused 30 days, first 10K"),
    ("diff-paused", "diff, paused 30 days, all"),
    ("diff-paused-range1pct", "diff, paused 30 days, 1% of keys"),
    ("diff-across-rewrite-first10k", "diff across the rewrite, first 10K"),
    ("diff-across-rewrite", "diff across the rewrite, all"),
    ("scan-head", "scan at head, all"),
    ("scan-head-range1pct", "scan at head, 1% of keys"),
    ("scan-1d-range1pct", "scan a day ago, 1% of keys"),
    ("get-30", "30 scattered gets"),
    ("get-100", "100 scattered gets"),
    ("get-1k", "1K scattered gets"),
    ("get-10k", "10K scattered gets (a batch at the scale target)"),
    ("merge-newest4", "merge the 4 newest layers"),
    ("fold-into-base", "fold every layer into the base"),
]


def cold(r):
    return f"{r['wall_s']:.2f} s · {r['gets']} · {r['mb']:.1f} MB"


def ratio(a, b, k):
    return "–" if not b[k] else f"{a[k] / b[k]:.2f}"


groups = defaultdict(dict)
for r in rows:
    groups[(r["workload"], r["keys"])][(r["query"], r.get("mode"))] = r
bad = sum(1 for r in rows if r.get("ok") is False)
for (w, n), g in sorted(groups.items(), key=lambda x: (x[0][1], x[0][0])):
    print(f"\n#### {w}, {n // 1_000_000}M keys\n")
    print(
        "| query | index, cold | T44 ours, cold | index / T44 (time · GETs · MB) | warm CPU s, index · T44 | peak MB, index · T44 | fold |"
    )
    print("|---|---|---|---|---|---|---|")
    for q, label in QUERIES:
        c = g.get((q, "cold"))
        if not c:
            continue
        warm = g.get((q, "warm"))
        o = t44.get((w, n, q, "cold"))
        ow = t44.get((w, n, q, "warm"))
        ok = "✓" if c["ok"] and (not warm or warm["ok"]) else "✗"
        if q.startswith(("merge", "fold")):
            mine = f"{c['wall_s']:.2f} s, read {c['mb']:.1f} MB, wrote {c['written_mb']:.1f} MB"
            theirs = (
                f"{o['wall_s']:.2f} s, read {o['mb']:.1f} MB, wrote {o['written_mb']:.1f} MB" if o else "–"
            )
            cpu = f"{c['cpu_s']:.2f} · {o['cpu_s']:.2f}" if o else f"{c['cpu_s']:.2f} · –"
        else:
            mine, theirs = cold(c), cold(o) if o else "–"
            cpu = f"{warm['cpu_s']:.2f} · {ow['cpu_s']:.2f}" if warm and ow else "–"
        rat = " · ".join(ratio(c, o, k) for k in ("wall_s", "gets", "mb")) if o else "–"
        peak = f"{c['peak_mb']:.0f} · {o['peak_mb']:.0f}" if o else f"{c['peak_mb']:.0f} · –"
        print(f"| {label} | {mine} | {theirs} | {rat} | {cpu} | {peak} | {ok} |")
    s = g.get(("stored", None))
    o = t44.get((w, n, "stored", None))
    if s:
        line = f"\nStored: {s['stored_mb']:,.1f} MB ({s['bytes_per_entry']:.2f} B/entry), metadata {s['meta_mb']:.2f} MB, {s['layers']} layers"
        if o:
            line += f"; T44 ours {o['stored_mb']:,.1f} MB ({o['bytes_per_entry']:.2f} B/entry), metadata {o['meta_mb']:.2f} MB"
        print(line + ".")
    rss = g.get(("rss", None))
    if rss:
        print(f"Process peak RSS (workload, oracle and bench included): {rss['max_rss_mb']:,.0f} MB.")
print(f"\n{len(rows)} lines, {bad} mismatches against the fold.", file=sys.stderr)
