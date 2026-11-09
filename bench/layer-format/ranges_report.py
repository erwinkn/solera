"""T44 ranges: our format with range records and run-length columns, against plain ours.

    python3 ranges_report.py results/ranges-*.jsonl   ->  Markdown on stdout

Per workload and scale: stored bytes, then each query's cold read
(time · GETs · MB) and warm CPU, per variant, with the fold check's verdict.
"""

import json
import sys
from collections import defaultdict

ORDER = ["ours", "ours+ranges", "ours+rle", "ours+ranges+rle"]
rows = [json.loads(line) for f in sys.argv[1:] for line in open(f) if line.strip()]
groups = defaultdict(list)
for r in rows:
    groups[(r["workload"], r["keys"])].append(r)

bad = sum(not r.get("ok", True) for r in rows)
for (w, n), rs in sorted(groups.items(), key=lambda g: (g[0][1], g[0][0])):
    fmts = [f for f in ORDER if any(r["fmt"] == f for r in rs)]
    print(f"\n### {w}, {n // 1_000_000}M keys\n")
    st = {r["fmt"]: r for r in rs if r["query"] == "stored"}
    base = st.get("ours")
    print("| variant | stored | B/entry | vs ours | index + records | range records | entries they stand for |")
    print("|---|---|---|---|---|---|---|")
    for f in fmts:
        s = st[f]
        ratio = s["stored_mb"] / base["stored_mb"] if base else 1
        print(f'| {f} | {s["stored_mb"]:,.1f} MB | {s["bytes_per_entry"]:.2f} | {ratio:.2f} | {s["meta_mb"]:.2f} MB '
              f'| {s.get("records", 0):,} | {s.get("covered", 0):,} |')
    by = defaultdict(dict)
    for r in rs:
        if r["query"] != "stored":
            by[(r["query"], r["mode"])][r["fmt"]] = r
    print(f'\nCold: `time · GETs · MB read`; warm: CPU s. ✓: matches the fold.\n')
    print("| query | " + " | ".join(f"{f}, cold" for f in fmts) + " | warm CPU s, " + " · ".join(fmts) + " | ✓ |")
    print("|---|" + "---|" * len(fmts) + "---|---|")
    queries = [q for (q, m) in by if m == "cold"]
    for q in queries:
        c, wm = by[(q, "cold")], by.get((q, "warm"), {})
        def cell(r):
            if q.startswith(("merge", "fold")):
                return f'{r["wall_s"]:.2f} s · {r["gets"]} · {r["mb"]:.1f} MB → {r["written_mb"]:.1f} MB'
            return f'{r["wall_s"]:.2f} s · {r["gets"]} · {r["mb"]:.1f} MB'
        cells = [cell(c[f]) if f in c else "—" for f in fmts]
        warm = " · ".join(f'{wm[f]["cpu_s"]:.2f}' if f in wm else (f'{c[f]["cpu_s"]:.2f}' if f in c else "—") for f in fmts)
        ok = "✓" if all(x["ok"] for x in list(c.values()) + list(wm.values())) else "✗"
        print(f"| {q} | " + " | ".join(cells) + f" | {warm} | {ok} |")
print(f"\n{len(rows)} lines, {bad} mismatches against the fold.", file=sys.stderr)
