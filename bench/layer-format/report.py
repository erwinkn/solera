"""T44: tables from the bench's JSON lines.

    python3 report.py results/A.jsonl [results/B.jsonl ...]   ->  Markdown on stdout

One table per workload and scale: per query, each format's cold read (time ·
GETs · MB · peak MB) and warm read (time · CPU s · peak MB), then stored
bytes, metadata, merges. Every number comes with the fold check's verdict.
"""

import json
import sys
from collections import defaultdict

rows = [json.loads(line) for f in sys.argv[1:] for line in open(f) if line.strip()]
groups = defaultdict(list)
for r in rows:
    groups[(r["workload"], r["keys"], r.get("_config", ""))].append(r)


def cold(r):
    return f'{r["wall_s"]:.3f} s · {r["gets"]} GETs · {r["mb"]:.2f} MB · {r["peak_mb"]:.0f} MB peak'


def warm(r):
    return f'{r["wall_s"]:.3f} s · {r["cpu_s"]:.3f} s CPU · {r["peak_mb"]:.0f} MB peak'


bad = 0
for (w, n, _), rs in groups.items():
    print(f"\n### {w}, {n:,} keys\n")
    by = defaultdict(dict)
    for r in rs:
        if r["query"] in ("stored",):
            continue
        by[(r["query"], r["mode"])][r["fmt"]] = r
        bad += not r.get("ok", True)
    print("| query | mode | ours | parquet | results | fold |")
    print("|---|---|---|---|---|---|")
    for (q, m), d in by.items():
        o, p = d.get("ours"), d.get("parquet")
        f = cold if m == "cold" else warm
        extra = ""
        if q.startswith(("merge", "fold")):
            f = lambda r: f'{r["wall_s"]:.2f} s · {r["cpu_s"]:.2f} s CPU · read {r["mb"]:.1f} MB in {r["gets"]} GETs · wrote {r["written_mb"]:.1f} MB · {r["peak_mb"]:.0f} MB peak'
        ok = "✓" if all(x["ok"] for x in d.values()) else "✗"
        print(f'| {q} | {m} | {f(o) if o else "—"} | {f(p) if p else "—"} | {(o or p)["results"]:,} | {ok} |')
    st = {r["fmt"]: r for r in rs if r["query"] == "stored"}
    if st:
        o, p = st["ours"], st["parquet"]
        print(f'\nStored: ours {o["stored_mb"]:.1f} MB ({o["bytes_per_entry"]:.2f} B/entry, metadata {o["meta_mb"]:.2f} MB), '
              f'parquet {p["stored_mb"]:.1f} MB ({p["bytes_per_entry"]:.2f} B/entry, metadata {p["meta_mb"]:.2f} MB); '
              f'base: ours {o["base_mb"]:.1f} MB, parquet {p["base_mb"]:.1f} MB. '
              f'{o["layers"]} layers (at most {o["max_layers"]}), {o["commits"]} commits, merges wrote '
              f'{o["merge_amp"]:.2f} entries per entry committed. Layers: {", ".join(o["layer_list"])}.')
print(f"\n{len(rows)} lines, {bad} mismatches against the fold.", file=sys.stderr)
