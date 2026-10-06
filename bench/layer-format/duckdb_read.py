"""T44: DuckDB reading each layer format, from a `dump=DIR` of the bench.

Parquet layers are read natively (`read_parquet`); ours through their Arrow
export, as a native table function would hand DuckDB the decoded batches
(the export's own time is in the manifest, and added here). History-style
questions, the kind the brief wants answered without a second copy:

- one key's history across every layer;
- how many entries a window of commits holds (one day);
- entries by commit over the whole log;
- a key prefix's entries.

    uv run python bench/layer-format/duckdb_read.py DIR  ->  JSON lines on stdout
"""

import json
import sys
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc

root = Path(sys.argv[1])
m = json.loads((root / "manifest.json").read_text())
head, day = m["head"], m["per_day"]
layers = m["layers"]
pq_files = [str(root / l["path"]) for l in layers if l["fmt"] == "parquet"]
ours = [l for l in layers if l["fmt"] == "ours"]
key = b"site-0000/part-42/obj-000000420042"  # a key prefix: one id's entries, whatever its hash part


def timed(con, sql, *params):
    t = time.perf_counter()
    rows = con.execute(sql, list(params)).fetchall()
    return time.perf_counter() - t, rows


QUERIES = {
    "history-1-key": ("SELECT key, \"commit\", new, replaced FROM t WHERE key >= ? AND key < ? ORDER BY key, \"commit\" DESC", [key, key + b"\xff"]),
    "window-1d": ("SELECT count(*) FROM t WHERE \"commit\" > ? AND \"commit\" <= ?", [head - day, head]),
    "by-commit": ("SELECT \"commit\", count(*) FROM t GROUP BY 1 ORDER BY 1", []),
    "prefix-site": ("SELECT count(*) FROM t WHERE key >= ? AND key < ?", [b"site-0005/", b"site-0006/"]),
}

out = []
con = duckdb.connect()
con.execute("SET threads TO 4")
files = ", ".join(f"'{f}'" for f in pq_files)
con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet([{files}])")
answers = {}
for name, (sql, params) in QUERIES.items():
    first, rows = timed(con, sql, *params)
    again, _ = timed(con, sql, *params)
    answers[name] = rows
    out.append({"fmt": "parquet", "query": name, "first_s": round(first, 4), "again_s": round(again, 4), "rows": len(rows)})

con2 = duckdb.connect()
con2.execute("SET threads TO 4")
export_s = sum(l["export_s"] for l in ours)
t = time.perf_counter()
tables = [ipc.open_file(pa.memory_map(str(root / l["arrow"]))).read_all() for l in ours]
table = pa.concat_tables(tables)
load_s = time.perf_counter() - t
con2.register("t", table)
for name, (sql, params) in QUERIES.items():
    first, rows = timed(con2, sql, *params)
    again, _ = timed(con2, sql, *params)
    same = rows == answers[name]
    out.append({"fmt": "ours", "query": name, "first_s": round(first, 4), "again_s": round(again, 4), "rows": len(rows),
                "same_answer_as_parquet": same, "export_s": round(export_s, 3), "arrow_load_s": round(load_s, 3)})

sizes = {
    "parquet_mb": sum(l["size"] for l in layers if l["fmt"] == "parquet") / 1e6,
    "ours_mb": sum(l["size"] for l in ours) / 1e6,
    "ours_arrow_export_mb": sum((root / l["arrow"]).stat().st_size for l in ours) / 1e6,
}
for r in out:
    print(json.dumps({"workload": m["workload"], "keys": m["keys"], **r}))
print(json.dumps({"workload": m["workload"], "keys": m["keys"], "query": "sizes", **sizes}))
