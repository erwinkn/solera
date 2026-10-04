"""The campaign's 1M tables (T31 phase 2): every build's quiet-pass reads
(runs-final/*.json) side by side, one table for the base trace and a
compact one per scenario, with the load next to every timing and dollars.

    python3 bench/keys/views/campaign_report.py > bench/keys/views/campaign.md

Prices: S3 Standard, PUT $5 and GET $0.40 per million, $0.023 per GB-month;
Railway buckets, $0.015 per GB-month, uploads $0.05 per GB, requests and
downloads free. A month is 259,200 commits (one per 10 s); its reads: a
consumer every commit 1 behind, an hourly one 360 behind, a daily one 8,640
behind; writers warm (engine cache) or cold (the measured `write` per commit).
"""

from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).parent
MONTH = 259_200
READS = {"1": MONTH, "360": 720, "8640": 30}


def label(name: str, b: dict) -> str | None:
    if "-1000000-" not in name:
        return None
    if name.startswith("layers-"):
        return "layers"
    if name.startswith("spans-"):
        if name.endswith("-as-zstd16"):
            return "spans, re-encoded zstd16"
        return "spans, zstd16" if "zstd-16k" in name else "spans, zlib64"
    if name.endswith("-cover"):
        return "two views, cover"
    if "-floor" in name:
        return "two views, floor + horizon 1,000"
    return "two views, window"


def scenario(name: str, b: dict) -> str:
    if "1000000x3000" in name:
        return "large"
    return b["scenario"] + ("-h1000" if b.get("horizon") else "")


def load() -> dict:
    out = {}
    for f in sorted((HERE / "runs-final").glob("*.json")):
        text = f.read_text().strip()
        if not text:
            continue
        d = json.loads(text)
        lab = label(f.stem, d["built"])
        if lab is None:
            continue
        out[(scenario(f.stem, d["built"]), lab)] = d
    return out


def reads(d: dict) -> dict:
    return {str(r["behind"]): r for r in d["reads"]}


def stored(d: dict) -> tuple[float, float]:
    b = d["built"]
    f = b["reencoded"]["mb_after"] / b["reencoded"]["mb_before"] if "reencoded" in b else 1.0
    return b["stored_mean_mb"] * f, b["stored_peak_mb"] * f


def dollars(d: dict) -> tuple[float, float, float]:
    b, r = d["built"], reads(d)
    f = b["reencoded"]["mb_after"] / b["reencoded"]["mb_before"] if "reencoded" in b else 1.0
    per = MONTH / b["commits"]
    puts = (b["delta_puts"] + b["merge_puts"]) * per
    gets = sum(r[k]["gets"] * n for k, n in READS.items() if k in r)
    gb = stored(d)[0] / 1e3
    up = (b["delta_mb"] + b["merge_bytes"] / 1e6) * f * per / 1e3
    s3 = puts * 5e-6 + gets * 4e-7 + gb * 0.023
    cold = r["write"]["gets"] * MONTH * 4e-7 if "write" in r else 0.0
    return s3, s3 + cold, gb * 0.015 + up * 0.05


def cell(r: dict | None, first: bool = True) -> str:
    if r is None:
        return "n/a"
    t = f"{r['first_s']:.2f} · {r['wall_s']:.2f} s" if first else f"{r['wall_s']:.2f} s"
    flag = "" if r.get("quiet", True) else " ⚠"
    return f"{t} · {r['gets']:,} GETs · {r['mb_in']:.1f} MB · {r['peak_mb']:.0f} MB peak · load {r.get('load', '?')}{flag}"


ROWS = [
    ("100 behind (first page · full)", "100", True),
    ("360 behind", "360", True),
    ("8,640 behind", "8640", True),
    ("10,000 behind", "10000", True),
    ("1K cold lookups", "lookups", False),
    ("1K-key cold write", "write", False),
    ("100K-key page", "page", False),
    ("scan cust-00042* (prefix)", "scan cust-00042*", False),
    ("scan *4242* (infix, matches)", "scan *4242*", False),
    ("scan *template* (infix, none)", "scan *template*", False),
]


def table(runs: dict, scen: str, cols: list[str], rows=ROWS) -> list[str]:
    have = [c for c in cols if (scen, c) in runs]
    out = ["| | " + " | ".join(have) + " |", "|---" * (len(have) + 1) + "|"]
    for title, key, first in rows:
        out.append(f"| {title} | " + " | ".join(cell(reads(runs[(scen, c)]).get(key), first) for c in have) + " |")
    if scen == "stall":
        out.append("| stalled pass: catch-up | " + " | ".join(cell(reads(runs[(scen, c)]).get("stalled pass: catch-up")) for c in have) + " |")
    st = {c: stored(runs[(scen, c)]) for c in have}
    out.append("| stored, mean · peak | " + " | ".join(f"{st[c][0]:.0f} · {st[c][1]:.0f} MB" for c in have) + " |")
    out.append(
        "| background entry writes per entry committed | "
        + " | ".join(f"{runs[(scen, c)]['built']['merge_entries'] / runs[(scen, c)]['built']['delta_entries']:.2f}" for c in have)
        + " |"
    )
    out.append(
        "| PUTs per commit | "
        + " | ".join(
            f"{(runs[(scen, c)]['built']['delta_puts'] + runs[(scen, c)]['built']['merge_puts']) / runs[(scen, c)]['built']['commits']:.2f}"
            for c in have
        )
        + " |"
    )
    ds = {c: dollars(runs[(scen, c)]) for c in have}
    out.append("| $ a month, S3, warm writer · cold writer | " + " | ".join(f"${ds[c][0]:.2f} · ${ds[c][1]:.2f}" for c in have) + " |")
    out.append("| $ a month, Railway | " + " | ".join(f"${ds[c][2]:.2f}" for c in have) + " |")
    return out


def main():
    runs = load()
    cols = ["layers", "spans, zlib64", "spans, re-encoded zstd16", "spans, zstd16", "two views, window", "two views, cover"]
    out = [
        "# Campaign, 1M keys (T31 phase 2)",
        "",
        "Each cell: first page · full (catch-ups) or wall time, GETs, MB read, the reader process's peak memory above its",
        "baseline, and the 1-minute load average when the read started (⚠: the 45-minute wait for a load below 6 was",
        "used up, timed anyway). Every read checked key by key against the per-commit fold.",
        "",
        "## Base trace",
        "",
    ]
    out += table(runs, "base", cols)
    for scen in ("daily100", "stall", "churn", "large", "daily100-h1000"):
        if not any(s == scen for s, _ in runs):
            continue
        out += ["", f"## {scen}", ""]
        out += table(runs, scen, cols + ["two views, floor + horizon 1,000"], ROWS[2:5])
    bad = sum(r.get("mismatches", 0) for d in runs.values() for r in d["reads"])
    n = sum(len(d["reads"]) for d in runs.values())
    loads = [r.get("load") for d in runs.values() for r in d["reads"] if r.get("load") is not None]
    out += [
        "",
        f"Reads: {n}, mismatches against the fold: {bad}. Load at the start of each read: "
        + (f"{min(loads)}–{max(loads)}, {sum(1 for d in runs.values() for r in d['reads'] if not r.get('quiet', True))} flagged." if loads else "not recorded."),
    ]
    print("\n".join(out))


if __name__ == "__main__":
    main()
