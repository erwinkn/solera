"""A20 measurements and explicit cost estimates. Standard library only."""

from __future__ import annotations

import json
import math
import random
import struct
import sys
import time
import zlib
from pathlib import Path

HERE = Path(__file__).resolve().parent


def codec_sample(path):
    rows = list(struct.iter_unpack("<QQ", Path(path).read_bytes()))
    rng = random.Random(83)
    output = []
    for shape in ("even_ids", "random_ids", "uuid_hex"):
        identifiers = sorted(rng.sample(range(10**12), 1_000_000)) if shape == "random_ids" else None
        for payload_size in (0, 16):
            pages = []
            keymap = {}
            for key, _generation in rows:
                if key not in keymap:
                    if shape == "uuid_hex":
                        keymap[key] = f"{rng.getrandbits(128):032x}".encode()
                    else:
                        keymap[key] = b"cust-%013d" % (identifiers[key] if identifiers else key * 1_000_000)
            t = time.perf_counter()
            for start in range(0, len(rows), 256):
                data, previous = bytearray(), b""
                for key, generation in sorted(rows[start : start + 256], key=lambda row: keymap[row[0]]):
                    if generation == 0:
                        continue  # snapshot leaves have no tombstones
                    k = keymap[key]
                    common = 0
                    while common < min(len(k), len(previous)) and k[common] == previous[common]:
                        common += 1
                    suffix = k[common:]
                    payload = rng.randbytes(payload_size)
                    data += struct.pack("<HH", common, len(suffix)) + suffix
                    data += struct.pack("<QI", generation, len(payload)) + payload
                    previous = k
                pages.append(zlib.compress(data, 1))
            output.append(
                {
                    "shape": shape,
                    "payload_bytes": payload_size,
                    "sample_slots": len(rows),
                    "live_rows": sum(g != 0 for _, g in rows),
                    "compressed_bytes": sum(map(len, pages)),
                    "compressed_bytes_per_slot": sum(map(len, pages)) / len(rows),
                    "python_encode_s": time.perf_counter() - t,
                }
            )
    return output


def expected_pages(n, page, events):
    return n / page * (-math.expm1(events * math.log1p(-page / n)))


def hierarchy_bytes(n, page, batch):
    # Estimate: fully occupied fanout-128 directory, 48 B/ref.
    # Distinct changes, independent sampling approximation for occupancy.
    total, covered = 0, page * 128
    while True:
        nodes = math.ceil(n / covered)
        touched = (
            nodes if covered >= n else nodes * (-math.expm1(batch * math.log1p(-min(covered / n, 1 - 1e-15))))
        )
        refs_per = min(128, math.ceil(n / (covered / 128)))
        total += touched * refs_per * 48
        if nodes == 1:
            return total
        covered *= 128


def derive():
    results = {}
    for filename in ("history.jsonl", "sweep.jsonl"):
        rows = [json.loads(line) for line in (HERE / filename).read_text().splitlines()]
        for row in rows:
            if row["kind"] == "write":
                row["leaf_write_amplification"] = row["copied_rows"] / (row["commits"] * row["batch"])
                row["estimated_total_raw_amplification"] = (
                    row["copied_rows"] * 16 + row["estimated_metadata_bytes"]
                ) / (row["commits"] * row["batch"] * 16)
        results[filename] = [r for r in rows if r["kind"] in ("write", "retention", "warm", "verification")]
        if filename == "history.jsonl":
            names = {r["name"] for r in rows if r["kind"] == "read"}
            plans = []
            for name in sorted(names):
                candidates = [r for r in rows if r.get("name") == name]
                for latency in (0.006, 0.030):
                    # Serial metadata rounds + 64 parallel data GETs + aggregate
                    # transfer at 500 MB/s. No CPU/encoding or PUT time included.
                    best = min(
                        candidates,
                        key=lambda r: math.ceil(r["data_gets"] / 64) * latency + r["raw_bytes"] / 500e6,
                    )
                    plans.append(
                        {
                            "name": name,
                            "latency_s": latency,
                            "gap": best.get("gap", 0),
                            "gets": best["data_gets"],
                            "raw_bytes": best["raw_bytes"],
                            "estimated_s": 2 * latency
                            + math.ceil(best["data_gets"] / 64) * latency
                            + best["raw_bytes"] / 500e6,
                        }
                    )
            results["remote_estimates"] = plans
    scale = []
    for n in (1_000_000, 100_000_000):
        for size in (16, 64, 256, 1024):
            pages = expected_pages(n, size, 1000)
            scale.append(
                {
                    "n": n,
                    "page_rows": size,
                    "dirty_pages_per_commit": pages,
                    "leaf_write_amplification": pages * size / 1000,
                    "raw_leaf_bytes_per_commit": pages * size * 16,
                    "estimated_directory_bytes_per_commit": hierarchy_bytes(n, size, 1000),
                }
            )
    results["scale_estimates"] = scale
    results["catchup_100m_estimates"] = [
        {
            "behind": behind,
            "changed_keys": 100_000_000 * (-math.expm1(behind * 1000 * math.log1p(-1 / 100_000_000))),
            "changed_leaf_fraction": -math.expm1(behind * 1000 * math.log1p(-256 / 100_000_000)),
            "two_root_leaf_raw_bytes": 2 * expected_pages(100_000_000, 256, behind * 1000) * 256 * 16,
        }
        for behind in (10, 1000, 10000)
    ]
    return results


if __name__ == "__main__":
    result = derive()
    if len(sys.argv) > 1:
        result["codec_measurements"] = codec_sample(sys.argv[1])
    print(json.dumps(result, indent=2))
