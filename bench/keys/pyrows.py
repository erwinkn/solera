"""Python rows to versions: a tuned pure-Python digest, the native walker, and Arrow.

    uv run python bench/keys/pyrows.py --sizes 1e4,1e6,1e7

Rows are shaped like an operational table: twelve columns — a string key,
four strings (two from small vocabularies), three integers, three floats
and a timezone-aware timestamp — with ~10% of the non-key fields None,
keys in shuffled order. Each case runs in a process of its own, its rows
built first; peak memory is what the measured step adds on top of them.

- `python`: `docs/row-digest.md` in pure Python, tuned (below), with
  `xxhash` for XXH3-128. Checked byte for byte against the extension.
- `native`: `solera._native` — `row_digests` for the digests alone,
  `Rows.records(...).entries()` for the versions.
- `arrow`: `pyarrow.Table.from_pylist`, then `Rows.arrow(...).entries()`;
  the conversion is timed with the rest.

Two steps are timed: `digest` (every row's `row(r)`, in list order) and
`versions` (each key's version, sorted by key, as a patch reads them).
`--native PATH` loads another build of the extension, for before/after.
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import struct
import subprocess
import sys
import time

CASES = ("python", "native", "arrow")
UTC = dt.UTC
T0 = dt.datetime(2026, 1, 1, tzinfo=UTC)
STATUS = ["active", "pending", "closed", "refunded", "disputed"]
REGIONS = [f"region-{i:02d}" for i in range(50)]


def rows(n: int) -> list[dict]:
    """`n` rows, keys shuffled (a fixed permutation), ~10% of fields None."""

    out = []
    step = 2654435761 % n if n > 1 else 1
    while n > 1 and _gcd(step, n) != 1:
        step += 1
    for j in range(n):
        i = (j * step) % n
        h = (i * 0x9E3779B1) & 0xFFFFFFFF
        row = {
            "id": f"ord-{i:010d}",
            "customer": f"cus_{(i * 48271) % 1_000_000_007:010d}",
            "status": STATUS[i % 5],
            "region": REGIONS[i % 50],
            "note": f"line item {i} for customer {i % 9973}",
            "quantity": i % 17,
            "amount_cents": 1000 + (i * 7919) % 10_000_000,
            "account": 10**12 + i,
            "price": (i % 10_000) / 100,
            "score": ((i * 31) % 1000) / 7.0,
            "weight": i * 0.001,
            "updated_at": T0 + dt.timedelta(seconds=i, microseconds=i % 1_000_000),
        }
        for c, name in enumerate(NULLABLE):
            if (h >> (2 * c)) % 10 == 0:
                row[name] = None
        out.append(row)
    return out


NULLABLE = (
    "customer",
    "status",
    "region",
    "note",
    "quantity",
    "amount_cents",
    "account",
    "price",
    "score",
    "weight",
    "updated_at",
)


def _gcd(a: int, b: int) -> int:
    while b:
        a, b = b, a % b
    return a


# -- the pure-Python digest ------------------------------------------------------------------
#
# docs/row-digest.md for the value types these rows hold (str, int, float,
# bool, None, datetime), tuned: one flat loop, globals bound to locals, each
# key set's sorted field plan computed once, values dispatched on their exact
# type, one join and one hash per row.


def _varint(n: int) -> bytes:
    out = bytearray()
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


SMALL = [bytes((i,)) for i in range(128)]
EPOCH_UTC = dt.datetime(1970, 1, 1, tzinfo=UTC)
EPOCH = dt.datetime(1970, 1, 1)


def py_row_digests(rows: list[dict], key: str) -> list[bytes]:
    import xxhash

    hash128 = xxhash.xxh3_128_intdigest
    pack = struct.Struct("<d").pack
    small, varint = SMALL, _varint
    datetime, epoch, epoch_utc = dt.datetime, EPOCH, EPOCH_UTC
    zero, nan = pack(0.0), bytes.fromhex("000000000000f87f")
    plans: dict[tuple, list] = {}
    out = []
    append = out.append
    for row in rows:
        names = tuple(row)
        plan = plans.get(names)
        if plan is None:
            ordered = sorted((n for n in names if n != key), key=str.encode)
            plan = plans[names] = [(n, varint(len(n.encode())) + n.encode()) for n in ordered]
        parts = []
        put = parts.append
        count = 0
        for name, head in plan:
            v = row[name]
            if v is None:
                continue
            count += 1
            put(head)
            t = type(v)
            if t is str:
                b = v.encode()
                size = len(b)
                put(b"s" + (small[size] if size < 128 else varint(size)) + b)
            elif t is int:
                b = b"%d" % v
                put(b"i" + small[len(b)] + b)
            elif t is float:
                put(b"f" + (nan if v != v else zero if v == 0 else pack(v)))
            elif t is datetime:
                offset = v.utcoffset()
                d = v - (epoch if offset is None else epoch_utc)
                ns = ((d.days * 86400 + d.seconds) * 1_000_000 + d.microseconds) * 1000
                put((b"t" if offset is None else b"T") + ns.to_bytes(16, "little", signed=True))
            elif t is bool:
                put(b"o\x01" if v else b"o\x00")
            else:
                raise TypeError(f"the baseline does not digest {t.__name__}")
        record = b"".join(parts)
        head = b"\x01Rr" + (small[count] if count < 128 else varint(count))
        append(hash128(head + record).to_bytes(16, "little"))
    return out


def py_versions(rows: list[dict], key: str) -> tuple[list[bytes], list[bytes]]:
    """Each key's `group(rows)`, sorted by key."""

    import xxhash

    hash128 = xxhash.xxh3_128_intdigest
    digests = py_row_digests(rows, key)
    keyed = sorted(
        zip(
            [(k.encode() if type(k) is str else b"%d" % k) for k in (r[key] for r in rows)],
            digests,
            strict=True,
        ),
        key=lambda e: e[0],
    )
    keys, versions = [], []
    i, n = 0, len(keyed)
    while i < n:
        k = keyed[i][0]
        j = i + 1
        while j < n and keyed[j][0] == k:
            j += 1
        group = sorted(d for _, d in keyed[i:j])
        keys.append(k)
        versions.append(hash128(b"\x01G" + _varint(j - i) + b"".join(group)).to_bytes(16, "little"))
        i = j
    return keys, versions


# -- one case, in its own process ------------------------------------------------------------


def memory() -> tuple[int, int]:
    """Resident and peak resident bytes of this process (as `bulk.py`, which
    imports the installed extension: `--native` must come first)."""

    fields = dict(line.split(":", 1) for line in open("/proc/self/status"))
    return int(fields["VmRSS"].split()[0]) * 1024, int(fields["VmHWM"].split()[0]) * 1024


def reset_peak() -> None:
    with open("/proc/self/clear_refs", "w") as f:
        f.write("5")


def _load(native: str | None):
    """The extension: the installed one, or the build at `native` in its place."""

    if native:
        spec = importlib.util.spec_from_file_location("solera._native", native)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        sys.modules["solera._native"] = module
    return importlib.import_module("solera._native")


def one(case: str, step: str, n: int, native: str | None) -> dict:
    with open("/proc/self/oom_score_adj", "w") as f:  # 10M rows are ~9 GB: this goes first
        f.write("1000")
    _native = _load(native)
    assert native is None or _native.__file__ == native
    data = rows(n)
    base, _ = memory()
    reset_peak()
    wall = float("inf")
    for _ in range(5 if n < 1_000_000 else 1):  # small sizes: the best of five
        t = time.perf_counter()
        result = measured(_native, case, step, data)
        wall = min(wall, time.perf_counter() - t)
    _, peak = memory()
    sample = data[:1000]
    if step == "digest":
        assert result[:16000] == _native.row_digests(sample, "id"), "the baseline disagrees"
    else:
        versions = dict(zip(*result, strict=True))
        keys, expected = _native.Rows.records(sample, "id").entries()
        assert [versions[k] for k in keys] == expected, "the versions disagree"
    return {"case": case, "step": step, "n": n, "wall": wall, "peak_gb": (peak - base) / 1e9}


def measured(_native, case: str, step: str, data: list[dict]):
    if step == "digest":
        if case == "python":
            return b"".join(py_row_digests(data, "id"))
        if case == "native":
            return _native.row_digests(data, "id")
        raise SystemExit("arrow digests rows by key only")
    if case == "python":
        return py_versions(data, "id")
    if case == "native":
        return _native.Rows.records(data, "id").entries()
    import pyarrow as pa

    return _native.Rows.arrow(pa.Table.from_pylist(data), "id").entries()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e4,1e6,1e7")
    ap.add_argument("--cases", default=",".join(CASES))
    ap.add_argument("--steps", default="digest,versions")
    ap.add_argument("--native")
    ap.add_argument("--label", default="")
    ap.add_argument("--one")
    ap.add_argument("--step")
    ap.add_argument("--n", type=int)
    args = ap.parse_args()
    if args.one:
        print(json.dumps(one(args.one, args.step, args.n, args.native)))
        return
    for n in (int(float(x)) for x in args.sizes.split(",")):
        for step in args.steps.split(","):
            for case in args.cases.split(","):
                if case == "arrow" and step == "digest":
                    continue
                cmd = [sys.executable, __file__, "--one", case, "--step", step, "--n", str(n)]
                if args.native:
                    cmd += ["--native", args.native]
                p = subprocess.run(cmd, capture_output=True, text=True)
                if p.returncode:
                    print(f"[{case} {step} at {n:,} failed]\n{p.stderr[-2000:]}", flush=True)
                    continue
                r = json.loads(p.stdout.strip().splitlines()[-1])
                r["label"] = args.label
                r["us_per_row"] = r["wall"] / n * 1e6
                print(json.dumps(r), flush=True)


if __name__ == "__main__":
    main()
