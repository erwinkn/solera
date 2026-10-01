"""Format parameters of the key index, measured without I/O.

    uv run python bench/keys/params.py [--n 1e6]

Entry size and compression by key and version shape, the Bloom filters'
false-positive rate by bits per item, and the block size trade-off, against
the assumptions in docs/key-index-costs.md. Uses the native extension when
it is installed. Results print as Markdown.
"""

from __future__ import annotations

import argparse
import math
import random
import time
import uuid

from solera import keys as K
from solera._native import value_digest
from solera.keys import CODEC_NONE, parse_footer, parse_tail


def random_ids(n: int, rng: random.Random) -> list[bytes]:
    """bench.py's keyspace: sorted random 13-digit ids."""

    gap, ident, out = max(2, 10**12 // n), 0, []
    for _ in range(n):
        ident += 1 + rng.randrange(2 * gap)
        out.append(b"cust-%013d" % ident)
    return out


KEYS = {
    "random ids `cust-%013d` (bench)": random_ids,
    "UUIDs": lambda n, rng: sorted(
        str(uuid.UUID(int=rng.getrandbits(128), version=4)).encode() for _ in range(n)
    ),
    "sequential ids `order-%012d`": lambda n, rng: [b"order-%012d" % i for i in range(n)],
    "paths `site-…/file-i` (cpu.py)": lambda n, rng: sorted(
        {f"site-{rng.randrange(10**12):012d}/file-{i}".encode() for i in range(n)}
    ),
}
VERSIONS = {
    "row digest (the default; bench, model)": lambda n, rng: [
        value_digest({"id": i, "value": rng.random()}) for i in range(n)
    ],
    "short revision `%d`": lambda n, rng: [b"%d" % rng.randrange(10**6) for _ in range(n)],
}


def layout(data: bytes) -> dict:
    footer = parse_footer(data[-48:])
    tail = parse_tail(data[footer["filters_offset"] :], len(data))
    return {
        "blocks": footer["filters_offset"],
        "filters": footer["filters_length"],
        "index": len(data) - footer["index_offset"],
        "nblocks": len(tail["blocks"]),
        "tail": tail,
    }


def entry_sizes(n: int):
    print(f"\n### Entry size by key and version shape ({n:,} entries)\n")
    print(
        "| Keys | Versions | Raw | Prefix-encoded | Blocks (zlib) | Compression | Filters | Index | "
        "**Total per entry** | Entries per block | Block, compressed |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for kname, kgen in KEYS.items():
        keys = kgen(n, random.Random(0))
        m = len(keys)
        for vname, vgen in VERSIONS.items():
            if vname.startswith("short") and not kname.startswith("sequential"):
                continue  # short revisions: one row is enough
            vers = vgen(m, random.Random(1))
            raw = sum(map(len, keys)) + sum(map(len, vers))
            dele = bytes(m)
            plain = layout(K.encode_file(keys, vers, dele, codec=CODEC_NONE))
            data = K.encode_file(keys, vers, dele)
            lay = layout(data)
            print(
                f"| {kname} | {vname} | {raw / m:.1f} B | {plain['blocks'] / m:.1f} B | {lay['blocks'] / m:.1f} B | "
                f"{raw / lay['blocks']:.2f}× | {lay['filters'] / m:.2f} B | {lay['index'] / m:.3f} B | "
                f"**{len(data) / m:.1f} B** | {m / lay['nblocks']:,.0f} | {lay['blocks'] / lay['nblocks'] / 1024:.1f} KiB |"
            )


def bloom_theory(bits: int, k: int) -> tuple[float, float]:
    """False-positive rate of a standard Bloom filter, and of one blocked into
    512-bit blocks (items per block ~ Poisson(512 / bits))."""

    standard = (1 - math.exp(-k / bits)) ** k
    lam, blocked, p = 512 / bits, 0.0, math.exp(-512 / bits)
    for j in range(0, 400):
        blocked += p * (1 - (1 - 1 / 512) ** (k * j)) ** k
        p *= lam / (j + 1)
    return standard, blocked


def false_positives(n: int, probes: int):
    print(f"\n### Bloom filter false positives ({n:,} items per filter, {probes:,} absent probes)\n")
    print(
        "| Bits per item | k | Filter bytes per entry (3 filters) | Key filter FP | Pair filter FP (changed version) | "
        "Theory, standard | Theory, 512-bit blocked |"
    )
    print("|---|---|---|---|---|---|---|")
    rng = random.Random(3)
    keys = random_ids(n, rng)
    vers = [rng.randbytes(16) for _ in keys]
    absent = [b"absent-%013d" % rng.randrange(10**13) for _ in range(probes)]
    probe_keys = [keys[rng.randrange(n)] for _ in range(probes)]
    other = [rng.randbytes(16) for _ in range(probes)]
    for bits, k in ((8, 6), (10, 7), (12, 8), (14, 10), (16, 11), (20, 14)):
        data = K.encode_file(keys, vers, bytes(n), bits_per_item=bits, k=k)
        lay = layout(data)
        nb, kk, fbits = lay["tail"]["key_filter"]
        fp_key = sum(K.bloom_check_keys(fbits, nb, kk, absent)) / probes
        nb, kk, fbits = lay["tail"]["pair_filter"]
        fp_pair = sum(K.bloom_check_pairs(fbits, nb, kk, probe_keys, other)) / probes
        std, blk = bloom_theory(bits, k)
        mark = " (default)" if (bits, k) == (14, 10) else ""
        print(
            f"| {bits}{mark} | {k} | {lay['filters'] / n:.2f} B | {fp_key:.3%} | {fp_pair:.3%} | "
            f"{std:.3%} | {blk:.3%} |"
        )


def block_sizes(n: int):
    print(f"\n### Block size ({n:,} entries: random ids, row digests)\n")
    print(
        "| Versions | Block (raw) | Total per entry | Blocks per entry | Index part | Entries per block | "
        "Block, compressed | Encode | Decode one block |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    rng = random.Random(4)
    keys = random_ids(n, rng)
    for vname, vgen in list(VERSIONS.items())[:1]:
        vers = vgen(n, random.Random(5))
        for bs in (4, 16, 32, 64, 128, 256):
            t = time.perf_counter()
            data = K.encode_file(keys, vers, bytes(n), block_size=bs * 1024)
            enc = time.perf_counter() - t
            lay = layout(data)
            blocks = lay["tail"]["blocks"]
            sample = blocks[:: max(1, len(blocks) // 200)]
            t = time.perf_counter()
            for _, off, size, _, _ in sample:
                K.decode_block(data[off : off + size], 1)
            dec = (time.perf_counter() - t) / len(sample)
            mark = " (default)" if bs == 64 else ""
            print(
                f"| {vname.split(' (')[0]} | {bs} KiB{mark} | {len(data) / n:.1f} B | {lay['blocks'] / n:.1f} B | "
                f"{lay['index'] / 1e3:,.0f} KB | {n / len(blocks):,.0f} | {lay['blocks'] / len(blocks) / 1024:.1f} KiB | "
                f"{n / enc / 1e6:.2f} M/s | {dec * 1e3:.2f} ms |"
            )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=float, default=1e6)
    ap.add_argument("--probes", type=float, default=2e6)
    args = ap.parse_args()
    n = int(args.n)
    entry_sizes(n)
    false_positives(n, int(args.probes))
    block_sizes(n)


if __name__ == "__main__":
    main()
