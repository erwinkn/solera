"""CPU throughput of the .kx format, native vs pure Python (no I/O)."""

import random
import sys
import time

import cursus_native
from cursus.keys import _python

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000


def gen(n, seed=0):
    rng = random.Random(seed)
    keys = sorted({f"site-{rng.randrange(10**12):012d}/file-{i}".encode() for i in range(n)})
    vers = [rng.randbytes(16) for _ in keys]
    return keys, vers, bytes(len(keys))


def timed(label, n, fn, *args, **kw):
    t = time.perf_counter()
    out = fn(*args, **kw)
    dt = time.perf_counter() - t
    print(f"  {label:34s} {dt:8.3f} s   {n / dt / 1e6:7.2f} M entries/s")
    return out


keys, vers, dele = gen(N)
n = len(keys)
raw = sum(len(k) + len(v) for k, v in zip(keys, vers, strict=True))
print(f"{n:,} entries, {raw / n:.1f} B raw per entry (key {sum(map(len, keys)) / n:.1f} B, version 16 B)")
for name, impl in (("native", cursus_native), ("python", _python)):
    if name == "python" and n > 2_000_000:
        continue
    print(name)
    data = timed("encode (blocks + filters)", n, impl.encode_file, keys, vers, dele)
    footer = _python.parse_footer(data[-48:])
    tail = _python.parse_tail(data[footer["filters_offset"]:], len(data))
    body = footer["filters_offset"]
    print(f"  file {len(data)/1e6:.1f} MB: blocks {body/n:.1f} B/entry, filters {footer['filters_length']/n:.2f} B/entry, "
          f"tail {(len(data)-body)/1e6:.2f} MB, {len(tail['blocks'])} blocks")

    def decode_all(impl, data, tail):
        for _, off, size, _, _ in tail["blocks"]:
            impl.decode_block(data[off:off + size], tail["codec"])
    timed("decode every block", n, decode_all, impl, data, tail)
    nb, kk, bits = tail["pair_filter"]
    probe = keys[:: max(1, n // 100_000)]
    other = [b"\x00" * 16] * len(probe)
    timed("pair filter check (changed versions)", len(probe), impl.bloom_check_pairs, bits, nb, kk, probe, other)
    shuffled = keys[:]
    random.Random(1).shuffle(shuffled)
    m = min(n, 1_000_000)
    timed("sort", m, impl.sort_entries, shuffled[:m], vers[:m], dele[:m])
    half_a = impl.encode_file(keys[::2], vers[::2], dele[::2])
    half_b = impl.encode_file(keys[1::2], vers[1::2], dele[1::2])
    timed("merge two files into one", n, impl.merge_files, [half_a, half_b], drop_deleted=True)
