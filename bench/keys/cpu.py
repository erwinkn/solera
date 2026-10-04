"""CPU throughput of the .kx format, native vs pure Python (no I/O).

    uv run python bench/keys/cpu.py [entries] [payload bytes]

Entries carry a generation and, given a size, a random payload (a source's version)."""

import random
import sys
import time

from solera import _native

from tests.sdk import keys_reference as _python

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
PAYLOAD = int(sys.argv[2]) if len(sys.argv) > 2 else 0


def gen(n, seed=0):
    rng = random.Random(seed)
    keys = sorted({f"site-{rng.randrange(10**12):012d}/file-{i}".encode() for i in range(n)})
    gens = [rng.randrange(1, 10**6) for _ in keys]
    payloads = [rng.randbytes(PAYLOAD) for _ in keys] if PAYLOAD else None
    return keys, gens, payloads, bytes(len(keys))


def timed(label, n, fn, *args, **kw):
    t = time.perf_counter()
    out = fn(*args, **kw)
    dt = time.perf_counter() - t
    print(f"  {label:34s} {dt:8.3f} s   {n / dt / 1e6:7.2f} M entries/s")
    return out


def merge_spans(files, base):
    """The native span merge, no endpoint inside, each whole file fed as one segment."""

    job = _native.Merge.spans(len(files), endpoints=[], base=base)
    fed, out = set(), []
    while (step := job.step()) is not None:
        kind, x = step
        if kind == "file":
            out.append(x)
        elif x in fed:
            job.end(x)
        else:
            fed.add(x)
            idx = _native.parse_index(files[x], len(files[x]))
            job.feed(x, files[x], [(off, size, crc) for _, off, size, _, crc in idx["blocks"]], idx["codec"])
    return out


def half(xs, i):
    return None if xs is None else xs[i::2]


keys, gens, payloads, dele = gen(N)
n = len(keys)
raw = sum(map(len, keys)) + n * PAYLOAD
print(
    f"{n:,} entries, {raw / n:.1f} B raw per entry "
    f"(key {sum(map(len, keys)) / n:.1f} B, payload {PAYLOAD} B, and a generation)"
)
for name, impl in (("native", _native), ("python", _python)):
    if name == "python" and n > 2_000_000:
        continue
    print(name)
    data = timed("encode (blocks + filters)", n, impl.encode_file, keys, gens, dele, payloads=payloads)
    footer = _python.parse_footer(data[-48:])
    tail = _python.parse_tail(data[footer["filters_offset"] :], len(data))
    body = footer["filters_offset"]
    print(
        f"  file {len(data) / 1e6:.1f} MB: blocks {body / n:.1f} B/entry, filters {footer['filters_length'] / n:.2f} B/entry, "
        f"tail {(len(data) - body) / 1e6:.2f} MB, {len(tail['blocks'])} blocks"
    )

    def decode_all(impl, data, tail):
        for _, off, size, _, _ in tail["blocks"]:
            impl.decode_block(data[off : off + size], tail["codec"])

    timed("decode every block", n, decode_all, impl, data, tail)
    nb, kk, bits = tail["key_filter"]
    probe = keys[:: max(1, n // 100_000)]
    timed("key filter check", len(probe), impl.bloom_check_keys, bits, nb, kk, probe)
    half_a = impl.encode_file(keys[::2], gens[::2], dele[::2], payloads=half(payloads, 0))
    half_b = impl.encode_file(keys[1::2], gens[1::2], dele[1::2], payloads=half(payloads, 1))
    merge = _python.merge_spans if impl is _python else merge_spans
    timed("merge two files into one", n, merge, [half_a, half_b], base=True)
