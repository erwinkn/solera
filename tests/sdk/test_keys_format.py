"""The `.kx` key index format (docs/key-index-format.md): the native
extension against the pure-Python reference, each reading the other's files."""

import random

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from solera import _native

from . import keys_reference as _python
from .keys_driver import drive

_MODULES = {"python": _python, "native": _native}
IMPLS = [pytest.param(m, id=name) for name, m in _MODULES.items()]
CROSS = [
    pytest.param(w, r, id=f"{wn}-writes-{rn}-reads")
    for wn, w in _MODULES.items()
    for rn, r in _MODULES.items()
]


def entries(n, seed=0, deleted_every=0):
    """Keys, the generations that wrote them, deleted flags, and payloads:
    a source's version on some, none on the rest."""

    rng = random.Random(seed)
    keys = sorted({f"site-{rng.randrange(10**9):09d}/file-{rng.randrange(10**6)}".encode() for _ in range(n)})
    generations = [rng.randrange(1, 1 << 40) for _ in keys]
    deleted = bytes(1 if deleted_every and i % deleted_every == 0 else 0 for i in range(len(keys)))
    payloads = [
        rng.randbytes(rng.randrange(12)) if i % 3 == 0 and not deleted[i] else None for i in range(len(keys))
    ]
    return keys, generations, deleted, payloads


def encode(impl, keys, generations, deleted, payloads=None, **kw):
    return impl.encode_file(keys, generations, deleted, payloads=payloads, **kw)


def decode_all(impl, data):
    footer = _python.parse_footer(data[-_python.FOOTER_SIZE :])
    tail = _python.parse_tail(data[footer["filters_offset"] :], len(data))
    keys, generations, flags, payloads = [], [], bytearray(), []
    for _, off, size, count, crc in tail["blocks"]:
        blk = data[off : off + size]
        _python.check_block(blk, crc)
        k, g, f, p, _ = impl.decode_block(blk, tail["codec"])
        assert len(k) == count
        keys += k
        generations += g
        flags += f
        payloads += p
    return tail, keys, generations, bytes(flags), payloads


@pytest.mark.parametrize("writer,reader", CROSS)
def test_roundtrip(writer, reader):
    keys, generations, deleted, payloads = entries(5000, deleted_every=7)
    data = encode(writer, keys, generations, deleted, payloads, block_size=4096)
    tail, k, g, f, p = decode_all(reader, data)
    assert (k, g, f, p) == (keys, generations, deleted, payloads)
    assert tail["entries"] == len(keys)
    assert tail["min_key"] == keys[0] and tail["max_key"] == keys[-1]
    assert len(tail["blocks"]) > 10  # small blocks: prefix compression restarts per block
    assert [b[0] for b in tail["blocks"]] == sorted(b[0] for b in tail["blocks"])


@pytest.mark.parametrize("writer,reader", CROSS)
def test_tail_alone_is_enough(writer, reader):
    """A reader only needs the tail to plan: filters, index and footer."""

    keys, generations, deleted, payloads = entries(2000)
    data = encode(writer, keys, generations, deleted, payloads, block_size=2048)
    footer = _python.parse_footer(data[-48:])
    tail_len = len(data) - footer["filters_offset"]
    tail = _python.parse_tail(data[-tail_len:], len(data))
    # Locate a key's block from the index, then decode just that block.
    target = keys[1234]
    firsts = [b[0] for b in tail["blocks"]]
    i = max(j for j, fk in enumerate(firsts) if fk <= target)
    _, off, size, _, _ = tail["blocks"][i]
    k, g, _, _, _ = reader.decode_block(data[off : off + size], tail["codec"])
    assert g[k.index(target)] == generations[1234]


@pytest.mark.parametrize("writer,reader", CROSS)
def test_filters_have_no_false_negatives(writer, reader):
    keys, generations, deleted, payloads = entries(3000, deleted_every=5)
    data = encode(writer, keys, generations, deleted, payloads)
    tail = _python.parse_tail(data[_python.parse_footer(data[-48:])["filters_offset"] :], len(data))
    nbits, k, bits = tail["key_filter"]
    assert reader.bloom_check_keys(bits, nbits, k, keys) == b"\x01" * len(keys)


@pytest.mark.parametrize("impl", IMPLS)
def test_filter_false_positive_rate(impl):
    keys, generations, deleted, _ = entries(20000)
    data = encode(impl, keys, generations, deleted)
    tail = _python.parse_tail(data[_python.parse_footer(data[-48:])["filters_offset"] :], len(data))
    absent = [b"absent-%d" % i for i in range(20000)]
    nbits, k, bits = tail["key_filter"]
    fp = sum(impl.bloom_check_keys(bits, nbits, k, absent)) / len(absent)
    assert fp < 0.005  # 14 bits per item, k=10: ~0.1% expected


@pytest.mark.parametrize("impl", IMPLS)
def test_a_file_has_one_filter_its_keys(impl):
    """Format v4: one filter, every key, deleted ones included. Writes are
    exact, so a key the filter holds is always read for its predecessor,
    and a filter of tombstones would decide nothing (docs/key-index-design.md)."""

    data = encode(impl, [b"a", b"b"], [1, 2], b"\x00\x01")
    tail = _python.parse_tail(data[_python.parse_footer(data[-48:])["filters_offset"] :], len(data))
    assert "tomb_filter" not in tail and "pair_filter" not in tail
    nbits, k, bits = tail["key_filter"]
    assert impl.bloom_check_keys(bits, nbits, k, [b"a", b"b"]) == b"\x01\x01"


@pytest.mark.parametrize("writer,reader", CROSS)
def test_empty_file(writer, reader):
    data = encode(writer, [], [], b"")
    tail, k, g, f, p = decode_all(reader, data)
    assert (k, g, f, p) == ([], [], b"", [])
    assert tail["entries"] == 0 and tail["blocks"] == []
    assert tail["key_filter"][0] == 512


@pytest.mark.parametrize("impl", IMPLS)
def test_rejects_unsorted_or_duplicate_keys(impl):
    with pytest.raises(ValueError):
        encode(impl, [b"b", b"a"], [0, 0], b"\x00\x00")
    with pytest.raises(ValueError):
        encode(impl, [b"a", b"a"], [0, 0], b"\x00\x00")


@pytest.mark.parametrize("writer,reader", CROSS)
def test_tails_parse_alike(writer, reader):
    keys, generations, deleted, payloads = entries(3000, deleted_every=9)
    data = encode(writer, keys, generations, deleted, payloads, block_size=2048)
    footer = reader.parse_footer(data[-48:])
    assert reader.parse_tail(data[footer["filters_offset"] :], len(data)) == _python.parse_tail(
        data[footer["filters_offset"] :], len(data)
    )
    assert reader.parse_index(data[footer["index_offset"] :], len(data)) == _python.parse_index(
        data[footer["index_offset"] :], len(data)
    )


@pytest.mark.parametrize("impl", IMPLS)
def test_detects_corruption(impl):
    keys, generations, deleted, payloads = entries(500)
    data = bytearray(encode(impl, keys, generations, deleted, payloads, block_size=1024))
    tail = impl.parse_tail(bytes(data[impl.parse_footer(bytes(data[-48:]))["filters_offset"] :]), len(data))
    _, off, size, _, crc = tail["blocks"][3]
    data[off + size // 2] ^= 0xFF
    with pytest.raises(impl.FormatError):
        impl.check_block(bytes(data[off : off + size]), crc)
    data[-60] ^= 0xFF  # inside the index
    with pytest.raises(impl.FormatError):
        impl.parse_tail(bytes(data[tail["filters_offset"] :]), len(data))
    with pytest.raises(impl.FormatError):
        impl.parse_footer(b"XXXX" + bytes(data[-44:]))


def merge(impl, files, **kw):
    """Merge whole files, newest first: the reference's `merge_files`, or a native compaction."""

    if impl is _python:
        return _python.merge_files(files, **kw)
    job = _native.Merge.compact(len(files), **kw)
    return drive(job, [[f] for f in files])


@pytest.mark.parametrize("writer,reader", CROSS)
def test_merge_newest_wins(writer, reader):
    old = encode(
        writer, [b"a", b"b", b"c", b"d"], [1, 1, 1, 1], b"\x00\x00\x00\x00", [b"r", None, None, None]
    )
    new = encode(writer, [b"b", b"c", b"e"], [2, 2, 2], b"\x00\x01\x00")  # c deleted
    [merged] = merge(reader, [new, old], drop_deleted=False)
    _, k, g, f, p = decode_all(reader, merged)
    assert list(zip(k, g, f, p, strict=True)) == [
        (b"a", 1, 0, b"r"),
        (b"b", 2, 0, None),
        (b"c", 2, 1, None),
        (b"d", 1, 0, None),
        (b"e", 2, 0, None),
    ]
    [bottom] = merge(reader, [new, old], drop_deleted=True)
    _, k, _, _, _ = decode_all(reader, bottom)
    assert k == [b"a", b"b", b"d", b"e"]


@pytest.mark.parametrize("impl", IMPLS)
def test_merge_splits_large_output(impl):
    keys, generations, deleted, payloads = entries(20000)
    a = encode(impl, keys[::2], generations[::2], deleted[::2], payloads[::2])
    b = encode(impl, keys[1::2], generations[1::2], deleted[1::2], payloads[1::2])
    out = merge(impl, [a, b], drop_deleted=True, max_file_bytes=100_000)
    assert len(out) > 2
    seen = []
    for f in out:
        _, k, _, _, _ = decode_all(impl, f)
        seen += k
    assert seen == keys  # split files are consecutive, non-overlapping key ranges


def blocks_of(data):
    tail = _python.parse_tail(data[_python.parse_footer(data[-48:])["filters_offset"] :], len(data))
    return tail, [data[off : off + size] for _, off, size, _, _ in tail["blocks"]]


@pytest.mark.parametrize("writer,reader", CROSS)
def test_lookup(writer, reader):
    keys, generations, deleted, payloads = entries(3000, deleted_every=11)
    tail, blocks = blocks_of(encode(writer, keys, generations, deleted, payloads, block_size=1024))
    probe = sorted([keys[5], keys[1500], keys[2999], b"zzz-absent", keys[11], b"aaa-absent", keys[3]])
    found, gens, dels, pays = reader.lookup(blocks, tail["codec"], probe)
    for key, f, g, d, p in zip(probe, found, gens, dels, pays, strict=True):
        if key.endswith(b"-absent"):
            assert (f, g, d, p) == (0, 0, 0, None)
        else:
            i = keys.index(key)
            assert (f, g, d, p) == (1, generations[i], deleted[i], payloads[i])


@pytest.mark.parametrize("writer,reader", CROSS)
def test_merge_range(writer, reader):
    # Each run in its own file's codec: one stored, one compressed.
    old = encode(writer, [b"a", b"b", b"c", b"d", b"f"], [1] * 5, b"\x00" * 5, block_size=8, codec=0)
    new = encode(writer, [b"b", b"c", b"e"], [2] * 3, b"\x00\x01\x00", [b"x", None, None], block_size=8)
    runs = [blocks_of(new)[1], blocks_of(old)[1]]
    k, g, f, p = reader.merge_range(runs, [1, 0], None, None, False)
    assert list(zip(k, g, f, p, strict=True)) == [
        (b"a", 1, 0, None),
        (b"b", 2, 0, b"x"),
        (b"c", 2, 1, None),
        (b"d", 1, 0, None),
        (b"e", 2, 0, None),
        (b"f", 1, 0, None),
    ]
    k, _, _, _ = reader.merge_range(runs, [1, 0], b"b", b"e", True)  # (after, upto], tombstones dropped
    assert k == [b"d", b"e"]


@pytest.mark.parametrize("impl", IMPLS)
def test_detects_filter_corruption_and_reads_the_index_alone(impl):
    keys, generations, deleted, payloads = entries(800)
    data = bytearray(encode(impl, keys, generations, deleted, payloads, block_size=1024))
    footer = _python.parse_footer(bytes(data[-48:]))
    # The index part alone parses (and is checksummed) without the filters.
    idx = _python.parse_index(bytes(data[footer["index_offset"] :]), len(data))
    assert idx["min_key"] == keys[0] and "key_filter" not in idx
    data[footer["filters_offset"] + 10] ^= 0xFF
    with pytest.raises(impl.FormatError):
        impl.parse_tail(bytes(data[footer["filters_offset"] :]), len(data))


@pytest.mark.parametrize("writer,reader", CROSS)
def test_generations_payloads_and_predecessors(writer, reader):
    """Every entry has the generation that wrote it, and may carry a
    payload; a delta entry may also have the generation its key had
    before, which a compaction drops — keeping each entry's payload."""

    keys, generations, deleted, payloads = entries(3000, deleted_every=7)
    predecessors = [i if i % 3 else None for i in range(len(keys))]
    data = encode(writer, keys, generations, deleted, payloads, predecessors=predecessors, block_size=1024)
    footer = reader.parse_footer(data[-48:])
    tail = reader.parse_tail(data[footer["filters_offset"] :], len(data))
    got_g, got_p = [], []
    for _, off, size, _, _ in tail["blocks"]:
        _, gs, _, _, ps = reader.decode_block(data[off : off + size], tail["codec"])
        got_g += gs
        got_p += ps
    assert got_g == generations and got_p == predecessors
    blocks = [data[off : off + size] for _, off, size, _, _ in tail["blocks"]]
    assert reader.lookup(blocks, tail["codec"], [keys[5]])[1] == [generations[5]]
    assert reader.merge_range([blocks], [tail["codec"]], None, None, False)[1] == generations
    [merged] = merge(reader, [data], drop_deleted=False)
    assert [(e[1], e[3], e[4]) for e in _python.iter_file(merged)] == [
        (g, p, None) for g, p in zip(generations, payloads, strict=True)
    ]


@pytest.mark.parametrize("impl", IMPLS)
def test_rejects_the_previous_format_version(impl):
    data = bytearray(encode(impl, [b"a"], [1], b"\x00"))
    data[-44:-42] = (2).to_bytes(2, "little")
    with pytest.raises(impl.FormatError, match="version"):
        impl.parse_footer(bytes(data[-48:]))


@pytest.mark.parametrize("impl", IMPLS)
def test_a_varint_past_64_bits_is_refused(impl):
    """F23: a block entry whose generation is a 10-byte varint with bits
    past 2^64 (`ff` x 9, `7f`). The native reader dropped those bits and
    read 2^64 - 1; the Python reference read 2^70 - 1. Both refuse it."""

    entry = b"\x00" + b"\x01a" + b"\x00" + b"\xff" * 9 + b"\x7f"  # shared, suffix, flags, generation
    with pytest.raises(impl.FormatError, match="varint"):
        impl.decode_block(entry, 0)
    impl.decode_block(entry[:-1] + b"\x01", 0)  # 2^64 - 1 itself fits


def _zlib_bomb(payload_bytes: int) -> bytes:
    """One well-formed block entry, key `a`, with a payload of zeros, zlib
    compressed: a few KB that inflate a thousandfold."""

    import zlib

    head = bytearray(b"\x00\x01a\x04\x01")  # shared 0, suffix "a", flags: payload, generation 1
    _python.put_varint(head, payload_bytes)
    z = zlib.compressobj(9)
    out = z.compress(bytes(head))
    chunk = bytes(1 << 20)
    for _ in range(payload_bytes >> 20):
        out += z.compress(chunk)
    return out + z.flush()


@pytest.mark.parametrize(
    "read",
    [
        lambda b: _native.lookup([b], 1, [b"a"]),
        lambda b: _native.merge_range([[b]], [1], None, None, False),
        lambda b: _native.decode_block(b, 1),
        lambda b: _python.decode_block(b, 1),
    ],
    ids=["lookup", "merge_range", "decode_block", "reference"],
)
def test_a_block_that_inflates_past_its_bound_is_refused(read):
    """F29: the readers that take a block without a limit inflated whatever
    its zlib stream held: here 32 KB to 32 MiB; a 64 MB block could ask for
    gigabytes. Format v4 bounds what any block decodes to (16 MiB, which
    writers refuse to exceed), and one past it fails fast, before it is
    inflated whole."""

    bomb = _zlib_bomb(32 << 20)
    assert len(bomb) < 64 << 10
    with pytest.raises(ValueError):
        read(bomb)


def test_a_writer_refuses_a_block_past_the_bound():
    """F29's other side: no writer produces a block readers would refuse."""

    with pytest.raises(ValueError):
        _native.encode_file([b"a"], [1], b"\x00", payloads=[bytes(17 << 20)])


def test_malformed_input_raises_errors_never_panics():
    """Whatever the bytes — mutated files and blocks, truncations, noise —
    every parser and kernel raises a `ValueError` (`FormatError`), never a
    Rust panic: a length is checked before it is added or sliced with."""

    rng = random.Random(11)
    keys, generations, deleted, payloads = entries(300)
    files = [encode(_native, keys, generations, deleted, payloads, block_size=512, codec=c) for c in (0, 1)]
    raw_blocks = blocks_of(files[0])[1]
    blobs = [b"\x00" + b"\xff" * 9 + b"\x01" + b"\x00" * 10]  # a suffix length that overflows
    for _ in range(2000):
        base = bytearray(rng.choice(files + raw_blocks))
        for _ in range(rng.randrange(1, 4)):
            base[rng.randrange(len(base))] = rng.randrange(256)
        blobs.append(bytes(base[: rng.randrange(len(base) + 1)] if rng.random() < 0.3 else base))
        blobs.append(rng.randbytes(rng.randrange(80)))
    parsers = [
        lambda b: _native.decode_block(b, 0),
        lambda b: _native.decode_block(b, 1),
        lambda b: _native.lookup([b], 0, [b"site-1"]),
        lambda b: _native.merge_range([[b]], [0], None, None, False),
        lambda b: _native.SortedEntries.decode(b),
        lambda b: _native.parse_index(b, len(b)),
        lambda b: _native.parse_index(b, len(b) // 2),  # a part longer than its file
        lambda b: _native.parse_tail(b, len(b)),
    ]
    for blob in blobs:
        for parse in parsers:
            try:
                parse(blob)
            except ValueError:
                pass


def test_a_sorted_run_round_trips_and_checks_what_it_decodes():
    run = _native.SortedEntries.of([b"c", b"a", b"d"], [b"3", None, b""], [b"b", b"b"])
    assert (len(run), run.upserts, run.removes) == (4, 3, 1)
    assert run.entries() == (
        [b"a", b"b", b"c", b"d"],
        [0, 0, 0, 0],
        b"\x00\x01\x00\x00",
        [None, None, b"3", b""],
    )
    data = run.encode()
    assert _native.SortedEntries.decode(data).entries() == run.entries()
    with pytest.raises(_native.LimitError):
        _native.SortedEntries.decode(data, max_entries=2)
    with pytest.raises(_native.LimitError):
        _native.SortedEntries.decode(data, max_bytes=4)
    with pytest.raises(ValueError):
        _native.SortedEntries.of([b"a"], None, [b"a"])  # written and removed
    with pytest.raises(ValueError):
        _native.SortedEntries.of([b"a", b"a"])


def test_a_runs_index_is_inside_its_decoding_budget():
    """Review round 2, finding 1: an empty file whose index decompresses to
    128 MiB — three bytes of grammar, then zeros — is refused at the
    caller's byte limit, not decoded whole; and an index with bytes past its
    grammar is malformed whatever the limit."""

    import struct
    import zlib

    data = bytearray(encode(_native, [], [], b""))
    foot = len(data) - _native.FOOTER_SIZE
    at, length = struct.unpack_from("<Q", data, foot + 28)[0], struct.unpack_from("<I", data, foot + 36)[0]

    def with_index(raw: bytes) -> bytes:
        packed = zlib.compress(raw, 9)
        out = bytearray(data[:at]) + packed + data[at + length :]
        f = len(out) - _native.FOOTER_SIZE
        struct.pack_into("<I", out, f + 36, len(packed))
        struct.pack_into("<I", out, f + 40, zlib.crc32(packed))
        return bytes(out)

    bomb = with_index(b"\0\0\0" + bytes(128 * 2**20))
    assert len(bomb) < 2**20
    with pytest.raises(_native.LimitError):
        _native.SortedEntries.decode(bomb, max_bytes=2**20)
    with pytest.raises(_native.FormatError):
        _native.SortedEntries.decode(with_index(b"\0\0\0" + bytes(100)))  # trailing bytes
    assert len(_native.SortedEntries.decode(with_index(b"\0\0\0"))) == 0


def test_an_overlong_varint_is_refused_by_the_reference():
    """As native: a u64 takes ten bytes at most, the tenth its top bit alone."""

    assert _python.get_varint(b"\xff" * 9 + b"\x01", 0) == (2**64 - 1, 10)
    with pytest.raises(ValueError, match="too long"):
        _python.get_varint(b"\xff" * 9 + b"\x7f", 0)


# -- the readers' varints and filters, native against the reference ------------------------
# (what Kani once proved for small inputs: docs/verification.md, "Kani: tried, then dropped")


def _generation_read(impl, varint: bytes):
    """The generation a block entry with this varint reads as, or "refused"."""

    try:
        return impl.decode_block(b"\x00\x01a\x00" + varint, 0)[1][0]
    except ValueError:
        return "refused"


def _leb128(data: bytes) -> int | None:
    """The exact value of a varint that ends at its last byte, or None."""

    if not data or data[-1] >= 0x80 or any(b < 0x80 for b in data[:-1]):
        return None
    return sum((b & 0x7F) << (7 * i) for i, b in enumerate(data))


def _varint_edges():
    for length in range(1, 12):
        for fill in (0x80, 0xFF):  # continuation bytes holding all zeros, or all ones
            for last in range(256):
                yield bytes([fill] * (length - 1) + [last])
    for fill in (0x80, 0xFF):  # every pattern of the ninth and tenth bytes
        for ninth in range(256):
            for tenth in range(256):
                yield bytes([fill] * 8 + [ninth, tenth])


def test_varints_read_alike_and_whole():
    """F23's property, at every length from 1 to 11 bytes and every pattern
    of the ninth and tenth bytes: native and the reference read the same
    generation or both refuse, and what they read is the bytes' exact value,
    below 2^64. (Before ab0c346 native read ff x 9, 7f as 2^64 - 1.)"""

    for varint in _varint_edges():
        native, reference = _generation_read(_native, varint), _generation_read(_python, varint)
        assert native == reference, (varint.hex(), native, reference)
        if native != "refused":
            assert native == _leb128(varint) and native < 2**64, (varint.hex(), native)


@pytest.mark.parametrize("writer,reader", CROSS)
def test_generations_at_the_varint_edges_round_trip(writer, reader):
    """Every u64 edge, written as a generation and a predecessor, reads back."""

    edges = sorted(
        {0, 2**64 - 1} | {2**k + d for k in range(64) for d in (-1, 0, 1) if 0 <= 2**k + d < 2**64}
    )
    keys = [f"k{i:03d}".encode() for i in range(len(edges))]
    data = encode(writer, keys, edges, bytes(len(keys)), predecessors=edges[::-1], codec=0)
    _, _, generations, _, _ = decode_all(reader, data)
    assert generations == edges


@settings(max_examples=300, deadline=None)
@given(filters=st.binary(max_size=80), matching=st.booleans(), short=st.integers(0, 3))
def test_filters_read_alike_whatever_they_hold(filters, matching, short):
    """Any bytes where a file's filters go, with a checksum that matches or
    not, and a length the footer may cut short: native and the reference
    read the same filters, of exactly nbits / 8 bytes each, or both refuse.
    (Any offsets are the kx-file fuzz target's, which makes the CRC match.)"""

    import struct
    import zlib

    base = encode(_native, [b"a", b"b"], [1, 2], b"\x00\x01")
    footer = bytearray(base[-_python.FOOTER_SIZE :])
    f_at, i_at, i_len = (
        struct.unpack_from("<Q", footer, 16)[0],
        struct.unpack_from("<Q", footer, 28)[0],
        struct.unpack_from("<I", footer, 36)[0],
    )
    region = filters + (struct.pack("<I", zlib.crc32(filters)) if matching else b"\x00\x00\x00\x00")
    struct.pack_into("<I", footer, 24, max(0, len(region) - short))
    struct.pack_into("<Q", footer, 28, f_at + len(region))
    data = base[:f_at] + region + base[i_at : i_at + i_len] + bytes(footer)

    def read(impl):
        try:
            tail = impl.parse_tail(data, len(data))
        except ValueError:
            return "refused"
        return tail["key_filter"]

    native, reference = read(_native), read(_python)
    assert native == reference
    if native != "refused":
        nbits, _, bits = native
        assert len(bits) == nbits // 8
