"""The `.kx` key index format (docs/key-index-format.md): the native
extension against the pure-Python reference, each reading the other's files."""

import random

import pytest
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
    gone = [keys[i] for i, d in enumerate(deleted) if d]
    nbits, k, bits = tail["tomb_filter"]
    assert reader.bloom_check_tombstones(bits, nbits, k, gone) == b"\x01" * len(gone)


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
def test_a_file_has_two_filters_keys_and_tombstones(impl):
    """docs/versions.md §3: no pair filter — a written key is a change
    whatever the index holds, so nothing asks whether a version is live."""

    data = encode(impl, [b"a", b"b"], [1, 2], b"\x00\x01")
    tail = _python.parse_tail(data[_python.parse_footer(data[-48:])["filters_offset"] :], len(data))
    assert "pair_filter" not in tail
    nbits, k, bits = tail["key_filter"]
    assert impl.bloom_check_keys(bits, nbits, k, [b"a", b"b"]) == b"\x01\x01"
    nbits, k, bits = tail["tomb_filter"]
    assert impl.bloom_check_tombstones(bits, nbits, k, [b"b"]) == b"\x01"


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


@pytest.mark.xfail(strict=True, reason="F23: open")
@pytest.mark.parametrize("impl", IMPLS)
def test_a_varint_past_64_bits_is_refused(impl):
    """F23: a block entry whose generation is a 10-byte varint with bits
    past 2^64 (`ff` x 9, `7f`). The native reader dropped those bits and
    read 2^64 - 1; the Python reference read 2^70 - 1. Both must refuse it."""

    entry = b"\x00" + b"\x01a" + b"\x00" + b"\xff" * 9 + b"\x7f"  # shared, suffix, flags, generation
    with pytest.raises(impl.FormatError, match="varint"):
        impl.decode_block(entry, 0)
    impl.decode_block(entry[:-1] + b"\x01", 0)  # 2^64 - 1 itself fits


def test_garbage_files_cross_decode():
    """docs/key-index-format.md § Garbage files: a key may repeat, at the
    generations a compaction dropped; each implementation reads the other's."""

    rng = random.Random(11)
    keys = sorted(f"k{rng.randrange(5000):05d}".encode() for _ in range(20_000))  # repeats
    generations = [rng.randrange(1 << 40) for _ in keys]
    data = _python.encode_garbage(keys, generations)
    assert _native.decode_garbage(data) == (keys, generations)
    bad = bytearray(data)
    bad[20] ^= 1
    for impl in (_python, _native):
        with pytest.raises(ValueError):
            impl.decode_garbage(bytes(bad))


def test_a_compaction_names_every_object_it_drops():
    """Every live entry a merge drops for a newer one of its key — from any
    run, so a key can appear at several generations — goes to the garbage
    files; tombstones, and entries at the generation that wins, do not."""

    l2 = _native.encode_file([b"a", b"b", b"c"], [1, 1, 1], b"\x00\x00\x00")
    l1 = _native.encode_file([b"a", b"b"], [2, 2], b"\x00\x01")  # b deleted
    l0 = _native.encode_file([b"a", b"c"], [3, 1], b"\x00\x00")  # c: the same object
    job = _native.Merge.compact(3, drop_deleted=True, garbage=True)
    garbage = []
    files = drive(job, [[l0], [l1], [l2]], on_garbage=garbage.append)
    _, k, g, _, _ = decode_all(_native, files[0])
    assert list(zip(k, g, strict=True)) == [(b"a", 3), (b"c", 1)]
    [gf] = garbage
    ks, gs = _python.decode_garbage(gf)
    assert list(zip(ks, gs, strict=True)) == [(b"a", 2), (b"a", 1), (b"b", 1)]
    assert job.garbage == 3


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
        lambda b: _native.decode_garbage(b),
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
