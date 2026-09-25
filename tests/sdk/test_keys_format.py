"""The `.kx` key index format (docs/key-index-format.md): both implementations,
and each reading the other's files."""

import random

import pytest
from cursus.keys import _python

try:
    import cursus_native
except ImportError:  # the extension is optional; the reference always runs
    cursus_native = None

_MODULES = {"python": _python}
if cursus_native is not None:
    _MODULES["native"] = cursus_native
IMPLS = [pytest.param(m, id=name) for name, m in _MODULES.items()]
CROSS = [
    pytest.param(w, r, id=f"{wn}-writes-{rn}-reads")
    for wn, w in _MODULES.items()
    for rn, r in _MODULES.items()
]


def entries(n, seed=0, deleted_every=0):
    rng = random.Random(seed)
    keys = sorted({f"site-{rng.randrange(10**9):09d}/file-{rng.randrange(10**6)}".encode() for _ in range(n)})
    versions = [rng.randbytes(16) for _ in keys]
    deleted = bytes(1 if deleted_every and i % deleted_every == 0 else 0 for i in range(len(keys)))
    return keys, versions, deleted


def decode_all(impl, data):
    footer = _python.parse_footer(data[-_python.FOOTER_SIZE :])
    tail = _python.parse_tail(data[footer["filters_offset"] :], len(data))
    keys, versions, flags = [], [], bytearray()
    for _, off, size, count, crc in tail["blocks"]:
        blk = data[off : off + size]
        _python.check_block(blk, crc)
        k, v, f = impl.decode_block(blk, tail["codec"])
        assert len(k) == count
        keys += k
        versions += v
        flags += f
    return tail, keys, versions, bytes(flags)


@pytest.mark.parametrize("writer,reader", CROSS)
def test_roundtrip(writer, reader):
    keys, versions, deleted = entries(5000, deleted_every=7)
    data = writer.encode_file(keys, versions, deleted, block_size=4096)
    tail, k, v, f = decode_all(reader, data)
    assert (k, v, f) == (keys, versions, deleted)
    assert tail["entries"] == len(keys)
    assert tail["min_key"] == keys[0] and tail["max_key"] == keys[-1]
    assert len(tail["blocks"]) > 10  # small blocks: prefix compression restarts per block
    assert [b[0] for b in tail["blocks"]] == sorted(b[0] for b in tail["blocks"])


@pytest.mark.parametrize("writer,reader", CROSS)
def test_tail_alone_is_enough(writer, reader):
    """A reader only needs the tail to plan: filters, index and footer."""

    keys, versions, deleted = entries(2000)
    data = writer.encode_file(keys, versions, deleted, block_size=2048)
    footer = _python.parse_footer(data[-48:])
    tail_len = len(data) - footer["filters_offset"]
    tail = _python.parse_tail(data[-tail_len:], len(data))
    # Locate a key's block from the index, then decode just that block.
    target = keys[1234]
    firsts = [b[0] for b in tail["blocks"]]
    i = max(j for j, fk in enumerate(firsts) if fk <= target)
    _, off, size, _, _ = tail["blocks"][i]
    k, v, _ = reader.decode_block(data[off : off + size], tail["codec"])
    assert v[k.index(target)] == versions[1234]


@pytest.mark.parametrize("writer,reader", CROSS)
def test_filters_have_no_false_negatives(writer, reader):
    keys, versions, deleted = entries(3000, deleted_every=5)
    data = writer.encode_file(keys, versions, deleted)
    tail = _python.parse_tail(data[_python.parse_footer(data[-48:])["filters_offset"] :], len(data))
    nbits, k, bits = tail["key_filter"]
    assert reader.bloom_check_keys(bits, nbits, k, keys) == b"\x01" * len(keys)
    live = [i for i, d in enumerate(deleted) if not d]
    nbits, k, bits = tail["pair_filter"]
    got = reader.bloom_check_pairs(bits, nbits, k, [keys[i] for i in live], [versions[i] for i in live])
    assert got == b"\x01" * len(live)


@pytest.mark.parametrize("impl", IMPLS)
def test_filter_false_positive_rate(impl):
    keys, versions, deleted = entries(20000)
    data = impl.encode_file(keys, versions, deleted)
    tail = _python.parse_tail(data[_python.parse_footer(data[-48:])["filters_offset"] :], len(data))
    absent = [b"absent-%d" % i for i in range(20000)]
    nbits, k, bits = tail["key_filter"]
    fp = sum(impl.bloom_check_keys(bits, nbits, k, absent)) / len(absent)
    assert fp < 0.005  # 14 bits per item, k=10: ~0.1% expected
    nbits, k, bits = tail["pair_filter"]
    changed = [bytes(16)] * len(keys)  # every key present, but at a different version
    fp = sum(impl.bloom_check_pairs(bits, nbits, k, keys, changed)) / len(keys)
    assert fp < 0.005


@pytest.mark.parametrize("impl", IMPLS)
def test_deleted_entries_are_in_the_key_filter_only(impl):
    keys, versions = [b"a", b"b"], [b"1", b"2"]
    data = impl.encode_file(keys, versions, b"\x00\x01")
    tail = _python.parse_tail(data[_python.parse_footer(data[-48:])["filters_offset"] :], len(data))
    nbits, k, bits = tail["key_filter"]
    assert impl.bloom_check_keys(bits, nbits, k, [b"a", b"b"]) == b"\x01\x01"
    # The pair filter holds live pairs only: "b" at "2" is a deletion marker.
    nbits, k, bits = tail["pair_filter"]
    assert impl.bloom_check_pairs(bits, nbits, k, [b"a"], [b"1"]) == b"\x01"


@pytest.mark.parametrize("writer,reader", CROSS)
def test_empty_file(writer, reader):
    data = writer.encode_file([], [], b"")
    tail, k, v, f = decode_all(reader, data)
    assert (k, v, f) == ([], [], b"")
    assert tail["entries"] == 0 and tail["blocks"] == []
    assert tail["key_filter"][0] == 512


@pytest.mark.parametrize("impl", IMPLS)
def test_rejects_unsorted_or_duplicate_keys(impl):
    with pytest.raises(ValueError):
        impl.encode_file([b"b", b"a"], [b"", b""], b"\x00\x00")
    with pytest.raises(ValueError):
        impl.encode_file([b"a", b"a"], [b"", b""], b"\x00\x00")


@pytest.mark.parametrize("impl", IMPLS)
def test_detects_corruption(impl):
    keys, versions, deleted = entries(500)
    data = bytearray(impl.encode_file(keys, versions, deleted, block_size=1024))
    tail = _python.parse_tail(
        bytes(data[_python.parse_footer(bytes(data[-48:]))["filters_offset"] :]), len(data)
    )
    _, off, size, _, crc = tail["blocks"][3]
    data[off + size // 2] ^= 0xFF
    with pytest.raises(_python.FormatError):
        _python.check_block(bytes(data[off : off + size]), crc)
    data[-60] ^= 0xFF  # inside the index
    with pytest.raises(_python.FormatError):
        _python.parse_tail(bytes(data[tail["filters_offset"] :]), len(data))
    with pytest.raises(_python.FormatError):
        _python.parse_footer(b"XXXX" + bytes(data[-44:]))


@pytest.mark.parametrize("impl", IMPLS)
def test_sort_entries(impl):
    keys, versions, deleted = entries(1000, deleted_every=3)
    order = list(range(len(keys)))
    random.Random(1).shuffle(order)
    got = impl.sort_entries(
        [keys[i] for i in order], [versions[i] for i in order], bytes(deleted[i] for i in order)
    )
    assert (got[0], got[1], bytes(got[2])) == (keys, versions, deleted)
    with pytest.raises(ValueError):
        impl.sort_entries([b"x", b"x"], [b"", b""], b"\x00\x00")


@pytest.mark.parametrize("writer,reader", CROSS)
def test_merge_newest_wins(writer, reader):
    old = writer.encode_file([b"a", b"b", b"c", b"d"], [b"1", b"1", b"1", b"1"], b"\x00\x00\x00\x00")
    new = writer.encode_file([b"b", b"c", b"e"], [b"2", b"2", b"2"], b"\x00\x01\x00")  # c deleted
    [merged] = reader.merge_files([new, old], drop_deleted=False)
    _, k, v, f = decode_all(reader, merged)
    assert list(zip(k, v, f, strict=True)) == [
        (b"a", b"1", 0),
        (b"b", b"2", 0),
        (b"c", b"2", 1),
        (b"d", b"1", 0),
        (b"e", b"2", 0),
    ]
    [bottom] = reader.merge_files([new, old], drop_deleted=True)
    _, k, _, _ = decode_all(reader, bottom)
    assert k == [b"a", b"b", b"d", b"e"]


@pytest.mark.parametrize("impl", IMPLS)
def test_merge_splits_large_output(impl):
    keys, versions, deleted = entries(20000)
    a = impl.encode_file(keys[::2], versions[::2], deleted[::2])
    b = impl.encode_file(keys[1::2], versions[1::2], deleted[1::2])
    out = impl.merge_files([a, b], drop_deleted=True, max_file_bytes=100_000)
    assert len(out) > 2
    seen = []
    for f in out:
        _, k, _, _ = decode_all(impl, f)
        seen += k
    assert seen == keys  # split files are consecutive, non-overlapping key ranges


def blocks_of(data):
    tail = _python.parse_tail(data[_python.parse_footer(data[-48:])["filters_offset"] :], len(data))
    return tail, [data[off : off + size] for _, off, size, _, _ in tail["blocks"]]


@pytest.mark.parametrize("writer,reader", CROSS)
def test_lookup(writer, reader):
    keys, versions, deleted = entries(3000, deleted_every=11)
    tail, blocks = blocks_of(writer.encode_file(keys, versions, deleted, block_size=1024))
    probe = sorted([keys[5], keys[1500], keys[2999], b"zzz-absent", keys[11], b"aaa-absent"])
    found, vers, dels = reader.lookup(blocks, tail["codec"], probe)
    for key, f, v, d in zip(probe, found, vers, dels, strict=True):
        if key.endswith(b"-absent"):
            assert (f, v, d) == (0, b"", 0)
        else:
            i = keys.index(key)
            assert (f, v, d) == (1, versions[i], deleted[i])


@pytest.mark.parametrize("writer,reader", CROSS)
def test_merge_range(writer, reader):
    old = writer.encode_file([b"a", b"b", b"c", b"d", b"f"], [b"1"] * 5, b"\x00" * 5, block_size=8)
    new = writer.encode_file([b"b", b"c", b"e"], [b"2"] * 3, b"\x00\x01\x00", block_size=8)
    runs = [blocks_of(new)[1], blocks_of(old)[1]]
    k, v, f = reader.merge_range(runs, 1, None, None, False)
    assert list(zip(k, v, f, strict=True)) == [
        (b"a", b"1", 0),
        (b"b", b"2", 0),
        (b"c", b"2", 1),
        (b"d", b"1", 0),
        (b"e", b"2", 0),
        (b"f", b"1", 0),
    ]
    k, _, _ = reader.merge_range(runs, 1, b"b", b"e", True)  # (after, upto], tombstones dropped
    assert k == [b"d", b"e"]


@pytest.mark.parametrize("writer,reader", CROSS)
def test_replace_diff(writer, reader):
    old = writer.encode_file([b"a", b"b", b"c", b"d"], [b"1", b"1", b"1", b"1"], b"\x00\x00\x00\x00")
    new = writer.encode_file([b"d"], [b"2"], b"\x01")  # d was deleted since
    runs = [blocks_of(new)[1], blocks_of(old)[1]]
    written = [b"a", b"c", b"d", b"e"]
    changed, existed, removed, live = reader.replace_diff(runs, 1, written, [b"1", b"9", b"1", b"1"])
    assert changed == b"\x00\x01\x01\x01"  # a same; c new version; d re-added; e new
    assert existed == b"\x01\x01\x00\x00"
    assert removed == [b"b"]
    assert live == 3


@pytest.mark.parametrize("impl", IMPLS)
def test_detects_filter_corruption_and_reads_the_index_alone(impl):
    keys, versions, deleted = entries(800)
    data = bytearray(impl.encode_file(keys, versions, deleted, block_size=1024))
    footer = _python.parse_footer(bytes(data[-48:]))
    # The index part alone parses (and is checksummed) without the filters.
    idx = _python.parse_index(bytes(data[footer["index_offset"] :]), len(data))
    assert idx["min_key"] == keys[0] and "key_filter" not in idx
    data[footer["filters_offset"] + 10] ^= 0xFF
    with pytest.raises(_python.FormatError):
        _python.parse_tail(bytes(data[footer["filters_offset"] :]), len(data))
