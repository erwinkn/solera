"""Pure-Python implementation of the `.kx` key index format (docs/key-index-format.md).

The reference for `solera._native`: its kernels have the same signatures,
its jobs must produce the same content (`merge_files` is a compaction), and
each must decode the other's files to identical content. Tests only.
"""

from __future__ import annotations

import heapq
import os
import struct
import zlib

import xxhash

MAGIC = b"CKX1"
FORMAT_VERSION = 3
DELETED, PREDECESSOR, PAYLOAD = 1, 2, 4  # entry flags
CODEC_NONE, CODEC_ZLIB = 0, 1
FOOTER = struct.Struct("<4sHBBQQIQII4s")
FOOTER_SIZE = FOOTER.size  # 48
M64 = (1 << 64) - 1

assert FOOTER_SIZE == 48


class FormatError(ValueError):
    """A key index file is malformed or fails a checksum."""


# -- varints ------------------------------------------------------------------------


def put_varint(out: bytearray, n: int) -> None:
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)


def get_varint(buf, pos: int) -> tuple[int, int]:
    n = shift = 0
    while True:
        b = buf[pos]
        pos += 1
        if shift >= 64 or (shift == 63 and b > 0x01):  # past a u64: as native refuses it
            raise ValueError("varint too long")
        n |= (b & 0x7F) << shift
        if b < 0x80:
            return n, pos
        shift += 7


def _put_bytes(out: bytearray, b: bytes) -> None:
    put_varint(out, len(b))
    out += b


def _get_bytes(buf, pos: int) -> tuple[bytes, int]:
    n, pos = get_varint(buf, pos)
    return bytes(buf[pos : pos + n]), pos + n


# -- compression ------------------------------------------------------------------------


def _compress(data: bytes, codec: int, level: int) -> bytes:
    return zlib.compress(data, level) if codec == CODEC_ZLIB else bytes(data)


def _decompress(data, codec: int) -> bytes:
    if codec == CODEC_ZLIB:
        return zlib.decompress(data)
    if codec == CODEC_NONE:
        return bytes(data)
    raise FormatError(f"unknown codec {codec}")


# -- Bloom filters ------------------------------------------------------------------------


def filter_nbits(items: int, bits_per_item: int) -> int:
    """Filter size in bits: whole 512-bit blocks, at least one."""

    return max(1, -(-items * bits_per_item // 512)) * 512


def _positions(item: bytes, nbits: int, k: int):
    """Blocked Bloom filter: all k bits of an item fall in one 64-byte block."""

    h = xxhash.xxh3_128_intdigest(item)
    h1, h2 = h & M64, h >> 64
    base = ((h1 * (nbits >> 9)) >> 64) << 9
    a, b = h2 & 0xFFFFFFFF, (h2 >> 32) | 1
    return [base + ((a + i * b) & 511) for i in range(k)]


def _key_item(key: bytes) -> bytes:
    return b"k" + key


def _tomb_item(key: bytes) -> bytes:
    return b"t" + key


def _set(bits: bytearray, item: bytes, nbits: int, k: int) -> None:
    for b in _positions(item, nbits, k):
        bits[b >> 3] |= 1 << (b & 7)


def _test(bits, item: bytes, nbits: int, k: int) -> bool:
    for b in _positions(item, nbits, k):
        if not bits[b >> 3] & (1 << (b & 7)):
            return False
    return True


def bloom_check_keys(bits, nbits: int, k: int, keys: list[bytes]) -> bytes:
    """One byte per key: 1 if the key may be present, 0 if it is definitely absent."""

    return bytes(1 if _test(bits, _key_item(key), nbits, k) else 0 for key in keys)


def bloom_check_tombstones(bits, nbits: int, k: int, keys: list[bytes]) -> bytes:
    """One byte per key: 1 if the key may be deleted in this file, 0 if it definitely is not."""

    return bytes(1 if _test(bits, _tomb_item(key), nbits, k) else 0 for key in keys)


# -- encoding ------------------------------------------------------------------------


def encode_file(
    keys: list[bytes],
    generations: list[int],
    deleted: bytes,
    *,
    payloads: list | None = None,
    predecessors: list | None = None,
    block_size: int = 64 * 1024,
    level: int = 1,
    bits_per_item: int = 14,
    k: int = 10,
    codec: int = CODEC_ZLIB,
) -> bytes:
    """Encode entries (strictly increasing by key) into one `.kx` file. Each
    has a generation, may carry a payload (None for none), and a delta
    entry may have the generation its key had before."""

    n = len(keys)
    if len(generations) != n or len(deleted) != n:
        raise ValueError("keys, generations and deleted must have the same length")
    payloads = payloads if payloads is not None else [None] * n
    predecessors = predecessors if predecessors is not None else [None] * n
    out = bytearray()
    index = []  # (first_key, offset, size, entries, crc)
    block = bytearray()
    block_first = block_prev = None  # prefix compression restarts in every block
    prev = None  # ordering is checked across blocks
    count = live = 0

    def close_block():
        data = _compress(bytes(block), codec, level)
        index.append((block_first, len(out), len(data), count, zlib.crc32(data)))
        out.extend(data)

    for i in range(n):
        key = keys[i]
        if prev is not None and key <= prev:
            raise ValueError(f"keys must be strictly increasing: {prev!r} then {key!r}")
        if block_prev is None:
            block_first, shared = key, 0
        else:
            shared = len(os.path.commonprefix([block_prev, key]))
        put_varint(block, shared)
        _put_bytes(block, key[shared:])
        flag = DELETED if deleted[i] else 0
        payload, before = payloads[i], predecessors[i]
        block.append(
            flag | (PREDECESSOR if before is not None else 0) | (PAYLOAD if payload is not None else 0)
        )
        put_varint(block, generations[i])
        if payload is not None:
            _put_bytes(block, payload)
        if before is not None:
            put_varint(block, before)
        live += 1 - flag
        count += 1
        prev = block_prev = key
        if len(block) >= block_size:
            close_block()
            block.clear()
            block_first = block_prev = None
            count = 0
    if count:
        close_block()

    # Filters: every key, and every deleted key.
    key_nbits = filter_nbits(n, bits_per_item)
    tomb_nbits = filter_nbits(n - live, bits_per_item)
    key_bits = bytearray(key_nbits // 8)
    tomb_bits = bytearray(tomb_nbits // 8)
    for i in range(n):
        _set(key_bits, _key_item(keys[i]), key_nbits, k)
        if deleted[i]:
            _set(tomb_bits, _tomb_item(keys[i]), tomb_nbits, k)
    filters = bytearray()
    for nbits, bits in ((key_nbits, key_bits), (tomb_nbits, tomb_bits)):
        put_varint(filters, nbits)
        filters.append(k)
        filters += bits
    filters += struct.pack("<I", zlib.crc32(bytes(filters)))

    idx = bytearray()
    _put_bytes(idx, keys[0] if n else b"")
    _put_bytes(idx, keys[-1] if n else b"")
    put_varint(idx, len(index))
    for fk, off, size, cnt, crc in index:
        _put_bytes(idx, fk)
        put_varint(idx, off)
        put_varint(idx, size)
        put_varint(idx, cnt)
        idx += struct.pack("<I", crc)
    idx_data = _compress(bytes(idx), codec, level)

    filters_offset = len(out)
    out += filters
    index_offset = len(out)
    out += idx_data
    index_crc = zlib.crc32(idx_data)
    out += FOOTER.pack(
        MAGIC,
        FORMAT_VERSION,
        codec,
        0,
        n,
        filters_offset,
        len(filters),
        index_offset,
        len(idx_data),
        index_crc,
        MAGIC,
    )
    return bytes(out)


# -- decoding ------------------------------------------------------------------------


def decode_block(data, codec: int):
    """A block's keys, generations, deleted flags, payloads (None for none),
    and each entry's predecessor generation or None."""

    raw = _decompress(data, codec)
    keys, generations, flags, payloads, predecessors = [], [], bytearray(), [], []
    pos, prev, n = 0, b"", len(raw)
    while pos < n:
        shared, pos = get_varint(raw, pos)
        suffix, pos = _get_bytes(raw, pos)
        flag = raw[pos]
        pos += 1
        if flag & ~(DELETED | PREDECESSOR | PAYLOAD):
            raise FormatError("unknown entry flags")
        generation, pos = get_varint(raw, pos)
        payload = before = None
        if flag & PAYLOAD:
            payload, pos = _get_bytes(raw, pos)
        if flag & PREDECESSOR:
            before, pos = get_varint(raw, pos)
        key = prev[:shared] + suffix
        keys.append(key)
        generations.append(generation)
        flags.append(flag & DELETED)
        payloads.append(payload)
        predecessors.append(before)
        prev = key
    return keys, generations, bytes(flags), payloads, predecessors


def parse_footer(footer) -> dict:
    if len(footer) != FOOTER_SIZE:
        raise FormatError("footer must be 48 bytes")
    magic, version, codec, _, entries, f_off, f_len, i_off, i_len, crc, magic2 = FOOTER.unpack(bytes(footer))
    if magic != MAGIC or magic2 != MAGIC:
        raise FormatError("not a key index file")
    if version != FORMAT_VERSION:
        raise FormatError(f"unsupported format version {version}")
    return {
        "codec": codec,
        "entries": entries,
        "filters_offset": f_off,
        "filters_length": f_len,
        "index_offset": i_off,
        "index_length": i_len,
        "index_crc": crc,
    }


def parse_index(part, file_size: int) -> dict:
    """Parse a file's block index from its last bytes (the index and footer are
    enough; `part` must end at `file_size`). Filters are not read."""

    part = memoryview(part)
    footer = parse_footer(part[-FOOTER_SIZE:])
    start = file_size - len(part)
    if footer["index_offset"] < start:
        raise FormatError("index part too short")
    irel = footer["index_offset"] - start
    raw = part[irel : irel + footer["index_length"]]
    if zlib.crc32(raw) != footer["index_crc"]:
        raise FormatError("index checksum mismatch")
    idx = _decompress(raw, footer["codec"])
    pos = 0
    min_key, pos = _get_bytes(idx, pos)
    max_key, pos = _get_bytes(idx, pos)
    nblocks, pos = get_varint(idx, pos)
    blocks = []
    for _ in range(nblocks):
        first_key, pos = _get_bytes(idx, pos)
        off, pos = get_varint(idx, pos)
        size, pos = get_varint(idx, pos)
        cnt, pos = get_varint(idx, pos)
        (crc,) = struct.unpack_from("<I", idx, pos)
        pos += 4
        blocks.append((first_key, off, size, cnt, crc))
    return {**footer, "size": file_size, "min_key": min_key, "max_key": max_key, "blocks": blocks}


def parse_tail(tail, file_size: int) -> dict:
    """Parse a file's tail (filters + index + footer). `tail` must end at `file_size`."""

    tail = memoryview(tail)
    out = parse_index(tail, file_size)
    start = file_size - len(tail)
    if out["filters_offset"] < start:
        raise FormatError("tail too short")
    rel = out["filters_offset"] - start
    filters = tail[rel : rel + out["filters_length"]]
    if len(filters) < 4 or zlib.crc32(filters[:-4]) != struct.unpack("<I", filters[-4:])[0]:
        raise FormatError("filters checksum mismatch")
    pos = 0
    parsed = []
    for _ in range(2):
        nbits, pos = get_varint(filters, pos)
        k = filters[pos]
        pos += 1
        nbytes = nbits // 8
        parsed.append((nbits, k, bytes(filters[pos : pos + nbytes])))
        pos += nbytes
    if pos + 4 != len(filters):
        raise FormatError("bytes past the filters")
    return {**out, "key_filter": parsed[0], "tomb_filter": parsed[1]}


def check_block(data, crc: int) -> None:
    if zlib.crc32(data) != crc:
        raise FormatError("block checksum mismatch")


# -- merging ------------------------------------------------------------------------


def iter_file(data) -> iter:
    """Every entry of a whole file, in key order: (key, generation, deleted, payload, predecessor)."""

    data = memoryview(data)
    footer = parse_footer(data[-FOOTER_SIZE:])
    tail = parse_tail(data[footer["filters_offset"] :], len(data))
    for _, off, size, _, crc in tail["blocks"]:
        blk = data[off : off + size]
        check_block(blk, crc)
        yield from zip(*decode_block(blk, tail["codec"]), strict=True)


def merge_files(
    files: list,
    *,
    drop_deleted: bool,
    block_size: int = 64 * 1024,
    level: int = 1,
    bits_per_item: int = 14,
    k: int = 10,
    max_file_bytes: int = 64 * 2**20,
) -> list[bytes]:
    """Merge whole files, newest first: for each key the newest entry wins,
    with its generation and payload; predecessors are dropped.

    `drop_deleted` removes deleted entries from the output (merging into the
    bottom level). Output is split into files of about `max_file_bytes`."""

    heap = []
    iters = [iter_file(f) for f in files]
    for rank, it in enumerate(iters):
        for key, gen, flag, payload, _ in it:
            heap.append((key, rank, gen, flag, payload, it))
            break
    heapq.heapify(heap)
    out: list[bytes] = []
    keys, generations, flags, payloads = [], [], bytearray(), []
    approx = 0
    raw_budget = 2 * max_file_bytes  # blocks compress about 2x

    def flush():
        nonlocal keys, generations, flags, payloads, approx
        if keys:
            out.append(
                encode_file(
                    keys,
                    generations,
                    bytes(flags),
                    payloads=payloads,
                    block_size=block_size,
                    level=level,
                    bits_per_item=bits_per_item,
                    k=k,
                )
            )
        keys, generations, flags, payloads, approx = [], [], bytearray(), [], 0

    last = None
    while heap:
        key, rank, gen, flag, payload, it = heapq.heappop(heap)
        nxt = next(it, None)
        if nxt is not None:
            heapq.heappush(heap, (nxt[0], rank, nxt[1], nxt[2], nxt[3], it))
        if key == last:
            continue  # an older entry for a key already taken from a newer file
        last = key
        if flag and drop_deleted:
            continue
        keys.append(key)
        generations.append(gen)
        flags.append(flag)
        payloads.append(payload)
        approx += len(key) + len(payload or b"") + 8
        if approx >= raw_budget:
            flush()
    flush()
    return out


# -- read kernels ------------------------------------------------------------------------
# The index layer (solera.keys.index) fetches block bytes and hands them here, so
# entries only cross into Python when they are part of the answer.


def lookup(blocks: list, codec: int, keys: list[bytes]):
    """Find sorted `keys` in one file's `blocks` (consecutive blocks, in key order).

    Returns, per key: found (0/1), generation (0 when not found), deleted
    (0/1), payload (None when not found or none)."""

    table = {}
    for blk in blocks:
        ks, gs, fs, ps, _ = decode_block(blk, codec)
        table.update(zip(ks, zip(gs, fs, ps, strict=True), strict=True))
    found, generations, deleted, payloads = bytearray(), [], bytearray(), []
    for key in keys:
        hit = table.get(key)
        found.append(1 if hit else 0)
        generations.append(hit[0] if hit else 0)
        deleted.append(hit[1] if hit else 0)
        payloads.append(hit[2] if hit else None)
    return bytes(found), generations, bytes(deleted), payloads


def _run_entries(run: list, codec: int, after, upto):
    for blk in run:
        ks, gs, fs, ps, _ = decode_block(blk, codec)
        for k, g, f, p in zip(ks, gs, fs, ps, strict=True):
            if after is not None and k <= after:
                continue
            if upto is not None and k > upto:
                return
            yield k, g, f, p


def merge_range(runs: list, codecs: list[int], after, upto, drop_deleted: bool):
    """The newest-wins merged view of `runs` (newest first; each a list of one
    file's consecutive blocks, in that file's codec) over keys in `(after,
    upto]`; `None` bounds are open. Returns keys, generations, deleted flags
    and payloads."""

    if len(codecs) != len(runs):
        raise ValueError("a codec per run")
    heap = []
    iters = [_run_entries(run, codec, after, upto) for run, codec in zip(runs, codecs, strict=True)]
    for rank, it in enumerate(iters):
        for k, g, f, p in it:
            heap.append((k, rank, g, f, p, it))
            break
    heapq.heapify(heap)
    keys, generations, flags, payloads = [], [], bytearray(), []
    last = None
    while heap:
        k, rank, g, f, p, it = heapq.heappop(heap)
        nxt = next(it, None)
        if nxt is not None:
            heapq.heappush(heap, (nxt[0], rank, nxt[1], nxt[2], nxt[3], it))
        if k == last:
            continue
        last = k
        if f and drop_deleted:
            continue
        keys.append(k)
        generations.append(g)
        flags.append(f)
        payloads.append(p)
    return keys, generations, bytes(flags), payloads


# -- garbage files ------------------------------------------------------------------------

GARBAGE_MAGIC = b"CKG1"
GARBAGE_VERSION = 2
GARBAGE_FOOTER = struct.Struct("<4sHBBQI4s")  # 24 bytes
GARBAGE_BLOCK = 64 * 1024


def encode_garbage(keys: list[bytes], generations: list[int], *, codec=CODEC_ZLIB, level=1):
    """One garbage file (docs/key-index-format.md § Garbage files) holding the
    entries in order; a key may repeat."""

    out, block, blocks = bytearray(), bytearray(), 0

    def close():
        nonlocal block, blocks
        if block:
            data = _compress(bytes(block), codec, level)
            out.extend(struct.pack("<II", len(data), zlib.crc32(data)))
            out.extend(data)
            blocks += 1
            block = bytearray()

    for key, generation in zip(keys, generations, strict=True):
        _put_bytes(block, key)
        put_varint(block, generation)
        if len(block) >= GARBAGE_BLOCK:
            close()
    close()
    out.extend(
        GARBAGE_FOOTER.pack(GARBAGE_MAGIC, GARBAGE_VERSION, codec, 0, len(keys), blocks, GARBAGE_MAGIC)
    )
    return bytes(out)


def decode_garbage(data):
    """Every entry of a garbage file: keys, generations."""

    data = memoryview(data)
    if len(data) < GARBAGE_FOOTER.size:
        raise FormatError("garbage file too short")
    m1, version, codec, _, entries, blocks, m2 = GARBAGE_FOOTER.unpack(data[-GARBAGE_FOOTER.size :])
    if m1 != GARBAGE_MAGIC or m2 != GARBAGE_MAGIC:
        raise FormatError("bad garbage file magic")
    if version != GARBAGE_VERSION:
        raise FormatError(f"unsupported garbage file version {version}")
    body = data[: -GARBAGE_FOOTER.size]
    keys, generations, pos, seen = [], [], 0, 0
    while pos < len(body):
        n, crc = struct.unpack_from("<II", body, pos)
        blk = body[pos + 8 : pos + 8 + n]
        if len(blk) != n or zlib.crc32(blk) != crc:
            raise FormatError("garbage block checksum mismatch")
        pos += 8 + n
        seen += 1
        raw, p = _decompress(blk, codec), 0
        while p < len(raw):
            key, p = _get_bytes(raw, p)
            gen, p = get_varint(raw, p)
            keys.append(key)
            generations.append(gen)
    if seen != blocks or len(keys) != entries:
        raise FormatError("garbage file counts do not match its footer")
    return keys, generations
