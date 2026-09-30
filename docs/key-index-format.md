# Key index file format (`.kx`, version 1)

Byte-level format of a key index file (`object-store-state.md` §6). The
`solera._native` Rust extension reads and writes it; `solera/keys/_python.py`
is an executable reference the tests hold the extension to, and each must
read the other's files. Compressed bytes may differ between them (different
deflate implementations); decoded content may not.

All integers are little-endian. `varint` is unsigned LEB128.

## Layout

```
file   := block* filters index footer
tail   := filters index footer          ← everything after the last block
```

A file holds **entries** `(key, version, deleted)`: `key` and `version`
are byte strings, `deleted` a flag. Entries are strictly increasing by
`key` (byte-wise comparison); a file never holds the same key twice.

## Blocks

A block is a run of consecutive entries, encoded and then compressed as a
unit. Writers close a block once its encoded size reaches the target
(default 64 KiB) — so blocks may slightly exceed it — and always after the
last entry.

Encoded entry:

```
shared   varint     bytes shared with the previous key in this block (0 for the first)
suffix   varint len + bytes
version  varint len + bytes
flags    u8         bit 0: deleted; other bits 0
```

The block's bytes are the concatenated encoded entries, compressed with
the file's codec (footer).

## Filters

```
filters := filter(keys) filter(pairs) filter(tombstones) crc32
filter  := nbits varint, k u8, bits (ceil(nbits / 8) bytes)
crc32   := u32, CRC-32 of the three filters' bytes
```

Three Bloom filters: one over every key in the file, one over every
`(key, version)` pair of a non-deleted entry, and one over every deleted
key. Each is sized to its own item count.

- Items: a key is `b"k" + key`; a pair is `b"p" + varint(len(key)) + key +
  version`; a deleted key is `b"t" + key`.
- Filters are **blocked**: `nbits` is a whole number of 512-bit (64-byte)
  blocks, and all `k` bits of an item fall in one block — one cache line
  per item.
- Hash: `h = XXH3-128(item)` with seed 0, as a 128-bit integer;
  `h1 = h mod 2^64` (the low half), `h2 = h >> 64` (the high half).
- Block: `(h1 · (nbits / 512)) >> 64` (a 128-bit product).
- Bits: with `a = h2 mod 2^32` and `b = (h2 >> 32) | 1`, bit `i` of `k` is
  `block · 512 + (a + i·b) mod 512`, for `i` in `0..k`; bit `p` lives in
  byte `p // 8`, at position `p % 8` (least significant first).
- Defaults: 14 bits per item, `k = 10`, `nbits = 512 · max(1, ceil(items ·
  14 / 512))`.

## Index

The index is compressed with the file's codec. Decompressed:

```
min_key   varint len + bytes
max_key   varint len + bytes
blocks    varint
per block:
  first_key  varint len + bytes
  offset     varint     byte offset of the block in the file
  size       varint     compressed size
  entries    varint
  crc32      u32        CRC-32 (zlib polynomial) of the compressed block
```

## Footer

Fixed 48 bytes at the very end of the file:

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | magic `CKX1` |
| 4 | 2 | format version, `1` |
| 6 | 1 | codec: `0` none, `1` zlib |
| 7 | 1 | reserved, `0` |
| 8 | 8 | entries |
| 16 | 8 | filters offset |
| 24 | 4 | filters length |
| 28 | 8 | index offset |
| 36 | 4 | index length |
| 40 | 4 | CRC-32 of the (compressed) index bytes |
| 44 | 4 | magic `CKX1` |

`tail length = file size − filters offset`; `index part = file size −
index offset`. A reader that needs only the block index (a scan) fetches
the index part alone; one that needs the filters fetches the whole tail.
Readers verify both magics, the version, the index CRC, the filters CRC
when they read the filters, and each block's CRC before decoding it.

## Empty files

A file with no entries has no blocks; each of its filters has `nbits = 512`, and
its index has empty `min_key` and `max_key` and `blocks = 0`. Writers only
produce one for an empty delta.
