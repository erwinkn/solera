# Key index file format (layers)

Byte-level format of the key index's files (`key-index-design.md`). The
`solera._native` Rust extension reads and writes them (`native/src/layers.rs`);
Python chooses files, fetches bytes and keeps the indexes
(`solera/keys/layers.py`).

All integers are little-endian. `varint` is unsigned LEB128.

## Files

```
file := block*
```

A file is blocks back to back: no index, filter or footer. Whoever writes
a file hands back its block boundaries, and the index keeps them (below).
Every object name carries its extension: `.lay` for a file, `.lix` for an
index.

## Blocks

```
block := clen:u32  crc:u32  rlen:u32  format:u8  data[clen]
```

`data` is `rlen` raw bytes compressed with zstd (level 1); `crc` is the
CRC-32 of `data` (the compressed bytes), checked before decompressing. A
reader refuses a block whose CRC, raw length or format does not hold, and a
raw length over 64 MiB. Writers close a block once its raw size reaches the
target (16 KiB by default) and after the last entry.

Raw bytes of a block, by `format`:

```
delta (0) := n:varint  entry*n
layer (1) := n:varint  commit_min:varint  generation_min:varint  entry*n

key   := shared:varint  suffix_len:varint  suffix[suffix_len]
           (the first `shared` bytes of the previous key in the block, then the suffix;
            the first key of a block shares nothing)
```

Entries are strictly increasing by key (byte-wise) within a file, and a
file's keys all come after the previous file's.

### A delta entry

```
entry := key  flags:u8  [payload]  [replaced]
flags := kind (bits 0–1: 0 added, 1 updated, 2 removed)
       | 4: a payload follows (len:varint, bytes)
       | 8: a replaced generation follows (varint: back from the commit's generation)
```

A commit's **delta** holds each key it changed once, with its change kind
— relative to the key's presence at the head before the commit — and a
source key's payload (its own version, `versions.md` §2). The commit's
number and generation are not in the file: they are the commit record's,
and every entry takes them.

The **replaced generation** is the generation the change replaced (an
updated or removed key's; an add replaces nothing): what an immutable
store's cleanup deletes (`lifecycle.md` §9.8). It is written as its distance
back from the commit's generation (a few bytes, often one), so reading it
needs the commit's generation. Writers record it for immutable stores'
outputs only, never for fenced stores or sources. It serves cleanup only:
**the index never reads it**, and merges drop it.

### A layer entry

```
entry := key  flags:u8  commit:varint  generation:varint  [flips]  [payload]
flags := 1: present at the layer's end
       | 2: a payload follows (len:varint, bytes)
       | 4: flips follow
       | 8: present at the layer's start (before its first commit)
commit, generation := offsets from the block's commit_min and generation_min
flips := m:varint  gap:varint*m
```

A layer covers commits `[a, b]`; per key changed in them it holds the
key's state after `b` (present, and the payload where present), the commit
and generation of its last change, its presence just before `a`, and its
**flips**: the commits in `[a, b]` that added or removed it, newest first,
each `gap` going back from the previous one (the first from `commit`).
Flips at or below the index's cut are dropped when a merge writes the
layer. A delta read as a layer: present unless removed; present at the
start unless added; commit and generation the commit record's; one flip at
the commit for an add or a remove.

## Indexes

A part's files with more than 256 KiB of blocks have an index object
beside them, written with them:

```
index := n:varint  block*n
block := file:varint  offset:varint  length:varint  first_key  entries:varint  newest:varint
first_key := key   (prefix-shared with the previous block's first key)
```

The index object is stored raw (not in blocks): it is small, and read whole.

`file` numbers the part's files from 0; `offset` and `length` place the
block (header included) in that file; `newest` is the newest commit any of
its entries changed at (0 in a delta's blocks, whose entries take their
commit from the record). A reader finds the blocks a key range or a key
list touches by bisecting first keys, skips blocks whose newest commit is
at or before its P, and may skip blocks a glob cannot match between their
first key and the next block's. Smaller parts have no index: they are read
whole.

## Names

- a commit's delta: `{commit:012d}-{attempt}-{n}.lay` and
  `{commit:012d}-{attempt}.lix`, under the index's prefix, written by the
  attempt;
- a merge's output: `{life}/l{a:012d}-{b:012d}-e{epoch}-{ulid}-{part}{n}.lay`
  and `…-{part}.lix` (`m` main, `s` side), written by the engine: the life
  and epoch let an orphan collector judge only its own and older engines'
  outputs (`key-index-design.md` § Lifecycles).
