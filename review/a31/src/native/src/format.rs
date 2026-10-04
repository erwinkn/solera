//! The `.kx` key index format (docs/key-index-format.md), without any Python.
//!
//! Mirrors `solera/keys/_python.py` function for function; the two must decode
//! each other's files to identical content.

use std::io::{Read, Write};
use std::sync::Arc;

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;

use crate::stream::{Block, Bytes, Merge, Next, Segment, Writer};

pub const MAGIC: &[u8; 4] = b"CKX1";
pub const FORMAT_VERSION: u16 = 4;
/// The most bytes a block may decode to: writers close a block near
/// `block_size` (64 KiB by default), so this bounds every reader, including
/// those handed a bare block, and a block past it fails fast.
pub const MAX_BLOCK_BYTES: u64 = 16 << 20;
pub const CODEC_NONE: u8 = 0;
pub const CODEC_ZLIB: u8 = 1;
/// zstd at the writer's `level` (W53 prototype: smaller and faster to decode than zlib).
pub const CODEC_ZSTD: u8 = 2;
pub const FOOTER_SIZE: usize = 48;

#[derive(Debug)]
pub enum Error {
    /// Malformed input or a failed checksum.
    Format(String),
    /// Caller error: unsorted or duplicate keys, mismatched lengths.
    Value(String),
    /// Raised by a caller's callback (reading Python values), passed through.
    Callback(Box<dyn std::error::Error + Send + Sync>),
    /// Well-formed input over a caller's limit: more entries or bytes than it takes.
    Limit(String),
    /// An engine cache's local copy failed a check: `(its source's path, what)`.
    Local(String, String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn fmt_err<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Format(msg.into()))
}

// -- varints ----------------------------------------------------------------------

pub fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8 & 0x7F) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

pub fn get_varint(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut n: u64 = 0;
    let mut shift = 0u32;
    loop {
        let Some(&b) = buf.get(*pos) else {
            return fmt_err("truncated varint");
        };
        *pos += 1;
        // A u64 takes ten bytes at most, the tenth holding its top bit alone:
        // anything past that is another value, never read as this one.
        if shift >= 64 || (shift == 63 && b > 0x01) {
            return fmt_err("varint too long");
        }
        n |= ((b & 0x7F) as u64) << shift;
        if b < 0x80 {
            return Ok(n);
        }
        shift += 7;
    }
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_varint(out, b.len() as u64);
    out.extend_from_slice(b);
}

pub(crate) fn get_bytes<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    let n = get_varint(buf, pos)? as usize;
    let end = pos.checked_add(n).filter(|&e| e <= buf.len());
    let Some(end) = end else {
        return fmt_err("truncated bytes");
    };
    let out = &buf[*pos..end];
    *pos = end;
    Ok(out)
}

// -- compression --------------------------------------------------------------------

pub(crate) fn compress(data: &[u8], codec: u8, level: u32) -> Vec<u8> {
    if codec == CODEC_ZLIB {
        let mut enc = ZlibEncoder::new(
            Vec::with_capacity(data.len() / 2 + 64),
            Compression::new(level),
        );
        enc.write_all(data).expect("writing to a Vec cannot fail");
        enc.finish().expect("writing to a Vec cannot fail")
    } else if codec == CODEC_ZSTD {
        zstd::bulk::compress(data, level as i32).expect("compressing to a Vec cannot fail")
    } else {
        data.to_vec()
    }
}

/// `data` decompressed, unless that is more than `limit` bytes: an
/// `Error::Limit` then, after reading no more than `limit + 1`.
pub(crate) fn decompress_at_most(data: &[u8], codec: u8, limit: u64) -> Result<Vec<u8>> {
    if codec == CODEC_NONE && data.len() as u64 > limit {
        return Err(Error::Limit(format!("more than {limit} bytes decoded")));
    }
    let out = match codec {
        CODEC_ZLIB => {
            let mut out = Vec::with_capacity(data.len().saturating_mul(3).min(limit as usize));
            ZlibDecoder::new(data)
                .take(limit.saturating_add(1))
                .read_to_end(&mut out)
                .map_err(|e| Error::Format(format!("bad zlib data: {e}")))?;
            out
        }
        CODEC_ZSTD => {
            let mut out = Vec::with_capacity(data.len().saturating_mul(3).min(limit as usize));
            zstd::stream::read::Decoder::new(data)
                .map_err(|e| Error::Format(format!("bad zstd data: {e}")))?
                .take(limit.saturating_add(1))
                .read_to_end(&mut out)
                .map_err(|e| Error::Format(format!("bad zstd data: {e}")))?;
            out
        }
        CODEC_NONE => data.to_vec(),
        other => return fmt_err(format!("unknown codec {other}")),
    };
    if out.len() as u64 > limit {
        return Err(Error::Limit(format!("more than {limit} bytes decoded")));
    }
    Ok(out)
}

/// `buf[at..at + len]`, or None when that is not inside `buf`.
pub(crate) fn slice_at(buf: &[u8], at: u64, len: u64) -> Option<&[u8]> {
    let start = usize::try_from(at).ok()?;
    let end = start.checked_add(usize::try_from(len).ok()?)?;
    buf.get(start..end)
}

// -- Bloom filters ------------------------------------------------------------------

/// Filter size in bits: whole 512-bit blocks, at least one.
pub fn filter_nbits(items: u64, bits_per_item: u64) -> u64 {
    std::cmp::max(1, (items * bits_per_item).div_ceil(512)) * 512
}

/// Blocked Bloom filter: all k bits of an item fall in one 64-byte block.
#[inline]
fn bit_positions(item: &[u8], nbits: u64, k: u8) -> impl Iterator<Item = u64> {
    hash_positions(xxhash_rust::xxh3::xxh3_128(item), nbits, k)
}

/// Bit positions of an item from its hash, `XXH3-128(item)`.
#[inline]
pub(crate) fn hash_positions(h: u128, nbits: u64, k: u8) -> impl Iterator<Item = u64> {
    let (h1, h2) = (h as u64, (h >> 64) as u64);
    let base = ((((h1 as u128) * ((nbits >> 9) as u128)) >> 64) as u64) << 9;
    let (a, b) = (h2 & 0xFFFF_FFFF, (h2 >> 32) | 1);
    (0..k as u64).map(move |i| base + (a.wrapping_add(i.wrapping_mul(b)) & 511))
}

pub(crate) fn key_item(buf: &mut Vec<u8>, key: &[u8]) {
    buf.clear();
    buf.push(b'k');
    buf.extend_from_slice(key);
}

/// Whether a filter may hold `item` (a `key_item`).
pub(crate) fn may_hold(bits: &[u8], item: &[u8], nbits: u64, k: u8) -> bool {
    test_bits(bits, item, nbits, k)
}

fn test_bits(bits: &[u8], item: &[u8], nbits: u64, k: u8) -> bool {
    bit_positions(item, nbits, k).all(|b| {
        bits.get((b >> 3) as usize)
            .is_some_and(|&x| x & (1 << (b & 7)) != 0)
    })
}

pub fn bloom_check_keys(bits: &[u8], nbits: u64, k: u8, keys: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    keys.iter()
        .map(|key| {
            key_item(&mut buf, key);
            test_bits(bits, &buf, nbits, k) as u8
        })
        .collect()
}

// -- sorting ----------------------------------------------------------------------

/// The permutation that sorts `keys`; errors on a duplicate key.
pub fn sort_order(keys: &[&[u8]]) -> Result<Vec<usize>> {
    let mut order: Vec<usize> = (0..keys.len()).collect();
    order.sort_unstable_by(|&a, &b| keys[a].cmp(keys[b]));
    for w in order.windows(2) {
        if keys[w[0]] == keys[w[1]] {
            return Err(Error::Value(format!(
                "duplicate key {:?}",
                String::from_utf8_lossy(keys[w[0]])
            )));
        }
    }
    Ok(order)
}

// -- encoding -----------------------------------------------------------------------

#[derive(Clone, Copy)]
pub struct Options {
    pub block_size: usize,
    pub level: u32,
    pub bits_per_item: u64,
    pub k: u8,
    pub codec: u8,
}

pub(crate) fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// Encode entries (strictly increasing by key) into one `.kx` file: per
/// entry its generation, deleted flag, payload if any, and — a delta's — the
/// generation it superseded, if known.
pub fn encode_file(
    keys: &[&[u8]],
    generations: &[u64],
    deleted: &[u8],
    payloads: &[Option<&[u8]>],
    predecessors: &[Option<u64>],
    o: Options,
) -> Result<Vec<u8>> {
    let n = keys.len();
    if generations.len() != n
        || deleted.len() != n
        || payloads.len() != n
        || predecessors.len() != n
    {
        return Err(Error::Value(
            "keys, generations, deleted, payloads and predecessors must have the same length"
                .into(),
        ));
    }
    let mut w = Writer::new(o, usize::MAX);
    for i in 0..n {
        w.push(
            keys[i],
            generations[i],
            deleted[i] != 0,
            payloads[i],
            predecessors[i].map(|g| (g, None)),
        )?;
    }
    w.finish(true)?;
    Ok(w.files.pop_front().expect("finish(true) writes a file"))
}

// -- decoding -----------------------------------------------------------------------

/// Entries' columns: keys, generations, deleted flags, payloads.
pub type Merged = (Vec<Vec<u8>>, Vec<u64>, Vec<u8>, Vec<Option<Vec<u8>>>);

/// A block's entries (`Merged`), and each one's predecessor.
pub type Decoded = (Merged, Vec<Option<u64>>);

pub fn decode_block(data: &[u8], codec: u8) -> Result<Decoded> {
    let b = Block::decode(data, codec)?;
    let n = b.len();
    Ok((
        (
            (0..n).map(|i| b.key(i).to_vec()).collect(),
            (0..n).map(|i| b.generation(i)).collect(),
            (0..n).map(|i| b.deleted(i) as u8).collect(),
            (0..n).map(|i| b.payload(i).map(<[u8]>::to_vec)).collect(),
        ),
        (0..n).map(|i| b.predecessor(i)).collect(),
    ))
}

pub struct Footer {
    pub codec: u8,
    pub entries: u64,
    pub filters_offset: u64,
    pub filters_length: u32,
    pub index_offset: u64,
    pub index_length: u32,
    pub index_crc: u32,
}

fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(b[at..at + 2].try_into().unwrap())
}
fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}
fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

pub fn parse_footer(f: &[u8]) -> Result<Footer> {
    if f.len() != FOOTER_SIZE {
        return fmt_err("footer must be 48 bytes");
    }
    if &f[0..4] != MAGIC || &f[44..48] != MAGIC {
        return fmt_err("not a key index file");
    }
    let version = u16_at(f, 4);
    if version != FORMAT_VERSION {
        return fmt_err(format!("unsupported format version {version}"));
    }
    Ok(Footer {
        codec: f[6],
        entries: u64_at(f, 8),
        filters_offset: u64_at(f, 16),
        filters_length: u32_at(f, 24),
        index_offset: u64_at(f, 28),
        index_length: u32_at(f, 36),
        index_crc: u32_at(f, 40),
    })
}

pub struct BlockMeta {
    pub offset: u64,
    pub size: u64,
    pub crc: u32,
}

pub struct Index {
    pub footer: Footer,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    /// Per block: first key, offset, compressed size, entries, CRC.
    pub blocks: Vec<(Vec<u8>, u64, u64, u64, u32)>,
}

/// A file's block index from its last bytes (`part` ends at `file_size`).
pub fn parse_index(part: &[u8], file_size: u64) -> Result<Index> {
    parse_index_at_most(part, file_size, u64::MAX)
}

/// The smallest index entry of a block: an empty first key, three one-byte
/// varints and a CRC.
const MIN_INDEX_ENTRY: usize = 1 + 3 + 4;

/// `parse_index`, decompressing at most `limit` bytes of index (`Error::Limit`
/// past it). The index's grammar must use every byte.
pub fn parse_index_at_most(part: &[u8], file_size: u64, limit: u64) -> Result<Index> {
    if part.len() < FOOTER_SIZE {
        return fmt_err("index part too short");
    }
    let footer = parse_footer(&part[part.len() - FOOTER_SIZE..])?;
    let Some(start) = file_size.checked_sub(part.len() as u64) else {
        return fmt_err("index part longer than the file");
    };
    if footer.index_offset < start {
        return fmt_err("index part too short");
    }
    let rel = footer.index_offset - start;
    let Some(raw) = slice_at(part, rel, footer.index_length as u64) else {
        return fmt_err("index out of bounds");
    };
    if crc32fast::hash(raw) != footer.index_crc {
        return fmt_err("index checksum mismatch");
    }
    let idx = decompress_at_most(raw, footer.codec, limit)?;
    let mut pos = 0;
    let min_key = get_bytes(&idx, &mut pos)?.to_vec();
    let max_key = get_bytes(&idx, &mut pos)?.to_vec();
    let nblocks = get_varint(&idx, &mut pos)?;
    if nblocks > ((idx.len() - pos) / MIN_INDEX_ENTRY) as u64 {
        return fmt_err("more blocks than the index has room for");
    }
    let mut blocks = Vec::with_capacity(nblocks as usize);
    for _ in 0..nblocks {
        let first = get_bytes(&idx, &mut pos)?.to_vec();
        let offset = get_varint(&idx, &mut pos)?;
        let size = get_varint(&idx, &mut pos)?;
        let entries = get_varint(&idx, &mut pos)?;
        let Some(crc) = slice_at(&idx, pos as u64, 4) else {
            return fmt_err("truncated index");
        };
        let crc = u32_at(crc, 0);
        pos += 4;
        blocks.push((first, offset, size, entries, crc));
    }
    if pos != idx.len() {
        return fmt_err("bytes past the end of the index");
    }
    Ok(Index {
        footer,
        min_key,
        max_key,
        blocks,
    })
}

/// A file's key filter, `(nbits, k, bits)`, from its tail (`tail` ends at
/// `file_size`).
pub fn parse_filters(tail: &[u8], file_size: u64) -> Result<(u64, u8, &[u8])> {
    if tail.len() < FOOTER_SIZE {
        return fmt_err("tail too short");
    }
    let footer = parse_footer(&tail[tail.len() - FOOTER_SIZE..])?;
    let Some(start) = file_size.checked_sub(tail.len() as u64) else {
        return fmt_err("tail longer than the file");
    };
    if footer.filters_offset < start {
        return fmt_err("tail too short");
    }
    let rel = footer.filters_offset - start;
    let Some(filters) = slice_at(tail, rel, footer.filters_length as u64) else {
        return fmt_err("filters out of bounds");
    };
    if filters.len() < 4
        || crc32fast::hash(&filters[..filters.len() - 4]) != u32_at(filters, filters.len() - 4)
    {
        return fmt_err("filters checksum mismatch");
    }
    let mut pos = 0;
    let mut one = || -> Result<(u64, u8, &[u8])> {
        let nbits = get_varint(filters, &mut pos)?;
        let Some(&k) = filters.get(pos) else {
            return fmt_err("truncated filters");
        };
        pos += 1;
        let Some(bits) = slice_at(filters, pos as u64, nbits / 8) else {
            return fmt_err("truncated filters");
        };
        pos += bits.len();
        Ok((nbits, k, bits))
    };
    let filters_out = one()?;
    if pos + 4 != filters.len() {
        return fmt_err("bytes past the filters");
    }
    Ok(filters_out)
}

/// The blocks of a whole file, after checking the footer and index checksum.
pub fn file_blocks(data: &[u8]) -> Result<(u8, Vec<BlockMeta>)> {
    let idx = parse_index(data, data.len() as u64)?;
    let blocks = idx
        .blocks
        .iter()
        .map(|&(_, offset, size, _, crc)| BlockMeta { offset, size, crc })
        .collect();
    Ok((idx.footer.codec, blocks))
}

// -- read kernels -------------------------------------------------------------------

/// Per key: found, then the entry's columns (`Merged` without keys).
pub type Found = (Vec<u8>, Vec<u64>, Vec<u8>, Vec<Option<Vec<u8>>>);

/// Find sorted `keys` in one file's consecutive `blocks`.
pub fn lookup(blocks: &[&[u8]], codec: u8, keys: &[&[u8]]) -> Result<Found> {
    let blocks: Vec<Block> = blocks
        .iter()
        .map(|b| Block::decode(b, codec))
        .collect::<Result<_>>()?;
    let mut out: Found = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for key in keys {
        // The key's first entry (its newest version, in a span's file): in the
        // last block starting below it, or the next if that starts with it.
        let b = blocks.partition_point(|b| !b.is_empty() && b.key(0) < *key);
        let hit = b
            .checked_sub(1)
            .into_iter()
            .chain((b < blocks.len()).then_some(b))
            .find_map(|b| {
                let blk = &blocks[b];
                let i = blk.lower_bound(key);
                (i < blk.len() && blk.key(i) == *key).then_some((blk, i))
            });
        match hit {
            Some((blk, j)) => {
                out.0.push(1);
                out.1.push(blk.generation(j));
                out.2.push(blk.deleted(j) as u8);
                out.3.push(blk.payload(j).map(<[u8]>::to_vec));
            }
            None => {
                out.0.push(0);
                out.1.push(0);
                out.2.push(0);
                out.3.push(None);
            }
        }
    }
    Ok(out)
}

/// The newest-wins merged view of `runs` (newest first, each a file's
/// consecutive blocks, in that file's codec) over keys in `(after, upto]`.
pub fn merge_range(
    runs: &[Vec<&[u8]>],
    codecs: &[u8],
    after: Option<&[u8]>,
    upto: Option<&[u8]>,
    drop_deleted: bool,
) -> Result<Merged> {
    if codecs.len() != runs.len() {
        return Err(Error::Value("a codec per run".into()));
    }
    let mut m = Merge::new(runs.len());
    for ((r, blocks), &codec) in runs.iter().enumerate().zip(codecs) {
        let mut data = Vec::new();
        let mut metas = Vec::new();
        for b in blocks {
            metas.push((data.len(), b.len(), crc32fast::hash(b)));
            data.extend_from_slice(b);
        }
        let data: Bytes = Arc::new(data);
        m.runs[r].feed(Segment {
            data,
            blocks: metas,
            codec,
        });
        m.runs[r].end();
    }
    let mut out: Merged = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    loop {
        match m.next_key()? {
            Next::Entry => {
                let key = m.key();
                if after.is_some_and(|a| key <= a) || (drop_deleted && m.deleted()) {
                    continue;
                }
                if upto.is_some_and(|u| key > u) {
                    break;
                }
                push_entry(&mut out, &m);
            }
            Next::Need(_) => unreachable!("every run is fed whole"),
            Next::End => break,
        }
    }
    Ok(out)
}

fn push_entry(out: &mut Merged, m: &Merge) {
    out.0.push(m.key().to_vec());
    out.1.push(m.generation());
    out.2.push(m.deleted() as u8);
    out.3.push(m.payload().map(<[u8]>::to_vec));
}

/// A page of a bounded merge: its entries, the last key it examined
/// (returned or dropped as deleted), and whether another follows it.
pub struct Page {
    pub entries: Merged,
    pub last: Option<Vec<u8>>,
    pub more: bool,
}

/// Up to `limit` entries of the newest-wins merge of `runs` (newest first,
/// each a file's consecutive blocks, in that file's codec) with keys past
/// `after` and before `bound`, deletions dropped with `drop_deleted`. Blocks
/// are decoded one at a time as the merge reaches them, and the merge stops
/// at the page: the entries past it are never built.
pub fn merge_page(
    runs: &[Vec<&[u8]>],
    codecs: &[u8],
    after: Option<&[u8]>,
    bound: Option<&[u8]>,
    limit: usize,
    drop_deleted: bool,
    below: Option<u64>,
) -> Result<Page> {
    if codecs.len() != runs.len() {
        return Err(Error::Value("a codec per run".into()));
    }
    let mut m = Merge::below(runs.len(), below);
    for ((r, blocks), &codec) in runs.iter().enumerate().zip(codecs) {
        let mut data = Vec::new();
        let mut metas = Vec::new();
        for b in blocks {
            metas.push((data.len(), b.len(), crc32fast::hash(b)));
            data.extend_from_slice(b);
        }
        let data: Bytes = Arc::new(data);
        for meta in metas {
            // A block a segment: decoded when the merge needs it, not 32 ahead.
            m.runs[r].feed(Segment {
                data: data.clone(),
                blocks: vec![meta],
                codec,
            });
        }
        m.runs[r].end();
    }
    let mut page = Page {
        entries: (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
        last: None,
        more: false,
    };
    loop {
        match m.next_key()? {
            Next::Entry => {
                let key = m.key();
                if after.is_some_and(|a| key <= a) {
                    continue;
                }
                if bound.is_some_and(|b| key >= b) {
                    return Ok(page);
                }
                if page.entries.0.len() == limit {
                    page.more = true;
                    return Ok(page);
                }
                page.last = Some(key.to_vec());
                if drop_deleted && m.deleted() {
                    continue;
                }
                push_entry(&mut page.entries, &m);
            }
            Next::Need(_) => unreachable!("every run is fed whole"),
            Next::End => return Ok(page),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_overlong_varint_is_refused_not_read_as_another_value() {
        let mut max = vec![0xFF; 9];
        max.push(0x01); // u64::MAX: the tenth byte holds the top bit alone
        assert_eq!(get_varint(&max, &mut 0).unwrap(), u64::MAX);
        let mut over = vec![0xFF; 9];
        over.push(0x7F);
        assert!(get_varint(&over, &mut 0).is_err());
        let mut eleven = vec![0xFF; 10];
        eleven.push(0x00);
        assert!(get_varint(&eleven, &mut 0).is_err());
    }

    const O: Options = Options {
        block_size: 64,
        level: 6,
        bits_per_item: 14,
        k: 10,
        codec: CODEC_ZLIB,
    };

    #[test]
    fn roundtrip() {
        let keys: Vec<Vec<u8>> = (0..500)
            .map(|i| format!("key-{i:05}").into_bytes())
            .collect();
        let pays: Vec<Vec<u8>> = (0..500).map(|i| format!("v{i}").into_bytes()).collect();
        let ks: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let ps: Vec<Option<&[u8]>> = (0..500)
            .map(|i| (i % 2 == 0).then_some(pays[i].as_slice()))
            .collect();
        let gens: Vec<u64> = (0..500).map(|i| i * 1000).collect();
        let prev: Vec<Option<u64>> = (0..500u64).map(|i| (i % 3 == 0).then_some(i)).collect();
        let f = encode_file(&ks, &gens, &[0u8; 500], &ps, &prev, O).unwrap();
        let (codec, blocks) = file_blocks(&f).unwrap();
        let mut n = 0;
        for b in blocks {
            let ((k, g, _, p), pre) =
                decode_block(&f[b.offset as usize..(b.offset + b.size) as usize], codec).unwrap();
            for i in 0..k.len() {
                assert_eq!((&k[i], g[i]), (&keys[n], gens[n]));
                assert_eq!(p[i].as_deref(), ps[n]);
                assert_eq!(pre[i], prev[n]);
                n += 1;
            }
        }
        assert_eq!(n, 500);
    }
}
