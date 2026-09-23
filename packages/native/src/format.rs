//! The `.kx` key index format (docs/key-index-format.md), without any Python.
//!
//! Mirrors `cursus/keys/_python.py` function for function; the two must decode
//! each other's files to identical content.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{Read, Write};

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;

pub const MAGIC: &[u8; 4] = b"CKX1";
pub const FORMAT_VERSION: u16 = 1;
pub const CODEC_NONE: u8 = 0;
pub const CODEC_ZLIB: u8 = 1;
pub const FOOTER_SIZE: usize = 48;

#[derive(Debug)]
pub enum Error {
    /// Malformed input or a failed checksum.
    Format(String),
    /// Caller error: unsorted or duplicate keys, mismatched lengths.
    Value(String),
}

pub type Result<T> = std::result::Result<T, Error>;

fn fmt_err<T>(msg: impl Into<String>) -> Result<T> {
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
        if shift >= 64 {
            return fmt_err("varint too long");
        }
        n |= ((b & 0x7F) as u64) << shift;
        if b < 0x80 {
            return Ok(n);
        }
        shift += 7;
    }
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_varint(out, b.len() as u64);
    out.extend_from_slice(b);
}

fn get_bytes<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
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

fn compress(data: &[u8], codec: u8, level: u32) -> Vec<u8> {
    if codec == CODEC_ZLIB {
        let mut enc = ZlibEncoder::new(
            Vec::with_capacity(data.len() / 2 + 64),
            Compression::new(level),
        );
        enc.write_all(data).expect("writing to a Vec cannot fail");
        enc.finish().expect("writing to a Vec cannot fail")
    } else {
        data.to_vec()
    }
}

fn decompress(data: &[u8], codec: u8) -> Result<Vec<u8>> {
    match codec {
        CODEC_ZLIB => {
            let mut out = Vec::with_capacity(data.len() * 3);
            ZlibDecoder::new(data)
                .read_to_end(&mut out)
                .map_err(|e| Error::Format(format!("bad zlib data: {e}")))?;
            Ok(out)
        }
        CODEC_NONE => Ok(data.to_vec()),
        other => fmt_err(format!("unknown codec {other}")),
    }
}

// -- Bloom filters ------------------------------------------------------------------

/// Filter size in bits: whole 512-bit blocks, at least one.
pub fn filter_nbits(items: u64, bits_per_item: u64) -> u64 {
    std::cmp::max(1, (items * bits_per_item).div_ceil(512)) * 512
}

/// Blocked Bloom filter: all k bits of an item fall in one 64-byte block.
#[inline]
fn bit_positions(item: &[u8], nbits: u64, k: u8) -> impl Iterator<Item = u64> {
    let h = xxhash_rust::xxh3::xxh3_128(item);
    let (h1, h2) = (h as u64, (h >> 64) as u64);
    let base = ((((h1 as u128) * ((nbits >> 9) as u128)) >> 64) as u64) << 9;
    let (a, b) = (h2 & 0xFFFF_FFFF, (h2 >> 32) | 1);
    (0..k as u64).map(move |i| base + (a.wrapping_add(i.wrapping_mul(b)) & 511))
}

fn key_item(buf: &mut Vec<u8>, key: &[u8]) {
    buf.clear();
    buf.push(b'k');
    buf.extend_from_slice(key);
}

fn tomb_item(buf: &mut Vec<u8>, key: &[u8]) {
    buf.clear();
    buf.push(b't');
    buf.extend_from_slice(key);
}

fn pair_item(buf: &mut Vec<u8>, key: &[u8], version: &[u8]) {
    buf.clear();
    buf.push(b'p');
    put_varint(buf, key.len() as u64);
    buf.extend_from_slice(key);
    buf.extend_from_slice(version);
}

fn set_bits(bits: &mut [u8], item: &[u8], nbits: u64, k: u8) {
    for b in bit_positions(item, nbits, k) {
        bits[(b >> 3) as usize] |= 1 << (b & 7);
    }
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

pub fn bloom_check_tombstones(bits: &[u8], nbits: u64, k: u8, keys: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    keys.iter()
        .map(|key| {
            tomb_item(&mut buf, key);
            test_bits(bits, &buf, nbits, k) as u8
        })
        .collect()
}

pub fn bloom_check_pairs(
    bits: &[u8],
    nbits: u64,
    k: u8,
    keys: &[&[u8]],
    versions: &[&[u8]],
) -> Vec<u8> {
    let mut buf = Vec::new();
    keys.iter()
        .zip(versions)
        .map(|(key, ver)| {
            pair_item(&mut buf, key, ver);
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

type IndexEntry<'a> = (&'a [u8], u64, u64, u64, u32);

fn close_block<'a>(
    block: &[u8],
    first: &'a [u8],
    count: u64,
    o: &Options,
    out: &mut Vec<u8>,
    index: &mut Vec<IndexEntry<'a>>,
) {
    let data = compress(block, o.codec, o.level);
    index.push((
        first,
        out.len() as u64,
        data.len() as u64,
        count,
        crc32fast::hash(&data),
    ));
    out.extend_from_slice(&data);
}

fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// Encode entries (strictly increasing by key) into one `.kx` file.
pub fn encode_file(
    keys: &[&[u8]],
    versions: &[&[u8]],
    deleted: &[u8],
    o: Options,
) -> Result<Vec<u8>> {
    let n = keys.len();
    if versions.len() != n || deleted.len() != n {
        return Err(Error::Value(
            "keys, versions and deleted must have the same length".into(),
        ));
    }
    let mut out: Vec<u8> = Vec::new();
    let mut index: Vec<IndexEntry> = Vec::new();
    let mut block: Vec<u8> = Vec::with_capacity(o.block_size + 256);
    let mut block_first: Option<&[u8]> = None;
    let mut block_prev: Option<&[u8]> = None;
    let mut prev: Option<&[u8]> = None;
    let mut count: u64 = 0;
    let mut live: u64 = 0;

    for i in 0..n {
        let key = keys[i];
        if let Some(p) = prev {
            if key <= p {
                return Err(Error::Value(format!(
                    "keys must be strictly increasing: {:?} then {:?}",
                    String::from_utf8_lossy(p),
                    String::from_utf8_lossy(key)
                )));
            }
        }
        let shared = match block_prev {
            None => {
                block_first = Some(key);
                0
            }
            Some(bp) => shared_prefix(bp, key),
        };
        put_varint(&mut block, shared as u64);
        put_bytes(&mut block, &key[shared..]);
        put_bytes(&mut block, versions[i]);
        let flag = (deleted[i] != 0) as u8;
        block.push(flag);
        live += 1 - flag as u64;
        count += 1;
        prev = Some(key);
        block_prev = Some(key);
        if block.len() >= o.block_size {
            close_block(
                &block,
                block_first.unwrap(),
                count,
                &o,
                &mut out,
                &mut index,
            );
            block.clear();
            block_first = None;
            block_prev = None;
            count = 0;
        }
    }
    if count > 0 {
        close_block(
            &block,
            block_first.unwrap(),
            count,
            &o,
            &mut out,
            &mut index,
        );
    }

    // Filters: every key, every live (key, version) pair, and every deleted key.
    let key_nbits = filter_nbits(n as u64, o.bits_per_item);
    let pair_nbits = filter_nbits(live, o.bits_per_item);
    let tomb_nbits = filter_nbits(n as u64 - live, o.bits_per_item);
    let mut key_bits = vec![0u8; (key_nbits / 8) as usize];
    let mut pair_bits = vec![0u8; (pair_nbits / 8) as usize];
    let mut tomb_bits = vec![0u8; (tomb_nbits / 8) as usize];
    let mut buf = Vec::new();
    for i in 0..n {
        key_item(&mut buf, keys[i]);
        set_bits(&mut key_bits, &buf, key_nbits, o.k);
        if deleted[i] != 0 {
            tomb_item(&mut buf, keys[i]);
            set_bits(&mut tomb_bits, &buf, tomb_nbits, o.k);
        } else {
            pair_item(&mut buf, keys[i], versions[i]);
            set_bits(&mut pair_bits, &buf, pair_nbits, o.k);
        }
    }
    let mut filters = Vec::with_capacity(key_bits.len() + pair_bits.len() + tomb_bits.len() + 48);
    for (nbits, bits) in [
        (key_nbits, &key_bits),
        (pair_nbits, &pair_bits),
        (tomb_nbits, &tomb_bits),
    ] {
        put_varint(&mut filters, nbits);
        filters.push(o.k);
        filters.extend_from_slice(bits);
    }

    let mut idx = Vec::new();
    put_bytes(&mut idx, if n > 0 { keys[0] } else { b"" });
    put_bytes(&mut idx, if n > 0 { keys[n - 1] } else { b"" });
    put_varint(&mut idx, index.len() as u64);
    for (fk, off, size, cnt, crc) in &index {
        put_bytes(&mut idx, fk);
        put_varint(&mut idx, *off);
        put_varint(&mut idx, *size);
        put_varint(&mut idx, *cnt);
        idx.extend_from_slice(&crc.to_le_bytes());
    }
    let idx_data = compress(&idx, o.codec, o.level);

    let filters_offset = out.len() as u64;
    out.extend_from_slice(&filters);
    let index_offset = out.len() as u64;
    out.extend_from_slice(&idx_data);
    let tail_crc = crc32fast::hash(&out[filters_offset as usize..]);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.push(o.codec);
    out.push(0);
    out.extend_from_slice(&(n as u64).to_le_bytes());
    out.extend_from_slice(&filters_offset.to_le_bytes());
    out.extend_from_slice(&(filters.len() as u32).to_le_bytes());
    out.extend_from_slice(&index_offset.to_le_bytes());
    out.extend_from_slice(&(idx_data.len() as u32).to_le_bytes());
    out.extend_from_slice(&tail_crc.to_le_bytes());
    out.extend_from_slice(MAGIC);
    Ok(out)
}

// -- decoding -----------------------------------------------------------------------

pub type Decoded = (Vec<Vec<u8>>, Vec<Vec<u8>>, Vec<u8>);

pub fn decode_block(data: &[u8], codec: u8) -> Result<Decoded> {
    let raw = decompress(data, codec)?;
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let mut versions = Vec::new();
    let mut flags = Vec::new();
    let mut pos = 0usize;
    let mut prev: Vec<u8> = Vec::new();
    while pos < raw.len() {
        let shared = get_varint(&raw, &mut pos)? as usize;
        let suffix = get_bytes(&raw, &mut pos)?;
        let version = get_bytes(&raw, &mut pos)?;
        let Some(&flag) = raw.get(pos) else {
            return fmt_err("truncated entry");
        };
        pos += 1;
        if shared > prev.len() {
            return fmt_err("bad shared prefix length");
        }
        let mut key = Vec::with_capacity(shared + suffix.len());
        key.extend_from_slice(&prev[..shared]);
        key.extend_from_slice(suffix);
        prev.clone_from(&key);
        keys.push(key);
        versions.push(version.to_vec());
        flags.push(flag & 1);
    }
    Ok((keys, versions, flags))
}

pub struct Footer {
    pub codec: u8,
    pub entries: u64,
    pub filters_offset: u64,
    pub filters_length: u32,
    pub index_offset: u64,
    pub index_length: u32,
    pub tail_crc: u32,
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
        tail_crc: u32_at(f, 40),
    })
}

pub struct BlockMeta {
    pub offset: u64,
    pub size: u64,
    pub crc: u32,
}

/// The blocks of a whole file, after checking the footer and tail checksum.
pub fn file_blocks(data: &[u8]) -> Result<(u8, Vec<BlockMeta>)> {
    if data.len() < FOOTER_SIZE {
        return fmt_err("file too short");
    }
    let f = parse_footer(&data[data.len() - FOOTER_SIZE..])?;
    let tail_end = data.len() - FOOTER_SIZE;
    let fo = f.filters_offset as usize;
    if fo > tail_end || crc32fast::hash(&data[fo..tail_end]) != f.tail_crc {
        return fmt_err("tail checksum mismatch");
    }
    let io = f.index_offset as usize;
    let il = f.index_length as usize;
    if io + il > tail_end {
        return fmt_err("index out of bounds");
    }
    let idx = decompress(&data[io..io + il], f.codec)?;
    let mut pos = 0;
    get_bytes(&idx, &mut pos)?; // min key
    get_bytes(&idx, &mut pos)?; // max key
    let nblocks = get_varint(&idx, &mut pos)?;
    let mut blocks = Vec::with_capacity(nblocks as usize);
    for _ in 0..nblocks {
        get_bytes(&idx, &mut pos)?; // first key
        let offset = get_varint(&idx, &mut pos)?;
        let size = get_varint(&idx, &mut pos)?;
        get_varint(&idx, &mut pos)?; // entries
        if pos + 4 > idx.len() {
            return fmt_err("truncated index");
        }
        let crc = u32_at(&idx, pos);
        pos += 4;
        blocks.push(BlockMeta { offset, size, crc });
    }
    let _ = f.entries;
    let _ = f.filters_length;
    Ok((f.codec, blocks))
}

// -- merging ------------------------------------------------------------------------

/// Iterates a whole file's entries in key order, decoding one block at a time.
struct FileIter<'a> {
    data: &'a [u8],
    codec: u8,
    blocks: Vec<BlockMeta>,
    next_block: usize,
    cur: Decoded,
    pos: usize,
}

impl<'a> FileIter<'a> {
    fn new(data: &'a [u8]) -> Result<Self> {
        let (codec, blocks) = file_blocks(data)?;
        Ok(FileIter {
            data,
            codec,
            blocks,
            next_block: 0,
            cur: (vec![], vec![], vec![]),
            pos: 0,
        })
    }

    fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>, u8)>> {
        while self.pos >= self.cur.0.len() {
            if self.next_block >= self.blocks.len() {
                return Ok(None);
            }
            let b = &self.blocks[self.next_block];
            self.next_block += 1;
            let end = (b.offset + b.size) as usize;
            if end > self.data.len() {
                return fmt_err("block out of bounds");
            }
            let raw = &self.data[b.offset as usize..end];
            if crc32fast::hash(raw) != b.crc {
                return fmt_err("block checksum mismatch");
            }
            self.cur = decode_block(raw, self.codec)?;
            self.pos = 0;
        }
        let i = self.pos;
        self.pos += 1;
        Ok(Some((
            std::mem::take(&mut self.cur.0[i]),
            std::mem::take(&mut self.cur.1[i]),
            self.cur.2[i],
        )))
    }
}

/// Merge whole files, newest first; for each key the newest entry wins.
pub fn merge_files(
    files: &[&[u8]],
    drop_deleted: bool,
    o: Options,
    max_file_bytes: usize,
) -> Result<Vec<Vec<u8>>> {
    let mut iters: Vec<FileIter> = files
        .iter()
        .map(|f| FileIter::new(f))
        .collect::<Result<_>>()?;
    // Heap of (key, rank): for equal keys the smallest rank — the newest file — pops first.
    let mut heap: BinaryHeap<Reverse<(Vec<u8>, usize)>> = BinaryHeap::new();
    let mut heads: Vec<Option<(Vec<u8>, u8)>> = vec![None; iters.len()];
    for (rank, it) in iters.iter_mut().enumerate() {
        if let Some((k, v, f)) = it.next_entry()? {
            heads[rank] = Some((v, f));
            heap.push(Reverse((k, rank)));
        }
    }
    let raw_budget = 2 * max_file_bytes;
    let mut out = Vec::new();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let mut versions: Vec<Vec<u8>> = Vec::new();
    let mut flags: Vec<u8> = Vec::new();
    let mut approx = 0usize;
    let mut last: Option<Vec<u8>> = None;

    let flush = |keys: &mut Vec<Vec<u8>>,
                 versions: &mut Vec<Vec<u8>>,
                 flags: &mut Vec<u8>,
                 out: &mut Vec<Vec<u8>>|
     -> Result<()> {
        if !keys.is_empty() {
            let ks: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
            let vs: Vec<&[u8]> = versions.iter().map(|v| v.as_slice()).collect();
            out.push(encode_file(&ks, &vs, flags, o)?);
        }
        keys.clear();
        versions.clear();
        flags.clear();
        Ok(())
    };

    while let Some(Reverse((key, rank))) = heap.pop() {
        let (ver, flag) = heads[rank].take().expect("a heap entry always has a head");
        if let Some((k, v, f)) = iters[rank].next_entry()? {
            heads[rank] = Some((v, f));
            heap.push(Reverse((k, rank)));
        }
        if last.as_deref() == Some(key.as_slice()) {
            continue; // an older entry for a key already taken from a newer file
        }
        last = Some(key.clone());
        if flag != 0 && drop_deleted {
            continue;
        }
        approx += key.len() + ver.len() + 4;
        keys.push(key);
        versions.push(ver);
        flags.push(flag);
        if approx >= raw_budget {
            flush(&mut keys, &mut versions, &mut flags, &mut out)?;
            approx = 0;
        }
    }
    flush(&mut keys, &mut versions, &mut flags, &mut out)?;
    Ok(out)
}

// -- read kernels -------------------------------------------------------------------

/// Find sorted `keys` in one file's consecutive `blocks`: per key, found, version, deleted.
pub fn lookup(
    blocks: &[&[u8]],
    codec: u8,
    keys: &[&[u8]],
) -> Result<(Vec<u8>, Vec<Vec<u8>>, Vec<u8>)> {
    let mut ek: Vec<Vec<u8>> = Vec::new();
    let mut ev: Vec<Vec<u8>> = Vec::new();
    let mut ef: Vec<u8> = Vec::new();
    for blk in blocks {
        let (k, v, f) = decode_block(blk, codec)?;
        ek.extend(k);
        ev.extend(v);
        ef.extend(f);
    }
    let mut found = Vec::with_capacity(keys.len());
    let mut versions = Vec::with_capacity(keys.len());
    let mut deleted = Vec::with_capacity(keys.len());
    for key in keys {
        match ek.binary_search_by(|probe| probe.as_slice().cmp(key)) {
            Ok(i) => {
                found.push(1);
                versions.push(ev[i].clone());
                deleted.push(ef[i]);
            }
            Err(_) => {
                found.push(0);
                versions.push(Vec::new());
                deleted.push(0);
            }
        }
    }
    Ok((found, versions, deleted))
}

/// Iterates one run — a file's consecutive blocks — decoding a block at a time.
struct RunIter<'a> {
    blocks: &'a [&'a [u8]],
    codec: u8,
    next_block: usize,
    cur: Decoded,
    pos: usize,
}

impl<'a> RunIter<'a> {
    fn new(blocks: &'a [&'a [u8]], codec: u8) -> Self {
        RunIter {
            blocks,
            codec,
            next_block: 0,
            cur: (vec![], vec![], vec![]),
            pos: 0,
        }
    }

    fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>, u8)>> {
        while self.pos >= self.cur.0.len() {
            if self.next_block >= self.blocks.len() {
                return Ok(None);
            }
            self.cur = decode_block(self.blocks[self.next_block], self.codec)?;
            self.next_block += 1;
            self.pos = 0;
        }
        let i = self.pos;
        self.pos += 1;
        Ok(Some((
            std::mem::take(&mut self.cur.0[i]),
            std::mem::take(&mut self.cur.1[i]),
            self.cur.2[i],
        )))
    }

    /// The next entry with a key inside `(after, upto]`, or None past `upto`.
    fn next_in(
        &mut self,
        after: Option<&[u8]>,
        upto: Option<&[u8]>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>, u8)>> {
        loop {
            let Some(e) = self.next_entry()? else {
                return Ok(None);
            };
            if after.is_some_and(|a| e.0.as_slice() <= a) {
                continue;
            }
            if upto.is_some_and(|u| e.0.as_slice() > u) {
                return Ok(None);
            }
            return Ok(Some(e));
        }
    }
}

/// The newest-wins merged view of `runs` (newest first) over keys in `(after, upto]`.
pub fn merge_range(
    runs: &[Vec<&[u8]>],
    codec: u8,
    after: Option<&[u8]>,
    upto: Option<&[u8]>,
    drop_deleted: bool,
) -> Result<Decoded> {
    let mut iters: Vec<RunIter> = runs
        .iter()
        .map(|r| RunIter::new(r.as_slice(), codec))
        .collect();
    let mut heap: BinaryHeap<Reverse<(Vec<u8>, usize)>> = BinaryHeap::new();
    let mut heads: Vec<Option<(Vec<u8>, u8)>> = vec![None; iters.len()];
    for (rank, it) in iters.iter_mut().enumerate() {
        if let Some((k, v, f)) = it.next_in(after, upto)? {
            heads[rank] = Some((v, f));
            heap.push(Reverse((k, rank)));
        }
    }
    let (mut keys, mut versions, mut flags) = (Vec::new(), Vec::new(), Vec::new());
    let mut last: Option<Vec<u8>> = None;
    while let Some(Reverse((key, rank))) = heap.pop() {
        let (ver, flag) = heads[rank].take().expect("a heap entry always has a head");
        if let Some((k, v, f)) = iters[rank].next_in(after, upto)? {
            heads[rank] = Some((v, f));
            heap.push(Reverse((k, rank)));
        }
        if last.as_deref() == Some(key.as_slice()) {
            continue;
        }
        last = Some(key.clone());
        if flag != 0 && drop_deleted {
            continue;
        }
        keys.push(key);
        versions.push(ver);
        flags.push(flag);
    }
    Ok((keys, versions, flags))
}

pub type ReplaceDiff = (Vec<u8>, Vec<u8>, Vec<Vec<u8>>, u64);

/// Compare a full replacement (sorted keys, versions) with the merged existing index.
pub fn replace_diff(
    runs: &[Vec<&[u8]>],
    codec: u8,
    keys: &[&[u8]],
    versions: &[&[u8]],
) -> Result<ReplaceDiff> {
    let (ek, ev, _) = merge_range(runs, codec, None, None, true)?;
    let mut changed = Vec::with_capacity(keys.len());
    let mut existed = Vec::with_capacity(keys.len());
    let mut removed = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < keys.len() || j < ek.len() {
        if j >= ek.len() || (i < keys.len() && keys[i] < ek[j].as_slice()) {
            changed.push(1);
            existed.push(0);
            i += 1;
        } else if i >= keys.len() || ek[j].as_slice() < keys[i] {
            removed.push(ek[j].clone());
            j += 1;
        } else {
            changed.push((versions[i] != ev[j].as_slice()) as u8);
            existed.push(1);
            i += 1;
            j += 1;
        }
    }
    Ok((changed, existed, removed, ek.len() as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    const O: Options = Options {
        block_size: 64,
        level: 6,
        bits_per_item: 14,
        k: 10,
        codec: CODEC_ZLIB,
    };

    #[test]
    fn roundtrip_and_merge() {
        let keys: Vec<Vec<u8>> = (0..500)
            .map(|i| format!("key-{i:05}").into_bytes())
            .collect();
        let vers: Vec<Vec<u8>> = (0..500).map(|i| format!("v{i}").into_bytes()).collect();
        let ks: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let vs: Vec<&[u8]> = vers.iter().map(|v| v.as_slice()).collect();
        let del = vec![0u8; 500];
        let f = encode_file(&ks, &vs, &del, O).unwrap();
        let merged = merge_files(&[&f], false, O, 1 << 20).unwrap();
        assert_eq!(merged.len(), 1);
        let mut it = FileIter::new(&merged[0]).unwrap();
        let mut n = 0;
        while let Some((k, v, _)) = it.next_entry().unwrap() {
            assert_eq!(k, keys[n]);
            assert_eq!(v, vers[n]);
            n += 1;
        }
        assert_eq!(n, 500);
    }
}
