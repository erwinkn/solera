//! Streaming over `.kx` files: decoded blocks, runs of files fed a segment at
//! a time, the newest-wins merge of runs, and a writer that cuts files as
//! they fill. Memory is a few blocks per run and one output file, whatever
//! the number of entries.

use std::collections::VecDeque;
use std::sync::Arc;

use rayon::prelude::*;
use xxhash_rust::xxh3::xxh3_128;

use crate::format::{
    compress, decompress, filter_nbits, fmt_err, get_varint, hash_positions, key_item, pair_item,
    put_bytes, put_varint, shared_prefix, tomb_item, Error, Options, Result, FORMAT_VERSION, MAGIC,
};

/// Bytes owned elsewhere — a Python `bytes`, or a `Vec` in tests.
pub type Bytes = Arc<dyn AsRef<[u8]> + Send + Sync>;

// -- blocks -----------------------------------------------------------------------

/// Entry flags (docs/key-index-format.md § Blocks).
const DELETED: u8 = 1;
const PREDECESSOR: u8 = 2;

/// One encoded entry, its byte strings as ranges of the block.
struct Fields {
    shared: usize,
    suffix: (usize, usize),
    version: (usize, usize),
    flags: u8,
    locator: u64,
    predecessor: Option<((usize, usize), u64)>,
}

fn range(buf: &[u8], pos: &mut usize) -> Result<(usize, usize)> {
    let n = get_varint(buf, pos)? as usize;
    let start = *pos;
    if start + n > buf.len() {
        return fmt_err("truncated entry");
    }
    *pos += n;
    Ok((start, start + n))
}

fn read_entry(raw: &[u8], pos: &mut usize) -> Result<Fields> {
    let shared = get_varint(raw, pos)? as usize;
    let suffix = range(raw, pos)?;
    let version = range(raw, pos)?;
    let Some(&flags) = raw.get(*pos) else {
        return fmt_err("truncated entry");
    };
    *pos += 1;
    if flags & !(DELETED | PREDECESSOR) != 0 {
        return fmt_err("unknown entry flags");
    }
    let locator = get_varint(raw, pos)?;
    let predecessor = if flags & PREDECESSOR != 0 {
        Some((range(raw, pos)?, get_varint(raw, pos)?))
    } else {
        None
    };
    Ok(Fields {
        shared,
        suffix,
        version,
        flags,
        locator,
        predecessor,
    })
}

#[allow(clippy::too_many_arguments)]
fn write_entry(
    out: &mut Vec<u8>,
    shared: usize,
    suffix: &[u8],
    version: &[u8],
    deleted: bool,
    locator: u64,
    predecessor: Option<(&[u8], u64)>,
) {
    put_varint(out, shared as u64);
    put_bytes(out, suffix);
    put_bytes(out, version);
    out.push(
        if deleted { DELETED } else { 0 }
            | if predecessor.is_some() {
                PREDECESSOR
            } else {
                0
            },
    );
    put_varint(out, locator);
    if let Some((v, l)) = predecessor {
        put_bytes(out, v);
        put_varint(out, l);
    }
}

#[derive(Clone, Copy)]
struct Ent {
    key: u32,
    key_len: u32,
    ver: (u32, u32),
    deleted: bool,
    locator: u64,
    predecessor: Option<((u32, u32), u64)>,
}

/// A decoded block: whole keys in one arena, versions in place in the raw bytes.
pub struct Block {
    raw: Vec<u8>,
    keys: Vec<u8>,
    ents: Vec<Ent>,
}

impl Block {
    pub fn decode(data: &[u8], codec: u8) -> Result<Block> {
        let raw = decompress(data, codec)?;
        let mut keys: Vec<u8> = Vec::with_capacity(raw.len());
        let mut ents = Vec::new();
        let (mut pos, mut prev, mut prev_len) = (0usize, 0usize, 0usize);
        let r = |(a, b): (usize, usize)| (a as u32, b as u32);
        while pos < raw.len() {
            let f = read_entry(&raw, &mut pos)?;
            if f.shared > prev_len {
                return fmt_err("bad shared prefix length");
            }
            let key = keys.len();
            keys.extend_from_within(prev..prev + f.shared);
            keys.extend_from_slice(&raw[f.suffix.0..f.suffix.1]);
            prev = key;
            prev_len = keys.len() - key;
            ents.push(Ent {
                key: key as u32,
                key_len: prev_len as u32,
                ver: r(f.version),
                deleted: f.flags & DELETED != 0,
                locator: f.locator,
                predecessor: f.predecessor.map(|(v, l)| (r(v), l)),
            });
        }
        Ok(Block { raw, keys, ents })
    }

    pub fn len(&self) -> usize {
        self.ents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ents.is_empty()
    }

    #[inline]
    pub fn key(&self, i: usize) -> &[u8] {
        let e = &self.ents[i];
        &self.keys[e.key as usize..(e.key + e.key_len) as usize]
    }

    #[inline]
    pub fn version(&self, i: usize) -> &[u8] {
        let (a, b) = self.ents[i].ver;
        &self.raw[a as usize..b as usize]
    }

    #[inline]
    pub fn deleted(&self, i: usize) -> bool {
        self.ents[i].deleted
    }

    #[inline]
    pub fn locator(&self, i: usize) -> u64 {
        self.ents[i].locator
    }

    /// The version and locator the key had before this entry (delta files only).
    pub fn predecessor(&self, i: usize) -> Option<(&[u8], u64)> {
        self.ents[i]
            .predecessor
            .map(|((a, b), l)| (&self.raw[a as usize..b as usize], l))
    }
}

// -- runs ---------------------------------------------------------------------------

/// Consecutive blocks of one file: their bytes, and each block's offset in
/// `data`, compressed size and CRC.
pub struct Segment {
    pub data: Bytes,
    pub blocks: Vec<(usize, usize, u32)>,
    pub codec: u8,
}

/// Blocks decoded at once per run: enough to keep every core busy.
const DECODE_AHEAD: usize = 32;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    Ready,
    Starved,
    Done,
}

/// One sorted sequence of entries — a file, or a level's files in key order —
/// fed a segment at a time. Only the blocks being read are decoded.
#[derive(Default)]
pub struct Run {
    segs: VecDeque<(Segment, usize)>,
    ready: VecDeque<Block>,
    pos: usize,
    ended: bool,
}

impl Run {
    pub fn feed(&mut self, seg: Segment) {
        self.segs.push_back((seg, 0));
    }

    /// No more segments will come.
    pub fn end(&mut self) {
        self.ended = true;
    }

    pub fn state(&mut self) -> Result<State> {
        loop {
            match self.ready.front() {
                Some(b) if self.pos < b.len() => return Ok(State::Ready),
                Some(_) => {
                    self.ready.pop_front();
                    self.pos = 0;
                }
                None => {
                    let Some((seg, next)) = self.segs.front_mut() else {
                        return Ok(if self.ended {
                            State::Done
                        } else {
                            State::Starved
                        });
                    };
                    let end = (*next + DECODE_AHEAD).min(seg.blocks.len());
                    let data = (*seg.data).as_ref();
                    let codec = seg.codec;
                    let decoded: Vec<Block> = seg.blocks[*next..end]
                        .par_iter()
                        .map(|&(off, size, crc)| {
                            let Some(raw) = data.get(off..off + size) else {
                                return fmt_err("block out of bounds");
                            };
                            if crc32fast::hash(raw) != crc {
                                return fmt_err("block checksum mismatch");
                            }
                            Block::decode(raw, codec)
                        })
                        .collect::<Result<_>>()?;
                    *next = end;
                    if end == seg.blocks.len() {
                        self.segs.pop_front();
                    }
                    self.ready.extend(decoded);
                }
            }
        }
    }

    #[inline]
    fn block(&self) -> &Block {
        &self.ready[0]
    }
}

// -- the merged view ------------------------------------------------------------------

pub enum Next {
    Entry,
    /// Run `r` needs its next segment (or `end`) before the merge can go on.
    Need(usize),
    End,
}

/// The newest-wins merge of runs, newest first: for each key, the entry of
/// the lowest-numbered run holding it.
pub struct Merge {
    pub runs: Vec<Run>,
    heap: Vec<usize>,
    pending: Vec<usize>,
    cur: (usize, usize),
}

impl Merge {
    pub fn new(runs: usize) -> Merge {
        Merge {
            runs: (0..runs).map(|_| Run::default()).collect(),
            heap: Vec::with_capacity(runs),
            pending: (0..runs).rev().collect(),
            cur: (usize::MAX, 0),
        }
    }

    #[inline]
    fn head(&self, r: usize) -> &[u8] {
        let run = &self.runs[r];
        run.block().key(run.pos)
    }

    #[inline]
    fn less(&self, a: usize, b: usize) -> bool {
        match self.head(a).cmp(self.head(b)) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => a < b,
            std::cmp::Ordering::Greater => false,
        }
    }

    fn push(&mut self, r: usize) {
        self.heap.push(r);
        let mut i = self.heap.len() - 1;
        while i > 0 {
            let p = (i - 1) / 2;
            if !self.less(self.heap[i], self.heap[p]) {
                break;
            }
            self.heap.swap(i, p);
            i = p;
        }
    }

    fn pop(&mut self) -> usize {
        let top = self.heap.swap_remove(0);
        let n = self.heap.len();
        let mut i = 0;
        loop {
            let (l, r) = (2 * i + 1, 2 * i + 2);
            let mut m = i;
            if l < n && self.less(self.heap[l], self.heap[m]) {
                m = l;
            }
            if r < n && self.less(self.heap[r], self.heap[m]) {
                m = r;
            }
            if m == i {
                return top;
            }
            self.heap.swap(i, m);
            i = m;
        }
    }

    /// Moves to the next key. The entry stays readable until the next call.
    pub fn next_key(&mut self) -> Result<Next> {
        // Runs the last entry came from have moved past it; they rejoin once readable.
        while let Some(&r) = self.pending.last() {
            match self.runs[r].state()? {
                State::Ready => {
                    self.pending.pop();
                    self.push(r);
                }
                State::Done => {
                    self.pending.pop();
                }
                State::Starved => return Ok(Next::Need(r)),
            }
        }
        if self.heap.is_empty() {
            return Ok(Next::End);
        }
        let r = self.pop();
        self.cur = (r, self.runs[r].pos);
        self.runs[r].pos += 1;
        self.pending.push(r);
        // Older entries for the same key are shadowed.
        while let Some(&o) = self.heap.first() {
            let run = &self.runs[o];
            if run.block().key(run.pos) != self.key() {
                break;
            }
            self.pop();
            self.runs[o].pos += 1;
            self.pending.push(o);
        }
        Ok(Next::Entry)
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        self.runs[self.cur.0].block().key(self.cur.1)
    }

    #[inline]
    pub fn version(&self) -> &[u8] {
        self.runs[self.cur.0].block().version(self.cur.1)
    }

    #[inline]
    pub fn deleted(&self) -> bool {
        self.runs[self.cur.0].block().deleted(self.cur.1)
    }

    #[inline]
    pub fn locator(&self) -> u64 {
        self.runs[self.cur.0].block().locator(self.cur.1)
    }
}

// -- writing ------------------------------------------------------------------------

/// Raw blocks compressed at once, on every core.
const COMPRESS_BATCH: usize = 64;

struct RawBlock {
    data: Vec<u8>,
    first: Vec<u8>,
    last: Vec<u8>,
    count: u64,
}

struct Packed {
    data: Vec<u8>,
    crc: u32,
    first: Vec<u8>,
    last: Vec<u8>,
    count: u64,
    keys: Vec<u128>,
    pairs: Vec<u128>,
    tombs: Vec<u128>,
}

/// Compresses a raw block and hashes its filter items.
fn pack(b: RawBlock, o: &Options) -> Result<Packed> {
    let (mut keys, mut pairs, mut tombs) = (Vec::new(), Vec::new(), Vec::new());
    let (mut pos, raw) = (0usize, &b.data);
    let mut key: Vec<u8> = Vec::new();
    let mut item = Vec::new();
    while pos < raw.len() {
        let f = read_entry(raw, &mut pos)?;
        key.truncate(f.shared);
        key.extend_from_slice(&raw[f.suffix.0..f.suffix.1]);
        let version = &raw[f.version.0..f.version.1];
        let deleted = f.flags & DELETED != 0;
        key_item(&mut item, &key);
        keys.push(xxh3_128(&item));
        if deleted {
            tomb_item(&mut item, &key);
            tombs.push(xxh3_128(&item));
        } else {
            pair_item(&mut item, &key, version);
            pairs.push(xxh3_128(&item));
        }
    }
    let data = compress(&b.data, o.codec, o.level);
    Ok(Packed {
        crc: crc32fast::hash(&data),
        data,
        first: b.first,
        last: b.last,
        count: b.count,
        keys,
        pairs,
        tombs,
    })
}

fn filter(hashes: &[u128], o: &Options, out: &mut Vec<u8>) {
    let nbits = filter_nbits(hashes.len() as u64, o.bits_per_item);
    let mut bits = vec![0u8; (nbits / 8) as usize];
    for &h in hashes {
        for b in hash_positions(h, nbits, o.k) {
            bits[(b >> 3) as usize] |= 1 << (b & 7);
        }
    }
    put_varint(out, nbits);
    out.push(o.k);
    out.extend_from_slice(&bits);
}

#[derive(Default)]
struct FileBuf {
    out: Vec<u8>,
    index: Vec<(Vec<u8>, u64, u64, u64, u32)>,
    keys: Vec<u128>,
    pairs: Vec<u128>,
    tombs: Vec<u128>,
    min: Vec<u8>,
    max: Vec<u8>,
    entries: u64,
}

impl FileBuf {
    fn add(&mut self, p: Packed) {
        if self.index.is_empty() {
            self.min = p.first.clone();
        }
        self.index.push((
            p.first,
            self.out.len() as u64,
            p.data.len() as u64,
            p.count,
            p.crc,
        ));
        self.out.extend_from_slice(&p.data);
        self.keys.extend_from_slice(&p.keys);
        self.pairs.extend_from_slice(&p.pairs);
        self.tombs.extend_from_slice(&p.tombs);
        self.max = p.last;
        self.entries += p.count;
    }

    fn finish(self, o: &Options) -> Vec<u8> {
        let FileBuf {
            mut out,
            index,
            keys,
            pairs,
            tombs,
            min,
            max,
            entries,
        } = self;
        let mut filters = Vec::with_capacity((keys.len() + pairs.len() + tombs.len()) * 2 + 256);
        filter(&keys, o, &mut filters);
        filter(&pairs, o, &mut filters);
        filter(&tombs, o, &mut filters);
        let crc = crc32fast::hash(&filters);
        filters.extend_from_slice(&crc.to_le_bytes());

        let mut idx = Vec::new();
        put_bytes(&mut idx, &min);
        put_bytes(&mut idx, &max);
        put_varint(&mut idx, index.len() as u64);
        for (first, off, size, count, crc) in &index {
            put_bytes(&mut idx, first);
            put_varint(&mut idx, *off);
            put_varint(&mut idx, *size);
            put_varint(&mut idx, *count);
            idx.extend_from_slice(&crc.to_le_bytes());
        }
        let idx = compress(&idx, o.codec, o.level);

        let filters_offset = out.len() as u64;
        out.extend_from_slice(&filters);
        let index_offset = out.len() as u64;
        out.extend_from_slice(&idx);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        out.push(o.codec);
        out.push(0);
        out.extend_from_slice(&entries.to_le_bytes());
        out.extend_from_slice(&filters_offset.to_le_bytes());
        out.extend_from_slice(&(filters.len() as u32).to_le_bytes());
        out.extend_from_slice(&index_offset.to_le_bytes());
        out.extend_from_slice(&(idx.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32fast::hash(&idx).to_le_bytes());
        out.extend_from_slice(MAGIC);
        out
    }
}

/// Writes entries, strictly increasing by key, as `.kx` files: a file is
/// closed once its blocks and filters reach `max_file_bytes`, and waits in
/// `files` for the caller to take it.
pub struct Writer {
    o: Options,
    max_file_bytes: usize,
    block: Vec<u8>,
    first: Vec<u8>,
    prev: Vec<u8>,
    count: u64,
    started: bool,
    raw: Vec<RawBlock>,
    file: FileBuf,
    pub files: VecDeque<Vec<u8>>,
    pub entries: u64,
}

impl Writer {
    pub fn new(o: Options, max_file_bytes: usize) -> Writer {
        Writer {
            o,
            max_file_bytes,
            block: Vec::with_capacity(o.block_size + 256),
            first: Vec::new(),
            prev: Vec::new(),
            count: 0,
            started: false,
            raw: Vec::new(),
            file: FileBuf::default(),
            files: VecDeque::new(),
            entries: 0,
        }
    }

    pub fn push(
        &mut self,
        key: &[u8],
        version: &[u8],
        deleted: bool,
        locator: u64,
        predecessor: Option<(&[u8], u64)>,
    ) -> Result<()> {
        if self.started && key <= self.prev.as_slice() {
            return Err(Error::Value(format!(
                "keys must be strictly increasing: {:?} then {:?}",
                String::from_utf8_lossy(&self.prev),
                String::from_utf8_lossy(key)
            )));
        }
        let shared = if self.count == 0 {
            self.first.clear();
            self.first.extend_from_slice(key);
            0
        } else {
            shared_prefix(&self.prev, key)
        };
        write_entry(
            &mut self.block,
            shared,
            &key[shared..],
            version,
            deleted,
            locator,
            predecessor,
        );
        self.prev.clear();
        self.prev.extend_from_slice(key);
        self.started = true;
        self.count += 1;
        self.entries += 1;
        if self.block.len() >= self.o.block_size {
            self.close_block()?;
        }
        Ok(())
    }

    fn close_block(&mut self) -> Result<()> {
        if self.count == 0 {
            return Ok(());
        }
        let data = std::mem::replace(&mut self.block, Vec::with_capacity(self.o.block_size + 256));
        self.raw.push(RawBlock {
            data,
            first: self.first.clone(),
            last: self.prev.clone(),
            count: self.count,
        });
        self.count = 0;
        if self.raw.len() >= COMPRESS_BATCH {
            self.compress()?;
        }
        Ok(())
    }

    fn compress(&mut self) -> Result<()> {
        let o = self.o;
        let packed: Vec<Packed> = std::mem::take(&mut self.raw)
            .into_par_iter()
            .map(|b| pack(b, &o))
            .collect::<Result<_>>()?;
        for p in packed {
            self.file.add(p);
            // The file so far: its blocks, and the filters they will need.
            let items = self.file.keys.len() + self.file.pairs.len() + self.file.tombs.len();
            if self.file.out.len() + items * self.o.bits_per_item as usize / 8
                >= self.max_file_bytes
            {
                self.cut();
            }
        }
        Ok(())
    }

    fn cut(&mut self) {
        let file = std::mem::take(&mut self.file);
        self.files.push_back(file.finish(&self.o));
    }

    /// Closes the last file; with `empty`, writes an empty file when there was nothing.
    pub fn finish(&mut self, empty: bool) -> Result<()> {
        self.close_block()?;
        self.compress()?;
        if self.file.entries > 0 || (empty && self.entries == 0) {
            self.cut();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{decode_block, encode_file, file_blocks, CODEC_ZLIB};

    const O: Options = Options {
        block_size: 256,
        level: 1,
        bits_per_item: 14,
        k: 10,
        codec: CODEC_ZLIB,
    };

    fn entries(n: usize, tag: &str, step: usize) -> Vec<(Vec<u8>, Vec<u8>, bool)> {
        (0..n)
            .map(|i| {
                let k = format!("key-{:06}", i * step).into_bytes();
                (k, format!("{tag}{i}").into_bytes(), i % 7 == 3)
            })
            .collect()
    }

    fn segments(file: &[u8], per: usize) -> Vec<Segment> {
        let (codec, blocks) = file_blocks(file).unwrap();
        let data: Bytes = Arc::new(file.to_vec());
        blocks
            .chunks(per)
            .map(|c| Segment {
                data: data.clone(),
                blocks: c
                    .iter()
                    .map(|b| (b.offset as usize, b.size as usize, b.crc))
                    .collect(),
                codec,
            })
            .collect()
    }

    fn write(es: &[(Vec<u8>, Vec<u8>, bool)], max: usize) -> Vec<Vec<u8>> {
        let mut w = Writer::new(O, max);
        for (k, v, d) in es {
            w.push(k, v, *d, 7, None).unwrap();
        }
        w.finish(true).unwrap();
        w.files.into_iter().collect()
    }

    #[test]
    fn writer_matches_encode_file() {
        let es = entries(3000, "v", 1);
        let files = write(&es, usize::MAX);
        assert_eq!(files.len(), 1);
        let ks: Vec<&[u8]> = es.iter().map(|e| e.0.as_slice()).collect();
        let vs: Vec<&[u8]> = es.iter().map(|e| e.1.as_slice()).collect();
        let ds: Vec<u8> = es.iter().map(|e| e.2 as u8).collect();
        let ls = vec![7u64; ks.len()];
        let ps = vec![None; ks.len()];
        assert_eq!(files[0], encode_file(&ks, &vs, &ds, &ls, &ps, O).unwrap());
        assert_eq!(
            write(&[], usize::MAX),
            vec![encode_file(&[], &[], &[], &[], &[], O).unwrap()]
        );
    }

    #[test]
    fn merge_newest_wins_across_split_files() {
        let old = entries(4000, "old", 1);
        let new = entries(1500, "new", 2);
        let old_files = write(&old, 4096);
        assert!(old_files.len() > 3);
        let new_files = write(&new, usize::MAX);
        let mut m = Merge::new(2);
        let mut feeds: Vec<VecDeque<Segment>> = vec![
            new_files.iter().flat_map(|f| segments(f, 3)).collect(),
            old_files.iter().flat_map(|f| segments(f, 2)).collect(),
        ];
        let mut got = Vec::new();
        loop {
            match m.next_key().unwrap() {
                Next::Entry => got.push((m.key().to_vec(), m.version().to_vec(), m.deleted())),
                Next::Need(r) => match feeds[r].pop_front() {
                    Some(s) => m.runs[r].feed(s),
                    None => m.runs[r].end(),
                },
                Next::End => break,
            }
        }
        let mut want: std::collections::BTreeMap<Vec<u8>, (Vec<u8>, bool)> =
            old.into_iter().map(|(k, v, d)| (k, (v, d))).collect();
        for (k, v, d) in new {
            want.insert(k, (v, d));
        }
        let want: Vec<_> = want.into_iter().map(|(k, (v, d))| (k, v, d)).collect();
        assert_eq!(got, want);
        // Blocks decode the same as the reference decoder.
        let (codec, blocks) = file_blocks(&old_files[0]).unwrap();
        let b = &blocks[0];
        let raw = &old_files[0][b.offset as usize..(b.offset + b.size) as usize];
        let (ks, vs, fs, _, _) = decode_block(raw, codec).unwrap();
        let blk = Block::decode(raw, codec).unwrap();
        for i in 0..ks.len() {
            assert_eq!(
                (blk.key(i), blk.version(i), blk.deleted(i)),
                (&ks[i][..], &vs[i][..], fs[i] != 0)
            );
        }
    }
}
