//! Our block format for MVCC entries: rows sorted by key then commit, newest
//! first, prefix-coded against the previous key, varints, zstd per block. A
//! block is cut only between keys (~`block` raw bytes). The block index
//! (offset, sizes, count, first key prefix-coded) sits at the tail in
//! segments of 128 blocks, zstd'd, under a top level that lists each
//! segment's first key: a reader of a large layer loads only the segments
//! it needs. Where the index and its top start is the layer's state.

use crate::run::{schema, Run};
use arrow_array::builder::{BinaryBuilder, Int64Builder};
use arrow_array::RecordBatch;
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;

pub fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

#[inline]
pub fn get_varint(b: &[u8], p: &mut usize) -> u64 {
    let mut n = 0u64;
    let mut s = 0;
    loop {
        let x = b[*p];
        *p += 1;
        n |= ((x & 0x7f) as u64) << s;
        if x < 0x80 {
            return n;
        }
        s += 7;
    }
}

fn shared(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

pub struct Writer<W: Write> {
    out: W,
    written: u64,
    level: i32,
    block: usize,
    seg_blocks: usize,
    raw: Vec<u8>,
    cols: Option<Cols>,
    n: u64,
    prev: Vec<u8>,
    first: Vec<u8>,
    // the current segment of the block index, and those written
    seg: Vec<u8>,
    seg_n: usize,
    seg_first: Vec<u8>,
    seg_prev: Vec<u8>,
    seg_prev_offset: u64,
    segs: Vec<u8>,
    top: Vec<u8>,
    top_prev: Vec<u8>,
    nsegs: u64,
}

impl<W: Write> Writer<W> {
    pub fn new(out: W, block: usize, level: i32) -> Self {
        Writer {
            out,
            written: 0,
            level,
            block,
            seg_blocks: 128,
            raw: Vec::with_capacity(block * 2),
            cols: None,
            n: 0,
            prev: Vec::new(),
            first: Vec::new(),
            seg: Vec::new(),
            seg_n: 0,
            seg_first: Vec::new(),
            seg_prev: Vec::new(),
            seg_prev_offset: 0,
            segs: Vec::new(),
            top: Vec::new(),
            top_prev: Vec::new(),
            nsegs: 0,
        }
    }

    /// Blocks laid out by column (`layout=cols`): each block holds its key
    /// prefixes, suffixes, commits, presence flags and versions as sections.
    pub fn by_columns(mut self) -> Self {
        self.cols = Some(Cols::default());
        self
    }

    fn raw_len(&self) -> usize {
        self.cols.as_ref().map_or(self.raw.len(), |c| c.len())
    }

    pub fn push(&mut self, key: &[u8], commit: u64, new: Option<u64>, replaced: Option<u64>) {
        if self.raw_len() >= self.block && key != self.prev.as_slice() {
            self.flush();
        }
        let s = if self.n == 0 {
            0
        } else {
            shared(&self.prev, key)
        };
        if self.n == 0 {
            self.first.clear();
            self.first.extend_from_slice(key);
        }
        if let Some(c) = &mut self.cols {
            c.push(s, &key[s..], commit, new, replaced);
            self.prev.clear();
            self.prev.extend_from_slice(key);
            self.n += 1;
            return;
        }
        put_varint(&mut self.raw, s as u64);
        put_varint(&mut self.raw, (key.len() - s) as u64);
        self.raw.extend_from_slice(&key[s..]);
        put_varint(&mut self.raw, commit);
        self.raw
            .push(new.is_some() as u8 | (replaced.is_some() as u8) << 1);
        if let Some(v) = new {
            put_varint(&mut self.raw, v);
        }
        if let Some(v) = replaced {
            put_varint(&mut self.raw, v);
        }
        self.prev.clear();
        self.prev.extend_from_slice(key);
        self.n += 1;
    }

    fn flush(&mut self) {
        if self.n == 0 {
            return;
        }
        if let Some(c) = &mut self.cols {
            self.raw.clear();
            c.finish(&mut self.raw);
        }
        let c = zstd::bulk::compress(&self.raw, self.level).unwrap();
        self.out.write_all(&c).unwrap();
        if self.seg_n == 0 {
            self.seg_first = self.first.clone();
            self.seg_prev.clear();
            self.seg_prev_offset = 0;
        }
        let ix = &mut self.seg;
        put_varint(ix, self.written - self.seg_prev_offset);
        self.seg_prev_offset = self.written;
        put_varint(ix, c.len() as u64);
        put_varint(ix, self.raw.len() as u64);
        put_varint(ix, self.n);
        let s = shared(&self.seg_prev, &self.first);
        put_varint(ix, s as u64);
        put_varint(ix, (self.first.len() - s) as u64);
        ix.extend_from_slice(&self.first[s..]);
        self.seg_prev = self.first.clone();
        self.written += c.len() as u64;
        self.seg_n += 1;
        self.raw.clear();
        self.n = 0;
        if self.seg_n == self.seg_blocks {
            self.flush_seg();
        }
    }

    fn flush_seg(&mut self) {
        if self.seg_n == 0 {
            return;
        }
        let c = zstd::bulk::compress(&self.seg, self.level).unwrap();
        let t = &mut self.top;
        put_varint(t, self.segs.len() as u64);
        put_varint(t, c.len() as u64);
        put_varint(t, self.seg.len() as u64);
        put_varint(t, self.seg_n as u64);
        let s = shared(&self.top_prev, &self.seg_first);
        put_varint(t, s as u64);
        put_varint(t, (self.seg_first.len() - s) as u64);
        t.extend_from_slice(&self.seg_first[s..]);
        self.top_prev = self.seg_first.clone();
        self.segs.extend_from_slice(&c);
        self.nsegs += 1;
        self.seg.clear();
        self.seg_n = 0;
    }

    /// The file is written: its size, where its block index starts (its
    /// segments), and where the index's top level starts.
    pub fn finish(mut self) -> (W, u64, u64, u64) {
        self.flush();
        self.flush_seg();
        let index_start = self.written;
        self.out.write_all(&self.segs).unwrap();
        let top_start = index_start + self.segs.len() as u64;
        let mut top = Vec::new();
        put_varint(&mut top, self.nsegs);
        top.extend_from_slice(&self.top);
        let c = zstd::bulk::compress(&top, self.level).unwrap();
        self.out.write_all(&c).unwrap();
        self.out
            .write_all(&(top.len() as u32).to_le_bytes())
            .unwrap();
        (
            self.out,
            top_start + c.len() as u64 + 4,
            index_start,
            top_start,
        )
    }
}

pub struct Block {
    pub offset: u64,
    pub clen: u64,
    pub rlen: u64,
    pub n: u64,
    pub first: Vec<u8>,
}

pub struct Seg {
    pub first: Vec<u8>,
    pub range: Range<u64>,
    pub rlen: u64,
}

/// A layer's block index: its top level (segments and their first keys), and
/// the segments loaded so far, in order.
pub struct Index {
    pub segs: Vec<Seg>,
    pub loaded: std::collections::BTreeMap<usize, Vec<Block>>,
}

/// The last item whose first key is at or below `k` (the first: none is).
fn at_or_below<T>(v: &[T], first: impl Fn(&T) -> &[u8], k: &[u8]) -> usize {
    v.partition_point(|x| first(x) <= k).saturating_sub(1)
}

impl Index {
    /// The top level, from the object's tail `[top_start, size)`.
    pub fn parse_top(tail: &[u8], index_start: u64) -> Index {
        let rlen = u32::from_le_bytes(tail[tail.len() - 4..].try_into().unwrap()) as usize;
        let t = zstd::bulk::decompress(&tail[..tail.len() - 4], rlen).unwrap();
        let mut p = 0;
        let count = get_varint(&t, &mut p);
        let mut prev: Vec<u8> = Vec::new();
        let segs = (0..count)
            .map(|_| {
                let off = index_start + get_varint(&t, &mut p);
                let clen = get_varint(&t, &mut p);
                let rlen = get_varint(&t, &mut p);
                let _blocks = get_varint(&t, &mut p);
                let s = get_varint(&t, &mut p) as usize;
                let l = get_varint(&t, &mut p) as usize;
                let mut first = prev[..s].to_vec();
                first.extend_from_slice(&t[p..p + l]);
                p += l;
                prev = first.clone();
                Seg {
                    first,
                    range: off..off + clen,
                    rlen,
                }
            })
            .collect();
        Index {
            segs,
            loaded: Default::default(),
        }
    }

    /// Segments that may hold keys in `[lo, hi)`.
    pub fn segs_for(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Vec<usize> {
        let s = &self.segs;
        let a = lo.map_or(0, |lo| at_or_below(s, |x| &x.first, lo));
        let b = hi.map_or(s.len(), |hi| s.partition_point(|x| x.first.as_slice() < hi));
        (a..b.max(a + 1).min(s.len())).collect()
    }

    pub fn segs_for_keys(&self, keys: &[Vec<u8>]) -> Vec<usize> {
        let mut v: Vec<usize> = keys
            .iter()
            .map(|k| at_or_below(&self.segs, |x| &x.first, k))
            .collect();
        v.dedup();
        v
    }

    pub fn load(&mut self, i: usize, data: &[u8]) {
        let seg = &self.segs[i];
        let ix = zstd::bulk::decompress(data, seg.rlen as usize).unwrap();
        let mut p = 0;
        let (mut offset, mut prev, mut blocks) = (0u64, Vec::<u8>::new(), Vec::new());
        while p < ix.len() {
            offset += get_varint(&ix, &mut p);
            let clen = get_varint(&ix, &mut p);
            let rlen = get_varint(&ix, &mut p);
            let n = get_varint(&ix, &mut p);
            let s = get_varint(&ix, &mut p) as usize;
            let l = get_varint(&ix, &mut p) as usize;
            let mut first = prev[..s].to_vec();
            first.extend_from_slice(&ix[p..p + l]);
            p += l;
            prev = first.clone();
            blocks.push(Block {
                offset,
                clen,
                rlen,
                n,
                first,
            });
        }
        self.loaded.insert(i, blocks);
    }

    fn blocks(&self) -> Vec<&Block> {
        self.loaded.values().flatten().collect()
    }

    /// Loaded blocks that may hold keys in `[lo, hi)`.
    pub fn span(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Vec<&Block> {
        let b = self.blocks();
        let a = lo.map_or(0, |lo| at_or_below(&b, |x| &x.first, lo));
        let e = hi.map_or(b.len(), |hi| b.partition_point(|x| x.first.as_slice() < hi));
        b[a..e.max(a)].to_vec()
    }

    /// Loaded blocks that may hold one of `keys`.
    pub fn of_keys(&self, keys: &[Vec<u8>]) -> Vec<&Block> {
        let b = self.blocks();
        let mut ix: Vec<usize> = keys
            .iter()
            .filter(|k| {
                b.first()
                    .is_some_and(|x| x.first.as_slice() <= k.as_slice())
            })
            .map(|k| at_or_below(&b, |x| &x.first, k))
            .collect();
        ix.dedup();
        ix.into_iter().map(|i| b[i]).collect()
    }
}

/// A block's entries by column, as it fills.
#[derive(Default)]
pub struct Cols {
    shared: Vec<u8>,
    suffix_len: Vec<u8>,
    suffix: Vec<u8>,
    commit: Vec<u8>,
    flags: Vec<u8>,
    new: Vec<u8>,
    replaced: Vec<u8>,
}

impl Cols {
    fn push(
        &mut self,
        s: usize,
        suffix: &[u8],
        commit: u64,
        new: Option<u64>,
        replaced: Option<u64>,
    ) {
        put_varint(&mut self.shared, s as u64);
        put_varint(&mut self.suffix_len, suffix.len() as u64);
        self.suffix.extend_from_slice(suffix);
        put_varint(&mut self.commit, commit);
        self.flags
            .push(new.is_some() as u8 | (replaced.is_some() as u8) << 1);
        if let Some(v) = new {
            put_varint(&mut self.new, v);
        }
        if let Some(v) = replaced {
            put_varint(&mut self.replaced, v);
        }
    }
    fn parts(&mut self) -> [&mut Vec<u8>; 7] {
        [
            &mut self.shared,
            &mut self.suffix_len,
            &mut self.suffix,
            &mut self.commit,
            &mut self.flags,
            &mut self.new,
            &mut self.replaced,
        ]
    }
    fn len(&self) -> usize {
        self.shared.len()
            + self.suffix_len.len()
            + self.suffix.len()
            + self.commit.len()
            + self.flags.len()
            + self.new.len()
            + self.replaced.len()
    }
    /// The sections' lengths, then the sections; emptied for the next block.
    fn finish(&mut self, out: &mut Vec<u8>) {
        out.push(0xC0); // a columnar block
        for p in self.parts() {
            put_varint(out, p.len() as u64);
        }
        for p in self.parts() {
            out.extend_from_slice(p);
            p.clear();
        }
    }
}

pub fn decode(data: &[u8], rlen: u64, n: u64) -> Run {
    let raw = zstd::bulk::decompress(data, rlen as usize).unwrap();
    if raw.first() == Some(&0xC0) {
        return decode_cols(&raw, n as usize);
    }
    let n = n as usize;
    let mut key = BinaryBuilder::with_capacity(n, n * 48);
    let (mut commit, mut new, mut replaced) = (
        Int64Builder::with_capacity(n),
        Int64Builder::with_capacity(n),
        Int64Builder::with_capacity(n),
    );
    let mut prev: Vec<u8> = Vec::with_capacity(64);
    let mut p = 0;
    for _ in 0..n {
        let s = get_varint(&raw, &mut p) as usize;
        let l = get_varint(&raw, &mut p) as usize;
        prev.truncate(s);
        prev.extend_from_slice(&raw[p..p + l]);
        p += l;
        key.append_value(&prev);
        commit.append_value(get_varint(&raw, &mut p) as i64);
        let f = raw[p];
        p += 1;
        if f & 1 != 0 {
            new.append_value(get_varint(&raw, &mut p) as i64)
        } else {
            new.append_null()
        }
        if f & 2 != 0 {
            replaced.append_value(get_varint(&raw, &mut p) as i64)
        } else {
            replaced.append_null()
        }
    }
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(key.finish()),
            Arc::new(commit.finish()),
            Arc::new(new.finish()),
            Arc::new(replaced.finish()),
        ],
    )
    .unwrap();
    Run::of(&batch)
}

fn decode_cols(raw: &[u8], n: usize) -> Run {
    let mut p = 1;
    let lens: Vec<usize> = (0..7).map(|_| get_varint(raw, &mut p) as usize).collect();
    let mut at = [0usize; 7];
    for i in 0..7 {
        at[i] = p;
        p += lens[i];
    }
    let [mut sh, mut sl, mut sx, mut cm, fl, mut nw, mut rp] = at;
    let mut key = BinaryBuilder::with_capacity(n, n * 56);
    let (mut commit, mut new, mut replaced) = (
        Int64Builder::with_capacity(n),
        Int64Builder::with_capacity(n),
        Int64Builder::with_capacity(n),
    );
    let mut prev: Vec<u8> = Vec::with_capacity(64);
    for i in 0..n {
        let s = get_varint(raw, &mut sh) as usize;
        let l = get_varint(raw, &mut sl) as usize;
        prev.truncate(s);
        prev.extend_from_slice(&raw[sx..sx + l]);
        sx += l;
        key.append_value(&prev);
        commit.append_value(get_varint(raw, &mut cm) as i64);
        let f = raw[fl + i];
        if f & 1 != 0 {
            new.append_value(get_varint(raw, &mut nw) as i64)
        } else {
            new.append_null()
        }
        if f & 2 != 0 {
            replaced.append_value(get_varint(raw, &mut rp) as i64)
        } else {
            replaced.append_null()
        }
    }
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(key.finish()),
            Arc::new(commit.finish()),
            Arc::new(new.finish()),
            Arc::new(replaced.finish()),
        ],
    )
    .unwrap();
    Run::of(&batch)
}
