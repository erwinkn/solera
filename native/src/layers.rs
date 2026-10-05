//! Stamped layers (docs/key-index-design.md): the key index's files and the
//! work over them.
//!
//! A file is blocks back to back, with no index, filter or footer. A block is
//! a 13-byte header — compressed length, CRC-32 of the compressed bytes, raw
//! length (each u32, little-endian), a format byte — then zstd-compressed raw
//! bytes. Writers hand back the block boundaries; the caller keeps them as the
//! file's index (`Index`).
//!
//! Two formats of entries:
//!
//! - a **delta** (format 0) is one commit's: key, change kind (added,
//!   updated, removed), a source's payload, and — when the writer was asked —
//!   the generation the change replaced (what an immutable store's cleanup
//!   deletes). Its commit and generation are the commit record's, given when
//!   it is read;
//! - a **layer** (format 1) covers commits `[a, b]`; per key changed in them:
//!   presence after `b`, presence before `a` (its start), the commit and
//!   generation of its last change, its flips (the commits that added or
//!   removed it, newest first, those at or below the cut dropped) and a
//!   source's payload.
//!
//! Presence at a commit P at or after the cut is presence at H flipped once
//! per flip after P: no state at P is kept.

use std::collections::VecDeque;

use crate::delta::{Collected, Old, Write};
use crate::entries::SortedEntries;
use crate::error::{Error, Result};
use crate::rows::Source;
use crate::stream::{Bytes, State};

pub const DELTA: u8 = 0;
pub const LAYER: u8 = 1;
pub const HEADER: usize = 13;

pub const ADDED: u8 = 0;
pub const UPDATED: u8 = 1;
pub const REMOVED: u8 = 2;
const KIND: u8 = 3;
const D_PAYLOAD: u8 = 4;
const D_REPLACED: u8 = 8;

const PRESENT: u8 = 1;
const L_PAYLOAD: u8 = 2;
const FLIPS: u8 = 4;
const START: u8 = 8;

/// The largest raw block a reader accepts.
pub const MAX_RAW: usize = 64 << 20;

fn bad<T>(m: impl Into<String>) -> Result<T> {
    Err(Error::Format(m.into()))
}

// -- varints ---------------------------------------------------------------------------

fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn get_varint(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut n = 0u64;
    let mut shift = 0;
    loop {
        let Some(&b) = buf.get(*pos) else {
            return bad("truncated varint");
        };
        *pos += 1;
        if shift == 63 && b > 1 {
            return bad("varint past 64 bits"); // its tenth byte holds one bit (F23)
        }
        n |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            return Ok(n);
        }
        shift += 7;
        if shift > 63 {
            return bad("varint too long");
        }
    }
}

fn get_bytes<'a>(buf: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
    let Some(s) = buf.get(*pos..pos.saturating_add(n)) else {
        return bad("truncated block");
    };
    *pos += n;
    Ok(s)
}

fn shared(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn put_key(out: &mut Vec<u8>, prev: &[u8], key: &[u8]) {
    let s = shared(prev, key);
    put_varint(out, s as u64);
    put_varint(out, (key.len() - s) as u64);
    out.extend_from_slice(&key[s..]);
}

/// The next key of a block, into `key` (which holds the previous one).
fn next_key(raw: &[u8], pos: &mut usize, key: &mut Vec<u8>) -> Result<()> {
    let s = get_varint(raw, pos)? as usize;
    let l = get_varint(raw, pos)? as usize;
    if s > key.len() {
        return bad("shared prefix past the previous key");
    }
    key.truncate(s);
    key.extend_from_slice(get_bytes(raw, pos, l)?);
    Ok(())
}

// -- entries -------------------------------------------------------------------------------

/// A key's entry, in a layer's terms (a delta's entries read as such).
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Entry {
    pub key: Vec<u8>,
    pub present: bool,
    pub start: bool,
    pub commit: u64,
    pub generation: u64,
    pub flips: Vec<u64>,
    pub payload: Option<Vec<u8>>,
    /// A delta's: the generation its change replaced, when written.
    pub replaced: Option<u64>,
}

impl Entry {
    /// What a delta records for this entry: its change kind.
    pub fn kind(&self) -> u8 {
        match (self.present, self.start) {
            (true, false) => ADDED,
            (true, true) => UPDATED,
            (false, _) => REMOVED,
        }
    }
}

/// The commit and generation a delta's entries take (a layer's carry their own).
#[derive(Clone, Copy, Debug, Default)]
pub struct Stamp {
    pub commit: u64,
    pub generation: u64,
}

fn encode_block(format: u8, entries: &[Entry]) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint(&mut out, entries.len() as u64);
    let (cmin, gmin) = if format == LAYER {
        let c = entries.iter().map(|e| e.commit).min().unwrap_or(0);
        let g = entries.iter().map(|e| e.generation).min().unwrap_or(0);
        put_varint(&mut out, c);
        put_varint(&mut out, g);
        (c, g)
    } else {
        (0, 0)
    };
    let mut prev: &[u8] = &[];
    for e in entries {
        put_key(&mut out, prev, &e.key);
        if format == DELTA {
            let mut k = e.kind();
            if e.payload.is_some() {
                k |= D_PAYLOAD;
            }
            if e.replaced.is_some() {
                k |= D_REPLACED;
            }
            out.push(k);
            if let Some(p) = &e.payload {
                put_varint(&mut out, p.len() as u64);
                out.extend_from_slice(p);
            }
            if let Some(g) = e.replaced {
                // Compact: back from the commit's generation, which the writer gives.
                put_varint(&mut out, e.generation - g);
            }
        } else {
            let mut f = 0;
            if e.present {
                f |= PRESENT;
            }
            if e.start {
                f |= START;
            }
            if e.payload.is_some() {
                f |= L_PAYLOAD;
            }
            if !e.flips.is_empty() {
                f |= FLIPS;
            }
            out.push(f);
            put_varint(&mut out, e.commit - cmin);
            put_varint(&mut out, e.generation - gmin);
            if !e.flips.is_empty() {
                put_varint(&mut out, e.flips.len() as u64);
                let mut last = e.commit;
                for &x in &e.flips {
                    put_varint(&mut out, last - x);
                    last = x;
                }
            }
            if let Some(p) = &e.payload {
                put_varint(&mut out, p.len() as u64);
                out.extend_from_slice(p);
            }
        }
        prev = &e.key;
    }
    out
}

/// One block's raw bytes, parsed lazily: `next` reads the next key into
/// `key`, `entry` builds the entry just read (`skip` passes it).
struct BlockReader<'a> {
    format: u8,
    raw: &'a [u8],
    pos: usize,
    left: usize,
    cmin: u64,
    gmin: u64,
    stamp: Stamp,
    key: Vec<u8>,
}

impl<'a> BlockReader<'a> {
    fn new(format: u8, raw: &'a [u8], stamp: Stamp) -> Result<Self> {
        let mut pos = 0;
        let left = get_varint(raw, &mut pos)? as usize;
        let (cmin, gmin) = match format {
            LAYER => (get_varint(raw, &mut pos)?, get_varint(raw, &mut pos)?),
            DELTA => (0, 0),
            f => return bad(format!("unknown block format {f}")),
        };
        Ok(BlockReader {
            format,
            raw,
            pos,
            left,
            cmin,
            gmin,
            stamp,
            key: Vec::new(),
        })
    }

    fn next_key(&mut self) -> Result<bool> {
        if self.left == 0 {
            if self.pos != self.raw.len() {
                return bad("trailing bytes in a block");
            }
            return Ok(false);
        }
        self.left -= 1;
        next_key(self.raw, &mut self.pos, &mut self.key)?;
        Ok(true)
    }

    /// The fields of the entry whose key was just read, as an entry (with
    /// the key) when `build`, or skipped.
    fn fields(&mut self, build: bool) -> Result<Option<Entry>> {
        let raw = self.raw;
        let pos = &mut self.pos;
        let Some(&f) = raw.get(*pos) else {
            return bad("truncated block");
        };
        *pos += 1;
        if self.format == DELTA {
            let kind = f & KIND;
            if kind > REMOVED {
                return bad(format!("unknown change kind {kind}"));
            }
            let payload = if f & D_PAYLOAD != 0 {
                let l = get_varint(raw, pos)? as usize;
                Some(get_bytes(raw, pos, l)?)
            } else {
                None
            };
            let replaced = if f & D_REPLACED != 0 {
                let back = get_varint(raw, pos)?;
                match self.stamp.generation.checked_sub(back) {
                    Some(g) => Some(g),
                    None => return bad("a replaced generation before 0"),
                }
            } else {
                None
            };
            if !build {
                return Ok(None);
            }
            let s = self.stamp;
            return Ok(Some(Entry {
                key: self.key.clone(),
                present: kind != REMOVED,
                start: kind != ADDED,
                commit: s.commit,
                generation: s.generation,
                flips: if kind == UPDATED {
                    vec![]
                } else {
                    vec![s.commit]
                },
                payload: payload.map(|p| p.to_vec()),
                replaced,
            }));
        }
        let commit = self.cmin + get_varint(raw, pos)?;
        let generation = self.gmin + get_varint(raw, pos)?;
        let mut flips = Vec::new();
        if f & FLIPS != 0 {
            let m = get_varint(raw, pos)? as usize;
            let mut last = commit;
            for _ in 0..m {
                let gap = get_varint(raw, pos)?;
                let Some(x) = last.checked_sub(gap) else {
                    return bad("a flip before commit 0");
                };
                last = x;
                if build {
                    flips.push(x);
                }
            }
        }
        let payload = if f & L_PAYLOAD != 0 {
            let l = get_varint(raw, pos)? as usize;
            Some(get_bytes(raw, pos, l)?)
        } else {
            None
        };
        if !build {
            return Ok(None);
        }
        Ok(Some(Entry {
            key: self.key.clone(),
            present: f & PRESENT != 0,
            start: f & START != 0,
            commit,
            generation,
            flips,
            payload: payload.map(|p| p.to_vec()),
            replaced: None,
        }))
    }
}

/// The block at `pos` of `buf`: (format, raw bytes, the next block's position).
pub fn read_block(buf: &[u8], pos: usize) -> Result<(u8, Vec<u8>, usize)> {
    let Some(h) = buf.get(pos..pos + HEADER) else {
        return bad("truncated block header");
    };
    let clen = u32::from_le_bytes(h[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(h[4..8].try_into().unwrap());
    let rlen = u32::from_le_bytes(h[8..12].try_into().unwrap()) as usize;
    let format = h[12];
    if rlen > MAX_RAW {
        return bad(format!("a block of {rlen} raw bytes"));
    }
    let Some(data) = buf.get(pos + HEADER..pos + HEADER + clen) else {
        return bad("truncated block");
    };
    if crc32fast::hash(data) != crc {
        return bad(format!("block checksum mismatch at offset {pos}"));
    }
    let raw =
        zstd::bulk::decompress(data, rlen).map_err(|e| Error::Format(format!("zstd: {e}")))?;
    if raw.len() != rlen {
        return bad("block raw length mismatch");
    }
    Ok((format, raw, pos + HEADER + clen))
}

/// Every entry of a file's blocks (a delta read at `stamp`).
pub fn decode(data: &[u8], stamp: Stamp) -> Result<Vec<Entry>> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        let (format, raw, next) = read_block(data, pos)?;
        pos = next;
        let mut r = BlockReader::new(format, &raw, stamp)?;
        while r.next_key()? {
            out.push(r.fields(true)?.unwrap());
        }
    }
    Ok(out)
}

/// A write's sorted entries as one delta file — upserts as updated, with
/// their payloads, removes as removed — the resolver's transport form.
pub fn encode_run(run: &SortedEntries, block_size: usize, level: i32) -> Result<Vec<u8>> {
    let mut w = BlockWriter::new(DELTA, block_size, level, usize::MAX);
    for i in 0..run.len() {
        let removed = run.deleted[i];
        w.push(Entry {
            key: run.key(i).to_vec(),
            present: !removed,
            start: true,
            commit: 0,
            generation: 0,
            flips: if removed { vec![0] } else { vec![] },
            payload: if removed {
                None
            } else {
                run.payload(i).map(<[u8]>::to_vec)
            },
            replaced: None,
        })?;
    }
    w.finish()?;
    Ok(w.ready.pop_front().map(|f| f.data).unwrap_or_default())
}

/// Sorted entries back from their transport form, every block checked (its
/// CRC, its raw length, its format, keys strictly increasing), decoding at
/// most `max_entries` entries and `max_bytes` bytes (raw, then keys and
/// payloads): past either, an `Error::Limit`, whatever the file claims.
pub fn decode_run(data: &[u8], max_entries: u64, max_bytes: u64) -> Result<SortedEntries> {
    let mut out = SortedEntries::default();
    let mut budget = max_bytes;
    let mut take = |n: usize| -> Result<()> {
        budget = budget
            .checked_sub(n as u64)
            .ok_or_else(|| Error::Limit(format!("more than {max_bytes} bytes decoded")))?;
        Ok(())
    };
    let mut pos = 0;
    while pos < data.len() {
        let Some(h) = data.get(pos..pos + HEADER) else {
            return bad("truncated block header");
        };
        take(u32::from_le_bytes(h[8..12].try_into().unwrap()) as usize)?;
        let (format, raw, next) = read_block(data, pos)?;
        if format != DELTA {
            return bad("a run's block is not a delta's");
        }
        pos = next;
        let mut r = BlockReader::new(format, &raw, Stamp::default())?;
        if out.len() as u64 + r.left as u64 > max_entries {
            return Err(Error::Limit(format!("more than {max_entries} entries")));
        }
        while r.next_key()? {
            let e = r.fields(true)?.unwrap();
            take(e.key.len() + e.payload.as_ref().map_or(0, Vec::len))?;
            out.push(&e.key, 0, !e.present, e.payload.as_deref())
                .map_err(|_| Error::Format("keys out of order".into()))?;
        }
    }
    Ok(out.shrink())
}

// -- the writer --------------------------------------------------------------------------------

/// Where a block is, and what it holds.
#[derive(Clone, Debug, PartialEq)]
pub struct BlockInfo {
    pub file: u32,
    pub offset: u64,
    pub length: u32,
    pub first: Vec<u8>,
    pub entries: u32,
    /// The newest commit a block's entries changed at (0 for a delta's).
    pub newest: u64,
}

/// A file of a part, as written: its bytes, entries, first and last keys.
pub struct FileOut {
    pub data: Vec<u8>,
    pub entries: u64,
    pub first: Vec<u8>,
    pub last: Vec<u8>,
}

/// Entries in key order as blocks of about `block_size` raw bytes, in files
/// of about `file_limit` bytes. Full files go to `ready` as they close;
/// `blocks` is every block written, for the part's index.
pub struct BlockWriter {
    format: u8,
    block_size: usize,
    level: i32,
    file_limit: usize,
    pending: Vec<Entry>,
    pending_raw: usize,
    file: Option<FileOut>,
    files: u32,
    pub ready: VecDeque<FileOut>,
    pub blocks: Vec<BlockInfo>,
    last: Vec<u8>,
    pub entries: u64,
}

impl BlockWriter {
    pub fn new(format: u8, block_size: usize, level: i32, file_limit: usize) -> BlockWriter {
        BlockWriter {
            format,
            block_size: block_size.max(256),
            level,
            file_limit: file_limit.max(1),
            pending: Vec::new(),
            pending_raw: 0,
            file: None,
            files: 0,
            ready: VecDeque::new(),
            blocks: Vec::new(),
            last: Vec::new(),
            entries: 0,
        }
    }

    pub fn push(&mut self, e: Entry) -> Result<()> {
        if self.entries > 0 && e.key.as_slice() <= self.last.as_slice() {
            return Err(Error::Value("keys must be sorted and unique".into()));
        }
        if self.format == DELTA && e.present && e.flips.len() > 1 {
            return Err(Error::Value("a delta entry changes its key once".into()));
        }
        self.last.clear();
        self.last.extend_from_slice(&e.key);
        self.pending_raw +=
            e.key.len() + 8 + 3 * e.flips.len() + e.payload.as_ref().map_or(0, |p| p.len() + 2);
        self.pending.push(e);
        self.entries += 1;
        if self.pending_raw >= self.block_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let raw = encode_block(self.format, &self.pending);
        let data = zstd::bulk::compress(&raw, self.level)
            .map_err(|e| Error::Format(format!("zstd: {e}")))?;
        if self
            .file
            .as_ref()
            .is_some_and(|f| f.data.len() >= self.file_limit)
        {
            self.cut();
        }
        let first = self.pending[0].key.clone();
        let last = self.pending.last().unwrap().key.clone();
        let file = self.file.get_or_insert_with(|| FileOut {
            data: Vec::new(),
            entries: 0,
            first: first.clone(),
            last: vec![],
        });
        let offset = file.data.len() as u64;
        file.data
            .extend_from_slice(&(data.len() as u32).to_le_bytes());
        file.data
            .extend_from_slice(&crc32fast::hash(&data).to_le_bytes());
        file.data
            .extend_from_slice(&(raw.len() as u32).to_le_bytes());
        file.data.push(self.format);
        file.data.extend_from_slice(&data);
        file.entries += self.pending.len() as u64;
        file.last = last;
        self.blocks.push(BlockInfo {
            file: self.files,
            offset,
            length: (HEADER + data.len()) as u32,
            first,
            entries: self.pending.len() as u32,
            newest: if self.format == LAYER {
                self.pending.iter().map(|e| e.commit).max().unwrap_or(0)
            } else {
                0
            },
        });
        self.pending.clear();
        self.pending_raw = 0;
        Ok(())
    }

    fn cut(&mut self) {
        if let Some(f) = self.file.take() {
            self.ready.push_back(f);
            self.files += 1;
        }
    }

    /// The last block and file go out.
    pub fn finish(&mut self) -> Result<()> {
        self.flush()?;
        self.cut();
        Ok(())
    }
}

/// A part's index: per block its file, offset, length, first key, entries
/// and newest commit.
pub fn index_encode(blocks: &[BlockInfo]) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint(&mut out, blocks.len() as u64);
    let mut prev: &[u8] = &[];
    for b in blocks {
        put_varint(&mut out, b.file as u64);
        put_varint(&mut out, b.offset);
        put_varint(&mut out, b.length as u64);
        put_key(&mut out, prev, &b.first);
        put_varint(&mut out, b.entries as u64);
        put_varint(&mut out, b.newest);
        prev = &b.first;
    }
    out
}

pub fn index_decode(data: &[u8]) -> Result<Vec<BlockInfo>> {
    let mut pos = 0;
    let n = get_varint(data, &mut pos)? as usize;
    let mut out = Vec::with_capacity(n.min(1 << 20));
    let mut key = Vec::new();
    for _ in 0..n {
        let file = get_varint(data, &mut pos)? as u32;
        let offset = get_varint(data, &mut pos)?;
        let length = get_varint(data, &mut pos)? as u32;
        next_key(data, &mut pos, &mut key)?;
        let entries = get_varint(data, &mut pos)? as u32;
        let newest = get_varint(data, &mut pos)?;
        out.push(BlockInfo {
            file,
            offset,
            length,
            first: key.clone(),
            entries,
            newest,
        });
    }
    if pos != data.len() {
        return bad("trailing bytes in a layer index");
    }
    Ok(out)
}

// -- inputs ---------------------------------------------------------------------------------

/// A glob in Solera's grammar (`solera/patterns.py`): `**/` any run of path
/// segments, or none; `**` anything; `*` anything but `/`; `?` one byte but
/// `/`; anything else literal. Matched over bytes, with a quick rejection of
/// keys that lack its longest literal run.
pub struct Glob {
    toks: Vec<Tok>,
    need: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
enum Tok {
    Lit(u8),
    One,
    Star,
    DStar,
    SegStart,
    SegLoop,
}

impl Glob {
    pub fn new(g: &[u8]) -> Result<Glob> {
        let mut toks = Vec::new();
        let mut i = 0;
        while i < g.len() {
            if g[i..].starts_with(b"**/") {
                toks.push(Tok::SegStart);
                toks.push(Tok::SegLoop);
                i += 3;
            } else if g[i..].starts_with(b"**") {
                toks.push(Tok::DStar);
                i += 2;
            } else if g[i] == b'*' {
                toks.push(Tok::Star);
                i += 1;
            } else if g[i] == b'?' {
                toks.push(Tok::One);
                i += 1;
            } else {
                toks.push(Tok::Lit(g[i]));
                i += 1;
            }
        }
        if toks.len() >= 64 {
            return Err(Error::Value("a glob of more than 63 parts".into()));
        }
        let (mut best, mut cur) = (Vec::new(), Vec::new());
        for t in &toks {
            if let Tok::Lit(b) = t {
                cur.push(*b);
            } else {
                if cur.len() > best.len() {
                    best = std::mem::take(&mut cur);
                }
                cur.clear();
            }
        }
        if cur.len() > best.len() {
            best = cur;
        }
        Ok(Glob { toks, need: best })
    }

    fn close(&self, mut v: u64) -> u64 {
        for (i, t) in self.toks.iter().enumerate() {
            if v >> i & 1 == 1 {
                match t {
                    Tok::Star | Tok::DStar => v |= 1 << (i + 1),
                    Tok::SegStart => v |= 0b11 << (i + 1),
                    _ => {}
                }
            }
        }
        v
    }

    fn start(&self) -> u64 {
        self.close(1)
    }

    fn step(&self, cur: u64, c: u8) -> u64 {
        let n = self.toks.len();
        let mut nxt = 0u64;
        let mut live = cur;
        while live != 0 {
            let i = live.trailing_zeros() as usize;
            live &= live - 1;
            if i == n {
                continue;
            }
            match self.toks[i] {
                Tok::Lit(b) if b == c => nxt |= 1 << (i + 1),
                Tok::One if c != b'/' => nxt |= 1 << (i + 1),
                Tok::Star if c != b'/' => nxt |= 1 << i,
                Tok::DStar => nxt |= 1 << i,
                Tok::SegLoop => {
                    nxt |= 1 << i;
                    if c == b'/' {
                        nxt |= 1 << (i + 1);
                    }
                }
                _ => {}
            }
        }
        self.close(nxt)
    }

    fn accepts(&self, states: u64) -> bool {
        states >> self.toks.len() & 1 == 1
    }

    pub fn matches(&self, key: &[u8]) -> bool {
        if let Some(&first) = self.need.first() {
            let n = self.need.len();
            if key.len() < n
                || !(0..=key.len() - n).any(|i| key[i] == first && key[i..i + n] == self.need[..])
            {
                return false;
            }
        }
        let mut cur = self.start();
        for &c in key {
            cur = self.step(cur, c);
            if cur == 0 {
                return false;
            }
        }
        self.accepts(cur)
    }

    /// Whether some key `k` with `lo <= k <= hi` (`hi` None: no bound)
    /// matches: what a reader asks of a block from its index alone, before
    /// fetching it. A walk down the byte strings between the bounds, tight on
    /// either, that accepts a complete match wherever it lies inside both,
    /// and stops as soon as it is free of both: every live state of this
    /// automaton can still reach a match (A25 R1, R2: a short match between
    /// longer bounds, a terminal `**`).
    pub fn may_match_between(&self, lo: &[u8], hi: Option<&[u8]>) -> bool {
        let lits: Vec<u8> = {
            let mut v: Vec<u8> = self
                .toks
                .iter()
                .filter_map(|t| if let Tok::Lit(b) = t { Some(*b) } else { None })
                .collect();
            v.push(b'/');
            v.sort_unstable();
            v.dedup();
            v
        };
        self.walk(
            0,
            self.start(),
            lo,
            hi.unwrap_or(&[]),
            true,
            hi.is_some(),
            &lits,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn walk(
        &self,
        i: usize,
        states: u64,
        lo: &[u8],
        hi: &[u8],
        tlo: bool,
        thi: bool,
        lits: &[u8],
    ) -> bool {
        if states == 0 {
            return false;
        }
        // The string read so far, if it ends here: at least lo unless a
        // proper prefix of it; at most hi always.
        if self.accepts(states) && !(tlo && i < lo.len()) {
            return true;
        }
        let tlo = tlo && i < lo.len();
        if !tlo && !thi {
            return true;
        }
        if thi && i >= hi.len() {
            return false; // equal to hi: anything longer is past it
        }
        let low = if tlo { lo[i] } else { 0 };
        let high = if thi { hi[i] } else { 255 };
        let mut cands: Vec<u8> = vec![low, high];
        cands.extend(lits.iter().copied().filter(|&c| low <= c && c <= high));
        // One byte strictly inside both bounds and no literal: it frees the
        // walk of both bounds at once.
        if let Some(x) = (low.saturating_add(1)..high).find(|x| !lits.contains(x)) {
            cands.push(x);
        }
        cands.sort_unstable();
        cands.dedup();
        cands.into_iter().any(|x| {
            low <= x
                && x <= high
                && self.walk(
                    i + 1,
                    self.step(states, x),
                    lo,
                    hi,
                    tlo && x == low,
                    thi && x == high,
                    lits,
                )
        })
    }
}

/// What a stream's `next` found.
pub enum Next {
    Entry(Entry),
    /// It needs its next segment (`feed`), or its end (`end`).
    Starved,
    Done,
}

/// One input in key order — a part's files, or a delta's — fed segments of
/// whole blocks. Only matching keys are built when it has a glob.
pub struct LayerStream {
    segments: VecDeque<Bytes>,
    pos: usize,
    ended: bool,
    stamp: Stamp,
    buf: VecDeque<Entry>,
    last: Option<Vec<u8>>,
}

impl LayerStream {
    pub fn new(stamp: Stamp) -> LayerStream {
        LayerStream {
            segments: VecDeque::new(),
            pos: 0,
            ended: false,
            stamp,
            buf: VecDeque::new(),
            last: None,
        }
    }

    /// A stream over held bytes, ended.
    pub fn of(chunks: Vec<Bytes>, stamp: Stamp) -> LayerStream {
        let mut s = LayerStream::new(stamp);
        s.segments = chunks.into();
        s.ended = true;
        s
    }

    pub fn feed(&mut self, data: Bytes) {
        self.segments.push_back(data);
    }

    pub fn end(&mut self) {
        self.ended = true;
    }

    pub fn next(&mut self, glob: Option<&Glob>) -> Result<Next> {
        loop {
            if let Some(e) = self.buf.pop_front() {
                if self
                    .last
                    .as_ref()
                    .is_some_and(|l| e.key.as_slice() <= l.as_slice())
                {
                    return bad("an input's keys are not sorted");
                }
                self.last = Some(e.key.clone());
                return Ok(Next::Entry(e));
            }
            let Some(seg) = self.segments.front() else {
                return Ok(if self.ended {
                    Next::Done
                } else {
                    Next::Starved
                });
            };
            let seg: &[u8] = (**seg).as_ref();
            if self.pos >= seg.len() {
                self.segments.pop_front();
                self.pos = 0;
                continue;
            }
            let (format, raw, next) = read_block(seg, self.pos)?;
            self.pos = next;
            let mut r = BlockReader::new(format, &raw, self.stamp)?;
            while r.next_key()? {
                let take = glob.is_none_or(|g| g.matches(&r.key));
                if let Some(e) = r.fields(take)? {
                    self.buf.push_back(e);
                }
            }
        }
    }
}

/// A k-way merge of inputs by key: per key, every input's entry, by rank
/// (the order the inputs were given in). Inputs are few (the layers a read
/// or a merge spans): the smallest head is found by a scan.
pub struct Merger {
    pub inputs: Vec<LayerStream>,
    heads: Vec<Option<Entry>>,
    done: Vec<bool>,
    glob: Option<Glob>,
}

pub enum Group {
    /// Every entry for the next key, by rank ascending.
    Entries(Vec<(usize, Entry)>),
    Starved(usize),
    Done,
}

impl Merger {
    pub fn new(inputs: Vec<LayerStream>, glob: Option<Glob>) -> Merger {
        let n = inputs.len();
        Merger {
            inputs,
            heads: vec![None; n],
            done: vec![false; n],
            glob,
        }
    }

    pub fn next_group(&mut self) -> Result<Group> {
        for r in 0..self.inputs.len() {
            if self.heads[r].is_none() && !self.done[r] {
                match self.inputs[r].next(self.glob.as_ref())? {
                    Next::Entry(e) => self.heads[r] = Some(e),
                    Next::Starved => return Ok(Group::Starved(r)),
                    Next::Done => self.done[r] = true,
                }
            }
        }
        let mut min: Option<&[u8]> = None;
        for e in self.heads.iter().flatten() {
            if min.is_none_or(|m| e.key.as_slice() < m) {
                min = Some(&e.key);
            }
        }
        let Some(min) = min.map(|m| m.to_vec()) else {
            return Ok(Group::Done);
        };
        let mut group = Vec::new();
        for r in 0..self.heads.len() {
            if self.heads[r].as_ref().is_some_and(|e| e.key == min) {
                group.push((r, self.heads[r].take().unwrap()));
            }
        }
        Ok(Group::Entries(group))
    }
}

/// The newest state of a key from its group (inputs newest first): what the
/// index holds for it at the head.
fn old_of(group: &[(usize, Entry)]) -> Old<'_> {
    let e = &group[0].1;
    if e.present {
        Old::Live(e.generation, e.payload.as_deref())
    } else {
        Old::Absent
    }
}

// -- the delta writer ----------------------------------------------------------------------------

/// A commit's delta being written: the one rule per key (`delta.rs`), the
/// count's change, and — when asked — each change's replaced generation.
pub struct DeltaWriter {
    pub out: BlockWriter,
    pub added: u64,
    pub removed: u64,
    pub changed: u64,
    /// The keys it changed, written and removed, up to a limit: what a store
    /// is told to write and delete.
    pub collected: Collected,
    /// The commit's generation when each change records the generation it
    /// replaced (written as the distance back from it), else None.
    replaced: Option<u64>,
}

impl DeltaWriter {
    pub fn new(
        block_size: usize,
        level: i32,
        file_limit: usize,
        replaced: Option<u64>,
        collect: usize,
    ) -> DeltaWriter {
        DeltaWriter {
            out: BlockWriter::new(DELTA, block_size, level, file_limit),
            added: 0,
            removed: 0,
            changed: 0,
            collected: Collected::new(collect),
            replaced,
        }
    }

    /// A write of `key` over what the index holds. Keys come strictly increasing.
    pub fn apply(&mut self, key: &[u8], new: Write, old: Old) -> Result<()> {
        let (start, before) = match old {
            Old::Live(g, p) => (true, Some((g, p))),
            Old::Absent => (false, None),
        };
        let (present, payload) = match (new, old) {
            (Write::Remove, Old::Absent) => return Ok(()),
            (Write::Remove, _) => {
                self.removed += 1;
                (false, None)
            }
            (Write::Upsert(Some(p)), Old::Live(_, Some(was))) if was == p => return Ok(()),
            (Write::Upsert(p), Old::Absent) => {
                self.added += 1;
                (true, p)
            }
            (Write::Upsert(p), _) => {
                self.changed += 1;
                (true, p)
            }
        };
        self.collected.add(key, !present);
        self.out.push(Entry {
            key: key.to_vec(),
            present,
            start,
            commit: 0,
            generation: self.replaced.unwrap_or(0),
            flips: if present && start { vec![] } else { vec![0] },
            payload: payload.map(|p| p.to_vec()),
            replaced: match (self.replaced, before) {
                (Some(at), Some((g, _))) if g > at => {
                    return Err(Error::Value(format!(
                        "a replaced generation {g} after the commit's {at}"
                    )))
                }
                (Some(_), Some((g, _))) => Some(g),
                _ => None,
            },
        })
    }
}

/// Resolve a commit's sorted written entries against the index's inputs
/// (newest first, held), writing its delta: the sparse path, where the inputs
/// are the blocks the keys fall in (or whole parts).
pub fn resolve(
    inputs: Vec<LayerStream>,
    written: &SortedEntries,
    w: &mut DeltaWriter,
) -> Result<()> {
    let keys: Vec<&[u8]> = (0..written.len()).map(|i| written.key(i)).collect();
    let found = lookup(inputs, &keys)?;
    for (i, f) in found.iter().enumerate() {
        let old = match f {
            Some(e) if e.present => Old::Live(e.generation, e.payload.as_deref()),
            _ => Old::Absent,
        };
        w.apply(written.key(i), written.write(i), old)?;
    }
    w.out.finish()
}

// -- lookups, Δ -----------------------------------------------------------------------------------

/// Per key (sorted, unique), the newest entry among the inputs (newest
/// first), or None. Blocks are walked without building entries but the
/// matches.
pub fn lookup(inputs: Vec<LayerStream>, keys: &[&[u8]]) -> Result<Vec<Option<Entry>>> {
    if keys.windows(2).any(|w| w[0] >= w[1]) {
        return Err(Error::Value("lookup keys must be sorted and unique".into()));
    }
    let mut out: Vec<Option<Entry>> = vec![None; keys.len()];
    for s in inputs {
        let stamp = s.stamp;
        let mut i = 0;
        'segments: for seg in s.segments.iter() {
            let seg: &[u8] = (**seg).as_ref();
            let mut pos = 0;
            while pos < seg.len() {
                if i == keys.len() {
                    break 'segments;
                }
                let (format, raw, next) = read_block(seg, pos)?;
                pos = next;
                let mut r = BlockReader::new(format, &raw, stamp)?;
                while r.next_key()? {
                    while i < keys.len() && keys[i] < r.key.as_slice() {
                        i += 1;
                    }
                    let hit = i < keys.len() && keys[i] == r.key.as_slice() && out[i].is_none();
                    let e = r.fields(hit)?;
                    if hit {
                        out[i] = e;
                    }
                    if i == keys.len() {
                        break;
                    }
                }
            }
        }
    }
    Ok(out)
}

/// One key's difference between P and H.
#[derive(Debug, PartialEq)]
pub struct Diff {
    pub key: Vec<u8>,
    pub before: bool,
    pub after: bool,
    pub generation: u64,
    pub payload: Option<Vec<u8>>,
}

/// Δ(P, H): over the inputs (newest first) that end after P — P None is −∞ —
/// every key in (`after`, `upto`] matching `glob` (and among `keys`, when
/// given: a sorted key list is the same read) whose state differs: an
/// entry changed after P, its presence at H the newest's, at P that flipped
/// once per flip after P; absent at both ends, it is left out. Stops after
/// `limit` and returns the last key delivered; None when it reached `upto`
/// or the inputs' end. Starved inputs are an error: every input is held.
#[allow(clippy::too_many_arguments)]
pub fn scan(
    inputs: Vec<LayerStream>,
    p: Option<u64>,
    after: Option<&[u8]>,
    upto: Option<&[u8]>,
    limit: usize,
    glob: Option<Glob>,
    keys: Option<&[&[u8]]>,
    out: &mut Vec<Diff>,
) -> Result<Option<Vec<u8>>> {
    if keys.is_some_and(|k| k.windows(2).any(|w| w[0] >= w[1])) {
        return Err(Error::Value("keys must be sorted and unique".into()));
    }
    let mut m = Merger::new(inputs, glob);
    let mut ki = 0;
    loop {
        let group = match m.next_group()? {
            Group::Entries(g) => g,
            Group::Done => return Ok(None),
            Group::Starved(_) => {
                return Err(Error::Value("a scan's inputs must be held whole".into()))
            }
        };
        let key = &group[0].1.key;
        if after.is_some_and(|a| key.as_slice() <= a) {
            continue;
        }
        if upto.is_some_and(|u| key.as_slice() > u) {
            return Ok(None);
        }
        if let Some(keys) = keys {
            while ki < keys.len() && keys[ki] < key.as_slice() {
                ki += 1;
            }
            if ki == keys.len() {
                return Ok(None);
            }
            if keys[ki] != key.as_slice() {
                continue;
            }
        }
        let mut newest: Option<usize> = None;
        let mut odd = false;
        for (j, (_, e)) in group.iter().enumerate() {
            if p.is_some_and(|p| e.commit <= p) {
                continue;
            }
            if newest.is_none() {
                newest = Some(j);
            }
            if let Some(p) = p {
                odd ^= e.flips.iter().filter(|&&f| f > p).count() % 2 == 1;
            }
        }
        let Some(j) = newest else { continue };
        let mut group = group;
        let e = std::mem::take(&mut group[j].1);
        let before = p.is_some() && (e.present ^ odd);
        if !before && !e.present {
            continue;
        }
        let present = e.present;
        out.push(Diff {
            key: e.key,
            before,
            after: present,
            generation: e.generation,
            payload: if present { e.payload } else { None },
        });
        if out.len() >= limit {
            return Ok(Some(out.last().unwrap().key.clone()));
        }
    }
}

// -- jobs ------------------------------------------------------------------------------------------

pub enum Step {
    /// Input `r` needs its next segment, or its end.
    Run(usize),
    /// The written rows need their next sorted chunk, or their end.
    Rows,
    /// A file is ready.
    File,
    Done,
}

/// Merging adjacent layers (inputs oldest first): per key, the newest state,
/// the oldest's start, every flip after the cut. Entries go to one of two
/// parts — **main**, what head reads and readers after the layer need:
/// present keys and (above the base) keys present at the start, which shadow
/// older layers; **side**, what only a reader whose P falls inside the layer
/// needs: in the base (`bottom`) every absent key, elsewhere keys absent at
/// both ends. An absent key with no flip after the cut serves no reader, and
/// shadows nothing when it was absent at the start or is in the base: it goes.
pub struct LayerMerge {
    pub merger: Merger,
    pub main: BlockWriter,
    pub side: BlockWriter,
    /// Flips at or below it go (None: no cut yet, every flip stays).
    cut: Option<u64>,
    bottom: bool,
    /// Every input kept all its flips (none at or below a cut): each key's
    /// presence must then equal its start flipped once per flip.
    check: bool,
    pub read: u64,
    pub dropped: u64,
    done: bool,
}

impl LayerMerge {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        inputs: Vec<LayerStream>,
        cut: Option<u64>,
        bottom: bool,
        check: bool,
        block_size: usize,
        level: i32,
        file_limit: usize,
    ) -> LayerMerge {
        LayerMerge {
            merger: Merger::new(inputs, None),
            main: BlockWriter::new(LAYER, block_size, level, file_limit),
            side: BlockWriter::new(LAYER, block_size, level, file_limit),
            cut,
            bottom,
            check,
            read: 0,
            dropped: 0,
            done: false,
        }
    }

    pub fn step(&mut self) -> Result<Step> {
        loop {
            if !self.main.ready.is_empty() || !self.side.ready.is_empty() {
                return Ok(Step::File);
            }
            if self.done {
                return Ok(Step::Done);
            }
            let mut group = match self.merger.next_group()? {
                Group::Starved(r) => return Ok(Step::Run(r)),
                Group::Done => {
                    self.main.finish()?;
                    self.side.finish()?;
                    self.done = true;
                    continue;
                }
                Group::Entries(g) => g,
            };
            self.read += group.len() as u64;
            if self.check {
                // Presence is the start flipped once per add or remove: a
                // wrong change kind in any input breaks it (R4).
                let flips: usize = group.iter().map(|(_, e)| e.flips.len()).sum();
                let (first, last) = (&group[0].1, &group[group.len() - 1].1);
                if last.present != (first.start ^ (flips % 2 == 1)) {
                    return Err(Error::Format(format!(
                        "key {:?}: presence {} after start {} and {flips} flips",
                        String::from_utf8_lossy(&first.key),
                        last.present,
                        first.start
                    )));
                }
            }
            let cut = self.cut;
            let mut flips: Vec<u64> = group
                .iter()
                .flat_map(|(_, e)| e.flips.iter().copied())
                .filter(|&f| cut.is_none_or(|c| f > c))
                .collect();
            flips.sort_unstable_by(|a, b| b.cmp(a));
            flips.dedup();
            let start = group[0].1.start; // the oldest input first
            let (_, newest) = group.pop().unwrap(); // the newest last
            let e = Entry {
                flips,
                start,
                replaced: None,
                ..newest
            };
            if e.present || (e.start && !self.bottom) {
                self.main.push(e)?;
            } else if e.flips.is_empty() {
                self.dropped += 1;
            } else {
                self.side.push(e)?;
            }
        }
    }
}

/// A streamed write: written rows (sorted chunks) or sorted entries,
/// merge-joined with the index's inputs (newest first, fed by segments),
/// writing the delta. A patch passes over keys it does not write and stops
/// reading the index once its writes are done; a replacement (`replace`) is
/// the whole new content, and present keys it omits are removed.
pub struct LayerJoin {
    pub src: Source,
    pub merger: Merger,
    pub delta: DeltaWriter,
    replace: bool,
    old: Option<Vec<(usize, Entry)>>,
    old_done: bool,
    done: bool,
}

impl LayerJoin {
    pub fn new(
        src: Source,
        inputs: Vec<LayerStream>,
        replace: bool,
        delta: DeltaWriter,
    ) -> Result<LayerJoin> {
        if replace && matches!(&src, Source::Entries(sorted, _) if sorted.removes() > 0) {
            return Err(Error::Value("a replacement has no removes".into()));
        }
        Ok(LayerJoin {
            src,
            merger: Merger::new(inputs, None),
            delta,
            replace,
            old: None,
            old_done: false,
            done: false,
        })
    }

    pub fn step(&mut self) -> Result<Step> {
        loop {
            if !self.delta.out.ready.is_empty() {
                return Ok(Step::File);
            }
            if self.done {
                return Ok(Step::Done);
            }
            let new = match self.src.state()? {
                State::Starved => return Ok(Step::Rows),
                s => s == State::Ready,
            };
            if !new && !self.replace {
                self.delta.out.finish()?; // a patch is done with its writes
                self.done = true;
                continue;
            }
            while self.old.is_none() && !self.old_done {
                match self.merger.next_group()? {
                    Group::Starved(r) => return Ok(Step::Run(r)),
                    Group::Done => self.old_done = true,
                    Group::Entries(g) => {
                        if g[0].1.present {
                            self.old = Some(g);
                        }
                    }
                }
            }
            let order = match (&self.old, new) {
                (None, false) => {
                    self.delta.out.finish()?;
                    self.done = true;
                    continue;
                }
                (None, true) => std::cmp::Ordering::Less,
                (Some(_), false) => std::cmp::Ordering::Greater,
                (Some(g), true) => self.src.key().cmp(g[0].1.key.as_slice()),
            };
            match order {
                std::cmp::Ordering::Less => {
                    let key = self.src.key().to_vec();
                    self.delta.apply(&key, self.src.write(), Old::Absent)?;
                    self.src.advance();
                }
                std::cmp::Ordering::Equal => {
                    let g = self.old.take().unwrap();
                    let key = self.src.key().to_vec();
                    self.delta.apply(&key, self.src.write(), old_of(&g))?;
                    self.src.advance();
                }
                std::cmp::Ordering::Greater => {
                    let g = self.old.take().unwrap();
                    if self.replace {
                        let key = g[0].1.key.clone();
                        self.delta.apply(&key, Write::Remove, old_of(&g))?;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn e(k: &str, present: bool, start: bool, commit: u64, flips: &[u64]) -> Entry {
        Entry {
            key: k.into(),
            present,
            start,
            commit,
            generation: commit * 10,
            flips: flips.to_vec(),
            ..Default::default()
        }
    }

    fn bytes(v: Vec<u8>) -> Bytes {
        Arc::new(v)
    }

    fn write(format: u8, entries: &[Entry]) -> (Vec<u8>, Vec<BlockInfo>) {
        let mut w = BlockWriter::new(format, 64, 1, 1 << 20);
        for x in entries {
            w.push(x.clone()).unwrap();
        }
        w.finish().unwrap();
        let data = w.ready.into_iter().flat_map(|f| f.data).collect();
        (data, w.blocks)
    }

    fn stream(data: &[u8], stamp: Stamp) -> LayerStream {
        LayerStream::of(vec![bytes(data.to_vec())], stamp)
    }

    fn merged(
        inputs: Vec<LayerStream>,
        cut: Option<u64>,
        bottom: bool,
    ) -> (Vec<Entry>, Vec<Entry>, u64) {
        let mut m = LayerMerge::new(inputs, cut, bottom, false, 64, 1, 1 << 20);
        let (mut main, mut side) = (vec![], vec![]);
        loop {
            match m.step().unwrap() {
                Step::File => {
                    while let Some(f) = m.main.ready.pop_front() {
                        main.extend(f.data)
                    }
                    while let Some(f) = m.side.ready.pop_front() {
                        side.extend(f.data)
                    }
                }
                Step::Done => break,
                _ => unreachable!(),
            }
        }
        (
            decode(&main, Stamp::default()).unwrap(),
            decode(&side, Stamp::default()).unwrap(),
            m.dropped,
        )
    }

    #[test]
    fn a_varint_past_64_bits_is_refused() {
        let mut max = Vec::new();
        put_varint(&mut max, u64::MAX);
        assert_eq!(get_varint(&max, &mut 0).unwrap(), u64::MAX);
        // F23: a tenth byte above 0x01 is refused, never read with its high bits dropped.
        let past = [[0xff; 9].as_slice(), &[0x7f]].concat();
        assert!(get_varint(&past, &mut 0).is_err());
        let long = [0xff; 11];
        assert!(get_varint(&long, &mut 0).is_err());
    }

    #[test]
    fn a_block_claiming_more_than_max_raw_is_refused_before_inflating() {
        // F29: a header's raw length bounds what its block inflates to, and past
        // MAX_RAW it is refused before anything is decompressed.
        let mut block = vec![0u8; HEADER];
        block[8..12].copy_from_slice(&((MAX_RAW + 1) as u32).to_le_bytes());
        let err = read_block(&block, 0).unwrap_err();
        assert!(format!("{err:?}").contains("raw bytes"));
    }

    #[test]
    fn round_trips_and_indexes() {
        let entries: Vec<Entry> = (0..200)
            .map(|i| {
                e(
                    &format!("k{i:04}"),
                    i % 3 != 0,
                    i % 2 == 0,
                    100 + i,
                    &[100 + i],
                )
            })
            .collect();
        let (data, blocks) = write(LAYER, &entries);
        assert!(blocks.len() > 1);
        assert_eq!(decode(&data, Stamp::default()).unwrap(), entries);
        assert_eq!(index_decode(&index_encode(&blocks)).unwrap(), blocks);
        // A corrupted byte fails the checksum.
        let mut bad = data.clone();
        bad[HEADER + 3] ^= 1;
        assert!(decode(&bad, Stamp::default()).is_err());
    }

    #[test]
    fn a_delta_reads_as_stamped_entries() {
        let mut w = DeltaWriter::new(64, 1, 1 << 20, Some(50), 100);
        w.apply(b"a", Write::Upsert(None), Old::Absent).unwrap();
        w.apply(b"b", Write::Upsert(Some(b"v2")), Old::Live(7, Some(b"v1")))
            .unwrap();
        w.apply(b"c", Write::Upsert(Some(b"v1")), Old::Live(7, Some(b"v1")))
            .unwrap(); // unchanged: nothing
        w.apply(b"d", Write::Remove, Old::Live(9, None)).unwrap();
        w.apply(b"e", Write::Remove, Old::Absent).unwrap(); // nothing
        w.out.finish().unwrap();
        assert_eq!((w.added, w.changed, w.removed), (1, 1, 1));
        let data: Vec<u8> = w.out.ready.into_iter().flat_map(|f| f.data).collect();
        let got = decode(
            &data,
            Stamp {
                commit: 5,
                generation: 50,
            },
        )
        .unwrap();
        let want = vec![
            Entry {
                key: b"a".to_vec(),
                present: true,
                start: false,
                commit: 5,
                generation: 50,
                flips: vec![5],
                ..Default::default()
            },
            Entry {
                key: b"b".to_vec(),
                present: true,
                start: true,
                commit: 5,
                generation: 50,
                payload: Some(b"v2".to_vec()),
                replaced: Some(7),
                ..Default::default()
            },
            Entry {
                key: b"d".to_vec(),
                present: false,
                start: true,
                commit: 5,
                generation: 50,
                flips: vec![5],
                replaced: Some(9),
                ..Default::default()
            },
        ];
        assert_eq!(got, want);
    }

    #[test]
    fn merges_keep_flips_after_the_cut_and_split_parts() {
        // a: added at 5 (older layer), removed at 8: absent at both ends.
        // b: present before, updated at 6. c: present before, removed at 7.
        // d: added at 3, removed at 4: both flips at or below the cut 4.
        let older = write(
            LAYER,
            &[
                e("a", true, false, 5, &[5]),
                e("b", true, true, 6, &[]),
                e("d", false, false, 4, &[4, 3]),
            ],
        )
        .0;
        let newer = write(
            LAYER,
            &[e("a", false, true, 8, &[8]), e("c", false, true, 7, &[7])],
        )
        .0;
        let ins = || {
            vec![
                stream(&older, Stamp::default()),
                stream(&newer, Stamp::default()),
            ]
        };
        let (main, side, dropped) = merged(ins(), Some(4), false);
        assert_eq!(
            main,
            vec![e("b", true, true, 6, &[]), e("c", false, true, 7, &[7])]
        );
        assert_eq!(side, vec![e("a", false, false, 8, &[8, 5])]);
        assert_eq!(dropped, 1);
        // In the base, every absent key with a flip left goes to the side.
        let (main, side, _) = merged(ins(), Some(4), true);
        assert_eq!(main, vec![e("b", true, true, 6, &[])]);
        assert_eq!(
            side,
            vec![
                e("a", false, false, 8, &[8, 5]),
                e("c", false, true, 7, &[7])
            ]
        );
    }

    #[test]
    fn presence_at_p_is_a_parity_of_flips() {
        let layer = write(
            LAYER,
            &[
                e("a", false, false, 8, &[8, 5]),
                e("b", true, true, 6, &[]),
                e("c", false, true, 7, &[7]),
            ],
        )
        .0;
        let at = |p: Option<u64>| {
            let mut out = vec![];
            scan(
                vec![stream(&layer, Stamp::default())],
                p,
                None,
                None,
                usize::MAX,
                None,
                None,
                &mut out,
            )
            .unwrap();
            out.into_iter()
                .map(|d| (String::from_utf8(d.key).unwrap(), d.before, d.after))
                .collect::<Vec<_>>()
        };
        // P = 6: a was present (added at 5, removed at 8); c was present.
        assert_eq!(
            at(Some(6)),
            vec![("a".into(), true, false), ("c".into(), true, false)]
        );
        // P = 2: a absent then and now: left out; b updated; c removed.
        assert_eq!(
            at(Some(2)),
            vec![("b".into(), true, true), ("c".into(), true, false)]
        );
        // P = −∞: present keys only.
        assert_eq!(at(None), vec![("b".into(), false, true)]);
    }

    #[test]
    fn a_replacement_removes_what_it_omits() {
        let base = write(
            LAYER,
            &[
                e("a", true, false, 1, &[]),
                e("b", true, false, 1, &[]),
                e("c", true, false, 1, &[]),
            ],
        )
        .0;
        let delta = write(
            DELTA,
            &[Entry {
                key: b"b".to_vec(),
                present: false,
                start: true,
                flips: vec![0],
                ..Default::default()
            }],
        )
        .0;
        let inputs = vec![
            stream(
                &delta,
                Stamp {
                    commit: 2,
                    generation: 20,
                },
            ),
            stream(&base, Stamp::default()),
        ];
        let written = SortedEntries::of(&[b"c", b"d"], None, &[]).unwrap();
        let mut j = LayerJoin::new(
            Source::Entries(Arc::new(written), 0),
            inputs,
            true,
            DeltaWriter::new(64, 1, 1 << 20, Some(100), 100),
        )
        .unwrap();
        let mut data = vec![];
        loop {
            match j.step().unwrap() {
                Step::File => data.extend(j.delta.out.ready.drain(..).flat_map(|f| f.data)),
                Step::Done => break,
                _ => unreachable!(),
            }
        }
        let stamp = Stamp {
            commit: 1,
            generation: 100,
        };
        let got: Vec<(String, u8, Option<u64>)> = decode(&data, stamp)
            .unwrap()
            .iter()
            .map(|x| {
                (
                    String::from_utf8(x.key.clone()).unwrap(),
                    x.kind(),
                    x.replaced,
                )
            })
            .collect();
        // Updates and removes name what they replaced; an add names nothing.
        assert!(matches!(
            got.as_slice(),
            [(a, REMOVED, Some(_)), (c, UPDATED, Some(_)), (d, ADDED, None)] if a == "a" && c == "c" && d == "d"
        ));
        assert!(decode(&data, Stamp::default()).is_err()); // read without its commit's generation
        assert_eq!((j.delta.added, j.delta.changed, j.delta.removed), (1, 1, 1));
    }

    #[test]
    fn the_interval_test_never_skips_a_match() {
        let g = |x: &str| Glob::new(x.as_bytes()).unwrap();
        // A25 R1: a short match between longer bounds; a terminal `**` (R2).
        assert!(g("?").may_match_between(b"aa", Some(b"ca")));
        assert!(g("a").may_match_between(b"a", Some(b"ab")));
        assert!(g("tenant/**").may_match_between(b"tenant/a/x", Some(b"tenant/a/z")));
        assert!(!g("tenant-03/**").may_match_between(b"tenant-04/a", Some(b"tenant-05/b")));
        assert!(g("*4242*").may_match_between(b"cust-0000000000000", None));
        // Exhaustive over short keys of a, b, /: whenever a key between the
        // bounds matches, the test says so.
        let alphabet = [b'a', b'b', b'/'];
        let mut keys: Vec<Vec<u8>> = vec![vec![]];
        for _ in 0..4 {
            let more: Vec<Vec<u8>> = keys
                .iter()
                .flat_map(|k| alphabet.iter().map(move |c| [k.clone(), vec![*c]].concat()))
                .collect();
            keys.extend(more);
        }
        keys.sort();
        keys.dedup();
        for pat in [
            "?", "a*", "*b", "**/a", "a/**", "*/b", "?a?", "**", "b**a", "**/b/**",
        ] {
            let gl = g(pat);
            for lo in (0..keys.len()).step_by(7) {
                for hi in (lo..keys.len()).step_by(11) {
                    let any = keys[lo..=hi].iter().any(|k| gl.matches(k));
                    if any {
                        assert!(
                            gl.may_match_between(&keys[lo], Some(&keys[hi])),
                            "{pat} {:?} {:?}",
                            keys[lo],
                            keys[hi]
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn a_merge_checks_presence_against_flips() {
        // b is present after starting present, with one flip at 4: a change
        // kind written wrong (an "added" for a key that was there).
        let older = write(LAYER, &[e("b", true, true, 4, &[4])]).0;
        let mut m = LayerMerge::new(
            vec![stream(&older, Stamp::default())],
            None,
            false,
            true,
            64,
            1,
            1 << 20,
        );
        assert!(m.step().is_err());
    }

    #[test]
    fn globs_follow_soleras_grammar() {
        let m = |g: &str, k: &str| Glob::new(g.as_bytes()).unwrap().matches(k.as_bytes());
        assert!(m("?", "b") && !m("?", "aa") && !m("?", "/"));
        assert!(m("tenant/**", "tenant/a/y") && m("**", "x/y/z") && !m("tenant/*", "tenant/a/y"));
        assert!(
            m("**/archive/**", "a/b/archive/c")
                && m("**/archive/**", "archive/c")
                && !m("**/archive/**", "xarchive/c")
        );
        assert!(
            m("*template*", "my-template-1")
                && !m("*template*", "dir/template")
                && m("**template*", "dir/template")
        );
    }
}
