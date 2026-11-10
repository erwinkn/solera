//! The layer file: entries sorted by key, then by commit (newest first), in
//! zstd blocks; a block index in segments; a top level; a fixed trailer.
//!
//! ```text
//! [block]...[block] [commits] [segment]...[segment] [top] [trailer]
//! ^0                ^commits_start ^index_start      ^top_start   ^size-48
//! ```
//!
//! - A block holds rows of about `block_bytes` raw bytes, cut only between
//!   keys, so one key's entries never span blocks. A row is the key
//!   prefix-coded against the previous row's (varint shared length, varint
//!   suffix length, suffix), the commit (varint), a flag byte (`run::NEW`,
//!   `REPLACED`, `NEW_BYTES`, `REPLACED_BYTES`), then each present version:
//!   a varint, or a varint length and the bytes. A row repeating the previous
//!   key has shared = its length and no suffix.
//! - A segment indexes up to `seg_blocks` blocks: for each, its offset (delta),
//!   compressed and raw lengths, rows, xxh3 checksum (low 32 bits, 4 bytes LE)
//!   and first key (prefix-coded against the previous block's).
//! - The top level holds the layer's own description (commit run, floor,
//!   counts, key bounds, flags) and one descriptor per segment, laid out like
//!   a block's. A reader of a large layer loads only the segments it needs.
//! - The commits section holds each commit's header; queries never read it.
//! - The trailer: index_start, commits_start, top_start (u64 LE), top raw
//!   length, top checksum, format version, trailer checksum (u32 LE), magic.
//!
//! Every section is checksummed; a reader verifies each block, segment and top
//! level it reads before using it.

use crate::error::{format, invalid, Error, Result};
use crate::run::{Run, Version, KNOWN_FLAGS, NEW, NEW_BYTES, REPLACED, REPLACED_BYTES};
use xxhash_rust::xxh3::xxh3_64;

pub const MAGIC: [u8; 8] = *b"SOLCIDX1";
pub const VERSION: u32 = 1;
pub const TRAILER: usize = 48;
/// Commit flags this reader understands. None yet: a full run's "clear" would
/// be the first, and a reader that does not know a flag refuses the layer.
pub const KNOWN_COMMIT_FLAGS: u64 = 0;

/// The most raw bytes a block, a segment, the commits section or the top
/// level may hold: a reader allocates no more than this for any of them,
/// whatever a damaged file says.
pub const MAX_RAW: usize = 64 << 20;

/// The longest key. With `MAX_RAW`, it bounds what a reader rebuilds from
/// prefix-coded keys: a block's distinct keys, a segment's or the top level's
/// first keys, at most `MAX_RAW` bytes each.
pub const MAX_KEY: usize = 64 << 10;

fn fits(what: &str, len: usize) -> Result<()> {
    if len > MAX_RAW {
        return invalid(format!(
            "{what} of {len} bytes: the format allows {MAX_RAW}"
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub struct WriteOptions {
    pub block_bytes: usize,
    pub level: i32,
    pub seg_blocks: usize,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            block_bytes: 32 << 10,
            level: 1,
            seg_blocks: 128,
        }
    }
}

/// A layer as the engine's state lists it: where it is, which commits it
/// covers, and where its metadata starts, so that opening it costs one read
/// and nothing is found by LIST.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct LayerRef {
    pub path: String,
    pub size: u64,
    pub index_start: u64,
    pub top_start: u64,
    /// The commits the layer covers, `lo..=hi`.
    pub lo: u64,
    pub hi: u64,
    /// A base: the commit whose state it holds, one entry per key present
    /// there. Entries at or below the floor are that state, not changes.
    pub floor: Option<u64>,
    pub entries: u64,
    pub keys: u64,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    /// The union of its commits' flags.
    pub flags: u64,
}

impl LayerRef {
    /// What a reference must hold before any of its file is read: offsets in
    /// order within the file, and no commit flag this reader does not know.
    pub fn check(&self) -> Result<()> {
        if self.flags & !KNOWN_COMMIT_FLAGS != 0 {
            return format(format!(
                "{}: commit flags {:#x} are not understood by this reader",
                self.path, self.flags
            ));
        }
        if self.index_start > self.top_start
            || self
                .top_start
                .checked_add(TRAILER as u64)
                .is_none_or(|end| end > self.size)
        {
            return invalid(format!(
                "{}: offsets out of order: index {}, top {}, size {}",
                self.path, self.index_start, self.top_start, self.size
            ));
        }
        Ok(())
    }
}

/// One commit's header. Encoded as a length-prefixed record whose fields are
/// read in order, so later versions append fields and older readers skip them.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct CommitHeader {
    pub commit: u64,
    pub time_ms: u64,
    pub flags: u64,
    pub added: u64,
    pub updated: u64,
    pub removed: u64,
    /// Keys present after the commit.
    pub live: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    /// Who produced it (run, batch, attempt; or a source report), as the engine encodes it.
    pub producer: Vec<u8>,
}

#[inline]
pub(crate) fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

#[inline]
pub(crate) fn varint(b: &[u8], p: &mut usize) -> Result<u64> {
    let mut n = 0u64;
    let mut s = 0;
    loop {
        let Some(&x) = b.get(*p) else {
            return format("truncated varint");
        };
        *p += 1;
        n |= ((x & 0x7f) as u64) << s;
        if x < 0x80 {
            return Ok(n);
        }
        s += 7;
        if s > 63 {
            return format("varint too long");
        }
    }
}

#[inline]
fn take<'a>(b: &'a [u8], p: &mut usize, n: usize) -> Result<&'a [u8]> {
    let end = p.checked_add(n).filter(|&e| e <= b.len());
    let Some(end) = end else {
        return format("truncated field");
    };
    let s = &b[*p..end];
    *p = end;
    Ok(s)
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_varint(out, b.len() as u64);
    out.extend_from_slice(b);
}

fn get_bytes(b: &[u8], p: &mut usize) -> Result<Vec<u8>> {
    let n = varint(b, p)? as usize;
    Ok(take(b, p, n)?.to_vec())
}

fn shared(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

pub(crate) fn checksum(b: &[u8]) -> u32 {
    xxh3_64(b) as u32
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

#[inline]
fn put_version(out: &mut Vec<u8>, v: Version<'_>) {
    match v {
        Version::Absent => {}
        Version::Int(n) => put_varint(out, n),
        Version::Bytes(b) => put_bytes(out, b),
    }
}

/// Writes one layer. Rows arrive in order (key ascending, then commit
/// descending); the file's bytes accumulate in `out` for the caller to drain
/// as it uploads.
pub struct Writer {
    o: WriteOptions,
    lo: u64,
    hi: u64,
    floor: Option<u64>,
    zc: zstd::bulk::Compressor<'static>,
    raw: Vec<u8>,
    cbuf: Vec<u8>,
    rows: u64,
    first: Vec<u8>,
    prev: Vec<u8>,
    prev_commit: u64,
    prev_state: bool,
    out: Vec<u8>,
    written: u64,
    seg: Vec<u8>,
    seg_n: u64,
    seg_first: Vec<u8>,
    seg_prev_first: Vec<u8>,
    seg_prev_off: u64,
    segs: Vec<u8>,
    top: Vec<u8>,
    top_prev: Vec<u8>,
    nsegs: u64,
    entries: u64,
    keys: u64,
    first_key: Vec<u8>,
    /// Bytes of the distinct keys in the current block, and of the top
    /// level's first keys, as a reader rebuilds them.
    block_keys: usize,
    top_keys: usize,
}

impl Writer {
    /// A layer covering commits `lo..=hi`; with `floor`, a base holding the
    /// state at that commit.
    pub fn new(o: WriteOptions, lo: u64, hi: u64, floor: Option<u64>) -> Result<Writer> {
        if lo > hi || lo == 0 || floor.is_some_and(|f| f < lo || f > hi) {
            return invalid(format!(
                "a layer covers commits lo..=hi with 1 <= lo <= hi and lo <= floor <= hi, got {lo}..={hi}, floor {floor:?}"
            ));
        }
        fits("a block", o.block_bytes.saturating_mul(2))?;
        if o.seg_blocks == 0 || o.seg_blocks * MAX_KEY > MAX_RAW {
            return invalid(format!(
                "seg_blocks {}: from 1 to {}",
                o.seg_blocks,
                MAX_RAW / MAX_KEY
            ));
        }
        let zc = zstd::bulk::Compressor::new(o.level)
            .map_err(|e| Error::Invalid(format!("zstd level {}: {e}", o.level)))?;
        Ok(Writer {
            o,
            lo,
            hi,
            floor,
            zc,
            raw: Vec::with_capacity(o.block_bytes * 2),
            cbuf: Vec::new(),
            rows: 0,
            first: Vec::new(),
            prev: Vec::new(),
            prev_commit: 0,
            prev_state: false,
            out: Vec::new(),
            written: 0,
            seg: Vec::new(),
            seg_n: 0,
            seg_first: Vec::new(),
            seg_prev_first: Vec::new(),
            seg_prev_off: 0,
            segs: Vec::new(),
            top: Vec::new(),
            top_prev: Vec::new(),
            nsegs: 0,
            entries: 0,
            keys: 0,
            first_key: Vec::new(),
            block_keys: 0,
            top_keys: 0,
        })
    }

    /// Bytes of the file written so far and not yet taken.
    pub fn pending(&self) -> usize {
        self.out.len()
    }

    /// Take the bytes written so far (the file continues where they end).
    pub fn take(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.out)
    }

    pub fn entries(&self) -> u64 {
        self.entries
    }

    /// The next row. Keys ascend; one key's commits descend.
    pub fn push(
        &mut self,
        key: &[u8],
        commit: u64,
        new: Version<'_>,
        replaced: Version<'_>,
    ) -> Result<()> {
        let same = self.entries > 0 && key == self.prev.as_slice();
        if self.entries > 0 && !same && key < self.prev.as_slice() {
            return invalid(format!(
                "keys out of order: {:?} after {:?}",
                lossy(key),
                lossy(&self.prev)
            ));
        }
        if same && commit >= self.prev_commit {
            return invalid(format!(
                "{:?}: commit {commit} after commit {} (a key's entries go newest first)",
                lossy(key),
                self.prev_commit
            ));
        }
        if commit < self.lo || commit > self.hi {
            return invalid(format!(
                "{:?}: commit {commit} outside the layer's commits {}..={}",
                lossy(key),
                self.lo,
                self.hi
            ));
        }
        if new.is_absent() && replaced.is_absent() {
            return invalid(format!(
                "{:?}: an entry at commit {commit} with neither version",
                lossy(key)
            ));
        }
        if new == replaced {
            return invalid(format!(
                "{:?}: an entry at commit {commit} that changes nothing",
                lossy(key)
            ));
        }
        let state = self.floor.is_some_and(|f| commit <= f);
        if state && (new.is_absent() || !replaced.is_absent() || (same && self.prev_state)) {
            return invalid(format!(
                "{:?}: at or below the floor a base holds one entry per key, its present version",
                lossy(key)
            ));
        }
        if key.len() > MAX_KEY {
            return invalid(format!(
                "{:?}…: a key of {} bytes, the format allows {MAX_KEY}",
                lossy(&key[..64]),
                key.len()
            ));
        }
        if !same && (self.raw.len() >= self.o.block_bytes || self.block_keys + key.len() > MAX_RAW)
        {
            self.flush_block()?;
        }
        let s = if self.rows == 0 {
            0
        } else {
            shared(&self.prev, key)
        };
        if self.rows == 0 {
            self.first.clear();
            self.first.extend_from_slice(key);
        }
        if self.entries == 0 {
            self.first_key = key.to_vec();
        }
        put_varint(&mut self.raw, s as u64);
        put_varint(&mut self.raw, (key.len() - s) as u64);
        self.raw.extend_from_slice(&key[s..]);
        put_varint(&mut self.raw, commit);
        let flag = |v: Version<'_>, present: u8, bytes: u8| match v {
            Version::Absent => 0,
            Version::Int(_) => present,
            Version::Bytes(_) => present | bytes,
        };
        self.raw
            .push(flag(new, NEW, NEW_BYTES) | flag(replaced, REPLACED, REPLACED_BYTES));
        put_version(&mut self.raw, new);
        put_version(&mut self.raw, replaced);
        if !same {
            self.prev.clear();
            self.prev.extend_from_slice(key);
            self.keys += 1;
            self.block_keys += key.len();
        }
        self.prev_commit = commit;
        self.prev_state = state;
        self.rows += 1;
        self.entries += 1;
        fits("a block", self.raw.len())
    }

    /// Every entry of key `i` of `run`, newest first.
    pub fn push_key(&mut self, run: &Run, i: usize) -> Result<()> {
        for row in run.rows(i) {
            self.push(
                run.key(i),
                run.commit(row),
                run.new_version(row),
                run.replaced_version(row),
            )?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        self.cbuf = self.zc.compress(&self.raw).expect("zstd compression");
        let check = checksum(&self.cbuf);
        self.out.extend_from_slice(&self.cbuf);
        if self.seg_n == 0 {
            self.seg_first.clone_from(&self.first);
            self.seg_prev_first.clear();
            self.seg_prev_off = 0;
        }
        let ix = &mut self.seg;
        put_varint(ix, self.written - self.seg_prev_off);
        self.seg_prev_off = self.written;
        put_varint(ix, self.cbuf.len() as u64);
        put_varint(ix, self.raw.len() as u64);
        put_varint(ix, self.rows);
        ix.extend_from_slice(&check.to_le_bytes());
        let s = shared(&self.seg_prev_first, &self.first);
        put_varint(ix, s as u64);
        put_varint(ix, (self.first.len() - s) as u64);
        ix.extend_from_slice(&self.first[s..]);
        self.seg_prev_first.clone_from(&self.first);
        self.written += self.cbuf.len() as u64;
        self.seg_n += 1;
        self.raw.clear();
        self.rows = 0;
        self.block_keys = 0;
        if self.seg_n as usize == self.o.seg_blocks {
            self.flush_seg()?;
        }
        Ok(())
    }

    fn flush_seg(&mut self) -> Result<()> {
        if self.seg_n == 0 {
            return Ok(());
        }
        fits("a segment", self.seg.len())?;
        self.top_keys += self.seg_first.len();
        fits("the top level's keys", self.top_keys)?;
        let c = self.zc.compress(&self.seg).expect("zstd compression");
        let t = &mut self.top;
        put_varint(t, self.segs.len() as u64);
        put_varint(t, c.len() as u64);
        put_varint(t, self.seg.len() as u64);
        put_varint(t, self.seg_n);
        t.extend_from_slice(&checksum(&c).to_le_bytes());
        let s = shared(&self.top_prev, &self.seg_first);
        put_varint(t, s as u64);
        put_varint(t, (self.seg_first.len() - s) as u64);
        t.extend_from_slice(&self.seg_first[s..]);
        self.top_prev.clone_from(&self.seg_first);
        self.segs.extend_from_slice(&c);
        self.nsegs += 1;
        self.seg.clear();
        self.seg_n = 0;
        Ok(())
    }

    /// Finish the file with its commits' headers; the last bytes stay in
    /// `out` for `take`. Returns the layer's reference, with `path` empty.
    pub fn finish(&mut self, commits: &[CommitHeader]) -> Result<LayerRef> {
        let floor = self.floor.unwrap_or(0);
        for (i, h) in commits.iter().enumerate() {
            if h.commit <= floor
                || h.commit < self.lo
                || h.commit > self.hi
                || (i > 0 && h.commit <= commits[i - 1].commit)
            {
                return invalid(format!(
                    "commit headers ascend within the layer's commits above its floor: {} in {}..={}, floor {floor}",
                    h.commit, self.lo, self.hi
                ));
            }
        }
        self.flush_block()?;
        self.flush_seg()?;
        let flags = commits.iter().fold(0, |f, h| f | h.flags);
        let commits_start = self.written;
        let mut raw = Vec::new();
        put_varint(&mut raw, commits.len() as u64);
        let mut rec = Vec::new();
        for h in commits {
            rec.clear();
            for v in [
                h.commit, h.time_ms, h.flags, h.added, h.updated, h.removed, h.live,
            ] {
                put_varint(&mut rec, v);
            }
            put_bytes(&mut rec, &h.min_key);
            put_bytes(&mut rec, &h.max_key);
            put_bytes(&mut rec, &h.producer);
            put_bytes(&mut raw, &rec);
        }
        fits("the commits section", raw.len())?;
        let c = self.zc.compress(&raw).expect("zstd compression");
        let mut commits_section = Vec::with_capacity(8 + c.len());
        commits_section.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        commits_section.extend_from_slice(&checksum(&c).to_le_bytes());
        commits_section.extend_from_slice(&c);
        self.out.extend_from_slice(&commits_section);
        self.written += commits_section.len() as u64;
        let index_start = self.written;
        self.out.extend_from_slice(&self.segs);
        self.written += self.segs.len() as u64;
        let mut top = Vec::new();
        for v in [
            self.lo,
            self.hi,
            self.floor.map_or(0, |f| f + 1),
            self.entries,
            self.keys,
            flags,
        ] {
            put_varint(&mut top, v);
        }
        put_bytes(&mut top, &self.first_key);
        put_bytes(&mut top, if self.entries == 0 { &[] } else { &self.prev });
        put_varint(&mut top, self.nsegs);
        top.extend_from_slice(&self.top);
        fits("the top level", top.len())?;
        let c = self.zc.compress(&top).expect("zstd compression");
        let top_start = self.written;
        self.out.extend_from_slice(&c);
        let mut trailer = Vec::with_capacity(TRAILER);
        trailer.extend_from_slice(&index_start.to_le_bytes());
        trailer.extend_from_slice(&commits_start.to_le_bytes());
        trailer.extend_from_slice(&top_start.to_le_bytes());
        trailer.extend_from_slice(&(top.len() as u32).to_le_bytes());
        trailer.extend_from_slice(&checksum(&c).to_le_bytes());
        trailer.extend_from_slice(&VERSION.to_le_bytes());
        let tc = checksum(&trailer);
        trailer.extend_from_slice(&tc.to_le_bytes());
        trailer.extend_from_slice(&MAGIC);
        debug_assert_eq!(trailer.len(), TRAILER);
        self.out.extend_from_slice(&trailer);
        self.written += (c.len() + TRAILER) as u64;
        Ok(LayerRef {
            path: String::new(),
            size: self.written,
            index_start,
            top_start,
            lo: self.lo,
            hi: self.hi,
            floor: self.floor,
            entries: self.entries,
            keys: self.keys,
            first_key: self.first_key.clone(),
            last_key: if self.entries == 0 {
                Vec::new()
            } else {
                self.prev.clone()
            },
            flags,
        })
    }
}

pub(crate) fn lossy(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// A block or segment list: per item its offset, lengths, rows (or blocks),
/// checksum and first key, in flat buffers.
#[derive(Default, Debug)]
pub struct Index {
    pub off: Vec<u64>,
    pub clen: Vec<u32>,
    pub rlen: Vec<u32>,
    pub n: Vec<u32>,
    pub check: Vec<u32>,
    first: Vec<u8>,
    first_end: Vec<u32>,
}

impl Index {
    pub fn len(&self) -> usize {
        self.off.len()
    }

    pub fn is_empty(&self) -> bool {
        self.off.is_empty()
    }

    pub fn first(&self, i: usize) -> &[u8] {
        let a = if i == 0 {
            0
        } else {
            self.first_end[i - 1] as usize
        };
        &self.first[a..self.first_end[i] as usize]
    }

    fn after_first_at_or_below(&self, k: &[u8]) -> usize {
        let (mut a, mut b) = (0, self.len());
        while a < b {
            let m = (a + b) / 2;
            if self.first(m) <= k {
                a = m + 1;
            } else {
                b = m;
            }
        }
        a
    }

    /// Items that may hold a key in `(lo, hi]` (`None`: unbounded).
    pub fn span(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> std::ops::Range<usize> {
        let a = lo.map_or(0, |lo| self.after_first_at_or_below(lo).saturating_sub(1));
        let b = hi.map_or(self.len(), |hi| self.after_first_at_or_below(hi));
        a..b.max(a)
    }

    /// The item that may hold `k`, if any.
    pub fn holding(&self, k: &[u8]) -> Option<usize> {
        self.after_first_at_or_below(k).checked_sub(1)
    }

    /// Memory it holds, for the cache's budget.
    pub(crate) fn bytes(&self) -> usize {
        self.off.capacity() * 8
            + (self.clen.capacity()
                + self.rlen.capacity()
                + self.n.capacity()
                + self.check.capacity()
                + self.first_end.capacity())
                * 4
            + self.first.capacity()
    }

    /// Entries whose ranges lie in `within`.
    fn parse(
        b: &[u8],
        count: Option<u64>,
        base: u64,
        relative: bool,
        within: std::ops::Range<u64>,
    ) -> Result<Index> {
        let mut ix = Index::default();
        let mut p = 0;
        let mut off = 0u64;
        let mut prev: std::ops::Range<usize> = 0..0;
        let mut i = 0u64;
        while count.map_or(p < b.len(), |c| i < c) {
            let d = varint(b, &mut p)?;
            let Some(o) = (if relative { base } else { off }).checked_add(d) else {
                return format("index offset overflows");
            };
            off = o;
            let mut small = || -> Result<u32> {
                u32::try_from(varint(b, &mut p)?).or_else(|_| format("index length overflows"))
            };
            let (clen, rlen, n) = (small()?, small()?, small()?);
            if clen as usize > zstd::zstd_safe::compress_bound(MAX_RAW) || rlen as usize > MAX_RAW {
                return format(format!("index entry of {clen} bytes, {rlen} raw"));
            }
            if off < within.start
                || off
                    .checked_add(clen as u64)
                    .is_none_or(|end| end > within.end)
            {
                return format(format!(
                    "index entry {off}+{clen} outside its section {within:?}"
                ));
            }
            ix.off.push(off);
            ix.clen.push(clen);
            ix.rlen.push(rlen);
            ix.n.push(n);
            ix.check
                .push(u32::from_le_bytes(take(b, &mut p, 4)?.try_into().unwrap()));
            let s = varint(b, &mut p)? as usize;
            let l = varint(b, &mut p)? as usize;
            if s > prev.len() {
                return format("index entry shares more than its predecessor's key");
            }
            if s + l > MAX_KEY || ix.first.len() + s + l > MAX_RAW {
                return format("index keys longer than the format allows");
            }
            let start = ix.first.len();
            ix.first.extend_from_within(prev.start..prev.start + s);
            ix.first.extend_from_slice(take(b, &mut p, l)?);
            ix.first_end.push(ix.first.len() as u32);
            prev = start..ix.first.len();
            i += 1;
        }
        if p != b.len() {
            return format("index has trailing bytes");
        }
        Ok(ix)
    }
}

/// A layer's top level: its description and its segments.
#[derive(Debug)]
pub struct Top {
    pub lo: u64,
    pub hi: u64,
    pub floor: Option<u64>,
    pub entries: u64,
    pub keys: u64,
    pub flags: u64,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub index_start: u64,
    pub commits_start: u64,
    pub top_start: u64,
    pub size: u64,
    pub segs: Index,
}

impl Top {
    pub fn bytes(&self) -> usize {
        self.segs.bytes() + self.first_key.len() + self.last_key.len() + 128
    }

    /// The top level from the file's tail `[top_start, size)`.
    pub fn parse(tail: &[u8], top_start: u64, size: u64) -> Result<Top> {
        if tail.len() < TRAILER || tail.len() as u64 != size - top_start {
            return format("tail shorter than its trailer");
        }
        let t = &tail[tail.len() - TRAILER..];
        if t[40..48] != MAGIC {
            return format("not a change-index layer (bad magic)");
        }
        if checksum(&t[..36]) != u32_at(t, 36) {
            return format("trailer checksum mismatch");
        }
        let version = u32_at(t, 32);
        if version != VERSION {
            return format(format!(
                "format version {version}, this reader reads {VERSION}"
            ));
        }
        let (index_start, commits_start, ts) = (u64_at(t, 0), u64_at(t, 8), u64_at(t, 16));
        if ts != top_start || commits_start > index_start || index_start > top_start {
            return format("trailer offsets do not match the layer reference");
        }
        if !(8..=8 + zstd::zstd_safe::compress_bound(MAX_RAW) as u64)
            .contains(&(index_start - commits_start))
        {
            return format("commits section length out of bounds");
        }
        let c = &tail[..tail.len() - TRAILER];
        if checksum(c) != u32_at(t, 28) {
            return format("top level checksum mismatch");
        }
        let raw = decompress(c, u32_at(t, 24) as usize)?;
        let mut p = 0;
        let mut v = [0u64; 6];
        for x in &mut v {
            *x = varint(&raw, &mut p)?;
        }
        let first_key = get_bytes(&raw, &mut p)?;
        let last_key = get_bytes(&raw, &mut p)?;
        let nsegs = varint(&raw, &mut p)?;
        let segs = Index::parse(
            &raw[p..],
            Some(nsegs),
            index_start,
            true,
            index_start..top_start,
        )?;
        Ok(Top {
            lo: v[0],
            hi: v[1],
            floor: v[2].checked_sub(1),
            entries: v[3],
            keys: v[4],
            flags: v[5],
            first_key,
            last_key,
            index_start,
            commits_start,
            top_start,
            size,
            segs,
        })
    }

    /// Whether this top level describes the layer its reference names.
    pub fn check(&self, l: &LayerRef) -> Result<()> {
        if (
            self.lo,
            self.hi,
            self.floor,
            self.entries,
            self.keys,
            self.flags,
        ) != (l.lo, l.hi, l.floor, l.entries, l.keys, l.flags)
            || self.first_key != l.first_key
            || self.last_key != l.last_key
            || (self.index_start, self.top_start, self.size) != (l.index_start, l.top_start, l.size)
        {
            return format(format!(
                "{}: the file does not match its layer reference",
                l.path
            ));
        }
        if self.flags & !KNOWN_COMMIT_FLAGS != 0 {
            return format(format!(
                "{}: commit flags {:#x} are not understood by this reader",
                l.path, self.flags
            ));
        }
        Ok(())
    }
}

/// A compressed frame must declare the length it is read for, before any
/// buffer of that length is allocated.
fn frame_holds(c: &[u8], rlen: usize) -> Result<()> {
    if rlen > MAX_RAW {
        return format(format!("{rlen} raw bytes: the format allows {MAX_RAW}"));
    }
    if !matches!(zstd::zstd_safe::get_frame_content_size(c), Ok(Some(n)) if n == rlen as u64) {
        return format(format!("the compressed frame does not hold {rlen} bytes"));
    }
    Ok(())
}

pub(crate) fn decompress(c: &[u8], rlen: usize) -> Result<Vec<u8>> {
    frame_holds(c, rlen)?;
    let raw = zstd::bulk::decompress(c, rlen).map_err(|e| Error::Format(format!("zstd: {e}")))?;
    if raw.len() != rlen {
        return format("decompressed length mismatch");
    }
    Ok(raw)
}

/// A segment's block list, from its verified bytes; its blocks lie in the
/// data section `[0, data_end)`.
pub fn parse_segment(c: &[u8], rlen: u32, check: u32, blocks: u32, data_end: u64) -> Result<Index> {
    if checksum(c) != check {
        return format("segment checksum mismatch");
    }
    let ix = Index::parse(&decompress(c, rlen as usize)?, None, 0, false, 0..data_end)?;
    if ix.len() != blocks as usize {
        return format("segment block count mismatch");
    }
    Ok(ix)
}

/// The commit headers, from the commits section `[commits_start, index_start)`.
pub fn parse_commits(b: &[u8]) -> Result<Vec<CommitHeader>> {
    if b.len() < 8 {
        return format("commits section too short");
    }
    let (rlen, check) = (u32_at(b, 0) as usize, u32_at(b, 4));
    if checksum(&b[8..]) != check {
        return format("commits section checksum mismatch");
    }
    let raw = decompress(&b[8..], rlen)?;
    let mut p = 0;
    let n = varint(&raw, &mut p)?;
    // a header takes at least 8 bytes: its length and seven fields
    if n > (raw.len() - p) as u64 / 8 {
        return format(format!(
            "{n} commit headers in a {} byte section",
            raw.len()
        ));
    }
    let mut out = Vec::new();
    for _ in 0..n {
        let len = varint(&raw, &mut p)? as usize;
        let rec = take(&raw, &mut p, len)?;
        let mut q = 0;
        let mut v = [0u64; 7];
        for x in &mut v {
            *x = varint(rec, &mut q)?;
        }
        out.push(CommitHeader {
            commit: v[0],
            time_ms: v[1],
            flags: v[2],
            added: v[3],
            updated: v[4],
            removed: v[5],
            live: v[6],
            min_key: get_bytes(rec, &mut q)?,
            max_key: get_bytes(rec, &mut q)?,
            producer: get_bytes(rec, &mut q)?,
        });
    }
    Ok(out)
}

/// Decodes blocks into runs, reusing its buffers.
pub struct Decoder {
    zd: zstd::bulk::Decompressor<'static>,
    raw: Vec<u8>,
}

impl Default for Decoder {
    fn default() -> Self {
        Decoder {
            zd: zstd::bulk::Decompressor::new().expect("zstd decompressor"),
            raw: Vec::new(),
        }
    }
}

impl Decoder {
    /// Block bytes (already verified) into `out`, which is cleared first.
    pub fn decode(&mut self, c: &[u8], rlen: u32, rows: u32, out: &mut Run) -> Result<()> {
        out.clear();
        self.raw.clear();
        frame_holds(c, rlen as usize)?;
        self.raw.reserve(rlen as usize);
        let n = self
            .zd
            .decompress_to_buffer(c, &mut self.raw)
            .map_err(|e| Error::Format(format!("zstd: {e}")))?;
        if n != rlen as usize {
            return format("block length mismatch");
        }
        let raw = &self.raw[..];
        let mut p = 0;
        let mut prev_commit = 0;
        let mut key_bytes = 0;
        for _ in 0..rows {
            let s = varint(raw, &mut p)? as usize;
            let l = varint(raw, &mut p)? as usize;
            if s.saturating_add(l) > MAX_KEY {
                return format("a key longer than the format allows");
            }
            let suffix = take(raw, &mut p, l)?;
            let commit = varint(raw, &mut p)?;
            let Some(&f) = raw.get(p) else {
                return format("truncated row");
            };
            p += 1;
            if f & !KNOWN_FLAGS != 0 || f & (NEW | REPLACED) == 0 {
                return format(format!("bad row flags {f:#x}"));
            }
            let mut ver = |present: u8, bytes: u8| -> Result<(u8, u64)> {
                if f & present == 0 {
                    Ok((0, 0))
                } else if f & bytes == 0 {
                    Ok((present, varint(raw, &mut p)?))
                } else {
                    let n = varint(raw, &mut p)? as usize;
                    let b = take(raw, &mut p, n)?;
                    Ok(out.put_version(Version::Bytes(b), present, bytes))
                }
            };
            let (fnew, vnew) = ver(NEW, NEW_BYTES)?;
            let (frep, vrep) = ver(REPLACED, REPLACED_BYTES)?;
            let prev = out.last_key();
            let same = prev.is_some_and(|k| s == k.len() && l == 0);
            if same {
                if commit >= prev_commit {
                    return format("a key's commits do not descend");
                }
            } else {
                let ascends = match prev {
                    None => s == 0,
                    Some(k) => {
                        s <= k.len()
                            && (if s < k.len() {
                                l > 0 && suffix[0] > k[s]
                            } else {
                                l > 0
                            })
                    }
                };
                if !ascends {
                    return format("keys do not ascend");
                }
                key_bytes += s + l;
                if key_bytes > MAX_RAW {
                    return format("a block's keys longer than the format allows");
                }
                out.push_key_prefixed(s, suffix);
            }
            out.push_raw(commit, fnew | frep, vnew, vrep);
            prev_commit = commit;
        }
        if p != raw.len() {
            return format("block has trailing bytes");
        }
        Ok(())
    }
}
