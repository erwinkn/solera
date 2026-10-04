//! The engine cache's local form of a `.kx` file (docs/resolved-commits.md
//! §5), and exact reads over a snapshot of them.
//!
//! A local file holds the source's blocks decompressed, each re-encoded with
//! a restart point every `RESTART` entries — an entry with its whole key —
//! so a lookup is a binary search over the directory (held in memory), then
//! over the block's restart points, then a scan of at most `RESTART`
//! entries. Nothing is used unverified: the tail — identity and directory —
//! carries one CRC, checked on open, and each block one over its entries and
//! restart table, checked on every read. A file is written front to back as
//! its blocks fill, its tail and footer last, so a build holds one block, not
//! the file, and stops at a ceiling before writing past it.
//!
//! ```text
//! file       := "KXL3" · blocks · tail · footer
//! block      := entries (as in `.kx` blocks, uncompressed) · restart offsets (u32 each)
//! tail       := source size u64 · source digest (16 bytes) · source path (u32 len + bytes)
//!               · blocks u32 · entries u64 · directory
//! directory  := per block: first key, last key (varint len + bytes) · offset u64 (from the
//!               file's start) · entries length u32 · restarts u32 · entries u32 · crc u32
//! footer     := tail offset u64 · tail length u32 · tail crc u32 · "KXL3"
//! ```

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use crate::delta::{Delta, Old};
use crate::entries::SortedEntries;
use crate::format::Options;
use crate::format::{
    decompress_at_most, fmt_err, get_bytes, parse_index, put_bytes, shared_prefix, slice_at, Error,
    Result,
};
use crate::jobs::{Join, Step};
use crate::rows::Source;
use crate::stream::{read_entry, write_entry, Block, Merge, Next};

pub const MAGIC: &[u8; 4] = b"KXL3";
pub const RESTART: usize = 16;
const FOOTER: usize = 20;

struct Dir {
    first: Vec<u8>,
    last: Vec<u8>,
    offset: u64,
    entries_len: u32,
    restarts: u32,
    entries: u32,
    crc: u32,
}

impl Dir {
    fn len(&self) -> u64 {
        self.entries_len as u64 + 4 * self.restarts as u64
    }
}

/// Encoded entries per local block: a lookup reads one, so small blocks keep
/// a lookup's read small; the directory holds a few dozen bytes per block.
const LOCAL_BLOCK: usize = 8 * 1024;

/// A local block being written.
#[derive(Default)]
struct LocalBlock {
    buf: Vec<u8>,
    entries: u32,
    restarts: Vec<u32>,
    first: Vec<u8>,
    prev: Vec<u8>,
}

impl LocalBlock {
    fn push(
        &mut self,
        key: &[u8],
        generation: u64,
        deleted: bool,
        payload: Option<&[u8]>,
        predecessor: Option<u64>,
    ) {
        if self.entries == 0 {
            self.first = key.to_vec();
        }
        let shared = if (self.entries as usize).is_multiple_of(RESTART) {
            self.restarts.push(self.buf.len() as u32);
            0
        } else {
            shared_prefix(&self.prev, key)
        };
        write_entry(
            &mut self.buf,
            shared,
            &key[shared..],
            generation,
            deleted,
            payload,
            predecessor,
        );
        self.prev.clear();
        self.prev.extend_from_slice(key);
        self.entries += 1;
    }

    /// Appends the restart table; the block's bytes and directory entry, at `offset`.
    fn close(&mut self, offset: u64) -> (Vec<u8>, Dir) {
        let entries_len = self.buf.len() as u32;
        let mut buf = std::mem::take(&mut self.buf);
        for r in &self.restarts {
            buf.extend_from_slice(&r.to_le_bytes());
        }
        let d = Dir {
            first: std::mem::take(&mut self.first),
            last: self.prev.clone(),
            offset,
            entries_len,
            restarts: self.restarts.len() as u32,
            entries: self.entries,
            crc: crc32fast::hash(&buf),
        };
        *self = LocalBlock::default();
        (buf, d)
    }
}

/// A file being written that may not grow past `max` bytes.
struct Bounded {
    w: BufWriter<File>,
    written: u64,
    max: u64,
}

impl Bounded {
    fn write(&mut self, b: &[u8]) -> Result<()> {
        if self.written + b.len() as u64 > self.max {
            return Err(Error::Limit(format!(
                "a local file over {} bytes",
                self.max
            )));
        }
        self.w
            .write_all(b)
            .map_err(|e| Error::Format(format!("cannot write a local file: {e}")))?;
        self.written += b.len() as u64;
        Ok(())
    }
}

/// Writes the local form of the `.kx` file `kx` — named `source`, with
/// content digest `digest` — to `out`, after checking its CRCs: at most
/// `max_bytes` (`Error::Limit` past them, the file then partial). Returns
/// its size.
pub fn build(kx: &[u8], source: &str, digest: &[u8], out: &Path, max_bytes: u64) -> Result<u64> {
    if digest.len() != 16 {
        return Err(Error::Value("a digest is 16 bytes".into()));
    }
    let idx = parse_index(kx, kx.len() as u64)?;
    let file =
        File::create(out).map_err(|e| Error::Format(format!("cannot create {out:?}: {e}")))?;
    let mut w = Bounded {
        w: BufWriter::new(file),
        written: 0,
        max: max_bytes,
    };
    w.write(MAGIC)?;
    let mut dir: Vec<Dir> = Vec::new();
    let mut total = 0u64;
    let mut local = LocalBlock::default();
    for (_, offset, size, _, crc) in &idx.blocks {
        let Some(raw) = slice_at(kx, *offset, *size) else {
            return fmt_err("block out of bounds");
        };
        if crc32fast::hash(raw) != *crc {
            return fmt_err("block checksum mismatch");
        }
        // A source block decompresses to no more than the file may hold.
        // Each entry as it is read, one key rebuilt at a time: a source block's
        // shared prefixes are never all expanded at once.
        let data = decompress_at_most(raw, idx.footer.codec, max_bytes)?;
        let (mut pos, mut key) = (0usize, Vec::new());
        while pos < data.len() {
            let f = read_entry(&data, &mut pos)?;
            if f.shared > key.len() {
                return fmt_err("bad shared prefix length");
            }
            key.truncate(f.shared);
            key.extend_from_slice(&data[f.suffix.0..f.suffix.1]);
            if key.len() as u64 > max_bytes {
                return Err(Error::Limit(format!("a key over {max_bytes} bytes")));
            }
            local.push(
                &key,
                f.generation,
                f.deleted(),
                f.payload(&data),
                f.predecessor,
            );
            if local.buf.len() >= LOCAL_BLOCK {
                let (bytes, d) = local.close(w.written);
                w.write(&bytes)?;
                dir.push(d);
            }
            total += 1;
        }
    }
    if local.entries > 0 {
        let (bytes, d) = local.close(w.written);
        w.write(&bytes)?;
        dir.push(d);
    }
    let mut tail = Vec::with_capacity(64 + dir.len() * 64);
    tail.extend_from_slice(&(kx.len() as u64).to_le_bytes());
    tail.extend_from_slice(digest);
    tail.extend_from_slice(&(source.len() as u32).to_le_bytes());
    tail.extend_from_slice(source.as_bytes());
    tail.extend_from_slice(&(dir.len() as u32).to_le_bytes());
    tail.extend_from_slice(&total.to_le_bytes());
    for d in &dir {
        put_bytes(&mut tail, &d.first);
        put_bytes(&mut tail, &d.last);
        tail.extend_from_slice(&d.offset.to_le_bytes());
        tail.extend_from_slice(&d.entries_len.to_le_bytes());
        tail.extend_from_slice(&d.restarts.to_le_bytes());
        tail.extend_from_slice(&d.entries.to_le_bytes());
        tail.extend_from_slice(&d.crc.to_le_bytes());
    }
    let mut footer = Vec::with_capacity(FOOTER);
    footer.extend_from_slice(&w.written.to_le_bytes());
    footer.extend_from_slice(&(tail.len() as u32).to_le_bytes());
    footer.extend_from_slice(&crc32fast::hash(&tail).to_le_bytes());
    footer.extend_from_slice(MAGIC);
    w.write(&tail)?;
    w.write(&footer)?;
    w.w.flush()
        .map_err(|e| Error::Format(format!("cannot write a local file: {e}")))?;
    Ok(w.written)
}

/// An entry found by a lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hit {
    pub generation: u64,
    pub deleted: bool,
    pub payload: Option<Vec<u8>>,
}

/// A local file, open: its directory in memory, its blocks read on demand.
pub struct Local {
    pub source: String,
    pub source_size: u64,
    pub digest: [u8; 16],
    pub entries: u64,
    pub size: u64,
    dir: Vec<Dir>,
    file: File,
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

impl Local {
    /// Opens a local file, verifying its tail.
    pub fn open(path: &Path) -> Result<Local> {
        let file =
            File::open(path).map_err(|e| Error::Format(format!("cannot open {path:?}: {e}")))?;
        let size = file
            .metadata()
            .map_err(|e| Error::Format(format!("cannot stat {path:?}: {e}")))?
            .len();
        let mut footer = [0u8; FOOTER];
        if size < (MAGIC.len() + FOOTER) as u64
            || file
                .read_exact_at(&mut footer, size - FOOTER as u64)
                .is_err()
        {
            return fmt_err("local file too short");
        }
        if &footer[16..20] != MAGIC {
            return fmt_err("not a local key index file");
        }
        let (at, n, crc) = (
            u64_at(&footer, 0),
            u32_at(&footer, 8) as usize,
            u32_at(&footer, 12),
        );
        if at < MAGIC.len() as u64 || at + n as u64 + FOOTER as u64 != size {
            return fmt_err("bad local tail");
        }
        let mut h = vec![0u8; n];
        file.read_exact_at(&mut h, at)
            .map_err(|e| Error::Format(format!("cannot read {path:?}: {e}")))?;
        if crc32fast::hash(&h) != crc {
            return fmt_err("local directory checksum mismatch");
        }
        let mut pos = 0;
        let need = |pos: usize, k: usize| -> Result<()> {
            if pos + k > n {
                fmt_err("truncated local tail")
            } else {
                Ok(())
            }
        };
        need(pos, 8 + 16 + 4)?;
        let source_size = u64_at(&h, pos);
        let mut digest = [0u8; 16];
        digest.copy_from_slice(&h[pos + 8..pos + 24]);
        let plen = u32_at(&h, pos + 24) as usize;
        pos += 28;
        need(pos, plen + 12)?;
        let source = String::from_utf8_lossy(&h[pos..pos + plen]).into_owned();
        pos += plen;
        let blocks = u32_at(&h, pos) as usize;
        let entries = u64_at(&h, pos + 4);
        pos += 12;
        let mut dir = Vec::with_capacity(blocks.min(n / 24));
        for _ in 0..blocks {
            let first = get_bytes(&h, &mut pos)?.to_vec();
            let last = get_bytes(&h, &mut pos)?.to_vec();
            need(pos, 24)?;
            dir.push(Dir {
                first,
                last,
                offset: u64_at(&h, pos),
                entries_len: u32_at(&h, pos + 8),
                restarts: u32_at(&h, pos + 12),
                entries: u32_at(&h, pos + 16),
                crc: u32_at(&h, pos + 20),
            });
            pos += 24;
        }
        if dir
            .iter()
            .any(|d| d.offset < MAGIC.len() as u64 || d.offset + d.len() > at)
        {
            return fmt_err("local directory points past the blocks");
        }
        Ok(Local {
            source,
            source_size,
            digest,
            entries,
            size,
            dir,
            file,
        })
    }

    pub fn min(&self) -> Option<&[u8]> {
        self.dir.first().map(|d| d.first.as_slice())
    }

    pub fn max(&self) -> Option<&[u8]> {
        self.dir.last().map(|d| d.last.as_slice())
    }

    pub fn blocks(&self) -> usize {
        self.dir.len()
    }

    /// The first block a scan of the keys past `key` reads: the one `key`
    /// would be in, or the first.
    fn block_from(&self, key: &[u8]) -> usize {
        self.dir
            .partition_point(|d| d.first.as_slice() <= key)
            .saturating_sub(1)
    }

    /// The block that could hold `key`, or None.
    fn block_of(&self, key: &[u8]) -> Option<usize> {
        let i = self.dir.partition_point(|d| d.first.as_slice() <= key);
        (i > 0 && key <= self.dir[i - 1].last.as_slice()).then(|| i - 1)
    }

    /// An error naming this file: the cache drops it and fetches its source again.
    fn bad<T>(&self, what: impl std::fmt::Display) -> Result<T> {
        Err(Error::Local(self.source.clone(), what.to_string()))
    }

    /// Block `i`'s bytes — entries, then restart offsets — after its CRC.
    fn read(&self, i: usize) -> Result<Vec<u8>> {
        let d = &self.dir[i];
        let mut buf = vec![0u8; d.len() as usize];
        if let Err(e) = self.file.read_exact_at(&mut buf, d.offset) {
            return self.bad(format!("cannot read a block: {e}"));
        }
        if crc32fast::hash(&buf) != d.crc {
            return self.bad("block checksum mismatch");
        }
        Ok(buf)
    }

    /// Block `i`, decoded for a merge.
    pub fn decoded(&self, i: usize) -> Result<Block> {
        let buf = self.read(i)?;
        let b = Block::decode(
            &buf[..self.dir[i].entries_len as usize],
            crate::format::CODEC_NONE,
        )?;
        if b.len() != self.dir[i].entries as usize
            || b.key(0) != self.dir[i].first.as_slice()
            || b.key(b.len() - 1) != self.dir[i].last.as_slice()
        {
            return self.bad("a block does not match its directory");
        }
        Ok(b)
    }

    /// Finds `key` in block `i`, whose bytes are `buf`.
    fn find_in(&self, i: usize, buf: &[u8], key: &[u8]) -> Result<Option<Hit>> {
        let d = &self.dir[i];
        let entries = &buf[..d.entries_len as usize];
        let restart = |r: usize| u32_at(buf, d.entries_len as usize + 4 * r) as usize;
        let key_at = |off: usize| -> Result<&[u8]> {
            let mut p = off;
            let f = read_entry(entries, &mut p)?;
            if f.shared != 0 {
                return self.bad("a restart point shares a prefix");
            }
            Ok(&entries[f.suffix.0..f.suffix.1])
        };
        // The last restart point whose key is <= key.
        let (mut lo, mut hi) = (0usize, d.restarts as usize);
        while hi - lo > 1 {
            let mid = (lo + hi) / 2;
            if key_at(restart(mid))? <= key {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        let mut p = restart(lo);
        let end = if lo + 1 < d.restarts as usize {
            restart(lo + 1)
        } else {
            entries.len()
        };
        let mut cur: Vec<u8> = Vec::new();
        while p < end {
            let f = read_entry(entries, &mut p)?;
            if f.shared > cur.len() {
                return self.bad("bad shared prefix length");
            }
            cur.truncate(f.shared);
            cur.extend_from_slice(&entries[f.suffix.0..f.suffix.1]);
            match cur.as_slice().cmp(key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => {
                    return Ok(Some(Hit {
                        generation: f.generation,
                        deleted: f.deleted(),
                        payload: f.payload(entries).map(<[u8]>::to_vec),
                    }))
                }
                std::cmp::Ordering::Greater => return Ok(None),
            }
        }
        Ok(None)
    }
}

/// An index as local files: runs newest first, each a level-0 file alone or
/// a deeper level's files in key order. Each run keeps the block it read
/// last: a resolve's keys come sorted, so it never needs an earlier one.
pub struct Snapshot {
    pub runs: Vec<Vec<Arc<Local>>>,
    last: Vec<Option<(usize, usize, Vec<u8>)>>, // per run: file, block, its bytes
}

impl Snapshot {
    pub fn new(runs: Vec<Vec<Arc<Local>>>) -> Snapshot {
        Snapshot {
            last: runs.iter().map(|_| None).collect(),
            runs,
        }
    }

    pub fn entries(&self) -> u64 {
        self.runs.iter().flatten().map(|f| f.entries).sum()
    }

    pub fn blocks(&self) -> usize {
        self.runs.iter().flatten().map(|f| f.blocks()).sum()
    }

    /// The newest entry of `key`, live or deleted.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Hit>> {
        for r in 0..self.runs.len() {
            let run = &self.runs[r];
            let fi = run.partition_point(|f| f.min().is_some_and(|m| m <= key));
            if fi == 0 {
                continue;
            }
            let f = &run[fi - 1];
            let Some(b) = f.block_of(key) else { continue };
            let held = matches!(&self.last[r], Some((lf, lb, _)) if (*lf, *lb) == (fi - 1, b));
            if !held {
                self.last[r] = Some((fi - 1, b, f.read(b)?));
            }
            let buf = &self.last[r].as_ref().expect("just read").2;
            if let Some(hit) = f.find_in(b, buf, key)? {
                return Ok(Some(hit));
            }
        }
        Ok(None)
    }

    /// The delta of `sorted` against this snapshot: as a patch, or with
    /// `replace` as the whole new content, live keys it omits deleted. A
    /// patch looks up each of its keys while that reads less than every
    /// block; anything else is the streaming job, fed local blocks.
    pub fn resolve(
        &mut self,
        sorted: &Arc<SortedEntries>,
        replace: bool,
        generation: u64,
        o: Options,
        max_file_bytes: usize,
    ) -> Result<Delta> {
        // A point lookup reads and checks one small block (~2.5 µs); a merge decodes every entry
        // (~120 ns each, bench/keys/warm.py): points win until about one lookup per 20 entries.
        if !replace && 16 * sorted.len() * self.runs.len() < self.entries() as usize {
            let mut d = Delta::new(o, max_file_bytes, 0, generation);
            for i in 0..sorted.len() {
                let hit = self.get(sorted.key(i))?;
                let was = match &hit {
                    Some(h) if !h.deleted => Old::Live(h.generation, h.payload.as_deref()),
                    _ => Old::Absent,
                };
                d.apply(sorted.key(i), sorted.write(i), was)?;
            }
            d.finish()?;
            return Ok(d);
        }
        let mut job = Join::new(
            Source::Entries(sorted.clone(), 0),
            replace,
            self.runs.len(),
            o,
            max_file_bytes,
            0,
            generation,
        )?;
        let mut feed = Feed::new(self.runs.len());
        let mut files = VecDeque::new();
        loop {
            match job.step()? {
                Step::Run(r) => feed.feed(self, &mut job.merge, r)?,
                Step::File => files.extend(job.delta.writer.files.pop_front()),
                Step::Done => break,
                Step::Rows | Step::Page => {
                    unreachable!("a patch reads no rows, makes no pages")
                }
            }
        }
        let mut d = job.delta;
        d.writer.files = files;
        Ok(d)
    }

    /// The entries of the merged snapshot, in key order.
    pub fn merge(&self) -> LocalMerge<'_> {
        LocalMerge {
            snap: self,
            merge: Merge::new(self.runs.len()),
            feed: Feed::new(self.runs.len()),
        }
    }

    /// Up to `limit` entries of the merged snapshot past `after` —
    /// deletions dropped with `drop_deleted` — as sorted entries, and the
    /// cursor to continue from: the last one's key, or None when nothing
    /// lies past it. Each run is read from the block holding `after`; past
    /// `max_bytes` of keys and payloads, an `Error::Limit`.
    pub fn scan(
        &self,
        after: Option<&[u8]>,
        limit: usize,
        drop_deleted: bool,
        max_bytes: u64,
    ) -> Result<(SortedEntries, Option<Vec<u8>>)> {
        let mut m = self.merge();
        if let Some(a) = after {
            m.feed.seek(self, a);
        }
        let mut page = SortedEntries::default();
        while m.advance()? {
            let e = &m.merge;
            if after.is_some_and(|a| e.key() <= a) || (drop_deleted && e.deleted()) {
                continue;
            }
            if page.len() == limit {
                let last = page.key(page.len() - 1).to_vec();
                return Ok((page.shrink(), Some(last)));
            }
            let held = page.keys.data.len() + page.payloads.data.len();
            if (held + e.key().len() + e.payload().map_or(0, <[u8]>::len)) as u64 > max_bytes {
                return Err(Error::Limit(format!("a page over {max_bytes} bytes")));
            }
            page.push(e.key(), e.generation(), e.deleted(), e.payload())?;
        }
        Ok((page.shrink(), None))
    }
}

/// Feeds a `Merge` the decoded blocks of a snapshot's runs, one at a time.
pub struct Feed {
    next: Vec<(usize, usize)>, // per run: file, block
}

impl Feed {
    pub fn new(runs: usize) -> Feed {
        Feed {
            next: vec![(0, 0); runs],
        }
    }

    /// Starts every run at the block holding `key`: a scan of the keys past it.
    fn seek(&mut self, snap: &Snapshot, key: &[u8]) {
        for (r, run) in snap.runs.iter().enumerate() {
            let fi = run
                .partition_point(|f| f.min().is_some_and(|m| m <= key))
                .saturating_sub(1);
            self.next[r] = match run.get(fi) {
                Some(f) => (fi, f.block_from(key)),
                None => (0, 0),
            };
        }
    }

    /// Run `r`'s next block into `m`, or its end.
    pub fn feed(&mut self, snap: &Snapshot, m: &mut Merge, r: usize) -> Result<()> {
        let run = &snap.runs[r];
        let skip = |mut at: (usize, usize)| {
            while at.0 < run.len() && at.1 >= run[at.0].blocks() {
                at = (at.0 + 1, 0);
            }
            at
        };
        let (fi, bi) = skip(self.next[r]);
        if fi >= run.len() {
            m.runs[r].end();
            return Ok(());
        }
        let block = run[fi].decoded(bi)?;
        self.next[r] = skip((fi, bi + 1));
        m.runs[r].push_block(block, self.next[r].0 >= run.len());
        Ok(())
    }
}

/// The merged view of a snapshot, a key at a time.
pub struct LocalMerge<'a> {
    snap: &'a Snapshot,
    pub merge: Merge,
    feed: Feed,
}

impl LocalMerge<'_> {
    /// The next merged entry, or false at the end.
    pub fn advance(&mut self) -> Result<bool> {
        loop {
            match self.merge.next_key()? {
                Next::Entry => return Ok(true),
                Next::End => return Ok(false),
                Next::Need(r) => self.feed.feed(self.snap, &mut self.merge, r)?,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{encode_file, Options, CODEC_ZLIB};

    const O: Options = Options {
        block_size: 512,
        level: 1,
        bits_per_item: 14,
        k: 10,
        codec: CODEC_ZLIB,
    };

    fn kx(n: usize, tag: &str, step: usize, gen: u64) -> Vec<u8> {
        let keys: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("k{:06}", i * step).into_bytes())
            .collect();
        let pays: Vec<Vec<u8>> = (0..n).map(|i| format!("{tag}{i}").into_bytes()).collect();
        let k: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let p: Vec<Option<&[u8]>> = pays.iter().map(|p| Some(p.as_slice())).collect();
        encode_file(&k, &vec![gen; n], &vec![0; n], &p, &vec![None; n], O).unwrap()
    }

    fn local(dir: &Path, name: &str, data: &[u8]) -> Arc<Local> {
        let path = dir.join(name);
        build(data, name, &[7; 16], &path, u64::MAX).unwrap();
        Arc::new(Local::open(&path).unwrap())
    }

    #[test]
    fn lookups_and_merges_over_local_files() {
        let dir = std::env::temp_dir().join(format!("kxl-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let old = local(&dir, "old", &kx(20_000, "o", 1, 1));
        let new = local(&dir, "new", &kx(300, "n", 3, 2));
        assert!(old.blocks() > 10);
        assert_eq!(old.entries, 20_000);
        let mut s = Snapshot::new(vec![vec![new.clone()], vec![old.clone()]]);
        for i in 0..20_000 {
            let hit = s.get(format!("k{i:06}").as_bytes()).unwrap().unwrap();
            if i % 3 == 0 && i / 3 < 300 {
                assert_eq!(
                    (hit.payload, hit.generation),
                    (Some(format!("n{}", i / 3).into_bytes()), 2)
                );
            } else {
                assert_eq!(
                    (hit.payload, hit.generation),
                    (Some(format!("o{i}").into_bytes()), 1)
                );
            }
        }
        assert!(s.get(b"k9999999").unwrap().is_none());
        assert!(s.get(b"a").unwrap().is_none());
        let mut m = s.merge();
        let mut n = 0;
        while m.advance().unwrap() {
            n += 1;
        }
        assert_eq!(n, 20_000);

        // A corrupted directory is refused on open; a corrupted block on read.
        let path = dir.join("old");
        let mut bytes = std::fs::read(&path).unwrap();
        let tail = u64_at(&bytes, bytes.len() - FOOTER) as usize;
        bytes[tail + 40] ^= 1;
        std::fs::write(dir.join("bad-dir"), &bytes).unwrap();
        assert!(Local::open(&dir.join("bad-dir")).is_err());
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[tail - 10] ^= 1; // the last block
        std::fs::write(dir.join("bad-block"), &bytes).unwrap();
        let bad = Arc::new(Local::open(&dir.join("bad-block")).unwrap());
        let mut s = Snapshot::new(vec![vec![bad]]);
        assert!(s.get(b"k019999").is_err());

        // A build stops at its ceiling, before writing past it.
        let big = kx(20_000, "o", 1, 1);
        let out = dir.join("capped");
        assert!(matches!(
            build(&big, "capped", &[7; 16], &out, 10_000),
            Err(Error::Limit(_))
        ));
        assert!(std::fs::metadata(&out).unwrap().len() <= 10_000);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
