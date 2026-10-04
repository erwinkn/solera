//! Format v4 prototype (docs/key-index-design.md § The structure): a span's
//! files hold several versions of a key, newest first, with the
//! predecessor on the oldest. A merge keeps the versions live endpoints
//! see; readers clip every key to a generation range. Blocks are decoded
//! one at a time per run, over runs held whole in memory: a prototype for
//! differential tests and measurements, not the streaming job.
//!
//! A run is one span: its files' blocks in key order, which may split a
//! key's versions across blocks and files. Runs are given newest first, so
//! a key's versions, read run by run, come out newest first.

use crate::format::{Error, Options, Result};
use crate::stream::{Block, Writer};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version {
    pub generation: u64,
    pub deleted: bool,
    pub payload: Option<Vec<u8>>,
    pub predecessor: Option<u64>,
}

/// One run's blocks, decoded as the cursor reaches them.
struct Run<'a> {
    blocks: &'a [&'a [u8]],
    codec: u8,
    next: usize,
    block: Option<Block>,
    i: usize,
}

impl<'a> Run<'a> {
    fn new(blocks: &'a [&'a [u8]], codec: u8) -> Result<Run<'a>> {
        let mut r = Run {
            blocks,
            codec,
            next: 0,
            block: None,
            i: 0,
        };
        r.fill()?;
        Ok(r)
    }

    /// Moves to the next non-empty block when the current one is used up.
    fn fill(&mut self) -> Result<()> {
        while self.block.as_ref().is_none_or(|b| self.i >= b.len()) {
            if self.next == self.blocks.len() {
                self.block = None;
                return Ok(());
            }
            self.block = Some(Block::decode(self.blocks[self.next], self.codec)?);
            self.next += 1;
            self.i = 0;
        }
        Ok(())
    }

    fn key(&self) -> Option<&[u8]> {
        self.block.as_ref().map(|b| b.key(self.i))
    }

    fn take(&mut self) -> Result<Version> {
        let b = self.block.as_ref().expect("a current entry");
        let v = Version {
            generation: b.generation(self.i),
            deleted: b.deleted(self.i),
            payload: b.payload(self.i).map(<[u8]>::to_vec),
            predecessor: b.predecessor(self.i),
        };
        self.i += 1;
        self.fill()?;
        Ok(v)
    }
}

/// Runs merged by key: each key once, with every version the runs hold,
/// newest first.
pub struct Groups<'a> {
    runs: Vec<Run<'a>>,
}

impl<'a> Groups<'a> {
    pub fn new(runs: &'a [Vec<&'a [u8]>], codecs: &[u8]) -> Result<Groups<'a>> {
        if runs.len() != codecs.len() {
            return Err(Error::Value("a codec per run".into()));
        }
        let runs = runs
            .iter()
            .zip(codecs)
            .map(|(b, &c)| Run::new(b, c))
            .collect::<Result<_>>()?;
        Ok(Groups { runs })
    }

    pub fn next_group(&mut self) -> Result<Option<(Vec<u8>, Vec<Version>)>> {
        let Some(key) = self
            .runs
            .iter()
            .filter_map(Run::key)
            .min()
            .map(<[u8]>::to_vec)
        else {
            return Ok(None);
        };
        let mut versions = Vec::new();
        for r in &mut self.runs {
            while r.key() == Some(key.as_slice()) {
                let v = r.take()?;
                if let Some(prev) = versions.last().map(|p: &Version| p.generation) {
                    if v.generation >= prev {
                        return Err(Error::Format(format!(
                            "versions of {:?} out of order: {} after {}",
                            String::from_utf8_lossy(&key),
                            v.generation,
                            prev
                        )));
                    }
                }
                versions.push(v);
            }
        }
        Ok(Some((key, versions)))
    }
}

/// The versions a merge keeps of one key (newest first): the newest, and
/// each one a live endpoint sees (an endpoint generation `g` with
/// `version < g <= next newer version`). The predecessor goes on the oldest
/// kept version. With `base` (the output starts at commit 0), its initial
/// segment, before the first live endpoint, keeps live keys only, and no
/// version keeps a predecessor; later segments keep their tombstones.
pub fn retain(versions: &[Version], endpoints: &[u64], base: bool) -> Vec<Version> {
    let Some(oldest) = versions.last() else {
        return Vec::new();
    };
    let span_predecessor = oldest.predecessor;
    let mut kept: Vec<Version> = Vec::new();
    for (i, v) in versions.iter().enumerate() {
        let seen = i > 0
            && endpoints
                .iter()
                .any(|&g| v.generation < g && g <= versions[i - 1].generation);
        if i == 0 || seen {
            kept.push(Version {
                predecessor: None,
                ..v.clone()
            });
        }
    }
    if base {
        let first = endpoints.iter().copied().min().unwrap_or(u64::MAX);
        if kept
            .last()
            .is_some_and(|v| v.generation < first && v.deleted)
        {
            kept.pop();
        }
    } else if let Some(v) = kept.last_mut() {
        v.predecessor = span_predecessor;
    }
    kept
}

/// Adjacent spans (`runs`, newest first) merged into one span's files.
/// Returns them, and the entries of each segment: `segments[i]` counts the
/// kept versions with `i` of the (sorted) `endpoints` at or below their
/// generation.
pub fn merge(
    runs: &[Vec<&[u8]>],
    codecs: &[u8],
    endpoints: &[u64],
    base: bool,
    o: Options,
    max_file_bytes: usize,
) -> Result<(Vec<Vec<u8>>, Vec<u64>)> {
    let mut sorted = endpoints.to_vec();
    sorted.sort_unstable();
    let mut segments = vec![0u64; sorted.len() + 1];
    let mut g = Groups::new(runs, codecs)?;
    let mut w = Writer::new(o, max_file_bytes).repeating();
    while let Some((key, versions)) = g.next_group()? {
        for v in retain(&versions, &sorted, base) {
            segments[sorted.partition_point(|&e| e <= v.generation)] += 1;
            w.push(
                &key,
                v.generation,
                v.deleted,
                v.payload.as_deref(),
                v.predecessor,
            )?;
        }
    }
    w.finish(true)?;
    Ok((w.files.into_iter().collect(), segments))
}

pub const ADDED: u8 = 0;
pub const UPDATED: u8 = 1;
pub const REMOVED: u8 = 2;
pub const NEITHER: u8 = 3;

/// One key's change over `[P, N]`, from the versions of the spans
/// overlapping it (newest first), clipped to generations `[g_p, g_n1)`:
/// None if it has no version in the range; else its class, and its newest
/// version in the range (its state at N).
pub fn change(versions: &[Version], g_p: u64, g_n1: u64) -> Option<(u8, &Version)> {
    let at_n = versions.iter().find(|v| v.generation < g_n1)?;
    if at_n.generation < g_p {
        return None; // nothing in the range
    }
    let before = match versions.iter().find(|v| v.generation < g_p) {
        Some(v) => !v.deleted,
        None => versions.last().is_some_and(|v| v.predecessor.is_some()),
    };
    let class = match (before, !at_n.deleted) {
        (false, true) => ADDED,
        (true, true) => UPDATED,
        (true, false) => REMOVED,
        (false, false) => NEITHER,
    };
    Some((class, at_n))
}

/// A key's state at a reserved endpoint: its newest version older than
/// `g_bound` (`u64::MAX`: the head).
pub fn at(versions: &[Version], g_bound: u64) -> Option<&Version> {
    versions.iter().find(|v| v.generation < g_bound)
}

pub struct Page<T> {
    pub items: Vec<(Vec<u8>, T)>,
    /// The last key examined: where the next page starts.
    pub last: Option<Vec<u8>>,
    /// Whether keys below `bound` remain past `last`.
    pub more: bool,
    /// Keys examined that the page does not return (neither, or absent).
    pub skipped: u64,
}

/// A page of keys in `(after, bound)`, each mapped by `f` (None: skipped).
/// Every run must hold all of its blocks for that key window: below `bound`,
/// nothing a run holds is missing.
pub fn page<T>(
    runs: &[Vec<&[u8]>],
    codecs: &[u8],
    after: Option<&[u8]>,
    bound: Option<&[u8]>,
    limit: usize,
    mut f: impl FnMut(&[Version]) -> Option<T>,
) -> Result<Page<T>> {
    let mut g = Groups::new(runs, codecs)?;
    let mut out = Page {
        items: Vec::new(),
        last: None,
        more: false,
        skipped: 0,
    };
    while let Some((key, versions)) = g.next_group()? {
        if after.is_some_and(|a| key.as_slice() <= a) {
            continue;
        }
        if bound.is_some_and(|b| key.as_slice() >= b) {
            return Ok(out);
        }
        if out.items.len() == limit {
            out.more = true;
            return Ok(out);
        }
        match f(&versions) {
            Some(t) => out.items.push((key.clone(), t)),
            None => out.skipped += 1,
        }
        out.last = Some(key);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{file_blocks, CODEC_ZLIB};

    const O: Options = Options {
        block_size: 64,
        level: 1,
        bits_per_item: 14,
        k: 10,
        codec: CODEC_ZLIB,
    };

    fn v(generation: u64, deleted: bool, predecessor: Option<u64>) -> Version {
        Version {
            generation,
            deleted,
            payload: None,
            predecessor,
        }
    }

    fn blocks(files: &[Vec<u8>]) -> Vec<&[u8]> {
        let mut out = Vec::new();
        for f in files {
            let (_, bs) = file_blocks(f).unwrap();
            for b in bs {
                out.push(&f[b.offset as usize..(b.offset + b.size) as usize]);
            }
        }
        out
    }

    #[test]
    fn retain_keeps_what_endpoints_see() {
        // k changed at g10, g20 (removed), g30; endpoints at 15 and 25.
        let vs = vec![v(30, false, None), v(20, true, None), v(10, false, Some(5))];
        let kept = retain(&vs, &[15, 25], false);
        assert_eq!(
            kept,
            vec![v(30, false, None), v(20, true, None), v(10, false, Some(5))]
        );
        // Endpoint 15 retired: g10 is seen by nobody; the predecessor moves to g20.
        let kept = retain(&vs, &[25], false);
        assert_eq!(kept, vec![v(30, false, None), v(20, true, Some(5))]);
        // No endpoint inside: the newest alone, with the span's predecessor.
        assert_eq!(retain(&vs, &[], false), vec![v(30, false, Some(5))]);
    }

    #[test]
    fn base_normalizes_its_initial_segment_only() {
        // A12-1: added at 10, removed at 20, an endpoint at 15 inside the base.
        let vs = vec![v(20, true, None), v(10, false, None)];
        // Endpoint 15 sees g10: kept; the tombstone at 20 is in a later segment: kept.
        assert_eq!(
            retain(&vs, &[15], true),
            vec![v(20, true, None), v(10, false, None)]
        );
        // No endpoint: the tombstone is the initial segment's newest: dropped.
        assert_eq!(retain(&vs, &[], true), vec![]);
        // Endpoint at 5, before both: the initial segment is empty; the tombstone stays.
        assert_eq!(retain(&vs, &[5], true), vec![v(20, true, None)]);
    }

    #[test]
    fn change_is_clipped_to_its_range() {
        // A12-2: k added at 10 (no pred), removed at 20; changes over [10, 20).
        let vs = vec![v(20, true, None), v(10, false, None)];
        assert_eq!(change(&vs, 10, 20).map(|c| c.0), Some(ADDED));
        assert_eq!(change(&vs, 10, u64::MAX).map(|c| c.0), Some(NEITHER));
        assert_eq!(change(&vs, 20, u64::MAX).map(|c| c.0), Some(REMOVED));
        // First touched after N: not in the range.
        assert_eq!(change(&vs, 0, 10), None);
    }

    #[test]
    fn versions_split_across_blocks_and_files() {
        // One key with many versions, written with tiny blocks and files.
        let o = O;
        let mut w = Writer::new(o, 200).repeating();
        for g in (1..=60u64).rev() {
            w.push(
                b"hot",
                g * 10,
                g % 3 == 0,
                None,
                if g == 1 { Some(1) } else { None },
            )
            .unwrap();
        }
        w.push(b"zz", 5, false, None, None).unwrap();
        w.finish(true).unwrap();
        let files: Vec<Vec<u8>> = w.files.into_iter().collect();
        assert!(files.len() > 1);
        let bs = blocks(&files);
        let runs = vec![bs];
        let mut g = Groups::new(&runs, &[CODEC_ZLIB]).unwrap();
        let (k, vs) = g.next_group().unwrap().unwrap();
        assert_eq!(k, b"hot");
        assert_eq!(vs.len(), 60);
        assert_eq!(vs.last().unwrap().predecessor, Some(1));
        assert_eq!(g.next_group().unwrap().unwrap().0, b"zz");
        assert!(g.next_group().unwrap().is_none());
    }

    #[test]
    fn writer_refuses_misordered_versions() {
        let mut w = Writer::new(O, usize::MAX).repeating();
        w.push(b"a", 20, false, None, None).unwrap();
        assert!(w.push(b"a", 20, false, None, None).is_err());
    }
}
