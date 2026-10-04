//! Spans (docs/key-index-design.md § The structure): a span's files hold
//! several versions of a key, newest first, with the predecessor on the
//! oldest. A merge keeps the versions live endpoints see (`retain`; the
//! streaming job is `jobs::SpanMerge`); readers clip every key to a
//! generation range (`change`, `at`), a page of keys at a time over the
//! blocks a page needs (`page`).
//!
//! A run is one span: its files' blocks in key order, which may split a
//! key's versions across blocks and files. Runs are given newest first, so
//! a key's versions, read run by run, come out newest first.

use std::sync::Arc;

use crate::format::{Error, Options, Result};
use crate::jobs::{SpanMerge, Step};
use crate::stream::{Block, Bytes, Segment};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version {
    pub generation: u64,
    pub deleted: bool,
    pub payload: Option<Vec<u8>>,
    pub predecessor: Option<u64>,
    /// The predecessor's payload, on a payload-bearing index.
    pub prior: Option<Vec<u8>>,
}

/// A run's blocks in key order, decoded one at a time; None at its end.
pub type Blocks<'a> = Box<dyn FnMut() -> Result<Option<Block>> + 'a>;

/// One run's blocks, decoded as the cursor reaches them.
struct Run<'a> {
    blocks: Blocks<'a>,
    block: Option<Block>,
    i: usize,
}

impl<'a> Run<'a> {
    fn new(blocks: Blocks<'a>) -> Result<Run<'a>> {
        let mut r = Run {
            blocks,
            block: None,
            i: 0,
        };
        r.fill()?;
        Ok(r)
    }

    /// Moves to the next non-empty block when the current one is used up.
    fn fill(&mut self) -> Result<()> {
        while self.block.as_ref().is_none_or(|b| self.i >= b.len()) {
            self.block = (self.blocks)()?;
            self.i = 0;
            if self.block.is_none() {
                return Ok(());
            }
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
            prior: b.prior(self.i).and_then(|(_, p)| p).map(<[u8]>::to_vec),
        };
        self.i += 1;
        self.fill()?;
        Ok(v)
    }
}

/// One key's versions, newest first, taken one at a time: what a reader
/// or a merge keeps of a key is its fold's state, never every version the
/// key holds (A17 R10: one hot key may hold a version per live endpoint).
pub trait Fold {
    fn push(&mut self, v: Version) -> Result<()>;
}

/// Versions passed over: a key a page skips.
pub struct Skip;

impl Fold for Skip {
    fn push(&mut self, _: Version) -> Result<()> {
        Ok(())
    }
}

/// Every version, collected: tests, and folds over a key's whole history.
pub struct Collect(pub Vec<Version>);

impl Fold for Collect {
    fn push(&mut self, v: Version) -> Result<()> {
        self.0.push(v);
        Ok(())
    }
}

/// Runs merged by key: each key once, its versions streamed newest first.
pub struct Groups<'a> {
    runs: Vec<Run<'a>>,
}

impl<'a> Groups<'a> {
    /// Runs of encoded blocks, each run in its codec.
    pub fn new(runs: &'a [Vec<&'a [u8]>], codecs: &[u8]) -> Result<Groups<'a>> {
        if runs.len() != codecs.len() {
            return Err(Error::Value("a codec per run".into()));
        }
        Groups::of(
            runs.iter()
                .zip(codecs)
                .map(|(blocks, &codec)| {
                    let mut next = blocks.iter();
                    Box::new(move || next.next().map(|b| Block::decode(b, codec)).transpose())
                        as Blocks<'a>
                })
                .collect(),
        )
    }

    /// Runs of blocks from anywhere: a snapshot's local files.
    pub fn of(runs: Vec<Blocks<'a>>) -> Result<Groups<'a>> {
        let runs = runs.into_iter().map(Run::new).collect::<Result<_>>()?;
        Ok(Groups { runs })
    }

    /// The next key, or None at the end.
    pub fn peek(&self) -> Option<Vec<u8>> {
        self.runs
            .iter()
            .filter_map(Run::key)
            .min()
            .map(<[u8]>::to_vec)
    }

    /// The next key, its versions pushed into `fold` newest first across the
    /// runs (their order checked); None at the end.
    pub fn next_into(&mut self, fold: &mut impl Fold) -> Result<Option<Vec<u8>>> {
        let Some(key) = self.peek() else {
            return Ok(None);
        };
        let mut prev: Option<u64> = None;
        for r in &mut self.runs {
            while r.key() == Some(key.as_slice()) {
                let v = r.take()?;
                if let Some(p) = prev.filter(|&p| v.generation >= p) {
                    return Err(Error::Format(format!(
                        "versions of {:?} out of order: {} after {}",
                        String::from_utf8_lossy(&key),
                        v.generation,
                        p
                    )));
                }
                prev = Some(v.generation);
                fold.push(v)?;
            }
        }
        Ok(Some(key))
    }

    /// The next key and every version of it.
    pub fn next_group(&mut self) -> Result<Option<(Vec<u8>, Vec<Version>)>> {
        let mut all = Collect(Vec::new());
        Ok(self.next_into(&mut all)?.map(|k| (k, all.0)))
    }
}

/// The versions a merge keeps of one key (newest first): the newest, and
/// each one a live endpoint sees (an endpoint generation `g` with
/// `version < g <= next newer version`). The predecessor, with its payload,
/// goes on the oldest kept version. With `base` (the output starts at commit
/// 0), its initial segment, before the first live endpoint, keeps live keys
/// only, and no version keeps a predecessor; later segments keep their
/// tombstones. One version at a time: only the last kept one waits, for the
/// predecessor only the key's end can tell.
pub struct Retainer {
    endpoints: Vec<u64>,
    base: bool,
    drop_absent: bool,
    newer: Option<u64>,
    pending: Option<Version>,
    oldest: (Option<u64>, Option<Vec<u8>>),
}

impl Retainer {
    pub fn new(mut endpoints: Vec<u64>, base: bool) -> Retainer {
        endpoints.sort_unstable();
        endpoints.dedup();
        Retainer {
            endpoints,
            base,
            drop_absent: false,
            newer: None,
            pending: None,
            oldest: (None, None),
        }
    }

    /// Drop a key that is a tombstone naming no predecessor: absent before
    /// and after (the two views' net merge; spans keep it, A12-1).
    pub fn drop_absent(&mut self) {
        self.drop_absent = true;
    }

    /// The (sorted) endpoints it keeps versions for.
    pub fn endpoints(&self) -> &[u64] {
        &self.endpoints
    }

    /// The next version (newest first): a kept version now final, if any.
    pub fn push(&mut self, mut v: Version) -> Option<Version> {
        let seen = match self.newer {
            None => true,
            Some(n) => {
                // An endpoint g with v < g <= n: the first endpoint above v.
                let i = self.endpoints.partition_point(|&g| g <= v.generation);
                self.endpoints.get(i).is_some_and(|&g| g <= n)
            }
        };
        self.newer = Some(v.generation);
        self.oldest = (v.predecessor.take(), v.prior.take());
        if !seen {
            return None;
        }
        self.pending.replace(v)
    }

    /// The key's last kept version, with the predecessor of its oldest.
    pub fn finish(&mut self) -> Option<Version> {
        let mut v = self.pending.take()?;
        let oldest = std::mem::take(&mut self.oldest);
        self.newer = None;
        if self.base {
            let first = self.endpoints.first().copied();
            if v.deleted && first.is_none_or(|f| v.generation < f) {
                return None;
            }
        } else {
            (v.predecessor, v.prior) = oldest;
            if self.drop_absent && v.deleted && v.predecessor.is_none() {
                return None;
            }
        }
        Some(v)
    }
}

/// `Retainer` over a key's whole history (`endpoints` in any order).
pub fn retain(versions: &[Version], endpoints: &[u64], base: bool) -> Vec<Version> {
    let mut r = Retainer::new(endpoints.to_vec(), base);
    let mut kept: Vec<Version> = versions.iter().filter_map(|v| r.push(v.clone())).collect();
    kept.extend(r.finish());
    kept
}

/// Adjacent spans (`runs`, newest first, held whole) merged into one span's
/// files, through the streaming job. Returns them, and the entries of each
/// segment (`jobs::SpanMerge::segments`).
pub fn merge(
    runs: &[Vec<&[u8]>],
    codecs: &[u8],
    endpoints: &[u64],
    base: bool,
    o: Options,
    max_file_bytes: usize,
) -> Result<(Vec<Vec<u8>>, Vec<u64>)> {
    if runs.len() != codecs.len() {
        return Err(Error::Value("a codec per run".into()));
    }
    let mut job = SpanMerge::new(runs.len(), endpoints.to_vec(), base, o, max_file_bytes);
    for ((r, blocks), &codec) in runs.iter().enumerate().zip(codecs) {
        let mut data = Vec::new();
        let mut metas = Vec::new();
        for b in blocks {
            metas.push((data.len(), b.len(), crc32fast::hash(b)));
            data.extend_from_slice(b);
        }
        let data: Bytes = Arc::new(data);
        for meta in metas {
            job.merge.runs[r].feed(Segment {
                data: data.clone(),
                blocks: vec![meta],
                codec,
            });
        }
        job.merge.runs[r].end();
    }
    let mut files = Vec::new();
    loop {
        match job.step()? {
            Step::File => files.extend(job.writer.files.pop_front()),
            Step::Done => break,
            _ => unreachable!("every run is fed whole"),
        }
    }
    Ok((files, job.segments))
}

pub const ADDED: u8 = 0;
pub const UPDATED: u8 = 1;
pub const REMOVED: u8 = 2;
pub const NEITHER: u8 = 3;

/// One key's change over `[P, N]`, from the versions of the spans
/// overlapping it (newest first), clipped to generations `[g_p, g_n1)`
/// (`g_n1` None: the head): None if it has no version in the range; else
/// its class, and its newest version in the range (its state at N). Classes
/// are by presence at the two ends — the net rule: a key absent at both is
/// neither, and so is one live at both with equal payloads (a source key
/// back at the version it had before P). Without payloads, live at both
/// ends is updated: every write of a derived output is a change. One version
/// at a time: it keeps the state at N, the state before P, and what the
/// oldest version replaced.
pub struct Changed {
    g_p: u64,
    g_n1: Option<u64>,
    at_n: Option<Version>,
    before: Option<(bool, Option<Vec<u8>>)>,
    oldest: (Option<u64>, Option<Vec<u8>>),
}

impl Changed {
    pub fn new(g_p: u64, g_n1: Option<u64>) -> Changed {
        Changed {
            g_p,
            g_n1,
            at_n: None,
            before: None,
            oldest: (None, None),
        }
    }

    pub fn finish(self) -> Option<(u8, Version)> {
        let at_n = self.at_n?;
        if at_n.generation < self.g_p {
            return None; // nothing in the range
        }
        // The state before P: its version, else what the oldest version replaced.
        let (before, payload) = match self.before {
            Some(b) => b,
            None => match self.oldest {
                (Some(_), prior) => (true, prior),
                _ => (false, None),
            },
        };
        let class = match (before, !at_n.deleted) {
            (false, true) => ADDED,
            (true, true) if payload.is_some() && payload == at_n.payload => NEITHER,
            (true, true) => UPDATED,
            (true, false) => REMOVED,
            (false, false) => NEITHER,
        };
        Some((class, at_n))
    }
}

impl Fold for Changed {
    fn push(&mut self, mut v: Version) -> Result<()> {
        self.oldest = (v.predecessor, v.prior.take());
        if self.before.is_none() && v.generation < self.g_p {
            self.before = Some((!v.deleted, v.payload.clone()));
        }
        if self.at_n.is_none() && older(v.generation, self.g_n1) {
            self.at_n = Some(v);
        }
        Ok(())
    }
}

/// `Changed` over a key's whole history.
pub fn change(versions: &[Version], g_p: u64, g_n1: Option<u64>) -> Option<(u8, Version)> {
    let mut c = Changed::new(g_p, g_n1);
    for v in versions {
        c.push(v.clone()).expect("a fold");
    }
    c.finish()
}

/// A key's state at a reserved endpoint: its newest version older than
/// `bound` (None: the head), one version at a time.
pub struct At {
    bound: Option<u64>,
    pub found: Option<Version>,
}

impl At {
    pub fn new(bound: Option<u64>) -> At {
        At { bound, found: None }
    }
}

impl Fold for At {
    fn push(&mut self, v: Version) -> Result<()> {
        if self.found.is_none() && older(v.generation, self.bound) {
            self.found = Some(v);
        }
        Ok(())
    }
}

/// Whether generation `g` lies below `bound`; None bounds nothing (the
/// head). `Some(u64::MAX)` is an exclusive bound like any other (A17 R9).
pub fn older(g: u64, bound: Option<u64>) -> bool {
    bound.is_none_or(|b| g < b)
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

/// A page of keys in `(after, bound)`, each folded by a fold from `new`
/// and mapped by `finish` (None: skipped). Every run must hold all of its
/// blocks for that key window: below `bound`, nothing a run holds is missing.
pub fn page<T, F: Fold>(
    runs: &[Vec<&[u8]>],
    codecs: &[u8],
    after: Option<&[u8]>,
    bound: Option<&[u8]>,
    limit: usize,
    new: impl FnMut() -> F,
    finish: impl FnMut(F) -> Option<T>,
) -> Result<Page<T>> {
    page_of(Groups::new(runs, codecs)?, after, bound, limit, new, finish)
}

/// `page`, over any runs.
pub fn page_of<T, F: Fold>(
    mut g: Groups<'_>,
    after: Option<&[u8]>,
    bound: Option<&[u8]>,
    limit: usize,
    mut new: impl FnMut() -> F,
    mut finish: impl FnMut(F) -> Option<T>,
) -> Result<Page<T>> {
    let mut out = Page {
        items: Vec::new(),
        last: None,
        more: false,
        skipped: 0,
    };
    while let Some(key) = g.peek() {
        if after.is_some_and(|a| key.as_slice() <= a) {
            g.next_into(&mut Skip)?;
            continue;
        }
        if bound.is_some_and(|b| key.as_slice() >= b) {
            return Ok(out);
        }
        if out.items.len() == limit {
            out.more = true;
            return Ok(out);
        }
        let mut f = new();
        g.next_into(&mut f)?;
        match finish(f) {
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
    use crate::stream::Writer;

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
            prior: None,
        }
    }

    fn p(generation: u64, payload: &[u8], prior: Option<&[u8]>) -> Version {
        Version {
            generation,
            deleted: false,
            payload: Some(payload.to_vec()),
            predecessor: prior.map(|_| 1),
            prior: prior.map(<[u8]>::to_vec),
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
        assert_eq!(change(&vs, 10, Some(20)).map(|c| c.0), Some(ADDED));
        assert_eq!(change(&vs, 10, None).map(|c| c.0), Some(NEITHER));
        assert_eq!(change(&vs, 20, None).map(|c| c.0), Some(REMOVED));
        // First touched after N: not in the range.
        assert_eq!(change(&vs, 0, Some(10)), None);
    }

    #[test]
    fn a_source_key_back_at_its_version_is_neither() {
        // k: v1 before P (commit 0, gen 1); v2 at gen 10, v1 again at gen 20.
        let held = [p(20, b"v1", None), p(10, b"v2", None), p(1, b"v1", None)];
        assert_eq!(change(&held, 5, None).unwrap().0, NEITHER);
        assert_eq!(change(&held, 5, Some(15)).unwrap().0, UPDATED); // at N it is v2
                                                                    // The version before P merged out of the spans read: its payload rides the predecessor.
        let merged = [p(20, b"v1", None), p(10, b"v2", Some(b"v1"))];
        assert_eq!(change(&merged, 5, None).unwrap().0, NEITHER);
        let moved = [p(20, b"v3", None), p(10, b"v2", Some(b"v1"))];
        assert_eq!(change(&moved, 5, None).unwrap().0, UPDATED);
        // A merge keeps the payload with the oldest kept version's predecessor.
        let kept = retain(&merged, &[], false);
        assert_eq!(kept.len(), 1);
        assert_eq!(
            (kept[0].predecessor, kept[0].prior.as_deref()),
            (Some(1), Some(&b"v1"[..]))
        );
        // Derived outputs carry no payload: live at both ends is updated.
        let derived = [v(20, false, None), v(10, false, Some(1))];
        assert_eq!(change(&derived, 5, None).unwrap().0, UPDATED);
    }

    #[test]
    fn an_explicit_maximum_bound_is_not_the_head() {
        // A17 R9: k at u64::MAX - 1 (commit 0), then at u64::MAX (commit 1):
        // below the bound u64::MAX lies the first, at the head the second.
        let vs = [v(u64::MAX, false, None), v(u64::MAX - 1, false, None)];
        assert!(!older(u64::MAX, Some(u64::MAX)) && older(u64::MAX, None));
        let mut at = At::new(Some(u64::MAX));
        for x in &vs {
            at.push(x.clone()).unwrap();
        }
        assert_eq!(at.found.map(|x| x.generation), Some(u64::MAX - 1));
        assert_eq!(
            change(&vs, 0, Some(u64::MAX)).map(|c| c.1.generation),
            Some(u64::MAX - 1)
        );
        assert_eq!(change(&vs, 0, None).map(|c| c.1.generation), Some(u64::MAX));
    }

    #[test]
    fn retaining_a_version_at_a_time_keeps_what_the_whole_history_would() {
        // 300 versions, endpoints at every 7th generation: the streamed
        // retention holds one pending version, and keeps what a pass over the
        // whole history keeps.
        let vs: Vec<Version> = (1..=300u64)
            .rev()
            .map(|g| v(g * 10, g % 5 == 0, if g == 1 { Some(3) } else { None }))
            .collect();
        let ends: Vec<u64> = (1..=300u64)
            .filter(|g| g % 7 == 0)
            .map(|g| g * 10 + 5)
            .collect();
        for base in [false, true] {
            let kept = retain(&vs, &ends, base);
            let mut want: Vec<Version> = Vec::new();
            for (i, x) in vs.iter().enumerate() {
                if i == 0
                    || ends
                        .iter()
                        .any(|&e| x.generation < e && e <= vs[i - 1].generation)
                {
                    want.push(Version {
                        predecessor: None,
                        prior: None,
                        ..x.clone()
                    });
                }
            }
            if base {
                let first = ends.iter().min().copied().unwrap();
                if want
                    .last()
                    .is_some_and(|x| x.deleted && x.generation < first)
                {
                    want.pop();
                }
            } else {
                want.last_mut().unwrap().predecessor = Some(3);
            }
            assert_eq!(kept, want);
        }
    }

    #[test]
    fn prior_payloads_round_trip_through_blocks() {
        let mut w = Writer::new(O, usize::MAX);
        w.push(b"a", 9, false, Some(b"v2"), Some((3, Some(b"v1"))))
            .unwrap();
        w.push(b"b", 9, true, None, Some((4, None))).unwrap();
        w.finish(true).unwrap();
        let files: Vec<Vec<u8>> = w.files.into_iter().collect();
        let b = Block::decode(blocks(&files)[0], O.codec).unwrap();
        assert_eq!(b.prior(0), Some((3, Some(&b"v1"[..]))));
        assert_eq!(b.prior(1), Some((4, None)));
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
                if g == 1 { Some((1, None)) } else { None },
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
