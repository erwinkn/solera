//! Reading layers: open a layer (its top level and the segments a read
//! needs), plan the blocks, fetch them in batches that double from 256 KiB to
//! `max_get` with one batch in flight ahead, decode; and walk several layers at
//! once, merged by key, newest layer first.

use crate::error::{Error, Result};
use crate::format::{parse_segment, Decoder, LayerRef};
use crate::glob::Pattern;
use crate::run::Run;
use crate::store::{Cached, Part, Store, Want};
use bytes::Bytes;
use std::collections::VecDeque;
use std::ops::{Bound, Range};
use std::sync::Arc;

const FIRST: u64 = 256 << 10;
/// A read of listed keys launches its batches together, each about this size,
/// cut only where a GET would end anyway.
const ALL: u64 = 4 << 20;
/// A read of listed keys keeps at most this many bytes fetched ahead of its
/// decoding, per layer.
const WINDOW: u64 = 16 << 20;
/// Opening a layer fetches the index segments it needs about this many
/// compressed bytes at a time.
const SEG_BATCH: u64 = 16 << 20;

/// The blocks of one batch, being fetched.
type Fetch = tokio::task::JoinHandle<Result<Vec<Cached>>>;

/// Sorted, distinct keys, back to back.
#[derive(Default, Clone, Debug)]
pub struct KeyList {
    buf: Vec<u8>,
    end: Vec<usize>,
}

impl KeyList {
    /// The keys, sorted and deduplicated.
    pub fn new<K: AsRef<[u8]>>(keys: &[K]) -> KeyList {
        let mut ix: Vec<usize> = (0..keys.len()).collect();
        ix.sort_unstable_by(|&a, &b| keys[a].as_ref().cmp(keys[b].as_ref()));
        ix.dedup_by(|a, b| keys[*a].as_ref() == keys[*b].as_ref());
        let mut l = KeyList::default();
        for i in ix {
            l.buf.extend_from_slice(keys[i].as_ref());
            l.end.push(l.buf.len());
        }
        l
    }

    pub fn len(&self) -> usize {
        self.end.len()
    }

    pub fn is_empty(&self) -> bool {
        self.end.is_empty()
    }

    pub fn get(&self, i: usize) -> &[u8] {
        let a = if i == 0 { 0 } else { self.end[i - 1] };
        &self.buf[a..self.end[i]]
    }
}

/// Key bounds of a read.
#[derive(Clone, Debug)]
pub(crate) struct Bounds {
    pub lo: Bound<Vec<u8>>,
    pub hi: Bound<Vec<u8>>,
}

impl Bounds {
    pub fn all() -> Bounds {
        Bounds {
            lo: Bound::Unbounded,
            hi: Bound::Unbounded,
        }
    }

    #[inline]
    pub fn below(&self, k: &[u8]) -> bool {
        match &self.lo {
            Bound::Unbounded => false,
            Bound::Included(lo) => k < lo.as_slice(),
            Bound::Excluded(lo) => k <= lo.as_slice(),
        }
    }

    #[inline]
    pub fn above(&self, k: &[u8]) -> bool {
        match &self.hi {
            Bound::Unbounded => false,
            Bound::Included(hi) => k > hi.as_slice(),
            Bound::Excluded(hi) => k >= hi.as_slice(),
        }
    }

    fn lo_key(&self) -> Option<&[u8]> {
        match &self.lo {
            Bound::Unbounded => None,
            Bound::Included(k) | Bound::Excluded(k) => Some(k),
        }
    }

    fn hi_key(&self) -> Option<&[u8]> {
        match &self.hi {
            Bound::Unbounded => None,
            Bound::Included(k) | Bound::Excluded(k) => Some(k),
        }
    }

    pub fn is_empty(&self) -> bool {
        match (&self.lo, &self.hi) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
            (Bound::Included(a), Bound::Included(b)) => a > b,
            (Bound::Included(a) | Bound::Excluded(a), Bound::Excluded(b))
            | (Bound::Excluded(a), Bound::Included(b)) => a >= b,
        }
    }

    /// Whether a layer whose keys are `first..=last` may hold a key in bounds.
    pub fn overlaps(&self, l: &LayerRef) -> bool {
        l.entries > 0 && !self.below(&l.last_key) && !self.above(&l.first_key)
    }

    /// Raise the lower bound to `b` if it is higher.
    pub fn raise(&mut self, b: Bound<Vec<u8>>) {
        let higher = match (&self.lo, &b) {
            (_, Bound::Unbounded) => false,
            (Bound::Unbounded, _) => true,
            (Bound::Included(a) | Bound::Excluded(a), Bound::Included(n)) => n > a,
            (Bound::Included(a), Bound::Excluded(n)) => n >= a,
            (Bound::Excluded(a), Bound::Excluded(n)) => n > a,
        };
        if higher {
            self.lo = b;
        }
    }

    /// Lower the upper bound to `b` if it is lower.
    pub fn lower(&mut self, b: Bound<Vec<u8>>) {
        let lower = match (&self.hi, &b) {
            (_, Bound::Unbounded) => false,
            (Bound::Unbounded, _) => true,
            (Bound::Included(a) | Bound::Excluded(a), Bound::Included(n)) => n < a,
            (Bound::Included(a), Bound::Excluded(n)) => n <= a,
            (Bound::Excluded(a), Bound::Excluded(n)) => n < a,
        };
        if lower {
            self.hi = b;
        }
    }
}

#[derive(Clone, Copy)]
struct BlockRef {
    off: u64,
    clen: u32,
    rlen: u32,
    rows: u32,
    check: u32,
}

/// One layer's keys in order, its blocks fetched a batch ahead.
pub(crate) struct Cursor {
    store: Arc<Store>,
    path: Arc<str>,
    blocks: Vec<BlockRef>,
    next: usize,
    target: u64,
    /// A read of listed keys: batches are launched together, up to
    /// `WINDOW` bytes ahead, and decoded as they arrive.
    all: bool,
    /// Bytes launched and not yet taken.
    ahead: u64,
    /// A read of listed keys small enough to keep in the memory cache.
    keep_all: bool,
    pending: VecDeque<(Range<usize>, u64, Fetch)>,
    ready: VecDeque<(Bytes, BlockRef)>,
    run: Run,
    pos: usize,
    dec: Decoder,
    bounds: Arc<Bounds>,
}

impl Drop for Cursor {
    fn drop(&mut self) {
        for (.., h) in &self.pending {
            h.abort();
        }
    }
}

impl Cursor {
    /// Open a layer for a read of `bounds`, or of `keys` only.
    pub(crate) async fn open(
        store: &Arc<Store>,
        l: &LayerRef,
        bounds: Arc<Bounds>,
        keys: Option<&KeyList>,
        all_at_once: bool,
    ) -> Result<Cursor> {
        let top = store.top(l).await?;
        let path: Arc<str> = Arc::from(l.path.as_str());
        let in_bounds = |i: usize| {
            let k = keys.map_or(&[][..], |k| k.get(i));
            !bounds.below(k) && !bounds.above(k)
        };
        // the listed keys in bounds, with the segment that may hold each
        let listed: Vec<(usize, usize)> = keys.map_or(Vec::new(), |k| {
            (0..k.len())
                .filter(|&i| in_bounds(i))
                .filter_map(|i| Some((i, top.segs.holding(k.get(i))?)))
                .collect()
        });
        let segs: Vec<usize> = match keys {
            None => top.segs.span(bounds.lo_key(), bounds.hi_key()).collect(),
            Some(_) => {
                let mut v: Vec<usize> = listed.iter().map(|&(_, s)| s).collect();
                v.dedup();
                v
            }
        };
        // Segments the cache does not hold are fetched as compressed bytes,
        // SEG_BATCH at a time, and decoded one at a time, so a read holds a
        // bounded part of its index, however many segments it spans.
        let s = &top.segs;
        let mut blocks: Vec<BlockRef> = Vec::new();
        let mut at = 0;
        let mut from = 0;
        while from < segs.len() {
            let (mut cached, mut wants, mut bytes) = (Vec::new(), Vec::new(), 0);
            let mut to = from;
            while to < segs.len() && bytes < SEG_BATCH {
                let i = segs[to];
                let c = store.cached_seg(&path, i);
                if c.is_none() {
                    bytes += s.clen[i] as u64;
                    wants.push((
                        Part::Block(s.off[i]),
                        s.off[i]..s.off[i] + s.clen[i] as u64,
                        Want::Block {
                            check: s.check[i],
                            keep: false,
                        },
                    ));
                }
                cached.push(c);
                to += 1;
            }
            let mut fetched = store.load(&path, wants).await?.into_iter();
            for (&i, c) in segs[from..to].iter().zip(cached) {
                let ix = match c {
                    Some(ix) => ix,
                    None => {
                        let Some(Cached::Block(b)) = fetched.next() else {
                            unreachable!()
                        };
                        let ix = Arc::new(
                            parse_segment(&b, s.rlen[i], s.check[i], s.n[i], top.commits_start)
                                .map_err(|e| match e {
                                    Error::Format(m) => Error::Format(format!("{path}: {m}")),
                                    e => e,
                                })?,
                        );
                        store.remember_seg(&path, i, ix.clone());
                        ix
                    }
                };
                let mut push = |b: usize| {
                    blocks.push(BlockRef {
                        off: ix.off[b],
                        clen: ix.clen[b],
                        rlen: ix.rlen[b],
                        rows: ix.n[b],
                        check: ix.check[b],
                    })
                };
                match keys {
                    None => ix
                        .span(bounds.lo_key(), bounds.hi_key())
                        .for_each(&mut push),
                    Some(k) => {
                        let mut last = None;
                        while at < listed.len() && listed[at].1 == i {
                            let b = ix.holding(k.get(listed[at].0));
                            if let Some(b) = b.filter(|&b| Some(b) != last) {
                                push(b);
                                last = Some(b);
                            }
                            at += 1;
                        }
                    }
                }
            }
            from = to;
        }
        let keep_all = blocks.iter().map(|b| b.clen as u64).sum::<u64>() <= store.opts.cache_batch;
        Ok(Cursor {
            store: store.clone(),
            path,
            blocks,
            next: 0,
            target: if all_at_once { ALL } else { FIRST },
            all: all_at_once,
            ahead: 0,
            keep_all,
            pending: VecDeque::new(),
            ready: VecDeque::new(),
            run: Run::new(),
            pos: 0,
            dec: Decoder::default(),
            bounds,
        })
    }

    fn launch(&mut self) {
        let a = self.next;
        let (gap, max_get) = (self.store.opts.coalesce, self.store.opts.max_get);
        // block bytes, and what the GETs fetch and hold: the blocks and the
        // gaps they coalesce across
        let (mut bytes, mut held) = (0u64, 0u64);
        while self.next < self.blocks.len() {
            let b = &self.blocks[self.next];
            let between = (self.next > a)
                .then(|| {
                    let p = &self.blocks[self.next - 1];
                    b.off.saturating_sub(p.off + p.clen as u64)
                })
                .filter(|&g| g <= gap);
            let more = between.unwrap_or(0) + b.clen as u64;
            if self.next > a
                && ((bytes >= self.target && !(self.all && between.is_some()))
                    || held + more > max_get)
            {
                break;
            }
            bytes += b.clen as u64;
            held += more;
            self.next += 1;
        }
        if self.next == a {
            return;
        }
        self.ahead += held;
        let keep = if self.all {
            self.keep_all
        } else {
            self.target = (self.target * 2).min(max_get);
            bytes <= self.store.opts.cache_batch
        };
        let wants: Vec<(Part, Range<u64>, Want)> = self.blocks[a..self.next]
            .iter()
            .map(|b| {
                (
                    Part::Block(b.off),
                    b.off..b.off + b.clen as u64,
                    Want::Block {
                        check: b.check,
                        keep,
                    },
                )
            })
            .collect();
        let (store, path) = (self.store.clone(), self.path.clone());
        let h = self
            .store
            .rt
            .spawn(async move { store.load(&path, wants).await });
        self.pending.push_back((a..self.next, held, h));
    }

    /// Make the current key valid; false once the layer has no more.
    pub(crate) async fn fill(&mut self) -> Result<bool> {
        loop {
            if self.pos < self.run.len() {
                return Ok(true);
            }
            if let Some((c, b)) = self.ready.pop_front() {
                self.dec
                    .decode(&c, b.rlen, b.rows, &mut self.run)
                    .map_err(|e| match e {
                        Error::Format(m) => {
                            Error::Format(format!("{}: block at {}: {m}", self.path, b.off))
                        }
                        e => e,
                    })?;
                self.pos = 0;
                while self.pos < self.run.len() && self.bounds.below(self.run.key(self.pos)) {
                    self.pos += 1;
                }
                continue;
            }
            if self.pending.is_empty() {
                self.launch();
            }
            while self.all && self.next < self.blocks.len() && self.ahead < WINDOW {
                self.launch();
            }
            let Some((span, held, h)) = self.pending.pop_front() else {
                return Ok(false);
            };
            let got = h
                .await
                .map_err(|e| Error::Store(format!("fetch task: {e}").into()))??;
            self.ahead -= held;
            if !self.all {
                self.launch();
            }
            for (i, c) in span.zip(got) {
                let Cached::Block(bytes) = c else {
                    unreachable!()
                };
                self.ready.push_back((bytes, self.blocks[i]));
            }
        }
    }

    #[inline]
    pub(crate) fn key(&self) -> &[u8] {
        self.run.key(self.pos)
    }

    /// Move to the first key at or after `target`, searching each decoded
    /// block instead of stepping through it; false once the layer has none.
    async fn seek(&mut self, target: &[u8]) -> Result<bool> {
        loop {
            let (mut lo, mut hi) = (self.pos, self.run.len());
            while lo < hi {
                let m = (lo + hi) / 2;
                if self.run.key(m) < target {
                    lo = m + 1;
                } else {
                    hi = m;
                }
            }
            self.pos = lo;
            if !self.fill().await? {
                return Ok(false);
            }
            if self.key() >= target {
                return Ok(true);
            }
        }
    }
}

/// Several layers merged by key: each key once, with its entries from every
/// layer, newest first.
pub(crate) struct Walk {
    curs: Vec<Cursor>,
    heap: Vec<usize>,
    bounds: Arc<Bounds>,
    pattern: Option<Pattern>,
    keys: Option<KeyList>,
    kp: usize,
}

impl Walk {
    /// `layers` newest first.
    pub(crate) async fn open(
        store: &Arc<Store>,
        layers: &[&LayerRef],
        bounds: Bounds,
        keys: Option<KeyList>,
        pattern: Option<Pattern>,
        all_at_once: bool,
    ) -> Result<Walk> {
        let bounds = Arc::new(bounds);
        let mut curs = futures::future::try_join_all(
            layers
                .iter()
                .map(|l| Cursor::open(store, l, bounds.clone(), keys.as_ref(), all_at_once)),
        )
        .await?;
        let live = futures::future::try_join_all(curs.iter_mut().map(|c| c.fill())).await?;
        let mut w = Walk {
            curs,
            heap: Vec::new(),
            bounds,
            pattern,
            keys,
            kp: 0,
        };
        for (i, live) in live.into_iter().enumerate() {
            if live {
                w.push(i);
            }
        }
        Ok(w)
    }

    #[inline]
    fn less(&self, a: usize, b: usize) -> bool {
        (self.curs[a].key(), a) < (self.curs[b].key(), b)
    }

    fn push(&mut self, i: usize) {
        self.heap.push(i);
        let mut k = self.heap.len() - 1;
        while k > 0 {
            let p = (k - 1) / 2;
            if self.less(self.heap[k], self.heap[p]) {
                self.heap.swap(k, p);
                k = p;
            } else {
                break;
            }
        }
    }

    /// Remove the top cursor (it has no more keys).
    fn pop(&mut self) {
        let n = self.heap.len();
        self.heap.swap(0, n - 1);
        self.heap.pop();
        self.sift_down();
    }

    /// The top cursor's key grew: move it down to its place.
    fn sift_down(&mut self) {
        let n = self.heap.len();
        let mut k = 0;
        loop {
            let (l, r) = (2 * k + 1, 2 * k + 2);
            let mut m = k;
            if l < n && self.less(self.heap[l], self.heap[m]) {
                m = l;
            }
            if r < n && self.less(self.heap[r], self.heap[m]) {
                m = r;
            }
            if m == k {
                break;
            }
            self.heap.swap(k, m);
            k = m;
        }
    }

    /// The next key the read takes, with its entries, into `group` (cleared
    /// first); false at the end.
    pub(crate) async fn next(&mut self, group: &mut Run) -> Result<bool> {
        loop {
            group.clear();
            let Some(&top) = self.heap.first() else {
                return Ok(false);
            };
            let key = self.curs[top].key();
            if self.bounds.above(key) {
                return Ok(false);
            }
            if let Some(keys) = &self.keys {
                while self.kp < keys.len() && keys.get(self.kp) < key {
                    self.kp += 1;
                }
                if self.kp == keys.len() {
                    return Ok(false);
                }
                if key < keys.get(self.kp) {
                    // not a listed key: jump this layer to the next one listed
                    let want = keys.get(self.kp).to_vec();
                    if self.curs[top].seek(&want).await? {
                        self.sift_down();
                    } else {
                        self.pop();
                    }
                    continue;
                }
            }
            let key = self.curs[top].key();
            let take = self.pattern.as_ref().is_none_or(|p| p.matches(key));
            group.push_key(key);
            while self
                .heap
                .first()
                .is_some_and(|&i| self.curs[i].key() == group.key(0))
            {
                let c = &mut self.curs[self.heap[0]];
                if take {
                    for row in c.run.rows(c.pos) {
                        group.push_entry_from(&c.run, row);
                    }
                }
                c.pos += 1;
                if c.fill().await? {
                    self.sift_down();
                } else {
                    self.pop();
                }
            }
            if take {
                return Ok(true);
            }
        }
    }
}
