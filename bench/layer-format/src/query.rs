//! Reading layers, the same way for both formats: open a layer (one GET of
//! its index or footer and page indexes), plan the units a predicate needs
//! (blocks, or row groups with a row selection), fetch them in batches that
//! double in size (256 KiB to 32 MiB, one batch ahead), decode, and merge the
//! layers by key. The queries — diff, scan, get — and the merges sit on top.

use crate::gen::{id_of, Digest, NONE};
use crate::ours;
use crate::ours::Rec;
use crate::pq;
use crate::run::Run;
use crate::store::{Sparse, Store};
use parquet::arrow::arrow_reader::RowSelection;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use tokio::task::JoinHandle;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Fmt {
    Ours,
    Pq,
    OursRanges,
    OursRle,
    OursRangesRle,
}

impl Fmt {
    pub fn name(self) -> &'static str {
        match self {
            Fmt::Ours => "ours",
            Fmt::Pq => "parquet",
            Fmt::OursRanges => "ours+ranges",
            Fmt::OursRle => "ours+rle",
            Fmt::OursRangesRle => "ours+ranges+rle",
        }
    }
    pub fn parse(s: &str) -> Fmt {
        [Fmt::Ours, Fmt::Pq, Fmt::OursRanges, Fmt::OursRle, Fmt::OursRangesRle]
            .into_iter()
            .find(|f| f.name() == s)
            .expect("fmt?")
    }
    /// Our block format, with or without range records and run-length columns.
    pub fn ours(self) -> bool {
        self != Fmt::Pq
    }
    pub fn ranges(self) -> bool {
        matches!(self, Fmt::OursRanges | Fmt::OursRangesRle)
    }
    pub fn rle(self) -> bool {
        matches!(self, Fmt::OursRle | Fmt::OursRangesRle)
    }
}

/// A layer as engine state lists it: no GET to find it, its commits or keys,
/// or where its metadata sits (`meta_start`: the whole index, or footer and
/// page indexes; `meta2`: the index's top level, or the footer alone).
#[derive(Clone, Debug)]
pub struct Layer {
    pub fmt: Fmt,
    pub path: String,
    pub size: u64,
    pub meta_start: u64,
    pub meta2: u64,
    pub lo: u64,
    pub hi: u64,
    pub base: bool,
    pub entries: u64,
    pub first: Vec<u8>,
    pub last: Vec<u8>,
}

/// Metadata under this many bytes is read in one GET; above it, its top level
/// first, then only the index pieces a read needs.
pub static ONE_GET: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1 << 20);

#[derive(Clone, Copy)]
pub enum Pred<'a> {
    Range(Option<&'a [u8]>, Option<&'a [u8]>),
    Keys(&'a [Vec<u8>]),
    /// Sorted, disjoint `[lo, hi)` key ranges (ours only).
    Ranges(&'a [(Vec<u8>, Vec<u8>)]),
}

#[derive(Clone)]
enum Unit {
    Block(Range<u64>, u64, u64), // its bytes, raw length, entries
    Rg(usize, RowSelection, Vec<Range<u64>>),
}

impl Unit {
    fn ranges(&self) -> Vec<Range<u64>> {
        match self {
            Unit::Block(r, _, _) => vec![r.clone()],
            Unit::Rg(_, _, rs) => rs.clone(),
        }
    }
    fn bytes(&self) -> u64 {
        self.ranges().iter().map(|r| r.end - r.start).sum()
    }
}

enum Dec {
    Ours,
    Pq(Arc<pq::Meta>),
}

/// Open a layer for a read: its metadata (one GET, or the top level and then
/// the pieces the read needs), and the units it must read.
async fn prepare(store: &Store, l: &Layer, p: Pred<'_>) -> (Vec<Unit>, Dec, Vec<Rec>) {
    let whole = l.size - l.meta_start <= ONE_GET.load(std::sync::atomic::Ordering::Relaxed);
    let sp = store
        .fetch(
            &l.path,
            &[if whole { l.meta_start } else { l.meta2 }..l.size],
        )
        .await;
    let (lo, hi, keys) = match p {
        Pred::Range(lo, hi) => (lo, hi, None),
        Pred::Keys(k) => (None, None, Some(k)),
        Pred::Ranges(_) => (None, None, None),
    };
    match l.fmt {
        f if f.ours() => {
            let mut ix = ours::Index::parse_top(&sp.slice(l.meta2..l.size), l.meta_start);
            let segs = match p {
                Pred::Keys(k) => ix.segs_for_keys(k),
                Pred::Range(lo, hi) => ix.segs_for(lo, hi),
                Pred::Ranges(rs) => ix.segs_for_ranges(rs),
            };
            let ranges: Vec<Range<u64>> = segs.iter().map(|&i| ix.segs[i].range.clone()).collect();
            let sp = if whole {
                sp
            } else {
                store.fetch(&l.path, &ranges).await
            };
            for (&i, r) in segs.iter().zip(ranges) {
                ix.load(i, &sp.slice(r));
            }
            let recs = std::mem::take(&mut ix.recs);
            let blocks = match p {
                Pred::Keys(k) => ix.of_keys(k),
                Pred::Range(lo, hi) => ix.span(lo, hi),
                Pred::Ranges(rs) => ix.span_ranges(rs),
            };
            (
                blocks
                    .into_iter()
                    .map(|b| Unit::Block(b.offset..b.offset + b.clen, b.rlen, b.n))
                    .collect(),
                Dec::Ours,
                recs,
            )
        }
        _ => {
            assert!(!matches!(p, Pred::Ranges(_)), "key ranges: ours only");
            let meta = if whole {
                pq::Meta::parse(sp, l.size)
            } else {
                let footer = pq::Meta::footer(&sp.slice(l.meta2..l.size));
                let rgs = pq::Meta::row_groups(&footer, lo, hi, keys);
                let sp = store
                    .fetch(&l.path, &pq::Meta::index_ranges(&footer, &rgs))
                    .await;
                pq::Meta::with_index(&footer, &rgs, &sp)
            };
            let units = meta
                .plan(lo, hi, keys)
                .into_iter()
                .map(|(g, s, r)| Unit::Rg(g, s, r))
                .collect();
            (units, Dec::Pq(Arc::new(meta)), Vec::new())
        }
    }
}

fn decode(dec: &Dec, l: &Layer, sp: &Sparse, u: &Unit) -> Vec<Run> {
    match (dec, u) {
        (Dec::Ours, Unit::Block(r, rlen, n)) => vec![ours::decode(&sp.slice(r.clone()), *rlen, *n)],
        (Dec::Pq(m), Unit::Rg(g, sel, _)) => m.decode(l.size, sp, *g, sel),
        _ => unreachable!(),
    }
}

const FIRST: u64 = 256 << 10;
const MAX: u64 = 32 << 20;

/// One layer's runs in key order, fetched a batch ahead.
pub struct Stream {
    store: Store,
    layer: Arc<Layer>,
    dec: Dec,
    units: Arc<Vec<Unit>>,
    next: usize,
    target: u64,
    pending: Option<(Range<usize>, JoinHandle<Sparse>)>,
    ready: VecDeque<Run>,
    /// The layer's range records.
    pub recs: Vec<Rec>,
}

impl Stream {
    pub async fn new(store: &Store, layer: Arc<Layer>, p: Pred<'_>, all_at_once: bool) -> Stream {
        let (units, dec, recs) = prepare(store, &layer, p).await;
        let units = Arc::new(units);
        if std::env::var("LF_DEBUG").is_ok() {
            let b: u64 = units.iter().map(|u| u.bytes()).sum();
            let meta = layer.size - layer.meta_start;
            eprintln!(
                "  {} {}-{}: {} units, {} data bytes, {} meta bytes",
                layer.fmt.name(),
                layer.lo,
                layer.hi,
                units.len(),
                b,
                meta
            );
        }
        Stream {
            store: store.clone(),
            layer,
            dec,
            units,
            next: 0,
            target: if all_at_once { u64::MAX } else { FIRST },
            pending: None,
            ready: VecDeque::new(),
            recs,
        }
    }

    fn launch(&mut self) {
        let a = self.next;
        let mut bytes = 0;
        while self.next < self.units.len() && (self.next == a || bytes < self.target) {
            bytes += self.units[self.next].bytes();
            self.next += 1;
        }
        if self.next == a {
            return;
        }
        if self.target != u64::MAX {
            self.target = (self.target * 2).min(MAX);
        }
        let ranges: Vec<Range<u64>> = self.units[a..self.next]
            .iter()
            .flat_map(|u| u.ranges())
            .collect();
        let (store, path) = (self.store.clone(), self.layer.path.clone());
        self.pending = Some((
            a..self.next,
            tokio::spawn(async move { store.fetch(&path, &ranges).await }),
        ));
    }

    pub async fn next_run(&mut self) -> Option<Run> {
        loop {
            if let Some(r) = self.ready.pop_front() {
                return Some(r);
            }
            if self.pending.is_none() {
                self.launch();
            }
            let (span, h) = self.pending.take()?;
            let sp = h.await.unwrap();
            self.launch(); // the next batch is fetched while this one decodes
            for u in &self.units[span] {
                self.ready.extend(
                    decode(&self.dec, &self.layer, &sp, u)
                        .into_iter()
                        .filter(|r| r.len() > 0),
                );
            }
        }
    }
}

struct Cur {
    s: Stream,
    run: Option<Run>,
    pos: usize,
}

impl Cur {
    async fn fill(&mut self) {
        while self.run.as_ref().is_none_or(|r| self.pos >= r.len()) {
            match self.s.next_run().await {
                Some(r) => {
                    self.run = Some(r);
                    self.pos = 0;
                }
                None => {
                    self.run = None;
                    return;
                }
            }
            if self.run.is_none() {
                return;
            }
        }
    }
    fn key(&self) -> Option<&[u8]> {
        self.run.as_ref().map(|r| r.key.value(self.pos))
    }
}

pub type E = (u64, Option<u64>, Option<u64>); // commit, new, replaced

/// A min-heap of cursor indices by (key, rank: newest layer first).
struct Heap(Vec<usize>);

impl Heap {
    fn less(c: &[Cur], a: usize, b: usize) -> bool {
        (c[a].key().unwrap(), a) < (c[b].key().unwrap(), b)
    }
    fn push(&mut self, c: &[Cur], i: usize) {
        self.0.push(i);
        let mut k = self.0.len() - 1;
        while k > 0 {
            let p = (k - 1) / 2;
            if Self::less(c, self.0[k], self.0[p]) {
                self.0.swap(k, p);
                k = p;
            } else {
                break;
            }
        }
    }
    fn pop(&mut self, c: &[Cur]) -> Option<usize> {
        let n = self.0.len();
        if n == 0 {
            return None;
        }
        self.0.swap(0, n - 1);
        let top = self.0.pop();
        let n = self.0.len();
        let mut k = 0;
        loop {
            let (l, r) = (2 * k + 1, 2 * k + 2);
            let mut m = k;
            if l < n && Self::less(c, self.0[l], self.0[m]) {
                m = l;
            }
            if r < n && Self::less(c, self.0[r], self.0[m]) {
                m = r;
            }
            if m == k {
                break;
            }
            self.0.swap(k, m);
            k = m;
        }
        top
    }
    fn peek(&self) -> Option<usize> {
        self.0.first().copied()
    }
}

/// The layers opened for a read, in parallel.
pub async fn open(
    store: &Store,
    layers: &[Arc<Layer>],
    p: Pred<'_>,
    all_at_once: bool,
) -> Vec<Stream> {
    futures::future::join_all(
        layers
            .iter()
            .map(|l| Stream::new(store, l.clone(), p, all_at_once)),
    )
    .await
}

/// The streams' range records, by their first key.
fn recs_of(streams: &[Stream]) -> Vec<Rec> {
    let mut v: Vec<Rec> = streams.iter().flat_map(|s| s.recs.iter().cloned()).collect();
    v.sort_by(|a, b| a.lo.cmp(&b.lo));
    v
}

/// A key's entries (newest first) with the range records covering it spelled
/// out as entries: a record at commit c applies to the key if it was present
/// just before c and has no entry of its own at c. Needs the key's history
/// back to the base.
fn resolve(es: &mut Vec<E>, covering: &mut Vec<(u64, Option<u64>)>) {
    covering.sort_unstable_by_key(|r| r.0);
    let mut out: Vec<E> = Vec::with_capacity(es.len() + covering.len());
    let mut state: Option<u64> = None;
    let mut j = 0;
    let apply = |c: u64, new: Option<u64>, state: &mut Option<u64>, out: &mut Vec<E>| {
        if let Some(s) = *state {
            out.push((c, new, Some(s)));
            *state = new;
        }
    };
    for &e in es.iter().rev() {
        while j < covering.len() && covering[j].0 <= e.0 {
            if covering[j].0 < e.0 {
                apply(covering[j].0, covering[j].1, &mut state, &mut out);
            }
            j += 1;
        }
        out.push(e);
        state = e.1;
    }
    for &(c, new) in &covering[j..] {
        apply(c, new, &mut state, &mut out);
    }
    out.reverse();
    *es = out;
}

/// Every key in `[lo, hi)` of the layers (newest first), with its entries
/// across them, newest first; `on` returns false to stop.
pub async fn walk(
    store: &Store,
    layers: &[Arc<Layer>],
    p: Pred<'_>,
    all_at_once: bool,
    on: impl FnMut(&[u8], &[E]) -> bool,
) {
    let streams = open(store, layers, p, all_at_once).await;
    let recs = recs_of(&streams);
    walk_streams(streams, p, &recs, on).await
}

/// `walk` over opened streams, newest layer first; `recs` are resolved into
/// the entries of the keys they cover (none: entries as stored).
pub async fn walk_streams(
    streams: Vec<Stream>,
    p: Pred<'_>,
    recs: &[Rec],
    mut on: impl FnMut(&[u8], &[E]) -> bool,
) {
    let (lo, hi) = match p {
        Pred::Range(lo, hi) => (lo, hi),
        _ => (None, None),
    };
    let (mut next_rec, mut active, mut covering) = (0, Vec::<usize>::new(), Vec::new());
    let mut curs: Vec<Cur> = streams
        .into_iter()
        .map(|s| Cur {
            s,
            run: None,
            pos: 0,
        })
        .collect();
    futures::future::join_all(curs.iter_mut().map(|c| c.fill())).await;
    let mut heap = Heap(Vec::new());
    for i in 0..curs.len() {
        if curs[i].key().is_some() {
            heap.push(&curs, i);
        }
    }
    let mut key: Vec<u8> = Vec::with_capacity(64);
    let mut es: Vec<E> = Vec::with_capacity(64);
    while let Some(top) = heap.peek() {
        key.clear();
        key.extend_from_slice(curs[top].key().unwrap());
        if hi.is_some_and(|hi| key.as_slice() >= hi) {
            break;
        }
        es.clear();
        while heap
            .peek()
            .is_some_and(|i| curs[i].key().unwrap() == key.as_slice())
        {
            let i = heap.pop(&curs).unwrap();
            loop {
                let c = &mut curs[i];
                let r = c.run.as_ref().unwrap();
                if r.key.value(c.pos) != key.as_slice() {
                    break;
                }
                es.push((
                    r.commit.value(c.pos) as u64,
                    Run::opt(&r.new, c.pos),
                    Run::opt(&r.replaced, c.pos),
                ));
                c.pos += 1;
                if c.pos >= r.len() {
                    c.fill().await;
                    if c.run.is_none() {
                        break;
                    }
                }
            }
            if curs[i].key().is_some() {
                heap.push(&curs, i);
            }
        }
        if lo.is_some_and(|lo| key.as_slice() < lo) {
            continue;
        }
        if !recs.is_empty() {
            while next_rec < recs.len() && recs[next_rec].lo.as_slice() <= key.as_slice() {
                active.push(next_rec);
                next_rec += 1;
            }
            active.retain(|&i| recs[i].hi.as_slice() >= key.as_slice());
            if !active.is_empty() {
                covering.clear();
                covering.extend(active.iter().map(|&i| (recs[i].commit, recs[i].new)));
                resolve(&mut es, &mut covering);
            }
        }
        if !on(&key, &es) {
            break;
        }
    }
}

fn in_keys(l: &Layer, lo: Option<&[u8]>, hi: Option<&[u8]>) -> bool {
    lo.is_none_or(|lo| l.last.as_slice() >= lo) && hi.is_none_or(|hi| l.first.as_slice() < hi)
}

/// Newest first: layers whose commits overlap `(c1, c2]`, keys `[lo, hi)`.
pub fn diff_layers(
    all: &[Arc<Layer>],
    c1: u64,
    c2: u64,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> Vec<Arc<Layer>> {
    assert!(
        c1 >= all.iter().find(|l| l.base).unwrap().hi,
        "c1 is below the base: not retained"
    );
    let mut v: Vec<_> = all
        .iter()
        .filter(|l| !l.base && l.lo <= c2 && l.hi > c1 && in_keys(l, lo, hi))
        .cloned()
        .collect();
    v.sort_by_key(|l| std::cmp::Reverse(l.hi));
    v
}

/// Newest first: the layers below a diff's window (and the base), keys `[lo, hi)`:
/// what a range record in the window needs to recover the versions it replaced.
pub fn older_layers(
    all: &[Arc<Layer>],
    c1: u64,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> Vec<Arc<Layer>> {
    let mut v: Vec<_> = all
        .iter()
        .filter(|l| (l.base || l.hi <= c1) && in_keys(l, lo, hi))
        .cloned()
        .collect();
    v.sort_by_key(|l| std::cmp::Reverse(l.hi));
    v
}

/// The key ranges records cover within `[lo, hi)`, as sorted disjoint `[a, b)`.
fn cover(recs: &[Rec], lo: Option<&[u8]>, hi: Option<&[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut u: Vec<(Vec<u8>, Vec<u8>)> = recs
        .iter()
        .map(|r| {
            let a = match lo {
                Some(lo) if lo > r.lo.as_slice() => lo.to_vec(),
                _ => r.lo.clone(),
            };
            let mut b = r.hi.clone();
            b.push(0); // the key right after hi
            let b = match hi {
                Some(hi) if hi < b.as_slice() => hi.to_vec(),
                _ => b,
            };
            (a, b)
        })
        .filter(|(a, b)| a < b)
        .collect();
    u.sort();
    let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for (a, b) in u {
        match out.last_mut() {
            Some(l) if a <= l.1 => {
                if b > l.1 {
                    l.1 = b
                }
            }
            _ => out.push((a, b)),
        }
    }
    out
}

pub fn scan_layers(
    all: &[Arc<Layer>],
    c: u64,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> Vec<Arc<Layer>> {
    let mut v: Vec<_> = all
        .iter()
        .filter(|l| l.lo <= c && in_keys(l, lo, hi))
        .cloned()
        .collect();
    v.sort_by_key(|l| std::cmp::Reverse(l.hi));
    v
}

/// `window`: the layers overlapping `(c1, c2]`; `older`: those below it, read
/// only over the key ranges of the window's range records, if it has any.
#[allow(clippy::too_many_arguments)]
pub async fn diff(
    store: &Store,
    window: &[Arc<Layer>],
    older: &[Arc<Layer>],
    c1: u64,
    c2: u64,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    limit: u64,
) -> Digest {
    let p = Pred::Range(lo, hi);
    let mut streams = open(store, window, p, false).await;
    let mut inside = recs_of(&streams);
    inside.retain(|r| {
        c1 < r.commit
            && r.commit <= c2
            && lo.is_none_or(|lo| r.hi.as_slice() >= lo)
            && hi.is_none_or(|hi| r.lo.as_slice() < hi)
    });
    let mut recs = Vec::new();
    if !inside.is_empty() {
        let u = cover(&inside, lo, hi);
        let older: Vec<Arc<Layer>> = older
            .iter()
            .filter(|l| u.iter().any(|(a, b)| in_keys(l, Some(a), Some(b))))
            .cloned()
            .collect();
        streams.extend(open(store, &older, Pred::Ranges(&u), false).await);
        recs = recs_of(&streams);
    }
    let mut d = Digest::default();
    walk_streams(streams, p, &recs, |key, es| {
        let mut window = es.iter().filter(|e| c1 < e.0 && e.0 <= c2);
        let Some(newest) = window.next() else {
            return true;
        };
        let oldest = window.last().unwrap_or(newest);
        let (before, after) = (oldest.2.unwrap_or(NONE), newest.1.unwrap_or(NONE));
        if before != after {
            d.add(id_of(key), before, after);
        }
        d.n < limit
    })
    .await;
    d
}

fn state(es: &[E], c: u64) -> Option<u64> {
    es.iter().find(|e| e.0 <= c).and_then(|e| e.1)
}

pub async fn scan(
    store: &Store,
    layers: &[Arc<Layer>],
    c: u64,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> Digest {
    let mut d = Digest::default();
    walk(store, layers, Pred::Range(lo, hi), false, |key, es| {
        if let Some(v) = state(es, c) {
            d.add(id_of(key), v, 0);
        }
        true
    })
    .await;
    d
}

pub async fn get(store: &Store, layers: &[Arc<Layer>], c: u64, keys: &[Vec<u8>]) -> Digest {
    let mut d = Digest::default();
    let mut k = 0;
    walk(store, layers, Pred::Keys(keys), true, |key, es| {
        while k < keys.len() && keys[k].as_slice() < key {
            k += 1;
        }
        if k < keys.len() && keys[k].as_slice() == key {
            if let Some(v) = state(es, c) {
                d.add(id_of(key), v, 0);
            }
        }
        true
    })
    .await;
    d
}

/// Every entry of the layers, in order, to `push`; with `fold` at a commit,
/// one entry per key present there instead (a new base).
/// A merge keeps the inputs' range records as they are, and returns them for
/// the output; a fold resolves them (it reads every layer down to the base).
pub async fn merge(
    store: &Store,
    layers: &[Arc<Layer>],
    fold: Option<u64>,
    mut push: impl FnMut(&[u8], &E),
) -> Vec<Rec> {
    let mut layers = layers.to_vec();
    layers.sort_by_key(|l| std::cmp::Reverse(l.hi)); // newest first: a key's entries come out newest first
    let p = Pred::Range(None, None);
    let streams = open(store, &layers, p, false).await;
    let recs = recs_of(&streams);
    let resolve = if fold.is_some() { &recs[..] } else { &[] };
    walk_streams(streams, p, resolve, |key, es| {
        match fold {
            None => es.iter().for_each(|e| push(key, e)),
            Some(c) => {
                if let Some(e) = es.iter().find(|e| e.0 <= c) {
                    if let Some(v) = e.1 {
                        push(key, &(e.0, Some(v), None));
                    }
                }
            }
        }
        true
    })
    .await;
    if fold.is_some() {
        Vec::new()
    } else {
        recs
    }
}
