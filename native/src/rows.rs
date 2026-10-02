//! A write's content, as the key index and its store read it: in key order,
//! each key the group of rows that carry it.
//!
//! A `Table` holds every key in place (an Arrow column, or keys packed once)
//! plus the permutation that sorts them — none when they arrive sorted — and
//! is shared: each pass over it is a `Cursor`, computing versions as it
//! reaches them, and `find` gives the rows of chosen keys. A `Stream` is fed
//! sorted chunks and holds one at a time.

use std::collections::VecDeque;
use std::sync::{Arc, OnceLock};

use xxhash_rust::xxh3::Xxh3Default;

use rayon::prelude::*;

use crate::digest::{self, Digest};
use crate::format::{Error, Result};
use crate::run::SortedRun;
use crate::sort::{self, Keys};
use crate::stream::State;

/// Byte strings back to back.
#[derive(Default)]
pub struct Arena {
    pub data: Vec<u8>,
    pub ends: Vec<usize>,
}

impl Arena {
    pub fn push(&mut self, b: &[u8]) {
        self.data.extend_from_slice(b);
        self.ends.push(self.data.len());
    }

    pub fn append(&mut self, other: Arena) {
        let base = self.data.len();
        self.data.extend_from_slice(&other.data);
        self.ends.extend(other.ends.iter().map(|e| e + base));
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.ends.clear();
    }

    pub fn len(&self) -> usize {
        self.ends.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    #[inline]
    pub fn get(&self, i: usize) -> &[u8] {
        let start = if i == 0 { 0 } else { self.ends[i - 1] };
        &self.data[start..self.ends[i]]
    }
}

impl Keys for Arena {
    fn len(&self) -> usize {
        self.ends.len()
    }
    fn key(&self, i: usize) -> &[u8] {
        self.get(i)
    }
}

/// Where versions come from: `fill` appends the versions of `rows` to `out`.
/// Either they are row digests (`rows`), folded into each key's `group`, or
/// they are final, and every row of a key must have the same one.
pub trait Versions: Send + Sync {
    fn fill(&self, rows: &[u32], out: &mut Arena) -> Result<()>;

    fn rows(&self) -> bool {
        false
    }
}

/// Every row has the same version (a partition set's elements).
pub struct Constant(pub Vec<u8>);

impl Versions for Constant {
    fn fill(&self, rows: &[u32], out: &mut Arena) -> Result<()> {
        for _ in rows {
            out.push(&self.0);
        }
        Ok(())
    }
}

/// The rows of one key, read one at a time: their version is the group of
/// their row digests (`fold`), else the version they all share. A marker
/// stands for a key written with no rows: it adds none, and a key that has
/// only markers is the empty group, whatever its revision would have been.
#[derive(Default)]
pub struct Group {
    pub key: Vec<u8>,
    pub version: Vec<u8>,
    digests: Vec<Digest>,
    fold: bool,
    rows: usize,
}

impl Group {
    fn start(&mut self, key: &[u8], fold: bool) {
        self.key.clear();
        self.key.extend_from_slice(key);
        self.version.clear();
        self.digests.clear();
        self.fold = fold;
        self.rows = 0;
    }

    fn add(&mut self, version: &[u8], marker: bool) -> Result<()> {
        if marker {
            return Ok(());
        }
        if self.fold {
            let d = version
                .try_into()
                .map_err(|_| Error::Value("a row digest is 16 bytes".into()))?;
            self.digests.push(d);
        } else if self.rows == 0 {
            self.version.extend_from_slice(version);
        } else if version != self.version.as_slice() {
            return Err(Error::Value(format!(
                "the rows of key {:?} have different revisions",
                String::from_utf8_lossy(&self.key)
            )));
        }
        self.rows += 1;
        Ok(())
    }

    fn finish(&mut self) {
        if self.fold || self.rows == 0 {
            self.version.clear();
            self.version
                .extend_from_slice(&digest::group(&mut self.digests));
        }
    }
}

/// Keys written with no rows, after the rows' own: their markers' keys.
struct Marked {
    keys: Box<dyn Keys + Send>,
    empty: Arena,
}

impl Keys for Marked {
    fn len(&self) -> usize {
        self.keys.len() + self.empty.len()
    }
    fn key(&self, i: usize) -> &[u8] {
        match i.checked_sub(self.keys.len()) {
            None => self.keys.key(i),
            Some(j) => self.empty.get(j),
        }
    }
}

/// Versions computed a window of rows at a time, in key order.
const WINDOW: usize = 1 << 14;

/// A write's rows, sorted once and shared by every pass over them.
pub struct Table {
    keys: Box<dyn Keys + Send>,
    order: Option<Vec<u32>>,
    versions: Box<dyn Versions>,
    /// Rows below are the write's; the rest are markers of empty groups.
    real: usize,
    /// The digest of every `(key, version)`, once a pass has read them all.
    content: OnceLock<Digest>,
}

impl Table {
    /// Sorts the keys unless they arrive sorted; `empty` are keys written
    /// with no rows, none of them a key of the rows.
    pub fn new(
        keys: Box<dyn Keys + Send>,
        versions: Box<dyn Versions>,
        empty: Arena,
    ) -> Result<Table> {
        let real = keys.len();
        let keys: Box<dyn Keys + Send> = if empty.is_empty() {
            keys
        } else {
            Box::new(Marked { keys, empty })
        };
        let order = (!sort::is_sorted(&*keys)).then(|| sort::order(&*keys));
        Ok(Table {
            keys,
            order,
            versions,
            real,
            content: OnceLock::new(),
        })
    }

    /// The write's rows, not keys or markers.
    pub fn len(&self) -> usize {
        self.real
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Whether the rows arrived sorted.
    pub fn presorted(&self) -> bool {
        self.order.is_none()
    }

    /// The row at sorted position `p`.
    #[inline]
    fn row(&self, p: usize) -> u32 {
        self.order.as_ref().map_or(p as u32, |o| o[p])
    }

    /// The first sorted position whose key is not below `key`.
    fn lower(&self, key: &[u8]) -> usize {
        let (mut lo, mut hi) = (0, self.keys.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.keys.key(self.row(mid) as usize) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// The rows of `key`, in the order the write had them; None when the
    /// write does not hold the key (an empty group holds it, with no rows).
    pub fn find(&self, key: &[u8]) -> Option<Vec<u32>> {
        let mut p = self.lower(key);
        let mut rows = Vec::new();
        let mut held = false;
        while p < self.keys.len() && self.keys.key(self.row(p) as usize) == key {
            let r = self.row(p);
            held = true;
            if (r as usize) < self.real {
                rows.push(r);
            }
            p += 1;
        }
        rows.sort_unstable();
        held.then_some(rows)
    }

    /// The version of `key`, from its rows alone; None when the write does
    /// not hold it.
    pub fn version(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(rows) = self.find(key) else {
            return Ok(None);
        };
        let mut versions = Arena::default();
        self.versions.fill(&rows, &mut versions)?;
        let mut g = Group::default();
        g.start(key, self.versions.rows());
        for i in 0..versions.len() {
            g.add(versions.get(i), false)?;
        }
        g.finish();
        Ok(Some(g.version))
    }

    /// The digest of every `(key, version)` in key order, reading them all
    /// unless a pass already has.
    pub fn content(self: &Arc<Table>) -> Result<Digest> {
        if let Some(d) = self.content.get() {
            return Ok(*d);
        }
        let mut c = Cursor::new(self.clone());
        while c.read()? {}
        Ok(*self
            .content
            .get()
            .expect("a whole pass records the content"))
    }
}

/// One pass over a `Table`, a key at a time.
pub struct Cursor {
    table: Arc<Table>,
    // The next rows in key order, keys and versions side by side: rows sit in
    // any order in memory, so they are gathered a window at a time, on every core.
    wrows: Vec<u32>,
    wkeys: Arena,
    wvers: Arena,
    start: usize,
    pos: usize,
    group: Group,
    ready: bool,
    hash: Xxh3Default,
}

impl Cursor {
    pub fn new(table: Arc<Table>) -> Cursor {
        Cursor {
            table,
            wrows: Vec::new(),
            wkeys: Arena::default(),
            wvers: Arena::default(),
            start: 0,
            pos: 0,
            group: Group::default(),
            ready: false,
            hash: Xxh3Default::new(),
        }
    }

    pub fn table(&self) -> &Arc<Table> {
        &self.table
    }

    /// Makes the window hold row `pos`.
    fn fill(&mut self) -> Result<()> {
        if self.pos < self.start + self.wkeys.len() {
            return Ok(());
        }
        let t = &*self.table;
        let end = (self.pos + WINDOW).min(t.keys.len());
        self.wrows.clear();
        self.wrows.extend((self.pos..end).map(|p| t.row(p)));
        self.wkeys.clear();
        gather(&*t.keys, &self.wrows, &mut self.wkeys);
        self.wvers.clear();
        let real: Vec<u32> = self
            .wrows
            .iter()
            .copied()
            .filter(|&r| (r as usize) < t.real)
            .collect();
        if real.len() == self.wrows.len() {
            t.versions.fill(&self.wrows, &mut self.wvers)?;
        } else {
            // Markers have no version of their own: an empty one keeps the window aligned.
            let mut found = Arena::default();
            t.versions.fill(&real, &mut found)?;
            let mut j = 0;
            for &r in &self.wrows {
                if (r as usize) < t.real {
                    self.wvers.push(found.get(j));
                    j += 1;
                } else {
                    self.wvers.push(b"");
                }
            }
        }
        self.start = self.pos;
        Ok(())
    }

    /// Reads the next key's rows into `group`; false past the last.
    pub fn read(&mut self) -> Result<bool> {
        let n = self.table.keys.len();
        if self.pos >= n {
            if self.pos == n && self.table.content.get().is_none() {
                let d = self.hash.digest128().to_le_bytes();
                let _ = self.table.content.set(d);
            }
            self.pos = n + 1; // recorded once
            return Ok(false);
        }
        self.fill()?;
        let fold = self.table.versions.rows();
        let real = self.table.real;
        self.group
            .start(self.wkeys.get(self.pos - self.start), fold);
        loop {
            let i = self.pos - self.start;
            self.group
                .add(self.wvers.get(i), self.wrows[i] as usize >= real)?;
            self.pos += 1;
            if self.pos >= n {
                break;
            }
            self.fill()?;
            if self.wkeys.get(self.pos - self.start) != self.group.key.as_slice() {
                break;
            }
        }
        self.group.finish();
        for part in [&self.group.key, &self.group.version] {
            self.hash.update(&(part.len() as u64).to_le_bytes());
            self.hash.update(part);
        }
        Ok(true)
    }

    /// The current key and its version, after `read` returned true.
    pub fn entry(&self) -> (&[u8], &[u8]) {
        (&self.group.key, &self.group.version)
    }
}

/// Appends the keys of `rows` to `out`, on every core.
fn gather<K: Keys + ?Sized>(keys: &K, rows: &[u32], out: &mut Arena) {
    let parts: Vec<Arena> = rows
        .par_chunks(1024)
        .map(|c| {
            let mut a = Arena::default();
            for &r in c {
                a.push(keys.key(r as usize));
            }
            a
        })
        .collect();
    for p in parts {
        out.append(p);
    }
}

/// Sorted chunks of `(key, version)`, fed as they are read. A key may
/// repeat: its versions are row digests to `fold`, or must agree.
#[derive(Default)]
pub struct Stream {
    chunks: VecDeque<(Arena, Arena)>,
    pos: usize,
    last: Option<Vec<u8>>,
    ended: bool,
    fold: bool,
    group: Group,
    open: bool,
    ready: bool,
}

impl Stream {
    pub fn new(fold: bool) -> Stream {
        Stream {
            fold,
            ..Stream::default()
        }
    }

    pub fn feed(&mut self, keys: Arena, versions: Arena) -> Result<()> {
        for i in 0..keys.len() {
            let k = keys.get(i);
            let prev = if i > 0 {
                Some(keys.get(i - 1))
            } else {
                self.last.as_deref()
            };
            if let Some(p) = prev.filter(|&p| k < p) {
                return Err(Error::Value(format!(
                    "keys must arrive sorted: {:?} then {:?}",
                    String::from_utf8_lossy(p),
                    String::from_utf8_lossy(k)
                )));
            }
        }
        if !keys.is_empty() {
            self.last = Some(keys.get(keys.len() - 1).to_vec());
            self.chunks.push_back((keys, versions));
        }
        Ok(())
    }

    pub fn end(&mut self) {
        self.ended = true;
    }

    /// Reads the next key's rows into `group`, across chunks.
    fn state(&mut self) -> Result<State> {
        if self.ready {
            return Ok(State::Ready);
        }
        loop {
            let Some((k, v)) = self.chunks.front() else {
                if !self.ended {
                    return Ok(State::Starved); // the group may go on in the next chunk
                }
                if self.open {
                    self.group.finish();
                    (self.open, self.ready) = (false, true);
                    return Ok(State::Ready);
                }
                return Ok(State::Done);
            };
            if self.pos >= k.len() {
                self.chunks.pop_front();
                self.pos = 0;
                continue;
            }
            let key = k.get(self.pos);
            if self.open && key != self.group.key.as_slice() {
                self.group.finish();
                (self.open, self.ready) = (false, true);
                return Ok(State::Ready);
            }
            if !self.open {
                self.group.start(key, self.fold);
                self.open = true;
            }
            self.group.add(v.get(self.pos), false)?;
            self.pos += 1;
        }
    }
}

/// A stream with a sorted run laid over it: the run's upserts in place of
/// the stream's entries of their keys, and its removes gone — a patch over
/// what a store holds, read back a chunk at a time.
pub struct Overlay {
    pub base: Stream,
    run: Arc<SortedRun>,
    i: usize,
    current: Option<bool>, // Some(true): the run's entry `i`; Some(false): the base's
}

impl Overlay {
    pub fn new(base: Stream, run: Arc<SortedRun>) -> Overlay {
        Overlay {
            base,
            run,
            i: 0,
            current: None,
        }
    }

    fn state(&mut self) -> Result<State> {
        loop {
            if self.current.is_some() {
                return Ok(State::Ready);
            }
            let base = self.base.state()?;
            if base == State::Starved {
                return Ok(State::Starved);
            }
            let ready = base == State::Ready;
            if self.i >= self.run.len() {
                if !ready {
                    return Ok(State::Done);
                }
                self.current = Some(false);
                continue;
            }
            let key = self.run.key(self.i);
            if ready && self.base.group.key.as_slice() < key {
                self.current = Some(false);
                continue;
            }
            if ready && self.base.group.key.as_slice() == key {
                self.base.ready = false; // the run's entry stands for it
                continue;
            }
            if self.run.write(self.i).is_none() {
                self.i += 1; // a remove: nothing of the key stays
                continue;
            }
            self.current = Some(true);
        }
    }

    fn entry(&self) -> (&[u8], &[u8]) {
        match self.current {
            Some(true) => (
                self.run.key(self.i),
                self.run.write(self.i).expect("an upsert"),
            ),
            _ => (&self.base.group.key, &self.base.group.version),
        }
    }

    fn advance(&mut self) {
        match self.current.take() {
            Some(true) => self.i += 1,
            Some(false) => self.base.ready = false,
            None => {}
        }
    }
}

/// The written side of a merge-join: rows, sorted chunks as they stream
/// (with a run laid over them, or not), or a sorted run (whose entries may
/// be removes).
pub enum Source {
    Table(Box<Cursor>),
    Stream(Stream),
    Run(Arc<SortedRun>, usize),
    Overlay(Box<Overlay>),
}

impl Source {
    pub fn state(&mut self) -> Result<State> {
        match self {
            Source::Run(run, i) => Ok(if *i < run.len() {
                State::Ready
            } else {
                State::Done
            }),
            Source::Table(t) => {
                if !t.ready {
                    if !t.read()? {
                        return Ok(State::Done);
                    }
                    t.ready = true;
                }
                Ok(State::Ready)
            }
            Source::Stream(s) => s.state(),
            Source::Overlay(o) => o.state(),
        }
    }

    /// The current key; only when `state` is `Ready`.
    #[inline]
    pub fn key(&self) -> &[u8] {
        self.entry().0
    }

    /// The current write: its version, or None for a remove.
    #[inline]
    pub fn write(&self) -> Option<&[u8]> {
        match self {
            Source::Run(run, i) => run.write(*i),
            _ => Some(self.entry().1),
        }
    }

    /// The current key and its version; only when `state` is `Ready`.
    pub fn entry(&self) -> (&[u8], &[u8]) {
        match self {
            Source::Table(t) => (&t.group.key, &t.group.version),
            Source::Stream(s) => (&s.group.key, &s.group.version),
            Source::Run(run, i) => (run.key(*i), run.versions.get(*i)),
            Source::Overlay(o) => o.entry(),
        }
    }

    pub fn advance(&mut self) {
        match self {
            Source::Table(t) => t.ready = false,
            Source::Stream(s) => s.ready = false,
            Source::Run(_, i) => *i += 1,
            Source::Overlay(o) => o.advance(),
        }
    }

    /// The stream fed sorted chunks, if this is one.
    pub fn stream(&mut self) -> Option<&mut Stream> {
        match self {
            Source::Table(_) | Source::Run(..) => None,
            Source::Stream(s) => Some(s),
            Source::Overlay(o) => Some(&mut o.base),
        }
    }
}
