//! A write's content, as the key index and its store read it: in key order,
//! each key the group of rows that carry it.
//!
//! A `Table` holds every key in place (an Arrow column, or keys packed once)
//! plus the permutation that sorts them — none when they arrive sorted — and
//! is shared: each pass over it is a `Cursor`, and `find` gives the rows of
//! chosen keys. Only keys are read: a row's other values never reach native
//! code (docs/versions.md). A source's keys may carry a payload each, its
//! version. A `Stream` is fed sorted chunks of keys and holds one at a time.

use std::collections::VecDeque;
use std::sync::Arc;

use rayon::prelude::*;

use crate::delta::Write;
use crate::entries::SortedEntries;
use crate::error::{Error, Result};
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

/// Where a source's payloads come from: `fill` appends the payload of each
/// of `rows` to `out`, None where a row carries none.
pub trait Payloads: Send + Sync {
    fn fill(&self, rows: &[u32], out: &mut Vec<Option<Vec<u8>>>) -> Result<()>;
}

/// Every row carries the same payload (a partition set's elements: empty).
pub struct Constant(pub Vec<u8>);

impl Payloads for Constant {
    fn fill(&self, rows: &[u32], out: &mut Vec<Option<Vec<u8>>>) -> Result<()> {
        out.extend(rows.iter().map(|_| Some(self.0.clone())));
        Ok(())
    }
}

/// The rows of one key, read one at a time: a key has rows, and one written
/// with none does not exist. Rows that carry payloads must agree on it.
#[derive(Default)]
pub struct Group {
    pub key: Vec<u8>,
    pub payload: Option<Vec<u8>>,
    rows: usize,
}

impl Group {
    fn start(&mut self, key: &[u8]) {
        self.key.clear();
        self.key.extend_from_slice(key);
        self.payload = None;
        self.rows = 0;
    }

    fn add(&mut self, payload: Option<&[u8]>) -> Result<()> {
        if self.rows == 0 {
            self.payload = payload.map(<[u8]>::to_vec);
        } else if payload != self.payload.as_deref() {
            return Err(Error::Value(format!(
                "key {:?} is given two versions",
                String::from_utf8_lossy(&self.key)
            )));
        }
        self.rows += 1;
        Ok(())
    }
}

/// Keys gathered a window of rows at a time, in key order.
const WINDOW: usize = 1 << 14;

/// A write's rows, sorted once and shared by every pass over them.
pub struct Table {
    keys: Box<dyn Keys + Send>,
    order: Option<Vec<u32>>,
    payloads: Option<Box<dyn Payloads>>,
}

impl Table {
    /// Sorts the keys unless they arrive sorted.
    pub fn new(keys: Box<dyn Keys + Send>, payloads: Option<Box<dyn Payloads>>) -> Table {
        let order = (!sort::is_sorted(&*keys)).then(|| sort::order(&*keys));
        Table {
            keys,
            order,
            payloads,
        }
    }

    /// Rows, not keys.
    pub fn len(&self) -> usize {
        self.keys.len()
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
    /// write does not hold the key.
    pub fn find(&self, key: &[u8]) -> Option<Vec<u32>> {
        let mut p = self.lower(key);
        let mut rows = Vec::new();
        while p < self.keys.len() && self.keys.key(self.row(p) as usize) == key {
            rows.push(self.row(p));
            p += 1;
        }
        rows.sort_unstable();
        (!rows.is_empty()).then_some(rows)
    }
}

/// One pass over a `Table`, a key at a time.
pub struct Cursor {
    table: Arc<Table>,
    // The next rows in key order: rows sit in any order in memory, so their
    // keys are gathered a window at a time, on every core.
    wrows: Vec<u32>,
    wkeys: Arena,
    wpays: Vec<Option<Vec<u8>>>,
    start: usize,
    pos: usize,
    group: Group,
    ready: bool,
}

impl Cursor {
    pub fn new(table: Arc<Table>) -> Cursor {
        Cursor {
            table,
            wrows: Vec::new(),
            wkeys: Arena::default(),
            wpays: Vec::new(),
            start: 0,
            pos: 0,
            group: Group::default(),
            ready: false,
        }
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
        self.wpays.clear();
        if let Some(p) = &t.payloads {
            p.fill(&self.wrows, &mut self.wpays)?;
        }
        self.start = self.pos;
        Ok(())
    }

    /// Reads the next key's rows into `group`; false past the last.
    pub fn read(&mut self) -> Result<bool> {
        let n = self.table.keys.len();
        if self.pos >= n {
            return Ok(false);
        }
        self.fill()?;
        self.group.start(self.wkeys.get(self.pos - self.start));
        loop {
            let i = self.pos - self.start;
            self.group
                .add(self.wpays.get(i).and_then(Option::as_deref))?;
            self.pos += 1;
            if self.pos >= n {
                break;
            }
            self.fill()?;
            if self.wkeys.get(self.pos - self.start) != self.group.key.as_slice() {
                break;
            }
        }
        Ok(true)
    }

    /// The current key and its payload, after `read` returned true.
    pub fn entry(&self) -> (&[u8], Option<&[u8]>) {
        (&self.group.key, self.group.payload.as_deref())
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

/// Sorted chunks of keys, fed as they are read. A key may repeat: it is
/// one key, as rows are.
#[derive(Default)]
pub struct Stream {
    chunks: VecDeque<Arena>,
    pos: usize,
    last: Option<Vec<u8>>,
    ended: bool,
    group: Group,
    open: bool,
    ready: bool,
}

impl Stream {
    pub fn feed(&mut self, keys: Arena) -> Result<()> {
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
            self.chunks.push_back(keys);
        }
        Ok(())
    }

    pub fn end(&mut self) {
        self.ended = true;
    }

    /// Reads the next key into `group`, across chunks.
    fn state(&mut self) -> Result<State> {
        if self.ready {
            return Ok(State::Ready);
        }
        loop {
            let Some(k) = self.chunks.front() else {
                if !self.ended {
                    return Ok(State::Starved); // the key may go on in the next chunk
                }
                if self.open {
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
                (self.open, self.ready) = (false, true);
                return Ok(State::Ready);
            }
            if !self.open {
                self.group.start(key);
                self.open = true;
            }
            self.pos += 1;
        }
    }
}

/// A stream with sorted entries laid over it: their upserts in place of
/// the stream's entries of their keys, and its removes gone — a patch over
/// the keys a store holds, read back a chunk at a time.
pub struct Overlay {
    pub base: Stream,
    sorted: Arc<SortedEntries>,
    i: usize,
    current: Option<bool>, // Some(true): the sorted entry `i`; Some(false): the base's
}

impl Overlay {
    pub fn new(base: Stream, sorted: Arc<SortedEntries>) -> Overlay {
        Overlay {
            base,
            sorted,
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
            if self.i >= self.sorted.len() {
                if !ready {
                    return Ok(State::Done);
                }
                self.current = Some(false);
                continue;
            }
            let key = self.sorted.key(self.i);
            if ready && self.base.group.key.as_slice() < key {
                self.current = Some(false);
                continue;
            }
            if ready && self.base.group.key.as_slice() == key {
                self.base.ready = false; // the sorted entry stands for it
                continue;
            }
            if matches!(self.sorted.write(self.i), Write::Remove) {
                self.i += 1; // a remove: nothing of the key stays
                continue;
            }
            self.current = Some(true);
        }
    }

    fn entry(&self) -> (&[u8], Option<&[u8]>) {
        match self.current {
            Some(true) => (self.sorted.key(self.i), self.sorted.payload(self.i)),
            _ => (&self.base.group.key, None),
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
/// (with sorted entries laid over them, or not), or sorted entries (which may
/// be removes).
pub enum Source {
    Table(Box<Cursor>),
    Stream(Stream),
    Entries(Arc<SortedEntries>, usize),
    Overlay(Box<Overlay>),
}

impl Source {
    pub fn state(&mut self) -> Result<State> {
        match self {
            Source::Entries(sorted, i) => Ok(if *i < sorted.len() {
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

    /// The current write: an upsert with its payload, or a remove.
    #[inline]
    pub fn write(&self) -> Write<'_> {
        match self {
            Source::Entries(sorted, i) => sorted.write(*i),
            _ => Write::Upsert(self.entry().1),
        }
    }

    /// The current key and its payload; only when `state` is `Ready`.
    pub fn entry(&self) -> (&[u8], Option<&[u8]>) {
        match self {
            Source::Table(t) => t.entry(),
            Source::Stream(s) => (&s.group.key, None),
            Source::Entries(sorted, i) => (sorted.key(*i), sorted.payload(*i)),
            Source::Overlay(o) => o.entry(),
        }
    }

    pub fn advance(&mut self) {
        match self {
            Source::Table(t) => t.ready = false,
            Source::Stream(s) => s.ready = false,
            Source::Entries(_, i) => *i += 1,
            Source::Overlay(o) => o.advance(),
        }
    }

    /// The stream fed sorted chunks, if this is one.
    pub fn stream(&mut self) -> Option<&mut Stream> {
        match self {
            Source::Table(_) | Source::Entries(..) => None,
            Source::Stream(s) => Some(s),
            Source::Overlay(o) => Some(&mut o.base),
        }
    }
}
