//! The written content of a full replacement, as the merge-join reads it: in
//! key order, each row's version computed only when the join reaches it.
//!
//! A `Table` holds every key in place (an Arrow column, or keys packed once)
//! plus the permutation that sorts them — none when they arrive sorted. A
//! `Stream` is fed sorted chunks and holds one at a time.

use std::collections::VecDeque;

use rayon::prelude::*;

use crate::digest::{self, Digest};
use crate::format::{Error, Result};
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
    fn fill(&mut self, rows: &[u32], out: &mut Arena) -> Result<()>;

    fn rows(&self) -> bool {
        false
    }
}

/// Every row has the same version (a partition set's elements).
pub struct Constant(pub Vec<u8>);

impl Versions for Constant {
    fn fill(&mut self, rows: &[u32], out: &mut Arena) -> Result<()> {
        for _ in rows {
            out.push(&self.0);
        }
        Ok(())
    }
}

/// The rows of one key, read one at a time: their version is the group of
/// their row digests (`fold`), else the version they all share.
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

    fn add(&mut self, version: &[u8]) -> Result<()> {
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
        if self.fold {
            self.version.clear();
            self.version
                .extend_from_slice(&digest::group(&mut self.digests));
        }
    }
}

/// Versions computed a window of rows at a time, in key order.
const WINDOW: usize = 1 << 14;

/// Written rows, read a key — a group of rows — at a time, in key order.
pub struct Table {
    keys: Box<dyn Keys + Send>,
    order: Option<Vec<u32>>,
    versions: Box<dyn Versions>,
    // The next rows in key order, keys and versions side by side: rows sit in
    // any order in memory, so they are gathered a window at a time, on every core.
    wkeys: Arena,
    wvers: Arena,
    start: usize,
    pos: usize,
    group: Group,
    ready: bool,
}

impl Table {
    /// Sorts the keys unless they arrive sorted.
    pub fn new(keys: Box<dyn Keys + Send>, versions: Box<dyn Versions>) -> Result<Table> {
        let order = (!sort::is_sorted(&*keys)).then(|| sort::order(&*keys));
        Ok(Table {
            keys,
            order,
            versions,
            wkeys: Arena::default(),
            wvers: Arena::default(),
            start: 0,
            pos: 0,
            group: Group::default(),
            ready: false,
        })
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

    /// Makes the window hold row `pos`.
    fn fill(&mut self) -> Result<()> {
        if self.pos < self.start + self.wkeys.len() {
            return Ok(());
        }
        let end = (self.pos + WINDOW).min(self.len());
        let rows: Vec<u32> = match &self.order {
            Some(o) => o[self.pos..end].to_vec(),
            None => (self.pos as u32..end as u32).collect(),
        };
        self.wkeys.clear();
        gather(&*self.keys, &rows, &mut self.wkeys);
        self.wvers.clear();
        self.versions.fill(&rows, &mut self.wvers)?;
        self.start = self.pos;
        Ok(())
    }

    /// Reads the next key's rows into `group`; false past the last.
    fn group(&mut self) -> Result<bool> {
        if self.pos >= self.len() {
            return Ok(false);
        }
        self.fill()?;
        let fold = self.versions.rows();
        self.group
            .start(self.wkeys.get(self.pos - self.start), fold);
        loop {
            self.group.add(self.wvers.get(self.pos - self.start))?;
            self.pos += 1;
            if self.pos >= self.len() {
                break;
            }
            self.fill()?;
            if self.wkeys.get(self.pos - self.start) != self.group.key.as_slice() {
                break;
            }
        }
        self.group.finish();
        Ok(true)
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
            self.group.add(v.get(self.pos))?;
            self.pos += 1;
        }
    }
}

/// The new side of a replacement.
pub enum Source {
    Table(Table),
    Stream(Stream),
}

impl Source {
    pub fn state(&mut self) -> Result<State> {
        match self {
            Source::Table(t) => {
                if !t.ready {
                    if !t.group()? {
                        return Ok(State::Done);
                    }
                    t.ready = true;
                }
                Ok(State::Ready)
            }
            Source::Stream(s) => s.state(),
        }
    }

    /// The current key; only when `state` is `Ready`.
    #[inline]
    pub fn key(&self) -> &[u8] {
        match self {
            Source::Table(t) => &t.group.key,
            Source::Stream(s) => &s.group.key,
        }
    }

    /// The current key and its version; only when `state` is `Ready`.
    pub fn entry(&self) -> (&[u8], &[u8]) {
        match self {
            Source::Table(t) => (&t.group.key, &t.group.version),
            Source::Stream(s) => (&s.group.key, &s.group.version),
        }
    }

    pub fn advance(&mut self) {
        match self {
            Source::Table(t) => t.ready = false,
            Source::Stream(s) => s.ready = false,
        }
    }
}
