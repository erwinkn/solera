//! The written content of a full replacement, as the merge-join reads it: in
//! key order, each row's version computed only when the join reaches it.
//!
//! A `Table` holds every key in place (an Arrow column, or keys packed once)
//! plus the permutation that sorts them — none when they arrive sorted. A
//! `Stream` is fed sorted chunks and holds one at a time.

use std::collections::VecDeque;

use rayon::prelude::*;

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
pub trait Versions: Send + Sync {
    fn fill(&mut self, rows: &[u32], out: &mut Arena) -> Result<()>;
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

/// Versions computed a window of rows at a time, in key order.
const WINDOW: usize = 1 << 14;

fn duplicate(key: &[u8]) -> Error {
    Error::Value(format!("duplicate key {:?}", String::from_utf8_lossy(key)))
}

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
}

impl Table {
    /// Sorts the keys unless they arrive sorted. A duplicate key is an error
    /// here when they arrive sorted, else when the replacement reaches it.
    pub fn new(keys: Box<dyn Keys + Send>, versions: Box<dyn Versions>) -> Result<Table> {
        let order = match sort::is_sorted(&*keys) {
            Ok(true) => None,
            Err(i) => return Err(duplicate(keys.key(i))),
            Ok(false) => Some(sort::order(&*keys)),
        };
        Ok(Table {
            keys,
            order,
            versions,
            wkeys: Arena::default(),
            wvers: Arena::default(),
            start: 0,
            pos: 0,
        })
    }

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
        let last = (!self.wkeys.is_empty()).then(|| self.wkeys.get(self.wkeys.len() - 1).to_vec());
        self.wkeys.clear();
        gather(&*self.keys, &rows, &mut self.wkeys);
        let first = (last.as_deref() == Some(self.wkeys.get(0))).then_some(0);
        if let Some(i) = first
            .or_else(|| (1..self.wkeys.len()).find(|&i| self.wkeys.get(i - 1) == self.wkeys.get(i)))
        {
            return Err(duplicate(self.wkeys.get(i)));
        }
        self.wvers.clear();
        self.versions.fill(&rows, &mut self.wvers)?;
        self.start = self.pos;
        Ok(())
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

/// Sorted chunks of `(key, version)`, fed as they are read.
#[derive(Default)]
pub struct Stream {
    chunks: VecDeque<(Arena, Arena)>,
    pos: usize,
    last: Option<Vec<u8>>,
    ended: bool,
}

impl Stream {
    pub fn feed(&mut self, keys: Arena, versions: Arena) -> Result<()> {
        for i in 0..keys.len() {
            let k = keys.get(i);
            let prev = if i > 0 {
                Some(keys.get(i - 1))
            } else {
                self.last.as_deref()
            };
            if let Some(p) = prev {
                if k == p {
                    return Err(duplicate(k));
                }
                if k < p {
                    return Err(Error::Value(format!(
                        "keys must arrive sorted: {:?} then {:?}",
                        String::from_utf8_lossy(p),
                        String::from_utf8_lossy(k)
                    )));
                }
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
                if t.pos >= t.len() {
                    return Ok(State::Done);
                }
                t.fill()?;
                Ok(State::Ready)
            }
            Source::Stream(s) => loop {
                match s.chunks.front() {
                    Some((k, _)) if s.pos < k.len() => return Ok(State::Ready),
                    Some(_) => {
                        s.chunks.pop_front();
                        s.pos = 0;
                    }
                    None if s.ended => return Ok(State::Done),
                    None => return Ok(State::Starved),
                }
            },
        }
    }

    /// The current key; only when `state` is `Ready`.
    #[inline]
    pub fn key(&self) -> &[u8] {
        match self {
            Source::Table(t) => t.wkeys.get(t.pos - t.start),
            Source::Stream(s) => s.chunks[0].0.get(s.pos),
        }
    }

    /// The current key and its version; only when `state` is `Ready`.
    pub fn entry(&self) -> (&[u8], &[u8]) {
        match self {
            Source::Table(t) => (t.wkeys.get(t.pos - t.start), t.wvers.get(t.pos - t.start)),
            Source::Stream(s) => {
                let (k, v) = &s.chunks[0];
                (k.get(s.pos), v.get(s.pos))
            }
        }
    }

    pub fn advance(&mut self) {
        match self {
            Source::Table(t) => t.pos += 1,
            Source::Stream(s) => s.pos += 1,
        }
    }
}
