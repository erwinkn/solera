//! The written content of a full replacement, as the merge-join reads it: in
//! key order, each row's version computed only when the join reaches it.
//!
//! A `Table` holds every key in place (an Arrow column, or keys packed once)
//! plus the permutation that sorts them — none when they arrive sorted. A
//! `Stream` is fed sorted chunks and holds one at a time.

use std::collections::VecDeque;

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
    window: Arena,
    start: usize,
    pos: usize,
}

impl Table {
    /// Sorts the keys unless they arrive sorted; a duplicate key is an error.
    pub fn new(keys: Box<dyn Keys + Send>, versions: Box<dyn Versions>) -> Result<Table> {
        let order = match sort::is_sorted(&*keys) {
            Ok(true) => None,
            Err(i) => return Err(duplicate(keys.key(i))),
            Ok(false) => {
                let order = sort::order(&*keys);
                if let Some(i) = sort::duplicate(&*keys, &order) {
                    return Err(duplicate(keys.key(i as usize)));
                }
                Some(order)
            }
        };
        Ok(Table {
            keys,
            order,
            versions,
            window: Arena::default(),
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

    #[inline]
    fn row(&self, i: usize) -> usize {
        self.order.as_ref().map_or(i, |o| o[i] as usize)
    }

    #[inline]
    fn key(&self) -> &[u8] {
        self.keys.key(self.row(self.pos))
    }

    fn version(&mut self) -> Result<&[u8]> {
        if self.pos >= self.start + self.window.len() {
            self.window.clear();
            self.start = self.pos;
            let end = (self.pos + WINDOW).min(self.len());
            let rows: Vec<u32> = match &self.order {
                Some(o) => o[self.pos..end].to_vec(),
                None => (self.pos as u32..end as u32).collect(),
            };
            self.versions.fill(&rows, &mut self.window)?;
        }
        Ok(self.window.get(self.pos - self.start))
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
    pub fn state(&mut self) -> State {
        match self {
            Source::Table(t) => {
                if t.pos < t.len() {
                    State::Ready
                } else {
                    State::Done
                }
            }
            Source::Stream(s) => loop {
                match s.chunks.front() {
                    Some((k, _)) if s.pos < k.len() => return State::Ready,
                    Some(_) => {
                        s.chunks.pop_front();
                        s.pos = 0;
                    }
                    None if s.ended => return State::Done,
                    None => return State::Starved,
                }
            },
        }
    }

    /// The current key; only when `state` is `Ready`.
    #[inline]
    pub fn key(&self) -> &[u8] {
        match self {
            Source::Table(t) => t.key(),
            Source::Stream(s) => s.chunks[0].0.get(s.pos),
        }
    }

    /// The current key and its version.
    pub fn entry(&mut self) -> Result<(&[u8], &[u8])> {
        match self {
            Source::Table(t) => {
                t.version()?;
                let v = t.window.get(t.pos - t.start);
                Ok((t.keys.key(t.row(t.pos)), v))
            }
            Source::Stream(s) => {
                let (k, v) = &s.chunks[0];
                Ok((k.get(s.pos), v.get(s.pos)))
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
