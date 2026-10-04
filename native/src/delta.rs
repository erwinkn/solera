//! What a write does to one key — the one rule every resolver applies: the
//! sparse reader, the streaming patch and replacement, and the engine
//! cache's lookups and merges (docs/resolved-commits.md §6, docs/versions.md).
//!
//! A write of a key is an upsert, which may carry a payload (a source's
//! version, a failure record), or a removal; what the index held is
//! absent, or live at a known generation and payload: every writer reads it
//! exactly. Writing a key changes it: a new key is
//! added, a removed live key deleted, any other upsert written — unless it
//! carries a payload equal to the live entry's, which says the key is as it
//! was. Written entries carry the writer's generation, and a key read live
//! its predecessor's.

use crate::format::{Options, Result};
use crate::rows::Arena;
use crate::stream::Writer;

/// A write of one key.
#[derive(Clone, Copy)]
pub enum Write<'a> {
    /// An upsert, with its payload if it carries one.
    Upsert(Option<&'a [u8]>),
    Remove,
}

/// What the index holds for a key.
#[derive(Clone, Copy)]
pub enum Old<'a> {
    /// No live entry: never written, or deleted.
    Absent,
    /// Live, at this generation, with this payload.
    Live(u64, Option<&'a [u8]>),
}

/// Keys a delta changed, up to a limit: beyond it, `None`.
pub struct Collected {
    pub upserts: Option<Arena>,
    pub removes: Option<Arena>,
    limit: usize,
}

impl Collected {
    pub(crate) fn new(limit: usize) -> Collected {
        Collected {
            upserts: Some(Arena::default()),
            removes: Some(Arena::default()),
            limit,
        }
    }

    pub(crate) fn add(&mut self, key: &[u8], removed: bool) {
        let total = self.upserts.as_ref().map_or(0, |a| a.len())
            + self.removes.as_ref().map_or(0, |a| a.len());
        if total >= self.limit {
            self.upserts = None;
            self.removes = None;
        }
        let into = if removed {
            &mut self.removes
        } else {
            &mut self.upserts
        };
        if let Some(a) = into {
            a.push(key);
        }
    }
}

/// A delta being written: the entries, the count changes, the changed keys.
pub struct Delta {
    pub writer: Writer,
    pub added: u64,
    pub removed: u64,
    pub changed: u64,
    pub collected: Collected,
    generation: u64,
}

impl Delta {
    pub fn new(o: Options, max_file_bytes: usize, collect: usize, generation: u64) -> Delta {
        Delta {
            writer: Writer::new(o, max_file_bytes),
            added: 0,
            removed: 0,
            changed: 0,
            collected: Collected::new(collect),
            generation,
        }
    }

    /// A write of `key` over `old`. Keys come strictly increasing.
    pub fn apply(&mut self, key: &[u8], new: Write, old: Old) -> Result<()> {
        let g = self.generation;
        // What it replaces: its generation, and its payload on a payload-bearing index.
        let before = match old {
            Old::Live(was, payload) => Some((was, payload)),
            _ => None,
        };
        match (new, old) {
            (Write::Remove, Old::Absent) => return Ok(()),
            (Write::Remove, _) => {
                self.writer.push(key, g, true, None, before)?;
                self.removed += 1;
            }
            (Write::Upsert(p), Old::Absent) => {
                self.writer.push(key, g, false, p, None)?;
                self.added += 1;
            }
            (Write::Upsert(Some(p)), Old::Live(_, Some(was))) if was == p => return Ok(()),
            (Write::Upsert(p), _) => {
                self.writer.push(key, g, false, p, before)?;
                self.changed += 1;
            }
        }
        self.collected.add(key, matches!(new, Write::Remove));
        Ok(())
    }

    /// Ends the delta: its last file goes to `writer.files`.
    pub fn finish(&mut self) -> Result<()> {
        self.writer.finish(false)
    }
}
