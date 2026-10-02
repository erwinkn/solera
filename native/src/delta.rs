//! What a write does to one key — the one rule every resolver applies: the
//! sparse reader, the streaming patch and replacement, and the engine
//! cache's lookups and merges (docs/resolved-commits.md §6).
//!
//! A write of a key is an upsert at a version or a removal; what the index
//! held is absent, live at a known version and locator, or — from the
//! filters alone — live at some other version. A new key is added, a removed
//! live key deleted, a changed version written, an unchanged one dropped.
//! Written entries carry the writer's generation as their locator, and a key
//! read live its predecessor.

use crate::format::{Options, Result};
use crate::rows::Arena;
use crate::stream::Writer;

/// What the index holds for a key.
#[derive(Clone, Copy)]
pub enum Old<'a> {
    /// No live entry: never written, or deleted.
    Absent,
    /// Live, at this version and locator.
    Live(&'a [u8], u64),
    /// Live at a version other than the one written, as the filters said:
    /// neither is known (or, behind a false positive, not there at all).
    Other,
}

/// Keys a delta changed, with their versions, up to a limit: beyond it, `None`.
pub struct Collected {
    pub upserts: Option<(Arena, Arena)>,
    pub removes: Option<Arena>,
    limit: usize,
}

impl Collected {
    fn new(limit: usize) -> Collected {
        Collected {
            upserts: Some((Arena::default(), Arena::default())),
            removes: Some(Arena::default()),
            limit,
        }
    }

    fn add(&mut self, key: &[u8], version: Option<&[u8]>) {
        let total = self.upserts.as_ref().map_or(0, |a| a.0.len())
            + self.removes.as_ref().map_or(0, |a| a.len());
        if total >= self.limit {
            self.upserts = None;
            self.removes = None;
        }
        match version {
            Some(v) => {
                if let Some((k, vs)) = &mut self.upserts {
                    k.push(key);
                    vs.push(v);
                }
            }
            None => {
                if let Some(a) = &mut self.removes {
                    a.push(key);
                }
            }
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

    /// A write of `key` — an upsert at `new`, or with None a removal — over
    /// `old`. Keys come strictly increasing.
    pub fn apply(&mut self, key: &[u8], new: Option<&[u8]>, old: Old) -> Result<()> {
        let g = self.generation;
        let before = match old {
            Old::Live(v, l) => Some((v, l)),
            _ => None,
        };
        match (new, old) {
            (None, Old::Absent) => return Ok(()),
            (None, _) => {
                self.writer.push(key, b"", true, g, before)?;
                self.removed += 1;
            }
            (Some(v), Old::Absent) => {
                self.writer.push(key, v, false, g, None)?;
                self.added += 1;
            }
            (Some(v), Old::Live(was, _)) if was == v => return Ok(()),
            (Some(v), _) => {
                self.writer.push(key, v, false, g, before)?;
                self.changed += 1;
            }
        }
        self.collected.add(key, new);
        Ok(())
    }

    /// Ends the delta: its last file goes to `writer.files`.
    pub fn finish(&mut self) -> Result<()> {
        self.writer.finish(false)
    }
}
