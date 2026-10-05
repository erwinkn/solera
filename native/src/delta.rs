//! What a write does to one key — the one rule every resolver applies, the
//! sparse and the streamed (`layers::DeltaWriter`; docs/versions.md).
//!
//! A write of a key is an upsert, which may carry a payload (a source's
//! version, a stored outcome), or a removal; what the index held is
//! absent, or live at a known generation and payload: every writer reads it
//! exactly. Writing a key changes it: a new key is
//! added, a removed live key deleted, any other upsert written — unless it
//! carries a payload equal to the live entry's, which says the key is as it
//! was.

use crate::rows::Arena;

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
