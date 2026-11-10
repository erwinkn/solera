//! The event envelope. One event is what the engine journals for one state
//! change (a batch commit, a source commit, a deploy's change to an edge),
//! and it may carry parts from several units: a batch commit holds the
//! output commit (B4), the edge's new piece and points with the batch record
//! (B3), and the key outcomes that changed (B6).
//!
//! Bytes: `[FORMAT] (tag, len, body)*`, `tag` one byte, `len` a varint, the
//! body the part's own encoding. A reader refuses an unknown tag, a part
//! whose body it does not consume exactly, and an empty event.
//!
//! Tags are allocated by unit, so units add parts without touching each
//! other's: 0x01-0x1f progress and planner (B3), 0x20-0x2f output and
//! source commits (B4), 0x30-0x3f key outcomes (B6), 0x40-0x4f staleness
//! (B7), 0x50-0x5f retention (B9).
//!
//! [`crate::Tracking::apply`] decodes every part, checks every part against
//! the state as it was before the event, and only then applies them in order,
//! so an event applies whole or not at all. Applying a checked part cannot
//! fail. Parts of one event therefore never depend on each other's effects
//! to pass their check: an edge is opened by an event of its own.

use crate::codec::{Dec, Enc};
use crate::commit;
use crate::error::{format, Result};
use crate::progress;
use crate::stale;

pub const FORMAT: u8 = 1;

/// One part of an event. Each unit adds its variants and the lines that
/// dispatch to its module.
#[derive(Clone, Debug, PartialEq)]
pub enum Part {
    /// B3: an edge starts, with empty progress.
    EdgeOpen(progress::EdgeOpen),
    /// B3: an edge ends: its input was removed, or its consumer.
    EdgeClose(progress::EdgeClose),
    /// B3: an edge is renamed, keeping its progress.
    EdgeRename(progress::EdgeRename),
    /// B3: an edge's progress starts over, empty or held.
    Reset(progress::Reset),
    /// B3: what a batch commit writes to a keyed edge.
    Batch(progress::BatchProgress),
    /// B3: an unkeyed edge consumed its input through a commit.
    Consumed(progress::Consumed),
    /// B4: a commit on an output: its layer, its head, its live count.
    OutputCommit(commit::OutputCommit),
    /// B7: an edge's conditions now.
    Watch(stale::SetWatch),
    /// B7: what a refresh or a notice found for an edge.
    Witness(stale::Witness),
    /// B7: an output's full run starts or completes.
    Incomplete(stale::Incomplete),
}

impl Part {
    fn tag(&self) -> u8 {
        match self {
            Part::EdgeOpen(_) => progress::TAG_EDGE_OPEN,
            Part::EdgeClose(_) => progress::TAG_EDGE_CLOSE,
            Part::EdgeRename(_) => progress::TAG_EDGE_RENAME,
            Part::Reset(_) => progress::TAG_RESET,
            Part::Batch(_) => progress::TAG_BATCH,
            Part::Consumed(_) => progress::TAG_CONSUMED,
            Part::OutputCommit(_) => commit::TAG_OUTPUT_COMMIT,
            Part::Watch(_) => stale::TAG_WATCH,
            Part::Witness(_) => stale::TAG_WITNESS,
            Part::Incomplete(_) => stale::TAG_INCOMPLETE,
        }
    }

    fn encode_body(&self, e: &mut Enc) {
        match self {
            Part::EdgeOpen(p) => p.encode(e),
            Part::EdgeClose(p) => p.encode(e),
            Part::EdgeRename(p) => p.encode(e),
            Part::Reset(p) => p.encode(e),
            Part::Batch(p) => p.encode(e),
            Part::Consumed(p) => p.encode(e),
            Part::OutputCommit(p) => p.encode(e),
            Part::Watch(p) => p.encode(e),
            Part::Witness(p) => p.encode(e),
            Part::Incomplete(p) => p.encode(e),
        }
    }

    fn decode(tag: u8, d: &mut Dec<'_>) -> Result<Part> {
        Ok(match tag {
            progress::TAG_EDGE_OPEN => Part::EdgeOpen(progress::EdgeOpen::decode(d)?),
            progress::TAG_EDGE_CLOSE => Part::EdgeClose(progress::EdgeClose::decode(d)?),
            progress::TAG_EDGE_RENAME => Part::EdgeRename(progress::EdgeRename::decode(d)?),
            progress::TAG_RESET => Part::Reset(progress::Reset::decode(d)?),
            progress::TAG_BATCH => Part::Batch(progress::BatchProgress::decode(d)?),
            progress::TAG_CONSUMED => Part::Consumed(progress::Consumed::decode(d)?),
            commit::TAG_OUTPUT_COMMIT => Part::OutputCommit(commit::OutputCommit::decode(d)?),
            stale::TAG_WATCH => Part::Watch(stale::SetWatch::decode(d)?),
            stale::TAG_WITNESS => Part::Witness(stale::Witness::decode(d)?),
            stale::TAG_INCOMPLETE => Part::Incomplete(stale::Incomplete::decode(d)?),
            t => return format(format!("unknown part tag {t:#04x}")),
        })
    }
}

/// One journaled event: its parts, in the order they apply.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Event {
    pub parts: Vec<Part>,
}

impl Event {
    pub fn of(parts: Vec<Part>) -> Event {
        Event { parts }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Enc::new();
        out.u8(FORMAT);
        let mut body = Enc::new();
        for p in &self.parts {
            body.buf.clear();
            p.encode_body(&mut body);
            out.u8(p.tag());
            out.bytes(&body.buf);
        }
        out.buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Event> {
        let mut d = Dec::new(bytes);
        let f = d.u8()?;
        if f != FORMAT {
            return format(format!("event format {f}, expected {FORMAT}"));
        }
        let mut parts = Vec::new();
        while !d.done() {
            let tag = d.u8()?;
            let mut body = Dec::new(d.bytes()?);
            parts.push(Part::decode(tag, &mut body)?);
            body.end()?;
        }
        if parts.is_empty() {
            return format("an event without parts");
        }
        Ok(Event { parts })
    }
}
