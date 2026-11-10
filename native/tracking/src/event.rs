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
//! fail. Parts of one event change distinct things ([`Part::touches`]; an
//! event where two parts name the same edge or output is refused), so no
//! part's check depends on another's effect: an edge is opened by an event
//! of its own.

use crate::codec::{Dec, Enc};
use crate::commit;
use crate::error::{format, Result};
use crate::outcomes;
use crate::progress;
use std::borrow::Cow;

/// A thing one part of an event changes, as (kind, id).
pub type Touch<'a> = (&'static str, Cow<'a, [u8]>);

fn on<'a>(kind: &'static str, id: &'a [u8]) -> Touch<'a> {
    (kind, Cow::Borrowed(id))
}

/// An edge's outcome state and its record outputs, which B6 changes when
/// B3 closes, resets or renames the edge ([crate::outcomes::follow]).
fn outcome_state(edge: &[u8]) -> impl Iterator<Item = Touch<'_>> {
    std::iter::once(on("outcomes", edge)).chain(
        [outcomes::FAILURES, outcomes::DUE, outcomes::PASS].map(|k| {
            (
                "output",
                Cow::Owned(outcomes::output_id(k, edge).into_vec()),
            )
        }),
    )
}

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
    /// B6: a batch's outcomes: an edge's counts, conditions and retry step.
    Outcomes(outcomes::Outcomes),
    /// B6: a retry pass begins.
    PassStart(outcomes::PassStart),
    /// B6: a full run of a per-key asset asked for by hand raises its input's rebuild counter.
    Rebuild(outcomes::Rebuild),
    /// B4: an output's full run ends without a commit of its own.
    FullRunDone(commit::FullRunDone),
}

impl Part {
    /// What the part changes, as (kind, id): parts of one event change
    /// distinct things, so each part's check against the state before the
    /// event still holds when the parts before it have applied.
    pub fn touches(&self) -> Vec<Touch<'_>> {
        match self {
            Part::EdgeOpen(p) if p.keyed => vec![on("edge", &p.edge), on("outcomes", &p.edge)],
            Part::EdgeOpen(p) => vec![on("edge", &p.edge)],
            Part::EdgeClose(p) => [on("edge", &p.edge)]
                .into_iter()
                .chain(outcome_state(&p.edge))
                .collect(),
            Part::EdgeRename(p) if p.from == p.to => vec![on("edge", &p.from)],
            Part::EdgeRename(p) => [on("edge", &p.from), on("edge", &p.to)]
                .into_iter()
                .chain(outcome_state(&p.from))
                .chain(outcome_state(&p.to))
                .collect(),
            Part::Reset(p) => [on("edge", &p.edge)]
                .into_iter()
                .chain(outcome_state(&p.edge))
                .collect(),
            Part::Batch(p) if p.reset => [on("edge", &p.edge)]
                .into_iter()
                .chain(outcome_state(&p.edge))
                .collect(),
            Part::Batch(p) => vec![on("edge", &p.edge)],
            Part::Consumed(p) => vec![on("edge", &p.edge)],
            Part::OutputCommit(p) => vec![on("output", &p.output)],
            Part::Outcomes(p) => {
                let mut v = vec![on("outcomes", &p.edge)];
                if !p.points.is_empty() {
                    v.push(on("edge", &p.edge));
                }
                if p.step.as_ref().is_some_and(|s| s.to.is_none()) {
                    v.push((
                        "output",
                        Cow::Owned(outcomes::output_id(outcomes::PASS, &p.edge).into_vec()),
                    ));
                }
                v
            }
            Part::PassStart(p) => vec![on("outcomes", &p.edge)],
            Part::Rebuild(p) => vec![on("outcomes", &p.edge)],
            Part::FullRunDone(p) => vec![on("output", &p.output)],
        }
    }

    fn tag(&self) -> u8 {
        match self {
            Part::EdgeOpen(_) => progress::TAG_EDGE_OPEN,
            Part::EdgeClose(_) => progress::TAG_EDGE_CLOSE,
            Part::EdgeRename(_) => progress::TAG_EDGE_RENAME,
            Part::Reset(_) => progress::TAG_RESET,
            Part::Batch(_) => progress::TAG_BATCH,
            Part::Consumed(_) => progress::TAG_CONSUMED,
            Part::OutputCommit(_) => commit::TAG_OUTPUT_COMMIT,
            Part::Outcomes(_) => outcomes::TAG_OUTCOMES,
            Part::PassStart(_) => outcomes::TAG_PASS_START,
            Part::Rebuild(_) => outcomes::TAG_REBUILD,
            Part::FullRunDone(_) => commit::TAG_FULL_RUN_DONE,
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
            Part::Outcomes(p) => p.encode(e),
            Part::PassStart(p) => p.encode(e),
            Part::Rebuild(p) => p.encode(e),
            Part::FullRunDone(p) => p.encode(e),
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
            outcomes::TAG_OUTCOMES => Part::Outcomes(outcomes::Outcomes::decode(d)?),
            outcomes::TAG_PASS_START => Part::PassStart(outcomes::PassStart::decode(d)?),
            outcomes::TAG_REBUILD => Part::Rebuild(outcomes::Rebuild::decode(d)?),
            commit::TAG_FULL_RUN_DONE => Part::FullRunDone(commit::FullRunDone::decode(d)?),
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
