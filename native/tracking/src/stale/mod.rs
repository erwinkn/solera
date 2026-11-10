//! Staleness kept exact at every commit (docs/tracking-design.md §6, in the
//! decided form: one witness due key per input; `handoffs/decisions.md`,
//! P14 and SOLE-7's model in `tests/tracking/staleness.py`).
//!
//! Per keyed edge the core keeps what it knows: `fresh` (nothing is due) or
//! `stale(w)` (key `w` is due), exact for the edge's progress and conditions
//! as they were at one generation, as of one upstream commit `x`. Counts are
//! computed on request ([`query`]).
//!
//! - Every change to an edge's progress (a batch commit, a reset) and every
//!   change of its conditions ([`Watch`]) raises the edge's generation and
//!   records which key ranges changed, so what is known stops being current.
//!   Applying events never reads the index.
//! - [`refresh`] brings an edge current at its upstream's head, off the event
//!   loop: rule 1 over the upstream's changes since `x` outside the changed
//!   ranges, then one probe (the planner with a limit of one key) from the first
//!   changed range; a stale witness is rechecked at the head and probed for only
//!   if it is no longer due. The result is a [`Witness`] part to journal.
//! - [`notice`]: the same rule 1 over a commit's entries as the commit builder
//!   produces them, with no index read, for every current edge reading that
//!   output. Its parts go into the commit's own event.
//!
//! Generations are drawn from one counter and never reused, so a part
//! computed for an edge since closed never applies to one reopened under its
//! name. A [`Witness`] part applies only if the edge's generation is still the
//! one it was computed for, and never moves `x` back: a part computed against
//! progress that has since moved is dropped, and the next refresh recomputes
//! it. So a lost or late notice can delay an answer but never change it
//! (the amendment's rule 7).

mod query;
mod refresh;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::codec::{Dec, Enc};
use crate::error::{format, invalid, Result};
use crate::event::Part;
use crate::progress::{Conditions, Key, KeyRange};
use crate::state::{Id, Tracking};

pub use query::{graph, Counts, Partition, Status};
pub use refresh::{notice, refresh, retry_due, Ask, Due, NoneDue};

pub const TAG_WATCH: u8 = 0x40;
pub const TAG_WITNESS: u8 = 0x41;
pub const TAG_INCOMPLETE: u8 = 0x42;

/// Past this many changed ranges an edge is probed from its first key.
const MAX_RANGES: usize = 64;

/// Why the witness is due.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Reason {
    /// Upstream changed the key since the consumer processed it.
    InputChanged = 1,
    /// The consumer's patterns, context, definition or rebuild counter changed.
    ConditionsChanged = 2,
    /// The consumer never processed the key since its progress started over.
    NeverProcessed = 3,
    /// The key failed and its retry is due.
    RetryDue = 4,
}

impl Reason {
    pub fn name(self) -> &'static str {
        match self {
            Reason::InputChanged => "input changed",
            Reason::ConditionsChanged => "conditions changed",
            Reason::NeverProcessed => "never processed",
            Reason::RetryDue => "retry due",
        }
    }

    fn from_u8(b: u8) -> Result<Reason> {
        Ok(match b {
            1 => Reason::InputChanged,
            2 => Reason::ConditionsChanged,
            3 => Reason::NeverProcessed,
            4 => Reason::RetryDue,
            _ => return format(format!("stale reason {b}")),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum State {
    Fresh,
    Stale {
        key: Key,
        reason: Reason,
        /// When the edge last went from fresh to stale.
        since_ms: u64,
    },
}

/// What is known of an edge: exact for its progress and conditions at
/// generation `gen`, as of commit `x` of its upstream.
#[derive(Clone, Debug, PartialEq)]
pub struct Known {
    pub gen: u64,
    pub x: u64,
    pub state: State,
}

/// What changed in an edge's progress since what is known was computed.
#[derive(Clone, Debug, PartialEq)]
pub enum Changed {
    Ranges(Vec<KeyRange>),
    All,
}

impl Changed {
    fn add(&mut self, r: &KeyRange) {
        if let Changed::Ranges(rs) = self {
            if rs.len() >= MAX_RANGES {
                *self = Changed::All;
            } else {
                rs.push(r.clone());
            }
        }
    }

    pub fn contains(&self, k: &[u8]) -> bool {
        match self {
            Changed::All => true,
            Changed::Ranges(rs) => rs.iter().any(|r| r.contains(k)),
        }
    }
}

/// An edge's conditions now, as the engine plans it.
#[derive(Clone, Debug, PartialEq)]
pub struct Watch {
    pub conditions: Arc<Conditions>,
    /// The consumer's own output, read under a `held` piece.
    pub own: Option<Id>,
    /// The edge waits while its upstream is incomplete (P12: an input may opt out).
    pub gate: bool,
}

/// The staleness of one keyed edge.
#[derive(Clone, Debug, PartialEq)]
pub struct Input {
    pub watch: Option<Watch>,
    /// Raised by every change to the edge's progress or conditions.
    pub gen: u64,
    /// What changed since `known` was computed, while it is not current.
    pub changed: Changed,
    pub known: Option<Known>,
}

impl Default for Input {
    fn default() -> Input {
        Input {
            watch: None,
            gen: 0,
            changed: Changed::All,
            known: None,
        }
    }
}

impl Input {
    /// What is known holds for the edge's progress and conditions now.
    pub fn current(&self) -> Option<&Known> {
        self.known.as_ref().filter(|k| k.gen == self.gen)
    }

    fn moved(&mut self, gen: u64, r: Option<&KeyRange>) {
        if self.current().is_some() {
            self.changed = Changed::Ranges(vec![]);
        }
        match r {
            Some(r) => self.changed.add(r),
            None => self.changed = Changed::All,
        }
        self.gen = gen;
    }
}

/// Staleness of every keyed edge, and the outputs in the middle of a full run.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Stale {
    pub inputs: BTreeMap<Id, Input>,
    /// Outputs whose full run has started and not completed (rule 8).
    pub incomplete: BTreeSet<Id>,
    /// The last generation given out.
    pub gens: u64,
}

impl Stale {
    /// Whether the gate holds an edge reading `output`.
    pub fn held(&self, input: &Input, output: &[u8]) -> bool {
        input.watch.as_ref().is_some_and(|w| w.gate) && self.incomplete.contains(output)
    }

    /// Follows the other units' parts that change an edge's progress. Called
    /// for every part, before it applies.
    pub(crate) fn touch(&mut self, p: &Part) {
        match p {
            Part::EdgeOpen(o) if o.keyed => {
                self.gens += 1;
                let i = Input {
                    gen: self.gens,
                    ..Input::default()
                };
                self.inputs.insert(o.edge.clone(), i);
            }
            Part::EdgeClose(c) => {
                self.inputs.remove(&c.edge);
            }
            Part::EdgeRename(r) => {
                if let Some(mut i) = self.inputs.remove(&r.from) {
                    self.gens += 1;
                    i.moved(self.gens, None);
                    self.inputs.insert(r.to.clone(), i);
                }
            }
            Part::Reset(r) => {
                if let Some(i) = self.inputs.get_mut(&r.edge) {
                    self.gens += 1;
                    i.moved(self.gens, None);
                }
            }
            Part::Batch(b) => {
                if let Some(i) = self.inputs.get_mut(&b.edge) {
                    self.gens += 1;
                    i.moved(self.gens, (!b.reset).then_some(&b.range));
                }
            }
            _ => {}
        }
    }

    pub(crate) fn encode(&self, e: &mut Enc) {
        e.uv(self.inputs.len() as u64);
        for (id, i) in &self.inputs {
            e.bytes(id);
            match &i.watch {
                None => e.u8(0),
                Some(w) => {
                    e.u8(1);
                    w.encode(e);
                }
            }
            e.uv(i.gen);
            match &i.changed {
                Changed::All => e.u8(0),
                Changed::Ranges(rs) => {
                    e.u8(1);
                    e.uv(rs.len() as u64);
                    for r in rs {
                        r.encode(e);
                    }
                }
            }
            match &i.known {
                None => e.u8(0),
                Some(k) => {
                    e.u8(1);
                    e.uv(k.gen);
                    e.uv(k.x);
                    encode_state(e, &k.state);
                }
            }
        }
        e.uv(self.incomplete.len() as u64);
        for o in &self.incomplete {
            e.bytes(o);
        }
        e.uv(self.gens);
    }

    pub(crate) fn decode(d: &mut Dec<'_>) -> Result<Stale> {
        let mut s = Stale::default();
        for _ in 0..d.count()? {
            let id: Id = d.bytes()?.into();
            let watch = match d.u8()? {
                0 => None,
                1 => Some(Watch::decode(d)?),
                t => return format(format!("watch tag {t}")),
            };
            let gen = d.uv()?;
            let changed = match d.u8()? {
                0 => Changed::All,
                1 => Changed::Ranges(
                    (0..d.count()?)
                        .map(|_| KeyRange::decode(d))
                        .collect::<Result<_>>()?,
                ),
                t => return format(format!("changed tag {t}")),
            };
            let known = match d.u8()? {
                0 => None,
                1 => Some(Known {
                    gen: d.uv()?,
                    x: d.uv()?,
                    state: decode_state(d)?,
                }),
                t => return format(format!("known tag {t}")),
            };
            s.inputs.insert(
                id,
                Input {
                    watch,
                    gen,
                    changed,
                    known,
                },
            );
        }
        for _ in 0..d.count()? {
            s.incomplete.insert(d.bytes()?.into());
        }
        s.gens = d.uv()?;
        Ok(s)
    }
}

fn encode_state(e: &mut Enc, s: &State) {
    match s {
        State::Fresh => e.u8(0),
        State::Stale {
            key,
            reason,
            since_ms,
        } => {
            e.u8(1);
            e.bytes(key);
            e.u8(*reason as u8);
            e.uv(*since_ms);
        }
    }
}

fn decode_state(d: &mut Dec<'_>) -> Result<State> {
    Ok(match d.u8()? {
        0 => State::Fresh,
        1 => State::Stale {
            key: d.bytes()?.into(),
            reason: Reason::from_u8(d.u8()?)?,
            since_ms: d.uv()?,
        },
        t => return format(format!("stale state tag {t}")),
    })
}

fn bool_of(d: &mut Dec<'_>) -> Result<bool> {
    match d.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        b => format(format!("bool {b}")),
    }
}

impl Watch {
    fn encode(&self, e: &mut Enc) {
        self.conditions.encode(e);
        match &self.own {
            None => e.u8(0),
            Some(o) => {
                e.u8(1);
                e.bytes(o);
            }
        }
        e.u8(self.gate as u8);
    }

    fn decode(d: &mut Dec<'_>) -> Result<Watch> {
        Ok(Watch {
            conditions: Arc::new(Conditions::decode(d)?),
            own: match d.u8()? {
                0 => None,
                1 => Some(d.bytes()?.into()),
                t => return format(format!("own tag {t}")),
            },
            gate: bool_of(d)?,
        })
    }
}

fn keyed_input<'a>(t: &'a Tracking, edge: &[u8]) -> Result<&'a Input> {
    match t.stale.inputs.get(edge) {
        Some(i) => Ok(i),
        None => invalid(format!("no keyed edge {}", String::from_utf8_lossy(edge))),
    }
}

/// An edge's conditions now (rule 4): journaled when the edge opens and
/// whenever a deploy changes them. A change makes what is known stale.
#[derive(Clone, Debug, PartialEq)]
pub struct SetWatch {
    pub edge: Id,
    pub watch: Watch,
}

impl SetWatch {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.edge);
        self.watch.encode(e);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<SetWatch> {
        Ok(SetWatch {
            edge: d.bytes()?.into(),
            watch: Watch::decode(d)?,
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        keyed_input(t, &self.edge).map(|_| ())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        let i = t.stale.inputs.get_mut(&self.edge).expect("checked");
        if i.watch.as_ref() != Some(&self.watch) {
            i.watch = Some(self.watch);
            t.stale.gens += 1;
            i.moved(t.stale.gens, None);
        }
    }
}

/// What a refresh or a notice found for one edge.
#[derive(Clone, Debug, PartialEq)]
pub struct Witness {
    pub edge: Id,
    /// The edge's generation it was computed for: dropped if it moved since.
    pub gen: u64,
    /// The upstream commit it holds as of.
    pub x: u64,
    pub state: State,
    /// A failed key's retry came due: what is due changed with no change to
    /// progress, so the part starts a new generation and every part computed
    /// before it is dropped.
    pub retry: bool,
}

impl Witness {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.edge);
        e.uv(self.gen);
        e.uv(self.x);
        encode_state(e, &self.state);
        e.u8(self.retry as u8);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Witness> {
        Ok(Witness {
            edge: d.bytes()?.into(),
            gen: d.uv()?,
            x: d.uv()?,
            state: decode_state(d)?,
            retry: bool_of(d)?,
        })
    }

    pub(crate) fn check(&self, _: &Tracking) -> Result<()> {
        Ok(())
    }

    /// Installs what was found, unless the edge moved since it was computed
    /// or something newer is already known. An edge that stays stale keeps
    /// the moment it went stale.
    pub(crate) fn apply(self, t: &mut Tracking) {
        let Some(i) = t.stale.inputs.get_mut(&self.edge) else {
            return;
        };
        if self.gen != i.gen || i.current().is_some_and(|k| k.x > self.x) {
            return;
        }
        let state = match (self.state, i.known.as_ref().map(|k| &k.state)) {
            (State::Stale { key, reason, .. }, Some(State::Stale { since_ms, .. })) => {
                State::Stale {
                    key,
                    reason,
                    since_ms: *since_ms,
                }
            }
            (s, _) => s,
        };
        if self.retry {
            t.stale.gens += 1;
            i.gen = t.stale.gens;
        }
        i.known = Some(Known {
            gen: i.gen,
            x: self.x,
            state,
        });
        i.changed = Changed::Ranges(vec![]);
    }
}

/// An output's full run starts (`on`) or completes (rule 8): while it runs,
/// every edge reading it that keeps the gate reads "held: upstream
/// incomplete". When it completes, those edges are probed again.
#[derive(Clone, Debug, PartialEq)]
pub struct Incomplete {
    pub output: Id,
    pub on: bool,
}

impl Incomplete {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.output);
        e.u8(self.on as u8);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Incomplete> {
        Ok(Incomplete {
            output: d.bytes()?.into(),
            on: bool_of(d)?,
        })
    }

    pub(crate) fn check(&self, _: &Tracking) -> Result<()> {
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        if self.on {
            t.stale.incomplete.insert(self.output);
            return;
        }
        if !t.stale.incomplete.remove(&self.output) {
            return;
        }
        for (id, e) in &t.edges {
            if e.input == self.output {
                if let Some(i) = t.stale.inputs.get_mut(id) {
                    if i.watch.as_ref().is_some_and(|w| w.gate) {
                        t.stale.gens += 1;
                        i.moved(t.stale.gens, None);
                    }
                }
            }
        }
    }
}
