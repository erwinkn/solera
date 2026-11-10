//! The object that holds the core's state: every output's layer list, every
//! edge's progress. It changes only through [`Tracking::apply`]; a snapshot
//! round-trips it exactly.
//!
//! Planning reads the state from other threads while the engine applies
//! events: an edge's progress sits behind an `Arc`, so a plan clones the
//! `Arc` under a short lock and an `apply` copies the progress only if a plan
//! still holds the old one (`Arc::make_mut`).

use std::collections::BTreeMap;
use std::sync::Arc;

use solera_index::LayerRef;

use crate::codec::{Dec, Enc};
use crate::error::{format, Result};
use crate::event::{Event, Part};
use crate::progress::Progress;

/// An opaque name the engine chooses: an output (asset, partition, output
/// name) or an edge (consumer partition, input name), encoded by Python.
pub type Id = Box<[u8]>;

pub const SNAPSHOT_FORMAT: u8 = 2;

/// One output's log: its layer list, oldest first, and its head. B4's
/// commit part appends to it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Output {
    pub layers: Vec<LayerRef>,
    pub head: u64,
    pub live: u64,
}

/// One input of one consumer partition.
#[derive(Clone, Debug, PartialEq)]
pub struct Edge {
    /// The output it reads.
    pub input: Id,
    pub progress: Arc<Progress>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Tracking {
    pub outputs: BTreeMap<Id, Output>,
    pub edges: BTreeMap<Id, Edge>,
    /// B6: per-key edges' counters, outcome counts and retry passes.
    pub per_key: BTreeMap<Id, crate::outcomes::PerKey>,
    /// B6: the last life given to a per-key edge.
    pub lives: u64,
}

impl Tracking {
    pub fn new() -> Tracking {
        Tracking::default()
    }

    /// Applies one journaled event, whole or not at all.
    pub fn apply(&mut self, bytes: &[u8]) -> Result<()> {
        let ev = Event::decode(bytes)?;
        self.apply_event(ev)
    }

    pub fn apply_event(&mut self, ev: Event) -> Result<()> {
        self.check_event(&ev)?;
        for p in ev.parts {
            self.apply_part(p);
        }
        Ok(())
    }

    /// Whether `apply` would take the event, applying nothing: the engine
    /// checks a commit's event before it journals it.
    pub fn check_event(&self, ev: &Event) -> Result<()> {
        let mut touched = std::collections::HashSet::new();
        for p in &ev.parts {
            for t in p.touches() {
                if touched.contains(&t) {
                    return crate::error::invalid(format!(
                        "two parts of one event change {} {}",
                        t.0,
                        String::from_utf8_lossy(&t.1)
                    ));
                }
                touched.insert(t);
            }
        }
        for p in &ev.parts {
            self.check(p)?;
        }
        Ok(())
    }

    fn check(&self, p: &Part) -> Result<()> {
        match p {
            Part::EdgeOpen(p) => p.check(self),
            Part::EdgeClose(p) => p.check(self),
            Part::EdgeRename(p) => p.check(self),
            Part::Reset(p) => p.check(self),
            Part::Batch(p) => p.check(self),
            Part::Consumed(p) => p.check(self),
            Part::OutputCommit(p) => p.check(self),
            Part::Outcomes(p) => p.check(self),
            Part::PassStart(p) => p.check(self),
            Part::Rebuild(p) => p.check(self),
        }
    }

    fn apply_part(&mut self, p: Part) {
        crate::outcomes::follow(&p, self);
        match p {
            Part::EdgeOpen(p) => p.apply(self),
            Part::EdgeClose(p) => p.apply(self),
            Part::EdgeRename(p) => p.apply(self),
            Part::Reset(p) => p.apply(self),
            Part::Batch(p) => p.apply(self),
            Part::Consumed(p) => p.apply(self),
            Part::OutputCommit(p) => p.apply(self),
            Part::Outcomes(p) => p.apply(self),
            Part::PassStart(p) => p.apply(self),
            Part::Rebuild(p) => p.apply(self),
        }
    }

    pub fn snapshot(&self) -> Vec<u8> {
        let mut e = Enc::new();
        e.u8(SNAPSHOT_FORMAT);
        e.uv(self.outputs.len() as u64);
        for (id, o) in &self.outputs {
            e.bytes(id);
            e.uv(o.head);
            e.uv(o.live);
            e.uv(o.layers.len() as u64);
            for l in &o.layers {
                e.layer(l);
            }
        }
        e.uv(self.edges.len() as u64);
        for (id, edge) in &self.edges {
            e.bytes(id);
            e.bytes(&edge.input);
            edge.progress.encode(&mut e);
        }
        e.uv(self.per_key.len() as u64);
        for (id, k) in &self.per_key {
            e.bytes(id);
            k.encode(&mut e);
        }
        e.uv(self.lives);
        e.buf
    }

    pub fn restore(bytes: &[u8]) -> Result<Tracking> {
        let mut d = Dec::new(bytes);
        let f = d.u8()?;
        if f != SNAPSHOT_FORMAT {
            return format(format!("snapshot format {f}, expected {SNAPSHOT_FORMAT}"));
        }
        let mut t = Tracking::new();
        for _ in 0..d.count()? {
            let id: Id = d.bytes()?.into();
            let head = d.uv()?;
            let live = d.uv()?;
            let layers = (0..d.count()?).map(|_| d.layer()).collect::<Result<_>>()?;
            t.outputs.insert(id, Output { layers, head, live });
        }
        for _ in 0..d.count()? {
            let id: Id = d.bytes()?.into();
            let input: Id = d.bytes()?.into();
            let progress = Arc::new(Progress::decode(&mut d)?);
            t.edges.insert(id, Edge { input, progress });
        }
        for _ in 0..d.count()? {
            let id: Id = d.bytes()?.into();
            t.per_key
                .insert(id, crate::outcomes::PerKey::decode(&mut d)?);
        }
        t.lives = d.uv()?;
        d.end()?;
        Ok(t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::{EdgeClose, EdgeOpen, Reset};

    #[test]
    fn events_and_snapshots_round_trip() {
        let mut t = Tracking::new();
        let open = |e: &str, keyed| {
            Event::of(vec![Part::EdgeOpen(EdgeOpen {
                edge: e.as_bytes().into(),
                input: b"items".as_slice().into(),
                keyed,
            })])
        };
        for (e, keyed) in [("tally/items", true), ("count/items", false)] {
            let ev = open(e, keyed);
            assert_eq!(Event::decode(&ev.encode()).unwrap(), ev);
            t.apply(&ev.encode()).unwrap();
        }
        assert!(t.apply(&open("tally/items", true).encode()).is_err());
        t.outputs.insert(
            b"items".as_slice().into(),
            Output {
                head: 3,
                live: 7,
                ..Default::default()
            },
        );
        let back = Tracking::restore(&t.snapshot()).unwrap();
        assert_eq!(back, t);
        let close = Event::of(vec![Part::EdgeClose(EdgeClose {
            edge: b"count/items".as_slice().into(),
        })]);
        t.apply(&close.encode()).unwrap();
        assert_eq!(t.edges.len(), 1);
        assert!(Event::decode(&[FORMAT_BAD]).is_err());
        assert!(Event::decode(&[crate::event::FORMAT]).is_err());
        assert!(Event::decode(&[crate::event::FORMAT, 0x7f, 0]).is_err());
    }

    #[test]
    fn an_event_that_changes_one_edge_twice_is_refused_whole() {
        let mut t = Tracking::new();
        let e = || b"e".as_slice().into();
        t.apply(
            &Event::of(vec![Part::EdgeOpen(EdgeOpen {
                edge: e(),
                input: b"items".as_slice().into(),
                keyed: true,
            })])
            .encode(),
        )
        .unwrap();
        let before = t.snapshot();
        let both = Event::of(vec![
            Part::EdgeClose(EdgeClose { edge: e() }),
            Part::Reset(Reset {
                edge: e(),
                held: false,
            }),
        ]);
        assert!(t.apply(&both.encode()).is_err());
        assert_eq!(t.snapshot(), before);
        let twice = Event::of(vec![
            Part::Reset(Reset {
                edge: e(),
                held: true,
            }),
            Part::Reset(Reset {
                edge: e(),
                held: false,
            }),
        ]);
        assert!(t.apply(&twice.encode()).is_err());
        assert_eq!(t.snapshot(), before);
    }

    #[test]
    fn check_answers_as_apply_would_and_applies_nothing() {
        let mut t = Tracking::new();
        let open = Event::of(vec![Part::EdgeOpen(EdgeOpen {
            edge: b"e".as_slice().into(),
            input: b"items".as_slice().into(),
            keyed: true,
        })]);
        let before = t.snapshot();
        t.check_event(&open).unwrap();
        assert_eq!(t.snapshot(), before);
        t.apply(&open.encode()).unwrap();
        let before = t.snapshot();
        assert!(t.check_event(&open).is_err());
        assert!(t.apply(&open.encode()).is_err());
        assert_eq!(t.snapshot(), before);
    }

    #[test]
    fn a_varint_past_64_bits_is_refused() {
        let mut b = vec![0xff; 9];
        b.push(0x01);
        assert_eq!(crate::codec::Dec::new(&b).uv().unwrap(), u64::MAX);
        *b.last_mut().unwrap() = 0x7f;
        assert!(crate::codec::Dec::new(&b).uv().is_err());
    }

    const FORMAT_BAD: u8 = 99;
}
