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
use crate::retain::Retain;
use crate::stale::Stale;

/// An opaque name the engine chooses: an output (asset, partition, output
/// name) or an edge (consumer partition, input name), encoded by Python.
pub type Id = Box<[u8]>;

pub const SNAPSHOT_FORMAT: u8 = 5;

/// Whether the core owns `id`: a per-key edge's record outputs
/// (`outcomes::output_id`, a zero byte first). Their lives follow their
/// edge's (dropped at a reset or close, moved at a rename), so nothing else
/// names them: no edge reads one, none is declared or cleaned.
pub fn core_owned(id: &[u8]) -> bool {
    id.first() == Some(&0)
}

/// One output's log: its layer list, oldest first, and its head. B4's
/// commit part appends to it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Output {
    /// The output's one life, never 0: given from [Tracking::lives] when the
    /// output is first named ([Tracking::name]: an edge opens on it, it is
    /// declared or tracked, or its first commit lands), and ended only by
    /// [Tracking::end]. A retired output's commit numbers start again at 1
    /// (B9), so an id and a commit name a version only within a life: a plan
    /// names the lives it reads, its commit applies only in them, and its
    /// record stores them; a cleanup step names the life it was taken in.
    /// An entry may hold no commit yet (head 0, no layers).
    pub life: u64,
    pub layers: Vec<LayerRef>,
    pub head: u64,
    pub live: u64,
    /// The clear that started a full run still under way.
    pub full_run: Option<u64>,
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
    /// The last life given to a per-key edge (B6) or an output.
    pub lives: u64,
    /// B7: staleness per keyed edge.
    pub stale: Stale,
    /// B9: the cleanup cursors.
    pub retain: Retain,
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
        let alone = |p: &Part| matches!(p, Part::Merged(_) | Part::Cleaned(_) | Part::Retire(_));
        if ev.parts.len() > 1 && ev.parts.iter().any(alone) {
            return crate::error::invalid(
                "a merge, a cleanup step or a retirement is an event of its own: each part is checked on the state before the event",
            );
        }
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
            Part::FullRunDone(p) => p.check(self),
            Part::Declare(p) => p.check(self),
            Part::Outcomes(p) => p.check(self),
            Part::PassStart(p) => p.check(self),
            Part::Rebuild(p) => p.check(self),
            Part::Watch(p) => p.check(self),
            Part::Witness(p) => p.check(self),
            Part::Merged(p) => p.check(self),
            Part::Track(p) => p.check(self),
            Part::Cleaned(p) => p.check(self),
            Part::Retire(p) => p.check(self),
        }
    }

    fn apply_part(&mut self, p: Part) {
        crate::outcomes::follow(&p, self);
        self.stale_touch(&p);
        match p {
            Part::EdgeOpen(p) => p.apply(self),
            Part::EdgeClose(p) => p.apply(self),
            Part::EdgeRename(p) => p.apply(self),
            Part::Reset(p) => p.apply(self),
            Part::Batch(p) => p.apply(self),
            Part::Consumed(p) => p.apply(self),
            Part::OutputCommit(p) => p.apply(self),
            Part::FullRunDone(p) => p.apply(self),
            Part::Declare(p) => p.apply(self),
            Part::Outcomes(p) => p.apply(self),
            Part::PassStart(p) => p.apply(self),
            Part::Rebuild(p) => p.apply(self),
            Part::Watch(p) => p.apply(self),
            Part::Witness(p) => p.apply(self),
            Part::Merged(p) => p.apply(self),
            Part::Track(p) => p.apply(self),
            Part::Cleaned(p) => p.apply(self),
            Part::Retire(p) => p.apply(self),
        }
    }

    /// Names an output: gives it a life if it has none, and returns its life.
    pub(crate) fn name(&mut self, id: &[u8]) -> u64 {
        if let Some(o) = self.outputs.get(id) {
            return o.life;
        }
        let life = self.next_life();
        self.outputs.insert(
            id.into(),
            Output {
                life,
                ..Output::default()
            },
        );
        life
    }

    /// Ends an output's life: its entry and its cleanup cursor leave the
    /// state, and the next naming gives it a new life. Its callers make sure
    /// no edge reads it (`Retire` refuses that, and no edge reads a per-key
    /// edge's record outputs: see [`core_owned`]).
    pub(crate) fn end(&mut self, id: &[u8]) -> Option<Output> {
        self.retain.cursors.remove(id);
        self.outputs.remove(id)
    }

    /// The output's life now, 0 when it has none.
    pub fn life(&self, id: &[u8]) -> u64 {
        self.outputs.get(id).map_or(0, |o| o.life)
    }

    /// Refused unless `id` has life `life` now: what a plan, a commit or a
    /// cleanup step read is of the output's life that the state holds.
    pub(crate) fn in_life(&self, id: &[u8], life: u64, what: &str) -> Result<()> {
        let now = self.life(id);
        if now != life {
            return crate::error::invalid(format!(
                "{what} {} in life {life}, its life is {now}: it was retired, dropped or started again since",
                String::from_utf8_lossy(id)
            ));
        }
        Ok(())
    }

    /// Sets an output's log by hand, for tests and benches that write its
    /// layers themselves: it keeps the output's life, or names it.
    pub fn set_output(
        &mut self,
        id: Id,
        layers: Vec<LayerRef>,
        head: u64,
        live: u64,
        full_run: Option<u64>,
    ) {
        let life = self.name(&id);
        self.outputs.insert(
            id,
            Output {
                life,
                layers,
                head,
                live,
                full_run,
            },
        );
    }

    /// What every state holds: each output's life is above 0, at most the
    /// counter and its own; every edge's input and every cleaned output has
    /// a life, and none is the core's. Restore refuses a snapshot that
    /// breaks it.
    pub fn check_lives(&self) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        for (id, o) in &self.outputs {
            if o.life == 0 || o.life > self.lives || !seen.insert(o.life) {
                return format(format!(
                    "output {} has life {}, of {} given",
                    String::from_utf8_lossy(id),
                    o.life,
                    self.lives
                ));
            }
        }
        let named = |id: &[u8]| self.outputs.contains_key(id) && !core_owned(id);
        if let Some((e, _)) = self.edges.iter().find(|(_, e)| !named(&e.input)) {
            return format(format!(
                "edge {} reads an output with no life, or the core's",
                String::from_utf8_lossy(e)
            ));
        }
        if let Some(id) = self.retain.cursors.keys().find(|id| !named(id)) {
            return format(format!(
                "cleaned output {} has no life, or is the core's",
                String::from_utf8_lossy(id)
            ));
        }
        Ok(())
    }

    pub fn snapshot(&self) -> Vec<u8> {
        let mut e = Enc::new();
        e.u8(SNAPSHOT_FORMAT);
        e.uv(self.outputs.len() as u64);
        for (id, o) in &self.outputs {
            e.bytes(id);
            e.uv(o.life);
            e.uv(o.head);
            e.uv(o.live);
            e.opt_u64(o.full_run);
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
        self.stale.encode(&mut e);
        self.retain.encode(&mut e);
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
            let life = d.uv()?;
            let head = d.uv()?;
            let live = d.uv()?;
            let full_run = d.opt_u64()?;
            let layers = (0..d.count()?).map(|_| d.layer()).collect::<Result<_>>()?;
            t.outputs.insert(
                id,
                Output {
                    life,
                    layers,
                    head,
                    live,
                    full_run,
                },
            );
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
        t.stale = Stale::decode(&mut d)?;
        t.retain = Retain::decode(&mut d)?;
        d.end()?;
        t.check_lives()?;
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
        t.set_output(b"items".as_slice().into(), vec![], 3, 7, None);
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
