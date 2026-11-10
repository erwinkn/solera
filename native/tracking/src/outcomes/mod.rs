//! Key outcomes of per-key assets (docs/tracking-design.md §11, D179, D191):
//! one record per key that did not succeed, holding its point and its
//! outcome together.
//!
//! Progress records success only (P9). A failing key's batch still writes
//! its piece over the key, so the piece says what the batch **tried**: the
//! version it failed on, under the conditions it failed under. What the
//! consumer really **holds** is in the key's record. Decoding a key reads its
//! record first, then its point, then its piece. The point of this split is
//! cost: a failing key whose input has not changed since it failed is not
//! owed by its piece, so no plan visits it, however many keys fail. A plan
//! meets a record only for keys its piece already owes (the input changed,
//! or the conditions did), and then reads the record to class the key.
//!
//! Records live in the change index, as the version word of an output the
//! core keeps for the edge (\0f + edge): a failing batch is one more commit of
//! that output, built by B4's `build`, and merged like any other output.
//! Measured against a store of its own in handoffs/SOLE-13/outcomes-store.md.
//! The retry clock is a second output, keyed by due time (\0d + edge), and a
//! retry pass's key list a third (\0p + edge); [`crate::due`] reads them.
//!
//! Event parts (tags 0x30-0x3f): [`Outcomes`] with a batch commit,
//! [`PassStart`] when a retry pass begins, [`Rebuild`] for a full run asked
//! for by hand (the amended P8).

pub mod commit;
pub mod transition;

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::codec::{Dec, Enc, Ver};
use crate::error::{format, invalid, Result};
use crate::event::Part;
use crate::progress::{BatchProgress, Conditions, Holding, Key, Reset};
use crate::state::{Id, Tracking};

pub use transition::{eligible, transition, Counters, Kind, Reported, Stored};

pub const TAG_OUTCOMES: u8 = 0x30;
pub const TAG_PASS_START: u8 = 0x31;
pub const TAG_REBUILD: u8 = 0x32;

/// The outputs the core keeps for a per-key edge: its records, its retry
/// clock and its retry pass. Ids starting with a zero byte are the core's;
/// the engine never names an output so.
pub fn output_id(kind: u8, edge: &[u8]) -> Id {
    let mut v = Vec::with_capacity(edge.len() + 2);
    v.push(0);
    v.push(kind);
    v.extend_from_slice(edge);
    v.into()
}

pub const FAILURES: u8 = b'f';
pub const DUE: u8 = b'd';
pub const PASS: u8 = b'p';

/// The conditions records refer to, by the digest of their encoding.
pub type Conds = BTreeMap<u64, Arc<Conditions>>;

/// One record's part of a plan's `records` digest: the sum of these over the
/// keys a batch touches, so that a commit can check them in any order.
pub fn record_digest(key: &[u8], r: &Record) -> u64 {
    let mut e = Enc::new();
    e.bytes(key);
    e.bytes(&r.encode());
    xxhash_rust::xxh3::xxh3_64(&e.buf)
}

pub fn cond_id(c: &Conditions) -> u64 {
    let mut e = Enc::new();
    c.encode(&mut e);
    xxhash_rust::xxh3::xxh3_64(&e.buf)
}

/// What the consumer holds of a failing key, with conditions by id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Held {
    Absent,
    Held,
    Version(Ver, u64),
}

impl Held {
    pub fn of(h: &Holding) -> Held {
        match h {
            Holding::Absent => Held::Absent,
            Holding::Held => Held::Held,
            Holding::Version(v, c) => Held::Version(v.clone(), cond_id(c)),
        }
    }

    pub fn holding(&self, conds: &Conds) -> Result<Holding> {
        Ok(match self {
            Held::Absent => Holding::Absent,
            Held::Held => Holding::Held,
            Held::Version(v, c) => Holding::Version(v.clone(), cond(conds, *c)?),
        })
    }

    fn cond(&self) -> Option<u64> {
        match self {
            Held::Version(_, c) => Some(*c),
            _ => None,
        }
    }
}

fn cond(conds: &Conds, id: u64) -> Result<Arc<Conditions>> {
    match conds.get(&id) {
        Some(c) => Ok(c.clone()),
        None => format(format!("a record refers to unknown conditions {id:#x}")),
    }
}

/// One failing key's record: its stored outcome, what it was last tried on
/// (the version, the conditions, and the input's head the batch planned
/// at), and what the consumer holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub stored: Stored,
    pub on: Ver,
    pub on_cond: u64,
    pub at: u64,
    pub held: Held,
}

impl Record {
    pub fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new();
        self.stored.encode(&mut e);
        e.version(self.on.as_version());
        e.uv(self.on_cond);
        e.uv(self.at);
        match &self.held {
            Held::Absent => e.u8(0),
            Held::Held => e.u8(1),
            Held::Version(v, c) => {
                e.u8(2);
                e.version(v.as_version());
                e.uv(*c);
            }
        }
        e.buf
    }

    pub fn decode(b: &[u8]) -> Result<Record> {
        let mut d = Dec::new(b);
        let stored = Stored::decode(&mut d)?;
        let on = d.ver()?;
        let on_cond = d.uv()?;
        let at = d.uv()?;
        let held = match d.u8()? {
            0 => Held::Absent,
            1 => Held::Held,
            2 => Held::Version(d.ver()?, d.uv()?),
            t => return format(format!("held tag {t}")),
        };
        d.end()?;
        Ok(Record {
            stored,
            on,
            on_cond,
            at,
            held,
        })
    }

    /// What it was tried on, as a holding: the version under its conditions.
    pub fn tried(&self, key: &[u8], conds: &Conds) -> Result<Holding> {
        Ok(Holding::of(
            self.on.as_version(),
            &cond(conds, self.on_cond)?,
            key,
        ))
    }

    pub(crate) fn conds(&self) -> impl Iterator<Item = u64> {
        [Some(self.on_cond), self.held.cond()].into_iter().flatten()
    }
}

/// A retry pass under way: its number, the time and counters it runs
/// under, and the last key of its list taken so far.
#[derive(Clone, Debug, PartialEq)]
pub struct Pass {
    pub n: u64,
    pub at: u64,
    pub counters: Counters,
    pub after: Option<Key>,
    /// The least counters a record written since the pass began carries,
    /// by class: what the edge's last pass covers when this one ends.
    pub low: Counters,
}

/// One per-key edge's state beside its progress.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PerKey {
    /// The amended P8's rebuild counter: raised by a full run asked for by
    /// hand, part of the input's conditions.
    pub rebuild: u64,
    /// Conditions the records refer to, and how many references each has.
    pub conds: Arc<Conds>,
    pub refs: BTreeMap<u64, u64>,
    /// Records by stored outcome code (index 0 unused).
    pub counts: [u64; 6],
    /// Records by the class a try at what they were tried on would have:
    /// added, updated, removed.
    pub classes: [u64; 3],
    /// The input's commit through which reverts were looked for: a failing
    /// key whose input returns to what the consumer holds is moot (KEY-3).
    pub seen: u64,
    /// The conditions (their id) they were last looked for under: a failing
    /// key whose input the conditions now take as what the consumer holds
    /// is moot too (KEY-3).
    pub cond: u64,
    pub passes: u64,
    pub pass: Option<Pass>,
    /// The edge's life: given from [Tracking::lives] when it opens and each
    /// time its records start over (a reset, a rename). Every part B6
    /// builds names the life it was built in and applies only in it, since
    /// heads and counts start over too; a life is never given twice.
    pub life: u64,
    /// The counters the last pass covers: those it ran under, lowered to
    /// the least a record written since carries (§9's deploy_min), so a
    /// record a deploy or forced request has not retried yet starts a pass.
    pub last: Counters,
}

impl PerKey {
    fn clear(&mut self) {
        *self = PerKey {
            rebuild: self.rebuild,
            passes: self.passes,
            seen: self.seen,
            cond: self.cond,
            life: self.life,
            ..PerKey::default()
        };
    }

    pub(crate) fn encode(&self, e: &mut Enc) {
        e.uv(self.rebuild);
        e.uv(self.conds.len() as u64);
        for (id, c) in self.conds.iter() {
            e.uv(*id);
            c.encode(e);
            e.uv(self.refs.get(id).copied().unwrap_or(0));
        }
        for n in self.counts.iter().chain(&self.classes) {
            e.uv(*n);
        }
        e.uv(self.seen);
        e.uv(self.cond);
        e.uv(self.passes);
        match &self.pass {
            None => e.u8(0),
            Some(p) => {
                e.u8(1);
                e.uv(p.n);
                e.uv(p.at);
                counters(e, &p.counters);
                opt_key(e, &p.after);
                counters(e, &p.low);
            }
        }
        counters(e, &self.last);
        e.uv(self.life);
    }

    pub(crate) fn decode(d: &mut Dec<'_>) -> Result<PerKey> {
        let rebuild = d.uv()?;
        let mut conds = Conds::new();
        let mut refs = BTreeMap::new();
        for _ in 0..d.count()? {
            let id = d.uv()?;
            conds.insert(id, Arc::new(Conditions::decode(d)?));
            refs.insert(id, d.uv()?);
        }
        let mut counts = [0; 6];
        let mut classes = [0; 3];
        for n in counts.iter_mut().chain(classes.iter_mut()) {
            *n = d.uv()?;
        }
        let seen = d.uv()?;
        let cond = d.uv()?;
        let passes = d.uv()?;
        let pass = match d.u8()? {
            0 => None,
            1 => Some(Pass {
                n: d.uv()?,
                at: d.uv()?,
                counters: read_counters(d)?,
                after: read_opt_key(d)?,
                low: read_counters(d)?,
            }),
            t => return format(format!("pass tag {t}")),
        };
        Ok(PerKey {
            rebuild,
            conds: Arc::new(conds),
            refs,
            counts,
            classes,
            seen,
            cond,
            passes,
            pass,
            last: read_counters(d)?,
            life: d.uv()?,
        })
    }
}

pub(crate) fn counters(e: &mut Enc, c: &Counters) {
    e.uv(c.deploy);
    for f in &c.forced[1..] {
        e.uv(*f);
    }
}

pub(crate) fn read_counters(d: &mut Dec<'_>) -> Result<Counters> {
    let mut c = Counters {
        deploy: d.uv()?,
        forced: [0; 6],
    };
    for f in &mut c.forced[1..] {
        *f = d.uv()?;
    }
    Ok(c)
}

pub(crate) fn opt_key(e: &mut Enc, k: &Option<Key>) {
    match k {
        None => e.u8(0),
        Some(k) => {
            e.u8(1);
            e.bytes(k);
        }
    }
}

pub(crate) fn read_opt_key(d: &mut Dec<'_>) -> Result<Option<Key>> {
    match d.u8()? {
        0 => Ok(None),
        1 => Ok(Some(d.bytes()?.into())),
        t => format(format!("key tag {t}")),
    }
}

fn keyed(t: &Tracking, edge: &[u8]) -> Result<()> {
    match t.edges.get(edge).map(|e| e.progress.keyed().is_some()) {
        Some(true) => Ok(()),
        Some(false) => invalid(format!(
            "edge {} is not keyed",
            String::from_utf8_lossy(edge)
        )),
        None => invalid(format!("no edge {}", String::from_utf8_lossy(edge))),
    }
}

/// A pass's step in a retry batch: the list was read from `from` through
/// `to` (None: to its end, which completes the pass).
#[derive(Clone, Debug, PartialEq)]
pub struct Step {
    pub n: u64,
    pub from: Option<Key>,
    pub to: Option<Key>,
}

/// Where reverts were looked for (KEY-3): the input's commit through which,
/// and the conditions (their id) under which.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Look {
    pub seen: u64,
    pub cond: u64,
}

/// What a batch commit changes of an edge's records, beside the commits of
/// its record and clock outputs: the counts after it, the conditions it
/// adds and the references it moves, and a retry batch's step.
#[derive(Clone, Debug, PartialEq)]
pub struct Outcomes {
    pub edge: Id,
    pub counts: [u64; 6],
    pub classes: [u64; 3],
    pub conds: Vec<(u64, Arc<Conditions>)>,
    /// Conditions whose reference count changes, with the count after.
    pub refs: Vec<(u64, u64)>,
    pub step: Option<Step>,
    /// The failure output's head the counts were computed at: the part
    /// applies only there (the output's own commit, if any, moves it on).
    pub at: u64,
    /// Where reverts were looked for when the part was built, and where they
    /// are looked for once it applies: the part applies only from there, so
    /// that a revert is never looked for again under other conditions. Set
    /// by a catch-up, and by a batch in which a key failed.
    pub look: Option<(Look, Look)>,
    /// The least counters a record it writes carries, by class.
    pub low: Option<Counters>,
    /// The edge's outcome life it was built in.
    pub life: u64,
    /// The input's head the batch's failures were made, and checked for
    /// moot, at: the part applies only while the input is still there,
    /// since reverts are looked for after it. None when no key failed.
    pub made: Option<u64>,
    /// Points to set (B3's progress): a failure gone moot outside a range
    /// batch leaves the consumer holding what its record held.
    pub points: Vec<(Key, crate::progress::Point)>,
}

impl Outcomes {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.edge);
        for n in self.counts.iter().chain(&self.classes) {
            e.uv(*n);
        }
        e.uv(self.conds.len() as u64);
        for (id, c) in &self.conds {
            e.uv(*id);
            c.encode(e);
        }
        e.uv(self.refs.len() as u64);
        for (id, n) in &self.refs {
            e.uv(*id);
            e.uv(*n);
        }
        match &self.step {
            None => e.u8(0),
            Some(s) => {
                e.u8(1);
                e.uv(s.n);
                opt_key(e, &s.from);
                opt_key(e, &s.to);
            }
        }
        e.uv(self.at);
        match &self.look {
            None => e.u8(0),
            Some((from, to)) => {
                e.u8(1);
                for l in [from, to] {
                    e.uv(l.seen);
                    e.uv(l.cond);
                }
            }
        }
        match &self.low {
            None => e.u8(0),
            Some(c) => {
                e.u8(1);
                counters(e, c);
            }
        }
        e.uv(self.life);
        e.uv(self.made.map_or(0, |m| m + 1));
        let mut t = crate::progress::CondTable::default();
        let mut body = Enc::new();
        crate::progress::encode_points(&mut body, &mut t, self.points.iter().map(|(k, p)| (k, p)));
        t.encode(e);
        e.buf.extend_from_slice(&body.buf);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Outcomes> {
        let edge = d.bytes()?.into();
        let mut counts = [0; 6];
        let mut classes = [0; 3];
        for n in counts.iter_mut().chain(classes.iter_mut()) {
            *n = d.uv()?;
        }
        let conds = (0..d.count()?)
            .map(|_| Ok((d.uv()?, Arc::new(Conditions::decode(d)?))))
            .collect::<Result<_>>()?;
        let refs = (0..d.count()?)
            .map(|_| Ok((d.uv()?, d.uv()?)))
            .collect::<Result<_>>()?;
        let step = match d.u8()? {
            0 => None,
            1 => Some(Step {
                n: d.uv()?,
                from: read_opt_key(d)?,
                to: read_opt_key(d)?,
            }),
            t => return format(format!("step tag {t}")),
        };
        let at = d.uv()?;
        let look = |d: &mut Dec<'_>| -> Result<Look> {
            Ok(Look {
                seen: d.uv()?,
                cond: d.uv()?,
            })
        };
        let look = match d.u8()? {
            0 => None,
            1 => Some((look(d)?, look(d)?)),
            t => return format(format!("look tag {t}")),
        };
        let low = match d.u8()? {
            0 => None,
            1 => Some(read_counters(d)?),
            t => return format(format!("counters tag {t}")),
        };
        let life = d.uv()?;
        let made = d.uv()?.checked_sub(1);
        let table = crate::progress::CondTable::decode(d)?;
        let points = crate::progress::decode_points(d, &table)?;
        Ok(Outcomes {
            edge,
            counts,
            classes,
            conds,
            refs,
            step,
            at,
            look,
            low,
            life,
            made,
            points,
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        keyed(t, &self.edge)?;
        if self.life != t.per_key.get(&self.edge).map_or(0, |k| k.life) {
            return invalid(
                "the edge's records started over since the batch's outcomes were built",
            );
        }
        let head = t
            .outputs
            .get(&output_id(FAILURES, &self.edge))
            .map_or(0, |o| o.head);
        if head != self.at {
            return invalid("the edge's records moved since the batch's outcomes were built");
        }
        if let Some((from, _)) = &self.look {
            let k = t.per_key.get(&self.edge);
            let now = k.map_or(Look::default(), |k| Look {
                seen: k.seen,
                cond: k.cond,
            });
            if now != *from {
                return invalid("the edge's reverts were looked for since the part was built");
            }
        }
        if let Some(made) = self.made {
            let input = &t.edges.get(&self.edge).expect("keyed").input;
            if t.outputs.get(input).map_or(0, |o| o.head) != made {
                return invalid("the edge's input moved since the batch's records were made");
            }
        }
        if self.points.windows(2).any(|w| w[0].0 >= w[1].0) {
            return invalid("points out of order");
        }
        if let Some(s) = &self.step {
            let pass = t.per_key.get(&self.edge).and_then(|k| k.pass.as_ref());
            match pass {
                Some(p) if p.n == s.n && p.after == s.from => {}
                _ => return invalid("a retry batch of a pass that moved or ended"),
            }
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        let list = output_id(PASS, &self.edge);
        if !self.points.is_empty() {
            let e = t.edges.get_mut(&self.edge).expect("checked");
            if let crate::progress::Progress::Keyed(k) = Arc::make_mut(&mut e.progress) {
                k.points.extend(self.points);
            }
        }
        let k = t.per_key.entry(self.edge).or_default();
        if let Some((_, to)) = self.look {
            k.seen = to.seen;
            k.cond = to.cond;
        }
        if let Some(low) = self.low {
            let last = match &mut k.pass {
                Some(p) => &mut p.low,
                None => &mut k.last,
            };
            *last = last.least(&low);
        }
        k.counts = self.counts;
        k.classes = self.classes;
        let conds = Arc::make_mut(&mut k.conds);
        for (id, c) in self.conds {
            conds.entry(id).or_insert(c);
        }
        for (id, n) in self.refs {
            if n == 0 {
                k.refs.remove(&id);
                conds.remove(&id);
            } else {
                k.refs.insert(id, n);
            }
        }
        if let Some(s) = self.step {
            let p = k.pass.as_mut().expect("checked");
            match s.to {
                Some(to) => p.after = Some(to),
                None => {
                    k.last = p.low;
                    k.pass = None;
                    t.outputs.remove(&list);
                }
            }
        }
    }
}

/// A retry pass begins: its list (the outcome of B4's commit of \0p + edge
/// in the same event, if it holds any key) is taken in key order.
#[derive(Clone, Debug, PartialEq)]
pub struct PassStart {
    pub edge: Id,
    pub n: u64,
    pub at: u64,
    pub counters: Counters,
    /// The edge's outcome life it was built in.
    pub life: u64,
}

impl PassStart {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.edge);
        e.uv(self.n);
        e.uv(self.at);
        counters(e, &self.counters);
        e.uv(self.life);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<PassStart> {
        Ok(PassStart {
            edge: d.bytes()?.into(),
            n: d.uv()?,
            at: d.uv()?,
            counters: read_counters(d)?,
            life: d.uv()?,
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        keyed(t, &self.edge)?;
        let k = t.per_key.get(&self.edge);
        if k.is_some_and(|k| k.pass.is_some()) {
            return invalid("a retry pass is already under way");
        }
        if self.n != k.map_or(0, |k| k.passes) + 1 {
            return invalid("a retry pass out of turn");
        }
        if self.life != k.map_or(0, |k| k.life) {
            return invalid("the edge's records started over since the pass was found due");
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        let k = t.per_key.entry(self.edge).or_default();
        k.passes = self.n;
        k.pass = Some(Pass {
            n: self.n,
            at: self.at,
            counters: self.counters,
            after: None,
            low: self.counters,
        });
    }
}

/// A full run of a per-key asset asked for by hand (D194): the input's
/// rebuild counter rises, which makes every key it holds owed.
#[derive(Clone, Debug, PartialEq)]
pub struct Rebuild {
    pub edge: Id,
    pub to: u64,
    /// The edge's outcome life it was asked for in: the counter starts over
    /// with the records.
    pub life: u64,
}

impl Rebuild {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.edge);
        e.uv(self.to);
        e.uv(self.life);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Rebuild> {
        Ok(Rebuild {
            edge: d.bytes()?.into(),
            to: d.uv()?,
            life: d.uv()?,
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        keyed(t, &self.edge)?;
        if self.life != t.per_key.get(&self.edge).map_or(0, |k| k.life) {
            return invalid("the edge's records started over since the rebuild was asked for");
        }
        if self.to != t.rebuild(&self.edge) + 1 {
            return invalid("a rebuild counter moves up by one");
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        t.per_key.entry(self.edge).or_default().rebuild = self.to;
    }
}

impl Tracking {
    /// The edge's rebuild counter (0 until a full run is asked for by hand).
    pub fn rebuild(&self, edge: &[u8]) -> u64 {
        self.per_key.get(edge).map_or(0, |k| k.rebuild)
    }

    fn next_life(&mut self) -> u64 {
        self.lives += 1;
        self.lives
    }

    fn drop_records(&mut self, edge: &[u8]) {
        for kind in [FAILURES, DUE, PASS] {
            self.outputs.remove(&output_id(kind, edge));
        }
    }
}

/// What B3's parts do to an edge's records, applied after them: a reset
/// starts the records over (they failed against a progress that is gone),
/// a close drops them, a rename moves them.
pub(crate) fn follow(p: &Part, t: &mut Tracking) {
    let keyed = |t: &Tracking, edge: &[u8]| {
        t.edges
            .get(edge)
            .is_some_and(|e| e.progress.keyed().is_some())
    };
    match p {
        Part::EdgeOpen(o) if o.keyed => {
            let life = t.next_life();
            t.per_key.insert(
                o.edge.clone(),
                PerKey {
                    life,
                    ..PerKey::default()
                },
            );
        }
        Part::Reset(Reset { edge, .. })
        | Part::Batch(BatchProgress {
            edge, reset: true, ..
        }) if keyed(t, edge) => {
            t.drop_records(edge);
            let life = t.next_life();
            let k = t.per_key.entry(edge.clone()).or_default();
            k.clear();
            k.life = life;
        }
        Part::EdgeClose(c) => {
            t.drop_records(&c.edge);
            t.per_key.remove(&c.edge);
        }
        Part::EdgeRename(r) if r.from != r.to && keyed(t, &r.from) => {
            for kind in [FAILURES, DUE, PASS] {
                if let Some(o) = t.outputs.remove(&output_id(kind, &r.from)) {
                    t.outputs.insert(output_id(kind, &r.to), o);
                }
            }
            let mut k = t.per_key.remove(&r.from).unwrap_or_default();
            k.life = t.next_life();
            t.per_key.insert(r.to.clone(), k);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip() {
        let r = Record {
            stored: Stored {
                outcome: transition::FAILED,
                tries: 3,
                deploy: 7,
                forced: 0,
                since: 10,
                last: 20,
                next_at: 0,
                until: 0,
                message: b"boom".as_slice().into(),
            },
            on: Ver::Int(5),
            on_cond: 9,
            at: 12,
            held: Held::Version(Ver::Bytes(b"etag".as_slice().into()), 8),
        };
        assert_eq!(Record::decode(&r.encode()).unwrap(), r);
        let absent = Record {
            held: Held::Absent,
            ..r
        };
        assert_eq!(Record::decode(&absent.encode()).unwrap(), absent);
        assert!(Record::decode(&[9]).is_err());
    }

    #[test]
    fn b3_parts_that_move_outcome_state_are_refused_beside_b6_parts() {
        use crate::event::{Event, Part};
        use crate::progress::{EdgeClose, EdgeOpen, EdgeRename, Reset};
        let e = || -> Id { b"e".as_slice().into() };
        let mut t = Tracking::new();
        let apply = |t: &mut Tracking, parts| t.apply(&Event::of(parts).encode());
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: e(),
                input: b"items".as_slice().into(),
                keyed: true,
            })],
        )
        .unwrap();
        let life = t.per_key[b"e".as_slice()].life;
        let start = PassStart {
            edge: e(),
            n: 1,
            at: 0,
            counters: Counters::default(),
            life,
        };
        apply(&mut t, vec![Part::PassStart(start)]).unwrap();
        let done = Part::Outcomes(Outcomes {
            edge: e(),
            counts: [0; 6],
            classes: [0; 3],
            conds: vec![],
            refs: vec![],
            step: Some(Step {
                n: 1,
                from: None,
                to: None,
            }),
            at: 0,
            look: None,
            low: None,
            life,
            made: None,
            points: vec![],
        });
        let rebuild = |edge: Id| Part::Rebuild(Rebuild { edge, to: 1, life });
        let before = t.snapshot();
        for parts in [
            vec![Part::EdgeClose(EdgeClose { edge: e() }), done.clone()],
            vec![
                Part::Reset(Reset {
                    edge: e(),
                    held: false,
                }),
                rebuild(e()),
            ],
            vec![
                Part::EdgeRename(EdgeRename {
                    from: e(),
                    to: b"f".as_slice().into(),
                    input: b"items".as_slice().into(),
                }),
                rebuild(b"f".as_slice().into()),
            ],
        ] {
            let err = apply(&mut t, parts).unwrap_err().to_string();
            assert!(err.contains("two parts of one event change"), "{err}");
            assert_eq!(t.snapshot(), before);
        }
        let close = Part::EdgeClose(EdgeClose { edge: e() });
        for kind in [FAILURES, DUE, PASS] {
            let id = output_id(kind, b"e");
            assert!(close.touches().contains(&("output", id.to_vec().into())));
        }
        assert!(done
            .touches()
            .contains(&("output", output_id(PASS, b"e").to_vec().into())));
        apply(&mut t, vec![done]).unwrap();
        assert!(t.per_key.get(b"e".as_slice()).unwrap().pass.is_none());
    }
}
