//! Per-key edges with failing keys, over random histories checked after
//! every step against a brute-force model of what the consumer holds and of
//! each failing key's outcome (docs/tracking-design.md §11, KEY-3, KEY-5,
//! KEY-8, KEY-10, KEY-12, D191, D194):
//!
//! - progress and the records together decode to what the consumer holds;
//! - a change plan is exactly the due keys in key order: owed, and not a
//!   failing key tried at what upstream has now (its retry is a pass's);
//! - owed counts include the keys waiting for their retry;
//! - a retry pass takes exactly the keys eligible when it starts that are
//!   still owed and unchanged since they failed, in key order;
//! - a failure whose input returns to what the consumer holds is moot;
//! - counts by outcome match the records; snapshots round-trip;
//! - staleness (B7, D210) agrees with the planner and the retry plan: an
//!   edge reads fresh only if no key is due and the pass under way takes
//!   none, and stale only on a key that is due, with the reason what the
//!   consumer holds gives, or on one the pass takes ("retry due"). Upstream
//!   commits carry the staleness notice (on two commits in three, so a
//!   lost one is covered); the status is checked after each, and a refresh
//!   runs at random before and after each step's catch-up.
//!
//! The multi-input histories give one consumer two keyed inputs, with
//! retries on either input taken on both (`due::plan_all`), during full
//! runs and clears of the other.
//!
//! `OUTCOME_SEEDS` and `MULTI_SEEDS` set how many histories run.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use solera_index::{ChangeIndex, LayerRef, NewCommit, Run, Store, StoreOptions, WriteOptions};
use solera_tracking::codec::Ver;
use solera_tracking::commit::OutputCommit;
use solera_tracking::due;
use solera_tracking::event::{Event, Part};
use solera_tracking::outcomes::commit::{self as oc, records, Ctx};
use solera_tracking::outcomes::transition::{
    eligible, transition, Counters, Kind, Reported, Stored, FAILED,
};
use solera_tracking::outcomes::{output_id, record_digest, Rebuild, FAILURES};
use solera_tracking::plan::{self, InputRequest, Mode, NoHold, OneIndex, Request};
use solera_tracking::progress::{
    owed, Class, Conditions, EdgeClose, EdgeOpen, EdgeRename, Holding, InputReport, Key, Ops, Pat,
    Patterns, Reset, Value,
};
use solera_tracking::stale::{self, NoneDue, Reason as Why, SetWatch, Status, Watch};
use solera_tracking::Tracking;

fn key(s: &str) -> Key {
    s.as_bytes().into()
}

const EDGE: &str = "checks/items";

struct Log {
    /// The input's output.
    name: String,
    states: Vec<BTreeMap<Key, Ver>>,
    layers: Vec<LayerRef>,
    /// The commits that cleared, ascending.
    clears: Vec<u64>,
    /// The clear that started a full run still under way.
    full_run: Option<u64>,
}

impl Log {
    fn head(&self) -> u64 {
        self.states.len() as u64 - 1
    }

    fn at(&self, c: u64, k: &[u8]) -> Ver {
        self.states[c as usize].get(k).cloned().unwrap_or_default()
    }

    async fn commit(
        &mut self,
        ix: &ChangeIndex,
        t: &mut Tracking,
        changes: BTreeMap<Key, Ver>,
        clear: bool,
        ends: bool,
    ) -> bool {
        let before = self.states.last().unwrap().clone();
        let mut after = if clear {
            BTreeMap::new()
        } else {
            before.clone()
        };
        let mut run = Run::new();
        let c = self.head() + 1;
        let mut notice =
            (c % 3 != 0).then(|| stale::Notice::new(t, self.name.as_bytes(), c, clear, 0));
        for (k, new) in &changes {
            let old = if clear {
                Ver::Absent
            } else {
                before.get(k).cloned().unwrap_or_default()
            };
            if *new == old {
                continue;
            }
            run.push_key(k);
            run.push_entry(c, new.as_version(), old.as_version());
            if let Some(n) = notice.as_mut() {
                n.push(k, new.as_version());
            }
            match new {
                Ver::Absent => after.remove(k),
                v => after.insert(k.clone(), v.clone()),
            };
        }
        if run.is_empty() {
            return false;
        }
        let nc = NewCommit {
            commit: c,
            time_ms: 0,
            producer: b"",
            live_before: before.len() as u64,
            clear,
        };
        let path = format!("{}/c{c}", self.name);
        self.layers
            .push(ix.write_commit(&path, nc, &run).await.unwrap());
        self.states.push(after);
        if clear {
            self.clears.push(c);
            self.full_run = Some(c);
        }
        let was = t
            .outputs
            .get(self.name.as_bytes())
            .is_some_and(|o| o.full_run.is_some());
        if ends {
            self.full_run = None;
        }
        let mut parts = vec![Part::OutputCommit(OutputCommit {
            output: key(&self.name),
            layer: self.layers.last().unwrap().clone(),
            live: self.states.last().unwrap().len() as u64,
            ends_full_run: ends && (clear || was),
        })];
        parts.extend(notice.map_or(vec![], |n| n.finish()));
        t.apply(&Event::of(parts).encode()).unwrap();
        let o = &t.outputs[self.name.as_bytes()];
        assert_eq!(
            (o.head, o.full_run, o.layers.len()),
            (c, self.full_run, self.layers.len())
        );
        true
    }
}

/// The truth about one failing key.
#[derive(Clone, Debug)]
struct Fail {
    stored: Stored,
    tried: Holding,
    tried_cond: Arc<Conditions>,
    tried_ver: Ver,
    at: u64,
}

enum Deferred {
    Start(Vec<Part>),
    CatchUp(Vec<Part>),
    Commit(Built),
}

/// A batch's commit, built and not yet applied.
struct Built {
    plan: plan::Plan,
    /// Whether the input was unsettled when B3 built the batch's progress,
    /// which decides its hold-backs.
    unsettled: bool,
    parts: Vec<Part>,
    ops: Ops,
    failed: Vec<(Key, Reported)>,
    loaded: Vec<(Key, Ver)>,
    counters: Counters,
    clock: f64,
}

fn trace(m: &str) {
    if std::env::var("OUTCOME_TRACE").is_ok() {
        eprintln!("  {m}");
    }
}

struct World {
    rng: SmallRng,
    /// Draws for the staleness checks, apart so that they leave the
    /// history each seed gives unchanged.
    stale_rng: SmallRng,
    /// Whether the edge's watch keeps the gate (rule 8).
    gate: bool,
    t: Tracking,
    log: Log,
    universe: Vec<Key>,
    now: Arc<Conditions>,
    holds: BTreeMap<Key, Holding>,
    fails: BTreeMap<Key, Fail>,
    cursor: Option<Key>,
    clock: f64,
    counters: Counters,
    files: u64,
    events: u64,
    stats: [usize; 11],
    /// Plans held back to commit late, after the state moved on.
    stale: Vec<plan::Plan>,
    /// Every key a batch runs fails (a bad deploy).
    fail_all: bool,
    /// The input may commit while a change batch's outcomes are built.
    races: bool,
    /// Parts built and not yet applied, with the era they were built in.
    deferred: Vec<(u64, Deferred)>,
    /// Bumped each time the edge starts a new life: a reset, a close and
    /// reopen, a rename.
    era: u64,
}

fn patterns(rng: &mut SmallRng) -> Patterns {
    let glob = |ks: &[&str]| {
        ks.iter()
            .map(|k| Pat::Glob(k.to_string()))
            .collect::<Vec<_>>()
    };
    match rng.random_range(0..5) {
        0 | 1 | 2 => Patterns::default(),
        3 => Patterns::new(glob(&["k1", "k2", "k3", "k4"]), vec![]).unwrap(),
        _ => Patterns::new(vec![Pat::Glob("k*".into())], vec![Pat::Glob("k3".into())]).unwrap(),
    }
}

fn version(rng: &mut SmallRng) -> Ver {
    match rng.random_range(0..5) {
        0 => Ver::Absent,
        1 => Ver::Bytes(b"s1".as_slice().into()),
        n => Ver::Int(n as u64),
    }
}

fn same(a: &Holding, b: &Holding) -> bool {
    match (a, b) {
        (Holding::Absent, Holding::Absent) | (Holding::Held, Holding::Held) => true,
        (Holding::Version(..), Holding::Version(..)) => !owed(a, b),
        _ => false,
    }
}

fn index(rt: &tokio::runtime::Runtime) -> ChangeIndex {
    let opts = StoreOptions {
        memory_bytes: 1 << 20,
        ..Default::default()
    };
    let store = Store::new(
        Arc::new(InMemory::new()),
        Path::from("t"),
        rt.handle().clone(),
        opts,
    )
    .unwrap();
    ChangeIndex::new(
        store,
        WriteOptions {
            block_bytes: 64,
            ..Default::default()
        },
    )
}

impl World {
    fn edge(&self) -> Key {
        key(EDGE)
    }

    fn apply(&mut self, parts: Vec<Part>) {
        self.events += 1;
        self.t.apply(&Event::of(parts).encode()).unwrap();
    }

    fn holding(&self, k: &[u8]) -> Holding {
        self.holds.get(k).cloned().unwrap_or(Holding::Absent)
    }

    fn wanted(&self, k: &[u8]) -> Holding {
        Holding::of(self.log.at(self.log.head(), k).as_version(), &self.now, k)
    }

    fn waiting(&self, k: &[u8]) -> bool {
        self.fails
            .get(k)
            .is_some_and(|f| !owed(&f.tried, &self.wanted(k)))
    }

    fn owed(&self) -> Vec<Key> {
        self.universe
            .iter()
            .filter(|k| owed(&self.holding(k), &self.wanted(k)))
            .cloned()
            .collect()
    }

    fn due(&self) -> Vec<Key> {
        self.owed()
            .into_iter()
            .filter(|k| !self.waiting(k))
            .collect()
    }

    fn ctx(&mut self) -> (String, String) {
        self.files += 1;
        (format!("f/{}", self.files), format!("d/{}", self.files))
    }

    fn request(&self, after: Option<Key>, limit: usize, mode: Mode) -> Request {
        Request {
            mode,
            reset: false,
            after,
            limit,
            inputs: vec![InputRequest {
                edge: self.edge(),
                conditions: self.now.clone(),
                own: None,
            }],
            unkeyed: vec![],
        }
    }

    /// The engine looks for reverts before it plans or changes conditions.
    async fn catch_up(&mut self, ix: &ChangeIndex) {
        let v = self.t.outcome_view(EDGE.as_bytes()).unwrap();
        let (f, d) = self.ctx();
        let ctx = Ctx {
            now: self.clock,
            counters: self.counters,
            retries: 2,
            time_ms: 0,
            producer: b"",
            failures_path: &f,
            due_path: &d,
        };
        if let Some(parts) = oc::catch_up(&OneIndex(ix), &v, &self.now, &ctx)
            .await
            .unwrap()
        {
            self.apply(parts);
        }
    }

    /// Journals the watch the conditions now call for, refreshes at random,
    /// and checks the status. A refresh on an edge with failure records
    /// waits for the catch-up.
    async fn stale_check(&mut self, ix: &ChangeIndex, label: &str) {
        let edge = self.edge();
        let watch = self.t.stale.inputs[&edge].watch.as_ref();
        if watch.is_none_or(|w| w.conditions != self.now || w.gate != self.gate) {
            self.apply(vec![Part::Watch(SetWatch {
                edge: edge.clone(),
                watch: Watch {
                    conditions: self.now.clone(),
                    own: None,
                    gate: self.gate,
                },
            })]);
        }
        if self.stale_rng.random_bool(0.6) {
            match self.t.stale_ask(&edge) {
                Ok(ask) => {
                    let w = stale::refresh(&ask, &OneIndex(ix), &NoneDue, 1)
                        .await
                        .unwrap();
                    if let Some(w) = w {
                        self.apply(vec![Part::Witness(w)]);
                    }
                }
                Err(e) => {
                    assert!(e.to_string().contains("catch up first"), "{e}");
                    self.stats[10] += 1;
                }
            }
        }
        self.stale_verify(ix, label).await;
    }

    /// The status against the planner's due keys and the retry plan's.
    async fn stale_verify(&mut self, ix: &ChangeIndex, label: &str) {
        let edge = self.edge();
        let status = self.t.stale_status(&edge).unwrap();
        if matches!(status, Status::Unknown | Status::Unwatched | Status::Held) {
            return;
        }
        let ask = self.t.stale_ask(&edge).unwrap();
        let (due, _) = ask.keys(&OneIndex(ix), &NoneDue, None, 1000).await.unwrap();
        let retry: Vec<Key> = match &ask.retry {
            Some(r) => due::plan(&OneIndex(ix), r, &ask.req.conditions, 1000)
                .await
                .unwrap()
                .inputs[0]
                .keys
                .iter()
                .map(|c| c.key.clone())
                .collect(),
            None => vec![],
        };
        let keyed = ask.keyed();
        let now = &ask.req.conditions;
        let other = |h: &Conditions, key: &[u8]| !h.same_processing(now) || !h.patterns.takes(key);
        let why = |key: &[u8], held: &Holding| match held {
            Holding::Held => Why::NeverProcessed,
            Holding::Version(_, h) if other(h, key) || !now.patterns.takes(key) => {
                Why::ConditionsChanged
            }
            Holding::Version(..) => Why::InputChanged,
            Holding::Absent => match (keyed.point(key), keyed.value_of(key)) {
                (None, Value::Empty | Value::Held) => Why::NeverProcessed,
                (None, Value::At(_, h)) if other(&h, key) => Why::ConditionsChanged,
                _ => Why::InputChanged,
            },
        };
        let due_keys: Vec<(&Key, &Holding)> = due.iter().map(|c| (&c.key, &c.held)).collect();
        let at = format!("[{label}] {status:?}; due {due_keys:?}; retry {retry:?}");
        match &status {
            Status::Fresh => {
                self.stats[7] += 1;
                assert!(
                    due.is_empty() && retry.is_empty(),
                    "fresh, but something is due {at}"
                );
            }
            Status::Stale { key, reason, .. } => {
                let key = key.as_ref().unwrap();
                if *reason == Why::RetryDue {
                    self.stats[9] += 1;
                    assert!(
                        retry.contains(key),
                        "retry due on a key the pass does not take {at}"
                    );
                } else {
                    self.stats[8] += 1;
                    let Some(c) = due.iter().find(|c| c.key == *key) else {
                        panic!("stale on a key that is not due {at}");
                    };
                    assert_eq!(*reason, why(key, &c.held), "the reason {at}");
                }
            }
            _ => unreachable!(),
        }
    }

    /// The truth after an upstream commit, a change of conditions or a
    /// batch's commit (the model's `reverted`): a failing key no longer owed,
    /// because upstream returned to what the consumer holds or the conditions
    /// now take that, is moot.
    fn reverted(&mut self) {
        let gone: Vec<Key> = self
            .fails
            .keys()
            .filter(|k| !owed(&self.holding(k), &self.wanted(k)))
            .cloned()
            .collect();
        for k in gone {
            self.fails.remove(&k);
        }
    }

    fn report(&mut self) -> Reported {
        let kinds = [
            Kind::Failed,
            Kind::Rejected,
            Kind::Transient,
            Kind::TimedOut,
            Kind::Canceled,
        ];
        let kind = kinds[self.rng.random_range(0..kinds.len())];
        Reported {
            kind,
            message: format!("{kind:?}").into_bytes().into(),
            retry_after: self
                .rng
                .random_bool(0.3)
                .then(|| self.rng.random_range(0..300) as f64),
            retry_for: Some(self.rng.random_range(0..2000) as f64),
        }
    }

    /// Lineage (§7, HIS-7): a range batch's input recomputed from its record
    /// alone (the slice it replaced, its head and conditions, the failing
    /// keys it left waiting) matches the record's digest.
    fn lineage(&self, rec: &solera_tracking::record::Record) {
        let within = |r: &solera_tracking::progress::KeyRange, k: &Key| {
            r.lo.as_ref().is_none_or(|lo| k > lo) && r.hi.as_ref().is_none_or(|hi| k <= hi)
        };
        let mut keys = vec![];
        for k in self.universe.iter().filter(|k| within(&rec.range, k)) {
            let held = match rec.replaced.points.iter().find(|(x, _)| x == k) {
                Some((_, p)) => p.holding.clone(),
                None => match rec
                    .replaced
                    .pieces
                    .iter()
                    .find(|pc| within(&pc.range, k))
                    .map(|pc| &pc.value)
                {
                    Some(Value::At(c, cond)) => {
                        Holding::of(self.log.at(*c, k).as_version(), cond, k)
                    }
                    _ => Holding::Absent,
                },
            };
            let v = self.log.at(rec.head, k);
            let wanted = Holding::of(v.as_version(), &rec.conditions, k);
            if !owed(&held, &wanted) || rec.waiting.contains(k) {
                continue;
            }
            keys.push(plan::Cand {
                key: k.clone(),
                class: Class::of(&held, &wanted),
                version: if matches!(wanted, Holding::Absent) {
                    Ver::Absent
                } else {
                    v
                },
                held,
            });
        }
        let replay = plan::InputPlan {
            edge: rec.edge.clone(),
            output: rec.output.clone(),
            head: rec.head,
            conditions: rec.conditions.clone(),
            own_head: None,
            keys,
            held_back: vec![],
            moot: vec![],
            waiting: vec![],
            expect: 0,
            records: 0,
        };
        assert_eq!(
            replay.digest(),
            rec.digest,
            "lineage of a batch over {:?}: {:?}",
            rec.range,
            replay.keys
        );
    }

    /// The worker runs a plan's keys and the engine commits them; the truth follows.
    /// None when the commit is refused: the state moved since the plan.
    async fn run(&mut self, ix: &ChangeIndex, p: &plan::Plan, retry: bool) -> Option<Vec<Key>> {
        let (failed, loaded) = self.work(p);
        let report = InputReport {
            failed: failed.iter().map(|(k, _)| k.clone()).collect(),
            loaded: loaded.clone(),
        };
        let counters = match (
            &self
                .t
                .per_key
                .get(EDGE.as_bytes())
                .and_then(|k| k.pass.clone()),
            retry,
        ) {
            (Some(pass), true) => pass.counters,
            _ => self.counters,
        };
        // The engine builds the outcomes from a view, then applies them under
        // its lock: the input may commit in between, and a refused commit is
        // built again, its progress too.
        loop {
            let planned = self.t.progress_ops(p, &[report.clone()], b"", None).ok()?;
            let b = self
                .build(ix, p, &planned, &failed, &loaded, counters)
                .await?;
            let race = self.races && self.rng.random_bool(0.1);
            if race {
                trace("the input commits while the outcomes are built");
                for _ in 0..self.rng.random_range(1..=2) {
                    self.upstream(ix).await;
                }
            }
            match self.land(b) {
                Some(keys) => return Some(keys),
                None if race => continue,
                None => return None,
            }
        }
    }

    /// What the worker reports for a plan's keys: those that failed, and
    /// those it loaded at another version.
    fn work(&mut self, p: &plan::Plan) -> (Vec<(Key, Reported)>, Vec<(Key, Ver)>) {
        let ip = &p.inputs[0];
        let mut failed: Vec<(Key, Reported)> = vec![];
        let mut loaded: Vec<(Key, Ver)> = vec![];
        for c in &ip.keys {
            if c.class != Class::Removed && (self.fail_all || self.rng.random_bool(0.35)) {
                let r = self.report();
                failed.push((c.key.clone(), r));
            }
            if self.rng.random_bool(0.3) {
                let v = self.log.at(self.log.head(), &c.key);
                if v != c.version && !v.is_absent() && !c.version.is_absent() {
                    loaded.push((c.key.clone(), v));
                }
            }
        }
        (failed, loaded)
    }

    /// The engine catches up, then builds a batch's commit from a view.
    async fn build(
        &mut self,
        ix: &ChangeIndex,
        p: &plan::Plan,
        planned: &Ops,
        failed: &[(Key, Reported)],
        loaded: &[(Key, Ver)],
        counters: Counters,
    ) -> Option<Built> {
        let head = p.inputs[0].head;
        let unsettled = self.log.full_run.is_some()
            || self
                .log
                .clears
                .iter()
                .any(|&c| head < c && c <= self.log.head());
        self.catch_up(ix).await;
        let mut ops = planned.clone();
        let v = self.t.outcome_view(EDGE.as_bytes()).unwrap();
        let (f, d) = self.ctx();
        let ctx = Ctx {
            now: self.clock,
            counters: self.counters,
            retries: 2,
            time_ms: 0,
            producer: b"",
            failures_path: &f,
            due_path: &d,
        };
        let parts = oc::commit(
            &OneIndex(ix),
            &v,
            p,
            0,
            failed,
            loaded,
            &self.now,
            &ctx,
            &mut ops,
        )
        .await
        .inspect_err(|e| trace(&format!("the commit is refused: {e}")))
        .ok()?;
        Some(Built {
            plan: p.clone(),
            unsettled,
            parts,
            ops,
            failed: failed.to_vec(),
            loaded: loaded.to_vec(),
            counters,
            clock: self.clock,
        })
    }

    /// A built commit applies, or is refused; the truth follows it.
    fn land(&mut self, b: Built) -> Option<Vec<Key>> {
        let Built {
            plan: p,
            unsettled,
            mut parts,
            ops,
            failed,
            loaded,
            counters,
            clock,
        } = b;
        let mut all = ops.parts.clone();
        all.append(&mut parts);
        if let Err(e) = self.t.apply(&Event::of(all).encode()) {
            trace(&format!("the event is refused: {e}"));
            return None;
        }
        self.events += 1;
        if p.reset {
            self.era += 1;
        }
        for part in &ops.parts {
            if let Part::Batch(b) = part {
                assert_eq!(
                    ops.records[0].points, b.points,
                    "the batch record holds the points the batch set"
                );
            }
        }
        if p.mode == Mode::Incremental && !p.reset {
            self.lineage(&ops.records[0]);
        }
        // The truth.
        let ip = &p.inputs[0];
        let fails: BTreeMap<Key, Reported> = failed.into_iter().collect();
        let loaded: BTreeMap<Key, Ver> = loaded.into_iter().collect();
        // A key loaded at another version than planned while the input was
        // mid full run, or cleared since the plan, when B3 built the batch's
        // progress is held back: it keeps what it held and its outcome (D205,
        // the model's ev_commit).
        let held: Vec<Key> = ip
            .keys
            .iter()
            .filter(|c| {
                unsettled
                    && loaded.get(&c.key).is_some_and(|v| {
                        owed(
                            &Holding::of(v.as_version(), &ip.conditions, &c.key),
                            &Holding::of(c.version.as_version(), &ip.conditions, &c.key),
                        )
                    })
            })
            .map(|c| c.key.clone())
            .collect();
        assert_eq!(ops.held_back[0], held, "B3 holds back what the model does");
        self.stats[6] += held.len();
        for c in &ip.keys {
            if held.binary_search(&c.key).is_ok() {
                continue;
            }
            let got = loaded.get(&c.key).unwrap_or(&c.version);
            let tried = Holding::of(got.as_version(), &ip.conditions, &c.key);
            if fails.contains_key(&c.key) && !owed(&self.holding(&c.key), &tried) {
                self.fails.remove(&c.key);
            } else if let Some(r) = fails.get(&c.key) {
                let prior = self.fails.get(&c.key);
                let fresh = prior.is_none_or(|f| {
                    f.tried_ver != *got || !f.tried_cond.same_processing(&ip.conditions)
                });
                let s = transition(
                    prior.map(|f| &f.stored),
                    fresh,
                    r,
                    clock,
                    counters.deploy,
                    counters.forced_at(),
                    2,
                )
                .unwrap();
                self.fails.insert(
                    c.key.clone(),
                    Fail {
                        stored: s,
                        tried,
                        tried_cond: ip.conditions.clone(),
                        tried_ver: got.clone(),
                        at: ip.head,
                    },
                );
            } else {
                match tried {
                    Holding::Absent => self.holds.remove(&c.key),
                    h => self.holds.insert(c.key.clone(), h),
                };
                self.fails.remove(&c.key);
            }
        }
        self.reverted();
        Some(ip.keys.iter().map(|c| c.key.clone()).collect())
    }

    /// The engine builds a catch-up or a batch's commit, and applies it only
    /// later, after other events: refused, or the truth follows it.
    async fn defer(&mut self, ix: &ChangeIndex) {
        if self.rng.random_bool(0.4) {
            let v = self.t.outcome_view(EDGE.as_bytes()).unwrap();
            let (f, d) = self.ctx();
            let ctx = Ctx {
                now: self.clock,
                counters: self.counters,
                retries: 2,
                time_ms: 0,
                producer: b"",
                failures_path: &f,
                due_path: &d,
            };
            if let Some(parts) = oc::catch_up(&OneIndex(ix), &v, &self.now, &ctx)
                .await
                .unwrap()
            {
                self.deferred.push((self.era, Deferred::CatchUp(parts)));
            }
            return;
        }
        let mode = match self.rng.random_range(0..3) {
            0 => Mode::All,
            1 => Mode::Keys(
                self.universe
                    .iter()
                    .filter(|_| self.rng.random_bool(0.4))
                    .cloned()
                    .collect(),
            ),
            _ => Mode::Incremental,
        };
        let limit = self.rng.random_range(1..4);
        let req = self.request(None, limit, mode);
        let view = self.t.view(&req).unwrap();
        let p = plan::plan(&view, &req, &OneIndex(ix), &NoHold)
            .await
            .unwrap();
        let (failed, loaded) = self.work(&p);
        let report = InputReport {
            failed: failed.iter().map(|(k, _)| k.clone()).collect(),
            loaded: loaded.clone(),
        };
        let Ok(planned) = self.t.progress_ops(&p, &[report], b"", None) else {
            return;
        };
        let counters = self.counters;
        if let Some(b) = self
            .build(ix, &p, &planned, &failed, &loaded, counters)
            .await
        {
            self.deferred.push((self.era, Deferred::Commit(b)));
        }
    }

    fn land_deferred(&mut self) {
        if self.deferred.is_empty() {
            return;
        }
        let i = self.rng.random_range(0..self.deferred.len());
        let (era, d) = self.deferred.remove(i);
        let earlier = era != self.era;
        match d {
            Deferred::Start(parts) => match self.t.apply(&Event::of(parts).encode()) {
                Ok(()) => {
                    assert!(!earlier, "a pass start built in an earlier life applies");
                    trace("a deferred pass start applies");
                    self.events += 1;
                    self.stats[2] += 1;
                }
                Err(e) => trace(&format!("a deferred pass start is refused: {e}")),
            },
            Deferred::CatchUp(parts) => match self.t.apply(&Event::of(parts).encode()) {
                Ok(()) => {
                    assert!(!earlier, "a catch-up built in an earlier life applies");
                    trace("a deferred catch-up applies");
                    self.events += 1;
                }
                Err(e) => trace(&format!("a deferred catch-up is refused: {e}")),
            },
            Deferred::Commit(b) => {
                if self.land(b).is_some() {
                    assert!(!earlier, "a commit built in an earlier life applies");
                    trace("a deferred commit applies");
                    self.stats[5] += 1;
                }
            }
        }
    }

    fn under_way(&self) -> bool {
        self.t
            .per_key
            .get(EDGE.as_bytes())
            .is_some_and(|k| k.pass.is_some())
    }

    /// One step of a retry pass, interleaved with everything else: its start
    /// (applied now or held), or its next batch (committed now or held).
    async fn pass_step(&mut self, ix: &ChangeIndex) {
        if !self.under_way() {
            let Some(parts) = self.start_of(ix).await else {
                return;
            };
            if self.rng.random_bool(0.3) {
                trace("a pass start is held");
                self.deferred.push((self.era, Deferred::Start(parts)));
            } else {
                trace("a pass starts");
                self.apply(parts);
                self.stats[2] += 1;
            }
            return;
        }
        let r = self.t.retry_view(EDGE.as_bytes()).unwrap();
        let limit = self.rng.random_range(1..4);
        let p = due::plan(&OneIndex(ix), &r, &self.now, limit)
            .await
            .unwrap();
        for c in &p.inputs[0].keys {
            assert!(
                same(&c.held, &self.holding(&c.key)),
                "retry of {:?} holds {:?}, truth {:?}",
                c.key,
                c.held,
                self.holding(&c.key)
            );
            assert_eq!(
                c.class,
                Class::of(&self.holding(&c.key), &self.wanted(&c.key))
            );
        }
        self.stats[3] += p.inputs[0].keys.len();
        if self.rng.random_bool(0.25) {
            let (failed, loaded) = self.work(&p);
            let report = InputReport {
                failed: failed.iter().map(|(k, _)| k.clone()).collect(),
                loaded: loaded.clone(),
            };
            let Ok(planned) = self.t.progress_ops(&p, &[report], b"", None) else {
                return;
            };
            let counters = self.t.per_key[EDGE.as_bytes()]
                .pass
                .as_ref()
                .unwrap()
                .counters;
            if let Some(b) = self
                .build(ix, &p, &planned, &failed, &loaded, counters)
                .await
            {
                trace("a retry batch's commit is held");
                self.deferred.push((self.era, Deferred::Commit(b)));
            }
            return;
        }
        if self.rng.random_bool(0.2) {
            self.stale.push(p.clone());
        }
        self.run(ix, &p, true)
            .await
            .expect("a retry planned now commits");
    }

    /// The edge's records start over: the consumer holds nothing.
    fn forget(&mut self) {
        self.era += 1;
        self.holds.clear();
        self.fails.clear();
        self.cursor = None;
    }

    fn close_reopen(&mut self) {
        trace("close and reopen");
        self.apply(vec![Part::EdgeClose(EdgeClose { edge: key(EDGE) })]);
        self.apply(vec![Part::EdgeOpen(EdgeOpen {
            edge: key(EDGE),
            input: key("items"),
            keyed: true,
        })]);
        self.forget();
    }

    /// Renamed away and back: the records go and come back with the edge,
    /// in a later life.
    fn rename_round(&mut self) {
        trace("rename away and back");
        self.era += 1;
        self.apply(vec![Part::EdgeRename(EdgeRename {
            from: key(EDGE),
            to: key("tmp/items"),
            input: key("items"),
        })]);
        if self.rng.random_bool(0.5) {
            self.land_deferred();
        }
        self.apply(vec![Part::EdgeRename(EdgeRename {
            from: key("tmp/items"),
            to: key(EDGE),
            input: key("items"),
        })]);
    }

    fn rename_away_reopen(&mut self) {
        trace("rename away and reopen");
        self.files += 1;
        self.apply(vec![Part::EdgeRename(EdgeRename {
            from: key(EDGE),
            to: key(&format!("old{}/items", self.files)),
            input: key("items"),
        })]);
        self.apply(vec![Part::EdgeOpen(EdgeOpen {
            edge: key(EDGE),
            input: key("items"),
            keyed: true,
        })]);
        self.forget();
    }

    async fn change_batch(&mut self, ix: &ChangeIndex, upstream_meanwhile: bool) {
        let limit = self.rng.random_range(1..4);
        let mode = if self.rng.random_bool(0.1) {
            Mode::All
        } else {
            Mode::Incremental
        };
        let req = self.request(self.cursor.clone(), limit, mode);
        let view = self.t.view(&req).unwrap();
        let p = plan::plan(&view, &req, &OneIndex(ix), &NoHold)
            .await
            .unwrap();
        if upstream_meanwhile {
            if self.rng.random_bool(0.4) {
                self.upstream_clear(ix).await;
            } else {
                self.upstream(ix).await;
            }
        }
        self.stats[0] += 1;
        // A batch whose input moved on commits only after the catch-up, and
        // is planned again if the catch-up changed its range.
        if self.run(ix, &p, false).await.is_none() {
            assert!(
                self.log.head() > p.inputs[0].head,
                "a batch planned now commits"
            );
            return;
        }
        self.cursor = p.range.hi.clone();
    }

    async fn key_list(&mut self, ix: &ChangeIndex) {
        let keys: Vec<Key> = self
            .universe
            .iter()
            .filter(|_| self.rng.random_bool(0.4))
            .cloned()
            .collect();
        let req = self.request(None, 10, Mode::Keys(keys));
        let view = self.t.view(&req).unwrap();
        let p = plan::plan(&view, &req, &OneIndex(ix), &NoHold)
            .await
            .unwrap();
        self.stats[1] += 1;
        self.run(ix, &p, false)
            .await
            .expect("a batch planned now commits");
    }

    /// A plan held back, to commit after the state moved on.
    async fn hold(&mut self, ix: &ChangeIndex) {
        let req = if self.rng.random_bool(0.5) {
            let keys: Vec<Key> = self
                .universe
                .iter()
                .filter(|_| self.rng.random_bool(0.4))
                .cloned()
                .collect();
            self.request(None, 10, Mode::Keys(keys))
        } else {
            let limit = self.rng.random_range(1..4);
            self.request(None, limit, Mode::Incremental)
        };
        let view = self.t.view(&req).unwrap();
        let p = plan::plan(&view, &req, &OneIndex(ix), &NoHold)
            .await
            .unwrap();
        self.stale.push(p);
    }

    /// A held plan commits late: refused, or the truth follows it.
    async fn late(&mut self, ix: &ChangeIndex) {
        if self.stale.is_empty() {
            return;
        }
        let i = self.rng.random_range(0..self.stale.len());
        let p = self.stale.remove(i);
        let pass = self
            .t
            .per_key
            .get(EDGE.as_bytes())
            .and_then(|k| k.pass.as_ref().map(|p| (k.life, p.n)));
        if self.run(ix, &p, p.pass.is_some()).await.is_some() {
            assert!(
                p.pass.is_none() || p.pass.as_ref().map(|p| (p.life, p.n)) == pass,
                "a retry batch of pass {:?} commits in pass {pass:?}",
                p.pass
            );
            self.stats[5] += 1;
        }
    }

    async fn upstream(&mut self, ix: &ChangeIndex) {
        let mut changes = BTreeMap::new();
        for k in self.universe.clone() {
            if self.rng.random_bool(0.3) {
                changes.insert(k, version(&mut self.rng));
            }
        }
        let ends = self.rng.random_bool(0.5);
        if self.log.commit(ix, &mut self.t, changes, false, ends).await {
            self.stale_verify(ix, "after the commit's notice").await;
            self.reverted();
        }
    }

    /// A full run of the input begins: its first commit clears, and the run
    /// may go on over later commits.
    async fn upstream_clear(&mut self, ix: &ChangeIndex) {
        let mut changes = BTreeMap::new();
        for k in self.universe.clone() {
            if self.rng.random_bool(0.6) {
                changes.insert(k, Ver::Int(self.rng.random_range(1..4)));
            }
        }
        let ends = self.rng.random_bool(0.3);
        trace("the input clears");
        if self.log.commit(ix, &mut self.t, changes, true, ends).await {
            self.stale_verify(ix, "after the commit's notice").await;
            self.reverted();
        }
    }

    /// A retry pass, run to its end with nothing else happening.
    async fn pass(&mut self, ix: &ChangeIndex) {
        let v = self.t.outcome_view(EDGE.as_bytes()).unwrap();
        let list = self.t.retry_view(EDGE.as_bytes()).unwrap().pass_list;
        let at = self.clock as u64;
        self.files += 1;
        let path = format!("p/{}", self.files);
        let started = due::start(&OneIndex(ix), &v, &list, at, self.counters, &path)
            .await
            .unwrap();
        let by_counters = due::by_counters(&v, &self.counters);
        // Expected: records eligible at the start (by the clock alone unless a
        // deploy or a forced request is pending) that are owed and unchanged.
        // Those it reads that hold what is wanted are moot.
        let read: Vec<Key> = self
            .fails
            .iter()
            .filter(|(_, f)| {
                if by_counters {
                    eligible(&f.stored, at as f64, &self.counters)
                } else {
                    f.stored.timed() && f.stored.next_at <= at
                }
            })
            .map(|(k, _)| k.clone())
            .collect();
        let expected: BTreeSet<Key> = read
            .iter()
            .filter(|k| {
                !owed(&self.fails[*k].tried, &self.wanted(k))
                    && owed(&self.holding(k), &self.wanted(k))
            })
            .cloned()
            .collect();
        let moot: Vec<Key> = read
            .iter()
            .filter(|k| !owed(&self.holding(k), &self.wanted(k)))
            .cloned()
            .collect();
        let Some(parts) = started else {
            assert!(
                expected.is_empty(),
                "no pass started, but {expected:?} are due"
            );
            return;
        };
        self.apply(parts);
        self.stats[2] += 1;
        let mut taken = BTreeSet::new();
        let mut guard = 0;
        while self.t.per_key[EDGE.as_bytes()].pass.is_some() {
            guard += 1;
            assert!(guard < 100, "a pass that never ends");
            let r = self.t.retry_view(EDGE.as_bytes()).unwrap();
            let limit = self.rng.random_range(1..4);
            let p = due::plan(&OneIndex(ix), &r, &self.now, limit)
                .await
                .unwrap();
            for c in &p.inputs[0].keys {
                assert!(!taken.contains(&c.key), "a pass takes {:?} twice", c.key);
                let wanted = self.wanted(&c.key);
                assert!(
                    same(&c.held, &self.holding(&c.key)),
                    "retry of {:?} holds {:?}, truth {:?}",
                    c.key,
                    c.held,
                    self.holding(&c.key)
                );
                assert_eq!(c.class, Class::of(&self.holding(&c.key), &wanted));
            }
            self.stats[3] += p.inputs[0].keys.len();
            if self.rng.random_bool(0.3) {
                self.stale.push(p.clone());
            }
            let got = self
                .run(ix, &p, true)
                .await
                .expect("a retry planned now commits");
            taken.extend(got);
        }
        assert_eq!(
            taken, expected,
            "the pass took {taken:?}, expected {expected:?}"
        );
        for k in moot {
            self.fails.remove(&k);
        }
    }

    async fn check(&mut self, ix: &ChangeIndex) {
        let edge = self.edge();
        let keyed = self.t.edges[&edge].progress.keyed().unwrap().clone();
        let fo = self
            .t
            .outputs
            .get(&output_id(FAILURES, &edge))
            .cloned()
            .unwrap_or_default();
        let ks: Vec<&[u8]> = self.universe.iter().map(|k| k.as_ref()).collect();
        let recs = records(&OneIndex(ix), &output_id(FAILURES, &edge), &fo, &ks)
            .await
            .unwrap();
        let pk = self.t.per_key.get(&edge).cloned().unwrap_or_default();
        for k in &self.universe {
            let decoded = match recs.get(k) {
                Some(r) => r.held.holding(&pk.conds).unwrap(),
                None => match keyed.point(k) {
                    Some(p) => p.holding.clone(),
                    None => match keyed.value_of(k) {
                        Value::Empty => Holding::Absent,
                        Value::Held => Holding::Held,
                        Value::At(p, c) => Holding::of(self.log.at(*p, k).as_version(), c, k),
                    },
                },
            };
            assert!(
                same(&decoded, &self.holding(k)),
                "{k:?} decodes to {decoded:?}, the consumer holds {:?}",
                self.holding(k)
            );
            if let (Some(r), Some(f)) = (recs.get(k), self.fails.get(k)) {
                if owed(&self.holding(k), &self.wanted(k)) {
                    assert_eq!(r.stored, f.stored, "{k:?}'s outcome");
                }
            }
            if self.fails.contains_key(k) {
                assert!(recs.contains_key(k), "{k:?} fails but has no record");
            }
        }
        // Counts follow the records.
        let mut counts = [0u64; 6];
        for r in recs.values() {
            counts[r.stored.outcome as usize] += 1;
        }
        assert_eq!(pk.counts, counts, "counts by outcome");
        assert_eq!(fo.live, recs.len() as u64);
        // A plan from the start takes exactly the due keys.
        let req = self.request(None, 1000, Mode::Incremental);
        let view = self.t.view(&req).unwrap();
        let p = plan::plan(&view, &req, &OneIndex(ix), &NoHold)
            .await
            .unwrap();
        let taken: Vec<Key> = p.inputs[0].keys.iter().map(|c| c.key.clone()).collect();
        assert_eq!(taken, self.due(), "the plan takes {taken:?}; due {:?}; owed {:?}\nrecs {recs:?}\nfails {:?}\nholds {:?}\nclock {} counters {:?}\nkeyed {keyed:?}\nhead {} {:?}\nnow {:?}", self.due(), self.owed(), self.fails, self.holds, self.clock, self.counters, self.log.head(), self.log.states.last(), self.now);
        for c in &p.inputs[0].keys {
            assert_eq!(
                c.class,
                Class::of(&self.holding(&c.key), &self.wanted(&c.key)),
                "{:?}'s class",
                c.key
            );
        }
        let (keys, _) = plan::owed_keys(&view, &req.inputs[0], &OneIndex(ix), None, 1000)
            .await
            .unwrap();
        assert_eq!(
            keys.iter().map(|c| c.key.clone()).collect::<Vec<_>>(),
            self.due()
        );
        let n = plan::owed_count(&view, &req.inputs[0], &OneIndex(ix))
            .await
            .unwrap();
        assert_eq!(
            n.iter().sum::<u64>() as usize,
            self.owed().len(),
            "owed count {n:?}, owed {:?}",
            self.owed()
        );
        let back = Tracking::restore(&self.t.snapshot()).unwrap();
        assert_eq!(back, self.t);
        // A failing key waiting for a retry it is eligible for gets a pass.
        if pk.pass.is_none() {
            let eligible_now: Vec<Key> = self
                .fails
                .iter()
                .filter(|(k, f)| eligible(&f.stored, self.clock, &self.counters) && self.waiting(k))
                .map(|(k, _)| k.clone())
                .collect();
            if !eligible_now.is_empty() {
                assert!(
                    self.start_of(ix).await.is_some(),
                    "{eligible_now:?} wait for a retry they are eligible for, and no pass starts"
                );
            }
        }
    }
}

fn world(seed: u64) -> World {
    let mut rng = SmallRng::seed_from_u64(seed);
    let universe: Vec<Key> = (1..=rng.random_range(3..8))
        .map(|i| key(&format!("k{i}")))
        .chain(std::iter::once(key("x1")))
        .collect();
    let now = Arc::new(Conditions {
        patterns: patterns(&mut rng),
        definition: b"d1".as_slice().into(),
        ..Default::default()
    });
    let mut w = World {
        fail_all: false,
        races: false,
        deferred: vec![],
        stale_rng: SmallRng::seed_from_u64(seed ^ 0x5eed),
        gate: seed % 2 == 0,
        rng,
        t: Tracking::new(),
        log: Log {
            name: "items".into(),
            states: vec![BTreeMap::new()],
            layers: vec![],
            clears: vec![],
            full_run: None,
        },
        universe,
        now,
        holds: BTreeMap::new(),
        fails: BTreeMap::new(),
        cursor: None,
        clock: 1_000_000.0,
        counters: Counters {
            deploy: 1,
            forced: [0; 6],
        },
        files: 0,
        events: 0,
        stats: [0; 11],
        stale: vec![],
        era: 0,
    };
    w.apply(vec![Part::EdgeOpen(EdgeOpen {
        edge: key(EDGE),
        input: key("items"),
        keyed: true,
    })]);
    w
}

async fn history(seed: u64, ix: &ChangeIndex) -> [usize; 11] {
    let mut w = world(seed);
    let trace = std::env::var("OUTCOME_TRACE").is_ok();
    for step in 0..60 {
        let op = w.rng.random_range(0..36);
        if trace {
            eprintln!(
                "step {step} op {op} head {} now {:?} clock {} counters {:?}",
                w.log.head(),
                w.now.definition,
                w.clock,
                w.counters
            );
        }
        match op {
            0..=4 => w.upstream(ix).await,
            5..=9 => {
                let meanwhile = w.rng.random_bool(0.15);
                w.races = true;
                w.change_batch(ix, meanwhile).await;
                w.races = false;
            }
            10 => w.key_list(ix).await,
            11 => {
                w.catch_up(ix).await;
                w.now = Arc::new(Conditions {
                    definition: format!("d{}", w.rng.random_range(1..3)).into_bytes().into(),
                    ..(*w.now).clone()
                });
                w.cursor = None;
                w.reverted();
            }
            12 => {
                w.catch_up(ix).await;
                let to = w.t.rebuild(EDGE.as_bytes()) + 1;
                let life = w.t.per_key[EDGE.as_bytes()].life;
                w.apply(vec![Part::Rebuild(Rebuild {
                    edge: key(EDGE),
                    to,
                    life,
                })]);
                w.now = Arc::new(Conditions {
                    rebuild: to,
                    ..(*w.now).clone()
                });
                w.cursor = None;
                w.reverted();
            }
            13 => {
                w.catch_up(ix).await;
                let p = patterns(&mut w.rng);
                w.now = Arc::new(Conditions {
                    patterns: p,
                    ..(*w.now).clone()
                });
                w.reverted();
            }
            14 | 15 => w.clock += w.rng.random_range(0..600) as f64,
            16 => w.counters.deploy += 1,
            17 => {
                let c = w.rng.random_range(1..6);
                w.counters.forced[c] = w.events + 1;
            }
            18 | 20..=22 if w.under_way() => w.pass_step(ix).await,
            18 | 20..=22 => w.pass(ix).await,
            27..=30 => w.pass_step(ix).await,
            31 => w.close_reopen(),
            32 => w.rename_round(),
            33 => w.rename_away_reopen(),
            34 | 35 => w.upstream_clear(ix).await,
            23 => w.hold(ix).await,
            24 => w.late(ix).await,
            25 => w.defer(ix).await,
            26 => w.land_deferred(),
            19 => {
                if w.rng.random_bool(0.3) {
                    w.apply(vec![Part::Reset(Reset {
                        edge: key(EDGE),
                        held: false,
                    })]);
                    w.forget();
                } else {
                    w.t = Tracking::restore(&w.t.snapshot()).unwrap();
                }
            }
            _ => unreachable!(),
        }
        w.stale_check(ix, "before the catch-up").await;
        w.catch_up(ix).await;
        w.stale_check(ix, "after the catch-up").await;
        if trace {
            let short = |h: &Holding| match h {
                Holding::Version(v, c) => format!(
                    "{v:?}@{:?}/{}",
                    String::from_utf8_lossy(&c.definition),
                    c.rebuild
                ),
                h => format!("{h:?}"),
            };
            let fails: Vec<String> = w
                .fails
                .iter()
                .map(|(k, f)| {
                    format!(
                        "{}:o{} tried {} at {}",
                        String::from_utf8_lossy(k),
                        f.stored.outcome,
                        short(&f.tried),
                        f.at
                    )
                })
                .collect();
            let holds: Vec<String> = w
                .holds
                .iter()
                .map(|(k, h)| format!("{}:{}", String::from_utf8_lossy(k), short(h)))
                .collect();
            let keyed = w.t.edges[&w.edge()].progress.keyed().unwrap();
            let points: Vec<String> = keyed
                .points_in(&Default::default())
                .map(|(k, pt)| {
                    format!(
                        "{}:{}/{:?}",
                        String::from_utf8_lossy(k),
                        short(&pt.holding),
                        pt.reason
                    )
                })
                .collect();
            let fo =
                w.t.outputs
                    .get(&output_id(FAILURES, &w.edge()))
                    .cloned()
                    .unwrap_or_default();
            let ks: Vec<&[u8]> = w.universe.iter().map(|k| k.as_ref()).collect();
            let recs = records(&OneIndex(ix), &output_id(FAILURES, &w.edge()), &fo, &ks)
                .await
                .unwrap();
            let recs: Vec<String> = recs
                .iter()
                .map(|(k, r)| {
                    format!(
                        "{}:o{} on {:?} held {:?}",
                        String::from_utf8_lossy(k),
                        r.stored.outcome,
                        r.on,
                        r.held
                    )
                })
                .collect();
            eprintln!("  fails {fails:?}\n  recs {recs:?}\n  holds {holds:?}\n  pieces {:?}\n  points {points:?}\n  head {:?}", keyed.pieces(&Default::default()).iter().map(|pc| format!("{:?}..{:?} {}", pc.range.lo.as_ref().map(|k| String::from_utf8_lossy(k).to_string()), pc.range.hi.as_ref().map(|k| String::from_utf8_lossy(k).to_string()), match &pc.value { Value::At(p, c) => format!("At {p} {:?}", String::from_utf8_lossy(&c.definition)), v => format!("{v:?}")})).collect::<Vec<_>>(), w.log.states.last());
        }
        w.check(ix).await;
    }
    w.stats[4] = w.fails.len();
    w.stats
}

#[test]
fn random_histories() {
    let seeds: u64 = std::env::var("OUTCOME_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut total = [0usize; 11];
    let only: Option<u64> = std::env::var("OUTCOME_SEED")
        .ok()
        .and_then(|s| s.parse().ok());
    for seed in (0..seeds).filter(|s| only.is_none_or(|o| o == *s)) {
        let ix = index(&rt);
        let s = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(history(seed, &ix))
        }));
        match s {
            Ok(s) => {
                for i in 0..11 {
                    total[i] += s[i];
                }
            }
            Err(e) => {
                eprintln!("seed {seed} failed");
                std::panic::resume_unwind(e);
            }
        }
    }
    eprintln!("{seeds} histories: change batches {}, key lists {}, retry passes {} taking {} keys, failing keys left {}, late commits accepted {}, keys held back {}; staleness checked fresh {}, stale {}, retry due {}; refreshes refused before a catch-up {}", total[0], total[1], total[2], total[3], total[4], total[5], total[6], total[7], total[8], total[9], total[10]);
}

/// A retry batch planned in one pass never commits in another (review F3):
/// a pass ends, a new one starts at the same place, and the old pass's plan
/// is refused.
#[test]
fn a_retry_batch_belongs_to_its_pass() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = world(7);
        w.now = Arc::new(Conditions {
            definition: b"d1".as_slice().into(),
            ..Default::default()
        });
        let changes = w
            .universe
            .iter()
            .map(|k| (k.clone(), Ver::Int(5)))
            .collect();
        assert!(w.log.commit(&ix, &mut w.t, changes, false, true).await);
        w.fail_all = true;
        loop {
            w.change_batch(&ix, false).await;
            if w.cursor.is_none() {
                break;
            }
        }
        let start = |w: &mut World| {
            for f in &mut w.counters.forced[1..] {
                *f = w.events + 1;
            }
            w.files += 1;
            (w.t.outcome_view(EDGE.as_bytes()).unwrap(), w.files)
        };
        let (v, f) = start(&mut w);
        let list = w.t.retry_view(EDGE.as_bytes()).unwrap().pass_list;
        let parts = due::start(
            &OneIndex(&ix),
            &v,
            &list,
            w.clock as u64,
            w.counters,
            &format!("p/{f}"),
        )
        .await
        .unwrap()
        .unwrap();
        w.apply(parts);
        let r = w.t.retry_view(EDGE.as_bytes()).unwrap();
        let p = due::plan(&OneIndex(&ix), &r, &w.now, 1000).await.unwrap();
        assert!(p.range.hi.is_none() && !p.inputs[0].keys.is_empty());
        w.run(&ix, &p, true)
            .await
            .expect("the pass's batch commits");
        assert!(w.t.per_key[EDGE.as_bytes()].pass.is_none());
        let (v, f) = start(&mut w);
        let list = w.t.retry_view(EDGE.as_bytes()).unwrap().pass_list;
        let parts = due::start(
            &OneIndex(&ix),
            &v,
            &list,
            w.clock as u64,
            w.counters,
            &format!("p/{f}"),
        )
        .await
        .unwrap()
        .unwrap();
        w.apply(parts);
        let ops = w.t.progress_ops(&p, &[Default::default()], b"", None);
        let v = w.t.outcome_view(EDGE.as_bytes()).unwrap();
        let ctx = Ctx {
            now: w.clock,
            counters: w.counters,
            retries: 2,
            time_ms: 0,
            producer: b"",
            failures_path: "f/x",
            due_path: "d/x",
        };
        let got = match ops {
            Err(e) => panic!("progress refused it first: {e:?}"),
            Ok(mut ops) => {
                oc::commit(&OneIndex(&ix), &v, &p, 0, &[], &[], &w.now, &ctx, &mut ops).await
            }
        };
        let err = got.expect_err("an old pass's batch is refused");
        assert!(format!("{err:?}").contains("pass that ended"), "{err:?}");
    });
}

/// One scripted history for a regression test: the keys k1 to k3, all taken,
/// under one definition. No brute-force truth is kept: these tests look at
/// the core's own state.
fn scripted() -> World {
    let mut w = world(7);
    w.universe = ["k1", "k2", "k3"].map(key).to_vec();
    w.now = Arc::new(Conditions {
        definition: b"d1".as_slice().into(),
        ..Default::default()
    });
    w
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

/// The same conditions, with patterns that take only k.
fn only(c: &Conditions, k: &str) -> Arc<Conditions> {
    Arc::new(Conditions {
        patterns: Patterns::new(vec![Pat::Glob(k.into())], vec![]).unwrap(),
        ..c.clone()
    })
}

fn taken(p: &plan::Plan) -> Vec<Key> {
    p.inputs[0].keys.iter().map(|c| c.key.clone()).collect()
}

impl World {
    /// The input commits each key at its version (0: removed).
    async fn input(&mut self, ix: &ChangeIndex, changes: &[(&str, u64)]) {
        let changes = changes
            .iter()
            .map(|(k, v)| match v {
                0 => (key(k), Ver::Absent),
                v => (key(k), Ver::Int(*v)),
            })
            .collect();
        assert!(self.log.commit(ix, &mut self.t, changes, false, true).await);
    }

    async fn planned(
        &self,
        ix: &ChangeIndex,
        after: Option<Key>,
        limit: usize,
        mode: Mode,
    ) -> plan::Plan {
        let req = self.request(after, limit, mode);
        let view = self.t.view(&req).unwrap();
        plan::plan(&view, &req, &OneIndex(ix), &NoHold)
            .await
            .unwrap()
    }

    /// The event of a plan's commit, built from a view now and not applied,
    /// without the catch-up the engine runs first: fail names the keys that
    /// fail and how, loaded those the worker read at another version. Or why
    /// the commit is refused.
    async fn commit_of(
        &mut self,
        ix: &ChangeIndex,
        p: &plan::Plan,
        fail: &[(&str, Kind)],
        loaded: &[(&str, u64)],
        counters: Counters,
    ) -> Result<Vec<Part>, String> {
        let failed: Vec<(Key, Reported)> = fail
            .iter()
            .map(|(k, kind)| {
                let r = Reported {
                    kind: *kind,
                    message: b"boom".as_slice().into(),
                    retry_after: None,
                    retry_for: Some(1000.0),
                };
                (key(k), r)
            })
            .collect();
        let loaded: Vec<(Key, Ver)> = loaded.iter().map(|(k, v)| (key(k), Ver::Int(*v))).collect();
        let report = InputReport {
            failed: failed.iter().map(|(k, _)| k.clone()).collect(),
            loaded: loaded.clone(),
        };
        let mut ops = self
            .t
            .progress_ops(p, &[report], b"", None)
            .map_err(|e| e.to_string())?;
        let v = self.t.outcome_view(EDGE.as_bytes()).unwrap();
        let (f, d) = self.ctx();
        let ctx = Ctx {
            now: self.clock,
            counters,
            retries: 2,
            time_ms: 0,
            producer: b"",
            failures_path: &f,
            due_path: &d,
        };
        let now = self.now.clone();
        let mut parts = oc::commit(
            &OneIndex(ix),
            &v,
            p,
            0,
            &failed,
            &loaded,
            &now,
            &ctx,
            &mut ops,
        )
        .await
        .map_err(|e| e.to_string())?;
        let mut all = ops.parts;
        all.append(&mut parts);
        Ok(all)
    }

    /// A catch-up built from a view now, not applied.
    async fn caught(&mut self, ix: &ChangeIndex) -> Option<Vec<Part>> {
        let v = self.t.outcome_view(EDGE.as_bytes()).unwrap();
        let (f, d) = self.ctx();
        let ctx = Ctx {
            now: self.clock,
            counters: self.counters,
            retries: 2,
            time_ms: 0,
            producer: b"",
            failures_path: &f,
            due_path: &d,
        };
        oc::catch_up(&OneIndex(ix), &v, &self.now, &ctx)
            .await
            .unwrap()
    }

    /// A retry pass's start built from a view now, not applied; None when
    /// nothing is due.
    async fn start_of(&mut self, ix: &ChangeIndex) -> Option<Vec<Part>> {
        let r = self.t.retry_view(EDGE.as_bytes()).unwrap();
        self.files += 1;
        let path = format!("p/{}", self.files);
        let at = self.clock as u64;
        due::start(
            &OneIndex(ix),
            &r.outcomes,
            &r.pass_list,
            at,
            self.counters,
            &path,
        )
        .await
        .unwrap()
    }

    async fn retry_plan(&self, ix: &ChangeIndex, limit: usize) -> plan::Plan {
        let r = self.t.retry_view(EDGE.as_bytes()).unwrap();
        due::plan(&OneIndex(ix), &r, &self.now, limit)
            .await
            .unwrap()
    }

    /// The edge's records, by key.
    async fn stored(&self, ix: &ChangeIndex) -> BTreeMap<Key, solera_tracking::outcomes::Record> {
        let id = output_id(FAILURES, &self.edge());
        let out = self.t.outputs.get(&id).cloned().unwrap_or_default();
        let ks: Vec<&[u8]> = self.universe.iter().map(|k| k.as_ref()).collect();
        records(&OneIndex(ix), &id, &out, &ks).await.unwrap()
    }

    /// Where the edge last looked for reverts: through which commit of the
    /// input, under which conditions.
    fn looked(&self) -> (u64, u64) {
        let k = &self.t.per_key[EDGE.as_bytes()];
        (k.seen, k.cond)
    }
}

/// What starts an edge's records over: a reset, a close and a reopen, a
/// rename and a reopen of the old name.
fn restarts() -> [Vec<Part>; 3] {
    [
        vec![Part::Reset(Reset {
            edge: key(EDGE),
            held: false,
        })],
        vec![
            Part::EdgeClose(EdgeClose { edge: key(EDGE) }),
            Part::EdgeOpen(EdgeOpen {
                edge: key(EDGE),
                input: key("items"),
                keyed: true,
            }),
        ],
        vec![
            Part::EdgeRename(EdgeRename {
                from: key(EDGE),
                to: key("moved"),
                input: key("items"),
            }),
            Part::EdgeOpen(EdgeOpen {
                edge: key(EDGE),
                input: key("items"),
                keyed: true,
            }),
        ],
    ]
}

/// A part built before the edge's records started over applies to none of
/// the records after (second review H1). A reset or a close starts the
/// failures head and the pass numbers over, so a commit or a pass start built
/// before would meet its head and its turn again.
#[test]
fn a_part_built_before_the_records_started_over_is_refused() {
    let rt = runtime();
    for restart in restarts() {
        let ix = index(&rt);
        rt.block_on(async {
            let mut w = scripted();
            let c = w.counters;
            w.input(&ix, &[("k1", 5), ("k2", 5)]).await;
            let first = w.planned(&ix, None, 1, Mode::Incremental).await;
            let ev = w
                .commit_of(&ix, &first, &[("k1", Kind::Failed)], &[], c)
                .await
                .unwrap();
            w.apply(ev);
            // Built before: k2's failure in the next range, and a pass.
            let next = first.range.hi.clone();
            let next = w.planned(&ix, next, 1, Mode::Incremental).await;
            let commit = w.commit_of(&ix, &next, &[("k2", Kind::Failed)], &[], c);
            let commit = commit.await.unwrap();
            let start = w.start_of(&ix).await.expect("a failed key, a deploy");
            let rebuild = vec![Part::Rebuild(Rebuild {
                edge: key(EDGE),
                to: w.t.rebuild(EDGE.as_bytes()) + 1,
                life: w.t.per_key[EDGE.as_bytes()].life,
            })];
            for part in restart {
                w.apply(vec![part]);
            }
            // The first range fails again, as another outcome: the failures
            // head is where it was, over other records.
            let first = w.planned(&ix, None, 1, Mode::Incremental).await;
            let ev = w
                .commit_of(&ix, &first, &[("k1", Kind::Rejected)], &[], c)
                .await
                .unwrap();
            w.apply(ev);
            let before = w.t.snapshot();
            for stale in [commit, start, rebuild] {
                let err = w.t.apply(&Event::of(stale).encode()).unwrap_err();
                assert!(err.to_string().contains("started over"), "{err}");
            }
            assert_eq!(w.t.snapshot(), before);
        });
    }
}

/// A failure recorded under an older deploy than the last pass ran under
/// still gets that deploy's retry (second review M1, §9's deploy_min): its
/// batch was in flight when the deploy's pass ended.
#[test]
fn a_failure_under_an_older_deploy_than_the_last_pass_starts_a_pass() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = scripted();
        let old = w.counters;
        w.input(&ix, &[("k1", 5), ("k2", 5)]).await;
        let first = w.planned(&ix, None, 1, Mode::Incremental).await;
        let ev = w
            .commit_of(&ix, &first, &[("k1", Kind::Failed)], &[], old)
            .await
            .unwrap();
        w.apply(ev);
        let flying = first.range.hi.clone();
        let flying = w.planned(&ix, flying, 1, Mode::Incremental).await;
        w.counters.deploy += 1;
        let new = w.counters;
        let start = w.start_of(&ix).await.expect("the deploy's pass");
        w.apply(start);
        let retry = w.retry_plan(&ix, 10).await;
        assert_eq!(taken(&retry), [key("k1")]);
        let ev = w.commit_of(&ix, &retry, &[], &[], new).await.unwrap();
        w.apply(ev);
        assert!(w.t.per_key[EDGE.as_bytes()].pass.is_none());
        // The batch that ran under the old deploy commits its failure.
        let ev = w
            .commit_of(&ix, &flying, &[("k2", Kind::Failed)], &[], old)
            .await
            .unwrap();
        w.apply(ev);
        let start = w.start_of(&ix).await;
        w.apply(start.expect("k2 has not had the deploy's retry"));
        assert_eq!(taken(&w.retry_plan(&ix, 10).await), [key("k2")]);
    });
}

/// The same behind a pass's walk (second review M1): a failure recorded
/// under an older deploy, at a key the deploy's pass has gone past, starts
/// the next pass when this one ends.
#[test]
fn a_failure_behind_a_pass_under_an_older_deploy_starts_the_next() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = scripted();
        let old = w.counters;
        w.input(&ix, &[("k1", 5), ("k2", 5), ("k3", 5)]).await;
        let flying = w.planned(&ix, None, 1, Mode::Incremental).await;
        let rest = flying.range.hi.clone();
        let rest = w.planned(&ix, rest, 10, Mode::Incremental).await;
        let fail = [("k2", Kind::Failed), ("k3", Kind::Failed)];
        let ev = w.commit_of(&ix, &rest, &fail, &[], old).await.unwrap();
        w.apply(ev);
        w.counters.deploy += 1;
        let new = w.counters;
        let start = w.start_of(&ix).await.expect("the deploy's pass");
        w.apply(start);
        let step = w.retry_plan(&ix, 1).await;
        assert_eq!(taken(&step), [key("k2")]);
        let ev = w.commit_of(&ix, &step, &[], &[], new).await.unwrap();
        w.apply(ev);
        assert!(w.t.per_key[EDGE.as_bytes()].pass.is_some());
        // Behind the walk, the batch that ran under the old deploy commits.
        let ev = w
            .commit_of(&ix, &flying, &[("k1", Kind::Failed)], &[], old)
            .await
            .unwrap();
        w.apply(ev);
        let step = w.retry_plan(&ix, 10).await;
        assert_eq!(taken(&step), [key("k3")]);
        let ev = w.commit_of(&ix, &step, &[], &[], new).await.unwrap();
        w.apply(ev);
        assert!(w.t.per_key[EDGE.as_bytes()].pass.is_none());
        let start = w.start_of(&ix).await;
        w.apply(start.expect("k1 has not had the deploy's retry"));
        assert_eq!(taken(&w.retry_plan(&ix, 10).await), [key("k1")]);
    });
}

/// k1, held at 5, fails on 6, and reverts were looked for through the
/// input's head: where the tests of the second review's L1 start.
async fn k1_fails_on_6(w: &mut World, ix: &ChangeIndex) {
    let c = w.counters;
    w.input(ix, &[("k1", 5), ("k2", 5)]).await;
    let p = w.planned(ix, None, 10, Mode::Incremental).await;
    let ev = w.commit_of(ix, &p, &[], &[], c).await.unwrap();
    w.apply(ev);
    w.input(ix, &[("k1", 6)]).await;
    let p = w.planned(ix, None, 10, Mode::Incremental).await;
    let ev = w
        .commit_of(ix, &p, &[("k1", Kind::Failed)], &[], c)
        .await
        .unwrap();
    w.apply(ev);
}

/// Under patterns that do not take k1, the input returns it to the 5 the
/// consumer holds and then to the 6 it failed on, the edge catching up each
/// time: under those patterns the failure is not moot.
async fn k1_returns_and_leaves_untaken(w: &mut World, ix: &ChangeIndex) {
    w.now = only(&w.now.clone(), "k2");
    w.catch_up(ix).await;
    for v in [5, 6] {
        w.input(ix, &[("k1", v)]).await;
        w.catch_up(ix).await;
    }
}

/// A catch-up applies only from where reverts were last looked for when it
/// was built (second review L1). Applied later it would have the commits
/// since looked at again under other conditions, and k1's return to 5, which
/// the patterns then did not take, would make its failure moot.
#[test]
fn a_catch_up_built_before_another_looked_is_refused() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = scripted();
        let first = w.now.clone();
        k1_fails_on_6(&mut w, &ix).await;
        w.input(&ix, &[("k2", 7)]).await;
        let stale = w.caught(&ix).await.expect("the input moved");
        w.catch_up(&ix).await;
        k1_returns_and_leaves_untaken(&mut w, &ix).await;
        let err = w.t.apply(&Event::of(stale).encode()).unwrap_err();
        assert!(err.to_string().contains("looked for since"), "{err}");
        w.now = first;
        w.catch_up(&ix).await;
        let recs = w.stored(&ix).await;
        assert!(recs.contains_key(&key("k1")), "k1's failure stands");
    });
}

/// A batch in which no key failed applies whatever was looked for since it
/// was built, and leaves that as it is (second review L1): it once wrote back
/// its view's, with the same effect as the catch-up above.
#[test]
fn a_batch_without_a_failure_leaves_where_reverts_were_looked_for() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = scripted();
        let c = w.counters;
        let first = w.now.clone();
        k1_fails_on_6(&mut w, &ix).await;
        w.input(&ix, &[("k2", 7)]).await;
        w.catch_up(&ix).await;
        let p = w.planned(&ix, None, 10, Mode::Incremental).await;
        assert_eq!(taken(&p), [key("k2")]);
        let built = w.commit_of(&ix, &p, &[], &[], c).await.unwrap();
        k1_returns_and_leaves_untaken(&mut w, &ix).await;
        let looked = w.looked();
        w.apply(built);
        assert_eq!(w.looked(), looked);
        w.now = first;
        w.catch_up(&ix).await;
        let recs = w.stored(&ix).await;
        assert!(recs.contains_key(&key("k1")), "k1's failure stands");
    });
}

/// A new failure is checked for moot under the conditions now, not the
/// plan's (second review L2). The input is back at what the consumer holds,
/// but the context moved since: the key is still owed, so its failure stands.
#[test]
fn a_new_failure_stands_if_the_key_is_owed_under_the_conditions_now() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = scripted();
        let c = w.counters;
        w.input(&ix, &[("k1", 5)]).await;
        let p = w.planned(&ix, None, 10, Mode::Incremental).await;
        let ev = w.commit_of(&ix, &p, &[], &[], c).await.unwrap();
        w.apply(ev);
        w.input(&ix, &[("k1", 6)]).await;
        let list = Mode::Keys(vec![key("k1")]);
        let p = w.planned(&ix, None, 10, list).await;
        w.input(&ix, &[("k1", 5)]).await;
        w.now = Arc::new(Conditions {
            context: b"w2".as_slice().into(),
            ..(*w.now).clone()
        });
        let ev = w
            .commit_of(&ix, &p, &[("k1", Kind::Failed)], &[], c)
            .await
            .unwrap();
        w.apply(ev);
        let recs = w.stored(&ix).await;
        assert!(recs.contains_key(&key("k1")), "k1's failure stands");
    });
}

/// And a new failure is moot under the conditions now even if the input did
/// not move since the plan (second review L2): the patterns stopped taking
/// the key, which the consumer does not hold.
#[test]
fn a_new_failure_is_moot_if_the_conditions_now_take_what_is_held() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = scripted();
        let c = w.counters;
        w.input(&ix, &[("k1", 5)]).await;
        let p = w.planned(&ix, None, 10, Mode::Incremental).await;
        assert_eq!(taken(&p), [key("k1")]);
        w.now = only(&w.now.clone(), "k2");
        let ev = w
            .commit_of(&ix, &p, &[("k1", Kind::Failed)], &[], c)
            .await
            .unwrap();
        w.apply(ev);
        assert!(w.stored(&ix).await.is_empty());
    });
}

/// A commit is built only once reverts were looked for through the input's
/// head (second review L3). k1 failed on 6; the input goes to 7, where a
/// batch takes it, back to the 5 the consumer holds, which makes the failure
/// moot, and to 6 again, where the worker loads it and fails. Built before
/// the catch-up, the commit would count the moot failure as this one's first
/// try.
#[test]
fn a_commit_waits_for_the_catch_up() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = scripted();
        let c = w.counters;
        k1_fails_on_6(&mut w, &ix).await;
        w.input(&ix, &[("k1", 7)]).await;
        w.catch_up(&ix).await;
        let p = w.planned(&ix, None, 10, Mode::Incremental).await;
        assert_eq!(taken(&p), [key("k1")]);
        w.input(&ix, &[("k1", 5)]).await;
        w.input(&ix, &[("k1", 6)]).await;
        let fail = [("k1", Kind::Failed)];
        let err = w.commit_of(&ix, &p, &fail, &[("k1", 6)], c).await;
        let err = err.unwrap_err();
        assert!(err.contains("catch up first"), "{err}");
        w.catch_up(&ix).await;
        assert!(w.stored(&ix).await.is_empty(), "the failure went moot");
        // The batch read a record that is gone: it is planned again.
        let again = w.commit_of(&ix, &p, &fail, &[("k1", 6)], c).await;
        assert!(again.is_err());
        let p = w.planned(&ix, None, 10, Mode::Incremental).await;
        let ev = w.commit_of(&ix, &p, &fail, &[], c).await.unwrap();
        w.apply(ev);
        assert_eq!(w.stored(&ix).await[&key("k1")].stored.tries, 1);
    });
}

/// A full batch that takes a key whose failure is moot reads its record once
/// (second review L4): counted twice in the plan's digest of the records it
/// read, the commit found them moved and refused. k2, held at 5, failed on
/// 6; a full batch is planned while the input is back at 5, and commits
/// after it left again, the edge caught up under patterns that do not take
/// k2 meanwhile.
#[test]
fn a_full_batch_commits_a_key_whose_failure_is_moot() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = scripted();
        let c = w.counters;
        let first = w.now.clone();
        w.input(&ix, &[("k1", 5), ("k2", 5)]).await;
        let p = w.planned(&ix, None, 10, Mode::Incremental).await;
        let ev = w.commit_of(&ix, &p, &[], &[], c).await.unwrap();
        w.apply(ev);
        w.input(&ix, &[("k2", 6)]).await;
        let p = w.planned(&ix, None, 10, Mode::Incremental).await;
        let ev = w
            .commit_of(&ix, &p, &[("k2", Kind::Failed)], &[], c)
            .await
            .unwrap();
        w.apply(ev);
        w.now = only(&first, "k1");
        w.catch_up(&ix).await;
        w.input(&ix, &[("k2", 5)]).await;
        w.catch_up(&ix).await;
        w.now = first.clone();
        let p = w.planned(&ix, None, 10, Mode::All).await;
        assert_eq!(taken(&p), [key("k1"), key("k2")]);
        assert_eq!(p.inputs[0].moot, [key("k2")]);
        let rec = &w.stored(&ix).await[&key("k2")];
        assert_eq!(p.inputs[0].records, record_digest(b"k2", rec));
        w.now = only(&first, "k1");
        w.input(&ix, &[("k2", 6)]).await;
        w.catch_up(&ix).await;
        w.now = first;
        w.catch_up(&ix).await;
        assert!(w.stored(&ix).await.contains_key(&key("k2")));
        let ev = w.commit_of(&ix, &p, &[], &[], c).await.unwrap();
        w.apply(ev);
        assert!(w.stored(&ix).await.is_empty(), "k2 succeeded at 5");
    });
}

/// A pass's start built on another view's list is refused, where it once
/// panicked (second review L5): the view saw no pass, and the list is the one
/// the pass started since wrote, with the same keys.
#[test]
fn a_pass_start_on_another_views_list_is_refused() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = scripted();
        let c = w.counters;
        w.input(&ix, &[("k1", 5)]).await;
        let p = w.planned(&ix, None, 10, Mode::Incremental).await;
        let ev = w
            .commit_of(&ix, &p, &[("k1", Kind::Transient)], &[], c)
            .await
            .unwrap();
        w.apply(ev);
        w.clock += 100.0;
        let v = w.t.outcome_view(EDGE.as_bytes()).unwrap();
        let start = w.start_of(&ix).await.expect("k1's retry is due");
        w.apply(start);
        let list = w.t.retry_view(EDGE.as_bytes()).unwrap().pass_list;
        let at = w.clock as u64;
        let err = due::start(&OneIndex(&ix), &v, &list, at, c, "p/again")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("pass list"), "{err}");
    });
}

/// A first batch built on an edge no B6 part has applied to yet is refused
/// once the edge starts over (round 5): every life is given once.
#[test]
fn a_first_batch_built_before_the_edge_started_over_is_refused() {
    let rt = runtime();
    for restart in restarts() {
        let ix = index(&rt);
        rt.block_on(async {
            let mut w = scripted();
            let c = w.counters;
            w.input(&ix, &[("k1", 5), ("k2", 5)]).await;
            let first = w.planned(&ix, None, 1, Mode::Incremental).await;
            let commit = w
                .commit_of(&ix, &first, &[("k1", Kind::Failed)], &[], c)
                .await
                .unwrap();
            for part in restart {
                w.apply(vec![part]);
            }
            let before = w.t.snapshot();
            assert!(w.t.apply(&Event::of(commit).encode()).is_err());
            assert_eq!(w.t.snapshot(), before);
        });
    }
}

/// A change of patterns alone does not make a try fresh (round 5): a key
/// both patterns take keeps its tries, and its timeouts run out.
#[test]
fn a_change_of_patterns_alone_keeps_a_keys_tries() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = scripted();
        w.input(&ix, &[("k1", 5)]).await;
        let first = w.planned(&ix, None, 10, Mode::Incremental).await;
        let c = w.counters;
        let ev = w
            .commit_of(&ix, &first, &[("k1", Kind::TimedOut)], &[], c)
            .await
            .unwrap();
        w.apply(ev);
        for n in 2..=3 {
            if n == 3 {
                w.now = only(&w.now, "k1");
                if let Some(parts) = w.caught(&ix).await {
                    w.apply(parts);
                }
            }
            w.clock += 100_000.0;
            let start = w.start_of(&ix).await.expect("k1's timeout is due");
            w.apply(start);
            let step = w.retry_plan(&ix, 10).await;
            assert_eq!(taken(&step), [key("k1")]);
            let c = w.counters;
            let ev = w
                .commit_of(&ix, &step, &[("k1", Kind::TimedOut)], &[], c)
                .await
                .unwrap();
            w.apply(ev);
            let r = &w.stored(&ix).await[&key("k1")];
            assert_eq!(r.stored.tries, n, "k1's tries");
        }
        let r = &w.stored(&ix).await[&key("k1")];
        assert_eq!(r.stored.outcome, FAILED, "k1's timeouts ran out");
    });
}

/// A pass by counters leaves a failed key whose input changed since it
/// failed; the input then returns to what the key failed on, so the key waits
/// for its retry again. The catch-up that sees it return lowers what the last
/// pass covers, and the deploy's retry still comes (second review, round 2).
#[test]
fn a_key_a_pass_left_while_its_input_had_changed_gets_its_deploy_retry() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = scripted();
        let c1 = w.counters;
        w.input(&ix, &[("k1", 5)]).await;
        let first = w.planned(&ix, None, 10, Mode::Incremental).await;
        let ev = w
            .commit_of(&ix, &first, &[("k1", Kind::Failed)], &[], c1)
            .await
            .unwrap();
        w.apply(ev);
        while let Some(parts) = w.start_of(&ix).await {
            w.apply(parts);
            let step = w.retry_plan(&ix, 10).await;
            let ev = w.commit_of(&ix, &step, &[], &[], c1).await.unwrap();
            w.apply(ev);
        }
        w.counters.deploy = 2;
        let c2 = w.counters;
        w.input(&ix, &[("k1", 6)]).await;
        if let Some(parts) = w.caught(&ix).await {
            w.apply(parts);
        }
        let start = w.start_of(&ix).await.expect("the deploy starts a pass");
        w.apply(start);
        let step = w.retry_plan(&ix, 10).await;
        assert!(
            taken(&step).is_empty(),
            "k1's input changed: it is not a retry's"
        );
        let ev = w.commit_of(&ix, &step, &[], &[], c2).await.unwrap();
        w.apply(ev);
        // It changes again, then returns; one catch-up sees both.
        w.input(&ix, &[("k1", 7)]).await;
        w.input(&ix, &[("k1", 5)]).await;
        if let Some(parts) = w.caught(&ix).await {
            w.apply(parts);
        }
        let rec = w.stored(&ix).await[&key("k1")].clone();
        assert!(eligible(&rec.stored, w.clock, &w.counters));
        assert!(taken(&w.planned(&ix, None, 10, Mode::Incremental).await).is_empty());
        let start = w.start_of(&ix).await;
        w.apply(start.expect("k1 waits for the deploy's retry"));
        assert_eq!(taken(&w.retry_plan(&ix, 10).await), [key("k1")]);
    });
}

/// A retry plan names the life its pass runs in (review, round 7). Pass
/// numbers start over with the edge's records, so a retry plan held across a
/// restart would otherwise meet a pass of the same number, with the same
/// points and no records read, and end it before it retried anything.
#[test]
fn a_retry_batch_of_a_pass_in_an_earlier_life_is_refused() {
    let rt = runtime();
    let mut met = 0;
    for restart in restarts() {
        let ix = index(&rt);
        rt.block_on(async {
            let mut w = scripted();
            let k1 = Mode::Keys(vec![key("k1")]);
            let c1 = w.counters;
            w.input(&ix, &[("k1", 5)]).await;
            let first = w.planned(&ix, None, 10, k1.clone()).await;
            let ev = w
                .commit_of(&ix, &first, &[("k1", Kind::Failed)], &[], c1)
                .await
                .unwrap();
            w.apply(ev);
            w.counters.deploy = 2;
            w.input(&ix, &[("k1", 6)]).await;
            if let Some(parts) = w.caught(&ix).await {
                w.apply(parts);
            }
            let start = w.start_of(&ix).await.expect("the deploy starts a pass");
            w.apply(start);
            let old = w.retry_plan(&ix, 10).await;
            assert!(taken(&old).is_empty());
            for part in restart {
                w.apply(vec![part]);
            }
            let c2 = w.counters;
            let p = w.planned(&ix, None, 10, k1.clone()).await;
            let ev = w
                .commit_of(&ix, &p, &[("k1", Kind::Failed)], &[], c2)
                .await
                .unwrap();
            w.apply(ev);
            w.counters.deploy = 3;
            let start = w.start_of(&ix).await.expect("the deploy starts a pass");
            w.apply(start);
            if old.pass.as_ref().unwrap().n == w.t.per_key[EDGE.as_bytes()].pass.as_ref().unwrap().n
            {
                met += 1;
            }
            let c3 = w.counters;
            if let Ok(ev) = w.commit_of(&ix, &old, &[], &[], c3).await {
                assert!(w.t.apply(&Event::of(ev).encode()).is_err());
            }
            assert!(w.t.per_key[EDGE.as_bytes()].pass.is_some());
            assert_eq!(taken(&w.retry_plan(&ix, 10).await), [key("k1")]);
        });
    }
    assert!(met > 0, "no restart gave the new pass the old one's number");
}

/// A key B3 holds back keeps its record as it is (D205): it was loaded at
/// another version than planned while the input is mid full run, so the
/// failure the worker reports for it is not recorded, and its point holds
/// what the consumer held.
#[test]
fn a_held_back_key_keeps_its_record() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = scripted();
        let c = w.counters;
        w.input(&ix, &[("k1", 5), ("k2", 5)]).await;
        let first = w.planned(&ix, None, 10, Mode::Incremental).await;
        let ev = w
            .commit_of(&ix, &first, &[("k1", Kind::Transient)], &[], c)
            .await
            .unwrap();
        w.apply(ev);
        let before = w.stored(&ix).await[&key("k1")].clone();
        w.input(&ix, &[("k1", 6)]).await;
        let p = w.planned(&ix, None, 10, Mode::Incremental).await;
        assert_eq!(taken(&p), [key("k1")]);
        let changes = [(key("k1"), Ver::Int(7)), (key("k2"), Ver::Int(5))].into();
        assert!(w.log.commit(&ix, &mut w.t, changes, true, false).await);
        if let Some(parts) = w.caught(&ix).await {
            w.apply(parts);
        }
        let ev = w
            .commit_of(&ix, &p, &[("k1", Kind::Transient)], &[("k1", 7)], c)
            .await
            .unwrap();
        w.apply(ev);
        assert_eq!(w.stored(&ix).await[&key("k1")], before);
        let edge = &w.t.edges[EDGE.as_bytes()];
        let pt = edge.progress.keyed().unwrap().point(b"k1").unwrap();
        assert_eq!(pt.reason, solera_tracking::progress::Reason::Unsettled);
    });
}

/// A retry batch whose only key B3 holds back (D205) still moves its pass
/// on, and the key's record stays as it is, so the pass ends covering a
/// deploy the key was not retried under. The key gets that retry all the
/// same: it was held back because its input moved after the plan, so either
/// the input returns to what the key failed on and the catch-up lowers what
/// the last pass covers, or it stays changed and the key comes with its
/// change, tried under the deploy now.
#[test]
fn a_retry_batch_whose_only_key_is_held_back_still_gets_its_deploy_retry() {
    let rt = runtime();
    for returns in [true, false] {
        let ix = index(&rt);
        rt.block_on(async {
            let mut w = scripted();
            let c1 = w.counters;
            w.input(&ix, &[("k1", 5)]).await;
            let first = w.planned(&ix, None, 10, Mode::Incremental).await;
            let ev = w
                .commit_of(&ix, &first, &[("k1", Kind::Failed)], &[], c1)
                .await
                .unwrap();
            w.apply(ev);
            while let Some(parts) = w.start_of(&ix).await {
                w.apply(parts);
                let step = w.retry_plan(&ix, 10).await;
                let ev = w.commit_of(&ix, &step, &[], &[], c1).await.unwrap();
                w.apply(ev);
            }
            w.counters.deploy = 2;
            let c2 = w.counters;
            let start = w.start_of(&ix).await.expect("the deploy starts a pass");
            w.apply(start);
            let step = w.retry_plan(&ix, 10).await;
            assert_eq!(taken(&step), [key("k1")]);
            // The input starts a full run under the retry batch: the worker
            // loads k1 at another version than planned.
            let changes = [(key("k1"), Ver::Int(7))].into();
            assert!(w.log.commit(&ix, &mut w.t, changes, true, false).await);
            if let Some(parts) = w.caught(&ix).await {
                w.apply(parts);
            }
            let before = w.stored(&ix).await[&key("k1")].clone();
            let ev = w
                .commit_of(&ix, &step, &[("k1", Kind::Failed)], &[("k1", 7)], c2)
                .await
                .unwrap();
            w.apply(ev);
            assert_eq!(w.stored(&ix).await[&key("k1")], before);
            let edge = &w.t.edges[EDGE.as_bytes()];
            let pt = edge.progress.keyed().unwrap().point(b"k1").unwrap();
            assert_eq!(pt.reason, solera_tracking::progress::Reason::Unsettled);
            let k = &w.t.per_key[EDGE.as_bytes()];
            assert!(k.pass.is_none(), "the batch ended its pass");
            assert_eq!(k.last.deploy, 2, "the pass covers the deploy");
            assert_eq!(before.stored.deploy, 1, "k1 was not retried under it");
            assert!(
                w.start_of(&ix).await.is_none(),
                "k1's input changed: no pass is due for it"
            );
            if returns {
                // The input returns to what k1 failed on: it waits for its
                // retry again, and the deploy's pass comes back for it.
                let changes = [(key("k1"), Ver::Int(5))].into();
                assert!(w.log.commit(&ix, &mut w.t, changes, false, true).await);
                if let Some(parts) = w.caught(&ix).await {
                    w.apply(parts);
                }
                assert!(taken(&w.planned(&ix, None, 10, Mode::Incremental).await).is_empty());
                let start = w.start_of(&ix).await;
                w.apply(start.expect("k1 waits for the deploy's retry"));
                assert_eq!(taken(&w.retry_plan(&ix, 10).await), [key("k1")]);
            } else {
                // The input stays changed: k1 comes with its change, and its
                // failure is recorded under the deploy now.
                let p = w.planned(&ix, None, 10, Mode::Incremental).await;
                assert_eq!(taken(&p), [key("k1")]);
                let ev = w
                    .commit_of(&ix, &p, &[("k1", Kind::Failed)], &[], c2)
                    .await
                    .unwrap();
                w.apply(ev);
                let rec = w.stored(&ix).await[&key("k1")].clone();
                assert_eq!((rec.on.clone(), rec.stored.deploy), (Ver::Int(7), 2));
                assert!(!eligible(&rec.stored, w.clock, &w.counters));
                assert!(w.start_of(&ix).await.is_none());
            }
        });
    }
}

/// One keyed input of a consumer with several, and the truth about it.
struct Side {
    edge: Key,
    log: Log,
    holds: BTreeMap<Key, Holding>,
    fails: BTreeMap<Key, Fail>,
}

impl Side {
    fn holding(&self, k: &[u8]) -> Holding {
        self.holds.get(k).cloned().unwrap_or(Holding::Absent)
    }

    fn wanted(&self, k: &[u8], now: &Arc<Conditions>) -> Holding {
        Holding::of(self.log.at(self.log.head(), k).as_version(), now, k)
    }

    fn waiting(&self, k: &[u8], now: &Arc<Conditions>) -> bool {
        self.fails
            .get(k)
            .is_some_and(|f| !owed(&f.tried, &self.wanted(k, now)))
    }

    fn unsettled(&self, planned: u64) -> bool {
        self.log.full_run.is_some()
            || self
                .log
                .clears
                .iter()
                .any(|&c| planned < c && c <= self.log.head())
    }

    fn reverted(&mut self, now: &Arc<Conditions>) {
        let gone: Vec<Key> = self
            .fails
            .keys()
            .filter(|k| !owed(&self.holding(k), &self.wanted(k, now)))
            .cloned()
            .collect();
        for k in gone {
            self.fails.remove(&k);
        }
    }
}

/// A consumer with two keyed inputs, a and b: change batches over both,
/// retry passes on each, and full runs of a.
struct Multi {
    rng: SmallRng,
    t: Tracking,
    sides: Vec<Side>,
    universe: Vec<Key>,
    now: Arc<Conditions>,
    cursor: Option<Key>,
    clock: f64,
    counters: Counters,
    files: u64,
    /// Change batches, retry batches, keys retried, keys held back, retry
    /// batches during a full run of a, keys a retry batch held back.
    stats: [usize; 6],
}

impl Multi {
    fn new(seed: u64) -> Multi {
        let side = |name: &str| Side {
            edge: key(&format!("c/{name}")),
            log: Log {
                name: name.into(),
                states: vec![BTreeMap::new()],
                layers: vec![],
                clears: vec![],
                full_run: None,
            },
            holds: BTreeMap::new(),
            fails: BTreeMap::new(),
        };
        let mut w = Multi {
            rng: SmallRng::seed_from_u64(seed),
            t: Tracking::new(),
            sides: vec![side("a"), side("b")],
            universe: ["k1", "k2", "k3", "k4"].map(key).to_vec(),
            now: Arc::new(Conditions {
                definition: b"d1".as_slice().into(),
                ..Default::default()
            }),
            cursor: None,
            clock: 1_000_000.0,
            counters: Counters {
                deploy: 1,
                forced: [0; 6],
            },
            files: 0,
            stats: [0; 6],
        };
        for name in ["a", "b"] {
            w.apply(vec![Part::EdgeOpen(EdgeOpen {
                edge: key(&format!("c/{name}")),
                input: key(name),
                keyed: true,
            })]);
        }
        w
    }

    fn apply(&mut self, parts: Vec<Part>) {
        self.t.apply(&Event::of(parts).encode()).unwrap();
    }

    fn ctx(&mut self) -> (String, String) {
        self.files += 1;
        (format!("f/{}", self.files), format!("d/{}", self.files))
    }

    fn request(&self, after: Option<Key>, limit: usize) -> Request {
        Request {
            mode: Mode::Incremental,
            reset: false,
            after,
            limit,
            inputs: self
                .sides
                .iter()
                .map(|s| InputRequest {
                    edge: s.edge.clone(),
                    conditions: self.now.clone(),
                    own: None,
                })
                .collect(),
            unkeyed: vec![],
        }
    }

    fn report(&mut self) -> Reported {
        let kinds = [
            Kind::Failed,
            Kind::Rejected,
            Kind::Transient,
            Kind::TimedOut,
            Kind::Canceled,
        ];
        let kind = kinds[self.rng.random_range(0..kinds.len())];
        Reported {
            kind,
            message: format!("{kind:?}").into_bytes().into(),
            retry_after: self
                .rng
                .random_bool(0.3)
                .then(|| self.rng.random_range(0..300) as f64),
            retry_for: Some(self.rng.random_range(0..2000) as f64),
        }
    }

    async fn catch_up(&mut self, ix: &ChangeIndex) {
        for i in 0..self.sides.len() {
            let v = self.t.outcome_view(&self.sides[i].edge).unwrap();
            let (f, d) = self.ctx();
            let ctx = Ctx {
                now: self.clock,
                counters: self.counters,
                retries: 2,
                time_ms: 0,
                producer: b"",
                failures_path: &f,
                due_path: &d,
            };
            if let Some(parts) = oc::catch_up(&OneIndex(ix), &v, &self.now, &ctx)
                .await
                .unwrap()
            {
                self.apply(parts);
            }
        }
    }

    /// Input i commits; a clear starts a full run, which may go on over
    /// later commits.
    async fn upstream(&mut self, ix: &ChangeIndex, i: usize, clear: bool) {
        let mut changes = BTreeMap::new();
        for k in self.universe.clone() {
            if self.rng.random_bool(if clear { 0.6 } else { 0.4 }) {
                let v = if clear {
                    Ver::Int(self.rng.random_range(1..4))
                } else {
                    version(&mut self.rng)
                };
                changes.insert(k, v);
            }
        }
        let ends = self.rng.random_bool(if clear { 0.3 } else { 0.5 });
        let s = &mut self.sides[i];
        if s.log.commit(ix, &mut self.t, changes, clear, ends).await {
            trace(&format!(
                "input {i} commits{}",
                if clear { ", clearing" } else { "" }
            ));
            s.reverted(&self.now);
        }
    }

    async fn start(&mut self, ix: &ChangeIndex, i: usize) -> Option<Vec<Part>> {
        let r = self.t.retry_view(&self.sides[i].edge).unwrap();
        self.files += 1;
        let path = format!("p/{}", self.files);
        due::start(
            &OneIndex(ix),
            &r.outcomes,
            &r.pass_list,
            self.clock as u64,
            self.counters,
            &path,
        )
        .await
        .unwrap()
    }

    fn under_way(&self, i: usize) -> Option<solera_tracking::outcomes::Pass> {
        self.t
            .per_key
            .get(&self.sides[i].edge)
            .and_then(|k| k.pass.clone())
    }

    /// The worker runs a plan's keys, loading each on every input that took
    /// it; the input may commit first. Then the commit, on every input in
    /// one event, and the truth. False when the commit is refused.
    async fn run(&mut self, ix: &ChangeIndex, p: &plan::Plan) -> bool {
        let meanwhile = self
            .rng
            .random_bool(if p.pass.is_some() { 0.6 } else { 0.3 });
        if meanwhile {
            let i = self.rng.random_range(0..self.sides.len());
            let clear = i == 0 && self.rng.random_bool(0.4);
            self.upstream(ix, i, clear).await;
        }
        let keys: BTreeSet<Key> = p
            .inputs
            .iter()
            .flat_map(|ip| ip.keys.iter().map(|c| c.key.clone()))
            .collect();
        let mut failed: BTreeMap<Key, Reported> = BTreeMap::new();
        for k in keys {
            if self.rng.random_bool(0.35) {
                let r = self.report();
                failed.insert(k, r);
            }
        }
        let mut loaded: Vec<Vec<(Key, Ver)>> = vec![];
        for (j, ip) in p.inputs.iter().enumerate() {
            let mut l = vec![];
            for c in &ip.keys {
                if self.rng.random_bool(0.6) {
                    let log = &self.sides[j].log;
                    let v = log.at(log.head(), &c.key);
                    if v != c.version && !v.is_absent() && !c.version.is_absent() {
                        l.push((c.key.clone(), v));
                    }
                }
            }
            loaded.push(l);
        }
        self.commit_batch(ix, p, failed, loaded, meanwhile).await
    }

    /// A batch's commit on every input in one event, and the truth. False
    /// when it is refused, which only an input's commit since the plan
    /// (meanwhile) may cause.
    async fn commit_batch(
        &mut self,
        ix: &ChangeIndex,
        p: &plan::Plan,
        failed: BTreeMap<Key, Reported>,
        loaded: Vec<Vec<(Key, Ver)>>,
        meanwhile: bool,
    ) -> bool {
        let reported = |ip: &plan::InputPlan| -> Vec<(Key, Reported)> {
            ip.keys
                .iter()
                .filter_map(|c| failed.get(&c.key).map(|r| (c.key.clone(), r.clone())))
                .collect()
        };
        let reports: Vec<InputReport> = p
            .inputs
            .iter()
            .zip(&loaded)
            .map(|(ip, l)| InputReport {
                failed: reported(ip).into_iter().map(|(k, _)| k).collect(),
                loaded: l.clone(),
            })
            .collect();
        let mut ops = match self.t.progress_ops(p, &reports, b"", None) {
            Ok(ops) => ops,
            Err(e) => {
                assert!(meanwhile, "progress refuses a batch planned now: {e}");
                return false;
            }
        };
        self.catch_up(ix).await;
        let counters: Vec<Counters> = (0..self.sides.len())
            .map(|j| match (&p.pass, self.under_way(j)) {
                (Some(id), Some(pass)) if id.edge == self.sides[j].edge => pass.counters,
                _ => self.counters,
            })
            .collect();
        let mut parts = vec![];
        for j in 0..self.sides.len() {
            let v = self.t.outcome_view(&self.sides[j].edge).unwrap();
            let (f, d) = self.ctx();
            let ctx = Ctx {
                now: self.clock,
                counters: self.counters,
                retries: 2,
                time_ms: 0,
                producer: b"",
                failures_path: &f,
                due_path: &d,
            };
            let now = self.now.clone();
            match oc::commit(
                &OneIndex(ix),
                &v,
                p,
                j,
                &reported(&p.inputs[j]),
                &loaded[j],
                &now,
                &ctx,
                &mut ops,
            )
            .await
            {
                Ok(mut ps) => parts.append(&mut ps),
                Err(e) => {
                    assert!(
                        meanwhile,
                        "the outcomes of a batch planned now are refused: {e}"
                    );
                    return false;
                }
            }
        }
        let mut all = ops.parts.clone();
        all.append(&mut parts);
        if let Err(e) = self.t.apply(&Event::of(all).encode()) {
            assert!(meanwhile, "a batch planned now is refused: {e}");
            trace(&format!("the batch is refused: {e}"));
            return false;
        }
        // The truth: on an input mid full run, or cleared since the plan, a
        // key loaded at another version than planned, or a key of the batch
        // the input did not take, is held back on every input (D205, the
        // union rule, #36).
        let batch: BTreeSet<&Key> = p
            .inputs
            .iter()
            .flat_map(|ip| ip.keys.iter().map(|c| &c.key))
            .collect();
        let mut union: BTreeSet<Key> = BTreeSet::new();
        for (j, ip) in p.inputs.iter().enumerate() {
            if !self.sides[j].unsettled(ip.head) {
                continue;
            }
            for k in &batch {
                if !ip.keys.iter().any(|c| c.key == **k) {
                    union.insert((*k).clone());
                }
            }
            for c in &ip.keys {
                let moved = loaded[j].iter().any(|(k, v)| {
                    *k == c.key
                        && owed(
                            &Holding::of(v.as_version(), &ip.conditions, k),
                            &Holding::of(c.version.as_version(), &ip.conditions, k),
                        )
                });
                if moved {
                    union.insert(c.key.clone());
                }
            }
        }
        self.stats[3] += union.len();
        if p.pass.is_some() {
            self.stats[5] += union.len();
        }
        for (j, ip) in p.inputs.iter().enumerate() {
            let held: Vec<Key> = ip
                .keys
                .iter()
                .filter(|c| union.contains(&c.key))
                .map(|c| c.key.clone())
                .collect();
            assert_eq!(
                ops.held_back[j], held,
                "input {j} holds back what the model does"
            );
            let s = &mut self.sides[j];
            for c in &ip.keys {
                if union.contains(&c.key) {
                    continue;
                }
                let got = loaded[j]
                    .iter()
                    .find(|(k, _)| *k == c.key)
                    .map_or(&c.version, |(_, v)| v);
                let tried = Holding::of(got.as_version(), &ip.conditions, &c.key);
                match failed.get(&c.key) {
                    Some(_) if !owed(&s.holding(&c.key), &tried) => {
                        s.fails.remove(&c.key);
                    }
                    Some(r) => {
                        let prior = s.fails.get(&c.key);
                        let fresh = prior.is_none_or(|f| {
                            f.tried_ver != *got || !f.tried_cond.same_processing(&ip.conditions)
                        });
                        let stored = transition(
                            prior.map(|f| &f.stored),
                            fresh,
                            r,
                            self.clock,
                            counters[j].deploy,
                            counters[j].forced_at(),
                            2,
                        )
                        .unwrap();
                        s.fails.insert(
                            c.key.clone(),
                            Fail {
                                stored,
                                tried,
                                tried_cond: ip.conditions.clone(),
                                tried_ver: got.clone(),
                                at: ip.head,
                            },
                        );
                    }
                    None => {
                        match tried {
                            Holding::Absent => s.holds.remove(&c.key),
                            h => s.holds.insert(c.key.clone(), h),
                        };
                        s.fails.remove(&c.key);
                    }
                }
            }
            s.reverted(&self.now);
        }
        true
    }

    async fn change_batch(&mut self, ix: &ChangeIndex) {
        self.catch_up(ix).await;
        let limit = self.rng.random_range(1..4);
        let req = self.request(self.cursor.clone(), limit);
        let view = self.t.view(&req).unwrap();
        let p = plan::plan(&view, &req, &OneIndex(ix), &NoHold)
            .await
            .unwrap();
        self.stats[0] += 1;
        if self.run(ix, &p).await {
            self.cursor = p.range.hi.clone();
        }
    }

    /// A step of input i's retry pass: its start, or its next batch, taken
    /// on both inputs.
    async fn pass_step(&mut self, ix: &ChangeIndex, i: usize) {
        self.catch_up(ix).await;
        let Some(pass) = self.under_way(i) else {
            if let Some(parts) = self.start(ix, i).await {
                trace(&format!("input {i}'s pass starts"));
                self.apply(parts);
            }
            return;
        };
        let edge = self.sides[i].edge.clone();
        let r = self.t.retry_view(&edge).unwrap();
        let req = self.request(None, 1);
        let view = self.t.view(&req).unwrap();
        let limit = self.rng.random_range(1..4);
        let p = due::plan_all(&OneIndex(ix), &r, &view, &req, &NoHold, limit)
            .await
            .unwrap();
        assert_eq!(p.inputs.len(), 2, "a retry takes its keys on every input");
        assert_eq!(p.pass.as_ref().unwrap().edge, edge);
        let retried: Vec<Key> = p.inputs[i].keys.iter().map(|c| c.key.clone()).collect();
        let s = &self.sides[i];
        for c in &p.inputs[i].keys {
            let f = s.fails.get(&c.key).expect("a retried key fails");
            assert!(eligible(&f.stored, pass.at as f64, &pass.counters));
            assert!(!owed(&f.tried, &s.wanted(&c.key, &self.now)));
            assert!(same(&c.held, &s.holding(&c.key)));
        }
        let o = &self.sides[1 - i];
        let listed: Vec<Key> = retried
            .iter()
            .filter(|k| {
                !matches!(o.holding(k), Holding::Absent)
                    || !matches!(o.wanted(k, &self.now), Holding::Absent)
            })
            .cloned()
            .collect();
        let other: Vec<Key> = p.inputs[1 - i].keys.iter().map(|c| c.key.clone()).collect();
        assert_eq!(
            other, listed,
            "the other input lists the retried keys it has"
        );
        for c in &p.inputs[1 - i].keys {
            assert!(same(&c.held, &o.holding(&c.key)));
            assert_eq!(
                c.class,
                Class::of(&o.holding(&c.key), &o.wanted(&c.key, &self.now))
            );
        }
        self.stats[1] += 1;
        self.stats[2] += retried.len();
        if self.sides[0].log.full_run.is_some() {
            self.stats[4] += 1;
        }
        trace(&format!("input {i}'s pass retries {retried:?}"));
        self.run(ix, &p).await;
    }

    async fn check(&mut self, ix: &ChangeIndex) {
        // The engine catches up before it plans or counts.
        self.catch_up(ix).await;
        let ks: Vec<&[u8]> = self.universe.iter().map(|k| k.as_ref()).collect();
        for s in &self.sides {
            let keyed = self.t.edges[&s.edge].progress.keyed().unwrap().clone();
            let fid = output_id(FAILURES, &s.edge);
            let fo = self.t.outputs.get(&fid).cloned().unwrap_or_default();
            let recs = records(&OneIndex(ix), &fid, &fo, &ks).await.unwrap();
            let pk = self.t.per_key.get(&s.edge).cloned().unwrap_or_default();
            for k in &self.universe {
                let decoded = match recs.get(k) {
                    Some(r) => r.held.holding(&pk.conds).unwrap(),
                    None => match keyed.point(k) {
                        Some(p) => p.holding.clone(),
                        None => match keyed.value_of(k) {
                            Value::Empty => Holding::Absent,
                            Value::Held => Holding::Held,
                            Value::At(p, c) => Holding::of(s.log.at(*p, k).as_version(), c, k),
                        },
                    },
                };
                assert!(
                    same(&decoded, &s.holding(k)),
                    "{:?} {k:?} decodes to {decoded:?}, the consumer holds {:?}",
                    s.edge,
                    s.holding(k)
                );
                match s.fails.get(k) {
                    Some(f) => {
                        let r = recs.get(k).expect("a failing key has a record");
                        assert_eq!(r.stored, f.stored, "{:?} {k:?}'s outcome", s.edge);
                    }
                    None => assert!(!recs.contains_key(k), "{:?} {k:?} has a record", s.edge),
                }
            }
        }
        // A plan from the start takes, on each input, exactly its due keys.
        let req = self.request(None, 1000);
        let view = self.t.view(&req).unwrap();
        let p = plan::plan(&view, &req, &OneIndex(ix), &NoHold)
            .await
            .unwrap();
        for (s, ip) in self.sides.iter().zip(&p.inputs) {
            let taken: Vec<Key> = ip.keys.iter().map(|c| c.key.clone()).collect();
            let due: Vec<Key> = self
                .universe
                .iter()
                .filter(|k| {
                    owed(&s.holding(k), &s.wanted(k, &self.now)) && !s.waiting(k, &self.now)
                })
                .cloned()
                .collect();
            assert_eq!(taken, due, "{:?} plans {taken:?}, due {due:?}", s.edge);
        }
        let back = Tracking::restore(&self.t.snapshot()).unwrap();
        assert_eq!(back, self.t);
        // A failing key waiting for a retry it is eligible for gets a pass.
        for i in 0..self.sides.len() {
            if self.under_way(i).is_some() {
                continue;
            }
            let s = &self.sides[i];
            let waiting = s.fails.iter().any(|(k, f)| {
                eligible(&f.stored, self.clock, &self.counters) && s.waiting(k, &self.now)
            });
            if waiting {
                assert!(
                    self.start(ix, i).await.is_some(),
                    "input {i} waits for a retry and no pass starts"
                );
            }
        }
    }
}

async fn multi_history(seed: u64, ix: &ChangeIndex) -> [usize; 6] {
    let mut w = Multi::new(seed);
    for step in 0..60 {
        let op = w.rng.random_range(0..15);
        trace(&format!("step {step} op {op}"));
        match op {
            0 => w.upstream(ix, 0, false).await,
            1 => w.upstream(ix, 1, false).await,
            2 => w.upstream(ix, 0, true).await,
            3..=5 => w.change_batch(ix).await,
            6..=11 => {
                let i = w.rng.random_range(0..2);
                w.pass_step(ix, i).await;
            }
            12 => w.clock += w.rng.random_range(0..600) as f64,
            13 => w.counters.deploy += 1,
            _ => w.catch_up(ix).await,
        }
        w.check(ix).await;
    }
    w.stats
}

/// A consumer with two keyed inputs: a retry of a key one input failed on
/// takes it on both, so the worker reports what it loaded on each, and a
/// load mid full run of a is held back on both (D205).
#[test]
fn multi_input_histories() {
    let seeds: u64 = std::env::var("MULTI_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);
    let only: Option<u64> = std::env::var("MULTI_SEED")
        .ok()
        .and_then(|s| s.parse().ok());
    let rt = runtime();
    let mut total = [0usize; 6];
    for seed in (0..seeds).filter(|s| only.is_none_or(|o| o == *s)) {
        let ix = index(&rt);
        let s = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(multi_history(seed, &ix))
        }));
        match s {
            Ok(s) => {
                for i in 0..6 {
                    total[i] += s[i];
                }
            }
            Err(e) => {
                eprintln!("seed {seed} failed");
                std::panic::resume_unwind(e);
            }
        }
    }
    eprintln!(
        "{seeds} multi-input histories: change batches {}, retry batches {} taking {} keys ({} during a full run of a), keys held back {} ({} by retry batches)",
        total[0], total[1], total[2], total[4], total[3], total[5]
    );
}

/// A retry of k1 on b, taken on a too, is held back because a's full run
/// moved what the worker loaded there; b's own input did not change. The
/// key was not retried, so b's pass covers its deploy no more, and the next
/// pass takes it again (§9's deploy_min, D205).
#[test]
fn a_retry_held_back_by_another_inputs_full_run_comes_again() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = Multi::new(0);
        for i in 0..2 {
            let changes = [(key("k1"), Ver::Int(1))].into();
            assert!(
                w.sides[i]
                    .log
                    .commit(&ix, &mut w.t, changes, false, true)
                    .await
            );
        }
        let req = w.request(None, 10);
        let view = w.t.view(&req).unwrap();
        let p = plan::plan(&view, &req, &OneIndex(&ix), &NoHold)
            .await
            .unwrap();
        let boom = Reported {
            kind: Kind::Failed,
            message: b"boom".as_slice().into(),
            retry_after: None,
            retry_for: None,
        };
        let failed: BTreeMap<Key, Reported> = [(key("k1"), boom)].into();
        assert!(
            w.commit_batch(&ix, &p, failed, vec![vec![], vec![]], false)
                .await
        );
        w.counters.deploy = 2;
        let start = w.start(&ix, 1).await.expect("the deploy starts b's pass");
        w.apply(start);
        let r = w.t.retry_view(&w.sides[1].edge).unwrap();
        let req = w.request(None, 1);
        let view = w.t.view(&req).unwrap();
        let p = due::plan_all(&OneIndex(&ix), &r, &view, &req, &NoHold, 10)
            .await
            .unwrap();
        assert_eq!(p.inputs[0].keys.len(), 1, "a lists k1");
        assert_eq!(p.inputs[1].keys.len(), 1, "b retries k1");
        // a starts a full run; the worker loads a's k1 at its new version.
        let changes = [(key("k1"), Ver::Int(2))].into();
        assert!(
            w.sides[0]
                .log
                .commit(&ix, &mut w.t, changes, true, false)
                .await
        );
        let boom = Reported {
            kind: Kind::Failed,
            message: b"boom".as_slice().into(),
            retry_after: None,
            retry_for: None,
        };
        let failed: BTreeMap<Key, Reported> = [(key("k1"), boom)].into();
        let loaded = vec![vec![(key("k1"), Ver::Int(2))], vec![]];
        assert!(w.commit_batch(&ix, &p, failed, loaded, true).await);
        assert_eq!(w.stats[3], 1, "k1 is held back");
        assert!(w.under_way(1).is_none(), "the batch ended b's pass");
        assert!(
            w.start(&ix, 1).await.is_some(),
            "b's k1 still waits for its deploy retry"
        );
    });
}

/// A failure on b at v1, then b's head moves to v2 and a deploy's pass on b
/// finds nothing to retry. A retry on a takes k1 on b at v2 and is held
/// back by a's full run: b's record is of v1, so b's k1 is a change batch's
/// and b's pass still covers the deploy.
#[test]
fn a_held_back_key_planned_at_another_version_than_its_record_lowers_nothing() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = Multi::new(0);
        for i in 0..2 {
            let changes = [(key("k1"), Ver::Int(1))].into();
            assert!(
                w.sides[i]
                    .log
                    .commit(&ix, &mut w.t, changes, false, true)
                    .await
            );
        }
        let req = w.request(None, 10);
        let view = w.t.view(&req).unwrap();
        let p = plan::plan(&view, &req, &OneIndex(&ix), &NoHold)
            .await
            .unwrap();
        let failed = [(key("k1"), boom())].into();
        assert!(
            w.commit_batch(&ix, &p, failed, vec![vec![], vec![]], false)
                .await
        );
        let changes = [(key("k1"), Ver::Int(2))].into();
        assert!(
            w.sides[1]
                .log
                .commit(&ix, &mut w.t, changes, false, true)
                .await
        );
        w.catch_up(&ix).await;
        w.counters.deploy = 2;
        for i in [1, 0] {
            let start = w.start(&ix, i).await.expect("the deploy starts a pass");
            w.apply(start);
            let req = w.request(None, 10);
            let view = w.t.view(&req).unwrap();
            let r = w.t.retry_view(&w.sides[i].edge).unwrap();
            let p = due::plan_all(&OneIndex(&ix), &r, &view, &req, &NoHold, 10)
                .await
                .unwrap();
            if i == 1 {
                assert!(p.inputs[1].keys.is_empty(), "b's k1 changed");
                assert!(
                    w.commit_batch(&ix, &p, BTreeMap::new(), vec![vec![], vec![]], false)
                        .await
                );
                continue;
            }
            assert_eq!(p.inputs[1].keys[0].version, Ver::Int(2));
            let changes = [(key("k1"), Ver::Int(2))].into();
            assert!(
                w.sides[0]
                    .log
                    .commit(&ix, &mut w.t, changes, true, false)
                    .await
            );
            let loaded = vec![vec![(key("k1"), Ver::Int(2))], vec![]];
            assert!(w.commit_batch(&ix, &p, BTreeMap::new(), loaded, true).await);
        }
        assert_eq!(w.stats[3], 1, "k1 is held back");
        assert_eq!(w.t.per_key[&w.sides[1].edge].last.deploy, 2);
        assert!(w.start(&ix, 1).await.is_none(), "no pass is due on b");
    });
}

/// A retry plan keeps the consumer's unkeyed inputs, with one keyed input
/// or several, so the batch consumes them as any batch does.
#[test]
fn a_retry_plan_keeps_the_unkeyed_inputs() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = Multi::new(0);
        w.apply(vec![Part::EdgeOpen(EdgeOpen {
            edge: key("c/u"),
            input: key("u"),
            keyed: false,
        })]);
        let mut u = Log {
            name: "u".into(),
            states: vec![BTreeMap::new()],
            layers: vec![],
            clears: vec![],
            full_run: None,
        };
        let changes = [(key("v"), Ver::Int(1))].into();
        assert!(u.commit(&ix, &mut w.t, changes, false, true).await);
        for i in 0..2 {
            let changes = [(key("k1"), Ver::Int(1))].into();
            assert!(
                w.sides[i]
                    .log
                    .commit(&ix, &mut w.t, changes, false, true)
                    .await
            );
        }
        let req = w.request(None, 10);
        let view = w.t.view(&req).unwrap();
        let p = plan::plan(&view, &req, &OneIndex(&ix), &NoHold)
            .await
            .unwrap();
        let failed = [(key("k1"), boom())].into();
        assert!(
            w.commit_batch(&ix, &p, failed, vec![vec![], vec![]], false)
                .await
        );
        w.counters.deploy = 2;
        let start = w.start(&ix, 1).await.expect("the deploy starts b's pass");
        w.apply(start);
        for keyed in [vec![1], vec![0, 1]] {
            let mut req = w.request(None, 10);
            req.inputs = keyed.iter().map(|&j| req.inputs[j].clone()).collect();
            req.unkeyed = vec![key("c/u")];
            let view = w.t.view(&req).unwrap();
            let r = w.t.retry_view(&w.sides[1].edge).unwrap();
            let p = due::plan_all(&OneIndex(&ix), &r, &view, &req, &NoHold, 10)
                .await
                .unwrap();
            let want = plan::plan(&view, &req, &OneIndex(&ix), &NoHold)
                .await
                .unwrap();
            assert!(want.unkeyed[0].owed());
            assert_eq!(p.unkeyed, want.unkeyed, "{} keyed inputs", keyed.len());
            assert_eq!(p.inputs.last().unwrap().keys.len(), 1, "b retries k1");
        }
    });
}

/// The same with b's conditions moved instead of its version: after a
/// rebuild on b, a's retry takes b's k1 at v1 under the new conditions, and
/// b's record of v1 under the old ones is a change batch's.
#[test]
fn a_held_back_key_planned_under_other_conditions_than_its_record_lowers_nothing() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = Multi::new(0);
        for i in 0..2 {
            let changes = [(key("k1"), Ver::Int(1))].into();
            assert!(
                w.sides[i]
                    .log
                    .commit(&ix, &mut w.t, changes, false, true)
                    .await
            );
        }
        let req = w.request(None, 10);
        let view = w.t.view(&req).unwrap();
        let p = plan::plan(&view, &req, &OneIndex(&ix), &NoHold)
            .await
            .unwrap();
        let failed = [(key("k1"), boom())].into();
        assert!(
            w.commit_batch(&ix, &p, failed, vec![vec![], vec![]], false)
                .await
        );
        w.catch_up(&ix).await;
        let life = w.t.per_key[&w.sides[1].edge].life;
        w.apply(vec![Part::Rebuild(Rebuild {
            edge: w.sides[1].edge.clone(),
            to: 1,
            life,
        })]);
        let nows = [
            w.now.clone(),
            Arc::new(Conditions {
                rebuild: 1,
                ..(*w.now).clone()
            }),
        ];
        catch_up_under(&mut w, &ix, &nows).await;
        w.counters.deploy = 2;
        let mut req = w.request(None, 10);
        req.inputs[1].conditions = nows[1].clone();
        for i in [1, 0] {
            let start = w.start(&ix, i).await.expect("the deploy starts a pass");
            w.apply(start);
            let view = w.t.view(&req).unwrap();
            let r = w.t.retry_view(&w.sides[i].edge).unwrap();
            let p = due::plan_all(&OneIndex(&ix), &r, &view, &req, &NoHold, 10)
                .await
                .unwrap();
            if i == 1 {
                assert!(p.inputs[1].keys.is_empty(), "b's k1 is owed by the rebuild");
                let reports = [InputReport::default(), InputReport::default()];
                commit_under(&mut w, &ix, &p, &nows, &reports).await;
                continue;
            }
            assert_eq!(p.inputs[1].keys[0].version, Ver::Int(1));
            let changes = [(key("k1"), Ver::Int(2))].into();
            assert!(
                w.sides[0]
                    .log
                    .commit(&ix, &mut w.t, changes, true, false)
                    .await
            );
            let reports = [
                InputReport {
                    failed: vec![],
                    loaded: vec![(key("k1"), Ver::Int(2))],
                },
                InputReport::default(),
            ];
            commit_under(&mut w, &ix, &p, &nows, &reports).await;
        }
        assert_eq!(w.t.per_key[&w.sides[1].edge].last.deploy, 2);
        assert!(w.start(&ix, 1).await.is_none(), "no pass is due on b");
    });
}

async fn catch_up_under(w: &mut Multi, ix: &ChangeIndex, nows: &[Arc<Conditions>]) {
    for j in 0..w.sides.len() {
        let v = w.t.outcome_view(&w.sides[j].edge).unwrap();
        let (f, d) = w.ctx();
        let ctx = Ctx {
            now: w.clock,
            counters: w.counters,
            retries: 2,
            time_ms: 0,
            producer: b"",
            failures_path: &f,
            due_path: &d,
        };
        if let Some(parts) = oc::catch_up(&OneIndex(ix), &v, &nows[j], &ctx)
            .await
            .unwrap()
        {
            w.apply(parts);
        }
    }
}

/// A batch with no failures committed under each input's own conditions.
async fn commit_under(
    w: &mut Multi,
    ix: &ChangeIndex,
    p: &plan::Plan,
    nows: &[Arc<Conditions>],
    reports: &[InputReport],
) {
    let mut ops = w.t.progress_ops(p, reports, b"", None).unwrap();
    catch_up_under(w, ix, nows).await;
    let mut parts = vec![];
    for j in 0..w.sides.len() {
        let v = w.t.outcome_view(&w.sides[j].edge).unwrap();
        let (f, d) = w.ctx();
        let ctx = Ctx {
            now: w.clock,
            counters: w.counters,
            retries: 2,
            time_ms: 0,
            producer: b"",
            failures_path: &f,
            due_path: &d,
        };
        let loaded = &reports[j].loaded;
        let mut ps = oc::commit(
            &OneIndex(ix),
            &v,
            p,
            j,
            &[],
            loaded,
            &nows[j],
            &ctx,
            &mut ops,
        )
        .await
        .unwrap();
        parts.append(&mut ps);
    }
    let mut all = ops.parts;
    all.append(&mut parts);
    w.apply(all);
}

/// The same as a retry held back by another input's full run, with the
/// worker naming b's load of k1 at the version planned: b still read k1 as
/// planned, so the key comes again.
#[test]
fn a_load_named_at_the_planned_version_is_read_as_planned() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut w = Multi::new(0);
        for i in 0..2 {
            let changes = [(key("k1"), Ver::Int(1))].into();
            assert!(
                w.sides[i]
                    .log
                    .commit(&ix, &mut w.t, changes, false, true)
                    .await
            );
        }
        let req = w.request(None, 10);
        let view = w.t.view(&req).unwrap();
        let p = plan::plan(&view, &req, &OneIndex(&ix), &NoHold)
            .await
            .unwrap();
        let failed = [(key("k1"), boom())].into();
        assert!(
            w.commit_batch(&ix, &p, failed, vec![vec![], vec![]], false)
                .await
        );
        w.counters.deploy = 2;
        let start = w.start(&ix, 1).await.expect("the deploy starts b's pass");
        w.apply(start);
        let r = w.t.retry_view(&w.sides[1].edge).unwrap();
        let req = w.request(None, 1);
        let view = w.t.view(&req).unwrap();
        let p = due::plan_all(&OneIndex(&ix), &r, &view, &req, &NoHold, 10)
            .await
            .unwrap();
        let changes = [(key("k1"), Ver::Int(2))].into();
        assert!(
            w.sides[0]
                .log
                .commit(&ix, &mut w.t, changes, true, false)
                .await
        );
        let failed = [(key("k1"), boom())].into();
        let loaded = vec![
            vec![(key("k1"), Ver::Int(2))],
            vec![(key("k1"), Ver::Int(1))],
        ];
        assert!(w.commit_batch(&ix, &p, failed, loaded, true).await);
        assert_eq!(w.stats[3], 1, "k1 is held back");
        assert_eq!(w.t.per_key[&w.sides[1].edge].last.deploy, 1);
        assert!(
            w.start(&ix, 1).await.is_some(),
            "k1 waits for its deploy retry"
        );
    });
}

fn boom() -> Reported {
    Reported {
        kind: Kind::Failed,
        message: b"boom".as_slice().into(),
        retry_after: None,
        retry_for: None,
    }
}
