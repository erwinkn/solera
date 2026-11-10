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
//! - counts by outcome match the records; snapshots round-trip.
//!
//! `OUTCOME_SEEDS` sets how many histories run.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use solera_index::{ChangeIndex, LayerRef, NewCommit, Run, Store, StoreOptions, WriteOptions};
use solera_tracking::codec::Ver;
use solera_tracking::due;
use solera_tracking::event::{Event, Part};
use solera_tracking::outcomes::commit::{self as oc, records, Ctx};
use solera_tracking::outcomes::transition::{
    eligible, transition, Counters, Kind, Reported, Stored,
};
use solera_tracking::outcomes::{cond_id, output_id, Rebuild, FAILURES};
use solera_tracking::plan::{self, InputRequest, Mode, NoHold, OneIndex, Request};
use solera_tracking::progress::{
    owed, Class, Conditions, EdgeOpen, Holding, InputReport, Key, Pat, Patterns, Reset, Value,
};
use solera_tracking::{Output, Tracking};

fn key(s: &str) -> Key {
    s.as_bytes().into()
}

const EDGE: &str = "checks/items";

struct Log {
    states: Vec<BTreeMap<Key, Ver>>,
    layers: Vec<LayerRef>,
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
    ) -> bool {
        let before = self.states.last().unwrap().clone();
        let mut after = before.clone();
        let mut run = Run::new();
        let c = self.head() + 1;
        for (k, new) in &changes {
            let old = before.get(k).cloned().unwrap_or_default();
            if *new == old {
                continue;
            }
            run.push_key(k);
            run.push_entry(c, new.as_version(), old.as_version());
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
        };
        let path = format!("items/c{c}");
        self.layers
            .push(ix.write_commit(&path, nc, &run).await.unwrap());
        self.states.push(after);
        t.outputs.insert(
            key("items"),
            Output {
                layers: self.layers.clone(),
                head: c,
                live: self.states.last().unwrap().len() as u64,
            },
        );
        true
    }
}

/// The truth about one failing key.
#[derive(Clone, Debug)]
struct Fail {
    stored: Stored,
    tried: Holding,
    tried_cond: u64,
    tried_ver: Ver,
    at: u64,
}

struct World {
    rng: SmallRng,
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
    stats: [usize; 6],
    /// Plans held back to commit late, after the state moved on.
    stale: Vec<plan::Plan>,
    /// Every key a batch runs fails (a bad deploy).
    fail_all: bool,
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

    /// The worker runs a plan's keys and the engine commits them; the truth follows.
    /// None when the commit is refused: the state moved since the plan.
    async fn run(&mut self, ix: &ChangeIndex, p: &plan::Plan, retry: bool) -> Option<Vec<Key>> {
        let ip = &p.inputs[0];
        let mut failed: Vec<(Key, Reported)> = vec![];
        let mut loaded: Vec<(Key, Ver)> = vec![];
        for c in &ip.keys {
            if c.class != Class::Removed && (self.fail_all || self.rng.random_bool(0.35)) {
                let r = self.report();
                failed.push((c.key.clone(), r));
            }
            if self.rng.random_bool(0.15) {
                let v = self.log.at(self.log.head(), &c.key);
                if v != c.version && !v.is_absent() && !c.version.is_absent() {
                    loaded.push((c.key.clone(), v));
                }
            }
        }
        let report = InputReport {
            failed: failed.iter().map(|(k, _)| k.clone()).collect(),
            loaded: loaded.clone(),
        };
        let mut ops = self.t.progress_ops(p, &[report], b"", None).ok()?;
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
        let mut parts = oc::commit(&OneIndex(ix), &v, p, 0, &failed, &loaded, &ctx, &mut ops)
            .await
            .ok()?;
        let mut all = ops.parts.clone();
        all.append(&mut parts);
        self.t.apply(&Event::of(all).encode()).ok()?;
        self.events += 1;
        // The truth.
        let cid = cond_id(&ip.conditions);
        let fails: BTreeMap<Key, Reported> = failed.into_iter().collect();
        let loaded: BTreeMap<Key, Ver> = loaded.into_iter().collect();
        for c in &ip.keys {
            let got = loaded.get(&c.key).unwrap_or(&c.version);
            let tried = Holding::of(got.as_version(), &ip.conditions, &c.key);
            if fails.contains_key(&c.key) && !owed(&self.holding(&c.key), &tried) {
                self.fails.remove(&c.key);
            } else if let Some(r) = fails.get(&c.key) {
                let prior = self.fails.get(&c.key);
                let fresh = prior.is_none_or(|f| f.tried_ver != *got || f.tried_cond != cid);
                let s = transition(
                    prior.map(|f| &f.stored),
                    fresh,
                    r,
                    self.clock,
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
                        tried_cond: cid,
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

    async fn change_batch(&mut self, ix: &ChangeIndex, upstream_meanwhile: bool) {
        let limit = self.rng.random_range(1..4);
        let req = self.request(self.cursor.clone(), limit, Mode::Incremental);
        let view = self.t.view(&req).unwrap();
        let p = plan::plan(&view, &req, &OneIndex(ix), &NoHold)
            .await
            .unwrap();
        if upstream_meanwhile {
            self.upstream(ix).await;
        }
        self.stats[0] += 1;
        self.run(ix, &p, false)
            .await
            .expect("a batch planned now commits");
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
            .and_then(|k| k.pass.as_ref())
            .map(|p| p.n);
        if self.run(ix, &p, p.pass.is_some()).await.is_some() {
            assert!(
                p.pass.is_none() || p.pass == pass,
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
        if self.log.commit(ix, &mut self.t, changes).await {
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
        rng,
        t: Tracking::new(),
        log: Log {
            states: vec![BTreeMap::new()],
            layers: vec![],
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
        stats: [0; 6],
        stale: vec![],
    };
    w.apply(vec![Part::EdgeOpen(EdgeOpen {
        edge: key(EDGE),
        input: key("items"),
        keyed: true,
    })]);
    w
}

async fn history(seed: u64, ix: &ChangeIndex) -> [usize; 6] {
    let mut w = world(seed);
    let trace = std::env::var("OUTCOME_TRACE").is_ok();
    for step in 0..60 {
        let op = w.rng.random_range(0..25);
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
                w.change_batch(ix, meanwhile).await
            }
            10 => w.key_list(ix).await,
            11 => {
                w.now = Arc::new(Conditions {
                    definition: format!("d{}", w.rng.random_range(1..3)).into_bytes().into(),
                    ..(*w.now).clone()
                });
                w.cursor = None;
                w.reverted();
            }
            12 => {
                let to = w.t.rebuild(EDGE.as_bytes()) + 1;
                w.apply(vec![Part::Rebuild(Rebuild {
                    edge: key(EDGE),
                    to,
                })]);
                w.now = Arc::new(Conditions {
                    rebuild: to,
                    ..(*w.now).clone()
                });
                w.cursor = None;
                w.reverted();
            }
            13 => {
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
            18 | 20..=22 => w.pass(ix).await,
            23 => w.hold(ix).await,
            24 => w.late(ix).await,
            19 => {
                if w.rng.random_bool(0.3) {
                    w.apply(vec![Part::Reset(Reset {
                        edge: key(EDGE),
                        held: false,
                    })]);
                    w.holds.clear();
                    w.fails.clear();
                    w.cursor = None;
                } else {
                    w.t = Tracking::restore(&w.t.snapshot()).unwrap();
                }
            }
            _ => unreachable!(),
        }
        w.catch_up(ix).await;
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
    let mut total = [0usize; 6];
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
    eprintln!("{seeds} histories: change batches {}, key lists {}, retry passes {} taking {} keys, failing keys left {}, late commits accepted {}", total[0], total[1], total[2], total[3], total[4], total[5]);
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
        assert!(w.log.commit(&ix, &mut w.t, changes).await);
        w.fail_all = true;
        loop {
            w.change_batch(&ix, false).await;
            if w.cursor.is_none() {
                break;
            }
        }
        let mut start = |w: &mut World| {
            w.counters.forced[2] = w.events + 1;
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
            Ok(mut ops) => oc::commit(&OneIndex(&ix), &v, &p, 0, &[], &[], &ctx, &mut ops).await,
        };
        let err = got.expect_err("an old pass's batch is refused");
        assert!(format!("{err:?}").contains("pass that ended"), "{err:?}");
    });
}
