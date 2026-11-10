//! Random histories, checked after every step against a brute-force model
//! of what the consumer holds (the design's model.py, P4–P6 and §5–§10):
//! progress decodes to what the consumer really holds, and a plan is exactly
//! the first N owed keys after the cursor in key order, with the right
//! classes, versions and held-back keys, in every mode, over one or two
//! keyed inputs, with batches committed several upstream commits after they
//! were planned, loads at other versions, failures, resets and merges.
//! `HISTORY_SEEDS` sets how many histories run.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use solera_index::{
    pick_merge, ChangeIndex, LayerRef, NewCommit, Run, Store, StoreOptions, Version, WriteOptions,
};
use solera_tracking::codec::Ver;
use solera_tracking::event::{Event, Part};
use solera_tracking::plan::{self, Hold, InputRequest, Mode, OneIndex, Request};
use solera_tracking::progress::{
    owed, Class, Conditions, EdgeOpen, Holding, InputReport, Key, KeyRange, Pat, Patterns, Reset,
    Value,
};
use solera_tracking::{Output, Tracking};

fn key(s: &str) -> Key {
    s.as_bytes().into()
}

/// One output's log: its states, and its layers in the index.
struct Log {
    name: Key,
    states: Vec<BTreeMap<Key, Ver>>,
    layers: Vec<LayerRef>,
    files: usize,
}

impl Log {
    fn new(name: &str) -> Log {
        Log {
            name: key(name),
            states: vec![BTreeMap::new()],
            layers: vec![],
            files: 0,
        }
    }

    fn head(&self) -> u64 {
        self.states.len() as u64 - 1
    }

    fn at(&self, c: u64, k: &[u8]) -> Ver {
        self.states[c as usize].get(k).cloned().unwrap_or_default()
    }

    async fn commit(&mut self, ix: &ChangeIndex, t: &mut Tracking, changes: BTreeMap<Key, Ver>) {
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
            return;
        }
        self.files += 1;
        let path = format!("{}/c{}", String::from_utf8_lossy(&self.name), self.files);
        let nc = NewCommit {
            commit: c,
            time_ms: 0,
            producer: b"",
            live_before: before.len() as u64,
        };
        self.layers
            .push(ix.write_commit(&path, nc, &run).await.unwrap());
        self.states.push(after);
        self.sync(t);
    }

    async fn merge(&mut self, ix: &ChangeIndex, t: &mut Tracking, cut: u64) {
        if let Some(r) = pick_merge(&self.layers, cut) {
            self.files += 1;
            let path = format!("{}/m{}", String::from_utf8_lossy(&self.name), self.files);
            let l = ix.merge(&self.layers, r.clone(), cut, &path).await.unwrap();
            self.layers.splice(r, [l]);
            self.sync(t);
        }
    }

    fn sync(&self, t: &mut Tracking) {
        let live = self.states.last().unwrap().len() as u64;
        t.outputs.insert(
            self.name.clone(),
            Output {
                layers: self.layers.clone(),
                head: self.head(),
                live,
            },
        );
    }
}

/// One keyed input of the consumer: its upstream, and what the consumer holds.
struct Input {
    edge: Key,
    log: Log,
    now: Arc<Conditions>,
    holds: BTreeMap<Key, Holding>,
}

impl Input {
    fn holding(&self, k: &[u8]) -> Holding {
        self.holds.get(k).cloned().unwrap_or(Holding::Absent)
    }

    /// What progress decodes a key to, read by brute force.
    fn decode(&self, t: &Tracking, own: &Log, k: &[u8]) -> Holding {
        let keyed = t.edges[&self.edge].progress.keyed().unwrap().clone();
        if let Some(p) = keyed.point(k) {
            return p.holding.clone();
        }
        match keyed.value_of(k) {
            Value::Empty => Holding::Absent,
            Value::Held => {
                if own.at(own.head(), k).is_absent() {
                    Holding::Absent
                } else {
                    Holding::Held
                }
            }
            Value::At(p, c) => Holding::of(self.log.at(*p, k).as_version(), c, k),
        }
    }
}

struct SetHold(BTreeSet<Key>);

impl Hold for SetHold {
    fn held_back(&self, _: usize, key: &[u8]) -> bool {
        self.0.contains(key)
    }
}

fn patterns(rng: &mut SmallRng) -> Patterns {
    let glob = |ks: &[&str]| {
        ks.iter()
            .map(|k| Pat::Glob(k.to_string()))
            .collect::<Vec<_>>()
    };
    match rng.random_range(0..6) {
        0 | 1 => Patterns::default(),
        2 => Patterns::new(glob(&["k1", "k2", "k3"]), vec![]).unwrap(),
        3 => Patterns::new(glob(&["k2", "k4", "k5", "k6"]), vec![]).unwrap(),
        4 => Patterns::new(vec![Pat::Regex("k[15]".into())], vec![]).unwrap(),
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

/// Two holdings that hold the same: the same version under the same
/// context, definition and rebuild counter (patterns only decide what is
/// taken, so a holding carries whichever pattern set it was last written under).
fn same(a: &Holding, b: &Holding) -> bool {
    match (a, b) {
        (Holding::Absent, Holding::Absent) | (Holding::Held, Holding::Held) => true,
        (Holding::Version(..), Holding::Version(..)) => !owed(a, b),
        _ => false,
    }
}

fn apply(t: &mut Tracking, parts: Vec<Part>) {
    let bytes = Event::of(parts).encode();
    t.apply(&bytes).unwrap();
}

/// Brute force: the owed keys of one input after `after`, with their classes,
/// as the plan must find them: (key, class, version at head, held, held back).
fn brute_owed(
    inp: &Input,
    reset: bool,
    all: bool,
    after: Option<&Key>,
    universe: &[Key],
    hold: &SetHold,
) -> Vec<(Key, Class, Ver, Holding, bool)> {
    let h = inp.log.head();
    let mut out = vec![];
    for k in universe {
        if after.is_some_and(|a| k <= a) {
            continue;
        }
        let held = if reset {
            Holding::Absent
        } else {
            inp.holding(k)
        };
        let v = inp.log.at(h, k);
        let wanted = Holding::of(v.as_version(), &inp.now, k);
        let v = if matches!(wanted, Holding::Absent) {
            Ver::Absent
        } else {
            v
        };
        if owed(&held, &wanted) {
            out.push((
                k.clone(),
                Class::of(&held, &wanted),
                v,
                held,
                hold.0.contains(k),
            ));
        } else if all && !matches!(wanted, Holding::Absent) {
            out.push((k.clone(), Class::Unchanged, v, held, false));
        }
    }
    out
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
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

#[test]
fn random_histories() {
    let seeds: u64 = std::env::var("HISTORY_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(400);
    let rt = runtime();
    let mut steps = 0;
    let mut planned = [0usize; 4];
    for seed in 0..seeds {
        let mut rng = SmallRng::seed_from_u64(seed);
        let ix = index(&rt);
        steps += rt.block_on(history(&mut rng, &ix, &mut planned));
    }
    eprintln!("{steps} steps over {seeds} histories; batches planned by mode (incremental, all, keys, full): {planned:?}");
}

async fn history(rng: &mut SmallRng, ix: &ChangeIndex, planned: &mut [usize; 4]) -> usize {
    let mut universe: Vec<Key> = (1..=rng.random_range(3..8))
        .map(|i| key(&format!("k{i}")))
        .collect();
    if rng.random_bool(0.5) {
        universe.push(key("x1"));
    }
    let mut t = Tracking::new();
    let mut own = Log::new("own");
    let n_inputs = if rng.random_bool(0.3) { 2 } else { 1 };
    let mut inputs: Vec<Input> = (0..n_inputs)
        .map(|i| Input {
            edge: key(&format!("consumer/in{i}")),
            log: Log::new(&format!("up{i}")),
            now: Arc::new(Conditions {
                patterns: patterns(rng),
                ..Default::default()
            }),
            holds: BTreeMap::new(),
        })
        .collect();
    for inp in &inputs {
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: inp.edge.clone(),
                input: inp.log.name.clone(),
                keyed: true,
            })],
        );
    }
    let mut cursor: Option<Key> = None;
    let n = rng.random_range(5..45);
    for _ in 0..n {
        let r: f64 = rng.random();
        if r < 0.03 {
            // A held reset (an upstream deleted and declared again).
            let mut changes = BTreeMap::new();
            for k in &universe {
                changes.insert(
                    k.clone(),
                    if rng.random_bool(0.5) {
                        Ver::Int(own.head() + 1)
                    } else {
                        Ver::Absent
                    },
                );
            }
            own.commit(ix, &mut t, changes).await;
            apply(
                &mut t,
                vec![Part::Reset(Reset {
                    edge: inputs[0].edge.clone(),
                    held: true,
                })],
            );
            inputs[0].holds = universe
                .iter()
                .filter(|k| !own.at(own.head(), k).is_absent())
                .map(|k| (k.clone(), Holding::Held))
                .collect();
            cursor = None;
        } else if r < 0.10 {
            let inp = &mut inputs[rng.random_range(0..n_inputs)];
            let mut c = (*inp.now).clone();
            match rng.random_range(0..4) {
                0 => c.patterns = patterns(rng),
                1 => c.context = vec![rng.random_range(0..2u8)].into(),
                2 => c.definition = vec![rng.random_range(0..2u8)].into(),
                _ => c.rebuild += 1,
            }
            inp.now = Arc::new(c);
        } else if r < 0.5 {
            let inp = &mut inputs[rng.random_range(0..n_inputs)];
            upstream_commit(rng, ix, &mut t, inp, &universe).await;
            if rng.random_bool(0.3) {
                let cut = min_piece_commit(&t, inp);
                inp.log.merge(ix, &mut t, cut).await;
            }
        } else {
            batch(
                rng,
                ix,
                &mut t,
                &mut inputs,
                &own,
                &universe,
                &mut cursor,
                planned,
            )
            .await;
        }
        check(ix, &t, &inputs, &own, &universe).await;
    }
    n
}

async fn upstream_commit(
    rng: &mut SmallRng,
    ix: &ChangeIndex,
    t: &mut Tracking,
    inp: &mut Input,
    universe: &[Key],
) {
    let mut changes = BTreeMap::new();
    for k in universe {
        if rng.random_bool(0.4) {
            changes.insert(k.clone(), version(rng));
        }
    }
    inp.log.commit(ix, t, changes).await;
}

fn min_piece_commit(t: &Tracking, inp: &Input) -> u64 {
    let keyed = t.edges[&inp.edge].progress.keyed().unwrap().clone();
    keyed
        .pieces(&KeyRange::all())
        .iter()
        .filter_map(|p| match p.value {
            Value::At(c, _) => Some(c),
            _ => None,
        })
        .min()
        .unwrap_or(inp.log.head())
}

#[allow(clippy::too_many_arguments)]
async fn batch(
    rng: &mut SmallRng,
    ix: &ChangeIndex,
    t: &mut Tracking,
    inputs: &mut [Input],
    own: &Log,
    universe: &[Key],
    cursor: &mut Option<Key>,
    planned: &mut [usize; 4],
) {
    let which = rng.random_range(0..10);
    let (mode, reset) = match which {
        0 => (Mode::All, false),
        1 => {
            let keys: Vec<Key> = universe
                .iter()
                .filter(|_| rng.random_bool(0.5))
                .cloned()
                .collect();
            (Mode::Keys(keys), rng.random_bool(0.2))
        }
        2 => (Mode::Incremental, true),
        _ => (Mode::Incremental, false),
    };
    planned[match (&mode, reset) {
        (Mode::Incremental, false) => 0,
        (Mode::All, _) => 1,
        (Mode::Keys(_), _) => 2,
        _ => 3,
    }] += 1;
    if reset || matches!(mode, Mode::Keys(_)) || rng.random_bool(0.15) {
        *cursor = None;
    }
    let hold = SetHold(if matches!(mode, Mode::Keys(_)) {
        BTreeSet::new()
    } else {
        universe
            .iter()
            .filter(|_| rng.random_bool(0.15))
            .cloned()
            .collect()
    });
    let limit = rng.random_range(1..4);
    let req = Request {
        mode: mode.clone(),
        reset,
        after: cursor.clone(),
        limit,
        inputs: inputs
            .iter()
            .enumerate()
            .map(|(i, inp)| InputRequest {
                edge: inp.edge.clone(),
                conditions: inp.now.clone(),
                own: (i == 0).then(|| own.name.clone()),
            })
            .collect(),
        unkeyed: vec![],
    };
    let view = t.view(&req).unwrap();
    let p = plan::plan(&view, &req, &OneIndex(ix), &hold).await.unwrap();
    assert_eq!(
        plan::Plan::decode(&p.encode()).unwrap(),
        p,
        "a plan round-trips"
    );

    // The plan is exactly what brute force owes.
    let all = mode == Mode::All;
    let expected: Vec<Vec<(Key, Class, Ver, Holding, bool)>> = match &mode {
        Mode::Keys(keys) => {
            let batch: Vec<&Key> = keys.iter().take(limit).collect();
            let hi = (keys.len() > limit).then(|| keys[limit - 1].clone());
            assert_eq!(p.range, KeyRange { lo: None, hi });
            inputs
                .iter()
                .map(|inp| {
                    batch
                        .iter()
                        .filter_map(|k| {
                            let held = if reset {
                                Holding::Absent
                            } else {
                                inp.holding(k)
                            };
                            let v = inp.log.at(inp.log.head(), k);
                            let wanted = Holding::of(v.as_version(), &inp.now, k);
                            let v = if matches!(wanted, Holding::Absent) {
                                Ver::Absent
                            } else {
                                v
                            };
                            (!matches!((&held, &wanted), (Holding::Absent, Holding::Absent)))
                                .then(|| ((*k).clone(), Class::of(&held, &wanted), v, held, false))
                        })
                        .collect()
                })
                .collect()
        }
        _ => {
            let owed: Vec<_> = inputs
                .iter()
                .map(|inp| brute_owed(inp, reset, all, cursor.as_ref(), universe, &hold))
                .collect();
            let mut keys: Vec<&Key> = owed
                .iter()
                .flatten()
                .filter(|o| !o.4)
                .map(|o| &o.0)
                .collect();
            keys.sort();
            keys.dedup();
            let hi = (keys.len() > limit).then(|| keys[limit - 1].clone());
            assert_eq!(
                p.range,
                KeyRange {
                    lo: cursor.clone(),
                    hi: hi.clone()
                },
                "the batch's range"
            );
            owed.into_iter()
                .map(|o| {
                    o.into_iter()
                        .filter(|x| hi.as_ref().is_none_or(|h| x.0 <= *h))
                        .collect()
                })
                .collect()
        }
    };
    for (ip, exp) in p.inputs.iter().zip(&expected) {
        let got: Vec<_> = ip
            .keys
            .iter()
            .map(|c| (c.key.clone(), c.class, c.version.clone()))
            .collect();
        let want: Vec<_> = exp
            .iter()
            .filter(|x| !x.4)
            .map(|x| (x.0.clone(), x.1, x.2.clone()))
            .collect();
        assert_eq!(got, want, "the plan's keys");
        let want_held = exp.iter().filter(|x| !x.4).map(|x| &x.3);
        assert!(
            ip.keys.iter().zip(want_held).all(|(c, h)| same(&c.held, h)),
            "what each key held"
        );
        let got: Vec<_> = ip
            .held_back
            .iter()
            .map(|h| (h.key.clone(), h.version.clone()))
            .collect();
        let want: Vec<_> = exp
            .iter()
            .filter(|x| x.4)
            .map(|x| (x.0.clone(), x.2.clone()))
            .collect();
        assert_eq!(got, want, "the plan's held-back keys");
        let want_held = exp.iter().filter(|x| x.4).map(|x| &x.3);
        assert!(
            ip.held_back
                .iter()
                .zip(want_held)
                .all(|(c, h)| same(&c.held, h)),
            "what each held-back key held"
        );
    }

    // Upstream may move before the batch commits, and conditions may change.
    for _ in 0..rng.random_range(0..3) {
        let i = rng.random_range(0..inputs.len());
        upstream_commit(rng, ix, t, &mut inputs[i], universe).await;
    }

    // The worker: some keys fail, some load at another version.
    let mut reports = vec![];
    for (ip, inp) in p.inputs.iter().zip(inputs.iter_mut()) {
        let mut report = InputReport::default();
        if reset {
            inp.holds.clear();
        }
        for c in &ip.keys {
            if rng.random_bool(0.15) {
                report.failed.push(c.key.clone());
                continue;
            }
            let mut v = c.version.clone();
            if rng.random_bool(0.1) {
                v = inp.log.at(inp.log.head(), &c.key);
                report.loaded.push((c.key.clone(), v.clone()));
            }
            match Holding::of(v.as_version(), &ip.conditions, &c.key) {
                Holding::Absent => inp.holds.remove(&c.key),
                h => inp.holds.insert(c.key.clone(), h),
            };
        }
        reports.push(report);
    }
    let ops = t.progress_ops(&p, &reports, b"run/batch", None).unwrap();
    for (rec, ip) in ops.records.iter().zip(&p.inputs) {
        let back = solera_tracking::record::Record::decode(&rec.encode()).unwrap();
        assert_eq!(&back, rec);
        assert_eq!(rec.counts, ip.counts());
    }
    apply(t, ops.parts);
    if !matches!(mode, Mode::Keys(_)) {
        *cursor = p.range.hi.clone();
    }
}

async fn check(ix: &ChangeIndex, t: &Tracking, inputs: &[Input], own: &Log, universe: &[Key]) {
    for inp in inputs {
        for k in universe {
            let (got, want) = (inp.decode(t, own, k), inp.holding(k));
            assert!(
                same(&got, &want),
                "progress decodes {} to what the consumer holds: {got:?} != {want:?}",
                String::from_utf8_lossy(k)
            );
        }
    }
    assert_eq!(
        &Tracking::restore(&t.snapshot()).unwrap(),
        t,
        "the snapshot round-trips"
    );
    for (i, inp) in inputs.iter().enumerate() {
        let req = Request {
            mode: Mode::Incremental,
            reset: false,
            after: None,
            limit: 1,
            inputs: vec![InputRequest {
                edge: inp.edge.clone(),
                conditions: inp.now.clone(),
                own: (i == 0).then(|| own.name.clone()),
            }],
            unkeyed: vec![],
        };
        let view = t.view(&req).unwrap();
        let brute = brute_owed(inp, false, false, None, universe, &SetHold(BTreeSet::new()));
        let mut counts = [0u64; 4];
        for b in &brute {
            counts[b.1 as usize - 1] += 1;
        }
        assert_eq!(
            plan::owed_count(&view, &req.inputs[0], &OneIndex(ix))
                .await
                .unwrap(),
            counts,
            "owed counts"
        );
        let mut paged = vec![];
        let mut after: Option<Key> = None;
        loop {
            let (keys, next) =
                plan::owed_keys(&view, &req.inputs[0], &OneIndex(ix), after.as_ref(), 2)
                    .await
                    .unwrap();
            paged.extend(keys.into_iter().map(|c| (c.key, c.class)));
            match next {
                Some(n) => after = Some(n),
                None => break,
            }
        }
        assert_eq!(
            paged,
            brute.iter().map(|b| (b.0.clone(), b.1)).collect::<Vec<_>>(),
            "owed keys, paged"
        );
    }
}

#[test]
fn unkeyed_edges_consume_through_the_head_they_planned_at() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut t = Tracking::new();
        let mut log = Log::new("totals");
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: key("report/totals"),
                input: log.name.clone(),
                keyed: false,
            })],
        );
        log.commit(&ix, &mut t, [(key("v"), Ver::Int(1))].into())
            .await;
        let req = Request {
            mode: Mode::Incremental,
            reset: false,
            after: None,
            limit: 10,
            inputs: vec![],
            unkeyed: vec![key("report/totals")],
        };
        let p = plan::plan(&t.view(&req).unwrap(), &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        assert!(p.unkeyed[0].owed());
        log.commit(&ix, &mut t, [(key("v"), Ver::Int(2))].into())
            .await;
        let ops = t.progress_ops(&p, &[], b"", None).unwrap();
        apply(&mut t, ops.parts);
        let p = plan::plan(&t.view(&req).unwrap(), &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        assert_eq!(
            (p.unkeyed[0].through, p.unkeyed[0].head),
            (1, 2),
            "the commit after the plan stays owed"
        );
    });
}

#[test]
fn a_commit_over_progress_that_moved_is_refused() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut t = Tracking::new();
        let mut log = Log::new("items");
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: key("tally/items"),
                input: log.name.clone(),
                keyed: true,
            })],
        );
        log.commit(
            &ix,
            &mut t,
            [(key("k1"), Ver::Int(1)), (key("k2"), Ver::Int(1))].into(),
        )
        .await;
        let req = Request {
            mode: Mode::Incremental,
            reset: false,
            after: None,
            limit: 10,
            inputs: vec![InputRequest {
                edge: key("tally/items"),
                conditions: Arc::new(Conditions::default()),
                own: None,
            }],
            unkeyed: vec![],
        };
        let p1 = plan::plan(&t.view(&req).unwrap(), &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        let p2 = p1.clone();
        let ops = t
            .progress_ops(&p1, &[InputReport::default()], b"", None)
            .unwrap();
        let stale = Event::of(
            t.progress_ops(&p2, &[InputReport::default()], b"", None)
                .unwrap()
                .parts,
        )
        .encode();
        apply(&mut t, ops.parts);
        assert!(
            t.apply(&stale).is_err(),
            "the second commit planned over the same progress"
        );
        assert!(t
            .progress_ops(&p2, &[InputReport::default()], b"", None)
            .is_err());
    });
}

#[test]
fn the_worked_example_tally() {
    // docs/tracking-design.md §5: run 1 at c1 and c2, run 2 at c4.
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut t = Tracking::new();
        let mut items = Log::new("items");
        let edge = key("tally/items");
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: edge.clone(),
                input: items.name.clone(),
                keyed: true,
            })],
        );
        let cond = Arc::new(Conditions::default());
        let v = |n| Ver::Int(n);
        let run = |t: &mut Tracking, after: Option<Key>| {
            let req = Request {
                mode: Mode::Incremental,
                reset: false,
                after,
                limit: 2,
                inputs: vec![InputRequest {
                    edge: edge.clone(),
                    conditions: cond.clone(),
                    own: None,
                }],
                unkeyed: vec![],
            };
            let view = t.view(&req).unwrap();
            (req, view)
        };
        items
            .commit(
                &ix,
                &mut t,
                [(key("k1"), v(1)), (key("k2"), v(1)), (key("k3"), v(1))].into(),
            )
            .await;
        let (req, view) = run(&mut t, None);
        let p = plan::plan(&view, &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        let taken: Vec<_> = p.inputs[0]
            .keys
            .iter()
            .map(|c| (c.key.clone(), c.class))
            .collect();
        assert_eq!(
            taken,
            vec![(key("k1"), Class::Added), (key("k2"), Class::Added)]
        );
        assert_eq!(p.range.hi, Some(key("k2")));
        let parts = t
            .progress_ops(&p, &[InputReport::default()], b"", None)
            .unwrap()
            .parts;
        apply(&mut t, parts);
        items
            .commit(&ix, &mut t, [(key("k2"), v(2)), (key("k4"), v(2))].into())
            .await;
        let (req, view) = run(&mut t, Some(key("k2")));
        let p = plan::plan(&view, &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        let taken: Vec<_> = p.inputs[0]
            .keys
            .iter()
            .map(|c| (c.key.clone(), c.class))
            .collect();
        assert_eq!(
            taken,
            vec![(key("k3"), Class::Added), (key("k4"), Class::Added)]
        );
        assert_eq!(p.range.hi, None, "nothing beyond: the range runs to +inf");
        let parts = t
            .progress_ops(&p, &[InputReport::default()], b"", None)
            .unwrap()
            .parts;
        apply(&mut t, parts);
        items
            .commit(
                &ix,
                &mut t,
                [(key("k3"), Ver::Absent), (key("k5"), v(3))].into(),
            )
            .await;
        items.commit(&ix, &mut t, [(key("k1"), v(4))].into()).await;
        let (req, view) = run(&mut t, None);
        let p = plan::plan(&view, &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        let taken: Vec<_> = p.inputs[0]
            .keys
            .iter()
            .map(|c| (c.key.clone(), c.class, c.version.clone()))
            .collect();
        assert_eq!(
            taken,
            vec![
                (key("k1"), Class::Updated, v(4)),
                (key("k2"), Class::Updated, v(2))
            ]
        );
        let parts = t
            .progress_ops(&p, &[InputReport::default()], b"", None)
            .unwrap()
            .parts;
        apply(&mut t, parts);
        let keyed = t.edges[&edge].progress.keyed().unwrap().clone();
        let pieces: Vec<_> = keyed
            .pieces(&KeyRange::all())
            .into_iter()
            .map(|p| (p.range.hi, p.value))
            .collect();
        assert_eq!(
            pieces,
            vec![
                (Some(key("k2")), Value::At(4, cond.clone())),
                (None, Value::At(2, cond.clone()))
            ]
        );
        let (req, view) = run(&mut t, None);
        let (owed, _) = plan::owed_keys(&view, &req.inputs[0], &OneIndex(&ix), None, 10)
            .await
            .unwrap();
        let owed: Vec<_> = owed.into_iter().map(|c| (c.key, c.class)).collect();
        assert_eq!(
            owed,
            vec![(key("k3"), Class::Removed), (key("k5"), Class::Added)]
        );
    });
}

#[allow(dead_code)]
fn _version_is_used(v: Version<'_>) -> bool {
    v.is_absent()
}
