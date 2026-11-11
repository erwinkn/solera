//! Random histories, checked after every step against a brute-force model
//! of what the consumer holds (the design's model.py, P4–P6 and §5–§10):
//! progress decodes to what the consumer really holds, and a plan is exactly
//! the first N owed keys after the cursor in key order, with the right
//! classes, versions and held-back keys, in every mode, over one or two
//! keyed inputs, with batches committed several upstream commits after they
//! were planned, loads at other versions, failures, resets, merges, and
//! inputs retired and started again. Every batch's lineage, recomputed from
//! its record then and at every later step, is what the batch received, or
//! says its history was cut or its input retired.
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
use solera_tracking::commit::Declare;
use solera_tracking::event::{Event, Part};
use solera_tracking::lineage::{self, Answer, Received};
use solera_tracking::plan::{self, Hold, InputRequest, Mode, OneIndex, Request};
use solera_tracking::progress::{BatchProgress, EdgeClose, EdgeRename};
use solera_tracking::progress::{
    Class, Conditions, EdgeOpen, Holding, InputReport, Key, KeyRange, Pat, Patterns, Reason, Reset,
    Value,
};
use solera_tracking::record::Record;
use solera_tracking::retain::Retire;
use solera_tracking::Tracking;

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
            clear: false,
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
        t.set_output(
            self.name.clone(),
            self.layers.clone(),
            self.head(),
            live,
            None,
        );
    }
}

/// One keyed input of the consumer: its upstream, and what the consumer holds.
struct Input {
    edge: Key,
    log: Log,
    /// Bumped each time the input is retired and started again.
    gen: u32,
    /// The life of generation `gen`, given when the edge first named it.
    life: u64,
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
            Value::At(p, c) => bf_holding(&self.log.at(*p, k), c, k),
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

/// §5's owed table, written out here so the oracle shares no rule with the
/// code under test: a version is held the same only under the same context,
/// definition and rebuild counter.
fn bf_owed(h: &Holding, w: &Holding) -> bool {
    match (h, w) {
        (Holding::Absent, Holding::Absent) => false,
        (Holding::Version(a, ca), Holding::Version(b, cb)) => {
            a != b
                || ca.context != cb.context
                || ca.definition != cb.definition
                || ca.rebuild != cb.rebuild
        }
        _ => true,
    }
}

fn bf_class(h: &Holding, w: &Holding) -> Class {
    match (h, w) {
        _ if !bf_owed(h, w) => Class::Unchanged,
        (Holding::Absent, _) => Class::Added,
        (_, Holding::Absent) => Class::Removed,
        _ => Class::Updated,
    }
}

/// What processing `v` under `c` leaves, from the patterns' sources
/// (globs as `solera.patterns.glob_regex` reads them; regexes in full).
fn bf_holding(v: &Ver, c: &Arc<Conditions>, k: &[u8]) -> Holding {
    let matches = |p: &Pat| {
        let re = match p {
            Pat::Glob(g) => glob_regex(g),
            Pat::Regex(r) => r.clone(),
        };
        regex::bytes::Regex::new(&format!("(?s)^(?:{re})$"))
            .unwrap()
            .is_match(k)
    };
    let taken = (c.patterns.include().is_empty() || c.patterns.include().iter().any(matches))
        && !c.patterns.exclude().iter().any(matches);
    if v.is_absent() || !taken {
        Holding::Absent
    } else {
        Holding::Version(v.clone(), c.clone())
    }
}

fn glob_regex(g: &str) -> String {
    let (mut out, mut i) = (String::new(), 0);
    while i < g.len() {
        let rest = &g[i..];
        let (piece, n) = if rest.starts_with("**/") {
            ("(?:.*/)?".to_string(), 3)
        } else if rest.starts_with("**") {
            (".*".to_string(), 2)
        } else if rest.starts_with('*') {
            ("[^/]*".to_string(), 1)
        } else if rest.starts_with('?') {
            ("[^/]".to_string(), 1)
        } else {
            let ch = rest.chars().next().unwrap();
            (regex::escape(&ch.to_string()), ch.len_utf8())
        };
        out.push_str(&piece);
        i += n;
    }
    out
}

/// Two holdings that hold the same: the same version under the same
/// context, definition and rebuild counter (patterns only decide what is
/// taken, so a holding carries whichever pattern set it was last written under).
fn same(a: &Holding, b: &Holding) -> bool {
    match (a, b) {
        (Holding::Absent, Holding::Absent) | (Holding::Held, Holding::Held) => true,
        (Holding::Version(..), Holding::Version(..)) => !bf_owed(a, b),
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
        let wanted = bf_holding(&v, &inp.now, k);
        let v = if matches!(wanted, Holding::Absent) {
            Ver::Absent
        } else {
            v
        };
        if bf_owed(&held, &wanted) {
            out.push((
                k.clone(),
                bf_class(&held, &wanted),
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

fn commit_part(t: &Tracking, output: &str, layer: &LayerRef, live: u64) -> Part {
    Part::OutputCommit(solera_tracking::commit::OutputCommit {
        output: key(output),
        life: t.life(output.as_bytes()),
        layer: layer.clone(),
        live,
        ends_full_run: false,
    })
}

fn commit_to(t: &mut Tracking, output: &str, layer: &LayerRef, live: u64) {
    let p = commit_part(t, output, layer, live);
    apply(t, vec![p]);
}

fn held_plan_request() -> Request {
    Request {
        mode: Mode::Incremental,
        reset: false,
        after: None,
        limit: 10,
        inputs: vec![InputRequest {
            edge: key("e"),
            conditions: Arc::default(),
            own: Some(key("own")),
        }],
        unkeyed: vec![],
    }
}

/// A batch planned before its input's first commit can load what that
/// commit wrote (Sol, round 6): its plan names the life the edge's opening
/// gave the input, which that commit keeps, and its record stores it, so
/// once the input is retired its lineage is retired, never a version of
/// another life.
#[test]
fn a_batch_planned_before_its_input_started_names_the_life_it_loaded() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut t = Tracking::new();
        let mut own = Log::new("own");
        own.commit(&ix, &mut t, [(key("k"), Ver::Int(90))].into())
            .await;
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: key("e"),
                input: key("items"),
                keyed: true,
            })],
        );
        apply(
            &mut t,
            vec![Part::Reset(Reset {
                edge: key("e"),
                held: true,
            })],
        );
        let req = held_plan_request();
        let p = plan::plan(&t.view(&req).unwrap(), &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        assert_eq!(
            (p.inputs[0].head, p.inputs[0].life, p.inputs[0].keys.len()),
            (0, t.life(b"items"), 1)
        );
        assert!(p.inputs[0].life > 0, "the edge named its input");
        let mut items = Log::new("items");
        let mut staging = Tracking::new();
        items
            .commit(&ix, &mut staging, [(key("k"), Ver::Int(10))].into())
            .await;
        commit_to(&mut t, "items", &items.layers[0], 1);
        let report = InputReport {
            failed: vec![],
            loaded: vec![(key("k"), Ver::Int(10))],
        };
        let ops = t.progress_ops(&p, &[report], b"run/batch", None).unwrap();
        apply(&mut t, ops.parts);
        let rec = &ops.records[0];
        assert_eq!(rec.life, t.outputs[b"items".as_slice()].life);
        let found = lineage::input(&t.logs(rec, Some(b"own")), rec, &OneIndex(&ix), 0)
            .await
            .unwrap();
        assert!(
            matches!(&found, Answer::Found(rs) if rs[0].loaded == Some(Ver::Int(10))),
            "{found:?}"
        );
        apply(&mut t, vec![Part::EdgeClose(EdgeClose { edge: key("e") })]);
        apply(
            &mut t,
            vec![Part::Retire(Retire {
                output: key("items"),
            })],
        );
        let retired = Answer::Retired {
            output: key("items"),
        };
        let mut next = Log {
            files: items.files,
            ..Log::new("items")
        };
        next.commit(&ix, &mut staging, [(key("k"), Ver::Int(10))].into())
            .await;
        for gone in [false, true] {
            if gone {
                commit_to(&mut t, "items", &next.layers[0], 1);
            }
            let logs = t.logs(rec, Some(b"own"));
            assert_eq!(
                lineage::input(&logs, rec, &OneIndex(&ix), 1).await.unwrap(),
                retired
            );
            assert_eq!(
                lineage::received_key(&logs, rec, b"k", &OneIndex(&ix), 1)
                    .await
                    .unwrap(),
                Answer::Retired {
                    output: key("items")
                }
            );
        }
    });
}

/// A plan made in one life of its input does not commit in the next.
#[test]
fn a_plan_of_an_earlier_life_of_its_input_does_not_commit() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut t = Tracking::new();
        let mut items = Log::new("items");
        items
            .commit(&ix, &mut t, [(key("k"), Ver::Int(1))].into())
            .await;
        let open = Part::EdgeOpen(EdgeOpen {
            edge: key("e"),
            input: key("items"),
            keyed: true,
        });
        apply(&mut t, vec![open.clone()]);
        let req = Request {
            inputs: vec![InputRequest {
                own: None,
                ..held_plan_request().inputs[0].clone()
            }],
            ..held_plan_request()
        };
        let p = plan::plan(&t.view(&req).unwrap(), &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        apply(&mut t, vec![Part::EdgeClose(EdgeClose { edge: key("e") })]);
        apply(
            &mut t,
            vec![Part::Retire(Retire {
                output: key("items"),
            })],
        );
        let mut next = Log {
            files: items.files,
            ..Log::new("items")
        };
        next.commit(&ix, &mut t, [(key("k"), Ver::Int(1))].into())
            .await;
        apply(&mut t, vec![open]);
        let e = t
            .progress_ops(&p, &[InputReport::default()], b"run/batch", None)
            .unwrap_err();
        assert!(e.to_string().contains(" in life "), "{e}");
    });
}

/// A record whose held progress read the own output answers retired once
/// that output is retired and started again.
#[test]
fn a_batch_whose_own_output_was_retired_answers_retired() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut t = Tracking::new();
        let mut own = Log::new("own");
        let mut input = Log::new("items");
        own.commit(&ix, &mut t, [(key("k"), Ver::Int(1))].into())
            .await;
        input
            .commit(&ix, &mut t, [(key("k"), Ver::Int(10))].into())
            .await;
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: key("e"),
                input: key("items"),
                keyed: true,
            })],
        );
        apply(
            &mut t,
            vec![Part::Reset(Reset {
                edge: key("e"),
                held: true,
            })],
        );
        let req = held_plan_request();
        let p = plan::plan(&t.view(&req).unwrap(), &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        let ops = t
            .progress_ops(&p, &[InputReport::default()], b"run/batch", None)
            .unwrap();
        apply(&mut t, ops.parts.clone());
        let rec = &ops.records[0];
        apply(&mut t, vec![Part::Retire(Retire { output: key("own") })]);
        let mut next = Log {
            files: own.files,
            ..Log::new("own")
        };
        next.commit(&ix, &mut t, [(key("k"), Ver::Int(2))].into())
            .await;
        let logs = t.logs(rec, Some(b"own"));
        assert_eq!(
            lineage::input(&logs, rec, &OneIndex(&ix), 0).await.unwrap(),
            Answer::Retired { output: key("own") }
        );
        assert_eq!(
            lineage::received_key(&logs, rec, b"k", &OneIndex(&ix), 0)
                .await
                .unwrap(),
            Answer::Retired { output: key("own") }
        );
    });
}

/// An output's life is given when the output is first named, from one
/// counter for every output, kept across commits and a snapshot, and given
/// anew once it was retired; a cleanup step names it.
#[test]
fn an_output_life_follows_its_starts() {
    use solera_tracking::retain::{Cleaned, Track};
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut staging = Tracking::new();
        let mut log = Log::new("staging");
        log.commit(&ix, &mut staging, [(key("k"), Ver::Int(1))].into())
            .await;
        log.commit(&ix, &mut staging, [(key("k"), Ver::Int(2))].into())
            .await;
        let commit = |t: &mut Tracking, output: Key, i: usize| {
            let p = Part::OutputCommit(solera_tracking::commit::OutputCommit {
                life: t.life(&output),
                output,
                layer: log.layers[i].clone(),
                live: 1,
                ends_full_run: false,
            });
            apply(t, vec![p]);
        };
        let mut t = Tracking::new();
        let life = |t: &Tracking, id: &[u8]| {
            t.outputs
                .get(id)
                .unwrap_or_else(|| panic!("no output {:?}", String::from_utf8_lossy(id)))
                .life
        };
        apply(
            &mut t,
            vec![Part::Track(Track {
                output: key("items"),
                at: 0,
            })],
        );
        commit(&mut t, key("items"), 0);
        let first = life(&t, b"items");
        assert!(first > 0);
        commit(&mut t, key("items"), 1);
        assert_eq!(life(&t, b"items"), first);
        let stale = Cleaned {
            output: key("items"),
            life: first,
            from: 0,
            to: 2,
        };
        stale.check(&t).unwrap();
        commit(&mut t, key("other"), 0);
        let other = life(&t, b"other");
        assert!(other > first);
        t = Tracking::restore(&t.snapshot()).unwrap();
        assert_eq!((life(&t, b"items"), life(&t, b"other")), (first, other));
        apply(
            &mut t,
            vec![Part::Retire(Retire {
                output: key("items"),
            })],
        );
        apply(
            &mut t,
            vec![Part::Track(Track {
                output: key("items"),
                at: 0,
            })],
        );
        commit(&mut t, key("items"), 0);
        commit(&mut t, key("items"), 1);
        assert!(life(&t, b"items") > other);
        assert!(stale.check(&t).is_err());
        Cleaned {
            life: life(&t, b"items"),
            ..stale
        }
        .check(&t)
        .unwrap();
    });
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
    let mut lives = Lives::default();
    apply(
        &mut t,
        vec![Part::Declare(Declare {
            output: own.name.clone(),
        })],
    );
    lives.own_life = lives.fresh(t.life(&own.name));
    let n_inputs = if rng.random_bool(0.3) { 2 } else { 1 };
    let mut inputs: Vec<Input> = (0..n_inputs)
        .map(|i| Input {
            edge: key(&format!("consumer/in{i}")),
            log: Log::new(&format!("up{i}")),
            gen: 0,
            life: 0,
            now: Arc::new(Conditions {
                patterns: patterns(rng),
                ..Default::default()
            }),
            holds: BTreeMap::new(),
        })
        .collect();
    for inp in &mut inputs {
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: inp.edge.clone(),
                input: inp.log.name.clone(),
                keyed: true,
            })],
        );
        inp.life = lives.fresh(t.life(&inp.log.name));
    }
    let mut cursor: Option<Key> = None;
    let mut stored: Vec<Stored> = vec![];
    let n = rng.random_range(5..45);
    for step in 0..n {
        // Sometimes a held reset comes first, so a batch plans at its
        // input's head 0 and loads what the input's first commit wrote.
        let r: f64 = if step == 0 && rng.random_bool(0.3) {
            0.0
        } else {
            rng.random()
        };
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
        } else if r < 0.05 && inputs.iter().all(|i| i.log.head() > 0) {
            let i = rng.random_range(0..n_inputs);
            restart(rng, ix, &mut t, &mut inputs[i], &universe, &mut lives).await;
            cursor = None;
        } else if r < 0.06 {
            restart_own(&mut t, &mut inputs[0], &mut own, &mut lives);
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
            stored.extend(
                batch(
                    rng,
                    ix,
                    &mut t,
                    &mut inputs,
                    &mut own,
                    &mut lives,
                    &universe,
                    &mut cursor,
                    planned,
                )
                .await,
            );
        }
        check(ix, &t, &inputs, &own, &universe).await;
        check_lineage(
            rng,
            ix,
            &t,
            &inputs,
            &own,
            lives.own_gen,
            &universe,
            &stored,
        )
        .await;
    }
    n
}

/// The model of lives: every life the state gave an output, and the own
/// output's generation and life. Distinct generations have distinct lives.
#[derive(Default)]
struct Lives {
    seen: BTreeSet<u64>,
    own_gen: u32,
    own_life: u64,
}

impl Lives {
    fn fresh(&mut self, life: u64) -> u64 {
        assert!(life > 0, "a named output has a life");
        assert!(self.seen.insert(life), "life {life} was given before");
        life
    }
}

/// The input leaves its edge, is retired and starts again in a new life,
/// its commits from 1: by a close and a reopen, or by a retarget away and
/// back where the edge's progress names nothing of the input (else the
/// retarget is refused). The consumer reads it anew.
async fn restart(
    rng: &mut SmallRng,
    ix: &ChangeIndex,
    t: &mut Tracking,
    inp: &mut Input,
    universe: &[Key],
    lives: &mut Lives,
) {
    let rename = |input: Key| {
        Event::of(vec![Part::EdgeRename(EdgeRename {
            from: inp.edge.clone(),
            to: inp.edge.clone(),
            input,
        })])
    };
    let mut away = rng.random_bool(0.5);
    if away {
        let ev = rename(key("parking"));
        match t.check_event(&ev) {
            Ok(()) => {
                t.apply_event(ev).unwrap();
                lives.seen.insert(t.life(b"parking"));
            }
            Err(e) => {
                assert!(e.to_string().contains("reset it first"), "{e}");
                away = false;
            }
        }
    }
    if !away {
        apply(
            t,
            vec![Part::EdgeClose(EdgeClose {
                edge: inp.edge.clone(),
            })],
        );
        inp.holds.clear();
    }
    apply(
        t,
        vec![Part::Retire(Retire {
            output: inp.log.name.clone(),
        })],
    );
    if away {
        t.apply_event(rename(inp.log.name.clone())).unwrap();
    } else {
        apply(
            t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: inp.edge.clone(),
                input: inp.log.name.clone(),
                keyed: true,
            })],
        );
    }
    let name = String::from_utf8_lossy(&inp.log.name).into_owned();
    inp.log = Log {
        files: inp.log.files,
        ..Log::new(&name)
    };
    inp.gen += 1;
    inp.life = lives.fresh(t.life(&inp.log.name));
    upstream_commit(rng, ix, t, inp, universe).await;
}

/// The consumer's own output is retired and declared again, in a new life;
/// the first input, whose held progress read it, is reset.
fn restart_own(t: &mut Tracking, first: &mut Input, own: &mut Log, lives: &mut Lives) {
    for part in [
        Part::Reset(Reset {
            edge: first.edge.clone(),
            held: false,
        }),
        Part::Retire(Retire {
            output: own.name.clone(),
        }),
        Part::Declare(Declare {
            output: own.name.clone(),
        }),
    ] {
        apply(t, vec![part]);
    }
    first.holds.clear();
    *own = Log {
        files: own.files,
        ..Log::new("own")
    };
    lives.own_gen += 1;
    lives.own_life = lives.fresh(t.life(&own.name));
}

/// Starts input `i` again, or the own output where `i` is past the inputs.
#[allow(clippy::too_many_arguments)]
async fn restart_one(
    rng: &mut SmallRng,
    ix: &ChangeIndex,
    t: &mut Tracking,
    inputs: &mut [Input],
    own: &mut Log,
    lives: &mut Lives,
    universe: &[Key],
    i: usize,
) {
    match inputs.get_mut(i) {
        Some(inp) => restart(rng, ix, t, inp, universe, lives).await,
        None => restart_own(t, &mut inputs[0], own, lives),
    }
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
    own: &mut Log,
    lives: &mut Lives,
    universe: &[Key],
    cursor: &mut Option<Key>,
    planned: &mut [usize; 4],
) -> Vec<Stored> {
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
    for (ip, inp) in p.inputs.iter().zip(inputs.iter()) {
        assert_eq!(ip.life, inp.life, "a plan names the life of its input now");
    }
    assert_eq!(
        p.inputs[0].own.as_ref().map(|o| o.life),
        Some(lives.own_life),
        "a plan names the life of its own output now"
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
                            let wanted = bf_holding(&v, &inp.now, k);
                            let v = if matches!(wanted, Holding::Absent) {
                                Ver::Absent
                            } else {
                                v
                            };
                            (!matches!((&held, &wanted), (Holding::Absent, Holding::Absent)))
                                .then(|| ((*k).clone(), bf_class(&held, &wanted), v, held, false))
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

    // Sometimes an input, or the own output, starts a new life before the
    // batch commits: before its commit is built, or between that and the
    // event's apply. Either refuses the commit.
    let restarts = rng
        .random_bool(0.15)
        .then(|| rng.random_range(0..=inputs.len()));
    let built_first = rng.random_bool(0.5);
    if !built_first {
        if let Some(i) = restarts {
            restart_one(rng, ix, t, inputs, own, lives, universe, i).await;
            *cursor = None;
        }
    }
    let before = t.snapshot();
    let holds: Vec<_> = inputs.iter().map(|i| i.holds.clone()).collect();

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
                if rng.random_bool(0.3) {
                    report
                        .loaded
                        .push((c.key.clone(), inp.log.at(inp.log.head(), &c.key)));
                }
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
    let ops = t.progress_ops(&p, &reports, b"run/batch", None);
    if let Some(i) = restarts {
        for (inp, h) in inputs.iter_mut().zip(holds) {
            inp.holds = h;
        }
        let e = if built_first {
            let ops = ops.unwrap();
            restart_one(rng, ix, t, inputs, own, lives, universe, i).await;
            *cursor = None;
            t.check_event(&ops.event()).unwrap_err()
        } else {
            let e = ops.unwrap_err();
            assert!(t.snapshot() == before, "a refused commit changes nothing");
            e
        };
        assert!(e.to_string().contains(" in life "), "{e}");
        return vec![];
    }
    let ops = ops.unwrap();
    for (rec, ip) in ops.records.iter().zip(&p.inputs) {
        let back = Record::decode(&rec.encode()).unwrap();
        assert_eq!(&back, rec);
        assert_eq!(rec.counts, ip.counts());
        assert_eq!(rec.life, ip.life, "a record names the life its batch read");
        assert_eq!(
            (rec.own_head, rec.own_life),
            ip.own
                .as_ref()
                .map_or((None, 0), |o| (Some(o.head), o.life)),
            "a record names the own output's life its batch read"
        );
    }
    apply(t, ops.parts.clone());
    if !matches!(mode, Mode::Keys(_)) {
        *cursor = p.range.hi.clone();
    }

    // What each input received: the plan's keys, each with what the worker
    // did (the version it loaded where it was not the one planned, failed or
    // not) and what the commit did (held it back on every input).
    let mut stored = vec![];
    for (i, ((rec, ip), report)) in ops.records.iter().zip(&p.inputs).zip(&reports).enumerate() {
        let expected = ip
            .keys
            .iter()
            .map(|c| {
                let loaded = report
                    .loaded
                    .iter()
                    .find(|(k, _)| *k == c.key)
                    .map(|(_, v)| v.clone())
                    .filter(|v| *v != c.version);
                let reason = if ops.held_back[i].contains(&c.key) {
                    Some(Reason::Unsettled)
                } else if report.failed.contains(&c.key) {
                    Some(Reason::Failed)
                } else {
                    loaded.is_some().then_some(Reason::Deviation)
                };
                Received {
                    key: c.key.clone(),
                    class: c.class,
                    held: c.held.clone(),
                    planned: c.version.clone(),
                    loaded,
                    reason,
                    deleted: false,
                }
            })
            .collect();
        stored.push(Stored {
            rec: rec.clone(),
            input: i,
            gen: inputs[i].gen,
            life: inputs[i].life,
            own_gen: lives.own_gen,
            expected,
        });
    }
    stored
}

/// A batch's record and what that batch received.
struct Stored {
    rec: Record,
    input: usize,
    /// The input's generation at the batch's plan and commit, and its life.
    gen: u32,
    life: u64,
    /// The own output's generation then.
    own_gen: u32,
    expected: Vec<Received>,
}

/// Whether cleanup, through commit `cleaned`, has released version `r` of `k`,
/// which the log held at `head`: a later commit up to `cleaned` replaced it,
/// and no commit after that one wrote it again.
fn released(log: &Log, k: &[u8], r: &Ver, head: u64, cleaned: u64) -> bool {
    !r.is_absent()
        && (head + 1..=cleaned.min(log.head()))
            .rev()
            .find(|&c| log.at(c - 1, k) == *r && log.at(c, k) != *r)
            .is_some_and(|c| (c + 1..=log.head()).all(|later| log.at(later, k) != *r))
}

fn floor(layers: &[LayerRef]) -> u64 {
    layers.first().and_then(|l| l.floor).unwrap_or(0)
}

/// Every stored batch's lineage, at a random cleanup cursor: what the batch
/// received; "retired" exactly when the batch read a commit of an input that
/// has been retired and started again since; "not retained" exactly when a
/// commit the record names (its head, and without a reset its replaced
/// pieces' commits) is below its log's floor. A record that disagrees with
/// its digest or planner, names another life of an output it read, or whose
/// held progress has no own output to read, is an error or retired. One
/// key's trace agrees with the batch's, and a key's history is its log's.
async fn check_lineage(
    rng: &mut SmallRng,
    ix: &ChangeIndex,
    t: &Tracking,
    inputs: &[Input],
    own: &Log,
    own_gen: u32,
    universe: &[Key],
    stored: &[Stored],
) {
    for s in stored {
        let log = &inputs[s.input].log;
        let logs = t.logs(&s.rec, (s.input == 0).then_some(own.name.as_ref()));
        let cleaned = rng.random_range(0..=log.head());
        let got = lineage::input(&logs, &s.rec, &OneIndex(ix), cleaned)
            .await
            .unwrap();
        if s.gen != inputs[s.input].gen {
            let retired = Answer::Retired {
                output: log.name.clone(),
            };
            assert_eq!(got, retired, "a batch of an earlier life of its input");
            for k in universe {
                let one = lineage::received_key(&logs, &s.rec, k, &OneIndex(ix), cleaned)
                    .await
                    .unwrap();
                if s.expected.iter().any(|r| r.key == *k) {
                    assert!(
                        matches!(&one, Answer::Retired { output } if *output == log.name),
                        "the trace of a key it took: {one:?}"
                    );
                } else {
                    assert!(
                        matches!(one, Answer::Found(None) | Answer::Retired { .. }),
                        "{one:?}"
                    );
                }
            }
            continue;
        }
        let pieces = if s.rec.reset {
            &[][..]
        } else {
            &s.rec.replaced.pieces[..]
        };
        let cut = std::iter::once(s.rec.head)
            .chain(pieces.iter().filter_map(|p| match p.value {
                Value::At(c, _) => Some(c),
                _ => None,
            }))
            .filter(|&c| c < floor(&log.layers))
            .min();
        let own_gone =
            s.input == 0 && s.own_gen != own_gen && pieces.iter().any(|p| p.value == Value::Held);
        let got = match got {
            Answer::Retired { output } if output == own.name => {
                assert!(
                    own_gone,
                    "the own output retired in the life the batch read"
                );
                continue;
            }
            Answer::NotRetained { output, commit } => {
                assert_eq!(output, log.name, "only the input's history is cut");
                assert_eq!(
                    Some(commit),
                    cut,
                    "not retained: the oldest commit the record names, below the floor"
                );
                continue;
            }
            Answer::Found(got) => got,
            Answer::Retired { output } => panic!("{output:?} retired in the life the batch read"),
        };
        assert_eq!(
            cut, None,
            "found although the record names commit {cut:?}, below the floor"
        );
        assert!(!own_gone, "found although its own output was retired");
        for bad in [
            Record {
                digest: s.rec.digest ^ 1,
                ..s.rec.clone()
            },
            Record {
                planner: s.rec.planner + 1,
                ..s.rec.clone()
            },
        ] {
            assert!(lineage::input(&logs, &bad, &OneIndex(ix), cleaned)
                .await
                .is_err());
        }
        assert_eq!(
            s.rec.life, s.life,
            "a record names the one life its batch read"
        );
        let other_life = Record {
            life: s.rec.life + 1,
            ..s.rec.clone()
        };
        let now = t.outputs.get(&log.name[..]).map_or(0, |o| o.life);
        assert_eq!(
            lineage::input(&logs, &other_life, &OneIndex(ix), cleaned)
                .await
                .unwrap(),
            if other_life.life != now {
                Answer::Retired {
                    output: log.name.clone(),
                }
            } else {
                Answer::Found(got.clone())
            },
            "a record of another life of its input"
        );
        if !s.rec.reset && s.rec.replaced.pieces.iter().any(|p| p.value == Value::Held) {
            let none = t.logs(&s.rec, None);
            assert!(lineage::input(&none, &s.rec, &OneIndex(ix), cleaned)
                .await
                .is_err());
            if s.rec.own_head > Some(0) {
                let other_own = Record {
                    own_life: s.rec.own_life + 1,
                    ..s.rec.clone()
                };
                assert_eq!(
                    lineage::input(&logs, &other_own, &OneIndex(ix), cleaned)
                        .await
                        .unwrap(),
                    Answer::Retired {
                        output: own.name.clone(),
                    },
                    "a record of another life of its own output"
                );
            }
        }
        let want: Vec<Received> = s
            .expected
            .iter()
            .map(|r| Received {
                deleted: released(
                    log,
                    &r.key,
                    r.loaded.as_ref().unwrap_or(&r.planned),
                    s.rec.head,
                    cleaned,
                ),
                ..r.clone()
            })
            .collect();
        assert_eq!(got, want, "lineage of a batch over {:?}", s.rec.range);
        for k in universe {
            let one = lineage::received_key(&logs, &s.rec, k, &OneIndex(ix), cleaned)
                .await
                .unwrap();
            assert_eq!(
                one,
                Answer::Found(got.iter().find(|r| r.key == *k).cloned()),
                "the trace of {}",
                String::from_utf8_lossy(k)
            );
        }
    }
    for inp in inputs {
        let log = &inp.log;
        let f = floor(&log.layers);
        for k in universe {
            let h = lineage::history(ix, &log.layers, k).await.unwrap();
            let changes: Vec<(u64, Ver, Ver)> = (f + 1..=log.head())
                .rev()
                .filter(|&c| log.at(c, k) != log.at(c - 1, k))
                .map(|c| (c, log.at(c, k), log.at(c - 1, k)))
                .collect();
            let at_floor = (f > 0).then(|| log.at(f, k)).filter(|v| !v.is_absent());
            assert_eq!(
                (h.floor, h.at_floor, h.changes),
                (f, at_floor, changes),
                "the history of {}",
                String::from_utf8_lossy(k)
            );
        }
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

#[test]
fn a_key_held_back_at_commit_is_recorded_apart_from_one_the_planner_held_back() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut t = Tracking::new();
        let mut log = Log::new("items");
        let edge = key("tally/items");
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: edge.clone(),
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
                edge: edge.clone(),
                conditions: Arc::new(Conditions::default()),
                own: None,
            }],
            unkeyed: vec![],
        };
        let hold = SetHold([key("k2")].into());
        let p = plan::plan(&t.view(&req).unwrap(), &req, &OneIndex(&ix), &hold)
            .await
            .unwrap();
        log.commit(&ix, &mut t, [(key("k1"), Ver::Int(2))].into())
            .await;
        t.outputs.get_mut(&log.name).unwrap().full_run = Some(2);
        let report = InputReport {
            failed: vec![],
            loaded: vec![(key("k1"), Ver::Int(2))],
        };
        let ops = t.progress_ops(&p, &[report], b"", None).unwrap();
        assert_eq!(ops.held_back, vec![vec![key("k1")]]);
        let Part::Batch(b) = &ops.parts[0] else {
            panic!("a batch part");
        };
        let reasons: Vec<_> = b
            .points
            .iter()
            .map(|(k, p)| (k.clone(), p.reason))
            .collect();
        let want = vec![
            (key("k1"), Reason::Unsettled),
            (key("k2"), Reason::HeldBack),
        ];
        assert_eq!(reasons, want);
        let rec: Vec<_> = ops.records[0]
            .marks
            .iter()
            .map(|(k, m)| (k.clone(), m.reason, m.loaded.clone()))
            .collect();
        let want = vec![
            (key("k1"), Reason::Unsettled, Some(Ver::Int(2))),
            (key("k2"), Reason::HeldBack, None),
        ];
        assert_eq!(
            rec, want,
            "the record tells the two apart, with the version loaded"
        );
    });
}

// Round 7 of #49 (Sol R7-1, R7-2; Astra R7-A1, R7-A2), strict xfails until
// ERW-34: a plan made before its input's first commit named life 0, and the
// commit inferred the life its batch loaded from the state now. An output's
// life now starts when it is first named, every plan names it, and the
// commit requires it: each probe is refused, or answers retired.

/// A batch plans at its input's head 0 and loads what the first commit
/// wrote, then the input leaves the edge (path 0: a retarget away, a retire
/// and back; 1: close, retire, reopen with the same held progress) and starts
/// again. The commit is refused. Path 2, a reset of the owner of a per-key
/// record output another edge reads, cannot happen: no edge reads a record
/// output.
async fn r7_load_then_remove_input(ix: &ChangeIndex, path: u8, restart_before_commit: bool) {
    use solera_tracking::outcomes::{output_id, FAILURES};

    let mut t = Tracking::new();
    let mut own = Log::new("own");
    own.commit(ix, &mut t, [(key("k"), Ver::Int(90))].into())
        .await;
    let input = if path == 2 {
        output_id(FAILURES, b"owner")
    } else {
        key("items")
    };
    let name = std::str::from_utf8(&input).unwrap();
    let open = Part::EdgeOpen(EdgeOpen {
        edge: key("e"),
        input: input.clone(),
        keyed: true,
    });
    if path == 2 {
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: key("owner"),
                input: key("source"),
                keyed: true,
            })],
        );
        let e = t.check_event(&Event::of(vec![open])).unwrap_err();
        assert!(e.to_string().contains("no edge reads them"), "{e}");
        return;
    }
    let held = Part::Reset(Reset {
        edge: key("e"),
        held: true,
    });
    apply(&mut t, vec![open.clone()]);
    apply(&mut t, vec![held.clone()]);
    let req = held_plan_request();
    let p = plan::plan(&t.view(&req).unwrap(), &req, &OneIndex(ix), &plan::NoHold)
        .await
        .unwrap();
    let life = t.life(&input);
    assert!(life > 0, "the edge named its input");
    assert_eq!(
        (p.inputs[0].head, p.inputs[0].life, p.inputs[0].keys.len()),
        (0, life, 1)
    );
    let mut items = Log::new("probe-input");
    let mut staging = Tracking::new();
    items
        .commit(ix, &mut staging, [(key("k"), Ver::Int(10))].into())
        .await;
    commit_to(&mut t, name, &items.layers[0], 1);
    assert_eq!(t.life(&input), life, "a commit keeps the life");
    let loaded = InputReport {
        failed: vec![],
        loaded: vec![(key("k"), Ver::Int(10))],
    };
    match path {
        0 => {
            let rename = |to: Key| {
                vec![Part::EdgeRename(EdgeRename {
                    from: key("e"),
                    to: key("e"),
                    input: to,
                })]
            };
            apply(&mut t, rename(key("parking")));
            apply(
                &mut t,
                vec![Part::Retire(Retire {
                    output: input.clone(),
                })],
            );
            apply(&mut t, rename(input.clone()));
        }
        1 => {
            apply(&mut t, vec![Part::EdgeClose(EdgeClose { edge: key("e") })]);
            apply(
                &mut t,
                vec![Part::Retire(Retire {
                    output: input.clone(),
                })],
            );
            apply(&mut t, vec![open]);
            apply(&mut t, vec![held]);
        }
        _ => unreachable!(),
    }
    assert!(
        t.life(&input) > life,
        "the input starts again in a new life"
    );
    if restart_before_commit {
        let mut next = Log {
            files: items.files,
            ..Log::new("probe-input")
        };
        next.commit(ix, &mut staging, [(key("k"), Ver::Int(20))].into())
            .await;
        commit_to(&mut t, name, &next.layers[0], 1);
    }
    let before = t.snapshot();
    let e = t
        .progress_ops(&p, &[loaded], b"r7/batch", None)
        .unwrap_err();
    assert!(e.to_string().contains(" in life "), "{e}");
    assert_eq!(before, t.snapshot());
}

#[test]
fn r7_head_zero_load_survives_retarget_retire_and_return() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(r7_load_then_remove_input(&ix, 0, false));
}

#[test]
fn r7_head_zero_load_survives_close_retire_reopen() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(r7_load_then_remove_input(&ix, 1, true));
}

#[test]
fn r7_record_output_reset_drops_an_active_readers_input() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(r7_load_then_remove_input(&ix, 2, true));
}

/// An unkeyed plan names its input's life too, so a plan made before the
/// input was retired and started again does not commit, and the new input
/// stays owed (Astra, R7-A2: the consumed counter alone cannot tell lives
/// apart, since a reopened edge restarts it at 0 and the output's commits
/// restart at 1).
#[test]
fn r7_preexisting_unkeyed_plan_crosses_input_restart() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut t = Tracking::new();
        let mut input = Log::new("items");
        input
            .commit(&ix, &mut t, [(key("k"), Ver::Int(10))].into())
            .await;
        input
            .commit(&ix, &mut t, [(key("k"), Ver::Int(20))].into())
            .await;
        let open = Part::EdgeOpen(EdgeOpen {
            edge: key("u"),
            input: key("items"),
            keyed: false,
        });
        apply(&mut t, vec![open.clone()]);
        let req = Request {
            inputs: vec![],
            unkeyed: vec![key("u")],
            ..held_plan_request()
        };
        let p = plan::plan(&t.view(&req).unwrap(), &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        assert_eq!(
            (p.unkeyed[0].head, p.unkeyed[0].life, p.unkeyed[0].through),
            (2, t.life(b"items"), 0)
        );
        apply(&mut t, vec![Part::EdgeClose(EdgeClose { edge: key("u") })]);
        apply(
            &mut t,
            vec![Part::Retire(Retire {
                output: key("items"),
            })],
        );
        let mut next = Log {
            files: input.files,
            ..Log::new("items")
        };
        next.commit(&ix, &mut t, [(key("k"), Ver::Int(30))].into())
            .await;
        apply(&mut t, vec![open]);
        let e = t.progress_ops(&p, &[], b"stale", None).unwrap_err();
        assert!(e.to_string().contains(" in life "), "{e}");
        let now = plan::plan(&t.view(&req).unwrap(), &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        assert!(now.unkeyed[0].owed(), "{:?}", now.unkeyed);
    });
}

/// The input's first commit is in the batch's own event: the plan named the
/// life the edge gave the input, the commit keeps it, and the record names
/// it, so retiring the input answers retired (Sol, R7-1).
#[test]
fn r7_a_first_input_commit_in_the_batch_event_names_its_life() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut t = Tracking::new();
        let mut own = Log::new("own");
        own.commit(&ix, &mut t, [(key("k"), Ver::Int(90))].into())
            .await;
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: key("e"),
                input: key("items"),
                keyed: true,
            })],
        );
        apply(
            &mut t,
            vec![Part::Reset(Reset {
                edge: key("e"),
                held: true,
            })],
        );
        let req = held_plan_request();
        let p = plan::plan(&t.view(&req).unwrap(), &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        let life = t.life(b"items");
        assert_eq!(
            (p.inputs[0].head, p.inputs[0].life, p.inputs[0].keys.len()),
            (0, life, 1)
        );
        let mut staging = Tracking::new();
        let mut items = Log::new("items");
        items
            .commit(&ix, &mut staging, [(key("k"), Ver::Int(10))].into())
            .await;
        let report = InputReport {
            failed: vec![],
            loaded: vec![(key("k"), Ver::Int(10))],
        };
        let mut ops = t.progress_ops(&p, &[report], b"run/batch", None).unwrap();
        ops.parts
            .push(commit_part(&t, "items", &items.layers[0], 1));
        apply(&mut t, ops.parts);
        let rec = &ops.records[0];
        assert_eq!((rec.life, t.life(b"items")), (life, life));
        apply(&mut t, vec![Part::EdgeClose(EdgeClose { edge: key("e") })]);
        apply(
            &mut t,
            vec![Part::Retire(Retire {
                output: key("items"),
            })],
        );
        let got = lineage::input(&t.logs(rec, Some(b"own")), rec, &OneIndex(&ix), 0)
            .await
            .unwrap();
        assert_eq!(
            got,
            Answer::Retired {
                output: key("items")
            }
        );
    });
}

// Strict xfails owned by B5 PR 3 (ERW-34's deferrals): each needs the
// binding (native/src/tracking_py.rs, PR #40's file) or the engine to pass
// a life, and must keep failing until it does; then drop its should_panic.

/// A full run's end names no life: one built in an earlier life of its
/// output ends the next life's full run (the binding builds the part at
/// tracking_py.rs:887-896; B5 PR 3 gives it the life it was built on).
#[test]
#[should_panic(expected = "B5 PR 3")]
fn a_full_run_end_of_an_earlier_life_does_not_apply() {
    use solera_tracking::commit::FullRunDone;
    let mut t = Tracking::new();
    t.set_output(key("x"), vec![], 1, 0, Some(1));
    let done = Event::of(vec![Part::FullRunDone(FullRunDone { output: key("x") })]);
    t.check_event(&done).unwrap();
    apply(&mut t, vec![Part::Retire(Retire { output: key("x") })]);
    t.set_output(key("x"), vec![], 1, 0, Some(1));
    assert!(
        t.check_event(&done).is_err(),
        "a full run's end of an earlier life applies (strict xfail, B5 PR 3)"
    );
}

/// A batch's output commit lands in its own output's life only if the plan
/// read that life: a plan with no own output lets the commit land in any
/// (the engine passes output_commit with no own, commit.py:155; B5 PR 3
/// passes own on every plan, then progress_ops refuses this).
#[test]
#[should_panic(expected = "B5 PR 3")]
fn an_output_commit_needs_the_own_output_the_plan_read() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut t = Tracking::new();
        let mut items = Log::new("items");
        items
            .commit(&ix, &mut t, [(key("k"), Ver::Int(1))].into())
            .await;
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: key("e"),
                input: key("items"),
                keyed: true,
            })],
        );
        let req = Request {
            inputs: vec![InputRequest {
                own: None,
                ..held_plan_request().inputs[0].clone()
            }],
            ..held_plan_request()
        };
        let p = plan::plan(&t.view(&req).unwrap(), &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        assert!(
            t.progress_ops(&p, &[InputReport::default()], b"run/batch", Some(1))
                .is_err(),
            "an output commit with no own output's life (strict xfail, B5 PR 3)"
        );
    });
}

/// A batch's parts never name life 0: an output with no life is read by no
/// plan, so a part naming 0 is refused, even beside a part that names it.
#[test]
fn a_batch_part_naming_life_0_is_refused() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut t = Tracking::new();
        let mut items = Log::new("items");
        items
            .commit(&ix, &mut t, [(key("k"), Ver::Int(1))].into())
            .await;
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: key("e"),
                input: key("items"),
                keyed: true,
            })],
        );
        let req = Request {
            inputs: vec![InputRequest {
                own: None,
                ..held_plan_request().inputs[0].clone()
            }],
            ..held_plan_request()
        };
        let p = plan::plan(&t.view(&req).unwrap(), &req, &OneIndex(&ix), &plan::NoHold)
            .await
            .unwrap();
        let ops = t
            .progress_ops(&p, &[InputReport::default()], b"run/batch", None)
            .unwrap();
        let mut parts = vec![Part::Declare(Declare {
            output: key("ghost"),
        })];
        for part in ops.parts {
            parts.push(match part {
                Part::Batch(b) => Part::Batch(BatchProgress {
                    own: Some((key("ghost"), 0)),
                    ..b
                }),
                part => part,
            });
        }
        let ev = Event::decode(&Event::of(parts).encode()).unwrap();
        let e = t.check_event(&ev).unwrap_err();
        assert!(e.to_string().contains("a life is never 0"), "{e}");
    });
}

/// A first commit built while its output had no life lands only while it
/// still has none: built before a life began and held past its end, it does
/// not become the next life's first commit (Fable, round 1: a commit naming
/// life 0 matches every stretch with no entry). B5 PR 3 declares every
/// output before its first commit; then a commit naming 0 is refused.
#[test]
#[should_panic(expected = "B5 PR 3")]
fn a_first_commit_built_before_a_whole_life_does_not_apply() {
    let rt = runtime();
    let ix = index(&rt);
    rt.block_on(async {
        let mut t = Tracking::new();
        let mut staging = Tracking::new();
        let mut a = Log::new("x");
        a.commit(&ix, &mut staging, [(key("k"), Ver::Int(1))].into())
            .await;
        let pending = commit_part(&t, "x", &a.layers[0], 1);
        assert!(matches!(&pending, Part::OutputCommit(c) if c.life == 0));
        apply(&mut t, vec![Part::Declare(Declare { output: key("x") })]);
        let mut staging = Tracking::new();
        let mut b = Log {
            files: a.files,
            ..Log::new("x")
        };
        b.commit(&ix, &mut staging, [(key("k"), Ver::Int(7))].into())
            .await;
        commit_to(&mut t, "x", &b.layers[0], 1);
        apply(&mut t, vec![Part::Retire(Retire { output: key("x") })]);
        assert!(
            t.check_event(&Event::of(vec![pending])).is_err(),
            "a first commit built before a whole life applies after it (strict xfail, B5 PR 3)"
        );
    });
}
