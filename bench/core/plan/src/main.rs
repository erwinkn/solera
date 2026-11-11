//! The planner at the scale target: 100M keys, 1% changing a day, 10k-key
//! batches, consumers a day behind (build plan N2 to N5 and N12).
//!
//! `core-plan-bench workload=daily-scattered keys=100000000 dir=/data/x [tasks=50 threads=4 batch=10000 behind=24]`
//! `core-plan-bench run=lineage keys=100000000 dir=/data/x [rounds=20 scenarios=all layers=N]`:
//! B10's lineage beside the plan it replays.
//!
//! `layers=N` writes the output as the engine leaves it before merges: one
//! layer holding every commit up to the newest N - 1, then one layer per
//! commit (N = 1: one layer for the whole history). Without it, the layers
//! follow the change-index bench's merge schedule.
//!
//! The output's layers are the change-index bench's (same workloads, seed and
//! layer schedule), written once to `dir` and reused. One JSON line per
//! measurement on stdout. Every plan over a piece one day behind must equal
//! the first N keys of `diff(P, H)` from its cursor; the bench checks that.

#[path = "../../../change-index/src/alloc.rs"]
mod alloc;
#[path = "../../../change-index/src/gen.rs"]
#[allow(dead_code)]
mod gen;
#[path = "../../../change-index/src/sched.rs"]
#[allow(dead_code)]
mod sched;
#[path = "../../../change-index/src/sim.rs"]
mod sim;

#[global_allocator]
static A: alloc::Counting = alloc::Counting;

use gen::{key, key_of, Oracle, Workload, LOAD, NONE};
use object_store::path::Path;
use serde_json::json;
use solera_index::{
    ChangeIndex, CommitHeader, LayerRef, Query, Store, StoreOptions, Version, WriteOptions, Writer,
};
use solera_tracking::codec::Ver;
use solera_tracking::codec::{Dec, Enc};
use solera_tracking::event::{Event, Part};
use solera_tracking::lineage::{self, Answer};
use solera_tracking::plan::{self, InputRequest, Mode, OneIndex, Plan, Request};
use solera_tracking::progress::{
    read_digest, BatchProgress, Conditions, EdgeOpen, Holding, InputReport, Key, KeyRange, Keyed,
    Point, Reason, Value,
};
use solera_tracking::record::Record;
use solera_tracking::{Output, Tracking};
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

const NET: (f64, f64) = (0.030, 80e6);

struct Args(HashMap<String, String>);

impl Args {
    fn get<T: std::str::FromStr>(&self, k: &str, d: T) -> T {
        self.0.get(k).map(|v| v.parse().ok().expect(k)).unwrap_or(d)
    }
    fn s(&self, k: &str, d: &str) -> String {
        self.0.get(k).cloned().unwrap_or(d.into())
    }
}

fn ver(v: u64) -> Version<'static> {
    if v == NONE {
        Version::Absent
    } else {
        Version::Int(v)
    }
}

fn entries_of(w: &Workload, lo: u64, hi: u64) -> Vec<(u32, u32, u64, u64)> {
    let mut v: Vec<(u32, u32, u64, u64)> = w.commits[lo as usize..=hi as usize]
        .iter()
        .enumerate()
        .flat_map(|(i, c)| {
            c.iter()
                .map(move |e| (e.id, lo as u32 + i as u32, e.new, e.replaced))
        })
        .collect();
    v.sort_unstable_by_key(|e| (e.0, std::cmp::Reverse(e.1)));
    v
}

/// One layer of the schedule (as bench/change-index writes it). A base holds
/// the state at `fold`, and the changes above it up to its `hi`.
fn write_layer(
    dir: &std::path::Path,
    path: &str,
    spec: &sched::Spec,
    fold: u64,
    w: &Workload,
    o: &Oracle,
) -> LayerRef {
    let mut file =
        std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(dir.join(path)).unwrap());
    let (lo, floor) = if spec.base {
        (1, Some(fold))
    } else {
        (spec.lo, None)
    };
    let mut wr = Writer::new(WriteOptions::default(), lo, spec.hi, floor).unwrap();
    let mut buf = Vec::with_capacity(64);
    let drain = |wr: &mut Writer, file: &mut std::io::BufWriter<std::fs::File>| {
        if wr.pending() >= 8 << 20 {
            file.write_all(&wr.take()).unwrap();
        }
    };
    let (changed, folded) = if spec.base {
        (fold + 1, Some(fold))
    } else {
        (spec.lo, None)
    };
    let changes = entries_of(w, changed, spec.hi);
    let mut next = changes.iter().peekable();
    let mut push = |wr: &mut Writer, id: u32| {
        while let Some(e) = next.next_if(|e| e.0 <= id) {
            key_of(e.0, &mut buf);
            wr.push(&buf, e.1 as u64, ver(e.2), ver(e.3)).unwrap();
            drain(wr, &mut file);
        }
        let Some(f) = folded else { return };
        let (v, c) = o.at(id, f);
        if v != NONE {
            key_of(id, &mut buf);
            wr.push(&buf, c, Version::Int(v), Version::Absent).unwrap();
            drain(wr, &mut file);
        }
    };
    if folded.is_some() {
        for id in 0..o.space() {
            push(&mut wr, id);
        }
    } else if let Some(last) = changes.last() {
        push(&mut wr, last.0);
    }
    let headers: Vec<CommitHeader> = (changed..=spec.hi)
        .map(|commit| CommitHeader {
            commit,
            ..Default::default()
        })
        .collect();
    let mut l = wr.finish(&headers).unwrap();
    file.write_all(&wr.take()).unwrap();
    file.flush().unwrap();
    l.path = path.to_string();
    l
}

/// The output's layers: read from `dir/layers.bin`, or generated and written there.
fn layers(a: &Args, dir: &std::path::Path) -> (Vec<LayerRef>, u64) {
    let list = dir.join("layers.bin");
    if let Ok(bytes) = std::fs::read(&list) {
        let mut d = Dec::new(&bytes);
        let head = d.uv().unwrap();
        let n = d.count().unwrap();
        let layers: Vec<LayerRef> = (0..n).map(|_| d.layer().unwrap()).collect();
        let split = a.get::<usize>("layers", 0);
        assert!(
            split == 0 || layers.len() == split,
            "{} holds {} layers, not {split}: use one dir per layer count",
            dir.display(),
            layers.len()
        );
        return (layers, head);
    }
    std::fs::create_dir_all(dir).unwrap();
    let (name, n) = (
        a.s("workload", "daily-scattered"),
        a.get::<u32>("keys", 1_000_000),
    );
    let (days, per_day, seed) = (
        a.get::<u64>("days", 30),
        a.get::<u64>("per_day", 24),
        a.get::<u64>("seed", 7),
    );
    let t0 = Instant::now();
    let w = match name.as_str() {
        "daily-scattered" => gen::daily(n, days, per_day, 0.01, false, seed),
        "daily-clustered" => gen::daily(n, days, per_day, 0.01, true, seed),
        _ => panic!("workload?"),
    };
    let oracle = Oracle::new(&w);
    let counts: Vec<u64> = w.commits.iter().map(|c| c.len() as u64).collect();
    let mut live = vec![0u64; w.commits.len()];
    let mut n_live = 0i64;
    for (i, c) in w.commits.iter().enumerate() {
        for e in c {
            n_live += (e.new != NONE) as i64 - (e.replaced != NONE) as i64;
        }
        live[i] = n_live as u64;
    }
    let mut sch = sched::schedule(&counts, LOAD, |c| live[c as usize], per_day, 7, Some(LOAD));
    let head = w.commits.len() as u64 - 1;
    let split = a.get::<u64>("layers", 0);
    assert!(
        split <= head - LOAD,
        "at most {} layers after the load",
        head - LOAD
    );
    if split > 0 {
        let one = |lo: u64, hi: u64| sched::Spec {
            lo,
            hi,
            base: false,
            entries: 0,
        };
        sch.layers = std::iter::once(sched::Spec {
            base: true,
            ..one(1, head + 1 - split)
        })
        .chain((head + 2 - split..=head).map(|c| one(c, c)))
        .collect();
    }
    let next = std::sync::atomic::AtomicUsize::new(0);
    let built = Mutex::new(Vec::new());
    std::thread::scope(|s| {
        for _ in 0..4 {
            s.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(spec) = sch.layers.get(i) else {
                    return;
                };
                let path = format!("{i:03}-{}-{}", spec.lo, spec.hi);
                let fold = if split > 0 { LOAD } else { spec.hi };
                let l = write_layer(dir, &path, spec, fold, &w, &oracle);
                built.lock().unwrap().push((i, l));
            });
        }
    });
    let mut built = built.into_inner().unwrap();
    built.sort_by_key(|x| x.0);
    let layers: Vec<LayerRef> = built.into_iter().map(|x| x.1).collect();
    solera_index::check_layers(&layers).unwrap();
    let mut e = Enc::new();
    e.uv(head);
    e.uv(layers.len() as u64);
    for l in &layers {
        e.layer(l);
    }
    std::fs::write(&list, &e.buf).unwrap();
    eprintln!(
        "{name} {n}: {} layers written in {:.0}s",
        layers.len(),
        t0.elapsed().as_secs_f64()
    );
    (layers, head)
}

fn index(
    dir: &std::path::Path,
    net: Option<(f64, f64)>,
    rt: &tokio::runtime::Runtime,
    memory: u64,
) -> ChangeIndex {
    let opts = StoreOptions {
        memory_bytes: memory,
        ..Default::default()
    };
    let store = Store::new(
        Arc::new(sim::Sim::new(dir, net)),
        Path::from(""),
        rt.handle().clone(),
        opts,
    )
    .unwrap();
    ChangeIndex::new(store, WriteOptions::default())
}

fn edge(i: usize) -> Key {
    format!("consumer-{i:02}/items").into_bytes().into()
}

/// The state: one output, and `tasks` consumers each holding everything at `behind` commits ago.
fn state(
    layers: &[LayerRef],
    head: u64,
    tasks: usize,
    at: u64,
    cond: &Arc<Conditions>,
) -> Tracking {
    scenario_state(
        layers,
        head,
        tasks,
        Some(Value::At(at, cond.clone())),
        0,
        0,
        None,
    )
}

/// The keys of `points` failed keys spread over the key space, shifted by `shift`.
fn point_keys(points: u32, space: u32, shift: u32) -> Vec<Key> {
    (0..points)
        .map(|j| key((space as u64 * j as u64 / points as u64) as u32 + shift).into())
        .collect()
}

/// One output, and `tasks` consumers each with one piece of `value` over
/// everything (None: a new edge, empty) and `points` failed keys spread
/// over the key space. With `seen` (each key's version at the piece's
/// commit, as a batch commit records it), every consumer's points sit at the
/// same keys; without, each consumer's are shifted, and the planner looks
/// each one up.
fn scenario_state(
    layers: &[LayerRef],
    head: u64,
    tasks: usize,
    value: Option<Value>,
    points: u32,
    space: u32,
    seen: Option<&[Ver]>,
) -> Tracking {
    let mut t = Tracking::new();
    t.outputs.insert(
        b"items".as_slice().into(),
        Output {
            layers: layers.to_vec(),
            head,
            live: 0,
            full_run: None,
        },
    );
    for i in 0..tasks {
        let open = Part::EdgeOpen(EdgeOpen {
            edge: edge(i),
            input: b"items".as_slice().into(),
            keyed: true,
        });
        t.apply(&Event::of(vec![open]).encode()).unwrap();
        let Some(value) = value.clone() else { continue };
        let cond = match &value {
            Value::At(_, c) => c.clone(),
            _ => Arc::new(Conditions::default()),
        };
        let shift = if seen.is_some() { 0 } else { i as u32 };
        let pts = point_keys(points, space, shift)
            .into_iter()
            .enumerate()
            .map(|(j, k)| {
                (
                    k,
                    Point {
                        holding: Holding::Version(Ver::Int(1), cond.clone()),
                        reason: Reason::Failed,
                        seen: seen.map(|v| v[j].clone()),
                    },
                )
            })
            .collect();
        let caught_up = Part::Batch(BatchProgress {
            edge: edge(i),
            reset: false,
            range: KeyRange::all(),
            piece: Some(value),
            points: pts,
            expect: read_digest(&Keyed::default(), false, &KeyRange::all()),
            fence: None,
        });
        t.apply(&Event::of(vec![caught_up]).encode()).unwrap();
    }
    t
}

fn request(i: usize, after: Option<Key>, limit: usize, cond: &Arc<Conditions>) -> Request {
    Request {
        mode: Mode::Incremental,
        reset: false,
        after,
        limit,
        inputs: vec![InputRequest {
            edge: edge(i),
            conditions: cond.clone(),
            own: None,
        }],
        unkeyed: vec![],
    }
}

/// One plan: the view under a short read lock, then the plan without it.
async fn plan_once(t: &RwLock<Tracking>, ix: &ChangeIndex, req: &Request) -> Plan {
    let view = t.read().unwrap().view(req).unwrap();
    plan::plan(&view, req, &OneIndex(ix), &plan::NoHold)
        .await
        .unwrap()
}

fn check(
    ix: &ChangeIndex,
    rt: &tokio::runtime::Runtime,
    plan: &Plan,
    layers: &[LayerRef],
    p: u64,
    head: u64,
    limit: usize,
) {
    let q = Query {
        lo: plan.range.lo.as_deref().map(<[u8]>::to_vec),
        limit: Some(limit as u64),
        ..Default::default()
    };
    let page = rt.block_on(ix.diff(layers, p, head, &q)).unwrap();
    let keys = &plan.inputs[0].keys;
    assert_eq!(
        keys.len(),
        page.run.len(),
        "the plan takes the diff's first N keys"
    );
    for (i, c) in keys.iter().enumerate() {
        assert_eq!(&*c.key, page.run.key(i));
        assert_eq!(
            c.version.as_version(),
            page.run.new_version(page.run.rows(i).start)
        );
    }
    if page.next.is_none() {
        assert_eq!(plan.range.hi, None);
    } else {
        assert_eq!(plan.range.hi.as_deref(), page.run.last_key());
    }
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if v.is_empty() {
        return 0.0;
    }
    v[((v.len() - 1) as f64 * p).round() as usize]
}

fn r3(x: f64) -> f64 {
    (x * 1e3).round() / 1e3
}

fn rss_mb() -> f64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0)
        / 1024.0
}

fn main() {
    let a = Args(
        std::env::args()
            .skip(1)
            .filter_map(|x| x.split_once('=').map(|(k, v)| (k.into(), v.into())))
            .collect(),
    );
    let name = a.s("workload", "daily-scattered");
    let n = a.get::<u32>("keys", 1_000_000);
    let dir = std::path::PathBuf::from(a.s("dir", &format!("/tmp/core-plan/{name}-{n}")));
    let (tasks, threads, limit, behind) = (
        a.get::<usize>("tasks", 50),
        a.get::<usize>("threads", 4),
        a.get::<usize>("batch", 10_000),
        a.get::<u64>("behind", 24),
    );
    let memory = a.get::<u64>("memory_mb", 256) << 20;
    let (layers, head) = layers(&a, &dir);
    let p = head - behind;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let cond = Arc::new(Conditions::default());
    let emit = |q: &str, extra: serde_json::Value| {
        let mut v = json!({"workload": name, "keys": n, "layers": layers.len(), "query": q, "batch": limit, "behind": behind});
        v.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        println!("{v}");
    };
    if a.s("run", "plan") == "lineage" {
        lineage_runs(
            &a, &dir, &rt, &layers, head, n, limit, behind, memory, &emit,
        );
        return;
    }
    let heap0 = alloc::live();
    let t = RwLock::new(state(&layers, head, tasks, p, &cond));
    emit(
        "state",
        json!({"tasks": tasks, "state_mb": r3((alloc::live() - heap0) as f64 / 1e6), "snapshot_bytes": t.read().unwrap().snapshot().len()}),
    );

    // N2, N4 cold: one plan, a day behind, on a cold index over the simulated network.
    for (mode, net) in [("cold", Some(NET)), ("local-cold", None)] {
        let ix = index(&dir, net, &rt, memory);
        let req = request(0, None, limit, &cond);
        let (cpu, t0, base) = (proc_cpu(), Instant::now(), alloc::reset_peak());
        let plan = rt.block_on(plan_once(&t, &ix, &req));
        let (wall, cpu) = (t0.elapsed().as_secs_f64(), proc_cpu() - cpu);
        let c = ix.store.stats.counts();
        check(&ix, &rt, &plan, &layers, p, head, limit);
        emit(
            "plan-1",
            json!({"mode": mode, "wall_s": r3(wall), "cpu_s": r3(cpu), "gets": c.gets, "mb": r3(c.get_bytes as f64 / 1e6), "planned": plan.inputs[0].keys.len(), "us_per_key": r3(cpu * 1e6 / plan.inputs[0].keys.len() as f64), "peak_mb": r3((alloc::peak() - base) as f64 / 1e6), "plan_bytes": plan.encode().len()}),
        );
    }

    // N2: 50 plans at once on a cold index fetch each object once.
    {
        let ix = index(&dir, Some(NET), &rt, memory);
        let t0 = Instant::now();
        std::thread::scope(|s| {
            for i in 0..tasks {
                let (ix, t, rt, cond) = (&ix, &t, &rt, &cond);
                s.spawn(move || {
                    let req = request(i, None, limit, cond);
                    rt.block_on(plan_once(t, ix, &req));
                });
            }
        });
        let c = ix.store.stats.counts();
        emit(
            "plan-50-at-once",
            json!({"mode": "cold", "wall_s": r3(t0.elapsed().as_secs_f64()), "gets": c.gets, "mb": r3(c.get_bytes as f64 / 1e6), "waits": c.waits}),
        );
    }

    // N4 warm alone, N5: one consumer through its whole day of changes, twice;
    // the second pass is warm.
    {
        let ix = index(&dir, None, &rt, memory);
        for pass in ["warm-up", "warm"] {
            let tt = RwLock::new(state(&layers, head, 1, p, &cond));
            let (mut lat, mut cpus, mut keys, mut commit_s) = (vec![], 0.0, 0usize, 0.0);
            let mut after: Option<Key> = None;
            loop {
                let req = request(0, after.clone(), limit, &cond);
                let (cpu, t0) = (proc_cpu(), Instant::now());
                let plan = rt.block_on(plan_once(&tt, &ix, &req));
                lat.push(t0.elapsed().as_secs_f64());
                cpus += proc_cpu() - cpu;
                keys += plan.inputs[0].keys.len();
                if pass == "warm" && lat.len() % 10 == 1 {
                    check(&ix, &rt, &plan, &layers, p, head, limit);
                }
                let t1 = Instant::now();
                let ops = tt
                    .read()
                    .unwrap()
                    .progress_ops(
                        &plan,
                        &[InputReport::default()],
                        b"run/batch/attempt",
                        Some(1),
                    )
                    .unwrap();
                tt.write().unwrap().apply(&ops.event().encode()).unwrap();
                commit_s += t1.elapsed().as_secs_f64();
                after = plan.range.hi.clone();
                if after.is_none() {
                    break;
                }
            }
            let batches = lat.len();
            emit(
                "plan-day",
                json!({"mode": pass, "batches": batches, "keys_planned": keys, "p50_s": r3(pct(&mut lat.clone(), 0.5)), "p99_s": r3(pct(&mut lat, 0.99)), "cpu_s": r3(cpus), "us_per_key": r3(cpus * 1e6 / keys as f64), "commit_us_per_batch": r3(commit_s * 1e6 / batches as f64), "pieces_after": tt.read().unwrap().edges[&edge(0)].progress.keyed().unwrap().piece_count()}),
            );
        }
    }

    // N3, N4 at 50 tasks, N12: every consumer planning back to back on
    // `threads` planning threads sharing one index, each from its own place in
    // the key space, for at most `max_batches` batches (a day behind: to the end).
    let max_batches = a.get::<usize>("max_batches", 20);
    let old = Arc::new(Conditions {
        context: b"old".as_slice().into(),
        ..Default::default()
    });
    let seen: Vec<Ver> = {
        let ix = index(&dir, None, &rt, memory);
        let keys = point_keys(10_000, n, 0);
        let page = rt
            .block_on(ix.get(&layers, p, &keys, &Query::default()))
            .unwrap();
        let mut got = HashMap::new();
        for i in 0..page.run.len() {
            got.insert(
                page.run.key(i).to_vec(),
                Ver::of(page.run.new_version(page.run.rows(i).start)),
            );
        }
        keys.iter()
            .map(|k| got.get(&k[..]).cloned().unwrap_or(Ver::Absent))
            .collect()
    };
    let scenarios: Vec<(&str, Option<Value>, u32, usize)> = vec![
        ("day", Some(Value::At(p, cond.clone())), 0, usize::MAX),
        (
            "week",
            Some(Value::At(head - 7 * behind, cond.clone())),
            0,
            max_batches,
        ),
        ("empty", None, 0, max_batches),
        ("context", Some(Value::At(p, old.clone())), 0, max_batches),
        (
            "points",
            Some(Value::At(p, cond.clone())),
            10_000,
            max_batches,
        ),
        (
            "points-lookup",
            Some(Value::At(p, cond.clone())),
            10_000,
            max_batches,
        ),
    ];
    let only = a.s("scenarios", "all");
    for (scenario, value, points, cap) in scenarios {
        if only != "all" && !only.split(',').any(|o| o == scenario) {
            continue;
        }
        for mode in ["cold", "warm"] {
            let ix = index(
                &dir,
                if mode == "cold" { Some(NET) } else { None },
                &rt,
                memory,
            );
            let tt = RwLock::new(scenario_state(
                &layers,
                head,
                tasks,
                value.clone(),
                points,
                n,
                (scenario == "points").then_some(seen.as_slice()),
            ));
            let starts: Vec<Option<Key>> = (0..tasks)
                .map(|i| (i > 0).then(|| key((n as u64 * i as u64 / tasks as u64) as u32).into()))
                .collect();
            if mode == "warm" {
                for i in 0..tasks {
                    let mut after = starts[i].clone();
                    for _ in 0..cap.min(1 << 20) {
                        let plan = rt.block_on(plan_once(
                            &tt,
                            &ix,
                            &request(i, after.clone(), limit, &cond),
                        ));
                        after = plan.range.hi.clone();
                        if after.is_none() {
                            break;
                        }
                    }
                }
            }
            let c0 = ix.store.stats.counts();
            let heap = alloc::live();
            let base = alloc::reset_peak();
            let queue: Mutex<VecDeque<(usize, Option<Key>, Instant, usize)>> = Mutex::new(
                (0..tasks)
                    .map(|i| (i, starts[i].clone(), Instant::now(), 0))
                    .collect(),
            );
            let stats = Mutex::new((vec![], vec![], 0usize, 0.0f64));
            let t0 = Instant::now();
            let cpu0 = proc_cpu();
            std::thread::scope(|s| {
                for _ in 0..threads {
                    s.spawn(|| loop {
                        let Some((i, after, asked, done)) = queue.lock().unwrap().pop_front()
                        else {
                            return;
                        };
                        let req = request(i, after, limit, &cond);
                        let (cpu, t1) = (alloc::cpu(), Instant::now());
                        let plan = rt.block_on(plan_once(&tt, &ix, &req));
                        let (plan_s, cpu) = (t1.elapsed().as_secs_f64(), alloc::cpu() - cpu);
                        if scenario == "day" && done == 0 {
                            check(&ix, &rt, &plan, &layers, p, head, limit);
                        }
                        let ops = tt
                            .read()
                            .unwrap()
                            .progress_ops(
                                &plan,
                                &[InputReport::default()],
                                b"run/batch/attempt",
                                Some(1),
                            )
                            .unwrap();
                        tt.write().unwrap().apply(&ops.event().encode()).unwrap();
                        {
                            let mut st = stats.lock().unwrap();
                            st.0.push(plan_s);
                            st.1.push(asked.elapsed().as_secs_f64());
                            st.2 += plan.inputs[0].keys.len();
                            st.3 += cpu;
                        }
                        if let Some(hi) = plan.range.hi.clone() {
                            if done + 1 < cap {
                                queue.lock().unwrap().push_back((
                                    i,
                                    Some(hi),
                                    Instant::now(),
                                    done + 1,
                                ));
                            }
                        }
                    });
                }
            });
            let wall = t0.elapsed().as_secs_f64();
            let pcpu = proc_cpu() - cpu0;
            let c = ix.store.stats.counts();
            let (mut plan_s, mut task_s, keys, cpu) = stats.into_inner().unwrap();
            emit(
                "plan-50-tasks",
                json!({"scenario": scenario, "mode": mode, "tasks": tasks, "threads": threads, "batches": plan_s.len(), "keys_planned": keys, "wall_s": r3(wall),
                    "keys_per_s": (keys as f64 / wall).round(), "plan_p50_s": r3(pct(&mut plan_s, 0.5)), "plan_p99_s": r3(pct(&mut plan_s, 0.99)),
                    "task_p50_s": r3(pct(&mut task_s, 0.5)), "task_p99_s": r3(pct(&mut task_s, 0.99)), "plan_thread_cpu_s": r3(cpu), "proc_cpu_s": r3(pcpu),
                    "us_per_key": r3(pcpu * 1e6 / keys as f64), "gets": c.gets - c0.gets, "mb": r3((c.get_bytes - c0.get_bytes) as f64 / 1e6),
                    "heap_start_mb": r3(heap as f64 / 1e6), "peak_mb": r3(alloc::peak() as f64 / 1e6), "peak_above_start_mb": r3((alloc::peak() - base) as f64 / 1e6), "rss_mb": r3(rss_mb())}),
            );
        }
    }
}

/// B10: a lineage question costs what a plan costs (N2, N4). Per scenario,
/// one consumer plans a batch from the middle of the key space and commits
/// it; its lineage is then recomputed from its record. Cold: the plan and
/// the lineage each on their own cold index over the simulated network.
/// Warm: `rounds` batches planned and committed, then each one planned again
/// on a fresh state and its lineage recomputed, both on the warmed index.
#[allow(clippy::too_many_arguments)]
fn lineage_runs(
    a: &Args,
    dir: &std::path::Path,
    rt: &tokio::runtime::Runtime,
    layers: &[LayerRef],
    head: u64,
    n: u32,
    limit: usize,
    behind: u64,
    memory: u64,
    emit: &dyn Fn(&str, serde_json::Value),
) {
    let rounds = a.get::<usize>("rounds", 20);
    let p = head - behind;
    let cond = Arc::new(Conditions::default());
    let old = Arc::new(Conditions {
        context: b"old".as_slice().into(),
        ..Default::default()
    });
    let items: Key = b"items".as_slice().into();
    let seen: Vec<Ver> = {
        let ix = index(dir, None, rt, memory);
        let keys = point_keys(10_000, n, 0);
        let page = rt
            .block_on(ix.get(layers, p, &keys, &Query::default()))
            .unwrap();
        let mut got = HashMap::new();
        for i in 0..page.run.len() {
            got.insert(
                page.run.key(i).to_vec(),
                Ver::of(page.run.new_version(page.run.rows(i).start)),
            );
        }
        keys.iter()
            .map(|k| got.get(&k[..]).cloned().unwrap_or(Ver::Absent))
            .collect()
    };
    // (scenario, the consumer's progress, failed points, mode, the head it plans at, cleanup cursor)
    let scenarios: Vec<(&str, Option<Value>, u32, Mode, u64, u64)> = vec![
        (
            "day",
            Some(Value::At(p, cond.clone())),
            0,
            Mode::Incremental,
            head,
            0,
        ),
        (
            "context",
            Some(Value::At(p, old.clone())),
            0,
            Mode::Incremental,
            head,
            0,
        ),
        (
            "points",
            Some(Value::At(p, cond.clone())),
            10_000,
            Mode::Incremental,
            head,
            0,
        ),
        ("empty", None, 0, Mode::Incremental, head, 0),
        ("held", Some(Value::Held), 0, Mode::Incremental, head, 0),
        (
            "all",
            Some(Value::At(p, cond.clone())),
            0,
            Mode::All,
            head,
            0,
        ),
        (
            "day-old-cleaned",
            Some(Value::At(p - behind, cond.clone())),
            0,
            Mode::Incremental,
            p,
            head,
        ),
        (
            "keys-old-cleaned",
            Some(Value::At(p - behind, cond.clone())),
            0,
            Mode::Keys(point_keys(10_000, n, 1)),
            p,
            head,
        ),
    ];
    let only = a.s("scenarios", "all");
    let start: Option<Key> = Some(key(n / 2).into());
    for (scenario, value, points, mode, at, cleaned) in scenarios {
        if only != "all" && !only.split(',').any(|o| o == scenario) {
            continue;
        }
        let held = value == Some(Value::Held);
        let state = || {
            RwLock::new(scenario_state(
                layers,
                at,
                1,
                value.clone(),
                points,
                n,
                (points > 0).then_some(seen.as_slice()),
            ))
        };
        let req = |after: Option<Key>| Request {
            mode: mode.clone(),
            reset: false,
            after,
            limit,
            inputs: vec![InputRequest {
                edge: edge(0),
                conditions: cond.clone(),
                own: held.then(|| items.clone()),
            }],
            unkeyed: vec![],
        };
        let commit = |t: &RwLock<Tracking>, plan: &Plan| {
            let ops = t
                .read()
                .unwrap()
                .progress_ops(
                    plan,
                    &[InputReport::default()],
                    b"run/batch/attempt",
                    Some(1),
                )
                .unwrap();
            t.write().unwrap().apply(&ops.event().encode()).unwrap();
            ops.records.into_iter().next().unwrap()
        };
        let replay = |t: &RwLock<Tracking>, ix: &ChangeIndex, rec: &Record| {
            let logs = t.read().unwrap().logs(rec, held.then_some(&items[..]));
            match rt
                .block_on(lineage::input(&logs, rec, &OneIndex(ix), cleaned))
                .unwrap()
            {
                Answer::Found(keys) => keys,
                Answer::NotRetained { commit, .. } => panic!("not retained at {commit}"),
            }
        };

        let t = state();
        let ix = index(dir, Some(NET), rt, memory);
        let (cpu, t0) = (proc_cpu(), Instant::now());
        let plan = rt.block_on(plan_once(&t, &ix, &req(start.clone())));
        let (plan_s, plan_cpu, pc) = (
            t0.elapsed().as_secs_f64(),
            proc_cpu() - cpu,
            ix.store.stats.counts(),
        );
        let rec = commit(&t, &plan);
        let ix = index(dir, Some(NET), rt, memory);
        let (cpu, t0) = (proc_cpu(), Instant::now());
        let got = replay(&t, &ix, &rec);
        let (lin_s, lin_cpu, lc) = (
            t0.elapsed().as_secs_f64(),
            proc_cpu() - cpu,
            ix.store.stats.counts(),
        );
        assert_eq!(got.len(), plan.inputs[0].keys.len());
        emit(
            "lineage-1",
            json!({"scenario": scenario, "mode": "cold", "keys_in_batch": got.len(), "deleted": got.iter().filter(|r| r.deleted).count(),
                "plan_wall_s": r3(plan_s), "plan_cpu_s": r3(plan_cpu), "plan_gets": pc.gets, "plan_mb": r3(pc.get_bytes as f64 / 1e6),
                "lineage_wall_s": r3(lin_s), "lineage_cpu_s": r3(lin_cpu), "lineage_gets": lc.gets, "lineage_mb": r3(lc.get_bytes as f64 / 1e6)}),
        );

        let ix = index(dir, None, rt, memory);
        let t = state();
        let mut recs = vec![];
        let mut after = start.clone();
        for _ in 0..rounds {
            let plan = rt.block_on(plan_once(&t, &ix, &req(after.clone())));
            after = plan.range.hi.clone();
            recs.push(commit(&t, &plan));
            if after.is_none() {
                break;
            }
        }
        let fresh = state();
        let (mut plan_lat, mut lin_lat, mut plan_cpu, mut lin_cpu, mut keys) =
            (vec![], vec![], 0.0, 0.0, 0);
        let mut after = start.clone();
        for rec in &recs {
            let (cpu, t0) = (proc_cpu(), Instant::now());
            let plan = rt.block_on(plan_once(&fresh, &ix, &req(after.clone())));
            plan_lat.push(t0.elapsed().as_secs_f64());
            plan_cpu += proc_cpu() - cpu;
            commit(&fresh, &plan);
            after = plan.range.hi.clone();
            let (cpu, t0) = (proc_cpu(), Instant::now());
            keys += replay(&t, &ix, rec).len();
            lin_lat.push(t0.elapsed().as_secs_f64());
            lin_cpu += proc_cpu() - cpu;
        }
        emit(
            "lineage-batches",
            json!({"scenario": scenario, "mode": "warm", "batches": recs.len(), "keys": keys,
                "plan_p50_s": r3(pct(&mut plan_lat.clone(), 0.5)), "plan_p99_s": r3(pct(&mut plan_lat, 0.99)), "plan_cpu_s": r3(plan_cpu),
                "lineage_p50_s": r3(pct(&mut lin_lat.clone(), 0.5)), "lineage_p99_s": r3(pct(&mut lin_lat, 0.99)), "lineage_cpu_s": r3(lin_cpu),
                "lineage_us_per_key": r3(lin_cpu * 1e6 / keys.max(1) as f64)}),
        );
    }
}

fn proc_cpu() -> f64 {
    let mut r: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut r) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(r.ru_utime) + t(r.ru_stime)
}
