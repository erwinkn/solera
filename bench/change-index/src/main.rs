//! The change index at the scale target, measured the way the format
//! benchmark (T44, branch `exp/layer-format`, `bench/layer-format`) measured
//! its "ours" prototype: the same workloads, seed, layer schedule, simulated
//! store and queries, read through the shipped reader (`native/index`), and
//! every result checked against a brute-force fold of the commit stream.
//!
//! `change-index-bench workload=daily-scattered keys=10000000 [dir=/tmp/ci
//! queries=all merges=1 threads=4 days=30 per_day=24 seed=7]`: one JSON line
//! per measurement on stdout. Workloads: daily-scattered, daily-clustered,
//! hot, rewrite.

mod alloc;
mod gen;
mod sched;
mod sim;

#[global_allocator]
static A: alloc::Counting = alloc::Counting;

use gen::{id_of, key, key_of, Digest, Oracle, Workload, LOAD, NONE};
use object_store::path::Path;
use serde_json::json;
use solera_index::{ChangeIndex, CommitHeader, KeyList, LayerRef, Query, Run, Store, StoreOptions, Version, WriteOptions, Writer};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
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

fn unver(v: Version<'_>) -> u64 {
    match v {
        Version::Absent => NONE,
        Version::Int(n) => n,
        Version::Bytes(_) => unreachable!(),
    }
}

fn entries_of(w: &Workload, lo: u64, hi: u64) -> Vec<(u32, u32, u64, u64)> {
    let mut v: Vec<(u32, u32, u64, u64)> = w.commits[lo as usize..=hi as usize]
        .iter()
        .enumerate()
        .flat_map(|(i, c)| c.iter().map(move |e| (e.id, lo as u32 + i as u32, e.new, e.replaced)))
        .collect();
    v.sort_unstable_by_key(|e| (e.0, std::cmp::Reverse(e.1)));
    v
}

/// One layer of the schedule, written to `dir/path` in the index's format.
fn write_layer(dir: &std::path::Path, path: &str, spec: &sched::Spec, w: &Workload, o: &Oracle, live: &[u64]) -> LayerRef {
    let mut file = std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(dir.join(path)).unwrap());
    let (lo, floor) = if spec.base { (1, Some(spec.hi)) } else { (spec.lo, None) };
    let mut wr = Writer::new(WriteOptions::default(), lo, spec.hi, floor).unwrap();
    let mut buf = Vec::with_capacity(64);
    let drain = |wr: &mut Writer, file: &mut std::io::BufWriter<std::fs::File>| {
        if wr.pending() >= 8 << 20 {
            file.write_all(&wr.take()).unwrap();
        }
    };
    let mut headers = Vec::new();
    if spec.base {
        for id in 0..o.space() {
            let (v, c) = o.at(id, spec.hi);
            if v != NONE {
                key_of(id, &mut buf);
                wr.push(&buf, c, Version::Int(v), Version::Absent).unwrap();
                drain(&mut wr, &mut file);
            }
        }
    } else {
        for e in entries_of(w, spec.lo, spec.hi) {
            key_of(e.0, &mut buf);
            wr.push(&buf, e.1 as u64, ver(e.2), ver(e.3)).unwrap();
            drain(&mut wr, &mut file);
        }
        for c in spec.lo..=spec.hi {
            let es = &w.commits[c as usize];
            let n = |f: &dyn Fn(&gen::Change) -> bool| es.iter().filter(|e| f(e)).count() as u64;
            headers.push(CommitHeader {
                commit: c,
                time_ms: 1_700_000_000_000 + c * 3_600_000,
                flags: 0,
                added: n(&|e| e.replaced == NONE),
                updated: n(&|e| e.replaced != NONE && e.new != NONE),
                removed: n(&|e| e.new == NONE),
                live: live[c as usize],
                min_key: es.first().map_or(vec![], |e| key(e.id)),
                max_key: es.last().map_or(vec![], |e| key(e.id)),
                producer: format!("run-{c:06}/batch-0001/attempt-1").into_bytes(),
            });
        }
    }
    let mut l = wr.finish(&headers).unwrap();
    file.write_all(&wr.take()).unwrap();
    file.flush().unwrap();
    l.path = path.to_string();
    l
}

struct Stats {
    wall: f64,
    cpu: f64,
    pcpu: f64,
    peak: f64,
    gets: u64,
    bytes: u64,
}

fn proc_cpu() -> f64 {
    let mut r: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut r) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(r.ru_utime) + t(r.ru_stime)
}

fn max_rss_mb() -> f64 {
    let mut r: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut r) };
    r.ru_maxrss as f64 / 1024.0
}

fn measure<T>(rt: &tokio::runtime::Runtime, ix: &ChangeIndex, f: impl std::future::Future<Output = T>) -> (T, Stats) {
    let base = alloc::reset_peak();
    let c0 = ix.store.stats.counts();
    let (cpu, pcpu, t) = (alloc::cpu(), proc_cpu(), Instant::now());
    let out = rt.block_on(f);
    let c1 = ix.store.stats.counts();
    let st = Stats {
        wall: t.elapsed().as_secs_f64(),
        cpu: alloc::cpu() - cpu,
        pcpu: proc_cpu() - pcpu,
        peak: (alloc::peak() - base) as f64 / 1e6,
        gets: c1.gets - c0.gets,
        bytes: c1.get_bytes - c0.get_bytes,
    };
    (out, st)
}

#[allow(clippy::too_many_arguments)]
fn emit(w: &str, n: u32, q: &str, mode: &str, d: Digest, want: Digest, st: &Stats, extra: serde_json::Value) {
    let mut v = json!({
        "workload": w, "keys": n, "fmt": "index", "query": q, "mode": mode,
        "wall_s": (st.wall * 1e3).round() / 1e3, "cpu_s": (st.cpu * 1e3).round() / 1e3, "proc_cpu_s": (st.pcpu * 1e3).round() / 1e3,
        "peak_mb": (st.peak * 10.0).round() / 10.0, "gets": st.gets, "mb": (st.bytes as f64 / 1e4).round() / 100.0,
        "results": d.n, "ok": d == want,
    });
    if let serde_json::Value::Object(e) = extra {
        v.as_object_mut().unwrap().extend(e);
    }
    println!("{v}");
    if d != want {
        eprintln!("MISMATCH {w} {q} {mode}: got {d:?}, the fold says {want:?}");
    }
}

enum Q {
    Diff(u64, u64, Option<(u32, u32)>, u64),
    Scan(u64, Option<(u32, u32)>),
    Get(u64, Vec<u32>),
}

fn expect(o: &Oracle, q: &Q) -> Digest {
    let mut d = Digest::default();
    match q {
        Q::Diff(c1, c2, r, limit) => {
            let (a, b) = r.unwrap_or((0, o.space()));
            for id in a..b.min(o.space()) {
                let (x, y) = (o.at(id, *c1).0, o.at(id, *c2).0);
                if x != y {
                    d.add(id, x, y);
                    if d.n == *limit {
                        break;
                    }
                }
            }
        }
        Q::Scan(c, r) => {
            let (a, b) = r.unwrap_or((0, o.space()));
            for id in a..b.min(o.space()) {
                let v = o.at(id, *c).0;
                if v != NONE {
                    d.add(id, v, 0);
                }
            }
        }
        Q::Get(c, ids) => {
            for &id in ids {
                let v = o.at(id, *c).0;
                if v != NONE {
                    d.add(id, v, 0);
                }
            }
        }
    }
    d
}

/// The ids `[a, b)` as the index's `(lo, hi]`.
fn range(r: Option<(u32, u32)>) -> Query {
    match r {
        None => Query::default(),
        Some((a, b)) => Query {
            lo: (a > 0).then(|| key(a - 1)),
            hi: Some(key(b - 1)),
            ..Default::default()
        },
    }
}

fn index(dir: &std::path::Path, net: Option<(f64, f64)>, rt: &tokio::runtime::Runtime) -> ChangeIndex {
    let store = Store::new(Arc::new(sim::Sim::new(dir, net)), Path::from(""), rt.handle().clone(), StoreOptions::default()).unwrap();
    ChangeIndex::new(store, WriteOptions::default())
}

fn main() {
    let a = Args(
        std::env::args()
            .skip(1)
            .filter_map(|x| x.split_once('=').map(|(k, v)| (k.into(), v.into())))
            .collect(),
    );
    let (name, n) = (a.s("workload", "daily-scattered"), a.get::<u32>("keys", 1_000_000));
    let (days, per_day, seed) = (a.get::<u64>("days", 30), a.get::<u64>("per_day", 24), a.get::<u64>("seed", 7));
    let dir = std::path::PathBuf::from(a.s("dir", &format!("/tmp/change-index/{name}-{n}")));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let t0 = Instant::now();
    let w = match name.as_str() {
        "daily-scattered" => gen::daily(n, days, per_day, 0.01, false, seed),
        "daily-clustered" => gen::daily(n, days, per_day, 0.01, true, seed),
        "hot" => gen::hot(n, days, per_day, 0.01, 0.0005, seed),
        "rewrite" => gen::rewrite(n, a.get("days", 7), per_day, 0.01, a.get("batch", (n / 100).max(1000)), seed),
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
    let sch = sched::schedule(&counts, LOAD, |c| live[c as usize], per_day, 7, Some(a.get("preserved", LOAD)));
    let head = w.commits.len() as u64 - 1;
    eprintln!(
        "{name} {n}: {} commits, {} entries after the load, {} layers (max {}), merges wrote {:.2} entries per entry committed, generated in {:.1}s",
        head, sch.committed, sch.layers.len(), sch.max_layers, sch.merged as f64 / sch.committed as f64, t0.elapsed().as_secs_f64()
    );

    // Every layer of the schedule, a few at a time.
    let threads: usize = a.get("threads", 4);
    let next = std::sync::atomic::AtomicUsize::new(0);
    let built = std::sync::Mutex::new(Vec::new());
    let t1 = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(spec) = sch.layers.get(i) else { return };
                let path = format!("{i:03}-{}-{}", spec.lo, spec.hi);
                let l = write_layer(&dir, &path, spec, &w, &oracle, &live);
                built.lock().unwrap().push((i, l));
            });
        }
    });
    let mut built = built.into_inner().unwrap();
    built.sort_by_key(|x| x.0);
    let layers: Vec<LayerRef> = built.into_iter().map(|x| x.1).collect();
    solera_index::check_layers(&layers).unwrap();
    drop(w);
    eprintln!("layers written in {:.1}s", t1.elapsed().as_secs_f64());
    let stored: u64 = layers.iter().map(|l| l.size).sum();
    let meta: u64 = layers.iter().map(|l| l.size - l.index_start).sum();
    let entries: u64 = layers.iter().map(|l| l.entries).sum();
    println!(
        "{}",
        json!({"workload": name, "keys": n, "fmt": "index", "query": "stored", "layers": layers.len(),
            "stored_mb": stored as f64 / 1e6, "meta_mb": meta as f64 / 1e6, "base_mb": layers[0].size as f64 / 1e6,
            "base_meta_mb": (layers[0].size - layers[0].index_start) as f64 / 1e6,
            "bytes_per_entry": stored as f64 / entries as f64,
            "commits": head - LOAD, "committed": sch.committed, "merge_amp": sch.merged as f64 / sch.committed as f64,
            "max_layers": sch.max_layers, "layer_list": layers.iter().map(|l| format!("{}-{}:{}", l.lo, l.hi, l.entries)).collect::<Vec<_>>()})
    );

    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    let day = per_day;
    let space = oracle.space();
    let narrow = Some((space / 2, space / 2 + space / 100));
    let mut rng = <rand::rngs::SmallRng as rand::SeedableRng>::seed_from_u64(seed + 9);
    let mut probe: Vec<u32> = (0..1000).map(|_| rand::Rng::random_range(&mut rng, 0..space)).collect();
    probe.sort_unstable();
    probe.dedup();
    let mut batch: Vec<u32> = (0..10_000).map(|_| rand::Rng::random_range(&mut rng, 0..space)).collect();
    batch.sort_unstable();
    batch.dedup();
    let r0 = LOAD + if name == "rewrite" { a.get::<u64>("days", 7) * per_day } else { 0 };
    let mut qs: Vec<(&str, Q)> = vec![
        ("diff-1c", Q::Diff(head - 1, head, None, u64::MAX)),
        ("diff-1d-first10k", Q::Diff(head - day, head, None, 10_000)),
        ("diff-1d", Q::Diff(head - day, head, None, u64::MAX)),
        ("diff-1d-range1pct", Q::Diff(head - day, head, narrow, u64::MAX)),
        ("diff-paused-first10k", Q::Diff(LOAD, head, None, 10_000)),
        ("diff-paused", Q::Diff(LOAD, head, None, u64::MAX)),
        ("diff-paused-range1pct", Q::Diff(LOAD, head, narrow, u64::MAX)),
        ("scan-head", Q::Scan(head, None)),
        ("scan-head-range1pct", Q::Scan(head, narrow)),
        ("scan-1d-range1pct", Q::Scan(head - day, narrow)),
        ("get-1k", Q::Get(head, probe.clone())),
        ("get-10k", Q::Get(head, batch)),
    ];
    if name == "rewrite" {
        qs.push(("diff-across-rewrite", Q::Diff(r0, head, None, u64::MAX)));
        qs.push(("diff-across-rewrite-first10k", Q::Diff(r0, head, None, 10_000)));
    }
    let only = a.s("queries", "all");
    qs.retain(|(q, _)| only == "all" || only.split(',').any(|o| *q == o));
    let modes = a.s("modes", "cold,warm");
    for (qn, q) in &qs {
        let want = expect(&oracle, q);
        for (mode, net) in [("cold", Some(NET)), ("warm", None)] {
            if !modes.split(',').any(|m| m == mode) {
                continue;
            }
            let ix = index(&dir, net, &rt);
            let mut d = Digest::default();
            let (r, st) = match q {
                Q::Diff(c1, c2, r, limit) => {
                    let qq = Query {
                        limit: (*limit != u64::MAX).then_some(*limit),
                        ..range(*r)
                    };
                    measure(&rt, &ix, ix.diff_with(&layers, *c1, *c2, &qq, |k, _, new, rep| d.add(id_of(k), unver(rep), unver(new))))
                }
                Q::Scan(c, r) => measure(&rt, &ix, ix.state_with(&layers, *c, None, &range(*r), |k, _, v, _| d.add(id_of(k), unver(v), 0))),
                Q::Get(c, ids) => {
                    let keys: Vec<Vec<u8>> = ids.iter().map(|&i| key(i)).collect();
                    measure(&rt, &ix, ix.state_with(&layers, *c, Some(KeyList::new(&keys)), &Query::default(), |k, _, v, _| d.add(id_of(k), unver(v), 0)))
                }
            };
            r.unwrap();
            emit(&name, n, qn, mode, d, want, &st, json!({}));
        }
    }

    // Merges: the four newest layers, and every layer folded into a new base at the head.
    if a.get::<u32>("merges", 1) == 1 {
        for (mn, fold) in [("merge-newest4", false), ("fold-into-base", true)] {
            let len = layers.len();
            let run = if fold { 0..len } else { len - 4..len };
            let ix = index(&dir, Some(NET), &rt);
            let out = format!("out-{mn}");
            let (l, st) = measure(&rt, &ix, ix.merge(&layers, run.clone(), head, &out));
            let l = l.unwrap();
            // Its output, read back, is every entry it should hold.
            let check = index(&dir, None, &rt);
            let mut got = Digest::default();
            let mut g = Run::new();
            rt.block_on(async {
                let mut es = check.entries(&l).await.unwrap();
                while es.next(&mut g).await.unwrap() {
                    let id = id_of(g.key(0));
                    for r in g.rows(0) {
                        got.add4(id, g.commit(r), unver(g.new_version(r)), unver(g.replaced_version(r)));
                    }
                }
            });
            let (lo, hi) = (layers[run.start].lo, layers[run.end - 1].hi);
            let mut want = Digest::default();
            let mut es = Vec::new();
            for id in 0..oracle.space() {
                if fold {
                    let (v, at) = oracle.at(id, head);
                    if v != NONE {
                        want.add4(id, at, v, NONE);
                    }
                } else {
                    oracle.entries(id, lo, hi, &mut es);
                    for e in &es {
                        want.add4(id, e.0, e.1, e.2);
                    }
                }
            }
            let written = l.size as f64 / 1e6;
            std::fs::remove_file(dir.join(&out)).ok();
            emit(&name, n, mn, "cold", got, want, &st, json!({"inputs": run.len(), "written_mb": written, "entries": got.n}));
        }
    }
    eprintln!("max RSS {:.0} MB", max_rss_mb());
    println!("{}", json!({"workload": name, "keys": n, "fmt": "index", "query": "rss", "max_rss_mb": max_rss_mb()}));
}
