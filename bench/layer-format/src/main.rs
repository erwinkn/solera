//! T44: MVCC change-index layers, our block format vs Parquet, measured.
//!
//! `layer-format workload=daily-scattered keys=1000000 [days=30 per_day=24
//! block=16384 level=1 rg_rows=131072 page_bytes=16384 page_rows=1024
//! threads=8 queries=all merges=1]`: one JSON line per measurement on stdout.

mod alloc;
mod gen;
mod ours;
mod pq;
mod query;
mod run;
mod sched;
mod store;

#[global_allocator]
static A: alloc::Counting = alloc::Counting;

use bytes::Bytes;
use gen::{key, Digest, Oracle, Workload, LOAD, NONE};
use query::{Fmt, Layer};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use store::Store;

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

/// The entries of commits `[lo, hi]`, sorted; with `ranged`, only those no
/// range record covers.
fn entries_of(w: &Workload, lo: u64, hi: u64, ranged: Option<&Ranged>) -> Vec<(u32, u32, u64, u64)> {
    let mut v: Vec<(u32, u32, u64, u64)> = w.commits[lo as usize..=hi as usize]
        .iter()
        .enumerate()
        .flat_map(|(i, c)| {
            let cov = ranged.map(|r| &r.covered[lo as usize + i]);
            c.iter()
                .enumerate()
                .filter(move |(j, _)| cov.is_none_or(|cov| cov.is_empty() || !cov[*j]))
                .map(move |(_, e)| (e.id, lo as u32 + i as u32, e.new, e.replaced))
        })
        .collect();
    v.sort_unstable_by_key(|e| (e.0, std::cmp::Reverse(e.1)));
    v
}

/// Range records, found the way the engine would know them: a commit whose
/// changes include every key present just before it in an id interval, all
/// rewritten to one version (or all removed), at least `min` keys.
struct Ranged {
    recs: Vec<Vec<(u32, u32, u64)>>, // by commit: first id, last id, new (NONE: removed)
    covered: Vec<Vec<bool>>,         // by commit, by change: in a record (empty: none is)
}

impl Ranged {
    fn recs(&self, lo: u64, hi: u64) -> Vec<ours::Rec> {
        (lo..=hi)
            .flat_map(|c| {
                self.recs[c as usize].iter().map(move |&(a, b, new)| ours::Rec {
                    lo: key(a),
                    hi: key(b),
                    commit: c,
                    new: opt(new),
                })
            })
            .collect()
    }
}

fn detect(w: &Workload, o: &Oracle, min: usize, threads: usize) -> Ranged {
    let n = w.commits.len();
    let one = |c: usize| -> (Vec<(u32, u32, u64)>, Vec<bool>) {
        let ch = &w.commits[c];
        let present = |id: u32| o.at(id, c as u64 - 1).0 != NONE;
        let (mut recs, mut cov) = (Vec::new(), Vec::new());
        let mut i = 0;
        while i < ch.len() {
            let x = ch[i];
            if x.replaced == NONE {
                i += 1;
                continue; // an add: the key was not there before
            }
            let (mut members, mut last, mut k) = (vec![i], x.id, i + 1);
            while k < ch.len() {
                let y = ch[k];
                if y.replaced != NONE {
                    if y.new != x.new || (last + 1..y.id).any(present) {
                        break;
                    }
                    members.push(k);
                    last = y.id;
                }
                k += 1;
            }
            if members.len() >= min {
                if cov.is_empty() {
                    cov = vec![false; ch.len()];
                }
                members.iter().for_each(|&m| cov[m] = true);
                recs.push((x.id, last, x.new));
            }
            i = k;
        }
        (recs, cov)
    };
    let next = std::sync::atomic::AtomicUsize::new(LOAD as usize + 1);
    let out = std::sync::Mutex::new(vec![(Vec::new(), Vec::new()); n]);
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| loop {
                let c = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if c >= n {
                    return;
                }
                let r = one(c);
                out.lock().unwrap()[c] = r;
            });
        }
    });
    let (recs, covered) = out.into_inner().unwrap().into_iter().unzip();
    Ranged { recs, covered }
}

fn opt(v: u64) -> Option<u64> {
    if v == NONE {
        None
    } else {
        Some(v)
    }
}

/// One layer written in a format: the file and its state entry.
fn write(
    fmt: Fmt,
    a: &Args,
    path: String,
    spec: &sched::Spec,
    recs: Vec<ours::Rec>,
    each: &mut dyn FnMut(&mut dyn FnMut(&[u8], u64, Option<u64>, Option<u64>)),
) -> (Layer, Bytes) {
    let (mut first, mut last) = (Vec::new(), Vec::new());
    let mut track = |k: &[u8]| {
        if first.is_empty() {
            first = k.to_vec();
        }
        last.clear();
        last.extend_from_slice(k);
    };
    let (data, size, meta_start, meta2) = match fmt {
        f if f.ours() => {
            let mut w = writer(f, a, Vec::new());
            each(&mut |k, c, n, r| {
                track(k);
                w.push(k, c, n, r)
            });
            for r in recs {
                if first.is_empty() || r.lo < first {
                    first = r.lo.clone();
                }
                if r.hi > last {
                    last = r.hi.clone();
                }
                w.push_range(r);
            }
            w.finish()
        }
        _ => {
            assert!(recs.is_empty());
            let out = pq::Shared::default();
            let mut w = pq::Writer::new(out.clone(), pq_conf(a));
            each(&mut |k, c, n, r| {
                track(k);
                w.push(k, c, n, r)
            });
            let (size, start) = w.finish();
            let data = std::mem::take(&mut *out.0.lock().unwrap());
            let footer = pq::footer_start(&data);
            (data, size, start, footer)
        }
    };
    assert_eq!(size, data.len() as u64);
    let layer = Layer {
        fmt,
        path,
        size,
        meta_start,
        meta2,
        lo: spec.lo,
        hi: spec.hi,
        base: spec.base,
        entries: spec.entries,
        first,
        last,
    };
    (layer, Bytes::from(data))
}

fn writer<W: std::io::Write>(fmt: Fmt, a: &Args, out: W) -> ours::Writer<W> {
    let mut w = ours::Writer::new(out, a.get("block", 16384), a.get("level", 1));
    if a.s("layout", "rows") == "cols" {
        w = w.by_columns();
    }
    if fmt.rle() {
        w = w.with_rle();
    }
    w
}

fn pq_conf(a: &Args) -> pq::Conf {
    pq::Conf {
        rg_rows: a.get("rg_rows", 131072),
        page_bytes: a.get("page_bytes", 16384),
        page_rows: a.get("page_rows", 1024),
        level: a.get("level", 1),
    }
}

struct Stats {
    wall: f64,
    cpu: f64,
    peak: f64,
    gets: u64,
    bytes: u64,
}

fn measure<T>(
    rt: &tokio::runtime::Runtime,
    s: &Store,
    f: impl std::future::Future<Output = T>,
) -> (T, Stats) {
    let base = alloc::reset_peak();
    let (cpu, t) = (alloc::cpu(), Instant::now());
    let out = rt.block_on(f);
    let st = Stats {
        wall: t.elapsed().as_secs_f64(),
        cpu: alloc::cpu() - cpu,
        peak: (alloc::peak() - base) as f64 / 1e6,
        gets: s.counts().0,
        bytes: s.counts().1,
    };
    (out, st)
}

fn emit(
    w: &str,
    n: u32,
    fmt: Fmt,
    q: &str,
    mode: &str,
    d: Digest,
    want: Digest,
    st: &Stats,
    extra: serde_json::Value,
) {
    let mut v = json!({
        "workload": w, "keys": n, "fmt": fmt.name(), "query": q, "mode": mode,
        "wall_s": (st.wall * 1e3).round() / 1e3, "cpu_s": (st.cpu * 1e3).round() / 1e3,
        "peak_mb": (st.peak * 10.0).round() / 10.0, "gets": st.gets, "mb": (st.bytes as f64 / 1e4).round() / 100.0,
        "results": d.n, "ok": d == want,
    });
    if let serde_json::Value::Object(e) = extra {
        v.as_object_mut().unwrap().extend(e);
    }
    println!("{v}");
    if d != want {
        eprintln!(
            "MISMATCH {w} {} {q} {mode}: got {d:?}, the fold says {want:?}",
            fmt.name()
        );
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

fn main() {
    let a = Args(
        std::env::args()
            .skip(1)
            .filter_map(|x| x.split_once('=').map(|(k, v)| (k.into(), v.into())))
            .collect(),
    );
    let (name, n) = (
        a.s("workload", "daily-scattered"),
        a.get::<u32>("keys", 1_000_000),
    );
    let (days, per_day, seed) = (
        a.get::<u64>("days", 30),
        a.get::<u64>("per_day", 24),
        a.get::<u64>("seed", 7),
    );
    store::COALESCE.store(
        a.get("coalesce", 64u64 << 10),
        std::sync::atomic::Ordering::Relaxed,
    );
    pq::BATCH_ROWGROUP.store(a.s("pq_batch", "selection") == "rowgroup", std::sync::atomic::Ordering::Relaxed);
    pq::UNIT_PAGES.store(
        a.get("unit_pages", 2usize),
        std::sync::atomic::Ordering::Relaxed,
    );
    query::ONE_GET.store(
        a.get("one_get", 1u64 << 20),
        std::sync::atomic::Ordering::Relaxed,
    );
    let t0 = Instant::now();
    let w = match name.as_str() {
        "daily-scattered" => gen::daily(n, days, per_day, 0.01, false, seed),
        "daily-clustered" => gen::daily(n, days, per_day, 0.01, true, seed),
        "hot" => gen::hot(n, days, per_day, 0.01, 0.0005, seed),
        "rewrite" => gen::rewrite(
            n,
            a.get("days", 7),
            per_day,
            0.01,
            a.get("batch", (n / 100).max(1000)),
            seed,
        ),
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
    // A paused consumer holds commit 0: the base stays there, every diff from it answerable.
    let sch = sched::schedule(
        &counts,
        LOAD,
        |c| live[c as usize],
        per_day,
        7,
        Some(a.get("preserved", LOAD)),
    );
    let head = w.commits.len() as u64 - 1;
    eprintln!(
        "{name} {n}: {} commits, {} entries after the load, {} layers (max {}), merges wrote {:.2} entries per entry committed, generated in {:.1}s",
        head, sch.committed, sch.layers.len(), sch.max_layers, sch.merged as f64 / sch.committed as f64, t0.elapsed().as_secs_f64()
    );

    // Every layer in both formats, a few at a time.
    let store = Store::new();
    let threads: usize = a.get("threads", 8);
    let fmts: Vec<Fmt> = a.s("fmts", "ours,parquet").split(',').map(Fmt::parse).collect();
    let ranged = fmts.iter().any(|f| f.ranges()).then(|| {
        let t = Instant::now();
        let r = detect(&w, &oracle, a.get("min_run", 16), threads);
        let (n, k): (usize, usize) = (
            r.recs.iter().map(|v| v.len()).sum(),
            r.covered.iter().map(|v| v.iter().filter(|&&x| x).count()).sum(),
        );
        eprintln!(
            "range records: {n}, covering {k} entries, found in {:.1}s",
            t.elapsed().as_secs_f64()
        );
        r
    });
    let jobs: Vec<(usize, Fmt)> = (0..sch.layers.len())
        .flat_map(|i| fmts.iter().map(move |&f| (i, f)))
        .collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let built = std::sync::Mutex::new(Vec::new());
    let t1 = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| loop {
                let j = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(&(i, fmt)) = jobs.get(j) else { return };
                let spec = &sch.layers[i];
                let path = format!("{}/{i:03}-{}-{}", fmt.name(), spec.lo, spec.hi);
                let mut buf = Vec::with_capacity(48);
                let (layer, data) = if spec.base {
                    write(fmt, &a, path, spec, Vec::new(), &mut |push| {
                        for id in 0..oracle.space() {
                            let (v, c) = oracle.at(id, spec.hi);
                            if v != NONE {
                                gen::key_of(id, &mut buf);
                                push(&buf, c, Some(v), None);
                            }
                        }
                    })
                } else {
                    let r = ranged.as_ref().filter(|_| fmt.ranges());
                    let es = entries_of(&w, spec.lo, spec.hi, r);
                    let recs = r.map_or(Vec::new(), |r| r.recs(spec.lo, spec.hi));
                    write(fmt, &a, path, spec, recs, &mut |push| {
                        for e in &es {
                            gen::key_of(e.0, &mut buf);
                            push(&buf, e.1 as u64, opt(e.2), opt(e.3));
                        }
                    })
                };
                store.put(&layer.path, data);
                built.lock().unwrap().push(layer);
            });
        }
    });
    let mut built = built.into_inner().unwrap();
    built.sort_by_key(|l| (l.fmt.name(), l.lo));
    eprintln!("layers written in {:.1}s", t1.elapsed().as_secs_f64());
    drop(w); // the oracle answers from here on: room for 100M keys in 16 GB
    for &fmt in &fmts {
        let ls: Vec<&Layer> = built.iter().filter(|l| l.fmt == fmt).collect();
        let records: usize = ranged.as_ref().filter(|_| fmt.ranges()).map_or(0, |r| {
            ls.iter().filter(|l| !l.base).map(|l| r.recs(l.lo, l.hi).len()).sum()
        });
        let covered: usize = ranged.as_ref().filter(|_| fmt.ranges()).map_or(0, |r| {
            r.covered.iter().map(|v| v.iter().filter(|&&x| x).count()).sum()
        });
        let stored: u64 = ls.iter().map(|l| l.size).sum();
        let meta: u64 = ls.iter().map(|l| l.size - l.meta_start).sum();
        let base = ls.iter().find(|l| l.base).unwrap();
        println!(
            "{}",
            json!({"workload": name, "keys": n, "fmt": fmt.name(), "query": "stored", "layers": ls.len(),
                "stored_mb": stored as f64 / 1e6, "meta_mb": meta as f64 / 1e6, "base_mb": base.size as f64 / 1e6,
                "base_meta_mb": (base.size - base.meta_start) as f64 / 1e6,
                "bytes_per_entry": stored as f64 / ls.iter().map(|l| l.entries).sum::<u64>() as f64,
                "commits": head - LOAD, "committed": sch.committed, "merge_amp": sch.merged as f64 / sch.committed as f64,
                "max_layers": sch.max_layers, "folds": sch.folds, "records": records, "covered": covered,
                "layer_list": ls.iter().map(|l| format!("{}-{}:{}", l.lo, l.hi, l.entries)).collect::<Vec<_>>()})
        );
    }

    // Layer files on disk for DuckDB (duckdb_read.py): Parquet as is, ours also
    // exported to Arrow IPC, the export timed (what a native table function does).
    let dump = a.s("dump", "");
    if !dump.is_empty() {
        let dir = std::path::PathBuf::from(&dump);
        let mut manifest = Vec::new();
        for l in &built {
            let file = dir.join(&l.path);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(&file, store.whole(&l.path)).unwrap();
            let mut entry = json!({"fmt": l.fmt.name(), "path": l.path, "size": l.size, "lo": l.lo, "hi": l.hi, "base": l.base});
            if l.fmt == Fmt::Ours {
                let t = Instant::now();
                let ipc = dir.join(format!("{}.arrow", l.path));
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap();
                let s = store.view(None);
                let mut w = arrow_ipc::writer::FileWriter::try_new(
                    std::fs::File::create(&ipc).unwrap(),
                    &run::schema(),
                )
                .unwrap();
                let layer = Arc::new(l.clone());
                rt.block_on(async {
                    let mut st =
                        query::Stream::new(&s, layer, query::Pred::Range(None, None), false).await;
                    while let Some(r) = st.next_run().await {
                        w.write(&r.batch()).unwrap();
                    }
                });
                w.finish().unwrap();
                entry["export_s"] = json!(t.elapsed().as_secs_f64());
                entry["arrow"] = json!(format!("{}.arrow", l.path));
            }
            manifest.push(entry);
        }
        std::fs::write(dir.join("manifest.json"), serde_json::to_string_pretty(&json!({"workload": name, "keys": n, "head": head, "per_day": per_day, "load": LOAD, "layers": manifest})).unwrap()).unwrap();
        eprintln!("dumped to {dump}");
    }

    // The queries.
    let day = per_day;
    let space = oracle.space();
    let narrow = Some((space / 2, space / 2 + space / 100));
    let mut rng = <rand::rngs::SmallRng as rand::SeedableRng>::seed_from_u64(seed + 9);
    let mut probe: Vec<u32> = (0..1000)
        .map(|_| rand::Rng::random_range(&mut rng, 0..space))
        .collect();
    probe.sort_unstable();
    probe.dedup();
    let r0 = LOAD
        + if name == "rewrite" {
            a.get::<u64>("days", 7) * per_day
        } else {
            0
        };
    let mut qs: Vec<(&str, Q)> = vec![
        ("diff-1c", Q::Diff(head - 1, head, None, u64::MAX)),
        ("diff-1d-first10k", Q::Diff(head - day, head, None, 10_000)),
        ("diff-1d", Q::Diff(head - day, head, None, u64::MAX)),
        (
            "diff-1d-range1pct",
            Q::Diff(head - day, head, narrow, u64::MAX),
        ),
        ("diff-paused-first10k", Q::Diff(LOAD, head, None, 10_000)),
        ("diff-paused", Q::Diff(LOAD, head, None, u64::MAX)),
        (
            "diff-paused-range1pct",
            Q::Diff(LOAD, head, narrow, u64::MAX),
        ),
        ("scan-head", Q::Scan(head, None)),
        ("scan-head-range1pct", Q::Scan(head, narrow)),
        ("scan-1d-range1pct", Q::Scan(head - day, narrow)),
        ("get-1k", Q::Get(head, probe.clone())),
    ];
    if name == "rewrite" {
        qs.push(("diff-across-rewrite", Q::Diff(r0, head, None, u64::MAX)));
        qs.push((
            "diff-across-rewrite-first10k",
            Q::Diff(r0, head, None, 10_000),
        ));
    }
    let only = a.s("queries", "all");
    qs.retain(|(q, _)| only == "all" || only.split(',').any(|o| q.starts_with(o)));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let layers = |fmt: Fmt| -> Vec<Arc<Layer>> {
        built
            .iter()
            .filter(|l| l.fmt == fmt)
            .cloned()
            .map(Arc::new)
            .collect()
    };
    let by_fmt: Vec<(Fmt, Vec<Arc<Layer>>)> = fmts.iter().map(|&f| (f, layers(f))).collect();
    for (qn, q) in &qs {
        let want = expect(&oracle, q);
        for (fmt, ls) in &by_fmt {
            let fmt = *fmt;
            for (mode, net) in [("cold", Some(NET)), ("warm", None)] {
                let s = store.view(net);
                let (lo, hi);
                let (d, st, nl) = match q {
                    Q::Diff(c1, c2, r, limit) => {
                        lo = r.map(|r| key(r.0));
                        hi = r.map(|r| key(r.1));
                        let sel = query::diff_layers(ls, *c1, *c2, lo.as_deref(), hi.as_deref());
                        let older = query::older_layers(ls, *c1, lo.as_deref(), hi.as_deref());
                        let (d, st) = measure(
                            &rt,
                            &s,
                            query::diff(&s, &sel, &older, *c1, *c2, lo.as_deref(), hi.as_deref(), *limit),
                        );
                        (d, st, sel.len())
                    }
                    Q::Scan(c, r) => {
                        lo = r.map(|r| key(r.0));
                        hi = r.map(|r| key(r.1));
                        let sel = query::scan_layers(ls, *c, lo.as_deref(), hi.as_deref());
                        let (d, st) = measure(
                            &rt,
                            &s,
                            query::scan(&s, &sel, *c, lo.as_deref(), hi.as_deref()),
                        );
                        (d, st, sel.len())
                    }
                    Q::Get(c, ids) => {
                        let keys: Vec<Vec<u8>> = ids.iter().map(|&i| key(i)).collect();
                        let sel = query::scan_layers(ls, *c, None, None);
                        let (d, st) = measure(&rt, &s, query::get(&s, &sel, *c, &keys));
                        (d, st, sel.len())
                    }
                };
                emit(
                    &name,
                    n,
                    fmt,
                    qn,
                    mode,
                    d,
                    want,
                    &st,
                    json!({"layers_read": nl}),
                );
            }
        }
    }

    // Merges: the four newest layers, and folding every layer into a new base at the head.
    if a.get::<u32>("merges", 1) == 1 {
        let tmp = std::path::PathBuf::from(a.s("tmp", "/tmp/layer-format"));
        std::fs::create_dir_all(&tmp).unwrap();
        for (mn, fold) in [("merge-newest4", None), ("fold-into-base", Some(head))] {
            for (fmt, ls) in &by_fmt {
                let fmt = *fmt;
                let mut inputs: Vec<Arc<Layer>> = ls.iter().filter(|l| !l.base).cloned().collect();
                inputs.sort_by_key(|l| l.hi);
                if fold.is_none() {
                    inputs = inputs.split_off(inputs.len().saturating_sub(4));
                } else {
                    inputs.extend(ls.iter().filter(|l| l.base).cloned());
                }
                let (lo, hi) = (
                    inputs.iter().map(|l| l.lo).min().unwrap(),
                    inputs.iter().map(|l| l.hi).max().unwrap(),
                );
                let path = tmp.join(format!("{name}-{mn}-{}", fmt.name()));
                let file = std::io::BufWriter::with_capacity(
                    1 << 20,
                    std::fs::File::create(&path).unwrap(),
                );
                let s = store.view(Some(NET));
                let spec = sched::Spec {
                    lo,
                    hi,
                    base: fold.is_some(),
                    entries: 0,
                };
                let (written, st) = match fmt {
                    f if f.ours() => {
                        let mut wr = writer(f, &a, file);
                        let (recs, st) = measure(
                            &rt,
                            &s,
                            query::merge(&s, &inputs, fold, |k, e| wr.push(k, e.0, e.1, e.2)),
                        );
                        recs.into_iter().for_each(|r| wr.push_range(r));
                        let (_, size, ms, m2) = wr.finish();
                        ((size, ms, m2), st)
                    }
                    _ => {
                        let mut wr = pq::Writer::new(file, pq_conf(&a));
                        let (_, st) = measure(
                            &rt,
                            &s,
                            query::merge(&s, &inputs, fold, |k, e| wr.push(k, e.0, e.1, e.2)),
                        );
                        let (size, ms) = wr.finish();
                        ((size, ms, 0), st)
                    }
                };
                // Its output, read back, is every entry it should hold.
                let data = Bytes::from(std::fs::read(&path).unwrap());
                std::fs::remove_file(&path).ok();
                let meta2 = if fmt == Fmt::Pq {
                    pq::footer_start(&data)
                } else {
                    written.2
                };
                let out = Layer {
                    fmt,
                    path: format!("check/{mn}"),
                    size: written.0,
                    meta_start: written.1,
                    meta2,
                    lo,
                    hi,
                    base: false,
                    entries: 0,
                    first: vec![],
                    last: vec![],
                };
                store.put(&out.path, data);
                let check = store.view(None);
                let mut got = Digest::default();
                let mut kept: Vec<(u32, u64, u64, u64)> = Vec::new();
                let recs = rt.block_on(query::merge(&check, &[Arc::new(out)], None, |k, e| {
                    let e = (gen::id_of(k), e.0, e.1.unwrap_or(NONE), e.2.unwrap_or(NONE));
                    if fmt.ranges() && fold.is_none() {
                        kept.push(e);
                    } else {
                        got.add4(e.0, e.1, e.2, e.3);
                    }
                }));
                // Range records kept by the merge: spelled out against the fold, the
                // entries they stand for.
                if fmt.ranges() && fold.is_none() {
                    for r in &recs {
                        for id in gen::id_of(&r.lo)..=gen::id_of(&r.hi) {
                            let (v, _) = oracle.at(id, r.commit - 1);
                            if v != NONE {
                                kept.push((id, r.commit, r.new.unwrap_or(NONE), v));
                            }
                        }
                    }
                    kept.sort_unstable_by_key(|e| (e.0, std::cmp::Reverse(e.1)));
                    kept.iter().for_each(|e| got.add4(e.0, e.1, e.2, e.3));
                }
                let mut want = Digest::default();
                let mut es = Vec::new();
                for id in 0..oracle.space() {
                    match fold {
                        None => {
                            oracle.entries(id, lo, hi, &mut es);
                            for e in &es {
                                want.add4(id, e.0, e.1, e.2);
                            }
                        }
                        Some(c) => {
                            let (v, at) = oracle.at(id, c);
                            if v != NONE {
                                want.add4(id, at, v, NONE);
                            }
                        }
                    }
                }
                let _ = spec;
                emit(
                    &name,
                    n,
                    fmt,
                    mn,
                    "cold",
                    got,
                    want,
                    &st,
                    json!({"inputs": inputs.len(), "written_mb": written.0 as f64 / 1e6, "entries": got.n, "records_kept": recs.len()}),
                );
            }
        }
    }
}
