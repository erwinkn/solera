//! B6's measurement: where a per-key asset's key outcomes are stored, for a
//! bad deploy that fails every key of a 100M-key input (handoffs/SOLE-13/
//! outcomes-store.md).
//!
//! Form A: index layers, the outcome record as the version word. Each failing
//! batch is one commit of the outcome output through B4's `build`, followed
//! by the size rule's merges with the cut at the head (nothing reads the
//! outcomes' history, so everything folds).
//! Form B: a store of its own in the engine's memory, records inline in the
//! journal: a sorted map in the core's state.
//!
//! `outcomes-bench a <dir> <keys>`: the bad deploy, then lookups, then the fix.
//! `outcomes-bench b <keys>`: form B's memory, snapshot and journal bytes,
//! and B3's failed points for the same keys.

use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::ObjectStore;
use solera_index::*;
use solera_tracking::codec::{Dec, Enc, Ver};
use solera_tracking::commit::{build, Change, Changes, Held, Mode, Spec};
use solera_tracking::plan::OneIndex;
use solera_tracking::progress::{Conditions, Holding, Point, Reason};
use solera_tracking::Output;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

const BATCH: u64 = 10_000;

/// T44's keys: `site-0042/part-17/obj-000042170123-9f3c1a2e.json`, 53 bytes, id order.
fn key_of(id: u64, out: &mut Vec<u8>) {
    use std::io::Write;
    let h = splitmix(id ^ 0x5eed) as u32;
    write!(
        out,
        "site-{:04}/part-{:02}/obj-{:012}-{:08x}.json",
        id / 1_000_000,
        (id / 10_000) % 100,
        id,
        h
    )
    .unwrap();
}

fn key(id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(53);
    key_of(id, &mut k);
    k
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

/// A failing key's record as the core would store it: the outcome and its
/// counters, what it failed on, what the consumer kept, the message.
fn record(id: u64, t: u64, out: &mut Vec<u8>) {
    let mut e = Enc::new();
    e.u8(2); // failed
    e.uv(1); // tries
    e.uv(7); // deploy
    e.uv(0); // forced
    e.uv(t); // since
    e.uv(t); // last
    e.uv(0); // next_at
    e.uv(0); // until
    e.version(Version::Int(2_000_000 + id % 1000)); // the version it failed on
    e.uv(1); // ... under conditions #1
    e.u8(2); // held: a version
    e.version(Version::Int(1_000_000 + id % 1000));
    e.uv(0); // ... under conditions #0
    e.bytes(b"KeyError: 'sample_id' (parsers.py:212, in read_results; columns: run, well, value)");
    out.clear();
    out.extend_from_slice(&e.buf);
}

fn cpu() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(u.ru_utime) + t(u.ru_stime)
}

fn status_mb(field: &str) -> f64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap();
    let l = s.lines().find(|l| l.starts_with(field)).unwrap();
    l.split_whitespace().nth(1).unwrap().parse::<f64>().unwrap() / 1024.0
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * p) as usize]
}

struct Env {
    rt: tokio::runtime::Runtime,
    raw: Arc<dyn ObjectStore>,
}

impl Env {
    fn index(&self) -> ChangeIndex {
        let store = Store::new(
            self.raw.clone(),
            Path::default(),
            self.rt.handle().clone(),
            StoreOptions::default(),
        )
        .unwrap();
        ChangeIndex::new(store, WriteOptions::default())
    }
}

#[derive(Default)]
struct Merges {
    n: u64,
    cpu: f64,
    wall: f64,
    bytes: u64,
    max_s: f64,
}

/// The size rule's merges, the cut at the head: everything may fold.
fn merge(env: &Env, ix: &ChangeIndex, out: &mut Output, m: &mut Merges, tag: &str) {
    while let Some(r) = pick_merge(&out.layers, out.head) {
        m.n += 1;
        let (t0, c0) = (Instant::now(), cpu());
        let path = format!("o/{tag}m{}.layer", m.n);
        let l = env
            .rt
            .block_on(ix.merge(&out.layers, r.clone(), out.head, &path))
            .unwrap();
        let s = t0.elapsed().as_secs_f64();
        m.wall += s;
        m.max_s = m.max_s.max(s);
        m.cpu += cpu() - c0;
        m.bytes += l.size;
        out.layers.splice(r, [l]);
    }
}

fn store_bytes(out: &Output) -> u64 {
    out.layers.iter().map(|l| l.size).sum()
}

fn save(dir: &str, out: &Output) {
    let mut e = Enc::new();
    e.uv(out.head);
    e.uv(out.live);
    e.uv(out.layers.len() as u64);
    for l in &out.layers {
        e.layer(l);
    }
    std::fs::write(format!("{dir}/outcomes.state"), e.buf).unwrap();
}

fn load(dir: &str) -> Output {
    let b = std::fs::read(format!("{dir}/outcomes.state")).unwrap();
    let mut d = Dec::new(&b);
    let head = d.uv().unwrap();
    let live = d.uv().unwrap();
    let layers = (0..d.uv().unwrap()).map(|_| d.layer().unwrap()).collect();
    Output { layers, head, live }
}

/// One commit of the outcome output: `ids` fail (a record each) or succeed
/// (their record goes).
fn commit(
    env: &Env,
    ix: &ChangeIndex,
    out: &mut Output,
    ids: &[u64],
    fail: bool,
) -> (f64, f64, u64) {
    let (t0, c0) = (Instant::now(), cpu());
    let mut ch = Changes::new();
    let mut k = Vec::new();
    let mut r = Vec::new();
    for &id in ids {
        k.clear();
        key_of(id, &mut k);
        if fail {
            record(id, 1_762_000_000 + out.head, &mut r);
            ch.push(&k, Change::Upsert(Some(&r))).unwrap();
        } else {
            ch.push(&k, Change::Removed).unwrap();
        }
    }
    let path = format!("o/c{}.layer", out.head + 1);
    let spec = Spec {
        version: 0,
        time_ms: 0,
        producer: b"",
        path: &path,
        mode: Mode::Patch,
        collect: 0,
    };
    let id: Box<[u8]> = b"\x00outcomes".as_slice().into();
    let built = env
        .rt
        .block_on(build(&OneIndex(ix), &id, out, spec, &mut Held(Some(&ch))))
        .unwrap();
    let mut size = 0;
    if let Some(b) = built {
        size = b.part.layer.size;
        out.layers.push(b.part.layer);
        out.head += 1;
        out.live = b.part.live;
    }
    (t0.elapsed().as_secs_f64(), cpu() - c0, size)
}

fn sweep(env: &Env, ix: &ChangeIndex, out: &mut Output, n: u64, fail: bool, dir: &str) {
    let (t0, c0) = (Instant::now(), cpu());
    let mut m = Merges::default();
    let (mut walls, mut cpus) = (vec![], vec![]);
    let mut puts = 0u64;
    let mut layer_bytes = 0u64;
    let mut max_layers = 0;
    let s0 = ix.store.stats.counts();
    let batches = n / BATCH;
    for b in 0..batches {
        let ids: Vec<u64> = (b * BATCH..(b + 1) * BATCH).collect();
        let (w, c, size) = commit(env, ix, out, &ids, fail);
        walls.push(w);
        cpus.push(c);
        puts += 1;
        layer_bytes += size;
        merge(env, ix, out, &mut m, if fail { "d" } else { "f" });
        max_layers = max_layers.max(out.layers.len());
        if (b + 1) % (batches / 10).max(1) == 0 {
            println!(
                "  {:>6}/{batches} batches: {:.0}s, live={} layers={} bytes={:.1}MB merges={} merge_cpu={:.1}s rss={:.0}MB",
                b + 1,
                t0.elapsed().as_secs_f64(),
                out.live,
                out.layers.len(),
                store_bytes(out) as f64 / 1e6,
                m.n,
                m.cpu,
                status_mb("VmRSS")
            );
        }
    }
    let s1 = ix.store.stats.counts();
    let total_cpu = cpu() - c0;
    println!(
        "{}: {batches} commits in {:.1}s wall, {:.1} CPU-s; commit p50 {:.1} ms (CPU {:.1} ms), p99 {:.1} ms; \
         merges {} ({:.1} CPU-s, {:.1}s wall, longest {:.1}s, {:.0} MB written); commit layers {:.0} MB; \
         store after: {} layers, {:.1} MB, live {}; {:.1} bytes per live record; \
         PUTs: {} commits + {} merges; GETs {} ({:.0} MB); peak RSS {:.0} MB",
        if fail { "deploy (every key fails)" } else { "fix (every key succeeds)" },
        t0.elapsed().as_secs_f64(),
        total_cpu,
        pct(&mut walls, 0.5) * 1e3,
        pct(&mut cpus, 0.5) * 1e3,
        pct(&mut walls, 0.99) * 1e3,
        m.n,
        m.cpu,
        m.wall,
        m.max_s,
        m.bytes as f64 / 1e6,
        layer_bytes as f64 / 1e6,
        out.layers.len(),
        store_bytes(out) as f64 / 1e6,
        out.live,
        store_bytes(out) as f64 / out.live.max(1) as f64,
        puts,
        m.n,
        s1.gets - s0.gets,
        (s1.get_bytes - s0.get_bytes) as f64 / 1e6,
        status_mb("VmHWM"),
    );
    save(dir, out);
}

/// What a plan pays to find the records of its candidates.
fn lookups(env: &Env, out: &Output, n: u64) {
    for (name, stride) in [
        ("10,000 keys in one range", 1u64),
        (
            "10,000 keys over 1% of the key space (a day's changes)",
            100,
        ),
        (
            "10,000 keys scattered over all of it (a retry pass in time order)",
            n / BATCH,
        ),
    ] {
        for warm in [false, true] {
            let ix = env.index();
            let rounds: usize = if warm { 6 } else { 1 };
            let mut times = vec![];
            let mut cpus = vec![];
            let s0 = ix.store.stats.counts();
            for round in 0..rounds {
                // warm: the same keys asked again, as a cache that holds them
                let base = if warm { 7 } else { 7 + round as u64 * 13 };
                let keys: Vec<Vec<u8>> = (0..BATCH)
                    .map(|i| key((base * 1_000_003 + i * stride) % n))
                    .collect();
                let mut keys = keys;
                keys.sort();
                let (t0, c0) = (Instant::now(), cpu());
                let page = env
                    .rt
                    .block_on(ix.get(&out.layers, out.head, &keys, &Query::default()))
                    .unwrap();
                assert_eq!(page.run.len() as u64, BATCH);
                times.push(t0.elapsed().as_secs_f64());
                cpus.push(cpu() - c0);
            }
            let s1 = ix.store.stats.counts();
            let k = if warm { 1 } else { 0 };
            println!(
                "lookup, {name}, {}: {:.1} ms wall, {:.1} ms CPU; GETs {} ({:.1} MB) over {rounds} rounds",
                if warm { "warm" } else { "cold" },
                times[k..].iter().sum::<f64>() / (rounds - k) as f64 * 1e3,
                cpus[k..].iter().sum::<f64>() / (rounds - k) as f64 * 1e3,
                s1.gets - s0.gets,
                (s1.get_bytes - s0.get_bytes) as f64 / 1e6,
            );
        }
    }
}

fn form_a(dir: &str, n: u64, phase: &str) {
    std::fs::create_dir_all(format!("{dir}/o")).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let env = Env {
        rt,
        raw: Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap()),
    };
    let _rt = env.rt.enter();
    let ix = env.index();
    match phase {
        "deploy" => {
            let mut out = Output::default();
            sweep(&env, &ix, &mut out, n, true, dir);
        }
        "lookup" => lookups(&env, &load(dir), n),
        "fix" => {
            let mut out = load(dir);
            sweep(&env, &ix, &mut out, n, false, dir);
        }
        _ => panic!("phase: deploy | lookup | fix"),
    }
}

fn form_b(n: u64) {
    let rss0 = status_mb("VmRSS");
    let (t0, c0) = (Instant::now(), cpu());
    let mut map: BTreeMap<Box<[u8]>, Box<[u8]>> = BTreeMap::new();
    let mut r = Vec::new();
    let mut journal = 0u64;
    for id in 0..n {
        record(id, 1_762_000_000, &mut r);
        let k = key(id);
        journal += (k.len() + r.len() + 2) as u64;
        map.insert(k.into(), r.as_slice().into());
    }
    let rss1 = status_mb("VmRSS");
    println!(
        "form B, {n} records in memory: {:.1}s, {:.1} CPU-s; RSS +{:.0} MB = {:.0} bytes a record; journal {:.0} MB ({:.0} bytes a record; a 10,000-key batch's event {:.0} KB)",
        t0.elapsed().as_secs_f64(),
        cpu() - c0,
        rss1 - rss0,
        (rss1 - rss0) * 1048576.0 / n as f64,
        journal as f64 / 1e6,
        journal as f64 / n as f64,
        journal as f64 / n as f64 * BATCH as f64 / 1e3,
    );
    let (t0, c0) = (Instant::now(), cpu());
    let mut e = Enc::new();
    for (k, v) in &map {
        e.bytes(k);
        e.bytes(v);
    }
    println!(
        "form B, snapshot of {n} records: {:.0} MB in {:.1}s ({:.1} CPU-s); peak RSS {:.0} MB",
        e.buf.len() as f64 / 1e6,
        t0.elapsed().as_secs_f64(),
        cpu() - c0,
        status_mb("VmHWM")
    );
}

fn points(n: u64) {
    let rss0 = status_mb("VmRSS");
    let old = Arc::new(Conditions::default());
    let mut points: BTreeMap<Box<[u8]>, Point> = BTreeMap::new();
    for id in 0..n {
        points.insert(
            key(id).into(),
            Point {
                holding: Holding::Version(Ver::Int(1_000_000 + id % 1000), old.clone()),
                reason: Reason::Failed,
                seen: Some(Ver::Int(2_000_000 + id % 1000)),
            },
        );
    }
    let rss1 = status_mb("VmRSS");
    println!(
        "B3's failed points, {n} in memory: RSS +{:.0} MB = {:.0} bytes a point",
        rss1 - rss0,
        (rss1 - rss0) * 1048576.0 / n as f64
    );
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    match a[1].as_str() {
        "a" => form_a(&a[2], a[3].parse().unwrap(), &a[4]),
        "b" => form_b(a[2].parse().unwrap()),
        "points" => points(a[2].parse().unwrap()),
        _ => panic!("a <dir> <keys> <phase> | b <keys> | points <keys>"),
    }
}
