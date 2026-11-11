//! B4's measurements (N7, N8): building an output commit on the change index
//! at 100M keys. A commit's entries are its written keys, sorted, joined with
//! one \`get\` of those keys at the head (their replaced versions), written
//! through the layer \`Writer\` and uploaded as it fills.
//!
//! \`commit-bench <dir> <keys> <days>\`: loads <keys> keys in one commit (N8, a
//! first listing), adds <days> daily commits of 1% scattered changes, measures
//! 10,000-key batches in one range and scattered (N7), cold and warm, then one
//! commit rewriting every key (N8, a rewrite).

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use solera_index::*;
use std::sync::Arc;
use std::time::Instant;

/// T44's keys: \`site-0042/part-17/obj-000042170123-9f3c1a2e.json\`, 53 bytes, id order.
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

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

/// A chunk of a write's keys, sorted: back to back, with removals marked.
#[derive(Default)]
struct Chunk {
    keys: Vec<u8>,
    ends: Vec<u32>,
    removed: Vec<bool>,
}

impl Chunk {
    fn clear(&mut self) {
        self.keys.clear();
        self.ends.clear();
        self.removed.clear();
    }
    fn push(&mut self, id: u64, removed: bool) {
        key_of(id, &mut self.keys);
        self.ends.push(self.keys.len() as u32);
        self.removed.push(removed);
    }
    fn key(&self, i: usize) -> &[u8] {
        let a = if i == 0 { 0 } else { self.ends[i - 1] as usize };
        &self.keys[a..self.ends[i] as usize]
    }
    fn len(&self) -> usize {
        self.ends.len()
    }
}

fn cpu() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(u.ru_utime) + t(u.ru_stime)
}

fn reset_peak() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

fn peak_mb() -> f64 {
    status_mb("VmHWM")
}

fn status_mb(field: &str) -> f64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap();
    let l = s.lines().find(|l| l.starts_with(field)).unwrap();
    l.split_whitespace().nth(1).unwrap().parse::<f64>().unwrap() / 1024.0
}

struct Env {
    rt: tokio::runtime::Runtime,
    raw: Arc<dyn ObjectStore>,
}

impl Env {
    fn index(&self, cold: bool) -> ChangeIndex {
        let _ = cold;
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

#[derive(Default, Debug, Clone, Copy)]
struct Took {
    get_s: f64,
    get_cpu: f64,
    build_cpu: f64,
    total_s: f64,
    total_cpu: f64,
    gets: u64,
    get_mb: f64,
    puts: u64,
    put_kb: f64,
    entries: u64,
}

/// Builds and writes one commit from chunks of sorted written keys: the
/// replaced versions from one \`get\` per chunk at the head, an entry per key
/// whose version changes, the layer uploaded as it fills (one PUT under 16 MiB).
fn commit(
    scan: bool,
    env: &Env,
    ix: &ChangeIndex,
    layers: &[LayerRef],
    commit: u64,
    gen: u64,
    path: &str,
    mut next: impl FnMut(&mut Chunk) -> bool,
) -> (Option<LayerRef>, Took) {
    let head = layers.last().map_or(0, |l| l.hi);
    let live_before = 0;
    let (t0, c0, s0) = (Instant::now(), cpu(), ix.store.stats.counts());
    let mut took = Took::default();
    let mut w = Writer::new(ix.write, commit, commit, None).unwrap();
    let mut h = CommitHeader {
        commit,
        ..Default::default()
    };
    let mut parts: Option<WriteMultipart> = None;
    let mut buf: Vec<u8> = Vec::new();
    let mut put_bytes = 0u64;
    let part = 16 << 20;
    let p = Path::from(path);
    let mut chunk = Chunk::default();
    let mut first: Option<Vec<u8>> = None;
    let mut last = Vec::new();
    let mut prev_hi: Option<Vec<u8>> = None;
    loop {
        chunk.clear();
        if !next(&mut chunk) {
            break;
        }
        let keys: Vec<&[u8]> = (0..chunk.len()).map(|i| chunk.key(i)).collect();
        let (g0, gc) = (Instant::now(), cpu());
        let page = if head == 0 {
            Page::default()
        } else if scan {
            let q = Query {
                lo: prev_hi.clone(),
                hi: Some(keys[keys.len() - 1].to_vec()),
                ..Default::default()
            };
            env.rt.block_on(ix.scan(layers, head, &q)).unwrap()
        } else {
            env.rt
                .block_on(ix.get(layers, head, &keys, &Query::default()))
                .unwrap()
        };
        prev_hi = Some(keys[keys.len() - 1].to_vec());
        took.get_s += g0.elapsed().as_secs_f64();
        let bc = cpu();
        took.get_cpu += bc - gc;
        let run = &page.run;
        let mut j = 0;
        for (i, k) in keys.iter().enumerate() {
            while j < run.len() && run.key(j) < *k {
                j += 1;
            }
            let old = if j < run.len() && run.key(j) == *k {
                run.new_version(run.rows(j).start)
            } else {
                Version::Absent
            };
            let new = if chunk.removed[i] {
                Version::Absent
            } else {
                Version::Int(gen)
            };
            if new == old {
                continue;
            }
            w.push(k, commit, new, old).unwrap();
            match (new.is_absent(), old.is_absent()) {
                (false, true) => h.added += 1,
                (false, false) => h.updated += 1,
                _ => h.removed += 1,
            }
            if first.is_none() {
                first = Some(k.to_vec());
            }
            last.clear();
            last.extend_from_slice(k);
        }
        took.build_cpu += cpu() - bc;
        if w.pending() >= part {
            let b = w.take();
            put_bytes += b.len() as u64;
            if parts.is_none() {
                let up = env.rt.block_on(env.raw.put_multipart(&p)).unwrap();
                parts = Some(WriteMultipart::new_with_chunk_size(up, part));
            }
            let wm = parts.as_mut().unwrap();
            wm.put(Bytes::from(b));
            env.rt.block_on(wm.wait_for_capacity(4)).unwrap();
        }
    }
    if h.added + h.updated + h.removed == 0 {
        return (None, took);
    }
    h.live = live_before + h.added - h.removed.min(live_before + h.added);
    h.min_key = first.unwrap();
    h.max_key = last;
    let fc = cpu();
    let mut l = w.finish(&[h]).unwrap();
    let b = w.take();
    put_bytes += b.len() as u64;
    took.build_cpu += cpu() - fc;
    match parts {
        None => {
            buf.extend_from_slice(&b);
            env.rt
                .block_on(env.raw.put(&p, PutPayload::from(buf)))
                .unwrap();
        }
        Some(mut wm) => {
            wm.put(Bytes::from(b));
            env.rt.block_on(wm.finish()).unwrap();
        }
    }
    l.path = path.to_string();
    let s1 = ix.store.stats.counts();
    took.total_s = t0.elapsed().as_secs_f64();
    took.total_cpu = cpu() - c0;
    took.gets = s1.gets - s0.gets;
    took.get_mb = (s1.get_bytes - s0.get_bytes) as f64 / 1e6;
    took.puts = 1;
    took.put_kb = put_bytes as f64 / 1e3;
    took.entries = l.entries;
    (Some(l), took)
}

fn report(name: &str, t: &Took, layers: usize) {
    println!(
        "{name}: entries={} layers={layers} total={:.3}s cpu={:.3}s get={:.3}s get_cpu={:.3}s build_cpu={:.3}s gets={} get_mb={:.2} put_kb={:.1}",
        t.entries, t.total_s, t.total_cpu, t.get_s, t.get_cpu, t.build_cpu, t.gets, t.get_mb, t.put_kb
    );
}

fn merge_all(env: &Env, ix: &ChangeIndex, layers: &mut Vec<LayerRef>, cut: u64, n: &mut u64) {
    while let Some(r) = pick_merge(layers, cut) {
        if r.start == 0 {
            break;
        }
        *n += 1;
        let path = format!("m{n}.layer");
        let l = env
            .rt
            .block_on(ix.merge(layers, r.clone(), cut, &path))
            .unwrap();
        layers.splice(r, [l]);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = &args[1];
    let n: u64 = args[2].parse().unwrap();
    let days: u64 = args[3].parse().unwrap();
    let only: Option<&str> = args.get(4).map(|s| s.as_str());
    std::fs::create_dir_all(dir).unwrap();
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
    if let Some(how) = only.and_then(|o| o.strip_prefix("core-")) {
        let layers = chain(dir);
        core_rewrite(&env, &layers, n, how);
        return;
    }
    if let Some(how) = only.and_then(|o| o.strip_prefix("rewrite-only-")) {
        let mut layers = chain(dir);
        let c = layers.last().unwrap().hi + 1;
        rewrite(&env, &mut layers, c, n, how == "scan");
        return;
    }
    let ix = env.index(false);
    let mut layers: Vec<LayerRef> = Vec::new();
    let mut c = 0u64;
    let mut merges = 0u64;
    let chunk_keys = 65_536u64;

    // N8: a first listing of n keys, streamed.
    reset_peak();
    c += 1;
    let mut id = 0u64;
    let (l, t) = commit(
        false,
        &env,
        &ix,
        &layers,
        c,
        1_000_000 + c,
        "c1.layer",
        |ch| {
            if id >= n {
                return false;
            }
            let end = (id + chunk_keys).min(n);
            for i in id..end {
                ch.push(i, false);
            }
            id = end;
            true
        },
    );
    report("N8 first listing", &t, 0);
    println!(
        "N8 first listing: peak_rss_mb={:.0} layer_mb={:.1}",
        peak_mb(),
        l.as_ref().unwrap().size as f64 / 1e6
    );
    layers.push(l.unwrap());

    // Days of 1% scattered change: updates, a few removals and adds.
    let mut rng = SmallRng::seed_from_u64(7);
    for _ in 0..days {
        c += 1;
        let mut ids: Vec<(u64, bool)> = (0..n / 100)
            .map(|_| {
                let x: f64 = rng.random();
                if x < 0.02 {
                    (n + rng.random_range(0..n / 10), false)
                } else {
                    (rng.random_range(0..n), x < 0.05)
                }
            })
            .collect();
        ids.sort_unstable();
        ids.dedup_by_key(|x| x.0);
        let mut at = 0;
        let (l, t) = commit(
            false,
            &env,
            &ix,
            &layers,
            c,
            1_000_000 + c,
            &format!("c{c}.layer"),
            |ch| {
                if at >= ids.len() {
                    return false;
                }
                let end = (at + chunk_keys as usize).min(ids.len());
                for &(i, r) in &ids[at..end] {
                    ch.push(i, r);
                }
                at = end;
                true
            },
        );
        report(&format!("day {c}"), &t, layers.len());
        layers.push(l.unwrap());
        merge_all(&env, &ix, &mut layers, 1, &mut merges);
    }
    println!(
        "layers after the days: {} sizes_mb={:?}",
        layers.len(),
        layers
            .iter()
            .map(|l| (l.size as f64 / 1e6).round())
            .collect::<Vec<_>>()
    );

    // N7: 10,000-key batches, in one range and scattered, cold and warm.
    let batch = 10_000u64;
    for kind in ["range", "scattered"] {
        if only.is_some_and(|o| o != kind && o != "batches") {
            continue;
        }
        for (mode, rounds) in [("cold", 3), ("warm", 12)] {
            let ixm = if mode == "cold" {
                None
            } else {
                Some(env.index(false))
            };
            for r in 0..rounds {
                let fresh;
                let ixr = match &ixm {
                    Some(i) => i,
                    None => {
                        fresh = env.index(true);
                        &fresh
                    }
                };
                let ids: Vec<u64> = if kind == "range" {
                    let s = rng.random_range(0..n - batch);
                    (s..s + batch).collect()
                } else {
                    let mut v: Vec<u64> = (0..batch).map(|_| rng.random_range(0..n)).collect();
                    v.sort_unstable();
                    v.dedup();
                    v
                };
                c += 1;
                let (l, t) = core_commit(&env, ixr, &layers, c, &ids);
                report(&format!("N7 {kind} {mode} #{r}"), &t, layers.len());
                layers.push(l);
                merge_all(&env, &ix, &mut layers, 1, &mut merges);
            }
        }
    }

    if only.is_some_and(|o| o != "rewrite") {
        return;
    }
    // N8: one commit rewriting every key, streamed, on a cold index: the
    // replaced versions by `get` of each chunk, or by `scan` over its span.
    drop(ix);
    for scan in [true, false] {
        let cold = env.index(false);
        let base = status_mb("VmRSS");
        reset_peak();
        c += 1;
        let mut id = 0u64;
        let (l, t) = commit(
            scan,
            &env,
            &cold,
            &layers,
            c,
            1_000_000 + c,
            &format!("c{c}.layer"),
            |ch| {
                if id >= n {
                    return false;
                }
                let end = (id + chunk_keys).min(n);
                for i in id..end {
                    ch.push(i, false);
                }
                id = end;
                true
            },
        );
        let how = if scan { "scan" } else { "get" };
        report(&format!("N8 rewrite by {how}"), &t, layers.len());
        println!(
            "N8 rewrite by {how}: rss_before_mb={base:.0} peak_rss_mb={:.0} layer_mb={:.1}",
            peak_mb(),
            l.as_ref().unwrap().size as f64 / 1e6
        );
        layers.push(l.unwrap());
    }
    // Generating the keys alone, to subtract from the totals above.
    let c0 = cpu();
    let mut ch = Chunk::default();
    for a in (0..n).step_by(chunk_keys as usize) {
        ch.clear();
        for i in a..(a + chunk_keys).min(n) {
            ch.push(i, false);
        }
    }
    println!("key generation alone: cpu={:.3}s for {n} keys", cpu() - c0);
}

/// N8 in a fresh process: one commit rewriting every key, on a cold index.
fn rewrite(env: &Env, layers: &mut Vec<LayerRef>, c: u64, n: u64, scan: bool) {
    let cold = env.index(false);
    let base = status_mb("VmRSS");
    let mut id = 0u64;
    let chunk_keys = 65_536u64;
    let (l, t) = commit(
        scan,
        env,
        &cold,
        layers,
        c,
        1_000_000 + c,
        &format!("r{c}.layer"),
        |ch| {
            if id >= n {
                return false;
            }
            let end = (id + chunk_keys).min(n);
            for i in id..end {
                ch.push(i, false);
            }
            id = end;
            true
        },
    );
    let how = if scan { "scan" } else { "get" };
    report(
        &format!("N8 rewrite by {how} (fresh process)"),
        &t,
        layers.len(),
    );
    println!(
        "N8 rewrite by {how} (fresh process): rss_before_mb={base:.0} peak_rss_mb={:.0} layer_mb={:.1}",
        peak_mb(),
        l.as_ref().unwrap().size as f64 / 1e6
    );
}

/// The layer list from the files in \`dir\`: from commit 1, each step the
/// layer starting there that reaches furthest (a merge replaced its inputs).
fn chain(dir: &str) -> Vec<LayerRef> {
    let mut all = Vec::new();
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        let name = p.file_name().unwrap().to_str().unwrap().to_string();
        if !name.ends_with(".layer") || name.starts_with('r') || name.starts_with("core-") {
            continue;
        }
        let f = std::fs::read(&p).unwrap();
        let t = &f[f.len() - 48..];
        let top_start = u64::from_le_bytes(t[16..24].try_into().unwrap());
        let top =
            solera_index::format::Top::parse(&f[top_start as usize..], top_start, f.len() as u64)
                .unwrap();
        all.push(LayerRef {
            path: name,
            size: f.len() as u64,
            index_start: top.index_start,
            top_start,
            lo: top.lo,
            hi: top.hi,
            floor: top.floor,
            entries: top.entries,
            keys: top.keys,
            first_key: top.first_key.clone(),
            last_key: top.last_key.clone(),
            flags: top.flags,
            clears: Vec::new(),
        });
    }
    let mut out: Vec<LayerRef> = Vec::new();
    let mut next = 1;
    while let Some(l) = all.iter().filter(|l| l.lo == next).max_by_key(|l| l.hi) {
        next = l.hi + 1;
        out.push(l.clone());
    }
    println!("chain: {} layers up to commit {}", out.len(), next - 1);
    out
}

/// N7 through the core's own builder (`solera_tracking::commit::build`).
fn core_commit(
    env: &Env,
    ix: &ChangeIndex,
    layers: &[LayerRef],
    c: u64,
    ids: &[u64],
) -> (LayerRef, Took) {
    use solera_tracking::commit::{build, Change, Changes, Held, Mode, Spec};
    let mut ch = Changes::new();
    let mut k = Vec::new();
    for &i in ids {
        k.clear();
        key_of(i, &mut k);
        ch.push(&k, Change::Upsert(None)).unwrap();
    }
    let out = solera_tracking::Output {
        life: 1,
        layers: layers.to_vec(),
        head: c - 1,
        live: 0,
        full_run: None,
    };
    let path = format!("c{c}.layer");
    let spec = Spec {
        version: 1_000_000 + c,
        time_ms: 0,
        producer: b"bench",
        path: &path,
        mode: Mode::Patch,
        ends_full_run: false,
        collect: 0,
    };
    let (t0, c0, s0) = (Instant::now(), cpu(), ix.store.stats.counts());
    let id: solera_tracking::Id = b"o".as_slice().into();
    let built = env
        .rt
        .block_on(build(
            &solera_tracking::plan::OneIndex(ix),
            &id,
            &out,
            spec,
            &mut Held(Some(&ch)),
        ))
        .unwrap()
        .unwrap();
    let s1 = ix.store.stats.counts();
    let t = Took {
        total_s: t0.elapsed().as_secs_f64(),
        total_cpu: cpu() - c0,
        gets: s1.gets - s0.gets,
        get_mb: (s1.get_bytes - s0.get_bytes) as f64 / 1e6,
        puts: s1.puts - s0.puts,
        put_kb: (s1.put_bytes - s0.put_bytes) as f64 / 1e3,
        entries: built.part.layer.entries,
        ..Default::default()
    };
    (built.part.layer, t)
}

/// Generated changed keys, a chunk at a time, as a worker's file would hand them over.
struct Gen {
    id: u64,
    n: u64,
}

impl solera_tracking::commit::Source for Gen {
    fn next(
        &mut self,
        out: &mut solera_tracking::commit::Changes,
    ) -> solera_tracking::Result<bool> {
        use solera_tracking::commit::Change;
        out.clear();
        if self.id >= self.n {
            return Ok(false);
        }
        let end = (self.id + 65_536).min(self.n);
        let mut k = Vec::new();
        for i in self.id..end {
            k.clear();
            key_of(i, &mut k);
            out.push(&k, Change::Upsert(None))?;
        }
        self.id = end;
        Ok(true)
    }
}

/// N8 through the core's builder: one commit of every key, `first` on an
/// empty output (a first listing), `patch` or `replace` over the index in `dir`.
fn core_rewrite(env: &Env, layers: &[LayerRef], n: u64, how: &str) {
    use solera_tracking::commit::{build, Mode, Spec};
    let ix = env.index(false);
    let (out, mode) = match how {
        "first" => (solera_tracking::Output::default(), Mode::Patch),
        "patch" | "replace" => (
            solera_tracking::Output {
                life: 1,
                layers: layers.to_vec(),
                head: layers.last().unwrap().hi,
                live: 0,
                full_run: None,
            },
            if how == "patch" {
                Mode::Patch
            } else {
                Mode::Replace
            },
        ),
        _ => panic!("core-first, core-patch or core-replace"),
    };
    let path = format!("core-{how}.layer");
    let spec = Spec {
        version: 9_000_000,
        time_ms: 0,
        producer: b"bench",
        path: &path,
        mode,
        ends_full_run: false,
        collect: 0,
    };
    let base = status_mb("VmRSS");
    reset_peak();
    let (t0, c0) = (Instant::now(), cpu());
    let id: solera_tracking::Id = b"o".as_slice().into();
    let built = env
        .rt
        .block_on(build(
            &solera_tracking::plan::OneIndex(&ix),
            &id,
            &out,
            spec,
            &mut Gen { id: 0, n },
        ))
        .unwrap()
        .unwrap();
    let s = ix.store.stats.counts();
    println!(
        "N8 core {how}: entries={} wall={:.1}s cpu={:.1}s gets={} get_mb={:.0} put_mb={:.0} rss_before_mb={base:.0} peak_rss_mb={:.0}",
        built.part.layer.entries,
        t0.elapsed().as_secs_f64(),
        cpu() - c0,
        s.gets,
        s.get_bytes as f64 / 1e6,
        s.put_bytes as f64 / 1e6,
        peak_mb()
    );
    let _ = std::fs::remove_file(format!("{}/{path}", std::env::args().nth(1).unwrap()));
}
