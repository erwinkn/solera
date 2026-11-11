//! The core's own cost of a bulk batch: one output commit of many keys, built
//! by `commit::build` from the transport form the worker hands over, and the
//! prior-version read inside it (one `get` per chunk at the head).
//!
//! `cargo bench -p solera-tracking --bench bulk -- <dir> setup <keys> <layers> [interleaved]`
//!   writes an output of <keys> keys `k000000000`.. under <dir>: a first run's
//!   clear of <keys>/<layers> keys, then <layers>-1 commits of as many new keys
//!   each, every one a key range after the last (a keyed first run in batches).
//!   With `interleaved` after <layers>, commit i writes every <layers>-th key
//!   from key i instead, so every layer spans the whole output (an output
//!   patched by scattered commits).
//! `cargo bench -p solera-tracking --bench bulk -- <dir> run <case> <keys> [reps]`
//!   builds one commit on that output and throws it away, <reps> times:
//!   - `empty`: <keys> new keys into an empty output (a first run's first batch)
//!   - `range`: <keys> keys in one range in the middle of the output, each updated
//!   - `append`: <keys> new keys after the output's last key
//!   - `scattered`: <keys> keys spread evenly over the output, each updated
//!   - `get-range`, `get-append`, `get-scattered`: the same keys' prior-version
//!     read only, chunked as `build` chunks it
//!
//! Prints wall and process CPU per rep. Run under `perf record` for where.

use object_store::local::LocalFileSystem;
use object_store::path::Path;
use solera_index::*;
use solera_tracking::commit::{self, Change, Changes, Mode, Reader, Source, Spec};
use solera_tracking::plan::OneIndex;
use solera_tracking::Output;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Instant;

/// The lock bench's keys, `k000000042`; with `KEYS=t44`, T44's 53-byte keys,
/// `site-0042/part-17/obj-000042170123-9f3c1a2e.json`. Both sort in id order.
fn key(id: u64) -> Vec<u8> {
    if std::env::var("KEYS").is_ok_and(|k| k == "t44") {
        let mut h = id ^ 0x5eed;
        h = h.wrapping_add(0x9E3779B97F4A7C15);
        h = (h ^ (h >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        h = (h ^ (h >> 27)).wrapping_mul(0x94D049BB133111EB);
        h ^= h >> 31;
        return format!(
            "site-{:04}/part-{:02}/obj-{id:012}-{:08x}.json",
            id / 1_000_000,
            (id / 10_000) % 100,
            h as u32
        )
        .into_bytes();
    }
    format!("k{id:09}").into_bytes()
}

fn cpu() -> f64 {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut t) };
    t.tv_sec as f64 + t.tv_nsec as f64 / 1e9
}

/// Keys `ids` as upserts without a version word, a chunk at a time.
struct Gen<I: Iterator<Item = u64>> {
    ids: I,
}

impl<I: Iterator<Item = u64>> Source for Gen<I> {
    fn next(&mut self, out: &mut Changes) -> solera_tracking::Result<bool> {
        out.clear();
        for id in self.ids.by_ref().take(65_536) {
            out.push(&key(id), Change::Upsert(None))?;
        }
        Ok(!out.is_empty())
    }
}

fn encode(ids: impl Iterator<Item = u64>) -> Vec<u8> {
    let mut c = Changes::new();
    for id in ids {
        c.push(&key(id), Change::Upsert(None)).unwrap();
    }
    c.encode()
}

fn hex(b: &[u8]) -> String {
    b.iter().fold(String::new(), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    })
}

fn unhex(h: &str) -> Vec<u8> {
    (0..h.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap())
        .collect()
}

fn save(dir: &str, out: &Output) {
    let mut s = format!("{} {}\n", out.head, out.live);
    for l in &out.layers {
        let clears: Vec<String> = l.clears.iter().map(|c| c.to_string()).collect();
        writeln!(
            s,
            "{} {} {} {} {} {} {} {} {} {} {} {} {}",
            l.path,
            l.size,
            l.index_start,
            l.top_start,
            l.lo,
            l.hi,
            l.floor.map_or("-".into(), |f| f.to_string()),
            l.entries,
            l.keys,
            hex(&l.first_key),
            hex(&l.last_key),
            l.flags,
            if clears.is_empty() {
                "-".into()
            } else {
                clears.join(",")
            }
        )
        .unwrap();
    }
    std::fs::write(format!("{dir}/output.txt"), s).unwrap();
}

fn load(dir: &str) -> Output {
    let s = std::fs::read_to_string(format!("{dir}/output.txt")).unwrap();
    let mut lines = s.lines();
    let mut hl = lines.next().unwrap().split(' ');
    let mut out = Output {
        head: hl.next().unwrap().parse().unwrap(),
        live: hl.next().unwrap().parse().unwrap(),
        ..Default::default()
    };
    for line in lines {
        let f: Vec<&str> = line.split(' ').collect();
        out.layers.push(LayerRef {
            path: f[0].into(),
            size: f[1].parse().unwrap(),
            index_start: f[2].parse().unwrap(),
            top_start: f[3].parse().unwrap(),
            lo: f[4].parse().unwrap(),
            hi: f[5].parse().unwrap(),
            floor: (f[6] != "-").then(|| f[6].parse().unwrap()),
            entries: f[7].parse().unwrap(),
            keys: f[8].parse().unwrap(),
            first_key: unhex(f[9]),
            last_key: unhex(f[10]),
            flags: f[11].parse().unwrap(),
            clears: if f[12] == "-" {
                Vec::new()
            } else {
                f[12].split(',').map(|c| c.parse().unwrap()).collect()
            },
        });
    }
    out
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let dir = args[0].as_str();
    std::fs::create_dir_all(dir).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let store = Store::new(
        Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap()),
        Path::default(),
        rt.handle().clone(),
        StoreOptions::default(),
    )
    .unwrap();
    let ix = ChangeIndex::new(store, WriteOptions::default());
    let ixs = OneIndex(&ix);
    let id: solera_tracking::Id = b"out".as_slice().into();
    let spec = |path: &'static str, mode: Mode, version: u64| Spec {
        version,
        time_ms: 0,
        producer: b"",
        path,
        mode,
        ends_full_run: false,
        collect: 0,
    };

    match args[1].as_str() {
        "setup" => {
            let n: u64 = args[2].parse().unwrap();
            let layers: u64 = args[3].parse().unwrap();
            assert!(
                layers > 0 && n % layers == 0,
                "setup takes a key count that the layer count divides"
            );
            let interleaved = args.get(4).is_some_and(|a| a == "interleaved");
            let per = n / layers;
            let mut out = Output::default();
            for i in 0..layers {
                let t = Instant::now();
                let path: &'static str = format!("c{}.layer", i + 1).leak();
                let mode = if i == 0 { Mode::Clear } else { Mode::Patch };
                let ids: Box<dyn Iterator<Item = u64>> = if interleaved {
                    Box::new((i..n).step_by(layers as usize))
                } else {
                    Box::new(i * per..(i + 1) * per)
                };
                let mut src = Gen { ids };
                let b = rt
                    .block_on(commit::build(
                        &ixs,
                        &id,
                        &out,
                        spec(path, mode, i + 1),
                        &mut src,
                    ))
                    .unwrap()
                    .unwrap();
                out.layers.push(b.part.layer);
                out.head += 1;
                out.live = b.part.live;
                eprintln!("commit {}: {:.1}s", i + 1, t.elapsed().as_secs_f64());
            }
            save(dir, &out);
            println!("output: {} keys in {} layers", out.live, out.layers.len());
        }
        "run" => {
            let case = args[2].as_str();
            let k: u64 = args[3].parse().unwrap();
            let reps: usize = args.get(4).map_or(3, |r| r.parse().unwrap());
            let out = if case == "empty" {
                Output::default()
            } else {
                load(dir)
            };
            let n = out.live;
            let data = match case.strip_prefix("get-").unwrap_or(case) {
                "empty" => encode(0..k),
                "range" => encode(n / 2..n / 2 + k),
                "append" => encode(n..n + k),
                "scattered" => encode((0..k).map(|i| i * (n / k))),
                _ => panic!("unknown case {case}"),
            };
            println!(
                "{case}: {k} keys, output of {n} keys in {} layers, changes {} bytes",
                out.layers.len(),
                data.len()
            );
            for rep in 0..reps {
                let (t, c) = (Instant::now(), cpu());
                let s0 = ix.store.stats.counts();
                let mut found = 0;
                if case.starts_with("get-") {
                    let mut src = Reader::new(&data).unwrap();
                    let mut ch = Changes::new();
                    while src.next(&mut ch).unwrap() {
                        let keys: Vec<&[u8]> = (0..ch.len()).map(|i| ch.key(i)).collect();
                        let p = rt
                            .block_on(ix.get(&out.layers, out.head, &keys, &Query::default()))
                            .unwrap();
                        found += p.run.len() as u64;
                    }
                } else {
                    let mut src = Reader::new(&data).unwrap();
                    let b = rt
                        .block_on(commit::build(
                            &ixs,
                            &id,
                            &out,
                            spec("bench.layer", Mode::Patch, out.head + 1),
                            &mut src,
                        ))
                        .unwrap()
                        .unwrap();
                    found = b.updated;
                    assert_eq!(b.added + b.updated, k);
                }
                let (wall, cpu_s) = (t.elapsed().as_secs_f64(), cpu() - c);
                let s1 = ix.store.stats.counts();
                println!(
                    "  rep {rep}: wall {wall:.3}s cpu {cpu_s:.3}s ({:.3} us/key) found {found} gets {} get_mb {:.1}",
                    cpu_s * 1e6 / k as f64,
                    s1.gets - s0.gets,
                    (s1.get_bytes - s0.get_bytes) as f64 / 1e6
                );
                let _ = std::fs::remove_file(format!("{dir}/bench.layer"));
            }
        }
        c => panic!("unknown command {c}"),
    }
}
