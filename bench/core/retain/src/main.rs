//! B9's measurement (N1): bytes stored per entry and entries merges rewrite
//! per entry committed, over days of 1% scattered change in 10,000-key
//! commits, with merges and cleanup driven by the retention core
//! (`solera_tracking::retain`): each merge is the one `retain::merge` picks
//! at the cut and is journaled as a `Merged` part, refused if it folds past
//! what the state holds; cleanup steps once a day and reads every version it
//! releases.
//!
//! `retain-bench <dir> <keys> <days> [per_day] [keep_runs]`: loads <keys> keys
//! in one commit, then <days> days of <per_day> commits (default 100) of
//! <keys>/100/<per_day> scattered keys each. A daily consumer catches up at
//! the end of each day; it keeps its last <keep_runs> runs' records (default
//! 1), each pinning the head it read at. Commits are written straight from
//! an oracle of each key's version (no reads), so the run measures what
//! retention costs, not what commits cost (that is B4's N7).

use object_store::local::LocalFileSystem;
use object_store::path::Path;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use solera_index::*;
use solera_tracking::event::{Event, Part};
use solera_tracking::retain::{self, Holder, Pin, Track};
use solera_tracking::{Id, Output, Tracking};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

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

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

fn cpu() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(u.ru_utime) + t(u.ru_stime)
}

fn peak_mb() -> f64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap();
    let l = s.lines().find(|l| l.starts_with("VmHWM")).unwrap();
    l.split_whitespace().nth(1).unwrap().parse::<f64>().unwrap() / 1024.0
}

fn ver(v: u32) -> Version<'static> {
    if v == 0 {
        Version::Absent
    } else {
        Version::Int(v as u64)
    }
}

/// Writes commit `c` of the sorted, distinct `ids`: each one upserted at
/// version `c`, or removed when `remove` says so, its replaced version from
/// the oracle.
fn write(
    rt: &tokio::runtime::Runtime,
    ix: &ChangeIndex,
    vers: &mut [u32],
    c: u64,
    live: &mut u64,
    ids: impl Iterator<Item = (u64, bool)>,
) -> LayerRef {
    let path = format!("c{c}.layer");
    let commit = NewCommit {
        commit: c,
        time_ms: 0,
        producer: b"bench",
        live_before: *live,
        clear: false,
    };
    let mut w = ix.commit_writer(&path, commit).unwrap();
    let mut k = Vec::new();
    rt.block_on(async {
        for (id, remove) in ids {
            let old = vers[id as usize];
            let new = if remove { 0 } else { c as u32 };
            if old == new {
                continue;
            }
            k.clear();
            key_of(id, &mut k);
            w.push(&k, ver(new), ver(old)).await.unwrap();
            vers[id as usize] = new;
            match (old, new) {
                (0, _) => *live += 1,
                (_, 0) => *live -= 1,
                _ => {}
            }
        }
        w.finish().await.unwrap().unwrap()
    })
}

fn apply(t: &mut Tracking, part: Part) {
    t.apply_event(Event::of(vec![part])).unwrap();
}

#[derive(Default)]
struct Totals {
    committed: u64,
    merges: u64,
    folds: u64,
    rewritten: u64,
    folded: u64,
    merge_s: f64,
    merge_cpu: f64,
    merge_max_s: f64,
    loop_s: f64,
    loop_max_s: f64,
    released: u64,
    clean_s: f64,
    max_layers: usize,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = &args[1];
    let n: u64 = args[2].parse().unwrap();
    let days: u64 = args[3].parse().unwrap();
    let per_day: u64 = args.get(4).map_or(100, |s| s.parse().unwrap());
    let keep_runs: usize = args.get(5).map_or(1, |s| s.parse().unwrap());
    let batch = (n / 100 / per_day).max(1);
    std::fs::create_dir_all(dir).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let raw = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let store = Store::new(raw, Path::default(), rt.handle().clone(), StoreOptions::default()).unwrap();
    let ix = ChangeIndex::new(store, WriteOptions::default());
    let o: Id = b"items".as_slice().into();
    let space = n + n / 10;
    let mut vers = vec![0u32; space as usize];
    let mut live = 0u64;

    let t0 = Instant::now();
    let base = write(&rt, &ix, &mut vers, 1, &mut live, (0..n).map(|i| (i, false)));
    println!(
        "load: keys={n} layer_mb={:.1} bytes_per_entry={:.2} took={:.1}s",
        base.size as f64 / 1e6,
        base.size as f64 / base.entries as f64,
        t0.elapsed().as_secs_f64()
    );
    let mut t = Tracking::new();
    t.outputs.insert(
        o.clone(),
        Output {
            layers: vec![base],
            head: 1,
            live,
            full_run: None,
        },
    );
    apply(&mut t, Part::Track(Track { output: o.clone(), at: 1 }));

    let mut runs: VecDeque<u64> = VecDeque::from([1]);
    let mut rng = SmallRng::seed_from_u64(7);
    let mut tot = Totals::default();
    let mut c = 1u64;
    let mut m = 0u64;
    for day in 1..=days {
        for _ in 0..per_day {
            c += 1;
            let mut ids: Vec<(u64, bool)> = (0..batch)
                .map(|_| {
                    let x: f64 = rng.random();
                    if x < 0.02 {
                        (n + rng.random_range(0..n / 10), false)
                    } else {
                        (rng.random_range(0..n), x < 0.05)
                    }
                })
                .collect();
            ids.sort_unstable_by_key(|p| p.0);
            ids.dedup_by_key(|p| p.0);
            let l = write(&rt, &ix, &mut vers, c, &mut live, ids.into_iter());
            tot.committed += l.entries;
            let out = t.outputs.get_mut(&o).unwrap();
            out.layers.push(l);
            out.head = c;
            out.live = live;

            let pins: Vec<Pin> = runs
                .iter()
                .map(|&h| Pin {
                    output: o.clone(),
                    commit: h,
                    by: Holder::Run(b"daily".as_slice().into()),
                })
                .collect();
            loop {
                let (l0, c0) = (Instant::now(), cpu());
                let cut = retain::cut(&t, &o, &pins).commit;
                let layers = t.outputs[&o].layers.clone();
                let mut loop_s = l0.elapsed().as_secs_f64();
                let path = {
                    m += 1;
                    format!("m{m}.layer")
                };
                let Some(merged) = rt.block_on(retain::merge(&ix, &o, &layers, cut, &path)).unwrap() else {
                    break;
                };
                let took = l0.elapsed().as_secs_f64() - loop_s;
                tot.merges += 1;
                if merged.layer.floor.is_some() {
                    tot.folds += 1;
                    tot.folded += merged.layer.entries;
                }
                tot.rewritten += merged.layer.entries;
                tot.merge_s += took;
                tot.merge_cpu += cpu() - c0;
                tot.merge_max_s = tot.merge_max_s.max(took);
                let a0 = Instant::now();
                apply(&mut t, Part::Merged(merged));
                loop_s += a0.elapsed().as_secs_f64();
                tot.loop_s += loop_s;
                tot.loop_max_s = tot.loop_max_s.max(loop_s);
            }
            tot.max_layers = tot.max_layers.max(t.outputs[&o].layers.len());
        }

        let s0 = Instant::now();
        if let Some(step) = retain::step(&t, &o, &[]) {
            let layers = t.outputs[&o].layers.clone();
            let mut after = None;
            loop {
                let page = rt
                    .block_on(retain::released(&ix, &layers, &step, after, 100_000))
                    .unwrap();
                tot.released += page.versions.len() as u64;
                after = page.next;
                if after.is_none() {
                    break;
                }
            }
            apply(&mut t, Part::Cleaned(step));
        }
        tot.clean_s += s0.elapsed().as_secs_f64();
        runs.push_back(c);
        while runs.len() > keep_runs {
            runs.pop_front();
        }

        let out = &t.outputs[&o];
        let bytes: u64 = out.layers.iter().map(|l| l.size).sum();
        let entries: u64 = out.layers.iter().map(|l| l.entries).sum();
        println!(
            "day {day}: head={c} layers={} stored_mb={:.1} entries={entries} bytes_per_entry={:.2} committed={} rewritten={} rewritten_per_committed={:.2} merges_alone_per_committed={:.2} folded={} merges={} folds={} merge_s={:.1} merge_cpu={:.1} merge_max_s={:.2} loop_ms_total={:.2} loop_ms_max={:.3} released={} clean_s={:.1} peak_rss_mb={:.0}",
            out.layers.len(),
            bytes as f64 / 1e6,
            bytes as f64 / entries as f64,
            tot.committed,
            tot.rewritten,
            tot.rewritten as f64 / tot.committed as f64,
            (tot.rewritten - tot.folded) as f64 / tot.committed as f64,
            tot.folded,
            tot.merges,
            tot.folds,
            tot.merge_s,
            tot.merge_cpu,
            tot.merge_max_s,
            tot.loop_s * 1e3,
            tot.loop_max_s * 1e3,
            tot.released,
            tot.clean_s,
            peak_mb()
        );
    }
    println!("max_layers={} total={:.0}s", tot.max_layers, t0.elapsed().as_secs_f64());
}
