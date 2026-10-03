//! Differential test and throughput of the Bend-proven delta merge
//! (../delta/main.bend, compiled to C as ../delta/run.bend) against Rust:
//! the native newest-wins `stream::Merge` over real `.kx` files, and a
//! line-for-line Rust transcription of the Bend merge (the existed-before bit
//! has no Rust implementation yet: K44 is waiting).
//!
//!   bend-harness RUN diff SEEDS     random histories, every output compared
//!   bend-harness RUN bench C N K     C commits of N entries over K keys

use std::collections::BTreeMap;
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use solera_native::format::{file_blocks, Options, CODEC_NONE};
use solera_native::stream::{Bytes, Merge, Next, Segment, Writer};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Info {
    gen: u64,
    del: bool,
    bef: bool,
}

type Delta = Vec<(u64, Info)>;

extern "C" {
    fn clock() -> std::ffi::c_ulong;
}

/// Process CPU seconds (macOS: CLOCKS_PER_SEC is 1e6). The machine is shared
/// and loaded, so CPU time, not wall time, is what the runs compare.
fn cpu() -> f64 {
    unsafe { clock() as f64 / 1e6 }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// A consistent history: each commit's entries say whether their key was
/// live before it. Keys are spread over 48 bits; generations use all 64.
fn history(rng: &mut Rng, commits: usize, per: usize, keys: u64) -> (Vec<Delta>, BTreeMap<u64, bool>) {
    let stride = ((1u64 << 48) - 1) / keys.max(1);
    let mut live: BTreeMap<u64, bool> = BTreeMap::new();
    for _ in 0..keys / 2 {
        live.insert(rng.below(keys) * stride, true);
    }
    let start = live.clone();
    let mut out = Vec::new();
    for c in 0..commits {
        let gen = match c % 3 {
            0 => u64::MAX - (commits - c) as u64,
            1 => ((c as u64 + 1) << 32) | 0xffff_fff0,
            _ => c as u64 + 1,
        };
        let mut ks: Vec<u64> = (0..rng.below(per as u64 + 1)).map(|_| rng.below(keys) * stride).collect();
        ks.sort_unstable();
        ks.dedup();
        let d: Delta = ks
            .into_iter()
            .map(|k| {
                let bef = *live.get(&k).unwrap_or(&false);
                let del = rng.below(3) == 0;
                live.insert(k, !del);
                (k, Info { gen, del, bef })
            })
            .collect();
        out.push(d);
    }
    (out, start)
}

/// The Bend merge, transcribed: the older entry's "before", the newer's state.
fn merge(a: &[(u64, Info)], b: &[(u64, Info)]) -> Delta {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        let (ka, ia) = a[i];
        let (kb, ib) = b[j];
        if ka < kb {
            out.push((ka, ia));
            i += 1;
        } else if ka > kb {
            out.push((kb, ib));
            j += 1;
        } else {
            out.push((ka, Info { bef: ia.bef, ..ib }));
            i += 1;
            j += 1;
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

fn summary(cs: &[Delta]) -> Delta {
    cs.iter().fold(Vec::new(), |acc, c| merge(&acc, c))
}

/// The balanced tree of merges, as `tree` in main.bend (sequential here).
fn tree(cs: &[Delta]) -> Delta {
    match cs.len() {
        0 => Vec::new(),
        1 => cs[0].clone(),
        n => merge(&tree(&cs[..n / 2]), &tree(&cs[n / 2..])),
    }
}

fn key_bytes(k: u64) -> [u8; 6] {
    let b = k.to_be_bytes();
    [b[2], b[3], b[4], b[5], b[6], b[7]]
}

const O: Options = Options { block_size: 65536, level: 1, bits_per_item: 14, k: 10, codec: CODEC_NONE };

fn kx(d: &Delta) -> Vec<u8> {
    let mut w = Writer::new(O, usize::MAX);
    for (k, i) in d {
        let pre = i.bef.then_some(0);
        w.push(&key_bytes(*k), i.gen, i.del, None, pre).unwrap();
    }
    w.finish(true).unwrap();
    assert_eq!(w.files.len(), 1);
    w.files.pop_front().unwrap()
}

/// The native newest-wins merge over the commits' `.kx` files.
fn native(files: &[Vec<u8>]) -> Vec<(u64, u64, bool)> {
    let mut m = Merge::new(files.len());
    // Newest first.
    for (r, f) in files.iter().rev().enumerate() {
        let (codec, blocks) = file_blocks(f).unwrap();
        let data: Bytes = Arc::new(f.clone());
        m.runs[r].feed(Segment {
            data,
            blocks: blocks.iter().map(|b| (b.offset as usize, b.size as usize, b.crc)).collect(),
            codec,
        });
        m.runs[r].end();
    }
    let mut out = Vec::new();
    loop {
        match m.next_key().unwrap() {
            Next::Entry => {
                let mut b = [0u8; 8];
                b[2..].copy_from_slice(m.key());
                out.push((u64::from_be_bytes(b), m.generation(), m.deleted()));
            }
            Next::Need(_) => unreachable!(),
            Next::End => break,
        }
    }
    out
}

fn words(cs: &[Delta]) -> Vec<u8> {
    let mut w: Vec<u32> = vec![cs.len() as u32];
    for c in cs {
        w.push(c.len() as u32);
        for (k, i) in c.iter().rev() {
            w.extend([(k >> 32) as u32, *k as u32, (i.gen >> 32) as u32, i.gen as u32, i.del as u32 | (i.bef as u32) << 1]);
        }
    }
    w.iter().flat_map(|x| x.to_le_bytes()).collect()
}

/// Runs the Bend binary: the summary, the CPU microseconds its merge took,
/// and the merge's wall seconds.
fn bend(run: &str, cs: &[Delta], tag: &str, mode: &str, threads: u32) -> (Delta, u64, f64) {
    let inp = format!("/tmp/bendx/{tag}.in");
    let out = format!("/tmp/bendx/{tag}.out");
    std::fs::write(&inp, words(cs)).unwrap();
    let t = Instant::now();
    let threads = threads.to_string();
    let st = Command::new(run).args(["--threads", &threads, &inp, &out, mode]).output().unwrap();
    let wall = t.elapsed().as_secs_f64();
    assert!(st.status.success(), "bend failed: {}", String::from_utf8_lossy(&st.stderr));
    // The merge's own wall time, from stderr ("merge wall us N, cpu us M").
    let err = String::from_utf8_lossy(&st.stderr);
    let merge_wall = err
        .split("merge wall us ")
        .nth(1)
        .and_then(|r| r.split(',').next())
        .and_then(|n| n.trim().parse::<f64>().ok())
        .unwrap_or(f64::NAN)
        / 1e6;
    let raw = std::fs::read(&out).unwrap();
    let w: Vec<u32> = raw.chunks(4).map(|c| u32::from_le_bytes(c.try_into().unwrap())).collect();
    let d = w[1..]
        .chunks(5)
        .map(|e| {
            let k = (e[0] as u64) << 32 | e[1] as u64;
            let gen = (e[2] as u64) << 32 | e[3] as u64;
            (k, Info { gen, del: e[4] & 1 != 0, bef: e[4] & 2 != 0 })
        })
        .collect();
    let _ = wall;
    (d, w[0] as u64, merge_wall)
}

fn diff(run: &str, seeds: u64) {
    let mut entries = 0;
    for seed in 0..seeds {
        let mut rng = Rng(seed);
        let commits = 1 + rng.below(12) as usize;
        let per = rng.below(200) as usize;
        let keys = 1 + rng.below(400);
        let (cs, start) = history(&mut rng, commits, per, keys);
        let (got, _, _) = bend(run, &cs, "diff", "fold", 1);
        let want = summary(&cs);
        assert_eq!(got, want, "seed {seed}: Bend and the Rust transcription differ");
        let (got_tree, _, _) = bend(run, &cs, "diff", "tree", 2);
        assert_eq!(got_tree, want, "seed {seed}: Bend's parallel tree differs");
        let files: Vec<Vec<u8>> = cs.iter().map(kx).collect();
        let nat = native(&files);
        let got3: Vec<(u64, u64, bool)> = got.iter().map(|(k, i)| (*k, i.gen, i.del)).collect();
        assert_eq!(got3, nat, "seed {seed}: Bend and stream::Merge differ");
        // Law (c) on the data: before is the presence at the start, the state at the end.
        let mut end = start.clone();
        for c in &cs {
            for (k, i) in c {
                end.insert(*k, !i.del);
            }
        }
        for (k, i) in &got {
            assert_eq!(i.bef, *start.get(k).unwrap_or(&false), "seed {seed}: before of {k}");
            assert_eq!(!i.del, end[k], "seed {seed}: state of {k}");
        }
        entries += got.len();
    }
    println!("diff: {seeds} histories, {entries} summary entries, all equal (Bend fold and parallel tree, transcription, stream::Merge, law c)");
}

fn best_bend(run: &str, cs: &[Delta], mode: &str, threads: u32, want: &Delta) -> (f64, f64) {
    let (mut cpu_s, mut wall) = (f64::MAX, f64::MAX);
    for _ in 0..5 {
        let (got, us, w) = bend(run, cs, "bench", mode, threads);
        assert_eq!(&got, want);
        cpu_s = cpu_s.min(us as f64 / 1e6);
        wall = wall.min(w);
    }
    (cpu_s, wall)
}

fn bench(run: &str, commits: usize, per: usize, keys: u64) {
    let mut rng = Rng(42);
    let (cs, _) = history(&mut rng, commits, per, keys);
    let input: usize = cs.iter().map(Vec::len).sum();
    let reps = 5;
    let rust = |f: &dyn Fn(&[Delta]) -> Delta| {
        let mut best = f64::MAX;
        let mut out = Vec::new();
        for _ in 0..reps {
            let t = cpu();
            out = f(&cs);
            best = best.min(cpu() - t);
        }
        (out, best)
    };
    let (want, fold_s) = rust(&summary);
    let (want_tree, tree_s) = rust(&tree);
    assert_eq!(want, want_tree);

    let files: Vec<Vec<u8>> = cs.iter().map(kx).collect();
    let (mut nbest, mut nwall) = (f64::MAX, f64::MAX);
    for _ in 0..reps {
        let (t, w) = (cpu(), Instant::now());
        let n = native(&files);
        nbest = nbest.min(cpu() - t);
        nwall = nwall.min(w.elapsed().as_secs_f64());
        assert_eq!(n.len(), want.len());
    }
    let rate = |s: f64| input as f64 / s / 1e6;
    println!("bench: {commits} commits x <= {per} entries over {keys} keys: {input} entries in, {} out; CPU time, best of {reps}", want.len());
    let (b, w) = best_bend(run, &cs, "fold", 1, &want);
    println!("  bend  fold, 1 thread (C, + checksum):  {:8.1} ms  {:7.2} M entries/s  ({:.1} ms wall)", b * 1e3, rate(b), w * 1e3);
    let (b, w) = best_bend(run, &cs, "tree", 1, &want);
    println!("  bend  tree, 1 thread:                  {:8.1} ms  {:7.2} M entries/s  ({:.1} ms wall)", b * 1e3, rate(b), w * 1e3);
    let (b, w) = best_bend(run, &cs, "tree", 2, &want);
    println!("  bend  tree, 2 threads (CPU of both):   {:8.1} ms  {:7.2} M entries/s  ({:.1} ms wall)", b * 1e3, rate(b), w * 1e3);
    println!("  rust  fold (transcription):            {:8.1} ms  {:7.2} M entries/s", fold_s * 1e3, rate(fold_s));
    println!("  rust  tree (transcription):            {:8.1} ms  {:7.2} M entries/s", tree_s * 1e3, rate(tree_s));
    println!("  rust  stream::Merge, k-way over .kx:   {:8.1} ms  {:7.2} M entries/s  ({:.1} ms wall, rayon decode)", nbest * 1e3, rate(nbest), nwall * 1e3);
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    std::fs::create_dir_all("/tmp/bendx").unwrap();
    match a.get(2).map(String::as_str) {
        Some("diff") => diff(&a[1], a[3].parse().unwrap()),
        Some("bench") => bench(&a[1], a[3].parse().unwrap(), a[4].parse().unwrap(), a[5].parse().unwrap()),
        _ => eprintln!("usage: bend-harness RUN diff SEEDS | RUN bench COMMITS PER KEYS"),
    }
}
