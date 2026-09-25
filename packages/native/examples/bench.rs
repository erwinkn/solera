//! Encode and merge throughput without Python: `cargo run --release --example bench`.

use cursus_native::format::{self, Options, CODEC_NONE, CODEC_ZLIB};
use std::time::Instant;

fn main() {
    let n = 1_000_000usize;
    let mut x: u64 = 0x9E3779B97F4A7C15;
    let mut rnd = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let mut keys: Vec<Vec<u8>> = (0..n)
        .map(|i| format!("site-{:012}/file-{i}", rnd() % 1_000_000_000_000).into_bytes())
        .collect();
    keys.sort();
    keys.dedup();
    let vers: Vec<Vec<u8>> = keys
        .iter()
        .map(|_| {
            rnd()
                .to_le_bytes()
                .iter()
                .chain(rnd().to_le_bytes().iter())
                .copied()
                .collect()
        })
        .collect();
    let ks: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let vs: Vec<&[u8]> = vers.iter().map(|v| v.as_slice()).collect();
    let del = vec![0u8; ks.len()];
    let o = Options {
        block_size: 65536,
        level: 1,
        bits_per_item: 14,
        k: 10,
        codec: CODEC_ZLIB,
    };
    for (label, o) in [
        ("zlib-rs level 1, filters", o),
        (
            "no compression, filters",
            Options {
                codec: CODEC_NONE,
                ..o
            },
        ),
        (
            "no compression, k=1 filters",
            Options {
                codec: CODEC_NONE,
                k: 1,
                bits_per_item: 1,
                ..o
            },
        ),
        ("zlib-rs level 6, filters", Options { level: 6, ..o }),
    ] {
        let t = Instant::now();
        let f = format::encode_file(&ks, &vs, &del, o).unwrap();
        println!(
            "{label:32} {:6.3} s  {:.1} MB",
            t.elapsed().as_secs_f64(),
            f.len() as f64 / 1e6
        );
    }
    let f = format::encode_file(&ks, &vs, &del, o).unwrap();
    let t = Instant::now();
    let out = format::merge_files(&[&f], true, o, 64 << 20).unwrap();
    println!(
        "{:32} {:6.3} s  {} files",
        "merge (decode + re-encode)",
        t.elapsed().as_secs_f64(),
        out.len()
    );
    let t = Instant::now();
    let _ = format::merge_range(&[vec![&f[..f.len()]]], CODEC_ZLIB, None, None, true);
    println!(
        "{:32} {:6.3} s",
        "(bogus single-run merge_range)",
        t.elapsed().as_secs_f64()
    );
}
