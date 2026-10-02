//! Encode and merge throughput without Python: `cargo run --release --example bench`.

use solera_native::format::{self, Options, CODEC_NONE, CODEC_ZLIB};
use solera_native::jobs::{Compact, Step};
use solera_native::stream::{Bytes, Segment, Writer};
use std::sync::Arc;
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
    let locs = vec![1u64; ks.len()];
    let prev = vec![None; ks.len()];
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
        let f = format::encode_file(&ks, &vs, &del, &locs, &prev, o).unwrap();
        println!(
            "{label:32} {:6.3} s  {:.1} MB",
            t.elapsed().as_secs_f64(),
            f.len() as f64 / 1e6
        );
    }
    let t = Instant::now();
    let mut w = Writer::new(o, 64 << 20);
    for i in 0..ks.len() {
        w.push(ks[i], vs[i], false, 1, None).unwrap();
    }
    w.finish(false).unwrap();
    println!(
        "{:32} {:6.3} s  {} files",
        "streaming writer (every core)",
        t.elapsed().as_secs_f64(),
        w.files.len()
    );
    let f = format::encode_file(&ks, &vs, &del, &locs, &prev, o).unwrap();
    let (codec, blocks) = format::file_blocks(&f).unwrap();
    let data: Bytes = Arc::new(f);
    let t = Instant::now();
    let mut job = Compact::new(1, true, false, o, 64 << 20);
    let mut fed = false;
    let mut files = 0;
    loop {
        match job.step().unwrap() {
            Step::Run(r) if !fed => {
                job.merge.runs[r].feed(Segment {
                    data: data.clone(),
                    blocks: blocks
                        .iter()
                        .map(|b| (b.offset as usize, b.size as usize, b.crc))
                        .collect(),
                    codec,
                });
                fed = true;
            }
            Step::Run(r) => job.merge.runs[r].end(),
            Step::File => {
                job.writer.files.pop_front();
                files += 1;
            }
            Step::Garbage => unreachable!("no garbage asked for"),
            Step::Rows => unreachable!(),
            Step::Done => break,
        }
    }
    println!(
        "{:32} {:6.3} s  {files} files",
        "compact (decode + re-encode)",
        t.elapsed().as_secs_f64(),
    );
}
