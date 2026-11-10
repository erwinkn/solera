//! Where a scattered get's time goes: every block of one layer decompressed
//! (zstd alone), then decoded into a run (zstd and rows), per block.
//! \`decode <layer file>\`

use solera_index::format::{parse_segment, Decoder, Top};
use solera_index::Run;
use std::time::Instant;

fn main() {
    let path = std::env::args().nth(1).unwrap();
    let f = std::fs::read(&path).unwrap();
    let t = &f[f.len() - 48..];
    let top_start = u64::from_le_bytes(t[16..24].try_into().unwrap());
    let top = Top::parse(&f[top_start as usize..], top_start, f.len() as u64).unwrap();
    let mut blocks = Vec::new();
    for i in 0..top.segs.len() {
        let s = &top.segs;
        let c = &f[s.off[i] as usize..s.off[i] as usize + s.clen[i] as usize];
        let ix = parse_segment(c, s.rlen[i], s.check[i], s.n[i]).unwrap();
        for b in 0..ix.len() {
            blocks.push((ix.off[b] as usize, ix.clen[b] as usize, ix.rlen[b], ix.n[b]));
        }
    }
    let rows: u64 = blocks.iter().map(|b| b.3 as u64).sum();
    let mut zd = zstd::bulk::Decompressor::new().unwrap();
    let mut raw = Vec::with_capacity(1 << 20);
    let t0 = Instant::now();
    for &(o, c, r, _) in &blocks {
        raw.clear();
        zd.decompress_to_buffer(&f[o..o + c], &mut raw).unwrap();
        assert_eq!(raw.len(), r as usize);
    }
    let z = t0.elapsed().as_secs_f64();
    let t1 = Instant::now();
    let mut x = 0u64;
    for &(o, c, _, _) in &blocks {
        x ^= xxhash_rust::xxh3::xxh3_64(&f[o..o + c]);
    }
    let h = t1.elapsed().as_secs_f64();
    let mut d = Decoder::default();
    let mut run = Run::new();
    let t2 = Instant::now();
    for &(o, c, r, n) in &blocks {
        d.decode(&f[o..o + c], r, n, &mut run).unwrap();
    }
    let all = t2.elapsed().as_secs_f64();
    let nb = blocks.len() as f64;
    println!(
        "{path}: {} blocks, {rows} rows ({:.0} per block), {:.1} KB compressed per block; per block: checksum {:.1} us, zstd {:.1} us, zstd+rows {:.1} us ({x:x})",
        blocks.len(),
        rows as f64 / nb,
        f.len() as f64 / nb / 1e3,
        h / nb * 1e6,
        z / nb * 1e6,
        all / nb * 1e6
    );
}
