//! Damage to any section of a layer, sealed again with a valid checksum (so
//! the parsers see it, as they would a crafted file): every parse returns a
//! value or a format error, never panics, and holds memory in proportion to
//! the bytes it decompresses, plus the keys it rebuilds from prefixes, which
//! the format bounds by `MAX_RAW`. Never in proportion to a length or count
//! it read. Its own test binary: it measures the heap.

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use solera_index::format::*;
use solera_index::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use xxhash_rust::xxh3::xxh3_64;

struct Counting;

static NOW: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grew(n: usize) {
    let now = NOW.fetch_add(n, Relaxed) + n;
    PEAK.fetch_max(now, Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        grew(l.size());
        unsafe { System.alloc(l) }
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        NOW.fetch_sub(l.size(), Relaxed);
        unsafe { System.dealloc(p, l) }
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, size: usize) -> *mut u8 {
        if size > l.size() {
            grew(size - l.size());
        } else {
            NOW.fetch_sub(l.size() - size, Relaxed);
        }
        unsafe { System.realloc(p, l, size) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn check(b: &[u8]) -> u32 {
    xxh3_64(b) as u32
}

fn seal(raw: &[u8]) -> Vec<u8> {
    zstd::bulk::compress(raw, 1).unwrap()
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

/// A valid layer in tiny blocks and segments, with byte and integer
/// versions, removals and several commits. Its keys share a prefix of
/// `shared` bytes, and each commit header carries `producer` zero bytes (a
/// section that compresses well).
fn layer(rng: &mut SmallRng, keys: u32, shared: usize, producer: usize) -> (Vec<u8>, LayerRef) {
    let o = WriteOptions {
        block_bytes: 128,
        level: 1,
        seg_blocks: 2,
    };
    let mut w = Writer::new(o, 1, 20, None).unwrap();
    let prefix = "p".repeat(shared);
    for k in 0..keys {
        let key = format!("{prefix}key/{k:05}/{}", "x".repeat(rng.random_range(0..20)));
        let n = rng.random_range(1..4u64);
        let mut c = 21 - rng.random_range(0..4u64);
        for _ in 0..n {
            c -= rng.random_range(1..5u64).min(c - 1).max(1);
            let new = match rng.random_range(0..3) {
                0 => Version::Absent,
                1 => Version::Int(rng.random()),
                _ => Version::Bytes(b"etag-0123456789"),
            };
            w.push(key.as_bytes(), c.max(1), new, Version::Int(c)).ok();
            if c <= 1 {
                break;
            }
        }
    }
    let commits: Vec<CommitHeader> = (1..=20)
        .map(|c| CommitHeader {
            commit: c,
            time_ms: c * 1000,
            added: c,
            producer: vec![0; producer],
            min_key: b"key/".to_vec(),
            max_key: b"key/9".to_vec(),
            ..Default::default()
        })
        .collect();
    let l = w.finish(&commits).unwrap();
    (w.take(), l)
}

/// One damage to raw bytes: a bit flip, a truncation, a length or count
/// rewritten as a large varint or word, or a slice cut out or repeated.
fn damage(rng: &mut SmallRng, raw: &mut Vec<u8>) {
    if raw.is_empty() {
        raw.push(rng.random());
        return;
    }
    // counts and lengths that size what follows sit at the start of a section
    let at = if rng.random() {
        rng.random_range(0..raw.len().min(16))
    } else {
        rng.random_range(0..raw.len())
    };
    match rng.random_range(0..5) {
        0 => raw[at] ^= 1 << rng.random_range(0..8),
        1 => raw.truncate(at),
        2 => {
            let v: u64 = [
                1 << 20,
                1 << 31,
                (1 << 32) + 5,
                1 << 62,
                u64::MAX,
                rng.random(),
            ][rng.random_range(0..6)];
            let mut enc = Vec::new();
            let mut n = v;
            while n >= 0x80 {
                enc.push(n as u8 | 0x80);
                n >>= 7;
            }
            enc.push(n as u8);
            if rng.random() {
                enc = v.to_le_bytes()[..rng.random_range(1..9)].to_vec();
            }
            let end = (at + enc.len()).min(raw.len());
            raw.splice(at..end, enc);
        }
        3 => {
            let end = rng.random_range(at..=raw.len());
            raw.drain(at..end);
        }
        _ => {
            let end = rng.random_range(at..=raw.len().min(at + 64));
            let s = raw[at..end].to_vec();
            raw.splice(at..at, s);
        }
    }
}

/// Run `parse` on damaged input that decompresses to `raw` bytes: it
/// returns a value or a format error, and holds at most 1 MiB plus 64 times
/// `raw`, plus twice `MAX_RAW` when it rebuilds prefix-coded keys.
fn parsed<T: std::fmt::Debug>(
    what: &str,
    raw: usize,
    keys: bool,
    parse: impl FnOnce() -> Result<T>,
) {
    let given = (1 << 20) + 64 * raw + if keys { 2 * MAX_RAW } else { 0 };
    let base = NOW.load(Relaxed);
    PEAK.store(base, Relaxed);
    let got = parse();
    let held = PEAK.load(Relaxed) - base;
    assert!(
        matches!(got, Ok(_) | Err(Error::Format(_))),
        "{what}: {got:?}"
    );
    assert!(
        held <= given,
        "{what}: {held} bytes held for {raw} bytes decompressed"
    );
}

#[test]
fn damage_to_any_section_is_a_format_error_within_bounded_memory() {
    let mut rng = SmallRng::seed_from_u64(11);
    let trials: usize = std::env::var("DAMAGE_TRIALS").map_or(4000, |s| s.parse().unwrap());
    let corpora = [
        layer(&mut rng, 300, 0, 5),
        layer(&mut rng, 48, 65_000, 1 << 20),
    ];
    for trial in 0..trials {
        let (file, l) = &corpora[trial / 5 % 2];
        one(&mut rng, trial, file, l);
    }
}

fn one(rng: &mut SmallRng, trial: usize, file: &[u8], l: &LayerRef) {
    let top_start = l.top_start as usize;
    let tail = &file[top_start..];
    let top = Top::parse(tail, l.top_start, l.size).unwrap();
    let trailer = &file[file.len() - TRAILER..];
    let top_raw =
        zstd::bulk::decompress(&tail[..tail.len() - TRAILER], u32_at(trailer, 24) as usize)
            .unwrap();
    let commits = &file[top.commits_start as usize..top.index_start as usize];
    let commits_raw = zstd::bulk::decompress(&commits[8..], u32_at(commits, 0) as usize).unwrap();
    let segs = &top.segs;
    let seg = |i: usize| {
        let (o, c) = (segs.off[i] as usize, segs.clen[i] as usize);
        &file[o..o + c]
    };
    let blocks = parse_segment(
        seg(0),
        segs.rlen[0],
        segs.check[0],
        segs.n[0],
        top.commits_start,
    )
    .unwrap();
    assert!(segs.len() > 2 && blocks.len() == 2);
    {
        let what = |s: &str| format!("trial {trial}, {s}");
        match trial % 5 {
            // the top level, sealed again under a valid trailer
            0 => {
                let mut raw = top_raw.clone();
                damage(rng, &mut raw);
                let c = seal(&raw);
                let mut t = trailer.to_vec();
                t[24..28].copy_from_slice(&(raw.len() as u32).to_le_bytes());
                t[28..32].copy_from_slice(&check(&c).to_le_bytes());
                let tc = check(&t[..36]);
                t[36..40].copy_from_slice(&tc.to_le_bytes());
                let mut tail = c;
                tail.extend_from_slice(&t);
                let size = l.top_start + tail.len() as u64;
                parsed(&what("top"), raw.len(), true, || {
                    let t = Top::parse(&tail, l.top_start, size)?;
                    t.check(&LayerRef { size, ..l.clone() }).map(|_| t.lo)
                });
            }
            // the trailer's offsets and lengths
            1 => {
                let mut t = trailer[..36].to_vec();
                damage(rng, &mut t);
                t.resize(36, 0);
                let tc = check(&t);
                t.extend_from_slice(&tc.to_le_bytes());
                t.extend_from_slice(&MAGIC);
                let mut tail = tail[..tail.len() - TRAILER].to_vec();
                tail.extend_from_slice(&t);
                parsed(&what("trailer"), top_raw.len(), true, || {
                    let t = Top::parse(&tail, l.top_start, l.size)?;
                    t.check(l).map(|_| t.lo)
                });
            }
            // a segment
            2 => {
                let i = rng.random_range(0..segs.len());
                let mut raw = zstd::bulk::decompress(seg(i), segs.rlen[i] as usize).unwrap();
                damage(rng, &mut raw);
                let c = seal(&raw);
                let n = if rng.random_ratio(1, 4) {
                    rng.random()
                } else {
                    segs.n[i]
                };
                parsed(&what("segment"), raw.len(), true, || {
                    parse_segment(&c, raw.len() as u32, check(&c), n, top.commits_start)
                });
            }
            // a block, and every key and version decoded from it
            3 => {
                let i = rng.random_range(0..blocks.len());
                let (o, cl) = (blocks.off[i] as usize, blocks.clen[i] as usize);
                let mut raw =
                    zstd::bulk::decompress(&file[o..o + cl], blocks.rlen[i] as usize).unwrap();
                damage(rng, &mut raw);
                let c = seal(&raw);
                let rows = if rng.random_ratio(1, 4) {
                    rng.random()
                } else {
                    blocks.n[i]
                };
                let rlen = if rng.random_ratio(1, 8) {
                    rng.random()
                } else {
                    raw.len() as u32
                };
                parsed(&what("block"), raw.len(), true, || {
                    let mut run = Run::new();
                    Decoder::default().decode(&c, rlen, rows, &mut run)?;
                    for k in 0..run.len() {
                        for r in run.rows(k) {
                            let _ = (run.key(k), run.new_version(r), run.replaced_version(r));
                        }
                    }
                    Ok(run.len())
                });
            }
            // the commits section
            _ => {
                let mut raw = commits_raw.clone();
                damage(rng, &mut raw);
                let c = seal(&raw);
                let mut b = (raw.len() as u32).to_le_bytes().to_vec();
                b.extend_from_slice(&check(&c).to_le_bytes());
                b.extend_from_slice(&c);
                parsed(&what("commits"), raw.len(), false, || parse_commits(&b));
            }
        }
    }
}
