//! W53 phase 1: what a block of key index entries costs, by layout, block size
//! and codec. One core, no I/O: bytes per entry, encode and decode speed.
//!
//! Entries are the v4 shape (key, flags, generation, payload?, predecessor?,
//! prior payload?), in two layouts:
//!
//! - row: v4's encoding, entry after entry (shared prefix, suffix, flags,
//!   generation, ...);
//! - columnar: the same fields, one section per field; generations and
//!   predecessors as offsets from the block's smallest (frame of reference).
//!
//! Blocks are cut where the row encoding reaches the target, so both layouts
//! hold the same entries per block. Decoding means decompressing and parsing
//! every entry into arrays (keys rebuilt in an arena), as a page or a lookup
//! needs them.
//!
//! Usage: views-codec [entries per dataset, default 400000]

use std::io::{Read, Write};
use std::time::Instant;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn bytes16(&mut self) -> [u8; 16] {
        let a = self.next().to_le_bytes();
        let b = self.next().to_le_bytes();
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&a);
        out[8..].copy_from_slice(&b);
        out
    }
}

const DELETED: u8 = 1;
const PRED: u8 = 2;
const PAYLOAD: u8 = 4;
const PRIOR: u8 = 8;

#[derive(Clone)]
struct Entry {
    key: Vec<u8>,
    flags: u8,
    gen: u64,
    payload: Option<[u8; 16]>,
    pred: u64,
    prior: Option<[u8; 16]>,
}

// -- datasets -----------------------------------------------------------------------

fn sorted_unique(mut keys: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// 12-digit decimal ids at the density of `total` keys in 10^12.
fn ids(n: usize, total: u64, seed: u64) -> Vec<Vec<u8>> {
    let mut r = Rng(seed);
    let space = (n as u128 * 1_000_000_000_000u128 / total as u128) as u64;
    let mut keys = Vec::with_capacity(n);
    while keys.len() < n {
        keys.push(format!("{:012}", r.below(space)).into_bytes());
    }
    sorted_unique(keys)
}

/// UUIDs at the density of about 100M keys.
fn uuids(n: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut r = Rng(seed);
    let shift = ((100_000_000f64 / n as f64).log2().ceil()) as u32;
    let mut keys = Vec::with_capacity(n);
    for _ in 0..n {
        let v = (((r.next() as u128) << 64) | r.next() as u128) >> shift;
        let h = format!("{:032x}", v);
        keys.push(format!("{}-{}-{}-{}-{}", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..]).into_bytes());
    }
    sorted_unique(keys)
}

/// Paths: tenant/site/date/file, as a document inventory holds them.
fn paths(n: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut r = Rng(seed);
    let mut keys = Vec::with_capacity(n);
    while keys.len() < n {
        let t = r.below(8);
        let s = r.below(250);
        let y = 2021 + r.below(5);
        let m = 1 + r.below(12);
        let d = 1 + r.below(28);
        let f = r.below(5000);
        let ext = ["csv", "xlsx", "pdf", "json"][r.below(4) as usize];
        keys.push(format!("tenant-{t:02}/site-{s:04}/{y}-{m:02}-{d:02}/report-{f:05}.{ext}").into_bytes());
    }
    sorted_unique(keys)
}

/// A base run: every key live once, written at some commit of a long history.
fn base(keys: &[Vec<u8>], payload: bool, seed: u64) -> Vec<Entry> {
    let mut r = Rng(seed);
    keys.iter()
        .map(|k| Entry {
            key: k.clone(),
            flags: if payload { PAYLOAD } else { 0 },
            gen: 10_000_000 + r.below(12_000) * 50 + r.below(50),
            payload: payload.then(|| r.bytes16()),
            pred: 0,
            prior: None,
        })
        .collect()
}

/// A net-change node over ~4,096 recent commits: mostly updates (a predecessor,
/// its payload), 5% removals, 5% additions.
fn node(keys: &[Vec<u8>], payload: bool, seed: u64) -> Vec<Entry> {
    let mut r = Rng(seed);
    keys.iter()
        .map(|k| {
            let roll = r.below(100);
            let deleted = roll < 5;
            let added = (5..10).contains(&roll);
            let mut flags = if deleted { DELETED } else { 0 };
            if !added {
                flags |= PRED;
            }
            if payload && !deleted {
                flags |= PAYLOAD;
            }
            if payload && !added {
                flags |= PRIOR;
            }
            Entry {
                key: k.clone(),
                flags,
                gen: 10_600_000 + r.below(4_096) * 50 + r.below(50),
                payload: (flags & PAYLOAD != 0).then(|| r.bytes16()),
                pred: if added { 0 } else { 10_000_000 + r.below(12_000) * 50 },
                prior: (flags & PRIOR != 0).then(|| r.bytes16()),
            }
        })
        .collect()
}

// -- encodings ------------------------------------------------------------------------

fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

#[inline]
fn get_varint(buf: &[u8], pos: &mut usize) -> u64 {
    let mut n = 0u64;
    let mut shift = 0;
    loop {
        let b = buf[*pos];
        *pos += 1;
        n |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            return n;
        }
        shift += 7;
    }
}

fn shared(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn row_entry(out: &mut Vec<u8>, prev: &[u8], e: &Entry) {
    let s = shared(prev, &e.key);
    put_varint(out, s as u64);
    put_varint(out, (e.key.len() - s) as u64);
    out.extend_from_slice(&e.key[s..]);
    out.push(e.flags);
    put_varint(out, e.gen);
    if let Some(p) = &e.payload {
        put_varint(out, 16);
        out.extend_from_slice(p);
    }
    if e.flags & PRED != 0 {
        put_varint(out, e.pred);
    }
    if let Some(p) = &e.prior {
        put_varint(out, 16);
        out.extend_from_slice(p);
    }
}

/// Cut entries into blocks whose row encoding reaches `target` bytes.
fn blocks(entries: &[Entry], target: usize) -> Vec<&[Entry]> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut size = 0;
    let mut buf = Vec::new();
    for i in 0..entries.len() {
        buf.clear();
        let prev: &[u8] = if i == start { &[] } else { &entries[i - 1].key };
        row_entry(&mut buf, prev, &entries[i]);
        size += buf.len();
        if size >= target {
            out.push(&entries[start..=i]);
            start = i + 1;
            size = 0;
        }
    }
    if start < entries.len() {
        out.push(&entries[start..]);
    }
    out
}

fn encode_row(block: &[Entry]) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint(&mut out, block.len() as u64);
    let mut prev: &[u8] = &[];
    for e in block {
        row_entry(&mut out, prev, e);
        prev = &e.key;
    }
    out
}

fn encode_col(block: &[Entry]) -> Vec<u8> {
    let mut sh = Vec::new();
    let mut sl = Vec::new();
    let mut suffix = Vec::new();
    let mut flags = Vec::new();
    let mut gens = Vec::new();
    let mut preds = Vec::new();
    let mut payloads = Vec::new();
    let gmin = block.iter().map(|e| e.gen).min().unwrap_or(0);
    let pmin = block.iter().filter(|e| e.flags & PRED != 0).map(|e| e.pred).min().unwrap_or(0);
    let mut prev: &[u8] = &[];
    for e in block {
        let s = shared(prev, &e.key);
        put_varint(&mut sh, s as u64);
        put_varint(&mut sl, (e.key.len() - s) as u64);
        suffix.extend_from_slice(&e.key[s..]);
        flags.push(e.flags);
        put_varint(&mut gens, e.gen - gmin);
        if e.flags & PRED != 0 {
            put_varint(&mut preds, e.pred - pmin);
        }
        if let Some(p) = &e.payload {
            put_varint(&mut payloads, 16);
            payloads.extend_from_slice(p);
        }
        if let Some(p) = &e.prior {
            put_varint(&mut payloads, 16);
            payloads.extend_from_slice(p);
        }
        prev = &e.key;
    }
    let mut out = Vec::new();
    put_varint(&mut out, block.len() as u64);
    put_varint(&mut out, gmin);
    put_varint(&mut out, pmin);
    for sec in [&sh, &sl, &suffix, &flags, &gens, &preds, &payloads] {
        put_varint(&mut out, sec.len() as u64);
    }
    for sec in [sh, sl, suffix, flags, gens, preds, payloads] {
        out.extend_from_slice(&sec);
    }
    out
}

/// What a reader needs from a block: keys (arena + ends), generations, flags,
/// predecessors, and where each payload starts (0: none).
#[derive(Default)]
struct Decoded {
    arena: Vec<u8>,
    ends: Vec<u32>,
    gens: Vec<u64>,
    flags: Vec<u8>,
    preds: Vec<u64>,
    payload_at: Vec<u32>,
}

impl Decoded {
    fn clear(&mut self) {
        self.arena.clear();
        self.ends.clear();
        self.gens.clear();
        self.flags.clear();
        self.preds.clear();
        self.payload_at.clear();
    }
    /// Append a key: the first `s` bytes of the previous key, then `suffix`.
    fn push_key(&mut self, s: usize, suffix: &[u8]) {
        let prev = if self.ends.len() >= 2 { self.ends[self.ends.len() - 2] as usize } else { 0 };
        self.arena.extend_from_within(prev..prev + s);
        self.arena.extend_from_slice(suffix);
        self.ends.push(self.arena.len() as u32);
    }
}

fn decode_row(raw: &[u8], d: &mut Decoded) {
    d.clear();
    let mut pos = 0;
    let n = get_varint(raw, &mut pos) as usize;
    for _ in 0..n {
        let s = get_varint(raw, &mut pos) as usize;
        let l = get_varint(raw, &mut pos) as usize;
        d.push_key(s, &raw[pos..pos + l]);
        pos += l;
        let f = raw[pos];
        pos += 1;
        d.flags.push(f);
        d.gens.push(get_varint(raw, &mut pos));
        if f & PAYLOAD != 0 {
            let l = get_varint(raw, &mut pos) as usize;
            d.payload_at.push(pos as u32);
            pos += l;
        } else {
            d.payload_at.push(0);
        }
        d.preds.push(if f & PRED != 0 { get_varint(raw, &mut pos) } else { 0 });
        if f & PRIOR != 0 {
            let l = get_varint(raw, &mut pos) as usize;
            pos += l;
        }
    }
}

fn decode_col(raw: &[u8], d: &mut Decoded) {
    d.clear();
    let mut pos = 0;
    let n = get_varint(raw, &mut pos) as usize;
    let gmin = get_varint(raw, &mut pos);
    let pmin = get_varint(raw, &mut pos);
    let mut lens = [0usize; 7];
    for l in lens.iter_mut() {
        *l = get_varint(raw, &mut pos) as usize;
    }
    let mut starts = [0usize; 7];
    let mut at = pos;
    for i in 0..7 {
        starts[i] = at;
        at += lens[i];
    }
    let (mut ps, mut pl, mut px, mut pf, mut pg, mut pp, mut pd) =
        (starts[0], starts[1], starts[2], starts[3], starts[4], starts[5], starts[6]);
    for _ in 0..n {
        let s = get_varint(raw, &mut ps) as usize;
        let l = get_varint(raw, &mut pl) as usize;
        d.push_key(s, &raw[px..px + l]);
        px += l;
        let f = raw[pf];
        pf += 1;
        d.flags.push(f);
        d.gens.push(gmin + get_varint(raw, &mut pg));
        d.preds.push(if f & PRED != 0 { pmin + get_varint(raw, &mut pp) } else { 0 });
        if f & PAYLOAD != 0 {
            let l = get_varint(raw, &mut pd) as usize;
            d.payload_at.push(pd as u32);
            pd += l;
        } else {
            d.payload_at.push(0);
        }
        if f & PRIOR != 0 {
            let l = get_varint(raw, &mut pd) as usize;
            pd += l;
        }
    }
}

// -- codecs ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Codec {
    None,
    Zlib1,
    Lz4,
    Zstd1,
    Zstd3,
}

impl Codec {
    fn name(self) -> &'static str {
        match self {
            Codec::None => "none",
            Codec::Zlib1 => "zlib-1",
            Codec::Lz4 => "lz4",
            Codec::Zstd1 => "zstd-1",
            Codec::Zstd3 => "zstd-3",
        }
    }
    fn compress(self, raw: &[u8]) -> Vec<u8> {
        match self {
            Codec::None => raw.to_vec(),
            Codec::Zlib1 => {
                let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(1));
                e.write_all(raw).unwrap();
                e.finish().unwrap()
            }
            Codec::Lz4 => lz4_flex::block::compress_prepend_size(raw),
            Codec::Zstd1 => zstd::bulk::compress(raw, 1).unwrap(),
            Codec::Zstd3 => zstd::bulk::compress(raw, 3).unwrap(),
        }
    }
    fn decompress(self, data: &[u8], raw_len: usize, out: &mut Vec<u8>) {
        out.clear();
        match self {
            Codec::None => out.extend_from_slice(data),
            Codec::Zlib1 => {
                flate2::read::ZlibDecoder::new(data).read_to_end(out).unwrap();
            }
            Codec::Lz4 => {
                *out = lz4_flex::block::decompress_size_prepended(data).unwrap();
            }
            Codec::Zstd1 | Codec::Zstd3 => {
                *out = zstd::bulk::decompress(data, raw_len).unwrap();
            }
        }
    }
}

// -- the measurement --------------------------------------------------------------------

struct Result {
    bytes_per_entry: f64,
    entries_per_block: f64,
    encode_m: f64,
    decode_m: f64,
}

fn measure(entries: &[Entry], target: usize, columnar: bool, codec: Codec) -> Result {
    let bl = blocks(entries, target);
    let t = Instant::now();
    let mut stored: Vec<(Vec<u8>, usize)> = Vec::with_capacity(bl.len());
    for b in &bl {
        let raw = if columnar { encode_col(b) } else { encode_row(b) };
        let len = raw.len();
        stored.push((codec.compress(&raw), len));
    }
    let encode = t.elapsed().as_secs_f64();
    let bytes: usize = stored.iter().map(|(c, _)| c.len()).sum();

    let mut d = Decoded::default();
    let mut buf = Vec::new();
    let mut reps = 0usize;
    let mut check = 0u64;
    let t = Instant::now();
    while reps == 0 || t.elapsed().as_secs_f64() < 0.4 {
        for (c, len) in &stored {
            codec.decompress(c, *len, &mut buf);
            if columnar {
                decode_col(&buf, &mut d)
            } else {
                decode_row(&buf, &mut d)
            }
            check = check.wrapping_add(d.gens.len() as u64 + d.arena.len() as u64);
        }
        reps += 1;
    }
    let decode = t.elapsed().as_secs_f64() / reps as f64;
    assert!(check > 0);
    let n = entries.len() as f64;
    Result {
        bytes_per_entry: bytes as f64 / n,
        entries_per_block: n / bl.len() as f64,
        encode_m: n / encode / 1e6,
        decode_m: n / decode / 1e6,
    }
}

fn verify(entries: &[Entry]) {
    // Both layouts decode to the entries they encoded.
    for columnar in [false, true] {
        for b in blocks(entries, 16 * 1024).iter().take(20) {
            let raw = if columnar { encode_col(b) } else { encode_row(b) };
            let mut d = Decoded::default();
            if columnar {
                decode_col(&raw, &mut d)
            } else {
                decode_row(&raw, &mut d)
            }
            let mut start = 0usize;
            for (i, e) in b.iter().enumerate() {
                let end = d.ends[i] as usize;
                assert_eq!(&d.arena[start..end], &e.key[..]);
                assert_eq!(d.gens[i], e.gen);
                assert_eq!(d.flags[i], e.flags);
                if e.flags & PRED != 0 {
                    assert_eq!(d.preds[i], e.pred);
                }
                start = end;
            }
        }
    }
}

fn main() {
    let n: usize = std::env::args().nth(1).map(|s| s.parse().unwrap()).unwrap_or(400_000);
    let sets: Vec<(&str, Vec<Vec<u8>>)> = vec![
        ("ids@1M", ids(n, 1_000_000, 1)),
        ("ids@100M", ids(n, 100_000_000, 2)),
        ("uuid@100M", uuids(n, 3)),
        ("paths", paths(n, 4)),
    ];
    let codecs = [Codec::None, Codec::Lz4, Codec::Zstd1, Codec::Zstd3, Codec::Zlib1];
    println!("dataset\tkind\tpayload\tlayout\tblock_kib\tcodec\tB/entry\tentries/block\tencode_M/s\tdecode_M/s");
    for (name, keys) in &sets {
        let avg_key = keys.iter().map(|k| k.len()).sum::<usize>() as f64 / keys.len() as f64;
        eprintln!("{name}: {} keys, {avg_key:.1} B each", keys.len());
        for (kind, payload) in [("base", false), ("base", true), ("node", false), ("node", true)] {
            let entries = if kind == "base" { base(keys, payload, 7) } else { node(keys, payload, 8) };
            verify(&entries);
            for target in [16 * 1024, 64 * 1024, 256 * 1024] {
                for columnar in [false, true] {
                    for codec in codecs {
                        let r = measure(&entries, target, columnar, codec);
                        println!(
                            "{name}\t{kind}\t{}\t{}\t{}\t{}\t{:.2}\t{:.0}\t{:.2}\t{:.2}",
                            if payload { "16B" } else { "-" },
                            if columnar { "col" } else { "row" },
                            target / 1024,
                            codec.name(),
                            r.bytes_per_entry,
                            r.entries_per_block,
                            r.encode_m,
                            r.decode_m,
                        );
                    }
                }
            }
        }
    }
}
