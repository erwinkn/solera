//! W57 phase 1: what a stamped entry costs against the minimal delta's.
//!
//! A stamped entry is what a run holds per key: the key, whether it is present
//! at the run's end, the generation of its last change, its presence flips
//! (adds and removes) inside the retention window, and a source's payload. A
//! minimal delta entry is the commit's: key, change kind, payload.
//!
//! Both are row-encoded (shared prefix, suffix, fields), cut into 16 KiB
//! blocks, compressed with zstd-1. Generations are stored relative to the
//! block's smallest; flips as gaps going back from the last generation.
//! Decoding means decompressing and parsing every entry into arrays. One
//! core, no I/O.
//!
//! Also measured: a k-way merge of eight runs into one (decode, newest wins,
//! flips united, encode, compress), and the block index a run needs (first
//! key, offset, length, newest generation per block).
//!
//! Usage: fp-codec [entries per dataset, default 400000]

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
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.next().to_le_bytes());
        out[8..].copy_from_slice(&self.next().to_le_bytes());
        out
    }
}

const PRESENT: u8 = 1;
const PAYLOAD: u8 = 2;
const FLIPS: u8 = 4;

#[derive(Clone)]
struct Entry {
    key: Vec<u8>,
    present: bool,
    gen: u64,
    flips: Vec<u64>, // descending, each <= gen
    payload: Option<[u8; 16]>,
}

// -- datasets -------------------------------------------------------------------------

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

/// Generation of commit c: commits are ~50 generations apart (W53's model).
fn gen_of(c: u64, r: &mut Rng) -> u64 {
    10_000_000 + c * 50 + r.below(50)
}

/// The base: every key present, last changed anywhere in a long history
/// (200,000 commits), no flips (all older than the cut).
fn base(keys: &[Vec<u8>], payload: bool, seed: u64) -> Vec<Entry> {
    let mut r = Rng(seed);
    keys.iter()
        .map(|k| Entry {
            key: k.clone(),
            present: true,
            gen: gen_of(r.below(200_000), &mut r),
            flips: vec![],
            payload: payload.then(|| r.bytes16()),
        })
        .collect()
}

/// A run over `span` recent commits: 95% updates (no flips), 2.5% added in
/// the run (one flip), 2.5% removed (absent, one flip), and a share `temp`
/// of temporary keys, added and removed 100 commits later (absent, two flips).
fn run(keys: &[Vec<u8>], span: u64, temp: u64, payload: bool, seed: u64) -> Vec<Entry> {
    let mut r = Rng(seed);
    let first = 190_000;
    keys.iter()
        .map(|k| {
            let c = first + r.below(span);
            let g = gen_of(c, &mut r);
            let roll = r.below(1000);
            if roll < temp * 10 {
                let a = gen_of(c.saturating_sub(100).max(first), &mut r).min(g);
                return Entry { key: k.clone(), present: false, gen: g, flips: vec![g, a], payload: None };
            }
            let roll = r.below(1000);
            let (present, flips) = if roll < 25 {
                (true, vec![gen_of(first + r.below(c - first + 1), &mut r).min(g)])
            } else if roll < 50 {
                (false, vec![g])
            } else {
                (true, vec![])
            };
            Entry { key: k.clone(), present, gen: g, flips, payload: (payload && present).then(|| r.bytes16()) }
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

fn put_key(out: &mut Vec<u8>, prev: &[u8], key: &[u8]) {
    let s = shared(prev, key);
    put_varint(out, s as u64);
    put_varint(out, (key.len() - s) as u64);
    out.extend_from_slice(&key[s..]);
}

/// Stamped entry. `gmin` is the block's smallest generation.
fn stamped_entry(out: &mut Vec<u8>, prev: &[u8], e: &Entry, gmin: u64) {
    put_key(out, prev, &e.key);
    let mut f = if e.present { PRESENT } else { 0 };
    if e.payload.is_some() {
        f |= PAYLOAD;
    }
    if !e.flips.is_empty() {
        f |= FLIPS;
    }
    out.push(f);
    put_varint(out, e.gen - gmin);
    if !e.flips.is_empty() {
        put_varint(out, e.flips.len() as u64);
        let mut last = e.gen;
        for &x in &e.flips {
            put_varint(out, last - x);
            last = x;
        }
    }
    if let Some(p) = &e.payload {
        out.extend_from_slice(p);
    }
}

/// Minimal delta entry: key, change kind, payload.
fn minimal_entry(out: &mut Vec<u8>, prev: &[u8], e: &Entry) {
    put_key(out, prev, &e.key);
    let kind = if !e.present { 2u8 } else if e.flips.is_empty() { 1 } else { 0 };
    out.push(kind | if e.payload.is_some() { 4 } else { 0 });
    if let Some(p) = &e.payload {
        out.extend_from_slice(p);
    }
}

fn encode_block(block: &[Entry], minimal: bool) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint(&mut out, block.len() as u64);
    let gmin = block.iter().map(|e| e.gen).min().unwrap_or(0);
    if !minimal {
        put_varint(&mut out, gmin);
    }
    let mut prev: &[u8] = &[];
    for e in block {
        if minimal {
            minimal_entry(&mut out, prev, e)
        } else {
            stamped_entry(&mut out, prev, e, gmin)
        }
        prev = &e.key;
    }
    out
}

/// Cut entries into blocks whose raw encoding reaches `target` bytes.
fn cut(entries: &[Entry], target: usize, minimal: bool) -> Vec<&[Entry]> {
    let mut out = Vec::new();
    let (mut start, mut size) = (0, 0);
    let mut buf = Vec::new();
    for i in 0..entries.len() {
        buf.clear();
        let prev: &[u8] = if i == start { &[] } else { &entries[i - 1].key };
        if minimal {
            minimal_entry(&mut buf, prev, &entries[i])
        } else {
            stamped_entry(&mut buf, prev, &entries[i], 0)
        }
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

/// What a reader needs from a stamped block.
#[derive(Default)]
struct Decoded {
    arena: Vec<u8>,
    ends: Vec<u32>,
    gens: Vec<u64>,
    flags: Vec<u8>,
    flips: Vec<u64>,
    flip_end: Vec<u32>,
    payload_at: Vec<u32>,
}

impl Decoded {
    fn clear(&mut self) {
        self.arena.clear();
        self.ends.clear();
        self.gens.clear();
        self.flags.clear();
        self.flips.clear();
        self.flip_end.clear();
        self.payload_at.clear();
    }
    fn push_key(&mut self, s: usize, suffix: &[u8]) {
        let prev = if self.ends.len() >= 2 { self.ends[self.ends.len() - 2] as usize } else { 0 };
        self.arena.extend_from_within(prev..prev + s);
        self.arena.extend_from_slice(suffix);
        self.ends.push(self.arena.len() as u32);
    }
    fn key(&self, i: usize) -> &[u8] {
        let a = if i == 0 { 0 } else { self.ends[i - 1] as usize };
        &self.arena[a..self.ends[i] as usize]
    }
    fn entry(&self, i: usize, raw: &[u8]) -> Entry {
        let f0 = if i == 0 { 0 } else { self.flip_end[i - 1] as usize };
        let p = self.payload_at[i] as usize;
        Entry {
            key: self.key(i).to_vec(),
            present: self.flags[i] & PRESENT != 0,
            gen: self.gens[i],
            flips: self.flips[f0..self.flip_end[i] as usize].to_vec(),
            payload: (self.flags[i] & PAYLOAD != 0).then(|| raw[p..p + 16].try_into().unwrap()),
        }
    }
}

fn decode_stamped(raw: &[u8], d: &mut Decoded) {
    d.clear();
    let mut pos = 0;
    let n = get_varint(raw, &mut pos) as usize;
    let gmin = get_varint(raw, &mut pos);
    for _ in 0..n {
        let s = get_varint(raw, &mut pos) as usize;
        let l = get_varint(raw, &mut pos) as usize;
        d.push_key(s, &raw[pos..pos + l]);
        pos += l;
        let f = raw[pos];
        pos += 1;
        d.flags.push(f);
        let g = gmin + get_varint(raw, &mut pos);
        d.gens.push(g);
        if f & FLIPS != 0 {
            let m = get_varint(raw, &mut pos);
            let mut last = g;
            for _ in 0..m {
                last -= get_varint(raw, &mut pos);
                d.flips.push(last);
            }
        }
        d.flip_end.push(d.flips.len() as u32);
        if f & PAYLOAD != 0 {
            d.payload_at.push(pos as u32);
            pos += 16;
        } else {
            d.payload_at.push(0);
        }
    }
}

fn decode_minimal(raw: &[u8], d: &mut Decoded) {
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
        if f & 4 != 0 {
            d.payload_at.push(pos as u32);
            pos += 16;
        } else {
            d.payload_at.push(0);
        }
    }
}

struct Stored {
    blocks: Vec<(Vec<u8>, usize)>,
    first_keys: Vec<Vec<u8>>,
}

fn store(entries: &[Entry], minimal: bool) -> Stored {
    let bl = cut(entries, 16 * 1024, minimal);
    Stored {
        first_keys: bl.iter().map(|b| b[0].key.clone()).collect(),
        blocks: bl
            .iter()
            .map(|b| {
                let raw = encode_block(b, minimal);
                let len = raw.len();
                (zstd::bulk::compress(&raw, 1).unwrap(), len)
            })
            .collect(),
    }
}

/// Block index: per block, the first key (prefix-shared with the previous
/// block's), the offset gap, compressed length, newest generation.
fn index_bytes(s: &Stored) -> usize {
    let mut out = Vec::new();
    let mut prev: &[u8] = &[];
    for (k, (c, _)) in s.first_keys.iter().zip(&s.blocks) {
        put_key(&mut out, prev, k);
        put_varint(&mut out, c.len() as u64);
        put_varint(&mut out, 10_000_000);
        prev = k;
    }
    out.len()
}

fn measure(entries: &[Entry], minimal: bool) -> (f64, f64, f64, f64) {
    let t = Instant::now();
    let s = store(entries, minimal);
    let encode = t.elapsed().as_secs_f64();
    let bytes: usize = s.blocks.iter().map(|(c, _)| c.len()).sum();
    let mut d = Decoded::default();
    let mut buf;
    let mut reps = 0usize;
    let mut check = 0u64;
    let t = Instant::now();
    while reps == 0 || t.elapsed().as_secs_f64() < 0.4 {
        for (c, len) in &s.blocks {
            buf = zstd::bulk::decompress(c, *len).unwrap();
            if minimal {
                decode_minimal(&buf, &mut d)
            } else {
                decode_stamped(&buf, &mut d)
            }
            check = check.wrapping_add(d.ends.len() as u64 + d.arena.len() as u64);
        }
        reps += 1;
    }
    assert!(check > 0);
    let decode = t.elapsed().as_secs_f64() / reps as f64;
    let n = entries.len() as f64;
    (bytes as f64 / n, index_bytes(&s) as f64 / n, n / encode / 1e6, n / decode / 1e6)
}

fn verify(entries: &[Entry]) {
    let s = store(entries, false);
    let mut d = Decoded::default();
    let mut i = 0;
    for (c, len) in &s.blocks {
        let raw = zstd::bulk::decompress(c, *len).unwrap();
        decode_stamped(&raw, &mut d);
        for j in 0..d.ends.len() {
            let e = d.entry(j, &raw);
            let x = &entries[i];
            assert!(e.key == x.key && e.present == x.present && e.gen == x.gen && e.flips == x.flips);
            assert!(e.payload == x.payload);
            i += 1;
        }
    }
    assert_eq!(i, entries.len());
}

/// Merge `runs` (oldest first): per key the newest entry's state, flips
/// united and those at or below `cut` dropped; an absent key with no flips
/// left is dropped when `bottom` (the merge includes the oldest run).
fn merge(runs: &[Vec<Entry>], cut: u64, bottom: bool) -> Vec<Entry> {
    let mut all: Vec<(usize, &Entry)> = Vec::new();
    for (i, r) in runs.iter().enumerate() {
        all.extend(r.iter().map(|e| (i, e)));
    }
    all.sort_by(|a, b| a.1.key.cmp(&b.1.key).then(b.0.cmp(&a.0)));
    let mut out: Vec<Entry> = Vec::new();
    let mut i = 0;
    while i < all.len() {
        let mut e = all[i].1.clone();
        let mut j = i + 1;
        while j < all.len() && all[j].1.key == e.key {
            e.flips.extend(all[j].1.flips.iter().copied());
            j += 1;
        }
        e.flips.retain(|&f| f > cut);
        e.flips.sort_unstable_by(|a, b| b.cmp(a));
        if e.present || !e.flips.is_empty() || !bottom {
            out.push(e);
        }
        i = j;
    }
    out
}

/// The same merge, over stored runs, streaming: decode every run's blocks,
/// merge, encode and compress the output. Entries in per second.
fn merge_speed(runs: &[Vec<Entry>]) -> f64 {
    let stored: Vec<Stored> = runs.iter().map(|r| store(r, false)).collect();
    let n: usize = runs.iter().map(|r| r.len()).sum();
    let t = Instant::now();
    let mut decoded: Vec<Vec<Entry>> = Vec::new();
    let mut d = Decoded::default();
    for s in &stored {
        let mut v = Vec::new();
        for (c, len) in &s.blocks {
            let raw = zstd::bulk::decompress(c, *len).unwrap();
            decode_stamped(&raw, &mut d);
            for j in 0..d.ends.len() {
                v.push(d.entry(j, &raw));
            }
        }
        decoded.push(v);
    }
    let out = merge(&decoded, 0, false);
    let s = store(&out, false);
    assert!(!s.blocks.is_empty());
    n as f64 / t.elapsed().as_secs_f64() / 1e6
}

fn sample(keys: &[Vec<u8>], share: u64, seed: u64) -> Vec<Vec<u8>> {
    let mut r = Rng(seed);
    keys.iter().filter(|_| r.below(1000) < share).cloned().collect()
}

/// Bytes per entry, index bytes per entry and decode speed over `files`
/// files of `m` ids each, at the density of `density` keys in the index.
fn ids_row(label: &str, m: usize, density: u64, files: usize, make: &dyn Fn(&[Vec<u8>], u64) -> Vec<Entry>, minimal: bool) {
    let (mut b, mut ix, mut dec, mut n) = (0f64, 0f64, 0f64, 0f64);
    for f in 0..files {
        let keys = ids(m, density, 1000 + f as u64);
        let entries = make(&keys, 7 + f as u64);
        if !minimal && f == 0 {
            verify(&entries);
        }
        let (bb, ii, _, dd) = measure(&entries, minimal);
        let k = entries.len() as f64;
        b += bb * k;
        ix += ii * k;
        dec += k / dd;
        n += k;
    }
    println!("{label}\t{:.2}\t{:.3}\t{:.1}", b / n, ix / n, n / dec);
}

fn main() {
    let n: usize = std::env::args().nth(1).map(|x| x.parse().unwrap()).unwrap_or(400_000);
    println!("12-digit ids, each file at its own key density\nfile\tB/entry\tindex B/entry\tdecode M/s");
    let delta = |k: &[Vec<u8>], s: u64| run(k, 1, 0, false, s);
    let delta_p = |k: &[Vec<u8>], s: u64| run(k, 1, 0, true, s);
    let hundred = |k: &[Vec<u8>], s: u64| run(k, 100, 0, false, s);
    let day = |k: &[Vec<u8>], s: u64| run(k, 8_640, 0, false, s);
    let day_p = |k: &[Vec<u8>], s: u64| run(k, 8_640, 0, true, s);
    let churn = |k: &[Vec<u8>], s: u64| run(k, 8_640, 50, false, s);
    let base_ = |k: &[Vec<u8>], s: u64| base(k, false, s);
    let base_p = |k: &[Vec<u8>], s: u64| base(k, true, s);
    ids_row("100M: delta of 1K keys (minimal)", 1_000, 1_000, 200, &delta, true);
    ids_row("100M: delta of 1K keys (minimal), 16 B payloads", 1_000, 1_000, 200, &delta_p, true);
    ids_row("100M: delta of 1M keys (minimal)", n, 1_000_000, 1, &delta, true);
    ids_row("100M: run of 100 commits (~100K keys)", 100_000, 100_000, 2, &hundred, false);
    ids_row("100M: run of a day (~8.3M keys)", n, 8_300_000, 1, &day, false);
    ids_row("100M: run of a day, 16 B payloads", n, 8_300_000, 1, &day_p, false);
    ids_row("100M: run of a day, churn 50%", n, 12_000_000, 1, &churn, false);
    ids_row("100M: base", n, 100_000_000, 1, &base_, false);
    ids_row("100M: base, 16 B payloads", n, 100_000_000, 1, &base_p, false);
    ids_row("1M: delta of 1K keys (minimal)", 1_000, 1_000, 200, &delta, true);
    ids_row("1M: run of 100 commits (~95K keys)", 95_000, 95_000, 2, &hundred, false);
    ids_row("1M: run of a day (~1M keys)", n, 1_000_000, 1, &day, false);
    ids_row("1M: base", n, 1_000_000, 1, &base_, false);

    let datasets: Vec<(&str, Vec<Vec<u8>>)> = vec![("uuids", uuids(n, 3)), ("paths", paths(n, 4))];
    println!("\nother keys, at the density of a 100M-key index\ndataset\tfile\tB/entry\tindex B/entry\tencode M/s\tdecode M/s");
    for (name, keys) in &datasets {
        let rows: Vec<(&str, Vec<Entry>, bool)> = vec![
            ("delta (minimal)", run(keys, 1, 0, false, 7), true),
            ("run, a day", run(keys, 8_640, 0, false, 7), false),
            ("run, churn 50%", run(keys, 8_640, 50, false, 7), false),
            ("base", base(keys, false, 7), false),
        ];
        for (file, entries, minimal) in &rows {
            if !minimal {
                verify(entries);
            }
            let (b, ix, enc, dec) = measure(entries, *minimal);
            println!("{name}\t{file}\t{b:.2}\t{ix:.3}\t{enc:.1}\t{dec:.1}");
        }
    }
    // Merge: eight runs of 1/8 of the keys each (a tier merge at 100M density).
    let keys = ids(n, 100_000_000, 1);
    let runs: Vec<Vec<Entry>> = (0..8).map(|i| run(&sample(&keys, 125, 100 + i), 500, 0, false, 200 + i)).collect();
    println!("\nmerge of 8 runs, ids@100M, one core: {:.1} M entries in/s", merge_speed(&runs));
    // The merge's semantics: a key added at 5 and removed at 8, read at
    // P = 6 (present then) and P = 2 (absent then).
    let k = b"k".to_vec();
    let older = vec![Entry { key: k.clone(), present: true, gen: 5, flips: vec![5], payload: None }];
    let newer = vec![Entry { key: k.clone(), present: false, gen: 8, flips: vec![8], payload: None }];
    let m = merge(&[older, newer], 0, false);
    let at = |p: u64| m[0].present ^ (m[0].flips.iter().filter(|&&f| f > p).count() % 2 == 1);
    assert!(at(6) && !at(2) && m[0].flips == vec![8, 5]);
    println!("flip parity check: ok");
}
