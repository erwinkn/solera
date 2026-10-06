//! Workloads: an output's commit stream over key ids, and the brute-force
//! oracle — every key's history of versions — that every query is checked
//! against. Key bytes are an order-preserving function of the id, so id order
//! is key order.

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

pub const NONE: u64 = u64::MAX;
/// Commits of the initial load: the base sits at `LOAD`, its keys written over
/// that many earlier commits.
pub const LOAD: u64 = 1000;

/// The version a commit's writes carry: its attempt's generation.
pub fn gen_of(c: u64) -> u64 {
    5_000_000 + c * 1000 + splitmix(c) % 1000
}

#[derive(Clone, Copy)]
pub struct Change {
    pub id: u32,
    pub new: u64,      // NONE: removed
    pub replaced: u64, // NONE: added
}

pub struct Workload {
    pub space: u32,                // ids < space
    pub commits: Vec<Vec<Change>>, // [0]: the initial load; each sorted by id
}

/// `site-0042/part-17/obj-000042170123-9f3c1a2e.json`: 53 bytes, in id
/// order, with a hash part as real object names carry (a content hash, a
/// uuid), so that keys are not trivially compressible.
pub fn key_of(id: u32, out: &mut Vec<u8>) {
    use std::io::Write;
    out.clear();
    let h = splitmix(id as u64 ^ 0x5eed) as u32;
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

pub fn key(id: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(40);
    key_of(id, &mut v);
    v
}

pub fn id_of(key: &[u8]) -> u32 {
    let d = &key[key.len() - 26..key.len() - 14];
    d.iter().fold(0u32, |n, c| n * 10 + (c - b'0') as u32)
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

struct Gen {
    cur: Vec<u64>,
    rng: SmallRng,
    commits: Vec<Vec<Change>>,
}

impl Gen {
    fn new(space: u32, present: impl Fn(u32) -> bool, seed: u64) -> Gen {
        let mut cur = vec![NONE; space as usize];
        let mut commits: Vec<Vec<Change>> = vec![Vec::new(); LOAD as usize + 1];
        for id in 0..space {
            if present(id) {
                let c = 1 + splitmix(id as u64 ^ seed) % LOAD;
                cur[id as usize] = gen_of(c);
                commits[c as usize].push(Change {
                    id,
                    new: gen_of(c),
                    replaced: NONE,
                });
            }
        }
        Gen {
            cur,
            rng: SmallRng::seed_from_u64(seed),
            commits,
        }
    }

    fn commit(&mut self, mut changes: Vec<(u32, u64)>) {
        changes.sort_unstable_by_key(|c| c.0);
        changes.dedup_by_key(|c| c.0);
        let mut out = Vec::with_capacity(changes.len());
        for (id, new) in changes {
            if id as usize >= self.cur.len() {
                self.cur.resize(id as usize + 1, NONE);
            }
            let old = self.cur[id as usize];
            if old == new {
                continue;
            }
            self.cur[id as usize] = new;
            out.push(Change {
                id,
                new,
                replaced: old,
            });
        }
        self.commits.push(out);
    }

    fn c(&self) -> u64 {
        self.commits.len() as u64 // the number of the commit being made
    }

    fn random_where(&mut self, n: usize, want_present: bool) -> Vec<u32> {
        let space = self.cur.len() as u32;
        let mut out = Vec::with_capacity(n);
        let mut tries = 0;
        while out.len() < n && tries < n * 50 {
            tries += 1;
            let id = self.rng.random_range(0..space);
            if (self.cur[id as usize] != NONE) == want_present {
                out.push(id);
            }
        }
        out
    }
}

/// `days` days of `per_day` commits, `rate` of the keys updated a day plus a
/// twentieth of that each added and removed: scattered (uniform ids) or
/// clustered (contiguous id runs; adds appended at the end of the key space).
pub fn daily(n: u32, days: u64, per_day: u64, rate: f64, clustered: bool, seed: u64) -> Workload {
    let space = if clustered { n } else { n + n / 100 };
    let mut g = Gen::new(
        space,
        |id| clustered || splitmix(id as u64) % 101 != 0,
        seed,
    );
    let upd = ((n as f64 * rate) / per_day as f64).round() as usize;
    let churn = (upd / 20).max(1);
    let mut next_add = space;
    for _day in 0..days {
        let start = g
            .rng
            .random_range(0..n.saturating_sub(upd as u32 * per_day as u32).max(1));
        for h in 0..per_day {
            let c = g.c();
            let mut ch: Vec<(u32, u64)> = Vec::new();
            if clustered {
                let a = start + (h as u32) * upd as u32;
                ch.extend(
                    (a..a + upd as u32)
                        .filter(|&id| g.cur[id as usize] != NONE)
                        .map(|id| (id, gen_of(c))),
                );
                let r = g.rng.random_range(0..n - churn as u32);
                ch.extend(
                    (r..r + churn as u32)
                        .filter(|&id| g.cur[id as usize] != NONE)
                        .map(|id| (id, NONE)),
                );
                ch.extend((next_add..next_add + churn as u32).map(|id| (id, gen_of(c))));
                next_add += churn as u32;
            } else {
                ch.extend(
                    g.random_where(upd, true)
                        .into_iter()
                        .map(|id| (id, gen_of(c))),
                );
                ch.extend(g.random_where(churn, true).into_iter().map(|id| (id, NONE)));
                ch.extend(
                    g.random_where(churn, false)
                        .into_iter()
                        .map(|id| (id, gen_of(c))),
                );
            }
            g.commit(ch);
        }
    }
    Workload {
        space: g.cur.len() as u32,
        commits: g.commits,
    }
}

/// The same volume as `daily` (`rate` of the keys a day), all of it on a hot
/// set of `hot` keys: many entries per key; a third of the writes revert a key
/// to its version before, which a diff must see as unchanged.
pub fn hot(n: u32, days: u64, per_day: u64, rate: f64, hot: f64, seed: u64) -> Workload {
    let mut g = Gen::new(n, |_| true, seed);
    let set: Vec<u32> = (0..(n as f64 * hot) as usize)
        .map(|_| g.rng.random_range(0..n))
        .collect();
    let upd = ((n as f64 * rate) / per_day as f64).round() as usize;
    let mut before: std::collections::HashMap<u32, u64> = Default::default();
    for _ in 0..days * per_day {
        let c = g.c();
        let mut ch: Vec<(u32, u64)> = (0..upd)
            .map(|_| set[g.rng.random_range(0..set.len())])
            .map(|id| (id, 0))
            .collect();
        ch.sort_unstable();
        ch.dedup();
        for e in &mut ch {
            let now = g.cur[e.0 as usize];
            e.1 = match before.get(&e.0) {
                Some(&b) if b != NONE && g.rng.random_range(0..3) == 0 => b,
                _ => gen_of(c),
            };
            before.insert(e.0, now);
        }
        g.commit(ch);
    }
    Workload {
        space: n,
        commits: g.commits,
    }
}

/// `days` of scattered `rate` churn, then a full run rewriting every key in
/// key order, `batch` keys a commit.
pub fn rewrite(n: u32, days: u64, per_day: u64, rate: f64, batch: u32, seed: u64) -> Workload {
    let mut w = daily(n, days, per_day, rate, false, seed);
    let mut g = Gen {
        cur: vec![NONE; w.space as usize],
        rng: SmallRng::seed_from_u64(seed + 1),
        commits: std::mem::take(&mut w.commits),
    };
    for commit in &g.commits {
        for c in commit {
            g.cur[c.id as usize] = c.new;
        }
    }
    let mut a = 0;
    while a < w.space {
        let c = g.c();
        let b = (a + batch).min(w.space);
        let ch = (a..b)
            .filter(|&id| g.cur[id as usize] != NONE)
            .map(|id| (id, gen_of(c)))
            .collect();
        g.commit(ch);
        a = b;
    }
    Workload {
        commits: g.commits,
        ..w
    }
}

/// Every key's versions by commit: CSR over ids.
pub struct Oracle {
    off: Vec<u64>,
    hist: Vec<(u32, u64)>, // (commit, new)
}

impl Oracle {
    pub fn new(w: &Workload) -> Oracle {
        let mut off = vec![0u64; w.space as usize + 1];
        for commit in &w.commits {
            for c in commit {
                off[c.id as usize + 1] += 1;
            }
        }
        for i in 1..off.len() {
            off[i] += off[i - 1];
        }
        let mut at = off.clone();
        let mut hist = vec![(0u32, 0u64); *off.last().unwrap() as usize];
        for (n, commit) in w.commits.iter().enumerate() {
            for c in commit {
                hist[at[c.id as usize] as usize] = (n as u32, c.new);
                at[c.id as usize] += 1;
            }
        }
        Oracle { off, hist }
    }

    pub fn space(&self) -> u32 {
        (self.off.len() - 1) as u32
    }

    /// `id`'s version after commit `c`, and the commit that set it (NONE: absent).
    pub fn at(&self, id: u32, c: u64) -> (u64, u64) {
        let h = &self.hist[self.off[id as usize] as usize..self.off[id as usize + 1] as usize];
        let i = h.partition_point(|e| e.0 as u64 <= c);
        if i == 0 {
            (NONE, NONE)
        } else {
            (h[i - 1].1, h[i - 1].0 as u64)
        }
    }

    /// Every entry of `id` with a commit in `[lo, hi]`, newest first: (commit, new, replaced).
    pub fn entries(&self, id: u32, lo: u64, hi: u64, out: &mut Vec<(u64, u64, u64)>) {
        out.clear();
        let h = &self.hist[self.off[id as usize] as usize..self.off[id as usize + 1] as usize];
        for (i, e) in h.iter().enumerate() {
            let c = e.0 as u64;
            if lo <= c && c <= hi {
                let replaced = if i == 0 { NONE } else { h[i - 1].1 };
                out.push((c, e.1, replaced));
            }
        }
        out.reverse();
    }
}

/// An order-sensitive digest of a result stream: every key and its versions.
#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
pub struct Digest {
    pub n: u64,
    pub h: u64,
}

impl Digest {
    #[inline]
    pub fn add(&mut self, id: u32, a: u64, b: u64) {
        let mut buf = [0u8; 20];
        buf[..4].copy_from_slice(&id.to_le_bytes());
        buf[4..12].copy_from_slice(&a.to_le_bytes());
        buf[12..].copy_from_slice(&b.to_le_bytes());
        self.h = self.h.rotate_left(7) ^ xxhash_rust::xxh3::xxh3_64(&buf);
        self.n += 1;
    }

    pub fn add4(&mut self, id: u32, c: u64, new: u64, replaced: u64) {
        self.add(id, c.rotate_left(32) ^ new, replaced);
    }
}
