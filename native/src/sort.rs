//! Ordering written keys: the permutation that sorts them, as `u32` row
//! indices. Keys are bucketed by their first varying bytes, then each bucket
//! sorted as prefix pairs on every core — the strategy the benchmarks in
//! bench/keys/results.md picked over plain and radix sorts of the keys.

use rayon::prelude::*;

use crate::error::{Error, Result};

/// The permutation that sorts `keys`; errors on a duplicate key.
pub fn sort_order(keys: &[&[u8]]) -> Result<Vec<usize>> {
    let mut order: Vec<usize> = (0..keys.len()).collect();
    order.sort_unstable_by(|&a, &b| keys[a].cmp(keys[b]));
    for w in order.windows(2) {
        if keys[w[0]] == keys[w[1]] {
            return Err(Error::Value(format!(
                "duplicate key {:?}",
                String::from_utf8_lossy(keys[w[0]])
            )));
        }
    }
    Ok(order)
}

/// Random access to the keys being sorted.
pub trait Keys: Sync {
    fn len(&self) -> usize;
    fn key(&self, i: usize) -> &[u8];
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Whether the keys are in order already (equal keys may repeat).
pub fn is_sorted<K: Keys + ?Sized>(keys: &K) -> bool {
    (1..keys.len()).all(|i| keys.key(i - 1) <= keys.key(i))
}

/// The permutation that sorts `keys`: as fast as sorting prefix pairs on
/// every core, at a third of the memory. Bucketed by the two key bytes
/// after the common prefix, each bucket then sorted as prefix pairs
/// (`sort_buckets`); peaks at the permutation plus the pairs of the buckets
/// being sorted at once.
pub fn order<K: Keys + ?Sized>(keys: &K) -> Vec<u32> {
    buckets(keys, (keys.len() / 64).max(1 << 16))
}

/// 12 bytes per key: the 8 key bytes after the prefix all keys share, and the row.
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
struct Pair {
    prefix: u64,
    row: u32,
}

/// Bytes every key starts with.
fn common_prefix<K: Keys + ?Sized>(keys: &K) -> usize {
    if keys.is_empty() {
        return 0;
    }
    let first = keys.key(0);
    let mut n = first.len();
    for i in 1..keys.len() {
        let k = keys.key(i);
        n = n.min(k.len());
        n = first[..n].iter().zip(k).take_while(|(a, b)| a == b).count();
        if n == 0 {
            break;
        }
    }
    n
}

fn prefix_at(key: &[u8], skip: usize) -> u64 {
    let mut b = [0u8; 8];
    let tail = &key[skip.min(key.len())..];
    let m = tail.len().min(8);
    b[..m].copy_from_slice(&tail[..m]);
    u64::from_be_bytes(b)
}

/// Sorts pairs whose keys share `depth` bytes (the prefix holding the next
/// 8): by prefix, then each run of equal prefixes by the next 8 bytes, and
/// so on — through the keys only where a key ends inside a run.
fn sort_deep<K: Keys + ?Sized>(keys: &K, p: &mut [Pair], depth: usize) {
    if p.len() > 1 << 20 {
        p.par_sort_unstable_by_key(|x| x.prefix); // a skewed bucket: use every core on it
    } else {
        p.sort_unstable_by_key(|x| x.prefix);
    }
    let mut i = 0;
    while i < p.len() {
        let pi = p[i].prefix;
        let mut j = i + 1;
        while j < p.len() && p[j].prefix == pi {
            j += 1;
        }
        if j - i > 1 {
            let run = &mut p[i..j];
            let next = depth + 8;
            if run.iter().all(|x| keys.key(x.row as usize).len() > next) {
                for x in run.iter_mut() {
                    x.prefix = prefix_at(keys.key(x.row as usize), next);
                }
                sort_deep(keys, run, next);
            } else {
                run.sort_unstable_by(|a, b| {
                    keys.key(a.row as usize)[depth..].cmp(&keys.key(b.row as usize)[depth..])
                });
            }
        }
        i = j;
    }
}

/// Scatters rows (`from[i]`, or `i` itself) into `out` grouped by the two key
/// bytes at `depth`, on every core; returns where each of the 65,536
/// buckets starts, and the end.
fn bucket_into<K: Keys + ?Sized>(
    keys: &K,
    from: Option<&[u32]>,
    depth: usize,
    out: &mut [u32],
) -> Vec<usize> {
    let n = out.len();
    let row = |i: usize| from.map_or(i as u32, |f| f[i]);
    let bucket = |r: u32| -> usize {
        let k = keys.key(r as usize);
        let b = |j: usize| k.get(depth + j).map_or(0, |&x| x as usize);
        (b(0) << 8) | b(1)
    };
    // A 256 KB histogram per chunk: a few dozen chunks, whatever `n`.
    let chunks = n.div_ceil(1 << 16).clamp(1, 64);
    let chunk = n.div_ceil(chunks);
    let mut counts: Vec<Vec<u32>> = (0..chunks)
        .into_par_iter()
        .map(|c| {
            let mut h = vec![0u32; 1 << 16];
            for i in c * chunk..((c + 1) * chunk).min(n) {
                h[bucket(row(i))] += 1;
            }
            h
        })
        .collect();
    // Where each chunk writes each bucket: buckets in order, chunks in order within.
    let mut starts = vec![0usize; (1 << 16) + 1];
    let mut at = 0usize;
    for b in 0..1 << 16 {
        starts[b] = at;
        for h in counts.iter_mut() {
            let c = h[b] as usize;
            h[b] = at as u32;
            at += c;
        }
    }
    starts[1 << 16] = n;
    let out = out.as_mut_ptr() as usize;
    counts
        .into_par_iter()
        .enumerate()
        .for_each(|(c, mut next)| {
            for i in c * chunk..((c + 1) * chunk).min(n) {
                let r = row(i);
                let b = bucket(r);
                // SAFETY: every chunk writes its own disjoint positions within each bucket.
                unsafe { (out as *mut u32).add(next[b] as usize).write(r) };
                next[b] += 1;
            }
        });
    starts
}

/// Sorts rows whose keys share `depth` bytes as prefix pairs (`sort_deep`).
fn sort_rows<K: Keys + ?Sized>(keys: &K, rows: &mut [u32], depth: usize) {
    let mut p: Vec<Pair> = rows
        .iter()
        .map(|&r| Pair {
            prefix: prefix_at(keys.key(r as usize), depth),
            row: r,
        })
        .collect();
    if rows.iter().any(|&r| keys.key(r as usize).len() <= depth) {
        // Some keys end before `depth`: their shared bytes were padding.
        let cmp = |a: &Pair, b: &Pair| keys.key(a.row as usize).cmp(keys.key(b.row as usize));
        if p.len() > 1 << 20 {
            p.par_sort_unstable_by(cmp);
        } else {
            p.sort_unstable_by(cmp);
        }
    } else {
        sort_deep(keys, &mut p, depth);
    }
    for (r, x) in rows.iter_mut().zip(&p) {
        *r = x.row;
    }
}

/// Sorts each bucket of `rows` (`starts` from `bucket_into`), whose keys
/// share `depth` bytes: buckets up to `limit` rows as prefix pairs, many at
/// once; bigger ones — too many pairs to hold at once — split again by their
/// next two bytes, one at a time.
fn sort_buckets<K: Keys + ?Sized>(
    keys: &K,
    rows: &mut [u32],
    starts: &[usize],
    depth: usize,
    limit: usize,
) {
    let (mut small, mut big) = (Vec::new(), Vec::new());
    let mut rest = rows;
    for b in 0..1 << 16 {
        let (x, r) = rest.split_at_mut(starts[b + 1] - starts[b]);
        match x.len() {
            0 | 1 => {}
            n if n > limit => big.push(x),
            _ => small.push(x),
        }
        rest = r;
    }
    small.sort_unstable_by_key(|b| std::cmp::Reverse(b.len()));
    small
        .into_par_iter()
        .for_each(|b| sort_rows(keys, b, depth));
    for b in big {
        // Skip what the bucket's keys still share, then split on the next two bytes.
        let tail = |r: u32| keys.key(r as usize).get(depth..).unwrap_or(&[]);
        let first = tail(b[0]);
        let shared = b.iter().fold(first.len(), |n, &r| {
            n.min(
                first
                    .iter()
                    .zip(tail(r))
                    .take_while(|(x, y)| x == y)
                    .count(),
            )
        });
        let d = depth + shared;
        let scratch = b.to_vec();
        let sub = bucket_into(keys, Some(&scratch), d, b);
        drop(scratch);
        if sub.windows(2).any(|w| w[1] - w[0] == b.len()) {
            sort_rows(keys, b, d); // no progress: keys ending here, or duplicates
        } else {
            sort_buckets(keys, b, &sub, d + 2, limit);
        }
    }
}

fn buckets<K: Keys + ?Sized>(keys: &K, limit: usize) -> Vec<u32> {
    let skip = common_prefix(keys);
    let mut perm = vec![0u32; keys.len()];
    let starts = bucket_into(keys, None, skip, &mut perm);
    sort_buckets(keys, &mut perm, &starts, skip + 2, limit);
    perm
}

#[cfg(test)]
mod tests {
    use super::*;

    struct V(Vec<Vec<u8>>);
    impl Keys for V {
        fn len(&self) -> usize {
            self.0.len()
        }
        fn key(&self, i: usize) -> &[u8] {
            &self.0[i]
        }
    }

    #[test]
    fn orders_agree_with_a_plain_sort() {
        let mut x: u64 = 7;
        let mut keys = V((0..20_000)
            .map(|i| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                // Shared prefixes, ties on the first 8 varying bytes, and short keys.
                match i % 4 {
                    0 => format!("cust-{:013}", x % 10u64.pow(12)).into_bytes(),
                    1 => format!("cust-{:08}-{}", x % 1000, i).into_bytes(),
                    2 => format!("cust-{}", x % 97).into_bytes(),
                    _ => format!("cust-{:x}", x).into_bytes(),
                }
            })
            .collect());
        // Keys that end where others go on, some with zero bytes: padding must not tie them.
        for k in [
            "",
            "c",
            "cust-7",
            "cust-7\0",
            "cust-7\0\0",
            "cust-70",
            "cust-\0",
        ] {
            keys.0.push(k.as_bytes().to_vec());
        }
        keys.0.sort();
        keys.0.dedup();
        let sorted = keys.0.clone();
        let mut r: u64 = 1;
        for i in (1..keys.0.len()).rev() {
            r = r.wrapping_mul(6364136223846793005).wrapping_add(1);
            keys.0.swap(i, (r >> 33) as usize % (i + 1));
        }
        assert!(!is_sorted(&keys));
        // As the index sorts, and with buckets small enough to be split again.
        for o in [order(&keys), buckets(&keys, 16)] {
            let got: Vec<Vec<u8>> = o.iter().map(|&i| keys.0[i as usize].clone()).collect();
            assert_eq!(got, sorted);
        }
        assert!(is_sorted(&V(sorted.clone())));
        // Repeated keys sort next to each other.
        let dup = V(vec![
            b"2".to_vec(),
            b"1".to_vec(),
            b"2".to_vec(),
            b"1".to_vec(),
        ]);
        for o in [order(&dup), buckets(&dup, 1)] {
            let got: Vec<&[u8]> = o.iter().map(|&i| dup.key(i as usize)).collect();
            assert_eq!(got, [b"1", b"1", b"2", b"2"]);
        }
    }
}
