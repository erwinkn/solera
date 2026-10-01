//! Ordering written keys: the permutation that sorts them.
//!
//! Several strategies, benchmarked against each other by `examples/sort.rs`
//! (bench/keys/results.md); `order` is the one the index uses. Every strategy
//! returns the same permutation of row indices, as `u32`.

use rayon::prelude::*;
use std::cmp::Ordering;

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

/// The permutation that sorts `keys`, by the strategy the benchmarks picked:
/// as fast as sorting prefix pairs on every core, at a third of the memory.
pub fn order<K: Keys + ?Sized>(keys: &K) -> Vec<u32> {
    buckets_par(keys)
}

// -- strategies -----------------------------------------------------------------------

/// A bare permutation, compared through the keys.
pub fn perm<K: Keys + ?Sized>(keys: &K) -> Vec<u32> {
    let mut p: Vec<u32> = (0..keys.len() as u32).collect();
    p.sort_unstable_by(|&a, &b| keys.key(a as usize).cmp(keys.key(b as usize)));
    p
}

/// A bare permutation, sorted on every core.
pub fn perm_par<K: Keys + ?Sized>(keys: &K) -> Vec<u32> {
    let mut p: Vec<u32> = (0..keys.len() as u32).collect();
    p.par_sort_unstable_by(|&a, &b| keys.key(a as usize).cmp(keys.key(b as usize)));
    p
}

/// 12 bytes per key: the 8 key bytes after the prefix all keys share, and the row.
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
pub struct Pair {
    prefix: u64,
    row: u32,
}

/// Bytes every key starts with.
pub fn common_prefix<K: Keys + ?Sized>(keys: &K) -> usize {
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

fn make_pairs<K: Keys + ?Sized>(keys: &K, skip: usize) -> Vec<Pair> {
    (0..keys.len())
        .into_par_iter()
        .map(|i| Pair {
            prefix: prefix_at(keys.key(i), skip),
            row: i as u32,
        })
        .collect()
}

#[inline]
fn cmp_pairs<K: Keys + ?Sized>(keys: &K, skip: usize, a: &Pair, b: &Pair) -> Ordering {
    let (pa, pb) = (a.prefix, b.prefix);
    pa.cmp(&pb).then_with(|| {
        let (ra, rb) = (a.row as usize, b.row as usize);
        keys.key(ra)[skip..].cmp(&keys.key(rb)[skip..])
    })
}

/// Reuse the pairs' allocation for the permutation: row `j` goes where pair
/// `j`'s first 4 bytes were, never past a pair not yet read.
fn into_rows(pairs: Vec<Pair>) -> Vec<u32> {
    let n = pairs.len();
    let mut pairs = std::mem::ManuallyDrop::new(pairs);
    let (ptr, cap) = (pairs.as_mut_ptr(), pairs.capacity());
    let rows = ptr as *mut u32;
    for j in 0..n {
        // SAFETY: pair j starts at byte 12j >= 4j; each pair is read before its bytes are reused.
        unsafe {
            let row = std::ptr::addr_of!((*ptr.add(j)).row).read_unaligned();
            rows.add(j).write(row);
        }
    }
    // SAFETY: same allocation, alignment 4 for both types, capacity scaled by 12/4.
    let mut out = unsafe { Vec::from_raw_parts(rows, n, cap * 3) };
    out.shrink_to_fit();
    out
}

/// Prefix pairs, compared by prefix and through the keys on a tie.
pub fn pairs<K: Keys + ?Sized>(keys: &K) -> Vec<u32> {
    let skip = common_prefix(keys);
    let mut p = make_pairs(keys, skip);
    p.sort_unstable_by(|a, b| cmp_pairs(keys, skip, a, b));
    into_rows(p)
}

/// Prefix pairs, sorted on every core.
pub fn pairs_par<K: Keys + ?Sized>(keys: &K) -> Vec<u32> {
    let skip = common_prefix(keys);
    let mut p = make_pairs(keys, skip);
    p.par_sort_unstable_by(|a, b| cmp_pairs(keys, skip, a, b));
    into_rows(p)
}

/// Sorts runs of pairs with equal prefixes through the keys.
fn fix_ties<K: Keys + ?Sized>(keys: &K, skip: usize, p: &mut [Pair]) {
    let mut i = 0;
    while i < p.len() {
        let pi = p[i].prefix;
        let mut j = i + 1;
        while j < p.len() && p[j].prefix == pi {
            j += 1;
        }
        if j - i > 1 {
            p[i..j].sort_unstable_by(|a, b| cmp_pairs(keys, skip, a, b));
        }
        i = j;
    }
}

/// Prefix pairs, least-significant-digit radix sort on the prefix (a scratch
/// copy of the pairs), then ties through the keys.
pub fn pairs_lsd<K: Keys + ?Sized>(keys: &K) -> Vec<u32> {
    let skip = common_prefix(keys);
    let mut p = make_pairs(keys, skip);
    let mut scratch = p.clone();
    for byte in 0..8 {
        let shift = byte * 8;
        let mut counts = [0usize; 256];
        for x in &p {
            counts[((x.prefix >> shift) & 0xFF) as usize] += 1;
        }
        if counts.contains(&p.len()) {
            continue; // this byte never varies
        }
        let mut at = 0;
        for c in counts.iter_mut() {
            let n = *c;
            *c = at;
            at += n;
        }
        for x in &p {
            let d = ((x.prefix >> shift) & 0xFF) as usize;
            scratch[counts[d]] = *x;
            counts[d] += 1;
        }
        std::mem::swap(&mut p, &mut scratch);
    }
    drop(scratch);
    fix_ties(keys, skip, &mut p);
    into_rows(p)
}

/// Prefix pairs, one in-place most-significant-digit pass on the first
/// varying prefix byte, then the 256 buckets sorted on every core.
pub fn pairs_msd_par<K: Keys + ?Sized>(keys: &K) -> Vec<u32> {
    let skip = common_prefix(keys);
    let mut p = make_pairs(keys, skip);
    let n = p.len();
    let mut shift = 56;
    let mut counts;
    loop {
        counts = [0usize; 256];
        for x in &p {
            counts[((x.prefix >> shift) & 0xFF) as usize] += 1;
        }
        if shift == 0 || !counts.contains(&n) {
            break;
        }
        shift -= 8;
    }
    // American flag sort: permute in place into bucket order.
    let mut starts = [0usize; 257];
    for d in 0..256 {
        starts[d + 1] = starts[d] + counts[d];
    }
    let mut next = starts;
    for d in 0..256 {
        while next[d] < starts[d + 1] {
            let mut x = p[next[d]];
            loop {
                let e = ((x.prefix >> shift) & 0xFF) as usize;
                if e == d {
                    break;
                }
                std::mem::swap(&mut x, &mut p[next[e]]);
                next[e] += 1;
            }
            p[next[d]] = x;
            next[d] += 1;
        }
    }
    let mut buckets: Vec<&mut [Pair]> = Vec::with_capacity(256);
    let mut rest: &mut [Pair] = &mut p;
    for d in 0..256 {
        let (b, r) = rest.split_at_mut(starts[d + 1] - starts[d]);
        buckets.push(b);
        rest = r;
    }
    buckets
        .into_par_iter()
        .for_each(|b| b.sort_unstable_by(|x, y| cmp_pairs(keys, skip, x, y)));
    into_rows(p)
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

/// A permutation bucketed by the two key bytes after the common prefix, each
/// bucket then sorted as prefix pairs on every core (`sort_buckets`). Peaks at
/// the permutation plus the pairs of the buckets being sorted at once.
pub fn buckets_par<K: Keys + ?Sized>(keys: &K) -> Vec<u32> {
    buckets(keys, (keys.len() / 64).max(1 << 16))
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
    fn strategies_agree() {
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
        for f in [
            perm,
            perm_par,
            pairs,
            pairs_par,
            pairs_lsd,
            pairs_msd_par,
            buckets_par,
        ] {
            let o = f(&keys);
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
        for f in [buckets_par, pairs_par, perm] {
            let o = f(&dup);
            let got: Vec<&[u8]> = o.iter().map(|&i| dup.key(i as usize)).collect();
            assert_eq!(got, [b"1", b"1", b"2", b"2"]);
        }
    }
}
