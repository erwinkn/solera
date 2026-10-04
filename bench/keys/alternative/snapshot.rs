//! A20 measurement fixture, NOT a production index.
//! Immutable fixed-range leaves, packed per commit, two directory levels.
//! Numeric keys and fixed 16-byte rows favor CoW; see README.md for omissions.
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    env, fs,
    hint::black_box,
    sync::Arc,
    time::Instant,
};
const FAN: usize = 128;
#[derive(Clone, Copy)]
struct Row {
    key: u64,
    generation: u64,
}
#[derive(Clone)]
struct Leaf {
    pack: u64,
    offset: usize,
    rows: Vec<Row>,
}
type Group = Vec<Arc<Leaf>>;
type Root = Vec<Arc<Group>>;
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}
fn rss() -> usize {
    fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find(|l| l.starts_with("VmHWM:"))
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<usize>()
        .unwrap()
        * 1024
}
fn leaf(root: &Root, i: usize) -> &Arc<Leaf> {
    &root[i / FAN][i % FAN]
}
fn build(n: usize, size: usize) -> Root {
    let leaves: Vec<_> = (0..n.div_ceil(size))
        .map(|i| {
            Arc::new(Leaf {
                pack: 0,
                offset: i * size * 16,
                rows: (i * size..((i + 1) * size).min(n))
                    .map(|j| Row {
                        key: j as u64,
                        generation: 1,
                    })
                    .collect(),
            })
        })
        .collect();
    leaves.chunks(FAN).map(|s| Arc::new(s.to_vec())).collect()
}
fn updates(
    rng: &mut Rng,
    n: usize,
    count: usize,
    generation: u64,
    hot: bool,
    churn: bool,
) -> BTreeMap<usize, u64> {
    let mut out = BTreeMap::new();
    while out.len() < count {
        let k = if hot && rng.next() % 2 == 0 {
            (rng.next() as usize % (n / 100)) * 100
        } else {
            rng.next() as usize % n
        };
        let g = if churn && rng.next() % 10 == 0 {
            0
        } else {
            generation
        };
        out.insert(k, g);
    }
    out
}
fn apply(
    root: &Root,
    size: usize,
    changes: &BTreeMap<usize, u64>,
    pack: u64,
) -> (Root, usize, usize, usize) {
    let mut out = root.clone();
    let mut dirty = BTreeMap::<usize, Vec<(usize, u64)>>::new();
    for (&k, &g) in changes {
        if leaf(root, k / size).rows[k % size].generation != g {
            dirty.entry(k / size).or_default().push((k % size, g));
        }
    }
    let groups = dirty.keys().map(|k| k / FAN).collect::<BTreeSet<_>>();
    // 48-byte on-wire reference is a cost assumption, not Vec/Arc memory size.
    let metadata = (root.len() + groups.iter().map(|&g| root[g].len()).sum::<usize>()) * 48;
    let mut copied = 0;
    let mut offset = 0;
    for (&p, edits) in &dirty {
        let mut rows = leaf(root, p).rows.clone();
        copied += rows.len();
        for &(i, g) in edits {
            rows[i].generation = g;
        }
        let bytes = rows.len() * 16;
        Arc::make_mut(&mut out[p / FAN])[p % FAN] = Arc::new(Leaf { pack, offset, rows });
        offset += bytes;
    }
    (out, copied, dirty.len(), metadata)
}
#[derive(Default)]
struct Read {
    refs: BTreeSet<(u64, usize, usize)>,
    count: usize,
    sum: u64,
}
impl Read {
    fn add(&mut self, l: &Leaf) {
        self.refs.insert((l.pack, l.offset, l.rows.len() * 16));
    }
    fn costs(&self, gap: usize) -> (usize, usize) {
        let mut gets = 0;
        let mut bytes = 0;
        let mut previous = None;
        for &(p, o, len) in &self.refs {
            bytes += len;
            match previous {
                Some((q, end, start))
                    if p == q
                        && o >= end
                        && o - end <= gap
                        && o + len - start <= 16 * 1024 * 1024 =>
                {
                    bytes += o - end;
                    previous = Some((p, o + len, start));
                }
                _ => {
                    gets += 1;
                    previous = Some((p, o + len, o));
                }
            }
        }
        (gets, bytes)
    }
}
fn diff(a: &Root, b: &Root, after: Option<u64>, limit: usize) -> Read {
    let mut read = Read::default();
    for (ga, gb) in a.iter().zip(b) {
        if Arc::ptr_eq(ga, gb) {
            continue;
        }
        for (la, lb) in ga.iter().zip(gb.iter()) {
            if Arc::ptr_eq(la, lb) || after.is_some_and(|x| lb.rows.last().unwrap().key <= x) {
                continue;
            }
            read.add(la);
            read.add(lb);
            for (ra, rb) in la.rows.iter().zip(&lb.rows) {
                if after.is_some_and(|x| rb.key <= x) {
                    continue;
                }
                if ra.generation != rb.generation {
                    read.count += 1;
                    read.sum = read.sum.wrapping_add(rb.key ^ rb.generation);
                    if read.count == limit {
                        return read;
                    }
                }
            }
        }
    }
    read
}
fn points(root: &Root, size: usize, keys: &[usize]) -> Read {
    let mut r = Read::default();
    for &k in keys {
        let l = leaf(root, k / size);
        r.add(l);
        r.sum = r.sum.wrapping_add(l.rows[k % size].generation);
    }
    r.count = keys.len();
    r
}
fn scan(root: &Root, after: Option<u64>, limit: usize) -> Read {
    let mut r = Read::default();
    for g in root {
        for l in g.iter() {
            if after.is_some_and(|a| l.rows.last().unwrap().key <= a) {
                continue;
            }
            r.add(l);
            for row in &l.rows {
                if row.generation != 0 && after.is_none_or(|a| row.key > a) {
                    r.count += 1;
                    r.sum = r.sum.wrapping_add(row.key ^ row.generation);
                    if r.count == limit {
                        return r;
                    }
                }
            }
        }
    }
    r
}
fn emit_read(name: &str, r: &Read, seconds: f64) {
    let (gets, bytes) = r.costs(0);
    println!("{{\"kind\":\"read\",\"name\":\"{name}\",\"count\":{},\"data_gets\":{gets},\"raw_bytes\":{bytes},\"local_s\":{seconds},\"checksum\":{}}}",r.count,r.sum);
    for gap in [65536, 1048576, 16777216] {
        let (g, b) = r.costs(gap);
        println!("{{\"kind\":\"read_plan\",\"name\":\"{name}\",\"gap\":{gap},\"data_gets\":{g},\"raw_bytes\":{b}}}");
    }
}
fn retained(roots: &[Root], pack_sizes: &BTreeMap<u64, usize>) -> (usize, usize, usize, usize) {
    let mut packs = HashSet::new();
    let mut seen = HashSet::new();
    let mut groups = HashSet::new();
    let mut pages = 0;
    let mut bytes = 0;
    for r in roots {
        for g in r {
            groups.insert(Arc::as_ptr(g) as usize);
            for l in g.iter() {
                packs.insert(l.pack);
                if seen.insert(Arc::as_ptr(l) as usize) {
                    pages += 1;
                    bytes += l.rows.len() * 16;
                }
            }
        }
    }
    (
        pages,
        bytes,
        groups.len() * FAN * 48 + roots.iter().map(|r| r.len() * 48).sum::<usize>(),
        packs.iter().map(|p| pack_sizes[p]).sum(),
    )
}
fn main() {
    let args: Vec<_> = env::args().collect();
    let n: usize = args[1].parse().unwrap();
    let size: usize = args[2].parse().unwrap();
    let commits: usize = args[3].parse().unwrap();
    let batch: usize = args[4].parse().unwrap();
    let mode = &args[5];
    let hot = mode.contains("hot");
    let churn = mode.contains("churn");
    let history = mode.contains("history");
    let mut rng = Rng(11);
    let mut root = build(n, size);
    let mut oracle = vec![1u64; n];
    let mut targets = BTreeMap::<usize, (Root, Vec<u64>)>::new();
    let mut pinned = Vec::<Root>::new();
    let mut pin_expected = Vec::<Vec<u64>>::new();
    let sample: Vec<_> = (0..1000).map(|_| rng.next() as usize % n).collect();
    let mut pack_sizes = BTreeMap::from([(0u64, n * 16)]);
    let mut rows = 0;
    let mut pages = 0;
    let mut metadata = 0;
    let mut local = 0.;
    let mut commit_times = Vec::new();
    for c in 1..=commits {
        let changes = updates(&mut rng, n, batch, c as u64 + 1, hot, churn);
        let t = Instant::now();
        let (next, r, p, m) = apply(&root, size, &changes, c as u64);
        let dt = t.elapsed().as_secs_f64();
        local += dt;
        commit_times.push(dt);
        root = next;
        rows += r;
        pages += p;
        metadata += m;
        pack_sizes.insert(c as u64, r * 16);
        for (&k, &g) in &changes {
            oracle[k] = g;
        }
        if history {
            if [10, 1000, 10000]
                .iter()
                .any(|&d| commits >= d && c == commits - d)
            {
                targets.insert(commits - c, (root.clone(), oracle.clone()));
            }
            if c >= commits.saturating_sub(10000) && (c - commits.saturating_sub(10000)) % 100 == 0
            {
                pinned.push(root.clone());
                pin_expected.push(sample.iter().map(|&k| oracle[k]).collect());
            }
        }
    }
    commit_times.sort_by(f64::total_cmp);
    println!("{{\"kind\":\"write\",\"n\":{n},\"page_rows\":{size},\"commits\":{commits},\"batch\":{batch},\"mode\":\"{mode}\",\"copied_rows\":{rows},\"pages\":{pages},\"estimated_metadata_bytes\":{metadata},\"local_s\":{local},\"p50_ms\":{},\"p95_ms\":{},\"rss_hwm_bytes\":{}}}",commit_times[commits/2]*1000.,commit_times[commits*95/100]*1000.,rss());
    for (behind, (old, expected)) in &targets {
        for k in 0..n {
            assert_eq!(leaf(old, k / size).rows[k % size].generation, expected[k]);
            assert_eq!(leaf(&root, k / size).rows[k % size].generation, oracle[k]);
        }
        let t = Instant::now();
        let r = diff(old, &root, None, usize::MAX);
        let dt = t.elapsed().as_secs_f64();
        let expected_count = expected.iter().zip(&oracle).filter(|(a, b)| a != b).count();
        let expected_sum = expected
            .iter()
            .zip(&oracle)
            .enumerate()
            .filter(|(_, (a, b))| a != b)
            .map(|(k, (_, g))| k as u64 ^ g)
            .fold(0u64, |a, b| a.wrapping_add(b));
        assert_eq!((r.count, r.sum), (expected_count, expected_sum));
        emit_read(&format!("diff_{behind}_full"), &r, dt);
        let t = Instant::now();
        let r = diff(old, &root, None, 100000);
        emit_read(
            &format!("diff_{behind}_first100k"),
            &r,
            t.elapsed().as_secs_f64(),
        );
        let t = Instant::now();
        let r = points(old, size, &sample);
        assert_eq!(r.sum, sample.iter().map(|&k| expected[k]).sum());
        emit_read(&format!("points_{behind}"), &r, t.elapsed().as_secs_f64());
    }
    let t = Instant::now();
    let r = points(&root, size, &sample);
    assert_eq!(r.sum, sample.iter().map(|&k| oracle[k]).sum());
    emit_read("head_points1000", &r, t.elapsed().as_secs_f64());
    let t = Instant::now();
    let mut sum = 0u64;
    for _ in 0..100 {
        sum = sum.wrapping_add(black_box(points(&root, size, &sample)).sum);
    }
    black_box(sum);
    println!(
        "{{\"kind\":\"warm\",\"points\":1000,\"local_ms\":{}}}",
        t.elapsed().as_secs_f64() * 10.
    );
    let t = Instant::now();
    let r = scan(&root, Some(n as u64 / 2), 100000);
    emit_read("head_scan100k", &r, t.elapsed().as_secs_f64());
    if history {
        for (r, expected) in pinned.iter().zip(pin_expected) {
            for (&k, &g) in sample.iter().zip(&expected) {
                assert_eq!(leaf(r, k / size).rows[k % size].generation, g);
            }
        }
        for how_many in [1, 2, 11, pinned.len()] {
            let (p, b, m, pack_bytes) = retained(&pinned[pinned.len() - how_many..], &pack_sizes);
            println!("{{\"kind\":\"retention\",\"roots\":{how_many},\"leaf_pages\":{p},\"raw_bytes\":{b},\"estimated_metadata_bytes\":{m},\"retained_pack_raw_leaf_bytes\":{pack_bytes}}}");
        }
    }
    // Samples of actual head leaves for a separate generic string-key codec measurement.
    if args.len() > 6 {
        let mut bytes = Vec::new();
        for i in (0..n.div_ceil(size))
            .step_by((n.div_ceil(size) / 128).max(1))
            .take(128)
        {
            for row in &leaf(&root, i).rows {
                bytes.extend_from_slice(&row.key.to_le_bytes());
                bytes.extend_from_slice(&row.generation.to_le_bytes());
            }
        }
        fs::write(&args[6], bytes).unwrap();
    }
    println!(
        "{{\"kind\":\"verification\",\"status\":\"passed\",\"rss_hwm_bytes\":{}}}",
        rss()
    );
}
