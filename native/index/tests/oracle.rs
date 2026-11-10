//! Property tests: random histories, written commit by commit, merged and
//! folded at random cuts, every query compared with a brute-force fold of the
//! history (the format benchmark's check). `ORACLE_SEEDS` sets how many.

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStoreExt;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use solera_index::*;
use std::sync::Arc;

/// Allocations past 1 GiB fail (and abort the test), so a damaged length can
/// never pass for a buffer size.
struct Capped;

unsafe impl std::alloc::GlobalAlloc for Capped {
    unsafe fn alloc(&self, l: std::alloc::Layout) -> *mut u8 {
        if l.size() > 1 << 30 {
            return std::ptr::null_mut();
        }
        unsafe { std::alloc::System.alloc(l) }
    }

    unsafe fn dealloc(&self, p: *mut u8, l: std::alloc::Layout) {
        unsafe { std::alloc::System.dealloc(p, l) }
    }

    unsafe fn realloc(&self, p: *mut u8, l: std::alloc::Layout, size: usize) -> *mut u8 {
        if size > 1 << 30 {
            return std::ptr::null_mut();
        }
        unsafe { std::alloc::System.realloc(p, l, size) }
    }
}

#[global_allocator]
static ALLOC: Capped = Capped;

#[derive(Clone, PartialEq, Eq, Debug)]
enum V {
    I(u64),
    B(Vec<u8>),
}

type O = Option<V>;
/// A key's entries: commit, new version, replaced version.
type Entries = Vec<(u64, O, O)>;

fn ver(v: &O) -> Version<'_> {
    match v {
        None => Version::Absent,
        Some(V::I(n)) => Version::Int(*n),
        Some(V::B(b)) => Version::Bytes(b),
    }
}

fn own(v: Version<'_>) -> O {
    match v {
        Version::Absent => None,
        Version::Int(n) => Some(V::I(n)),
        Version::Bytes(b) => Some(V::B(b.to_vec())),
    }
}

/// A key, its entries: (commit, new, replaced).
type Got = Vec<(Vec<u8>, Vec<(u64, O, O)>)>;

struct History {
    keys: Vec<Vec<u8>>,
    /// commits[c]: (key index, new, replaced), by key; [0] is empty.
    commits: Vec<Vec<(usize, O, O)>>,
    /// states[c][k]: k's version after commit c.
    states: Vec<Vec<O>>,
}

impl History {
    fn gen(rng: &mut SmallRng) -> History {
        let dirs = ["a", "a/b", "ab", "b", "é", "z/z/z", "a/b/c", "€😀"];
        let mut keys: Vec<Vec<u8>> = (0..rng.random_range(1..80))
            .map(|_| {
                let mut k = dirs[rng.random_range(0..dirs.len())].to_string();
                k.push('/');
                for _ in 0..rng.random_range(1..5) {
                    k.push(b"abc012"[rng.random_range(0..6)] as char);
                }
                k.into_bytes()
            })
            .collect();
        keys.sort();
        keys.dedup();
        let n = rng.random_range(1..40);
        let p = rng.random_range(0.05..0.6);
        let mut state: Vec<O> = vec![None; keys.len()];
        let mut h = History {
            keys,
            commits: vec![vec![]],
            states: vec![state.clone()],
        };
        for c in 1..=n as u64 {
            let mut changes = Vec::new();
            for (k, cur) in state.iter_mut().enumerate() {
                if !rng.random_bool(p) {
                    continue;
                }
                let new = if cur.is_some() && rng.random_bool(0.3) {
                    None
                } else {
                    match rng.random_range(0..3) {
                        0 => Some(V::I(rng.random_range(1..5))),
                        1 => Some(V::I(c * 100 + rng.random_range(0..3))),
                        _ => Some(V::B(
                            ["x", "y", "zz", ""][rng.random_range(0..4)]
                                .as_bytes()
                                .to_vec(),
                        )),
                    }
                };
                if new == *cur {
                    continue;
                }
                changes.push((k, new.clone(), cur.clone()));
                *cur = new;
            }
            h.commits.push(changes);
            h.states.push(state.clone());
        }
        h
    }

    fn head(&self) -> u64 {
        self.commits.len() as u64 - 1
    }

    /// The newest commit at or below `c` that touched key `k`.
    fn set_at(&self, k: usize, c: u64) -> Option<u64> {
        (1..=c)
            .rev()
            .find(|&x| self.commits[x as usize].iter().any(|e| e.0 == k))
    }

    fn entry(&self, k: usize, c: u64) -> Option<&(usize, O, O)> {
        self.commits[c as usize].iter().find(|e| e.0 == k)
    }
}

fn glob_regex(g: &str) -> regex::bytes::Regex {
    let mut out = String::from("(?s-u)^(?:");
    let mut i = 0;
    let b = g.as_bytes();
    while i < b.len() {
        if g[i..].starts_with("**/") {
            out.push_str("(?:.*/)?");
            i += 3;
        } else if g[i..].starts_with("**") {
            out.push_str(".*");
            i += 2;
        } else if b[i] == b'*' {
            out.push_str("[^/]*");
            i += 1;
        } else if b[i] == b'?' {
            out.push_str("(?:[\\x00-\\x2e\\x30-\\x7f]|[\\xc0-\\xdf][\\x80-\\xbf]|[\\xe0-\\xef][\\x80-\\xbf]{2}|[\\xf0-\\xf7][\\x80-\\xbf]{3})");
            i += 1;
        } else {
            let ch = g[i..].chars().next().unwrap();
            out.push_str(&regex::escape(&ch.to_string()));
            i += ch.len_utf8();
        }
    }
    out.push_str(")$");
    regex::bytes::Regex::new(&out).unwrap()
}

struct Q {
    q: Query,
    inc: Vec<regex::bytes::Regex>,
    exc: Vec<regex::bytes::Regex>,
}

impl Q {
    fn gen(rng: &mut SmallRng, h: &History) -> Q {
        let key = |rng: &mut SmallRng| -> Vec<u8> {
            if rng.random_bool(0.7) {
                h.keys[rng.random_range(0..h.keys.len())].clone()
            } else {
                ["", "a", "a/b/", "b/zz", "é/", "zz"][rng.random_range(0..6)]
                    .as_bytes()
                    .to_vec()
            }
        };
        let mut q = Query::default();
        if rng.random_bool(0.3) {
            q.lo = Some(key(rng));
        }
        if rng.random_bool(0.3) {
            q.hi = Some(key(rng));
        }
        if rng.random_bool(0.3) {
            q.after = Some(key(rng));
        }
        if rng.random_bool(0.4) {
            q.limit = Some(rng.random_range(0..8));
        }
        let (mut inc, mut exc) = (Vec::new(), Vec::new());
        if rng.random_bool(0.4) {
            let pieces = [
                "a", "b", "/", "*", "**", "**/", "?", "é", "a/b", "ab", "0", "z", "€", "😀",
            ];
            let glob = |rng: &mut SmallRng| -> String {
                (0..rng.random_range(1..5))
                    .map(|_| pieces[rng.random_range(0..pieces.len())])
                    .collect()
            };
            let (mut gi, mut ge) = (Vec::new(), Vec::new());
            for _ in 0..rng.random_range(0..3) {
                gi.push(glob(rng));
            }
            for _ in 0..rng.random_range(0..2) {
                ge.push(glob(rng));
            }
            inc = gi.iter().map(|g| glob_regex(g)).collect();
            exc = ge.iter().map(|g| glob_regex(g)).collect();
            q.pattern = Some(Pattern::new(&gi, &ge).unwrap());
        }
        Q { q, inc, exc }
    }

    fn takes(&self, k: &[u8]) -> bool {
        let q = &self.q;
        q.lo.as_ref().is_none_or(|lo| k > lo.as_slice())
            && q.hi.as_ref().is_none_or(|hi| k <= hi.as_slice())
            && q.after.as_ref().is_none_or(|a| k > a.as_slice())
            && (self.inc.is_empty() || self.inc.iter().any(|r| r.is_match(k)))
            && !self.exc.iter().any(|r| r.is_match(k))
    }

    /// The first `limit` of `all`, and the next page's start if it stopped there.
    fn page(&self, mut all: Got) -> (Got, Option<Vec<u8>>) {
        match self.q.limit {
            Some(n) if all.len() as u64 >= n && n > 0 => {
                all.truncate(n as usize);
                let next = all.last().map(|x| x.0.clone());
                (all, next)
            }
            Some(0) => (Vec::new(), None),
            _ => (all, None),
        }
    }
}

fn got(p: &Page) -> (Got, Option<Vec<u8>>) {
    let r = &p.run;
    let v = (0..r.len())
        .map(|i| {
            (
                r.key(i).to_vec(),
                r.rows(i)
                    .map(|x| {
                        (
                            r.commit(x),
                            own(r.new_version(x)),
                            own(r.replaced_version(x)),
                        )
                    })
                    .collect(),
            )
        })
        .collect();
    (v, p.next.clone())
}

struct World {
    h: History,
    ix: ChangeIndex,
    layers: Vec<LayerRef>,
    floor: u64,
    head: u64,
    names: u64,
}

impl World {
    async fn commit(&mut self, c: u64) {
        let mut run = Run::new();
        for (k, new, rep) in &self.h.commits[c as usize] {
            run.push_key(&self.h.keys[*k]);
            run.push_entry(c, ver(new), ver(rep));
        }
        let live_before = self.h.states[c as usize - 1]
            .iter()
            .filter(|v| v.is_some())
            .count() as u64;
        let producer = format!("run-{c}");
        let nc = NewCommit {
            commit: c,
            time_ms: c * 1000,
            producer: producer.as_bytes(),
            live_before,
        };
        let l = self
            .ix
            .write_commit(&format!("c{c}"), nc, &run)
            .await
            .unwrap();
        self.layers.push(l);
        self.head = c;
    }

    async fn merge(&mut self, run: std::ops::Range<usize>, cut: u64) {
        self.names += 1;
        let out = self
            .ix
            .merge(&self.layers, run.clone(), cut, &format!("m{}", self.names))
            .await
            .unwrap();
        if run.start == 0 {
            self.floor = out.floor.unwrap_or(0);
        }
        self.layers.splice(run, [out]);
        check_layers(&self.layers).unwrap();
    }

    async fn check(&self, rng: &mut SmallRng, seed: u64) {
        let h = &self.h;
        let (floor, head) = (self.floor, self.head);
        let ctx = |what: &str| {
            format!(
                "seed {seed}: {what}, floor {floor}, head {head}, layers {:?}",
                self.layers
                    .iter()
                    .map(|l| (l.lo, l.hi, l.floor))
                    .collect::<Vec<_>>()
            )
        };
        for _ in 0..4 {
            let q = Q::gen(rng, h);
            let c1 = rng.random_range(floor..=head);
            let c2 = rng.random_range(c1..=head);
            // diff
            let want: Got = (0..h.keys.len())
                .filter(|&k| q.takes(&h.keys[k]))
                .filter(|&k| h.states[c1 as usize][k] != h.states[c2 as usize][k])
                .map(|k| {
                    let newest = (c1 + 1..=c2)
                        .rev()
                        .find(|&c| h.entry(k, c).is_some())
                        .unwrap();
                    let (a, b) = (
                        h.states[c2 as usize][k].clone(),
                        h.states[c1 as usize][k].clone(),
                    );
                    (h.keys[k].clone(), vec![(newest, a, b)])
                })
                .collect();
            let page = self.ix.diff(&self.layers, c1, c2, &q.q).await.unwrap();
            assert_eq!(
                got(&page),
                q.page(want),
                "{}",
                ctx(&format!("diff({c1}, {c2}, {:?})", q.q))
            );
            // scan and get: (key, commit that set the version, version)
            let present = |k: usize, c: u64| -> Option<(Vec<u8>, Entries)> {
                let v = h.states[c as usize][k].clone()?;
                Some((
                    h.keys[k].clone(),
                    vec![(h.set_at(k, c).unwrap(), Some(v), None)],
                ))
            };
            let strip = |(g, n): (Got, Option<Vec<u8>>)| -> (Got, Option<Vec<u8>>) {
                (
                    g.into_iter()
                        .map(|(k, es)| (k, es.into_iter().map(|e| (e.0, e.1, None)).collect()))
                        .collect(),
                    n,
                )
            };
            let want: Got = (0..h.keys.len())
                .filter(|&k| q.takes(&h.keys[k]))
                .filter_map(|k| present(k, c2))
                .collect();
            let page = self.ix.scan(&self.layers, c2, &q.q).await.unwrap();
            assert_eq!(
                strip(got(&page)),
                q.page(want),
                "{}",
                ctx(&format!("scan({c2}, {:?})", q.q))
            );
            let mut ask: Vec<Vec<u8>> = (0..rng.random_range(0..12))
                .map(|_| {
                    if rng.random_bool(0.8) {
                        h.keys[rng.random_range(0..h.keys.len())].clone()
                    } else {
                        b"a/nothing".to_vec()
                    }
                })
                .collect();
            let page = self.ix.get(&self.layers, c1, &ask, &q.q).await.unwrap();
            ask.sort();
            ask.dedup();
            let want: Got = (0..h.keys.len())
                .filter(|&k| q.takes(&h.keys[k]) && ask.contains(&h.keys[k]))
                .filter_map(|k| present(k, c1))
                .collect();
            assert_eq!(
                strip(got(&page)),
                q.page(want),
                "{}",
                ctx(&format!("get({c1}, {ask:?}, {:?})", q.q))
            );
            // history: every entry above the floor, and the state at the floor
            let want: Got = (0..h.keys.len())
                .filter(|&k| q.takes(&h.keys[k]))
                .filter_map(|k| {
                    let mut es: Vec<(u64, O, O)> = (floor + 1..=head)
                        .rev()
                        .filter_map(|c| h.entry(k, c).map(|e| (c, e.1.clone(), e.2.clone())))
                        .collect();
                    if floor > 0 {
                        if let Some(v) = &h.states[floor as usize][k] {
                            es.push((h.set_at(k, floor).unwrap(), Some(v.clone()), None));
                        }
                    }
                    (!es.is_empty()).then(|| (h.keys[k].clone(), es))
                })
                .collect();
            let page = self.ix.history(&self.layers, &q.q).await.unwrap();
            assert_eq!(
                got(&page),
                q.page(want),
                "{}",
                ctx(&format!("history({:?})", q.q))
            );
        }
        // the entries of a window of commits above the floor
        let (w1, w2) = (
            rng.random_range(floor + 1..=head + 1),
            rng.random_range(floor..=head + 1),
        );
        let q = Q::gen(rng, h);
        let want: Got = (0..h.keys.len())
            .filter(|&k| q.takes(&h.keys[k]))
            .filter_map(|k| {
                let es: Vec<(u64, O, O)> = (w1..=w2.min(head))
                    .rev()
                    .filter_map(|c| h.entry(k, c).map(|e| (c, e.1.clone(), e.2.clone())))
                    .collect();
                (!es.is_empty()).then(|| (h.keys[k].clone(), es))
            })
            .collect();
        let page = self.ix.window(&self.layers, w1..=w2, &q.q).await.unwrap();
        assert_eq!(
            got(&page),
            q.page(want),
            "{}",
            ctx(&format!("window({w1}..={w2}, {:?})", q.q))
        );
        // commit headers above the floor
        let hs = self.ix.commits(&self.layers, 0, head).await.unwrap();
        let want: Vec<u64> = (floor + 1..=head).collect();
        assert_eq!(
            hs.iter().map(|x| x.commit).collect::<Vec<_>>(),
            want,
            "{}",
            ctx("commits")
        );
        for x in &hs {
            let es = &h.commits[x.commit as usize];
            let count =
                |f: &dyn Fn(&(usize, O, O)) -> bool| es.iter().filter(|e| f(e)).count() as u64;
            assert_eq!(x.added, count(&|e| e.2.is_none()));
            assert_eq!(x.removed, count(&|e| e.1.is_none()));
            assert_eq!(x.updated, count(&|e| e.1.is_some() && e.2.is_some()));
            assert_eq!(
                x.live,
                h.states[x.commit as usize]
                    .iter()
                    .filter(|v| v.is_some())
                    .count() as u64
            );
            assert_eq!(x.producer, format!("run-{}", x.commit).into_bytes());
        }
        // below the floor nothing is answered
        if floor > 0 {
            assert!(matches!(
                self.ix
                    .scan(&self.layers, floor - 1, &Query::default())
                    .await,
                Err(Error::Invalid(_))
            ));
        }
    }
}

fn index(rng: &mut SmallRng, rt: &tokio::runtime::Runtime) -> ChangeIndex {
    let store = Store::new(
        Arc::new(InMemory::new()),
        Path::from("t"),
        rt.handle().clone(),
        StoreOptions {
            memory_bytes: [0, 2_000, 1 << 30][rng.random_range(0..3)],
            coalesce: [0, 64, 1 << 16][rng.random_range(0..3)],
            max_get: [100, 1 << 20][rng.random_range(0..2)],
            one_get: [0, 300, 1 << 20][rng.random_range(0..3)],
            max_inflight: rng.random_range(1..8),
            part_bytes: [200, 1 << 20][rng.random_range(0..2)],
            cache_batch: [0, 300, 1 << 20][rng.random_range(0..3)],
            disk: rng.random_bool(0.3).then(|| DiskOptions {
                dir: scratch("oracle").join(rng.random::<u64>().to_string()),
                bytes: [1_000, 1 << 30][rng.random_range(0..2)],
                store: "oracle".into(),
            }),
        },
    )
    .unwrap();
    ChangeIndex::new(
        store,
        WriteOptions {
            block_bytes: rng.random_range(1..400),
            level: 1,
            seg_blocks: rng.random_range(1..5),
        },
    )
}

/// A directory for one test's disk caches.
fn scratch(test: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("solera-index-{test}-{}", std::process::id()))
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn seeds() -> u64 {
    std::env::var("ORACLE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300)
}

#[test]
fn queries_and_merges_match_the_fold() {
    let rt = runtime();
    for seed in 0..seeds() {
        let mut rng = SmallRng::seed_from_u64(seed);
        let h = History::gen(&mut rng);
        let ix = index(&mut rng, &rt);
        rt.block_on(async {
            let mut w = World {
                h,
                ix,
                layers: Vec::new(),
                floor: 0,
                head: 0,
                names: 0,
            };
            let n = w.h.head();
            for c in 1..=n {
                w.commit(c).await;
                if rng.random_bool(0.4) {
                    let len = w.layers.len();
                    let cut = rng.random_range(w.floor..=w.head);
                    let run = if rng.random_bool(0.5) {
                        pick_merge(&w.layers, cut)
                    } else {
                        let a = rng.random_range(0..len);
                        Some(a..rng.random_range(a + 1..=len.min(a + 5)))
                    };
                    if let Some(run) = run {
                        w.merge(run, cut).await;
                    }
                }
                if rng.random_bool(0.3) || c == n {
                    w.check(&mut rng, seed).await;
                }
            }
            // fold everything at a random cut, then check again
            let cut = rng.random_range(w.floor..=w.head);
            let len = w.layers.len();
            w.merge(0..len, cut).await;
            w.check(&mut rng, seed).await;
        });
    }
    let _ = std::fs::remove_dir_all(scratch("oracle"));
}

#[test]
fn the_disk_cache_serves_reads_after_a_write_and_after_a_restart() {
    let rt = runtime();
    let mem = Arc::new(InMemory::new());
    let (dir, fresh_dir) = (scratch("disk"), scratch("disk-fresh"));
    let index = |disk: &std::path::Path| {
        let opts = StoreOptions {
            memory_bytes: 0,
            disk: Some(DiskOptions {
                dir: disk.to_path_buf(),
                bytes: 1 << 30,
                store: "t".into(),
            }),
            ..Default::default()
        };
        let store = Store::new(mem.clone(), Path::from("t"), rt.handle().clone(), opts).unwrap();
        ChangeIndex::new(store, WriteOptions::default())
    };
    let reopen = |disk: &std::path::Path| {
        let disk = disk.to_path_buf();
        let index = &index;
        async move {
            // the background fill lets go of the directory just after it counts
            for _ in 0..200 {
                if let Ok(ix) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| index(&disk)))
                {
                    return ix;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            panic!("the disk cache stayed locked")
        }
    };
    rt.block_on(async {
        let ix = index(&dir);
        let mut run = Run::new();
        for i in 0..5000u64 {
            run.push_key(format!("k{i:05}").as_bytes());
            run.push_entry(1, Version::Int(i), Version::Absent);
        }
        let nc = NewCommit {
            commit: 1,
            time_ms: 0,
            producer: b"",
            live_before: 0,
        };
        let layers = vec![ix.write_commit("c1", nc, &run).await.unwrap()];
        // written through: no GET
        assert_eq!(ix_scan(&ix, &layers).await, 5000);
        assert_eq!(ix.store.stats.counts().gets, 0);
        // a restart finds the file
        drop(ix);
        let again = index(&dir);
        assert_eq!(ix_scan(&again, &layers).await, 5000);
        assert_eq!(again.store.stats.counts().gets, 0);
        // an empty cache reads from the store, then fills in the background
        let fresh = index(&fresh_dir);
        assert_eq!(ix_scan(&fresh, &layers).await, 5000);
        assert!(fresh.store.stats.counts().gets > 0);
        for _ in 0..200 {
            if fresh.store.stats.counts().fills == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(fresh.store.stats.counts().fills, 1);
        drop(fresh);
        let fresh = reopen(&fresh_dir).await;
        assert_eq!(ix_scan(&fresh, &layers).await, 5000);
        assert_eq!(fresh.store.stats.counts().gets, 0);
        assert!(fresh.store.stats.counts().disk_reads > 0);
    });
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(fresh_dir);
}

#[test]
fn bad_references_and_abandoned_reads_leave_the_index_working() {
    let rt = runtime();
    let mem = Arc::new(InMemory::new());
    let index = |prefix: &str, disk: Option<std::path::PathBuf>| {
        let opts = StoreOptions {
            max_inflight: 1,
            disk: disk.map(|dir| DiskOptions {
                dir,
                bytes: 1 << 30,
                store: prefix.into(),
            }),
            ..Default::default()
        };
        let store = Store::new(mem.clone(), Path::from(prefix), rt.handle().clone(), opts).unwrap();
        ChangeIndex::new(store, WriteOptions::default())
    };
    let nc = |c| NewCommit {
        commit: c,
        time_ms: 0,
        producer: b"",
        live_before: 0,
    };
    let one = |v: u64| {
        let mut run = Run::new();
        run.push_key(b"k");
        run.push_entry(1, Version::Int(v), Version::Absent);
        run
    };
    let dir = scratch("refs");
    std::fs::create_dir_all(&dir).unwrap();
    rt.block_on(async {
        let ix = index("t", None);
        let l1 = ix.write_commit("c1", nc(1), &one(1)).await.unwrap();
        let l2 = ix.write_commit("c2", nc(2), &Run::new()).await.unwrap();
        let q = Query::default();
        // a flag this reader does not know is refused, even on a layer no read opens
        let flagged = LayerRef {
            flags: 1,
            ..l2.clone()
        };
        assert!(matches!(
            ix.scan(&[l1.clone(), flagged], 2, &q).await,
            Err(Error::Format(_))
        ));
        // offsets out of order are refused before any read, and the index still reads
        let bad = LayerRef {
            top_start: l1.size + 1,
            ..l1.clone()
        };
        assert!(matches!(
            ix.scan(&[bad], 1, &q).await,
            Err(Error::Invalid(_))
        ));
        let bad = LayerRef {
            index_start: l1.top_start + 1,
            ..l1.clone()
        };
        assert!(matches!(
            ix.scan(&[bad], 1, &q).await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(
            ix.scan(std::slice::from_ref(&l1), 1, &q)
                .await
                .unwrap()
                .run
                .len(),
            1
        );
        // a reference that does not match its file is refused with the top level cached too
        let wrong = LayerRef {
            entries: 5,
            ..l1.clone()
        };
        assert!(matches!(
            ix.scan(&[wrong], 1, &q).await,
            Err(Error::Format(_))
        ));
        let moved = LayerRef {
            top_start: l1.top_start + 1,
            ..l1.clone()
        };
        assert!(matches!(
            ix.scan(&[moved], 1, &q).await,
            Err(Error::Format(_))
        ));
        let past = LayerRef {
            top_start: u64::MAX,
            ..l1.clone()
        };
        assert!(matches!(
            ix.scan(&[past], 1, &q).await,
            Err(Error::Invalid(_))
        ));
        // a read dropped mid-GET gives its GET slot back: another layer's read
        // still runs with one slot
        let slow = Arc::new(object_store::throttle::ThrottledStore::new(
            InMemory::new(),
            object_store::throttle::ThrottleConfig {
                wait_get_per_call: std::time::Duration::from_millis(30),
                ..Default::default()
            },
        ));
        let slow_index = |slots| {
            let opts = StoreOptions {
                max_inflight: slots,
                ..Default::default()
            };
            let store =
                Store::new(slow.clone(), Path::from(""), rt.handle().clone(), opts).unwrap();
            ChangeIndex::new(store, WriteOptions::default())
        };
        let w = slow_index(4);
        let la = [w.write_commit("la", nc(1), &one(1)).await.unwrap()];
        let lb = [w.write_commit("lb", nc(1), &one(2)).await.unwrap()];
        for i in 0..5 {
            let ix = slow_index(1);
            let wait = std::time::Duration::from_millis(5 + i * 20);
            let _ = tokio::time::timeout(wait, ix.scan(&la, 1, &q)).await;
            let read = tokio::time::timeout(std::time::Duration::from_secs(5), ix.scan(&lb, 1, &q));
            assert_eq!(read.await.expect("a GET slot leaked").unwrap().run.len(), 1);
        }
        // a damaged length in the commits section is a format error, not an allocation
        let got = mem.get(&Path::from("t/c1")).await.unwrap();
        let mut b = got.bytes().await.unwrap().to_vec();
        let t = b.len() - 48;
        let commits_start = u64::from_le_bytes(b[t + 8..t + 16].try_into().unwrap());
        b[commits_start as usize + 3] ^= 0x80;
        mem.put(&Path::from("t/c1-damaged"), b.into())
            .await
            .unwrap();
        let damaged = LayerRef {
            path: "c1-damaged".into(),
            ..l1.clone()
        };
        assert!(matches!(
            ix.commits(&[damaged], 1, 1).await,
            Err(Error::Format(_))
        ));
        // a read that failed is not remembered: once the file is there, it reads
        let late = LayerRef {
            path: "late".into(),
            ..l1.clone()
        };
        assert!(ix.scan(std::slice::from_ref(&late), 1, &q).await.is_err());
        let c1 = mem
            .get(&Path::from("t/c1"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        mem.put(&Path::from("t/late"), c1.into()).await.unwrap();
        assert_eq!(ix.scan(&[late], 1, &q).await.unwrap().run.len(), 1);
        // a path too long for a cache file name is written, just not kept on disk
        let long_dir = scratch("refs-long");
        let long = index("long", Some(long_dir.clone()));
        let path = ["d".repeat(100), "e".repeat(100), "f".repeat(100)].join("/");
        let ll = long.write_commit(&path, nc(1), &one(3)).await.unwrap();
        let rl = long.scan(&[ll], 1, &q).await.unwrap();
        assert_eq!(rl.run.new_version(0), Version::Int(3));
        drop(long);
        let _ = std::fs::remove_dir_all(long_dir);
        // a trailer cannot make the commits section larger than the format allows
        let c2 = mem
            .get(&Path::from("t/c2"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let mut tail = c2[l2.top_start as usize..].to_vec();
        let t = tail.len() - 48;
        let far = 1u64 << 40;
        tail[t..t + 8].copy_from_slice(&far.to_le_bytes());
        tail[t + 8..t + 16].copy_from_slice(&0u64.to_le_bytes());
        tail[t + 16..t + 24].copy_from_slice(&far.to_le_bytes());
        let check = xxhash_rust::xxh3::xxh3_64(&tail[t..t + 36]) as u32;
        tail[t + 36..t + 40].copy_from_slice(&check.to_le_bytes());
        let got = format::Top::parse(&tail, far, far + tail.len() as u64);
        assert!(matches!(got, Err(Error::Format(_))));
        // a disk cache directory serves one store at a time, never another's files
        let a = index("a", Some(dir.clone()));
        let again = StoreOptions {
            disk: Some(DiskOptions {
                dir: dir.clone(),
                bytes: 1 << 30,
                store: "a".into(),
            }),
            ..Default::default()
        };
        assert!(matches!(
            Store::new(mem.clone(), Path::from("a"), rt.handle().clone(), again),
            Err(Error::Invalid(_))
        ));
        let la = a.write_commit("c1", nc(1), &one(1)).await.unwrap();
        drop(a);
        let b = index("b", Some(dir.clone()));
        let lb = b.write_commit("c1", nc(1), &one(2)).await.unwrap();
        let rb = b.scan(&[lb], 1, &q).await.unwrap();
        assert_eq!(rb.run.new_version(0), Version::Int(2));
        drop(b);
        let a = index("a", Some(dir.clone()));
        let ra = a.scan(&[la], 1, &q).await.unwrap();
        assert_eq!(ra.run.new_version(0), Version::Int(1));
    });
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_block_is_never_decoded_past_what_its_frame_holds() {
    let c = zstd::bulk::compress(b"x", 1).unwrap();
    let mut run = Run::new();
    let got = format::Decoder::default().decode(&c, u32::MAX, 1, &mut run);
    assert!(matches!(got, Err(Error::Format(_))));
}

fn varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

#[test]
fn a_segment_cannot_point_a_block_outside_the_data_or_name_keys_past_the_limit() {
    let segment = |off: u64, clen: u64, key: usize, data_end: u64| {
        let mut raw = Vec::new();
        for v in [off, clen, 10, 1] {
            varint(&mut raw, v);
        }
        raw.extend_from_slice(&[0, 0, 0, 0, 0]);
        varint(&mut raw, key as u64);
        raw.extend(std::iter::repeat_n(b'k', key));
        let c = zstd::bulk::compress(&raw, 1).unwrap();
        let check = xxhash_rust::xxh3::xxh3_64(&c) as u32;
        format::parse_segment(&c, raw.len() as u32, check, 1, data_end)
    };
    assert!(segment(0, 100, 1, 100).is_ok());
    assert!(segment(0, 100, format::MAX_KEY, 100).is_ok());
    for (off, clen, key, end) in [
        (0, 101, 1, 100),
        (0, u32::MAX as u64, 1, 100),
        (0, 1 << 40, 1, 100),
        (u64::MAX, 1, 1, 100),
        (0, 1 << 30, 1, 1 << 31),
        (0, 100, format::MAX_KEY + 1, 100),
    ] {
        let got = segment(off, clen, key, end);
        assert!(
            matches!(got, Err(Error::Format(_))),
            "{off} {clen} {key} {end}"
        );
    }
}

#[test]
fn keys_past_the_limit_are_refused_when_written_and_when_read() {
    let mut w = Writer::new(WriteOptions::default(), 1, 1, None).unwrap();
    let long = vec![b'k'; format::MAX_KEY + 1];
    let got = w.push(&long, 1, Version::Int(1), Version::Absent);
    assert!(matches!(got, Err(Error::Invalid(_))));
    let mut raw = Vec::new();
    varint(&mut raw, 0);
    varint(&mut raw, long.len() as u64);
    raw.extend_from_slice(&long);
    varint(&mut raw, 1);
    let c = zstd::bulk::compress(&raw, 1).unwrap();
    let mut run = Run::new();
    let got = format::Decoder::default().decode(&c, raw.len() as u32, 1, &mut run);
    assert!(matches!(got, Err(Error::Format(_))));
}

#[test]
fn the_caches_do_what_they_promise() {
    let rt = runtime();
    let slow = Arc::new(object_store::throttle::ThrottledStore::new(
        InMemory::new(),
        object_store::throttle::ThrottleConfig {
            wait_get_per_call: std::time::Duration::from_millis(20),
            ..Default::default()
        },
    ));
    let small = WriteOptions {
        block_bytes: 1 << 10,
        ..Default::default()
    };
    let index = |opts: StoreOptions| {
        let store = Store::new(slow.clone(), Path::from("t"), rt.handle().clone(), opts).unwrap();
        ChangeIndex::new(store, small)
    };
    let nc = |c| NewCommit {
        commit: c,
        time_ms: 0,
        producer: b"",
        live_before: 0,
    };
    let layer = |i: u64| {
        let mut run = Run::new();
        for k in 0..5000u64 {
            run.push_key(format!("k{k:05}").as_bytes());
            run.push_entry(1, Version::Int(k * 7 + i), Version::Absent);
        }
        run
    };
    let (dir, q) = (scratch("caches"), Query::default());
    rt.block_on(async {
        let w = index(StoreOptions::default());
        let mut ls = Vec::new();
        for i in 0..5 {
            ls.push(
                w.write_commit(&format!("c{i}"), nc(1), &layer(i))
                    .await
                    .unwrap(),
            );
        }
        let one = [ls[0].clone()];
        // single flight: eight reads of one layer at once make one read's GETs
        let alone = index(StoreOptions::default());
        alone.scan(&one, 1, &q).await.unwrap();
        let shared = index(StoreOptions::default());
        let reads = (0..8).map(|_| shared.scan(&one, 1, &q));
        for r in futures::future::join_all(reads).await {
            assert_eq!(r.unwrap().run.len(), 5000);
        }
        let (a, s) = (alone.store.stats.counts(), shared.store.stats.counts());
        assert_eq!(s.gets, a.gets);
        assert!(s.waits > 0);
        // reads up to cache_batch are kept, bulk reads are not
        for (batch, kept) in [(64 << 20, true), (1 << 10, false)] {
            let ix = index(StoreOptions {
                cache_batch: batch,
                ..Default::default()
            });
            ix.scan(&one, 1, &q).await.unwrap();
            let before = ix.store.stats.counts().gets;
            ix.scan(&one, 1, &q).await.unwrap();
            assert_eq!(
                ix.store.stats.counts().gets == before,
                kept,
                "cache_batch {batch}"
            );
        }
        // the memory cache holds what it weighs, within its budget
        let budget = 16 << 10;
        let ix = index(StoreOptions {
            memory_bytes: budget,
            ..Default::default()
        });
        for l in &ls {
            ix.scan(std::slice::from_ref(l), 1, &q).await.unwrap();
            let held = ix.store.memory_bytes();
            assert!(held > 0 && held <= budget, "{held}");
        }
        // the disk cache keeps within its size, and drops a damaged copy
        let size = ls[0].size;
        let ix = index(StoreOptions {
            disk: Some(DiskOptions {
                dir: dir.clone(),
                bytes: size * 9 / 2,
                store: "t".into(),
            }),
            memory_bytes: 0,
            ..Default::default()
        });
        let mut kept = Vec::new();
        for i in 5..10 {
            kept.push(
                ix.write_commit(&format!("c{i}"), nc(1), &layer(i))
                    .await
                    .unwrap(),
            );
            assert!(ix.store.disk_bytes() <= size * 9 / 2);
        }
        let newest = ix.write_commit("c10", nc(1), &layer(10)).await.unwrap();
        let file = dir.join("c10");
        let mut b = std::fs::read(&file).unwrap();
        b[10] ^= 0xff;
        std::fs::write(&file, b).unwrap();
        let held = ix.store.disk_bytes();
        let got = ix.scan(std::slice::from_ref(&newest), 1, &q).await.unwrap();
        assert_eq!(got.run.new_version(0), Version::Int(10));
        assert_eq!(ix.store.disk_bytes(), held - newest.size);
        // and so does a read of the commits section alone
        let l9 = kept.pop().unwrap();
        let file = dir.join("c9");
        let mut b = std::fs::read(&file).unwrap();
        b[l9.index_start as usize - 1] ^= 0xff;
        std::fs::write(&file, b).unwrap();
        for _ in 0..2 {
            ix.commits(std::slice::from_ref(&l9), 1, 1).await.unwrap();
        }
    });
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_fill_that_cannot_be_kept_gives_its_room_back_once() {
    let rt = runtime();
    let mem = Arc::new(InMemory::new());
    let dir = scratch("refill");
    let index = |disk: bool| {
        let opts = StoreOptions {
            memory_bytes: 0,
            disk: disk.then(|| DiskOptions {
                dir: dir.clone(),
                bytes: 1 << 30,
                store: "t".into(),
            }),
            ..Default::default()
        };
        let store = Store::new(mem.clone(), Path::from("t"), rt.handle().clone(), opts).unwrap();
        ChangeIndex::new(store, WriteOptions::default())
    };
    let nc = NewCommit {
        commit: 1,
        time_ms: 0,
        producer: b"",
        live_before: 0,
    };
    let run = |n: u64| {
        let mut run = Run::new();
        for i in 0..n {
            run.push_key(format!("k{i:04}").as_bytes());
            run.push_entry(1, Version::Int(i), Version::Absent);
        }
        run
    };
    rt.block_on(async {
        let w = index(false);
        let la = w.write_commit("a", nc, &run(100)).await.unwrap();
        let lb = w.write_commit("b", nc, &run(1)).await.unwrap();
        let ix = index(true);
        // a directory where the file would go: the fill's rename fails
        std::fs::create_dir_all(dir.join("a")).unwrap();
        let wait_fill = |n: u64| {
            let ix = &ix;
            async move {
                for _ in 0..200 {
                    if ix.store.stats.counts().fills >= n {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        };
        ix.scan(&[la], 1, &Query::default()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(ix.store.stats.counts().fills, 0);
        ix.scan(std::slice::from_ref(&lb), 1, &Query::default())
            .await
            .unwrap();
        wait_fill(1).await;
        assert_eq!(ix.store.stats.counts().fills, 1);
        assert_eq!(ix.store.disk_bytes(), lb.size);
    });
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_commit_writer_writes_what_write_commit_writes() {
    let rt = runtime();
    let mem = Arc::new(InMemory::new());
    let opts = StoreOptions {
        part_bytes: 64 << 10,
        ..Default::default()
    };
    let store = Store::new(mem.clone(), Path::from("t"), rt.handle().clone(), opts).unwrap();
    let ix = ChangeIndex::new(store, WriteOptions::default());
    let nc = NewCommit {
        commit: 1,
        time_ms: 5,
        producer: b"p",
        live_before: 10,
    };
    let mut rng = SmallRng::seed_from_u64(1);
    let mut run = Run::new();
    for i in 0..20_000u64 {
        run.push_key(format!("k{i:06}").as_bytes());
        let v = Version::Int(rng.random());
        match i % 3 {
            0 => run.push_entry(1, v, Version::Absent),
            1 => run.push_entry(1, v, Version::Int(i)),
            _ => run.push_entry(1, Version::Absent, Version::Int(i)),
        }
    }
    rt.block_on(async {
        let a = ix.write_commit("a", nc, &run).await.unwrap();
        let mut w = ix.commit_writer("b", nc).unwrap();
        for i in 0..run.len() {
            let r = run.rows(i).start;
            w.push(run.key(i), run.new_version(r), run.replaced_version(r))
                .await
                .unwrap();
        }
        let b = w.finish().await.unwrap().unwrap();
        assert_eq!(
            LayerRef {
                path: "a".into(),
                ..b
            },
            a
        );
        let bytes = |p: &'static str| {
            let mem = mem.clone();
            async move {
                mem.get(&Path::from(p))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
            }
        };
        assert_eq!(bytes("t/a").await, bytes("t/b").await);
        let h = CommitHeader {
            commit: 1,
            time_ms: 5,
            producer: b"p".to_vec(),
            added: 6667,
            updated: 6667,
            removed: 6666,
            live: 10 + 6667 - 6666,
            min_key: b"k000000".to_vec(),
            max_key: b"k019999".to_vec(),
            ..Default::default()
        };
        assert_eq!(ix.commits(&[a], 1, 1).await.unwrap(), vec![h.clone()]);
        // the same bytes as the format's writer, given that header
        let mut fw = Writer::new(WriteOptions::default(), 1, 1, None).unwrap();
        for i in 0..run.len() {
            let r = run.rows(i).start;
            fw.push(run.key(i), 1, run.new_version(r), run.replaced_version(r))
                .unwrap();
        }
        fw.finish(&[h]).unwrap();
        assert_eq!(bytes("t/b").await, fw.take());
        // nothing pushed: nothing written
        let w = ix.commit_writer("e", nc).unwrap();
        assert!(w.finish().await.unwrap().is_none());
        assert!(mem.head(&Path::from("t/e")).await.is_err());
        // one entry per key, keys ascending
        let mut w = ix.commit_writer("d", nc).unwrap();
        w.push(b"k", Version::Int(1), Version::Absent)
            .await
            .unwrap();
        assert!(matches!(
            w.push(b"k", Version::Int(2), Version::Absent).await,
            Err(Error::Invalid(_))
        ));
        let mut w = ix.commit_writer("d", nc).unwrap();
        w.push(b"k", Version::Int(1), Version::Absent)
            .await
            .unwrap();
        assert!(matches!(
            w.push(b"j", Version::Int(2), Version::Absent).await,
            Err(Error::Invalid(_))
        ));
    });
}

#[test]
fn a_commit_count_past_its_section_is_a_format_error() {
    let raw = [0x80u8, 0x80, 0x40];
    let c = zstd::bulk::compress(&raw, 1).unwrap();
    let mut b = (raw.len() as u32).to_le_bytes().to_vec();
    b.extend((xxhash_rust::xxh3::xxh3_64(&c) as u32).to_le_bytes());
    b.extend(&c);
    match format::parse_commits(&b) {
        Err(Error::Format(m)) => assert!(m.contains("1048576 commit headers"), "{m}"),
        got => panic!("{got:?}"),
    }
}

#[test]
fn a_truncated_layer_is_a_format_error() {
    let rt = runtime();
    let dir = scratch("short");
    std::fs::create_dir_all(&dir).unwrap();
    let index = || {
        let local = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&dir).unwrap());
        let store = Store::new(
            local,
            Path::from(""),
            rt.handle().clone(),
            StoreOptions::default(),
        )
        .unwrap();
        ChangeIndex::new(store, WriteOptions::default())
    };
    let mut run = Run::new();
    run.push_key(b"k");
    run.push_entry(1, Version::Int(1), Version::Absent);
    let nc = NewCommit {
        commit: 1,
        time_ms: 0,
        producer: b"",
        live_before: 0,
    };
    let l = rt.block_on(index().write_commit("c1", nc, &run)).unwrap();
    let file = dir.join("c1");
    let b = std::fs::read(&file).unwrap();
    std::fs::write(&file, &b[..b.len() - 1]).unwrap();
    let got = rt.block_on(index().scan(&[l], 1, &Query::default()));
    let _ = std::fs::remove_dir_all(&dir);
    assert!(matches!(got, Err(Error::Format(_))), "{got:?}");
}

#[test]
fn write_throughs_stay_within_the_disk_cache() {
    let rt = runtime();
    let dir = scratch("tee");
    let slow = Arc::new(object_store::throttle::ThrottledStore::new(
        InMemory::new(),
        object_store::throttle::ThrottleConfig {
            wait_put_per_call: std::time::Duration::from_millis(300),
            ..Default::default()
        },
    ));
    let index = |disk: Option<u64>| {
        let opts = StoreOptions {
            disk: disk.map(|bytes| DiskOptions {
                dir: dir.clone(),
                bytes,
                store: "t".into(),
            }),
            ..Default::default()
        };
        let store = Store::new(slow.clone(), Path::from("t"), rt.handle().clone(), opts).unwrap();
        ChangeIndex::new(store, WriteOptions::default())
    };
    let one = |v: u64| {
        let mut run = Run::new();
        run.push_key(b"k");
        run.push_entry(1, Version::Int(v), Version::Absent);
        run
    };
    let nc = NewCommit {
        commit: 1,
        time_ms: 0,
        producer: b"",
        live_before: 0,
    };
    let in_dir = |dir: &std::path::Path| -> u64 {
        std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .filter(|e| !e.file_name().to_string_lossy().starts_with(".l"))
            .filter(|e| e.file_name() != ".store")
            .map(|e| e.metadata().unwrap().len())
            .sum()
    };
    rt.block_on(async {
        let size = index(None)
            .write_commit("w0", nc, &one(1))
            .await
            .unwrap()
            .size;
        let bytes = size * 9 / 2;
        let ix = index(Some(bytes));
        let runs: Vec<(String, Run)> = (1..=8).map(|i| (format!("w{i}"), one(i))).collect();
        let writes = futures::future::join_all(runs.iter().map(|(p, r)| ix.write_commit(p, nc, r)));
        let probe = async {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            in_dir(&dir)
        };
        let (done, used) = tokio::join!(writes, probe);
        assert!(done.iter().all(|w| w.is_ok()));
        assert!(
            used <= bytes,
            "{used} bytes on disk, the cache holds {bytes}"
        );
        assert!(in_dir(&dir) <= bytes && ix.store.disk_bytes() <= bytes);
    });
    let _ = std::fs::remove_dir_all(dir);
}

async fn ix_scan(ix: &ChangeIndex, layers: &[LayerRef]) -> usize {
    ix.scan(layers, 1, &Query::default())
        .await
        .unwrap()
        .run
        .len()
}

#[test]
fn merges_refuse_entries_that_do_not_chain() {
    let rt = runtime();
    let mut rng = SmallRng::seed_from_u64(1);
    let ix = index(&mut rng, &rt);
    rt.block_on(async {
        let mut run = Run::new();
        run.push_key(b"k");
        run.push_entry(1, Version::Int(1), Version::Absent);
        let nc = |c| NewCommit {
            commit: c,
            time_ms: 0,
            producer: b"",
            live_before: 0,
        };
        let l1 = ix.write_commit("c1", nc(1), &run).await.unwrap();
        let mut run = Run::new();
        run.push_key(b"k");
        run.push_entry(2, Version::Int(3), Version::Int(2));
        let l2 = ix
            .write_commit(
                "c2",
                NewCommit {
                    live_before: 1,
                    ..nc(2)
                },
                &run,
            )
            .await
            .unwrap();
        let layers = vec![l1.clone(), l2];
        let e = ix.merge(&layers, 0..2, 0, "m").await.unwrap_err();
        assert!(matches!(e, Error::Chain(_)), "{e}");
        // a key's first entry replaces nothing
        let mut run = Run::new();
        run.push_key(b"j");
        run.push_entry(1, Version::Int(1), Version::Int(7));
        let bad = ix.write_commit("b1", nc(1), &run).await.unwrap();
        let e = ix.merge(&[bad], 0..1, 1, "m2").await.unwrap_err();
        assert!(matches!(e, Error::Chain(_)), "{e}");
        // a merge that does not start at the oldest layer cannot know what came before
        let mut run = Run::new();
        run.push_key(b"j");
        run.push_entry(2, Version::Int(2), Version::Int(9));
        let l2 = ix
            .write_commit(
                "ok2",
                NewCommit {
                    live_before: 1,
                    ..nc(2)
                },
                &run,
            )
            .await
            .unwrap();
        ix.merge(&[l1, l2], 1..2, 0, "m3").await.unwrap();
    });
}

#[test]
fn a_flipped_byte_or_a_cut_file_is_a_format_error() {
    let rt = runtime();
    let mut rng = SmallRng::seed_from_u64(5);
    for trial in 0..200u64 {
        let h = History::gen(&mut rng);
        let mem = Arc::new(InMemory::new());
        let store = |mem: &Arc<InMemory>| {
            Store::new(
                mem.clone(),
                Path::from("t"),
                rt.handle().clone(),
                StoreOptions {
                    memory_bytes: 0,
                    ..Default::default()
                },
            )
        };
        let ix = ChangeIndex::new(
            store(&mem).unwrap(),
            WriteOptions {
                block_bytes: 64,
                level: 1,
                seg_blocks: 2,
            },
        );
        rt.block_on(async {
            let mut w = World {
                h,
                ix,
                layers: Vec::new(),
                floor: 0,
                head: 0,
                names: 0,
            };
            for c in 1..=w.h.head() {
                w.commit(c).await;
            }
            let len = w.layers.len();
            w.merge(0..len, w.head / 2).await;
            let l = w.layers[0].clone();
            let path = Path::from(format!("t/{}", l.path));
            use object_store::ObjectStoreExt;
            let mut b = mem
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec();
            let at = rng.random_range(0..b.len());
            if trial % 2 == 0 {
                b[at] ^= 1 << rng.random_range(0..8);
            } else {
                b.truncate(at);
            }
            mem.put(&path, b.into()).await.unwrap();
            let ix = ChangeIndex::new(store(&mem).unwrap(), WriteOptions::default());
            let all = ix.history(&w.layers, &Query::default()).await;
            let commits = ix.commits(&w.layers, 0, w.head).await;
            let failed = [all.err(), commits.err()];
            assert!(
                failed.iter().any(|e| matches!(e, Some(Error::Format(_)))),
                "trial {trial}: byte {at} of {} flipped or cut, nothing noticed: {failed:?}",
                l.size
            );
        });
    }
}

#[test]
fn a_store_refuses_zero_for_its_sizes() {
    let rt = runtime();
    for o in [
        StoreOptions {
            max_inflight: 0,
            ..Default::default()
        },
        StoreOptions {
            max_get: 0,
            ..Default::default()
        },
        StoreOptions {
            part_bytes: 0,
            ..Default::default()
        },
    ] {
        let got = Store::new(
            Arc::new(InMemory::new()),
            Path::from(""),
            rt.handle().clone(),
            o,
        );
        assert!(matches!(got, Err(Error::Invalid(_))));
    }
}

#[test]
fn a_writer_refuses_seg_blocks_past_its_limit() {
    for seg_blocks in [0, format::MAX_RAW / format::MAX_KEY + 1, 1 << 48] {
        let o = WriteOptions {
            seg_blocks,
            ..Default::default()
        };
        assert!(matches!(Writer::new(o, 1, 1, None), Err(Error::Invalid(_))));
    }
}
