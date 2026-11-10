//! Property tests: random histories, written commit by commit, merged and
//! folded at random cuts, every query compared with a brute-force fold of the
//! history (the format benchmark's check). `ORACLE_SEEDS` sets how many.

use object_store::memory::InMemory;
use object_store::path::Path;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use solera_index::*;
use std::sync::Arc;

#[derive(Clone, PartialEq, Eq, Debug)]
enum V {
    I(u64),
    B(Vec<u8>),
}

type O = Option<V>;

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
        let dirs = ["a", "a/b", "ab", "b", "é", "z/z/z", "a/b/c"];
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
                "a", "b", "/", "*", "**", "**/", "?", "é", "a/b", "ab", "0", "z",
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
            q.pattern = Some(Pattern::new(&gi, &ge));
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
            let present = |k: usize, c: u64| -> Option<(Vec<u8>, Vec<(u64, O, O)>)> {
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
            }),
            ..Default::default()
        };
        let store = Store::new(mem.clone(), Path::from("t"), rt.handle().clone(), opts).unwrap();
        ChangeIndex::new(store, WriteOptions::default())
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
        let fresh = index(&fresh_dir);
        assert_eq!(ix_scan(&fresh, &layers).await, 5000);
        assert_eq!(fresh.store.stats.counts().gets, 0);
        assert!(fresh.store.stats.counts().disk_reads > 0);
    });
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(fresh_dir);
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
fn a_flipped_byte_is_a_format_error() {
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
            b[at] ^= 1 << rng.random_range(0..8);
            mem.put(&path, b.into()).await.unwrap();
            let ix = ChangeIndex::new(store(&mem).unwrap(), WriteOptions::default());
            let all = ix.history(&w.layers, &Query::default()).await;
            let commits = ix.commits(&w.layers, 0, w.head).await;
            let failed = [all.err(), commits.err()];
            assert!(
                failed.iter().any(|e| matches!(e, Some(Error::Format(_)))),
                "trial {trial}: byte {at} of {} flipped, nothing noticed: {failed:?}",
                l.size
            );
        });
    }
}
