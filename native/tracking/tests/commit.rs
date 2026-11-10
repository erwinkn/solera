//! Output commits over random histories, checked after every commit against
//! a brute-force fold: the entries each commit writes, the state at the head,
//! the live count, the changed keys; entries chain (the index's merges, which
//! check it, accept every layer); replaying the journaled events rebuilds the
//! same state. `COMMIT_SEEDS` sets how many histories.

use object_store::memory::InMemory;
use object_store::path::Path;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use solera_index::*;
use solera_tracking::commit::{
    self, Change, Changes, FullRunDone, Held, Mode, Reader, Source, Spec,
};
use solera_tracking::plan::OneIndex;
use solera_tracking::{Event, Id, Part, Tracking};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
enum V {
    I(u64),
    B(Vec<u8>),
}

type State = BTreeMap<Vec<u8>, V>;

fn own(v: Version<'_>) -> Option<V> {
    match v {
        Version::Absent => None,
        Version::Int(n) => Some(V::I(n)),
        Version::Bytes(b) => Some(V::B(b.to_vec())),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
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
            ..Default::default()
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

fn seeds() -> u64 {
    std::env::var("COMMIT_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

fn keys(rng: &mut SmallRng) -> Vec<Vec<u8>> {
    let mut ks: Vec<Vec<u8>> = (0..rng.random_range(1..60))
        .map(|_| {
            let mut k = ["a/", "a/b/", "b/", "é/", ""][rng.random_range(0..5)].to_string();
            for _ in 0..rng.random_range(1..4) {
                k.push(b"xyz019"[rng.random_range(0..6)] as char);
            }
            k.into_bytes()
        })
        .collect();
    ks.sort();
    ks.dedup();
    ks
}

/// A random change; version words come from a small set, so keys revert.
fn change<'a>(rng: &mut SmallRng, words: &'a [&'a [u8]]) -> Change<'a> {
    match rng.random_range(0..4) {
        0 => Change::Removed,
        1 => Change::Upsert(None),
        _ => Change::Upsert(Some(words[rng.random_range(0..words.len())])),
    }
}

fn new_of(c: Change<'_>, version: u64) -> Option<V> {
    match c {
        Change::Upsert(None) => Some(V::I(version)),
        Change::Upsert(Some(w)) => Some(V::B(w.to_vec())),
        Change::Removed => None,
    }
}

/// What one commit should change, by brute force: (key, new, replaced). A
/// clear's are its effective changes, as a diff across it returns them.
fn expected(
    states: &[State],
    changes: &Changes,
    mode: Mode,
    version: u64,
) -> Vec<(Vec<u8>, Option<V>, Option<V>)> {
    let now = states.last().unwrap();
    let mut named: BTreeMap<Vec<u8>, Option<V>> = (0..changes.len())
        .map(|i| (changes.key(i).to_vec(), new_of(changes.change(i), version)))
        .collect();
    if mode == Mode::Clear {
        named.retain(|_, v| v.is_some());
    }
    if matches!(mode, Mode::Replace | Mode::Clear) {
        for k in now.keys() {
            named.entry(k.clone()).or_insert(None);
        }
    }
    let mut out = Vec::new();
    for (k, new) in named {
        let old = now.get(&k).cloned();
        if let Mode::Report(at) = mode {
            if states[at as usize].get(&k) != old.as_ref() {
                continue;
            }
        }
        if new != old {
            out.push((k, new, old));
        }
    }
    out
}

fn scan_all(rt: &tokio::runtime::Runtime, ix: &ChangeIndex, layers: &[LayerRef], c: u64) -> State {
    if layers.is_empty() {
        return State::new();
    }
    let p = rt.block_on(ix.scan(layers, c, &Query::default())).unwrap();
    (0..p.run.len())
        .map(|i| {
            let r = p.run.rows(i).start;
            (p.run.key(i).to_vec(), own(p.run.new_version(r)).unwrap())
        })
        .collect()
}

#[test]
fn commits_match_the_fold() {
    let rt = runtime();
    let words: [&[u8]; 4] = [b"", b"v1", b"v2", b"etag-3"];
    let output: Id = b"items".as_slice().into();
    for seed in 0..seeds() {
        let mut rng = SmallRng::seed_from_u64(seed);
        let ix = index(&mut rng, &rt);
        let universe = keys(&mut rng);
        let mut t = Tracking::new();
        let mut replay = Tracking::new();
        let mut states: Vec<State> = vec![State::new()];
        let mut version = 100;
        for step in 0..rng.random_range(1..25) {
            let out = t.outputs.get(&output).cloned().unwrap_or_default();
            let head = out.head;
            assert_eq!(states.len() as u64, head + 1);
            let mode = match rng.random_range(0..7) {
                0 => Mode::Replace,
                1 => Mode::Report(rng.random_range(check_layers(&out.layers).unwrap().0..=head)),
                2 => Mode::Clear,
                _ => Mode::Patch,
            };
            let named: Vec<&Vec<u8>> = universe
                .iter()
                .filter(|_| rng.random_bool([0.1, 0.5, 0.9][step % 3]))
                .collect();
            let items: Vec<(&[u8], Change<'_>)> = named
                .iter()
                .map(|k| (k.as_slice(), change(&mut rng, &words)))
                .collect();
            let changes = Changes::of(&items).unwrap();
            version += 1 + rng.random_range(0..3);
            let want = expected(&states, &changes, mode, version);
            let collect = rng.random_range(0..8);
            let ends = rng.random_bool(0.25);
            let path = format!("l/{seed}-{step}");
            let spec = Spec {
                version,
                time_ms: step as u64,
                producer: b"test",
                path: &path,
                mode,
                ends_full_run: ends,
                collect,
            };
            let bytes = changes.encode();
            let built = rt
                .block_on(async {
                    if rng.random_bool(0.5) {
                        let mut src = Reader::new(&bytes).unwrap();
                        commit::build(&OneIndex(&ix), &output, &out, spec, &mut src).await
                    } else {
                        commit::build(
                            &OneIndex(&ix),
                            &output,
                            &out,
                            spec,
                            &mut Held(Some(&changes)),
                        )
                        .await
                    }
                })
                .unwrap();
            let Some(built) = built else {
                assert!(
                    want.is_empty() && mode != Mode::Clear,
                    "seed {seed} step {step}: nothing built, {want:?} expected"
                );
                if ends {
                    let ev = Event::of(vec![Part::FullRunDone(FullRunDone {
                        output: output.clone(),
                    })])
                    .encode();
                    let ok = out.full_run.is_some();
                    assert_eq!(t.apply(&ev).is_ok(), ok, "seed {seed} step {step}");
                    assert_eq!(replay.apply(&ev).is_ok(), ok);
                    assert_eq!(t.outputs.get(&output).and_then(|o| o.full_run), None);
                }
                continue;
            };
            let ev = Event::of(vec![Part::OutputCommit(built.part.clone())]).encode();
            if ends && mode != Mode::Clear && out.full_run.is_none() {
                assert!(t.apply(&ev).is_err(), "seed {seed} step {step}");
                continue;
            }
            t.apply(&ev).unwrap();
            replay.apply(&ev).unwrap();
            let full_run = match (ends, mode) {
                (true, _) => None,
                (false, Mode::Clear) => Some(head + 1),
                _ => out.full_run,
            };
            assert_eq!(
                t.outputs[&output].full_run, full_run,
                "seed {seed} step {step}"
            );
            let layers = t.outputs[&output].layers.clone();
            let c = head + 1;
            let got = rt
                .block_on(ix.diff(&layers, head, c, &Query::default()))
                .unwrap();
            let got: Vec<_> = (0..got.run.len())
                .map(|i| {
                    let r = got.run.rows(i).start;
                    (
                        got.run.key(i).to_vec(),
                        own(got.run.new_version(r)),
                        own(got.run.replaced_version(r)),
                    )
                })
                .collect();
            assert_eq!(got, want, "seed {seed} step {step} {mode:?}");
            let mut state = states.last().unwrap().clone();
            for (k, new, _) in &want {
                match new {
                    Some(v) => state.insert(k.clone(), v.clone()),
                    None => state.remove(k),
                };
            }
            assert_eq!(
                scan_all(&rt, &ix, &layers, c),
                state,
                "seed {seed} step {step}"
            );
            assert_eq!(t.outputs[&output].live, state.len() as u64);
            let counts = |f: fn(&(Vec<u8>, Option<V>, Option<V>)) -> bool| {
                want.iter().filter(|e| f(e)).count() as u64
            };
            if mode == Mode::Clear {
                let written = (0..changes.len())
                    .filter(|&i| changes.change(i) != Change::Removed)
                    .count() as u64;
                assert_eq!(
                    (built.added, built.updated, built.removed),
                    (written, 0, out.live)
                );
                assert_eq!(built.part.layer.clears, [c]);
            } else {
                assert_eq!(built.added, counts(|e| e.2.is_none()));
                assert_eq!(built.removed, counts(|e| e.1.is_none()));
                assert_eq!(built.updated, counts(|e| e.1.is_some() && e.2.is_some()));
            }
            match &built.keys {
                _ if mode == Mode::Clear && out.live > 0 => assert!(built.keys.is_none()),
                Some(k) => {
                    assert!(want.len() <= collect);
                    let up: Vec<&[u8]> = want
                        .iter()
                        .filter(|e| e.1.is_some())
                        .map(|e| e.0.as_slice())
                        .collect();
                    let rm: Vec<&[u8]> = want
                        .iter()
                        .filter(|e| e.1.is_none())
                        .map(|e| e.0.as_slice())
                        .collect();
                    assert_eq!(k.upserted.iter().map(|k| &k[..]).collect::<Vec<_>>(), up);
                    assert_eq!(k.removed.iter().map(|k| &k[..]).collect::<Vec<_>>(), rm);
                }
                None => assert!(collect == 0 || want.len() > collect),
            }
            states.push(state);
            if rng.random_bool(0.3) {
                let cut = rng.random_range(0..=c);
                if let Some(r) = pick_merge(&layers, cut) {
                    let path = format!("m/{seed}-{step}");
                    let m = rt
                        .block_on(ix.merge(&layers, r.clone(), cut, &path))
                        .unwrap();
                    for s in [&mut t, &mut replay] {
                        s.outputs
                            .get_mut(&output)
                            .unwrap()
                            .layers
                            .splice(r.clone(), [m.clone()]);
                    }
                    let (floor, _) = check_layers(&t.outputs[&output].layers).unwrap();
                    for (at, s) in states.iter().enumerate().skip(floor as usize) {
                        assert_eq!(
                            &scan_all(&rt, &ix, &t.outputs[&output].layers, at as u64),
                            s
                        );
                    }
                    if floor > 0 {
                        states
                            .iter_mut()
                            .take(floor as usize)
                            .for_each(|s| s.clear());
                    }
                }
            }
        }
        assert_eq!(replay, t);
        assert_eq!(Tracking::restore(&t.snapshot()).unwrap(), t);
        if let Some(o) = t.outputs.get(&output) {
            let n = o.layers.len();
            rt.block_on(ix.merge(&o.layers, 0..n, o.head, "final"))
                .unwrap();
        }
    }
}

#[test]
fn a_commit_built_at_an_old_head_is_refused() {
    let rt = runtime();
    let mut rng = SmallRng::seed_from_u64(1);
    let ix = index(&mut rng, &rt);
    let output: Id = b"o".as_slice().into();
    let mut t = Tracking::new();
    let spec = |path| Spec {
        version: 7,
        time_ms: 0,
        producer: b"",
        path,
        mode: Mode::Patch,
        ends_full_run: false,
        collect: 0,
    };
    let c = Changes::of(&[(b"k", Change::Upsert(None))]).unwrap();
    let out = t.outputs.get(&output).cloned().unwrap_or_default();
    let a = rt
        .block_on(commit::build(
            &OneIndex(&ix),
            &output,
            &out,
            spec("a"),
            &mut Held(Some(&c)),
        ))
        .unwrap()
        .unwrap();
    let b = rt
        .block_on(commit::build(
            &OneIndex(&ix),
            &output,
            &out,
            spec("b"),
            &mut Held(Some(&c)),
        ))
        .unwrap()
        .unwrap();
    t.apply(&Event::of(vec![Part::OutputCommit(a.part)]).encode())
        .unwrap();
    let before = t.clone();
    assert!(t
        .apply(&Event::of(vec![Part::OutputCommit(b.part)]).encode())
        .is_err());
    assert_eq!(t, before);
    let none = rt
        .block_on(commit::build(
            &OneIndex(&ix),
            &output,
            &t.outputs[&output],
            spec("c"),
            &mut Held(Some(&Changes::of(&[(b"x", Change::Removed)]).unwrap())),
        ))
        .unwrap();
    assert!(none.is_none(), "removing an absent key changes nothing");
}

#[test]
fn changes_round_trip_and_refuse_bad_bytes() {
    let mut rng = SmallRng::seed_from_u64(3);
    let mut ks: Vec<Vec<u8>> = (0..40_000u32)
        .map(|i| format!("site-{:03}/obj-{i:08}-{:x}", i / 1000, rng.random::<u32>()).into_bytes())
        .collect();
    ks.sort();
    let words: [&[u8]; 3] = [b"", b"w", b"etag"];
    let items: Vec<(&[u8], Change<'_>)> = ks
        .iter()
        .map(|k| (k.as_slice(), change(&mut rng, &words)))
        .collect();
    let c = Changes::of(&items).unwrap();
    let bytes = c.encode();
    let mut r = Reader::new(&bytes).unwrap();
    let (mut back, mut chunk, mut chunks) = (Changes::new(), Changes::new(), 0);
    while r.next(&mut chunk).unwrap() {
        chunks += 1;
        for i in 0..chunk.len() {
            back.push(chunk.key(i), chunk.change(i)).unwrap();
        }
    }
    assert!(chunks > 1);
    assert_eq!(back, c);
    let empty = Changes::new().encode();
    assert!(!Reader::new(&empty).unwrap().next(&mut chunk).unwrap());
    assert!(Reader::new(b"nope").is_err());
    for at in [9, 20, bytes.len() / 2, bytes.len() - 1] {
        let mut bad = bytes.clone();
        bad[at] ^= 0x40;
        let mut r = Reader::new(&bad).unwrap();
        let mut failed = false;
        while !failed {
            match r.next(&mut chunk) {
                Ok(true) => {}
                Ok(false) => break,
                Err(_) => failed = true,
            }
        }
        assert!(failed, "a flipped byte at {at} went unnoticed");
    }
    assert!(Changes::of(&[(b"a", Change::Removed), (b"a", Change::Upsert(None))]).is_err());
    let big = vec![b'v'; commit::changes::MAX_ROW];
    assert!(Changes::of(&[(b"a", Change::Upsert(Some(&big)))]).is_err());
}

/// Keys sharing a long prefix spell out to far more than their encoded
/// bytes: the encoder cuts blocks by both, and the reader takes them back.
#[test]
fn long_shared_prefixes_round_trip_in_bounded_blocks() {
    let prefix = vec![b'p'; 60_000];
    let ks: Vec<Vec<u8>> = (0..2_000u32)
        .map(|i| [prefix.as_slice(), format!("{i:06}").as_bytes()].concat())
        .collect();
    let items: Vec<(&[u8], Change<'_>)> = ks
        .iter()
        .map(|k| (k.as_slice(), Change::Upsert(None)))
        .collect();
    let c = Changes::of(&items).unwrap();
    let bytes = c.encode();
    let mut r = Reader::new(&bytes).unwrap();
    let (mut back, mut chunk) = (Changes::new(), Changes::new());
    while r.next(&mut chunk).unwrap() {
        for i in 0..chunk.len() {
            back.push(chunk.key(i), chunk.change(i)).unwrap();
        }
    }
    assert_eq!(back, c);
}

/// Commits past one page of the head's state and one transport block: a
/// full listing and a patch over 150,000 keys, against the fold.
#[test]
fn large_commits_cross_pages_and_blocks() {
    let rt = runtime();
    let mut rng = SmallRng::seed_from_u64(9);
    let store = Store::new(
        Arc::new(InMemory::new()),
        Path::from("t"),
        rt.handle().clone(),
        StoreOptions::default(),
    )
    .unwrap();
    let ix = ChangeIndex::new(store, WriteOptions::default());
    let output: Id = b"big".as_slice().into();
    let key = |i: u32| format!("site-{:02}/obj-{i:08}", i / 10_000).into_bytes();
    let mut t = Tracking::new();
    let mut state = State::new();
    let words: [&[u8]; 3] = [b"", b"a", b"b"];
    for (step, mode) in [Mode::Replace, Mode::Replace, Mode::Patch, Mode::Replace]
        .into_iter()
        .enumerate()
    {
        let version = 10 + step as u64;
        let mut items: Vec<(Vec<u8>, Change<'_>)> = Vec::new();
        for i in 0..150_000u32 {
            if rng.random_bool(0.9) {
                items.push((key(i), change(&mut rng, &words)));
            }
        }
        let refs: Vec<(&[u8], Change<'_>)> =
            items.iter().map(|(k, c)| (k.as_slice(), *c)).collect();
        let changes = Changes::of(&refs).unwrap();
        let mut next = if mode == Mode::Replace {
            State::new()
        } else {
            state.clone()
        };
        for (k, c) in &items {
            match new_of(*c, version) {
                Some(v) => next.insert(k.clone(), v),
                None => next.remove(k),
            };
        }
        let bytes = changes.encode();
        let out = t.outputs.get(&output).cloned().unwrap_or_default();
        let path = format!("big/{step}");
        let spec = Spec {
            version,
            time_ms: 0,
            producer: b"",
            path: &path,
            mode,
            ends_full_run: false,
            collect: 0,
        };
        let built = rt
            .block_on(commit::build(
                &OneIndex(&ix),
                &output,
                &out,
                spec,
                &mut Reader::new(&bytes).unwrap(),
            ))
            .unwrap()
            .unwrap();
        t.apply(&Event::of(vec![Part::OutputCommit(built.part)]).encode())
            .unwrap();
        let o = &t.outputs[&output];
        assert_eq!(scan_all(&rt, &ix, &o.layers, o.head), next, "step {step}");
        assert_eq!(o.live, next.len() as u64);
        state = next;
    }
    let o = &t.outputs[&output];
    rt.block_on(ix.merge(&o.layers, 0..o.layers.len(), o.head, "big/m"))
        .unwrap();
}
