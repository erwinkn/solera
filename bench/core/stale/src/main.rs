//! Staleness at the scale target: 100M keys, 1% changing a day in 24 commits
//! (build plan N10, N11), on the output bench/core/plan writes to `dir`
//! (run it first; this bench only reads `dir/layers.bin` and the layers).
//!
//! `core-stale-bench dir=/data/daily-scattered-100000000 [readers=50 reps=200]`
//!
//! One JSON line per measurement on stdout. Cold: a fresh index over the
//! simulated network (30 ms a request, 80 MB/s). Local-cold: a fresh index on
//! local files. Warm: the same index again.

#[path = "../../../change-index/src/sim.rs"]
mod sim;

use object_store::path::Path;
use serde_json::json;
use solera_index::{
    ChangeIndex, LayerRef, NewCommit, Query, Run, Store, StoreOptions, WriteOptions,
};
use solera_tracking::codec::{Dec, Ver};
use solera_tracking::event::{Event, Part};
use solera_tracking::outcomes::transition::{Stored, FAILED};
use solera_tracking::outcomes::{self, Held, Record};
use solera_tracking::plan::OneIndex;
use solera_tracking::progress::{
    read_digest, BatchProgress, Conditions, EdgeOpen, Key, KeyRange, Keyed, Pat, Patterns, Value,
};
use solera_tracking::stale::{self, Counts, NoneDue, SetWatch, State, Watch, Witness};
use solera_tracking::{Output, Tracking};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

const NET: (f64, f64) = (0.030, 80e6);
const ITEMS: &[u8] = b"items";

fn proc_cpu() -> f64 {
    let mut r: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut r) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(r.ru_utime) + t(r.ru_stime)
}

fn r3(x: f64) -> f64 {
    (x * 1e3).round() / 1e3
}

fn layers(dir: &std::path::Path) -> (Vec<LayerRef>, u64) {
    let bytes =
        std::fs::read(dir.join("layers.bin")).expect("run bench/core/plan on this dir first");
    let mut d = Dec::new(&bytes);
    let head = d.uv().unwrap();
    let n = d.count().unwrap();
    ((0..n).map(|_| d.layer().unwrap()).collect(), head)
}

fn index(
    dir: &std::path::Path,
    net: Option<(f64, f64)>,
    rt: &tokio::runtime::Runtime,
) -> ChangeIndex {
    let store = Store::new(
        Arc::new(sim::Sim::new(dir, net)),
        Path::from(""),
        rt.handle().clone(),
        StoreOptions::default(),
    )
    .unwrap();
    ChangeIndex::new(store, WriteOptions::default())
}

fn edge(i: usize) -> Key {
    format!("consumer-{i:02}/items").into_bytes().into()
}

/// One output at `head` and `n` edges, each holding everything as of commit
/// `at` under `cond`, watched under `cond`, with `known` as what is known.
fn state(
    layers: &[LayerRef],
    head: u64,
    n: usize,
    at: u64,
    cond: &Arc<Conditions>,
    known: Option<(u64, State)>,
) -> Tracking {
    let mut t = Tracking::new();
    t.outputs.insert(
        ITEMS.into(),
        Output {
            life: 1,
            layers: layers.to_vec(),
            head,
            live: 0,
            full_run: None,
        },
    );
    for i in 0..n {
        let mut parts = vec![Part::EdgeOpen(EdgeOpen {
            edge: edge(i),
            input: ITEMS.into(),
            keyed: true,
        })];
        t.apply_event(Event::of(std::mem::take(&mut parts)))
            .unwrap();
        parts.push(Part::Batch(BatchProgress {
            edge: edge(i),
            reset: false,
            range: KeyRange::all(),
            piece: Some(Value::At(at, cond.clone())),
            head: at,
            life: t.life(ITEMS),
            own: None,
            points: vec![],
            expect: read_digest(&Keyed::default(), false, &KeyRange::all()),
            fence: false,
        }));
        t.apply_event(Event::of(parts)).unwrap();
        t.apply_event(Event::of(vec![Part::Watch(SetWatch {
            edge: edge(i),
            watch: Watch {
                conditions: cond.clone(),
                own: None,
                gate: true,
            },
        })]))
        .unwrap();
        if let Some((x, s)) = &known {
            let gen = t.stale.inputs[&edge(i)].gen;
            t.apply_event(Event::of(vec![Part::Witness(Witness {
                edge: edge(i),
                gen,
                x: *x,
                state: s.clone(),
            })]))
            .unwrap();
        }
    }
    t
}

/// Edge 0's failure records (B6): `n` records in all, failed under `c`, on
/// keys spread over the key space; with `waiting`, among them every key of
/// `commit`, tried at the version the commit gives it (waiting for its
/// retry). Written once to `dir` and reused. The edge has looked for
/// reverts through the head, as the engine does before it refreshes.
fn failures(
    t: &mut Tracking,
    ix: &ChangeIndex,
    rt: &tokio::runtime::Runtime,
    commit: &Run,
    waiting: bool,
    n: usize,
    c: &Arc<Conditions>,
) {
    let cid = outcomes::cond_id(c);
    let rec = |on: Ver| {
        Record {
            stored: Stored {
                outcome: FAILED,
                tries: 1,
                deploy: 0,
                forced: 0,
                since: 0,
                last: 0,
                next_at: u64::MAX,
                until: 0,
                message: b"error".as_slice().into(),
            },
            on,
            on_cond: cid,
            at: 1,
            held: Held::Absent,
        }
        .encode()
    };
    let mut recs: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = Default::default();
    if waiting {
        for i in 0..commit.len() {
            let on = Ver::of(commit.new_version(commit.rows(i).start));
            recs.insert(commit.key(i).to_vec(), rec(on));
        }
    }
    let mut i = 0u64;
    while recs.len() < n {
        let h = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20;
        let k = format!(
            "site-{:04}/part-{:02}/obj-{:012}-fail.json",
            h % 10_000,
            (h >> 14) % 100,
            i
        );
        recs.insert(k.into_bytes(), rec(Ver::Int(1)));
        i += 1;
    }
    let mut run = Run::new();
    for (k, v) in &recs {
        run.push_key(k);
        run.push_entry(
            1,
            solera_index::Version::Bytes(v),
            solera_index::Version::Absent,
        );
    }
    let path = format!("stale-failures-{n}-{waiting}");
    let nc = NewCommit {
        commit: 1,
        time_ms: 0,
        producer: b"",
        live_before: 0,
        clear: false,
    };
    let layer = rt.block_on(ix.write_commit(&path, nc, &run)).unwrap();
    t.outputs.insert(
        outcomes::output_id(outcomes::FAILURES, &edge(0)),
        Output {
            life: 1,
            layers: vec![layer],
            head: 1,
            live: recs.len() as u64,
            full_run: None,
        },
    );
    let seen = t.outputs[&t.edges[&edge(0)].input].head;
    let k = t.per_key.entry(edge(0)).or_default();
    k.seen = seen;
    k.cond = cid;
    k.conds = Arc::new([(cid, c.clone())].into_iter().collect());
    k.counts[FAILED as usize] = recs.len() as u64;
    k.classes = [recs.len() as u64, 0, 0];
}

fn cond(include: &[&str], exclude: &[&str]) -> Arc<Conditions> {
    let g = |v: &[&str]| v.iter().map(|s| Pat::Glob(s.to_string())).collect();
    Arc::new(Conditions {
        patterns: Patterns::new(g(include), g(exclude)).unwrap(),
        ..Default::default()
    })
}

fn main() {
    let a: HashMap<String, String> = std::env::args()
        .skip(1)
        .filter_map(|x| x.split_once('=').map(|(k, v)| (k.into(), v.into())))
        .collect();
    let dir = std::path::PathBuf::from(a.get("dir").expect("dir="));
    let readers: usize = a.get("readers").map_or(50, |v| v.parse().unwrap());
    let reps: usize = a.get("reps").map_or(200, |v| v.parse().unwrap());
    let only = a.get("only").cloned().unwrap_or_default();
    let run = |name: &str| only.is_empty() || only.split(',').any(|o| name.starts_with(o));
    let (layers, head) = layers(&dir);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let all = Arc::new(Conditions::default());
    let none = cond(&[], &["**"]);
    let emit = |q: &str, extra: serde_json::Value| {
        let mut v = json!({"query": q, "head": head});
        v.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        println!("{v}");
    };
    let modes: [(&str, Option<(f64, f64)>); 2] = [("cold", Some(NET)), ("local-cold", None)];

    // N11: counting owed keys on request, a day (24 commits) and 30 days behind.
    for behind in [24u64, 720] {
        if !run("count") {
            break;
        }
        let t = state(&layers, head, 1, head - behind, &all, None);
        let ask = t.stale_ask(&edge(0)).unwrap();
        for (mode, net) in modes {
            let ix = index(&dir, net, &rt);
            let counts = Counts::default();
            for pass in [mode, "warm", "cached"] {
                let (c0, t0, s0) = (proc_cpu(), Instant::now(), ix.store.stats.counts());
                if pass != "cached" {
                    counts.retain(&Tracking::new());
                }
                let o = rt
                    .block_on(counts.get(&ask, &OneIndex(&ix), &NoneDue))
                    .unwrap()
                    .unwrap();
                let n = o.due;
                let (wall, cpu) = (t0.elapsed().as_secs_f64(), proc_cpu() - c0);
                let s = ix.store.stats.counts();
                emit(
                    "count",
                    json!({"behind_days": behind / 24, "mode": pass, "wall_s": r3(wall), "cpu_s": r3(cpu), "gets": s.gets - s0.gets, "mb": r3((s.get_bytes - s0.get_bytes) as f64 / 1e6), "owed": n.iter().sum::<u64>(), "added": n[0], "updated": n[1], "removed": n[2], "reasons": o.reasons.iter().map(|r| r.name()).collect::<Vec<_>>()}),
                );
            }
        }
    }

    // The floor under any count: the index's own diff, counted by a callback
    // that builds nothing (what a counting path of its own could reach).
    for behind in [24u64, 720] {
        if !run("rawdiff") {
            break;
        }
        for (mode, net) in modes {
            let ix = index(&dir, net, &rt);
            for pass in [mode, "warm"] {
                let (c0, t0) = (proc_cpu(), Instant::now());
                let mut n = 0u64;
                rt.block_on(ix.diff_with(
                    &layers,
                    head - behind,
                    head,
                    &Query::default(),
                    |_, _, _, _| n += 1,
                ))
                .unwrap();
                emit(
                    "raw-diff",
                    json!({"behind_days": behind / 24, "mode": pass, "wall_s": r3(t0.elapsed().as_secs_f64()), "cpu_s": r3(proc_cpu() - c0), "keys": n}),
                );
            }
        }
    }

    // The head commit's entries, as the commit builder hands them over; and a
    // 10,000-key batch commit (its first 10,000 entries).
    let ix = index(&dir, None, &rt);
    let page = rt
        .block_on(ix.window(&layers, head..=head, &Query::default()))
        .unwrap();
    let commit = page.run;
    let mut batch = Run::new();
    for i in 0..commit.len().min(10_000) {
        batch.push_key_from(&commit, i);
        for r in commit.rows(i) {
            batch.push_entry_from(&commit, r);
        }
    }

    // N10, notice: rule 1 or 2 at one upstream commit, for `readers` edges reading it.
    if run("notice") {
        for (name, entries) in [("hourly", &commit), ("batch", &batch)] {
            for (case, c, known) in [
                ("fresh, the first entry matters", &all, State::Fresh),
                (
                    "fresh, no entry matters (patterns leave every key out)",
                    &none,
                    State::Fresh,
                ),
                (
                    "stale, the commit does not touch the witness",
                    &all,
                    State::Stale {
                        key: b"site-0000/part-00/obj-000000000000-x".as_slice().into(),
                        reason: stale::Reason::InputChanged,
                        since_ms: 1,
                    },
                ),
            ] {
                let t = state(
                    &layers,
                    head - 1,
                    readers,
                    head - 1,
                    c,
                    Some((head - 1, known)),
                );
                let mut lat = vec![];
                let mut found = 0;
                for _ in 0..reps {
                    let t0 = Instant::now();
                    let mut n = stale::Notice::new(&t, ITEMS, head, false, 7);
                    for i in 0..entries.len() {
                        n.push(entries.key(i), entries.new_version(entries.rows(i).start));
                    }
                    let parts = n.finish();
                    lat.push(t0.elapsed().as_secs_f64());
                    found = parts.len();
                }
                lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let p50 = lat[lat.len() / 2];
                emit(
                    "notice",
                    json!({"commit": name, "entries": entries.len(), "case": case, "readers": readers, "parts": found, "p50_ms": r3(p50 * 1e3), "us_per_reader": r3(p50 * 1e6 / readers as f64), "ns_per_entry_per_reader": r3(p50 * 1e9 / readers as f64 / entries.len() as f64)}),
                );
            }
        }
    }

    // N10, refresh: the same rules from the index, for an edge the notice
    // missed (fresh as of the commit before the head).
    let timed = |q: &str,
                 extra: serde_json::Value,
                 t: &Tracking,
                 e: usize,
                 mode: &str,
                 ix: &ChangeIndex,
                 f: &dyn Fn(&stale::Ask, &ChangeIndex) -> String| {
        let ask = t.stale_ask(&edge(e)).unwrap();
        let (c0, t0, s0) = (proc_cpu(), Instant::now(), ix.store.stats.counts());
        let out = f(&ask, ix);
        let (wall, cpu) = (t0.elapsed().as_secs_f64(), proc_cpu() - c0);
        let s = ix.store.stats.counts();
        let mut v = json!({"mode": mode, "wall_ms": r3(wall * 1e3), "cpu_ms": r3(cpu * 1e3), "gets": s.gets - s0.gets, "mb": r3((s.get_bytes - s0.get_bytes) as f64 / 1e6), "found": out});
        v.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        emit(q, v);
    };
    let refresh = |ask: &stale::Ask, ix: &ChangeIndex| -> String {
        let w = rt
            .block_on(stale::refresh(ask, &OneIndex(ix), &NoneDue, 7))
            .unwrap();
        format!(
            "{:?}",
            w.map(|w| match w.state {
                State::Fresh => "fresh".to_string(),
                State::Stale { key, reason, .. } =>
                    format!("{} ({})", String::from_utf8_lossy(&key), reason.name()),
            })
        )
    };
    if run("refresh") {
        for (case, c, at, known) in [
            (
                "fresh as of head-1, rule 1 over the head commit, the first entry matters",
                &all,
                head - 1,
                Some((head - 1, State::Fresh)),
            ),
            (
                "fresh as of head-1, rule 1 over the head commit, no entry matters",
                &none,
                head - 1,
                Some((head - 1, State::Fresh)),
            ),
            (
                "fresh as of a day ago, rule 1 over a day, the first entry matters",
                &all,
                head - 24,
                Some((head - 24, State::Fresh)),
            ),
            (
                "a day behind, nothing known: probe from the first key",
                &all,
                head - 24,
                None,
            ),
            (
                "a day behind, patterns leave every key out: probe finds nothing",
                &none,
                head - 24,
                None,
            ),
            (
                "30 days behind, nothing known: probe from the first key",
                &all,
                head - 720,
                None,
            ),
            (
                "30 days behind, patterns leave every key out: probe finds nothing",
                &none,
                head - 720,
                None,
            ),
        ] {
            let t = state(&layers, head, 1, at, c, known);
            for (mode, net) in modes {
                let ix = index(&dir, net, &rt);
                for m in [mode, "warm"] {
                    timed("refresh", json!({"case": case}), &t, 0, m, &ix, &refresh);
                }
            }
        }
        // A batch commit mid-run: a day behind, the first 10,000 owed keys
        // processed; the probe resumes after them.
        let mut t = state(
            &layers,
            head,
            1,
            head - 24,
            &all,
            Some((head, State::Fresh)),
        );
        let lo: Key = commit.key(0).into();
        let hi: Key = commit.key(10_000.min(commit.len() - 1)).into();
        let k = t.edges[&edge(0)].progress.keyed().unwrap().clone();
        let range = KeyRange {
            lo: Some(lo),
            hi: Some(hi),
        };
        t.apply_event(Event::of(vec![Part::Batch(BatchProgress {
            edge: edge(0),
            reset: false,
            range: range.clone(),
            piece: Some(Value::At(head, all.clone())),
            head,
            life: t.life(ITEMS),
            own: None,
            points: vec![],
            expect: read_digest(&k, false, &range),
            fence: false,
        })]))
        .unwrap();
        for (mode, net) in modes {
            let ix = index(&dir, net, &rt);
            for m in [mode, "warm"] {
                timed(
                    "refresh",
                    json!({"case": "after a batch commit mid-run, a day behind: probe from the batch's range"}),
                    &t,
                    0,
                    m,
                    &ix,
                    &refresh,
                );
            }
        }
    }

    // N10 and N11 on an edge with B6 failure records: no notice, so every
    // upstream commit is a refresh, which reads the records of rule 1's
    // candidates.
    if run("records") {
        let n: usize = a.get("records").map_or(1_000_000, |v| v.parse().unwrap());
        for (case, waiting, at) in [
            (
                "fresh as of head-1, the head commit touches no failing key",
                false,
                head - 1,
            ),
            (
                "fresh as of head-1, every key of the head commit is a failing key waiting for its retry",
                true,
                head - 1,
            ),
        ] {
            let mut t = state(&layers, head, 1, at, &all, Some((at, State::Fresh)));
            failures(&mut t, &ix, &rt, &commit, waiting, n, &all);
            for (mode, net) in modes {
                let ix = index(&dir, net, &rt);
                for m in [mode, "warm"] {
                    timed(
                        "refresh-records",
                        json!({"case": case, "records": n, "commit_entries": commit.len()}),
                        &t,
                        0,
                        m,
                        &ix,
                        &refresh,
                    );
                }
            }
        }
        let mut t = state(&layers, head, 1, head - 24, &all, None);
        failures(&mut t, &ix, &rt, &commit, false, n, &all);
        let ask = t.stale_ask(&edge(0)).unwrap();
        for (mode, net) in modes {
            let ix = index(&dir, net, &rt);
            let counts = Counts::default();
            for pass in [mode, "warm"] {
                counts.retain(&Tracking::new());
                let (c0, t0, s0) = (proc_cpu(), Instant::now(), ix.store.stats.counts());
                let o = rt
                    .block_on(counts.get(&ask, &OneIndex(&ix), &NoneDue))
                    .unwrap()
                    .unwrap();
                let s = ix.store.stats.counts();
                emit(
                    "count-records",
                    json!({"behind_days": 1, "records": n, "mode": pass, "wall_s": r3(t0.elapsed().as_secs_f64()), "cpu_s": r3(proc_cpu() - c0), "gets": s.gets - s0.gets, "mb": r3((s.get_bytes - s0.get_bytes) as f64 / 1e6), "due": o.due.iter().sum::<u64>(), "waiting": o.waiting.iter().sum::<u64>()}),
                );
            }
        }
    }
}
