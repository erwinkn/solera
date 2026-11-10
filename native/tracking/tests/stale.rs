//! Staleness (B7): what the events do to what is known, the notice over a
//! commit's entries, a refresh over a real index, and the answers. The
//! random histories against SOLE-7's model are in tests/core/test_stale_model.py.

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use solera_index::{ChangeIndex, NewCommit, Run, Store, StoreOptions, Version, WriteOptions};
use solera_tracking::commit::OutputCommit;
use solera_tracking::event::{Event, Part};
use solera_tracking::plan::OneIndex;
use solera_tracking::progress::{
    read_digest, BatchProgress, Conditions, EdgeClose, EdgeOpen, EdgeRename, Key, KeyRange, Pat,
    Patterns, Progress, Reset, Value,
};
use solera_tracking::stale::{
    self, graph, Incomplete, NoneDue, Partition, Reason, SetWatch, State, Status, Watch, Witness,
};
use solera_tracking::Tracking;

const ITEMS: &[u8] = b"items";
const EDGE: &[u8] = b"checks/items";

fn key(s: &str) -> Key {
    s.as_bytes().into()
}

fn apply(t: &mut Tracking, parts: Vec<Part>) {
    t.apply(&Event::of(parts).encode()).unwrap();
}

fn watch(c: &Arc<Conditions>) -> Part {
    Part::Watch(SetWatch {
        edge: EDGE.into(),
        watch: Watch {
            conditions: c.clone(),
            own: None,
            gate: true,
        },
    })
}

fn opened() -> Tracking {
    let mut t = Tracking::new();
    apply(
        &mut t,
        vec![Part::EdgeOpen(EdgeOpen {
            edge: EDGE.into(),
            input: ITEMS.into(),
            keyed: true,
        })],
    );
    apply(&mut t, vec![watch(&Arc::default())]);
    t
}

fn witness(t: &Tracking, x: u64, state: State) -> Part {
    Part::Witness(Witness {
        edge: EDGE.into(),
        gen: t.stale.inputs[EDGE].gen,
        x,
        state,
        retry: false,
    })
}

fn know(t: &mut Tracking, x: u64, state: State) {
    let w = witness(t, x, state);
    apply(t, vec![w]);
}

fn stale(k: &str, since: u64) -> State {
    State::Stale {
        key: key(k),
        reason: Reason::InputChanged,
        since_ms: since,
    }
}

fn batch(t: &Tracking, range: KeyRange, at: u64, reset: bool) -> Part {
    let k = t.edges[EDGE].progress.keyed().unwrap();
    Part::Batch(BatchProgress {
        edge: EDGE.into(),
        reset,
        range: range.clone(),
        piece: Some(Value::At(at, Arc::default())),
        points: vec![],
        expect: read_digest(k, reset, &range),
    })
}

fn range(lo: Option<&str>, hi: Option<&str>) -> KeyRange {
    KeyRange {
        lo: lo.map(key),
        hi: hi.map(key),
    }
}

#[test]
fn a_witness_applies_only_to_the_generation_it_was_computed_for() {
    let mut t = opened();
    assert!(t.stale.inputs[EDGE].current().is_none());
    let w = witness(&t, 0, State::Fresh);
    apply(&mut t, vec![w.clone()]);
    assert_eq!(t.stale.inputs[EDGE].current().unwrap().state, State::Fresh);
    let b = batch(&t, range(None, Some("k2")), 0, false);
    apply(&mut t, vec![b]);
    let i = &t.stale.inputs[EDGE];
    assert!(
        i.current().is_none(),
        "a batch commit makes what is known stale"
    );
    assert_eq!(
        i.changed,
        stale::Changed::Ranges(vec![range(None, Some("k2"))])
    );
    apply(&mut t, vec![w]);
    assert!(
        t.stale.inputs[EDGE].current().is_none(),
        "a part for an older generation is dropped"
    );
    know(&mut t, 3, stale("k1", 10));
    know(&mut t, 2, State::Fresh);
    assert_eq!(
        t.stale.inputs[EDGE].current().unwrap().x,
        3,
        "x never moves back"
    );
    know(&mut t, 4, stale("k3", 20));
    assert_eq!(
        t.stale.inputs[EDGE].current().unwrap().state,
        stale("k3", 10),
        "an edge that stays stale keeps the moment it went stale"
    );
}

#[test]
fn a_retry_drops_every_part_computed_before_it() {
    let mut t = opened();
    know(&mut t, 1, State::Fresh);
    let notice = witness(&t, 2, State::Fresh);
    let retry = Part::Witness(Witness {
        edge: EDGE.into(),
        gen: t.stale.inputs[EDGE].gen,
        x: 1,
        state: stale("k1", 10),
        retry: true,
    });
    apply(&mut t, vec![retry]);
    apply(&mut t, vec![notice]);
    let k = t.stale.inputs[EDGE].current().unwrap();
    assert_eq!(
        (k.x, &k.state),
        (1, &stale("k1", 10)),
        "a notice built before the retry came due does not make the edge fresh"
    );

    let mut t = opened();
    know(&mut t, 1, State::Fresh);
    let retry = Part::Witness(Witness {
        edge: EDGE.into(),
        gen: t.stale.inputs[EDGE].gen,
        x: 1,
        state: stale("k1", 10),
        retry: true,
    });
    know(&mut t, 2, State::Fresh);
    apply(&mut t, vec![retry]);
    let i = &t.stale.inputs[EDGE];
    assert!(
        i.current().is_none() && i.changed == stale::Changed::All,
        "a retry that lost the race to a later notice still makes the edge unknown"
    );
}

#[test]
fn a_reopened_edge_never_takes_a_part_for_the_edge_closed_under_its_name() {
    let mut t = opened();
    let old = witness(&t, 1, State::Fresh);
    apply(
        &mut t,
        vec![Part::EdgeClose(EdgeClose { edge: EDGE.into() })],
    );
    apply(
        &mut t,
        vec![Part::EdgeOpen(EdgeOpen {
            edge: EDGE.into(),
            input: ITEMS.into(),
            keyed: true,
        })],
    );
    apply(&mut t, vec![watch(&Arc::default())]);
    apply(&mut t, vec![old]);
    assert!(t.stale.inputs[EDGE].current().is_none());
}

#[test]
fn every_change_to_progress_or_conditions_moves_the_edge() {
    let mut t = opened();
    let gen = |t: &Tracking| t.stale.inputs[EDGE].gen;
    let g = gen(&t);
    apply(&mut t, vec![watch(&Arc::default())]);
    assert_eq!(gen(&t), g, "the same conditions again change nothing");
    let other = Arc::new(Conditions {
        context: b"w2".as_slice().into(),
        ..Default::default()
    });
    apply(&mut t, vec![watch(&other)]);
    assert_eq!(gen(&t), g + 1);
    know(&mut t, 0, State::Fresh);
    let b = batch(&t, range(Some("k1"), Some("k3")), 0, true);
    apply(&mut t, vec![b]);
    assert_eq!(
        t.stale.inputs[EDGE].changed,
        stale::Changed::All,
        "a reset probes everything"
    );
    know(&mut t, 0, State::Fresh);
    apply(
        &mut t,
        vec![Part::Reset(Reset {
            edge: EDGE.into(),
            held: true,
        })],
    );
    assert_eq!(t.stale.inputs[EDGE].changed, stale::Changed::All);
    apply(
        &mut t,
        vec![Part::EdgeRename(EdgeRename {
            from: EDGE.into(),
            to: key("checks/feed"),
            input: ITEMS.into(),
        })],
    );
    assert!(t.stale.inputs.contains_key(b"checks/feed".as_slice()));
    apply(
        &mut t,
        vec![Part::EdgeClose(EdgeClose {
            edge: key("checks/feed"),
        })],
    );
    assert!(t.stale.inputs.is_empty());
}

#[test]
fn the_gate_holds_readers_of_an_incomplete_upstream_and_releases_them_to_a_probe() {
    let mut t = opened();
    know(&mut t, 0, State::Fresh);
    let on = |on| {
        Part::Incomplete(Incomplete {
            output: ITEMS.into(),
            on,
        })
    };
    apply(&mut t, vec![on(true)]);
    assert_eq!(t.stale_status(EDGE).unwrap(), Status::Held);
    let (rt, ix) = index();
    let ask = t.stale_ask(EDGE).unwrap();
    assert_eq!(
        rt.block_on(stale::Counts::default().get(&ask, &OneIndex(&ix)))
            .unwrap(),
        None,
        "a held edge has no count (D197)"
    );
    apply(&mut t, vec![on(false)]);
    assert_eq!(t.stale_status(EDGE).unwrap(), Status::Unknown);
    assert_eq!(t.stale.inputs[EDGE].changed, stale::Changed::All);
}

#[test]
fn state_round_trips_through_events_and_snapshots() {
    let mut t = opened();
    know(&mut t, 0, stale("k1", 5));
    let b = batch(&t, range(Some("a"), Some("b")), 0, false);
    apply(
        &mut t,
        vec![
            b,
            Part::Incomplete(Incomplete {
                output: key("other"),
                on: true,
            }),
        ],
    );
    let back = Tracking::restore(&t.snapshot()).unwrap();
    assert_eq!(back.stale, t.stale);
    for p in [witness(&t, 9, stale("k", 1)), watch(&Arc::default())] {
        let e = Event::of(vec![p]);
        assert_eq!(Event::decode(&e.encode()).unwrap(), e);
    }
}

fn entries(keys: &[(&str, Option<u64>)], commit: u64) -> Run {
    let mut r = Run::new();
    for (k, v) in keys {
        r.push_key(k.as_bytes());
        let new = v.map_or(Version::Absent, Version::Int);
        let old = if v.is_some() {
            Version::Int(0)
        } else {
            Version::Int(1)
        };
        r.push_entry(commit, new, old);
    }
    r
}

fn commit_part(t: &Tracking) -> Part {
    let head = t.outputs.get(ITEMS).map_or(0, |o| o.head);
    let mut layer = solera_index::LayerRef::default();
    layer.lo = head + 1;
    layer.hi = head + 1;
    Part::OutputCommit(OutputCommit {
        output: ITEMS.into(),
        layer,
        live: 0,
    })
}

#[test]
fn a_notice_decides_from_the_commits_entries_alone() {
    let only_k = Arc::new(Conditions {
        patterns: Patterns::new(vec![Pat::Glob("k*".into())], vec![]).unwrap(),
        ..Default::default()
    });
    let mut t = opened();
    apply(&mut t, vec![watch(&only_k)]);
    know(&mut t, 0, State::Fresh);

    // Fresh: keys the patterns leave out are skipped; the first one taken is due.
    let e = entries(&[("a1", Some(1)), ("k2", Some(1)), ("k3", Some(1))], 1);
    let parts = stale::notice(&t, ITEMS, 1, &e, 7);
    let Part::Witness(w) = &parts[0] else {
        panic!()
    };
    assert_eq!(
        w.state,
        State::Stale {
            key: key("k2"),
            reason: Reason::NeverProcessed,
            since_ms: 7
        }
    );
    let mut ev = vec![commit_part(&t)];
    ev.extend(parts);
    apply(&mut t, ev);
    assert_eq!(
        t.stale_status(EDGE).unwrap(),
        Status::Stale {
            key: Some(key("k2")),
            reason: Reason::NeverProcessed,
            since_ms: 7
        }
    );

    // Stale: a commit that does not touch the witness keeps it; one that
    // does is left for a refresh.
    let parts = stale::notice(&t, ITEMS, 2, &entries(&[("k1", Some(2))], 2), 8);
    assert_eq!(parts.len(), 1);
    assert!(stale::notice(&t, ITEMS, 2, &entries(&[("k2", None)], 2), 8).is_empty());
    // Only the commit right after what is known is noticed.
    assert!(stale::notice(&t, ITEMS, 3, &entries(&[("k1", Some(2))], 3), 8).is_empty());

    // Fresh, nothing taken: fresh as of the commit.
    let mut t = opened();
    apply(&mut t, vec![watch(&only_k)]);
    know(&mut t, 0, State::Fresh);
    let parts = stale::notice(
        &t,
        ITEMS,
        1,
        &entries(&[("a1", Some(1)), ("x", None)], 1),
        7,
    );
    let Part::Witness(w) = &parts[0] else {
        panic!()
    };
    assert_eq!((w.x, &w.state), (1, &State::Fresh));
}

fn index() -> (tokio::runtime::Runtime, ChangeIndex) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let store = Store::new(
        Arc::new(InMemory::new()),
        Path::from("x"),
        rt.handle().clone(),
        StoreOptions::default(),
    )
    .unwrap();
    (rt, ChangeIndex::new(store, WriteOptions::default()))
}

/// Commits \`keys\` (version, or None for a removal) to items and applies it.
fn up(
    rt: &tokio::runtime::Runtime,
    ix: &ChangeIndex,
    t: &mut Tracking,
    keys: &[(&str, Option<u64>)],
) {
    let head = t.outputs.get(ITEMS).map_or(0, |o| o.head);
    let layers = t.outputs.get(ITEMS).map_or(vec![], |o| o.layers.clone());
    let live = t.outputs.get(ITEMS).map_or(0, |o| o.live);
    let mut delta = 0i64;
    let mut run = Run::new();
    for (k, v) in keys {
        let old = if head == 0 {
            solera_index::Page::default()
        } else {
            rt.block_on(ix.get(&layers, head, &[k.as_bytes()], &Default::default()))
                .unwrap()
        };
        let old_v = if old.run.is_empty() {
            Version::Absent
        } else {
            old.run.new_version(0)
        };
        delta += v.is_some() as i64 - (!old_v.is_absent()) as i64;
        run.push_key(k.as_bytes());
        run.push_entry(head + 1, v.map_or(Version::Absent, Version::Int), old_v);
    }
    let nc = NewCommit {
        commit: head + 1,
        time_ms: 0,
        producer: b"",
        live_before: live,
    };
    let layer = rt
        .block_on(ix.write_commit(&format!("items/{}", head + 1), nc, &run))
        .unwrap();
    apply(
        t,
        vec![Part::OutputCommit(OutputCommit {
            output: ITEMS.into(),
            layer,
            live: (live as i64 + delta) as u64,
        })],
    );
}

fn refresh(rt: &tokio::runtime::Runtime, ix: &ChangeIndex, t: &mut Tracking) -> Status {
    let ask = t.stale_ask(EDGE).unwrap();
    if let Some(w) = rt
        .block_on(stale::refresh(&ask, &OneIndex(ix), &NoneDue, 1))
        .unwrap()
    {
        apply(t, vec![Part::Witness(w)]);
    }
    t.stale_status(EDGE).unwrap()
}

#[test]
fn a_refresh_catches_up_over_commits_and_batches_at_once() {
    let (rt, ix) = index();
    let mut t = opened();
    up(
        &rt,
        &ix,
        &mut t,
        &[("k1", Some(1)), ("k2", Some(1)), ("k3", Some(1))],
    );
    assert!(
        matches!(refresh(&rt, &ix, &mut t), Status::Stale { key: Some(k), reason: Reason::NeverProcessed, .. } if *k == *b"k1")
    );
    let b = batch(&t, KeyRange::all(), 1, false);
    apply(&mut t, vec![b]);
    assert_eq!(refresh(&rt, &ix, &mut t), Status::Fresh);
    // A revert upstream, a change, and a batch that processed the change at
    // an older head: rule 1 outside the batch's range, a probe inside it.
    up(&rt, &ix, &mut t, &[("k2", Some(2))]);
    up(&rt, &ix, &mut t, &[("k2", Some(1)), ("k3", Some(3))]);
    let b = batch(&t, range(Some("k2"), Some("k3")), 3, false);
    apply(&mut t, vec![b]);
    assert_eq!(refresh(&rt, &ix, &mut t), Status::Fresh);
    up(&rt, &ix, &mut t, &[("k1", None)]);
    let b = batch(&t, range(Some("k2"), None), 3, false);
    apply(&mut t, vec![b]);
    assert!(
        matches!(refresh(&rt, &ix, &mut t), Status::Stale { key: Some(k), reason: Reason::InputChanged, .. } if *k == *b"k1")
    );
    let ask = t.stale_ask(EDGE).unwrap();
    let counts = stale::Counts::default();
    assert_eq!(
        rt.block_on(counts.get(&ask, &OneIndex(&ix))).unwrap(),
        Some([0, 0, 1, 0])
    );
    assert!(rt.block_on(ask.complete(&OneIndex(&ix))).unwrap());
    apply(
        &mut t,
        vec![Part::Reset(Reset {
            edge: EDGE.into(),
            held: false,
        })],
    );
    let ask = t.stale_ask(EDGE).unwrap();
    assert!(!rt.block_on(ask.complete(&OneIndex(&ix))).unwrap());
}

#[test]
fn staleness_flows_down_the_graph() {
    let mut t = opened();
    for (e, input, keyed) in [
        ("tally/checks", "checks", false),
        ("report/tally", "tally", false),
    ] {
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: key(e),
                input: key(input),
                keyed,
            })],
        );
    }
    know(&mut t, 0, stale("k1", 1));
    let p = |i: &[&str], o: &[&str]| Partition {
        inputs: i.iter().map(|s| key(s)).collect(),
        outputs: o.iter().map(|s| key(s)).collect(),
    };
    let parts = [
        p(&["checks/items"], &["checks"]),
        p(&["tally/checks"], &["tally"]),
        p(&["report/tally"], &[]),
    ];
    assert_eq!(
        graph(&t, &parts).unwrap(),
        vec![
            (Some(true), false),
            (Some(false), true),
            (Some(false), true)
        ]
    );
    assert!(matches!(
        *t.edges[b"tally/checks".as_slice()].progress,
        Progress::Unkeyed(0)
    ));
}
