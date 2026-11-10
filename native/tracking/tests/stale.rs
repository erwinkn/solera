//! Staleness (B7): what the events do to what is known, the notice over a
//! commit's entries, a refresh over a real index, and the answers. The
//! random histories against SOLE-7's model are in tests/core/test_stale_model.py.

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use solera_index::{ChangeIndex, NewCommit, Run, Store, StoreOptions, Version, WriteOptions};
use solera_tracking::codec::Ver;
use solera_tracking::commit::{FullRunDone, OutputCommit};
use solera_tracking::event::{Event, Part};
use solera_tracking::outcomes::commit::{self as oc, Ctx};
use solera_tracking::outcomes::transition::{Counters, Stored, FAILED};
use solera_tracking::outcomes::{self, Held, Outcomes, PassStart, Record};
use solera_tracking::plan::OneIndex;
use solera_tracking::progress::{
    self, read_digest, BatchProgress, Conditions, EdgeClose, EdgeOpen, EdgeRename, Holding, Key,
    KeyRange, Pat, Patterns, Point, Progress, Reset, Value,
};
use solera_tracking::stale::{
    self, graph, NoneDue, Partition, Reason, SetWatch, State, Status, Watch, Witness,
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
        fence: None,
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
fn the_gate_holds_readers_of_an_upstream_mid_full_run_and_releases_them_to_a_probe() {
    let clear = |t: &Tracking, ends_full_run| {
        let Part::OutputCommit(mut c) = commit_part(t) else {
            unreachable!()
        };
        c.layer.clears = vec![c.layer.hi];
        c.layer.flags = solera_index::format::CLEAR;
        c.ends_full_run = ends_full_run;
        Part::OutputCommit(c)
    };
    let (rt, ix) = index();
    let mut t = opened();
    know(&mut t, 0, State::Fresh);
    let p = clear(&t, false);
    apply(&mut t, vec![p]);
    assert_eq!(t.stale_status(EDGE).unwrap(), Status::Held);
    let ask = t.stale_ask(EDGE).unwrap();
    assert_eq!(
        rt.block_on(stale::Counts::default().get(&ask, &OneIndex(&ix), &NoneDue))
            .unwrap(),
        None,
        "a held edge has no count (D197)"
    );
    assert!(notice(&t, 2, &entries(&[("k", Some(2))], 2), 7).is_empty());
    apply(
        &mut t,
        vec![Part::FullRunDone(FullRunDone {
            output: ITEMS.into(),
        })],
    );
    assert_eq!(t.stale_status(EDGE).unwrap(), Status::Unknown);
    assert_eq!(t.stale.inputs[EDGE].changed, stale::Changed::All);

    let mut t = opened();
    know(&mut t, 0, State::Fresh);
    let p = clear(&t, true);
    apply(&mut t, vec![p]);
    assert_eq!(
        t.stale_status(EDGE).unwrap(),
        Status::Unknown,
        "a clear that also ends its run releases the edge to a probe"
    );
}

#[test]
fn a_notice_leaves_a_clear_and_an_edge_with_failure_records_for_a_refresh() {
    let mut t = opened();
    know(&mut t, 0, State::Fresh);
    let e = entries(&[("k", Some(1))], 1);
    assert_eq!(notice(&t, 1, &e, 7).len(), 1);
    let mut n = stale::Notice::new(&t, ITEMS, 1, true, 7);
    n.push(e.key(0), e.new_version(0));
    assert!(
        n.finish().is_empty(),
        "a clear's entries do not show what it removed"
    );
    let failures = solera_tracking::outcomes::output_id(solera_tracking::outcomes::FAILURES, EDGE);
    t.outputs.entry(failures).or_default().live = 1;
    assert!(notice(&t, 1, &e, 7).is_empty());
}

#[test]
fn state_round_trips_through_events_and_snapshots() {
    let mut t = opened();
    know(&mut t, 0, stale("k1", 5));
    let b = batch(&t, range(Some("a"), Some("b")), 0, false);
    apply(&mut t, vec![b]);
    let back = Tracking::restore(&t.snapshot()).unwrap();
    assert_eq!(back.stale, t.stale);
    for p in [witness(&t, 9, stale("k", 1)), watch(&Arc::default())] {
        let e = Event::of(vec![p]);
        assert_eq!(Event::decode(&e.encode()).unwrap(), e);
    }
}

#[test]
fn a_watch_shares_no_event_with_a_change_to_its_edge() {
    let mut t = opened();
    let before = t.snapshot();
    let other = Arc::new(Conditions {
        rebuild: 1,
        ..Default::default()
    });
    for p in [
        Part::EdgeClose(EdgeClose { edge: EDGE.into() }),
        Part::EdgeRename(EdgeRename {
            from: EDGE.into(),
            to: key("renamed"),
            input: ITEMS.into(),
        }),
        batch(&t, range(Some("a"), Some("b")), 0, false),
    ] {
        assert!(t
            .apply(&Event::of(vec![p, watch(&other)]).encode())
            .is_err());
        assert_eq!(t.snapshot(), before);
    }
}

#[test]
fn points_set_by_outcomes_move_the_edge_on() {
    let mut t = opened();
    know(&mut t, 0, State::Fresh);
    let gen = t.stale.inputs[EDGE].gen;
    let life = t.per_key[EDGE].life;
    apply(
        &mut t,
        vec![Part::Outcomes(Outcomes {
            edge: EDGE.into(),
            counts: [0; 6],
            classes: [0; 3],
            conds: vec![],
            refs: vec![],
            step: None,
            at: 0,
            look: None,
            low: None,
            life,
            made: None,
            points: vec![(
                key("k"),
                Point {
                    holding: Holding::Held,
                    reason: progress::Reason::Failed,
                    seen: None,
                },
            )],
        })],
    );
    let i = &t.stale.inputs[EDGE];
    assert!(i.gen > gen);
    assert!(i.current().is_none());
}

/// An edge `e` reading `up`, whose k1 (Int(1) at commit 1) failed in a batch
/// at commit 1 while the consumer held nothing: B6 keeps that in a record.
fn failed_k1(rt: &tokio::runtime::Runtime, ix: &ChangeIndex) -> Tracking {
    failed(rt, ix, &["k1"])
}

/// `failed_k1` for each of `keys`.
fn failed(rt: &tokio::runtime::Runtime, ix: &ChangeIndex, keys: &[&str]) -> Tracking {
    let c = Arc::new(Conditions::default());
    let mut t = Tracking::new();
    apply(
        &mut t,
        vec![Part::EdgeOpen(EdgeOpen {
            edge: key("e"),
            input: key("up"),
            keyed: true,
        })],
    );
    apply(
        &mut t,
        vec![Part::Watch(SetWatch {
            edge: key("e"),
            watch: Watch {
                conditions: c.clone(),
                own: None,
                gate: true,
            },
        })],
    );
    let ups: Vec<_> = keys
        .iter()
        .map(|k| (*k, Version::Absent, Version::Int(1)))
        .collect();
    let u = write(rt, ix, b"up", 1, 0, &ups);
    apply(&mut t, vec![u]);
    let b = Part::Batch(BatchProgress {
        edge: key("e"),
        reset: false,
        range: KeyRange::all(),
        piece: Some(Value::At(1, c.clone())),
        points: vec![],
        expect: read_digest(
            t.edges[b"e".as_slice()].progress.keyed().unwrap(),
            false,
            &KeyRange::all(),
        ),
        fence: None,
    });
    let cid = outcomes::cond_id(&c);
    let rec = Record {
        stored: Stored {
            outcome: FAILED,
            tries: 1,
            deploy: 0,
            forced: 0,
            since: 0,
            last: 0,
            next_at: 100,
            until: 0,
            message: b"error".as_slice().into(),
        },
        on: Ver::Int(1),
        on_cond: cid,
        at: 1,
        held: Held::Absent,
    }
    .encode();
    let fid = outcomes::output_id(outcomes::FAILURES, b"e");
    let recs: Vec<_> = keys
        .iter()
        .map(|k| (*k, Version::Absent, Version::Bytes(&rec)))
        .collect();
    let f = write(rt, ix, &fid, 1, 0, &recs);
    let o = Part::Outcomes(Outcomes {
        edge: key("e"),
        counts: [0, keys.len() as u64, 0, 0, 0, 0],
        classes: [keys.len() as u64, 0, 0],
        conds: vec![(cid, c)],
        refs: vec![(cid, keys.len() as u64)],
        step: None,
        at: 0,
        look: None,
        low: None,
        life: t.per_key[b"e".as_slice()].life,
        made: Some(1),
        points: vec![],
    });
    apply(&mut t, vec![f, b, o]);
    t
}

fn write_k1(
    rt: &tokio::runtime::Runtime,
    ix: &ChangeIndex,
    output: &[u8],
    commit: u64,
    old: Version<'_>,
    new: Version<'_>,
) -> Part {
    write(
        rt,
        ix,
        output,
        commit,
        (commit > 1) as u64,
        &[("k1", old, new)],
    )
}

/// Commits `entries` (key, replaced, new) to `output`, which held `live` keys.
fn write(
    rt: &tokio::runtime::Runtime,
    ix: &ChangeIndex,
    output: &[u8],
    commit: u64,
    live: u64,
    entries: &[(&str, Version<'_>, Version<'_>)],
) -> Part {
    let mut r = Run::new();
    let mut after = live as i64;
    for (k, old, new) in entries {
        r.push_key(k.as_bytes());
        r.push_entry(commit, *new, *old);
        after += !new.is_absent() as i64 - !old.is_absent() as i64;
    }
    let nc = NewCommit {
        commit,
        time_ms: 0,
        producer: b"",
        live_before: live,
        clear: false,
    };
    let path = format!(
        "{}/{commit}",
        String::from_utf8_lossy(output).replace('\0', "_")
    );
    let layer = rt.block_on(ix.write_commit(&path, nc, &r)).unwrap();
    Part::OutputCommit(OutputCommit {
        output: output.into(),
        layer,
        live: after as u64,
        ends_full_run: false,
    })
}

/// What the engine does before it refreshes or counts an edge with failure
/// records: look for reverts through the input's head (B6, KEY-3).
fn catch_up(rt: &tokio::runtime::Runtime, ix: &ChangeIndex, t: &mut Tracking) {
    let v = t.outcome_view(b"e").unwrap();
    let now = t.watched(
        b"e",
        t.stale.inputs[b"e".as_slice()].watch.as_ref().unwrap(),
    );
    let ctx = Ctx {
        now: 0.0,
        counters: Counters::default(),
        retries: 2,
        time_ms: 0,
        producer: b"",
        failures_path: &format!("f/{}", v.upstream.head),
        due_path: &format!("d/{}", v.upstream.head),
    };
    if let Some(parts) = rt
        .block_on(oc::catch_up(&OneIndex(ix), &v, &now, &ctx))
        .unwrap()
    {
        apply(t, parts);
    }
}

fn refresh_e(rt: &tokio::runtime::Runtime, ix: &ChangeIndex, t: &mut Tracking) -> Status {
    catch_up(rt, ix, t);
    let ask = t.stale_ask(b"e").unwrap();
    if let Some(w) = rt
        .block_on(stale::refresh(&ask, &OneIndex(ix), &NoneDue, 1))
        .unwrap()
    {
        apply(t, vec![Part::Witness(w)]);
    }
    t.stale_status(b"e").unwrap()
}

#[test]
fn a_failing_key_is_owed_by_what_its_record_holds() {
    let (rt, ix) = index();
    let mut t = failed_k1(&rt, &ix);
    assert_eq!(
        refresh_e(&rt, &ix, &mut t),
        Status::Fresh,
        "k1 waits for its retry"
    );

    let removal = write_k1(&rt, &ix, b"up", 2, Version::Int(1), Version::Absent);
    let mut n = stale::Notice::new(&t, b"up", 2, false, 7);
    n.push(b"k1", Version::Absent);
    assert!(
        n.finish().is_empty(),
        "an edge with records is left for a refresh"
    );
    apply(&mut t, vec![removal]);
    assert_eq!(
        refresh_e(&rt, &ix, &mut t),
        Status::Fresh,
        "upstream removed k1 and the consumer holds nothing: nothing is owed"
    );

    let back = write_k1(&rt, &ix, b"up", 3, Version::Absent, Version::Int(3));
    apply(&mut t, vec![back]);
    assert_eq!(
        refresh_e(&rt, &ix, &mut t),
        Status::Stale {
            key: Some(key("k1")),
            reason: Reason::InputChanged,
            since_ms: 1
        },
        "k1 changed since it failed: due"
    );
}

#[test]
fn a_witness_that_failed_waits_for_its_retry() {
    let (rt, ix) = index();
    let mut t = failed_k1(&rt, &ix);
    let gen = t.stale.inputs[b"e".as_slice()].gen;
    apply(
        &mut t,
        vec![Part::Witness(Witness {
            edge: key("e"),
            gen,
            x: 0,
            state: stale("k1", 1),
        })],
    );
    assert_eq!(
        refresh_e(&rt, &ix, &mut t),
        Status::Fresh,
        "the recheck of a witness that has failed since reads its record"
    );
}

#[test]
fn a_retry_pass_makes_its_next_key_due() {
    let (rt, ix) = index();
    let mut t = failed_k1(&rt, &ix);
    assert_eq!(refresh_e(&rt, &ix, &mut t), Status::Fresh);
    let count = |t: &Tracking| {
        let ask = t.stale_ask(b"e").unwrap();
        let o = rt.block_on(stale::Counts::default().get(&ask, &OneIndex(&ix), &NoneDue));
        let keys = rt.block_on(ask.keys(&OneIndex(&ix), &NoneDue, None, 10));
        (o.unwrap().unwrap(), keys.unwrap().0.len())
    };
    let (o, listed) = count(&t);
    assert_eq!(
        (o.due, o.waiting, o.reasons, listed),
        ([0; 4], [1, 0, 0, 0], vec![], 0),
        "k1 waits: owed, not due, not listed"
    );

    let gen = t.stale.inputs[b"e".as_slice()].gen;
    let notice = Part::Witness(Witness {
        edge: key("e"),
        gen,
        x: 1,
        state: State::Fresh,
    });
    let life = t.per_key[b"e".as_slice()].life;
    apply(
        &mut t,
        vec![Part::PassStart(PassStart {
            edge: key("e"),
            n: 1,
            at: 200,
            counters: Counters {
                deploy: 1,
                ..Counters::default()
            },
            life,
        })],
    );
    apply(&mut t, vec![notice]);
    assert_eq!(
        t.stale_status(b"e").unwrap(),
        Status::Unknown,
        "a part computed before the pass began is dropped"
    );
    assert_eq!(
        refresh_e(&rt, &ix, &mut t),
        Status::Stale {
            key: Some(key("k1")),
            reason: Reason::RetryDue,
            since_ms: 1
        }
    );
    let (o, listed) = count(&t);
    assert_eq!(
        (o.due, o.waiting, o.reasons, listed),
        ([0; 4], [1, 0, 0, 0], vec![Reason::RetryDue], 0),
        "a key the pass retries stays waiting; the pass gives the reason"
    );
}

#[test]
fn a_full_run_asked_for_by_hand_raises_the_rebuild_counter_staleness_reads() {
    let mut t = opened();
    know(&mut t, 0, State::Fresh);
    let life = t.per_key[EDGE].life;
    apply(
        &mut t,
        vec![Part::Rebuild(outcomes::Rebuild {
            edge: EDGE.into(),
            to: 1,
            life,
        })],
    );
    let i = &t.stale.inputs[EDGE];
    assert_eq!(i.changed, stale::Changed::All);
    assert_eq!(t.stale_status(EDGE).unwrap(), Status::Unknown);
    assert_eq!(t.stale_ask(EDGE).unwrap().req.conditions.rebuild, 1);
    apply(&mut t, vec![watch(&Arc::new(Conditions::default()))]);
    assert_eq!(
        t.stale_ask(EDGE).unwrap().req.conditions.rebuild,
        1,
        "a watch journaled with an older counter does not undo the rebuild"
    );

    let mut t = opened();
    know(&mut t, 0, State::Fresh);
    let life = t.per_key[EDGE].life;
    let rebuild = Part::Rebuild(outcomes::Rebuild {
        edge: EDGE.into(),
        to: 1,
        life,
    });
    let e = Event::of(vec![rebuild, watch(&Arc::new(Conditions::default()))]);
    t.apply(&e.encode()).unwrap();
    assert_eq!(
        t.stale_ask(EDGE).unwrap().req.conditions.rebuild,
        1,
        "nor does one in the same event"
    );
}

#[test]
fn a_refresh_on_an_edge_with_failure_records_waits_for_the_catch_up() {
    let (rt, ix) = index();
    let mut t = failed_k1(&rt, &ix);
    assert_eq!(refresh_e(&rt, &ix, &mut t), Status::Fresh, "k1 waits");
    let removal = write_k1(&rt, &ix, b"up", 2, Version::Int(1), Version::Absent);
    let back = write_k1(&rt, &ix, b"up", 3, Version::Absent, Version::Int(1));
    apply(&mut t, vec![removal]);
    apply(&mut t, vec![back]);
    let refused = t.stale_ask(b"e").err().expect("refused").to_string();
    assert!(refused.contains("catch up first"), "{refused}");
    assert!(
        matches!(
            refresh_e(&rt, &ix, &mut t),
            Status::Stale { key: Some(k), .. } if k == key("k1")
        ),
        "k1's failure went moot when upstream removed it, so its return is owed"
    );
}

struct HoldK1;

impl stale::Due for HoldK1 {
    fn not_due(&self, _: &[u8], key: &[u8]) -> bool {
        key == b"k1"
    }
    fn revision(&self, _: &[u8]) -> u64 {
        1
    }
}

#[test]
fn rule_1_never_finds_due_a_key_held_back() {
    let (rt, ix) = index();
    let mut t = opened();
    up(&rt, &ix, &mut t, &[("k1", Some(1))]);
    let b = batch(&t, range(None, None), 1, false);
    apply(&mut t, vec![b]);
    let go = |t: &mut Tracking| {
        let ask = t.stale_ask(EDGE).unwrap();
        if let Some(w) = rt
            .block_on(stale::refresh(&ask, &OneIndex(&ix), &HoldK1, 1))
            .unwrap()
        {
            apply(t, vec![Part::Witness(w)]);
        }
        t.stale_status(EDGE).unwrap()
    };
    assert_eq!(go(&mut t), Status::Fresh);
    up(&rt, &ix, &mut t, &[("k1", Some(2))]);
    assert_eq!(go(&mut t), Status::Fresh, "rule 1 over the change");
    let i = t.stale.inputs.get_mut(EDGE).unwrap();
    i.known = None;
    i.changed = stale::Changed::All;
    assert_eq!(go(&mut t), Status::Fresh, "a probe from nothing known");
}

#[test]
fn a_retry_pass_reads_past_keys_gone_moot() {
    let (rt, ix) = index();
    let mut t = failed(&rt, &ix, &["k1", "k2"]);
    assert_eq!(refresh_e(&rt, &ix, &mut t), Status::Fresh);
    let removal = write(
        &rt,
        &ix,
        b"up",
        2,
        2,
        &[("k1", Version::Int(1), Version::Absent)],
    );
    apply(&mut t, vec![removal]);
    let life = t.per_key[b"e".as_slice()].life;
    apply(
        &mut t,
        vec![Part::PassStart(PassStart {
            edge: key("e"),
            n: 1,
            at: 200,
            counters: Counters {
                deploy: 1,
                ..Counters::default()
            },
            life,
        })],
    );
    assert_eq!(
        refresh_e(&rt, &ix, &mut t),
        Status::Stale {
            key: Some(key("k2")),
            reason: Reason::RetryDue,
            since_ms: 1
        },
        "k1 went moot when upstream removed it; the pass's next key is k2"
    );
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

/// The notice over `entries`, fed as the commit builder feeds it.
fn notice(t: &Tracking, commit: u64, entries: &Run, time_ms: u64) -> Vec<Part> {
    let mut n = stale::Notice::new(t, ITEMS, commit, false, time_ms);
    for i in 0..entries.len() {
        n.push(entries.key(i), entries.new_version(entries.rows(i).start));
    }
    n.finish()
}

fn commit_part(t: &Tracking) -> Part {
    let head = t.outputs.get(ITEMS).map_or(0, |o| o.head);
    let layer = solera_index::LayerRef {
        lo: head + 1,
        hi: head + 1,
        ..Default::default()
    };
    Part::OutputCommit(OutputCommit {
        output: ITEMS.into(),
        layer,
        live: 0,
        ends_full_run: false,
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
    let parts = notice(&t, 1, &e, 7);
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
    let parts = notice(&t, 2, &entries(&[("k1", Some(2))], 2), 8);
    assert_eq!(parts.len(), 1);
    assert!(notice(&t, 2, &entries(&[("k2", None)], 2), 8).is_empty());
    // Only the commit right after what is known is noticed.
    assert!(notice(&t, 3, &entries(&[("k1", Some(2))], 3), 8).is_empty());

    // Fresh, nothing taken: fresh as of the commit.
    let mut t = opened();
    apply(&mut t, vec![watch(&only_k)]);
    know(&mut t, 0, State::Fresh);
    let parts = notice(&t, 1, &entries(&[("a1", Some(1)), ("x", None)], 1), 7);
    let Part::Witness(w) = &parts[0] else {
        panic!()
    };
    assert_eq!((w.x, &w.state), (1, &State::Fresh));
}

#[test]
fn a_notice_settles_each_stale_witness_at_the_first_entry_at_or_past_it() {
    let mut t = Tracking::new();
    for (e, w) in [("a/items", "k5"), ("b/items", "k1"), ("c/items", "k3")] {
        apply(
            &mut t,
            vec![Part::EdgeOpen(EdgeOpen {
                edge: key(e),
                input: ITEMS.into(),
                keyed: true,
            })],
        );
        apply(
            &mut t,
            vec![Part::Watch(SetWatch {
                edge: key(e),
                watch: Watch {
                    conditions: Arc::default(),
                    own: None,
                    gate: true,
                },
            })],
        );
        let gen = t.stale.inputs[e.as_bytes()].gen;
        apply(
            &mut t,
            vec![Part::Witness(Witness {
                edge: key(e),
                gen,
                x: 0,
                state: stale(w, 1),
            })],
        );
    }
    let e = entries(&[("k2", Some(1)), ("k3", Some(1)), ("k4", Some(1))], 1);
    let mut kept: Vec<(Key, State)> = notice(&t, 1, &e, 9)
        .into_iter()
        .map(|p| match p {
            Part::Witness(w) => (w.edge, w.state),
            _ => panic!(),
        })
        .collect();
    kept.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        kept,
        vec![
            (key("a/items"), stale("k5", 1)),
            (key("b/items"), stale("k1", 1))
        ],
        "c's witness k3 is touched and left for a refresh"
    );
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
        clear: false,
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
            ends_full_run: false,
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
        rt.block_on(counts.get(&ask, &OneIndex(&ix), &NoneDue))
            .unwrap(),
        Some(stale::Owed {
            due: [0, 0, 1, 0],
            waiting: [0; 4],
            reasons: vec![Reason::InputChanged]
        })
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
fn a_count_says_every_reason_that_holds() {
    let (rt, ix) = index();
    let mut t = opened();
    up(&rt, &ix, &mut t, &[("k1", Some(1)), ("k2", Some(1))]);
    let b = batch(&t, range(None, Some("k1")), 1, false);
    apply(&mut t, vec![b]);
    up(&rt, &ix, &mut t, &[("k1", Some(2)), ("k3", Some(1))]);
    struct Waiting;
    impl stale::Due for Waiting {
        fn not_due(&self, _: &[u8], key: &[u8]) -> bool {
            key == b"k1"
        }

        fn revision(&self, _: &[u8]) -> u64 {
            1
        }
    }
    let counts = stale::Counts::default();
    let get = |t: &Tracking, due: &dyn stale::Due| {
        let ask = t.stale_ask(EDGE).unwrap();
        let o = rt.block_on(counts.get(&ask, &OneIndex(&ix), due));
        o.unwrap().unwrap()
    };
    let o = get(&t, &NoneDue);
    assert_eq!((o.due, o.waiting), ([2, 1, 0, 0], [0; 4]));
    assert_eq!(
        o.reasons,
        vec![Reason::InputChanged, Reason::NeverProcessed]
    );
    let o = get(&t, &Waiting);
    assert_eq!(
        (o.due, o.waiting, o.reasons),
        ([2, 0, 0, 0], [0, 1, 0, 0], vec![Reason::NeverProcessed]),
        "a key held back is owed and waiting, and gives no reason"
    );
    assert_eq!(
        get(&t, &NoneDue).reasons,
        vec![Reason::InputChanged, Reason::NeverProcessed],
        "a cached count holds for the keys not due it was counted with"
    );
    let other = Arc::new(Conditions {
        context: b"w2".as_slice().into(),
        ..Default::default()
    });
    apply(&mut t, vec![watch(&other)]);
    assert_eq!(
        get(&t, &NoneDue).reasons,
        vec![Reason::ConditionsChanged, Reason::NeverProcessed]
    );
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
