//! What a batch commit writes to an edge's records: each taken key's
//! outcome through the transition table, each moot failure dropped, the
//! retry clock moved with them; and the points progress no longer needs,
//! since a failing key's record now holds what it held.

use std::collections::BTreeMap;
use std::sync::Arc;

use solera_index::{Query, Version};

use crate::codec::Ver;
use crate::commit::{build, Change, Changes, Held as Source, Mode, Spec};
use crate::error::{invalid, Result};
use crate::event::Part;
use crate::plan::{Indexes, Mode as PlanMode, Plan};
use crate::progress::{owed, Class, Holding, Key, Ops, Point, Progress, Reason};
use crate::state::{Id, Output, Tracking};

use super::transition::{transition, Counters, Reported};
use super::{
    cond_id, output_id, record_digest, Conds, Held, Look, Outcomes, PerKey, Record, Step, DUE,
    FAILURES,
};

/// What committing reads of the state, taken under the caller's lock.
#[derive(Clone)]
pub struct View {
    pub edge: Id,
    pub failures: Output,
    pub due: Output,
    pub per_key: PerKey,
    pub progress: Arc<Progress>,
    /// The output the edge reads, and its log.
    pub input: Id,
    pub upstream: Output,
}

impl Tracking {
    pub fn outcome_view(&self, edge: &[u8]) -> Result<View> {
        let Some(e) = self.edges.get(edge) else {
            return invalid(format!("no edge {}", String::from_utf8_lossy(edge)));
        };
        let out = |k| {
            self.outputs
                .get(&output_id(k, edge))
                .cloned()
                .unwrap_or_default()
        };
        Ok(View {
            edge: edge.into(),
            failures: out(FAILURES),
            due: out(DUE),
            per_key: self.per_key.get(edge).cloned().unwrap_or_default(),
            progress: e.progress.clone(),
            input: e.input.clone(),
            upstream: self.outputs.get(&e.input).cloned().unwrap_or_default(),
        })
    }
}

/// Where and when a batch's outcomes are committed.
#[derive(Clone, Debug)]
pub struct Ctx<'a> {
    /// Seconds since the epoch, the engine's clock at commit.
    pub now: f64,
    /// The counters the batch ran under (a retry batch runs under its pass's).
    pub counters: Counters,
    /// The asset's `retries=`, which bounds timeouts.
    pub retries: u64,
    pub time_ms: u64,
    pub producer: &'a [u8],
    /// Paths for the layers of the record and clock outputs.
    pub failures_path: &'a str,
    pub due_path: &'a str,
}

/// The key of a retrying key's entry on the retry clock: its due time, then
/// the key.
pub fn due_key(at: u64, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(8 + key.len());
    k.extend_from_slice(&at.to_be_bytes());
    k.extend_from_slice(key);
    k
}

/// The point a failure gone moot leaves: the consumer keeps what it held,
/// and its piece says what it was tried on.
pub(crate) fn moot_point(r: &Record, conds: &Conds) -> Result<Point> {
    Ok(Point {
        holding: r.held.holding(conds)?,
        reason: Reason::Failed,
        seen: None,
    })
}

/// The records of `keys` (sorted, distinct) at the output's head.
pub async fn records(
    ixs: &dyn Indexes,
    output: &Id,
    out: &Output,
    keys: &[&[u8]],
) -> Result<BTreeMap<Key, Record>> {
    let mut found = BTreeMap::new();
    if out.live == 0 || keys.is_empty() {
        return Ok(found);
    }
    let page = ixs
        .index(output)?
        .get(&out.layers, out.head, keys, &Query::default())
        .await?;
    for i in 0..page.run.len() {
        let solera_index::Version::Bytes(b) = page.run.new_version(page.run.rows(i).start) else {
            return invalid("a record that is not a version word");
        };
        found.insert(page.run.key(i).into(), Record::decode(b)?);
    }
    Ok(found)
}

/// One record's share of the edge's counts.
pub(crate) fn tally(
    r: &Record,
    key: &[u8],
    conds: &Conds,
    counts: &mut [u64; 6],
    classes: &mut [u64; 3],
    refs: &mut BTreeMap<u64, u64>,
    add: bool,
) -> Result<()> {
    let bump = |n: &mut u64| {
        if add {
            *n += 1
        } else {
            *n -= 1
        }
    };
    bump(&mut counts[r.stored.outcome as usize]);
    let class = Class::of(&r.held.holding(conds)?, &r.tried(key, conds)?);
    if class != Class::Unchanged {
        bump(&mut classes[class as usize - 1]);
    }
    for c in r.conds() {
        bump(refs.entry(c).or_default());
    }
    Ok(())
}

/// The parts that commit a batch's outcomes for one keyed input of `plan`:
/// its record output's commit, its retry clock's, and [`Outcomes`]; it also
/// drops from `ops` (B3's progress parts and records for this plan) the
/// points that the records now stand for. `reported` names every taken key
/// that did not succeed (sorted); every other taken key succeeded. `retry`:
/// the pass whose step this batch is.
pub async fn commit(
    ixs: &dyn Indexes,
    v: &View,
    plan: &Plan,
    input: usize,
    reported: &[(Key, Reported)],
    loaded: &[(Key, Ver)],
    now: &Arc<crate::progress::Conditions>,
    ctx: &Ctx<'_>,
    ops: &mut Ops,
) -> Result<Vec<Part>> {
    let Some(p) = plan.inputs.get(input) else {
        return invalid("no such keyed input in the plan");
    };
    if p.edge != v.edge {
        return invalid("the view is of another edge");
    }
    if plan.reset {
        return invalid("a per-key batch never resets its progress (D194)");
    }
    if reported.windows(2).any(|w| w[0].0 >= w[1].0) {
        return invalid("reported keys are sorted and distinct");
    }
    let cond_now = cond_id(now);
    let caught_up = Look {
        seen: v.upstream.head,
        cond: cond_now,
    };
    let looked = Look {
        seen: v.per_key.seen,
        cond: v.per_key.cond,
    };
    if v.failures.live > 0 && looked != caught_up {
        return invalid("catch up first: the edge's reverts are not looked for through the input's head under the conditions now");
    }
    let keyed = v.progress.keyed().expect("a per-key edge is keyed");
    let pass = plan.pass.as_ref().filter(|id| id.edge == v.edge);
    let retry = pass.is_some();
    let step = if let Some(id) = pass {
        if id.life != v.per_key.life {
            return invalid("a retry batch of a pass in an earlier life of the edge");
        }
        let Some(pass) = v.per_key.pass.as_ref().filter(|p| p.n == id.n) else {
            return invalid("a retry batch of a pass that ended");
        };
        Some(Step {
            n: pass.n,
            from: plan.range.lo.clone(),
            to: plan.range.hi.clone(),
        })
    } else {
        None
    };
    let counters = match (&step, &v.per_key.pass) {
        (Some(_), Some(pass)) => pass.counters,
        _ => ctx.counters,
    };
    let fid = output_id(FAILURES, &v.edge);
    if loaded.windows(2).any(|w| w[0].0 >= w[1].0) {
        return invalid("loaded keys are sorted and distinct");
    }
    let mut touched: Vec<&[u8]> = p.touched().map(|k| k.as_ref()).collect();
    touched.sort_unstable();
    touched.dedup();
    let priors = records(ixs, &fid, &v.failures, &touched).await?;
    let read = priors
        .iter()
        .map(|(k, r)| record_digest(k, r))
        .fold(0, u64::wrapping_add);
    if read != p.records {
        return invalid("the edge's records moved since the batch was planned");
    }

    let mut conds: Conds = (*v.per_key.conds).clone();
    let mut added: Vec<(u64, Arc<crate::progress::Conditions>)> = vec![];
    let mut learn = |c: &Arc<crate::progress::Conditions>, conds: &mut Conds| -> u64 {
        let id = cond_id(c);
        if !conds.contains_key(&id) {
            conds.insert(id, c.clone());
            added.push((id, c.clone()));
        }
        id
    };
    let on_cond = learn(&p.conditions, &mut conds);
    // What B3 held back keeps what it held, and its record as it is: the
    // hold-back comes before the failure check (D205).
    let Some(held_back) = ops.held_back.get(input).cloned() else {
        return invalid("the batch's ops name no held-back keys for this input");
    };
    let mut next: BTreeMap<Key, Option<Record>> = BTreeMap::new();
    let mut rep = reported.iter().peekable();
    for c in &p.keys {
        while rep.peek().is_some_and(|(k, _)| *k < c.key) {
            rep.next();
        }
        let r = rep.next_if(|(k, _)| *k == c.key).map(|(_, r)| r);
        if held_back.binary_search(&c.key).is_ok() {
            continue;
        }
        let prior = priors.get(&c.key);
        let tried = loaded
            .binary_search_by(|(k, _)| k.cmp(&c.key))
            .map_or(&c.version, |i| &loaded[i].1);
        let stored = match r {
            None => None,
            Some(_)
                if !owed(
                    &c.held,
                    &Holding::of(tried.as_version(), &p.conditions, &c.key),
                ) =>
            {
                None
            }
            Some(r) => {
                // A try is fresh on another version, or under conditions that
                // process differently; a change of patterns alone is not one.
                let fresh = prior.is_none_or(|pr| {
                    pr.on != *tried
                        || !conds
                            .get(&pr.on_cond)
                            .is_some_and(|c| c.same_processing(&p.conditions))
                });
                transition(
                    prior.map(|p| &p.stored),
                    fresh,
                    r,
                    ctx.now,
                    counters.deploy,
                    counters.forced_at(),
                    ctx.retries,
                )
            }
        };
        let rec = match stored {
            None => None,
            Some(s) => {
                let held = match &c.held {
                    Holding::Version(_, hc) => {
                        let id = learn(hc, &mut conds);
                        match Held::of(&c.held) {
                            Held::Version(v, _) => Held::Version(v, id),
                            h => h,
                        }
                    }
                    h => Held::of(h),
                };
                Some(Record {
                    stored: s,
                    on: tried.clone(),
                    on_cond,
                    at: p.head,
                    held,
                })
            }
        };
        if rec.is_some() || prior.is_some() {
            next.insert(c.key.clone(), rec);
        }
    }
    // New failures are made, and checked for moot, at the view's head of the
    // input and under the conditions now; the commit applies only while the
    // input is still there and reverts were looked for nowhere else.
    let made = next
        .values()
        .any(Option::is_some)
        .then_some(v.upstream.head);
    let mut points = Vec::new();
    // A new failure is checked against the input's head under the conditions
    // now, as the model does at commit: one whose input already returned to
    // what the consumer holds is moot, and the consumer keeps what it held as
    // a point.
    if v.upstream.head > p.head || cond_now != on_cond {
        let fresh: Vec<Key> = next
            .iter()
            .filter(|(_, r)| r.is_some())
            .map(|(k, _)| k.clone())
            .collect();
        let keys: Vec<&[u8]> = fresh.iter().map(|k| k.as_ref()).collect();
        let mut at_head = vec![Ver::Absent; keys.len()];
        if !keys.is_empty() {
            let got = ixs
                .index(&v.input)?
                .get(
                    &v.upstream.layers,
                    v.upstream.head,
                    &keys,
                    &Query::default(),
                )
                .await?;
            let mut j = 0;
            for i in 0..got.run.len() {
                while keys[j] != got.run.key(i) {
                    j += 1;
                }
                at_head[j] = Ver::of(got.run.new_version(got.run.rows(i).start));
            }
        }
        for (k, ver) in fresh.into_iter().zip(at_head) {
            let rec = next[&k].as_ref().expect("a new record");
            let held = rec.held.holding(&conds)?;
            if owed(&held, &Holding::of(ver.as_version(), now, &k)) {
                continue;
            }
            points.push((k.clone(), moot_point(rec, &conds)?));
            if priors.contains_key(&k) {
                next.insert(k, None);
            } else {
                next.remove(&k);
            }
        }
    }
    let range_batch = !matches!(plan.mode, PlanMode::Keys(_));
    for k in &p.moot {
        if next.contains_key(k) {
            continue;
        }
        let Some(prior) = priors.get(k) else { continue };
        next.insert(k.clone(), None);
        if !range_batch {
            points.push((k.clone(), moot_point(prior, &conds)?));
        }
    }
    if rep.next().is_some()
        || reported
            .iter()
            .any(|(k, _)| p.keys.binary_search_by(|c| c.key.cmp(k)).is_err())
    {
        return invalid("an outcome reported for a key the batch did not take");
    }

    let mut counts = v.per_key.counts;
    let mut classes = v.per_key.classes;
    let mut refs = v.per_key.refs.clone();
    let before = refs.clone();
    let mut fch = Changes::new();
    let mut words: Vec<(Key, Vec<u8>)> = Vec::new();
    let mut due: Vec<(Vec<u8>, bool)> = Vec::new();
    for (k, rec) in &next {
        let prior = priors.get(k);
        if let Some(p) = prior {
            tally(p, k, &conds, &mut counts, &mut classes, &mut refs, false)?;
        }
        if let Some(r) = rec {
            tally(r, k, &conds, &mut counts, &mut classes, &mut refs, true)?;
        }
        let at = |r: Option<&Record>| {
            r.map(|r| &r.stored)
                .filter(|s| s.timed())
                .map(|s| s.next_at)
        };
        let (old, new) = (at(prior), at(rec.as_ref()));
        if old != new {
            if let Some(t) = old {
                due.push((due_key(t, k), false));
            }
            if let Some(t) = new {
                due.push((due_key(t, k), true));
            }
        }
        if let Some(r) = rec {
            words.push((k.clone(), r.encode()));
        }
    }
    let mut w = words.iter().peekable();
    for (k, rec) in &next {
        match rec {
            Some(_) => {
                let (_, word) = w.next().expect("one word per record");
                fch.push(k, Change::Upsert(Some(word)))?;
            }
            None => fch.push(k, Change::Removed)?,
        }
    }
    due.sort();
    let mut dch = Changes::new();
    for (k, add) in &due {
        dch.push(
            k,
            if *add {
                Change::Upsert(Some(b""))
            } else {
                Change::Removed
            },
        )?;
    }

    let spec = |path| Spec {
        version: 0,
        time_ms: ctx.time_ms,
        producer: ctx.producer,
        path,
        mode: Mode::Patch,
        ends_full_run: false,
        collect: 0,
    };
    let mut parts = Vec::new();
    if let Some(b) = build(
        ixs,
        &fid,
        &v.failures,
        spec(ctx.failures_path),
        &mut Source(Some(&fch)),
    )
    .await?
    {
        parts.push(Part::OutputCommit(b.part));
    }
    let did = output_id(DUE, &v.edge);
    if let Some(b) = build(
        ixs,
        &did,
        &v.due,
        spec(ctx.due_path),
        &mut Source(Some(&dch)),
    )
    .await?
    {
        parts.push(Part::OutputCommit(b.part));
    }
    let refs_moved = moved(&before, &refs);
    let referenced = |id: &u64| refs.get(id).is_some_and(|n| *n > 0);
    // A failing key tried at another version than planned keeps its point
    // even where it holds what the piece says, which is not what it was
    // tried on: a retry's point is needless only where the piece says that.
    for (k, rec) in &next {
        if let Some(rec) = rec {
            if loaded.binary_search_by(|(x, _)| x.cmp(k)).is_ok() {
                points.push((k.clone(), moot_point(rec, &conds)?));
            }
        }
    }
    points.sort_by(|a, b| a.0.cmp(&b.0));
    points.dedup_by(|a, b| a.0 == b.0);
    parts.push(Part::Outcomes(Outcomes {
        edge: v.edge.clone(),
        counts,
        classes,
        conds: added.into_iter().filter(|(id, _)| referenced(id)).collect(),
        refs: refs_moved,
        step,
        at: v.failures.head,
        look: made.map(|_| (looked, caught_up)),
        life: v.per_key.life,
        // A key held back while this input read it as planned, at what its
        // record failed on (another input moved), was not retried and its
        // input has not moved: it waits for its retry again, so it lowers
        // what the pass covers like a record written. One read at another
        // version comes back with its change, or the catch-up lowers it
        // when the input returns; one planned at another holding than its
        // record's is a change batch's.
        low: next
            .values()
            .flatten()
            .chain(
                p.keys
                    .iter()
                    .filter(|c| held_back.binary_search(&c.key).is_ok())
                    .filter_map(|c| {
                        let pr = priors.get(&c.key)?;
                        let on = conds.get(&pr.on_cond)?;
                        let tried = Holding::of(pr.on.as_version(), on, &c.key);
                        let planned = Holding::of(c.version.as_version(), &p.conditions, &c.key);
                        let got = loaded
                            .binary_search_by(|(x, _)| x.cmp(&c.key))
                            .map_or(&c.version, |i| &loaded[i].1);
                        let read = Holding::of(got.as_version(), &p.conditions, &c.key);
                        (!owed(&tried, &planned) && !owed(&read, &planned)).then_some(pr)
                    }),
            )
            .fold(None, |mut low, r| {
                cover(&mut low, &r.stored);
                low
            }),
        made,
        points: vec![],
    }));

    // A failing key the worker tried at another version than planned keeps
    // its point: the piece no longer says what it was tried on.
    let needless = |key: &Key, reason: Reason, holding: &Holding| -> bool {
        if loaded.binary_search_by(|(k, _)| k.cmp(key)).is_ok() {
            return false;
        }
        if range_batch {
            return reason == Reason::Failed;
        }
        if !retry || keyed.point(key).is_some() {
            return false;
        }
        match reason {
            Reason::Failed => true,
            Reason::KeyList => priors
                .get(key)
                .and_then(|r| r.tried(key, &conds).ok())
                .is_some_and(|t| !crate::progress::owed(&t, holding)),
            _ => false,
        }
    };
    // The points this commit sets go with the batch's own in its progress
    // part: one part per edge. Its record keeps the batch's marks, which say
    // what it did with each key, not what progress needs.
    let set = |into: &mut Vec<(Key, Point)>| {
        into.retain(|(k, pt)| !needless(k, pt.reason, &pt.holding));
        for (k, pt) in &points {
            match into.binary_search_by(|(x, _)| x.cmp(k)) {
                Ok(i) => into[i].1 = pt.clone(),
                Err(i) => into.insert(i, (k.clone(), pt.clone())),
            }
        }
    };
    for part in &mut ops.parts {
        if let Part::Batch(b) = part {
            if b.edge == v.edge {
                set(&mut b.points);
            }
        }
    }
    for r in &mut ops.records {
        if r.edge == v.edge {
            // What the consumer held where a record said so: lineage
            // recomputes the batch's input from this slice alone (§7).
            for (k, prior) in &priors {
                if let Err(i) = r.replaced.points.binary_search_by(|(x, _)| x.cmp(k)) {
                    r.replaced
                        .points
                        .insert(i, (k.clone(), moot_point(prior, &conds)?));
                }
            }
        }
    }
    Ok(parts)
}

/// Looks for reverts since the edge last looked (KEY-3, §11): a failing key
/// whose input returns to what the consumer holds, after it was tried, is
/// moot. Its record goes, and a point keeps what the consumer holds, since
/// its piece says what it was tried on. Reads the input's entries since
/// then, and the records of their keys. The engine calls it after the
/// input's commits and before it plans, counts or changes the conditions,
/// as the witness catches up (§6). None when there is nothing to record.
pub async fn catch_up(
    ixs: &dyn Indexes,
    v: &View,
    now: &Arc<crate::progress::Conditions>,
    ctx: &Ctx<'_>,
) -> Result<Option<Vec<Part>>> {
    let head = v.upstream.head;
    let seen = v.per_key.seen;
    let cond = cond_id(now);
    let moved_on = cond != v.per_key.cond;
    if v.failures.live == 0 || (seen >= head && !moved_on) {
        return Ok(None);
    }
    let fid = output_id(FAILURES, &v.edge);
    let conds: &Conds = &v.per_key.conds;
    let ix = ixs.index(&v.input)?;
    let mut gone: BTreeMap<Key, Record> = BTreeMap::new();
    let mut low = None;
    let mut after: Option<Vec<u8>> = None;
    while seen < head {
        let q = Query {
            after: after.take(),
            limit: Some(65_536),
            ..Default::default()
        };
        let page = ix.window(&v.upstream.layers, seen + 1..=head, &q).await?;
        let run = &page.run;
        let keys: Vec<&[u8]> = (0..run.len()).map(|i| run.key(i)).collect();
        let recs = records(ixs, &fid, &v.failures, &keys).await?;
        for i in 0..run.len() {
            let k = run.key(i);
            let Some(rec) = recs.get(k) else { continue };
            let held = rec.held.holding(conds)?;
            let back = run.rows(i).any(|row| {
                !crate::progress::owed(&held, &Holding::of(run.new_version(row), now, k))
            });
            if back {
                gone.insert(k.into(), rec.clone());
                continue;
            }
            // A key back on what it was tried on waits for its retry again:
            // a pass that left it while its input had changed did not cover it.
            let at_head = Holding::of(run.new_version(run.rows(i).start), now, k);
            if !crate::progress::owed(&rec.tried(k, conds)?, &at_head) {
                cover(&mut low, &rec.stored);
            }
        }
        match page.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    // The conditions moved on: a record is moot if the consumer holds what
    // they now take. Only a key held absent that they no longer take, or one
    // held under conditions that process as these do, can be.
    while moved_on {
        let q = Query {
            after: after.take(),
            limit: Some(65_536),
            ..Default::default()
        };
        let page = ixs
            .index(&fid)?
            .scan(&v.failures.layers, v.failures.head, &q)
            .await?;
        let run = &page.run;
        let mut maybe: Vec<(&[u8], Record, Holding)> = Vec::new();
        for i in 0..run.len() {
            let Version::Bytes(b) = run.new_version(run.rows(i).start) else {
                return invalid("a record that is not a version word");
            };
            let rec = Record::decode(b)?;
            let k = run.key(i);
            let held = rec.held.holding(conds)?;
            let check = match &held {
                Holding::Absent => !now.patterns.takes(k),
                Holding::Version(_, c) => c.same_processing(now),
                Holding::Held => false,
            };
            if check {
                maybe.push((k, rec, held));
            } else {
                cover(&mut low, &rec.stored);
            }
        }
        let keys: Vec<&[u8]> = maybe.iter().map(|m| m.0).collect();
        let mut at_head = vec![Ver::Absent; keys.len()];
        if head > 0 && !keys.is_empty() {
            let got = ix
                .get(&v.upstream.layers, head, &keys, &Query::default())
                .await?;
            let mut j = 0;
            for i in 0..got.run.len() {
                while keys[j] != got.run.key(i) {
                    j += 1;
                }
                at_head[j] = Ver::of(got.run.new_version(got.run.rows(i).start));
            }
        }
        for ((k, rec, held), ver) in maybe.into_iter().zip(at_head) {
            if !crate::progress::owed(&held, &Holding::of(ver.as_version(), now, k)) {
                gone.insert(k.into(), rec);
            } else {
                cover(&mut low, &rec.stored);
            }
        }
        match page.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    let mut counts = v.per_key.counts;
    let mut classes = v.per_key.classes;
    let mut refs = v.per_key.refs.clone();
    let before = refs.clone();
    let mut fch = Changes::new();
    let mut due = Vec::new();
    let mut points = Vec::new();
    for (k, rec) in &gone {
        tally(rec, k, conds, &mut counts, &mut classes, &mut refs, false)?;
        fch.push(k, Change::Removed)?;
        if rec.stored.timed() {
            due.push(due_key(rec.stored.next_at, k));
        }
        points.push((k.clone(), moot_point(rec, conds)?));
    }
    due.sort();
    let mut dch = Changes::new();
    for k in &due {
        dch.push(k, Change::Removed)?;
    }
    let spec = |path| Spec {
        version: 0,
        time_ms: ctx.time_ms,
        producer: ctx.producer,
        path,
        mode: Mode::Patch,
        ends_full_run: false,
        collect: 0,
    };
    let mut parts = Vec::new();
    if let Some(b) = build(
        ixs,
        &fid,
        &v.failures,
        spec(ctx.failures_path),
        &mut Source(Some(&fch)),
    )
    .await?
    {
        parts.push(Part::OutputCommit(b.part));
    }
    let did = output_id(DUE, &v.edge);
    if let Some(b) = build(
        ixs,
        &did,
        &v.due,
        spec(ctx.due_path),
        &mut Source(Some(&dch)),
    )
    .await?
    {
        parts.push(Part::OutputCommit(b.part));
    }
    parts.push(Part::Outcomes(Outcomes {
        edge: v.edge.clone(),
        counts,
        classes,
        conds: vec![],
        refs: moved(&before, &refs),
        step: None,
        at: v.failures.head,
        look: Some((
            Look {
                seen,
                cond: v.per_key.cond,
            },
            Look { seen: head, cond },
        )),
        low,
        life: v.per_key.life,
        made: None,
        points,
    }));
    Ok(Some(parts))
}

/// Lowers what a pass covers to what a record carries (§9's deploy_min).
fn cover(low: &mut Option<Counters>, s: &super::Stored) {
    *low = Some(low.map_or(s.low(), |l| l.least(&s.low())));
}

/// Reference counts that changed, with the count after (0: gone).
fn moved(before: &BTreeMap<u64, u64>, after: &BTreeMap<u64, u64>) -> Vec<(u64, u64)> {
    after
        .iter()
        .filter(|(id, n)| before.get(id) != Some(n))
        .map(|(id, n)| (*id, *n))
        .chain(
            before
                .keys()
                .filter(|id| !after.contains_key(id))
                .map(|id| (*id, 0)),
        )
        .collect()
}
