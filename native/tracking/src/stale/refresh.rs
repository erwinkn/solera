//! Keeping what is known current: [`refresh`] at the upstream's head, from
//! the index, which also names the next key of a retry pass under way (rule
//! 5, B6); [`Notice`] at one commit, from the entries the commit builder
//! produced.

use std::sync::Arc;

use solera_index::{LayerRef, Query, Version};

use crate::codec::Ver;
use crate::due::RetryView;
use crate::error::{invalid, Result};
use crate::event::Part;
use crate::outcomes::{output_id, Conds, Record, FAILURES};
use crate::plan::{plan, Cand, Hold, Indexes, InputRequest, Mode, Request, View};
use crate::progress::{owed, Class, Conditions, Holding, Key, Keyed, Progress, Value};
use crate::stale::{Changed, Input, Reason, State, Witness};
use crate::state::{Id, Tracking};

/// Keys owed but not due: failed keys waiting for their retry (B6's due
/// index; until it lands, the lists the engine passes).
/// Failure records read in one lookup when rule 1 settles its candidates.
const RECORD_CHUNK: usize = 1024;
/// Retry records a page of the retry plan reads past moot keys.
const RETRY_PAGE: usize = 256;

pub trait Due: Sync {
    fn not_due(&self, edge: &[u8], key: &[u8]) -> bool;
    /// Changes whenever the keys not due on `edge` change: a cached count's
    /// reasons hold for one revision.
    fn revision(&self, edge: &[u8]) -> u64;
}

pub struct NoneDue;

impl Due for NoneDue {
    fn not_due(&self, _: &[u8], _: &[u8]) -> bool {
        false
    }

    fn revision(&self, _: &[u8]) -> u64 {
        0
    }
}

pub(super) struct OneEdge<'a> {
    due: &'a dyn Due,
    edge: &'a [u8],
}

impl Hold for OneEdge<'_> {
    fn held_back(&self, _: usize, key: &[u8]) -> bool {
        self.due.not_due(self.edge, key)
    }
}

/// What a refresh reads of one edge, taken under a short lock.
pub struct Ask {
    pub edge: Id,
    pub output: Id,
    pub input: Input,
    /// The gate holds the edge: its upstream is incomplete (rule 8).
    pub held: bool,
    pub layers: Vec<LayerRef>,
    pub head: u64,
    pub progress: Arc<Progress>,
    pub req: InputRequest,
    pub view: View,
    /// B6: the retry pass under way on the edge, if any.
    pub retry: Option<RetryView>,
    /// B6: the head of the edge's failure records.
    pub failures_head: u64,
}

impl Ask {
    pub fn keyed(&self) -> &Keyed {
        self.progress.keyed().expect("a keyed edge")
    }

    pub(super) fn hold<'a>(&'a self, due: &'a dyn Due) -> OneEdge<'a> {
        OneEdge {
            due,
            edge: &self.edge,
        }
    }

    fn request(&self, mode: Mode, after: Option<Key>) -> Request {
        Request {
            mode,
            reset: false,
            after,
            limit: 1,
            inputs: vec![self.req.clone()],
            unkeyed: vec![],
        }
    }

    /// Rule 6, the probe: the first due key after `after`, by the planner
    /// with a limit of one key.
    async fn probe(&self, ixs: &dyn Indexes, due: &dyn Due, after: Option<Key>) -> Result<State> {
        let req = self.request(Mode::Incremental, after);
        let p = plan(&self.view, &req, ixs, &self.hold(due)).await?;
        Ok(match p.inputs[0].keys.first() {
            Some(c) => self.stale(c),
            None => State::Fresh,
        })
    }

    /// Whether `key` is due at the head: its state, if it is. A key list
    /// takes a failing key whatever its record says, so a key waiting for
    /// its retry is not due here.
    async fn recheck(&self, ixs: &dyn Indexes, due: &dyn Due, key: &Key) -> Result<Option<State>> {
        let req = self.request(Mode::Keys(vec![key.clone()]), None);
        let p = plan(&self.view, &req, ixs, &crate::plan::NoHold).await?;
        let Some(c) = p.inputs[0].keys.first() else {
            return Ok(None);
        };
        if c.class == Class::Unchanged || due.not_due(&self.edge, key) {
            return Ok(None);
        }
        if let Some((recs, conds)) = self.view.failure_records(ixs, &[key]).await? {
            if let Some(r) = recs.get(key) {
                if due_by_record(r, &conds, key, &c.version, &self.req.conditions)?.is_none() {
                    return Ok(None);
                }
            }
        }
        Ok(Some(self.stale(c)))
    }

    /// Of rule 1's candidates on an edge with failure records, in key order,
    /// the first still due once each key's record (B6) is read: a record
    /// says what the consumer holds, and a key tried at what upstream has
    /// now waits for its retry.
    async fn settle(
        &self,
        ixs: &dyn Indexes,
        cands: &[(Key, Ver, Reason)],
    ) -> Result<Option<State>> {
        let now = &self.req.conditions;
        for chunk in cands.chunks(RECORD_CHUNK) {
            let keys: Vec<&[u8]> = chunk.iter().map(|c| c.0.as_ref()).collect();
            let Some((recs, conds)) = self.view.failure_records(ixs, &keys).await? else {
                return Ok(chunk.first().map(|c| stale_at(&c.0, c.2)));
            };
            for (key, version, reason) in chunk {
                let Some(r) = recs.get(key) else {
                    return Ok(Some(stale_at(key, *reason)));
                };
                if let Some(held) = due_by_record(r, &conds, key, version, now)? {
                    let reason = reason_of(self.keyed(), now, key, &held);
                    return Ok(Some(stale_at(key, reason)));
                }
            }
        }
        Ok(None)
    }

    fn stale(&self, c: &Cand) -> State {
        State::Stale {
            key: c.key.clone(),
            reason: reason_of(self.keyed(), &self.req.conditions, &c.key, &c.held),
            since_ms: 0,
        }
    }

    /// Rule 5: the next key of the retry pass under way (B6's retry plan),
    /// if one is due. A key gone moot fills the plan's limit without being
    /// taken, so past one the plan reads on in pages of `RETRY_PAGE` from
    /// where the last stopped, until a key is taken or the pass is read.
    pub(super) async fn next_retry(&self, ixs: &dyn Indexes) -> Result<Option<State>> {
        let Some(r) = &self.retry else {
            return Ok(None);
        };
        let now = &self.req.conditions;
        let mut p = crate::due::plan(ixs, r, now, 1).await?;
        let mut from: Option<RetryView> = None;
        loop {
            if let Some(c) = p.inputs[0].keys.first() {
                return Ok(Some(stale_at(&c.key, Reason::RetryDue)));
            }
            let Some(hi) = p.range.hi.take() else {
                return Ok(None);
            };
            let v = from.get_or_insert_with(|| RetryView {
                outcomes: r.outcomes.clone(),
                pass_list: r.pass_list.clone(),
                input: r.input.clone(),
                layers: r.layers.clone(),
                head: r.head,
            });
            if let Some(pass) = v.outcomes.per_key.pass.as_mut() {
                pass.after = Some(hi);
            }
            p = crate::due::plan(ixs, v, now, RETRY_PAGE).await?;
        }
    }

    /// Rule 1 over every change upstream made after commit `x`, outside the
    /// ranges whose progress changed since: the first due key, if any.
    async fn changes_since(
        &self,
        ixs: &dyn Indexes,
        due: &dyn Due,
        x: u64,
        changed: &Changed,
    ) -> Result<Option<State>> {
        let ix = ixs.index(&self.output)?;
        let hold = self.hold(due);
        let records = self.view.has_failures();
        let mut q = Query {
            pattern: self.req.conditions.patterns.index_pattern(),
            ..Default::default()
        };
        let mut limit = 16;
        loop {
            q.limit = Some(limit);
            let page = ix.diff(&self.layers, x, self.head, &q).await?;
            let mut cands = vec![];
            for i in 0..page.run.len() {
                let key = page.run.key(i);
                if changed.contains(key) {
                    continue;
                }
                let new = page.run.new_version(page.run.rows(i).start);
                match rule1(self.keyed(), &self.req.conditions, key, new, Some(&hold)) {
                    Step::Skip => {}
                    Step::Due(reason) if !records => return Ok(Some(stale_at(key, reason))),
                    Step::Due(reason) => cands.push((key.into(), Ver::of(new), reason)),
                    Step::Unsure => unreachable!("rule 1 with a hold decides"),
                }
            }
            if let Some(s) = self.settle(ixs, &cands).await? {
                return Ok(Some(s));
            }
            match page.next {
                Some(n) => q.after = Some(n),
                None => return Ok(None),
            }
            limit = (limit * 4).min(1 << 20);
        }
    }
}

impl Tracking {
    /// What refreshing `edge` reads, under the caller's short lock.
    pub fn stale_ask(&self, edge: &[u8]) -> Result<Ask> {
        let Some(input) = self.stale.inputs.get(edge) else {
            return invalid(format!("no keyed edge {}", String::from_utf8_lossy(edge)));
        };
        let Some(w) = &input.watch else {
            return invalid(format!(
                "edge {} has no conditions: journal a watch first",
                String::from_utf8_lossy(edge)
            ));
        };
        let e = &self.edges[edge];
        let req = InputRequest {
            edge: edge.into(),
            conditions: self.watched(edge, w),
            own: w.own.clone(),
        };
        let view = self.view(&Request {
            mode: Mode::Incremental,
            reset: false,
            after: None,
            limit: 1,
            inputs: vec![req.clone()],
            unkeyed: vec![],
        })?;
        let (layers, head) = self
            .outputs
            .get(&e.input)
            .map_or((vec![], 0), |o| (o.layers.clone(), o.head));
        let pass = self.per_key.get(edge).is_some_and(|k| k.pass.is_some());
        let retry = if pass {
            Some(self.retry_view(edge)?)
        } else {
            None
        };
        let failures_head = self
            .outputs
            .get(&output_id(FAILURES, edge))
            .map_or(0, |o| o.head);
        Ok(Ask {
            edge: edge.into(),
            output: e.input.clone(),
            input: input.clone(),
            held: self.held(input, &e.input),
            layers,
            head,
            progress: e.progress.clone(),
            req,
            view,
            retry,
            failures_head,
        })
    }
}

/// Brings one edge current at its upstream's head: what to journal, or None
/// if it already is or the gate holds it.
///
/// - Stale with witness `w`: `w` is rechecked at the head; if it is no longer
///   due, a probe looks for another from the first key.
/// - Fresh as of commit `x`, with some ranges changed since: rule 1 over the
///   upstream's changes after `x` outside those ranges (outside them the edge
///   still holds what upstream had at `x`, so a change to a key the patterns
///   take and no point covers is due with no lookup), then a probe from the
///   first changed range.
/// - Otherwise (a reset, new conditions, nothing known): a probe from the first key.
///
/// With a retry pass under way (B6), an edge found fresh is stale with the
/// pass's next key, "retry due"; a witness no longer due gives way to it
/// before any probe.
pub async fn refresh(
    ask: &Ask,
    ixs: &dyn Indexes,
    due: &dyn Due,
    time_ms: u64,
) -> Result<Option<Witness>> {
    let i = &ask.input;
    if ask.held || i.current().is_some_and(|k| k.x == ask.head) {
        return Ok(None);
    }
    let floor = ask.layers.first().and_then(|l| l.floor).unwrap_or(0);
    let state = match (&i.known, &i.changed) {
        (
            Some(super::Known {
                state: State::Stale { key, .. },
                ..
            }),
            _,
        ) => match ask.recheck(ixs, due, key).await? {
            Some(s) => s,
            None => match ask.next_retry(ixs).await? {
                Some(s) => s,
                None => ask.probe(ixs, due, None).await?,
            },
        },
        (Some(k), Changed::Ranges(rs)) if k.x >= floor => {
            match ask.changes_since(ixs, due, k.x, &i.changed).await? {
                Some(s) => s,
                None if rs.is_empty() => State::Fresh,
                None => {
                    let after = if rs.iter().any(|r| r.lo.is_none()) {
                        None
                    } else {
                        rs.iter().filter_map(|r| r.lo.clone()).min()
                    };
                    ask.probe(ixs, due, after).await?
                }
            }
        }
        _ => ask.probe(ixs, due, None).await?,
    };
    let state = match state {
        State::Fresh => ask.next_retry(ixs).await?.unwrap_or(State::Fresh),
        s => s,
    };
    Ok(Some(Witness {
        edge: ask.edge.clone(),
        gen: i.gen,
        x: ask.head,
        state: since(state, time_ms),
    }))
}

/// A state found now: stale since now, unless the edge was stale already
/// (the part keeps that moment when it applies).
fn since(state: State, time_ms: u64) -> State {
    match state {
        State::Stale { key, reason, .. } => State::Stale {
            key,
            reason,
            since_ms: time_ms,
        },
        s => s,
    }
}

/// Rules 1 and 2 at commit `commit` of `output`, over the commit's entries
/// as the commit builder writes them (key order), with no index read. Made
/// before the build, for each edge reading `output` whose state is current
/// as of the commit before; fed every entry; `finish` gives the parts for the
/// commit's event. Fresh, the first entry that matters makes the edge stale;
/// stale with a witness the commit does not touch, it stays so. An edge the
/// entries cannot decide (the commit touches its witness, or a key with a
/// point whose state differs) is left for a refresh, and so is every edge
/// when the commit clears (its entries do not show the keys it removed) or
/// `output` is mid full run, and an edge with failure records (B6: a
/// record, not a point, says what a failing key holds).
pub struct Notice {
    commit: u64,
    time_ms: u64,
    /// Fresh edges rule 1 has not decided yet.
    fresh: Vec<Noticed>,
    /// Stale edges whose witness no entry has reached yet, the smallest
    /// witness last: each entry is compared with that one only.
    stale: Vec<Noticed>,
    parts: Vec<Part>,
}

struct Noticed {
    edge: Id,
    gen: u64,
    progress: Arc<Progress>,
    now: Arc<Conditions>,
    state: State,
}

impl Noticed {
    fn witness(&self) -> &[u8] {
        match &self.state {
            State::Stale { key, .. } => key,
            State::Fresh => &[],
        }
    }

    fn part(self, commit: u64) -> Part {
        Part::Witness(Witness {
            edge: self.edge,
            gen: self.gen,
            x: commit,
            state: self.state,
        })
    }
}

impl Notice {
    pub fn new(t: &Tracking, output: &[u8], commit: u64, clears: bool, time_ms: u64) -> Notice {
        let mut n = Notice {
            commit,
            time_ms,
            fresh: vec![],
            stale: vec![],
            parts: vec![],
        };
        if clears || t.outputs.get(output).is_some_and(|o| o.full_run.is_some()) {
            return n;
        }
        for (id, e) in &t.edges {
            let Some(i) = t.stale.inputs.get(id) else {
                continue;
            };
            let (Some(w), Some(k)) = (&i.watch, i.current()) else {
                continue;
            };
            if *e.input != *output || e.progress.keyed().is_none() || k.x + 1 != commit {
                continue;
            }
            let failures = t.outputs.get(&output_id(FAILURES, id));
            if failures.is_some_and(|o| o.live > 0) {
                continue;
            }
            let noticed = Noticed {
                edge: id.clone(),
                gen: i.gen,
                progress: e.progress.clone(),
                now: t.watched(id, w),
                state: k.state.clone(),
            };
            match k.state {
                State::Fresh => n.fresh.push(noticed),
                State::Stale { .. } => n.stale.push(noticed),
            }
        }
        n.stale.sort_by(|a, b| b.witness().cmp(a.witness()));
        n
    }

    /// The next entry, in key order: `key` became `new`.
    pub fn push(&mut self, key: &[u8], new: Version<'_>) {
        while let Some(n) = self.stale.last() {
            match n.witness().cmp(key) {
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Equal => {
                    self.stale.pop();
                }
                std::cmp::Ordering::Less => {
                    let n = self.stale.pop().expect("a last");
                    self.parts.push(n.part(self.commit));
                }
            }
        }
        if self.fresh.is_empty() {
            return;
        }
        let (commit, time_ms) = (self.commit, self.time_ms);
        let parts = &mut self.parts;
        self.fresh.retain_mut(|n| {
            let keyed = n.progress.keyed().expect("a keyed edge");
            match rule1(keyed, &n.now, key, new, None) {
                Step::Skip => true,
                Step::Unsure => false,
                Step::Due(reason) => {
                    parts.push(Part::Witness(Witness {
                        edge: n.edge.clone(),
                        gen: n.gen,
                        x: commit,
                        state: State::Stale {
                            key: key.into(),
                            reason,
                            since_ms: time_ms,
                        },
                    }));
                    false
                }
            }
        });
    }

    /// The parts for the commit's event.
    pub fn finish(mut self) -> Vec<Part> {
        for n in self.fresh.into_iter().chain(self.stale) {
            self.parts.push(n.part(self.commit));
        }
        self.parts
    }
}

enum Step {
    Skip,
    Due(Reason),
    Unsure,
}

/// Rule 1 for one change: upstream moved `key` to `new`, and the edge held
/// what upstream had before. A key the patterns take with no point is due;
/// a key with a point is due if its state differs from the point and the
/// key is not waiting for a retry (unsure without `hold`).
fn rule1(
    keyed: &Keyed,
    now: &Arc<Conditions>,
    key: &[u8],
    new: Version<'_>,
    hold: Option<&dyn Hold>,
) -> Step {
    if let Some(p) = keyed.point(key) {
        let wanted = Holding::of(new, now, key);
        if !owed(&p.holding, &wanted) {
            return Step::Skip;
        }
        return match hold {
            None => Step::Unsure,
            Some(h) if h.held_back(0, key) => Step::Skip,
            Some(_) => Step::Due(reason_of(keyed, now, key, &p.holding)),
        };
    }
    if !now.patterns.takes(key) {
        return Step::Skip;
    }
    Step::Due(match keyed.value_of(key) {
        Value::Empty | Value::Held => Reason::NeverProcessed,
        Value::At(_, h) if !h.same_processing(now) || !h.patterns.takes(key) => {
            Reason::ConditionsChanged
        }
        Value::At(..) => Reason::InputChanged,
    })
}

fn stale_at(key: &[u8], reason: Reason) -> State {
    State::Stale {
        key: key.into(),
        reason,
        since_ms: 0,
    }
}

/// What a failing key's record (B6) says of it with upstream at `version`:
/// what the consumer holds if the key is owed and not waiting for its retry
/// (tried at `version` already), else None.
fn due_by_record(
    r: &Record,
    conds: &Conds,
    key: &[u8],
    version: &Ver,
    now: &Arc<Conditions>,
) -> Result<Option<Holding>> {
    let wanted = Holding::of(version.as_version(), now, key);
    let held = r.held.holding(conds)?;
    if !owed(&held, &wanted) || !owed(&r.tried(key, conds)?, &wanted) {
        return Ok(None);
    }
    Ok(Some(held))
}

/// Why a due key is due.
pub(super) fn reason_of(keyed: &Keyed, now: &Conditions, key: &[u8], held: &Holding) -> Reason {
    let point = keyed.point(key);
    match held {
        Holding::Held => Reason::NeverProcessed,
        Holding::Version(_, h) if !h.same_processing(now) || !now.patterns.takes(key) => {
            Reason::ConditionsChanged
        }
        Holding::Version(..) => Reason::InputChanged,
        Holding::Absent => match (point, keyed.value_of(key)) {
            (None, Value::Empty | Value::Held) => Reason::NeverProcessed,
            (None, Value::At(_, h)) if !h.same_processing(now) || !h.patterns.takes(key) => {
                Reason::ConditionsChanged
            }
            _ => Reason::InputChanged,
        },
    }
}
