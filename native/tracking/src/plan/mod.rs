//! The planner (docs/tracking-design.md §7): from a run's cursor, the first
//! N owed keys in key order, with their classes, over every keyed input of
//! one consumer partition.
//!
//! Candidates come from the change index, piece by piece (§7's table):
//!
//! | Under the walk | Read |
//! |---|---|
//! | `empty` | `scan(H)` under the patterns now |
//! | `at(P)`, same conditions | `diff(P, H)` under the patterns |
//! | `at(P)`, other conditions | `diff(P, H)` merged with `scan(H)` under each pattern set |
//! | `held` | `scan(H)` merged with a scan of the consumer's own output |
//! | a point | `get(H)` of the point's key |
//!
//! Every read is paged and stops once the batch is full, so a plan costs what
//! its batch holds, not what the key space holds.

use std::collections::BTreeMap;
use std::sync::Arc;

use solera_index::{ChangeIndex, LayerRef, Pattern, Query, Run, Version};
use xxhash_rust::xxh3::xxh3_64;

use crate::codec::{Dec, Enc, Ver};
use crate::error::{format, invalid, Error, Result};
use crate::outcomes::commit::records;
use crate::outcomes::{output_id, record_digest, Conds, FAILURES};
use crate::progress::{
    owed, read_digest, Class, CondTable, Conditions, Holding, Key, KeyRange, Keyed, Log, Point,
    Progress, Value,
};
use crate::state::{Edge, Id, Output, Tracking};

pub const PLANNER_VERSION: u64 = 1;
pub const PLAN_FORMAT: u8 = 6;
const POINT_CHUNK: usize = 1024;
/// Owed keys whose records are read in one lookup, where the edge has any.
const RECORD_CHUNK: usize = 1024;
const MAX_PAGE: u64 = 1 << 20;

/// A life a plan names: never 0, since every output it reads is named.
fn alive(life: u64) -> Result<u64> {
    if life == 0 {
        return format("a plan names life 0");
    }
    Ok(life)
}

/// The change index of each output's object store.
pub trait Indexes: Sync {
    fn index(&self, output: &[u8]) -> Result<&ChangeIndex>;
}

/// One index for every output (tests and benchmarks).
pub struct OneIndex<'a>(pub &'a ChangeIndex);

impl Indexes for OneIndex<'_> {
    fn index(&self, _: &[u8]) -> Result<&ChangeIndex> {
        Ok(self.0)
    }
}

/// Owed keys the engine leaves out of a batch: B6 says which are not due.
/// A key held back keeps what it held, as a point.
pub trait Hold: Sync {
    fn held_back(&self, input: usize, key: &[u8]) -> bool;
}

pub struct NoHold;

impl Hold for NoHold {
    fn held_back(&self, _: usize, _: &[u8]) -> bool {
        false
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Mode {
    /// Owed keys, in key order.
    Incremental,
    /// Owed keys in their class, and every other present key as unchanged.
    All,
    /// Exactly these keys (sorted, distinct), those a consumer holds or
    /// upstream has under the patterns.
    Keys(Vec<Key>),
}

impl Mode {
    /// The mode as a batch record stores it.
    pub fn tag(&self) -> u8 {
        match self {
            Mode::Incremental => 0,
            Mode::All => 1,
            Mode::Keys(_) => 2,
        }
    }
}

#[derive(Clone, Debug)]
pub struct InputRequest {
    pub edge: Id,
    /// The conditions now.
    pub conditions: Arc<Conditions>,
    /// The consumer's own output, read where progress is `held`.
    pub own: Option<Id>,
}

#[derive(Clone, Debug)]
pub struct Request {
    pub mode: Mode,
    /// A full run's first batch: plan as if progress were empty; its
    /// commit resets progress.
    pub reset: bool,
    /// The run's cursor: plan keys after it.
    pub after: Option<Key>,
    /// The batch size: distinct keys over every keyed input.
    pub limit: usize,
    pub inputs: Vec<InputRequest>,
    /// The logs it reads.
    pub logs: Vec<LogRequest>,
}

/// One log a batch reads.
#[derive(Clone, Debug)]
pub struct LogRequest {
    pub edge: Id,
    /// The batch size: commits of the log.
    pub limit: u64,
}

/// One key of a batch.
#[derive(Clone, Debug, PartialEq)]
pub struct Cand {
    pub key: Key,
    pub class: Class,
    /// The version to hold: the version at the head if the patterns take
    /// the key, else absent (a removal).
    pub version: Ver,
    /// What the consumer held.
    pub held: Holding,
}

/// An output a plan read at `head` in life `life` (never 0).
#[derive(Clone, Debug, PartialEq)]
pub struct Own {
    pub output: Id,
    pub head: u64,
    pub life: u64,
}

/// An owed key left out of the batch, inside its range.
#[derive(Clone, Debug, PartialEq)]
pub struct HeldBack {
    pub key: Key,
    pub held: Holding,
    pub version: Ver,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InputPlan {
    pub edge: Id,
    pub output: Id,
    pub head: u64,
    /// The input's life ([crate::Output::life]) at plan, never 0: the commit
    /// refuses a plan of another life.
    pub life: u64,
    pub conditions: Arc<Conditions>,
    /// The consumer's own output as the plan read it, where it read it.
    pub own: Option<Own>,
    pub keys: Vec<Cand>,
    pub held_back: Vec<HeldBack>,
    /// Keys in the range whose failure is moot: the consumer holds what
    /// upstream has now, so the commit drops their records (B6).
    pub moot: Vec<Key>,
    /// Failing keys in the range the plan met and left to their retry: tried
    /// at what upstream has now (B6).
    pub waiting: Vec<Key>,
    /// The digest of the progress this plan read.
    pub expect: u64,
    /// The digest of the outcome records it read for its keys, held-back
    /// and moot keys (B6): the commit checks they did not move.
    pub records: u64,
}

impl InputPlan {
    /// Keys by class: added, updated, removed, unchanged.
    pub fn counts(&self) -> [u64; 4] {
        let mut n = [0; 4];
        for c in &self.keys {
            n[c.class as usize - 1] += 1;
        }
        n
    }

    /// The keys the commit reads the records of: taken, held back, moot.
    pub fn touched(&self) -> impl Iterator<Item = &Key> {
        self.keys
            .iter()
            .map(|c| &c.key)
            .chain(self.held_back.iter().map(|h| &h.key))
            .chain(&self.moot)
            .chain(&self.waiting)
    }

    /// A digest of the batch's input: lineage recomputes it (§7).
    pub fn digest(&self) -> u64 {
        digest(&self.keys)
    }
}

/// A digest of a batch's keys, classes and versions.
pub fn digest(keys: &[Cand]) -> u64 {
    let mut e = Enc::new();
    for c in keys {
        e.bytes(&c.key);
        e.u8(c.class as u8);
        e.version(c.version.as_version());
    }
    xxh3_64(&e.buf)
}

/// What a batch reads of one log: its commits `from + 1..=to`.
#[derive(Clone, Debug, PartialEq)]
pub struct LogPlan {
    pub edge: Id,
    pub output: Id,
    pub head: u64,
    /// The input's life at plan, never 0: the commit refuses another.
    pub life: u64,
    /// What the edge had consumed through at plan.
    pub through: u64,
    pub from: u64,
    pub to: u64,
    /// Whether the walk from the log's start goes on after this batch.
    pub walking: bool,
}

impl LogPlan {
    /// Whether the batch reads a commit.
    pub fn owed(&self) -> bool {
        self.to > self.from
    }

    /// Whether more commits follow the batch.
    pub fn more(&self) -> bool {
        self.head > self.to
    }

    /// The batch of `limit` commits after what `held` consumed, on a log
    /// with these layers. A batch that starts over (`reset`) reads from the
    /// log's start; so does one whose edge consumed nothing. A log that
    /// started over past what the edge read is refused: its consumer starts
    /// over, which is the caller's to ask for.
    fn of(req: &LogRequest, e: &Edge, o: &Output, held: Log, reset: bool) -> Result<LogPlan> {
        if req.limit == 0 {
            return invalid("a batch takes at least one commit of a log");
        }
        if !reset && held.restarted(&o.layers) {
            return Err(Error::Unsettled);
        }
        let through = if reset { 0 } else { held.through };
        let from = through.max(crate::log::start(&o.layers).saturating_sub(1));
        let to = o.head.min(from.saturating_add(req.limit)).max(from);
        Ok(LogPlan {
            edge: req.edge.clone(),
            output: e.input.clone(),
            head: o.head,
            life: o.life,
            through: held.through,
            from,
            to,
            walking: (reset || held.walking) && to < o.head,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    pub mode: Mode,
    pub reset: bool,
    pub range: KeyRange,
    pub inputs: Vec<InputPlan>,
    pub logs: Vec<LogPlan>,
    /// The retry pass a retry batch belongs to (B6).
    pub pass: Option<PassId>,
}

/// A retry pass: its number, which starts over with the edge's records, and
/// the life of the edge it runs in. A retry batch of a consumer with several
/// keyed inputs takes its keys on each of them, and is a step of the pass of
/// this edge only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PassId {
    pub edge: Id,
    pub life: u64,
    pub n: u64,
}

/// What a plan reads of the state, taken under a short lock.
pub struct View {
    inputs: Vec<InputView>,
    logs: Vec<LogPlan>,
}

struct InputView {
    edge: Id,
    output: Id,
    layers: Vec<LayerRef>,
    head: u64,
    life: u64,
    progress: Arc<Progress>,
    own: Option<(Id, Vec<LayerRef>, u64, u64)>,
    failures: Option<Failures>,
}

/// An edge's records of failing keys (B6), where it has any.
struct Failures {
    output: Id,
    out: Output,
    conds: Arc<Conds>,
    /// Records with a stored outcome, by the class a try would have.
    classes: [u64; 3],
}

impl View {
    /// The digest of an input's progress over a range, as a plan reads it.
    pub(crate) fn expect(&self, input: usize, range: &KeyRange) -> u64 {
        let keyed = self.inputs[input].progress.keyed().expect("a keyed view");
        read_digest(keyed, false, range)
    }

    /// B6: the first input's failure records of `keys` and the conditions
    /// they name, or None when the edge has no records.
    pub async fn failure_records(
        &self,
        ixs: &dyn Indexes,
        keys: &[&[u8]],
    ) -> Result<Option<(BTreeMap<Key, crate::outcomes::Record>, Arc<Conds>)>> {
        let Some(f) = self.inputs.first().and_then(|v| v.failures.as_ref()) else {
            return Ok(None);
        };
        Ok(Some((
            records(ixs, &f.output, &f.out, keys).await?,
            f.conds.clone(),
        )))
    }

    /// The own output of input `input` as this view read it, where it did.
    pub(crate) fn own(&self, input: usize) -> Option<Own> {
        self.inputs[input].own.as_ref().map(|o| Own {
            output: o.0.clone(),
            head: o.2,
            life: o.3,
        })
    }

    /// B6: whether the first input has failure records.
    pub fn has_failures(&self) -> bool {
        self.inputs.first().is_some_and(|v| v.failures.is_some())
    }
}

impl Tracking {
    pub fn view(&self, req: &Request) -> Result<View> {
        let output = |id: &[u8], what: &str| match self.outputs.get(id) {
            Some(o) => Ok((o.layers.clone(), o.head, o.life)),
            None => invalid(format!(
                "{what} {} has no life: declare it first",
                String::from_utf8_lossy(id)
            )),
        };
        let mut inputs = Vec::with_capacity(req.inputs.len());
        for i in &req.inputs {
            let Some(e) = self.edges.get(&i.edge) else {
                return invalid(format!("no edge {}", String::from_utf8_lossy(&i.edge)));
            };
            if e.progress.keyed().is_none() {
                return invalid(format!(
                    "edge {} is not keyed",
                    String::from_utf8_lossy(&i.edge)
                ));
            }
            let (layers, head, life) = output(&e.input, "the input")?;
            let own = match &i.own {
                Some(o) => {
                    let (l, h, life) = output(o, "the own output")?;
                    Some((o.clone(), l, h, life))
                }
                None => None,
            };
            let fid = output_id(FAILURES, &i.edge);
            let failures = self.outputs.get(&fid).filter(|o| o.live > 0).map(|o| {
                let k = self.per_key.get(&i.edge).cloned().unwrap_or_default();
                Failures {
                    output: fid.clone(),
                    out: o.clone(),
                    conds: k.conds,
                    classes: k.classes,
                }
            });
            inputs.push(InputView {
                edge: i.edge.clone(),
                output: e.input.clone(),
                layers,
                head,
                life,
                progress: e.progress.clone(),
                own,
                failures,
            });
        }
        let mut logs = Vec::with_capacity(req.logs.len());
        for l in &req.logs {
            let Some(e) = self.edges.get(&l.edge) else {
                return invalid(format!("no edge {}", String::from_utf8_lossy(&l.edge)));
            };
            let Progress::Log(held) = *e.progress else {
                return invalid(format!(
                    "edge {} is keyed",
                    String::from_utf8_lossy(&l.edge)
                ));
            };
            let Some(o) = self.outputs.get(&e.input) else {
                return invalid(format!(
                    "the input {} has no life: declare it first",
                    String::from_utf8_lossy(&e.input)
                ));
            };
            logs.push(LogPlan::of(l, e, o, held, req.reset)?);
        }
        Ok(View { inputs, logs })
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Role {
    Diff,
    Head,
    Own,
}

/// One paged read, consumed key by key.
struct Src<'a> {
    role: Role,
    ix: &'a ChangeIndex,
    layers: &'a [LayerRef],
    c1: u64,
    at: u64,
    q: Query,
    run: Run,
    pos: usize,
    more: bool,
    limit: u64,
}

impl<'a> Src<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        role: Role,
        ix: &'a ChangeIndex,
        layers: &'a [LayerRef],
        c1: u64,
        at: u64,
        r: &KeyRange,
        pattern: Option<Pattern>,
        limit: u64,
    ) -> Src<'a> {
        let q = Query {
            lo: r.lo.as_deref().map(<[u8]>::to_vec),
            hi: r.hi.as_deref().map(<[u8]>::to_vec),
            after: None,
            limit: None,
            pattern,
        };
        Src {
            role,
            ix,
            layers,
            c1,
            at,
            q,
            run: Run::new(),
            pos: 0,
            more: true,
            limit: limit.clamp(1, MAX_PAGE),
        }
    }

    async fn fill(&mut self) -> Result<()> {
        while self.pos == self.run.len() && self.more {
            let mut q = self.q.clone();
            q.limit = Some(self.limit);
            let page = match self.role {
                Role::Diff => self.ix.diff(self.layers, self.c1, self.at, &q).await?,
                Role::Head | Role::Own => self.ix.scan(self.layers, self.at, &q).await?,
            };
            self.run = page.run;
            self.pos = 0;
            match page.next {
                Some(n) => self.q.after = Some(n),
                None => self.more = false,
            }
            self.limit = (self.limit * 2).min(MAX_PAGE);
        }
        Ok(())
    }

    fn head(&self) -> Option<&[u8]> {
        (self.pos < self.run.len()).then(|| self.run.key(self.pos))
    }

    fn row(&self) -> usize {
        self.run.rows(self.pos).start
    }
}

/// A callback shown each owed key a walk counts: the key, what the consumer
/// held, and the version to hold.
pub type Each<'a> = &'a mut (dyn FnMut(&[u8], &Holding, &Ver) + Send);

/// What a walk found.
struct Found<'a> {
    keep: bool,
    /// Sees every owed key the walk counts, kept or not: the key, what the
    /// consumer held, and the version to hold.
    each: Option<Each<'a>>,
    limit: usize,
    keys: Vec<Cand>,
    held_back: Vec<HeldBack>,
    counts: [u64; 4],
    /// Owed keys not due, counted when `waiting`: failing keys waiting for
    /// their retry, and keys the hold keeps back.
    waits: [u64; 4],
    n: usize,
    /// Owed keys whose records are still to be read (B6).
    pend: Vec<Pend>,
    moot: Vec<Key>,
    /// Count owed keys that are not due in `waits`, without taking them.
    waiting: bool,
    /// Records with a stored outcome the walk met, by the class a try at
    /// what they were tried on would have.
    visited: [u64; 3],
    /// Failing keys met and left to their retry.
    skipped: Vec<Key>,
    /// Each record read, by key: its part of the plan's `records` digest.
    read: BTreeMap<Key, u64>,
}

struct Pend {
    key: Key,
    held: Holding,
    version: Ver,
    wanted: Holding,
}

impl Found<'_> {
    fn new(limit: usize, keep: bool) -> Self {
        Found {
            keep,
            each: None,
            limit,
            keys: vec![],
            held_back: vec![],
            counts: [0; 4],
            waits: [0; 4],
            n: 0,
            pend: vec![],
            moot: vec![],
            waiting: false,
            visited: [0; 3],
            skipped: vec![],
            read: BTreeMap::new(),
        }
    }

    fn full(&self) -> bool {
        self.n >= self.limit
    }
}

/// Walks one keyed input from a cursor, collecting owed keys.
struct Walker<'a> {
    ix: &'a ChangeIndex,
    layers: &'a [LayerRef],
    head: u64,
    now: &'a Arc<Conditions>,
    keyed: &'a Keyed,
    own: Option<(&'a ChangeIndex, &'a [LayerRef], u64)>,
    all: bool,
    input: usize,
    hold: &'a dyn Hold,
    fails: Option<(&'a dyn Indexes, &'a Failures)>,
}

impl Walker<'_> {
    async fn walk(&self, after: Option<&Key>, out: &mut Found<'_>) -> Result<()> {
        let r = KeyRange {
            lo: after.cloned(),
            hi: None,
        };
        for piece in self.keyed.pieces(&r) {
            self.piece(&piece.range, &piece.value, out).await?;
            if out.full() {
                break;
            }
        }
        Ok(())
    }

    async fn piece(&self, r: &KeyRange, value: &Value, out: &mut Found<'_>) -> Result<()> {
        let page = (out.limit - out.n).saturating_add(1) as u64;
        let now_pat = self.now.patterns.index_pattern();
        let mut srcs: Vec<Src<'_>> = Vec::new();
        let head_src = |pattern: Option<Pattern>| {
            Src::new(
                Role::Head,
                self.ix,
                self.layers,
                0,
                self.head,
                r,
                pattern,
                page,
            )
        };
        match value {
            Value::Empty => srcs.push(head_src(now_pat)),
            Value::Held => {
                let Some((oix, olayers, ohead)) = self.own else {
                    return invalid("progress is held: the plan needs the consumer's own output");
                };
                srcs.push(head_src(now_pat));
                srcs.push(Src::new(Role::Own, oix, olayers, 0, ohead, r, None, page));
            }
            Value::At(p, c) => {
                if *p > self.head {
                    return invalid(format!(
                        "progress at commit {p} is past the input's head {}",
                        self.head
                    ));
                }
                let same_pat = c.patterns == self.now.patterns;
                if c.same_processing(self.now) && same_pat {
                    if *p < self.head {
                        srcs.push(Src::new(
                            Role::Diff,
                            self.ix,
                            self.layers,
                            *p,
                            self.head,
                            r,
                            now_pat.clone(),
                            page,
                        ));
                    }
                    if self.all {
                        srcs.push(head_src(now_pat));
                    }
                } else {
                    if *p < self.head {
                        let pat = if same_pat { now_pat.clone() } else { None };
                        srcs.push(Src::new(
                            Role::Diff,
                            self.ix,
                            self.layers,
                            *p,
                            self.head,
                            r,
                            pat,
                            page,
                        ));
                    }
                    srcs.push(head_src(now_pat));
                    if !same_pat {
                        srcs.push(head_src(c.patterns.index_pattern()));
                    }
                }
            }
        }
        let points: Vec<(&Key, &Point)> = self.keyed.points_in(r).collect();
        let seen_holds = matches!(value, Value::At(_, c) if c.patterns == self.now.patterns);
        let mut next_point = 0;
        let mut chunk: Vec<Option<Ver>> = Vec::new();
        let (mut chunk_start, mut chunk_at) = (0, 0);
        loop {
            for s in &mut srcs {
                s.fill().await?;
            }
            if next_point == chunk_at && chunk_at < points.len() {
                let frontier = srcs
                    .iter()
                    .filter(|s| s.more)
                    .filter_map(|s| s.run.last_key())
                    .min();
                let end = (chunk_at + POINT_CHUNK).min(points.len());
                let end = match frontier {
                    Some(f) => {
                        chunk_at + points[chunk_at..end].partition_point(|(k, _)| k.as_ref() <= f)
                    }
                    None => end,
                };
                if end > chunk_at {
                    let span = &points[chunk_at..end];
                    let lookup = |p: &Point| !seen_holds || p.seen.is_none();
                    let keys: Vec<&[u8]> = span
                        .iter()
                        .filter(|(_, p)| lookup(p))
                        .map(|(k, _)| k.as_ref())
                        .collect();
                    let mut got = versions_at(self.ix, self.layers, self.head, &keys)
                        .await?
                        .into_iter();
                    chunk = span
                        .iter()
                        .map(|(_, p)| if lookup(p) { got.next() } else { None })
                        .collect();
                    (chunk_start, chunk_at) = (chunk_at, end);
                }
            }
            let pk: Option<&[u8]> = (next_point < chunk_at).then(|| points[next_point].0.as_ref());
            let sk: Option<&[u8]> = srcs.iter().filter_map(|s| s.head()).min();
            let key: Key = match (pk, sk) {
                (None, None) => {
                    self.flush(out).await?;
                    return Ok(());
                }
                (Some(a), Some(b)) => a.min(b).into(),
                (Some(a), None) => a.into(),
                (None, Some(b)) => b.into(),
            };
            let mut diff: Option<(Version<'_>, Version<'_>)> = None;
            let mut at_head: Option<Version<'_>> = None;
            let mut own = false;
            for s in &srcs {
                if s.head() != Some(&key[..]) {
                    continue;
                }
                let row = s.row();
                match s.role {
                    Role::Diff => {
                        diff = Some((s.run.replaced_version(row), s.run.new_version(row)))
                    }
                    Role::Head => at_head = Some(s.run.new_version(row)),
                    Role::Own => own = true,
                }
            }
            let point = pk == Some(&key[..]);
            let (held, version) = if point {
                let point = points[next_point].1;
                let ver = match (chunk[next_point - chunk_start].take(), diff, &point.seen) {
                    (Some(v), _, _) => v,
                    (None, Some((_, new)), _) => Ver::of(new),
                    (None, None, seen) => {
                        seen.clone().expect("a point read without lookup has seen")
                    }
                };
                let held = point.holding.clone();
                next_point += 1;
                (held, ver)
            } else {
                let (vp, vh) = match diff {
                    Some((rep, new)) => (rep, new),
                    None => {
                        let v = at_head.unwrap_or(Version::Absent);
                        (v, v)
                    }
                };
                let held = match value {
                    Value::Empty => Holding::Absent,
                    Value::Held if own => Holding::Held,
                    Value::Held => Holding::Absent,
                    Value::At(_, c) => Holding::of(vp, c, &key),
                };
                (held, Ver::of(vh))
            };
            for s in &mut srcs {
                if s.head() == Some(&key[..]) {
                    s.pos += 1;
                }
            }
            self.take(key, held, version, point, out);
            if out.pend.len() >= RECORD_CHUNK || out.n + out.pend.len() >= out.limit {
                self.flush(out).await?;
            }
            if out.full() {
                return Ok(());
            }
        }
    }

    /// A key a record may speak for waits for the records: an owed key (the
    /// record says what the consumer holds) and a point (a record beside a
    /// point that holds what is wanted is moot).
    fn take(&self, key: Key, held: Holding, version: Ver, point: bool, out: &mut Found<'_>) {
        let wanted = Holding::of(version.as_version(), self.now, &key);
        let version = if matches!(wanted, Holding::Absent) {
            Ver::Absent
        } else {
            version
        };
        if self.fails.is_some() && (self.all || point || owed(&held, &wanted)) {
            out.pend.push(Pend {
                key,
                held,
                version,
                wanted,
            });
            return;
        }
        self.decide(key, held, version, wanted, out);
    }

    /// B6: what the edge's records say of the owed keys pending. A record
    /// holds what the consumer really holds; a key whose failure is moot
    /// (it holds what upstream has now) is not owed, and its record goes
    /// at the commit; a failing key tried at what upstream has now waits
    /// for its retry: owed, not due.
    async fn flush(&self, out: &mut Found<'_>) -> Result<()> {
        let Some((ixs, f)) = self.fails else {
            return Ok(());
        };
        if out.pend.is_empty() {
            return Ok(());
        }
        let pend = std::mem::take(&mut out.pend);
        let keys: Vec<&[u8]> = pend.iter().map(|p| p.key.as_ref()).collect();
        let recs = records(ixs, &f.output, &f.out, &keys).await?;
        for p in pend {
            let Some(rec) = recs.get(&p.key) else {
                self.decide(p.key, p.held, p.version, p.wanted, out);
                continue;
            };
            if out.keep {
                out.read.insert(p.key.clone(), record_digest(&p.key, rec));
            }
            let held = rec.held.holding(&f.conds)?;
            let tried = rec.tried(&p.key, &f.conds)?;
            let c = Class::of(&held, &tried);
            if c != Class::Unchanged {
                out.visited[c as usize - 1] += 1;
            }
            if !owed(&held, &p.wanted) {
                if out.keep {
                    out.moot.push(p.key.clone());
                }
            } else if !owed(&tried, &p.wanted) {
                if out.waiting {
                    out.waits[Class::of(&held, &p.wanted) as usize - 1] += 1;
                }
                if out.keep {
                    out.skipped.push(p.key);
                }
                continue;
            }
            self.decide(p.key, held, p.version, p.wanted, out);
        }
        Ok(())
    }

    fn decide(&self, key: Key, held: Holding, version: Ver, wanted: Holding, out: &mut Found<'_>) {
        let class = if owed(&held, &wanted) {
            let class = Class::of(&held, &wanted);
            if self.hold.held_back(self.input, &key) {
                if out.waiting {
                    out.waits[class as usize - 1] += 1;
                }
                if out.keep {
                    out.held_back.push(HeldBack { key, held, version });
                }
                return;
            }
            class
        } else if self.all && !matches!(wanted, Holding::Absent) {
            Class::Unchanged
        } else {
            return;
        };
        out.n += 1;
        out.counts[class as usize - 1] += 1;
        if let Some(each) = out.each.as_mut() {
            each(&key, &held, &version);
        }
        if out.keep {
            out.keys.push(Cand {
                key,
                class,
                version,
                held,
            });
        }
    }
}

fn empty_keyed() -> &'static Keyed {
    static EMPTY: std::sync::OnceLock<Keyed> = std::sync::OnceLock::new();
    EMPTY.get_or_init(Keyed::default)
}

/// Plans one batch. Reads the view only: the state may move meanwhile, and
/// the commit checks that the progress it read did not.
pub async fn plan(view: &View, req: &Request, ixs: &dyn Indexes, hold: &dyn Hold) -> Result<Plan> {
    if req.limit == 0 {
        return invalid("a batch takes at least one key");
    }
    let Some(ahead) = req.limit.checked_add(1) else {
        return invalid("a batch's limit is too large");
    };
    if req.inputs.len() != view.inputs.len() {
        return invalid("the view is not the request's");
    }
    let mut inputs: Vec<InputPlan> = Vec::with_capacity(view.inputs.len());
    let mut reads: Vec<BTreeMap<Key, u64>> = Vec::with_capacity(view.inputs.len());
    let listed: Vec<Key> = match &req.mode {
        Mode::Keys(keys) => {
            if keys.windows(2).any(|w| w[0] >= w[1]) {
                return invalid("a key list is sorted and distinct");
            }
            keys.iter()
                .filter(|k| req.after.as_ref().is_none_or(|a| *k > a))
                .take(ahead)
                .cloned()
                .collect()
        }
        _ => vec![],
    };
    for (i, (v, r)) in view.inputs.iter().zip(&req.inputs).enumerate() {
        let real = v.progress.keyed().expect("a keyed view");
        let keyed = if req.reset { empty_keyed() } else { real };
        let ix = ixs.index(&v.output)?;
        let own = match &v.own {
            Some((o, layers, head, _)) => Some((ixs.index(o)?, layers.as_slice(), *head)),
            None => None,
        };
        let w = Walker {
            ix,
            layers: &v.layers,
            head: v.head,
            now: &r.conditions,
            keyed,
            own,
            all: req.mode == Mode::All,
            input: i,
            hold,
            fails: v.failures.as_ref().filter(|_| !req.reset).map(|f| (ixs, f)),
        };
        let mut found = Found::new(ahead, true);
        match &req.mode {
            Mode::Keys(_) => {
                let batch = &listed[..listed.len().min(req.limit)];
                (found.keys, found.read) = listed_keys(&w, batch).await?;
            }
            _ => w.walk(req.after.as_ref(), &mut found).await?,
        }
        inputs.push(InputPlan {
            edge: v.edge.clone(),
            output: v.output.clone(),
            head: v.head,
            life: v.life,
            conditions: r.conditions.clone(),
            own: view.own(i),
            keys: found.keys,
            held_back: found.held_back,
            moot: found.moot,
            waiting: found.skipped,
            expect: 0,
            records: 0,
        });
        reads.push(found.read);
    }
    let hi: Option<Key> = match &req.mode {
        Mode::Keys(_) => (listed.len() > req.limit).then(|| listed[req.limit - 1].clone()),
        _ => {
            let mut all: Vec<&Key> = inputs
                .iter()
                .flat_map(|p| p.keys.iter().map(|c| &c.key))
                .collect();
            all.sort_unstable();
            all.dedup();
            (all.len() > req.limit).then(|| all[req.limit - 1].clone())
        }
    };
    let range = KeyRange {
        lo: req.after.clone(),
        hi,
    };
    for ((p, v), read) in inputs.iter_mut().zip(&view.inputs).zip(&reads) {
        if let Some(hi) = &range.hi {
            p.keys.retain(|c| c.key <= *hi);
            p.held_back.retain(|c| c.key <= *hi);
            p.moot.retain(|k| k <= hi);
            p.waiting.retain(|k| k <= hi);
        }
        p.expect = read_digest(v.progress.keyed().expect("a keyed view"), req.reset, &range);
        let touched: std::collections::BTreeSet<&Key> = p.touched().collect();
        p.records = touched
            .into_iter()
            .filter_map(|k| read.get(k))
            .fold(0, |a, h| a.wrapping_add(*h));
    }
    Ok(Plan {
        mode: req.mode.clone(),
        reset: req.reset,
        range,
        inputs,
        logs: view.logs.clone(),
        pass: None,
    })
}

/// What a batch's lineage replays (§12): the progress its record says it
/// replaced, read the way `plan` read it.
pub(crate) struct Replay<'a> {
    pub ix: &'a ChangeIndex,
    pub layers: &'a [LayerRef],
    pub head: u64,
    pub conditions: &'a Arc<Conditions>,
    /// The replaced slice; empty for a full run's first batch.
    pub keyed: &'a Keyed,
    pub own: Option<(&'a ChangeIndex, &'a [LayerRef], u64)>,
    pub all: bool,
    /// The keys the batch left out: held back, or waiting for their retry.
    pub hold: &'a dyn Hold,
}

impl Replay<'_> {
    fn walker(&self) -> Walker<'_> {
        Walker {
            ix: self.ix,
            layers: self.layers,
            head: self.head,
            now: self.conditions,
            keyed: self.keyed,
            own: self.own,
            all: self.all,
            input: 0,
            hold: self.hold,
            fails: None,
        }
    }

    /// The keys a range batch planned over `range`, stopping past `limit`.
    pub async fn range(&self, range: &KeyRange, limit: usize) -> Result<Vec<Cand>> {
        let w = self.walker();
        let mut found = Found::new(limit.saturating_add(1), true);
        for piece in self.keyed.pieces(range) {
            w.piece(&piece.range, &piece.value, &mut found).await?;
            if found.full() {
                break;
            }
        }
        Ok(found.keys)
    }

    /// `keys` (sorted, distinct) as a key list plans them: each one the
    /// consumer held or upstream has under the conditions, in its class.
    pub async fn keys(&self, keys: &[Key]) -> Result<Vec<Cand>> {
        Ok(listed_keys(&self.walker(), keys).await?.0)
    }
}

/// The versions of `keys` (sorted, distinct) at commit `c`.
async fn versions_at(
    ix: &ChangeIndex,
    layers: &[LayerRef],
    c: u64,
    keys: &[&[u8]],
) -> Result<Vec<Ver>> {
    let mut out = vec![Ver::Absent; keys.len()];
    if c == 0 || keys.is_empty() {
        return Ok(out);
    }
    let page = ix.get(layers, c, keys, &Query::default()).await?;
    let mut j = 0;
    for i in 0..page.run.len() {
        while keys[j] < page.run.key(i) {
            j += 1;
        }
        out[j] = Ver::of(page.run.new_version(page.run.rows(i).start));
    }
    Ok(out)
}

/// A key list: each listed key the consumer holds or upstream has under the
/// patterns, in its class (`unchanged` when held as it is).
async fn listed_keys(w: &Walker<'_>, keys: &[Key]) -> Result<(Vec<Cand>, BTreeMap<Key, u64>)> {
    let all: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
    let at_head = versions_at(w.ix, w.layers, w.head, &all).await?;
    let mut held: Vec<Option<Holding>> = keys
        .iter()
        .map(|k| w.keyed.point(k).map(|p| p.holding.clone()))
        .collect();
    let mut read = BTreeMap::new();
    if let Some((ixs, f)) = w.fails {
        let recs = records(ixs, &f.output, &f.out, &all).await?;
        for (i, k) in keys.iter().enumerate() {
            if let Some(r) = recs.get(k) {
                held[i] = Some(r.held.holding(&f.conds)?);
                read.insert(k.clone(), record_digest(k, r));
            }
        }
    }
    let mut by_commit: std::collections::BTreeMap<u64, Vec<usize>> = Default::default();
    let mut own: Vec<usize> = vec![];
    for (i, k) in keys.iter().enumerate() {
        if held[i].is_some() {
            continue;
        }
        match w.keyed.value_of(k) {
            Value::Empty => held[i] = Some(Holding::Absent),
            Value::Held => own.push(i),
            Value::At(p, _) => by_commit.entry(*p).or_default().push(i),
        }
    }
    for (p, idx) in by_commit {
        let ks: Vec<&[u8]> = idx.iter().map(|&i| all[i]).collect();
        let vs = versions_at(w.ix, w.layers, p, &ks).await?;
        for (&i, v) in idx.iter().zip(vs) {
            let Value::At(_, c) = w.keyed.value_of(&keys[i]) else {
                unreachable!()
            };
            held[i] = Some(Holding::of(v.as_version(), c, &keys[i]));
        }
    }
    if !own.is_empty() {
        let Some((oix, olayers, ohead)) = w.own else {
            return invalid("progress is held: the plan needs the consumer's own output");
        };
        let ks: Vec<&[u8]> = own.iter().map(|&i| all[i]).collect();
        let vs = versions_at(oix, olayers, ohead, &ks).await?;
        for (&i, v) in own.iter().zip(vs) {
            held[i] = Some(if v.is_absent() {
                Holding::Absent
            } else {
                Holding::Held
            });
        }
    }
    let mut out = Vec::new();
    for ((k, h), v) in keys.iter().zip(held).zip(at_head) {
        let h = h.expect("every key resolved");
        let wanted = Holding::of(v.as_version(), w.now, k);
        if matches!((&h, &wanted), (Holding::Absent, Holding::Absent)) {
            continue;
        }
        let v = if matches!(wanted, Holding::Absent) {
            Ver::Absent
        } else {
            v
        };
        out.push(Cand {
            key: k.clone(),
            class: Class::of(&h, &wanted),
            version: v,
            held: h,
        });
    }
    Ok((out, read))
}

/// Owed keys of one edge after `after`, at most `limit`, and where the next page
/// starts: what staleness probes and the console pages through.
pub async fn owed_keys(
    view: &View,
    req: &InputRequest,
    ixs: &dyn Indexes,
    after: Option<&Key>,
    limit: usize,
) -> Result<(Vec<Cand>, Option<Key>)> {
    owed_keys_due(view, req, ixs, &NoHold, after, limit).await
}

/// `owed_keys` without the keys `hold` keeps back: the keys due now.
pub async fn owed_keys_due(
    view: &View,
    req: &InputRequest,
    ixs: &dyn Indexes,
    hold: &dyn Hold,
    after: Option<&Key>,
    limit: usize,
) -> Result<(Vec<Cand>, Option<Key>)> {
    if limit == 0 {
        return invalid("a page of owed keys holds at least one key");
    }
    let mut found = Found::new(limit.saturating_add(1), true);
    walker(view, req, ixs, hold)?
        .walk(after, &mut found)
        .await?;
    let mut keys = found.keys;
    let next = (keys.len() > limit).then(|| {
        keys.truncate(limit);
        keys[limit - 1].key.clone()
    });
    Ok((keys, next))
}

/// How many keys of one edge are owed, by class: added, updated, removed.
/// Failing keys waiting for their retry are owed (B6): those the walk did
/// not meet, because their input has not changed since they failed, are
/// counted from the edge's tally of records.
pub async fn owed_count(view: &View, req: &InputRequest, ixs: &dyn Indexes) -> Result<[u64; 4]> {
    let (due, waiting) = owed_count_each(view, req, ixs, &NoHold, &mut |_, _, _| {}).await?;
    Ok(std::array::from_fn(|i| due[i] + waiting[i]))
}

/// `owed_count` in two parts: the keys due now, which `owed_keys_due`
/// lists and `each` is shown, and the keys not due (failing keys waiting
/// for their retry, and keys `hold` keeps back).
pub async fn owed_count_each(
    view: &View,
    req: &InputRequest,
    ixs: &dyn Indexes,
    hold: &dyn Hold,
    each: Each<'_>,
) -> Result<([u64; 4], [u64; 4])> {
    let mut found = Found::new(usize::MAX, false);
    found.waiting = true;
    found.each = Some(each);
    let w = walker(view, req, ixs, hold)?;
    w.walk(None, &mut found).await?;
    if let Some((_, f)) = w.fails {
        for i in 0..3 {
            found.waits[i] += f.classes[i].saturating_sub(found.visited[i]);
        }
    }
    Ok((found.counts, found.waits))
}

fn walker<'a>(
    view: &'a View,
    req: &'a InputRequest,
    ixs: &'a dyn Indexes,
    hold: &'a dyn Hold,
) -> Result<Walker<'a>> {
    let Some(v) = view.inputs.first() else {
        return invalid("the view holds no keyed edge");
    };
    let own = match &v.own {
        Some((o, layers, head, _)) => Some((ixs.index(o)?, layers.as_slice(), *head)),
        None => None,
    };
    let keyed = v.progress.keyed().expect("a keyed view");
    Ok(Walker {
        ix: ixs.index(&v.output)?,
        layers: &v.layers,
        head: v.head,
        now: &req.conditions,
        keyed,
        own,
        all: false,
        input: 0,
        hold,
        fails: v.failures.as_ref().map(|f| (ixs, f)),
    })
}

impl Plan {
    pub fn encode(&self) -> Vec<u8> {
        let mut t = CondTable::default();
        let mut e = Enc::new();
        e.uv(PLANNER_VERSION);
        e.u8(self.mode.tag());
        if let Mode::Keys(keys) = &self.mode {
            e.uv(keys.len() as u64);
            for k in keys {
                e.bytes(k);
            }
        }
        e.u8(self.reset as u8);
        self.range.encode(&mut e);
        e.uv(self.inputs.len() as u64);
        for p in &self.inputs {
            e.bytes(&p.edge);
            e.bytes(&p.output);
            e.uv(p.head);
            e.uv(p.life);
            e.uv(t.add(&p.conditions) as u64);
            match &p.own {
                None => e.u8(0),
                Some(o) => {
                    e.u8(1);
                    e.bytes(&o.output);
                    e.uv(o.head);
                    e.uv(o.life);
                }
            }
            e.uv(p.expect);
            e.uv(p.records);
            e.uv(p.keys.len() as u64);
            for c in &p.keys {
                e.bytes(&c.key);
                e.u8(c.class as u8);
                e.version(c.version.as_version());
                c.held.encode(&mut e, &mut t);
            }
            e.uv(p.held_back.len() as u64);
            for h in &p.held_back {
                e.bytes(&h.key);
                e.version(h.version.as_version());
                h.held.encode(&mut e, &mut t);
            }
            for ks in [&p.moot, &p.waiting] {
                e.uv(ks.len() as u64);
                for k in ks {
                    e.bytes(k);
                }
            }
        }
        e.uv(self.logs.len() as u64);
        for l in &self.logs {
            e.bytes(&l.edge);
            e.bytes(&l.output);
            e.uv(l.head);
            e.uv(l.life);
            e.uv(l.through);
            e.uv(l.from);
            e.uv(l.to);
            e.u8(l.walking as u8);
        }
        e.opt_u64(self.pass.as_ref().map(|p| p.life));
        if let Some(p) = &self.pass {
            e.uv(p.n);
            e.bytes(&p.edge);
        }
        let mut out = Enc::new();
        out.u8(PLAN_FORMAT);
        t.encode(&mut out);
        out.buf.extend_from_slice(&e.buf);
        out.buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Plan> {
        let mut d = Dec::new(bytes);
        let f = d.u8()?;
        if f != PLAN_FORMAT {
            return format(format!("plan format {f}, expected {PLAN_FORMAT}"));
        }
        let conds = CondTable::decode(&mut d)?;
        let cond = |i: u64| {
            conds
                .get(i as usize)
                .cloned()
                .map_or_else(|| format(format!("conditions #{i}")), Ok)
        };
        let planner = d.uv()?;
        if planner != PLANNER_VERSION {
            return format(format!(
                "a plan of planner {planner}, this is {PLANNER_VERSION}"
            ));
        }
        let mode = match d.u8()? {
            0 => Mode::Incremental,
            1 => Mode::All,
            2 => Mode::Keys(
                (0..d.count()?)
                    .map(|_| d.bytes().map(Key::from))
                    .collect::<Result<_>>()?,
            ),
            t => return format(format!("mode {t}")),
        };
        let reset = match d.u8()? {
            0 => false,
            1 => true,
            b => return format(format!("bool {b}")),
        };
        let range = KeyRange::decode(&mut d)?;
        let mut inputs = Vec::new();
        for _ in 0..d.count()? {
            let edge = d.bytes()?.into();
            let output = d.bytes()?.into();
            let head = d.uv()?;
            let life = alive(d.uv()?)?;
            let conditions = cond(d.uv()?)?;
            let own = match d.u8()? {
                0 => None,
                1 => Some(Own {
                    output: d.bytes()?.into(),
                    head: d.uv()?,
                    life: alive(d.uv()?)?,
                }),
                t => return format(format!("own tag {t}")),
            };
            let expect = d.uv()?;
            let records = d.uv()?;
            let mut keys = Vec::new();
            for _ in 0..d.count()? {
                let key = d.bytes()?.into();
                let class = Class::from_u8(d.u8()?)?;
                let version = d.ver()?;
                let held = Holding::decode(&mut d, &conds)?;
                keys.push(Cand {
                    key,
                    class,
                    version,
                    held,
                });
            }
            let mut held_back = Vec::new();
            for _ in 0..d.count()? {
                let key = d.bytes()?.into();
                let version = d.ver()?;
                let held = Holding::decode(&mut d, &conds)?;
                held_back.push(HeldBack { key, held, version });
            }
            let moot = (0..d.count()?)
                .map(|_| d.bytes().map(Key::from))
                .collect::<Result<_>>()?;
            let waiting = (0..d.count()?)
                .map(|_| d.bytes().map(Key::from))
                .collect::<Result<_>>()?;
            inputs.push(InputPlan {
                edge,
                output,
                head,
                life,
                conditions,
                own,
                keys,
                held_back,
                moot,
                waiting,
                expect,
                records,
            });
        }
        let mut logs = Vec::new();
        for _ in 0..d.count()? {
            logs.push(LogPlan {
                edge: d.bytes()?.into(),
                output: d.bytes()?.into(),
                head: d.uv()?,
                life: alive(d.uv()?)?,
                through: d.uv()?,
                from: d.uv()?,
                to: d.uv()?,
                walking: d.u8()? != 0,
            });
        }
        let pass = match d.opt_u64()? {
            Some(life) => Some(PassId {
                life,
                n: d.uv()?,
                edge: d.bytes()?.into(),
            }),
            None => None,
        };
        d.end()?;
        Ok(Plan {
            mode,
            reset,
            range,
            inputs,
            logs,
            pass,
        })
    }
}
