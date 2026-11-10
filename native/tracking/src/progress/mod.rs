//! Progress: what a consumer holds of one input (docs/tracking-design.md §5).
//!
//! A keyed edge holds **pieces**, key ranges with one value that never
//! overlap and cover the key space, and **points**, single keys whose value
//! overrides their piece. An unkeyed edge holds "consumed through commit c".
//! Progress changes only by a batch commit ([`BatchProgress`]) or a reset
//! ([`Reset`]); nothing relabels it (no fold).

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;

use solera_index::{Glob, Pattern, Version};
use xxhash_rust::xxh3::xxh3_64;

use crate::codec::{Dec, Enc, Ver};
use crate::error::{format, invalid, Result};
use crate::state::{Edge, Id, Tracking};

mod ops;
pub use ops::{InputReport, Ops};

pub const TAG_EDGE_OPEN: u8 = 0x01;
pub const TAG_EDGE_CLOSE: u8 = 0x02;
pub const TAG_EDGE_RENAME: u8 = 0x03;
pub const TAG_RESET: u8 = 0x04;
pub const TAG_BATCH: u8 = 0x05;
pub const TAG_CONSUMED: u8 = 0x06;

/// One key pattern: a glob (`solera.patterns.glob_regex`'s meaning) or a regular
/// expression matched against the whole key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Pat {
    Glob(String),
    Regex(String),
}

/// One pattern, compiled. A `Regex` is in the Rust `regex` dialect, matched
/// against the whole key: matching is linear in the key, so lookaround and
/// backreferences are refused.
#[derive(Clone, Debug)]
pub enum Matcher {
    Glob(Glob),
    Regex(regex::bytes::Regex),
}

impl Matcher {
    pub fn new(p: &Pat) -> Result<Matcher> {
        Ok(match p {
            Pat::Glob(g) => match Glob::new(g) {
                Ok(g) => Matcher::Glob(g),
                Err(e) => return invalid(format!("pattern {g:?}: {e}")),
            },
            Pat::Regex(r) => match regex::bytes::Regex::new(&format!(r"(?s)\A(?:{r})\z")) {
                Ok(re) => Matcher::Regex(re),
                Err(e) => {
                    return invalid(format!(
                        "pattern Regex({r:?}) is not in Solera's regex dialect (Rust `regex`, \
                         matched against the whole key; lookaround and backreferences are not \
                         supported): {e}"
                    ))
                }
            },
        })
    }

    pub fn matches(&self, key: &[u8]) -> bool {
        match self {
            Matcher::Glob(g) => g.matches(key),
            Matcher::Regex(r) => r.is_match(key),
        }
    }
}

/// An input's patterns: a key is taken when it matches an include (any key,
/// without one) and no exclude.
#[derive(Clone, Debug)]
pub struct Patterns {
    include: Vec<Pat>,
    exclude: Vec<Pat>,
    inc: Vec<Matcher>,
    exc: Vec<Matcher>,
}

impl PartialEq for Patterns {
    fn eq(&self, o: &Patterns) -> bool {
        self.include == o.include && self.exclude == o.exclude
    }
}

impl Default for Patterns {
    fn default() -> Patterns {
        Patterns {
            include: vec![],
            exclude: vec![],
            inc: vec![],
            exc: vec![],
        }
    }
}

impl Patterns {
    pub fn new(include: Vec<Pat>, exclude: Vec<Pat>) -> Result<Patterns> {
        let inc = include.iter().map(Matcher::new).collect::<Result<_>>()?;
        let exc = exclude.iter().map(Matcher::new).collect::<Result<_>>()?;
        Ok(Patterns {
            include,
            exclude,
            inc,
            exc,
        })
    }

    pub fn include(&self) -> &[Pat] {
        &self.include
    }

    pub fn exclude(&self) -> &[Pat] {
        &self.exclude
    }

    #[inline]
    pub fn takes(&self, key: &[u8]) -> bool {
        (self.inc.is_empty() || self.inc.iter().any(|m| m.matches(key)))
            && !self.exc.iter().any(|m| m.matches(key))
    }

    /// What the index can filter by: every key these patterns take matches
    /// it (globs only; an include that is a regex leaves the read unfiltered).
    pub fn index_pattern(&self) -> Option<Pattern> {
        let globs = |ms: &[Matcher]| -> Vec<Glob> {
            ms.iter()
                .filter_map(|m| match m {
                    Matcher::Glob(g) => Some(g.clone()),
                    Matcher::Regex(_) => None,
                })
                .collect()
        };
        let include = if self.inc.iter().any(|m| matches!(m, Matcher::Regex(_))) {
            vec![]
        } else {
            globs(&self.inc)
        };
        let exclude = globs(&self.exc);
        if include.is_empty() && exclude.is_empty() {
            return None;
        }
        Some(Pattern { include, exclude })
    }

    /// Whether the index's filter is exactly these patterns.
    pub fn exact_in_index(&self) -> bool {
        !self
            .include
            .iter()
            .chain(&self.exclude)
            .any(|p| matches!(p, Pat::Regex(_)))
    }

    fn encode(&self, e: &mut Enc) {
        for list in [&self.include, &self.exclude] {
            e.uv(list.len() as u64);
            for p in list {
                match p {
                    Pat::Glob(g) => {
                        e.u8(0);
                        e.bytes(g.as_bytes());
                    }
                    Pat::Regex(r) => {
                        e.u8(1);
                        e.bytes(r.as_bytes());
                    }
                }
            }
        }
    }

    fn decode(d: &mut Dec<'_>) -> Result<Patterns> {
        let mut lists = [vec![], vec![]];
        for list in &mut lists {
            for _ in 0..d.count()? {
                list.push(match d.u8()? {
                    0 => Pat::Glob(d.string()?),
                    1 => Pat::Regex(d.string()?),
                    t => return format(format!("pattern tag {t}")),
                });
            }
        }
        let [include, exclude] = lists;
        Patterns::new(include, exclude).or_else(|e| format(e.to_string()))
    }
}

/// What a batch was processed under (§9). A difference from now makes keys
/// owed: patterns, the keys taken by exactly one of the two sets; context,
/// definition or the rebuild counter, every present key.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Conditions {
    pub patterns: Patterns,
    /// The heads of the consumer's whole and dep inputs, as the engine encodes them.
    pub context: Box<[u8]>,
    pub definition: Box<[u8]>,
    /// Raised by a full run asked for by hand (the amended P8).
    pub rebuild: u64,
}

impl Conditions {
    /// The same context, definition and rebuild counter: a version held
    /// under one is held under the other.
    #[inline]
    pub fn same_processing(&self, o: &Conditions) -> bool {
        self.rebuild == o.rebuild && self.context == o.context && self.definition == o.definition
    }

    pub fn encode(&self, e: &mut Enc) {
        self.patterns.encode(e);
        e.bytes(&self.context);
        e.bytes(&self.definition);
        e.uv(self.rebuild);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Conditions> {
        Ok(Conditions {
            patterns: Patterns::decode(d)?,
            context: d.bytes()?.into(),
            definition: d.bytes()?.into(),
            rebuild: d.uv()?,
        })
    }
}

/// Conditions referenced by index while encoding: each distinct one is
/// written once, before what refers to it.
#[derive(Default)]
pub struct CondTable {
    pub list: Vec<Arc<Conditions>>,
    at: HashMap<*const Conditions, usize>,
}

impl CondTable {
    pub fn add(&mut self, c: &Arc<Conditions>) -> usize {
        let n = self.list.len();
        *self.at.entry(Arc::as_ptr(c)).or_insert_with(|| {
            self.list.push(c.clone());
            n
        })
    }

    pub fn encode(&self, e: &mut Enc) {
        e.uv(self.list.len() as u64);
        for c in &self.list {
            c.encode(e);
        }
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Vec<Arc<Conditions>>> {
        (0..d.count()?)
            .map(|_| Conditions::decode(d).map(Arc::new))
            .collect()
    }
}

fn cond_at(conds: &[Arc<Conditions>], i: u64) -> Result<Arc<Conditions>> {
    match conds.get(i as usize) {
        Some(c) => Ok(c.clone()),
        None => format(format!("conditions #{i} of {}", conds.len())),
    }
}

/// What a consumer holds of one key.
#[derive(Clone, Debug, PartialEq)]
pub enum Holding {
    Absent,
    /// Whatever its own output holds, at no known upstream version.
    Held,
    Version(Ver, Arc<Conditions>),
}

impl Holding {
    /// What processing `v` under `c` leaves: the version if the patterns take
    /// the key, else nothing.
    #[inline]
    pub fn of(v: Version<'_>, c: &Arc<Conditions>, key: &[u8]) -> Holding {
        if v.is_absent() || !c.patterns.takes(key) {
            Holding::Absent
        } else {
            Holding::Version(Ver::of(v), c.clone())
        }
    }

    pub fn encode(&self, e: &mut Enc, t: &mut CondTable) {
        match self {
            Holding::Absent => e.u8(0),
            Holding::Held => e.u8(1),
            Holding::Version(v, c) => {
                e.u8(2);
                e.version(v.as_version());
                e.uv(t.add(c) as u64);
            }
        }
    }

    pub fn decode(d: &mut Dec<'_>, conds: &[Arc<Conditions>]) -> Result<Holding> {
        Ok(match d.u8()? {
            0 => Holding::Absent,
            1 => Holding::Held,
            2 => Holding::Version(d.ver()?, cond_at(conds, d.uv()?)?),
            t => return format(format!("holding tag {t}")),
        })
    }
}

/// Whether a consumer holding `h` owes work to hold `w` (§5's table).
#[inline]
pub fn owed(h: &Holding, w: &Holding) -> bool {
    match (h, w) {
        (Holding::Absent, Holding::Absent) => false,
        (Holding::Version(a, ca), Holding::Version(b, cb)) => {
            !(a == b && (Arc::ptr_eq(ca, cb) || ca.same_processing(cb)))
        }
        _ => true,
    }
}

/// A key's class in a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Class {
    Added = 1,
    Updated = 2,
    Removed = 3,
    Unchanged = 4,
}

impl Class {
    pub fn of(h: &Holding, w: &Holding) -> Class {
        if !owed(h, w) {
            Class::Unchanged
        } else if matches!(h, Holding::Absent) {
            Class::Added
        } else if matches!(w, Holding::Absent) {
            Class::Removed
        } else {
            Class::Updated
        }
    }

    pub fn from_u8(b: u8) -> Result<Class> {
        Ok(match b {
            1 => Class::Added,
            2 => Class::Updated,
            3 => Class::Removed,
            4 => Class::Unchanged,
            _ => return format(format!("class {b}")),
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Class::Added => "added",
            Class::Updated => "updated",
            Class::Removed => "removed",
            Class::Unchanged => "unchanged",
        }
    }
}

/// A piece's value.
#[derive(Clone, Debug)]
pub enum Value {
    /// The consumer holds nothing.
    Empty,
    /// The consumer holds whatever its own output holds, at no known version.
    Held,
    /// The consumer holds each key as upstream had it at this commit, if the
    /// conditions' patterns take it.
    At(u64, Arc<Conditions>),
}

impl PartialEq for Value {
    fn eq(&self, o: &Value) -> bool {
        match (self, o) {
            (Value::Empty, Value::Empty) | (Value::Held, Value::Held) => true,
            (Value::At(a, ca), Value::At(b, cb)) => a == b && (Arc::ptr_eq(ca, cb) || ca == cb),
            _ => false,
        }
    }
}

impl Value {
    pub fn encode(&self, e: &mut Enc, t: &mut CondTable) {
        match self {
            Value::Empty => e.u8(0),
            Value::Held => e.u8(1),
            Value::At(c, cond) => {
                e.u8(2);
                e.uv(*c);
                e.uv(t.add(cond) as u64);
            }
        }
    }

    pub fn decode(d: &mut Dec<'_>, conds: &[Arc<Conditions>]) -> Result<Value> {
        Ok(match d.u8()? {
            0 => Value::Empty,
            1 => Value::Held,
            2 => Value::At(d.uv()?, cond_at(conds, d.uv()?)?),
            t => return format(format!("value tag {t}")),
        })
    }
}

/// Why a point exists (lineage reads it, §12).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Reason {
    /// The key failed: the consumer keeps what it held before.
    Failed = 1,
    /// The key was owed but not due, and left out of the batch.
    HeldBack = 2,
    /// The key was loaded at another version than planned.
    Deviation = 3,
    /// The key was processed through an explicit key list.
    KeyList = 4,
}

impl Reason {
    fn from_u8(b: u8) -> Result<Reason> {
        Ok(match b {
            1 => Reason::Failed,
            2 => Reason::HeldBack,
            3 => Reason::Deviation,
            4 => Reason::KeyList,
            _ => return format(format!("reason {b}")),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Point {
    pub holding: Holding,
    pub reason: Reason,
    /// The version upstream had, under the point's piece's patterns, at the
    /// commit of the piece the point was written with (a batch writes its
    /// points with its piece): the planner then reads the key's version at
    /// the head from the piece's diff, with no lookup. `None` for a key
    /// list's points, which sit in an older piece.
    pub seen: Option<Ver>,
}

pub type Key = Box<[u8]>;

/// A key range `(lo, hi]`; `None` is unbounded.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyRange {
    pub lo: Option<Key>,
    pub hi: Option<Key>,
}

impl KeyRange {
    pub fn all() -> KeyRange {
        KeyRange::default()
    }

    #[inline]
    pub fn contains(&self, k: &[u8]) -> bool {
        self.lo.as_deref().is_none_or(|lo| lo < k) && self.hi.as_deref().is_none_or(|hi| k <= hi)
    }

    pub fn is_empty(&self) -> bool {
        matches!((&self.lo, &self.hi), (Some(lo), Some(hi)) if hi <= lo)
    }

    pub fn encode(&self, e: &mut Enc) {
        for b in [&self.lo, &self.hi] {
            match b {
                None => e.u8(0),
                Some(k) => {
                    e.u8(1);
                    e.bytes(k);
                }
            }
        }
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<KeyRange> {
        let mut one = || -> Result<Option<Key>> {
            match d.u8()? {
                0 => Ok(None),
                1 => Ok(Some(d.bytes()?.into())),
                t => format(format!("bound tag {t}")),
            }
        };
        let lo = one()?;
        let hi = one()?;
        let r = KeyRange { lo, hi };
        if r.is_empty() {
            return format("a range whose low end is not below its high end");
        }
        Ok(r)
    }

    fn bounds(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
        (
            self.lo.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
            self.hi.as_deref().map_or(Bound::Unbounded, Bound::Included),
        )
    }
}

/// Pieces and points of a keyed edge.
#[derive(Clone, Debug, PartialEq)]
pub struct Keyed {
    /// `bounds[i]` is the last key of piece `i`; the last piece runs to +∞.
    bounds: Vec<Key>,
    values: Vec<Value>,
    pub points: BTreeMap<Key, Point>,
}

impl Default for Keyed {
    fn default() -> Keyed {
        Keyed::of(Value::Empty)
    }
}

/// One piece, as a walk hands it out.
#[derive(Clone, Debug, PartialEq)]
pub struct Piece {
    pub range: KeyRange,
    pub value: Value,
}

/// The progress over a range: its pieces, clipped to the range, and its points.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Slice {
    pub pieces: Vec<Piece>,
    pub points: Vec<(Key, Point)>,
}

impl Slice {
    pub fn encode(&self, e: &mut Enc) {
        let mut t = CondTable::default();
        let mut body = Enc::new();
        body.uv(self.pieces.len() as u64);
        for p in &self.pieces {
            p.range.encode(&mut body);
            p.value.encode(&mut body, &mut t);
        }
        encode_points(&mut body, &mut t, self.points.iter().map(|(k, p)| (k, p)));
        t.encode(e);
        e.buf.extend_from_slice(&body.buf);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Slice> {
        let conds = CondTable::decode(d)?;
        let pieces = (0..d.count()?)
            .map(|_| {
                Ok(Piece {
                    range: KeyRange::decode(d)?,
                    value: Value::decode(d, &conds)?,
                })
            })
            .collect::<Result<_>>()?;
        let points = decode_points(d, &conds)?;
        Ok(Slice { pieces, points })
    }

    /// A digest of the slice: a commit refuses to apply over progress that
    /// moved since its batch was planned.
    pub fn digest(&self) -> u64 {
        let mut e = Enc::new();
        self.encode(&mut e);
        xxh3_64(&e.buf)
    }
}

fn encode_points<'a>(
    e: &mut Enc,
    t: &mut CondTable,
    points: impl ExactSizeIterator<Item = (&'a Key, &'a Point)>,
) {
    e.uv(points.len() as u64);
    for (k, p) in points {
        e.bytes(k);
        e.u8(p.reason as u8);
        p.holding.encode(e, t);
        match &p.seen {
            None => e.u8(0),
            Some(v) => {
                e.u8(1);
                e.version(v.as_version());
            }
        }
    }
}

fn decode_points(d: &mut Dec<'_>, conds: &[Arc<Conditions>]) -> Result<Vec<(Key, Point)>> {
    let mut out: Vec<(Key, Point)> = Vec::new();
    for _ in 0..d.count()? {
        let k: Key = d.bytes()?.into();
        let reason = Reason::from_u8(d.u8()?)?;
        let holding = Holding::decode(d, conds)?;
        let seen = match d.u8()? {
            0 => None,
            1 => Some(d.ver()?),
            t => return format(format!("seen tag {t}")),
        };
        if out.last().is_some_and(|(last, _)| *last >= k) {
            return format("points out of order");
        }
        out.push((
            k,
            Point {
                holding,
                reason,
                seen,
            },
        ));
    }
    Ok(out)
}

impl Keyed {
    pub fn of(v: Value) -> Keyed {
        Keyed {
            bounds: vec![],
            values: vec![v],
            points: BTreeMap::new(),
        }
    }

    pub fn piece_count(&self) -> usize {
        self.values.len()
    }

    /// The index of the piece holding `k`.
    #[inline]
    pub fn piece_of(&self, k: &[u8]) -> usize {
        self.bounds.partition_point(|b| b.as_ref() < k)
    }

    pub fn value_of(&self, k: &[u8]) -> &Value {
        &self.values[self.piece_of(k)]
    }

    fn piece_range(&self, i: usize) -> KeyRange {
        KeyRange {
            lo: i.checked_sub(1).map(|j| self.bounds[j].clone()),
            hi: self.bounds.get(i).cloned(),
        }
    }

    /// The pieces overlapping `r`, clipped to it, in key order.
    pub fn pieces(&self, r: &KeyRange) -> Vec<Piece> {
        if r.is_empty() {
            return vec![];
        }
        let first =
            r.lo.as_deref()
                .map_or(0, |lo| self.bounds.partition_point(|b| b.as_ref() <= lo));
        let mut out = Vec::new();
        for i in first..self.values.len() {
            let pr = self.piece_range(i);
            let lo = match (&pr.lo, &r.lo) {
                (Some(a), Some(b)) => Some(a.max(b).clone()),
                (a, b) => a.clone().or_else(|| b.clone()),
            };
            let last = match (&pr.hi, &r.hi) {
                (_, None) => false,
                (None, Some(_)) => true,
                (Some(phi), Some(hi)) => phi >= hi,
            };
            let hi = if last { r.hi.clone() } else { pr.hi };
            out.push(Piece {
                range: KeyRange { lo, hi },
                value: self.values[i].clone(),
            });
            if last {
                break;
            }
        }
        out
    }

    pub fn points_in<'a>(
        &'a self,
        r: &'a KeyRange,
    ) -> impl Iterator<Item = (&'a Key, &'a Point)> + 'a {
        (!r.is_empty())
            .then(|| self.points.range::<[u8], _>(r.bounds()))
            .into_iter()
            .flatten()
    }

    pub fn slice(&self, r: &KeyRange) -> Slice {
        Slice {
            pieces: self.pieces(r),
            points: self
                .points_in(r)
                .map(|(k, p)| (k.clone(), p.clone()))
                .collect(),
        }
    }

    /// What a key decodes to, when its piece is not `At` (no index read needed).
    pub fn point(&self, k: &[u8]) -> Option<&Point> {
        self.points.get(k)
    }

    /// Sets `(lo, hi]` to one piece of value `v` and drops the range's points;
    /// adjacent equal pieces merge.
    pub fn write(&mut self, r: &KeyRange, v: Value) {
        if r.is_empty() {
            return;
        }
        let n = self.values.len();
        let mut bounds: Vec<Key> = Vec::with_capacity(self.bounds.len() + 2);
        let mut values: Vec<Value> = Vec::with_capacity(n + 2);
        let push = |hi: Option<Key>, v: Value, bounds: &mut Vec<Key>, values: &mut Vec<Value>| {
            if values.last() == Some(&v) {
                if let Some(hi) = hi {
                    *bounds.last_mut().unwrap() = hi;
                } else {
                    bounds.pop();
                }
                return;
            }
            if let Some(hi) = hi {
                bounds.push(hi);
            }
            values.push(v);
        };
        let mut placed = false;
        for i in 0..n {
            let pr = self.piece_range(i);
            let below_lo = match (&pr.hi, &r.lo) {
                (Some(phi), Some(lo)) => phi <= lo,
                (None, _) | (_, None) => false,
            };
            if below_lo {
                push(pr.hi, self.values[i].clone(), &mut bounds, &mut values);
                continue;
            }
            if !placed {
                let starts_before = match (&pr.lo, &r.lo) {
                    (_, None) => false,
                    (None, Some(_)) => true,
                    (Some(plo), Some(lo)) => plo < lo,
                };
                if starts_before {
                    push(
                        r.lo.clone(),
                        self.values[i].clone(),
                        &mut bounds,
                        &mut values,
                    );
                }
                push(r.hi.clone(), v.clone(), &mut bounds, &mut values);
                placed = true;
            }
            let ends_after = match (&pr.hi, &r.hi) {
                (_, None) => false,
                (None, Some(_)) => true,
                (Some(phi), Some(hi)) => phi > hi,
            };
            if ends_after {
                push(pr.hi, self.values[i].clone(), &mut bounds, &mut values);
            }
        }
        self.bounds = bounds;
        self.values = values;
        let doomed: Vec<Key> = self.points_in(r).map(|(k, _)| k.clone()).collect();
        for k in doomed {
            self.points.remove(&k);
        }
    }

    fn encode(&self, e: &mut Enc) {
        let mut t = CondTable::default();
        let mut body = Enc::new();
        body.uv(self.values.len() as u64);
        for b in &self.bounds {
            body.bytes(b);
        }
        for v in &self.values {
            v.encode(&mut body, &mut t);
        }
        encode_points(&mut body, &mut t, self.points.iter());
        t.encode(e);
        e.buf.extend_from_slice(&body.buf);
    }

    fn decode(d: &mut Dec<'_>) -> Result<Keyed> {
        let conds = CondTable::decode(d)?;
        let n = d.count()?;
        if n == 0 {
            return format("keyed progress without a piece");
        }
        let bounds: Vec<Key> = (1..n)
            .map(|_| d.bytes().map(Key::from))
            .collect::<Result<_>>()?;
        if bounds.windows(2).any(|w| w[0] >= w[1]) {
            return format("piece bounds out of order");
        }
        let values = (0..n)
            .map(|_| Value::decode(d, &conds))
            .collect::<Result<_>>()?;
        let points = decode_points(d, &conds)?.into_iter().collect();
        Ok(Keyed {
            bounds,
            values,
            points,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Progress {
    Keyed(Keyed),
    /// Consumed through this commit of the input (0: nothing).
    Unkeyed(u64),
}

impl Progress {
    pub fn new(keyed: bool) -> Progress {
        if keyed {
            Progress::Keyed(Keyed::default())
        } else {
            Progress::Unkeyed(0)
        }
    }

    pub fn keyed(&self) -> Option<&Keyed> {
        match self {
            Progress::Keyed(k) => Some(k),
            Progress::Unkeyed(_) => None,
        }
    }

    pub fn encode(&self, e: &mut Enc) {
        match self {
            Progress::Keyed(k) => {
                e.u8(0);
                k.encode(e);
            }
            Progress::Unkeyed(c) => {
                e.u8(1);
                e.uv(*c);
            }
        }
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Progress> {
        match d.u8()? {
            0 => Ok(Progress::Keyed(Keyed::decode(d)?)),
            1 => Ok(Progress::Unkeyed(d.uv()?)),
            t => format(format!("progress tag {t}")),
        }
    }
}

fn bool_of(d: &mut Dec<'_>) -> Result<bool> {
    match d.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        b => format(format!("bool {b}")),
    }
}

fn edge<'a>(t: &'a Tracking, id: &[u8]) -> Result<&'a Edge> {
    match t.edges.get(id) {
        Some(e) => Ok(e),
        None => invalid(format!("no edge {}", String::from_utf8_lossy(id))),
    }
}

fn keyed_edge<'a>(t: &'a Tracking, id: &[u8]) -> Result<&'a Keyed> {
    match edge(t, id)?.progress.keyed() {
        Some(k) => Ok(k),
        None => invalid(format!("edge {} is not keyed", String::from_utf8_lossy(id))),
    }
}

fn keyed_mut<'a>(t: &'a mut Tracking, id: &[u8]) -> &'a mut Keyed {
    let e = t.edges.get_mut(id).expect("checked");
    match Arc::make_mut(&mut e.progress) {
        Progress::Keyed(k) => k,
        Progress::Unkeyed(_) => unreachable!("checked"),
    }
}

/// An edge starts, with empty progress.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeOpen {
    pub edge: Id,
    /// The output it reads.
    pub input: Id,
    pub keyed: bool,
}

impl EdgeOpen {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.edge);
        e.bytes(&self.input);
        e.u8(self.keyed as u8);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<EdgeOpen> {
        Ok(EdgeOpen {
            edge: d.bytes()?.into(),
            input: d.bytes()?.into(),
            keyed: bool_of(d)?,
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        if t.edges.contains_key(&self.edge) {
            return invalid("the edge is already open");
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        let progress = Arc::new(Progress::new(self.keyed));
        t.edges.insert(
            self.edge,
            Edge {
                input: self.input,
                progress,
            },
        );
    }
}

/// An edge ends: its input was removed, or its consumer.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeClose {
    pub edge: Id,
}

impl EdgeClose {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.edge);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<EdgeClose> {
        Ok(EdgeClose {
            edge: d.bytes()?.into(),
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        edge(t, &self.edge).map(|_| ())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        t.edges.remove(&self.edge);
    }
}

/// An edge is renamed, keeping its progress: its consumer or input was
/// renamed, or the output it reads (`input`).
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeRename {
    pub from: Id,
    pub to: Id,
    pub input: Id,
}

impl EdgeRename {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.from);
        e.bytes(&self.to);
        e.bytes(&self.input);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<EdgeRename> {
        Ok(EdgeRename {
            from: d.bytes()?.into(),
            to: d.bytes()?.into(),
            input: d.bytes()?.into(),
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        edge(t, &self.from)?;
        if self.from != self.to && t.edges.contains_key(&self.to) {
            return invalid("the new name is taken");
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        let mut e = t.edges.remove(&self.from).expect("checked");
        e.input = self.input;
        t.edges.insert(self.to, e);
    }
}

/// An edge's progress starts over: `empty` (nothing held), or `held`
/// for progress that cannot be trusted (an upstream deleted and declared
/// again, state brought in from elsewhere).
#[derive(Clone, Debug, PartialEq)]
pub struct Reset {
    pub edge: Id,
    pub held: bool,
}

impl Reset {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.edge);
        e.u8(self.held as u8);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Reset> {
        Ok(Reset {
            edge: d.bytes()?.into(),
            held: bool_of(d)?,
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        edge(t, &self.edge).map(|_| ())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        let e = t.edges.get_mut(&self.edge).expect("checked");
        let keyed = e.progress.keyed().is_some();
        e.progress = Arc::new(match (keyed, self.held) {
            (true, true) => Progress::Keyed(Keyed::of(Value::Held)),
            (true, false) => Progress::Keyed(Keyed::of(Value::Empty)),
            (false, _) => Progress::Unkeyed(0),
        });
    }
}

/// An unkeyed edge consumed its input through `through`.
#[derive(Clone, Debug, PartialEq)]
pub struct Consumed {
    pub edge: Id,
    pub through: u64,
}

impl Consumed {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.edge);
        e.uv(self.through);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Consumed> {
        Ok(Consumed {
            edge: d.bytes()?.into(),
            through: d.uv()?,
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        match edge(t, &self.edge)?.progress.as_ref() {
            Progress::Unkeyed(_) => Ok(()),
            Progress::Keyed(_) => invalid("a keyed edge consumes by batch"),
        }
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        let e = t.edges.get_mut(&self.edge).expect("checked");
        e.progress = Arc::new(Progress::Unkeyed(self.through));
    }
}

/// What a batch commit writes to one keyed edge (§5): optionally a reset to
/// empty first (a full run's first batch), then one piece over its range
/// (none for a key list, which writes points only), then its points.
#[derive(Clone, Debug, PartialEq)]
pub struct BatchProgress {
    pub edge: Id,
    pub reset: bool,
    /// The batch's range. Its points lie inside it.
    pub range: KeyRange,
    /// `At(head, the conditions it was planned under)`; `None` for a key list.
    pub piece: Option<Value>,
    /// Sorted by key.
    pub points: Vec<(Key, Point)>,
    /// The digest of the progress the plan read (the range's slice; with a
    /// reset, all of it): the commit applies only over that progress.
    pub expect: u64,
}

impl BatchProgress {
    pub fn encode(&self, e: &mut Enc) {
        let mut t = CondTable::default();
        let mut body = Enc::new();
        body.bytes(&self.edge);
        body.u8(self.reset as u8);
        self.range.encode(&mut body);
        match &self.piece {
            None => body.u8(0),
            Some(v) => {
                body.u8(1);
                v.encode(&mut body, &mut t);
            }
        }
        encode_points(&mut body, &mut t, self.points.iter().map(|(k, p)| (k, p)));
        body.uv(self.expect);
        t.encode(e);
        e.buf.extend_from_slice(&body.buf);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<BatchProgress> {
        let conds = CondTable::decode(d)?;
        let edge = d.bytes()?.into();
        let reset = bool_of(d)?;
        let range = KeyRange::decode(d)?;
        let piece = match d.u8()? {
            0 => None,
            1 => Some(Value::decode(d, &conds)?),
            t => return format(format!("piece tag {t}")),
        };
        let points = decode_points(d, &conds)?;
        let expect = d.uv()?;
        Ok(BatchProgress {
            edge,
            reset,
            range,
            piece,
            points,
            expect,
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        let k = keyed_edge(t, &self.edge)?;
        if self.points.iter().any(|(key, _)| !self.range.contains(key)) {
            return invalid("a point outside the batch's range");
        }
        if self.points.windows(2).any(|w| w[0].0 >= w[1].0) {
            return invalid("points out of order");
        }
        if read_digest(k, self.reset, &self.range) != self.expect {
            return invalid("the edge's progress moved since the batch was planned");
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        let k = keyed_mut(t, &self.edge);
        if self.reset {
            *k = Keyed::of(Value::Empty);
        }
        if let Some(v) = self.piece {
            let v = match v {
                Value::At(h, c) => {
                    let same = k.values.iter().find_map(|v| match v {
                        Value::At(_, old) if **old == *c => Some(old.clone()),
                        _ => None,
                    });
                    Value::At(h, same.unwrap_or(c))
                }
                v => v,
            };
            k.write(&self.range, v);
        }
        for (key, p) in self.points {
            k.points.insert(key, p);
        }
    }
}

/// The digest of what a plan reads of an edge's progress: its range's slice,
/// or with a reset all of it.
pub fn read_digest(k: &Keyed, reset: bool, range: &KeyRange) -> u64 {
    if reset {
        k.slice(&KeyRange::all()).digest()
    } else {
        k.slice(range).digest()
    }
}

#[cfg(test)]
mod tests;
