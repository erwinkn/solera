//! Lineage (docs/tracking-design.md §12; HIS-3, 5, 7, 8): what a batch
//! received, recomputed from its record. The record holds the progress the
//! batch replaced over its range, so the planner's own walk over that slice,
//! at the batch's head and under its conditions, gives the planned keys back
//! with the reads the plan made: a lineage question costs what a plan costs.
//! The record's digest checks the list; the record's marks then say which
//! keys failed, were held back at commit, or were loaded at another version,
//! with the version loaded.
//!
//! An answer is exact or says why not. An output the batch read that has
//! since been retired is `Retired`, whatever its new life holds; a commit
//! the record names that the input's index no longer answers at is
//! `NotRetained`; a version that cleanup has released is marked deleted.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use solera_index::{LayerRef, Query};

use crate::codec::Ver;
use crate::error::{invalid, Result};
use crate::plan::{digest, Cand, Hold, Indexes, Mode, Replay, PLANNER_VERSION};
use crate::progress::{Class, Holding, Key, Keyed, Reason, Value};
use crate::record::Record;
use crate::state::{Id, Tracking};

/// The logs a lineage question reads, taken from the state under a short
/// lock: each output's life (0 when it has none) and layers.
pub struct Logs {
    input: (u64, Vec<LayerRef>),
    own: Option<(Id, u64, Vec<LayerRef>)>,
}

impl Tracking {
    /// What lineage reads for `rec`: its input's layers and, with `own` (the
    /// consumer's own output), that output's, which a replaced `held` piece
    /// reads.
    pub fn logs(&self, rec: &Record, own: Option<&[u8]>) -> Logs {
        let log = |id: &[u8]| {
            self.outputs
                .get(id)
                .map_or((0, vec![]), |o| (o.life, o.layers.clone()))
        };
        Logs {
            input: log(&rec.output),
            own: own.map(|o| {
                let (life, layers) = log(o);
                (o.into(), life, layers)
            }),
        }
    }
}

/// An answer, or the commit its history no longer reaches.
#[derive(Clone, Debug, PartialEq)]
pub enum Answer<T> {
    Found(T),
    /// `output`, which the batch read, has been retired since (B9): the
    /// life the record names is gone, and a later life's commits are other
    /// versions under the same numbers.
    Retired {
        output: Id,
    },
    /// The index of `output` no longer answers at `commit`, which the record
    /// names: retention cut its history there (§13).
    NotRetained {
        output: Id,
        commit: u64,
    },
}

/// One key a batch received.
#[derive(Clone, Debug, PartialEq)]
pub struct Received {
    pub key: Key,
    pub class: Class,
    /// What the consumer held before the batch.
    pub held: Holding,
    /// The version planned: the key's at the batch's head, absent for a removal.
    pub planned: Ver,
    /// The version loaded, where it was not the one planned (HIS-8).
    pub loaded: Option<Ver>,
    /// The batch's mark on the key, if any: it failed, it was held back at
    /// commit (`Unsettled`), or it was loaded at another version.
    pub reason: Option<Reason>,
    /// Cleanup has since deleted the version the batch read.
    pub deleted: bool,
}

/// The commits that wrote or removed one key, newest first, above the floor
/// of its output's index; below it, only the state at the floor is left.
#[derive(Clone, Debug, PartialEq)]
pub struct History {
    pub floor: u64,
    /// The key's version at the floor, when the index has folded its older
    /// changes into a base.
    pub at_floor: Option<Ver>,
    /// `(commit, version, replaced)`, newest first.
    pub changes: Vec<(u64, Ver, Ver)>,
}

/// The keys a batch left out: held back, or waiting for their retry.
struct Left(BTreeSet<Key>);

impl Left {
    fn of(rec: &Record) -> Left {
        Left(
            rec.marks
                .iter()
                .filter(|(_, m)| m.reason == Reason::HeldBack)
                .map(|(k, _)| k.clone())
                .chain(rec.waiting.iter().cloned())
                .collect(),
        )
    }
}

impl Hold for Left {
    fn held_back(&self, _: usize, key: &[u8]) -> bool {
        self.0.contains(key)
    }
}

/// The commits `layers` answer at: `floor..=head`.
fn span(layers: &[LayerRef]) -> (u64, u64) {
    match (layers.first(), layers.last()) {
        (Some(f), Some(l)) => (f.floor.unwrap_or(0), l.hi),
        _ => (0, 0),
    }
}

impl Logs {
    /// Why the logs no longer answer for a commit the record names, read
    /// before anything else: the commits are its head, each `At` piece of
    /// the slice it replaced (without a reset) and, for a held piece, the
    /// consumer's own head at plan. An output whose life is not the one the
    /// record names for it (0 names none: nothing of it was read) is
    /// `Retired`; otherwise the oldest commit below its floor is
    /// `NotRetained`. Every piece counts whatever the
    /// mode, though a key list reads only its keys' pieces: the cut keeps
    /// every commit a retained record names (§13), so the rule is the
    /// record's, not the plan's. A commit past an output's head is an error:
    /// that is not the index the batch read.
    fn gone<T>(&self, rec: &Record) -> Result<Option<Answer<T>>> {
        let pieces = if rec.reset {
            &[][..]
        } else {
            &rec.replaced.pieces[..]
        };
        let missing =
            |id: &Id, named: u64, (life, layers): (u64, &[LayerRef]), commits: Vec<u64>| {
                if named != life {
                    return Ok(Some(Answer::Retired { output: id.clone() }));
                }
                let (floor, head) = span(layers);
                if let Some(c) = commits.iter().find(|&&c| c > head) {
                    return invalid(format!(
                        "batch {}: its record names commit {c} of {}, whose index ends at {head}",
                        producer(rec),
                        String::from_utf8_lossy(id)
                    ));
                }
                Ok(commits
                    .into_iter()
                    .filter(|&c| c < floor)
                    .min()
                    .map(|commit| Answer::NotRetained {
                        output: id.clone(),
                        commit,
                    }))
            };
        let mut need = vec![rec.head];
        need.extend(pieces.iter().filter_map(|p| match p.value {
            Value::At(c, _) => Some(c),
            _ => None,
        }));
        if let Some(a) = missing(&rec.output, rec.life, (self.input.0, &self.input.1), need)? {
            return Ok(Some(a));
        }
        if !pieces.iter().any(|p| p.value == Value::Held) {
            return Ok(None);
        }
        let (Some((id, life, layers)), Some(own_head)) = (&self.own, rec.own_head) else {
            return invalid(format!(
                "batch {}: progress was held, so its lineage needs the consumer's own output",
                producer(rec)
            ));
        };
        missing(id, rec.own_life, (*life, layers), vec![own_head])
    }

    fn replay<'a>(
        &'a self,
        rec: &'a Record,
        ixs: &'a dyn Indexes,
        keyed: &'a Keyed,
        left: &'a Left,
    ) -> Result<Replay<'a>> {
        let own = match (&self.own, rec.own_head) {
            (Some((id, _, layers)), Some(h)) => Some((ixs.index(id)?, layers.as_slice(), h)),
            _ => None,
        };
        Ok(Replay {
            ix: ixs.index(&rec.output)?,
            layers: &self.input.1,
            head: rec.head,
            conditions: &rec.conditions,
            keyed,
            own,
            all: rec.mode == Mode::All.tag(),
            hold: left,
        })
    }

    /// Per key of `keys`, the versions cleanup has released since the
    /// batch's head: replaced by a commit after it and at or below `cleaned`
    /// (§13's cleanup cursor), and not written again by a later commit. Reads
    /// those keys only, from the batch's head to the output's.
    async fn released(
        &self,
        ixs: &dyn Indexes,
        rec: &Record,
        keys: &[&[u8]],
        cleaned: u64,
    ) -> Result<BTreeMap<Key, Vec<Ver>>> {
        let mut out: BTreeMap<Key, Vec<Ver>> = BTreeMap::new();
        let head = span(&self.input.1).1;
        if cleaned.min(head) <= rec.head || keys.is_empty() {
            return Ok(out);
        }
        ixs.index(&rec.output)?
            .window_keys_with(
                &self.input.1,
                rec.head + 1..=head,
                keys,
                &Query::default(),
                |g| {
                    let mut rows: Vec<(u64, Ver, Ver)> = g
                        .rows(0)
                        .map(|r| {
                            (
                                g.commit(r),
                                Ver::of(g.new_version(r)),
                                Ver::of(g.replaced_version(r)),
                            )
                        })
                        .collect();
                    rows.sort_unstable_by(|a, b| b.0.cmp(&a.0));
                    let mut written_later = HashSet::new();
                    let mut vs = vec![];
                    for (c, new, was) in rows {
                        if c <= cleaned
                            && was != new
                            && !was.is_absent()
                            && !written_later.contains(&was)
                        {
                            vs.push(was);
                        }
                        written_later.insert(new);
                    }
                    if !vs.is_empty() {
                        out.insert(g.key(0).into(), vs);
                    }
                },
            )
            .await?;
        Ok(out)
    }
}

fn producer(rec: &Record) -> String {
    String::from_utf8_lossy(&rec.producer).into_owned()
}

/// The replaced slice as the plan read it: nothing for a full run's first batch.
fn replaced(rec: &Record) -> Keyed {
    if rec.reset {
        return Keyed::default();
    }
    Keyed::of_slice(&rec.replaced)
}

fn check(rec: &Record) -> Result<()> {
    if rec.planner != PLANNER_VERSION {
        return invalid(format!(
            "batch {} was planned by planner {}, this is {PLANNER_VERSION}",
            producer(rec),
            rec.planner
        ));
    }
    Ok(())
}

/// What the batch received of a planned key: its mark, and whether the
/// version it read is gone.
fn received(rec: &Record, c: Cand, released: &BTreeMap<Key, Vec<Ver>>) -> Received {
    let mark = rec
        .marks
        .binary_search_by(|(k, _)| k.cmp(&c.key))
        .ok()
        .map(|i| &rec.marks[i].1);
    let loaded = mark.and_then(|m| m.loaded.clone());
    let read = loaded.as_ref().unwrap_or(&c.version);
    let deleted = !read.is_absent() && released.get(&c.key).is_some_and(|vs| vs.contains(read));
    Received {
        class: c.class,
        held: c.held,
        planned: c.version,
        loaded,
        reason: mark.map(|m| m.reason),
        deleted,
        key: c.key,
    }
}

/// The batch's planned keys, recomputed from its record and checked
/// against its digest. A digest that does not match is an error naming the
/// batch.
async fn planned(logs: &Logs, rec: &Record, ixs: &dyn Indexes) -> Result<Answer<Vec<Cand>>> {
    check(rec)?;
    if let Some(gone) = logs.gone(rec)? {
        return Ok(gone);
    }
    let keyed = replaced(rec);
    let left = Left::of(rec);
    let r = logs.replay(rec, ixs, &keyed, &left)?;
    let cands = match &rec.keys {
        Some(keys) => r.keys(keys).await?,
        None => {
            let n: u64 = rec.counts.iter().sum();
            r.range(&rec.range, n as usize).await?
        }
    };
    let got = digest(&cands);
    if got != rec.digest {
        return invalid(format!(
            "batch {} of edge {}: its input recomputed to {} keys with digest {got:#x}, its record says {} keys with {:#x}",
            producer(rec),
            String::from_utf8_lossy(&rec.edge),
            cands.len(),
            rec.counts.iter().sum::<u64>(),
            rec.digest
        ));
    }
    Ok(Answer::Found(cands))
}

/// What batch `rec` received (HIS-7), key by key: its planned keys
/// recomputed from its record and checked against its digest, then its
/// marks. `cleaned` is the input's cleanup cursor (0 before any
/// cleanup).
pub async fn input(
    logs: &Logs,
    rec: &Record,
    ixs: &dyn Indexes,
    cleaned: u64,
) -> Result<Answer<Vec<Received>>> {
    let cands = match planned(logs, rec, ixs).await? {
        Answer::Found(cands) => cands,
        Answer::NotRetained { output, commit } => {
            return Ok(Answer::NotRetained { output, commit })
        }
        Answer::Retired { output } => return Ok(Answer::Retired { output }),
    };
    let keys: Vec<&[u8]> = cands.iter().map(|c| &c.key[..]).collect();
    let released = logs.released(ixs, rec, &keys, cleaned).await?;
    Ok(Answer::Found(
        cands
            .into_iter()
            .map(|c| received(rec, c, &released))
            .collect(),
    ))
}

/// What batch `rec` received of `key` (the key trace, §12): None when the
/// batch did not take it. A key outside the batch's range or key list, or one
/// it left out, needs no read; otherwise the batch's planned keys are
/// recomputed and checked against its digest, as `input` does, at one
/// plan's cost, and only `key` is read for what cleanup released.
pub async fn received_key(
    logs: &Logs,
    rec: &Record,
    key: &[u8],
    ixs: &dyn Indexes,
    cleaned: u64,
) -> Result<Answer<Option<Received>>> {
    let listed = rec
        .keys
        .as_ref()
        .is_none_or(|ks| ks.binary_search_by(|k| k.as_ref().cmp(key)).is_ok());
    if !rec.range.contains(key) || !listed || Left::of(rec).0.contains(key) {
        return Ok(Answer::Found(None));
    }
    let cands = match planned(logs, rec, ixs).await? {
        Answer::Found(cands) => cands,
        Answer::NotRetained { output, commit } => {
            return Ok(Answer::NotRetained { output, commit })
        }
        Answer::Retired { output } => return Ok(Answer::Retired { output }),
    };
    let Some(c) = cands.into_iter().find(|c| *c.key == *key) else {
        return Ok(Answer::Found(None));
    };
    let released = logs.released(ixs, rec, &[key], cleaned).await?;
    Ok(Answer::Found(Some(received(rec, c, &released))))
}

/// Every retained change of `key` on an output (the key trace's first step):
/// the commits that wrote or removed it.
pub async fn history(
    ix: &solera_index::ChangeIndex,
    layers: &[LayerRef],
    key: &[u8],
) -> Result<History> {
    let floor = span(layers).0;
    let mut out = History {
        floor,
        at_floor: None,
        changes: vec![],
    };
    ix.window_keys_with(layers, 0..=u64::MAX, &[key], &Query::default(), |g| {
        for r in g.rows(0) {
            let c = g.commit(r);
            if c <= floor && floor > 0 {
                out.at_floor = Some(Ver::of(g.new_version(r)));
            } else {
                out.changes
                    .push((c, Ver::of(g.new_version(r)), Ver::of(g.replaced_version(r))));
            }
        }
    })
    .await?;
    Ok(out)
}

/// Flow metrics (§12) over one window of time, from the counts each batch
/// record stored at commit.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Flow {
    pub start_ms: u64,
    pub batches: u64,
    /// Full runs started: their first batches.
    pub resets: u64,
    /// Keys by class: added, updated, removed, unchanged.
    pub counts: [u64; 4],
}

/// Flow per window of `bucket_ms`, oldest first, from `(time_ms, record)`.
pub fn flow<'a>(
    records: impl IntoIterator<Item = (u64, &'a Record)>,
    bucket_ms: u64,
) -> Result<Vec<Flow>> {
    if bucket_ms == 0 {
        return invalid("a flow window lasts at least a millisecond");
    }
    let mut out: BTreeMap<u64, Flow> = BTreeMap::new();
    for (t, rec) in records {
        let start_ms = t - t % bucket_ms;
        let f = out.entry(start_ms).or_insert_with(|| Flow {
            start_ms,
            ..Default::default()
        });
        f.batches += 1;
        f.resets += rec.reset as u64;
        for (n, c) in f.counts.iter_mut().zip(rec.counts) {
            *n += c;
        }
    }
    Ok(out.into_values().collect())
}
