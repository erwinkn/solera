//! The cut (§13, D192, P11): the oldest commit of an output that anything
//! still reads history from. Merges keep every entry above it and fold the
//! rest into a base. It is the oldest of:
//!
//! - the commits of consumers' live pieces: a consumer catches up with one
//!   diff from its piece (CLN-11), and a batch planned at a head writes a
//!   piece at that head;
//! - B6's `seen` of a per-key edge with failing keys: its revert check reads
//!   the window after it;
//! - the cleanup cursor: cleanup reads the replaced versions above it;
//! - the commits retained runs' batch records name ([`record_pins`]):
//!   lineage diffs from a record's replaced pieces to its head;
//! - the heads live attempts planned at: a batch's commit reads what moved
//!   since its head, and its piece lands there (Fable #36 low 1: a batch
//!   whose input folded past its head is refused, see
//!   `BatchProgress::check`).
//!
//! The first three are the core's state; the engine passes the last two as
//! [`Pin`]s. Every pin only rises: a new attempt plans at the head, a new
//! record names a head and pieces that were pinned, a new piece is at an
//! attempt's head. So a cut taken on the event loop still holds when a merge
//! built from it off the loop applies, and [`super::Merged`]'s check refuses
//! it if the state's own pins say otherwise.

use std::collections::{BTreeMap, HashSet};

use solera_index::{pick_merge, ChangeIndex, LayerRef, Query};

use super::{head, Cleaned, Merged};
use crate::codec::Ver;
use crate::error::{invalid, Result};
use crate::outcomes::{output_id, FAILURES};
use crate::progress::{Key, Value};
use crate::record::Record;
use crate::state::{Edge, Id, Output, Tracking};

/// What holds an output's commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Holder {
    /// Nothing: the cut is the head.
    Head,
    /// The output's cleanup cursor.
    Cursor,
    /// A consumer's piece (its edge).
    Piece(Id),
    /// B6: a per-key edge with failing keys looks for reverts after it.
    Seen(Id),
    /// A retained run's batch records (the engine's name for the run).
    Run(Box<[u8]>),
    /// A live attempt planned at it (the engine's name for the attempt).
    Attempt(Box<[u8]>),
}

/// A commit of an output that something holds.
#[derive(Clone, Debug, PartialEq)]
pub struct Pin {
    pub output: Id,
    pub commit: u64,
    pub by: Holder,
}

/// An output's cut and what holds it there (the oldest pin; the first
/// found among equals).
#[derive(Clone, Debug, PartialEq)]
pub struct Cut {
    pub commit: u64,
    pub by: Holder,
}

#[cfg(test)]
thread_local! {
    /// Rules the model switches off, one at a time, to show each is needed.
    pub(crate) static OFF: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

pub(crate) const PIECES: u8 = 1;
pub(crate) const SEEN: u8 = 2;
pub(crate) const CURSOR: u8 = 4;

#[cfg(not(test))]
fn on(_: u8) -> bool {
    true
}

#[cfg(test)]
fn on(rule: u8) -> bool {
    OFF.with(|o| o.get() & rule == 0)
}

fn lowest(t: &Tracking, pins: &[Pin], want: impl Fn(&[u8]) -> bool) -> BTreeMap<Id, Cut> {
    let mut cuts: BTreeMap<Id, Cut> = t
        .outputs
        .iter()
        .filter(|(id, _)| want(id))
        .map(|(id, o)| {
            let cut = Cut {
                commit: o.head,
                by: Holder::Head,
            };
            (id.clone(), cut)
        })
        .collect();
    let mut hold = |output: &[u8], commit: u64, by: &dyn Fn() -> Holder| {
        if let Some(c) = cuts.get_mut(output).filter(|c| commit < c.commit) {
            *c = Cut { commit, by: by() };
        }
    };
    if on(CURSOR) {
        for (id, c) in &t.retain.cursors {
            hold(id, c.at, &|| Holder::Cursor);
        }
    }
    for (id, e) in t.edges.iter().filter(|(_, e)| want(&e.input)) {
        for (c, by) in edge_holds(t, id, e) {
            hold(&e.input, c, &|| by.clone());
        }
    }
    for p in pins {
        hold(&p.output, p.commit, &|| p.by.clone());
    }
    cuts
}

/// The commits of its input an edge holds: its live pieces' and, while
/// its per-key keys fail, B6's `seen`.
pub(crate) fn edge_holds(t: &Tracking, id: &Id, e: &Edge) -> Vec<(u64, Holder)> {
    let mut held = vec![];
    if let Some(k) = e.progress.keyed().filter(|_| on(PIECES)) {
        held.extend(k.commits().map(|c| (c, Holder::Piece(id.clone()))));
    }
    let failing = || {
        t.outputs
            .get(&output_id(FAILURES, id))
            .is_some_and(|o| o.live > 0)
    };
    if let Some(k) = t.per_key.get(id).filter(|_| on(SEEN) && failing()) {
        held.push((k.seen, Holder::Seen(id.clone())));
    }
    held
}

/// Every output's cut.
pub fn cuts(t: &Tracking, pins: &[Pin]) -> BTreeMap<Id, Cut> {
    lowest(t, pins, |_| true)
}

/// One output's cut (0 for an output with no commits).
pub fn cut(t: &Tracking, output: &[u8], pins: &[Pin]) -> Cut {
    lowest(t, pins, |o| o == output)
        .remove(output)
        .unwrap_or(Cut {
            commit: 0,
            by: Holder::Head,
        })
}

/// The commits one retained batch record holds: on its input, the oldest
/// of its head and its replaced pieces' commits; on the consumer's own
/// output `own`, the head its held pieces were read at. The engine keeps,
/// per retained run, the oldest per output, and lets them go with the run.
pub fn record_pins(rec: &Record, own: &[u8], run: &[u8]) -> Vec<Pin> {
    let low = rec
        .replaced
        .pieces
        .iter()
        .filter_map(|p| match p.value {
            Value::At(c, _) => Some(c),
            _ => None,
        })
        .fold(rec.head, u64::min);
    let mut pins = vec![Pin {
        output: rec.output.clone(),
        commit: low,
        by: Holder::Run(run.into()),
    }];
    if let Some(h) = rec.own_head {
        pins.push(Pin {
            output: own.into(),
            commit: h,
            by: Holder::Run(run.into()),
        });
    }
    pins
}

/// An upper bound, from the layer list alone, of the entries an output's
/// index holds above `cut`: every entry of a layer that reaches above it.
/// It also counts the entries at or below the cut in those layers (a
/// base's state included), so it never hides an alarm and may raise one
/// early by at most those.
pub fn above(o: &Output, cut: u64) -> u64 {
    o.layers
        .iter()
        .filter(|l| l.hi > cut)
        .map(|l| l.entries)
        .sum()
}

/// An output whose entries above its cut exceed a multiple of its live keys.
#[derive(Clone, Debug, PartialEq)]
pub struct Alarm {
    pub output: Id,
    pub above: u64,
    pub live: u64,
    pub cut: Cut,
}

/// The alarm (P11, review F5): every output whose entries above its cut
/// ([`above`]'s bound) pass `multiple` times its live keys, with what holds the cut. A consumer
/// holding it is released by resetting its progress to held (B3's
/// `Reset { held: true }`: it owes a full run and its pieces go); a run, by
/// letting its records go.
pub fn alarms(t: &Tracking, cuts: BTreeMap<Id, Cut>, multiple: u64) -> Vec<Alarm> {
    cuts.into_iter()
        .filter_map(|(output, cut)| {
            let o = t.outputs.get(&output)?;
            let above = above(o, cut.commit);
            (above > multiple.saturating_mul(o.live.max(1))).then_some(Alarm {
                output,
                above,
                live: o.live,
                cut,
            })
        })
        .collect()
}

/// The cleanup cursor's next step (§13, D183): up to the oldest head a live
/// attempt planned at on the output (its `Attempt` pins), or the head when
/// none is live. None if the output is not cleaned or nothing is released.
/// The engine deletes what [`released`] lists, then journals the step.
pub fn step(t: &Tracking, output: &[u8], pins: &[Pin]) -> Option<Cleaned> {
    let c = *t.retain.cursors.get(output)?;
    let to = pins
        .iter()
        .filter(|p| *p.output == *output && matches!(p.by, Holder::Attempt(_)))
        .map(|p| p.commit)
        .fold(head(t, output), u64::min);
    (to > c.at).then(|| Cleaned {
        output: output.into(),
        life: c.life,
        from: c.at,
        to,
    })
}

/// One page of what a step releases.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Released {
    /// Each version a commit of the step replaced, by key.
    pub versions: Vec<(Key, Ver)>,
    /// Where the next page starts, if any.
    pub next: Option<Vec<u8>>,
}

/// What a step releases, one page of at most `limit` keys after `after`:
/// every version the commits `from + 1..=to` replaced (at a clear, every
/// key present before it). `layers` are the output's now: its cursor, at
/// `from` until the step is journaled, keeps every entry above it. Versions
/// of a cleaned output are never written twice for a key (an upsert gets
/// its attempt's generation), so a released version is never live again: a
/// replaced version that is not a generation, which a later commit may
/// write again, is refused. So is a page of no keys, which would read as
/// nothing released.
pub async fn released(
    ix: &ChangeIndex,
    layers: &[LayerRef],
    s: &Cleaned,
    after: Option<Vec<u8>>,
    limit: u64,
) -> Result<Released> {
    if limit == 0 {
        return invalid("a page of released versions holds at least one key");
    }
    let q = Query {
        after,
        limit: Some(limit),
        ..Default::default()
    };
    let (mut versions, mut named) = (vec![], None);
    let next = ix
        .window_with(layers, s.from + 1..=s.to, &q, |run| {
            for i in 0..run.len() {
                for r in run.rows(i) {
                    match Ver::of(run.replaced_version(r)) {
                        Ver::Absent => {}
                        v @ Ver::Int(_) => versions.push((run.key(i).into(), v)),
                        Ver::Bytes(_) if named.is_none() => named = Some(run.key(i).to_vec()),
                        Ver::Bytes(_) => {}
                    }
                }
            }
        })
        .await?;
    if let Some(k) = named {
        return invalid(format!(
            "{}: a cleaned output replaced a version it named, not a generation, which may be live again",
            String::from_utf8_lossy(&k)
        ));
    }
    Ok(Released { versions, next })
}

/// The next merge SOLE-6's size rule asks for at `cut`, written at `path`:
/// the part to journal, or None when no merge is due. `cut` is [`cut`]'s,
/// taken with the engine's pins from the state `layers` were read from.
/// `path` is fresh (the engine's random token): refused, before anything
/// is written, if the list holds it. It reads and writes the whole run, so
/// the engine runs it off its event loop. Once the part applies, its
/// replaced files may go.
pub async fn merge(
    ix: &ChangeIndex,
    output: &[u8],
    layers: &[LayerRef],
    cut: u64,
    path: &str,
) -> Result<Option<Merged>> {
    if layers.iter().any(|l| l.path == path) {
        return invalid(format!("the layer path {path} is written twice"));
    }
    let Some(run) = pick_merge(layers, cut) else {
        return Ok(None);
    };
    let layer = ix.merge(layers, run.clone(), cut, path).await?;
    Ok(Some(Merged {
        output: output.into(),
        replaced: layers[run].iter().map(|l| l.path.clone()).collect(),
        layer,
    }))
}

/// The layer files that may be deleted (one call per sweep, over the
/// whole listing: it reads every output's list): those `listed` that no output's
/// layer list names (paths are the index store's, whatever output id holds
/// them: a rename moves layers to another id) and that no build in flight
/// (`building`: the paths of commits and merges started and neither
/// journaled nor given up) will. List first, then call this on the event
/// loop: a file listed whose build journaled since is in a list by now; one
/// whose event was refused, or whose engine died, is in neither. That
/// covers a write with no journal event, a merge's replaced files, and the
/// files of an output no longer in the state.
pub fn orphans(t: &Tracking, listed: &[String], building: &[String]) -> Vec<String> {
    let live: HashSet<&str> = t
        .outputs
        .values()
        .flat_map(|o| o.layers.iter().map(|l| l.path.as_str()))
        .chain(building.iter().map(String::as_str))
        .collect();
    listed
        .iter()
        .filter(|p| !live.contains(p.as_str()))
        .cloned()
        .collect()
}
