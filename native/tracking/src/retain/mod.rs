//! Retention and cleanup (docs/tracking-design.md §13, B9): the cut below
//! which an output's index entries fold into a base, the cleanup cursor that
//! releases the versions commits replaced, the alarm when the cut is pinned,
//! and which layer files may be deleted.
//!
//! The core reads what its state holds: consumers' pieces, B6's `seen`, the
//! cleanup cursors. The engine passes in what only it knows, as [`Pin`]s:
//! the commits its retained runs' batch records name ([`record_pins`]) and
//! the heads its live attempts planned at.
//!
//! - [`cut`]: the cut, the alarm, cleanup's steps and the versions they
//!   release, merges, orphan layer files.
//!
//! Parts (tags 0x50-0x5f): [`Merged`] installs a merge, [`Track`] starts an
//! output's cleanup cursor, [`Cleaned`] moves it, [`Retire`] drops an output.

pub mod cut;

use std::collections::BTreeMap;

use solera_index::LayerRef;

use crate::codec::{Dec, Enc};
use crate::error::{invalid, Result};
use crate::state::{Id, Tracking};

pub use cut::{
    alarms, cut, cuts, merge, orphans, record_pins, released, step, Alarm, Cut, Holder, Pin,
    Released,
};

pub const TAG_MERGED: u8 = 0x50;
pub const TAG_TRACK: u8 = 0x51;
pub const TAG_CLEANED: u8 = 0x52;
pub const TAG_RETIRE: u8 = 0x53;

/// The cleanup cursor of each output whose store names its objects by
/// version (a file store): every version a commit at or below it replaced
/// is deleted. An output without one is never cleaned and its cursor holds
/// nothing.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Retain {
    pub cursors: BTreeMap<Id, u64>,
}

impl Retain {
    pub fn encode(&self, e: &mut Enc) {
        e.uv(self.cursors.len() as u64);
        for (id, c) in &self.cursors {
            e.bytes(id);
            e.uv(*c);
        }
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Retain> {
        let mut cursors = BTreeMap::new();
        for _ in 0..d.count()? {
            let id: Id = d.bytes()?.into();
            cursors.insert(id, d.uv()?);
        }
        Ok(Retain { cursors })
    }
}

fn head(t: &Tracking, output: &[u8]) -> u64 {
    t.outputs.get(output).map_or(0, |o| o.head)
}

fn name(id: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(id)
}

/// A merge installed: the adjacent layers `replaced`, by path, give way to
/// `layer`. Refused if they are no longer adjacent in the list (another
/// merge took them), if it folds past what the state holds (see
/// [`cut::cut`]), or if its path is one the list already holds. Once it
/// applies, the replaced files may be deleted.
#[derive(Clone, Debug, PartialEq)]
pub struct Merged {
    pub output: Id,
    pub replaced: Vec<String>,
    pub layer: LayerRef,
}

impl Merged {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.output);
        e.uv(self.replaced.len() as u64);
        for p in &self.replaced {
            e.bytes(p.as_bytes());
        }
        e.layer(&self.layer);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Merged> {
        Ok(Merged {
            output: d.bytes()?.into(),
            replaced: (0..d.count()?).map(|_| d.string()).collect::<Result<_>>()?,
            layer: d.layer()?,
        })
    }

    /// Where the replaced layers start in the output's list.
    fn start(&self, t: &Tracking) -> Result<usize> {
        let Some(o) = t.outputs.get(&self.output) else {
            return invalid(format!(
                "a merge of {}, which has no layers",
                name(&self.output)
            ));
        };
        let start = o
            .layers
            .iter()
            .position(|l| Some(&l.path) == self.replaced.first());
        let run = start.and_then(|s| o.layers.get(s..s + self.replaced.len()));
        match (start, run) {
            (Some(s), Some(run)) if run.iter().zip(&self.replaced).all(|(l, p)| l.path == *p) => {
                Ok(s)
            }
            _ => invalid(format!(
                "the layers a merge of {} replaces are no longer adjacent in its list",
                name(&self.output)
            )),
        }
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        let start = self.start(t)?;
        let o = &t.outputs[&self.output];
        let run = &o.layers[start..start + self.replaced.len()];
        let l = &self.layer;
        l.check()?;
        if (l.lo, l.hi) != (run[0].lo, run[run.len() - 1].hi) {
            return invalid(format!(
                "a merged layer covers {}..={}, the layers it replaces {}..={}",
                l.lo,
                l.hi,
                run[0].lo,
                run[run.len() - 1].hi
            ));
        }
        let was = run[0].floor;
        match l.floor {
            Some(_) if start > 0 => return invalid("only a merge from the oldest layer folds"),
            None if was.is_some() => return invalid("a merge of the base keeps a base"),
            Some(f) if f > l.hi || f < was.unwrap_or(0) => {
                return invalid(format!("a merged base at {f} outside {was:?}..={}", l.hi))
            }
            Some(f) if f > was.unwrap_or(0) => {
                let c = cut(t, &self.output, &[]);
                if f > c.commit {
                    return invalid(format!(
                        "a merge of {} folds through commit {f}, past {}, which {:?} holds",
                        name(&self.output),
                        c.commit,
                        c.by
                    ));
                }
            }
            _ => {}
        }
        if o.layers.iter().any(|x| x.path == l.path) {
            return invalid(format!("the layer path {} is written twice", l.path));
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        let start = self.start(t).expect("checked");
        let o = t.outputs.get_mut(&self.output).expect("checked");
        o.layers
            .splice(start..start + self.replaced.len(), [self.layer]);
    }
}

/// An output's cleanup starts, at its head `at`: from here on, merges never
/// fold past its cursor. The engine journals it when it deploys an output
/// on a store that names objects by version, before its first commit or in
/// the same event.
#[derive(Clone, Debug, PartialEq)]
pub struct Track {
    pub output: Id,
    pub at: u64,
}

impl Track {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.output);
        e.uv(self.at);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Track> {
        Ok(Track {
            output: d.bytes()?.into(),
            at: d.uv()?,
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        if t.retain.cursors.contains_key(&self.output) {
            return invalid(format!("{} is already cleaned", name(&self.output)));
        }
        if self.at != head(t, &self.output) {
            return invalid(format!(
                "cleanup of {} starts at {}, its head is {}",
                name(&self.output),
                self.at,
                head(t, &self.output)
            ));
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        t.retain.cursors.insert(self.output, self.at);
    }
}

/// Cleanup deleted every version commits `from + 1..=to` replaced
/// ([`cut::released`]), and its cursor moves to `to`. Refused unless the
/// cursor is at `from`, so a step is never journaled twice.
#[derive(Clone, Debug, PartialEq)]
pub struct Cleaned {
    pub output: Id,
    pub from: u64,
    pub to: u64,
}

impl Cleaned {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.output);
        e.uv(self.from);
        e.uv(self.to);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Cleaned> {
        Ok(Cleaned {
            output: d.bytes()?.into(),
            from: d.uv()?,
            to: d.uv()?,
        })
    }

    pub fn check(&self, t: &Tracking) -> Result<()> {
        let at = t.retain.cursors.get(&self.output);
        if at != Some(&self.from) {
            return invalid(format!(
                "a cleanup step of {} from {}, its cursor is at {at:?}",
                name(&self.output),
                self.from
            ));
        }
        if self.to <= self.from || self.to > head(t, &self.output) {
            return invalid(format!(
                "a cleanup step of {} to {}, outside {}..={}",
                name(&self.output),
                self.to,
                self.from + 1,
                head(t, &self.output)
            ));
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        t.retain.cursors.insert(self.output, self.to);
    }
}

/// An output is gone, with its data (CLN-6, Q10: a removed output or
/// dynamic partition once its `cleanup_after` window has passed): its
/// layers and its cursor leave the state, and a later commit starts it
/// anew. The engine deletes its layer files and objects. Refused while an
/// edge reads it.
#[derive(Clone, Debug, PartialEq)]
pub struct Retire {
    pub output: Id,
}

impl Retire {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.output);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Retire> {
        Ok(Retire {
            output: d.bytes()?.into(),
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        if !t.outputs.contains_key(&self.output) && !t.retain.cursors.contains_key(&self.output) {
            return invalid(format!("no output {} to retire", name(&self.output)));
        }
        if let Some((e, _)) = t.edges.iter().find(|(_, e)| e.input == self.output) {
            return invalid(format!(
                "{} is still read by edge {}",
                name(&self.output),
                name(e)
            ));
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        t.outputs.remove(&self.output);
        t.retain.cursors.remove(&self.output);
    }
}

#[cfg(test)]
mod tests;
