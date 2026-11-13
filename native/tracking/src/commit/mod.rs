//! Output commits (docs/tracking-design.md §4): the keys a write changed
//! become the commit's entries, each with the version it replaced, in one
//! layer of the output's change index; the event part installs the layer.
//!
//! - [`changes`]: the changed keys, and the form a worker hands them over in.
//! - [`build`]: building and writing one commit, for an asset's batch, a
//!   source's listing or patch, or a load's report on a source.

pub mod build;
pub mod changes;

use solera_index::LayerRef;

use crate::codec::{Dec, Enc};
use crate::error::{format, invalid, Result};
use crate::state::{core_owned, Id, Tracking};

pub use build::{build, build_each, Built, Changed, Each, Mode, Spec};
pub use changes::{Change, Changes, Held, Reader, Source};

pub const TAG_OUTPUT_COMMIT: u8 = 0x20;
pub const TAG_FULL_RUN_DONE: u8 = 0x21;
pub const TAG_DECLARE: u8 = 0x22;

fn in_full_run(t: &Tracking, output: &Id) -> bool {
    t.outputs.get(output).is_some_and(|o| o.full_run.is_some())
}

/// One commit installed on one output: its layer appended, the head moved to
/// it, the live key count after it. A first commit names an output no one
/// named. A clear starts a full run (form (a)); `ends_full_run` marks its
/// last commit, which may be the clear itself.
#[derive(Clone, Debug, PartialEq)]
pub struct OutputCommit {
    pub output: Id,
    /// The output's life it was built on (its replaced versions and live
    /// count are that life's): it applies only in that life. 0 when the
    /// output had no life: a first commit, which the head check lets land
    /// only on an output with no commit, in whatever life a naming gave it
    /// since (an edge opening on it while the commit is built): a life with
    /// no commit holds what no life holds. So it may also land after a whole
    /// life began and ended (strict xfail, B5 PR 3, which declares every
    /// output before its first commit and then refuses 0 here).
    pub life: u64,
    pub layer: LayerRef,
    pub live: u64,
    pub ends_full_run: bool,
}

impl OutputCommit {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.output);
        e.uv(self.life);
        e.layer(&self.layer);
        e.uv(self.live);
        e.u8(self.ends_full_run.into());
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<OutputCommit> {
        Ok(OutputCommit {
            output: d.bytes()?.into(),
            life: d.uv()?,
            layer: d.layer()?,
            live: d.uv()?,
            ends_full_run: match d.u8()? {
                0 => false,
                1 => true,
                b => return format(format!("ends_full_run {b}")),
            },
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        if self.life != 0 {
            t.in_life(&self.output, self.life, "the commit was built")?;
        }
        let head = t.outputs.get(&self.output).map_or(0, |o| o.head);
        let l = &self.layer;
        if l.lo != head + 1 || l.hi != l.lo || l.floor.is_some() {
            return invalid(format!(
                "a commit's layer covers commit {}..={} on an output at head {head}: it was built at another head",
                l.lo, l.hi
            ));
        }
        if t.outputs
            .get(&self.output)
            .is_some_and(|o| o.layers.iter().any(|x| x.path == l.path))
        {
            return invalid(format!("the layer path {} is written twice", l.path));
        }
        if self.ends_full_run && l.clears.is_empty() && !in_full_run(t, &self.output) {
            return invalid("a commit ends a full run on an output that is not in one");
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        t.name(&self.output);
        let o = t.outputs.get_mut(&self.output).expect("named");
        if !self.layer.clears.is_empty() {
            o.full_run = Some(self.layer.hi);
        }
        if self.ends_full_run {
            o.full_run = None;
        }
        o.head = self.layer.hi;
        o.live = self.live;
        o.layers.push(self.layer);
    }
}

impl Tracking {
    /// The commit that installs on a log the layer its writer wrote for it
    /// ([`crate::log::write`]): one commit, the head's next, that clears or
    /// not as the layer says.
    pub fn log_commit(
        &self,
        output: &[u8],
        layer: LayerRef,
        ends_full_run: bool,
    ) -> Result<OutputCommit> {
        let Some(o) = self.outputs.get(output) else {
            return invalid(format!(
                "{} has no life: declare it first",
                String::from_utf8_lossy(output)
            ));
        };
        if layer.clears.iter().any(|c| *c != layer.lo) {
            return invalid("a commit's layer clears at its own commit only");
        }
        let before = if layer.clears.is_empty() { o.live } else { 0 };
        let commit = OutputCommit {
            output: output.into(),
            life: o.life,
            live: before + layer.keys,
            layer,
            ends_full_run,
        };
        commit.check(self)?;
        Ok(commit)
    }
}

/// A full run ends with no commit of its own: its last batch changed
/// nothing.
#[derive(Clone, Debug, PartialEq)]
pub struct FullRunDone {
    pub output: Id,
}

impl FullRunDone {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.output);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<FullRunDone> {
        Ok(FullRunDone {
            output: d.bytes()?.into(),
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        if !in_full_run(t, &self.output) {
            return invalid(format!(
                "{} is not in a full run",
                String::from_utf8_lossy(&self.output)
            ));
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        if let Some(o) = t.outputs.get_mut(&self.output) {
            o.full_run = None;
        }
    }
}

/// An output is declared: the engine names it when it deploys it, so it has
/// a life before anything reads or writes it (a consumer's own output before
/// its first plan; the engine's facts about it before its first commit). It
/// keeps the life it has.
#[derive(Clone, Debug, PartialEq)]
pub struct Declare {
    pub output: Id,
}

impl Declare {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.output);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Declare> {
        Ok(Declare {
            output: d.bytes()?.into(),
        })
    }

    pub(crate) fn check(&self, _: &Tracking) -> Result<()> {
        if core_owned(&self.output) {
            return invalid("a per-key edge's record outputs are the core's, never declared");
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        t.name(&self.output);
    }
}
