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
use crate::error::{invalid, Result};
use crate::state::{Id, Tracking};

pub use build::{build, build_each, Built, Changed, Each, Mode, Spec};
pub use changes::{Change, Changes, Held, Reader, Source};

pub const TAG_OUTPUT_COMMIT: u8 = 0x20;

/// One commit installed on one output: its layer appended, the head moved to
/// it, the live key count after it. An output's first commit creates it.
#[derive(Clone, Debug, PartialEq)]
pub struct OutputCommit {
    pub output: Id,
    pub layer: LayerRef,
    pub live: u64,
}

impl OutputCommit {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.output);
        e.layer(&self.layer);
        e.uv(self.live);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<OutputCommit> {
        Ok(OutputCommit {
            output: d.bytes()?.into(),
            layer: d.layer()?,
            live: d.uv()?,
        })
    }

    pub(crate) fn check(&self, t: &Tracking) -> Result<()> {
        let head = t.outputs.get(&self.output).map_or(0, |o| o.head);
        let l = &self.layer;
        if l.lo != head + 1 || l.hi != l.lo || l.floor.is_some() {
            return invalid(format!(
                "a commit's layer covers commit {}..={} on an output at head {head}: it was built at another head",
                l.lo, l.hi
            ));
        }
        Ok(())
    }

    pub(crate) fn apply(self, t: &mut Tracking) {
        let o = t.outputs.entry(self.output).or_default();
        o.head = self.layer.hi;
        o.live = self.live;
        o.layers.push(self.layer);
    }
}
