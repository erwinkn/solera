//! Solera's tracking core: what each consumer holds of its inputs, the
//! planner, batch records and commits (docs/tracking-design.md §5 to §7).
//!
//! The core's state lives in one [`Tracking`] object, in memory. It changes
//! only through [`Tracking::apply`], which takes one journaled [`event`]:
//! the engine stores each event as bytes it does not read, and a checkpoint
//! stores [`Tracking::snapshot`]. Replaying the journal from a snapshot
//! rebuilds the same state.
//!
//! - [`codec`]: the byte encoding shared by events and snapshots.
//! - [`event`]: the envelope, one event of several parts, one part kind per unit.
//! - [`state`]: the object holding the core's state.
//! - [`progress`]: pieces and points per edge (B3).
//! - [`plan`]: the planner (B3).
//! - [`record`]: the batch record (B3).
//! - [`outcomes`]: key outcomes of per-key assets, held with their points (B6).
//! - [`due`]: what is due by time, and retry passes (B6).
//! - [`commit`]: output and source commits (B4).
//! - [`stale`]: staleness kept exact at every commit (B7).
//! - [`lineage`]: what a batch received, recomputed from its record (B10).

pub mod codec;
pub mod commit;
pub mod due;
pub mod error;
pub mod event;
pub mod lineage;
pub mod outcomes;
pub mod plan;
pub mod progress;
pub mod record;
pub mod stale;
pub mod state;

pub use error::{Error, Result};
pub use event::{Event, Part};
pub use state::{Edge, Id, Output, Tracking};
