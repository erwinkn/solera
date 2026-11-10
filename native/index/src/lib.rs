//! Solera's change index: each output's log of changes, one entry per changed
//! key per commit, `(key, commit, new version, replaced version)`, sorted by
//! key then commit (newest first), in a few immutable layers on object
//! storage (docs/tracking-design.md §4).
//!
//! - [`format`]: the layer file, its writer and its parsers.
//! - [`ChangeIndex`]: the queries (`diff`, `scan`, `get`, `history`) at any
//!   commit at or above the floor, writing a commit, and merging layers.
//! - [`pick_merge`]: the size rule.
//! - [`Store`]: range GETs, a shared cache of blocks and metadata, single flight.
//!
//! The layer list is plain data the caller passes to each call: the engine's
//! journaled state owns it, and nothing is found by LIST.

mod disk;
pub mod error;
pub mod format;
pub mod glob;
mod query;
mod read;
pub mod run;
pub mod store;

pub use disk::DiskOptions;
pub use error::{Error, Result};
pub use format::{CommitHeader, LayerRef, WriteOptions, Writer};
pub use glob::{Glob, Pattern};
pub use query::{
    check_layers, pick_merge, ChangeIndex, CommitWriter, Entries, NewCommit, Page, Query,
};
pub use read::KeyList;
pub use run::{Run, Version};
pub use store::{Counts, Store, StoreOptions};
