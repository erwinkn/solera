//! What streamed inputs share: bytes owned elsewhere, and where a source stands.

use std::sync::Arc;

/// Bytes owned elsewhere — a Python `bytes`, or a `Vec` in tests.
pub type Bytes = Arc<dyn AsRef<[u8]> + Send + Sync>;

/// Where a streamed source stands: an entry ready, waiting for its next
/// chunk, or at its end.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    Ready,
    Starved,
    Done,
}
