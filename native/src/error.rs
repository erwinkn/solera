//! The errors every part of the extension raises, as Python sees them
//! (`lib.rs`'s `to_py`).

#[derive(Debug)]
pub enum Error {
    /// Malformed input or a failed checksum.
    Format(String),
    /// Caller error: unsorted or duplicate keys, mismatched lengths.
    Value(String),
    /// Raised by a caller's callback (reading Python values), passed through.
    Callback(Box<dyn std::error::Error + Send + Sync>),
    /// Well-formed input over a caller's limit: more entries or bytes than it takes.
    Limit(String),
}

pub type Result<T> = std::result::Result<T, Error>;
