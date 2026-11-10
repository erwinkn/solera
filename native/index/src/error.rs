use std::fmt;
use std::sync::Arc;

/// What a call into the index can fail with.
#[derive(Debug, Clone)]
pub enum Error {
    /// A layer file is malformed, fails a checksum, or does not match its reference.
    Format(String),
    /// Entries do not chain: an entry's replaced version is not the previous entry's new one.
    Chain(String),
    /// The caller asked for something the index cannot answer (a bad layer list, a commit below the floor).
    Invalid(String),
    /// The object store failed.
    Store(Arc<str>),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Format(m) => write!(f, "malformed layer: {m}"),
            Error::Chain(m) => write!(f, "entries do not chain: {m}"),
            Error::Invalid(m) => write!(f, "{m}"),
            Error::Store(m) => write!(f, "object store: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        Error::Store(e.to_string().into())
    }
}

pub(crate) fn format<T>(m: impl Into<String>) -> Result<T> {
    Err(Error::Format(m.into()))
}

pub(crate) fn invalid<T>(m: impl Into<String>) -> Result<T> {
    Err(Error::Invalid(m.into()))
}
