use std::fmt;

#[derive(Debug, Clone)]
pub enum Error {
    /// Bytes that do not decode: an event or a snapshot.
    Format(String),
    /// A well-formed event or request that the state refuses.
    Invalid(String),
    /// The change index failed.
    Index(solera_index::Error),
    /// A batch's fence refuses it: the input became unsettled since its
    /// commit was built, so the batch is planned again.
    Unsettled,
}

pub const UNSETTLED: &str = "the input became unsettled since the batch's commit was built";

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Format(m) => write!(f, "format: {m}"),
            Error::Invalid(m) => write!(f, "invalid: {m}"),
            Error::Index(e) => write!(f, "index: {e:?}"),
            Error::Unsettled => write!(f, "invalid: {UNSETTLED}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<solera_index::Error> for Error {
    fn from(e: solera_index::Error) -> Error {
        Error::Index(e)
    }
}

pub(crate) fn format<T>(m: impl Into<String>) -> Result<T> {
    Err(Error::Format(m.into()))
}

pub(crate) fn invalid<T>(m: impl Into<String>) -> Result<T> {
    Err(Error::Invalid(m.into()))
}
