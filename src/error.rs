use std::fmt;
use std::io;
use std::sync::Arc;

/// Why an event was rejected, or a log operation failed.
#[derive(Debug, Clone)]
pub enum Error {
    /// Malformed JSON or a field of the wrong shape, at byte `at`.
    Json { at: usize, what: &'static str },
    /// The input is not UTF-8.
    Utf8,
    /// A required field is absent.
    Missing(&'static str),
    /// A field appears twice.
    Duplicate(&'static str),
    /// Over a format limit: more than 65535 tags or tag values, or json over 2 GiB.
    TooLarge(&'static str),
    /// The pubkey is not a point on the curve.
    BadPubkey,
    /// The id is not the hash of the event.
    BadId,
    /// The signature does not verify against the id and pubkey.
    BadSig,
    /// A span index too short for the sizes in its own head.
    BadIndex,
    /// A filesystem call failed.
    Io(Arc<io::Error>),
    /// Another process has the log open.
    Locked,
    /// A config value out of range.
    Config(&'static str),
    /// A segment file that can't be opened as one.
    Corrupt(&'static str),
    /// The lease ran out (or was already ended) before it was acked, nacked
    /// or extended; the item went back to the queue.
    LeaseExpired,
}

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        use Error::*;
        match (self, other) {
            (Json { at: a, what: w }, Json { at: b, what: v }) => a == b && w == v,
            (Missing(a), Missing(b))
            | (Duplicate(a), Duplicate(b))
            | (TooLarge(a), TooLarge(b)) => a == b,
            (Config(a), Config(b)) | (Corrupt(a), Corrupt(b)) => a == b,
            (Io(a), Io(b)) => a.kind() == b.kind(),
            _ => std::mem::discriminant(self) == std::mem::discriminant(other),
        }
    }
}

impl Eq for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::Io(Arc::new(e))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Json { at, what } => write!(f, "invalid json at byte {at}: {what}"),
            Error::Utf8 => write!(f, "json is not utf-8"),
            Error::Missing(field) => write!(f, "missing field {field:?}"),
            Error::Duplicate(field) => write!(f, "duplicate field {field:?}"),
            Error::TooLarge(what) => write!(f, "{what} over the format limit"),
            Error::BadPubkey => write!(f, "pubkey is not a valid x-only key"),
            Error::BadId => write!(f, "id does not match the event hash"),
            Error::BadSig => write!(f, "signature does not verify"),
            Error::BadIndex => write!(f, "corrupt note index"),
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Locked => write!(f, "the log is open in another process"),
            Error::Config(what) => write!(f, "bad config: {what}"),
            Error::Corrupt(what) => write!(f, "corrupt segment: {what}"),
            Error::LeaseExpired => write!(f, "the lease expired"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(&**e),
            _ => None,
        }
    }
}
