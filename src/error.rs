use std::fmt;

/// Why an event was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        }
    }
}

impl std::error::Error for Error {}
