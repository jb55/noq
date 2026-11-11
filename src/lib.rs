//! noq — a nostr message-queue store.
//!
//! Events are pushed in, consumers lease them by NIP-01 filter, and an ack
//! deletes them. Each record keeps the event's original json verbatim, so a
//! leased event goes back out with no re-serialization, plus a small span
//! index into that json ([`NoteRef`]) for matching filters in place.

mod builder;
mod error;
mod json;
mod log;
mod note;
mod segment;
mod verify;

pub use builder::{NoteBuilder, build_note};
pub use error::Error;
pub use log::{Config, Fsync, Log, Record, Scan};
pub use note::{HEAD_LEN, NoteRef, Span, Tag, TagValues, Tags};
pub use segment::{RECORD_HEADER_LEN, RECORD_VERSION, RecordRef, Records, Segment, Seq, State};
pub use verify::{canonical, compute_id, verify_sig};
