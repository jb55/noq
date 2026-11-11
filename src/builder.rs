//! Builds a note's span index from its json.

use crate::error::Error;
use crate::json::{self, TagSections};
use crate::note::{Head, NoteRef, Span};
use crate::verify;

/// Parses event json into the span index described in [`crate::note`].
///
/// Keeps its scratch buffers between calls, so one builder per ingest
/// thread parses without allocating once it has warmed up.
#[derive(Default)]
pub struct NoteBuilder {
    tags: TagSections,
    canon: Vec<u8>,
}

impl NoteBuilder {
    pub fn new() -> NoteBuilder {
        NoteBuilder::default()
    }

    /// Parses `json`, checks its id and signature, and appends its span
    /// index to `out`. On error `out` is left as it was.
    pub fn build(&mut self, json: &[u8], out: &mut Vec<u8>) -> Result<(), Error> {
        let start = out.len();
        let sig = self.build_inner(json, out)?;
        let note = NoteRef::new(&out[start..], json).expect("builder wrote a consistent index");
        let checked = if verify::compute_id(&note, &mut self.canon) != *note.id() {
            Err(Error::BadId)
        } else {
            verify::verify_sig(&note, &sig)
        };
        if checked.is_err() {
            out.truncate(start);
        }
        checked
    }

    /// Like [`build`](Self::build) without the id or signature checks, for
    /// json that was verified before (a rebuild, a bench, a trusted source).
    pub fn build_unverified(&mut self, json: &[u8], out: &mut Vec<u8>) -> Result<(), Error> {
        self.build_inner(json, out).map(drop)
    }

    fn build_inner(&mut self, json: &[u8], out: &mut Vec<u8>) -> Result<[u8; 64], Error> {
        let f = json::parse_event(json, &mut self.tags)?;
        let t = &self.tags;
        let ntags = u16::try_from(t.starts.len()).map_err(|_| Error::TooLarge("tags"))?;
        let nvals = u16::try_from(t.vals.len()).map_err(|_| Error::TooLarge("tag values"))?;
        if t.side.len() > (u32::MAX >> 1) as usize {
            return Err(Error::TooLarge("escaped tag bytes"));
        }

        out.reserve(index_len(t));
        Head {
            id: f.id,
            pubkey: f.pubkey,
            created_at: f.created_at,
            kind: f.kind,
            ntags,
            nvals,
            content: f.content,
        }
        .write(out);
        // Every start is <= nvals, which fit in a u16 above.
        for &s in t.starts.iter().chain([&(nvals as u32)]) {
            out.extend_from_slice(&(s as u16).to_le_bytes());
        }
        for v in &t.vals {
            v.write(out);
        }
        out.extend_from_slice(&t.side);
        Ok(f.sig)
    }
}

fn index_len(t: &TagSections) -> usize {
    crate::note::HEAD_LEN + (t.starts.len() + 1) * 2 + t.vals.len() * Span::SIZE + t.side.len()
}

/// Builds and verifies one note's index in a fresh buffer.
pub fn build_note(json: &[u8]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    NoteBuilder::new().build(json, &mut out)?;
    Ok(out)
}
