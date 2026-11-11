//! Event id and signature checks.
//!
//! The id is sha256 of the NIP-01 canonical serialization
//! `[0,"<pubkey>",<created_at>,<kind>,<tags>,"<content>"]`, built here from a
//! [`NoteRef`] so the hash reads the same unescaped values a filter sees.

use secp256k1::XOnlyPublicKey;
use secp256k1::schnorr::{self, Signature};
use sha2::{Digest, Sha256};

use crate::error::Error;
use crate::json;
use crate::note::{NoteRef, hex_into};

/// Appends the canonical serialization of `note` to `out`.
pub fn canonical(note: &NoteRef<'_>, out: &mut Vec<u8>) {
    out.extend_from_slice(b"[0,\"");
    hex_into(note.pubkey(), out);
    out.extend_from_slice(b"\",");
    out.extend_from_slice(note.created_at().to_string().as_bytes());
    out.push(b',');
    out.extend_from_slice(note.kind().to_string().as_bytes());
    out.extend_from_slice(b",[");
    for (i, tag) in note.tags().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.push(b'[');
        for (j, val) in tag.iter().enumerate() {
            if j > 0 {
                out.push(b',');
            }
            escape_into(val, out);
        }
        out.push(b']');
    }
    out.extend_from_slice(b"],");
    let raw = note.content_raw();
    if raw.contains(&b'\\') {
        let start = out.len();
        json::unescape(raw, out).expect("content escapes were validated on ingest");
        // Unescape in place at the tail, then move it out to escape it.
        let content = out.split_off(start);
        escape_into(&content, out);
    } else {
        escape_into(raw, out);
    }
    out.push(b']');
}

/// The sha256 of the note's canonical serialization. `scratch` is reused
/// between calls to avoid an allocation per note.
pub fn compute_id(note: &NoteRef<'_>, scratch: &mut Vec<u8>) -> [u8; 32] {
    scratch.clear();
    canonical(note, scratch);
    Sha256::digest(&scratch[..]).into()
}

/// Checks that `sig` is the pubkey's schnorr signature over the id.
pub fn verify_sig(note: &NoteRef<'_>, sig: &[u8; 64]) -> Result<(), Error> {
    let pk = XOnlyPublicKey::from_byte_array(*note.pubkey()).map_err(|_| Error::BadPubkey)?;
    schnorr::verify(&Signature::from_byte_array(*sig), note.id(), &pk).map_err(|_| Error::BadSig)
}

/// NIP-01 string escaping: `"`, `\`, and the five named control characters
/// are escaped; every other byte goes out verbatim.
fn escape_into(s: &[u8], out: &mut Vec<u8>) {
    out.push(b'"');
    let mut run = 0;
    for (i, &c) in s.iter().enumerate() {
        let esc: &[u8] = match c {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            b'\t' => b"\\t",
            0x08 => b"\\b",
            0x0c => b"\\f",
            _ => continue,
        };
        out.extend_from_slice(&s[run..i]);
        out.extend_from_slice(esc);
        run = i + 1;
    }
    out.extend_from_slice(&s[run..]);
    out.push(b'"');
}
