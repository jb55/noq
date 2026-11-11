//! Sidecar `.idx` files: what indexing a sealed segment needs, written once
//! when it is sealed, so opening the log reads that instead of crc checking
//! and parsing every record in the segment.
//!
//! ```text
//!   magic     [u8; 8]  b"noqidx\0\x01"
//!   segment   u32      the segment's id
//!   end       u32      its committed end
//!   nrec      u32
//!   nkeys     u32
//!   records   [Entry; nrec], in offset order, every state included:
//!     off         u32
//!     flags       u32      1: the kind replaces  2: has an expiration tag
//!     created_at  u64
//!     expiration  u64      the NIP-40 tag's time, when flag 2
//!     id          [u8; 32]
//!   keys      nkeys times, sorted by key bytes:
//!     klen u32, key [u8; klen], n u32, [u32; n] ordinals into records, ascending
//!   crc32c    u32      over everything before it
//! ```
//!
//! All little endian. A key is `a` and a pubkey, `k` and a big-endian kind,
//! or `t`, a letter and a value: the [`Key`]s the index posts a record
//! under. A sealed segment only changes in its state bytes, which the
//! sidecar leaves out, so it stays true for as long as the segment lives.
//!
//! A sidecar is an optimization only. One that is missing, torn or
//! disagrees with its segment is ignored, and the segment is recovered the
//! slow way.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::Error;
use crate::index::{Key, keys};
use crate::note::KindClass;
use crate::segment::{Segment, Seq};

const MAGIC: [u8; 8] = *b"noqidx\0\x01";
const HEAD_LEN: usize = 24;
const ENTRY_LEN: usize = 56;

const REPLACES: u32 = 1;
const EXPIRES: u32 = 2;

/// One record of a sealed segment, as its sidecar lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Entry {
    pub off: u32,
    pub created_at: u64,
    /// The NIP-40 `expiration` tag.
    pub expiration: Option<u64>,
    /// Whether the kind is replaceable or addressable.
    pub replaces: bool,
    pub id: [u8; 32],
}

/// A sealed segment's records and the posting-list keys each is under.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Sidecar {
    pub segment: u32,
    pub end: u32,
    pub records: Vec<Entry>,
    /// Encoded keys (see the module docs) and the ordinals, into
    /// `records`, of the records under each.
    pub keys: Vec<(Vec<u8>, Vec<u32>)>,
}

impl Sidecar {
    /// The path of segment `id`'s sidecar in `dir`.
    pub fn path(dir: &Path, id: u32) -> PathBuf {
        dir.join(Segment::file_name(id)).with_extension("idx")
    }

    /// Lists the records of `seg`, which must be sealed.
    pub fn build(seg: &Segment) -> Sidecar {
        let mut records = Vec::new();
        let mut index: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();
        let mut key = Vec::new();
        for r in seg.records(0) {
            let note = r.note();
            let ord = records.len() as u32;
            records.push(Entry {
                off: r.seq().offset(),
                created_at: note.created_at(),
                expiration: note.expiration(),
                replaces: KindClass::of(note.kind()).replaces(),
                id: *note.id(),
            });
            keys(&note, |k| {
                encode_key(k, &mut key);
                match index.get_mut(&key) {
                    // A tag value repeated in one event is posted once.
                    Some(ords) if ords.last() == Some(&ord) => {}
                    Some(ords) => ords.push(ord),
                    None => {
                        index.insert(key.clone(), vec![ord]);
                    }
                }
            });
        }
        let mut keys: Vec<_> = index.into_iter().collect();
        keys.sort_unstable();
        Sidecar {
            segment: seg.id(),
            end: seg.end(),
            records,
            keys,
        }
    }

    /// The posting-list key an encoded key stands for.
    pub fn key(enc: &[u8]) -> Option<Key<'_>> {
        match enc {
            [b'a', rest @ ..] => Some(Key::Author(rest.try_into().ok()?)),
            [b'k', rest @ ..] => Some(Key::Kind(u32::from_be_bytes(rest.try_into().ok()?))),
            [b't', letter, value @ ..] => Some(Key::Tag(*letter, value)),
            _ => None,
        }
    }

    /// The seq of the record at `ord`.
    pub fn seq(&self, ord: u32) -> Seq {
        Seq::new(self.segment, self.records[ord as usize].off)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEAD_LEN + self.records.len() * ENTRY_LEN);
        out.extend_from_slice(&MAGIC);
        for n in [
            self.segment,
            self.end,
            self.records.len() as u32,
            self.keys.len() as u32,
        ] {
            out.extend_from_slice(&n.to_le_bytes());
        }
        for e in &self.records {
            let flags = if e.replaces { REPLACES } else { 0 }
                | if e.expiration.is_some() { EXPIRES } else { 0 };
            out.extend_from_slice(&e.off.to_le_bytes());
            out.extend_from_slice(&flags.to_le_bytes());
            out.extend_from_slice(&e.created_at.to_le_bytes());
            out.extend_from_slice(&e.expiration.unwrap_or(0).to_le_bytes());
            out.extend_from_slice(&e.id);
        }
        for (key, ords) in &self.keys {
            out.extend_from_slice(&(key.len() as u32).to_le_bytes());
            out.extend_from_slice(key);
            out.extend_from_slice(&(ords.len() as u32).to_le_bytes());
            for o in ords {
                out.extend_from_slice(&o.to_le_bytes());
            }
        }
        let crc = crc32c::crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    /// Parses a sidecar, or `None` if it is torn or malformed in any way.
    pub fn decode(buf: &[u8]) -> Option<Sidecar> {
        let (body, crc) = buf.split_last_chunk::<4>()?;
        if crc32c::crc32c(body) != u32::from_le_bytes(*crc) {
            return None;
        }
        let mut r = Reader(body);
        if r.take(8)? != MAGIC {
            return None;
        }
        let (segment, end, nrec, nkeys) = (r.u32()?, r.u32()?, r.u32()? as usize, r.u32()?);
        // Bounds the allocation by what the file can hold.
        if nrec > r.0.len() / ENTRY_LEN {
            return None;
        }
        let mut records = Vec::with_capacity(nrec);
        for _ in 0..nrec {
            let (off, flags, created_at, expiration) = (r.u32()?, r.u32()?, r.u64()?, r.u64()?);
            records.push(Entry {
                off,
                created_at,
                expiration: (flags & EXPIRES != 0).then_some(expiration),
                replaces: flags & REPLACES != 0,
                id: r.take(32)?.try_into().ok()?,
            });
        }
        let mut keys = Vec::new();
        for _ in 0..nkeys {
            let klen = r.u32()? as usize;
            let key = r.take(klen)?.to_vec();
            Sidecar::key(&key)?;
            let n = r.u32()? as usize;
            let ords = r
                .take(n.checked_mul(4)?)?
                .as_chunks::<4>()
                .0
                .iter()
                .map(|&c| u32::from_le_bytes(c))
                .collect::<Vec<_>>();
            if !ords.is_sorted_by(|a, b| a < b) || ords.last().is_some_and(|&o| o as usize >= nrec)
            {
                return None;
            }
            keys.push((key, ords));
        }
        r.0.is_empty().then_some(Sidecar {
            segment,
            end,
            records,
            keys,
        })
    }

    /// Writes the sidecar next to its segment in `dir`: to a temporary
    /// file first, synced, then renamed into place, so a crash leaves
    /// either the whole sidecar or none. The caller syncs `dir`.
    pub fn write(&self, dir: &Path) -> Result<(), Error> {
        let path = Sidecar::path(dir, self.segment);
        let tmp = path.with_extension("idx.tmp");
        let mut f = File::create(&tmp)?;
        f.write_all(&self.encode())?;
        f.sync_data()?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Reads segment `id`'s sidecar from `dir`, if there is a sound one.
    pub fn read(dir: &Path, id: u32) -> Option<Sidecar> {
        let sc = Sidecar::decode(&fs::read(Sidecar::path(dir, id)).ok()?)?;
        (sc.segment == id).then_some(sc)
    }
}

fn encode_key(key: Key<'_>, out: &mut Vec<u8>) {
    out.clear();
    match key {
        Key::Author(pubkey) => {
            out.push(b'a');
            out.extend_from_slice(pubkey);
        }
        Key::Kind(kind) => {
            out.push(b'k');
            out.extend_from_slice(&kind.to_be_bytes());
        }
        Key::Tag(letter, value) => {
            out.extend_from_slice(&[b't', letter]);
            out.extend_from_slice(value);
        }
    }
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if n > self.0.len() {
            return None;
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Some(a)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Sidecar {
        Sidecar {
            segment: 7,
            end: 4096,
            records: vec![
                Entry {
                    off: 0,
                    created_at: 1,
                    expiration: None,
                    replaces: false,
                    id: [1; 32],
                },
                Entry {
                    off: 512,
                    created_at: 2,
                    expiration: Some(0),
                    replaces: true,
                    id: [2; 32],
                },
            ],
            keys: vec![
                (b"a".iter().chain(&[9; 32]).copied().collect(), vec![0, 1]),
                (b"k\0\0\0\x01".to_vec(), vec![1]),
                (b"tpabc".to_vec(), vec![0]),
            ],
        }
    }

    #[test]
    fn round_trips() {
        let sc = sample();
        assert_eq!(Sidecar::decode(&sc.encode()), Some(sample()));
        assert_eq!(Sidecar::key(&sc.keys[1].0), Some(Key::Kind(1)));
        assert_eq!(Sidecar::key(&sc.keys[2].0), Some(Key::Tag(b'p', b"abc")));
    }

    #[test]
    fn rejects_anything_torn_or_off() {
        let buf = sample().encode();
        for n in 0..buf.len() {
            assert_eq!(Sidecar::decode(&buf[..n]), None, "cut at {n}");
        }
        let mut flipped = buf.clone();
        flipped[30] ^= 1;
        assert_eq!(Sidecar::decode(&flipped), None);

        // Sound crc, but an ordinal past the records.
        let mut sc = sample();
        sc.keys[0].1 = vec![0, 2];
        assert_eq!(Sidecar::decode(&sc.encode()), None);
        let mut sc = sample();
        sc.keys[0].1 = vec![1, 0];
        assert_eq!(Sidecar::decode(&sc.encode()), None);
        let mut sc = sample();
        sc.keys[0].0 = b"x".to_vec();
        assert_eq!(Sidecar::decode(&sc.encode()), None);
    }
}
