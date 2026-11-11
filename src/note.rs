//! The packed note: a span index over an event's original JSON.
//!
//! noq keeps every event's JSON verbatim, so the index never copies a string
//! it can point at. It is a fixed head followed by three variable sections:
//!
//! ```text
//!  off  NoteHead (88 bytes, little endian)
//!    0    id          [u8; 32]
//!   32    pubkey      [u8; 32]
//!   64    created_at  u64
//!   72    kind        u32
//!   76    ntags       u16
//!   78    nvals       u16
//!   80    content     Span            into the json, still escaped
//!   88  TagStart  [u16; ntags + 1]    tag i = vals[TagStart[i]..TagStart[i+1]]
//!       Vals      [Span; nvals]
//!       Side      [u8]                unescaped copies, only of strings with escapes
//! ```
//!
//! A [`Span`] is `{off u32, len u32}`. The high bit of `len` says `off` is
//! into Side rather than the json. There is no padding between sections:
//! every field is read byte-wise, so nothing in the index needs alignment.

use std::borrow::Cow;
use std::fmt;

use crate::error::Error;
use crate::json;

/// Size of the fixed [`NoteRef`] head.
pub const HEAD_LEN: usize = 88;

const OFF_ID: usize = 0;
const OFF_PUBKEY: usize = 32;
const OFF_CREATED_AT: usize = 64;
const OFF_KIND: usize = 72;
const OFF_NTAGS: usize = 76;
const OFF_NVALS: usize = 78;
const OFF_CONTENT: usize = 80;

const SIDE_BIT: u32 = 1 << 31;

/// A string's location: a range of the json, or of the Side buffer when the
/// string had escapes and was unescaped there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    pub off: u32,
    /// Length, with [`SIDE_BIT`] set when `off` is into Side.
    len: u32,
}

impl Span {
    pub const SIZE: usize = 8;

    pub fn json(off: u32, len: u32) -> Span {
        debug_assert!(len & SIDE_BIT == 0);
        Span { off, len }
    }

    pub fn side(off: u32, len: u32) -> Span {
        debug_assert!(len & SIDE_BIT == 0);
        Span {
            off,
            len: len | SIDE_BIT,
        }
    }

    pub fn is_side(self) -> bool {
        self.len & SIDE_BIT != 0
    }

    pub fn len(self) -> usize {
        (self.len & !SIDE_BIT) as usize
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    pub(crate) fn write(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.off.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
    }

    fn read(b: &[u8]) -> Span {
        Span {
            off: le_u32(&b[0..]),
            len: le_u32(&b[4..]),
        }
    }

    fn resolve<'a>(self, json: &'a [u8], side: &'a [u8]) -> &'a [u8] {
        let off = self.off as usize;
        let src = if self.is_side() { side } else { json };
        &src[off..off + self.len()]
    }
}

/// A zero-copy view of one note: its span index and the json it points into.
///
/// Construction checks the section sizes against the index length. A span
/// that points outside its buffer means the index is corrupt and panics on
/// access; records are crc-checked before they get here.
#[derive(Clone, Copy)]
pub struct NoteRef<'a> {
    index: &'a [u8],
    json: &'a [u8],
    ntags: usize,
    nvals: usize,
    vals_off: usize,
    side_off: usize,
}

impl<'a> NoteRef<'a> {
    pub fn new(index: &'a [u8], json: &'a [u8]) -> Result<NoteRef<'a>, Error> {
        if index.len() < HEAD_LEN {
            return Err(Error::BadIndex);
        }
        let ntags = le_u16(&index[OFF_NTAGS..]) as usize;
        let nvals = le_u16(&index[OFF_NVALS..]) as usize;
        let vals_off = HEAD_LEN + (ntags + 1) * 2;
        let side_off = vals_off + nvals * Span::SIZE;
        if index.len() < side_off || le_u16(&index[vals_off - 2..]) as usize != nvals {
            return Err(Error::BadIndex);
        }
        Ok(NoteRef {
            index,
            json,
            ntags,
            nvals,
            vals_off,
            side_off,
        })
    }

    /// The span index bytes.
    pub fn index(&self) -> &'a [u8] {
        self.index
    }

    /// The original event json, byte for byte as it was pushed.
    pub fn json(&self) -> &'a [u8] {
        self.json
    }

    pub fn id(&self) -> &'a [u8; 32] {
        self.index[OFF_ID..OFF_ID + 32].try_into().unwrap()
    }

    pub fn pubkey(&self) -> &'a [u8; 32] {
        self.index[OFF_PUBKEY..OFF_PUBKEY + 32].try_into().unwrap()
    }

    pub fn created_at(&self) -> u64 {
        u64::from_le_bytes(
            self.index[OFF_CREATED_AT..OFF_CREATED_AT + 8]
                .try_into()
                .unwrap(),
        )
    }

    pub fn kind(&self) -> u32 {
        le_u32(&self.index[OFF_KIND..])
    }

    /// The content exactly as it appears inside the json's quotes, escapes
    /// and all.
    pub fn content_raw(&self) -> &'a [u8] {
        Span::read(&self.index[OFF_CONTENT..]).resolve(self.json, self.side())
    }

    /// The content with its JSON escapes decoded. Borrows when there were none.
    pub fn content(&self) -> Cow<'a, str> {
        let raw = self.content_raw();
        if !raw.contains(&b'\\') {
            return String::from_utf8_lossy(raw);
        }
        let mut out = Vec::with_capacity(raw.len());
        // The parser validated every escape, so this cannot fail on a note
        // it built.
        json::unescape(raw, &mut out).expect("content escapes were validated on ingest");
        Cow::Owned(String::from_utf8_lossy(&out).into_owned())
    }

    pub fn ntags(&self) -> usize {
        self.ntags
    }

    /// Total tag values across every tag.
    pub fn nvals(&self) -> usize {
        self.nvals
    }

    pub fn tag(&self, i: usize) -> Option<Tag<'a>> {
        if i >= self.ntags {
            return None;
        }
        let start = self.tag_start(i);
        let end = self.tag_start(i + 1);
        Some(Tag {
            note: *self,
            start,
            end,
        })
    }

    pub fn tags(&self) -> Tags<'a> {
        Tags { note: *self, i: 0 }
    }

    fn tag_start(&self, i: usize) -> usize {
        le_u16(&self.index[HEAD_LEN + i * 2..]) as usize
    }

    fn val(&self, i: usize) -> &'a [u8] {
        let at = self.vals_off + i * Span::SIZE;
        Span::read(&self.index[at..]).resolve(self.json, self.side())
    }

    fn side(&self) -> &'a [u8] {
        &self.index[self.side_off..]
    }
}

impl fmt::Debug for NoteRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NoteRef")
            .field("id", &hex(self.id()))
            .field("pubkey", &hex(self.pubkey()))
            .field("created_at", &self.created_at())
            .field("kind", &self.kind())
            .field("ntags", &self.ntags)
            .finish()
    }
}

/// One tag: a run of string values, the first usually its name.
#[derive(Clone, Copy)]
pub struct Tag<'a> {
    note: NoteRef<'a>,
    start: usize,
    end: usize,
}

impl<'a> Tag<'a> {
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }

    /// Value `i`, unescaped.
    pub fn get(&self, i: usize) -> Option<&'a [u8]> {
        (i < self.len()).then(|| self.note.val(self.start + i))
    }

    /// Value `i` as a str, `None` when absent.
    pub fn str(&self, i: usize) -> Option<&'a str> {
        self.get(i).and_then(|b| std::str::from_utf8(b).ok())
    }

    pub fn iter(&self) -> TagValues<'a> {
        TagValues {
            note: self.note,
            i: self.start,
            end: self.end,
        }
    }
}

impl fmt::Debug for Tag<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.iter().map(String::from_utf8_lossy))
            .finish()
    }
}

impl<'a> IntoIterator for Tag<'a> {
    type Item = &'a [u8];
    type IntoIter = TagValues<'a>;
    fn into_iter(self) -> TagValues<'a> {
        self.iter()
    }
}

pub struct Tags<'a> {
    note: NoteRef<'a>,
    i: usize,
}

impl<'a> Iterator for Tags<'a> {
    type Item = Tag<'a>;
    fn next(&mut self) -> Option<Tag<'a>> {
        let tag = self.note.tag(self.i)?;
        self.i += 1;
        Some(tag)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.note.ntags - self.i;
        (n, Some(n))
    }
}

impl ExactSizeIterator for Tags<'_> {}

pub struct TagValues<'a> {
    note: NoteRef<'a>,
    i: usize,
    end: usize,
}

impl<'a> Iterator for TagValues<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.i >= self.end {
            return None;
        }
        self.i += 1;
        Some(self.note.val(self.i - 1))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.end - self.i;
        (n, Some(n))
    }
}

impl ExactSizeIterator for TagValues<'_> {}

/// Writes the fixed head; the builder appends the sections after it.
pub(crate) struct Head {
    pub id: [u8; 32],
    pub pubkey: [u8; 32],
    pub created_at: u64,
    pub kind: u32,
    pub ntags: u16,
    pub nvals: u16,
    pub content: Span,
}

impl Head {
    pub(crate) fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.id);
        out.extend_from_slice(&self.pubkey);
        out.extend_from_slice(&self.created_at.to_le_bytes());
        out.extend_from_slice(&self.kind.to_le_bytes());
        out.extend_from_slice(&self.ntags.to_le_bytes());
        out.extend_from_slice(&self.nvals.to_le_bytes());
        self.content.write(out);
    }
}

pub(crate) fn hex(b: &[u8]) -> String {
    let mut s = Vec::with_capacity(b.len() * 2);
    hex_into(b, &mut s);
    String::from_utf8(s).unwrap()
}

pub(crate) fn hex_into(b: &[u8], out: &mut Vec<u8>) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for &x in b {
        out.push(DIGITS[(x >> 4) as usize]);
        out.push(DIGITS[(x & 15) as usize]);
    }
}

fn le_u16(b: &[u8]) -> u16 {
    u16::from_le_bytes([b[0], b[1]])
}

fn le_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}
