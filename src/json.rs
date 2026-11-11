//! Single-pass parser for one nostr event object.
//!
//! It walks the json once, decodes the fixed fields, and records every tag
//! value as a [`Span`] into the json. Only a value containing a backslash is
//! copied, unescaped, into the Side buffer. Content is recorded as a raw
//! span, escapes and all, after a decode into scratch has checked them;
//! the id hash unescapes it again.
//!
//! The parser is strict JSON (no raw control characters in strings, no
//! leading zeros, no trailing garbage) and strict nostr: the seven fields
//! must each appear exactly once, ids and keys are lowercase hex, numbers are
//! plain non-negative integers and tag values are strings. Unknown fields are
//! skipped.

use crate::error::Error;
use crate::note::Span;

/// The fixed fields of an event, everything that is not a tag value.
pub(crate) struct Fields {
    pub id: [u8; 32],
    pub pubkey: [u8; 32],
    pub sig: [u8; 64],
    pub created_at: u64,
    pub kind: u32,
    pub content: Span,
}

/// Where tag values go while parsing; the builder lays them out afterwards.
#[derive(Default)]
pub(crate) struct TagSections {
    /// Index into `vals` where each tag starts.
    pub starts: Vec<u32>,
    pub vals: Vec<Span>,
    pub side: Vec<u8>,
}

impl TagSections {
    pub fn clear(&mut self) {
        self.starts.clear();
        self.vals.clear();
        self.side.clear();
    }
}

/// Nesting limit when skipping unknown fields' values.
const MAX_DEPTH: u32 = 64;

const F_ID: u8 = 1 << 0;
const F_PUBKEY: u8 = 1 << 1;
const F_SIG: u8 = 1 << 2;
const F_CREATED_AT: u8 = 1 << 3;
const F_KIND: u8 = 1 << 4;
const F_TAGS: u8 = 1 << 5;
const F_CONTENT: u8 = 1 << 6;
const F_ALL: u8 = (1 << 7) - 1;

pub(crate) fn parse_event(json: &[u8], tags: &mut TagSections) -> Result<Fields, Error> {
    if json.len() > (u32::MAX >> 1) as usize {
        return Err(Error::TooLarge("json"));
    }
    if std::str::from_utf8(json).is_err() {
        return Err(Error::Utf8);
    }
    tags.clear();

    let mut p = Parser::new(json);
    let mut f = Fields {
        id: [0; 32],
        pubkey: [0; 32],
        sig: [0; 64],
        created_at: 0,
        kind: 0,
        content: Span::json(0, 0),
    };
    let mut seen = 0u8;

    p.ws();
    p.expect(b'{')?;
    p.ws();
    if p.peek() == Some(b'}') {
        p.i += 1;
    } else {
        loop {
            let (ks, ke, _) = p.string()?;
            p.ws();
            p.expect(b':')?;
            p.ws();
            let bit = match &json[ks..ke] {
                b"id" => {
                    f.id = p.hex()?;
                    F_ID
                }
                b"pubkey" => {
                    f.pubkey = p.hex()?;
                    F_PUBKEY
                }
                b"sig" => {
                    f.sig = p.hex()?;
                    F_SIG
                }
                b"created_at" => {
                    f.created_at = p.uint()?;
                    F_CREATED_AT
                }
                b"kind" => {
                    let at = p.i;
                    f.kind =
                        u32::try_from(p.uint()?).map_err(|_| p.err_at(at, "kind out of range"))?;
                    F_KIND
                }
                b"tags" => {
                    // A repeated "tags" would append to the first; the
                    // duplicate check below rejects it either way.
                    p.tags(tags)?;
                    F_TAGS
                }
                b"content" => {
                    let (s, e, escaped) = p.string()?;
                    if escaped {
                        // The span stays raw, but its escapes must decode
                        // (no lone surrogates), since the id hash and
                        // NoteRef::content unescape it later and trust it.
                        // The side buffer is scratch here: what this
                        // appends is cut off again.
                        let mark = tags.side.len();
                        let r = unescape(&json[s..e], &mut tags.side);
                        tags.side.truncate(mark);
                        r.map_err(|what| p.err_at(s, what))?;
                    }
                    f.content = Span::json(s as u32, (e - s) as u32);
                    F_CONTENT
                }
                _ => {
                    p.skip_value(0)?;
                    0
                }
            };
            if seen & bit != 0 {
                return Err(Error::Duplicate(field_name(bit)));
            }
            seen |= bit;
            p.ws();
            match p.next() {
                Some(b',') => p.ws(),
                Some(b'}') => break,
                _ => return Err(p.err("expected ',' or '}'")),
            }
        }
    }
    p.ws();
    if p.i != json.len() {
        return Err(p.err("trailing bytes after event"));
    }
    if seen != F_ALL {
        let missing = F_ALL & !seen;
        return Err(Error::Missing(field_name(missing.isolate_lowest_one())));
    }
    Ok(f)
}

fn field_name(bit: u8) -> &'static str {
    match bit {
        F_ID => "id",
        F_PUBKEY => "pubkey",
        F_SIG => "sig",
        F_CREATED_AT => "created_at",
        F_KIND => "kind",
        F_TAGS => "tags",
        _ => "content",
    }
}

pub(crate) struct Parser<'a> {
    pub b: &'a [u8],
    pub i: usize,
}

impl<'a> Parser<'a> {
    pub fn new(b: &'a [u8]) -> Parser<'a> {
        Parser { b, i: 0 }
    }

    pub fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    pub fn next(&mut self) -> Option<u8> {
        let c = self.peek()?;
        self.i += 1;
        Some(c)
    }

    pub fn err(&self, what: &'static str) -> Error {
        self.err_at(self.i, what)
    }

    pub fn err_at(&self, at: usize, what: &'static str) -> Error {
        Error::Json { at, what }
    }

    pub fn ws(&mut self) {
        while let Some(b' ' | b'\t' | b'\n' | b'\r') = self.peek() {
            self.i += 1;
        }
    }

    pub fn expect(&mut self, c: u8) -> Result<(), Error> {
        if self.peek() == Some(c) {
            self.i += 1;
            Ok(())
        } else {
            Err(self.err(match c {
                b'{' => "expected '{'",
                b'[' => "expected '['",
                b':' => "expected ':'",
                b'"' => "expected string",
                _ => "unexpected byte",
            }))
        }
    }

    /// Consumes a string and returns the byte range between its quotes and
    /// whether it holds escapes. Escapes are checked for shape here; their
    /// meaning is decoded by [`unescape`].
    pub fn string(&mut self) -> Result<(usize, usize, bool), Error> {
        self.expect(b'"')?;
        let start = self.i;
        let mut escaped = false;
        loop {
            let Some(c) = self.next() else {
                return Err(self.err("unterminated string"));
            };
            match c {
                b'"' => return Ok((start, self.i - 1, escaped)),
                b'\\' => {
                    escaped = true;
                    match self.next() {
                        Some(b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') => {}
                        Some(b'u') => {
                            let hex = self.b.get(self.i..self.i + 4);
                            if !hex.is_some_and(|h| h.iter().all(u8::is_ascii_hexdigit)) {
                                return Err(self.err("bad \\u escape"));
                            }
                            self.i += 4;
                        }
                        _ => return Err(self.err("bad escape")),
                    }
                }
                0..0x20 => return Err(self.err_at(self.i - 1, "control character in string")),
                _ => {}
            }
        }
    }

    /// A lowercase-hex string of exactly `N` bytes.
    pub fn hex<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let at = self.i;
        let (s, e, _) = self.string()?;
        let raw = &self.b[s..e];
        if raw.len() != N * 2 {
            return Err(self.err_at(at, "wrong hex length"));
        }
        let mut out = [0u8; N];
        for (o, &[hi, lo]) in out.iter_mut().zip(raw.as_chunks::<2>().0) {
            match (nibble(hi), nibble(lo)) {
                (Some(h), Some(l)) => *o = h << 4 | l,
                _ => return Err(self.err_at(at, "not lowercase hex")),
            }
        }
        Ok(out)
    }

    /// A non-negative integer with no sign, fraction, exponent or leading
    /// zero, so its canonical form is its decimal digits.
    pub fn uint(&mut self) -> Result<u64, Error> {
        let start = self.i;
        let mut n: u64 = 0;
        while let Some(c @ b'0'..=b'9') = self.peek() {
            n = n
                .checked_mul(10)
                .and_then(|n| n.checked_add((c - b'0') as u64))
                .ok_or_else(|| self.err_at(start, "integer overflow"))?;
            self.i += 1;
        }
        let digits = self.i - start;
        if digits == 0 {
            return Err(self.err("expected integer"));
        }
        if digits > 1 && self.b[start] == b'0' {
            return Err(self.err_at(start, "leading zero"));
        }
        if let Some(b'.' | b'e' | b'E') = self.peek() {
            return Err(self.err("expected integer"));
        }
        Ok(n)
    }

    fn tags(&mut self, t: &mut TagSections) -> Result<(), Error> {
        self.expect(b'[')?;
        self.ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            return Ok(());
        }
        loop {
            self.expect(b'[')?;
            t.starts.push(t.vals.len() as u32);
            self.ws();
            if self.peek() == Some(b']') {
                self.i += 1;
            } else {
                loop {
                    let (s, e, escaped) = self.string()?;
                    let span = if escaped {
                        let off = t.side.len();
                        unescape(&self.b[s..e], &mut t.side)
                            .map_err(|what| self.err_at(s, what))?;
                        Span::side(off as u32, (t.side.len() - off) as u32)
                    } else {
                        Span::json(s as u32, (e - s) as u32)
                    };
                    t.vals.push(span);
                    self.ws();
                    match self.next() {
                        Some(b',') => self.ws(),
                        Some(b']') => break,
                        _ => return Err(self.err("expected ',' or ']' in tag")),
                    }
                }
            }
            self.ws();
            match self.next() {
                Some(b',') => self.ws(),
                Some(b']') => return Ok(()),
                _ => return Err(self.err("expected ',' or ']' in tags")),
            }
        }
    }

    fn skip_value(&mut self, depth: u32) -> Result<(), Error> {
        if depth > MAX_DEPTH {
            return Err(self.err("nested too deep"));
        }
        let (open, close) = match self.peek() {
            Some(b'"') => return self.string().map(drop),
            Some(b'{') => (b'{', b'}'),
            Some(b'[') => (b'[', b']'),
            Some(b't') => return self.literal(b"true"),
            Some(b'f') => return self.literal(b"false"),
            Some(b'n') => return self.literal(b"null"),
            Some(b'-' | b'0'..=b'9') => return self.number(),
            _ => return Err(self.err("expected value")),
        };
        self.i += 1;
        self.ws();
        if self.peek() == Some(close) {
            self.i += 1;
            return Ok(());
        }
        loop {
            if open == b'{' {
                self.string()?;
                self.ws();
                self.expect(b':')?;
                self.ws();
            }
            self.skip_value(depth + 1)?;
            self.ws();
            match self.next() {
                Some(b',') => self.ws(),
                Some(c) if c == close => return Ok(()),
                _ => return Err(self.err("expected ',' or close bracket")),
            }
        }
    }

    fn literal(&mut self, lit: &[u8]) -> Result<(), Error> {
        if self.b[self.i..].starts_with(lit) {
            self.i += lit.len();
            Ok(())
        } else {
            Err(self.err("bad literal"))
        }
    }

    /// Any JSON number, for skipped fields.
    fn number(&mut self) -> Result<(), Error> {
        let start = self.i;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        let int = self.digits();
        if int == 0 || (int > 1 && self.b[self.i - int] == b'0') {
            return Err(self.err_at(start, "bad number"));
        }
        if self.peek() == Some(b'.') {
            self.i += 1;
            if self.digits() == 0 {
                return Err(self.err_at(start, "bad number"));
            }
        }
        if let Some(b'e' | b'E') = self.peek() {
            self.i += 1;
            if let Some(b'+' | b'-') = self.peek() {
                self.i += 1;
            }
            if self.digits() == 0 {
                return Err(self.err_at(start, "bad number"));
            }
        }
        Ok(())
    }

    fn digits(&mut self) -> usize {
        let start = self.i;
        while let Some(b'0'..=b'9') = self.peek() {
            self.i += 1;
        }
        self.i - start
    }
}

fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

/// Decodes the escapes in a string's raw bytes (between its quotes),
/// appending the result to `out`. `\u` surrogate pairs become one UTF-8
/// character; a lone surrogate is an error, so the output is valid UTF-8
/// whenever the input was.
pub(crate) fn unescape(raw: &[u8], out: &mut Vec<u8>) -> Result<(), &'static str> {
    let mut i = 0;
    while i < raw.len() {
        let Some(n) = raw[i..].iter().position(|&c| c == b'\\') else {
            out.extend_from_slice(&raw[i..]);
            break;
        };
        out.extend_from_slice(&raw[i..i + n]);
        i += n + 1;
        let c = *raw.get(i).ok_or("bad escape")?;
        i += 1;
        let b = match c {
            b'"' => b'"',
            b'\\' => b'\\',
            b'/' => b'/',
            b'b' => 0x08,
            b'f' => 0x0c,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'u' => {
                let hi = hex4(raw, i)?;
                i += 4;
                let cp = match hi {
                    0xd800..=0xdbff => {
                        if raw.get(i..i + 2) != Some(b"\\u") {
                            return Err("lone surrogate");
                        }
                        let lo = hex4(raw, i + 2)?;
                        if !(0xdc00..=0xdfff).contains(&lo) {
                            return Err("lone surrogate");
                        }
                        i += 6;
                        0x10000 + ((hi - 0xd800) << 10) + (lo - 0xdc00)
                    }
                    0xdc00..=0xdfff => return Err("lone surrogate"),
                    _ => hi,
                };
                let ch = char::from_u32(cp).ok_or("bad \\u escape")?;
                out.extend_from_slice(ch.encode_utf8(&mut [0; 4]).as_bytes());
                continue;
            }
            _ => return Err("bad escape"),
        };
        out.push(b);
    }
    Ok(())
}

fn hex4(raw: &[u8], i: usize) -> Result<u32, &'static str> {
    let h = raw.get(i..i + 4).ok_or("bad \\u escape")?;
    let s = std::str::from_utf8(h).map_err(|_| "bad \\u escape")?;
    u32::from_str_radix(s, 16).map_err(|_| "bad \\u escape")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn un(s: &str) -> Result<String, &'static str> {
        let mut out = vec![];
        unescape(s.as_bytes(), &mut out)?;
        Ok(String::from_utf8(out).unwrap())
    }

    #[test]
    fn unescape_forms() {
        assert_eq!(
            un(r#"a\"b\\c\/d\n\t\r\b\f"#).unwrap(),
            "a\"b\\c/d\n\t\r\u{8}\u{c}"
        );
        assert_eq!(un(r"caf\u00e9 \u00E9").unwrap(), "café é");
        assert_eq!(un(r"\ud83d\ude00!").unwrap(), "😀!");
        assert_eq!(un(r"\u0000").unwrap(), "\0");
        assert!(un(r"\ud83d").is_err());
        assert!(un(r"\ude00").is_err());
        assert!(un(r"\ud83d\u0041").is_err());
    }
}
