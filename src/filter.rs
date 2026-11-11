//! NIP-01 filters, matched in place against a [`NoteRef`].
//!
//! A filter is a conjunction of clauses, each a set of accepted values:
//! an event matches when every clause the filter has accepts it. A clause
//! that is present but empty (`"kinds": []`) accepts nothing.
//!
//! Leasing consumes what it matches, so a filter that matches more than its
//! author meant takes items another consumer was waiting for. Parsing is
//! therefore strict where relays are lenient: an unknown field, a
//! multi-letter `#tag`, or an id or pubkey that is not 64 lowercase hex
//! chars (NIP-01's old prefix match) is an error, never ignored.

use crate::error::Error;
use crate::json::{self, Parser};
use crate::note::NoteRef;

/// A NIP-01 filter. Every value list is kept sorted and deduplicated, so
/// matching an event costs a binary search per clause.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Filter {
    ids: Option<Vec<[u8; 32]>>,
    authors: Option<Vec<[u8; 32]>>,
    kinds: Option<Vec<u32>>,
    /// `#x` clauses, one per letter, sorted by letter.
    tags: Vec<TagClause>,
    since: Option<u64>,
    until: Option<u64>,
    limit: Option<usize>,
}

/// One `#x` clause: the event needs an `x` tag whose first value is one of
/// `values`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagClause {
    pub letter: u8,
    /// Unescaped, sorted, deduplicated.
    pub values: Vec<Box<[u8]>>,
}

impl Filter {
    /// A filter with no clauses, which matches every event.
    pub fn new() -> Filter {
        Filter::default()
    }

    pub fn ids(mut self, ids: impl IntoIterator<Item = [u8; 32]>) -> Filter {
        self.ids = Some(sorted(ids));
        self
    }

    pub fn authors(mut self, authors: impl IntoIterator<Item = [u8; 32]>) -> Filter {
        self.authors = Some(sorted(authors));
        self
    }

    pub fn kinds(mut self, kinds: impl IntoIterator<Item = u32>) -> Filter {
        self.kinds = Some(sorted(kinds));
        self
    }

    /// Adds a `#letter` clause, replacing any earlier one for the letter.
    /// `letter` must be ASCII alphabetic: those are the tags NIP-01 indexes,
    /// and the only ones noq does.
    pub fn tag<V: AsRef<[u8]>>(
        mut self,
        letter: u8,
        values: impl IntoIterator<Item = V>,
    ) -> Filter {
        assert!(
            letter.is_ascii_alphabetic(),
            "tag filters are single letters"
        );
        let values = sorted(values.into_iter().map(|v| Box::from(v.as_ref())));
        match self.tags.binary_search_by_key(&letter, |c| c.letter) {
            Ok(i) => self.tags[i].values = values,
            Err(i) => self.tags.insert(i, TagClause { letter, values }),
        }
        self
    }

    /// Only events with `created_at >= since`.
    pub fn since(mut self, since: u64) -> Filter {
        self.since = Some(since);
        self
    }

    /// Only events with `created_at <= until`.
    pub fn until(mut self, until: u64) -> Filter {
        self.until = Some(until);
        self
    }

    /// The most events one query returns.
    pub fn limit(mut self, limit: usize) -> Filter {
        self.limit = Some(limit);
        self
    }

    pub fn get_ids(&self) -> Option<&[[u8; 32]]> {
        self.ids.as_deref()
    }

    pub fn get_authors(&self) -> Option<&[[u8; 32]]> {
        self.authors.as_deref()
    }

    pub fn get_kinds(&self) -> Option<&[u32]> {
        self.kinds.as_deref()
    }

    pub fn get_tags(&self) -> &[TagClause] {
        &self.tags
    }

    pub fn get_tag(&self, letter: u8) -> Option<&TagClause> {
        self.tags.iter().find(|c| c.letter == letter)
    }

    pub fn get_since(&self) -> Option<u64> {
        self.since
    }

    pub fn get_until(&self) -> Option<u64> {
        self.until
    }

    pub fn get_limit(&self) -> Option<usize> {
        self.limit
    }

    /// Whether `note` passes every clause.
    pub fn matches(&self, note: &NoteRef<'_>) -> bool {
        if self.since.is_some_and(|t| note.created_at() < t)
            || self.until.is_some_and(|t| note.created_at() > t)
        {
            return false;
        }
        if let Some(kinds) = &self.kinds
            && kinds.binary_search(&note.kind()).is_err()
        {
            return false;
        }
        if let Some(ids) = &self.ids
            && ids.binary_search(note.id()).is_err()
        {
            return false;
        }
        if let Some(authors) = &self.authors
            && authors.binary_search(note.pubkey()).is_err()
        {
            return false;
        }
        self.tags.iter().all(|c| c.matches(note))
    }

    /// Parses one filter object, as in a NIP-01 `REQ`.
    pub fn from_json(json: &[u8]) -> Result<Filter, Error> {
        let mut p = Parser::new(json);
        let mut f = Filter::new();
        let mut seen = Seen::default();
        p.ws();
        p.expect(b'{')?;
        p.ws();
        if p.peek() == Some(b'}') {
            p.i += 1;
        } else {
            loop {
                let at = p.i;
                let (ks, ke, escaped) = p.string()?;
                let key = &json[ks..ke];
                p.ws();
                p.expect(b':')?;
                p.ws();
                match key {
                    _ if escaped => return Err(p.err_at(at, "unknown filter field")),
                    b"ids" => {
                        seen.once("ids")?;
                        f.ids = Some(sorted(list(&mut p, |p| p.hex::<32>())?));
                    }
                    b"authors" => {
                        seen.once("authors")?;
                        f.authors = Some(sorted(list(&mut p, |p| p.hex::<32>())?));
                    }
                    b"kinds" => {
                        seen.once("kinds")?;
                        let kinds = list(&mut p, |p| {
                            let at = p.i;
                            u32::try_from(p.uint()?).map_err(|_| p.err_at(at, "kind out of range"))
                        })?;
                        f.kinds = Some(sorted(kinds));
                    }
                    b"since" => {
                        seen.once("since")?;
                        f.since = Some(p.uint()?);
                    }
                    b"until" => {
                        seen.once("until")?;
                        f.until = Some(p.uint()?);
                    }
                    b"limit" => {
                        seen.once("limit")?;
                        f.limit = Some(usize::try_from(p.uint()?).unwrap_or(usize::MAX));
                    }
                    &[b'#', letter] if letter.is_ascii_alphabetic() => {
                        if f.get_tag(letter).is_some() {
                            return Err(Error::Duplicate("#tag"));
                        }
                        let values = list(&mut p, |p| {
                            let (s, e, escaped) = p.string()?;
                            let raw = &p.b[s..e];
                            if !escaped {
                                return Ok(Box::from(raw));
                            }
                            let mut out = Vec::with_capacity(raw.len());
                            json::unescape(raw, &mut out).map_err(|what| p.err_at(s, what))?;
                            Ok(out.into_boxed_slice())
                        })?;
                        f = f.tag(letter, values);
                    }
                    [b'#', ..] => return Err(p.err_at(at, "tag filters are single letters")),
                    _ => return Err(p.err_at(at, "unknown filter field")),
                }
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
            return Err(p.err("trailing bytes after filter"));
        }
        Ok(f)
    }
}

impl TagClause {
    pub fn matches(&self, note: &NoteRef<'_>) -> bool {
        note.tags().any(|t| {
            t.len() >= 2
                && t.get(0) == Some(&[self.letter][..])
                && self
                    .values
                    .binary_search_by(|v| (**v).cmp(t.get(1).unwrap()))
                    .is_ok()
        })
    }
}

#[derive(Default)]
struct Seen(Vec<&'static str>);

impl Seen {
    fn once(&mut self, name: &'static str) -> Result<(), Error> {
        if self.0.contains(&name) {
            return Err(Error::Duplicate(name));
        }
        self.0.push(name);
        Ok(())
    }
}

/// A json array, each element read by `item`.
fn list<'a, T>(
    p: &mut Parser<'a>,
    mut item: impl FnMut(&mut Parser<'a>) -> Result<T, Error>,
) -> Result<Vec<T>, Error> {
    let mut out = Vec::new();
    p.expect(b'[')?;
    p.ws();
    if p.peek() == Some(b']') {
        p.i += 1;
        return Ok(out);
    }
    loop {
        out.push(item(p)?);
        p.ws();
        match p.next() {
            Some(b',') => p.ws(),
            Some(b']') => return Ok(out),
            _ => return Err(p.err("expected ',' or ']'")),
        }
    }
}

fn sorted<T: Ord>(items: impl IntoIterator<Item = T>) -> Vec<T> {
    let mut v: Vec<T> = items.into_iter().collect();
    v.sort_unstable();
    v.dedup();
    v
}
