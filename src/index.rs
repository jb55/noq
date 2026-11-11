//! In-memory indexes over the log, and the query planner that walks them.
//!
//! The log is the source of truth: [`Index::insert`] is fed every record on
//! open and every push after, so nothing here is ever written to disk.
//!
//! - `ids`: event id → seq, for every record still in the log, consumed or
//!   not. It is the dedup check on push, and the `ids` clause's lookup.
//! - posting lists for `authors`, `kinds` and single-letter `#tag` values:
//!   the seqs of pending records with that key, in arrival order.
//! - `slots`: for replaceable and addressable events, the newest version in
//!   the log per author, kind and `d` tag, consumed or not.
//! - `seen`: a bounded ring of ids the log has dropped, so dedup outlives
//!   the records it checks against.
//!
//! A posting list is never edited when a record is acked or killed. Walks
//! skip such records by their state byte, and each list keeps a cursor past
//! its prefix of consumed records, so a long-drained head is skipped once
//! rather than on every lease. When the log drops a segment, wherever it
//! is, [`Index::trim`] cuts every list (and `ids`) back to what is left,
//! and moves the forgotten ids to `seen`.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::filter::Filter;
use crate::log::Log;
use crate::note::{KindClass, NoteRef};
use crate::segment::{Seq, State};

/// How a query finds its candidates, picked by [`Index::plan`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Plan {
    /// Some clause matches no pending record, so nothing matches.
    Empty,
    /// Look up each of the filter's ids.
    Ids,
    /// Walk the posting lists of the filter's `#letter` values.
    Tag(u8),
    /// Walk the posting lists of the filter's authors.
    Authors,
    /// Walk the posting lists of the filter's kinds.
    Kinds,
    /// No indexed clause: walk the whole log.
    Scan,
}

#[derive(Default)]
struct Posting {
    /// Seqs in ascending order, each once.
    seqs: Vec<Seq>,
    /// `seqs[..skip]` are all consumed (ACKED, DEAD or reclaimed). Those
    /// states are final, so the cursor only moves forward; any reader
    /// advances it.
    skip: AtomicUsize,
}

impl Posting {
    fn push(&mut self, seq: Seq) {
        // A tag value repeated in one event indexes it once.
        if self.seqs.last() != Some(&seq) {
            self.seqs.push(seq);
        }
    }

    /// Candidates the list may still yield, consumed prefix aside.
    fn cost(&self) -> usize {
        self.seqs.len() - self.skip.load(Ordering::Relaxed)
    }

    /// Moves `skip` past consumed records and returns it.
    fn advance(&self, log: &Log) -> usize {
        let start = self.skip.load(Ordering::Relaxed);
        let mut i = start;
        while let Some(&seq) = self.seqs.get(i) {
            if !consumed(log, seq) {
                break;
            }
            i += 1;
        }
        if i > start {
            self.skip.fetch_max(i, Ordering::Relaxed);
        }
        i
    }

    /// Appends up to `n` of this list's seqs in `range` to `out`.
    fn collect(&self, log: &Log, range: &Range<Seq>, n: usize, out: &mut Vec<Seq>) {
        let skip = self.advance(log);
        let from = skip.max(self.seqs.partition_point(|&s| s < range.start));
        out.extend(
            self.seqs[from..]
                .iter()
                .take(n)
                .take_while(|&&s| s < range.end),
        );
    }

    /// Drops the consumed prefix and every seq outside `live`. Returns
    /// whether the list is now empty.
    fn trim(&mut self, live: &[u32]) -> bool {
        self.seqs.drain(..*self.skip.get_mut());
        *self.skip.get_mut() = 0;
        self.seqs.retain(|s| is_live(live, *s));
        if self.seqs.capacity() > 4 * self.seqs.len().max(4) {
            self.seqs.shrink_to_fit();
        }
        self.seqs.is_empty()
    }
}

/// Whether `seq` is in one of the `live` segment ids (sorted).
fn is_live(live: &[u32], seq: Seq) -> bool {
    live.binary_search(&seq.segment()).is_ok()
}

fn consumed(log: &Log, seq: Seq) -> bool {
    log.get(seq)
        .is_none_or(|r| matches!(r.state(), State::Acked | State::Dead))
}

/// One version of a replaceable or addressable event.
#[derive(Clone, Copy)]
struct Version {
    created_at: u64,
    id: [u8; 32],
    seq: Seq,
}

impl Version {
    /// NIP-01: the later `created_at` wins, and on a tie the lower id.
    fn beats(&self, other: &Version) -> bool {
        (self.created_at, Reverse(self.id)) > (other.created_at, Reverse(other.id))
    }
}

/// A replaceable or addressable event's author and kind; with a `d` tag,
/// where its versions compete.
type Slot = ([u8; 32], u32);

/// Where `note`'s versions compete, if its kind replaces: its author and
/// kind, then its `d` tag (empty for a replaceable kind).
fn slot<'a>(note: &NoteRef<'a>) -> Option<(Slot, &'a [u8])> {
    let key = (*note.pubkey(), note.kind());
    match KindClass::of(note.kind()) {
        KindClass::Replaceable => Some((key, b"")),
        KindClass::Addressable => Some((key, note.d_tag())),
        _ => None,
    }
}

/// Ids the log has dropped, at most `cap`, the oldest forgotten first.
struct Seen {
    cap: usize,
    order: VecDeque<[u8; 32]>,
    set: HashSet<[u8; 32]>,
}

impl Seen {
    fn insert(&mut self, id: [u8; 32]) {
        if self.cap == 0 || !self.set.insert(id) {
            return;
        }
        self.order.push_back(id);
        if self.order.len() > self.cap {
            let old = self.order.pop_front().unwrap();
            self.set.remove(&old);
        }
    }
}

pub(crate) struct Index {
    ids: HashMap<[u8; 32], Seq>,
    authors: HashMap<[u8; 32], Posting>,
    kinds: HashMap<u32, Posting>,
    /// Keyed by letter, then by the tag's first value.
    tags: HashMap<u8, HashMap<Box<[u8]>, Posting>>,
    /// The newest version per [`slot`]: keyed by author and kind, then by
    /// `d` tag.
    slots: HashMap<Slot, HashMap<Box<[u8]>, Version>>,
    seen: Seen,
}

impl Index {
    /// An empty index that remembers up to `seen_ids` dropped ids.
    pub(crate) fn new(seen_ids: usize) -> Index {
        Index {
            ids: HashMap::new(),
            authors: HashMap::new(),
            kinds: HashMap::new(),
            tags: HashMap::new(),
            slots: HashMap::new(),
            seen: Seen {
                cap: seen_ids,
                order: VecDeque::new(),
                set: HashSet::new(),
            },
        }
    }

    /// The record holding event `id`, if it is still in the log.
    pub(crate) fn id(&self, id: &[u8; 32]) -> Option<Seq> {
        self.ids.get(id).copied()
    }

    /// Whether event `id` was in a record the log has since dropped, as far
    /// as the seen ring remembers.
    pub(crate) fn seen(&self, id: &[u8; 32]) -> bool {
        self.seen.set.contains(id)
    }

    /// The seq of an indexed version that replaces `note`, if its kind
    /// replaces and a newer version than it is in the log. `note` itself
    /// being indexed doesn't count.
    pub(crate) fn newer(&self, note: &NoteRef<'_>) -> Option<Seq> {
        let (key, d) = slot(note)?;
        let cur = self.slots.get(&key)?.get(d)?;
        let v = Version {
            created_at: note.created_at(),
            id: *note.id(),
            seq: Seq(0),
        };
        cur.beats(&v).then_some(cur.seq)
    }

    /// Indexes the record at `seq`, which must be past every seq indexed
    /// before. A record that is already consumed goes in `ids` and `slots`
    /// only.
    ///
    /// If the event's kind replaces and another version of it is indexed,
    /// returns the seq of whichever of the two lost: the older one, or this
    /// one if it is the older.
    pub(crate) fn insert(&mut self, note: &NoteRef<'_>, seq: Seq, pending: bool) -> Option<Seq> {
        self.ids.insert(*note.id(), seq);
        let lost = self.place(note, seq);
        if pending {
            self.post(note, seq);
        }
        lost
    }

    /// Makes `note` its slot's newest version, if it is newer.
    fn place(&mut self, note: &NoteRef<'_>, seq: Seq) -> Option<Seq> {
        let (key, d) = slot(note)?;
        let v = Version {
            created_at: note.created_at(),
            id: *note.id(),
            seq,
        };
        let versions = self.slots.entry(key).or_default();
        match versions.get_mut(d) {
            None => {
                versions.insert(Box::from(d), v);
                None
            }
            Some(cur) if v.beats(cur) => Some(std::mem::replace(cur, v).seq),
            Some(_) => Some(seq),
        }
    }

    /// Adds a pending record to its posting lists.
    fn post(&mut self, note: &NoteRef<'_>, seq: Seq) {
        self.authors.entry(*note.pubkey()).or_default().push(seq);
        self.kinds.entry(note.kind()).or_default().push(seq);
        for tag in note.tags() {
            let (Some(&[letter]), Some(value)) = (tag.get(0), tag.get(1)) else {
                continue;
            };
            if !letter.is_ascii_alphabetic() {
                continue;
            }
            let values = self.tags.entry(letter).or_default();
            // get_mut first so an existing key doesn't cost an allocation.
            match values.get_mut(value) {
                Some(p) => p.push(seq),
                None => values.entry(Box::from(value)).or_default().push(seq),
            }
        }
    }

    /// Forgets every record outside the `live` segment ids (sorted): the
    /// log dropped the rest. Their ids go to the seen ring. Also drops
    /// every posting list's consumed prefix.
    pub(crate) fn trim(&mut self, live: &[u32]) {
        for (id, _) in self.ids.extract_if(|_, s| !is_live(live, *s)) {
            self.seen.insert(id);
        }
        self.slots.retain(|_, versions| {
            versions.retain(|_, v| is_live(live, v.seq));
            !versions.is_empty()
        });
        self.authors.retain(|_, p| !p.trim(live));
        self.kinds.retain(|_, p| !p.trim(live));
        self.tags.retain(|_, values| {
            values.retain(|_, p| !p.trim(live));
            !values.is_empty()
        });
    }

    /// Picks the indexed clause with the fewest candidates. Ties go to the
    /// most selective kind of key: ids, then tags, then authors, then kinds.
    pub(crate) fn plan(&self, f: &Filter) -> Plan {
        let mut best: Option<(usize, Plan)> = None;
        let mut consider = |cost: usize, plan: Plan| {
            if best.is_none_or(|(c, _)| cost < c) {
                best = Some((cost, plan));
            }
        };
        if let Some(ids) = f.get_ids() {
            consider(
                ids.iter().filter(|id| self.ids.contains_key(*id)).count(),
                Plan::Ids,
            );
        }
        for c in f.get_tags() {
            let values = self.tags.get(&c.letter);
            let cost = c
                .values
                .iter()
                .filter_map(|v| values?.get(v))
                .map(Posting::cost)
                .sum();
            consider(cost, Plan::Tag(c.letter));
        }
        if let Some(authors) = f.get_authors() {
            consider(cost(&self.authors, authors), Plan::Authors);
        }
        if let Some(kinds) = f.get_kinds() {
            consider(cost(&self.kinds, kinds), Plan::Kinds);
        }
        match best {
            Some((0, _)) => Plan::Empty,
            Some((_, plan)) => plan,
            None => Plan::Scan,
        }
    }

    /// Fills `out` with the first `n` candidate seqs in `range` for `plan`,
    /// ascending. Fewer than `n` means there are no more. Candidates
    /// may be consumed or not match the rest of the filter; the caller
    /// checks both.
    pub(crate) fn candidates(
        &self,
        log: &Log,
        f: &Filter,
        plan: Plan,
        range: Range<Seq>,
        n: usize,
        out: &mut Vec<Seq>,
    ) {
        out.clear();
        match plan {
            Plan::Empty | Plan::Scan => return,
            Plan::Ids => out.extend(
                f.get_ids()
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|id| self.ids.get(id).copied())
                    .filter(|s| range.contains(s)),
            ),
            Plan::Tag(letter) => {
                if let (Some(c), Some(values)) = (f.get_tag(letter), self.tags.get(&letter)) {
                    for v in &c.values {
                        if let Some(p) = values.get(v) {
                            p.collect(log, &range, n, out);
                        }
                    }
                }
            }
            Plan::Authors => {
                for a in f.get_authors().unwrap_or_default() {
                    if let Some(p) = self.authors.get(a) {
                        p.collect(log, &range, n, out);
                    }
                }
            }
            Plan::Kinds => {
                for k in f.get_kinds().unwrap_or_default() {
                    if let Some(p) = self.kinds.get(k) {
                        p.collect(log, &range, n, out);
                    }
                }
            }
        }
        // The n smallest of the union are among the first n of each list.
        out.sort_unstable();
        out.dedup();
        out.truncate(n);
    }

    /// Entries across every posting list, for stats and tests.
    pub(crate) fn postings(&self) -> usize {
        let sum = |m: &mut dyn Iterator<Item = &Posting>| m.map(|p| p.seqs.len()).sum::<usize>();
        sum(&mut self.authors.values())
            + sum(&mut self.kinds.values())
            + sum(&mut self.tags.values().flat_map(|v| v.values()))
    }

    pub(crate) fn len(&self) -> usize {
        self.ids.len()
    }

    /// Ids in the seen ring.
    pub(crate) fn seen_len(&self) -> usize {
        self.seen.order.len()
    }
}

fn cost<K: std::hash::Hash + Eq>(lists: &HashMap<K, Posting>, keys: &[K]) -> usize {
    keys.iter()
        .filter_map(|k| lists.get(k))
        .map(Posting::cost)
        .sum()
}
