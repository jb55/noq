//! In-memory indexes over the log, and the query planner that walks them.
//!
//! The log is the source of truth: [`Index::insert`] is fed every record on
//! open and every push after, so nothing here is ever written to disk.
//!
//! - `ids`: event id → seq, for every record still in the log, consumed or
//!   not. It is the dedup check on push, and the `ids` clause's lookup.
//! - posting lists for `authors`, `kinds` and single-letter `#tag` values:
//!   the seqs of pending records with that key, in arrival order.
//! - `seen`: a bounded ring of ids the log has dropped, so dedup outlives
//!   the records it checks against.
//!
//! A posting list is never edited when a record is acked or killed. Walks
//! skip such records by their state byte, and each list keeps a cursor past
//! its prefix of consumed records, so a long-drained head is skipped once
//! rather than on every lease. Consumed records past the cursor, behind one
//! that is held, are skipped each time; once a list has skipped as many as
//! it holds, [`Index::compact`] drops them. When the log drops a segment,
//! wherever it is, [`Index::trim`] cuts every list (and `ids`) back to what
//! is left, and moves the forgotten ids to `seen`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::filter::Filter;
use crate::log::Log;
use crate::note::NoteRef;
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

/// Consumed entries a posting list may skip past its cursor before it is
/// worth compacting, however short it is.
const WASTE: usize = 64;

/// What a posting list is keyed by: one of the clauses a filter can be
/// planned on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Key<'a> {
    Author(&'a [u8; 32]),
    Kind(u32),
    /// A single-letter tag's letter and first value.
    Tag(u8, &'a [u8]),
}

/// Calls `f` with every key `note` is posted under.
pub(crate) fn keys<'a>(note: &NoteRef<'a>, mut f: impl FnMut(Key<'a>)) {
    f(Key::Author(note.pubkey()));
    f(Key::Kind(note.kind()));
    for tag in note.tags() {
        let (Some(&[letter]), Some(value)) = (tag.get(0), tag.get(1)) else {
            continue;
        };
        if letter.is_ascii_alphabetic() {
            f(Key::Tag(letter, value));
        }
    }
}

#[derive(Default)]
struct Posting {
    /// Seqs in ascending order, each once.
    seqs: Vec<Seq>,
    /// `seqs[..skip]` are all consumed (ACKED, DEAD or reclaimed). Those
    /// states are final, so the cursor only moves forward; any reader
    /// advances it.
    skip: AtomicUsize,
    /// Consumed entries past `skip` that walks have stepped over since the
    /// list was last compacted.
    waste: AtomicUsize,
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

    /// Appends up to `n` of this list's seqs in `range` that are not
    /// consumed to `out`. Returns whether the list now wants
    /// [`compact`](Self::compact)ing.
    fn collect(&self, log: &Log, range: &Range<Seq>, n: usize, out: &mut Vec<Seq>) -> bool {
        let skip = self.advance(log);
        let from = skip.max(self.seqs.partition_point(|&s| s < range.start));
        let mut wasted = 0;
        let mut taken = 0;
        for &seq in &self.seqs[from..] {
            if taken == n || seq >= range.end {
                break;
            }
            if consumed(log, seq) {
                wasted += 1;
            } else {
                out.push(seq);
                taken += 1;
            }
        }
        wasted > 0 && self.waste.fetch_add(wasted, Ordering::Relaxed) + wasted >= self.budget()
    }

    /// How much [`waste`](Self::waste) the list takes before compacting it
    /// pays: half as much as it holds, so its cost is spread over the
    /// walks that stepped over the waste.
    fn budget(&self) -> usize {
        WASTE.max(self.cost() / 2)
    }

    /// Drops every consumed entry if walks have wasted the list's budget on
    /// them. Returns whether the list is now empty.
    fn compact(&mut self, log: &Log) -> bool {
        if *self.waste.get_mut() >= self.budget() {
            let skip = std::mem::take(self.skip.get_mut());
            self.seqs.drain(..skip);
            self.seqs.retain(|&s| !consumed(log, s));
            *self.waste.get_mut() = 0;
            self.shrink();
        }
        self.seqs.is_empty()
    }

    /// Drops the consumed prefix and every seq outside `live`. Returns
    /// whether the list is now empty.
    fn trim(&mut self, live: &[u32]) -> bool {
        self.seqs.drain(..*self.skip.get_mut());
        *self.skip.get_mut() = 0;
        self.seqs.retain(|s| is_live(live, *s));
        self.shrink();
        self.seqs.is_empty()
    }

    fn shrink(&mut self) {
        if self.seqs.capacity() > 4 * self.seqs.len().max(4) {
            self.seqs.shrink_to_fit();
        }
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
    seen: Seen,
    /// Set by a walk that left some posting list wanting
    /// [`compact`](Index::compact)ing.
    wasteful: AtomicBool,
}

impl Index {
    /// An empty index that remembers up to `seen_ids` dropped ids.
    pub(crate) fn new(seen_ids: usize) -> Index {
        Index {
            ids: HashMap::new(),
            authors: HashMap::new(),
            kinds: HashMap::new(),
            tags: HashMap::new(),
            seen: Seen {
                cap: seen_ids,
                order: VecDeque::new(),
                set: HashSet::new(),
            },
            wasteful: AtomicBool::new(false),
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

    /// Indexes the record at `seq`, which must be past every seq indexed
    /// before. A record that is already consumed goes in `ids` only.
    ///
    /// Returns the seq of an earlier copy of the same event, which
    /// compaction leaves behind (see [`id_at`](Self::id_at)) and which
    /// should die.
    pub(crate) fn insert(&mut self, note: &NoteRef<'_>, seq: Seq, pending: bool) -> Option<Seq> {
        let copied = self.id_at(note.id(), seq);
        if pending {
            self.post(note, seq);
        }
        copied
    }

    /// Records that event `id` is at `seq`, which must be past every seq
    /// indexed before. Returns where it was before, if it was in the log
    /// already: only compaction copies an event, and the earlier copy is
    /// garbage, unless the move was undone (see [`id_back`](Self::id_back)).
    pub(crate) fn id_at(&mut self, id: &[u8; 32], seq: Seq) -> Option<Seq> {
        self.ids.insert(*id, seq)
    }

    /// Points event `id` back at `seq`, an earlier copy of it that outlived
    /// the later one: a move that compaction undid.
    pub(crate) fn id_back(&mut self, id: &[u8; 32], seq: Seq) {
        self.ids.insert(*id, seq);
    }

    /// Adds a pending record to its posting lists.
    fn post(&mut self, note: &NoteRef<'_>, seq: Seq) {
        keys(note, |key| self.post_key(key, seq));
    }

    /// Adds a pending record to `key`'s posting list. `seq` must be past
    /// every seq in it.
    pub(crate) fn post_key(&mut self, key: Key<'_>, seq: Seq) {
        match key {
            Key::Author(pubkey) => self.authors.entry(*pubkey).or_default().push(seq),
            Key::Kind(kind) => self.kinds.entry(kind).or_default().push(seq),
            Key::Tag(letter, value) => {
                let values = self.tags.entry(letter).or_default();
                // get_mut first so an existing key doesn't cost an allocation.
                match values.get_mut(value) {
                    Some(p) => p.push(seq),
                    None => values.entry(Box::from(value)).or_default().push(seq),
                }
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
        self.authors.retain(|_, p| !p.trim(live));
        self.kinds.retain(|_, p| !p.trim(live));
        self.tags.retain(|_, values| {
            values.retain(|_, p| !p.trim(live));
            !values.is_empty()
        });
    }

    /// Whether a walk left some posting list wanting [`compact`]ing.
    ///
    /// [`compact`]: Self::compact
    pub(crate) fn wasteful(&self) -> bool {
        self.wasteful.load(Ordering::Relaxed)
    }

    /// Drops the consumed entries from every posting list that walks have
    /// stepped over at least as many of as it holds.
    pub(crate) fn compact(&mut self, log: &Log) {
        *self.wasteful.get_mut() = false;
        self.authors.retain(|_, p| !p.compact(log));
        self.kinds.retain(|_, p| !p.compact(log));
        self.tags.retain(|_, values| {
            values.retain(|_, p| !p.compact(log));
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
    /// may be leased or not match the rest of the filter; the caller
    /// checks both. A posting list's consumed entries are skipped, and
    /// counted towards [`wasteful`](Self::wasteful).
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
        let mut wasteful = false;
        let mut collect = |p: &Posting, out: &mut Vec<Seq>| {
            wasteful |= p.collect(log, &range, n, out);
        };
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
                            collect(p, out);
                        }
                    }
                }
            }
            Plan::Authors => {
                for a in f.get_authors().unwrap_or_default() {
                    if let Some(p) = self.authors.get(a) {
                        collect(p, out);
                    }
                }
            }
            Plan::Kinds => {
                for k in f.get_kinds().unwrap_or_default() {
                    if let Some(p) = self.kinds.get(k) {
                        collect(p, out);
                    }
                }
            }
        }
        if wasteful {
            self.wasteful.store(true, Ordering::Relaxed);
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
