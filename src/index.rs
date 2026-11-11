//! In-memory indexes over the log, and the query planner that walks them.
//!
//! The log is the source of truth: [`Index::insert`] is fed every record on
//! open and every push after, so nothing here is ever written to disk.
//!
//! - `ids`: event id → seq, for every record still in the log, consumed or
//!   not. It is the dedup check on push, and the `ids` clause's lookup.
//! - posting lists for `authors`, `kinds` and single-letter `#tag` values:
//!   the seqs of pending records with that key, in arrival order.
//!
//! A posting list is never edited when a record is acked or killed. Walks
//! skip such records by their state byte, and each list keeps a cursor past
//! its prefix of consumed records, so a long-drained head is skipped once
//! rather than on every lease. When the log drops a segment, wherever it
//! is, [`Index::trim`] cuts every list (and `ids`) back to what is left.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};

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

#[derive(Default)]
pub(crate) struct Index {
    ids: HashMap<[u8; 32], Seq>,
    authors: HashMap<[u8; 32], Posting>,
    kinds: HashMap<u32, Posting>,
    /// Keyed by letter, then by the tag's first value.
    tags: HashMap<u8, HashMap<Box<[u8]>, Posting>>,
}

impl Index {
    /// The record holding event `id`, if it is still in the log.
    pub(crate) fn id(&self, id: &[u8; 32]) -> Option<Seq> {
        self.ids.get(id).copied()
    }

    /// Indexes the record at `seq`, which must be past every seq indexed
    /// before. A record that is already consumed goes in `ids` only.
    pub(crate) fn insert(&mut self, note: &NoteRef<'_>, seq: Seq, pending: bool) {
        self.ids.insert(*note.id(), seq);
        if !pending {
            return;
        }
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
    /// log dropped the rest. Also drops every posting list's consumed
    /// prefix.
    pub(crate) fn trim(&mut self, live: &[u32]) {
        self.ids.retain(|_, s| is_live(live, *s));
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
}

fn cost<K: std::hash::Hash + Eq>(lists: &HashMap<K, Posting>, keys: &[K]) -> usize {
    keys.iter()
        .filter_map(|k| lists.get(k))
        .map(Posting::cost)
        .sum()
}
