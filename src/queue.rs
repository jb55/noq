//! [`Noq`]: the queue. Events are pushed in, consumers lease them by
//! [`Filter`], and an ack consumes them for good.
//!
//! A lease is planned over the in-memory [`Index`]: the filter's most
//! selective indexed clause gives candidates in arrival order, each is
//! checked against the whole filter in place and taken by a CAS on its
//! state byte, until the limit. A filter with no indexed clause walks the
//! log.
//!
//! A lease holds an item out of the queue until its deadline. `ack` marks it
//! ACKED, `nack` puts it back now, and a lease that runs out (or is dropped
//! without either) is put back by the reaper thread at its deadline, so an
//! item is delivered at least once.
//!
//! Some items die without being consumed, moving to DEAD:
//!
//! - An event expires at its NIP-40 `expiration` tag, or once it is
//!   [`Config::max_age`] old. Walks never hand out an expired item, and a
//!   sweep run by pushes and leases kills the rest so their segments drop.
//!
//! Replaceable and addressable events are queued like any other: every
//! version is delivered, in arrival order, and deciding which one is
//! newest is the consumer's job. That is off-spec for a relay, on purpose.
//!
//! Every id stays known while its record is in the log, and a bounded seen
//! ring remembers it after, so pushing a consumed event again is a no-op.
//!
//! A sealed segment whose records are mostly consumed is compacted: its
//! queued items are copied to the tail and the originals die, so the
//! segment drains and drops instead of being pinned by a few items. Every
//! seal compacts the segments older than the one it sealed, and
//! [`compact`](Noq::compact) does all of them now. A moved item is queued
//! after the items pushed before the move: compaction trades its place in
//! line for the space.
//!
//! [`wait_lease`](Noq::wait_lease) blocks until a lease gets something. It
//! sleeps on a [`Signal`] that a push which queues an item, a nack and a
//! lease running out each bump, and tries again on every bump. While it
//! sleeps it also wakes at the next expiry, so a queue nobody pushes to
//! still sweeps.

use std::cell::RefCell;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::builder::NoteBuilder;
use crate::error::Error;
use crate::expire::{self, Expiry};
use crate::filter::Filter;
use crate::index::{Index, Plan};
use crate::lease::Leases;
use crate::log::{Config, Fsync, Log, Record};
use crate::note::NoteRef;
use crate::notify::Signal;
use crate::segment::{Segment, Seq, State};
use crate::sidecar::Sidecar;

/// Candidates read from the index per lock: a walk holds the index's read
/// lock only while it copies one batch, so pushes wait on that, not on the
/// whole lease.
const BATCH: usize = 256;

/// A sealed segment is compacted once fewer than one in this many of its
/// records are pending.
const COMPACT_BELOW: u64 = 4;

thread_local! {
    /// Parse scratch for [`Noq::push`], so verification runs outside every
    /// lock without allocating per push.
    static SCRATCH: RefCell<(NoteBuilder, Vec<u8>)> = RefCell::default();
}

/// What [`Noq::push`] did with an event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pushed {
    /// Queued at this seq.
    Queued(Seq),
    /// An event with this id is already in the log, at this seq, queued or
    /// consumed; nothing was added.
    Duplicate(Seq),
    /// An event with this id was consumed and its record has left the log,
    /// but the seen ring remembers it; nothing was added.
    Seen,
    /// The event is already expired; nothing was added.
    Expired,
}

impl Pushed {
    /// Where the event was queued, if it was.
    pub fn queued(self) -> Option<Seq> {
        match self {
            Pushed::Queued(seq) => Some(seq),
            _ => None,
        }
    }
}

/// Sizes of the in-memory index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexStats {
    /// Event ids known, one per record in the log.
    pub ids: usize,
    /// Entries across every posting list.
    pub postings: usize,
    /// Ids remembered in the seen ring after their records left the log.
    pub seen: usize,
    /// Records waiting for their expiry sweep.
    pub expiring: usize,
}

/// A queue over a [`Log`] directory. Cloning it is cheap; clones share the
/// queue.
#[derive(Clone)]
pub struct Noq(Arc<Inner>);

struct Inner {
    log: Log,
    index: RwLock<Index>,
    /// Held across a push's dedup check, append and index insert, so ids
    /// are unique in the log and posting lists stay in seq order.
    push_lock: Mutex<()>,
    /// [`Log::dropped`] when the index was last trimmed.
    trimmed: AtomicU64,
    /// [`Log::sealed`] when segments were last compacted.
    compacted: AtomicU64,
    expiry: Expiry,
    /// Bumped when an item may have become leasable.
    queued: Arc<Signal>,
    leases: Arc<Leases>,
    reaper: Option<JoinHandle<()>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.leases.stop();
        if let Some(h) = self.reaper.take() {
            let _ = h.join();
        }
    }
}

impl Noq {
    /// Opens the queue in `dir` and indexes every record in it. Items
    /// leased when the last process exited are queued again; expired ones
    /// die.
    pub fn open(dir: impl AsRef<Path>, config: Config) -> Result<Noq, Error> {
        let log = Log::open_indexed(dir, config)?;
        let trimmed = AtomicU64::new(log.dropped());
        let compacted = AtomicU64::new(log.sealed());
        let mut index = Index::new(config.seen_ids);
        let expiry = Expiry::new(config.max_age);
        // An event in two places: compaction copied it, then died, or its
        // sync failed, before either copy died. The later copy wins, unless
        // it is DEAD and the earlier LIVE: an undone move, or an expired
        // item that the sweep kills anyway.
        let settle = |index: &mut Index, id: &[u8; 32], seq: Seq, earlier: Option<Seq>| {
            let Some(old) = earlier.and_then(|s| log.get(s)) else {
                return;
            };
            let dead = log.get(seq).is_some_and(|r| r.state() == State::Dead);
            if dead && old.state() == State::Live {
                index.id_back(id, old.seq());
            } else {
                log.retire(&old, State::Live, State::Dead);
            }
        };
        for seg in log.segments() {
            if let Some(sc) = log.take_sidecar(seg.id()) {
                // Recovery made every pending record LIVE.
                let pending: Vec<bool> = sc
                    .records
                    .iter()
                    .map(|e| seg.record(e.off).is_some_and(|r| r.state() == State::Live))
                    .collect();
                for (e, &pending) in sc.records.iter().zip(&pending) {
                    let seq = Seq::new(seg.id(), e.off);
                    let earlier = index.id_at(&e.id, seq);
                    settle(&mut index, &e.id, seq, earlier);
                    if let (true, Some(at)) = (pending, expiry.at(e.created_at, e.expiration)) {
                        expiry.add(at, seq);
                    }
                }
                for (key, ords) in &sc.keys {
                    let key = Sidecar::key(key).expect("a decoded sidecar's keys parse");
                    for &o in ords.iter().filter(|&&o| pending[o as usize]) {
                        index.post_key(key, sc.seq(o));
                    }
                }
                continue;
            }
            for r in seg.records(0) {
                let note = r.note();
                let pending = r.state() == State::Live;
                let earlier = index.insert(&note, r.seq(), pending);
                settle(&mut index, note.id(), r.seq(), earlier);
                if let (true, Some(at)) = (pending, expiry.of(&note)) {
                    expiry.add(at, r.seq());
                }
            }
        }
        let queued = Arc::new(Signal::default());
        let leases = Leases::new(queued.clone());
        let reaper = Some(leases.spawn_reaper()?);
        let q = Noq(Arc::new(Inner {
            log,
            index: RwLock::new(index),
            push_lock: Mutex::new(()),
            trimmed,
            compacted,
            expiry,
            queued,
            leases,
            reaper,
        }));
        q.expire();
        Ok(q)
    }

    /// The log under the queue. Appending to it directly bypasses the
    /// index and the dedup check: such records are found only by
    /// [`lease_where`](Self::lease_where) until the queue is reopened.
    pub fn log(&self) -> &Log {
        &self.0.log
    }

    /// Parses, verifies and queues one event, unless it is a duplicate or
    /// expired.
    pub fn push(&self, json: &[u8]) -> Result<Pushed, Error> {
        SCRATCH.with_borrow_mut(|(builder, index)| {
            index.clear();
            builder.build(json, index)?;
            self.push_note(NoteRef::new(index, json)?)
        })
    }

    /// Queues an event whose index was already built, and its id and
    /// signature checked, by the caller.
    pub fn push_note(&self, note: NoteRef<'_>) -> Result<Pushed, Error> {
        let inner = &self.0;
        let now = expire::now();
        let pushed = {
            let _push = inner.push_lock.lock();
            let index = inner.index.read();
            if let Some(seq) = index.id(note.id()) {
                return Ok(Pushed::Duplicate(seq));
            }
            if index.seen(note.id()) {
                return Ok(Pushed::Seen);
            }
            let expires = inner.expiry.of(&note);
            if expires.is_some_and(|at| at <= now) {
                return Ok(Pushed::Expired);
            }
            drop(index);
            let seq = inner.log.append_note(note)?;
            let copied = inner.index.write().insert(&note, seq, true);
            debug_assert_eq!(copied, None, "the id was checked under the push lock");
            if let Some(at) = expires {
                inner.expiry.add(at, seq);
            }
            Pushed::Queued(seq)
        };
        inner.queued.bump();
        if inner.expiry.is_due(now) {
            inner.expiry.sweep(&inner.log, now);
        }
        // The append may have sealed a drained tail and dropped it, and the
        // sweep may have drained others.
        self.trim();
        let sealed = inner.log.sealed();
        if inner.compacted.swap(sealed, Ordering::AcqRel) != sealed {
            // The newest sealed segment is left a segment's worth of
            // pushes for its consumers to catch up.
            self.compact_older(2);
        }
        Ok(pushed)
    }

    /// Kills every queued item whose expiry has passed, now rather than
    /// when a push or lease next gets to it. Returns how many.
    pub fn expire(&self) -> usize {
        let killed = self.0.expiry.sweep(&self.0.log, expire::now());
        self.trim();
        killed
    }

    /// Leases up to `limit` queued items matching `filter`, oldest first,
    /// each for `ttl`. The filter's own limit, if it has one, caps `limit`.
    /// Items pushed while this runs are not considered.
    pub fn lease(&self, filter: &Filter, limit: usize, ttl: Duration) -> Vec<Lease> {
        let limit = filter.get_limit().map_or(limit, |l| l.min(limit));
        let mut taken = Vec::new();
        if limit > 0 {
            self.walk(filter, |rec| {
                if rec.get().cas_state(State::Live, State::Leased) {
                    taken.push(rec);
                }
                taken.len() < limit
            });
        }
        self.hold(taken, ttl)
    }

    /// Like [`lease`](Self::lease), but if nothing matches, waits for
    /// something to, for up to `timeout`. Returns as soon as one lease gets
    /// anything, so it may return fewer than `limit` items even if more
    /// arrive just after; empty means the timeout passed.
    ///
    /// Every push that queues an item, nack and lease running out wakes
    /// each waiter to lease again, matching or not.
    pub fn wait_lease(
        &self,
        filter: &Filter,
        limit: usize,
        ttl: Duration,
        timeout: Duration,
    ) -> Vec<Lease> {
        let until = Instant::now().checked_add(timeout);
        loop {
            // Read before leasing: anything queued after the lease's walk
            // started has moved it.
            let seen = self.0.queued.generation();
            let got = self.lease(filter, limit, ttl);
            if !got.is_empty() || filter.get_limit().map_or(limit, |l| l.min(limit)) == 0 {
                return got;
            }
            let now = Instant::now();
            if until.is_some_and(|at| at <= now) {
                return got;
            }
            // Wake for the next expiry too: the lease above sweeps it, so a
            // queue that is only waited on still lets expired segments go.
            let sweep = self.0.expiry.next().map(|at| {
                let secs = at.saturating_sub(expire::now()).max(1);
                now.checked_add(Duration::from_secs(secs))
            });
            let wake = match (until, sweep.flatten()) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            self.0.queued.wait(seen, wake);
        }
    }

    /// Up to `limit` queued items matching `filter`, oldest first, without
    /// leasing them: another consumer may take any of them right after.
    pub fn peek(&self, filter: &Filter, limit: usize) -> Vec<Record> {
        let limit = filter.get_limit().map_or(limit, |l| l.min(limit));
        let mut found = Vec::new();
        if limit > 0 {
            self.walk(filter, |rec| {
                found.push(rec);
                found.len() < limit
            });
        }
        found
    }

    /// How [`lease`](Self::lease) would find `filter`'s candidates now.
    pub fn plan(&self, filter: &Filter) -> Plan {
        self.0.index.read().plan(filter)
    }

    pub fn index_stats(&self) -> IndexStats {
        let index = self.0.index.read();
        IndexStats {
            ids: index.len(),
            postings: index.postings(),
            seen: index.seen_len(),
            expiring: self.0.expiry.len(),
        }
    }

    /// Calls `visit` with each LIVE record matching `filter` that was in the
    /// log when the walk started, in seq order, until it returns false.
    /// Expired records it finds on the way die instead.
    fn walk(&self, filter: &Filter, visit: impl FnMut(Record) -> bool) {
        let now = expire::now();
        if self.0.expiry.is_due(now) {
            self.0.expiry.sweep(&self.0.log, now);
        }
        self.walk_at(filter, now, visit);
        let inner = &self.0;
        if inner.index.read().wasteful() {
            inner.index.write().compact(&inner.log);
        }
        self.trim();
    }

    fn walk_at(&self, filter: &Filter, now: u64, mut visit: impl FnMut(Record) -> bool) {
        let inner = &self.0;
        let end = inner.log.tail_seq();
        let plan = inner.index.read().plan(filter);
        match plan {
            Plan::Empty => {}
            Plan::Scan => self.scan_live(end, now, |n| filter.matches(&n), visit),
            _ => {
                let mut batch = Vec::with_capacity(BATCH);
                let mut from = Seq(0);
                loop {
                    let index = inner.index.read();
                    index.candidates(&inner.log, filter, plan, from..end, BATCH, &mut batch);
                    drop(index);
                    for &seq in &batch {
                        let Some(rec) = inner.log.get(seq) else {
                            continue;
                        };
                        let r = rec.get();
                        let hit = r.state() == State::Live
                            && filter.matches(&r.note())
                            && !self.kill_if_dead(&rec, now);
                        if hit && !visit(rec) {
                            return;
                        }
                    }
                    if batch.len() < BATCH {
                        return;
                    }
                    from = Seq(batch.last().unwrap().0 + 1);
                }
            }
        }
    }

    /// Like [`walk`](Self::walk), over every record before `end`.
    fn scan_live(
        &self,
        end: Seq,
        now: u64,
        mut pred: impl FnMut(NoteRef<'_>) -> bool,
        mut visit: impl FnMut(Record) -> bool,
    ) {
        for rec in self.0.log.scan(Seq(0)) {
            if rec.seq() >= end {
                return;
            }
            let r = rec.get();
            let hit = r.state() == State::Live && pred(r.note()) && !self.kill_if_dead(&rec, now);
            if hit && !visit(rec) {
                return;
            }
        }
    }

    /// Whether a LIVE record is expired at `now`, killing it if so. The
    /// sweep kills most such records first; this catches the ones it
    /// couldn't because they were held, and expiries it hasn't reached.
    fn kill_if_dead(&self, rec: &Record, now: u64) -> bool {
        let inner = &self.0;
        let dead = inner.expiry.expired(&rec.note(), now);
        if dead {
            inner.log.retire(rec, State::Live, State::Dead);
        }
        dead
    }

    /// Leases up to `limit` queued items that `pred` accepts, oldest first,
    /// each for `ttl`. Items pushed while this runs are not considered.
    ///
    /// This walks every record in the log; [`lease`](Self::lease) uses the
    /// index.
    pub fn lease_where(
        &self,
        limit: usize,
        ttl: Duration,
        pred: impl FnMut(NoteRef<'_>) -> bool,
    ) -> Vec<Lease> {
        let end = self.0.log.tail_seq();
        let mut taken = Vec::new();
        if limit > 0 {
            self.scan_live(end, expire::now(), pred, |rec| {
                if rec.get().cas_state(State::Live, State::Leased) {
                    taken.push(rec);
                }
                taken.len() < limit
            });
            self.trim();
        }
        self.hold(taken, ttl)
    }

    /// Compacts every sealed segment that is mostly consumed: copies its
    /// queued items to the tail and kills the originals, so the segment
    /// drops. Leased items stay where they are until they are acked, or
    /// nacked and compacted later. Returns how many items moved.
    pub fn compact(&self) -> usize {
        self.compact_older(1)
    }

    /// Compacts the holey segments older than the newest `keep`.
    fn compact_older(&self, keep: usize) -> usize {
        let segs = self.0.log.segments();
        let old = &segs[..segs.len().saturating_sub(keep)];
        let moved = old
            .iter()
            .filter(|s| {
                let pending = s.pending() as u64;
                pending > 0 && pending * COMPACT_BELOW < s.count() as u64
            })
            .map(|s| self.compact_segment(s))
            .sum();
        self.trim();
        if moved > 0 {
            // A walk that ran while an item was between its two copies
            // missed it.
            self.0.queued.bump();
        }
        moved
    }

    /// Moves each LIVE item of `seg` to the tail. Returns how many moved.
    ///
    /// In two passes, so that no crash, power loss included, loses an
    /// item. The first holds each item (LIVE to LEASED), so no walk takes
    /// it while it is in two places, and appends its copy. Then the log is
    /// synced, and only after that does any original die: until a DEAD
    /// byte is on disk both copies are LIVE there, and reopening kills the
    /// original when the index sees the event's id again. If the sync
    /// fails, the copies die instead and the originals are queued again.
    fn compact_segment(&self, seg: &Segment) -> usize {
        let inner = &self.0;
        let now = expire::now();
        let _push = inner.push_lock.lock();
        let mut moves = Vec::new();
        for r in seg.records(0) {
            if r.state() != State::Live {
                continue;
            }
            let Some(rec) = inner.log.get(r.seq()) else {
                break;
            };
            // Not worth a copy.
            if self.kill_if_dead(&rec, now) || !r.cas_state(State::Live, State::Leased) {
                continue;
            }
            let note = r.note();
            let seq = match inner.log.append_note(note) {
                Ok(seq) => seq,
                Err(_) => {
                    r.cas_state(State::Leased, State::Live);
                    break;
                }
            };
            let copied = inner.index.write().insert(&note, seq, true);
            debug_assert_eq!(copied, Some(r.seq()), "the copy displaces the original");
            if let Some(at) = inner.expiry.of(&note) {
                inner.expiry.add(at, seq);
            }
            moves.push((rec, seq));
        }
        if moves.is_empty() {
            return 0;
        }
        // Under `Always` every append synced already.
        if inner.log.config().fsync != Fsync::Always && inner.log.sync().is_err() {
            for (rec, seq) in &moves {
                let copy = inner.log.get(*seq);
                if copy.is_some_and(|c| inner.log.retire(&c, State::Live, State::Dead)) {
                    rec.get().cas_state(State::Leased, State::Live);
                    inner.index.write().id_back(rec.note().id(), rec.seq());
                } else {
                    // A scan took the copy already: it is the one left.
                    inner.log.retire(rec, State::Leased, State::Dead);
                }
            }
            return 0;
        }
        for (rec, _) in &moves {
            inner.log.retire(rec, State::Leased, State::Dead);
        }
        moves.len()
    }

    /// Cuts the index back to the log's segments, if one was dropped since
    /// the last time.
    fn trim(&self) {
        let inner = &self.0;
        let dropped = inner.log.dropped();
        if dropped == inner.trimmed.load(Ordering::Acquire) {
            return;
        }
        let mut index = inner.index.write();
        if dropped != inner.trimmed.load(Ordering::Acquire) {
            // Read after `dropped`: a drop in between is trimmed now and
            // again next time, which is harmless.
            let live: Vec<u32> = inner.log.segments().iter().map(|s| s.id()).collect();
            index.trim(&live);
            inner.trimmed.store(dropped, Ordering::Release);
        }
    }

    /// Leases the item at `seq` for `ttl`, if it is still queued and not
    /// expired.
    pub fn lease_seq(&self, seq: Seq, ttl: Duration) -> Option<Lease> {
        let rec = self.0.log.get(seq)?;
        if rec.state() != State::Live || self.kill_if_dead(&rec, expire::now()) {
            self.trim();
            return None;
        }
        if !rec.get().cas_state(State::Live, State::Leased) {
            return None;
        }
        self.hold(vec![rec], ttl).pop()
    }

    fn hold(&self, records: Vec<Record>, ttl: Duration) -> Vec<Lease> {
        let deadline = Instant::now() + ttl;
        let tokens = self.0.leases.hold(&records, deadline);
        records
            .into_iter()
            .zip(tokens)
            .map(|(record, token)| Lease {
                noq: self.clone(),
                record,
                token,
                deadline,
            })
            .collect()
    }

    /// Requeues every lease past its deadline now, rather than when the
    /// reaper thread gets to it. Returns how many.
    pub fn reap(&self) -> usize {
        self.0.leases.reap(Instant::now())
    }

    /// How many leases are held.
    pub fn leased(&self) -> usize {
        self.0.leases.len()
    }
}

impl std::fmt::Debug for Noq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Noq")
            .field("log", &self.0.log)
            .field("leased", &self.leased())
            .finish()
    }
}

/// An item held out of the queue until [`deadline`](Self::deadline).
///
/// Dropping a lease without acking or nacking it leaves the item held until
/// the deadline, after which it is queued again.
pub struct Lease {
    noq: Noq,
    record: Record,
    token: u64,
    deadline: Instant,
}

impl Lease {
    pub fn seq(&self) -> Seq {
        self.record.seq()
    }

    /// The event's json, exactly as it was pushed.
    pub fn json(&self) -> &[u8] {
        self.record.json()
    }

    pub fn note(&self) -> NoteRef<'_> {
        self.record.note()
    }

    pub fn record(&self) -> &Record {
        &self.record
    }

    /// When the item goes back to the queue unless acked, nacked or
    /// extended first.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Consumes the item. Fails with [`Error::LeaseExpired`] if the lease
    /// ran out first; the item was then queued again.
    pub fn ack(self) -> Result<(), Error> {
        self.end(State::Acked)
    }

    /// Queues the item again now. Fails with [`Error::LeaseExpired`] if the
    /// lease ran out first.
    pub fn nack(self) -> Result<(), Error> {
        self.end(State::Live)
    }

    fn end(self, to: State) -> Result<(), Error> {
        let noq = &self.noq.0;
        if !noq.leases.release(self.seq(), self.token) {
            return Err(Error::LeaseExpired);
        }
        let moved = if to == State::Live {
            self.record.get().cas_state(State::Leased, State::Live)
        } else {
            noq.log.retire(&self.record, State::Leased, to)
        };
        debug_assert!(moved, "a released lease's record is still LEASED");
        if to == State::Live {
            noq.queued.bump();
        } else {
            self.noq.trim();
        }
        Ok(())
    }

    /// Holds the item for `ttl` from now instead. Fails with
    /// [`Error::LeaseExpired`] if the lease already ran out.
    pub fn extend(&mut self, ttl: Duration) -> Result<(), Error> {
        let deadline = Instant::now() + ttl;
        if !self.noq.0.leases.extend(self.seq(), self.token, deadline) {
            return Err(Error::LeaseExpired);
        }
        self.deadline = deadline;
        Ok(())
    }
}

impl std::fmt::Debug for Lease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lease")
            .field("seq", &self.seq())
            .field("deadline", &self.deadline)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_move_whose_sync_fails_is_undone() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            segment_size: 64 << 10,
            fsync: Fsync::Never,
            ..Config::default()
        };
        let q = Noq::open(dir.path(), config).unwrap();
        let data = std::fs::read("tests/data/many-events-sample.jsonl").unwrap();
        let mut first = Vec::new();
        for line in data.split(|&c| c == b'\n') {
            if q.log().segments().len() == 2 {
                break;
            }
            if let Ok(Pushed::Queued(seq)) = q.push(line) {
                first.push((seq, line));
            }
        }
        first.retain(|(s, _)| s.segment() == 0);
        for &(s, _) in &first[2..] {
            q.lease_seq(s, Duration::from_secs(600))
                .unwrap()
                .ack()
                .unwrap();
        }
        let state = |s: Seq| q.log().get(s).map(|r| r.state());
        let tail = q.log().tail_seq();

        q.log().fail_sync.store(true, Ordering::Relaxed);
        assert_eq!(q.compact(), 0);
        // The originals are queued again, the copies dead, and the index
        // points at the originals.
        for &(s, json) in &first[..2] {
            assert_eq!(state(s), Some(State::Live));
            assert_eq!(q.push(json).unwrap(), Pushed::Duplicate(s));
        }
        let copies: Vec<Seq> = q.log().scan(tail).map(|r| r.seq()).collect();
        assert_eq!(copies.len(), 2);
        assert!(copies.iter().all(|&s| state(s) == Some(State::Dead)));
        let queued: Vec<Seq> = q
            .peek(&Filter::new(), usize::MAX)
            .iter()
            .map(|r| r.seq())
            .filter(|s| s.segment() == 0)
            .collect();
        assert_eq!(queued, [first[0].0, first[1].0]);

        // The next try moves them.
        assert_eq!(q.compact(), 2);
        for &(s, json) in &first[..2] {
            assert_eq!(state(s), None, "segment 0 drained and dropped");
            assert!(matches!(q.push(json).unwrap(), Pushed::Duplicate(c) if c > copies[1]));
        }
    }
}
