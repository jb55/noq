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

use std::cell::RefCell;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::builder::NoteBuilder;
use crate::error::Error;
use crate::filter::Filter;
use crate::index::{Index, Plan};
use crate::lease::Leases;
use crate::log::{Config, Log, Record};
use crate::note::NoteRef;
use crate::segment::{Seq, State};

/// Candidates read from the index per lock: a walk holds the index's read
/// lock only while it copies one batch, so pushes wait on that, not on the
/// whole lease.
const BATCH: usize = 256;

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
}

impl Pushed {
    pub fn seq(self) -> Seq {
        match self {
            Pushed::Queued(s) | Pushed::Duplicate(s) => s,
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
    /// leased when the last process exited are queued again.
    pub fn open(dir: impl AsRef<Path>, config: Config) -> Result<Noq, Error> {
        let log = Log::open(dir, config)?;
        let mut index = Index::default();
        for rec in log.scan(Seq(0)) {
            let r = rec.get();
            let pending = matches!(r.state(), State::Live | State::Leased);
            index.insert(&r.note(), rec.seq(), pending);
        }
        let trimmed = AtomicU64::new(log.dropped());
        let leases = Leases::new();
        let reaper = Some(leases.spawn_reaper()?);
        Ok(Noq(Arc::new(Inner {
            log,
            index: RwLock::new(index),
            push_lock: Mutex::new(()),
            trimmed,
            leases,
            reaper,
        })))
    }

    /// The log under the queue. Appending to it directly bypasses the
    /// index and the dedup check: such records are found only by
    /// [`lease_where`](Self::lease_where) until the queue is reopened.
    pub fn log(&self) -> &Log {
        &self.0.log
    }

    /// Parses, verifies and queues one event, unless an event with its id
    /// is already in the log.
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
        let pushed = {
            let _push = inner.push_lock.lock();
            if let Some(seq) = inner.index.read().id(note.id()) {
                return Ok(Pushed::Duplicate(seq));
            }
            let seq = inner.log.append_note(note)?;
            inner.index.write().insert(&note, seq, true);
            Pushed::Queued(seq)
        };
        // The append may have sealed a drained tail and dropped it.
        self.trim();
        Ok(pushed)
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
        }
    }

    /// Calls `visit` with each LIVE record matching `filter` that was in the
    /// log when the walk started, in seq order, until it returns false.
    fn walk(&self, filter: &Filter, mut visit: impl FnMut(Record) -> bool) {
        let inner = &self.0;
        let end = inner.log.tail_seq();
        let plan = inner.index.read().plan(filter);
        match plan {
            Plan::Empty => {}
            Plan::Scan => self.scan_live(end, |n| filter.matches(&n), visit),
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
                        let hit = r.state() == State::Live && filter.matches(&r.note());
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
        mut pred: impl FnMut(NoteRef<'_>) -> bool,
        mut visit: impl FnMut(Record) -> bool,
    ) {
        for rec in self.0.log.scan(Seq(0)) {
            if rec.seq() >= end {
                return;
            }
            let r = rec.get();
            if r.state() == State::Live && pred(r.note()) && !visit(rec) {
                return;
            }
        }
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
            self.scan_live(end, pred, |rec| {
                if rec.get().cas_state(State::Live, State::Leased) {
                    taken.push(rec);
                }
                taken.len() < limit
            });
        }
        self.hold(taken, ttl)
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

    /// Leases the item at `seq` for `ttl`, if it is still queued.
    pub fn lease_seq(&self, seq: Seq, ttl: Duration) -> Option<Lease> {
        let rec = self.0.log.get(seq)?;
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
        if to != State::Live {
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
