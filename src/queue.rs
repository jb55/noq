//! [`Noq`]: the queue. Events are pushed in, consumers lease them, and an
//! ack consumes them for good.
//!
//! A lease holds an item out of the queue until its deadline. `ack` marks it
//! ACKED, `nack` puts it back now, and a lease that runs out (or is dropped
//! without either) is put back by the reaper thread at its deadline, so an
//! item is delivered at least once.

use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::error::Error;
use crate::lease::Leases;
use crate::log::{Config, Log, Record};
use crate::note::NoteRef;
use crate::segment::{Seq, State};

/// A queue over a [`Log`] directory. Cloning it is cheap; clones share the
/// queue.
#[derive(Clone)]
pub struct Noq(Arc<Inner>);

struct Inner {
    log: Log,
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
    /// Opens the queue in `dir`. Items leased when the last process exited
    /// are queued again.
    pub fn open(dir: impl AsRef<Path>, config: Config) -> Result<Noq, Error> {
        let log = Log::open(dir, config)?;
        let leases = Leases::new();
        let reaper = Some(leases.spawn_reaper()?);
        Ok(Noq(Arc::new(Inner {
            log,
            leases,
            reaper,
        })))
    }

    pub fn log(&self) -> &Log {
        &self.0.log
    }

    /// Parses, verifies and queues one event.
    pub fn push(&self, json: &[u8]) -> Result<Seq, Error> {
        self.0.log.append(json)
    }

    /// Leases up to `limit` queued items that `pred` accepts, oldest first,
    /// each for `ttl`. Items pushed while this runs are not considered.
    ///
    /// This walks every record in the log; index-driven leasing builds on
    /// [`lease_seq`](Self::lease_seq).
    pub fn lease_where(
        &self,
        limit: usize,
        ttl: Duration,
        mut pred: impl FnMut(NoteRef<'_>) -> bool,
    ) -> Vec<Lease> {
        let end = self.0.log.tail_seq();
        let mut taken = Vec::new();
        for rec in self.0.log.scan(Seq(0)) {
            if taken.len() >= limit || rec.seq() >= end {
                break;
            }
            let r = rec.get();
            if r.state() == State::Live && pred(r.note()) && r.cas_state(State::Live, State::Leased)
            {
                taken.push(rec);
            }
        }
        self.hold(taken, ttl)
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
