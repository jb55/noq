//! Expiry: when each pending record stops being worth delivering, and the
//! sweep that kills the ones whose time has come.
//!
//! A record expires at its NIP-40 `expiration` tag, or at `created_at` plus
//! the queue's `max_age`, whichever is first. Times are unix seconds, and a
//! record is expired from its expiry second on.
//!
//! Lease walks check each candidate's expiry in place, so an expired item
//! is never handed out. This set exists so expired records die even when
//! nobody walks past them, and their segments can drop.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use parking_lot::Mutex;

use crate::log::Log;
use crate::note::NoteRef;
use crate::segment::{Seq, State};

/// The unix time now, in seconds.
pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

pub(crate) struct Expiry {
    max_age: Option<u64>,
    /// `(expires_at, seq)` of pending records that expire, soonest first.
    /// Entries for records consumed some other way stay until their time.
    due: Mutex<BTreeSet<(u64, Seq)>>,
    /// The first entry's time, `u64::MAX` when there is none, so checking
    /// whether a sweep has work takes no lock.
    next: AtomicU64,
}

impl Expiry {
    pub(crate) fn new(max_age: Option<Duration>) -> Expiry {
        Expiry {
            max_age: max_age.map(|d| d.as_secs()),
            due: Mutex::new(BTreeSet::new()),
            next: AtomicU64::new(u64::MAX),
        }
    }

    /// When `note` expires, if it does.
    pub(crate) fn of(&self, note: &NoteRef<'_>) -> Option<u64> {
        self.at(note.created_at(), note.expiration())
    }

    /// When an event created at `created_at`, with this NIP-40
    /// `expiration` tag, expires, if it does.
    pub(crate) fn at(&self, created_at: u64, expiration: Option<u64>) -> Option<u64> {
        let aged = self.max_age.map(|a| created_at.saturating_add(a));
        match (expiration, aged) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Whether `note` is expired at `now`.
    pub(crate) fn expired(&self, note: &NoteRef<'_>, now: u64) -> bool {
        self.of(note).is_some_and(|at| at <= now)
    }

    /// Schedules the pending record at `seq` to die at `at`.
    pub(crate) fn add(&self, at: u64, seq: Seq) {
        let mut due = self.due.lock();
        due.insert((at, seq));
        self.next.fetch_min(at, Ordering::Relaxed);
    }

    /// Whether some scheduled record's time is at or before `now`.
    pub(crate) fn is_due(&self, now: u64) -> bool {
        self.next.load(Ordering::Relaxed) <= now
    }

    /// The unix second the next sweep has work, if any record is
    /// scheduled.
    pub(crate) fn next(&self) -> Option<u64> {
        Some(self.next.load(Ordering::Relaxed)).filter(|&at| at != u64::MAX)
    }

    /// Kills every LIVE record whose time is at or before `now`. Returns
    /// how many. A LEASED one is left to its holder and looked at again a
    /// second later: an ack consumes it, and a nack puts it where the next
    /// sweep, or the next walk past it, kills it.
    pub(crate) fn sweep(&self, log: &Log, now: u64) -> usize {
        let mut due = self.due.lock();
        let mut killed = 0;
        let mut held = Vec::new();
        while let Some(&(at, seq)) = due.first() {
            if at > now {
                break;
            }
            due.pop_first();
            let Some(rec) = log.get(seq) else {
                continue;
            };
            match rec.state() {
                State::Live if log.retire(&rec, State::Live, State::Dead) => killed += 1,
                // Taken between the two reads.
                State::Live | State::Leased => held.push(seq),
                _ => {}
            }
        }
        due.extend(held.into_iter().map(|seq| (now + 1, seq)));
        let next = due.first().map_or(u64::MAX, |&(at, _)| at);
        self.next.store(next, Ordering::Relaxed);
        killed
    }

    /// Records scheduled, for stats and tests.
    pub(crate) fn len(&self) -> usize {
        self.due.lock().len()
    }
}
