//! The lease table: which LEASED records are held, by which lease, until
//! when, and the reaper thread that puts expired ones back to LIVE.
//!
//! Taking a record is a lock-free CAS on its state byte, LIVE→LEASED, so two
//! consumers can never both win it. Every move *out of* LEASED (ack, nack,
//! expiry) happens only after removing the record's entry from this table
//! under its lock, and only for the lease whose token the entry holds. A
//! lease that expired and was handed to someone else can therefore no
//! longer ack or nack it.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

use parking_lot::{Condvar, Mutex};

use crate::log::Record;
use crate::segment::{Seq, State};

pub(crate) struct Leases {
    table: Mutex<Table>,
    /// Signalled when the earliest deadline may have moved up, or on stop.
    wake: Condvar,
}

struct Table {
    held: HashMap<Seq, Held>,
    /// Every held lease's deadline, earliest first.
    deadlines: BTreeSet<(Instant, Seq)>,
    next_token: u64,
    stop: bool,
}

struct Held {
    token: u64,
    deadline: Instant,
    record: Record,
}

impl Leases {
    pub(crate) fn new() -> Arc<Leases> {
        Arc::new(Leases {
            table: Mutex::new(Table {
                held: HashMap::new(),
                deadlines: BTreeSet::new(),
                next_token: 1,
                stop: false,
            }),
            wake: Condvar::new(),
        })
    }

    /// Starts the reaper thread. It runs until [`stop`](Self::stop).
    pub(crate) fn spawn_reaper(self: &Arc<Leases>) -> std::io::Result<JoinHandle<()>> {
        let leases = self.clone();
        std::thread::Builder::new()
            .name("noq-reaper".into())
            .spawn(move || leases.run_reaper())
    }

    fn run_reaper(&self) {
        let mut t = self.table.lock();
        while !t.stop {
            t.reap(Instant::now());
            match t.deadlines.first() {
                Some(&(at, _)) => {
                    self.wake.wait_until(&mut t, at);
                }
                None => self.wake.wait(&mut t),
            }
        }
    }

    pub(crate) fn stop(&self) {
        self.table.lock().stop = true;
        self.wake.notify_all();
    }

    /// Registers records the caller just moved LIVE→LEASED, each held until
    /// `deadline`. Returns their tokens, in order.
    pub(crate) fn hold(&self, records: &[Record], deadline: Instant) -> Vec<u64> {
        let mut t = self.table.lock();
        let earliest = t.deadlines.first().map(|&(at, _)| at);
        let tokens = records
            .iter()
            .map(|r| {
                let token = t.next_token;
                t.next_token += 1;
                t.insert(r.clone(), token, deadline);
                token
            })
            .collect();
        drop(t);
        if !records.is_empty() && earliest.is_none_or(|e| deadline < e) {
            self.wake.notify_all();
        }
        tokens
    }

    /// Ends the lease `token` holds on `seq`, if it still holds it. The
    /// record stays LEASED: the caller moves it out, and nobody else can.
    pub(crate) fn release(&self, seq: Seq, token: u64) -> bool {
        let mut t = self.table.lock();
        match t.held.get(&seq) {
            Some(h) if h.token == token => {
                let h = t.held.remove(&seq).unwrap();
                t.deadlines.remove(&(h.deadline, seq));
                true
            }
            _ => false,
        }
    }

    /// Moves the deadline of the lease `token` holds on `seq`, if it still
    /// holds it.
    pub(crate) fn extend(&self, seq: Seq, token: u64, deadline: Instant) -> bool {
        let mut t = self.table.lock();
        let Some(h) = t.held.get_mut(&seq).filter(|h| h.token == token) else {
            return false;
        };
        let old = std::mem::replace(&mut h.deadline, deadline);
        t.deadlines.remove(&(old, seq));
        t.deadlines.insert((deadline, seq));
        let earliest = t
            .deadlines
            .first()
            .is_some_and(|&(at, s)| at == deadline && s == seq);
        drop(t);
        if earliest {
            self.wake.notify_all();
        }
        true
    }

    /// Requeues every lease whose deadline is at or before `now`. Returns
    /// how many.
    pub(crate) fn reap(&self, now: Instant) -> usize {
        self.table.lock().reap(now)
    }

    /// How many leases are held.
    pub(crate) fn len(&self) -> usize {
        self.table.lock().held.len()
    }
}

impl Table {
    fn insert(&mut self, record: Record, token: u64, deadline: Instant) {
        let seq = record.seq();
        self.deadlines.insert((deadline, seq));
        let prev = self.held.insert(
            seq,
            Held {
                token,
                deadline,
                record,
            },
        );
        debug_assert!(prev.is_none(), "a record leased twice");
    }

    fn reap(&mut self, now: Instant) -> usize {
        let mut n = 0;
        while let Some(&(at, seq)) = self.deadlines.first() {
            if at > now {
                break;
            }
            self.deadlines.pop_first();
            let h = self.held.remove(&seq).expect("a deadline has its lease");
            let moved = h.record.get().cas_state(State::Leased, State::Live);
            debug_assert!(moved, "only the table moves a record out of LEASED");
            n += 1;
        }
        n
    }
}
