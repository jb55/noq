//! The signal [`Noq::wait_lease`](crate::Noq::wait_lease) sleeps on: a
//! generation counter bumped whenever an item may have become leasable, by
//! a push that queued one, a nack, or a lease running out.
//!
//! A waiter reads the generation, tries to lease, and sleeps only while the
//! generation is still the one it read, so a bump between its lease and its
//! sleep is never missed. Bumping costs one atomic add while nobody waits:
//! the lock and the condvar are only touched when someone does.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::time::Instant;

use parking_lot::{Condvar, Mutex};

#[derive(Default)]
pub(crate) struct Signal {
    generation: AtomicU64,
    waiters: AtomicUsize,
    lock: Mutex<()>,
    wake: Condvar,
}

impl Signal {
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(SeqCst)
    }

    /// Wakes every waiter.
    pub(crate) fn bump(&self) {
        self.generation.fetch_add(1, SeqCst);
        // A waiter counted after this load reads the new generation before
        // it sleeps; one counted before it is woken here.
        if self.waiters.load(SeqCst) > 0 {
            let _l = self.lock.lock();
            self.wake.notify_all();
        }
    }

    /// Sleeps until the generation is past `seen` or `until` passes,
    /// whichever is first; `None` sleeps for the bump alone. Returns
    /// whether the generation moved.
    pub(crate) fn wait(&self, seen: u64, until: Option<Instant>) -> bool {
        self.waiters.fetch_add(1, SeqCst);
        let mut l = self.lock.lock();
        let moved = loop {
            if self.generation.load(SeqCst) != seen {
                break true;
            }
            match until {
                Some(at) => {
                    if self.wake.wait_until(&mut l, at).timed_out() {
                        break self.generation.load(SeqCst) != seen;
                    }
                }
                None => self.wake.wait(&mut l),
            }
        };
        drop(l);
        self.waiters.fetch_sub(1, SeqCst);
        moved
    }
}
