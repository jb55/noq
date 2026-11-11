//! wait_lease: blocking until something matches, and what wakes it.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use common::Signer;
use noq::{Config, Filter, Fsync, Noq, State};

const LONG: Duration = Duration::from_secs(600);
/// Long enough that a waiter which isn't woken fails the test by timing out
/// rather than hanging it.
const PATIENT: Duration = Duration::from_secs(20);

fn small() -> Config {
    Config {
        segment_size: 64 << 10,
        fsync: Fsync::Never,
        ..Config::default()
    }
}

fn kinds(k: u32) -> Filter {
    Filter::new().kinds([k])
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Runs `f` on another thread once `q` has had time to start waiting.
fn later(f: impl FnOnce() + Send + 'static) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        f()
    })
}

#[test]
fn returns_at_once_when_something_matches() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let s = Signer::new(1);
    q.push(&s.event(1, 1, &[], "a")).unwrap();
    let t = Instant::now();
    let got = q.wait_lease(&kinds(1), 10, LONG, PATIENT);
    assert_eq!(got.len(), 1);
    assert!(t.elapsed() < Duration::from_secs(1));
}

#[test]
fn times_out_empty() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let t = Instant::now();
    assert!(
        q.wait_lease(&kinds(1), 10, LONG, Duration::from_millis(150))
            .is_empty()
    );
    assert!(t.elapsed() >= Duration::from_millis(150));
    // A zero limit never waits.
    let t = Instant::now();
    assert!(q.wait_lease(&kinds(1), 0, LONG, PATIENT).is_empty());
    assert!(
        q.wait_lease(&kinds(1).limit(0), 5, LONG, PATIENT)
            .is_empty()
    );
    assert!(t.elapsed() < Duration::from_secs(1));
}

#[test]
fn a_push_wakes_it_and_a_non_matching_one_does_not_satisfy_it() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let s = Signer::new(1);
    let wanted = s.event(2, 7, &[], "wanted");
    let pusher = {
        let q = q.clone();
        let other = s.event(1, 1, &[], "other");
        later(move || {
            q.push(&other).unwrap();
            thread::sleep(Duration::from_millis(100));
            q.push(&wanted).unwrap();
        })
    };
    let t = Instant::now();
    let got = q.wait_lease(&kinds(7), 10, LONG, PATIENT);
    pusher.join().unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].note().kind(), 7);
    assert!(
        t.elapsed() < PATIENT / 2,
        "woken by the push, not the timeout"
    );
    // The other push is still queued for someone else.
    assert_eq!(q.peek(&kinds(1), 10).len(), 1);
}

#[test]
fn a_nack_wakes_it() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    q.push(&Signer::new(1).event(1, 1, &[], "a")).unwrap();
    let held = q.lease(&kinds(1), 1, LONG).pop().unwrap();
    let nacker = later(move || held.nack().unwrap());
    let got = q.wait_lease(&kinds(1), 1, LONG, PATIENT);
    nacker.join().unwrap();
    assert_eq!(got.len(), 1);
}

#[test]
fn a_lease_running_out_wakes_it() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    q.push(&Signer::new(1).event(1, 1, &[], "a")).unwrap();
    let held = q.lease(&kinds(1), 1, Duration::from_millis(100));
    assert_eq!(held.len(), 1);
    // The reaper thread requeues it and wakes the waiter.
    let t = Instant::now();
    let got = q.wait_lease(&kinds(1), 1, LONG, PATIENT);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].seq(), held[0].seq());
    assert!(t.elapsed() < PATIENT / 2);
}

#[test]
fn waiters_share_the_pushes_without_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    const N: usize = 200;
    let got = Arc::new(AtomicUsize::new(0));
    let consumers: Vec<_> = (0..4)
        .map(|_| {
            let (q, got) = (q.clone(), got.clone());
            thread::spawn(move || {
                let mut mine = Vec::new();
                while got.load(Ordering::SeqCst) < N {
                    let leases = q.wait_lease(&kinds(1), 3, LONG, Duration::from_millis(50));
                    got.fetch_add(leases.len(), Ordering::SeqCst);
                    for l in leases {
                        mine.push(l.seq());
                        l.ack().unwrap();
                    }
                }
                mine
            })
        })
        .collect();
    let s = Signer::new(1);
    for i in 0..N {
        q.push(&s.event(i as u64, 1, &[], "")).unwrap();
        if i % 16 == 0 {
            thread::yield_now();
        }
    }
    let mut all: Vec<_> = consumers
        .into_iter()
        .flat_map(|c| c.join().unwrap())
        .collect();
    assert_eq!(all.len(), N);
    all.sort();
    all.dedup();
    assert_eq!(all.len(), N, "an item went to two waiters");
}

#[test]
fn waiting_sweeps_expired_items_when_nobody_pushes() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let t = now();
    let seq = q
        .push(&Signer::new(1).event(t, 1, &[&["expiration", &(t + 1).to_string()]], ""))
        .unwrap()
        .queued()
        .unwrap();
    assert_eq!(q.index_stats().expiring, 1);
    // Nobody wants kind 1; the waiter wakes for the expiry anyway.
    assert!(
        q.wait_lease(&kinds(2), 1, LONG, Duration::from_millis(2500))
            .is_empty()
    );
    assert_eq!(q.index_stats().expiring, 0);
    assert_eq!(q.log().get(seq).unwrap().state(), State::Dead);
}
