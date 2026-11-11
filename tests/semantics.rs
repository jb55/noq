//! Replaceable and addressable versions, expiry, and the seen ring.

mod common;

use std::time::{Duration, SystemTime};

use common::Signer;
use noq::{Config, Filter, Fsync, Noq, Pushed, Seq, State};

const LONG: Duration = Duration::from_secs(600);

fn small() -> Config {
    Config {
        segment_size: 64 << 10,
        fsync: Fsync::Never,
        ..Config::default()
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn queued(p: Pushed) -> Seq {
    p.queued().unwrap_or_else(|| panic!("not queued: {p:?}"))
}

fn state(q: &Noq, seq: Seq) -> State {
    q.log().get(seq).unwrap().state()
}

fn peek(q: &Noq, json: &str) -> Vec<Seq> {
    let f = Filter::from_json(json.as_bytes()).unwrap();
    q.peek(&f, usize::MAX).iter().map(|r| r.seq()).collect()
}

#[test]
fn every_version_of_a_replaceable_or_addressable_event_is_queued() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let s = Signer::new(1);
    // Newer first: the older one pushed after it is queued too.
    let v2 = queued(q.push(&s.event(200, 0, &[], "v2")).unwrap());
    let v1 = queued(q.push(&s.event(100, 0, &[], "v1")).unwrap());
    let x2 = queued(q.push(&s.event(200, 30023, &[&["d", "x"]], "")).unwrap());
    let x1 = queued(q.push(&s.event(100, 30023, &[&["d", "x"]], "")).unwrap());
    // Ephemeral and regular kinds, as ever.
    let e = queued(q.push(&s.event(100, 20001, &[], "")).unwrap());
    let r = queued(q.push(&s.event(100, 1, &[], "")).unwrap());
    // Still deduplicated by id.
    assert_eq!(
        q.push(&s.event(100, 0, &[], "v1")).unwrap(),
        Pushed::Duplicate(v1)
    );
    let leased: Vec<Seq> = q
        .lease(&Filter::new(), usize::MAX, LONG)
        .iter()
        .map(|l| l.seq())
        .collect();
    assert_eq!(leased, [v2, v1, x2, x1, e, r]);
}

#[test]
fn reopen_keeps_every_version() {
    let dir = tempfile::tempdir().unwrap();
    let s = Signer::new(1);
    let seqs = {
        let q = Noq::open(dir.path(), small()).unwrap();
        [
            queued(q.push(&s.event(200, 0, &[], "")).unwrap()),
            queued(q.push(&s.event(100, 0, &[], "")).unwrap()),
            queued(q.push(&s.event(100, 30000, &[&["d", "a"]], "")).unwrap()),
            queued(q.push(&s.event(200, 30000, &[&["d", "a"]], "")).unwrap()),
        ]
    };
    let q = Noq::open(dir.path(), small()).unwrap();
    assert_eq!(peek(&q, "{}"), seqs);
}

#[test]
fn expired_events_are_refused_then_killed_when_their_time_comes() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let s = Signer::new(1);
    let exp = |t: u64| t.to_string();
    let t = now();
    assert_eq!(
        q.push(&s.event(1, 1, &[&["expiration", &exp(t - 1)]], ""))
            .unwrap(),
        Pushed::Expired
    );
    // A malformed expiration is no expiration.
    queued(
        q.push(&s.event(2, 1, &[&["expiration", "soon"]], ""))
            .unwrap(),
    );
    queued(
        q.push(&s.event(3, 1, &[&["expiration", &exp(t + 3600)]], ""))
            .unwrap(),
    );
    let swept = queued(
        q.push(&s.event(4, 1, &[&["expiration", &exp(t + 1)]], ""))
            .unwrap(),
    );
    let walked = queued(
        q.push(&s.event(5, 1, &[&["expiration", &exp(t + 1)]], ""))
            .unwrap(),
    );
    let held = q.lease_seq(walked, LONG).unwrap();
    assert_eq!(q.index_stats().expiring, 3);
    assert_eq!(peek(&q, "{}").len(), 3);

    while now() <= t + 1 {
        std::thread::sleep(Duration::from_millis(50));
    }
    // The sweep kills the queued one and leaves the held one alone.
    assert_eq!(q.expire(), 1);
    assert_eq!(state(&q, swept), State::Dead);
    assert_eq!(state(&q, walked), State::Leased);
    held.nack().unwrap();
    // Back, expired: never handed out again.
    assert_eq!(peek(&q, "{}").len(), 2);
    assert!(
        q.lease(&Filter::new(), 9, LONG)
            .iter()
            .all(|l| l.seq() != walked)
    );
    assert_eq!(state(&q, walked), State::Dead);
}

#[test]
fn max_age_expires_by_created_at_including_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let config = |secs| Config {
        max_age: Some(Duration::from_secs(secs)),
        ..small()
    };
    let q = Noq::open(dir.path(), config(60)).unwrap();
    let s = Signer::new(1);
    let t = now();
    assert_eq!(
        q.push(&s.event(t - 120, 1, &[], "")).unwrap(),
        Pushed::Expired
    );
    let young = queued(q.push(&s.event(t, 1, &[], "")).unwrap());
    let mid = queued(q.push(&s.event(t - 30, 1, &[], "")).unwrap());
    let tagged = q
        .push(&s.event(t, 1, &[&["expiration", &(t + 10).to_string()]], ""))
        .unwrap();
    assert_eq!(q.index_stats().expiring, 3);
    drop(q);

    // A shorter max age on reopen kills what is now too old.
    let q = Noq::open(dir.path(), config(15)).unwrap();
    assert_eq!(state(&q, mid), State::Dead);
    assert_eq!(peek(&q, "{}"), [young, tagged.queued().unwrap()]);
}

#[test]
fn the_seen_ring_remembers_ids_whose_records_were_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let config = |seen_ids| Config {
        segment_size: 4096,
        seen_ids,
        ..small()
    };
    let q = Noq::open(dir.path(), config(3)).unwrap();
    let s = Signer::new(1);
    let pad = "x".repeat(1200);
    let evs: Vec<Vec<u8>> = (0..12).map(|i| s.event(i, 1, &[], &pad)).collect();
    let seqs: Vec<Seq> = evs.iter().map(|e| queued(q.push(e).unwrap())).collect();
    for l in q.lease(&Filter::new(), usize::MAX, LONG) {
        l.ack().unwrap();
    }
    // Every segment but the tail is gone, and with it the records.
    assert!(q.log().get(seqs[0]).is_none());
    let stats = q.index_stats();
    assert_eq!(stats.seen, 3);
    assert!(stats.ids < evs.len());
    let seen = evs
        .iter()
        .filter(|e| q.push(e).unwrap() == Pushed::Seen)
        .count();
    assert_eq!(seen, 3);
    drop(q);
    // In memory only.
    let q = Noq::open(dir.path(), config(3)).unwrap();
    assert_eq!(q.index_stats().seen, 0);
    drop(q);

    // Off at zero: a dropped id is pushed again like a new one.
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), config(0)).unwrap();
    let first = queued(q.push(&evs[0]).unwrap());
    for e in &evs[1..] {
        queued(q.push(e).unwrap());
    }
    for l in q.lease(&Filter::new(), usize::MAX, LONG) {
        l.ack().unwrap();
    }
    assert!(q.log().get(first).is_none());
    assert_eq!(q.index_stats().seen, 0);
    queued(q.push(&evs[0]).unwrap());
}
