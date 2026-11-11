//! Replaceable and addressable versions, expiry, and the seen ring.

mod common;

use std::time::{Duration, SystemTime};

use common::Signer;
use noq::{Config, Filter, Fsync, Log, Noq, Pushed, Seq, State};

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
fn newer_replaceable_kills_the_older_and_an_older_is_stale() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let s = Signer::new(1);
    let v1 = queued(q.push(&s.event(100, 0, &[], "v1")).unwrap());
    let Pushed::Replaced { seq: v2, old } = q.push(&s.event(200, 0, &[], "v2")).unwrap() else {
        panic!("v2 didn't replace v1")
    };
    assert_eq!(old, v1);
    assert_eq!(state(&q, v1), State::Dead);
    assert_eq!(peek(&q, r#"{"kinds":[0]}"#), [v2]);

    // Older than what is queued: dropped.
    assert_eq!(
        q.push(&s.event(150, 0, &[], "late")).unwrap(),
        Pushed::Stale(v2)
    );
    // Another author's, and another replaceable kind, are their own slots.
    queued(q.push(&Signer::new(2).event(50, 0, &[], "")).unwrap());
    queued(q.push(&s.event(50, 3, &[], "")).unwrap());
    queued(q.push(&s.event(50, 10002, &[], "")).unwrap());
    assert_eq!(peek(&q, r#"{"kinds":[0,3,10002]}"#).len(), 4);
}

#[test]
fn a_created_at_tie_goes_to_the_lower_id() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let s = Signer::new(1);
    let mut evs = [s.event(100, 0, &[], "a"), s.event(100, 0, &[], "b")];
    let id = |e: &[u8]| serde_json::from_slice::<serde_json::Value>(e).unwrap()["id"].clone();
    evs.sort_by_key(|e| id(e).as_str().unwrap().to_owned());
    // The higher id first: the lower one replaces it.
    let hi = queued(q.push(&evs[1]).unwrap());
    let lo = queued(q.push(&evs[0]).unwrap());
    assert_eq!(state(&q, hi), State::Dead);
    assert_eq!(state(&q, lo), State::Live);

    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let lo2 = queued(q.push(&evs[0]).unwrap());
    assert_eq!(q.push(&evs[1]).unwrap(), Pushed::Stale(lo2));
    assert_eq!(state(&q, lo2), State::Live);
}

#[test]
fn addressable_versions_compete_per_d_tag() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let s = Signer::new(1);
    let x1 = queued(q.push(&s.event(100, 30023, &[&["d", "x"]], "")).unwrap());
    let y1 = queued(q.push(&s.event(100, 30023, &[&["d", "y"]], "")).unwrap());
    let none = queued(q.push(&s.event(100, 30023, &[], "")).unwrap());
    let x2 = q.push(&s.event(200, 30023, &[&["d", "x"]], "")).unwrap();
    assert!(matches!(x2, Pushed::Replaced { old, .. } if old == x1));
    // No d tag and a d tag with no value are both the empty d.
    let empty = q.push(&s.event(200, 30023, &[&["d"]], "")).unwrap();
    assert!(matches!(empty, Pushed::Replaced { old, .. } if old == none));
    assert_eq!(state(&q, y1), State::Live);
    assert_eq!(
        peek(&q, r#"{"kinds":[30023]}"#),
        [y1, x2.queued().unwrap(), empty.queued().unwrap()]
    );
}

#[test]
fn ephemeral_and_regular_kinds_are_all_queued() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let s = Signer::new(1);
    for kind in [1, 20001] {
        queued(q.push(&s.event(100, kind, &[], "a")).unwrap());
        queued(q.push(&s.event(200, kind, &[], "b")).unwrap());
    }
    assert_eq!(peek(&q, "{}").len(), 4);
}

#[test]
fn a_held_version_is_left_to_its_holder_and_dies_if_it_comes_back() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let s = Signer::new(1);
    let v1 = queued(q.push(&s.event(100, 0, &[], "v1")).unwrap());
    let held = q.lease_seq(v1, LONG).unwrap();
    // Taken, so not replaced: both are delivered.
    let v2 = queued(q.push(&s.event(200, 0, &[], "v2")).unwrap());
    assert_eq!(
        q.push(&s.event(200, 0, &[], "v2")).unwrap(),
        Pushed::Duplicate(v2)
    );
    assert_eq!(state(&q, v1), State::Leased);
    held.nack().unwrap();
    // Back in the queue, but the first walk past it kills it.
    assert_eq!(state(&q, v1), State::Live);
    assert_eq!(peek(&q, r#"{"kinds":[0]}"#), [v2]);
    assert_eq!(state(&q, v1), State::Dead);
    assert!(q.lease_seq(v1, LONG).is_none());
}

#[test]
fn a_consumed_newer_version_still_makes_an_older_one_stale() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let s = Signer::new(1);
    let v2 = queued(q.push(&s.event(200, 0, &[], "v2")).unwrap());
    q.lease_seq(v2, LONG).unwrap().ack().unwrap();
    assert_eq!(
        q.push(&s.event(100, 0, &[], "v1")).unwrap(),
        Pushed::Stale(v2)
    );
    drop(q);
    let q = Noq::open(dir.path(), small()).unwrap();
    assert_eq!(
        q.push(&s.event(100, 0, &[], "v1")).unwrap(),
        Pushed::Stale(v2)
    );
}

#[test]
fn reopen_kills_versions_left_live_in_either_order() {
    let dir = tempfile::tempdir().unwrap();
    let s = Signer::new(1);
    // Appended to the log directly, as if a push died between appending a
    // newer version and killing the older.
    let (old_first, new_second, new_first, old_second) = {
        let log = Log::open(dir.path(), small()).unwrap();
        (
            log.append(&s.event(100, 0, &[], "")).unwrap(),
            log.append(&s.event(200, 0, &[], "")).unwrap(),
            log.append(&s.event(200, 30000, &[&["d", "a"]], ""))
                .unwrap(),
            log.append(&s.event(100, 30000, &[&["d", "a"]], ""))
                .unwrap(),
        )
    };
    let q = Noq::open(dir.path(), small()).unwrap();
    assert_eq!(state(&q, old_first), State::Dead);
    assert_eq!(state(&q, old_second), State::Dead);
    assert_eq!(peek(&q, "{}"), [new_second, new_first]);
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
