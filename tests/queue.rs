use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use noq::{Config, Error, Fsync, Lease, Log, Noq, Segment, Seq, State};

fn events() -> Vec<Vec<u8>> {
    std::fs::read("tests/data/many-events-sample.jsonl")
        .unwrap()
        .split(|&c| c == b'\n')
        .filter(|l| !l.is_empty())
        .map(<[u8]>::to_vec)
        .filter(|l| noq::build_note(l).is_ok())
        .collect()
}

fn small() -> Config {
    Config {
        segment_size: 64 << 10,
        fsync: Fsync::Never,
    }
}

const LONG: Duration = Duration::from_secs(600);

fn any(n: usize, q: &Noq, ttl: Duration) -> Vec<Lease> {
    q.lease_where(n, ttl, |_| true)
}

fn seg_files(dir: &Path) -> Vec<u32> {
    let mut ids: Vec<u32> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| Segment::parse_file_name(e.unwrap().file_name().to_str()?))
        .collect();
    ids.sort();
    ids
}

fn state(q: &Noq, seq: Seq) -> State {
    q.log().get(seq).unwrap().state()
}

#[test]
fn lease_ack_nack() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let evs = events();
    let seqs: Vec<Seq> = evs[..10].iter().map(|e| q.push(e).unwrap()).collect();

    let ls = any(4, &q, LONG);
    assert_eq!(ls.iter().map(Lease::seq).collect::<Vec<_>>(), seqs[..4]);
    assert_eq!(ls[0].json(), &evs[0][..]);
    assert_eq!(ls[0].note().json(), &evs[0][..]);
    assert!(ls.iter().all(|l| state(&q, l.seq()) == State::Leased));
    assert_eq!(q.leased(), 4);

    // Held items are out of the queue: the next lease skips them.
    let more = any(100, &q, LONG);
    assert_eq!(more.iter().map(Lease::seq).collect::<Vec<_>>(), seqs[4..]);
    assert!(any(100, &q, LONG).is_empty());

    let mut ls = ls.into_iter();
    ls.next().unwrap().ack().unwrap();
    assert_eq!(state(&q, seqs[0]), State::Acked);
    ls.next().unwrap().nack().unwrap();
    assert_eq!(state(&q, seqs[1]), State::Live);
    assert_eq!(q.leased(), 2 + 6);

    // Only the nacked one comes back.
    let again = any(100, &q, LONG);
    assert_eq!(again.iter().map(Lease::seq).collect::<Vec<_>>(), [seqs[1]]);

    // The predicate picks.
    drop((again, ls, more));
    assert!(
        q.lease_where(100, LONG, |n| n.kind() == u32::MAX)
            .is_empty()
    );
}

#[test]
fn lease_seq_takes_one_item() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let seq = q.push(&events()[0]).unwrap();
    let l = q.lease_seq(seq, LONG).unwrap();
    assert!(q.lease_seq(seq, LONG).is_none());
    l.ack().unwrap();
    assert!(q.lease_seq(seq, LONG).is_none());
    assert!(q.lease_seq(Seq::new(9, 0), LONG).is_none());
}

#[test]
fn expired_lease_is_requeued_and_its_holder_locked_out() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let seq = q.push(&events()[0]).unwrap();

    let mut old = any(1, &q, Duration::ZERO).pop().unwrap();
    assert_eq!(q.reap(), 1);
    assert_eq!(state(&q, seq), State::Live);
    assert_eq!(q.leased(), 0);

    // Someone else takes it; the stale lease can't touch theirs.
    let new = any(1, &q, LONG).pop().unwrap();
    assert_eq!(new.seq(), seq);
    assert_eq!(old.extend(LONG), Err(Error::LeaseExpired));
    assert_eq!(old.ack(), Err(Error::LeaseExpired));
    assert_eq!(state(&q, seq), State::Leased);
    new.ack().unwrap();
    assert_eq!(state(&q, seq), State::Acked);
}

#[test]
fn reaper_thread_requeues_on_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let seq = q.push(&events()[0]).unwrap();
    // A long lease first, so the reaper is asleep on a far deadline when
    // the short one arrives and has to be woken for it.
    let evs = events();
    q.push(&evs[1]).unwrap();
    let _long = q.lease_where(1, LONG, |n| n.json() == &evs[1][..]);
    let short = q.lease_seq(seq, Duration::from_millis(30)).unwrap();

    let start = Instant::now();
    while state(&q, seq) != State::Live {
        assert!(start.elapsed() < Duration::from_secs(5), "reaper never ran");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(q.leased(), 1);
    assert_eq!(short.nack(), Err(Error::LeaseExpired));
}

#[test]
fn extend_moves_the_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let seq = q.push(&events()[0]).unwrap();
    let mut l = any(1, &q, Duration::ZERO).pop().unwrap();
    l.extend(LONG).unwrap();
    assert!(l.deadline() > Instant::now() + LONG / 2);
    assert_eq!(q.reap(), 0);
    assert_eq!(state(&q, seq), State::Leased);
    l.ack().unwrap();
}

#[test]
fn concurrent_consumers_never_share_an_item() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let evs = events();
    let n = evs.len() * 2;
    for ev in evs.iter().chain(&evs) {
        q.push(ev).unwrap();
    }

    let held = Arc::new(Mutex::new(HashSet::new()));
    let acked = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|s| {
        for t in 0..8 {
            let (q, held, acked, done) = (q.clone(), held.clone(), acked.clone(), done.clone());
            s.spawn(move || {
                let mut i = t;
                while done.load(Ordering::Relaxed) < n {
                    for l in any(7, &q, LONG) {
                        assert!(held.lock().insert(l.seq()), "{:?} leased twice", l.seq());
                        i += 1;
                        let seq = l.seq();
                        // Ack two of three, nack the rest.
                        if i % 3 == 0 {
                            held.lock().remove(&seq);
                            l.nack().unwrap();
                        } else {
                            held.lock().remove(&seq);
                            l.ack().unwrap();
                            acked.lock().push(seq);
                            done.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });

    let mut acked = acked.lock().clone();
    acked.sort();
    let before = acked.len();
    acked.dedup();
    assert_eq!(before, acked.len(), "an item was acked twice");
    assert_eq!(acked.len(), n);
    assert_eq!(q.leased(), 0);
    assert!(any(1, &q, LONG).is_empty());
}

#[test]
fn drained_segments_are_unlinked() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let evs = events();
    for _ in 0..4 {
        for ev in &evs {
            q.push(ev).unwrap();
        }
    }
    let files = seg_files(dir.path());
    assert!(files.len() > 8, "{files:?}");
    let tail = *files.last().unwrap();

    // A record held across reclamation keeps its segment's memory.
    let peek = q.log().scan(Seq(0)).next().unwrap();

    loop {
        let ls = any(500, &q, LONG);
        if ls.is_empty() {
            break;
        }
        for l in ls {
            l.ack().unwrap();
        }
    }
    // Only the tail is left, drained but still taking appends.
    assert_eq!(seg_files(dir.path()), [tail]);
    assert_eq!(q.log().segments().len(), 1);
    assert_eq!(q.log().head_seq(), Seq::new(tail, 0));
    assert_eq!(q.log().segments()[0].pending(), 0);
    assert_eq!(peek.json(), &evs[0][..]);
    assert!(q.log().get(peek.seq()).is_none());
    assert_eq!(q.log().scan(Seq(0)).next().unwrap().seq().segment(), tail);

    // A tail that is drained when a rotation seals it goes right then.
    let mut i = 0;
    let last = loop {
        let seq = q.push(&evs[i % evs.len()]).unwrap();
        i += 1;
        if seq.segment() != tail {
            break seq;
        }
        q.lease_seq(seq, LONG).unwrap().ack().unwrap();
    };
    assert_eq!(seg_files(dir.path()), [last.segment()]);
    assert_eq!(q.log().segments()[0].pending(), 1);
}

#[test]
fn pending_counts_survive_reopen_and_drained_files_go_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let evs = events();
    let (first, second) = {
        let log = Log::open(dir.path(), small()).unwrap();
        let mut seqs = Vec::new();
        while log.segments().len() < 3 {
            for ev in &evs {
                seqs.push(log.append(ev).unwrap());
            }
        }
        let segs = log.segments();
        // Ack segment 0 behind the log's back, so it is never reclaimed in
        // flight, and lease one record of segment 1.
        for r in segs[0].records(0) {
            assert!(r.cas_state(State::Live, State::Acked));
        }
        let mut recs = segs[1].records(0);
        assert!(recs.next().unwrap().cas_state(State::Live, State::Leased));
        assert!(recs.next().unwrap().cas_state(State::Live, State::Dead));
        (segs[0].id(), segs[1].id())
    };
    assert!(seg_files(dir.path()).contains(&first));

    let q = Noq::open(dir.path(), small()).unwrap();
    let files = seg_files(dir.path());
    assert!(!files.contains(&first), "drained segment dropped at open");
    let segs = q.log().segments();
    assert_eq!(segs[0].id(), second);
    let count = |s: &Segment| s.records(0).filter(|r| r.state() == State::Live).count() as u32;
    for s in &segs {
        assert_eq!(s.pending(), count(s), "segment {}", s.id());
    }
    // The leased one came back, the dead one stayed dead.
    assert_eq!(segs[0].pending(), segs[0].records(0).count() as u32 - 1);
}

#[test]
fn dropping_the_queue_stops_the_reaper() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    q.push(&events()[0]).unwrap();
    let l = any(1, &q, LONG).pop().unwrap();
    drop(q);
    // The lease keeps the queue alive; dropping both joins the reaper.
    drop(l);
    // Reopening needs the lock back, so the log really was dropped.
    let q = Noq::open(dir.path(), small()).unwrap();
    assert_eq!(any(5, &q, LONG).len(), 1);
}
