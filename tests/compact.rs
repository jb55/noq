//! Sealed-segment sidecars, segment compaction, and posting-list
//! compaction.

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use common::Signer;
use noq::{Config, Filter, Fsync, Log, Noq, NoteRef, Pushed, Segment, Seq, State};

const LONG: Duration = Duration::from_secs(600);

fn small() -> Config {
    Config {
        segment_size: 64 << 10,
        fsync: Fsync::Never,
        ..Config::default()
    }
}

fn idx_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(Segment::file_name(id)).with_extension("idx")
}

fn files(dir: &Path, ext: &str) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.unwrap().file_name().into_string().ok())
        .filter(|n| n.ends_with(ext))
        .collect();
    v.sort();
    v
}

fn filter(json: &str) -> Filter {
    Filter::from_json(json.as_bytes()).unwrap()
}

/// What a reopened queue holds: every queued seq for a few filters, each
/// planned a different way, and the index's sizes.
fn snapshot(q: &Noq, filters: &[Filter]) -> (Vec<Vec<Seq>>, noq::IndexStats) {
    let found = filters
        .iter()
        .map(|f| q.peek(f, usize::MAX).iter().map(|r| r.seq()).collect())
        .collect();
    (found, q.index_stats())
}

/// Filters planned by kind, author, tag and a scan, picked from `evs`.
fn filters(evs: &[Vec<u8>]) -> Vec<Filter> {
    let note = |i: usize| {
        let idx = noq::build_note(&evs[i]).unwrap();
        let n = NoteRef::new(&idx, &evs[i]).unwrap();
        (n.kind(), common::hex(n.pubkey()))
    };
    let (kind, author) = note(evs.len() / 2);
    let p = evs
        .iter()
        .find_map(|e| {
            let v: serde_json::Value = serde_json::from_slice(e).unwrap();
            v["tags"]
                .as_array()?
                .iter()
                .find_map(|t| (t[0] == "p").then(|| t[1].as_str().unwrap().to_owned()))
        })
        .unwrap();
    vec![
        filter("{}"),
        filter(&format!(r#"{{"kinds":[{kind}]}}"#)),
        filter(&format!(r#"{{"authors":["{author}"]}}"#)),
        filter(&format!(r##"{{"#p":["{p}"]}}"##)),
    ]
}

#[test]
fn sealed_segments_reopen_from_their_sidecars_as_from_a_scan() {
    let dir = tempfile::tempdir().unwrap();
    let evs = common::events();
    let fs = filters(&evs);
    let (live, held) = {
        let q = Noq::open(dir.path(), small()).unwrap();
        let seqs: Vec<Seq> = evs
            .iter()
            .map(|e| q.push(e).unwrap().queued().unwrap())
            .collect();
        // Consume every third, hold every fifth across the reopen.
        let mut held = Vec::new();
        for (i, &seq) in seqs.iter().enumerate() {
            match i % 15 {
                0 | 3 | 6 | 9 | 12 => q.lease_seq(seq, LONG).unwrap().ack().unwrap(),
                5 | 10 => held.push(q.lease_seq(seq, LONG).unwrap()),
                _ => {}
            }
        }
        assert_eq!(q.compact(), 0, "nothing is holey enough");
        let segs = q.log().segments();
        assert!(segs.len() > 4, "{segs:?}");
        // Every sealed segment has one; the tail doesn't.
        let want: Vec<String> = segs[..segs.len() - 1]
            .iter()
            .map(|s| Segment::file_name(s.id()).replace(".seg", ".idx"))
            .collect();
        assert_eq!(files(dir.path(), ".idx"), want);
        (q.peek(&fs[0], usize::MAX).len(), held.len())
    };

    let from_sidecars = snapshot(&Noq::open(dir.path(), small()).unwrap(), &fs);
    // The held ones came back.
    assert_eq!(from_sidecars.0[0].len(), live + held);
    for f in files(dir.path(), ".idx") {
        std::fs::remove_file(dir.path().join(f)).unwrap();
    }
    let from_scan = snapshot(&Noq::open(dir.path(), small()).unwrap(), &fs);
    assert_eq!(from_sidecars, from_scan);
    for found in &from_scan.0 {
        assert!(!found.is_empty());
    }
    // The scan wrote them again.
    assert!(!files(dir.path(), ".idx").is_empty());
}

#[test]
fn a_bad_sidecar_is_ignored_and_strays_are_removed() {
    let dir = tempfile::tempdir().unwrap();
    let evs = common::events();
    let fs = filters(&evs);
    let (want, segs) = {
        let q = Noq::open(dir.path(), small()).unwrap();
        for e in &evs {
            q.push(e).unwrap();
        }
        let segs: Vec<u32> = q.log().segments().iter().map(|s| s.id()).collect();
        (snapshot(&q, &fs), segs)
    };
    // A flipped byte, another segment's sidecar, and one whose segment is
    // gone; and a half-written one.
    let a = idx_path(dir.path(), segs[0]);
    let mut buf = std::fs::read(&a).unwrap();
    buf[40] ^= 1;
    std::fs::write(&a, &buf).unwrap();
    std::fs::copy(idx_path(dir.path(), segs[1]), idx_path(dir.path(), segs[2])).unwrap();
    let stray = idx_path(dir.path(), 999);
    std::fs::copy(idx_path(dir.path(), segs[3]), &stray).unwrap();
    let tmp = idx_path(dir.path(), segs[3]).with_extension("idx.tmp");
    std::fs::write(&tmp, b"torn").unwrap();

    let q = Noq::open(dir.path(), small()).unwrap();
    assert_eq!(snapshot(&q, &fs), want);
    assert!(!stray.exists() && !tmp.exists());
    // The bad ones were written again from their segments.
    drop(q);
    let q = Noq::open(dir.path(), small()).unwrap();
    assert_eq!(snapshot(&q, &fs), want);
}

#[test]
fn compaction_moves_queued_items_off_a_holey_segment() {
    let dir = tempfile::tempdir().unwrap();
    let evs = common::events();
    let q = Noq::open(dir.path(), small()).unwrap();
    let seqs: Vec<Seq> = evs
        .iter()
        .map(|e| q.push(e).unwrap().queued().unwrap())
        .collect();
    let first: Vec<Seq> = seqs.iter().copied().filter(|s| s.segment() == 0).collect();
    assert!(first.len() > 20);

    // Segment 0: one held, two queued, the rest consumed.
    let held = q.lease_seq(first[0], LONG).unwrap();
    for &s in &first[3..] {
        q.lease_seq(s, LONG).unwrap().ack().unwrap();
    }
    let ids = |s: Seq| common::hex(q.log().get(s).unwrap().note().id());
    let (a, b) = (ids(first[1]), ids(first[2]));
    let tail = q.log().tail_seq();

    // Under `Fsync::Never` too, the copies are synced before the
    // originals die, once per segment.
    let syncs = q.log().syncs();
    assert_eq!(q.compact(), 2);
    assert_eq!(q.log().syncs(), syncs + 1);
    assert_eq!(q.compact(), 0, "only the held one is left");
    assert_eq!(q.log().syncs(), syncs + 1, "nothing moved, nothing synced");
    assert_eq!(q.log().get(first[1]).unwrap().state(), State::Dead);
    let moved = q.peek(&filter(&format!(r#"{{"ids":["{a}","{b}"]}}"#)), 10);
    let moved: Vec<Seq> = moved.iter().map(|r| r.seq()).collect();
    assert_eq!(moved.len(), 2);
    assert!(moved.iter().all(|&s| s >= tail), "{moved:?} past {tail:?}");
    // Pushing either again is a duplicate of the copy.
    assert_eq!(q.push(&evs[1]).unwrap(), Pushed::Duplicate(moved[0]));

    // The held one drains it.
    assert!(idx_path(dir.path(), 0).exists());
    held.ack().unwrap();
    assert_eq!(q.log().segments()[0].id(), 1);
    assert!(!dir.path().join(Segment::file_name(0)).exists());
    assert!(!idx_path(dir.path(), 0).exists());

    let n = q.peek(&filter("{}"), usize::MAX).len();
    let ids_known = q.index_stats().ids;
    drop((q, moved));
    let q = Noq::open(dir.path(), small()).unwrap();
    assert_eq!(q.peek(&filter("{}"), usize::MAX).len(), n);
    assert_eq!(q.index_stats().ids, ids_known);
}

#[test]
fn sealing_compacts_the_segments_older_than_the_newest_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let evs = common::events();
    let q = Noq::open(dir.path(), small()).unwrap();
    let mut seqs = Vec::new();
    let mut i = 0;
    // Consumers keep up with all but one item in each segment.
    while q.log().tail_seq().segment() < 3 {
        let seq = q.push(&evs[i]).unwrap().queued().unwrap();
        i += 1;
        if seqs
            .last()
            .is_some_and(|s: &Seq| s.segment() == seq.segment())
        {
            q.lease_seq(seq, LONG).unwrap().ack().unwrap();
        }
        seqs.push(seq);
    }
    // Segment 0 was compacted when segment 1 was sealed; segment 1 is the
    // newest sealed one when segment 2 is sealed, so it was compacted when
    // segment 2 was.
    let segs: Vec<u32> = q.log().segments().iter().map(|s| s.id()).collect();
    assert_eq!(segs, [2, 3]);
    assert_eq!(q.peek(&filter("{}"), usize::MAX).len(), 4);
}

#[test]
fn reopen_keeps_one_copy_of_an_event_left_in_two_places() {
    // What a crash in the middle of a move leaves: the event twice, both
    // LIVE. Once in the same segment, once across a sealed one.
    for across in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let evs = common::events();
        let (a, b) = {
            let log = Log::open(dir.path(), small()).unwrap();
            let a = log.append(&evs[0]).unwrap();
            if across {
                for e in &evs[1..] {
                    if log.segments().len() == 2 {
                        break;
                    }
                    log.append(e).unwrap();
                }
            }
            (a, log.append(&evs[0]).unwrap())
        };
        assert_eq!(a.segment() < b.segment(), across);
        let q = Noq::open(dir.path(), small()).unwrap();
        let id = common::hex(q.log().get(a).unwrap().note().id());
        let found = q.peek(&filter(&format!(r#"{{"ids":["{id}"]}}"#)), 10);
        assert_eq!(found.iter().map(|r| r.seq()).collect::<Vec<_>>(), [b]);
        assert_eq!(q.log().get(a).unwrap().state(), State::Dead);
        let found = q.peek(&filter("{}"), usize::MAX);
        assert_eq!(
            found
                .iter()
                .filter(|r| r.seq() == a || r.seq() == b)
                .count(),
            1
        );
    }
}

#[test]
fn reopen_keeps_the_original_of_a_copy_that_died() {
    // What a move whose sync failed leaves, if the copy's DEAD byte made it
    // to disk: the original LIVE, the copy DEAD. A later move that worked
    // and then crashed adds a third, LIVE copy, which wins.
    for (across, again) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let evs = common::events();
        let (a, c) = {
            let log = Log::open(dir.path(), small()).unwrap();
            let a = log.append(&evs[0]).unwrap();
            if across {
                for e in &evs[1..] {
                    if log.segments().len() == 2 {
                        break;
                    }
                    log.append(e).unwrap();
                }
            }
            let b = log.append(&evs[0]).unwrap();
            assert!(log.retire(&log.get(b).unwrap(), State::Live, State::Dead));
            let c = again.then(|| log.append(&evs[0]).unwrap());
            (a, c)
        };
        let q = Noq::open(dir.path(), small()).unwrap();
        let id = common::hex(q.log().get(a).unwrap().note().id());
        let found = q.peek(&filter(&format!(r#"{{"ids":["{id}"]}}"#)), 10);
        let found: Vec<Seq> = found.iter().map(|r| r.seq()).collect();
        assert_eq!(found, [c.unwrap_or(a)], "across {across} again {again}");
        let a_state = q.log().get(a).unwrap().state();
        assert_eq!(a_state, if again { State::Dead } else { State::Live });
    }
}

#[test]
fn posting_lists_drop_what_walks_step_over_behind_a_held_item() {
    let dir = tempfile::tempdir().unwrap();
    // One segment, so no drop trims the lists first.
    let config = Config {
        segment_size: 4 << 20,
        ..small()
    };
    let q = Noq::open(dir.path(), config).unwrap();
    let bob = Signer::new(2);
    let n = 600;
    let seqs: Vec<Seq> = (0..n)
        .map(|i| {
            q.push(&bob.event(1_700_000_000 + i, 5999, &[], "job"))
                .unwrap()
                .queued()
                .unwrap()
        })
        .collect();
    let jobs = filter(r#"{"kinds":[5999]}"#);
    // The oldest is held for good; the rest are consumed behind it.
    let _held = q.lease_seq(seqs[0], LONG).unwrap();
    for s in &seqs[1..] {
        q.lease_seq(*s, LONG).unwrap().ack().unwrap();
    }
    let before = q.index_stats().postings;
    assert!(q.lease(&jobs, 10, LONG).is_empty());
    // The kind's list was walked and cut down to the held one; the
    // author's, never walked, wasn't.
    assert_eq!(q.index_stats().postings, before - (n as usize - 1));
    let late = q
        .push(&bob.event(1_700_001_000, 5999, &[], "late"))
        .unwrap()
        .queued()
        .unwrap();
    let got = q.lease(&jobs, 10, LONG);
    assert_eq!(got.iter().map(|l| l.seq()).collect::<Vec<_>>(), [late]);
}

#[test]
fn compaction_racing_consumers_never_hands_an_event_out_twice() {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let evs = common::events();
    for e in &evs {
        q.push(e).unwrap();
    }
    let held = Arc::new(Mutex::new(HashSet::new()));
    let acked = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(AtomicBool::new(false));
    let moved = Arc::new(AtomicUsize::new(0));
    let any = filter("{}");
    std::thread::scope(|s| {
        let (q2, done2, moved2) = (q.clone(), done.clone(), moved.clone());
        s.spawn(move || {
            while !done2.load(Ordering::Relaxed) {
                moved2.fetch_add(q2.compact(), Ordering::Relaxed);
            }
        });
        for t in 0..6 {
            let (q, held, acked, any) = (q.clone(), held.clone(), acked.clone(), any.clone());
            s.spawn(move || {
                let mut i = t;
                loop {
                    let got = q.lease(&any, 5, LONG);
                    if got.is_empty() {
                        if q.leased() == 0 && q.peek(&any, 1).is_empty() {
                            return;
                        }
                        continue;
                    }
                    for l in got {
                        let id = *l.note().id();
                        assert!(held.lock().unwrap().insert(id), "held twice");
                        i += 1;
                        held.lock().unwrap().remove(&id);
                        // Nack one in four, so items keep going back to
                        // segments compaction is moving them off.
                        if i % 4 == 0 {
                            l.nack().unwrap();
                        } else {
                            l.ack().unwrap();
                            acked.lock().unwrap().push(id);
                        }
                    }
                }
            });
        }
        // Consumers finish on their own; then stop the compactor.
        let start = std::time::Instant::now();
        while acked.lock().unwrap().len() < evs.len() {
            assert!(start.elapsed() < Duration::from_secs(60), "stuck");
            std::thread::sleep(Duration::from_millis(5));
        }
        done.store(true, Ordering::Relaxed);
    });
    let mut acked = acked.lock().unwrap().clone();
    acked.sort();
    let n = acked.len();
    acked.dedup();
    assert_eq!(n, acked.len(), "an event was acked twice");
    assert_eq!(n, evs.len());
    assert!(moved.load(Ordering::Relaxed) > 0, "nothing was compacted");
}
