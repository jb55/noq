use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use noq::{Config, Error, Fsync, Log, RECORD_HEADER_LEN, Record, Segment, Seq, State};

fn events() -> Vec<Vec<u8>> {
    std::fs::read("tests/data/many-events-sample.jsonl")
        .unwrap()
        .split(|&c| c == b'\n')
        .filter(|l| !l.is_empty())
        .map(<[u8]>::to_vec)
        .filter(|l| noq::build_note(l).is_ok())
        .collect()
}

fn small(fsync: Fsync) -> Config {
    Config {
        segment_size: 64 << 10,
        fsync,
        ..Config::default()
    }
}

fn all(log: &Log) -> Vec<Record> {
    log.scan(Seq(0)).collect()
}

fn seg_path(dir: &Path, id: u32) -> std::path::PathBuf {
    dir.join(Segment::file_name(id))
}

fn poke(path: &Path, at: u64, bytes: &[u8]) {
    let mut f = OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(at)).unwrap();
    f.write_all(bytes).unwrap();
}

#[test]
fn append_scan_rotate_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let evs = events();
    assert!(evs.len() > 500);
    let mut seqs = Vec::new();
    {
        let log = Log::open(dir.path(), small(Fsync::Never)).unwrap();
        for ev in &evs {
            seqs.push(log.append(ev).unwrap());
        }
        assert!(log.segments().len() > 3, "small segments rotate");
        assert!(seqs.windows(2).all(|w| w[0] < w[1]));

        let recs = all(&log);
        assert_eq!(recs.len(), evs.len());
        for ((r, ev), seq) in recs.iter().zip(&evs).zip(&seqs) {
            assert_eq!(r.seq(), *seq);
            assert_eq!(r.json(), &ev[..]);
            assert_eq!(r.state(), State::Live);
            assert_eq!(r.note().json(), &ev[..]);
            assert_eq!(log.get(*seq).unwrap().json(), &ev[..]);
        }
        // A scan from the middle starts at that record.
        let mid = seqs[evs.len() / 2];
        assert_eq!(log.scan(mid).next().unwrap().seq(), mid);
    }

    let log = Log::open(dir.path(), small(Fsync::Never)).unwrap();
    let recs = all(&log);
    assert_eq!(recs.len(), evs.len());
    assert!(recs.iter().zip(&evs).all(|(r, ev)| r.json() == &ev[..]));
    let next = log.append(&evs[0]).unwrap();
    assert!(next > *seqs.last().unwrap());
}

#[test]
fn records_outlive_the_log_lock() {
    let dir = tempfile::tempdir().unwrap();
    let evs = events();
    let log = Log::open(dir.path(), small(Fsync::Never)).unwrap();
    let seq = log.append(&evs[0]).unwrap();
    let rec = log.get(seq).unwrap();
    drop(log);
    // The record holds its segment's map, not the log.
    assert_eq!(rec.json(), &evs[0][..]);
}

#[test]
fn rejects_invalid_and_oversized() {
    let dir = tempfile::tempdir().unwrap();
    let log = Log::open(
        dir.path(),
        Config {
            segment_size: 4096,
            fsync: Fsync::Never,
            ..Config::default()
        },
    )
    .unwrap();
    assert!(log.append(b"{}").is_err());
    let evs = events();
    let big = evs.iter().max_by_key(|e| e.len()).unwrap();
    if big.len() + RECORD_HEADER_LEN > 4096 {
        assert_eq!(
            log.append(big),
            Err(Error::TooLarge("record for the segment size"))
        );
    }
    assert_eq!(all(&log).len(), 0);
}

#[test]
fn second_open_is_locked() {
    let dir = tempfile::tempdir().unwrap();
    let _log = Log::open(dir.path(), small(Fsync::Never)).unwrap();
    assert_eq!(
        Log::open(dir.path(), small(Fsync::Never)).unwrap_err(),
        Error::Locked
    );
}

/// Appends three records, damages the second with `damage`, and checks the
/// reopened log keeps only the first, and that the third never comes back
/// once a new record is written over the damaged one.
fn truncates_at(damage: impl Fn(&Path, u64, u64)) {
    let dir = tempfile::tempdir().unwrap();
    let evs = events();
    let seqs: Vec<Seq> = {
        let log = Log::open(dir.path(), small(Fsync::Always)).unwrap();
        evs[..3].iter().map(|e| log.append(e).unwrap()).collect()
    };
    let path = seg_path(dir.path(), 0);
    damage(&path, seqs[1].offset() as u64, seqs[2].offset() as u64);

    {
        let log = Log::open(dir.path(), small(Fsync::Always)).unwrap();
        let recs = all(&log);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].json(), &evs[0][..]);
        // The append goes where the damaged record was. A shorter record
        // leaves the third's old bytes past it, if they weren't zeroed.
        let short = evs.iter().min_by_key(|e| e.len()).unwrap();
        assert_eq!(log.append(short).unwrap(), seqs[1]);
    }
    let log = Log::open(dir.path(), small(Fsync::Always)).unwrap();
    let recs = all(&log);
    assert_eq!(recs.len(), 2, "the record past the damage stays gone");
}

#[test]
fn recovery_truncates_at_writing_record() {
    truncates_at(|p, off, _| poke(p, off + 4, &[State::Writing as u8]));
}

#[test]
fn recovery_truncates_at_bad_crc() {
    truncates_at(|p, off, _| poke(p, off + RECORD_HEADER_LEN as u64 + 200, b"X"));
}

#[test]
fn recovery_truncates_at_torn_header() {
    // A record's page lost while a later one reached disk.
    truncates_at(|p, off, next| poke(p, off, &vec![0; (next - off) as usize]));
}

#[test]
fn recovery_truncates_at_overlong_record() {
    truncates_at(|p, off, _| poke(p, off, &u32::MAX.to_le_bytes()));
}

/// Reads everything a record hands out, so an out-of-bounds slice would
/// fault here rather than go unnoticed.
fn touch(r: &Record) -> usize {
    let _ = r.state();
    r.json().iter().map(|&b| b as usize).sum::<usize>() + r.get().index().len()
}

#[test]
fn seqs_off_a_record_start_read_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let evs = events();
    let log = Log::open(dir.path(), small(Fsync::Never)).unwrap();
    let seqs: Vec<Seq> = evs[..20].iter().map(|e| log.append(e).unwrap()).collect();
    let end = log.tail_seq().offset();

    assert!(log.get(Seq::new(0, 8)).is_none());
    assert_eq!(log.scan(Seq::new(0, 8)).count(), 0);

    // Every offset that isn't a record start, aligned or not, up to past
    // the end, is refused, and a scan from it finds nothing.
    for off in 0..end + 64 {
        let seq = Seq::new(0, off);
        if seqs.contains(&seq) {
            assert!(log.get(seq).is_some());
            continue;
        }
        assert!(log.get(seq).is_none(), "{off} is not a record start");
        assert!(log.scan(seq).next().is_none());
    }
    assert!(log.get(Seq::new(0, u32::MAX)).is_none());
    assert!(log.get(Seq::new(u32::MAX, 0)).is_none());
}

#[test]
fn forged_headers_inside_a_record_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let evs = events();
    let log = Log::open(dir.path(), small(Fsync::Never)).unwrap();
    let ev = evs.iter().max_by_key(|e| e.len()).unwrap();
    let first = log.append(ev).unwrap();
    log.append(&evs[0]).unwrap();
    let end = log.tail_seq().offset() as u64;

    // Write a header into the first record's json through the file, which
    // the log's shared map sees: an aligned offset whose bytes now claim a
    // record as long or longer than the rest of the segment.
    let path = seg_path(dir.path(), 0);
    let at = (first.offset() as u64 + RECORD_HEADER_LEN as u64 + 64) & !7;
    let seq = Seq::new(0, at as u32);
    let header = |total_len: u32, note_len: u32| {
        let mut h = [0u8; RECORD_HEADER_LEN];
        h[0..4].copy_from_slice(&total_len.to_le_bytes());
        h[4] = State::Live as u8;
        h[5] = 1;
        h[8..12].copy_from_slice(&note_len.to_le_bytes());
        h
    };
    let past_end = (end - at + 8) as u32;
    for (total_len, note_len) in [
        (1 << 30, 0),  // far past the map
        (past_end, 0), // just past the committed end
        (u32::MAX, 0), // stride overflows a u32
        (64, 1 << 20), // index longer than the record
        (8, 0),        // shorter than a header
    ] {
        poke(&path, at, &header(total_len, note_len));
        assert!(log.get(seq).is_none(), "total {total_len} note {note_len}");
        assert!(log.scan(seq).next().is_none());
    }

    // A header that fits but carries a bad state or version is refused too.
    let mut h = header(64, 0);
    h[4] = 9;
    poke(&path, at, &h);
    assert!(log.get(seq).is_none(), "unknown state");
    h[4] = State::Writing as u8;
    poke(&path, at, &h);
    assert!(log.get(seq).is_none(), "WRITING below the end");
    let mut h = header(64, 0);
    h[5] = 2;
    poke(&path, at, &h);
    assert!(log.get(seq).is_none(), "unknown version");

    // One that passes every check is in-bounds bytes, nothing worse.
    poke(&path, at, &header(64, 0));
    let r = log.get(seq).unwrap();
    assert_eq!(r.json().len(), 64 - RECORD_HEADER_LEN);
    touch(&r);
}

#[test]
fn seq_from_before_a_crash_cannot_reach_past_the_end() {
    // B is torn, recovery ends the segment at B, and a new record is
    // written over it. C's old seq now points into the new record.
    let dir = tempfile::tempdir().unwrap();
    let evs = events();
    let seqs: Vec<Seq> = {
        let log = Log::open(dir.path(), small(Fsync::Always)).unwrap();
        evs[..3].iter().map(|e| log.append(e).unwrap()).collect()
    };
    poke(
        &seg_path(dir.path(), 0),
        seqs[1].offset() as u64 + 4,
        &[State::Writing as u8],
    );

    let log = Log::open(dir.path(), small(Fsync::Always)).unwrap();
    assert!(log.get(seqs[2]).is_none(), "C is past the recovered end");
    let long = evs.iter().max_by_key(|e| e.len()).unwrap();
    assert!(long.len() > evs[1].len() + evs[2].len());
    assert_eq!(log.append(long).unwrap(), seqs[1]);
    if let Some(r) = log.get(seqs[2]) {
        touch(&r);
    }
    let mut scan = log.scan(seqs[2]);
    if let Some(r) = scan.next() {
        touch(&r);
    }
    assert!(scan.next().is_none());
}

#[test]
fn reopen_returns_leased_to_live() {
    let dir = tempfile::tempdir().unwrap();
    let evs = events();
    let seq = {
        let log = Log::open(dir.path(), small(Fsync::Never)).unwrap();
        let a = log.append(&evs[0]).unwrap();
        let b = log.append(&evs[1]).unwrap();
        assert!(
            log.get(a)
                .unwrap()
                .get()
                .cas_state(State::Live, State::Leased)
        );
        assert!(
            !log.get(a)
                .unwrap()
                .get()
                .cas_state(State::Live, State::Leased)
        );
        assert!(
            log.get(b)
                .unwrap()
                .get()
                .cas_state(State::Live, State::Acked)
        );
        a
    };
    let log = Log::open(dir.path(), small(Fsync::Never)).unwrap();
    let states: Vec<_> = all(&log).iter().map(Record::state).collect();
    assert_eq!(states, [State::Live, State::Acked]);
    assert_eq!(log.get(seq).unwrap().state(), State::Live);
}

#[test]
fn truncated_new_segment_file_is_resized() {
    let dir = tempfile::tempdir().unwrap();
    let evs = events();
    {
        let log = Log::open(dir.path(), small(Fsync::Never)).unwrap();
        log.append(&evs[0]).unwrap();
    }
    // A crash between creating the next segment and sizing it.
    std::fs::File::create(seg_path(dir.path(), 1)).unwrap();
    let log = Log::open(dir.path(), small(Fsync::Never)).unwrap();
    let seq = log.append(&evs[1]).unwrap();
    assert_eq!(seq, Seq::new(1, 0));
    assert_eq!(all(&log).len(), 2);
}

#[test]
fn scan_follows_concurrent_appends_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let evs = Arc::new(events());
    let log = Arc::new(Log::open(dir.path(), small(Fsync::Never)).unwrap());
    let writer = {
        let (log, evs) = (log.clone(), evs.clone());
        std::thread::spawn(move || {
            for ev in evs.iter() {
                log.append(ev).unwrap();
            }
        })
    };
    let mut seen = Vec::new();
    let mut scan = log.scan(Seq(0));
    while seen.len() < evs.len() {
        match scan.next() {
            Some(r) => seen.push(r),
            None => std::thread::yield_now(),
        }
    }
    writer.join().unwrap();
    assert!(scan.next().is_none());
    assert!(
        seen.iter()
            .zip(evs.iter())
            .all(|(r, ev)| r.json() == &ev[..])
    );
}

/// The child half of `kill9_mid_append`: appends forever.
#[test]
#[ignore]
fn kill9_child() {
    let Ok(dir) = std::env::var("NOQ_KILL9_DIR") else {
        return;
    };
    let evs = events();
    let log = Log::open(&dir, small(Fsync::Never)).unwrap();
    // Carry on the sequence where the last child was killed.
    let have = all(&log).len();
    println!("ready");
    std::io::stdout().flush().unwrap();
    for ev in evs.iter().cycle().skip(have % evs.len()) {
        log.append(ev).unwrap();
    }
}

#[test]
fn kill9_mid_append() {
    let dir = tempfile::tempdir().unwrap();
    let evs = events();
    let mut last = 0;
    for round in 0..3 {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["kill9_child", "--exact", "--ignored", "--nocapture"])
            .env("NOQ_KILL9_DIR", dir.path())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut out = child.stdout.take().unwrap();
        let mut buf = [0u8; 64];
        let mut got = Vec::new();
        while !String::from_utf8_lossy(&got).contains("ready") {
            let n = std::io::Read::read(&mut out, &mut buf).unwrap();
            assert!(n > 0, "child exited before it was ready");
            got.extend_from_slice(&buf[..n]);
        }
        std::thread::sleep(Duration::from_millis(50 + 30 * round));
        child.kill().unwrap(); // SIGKILL
        child.wait().unwrap();

        let log = Log::open(dir.path(), small(Fsync::Never)).unwrap();
        let recs = all(&log);
        assert!(recs.len() > last, "round {round} appended");
        last = recs.len();
        // Every record that survived is whole: the json is one of ours, in
        // the order it was appended, and its index reads back.
        for (i, r) in recs.iter().enumerate() {
            assert_eq!(
                r.json(),
                &evs[i % evs.len()][..],
                "round {round} record {i}"
            );
            assert_eq!(r.note().json(), r.json());
        }
    }
}
