mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use serde_json::Value;

use common::{Signer, events, hex};
use noq::{Config, Error, Filter, Fsync, Noq, NoteRef, Plan, Pushed, Seq};

const LONG: Duration = Duration::from_secs(600);

fn small() -> Config {
    Config {
        segment_size: 64 << 10,
        fsync: Fsync::Never,
        ..Config::default()
    }
}

fn filter(json: &str) -> Filter {
    Filter::from_json(json.as_bytes()).unwrap_or_else(|e| panic!("{json}: {e:?}"))
}

fn seqs<T>(items: &[T], seq: impl Fn(&T) -> Seq) -> Vec<Seq> {
    items.iter().map(seq).collect()
}

/// NIP-01 matching written against serde_json, independent of [`Filter`].
fn reference(f: &Value, ev: &Value) -> bool {
    let f = f.as_object().unwrap();
    f.iter().all(|(k, want)| {
        let want = |v: &Value| want.as_array().unwrap().contains(v);
        match k.as_str() {
            "ids" => want(&ev["id"]),
            "authors" => want(&ev["pubkey"]),
            "kinds" => want(&ev["kind"]),
            "since" => ev["created_at"].as_u64() >= f["since"].as_u64(),
            "until" => ev["created_at"].as_u64() <= f["until"].as_u64(),
            "limit" => true,
            tag => {
                let letter = tag.strip_prefix('#').unwrap();
                ev["tags"].as_array().unwrap().iter().any(|t| {
                    let t = t.as_array().unwrap();
                    t.len() >= 2 && t[0] == letter && want(&t[1])
                })
            }
        }
    })
}

/// A queue holding every sample event, and the parsed events by seq.
fn sample_queue(dir: &std::path::Path) -> (Noq, Vec<(Seq, Value)>) {
    let q = Noq::open(dir, small()).unwrap();
    let mut evs: Vec<(Seq, Value)> = Vec::new();
    for (e, index) in common::sample() {
        let ev = || serde_json::from_slice(e).unwrap();
        match q.push_note(NoteRef::new(index, e).unwrap()).unwrap() {
            Pushed::Queued(seq) => evs.push((seq, ev())),
            d => panic!("sample event pushed twice: {d:?}"),
        }
    }
    (q, evs)
}

/// Filters over the sample data's real keys: its most common kinds,
/// authors and tag values, alone, combined, and missing.
fn sample_filters(evs: &[(Seq, Value)]) -> Vec<String> {
    let ev = |i: usize| &evs[i * 97 % evs.len()].1;
    let top_tag = |letter: &str, nth: usize| {
        let mut counts = std::collections::HashMap::<String, usize>::new();
        for (_, e) in evs {
            for t in e["tags"].as_array().unwrap() {
                if t[0] == letter && t.get(1).is_some() {
                    *counts.entry(t[1].as_str().unwrap().to_owned()).or_default() += 1;
                }
            }
        }
        let mut v: Vec<_> = counts.into_iter().collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        v[nth].0.clone()
    };
    let (p0, p1, e0) = (top_tag("p", 0), top_tag("p", 1), top_tag("e", 0));
    let a0 = ev(0)["pubkey"].as_str().unwrap().to_owned();
    let a1 = evs
        .iter()
        .map(|(_, e)| e["pubkey"].as_str().unwrap())
        .find(|&p| p != a0)
        .unwrap()
        .to_owned();
    let mid = ev(5)["created_at"].as_u64().unwrap();
    let ids: Vec<&str> = (0..6).map(|i| ev(i)["id"].as_str().unwrap()).collect();
    vec![
        "{}".into(),
        r#"{"kinds":[1]}"#.into(),
        r#"{"kinds":[0,7]}"#.into(),
        r#"{"kinds":[1],"limit":3}"#.into(),
        r#"{"kinds":[9999]}"#.into(),
        r#"{"kinds":[]}"#.into(),
        format!(r#"{{"authors":["{a0}"]}}"#),
        format!(r#"{{"authors":["{a0}","{a1}"],"kinds":[1]}}"#),
        format!(r##"{{"#p":["{p0}","{p1}"]}}"##),
        format!(r##"{{"#p":["{p0}"],"authors":["{a0}"]}}"##),
        format!(r##"{{"#e":["{e0}"],"kinds":[1]}}"##),
        format!(r##"{{"#p":["{p0}"],"#e":["{e0}"]}}"##),
        format!(r##"{{"#p":["{p0}"],"#e":[]}}"##),
        format!(r#"{{"since":{mid}}}"#),
        format!(r#"{{"kinds":[1],"until":{mid}}}"#),
        format!(r#"{{"ids":["{}","{}","{}"]}}"#, ids[0], ids[1], ids[2]),
        format!(r#"{{"ids":["{}","{}"],"kinds":[0]}}"#, ids[3], ids[4]),
        format!(
            r#"{{"ids":["{}"],"authors":["{}"]}}"#,
            ids[5],
            "00".repeat(32)
        ),
    ]
}

fn expected(evs: &[(Seq, Value)], json: &str) -> Vec<Seq> {
    let f: Value = serde_json::from_str(json).unwrap();
    let limit = f["limit"].as_u64().map_or(usize::MAX, |l| l as usize);
    evs.iter()
        .filter(|(_, e)| reference(&f, e))
        .map(|&(s, _)| s)
        .take(limit)
        .collect()
}

#[test]
fn matches_agrees_with_reference() {
    let dir = tempfile::tempdir().unwrap();
    let (q, evs) = sample_queue(dir.path());
    for json in sample_filters(&evs) {
        let f = filter(&json);
        for &(seq, ref ev) in &evs {
            let rec = q.log().get(seq).unwrap();
            let want = reference(&serde_json::from_str(&json).unwrap(), ev);
            assert_eq!(f.matches(&rec.note()), want, "{json} on {seq:?}");
        }
    }
}

#[test]
fn peek_and_lease_find_what_a_full_scan_finds() {
    let dir = tempfile::tempdir().unwrap();
    let (q, evs) = sample_queue(dir.path());
    for json in sample_filters(&evs) {
        let f = filter(&json);
        let want = expected(&evs, &json);
        assert!(
            want.windows(2).all(|w| w[0] < w[1]),
            "reference is in seq order"
        );
        let got = q.peek(&f, usize::MAX);
        assert_eq!(
            seqs(&got, |r| r.seq()),
            want,
            "peek {json} via {:?}",
            q.plan(&f)
        );
    }
    assert!(q.peek(&filter(r#"{"kinds":[1]}"#), 0).is_empty());
}

#[test]
fn leasing_by_filter_drains_exactly_the_matches() {
    let evs = {
        let dir = tempfile::tempdir().unwrap();
        sample_queue(dir.path()).1
    };
    for json in sample_filters(&evs) {
        // A fresh queue per filter: each one consumes what it matches.
        let dir = tempfile::tempdir().unwrap();
        let (q, _) = sample_queue(dir.path());
        let f = filter(&json);
        let mut want = expected(&evs, &json);
        if f.get_limit().is_some() {
            want = expected(&evs, &json.replace(r#","limit":3"#, ""));
        }
        let mut got = Vec::new();
        loop {
            // Small batches, so later leases walk past acked prefixes.
            let ls = q.lease(&f, 7, LONG);
            if ls.is_empty() {
                break;
            }
            assert!(ls.len() <= f.get_limit().unwrap_or(7));
            let s = seqs(&ls, |l| l.seq());
            assert!(s.windows(2).all(|w| w[0] < w[1]), "oldest first");
            got.extend(s);
            for l in ls {
                l.ack().unwrap();
            }
        }
        assert_eq!(got, want, "{json}");
        assert!(q.peek(&f, usize::MAX).is_empty());
    }
}

#[test]
fn nacked_items_come_back_past_the_batch_size() {
    let dir = tempfile::tempdir().unwrap();
    let (q, evs) = sample_queue(dir.path());
    let f = filter(r#"{"kinds":[1]}"#);
    let all = expected(&evs, r#"{"kinds":[1]}"#);
    assert!(all.len() > 300, "a lease crosses index batches");

    let ls = q.lease(&f, 300, LONG);
    assert_eq!(seqs(&ls, |l| l.seq()), all[..300]);
    // Ack every other one, nack the rest: only the nacked come back.
    let mut back = Vec::new();
    for (i, l) in ls.into_iter().enumerate() {
        if i % 2 == 0 {
            l.ack().unwrap();
        } else {
            back.push(l.seq());
            l.nack().unwrap();
        }
    }
    back.extend(&all[300..]);
    let again = q.lease(&f, usize::MAX, LONG);
    assert_eq!(seqs(&again, |l| l.seq()), back);
}

#[test]
fn planner_picks_the_smallest_clause() {
    let dir = tempfile::tempdir().unwrap();
    let (q, evs) = sample_queue(dir.path());
    let fs = sample_filters(&evs);
    let plan = |json: &str| q.plan(&filter(json));

    assert_eq!(plan("{}"), Plan::Scan);
    assert_eq!(plan(&fs[13]), Plan::Scan, "since alone isn't indexed");
    assert_eq!(plan(r#"{"kinds":[1]}"#), Plan::Kinds);
    assert_eq!(plan(r#"{"kinds":[9999]}"#), Plan::Empty);
    assert_eq!(plan(r#"{"kinds":[]}"#), Plan::Empty);
    // An author with ~700 events against the 3 contact lists.
    let a0 = evs[0].1["pubkey"].as_str().unwrap();
    assert_eq!(
        plan(&format!(r#"{{"authors":["{a0}"],"kinds":[3]}}"#)),
        Plan::Kinds
    );
    // ~750 notes by the two authors, ~620 of kind 1.
    assert_eq!(plan(&fs[7]), Plan::Kinds);
    let a1 = evs
        .iter()
        .map(|(_, e)| e["pubkey"].as_str().unwrap())
        .find(|&p| p != a0)
        .unwrap();
    assert_eq!(
        plan(&format!(r#"{{"authors":["{a1}"],"kinds":[1]}}"#)),
        Plan::Authors
    );
    assert_eq!(plan(&fs[9]), Plan::Tag(b'p'));
    assert_eq!(plan(&fs[10]), Plan::Tag(b'e'));
    assert_eq!(plan(&fs[12]), Plan::Empty);
    assert_eq!(plan(&fs[15]), Plan::Ids);
    // The author is unknown, which no id can beat.
    assert_eq!(plan(&fs[17]), Plan::Empty);
}

#[test]
fn push_drops_duplicates_even_once_consumed_and_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let evs = events();
    let q = Noq::open(dir.path(), small()).unwrap();
    let Pushed::Queued(seq) = q.push(&evs[0]).unwrap() else {
        panic!()
    };
    assert_eq!(q.push(&evs[0]).unwrap(), Pushed::Duplicate(seq));
    q.lease_seq(seq, LONG).unwrap().ack().unwrap();
    // Consumed but still in the log: a relay re-sending it is ignored.
    assert_eq!(q.push(&evs[0]).unwrap(), Pushed::Duplicate(seq));
    assert!(q.push(&evs[1]).unwrap().queued().unwrap() > seq);
    drop(q);

    let q = Noq::open(dir.path(), small()).unwrap();
    assert_eq!(q.push(&evs[0]).unwrap(), Pushed::Duplicate(seq));
    assert_eq!(q.index_stats().ids, 2);
    // Only the unconsumed one is in the posting lists.
    let id0 = hex(q.log().get(seq).unwrap().note().id());
    assert!(
        q.peek(&filter(&format!(r#"{{"ids":["{id0}"]}}"#)), 9)
            .is_empty()
    );
    assert_eq!(q.peek(&Filter::new(), 9).len(), 1);
}

#[test]
fn reopen_rebuilds_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let (q, evs) = sample_queue(dir.path());
    let kind0 = filter(r#"{"kinds":[0]}"#);
    for l in q.lease(&kind0, usize::MAX, LONG) {
        l.ack().unwrap();
    }
    // Still LEASED on disk when the queue closes: queued again on reopen.
    drop(q.lease(&filter(r#"{"kinds":[7]}"#), 5, LONG));
    let before = q.index_stats();
    drop(q);

    let q = Noq::open(dir.path(), small()).unwrap();
    assert!(q.peek(&kind0, usize::MAX).is_empty());
    let after = q.index_stats();
    assert_eq!(after.ids, before.ids);
    assert!(after.postings <= before.postings);
    let left: Vec<(Seq, Value)> = evs
        .iter()
        .filter(|(_, e)| e["kind"] != 0)
        .cloned()
        .collect();
    for json in sample_filters(&evs) {
        let got = q.peek(&filter(&json), usize::MAX);
        assert_eq!(seqs(&got, |r| r.seq()), expected(&left, &json), "{json}");
    }
}

#[test]
fn dropped_segments_are_trimmed_from_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let (q, _) = sample_queue(dir.path());
    assert!(q.log().segments().len() > 3);
    let full = q.index_stats();
    loop {
        let ls = q.lease(&Filter::new(), 100, LONG);
        if ls.is_empty() {
            break;
        }
        for l in ls {
            l.ack().unwrap();
        }
    }
    // Only the tail is left, and every consumed entry is gone from the lists.
    let tail = q.log().segments();
    assert_eq!(tail.len(), 1);
    let left = q.index_stats();
    let in_tail = tail[0].records(0).count();
    assert_eq!(left.ids, in_tail);
    assert!(left.postings < full.postings / 3, "{left:?} of {full:?}");

    // The trimmed index still takes new events.
    let s = Signer::new(1);
    let ev = s.event(1, 4242, &[&["p", &hex(&s.pubkey)]], "hi");
    let Pushed::Queued(seq) = q.push(&ev).unwrap() else {
        panic!()
    };
    let f = Filter::new().tag(b'p', [hex(&s.pubkey)]);
    assert_eq!(q.plan(&f), Plan::Tag(b'p'));
    assert_eq!(seqs(&q.lease(&f, 9, LONG), |l| l.seq()), [seq]);
}

#[test]
fn tag_edge_cases() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let s = Signer::new(7);
    let push = |ev: Vec<u8>| q.push(&ev).unwrap().queued().unwrap();
    // The same value twice in one event is one candidate, not two.
    let twice = push(s.event(1, 1, &[&["p", "x"], &["p", "x"], &["p", "y"]], ""));
    // Only a tag's first value after its name is indexed.
    let second = push(s.event(2, 1, &[&["p", "z", "x"]], ""));
    // A name-only tag has no value to match.
    let bare = push(s.event(3, 1, &[&["p"]], ""));
    // Escapes in tag values match unescaped filter values.
    let quoted = push(s.event(4, 1, &[&["t", "a\"b\\c"]], ""));
    // Multi-letter tags aren't filterable.
    let long = push(s.event(5, 1, &[&["pp", "x"]], ""));

    let peek = |f: &str| seqs(&q.peek(&filter(f), 99), |r| r.seq());
    assert_eq!(peek(r##"{"#p":["x","y"]}"##), [twice]);
    assert_eq!(peek(r##"{"#p":["z"]}"##), [second]);
    assert_eq!(
        peek(r#"{"kinds":[1]}"#),
        [twice, second, bare, quoted, long]
    );
    assert_eq!(peek(r##"{"#t":["a\"b\\c"]}"##), [quoted]);
    assert_eq!(peek(r##"{"#t":["a\u0022b\u005cc"]}"##), [quoted]);
    assert_eq!(
        Filter::from_json(br##"{"#pp":["x"]}"##),
        Err(Error::Json {
            at: 1,
            what: "tag filters are single letters"
        })
    );
}

#[test]
fn from_json_is_strict() {
    let ok = filter(
        r##"{ "kinds": [7, 1, 1], "#e": ["b", "a"], "since": 5, "until": 9, "limit": 2,
             "authors": ["0000000000000000000000000000000000000000000000000000000000000001"] }"##,
    );
    let mut author = [0; 32];
    author[31] = 1;
    assert_eq!(
        ok,
        Filter::new()
            .kinds([1, 7])
            .tag(b'e', ["a", "b"])
            .since(5)
            .until(9)
            .limit(2)
            .authors([author])
    );
    assert_eq!(filter("{}"), Filter::new());

    let err = |json: &str| Filter::from_json(json.as_bytes()).unwrap_err();
    let json_err = |json: &str| match err(json) {
        Error::Json { what, .. } => what,
        e => panic!("{json}: {e:?}"),
    };
    assert_eq!(json_err(r#"{"search":"x"}"#), "unknown filter field");
    assert_eq!(json_err(r#"{"ids":["abcd"]}"#), "wrong hex length");
    assert_eq!(
        json_err(&format!(r#"{{"authors":["{}"]}}"#, "A".repeat(64))),
        "not lowercase hex"
    );
    assert_eq!(json_err(r#"{"kinds":[4294967296]}"#), "kind out of range");
    assert_eq!(json_err(r#"{"kinds":[-1]}"#), "expected integer");
    assert_eq!(json_err(r#"{"kinds":1}"#), "expected '['");
    assert_eq!(
        json_err(r##"{"#1":["x"]}"##),
        "tag filters are single letters"
    );
    assert_eq!(
        json_err(r#"{"kinds":[1]} x"#),
        "trailing bytes after filter"
    );
    assert_eq!(
        err(r#"{"kinds":[1],"kinds":[2]}"#),
        Error::Duplicate("kinds")
    );
    assert_eq!(
        err(r##"{"#p":["a"],"#p":["b"]}"##),
        Error::Duplicate("#tag")
    );
}

#[test]
fn concurrent_pushers_and_consumers_take_each_item_once() {
    let dir = tempfile::tempdir().unwrap();
    let q = Noq::open(dir.path(), small()).unwrap();
    let signer = Signer::new(3);
    let n = 2000;
    let evs: Vec<Vec<u8>> = (0..n)
        .map(|i| signer.event(i, 1 + (i % 2) as u32, &[&["p", &format!("{}", i % 5)]], ""))
        .collect();
    // Consumers take every other p value's items, of either kind.
    let f = filter(r##"{"#p":["0","2","4"],"kinds":[1,2]}"##);
    let pushed = Mutex::new(HashSet::new());
    let taken = Arc::new(Mutex::new(Vec::new()));
    let pushing = AtomicBool::new(true);
    std::thread::scope(|s| {
        s.spawn(|| {
            for (i, ev) in evs.iter().enumerate() {
                let seq = q.push(ev).unwrap().queued().unwrap();
                if i % 5 % 2 == 0 {
                    pushed.lock().insert(seq);
                }
            }
            pushing.store(false, Ordering::Release);
        });
        for _ in 0..4 {
            let taken = taken.clone();
            let (q, f, pushing) = (&q, &f, &pushing);
            s.spawn(move || {
                loop {
                    let done = !pushing.load(Ordering::Acquire);
                    let ls = q.lease(f, 3, LONG);
                    if ls.is_empty() && done {
                        break;
                    }
                    for l in ls {
                        taken.lock().push(l.seq());
                        l.ack().unwrap();
                    }
                }
            });
        }
    });
    let taken = taken.lock();
    let set: HashSet<Seq> = taken.iter().copied().collect();
    assert_eq!(set.len(), taken.len(), "an item was taken twice");
    assert_eq!(set, *pushed.lock());
    assert_eq!(set.len(), 1200);
}
