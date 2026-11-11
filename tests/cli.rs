//! The `noq` binary, run against a temp queue directory.

mod common;

use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use common::Signer;

fn noq(dir: &Path, args: &[&str], stdin: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_noq"))
        .arg("--dir")
        .arg(dir)
        .args(["--fsync", "never"])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin).unwrap();
    child.wait_with_output().unwrap()
}

fn lines(o: &Output) -> Vec<String> {
    String::from_utf8(o.stdout.clone())
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn id(json: &[u8]) -> String {
    let v: serde_json::Value = serde_json::from_slice(json).unwrap();
    v["id"].as_str().unwrap().to_owned()
}

fn stat(dir: &Path, key: &str) -> u64 {
    let o = noq(dir, &["stats"], b"");
    assert!(o.status.success());
    lines(&o)
        .iter()
        .find_map(|l| l.strip_prefix(key)?.trim().parse().ok())
        .unwrap_or_else(|| panic!("no {key} in stats"))
}

#[test]
fn push_reports_each_line() {
    let dir = tempfile::tempdir().unwrap();
    let s = Signer::new(1);
    let (v1, v2, old) = (
        s.event(100, 0, &[], "v1"),
        s.event(200, 0, &[], "v2"),
        s.event(50, 0, &[], "old"),
    );
    let mut input = Vec::new();
    for l in [&v1[..], b"", &v2, &v2, b"{\"nope\":1}", &old] {
        input.extend_from_slice(l);
        input.extend_from_slice(b"\r\n");
    }
    let o = noq(dir.path(), &["push"], &input);
    assert_eq!(o.status.code(), Some(1), "an invalid line fails the push");
    let out = lines(&o);
    let words: Vec<Vec<&str>> = out.iter().map(|l| l.split(' ').collect()).collect();
    assert_eq!(words.len(), 5, "{out:?}");
    assert_eq!(words[0][..2], ["queued", &id(&v1)]);
    assert_eq!(words[1][..2], ["replaced", &id(&v2)]);
    assert_eq!(words[1][3], words[0][2], "v2 replaced v1's seq");
    assert_eq!(words[2][..3], ["duplicate", &id(&v2), words[1][2]]);
    // Line numbers count the blank line.
    assert_eq!(words[3][..2], ["invalid", "5"]);
    assert_eq!(words[4][..3], ["stale", &id(&old), words[1][2]]);
    assert!(String::from_utf8_lossy(&o.stderr).contains("2 queued, 2 not queued, 1 invalid"));
}

#[test]
fn peek_lease_and_ack() {
    let dir = tempfile::tempdir().unwrap();
    let s = Signer::new(1);
    let evs: Vec<Vec<u8>> = (0..4)
        .map(|i| s.event(i, 1 + i as u32 % 2, &[], ""))
        .collect();
    let o = noq(dir.path(), &["push"], &evs.join(&b'\n'));
    assert!(o.status.success());
    assert_eq!(stat(dir.path(), "pending"), 4);

    // Printed exactly as pushed, oldest first.
    let o = noq(dir.path(), &["peek", r#"{"kinds":[1]}"#], b"");
    let want: Vec<String> = [&evs[0], &evs[2]]
        .iter()
        .map(|e| String::from_utf8(e.to_vec()).unwrap())
        .collect();
    assert_eq!(lines(&o), want);
    assert_eq!(lines(&noq(dir.path(), &["peek", "-n", "1"], b"")).len(), 1);

    // A lease without --ack ends with the process: the item is queued again.
    let o = noq(dir.path(), &["lease", r#"{"kinds":[2]}"#, "-n", "1"], b"");
    assert_eq!(lines(&o), [String::from_utf8(evs[1].clone()).unwrap()]);
    assert_eq!(stat(dir.path(), "pending"), 4);

    // --ack consumes what it prints.
    let o = noq(dir.path(), &["lease", r#"{"kinds":[2]}"#, "--ack"], b"");
    assert_eq!(lines(&o).len(), 2);
    assert_eq!(stat(dir.path(), "pending"), 2);
    let o = noq(dir.path(), &["lease", r#"{"kinds":[2]}"#], b"");
    assert_eq!(o.status.code(), Some(1), "nothing left to lease");

    // ack by id, and an id that isn't queued.
    let gone = id(&evs[1]);
    let o = noq(dir.path(), &["ack", &id(&evs[0]), &gone], b"");
    assert_eq!(o.status.code(), Some(1));
    assert_eq!(
        lines(&o),
        [format!("acked {}", id(&evs[0])), format!("missing {gone}")]
    );
    assert_eq!(stat(dir.path(), "pending"), 1);
}

#[test]
fn bad_arguments_exit_2() {
    let dir = tempfile::tempdir().unwrap();
    for args in [
        &[][..],
        &["nope"],
        &["--fsync", "sometimes", "stats"],
        &["peek", "{\"kinds\":\"x\"}"],
        &["peek", "{}", "{}"],
        &["ack", "abc"],
        &["stats", "extra"],
    ] {
        let o = noq(dir.path(), args, b"");
        assert_eq!(o.status.code(), Some(2), "{args:?}");
    }
    assert!(noq(dir.path(), &["--help"], b"").status.success());
}

#[test]
fn compact_moves_what_is_left_on_consumed_segments() {
    let dir = tempfile::tempdir().unwrap();
    let s = Signer::new(1);
    let evs: Vec<Vec<u8>> = (0..60)
        .map(|i| s.event(i, if i % 5 == 0 { 2 } else { 1 }, &[], ""))
        .collect();
    let o = noq(
        dir.path(),
        &["--segment-size", "4k", "push"],
        &evs.join(&b'\n'),
    );
    assert!(o.status.success());
    assert!(stat(dir.path(), "segments") > 3);
    let o = noq(dir.path(), &["lease", r#"{"kinds":[1]}"#, "--ack"], b"");
    assert_eq!(lines(&o).len(), 48);

    let o = noq(dir.path(), &["compact"], b"");
    assert!(o.status.success());
    let moved: u64 = lines(&o)[0]
        .strip_prefix("moved ")
        .unwrap()
        .parse()
        .unwrap();
    assert!(moved > 0, "{:?}", lines(&o));
    assert_eq!(lines(&noq(dir.path(), &["compact"], b"")), ["moved 0"]);
    assert_eq!(stat(dir.path(), "pending"), 12);
    let mut got = lines(&noq(dir.path(), &["peek", r#"{"kinds":[2]}"#], b""));
    let mut want: Vec<String> = evs
        .iter()
        .step_by(5)
        .map(|e| String::from_utf8(e.clone()).unwrap())
        .collect();
    got.sort();
    want.sort();
    assert_eq!(got, want);
}

#[test]
fn bench_pushes_and_drains_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let s = Signer::new(1);
    let mut input = Vec::new();
    for i in 0..60 {
        let ev = s.event(i, if i % 3 == 0 { 2 } else { 1 }, &[], "");
        // Bare events and relay messages both go in.
        if i % 2 == 0 {
            input.extend_from_slice(&ev);
        } else {
            input.extend_from_slice(b"[\"EVENT\",\"sub\",");
            input.extend_from_slice(&ev);
            input.push(b']');
        }
        input.push(b'\n');
    }
    input.extend_from_slice(b"{\"nope\":1}\n");
    let file = dir.path().join("events.jsonl");
    std::fs::write(&file, &input).unwrap();
    let q = dir.path().join("q");
    let bench = |extra: &[&str]| {
        let mut args = vec!["--segment-size", "4k", "bench", file.to_str().unwrap()];
        args.extend_from_slice(&["--threads", "3", "--consumers", "2", "--batch", "7"]);
        args.extend_from_slice(extra);
        noq(&q, &args, b"")
    };
    let field = |out: &[String], prefix: &str| -> String {
        out.iter()
            .find_map(|l| l.strip_prefix(prefix))
            .unwrap_or_else(|| panic!("no {prefix:?} in {out:?}"))
            .trim()
            .to_owned()
    };

    let o = bench(&[]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let out = lines(&o);
    assert!(field(&out, "input").starts_with("61 events"), "{out:?}");
    assert!(
        out.contains(&"         60 queued, 0 not queued, 1 invalid".into()),
        "{out:?}"
    );
    assert!(field(&out, "lease").starts_with("60 acked"), "{out:?}");
    assert!(field(&out, "drained").ends_with("; 0 pending"), "{out:?}");
    assert!(!q.exists(), "the queue is removed after");

    // A filter leases only what it matches; --keep leaves the queue.
    let o = bench(&["--filter", r#"{"kinds":[2]}"#, "--keep"]);
    assert!(o.status.success());
    let out = lines(&o);
    assert!(field(&out, "lease").starts_with("20 acked"), "{out:?}");
    assert!(field(&out, "drained").ends_with("; 40 pending"), "{out:?}");
    assert_eq!(stat(&q, "pending"), 40);

    // Not on a queue that has things in it.
    assert_eq!(bench(&[]).status.code(), Some(2));
    assert_eq!(stat(&q, "pending"), 40);
}
