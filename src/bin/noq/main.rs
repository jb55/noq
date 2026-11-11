//! `noq`: push events into a queue directory, lease or peek them by filter,
//! ack them, and print its stats.
//!
//! A lease lives only as long as the process that holds it, so a lease
//! taken by one `noq lease` ends when it exits and its items are queued
//! again. Consume in one step with `noq lease --ack`, or in two with
//! `noq lease` then `noq ack <id>...` once the events are dealt with.
//!
//! There is no waiting lease here: the queue directory is locked to the
//! process that has it open, so nothing could push while one waited.

mod bench;

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use noq::{Config, Error, Filter, Fsync, Noq, NoteBuilder, NoteRef, Pushed, Seq};

const USAGE: &str = "\
usage: noq [options] <command>

commands:
  push                 queue events from stdin, one json event per line
  peek [FILTER]        print queued events matching FILTER, one per line,
                       leaving them queued
  lease [FILTER]       lease events matching FILTER and print them, one per
                       line; they are queued again when noq exits unless
                       --ack consumes them
  ack ID...            consume the queued events with these ids
  compact              move queued events off mostly-consumed segments so
                       those can be dropped; prints how many moved
  stats                print the queue's size
  bench [FILE]         time pushing FILE's events into a fresh queue and
                       leasing them all back; see bench --help

FILTER is one NIP-01 filter object, e.g. '{\"kinds\":[1]}'; the default,
'{}', matches everything.

peek and lease:
  -n, --limit N        at most N events (default 100)
lease:
  --ttl SECS           how long the lease holds them (default 30)
  --ack                consume each event once it is printed

options:
  -d, --dir DIR        the queue directory (default $NOQ_DIR, else ./noq)
  --segment-size N     bytes per new segment file; k, m, g suffixes (default 64m)
  --fsync WHEN         always, never, or every MS milliseconds (default 100)
  --max-age SECS       expire events this long after their created_at
  --seen-ids N         consumed ids remembered against re-pushes (default 65536)
  -h, --help           print this
";

fn main() -> ExitCode {
    match run(std::env::args().skip(1).collect()) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("noq: {e}");
            ExitCode::from(2)
        }
    }
}

fn run(args: Vec<String>) -> Result<ExitCode, String> {
    let mut args = Args(args.into_iter());
    let mut dir: Option<PathBuf> = None;
    let mut config = Config::default();
    let cmd = loop {
        let Some(arg) = args.next() else {
            return Err(format!("no command\n\n{USAGE}"));
        };
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{USAGE}");
                return Ok(ExitCode::SUCCESS);
            }
            "-d" | "--dir" => dir = Some(args.value(&arg)?.into()),
            "--segment-size" => {
                let v = args.value(&arg)?;
                config.segment_size = parse_size(&v).ok_or(format!("bad {arg}: {v}"))?;
            }
            "--fsync" => {
                config.fsync = match args.value(&arg)?.as_str() {
                    "always" => Fsync::Always,
                    "never" => Fsync::Never,
                    ms => Fsync::Every(Duration::from_millis(args.num(&arg, ms)?)),
                }
            }
            "--max-age" => {
                let v = args.value(&arg)?;
                config.max_age = Some(Duration::from_secs(args.num(&arg, &v)?));
            }
            "--seen-ids" => {
                let v = args.value(&arg)?;
                config.seen_ids = args.num(&arg, &v)?;
            }
            a if a.starts_with('-') => return Err(format!("unknown option {a}\n\n{USAGE}")),
            _ => break arg,
        }
    };
    if cmd == "bench" {
        if args.0.as_slice().iter().any(|a| a == "-h" || a == "--help") {
            print!("{}", bench::USAGE);
            return Ok(ExitCode::SUCCESS);
        }
        let opts = bench::BenchOpts::parse(&mut args)?;
        return bench::bench(dir, config, &opts);
    }
    let dir = dir
        .or_else(|| std::env::var_os("NOQ_DIR").map(PathBuf::from))
        .unwrap_or_else(|| "noq".into());
    let open = || Noq::open(&dir, config).map_err(|e| format!("{}: {e}", dir.display()));
    match cmd.as_str() {
        "push" => {
            args.done()?;
            push(&open()?)
        }
        "peek" | "lease" => {
            let opts = TakeOpts::parse(&mut args, cmd == "lease")?;
            take(&open()?, &opts)
        }
        "ack" => {
            let ids = args
                .map(|a| parse_id(&a).ok_or(format!("bad id: {a}")))
                .collect::<Result<Vec<_>, _>>()?;
            ack(&open()?, &ids)
        }
        "compact" => {
            args.done()?;
            println!("moved {}", open()?.compact());
            Ok(ExitCode::SUCCESS)
        }
        "stats" => {
            args.done()?;
            stats(&open()?)
        }
        c => Err(format!("unknown command {c}\n\n{USAGE}")),
    }
}

/// Queues each line of stdin and prints what became of it, one line each:
///
/// ```text
/// queued <id> <seq>
/// replaced <id> <seq> <old seq>
/// duplicate <id> <seq>
/// seen <id>
/// stale <id> <newer seq>
/// expired <id>
/// invalid <line number> <why>
/// ```
///
/// Exits 1 if any line was invalid.
fn push(q: &Noq) -> Result<ExitCode, String> {
    let stdin = io::stdin().lock();
    let mut out = io::BufWriter::new(io::stdout().lock());
    let mut builder = NoteBuilder::new();
    let mut index = Vec::new();
    let (mut queued, mut not, mut invalid) = (0usize, 0usize, 0usize);
    for (n, line) in stdin.split(b'\n').enumerate() {
        let line = line.map_err(|e| format!("stdin: {e}"))?;
        let json = line.strip_suffix(b"\r").unwrap_or(&line);
        if json.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        index.clear();
        let pushed = builder
            .build(json, &mut index)
            .and_then(|()| NoteRef::new(&index, json))
            .and_then(|note| Ok((hex(note.id()), q.push_note(note)?)));
        let line = match pushed {
            Ok((id, p)) => {
                if p.queued().is_some() {
                    queued += 1;
                } else {
                    not += 1;
                }
                match p {
                    Pushed::Queued(s) => format!("queued {id} {}", seq(s)),
                    Pushed::Replaced { seq: s, old } => {
                        format!("replaced {id} {} {}", seq(s), seq(old))
                    }
                    Pushed::Duplicate(s) => format!("duplicate {id} {}", seq(s)),
                    Pushed::Seen => format!("seen {id}"),
                    Pushed::Stale(s) => format!("stale {id} {}", seq(s)),
                    Pushed::Expired => format!("expired {id}"),
                }
            }
            Err(e @ Error::Io(_)) => return Err(format!("line {}: {e}", n + 1)),
            Err(e) => {
                invalid += 1;
                format!("invalid {} {e}", n + 1)
            }
        };
        if writeln!(out, "{line}").is_err() {
            break;
        }
    }
    let _ = out.flush();
    eprintln!("{queued} queued, {not} not queued, {invalid} invalid");
    Ok(if invalid > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}

struct TakeOpts {
    filter: Filter,
    limit: usize,
    ttl: Duration,
    ack: bool,
    lease: bool,
}

impl TakeOpts {
    fn parse(args: &mut Args, lease: bool) -> Result<TakeOpts, String> {
        let mut o = TakeOpts {
            filter: Filter::new(),
            limit: 100,
            ttl: Duration::from_secs(30),
            ack: false,
            lease,
        };
        let mut filter = None;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-n" | "--limit" => {
                    let v = args.value(&arg)?;
                    o.limit = args.num(&arg, &v)?;
                }
                "--ttl" if lease => o.ttl = args.secs(&arg)?,
                "--ack" if lease => o.ack = true,
                a if a.starts_with('-') && a != "-" => {
                    return Err(format!("unknown option {a}\n\n{USAGE}"));
                }
                _ if filter.is_none() => filter = Some(arg),
                _ => return Err(format!("one filter only: {arg}")),
            }
        }
        if let Some(f) = filter {
            o.filter = Filter::from_json(f.as_bytes()).map_err(|e| format!("filter: {e}"))?;
        }
        Ok(o)
    }
}

/// Prints the events `peek` or `lease` finds, one json per line, exactly as
/// they were pushed. Exits 1 if there were none.
fn take(q: &Noq, o: &TakeOpts) -> Result<ExitCode, String> {
    let mut out = io::stdout().lock();
    let printed = if !o.lease {
        let found = q.peek(&o.filter, o.limit);
        for rec in &found {
            if out
                .write_all(rec.json())
                .and_then(|()| out.write_all(b"\n"))
                .is_err()
            {
                break;
            }
        }
        found.len()
    } else {
        let leases = q.lease(&o.filter, o.limit, o.ttl);
        let n = leases.len();
        for lease in leases {
            // Acked only once it is out: a reader that dies before it gets
            // the line leaves the event queued.
            let written = out
                .write_all(lease.json())
                .and_then(|()| out.write_all(b"\n"))
                .and_then(|()| out.flush());
            if written.is_err() {
                break;
            }
            if o.ack {
                let id = *lease.note().id();
                if let Err(e) = lease.ack() {
                    eprintln!("noq: {}: {e}", hex(&id));
                }
            }
        }
        n
    };
    let _ = out.flush();
    Ok(if printed > 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

/// Consumes the queued events with these ids, printing `acked <id>` or
/// `missing <id>` for each. Exits 1 if any was missing: already consumed,
/// never pushed, or dead.
fn ack(q: &Noq, ids: &[[u8; 32]]) -> Result<ExitCode, String> {
    if ids.is_empty() {
        return Err("ack: no ids".into());
    }
    let leases = q.lease(
        &Filter::new().ids(ids.iter().copied()),
        ids.len(),
        Duration::from_secs(60),
    );
    let mut acked = Vec::with_capacity(leases.len());
    for lease in leases {
        let id = *lease.note().id();
        lease.ack().map_err(|e| format!("{}: {e}", hex(&id)))?;
        acked.push(id);
    }
    let mut missing = false;
    for id in ids {
        if acked.contains(id) {
            println!("acked {}", hex(id));
        } else {
            missing = true;
            println!("missing {}", hex(id));
        }
    }
    Ok(if missing {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}

fn stats(q: &Noq) -> Result<ExitCode, String> {
    let log = q.log();
    let segments = log.segments();
    let pending: u64 = segments.iter().map(|s| s.pending() as u64).sum();
    let records: u64 = segments.iter().map(|s| s.count() as u64).sum();
    let used: u64 = segments.iter().map(|s| s.end() as u64).sum();
    let disk: u64 = segments.iter().map(|s| s.size() as u64).sum();
    let ix = q.index_stats();
    println!("dir       {}", log.dir().display());
    println!("segments  {}", segments.len());
    println!("head      {}", seq(log.head_seq()));
    println!("tail      {}", seq(log.tail_seq()));
    println!("records   {records}");
    println!("pending   {pending}");
    println!("leased    {}", q.leased());
    println!("used      {used}");
    println!("disk      {disk}");
    println!("ids       {}", ix.ids);
    println!("postings  {}", ix.postings);
    println!("seen      {}", ix.seen);
    println!("expiring  {}", ix.expiring);
    Ok(ExitCode::SUCCESS)
}

fn seq(s: Seq) -> String {
    format!("{}:{}", s.segment(), s.offset())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn parse_id(s: &str) -> Option<[u8; 32]> {
    let b = s.as_bytes();
    if b.len() != 64 {
        return None;
    }
    let nib = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut id = [0; 32];
    for (i, pair) in b.chunks(2).enumerate() {
        id[i] = nib(pair[0])? << 4 | nib(pair[1])?;
    }
    Some(id)
}

fn parse_size(s: &str) -> Option<u32> {
    let (n, shift) = match s.as_bytes().last()? {
        b'k' | b'K' => (&s[..s.len() - 1], 10),
        b'm' | b'M' => (&s[..s.len() - 1], 20),
        b'g' | b'G' => (&s[..s.len() - 1], 30),
        _ => (s, 0),
    };
    u32::try_from(n.parse::<u64>().ok()?.checked_shl(shift)?).ok()
}

struct Args(std::vec::IntoIter<String>);

impl Iterator for Args {
    type Item = String;
    fn next(&mut self) -> Option<String> {
        self.0.next()
    }
}

impl Args {
    fn value(&mut self, flag: &str) -> Result<String, String> {
        self.next().ok_or(format!("{flag} needs a value"))
    }

    fn num<T: std::str::FromStr>(&self, flag: &str, v: &str) -> Result<T, String> {
        v.parse().map_err(|_| format!("bad {flag}: {v}"))
    }

    fn positive(&mut self, flag: &str) -> Result<usize, String> {
        let v = self.value(flag)?;
        match self.num(flag, &v)? {
            0 => Err(format!("bad {flag}: {v}")),
            n => Ok(n),
        }
    }

    fn secs(&mut self, flag: &str) -> Result<Duration, String> {
        let v = self.value(flag)?;
        v.parse::<f64>()
            .ok()
            .and_then(|s| Duration::try_from_secs_f64(s).ok())
            .ok_or(format!("bad {flag}: {v}"))
    }

    fn done(&mut self) -> Result<(), String> {
        match self.next() {
            Some(a) => Err(format!("unexpected argument {a}")),
            None => Ok(()),
        }
    }
}
