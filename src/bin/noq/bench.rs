//! `noq bench`: push a file of events into a fresh queue, then lease and
//! ack all of it, timing each phase and measuring the disk and memory the
//! queue took. `bench/compare.sh` runs the same file through nostrdb and
//! strfry for comparison.

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use memmap2::Mmap;
use noq::{Config, Error, Filter, Noq, NoteBuilder, NoteRef};

use super::Args;

pub const USAGE: &str = "\
bench [FILE]:
  -n, --events N       use only the first N events of FILE
  --no-verify          push without checking ids and signatures
  --threads N          push threads (default: one per core)
  --consumers N        lease threads (default 1)
  --batch N            events per lease (default 100)
  --filter FILTER      lease only what matches (default '{}')
  --keep               keep the queue directory afterwards

FILE (default stdin) holds one event per line, bare or as a relay's
[\"EVENT\",<sub>,<event>] message. The queue goes in --dir if given, which
must be empty, else in a temporary directory.
";

/// Events claimed per grab of the shared cursor: big enough that threads
/// don't fight over it, small enough that they finish together.
const CHUNK: usize = 256;

pub struct BenchOpts {
    file: Option<PathBuf>,
    events: Option<usize>,
    verify: bool,
    threads: usize,
    consumers: usize,
    batch: usize,
    filter: Filter,
    filter_json: String,
    keep: bool,
}

impl BenchOpts {
    pub fn parse(args: &mut Args) -> Result<BenchOpts, String> {
        let mut o = BenchOpts {
            file: None,
            events: None,
            verify: true,
            threads: std::thread::available_parallelism().map_or(1, |n| n.get()),
            consumers: 1,
            batch: 100,
            filter: Filter::new(),
            filter_json: "{}".into(),
            keep: false,
        };
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-n" | "--events" => {
                    let v = args.value(&arg)?;
                    o.events = Some(args.num(&arg, &v)?);
                }
                "--no-verify" => o.verify = false,
                "--threads" => o.threads = args.positive(&arg)?,
                "--consumers" => o.consumers = args.positive(&arg)?,
                "--batch" => o.batch = args.positive(&arg)?,
                "--filter" => {
                    o.filter_json = args.value(&arg)?;
                    o.filter = Filter::from_json(o.filter_json.as_bytes())
                        .map_err(|e| format!("filter: {e}"))?;
                }
                "--keep" => o.keep = true,
                a if a.starts_with('-') && a != "-" => {
                    return Err(format!("unknown option {a}\n\n{}", super::USAGE));
                }
                _ if o.file.is_none() => o.file = Some(arg.into()),
                _ => return Err(format!("one file only: {arg}")),
            }
        }
        Ok(o)
    }
}

/// The bench's input: a mapped file, or stdin read into memory.
enum Input {
    Mapped(Mmap),
    Read(Vec<u8>),
}

impl Input {
    fn open(file: Option<&Path>) -> Result<Input, String> {
        match file.filter(|f| f.as_os_str() != "-") {
            Some(f) => {
                let file = fs::File::open(f).map_err(|e| format!("{}: {e}", f.display()))?;
                // SAFETY: a bench input nobody should be writing while it runs.
                let map =
                    unsafe { Mmap::map(&file) }.map_err(|e| format!("{}: {e}", f.display()))?;
                Ok(Input::Mapped(map))
            }
            None => {
                let mut buf = Vec::new();
                io::stdin()
                    .read_to_end(&mut buf)
                    .map_err(|e| format!("stdin: {e}"))?;
                Ok(Input::Read(buf))
            }
        }
    }

    fn bytes(&self) -> &[u8] {
        match self {
            Input::Mapped(m) => m,
            Input::Read(v) => v,
        }
    }
}

/// The event json on each non-blank line, unwrapped from a relay message
/// if it is in one: everything from the line's first `{` to its last `}`.
fn events(input: &[u8], max: Option<usize>) -> Vec<&[u8]> {
    input
        .split(|&c| c == b'\n')
        .map(|l| l.strip_suffix(b"\r").unwrap_or(l))
        .filter(|l| !l.iter().all(u8::is_ascii_whitespace))
        .map(|l| match l.first() {
            Some(b'[') => {
                let start = l.iter().position(|&c| c == b'{');
                let end = l.iter().rposition(|&c| c == b'}');
                match (start, end) {
                    (Some(s), Some(e)) if s < e => &l[s..=e],
                    _ => l,
                }
            }
            _ => l,
        })
        .take(max.unwrap_or(usize::MAX))
        .collect()
}

#[derive(Default)]
struct Counts {
    queued: AtomicUsize,
    not_queued: AtomicUsize,
    invalid: AtomicUsize,
    queued_bytes: AtomicUsize,
}

pub fn bench(dir: Option<PathBuf>, config: Config, o: &BenchOpts) -> Result<ExitCode, String> {
    let input = Input::open(o.file.as_deref())?;
    let evs = events(input.bytes(), o.events);
    let json_bytes: usize = evs.iter().map(|e| e.len()).sum();
    let name = o
        .file
        .as_deref()
        .map_or("stdin".into(), |f| f.display().to_string());
    println!(
        "input    {} events, {} json ({name})",
        evs.len(),
        bytes(json_bytes as u64)
    );

    let (dir, existed) = match dir {
        Some(d) => {
            let existed = d.exists();
            if existed
                && fs::read_dir(&d)
                    .map_err(|e| format!("{}: {e}", d.display()))?
                    .next()
                    .is_some()
            {
                return Err(format!(
                    "{}: not empty; bench wants a fresh queue",
                    d.display()
                ));
            }
            (d, existed)
        }
        None => (
            std::env::temp_dir().join(format!("noq-bench-{}", std::process::id())),
            false,
        ),
    };
    let result = run(&dir, config, o, &evs, json_bytes);
    if !o.keep {
        let _ = fs::remove_dir_all(&dir);
        if existed {
            let _ = fs::create_dir(&dir);
        }
    } else {
        println!("kept     {}", dir.display());
    }
    result
}

fn run(
    dir: &Path,
    config: Config,
    o: &BenchOpts,
    evs: &[&[u8]],
    json_bytes: usize,
) -> Result<ExitCode, String> {
    let open = || Noq::open(dir, config).map_err(|e| format!("{}: {e}", dir.display()));
    let q = open()?;

    let counts = Counts::default();
    let cursor = AtomicUsize::new(0);
    let failed = parking_lot::Mutex::new(None);
    let t = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..o.threads {
            s.spawn(|| {
                let mut builder = NoteBuilder::new();
                let mut index = Vec::new();
                loop {
                    let at = cursor.fetch_add(CHUNK, Ordering::Relaxed);
                    let Some(chunk) = evs.get(at..evs.len().min(at + CHUNK)) else {
                        break;
                    };
                    for json in chunk {
                        index.clear();
                        let built = if o.verify {
                            builder.build(json, &mut index)
                        } else {
                            builder.build_unverified(json, &mut index)
                        };
                        let pushed = built
                            .and_then(|()| NoteRef::new(&index, json))
                            .and_then(|note| q.push_note(note));
                        match pushed {
                            Ok(p) if p.queued().is_some() => {
                                counts.queued.fetch_add(1, Ordering::Relaxed);
                                counts.queued_bytes.fetch_add(json.len(), Ordering::Relaxed);
                            }
                            Ok(_) => {
                                counts.not_queued.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(e @ Error::Io(_)) => {
                                failed.lock().get_or_insert(e);
                                cursor.store(evs.len(), Ordering::Relaxed);
                                return;
                            }
                            Err(_) => {
                                counts.invalid.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            });
        }
    });
    let push = t.elapsed();
    if let Some(e) = failed.into_inner() {
        return Err(format!("push: {e}"));
    }
    let queued = counts.queued.into_inner();
    println!(
        "push     {} events in {}: {}, {}/s ({} thread{}, {})",
        evs.len(),
        secs(push),
        rate(evs.len(), push),
        bytes((json_bytes as f64 / push.as_secs_f64()) as u64),
        o.threads,
        plural(o.threads),
        if o.verify { "verified" } else { "unverified" },
    );
    println!(
        "         {queued} queued, {} not queued, {} invalid",
        counts.not_queued.into_inner(),
        counts.invalid.into_inner()
    );

    let t = Instant::now();
    q.log().sync().map_err(|e| format!("sync: {e}"))?;
    println!("sync     {}", secs(t.elapsed()));

    let full = Disk::of(&q);
    let queued_bytes = counts.queued_bytes.into_inner() as u64;
    println!(
        "disk     {full}; {:.2}x the queued json",
        full.total() as f64 / queued_bytes.max(1) as f64
    );
    println!("         {} pending", full.pending);
    if let Some(rss) = anon_rss() {
        println!("memory   {} anonymous rss", bytes(rss));
    }

    drop(q);
    let t = Instant::now();
    let q = open()?;
    println!("reopen   {}", secs(t.elapsed()));

    let acked = AtomicUsize::new(0);
    let leases = AtomicUsize::new(0);
    let failed = parking_lot::Mutex::new(None);
    let ttl = Duration::from_secs(600);
    let t = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..o.consumers {
            s.spawn(|| {
                loop {
                    let got = q.lease(&o.filter, o.batch, ttl);
                    if got.is_empty() {
                        break;
                    }
                    leases.fetch_add(1, Ordering::Relaxed);
                    let n = got.len();
                    for lease in got {
                        if let Err(e) = lease.ack() {
                            failed.lock().get_or_insert(e);
                            return;
                        }
                    }
                    acked.fetch_add(n, Ordering::Relaxed);
                }
            });
        }
    });
    let lease = t.elapsed();
    if let Some(e) = failed.into_inner() {
        return Err(format!("ack: {e}"));
    }
    let acked = acked.into_inner();
    println!(
        "lease    {acked} acked in {}: {} ({} consumer{}, {} leases of up to {}, filter {})",
        secs(lease),
        rate(acked, lease),
        o.consumers,
        plural(o.consumers),
        leases.into_inner(),
        o.batch,
        o.filter_json,
    );

    let drained = Disk::of(&q);
    println!("drained  {drained}; {} pending", drained.pending);
    Ok(ExitCode::SUCCESS)
}

/// What the queue holds on disk: the bytes written into its segments,
/// the segment files' size (each is made at full size, sparse, and
/// filled as it goes), and its sidecars. Logical sizes, not blocks: those
/// depend on the filesystem (compressed and accounted late on zfs).
struct Disk {
    segs: usize,
    used: u64,
    seg_len: u64,
    pending: u64,
    idxs: usize,
    idx_len: u64,
}

impl Disk {
    fn of(q: &Noq) -> Disk {
        let segments = q.log().segments();
        let mut d = Disk {
            segs: segments.len(),
            used: segments.iter().map(|s| s.end() as u64).sum(),
            seg_len: segments.iter().map(|s| s.size() as u64).sum(),
            pending: segments.iter().map(|s| s.pending() as u64).sum(),
            idxs: 0,
            idx_len: 0,
        };
        let entries = fs::read_dir(q.log().dir()).into_iter().flatten().flatten();
        for entry in entries.filter(|e| e.file_name().to_string_lossy().ends_with(".idx")) {
            d.idxs += 1;
            d.idx_len += entry.metadata().map_or(0, |m| m.len());
        }
        d
    }

    /// What the queue's data takes: segment bytes written plus sidecars.
    fn total(&self) -> u64 {
        self.used + self.idx_len
    }
}

impl std::fmt::Display for Disk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} in {} segment{} ({} of files), {} in {} sidecar{}",
            bytes(self.total()),
            bytes(self.used),
            self.segs,
            plural(self.segs),
            bytes(self.seg_len),
            bytes(self.idx_len),
            self.idxs,
            plural(self.idxs),
        )
    }
}

/// This process's anonymous resident memory: the heap and the indexes,
/// not the mapped input or segments.
fn anon_rss() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let kb = status.lines().find_map(|l| l.strip_prefix("RssAnon:"))?;
    Some(kb.trim().strip_suffix("kB")?.trim().parse::<u64>().ok()? * 1024)
}

fn secs(d: Duration) -> String {
    format!("{:.3} s", d.as_secs_f64())
}

fn rate(n: usize, d: Duration) -> String {
    format!("{:.0} ev/s", n as f64 / d.as_secs_f64())
}

fn bytes(n: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut v = n as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unwraps_relay_messages() {
        let input = b"{\"a\":1}\r\n\n[\"EVENT\",\"s\",{\"b\":{\"c\":2}}]\n  \n[\"EVENT\",\"x\"]\n";
        let got = events(input, None);
        assert_eq!(
            got,
            [
                &b"{\"a\":1}"[..],
                b"{\"b\":{\"c\":2}}",
                b"[\"EVENT\",\"x\"]"
            ]
        );
        assert_eq!(events(input, Some(1)).len(), 1);
    }

    #[test]
    fn formats_bytes() {
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(1536), "1.5 KiB");
        assert_eq!(bytes(3 << 30), "3.0 GiB");
    }
}
