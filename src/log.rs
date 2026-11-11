//! The segment log: a directory of [`Segment`] files, appended to by one
//! writer and scanned by any number of readers.
//!
//! The newest segment is the tail. An append that does not fit in it seals
//! the tail (flushes it in full) and starts the next segment, so every
//! sealed segment is on disk before a record lands past it. A crash can
//! only tear records in the tail, and opening the log recovers each segment
//! by truncating at its first bad record.
//!
//! Sealing a segment also writes its sidecar (`.idx`, see
//! [`crate::sidecar`]): its records and the keys they are indexed under. A
//! sealed segment with a sound sidecar opens without a crc check, and the
//! queue indexes it from the sidecar without parsing its records, so only
//! the tail is scanned in full.
//!
//! Each segment counts its pending (LIVE or LEASED) records. When a sealed
//! segment's count reaches zero, through [`Log::retire`], it holds only
//! garbage: it leaves the log and its files are unlinked. The tail is never
//! dropped, drained or not, until a rotation seals it.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::builder::NoteBuilder;
use crate::error::Error;
use crate::note::NoteRef;
use crate::segment::{RECORD_HEADER_LEN, RecordRef, Segment, Seq, State, stride};
use crate::sidecar::Sidecar;

/// When appended records are written back to disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fsync {
    /// Before every append returns.
    Always,
    /// By the first append at least this long after the last write-back,
    /// and when the log is dropped.
    Every(Duration),
    /// Only when a segment is sealed, or on [`Log::sync`].
    Never,
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Size of each new segment file. A record must fit in one segment.
    pub segment_size: u32,
    pub fsync: Fsync,
    /// [`Noq`](crate::Noq) only: an event is expired once its
    /// `created_at` is this far in the past, as if it had a NIP-40
    /// `expiration` tag. `None` keeps events until their own tag says.
    pub max_age: Option<Duration>,
    /// [`Noq`](crate::Noq) only: how many ids of consumed events to
    /// remember after their records leave the log, so a relay re-sending
    /// one doesn't queue it again. In memory: forgotten on reopen.
    pub seen_ids: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            segment_size: 64 << 20,
            fsync: Fsync::Every(Duration::from_millis(100)),
            max_age: None,
            seen_ids: 1 << 16,
        }
    }
}

/// An append-only log of records over a directory of segment files.
pub struct Log {
    dir: PathBuf,
    config: Config,
    /// Every segment, oldest first. The last is the tail.
    segments: RwLock<Vec<Arc<Segment>>>,
    /// Segments dropped since open.
    dropped: AtomicU64,
    /// Segments sealed since open.
    sealed: AtomicU64,
    /// Sidecars read on open, until the queue indexes their segments.
    listed: Mutex<HashMap<u32, Sidecar>>,
    writer: Mutex<Writer>,
    /// Held for the log's lifetime so a second process can't open it.
    _lock: File,
}

struct Writer {
    tail: Arc<Segment>,
    builder: NoteBuilder,
    index: Vec<u8>,
    /// The tail's end at the last write-back.
    synced: u32,
    synced_at: Instant,
}

impl Log {
    /// Opens the log in `dir`, creating it if needed, and recovers every
    /// segment in it.
    pub fn open(dir: impl AsRef<Path>, config: Config) -> Result<Log, Error> {
        Log::open_listing(dir.as_ref(), config, false)
    }

    /// Like [`open`](Self::open), but keeps the sidecars it opened sealed
    /// segments from, for the queue to index them from with
    /// [`take_sidecar`](Self::take_sidecar).
    pub(crate) fn open_indexed(dir: impl AsRef<Path>, config: Config) -> Result<Log, Error> {
        Log::open_listing(dir.as_ref(), config, true)
    }

    fn open_listing(dir: &Path, config: Config, keep: bool) -> Result<Log, Error> {
        if config.segment_size < 4096 || !config.segment_size.is_multiple_of(8) {
            return Err(Error::Config(
                "segment_size must be a multiple of 8, at least 4096",
            ));
        }
        let dir = dir.to_owned();
        fs::create_dir_all(&dir)?;
        let lock = File::create(dir.join("LOCK"))?;
        lock.try_lock().map_err(|_| Error::Locked)?;

        let mut ids: Vec<u32> = fs::read_dir(&dir)?
            .filter_map(|e| Segment::parse_file_name(e.ok()?.file_name().to_str()?))
            .collect();
        ids.sort_unstable();
        let mut listed = HashMap::new();
        let mut segments = Vec::with_capacity(ids.len());
        let mut wrote = false;
        for (i, &id) in ids.iter().enumerate() {
            let path = dir.join(Segment::file_name(id));
            let sealed = i + 1 < ids.len();
            let sc = sealed.then(|| Sidecar::read(&dir, id)).flatten();
            let mut seg = None;
            if let Some(sc) = sc {
                let offs: Vec<u32> = sc.records.iter().map(|e| e.off).collect();
                seg = Segment::open_listed(&path, id, sc.end, &offs)?;
                if seg.is_some() && keep {
                    listed.insert(id, sc);
                }
            }
            let listed_it = seg.is_some();
            let seg = match seg {
                Some(seg) => seg,
                None => Segment::open(&path, id, config.segment_size)?,
            };
            // Sealed by a process that died before writing its sidecar, or
            // one whose sidecar didn't check out.
            if sealed && !listed_it && seg.pending() > 0 {
                wrote |= Sidecar::build(&seg).write(&dir).is_ok();
            }
            segments.push(Arc::new(seg));
        }
        if segments.is_empty() {
            segments.push(Arc::new(Segment::create(&dir, 0, config.segment_size)?));
            wrote = true;
        }
        // Drained segments left by the last process, or whose unlink failed.
        let tail_id = segments.last().unwrap().id();
        for seg in segments.extract_if(.., |s| s.id() != tail_id && s.pending() == 0) {
            listed.remove(&seg.id());
            fs::remove_file(seg.path())?;
        }
        // Sidecars whose segment is gone, and half-written ones.
        for e in fs::read_dir(&dir)? {
            let name = e?.file_name();
            let Some(name) = name.to_str() else { continue };
            let orphan = match name.strip_suffix(".idx") {
                Some(seg) => Segment::parse_file_name(&format!("{seg}.seg"))
                    .is_some_and(|id| segments.binary_search_by_key(&id, |s| s.id()).is_err()),
                None => name.ends_with(".idx.tmp"),
            };
            if orphan {
                let _ = fs::remove_file(dir.join(name));
            }
        }
        if wrote {
            sync_dir(&dir)?;
        }

        let tail = segments.last().unwrap().clone();
        Ok(Log {
            writer: Mutex::new(Writer {
                synced: tail.end(),
                tail,
                builder: NoteBuilder::new(),
                index: Vec::new(),
                synced_at: Instant::now(),
            }),
            dir,
            config,
            segments: RwLock::new(segments),
            dropped: AtomicU64::new(0),
            sealed: AtomicU64::new(0),
            listed: Mutex::new(listed),
            _lock: lock,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Parses and verifies one event and appends it as a LIVE record.
    pub fn append(&self, json: &[u8]) -> Result<Seq, Error> {
        let mut w = self.writer.lock();
        let w = &mut *w;
        w.index.clear();
        w.builder.build(json, &mut w.index)?;
        self.append_indexed(w, json)
    }

    /// Appends an event whose index was already built (and its id and
    /// signature checked) by the caller.
    pub fn append_note(&self, note: NoteRef<'_>) -> Result<Seq, Error> {
        let mut w = self.writer.lock();
        let w = &mut *w;
        w.index.clear();
        w.index.extend_from_slice(note.index());
        self.append_indexed(w, note.json())
    }

    fn append_indexed(&self, w: &mut Writer, json: &[u8]) -> Result<Seq, Error> {
        let total_len = RECORD_HEADER_LEN + w.index.len() + json.len();
        if stride(total_len) > self.config.segment_size as usize {
            return Err(Error::TooLarge("record for the segment size"));
        }
        // SAFETY: appends only happen under the writer lock.
        let seq = match unsafe { w.tail.append(&w.index, json) } {
            Some(seq) => seq,
            None => {
                self.rotate(w)?;
                unsafe { w.tail.append(&w.index, json) }.expect("a record fits in an empty segment")
            }
        };
        match self.config.fsync {
            Fsync::Always => self.write_back(w)?,
            Fsync::Every(every) if w.synced_at.elapsed() >= every => self.write_back(w)?,
            _ => {}
        }
        Ok(seq)
    }

    /// Seals the tail and starts the next segment.
    fn rotate(&self, w: &mut Writer) -> Result<(), Error> {
        w.tail.flush()?;
        // Best effort: without it the segment is only slower to open.
        let _ = Sidecar::build(&w.tail).write(&self.dir);
        let next = Arc::new(Segment::create(
            &self.dir,
            w.tail.id() + 1,
            self.config.segment_size,
        )?);
        sync_dir(&self.dir)?;
        self.segments.write().push(next.clone());
        let sealed = std::mem::replace(&mut w.tail, next);
        self.sealed.fetch_add(1, Ordering::Release);
        w.synced = 0;
        w.synced_at = Instant::now();
        // Drained while it was the tail, so nothing reclaimed it then.
        if sealed.pending() == 0 {
            self.reclaim(&sealed);
        }
        Ok(())
    }

    /// Moves a pending record (`from` LIVE or LEASED) to a garbage state
    /// (`to` ACKED or DEAD), if it is still in `from`. Returns whether it
    /// moved. The record's segment is dropped if this was its last pending
    /// record and it is not the tail.
    ///
    /// Every such move must go through here, not [`RecordRef::cas_state`],
    /// or the segment's pending count never reaches zero. A LEASED record
    /// belongs to the [`Lease`](crate::Lease) holding it; retire it only
    /// through that.
    pub fn retire(&self, rec: &Record, from: State, to: State) -> bool {
        debug_assert!(matches!(from, State::Live | State::Leased));
        debug_assert!(matches!(to, State::Acked | State::Dead));
        if !rec.get().cas_state(from, to) {
            return false;
        }
        if rec.seg.release() {
            self.reclaim(&rec.seg);
        }
        true
    }

    /// Drops `seg` from the log and unlinks its files, if it is drained and
    /// not the tail. Records still held keep its memory mapped; the unlinked
    /// file's space comes back once the last is dropped.
    ///
    /// An unlink that fails leaves the file behind, and the next open drops
    /// it, so this never fails the caller.
    fn reclaim(&self, seg: &Arc<Segment>) {
        {
            let mut segs = self.segments.write();
            // The writer pushes the next tail under this lock before it
            // checks the old one, so a segment seen as the tail here is
            // reclaimed by the rotation instead.
            if segs.last().is_some_and(|t| Arc::ptr_eq(t, seg)) || seg.pending() != 0 {
                return;
            }
            let Some(i) = segs.iter().position(|s| Arc::ptr_eq(s, seg)) else {
                // Already reclaimed by a racing caller.
                return;
            };
            segs.remove(i);
            self.dropped.fetch_add(1, Ordering::Release);
        }
        self.listed.lock().remove(&seg.id());
        let _ = fs::remove_file(seg.path());
        let _ = fs::remove_file(Sidecar::path(&self.dir, seg.id()));
    }

    fn write_back(&self, w: &mut Writer) -> Result<(), Error> {
        let end = w.tail.end();
        w.tail.flush_range(w.synced, end)?;
        w.synced = end;
        w.synced_at = Instant::now();
        Ok(())
    }

    /// Writes every appended record back to disk now.
    pub fn sync(&self) -> Result<(), Error> {
        let mut w = self.writer.lock();
        self.write_back(&mut w)
    }

    /// A snapshot of the segments, oldest first.
    pub fn segments(&self) -> Vec<Arc<Segment>> {
        self.segments.read().clone()
    }

    /// How many segments have been dropped since the log was opened. Any
    /// change means some seqs are gone, not necessarily the oldest.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Acquire)
    }

    /// How many segments have been sealed since the log was opened.
    pub fn sealed(&self) -> u64 {
        self.sealed.load(Ordering::Acquire)
    }

    /// The sidecar segment `id` was opened from, the first time it is
    /// asked for.
    pub(crate) fn take_sidecar(&self, id: u32) -> Option<Sidecar> {
        self.listed.lock().remove(&id)
    }

    /// The seq of the oldest segment's start. Every seq before it is gone.
    pub fn head_seq(&self) -> Seq {
        Seq::new(self.segments.read()[0].id(), 0)
    }

    /// The seq the next append in the tail would get.
    pub fn tail_seq(&self) -> Seq {
        let w = self.writer.lock();
        Seq::new(w.tail.id(), w.tail.end())
    }

    /// The record at `seq`, if it is committed and its segment still exists.
    /// `seq` must be one the log handed out.
    pub fn get(&self, seq: Seq) -> Option<Record> {
        let seg = self.segment(seq.segment())?;
        Record::new(seg, seq.offset())
    }

    fn segment(&self, id: u32) -> Option<Arc<Segment>> {
        let segs = self.segments.read();
        let i = segs.binary_search_by_key(&id, |s| s.id()).ok()?;
        Some(segs[i].clone())
    }

    /// The first segment with an id at or after `id`.
    fn segment_from(&self, id: u32) -> Option<Arc<Segment>> {
        let segs = self.segments.read();
        let i = segs.partition_point(|s| s.id() < id);
        segs.get(i).cloned()
    }

    /// Every committed record from `from` on, in seq order, records of every
    /// state included. The scan follows the log as it grows: records
    /// appended while it runs, in later segments too, are returned once it
    /// gets to them.
    pub fn scan(&self, from: Seq) -> Scan<'_> {
        let seg = self.segment_from(from.segment());
        let off = match &seg {
            Some(s) if s.id() == from.segment() => from.offset(),
            _ => 0,
        };
        Scan {
            log: self,
            seg,
            off,
        }
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        if self.config.fsync != Fsync::Never {
            let _ = self.write_back(&mut self.writer.lock());
        }
    }
}

impl std::fmt::Debug for Log {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Log")
            .field("dir", &self.dir)
            .field("segments", &*self.segments.read())
            .finish()
    }
}

fn sync_dir(dir: &Path) -> Result<(), Error> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// A committed record that keeps its segment mapped while it is held.
#[derive(Clone)]
pub struct Record {
    seg: Arc<Segment>,
    off: u32,
}

impl Record {
    fn new(seg: Arc<Segment>, off: u32) -> Option<Record> {
        seg.record(off)?;
        Some(Record { seg, off })
    }

    /// The record, borrowed from its segment.
    pub fn get(&self) -> RecordRef<'_> {
        self.seg
            .record(self.off)
            .expect("a Record is always committed")
    }

    pub fn seq(&self) -> Seq {
        Seq::new(self.seg.id(), self.off)
    }

    pub fn state(&self) -> State {
        self.get().state()
    }

    pub fn json(&self) -> &[u8] {
        self.get().json()
    }

    pub fn note(&self) -> NoteRef<'_> {
        self.get().note()
    }

    pub fn segment(&self) -> &Arc<Segment> {
        &self.seg
    }
}

impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.get().fmt(f)
    }
}

/// Records of a [`Log`] in seq order; see [`Log::scan`].
pub struct Scan<'a> {
    log: &'a Log,
    seg: Option<Arc<Segment>>,
    off: u32,
}

impl Iterator for Scan<'_> {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        loop {
            let seg = self.seg.as_ref()?;
            if let Some(r) = seg.record(self.off) {
                let off = self.off;
                self.off = r.next();
                return Some(Record {
                    seg: seg.clone(),
                    off,
                });
            }
            // Past this segment's end. Move on only once a later segment
            // exists: until then this one is the tail and may still grow.
            let next = self.log.segment_from(seg.id().checked_add(1)?)?;
            // The tail was sealed before `next` was published; anything it
            // got after our last look is in it now.
            if seg.record(self.off).is_some() {
                continue;
            }
            self.seg = Some(next);
            self.off = 0;
        }
    }
}
