//! The segment log: a directory of [`Segment`] files, appended to by one
//! writer and scanned by any number of readers.
//!
//! The newest segment is the tail. An append that does not fit in it seals
//! the tail (flushes it in full) and starts the next segment, so every
//! sealed segment is on disk before a record lands past it. A crash can
//! only tear records in the tail, and opening the log recovers each segment
//! by truncating at its first bad record.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::builder::NoteBuilder;
use crate::error::Error;
use crate::note::NoteRef;
use crate::segment::{RECORD_HEADER_LEN, RecordRef, Segment, Seq, State, stride};

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
}

impl Default for Config {
    fn default() -> Config {
        Config {
            segment_size: 64 << 20,
            fsync: Fsync::Every(Duration::from_millis(100)),
        }
    }
}

/// An append-only log of records over a directory of segment files.
pub struct Log {
    dir: PathBuf,
    config: Config,
    /// Every segment, oldest first. The last is the tail.
    segments: RwLock<Vec<Arc<Segment>>>,
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
        if config.segment_size < 4096 || !config.segment_size.is_multiple_of(8) {
            return Err(Error::Config(
                "segment_size must be a multiple of 8, at least 4096",
            ));
        }
        let dir = dir.as_ref().to_owned();
        fs::create_dir_all(&dir)?;
        let lock = File::create(dir.join("LOCK"))?;
        lock.try_lock().map_err(|_| Error::Locked)?;

        let mut ids: Vec<u32> = fs::read_dir(&dir)?
            .filter_map(|e| Segment::parse_file_name(e.ok()?.file_name().to_str()?))
            .collect();
        ids.sort_unstable();
        let mut segments = ids
            .into_iter()
            .map(|id| {
                Segment::open(&dir.join(Segment::file_name(id)), id, config.segment_size)
                    .map(Arc::new)
            })
            .collect::<Result<Vec<_>, Error>>()?;
        if segments.is_empty() {
            segments.push(Arc::new(Segment::create(&dir, 0, config.segment_size)?));
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
        let next = Arc::new(Segment::create(
            &self.dir,
            w.tail.id() + 1,
            self.config.segment_size,
        )?);
        sync_dir(&self.dir)?;
        self.segments.write().push(next.clone());
        w.tail = next;
        w.synced = 0;
        w.synced_at = Instant::now();
        Ok(())
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
