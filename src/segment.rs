//! One segment file: a fixed-size mmap that records are appended to back to
//! back.
//!
//! ```text
//!  off  RecordHeader (16 bytes, little endian)
//!    0    total_len  u32    header + index + json, without the padding
//!    4    state      u8     see State
//!    5    ver        u8     RECORD_VERSION
//!    6    _          u16    zero
//!    8    note_len   u32    length of the span index
//!   12    crc32c     u32    over bytes 0..4 and 5..12 of the header, then the body
//!   16  index     [u8; note_len]     the note's span index (crate::note)
//!       json      [u8]               the event's json, verbatim
//!       pad       to the next multiple of 8
//! ```
//!
//! The crc leaves out the state byte, which changes in place as a record is
//! leased and acked. A header whose `total_len` is zero marks the end of the
//! records; everything past the end is zero.
//!
//! Records are written by one writer and read by any number of readers
//! concurrently. The writer copies a record past the committed end, marks it
//! LIVE and then moves the end over it, so a reader never sees a record that
//! is still being written. Below the end only the state byte ever changes,
//! and only atomically.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

use memmap2::MmapRaw;

use crate::error::Error;
use crate::note::NoteRef;

/// Size of the header in front of every record.
pub const RECORD_HEADER_LEN: usize = 16;
/// The record format version written into every header.
pub const RECORD_VERSION: u8 = 1;

const OFF_STATE: usize = 4;
const OFF_VER: usize = 5;
const OFF_NOTE_LEN: usize = 8;
const OFF_CRC: usize = 12;

/// Where a record is in its life. Stored as one byte in the record header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum State {
    /// Being copied in. Never visible to readers; a crash leaves it behind
    /// and recovery truncates there.
    Writing = 0,
    /// Queued, waiting to be leased.
    Live = 1,
    /// Held by a consumer until it acks, nacks or the lease runs out.
    Leased = 2,
    /// Consumed; the record is garbage.
    Acked = 3,
    /// Replaced or expired before it was consumed; also garbage.
    Dead = 4,
}

impl State {
    fn from_u8(b: u8) -> Option<State> {
        Some(match b {
            0 => State::Writing,
            1 => State::Live,
            2 => State::Leased,
            3 => State::Acked,
            4 => State::Dead,
            _ => return None,
        })
    }
}

/// A record's position: `segment_id << 32 | offset`, so seqs sort in arrival
/// order across segments.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(pub u64);

impl Seq {
    pub fn new(segment: u32, offset: u32) -> Seq {
        Seq((segment as u64) << 32 | offset as u64)
    }

    pub fn segment(self) -> u32 {
        (self.0 >> 32) as u32
    }

    pub fn offset(self) -> u32 {
        self.0 as u32
    }
}

/// Bytes a record of `total_len` takes up in the segment, padding included.
pub(crate) fn stride(total_len: usize) -> usize {
    (total_len + 7) & !7
}

/// One mapped segment file.
pub struct Segment {
    id: u32,
    path: PathBuf,
    file: File,
    map: MmapRaw,
    /// The committed end: every record below it is complete.
    end: AtomicU32,
    /// Committed records not yet consumed: LIVE or LEASED. Once a sealed
    /// segment's count is zero it holds only garbage and can be dropped.
    pending: AtomicU32,
}

impl Segment {
    /// The file name for segment `id`: its base seq in hex.
    pub fn file_name(id: u32) -> String {
        format!("{:016x}.seg", Seq::new(id, 0).0)
    }

    /// Parses a segment id back out of a [`file_name`](Self::file_name).
    pub fn parse_file_name(name: &str) -> Option<u32> {
        let hex = name.strip_suffix(".seg")?;
        if hex.len() != 16 {
            return None;
        }
        let base = u64::from_str_radix(hex, 16).ok()?;
        let seq = Seq(base);
        (seq.offset() == 0).then(|| seq.segment())
    }

    /// Creates an empty segment file of `size` bytes. The file is sparse, so
    /// it takes no disk until records are written into it.
    pub(crate) fn create(dir: &Path, id: u32, size: u32) -> Result<Segment, Error> {
        let path = dir.join(Segment::file_name(id));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        file.set_len(size as u64)?;
        let map = MmapRaw::map_raw(&file)?;
        Ok(Segment {
            id,
            path,
            file,
            map,
            end: AtomicU32::new(0),
            pending: AtomicU32::new(0),
        })
    }

    /// Opens an existing segment and recovers it: the records are checked
    /// from the start, and the first one that is torn, still WRITING, or
    /// fails its crc ends the segment. Everything from there on is zeroed.
    /// Records left LEASED by the last process go back to LIVE.
    ///
    /// A file shorter than a record header (a crash between creating it and
    /// sizing it) is resized to `default_size`.
    pub(crate) fn open(path: &Path, id: u32, default_size: u32) -> Result<Segment, Error> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut size = file.metadata()?.len();
        if size < RECORD_HEADER_LEN as u64 {
            size = default_size as u64;
            file.set_len(size)?;
        }
        if size > u32::MAX as u64 {
            return Err(Error::Corrupt("segment file over 4 GiB"));
        }
        let size = size as u32;

        let map = MmapRaw::map_raw(&file)?;
        let (end, pending) = recover(&map);
        if (end as u64) < size as u64 {
            // Zero the rest. A record past the end can be intact on disk
            // while one before it was torn (pages reach disk in any order),
            // and must not come back once new records are written over the
            // torn one. Truncating and re-extending makes the rest a hole.
            drop(map);
            file.set_len(end as u64)?;
            file.set_len(size as u64)?;
        } else {
            drop(map);
        }
        let map = MmapRaw::map_raw(&file)?;
        Ok(Segment {
            id,
            path: path.to_owned(),
            file,
            map,
            end: AtomicU32::new(end),
            pending: AtomicU32::new(pending),
        })
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The file's size: the most record bytes it can hold.
    pub fn size(&self) -> u32 {
        self.map.len() as u32
    }

    /// The committed end. Records below it are complete and readable.
    pub fn end(&self) -> u32 {
        self.end.load(Ordering::Acquire)
    }

    /// Bytes left after the committed end.
    pub fn remaining(&self) -> u32 {
        self.size() - self.end()
    }

    /// Committed records still LIVE or LEASED.
    pub fn pending(&self) -> u32 {
        self.pending.load(Ordering::Acquire)
    }

    /// Counts one record out of [`pending`](Self::pending), after the
    /// caller moved it from LIVE or LEASED to ACKED or DEAD. Returns whether
    /// that was the last one.
    pub(crate) fn release(&self) -> bool {
        let was = self.pending.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(was > 0, "released more records than were pending");
        was == 1
    }

    /// The record at `off`, if a committed record starts there. `off` must
    /// be the start of a record (0, or a previous record's
    /// [`next`](RecordRef::next)).
    pub fn record(&self, off: u32) -> Option<RecordRef<'_>> {
        if off >= self.end() {
            return None;
        }
        // SAFETY: off is a record boundary below the committed end, so the
        // header and body were written before `end` was released past them
        // and only the state byte changes from here on.
        unsafe {
            let base = self.map.as_ptr().add(off as usize);
            let total_len = read_u32(base) as usize;
            let note_len = read_u32(base.add(OFF_NOTE_LEN)) as usize;
            let body = base.add(RECORD_HEADER_LEN);
            Some(RecordRef {
                seq: Seq::new(self.id, off),
                state: AtomicU8::from_ptr(base.add(OFF_STATE) as *mut u8),
                index: std::slice::from_raw_parts(body, note_len),
                json: std::slice::from_raw_parts(
                    body.add(note_len),
                    total_len - RECORD_HEADER_LEN - note_len,
                ),
                next: off + stride(total_len) as u32,
            })
        }
    }

    /// The committed records from `off` on, in order.
    pub fn records(&self, off: u32) -> Records<'_> {
        Records { seg: self, off }
    }

    /// Appends one record at the committed end, or returns `None` if it
    /// does not fit.
    ///
    /// # Safety
    ///
    /// Only one thread may append to a segment: the log's writer. Two
    /// appends at once would write over each other.
    pub(crate) unsafe fn append(&self, index: &[u8], json: &[u8]) -> Option<Seq> {
        let total_len = RECORD_HEADER_LEN + index.len() + json.len();
        let off = self.end.load(Ordering::Relaxed);
        if stride(total_len) > (self.size() - off) as usize {
            return None;
        }
        let mut header = [0u8; RECORD_HEADER_LEN];
        header[0..4].copy_from_slice(&(total_len as u32).to_le_bytes());
        header[OFF_STATE] = State::Writing as u8;
        header[OFF_VER] = RECORD_VERSION;
        header[OFF_NOTE_LEN..OFF_NOTE_LEN + 4].copy_from_slice(&(index.len() as u32).to_le_bytes());
        let crc = record_crc(&header, index, json);
        header[OFF_CRC..].copy_from_slice(&crc.to_le_bytes());

        // SAFETY: [off, off + stride) is past the committed end, so no
        // reader looks at it, and the caller is the only writer. The pad
        // bytes are already zero: everything past the end is.
        unsafe {
            let base = self.map.as_mut_ptr().add(off as usize);
            std::ptr::copy_nonoverlapping(header.as_ptr(), base, RECORD_HEADER_LEN);
            let body = base.add(RECORD_HEADER_LEN);
            std::ptr::copy_nonoverlapping(index.as_ptr(), body, index.len());
            std::ptr::copy_nonoverlapping(json.as_ptr(), body.add(index.len()), json.len());
            AtomicU8::from_ptr(base.add(OFF_STATE)).store(State::Live as u8, Ordering::Release);
        }
        // Counted before it is visible, so a consumer can't release it first.
        self.pending.fetch_add(1, Ordering::AcqRel);
        self.end
            .store(off + stride(total_len) as u32, Ordering::Release);
        Some(Seq::new(self.id, off))
    }

    /// Writes `[from, to)` back to the file and waits for it.
    pub fn flush_range(&self, from: u32, to: u32) -> io::Result<()> {
        if to > from {
            self.map.flush_range(from as usize, (to - from) as usize)?;
        }
        Ok(())
    }

    /// Writes every committed record back to the file and waits for it.
    pub fn flush(&self) -> io::Result<()> {
        self.flush_range(0, self.end())?;
        self.file.sync_data()
    }
}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Segment")
            .field("id", &self.id)
            .field("end", &self.end())
            .field("size", &self.size())
            .finish()
    }
}

/// A committed record, borrowed from its segment.
#[derive(Clone, Copy)]
pub struct RecordRef<'a> {
    seq: Seq,
    state: &'a AtomicU8,
    index: &'a [u8],
    json: &'a [u8],
    next: u32,
}

impl<'a> RecordRef<'a> {
    pub fn seq(&self) -> Seq {
        self.seq
    }

    /// The offset of the record after this one.
    pub fn next(&self) -> u32 {
        self.next
    }

    pub fn state(&self) -> State {
        State::from_u8(self.state.load(Ordering::Acquire))
            .expect("committed record has a valid state")
    }

    /// Moves the record from `from` to `to` if it is still in `from`.
    /// Returns whether it moved; exactly one of any racing callers wins.
    ///
    /// This does not keep the segment's pending count: moves to ACKED or
    /// DEAD go through [`Log::retire`](crate::Log::retire).
    pub fn cas_state(&self, from: State, to: State) -> bool {
        self.state
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// The event's json, exactly as it was pushed.
    pub fn json(&self) -> &'a [u8] {
        self.json
    }

    /// The note's span index bytes.
    pub fn index(&self) -> &'a [u8] {
        self.index
    }

    pub fn note(&self) -> NoteRef<'a> {
        NoteRef::new(self.index, self.json).expect("recovered records have a valid index")
    }
}

impl std::fmt::Debug for RecordRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordRef")
            .field("seq", &self.seq)
            .field("state", &self.state())
            .field("json_len", &self.json.len())
            .finish()
    }
}

/// The committed records of one segment, from some offset on.
pub struct Records<'a> {
    seg: &'a Segment,
    off: u32,
}

impl<'a> Iterator for Records<'a> {
    type Item = RecordRef<'a>;

    fn next(&mut self) -> Option<RecordRef<'a>> {
        let r = self.seg.record(self.off)?;
        self.off = r.next;
        Some(r)
    }
}

fn record_crc(header: &[u8; RECORD_HEADER_LEN], index: &[u8], json: &[u8]) -> u32 {
    let crc = crc32c::crc32c(&header[0..OFF_STATE]);
    let crc = crc32c::crc32c_append(crc, &header[OFF_STATE + 1..OFF_CRC]);
    let crc = crc32c::crc32c_append(crc, index);
    crc32c::crc32c_append(crc, json)
}

/// Walks the records from the start and returns where the good ones end,
/// and how many of them are still LIVE. Runs before anyone else can see the
/// segment.
fn recover(map: &MmapRaw) -> (u32, u32) {
    // SAFETY: the segment is not shared yet, so nothing else reads or
    // writes the map while we hold this slice.
    let buf = unsafe { std::slice::from_raw_parts_mut(map.as_mut_ptr(), map.len()) };
    let mut off = 0usize;
    let mut pending = 0;
    while off + RECORD_HEADER_LEN <= buf.len() {
        let rec = &buf[off..];
        let total_len = read_le_u32(rec) as usize;
        if total_len == 0 || !record_ok(rec, total_len) {
            break;
        }
        if rec[OFF_STATE] == State::Leased as u8 {
            buf[off + OFF_STATE] = State::Live as u8;
        }
        if buf[off + OFF_STATE] == State::Live as u8 {
            pending += 1;
        }
        off += stride(total_len);
    }
    (off.min(buf.len()) as u32, pending)
}

/// Whether the record at the start of `rec` is whole and intact.
fn record_ok(rec: &[u8], total_len: usize) -> bool {
    if total_len < RECORD_HEADER_LEN || total_len > rec.len() {
        return false;
    }
    match State::from_u8(rec[OFF_STATE]) {
        None | Some(State::Writing) => return false,
        Some(_) => {}
    }
    let note_len = read_le_u32(&rec[OFF_NOTE_LEN..]) as usize;
    if rec[OFF_VER] != RECORD_VERSION || note_len > total_len - RECORD_HEADER_LEN {
        return false;
    }
    let header: &[u8; RECORD_HEADER_LEN] = rec[..RECORD_HEADER_LEN].try_into().unwrap();
    let index = &rec[RECORD_HEADER_LEN..RECORD_HEADER_LEN + note_len];
    let json = &rec[RECORD_HEADER_LEN + note_len..total_len];
    record_crc(header, index, json) == read_le_u32(&rec[OFF_CRC..])
        && NoteRef::new(index, json).is_ok()
}

fn read_le_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

/// # Safety
///
/// `p` must point at four readable bytes.
unsafe fn read_u32(p: *const u8) -> u32 {
    u32::from_le_bytes(unsafe { std::ptr::read_unaligned(p as *const [u8; 4]) })
}
