//! Mmap-backed activity tracker matching Go `pkg/util/activitytracker` 1:1 on disk.
//!
//! Ported from github.com/grafana/mimir/pkg/util/activitytracker/activity_tracker.go.
//!
//! Disk format:
//! - Fixed-size file of `max_entries * 1024` bytes.
//! - Each 1024-byte slot contains:
//!   - `[0..8]`: `uint64` big-endian Unix nanoseconds timestamp
//!   - `[8..1024]`: UTF-8 activity description zero-padded with `0x00`
//! - Empty slot = 1024 zero bytes (`0x00`).

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use memmap2::MmapMut;
use parking_lot::Mutex;
use prometheus::{Counter, CounterVec, Gauge, Opts, Registry};

/// Fixed byte size of each activity entry on disk.
pub const ENTRY_SIZE: usize = 1024;

/// Timestamp prefix length in bytes (`uint64` big-endian Unix nanoseconds).
pub const TIMESTAMP_LENGTH: usize = 8;

/// Maximum UTF-8 byte length of the activity text inside an entry.
pub const MAX_ACTIVITY_LEN: usize = ENTRY_SIZE - TIMESTAMP_LENGTH;

pub const REASON_FULL: &str = "tracker_full";
pub const REASON_EMPTY_ACTIVITY: &str = "empty_activity";

const EMPTY_ENTRY: [u8; ENTRY_SIZE] = [0u8; ENTRY_SIZE];

/// Configuration for [`ActivityTracker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityTrackerConfig {
    pub filepath: PathBuf,
    pub max_entries: usize,
}

impl Default for ActivityTrackerConfig {
    fn default() -> Self {
        Self {
            filepath: PathBuf::from("./metrics-activity.log"),
            max_entries: 1024,
        }
    }
}

/// Activity entry loaded from an existing activity tracker file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityEntry {
    pub timestamp: SystemTime,
    pub activity: String,
}

/// Validates raw signed `-activity-tracker.max-entries` matching Go's `getMappedFile`
/// (`MaxEntries < 0` -> `failed to truncate activity file: invalid argument`;
/// `MaxEntries == 0` -> `failed to mmap activity file: invalid argument`).
pub fn validate_raw_max_entries(raw: i64) -> io::Result<usize> {
    if raw < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "failed to truncate activity file: invalid argument",
        ));
    }
    if raw == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "failed to mmap activity file: invalid argument",
        ));
    }
    let entries = raw as usize;
    if entries
        .checked_mul(ENTRY_SIZE)
        .and_then(|bytes| i64::try_from(bytes).ok())
        .is_none()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "failed to truncate activity file: invalid argument",
        ));
    }
    Ok(entries)
}

struct FreeQueueState {
    queue: VecDeque<usize>,
    in_use: Vec<bool>,
}

/// Mmap-backed tracker remembering active tasks in fixed 1024-byte slots.
pub struct ActivityTracker {
    _file: File,
    mmap: Mutex<MmapMut>,
    free_index_queue: Mutex<FreeQueueState>,
    max_entries: usize,
    failed_full: Counter,
    failed_empty: Counter,
    free_slots: Gauge,
    loaded_activities_on_startup: Gauge,
}

impl ActivityTracker {
    /// Creates and maps a new [`ActivityTracker`]. Returns `Ok(None)` if `cfg.filepath` is empty.
    pub fn new(
        cfg: ActivityTrackerConfig,
        registry: Option<&Registry>,
    ) -> io::Result<Option<Self>> {
        if cfg.filepath.as_os_str().is_empty() {
            return Ok(None);
        }
        if cfg.max_entries == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "failed to mmap activity file: invalid argument",
            ));
        }
        let filesize = cfg
            .max_entries
            .checked_mul(ENTRY_SIZE)
            .and_then(|bytes| i64::try_from(bytes).ok().map(|_| bytes))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "failed to truncate activity file: invalid argument",
                )
            })?;

        // Load unfinished entries from any previous run before truncating.
        let (prev_entries, _) = load_unfinished_entries_partial(&cfg.filepath);

        let file = open_and_truncate(&cfg.filepath, filesize)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let failed_inserts = CounterVec::new(
            Opts::new(
                "activity_tracker_failed_total",
                "How many times has activity tracker failed to insert new activity.",
            ),
            &["reason"],
        )
        .map_err(io::Error::other)?;
        let failed_full = failed_inserts.with_label_values(&[REASON_FULL]);
        let failed_empty = failed_inserts.with_label_values(&[REASON_EMPTY_ACTIVITY]);

        let free_slots = Gauge::with_opts(Opts::new(
            "activity_tracker_free_slots",
            "Number of free slots in activity file.",
        ))
        .map_err(io::Error::other)?;
        free_slots.set(cfg.max_entries as f64);

        let loaded_activities_on_startup = Gauge::with_opts(Opts::new(
            "activity_tracker_unfinished_activities_loaded",
            "Number of unfinished activities loaded from the activity tracker file on startup.",
        ))
        .map_err(io::Error::other)?;
        loaded_activities_on_startup.set(prev_entries.len() as f64);

        if let Some(reg) = registry {
            reg.register(Box::new(failed_inserts))
                .map_err(io::Error::other)?;
            reg.register(Box::new(free_slots.clone()))
                .map_err(io::Error::other)?;
            reg.register(Box::new(loaded_activities_on_startup.clone()))
                .map_err(io::Error::other)?;
        }

        let mut free_index_queue = VecDeque::with_capacity(cfg.max_entries);
        for i in 0..cfg.max_entries {
            free_index_queue.push_back(i);
        }

        Ok(Some(Self {
            _file: file,
            mmap: Mutex::new(mmap),
            free_index_queue: Mutex::new(FreeQueueState {
                queue: free_index_queue,
                in_use: vec![false; cfg.max_entries],
            }),
            max_entries: cfg.max_entries,
            failed_full,
            failed_empty,
            free_slots,
            loaded_activities_on_startup,
        }))
    }

    /// Inserts an activity produced by `activity_generator` into a free slot,
    /// returning the slot index or `-1` if the tracker is full or the activity is empty.
    pub fn insert<F>(&self, activity_generator: F) -> isize
    where
        F: FnOnce() -> String,
    {
        let idx = {
            let mut q = self.free_index_queue.lock();
            match q.queue.pop_front() {
                Some(i) => {
                    q.in_use[i] = true;
                    self.free_slots.set(q.queue.len() as f64);
                    i
                }
                None => {
                    self.failed_full.inc();
                    return -1;
                }
            }
        };

        struct SlotGuard<'a> {
            tracker: &'a ActivityTracker,
            idx: usize,
            armed: bool,
        }
        impl Drop for SlotGuard<'_> {
            fn drop(&mut self) {
                if self.armed {
                    let mut q = self.tracker.free_index_queue.lock();
                    if q.in_use[self.idx] {
                        q.in_use[self.idx] = false;
                        q.queue.push_back(self.idx);
                        self.tracker.free_slots.set(q.queue.len() as f64);
                    }
                }
            }
        }
        let mut slot_guard = SlotGuard {
            tracker: self,
            idx,
            armed: true,
        };

        let activity = activity_generator();
        if activity.is_empty() {
            drop(slot_guard);
            self.failed_empty.inc();
            return -1;
        }

        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos()
            .min(u64::MAX as u128) as u64;

        let trimmed = trim_entry_to_size(&activity, MAX_ACTIVITY_LEN);
        let offset = idx * ENTRY_SIZE;
        {
            let mut mmap = self.mmap.lock();
            mmap[offset..offset + TIMESTAMP_LENGTH].copy_from_slice(&now_nanos.to_be_bytes());
            mmap[offset + TIMESTAMP_LENGTH..offset + TIMESTAMP_LENGTH + trimmed.len()]
                .copy_from_slice(trimmed.as_bytes());
            // Zero any remaining tail bytes in case a previous write hadn't cleared them
            if TIMESTAMP_LENGTH + trimmed.len() < ENTRY_SIZE {
                mmap[offset + TIMESTAMP_LENGTH + trimmed.len()..offset + ENTRY_SIZE].fill(0);
            }
        }
        slot_guard.armed = false;

        idx as isize
    }

    /// Convenience helper inserting a static string.
    pub fn insert_static(&self, activity: &str) -> isize {
        self.insert(|| activity.to_string())
    }

    /// Clears the slot at `activity_index` and returns it to the free queue.
    pub fn delete(&self, activity_index: isize) {
        if activity_index < 0 || (activity_index as usize) >= self.max_entries {
            return;
        }
        let idx = activity_index as usize;
        {
            let mut q = self.free_index_queue.lock();
            if !q.in_use[idx] {
                return;
            }
            q.in_use[idx] = false;
        }
        let offset = idx * ENTRY_SIZE;
        {
            let mut mmap = self.mmap.lock();
            mmap[offset..offset + ENTRY_SIZE].copy_from_slice(&EMPTY_ENTRY);
        }
        let mut q = self.free_index_queue.lock();
        q.queue.push_back(idx);
        self.free_slots.set(q.queue.len() as f64);
    }

    /// Flushes mmap changes to disk.
    pub fn flush(&self) -> io::Result<()> {
        self.mmap.lock().flush()
    }

    /// Closes the tracker, flushing the mmap before dropping.
    pub fn close(self) -> io::Result<()> {
        self.mmap.lock().flush()
    }

    pub fn free_slots_count(&self) -> f64 {
        self.free_slots.get()
    }

    pub fn failed_full_count(&self) -> f64 {
        self.failed_full.get()
    }

    pub fn failed_empty_count(&self) -> f64 {
        self.failed_empty.get()
    }

    pub fn loaded_activities_on_startup_count(&self) -> f64 {
        self.loaded_activities_on_startup.get()
    }
}

/// Nil-safe extension trait matching Go's nil `*ActivityTracker` receiver semantics.
pub trait ActivityTrackerExt {
    fn insert_static_or_noop(&self, activity: &str) -> isize;
    fn delete_or_noop(&self, activity_index: isize);
    fn close_or_noop(self) -> io::Result<()>;
}

impl ActivityTrackerExt for Option<ActivityTracker> {
    fn insert_static_or_noop(&self, activity: &str) -> isize {
        match self {
            Some(t) => t.insert_static(activity),
            None => -1,
        }
    }

    fn delete_or_noop(&self, activity_index: isize) {
        if let Some(t) = self {
            t.delete(activity_index);
        }
    }

    fn close_or_noop(self) -> io::Result<()> {
        match self {
            Some(t) => t.close(),
            None => Ok(()),
        }
    }
}

impl ActivityTrackerExt for Option<Arc<ActivityTracker>> {
    fn insert_static_or_noop(&self, activity: &str) -> isize {
        match self {
            Some(t) => t.insert_static(activity),
            None => -1,
        }
    }

    fn delete_or_noop(&self, activity_index: isize) {
        if let Some(t) = self {
            t.delete(activity_index);
        }
    }

    fn close_or_noop(self) -> io::Result<()> {
        match self {
            Some(t) => t.flush(),
            None => Ok(()),
        }
    }
}

/// Trims `entry` to at most `max_bytes` bytes without splitting a UTF-8 codepoint.
pub fn trim_entry_to_size(entry: &str, max_bytes: usize) -> &str {
    if entry.len() <= max_bytes {
        return entry;
    }
    let mut len = max_bytes;
    while len > 0 && !entry.is_char_boundary(len) {
        len -= 1;
    }
    &entry[..len]
}

fn open_and_truncate(path: &Path, filesize: usize) -> io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.create(true).read(true).write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let file = opts.open(path)?;
    file.set_len(filesize as u64)?;
    Ok(file)
}

/// Loads unfinished activity entries from an existing activity tracker file, returning both
/// any complete entries read so far and any trailing read/truncation error (matching Go
/// `LoadUnfinishedEntries` returning `(results, err)`).
pub fn load_unfinished_entries_partial(
    path: impl AsRef<Path>,
) -> (Vec<ActivityEntry>, Option<io::Error>) {
    let mut file = match File::open(path.as_ref()) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return (Vec::new(), None),
        Err(e) => return (Vec::new(), Some(e)),
    };

    let mut results = Vec::new();
    let mut buf = [0u8; ENTRY_SIZE];
    loop {
        let mut read_total = 0;
        while read_total < ENTRY_SIZE {
            match file.read(&mut buf[read_total..]) {
                Ok(0) => break,
                Ok(n) => read_total += n,
                Err(e) => return (results, Some(e)),
            }
        }
        if read_total == 0 {
            break;
        }
        if read_total < ENTRY_SIZE {
            return (
                results,
                Some(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated activity tracker entry",
                )),
            );
        }
        if buf == EMPTY_ENTRY {
            continue;
        }

        let ts_nanos = u64::from_be_bytes(buf[..TIMESTAMP_LENGTH].try_into().unwrap());
        let ts = UNIX_EPOCH + Duration::from_nanos(ts_nanos);

        let raw_activity: Vec<u8> = buf[TIMESTAMP_LENGTH..]
            .iter()
            .copied()
            .filter(|&b| b != 0)
            .collect();
        let activity = String::from_utf8_lossy(&raw_activity).into_owned();
        results.push(ActivityEntry {
            timestamp: ts,
            activity,
        });
    }

    (results, None)
}

/// Loads unfinished activity entries from an existing activity tracker file.
pub fn load_unfinished_entries(path: impl AsRef<Path>) -> io::Result<Vec<ActivityEntry>> {
    let (entries, err) = load_unfinished_entries_partial(path);
    match err {
        Some(e) => Err(e),
        None => Ok(entries),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trim_entry_to_size() {
        assert_eq!(trim_entry_to_size("hello world", 100), "hello world");
        assert_eq!(trim_entry_to_size("hello world", 10), "hello worl");
        assert_eq!(trim_entry_to_size("hello world", 5), "hello");
        assert_eq!(trim_entry_to_size("🎉🎄🎅", 5), "🎉");
        assert_eq!(trim_entry_to_size("🎉🎄🎅", 8), "🎉🎄");
        assert_eq!(trim_entry_to_size("🎉🎄🎅", 9), "🎉🎄");
        assert_eq!(trim_entry_to_size("🎉🎄🎅", 10), "🎉🎄");
        assert_eq!(trim_entry_to_size("🎉🎄🎅", 11), "🎉🎄");
        assert_eq!(trim_entry_to_size("🎉🎄🎅", 12), "🎉🎄🎅");
    }

    #[test]
    fn test_activity_tracker_insert_delete_and_reload() {
        let dir = std::env::temp_dir().join(format!(
            "mimir-activitytracker-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("activity");

        let reg = Registry::new();
        let tr = ActivityTracker::new(
            ActivityTrackerConfig {
                filepath: file.clone(),
                max_entries: 3,
            },
            Some(&reg),
        )
        .unwrap()
        .unwrap();

        assert_eq!(tr.loaded_activities_on_startup_count(), 0.0);
        let i0 = tr.insert_static("act-0");
        let i1 = tr.insert_static("act-1");
        let _i2 = tr.insert_static("act-2");
        assert!(i0 >= 0 && i1 >= 0);
        assert_eq!(tr.free_slots_count(), 0.0);

        // Full
        assert_eq!(tr.insert_static("extra"), -1);
        assert_eq!(tr.failed_full_count(), 1.0);

        tr.delete(i0);
        assert_eq!(tr.free_slots_count(), 1.0);
        assert_eq!(tr.insert_static(""), -1);
        assert_eq!(tr.failed_empty_count(), 1.0);

        tr.close().unwrap();

        // Reload detects 2 unfinished entries
        let reg2 = Registry::new();
        let tr2 = ActivityTracker::new(
            ActivityTrackerConfig {
                filepath: file.clone(),
                max_entries: 3,
            },
            Some(&reg2),
        )
        .unwrap()
        .unwrap();
        assert_eq!(tr2.loaded_activities_on_startup_count(), 2.0);
        tr2.close().unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_validate_raw_max_entries_and_zero_or_overflow() {
        let err_neg = validate_raw_max_entries(-1).unwrap_err();
        assert!(err_neg.to_string().contains("failed to truncate activity file"));
        let err_zero = validate_raw_max_entries(0).unwrap_err();
        assert!(err_zero.to_string().contains("failed to mmap activity file"));
        let err_ovf = validate_raw_max_entries((i64::MAX / ENTRY_SIZE as i64) + 1).unwrap_err();
        assert!(err_ovf.to_string().contains("failed to truncate activity file"));
        assert_eq!(validate_raw_max_entries(16).unwrap(), 16);

        let dir = std::env::temp_dir().join(format!(
            "mimir-activitytracker-w19-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("activity");

        let zero_res = ActivityTracker::new(
            ActivityTrackerConfig {
                filepath: file.clone(),
                max_entries: 0,
            },
            None,
        );
        assert!(zero_res
            .err()
            .unwrap()
            .to_string()
            .contains("failed to mmap activity file"));

        let ovf_res = ActivityTracker::new(
            ActivityTrackerConfig {
                filepath: file,
                max_entries: usize::MAX,
            },
            None,
        );
        assert!(ovf_res
            .err()
            .unwrap()
            .to_string()
            .contains("failed to truncate activity file"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
