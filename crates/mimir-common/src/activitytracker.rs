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

/// Mmap-backed tracker remembering active tasks in fixed 1024-byte slots.
pub struct ActivityTracker {
    _file: File,
    mmap: Mutex<MmapMut>,
    free_index_queue: Mutex<VecDeque<usize>>,
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

        // Load unfinished entries from any previous run before truncating.
        let prev_entries = load_unfinished_entries(&cfg.filepath).unwrap_or_default();

        let filesize = cfg.max_entries * ENTRY_SIZE;
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
            free_index_queue: Mutex::new(free_index_queue),
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
            match q.pop_front() {
                Some(i) => {
                    self.free_slots.set(q.len() as f64);
                    i
                }
                None => {
                    self.failed_full.inc();
                    return -1;
                }
            }
        };

        let activity = activity_generator();
        if activity.is_empty() {
            let mut q = self.free_index_queue.lock();
            q.push_back(idx);
            self.free_slots.set(q.len() as f64);
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
        let mut mmap = self.mmap.lock();
        mmap[offset..offset + TIMESTAMP_LENGTH].copy_from_slice(&now_nanos.to_be_bytes());
        mmap[offset + TIMESTAMP_LENGTH..offset + TIMESTAMP_LENGTH + trimmed.len()]
            .copy_from_slice(trimmed.as_bytes());
        // Zero any remaining tail bytes in case a previous write hadn't cleared them
        if TIMESTAMP_LENGTH + trimmed.len() < ENTRY_SIZE {
            mmap[offset + TIMESTAMP_LENGTH + trimmed.len()..offset + ENTRY_SIZE].fill(0);
        }

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
        let offset = idx * ENTRY_SIZE;
        {
            let mut mmap = self.mmap.lock();
            mmap[offset..offset + ENTRY_SIZE].copy_from_slice(&EMPTY_ENTRY);
        }
        let mut q = self.free_index_queue.lock();
        q.push_back(idx);
        self.free_slots.set(q.len() as f64);
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

/// Loads unfinished activity entries from an existing activity tracker file.
pub fn load_unfinished_entries(path: impl AsRef<Path>) -> io::Result<Vec<ActivityEntry>> {
    let mut file = match File::open(path.as_ref()) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut results = Vec::new();
    let mut buf = [0u8; ENTRY_SIZE];
    loop {
        let mut read_total = 0;
        while read_total < ENTRY_SIZE {
            let n = file.read(&mut buf[read_total..])?;
            if n == 0 {
                break;
            }
            read_total += n;
        }
        if read_total == 0 {
            break;
        }
        if read_total < ENTRY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated activity tracker entry",
            ));
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

    Ok(results)
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
}
