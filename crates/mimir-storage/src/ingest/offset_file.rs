//! Atomic file-based Kafka offset persistence.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/offset_file.go

use std::fs;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error, info};

use crate::shipper::atomic_write_file;

pub const OFFSET_FILE_VERSION: i32 = 1;

#[derive(Debug, Error)]
pub enum OffsetFileError {
    #[error("failed to create directory {path}: {source}")]
    CreateDir {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to serialize offset file data: {0}")]
    Json(#[from] serde_json::Error),
    #[error("failed to write offset file: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OffsetFileData {
    version: i32,
    partition_id: i32,
    offset: i64,
}

/// Thread-safe reader/writer for a partition's last committed Kafka offset file.
pub struct OffsetFile {
    file_path: PathBuf,
    partition_id: i32,
    mu: Mutex<()>,
}

impl OffsetFile {
    pub fn new(file_path: impl AsRef<Path>, partition_id: i32) -> Self {
        Self {
            file_path: file_path.as_ref().to_path_buf(),
            partition_id,
            mu: Mutex::new(()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.file_path
    }

    pub fn partition_id(&self) -> i32 {
        self.partition_id
    }

    /// Reads the last committed offset from the file.
    /// Returns `Some(offset)` if the file exists, parses cleanly, version == 1, and partition_id matches.
    /// Returns `None` otherwise (matching `offsetFile.Read()` in Go).
    pub fn read(&self) -> Option<i64> {
        let _guard = self.mu.lock();

        let data = match fs::read(&self.file_path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => {
                error!(file = %self.file_path.display(), err = %e, "failed to read offset file");
                return None;
            }
        };

        let parsed: OffsetFileData = match serde_json::from_slice(&data) {
            Ok(p) => p,
            Err(e) => {
                error!(file = %self.file_path.display(), err = %e, "failed to parse offset file");
                return None;
            }
        };

        if parsed.version != OFFSET_FILE_VERSION {
            error!(
                file = %self.file_path.display(),
                version = parsed.version,
                "offset file has unknown version"
            );
            return None;
        }

        if parsed.partition_id != self.partition_id {
            error!(
                file = %self.file_path.display(),
                expected_partition = self.partition_id,
                file_partition = parsed.partition_id,
                "offset file partition ID does not match expected partition, wrong volume may be mounted"
            );
            return None;
        }

        info!(file = %self.file_path.display(), offset = parsed.offset, "read last committed offset from file");
        Some(parsed.offset)
    }

    /// Writes `offset` atomically to disk.
    pub fn write(&self, offset: i64) -> Result<(), OffsetFileError> {
        let _guard = self.mu.lock();

        if let Some(dir) = self.file_path.parent() {
            if !dir.as_os_str().is_empty() {
                fs::create_dir_all(dir).map_err(|source| OffsetFileError::CreateDir {
                    path: dir.display().to_string(),
                    source,
                })?;
            }
        }

        let data = OffsetFileData {
            version: OFFSET_FILE_VERSION,
            partition_id: self.partition_id,
            offset,
        };
        let json_bytes = serde_json::to_vec(&data)?;
        atomic_write_file(&self.file_path, &json_bytes)?;

        debug!(file = %self.file_path.display(), offset, "wrote offset to file");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const TEST_PARTITION_ID: i32 = 1;

    #[test]
    fn test_offset_file_read_write() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("partition-1.offset");
        let f = OffsetFile::new(&file_path, TEST_PARTITION_ID);

        assert_eq!(f.read(), None);

        f.write(12345).unwrap();
        assert!(file_path.exists());
        assert_eq!(f.read(), Some(12345));

        f.write(67890).unwrap();
        assert_eq!(f.read(), Some(67890));
    }

    #[test]
    fn test_offset_file_creates_directory() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("nested").join("path").join("partition-1.offset");
        let f = OffsetFile::new(&file_path, TEST_PARTITION_ID);

        f.write(1).unwrap();
        assert_eq!(f.read(), Some(1));
    }

    #[test]
    fn test_offset_file_invalid_and_empty() {
        let dir = tempfile::tempdir().unwrap();
        let invalid = dir.path().join("invalid.offset");
        fs::write(&invalid, b"not-valid-json").unwrap();
        assert_eq!(OffsetFile::new(&invalid, TEST_PARTITION_ID).read(), None);

        let empty = dir.path().join("empty.offset");
        fs::write(&empty, b"").unwrap();
        assert_eq!(OffsetFile::new(&empty, TEST_PARTITION_ID).read(), None);
    }

    #[test]
    fn test_offset_file_wrong_partition_and_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partition.offset");
        OffsetFile::new(&path, 1).write(42).unwrap();
        assert_eq!(OffsetFile::new(&path, 2).read(), None);

        let ver_path = dir.path().join("version.offset");
        let bad = OffsetFileData {
            version: 2,
            partition_id: TEST_PARTITION_ID,
            offset: 0,
        };
        fs::write(&ver_path, serde_json::to_vec(&bad).unwrap()).unwrap();
        assert_eq!(OffsetFile::new(&ver_path, TEST_PARTITION_ID).read(), None);
    }

    #[test]
    fn test_offset_file_atomic_and_concurrent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concurrent.offset");
        let f = Arc::new(OffsetFile::new(&path, TEST_PARTITION_ID));

        let f1 = Arc::clone(&f);
        let h1 = std::thread::spawn(move || {
            for i in 0..100 {
                f1.write(i).unwrap();
            }
        });
        let f2 = Arc::clone(&f);
        let h2 = std::thread::spawn(move || {
            for _ in 0..100 {
                let _ = f2.read();
            }
        });

        h1.join().unwrap();
        h2.join().unwrap();

        let off = f.read().unwrap();
        assert!((0..100).contains(&off));
        assert!(!path.with_extension("offset.tmp").exists());
    }
}
