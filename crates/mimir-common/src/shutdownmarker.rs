//! Shutdown marker file persistence matching Go `pkg/util/shutdownmarker` & `pkg/util/atomicfs`.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/util/shutdownmarker/shutdown_marker.go
//! - github.com/grafana/mimir/pkg/util/atomicfs/fsync.go

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::logging::format_rfc3339_utc;

/// Shutdown marker file name (`shutdown-requested.txt`).
pub const SHUTDOWN_MARKER_FILENAME: &str = "shutdown-requested.txt";

/// Returns the path of the shutdown marker file inside `dir_path`.
pub fn get_path(dir_path: impl AsRef<Path>) -> PathBuf {
    dir_path.as_ref().join(SHUTDOWN_MARKER_FILENAME)
}

/// Atomically writes the shutdown marker file containing the current UTC RFC3339 timestamp,
/// fsyncing both the temporary file and the containing directory (`atomicfs.CreateFile`).
pub fn create(path: impl AsRef<Path>) -> io::Result<()> {
    let final_path = path.as_ref();
    let mut tmp_os = final_path.as_os_str().to_os_string();
    tmp_os.push(".tmp");
    let tmp_path = PathBuf::from(tmp_os);

    let timestamp = format_rfc3339_utc(SystemTime::now());
    let write_res = (|| -> io::Result<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        f.write_all(timestamp.as_bytes())?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp_path, final_path)?;
        if let Some(parent) = final_path.parent() {
            if !parent.as_os_str().is_empty() {
                let dir = File::open(parent)?;
                dir.sync_all()?;
            }
        }
        Ok(())
    })();

    if write_res.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_res
}

/// Removes the shutdown marker file if it exists and fsyncs the containing directory.
pub fn remove(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
    }
    Ok(())
}

/// Returns `true` if the shutdown marker file exists and is a regular file.
pub fn exists(path: impl AsRef<Path>) -> io::Result<bool> {
    match fs::metadata(path.as_ref()) {
        Ok(meta) => Ok(meta.is_file()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_marker_lifecycle() {
        let dir = tempfile_dir();
        let path = get_path(&dir);
        assert_eq!(path, dir.join("shutdown-requested.txt"));
        assert!(!exists(&path).unwrap());

        create(&path).unwrap();
        assert!(exists(&path).unwrap());
        let contents = fs::read_to_string(&path).unwrap();
        assert!(contents.ends_with('Z'));
        assert_eq!(contents.len(), 20); // YYYY-MM-DDTHH:MM:SSZ

        remove(&path).unwrap();
        assert!(!exists(&path).unwrap());

        let _ = fs::remove_dir_all(&dir);
    }

    fn tempfile_dir() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "mimir-shutdownmarker-test-{}",
            std::process::id()
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }
}
