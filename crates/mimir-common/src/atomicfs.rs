//! Atomic filesystem writes with file and parent-directory fsync.
//!
//! Ported from github.com/grafana/mimir/pkg/util/atomicfs/fsync.go

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};

/// Error marker equivalent to Go `os.ErrClosed` when calling `close` / `write` on an
/// already-closed `AtomicFile`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedError;

impl fmt::Display for ClosedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("file already closed")
    }
}

impl std::error::Error for ClosedError {}

pub fn is_closed_err(err: &io::Error) -> bool {
    err.get_ref()
        .and_then(|e| e.downcast_ref::<ClosedError>())
        .is_some()
}

fn err_closed() -> io::Error {
    io::Error::other(ClosedError)
}

/// Cleans a path lexically (similar to Go `filepath.Clean`) and returns the temporary
/// staging path `<clean>.tmp`.
pub fn temp_path(final_path: impl AsRef<Path>) -> PathBuf {
    let cleaned = clean_path(final_path.as_ref());
    let mut tmp = cleaned.into_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

fn clean_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push(Component::ParentDir);
                }
            }
            c => out.push(c),
        }
    }
    if out.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        out
    }
}

/// Wrapper around `std::fs::File` that writes to a temporary `.tmp` file in the same
/// directory and atomically renames to `final_path` on `close()`, fsyncing both the
/// file and its containing directory.
pub struct AtomicFile {
    file: Option<File>,
    tmp_path: PathBuf,
    final_path: PathBuf,
}

impl AtomicFile {
    /// Writes a string slice into the temporary file (Go `File.WriteString`).
    pub fn write_str(&mut self, s: &str) -> io::Result<usize> {
        self.write(s.as_bytes())
    }

    /// Syncs the temporary file, closes it, renames it to the final path, and fsyncs
    /// the containing directory. Calling `close` more than once returns an error
    /// matching `os.ErrClosed` without modifying the original file.
    pub fn close(&mut self) -> io::Result<()> {
        let Some(file) = self.file.take() else {
            return Err(err_closed());
        };

        let mut cleanup = true;
        let res = (|| -> io::Result<()> {
            file.sync_all()?;
            drop(file);
            fs::rename(&self.tmp_path, &self.final_path)?;
            cleanup = false;

            let parent = self
                .final_path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let dir = File::open(parent)?;
            dir.sync_all()?;
            Ok(())
        })();

        if cleanup {
            let _ = fs::remove_file(&self.tmp_path);
        }
        res
    }
}

impl Write for AtomicFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let Some(ref mut f) = self.file else {
            return Err(err_closed());
        };
        f.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let Some(ref mut f) = self.file else {
            return Err(err_closed());
        };
        f.flush()
    }
}

impl Drop for AtomicFile {
    fn drop(&mut self) {
        if self.file.take().is_some() {
            let _ = fs::remove_file(&self.tmp_path);
        }
    }
}

/// Creates a new temporary file next to `path` that will be atomically renamed to `path`
/// when `close()` is called.
pub fn create(path: impl AsRef<Path>) -> io::Result<AtomicFile> {
    let final_path = clean_path(path.as_ref());
    let tmp_path = temp_path(&final_path);
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;
    Ok(AtomicFile {
        file: Some(file),
        tmp_path,
        final_path,
    })
}

/// Safely writes the contents of `data` to `file_path`, ensuring that all data and
/// the containing directory are fsynced.
pub fn create_file(file_path: impl AsRef<Path>, mut data: impl Read) -> io::Result<()> {
    let mut f = create(file_path)?;
    let copy_res = io::copy(&mut data, &mut f);
    let close_res = f.close();
    copy_res?;
    close_res
}
