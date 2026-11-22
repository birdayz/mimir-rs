//! Big-endian fixed integer and varint encoding/decoding buffers for TSDB WAL and index files.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/encoding/encoding.go.

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EncodingError {
    #[error("invalid size / unexpected end of buffer")]
    InvalidSize,
    #[error("invalid checksum (expected {expected:#x}, actual {actual:#x})")]
    InvalidChecksum { expected: u32, actual: u32 },
    #[error("invalid varint encoding")]
    InvalidVarint,
    #[error("invalid UTF-8 string in buffer")]
    InvalidUtf8,
}

/// Reusable encoding buffer matching Prometheus `encoding.Encbuf`.
#[derive(Clone, Debug, Default)]
pub struct Encbuf {
    pub buf: Vec<u8>,
}

impl Encbuf {
    #[inline]
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.buf.clear();
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    #[inline]
    pub fn put_byte(&mut self, b: u8) {
        self.buf.push(b);
    }

    #[inline]
    pub fn put_bytes(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }

    #[inline]
    pub fn put_str(&mut self, s: &str) {
        self.buf.extend_from_slice(s.as_bytes());
    }

    #[inline]
    pub fn put_be32(&mut self, x: u32) {
        self.buf.extend_from_slice(&x.to_be_bytes());
    }

    #[inline]
    pub fn put_be64(&mut self, x: u64) {
        self.buf.extend_from_slice(&x.to_be_bytes());
    }

    #[inline]
    pub fn put_be64_i64(&mut self, x: i64) {
        self.buf.extend_from_slice(&x.to_be_bytes());
    }

    #[inline]
    pub fn put_be_f64(&mut self, x: f64) {
        self.put_be64(x.to_bits());
    }

    #[inline]
    pub fn put_uvarint64(&mut self, mut x: u64) {
        while x >= 0x80 {
            self.buf.push((x as u8) | 0x80);
            x >>= 7;
        }
        self.buf.push(x as u8);
    }

    #[inline]
    pub fn put_uvarint32(&mut self, x: u32) {
        self.put_uvarint64(u64::from(x));
    }

    #[inline]
    pub fn put_uvarint(&mut self, x: usize) {
        self.put_uvarint64(x as u64);
    }

    #[inline]
    pub fn put_varint64(&mut self, x: i64) {
        let mut ux = (x as u64) << 1;
        if x < 0 {
            ux = !ux;
        }
        self.put_uvarint64(ux);
    }

    #[inline]
    pub fn put_uvarint_str(&mut self, s: &str) {
        self.put_uvarint(s.len());
        self.put_str(s);
    }

    #[inline]
    pub fn put_uvarint_bytes(&mut self, b: &[u8]) {
        self.put_uvarint(b.len());
        self.put_bytes(b);
    }

    /// Appends CRC32C (Castagnoli) over the current buffer contents.
    #[inline]
    pub fn put_crc32c(&mut self) {
        let crc = crc32c::crc32c(&self.buf);
        self.put_be32(crc);
    }
}

/// Safe bounds-checked decoding cursor matching Prometheus `encoding.Decbuf`.
#[derive(Clone, Debug)]
pub struct Decbuf<'a> {
    pub buf: &'a [u8],
}

impl<'a> Decbuf<'a> {
    #[inline]
    pub const fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[inline]
    pub const fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    #[inline]
    pub const fn remaining(&self) -> &'a [u8] {
        self.buf
    }

    #[inline]
    pub fn skip(&mut self, n: usize) -> Result<(), EncodingError> {
        if self.buf.len() < n {
            return Err(EncodingError::InvalidSize);
        }
        self.buf = &self.buf[n..];
        Ok(())
    }

    #[inline]
    pub fn byte(&mut self) -> Result<u8, EncodingError> {
        if self.buf.is_empty() {
            return Err(EncodingError::InvalidSize);
        }
        let b = self.buf[0];
        self.buf = &self.buf[1..];
        Ok(b)
    }

    #[inline]
    pub fn be32(&mut self) -> Result<u32, EncodingError> {
        if self.buf.len() < 4 {
            return Err(EncodingError::InvalidSize);
        }
        let v = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
        self.buf = &self.buf[4..];
        Ok(v)
    }

    #[inline]
    pub fn be64(&mut self) -> Result<u64, EncodingError> {
        if self.buf.len() < 8 {
            return Err(EncodingError::InvalidSize);
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.buf[..8]);
        self.buf = &self.buf[8..];
        Ok(u64::from_be_bytes(arr))
    }

    #[inline]
    pub fn be64_i64(&mut self) -> Result<i64, EncodingError> {
        self.be64().map(|v| v as i64)
    }

    #[inline]
    pub fn be64_f64(&mut self) -> Result<f64, EncodingError> {
        self.be64().map(f64::from_bits)
    }

    #[inline]
    pub fn uvarint64(&mut self) -> Result<u64, EncodingError> {
        let mut x = 0u64;
        let mut s = 0u32;
        for (i, &b) in self.buf.iter().enumerate() {
            if i == 10 {
                return Err(EncodingError::InvalidVarint);
            }
            if b < 0x80 {
                if i == 9 && b > 1 {
                    return Err(EncodingError::InvalidVarint);
                }
                self.buf = &self.buf[i + 1..];
                return Ok(x | (u64::from(b) << s));
            }
            x |= u64::from(b & 0x7f) << s;
            s += 7;
        }
        Err(EncodingError::InvalidSize)
    }

    #[inline]
    pub fn uvarint32(&mut self) -> Result<u32, EncodingError> {
        self.uvarint64().map(|v| v as u32)
    }

    #[inline]
    pub fn uvarint(&mut self) -> Result<usize, EncodingError> {
        self.uvarint64().map(|v| v as usize)
    }

    #[inline]
    pub fn varint64(&mut self) -> Result<i64, EncodingError> {
        let ux = self.uvarint64()?;
        let x = (ux >> 1) as i64;
        if (ux & 1) != 0 {
            Ok(!x)
        } else {
            Ok(x)
        }
    }

    #[inline]
    pub fn uvarint_bytes(&mut self) -> Result<&'a [u8], EncodingError> {
        let len = self.uvarint()?;
        if self.buf.len() < len {
            return Err(EncodingError::InvalidSize);
        }
        let s = &self.buf[..len];
        self.buf = &self.buf[len..];
        Ok(s)
    }

    #[inline]
    pub fn uvarint_str(&mut self) -> Result<&'a str, EncodingError> {
        let b = self.uvarint_bytes()?;
        std::str::from_utf8(b).map_err(|_| EncodingError::InvalidUtf8)
    }
}

/// Encodes `v` as unsigned LEB128 into `out` and returns bytes written.
#[inline]
pub fn put_uvarint(out: &mut [u8], mut v: u64) -> usize {
    let mut i = 0;
    while v >= 0x80 {
        out[i] = (v as u8) | 0x80;
        v >>= 7;
        i += 1;
    }
    out[i] = v as u8;
    i + 1
}

/// Decodes unsigned LEB128 from `buf`, returning `(value, bytes_read)` or `None` on truncation/overflow.
#[inline]
pub fn decode_uvarint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut x = 0u64;
    let mut s = 0u32;
    for (i, &b) in buf.iter().enumerate() {
        if i == 10 {
            return None;
        }
        if b < 0x80 {
            if i == 9 && b > 1 {
                return None;
            }
            return Some((x | (u64::from(b) << s), i + 1));
        }
        x |= u64::from(b & 0x7f) << s;
        s += 7;
    }
    None
}

/// File utilities: directory size, advisory flock, and Linux Direct IO buffered writer.
///
/// Ported from github.com/prometheus/prometheus/tsdb/fileutil/*
pub mod fileutil {
    use std::fs::{self, File, OpenOptions};
    use std::io::{self, Seek, SeekFrom, Write};
    use std::ops::{Deref, DerefMut};
    use std::path::Path;

    pub const DEFAULT_ALIGNMENT: usize = 4096;
    pub const DEFAULT_BUF_SIZE: usize = 4096;
    pub const ERR_WRITER_INVALID: &str =
        "the last flush resulted in an unaligned offset, the writer can no longer ensure contiguous writes";

    /// Recursively computes the sum of regular file sizes under `dir`, matching Go `fileutil.DirSize`.
    pub fn dir_size(dir: &Path) -> io::Result<u64> {
        let mut total = 0u64;
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_dir() {
                total += dir_size(&entry.path())?;
            } else {
                total += meta.len();
            }
        }
        Ok(total)
    }

    /// Advisory file lock matching Go `fileutil.Flock` / `Releaser`.
    #[derive(Debug)]
    pub struct FileLock {
        file: File,
    }

    impl FileLock {
        /// Locks `path` exclusively (non-blocking). Creates the file (and parent dirs) if missing.
        /// Returns `(lock, existed)` where `existed` is whether `path` existed prior to locking.
        pub fn flock(path: impl AsRef<Path>) -> io::Result<(Self, bool)> {
            let path = path.as_ref();
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)?;
                }
            }
            let existed = path.try_exists()?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?;
            if let Err(e) = file.try_lock() {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("lock {}: {e}", path.display()),
                ));
            }
            Ok((Self { file }, existed))
        }

        /// Releases the advisory lock and closes the file descriptor (keeping the file on disk).
        pub fn release(self) -> io::Result<()> {
            self.file.unlock()?;
            drop(self.file);
            Ok(())
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct DirectIORqmts {
        pub memory_align: usize,
        pub offset_align: usize,
    }

    impl Default for DirectIORqmts {
        fn default() -> Self {
            Self {
                memory_align: DEFAULT_ALIGNMENT,
                offset_align: DEFAULT_ALIGNMENT,
            }
        }
    }

    /// An owned byte buffer whose starting slice is aligned to `memory_align`.
    #[derive(Clone, Debug)]
    pub struct AlignedBlock {
        backing: Vec<u8>,
        start: usize,
        len: usize,
    }

    impl AlignedBlock {
        pub fn as_slice(&self) -> &[u8] {
            &self.backing[self.start..self.start + self.len]
        }

        pub fn as_mut_slice(&mut self) -> &mut [u8] {
            &mut self.backing[self.start..self.start + self.len]
        }
    }

    impl Deref for AlignedBlock {
        type Target = [u8];
        fn deref(&self) -> &[u8] {
            self.as_slice()
        }
    }

    impl DerefMut for AlignedBlock {
        fn deref_mut(&mut self) -> &mut [u8] {
            self.as_mut_slice()
        }
    }

    #[inline]
    pub fn compute_alignment_offset(block: &[u8], alignment: usize) -> usize {
        if alignment == 0 {
            return 0;
        }
        assert!(!block.is_empty(), "empty block not supported");
        (block.as_ptr() as usize) & (alignment - 1)
    }

    #[inline]
    pub fn is_aligned(block: &[u8], rqmts: &DirectIORqmts) -> bool {
        compute_alignment_offset(block, rqmts.memory_align) == 0
            && block.len().is_multiple_of(rqmts.offset_align)
    }

    /// Allocates a block whose address is aligned to `rqmts.memory_align` and whose length is `size`.
    /// Panics if `size == 0` or `size` is not a multiple of `rqmts.offset_align`.
    pub fn aligned_block(size: usize, rqmts: &DirectIORqmts) -> AlignedBlock {
        assert!(
            size != 0 && size.is_multiple_of(rqmts.offset_align),
            "size {size} should be > 0 and a multiple of offset_align={}",
            rqmts.offset_align
        );
        if rqmts.memory_align == 0 {
            return AlignedBlock {
                backing: vec![0u8; size],
                start: 0,
                len: size,
            };
        }
        let backing = vec![0u8; size + rqmts.memory_align];
        let a = compute_alignment_offset(&backing, rqmts.memory_align);
        let start = if a == 0 { 0 } else { rqmts.memory_align - a };
        let blk = AlignedBlock {
            backing,
            start,
            len: size,
        };
        assert!(is_aligned(&blk, rqmts), "cannot create an aligned block");
        blk
    }

    pub fn current_file_offset(f: &mut File) -> io::Result<usize> {
        let off = f.stream_position()?;
        Ok(off as usize)
    }

    #[cfg(all(target_os = "linux", not(miri)))]
    mod linux_dio {
        use super::*;
        use std::os::unix::io::AsRawFd;

        // Kernel ABI constants (identical across all Linux architectures, independent of musl/glibc version).
        const STATX_DIOALIGN: libc::c_uint = 0x2000;
        const AT_STATX_DONT_SYNC: libc::c_int = 0x4000;

        #[repr(C, align(8))]
        struct RawStatx {
            data: [u8; 256],
        }

        pub fn enable_direct_io(f: &File) -> io::Result<()> {
            let fd = f.as_raw_fd();
            // SAFETY: valid fd and F_GETFL takes no extra argument.
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            if flags < 0 {
                return Err(io::Error::last_os_error());
            }
            if (flags & libc::O_DIRECT) == libc::O_DIRECT {
                return Ok(());
            }
            // SAFETY: valid fd and integer flag argument for F_SETFL.
            let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_DIRECT) };
            if rc < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        pub fn file_direct_io_rqmts(f: &File) -> io::Result<DirectIORqmts> {
            let mut stx = RawStatx { data: [0u8; 256] };
            let flags = libc::AT_SYMLINK_NOFOLLOW | libc::AT_EMPTY_PATH | AT_STATX_DONT_SYNC;
            let empty = b"\0";
            // SAFETY: SYS_statx with valid fd, NUL-terminated empty path, and 256-byte aligned statx buffer.
            let rc = unsafe {
                libc::syscall(
                    libc::SYS_statx,
                    f.as_raw_fd(),
                    empty.as_ptr().cast::<libc::c_char>(),
                    flags,
                    STATX_DIOALIGN,
                    &mut stx,
                )
            };
            if rc < 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::ENOSYS) {
                    return Ok(DirectIORqmts::default());
                }
                return Err(err);
            }
            let stx_mask = u32::from_ne_bytes(stx.data[0..4].try_into().unwrap());
            if (stx_mask & STATX_DIOALIGN) == 0 {
                return Ok(DirectIORqmts::default());
            }
            let mem_align = u32::from_ne_bytes(stx.data[152..156].try_into().unwrap()) as usize;
            let off_align = u32::from_ne_bytes(stx.data[156..160].try_into().unwrap()) as usize;
            if mem_align == 0 || off_align == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "direct IO is unsupported on this filesystem",
                ));
            }
            Ok(DirectIORqmts {
                memory_align: mem_align,
                offset_align: off_align,
            })
        }
    }

    #[cfg(not(all(target_os = "linux", not(miri))))]
    mod linux_dio {
        use super::*;

        pub fn enable_direct_io(_f: &File) -> io::Result<()> {
            Ok(())
        }

        pub fn file_direct_io_rqmts(_f: &File) -> io::Result<DirectIORqmts> {
            Ok(DirectIORqmts::default())
        }
    }

    pub use linux_dio::{enable_direct_io, file_direct_io_rqmts};

    fn check_initial_unaligned_offset(
        f: &mut File,
        rqmts: &DirectIORqmts,
    ) -> io::Result<usize> {
        let offset = current_file_offset(f)?;
        let alignment = rqmts.offset_align;
        let gap = (alignment - (offset % alignment)) % alignment;
        if gap == 0 {
            enable_direct_io(f)?;
        }
        Ok(gap)
    }

    /// Buffered `Write` wrapper supporting Linux `O_DIRECT` with automatic alignment padding.
    pub struct DirectIOWriter {
        buf: AlignedBlock,
        n: usize,
        f: File,
        offset_alignment_gap: usize,
        alignment_rqmts: DirectIORqmts,
        err: Option<io::Error>,
        invalid: bool,
    }

    impl DirectIOWriter {
        pub fn new(mut f: File, mut size: usize) -> io::Result<Self> {
            let rqmts = file_direct_io_rqmts(&f)?;
            if size == 0 {
                size = DEFAULT_BUF_SIZE;
            }
            if !size.is_multiple_of(rqmts.offset_align) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("size {size} should be a multiple of {}", rqmts.offset_align),
                ));
            }
            let gap = check_initial_unaligned_offset(&mut f, &rqmts)?;
            Ok(Self {
                buf: aligned_block(size, &rqmts),
                n: 0,
                f,
                offset_alignment_gap: gap,
                alignment_rqmts: rqmts,
                err: None,
                invalid: false,
            })
        }

        #[inline]
        pub fn available(&self) -> usize {
            self.buf.len() - self.n
        }

        #[inline]
        pub fn buffered(&self) -> usize {
            self.n
        }

        #[inline]
        pub fn is_invalid(&self) -> bool {
            self.invalid
        }

        #[inline]
        pub fn file_mut(&mut self) -> &mut File {
            &mut self.f
        }

        fn clone_err(e: &io::Error) -> io::Error {
            io::Error::new(e.kind(), e.to_string())
        }

        fn fill_initial_offset_gap(&mut self) {
            if self.n == 0 || self.offset_alignment_gap == 0 {
                return;
            }
            let bytes_to_align = self.n.min(self.offset_alignment_gap);
            let res = self.f.write(&self.buf[..bytes_to_align]);
            match res {
                Ok(n) => {
                    if n > 0 {
                        self.buf.copy_within(n..self.n, 0);
                        self.n -= n;
                    }
                    self.offset_alignment_gap -= n;
                    if n < bytes_to_align && self.err.is_none() {
                        self.err = Some(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "short write filling alignment gap",
                        ));
                    }
                    if self.offset_alignment_gap == 0 {
                        if let Err(e) = enable_direct_io(&self.f) {
                            if self.err.is_none() {
                                self.err = Some(e);
                            }
                        }
                    }
                }
                Err(e) => {
                    if self.err.is_none() {
                        self.err = Some(e);
                    }
                }
            }
        }

        fn direct_io_write_to(
            f: &mut File,
            rqmts: &DirectIORqmts,
            invalid: &mut bool,
            writer_err: &mut Option<io::Error>,
            p: &[u8],
            padding: usize,
        ) -> (usize, Option<io::Error>) {
            let mut relevant = p.len() - padding;
            let mut err: Option<io::Error> = None;
            match f.write(p) {
                Ok(n) => {
                    if n < relevant {
                        relevant = n;
                        err = Some(io::Error::new(
                            io::ErrorKind::WriteZero,
                            format!("short write (wrote {n} < {})", p.len() - padding),
                        ));
                    } else if n > relevant {
                        let written_padding = (n - relevant) as i64;
                        if let Err(seek_err) = f.seek(SeekFrom::Current(-written_padding)) {
                            let wrapped = io::Error::new(
                                seek_err.kind(),
                                format!("seek to discard written padding {written_padding}: {seek_err}"),
                            );
                            if writer_err.is_none() {
                                *writer_err = Some(wrapped);
                            }
                        }
                    }
                }
                Err(e) => {
                    relevant = 0;
                    err = Some(e);
                }
            }
            if !relevant.is_multiple_of(rqmts.offset_align) {
                *invalid = true;
            }
            (relevant, err)
        }

        fn direct_io_write(&mut self, p: &[u8], padding: usize) -> io::Result<usize> {
            let (written, err) = Self::direct_io_write_to(
                &mut self.f,
                &self.alignment_rqmts,
                &mut self.invalid,
                &mut self.err,
                p,
                padding,
            );
            match err {
                Some(e) => Err(e),
                None => Ok(written),
            }
        }

        fn can_direct_io_write(&self, p: &[u8]) -> bool {
            is_aligned(p, &self.alignment_rqmts) && self.offset_alignment_gap == 0
        }

        fn flush_internal(&mut self) -> io::Result<()> {
            if self.invalid {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, ERR_WRITER_INVALID));
            }
            if let Some(ref e) = self.err {
                return Err(Self::clone_err(e));
            }
            if self.n == 0 {
                return Ok(());
            }

            let align = self.alignment_rqmts.offset_align;
            let mut u_offset = self.n;
            if !u_offset.is_multiple_of(align) {
                u_offset = ((u_offset / align) + 1) * align;
                self.buf[self.n..u_offset].fill(0);
            }
            let padding = u_offset - self.n;
            let (written, err) = Self::direct_io_write_to(
                &mut self.f,
                &self.alignment_rqmts,
                &mut self.invalid,
                &mut self.err,
                &self.buf[..u_offset],
                padding,
            );
            if let Some(e) = err {
                if written > 0 && written < self.n {
                    self.buf.copy_within(written..self.n, 0);
                }
                self.n = self.n.saturating_sub(written);
                if self.err.is_none() {
                    self.err = Some(Self::clone_err(&e));
                }
                return Err(e);
            }
            self.n = 0;
            Ok(())
        }

        pub fn write_bytes(&mut self, mut p: &[u8]) -> io::Result<usize> {
            if self.invalid {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, ERR_WRITER_INVALID));
            }
            let mut nn = 0usize;
            while p.len() > self.available() && self.err.is_none() {
                let mut n1 = 0usize;
                let mut n2 = 0usize;
                if self.buffered() == 0 && self.can_direct_io_write(p) {
                    match self.direct_io_write(p, 0) {
                        Ok(n) => n1 = n,
                        Err(e) => {
                            self.err = Some(e);
                        }
                    }
                } else {
                    n1 = p.len().min(self.available());
                    self.buf[self.n..self.n + n1].copy_from_slice(&p[..n1]);
                    self.n += n1;
                    if self.offset_alignment_gap != 0 {
                        self.fill_initial_offset_gap();
                        n2 = (p.len() - n1).min(self.available());
                        self.buf[self.n..self.n + n2].copy_from_slice(&p[n1..n1 + n2]);
                        self.n += n2;
                    }
                    if self.available() == 0 {
                        if let Err(e) = self.flush_internal() {
                            if self.err.is_none() {
                                self.err = Some(e);
                            }
                        }
                    }
                }
                nn += n1 + n2;
                p = &p[n1 + n2..];
            }

            if let Some(ref e) = self.err {
                return Err(Self::clone_err(e));
            }
            let n = p.len().min(self.available());
            self.buf[self.n..self.n + n].copy_from_slice(&p[..n]);
            self.n += n;
            nn += n;
            Ok(nn)
        }

        pub fn flush_writer(&mut self) -> io::Result<()> {
            if self.offset_alignment_gap != 0 {
                self.fill_initial_offset_gap();
                if let Some(ref e) = self.err {
                    return Err(Self::clone_err(e));
                }
            }
            self.flush_internal()
        }

        pub fn reset(&mut self, f: File) -> io::Result<()> {
            let rqmts = file_direct_io_rqmts(&f)?;
            self.reset_with_rqmts(f, rqmts)
        }

        fn reset_with_rqmts(&mut self, mut f: File, rqmts: DirectIORqmts) -> io::Result<()> {
            if !is_aligned(&self.buf, &rqmts) {
                let new_len = self
                    .buf
                    .len()
                    .max(DEFAULT_BUF_SIZE)
                    .div_ceil(rqmts.offset_align)
                    * rqmts.offset_align;
                self.buf = aligned_block(new_len, &rqmts);
            }
            self.alignment_rqmts = rqmts;
            let gap = check_initial_unaligned_offset(&mut f, &rqmts)?;
            self.offset_alignment_gap = gap;
            self.err = None;
            self.invalid = false;
            self.n = 0;
            self.f = f;
            Ok(())
        }
    }

    impl Write for DirectIOWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.write_bytes(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flush_writer()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn direct_io_writer_reset_reallocates_for_larger_alignment() {
            let dir = tempfile::tempdir().unwrap();
            let p1 = dir.path().join("f1");
            let p2 = dir.path().join("f2");
            let f1 = OpenOptions::new().create(true).write(true).truncate(true).open(&p1).unwrap();
            let f2 = OpenOptions::new().create(true).write(true).truncate(true).open(&p2).unwrap();

            let small_rqmts = DirectIORqmts {
                memory_align: 512,
                offset_align: 512,
            };
            let large_rqmts = DirectIORqmts {
                memory_align: 4096,
                offset_align: 4096,
            };
            let mut w = DirectIOWriter {
                buf: aligned_block(512, &small_rqmts),
                n: 0,
                f: f1,
                offset_alignment_gap: 0,
                alignment_rqmts: small_rqmts,
                err: None,
                invalid: false,
            };

            w.write_bytes(&[0xAB; 100]).unwrap();
            w.flush_writer().unwrap();
            assert!(w.is_invalid());

            // Transition to a file with larger memory/offset alignment.
            w.reset_with_rqmts(f2, large_rqmts).unwrap();
            assert!(!w.is_invalid());
            assert!(is_aligned(&w.buf, &large_rqmts));
            assert!(w.buf.len() >= 4096);
            assert!(w.buf.len().is_multiple_of(large_rqmts.offset_align));

            // Writing 500 bytes (< 4096 offset_align) and flushing pads up to 4096 without OOB panic.
            let payload = vec![0xCD; 500];
            w.write_bytes(&payload).unwrap();
            w.flush_writer().unwrap();
            assert!(w.is_invalid());
            let disk = fs::read(&p2).unwrap();
            assert_eq!(disk.len(), 4096);
            assert_eq!(&disk[..500], payload.as_slice());
        }
    }
}
