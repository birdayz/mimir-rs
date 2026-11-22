//! Bit-exact Prometheus TSDB tombstones reader & writer.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/tombstones/tombstones.go.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use hashbrown::HashMap;
use parking_lot::RwLock;
use thiserror::Error;

use crate::encoding::{Decbuf, Encbuf, EncodingError};

pub const TOMBSTONES_FILENAME: &str = "tombstones";
pub const MAGIC_TOMBSTONE: u32 = 0x0130_BA30;
pub const TOMBSTONE_FORMAT_V1: u8 = 1;
const TOMBSTONE_FORMAT_VERSION_SIZE: usize = 1;
const TOMBSTONES_HEADER_SIZE: usize = 5;
const TOMBSTONES_CRC_SIZE: usize = 4;

#[derive(Debug, Error)]
pub enum TombstoneError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("encoding error: {0}")]
    Encoding(#[from] EncodingError),
    #[error("invalid tombstones magic number {0:#x}")]
    InvalidMagic(u32),
    #[error("invalid tombstones format version {0}")]
    InvalidFormat(u8),
    #[error("tombstones checksum mismatch (expected {expected:#x}, actual {actual:#x})")]
    ChecksumMismatch { expected: u32, actual: u32 },
}

/// Closed deletion interval `[mint, maxt]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Interval {
    pub mint: i64,
    pub maxt: i64,
}

impl Interval {
    #[inline]
    pub const fn new(mint: i64, maxt: i64) -> Self {
        Self { mint, maxt }
    }

    #[inline]
    pub const fn in_bounds(&self, t: i64) -> bool {
        t >= self.mint && t <= self.maxt
    }

    #[inline]
    pub const fn is_subrange(&self, mint: i64, maxt: i64) -> bool {
        mint >= self.mint && maxt <= self.maxt
    }

    #[inline]
    pub const fn overlaps(&self, mint: i64, maxt: i64) -> bool {
        self.mint <= maxt && mint <= self.maxt
    }
}

/// Sorted, non-overlapping list of deletion intervals for a single series.
pub type Intervals = Vec<Interval>;

/// Adds `itv` into a sorted, coalesced `Intervals` slice matching `tombstones.Intervals.Add`.
pub fn add_interval(ivs: &mut Intervals, itv: Interval) {
    if itv.mint > itv.maxt {
        return;
    }
    let pos = ivs.partition_point(|x| x.mint < itv.mint);
    if pos > 0 && ivs[pos - 1].maxt >= itv.mint.saturating_sub(1) {
        if ivs[pos - 1].maxt < itv.maxt {
            ivs[pos - 1].maxt = itv.maxt;
        }
        coalesce_from(ivs, pos - 1);
    } else {
        ivs.insert(pos, itv);
        coalesce_from(ivs, pos);
    }
}

fn coalesce_from(ivs: &mut Intervals, idx: usize) {
    while idx + 1 < ivs.len() && ivs[idx].maxt >= ivs[idx + 1].mint.saturating_sub(1) {
        if ivs[idx + 1].maxt > ivs[idx].maxt {
            ivs[idx].maxt = ivs[idx + 1].maxt;
        }
        ivs.remove(idx + 1);
    }
}

/// Returns `true` if the closed interval `[mint, maxt]` is completely covered by `ivs`.
pub fn is_completely_deleted(ivs: &[Interval], mint: i64, maxt: i64) -> bool {
    ivs.iter().any(|iv| iv.is_subrange(mint, maxt))
}

/// Thread-safe in-memory tombstone store matching `tombstones.MemTombstones`.
#[derive(Debug, Default)]
pub struct MemTombstones {
    groups: RwLock<HashMap<u64, Intervals>>,
    has_tombstones: AtomicBool,
}

impl PartialEq for MemTombstones {
    fn eq(&self, other: &Self) -> bool {
        let g1 = self.groups.read();
        let g2 = other.groups.read();
        *g1 == *g2
    }
}

impl Eq for MemTombstones {}

impl MemTombstones {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_interval(&self, ref_id: u64, itv: Interval) {
        let mut groups = self.groups.write();
        let ivs = groups.entry(ref_id).or_default();
        add_interval(ivs, itv);
        if !groups.is_empty() {
            self.has_tombstones.store(true, Ordering::Release);
        }
    }

    #[inline]
    pub fn has_any(&self) -> bool {
        self.has_tombstones.load(Ordering::Acquire)
    }

    pub fn get(&self, ref_id: u64) -> Intervals {
        if !self.has_tombstones.load(Ordering::Acquire) {
            return Vec::new();
        }
        let groups = self.groups.read();
        groups.get(&ref_id).cloned().unwrap_or_default()
    }

    pub fn get_into(&self, ref_id: u64, out: &mut Intervals) {
        out.clear();
        if !self.has_tombstones.load(Ordering::Acquire) {
            return;
        }
        let groups = self.groups.read();
        if let Some(ivs) = groups.get(&ref_id) {
            out.extend_from_slice(ivs);
        }
    }

    pub fn delete_tombstones(&self, refs: &HashSet<u64>) {
        if !self.has_tombstones.load(Ordering::Acquire) {
            return;
        }
        let mut groups = self.groups.write();
        for r in refs {
            groups.remove(r);
        }
        if groups.is_empty() {
            self.has_tombstones.store(false, Ordering::Release);
        }
    }

    pub fn truncate_before(&self, before_t: i64) {
        if !self.has_tombstones.load(Ordering::Acquire) {
            return;
        }
        let mut groups = self.groups.write();
        groups.retain(|_, ivs| {
            ivs.retain(|iv| iv.maxt >= before_t);
            !ivs.is_empty()
        });
        if groups.is_empty() {
            self.has_tombstones.store(false, Ordering::Release);
        }
    }

    pub fn total(&self) -> u64 {
        let groups = self.groups.read();
        groups.values().map(|v| v.len() as u64).sum()
    }

    pub fn iter<F>(&self, mut f: F)
    where
        F: FnMut(u64, &[Interval]),
    {
        let groups = self.groups.read();
        for (&ref_id, ivs) in groups.iter() {
            f(ref_id, ivs);
        }
    }
}

/// Encodes tombstones payload (1-byte version + uvarint/varint intervals), without magic/CRC.
pub fn encode_tombstones(tr: &MemTombstones) -> Vec<u8> {
    let mut buf = Encbuf::new();
    buf.put_byte(TOMBSTONE_FORMAT_V1);
    tr.iter(|ref_id, ivs| {
        for iv in ivs {
            buf.put_uvarint64(ref_id);
            buf.put_varint64(iv.mint);
            buf.put_varint64(iv.maxt);
        }
    });
    buf.into_vec()
}

/// Decodes tombstones payload produced by [`encode_tombstones`].
pub fn decode_tombstones(bytes: &[u8]) -> Result<MemTombstones, TombstoneError> {
    let mut dec = Decbuf::new(bytes);
    let flag = dec.byte()?;
    if flag != TOMBSTONE_FORMAT_V1 {
        return Err(TombstoneError::InvalidFormat(flag));
    }
    let stones = MemTombstones::new();
    while !dec.is_empty() {
        let ref_id = dec.uvarint64()?;
        let mint = dec.varint64()?;
        let maxt = dec.varint64()?;
        stones.add_interval(ref_id, Interval::new(mint, maxt));
    }
    Ok(stones)
}

/// Atomically writes `<dir>/tombstones` in exact Prometheus v1 format.
pub fn write_tombstones_file(dir: &Path, tr: &MemTombstones) -> Result<usize, TombstoneError> {
    let path = dir.join(TOMBSTONES_FILENAME);
    let tmp = dir.join(format!("{TOMBSTONES_FILENAME}.tmp"));

    let payload = encode_tombstones(tr);
    // Match Prometheus: CRC32C hashes everything after the 1-byte format version!
    let crc = crc32c::crc32c(&payload[TOMBSTONE_FORMAT_VERSION_SIZE..]);

    let mut out = Vec::with_capacity(4 + payload.len() + TOMBSTONES_CRC_SIZE);
    out.extend_from_slice(&MAGIC_TOMBSTONE.to_be_bytes());
    out.extend_from_slice(&payload);
    out.extend_from_slice(&crc.to_be_bytes());

    let write_res = (|| -> io::Result<()> {
        let mut f = File::create(&tmp)?;
        f.write_all(&out)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, &path)?;
        Ok(())
    })();
    if let Err(e) = write_res {
        let _ = fs::remove_file(&tmp);
        return Err(TombstoneError::Io(e));
    }
    crate::block::sync_dir(dir)?;
    Ok(out.len())
}

/// Reads `<dir>/tombstones`, returning an empty `MemTombstones` if the file does not exist.
pub fn read_tombstones_file(dir: &Path) -> Result<MemTombstones, TombstoneError> {
    let path = dir.join(TOMBSTONES_FILENAME);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(MemTombstones::new()),
        Err(e) => return Err(TombstoneError::Io(e)),
    };

    if bytes.len() < TOMBSTONES_HEADER_SIZE + TOMBSTONES_CRC_SIZE {
        return Err(TombstoneError::Encoding(EncodingError::InvalidSize));
    }

    let magic = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if magic != MAGIC_TOMBSTONE {
        return Err(TombstoneError::InvalidMagic(magic));
    }

    let payload = &bytes[4..bytes.len() - TOMBSTONES_CRC_SIZE];
    let expected_crc = u32::from_be_bytes(
        bytes[bytes.len() - TOMBSTONES_CRC_SIZE..]
            .try_into()
            .unwrap(),
    );
    let actual_crc = crc32c::crc32c(&payload[TOMBSTONE_FORMAT_VERSION_SIZE..]);
    if actual_crc != expected_crc {
        return Err(TombstoneError::ChecksumMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    decode_tombstones(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_and_read_tombstones_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let ts = MemTombstones::new();
        ts.add_interval(1, Interval::new(100, 200));
        ts.add_interval(1, Interval::new(201, 300)); // coalesces to [100, 300]
        ts.add_interval(42, Interval::new(500, 600));

        write_tombstones_file(dir.path(), &ts).unwrap();
        let read_back = read_tombstones_file(dir.path()).unwrap();
        assert_eq!(read_back.get(1), vec![Interval::new(100, 300)]);
        assert_eq!(read_back.get(42), vec![Interval::new(500, 600)]);
        assert_eq!(read_back.total(), 2);
    }
}
