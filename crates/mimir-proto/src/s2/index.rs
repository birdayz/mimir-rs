//! Ported from github.com/klauspost/compress/s2/index.go.

use std::io;

use super::block::{decode_block_uvarint, put_uvarint};
use super::{
    S2Error, CHECKSUM_SIZE, CHUNK_TYPE_COMPRESSED, CHUNK_TYPE_INDEX, CHUNK_TYPE_STREAM_IDENTIFIER,
    CHUNK_TYPE_UNCOMPRESSED, MAGIC_S2, MAGIC_SNAPPY, MAX_BLOCK_SIZE, MAX_CHUNK_SIZE,
    SKIPPABLE_FRAME_HEADER,
};

pub const S2_INDEX_HEADER: &[u8] = b"s2idx\x00";
pub const S2_INDEX_TRAILER: &[u8] = b"\x00xdi2s";
pub const MAX_INDEX_ENTRIES: usize = 1 << 16;
pub const MIN_INDEX_DIST: i64 = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub compressed_offset: i64,
    pub uncompressed_offset: i64,
}

#[derive(Debug, Clone)]
pub struct Index {
    pub total_uncompressed: i64,
    pub total_compressed: i64,
    pub info: Vec<IndexEntry>,
    pub est_block_uncomp: i64,
}

impl Default for Index {
    fn default() -> Self {
        Self {
            total_uncompressed: -1,
            total_compressed: -1,
            info: Vec::new(),
            est_block_uncomp: 0,
        }
    }
}

fn put_varint(dst: &mut Vec<u8>, v: i64) {
    let mut ux = (v as u64) << 1;
    if v < 0 {
        ux = !ux;
    }
    let mut tmp = [0u8; 10];
    let n = put_uvarint(&mut tmp, ux);
    dst.extend_from_slice(&tmp[..n]);
}

fn decode_varint(src: &[u8]) -> Option<(i64, usize)> {
    let mut ux: u64 = 0;
    let mut s = 0u32;
    for (i, &b) in src.iter().take(10).enumerate() {
        if i == 9 && b > 1 {
            return None;
        }
        if b < 0x80 {
            ux |= u64::from(b) << s;
            let mut x = (ux >> 1) as i64;
            if ux & 1 != 0 {
                x = !x;
            }
            return Some((x, i + 1));
        }
        ux |= u64::from(b & 0x7f) << s;
        s += 7;
    }
    None
}

impl Index {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self, max_block: usize) {
        self.est_block_uncomp = max_block as i64;
        self.total_compressed = -1;
        self.total_uncompressed = -1;
        self.info.clear();
    }

    pub fn add(&mut self, compressed_offset: i64, uncompressed_offset: i64) -> Result<(), S2Error> {
        if let Some(last) = self.info.last_mut() {
            if last.uncompressed_offset == uncompressed_offset {
                last.compressed_offset = compressed_offset;
                return Ok(());
            }
            if last.uncompressed_offset > uncompressed_offset {
                return Err(S2Error::Corrupt);
            }
            if last.compressed_offset > compressed_offset {
                return Err(S2Error::Corrupt);
            }
            if last.uncompressed_offset + MIN_INDEX_DIST > uncompressed_offset {
                return Ok(());
            }
        }
        self.info.push(IndexEntry {
            compressed_offset,
            uncompressed_offset,
        });
        Ok(())
    }

    /// Finds the compressed and uncompressed offsets at or before `offset`.
    pub fn find(&self, mut offset: i64) -> Result<(i64, i64), S2Error> {
        if self.total_uncompressed < 0 {
            return Err(S2Error::Corrupt);
        }
        if offset < 0 {
            offset += self.total_uncompressed;
            if offset < 0 {
                return Err(S2Error::Io(io::ErrorKind::UnexpectedEof));
            }
        }
        if offset > self.total_uncompressed {
            return Err(S2Error::Io(io::ErrorKind::UnexpectedEof));
        }
        if self.info.len() > 200 {
            let mut n = self.info.partition_point(|e| e.uncompressed_offset <= offset);
            if n == 0 {
                n = 1;
            }
            let e = self.info[n - 1];
            return Ok((e.compressed_offset, e.uncompressed_offset));
        }
        let mut comp_off = 0i64;
        let mut uncomp_off = 0i64;
        for info in &self.info {
            if info.uncompressed_offset > offset {
                break;
            }
            comp_off = info.compressed_offset;
            uncomp_off = info.uncompressed_offset;
        }
        Ok((comp_off, uncomp_off))
    }

    pub fn reduce(&mut self) {
        if self.info.len() < MAX_INDEX_ENTRIES && self.est_block_uncomp >= MIN_INDEX_DIST {
            return;
        }
        let mut remove_n = (self.info.len() + 1) / MAX_INDEX_ENTRIES;
        while self.est_block_uncomp * (remove_n as i64 + 1) < MIN_INDEX_DIST
            && self.info.len() / (remove_n + 1) > 1000
        {
            remove_n += 1;
        }
        if remove_n == 0 {
            return;
        }
        let mut j = 0usize;
        let mut idx = 0usize;
        while idx < self.info.len() {
            self.info[j] = self.info[idx];
            j += 1;
            idx += 1 + remove_n;
        }
        self.info.truncate(j);
        self.est_block_uncomp += self.est_block_uncomp * (remove_n as i64);
    }

    pub fn append_to(&mut self, b: &mut Vec<u8>, comp_total: i64, uncomp_total: i64) {
        self.reduce();
        let init_size = b.len();
        b.extend_from_slice(&[CHUNK_TYPE_INDEX, 0, 0, 0]);
        b.extend_from_slice(S2_INDEX_HEADER);
        put_varint(b, uncomp_total);
        put_varint(b, comp_total);
        put_varint(b, self.est_block_uncomp);
        put_varint(b, self.info.len() as i64);

        let mut has_uncompressed = 0u8;
        for (idx, info) in self.info.iter().enumerate() {
            if idx == 0 {
                if info.uncompressed_offset != 0 {
                    has_uncompressed = 1;
                    break;
                }
            } else if info.uncompressed_offset
                != self.info[idx - 1].uncompressed_offset + self.est_block_uncomp
            {
                has_uncompressed = 1;
                break;
            }
        }
        b.push(has_uncompressed);

        if has_uncompressed == 1 {
            for (idx, info) in self.info.iter().enumerate() {
                let mut u_off = info.uncompressed_offset;
                if idx > 0 {
                    u_off -= self.info[idx - 1].uncompressed_offset + self.est_block_uncomp;
                }
                put_varint(b, u_off);
            }
        }

        let mut c_predict = self.est_block_uncomp / 2;
        for (idx, info) in self.info.iter().enumerate() {
            let mut c_off = info.compressed_offset;
            if idx > 0 {
                c_off -= self.info[idx - 1].compressed_offset + c_predict;
                c_predict += c_off / 2;
            }
            put_varint(b, c_off);
        }

        let total_sz = (b.len() - init_size + 4 + S2_INDEX_TRAILER.len()) as u32;
        b.extend_from_slice(&total_sz.to_le_bytes());
        b.extend_from_slice(S2_INDEX_TRAILER);

        let chunk_len = b.len() - init_size - SKIPPABLE_FRAME_HEADER;
        b[init_size + 1] = (chunk_len & 0xff) as u8;
        b[init_size + 2] = ((chunk_len >> 8) & 0xff) as u8;
        b[init_size + 3] = ((chunk_len >> 16) & 0xff) as u8;
    }

    pub fn load<'a>(&mut self, mut b: &'a [u8]) -> Result<&'a [u8], S2Error> {
        if b.len() <= 4 + S2_INDEX_HEADER.len() + S2_INDEX_TRAILER.len() {
            return Err(S2Error::Io(io::ErrorKind::UnexpectedEof));
        }
        if b[0] != CHUNK_TYPE_INDEX {
            return Err(S2Error::Corrupt);
        }
        let chunk_len = (b[1] as usize) | ((b[2] as usize) << 8) | ((b[3] as usize) << 16);
        b = &b[4..];
        if b.len() < chunk_len {
            return Err(S2Error::Io(io::ErrorKind::UnexpectedEof));
        }
        if !b.starts_with(S2_INDEX_HEADER) {
            return Err(S2Error::Unsupported);
        }
        b = &b[S2_INDEX_HEADER.len()..];

        let (v, n) = decode_varint(b).ok_or(S2Error::Corrupt)?;
        if v < 0 {
            return Err(S2Error::Corrupt);
        }
        self.total_uncompressed = v;
        b = &b[n..];

        let (v, n) = decode_varint(b).ok_or(S2Error::Corrupt)?;
        self.total_compressed = v;
        b = &b[n..];

        let (v, n) = decode_varint(b).ok_or(S2Error::Corrupt)?;
        if v < 0 {
            return Err(S2Error::Corrupt);
        }
        self.est_block_uncomp = v;
        b = &b[n..];

        let (v, n) = decode_varint(b).ok_or(S2Error::Corrupt)?;
        if v < 0 || v > MAX_INDEX_ENTRIES as i64 {
            return Err(S2Error::Corrupt);
        }
        let entries = v as usize;
        b = &b[n..];

        if b.is_empty() {
            return Err(S2Error::Io(io::ErrorKind::UnexpectedEof));
        }
        let has_uncompressed = b[0];
        b = &b[1..];
        if (has_uncompressed & 1) != has_uncompressed {
            return Err(S2Error::Corrupt);
        }

        self.info.resize(
            entries,
            IndexEntry {
                compressed_offset: 0,
                uncompressed_offset: 0,
            },
        );
        for idx in 0..entries {
            let mut u_off = 0i64;
            if has_uncompressed != 0 {
                let (v, n) = decode_varint(b).ok_or(S2Error::Corrupt)?;
                u_off = v;
                b = &b[n..];
            }
            if idx > 0 {
                let prev = self.info[idx - 1].uncompressed_offset;
                u_off += prev + self.est_block_uncomp;
                if u_off <= prev {
                    return Err(S2Error::Corrupt);
                }
            }
            if u_off < 0 {
                return Err(S2Error::Corrupt);
            }
            self.info[idx].uncompressed_offset = u_off;
        }

        let mut c_predict = self.est_block_uncomp / 2;
        for idx in 0..entries {
            let (mut c_off, n) = decode_varint(b).ok_or(S2Error::Corrupt)?;
            b = &b[n..];
            if idx > 0 {
                let c_predict_new = c_predict + c_off / 2;
                let prev = self.info[idx - 1].compressed_offset;
                c_off += prev + c_predict;
                if c_off <= prev {
                    return Err(S2Error::Corrupt);
                }
                c_predict = c_predict_new;
            }
            if c_off < 0 {
                return Err(S2Error::Corrupt);
            }
            self.info[idx].compressed_offset = c_off;
        }

        if b.len() < 4 + S2_INDEX_TRAILER.len() {
            return Err(S2Error::Io(io::ErrorKind::UnexpectedEof));
        }
        b = &b[4..];
        if !b.starts_with(S2_INDEX_TRAILER) {
            return Err(S2Error::Corrupt);
        }
        Ok(&b[S2_INDEX_TRAILER.len()..])
    }

    pub fn load_stream<R: io::Read + io::Seek>(&mut self, rs: &mut R) -> Result<(), S2Error> {
        let trailer_len = (4 + S2_INDEX_TRAILER.len()) as i64;
        rs.seek(io::SeekFrom::End(-trailer_len))
            .map_err(|e| S2Error::Io(e.kind()))?;
        let mut trailer_buf = vec![0u8; trailer_len as usize];
        rs.read_exact(&mut trailer_buf)
            .map_err(|e| S2Error::Io(e.kind()))?;
        if &trailer_buf[4..] != S2_INDEX_TRAILER {
            return Err(S2Error::Unsupported);
        }
        let sz = u32::from_le_bytes([trailer_buf[0], trailer_buf[1], trailer_buf[2], trailer_buf[3]])
            as usize;
        if sz == 0 || sz > MAX_CHUNK_SIZE {
            return Err(S2Error::Corrupt);
        }
        rs.seek(io::SeekFrom::End(-(sz as i64)))
            .map_err(|e| S2Error::Io(e.kind()))?;
        let mut buf = vec![0u8; sz];
        rs.read_exact(&mut buf).map_err(|e| S2Error::Io(e.kind()))?;
        self.load(&buf).map(|_| ())
    }
}

/// Scans an S2 stream from `r` and builds its binary index.
pub fn index_stream<R: io::Read>(mut r: R) -> Result<Vec<u8>, S2Error> {
    let mut index = Index::new();
    index.total_compressed = 0;
    index.total_uncompressed = 0;
    let mut hdr = [0u8; 4];
    let mut buf = Vec::new();
    let mut read_header = false;

    loop {
        match r.read_exact(&mut hdr) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                let mut out = Vec::new();
                let tc = index.total_compressed;
                let tu = index.total_uncompressed;
                index.append_to(&mut out, tc, tu);
                return Ok(out);
            }
            Err(e) => return Err(S2Error::Io(e.kind())),
        }
        let start_chunk = index.total_compressed;
        index.total_compressed += 4;
        let chunk_type = hdr[0];
        if !read_header {
            if chunk_type != CHUNK_TYPE_STREAM_IDENTIFIER {
                return Err(S2Error::Corrupt);
            }
            read_header = true;
        }
        let chunk_len = (hdr[1] as usize) | ((hdr[2] as usize) << 8) | ((hdr[3] as usize) << 16);
        if chunk_len < CHECKSUM_SIZE {
            return Err(S2Error::Corrupt);
        }
        index.total_compressed += chunk_len as i64;
        buf.resize(chunk_len, 0);
        r.read_exact(&mut buf)
            .map_err(|_| S2Error::Io(io::ErrorKind::UnexpectedEof))?;

        match chunk_type {
            CHUNK_TYPE_COMPRESSED => {
                let (d_len, _) = decode_block_uvarint(&buf[CHECKSUM_SIZE..])?;
                if d_len > MAX_BLOCK_SIZE {
                    return Err(S2Error::Corrupt);
                }
                if index.est_block_uncomp == 0 {
                    index.est_block_uncomp = d_len as i64;
                }
                index.add(start_chunk, index.total_uncompressed)?;
                index.total_uncompressed += d_len as i64;
            }
            CHUNK_TYPE_UNCOMPRESSED => {
                let n2 = chunk_len - CHECKSUM_SIZE;
                if n2 > MAX_BLOCK_SIZE {
                    return Err(S2Error::Corrupt);
                }
                if index.est_block_uncomp == 0 {
                    index.est_block_uncomp = n2 as i64;
                }
                index.add(start_chunk, index.total_uncompressed)?;
                index.total_uncompressed += n2 as i64;
            }
            CHUNK_TYPE_STREAM_IDENTIFIER => {
                if chunk_len != MAGIC_S2.len() {
                    return Err(S2Error::Corrupt);
                }
                if buf != MAGIC_S2 && buf != MAGIC_SNAPPY {
                    return Err(S2Error::Corrupt);
                }
            }
            _ if chunk_type <= 0x7f => return Err(S2Error::Unsupported),
            _ => {
                if chunk_len > MAX_CHUNK_SIZE {
                    return Err(S2Error::Unsupported);
                }
            }
        }
    }
}

/// Removes outer framing header/trailer from an encoded index, returning `None` on malformed inputs.
pub fn remove_index_headers(b: &[u8]) -> Option<&[u8]> {
    let min_chunk_len = S2_INDEX_HEADER.len() + S2_INDEX_TRAILER.len() + 4;
    let save = SKIPPABLE_FRAME_HEADER + min_chunk_len;
    if b.len() <= save || b[0] != CHUNK_TYPE_INDEX {
        return None;
    }
    let chunk_len = (b[1] as usize) | ((b[2] as usize) << 8) | ((b[3] as usize) << 16);
    let body = &b[4..];
    if body.len() < chunk_len || chunk_len < min_chunk_len {
        return None;
    }
    let chunk = &body[..chunk_len];
    let after_hdr = chunk.strip_prefix(S2_INDEX_HEADER)?;
    let inner = after_hdr.strip_suffix(S2_INDEX_TRAILER)?;
    if inner.len() < 4 {
        return None;
    }
    Some(&inner[..inner.len() - 4])
}

/// Restores outer framing headers/trailers previously removed by `remove_index_headers`.
pub fn restore_index_headers(input: &[u8]) -> Vec<u8> {
    if input.is_empty() {
        return Vec::new();
    }
    let mut b = Vec::with_capacity(4 + S2_INDEX_HEADER.len() + input.len() + S2_INDEX_TRAILER.len() + 4);
    b.extend_from_slice(&[CHUNK_TYPE_INDEX, 0, 0, 0]);
    b.extend_from_slice(S2_INDEX_HEADER);
    b.extend_from_slice(input);
    let total = (b.len() + 4 + S2_INDEX_TRAILER.len()) as u32;
    b.extend_from_slice(&total.to_le_bytes());
    b.extend_from_slice(S2_INDEX_TRAILER);
    let chunk_len = b.len() - SKIPPABLE_FRAME_HEADER;
    b[1] = (chunk_len & 0xff) as u8;
    b[2] = ((chunk_len >> 8) & 0xff) as u8;
    b[3] = ((chunk_len >> 16) & 0xff) as u8;
    b
}
