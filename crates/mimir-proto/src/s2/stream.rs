//! Ported from github.com/klauspost/compress/s2/{writer,reader}.go.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use super::block::{
    decode_block_uvarint, encode_block, encode_block_best, encode_block_better,
    encode_block_snappy, max_encoded_len, put_uvarint, s2_decode,
};
use super::index::Index;
use super::{
    masked_crc32c, S2Error, CHECKSUM_SIZE, CHUNK_TYPE_COMPRESSED, CHUNK_TYPE_PADDING,
    CHUNK_TYPE_STREAM_IDENTIFIER, CHUNK_TYPE_UNCOMPRESSED, DEFAULT_BLOCK_SIZE, MAGIC_S2,
    MAGIC_SNAPPY, MAGIC_STREAM_S2, MAGIC_STREAM_SNAPPY, MAX_BLOCK_SIZE, MAX_CHUNK_SIZE,
    MAX_SNAPPY_BLOCK_SIZE, MIN_BLOCK_SIZE, OBUF_HEADER_LEN, SKIPPABLE_FRAME_HEADER,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterLevel {
    Uncompressed,
    Fast,
    Better,
    Best,
}

pub type CustomEncoderFn = Arc<dyn Fn(&mut [u8], &[u8]) -> isize + Send + Sync>;
pub type BufferDoneFn = Box<dyn FnMut(Vec<u8>) + Send>;

pub struct WriterOptions {
    pub block_size: usize,
    pub level: WriterLevel,
    pub concurrency: usize,
    pub add_index: bool,
    pub padding: usize,
    pub padding_src: Option<Box<dyn Read + Send>>,
    pub snappy_compat: bool,
    pub flush_on_write: bool,
    pub custom_encoder: Option<CustomEncoderFn>,
    pub buffer_done: Option<BufferDoneFn>,
}

impl Default for WriterOptions {
    fn default() -> Self {
        static CACHED_CPUS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        let cpus = *CACHED_CPUS.get_or_init(|| {
            std::thread::available_parallelism().map_or(1, |n| n.get())
        });
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            level: WriterLevel::Fast,
            concurrency: cpus,
            add_index: false,
            padding: 0,
            padding_src: None,
            snappy_compat: false,
            flush_on_write: false,
            custom_encoder: None,
            buffer_done: None,
        }
    }
}

pub struct Writer<W: Write> {
    writer: Option<W>,
    opts: WriterOptions,
    ibuf: Vec<u8>,
    written: i64,
    uncomp_written: i64,
    wrote_stream_header: bool,
    closed: bool,
    index: Index,
    err: Option<String>,
}

impl<W: Write> Writer<W> {
    pub fn new(w: W) -> Self {
        Self::with_options(w, WriterOptions::default())
    }

    pub fn with_options(w: W, mut opts: WriterOptions) -> Self {
        let mut err = None;
        if opts.snappy_compat && opts.block_size > MAX_SNAPPY_BLOCK_SIZE {
            opts.block_size = MAX_SNAPPY_BLOCK_SIZE;
        }
        if opts.block_size < MIN_BLOCK_SIZE || opts.block_size > MAX_BLOCK_SIZE {
            err = Some("s2: invalid WriterBlockSize".to_string());
        }
        if opts.concurrency == 0 {
            opts.concurrency = 1;
        }
        if opts.padding > MAX_CHUNK_SIZE {
            err = Some("s2: invalid WriterPadding".to_string());
        }
        let mut index = Index::new();
        index.reset(opts.block_size);
        let cap = opts.block_size;
        Self {
            writer: Some(w),
            opts,
            ibuf: Vec::with_capacity(cap),
            written: 0,
            uncomp_written: 0,
            wrote_stream_header: false,
            closed: false,
            index,
            err,
        }
    }

    pub fn pad(&self) -> usize {
        self.opts.padding
    }

    pub fn reset(&mut self, w: W) -> Option<W> {
        self.ibuf.clear();
        self.written = 0;
        self.uncomp_written = 0;
        self.wrote_stream_header = false;
        self.closed = false;
        self.err = None;
        self.index.reset(self.opts.block_size);
        self.writer.replace(w)
    }

    pub fn into_inner(mut self) -> Option<W> {
        self.writer.take()
    }

    fn ensure_stream_header(&mut self) -> io::Result<()> {
        if let Some(ref e) = self.err {
            return Err(io::Error::other(e.clone()));
        }
        if !self.wrote_stream_header {
            self.wrote_stream_header = true;
            let magic = if self.opts.snappy_compat {
                MAGIC_STREAM_SNAPPY
            } else {
                MAGIC_STREAM_S2
            };
            if let Some(w) = self.writer.as_mut() {
                w.write_all(magic)?;
                self.written += magic.len() as i64;
            }
        }
        Ok(())
    }

    pub fn add_skippable_block(&mut self, id: u8, data: &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if !(0x80..=CHUNK_TYPE_PADDING).contains(&id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid skippable block id {id:x}"),
            ));
        }
        if data.len() > MAX_CHUNK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "skippable block exceeded maximum size",
            ));
        }
        self.ensure_stream_header()?;
        let mut chunk = Vec::with_capacity(4 + data.len());
        chunk.push(id);
        chunk.push((data.len() & 0xff) as u8);
        chunk.push(((data.len() >> 8) & 0xff) as u8);
        chunk.push(((data.len() >> 16) & 0xff) as u8);
        chunk.extend_from_slice(data);
        if let Some(w) = self.writer.as_mut() {
            w.write_all(&chunk)?;
            self.written += chunk.len() as i64;
        }
        Ok(())
    }

    fn encode_one_chunk(
        uncompressed: &[u8],
        level: WriterLevel,
        snappy_compat: bool,
        custom: Option<&CustomEncoderFn>,
    ) -> Vec<u8> {
        let checksum = masked_crc32c(uncompressed);
        let obuf_cap = OBUF_HEADER_LEN + max_encoded_len(uncompressed.len() as i64).max(0) as usize;
        let mut obuf = vec![0u8; obuf_cap];
        let hdr_n = put_uvarint(&mut obuf[OBUF_HEADER_LEN..], uncompressed.len() as u64);

        let n2 = if let Some(c) = custom {
            let ret = c(&mut obuf[OBUF_HEADER_LEN + hdr_n..], uncompressed);
            if ret >= 0 {
                ret as usize
            } else {
                Self::encode_with_level(
                    &mut obuf[OBUF_HEADER_LEN + hdr_n..],
                    uncompressed,
                    level,
                    snappy_compat,
                )
            }
        } else {
            Self::encode_with_level(
                &mut obuf[OBUF_HEADER_LEN + hdr_n..],
                uncompressed,
                level,
                snappy_compat,
            )
        };

        let (chunk_type, chunk_len, total_len) = if n2 > 0 && n2 + hdr_n < uncompressed.len() {
            (
                CHUNK_TYPE_COMPRESSED,
                4 + hdr_n + n2,
                OBUF_HEADER_LEN + hdr_n + n2,
            )
        } else {
            obuf[OBUF_HEADER_LEN..OBUF_HEADER_LEN + uncompressed.len()]
                .copy_from_slice(uncompressed);
            (
                CHUNK_TYPE_UNCOMPRESSED,
                4 + uncompressed.len(),
                OBUF_HEADER_LEN + uncompressed.len(),
            )
        };

        obuf[0] = chunk_type;
        obuf[1] = (chunk_len & 0xff) as u8;
        obuf[2] = ((chunk_len >> 8) & 0xff) as u8;
        obuf[3] = ((chunk_len >> 16) & 0xff) as u8;
        obuf[4..8].copy_from_slice(&checksum.to_le_bytes());
        obuf.truncate(total_len);
        obuf
    }

    fn encode_with_level(
        dst: &mut [u8],
        uncompressed: &[u8],
        level: WriterLevel,
        snappy_compat: bool,
    ) -> usize {
        if snappy_compat {
            return match level {
                WriterLevel::Uncompressed => 0,
                WriterLevel::Fast => encode_block_snappy(dst, uncompressed),
                WriterLevel::Better | WriterLevel::Best => {
                    super::block::encode_block_better_snappy(dst, uncompressed)
                }
            };
        }
        match level {
            WriterLevel::Uncompressed => 0,
            WriterLevel::Fast => encode_block(dst, uncompressed),
            WriterLevel::Better => encode_block_better(dst, uncompressed),
            WriterLevel::Best => encode_block_best(dst, uncompressed, None),
        }
    }

    fn write_blocks(&mut self, data: &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.ensure_stream_header()?;
        let block_size = self.opts.block_size;
        let level = self.opts.level;
        let snappy = self.opts.snappy_compat;
        let custom = self.opts.custom_encoder.as_ref();
        let concurrency = self.opts.concurrency.max(1);

        let chunks: Vec<&[u8]> = data.chunks(block_size).collect();
        let encoded_chunks: Vec<(usize, Vec<u8>)> = if concurrency > 1 && chunks.len() > 1 {
            std::thread::scope(|scope| {
                let mut handles = Vec::with_capacity(chunks.len());
                for ch in &chunks {
                    let ch = *ch;
                    handles.push(scope.spawn(move || {
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            (ch.len(), Self::encode_one_chunk(ch, level, snappy, custom))
                        }))
                    }));
                }
                handles
                    .into_iter()
                    .map(|h| match h.join() {
                        Ok(Ok(res)) => Ok(res),
                        Ok(Err(_)) | Err(_) => Err(io::Error::other("s2 worker thread panicked")),
                    })
                    .collect::<io::Result<Vec<_>>>()
            })?
        } else {
            chunks
                .iter()
                .map(|&ch| (ch.len(), Self::encode_one_chunk(ch, level, snappy, custom)))
                .collect()
        };

        for (uncomp_len, enc) in encoded_chunks {
            let start_off = self.uncomp_written;
            self.uncomp_written += uncomp_len as i64;
            if let Some(w) = self.writer.as_mut() {
                w.write_all(&enc)?;
            }
            let _ = self.index.add(self.written, start_off);
            self.written += enc.len() as i64;
        }
        Ok(())
    }

    pub fn encode_buffer(&mut self, buf: &[u8]) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::other("s2: writer closed"));
        }
        if !self.ibuf.is_empty() {
            let pending = std::mem::take(&mut self.ibuf);
            self.write_blocks(&pending)?;
        }
        self.write_blocks(buf)
    }

    pub fn encode_buffer_owned(&mut self, buf: Vec<u8>) -> io::Result<()> {
        self.encode_buffer(&buf)?;
        if let Some(ref mut cb) = self.opts.buffer_done {
            cb(buf);
        }
        Ok(())
    }

    pub fn read_from<R: Read>(&mut self, mut r: R) -> io::Result<u64> {
        if self.closed {
            return Err(io::Error::other("s2: writer closed"));
        }
        let mut total = 0u64;
        let block_size = self.opts.block_size;
        let mut tmp = vec![0u8; block_size];
        loop {
            let mut filled = 0usize;
            while filled < block_size {
                match r.read(&mut tmp[filled..]) {
                    Ok(0) => break,
                    Ok(n) => filled += n,
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            }
            if filled == 0 {
                break;
            }
            total += filled as u64;
            self.write_all(&tmp[..filled])?;
        }
        Ok(total)
    }

    pub fn close_index(&mut self) -> io::Result<Vec<u8>> {
        self.close_internal(true)
    }

    pub fn close(&mut self) -> io::Result<()> {
        let _ = self.close_internal(self.opts.add_index)?;
        Ok(())
    }

    fn close_internal(&mut self, build_index: bool) -> io::Result<Vec<u8>> {
        if self.closed {
            return Ok(Vec::new());
        }
        self.flush()?;
        let mut idx_bytes = Vec::new();
        if build_index {
            let comp_size = if self.opts.padding <= 1 {
                self.written
            } else {
                -1
            };
            self.index
                .append_to(&mut idx_bytes, comp_size, self.uncomp_written);
            if self.opts.add_index {
                self.written += idx_bytes.len() as i64;
            }
        }
        if self.opts.padding > 1 {
            let add = calc_skippable_frame(self.written, self.opts.padding as i64);
            if add > 0 {
                let mut frame = Vec::with_capacity(add);
                let payload_len = add - SKIPPABLE_FRAME_HEADER;
                frame.push(CHUNK_TYPE_PADDING);
                frame.push((payload_len & 0xff) as u8);
                frame.push(((payload_len >> 8) & 0xff) as u8);
                frame.push(((payload_len >> 16) & 0xff) as u8);
                let start = frame.len();
                frame.resize(start + payload_len, 0);
                if let Some(ref mut src) = self.opts.padding_src {
                    src.read_exact(&mut frame[start..])?;
                }
                if let Some(w) = self.writer.as_mut() {
                    w.write_all(&frame)?;
                }
                self.written += frame.len() as i64;
            }
        }
        if !idx_bytes.is_empty() && self.opts.add_index {
            if let Some(w) = self.writer.as_mut() {
                w.write_all(&idx_bytes)?;
            }
        }
        self.closed = true;
        Ok(idx_bytes)
    }
}

impl<W: Write> Write for Writer<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.closed {
            return Err(io::Error::other("s2: writer closed"));
        }
        if let Some(ref e) = self.err {
            return Err(io::Error::other(e.clone()));
        }
        if self.opts.flush_on_write {
            self.write_blocks(buf)?;
            return Ok(buf.len());
        }
        let block_size = self.opts.block_size;
        let mut p = buf;
        while !p.is_empty() {
            if self.ibuf.is_empty() && p.len() >= block_size {
                let full_len = (p.len() / block_size) * block_size;
                self.write_blocks(&p[..full_len])?;
                p = &p[full_len..];
                break;
            }
            let space = block_size - self.ibuf.len();
            let take = space.min(p.len());
            self.ibuf.extend_from_slice(&p[..take]);
            p = &p[take..];
            if self.ibuf.len() >= block_size {
                let pending = std::mem::take(&mut self.ibuf);
                self.write_blocks(&pending)?;
            }
        }
        if !p.is_empty() {
            self.ibuf.extend_from_slice(p);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(ref e) = self.err {
            return Err(io::Error::other(e.clone()));
        }
        if !self.ibuf.is_empty() {
            let pending = std::mem::take(&mut self.ibuf);
            self.write_blocks(&pending)?;
        }
        if let Some(w) = self.writer.as_mut() {
            w.flush()?;
        }
        Ok(())
    }
}

fn calc_skippable_frame(written: i64, want_multiple: i64) -> usize {
    assert!(want_multiple > 0);
    assert!(written >= 0);
    let left_over = written % want_multiple;
    if left_over == 0 {
        return 0;
    }
    let mut to_add = want_multiple - left_over;
    while to_add < SKIPPABLE_FRAME_HEADER as i64 {
        to_add += want_multiple;
    }
    to_add as usize
}

// ---------------------------------------------------------------------------
// Reader & ReadSeeker
// ---------------------------------------------------------------------------

pub type SkippableCallback = Box<dyn FnMut(&[u8]) -> Result<(), S2Error> + Send>;

pub struct ReaderOptions {
    pub max_block_size: usize,
    pub alloc_block: usize,
    pub ignore_stream_identifier: bool,
    pub ignore_crc: bool,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            max_block_size: MAX_BLOCK_SIZE,
            alloc_block: 0,
            ignore_stream_identifier: false,
            ignore_crc: false,
        }
    }
}

pub struct Reader<R: Read> {
    r: R,
    opts: ReaderOptions,
    buf: Vec<u8>,
    decoded: Vec<u8>,
    i: usize,
    j: usize,
    block_start: i64,
    read_header: bool,
    snappy_frame: bool,
    eof: bool,
    err: Option<S2Error>,
    index: Option<Index>,
    skippable_cbs: Vec<Option<SkippableCallback>>,
}

impl<R: Read> Reader<R> {
    pub fn new(r: R) -> Self {
        Self::with_options(r, ReaderOptions::default())
    }

    pub fn with_options(r: R, mut opts: ReaderOptions) -> Self {
        let mut init_err = None;
        if opts.max_block_size == 0 || opts.max_block_size > MAX_BLOCK_SIZE {
            opts.max_block_size = MAX_BLOCK_SIZE;
        }
        if opts.alloc_block > opts.max_block_size {
            init_err = Some(S2Error::Unsupported);
        }
        let decoded = if opts.alloc_block > 0 {
            vec![0u8; opts.alloc_block]
        } else {
            Vec::new()
        };
        let mut cbs = Vec::with_capacity(126);
        cbs.resize_with(126, || None);
        Self {
            r,
            read_header: opts.ignore_stream_identifier,
            opts,
            buf: Vec::new(),
            decoded,
            i: 0,
            j: 0,
            block_start: 0,
            snappy_frame: false,
            eof: false,
            err: init_err,
            index: None,
            skippable_cbs: cbs,
        }
    }

    pub fn reset(&mut self, r: R) {
        self.r = r;
        self.i = 0;
        self.j = 0;
        self.block_start = 0;
        self.read_header = self.opts.ignore_stream_identifier;
        self.snappy_frame = false;
        self.eof = false;
        self.err = None;
        self.index = None;
    }

    pub fn skippable_cb(&mut self, id: u8, cb: SkippableCallback) -> Result<(), String> {
        if !(0x80..CHUNK_TYPE_PADDING).contains(&id) {
            return Err("ReaderSkippableCB: invalid id".to_string());
        }
        self.skippable_cbs[(id - 0x80) as usize] = Some(cb);
        Ok(())
    }

    fn read_exact_or_eof(&mut self, dst: &mut [u8], allow_eof_at_start: bool) -> Result<bool, S2Error> {
        let mut pos = 0;
        while pos < dst.len() {
            match self.r.read(&mut dst[pos..]) {
                Ok(0) => {
                    if pos == 0 && allow_eof_at_start {
                        self.eof = true;
                        return Ok(false);
                    }
                    let e = S2Error::Corrupt;
                    self.err = Some(e.clone());
                    return Err(e);
                }
                Ok(n) => pos += n,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    let se = S2Error::Io(e.kind());
                    self.err = Some(se.clone());
                    return Err(se);
                }
            }
        }
        Ok(true)
    }

    fn handle_skippable(&mut self, id: u8, chunk_len: usize) -> Result<(), S2Error> {
        if chunk_len > MAX_CHUNK_SIZE {
            self.err = Some(S2Error::Unsupported);
            return Err(S2Error::Unsupported);
        }
        self.buf.resize(chunk_len, 0);
        let mut tmp = std::mem::take(&mut self.buf);
        let res = self.read_exact_or_eof(&mut tmp, false);
        self.buf = tmp;
        res?;
        if (0x80..CHUNK_TYPE_PADDING).contains(&id) {
            if let Some(ref mut cb) = self.skippable_cbs[(id - 0x80) as usize] {
                cb(&self.buf[..chunk_len])?;
            }
        }
        Ok(())
    }

    fn next_block(&mut self) -> Result<bool, S2Error> {
        if let Some(ref e) = self.err {
            return Err(e.clone());
        }
        if self.eof {
            return Ok(false);
        }
        let mut hdr = [0u8; 4];
        loop {
            if !self.read_exact_or_eof(&mut hdr, true)? {
                return Ok(false);
            }
            let chunk_type = hdr[0];
            if !self.read_header {
                if chunk_type != CHUNK_TYPE_STREAM_IDENTIFIER {
                    self.err = Some(S2Error::Corrupt);
                    return Err(S2Error::Corrupt);
                }
                self.read_header = true;
            }
            let chunk_len =
                (hdr[1] as usize) | ((hdr[2] as usize) << 8) | ((hdr[3] as usize) << 16);

            match chunk_type {
                CHUNK_TYPE_COMPRESSED => {
                    self.block_start += self.j as i64;
                    if chunk_len < CHECKSUM_SIZE {
                        self.err = Some(S2Error::Corrupt);
                        return Err(S2Error::Corrupt);
                    }
                    self.buf.resize(chunk_len, 0);
                    let mut tmp = std::mem::take(&mut self.buf);
                    let res = self.read_exact_or_eof(&mut tmp, false);
                    self.buf = tmp;
                    res?;
                    let checksum =
                        u32::from_le_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
                    let block = &self.buf[CHECKSUM_SIZE..chunk_len];
                    let (n, var_len) = decode_block_uvarint(block).inspect_err(|e| {
                        self.err = Some(e.clone());
                    })?;
                    if (self.snappy_frame && n > MAX_SNAPPY_BLOCK_SIZE)
                        || n > self.opts.max_block_size
                    {
                        self.err = Some(S2Error::Corrupt);
                        return Err(S2Error::Corrupt);
                    }
                    if self.decoded.len() < n {
                        self.decoded.resize(n, 0);
                    }
                    if let Err(e) = s2_decode(&mut self.decoded[..n], &block[var_len..]) {
                        self.err = Some(e.clone());
                        return Err(e);
                    }
                    if !self.opts.ignore_crc && masked_crc32c(&self.decoded[..n]) != checksum {
                        self.err = Some(S2Error::CRC);
                        return Err(S2Error::CRC);
                    }
                    self.i = 0;
                    self.j = n;
                    return Ok(true);
                }
                CHUNK_TYPE_UNCOMPRESSED => {
                    self.block_start += self.j as i64;
                    if chunk_len < CHECKSUM_SIZE {
                        self.err = Some(S2Error::Corrupt);
                        return Err(S2Error::Corrupt);
                    }
                    let mut csum_buf = [0u8; 4];
                    self.read_exact_or_eof(&mut csum_buf, false)?;
                    let checksum = u32::from_le_bytes(csum_buf);
                    let n = chunk_len - CHECKSUM_SIZE;
                    if (self.snappy_frame && n > MAX_SNAPPY_BLOCK_SIZE)
                        || n > self.opts.max_block_size
                    {
                        self.err = Some(S2Error::Corrupt);
                        return Err(S2Error::Corrupt);
                    }
                    if self.decoded.len() < n {
                        self.decoded.resize(n, 0);
                    }
                    let mut dec = std::mem::take(&mut self.decoded);
                    let res = self.read_exact_or_eof(&mut dec[..n], false);
                    self.decoded = dec;
                    res?;
                    if !self.opts.ignore_crc && masked_crc32c(&self.decoded[..n]) != checksum {
                        self.err = Some(S2Error::CRC);
                        return Err(S2Error::CRC);
                    }
                    self.i = 0;
                    self.j = n;
                    return Ok(true);
                }
                CHUNK_TYPE_STREAM_IDENTIFIER => {
                    if chunk_len != MAGIC_S2.len() {
                        self.err = Some(S2Error::Corrupt);
                        return Err(S2Error::Corrupt);
                    }
                    let mut magic = [0u8; 6];
                    self.read_exact_or_eof(&mut magic, false)?;
                    if &magic == MAGIC_S2 {
                        self.snappy_frame = false;
                    } else if &magic == MAGIC_SNAPPY {
                        self.snappy_frame = true;
                    } else {
                        self.err = Some(S2Error::Corrupt);
                        return Err(S2Error::Corrupt);
                    }
                }
                _ if chunk_type <= 0x7f => {
                    self.err = Some(S2Error::Unsupported);
                    return Err(S2Error::Unsupported);
                }
                _ => {
                    self.handle_skippable(chunk_type, chunk_len)?;
                }
            }
        }
    }

    pub fn skip(&mut self, mut n: i64) -> Result<(), S2Error> {
        if n < 0 {
            return Err(S2Error::Corrupt);
        }
        if let Some(ref e) = self.err {
            return Err(e.clone());
        }
        let mut hdr = [0u8; 4];
        while n > 0 {
            if self.i < self.j {
                let left = (self.j - self.i) as i64;
                if left >= n {
                    self.i += n as usize;
                    return Ok(());
                }
                n -= left;
                self.i = self.j;
            }

            if !self.read_exact_or_eof(&mut hdr, true)? {
                self.err = Some(S2Error::Io(io::ErrorKind::UnexpectedEof));
                return Err(S2Error::Io(io::ErrorKind::UnexpectedEof));
            }
            let chunk_type = hdr[0];
            if !self.read_header {
                if chunk_type != CHUNK_TYPE_STREAM_IDENTIFIER {
                    self.err = Some(S2Error::Corrupt);
                    return Err(S2Error::Corrupt);
                }
                self.read_header = true;
            }
            let chunk_len =
                (hdr[1] as usize) | ((hdr[2] as usize) << 8) | ((hdr[3] as usize) << 16);

            match chunk_type {
                CHUNK_TYPE_COMPRESSED => {
                    self.block_start += self.j as i64;
                    if chunk_len < CHECKSUM_SIZE {
                        self.err = Some(S2Error::Corrupt);
                        return Err(S2Error::Corrupt);
                    }
                    self.buf.resize(chunk_len, 0);
                    let mut tmp = std::mem::take(&mut self.buf);
                    let res = self.read_exact_or_eof(&mut tmp, false);
                    self.buf = tmp;
                    res?;
                    let checksum =
                        u32::from_le_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
                    let block = &self.buf[CHECKSUM_SIZE..chunk_len];
                    let (d_len, var_len) = decode_block_uvarint(block).inspect_err(|e| {
                        self.err = Some(e.clone());
                    })?;
                    if d_len > self.opts.max_block_size
                        || (self.snappy_frame && d_len > MAX_SNAPPY_BLOCK_SIZE)
                    {
                        self.err = Some(S2Error::Corrupt);
                        return Err(S2Error::Corrupt);
                    }
                    if (d_len as i64) > n {
                        if self.decoded.len() < d_len {
                            self.decoded.resize(d_len, 0);
                        }
                        if let Err(e) = s2_decode(&mut self.decoded[..d_len], &block[var_len..]) {
                            self.err = Some(e.clone());
                            return Err(e);
                        }
                        if !self.opts.ignore_crc
                            && masked_crc32c(&self.decoded[..d_len]) != checksum
                        {
                            self.err = Some(S2Error::CRC);
                            return Err(S2Error::CRC);
                        }
                        self.i = 0;
                        self.j = d_len;
                    } else {
                        n -= d_len as i64;
                        self.block_start += d_len as i64;
                        self.i = 0;
                        self.j = 0;
                    }
                }
                CHUNK_TYPE_UNCOMPRESSED => {
                    self.block_start += self.j as i64;
                    if chunk_len < CHECKSUM_SIZE {
                        self.err = Some(S2Error::Corrupt);
                        return Err(S2Error::Corrupt);
                    }
                    let mut csum_buf = [0u8; 4];
                    self.read_exact_or_eof(&mut csum_buf, false)?;
                    let checksum = u32::from_le_bytes(csum_buf);
                    let n2 = chunk_len - CHECKSUM_SIZE;
                    if n2 > self.opts.max_block_size
                        || (self.snappy_frame && n2 > MAX_SNAPPY_BLOCK_SIZE)
                    {
                        self.err = Some(S2Error::Corrupt);
                        return Err(S2Error::Corrupt);
                    }
                    if self.decoded.len() < n2 {
                        self.decoded.resize(n2, 0);
                    }
                    let mut dec = std::mem::take(&mut self.decoded);
                    let res = self.read_exact_or_eof(&mut dec[..n2], false);
                    self.decoded = dec;
                    res?;
                    if !self.opts.ignore_crc && masked_crc32c(&self.decoded[..n2]) != checksum {
                        self.err = Some(S2Error::CRC);
                        return Err(S2Error::CRC);
                    }
                    self.i = 0;
                    self.j = n2;
                }
                CHUNK_TYPE_STREAM_IDENTIFIER => {
                    if chunk_len != MAGIC_S2.len() {
                        self.err = Some(S2Error::Corrupt);
                        return Err(S2Error::Corrupt);
                    }
                    let mut magic = [0u8; 6];
                    self.read_exact_or_eof(&mut magic, false)?;
                    if &magic == MAGIC_S2 {
                        self.snappy_frame = false;
                    } else if &magic == MAGIC_SNAPPY {
                        self.snappy_frame = true;
                    } else {
                        self.err = Some(S2Error::Corrupt);
                        return Err(S2Error::Corrupt);
                    }
                }
                _ if chunk_type <= 0x7f => {
                    self.err = Some(S2Error::Unsupported);
                    return Err(S2Error::Unsupported);
                }
                _ => {
                    self.handle_skippable(chunk_type, chunk_len)?;
                }
            }
        }
        Ok(())
    }

    pub fn read_byte(&mut self) -> io::Result<u8> {
        if self.i < self.j {
            let b = self.decoded[self.i];
            self.i += 1;
            return Ok(b);
        }
        let mut tmp = [0u8; 1];
        for _ in 0..10 {
            let n = self.read(&mut tmp)?;
            if n == 1 {
                return Ok(tmp[0]);
            }
        }
        Err(io::Error::other("s2: no progress"))
    }

    pub fn decode_concurrent<W: Write>(
        &mut self,
        w: &mut W,
        _concurrent: usize,
    ) -> Result<i64, S2Error> {
        let mut written = 0i64;
        let mut buf = vec![0u8; 64 << 10];
        loop {
            match self.read(&mut buf) {
                Ok(0) => return Ok(written),
                Ok(n) => {
                    w.write_all(&buf[..n])
                        .map_err(|e| S2Error::Io(e.kind()))?;
                    written += n as i64;
                }
                Err(e) => {
                    if let Some(se) = e.get_ref().and_then(|x| x.downcast_ref::<S2Error>()) {
                        return Err(se.clone());
                    }
                    return Err(S2Error::Io(e.kind()));
                }
            }
        }
    }
}

impl<R: Read> Read for Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.i < self.j {
                let n = buf.len().min(self.j - self.i);
                buf[..n].copy_from_slice(&self.decoded[self.i..self.i + n]);
                self.i += n;
                return Ok(n);
            }
            match self.next_block() {
                Ok(true) => {
                    if buf.is_empty() {
                        return Ok(0);
                    }
                    continue;
                }
                Ok(false) => return Ok(0),
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            }
        }
    }
}

impl<R: Read + Seek> Reader<R> {
    pub fn read_seeker(mut self, random: bool, index: Option<&[u8]>) -> Result<ReadSeeker<R>, S2Error> {
        if let Some(idx_bytes) = index {
            if !idx_bytes.is_empty() {
                let mut idx = Index::new();
                idx.load(idx_bytes)?;
                self.index = Some(idx);
                return Ok(ReadSeeker { reader: self });
            }
        }
        let pos = self
            .r
            .stream_position()
            .map_err(|e| S2Error::Io(e.kind()))?;
        let mut idx = Index::new();
        match idx.load_stream(&mut self.r) {
            Ok(()) => {
                self.r
                    .seek(SeekFrom::Start(pos))
                    .map_err(|e| S2Error::Io(e.kind()))?;
                self.index = Some(idx);
                Ok(ReadSeeker { reader: self })
            }
            Err(S2Error::Unsupported) if !random => {
                self.r
                    .seek(SeekFrom::Start(pos))
                    .map_err(|e| S2Error::Io(e.kind()))?;
                self.index = None;
                Ok(ReadSeeker { reader: self })
            }
            Err(e) => Err(e),
        }
    }
}

pub struct ReadSeeker<R: Read + Seek> {
    pub reader: Reader<R>,
}

impl<R: Read + Seek> ReadSeeker<R> {
    pub fn read_at(&mut self, p: &mut [u8], offset: i64) -> io::Result<usize> {
        self.seek(SeekFrom::Start(offset as u64))?;
        let mut n = 0;
        while n < p.len() {
            let n2 = self.read(&mut p[n..])?;
            if n2 == 0 {
                return Ok(n);
            }
            n += n2;
        }
        Ok(n)
    }
}

impl<R: Read + Seek> Read for ReadSeeker<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

impl<R: Read + Seek> Seek for ReadSeeker<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.reader.eof = false;
        self.reader.err = None;
        let abs_offset = match pos {
            SeekFrom::Start(o) => o as i64,
            SeekFrom::Current(o) => self.reader.block_start + self.reader.i as i64 + o,
            SeekFrom::End(o) => {
                let idx = self
                    .reader
                    .index
                    .as_ref()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "no index"))?;
                idx.total_uncompressed + o
            }
        };
        if abs_offset < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before start of file",
            ));
        }
        if !self.reader.read_header {
            let _ = self.reader.read(&mut [])?;
        }
        if abs_offset >= self.reader.block_start
            && abs_offset < self.reader.block_start + self.reader.j as i64
        {
            self.reader.i = (abs_offset - self.reader.block_start) as usize;
            return Ok((self.reader.block_start + self.reader.i as i64) as u64);
        }
        let Some(ref idx) = self.reader.index else {
            let cur = self.reader.block_start + self.reader.i as i64;
            if abs_offset >= cur {
                self.reader
                    .skip(abs_offset - cur)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                return Ok((self.reader.block_start + self.reader.i as i64) as u64);
            }
            return Err(io::Error::new(io::ErrorKind::Unsupported, "cannot seek backwards"));
        };
        let (c, u) = idx
            .find(abs_offset)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.reader.r.seek(SeekFrom::Start(c as u64))?;
        self.reader.i = self.reader.j;
        self.reader.block_start = u - self.reader.j as i64;
        if u < abs_offset {
            self.reader
                .skip(abs_offset - u)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }
        Ok(abs_offset as u64)
    }
}
