//! Prometheus `EncXOR` float sample chunk codec.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/xor.go.

use super::bstream::{BitReader, BitStream};
use super::varbit::bit_range;
use super::{ChunkError, Encoding};

const CHUNK_HEADER_SIZE: usize = 2;
const CHUNK_ALLOCATION_SIZE: usize = 128;
const CHUNK_COMPACT_CAPACITY_THRESHOLD: usize = 32;

/// Gorilla/Prometheus XOR-encoded float64 chunk (`EncXOR = 1`).
#[derive(Clone, Debug)]
pub struct XorChunk {
    b: BitStream,
}

impl PartialEq for XorChunk {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for XorChunk {}

impl Default for XorChunk {
    fn default() -> Self {
        Self::new()
    }
}

impl XorChunk {
    pub fn new() -> Self {
        Self {
            b: BitStream::with_header(CHUNK_HEADER_SIZE, CHUNK_ALLOCATION_SIZE),
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            b: BitStream::from_bytes(bytes, 0),
        }
    }

    #[inline]
    pub const fn encoding(&self) -> Encoding {
        Encoding::Xor
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.b.bytes()
    }

    #[inline]
    pub fn into_bytes(self) -> Vec<u8> {
        self.b.into_bytes()
    }

    #[inline]
    pub fn num_samples(&self) -> usize {
        let bytes = self.b.bytes();
        if bytes.len() < CHUNK_HEADER_SIZE {
            return 0;
        }
        u16::from_be_bytes([bytes[0], bytes[1]]) as usize
    }

    #[inline]
    pub fn compact(&mut self) {
        self.b.compact(CHUNK_COMPACT_CAPACITY_THRESHOLD);
    }

    pub fn appender(&mut self) -> Result<XorAppender<'_>, ChunkError> {
        if self.b.bytes().len() == CHUNK_HEADER_SIZE {
            return Ok(XorAppender {
                b: &mut self.b,
                t: i64::MIN,
                v: 0.0,
                t_delta: 0,
                leading: 0xff,
                trailing: 0,
            });
        }

        let (t, v, t_delta, leading, trailing, valid_bits) = {
            let mut it = self.iter();
            while it.next_sample()?.is_some() {}
            (
                it.t,
                it.val,
                it.t_delta,
                it.leading,
                it.trailing,
                it.br.valid_bits(),
            )
        };

        self.b.count = valid_bits;
        Ok(XorAppender {
            b: &mut self.b,
            t,
            v,
            t_delta,
            leading,
            trailing,
        })
    }

    #[inline]
    pub fn iter(&self) -> XorIterator<'_> {
        XorIterator::new(self.b.bytes())
    }
}

/// Appender for [`XorChunk`].
pub struct XorAppender<'a> {
    b: &'a mut BitStream,
    t: i64,
    v: f64,
    t_delta: u64,
    leading: u8,
    trailing: u8,
}

impl<'a> XorAppender<'a> {
    /// Appends a `(t, v)` sample (`st` is ignored in `EncXOR`).
    pub fn append(&mut self, _st: i64, t: i64, v: f64) -> Result<(), ChunkError> {
        let mut t_delta = 0u64;
        let num = {
            let hdr = self.b.bytes();
            u16::from_be_bytes([hdr[0], hdr[1]])
        };

        match num {
            0 => {
                self.b.write_varint(t);
                self.b.write_bits(v.to_bits(), 64);
            }
            1 => {
                t_delta = (t.wrapping_sub(self.t)) as u64;
                self.b.write_uvarint(t_delta);
                xor_write(self.b, v, self.v, &mut self.leading, &mut self.trailing);
            }
            u16::MAX => return Err(ChunkError::CapacityExceeded),
            _ => {
                t_delta = (t.wrapping_sub(self.t)) as u64;
                let dod = t_delta.wrapping_sub(self.t_delta) as i64;

                if dod == 0 {
                    self.b.write_bit(false);
                } else if bit_range(dod, 14) {
                    self.b
                        .write_byte((0b10 << 6) | (((dod >> 8) as u8) & ((1 << 6) - 1)));
                    self.b.write_byte(dod as u8);
                } else if bit_range(dod, 17) {
                    self.b.write_bits(0b110, 3);
                    self.b.write_bits(dod as u64, 17);
                } else if bit_range(dod, 20) {
                    self.b.write_bits(0b1110, 4);
                    self.b.write_bits(dod as u64, 20);
                } else {
                    self.b.write_bits(0b1111, 4);
                    self.b.write_bits(dod as u64, 64);
                }

                xor_write(self.b, v, self.v, &mut self.leading, &mut self.trailing);
            }
        }

        self.t = t;
        self.v = v;
        let next_num = (num + 1).to_be_bytes();
        let hdr = self.b.bytes_mut();
        hdr[0] = next_num[0];
        hdr[1] = next_num[1];
        self.t_delta = t_delta;
        Ok(())
    }
}

/// Iterator over samples in an [`XorChunk`].
#[derive(Clone, Debug)]
pub struct XorIterator<'a> {
    br: BitReader<'a>,
    num_total: u16,
    num_read: u16,
    leading: u8,
    trailing: u8,
    t: i64,
    val: f64,
    t_delta: u64,
}

impl<'a> XorIterator<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        if b.len() < CHUNK_HEADER_SIZE {
            return Self {
                br: BitReader::new(&[]),
                num_total: 0,
                num_read: 0,
                leading: 0,
                trailing: 0,
                t: i64::MIN,
                val: 0.0,
                t_delta: 0,
            };
        }
        let num_total = u16::from_be_bytes([b[0], b[1]]);
        Self {
            br: BitReader::new(&b[CHUNK_HEADER_SIZE..]),
            num_total,
            num_read: 0,
            leading: 0,
            trailing: 0,
            t: i64::MIN,
            val: 0.0,
            t_delta: 0,
        }
    }

    #[inline]
    pub fn at(&self) -> (i64, f64) {
        (self.t, self.val)
    }

    #[inline]
    pub fn next_sample(&mut self) -> Result<Option<(i64, f64)>, ChunkError> {
        if self.num_read == self.num_total {
            return Ok(None);
        }

        if self.num_read == 0 {
            let t = self.br.read_varint()?;
            let v_bits = self.br.read_bits(64)?;
            self.t = t;
            self.val = f64::from_bits(v_bits);
            self.num_read += 1;
            return Ok(Some((self.t, self.val)));
        }

        if self.num_read == 1 {
            let t_delta = self.br.read_uvarint()?;
            self.t_delta = t_delta;
            self.t = self.t.wrapping_add(t_delta as i64);
            return self.read_value();
        }

        if let Some(false) = self.br.peek_bit() {
            self.br.consume_bit_unchecked();
            self.t = self.t.wrapping_add(self.t_delta as i64);

            if let Some(false) = self.br.peek_bit() {
                self.br.consume_bit_unchecked();
                self.num_read += 1;
                return Ok(Some((self.t, self.val)));
            }
            return self.read_value();
        }

        let mut d: u8 = 0;
        for _ in 0..4 {
            d <<= 1;
            let bit = self.br.read_bit()?;
            if !bit {
                break;
            }
            d |= 1;
        }

        let mut sz: u8 = 0;
        let mut dod: i64 = 0;
        match d {
            0b0 => {}
            0b10 => sz = 14,
            0b110 => sz = 17,
            0b1110 => sz = 20,
            0b1111 => {
                dod = self.br.read_bits(64)? as i64;
            }
            _ => unreachable!("4-bit prefix only yields 0, 10, 110, 1110, 1111"),
        }

        if sz != 0 {
            let mut bits = self.br.read_bits(sz)?;
            if bits > (1u64 << (sz - 1)) {
                bits = bits.wrapping_sub(1u64 << sz);
            }
            dod = bits as i64;
        }

        self.t_delta = (self.t_delta as i64).wrapping_add(dod) as u64;
        self.t = self.t.wrapping_add(self.t_delta as i64);
        self.read_value()
    }

    #[inline]
    fn read_value(&mut self) -> Result<Option<(i64, f64)>, ChunkError> {
        if let Some(false) = self.br.peek_bit() {
            self.br.consume_bit_unchecked();
            self.num_read += 1;
            return Ok(Some((self.t, self.val)));
        }

        if self.leading != 0xff {
            let mbits = 64 - self.leading - self.trailing;
            if let Some(ctrl) = self.br.peek_bits_fast(2) {
                if (ctrl & 1) == 0 {
                    // Control bits `10`: reuse leading/trailing.
                    if let Some(bits) = self.br.peek_bits_fast(2 + mbits) {
                        self.br.consume_bits_unchecked(2 + mbits);
                        let masked = bits & if mbits >= 64 { u64::MAX } else { (1u64 << mbits) - 1 };
                        let vbits = self.val.to_bits() ^ (masked << self.trailing);
                        self.val = f64::from_bits(vbits);
                        self.num_read += 1;
                        return Ok(Some((self.t, self.val)));
                    }
                }
            }
        }

        xor_read(
            &mut self.br,
            &mut self.val,
            &mut self.leading,
            &mut self.trailing,
        )?;
        self.num_read += 1;
        Ok(Some((self.t, self.val)))
    }

    pub fn seek(&mut self, t: i64) -> Result<Option<(i64, f64)>, ChunkError> {
        while t > self.t || self.num_read == 0 {
            if self.next_sample()?.is_none() {
                return Ok(None);
            }
        }
        Ok(Some((self.t, self.val)))
    }
}

#[inline]
pub(crate) fn xor_write(
    b: &mut BitStream,
    new_value: f64,
    current_value: f64,
    leading: &mut u8,
    trailing: &mut u8,
) {
    let delta = new_value.to_bits() ^ current_value.to_bits();
    if delta == 0 {
        b.write_bit(false);
        return;
    }
    b.write_bit(true);

    let mut new_leading = delta.leading_zeros() as u8;
    let new_trailing = delta.trailing_zeros() as u8;
    if new_leading >= 32 {
        new_leading = 31;
    }

    if *leading != 0xff && new_leading >= *leading && new_trailing >= *trailing {
        b.write_bit(false);
        b.write_bits(delta >> *trailing, 64 - (*leading as usize) - (*trailing as usize));
        return;
    }

    *leading = new_leading;
    *trailing = new_trailing;

    b.write_bit(true);
    b.write_bits(u64::from(new_leading), 5);
    let sigbits = 64 - new_leading - new_trailing;
    // Note: if new_leading == 0 && new_trailing == 0, sigbits == 64, whose lower 6 bits are 0.
    b.write_bits(u64::from(sigbits & 0x3f), 6);
    b.write_bits(delta >> new_trailing, sigbits as usize);
}

#[inline]
pub(crate) fn xor_read(
    br: &mut BitReader<'_>,
    value: &mut f64,
    leading: &mut u8,
    trailing: &mut u8,
) -> Result<(), ChunkError> {
    if !br.read_bit()? {
        return Ok(());
    }

    let (_new_leading, new_trailing, mbits) = if !br.read_bit()? {
        (*leading, *trailing, 64 - *leading - *trailing)
    } else {
        let nl = br.read_bits(5)? as u8;
        let mut mb = br.read_bits(6)? as u8;
        if mb == 0 {
            mb = 64;
        }
        let nt = 64 - nl - mb;
        *leading = nl;
        *trailing = nt;
        (nl, nt, mb)
    };

    let bits = br.read_bits(mbits)?;
    let vbits = value.to_bits() ^ (bits << new_trailing);
    *value = f64::from_bits(vbits);
    Ok(())
}
