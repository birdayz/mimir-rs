//! Prometheus `EncXOR2` float sample chunk codec with optional start timestamp (ST) support.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/xor2.go.

use super::bstream::{BitReader, BitStream};
use super::st::{
    read_st_header, write_header_first_st_change_on, write_header_first_st_known,
    CHUNK_ST_HEADER_SIZE, MAX_FIRST_ST_CHANGE_ON,
};
use super::varbit::{put_varbit_int, read_varbit_int};
use super::{is_stale_nan, stale_nan, ChunkError, Encoding};

const CHUNK_HEADER_SIZE: usize = 2;
const XOR2_HEADER_SIZE: usize = CHUNK_HEADER_SIZE + CHUNK_ST_HEADER_SIZE;
const CHUNK_ALLOCATION_SIZE: usize = 128;
const CHUNK_COMPACT_CAPACITY_THRESHOLD: usize = 32;

/// Prometheus `EncXOR2` (`4`) encoded float64 chunk with optional start timestamp support.
#[derive(Clone, Debug)]
pub struct Xor2Chunk {
    b: BitStream,
}

impl PartialEq for Xor2Chunk {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for Xor2Chunk {}

impl Default for Xor2Chunk {
    fn default() -> Self {
        Self::new()
    }
}

impl Xor2Chunk {
    pub fn new() -> Self {
        Self {
            b: BitStream::with_header(XOR2_HEADER_SIZE, CHUNK_ALLOCATION_SIZE),
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            b: BitStream::from_bytes(bytes, 0),
        }
    }

    #[inline]
    pub const fn encoding(&self) -> Encoding {
        Encoding::Xor2
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

    pub fn appender(&mut self) -> Result<Xor2Appender<'_>, ChunkError> {
        if self.b.bytes().len() == XOR2_HEADER_SIZE {
            return Ok(Xor2Appender {
                b: &mut self.b,
                st: 0,
                t: i64::MIN,
                v: 0.0,
                t_delta: 0,
                st_diff: 0,
                leading: 0xff,
                trailing: 0,
                num_total: 0,
                first_st_change_on: 0,
                first_st_known: false,
            });
        }

        let (
            st,
            t,
            baseline_v,
            t_delta,
            st_diff,
            leading,
            trailing,
            num_total,
            first_st_known,
            first_st_change_on,
            valid_bits,
        ) = {
            let mut it = self.iter();
            while it.next_sample()?.is_some() {}
            (
                it.st,
                it.t,
                it.baseline_v,
                it.t_delta,
                it.st_diff,
                it.leading,
                it.trailing,
                it.num_total,
                it.first_st_known,
                it.first_st_change_on,
                it.br.valid_bits(),
            )
        };

        self.b.count = valid_bits;
        Ok(Xor2Appender {
            b: &mut self.b,
            st,
            t,
            v: baseline_v,
            t_delta,
            st_diff,
            leading,
            trailing,
            num_total,
            first_st_change_on: u16::from(first_st_change_on),
            first_st_known,
        })
    }

    #[inline]
    pub fn iter(&self) -> Xor2Iterator<'_> {
        Xor2Iterator::new(self.b.bytes())
    }
}

/// Appender for [`Xor2Chunk`].
pub struct Xor2Appender<'a> {
    b: &'a mut BitStream,
    st: i64,
    t: i64,
    v: f64,
    t_delta: u64,
    st_diff: i64,
    leading: u8,
    trailing: u8,
    num_total: u16,
    first_st_change_on: u16,
    first_st_known: bool,
}

impl<'a> Xor2Appender<'a> {
    pub fn append(&mut self, st: i64, t: i64, v: f64) -> Result<(), ChunkError> {
        let mut t_delta = 0u64;
        let mut st_diff = 0i64;

        match self.num_total {
            0 => {
                self.b.write_varint(t);
                self.b.write_bits(v.to_bits(), 64);

                if st != 0 {
                    self.b.write_varint(t.wrapping_sub(st));
                    self.first_st_known = true;
                    write_header_first_st_known(&mut self.b.bytes_mut()[CHUNK_HEADER_SIZE..]);
                }
            }
            1 => {
                t_delta = t.wrapping_sub(self.t) as u64;
                self.b.write_uvarint(t_delta);
                self.write_v_delta(v);

                if st != self.st {
                    st_diff = self.t.wrapping_sub(st);
                    self.first_st_change_on = 1;
                    write_header_first_st_change_on(
                        &mut self.b.bytes_mut()[CHUNK_HEADER_SIZE..],
                        1,
                    );
                    put_varbit_int(self.b, st_diff);
                }
            }
            u16::MAX => return Err(ChunkError::CapacityExceeded),
            _ => {
                t_delta = t.wrapping_sub(self.t) as u64;
                let dod = t_delta.wrapping_sub(self.t_delta) as i64;

                if self.first_st_change_on == 0
                    && st == self.st
                    && self.num_total != MAX_FIRST_ST_CHANGE_ON
                {
                    let vbits = v.to_bits();
                    if dod == 0 && vbits == self.v.to_bits() {
                        self.b.write_bit(false);
                    } else if (-(1i64 << 12)..=(1i64 << 12) - 1).contains(&dod)
                        && vbits == self.v.to_bits()
                    {
                        self.b
                            .write_byte(0b110_00000 | (((dod as u64) >> 8) as u8 & 0x1F));
                        self.b.write_byte(dod as u8);
                        self.b.write_bit(false);
                    } else {
                        self.encode_joint(dod, v);
                        if !is_stale_nan(v) {
                            self.v = v;
                        }
                    }
                    self.t = t;
                    self.t_delta = t_delta;
                    self.num_total += 1;
                    let be = self.num_total.to_be_bytes();
                    let hdr = self.b.bytes_mut();
                    hdr[0] = be[0];
                    hdr[1] = be[1];
                    return Ok(());
                }

                if self.first_st_change_on > 0 {
                    let new_st_diff = self.t.wrapping_sub(st);
                    let delta_st_diff = new_st_diff.wrapping_sub(self.st_diff);
                    let vbits = v.to_bits();
                    if dod == 0 && vbits == self.v.to_bits() {
                        match delta_st_diff {
                            0 => {
                                self.b.write_bit(false);
                                self.b.write_bit(false);
                            }
                            -3..=4 => {
                                self.b
                                    .write_bits((0b10 << 3) | ((delta_st_diff as u64) & 0x7), 6);
                            }
                            -31..=32 => {
                                self.b
                                    .write_bits((0b110 << 6) | ((delta_st_diff as u64) & 0x3F), 10);
                            }
                            -255..=256 => {
                                self.b.write_bits(
                                    (0b1110 << 9) | ((delta_st_diff as u64) & 0x1FF),
                                    14,
                                );
                            }
                            _ => {
                                self.b.write_bit(false);
                                put_varbit_int(self.b, delta_st_diff);
                            }
                        }
                    } else if (-(1i64 << 12)..=(1i64 << 12) - 1).contains(&dod)
                        && vbits == self.v.to_bits()
                    {
                        self.b
                            .write_byte(0b110_00000 | (((dod as u64) >> 8) as u8 & 0x1F));
                        self.b.write_byte(dod as u8);
                        match delta_st_diff {
                            0 => {
                                self.b.write_bit(false);
                                self.b.write_bit(false);
                            }
                            -3..=4 => {
                                self.b
                                    .write_bits((0b10 << 3) | ((delta_st_diff as u64) & 0x7), 6);
                            }
                            -31..=32 => {
                                self.b
                                    .write_bits((0b110 << 6) | ((delta_st_diff as u64) & 0x3F), 10);
                            }
                            -255..=256 => {
                                self.b.write_bits(
                                    (0b1110 << 9) | ((delta_st_diff as u64) & 0x1FF),
                                    14,
                                );
                            }
                            _ => {
                                self.b.write_bit(false);
                                put_varbit_int(self.b, delta_st_diff);
                            }
                        }
                    } else {
                        self.encode_joint(dod, v);
                        if !is_stale_nan(v) {
                            self.v = v;
                        }
                        match delta_st_diff {
                            0 => self.b.write_bit(false),
                            -3..=4 => {
                                self.b
                                    .write_bits((0b10 << 3) | ((delta_st_diff as u64) & 0x7), 5);
                            }
                            -31..=32 => {
                                self.b
                                    .write_bits((0b110 << 6) | ((delta_st_diff as u64) & 0x3F), 9);
                            }
                            -255..=256 => {
                                self.b.write_bits(
                                    (0b1110 << 9) | ((delta_st_diff as u64) & 0x1FF),
                                    13,
                                );
                            }
                            _ => put_varbit_int(self.b, delta_st_diff),
                        }
                    }
                    self.st_diff = new_st_diff;
                    self.st = st;
                    self.t = t;
                    self.t_delta = t_delta;
                    self.num_total += 1;
                    let be = self.num_total.to_be_bytes();
                    let hdr = self.b.bytes_mut();
                    hdr[0] = be[0];
                    hdr[1] = be[1];
                    return Ok(());
                }

                // Full slow path: first_st_change_on == 0 and ST may be initialised here.
                self.encode_joint(dod, v);
                if st != self.st || self.num_total == MAX_FIRST_ST_CHANGE_ON {
                    st_diff = self.t.wrapping_sub(st);
                    self.first_st_change_on = self.num_total;
                    write_header_first_st_change_on(
                        &mut self.b.bytes_mut()[CHUNK_HEADER_SIZE..],
                        self.num_total,
                    );
                    put_varbit_int(self.b, st_diff);
                }
            }
        }

        self.st = st;
        self.t = t;
        if !is_stale_nan(v) {
            self.v = v;
        }
        self.t_delta = t_delta;
        self.st_diff = st_diff;
        self.num_total += 1;
        let be = self.num_total.to_be_bytes();
        let hdr = self.b.bytes_mut();
        hdr[0] = be[0];
        hdr[1] = be[1];
        Ok(())
    }

    fn encode_joint(&mut self, dod: i64, v: f64) {
        if dod == 0 {
            if is_stale_nan(v) {
                self.b.write_bits(0b11111, 5);
                return;
            }
            let vbits = v.to_bits() ^ self.v.to_bits();
            if vbits == 0 {
                self.b.write_bit(false);
                return;
            }
            self.b.write_bits(0b10, 2);
            self.write_v_delta_known_non_zero(vbits);
            return;
        }

        if (-(1i64 << 12)..=(1i64 << 12) - 1).contains(&dod) {
            self.b
                .write_byte(0b110_00000 | (((dod as u64) >> 8) as u8 & 0x1F));
            self.b.write_byte(dod as u8);
        } else if (-(1i64 << 19)..=(1i64 << 19) - 1).contains(&dod) {
            self.b
                .write_byte(0b1110_0000 | (((dod as u64) >> 16) as u8 & 0x0F));
            self.b.write_byte(((dod as u64) >> 8) as u8);
            self.b.write_byte(dod as u8);
        } else {
            self.b.write_bits(0b11110, 5);
            self.b.write_bits(dod as u64, 64);
        }

        if v.to_bits() == self.v.to_bits() {
            self.b.write_bit(false);
        } else {
            self.write_v_delta(v);
        }
    }

    fn write_v_delta(&mut self, v: f64) {
        if is_stale_nan(v) {
            self.b.write_bits(0b111, 3);
            return;
        }

        let delta = v.to_bits() ^ self.v.to_bits();
        if delta == 0 {
            self.b.write_bit(false);
            return;
        }

        let mut new_leading = delta.leading_zeros() as u8;
        let mut new_trailing = delta.trailing_zeros() as u8;
        if new_leading >= 32 {
            new_leading = 31;
        }

        if self.leading != 0xff
            && new_leading >= self.leading
            && new_trailing >= self.trailing
            && new_leading + new_trailing <= self.leading + self.trailing + 12
        {
            self.b.write_bits(0b10, 2);
            self.b.write_bits(
                delta >> self.trailing,
                64 - (self.leading as usize) - (self.trailing as usize),
            );
            return;
        }

        if self.leading != 0xff && (new_leading < self.leading || new_trailing < self.trailing) {
            let union_leading = new_leading.min(self.leading);
            let union_trailing = new_trailing.min(self.trailing);
            if new_leading + new_trailing - union_leading - union_trailing <= 6 {
                new_leading = union_leading;
                new_trailing = union_trailing;
            }
        }

        self.leading = new_leading;
        self.trailing = new_trailing;

        self.b.write_bits(0b110, 3);
        self.b.write_bits(u64::from(new_leading), 5);
        let sigbits = 64 - new_leading - new_trailing;
        self.b.write_bits(u64::from(sigbits & 0x3f), 6);
        self.b.write_bits(delta >> new_trailing, sigbits as usize);
    }

    fn write_v_delta_known_non_zero(&mut self, delta: u64) {
        let mut new_leading = delta.leading_zeros() as u8;
        let mut new_trailing = delta.trailing_zeros() as u8;
        if new_leading >= 32 {
            new_leading = 31;
        }

        if self.leading != 0xff
            && new_leading >= self.leading
            && new_trailing >= self.trailing
            && new_leading + new_trailing <= self.leading + self.trailing + 11
        {
            self.b.write_bit(false);
            self.b.write_bits(
                delta >> self.trailing,
                64 - (self.leading as usize) - (self.trailing as usize),
            );
            return;
        }

        if self.leading != 0xff && (new_leading < self.leading || new_trailing < self.trailing) {
            let union_leading = new_leading.min(self.leading);
            let union_trailing = new_trailing.min(self.trailing);
            if new_leading + new_trailing - union_leading - union_trailing <= 5 {
                new_leading = union_leading;
                new_trailing = union_trailing;
            }
        }

        self.leading = new_leading;
        self.trailing = new_trailing;

        self.b.write_bit(true);
        self.b.write_bits(u64::from(new_leading), 5);
        let sigbits = 64 - new_leading - new_trailing;
        self.b.write_bits(u64::from(sigbits & 0x3f), 6);
        self.b.write_bits(delta >> new_trailing, sigbits as usize);
    }
}

/// Iterator over samples in an [`Xor2Chunk`].
#[derive(Clone, Debug)]
pub struct Xor2Iterator<'a> {
    br: BitReader<'a>,
    num_total: u16,
    num_read: u16,
    first_st_known: bool,
    first_st_change_on: u8,
    leading: u8,
    trailing: u8,
    st: i64,
    t: i64,
    val: f64,
    t_delta: u64,
    st_diff: i64,
    baseline_v: f64,
}

impl<'a> Xor2Iterator<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        if b.len() < XOR2_HEADER_SIZE {
            return Self {
                br: BitReader::new(&[]),
                num_total: 0,
                num_read: 0,
                first_st_known: false,
                first_st_change_on: 0,
                leading: 0,
                trailing: 0,
                st: 0,
                t: 0,
                val: 0.0,
                t_delta: 0,
                st_diff: 0,
                baseline_v: 0.0,
            };
        }
        let num_total = u16::from_be_bytes([b[0], b[1]]);
        let (first_st_known, first_st_change_on) = read_st_header(&b[CHUNK_HEADER_SIZE..]);
        Self {
            br: BitReader::new(&b[XOR2_HEADER_SIZE..]),
            num_total,
            num_read: 0,
            first_st_known,
            first_st_change_on,
            leading: 0,
            trailing: 0,
            st: 0,
            t: 0,
            val: 0.0,
            t_delta: 0,
            st_diff: 0,
            baseline_v: 0.0,
        }
    }

    #[inline]
    pub fn at(&self) -> (i64, f64) {
        (self.t, self.val)
    }

    #[inline]
    pub fn at_st(&self) -> i64 {
        self.st
    }

    fn read_xor2_control(&mut self) -> Result<u8, ChunkError> {
        if !self.br.read_bit()? {
            return Ok(0);
        }
        if !self.br.read_bit()? {
            return Ok(1);
        }
        if !self.br.read_bit()? {
            return Ok(2);
        }
        if !self.br.read_bit()? {
            return Ok(3);
        }
        if !self.br.read_bit()? {
            Ok(4)
        } else {
            Ok(5)
        }
    }

    fn read_dod(&mut self, w: u8) -> Result<(), ChunkError> {
        let mut b = self.br.read_bits(w)?;
        if w < 64 && b >= (1u64 << (w - 1)) {
            b = b.wrapping_sub(1u64 << w);
        }
        self.t_delta = (self.t_delta as i64).wrapping_add(b as i64) as u64;
        self.t = self.t.wrapping_add(self.t_delta as i64);
        Ok(())
    }

    fn decode_new_leading_trailing(&mut self) -> Result<(), ChunkError> {
        let nl = self.br.read_bits(5)? as u8;
        let mut sigbits = self.br.read_bits(6)? as u8;
        if sigbits == 0 {
            sigbits = 64;
        }
        self.leading = nl;
        self.trailing = 64 - nl - sigbits;
        let value_bits = self.br.read_bits(sigbits)?;
        let vbits = self.baseline_v.to_bits() ^ (value_bits << self.trailing);
        self.val = f64::from_bits(vbits);
        self.baseline_v = self.val;
        Ok(())
    }

    fn decode_value(&mut self) -> Result<(), ChunkError> {
        if !self.br.read_bit()? {
            self.val = self.baseline_v;
            return Ok(());
        }
        if !self.br.read_bit()? {
            let sz = 64 - self.leading - self.trailing;
            let value_bits = self.br.read_bits(sz)?;
            let vbits = self.baseline_v.to_bits() ^ (value_bits << self.trailing);
            self.val = f64::from_bits(vbits);
            self.baseline_v = self.val;
            return Ok(());
        }
        if !self.br.read_bit()? {
            return self.decode_new_leading_trailing();
        }
        self.val = stale_nan();
        Ok(())
    }

    fn decode_value_known_non_zero(&mut self) -> Result<(), ChunkError> {
        if !self.br.read_bit()? {
            let sz = 64 - self.leading - self.trailing;
            let value_bits = self.br.read_bits(sz)?;
            let vbits = self.baseline_v.to_bits() ^ (value_bits << self.trailing);
            self.val = f64::from_bits(vbits);
            self.baseline_v = self.val;
            return Ok(());
        }
        self.decode_new_leading_trailing()
    }

    pub fn next_sample(&mut self) -> Result<Option<(i64, f64)>, ChunkError> {
        if self.num_read == self.num_total {
            return Ok(None);
        }

        if self.num_read == 0 {
            let t = self.br.read_varint()?;
            let v_bits = self.br.read_bits(64)?;
            self.t = t;
            self.val = f64::from_bits(v_bits);
            if !is_stale_nan(self.val) {
                self.baseline_v = self.val;
            }
            if self.first_st_known {
                let st_diff = self.br.read_varint()?;
                self.st_diff = st_diff;
                self.st = t.wrapping_sub(st_diff);
            }
            self.num_read += 1;
            return Ok(Some((self.t, self.val)));
        }

        if self.num_read == 1 {
            let t_delta = self.br.read_uvarint()?;
            let prev_t = self.t;
            self.t_delta = t_delta;
            self.t = self.t.wrapping_add(t_delta as i64);
            self.decode_value()?;
            if self.first_st_change_on == 1 {
                let sdod = read_varbit_int(&mut self.br)?;
                self.st_diff = sdod;
                self.st = prev_t.wrapping_sub(sdod);
            }
            self.num_read += 1;
            return Ok(Some((self.t, self.val)));
        }

        let prev_t = self.t;
        let saved_num_read = self.num_read;
        let ctrl = self.read_xor2_control()?;

        match ctrl {
            0 => {
                self.t = self.t.wrapping_add(self.t_delta as i64);
                self.val = self.baseline_v;
            }
            1 => {
                self.t = self.t.wrapping_add(self.t_delta as i64);
                self.decode_value_known_non_zero()?;
            }
            2 => {
                self.read_dod(13)?;
                self.decode_value()?;
            }
            3 => {
                self.read_dod(20)?;
                self.decode_value()?;
            }
            4 => {
                self.read_dod(64)?;
                self.decode_value()?;
            }
            _ => {
                self.t = self.t.wrapping_add(self.t_delta as i64);
                self.val = stale_nan();
            }
        }

        if self.first_st_change_on > 0 && saved_num_read >= u16::from(self.first_st_change_on) {
            let sdod = read_varbit_int(&mut self.br)?;
            if saved_num_read == u16::from(self.first_st_change_on) {
                self.st_diff = sdod;
            } else {
                self.st_diff = self.st_diff.wrapping_add(sdod);
            }
            self.st = prev_t.wrapping_sub(self.st_diff);
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xor2_roundtrip_without_and_with_st() {
        let mut c = Xor2Chunk::new();
        {
            let mut app = c.appender().unwrap();
            app.append(0, 1000, 42.0).unwrap();
            app.append(0, 2000, 42.0).unwrap();
            app.append(0, 3000, 43.5).unwrap();
            app.append(0, 4500, 100.25).unwrap();
            app.append(0, 450000, stale_nan()).unwrap();
            app.append(0, 460000, 100.25).unwrap();
        }
        assert_eq!(c.num_samples(), 6);
        assert_eq!(c.as_bytes()[2], 0);
        let mut it = c.iter();
        let mut out = Vec::new();
        while let Some((t, v)) = it.next_sample().unwrap() {
            out.push((it.at_st(), t, v.to_bits()));
        }
        assert_eq!(
            out,
            vec![
                (0, 1000, 42.0f64.to_bits()),
                (0, 2000, 42.0f64.to_bits()),
                (0, 3000, 43.5f64.to_bits()),
                (0, 4500, 100.25f64.to_bits()),
                (0, 450000, stale_nan().to_bits()),
                (0, 460000, 100.25f64.to_bits()),
            ]
        );

        let mut c2 = Xor2Chunk::new();
        {
            let mut app = c2.appender().unwrap();
            app.append(100, 1000, 1.0).unwrap();
            app.append(100, 2000, 2.0).unwrap();
            app.append(500, 3000, 2.0).unwrap();
            app.append(500, 4000, 3.0).unwrap();
        }
        let mut it2 = c2.iter();
        let mut out2 = Vec::new();
        while let Some((t, v)) = it2.next_sample().unwrap() {
            out2.push((it2.at_st(), t, v));
        }
        assert_eq!(
            out2,
            vec![
                (100, 1000, 1.0),
                (100, 2000, 2.0),
                (500, 3000, 2.0),
                (500, 4000, 3.0),
            ]
        );
    }
}
