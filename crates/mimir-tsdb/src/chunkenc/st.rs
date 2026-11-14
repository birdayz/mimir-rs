//! Start timestamp (ST) chunk header and varbit delta codec for XOR2 and HistogramST chunks.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/st.go.

use super::bstream::{BitReader, BitStream};
use super::varbit::{put_varbit_int, read_varbit_int};
use super::ChunkError;

pub const MAX_FIRST_ST_CHANGE_ON: u16 = 0x7F;
pub const HISTOGRAM_ST_SAMPLE_COUNT_MASK: u16 = 0x3FFF;
pub const CHUNK_ST_HEADER_SIZE: usize = 1;

#[inline]
pub fn write_header_first_st_known(st_hdr: &mut [u8]) {
    st_hdr[0] = 0x80;
}

#[inline]
pub fn write_header_first_st_change_on(st_hdr: &mut [u8], first_st_change_on: u16) {
    if first_st_change_on > MAX_FIRST_ST_CHANGE_ON {
        return;
    }
    st_hdr[0] |= first_st_change_on as u8;
}

#[inline]
pub fn read_st_header(st_hdr: &[u8]) -> (bool, u8) {
    ((st_hdr[0] & 0x80) != 0, st_hdr[0] & 0x7F)
}

#[derive(Clone, Debug, Default)]
pub struct StEncoder {
    pub st: i64,
    pub st_diff: i64,
    pub first_st_change_on: u8,
    pub first_st_known: bool,
}

impl StEncoder {
    /// Writes start timestamp data for histogram/float-histogram sample `num` (1-based after increment)
    /// into `b`, updating the ST header byte at `b.bytes_mut()[st_hdr_pos]`.
    pub fn encode(&mut self, b: &mut BitStream, st_hdr_pos: usize, num: u16, cur_t: i64, prev_t: i64, st: i64) {
        match num {
            1 => {
                if st != 0 {
                    b.write_varint(cur_t.wrapping_sub(st));
                    self.first_st_known = true;
                    write_header_first_st_known(&mut b.bytes_mut()[st_hdr_pos..]);
                }
            }
            2 => {
                if st != self.st {
                    let st_diff = prev_t.wrapping_sub(st);
                    self.first_st_change_on = 1;
                    write_header_first_st_change_on(&mut b.bytes_mut()[st_hdr_pos..], 1);
                    put_varbit_int(b, st_diff);
                    self.st_diff = st_diff;
                }
            }
            _ => {
                if self.first_st_change_on == 0 && st == self.st && num - 1 != MAX_FIRST_ST_CHANGE_ON {
                    // Fast path: no ST data to write.
                } else if self.first_st_change_on == 0 {
                    if st != self.st || num - 1 == MAX_FIRST_ST_CHANGE_ON {
                        let st_diff = prev_t.wrapping_sub(st);
                        self.first_st_change_on = (num - 1) as u8;
                        write_header_first_st_change_on(&mut b.bytes_mut()[st_hdr_pos..], num - 1);
                        put_varbit_int(b, st_diff);
                        self.st_diff = st_diff;
                    }
                } else {
                    let st_diff = prev_t.wrapping_sub(st);
                    put_varbit_int(b, st_diff.wrapping_sub(self.st_diff));
                    self.st_diff = st_diff;
                }
            }
        }
        self.st = st;
    }
}

#[derive(Clone, Debug, Default)]
pub struct StDecoder {
    pub st: i64,
    pub st_diff: i64,
    pub first_st_change_on: u8,
    pub first_st_known: bool,
}

impl StDecoder {
    /// Decodes start timestamp data for sample `num_read` (1-based after increment).
    pub fn decode(
        &mut self,
        br: &mut BitReader<'_>,
        num_read: u16,
        cur_t: i64,
        prev_t: i64,
    ) -> Result<(), ChunkError> {
        match num_read {
            1 => {
                if self.first_st_known {
                    let st_diff = br.read_varint()?;
                    self.st_diff = st_diff;
                    self.st = cur_t.wrapping_sub(st_diff);
                }
            }
            2 => {
                if self.first_st_change_on == 1 {
                    let sdod = read_varbit_int(br)?;
                    self.st_diff = sdod;
                    self.st = prev_t.wrapping_sub(sdod);
                }
            }
            _ => {
                if self.first_st_change_on > 0 && num_read > u16::from(self.first_st_change_on) {
                    let sdod = read_varbit_int(br)?;
                    if num_read - 1 == u16::from(self.first_st_change_on) {
                        self.st_diff = sdod;
                    } else {
                        self.st_diff = self.st_diff.wrapping_add(sdod);
                    }
                    self.st = prev_t.wrapping_sub(self.st_diff);
                }
            }
        }
        Ok(())
    }
}
