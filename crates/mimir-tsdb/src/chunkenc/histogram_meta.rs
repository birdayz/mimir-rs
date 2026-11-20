//! Shared layout serialization and span/bucket expansion helpers for histogram chunks.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/histogram_meta.go.

use super::bstream::{BitReader, BitStream};
use super::histogram_model::{
    is_custom_buckets_schema, CounterResetHeader, CounterResetHint, HistogramSpan,
};
use super::varbit::{put_varbit_int, put_varbit_uint, read_varbit_int, read_varbit_uint};
use super::ChunkError;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Insert {
    pub pos: usize,
    pub num: usize,
    pub bucket_idx: i64,
}

#[inline]
pub(crate) fn count_spans(spans: &[HistogramSpan]) -> usize {
    spans.iter().map(|s| s.length as usize).sum()
}

#[inline]
pub(crate) fn checked_count_spans(spans: &[HistogramSpan]) -> Option<usize> {
    spans
        .iter()
        .try_fold(0usize, |acc, s| acc.checked_add(s.length as usize))
}

pub(crate) fn write_histogram_chunk_layout(
    b: &mut BitStream,
    schema: i32,
    zero_threshold: f64,
    positive_spans: &[HistogramSpan],
    negative_spans: &[HistogramSpan],
    custom_values: &[f64],
) {
    put_zero_threshold(b, zero_threshold);
    put_varbit_int(b, i64::from(schema));
    put_histogram_chunk_layout_spans(b, positive_spans);
    put_histogram_chunk_layout_spans(b, negative_spans);
    if is_custom_buckets_schema(schema) {
        put_histogram_chunk_layout_custom_bounds(b, custom_values);
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HistogramChunkLayout {
    pub schema: i32,
    pub zero_threshold: f64,
    pub positive_spans: Vec<HistogramSpan>,
    pub negative_spans: Vec<HistogramSpan>,
    pub custom_values: Vec<f64>,
}

pub(crate) fn read_histogram_chunk_layout(
    b: &mut BitReader<'_>,
) -> Result<HistogramChunkLayout, ChunkError> {
    let zero_threshold = read_zero_threshold(b)?;
    let schema = read_varbit_int(b)? as i32;
    let positive_spans = read_histogram_chunk_layout_spans(b)?;
    let negative_spans = read_histogram_chunk_layout_spans(b)?;
    let custom_values = if is_custom_buckets_schema(schema) {
        read_histogram_chunk_layout_custom_bounds(b)?
    } else {
        Vec::new()
    };
    Ok(HistogramChunkLayout {
        schema,
        zero_threshold,
        positive_spans,
        negative_spans,
        custom_values,
    })
}

fn put_histogram_chunk_layout_spans(b: &mut BitStream, spans: &[HistogramSpan]) {
    put_varbit_uint(b, spans.len() as u64);
    for s in spans {
        put_varbit_uint(b, u64::from(s.length));
        put_varbit_int(b, i64::from(s.offset));
    }
}

fn read_histogram_chunk_layout_spans(
    b: &mut BitReader<'_>,
) -> Result<Vec<HistogramSpan>, ChunkError> {
    let num_u64 = read_varbit_uint(b)?;
    if num_u64 > (b.remaining_bits() / 4) as u64 {
        return Err(ChunkError::UnexpectedEof);
    }
    let num = usize::try_from(num_u64).map_err(|_| ChunkError::InvalidChunk)?;
    let mut spans = Vec::with_capacity(num.min(4096));
    for _ in 0..num {
        let length = read_varbit_uint(b)? as u32;
        let offset = read_varbit_int(b)? as i32;
        spans.push(HistogramSpan { offset, length });
    }
    Ok(spans)
}

fn put_histogram_chunk_layout_custom_bounds(b: &mut BitStream, custom_values: &[f64]) {
    put_varbit_uint(b, custom_values.len() as u64);
    for &bound in custom_values {
        put_custom_bound(b, bound);
    }
}

fn read_histogram_chunk_layout_custom_bounds(
    b: &mut BitReader<'_>,
) -> Result<Vec<f64>, ChunkError> {
    let num_u64 = read_varbit_uint(b)?;
    if num_u64 > (b.remaining_bits() / 2) as u64 {
        return Err(ChunkError::UnexpectedEof);
    }
    let num = usize::try_from(num_u64).map_err(|_| ChunkError::InvalidChunk)?;
    let mut custom_values = Vec::with_capacity(num.min(4096));
    for _ in 0..num {
        custom_values.push(read_custom_bound(b)?);
    }
    Ok(custom_values)
}

/// Encodes `zero_threshold` matching Go `putZeroThreshold` (`math.Frexp` exact power-of-2 check).
pub(crate) fn put_zero_threshold(b: &mut BitStream, threshold: f64) {
    if threshold == 0.0 {
        b.write_byte(0);
        return;
    }
    let bits = threshold.to_bits();
    let sign = bits >> 63;
    let biased_exp = ((bits >> 52) & 0x7ff) as i32;
    let mantissa = bits & ((1u64 << 52) - 1);
    // In IEEE-754, positive normal power-of-2 has sign=0, mantissa=0, 1 <= biased_exp <= 2046,
    // and Go math.Frexp gives frac == 0.5, exp == biased_exp - 1022.
    // Range -242 <= exp <= 11 corresponds to 780 <= biased_exp <= 1033.
    if sign != 0 || mantissa != 0 || !(780..=1033).contains(&biased_exp) {
        b.write_byte(255);
        b.write_bits(bits, 64);
        return;
    }
    let exp = biased_exp - 1022;
    b.write_byte((exp + 243) as u8);
}

pub(crate) fn read_zero_threshold(br: &mut BitReader<'_>) -> Result<f64, ChunkError> {
    let b = br.read_byte()?;
    match b {
        0 => Ok(0.0),
        255 => {
            let v = br.read_bits(64)?;
            Ok(f64::from_bits(v))
        }
        other => {
            // math.Ldexp(0.5, int(b) - 243) => biased_exp = (b - 243) + 1022 = b + 779
            let biased_exp = u64::from(other) + 779;
            Ok(f64::from_bits(biased_exp << 52))
        }
    }
}

#[inline]
fn is_whole_when_multiplied(input: f64) -> bool {
    let i = (input * 1000.0).round() as u64;
    let out = (i as f64) / 1000.0;
    input == out
}

pub(crate) fn put_custom_bound(b: &mut BitStream, f: f64) {
    let tf = f * 1000.0;
    if !(0.0..=33_554_430.0).contains(&tf) || !is_whole_when_multiplied(f) {
        b.write_bit(false);
        b.write_bits(f.to_bits(), 64);
        return;
    }
    put_varbit_uint(b, (tf.round() as u64) + 1);
}

pub(crate) fn read_custom_bound(br: &mut BitReader<'_>) -> Result<f64, ChunkError> {
    let v = read_varbit_uint(br)?;
    if v == 0 {
        let bits = br.read_bits(64)?;
        Ok(f64::from_bits(bits))
    } else {
        Ok(((v - 1) as f64) / 1000.0)
    }
}

pub(crate) struct BucketIterator<'a> {
    spans: &'a [HistogramSpan],
    span: usize,
    bucket: isize,
    idx: i64,
}

impl<'a> BucketIterator<'a> {
    pub fn new(spans: &'a [HistogramSpan]) -> Self {
        let mut idx = -1i64;
        if let Some(first) = spans.first() {
            idx = idx.wrapping_add(i64::from(first.offset));
        }
        Self {
            spans,
            span: 0,
            bucket: -1,
            idx,
        }
    }

    pub fn next_idx(&mut self) -> Option<i64> {
        if self.span >= self.spans.len() {
            return None;
        }
        if self.bucket < (self.spans[self.span].length as isize) - 1 {
            self.bucket += 1;
            self.idx = self.idx.wrapping_add(1);
            return Some(self.idx);
        }
        while self.span + 1 < self.spans.len() {
            self.span += 1;
            self.idx = self
                .idx
                .wrapping_add(i64::from(self.spans[self.span].offset))
                .wrapping_add(1);
            self.bucket = 0;
            if self.spans[self.span].length == 0 {
                self.idx = self.idx.wrapping_sub(1);
                continue;
            }
            return Some(self.idx);
        }
        None
    }
}

pub(crate) fn expand_spans_both_ways(
    a: &[HistogramSpan],
    b: &[HistogramSpan],
) -> Option<(Vec<Insert>, Vec<Insert>, Vec<HistogramSpan>)> {
    let mut ai = BucketIterator::new(a);
    let mut bi = BucketIterator::new(b);

    let mut f_inserts = Vec::new();
    let mut b_inserts = Vec::new();
    let mut merged_spans: Vec<HistogramSpan> = Vec::new();
    let mut last_bucket = 0i64;
    let mut overflow = false;

    let mut add_bucket = |bucket: i64| {
        if overflow {
            return;
        }
        let mut offset = bucket - last_bucket - 1;
        if offset == 0 && !merged_spans.is_empty() {
            merged_spans.last_mut().unwrap().length += 1;
        } else {
            if merged_spans.is_empty() {
                offset += 1;
            }
            match i32::try_from(offset) {
                Ok(off32) => merged_spans.push(HistogramSpan {
                    offset: off32,
                    length: 1,
                }),
                Err(_) => {
                    overflow = true;
                    return;
                }
            }
        }
        last_bucket = bucket;
    };

    let mut f_inter = Insert::default();
    let mut b_inter = Insert::default();

    let mut av = ai.next_idx();
    let mut bv = bi.next_idx();

    loop {
        match (av, bv) {
            (Some(a_val), Some(b_val)) => {
                if a_val == b_val {
                    if f_inter.num > 0 {
                        f_inserts.push(f_inter);
                        f_inter.num = 0;
                    }
                    if b_inter.num > 0 {
                        b_inserts.push(b_inter);
                        b_inter.num = 0;
                    }
                    add_bucket(a_val);
                    av = ai.next_idx();
                    bv = bi.next_idx();
                    f_inter.pos += 1;
                    b_inter.pos += 1;
                } else if a_val < b_val {
                    b_inter.num += 1;
                    if f_inter.num > 0 {
                        f_inserts.push(f_inter);
                        f_inter.num = 0;
                    }
                    add_bucket(a_val);
                    f_inter.pos += 1;
                    av = ai.next_idx();
                } else {
                    f_inter.num += 1;
                    if b_inter.num > 0 {
                        b_inserts.push(b_inter);
                        b_inter.num = 0;
                    }
                    add_bucket(b_val);
                    b_inter.pos += 1;
                    bv = bi.next_idx();
                }
            }
            (Some(a_val), None) => {
                b_inter.num += 1;
                add_bucket(a_val);
                av = ai.next_idx();
            }
            (None, Some(b_val)) => {
                f_inter.num += 1;
                add_bucket(b_val);
                bv = bi.next_idx();
            }
            (None, None) => {
                if f_inter.num > 0 {
                    f_inserts.push(f_inter);
                }
                if b_inter.num > 0 {
                    b_inserts.push(b_inter);
                }
                break;
            }
        }
    }

    if overflow {
        return None;
    }
    Some((f_inserts, b_inserts, merged_spans))
}

pub(crate) trait BucketNum: Copy + Default + std::ops::AddAssign + std::ops::Neg<Output = Self> + std::ops::Add<Output = Self> {}
impl BucketNum for i64 {}
impl BucketNum for f64 {}

pub(crate) fn insert_buckets<BV: BucketNum>(
    in_vals: &[BV],
    out_len: usize,
    inserts: &[Insert],
    deltas: bool,
) -> Vec<BV> {
    let mut out = vec![BV::default(); out_len];
    let mut oi = 0usize;
    let mut v = BV::default();
    let mut ii = 0usize;

    for (i, &d) in in_vals.iter().enumerate() {
        if ii >= inserts.len() || i != inserts[ii].pos {
            out[oi] = d;
            oi += 1;
            v += d;
            continue;
        }
        let mut first_insert = true;
        while ii < inserts.len() && i == inserts[ii].pos {
            if deltas && first_insert {
                out[oi] = -v;
                first_insert = false;
            } else {
                out[oi] = BV::default();
            }
            oi += 1;
            for _ in 1..inserts[ii].num {
                out[oi] = BV::default();
                oi += 1;
            }
            ii += 1;
        }
        if deltas {
            out[oi] = d + v;
        } else {
            out[oi] = d;
        }
        oi += 1;
        v += d;
    }

    while ii < inserts.len() {
        assert!(
            inserts[ii].pos >= in_vals.len(),
            "leftover inserts must be after current buckets"
        );
        if deltas {
            out[oi] = -v;
        } else {
            out[oi] = BV::default();
        }
        oi += 1;
        for _ in 1..inserts[ii].num {
            out[oi] = BV::default();
            oi += 1;
        }
        ii += 1;
        v = BV::default();
    }

    out
}

#[inline]
pub(crate) fn counter_reset_hint_from_header(
    crh: CounterResetHeader,
    num_read: u16,
) -> CounterResetHint {
    if crh == CounterResetHeader::GaugeType {
        CounterResetHint::GaugeType
    } else if num_read > 1 {
        CounterResetHint::NotCounterReset
    } else {
        CounterResetHint::UnknownCounterReset
    }
}

pub(crate) fn adjust_for_inserts(
    spans: &[HistogramSpan],
    inserts: &[Insert],
) -> Option<Vec<HistogramSpan>> {
    if inserts.is_empty() {
        return Some(spans.to_vec());
    }
    let mut it = BucketIterator::new(spans);
    let mut merged_spans: Vec<HistogramSpan> = Vec::new();

    let mut last_bucket = 0i64;
    let mut i = 0usize;
    let mut insert_idx = inserts[0].bucket_idx;
    let mut insert_num = inserts[0].num;
    let mut overflow = false;

    let mut add_bucket = |b: i64| {
        if overflow {
            return;
        }
        let mut offset = b - last_bucket - 1;
        if offset == 0 && !merged_spans.is_empty() {
            merged_spans.last_mut().unwrap().length += 1;
        } else {
            if merged_spans.is_empty() {
                offset += 1;
            }
            match i32::try_from(offset) {
                Ok(off32) => merged_spans.push(HistogramSpan {
                    offset: off32,
                    length: 1,
                }),
                Err(_) => {
                    overflow = true;
                    return;
                }
            }
        }
        last_bucket = b;
    };

    let mut bucket = it.next_idx();
    while let Some(b_val) = bucket {
        if i < inserts.len() && insert_idx < b_val {
            add_bucket(insert_idx);
            insert_num -= 1;
            if insert_num == 0 {
                i += 1;
                if i < inserts.len() {
                    insert_idx = inserts[i].bucket_idx;
                    insert_num = inserts[i].num;
                }
            } else {
                insert_idx += 1;
            }
        } else {
            add_bucket(b_val);
            bucket = it.next_idx();
        }
    }
    while i < inserts.len() {
        add_bucket(insert_idx);
        insert_num -= 1;
        if insert_num == 0 {
            i += 1;
            if i < inserts.len() {
                insert_idx = inserts[i].bucket_idx;
                insert_num = inserts[i].num;
            }
        } else {
            insert_idx += 1;
        }
    }
    if overflow {
        return None;
    }
    Some(merged_spans)
}
