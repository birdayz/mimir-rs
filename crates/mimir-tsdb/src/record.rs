//! Binary WAL/WBL record encoder & decoder for all Prometheus TSDB record types `1..=14`.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/record/record.go.

use thiserror::Error;

use crate::chunkenc::histogram_model::{
    is_custom_buckets_schema, is_known_schema, CounterResetHint, FloatHistogram, Histogram,
    HistogramSpan, EXPONENTIAL_SCHEMA_MAX, EXPONENTIAL_SCHEMA_MAX_RESERVED,
};
use crate::chunkenc::histogram::{reduce_int_resolution};
use crate::chunkenc::float_histogram::{reduce_float_resolution};
use crate::encoding::{Decbuf, Encbuf, EncodingError};
use crate::labels::Labels;

const UNIT_META_NAME: &str = "UNIT";
const HELP_META_NAME: &str = "HELP";

const NO_ST: u8 = 0;
const SAME_ST: u8 = 1;
const EXPLICIT_ST: u8 = 2;

#[derive(Debug, Error)]
pub enum RecordError {
    #[error("encoding error: {0}")]
    Encoding(#[from] EncodingError),
    #[error("invalid record type {found} (expected {expected})")]
    InvalidType { found: u8, expected: &'static str },
    #[error("unexpected {remaining} trailing bytes in record")]
    TrailingBytes { remaining: usize },
}

/// WAL / WBL / Checkpoint record type tag (`tsdb/record/record.go`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RecordType {
    Series = 1,
    Samples = 2,
    Tombstones = 3,
    Exemplars = 4,
    MmapMarkers = 5,
    Metadata = 6,
    HistogramSamples = 7,
    FloatHistogramSamples = 8,
    CustomBucketsHistogramSamples = 9,
    CustomBucketsFloatHistogramSamples = 10,
    SamplesV2 = 11,
    HistogramSamplesV2 = 12,
    FloatHistogramSamplesV2 = 13,
    MinValidTime = 14,
    Unknown = 255,
}

impl RecordType {
    #[inline]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Series,
            2 => Self::Samples,
            3 => Self::Tombstones,
            4 => Self::Exemplars,
            5 => Self::MmapMarkers,
            6 => Self::Metadata,
            7 => Self::HistogramSamples,
            8 => Self::FloatHistogramSamples,
            9 => Self::CustomBucketsHistogramSamples,
            10 => Self::CustomBucketsFloatHistogramSamples,
            11 => Self::SamplesV2,
            12 => Self::HistogramSamplesV2,
            13 => Self::FloatHistogramSamplesV2,
            14 => Self::MinValidTime,
            _ => Self::Unknown,
        }
    }
}

/// Metric type stored in WAL `Metadata` records (`record.MetricType`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MetricType {
    Unknown = 0,
    Counter = 1,
    Gauge = 2,
    HistogramSample = 3,
    GaugeHistogram = 4,
    Summary = 5,
    Info = 6,
    Stateset = 7,
}

impl MetricType {
    #[inline]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Counter,
            2 => Self::Gauge,
            3 => Self::HistogramSample,
            4 => Self::GaugeHistogram,
            5 => Self::Summary,
            6 => Self::Info,
            7 => Self::Stateset,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefSeries {
    pub ref_id: u64,
    pub labels: Labels,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RefSample {
    pub ref_id: u64,
    pub st: i64,
    pub t: i64,
    pub v: f64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefMetadata {
    pub ref_id: u64,
    pub metric_type: u8,
    pub unit: String,
    pub help: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RefExemplar {
    pub ref_id: u64,
    pub t: i64,
    pub v: f64,
    pub labels: Labels,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RefHistogramSample {
    pub ref_id: u64,
    pub st: i64,
    pub t: i64,
    pub h: Histogram,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RefFloatHistogramSample {
    pub ref_id: u64,
    pub st: i64,
    pub t: i64,
    pub fh: FloatHistogram,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RefMmapMarker {
    pub ref_id: u64,
    pub mmap_ref: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tombstone {
    pub ref_id: u64,
    pub mint: i64,
    pub maxt: i64,
}

/// Decoded representation of any TSDB WAL/WBL/Checkpoint record.
#[derive(Clone, Debug, PartialEq)]
pub enum DecodedRecord {
    Series(Vec<RefSeries>),
    Samples(Vec<RefSample>),
    Tombstones(Vec<Tombstone>),
    Exemplars(Vec<RefExemplar>),
    MmapMarkers(Vec<RefMmapMarker>),
    Metadata(Vec<RefMetadata>),
    HistogramSamples(Vec<RefHistogramSample>),
    FloatHistogramSamples(Vec<RefFloatHistogramSample>),
    MinValidTime(i64),
    Unknown(u8),
}

/// Decoder for Prometheus TSDB WAL/WBL records (`record.Decoder`).
#[derive(Debug, Default)]
pub struct RecordDecoder;

impl RecordDecoder {
    #[inline]
    pub fn new() -> Self {
        Self
    }

    #[inline]
    pub fn record_type(&self, rec: &[u8]) -> RecordType {
        if rec.is_empty() {
            return RecordType::Unknown;
        }
        RecordType::from_u8(rec[0])
    }

    pub fn decode_record(&self, rec: &[u8]) -> Result<DecodedRecord, RecordError> {
        match self.record_type(rec) {
            RecordType::Series => Ok(DecodedRecord::Series(self.series(rec)?)),
            RecordType::Samples | RecordType::SamplesV2 => {
                Ok(DecodedRecord::Samples(self.samples(rec)?))
            }
            RecordType::Tombstones => Ok(DecodedRecord::Tombstones(self.tombstones(rec)?)),
            RecordType::Exemplars => Ok(DecodedRecord::Exemplars(self.exemplars(rec)?)),
            RecordType::MmapMarkers => Ok(DecodedRecord::MmapMarkers(self.mmap_markers(rec)?)),
            RecordType::Metadata => Ok(DecodedRecord::Metadata(self.metadata(rec)?)),
            RecordType::HistogramSamples
            | RecordType::CustomBucketsHistogramSamples
            | RecordType::HistogramSamplesV2 => {
                Ok(DecodedRecord::HistogramSamples(self.histogram_samples(rec)?))
            }
            RecordType::FloatHistogramSamples
            | RecordType::CustomBucketsFloatHistogramSamples
            | RecordType::FloatHistogramSamplesV2 => Ok(DecodedRecord::FloatHistogramSamples(
                self.float_histogram_samples(rec)?,
            )),
            RecordType::MinValidTime => Ok(DecodedRecord::MinValidTime(self.min_valid_time(rec)?)),
            RecordType::Unknown => Ok(DecodedRecord::Unknown(rec.first().copied().unwrap_or(255))),
        }
    }

    pub fn series(&self, rec: &[u8]) -> Result<Vec<RefSeries>, RecordError> {
        let mut out = Vec::new();
        self.series_into(rec, &mut out)?;
        Ok(out)
    }

    pub fn series_into(&self, rec: &[u8], out: &mut Vec<RefSeries>) -> Result<(), RecordError> {
        let mut dec = Decbuf::new(rec);
        let typ = dec.byte()?;
        if typ != RecordType::Series as u8 {
            return Err(RecordError::InvalidType {
                found: typ,
                expected: "Series(1)",
            });
        }
        while !dec.is_empty() {
            let ref_id = dec.be64()?;
            let labels = decode_labels(&mut dec)?;
            out.push(RefSeries { ref_id, labels });
        }
        Ok(())
    }

    pub fn min_valid_time(&self, rec: &[u8]) -> Result<i64, RecordError> {
        let mut dec = Decbuf::new(rec);
        let typ = dec.byte()?;
        if typ != RecordType::MinValidTime as u8 {
            return Err(RecordError::InvalidType {
                found: typ,
                expected: "MinValidTime(14)",
            });
        }
        let mint = dec.be64_i64()?;
        if !dec.is_empty() {
            return Err(RecordError::TrailingBytes {
                remaining: dec.len(),
            });
        }
        Ok(mint)
    }

    pub fn metadata(&self, rec: &[u8]) -> Result<Vec<RefMetadata>, RecordError> {
        let mut out = Vec::new();
        self.metadata_into(rec, &mut out)?;
        Ok(out)
    }

    pub fn metadata_into(&self, rec: &[u8], out: &mut Vec<RefMetadata>) -> Result<(), RecordError> {
        let mut dec = Decbuf::new(rec);
        let typ = dec.byte()?;
        if typ != RecordType::Metadata as u8 {
            return Err(RecordError::InvalidType {
                found: typ,
                expected: "Metadata(6)",
            });
        }
        while !dec.is_empty() {
            let ref_id = dec.uvarint64()?;
            let metric_type = dec.byte()?;
            let num_fields = dec.uvarint()?;
            let mut unit = String::new();
            let mut help = String::new();
            for _ in 0..num_fields {
                let field_name = dec.uvarint_str()?;
                let field_val = dec.uvarint_str()?;
                if field_val.is_empty() {
                    continue;
                }
                match field_name {
                    UNIT_META_NAME => unit = field_val.to_string(),
                    HELP_META_NAME => help = field_val.to_string(),
                    _ => {}
                }
            }
            out.push(RefMetadata {
                ref_id,
                metric_type,
                unit,
                help,
            });
        }
        Ok(())
    }

    pub fn samples(&self, rec: &[u8]) -> Result<Vec<RefSample>, RecordError> {
        let mut out = Vec::new();
        self.samples_into(rec, &mut out)?;
        Ok(out)
    }

    pub fn samples_into(&self, rec: &[u8], out: &mut Vec<RefSample>) -> Result<(), RecordError> {
        let mut dec = Decbuf::new(rec);
        let typ = dec.byte()?;
        match RecordType::from_u8(typ) {
            RecordType::Samples => Self::samples_v1(&mut dec, out),
            RecordType::SamplesV2 => Self::samples_v2(&mut dec, out),
            _ => Err(RecordError::InvalidType {
                found: typ,
                expected: "Samples(2) or SamplesV2(11)",
            }),
        }
    }

    fn samples_v1(dec: &mut Decbuf<'_>, out: &mut Vec<RefSample>) -> Result<(), RecordError> {
        if dec.is_empty() {
            return Ok(());
        }
        let base_ref = dec.be64()? as i64;
        let base_time = dec.be64_i64()?;
        while !dec.is_empty() {
            let dref = dec.varint64()?;
            let dtime = dec.varint64()?;
            let val = dec.be64_f64()?;
            out.push(RefSample {
                ref_id: base_ref.wrapping_add(dref) as u64,
                st: 0,
                t: base_time.wrapping_add(dtime),
                v: val,
            });
        }
        Ok(())
    }

    fn samples_v2(dec: &mut Decbuf<'_>, out: &mut Vec<RefSample>) -> Result<(), RecordError> {
        if dec.is_empty() {
            return Ok(());
        }
        let start_len = out.len();
        let mut first_t = 0i64;
        let mut first_st = 0i64;
        while !dec.is_empty() {
            let (ref_id, t, st) = if out.len() == start_len {
                let r = dec.varint64()?;
                first_t = dec.varint64()?;
                first_st = dec.varint64()?;
                (r, first_t, first_st)
            } else {
                let prev = out.last().unwrap();
                let r = (prev.ref_id as i64).wrapping_add(dec.varint64()?);
                let t = first_t.wrapping_add(dec.varint64()?);
                let st = read_st_marker(dec, prev.st, first_st)?;
                (r, t, st)
            };
            let v = dec.be64_f64()?;
            out.push(RefSample {
                ref_id: ref_id as u64,
                st,
                t,
                v,
            });
        }
        Ok(())
    }

    pub fn tombstones(&self, rec: &[u8]) -> Result<Vec<Tombstone>, RecordError> {
        let mut out = Vec::new();
        self.tombstones_into(rec, &mut out)?;
        Ok(out)
    }

    pub fn tombstones_into(&self, rec: &[u8], out: &mut Vec<Tombstone>) -> Result<(), RecordError> {
        let mut dec = Decbuf::new(rec);
        let typ = dec.byte()?;
        if typ != RecordType::Tombstones as u8 {
            return Err(RecordError::InvalidType {
                found: typ,
                expected: "Tombstones(3)",
            });
        }
        while !dec.is_empty() {
            let ref_id = dec.be64()?;
            let mint = dec.varint64()?;
            let maxt = dec.varint64()?;
            out.push(Tombstone { ref_id, mint, maxt });
        }
        Ok(())
    }

    pub fn exemplars(&self, rec: &[u8]) -> Result<Vec<RefExemplar>, RecordError> {
        let mut out = Vec::new();
        self.exemplars_into(rec, &mut out)?;
        Ok(out)
    }

    pub fn exemplars_into(&self, rec: &[u8], out: &mut Vec<RefExemplar>) -> Result<(), RecordError> {
        let mut dec = Decbuf::new(rec);
        let typ = dec.byte()?;
        if typ != RecordType::Exemplars as u8 {
            return Err(RecordError::InvalidType {
                found: typ,
                expected: "Exemplars(4)",
            });
        }
        if dec.is_empty() {
            return Ok(());
        }
        let base_ref = dec.be64()?;
        let base_time = dec.be64_i64()?;
        while !dec.is_empty() {
            let dref = dec.varint64()?;
            let dtime = dec.varint64()?;
            let v = dec.be64_f64()?;
            let labels = decode_labels(&mut dec)?;
            out.push(RefExemplar {
                ref_id: base_ref.wrapping_add(dref as u64),
                t: base_time.wrapping_add(dtime),
                v,
                labels,
            });
        }
        Ok(())
    }

    pub fn mmap_markers(&self, rec: &[u8]) -> Result<Vec<RefMmapMarker>, RecordError> {
        let mut out = Vec::new();
        self.mmap_markers_into(rec, &mut out)?;
        Ok(out)
    }

    pub fn mmap_markers_into(
        &self,
        rec: &[u8],
        out: &mut Vec<RefMmapMarker>,
    ) -> Result<(), RecordError> {
        let mut dec = Decbuf::new(rec);
        let typ = dec.byte()?;
        if typ != RecordType::MmapMarkers as u8 {
            return Err(RecordError::InvalidType {
                found: typ,
                expected: "MmapMarkers(5)",
            });
        }
        while !dec.is_empty() {
            let ref_id = dec.be64()?;
            let mmap_ref = dec.be64()?;
            out.push(RefMmapMarker { ref_id, mmap_ref });
        }
        Ok(())
    }

    pub fn histogram_samples(&self, rec: &[u8]) -> Result<Vec<RefHistogramSample>, RecordError> {
        let mut out = Vec::new();
        self.histogram_samples_into(rec, &mut out)?;
        Ok(out)
    }

    pub fn histogram_samples_into(
        &self,
        rec: &[u8],
        out: &mut Vec<RefHistogramSample>,
    ) -> Result<(), RecordError> {
        let mut dec = Decbuf::new(rec);
        let typ = dec.byte()?;
        match RecordType::from_u8(typ) {
            RecordType::HistogramSamples | RecordType::CustomBucketsHistogramSamples => {
                Self::histogram_samples_v1(&mut dec, out)
            }
            RecordType::HistogramSamplesV2 => Self::histogram_samples_v2(&mut dec, out),
            _ => Err(RecordError::InvalidType {
                found: typ,
                expected: "HistogramSamples(7/9/12)",
            }),
        }
    }

    fn histogram_samples_v1(
        dec: &mut Decbuf<'_>,
        out: &mut Vec<RefHistogramSample>,
    ) -> Result<(), RecordError> {
        if dec.is_empty() {
            return Ok(());
        }
        let base_ref = dec.be64()?;
        let base_time = dec.be64_i64()?;
        while !dec.is_empty() {
            let dref = dec.varint64()?;
            let dtime = dec.varint64()?;
            let mut h = decode_histogram(dec)?;
            if !is_known_schema(h.schema) {
                continue;
            }
            if h.schema > EXPONENTIAL_SCHEMA_MAX && h.schema <= EXPONENTIAL_SCHEMA_MAX_RESERVED {
                reduce_int_resolution(&mut h, EXPONENTIAL_SCHEMA_MAX);
            }
            out.push(RefHistogramSample {
                ref_id: base_ref.wrapping_add(dref as u64),
                st: 0,
                t: base_time.wrapping_add(dtime),
                h,
            });
        }
        Ok(())
    }

    fn histogram_samples_v2(
        dec: &mut Decbuf<'_>,
        out: &mut Vec<RefHistogramSample>,
    ) -> Result<(), RecordError> {
        if dec.is_empty() {
            return Ok(());
        }
        let first_ref = dec.varint64()?;
        let first_t = dec.varint64()?;
        let first_st = dec.varint64()?;
        let mut prev_ref = 0u64;
        let mut prev_st = 0i64;
        let mut has_prev = false;

        while !dec.is_empty() {
            let (ref_id, t, st) = if !has_prev {
                has_prev = true;
                (first_ref as u64, first_t, first_st)
            } else {
                let r = (prev_ref as i64).wrapping_add(dec.varint64()?) as u64;
                let t = first_t.wrapping_add(dec.varint64()?);
                let st = read_st_marker(dec, prev_st, first_st)?;
                (r, t, st)
            };
            prev_ref = ref_id;
            prev_st = st;

            let mut h = decode_histogram(dec)?;
            if !is_known_schema(h.schema) {
                continue;
            }
            if h.schema > EXPONENTIAL_SCHEMA_MAX && h.schema <= EXPONENTIAL_SCHEMA_MAX_RESERVED {
                reduce_int_resolution(&mut h, EXPONENTIAL_SCHEMA_MAX);
            }
            out.push(RefHistogramSample { ref_id, st, t, h });
        }
        Ok(())
    }

    pub fn float_histogram_samples(
        &self,
        rec: &[u8],
    ) -> Result<Vec<RefFloatHistogramSample>, RecordError> {
        let mut out = Vec::new();
        self.float_histogram_samples_into(rec, &mut out)?;
        Ok(out)
    }

    pub fn float_histogram_samples_into(
        &self,
        rec: &[u8],
        out: &mut Vec<RefFloatHistogramSample>,
    ) -> Result<(), RecordError> {
        let mut dec = Decbuf::new(rec);
        let typ = dec.byte()?;
        match RecordType::from_u8(typ) {
            RecordType::FloatHistogramSamples | RecordType::CustomBucketsFloatHistogramSamples => {
                Self::float_histogram_samples_v1(&mut dec, out)
            }
            RecordType::FloatHistogramSamplesV2 => Self::float_histogram_samples_v2(&mut dec, out),
            _ => Err(RecordError::InvalidType {
                found: typ,
                expected: "FloatHistogramSamples(8/10/13)",
            }),
        }
    }

    fn float_histogram_samples_v1(
        dec: &mut Decbuf<'_>,
        out: &mut Vec<RefFloatHistogramSample>,
    ) -> Result<(), RecordError> {
        if dec.is_empty() {
            return Ok(());
        }
        let base_ref = dec.be64()?;
        let base_time = dec.be64_i64()?;
        while !dec.is_empty() {
            let dref = dec.varint64()?;
            let dtime = dec.varint64()?;
            let mut fh = decode_float_histogram(dec)?;
            if !is_known_schema(fh.schema) {
                continue;
            }
            if fh.schema > EXPONENTIAL_SCHEMA_MAX && fh.schema <= EXPONENTIAL_SCHEMA_MAX_RESERVED {
                reduce_float_resolution(&mut fh, EXPONENTIAL_SCHEMA_MAX);
            }
            out.push(RefFloatHistogramSample {
                ref_id: base_ref.wrapping_add(dref as u64),
                st: 0,
                t: base_time.wrapping_add(dtime),
                fh,
            });
        }
        Ok(())
    }

    fn float_histogram_samples_v2(
        dec: &mut Decbuf<'_>,
        out: &mut Vec<RefFloatHistogramSample>,
    ) -> Result<(), RecordError> {
        if dec.is_empty() {
            return Ok(());
        }
        let first_ref = dec.varint64()?;
        let first_t = dec.varint64()?;
        let first_st = dec.varint64()?;
        let mut prev_ref = 0u64;
        let mut prev_st = 0i64;
        let mut has_prev = false;

        while !dec.is_empty() {
            let (ref_id, t, st) = if !has_prev {
                has_prev = true;
                (first_ref as u64, first_t, first_st)
            } else {
                let r = (prev_ref as i64).wrapping_add(dec.varint64()?) as u64;
                let t = first_t.wrapping_add(dec.varint64()?);
                let st = read_st_marker(dec, prev_st, first_st)?;
                (r, t, st)
            };
            prev_ref = ref_id;
            prev_st = st;

            let mut fh = decode_float_histogram(dec)?;
            if !is_known_schema(fh.schema) {
                continue;
            }
            if fh.schema > EXPONENTIAL_SCHEMA_MAX && fh.schema <= EXPONENTIAL_SCHEMA_MAX_RESERVED {
                reduce_float_resolution(&mut fh, EXPONENTIAL_SCHEMA_MAX);
            }
            out.push(RefFloatHistogramSample { ref_id, st, t, fh });
        }
        Ok(())
    }
}

fn read_st_marker(dec: &mut Decbuf<'_>, prev_st: i64, first_st: i64) -> Result<i64, EncodingError> {
    match dec.byte()? {
        NO_ST => Ok(0),
        SAME_ST => Ok(prev_st),
        _ => Ok(first_st.wrapping_add(dec.varint64()?)),
    }
}

fn write_st_marker(buf: &mut Encbuf, st: i64, first_st: i64, prev_st: i64) {
    if st == 0 {
        buf.put_byte(NO_ST);
    } else if st == prev_st {
        buf.put_byte(SAME_ST);
    } else {
        buf.put_byte(EXPLICIT_ST);
        buf.put_varint64(st.wrapping_sub(first_st));
    }
}

pub fn decode_labels(dec: &mut Decbuf<'_>) -> Result<Labels, EncodingError> {
    let n = dec.uvarint()?;
    let mut pairs: smallvec::SmallVec<[(&str, &str); 24]> =
        smallvec::SmallVec::with_capacity(n.min(256));
    for _ in 0..n {
        let name = dec.uvarint_str()?;
        let value = dec.uvarint_str()?;
        pairs.push((name, value));
    }
    Ok(Labels::from_sorted_slice(&pairs))
}

pub fn encode_labels(buf: &mut Encbuf, labels: &Labels) {
    buf.put_uvarint(labels.len());
    for (name, value) in labels.iter() {
        buf.put_uvarint_str(name);
        buf.put_uvarint_str(value);
    }
}

pub fn decode_histogram(dec: &mut Decbuf<'_>) -> Result<Histogram, EncodingError> {
    let counter_reset_hint = CounterResetHint::from_u8(dec.byte()?);
    let schema = dec.varint64()? as i32;
    let zero_threshold = dec.be64_f64()?;
    let zero_count = dec.uvarint64()?;
    let count = dec.uvarint64()?;
    let sum = dec.be64_f64()?;

    let p_spans_len = dec.uvarint()?;
    let mut positive_spans = Vec::with_capacity(p_spans_len.min(4096));
    for _ in 0..p_spans_len {
        let offset = dec.varint64()? as i32;
        let length = dec.uvarint32()?;
        positive_spans.push(HistogramSpan { offset, length });
    }

    let n_spans_len = dec.uvarint()?;
    let mut negative_spans = Vec::with_capacity(n_spans_len.min(4096));
    for _ in 0..n_spans_len {
        let offset = dec.varint64()? as i32;
        let length = dec.uvarint32()?;
        negative_spans.push(HistogramSpan { offset, length });
    }

    let p_buckets_len = dec.uvarint()?;
    let mut positive_buckets = Vec::with_capacity(p_buckets_len.min(4096));
    for _ in 0..p_buckets_len {
        positive_buckets.push(dec.varint64()?);
    }

    let n_buckets_len = dec.uvarint()?;
    let mut negative_buckets = Vec::with_capacity(n_buckets_len.min(4096));
    for _ in 0..n_buckets_len {
        negative_buckets.push(dec.varint64()?);
    }

    let custom_values = if is_custom_buckets_schema(schema) {
        let c_len = dec.uvarint()?;
        let mut cv = Vec::with_capacity(c_len.min(4096));
        for _ in 0..c_len {
            cv.push(dec.be64_f64()?);
        }
        cv
    } else {
        Vec::new()
    };

    Ok(Histogram {
        counter_reset_hint,
        schema,
        zero_threshold,
        zero_count,
        count,
        sum,
        positive_spans,
        negative_spans,
        positive_buckets,
        negative_buckets,
        custom_values,
    })
}

pub fn encode_histogram(buf: &mut Encbuf, h: &Histogram) {
    buf.put_byte(h.counter_reset_hint as u8);
    buf.put_varint64(i64::from(h.schema));
    buf.put_be_f64(h.zero_threshold);
    buf.put_uvarint64(h.zero_count);
    buf.put_uvarint64(h.count);
    buf.put_be_f64(h.sum);

    buf.put_uvarint(h.positive_spans.len());
    for s in &h.positive_spans {
        buf.put_varint64(i64::from(s.offset));
        buf.put_uvarint32(s.length);
    }

    buf.put_uvarint(h.negative_spans.len());
    for s in &h.negative_spans {
        buf.put_varint64(i64::from(s.offset));
        buf.put_uvarint32(s.length);
    }

    buf.put_uvarint(h.positive_buckets.len());
    for &b in &h.positive_buckets {
        buf.put_varint64(b);
    }

    buf.put_uvarint(h.negative_buckets.len());
    for &b in &h.negative_buckets {
        buf.put_varint64(b);
    }

    if is_custom_buckets_schema(h.schema) {
        buf.put_uvarint(h.custom_values.len());
        for &v in &h.custom_values {
            buf.put_be_f64(v);
        }
    }
}

pub fn decode_float_histogram(dec: &mut Decbuf<'_>) -> Result<FloatHistogram, EncodingError> {
    let counter_reset_hint = CounterResetHint::from_u8(dec.byte()?);
    let schema = dec.varint64()? as i32;
    let zero_threshold = dec.be64_f64()?;
    let zero_count = dec.be64_f64()?;
    let count = dec.be64_f64()?;
    let sum = dec.be64_f64()?;

    let p_spans_len = dec.uvarint()?;
    let mut positive_spans = Vec::with_capacity(p_spans_len.min(4096));
    for _ in 0..p_spans_len {
        let offset = dec.varint64()? as i32;
        let length = dec.uvarint32()?;
        positive_spans.push(HistogramSpan { offset, length });
    }

    let n_spans_len = dec.uvarint()?;
    let mut negative_spans = Vec::with_capacity(n_spans_len.min(4096));
    for _ in 0..n_spans_len {
        let offset = dec.varint64()? as i32;
        let length = dec.uvarint32()?;
        negative_spans.push(HistogramSpan { offset, length });
    }

    let p_buckets_len = dec.uvarint()?;
    let mut positive_buckets = Vec::with_capacity(p_buckets_len.min(4096));
    for _ in 0..p_buckets_len {
        positive_buckets.push(dec.be64_f64()?);
    }

    let n_buckets_len = dec.uvarint()?;
    let mut negative_buckets = Vec::with_capacity(n_buckets_len.min(4096));
    for _ in 0..n_buckets_len {
        negative_buckets.push(dec.be64_f64()?);
    }

    let custom_values = if is_custom_buckets_schema(schema) {
        let c_len = dec.uvarint()?;
        let mut cv = Vec::with_capacity(c_len.min(4096));
        for _ in 0..c_len {
            cv.push(dec.be64_f64()?);
        }
        cv
    } else {
        Vec::new()
    };

    Ok(FloatHistogram {
        counter_reset_hint,
        schema,
        zero_threshold,
        zero_count,
        count,
        sum,
        positive_spans,
        negative_spans,
        positive_buckets,
        negative_buckets,
        custom_values,
    })
}

pub fn encode_float_histogram(buf: &mut Encbuf, h: &FloatHistogram) {
    buf.put_byte(h.counter_reset_hint as u8);
    buf.put_varint64(i64::from(h.schema));
    buf.put_be_f64(h.zero_threshold);
    buf.put_be_f64(h.zero_count);
    buf.put_be_f64(h.count);
    buf.put_be_f64(h.sum);

    buf.put_uvarint(h.positive_spans.len());
    for s in &h.positive_spans {
        buf.put_varint64(i64::from(s.offset));
        buf.put_uvarint32(s.length);
    }

    buf.put_uvarint(h.negative_spans.len());
    for s in &h.negative_spans {
        buf.put_varint64(i64::from(s.offset));
        buf.put_uvarint32(s.length);
    }

    buf.put_uvarint(h.positive_buckets.len());
    for &b in &h.positive_buckets {
        buf.put_be_f64(b);
    }

    buf.put_uvarint(h.negative_buckets.len());
    for &b in &h.negative_buckets {
        buf.put_be_f64(b);
    }

    if is_custom_buckets_schema(h.schema) {
        buf.put_uvarint(h.custom_values.len());
        for &v in &h.custom_values {
            buf.put_be_f64(v);
        }
    }
}

/// Encoder for Prometheus TSDB WAL/WBL/Checkpoint records (`record.Encoder`).
#[derive(Clone, Copy, Debug, Default)]
pub struct RecordEncoder {
    pub enable_st_storage: bool,
}

impl RecordEncoder {
    #[inline]
    pub const fn new(enable_st_storage: bool) -> Self {
        Self { enable_st_storage }
    }

    pub fn series(&self, series: &[RefSeries], b: Vec<u8>) -> Vec<u8> {
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::Series as u8);
        for s in series {
            buf.put_be64(s.ref_id);
            encode_labels(&mut buf, &s.labels);
        }
        buf.into_vec()
    }

    pub fn min_valid_time(&self, mint: i64, b: Vec<u8>) -> Vec<u8> {
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::MinValidTime as u8);
        buf.put_be64_i64(mint);
        buf.into_vec()
    }

    pub fn metadata(&self, metadata: &[RefMetadata], b: Vec<u8>) -> Vec<u8> {
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::Metadata as u8);
        for m in metadata {
            buf.put_uvarint64(m.ref_id);
            buf.put_byte(m.metric_type);
            buf.put_uvarint(2);
            buf.put_uvarint_str(UNIT_META_NAME);
            buf.put_uvarint_str(&m.unit);
            buf.put_uvarint_str(HELP_META_NAME);
            buf.put_uvarint_str(&m.help);
        }
        buf.into_vec()
    }

    pub fn samples(&self, samples: &[RefSample], b: Vec<u8>) -> Vec<u8> {
        if self.enable_st_storage {
            self.samples_v2(samples, b)
        } else {
            self.samples_v1(samples, b)
        }
    }

    pub fn samples_iter<I>(&self, samples: I, b: Vec<u8>) -> Vec<u8>
    where
        I: IntoIterator<Item = RefSample>,
        I::IntoIter: Clone,
    {
        let mut it = samples.into_iter();
        let Some(first) = it.clone().next() else {
            let mut buf = Encbuf { buf: b };
            buf.put_byte(if self.enable_st_storage {
                RecordType::SamplesV2 as u8
            } else {
                RecordType::Samples as u8
            });
            return buf.into_vec();
        };
        let mut buf = Encbuf { buf: b };
        if self.enable_st_storage {
            buf.put_byte(RecordType::SamplesV2 as u8);
            let _ = it.next();
            buf.put_varint64(first.ref_id as i64);
            buf.put_varint64(first.t);
            buf.put_varint64(first.st);
            buf.put_be_f64(first.v);
            let mut prev_ref = first.ref_id;
            let mut prev_st = first.st;
            for s in it {
                buf.put_varint64((s.ref_id as i64).wrapping_sub(prev_ref as i64));
                buf.put_varint64(s.t.wrapping_sub(first.t));
                write_st_marker(&mut buf, s.st, first.st, prev_st);
                buf.put_be_f64(s.v);
                prev_ref = s.ref_id;
                prev_st = s.st;
            }
        } else {
            buf.put_byte(RecordType::Samples as u8);
            buf.put_be64(first.ref_id);
            buf.put_be64_i64(first.t);
            for s in it {
                buf.put_varint64((s.ref_id as i64).wrapping_sub(first.ref_id as i64));
                buf.put_varint64(s.t.wrapping_sub(first.t));
                buf.put_be_f64(s.v);
            }
        }
        buf.into_vec()
    }

    pub fn samples_v1(&self, samples: &[RefSample], b: Vec<u8>) -> Vec<u8> {
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::Samples as u8);
        let Some(first) = samples.first() else {
            return buf.into_vec();
        };
        buf.put_be64(first.ref_id);
        buf.put_be64_i64(first.t);
        for s in samples {
            buf.put_varint64((s.ref_id as i64).wrapping_sub(first.ref_id as i64));
            buf.put_varint64(s.t.wrapping_sub(first.t));
            buf.put_be_f64(s.v);
        }
        buf.into_vec()
    }

    pub fn samples_v2(&self, samples: &[RefSample], b: Vec<u8>) -> Vec<u8> {
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::SamplesV2 as u8);
        let Some(first) = samples.first() else {
            return buf.into_vec();
        };
        buf.put_varint64(first.ref_id as i64);
        buf.put_varint64(first.t);
        buf.put_varint64(first.st);
        buf.put_be_f64(first.v);
        for i in 1..samples.len() {
            let s = samples[i];
            let prev = samples[i - 1];
            buf.put_varint64((s.ref_id as i64).wrapping_sub(prev.ref_id as i64));
            buf.put_varint64(s.t.wrapping_sub(first.t));
            write_st_marker(&mut buf, s.st, first.st, prev.st);
            buf.put_be_f64(s.v);
        }
        buf.into_vec()
    }

    pub fn tombstones(&self, tombstones: &[Tombstone], b: Vec<u8>) -> Vec<u8> {
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::Tombstones as u8);
        for t in tombstones {
            buf.put_be64(t.ref_id);
            buf.put_varint64(t.mint);
            buf.put_varint64(t.maxt);
        }
        buf.into_vec()
    }

    pub fn exemplars(&self, exemplars: &[RefExemplar], b: Vec<u8>) -> Vec<u8> {
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::Exemplars as u8);
        let Some(first) = exemplars.first() else {
            return buf.into_vec();
        };
        buf.put_be64(first.ref_id);
        buf.put_be64_i64(first.t);
        for ex in exemplars {
            buf.put_varint64((ex.ref_id as i64).wrapping_sub(first.ref_id as i64));
            buf.put_varint64(ex.t.wrapping_sub(first.t));
            buf.put_be_f64(ex.v);
            encode_labels(&mut buf, &ex.labels);
        }
        buf.into_vec()
    }

    pub fn mmap_markers(&self, markers: &[RefMmapMarker], b: Vec<u8>) -> Vec<u8> {
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::MmapMarkers as u8);
        for m in markers {
            buf.put_be64(m.ref_id);
            buf.put_be64(m.mmap_ref);
        }
        buf.into_vec()
    }

    /// Encodes histogram samples, returning `(encoded_bytes, custom_bucket_leftovers)`.
    pub fn histogram_samples<'a>(
        &self,
        histograms: &'a [RefHistogramSample],
        b: Vec<u8>,
    ) -> (Vec<u8>, Vec<&'a RefHistogramSample>) {
        if self.enable_st_storage {
            return (self.histogram_samples_v2(histograms, b), Vec::new());
        }
        let start_len = b.len();
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::HistogramSamples as u8);
        let Some(first) = histograms.first() else {
            return (buf.into_vec(), Vec::new());
        };
        let mut custom = Vec::new();
        buf.put_be64(first.ref_id);
        buf.put_be64_i64(first.t);
        for h in histograms {
            if h.h.uses_custom_buckets() {
                custom.push(h);
                continue;
            }
            buf.put_varint64((h.ref_id as i64).wrapping_sub(first.ref_id as i64));
            buf.put_varint64(h.t.wrapping_sub(first.t));
            encode_histogram(&mut buf, &h.h);
        }
        if custom.len() == histograms.len() {
            buf.buf.truncate(start_len);
        }
        (buf.into_vec(), custom)
    }

    pub fn custom_buckets_histogram_samples(
        &self,
        histograms: &[&RefHistogramSample],
        b: Vec<u8>,
    ) -> Vec<u8> {
        if self.enable_st_storage {
            let owned: Vec<RefHistogramSample> = histograms.iter().map(|h| (*h).clone()).collect();
            return self.histogram_samples_v2(&owned, b);
        }
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::CustomBucketsHistogramSamples as u8);
        let Some(first) = histograms.first() else {
            return buf.into_vec();
        };
        buf.put_be64(first.ref_id);
        buf.put_be64_i64(first.t);
        for h in histograms {
            buf.put_varint64((h.ref_id as i64).wrapping_sub(first.ref_id as i64));
            buf.put_varint64(h.t.wrapping_sub(first.t));
            encode_histogram(&mut buf, &h.h);
        }
        buf.into_vec()
    }

    pub fn histogram_samples_v2(&self, histograms: &[RefHistogramSample], b: Vec<u8>) -> Vec<u8> {
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::HistogramSamplesV2 as u8);
        let Some(first) = histograms.first() else {
            return buf.into_vec();
        };
        buf.put_varint64(first.ref_id as i64);
        buf.put_varint64(first.t);
        buf.put_varint64(first.st);
        encode_histogram(&mut buf, &first.h);
        let mut prev = first;
        for h in &histograms[1..] {
            buf.put_varint64((h.ref_id as i64).wrapping_sub(prev.ref_id as i64));
            buf.put_varint64(h.t.wrapping_sub(first.t));
            write_st_marker(&mut buf, h.st, first.st, prev.st);
            encode_histogram(&mut buf, &h.h);
            prev = h;
        }
        buf.into_vec()
    }

    /// Encodes float histogram samples, returning `(encoded_bytes, custom_bucket_leftovers)`.
    pub fn float_histogram_samples<'a>(
        &self,
        histograms: &'a [RefFloatHistogramSample],
        b: Vec<u8>,
    ) -> (Vec<u8>, Vec<&'a RefFloatHistogramSample>) {
        if self.enable_st_storage {
            return (self.float_histogram_samples_v2(histograms, b), Vec::new());
        }
        let start_len = b.len();
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::FloatHistogramSamples as u8);
        let Some(first) = histograms.first() else {
            return (buf.into_vec(), Vec::new());
        };
        let mut custom = Vec::new();
        buf.put_be64(first.ref_id);
        buf.put_be64_i64(first.t);
        for fh in histograms {
            if fh.fh.uses_custom_buckets() {
                custom.push(fh);
                continue;
            }
            buf.put_varint64((fh.ref_id as i64).wrapping_sub(first.ref_id as i64));
            buf.put_varint64(fh.t.wrapping_sub(first.t));
            encode_float_histogram(&mut buf, &fh.fh);
        }
        if custom.len() == histograms.len() {
            buf.buf.truncate(start_len);
        }
        (buf.into_vec(), custom)
    }

    pub fn custom_buckets_float_histogram_samples(
        &self,
        histograms: &[&RefFloatHistogramSample],
        b: Vec<u8>,
    ) -> Vec<u8> {
        if self.enable_st_storage {
            let owned: Vec<RefFloatHistogramSample> =
                histograms.iter().map(|h| (*h).clone()).collect();
            return self.float_histogram_samples_v2(&owned, b);
        }
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::CustomBucketsFloatHistogramSamples as u8);
        let Some(first) = histograms.first() else {
            return buf.into_vec();
        };
        buf.put_be64(first.ref_id);
        buf.put_be64_i64(first.t);
        for fh in histograms {
            buf.put_varint64((fh.ref_id as i64).wrapping_sub(first.ref_id as i64));
            buf.put_varint64(fh.t.wrapping_sub(first.t));
            encode_float_histogram(&mut buf, &fh.fh);
        }
        buf.into_vec()
    }

    pub fn float_histogram_samples_v2(
        &self,
        histograms: &[RefFloatHistogramSample],
        b: Vec<u8>,
    ) -> Vec<u8> {
        let mut buf = Encbuf { buf: b };
        buf.put_byte(RecordType::FloatHistogramSamplesV2 as u8);
        let Some(first) = histograms.first() else {
            return buf.into_vec();
        };
        buf.put_varint64(first.ref_id as i64);
        buf.put_varint64(first.t);
        buf.put_varint64(first.st);
        encode_float_histogram(&mut buf, &first.fh);
        let mut prev = first;
        for fh in &histograms[1..] {
            buf.put_varint64((fh.ref_id as i64).wrapping_sub(prev.ref_id as i64));
            buf.put_varint64(fh.t.wrapping_sub(first.t));
            write_st_marker(&mut buf, fh.st, first.st, prev.st);
            encode_float_histogram(&mut buf, &fh.fh);
            prev = fh;
        }
        buf.into_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_14_record_types_roundtrip() {
        let dec = RecordDecoder::new();
        let enc_v1 = RecordEncoder::new(false);
        let enc_v2 = RecordEncoder::new(true);

        // 1: Series
        let series = vec![RefSeries {
            ref_id: 42,
            labels: Labels::from_pairs([("__name__", "up"), ("job", "api")]),
        }];
        let b = enc_v1.series(&series, Vec::new());
        assert_eq!(dec.series(&b).unwrap(), series);

        // 2: Samples V1 & 11: Samples V2
        let samples = vec![
            RefSample {
                ref_id: 1,
                st: 900,
                t: 1000,
                v: 1.25,
            },
            RefSample {
                ref_id: 2,
                st: 900,
                t: 2000,
                v: 2.5,
            },
            RefSample {
                ref_id: 5,
                st: 0,
                t: 3000,
                v: 3.75,
            },
        ];
        let b_s1 = enc_v1.samples(&samples, Vec::new());
        assert_eq!(b_s1[0], RecordType::Samples as u8);
        let dec_s1 = dec.samples(&b_s1).unwrap();
        assert_eq!(dec_s1[0].v, 1.25);
        assert_eq!(dec_s1[2].ref_id, 5);
        let b_s2 = enc_v2.samples(&samples, Vec::new());
        assert_eq!(b_s2[0], RecordType::SamplesV2 as u8);
        assert_eq!(dec.samples(&b_s2).unwrap(), samples);

        // 3: Tombstones
        let stones = vec![Tombstone {
            ref_id: 7,
            mint: 100,
            maxt: 500,
        }];
        let b_t = enc_v1.tombstones(&stones, Vec::new());
        assert_eq!(dec.tombstones(&b_t).unwrap(), stones);

        // 4: Exemplars
        let exs = vec![RefExemplar {
            ref_id: 7,
            t: 1234,
            v: 9.5,
            labels: Labels::from_pairs([("trace_id", "abc")]),
        }];
        let b_ex = enc_v1.exemplars(&exs, Vec::new());
        assert_eq!(dec.exemplars(&b_ex).unwrap(), exs);

        // 5: MmapMarkers
        let markers = vec![RefMmapMarker {
            ref_id: 7,
            mmap_ref: (1u64 << 32) | 8,
        }];
        let b_mm = enc_v1.mmap_markers(&markers, Vec::new());
        assert_eq!(dec.mmap_markers(&b_mm).unwrap(), markers);

        // 6: Metadata
        let meta = vec![RefMetadata {
            ref_id: 10,
            metric_type: MetricType::Counter as u8,
            unit: "seconds".to_string(),
            help: "Total requests".to_string(),
        }];
        let b_md = enc_v1.metadata(&meta, Vec::new());
        assert_eq!(dec.metadata(&b_md).unwrap(), meta);

        // 7, 9, 12: Histograms (exp + custom + v2)
        let h_exp = RefHistogramSample {
            ref_id: 1,
            st: 100,
            t: 1000,
            h: Histogram {
                counter_reset_hint: CounterResetHint::NotCounterReset,
                schema: 0,
                zero_threshold: 0.001,
                zero_count: 2,
                count: 5,
                sum: 12.5,
                positive_spans: vec![HistogramSpan {
                    offset: 0,
                    length: 2,
                }],
                positive_buckets: vec![2, -1],
                ..Default::default()
            },
        };
        let h_cb = RefHistogramSample {
            ref_id: 2,
            st: 200,
            t: 2000,
            h: Histogram {
                counter_reset_hint: CounterResetHint::NotCounterReset,
                schema: crate::chunkenc::CUSTOM_BUCKETS_SCHEMA,
                count: 3,
                sum: 4.0,
                positive_spans: vec![HistogramSpan {
                    offset: 0,
                    length: 2,
                }],
                positive_buckets: vec![1, 1],
                custom_values: vec![0.1, 1.0, 10.0],
                ..Default::default()
            },
        };
        let all_h = vec![h_exp.clone(), h_cb.clone()];
        let (b_h1, leftover) = enc_v1.histogram_samples(&all_h, Vec::new());
        assert_eq!(b_h1[0], RecordType::HistogramSamples as u8);
        assert_eq!(leftover.len(), 1);
        let b_hcb = enc_v1.custom_buckets_histogram_samples(&leftover, Vec::new());
        assert_eq!(b_hcb[0], RecordType::CustomBucketsHistogramSamples as u8);
        assert_eq!(dec.histogram_samples(&b_h1).unwrap()[0].h, h_exp.h);
        assert_eq!(dec.histogram_samples(&b_hcb).unwrap()[0].h, h_cb.h);

        let b_hv2 = enc_v2.histogram_samples(&all_h, Vec::new()).0;
        assert_eq!(b_hv2[0], RecordType::HistogramSamplesV2 as u8);
        assert_eq!(dec.histogram_samples(&b_hv2).unwrap(), all_h);

        // 8, 10, 13: FloatHistograms
        let fh_exp = RefFloatHistogramSample {
            ref_id: 1,
            st: 100,
            t: 1000,
            fh: h_exp.h.to_float(),
        };
        let fh_cb = RefFloatHistogramSample {
            ref_id: 2,
            st: 200,
            t: 2000,
            fh: h_cb.h.to_float(),
        };
        let all_fh = vec![fh_exp.clone(), fh_cb.clone()];
        let (b_fh1, f_leftover) = enc_v1.float_histogram_samples(&all_fh, Vec::new());
        assert_eq!(b_fh1[0], RecordType::FloatHistogramSamples as u8);
        assert_eq!(f_leftover.len(), 1);
        let b_fhcb = enc_v1.custom_buckets_float_histogram_samples(&f_leftover, Vec::new());
        assert_eq!(
            b_fhcb[0],
            RecordType::CustomBucketsFloatHistogramSamples as u8
        );
        assert_eq!(dec.float_histogram_samples(&b_fh1).unwrap()[0].fh, fh_exp.fh);
        assert_eq!(dec.float_histogram_samples(&b_fhcb).unwrap()[0].fh, fh_cb.fh);

        let b_fhv2 = enc_v2.float_histogram_samples(&all_fh, Vec::new()).0;
        assert_eq!(b_fhv2[0], RecordType::FloatHistogramSamplesV2 as u8);
        assert_eq!(dec.float_histogram_samples(&b_fhv2).unwrap(), all_fh);

        // 14: MinValidTime
        let b_mvt = enc_v1.min_valid_time(987654321, Vec::new());
        assert_eq!(b_mvt[0], RecordType::MinValidTime as u8);
        assert_eq!(dec.min_valid_time(&b_mvt).unwrap(), 987654321);
    }
}
