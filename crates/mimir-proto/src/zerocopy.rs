//! Zero-copy borrowed arena decoder and fast RW2 encoder for `cortexpb::WriteRequest`.
//!
//! Ported from github.com/grafana/mimir/pkg/mimirpb/timeseries.go,
//! compat_rw2.go, prealloc_rw2.go, and mimir.pb.go.expdiff.

use std::collections::HashMap;
use std::ops::Range;

use prost::Message;
use thiserror::Error;

use crate::cortexpb;
use crate::symbols::{
    CommonSymbols, FastSymbolsTable, V2_COMMON_SYMBOLS_LIST, V2_RECORD_SYMBOL_OFFSET,
};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DecodeError {
    #[error("unexpected EOF while decoding protobuf")]
    UnexpectedEof,
    #[error("varint overflow while decoding protobuf")]
    VarintOverflow,
    #[error("invalid protobuf wire type {wire_type} for field {field}")]
    InvalidWireType { field: u32, wire_type: u8 },
    #[error("illegal protobuf tag {0}")]
    InvalidFieldNumber(u32),
    #[error("invalid UTF-8 string in protobuf field")]
    InvalidUtf8,
    #[error("input buffer exceeds 4 GiB limit")]
    InputTooLarge,
    #[error("proto: Remote Write 1.0 field Timeseries in non-Remote Write 1.0 message")]
    UnexpectedRw1Timeseries,
    #[error("proto: Remote Write 1.0 field Metadata in non-Remote Write 1.0 message")]
    UnexpectedRw1Metadata,
    #[error("proto: Remote Write 2.0 field Symbols in non-Remote Write 2.0 message")]
    UnexpectedRw2Symbols,
    #[error("proto: Remote Write 2.0 field Timeseries in non-Remote Write 2.0 message")]
    UnexpectedRw2Timeseries,
    #[error("proto: Remote Write 2.0 symbols must start with empty string")]
    InvalidFirstSymbol,
    #[error("proto: Remote Write 2.0 odd number of label references")]
    OddNumberOfLabelRefs,
    #[error("proto: Remote Write 2.0 odd number of exemplar label references")]
    OddNumberOfExemplarLabelRefs,
    #[error("proto: Remote Write 2.0 invalid label reference {index} (offset {offset})")]
    InvalidLabelRef { index: u32, offset: u32 },
    #[error("proto: Remote Write 2.0 invalid exemplar label reference {index} (offset {offset})")]
    InvalidExemplarLabelRef { index: u32, offset: u32 },
    #[error("proto: Remote Write 2.0 invalid help reference {index} (offset {offset})")]
    InvalidHelpRef { index: u32, offset: u32 },
    #[error("proto: Remote Write 2.0 invalid unit reference {index} (offset {offset})")]
    InvalidUnitRef { index: u32, offset: u32 },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EncodeRw2Error {
    #[error("the provided request is already rw2")]
    AlreadyRw2,
    #[error("failed to encode protobuf: {0}")]
    Encode(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeMode<'a> {
    Classic,
    Rw2,
    KafkaV2,
    Rw2Custom {
        offset: u32,
        common_symbols: &'a [&'a str],
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeOptions<'a> {
    pub mode: DecodeMode<'a>,
    pub skip_unmarshaling_exemplars: bool,
    pub skip_normalize_metadata_metric_name: bool,
    pub skip_deduplicate_metadata: bool,
}

impl<'a> DecodeOptions<'a> {
    pub const fn classic() -> Self {
        Self {
            mode: DecodeMode::Classic,
            skip_unmarshaling_exemplars: false,
            skip_normalize_metadata_metric_name: false,
            skip_deduplicate_metadata: false,
        }
    }

    pub const fn rw2() -> Self {
        Self {
            mode: DecodeMode::Rw2,
            skip_unmarshaling_exemplars: false,
            skip_normalize_metadata_metric_name: false,
            skip_deduplicate_metadata: false,
        }
    }

    /// Matches `ingest.deserializeRecordContentV2` in `pkg/storage/ingest/version.go`.
    pub const fn kafka_v2() -> Self {
        Self {
            mode: DecodeMode::KafkaV2,
            skip_unmarshaling_exemplars: false,
            skip_normalize_metadata_metric_name: true,
            skip_deduplicate_metadata: true,
        }
    }
}

/// Reference to a byte slice either inside the input buffer or inside a static/common symbol table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SliceRef {
    Empty,
    Input { start: u32, len: u32 },
    Common { idx: u32, len: u32 },
}

impl SliceRef {
    #[inline]
    fn resolve<'a>(self, input: &'a [u8], common_symbols: &'a [&'a str]) -> &'a [u8] {
        match self {
            SliceRef::Empty => &[],
            SliceRef::Input { start, len } => {
                let s = start as usize;
                let e = s + len as usize;
                &input[s..e]
            }
            SliceRef::Common { idx, len } => &common_symbols[idx as usize].as_bytes()[..len as usize],
        }
    }

    #[inline]
    fn resolve_str<'a>(self, input: &'a [u8], common_symbols: &'a [&'a str]) -> &'a str {
        // SAFETY/validity: all String/str SliceRefs are UTF-8 validated when created.
        match self {
            SliceRef::Empty => "",
            SliceRef::Input { start, len } => {
                let s = start as usize;
                let e = s + len as usize;
                std::str::from_utf8(&input[s..e]).expect("UTF-8 validated on decode")
            }
            SliceRef::Common { idx, len } => &common_symbols[idx as usize][..len as usize],
        }
    }

    #[inline]
    fn is_empty(self) -> bool {
        matches!(self, SliceRef::Empty)
            || matches!(self, SliceRef::Input { len: 0, .. })
            || matches!(self, SliceRef::Common { len: 0, .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedLabel<'a> {
    pub name: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> BorrowedLabel<'a> {
    pub fn name_str(&self) -> Result<&'a str, std::str::Utf8Error> {
        std::str::from_utf8(self.name)
    }

    pub fn value_str(&self) -> Result<&'a str, std::str::Utf8Error> {
        std::str::from_utf8(self.value)
    }
}

#[derive(Debug, Clone, Copy)]
struct LabelRefPair {
    name: SliceRef,
    value: SliceRef,
}

#[derive(Debug, Clone)]
struct ExemplarMeta {
    labels: Range<usize>,
    value: f64,
    timestamp_ms: i64,
}

#[derive(Debug, Clone)]
struct HistogramMeta {
    count: Option<cortexpb::histogram::Count>,
    sum: f64,
    schema: i32,
    zero_threshold: f64,
    zero_count: Option<cortexpb::histogram::ZeroCount>,
    negative_spans: Range<usize>,
    negative_deltas: Range<usize>,
    negative_counts: Range<usize>,
    positive_spans: Range<usize>,
    positive_deltas: Range<usize>,
    positive_counts: Range<usize>,
    reset_hint: i32,
    timestamp: i64,
    custom_values: Range<usize>,
}

#[derive(Debug, Clone)]
struct TimeSeriesMeta {
    labels: Range<usize>,
    samples: Range<usize>,
    exemplars: Range<usize>,
    histograms: Range<usize>,
    created_timestamp: i64,
}

#[derive(Debug, Clone, Copy)]
struct MetadataMeta {
    metric_type: i32,
    family_name: SliceRef,
    help: SliceRef,
    unit: SliceRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedMetadata<'a> {
    pub metric_type: i32,
    pub metric_family_name: &'a str,
    pub help: &'a str,
    pub unit: &'a str,
}

/// Reusable lifetime-free scratch space for zero-allocation `WriteRequest` decoding.
#[derive(Debug, Default)]
pub struct WriteRequestScratch {
    labels: Vec<LabelRefPair>,
    samples: Vec<cortexpb::Sample>,
    exemplars: Vec<ExemplarMeta>,
    exemplar_labels: Vec<LabelRefPair>,
    histograms: Vec<HistogramMeta>,
    spans: Vec<cortexpb::BucketSpan>,
    deltas: Vec<i64>,
    floats: Vec<f64>,
    timeseries: Vec<TimeSeriesMeta>,
    metadata: Vec<MetadataMeta>,
    rw2_symbols: Vec<SliceRef>,
    rw2_label_refs: Vec<u32>,
    dedup_hashes: HashMap<u64, Vec<usize>>,
}

impl WriteRequestScratch {
    pub fn new() -> Self {
        Self {
            labels: Vec::with_capacity(256),
            samples: Vec::with_capacity(256),
            exemplars: Vec::with_capacity(32),
            exemplar_labels: Vec::with_capacity(64),
            histograms: Vec::with_capacity(32),
            spans: Vec::with_capacity(64),
            deltas: Vec::with_capacity(128),
            floats: Vec::with_capacity(128),
            timeseries: Vec::with_capacity(100),
            metadata: Vec::with_capacity(32),
            rw2_symbols: Vec::with_capacity(128),
            rw2_label_refs: Vec::with_capacity(64),
            dedup_hashes: HashMap::with_capacity(32),
        }
    }

    pub fn clear(&mut self) {
        self.labels.clear();
        self.samples.clear();
        self.exemplars.clear();
        self.exemplar_labels.clear();
        self.histograms.clear();
        self.spans.clear();
        self.deltas.clear();
        self.floats.clear();
        self.timeseries.clear();
        self.metadata.clear();
        self.rw2_symbols.clear();
        self.rw2_label_refs.clear();
        self.dedup_hashes.clear();
    }
}

/// Borrowed view over a decoded `cortexpb::WriteRequest` backed by `input` and `scratch`.
pub struct BorrowedWriteRequest<'a, 's> {
    input: &'a [u8],
    common_symbols: &'a [&'a str],
    scratch: &'s WriteRequestScratch,
    pub source: i32,
    pub skip_label_validation: bool,
    pub skip_label_count_validation: bool,
}

impl<'a, 's> BorrowedWriteRequest<'a, 's> {
    #[inline]
    pub fn timeseries_len(&self) -> usize {
        self.scratch.timeseries.len()
    }

    #[inline]
    pub fn metadata_len(&self) -> usize {
        self.scratch.metadata.len()
    }

    pub fn timeseries(&self) -> impl ExactSizeIterator<Item = BorrowedTimeSeriesView<'_, 'a>> {
        (0..self.scratch.timeseries.len()).map(move |i| self.timeseries_at(i))
    }

    #[inline]
    pub fn timeseries_at(&self, idx: usize) -> BorrowedTimeSeriesView<'_, 'a> {
        BorrowedTimeSeriesView {
            input: self.input,
            common_symbols: self.common_symbols,
            scratch: self.scratch,
            meta: &self.scratch.timeseries[idx],
        }
    }

    pub fn metadata(&self) -> impl ExactSizeIterator<Item = BorrowedMetadata<'a>> + '_ {
        self.scratch.metadata.iter().map(move |m| BorrowedMetadata {
            metric_type: m.metric_type,
            metric_family_name: m.family_name.resolve_str(self.input, self.common_symbols),
            help: m.help.resolve_str(self.input, self.common_symbols),
            unit: m.unit.resolve_str(self.input, self.common_symbols),
        })
    }

    /// Materializes this borrowed view into an owned `cortexpb::WriteRequest`.
    pub fn to_owned_write_request(&self) -> cortexpb::WriteRequest {
        let timeseries = self
            .timeseries()
            .map(|ts| ts.to_owned_timeseries())
            .collect();
        let metadata = self
            .metadata()
            .map(|m| cortexpb::MetricMetadata {
                r#type: m.metric_type,
                metric_family_name: m.metric_family_name.to_owned(),
                help: m.help.to_owned(),
                unit: m.unit.to_owned(),
            })
            .collect();

        cortexpb::WriteRequest {
            timeseries,
            source: self.source,
            metadata,
            symbols_rw2: Vec::new(),
            timeseries_rw2: Vec::new(),
            skip_label_validation: self.skip_label_validation,
            skip_label_count_validation: self.skip_label_count_validation,
        }
    }
}

pub struct BorrowedTimeSeriesView<'v, 'a> {
    input: &'a [u8],
    common_symbols: &'a [&'a str],
    scratch: &'v WriteRequestScratch,
    meta: &'v TimeSeriesMeta,
}

impl<'v, 'a> BorrowedTimeSeriesView<'v, 'a> {
    pub fn labels(&self) -> impl ExactSizeIterator<Item = BorrowedLabel<'a>> + '_ {
        self.scratch.labels[self.meta.labels.clone()]
            .iter()
            .map(move |pair| BorrowedLabel {
                name: pair.name.resolve(self.input, self.common_symbols),
                value: pair.value.resolve(self.input, self.common_symbols),
            })
    }

    #[inline]
    pub fn samples(&self) -> &[cortexpb::Sample] {
        &self.scratch.samples[self.meta.samples.clone()]
    }

    pub fn exemplars(&self) -> impl ExactSizeIterator<Item = BorrowedExemplarView<'_, 'a>> {
        self.scratch.exemplars[self.meta.exemplars.clone()]
            .iter()
            .map(move |ex| BorrowedExemplarView {
                input: self.input,
                common_symbols: self.common_symbols,
                scratch: self.scratch,
                meta: ex,
            })
    }

    pub fn histograms(&self) -> impl ExactSizeIterator<Item = BorrowedHistogramView<'_>> {
        self.scratch.histograms[self.meta.histograms.clone()]
            .iter()
            .map(move |h| BorrowedHistogramView {
                scratch: self.scratch,
                meta: h,
            })
    }

    #[inline]
    pub fn created_timestamp(&self) -> i64 {
        self.meta.created_timestamp
    }

    pub fn to_owned_timeseries(&self) -> cortexpb::TimeSeries {
        let labels = self
            .labels()
            .map(|l| cortexpb::LabelPair {
                name: bytes::Bytes::copy_from_slice(l.name),
                value: bytes::Bytes::copy_from_slice(l.value),
            })
            .collect();
        let samples = self.samples().to_vec();
        let exemplars = self
            .exemplars()
            .map(|e| cortexpb::Exemplar {
                labels: e
                    .labels()
                    .map(|l| cortexpb::LabelPair {
                        name: bytes::Bytes::copy_from_slice(l.name),
                        value: bytes::Bytes::copy_from_slice(l.value),
                    })
                    .collect(),
                value: e.value(),
                timestamp_ms: e.timestamp_ms(),
            })
            .collect();
        let histograms = self
            .histograms()
            .map(|h| h.to_owned_histogram())
            .collect();
        cortexpb::TimeSeries {
            labels,
            samples,
            exemplars,
            histograms,
            created_timestamp: self.created_timestamp(),
        }
    }
}

pub struct BorrowedExemplarView<'v, 'a> {
    input: &'a [u8],
    common_symbols: &'a [&'a str],
    scratch: &'v WriteRequestScratch,
    meta: &'v ExemplarMeta,
}

impl<'v, 'a> BorrowedExemplarView<'v, 'a> {
    pub fn labels(&self) -> impl ExactSizeIterator<Item = BorrowedLabel<'a>> + '_ {
        self.scratch.exemplar_labels[self.meta.labels.clone()]
            .iter()
            .map(move |pair| BorrowedLabel {
                name: pair.name.resolve(self.input, self.common_symbols),
                value: pair.value.resolve(self.input, self.common_symbols),
            })
    }

    #[inline]
    pub fn value(&self) -> f64 {
        self.meta.value
    }

    #[inline]
    pub fn timestamp_ms(&self) -> i64 {
        self.meta.timestamp_ms
    }
}

pub struct BorrowedHistogramView<'v> {
    scratch: &'v WriteRequestScratch,
    meta: &'v HistogramMeta,
}

impl<'v> BorrowedHistogramView<'v> {
    #[inline]
    pub fn count(&self) -> Option<cortexpb::histogram::Count> {
        self.meta.count
    }

    #[inline]
    pub fn sum(&self) -> f64 {
        self.meta.sum
    }

    #[inline]
    pub fn schema(&self) -> i32 {
        self.meta.schema
    }

    #[inline]
    pub fn zero_threshold(&self) -> f64 {
        self.meta.zero_threshold
    }

    #[inline]
    pub fn zero_count(&self) -> Option<cortexpb::histogram::ZeroCount> {
        self.meta.zero_count
    }

    #[inline]
    pub fn negative_spans(&self) -> &[cortexpb::BucketSpan] {
        &self.scratch.spans[self.meta.negative_spans.clone()]
    }

    #[inline]
    pub fn negative_deltas(&self) -> &[i64] {
        &self.scratch.deltas[self.meta.negative_deltas.clone()]
    }

    #[inline]
    pub fn negative_counts(&self) -> &[f64] {
        &self.scratch.floats[self.meta.negative_counts.clone()]
    }

    #[inline]
    pub fn positive_spans(&self) -> &[cortexpb::BucketSpan] {
        &self.scratch.spans[self.meta.positive_spans.clone()]
    }

    #[inline]
    pub fn positive_deltas(&self) -> &[i64] {
        &self.scratch.deltas[self.meta.positive_deltas.clone()]
    }

    #[inline]
    pub fn positive_counts(&self) -> &[f64] {
        &self.scratch.floats[self.meta.positive_counts.clone()]
    }

    #[inline]
    pub fn reset_hint(&self) -> i32 {
        self.meta.reset_hint
    }

    #[inline]
    pub fn timestamp(&self) -> i64 {
        self.meta.timestamp
    }

    #[inline]
    pub fn custom_values(&self) -> &[f64] {
        &self.scratch.floats[self.meta.custom_values.clone()]
    }

    pub fn to_owned_histogram(&self) -> cortexpb::Histogram {
        cortexpb::Histogram {
            sum: self.sum(),
            schema: self.schema(),
            zero_threshold: self.zero_threshold(),
            negative_spans: self.negative_spans().to_vec(),
            negative_deltas: self.negative_deltas().to_vec(),
            negative_counts: self.negative_counts().to_vec(),
            positive_spans: self.positive_spans().to_vec(),
            positive_deltas: self.positive_deltas().to_vec(),
            positive_counts: self.positive_counts().to_vec(),
            reset_hint: self.reset_hint(),
            timestamp: self.timestamp(),
            custom_values: self.custom_values().to_vec(),
            count: self.count(),
            zero_count: self.zero_count(),
        }
    }
}

/// Decodes a `cortexpb.WriteRequest` (classic RW1, RW2, or Kafka v2) directly from `input` into
/// reusable `scratch` with zero per-label string allocations.
pub fn decode_write_request<'a, 's>(
    input: &'a [u8],
    opts: DecodeOptions<'a>,
    scratch: &'s mut WriteRequestScratch,
) -> Result<BorrowedWriteRequest<'a, 's>, DecodeError> {
    if input.len() > u32::MAX as usize {
        return Err(DecodeError::InputTooLarge);
    }
    scratch.clear();

    let (is_rw2, rw2_offset, common_symbols): (bool, u32, &'a [&'a str]) = match opts.mode {
        DecodeMode::Classic => (false, 0, &[]),
        DecodeMode::Rw2 => (true, 0, &[]),
        DecodeMode::KafkaV2 => (true, V2_RECORD_SYMBOL_OFFSET, V2_COMMON_SYMBOLS_LIST),
        DecodeMode::Rw2Custom {
            offset,
            common_symbols,
        } => (true, offset, common_symbols),
    };

    let mut source: i32 = 0;
    let mut skip_label_validation = false;
    let mut skip_label_count_validation = false;
    let mut seen_first_symbol = false;
    let mut prescanned_symbols = false;

    let mut pos = 0usize;
    let len = input.len();
    while pos < len {
        let (field_num, wire_type) = read_key(input, &mut pos)?;
        match field_num {
            1 => {
                if is_rw2 {
                    return Err(DecodeError::UnexpectedRw1Timeseries);
                }
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range(input, &mut pos)?;
                let ts_meta = decode_timeseries_rw1(
                    input,
                    sub,
                    opts.skip_unmarshaling_exemplars,
                    scratch,
                )?;
                scratch.timeseries.push(ts_meta);
            }
            2 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 2,
                        wire_type,
                    });
                }
                source = read_varint(input, &mut pos)? as i32;
            }
            3 => {
                if is_rw2 {
                    return Err(DecodeError::UnexpectedRw1Metadata);
                }
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 3,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range(input, &mut pos)?;
                let meta = decode_metadata_rw1(input, sub)?;
                scratch.metadata.push(meta);
            }
            4 => {
                if !is_rw2 {
                    return Err(DecodeError::UnexpectedRw2Symbols);
                }
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 4,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range(input, &mut pos)?;
                if !prescanned_symbols {
                    let sym_str = std::str::from_utf8(&input[sub.clone()])
                        .map_err(|_| DecodeError::InvalidUtf8)?;
                    if !seen_first_symbol && !sym_str.is_empty() {
                        return Err(DecodeError::InvalidFirstSymbol);
                    }
                    seen_first_symbol = true;
                    scratch.rw2_symbols.push(if sub.is_empty() {
                        SliceRef::Empty
                    } else {
                        SliceRef::Input {
                            start: sub.start as u32,
                            len: sub.len() as u32,
                        }
                    });
                }
            }
            5 => {
                if !is_rw2 {
                    return Err(DecodeError::UnexpectedRw2Timeseries);
                }
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 5,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range(input, &mut pos)?;
                if !seen_first_symbol && !prescanned_symbols {
                    // Handle rare out-of-order protobuf fields where field 5 appears before field 4.
                    prescanned_symbols = true;
                    let mut scan_pos = pos;
                    while scan_pos < len {
                        let (sf, sw) = read_key(input, &mut scan_pos)?;
                        if sf == 4 {
                            if sw != 2 {
                                return Err(DecodeError::InvalidWireType {
                                    field: 4,
                                    wire_type: sw,
                                });
                            }
                            let sym_range = read_length_delimited_range(input, &mut scan_pos)?;
                            let sym_str = std::str::from_utf8(&input[sym_range.clone()])
                                .map_err(|_| DecodeError::InvalidUtf8)?;
                            if !seen_first_symbol && !sym_str.is_empty() {
                                return Err(DecodeError::InvalidFirstSymbol);
                            }
                            seen_first_symbol = true;
                            scratch.rw2_symbols.push(if sym_range.is_empty() {
                                SliceRef::Empty
                            } else {
                                SliceRef::Input {
                                    start: sym_range.start as u32,
                                    len: sym_range.len() as u32,
                                }
                            });
                        } else {
                            skip_field(input, &mut scan_pos, sw)?;
                        }
                    }
                }
                let ts_meta = decode_timeseries_rw2(
                    input,
                    sub,
                    rw2_offset,
                    common_symbols,
                    &opts,
                    scratch,
                )?;
                scratch.timeseries.push(ts_meta);
            }
            1000 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1000,
                        wire_type,
                    });
                }
                skip_label_validation = read_varint(input, &mut pos)? != 0;
            }
            1001 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1001,
                        wire_type,
                    });
                }
                skip_label_count_validation = read_varint(input, &mut pos)? != 0;
            }
            _ => {
                skip_field(input, &mut pos, wire_type)?;
            }
        }
    }

    Ok(BorrowedWriteRequest {
        input,
        common_symbols,
        scratch,
        source,
        skip_label_validation,
        skip_label_count_validation,
    })
}

fn decode_timeseries_rw1(
    input: &[u8],
    range: Range<usize>,
    skip_exemplars: bool,
    scratch: &mut WriteRequestScratch,
) -> Result<TimeSeriesMeta, DecodeError> {
    let labels_start = scratch.labels.len();
    let samples_start = scratch.samples.len();
    let exemplars_start = scratch.exemplars.len();
    let histograms_start = scratch.histograms.len();
    let mut created_timestamp = 0i64;

    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key(input, &mut pos)?;
        match field_num {
            1 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                let pair = decode_label_pair(input, sub)?;
                scratch.labels.push(pair);
            }
            2 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 2,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                let sample = decode_sample(input, sub)?;
                scratch.samples.push(sample);
            }
            3 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 3,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                if !skip_exemplars {
                    let ex = decode_exemplar_rw1(input, sub, scratch)?;
                    scratch.exemplars.push(ex);
                }
            }
            4 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 4,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                let hist = decode_histogram(input, sub, scratch)?;
                scratch.histograms.push(hist);
            }
            6 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 6,
                        wire_type,
                    });
                }
                created_timestamp = read_varint_bounded(input, &mut pos, end)? as i64;
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }

    Ok(TimeSeriesMeta {
        labels: labels_start..scratch.labels.len(),
        samples: samples_start..scratch.samples.len(),
        exemplars: exemplars_start..scratch.exemplars.len(),
        histograms: histograms_start..scratch.histograms.len(),
        created_timestamp,
    })
}

fn decode_timeseries_rw2(
    input: &[u8],
    range: Range<usize>,
    symbol_offset: u32,
    common_symbols: &[&str],
    opts: &DecodeOptions<'_>,
    scratch: &mut WriteRequestScratch,
) -> Result<TimeSeriesMeta, DecodeError> {
    let labels_start = scratch.labels.len();
    let samples_start = scratch.samples.len();
    let exemplars_start = scratch.exemplars.len();
    let histograms_start = scratch.histograms.len();
    let mut created_timestamp = 0i64;
    let mut metric_name_ref: SliceRef = SliceRef::Empty;

    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key(input, &mut pos)?;
        match field_num {
            1 => {
                scratch.rw2_label_refs.clear();
                if wire_type == 0 {
                    // Single unpacked varint means odd number of refs (matching Mimir compat_rw2.go).
                    let _ = read_varint_bounded(input, &mut pos, end)?;
                    return Err(DecodeError::OddNumberOfLabelRefs);
                } else if wire_type == 2 {
                    let packed = read_length_delimited_range_bounded(input, &mut pos, end)?;
                    let mut p = packed.start;
                    while p < packed.end {
                        let r = read_varint_bounded(input, &mut p, packed.end)? as u32;
                        scratch.rw2_label_refs.push(r);
                    }
                    if !scratch.rw2_label_refs.len().is_multiple_of(2) {
                        return Err(DecodeError::OddNumberOfLabelRefs);
                    }
                    let mut i = 0;
                    while i < scratch.rw2_label_refs.len() {
                        let name_idx = scratch.rw2_label_refs[i];
                        let val_idx = scratch.rw2_label_refs[i + 1];
                        let name = resolve_symbol_ref(
                            name_idx,
                            symbol_offset,
                            common_symbols,
                            &scratch.rw2_symbols,
                        )
                        .ok_or(DecodeError::InvalidLabelRef {
                            index: name_idx,
                            offset: symbol_offset,
                        })?;
                        let value = resolve_symbol_ref(
                            val_idx,
                            symbol_offset,
                            common_symbols,
                            &scratch.rw2_symbols,
                        )
                        .ok_or(DecodeError::InvalidLabelRef {
                            index: val_idx,
                            offset: symbol_offset,
                        })?;
                        if name.resolve(input, common_symbols) == b"__name__" {
                            metric_name_ref = value;
                        }
                        scratch.labels.push(LabelRefPair { name, value });
                        i += 2;
                    }
                } else {
                    return Err(DecodeError::InvalidWireType {
                        field: 1,
                        wire_type,
                    });
                }
            }
            2 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 2,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                let sample = decode_sample(input, sub)?;
                scratch.samples.push(sample);
            }
            3 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 3,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                let hist = decode_histogram(input, sub, scratch)?;
                scratch.histograms.push(hist);
            }
            4 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 4,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                if !opts.skip_unmarshaling_exemplars {
                    let ex =
                        decode_exemplar_rw2(input, sub, symbol_offset, common_symbols, scratch)?;
                    scratch.exemplars.push(ex);
                }
            }
            5 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 5,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                decode_metadata_rw2(
                    input,
                    sub,
                    symbol_offset,
                    common_symbols,
                    metric_name_ref,
                    opts,
                    scratch,
                )?;
            }
            6 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 6,
                        wire_type,
                    });
                }
                created_timestamp = read_varint_bounded(input, &mut pos, end)? as i64;
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }

    Ok(TimeSeriesMeta {
        labels: labels_start..scratch.labels.len(),
        samples: samples_start..scratch.samples.len(),
        exemplars: exemplars_start..scratch.exemplars.len(),
        histograms: histograms_start..scratch.histograms.len(),
        created_timestamp,
    })
}

fn decode_label_pair(input: &[u8], range: Range<usize>) -> Result<LabelRefPair, DecodeError> {
    let mut name = SliceRef::Empty;
    let mut value = SliceRef::Empty;
    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key(input, &mut pos)?;
        match field_num {
            1 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                name = SliceRef::Input {
                    start: sub.start as u32,
                    len: sub.len() as u32,
                };
            }
            2 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 2,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                value = SliceRef::Input {
                    start: sub.start as u32,
                    len: sub.len() as u32,
                };
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }
    Ok(LabelRefPair { name, value })
}

fn decode_sample(input: &[u8], range: Range<usize>) -> Result<cortexpb::Sample, DecodeError> {
    let mut value = 0.0f64;
    let mut timestamp_ms = 0i64;
    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key(input, &mut pos)?;
        match field_num {
            1 => {
                if wire_type != 1 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1,
                        wire_type,
                    });
                }
                value = read_f64_bounded(input, &mut pos, end)?;
            }
            2 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 2,
                        wire_type,
                    });
                }
                timestamp_ms = read_varint_bounded(input, &mut pos, end)? as i64;
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }
    Ok(cortexpb::Sample {
        value,
        timestamp_ms,
    })
}

fn decode_exemplar_rw1(
    input: &[u8],
    range: Range<usize>,
    scratch: &mut WriteRequestScratch,
) -> Result<ExemplarMeta, DecodeError> {
    let labels_start = scratch.exemplar_labels.len();
    let mut value = 0.0f64;
    let mut timestamp_ms = 0i64;
    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key(input, &mut pos)?;
        match field_num {
            1 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                let pair = decode_label_pair(input, sub)?;
                scratch.exemplar_labels.push(pair);
            }
            2 => {
                if wire_type != 1 {
                    return Err(DecodeError::InvalidWireType {
                        field: 2,
                        wire_type,
                    });
                }
                value = read_f64_bounded(input, &mut pos, end)?;
            }
            3 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 3,
                        wire_type,
                    });
                }
                timestamp_ms = read_varint_bounded(input, &mut pos, end)? as i64;
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }
    Ok(ExemplarMeta {
        labels: labels_start..scratch.exemplar_labels.len(),
        value,
        timestamp_ms,
    })
}

fn decode_exemplar_rw2(
    input: &[u8],
    range: Range<usize>,
    symbol_offset: u32,
    common_symbols: &[&str],
    scratch: &mut WriteRequestScratch,
) -> Result<ExemplarMeta, DecodeError> {
    let labels_start = scratch.exemplar_labels.len();
    let mut value = 0.0f64;
    let mut timestamp_ms = 0i64;
    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key(input, &mut pos)?;
        match field_num {
            1 => {
                scratch.rw2_label_refs.clear();
                if wire_type == 0 {
                    let _ = read_varint_bounded(input, &mut pos, end)?;
                    return Err(DecodeError::OddNumberOfExemplarLabelRefs);
                } else if wire_type == 2 {
                    let packed = read_length_delimited_range_bounded(input, &mut pos, end)?;
                    let mut p = packed.start;
                    while p < packed.end {
                        let r = read_varint_bounded(input, &mut p, packed.end)? as u32;
                        scratch.rw2_label_refs.push(r);
                    }
                    if !scratch.rw2_label_refs.len().is_multiple_of(2) {
                        return Err(DecodeError::OddNumberOfExemplarLabelRefs);
                    }
                    let mut i = 0;
                    while i < scratch.rw2_label_refs.len() {
                        let name_idx = scratch.rw2_label_refs[i];
                        let val_idx = scratch.rw2_label_refs[i + 1];
                        let name = resolve_symbol_ref(
                            name_idx,
                            symbol_offset,
                            common_symbols,
                            &scratch.rw2_symbols,
                        )
                        .ok_or(DecodeError::InvalidExemplarLabelRef {
                            index: name_idx,
                            offset: symbol_offset,
                        })?;
                        let val = resolve_symbol_ref(
                            val_idx,
                            symbol_offset,
                            common_symbols,
                            &scratch.rw2_symbols,
                        )
                        .ok_or(DecodeError::InvalidExemplarLabelRef {
                            index: val_idx,
                            offset: symbol_offset,
                        })?;
                        scratch
                            .exemplar_labels
                            .push(LabelRefPair { name, value: val });
                        i += 2;
                    }
                } else {
                    return Err(DecodeError::InvalidWireType {
                        field: 1,
                        wire_type,
                    });
                }
            }
            2 => {
                if wire_type != 1 {
                    return Err(DecodeError::InvalidWireType {
                        field: 2,
                        wire_type,
                    });
                }
                value = read_f64_bounded(input, &mut pos, end)?;
            }
            3 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 3,
                        wire_type,
                    });
                }
                timestamp_ms = read_varint_bounded(input, &mut pos, end)? as i64;
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }
    Ok(ExemplarMeta {
        labels: labels_start..scratch.exemplar_labels.len(),
        value,
        timestamp_ms,
    })
}

fn decode_histogram(
    input: &[u8],
    range: Range<usize>,
    scratch: &mut WriteRequestScratch,
) -> Result<HistogramMeta, DecodeError> {
    let mut count: Option<cortexpb::histogram::Count> = None;
    let mut sum = 0.0f64;
    let mut schema = 0i32;
    let mut zero_threshold = 0.0f64;
    let mut zero_count: Option<cortexpb::histogram::ZeroCount> = None;
    let mut reset_hint = 0i32;
    let mut timestamp = 0i64;

    // Note: spans/deltas/floats for negative/positive/custom might appear interleaved across
    // different fields, though in practice each field's items appear contiguously.
    // To guarantee contiguous slices even if fields are interleaved or split across messages,
    // wait: usually each repeated field in proto appears contiguously. If a field repeats later
    // in the same Histogram message after another field used the same pool vec, we should decode
    // cleanly. Let's check: spans are only tags 8 & 11; deltas are tags 9 & 12; floats are tags 10, 13, 16.
    // Using temporary small vecs or checking contiguity? Even simpler:
    // Record if another field wrote to the shared vec since our last write; in the 99.99% case
    // tags are ascending (8, 9, 10, 11, 12, 13, 14, 15, 16) so every repeated field IS contiguous!
    // If out-of-order/interleaved ever occurs, we can still handle it by only extending when
    // contiguous, or relocating if non-contiguous! Let's implement that cleanly in 5 lines.
    let mut neg_spans = scratch.spans.len()..scratch.spans.len();
    let mut pos_spans = scratch.spans.len()..scratch.spans.len();
    let mut neg_deltas = scratch.deltas.len()..scratch.deltas.len();
    let mut pos_deltas = scratch.deltas.len()..scratch.deltas.len();
    let mut neg_counts = scratch.floats.len()..scratch.floats.len();
    let mut pos_counts = scratch.floats.len()..scratch.floats.len();
    let mut custom_values = scratch.floats.len()..scratch.floats.len();

    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key(input, &mut pos)?;
        match field_num {
            1 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1,
                        wire_type,
                    });
                }
                count = Some(cortexpb::histogram::Count::CountInt(read_varint_bounded(
                    input, &mut pos, end,
                )?));
            }
            2 => {
                if wire_type != 1 {
                    return Err(DecodeError::InvalidWireType {
                        field: 2,
                        wire_type,
                    });
                }
                count = Some(cortexpb::histogram::Count::CountFloat(read_f64_bounded(
                    input, &mut pos, end,
                )?));
            }
            3 => {
                if wire_type != 1 {
                    return Err(DecodeError::InvalidWireType {
                        field: 3,
                        wire_type,
                    });
                }
                sum = read_f64_bounded(input, &mut pos, end)?;
            }
            4 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 4,
                        wire_type,
                    });
                }
                schema = zigzag_i32(read_varint_bounded(input, &mut pos, end)? as u32);
            }
            5 => {
                if wire_type != 1 {
                    return Err(DecodeError::InvalidWireType {
                        field: 5,
                        wire_type,
                    });
                }
                zero_threshold = read_f64_bounded(input, &mut pos, end)?;
            }
            6 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 6,
                        wire_type,
                    });
                }
                zero_count = Some(cortexpb::histogram::ZeroCount::ZeroCountInt(
                    read_varint_bounded(input, &mut pos, end)?,
                ));
            }
            7 => {
                if wire_type != 1 {
                    return Err(DecodeError::InvalidWireType {
                        field: 7,
                        wire_type,
                    });
                }
                zero_count = Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(
                    read_f64_bounded(input, &mut pos, end)?,
                ));
            }
            8 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 8,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                let span = decode_bucket_span(input, sub)?;
                push_contiguous(&mut scratch.spans, &mut neg_spans, span);
            }
            9 => {
                decode_sint64_repeated(
                    input,
                    &mut pos,
                    end,
                    wire_type,
                    9,
                    &mut scratch.deltas,
                    &mut neg_deltas,
                )?;
            }
            10 => {
                decode_f64_repeated(
                    input,
                    &mut pos,
                    end,
                    wire_type,
                    10,
                    &mut scratch.floats,
                    &mut neg_counts,
                )?;
            }
            11 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 11,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                let span = decode_bucket_span(input, sub)?;
                push_contiguous(&mut scratch.spans, &mut pos_spans, span);
            }
            12 => {
                decode_sint64_repeated(
                    input,
                    &mut pos,
                    end,
                    wire_type,
                    12,
                    &mut scratch.deltas,
                    &mut pos_deltas,
                )?;
            }
            13 => {
                decode_f64_repeated(
                    input,
                    &mut pos,
                    end,
                    wire_type,
                    13,
                    &mut scratch.floats,
                    &mut pos_counts,
                )?;
            }
            14 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 14,
                        wire_type,
                    });
                }
                reset_hint = read_varint_bounded(input, &mut pos, end)? as i32;
            }
            15 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 15,
                        wire_type,
                    });
                }
                timestamp = read_varint_bounded(input, &mut pos, end)? as i64;
            }
            16 => {
                decode_f64_repeated(
                    input,
                    &mut pos,
                    end,
                    wire_type,
                    16,
                    &mut scratch.floats,
                    &mut custom_values,
                )?;
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }

    Ok(HistogramMeta {
        count,
        sum,
        schema,
        zero_threshold,
        zero_count,
        negative_spans: neg_spans,
        negative_deltas: neg_deltas,
        negative_counts: neg_counts,
        positive_spans: pos_spans,
        positive_deltas: pos_deltas,
        positive_counts: pos_counts,
        reset_hint,
        timestamp,
        custom_values,
    })
}

fn push_contiguous<T: Copy>(vec: &mut Vec<T>, range: &mut Range<usize>, val: T) {
    if range.start == range.end {
        *range = vec.len()..vec.len() + 1;
        vec.push(val);
    } else if range.end == vec.len() {
        vec.push(val);
        range.end += 1;
    } else {
        // Relocate previous items to end of vec so the slice remains contiguous.
        let new_start = vec.len();
        vec.extend_from_within(range.clone());
        vec.push(val);
        *range = new_start..vec.len();
    }
}

fn prepare_contiguous_append<T: Copy>(vec: &mut Vec<T>, range: &mut Range<usize>) {
    if range.start == range.end {
        *range = vec.len()..vec.len();
    } else if range.end != vec.len() {
        let new_start = vec.len();
        vec.extend_from_within(range.clone());
        *range = new_start..vec.len();
    }
}

fn decode_sint64_repeated(
    input: &[u8],
    pos: &mut usize,
    end: usize,
    wire_type: u8,
    field: u32,
    vec: &mut Vec<i64>,
    range: &mut Range<usize>,
) -> Result<(), DecodeError> {
    prepare_contiguous_append(vec, range);
    if wire_type == 0 {
        let v = zigzag_i64(read_varint_bounded(input, pos, end)?);
        vec.push(v);
        range.end = vec.len();
        Ok(())
    } else if wire_type == 2 {
        let packed = read_length_delimited_range_bounded(input, pos, end)?;
        let mut p = packed.start;
        while p < packed.end {
            let v = zigzag_i64(read_varint_bounded(input, &mut p, packed.end)?);
            vec.push(v);
        }
        range.end = vec.len();
        Ok(())
    } else {
        Err(DecodeError::InvalidWireType { field, wire_type })
    }
}

fn decode_f64_repeated(
    input: &[u8],
    pos: &mut usize,
    end: usize,
    wire_type: u8,
    field: u32,
    vec: &mut Vec<f64>,
    range: &mut Range<usize>,
) -> Result<(), DecodeError> {
    prepare_contiguous_append(vec, range);
    if wire_type == 1 {
        let v = read_f64_bounded(input, pos, end)?;
        vec.push(v);
        range.end = vec.len();
        Ok(())
    } else if wire_type == 2 {
        let packed = read_length_delimited_range_bounded(input, pos, end)?;
        if packed.len() % 8 != 0 {
            return Err(DecodeError::UnexpectedEof);
        }
        let mut p = packed.start;
        while p < packed.end {
            let v = read_f64_bounded(input, &mut p, packed.end)?;
            vec.push(v);
        }
        range.end = vec.len();
        Ok(())
    } else {
        Err(DecodeError::InvalidWireType { field, wire_type })
    }
}

fn decode_bucket_span(
    input: &[u8],
    range: Range<usize>,
) -> Result<cortexpb::BucketSpan, DecodeError> {
    let mut offset = 0i32;
    let mut length = 0u32;
    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key(input, &mut pos)?;
        match field_num {
            1 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1,
                        wire_type,
                    });
                }
                offset = zigzag_i32(read_varint_bounded(input, &mut pos, end)? as u32);
            }
            2 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 2,
                        wire_type,
                    });
                }
                length = read_varint_bounded(input, &mut pos, end)? as u32;
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }
    Ok(cortexpb::BucketSpan { offset, length })
}

fn decode_metadata_rw1(input: &[u8], range: Range<usize>) -> Result<MetadataMeta, DecodeError> {
    let mut metric_type = 0i32;
    let mut family_name = SliceRef::Empty;
    let mut help = SliceRef::Empty;
    let mut unit = SliceRef::Empty;

    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key(input, &mut pos)?;
        match field_num {
            1 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1,
                        wire_type,
                    });
                }
                metric_type = read_varint_bounded(input, &mut pos, end)? as i32;
            }
            2 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 2,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                let _ =
                    std::str::from_utf8(&input[sub.clone()]).map_err(|_| DecodeError::InvalidUtf8)?;
                family_name = if sub.is_empty() {
                    SliceRef::Empty
                } else {
                    SliceRef::Input {
                        start: sub.start as u32,
                        len: sub.len() as u32,
                    }
                };
            }
            4 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 4,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                let _ =
                    std::str::from_utf8(&input[sub.clone()]).map_err(|_| DecodeError::InvalidUtf8)?;
                help = if sub.is_empty() {
                    SliceRef::Empty
                } else {
                    SliceRef::Input {
                        start: sub.start as u32,
                        len: sub.len() as u32,
                    }
                };
            }
            5 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 5,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                let _ =
                    std::str::from_utf8(&input[sub.clone()]).map_err(|_| DecodeError::InvalidUtf8)?;
                unit = if sub.is_empty() {
                    SliceRef::Empty
                } else {
                    SliceRef::Input {
                        start: sub.start as u32,
                        len: sub.len() as u32,
                    }
                };
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }

    Ok(MetadataMeta {
        metric_type,
        family_name,
        help,
        unit,
    })
}

fn decode_metadata_rw2(
    input: &[u8],
    range: Range<usize>,
    symbol_offset: u32,
    common_symbols: &[&str],
    metric_name_ref: SliceRef,
    opts: &DecodeOptions<'_>,
    scratch: &mut WriteRequestScratch,
) -> Result<(), DecodeError> {
    let mut metric_type = 0i32;
    let mut help = SliceRef::Empty;
    let mut unit = SliceRef::Empty;

    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key(input, &mut pos)?;
        match field_num {
            1 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1,
                        wire_type,
                    });
                }
                metric_type = read_varint_bounded(input, &mut pos, end)? as i32;
            }
            3 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 3,
                        wire_type,
                    });
                }
                let help_ref = read_varint_bounded(input, &mut pos, end)? as u32;
                help = resolve_symbol_ref(
                    help_ref,
                    symbol_offset,
                    common_symbols,
                    &scratch.rw2_symbols,
                )
                .ok_or(DecodeError::InvalidHelpRef {
                    index: help_ref,
                    offset: symbol_offset,
                })?;
            }
            4 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 4,
                        wire_type,
                    });
                }
                let unit_ref = read_varint_bounded(input, &mut pos, end)? as u32;
                unit = resolve_symbol_ref(
                    unit_ref,
                    symbol_offset,
                    common_symbols,
                    &scratch.rw2_symbols,
                )
                .ok_or(DecodeError::InvalidUnitRef {
                    index: unit_ref,
                    offset: symbol_offset,
                })?;
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }

    let series_name = metric_name_ref.resolve_str(input, common_symbols);
    let normalized_name = if opts.skip_normalize_metadata_metric_name {
        series_name
    } else {
        normalize_metric_name(series_name, metric_type)
    };
    if normalized_name.is_empty() {
        return Ok(());
    }
    if unit.is_empty() && help.is_empty() && metric_type == 0 {
        return Ok(());
    }

    let final_family_ref = truncate_slice_ref(metric_name_ref, normalized_name.len());

    if !opts.skip_deduplicate_metadata {
        let h = fnv64a(normalized_name.as_bytes());
        let bucket = scratch.dedup_hashes.entry(h).or_default();
        for &existing_idx in bucket.iter() {
            let existing = scratch.metadata[existing_idx];
            if resolve_str_any(existing.family_name, input, common_symbols) == normalized_name {
                return Ok(());
            }
        }
        let idx = scratch.metadata.len();
        bucket.push(idx);
    }

    scratch.metadata.push(MetadataMeta {
        metric_type,
        family_name: final_family_ref,
        help,
        unit,
    });
    Ok(())
}

#[inline]
fn truncate_slice_ref(r: SliceRef, new_len: usize) -> SliceRef {
    if new_len == 0 {
        return SliceRef::Empty;
    }
    match r {
        SliceRef::Empty => SliceRef::Empty,
        SliceRef::Input { start, len } => SliceRef::Input {
            start,
            len: (new_len as u32).min(len),
        },
        SliceRef::Common { idx, len } => SliceRef::Common {
            idx,
            len: (new_len as u32).min(len),
        },
    }
}

#[inline]
fn resolve_str_any<'a>(r: SliceRef, input: &'a [u8], common_symbols: &'a [&'a str]) -> &'a str {
    r.resolve_str(input, common_symbols)
}

#[inline]
fn fnv64a(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Ported from `normalizeMetricName` in `pkg/mimirpb/timeseries.go`.
pub fn normalize_metric_name(series_name: &str, metric_type: i32) -> &str {
    use cortexpb::metadata_rw2::MetricType;
    match MetricType::try_from(metric_type) {
        Ok(MetricType::Summary) => series_name
            .strip_suffix("_count")
            .or_else(|| series_name.strip_suffix("_sum"))
            .unwrap_or(series_name),
        Ok(MetricType::Histogram) => series_name
            .strip_suffix("_bucket")
            .or_else(|| series_name.strip_suffix("_count"))
            .or_else(|| series_name.strip_suffix("_sum"))
            .unwrap_or(series_name),
        Ok(MetricType::Gaugehistogram) => series_name
            .strip_suffix("_bucket")
            .or_else(|| series_name.strip_suffix("_gcount"))
            .or_else(|| series_name.strip_suffix("_gsum"))
            .unwrap_or(series_name),
        _ => series_name,
    }
}

#[inline]
fn resolve_symbol_ref(
    ref_id: u32,
    offset: u32,
    common_symbols: &[&str],
    rw2_symbols: &[SliceRef],
) -> Option<SliceRef> {
    if ref_id == 0 {
        return Some(SliceRef::Empty);
    }
    if ref_id < offset {
        if let Some(sym) = common_symbols.get(ref_id as usize) {
            return Some(SliceRef::Common {
                idx: ref_id,
                len: sym.len() as u32,
            });
        }
        return None;
    }
    let idx = (ref_id - offset) as usize;
    rw2_symbols.get(idx).copied()
}

#[inline]
fn read_key(input: &[u8], pos: &mut usize) -> Result<(u32, u8), DecodeError> {
    let wire = read_varint(input, pos)?;
    let field_num = (wire >> 3) as u32;
    let wire_type = (wire & 0x7) as u8;
    if wire_type == 4 || wire_type == 3 || wire_type > 5 {
        return Err(DecodeError::InvalidWireType {
            field: field_num,
            wire_type,
        });
    }
    if field_num == 0 {
        return Err(DecodeError::InvalidFieldNumber(field_num));
    }
    Ok((field_num, wire_type))
}

#[inline]
fn read_varint(input: &[u8], pos: &mut usize) -> Result<u64, DecodeError> {
    let mut val = 0u64;
    let mut shift = 0u32;
    loop {
        if *pos >= input.len() {
            return Err(DecodeError::UnexpectedEof);
        }
        let b = input[*pos];
        *pos += 1;
        if shift == 63 && b > 1 {
            return Err(DecodeError::VarintOverflow);
        }
        val |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            return Ok(val);
        }
        shift += 7;
        if shift >= 64 {
            return Err(DecodeError::VarintOverflow);
        }
    }
}

#[inline]
fn read_varint_bounded(input: &[u8], pos: &mut usize, end: usize) -> Result<u64, DecodeError> {
    let mut val = 0u64;
    let mut shift = 0u32;
    loop {
        if *pos >= end {
            return Err(DecodeError::UnexpectedEof);
        }
        let b = input[*pos];
        *pos += 1;
        if shift == 63 && b > 1 {
            return Err(DecodeError::VarintOverflow);
        }
        val |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            return Ok(val);
        }
        shift += 7;
        if shift >= 64 {
            return Err(DecodeError::VarintOverflow);
        }
    }
}

#[inline]
fn read_f64_bounded(input: &[u8], pos: &mut usize, end: usize) -> Result<f64, DecodeError> {
    let next = pos.checked_add(8).ok_or(DecodeError::UnexpectedEof)?;
    if next > end {
        return Err(DecodeError::UnexpectedEof);
    }
    let bytes: [u8; 8] = input[*pos..next]
        .try_into()
        .map_err(|_| DecodeError::UnexpectedEof)?;
    *pos = next;
    Ok(f64::from_le_bytes(bytes))
}

#[inline]
fn read_length_delimited_range(
    input: &[u8],
    pos: &mut usize,
) -> Result<Range<usize>, DecodeError> {
    read_length_delimited_range_bounded(input, pos, input.len())
}

#[inline]
fn read_length_delimited_range_bounded(
    input: &[u8],
    pos: &mut usize,
    end: usize,
) -> Result<Range<usize>, DecodeError> {
    let len = read_varint_bounded(input, pos, end)? as usize;
    let start = *pos;
    let next = start.checked_add(len).ok_or(DecodeError::UnexpectedEof)?;
    if next > end {
        return Err(DecodeError::UnexpectedEof);
    }
    *pos = next;
    Ok(start..next)
}

#[inline]
fn skip_field(input: &[u8], pos: &mut usize, wire_type: u8) -> Result<(), DecodeError> {
    skip_field_bounded(input, pos, input.len(), wire_type)
}

fn skip_field_bounded(
    input: &[u8],
    pos: &mut usize,
    end: usize,
    wire_type: u8,
) -> Result<(), DecodeError> {
    match wire_type {
        0 => {
            let _ = read_varint_bounded(input, pos, end)?;
            Ok(())
        }
        1 => {
            let next = pos.checked_add(8).ok_or(DecodeError::UnexpectedEof)?;
            if next > end {
                return Err(DecodeError::UnexpectedEof);
            }
            *pos = next;
            Ok(())
        }
        2 => {
            let _ = read_length_delimited_range_bounded(input, pos, end)?;
            Ok(())
        }
        5 => {
            let next = pos.checked_add(4).ok_or(DecodeError::UnexpectedEof)?;
            if next > end {
                return Err(DecodeError::UnexpectedEof);
            }
            *pos = next;
            Ok(())
        }
        _ => Err(DecodeError::InvalidWireType {
            field: 0,
            wire_type,
        }),
    }
}

#[inline]
fn zigzag_i32(n: u32) -> i32 {
    ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

#[inline]
fn zigzag_i64(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

/// Converts a classic RW1 `cortexpb::WriteRequest` into an RW2-encoded `cortexpb::WriteRequest`
/// (with `symbols_rw2` and `timeseries_rw2` populated), ported 1:1 from
/// `FromWriteRequestToRW2Request` in `pkg/mimirpb/prealloc_rw2.go`.
pub fn from_write_request_to_rw2(
    rw1: &cortexpb::WriteRequest,
    offset: u32,
    common_symbols: Option<&CommonSymbols>,
) -> Result<cortexpb::WriteRequest, EncodeRw2Error> {
    if !rw1.symbols_rw2.is_empty() || !rw1.timeseries_rw2.is_empty() {
        return Err(EncodeRw2Error::AlreadyRw2);
    }

    let mut symbols = FastSymbolsTable::new(100);
    symbols.configure_common_symbols(offset, common_symbols);

    let mut metadata_map: HashMap<&str, usize> = HashMap::new();
    let mut matched_metadata_mask: Vec<bool> = Vec::new();
    if !rw1.metadata.is_empty() && !rw1.timeseries.is_empty() {
        metadata_map.reserve(rw1.metadata.len());
        matched_metadata_mask.resize(rw1.metadata.len(), false);
        for (i, m) in rw1.metadata.iter().enumerate() {
            if let Some(&seen_idx) = metadata_map.get(m.metric_family_name.as_str()) {
                if rw1.metadata[i] == rw1.metadata[seen_idx] {
                    matched_metadata_mask[i] = true;
                }
            } else {
                metadata_map.insert(m.metric_family_name.as_str(), i);
            }
        }
    }

    let mut rw2_timeseries =
        Vec::with_capacity(rw1.timeseries.len() + rw1.metadata.len());

    for ts in &rw1.timeseries {
        let mut metric_name = "";
        let mut refs = Vec::with_capacity(ts.labels.len() * 2);
        for l in &ts.labels {
            let name = std::str::from_utf8(&l.name).unwrap_or("");
            let value = std::str::from_utf8(&l.value).unwrap_or("");
            if name == "__name__" {
                metric_name = value;
            }
            refs.push(symbols.symbolize(name));
            refs.push(symbols.symbolize(value));
        }

        let mut metadata = None;
        if !metadata_map.is_empty() {
            if let Some(&meta_idx) = metadata_map.get(metric_name) {
                metadata = Some(from_metric_metadata_to_metadata_rw2(
                    &rw1.metadata[meta_idx],
                    &mut symbols,
                ));
                matched_metadata_mask[meta_idx] = true;
            }
        }

        let exemplars = ts
            .exemplars
            .iter()
            .map(|ex| {
                let mut ex_refs = Vec::with_capacity(ex.labels.len() * 2);
                for l in &ex.labels {
                    let name = std::str::from_utf8(&l.name).unwrap_or("");
                    let value = std::str::from_utf8(&l.value).unwrap_or("");
                    ex_refs.push(symbols.symbolize(name));
                    ex_refs.push(symbols.symbolize(value));
                }
                cortexpb::ExemplarRw2 {
                    labels_refs: ex_refs,
                    value: ex.value,
                    timestamp: ex.timestamp_ms,
                }
            })
            .collect();

        rw2_timeseries.push(cortexpb::TimeSeriesRw2 {
            labels_refs: refs,
            samples: ts.samples.clone(),
            histograms: ts.histograms.clone(),
            exemplars,
            metadata,
            created_timestamp: ts.created_timestamp,
        });
    }

    for (i, m) in rw1.metadata.iter().enumerate() {
        if !matched_metadata_mask.is_empty() && matched_metadata_mask[i] {
            continue;
        }
        let labels_refs = vec![
            symbols.symbolize("__name__"),
            symbols.symbolize(&m.metric_family_name),
        ];
        let rw2_meta = from_metric_metadata_to_metadata_rw2(m, &mut symbols);
        rw2_timeseries.push(cortexpb::TimeSeriesRw2 {
            labels_refs,
            samples: Vec::new(),
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: Some(rw2_meta),
            created_timestamp: 0,
        });
    }

    let mut symbols_rw2 = Vec::with_capacity(symbols.count_symbols());
    symbols.symbols_into_drain(&mut symbols_rw2);

    Ok(cortexpb::WriteRequest {
        timeseries: Vec::new(),
        source: rw1.source,
        metadata: Vec::new(),
        symbols_rw2,
        timeseries_rw2: rw2_timeseries,
        skip_label_validation: rw1.skip_label_validation,
        skip_label_count_validation: rw1.skip_label_count_validation,
    })
}

fn from_metric_metadata_to_metadata_rw2(
    m: &cortexpb::MetricMetadata,
    symbols: &mut FastSymbolsTable<'_>,
) -> cortexpb::MetadataRw2 {
    cortexpb::MetadataRw2 {
        r#type: m.r#type,
        help_ref: symbols.symbolize(&m.help),
        unit_ref: symbols.symbolize(&m.unit),
    }
}

/// Converts a classic `cortexpb::WriteRequest` to RW2 (with optional `offset` / `common_symbols`)
/// and encodes it into `out`.
pub fn encode_write_request_rw2_bytes(
    rw1: &cortexpb::WriteRequest,
    offset: u32,
    common_symbols: Option<&CommonSymbols>,
    out: &mut Vec<u8>,
) -> Result<(), EncodeRw2Error> {
    let rw2 = from_write_request_to_rw2(rw1, offset, common_symbols)?;
    out.clear();
    rw2.encode(out)
        .map_err(|e| EncodeRw2Error::Encode(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::symbols::v2_common_symbols;

    #[test]
    fn test_classic_and_rw2_roundtrip() {
        let rw1 = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "http_requests_total"),
                    cortexpb::LabelPair::from_static("job", "api"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 42.5,
                    timestamp_ms: 1_700_000_000_000,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![cortexpb::LabelPair::from_static("trace_id", "abc")],
                    value: 1.5,
                    timestamp_ms: 1_700_000_000_123,
                }],
                histograms: vec![cortexpb::Histogram {
                    sum: 10.0,
                    schema: -1,
                    zero_threshold: 0.001,
                    negative_spans: vec![cortexpb::BucketSpan {
                        offset: -2,
                        length: 1,
                    }],
                    negative_deltas: vec![3],
                    positive_spans: vec![cortexpb::BucketSpan {
                        offset: 0,
                        length: 2,
                    }],
                    positive_deltas: vec![5, -2],
                    reset_hint: cortexpb::histogram::ResetHint::Yes as i32,
                    timestamp: 1_700_000_000_000,
                    custom_values: vec![0.1, 0.5],
                    count: Some(cortexpb::histogram::Count::CountInt(9)),
                    zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(1)),
                    ..Default::default()
                }],
                created_timestamp: 1_699_999_000_000,
            }],
            source: cortexpb::write_request::SourceEnum::Rule as i32,
            metadata: vec![cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "http_requests_total".to_string(),
                help: "total requests".to_string(),
                unit: "requests".to_string(),
            }],
            symbols_rw2: vec![],
            timeseries_rw2: vec![],
            skip_label_validation: true,
            skip_label_count_validation: true,
        };

        let mut scratch = WriteRequestScratch::new();

        // Classic
        let classic_bytes = rw1.encode_to_vec();
        let decoded = decode_write_request(&classic_bytes, DecodeOptions::classic(), &mut scratch)
            .expect("classic decode");
        assert_eq!(decoded.to_owned_write_request(), rw1);

        // RW2
        let mut rw2_bytes = Vec::new();
        encode_write_request_rw2_bytes(&rw1, 0, None, &mut rw2_bytes).expect("rw2 encode");
        let decoded_rw2 = decode_write_request(&rw2_bytes, DecodeOptions::rw2(), &mut scratch)
            .expect("rw2 decode");
        assert_eq!(decoded_rw2.to_owned_write_request(), rw1);

        // Kafka v2
        let mut kv2_bytes = Vec::new();
        encode_write_request_rw2_bytes(
            &rw1,
            V2_RECORD_SYMBOL_OFFSET,
            Some(v2_common_symbols()),
            &mut kv2_bytes,
        )
        .expect("kafka-v2 encode");
        let decoded_kv2 =
            decode_write_request(&kv2_bytes, DecodeOptions::kafka_v2(), &mut scratch)
                .expect("kafka-v2 decode");
        assert_eq!(decoded_kv2.to_owned_write_request(), rw1);
    }

    #[test]
    fn test_rw2_validation_errors() {
        let mut scratch = WriteRequestScratch::new();
        let rw1_bytes = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries::default()],
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(
            decode_write_request(&rw1_bytes, DecodeOptions::rw2(), &mut scratch).err(),
            Some(DecodeError::UnexpectedRw1Timeseries)
        );

        let bad_first_sym = cortexpb::WriteRequest {
            symbols_rw2: vec!["not_empty".to_string()],
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(
            decode_write_request(&bad_first_sym, DecodeOptions::rw2(), &mut scratch).err(),
            Some(DecodeError::InvalidFirstSymbol)
        );

        let odd_refs = cortexpb::WriteRequest {
            symbols_rw2: vec!["".to_string(), "__name__".to_string()],
            timeseries_rw2: vec![cortexpb::TimeSeriesRw2 {
                labels_refs: vec![1],
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(
            decode_write_request(&odd_refs, DecodeOptions::rw2(), &mut scratch).err(),
            Some(DecodeError::OddNumberOfLabelRefs)
        );
    }

    #[test]
    fn test_float_histogram_and_special_floats_and_out_of_order_rw2() {
        // Prometheus StaleNaN: 0x7ff0000000000002
        let stale_nan = f64::from_bits(0x7ff0_0000_0000_0002);
        let qnan = f64::from_bits(0x7ff8_0000_0000_0001);
        let rw1 = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "req_latency_bucket"),
                    cortexpb::LabelPair::from_static("le", "0.5"),
                ],
                samples: vec![
                    cortexpb::Sample {
                        value: stale_nan,
                        timestamp_ms: 1000,
                    },
                    cortexpb::Sample {
                        value: f64::INFINITY,
                        timestamp_ms: 2000,
                    },
                    cortexpb::Sample {
                        value: f64::NEG_INFINITY,
                        timestamp_ms: 3000,
                    },
                    cortexpb::Sample {
                        value: qnan,
                        timestamp_ms: 4000,
                    },
                ],
                histograms: vec![cortexpb::Histogram {
                    sum: 42.25,
                    schema: 3,
                    zero_threshold: 1e-128,
                    negative_spans: vec![cortexpb::BucketSpan {
                        offset: -3,
                        length: 2,
                    }],
                    negative_counts: vec![1.5, 2.5],
                    positive_spans: vec![cortexpb::BucketSpan {
                        offset: 1,
                        length: 2,
                    }],
                    positive_counts: vec![3.25, 4.75],
                    reset_hint: cortexpb::histogram::ResetHint::Gauge as i32,
                    timestamp: 5000,
                    custom_values: vec![0.25, 1.0, 2.5],
                    count: Some(cortexpb::histogram::Count::CountFloat(12.5)),
                    zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(0.5)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            metadata: vec![cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                metric_family_name: "req_latency_bucket".to_string(),
                help: "latency".to_string(),
                unit: "seconds".to_string(),
            }],
            ..Default::default()
        };

        let mut scratch = WriteRequestScratch::new();
        for mode_opts in [
            ("classic", DecodeOptions::classic(), 0u32, None),
            ("rw2", DecodeOptions::rw2(), 0u32, None),
            (
                "kafka-v2",
                DecodeOptions::kafka_v2(),
                V2_RECORD_SYMBOL_OFFSET,
                Some(v2_common_symbols()),
            ),
        ] {
            let wire = if mode_opts.0 == "classic" {
                rw1.encode_to_vec()
            } else {
                let mut buf = Vec::new();
                encode_write_request_rw2_bytes(&rw1, mode_opts.2, mode_opts.3, &mut buf).unwrap();
                buf
            };
            let decoded = decode_write_request(&wire, mode_opts.1, &mut scratch).unwrap();
            let owned = decoded.to_owned_write_request();
            assert_eq!(owned.timeseries.len(), 1);
            let s = &owned.timeseries[0].samples;
            assert_eq!(s[0].value.to_bits(), stale_nan.to_bits());
            assert_eq!(s[1].value.to_bits(), f64::INFINITY.to_bits());
            assert_eq!(s[2].value.to_bits(), f64::NEG_INFINITY.to_bits());
            assert_eq!(s[3].value.to_bits(), qnan.to_bits());
            assert_eq!(owned.timeseries[0].histograms, rw1.timeseries[0].histograms);
        }

        // Test out-of-order RW2 fields: field 5 encoded BEFORE field 4.
        let rw2 = from_write_request_to_rw2(&rw1, 0, None).unwrap();
        let part_ts = cortexpb::WriteRequest {
            timeseries_rw2: rw2.timeseries_rw2.clone(),
            ..Default::default()
        }
        .encode_to_vec();
        let part_syms = cortexpb::WriteRequest {
            symbols_rw2: rw2.symbols_rw2.clone(),
            ..Default::default()
        }
        .encode_to_vec();
        let mut out_of_order = Vec::new();
        out_of_order.extend_from_slice(&part_ts);
        out_of_order.extend_from_slice(&part_syms);
        let decoded_ooo =
            decode_write_request(&out_of_order, DecodeOptions::rw2(), &mut scratch).unwrap();
        assert_eq!(decoded_ooo.timeseries_len(), 1);
        assert_eq!(decoded_ooo.metadata().len(), 1);
        assert_eq!(
            decoded_ooo.metadata().next().unwrap().metric_family_name,
            "req_latency"
        );
    }

    #[test]
    fn test_rw2_metadata_normalization_and_dedup() {
        let mut syms = FastSymbolsTable::new(16);
        let name_k = syms.symbolize("__name__");
        let name_v1 = syms.symbolize("http_duration_bucket");
        let name_v2 = syms.symbolize("http_duration_count");
        let help_r = syms.symbolize("help text");
        let unit_r = syms.symbolize("seconds");

        let req = cortexpb::WriteRequest {
            symbols_rw2: syms.symbols(),
            timeseries_rw2: vec![
                cortexpb::TimeSeriesRw2 {
                    labels_refs: vec![name_k, name_v1],
                    metadata: Some(cortexpb::MetadataRw2 {
                        r#type: cortexpb::metadata_rw2::MetricType::Histogram as i32,
                        help_ref: help_r,
                        unit_ref: unit_r,
                    }),
                    ..Default::default()
                },
                cortexpb::TimeSeriesRw2 {
                    labels_refs: vec![name_k, name_v2],
                    metadata: Some(cortexpb::MetadataRw2 {
                        r#type: cortexpb::metadata_rw2::MetricType::Histogram as i32,
                        help_ref: help_r,
                        unit_ref: unit_r,
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let wire = req.encode_to_vec();
        let mut scratch = WriteRequestScratch::new();
        let decoded = decode_write_request(&wire, DecodeOptions::rw2(), &mut scratch).unwrap();
        let metas: Vec<_> = decoded.metadata().collect();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].metric_family_name, "http_duration");
        assert_eq!(metas[0].help, "help text");
        assert_eq!(metas[0].unit, "seconds");

        // With skip_normalize and skip_deduplicate:
        let mut opts = DecodeOptions::rw2();
        opts.skip_normalize_metadata_metric_name = true;
        opts.skip_deduplicate_metadata = true;
        let decoded2 = decode_write_request(&wire, opts, &mut scratch).unwrap();
        let metas2: Vec<_> = decoded2.metadata().collect();
        assert_eq!(metas2.len(), 2);
        assert_eq!(metas2[0].metric_family_name, "http_duration_bucket");
        assert_eq!(metas2[1].metric_family_name, "http_duration_count");
    }
}
