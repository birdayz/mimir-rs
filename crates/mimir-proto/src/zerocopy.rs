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
    Scratch { start: u32, len: u32 },
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
            SliceRef::Scratch { .. } => &[],
        }
    }

    #[inline]
    fn resolve_with_arena<'a>(
        self,
        input: &'a [u8],
        common_symbols: &'a [&'a str],
        utf8_arena: &'a [u8],
    ) -> &'a [u8] {
        match self {
            SliceRef::Empty => &[],
            SliceRef::Input { start, len } => {
                let s = start as usize;
                let e = s + len as usize;
                &input[s..e]
            }
            SliceRef::Common { idx, len } => &common_symbols[idx as usize].as_bytes()[..len as usize],
            SliceRef::Scratch { start, len } => {
                let s = start as usize;
                let e = s + len as usize;
                &utf8_arena[s..e]
            }
        }
    }

    #[inline]
    fn resolve_str<'a>(
        self,
        input: &'a [u8],
        common_symbols: &'a [&'a str],
        utf8_arena: &'a [u8],
    ) -> &'a str {
        match self {
            SliceRef::Empty => "",
            SliceRef::Input { start, len } => {
                let s = start as usize;
                let e = s + len as usize;
                std::str::from_utf8(&input[s..e]).unwrap_or("")
            }
            SliceRef::Common { idx, len } => &common_symbols[idx as usize][..len as usize],
            SliceRef::Scratch { start, len } => {
                let s = start as usize;
                let e = s + len as usize;
                std::str::from_utf8(&utf8_arena[s..e]).unwrap_or("")
            }
        }
    }

    #[inline(always)]
    unsafe fn resolve_str_unchecked<'a>(
        self,
        input: &'a [u8],
        common_symbols: &'a [&'a str],
    ) -> &'a str {
        match self {
            SliceRef::Empty => "",
            SliceRef::Input { start, len } => {
                let s = start as usize;
                let e = s + len as usize;
                // SAFETY: caller guarantees `SliceRef::Input` was validated as UTF-8 on decode.
                unsafe { std::str::from_utf8_unchecked(&input[s..e]) }
            }
            SliceRef::Common { idx, len } => &common_symbols[idx as usize][..len as usize],
            SliceRef::Scratch { .. } => "",
        }
    }

    #[inline]
    fn is_empty(self) -> bool {
        matches!(self, SliceRef::Empty)
            || matches!(self, SliceRef::Input { len: 0, .. })
            || matches!(self, SliceRef::Common { len: 0, .. })
            || matches!(self, SliceRef::Scratch { len: 0, .. })
    }
}

#[inline]
fn push_utf8_or_lossy(
    bytes: &[u8],
    base_offset: usize,
    utf8_arena: &mut Vec<u8>,
) -> SliceRef {
    if bytes.is_empty() {
        return SliceRef::Empty;
    }
    if std::str::from_utf8(bytes).is_ok() {
        return SliceRef::Input {
            start: base_offset as u32,
            len: bytes.len() as u32,
        };
    }
    let lossy = String::from_utf8_lossy(bytes);
    let start = utf8_arena.len();
    utf8_arena.extend_from_slice(lossy.as_bytes());
    let len = utf8_arena.len() - start;
    SliceRef::Scratch {
        start: start as u32,
        len: len as u32,
    }
}

#[inline]
fn ensure_utf8_slice_ref(
    r: SliceRef,
    input: &[u8],
    utf8_arena: &mut Vec<u8>,
) -> SliceRef {
    match r {
        SliceRef::Input { start, len } => {
            let s = start as usize;
            let e = s + len as usize;
            push_utf8_or_lossy(&input[s..e], s, utf8_arena)
        }
        other => other,
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

/// Reusable flat resolved symbol table scratch for `decode_rw2_stream`.
#[derive(Debug, Default)]
pub struct Rw2ResolvedScratch {
    v2_resolved: Vec<Option<&'static str>>,
    custom_resolved: Vec<Option<&'static str>>,
    lossy_strings: Vec<Box<str>>,
}

impl Rw2ResolvedScratch {
    pub fn new() -> Self {
        let mut v2_resolved = Vec::with_capacity(256);
        v2_resolved.extend_from_slice(&V2_COMMON_SYMBOLS_64);
        Self {
            v2_resolved,
            custom_resolved: Vec::with_capacity(128),
            lossy_strings: Vec::new(),
        }
    }
}

const V2_COMMON_SYMBOLS_64: [Option<&'static str>; 64] = {
    let mut arr: [Option<&'static str>; 64] = [None; 64];
    let mut i = 0;
    while i < V2_COMMON_SYMBOLS_LIST.len() && i < 64 {
        arr[i] = Some(V2_COMMON_SYMBOLS_LIST[i]);
        i += 1;
    }
    arr
};

/// Header fields decoded by [`decode_rw2_stream`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamedRw2Header {
    pub source: i32,
    pub skip_label_validation: bool,
    pub skip_label_count_validation: bool,
    pub header_changed_after_series: bool,
}

/// Borrowed view of a single `TimeSeriesRw2` emitted by [`decode_rw2_stream`].
///
/// Labels are encoded directly into `label_bytes_arena` in Prometheus `stringlabels` flat byte
/// format, and samples are appended directly into `samples_arena`.
pub struct StreamedRw2Series<'v, 'a> {
    pub label_bytes_range: Range<usize>,
    pub encoded_labels: &'v [u8],
    pub labels_sorted_unique: bool,
    pub samples_range: Range<usize>,
    pub min_sample_ts: i64,
    pub created_timestamp: i64,
    pub histograms_range: Range<usize>,
    pub exemplars_range: Range<usize>,
    pub source: i32,
    pub skip_label_validation: bool,
    scratch: &'v WriteRequestScratch,
    input: &'a [u8],
    common_symbols: &'a [&'a str],
}

impl<'v, 'a> StreamedRw2Series<'v, 'a> {
    #[inline]
    pub fn histograms_len(&self) -> usize {
        self.histograms_range.len()
    }

    #[inline]
    pub fn exemplars_len(&self) -> usize {
        self.exemplars_range.len()
    }

    pub fn histograms(&self) -> impl ExactSizeIterator<Item = BorrowedHistogramView<'_>> {
        self.scratch.histograms[self.histograms_range.clone()]
            .iter()
            .map(move |h| BorrowedHistogramView {
                scratch: self.scratch,
                meta: h,
            })
    }

    pub fn exemplars(&self) -> impl ExactSizeIterator<Item = BorrowedExemplarView<'_, 'a>> {
        self.scratch.exemplars[self.exemplars_range.clone()]
            .iter()
            .map(move |ex| BorrowedExemplarView {
                input: self.input,
                common_symbols: self.common_symbols,
                scratch: self.scratch,
                meta: ex,
            })
    }
}

/// Header fields decoded by [`decode_classic_stream`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamedClassicHeader {
    pub source: i32,
    pub skip_label_validation: bool,
    pub skip_label_count_validation: bool,
    pub header_changed_after_series: bool,
}

/// Borrowed view of a single Classic `TimeSeries` emitted by [`decode_classic_stream`].
///
/// Labels are encoded directly into `label_bytes_arena` in Prometheus `stringlabels` flat byte
/// format, samples are appended directly into `samples_arena`, and `prom_hash` (`xxh64`) is
/// precomputed when labels are valid UTF-8 and sorted/unique.
pub struct StreamedClassicSeries<'v, 'a> {
    pub label_bytes_range: Range<usize>,
    pub encoded_labels: &'v [u8],
    pub prom_hash: u64,
    pub labels_sorted_unique: bool,
    pub bad_utf8: bool,
    pub samples_range: Range<usize>,
    pub min_sample_ts: i64,
    pub created_timestamp: i64,
    pub histograms_range: Range<usize>,
    pub exemplars_range: Range<usize>,
    pub source: i32,
    pub skip_label_validation: bool,
    scratch: &'v WriteRequestScratch,
    input: &'a [u8],
}

impl<'v, 'a> StreamedClassicSeries<'v, 'a> {
    #[inline]
    pub fn histograms_len(&self) -> usize {
        self.histograms_range.len()
    }

    #[inline]
    pub fn exemplars_len(&self) -> usize {
        self.exemplars_range.len()
    }

    pub fn histograms(&self) -> impl ExactSizeIterator<Item = BorrowedHistogramView<'_>> {
        self.scratch.histograms[self.histograms_range.clone()]
            .iter()
            .map(move |h| BorrowedHistogramView {
                scratch: self.scratch,
                meta: h,
            })
    }

    pub fn exemplars(&self) -> impl ExactSizeIterator<Item = BorrowedExemplarView<'_, 'a>> {
        self.scratch.exemplars[self.exemplars_range.clone()]
            .iter()
            .map(move |ex| BorrowedExemplarView {
                input: self.input,
                common_symbols: &[],
                scratch: self.scratch,
                meta: ex,
            })
    }
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
    utf8_arena: Vec<u8>,
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
            utf8_arena: Vec::new(),
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
        self.utf8_arena.clear();
    }

    #[inline]
    pub fn metadata_len(&self) -> usize {
        self.metadata.len()
    }

    pub fn metadata<'a>(&'a self, input: &'a [u8]) -> impl ExactSizeIterator<Item = BorrowedMetadata<'a>> + 'a {
        self.metadata.iter().map(move |m| BorrowedMetadata {
            metric_type: m.metric_type,
            metric_family_name: m
                .family_name
                .resolve_str(input, &[], &self.utf8_arena),
            help: m.help.resolve_str(input, &[], &self.utf8_arena),
            unit: m.unit.resolve_str(input, &[], &self.utf8_arena),
        })
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
    pub labels_utf8_validated: bool,
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
            labels_utf8_validated: self.labels_utf8_validated,
        }
    }

    pub fn metadata(&self) -> impl ExactSizeIterator<Item = BorrowedMetadata<'_>> + '_ {
        self.scratch.metadata.iter().map(move |m| BorrowedMetadata {
            metric_type: m.metric_type,
            metric_family_name: m.family_name.resolve_str(
                self.input,
                self.common_symbols,
                &self.scratch.utf8_arena,
            ),
            help: m
                .help
                .resolve_str(self.input, self.common_symbols, &self.scratch.utf8_arena),
            unit: m
                .unit
                .resolve_str(self.input, self.common_symbols, &self.scratch.utf8_arena),
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
    labels_utf8_validated: bool,
}

impl<'v, 'a> BorrowedTimeSeriesView<'v, 'a> {
    #[inline]
    pub fn is_labels_utf8_validated(&self) -> bool {
        self.labels_utf8_validated
    }

    /// Zero-validation `(&str, &str)` label iterator when decoded in RW2/KafkaV2 mode
    /// (all `symbols_rw2` and `common_symbols` were already UTF-8 validated on decode).
    /// Returns `None` in Classic RW1 mode where individual label pairs were not pre-validated.
    #[inline]
    pub fn labels_str(&self) -> Option<impl ExactSizeIterator<Item = (&'a str, &'a str)> + '_> {
        if !self.labels_utf8_validated {
            return None;
        }
        Some(
            self.scratch.labels[self.meta.labels.clone()]
                .iter()
                .map(move |pair| {
                    // SAFETY: in RW2/KafkaV2 mode, every `SliceRef` in `scratch.labels` comes from
                    // `SliceRef::Empty`, `common_symbols: &[&str]`, or `rw2_symbols` which were
                    // already validated via `std::str::from_utf8` during `decode_write_request`.
                    unsafe {
                        (
                            pair.name.resolve_str_unchecked(self.input, self.common_symbols),
                            pair.value.resolve_str_unchecked(self.input, self.common_symbols),
                        )
                    }
                }),
        )
    }

    pub fn labels(&self) -> impl ExactSizeIterator<Item = BorrowedLabel<'a>> + '_ {
        self.scratch.labels[self.meta.labels.clone()]
            .iter()
            .map(move |pair| BorrowedLabel {
                name: pair.name.resolve(self.input, self.common_symbols),
                value: pair.value.resolve(self.input, self.common_symbols),
            })
    }

    #[inline]
    pub fn samples(&self) -> &'v [cortexpb::Sample] {
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
    pub fn labels(&self) -> impl ExactSizeIterator<Item = BorrowedLabel<'_>> + '_ {
        self.scratch.exemplar_labels[self.meta.labels.clone()]
            .iter()
            .map(move |pair| BorrowedLabel {
                name: pair.name.resolve_with_arena(
                    self.input,
                    self.common_symbols,
                    &self.scratch.utf8_arena,
                ),
                value: pair.value.resolve_with_arena(
                    self.input,
                    self.common_symbols,
                    &self.scratch.utf8_arena,
                ),
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
    let mut all_symbols_utf8 = true;

    let mut pos = 0usize;
    let len = input.len();
    while pos < len {
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, len)?;
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
                let sub = read_length_delimited_range_bounded(input, &mut pos, len)?;
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
                let sub = read_length_delimited_range_bounded(input, &mut pos, len)?;
                let meta = decode_metadata_rw1(input, sub, scratch)?;
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
                let sub = read_length_delimited_range_bounded(input, &mut pos, len)?;
                if !prescanned_symbols {
                    if !seen_first_symbol && !sub.is_empty() {
                        return Err(DecodeError::InvalidFirstSymbol);
                    }
                    seen_first_symbol = true;
                    if std::str::from_utf8(&input[sub.clone()]).is_err() {
                        all_symbols_utf8 = false;
                    }
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
                let sub = read_length_delimited_range_bounded(input, &mut pos, len)?;
                if !seen_first_symbol && !prescanned_symbols {
                    // Handle rare out-of-order protobuf fields where field 5 appears before field 4.
                    prescanned_symbols = true;
                    let mut scan_pos = pos;
                    while scan_pos < len {
                        let (sf, sw) = read_key_bounded(input, &mut scan_pos, len)?;
                        if sf == 4 {
                            if sw != 2 {
                                return Err(DecodeError::InvalidWireType {
                                    field: 4,
                                    wire_type: sw,
                                });
                            }
                            let sym_range =
                                read_length_delimited_range_bounded(input, &mut scan_pos, len)?;
                            if !seen_first_symbol && !sym_range.is_empty() {
                                return Err(DecodeError::InvalidFirstSymbol);
                            }
                            seen_first_symbol = true;
                            if std::str::from_utf8(&input[sym_range.clone()]).is_err() {
                                all_symbols_utf8 = false;
                            }
                            scratch.rw2_symbols.push(if sym_range.is_empty() {
                                SliceRef::Empty
                            } else {
                                SliceRef::Input {
                                    start: sym_range.start as u32,
                                    len: sym_range.len() as u32,
                                }
                            });
                        } else {
                            skip_field_bounded(input, &mut scan_pos, len, sw)?;
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
                skip_field_bounded(input, &mut pos, len, wire_type)?;
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
        labels_utf8_validated: is_rw2 && all_symbols_utf8,
    })
}

#[inline(always)]
fn is_valid_utf8_fast(b: &[u8]) -> bool {
    b.is_ascii() || std::str::from_utf8(b).is_ok()
}

#[inline(always)]
fn decode_label_pair_slices<'a>(
    input: &'a [u8],
    range: Range<usize>,
) -> Result<(&'a [u8], &'a [u8]), DecodeError> {
    let mut name: &'a [u8] = &[];
    let mut value: &'a [u8] = &[];
    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
        match field_num {
            1 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                name = &input[sub];
            }
            2 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 2,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                value = &input[sub];
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }
    Ok((name, value))
}

#[cold]
#[inline(never)]
fn reemit_raw_classic_labels(
    input: &[u8],
    range: Range<usize>,
    label_bytes_arena: &mut Vec<u8>,
    label_bytes_start: usize,
) -> Result<(), DecodeError> {
    label_bytes_arena.truncate(label_bytes_start);
    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
        if field_num == 1 && wire_type == 2 {
            let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
            let (name, value) = decode_label_pair_slices(input, sub)?;
            append_stringlabel_pair_proto(label_bytes_arena, name, value)?;
        } else {
            skip_field_bounded(input, &mut pos, end, wire_type)?;
        }
    }
    Ok(())
}

/// Streams a Classic RW1 `cortexpb.WriteRequest` directly into contiguous
/// `stringlabels` bytes (`label_bytes_arena`), prehashed series metadata (`xxh64`), and
/// `samples_arena`, without intermediate `Vec<LabelRefPair>` allocations per series.
///
/// Decoded `MetricMetadata` entries are stored in `scratch` and can be iterated afterwards via
/// [`WriteRequestScratch::metadata`].
pub fn decode_classic_stream<'a, F>(
    input: &'a [u8],
    opts: DecodeOptions<'a>,
    scratch: &mut WriteRequestScratch,
    label_bytes_arena: &mut Vec<u8>,
    samples_arena: &mut Vec<cortexpb::Sample>,
    mut on_series: F,
) -> Result<StreamedClassicHeader, DecodeError>
where
    F: FnMut(StreamedClassicSeries<'_, 'a>),
{
    if !matches!(opts.mode, DecodeMode::Classic) {
        return Err(DecodeError::UnexpectedRw2Timeseries);
    }
    if input.len() > u32::MAX as usize {
        return Err(DecodeError::InputTooLarge);
    }
    scratch.clear();
    let label_base = label_bytes_arena.len();
    let samples_base = samples_arena.len();

    let mut source: i32 = 0;
    let mut skip_label_validation = false;
    let mut skip_label_count_validation = false;
    let mut emitted_any_series = false;
    let mut header_changed_after_series = false;

    let res: Result<(), DecodeError> = (|| {
        let mut pos = 0usize;
        let len = input.len();
        while pos < len {
            let (field_num, wire_type) = read_key_bounded(input, &mut pos, len)?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(DecodeError::InvalidWireType {
                            field: 1,
                            wire_type,
                        });
                    }
                    let sub = read_length_delimited_range_bounded(input, &mut pos, len)?;
                    let series = decode_timeseries_classic_stream(
                        input,
                        sub,
                        opts.skip_unmarshaling_exemplars,
                        scratch,
                        label_bytes_arena,
                        samples_arena,
                        source,
                        skip_label_validation,
                    )?;
                    emitted_any_series = true;
                    on_series(series);
                }
                2 => {
                    if wire_type != 0 {
                        return Err(DecodeError::InvalidWireType {
                            field: 2,
                            wire_type,
                        });
                    }
                    let new_src = read_varint(input, &mut pos)? as i32;
                    if emitted_any_series && new_src != source {
                        header_changed_after_series = true;
                    }
                    source = new_src;
                }
                3 => {
                    if wire_type != 2 {
                        return Err(DecodeError::InvalidWireType {
                            field: 3,
                            wire_type,
                        });
                    }
                    let sub = read_length_delimited_range_bounded(input, &mut pos, len)?;
                    let meta = decode_metadata_rw1(input, sub, scratch)?;
                    scratch.metadata.push(meta);
                }
                4 => return Err(DecodeError::UnexpectedRw2Symbols),
                5 => return Err(DecodeError::UnexpectedRw2Timeseries),
                1000 => {
                    if wire_type != 0 {
                        return Err(DecodeError::InvalidWireType {
                            field: 1000,
                            wire_type,
                        });
                    }
                    let val = read_varint(input, &mut pos)? != 0;
                    if emitted_any_series && val != skip_label_validation {
                        header_changed_after_series = true;
                    }
                    skip_label_validation = val;
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
                    skip_field_bounded(input, &mut pos, len, wire_type)?;
                }
            }
        }
        Ok(())
    })();

    if let Err(e) = res {
        label_bytes_arena.truncate(label_base);
        samples_arena.truncate(samples_base);
        return Err(e);
    }

    Ok(StreamedClassicHeader {
        source,
        skip_label_validation,
        skip_label_count_validation,
        header_changed_after_series,
    })
}

#[allow(clippy::too_many_arguments)]
fn decode_timeseries_classic_stream<'v, 'a>(
    input: &'a [u8],
    range: Range<usize>,
    skip_exemplars: bool,
    scratch: &'v mut WriteRequestScratch,
    label_bytes_arena: &'v mut Vec<u8>,
    samples_arena: &mut Vec<cortexpb::Sample>,
    source: i32,
    skip_label_validation: bool,
) -> Result<StreamedClassicSeries<'v, 'a>, DecodeError> {
    let label_bytes_start = label_bytes_arena.len();
    let samples_start = samples_arena.len();
    let exemplars_start = scratch.exemplars.len();
    let histograms_start = scratch.histograms.len();
    let mut created_timestamp = 0i64;
    let mut min_sample_ts = i64::MAX;
    let mut prev_name: Option<&'a [u8]> = None;
    let mut labels_sorted_unique = true;
    let mut bad_utf8 = false;
    let mut dropped_empty = false;

    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
        match field_num {
            1 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                let (name, value) = decode_label_pair_slices(input, sub)?;
                if !is_valid_utf8_fast(name) || !is_valid_utf8_fast(value) {
                    bad_utf8 = true;
                } else {
                    if let Some(pn) = prev_name {
                        if pn >= name {
                            labels_sorted_unique = false;
                        }
                    }
                    prev_name = Some(name);
                }
                if !value.is_empty() {
                    append_stringlabel_pair_proto(label_bytes_arena, name, value)?;
                } else {
                    dropped_empty = true;
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
                min_sample_ts = min_sample_ts.min(sample.timestamp_ms);
                samples_arena.push(sample);
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

    if !bad_utf8 && !labels_sorted_unique && dropped_empty {
        reemit_raw_classic_labels(input, range, label_bytes_arena, label_bytes_start)?;
    }

    let label_bytes_end = label_bytes_arena.len();
    let encoded_labels = &label_bytes_arena[label_bytes_start..label_bytes_end];
    let prom_hash = if !bad_utf8 && labels_sorted_unique && !encoded_labels.is_empty() {
        xxhash_rust::xxh64::xxh64(encoded_labels, 0)
    } else {
        0
    };

    Ok(StreamedClassicSeries {
        label_bytes_range: label_bytes_start..label_bytes_end,
        encoded_labels,
        prom_hash,
        labels_sorted_unique,
        bad_utf8,
        samples_range: samples_start..samples_arena.len(),
        min_sample_ts,
        created_timestamp,
        histograms_range: histograms_start..scratch.histograms.len(),
        exemplars_range: exemplars_start..scratch.exemplars.len(),
        source,
        skip_label_validation,
        scratch,
        input,
    })
}

struct ResolvedTableGuard<'r> {
    vec: &'r mut Vec<Option<&'static str>>,
    base_len: usize,
}

impl Drop for ResolvedTableGuard<'_> {
    fn drop(&mut self) {
        self.vec.truncate(self.base_len);
    }
}

#[inline(always)]
fn append_stringlabel_pair_proto(
    buf: &mut Vec<u8>,
    name: &[u8],
    value: &[u8],
) -> Result<(), DecodeError> {
    let klen = name.len();
    let vlen = value.len();
    if klen < 255 && vlen < 255 {
        let need = 2 + klen + vlen;
        buf.reserve(need);
        let start = buf.len();
        // SAFETY: `reserve(need)` ensures capacity >= start + need, and `klen < 255 && vlen < 255`.
        unsafe {
            let ptr = buf.as_mut_ptr().add(start);
            *ptr = klen as u8;
            std::ptr::copy_nonoverlapping(name.as_ptr(), ptr.add(1), klen);
            *ptr.add(1 + klen) = vlen as u8;
            std::ptr::copy_nonoverlapping(value.as_ptr(), ptr.add(2 + klen), vlen);
            buf.set_len(start + need);
        }
        Ok(())
    } else {
        append_stringlabel_pair_proto_slow(buf, name, value)
    }
}

#[cold]
#[inline(never)]
fn append_stringlabel_pair_proto_slow(
    buf: &mut Vec<u8>,
    name: &[u8],
    value: &[u8],
) -> Result<(), DecodeError> {
    for part in [name, value] {
        let len = part.len();
        if len < 255 {
            buf.push(len as u8);
        } else {
            if len >= (1 << 24) {
                return Err(DecodeError::InputTooLarge);
            }
            buf.push(255);
            buf.push((len & 0xff) as u8);
            buf.push(((len >> 8) & 0xff) as u8);
            buf.push(((len >> 16) & 0xff) as u8);
        }
        buf.extend_from_slice(part);
    }
    Ok(())
}

/// Streams an RW2 / Kafka-v2 `cortexpb.WriteRequest` directly into contiguous
/// `stringlabels` bytes (`label_bytes_arena`) and `samples_arena`, without allocating
/// `Vec<LabelRefPair>` or copying samples through `WriteRequestScratch`.
#[allow(clippy::too_many_arguments)]
pub fn decode_rw2_stream<'r, 'a: 'r, F>(
    input: &'a [u8],
    opts: DecodeOptions<'a>,
    scratch: &mut WriteRequestScratch,
    resolved_scratch: &'r mut Rw2ResolvedScratch,
    label_bytes_arena: &mut Vec<u8>,
    samples_arena: &mut Vec<cortexpb::Sample>,
    meta_out: &mut Vec<BorrowedMetadata<'r>>,
    mut on_series: F,
) -> Result<StreamedRw2Header, DecodeError>
where
    F: FnMut(StreamedRw2Series<'_, 'a>),
{
    if input.len() > u32::MAX as usize {
        return Err(DecodeError::InputTooLarge);
    }
    scratch.clear();
    meta_out.clear();

    let (rw2_offset, common_symbols, is_kafka_v2): (u32, &'a [&'a str], bool) = match opts.mode {
        DecodeMode::Classic => return Err(DecodeError::UnexpectedRw1Timeseries),
        DecodeMode::Rw2 => (0, &[], false),
        DecodeMode::KafkaV2 => (V2_RECORD_SYMBOL_OFFSET, V2_COMMON_SYMBOLS_LIST, true),
        DecodeMode::Rw2Custom {
            offset,
            common_symbols,
        } => (offset, common_symbols, false),
    };

    resolved_scratch.lossy_strings.clear();
    let lossy_strings = &mut resolved_scratch.lossy_strings;
    let guard = if is_kafka_v2 {
        if resolved_scratch.v2_resolved.len() < 64 {
            resolved_scratch.v2_resolved.clear();
            resolved_scratch
                .v2_resolved
                .extend_from_slice(&V2_COMMON_SYMBOLS_64);
        }
        ResolvedTableGuard {
            vec: &mut resolved_scratch.v2_resolved,
            base_len: 64,
        }
    } else {
        resolved_scratch.custom_resolved.clear();
        resolved_scratch
            .custom_resolved
            .resize(rw2_offset as usize, None);
        ResolvedTableGuard {
            vec: &mut resolved_scratch.custom_resolved,
            base_len: 0,
        }
    };

    // SAFETY: `guard` truncates `guard.vec` back to `base_len` (`'static` common symbols or empty)
    // on every return/unwind path before `'r` ends.
    let resolved: &mut Vec<Option<&'r str>> = unsafe {
        &mut *(guard.vec as *mut Vec<Option<&'static str>>).cast()
    };
    if !is_kafka_v2 {
        for (i, &sym) in common_symbols.iter().enumerate() {
            if i < resolved.len() {
                resolved[i] = Some(sym);
            }
        }
    }

    let mut source: i32 = 0;
    let mut skip_label_validation = false;
    let mut skip_label_count_validation = false;
    let mut seen_first_symbol = false;
    let mut prescanned_symbols = false;
    let mut emitted_any_series = false;
    let mut header_changed_after_series = false;

    let mut pos = 0usize;
    let len = input.len();
    while pos < len {
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, len)?;
        match field_num {
            1 => return Err(DecodeError::UnexpectedRw1Timeseries),
            2 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 2,
                        wire_type,
                    });
                }
                let new_src = read_varint(input, &mut pos)? as i32;
                if emitted_any_series && new_src != source {
                    header_changed_after_series = true;
                }
                source = new_src;
            }
            3 => return Err(DecodeError::UnexpectedRw1Metadata),
            4 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 4,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, len)?;
                if !prescanned_symbols {
                    let sym_str: &'r str = match std::str::from_utf8(&input[sub.clone()]) {
                        Ok(s) => {
                            if !seen_first_symbol && !s.is_empty() {
                                return Err(DecodeError::InvalidFirstSymbol);
                            }
                            s
                        }
                        Err(_) => {
                            if !seen_first_symbol && !sub.is_empty() {
                                return Err(DecodeError::InvalidFirstSymbol);
                            }
                            let lossy: Box<str> =
                                String::from_utf8_lossy(&input[sub]).into_owned().into_boxed_str();
                            lossy_strings.push(lossy);
                            let ptr: *const str = &**lossy_strings.last().unwrap();
                            // SAFETY: `Box<str>` heap allocation does not move when `Vec<Box<str>>`
                            // pushes, and `lossy_strings` lives in `resolved_scratch` for `'r`.
                            unsafe { &*ptr }
                        }
                    };
                    seen_first_symbol = true;
                    resolved.push(Some(sym_str));
                }
            }
            5 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 5,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, len)?;
                if !seen_first_symbol && !prescanned_symbols {
                    prescanned_symbols = true;
                    let mut scan_pos = pos;
                    while scan_pos < len {
                        let (sf, sw) = read_key_bounded(input, &mut scan_pos, len)?;
                        if sf == 4 {
                            if sw != 2 {
                                return Err(DecodeError::InvalidWireType {
                                    field: 4,
                                    wire_type: sw,
                                });
                            }
                            let sym_range =
                                read_length_delimited_range_bounded(input, &mut scan_pos, len)?;
                            let sym_str: &'r str =
                                match std::str::from_utf8(&input[sym_range.clone()]) {
                                    Ok(s) => {
                                        if !seen_first_symbol && !s.is_empty() {
                                            return Err(DecodeError::InvalidFirstSymbol);
                                        }
                                        s
                                    }
                                    Err(_) => {
                                        if !seen_first_symbol && !sym_range.is_empty() {
                                            return Err(DecodeError::InvalidFirstSymbol);
                                        }
                                        let lossy: Box<str> =
                                            String::from_utf8_lossy(&input[sym_range])
                                                .into_owned()
                                                .into_boxed_str();
                                        lossy_strings.push(lossy);
                                        let ptr: *const str = &**lossy_strings.last().unwrap();
                                        // SAFETY: see above.
                                        unsafe { &*ptr }
                                    }
                                };
                            seen_first_symbol = true;
                            resolved.push(Some(sym_str));
                        } else {
                            skip_field_bounded(input, &mut scan_pos, len, sw)?;
                        }
                    }
                    if resolved.is_empty() && rw2_offset == 0 {
                        resolved.push(Some(""));
                    }
                }
                let series = decode_timeseries_rw2_stream(
                    input,
                    sub,
                    rw2_offset,
                    common_symbols,
                    resolved,
                    &opts,
                    scratch,
                    label_bytes_arena,
                    samples_arena,
                    meta_out,
                    source,
                    skip_label_validation,
                )?;
                emitted_any_series = true;
                on_series(series);
            }
            1000 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 1000,
                        wire_type,
                    });
                }
                let val = read_varint(input, &mut pos)? != 0;
                if emitted_any_series && val != skip_label_validation {
                    header_changed_after_series = true;
                }
                skip_label_validation = val;
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
                skip_field_bounded(input, &mut pos, len, wire_type)?;
            }
        }
    }

    Ok(StreamedRw2Header {
        source,
        skip_label_validation,
        skip_label_count_validation,
        header_changed_after_series,
    })
}

#[allow(clippy::too_many_arguments)]
fn decode_timeseries_rw2_stream<'v, 'r, 'a: 'r>(
    input: &'a [u8],
    range: Range<usize>,
    symbol_offset: u32,
    common_symbols: &'a [&'a str],
    resolved: &[Option<&'r str>],
    opts: &DecodeOptions<'a>,
    scratch: &'v mut WriteRequestScratch,
    label_bytes_arena: &'v mut Vec<u8>,
    samples_arena: &mut Vec<cortexpb::Sample>,
    meta_out: &mut Vec<BorrowedMetadata<'r>>,
    source: i32,
    skip_label_validation: bool,
) -> Result<StreamedRw2Series<'v, 'a>, DecodeError> {
    let label_bytes_start = label_bytes_arena.len();
    let samples_start = samples_arena.len();
    let exemplars_start = scratch.exemplars.len();
    let histograms_start = scratch.histograms.len();
    let mut created_timestamp = 0i64;
    let mut min_sample_ts = i64::MAX;
    let mut metric_name: &'r str = "";
    let mut prev_name: Option<&'r str> = None;
    let mut labels_sorted_unique = true;

    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
        match field_num {
            1 => {
                if wire_type == 0 {
                    let _ = read_varint_bounded(input, &mut pos, end)?;
                    return Err(DecodeError::OddNumberOfLabelRefs);
                } else if wire_type == 2 {
                    let packed = read_length_delimited_range_bounded(input, &mut pos, end)?;
                    let mut p = packed.start;
                    label_bytes_arena
                        .reserve((packed.end - p).saturating_mul(16).clamp(64, 65536));
                    while p < packed.end {
                        let name_idx = read_varint_bounded(input, &mut p, packed.end)? as usize;
                        if p >= packed.end {
                            return Err(DecodeError::OddNumberOfLabelRefs);
                        }
                        let val_idx = read_varint_bounded(input, &mut p, packed.end)? as usize;
                        let k = *resolved
                            .get(name_idx)
                            .and_then(|o| o.as_ref())
                            .ok_or(DecodeError::InvalidLabelRef {
                                index: name_idx as u32,
                                offset: symbol_offset,
                            })?;
                        let v = *resolved
                            .get(val_idx)
                            .and_then(|o| o.as_ref())
                            .ok_or(DecodeError::InvalidLabelRef {
                                index: val_idx as u32,
                                offset: symbol_offset,
                            })?;
                        if k == "__name__" {
                            metric_name = v;
                        }
                        if let Some(pn) = prev_name {
                            if pn >= k {
                                labels_sorted_unique = false;
                            }
                        }
                        prev_name = Some(k);
                        if !v.is_empty() {
                            append_stringlabel_pair_proto(
                                label_bytes_arena,
                                k.as_bytes(),
                                v.as_bytes(),
                            )?;
                        }
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
                min_sample_ts = min_sample_ts.min(sample.timestamp_ms);
                samples_arena.push(sample);
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
                    let ex = decode_exemplar_rw2_resolved(
                        input,
                        sub,
                        symbol_offset,
                        resolved,
                        scratch,
                    )?;
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
                decode_metadata_rw2_stream(
                    input,
                    sub,
                    symbol_offset,
                    resolved,
                    metric_name,
                    opts,
                    scratch,
                    meta_out,
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

    let label_bytes_end = label_bytes_arena.len();
    let encoded_labels = &label_bytes_arena[label_bytes_start..label_bytes_end];
    Ok(StreamedRw2Series {
        label_bytes_range: label_bytes_start..label_bytes_end,
        encoded_labels,
        labels_sorted_unique,
        samples_range: samples_start..samples_arena.len(),
        min_sample_ts,
        created_timestamp,
        histograms_range: histograms_start..scratch.histograms.len(),
        exemplars_range: exemplars_start..scratch.exemplars.len(),
        source,
        skip_label_validation,
        scratch,
        input,
        common_symbols,
    })
}

#[inline]
fn str_to_slice_ref(
    s: &str,
    idx: usize,
    symbol_offset: u32,
    input: &[u8],
    utf8_arena: &mut Vec<u8>,
) -> SliceRef {
    if s.is_empty() {
        SliceRef::Empty
    } else if idx < symbol_offset as usize {
        SliceRef::Common {
            idx: idx as u32,
            len: s.len() as u32,
        }
    } else {
        let input_start = input.as_ptr() as usize;
        let input_end = input_start + input.len();
        let s_ptr = s.as_ptr() as usize;
        if s_ptr >= input_start && s_ptr + s.len() <= input_end {
            let start = s_ptr - input_start;
            SliceRef::Input {
                start: start as u32,
                len: s.len() as u32,
            }
        } else {
            let start = utf8_arena.len();
            utf8_arena.extend_from_slice(s.as_bytes());
            SliceRef::Scratch {
                start: start as u32,
                len: s.len() as u32,
            }
        }
    }
}

fn decode_exemplar_rw2_resolved<'a>(
    input: &'a [u8],
    range: Range<usize>,
    symbol_offset: u32,
    resolved: &[Option<&'a str>],
    scratch: &mut WriteRequestScratch,
) -> Result<ExemplarMeta, DecodeError> {
    let labels_start = scratch.exemplar_labels.len();
    let mut value = 0.0f64;
    let mut timestamp_ms = 0i64;
    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
        match field_num {
            1 => {
                if wire_type == 0 {
                    let _ = read_varint_bounded(input, &mut pos, end)?;
                    return Err(DecodeError::OddNumberOfExemplarLabelRefs);
                } else if wire_type == 2 {
                    let packed = read_length_delimited_range_bounded(input, &mut pos, end)?;
                    let mut p = packed.start;
                    while p < packed.end {
                        let name_idx = read_varint_bounded(input, &mut p, packed.end)? as usize;
                        if p >= packed.end {
                            return Err(DecodeError::OddNumberOfExemplarLabelRefs);
                        }
                        let val_idx = read_varint_bounded(input, &mut p, packed.end)? as usize;
                        let k = *resolved
                            .get(name_idx)
                            .and_then(|o| o.as_ref())
                            .ok_or(DecodeError::InvalidExemplarLabelRef {
                                index: name_idx as u32,
                                offset: symbol_offset,
                            })?;
                        let v = *resolved
                            .get(val_idx)
                            .and_then(|o| o.as_ref())
                            .ok_or(DecodeError::InvalidExemplarLabelRef {
                                index: val_idx as u32,
                                offset: symbol_offset,
                            })?;
                        let name_ref =
                            str_to_slice_ref(k, name_idx, symbol_offset, input, &mut scratch.utf8_arena);
                        let val_ref =
                            str_to_slice_ref(v, val_idx, symbol_offset, input, &mut scratch.utf8_arena);
                        scratch.exemplar_labels.push(LabelRefPair {
                            name: name_ref,
                            value: val_ref,
                        });
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

#[allow(clippy::too_many_arguments)]
fn decode_metadata_rw2_stream<'r>(
    input: &[u8],
    range: Range<usize>,
    symbol_offset: u32,
    resolved: &[Option<&'r str>],
    metric_name: &'r str,
    opts: &DecodeOptions<'_>,
    scratch: &mut WriteRequestScratch,
    meta_out: &mut Vec<BorrowedMetadata<'r>>,
) -> Result<(), DecodeError> {
    let mut metric_type = 0i32;
    let mut help: &'r str = "";
    let mut unit: &'r str = "";

    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
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
                let help_ref = read_varint_bounded(input, &mut pos, end)? as usize;
                help = *resolved
                    .get(help_ref)
                    .and_then(|o| o.as_ref())
                    .ok_or(DecodeError::InvalidHelpRef {
                        index: help_ref as u32,
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
                let unit_ref = read_varint_bounded(input, &mut pos, end)? as usize;
                unit = *resolved
                    .get(unit_ref)
                    .and_then(|o| o.as_ref())
                    .ok_or(DecodeError::InvalidUnitRef {
                        index: unit_ref as u32,
                        offset: symbol_offset,
                    })?;
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }

    let normalized_name = if opts.skip_normalize_metadata_metric_name {
        metric_name
    } else {
        normalize_metric_name(metric_name, metric_type)
    };
    if normalized_name.is_empty() {
        return Ok(());
    }
    if unit.is_empty() && help.is_empty() && metric_type == 0 {
        return Ok(());
    }

    if !opts.skip_deduplicate_metadata {
        let h = fnv64a(normalized_name.as_bytes());
        let bucket = scratch.dedup_hashes.entry(h).or_default();
        for &existing_idx in bucket.iter() {
            if meta_out[existing_idx].metric_family_name == normalized_name {
                return Ok(());
            }
        }
        let idx = meta_out.len();
        bucket.push(idx);
    }

    meta_out.push(BorrowedMetadata {
        metric_type,
        metric_family_name: normalized_name,
        help,
        unit,
    });
    Ok(())
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
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
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
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
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
                    scratch.labels.reserve(((packed.end - p) / 2).min(4096));
                    while p < packed.end {
                        let name_idx = read_varint_bounded(input, &mut p, packed.end)? as u32;
                        if p >= packed.end {
                            return Err(DecodeError::OddNumberOfLabelRefs);
                        }
                        let val_idx = read_varint_bounded(input, &mut p, packed.end)? as u32;
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
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
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

#[inline(always)]
fn decode_sample(input: &[u8], range: Range<usize>) -> Result<cortexpb::Sample, DecodeError> {
    let pos = range.start;
    let end = range.end;
    if end.saturating_sub(pos) >= 10 && input[pos] == 0x09 && input[pos + 9] == 0x10 {
        let bytes: [u8; 8] = input[pos + 1..pos + 9]
            .try_into()
            .map_err(|_| DecodeError::UnexpectedEof)?;
        let value = f64::from_le_bytes(bytes);
        let mut p = pos + 10;
        let ts = read_varint_bounded(input, &mut p, end)?;
        if p == end {
            return Ok(cortexpb::Sample {
                value,
                timestamp_ms: ts as i64,
            });
        }
    }
    decode_sample_slow(input, pos, end)
}

#[inline(never)]
fn decode_sample_slow(
    input: &[u8],
    mut pos: usize,
    end: usize,
) -> Result<cortexpb::Sample, DecodeError> {
    let mut value = 0.0f64;
    let mut timestamp_ms = 0i64;
    while pos < end {
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
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
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
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
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
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
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
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
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
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

fn decode_metadata_rw1(
    input: &[u8],
    range: Range<usize>,
    scratch: &mut WriteRequestScratch,
) -> Result<MetadataMeta, DecodeError> {
    let mut metric_type = 0i32;
    let mut family_name = SliceRef::Empty;
    let mut help = SliceRef::Empty;
    let mut unit = SliceRef::Empty;

    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
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
                family_name = push_utf8_or_lossy(
                    &input[sub.clone()],
                    sub.start,
                    &mut scratch.utf8_arena,
                );
            }
            4 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 4,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                help = push_utf8_or_lossy(
                    &input[sub.clone()],
                    sub.start,
                    &mut scratch.utf8_arena,
                );
            }
            5 => {
                if wire_type != 2 {
                    return Err(DecodeError::InvalidWireType {
                        field: 5,
                        wire_type,
                    });
                }
                let sub = read_length_delimited_range_bounded(input, &mut pos, end)?;
                unit = push_utf8_or_lossy(
                    &input[sub.clone()],
                    sub.start,
                    &mut scratch.utf8_arena,
                );
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
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
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
                help = ensure_utf8_slice_ref(help, input, &mut scratch.utf8_arena);
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
                unit = ensure_utf8_slice_ref(unit, input, &mut scratch.utf8_arena);
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }

    let utf8_metric_name_ref =
        ensure_utf8_slice_ref(metric_name_ref, input, &mut scratch.utf8_arena);
    let series_name = utf8_metric_name_ref.resolve_str(
        input,
        common_symbols,
        &scratch.utf8_arena,
    );
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

    let final_family_ref = truncate_slice_ref(utf8_metric_name_ref, normalized_name.len());

    if !opts.skip_deduplicate_metadata {
        let h = fnv64a(normalized_name.as_bytes());
        let bucket = scratch.dedup_hashes.entry(h).or_default();
        for &existing_idx in bucket.iter() {
            let existing = scratch.metadata[existing_idx];
            if resolve_str_any(
                existing.family_name,
                input,
                common_symbols,
                &scratch.utf8_arena,
            ) == normalized_name
            {
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
        SliceRef::Scratch { start, len } => SliceRef::Scratch {
            start,
            len: (new_len as u32).min(len),
        },
    }
}

#[inline]
fn resolve_str_any<'a>(
    r: SliceRef,
    input: &'a [u8],
    common_symbols: &'a [&'a str],
    utf8_arena: &'a [u8],
) -> &'a str {
    r.resolve_str(input, common_symbols, utf8_arena)
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

#[inline(always)]
fn read_key_bounded(input: &[u8], pos: &mut usize, end: usize) -> Result<(u32, u8), DecodeError> {
    let wire = read_varint_bounded(input, pos, end)?;
    let field_num = (wire >> 3) as u32;
    let wire_type = (wire & 0x7) as u8;
    if wire_type == 4 || wire_type > 5 {
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

#[inline(always)]
fn read_varint_bounded(input: &[u8], pos: &mut usize, end: usize) -> Result<u64, DecodeError> {
    let p = *pos;
    if p < end {
        let b0 = input[p];
        if b0 < 0x80 {
            *pos = p + 1;
            return Ok(b0 as u64);
        }
        if p + 1 < end {
            let b1 = input[p + 1];
            if b1 < 0x80 {
                *pos = p + 2;
                return Ok(((b0 & 0x7f) as u64) | ((b1 as u64) << 7));
            }
        }
        return read_varint_bounded_slow(input, pos, end);
    }
    Err(DecodeError::UnexpectedEof)
}

#[inline(never)]
fn read_varint_bounded_slow(
    input: &[u8],
    pos: &mut usize,
    end: usize,
) -> Result<u64, DecodeError> {
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

#[inline(always)]
fn read_f64_bounded(input: &[u8], pos: &mut usize, end: usize) -> Result<f64, DecodeError> {
    let p = *pos;
    if end.checked_sub(p).is_none_or(|avail| avail < 8) {
        return Err(DecodeError::UnexpectedEof);
    }
    let next = p + 8;
    let bytes: [u8; 8] = input[p..next]
        .try_into()
        .map_err(|_| DecodeError::UnexpectedEof)?;
    *pos = next;
    Ok(f64::from_le_bytes(bytes))
}

#[allow(dead_code)]
#[inline(always)]
fn read_fixed32_bounded(input: &[u8], pos: &mut usize, end: usize) -> Result<u32, DecodeError> {
    let p = *pos;
    if end.checked_sub(p).is_none_or(|avail| avail < 4) {
        return Err(DecodeError::UnexpectedEof);
    }
    let next = p + 4;
    let bytes: [u8; 4] = input[p..next]
        .try_into()
        .map_err(|_| DecodeError::UnexpectedEof)?;
    *pos = next;
    Ok(u32::from_le_bytes(bytes))
}

#[inline(always)]
fn read_length_delimited_range_bounded(
    input: &[u8],
    pos: &mut usize,
    end: usize,
) -> Result<Range<usize>, DecodeError> {
    let len = read_varint_bounded(input, pos, end)?;
    let start = *pos;
    let avail = end.checked_sub(start).ok_or(DecodeError::UnexpectedEof)?;
    let len_usize = usize::try_from(len).map_err(|_| DecodeError::UnexpectedEof)?;
    if len_usize > avail {
        return Err(DecodeError::UnexpectedEof);
    }
    let next = start + len_usize;
    *pos = next;
    Ok(start..next)
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
            if end.checked_sub(*pos).is_none_or(|avail| avail < 8) {
                return Err(DecodeError::UnexpectedEof);
            }
            *pos += 8;
            Ok(())
        }
        2 => {
            let _ = read_length_delimited_range_bounded(input, pos, end)?;
            Ok(())
        }
        3 => {
            // Protobuf StartGroup: skip nested fields until matching EndGroup (wire_type 4).
            let mut depth = 1usize;
            while depth > 0 {
                let wire = read_varint_bounded(input, pos, end)?;
                let wt = (wire & 0x7) as u8;
                match wt {
                    0 => {
                        let _ = read_varint_bounded(input, pos, end)?;
                    }
                    1 => {
                        if end.checked_sub(*pos).is_none_or(|avail| avail < 8) {
                            return Err(DecodeError::UnexpectedEof);
                        }
                        *pos += 8;
                    }
                    2 => {
                        let _ = read_length_delimited_range_bounded(input, pos, end)?;
                    }
                    3 => {
                        depth = depth.checked_add(1).ok_or(DecodeError::UnexpectedEof)?;
                        if depth > 100 {
                            return Err(DecodeError::InvalidWireType {
                                field: (wire >> 3) as u32,
                                wire_type: wt,
                            });
                        }
                    }
                    4 => {
                        depth -= 1;
                    }
                    5 => {
                        if end.checked_sub(*pos).is_none_or(|avail| avail < 4) {
                            return Err(DecodeError::UnexpectedEof);
                        }
                        *pos += 4;
                    }
                    _ => {
                        return Err(DecodeError::InvalidWireType {
                            field: (wire >> 3) as u32,
                            wire_type: wt,
                        });
                    }
                }
            }
            Ok(())
        }
        5 => {
            if end.checked_sub(*pos).is_none_or(|avail| avail < 4) {
                return Err(DecodeError::UnexpectedEof);
            }
            *pos += 4;
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

    #[test]
    fn test_labels_str_rw2_and_classic() {
        let rw1 = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair {
                        name: bytes::Bytes::from_static(b"__name__"),
                        value: bytes::Bytes::from_static(b"up"),
                    },
                    cortexpb::LabelPair {
                        name: bytes::Bytes::from_static(b"job"),
                        value: bytes::Bytes::from_static(b"api"),
                    },
                ],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: 1000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let mut scratch = WriteRequestScratch::new();
        let rw1_wire = rw1.encode_to_vec();
        let dec1 = decode_write_request(&rw1_wire, DecodeOptions::classic(), &mut scratch).unwrap();
        assert!(!dec1.labels_utf8_validated);
        assert!(dec1.timeseries_at(0).labels_str().is_none());

        let mut rw2_wire = Vec::new();
        encode_write_request_rw2_bytes(
            &rw1,
            V2_RECORD_SYMBOL_OFFSET,
            Some(v2_common_symbols()),
            &mut rw2_wire,
        )
        .unwrap();
        let dec2 =
            decode_write_request(&rw2_wire, DecodeOptions::kafka_v2(), &mut scratch).unwrap();
        assert!(dec2.labels_utf8_validated);
        let pairs: Vec<_> = dec2.timeseries_at(0).labels_str().unwrap().collect();
        assert_eq!(pairs, vec![("__name__", "up"), ("job", "api")]);
    }

    #[test]
    fn test_decode_rw2_stream_matches_borrowed_decode() {
        let rw1 = cortexpb::WriteRequest {
            timeseries: vec![
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "http_requests_total"),
                        cortexpb::LabelPair::from_static("empty", ""),
                        cortexpb::LabelPair::from_static("job", "api"),
                    ],
                    samples: vec![
                        cortexpb::Sample {
                            value: 1.5,
                            timestamp_ms: 1_700_000_000_000,
                        },
                        cortexpb::Sample {
                            value: 2.5,
                            timestamp_ms: 1_700_000_015_000,
                        },
                    ],
                    exemplars: vec![cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_static("trace_id", "xyz")],
                        value: 0.75,
                        timestamp_ms: 1_700_000_015_000,
                    }],
                    created_timestamp: 1_699_999_000_000,
                    ..Default::default()
                },
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("z", "1"),
                        cortexpb::LabelPair::from_static("a", "2"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 3.0,
                        timestamp_ms: 1_700_000_000_500,
                    }],
                    ..Default::default()
                },
            ],
            source: cortexpb::write_request::SourceEnum::Rule as i32,
            metadata: vec![cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "http_requests_total".to_string(),
                help: "total".to_string(),
                unit: "requests".to_string(),
            }],
            skip_label_validation: true,
            ..Default::default()
        };

        let mut kv2_wire = Vec::new();
        encode_write_request_rw2_bytes(
            &rw1,
            V2_RECORD_SYMBOL_OFFSET,
            Some(v2_common_symbols()),
            &mut kv2_wire,
        )
        .unwrap();

        let mut scratch = WriteRequestScratch::new();
        let mut resolved_scratch = Rw2ResolvedScratch::new();
        let mut label_bytes_arena = Vec::new();
        let mut samples_arena = Vec::new();
        let mut meta_out = Vec::new();
        let mut series_meta = Vec::new();

        let hdr = decode_rw2_stream(
            &kv2_wire,
            DecodeOptions::kafka_v2(),
            &mut scratch,
            &mut resolved_scratch,
            &mut label_bytes_arena,
            &mut samples_arena,
            &mut meta_out,
            |s| {
                series_meta.push((
                    s.label_bytes_range.clone(),
                    s.labels_sorted_unique,
                    s.samples_range.clone(),
                    s.min_sample_ts,
                    s.created_timestamp,
                    s.exemplars_len(),
                ));
            },
        )
        .unwrap();

        assert_eq!(hdr.source, cortexpb::write_request::SourceEnum::Rule as i32);
        assert!(hdr.skip_label_validation);
        assert_eq!(series_meta.len(), 2);
        assert!(series_meta[0].1); // sorted
        assert!(!series_meta[1].1); // unsorted ("z", "a")
        assert_eq!(series_meta[0].3, 1_700_000_000_000);
        assert_eq!(series_meta[0].4, 1_699_999_000_000);
        assert_eq!(series_meta[0].5, 1);
        assert_eq!(
            &samples_arena[series_meta[0].2.clone()],
            &rw1.timeseries[0].samples[..]
        );
        assert_eq!(meta_out.len(), 1);
        assert_eq!(meta_out[0].metric_family_name, "http_requests_total");
    }

    #[test]
    fn test_malformed_protobuf_poc_and_fuzz_no_panic() {
        let mut scratch = WriteRequestScratch::new();
        let mut resolved = Rw2ResolvedScratch::new();
        let mut label_bytes = Vec::new();
        let mut samples = Vec::new();
        let mut meta = Vec::new();

        // Exact PoC that previously caused `end - p` usize underflow panic:
        let poc = [0x0a, 0x04, 0x12, 0x01, 0x89, 0x00];
        assert!(decode_write_request(&poc, DecodeOptions::classic(), &mut scratch).is_err());
        assert!(decode_write_request(&poc, DecodeOptions::rw2(), &mut scratch).is_err());
        assert!(decode_rw2_stream(
            &poc,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta,
            |_| {},
        )
        .is_err());

        // StartGroup / EndGroup wire types: unknown StartGroup(field 15) -> skipped cleanly,
        // stray EndGroup -> Err(InvalidWireType) without panic.
        let mut with_group = vec![
            (15 << 3) | 3, // field 15, StartGroup
            1 << 3,        // nested varint (wire type 0)
            42,
            (15 << 3) | 4, // field 15, EndGroup
        ];
        let classic = cortexpb::WriteRequest {
            source: 7,
            ..Default::default()
        }
        .encode_to_vec();
        with_group.extend_from_slice(&classic);
        let dec = decode_write_request(&with_group, DecodeOptions::classic(), &mut scratch)
            .expect("unknown group should skip cleanly");
        assert_eq!(dec.source, 7);

        let stray_end_group = [(15 << 3) | 4];
        assert!(
            decode_write_request(&stray_end_group, DecodeOptions::classic(), &mut scratch).is_err()
        );

        // Deterministic PRNG across 1,000 random/corrupted slices
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next_u8 = || -> u8 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xff) as u8
        };

        for iter in 0..1000usize {
            let len = (iter % 97) + 1;
            let mut buf = Vec::with_capacity(len);
            for _ in 0..len {
                buf.push(next_u8());
            }
            let _ = decode_write_request(&buf, DecodeOptions::classic(), &mut scratch);
            let _ = decode_write_request(&buf, DecodeOptions::rw2(), &mut scratch);
            let _ = decode_write_request(&buf, DecodeOptions::kafka_v2(), &mut scratch);
            label_bytes.clear();
            samples.clear();
            let mut meta_iter = Vec::new();
            let _ = decode_rw2_stream(
                &buf,
                DecodeOptions::rw2(),
                &mut scratch,
                &mut resolved,
                &mut label_bytes,
                &mut samples,
                &mut meta_iter,
                |_| {},
            );
            label_bytes.clear();
            samples.clear();
            let mut meta_iter2 = Vec::new();
            let _ = decode_rw2_stream(
                &buf,
                DecodeOptions::kafka_v2(),
                &mut scratch,
                &mut resolved,
                &mut label_bytes,
                &mut samples,
                &mut meta_iter2,
                |_| {},
            );
        }
    }

    #[test]
    fn test_invalid_utf8_and_duplicate_metric_name_rw2() {
        let mut scratch = WriteRequestScratch::new();
        let mut resolved = Rw2ResolvedScratch::new();
        let mut label_bytes = Vec::new();
        let mut samples = Vec::new();
        let mut meta = Vec::new();

        // RW1 metadata with invalid UTF-8 bytes in HELP / UNIT
        // Field 3 (metadata): field 1=1(Counter), field 2="up", field 4=b"bad\xffhelp", field 5=b"u\xfe"
        let mut meta_msg = Vec::new();
        meta_msg.extend_from_slice(&[0x08, 0x01]); // type = 1
        meta_msg.extend_from_slice(&[0x12, 0x02, b'u', b'p']); // family_name = "up"
        meta_msg.extend_from_slice(&[0x22, 0x08, b'b', b'a', b'd', 0xff, b'h', b'e', b'l', b'p']);
        meta_msg.extend_from_slice(&[0x2a, 0x02, b'u', 0xfe]);
        let mut rw1_wire = Vec::new();
        rw1_wire.push(0x1a); // field 3, len-delimited
        rw1_wire.push(meta_msg.len() as u8);
        rw1_wire.extend_from_slice(&meta_msg);
        let dec1 = decode_write_request(&rw1_wire, DecodeOptions::classic(), &mut scratch)
            .expect("classic decode should tolerate invalid UTF-8 in HELP/UNIT");
        let ms: Vec<_> = dec1.metadata().collect();
        assert_eq!(ms.len(), 1);
        assert_eq!(ms[0].metric_family_name, "up");
        assert!(ms[0].help.contains('\u{FFFD}'));
        assert!(ms[0].unit.contains('\u{FFFD}'));

        // RW2 with duplicate __name__ (last __name__ wins) + invalid UTF-8 symbol
        // symbols: ["", "__name__", "first_name", "last_name", b"help\xff"]
        let mut rw2_wire = Vec::new();
        let syms: [&[u8]; 5] = [b"", b"__name__", b"first_name", b"last_name", b"help\xff"];
        for s in syms {
            rw2_wire.push(0x22); // field 4, wire 2
            rw2_wire.push(s.len() as u8);
            rw2_wire.extend_from_slice(s);
        }
        // TimeSeriesRw2 (field 5): labels_refs=[1, 2, 1, 3], metadata={type: 1, help_ref: 4}
        let mut ts_msg = Vec::new();
        ts_msg.extend_from_slice(&[0x0a, 0x04, 1, 2, 1, 3]); // labels_refs
        ts_msg.extend_from_slice(&[0x2a, 0x04, 0x08, 0x01, 0x18, 0x04]); // metadata
        rw2_wire.push(0x2a); // field 5, wire 2
        rw2_wire.push(ts_msg.len() as u8);
        rw2_wire.extend_from_slice(&ts_msg);

        let dec2 = decode_write_request(&rw2_wire, DecodeOptions::rw2(), &mut scratch)
            .expect("rw2 decode should tolerate invalid UTF-8");
        assert!(!dec2.labels_utf8_validated);
        assert!(dec2.timeseries_at(0).labels_str().is_none());
        let ms2: Vec<_> = dec2.metadata().collect();
        assert_eq!(ms2.len(), 1);
        assert_eq!(ms2[0].metric_family_name, "last_name");
        assert_eq!(ms2[0].help, "help\u{FFFD}");

        let mut emitted = 0usize;
        decode_rw2_stream(
            &rw2_wire,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta,
            |_| emitted += 1,
        )
        .expect("decode_rw2_stream should tolerate invalid UTF-8");
        assert_eq!(emitted, 1);
        assert_eq!(meta.len(), 1);
        assert_eq!(meta[0].metric_family_name, "last_name");
        assert_eq!(meta[0].help, "help\u{FFFD}");

        // Multiple invalid-UTF8 symbols force lossy_strings Vec<Box<str>> to reallocate
        // while &*ptr references remain held in resolved.
        let mut multi_lossy_wire = Vec::new();
        for sym in [b"".as_slice(), b"__name__".as_slice()] {
            multi_lossy_wire.push(0x22);
            multi_lossy_wire.push(sym.len() as u8);
            multi_lossy_wire.extend_from_slice(sym);
        }
        for i in 0..10u8 {
            let s = [b'h', b'0' + i, 0xff, 0xfe];
            multi_lossy_wire.push(0x22);
            multi_lossy_wire.push(s.len() as u8);
            multi_lossy_wire.extend_from_slice(&s);
        }
        let mut ts_multi = Vec::new();
        // labels_refs: [1 ("__name__"), 2 ("h0\xff\xfe")], metadata: help_ref=11 ("h9\xff\xfe")
        ts_multi.extend_from_slice(&[0x0a, 0x02, 1, 2]);
        ts_multi.extend_from_slice(&[0x2a, 0x04, 0x08, 0x01, 0x18, 11]);
        multi_lossy_wire.push(0x2a);
        multi_lossy_wire.push(ts_multi.len() as u8);
        multi_lossy_wire.extend_from_slice(&ts_multi);
        let mut meta2 = Vec::new();
        let mut seen_labels = 0usize;
        decode_rw2_stream(
            &multi_lossy_wire,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta2,
            |_| seen_labels += 1,
        )
        .expect("multi lossy rw2 decode should succeed");
        assert_eq!(seen_labels, 1);
        assert_eq!(meta2.len(), 1);
        assert_eq!(meta2[0].help, "h9\u{FFFD}\u{FFFD}");
    }

    fn decode_stringlabels_pairs(mut data: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        while !data.is_empty() {
            let mut read_part = || {
                let len = if data[0] < 255 {
                    let l = data[0] as usize;
                    data = &data[1..];
                    l
                } else {
                    let l = (data[1] as usize)
                        | ((data[2] as usize) << 8)
                        | ((data[3] as usize) << 16);
                    data = &data[4..];
                    l
                };
                let slice = data[..len].to_vec();
                data = &data[len..];
                slice
            };
            let k = read_part();
            let v = read_part();
            out.push((k, v));
        }
        out
    }

    #[test]
    fn test_decode_classic_stream_differential_parity() {
        let long_val = "v".repeat(300);
        let req = cortexpb::WriteRequest {
            timeseries: vec![
                // Series 0: sorted unique labels with an empty value that must be dropped,
                // samples, exemplar, native histogram, created_timestamp.
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "http_requests_total"),
                        cortexpb::LabelPair::from_static("empty", ""),
                        cortexpb::LabelPair::from_static("job", "api"),
                        cortexpb::LabelPair::from_strs("long", &long_val),
                    ],
                    samples: vec![
                        cortexpb::Sample {
                            value: 1.5,
                            timestamp_ms: 1_700_000_000_000,
                        },
                        cortexpb::Sample {
                            value: 2.5,
                            timestamp_ms: 1_700_000_015_000,
                        },
                    ],
                    exemplars: vec![cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_static("trace_id", "abc")],
                        value: 0.25,
                        timestamp_ms: 1_700_000_015_000,
                    }],
                    histograms: vec![cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountInt(10)),
                        sum: 12.5,
                        schema: 3,
                        zero_threshold: 0.001,
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(2)),
                        negative_spans: vec![cortexpb::BucketSpan {
                            offset: -1,
                            length: 2,
                        }],
                        negative_deltas: vec![1, 2],
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: 2,
                        }],
                        positive_deltas: vec![3, -1],
                        reset_hint: 1,
                        timestamp: 1_700_000_015_000,
                        custom_values: vec![1.0, 2.0],
                        ..Default::default()
                    }],
                    created_timestamp: 1_699_999_000_000,
                },
                // Series 1: unsorted labels WITH an empty value — empty value must be preserved
                // in encoded_labels for labels_not_sorted error formatting parity.
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("z", "1"),
                        cortexpb::LabelPair::from_static("drop_or_keep", ""),
                        cortexpb::LabelPair::from_static("a", "2"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 3.0,
                        timestamp_ms: 1_700_000_000_500,
                    }],
                    ..Default::default()
                },
                // Series 2: duplicate label name -> labels_sorted_unique == false
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("a", "1"),
                        cortexpb::LabelPair::from_static("a", "2"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 4.0,
                        timestamp_ms: 1_700_000_000_600,
                    }],
                    ..Default::default()
                },
            ],
            source: cortexpb::write_request::SourceEnum::Rule as i32,
            metadata: vec![cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "http_requests_total".to_string(),
                help: "total requests".to_string(),
                unit: "requests".to_string(),
            }],
            skip_label_validation: true,
            skip_label_count_validation: true,
            ..Default::default()
        };

        let wire = req.encode_to_vec();

        let mut ref_scratch = WriteRequestScratch::new();
        let ref_dec =
            decode_write_request(&wire, DecodeOptions::classic(), &mut ref_scratch).unwrap();

        let mut stream_scratch = WriteRequestScratch::new();
        let mut label_bytes_arena = Vec::new();
        let mut samples_arena = Vec::new();
        struct Seen {
            label_range: Range<usize>,
            encoded_labels: Vec<u8>,
            prom_hash: u64,
            sorted_unique: bool,
            bad_utf8: bool,
            samples_range: Range<usize>,
            min_ts: i64,
            created_ts: i64,
            histograms: Vec<cortexpb::Histogram>,
            exemplar_count: usize,
        }
        let mut seen = Vec::new();
        let hdr = decode_classic_stream(
            &wire,
            DecodeOptions::classic(),
            &mut stream_scratch,
            &mut label_bytes_arena,
            &mut samples_arena,
            |s| {
                seen.push(Seen {
                    label_range: s.label_bytes_range.clone(),
                    encoded_labels: s.encoded_labels.to_vec(),
                    prom_hash: s.prom_hash,
                    sorted_unique: s.labels_sorted_unique,
                    bad_utf8: s.bad_utf8,
                    samples_range: s.samples_range.clone(),
                    min_ts: s.min_sample_ts,
                    created_ts: s.created_timestamp,
                    histograms: s.histograms().map(|h| h.to_owned_histogram()).collect(),
                    exemplar_count: s.exemplars_len(),
                });
            },
        )
        .unwrap();

        for s in &seen {
            assert_eq!(s.encoded_labels.as_slice(), &label_bytes_arena[s.label_range.clone()]);
        }

        assert_eq!(hdr.source, ref_dec.source);
        assert_eq!(hdr.skip_label_validation, ref_dec.skip_label_validation);
        assert_eq!(
            hdr.skip_label_count_validation,
            ref_dec.skip_label_count_validation
        );
        assert!(hdr.header_changed_after_series);
        assert_eq!(seen.len(), ref_dec.timeseries_len());

        // Series 0 parity
        assert!(seen[0].sorted_unique);
        assert!(!seen[0].bad_utf8);
        assert_eq!(
            seen[0].prom_hash,
            xxhash_rust::xxh64::xxh64(&label_bytes_arena[seen[0].label_range.clone()], 0)
        );
        assert_ne!(seen[0].prom_hash, 0);
        assert_eq!(
            decode_stringlabels_pairs(&label_bytes_arena[seen[0].label_range.clone()]),
            vec![
                (b"__name__".to_vec(), b"http_requests_total".to_vec()),
                (b"job".to_vec(), b"api".to_vec()),
                (b"long".to_vec(), long_val.as_bytes().to_vec()),
            ]
        );
        assert_eq!(
            &samples_arena[seen[0].samples_range.clone()],
            ref_dec.timeseries_at(0).samples()
        );
        assert_eq!(seen[0].min_ts, 1_700_000_000_000);
        assert_eq!(seen[0].created_ts, 1_699_999_000_000);
        assert_eq!(seen[0].exemplar_count, 1);
        assert_eq!(
            seen[0].histograms,
            ref_dec
                .timeseries_at(0)
                .histograms()
                .map(|h| h.to_owned_histogram())
                .collect::<Vec<_>>()
        );

        // Series 1 parity: unsorted + empty value retained
        assert!(!seen[1].sorted_unique);
        assert_eq!(seen[1].prom_hash, 0);
        assert_eq!(
            decode_stringlabels_pairs(&label_bytes_arena[seen[1].label_range.clone()]),
            vec![
                (b"z".to_vec(), b"1".to_vec()),
                (b"drop_or_keep".to_vec(), b"".to_vec()),
                (b"a".to_vec(), b"2".to_vec()),
            ]
        );

        // Series 2 parity: duplicate label name
        assert!(!seen[2].sorted_unique);
        assert_eq!(seen[2].prom_hash, 0);

        // Metadata parity
        let ref_meta: Vec<_> = ref_dec.metadata().collect();
        let stream_meta: Vec<_> = stream_scratch.metadata(&wire).collect();
        assert_eq!(stream_meta, ref_meta);
    }

    #[test]
    fn test_decode_classic_stream_bad_utf8_and_error_parity_fuzz() {
        // Invalid UTF-8 in label value sets bad_utf8=true and prom_hash=0 without erroring,
        // matching decode_write_request succeeding.
        let req_bad_utf8 = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair {
                    name: bytes::Bytes::from_static(b"__name__"),
                    value: bytes::Bytes::from_static(b"bad\xffval"),
                }],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: 100,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let bad_wire = req_bad_utf8.encode_to_vec();
        let mut scratch = WriteRequestScratch::new();
        assert!(decode_write_request(&bad_wire, DecodeOptions::classic(), &mut scratch).is_ok());
        let mut label_bytes = Vec::new();
        let mut samples = Vec::new();
        let mut bad_flags = Vec::new();
        decode_classic_stream(
            &bad_wire,
            DecodeOptions::classic(),
            &mut scratch,
            &mut label_bytes,
            &mut samples,
            |s| bad_flags.push((s.bad_utf8, s.prom_hash)),
        )
        .unwrap();
        assert_eq!(bad_flags, vec![(true, 0)]);

        // RW2 wire bytes must produce identical error in both classic decoders.
        let rw2 = cortexpb::WriteRequest {
            symbols_rw2: vec!["".into(), "__name__".into(), "up".into()],
            timeseries_rw2: vec![cortexpb::TimeSeriesRw2 {
                labels_refs: vec![1, 2],
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec();
        let e1 = decode_write_request(&rw2, DecodeOptions::classic(), &mut scratch)
            .err()
            .unwrap();
        let e2 = decode_classic_stream(
            &rw2,
            DecodeOptions::classic(),
            &mut scratch,
            &mut label_bytes,
            &mut samples,
            |_| {},
        )
        .err()
        .unwrap();
        assert_eq!(e1, e2);

        // Deterministic PRNG differential parity across 1,000 fuzzed inputs.
        let mut state: u64 = 0x517c_c1b7_2722_0a95;
        let mut next_u8 = || -> u8 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xff) as u8
        };
        let mut ref_scratch = WriteRequestScratch::new();
        for iter in 0..1000usize {
            let len = (iter % 97) + 1;
            let mut buf = Vec::with_capacity(len);
            for _ in 0..len {
                buf.push(next_u8());
            }
            let r_ref = decode_write_request(&buf, DecodeOptions::classic(), &mut ref_scratch);
            label_bytes.clear();
            samples.clear();
            let mut stream_count = 0usize;
            let r_stream = decode_classic_stream(
                &buf,
                DecodeOptions::classic(),
                &mut scratch,
                &mut label_bytes,
                &mut samples,
                |_| stream_count += 1,
            );
            match (r_ref, r_stream) {
                (Ok(ref_b), Ok(hdr)) => {
                    assert_eq!(stream_count, ref_b.timeseries_len());
                    assert_eq!(hdr.source, ref_b.source);
                    assert_eq!(hdr.skip_label_validation, ref_b.skip_label_validation);
                    assert_eq!(
                        hdr.skip_label_count_validation,
                        ref_b.skip_label_count_validation
                    );
                    assert_eq!(scratch.metadata(&buf).collect::<Vec<_>>(), ref_b.metadata().collect::<Vec<_>>());
                }
                (Err(e1), Err(e2)) => {
                    assert_eq!(e1, e2, "error mismatch on fuzz iter {iter}");
                    assert!(label_bytes.is_empty());
                    assert!(samples.is_empty());
                }
                (Ok(_), Err(e)) => panic!("ref Ok, stream Err({e:?}) on iter {iter}"),
                (Err(e), Ok(_)) => panic!("ref Err({e:?}), stream Ok on iter {iter}"),
            }
        }
    }
}
