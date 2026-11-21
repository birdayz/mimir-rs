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
    #[error("proto: Remote Write 2.0 expanded label arena exceeds limit ({limit} bytes)")]
    LabelArenaLimitExceeded { limit: usize },
}

/// Default maximum expanded `label_bytes_arena` growth allowed per [`decode_rw2_stream`] call
/// (512 MiB). Prevents small RW2 symbol-reference payloads from amplifying into multi-GiB arenas.
pub const DEFAULT_MAX_RW2_LABEL_BYTES: usize = 512 * 1024 * 1024;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EncodeRw2Error {
    #[error("the provided request is already rw2")]
    AlreadyRw2,
    #[error("invalid UTF-8 in label or exemplar for string-typed WriteRequest.symbols_rw2")]
    InvalidUtf8,
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
            SliceRef::Common { idx, len } => common_symbols
                .get(idx as usize)
                .map_or(&[][..], |s| {
                    let b = s.as_bytes();
                    &b[..(len as usize).min(b.len())]
                }),
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
            SliceRef::Common { idx, len } => common_symbols
                .get(idx as usize)
                .map_or(&[][..], |s| {
                    let b = s.as_bytes();
                    &b[..(len as usize).min(b.len())]
                }),
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
            SliceRef::Common { idx, len } => common_symbols
                .get(idx as usize)
                .map_or("", |s| &s[..(len as usize).min(s.len())]),
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
            SliceRef::Common { idx, len } => common_symbols
                .get(idx as usize)
                .map_or("", |s| &s[..(len as usize).min(s.len())]),
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

#[inline]
fn materialize_common_into_scratch(
    r: SliceRef,
    common_symbols: &[&str],
    utf8_arena: &mut Vec<u8>,
) -> SliceRef {
    match r {
        SliceRef::Common { idx, len } => {
            let sym = common_symbols.get(idx as usize).copied().unwrap_or("");
            let slice = &sym[..(len as usize).min(sym.len())];
            if slice.is_empty() {
                SliceRef::Empty
            } else {
                let start = utf8_arena.len();
                utf8_arena.extend_from_slice(slice.as_bytes());
                SliceRef::Scratch {
                    start: start as u32,
                    len: slice.len() as u32,
                }
            }
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
    v2_resolved: Vec<Option<(&'static [u8], bool)>>,
    custom_resolved: Vec<Option<(&'static [u8], bool)>>,
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

const V2_COMMON_SYMBOLS_64: [Option<(&'static [u8], bool)>; 64] = {
    let mut arr: [Option<(&'static [u8], bool)>; 64] = [None; 64];
    let mut i = 0;
    while i < V2_COMMON_SYMBOLS_LIST.len() && i < 64 {
        arr[i] = Some((V2_COMMON_SYMBOLS_LIST[i].as_bytes(), true));
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
    pub raw_label_bytes_range: Option<Range<usize>>,
    pub encoded_labels: &'v [u8],
    pub raw_encoded_labels: &'v [u8],
    pub labels_sorted_unique: bool,
    pub bad_utf8: bool,
    pub samples_range: Range<usize>,
    pub min_sample_ts: i64,
    pub created_timestamp: i64,
    pub histograms_range: Range<usize>,
    pub exemplars_range: Range<usize>,
    pub source: i32,
    pub skip_label_validation: bool,
    pub skip_label_count_validation: bool,
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
    pub raw_label_bytes_range: Option<Range<usize>>,
    pub encoded_labels: &'v [u8],
    pub raw_encoded_labels: &'v [u8],
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
    pub skip_label_count_validation: bool,
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
    neg_spans: Vec<cortexpb::BucketSpan>,
    pos_spans: Vec<cortexpb::BucketSpan>,
    neg_deltas: Vec<i64>,
    pos_deltas: Vec<i64>,
    neg_counts: Vec<f64>,
    pos_counts: Vec<f64>,
    custom_values: Vec<f64>,
    timeseries: Vec<TimeSeriesMeta>,
    metadata: Vec<MetadataMeta>,
    rw2_symbols: Vec<SliceRef>,
    rw2_label_refs: Vec<u32>,
    dedup_hashes: HashMap<u64, Vec<usize>>,
    utf8_arena: Vec<u8>,
    uses_v2_common_symbols: bool,
}

impl WriteRequestScratch {
    pub fn new() -> Self {
        Self {
            labels: Vec::with_capacity(256),
            samples: Vec::with_capacity(256),
            exemplars: Vec::with_capacity(32),
            exemplar_labels: Vec::with_capacity(64),
            histograms: Vec::with_capacity(32),
            neg_spans: Vec::with_capacity(32),
            pos_spans: Vec::with_capacity(32),
            neg_deltas: Vec::with_capacity(64),
            pos_deltas: Vec::with_capacity(64),
            neg_counts: Vec::with_capacity(64),
            pos_counts: Vec::with_capacity(64),
            custom_values: Vec::with_capacity(16),
            timeseries: Vec::with_capacity(100),
            metadata: Vec::with_capacity(32),
            rw2_symbols: Vec::with_capacity(128),
            rw2_label_refs: Vec::with_capacity(64),
            dedup_hashes: HashMap::with_capacity(32),
            utf8_arena: Vec::new(),
            uses_v2_common_symbols: false,
        }
    }

    pub fn clear(&mut self) {
        self.labels.clear();
        self.samples.clear();
        self.exemplars.clear();
        self.exemplar_labels.clear();
        self.histograms.clear();
        self.neg_spans.clear();
        self.pos_spans.clear();
        self.neg_deltas.clear();
        self.pos_deltas.clear();
        self.neg_counts.clear();
        self.pos_counts.clear();
        self.custom_values.clear();
        self.timeseries.clear();
        self.metadata.clear();
        self.rw2_symbols.clear();
        self.rw2_label_refs.clear();
        self.dedup_hashes.clear();
        self.utf8_arena.clear();
        self.uses_v2_common_symbols = false;
    }

    #[inline]
    pub fn metadata_len(&self) -> usize {
        self.metadata.len()
    }

    pub fn metadata<'a>(&'a self, input: &'a [u8]) -> impl ExactSizeIterator<Item = BorrowedMetadata<'a>> + 'a {
        let common: &'a [&'a str] = if self.uses_v2_common_symbols {
            V2_COMMON_SYMBOLS_LIST
        } else {
            &[]
        };
        self.metadata.iter().map(move |m| BorrowedMetadata {
            metric_type: m.metric_type,
            metric_family_name: m
                .family_name
                .resolve_str(input, common, &self.utf8_arena),
            help: m.help.resolve_str(input, common, &self.utf8_arena),
            unit: m.unit.resolve_str(input, common, &self.utf8_arena),
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
        &self.scratch.neg_spans[self.meta.negative_spans.clone()]
    }

    #[inline]
    pub fn negative_deltas(&self) -> &[i64] {
        &self.scratch.neg_deltas[self.meta.negative_deltas.clone()]
    }

    #[inline]
    pub fn negative_counts(&self) -> &[f64] {
        &self.scratch.neg_counts[self.meta.negative_counts.clone()]
    }

    #[inline]
    pub fn positive_spans(&self) -> &[cortexpb::BucketSpan] {
        &self.scratch.pos_spans[self.meta.positive_spans.clone()]
    }

    #[inline]
    pub fn positive_deltas(&self) -> &[i64] {
        &self.scratch.pos_deltas[self.meta.positive_deltas.clone()]
    }

    #[inline]
    pub fn positive_counts(&self) -> &[f64] {
        &self.scratch.pos_counts[self.meta.positive_counts.clone()]
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
        &self.scratch.custom_values[self.meta.custom_values.clone()]
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
        DecodeMode::KafkaV2 => {
            scratch.uses_v2_common_symbols = true;
            (true, V2_RECORD_SYMBOL_OFFSET, V2_COMMON_SYMBOLS_LIST)
        }
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
fn append_raw_classic_labels(
    input: &[u8],
    range: Range<usize>,
    label_bytes_arena: &mut Vec<u8>,
) -> Result<(), DecodeError> {
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

#[cold]
#[inline(never)]
fn append_raw_rw2_labels(
    input: &[u8],
    range: Range<usize>,
    symbol_offset: u32,
    resolved: &[Option<(&[u8], bool)>],
    label_bytes_arena: &mut Vec<u8>,
    label_base: usize,
    max_expanded_label_bytes: usize,
) -> Result<(), DecodeError> {
    let mut pos = range.start;
    let end = range.end;
    while pos < end {
        let (field_num, wire_type) = read_key_bounded(input, &mut pos, end)?;
        if field_num == 1 && wire_type == 2 {
            let packed = read_length_delimited_range_bounded(input, &mut pos, end)?;
            let mut p = packed.start;
            while p < packed.end {
                let name_idx = read_varint_bounded(input, &mut p, packed.end)? as u32;
                let val_idx = read_varint_bounded(input, &mut p, packed.end)? as u32;
                let &(k_bytes, _) = resolved
                    .get(name_idx as usize)
                    .and_then(|o| o.as_ref())
                    .ok_or(DecodeError::InvalidLabelRef {
                        index: name_idx,
                        offset: symbol_offset,
                    })?;
                let &(v_bytes, _) = resolved
                    .get(val_idx as usize)
                    .and_then(|o| o.as_ref())
                    .ok_or(DecodeError::InvalidLabelRef {
                        index: val_idx,
                        offset: symbol_offset,
                    })?;
                append_stringlabel_pair_proto_bounded(
                    label_bytes_arena,
                    k_bytes,
                    v_bytes,
                    label_base,
                    max_expanded_label_bytes,
                )?;
            }
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
                        skip_label_count_validation,
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
                    let val = read_varint(input, &mut pos)? != 0;
                    if emitted_any_series && val != skip_label_count_validation {
                        header_changed_after_series = true;
                    }
                    skip_label_count_validation = val;
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
    skip_label_count_validation: bool,
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

    let mut raw_label_bytes_range = None;
    let label_bytes_end = if !bad_utf8 && !labels_sorted_unique && dropped_empty {
        label_bytes_arena.truncate(label_bytes_start);
        append_raw_classic_labels(input, range, label_bytes_arena)?;
        label_bytes_arena.len()
    } else if !bad_utf8 && labels_sorted_unique && dropped_empty {
        let enc_end = label_bytes_arena.len();
        append_raw_classic_labels(input, range, label_bytes_arena)?;
        raw_label_bytes_range = Some(enc_end..label_bytes_arena.len());
        enc_end
    } else {
        label_bytes_arena.len()
    };
    let (encoded_labels, raw_encoded_labels) = match raw_label_bytes_range {
        Some(ref rr) => (
            &label_bytes_arena[label_bytes_start..label_bytes_end],
            &label_bytes_arena[rr.start..rr.end],
        ),
        None => {
            let enc = &label_bytes_arena[label_bytes_start..label_bytes_end];
            (enc, enc)
        }
    };
    let prom_hash = if !bad_utf8 && labels_sorted_unique && !encoded_labels.is_empty() {
        xxhash_rust::xxh64::xxh64(encoded_labels, 0)
    } else {
        0
    };

    Ok(StreamedClassicSeries {
        label_bytes_range: label_bytes_start..label_bytes_end,
        raw_label_bytes_range,
        encoded_labels,
        raw_encoded_labels,
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
        skip_label_count_validation,
        scratch,
        input,
    })
}

struct ResolvedTableGuard<'r> {
    vec: &'r mut Vec<Option<(&'static [u8], bool)>>,
    base_len: usize,
}

impl Drop for ResolvedTableGuard<'_> {
    fn drop(&mut self) {
        self.vec.truncate(self.base_len);
    }
}

#[inline(always)]
fn stringlabel_part_encoded_len(len: usize) -> Result<usize, DecodeError> {
    if len < 255 {
        Ok(1 + len)
    } else if len < (1 << 24) {
        Ok(4 + len)
    } else {
        Err(DecodeError::InputTooLarge)
    }
}

#[inline(always)]
fn append_stringlabel_pair_proto_bounded(
    buf: &mut Vec<u8>,
    name: &[u8],
    value: &[u8],
    label_base: usize,
    max_expanded_label_bytes: usize,
) -> Result<(), DecodeError> {
    let pair_len = stringlabel_part_encoded_len(name.len())?
        .saturating_add(stringlabel_part_encoded_len(value.len())?);
    if buf
        .len()
        .saturating_sub(label_base)
        .saturating_add(pair_len)
        > max_expanded_label_bytes
    {
        return Err(DecodeError::LabelArenaLimitExceeded {
            limit: max_expanded_label_bytes,
        });
    }
    append_stringlabel_pair_proto(buf, name, value)
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
    on_series: F,
) -> Result<StreamedRw2Header, DecodeError>
where
    F: FnMut(StreamedRw2Series<'_, 'a>),
{
    decode_rw2_stream_with_limit(
        input,
        opts,
        scratch,
        resolved_scratch,
        label_bytes_arena,
        samples_arena,
        meta_out,
        DEFAULT_MAX_RW2_LABEL_BYTES,
        on_series,
    )
}

/// Like [`decode_rw2_stream`], with an explicit cap on expanded `label_bytes_arena` growth.
#[allow(clippy::too_many_arguments)]
pub fn decode_rw2_stream_with_limit<'r, 'a: 'r, F>(
    input: &'a [u8],
    opts: DecodeOptions<'a>,
    scratch: &mut WriteRequestScratch,
    resolved_scratch: &'r mut Rw2ResolvedScratch,
    label_bytes_arena: &mut Vec<u8>,
    samples_arena: &mut Vec<cortexpb::Sample>,
    meta_out: &mut Vec<BorrowedMetadata<'r>>,
    max_expanded_label_bytes: usize,
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
    let label_base = label_bytes_arena.len();
    let samples_base = samples_arena.len();
    let meta_base = meta_out.len();

    let (rw2_offset, common_symbols, is_kafka_v2): (u32, &'a [&'a str], bool) = match opts.mode {
        DecodeMode::Classic => return Err(DecodeError::UnexpectedRw1Timeseries),
        DecodeMode::Rw2 => (0, &[], false),
        DecodeMode::KafkaV2 => {
            scratch.uses_v2_common_symbols = true;
            (V2_RECORD_SYMBOL_OFFSET, V2_COMMON_SYMBOLS_LIST, true)
        }
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
    let resolved: &mut Vec<Option<(&'r [u8], bool)>> = unsafe {
        &mut *(guard.vec as *mut Vec<Option<(&'static [u8], bool)>>).cast()
    };
    if !is_kafka_v2 {
        for (i, &sym) in common_symbols.iter().enumerate() {
            if i < resolved.len() {
                resolved[i] = Some((sym.as_bytes(), true));
            }
        }
        if !resolved.is_empty() {
            resolved[0] = Some((b"", true));
        }
    }

    let mut source: i32 = 0;
    let mut skip_label_validation = false;
    let mut skip_label_count_validation = false;
    let mut seen_first_symbol = false;
    let mut prescanned_symbols = false;
    let mut emitted_any_series = false;
    let mut header_changed_after_series = false;

    let res: Result<(), DecodeError> = (|| {
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
                        let sym_bytes = &input[sub];
                        if !seen_first_symbol && !sym_bytes.is_empty() {
                            return Err(DecodeError::InvalidFirstSymbol);
                        }
                        seen_first_symbol = true;
                        let valid_utf8 = is_valid_utf8_fast(sym_bytes);
                        resolved.push(Some((sym_bytes, valid_utf8)));
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
                                let sym_bytes = &input[sym_range];
                                if !seen_first_symbol && !sym_bytes.is_empty() {
                                    return Err(DecodeError::InvalidFirstSymbol);
                                }
                                seen_first_symbol = true;
                                let valid_utf8 = is_valid_utf8_fast(sym_bytes);
                                resolved.push(Some((sym_bytes, valid_utf8)));
                            } else {
                                skip_field_bounded(input, &mut scan_pos, len, sw)?;
                            }
                        }
                        if resolved.is_empty() && rw2_offset == 0 {
                            resolved.push(Some((b"", true)));
                        }
                    }
                    let series = decode_timeseries_rw2_stream(
                        input,
                        sub,
                        rw2_offset,
                        common_symbols,
                        resolved,
                        lossy_strings,
                        &opts,
                        scratch,
                        label_bytes_arena,
                        label_base,
                        max_expanded_label_bytes,
                        samples_arena,
                        meta_out,
                        source,
                        skip_label_validation,
                        skip_label_count_validation,
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
                    let val = read_varint(input, &mut pos)? != 0;
                    if emitted_any_series && val != skip_label_count_validation {
                        header_changed_after_series = true;
                    }
                    skip_label_count_validation = val;
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
        meta_out.truncate(meta_base);
        return Err(e);
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
    resolved: &[Option<(&'r [u8], bool)>],
    lossy_strings: &mut Vec<Box<str>>,
    opts: &DecodeOptions<'a>,
    scratch: &'v mut WriteRequestScratch,
    label_bytes_arena: &'v mut Vec<u8>,
    label_base: usize,
    max_expanded_label_bytes: usize,
    samples_arena: &mut Vec<cortexpb::Sample>,
    meta_out: &mut Vec<BorrowedMetadata<'r>>,
    source: i32,
    skip_label_validation: bool,
    skip_label_count_validation: bool,
) -> Result<StreamedRw2Series<'v, 'a>, DecodeError> {
    let label_bytes_start = label_bytes_arena.len();
    let samples_start = samples_arena.len();
    let exemplars_start = scratch.exemplars.len();
    let histograms_start = scratch.histograms.len();
    let mut created_timestamp = 0i64;
    let mut min_sample_ts = i64::MAX;
    let mut metric_name_bytes: &'r [u8] = b"";
    let mut metric_name_utf8 = true;
    let mut prev_name: Option<&'r [u8]> = None;
    let mut labels_sorted_unique = true;
    let mut bad_utf8 = false;
    let mut dropped_empty = false;

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
                        let name_idx = read_varint_bounded(input, &mut p, packed.end)? as u32;
                        if p >= packed.end {
                            return Err(DecodeError::OddNumberOfLabelRefs);
                        }
                        let val_idx = read_varint_bounded(input, &mut p, packed.end)? as u32;
                        let &(k_bytes, k_utf8) = resolved
                            .get(name_idx as usize)
                            .and_then(|o| o.as_ref())
                            .ok_or(DecodeError::InvalidLabelRef {
                                index: name_idx,
                                offset: symbol_offset,
                            })?;
                        let &(v_bytes, v_utf8) = resolved
                            .get(val_idx as usize)
                            .and_then(|o| o.as_ref())
                            .ok_or(DecodeError::InvalidLabelRef {
                                index: val_idx,
                                offset: symbol_offset,
                            })?;
                        if k_bytes == b"__name__" {
                            metric_name_bytes = v_bytes;
                            metric_name_utf8 = v_utf8;
                        }
                        if !k_utf8 || !v_utf8 {
                            bad_utf8 = true;
                        } else if !bad_utf8 {
                            if let Some(pn) = prev_name {
                                if pn >= k_bytes {
                                    labels_sorted_unique = false;
                                }
                            }
                            prev_name = Some(k_bytes);
                            if !v_bytes.is_empty() {
                                append_stringlabel_pair_proto_bounded(
                                    label_bytes_arena,
                                    k_bytes,
                                    v_bytes,
                                    label_base,
                                    max_expanded_label_bytes,
                                )?;
                            } else {
                                dropped_empty = true;
                            }
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
                    lossy_strings,
                    metric_name_bytes,
                    metric_name_utf8,
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

    let mut raw_label_bytes_range = None;
    let label_bytes_end = if bad_utf8 {
        label_bytes_arena.truncate(label_bytes_start);
        label_bytes_start
    } else if !labels_sorted_unique && dropped_empty {
        label_bytes_arena.truncate(label_bytes_start);
        append_raw_rw2_labels(
            input,
            range,
            symbol_offset,
            resolved,
            label_bytes_arena,
            label_base,
            max_expanded_label_bytes,
        )?;
        label_bytes_arena.len()
    } else if labels_sorted_unique && dropped_empty {
        let enc_end = label_bytes_arena.len();
        append_raw_rw2_labels(
            input,
            range,
            symbol_offset,
            resolved,
            label_bytes_arena,
            label_base,
            max_expanded_label_bytes,
        )?;
        raw_label_bytes_range = Some(enc_end..label_bytes_arena.len());
        enc_end
    } else {
        label_bytes_arena.len()
    };
    let (encoded_labels, raw_encoded_labels) = match raw_label_bytes_range {
        Some(ref rr) => (
            &label_bytes_arena[label_bytes_start..label_bytes_end],
            &label_bytes_arena[rr.start..rr.end],
        ),
        None => {
            let enc = &label_bytes_arena[label_bytes_start..label_bytes_end];
            (enc, enc)
        }
    };
    Ok(StreamedRw2Series {
        label_bytes_range: label_bytes_start..label_bytes_end,
        raw_label_bytes_range,
        encoded_labels,
        raw_encoded_labels,
        labels_sorted_unique,
        bad_utf8,
        samples_range: samples_start..samples_arena.len(),
        min_sample_ts,
        created_timestamp,
        histograms_range: histograms_start..scratch.histograms.len(),
        exemplars_range: exemplars_start..scratch.exemplars.len(),
        source,
        skip_label_validation,
        skip_label_count_validation,
        scratch,
        input,
        common_symbols,
    })
}

#[inline]
fn bytes_to_slice_ref(
    bytes: &[u8],
    idx: usize,
    symbol_offset: u32,
    input: &[u8],
) -> SliceRef {
    if bytes.is_empty() {
        SliceRef::Empty
    } else if idx < symbol_offset as usize {
        SliceRef::Common {
            idx: idx as u32,
            len: bytes.len() as u32,
        }
    } else {
        let input_start = input.as_ptr() as usize;
        let b_ptr = bytes.as_ptr() as usize;
        debug_assert!(b_ptr >= input_start && b_ptr + bytes.len() <= input_start + input.len());
        let start = b_ptr - input_start;
        SliceRef::Input {
            start: start as u32,
            len: bytes.len() as u32,
        }
    }
}

#[inline]
fn resolve_lossy_str<'r>(
    bytes: &'r [u8],
    valid_utf8: bool,
    lossy_strings: &mut Vec<Box<str>>,
) -> &'r str {
    if valid_utf8 {
        // SAFETY: caller verified `bytes` is valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(bytes) }
    } else {
        let lossy: Box<str> = String::from_utf8_lossy(bytes).into_owned().into_boxed_str();
        lossy_strings.push(lossy);
        let ptr: *const str = &**lossy_strings.last().unwrap();
        // SAFETY: `Box<str>` heap allocation does not move when `Vec<Box<str>>` pushes,
        // and `lossy_strings` lives in `resolved_scratch` for `'r`.
        unsafe { &*ptr }
    }
}

fn decode_exemplar_rw2_resolved<'a>(
    input: &'a [u8],
    range: Range<usize>,
    symbol_offset: u32,
    resolved: &[Option<(&'a [u8], bool)>],
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
                        let name_idx = read_varint_bounded(input, &mut p, packed.end)? as u32;
                        if p >= packed.end {
                            return Err(DecodeError::OddNumberOfExemplarLabelRefs);
                        }
                        let val_idx = read_varint_bounded(input, &mut p, packed.end)? as u32;
                        let &(k_bytes, _) = resolved
                            .get(name_idx as usize)
                            .and_then(|o| o.as_ref())
                            .ok_or(DecodeError::InvalidExemplarLabelRef {
                                index: name_idx,
                                offset: symbol_offset,
                            })?;
                        let &(v_bytes, _) = resolved
                            .get(val_idx as usize)
                            .and_then(|o| o.as_ref())
                            .ok_or(DecodeError::InvalidExemplarLabelRef {
                                index: val_idx,
                                offset: symbol_offset,
                            })?;
                        let name_ref = bytes_to_slice_ref(
                            k_bytes,
                            name_idx as usize,
                            symbol_offset,
                            input,
                        );
                        let val_ref = bytes_to_slice_ref(
                            v_bytes,
                            val_idx as usize,
                            symbol_offset,
                            input,
                        );
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
    resolved: &[Option<(&'r [u8], bool)>],
    lossy_strings: &mut Vec<Box<str>>,
    metric_name_bytes: &'r [u8],
    metric_name_utf8: bool,
    opts: &DecodeOptions<'_>,
    scratch: &mut WriteRequestScratch,
    meta_out: &mut Vec<BorrowedMetadata<'r>>,
) -> Result<(), DecodeError> {
    let mut metric_type = 0i32;
    let mut help_bytes: &'r [u8] = b"";
    let mut help_utf8 = true;
    let mut unit_bytes: &'r [u8] = b"";
    let mut unit_utf8 = true;

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
                let &(bytes, ok) = resolved
                    .get(help_ref as usize)
                    .and_then(|o| o.as_ref())
                    .ok_or(DecodeError::InvalidHelpRef {
                        index: help_ref,
                        offset: symbol_offset,
                    })?;
                help_bytes = bytes;
                help_utf8 = ok;
            }
            4 => {
                if wire_type != 0 {
                    return Err(DecodeError::InvalidWireType {
                        field: 4,
                        wire_type,
                    });
                }
                let unit_ref = read_varint_bounded(input, &mut pos, end)? as u32;
                let &(bytes, ok) = resolved
                    .get(unit_ref as usize)
                    .and_then(|o| o.as_ref())
                    .ok_or(DecodeError::InvalidUnitRef {
                        index: unit_ref,
                        offset: symbol_offset,
                    })?;
                unit_bytes = bytes;
                unit_utf8 = ok;
            }
            _ => {
                skip_field_bounded(input, &mut pos, end, wire_type)?;
            }
        }
    }

    let metric_name = resolve_lossy_str(metric_name_bytes, metric_name_utf8, lossy_strings);
    let help = resolve_lossy_str(help_bytes, help_utf8, lossy_strings);
    let unit = resolve_lossy_str(unit_bytes, unit_utf8, lossy_strings);

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

    let neg_spans_start = scratch.neg_spans.len();
    let pos_spans_start = scratch.pos_spans.len();
    let neg_deltas_start = scratch.neg_deltas.len();
    let pos_deltas_start = scratch.pos_deltas.len();
    let neg_counts_start = scratch.neg_counts.len();
    let pos_counts_start = scratch.pos_counts.len();
    let custom_values_start = scratch.custom_values.len();

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
                scratch.neg_spans.push(span);
            }
            9 => {
                decode_sint64_repeated(input, &mut pos, end, wire_type, 9, &mut scratch.neg_deltas)?;
            }
            10 => {
                decode_f64_repeated(input, &mut pos, end, wire_type, 10, &mut scratch.neg_counts)?;
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
                scratch.pos_spans.push(span);
            }
            12 => {
                decode_sint64_repeated(input, &mut pos, end, wire_type, 12, &mut scratch.pos_deltas)?;
            }
            13 => {
                decode_f64_repeated(input, &mut pos, end, wire_type, 13, &mut scratch.pos_counts)?;
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
                decode_f64_repeated(input, &mut pos, end, wire_type, 16, &mut scratch.custom_values)?;
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
        negative_spans: neg_spans_start..scratch.neg_spans.len(),
        negative_deltas: neg_deltas_start..scratch.neg_deltas.len(),
        negative_counts: neg_counts_start..scratch.neg_counts.len(),
        positive_spans: pos_spans_start..scratch.pos_spans.len(),
        positive_deltas: pos_deltas_start..scratch.pos_deltas.len(),
        positive_counts: pos_counts_start..scratch.pos_counts.len(),
        reset_hint,
        timestamp,
        custom_values: custom_values_start..scratch.custom_values.len(),
    })
}

fn decode_sint64_repeated(
    input: &[u8],
    pos: &mut usize,
    end: usize,
    wire_type: u8,
    field: u32,
    vec: &mut Vec<i64>,
) -> Result<(), DecodeError> {
    if wire_type == 0 {
        let v = zigzag_i64(read_varint_bounded(input, pos, end)?);
        vec.push(v);
        Ok(())
    } else if wire_type == 2 {
        let packed = read_length_delimited_range_bounded(input, pos, end)?;
        let mut p = packed.start;
        while p < packed.end {
            let v = zigzag_i64(read_varint_bounded(input, &mut p, packed.end)?);
            vec.push(v);
        }
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
) -> Result<(), DecodeError> {
    if wire_type == 1 {
        let v = read_f64_bounded(input, pos, end)?;
        vec.push(v);
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

    let (family_name, help, unit) = if !scratch.uses_v2_common_symbols && !common_symbols.is_empty() {
        (
            materialize_common_into_scratch(final_family_ref, common_symbols, &mut scratch.utf8_arena),
            materialize_common_into_scratch(help, common_symbols, &mut scratch.utf8_arena),
            materialize_common_into_scratch(unit, common_symbols, &mut scratch.utf8_arena),
        )
    } else {
        (final_family_ref, help, unit)
    };

    scratch.metadata.push(MetadataMeta {
        metric_type,
        family_name,
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
    let raw_field_num = wire >> 3;
    let field_num = raw_field_num as u32;
    let wire_type = (wire & 0x7) as u8;
    if wire_type == 4 || wire_type > 5 {
        return Err(DecodeError::InvalidWireType {
            field: field_num,
            wire_type,
        });
    }
    if raw_field_num == 0 || raw_field_num >= (1u64 << 29) {
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
            let name = std::str::from_utf8(&l.name).map_err(|_| EncodeRw2Error::InvalidUtf8)?;
            let value = std::str::from_utf8(&l.value).map_err(|_| EncodeRw2Error::InvalidUtf8)?;
            if name == "__name__" {
                metric_name = value;
            }
            refs.push(symbols.symbolize(name));
            refs.push(symbols.symbolize(value));
        }

        let mut metadata = Some(cortexpb::MetadataRw2::default());
        if !metadata_map.is_empty() {
            if let Some(&meta_idx) = metadata_map.get(metric_name) {
                metadata = Some(from_metric_metadata_to_metadata_rw2(
                    &rw1.metadata[meta_idx],
                    &mut symbols,
                ));
                matched_metadata_mask[meta_idx] = true;
            }
        }

        let mut exemplars = Vec::with_capacity(ts.exemplars.len());
        for ex in &ts.exemplars {
            let mut ex_refs = Vec::with_capacity(ex.labels.len() * 2);
            for l in &ex.labels {
                let name =
                    std::str::from_utf8(&l.name).map_err(|_| EncodeRw2Error::InvalidUtf8)?;
                let value =
                    std::str::from_utf8(&l.value).map_err(|_| EncodeRw2Error::InvalidUtf8)?;
                ex_refs.push(symbols.symbolize(name));
                ex_refs.push(symbols.symbolize(value));
            }
            exemplars.push(cortexpb::ExemplarRw2 {
                labels_refs: ex_refs,
                value: ex.value,
                timestamp: ex.timestamp_ms,
            });
        }

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
    match from_write_request_to_rw2(rw1, offset, common_symbols) {
        Ok(rw2) => {
            out.clear();
            rw2.encode(out)
                .map_err(|e| EncodeRw2Error::Encode(e.to_string()))
        }
        Err(EncodeRw2Error::InvalidUtf8) => {
            encode_write_request_rw2_raw_bytes(rw1, offset, common_symbols, out)
        }
        Err(e) => Err(e),
    }
}

fn symbolize_raw_bytes<'a>(
    b: &'a [u8],
    offset: u32,
    common_symbols: Option<&CommonSymbols>,
    syms: &mut Vec<&'a [u8]>,
    map: &mut HashMap<&'a [u8], u32>,
    syms_proto_size: &mut usize,
) -> u32 {
    if b.is_empty() {
        return 0;
    }
    if let Some(cs) = common_symbols {
        if let Ok(s) = std::str::from_utf8(b) {
            if let Some(r) = cs.get_ref(s) {
                return r;
            }
        }
    }
    if let Some(&r) = map.get(b) {
        return r;
    }
    let r = (syms.len() as u32) + offset;
    syms.push(b);
    map.insert(b, r);
    *syms_proto_size += 1 + b.len() + prost::encoding::encoded_len_varint(b.len() as u64);
    r
}

fn build_rw2_raw_parts<'a>(
    rw1: &'a cortexpb::WriteRequest,
    offset: u32,
    common_symbols: Option<&CommonSymbols>,
) -> Result<(Vec<&'a [u8]>, Vec<cortexpb::TimeSeriesRw2>), EncodeRw2Error> {
    if !rw1.symbols_rw2.is_empty() || !rw1.timeseries_rw2.is_empty() {
        return Err(EncodeRw2Error::AlreadyRw2);
    }

    let mut syms: Vec<&'a [u8]> = Vec::with_capacity(101);
    syms.push(b"");
    let mut map: HashMap<&'a [u8], u32> = HashMap::with_capacity(100);
    let mut syms_proto_size = 0usize;

    let mut metadata_map: HashMap<&[u8], usize> = HashMap::new();
    let mut matched_metadata_mask: Vec<bool> = Vec::new();
    if !rw1.metadata.is_empty() && !rw1.timeseries.is_empty() {
        metadata_map.reserve(rw1.metadata.len());
        matched_metadata_mask.resize(rw1.metadata.len(), false);
        for (i, m) in rw1.metadata.iter().enumerate() {
            let key = m.metric_family_name.as_bytes();
            if let Some(&seen_idx) = metadata_map.get(key) {
                if rw1.metadata[i] == rw1.metadata[seen_idx] {
                    matched_metadata_mask[i] = true;
                }
            } else {
                metadata_map.insert(key, i);
            }
        }
    }

    let mut rw2_timeseries = Vec::with_capacity(rw1.timeseries.len() + rw1.metadata.len());

    for ts in &rw1.timeseries {
        let mut metric_name: &[u8] = b"";
        let mut refs = Vec::with_capacity(ts.labels.len() * 2);
        for l in &ts.labels {
            let name = l.name.as_ref();
            let value = l.value.as_ref();
            if name == b"__name__" {
                metric_name = value;
            }
            refs.push(symbolize_raw_bytes(
                name,
                offset,
                common_symbols,
                &mut syms,
                &mut map,
                &mut syms_proto_size,
            ));
            refs.push(symbolize_raw_bytes(
                value,
                offset,
                common_symbols,
                &mut syms,
                &mut map,
                &mut syms_proto_size,
            ));
        }

        let mut metadata = Some(cortexpb::MetadataRw2::default());
        if !metadata_map.is_empty() {
            if let Some(&meta_idx) = metadata_map.get(metric_name) {
                let m = &rw1.metadata[meta_idx];
                metadata = Some(cortexpb::MetadataRw2 {
                    r#type: m.r#type,
                    help_ref: symbolize_raw_bytes(
                        m.help.as_bytes(),
                        offset,
                        common_symbols,
                        &mut syms,
                        &mut map,
                        &mut syms_proto_size,
                    ),
                    unit_ref: symbolize_raw_bytes(
                        m.unit.as_bytes(),
                        offset,
                        common_symbols,
                        &mut syms,
                        &mut map,
                        &mut syms_proto_size,
                    ),
                });
                matched_metadata_mask[meta_idx] = true;
            }
        }

        let exemplars = ts
            .exemplars
            .iter()
            .map(|ex| {
                let mut ex_refs = Vec::with_capacity(ex.labels.len() * 2);
                for l in &ex.labels {
                    ex_refs.push(symbolize_raw_bytes(
                        l.name.as_ref(),
                        offset,
                        common_symbols,
                        &mut syms,
                        &mut map,
                        &mut syms_proto_size,
                    ));
                    ex_refs.push(symbolize_raw_bytes(
                        l.value.as_ref(),
                        offset,
                        common_symbols,
                        &mut syms,
                        &mut map,
                        &mut syms_proto_size,
                    ));
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
            symbolize_raw_bytes(
                b"__name__",
                offset,
                common_symbols,
                &mut syms,
                &mut map,
                &mut syms_proto_size,
            ),
            symbolize_raw_bytes(
                m.metric_family_name.as_bytes(),
                offset,
                common_symbols,
                &mut syms,
                &mut map,
                &mut syms_proto_size,
            ),
        ];
        let rw2_meta = cortexpb::MetadataRw2 {
            r#type: m.r#type,
            help_ref: symbolize_raw_bytes(
                m.help.as_bytes(),
                offset,
                common_symbols,
                &mut syms,
                &mut map,
                &mut syms_proto_size,
            ),
            unit_ref: symbolize_raw_bytes(
                m.unit.as_bytes(),
                offset,
                common_symbols,
                &mut syms,
                &mut map,
                &mut syms_proto_size,
            ),
        };
        rw2_timeseries.push(cortexpb::TimeSeriesRw2 {
            labels_refs,
            samples: Vec::new(),
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: Some(rw2_meta),
            created_timestamp: 0,
        });
    }

    Ok((syms, rw2_timeseries))
}

fn rw2_raw_base_encoded_len(rw1: &cortexpb::WriteRequest) -> usize {
    let mut n = 0usize;
    if rw1.source != 0 {
        n += prost::encoding::int32::encoded_len(2, &rw1.source);
    }
    if rw1.skip_label_validation {
        n += prost::encoding::bool::encoded_len(1000, &rw1.skip_label_validation);
    }
    if rw1.skip_label_count_validation {
        n += prost::encoding::bool::encoded_len(1001, &rw1.skip_label_count_validation);
    }
    n
}

fn rw2_raw_encoded_len(
    rw1: &cortexpb::WriteRequest,
    syms: &[&[u8]],
    rw2_timeseries: &[cortexpb::TimeSeriesRw2],
) -> usize {
    let mut n = rw2_raw_base_encoded_len(rw1);
    for sym in syms {
        n += 1 + prost::encoding::encoded_len_varint(sym.len() as u64) + sym.len();
    }
    for ts in rw2_timeseries {
        n += prost::encoding::message::encoded_len(5, ts);
    }
    n
}

fn encode_rw2_raw_parts(
    rw1: &cortexpb::WriteRequest,
    syms: &[&[u8]],
    rw2_timeseries: &[cortexpb::TimeSeriesRw2],
    out: &mut Vec<u8>,
) {
    out.clear();
    if rw1.source != 0 {
        prost::encoding::int32::encode(2, &rw1.source, out);
    }
    for sym in syms {
        prost::encoding::encode_key(4, prost::encoding::WireType::LengthDelimited, out);
        prost::encoding::encode_varint(sym.len() as u64, out);
        out.extend_from_slice(sym);
    }
    for ts in rw2_timeseries {
        prost::encoding::message::encode(5, ts, out);
    }
    if rw1.skip_label_validation {
        prost::encoding::bool::encode(1000, &rw1.skip_label_validation, out);
    }
    if rw1.skip_label_count_validation {
        prost::encoding::bool::encode(1001, &rw1.skip_label_count_validation, out);
    }
}

/// Encodes an RW1 `WriteRequest` (which may contain arbitrary non-UTF-8 label or exemplar bytes)
/// into RW2 protobuf wire bytes, preserving raw byte slices verbatim in `symbols_rw2` (field 4)
/// just like Go's `FromWriteRequestToRW2Request` + `Marshal()`.
fn encode_write_request_rw2_raw_bytes(
    rw1: &cortexpb::WriteRequest,
    offset: u32,
    common_symbols: Option<&CommonSymbols>,
    out: &mut Vec<u8>,
) -> Result<(), EncodeRw2Error> {
    let (syms, rw2_timeseries) = build_rw2_raw_parts(rw1, offset, common_symbols)?;
    encode_rw2_raw_parts(rw1, &syms, &rw2_timeseries, out);
    Ok(())
}

/// Converts an RW1 `WriteRequest` (preserving arbitrary non-UTF-8 label/exemplar bytes verbatim)
/// into RW2 and splits it into encoded protobuf records each <= `max_size` bytes (best effort),
/// matching Go's `FromWriteRequestToRW2Request` + `SplitWriteRequestByMaxMarshalSizeRW2`.
pub fn encode_write_request_rw2_split_raw_bytes(
    rw1: &cortexpb::WriteRequest,
    max_size: usize,
    offset: u32,
    common_symbols: Option<&CommonSymbols>,
) -> Result<Vec<Vec<u8>>, EncodeRw2Error> {
    let (syms, rw2_timeseries) = build_rw2_raw_parts(rw1, offset, common_symbols)?;
    let req_size = rw2_raw_encoded_len(rw1, &syms, &rw2_timeseries);
    if max_size == 0 || req_size <= max_size {
        let mut buf = Vec::with_capacity(req_size);
        encode_rw2_raw_parts(rw1, &syms, &rw2_timeseries, &mut buf);
        return Ok(vec![buf]);
    }
    if rw2_timeseries.is_empty() {
        return Ok(Vec::new());
    }

    fn resolve_raw_ref<'a>(
        r: u32,
        symbols: &'a [&'a [u8]],
        common_slice: &'a [String],
        offset: u32,
    ) -> &'a [u8] {
        if r < offset {
            common_slice
                .get(r as usize)
                .map(|s| s.as_bytes())
                .unwrap_or(b"")
        } else {
            symbols
                .get((r - offset) as usize)
                .copied()
                .unwrap_or(b"")
        }
    }

    fn resolved_raw_symbol_size(r: u32, symbols: &[&[u8]], offset: u32) -> usize {
        if r == 0 {
            return 1 + prost::length_delimiter_len(0);
        }
        if r < offset {
            return 0;
        }
        let l = symbols.get((r - offset) as usize).map(|s| s.len()).unwrap_or(0);
        1 + l + prost::length_delimiter_len(l)
    }

    fn max_raw_series_size(
        ts: &cortexpb::TimeSeriesRw2,
        symbols: &[&[u8]],
        offset: u32,
    ) -> (usize, usize) {
        let highest_possible = symbols.len() as u64 + offset as u64;
        let sym_upper = 1 + prost::length_delimiter_len(highest_possible as usize);
        let mut series_size = ts.encoded_len();
        if ts.metadata.is_none() {
            series_size += 2;
        }
        let mut syms_size = 0usize;
        for &r in &ts.labels_refs {
            series_size += sym_upper.saturating_sub(prost::length_delimiter_len(r as usize));
            syms_size += resolved_raw_symbol_size(r, symbols, offset);
        }
        for ex in &ts.exemplars {
            for &r in &ex.labels_refs {
                series_size += sym_upper.saturating_sub(prost::length_delimiter_len(r as usize));
                syms_size += resolved_raw_symbol_size(r, symbols, offset);
            }
        }
        let (help_ref, unit_ref) = ts
            .metadata
            .as_ref()
            .map_or((0, 0), |md| (md.help_ref, md.unit_ref));
        series_size += sym_upper.saturating_sub(prost::length_delimiter_len(help_ref as usize));
        syms_size += resolved_raw_symbol_size(help_ref, symbols, offset);
        series_size += sym_upper.saturating_sub(prost::length_delimiter_len(unit_ref as usize));
        syms_size += resolved_raw_symbol_size(unit_ref, symbols, offset);
        (series_size, syms_size)
    }

    let common_slice: &[String] = common_symbols.map_or(&[], |cs| cs.get_slice());
    let base_size = rw2_raw_base_encoded_len(rw1);
    let mut out = Vec::new();
    let mut next_syms: Vec<&[u8]> = Vec::with_capacity(101);
    next_syms.push(b"");
    let mut next_map: HashMap<&[u8], u32> = HashMap::with_capacity(100);
    let mut next_syms_proto_size = 0usize;
    let mut next_ts: Vec<cortexpb::TimeSeriesRw2> = Vec::new();
    let mut next_req_size = base_size;
    let mut next_req_symbols_size = 0usize;

    for ts in &rw2_timeseries {
        let (series_size, syms_size) = max_raw_series_size(ts, &syms, offset);
        let series_field_size = 1 + prost::length_delimiter_len(series_size) + series_size;

        if next_req_size + series_field_size + syms_size > max_size && !next_ts.is_empty() {
            let mut buf = Vec::with_capacity(next_req_size);
            encode_rw2_raw_parts(rw1, &next_syms, &next_ts, &mut buf);
            out.push(buf);

            next_syms.clear();
            next_syms.push(b"");
            next_map.clear();
            next_syms_proto_size = 0;
            next_ts.clear();
            next_req_size = base_size;
            next_req_symbols_size = 0;
        }

        let labels_refs = ts
            .labels_refs
            .iter()
            .map(|&r| {
                symbolize_raw_bytes(
                    resolve_raw_ref(r, &syms, common_slice, offset),
                    offset,
                    common_symbols,
                    &mut next_syms,
                    &mut next_map,
                    &mut next_syms_proto_size,
                )
            })
            .collect();
        let exemplars = ts
            .exemplars
            .iter()
            .map(|ex| {
                let ex_refs = ex
                    .labels_refs
                    .iter()
                    .map(|&r| {
                        symbolize_raw_bytes(
                            resolve_raw_ref(r, &syms, common_slice, offset),
                            offset,
                            common_symbols,
                            &mut next_syms,
                            &mut next_map,
                            &mut next_syms_proto_size,
                        )
                    })
                    .collect();
                cortexpb::ExemplarRw2 {
                    labels_refs: ex_refs,
                    value: ex.value,
                    timestamp: ex.timestamp,
                }
            })
            .collect();
        let metadata = ts.metadata.map(|md| cortexpb::MetadataRw2 {
            r#type: md.r#type,
            help_ref: symbolize_raw_bytes(
                resolve_raw_ref(md.help_ref, &syms, common_slice, offset),
                offset,
                common_symbols,
                &mut next_syms,
                &mut next_map,
                &mut next_syms_proto_size,
            ),
            unit_ref: symbolize_raw_bytes(
                resolve_raw_ref(md.unit_ref, &syms, common_slice, offset),
                offset,
                common_symbols,
                &mut next_syms,
                &mut next_map,
                &mut next_syms_proto_size,
            ),
        });
        let new_ts = cortexpb::TimeSeriesRw2 {
            labels_refs,
            samples: ts.samples.clone(),
            histograms: ts.histograms.clone(),
            exemplars,
            metadata,
            created_timestamp: ts.created_timestamp,
        };
        let ts_len = new_ts.encoded_len();
        next_req_size += 1 + prost::length_delimiter_len(ts_len) + ts_len;
        next_req_size += next_syms_proto_size.saturating_sub(next_req_symbols_size);
        next_req_symbols_size = next_syms_proto_size;
        next_ts.push(new_ts);
    }

    if !next_ts.is_empty() {
        let mut buf = Vec::with_capacity(next_req_size);
        encode_rw2_raw_parts(rw1, &next_syms, &next_ts, &mut buf);
        out.push(buf);
    }

    Ok(out)
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

        let iters = if cfg!(miri) { 16 } else { 1000 };
        for iter in 0..iters {
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

        // Invalid UTF-8 in RW2 series labels must set bad_utf8=true and not mutate into U+FFFD.
        let mut saw_bad_utf8 = false;
        let mut saw_encoded_len = usize::MAX;
        let mut ex_raw_val: Option<Vec<u8>> = None;
        let mut bad_ex_wire = Vec::new();
        for sym in [b"".as_slice(), b"__name__".as_slice(), b"m".as_slice(), b"trace_id".as_slice()] {
            bad_ex_wire.push(0x22);
            bad_ex_wire.push(sym.len() as u8);
            bad_ex_wire.extend_from_slice(sym);
        }
        let ex_bad = [0xffu8, 0xfe];
        bad_ex_wire.push(0x22);
        bad_ex_wire.push(ex_bad.len() as u8);
        bad_ex_wire.extend_from_slice(&ex_bad);
        let mut ts_ex = Vec::new();
        ts_ex.extend_from_slice(&[0x0a, 0x02, 1, 2]); // valid __name__=m
        ts_ex.extend_from_slice(&[0x22, 0x04, 0x0a, 0x02, 3, 4]); // exemplar trace_id=0xff,0xfe
        bad_ex_wire.push(0x2a);
        bad_ex_wire.push(ts_ex.len() as u8);
        bad_ex_wire.extend_from_slice(&ts_ex);

        label_bytes.clear();
        samples.clear();
        let mut meta3 = Vec::new();
        decode_rw2_stream(
            &multi_lossy_wire,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta3,
            |s| {
                saw_bad_utf8 = s.bad_utf8;
                saw_encoded_len = s.encoded_labels.len();
            },
        )
        .expect("decode_rw2_stream bad utf8 series");
        assert!(saw_bad_utf8);
        assert_eq!(saw_encoded_len, 0);

        label_bytes.clear();
        samples.clear();
        let mut meta4 = Vec::new();
        decode_rw2_stream(
            &bad_ex_wire,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta4,
            |s| {
                assert!(!s.bad_utf8);
                let ex = s.exemplars().next().unwrap();
                let lp = ex.labels().next().unwrap();
                assert!(lp.value_str().is_err());
                ex_raw_val = Some(lp.value.to_vec());
            },
        )
        .expect("decode_rw2_stream bad utf8 exemplar");
        assert_eq!(ex_raw_val.as_deref(), Some([0xff, 0xfe].as_slice()));
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
        let iters = if cfg!(miri) { 16 } else { 1000 };
        for iter in 0..iters {
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

    #[test]
    fn w13_p0_1_interleaved_histogram_repeated_fields_linear_decode() {
        // Build a single Histogram with 20,000 interleaved unpacked neg_deltas (tag 9)
        // and pos_deltas (tag 12) + interleaved neg_counts (tag 10), pos_counts (tag 13),
        // custom_values (tag 16), and neg_spans (tag 8) / pos_spans (tag 11).
        let pairs = if cfg!(miri) { 64usize } else { 20_000usize };
        let mut hist_bytes = Vec::with_capacity(pairs * 10);
        // tag 4 (schema = 0): 0x20 0x00
        hist_bytes.extend_from_slice(&[0x20, 0x00]);
        for i in 0..pairs {
            // tag 9 (negative_deltas, sint64, wire type 0 -> key 0x48), zigzag(1) = 2
            hist_bytes.extend_from_slice(&[0x48, 0x02]);
            // tag 12 (positive_deltas, sint64, wire type 0 -> key 0x60), zigzag(2) = 4
            hist_bytes.extend_from_slice(&[0x60, 0x04]);
            if i % 100 == 0 {
                // tag 10 (negative_counts, fixed64 -> key 0x51)
                hist_bytes.push(0x51);
                hist_bytes.extend_from_slice(&1.5f64.to_le_bytes());
                // tag 13 (positive_counts, fixed64 -> key 0x69)
                hist_bytes.push(0x69);
                hist_bytes.extend_from_slice(&2.5f64.to_le_bytes());
                // tag 16 (custom_values, fixed64 -> key 0x81, 0x01)
                hist_bytes.extend_from_slice(&[0x81, 0x01]);
                hist_bytes.extend_from_slice(&3.5f64.to_le_bytes());
            }
        }
        let mut ts_bytes = Vec::new();
        // tag 1 (LabelPair __name__=h)
        let lp = cortexpb::LabelPair::from_static("__name__", "h").encode_to_vec();
        ts_bytes.push(0x0a);
        prost::encoding::encode_varint(lp.len() as u64, &mut ts_bytes);
        ts_bytes.extend_from_slice(&lp);
        // tag 4 (histograms)
        ts_bytes.push(0x22);
        prost::encoding::encode_varint(hist_bytes.len() as u64, &mut ts_bytes);
        ts_bytes.extend_from_slice(&hist_bytes);
        let mut wr_bytes = Vec::new();
        wr_bytes.push(0x0a);
        prost::encoding::encode_varint(ts_bytes.len() as u64, &mut wr_bytes);
        wr_bytes.extend_from_slice(&ts_bytes);

        let start = std::time::Instant::now();
        let mut scratch = WriteRequestScratch::new();
        let borrowed = decode_write_request(&wr_bytes, DecodeOptions::classic(), &mut scratch)
            .expect("decode_write_request");
        assert_eq!(borrowed.timeseries_len(), 1);
        let ts0 = borrowed.timeseries_at(0);
        let h = ts0.histograms().next().unwrap();
        assert_eq!(h.negative_deltas().len(), pairs);
        assert_eq!(h.positive_deltas().len(), pairs);
        assert_eq!(h.negative_counts().len(), pairs.div_ceil(100));
        assert_eq!(h.positive_counts().len(), pairs.div_ceil(100));
        assert_eq!(h.custom_values().len(), pairs.div_ceil(100));
        assert!(h.negative_deltas().iter().all(|&d| d == 1));
        assert!(h.positive_deltas().iter().all(|&d| d == 2));

        let mut label_bytes = Vec::new();
        let mut samples = Vec::new();
        let mut count = 0usize;
        decode_classic_stream(
            &wr_bytes,
            DecodeOptions::classic(),
            &mut scratch,
            &mut label_bytes,
            &mut samples,
            |s| {
                let h = s.histograms().next().unwrap();
                assert_eq!(h.negative_deltas().len(), pairs);
                assert_eq!(h.positive_deltas().len(), pairs);
                count += 1;
            },
        )
        .expect("decode_classic_stream");
        assert_eq!(count, 1);
        if !cfg!(miri) {
            assert!(
                start.elapsed() < std::time::Duration::from_secs(2),
                "interleaved histogram decode took {:?}",
                start.elapsed()
            );
        }
    }

    #[test]
    fn w13_p1_1_rw2_zero_ref_and_metadata_common_symbol_no_panic() {
        // 1) Rw2Custom with offset > 0 and empty common_symbols: ref 0 must resolve to "".
        let syms = ["", "__name__", "m_total", "job", "api"];
        let mut packed_refs = Vec::new();
        for r in [257u64, 258, 259, 0] {
            prost::encoding::encode_varint(r, &mut packed_refs);
        }
        let mut ts_rw2 = Vec::new();
        ts_rw2.push(0x0a);
        prost::encoding::encode_varint(packed_refs.len() as u64, &mut ts_rw2);
        ts_rw2.extend_from_slice(&packed_refs);
        // MetadataRw2 with type=1, help_ref=0, unit_ref=0 -> [0x2a, 0x02, 0x08, 0x01]
        ts_rw2.extend_from_slice(&[0x2a, 0x02, 0x08, 0x01]);

        let mut wire = Vec::new();
        for s in syms {
            wire.push(0x22);
            prost::encoding::encode_varint(s.len() as u64, &mut wire);
            wire.extend_from_slice(s.as_bytes());
        }
        wire.push(0x2a);
        prost::encoding::encode_varint(ts_rw2.len() as u64, &mut wire);
        wire.extend_from_slice(&ts_rw2);

        let opts = DecodeOptions {
            mode: DecodeMode::Rw2Custom {
                offset: 256,
                common_symbols: &[],
            },
            ..DecodeOptions::rw2()
        };
        let mut scratch = WriteRequestScratch::new();
        let mut resolved = Rw2ResolvedScratch::new();
        let mut label_bytes = Vec::new();
        let mut samples = Vec::new();
        let mut meta_out = Vec::new();
        decode_rw2_stream(
            &wire,
            opts,
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta_out,
            |_| {},
        )
        .expect("ref == 0 with offset > 0 and empty common_symbols must succeed");
        assert_eq!(meta_out.len(), 1);
        assert_eq!(meta_out[0].metric_family_name, "m_total");
        assert_eq!(meta_out[0].help, "");
        assert_eq!(meta_out[0].unit, "");

        // 2) Rw2Custom with common_symbols[0] non-empty: ref 0 still resolves to "".
        let opts_nonempty0 = DecodeOptions {
            mode: DecodeMode::Rw2Custom {
                offset: 256,
                common_symbols: &["SHOULD_NOT_USE", "custom_help"],
            },
            ..DecodeOptions::rw2()
        };
        // Also test WriteRequestScratch::metadata on SliceRef::Common (KafkaV2 & Rw2Custom).
        let rw1 = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "up")],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: 1000,
                }],
                ..Default::default()
            }],
            metadata: vec![cortexpb::MetricMetadata {
                r#type: 2,
                metric_family_name: "up".to_string(),
                help: "seconds".to_string(),
                unit: "seconds".to_string(),
            }],
            ..Default::default()
        };
        let mut kv2_wire = Vec::new();
        encode_write_request_rw2_bytes(&rw1, V2_RECORD_SYMBOL_OFFSET, Some(v2_common_symbols()), &mut kv2_wire).unwrap();
        decode_write_request(&kv2_wire, DecodeOptions::kafka_v2(), &mut scratch).unwrap();
        let metas: Vec<_> = scratch.metadata(&kv2_wire).collect();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].metric_family_name, "up");
        assert_eq!(metas[0].unit, "seconds");

        // Rw2Custom metadata referencing common_symbols[1] = "custom_help"
        let mut ts_custom = Vec::new();
        ts_custom.push(0x0a);
        prost::encoding::encode_varint(packed_refs.len() as u64, &mut ts_custom);
        ts_custom.extend_from_slice(&packed_refs);
        // MetadataRw2 { type: 1, help_ref: 1, unit_ref: 0 }
        ts_custom.extend_from_slice(&[0x2a, 0x04, 0x08, 0x01, 0x18, 0x01]);
        let mut wire_custom = Vec::new();
        for s in syms {
            wire_custom.push(0x22);
            prost::encoding::encode_varint(s.len() as u64, &mut wire_custom);
            wire_custom.extend_from_slice(s.as_bytes());
        }
        wire_custom.push(0x2a);
        prost::encoding::encode_varint(ts_custom.len() as u64, &mut wire_custom);
        wire_custom.extend_from_slice(&ts_custom);
        decode_write_request(&wire_custom, opts_nonempty0, &mut scratch).unwrap();
        let custom_metas: Vec<_> = scratch.metadata(&wire_custom).collect();
        assert_eq!(custom_metas.len(), 1);
        assert_eq!(custom_metas[0].help, "custom_help");
        assert_eq!(custom_metas[0].unit, "");
    }

    #[test]
    fn w13_p1_2_p1_5_p1_6_varint32_illegal_fieldnum_nonnull_metadata_and_tag1001() {
        // 1) uint32 truncation on (1 << 32) | 1 in decode_rw2_stream
        let syms = ["", "__name__", "metric_a"];
        let mut packed = Vec::new();
        prost::encoding::encode_varint((1u64 << 32) | 1, &mut packed);
        prost::encoding::encode_varint((1u64 << 32) | 2, &mut packed);
        let mut ts_rw2 = Vec::new();
        ts_rw2.push(0x0a);
        prost::encoding::encode_varint(packed.len() as u64, &mut ts_rw2);
        ts_rw2.extend_from_slice(&packed);
        let mut wire = Vec::new();
        for s in syms {
            wire.push(0x22);
            prost::encoding::encode_varint(s.len() as u64, &mut wire);
            wire.extend_from_slice(s.as_bytes());
        }
        wire.push(0x2a);
        prost::encoding::encode_varint(ts_rw2.len() as u64, &mut wire);
        wire.extend_from_slice(&ts_rw2);

        let mut scratch = WriteRequestScratch::new();
        let mut label_bytes = Vec::new();
        let mut samples = Vec::new();
        {
            let mut resolved = Rw2ResolvedScratch::new();
            let mut meta_out = Vec::new();
            decode_rw2_stream(
                &wire,
                DecodeOptions::rw2(),
                &mut scratch,
                &mut resolved,
                &mut label_bytes,
                &mut samples,
                &mut meta_out,
                |s| {
                    // Encoded stringlabels: [8, "__name__", 8, "metric_a"]
                    assert_eq!(s.encoded_labels, b"__name__metric_a");
                },
            )
            .expect("32-bit truncated varint refs must match Go");
        }

        // 2) Illegal field numbers >= 2^29 rejected in all decoders
        let mut bad_tag_wire = Vec::new();
        prost::encoding::encode_varint((1u64 << 29) << 3, &mut bad_tag_wire);
        bad_tag_wire.push(0x00);
        assert!(matches!(
            decode_write_request(&bad_tag_wire, DecodeOptions::classic(), &mut scratch),
            Err(DecodeError::InvalidFieldNumber(_))
        ));
        assert!(matches!(
            decode_classic_stream(
                &bad_tag_wire,
                DecodeOptions::classic(),
                &mut scratch,
                &mut label_bytes,
                &mut samples,
                |_| {},
            ),
            Err(DecodeError::InvalidFieldNumber(_))
        ));
        {
            let mut resolved = Rw2ResolvedScratch::new();
            let mut meta_out = Vec::new();
            assert!(matches!(
                decode_rw2_stream(
                    &bad_tag_wire,
                    DecodeOptions::rw2(),
                    &mut scratch,
                    &mut resolved,
                    &mut label_bytes,
                    &mut samples,
                    &mut meta_out,
                    |_| {},
                ),
                Err(DecodeError::InvalidFieldNumber(_))
            ));
        }

        // 3) Non-nullable MetadataRW2 (0x2a 0x00) emitted when no metadata
        let rw1 = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "m")],
                samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: 10 }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let rw2 = from_write_request_to_rw2(&rw1, 0, None).unwrap();
        assert!(rw2.timeseries_rw2[0].metadata.is_some());
        let rw2_bytes = rw2.encode_to_vec();
        assert!(rw2_bytes.windows(2).any(|w| w == [0x2a, 0x00]));

        // 4) Tag 1001 after series sets header_changed_after_series in both stream decoders
        let mut classic_tag1001 = rw1.encode_to_vec();
        prost::encoding::encode_varint(1001u64 << 3, &mut classic_tag1001);
        classic_tag1001.push(1);
        let hdr_c = decode_classic_stream(
            &classic_tag1001,
            DecodeOptions::classic(),
            &mut scratch,
            &mut label_bytes,
            &mut samples,
            |s| assert!(!s.skip_label_count_validation),
        )
        .unwrap();
        assert!(hdr_c.skip_label_count_validation);
        assert!(hdr_c.header_changed_after_series);

        let mut rw2_tag1001 = rw2_bytes;
        prost::encoding::encode_varint(1001u64 << 3, &mut rw2_tag1001);
        rw2_tag1001.push(1);
        let mut resolved = Rw2ResolvedScratch::new();
        let mut meta_out = Vec::new();
        let hdr_r = decode_rw2_stream(
            &rw2_tag1001,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta_out,
            |s| assert!(!s.skip_label_count_validation),
        )
        .unwrap();
        assert!(hdr_r.skip_label_count_validation);
        assert!(hdr_r.header_changed_after_series);
    }

    #[test]
    fn w18_wpe_04_decode_rw2_stream_preserves_empty_labels_on_unsorted_and_raw_slice() {
        // 1) Unsorted RW2 labels with an empty-value label: must re-emit raw wire labels including empty=""
        let rw1_unsorted = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "up"),
                    cortexpb::LabelPair::from_static("z", "1"),
                    cortexpb::LabelPair::from_static("a", ""),
                ],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: 100,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let rw2_unsorted = from_write_request_to_rw2(&rw1_unsorted, 0, None)
            .unwrap()
            .encode_to_vec();
        let mut scratch = WriteRequestScratch::new();
        let mut resolved = Rw2ResolvedScratch::new();
        let mut label_bytes = Vec::new();
        let mut samples = Vec::new();
        let mut got_unsorted = Vec::new();
        {
            let mut meta_out = Vec::new();
            decode_rw2_stream(
                &rw2_unsorted,
                DecodeOptions::rw2(),
                &mut scratch,
                &mut resolved,
                &mut label_bytes,
                &mut samples,
                &mut meta_out,
                |s| {
                    assert!(!s.labels_sorted_unique);
                    assert!(s.raw_label_bytes_range.is_none());
                    assert_eq!(s.encoded_labels, s.raw_encoded_labels);
                    got_unsorted.push(s.encoded_labels.to_vec());
                },
            )
            .unwrap();
        }
        assert_eq!(got_unsorted.len(), 1);
        // Should contain all 3 pairs including a=""
        let mut pairs = Vec::new();
        let mut p = 0usize;
        let enc = &got_unsorted[0];
        while p < enc.len() {
            let kl = enc[p] as usize;
            p += 1;
            let k = std::str::from_utf8(&enc[p..p + kl]).unwrap().to_string();
            p += kl;
            let vl = enc[p] as usize;
            p += 1;
            let v = std::str::from_utf8(&enc[p..p + vl]).unwrap().to_string();
            p += vl;
            pairs.push((k, v));
        }
        assert_eq!(
            pairs,
            vec![
                ("__name__".to_string(), "up".to_string()),
                ("z".to_string(), "1".to_string()),
                ("a".to_string(), String::new()),
            ]
        );

        // 2) Sorted RW2 labels with an empty-value label: encoded_labels strips empty, raw_encoded_labels preserves it
        let rw1_sorted = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "up"),
                    cortexpb::LabelPair::from_static("empty", ""),
                    cortexpb::LabelPair::from_static("job", "api"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: 100,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let rw2_sorted = from_write_request_to_rw2(&rw1_sorted, 0, None)
            .unwrap()
            .encode_to_vec();
        scratch.clear();
        label_bytes.clear();
        samples.clear();
        let mut saw_sorted = false;
        {
            let mut meta_out = Vec::new();
            decode_rw2_stream(
                &rw2_sorted,
                DecodeOptions::rw2(),
                &mut scratch,
                &mut resolved,
                &mut label_bytes,
                &mut samples,
                &mut meta_out,
                |s| {
                    saw_sorted = true;
                    assert!(s.labels_sorted_unique);
                    assert!(s.raw_label_bytes_range.is_some());
                    assert!(!s.encoded_labels.windows(5).any(|w| w == b"empty"));
                    assert!(s.raw_encoded_labels.windows(5).any(|w| w == b"empty"));
                },
            )
            .unwrap();
        }
        assert!(saw_sorted);
    }

    #[test]
    fn test_rw1_to_rw2_preserves_non_utf8_label_and_exemplar_bytes() {
        let bad_name: &[u8] = b"bad_\xff_name";
        let bad_val: &[u8] = b"bad_\xfe_val";
        let bad_ex_name: &[u8] = b"ex_\x80_k";
        let bad_ex_val: &[u8] = b"ex_\xff_v";

        let rw1 = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_strs("__name__", "non_utf8_rw2_metric"),
                    cortexpb::LabelPair {
                        name: bytes::Bytes::from_static(bad_name),
                        value: bytes::Bytes::from_static(bad_val),
                    },
                ],
                samples: vec![cortexpb::Sample {
                    value: 42.0,
                    timestamp_ms: 1_000,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![cortexpb::LabelPair {
                        name: bytes::Bytes::from_static(bad_ex_name),
                        value: bytes::Bytes::from_static(bad_ex_val),
                    }],
                    value: 1.5,
                    timestamp_ms: 1_000,
                }],
                ..Default::default()
            }],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };

        // Struct-based RW2 conversion rejects non-UTF-8 instead of silently erasing to "".
        assert_eq!(
            from_write_request_to_rw2(&rw1, 0, None),
            Err(EncodeRw2Error::InvalidUtf8)
        );

        // Wire-level RW2 conversion preserves raw non-UTF-8 bytes in symbols_rw2 (both Rw2 and KafkaV2).
        for (opts, offset, common) in [
            (DecodeOptions::rw2(), 0, None),
            (
                DecodeOptions::kafka_v2(),
                V2_RECORD_SYMBOL_OFFSET,
                Some(v2_common_symbols()),
            ),
        ] {
            let mut wire = Vec::new();
            encode_write_request_rw2_bytes(&rw1, offset, common, &mut wire).unwrap();
            let mut scratch = WriteRequestScratch::new();
            let decoded = decode_write_request(&wire, opts, &mut scratch).unwrap();
            let ts = decoded.timeseries().next().unwrap();
            assert!(!ts.is_labels_utf8_validated());
            let lbls: Vec<(&[u8], &[u8])> =
                ts.labels().map(|l| (l.name, l.value)).collect();
            assert_eq!(
                lbls,
                vec![(&b"__name__"[..], &b"non_utf8_rw2_metric"[..]), (bad_name, bad_val)]
            );
            let ex = ts.exemplars().next().unwrap();
            let ex_lbls: Vec<(&[u8], &[u8])> =
                ex.labels().map(|l| (l.name, l.value)).collect();
            assert_eq!(ex_lbls, vec![(bad_ex_name, bad_ex_val)]);
        }
    }

    #[test]
    fn test_encode_write_request_rw2_split_raw_bytes_preserves_non_utf8_across_splits() {
        let mut rw1 = cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            skip_label_validation: true,
            ..Default::default()
        };
        for i in 0..10u8 {
            let bad_val = vec![b'v', b'_', 0xff, 0xfe, b'0' + i];
            let bad_ex_val = vec![b'e', 0x80, b'0' + i];
            rw1.timeseries.push(cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "raw_split_metric"),
                    cortexpb::LabelPair {
                        name: bytes::Bytes::from_static(b"k\xff"),
                        value: bytes::Bytes::from(bad_val),
                    },
                ],
                samples: vec![cortexpb::Sample {
                    value: i as f64,
                    timestamp_ms: 1000 + i as i64,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![cortexpb::LabelPair {
                        name: bytes::Bytes::from_static(b"ex\xfe"),
                        value: bytes::Bytes::from(bad_ex_val),
                    }],
                    value: 1.0,
                    timestamp_ms: 1000 + i as i64,
                }],
                ..Default::default()
            });
        }

        let parts = encode_write_request_rw2_split_raw_bytes(
            &rw1,
            150,
            V2_RECORD_SYMBOL_OFFSET,
            Some(v2_common_symbols()),
        )
        .unwrap();
        assert!(parts.len() > 1, "expected split into >1 parts, got {}", parts.len());

        let mut scratch = WriteRequestScratch::new();
        let mut decoded_series = 0usize;
        for (idx, part) in parts.iter().enumerate() {
            let decoded =
                decode_write_request(part, DecodeOptions::kafka_v2(), &mut scratch).unwrap();
            assert!(decoded.skip_label_validation);
            for ts in decoded.timeseries() {
                let lbls: Vec<(&[u8], &[u8])> =
                    ts.labels().map(|l| (l.name, l.value)).collect();
                assert_eq!(lbls[0], (&b"__name__"[..], &b"raw_split_metric"[..]));
                assert_eq!(lbls[1].0, &b"k\xff"[..]);
                assert_eq!(lbls[1].1[0..4], [b'v', b'_', 0xff, 0xfe]);
                let ex = ts.exemplars().next().unwrap();
                let ex_lbls: Vec<(&[u8], &[u8])> =
                    ex.labels().map(|l| (l.name, l.value)).collect();
                assert_eq!(ex_lbls[0].0, &b"ex\xfe"[..]);
                assert_eq!(ex_lbls[0].1[0..2], [b'e', 0x80]);
                decoded_series += 1;
            }
            let _ = idx;
        }
        assert_eq!(decoded_series, 10);
    }
}
