//! Ported from github.com/grafana/mimir/pkg/mimirpb/timeseries.go and timeseries_pools.go.

use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use prost::Message as _;

use crate::compat::LabelAdapter;
use crate::cortexpb;
use crate::zerocopy::{decode_write_request, DecodeError, DecodeMode, DecodeOptions, WriteRequestScratch};

pub const MIN_PREALLOCATED_TIMESERIES: usize = 100;
pub const MIN_PREALLOCATED_LABELS: usize = 20;
pub const MAX_PREALLOCATED_LABELS: usize = 200;
pub const MIN_PREALLOCATED_SAMPLES_PER_SERIES: usize = 10;
pub const MAX_PREALLOCATED_SAMPLES_PER_SERIES: usize = 100;
pub const MAX_PREALLOCATED_HISTOGRAMS_PER_SERIES: usize = 100;
pub const MIN_PREALLOCATED_EXEMPLARS_PER_SERIES: usize = 1;
pub const MAX_PREALLOCATED_EXEMPLARS_PER_SERIES: usize = 10;

pub const REMOTE_WRITE_VERSION_1: &str = "1.0";
pub const REMOTE_WRITE_VERSION_2: &str = "2.0";

static TIMESERIES_UNMARSHAL_CACHING_ENABLED: AtomicBool = AtomicBool::new(true);

pub fn set_timeseries_unmarshal_caching_enabled(enabled: bool) {
    TIMESERIES_UNMARSHAL_CACHING_ENABLED.store(enabled, Ordering::Relaxed);
}

pub fn is_timeseries_unmarshal_caching_enabled() -> bool {
    TIMESERIES_UNMARSHAL_CACHING_ENABLED.load(Ordering::Relaxed)
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PoolExemplar {
    pub labels: Vec<LabelAdapter>,
    pub value: f64,
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PoolTimeSeries {
    pub labels: Vec<LabelAdapter>,
    pub samples: Vec<cortexpb::Sample>,
    pub exemplars: Vec<PoolExemplar>,
    pub histograms: Vec<cortexpb::Histogram>,
    pub created_timestamp: i64,
    pub skip_unmarshaling_exemplars: bool,
}

impl Default for PoolTimeSeries {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolTimeSeries {
    pub fn new() -> Self {
        Self {
            labels: Vec::with_capacity(MIN_PREALLOCATED_LABELS),
            samples: Vec::with_capacity(MIN_PREALLOCATED_SAMPLES_PER_SERIES),
            exemplars: Vec::with_capacity(MIN_PREALLOCATED_EXEMPLARS_PER_SERIES),
            histograms: Vec::new(),
            created_timestamp: 0,
            skip_unmarshaling_exemplars: false,
        }
    }

    pub fn to_proto(&self) -> cortexpb::TimeSeries {
        cortexpb::TimeSeries {
            labels: self.labels.iter().map(Into::into).collect(),
            samples: self.samples.clone(),
            exemplars: self
                .exemplars
                .iter()
                .map(|e| cortexpb::Exemplar {
                    labels: e.labels.iter().map(Into::into).collect(),
                    value: e.value,
                    timestamp_ms: e.timestamp_ms,
                })
                .collect(),
            histograms: self.histograms.clone(),
            created_timestamp: self.created_timestamp,
        }
    }

    pub fn from_proto(ts: &cortexpb::TimeSeries) -> Self {
        Self {
            labels: ts.labels.iter().map(Into::into).collect(),
            samples: ts.samples.clone(),
            exemplars: ts
                .exemplars
                .iter()
                .map(|e| PoolExemplar {
                    labels: e.labels.iter().map(Into::into).collect(),
                    value: e.value,
                    timestamp_ms: e.timestamp_ms,
                })
                .collect(),
            histograms: ts.histograms.clone(),
            created_timestamp: ts.created_timestamp,
            skip_unmarshaling_exemplars: false,
        }
    }

    pub fn encoded_len(&self) -> usize {
        self.to_proto().encoded_len()
    }

    pub fn marshal(&self) -> Vec<u8> {
        self.to_proto().encode_to_vec()
    }

    pub fn unmarshal(&mut self, data: &[u8]) -> Result<(), prost::DecodeError> {
        let proto = cortexpb::TimeSeries::decode(data)?;
        self.labels = proto.labels.iter().map(Into::into).collect();
        self.samples = proto.samples;
        if !self.skip_unmarshaling_exemplars {
            self.exemplars = proto
                .exemplars
                .iter()
                .map(|e| PoolExemplar {
                    labels: e.labels.iter().map(Into::into).collect(),
                    value: e.value,
                    timestamp_ms: e.timestamp_ms,
                })
                .collect();
        }
        self.histograms = proto.histograms;
        self.created_timestamp = proto.created_timestamp;
        Ok(())
    }

    /// Ported from `TimeSeries.MakeReferencesSafeToRetain` in `pkg/mimirpb/timeseries.go`.
    pub fn make_references_safe_to_retain(&mut self) {
        for l in &mut self.labels {
            l.name = l.name.clone();
            l.value = l.value.clone();
        }
        for ex in &mut self.exemplars {
            for l in &mut ex.labels {
                l.name = l.name.clone();
                l.value = l.value.clone();
            }
        }
    }
}

/// Ported from `ClearExemplars` in `pkg/mimirpb/timeseries.go`.
pub fn clear_exemplars(ts: &mut PoolTimeSeries) {
    if ts.exemplars.capacity() > MAX_PREALLOCATED_EXEMPLARS_PER_SERIES {
        ts.exemplars = Vec::new();
        return;
    }
    ts.exemplars.clear();
}

/// Thread-safe pool of `Box<PoolTimeSeries>` and `Vec<PreallocTimeseries>`.
#[derive(Default)]
#[allow(clippy::vec_box)]
pub struct TimeSeriesPool {
    ts_pool: Mutex<Vec<Box<PoolTimeSeries>>>,
    slice_pool: Mutex<Vec<Vec<PreallocTimeseries>>>,
    yolo_pool: Mutex<Vec<Vec<u8>>>,
}

impl TimeSeriesPool {
    pub const fn new() -> Self {
        Self {
            ts_pool: Mutex::new(Vec::new()),
            slice_pool: Mutex::new(Vec::new()),
            yolo_pool: Mutex::new(Vec::new()),
        }
    }

    pub fn put_raw_timeseries_for_test(&self, ts: Box<PoolTimeSeries>) {
        self.ts_pool.lock().push(ts);
    }

    pub fn drain_timeseries_for_test(&self) {
        self.ts_pool.lock().clear();
    }

    /// Ported from `TimeseriesFromPool` in `pkg/mimirpb/timeseries_pools.go`.
    pub fn timeseries_from_pool(&self) -> Box<PoolTimeSeries> {
        let ts = self
            .ts_pool
            .lock()
            .pop()
            .unwrap_or_else(|| Box::new(PoolTimeSeries::new()));
        if !ts.labels.is_empty()
            || !ts.samples.is_empty()
            || !ts.histograms.is_empty()
            || !ts.exemplars.is_empty()
            || ts.created_timestamp != 0
            || ts.skip_unmarshaling_exemplars
        {
            panic!("pool returned dirty TimeSeries: this indicates a bug where ReuseTimeseries was called on a TimeSeries still in use");
        }
        ts
    }

    /// Ported from `ReuseTimeseries` in `pkg/mimirpb/timeseries_pools.go`.
    pub fn reuse_timeseries(&self, mut ts: Box<PoolTimeSeries>) {
        if ts.labels.capacity() > MAX_PREALLOCATED_LABELS {
            ts.labels = Vec::new();
        } else {
            ts.labels.clear();
        }
        if ts.samples.capacity() > MAX_PREALLOCATED_SAMPLES_PER_SERIES {
            ts.samples = Vec::new();
        } else {
            ts.samples.clear();
        }
        if ts.histograms.capacity() > MAX_PREALLOCATED_HISTOGRAMS_PER_SERIES {
            ts.histograms = Vec::new();
        } else {
            ts.histograms.clear();
        }
        ts.created_timestamp = 0;
        ts.skip_unmarshaling_exemplars = false;
        clear_exemplars(&mut ts);
        self.ts_pool.lock().push(ts);
    }

    pub fn prealloc_timeseries_slice_from_pool(&self) -> Vec<PreallocTimeseries> {
        self.slice_pool
            .lock()
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(MIN_PREALLOCATED_TIMESERIES))
    }

    pub fn reuse_slice_only(&self, mut slice: Vec<PreallocTimeseries>) {
        slice.clear();
        self.slice_pool.lock().push(slice);
    }

    pub fn reuse_slice(&self, mut slice: Vec<PreallocTimeseries>) {
        if slice.capacity() == 0 {
            return;
        }
        for mut item in slice.drain(..) {
            if let Some(ts) = item.timeseries.take() {
                self.reuse_timeseries(ts);
            }
            if let Some(mut ys) = item.yolo_slice.take() {
                ys.clear();
                self.yolo_pool.lock().push(ys);
            }
            item.marshalled_data = None;
        }
        self.slice_pool.lock().push(slice);
    }

    pub fn yolo_slice_from_pool(&self) -> Vec<u8> {
        self.yolo_pool
            .lock()
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(200))
    }

    pub fn reuse_yolo_slice(&self, mut val: Vec<u8>) {
        val.clear();
        self.yolo_pool.lock().push(val);
    }
}

static GLOBAL_POOL: TimeSeriesPool = TimeSeriesPool::new();

pub fn global_timeseries_pool() -> &'static TimeSeriesPool {
    &GLOBAL_POOL
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PreallocTimeseries {
    pub timeseries: Option<Box<PoolTimeSeries>>,
    pub yolo_slice: Option<Vec<u8>>,
    pub marshalled_data: Option<Vec<u8>>,
    pub skip_unmarshaling_exemplars: bool,
}

impl PreallocTimeseries {
    pub fn from_timeseries(ts: PoolTimeSeries) -> Self {
        Self {
            timeseries: Some(Box::new(ts)),
            yolo_slice: None,
            marshalled_data: None,
            skip_unmarshaling_exemplars: false,
        }
    }

    pub fn ts(&self) -> &PoolTimeSeries {
        self.timeseries
            .as_deref()
            .expect("PreallocTimeseries missing TimeSeries")
    }

    pub fn ts_mut(&mut self) -> &mut PoolTimeSeries {
        self.timeseries
            .as_deref_mut()
            .expect("PreallocTimeseries missing TimeSeries")
    }

    pub fn clear_unmarshal_data(&mut self) {
        self.marshalled_data = None;
    }

    /// Ported from `PreallocTimeseries.RemoveLabel` in `pkg/mimirpb/timeseries.go`.
    pub fn remove_label(&mut self, label_name: &str) {
        let ts = self.ts_mut();
        if let Some(pos) = ts.labels.iter().position(|l| l.name == label_name) {
            ts.labels.remove(pos);
            self.clear_unmarshal_data();
        }
    }

    /// Ported from `PreallocTimeseries.SetLabels` in `pkg/mimirpb/timeseries.go`.
    pub fn set_labels(&mut self, lbls: Vec<LabelAdapter>) {
        self.ts_mut().labels = lbls;
        self.clear_unmarshal_data();
    }

    /// Ported from `PreallocTimeseries.RemoveEmptyLabelValues` in `pkg/mimirpb/timeseries.go`.
    pub fn remove_empty_label_values(&mut self) {
        let ts = self.ts_mut();
        let orig_len = ts.labels.len();
        ts.labels.retain(|l| !l.value.is_empty());
        if ts.labels.len() != orig_len {
            self.clear_unmarshal_data();
        }
    }

    /// Ported from `PreallocTimeseries.SortLabelsIfNeeded` in `pkg/mimirpb/timeseries.go`.
    pub fn sort_labels_if_needed(&mut self) {
        let labels = &mut self.ts_mut().labels;
        let mut sorted = true;
        for w in labels.windows(2) {
            if w[0].name > w[1].name {
                sorted = false;
                break;
            }
        }
        if sorted {
            return;
        }
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        self.clear_unmarshal_data();
    }

    pub fn clear_exemplars(&mut self) {
        clear_exemplars(self.ts_mut());
        self.clear_unmarshal_data();
    }

    /// Ported from `PreallocTimeseries.ResizeExemplars` in `pkg/mimirpb/timeseries.go`.
    pub fn resize_exemplars(&mut self, new_size: usize) {
        let ts = self.ts_mut();
        if ts.exemplars.len() <= new_size {
            return;
        }
        ts.exemplars.truncate(new_size);
        self.clear_unmarshal_data();
    }

    pub fn samples_updated(&mut self) {
        self.clear_unmarshal_data();
    }

    pub fn histograms_updated(&mut self) {
        self.clear_unmarshal_data();
    }

    pub fn delete_exemplar_by_moving_last(&mut self, idx: usize) {
        let ts = self.ts_mut();
        let last = ts.exemplars.len() - 1;
        if idx < last {
            ts.exemplars.swap(idx, last);
        }
        ts.exemplars.pop();
        self.clear_unmarshal_data();
    }

    pub fn sort_exemplars(&mut self) {
        self.ts_mut()
            .exemplars
            .sort_by_key(|a| a.timestamp_ms);
        self.clear_unmarshal_data();
    }

    pub fn unmarshal(&mut self, data: &[u8]) -> Result<(), prost::DecodeError> {
        if is_timeseries_unmarshal_caching_enabled() {
            self.marshalled_data = Some(data.to_vec());
        } else {
            self.marshalled_data = None;
        }
        let mut ts = global_timeseries_pool().timeseries_from_pool();
        ts.skip_unmarshaling_exemplars = self.skip_unmarshaling_exemplars;
        ts.unmarshal(data)?;
        self.timeseries = Some(ts);
        Ok(())
    }

    pub fn size(&self) -> usize {
        if let Some(ref cached) = self.marshalled_data {
            return cached.len();
        }
        self.ts().encoded_len()
    }

    pub fn marshal(&self) -> Vec<u8> {
        if let Some(ref cached) = self.marshalled_data {
            return cached.clone();
        }
        self.ts().marshal()
    }

    pub fn marshal_to(&self, buf: &mut [u8]) -> Result<usize, String> {
        let bytes = self.marshal();
        if buf.len() < bytes.len() {
            return Err("destination buffer too small".to_string());
        }
        buf[..bytes.len()].copy_from_slice(&bytes);
        Ok(bytes.len())
    }

    pub fn marshal_to_sized_buffer(&self, buf: &mut [u8]) -> Result<usize, String> {
        self.marshal_to(buf)
    }
}

/// Copies `src` into `buf` returning the copied UTF-8 slice and the remaining uninitialized/free slice.
/// Ported from `copyToYoloString` in `pkg/mimirpb/timeseries.go`.
pub fn copy_to_yolo_string<'a>(buf: &'a mut [u8], src: &str) -> (&'a str, &'a mut [u8]) {
    let len = src.len();
    let (dst, rest) = buf.split_at_mut(len);
    dst.copy_from_slice(src.as_bytes());
    let s = std::str::from_utf8(dst).expect("src is valid utf8");
    (s, rest)
}

fn count_total_label_len(ts: &PoolTimeSeries, include_exemplars: bool) -> usize {
    let mut label_len = 0usize;
    for l in &ts.labels {
        label_len += l.name.len() + l.value.len();
    }
    if include_exemplars {
        for ex in &ts.exemplars {
            for l in &ex.labels {
                label_len += l.name.len() + l.value.len();
            }
        }
    }
    label_len
}

/// Deep copies `src` into `dst` without sharing any backing slices or allocations.
/// Ported from `DeepCopyTimeseries` in `pkg/mimirpb/timeseries.go`.
pub fn deep_copy_timeseries(
    mut dst: PreallocTimeseries,
    src: &PreallocTimeseries,
    keep_histograms: bool,
    keep_exemplars: bool,
) -> PreallocTimeseries {
    if dst.timeseries.is_none() {
        dst.timeseries = Some(global_timeseries_pool().timeseries_from_pool());
    }
    let src_ts = src.ts();
    let required_cap = count_total_label_len(src_ts, keep_exemplars);
    let mut yolo = global_timeseries_pool().yolo_slice_from_pool();
    if yolo.capacity() < required_cap {
        yolo = Vec::with_capacity(required_cap);
    }
    yolo.clear();
    // Write all label bytes into yolo_slice to mirror Go's single-buffer allocation sizing.
    for l in &src_ts.labels {
        yolo.extend_from_slice(l.name.as_bytes());
        yolo.extend_from_slice(l.value.as_bytes());
    }
    if keep_exemplars {
        for ex in &src_ts.exemplars {
            for l in &ex.labels {
                yolo.extend_from_slice(l.name.as_bytes());
                yolo.extend_from_slice(l.value.as_bytes());
            }
        }
    }
    dst.yolo_slice = Some(yolo);

    let dst_ts = dst.timeseries.as_deref_mut().unwrap();
    dst_ts.labels = src_ts.labels.clone();
    dst_ts.created_timestamp = src_ts.created_timestamp;
    dst_ts.skip_unmarshaling_exemplars = src_ts.skip_unmarshaling_exemplars;
    dst_ts.samples = src_ts.samples.clone();

    if keep_histograms {
        dst_ts.histograms = src_ts.histograms.clone();
    } else {
        dst_ts.histograms = Vec::new();
    }

    if keep_exemplars {
        dst_ts.exemplars = src_ts.exemplars.clone();
    } else {
        dst_ts.exemplars = Vec::new();
    }

    dst
}

/// Ported from `PreallocWriteRequest` in `pkg/mimirpb/timeseries.go` and `compat_rw2.go`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PreallocWriteRequest {
    pub write_request: cortexpb::WriteRequest,
    pub skip_unmarshaling_exemplars: bool,
    pub unmarshal_from_rw2: bool,
    pub rw2_symbol_offset: u32,
    pub rw2_common_symbols: Vec<String>,
    pub skip_normalize_metadata_metric_name: bool,
    pub skip_deduplicate_metadata: bool,
}

impl PreallocWriteRequest {
    pub fn protocol_version(&self) -> &'static str {
        if self.unmarshal_from_rw2 {
            REMOTE_WRITE_VERSION_2
        } else {
            REMOTE_WRITE_VERSION_1
        }
    }

    pub fn unmarshal(&mut self, data: &[u8]) -> Result<(), DecodeError> {
        let common_strs: Vec<&str> = self.rw2_common_symbols.iter().map(String::as_str).collect();
        let mode = if self.unmarshal_from_rw2 {
            if self.rw2_symbol_offset == 0 && common_strs.is_empty() {
                DecodeMode::Rw2
            } else {
                DecodeMode::Rw2Custom {
                    offset: self.rw2_symbol_offset,
                    common_symbols: &common_strs,
                }
            }
        } else {
            DecodeMode::Classic
        };

        let opts = DecodeOptions {
            mode,
            skip_unmarshaling_exemplars: self.skip_unmarshaling_exemplars,
            skip_normalize_metadata_metric_name: self.skip_normalize_metadata_metric_name,
            skip_deduplicate_metadata: self.skip_deduplicate_metadata,
        };

        let mut scratch = WriteRequestScratch::new();
        let borrowed = decode_write_request(data, opts, &mut scratch)?;
        self.write_request = borrowed.to_owned_write_request();
        Ok(())
    }
}

/// Ported from `ToWriteRequest`, `NewWriteRequest`, `AddFloatSeries`, `AddHistogramSeries`,
/// and `AddExemplarsAt` in `pkg/mimirpb/compat.go`.
pub fn to_write_request(
    lbls: &[Vec<LabelAdapter>],
    samples: &[cortexpb::Sample],
    exemplars: Option<&[Option<cortexpb::Exemplar>]>,
    metadata: Vec<cortexpb::MetricMetadata>,
    source: cortexpb::write_request::SourceEnum,
) -> cortexpb::WriteRequest {
    let mut req = new_write_request(metadata, source);
    add_float_series(&mut req, lbls, samples, exemplars);
    req
}

pub fn new_write_request(
    metadata: Vec<cortexpb::MetricMetadata>,
    source: cortexpb::write_request::SourceEnum,
) -> cortexpb::WriteRequest {
    cortexpb::WriteRequest {
        metadata,
        source: source as i32,
        ..Default::default()
    }
}

pub fn add_float_series(
    req: &mut cortexpb::WriteRequest,
    lbls: &[Vec<LabelAdapter>],
    samples: &[cortexpb::Sample],
    exemplars: Option<&[Option<cortexpb::Exemplar>]>,
) {
    for (i, &s) in samples.iter().enumerate() {
        let mut ts = cortexpb::TimeSeries {
            labels: lbls[i]
                .iter()
                .map(|l| cortexpb::LabelPair {
                    name: Bytes::copy_from_slice(l.name.as_bytes()),
                    value: Bytes::copy_from_slice(l.value.as_bytes()),
                })
                .collect(),
            samples: vec![s],
            ..Default::default()
        };
        if let Some(exs) = exemplars {
            if let Some(Some(ex)) = exs.get(i) {
                ts.exemplars.push(ex.clone());
            }
        }
        req.timeseries.push(ts);
    }
}
