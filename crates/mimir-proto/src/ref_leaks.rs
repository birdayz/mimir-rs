//! Ported from github.com/grafana/mimir/pkg/mimirpb/ref_leaks.go and custom.go.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use prometheus::{Counter, Gauge, Opts, Registry};
use prost::Message as _;

use crate::cortexpb;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefLeakError(pub &'static str);

impl std::fmt::Display for RefLeakError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for RefLeakError {}

#[derive(Debug, Clone)]
pub struct InstrumentRefLeaksConfig {
    pub percentage: f64,
    pub before_reuse_period: Duration,
    pub max_inflight_instrumented_bytes: u64,
}

impl Default for InstrumentRefLeaksConfig {
    fn default() -> Self {
        Self {
            percentage: 0.0,
            before_reuse_period: Duration::from_secs(120),
            max_inflight_instrumented_bytes: 0,
        }
    }
}

impl InstrumentRefLeaksConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.percentage < 0.0 || self.percentage > 100.0 {
            return Err(format!(
                "percentage must be in [0-100], got: {}",
                self.percentage
            ));
        }
        Ok(())
    }

    pub fn tracker(&self, registry: Option<&Registry>) -> RefLeaksTracker {
        if self.percentage <= 0.0 {
            return RefLeaksTracker::disabled();
        }

        let instrument_one_in = (100.0 / self.percentage).trunc() as u64;
        let max_inflight = if self.max_inflight_instrumented_bytes == 0 {
            u64::MAX
        } else {
            self.max_inflight_instrumented_bytes
        };

        let inflight_gauge = Gauge::with_opts(Opts::new(
            "mimir_reference_leaks_inflight_instrumented_bytes",
            "Total bytes of buffers instrumented for reference leak detection.",
        ))
        .expect("valid gauge opts");
        let total_counter = Counter::with_opts(Opts::new(
            "mimir_reference_leaks_instrumented_buffers_total",
            "Total number of buffers instrumented for reference leak detection.",
        ))
        .expect("valid counter opts");

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(inflight_gauge.clone()));
            let _ = reg.register(Box::new(total_counter.clone()));
        }

        RefLeaksTracker {
            inner: Some(Arc::new(RefLeaksTrackerInner {
                instrument_one_in,
                wait_before_reuse: self.before_reuse_period,
                max_inflight_instrumented_bytes: max_inflight,
                unmarshaled_with_buffer_ref_count: AtomicU64::new(0),
                inflight_instrumented_bytes: AtomicU64::new(0),
                inflight_instrumented_bytes_metric: inflight_gauge,
                instrumented_buffers_total_metric: total_counter,
                delayed_unmap: Mutex::new(Vec::new()),
            })),
        }
    }
}

pub(crate) fn page_size() -> usize {
    4096
}

#[inline]
pub fn round_up_to_multiple(n: usize, of: usize) -> usize {
    if of == 0 {
        return n;
    }
    n.div_ceil(of) * of
}

struct RefLeaksTrackerInner {
    instrument_one_in: u64,
    wait_before_reuse: Duration,
    max_inflight_instrumented_bytes: u64,
    unmarshaled_with_buffer_ref_count: AtomicU64,
    inflight_instrumented_bytes: AtomicU64,
    inflight_instrumented_bytes_metric: Gauge,
    instrumented_buffers_total_metric: Counter,
    delayed_unmap: Mutex<Vec<(Instant, usize)>>,
}

impl RefLeaksTrackerInner {
    fn unmap(&self, aligned_len: usize) {
        let prev = self
            .inflight_instrumented_bytes
            .fetch_sub(aligned_len as u64, Ordering::SeqCst);
        let new_inflight = prev.saturating_sub(aligned_len as u64);
        self.inflight_instrumented_bytes_metric
            .set(new_inflight as f64);
    }
}

#[derive(Clone, Default)]
pub struct RefLeaksTracker {
    inner: Option<Arc<RefLeaksTrackerInner>>,
}

impl RefLeaksTracker {
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    pub fn instrumented_buffers_total(&self) -> f64 {
        self.inner
            .as_ref()
            .map_or(0.0, |i| i.instrumented_buffers_total_metric.get())
    }

    pub fn inflight_instrumented_bytes(&self) -> f64 {
        self.inner
            .as_ref()
            .map_or(0.0, |i| i.inflight_instrumented_bytes_metric.get())
    }

    pub fn maybe_instrument_ref_leaks(&self, data: &[u8]) -> Option<TrackedBuffer> {
        let inner = self.inner.as_ref()?;
        if data.is_empty() || inner.instrument_one_in == 0 {
            return None;
        }
        let count = inner
            .unmarshaled_with_buffer_ref_count
            .fetch_add(1, Ordering::SeqCst)
            + 1;
        if !count.is_multiple_of(inner.instrument_one_in) {
            return None;
        }

        let aligned_len = round_up_to_multiple(data.len(), page_size());
        let inflight = inner
            .inflight_instrumented_bytes
            .fetch_add(aligned_len as u64, Ordering::SeqCst)
            + aligned_len as u64;
        if inflight > inner.max_inflight_instrumented_bytes {
            inner
                .inflight_instrumented_bytes
                .fetch_sub(aligned_len as u64, Ordering::SeqCst);
            return None;
        }

        inner.instrumented_buffers_total_metric.inc();
        inner.inflight_instrumented_bytes_metric.set(inflight as f64);

        Some(TrackedBuffer::new_instrumented(
            Bytes::copy_from_slice(data),
            aligned_len,
            inner.wait_before_reuse,
            Arc::clone(inner),
        ))
    }
}

struct TrackedBufferState {
    data: Bytes,
    ref_count: AtomicI64,
    freed: AtomicBool,
    aligned_len: usize,
    wait_before_reuse: Duration,
    tracker: Option<Arc<RefLeaksTrackerInner>>,
}

/// Reference-counted buffer matching `grpc/mem.Buffer` + `instrumentLeaksBuf`.
#[derive(Clone)]
pub struct TrackedBuffer {
    state: Arc<TrackedBufferState>,
}

impl std::fmt::Debug for TrackedBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackedBuffer")
            .field("len", &self.state.data.len())
            .field("ref_count", &self.state.ref_count.load(Ordering::Relaxed))
            .field("freed", &self.state.freed.load(Ordering::Relaxed))
            .finish()
    }
}

impl TrackedBuffer {
    pub fn from_bytes(data: Bytes) -> Self {
        Self {
            state: Arc::new(TrackedBufferState {
                data,
                ref_count: AtomicI64::new(1),
                freed: AtomicBool::new(false),
                aligned_len: 0,
                wait_before_reuse: Duration::ZERO,
                tracker: None,
            }),
        }
    }

    fn new_instrumented(
        data: Bytes,
        aligned_len: usize,
        wait_before_reuse: Duration,
        tracker: Arc<RefLeaksTrackerInner>,
    ) -> Self {
        Self {
            state: Arc::new(TrackedBufferState {
                data,
                ref_count: AtomicI64::new(1),
                freed: AtomicBool::new(false),
                aligned_len,
                wait_before_reuse,
                tracker: Some(tracker),
            }),
        }
    }

    pub fn inc_ref(&self) {
        self.state.ref_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn free(&self) {
        let prev = self.state.ref_count.fetch_sub(1, Ordering::SeqCst);
        let new_rc = prev - 1;
        if new_rc == 0 {
            self.state.freed.store(true, Ordering::SeqCst);
            if let Some(ref tracker) = self.state.tracker {
                if !self.state.wait_before_reuse.is_zero() {
                    if let Ok(mut q) = tracker.delayed_unmap.lock() {
                        q.push((
                            Instant::now() + self.state.wait_before_reuse,
                            self.state.aligned_len,
                        ));
                    }
                    return;
                }
                tracker.unmap(self.state.aligned_len);
            }
        } else if new_rc < 0 {
            panic!("instrumentLeaksBuf reference count below zero");
        }
    }

    pub fn is_freed(&self) -> bool {
        self.state.freed.load(Ordering::SeqCst)
    }

    pub fn read_only_data(&self) -> &[u8] {
        if self.is_freed() {
            panic!("runtime error: invalid memory address or nil pointer dereference");
        }
        &self.state.data
    }

    pub fn read_only_data_unchecked(&self) -> &[u8] {
        &self.state.data
    }

    pub fn buffer_key(&self) -> usize {
        if self.state.data.is_empty() {
            0
        } else {
            self.state.data.as_ptr() as usize
        }
    }
}

/// Guarded string slice referencing a `TrackedBuffer` (analogous to `UnsafeMutableString`).
#[derive(Clone, Debug)]
pub struct TrackedStr {
    buf: TrackedBuffer,
    start: usize,
    end: usize,
}

impl TrackedStr {
    pub fn as_str(&self) -> &str {
        let slice = self.buf.read_only_data();
        std::str::from_utf8(&slice[self.start..self.end]).expect("valid utf-8")
    }
}

/// Ported from `BufferHolder` in `pkg/mimirpb/custom.go`.
#[derive(Clone, Default, Debug)]
pub struct BufferHolder {
    buffer: Option<TrackedBuffer>,
}

impl BufferHolder {
    pub fn new(buffer: Option<TrackedBuffer>) -> Self {
        Self { buffer }
    }

    pub fn buffer(&self) -> Option<&TrackedBuffer> {
        self.buffer.as_ref()
    }

    pub fn set_buffer(&mut self, buf: TrackedBuffer) {
        self.buffer = Some(buf);
    }

    /// Idempotent buffer release matching `BufferHolder.FreeBuffer` in `pkg/mimirpb/custom.go`.
    pub fn free_buffer(&mut self) {
        if let Some(buf) = self.buffer.take() {
            buf.free();
        }
    }
}

/// Wrapper around `cortexpb::WriteRequest` carrying `BufferHolder` and `sourceBufferHolders`
/// matching `WriteRequest` buffer lifecycle methods in `pkg/mimirpb/custom.go`.
#[derive(Clone, Default, Debug)]
pub struct WriteRequestWithBuffers {
    pub inner: cortexpb::WriteRequest,
    pub buffer_holder: BufferHolder,
    pub source_buffer_holders: Option<HashMap<usize, BufferHolder>>,
    pub tracked_label_refs: Vec<(TrackedStr, TrackedStr)>,
}

impl WriteRequestWithBuffers {
    pub fn buffer(&self) -> Option<&TrackedBuffer> {
        self.buffer_holder.buffer()
    }

    pub fn set_buffer(&mut self, buf: TrackedBuffer) {
        self.buffer_holder.set_buffer(buf);
    }

    pub fn free_buffer(&mut self) {
        self.buffer_holder.free_buffer();
        if let Some(mut holders) = self.source_buffer_holders.take() {
            for (_, mut h) in holders.drain() {
                h.free_buffer();
            }
        }
    }

    /// Ported from `WriteRequest.AddSourceBufferHolder` in `pkg/mimirpb/custom.go`.
    pub fn add_source_buffer_holder(&mut self, bufh: &BufferHolder) {
        let Some(buf) = bufh.buffer() else {
            return;
        };
        let key = buf.buffer_key();
        let holders = self.source_buffer_holders.get_or_insert_with(HashMap::new);
        if holders.contains_key(&key) {
            return;
        }
        buf.inc_ref();
        holders.insert(key, BufferHolder::new(Some(buf.clone())));
    }
}

/// Ported from `codecV2` in `pkg/mimirpb/custom.go`.
#[derive(Clone, Default)]
pub struct CustomCodec {
    tracker: RefLeaksTracker,
}

impl CustomCodec {
    pub fn new(config: &InstrumentRefLeaksConfig, registry: Option<&Registry>) -> Self {
        Self {
            tracker: config.tracker(registry),
        }
    }

    pub fn tracker(&self) -> &RefLeaksTracker {
        &self.tracker
    }

    pub fn marshal(&self, req: &cortexpb::WriteRequest) -> Vec<u8> {
        req.encode_to_vec()
    }

    pub fn unmarshal(
        &self,
        data: &[u8],
        out: &mut WriteRequestWithBuffers,
    ) -> Result<(), prost::DecodeError> {
        let buf = self
            .tracker
            .maybe_instrument_ref_leaks(data)
            .unwrap_or_else(|| TrackedBuffer::from_bytes(Bytes::copy_from_slice(data)));

        // Matches codecV2.Unmarshal: decode from buffer, then hold an extra ref for the holder.
        let decoded = cortexpb::WriteRequest::decode(buf.read_only_data())?;
        out.inner = decoded;

        // Track byte ranges of labels within buf for leak detection.
        out.tracked_label_refs.clear();
        let raw = buf.read_only_data();
        for ts in &out.inner.timeseries {
            for lp in &ts.labels {
                if let Some(pos) = raw.windows(lp.name.len()).position(|w| w == lp.name.as_ref()) {
                    if !lp.name.is_empty() {
                        out.tracked_label_refs.push((
                            TrackedStr {
                                buf: buf.clone(),
                                start: pos,
                                end: pos + lp.name.len(),
                            },
                            TrackedStr {
                                buf: buf.clone(),
                                start: pos,
                                end: pos + lp.name.len(),
                            },
                        ));
                    }
                }
            }
        }

        buf.inc_ref();
        out.set_buffer(buf.clone());
        buf.free(); // Drop initial unmarshal ref, leaving holder ref.
        Ok(())
    }
}
