//! Prometheus `EncFloatHistogram` floating-point native histogram chunk codec.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram.go.

use super::bstream::{BitReader, BitStream};
use super::histogram_meta::{
    adjust_for_inserts, checked_count_spans, count_spans, counter_reset_hint_from_header,
    expand_spans_both_ways, insert_buckets, read_histogram_chunk_layout,
    write_histogram_chunk_layout, BucketIterator, Insert,
};
use super::histogram_model::{
    custom_bucket_bounds_match, is_custom_buckets_schema, is_known_schema, CounterResetHeader,
    CounterResetHint, FloatHistogram, HistogramSpan, EXPONENTIAL_SCHEMA_MAX,
    EXPONENTIAL_SCHEMA_MAX_RESERVED,
};
use super::varbit::{put_varbit_int, read_varbit_int};
use super::xor::{xor_read, xor_write};
use super::{is_stale_nan, stale_nan, ChunkError, Encoding};

const HISTOGRAM_FLAG_POS: usize = 2;
const HISTOGRAM_HEADER_SIZE: usize = 3;
const CHUNK_ALLOCATION_SIZE: usize = 128;
const CHUNK_COMPACT_CAPACITY_THRESHOLD: usize = 32;

#[derive(Clone, Copy, Debug, Default)]
#[doc(hidden)]
pub struct XorValue {
    pub value: f64,
    pub leading: u8,
    pub trailing: u8,
}

impl XorValue {
    pub(super) const fn init_unseen() -> Self {
        Self {
            value: 0.0,
            leading: 0xff,
            trailing: 0,
        }
    }
}

/// Outcome of appending a float histogram sample to a [`FloatHistogramChunk`].
#[derive(Debug)]
pub enum AppendFloatHistogramOutcome {
    Appended { recoded: bool },
    NewChunk(FloatHistogramChunk),
}

/// Prometheus `EncFloatHistogram` (`3`) encoded floating-point native histogram chunk.
#[derive(Clone, Debug)]
pub struct FloatHistogramChunk {
    pub(crate) b: BitStream,
}

impl PartialEq for FloatHistogramChunk {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for FloatHistogramChunk {}

impl Default for FloatHistogramChunk {
    fn default() -> Self {
        Self::new()
    }
}

impl FloatHistogramChunk {
    pub fn new() -> Self {
        Self {
            b: BitStream::with_header(HISTOGRAM_HEADER_SIZE, CHUNK_ALLOCATION_SIZE),
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            b: BitStream::from_bytes(bytes, 0),
        }
    }

    #[inline]
    pub fn reset(&mut self, stream: Vec<u8>) {
        self.b.reset(stream);
    }

    #[inline]
    pub const fn encoding(&self) -> Encoding {
        Encoding::FloatHistogram
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
        if bytes.len() < HISTOGRAM_HEADER_SIZE {
            return 0;
        }
        u16::from_be_bytes([bytes[0], bytes[1]]) as usize
    }

    #[inline]
    pub fn counter_reset_header(&self) -> CounterResetHeader {
        let bytes = self.b.bytes();
        if bytes.len() < HISTOGRAM_HEADER_SIZE {
            return CounterResetHeader::UnknownCounterReset;
        }
        CounterResetHeader::from_header_byte(bytes[HISTOGRAM_FLAG_POS])
    }

    #[inline]
    pub fn compact(&mut self) {
        self.b.compact(CHUNK_COMPACT_CAPACITY_THRESHOLD);
    }

    #[inline]
    pub fn iter(&self) -> FloatHistogramIterator<'_> {
        FloatHistogramIterator::new(self.b.bytes())
    }

    pub fn appender(&mut self) -> Result<FloatHistogramAppender<'_>, ChunkError> {
        if self.b.bytes().len() < HISTOGRAM_HEADER_SIZE {
            return Err(ChunkError::InvalidHeader {
                len: self.b.bytes().len(),
                min: HISTOGRAM_HEADER_SIZE,
            });
        }
        if self.num_samples() == 0 {
            self.b.truncate_and_set_count(HISTOGRAM_HEADER_SIZE, 0);
            return Ok(FloatHistogramAppender {
                chunk: self,
                state: FloatHistogramAppenderState::empty(),
            });
        }

        let (state, (used_bytes, free_bits)) = {
            let mut it = self.iter();
            while it.next_sample()?.is_some() {}

            let p_buckets = it
                .p_buckets
                .iter()
                .enumerate()
                .map(|(i, &val)| XorValue {
                    value: val,
                    leading: it.p_buckets_leading[i],
                    trailing: it.p_buckets_trailing[i],
                })
                .collect();
            let n_buckets = it
                .n_buckets
                .iter()
                .enumerate()
                .map(|(i, &val)| XorValue {
                    value: val,
                    leading: it.n_buckets_leading[i],
                    trailing: it.n_buckets_trailing[i],
                })
                .collect();

            (
                FloatHistogramAppenderState {
                    num_samples: self.num_samples(),
                    counter_reset_header: self.counter_reset_header(),
                    schema: it.schema,
                    z_threshold: it.z_threshold,
                    p_spans: it.p_spans,
                    n_spans: it.n_spans,
                    custom_values: it.custom_values,
                    t: it.t,
                    t_delta: it.t_delta,
                    sum: it.sum,
                    cnt: it.cnt,
                    z_cnt: it.z_cnt,
                    p_buckets,
                    n_buckets,
                },
                it.br.consumed_bytes_and_free_bits(),
            )
        };

        self.b
            .truncate_and_set_count(HISTOGRAM_HEADER_SIZE + used_bytes, free_bits);
        Ok(FloatHistogramAppender { chunk: self, state })
    }

    /// Appends `(st, t, fh)` in O(1) using a cached [`FloatHistogramAppenderState`],
    /// recovering state once via [`Self::appender`] only if `*state` is `None`.
    pub fn append_with_state(
        &mut self,
        state: &mut Option<FloatHistogramAppenderState>,
        st: i64,
        t: i64,
        fh: &FloatHistogram,
    ) -> Result<AppendFloatHistogramOutcome, ChunkError> {
        self.append_with_state_prev(state, None, st, t, fh)
    }

    /// Appends `(st, t, fh)` in O(1) using a cached [`FloatHistogramAppenderState`] and
    /// optional previous-chunk state for computing `CounterResetHeader` on chunk rollover.
    pub fn append_with_state_prev(
        &mut self,
        state: &mut Option<FloatHistogramAppenderState>,
        prev: Option<&FloatHistogramAppenderState>,
        st: i64,
        t: i64,
        fh: &FloatHistogram,
    ) -> Result<AppendFloatHistogramOutcome, ChunkError> {
        let app = match state.take() {
            Some(s) => FloatHistogramAppender { chunk: self, state: s },
            None => self.appender()?,
        };
        let (out, next_state) = app.append_float_histogram_with_state(prev, st, t, fh, false)?;
        *state = Some(next_state);
        Ok(out)
    }
}

/// Snapshot of [`FloatHistogramAppender`] state used to detect counter resets across chunks.
#[derive(Clone, Debug)]
pub struct FloatHistogramAppenderState {
    pub(super) num_samples: usize,
    pub(super) counter_reset_header: CounterResetHeader,
    pub(super) schema: i32,
    pub(super) z_threshold: f64,
    pub(super) p_spans: Vec<HistogramSpan>,
    pub(super) n_spans: Vec<HistogramSpan>,
    pub(super) custom_values: Vec<f64>,
    pub(super) t: i64,
    pub(super) t_delta: i64,
    pub(super) sum: XorValue,
    pub(super) cnt: XorValue,
    pub(super) z_cnt: XorValue,
    pub(super) p_buckets: Vec<XorValue>,
    pub(super) n_buckets: Vec<XorValue>,
}

#[derive(Default)]
#[doc(hidden)]
pub struct AppendableGauge {
    pub p_fwd: Vec<Insert>,
    pub n_fwd: Vec<Insert>,
    pub p_bwd: Vec<Insert>,
    pub n_bwd: Vec<Insert>,
    pub p_merged: Vec<HistogramSpan>,
    pub n_merged: Vec<HistogramSpan>,
    pub ok_to_append: bool,
}

impl FloatHistogramAppenderState {
    pub(super) fn empty() -> Self {
        Self {
            num_samples: 0,
            counter_reset_header: CounterResetHeader::UnknownCounterReset,
            schema: 0,
            z_threshold: 0.0,
            p_spans: Vec::new(),
            n_spans: Vec::new(),
            custom_values: Vec::new(),
            t: i64::MIN,
            t_delta: 0,
            sum: XorValue::init_unseen(),
            cnt: XorValue::init_unseen(),
            z_cnt: XorValue::init_unseen(),
            p_buckets: Vec::new(),
            n_buckets: Vec::new(),
        }
    }

    #[doc(hidden)]
    pub fn appendable(
        &self,
        h: &FloatHistogram,
    ) -> (Vec<Insert>, Vec<Insert>, Vec<Insert>, Vec<Insert>, bool, bool) {
        if self.num_samples > 0 && self.counter_reset_header == CounterResetHeader::GaugeType {
            return (Vec::new(), Vec::new(), Vec::new(), Vec::new(), false, false);
        }
        if h.counter_reset_hint == CounterResetHint::CounterReset {
            return (Vec::new(), Vec::new(), Vec::new(), Vec::new(), false, true);
        }
        if is_stale_nan(h.sum) {
            return (Vec::new(), Vec::new(), Vec::new(), Vec::new(), true, false);
        }
        if is_stale_nan(self.sum.value) {
            return (Vec::new(), Vec::new(), Vec::new(), Vec::new(), false, false);
        }
        if h.count < self.cnt.value {
            return (Vec::new(), Vec::new(), Vec::new(), Vec::new(), false, true);
        }
        if h.schema != self.schema || h.zero_threshold != self.z_threshold {
            return (Vec::new(), Vec::new(), Vec::new(), Vec::new(), false, false);
        }
        if is_custom_buckets_schema(h.schema)
            && !custom_bucket_bounds_match(&h.custom_values, &self.custom_values)
        {
            return (Vec::new(), Vec::new(), Vec::new(), Vec::new(), false, true);
        }
        if h.zero_count < self.z_cnt.value {
            return (Vec::new(), Vec::new(), Vec::new(), Vec::new(), false, true);
        }

        let Some((p_fwd, p_bwd)) = expand_float_spans_and_buckets(
            &self.p_spans,
            &h.positive_spans,
            &self.p_buckets,
            &h.positive_buckets,
        ) else {
            return (Vec::new(), Vec::new(), Vec::new(), Vec::new(), false, true);
        };
        let Some((n_fwd, n_bwd)) = expand_float_spans_and_buckets(
            &self.n_spans,
            &h.negative_spans,
            &self.n_buckets,
            &h.negative_buckets,
        ) else {
            return (Vec::new(), Vec::new(), Vec::new(), Vec::new(), false, true);
        };

        (p_fwd, n_fwd, p_bwd, n_bwd, true, false)
    }

    #[doc(hidden)]
    pub fn appendable_gauge(&self, h: &FloatHistogram) -> AppendableGauge {
        if self.num_samples > 0 && self.counter_reset_header != CounterResetHeader::GaugeType {
            return AppendableGauge::default();
        }
        if is_stale_nan(h.sum) {
            return AppendableGauge {
                ok_to_append: true,
                ..Default::default()
            };
        }
        if is_stale_nan(self.sum.value) {
            return AppendableGauge::default();
        }
        if h.schema != self.schema || h.zero_threshold != self.z_threshold {
            return AppendableGauge::default();
        }
        if is_custom_buckets_schema(h.schema)
            && !custom_bucket_bounds_match(&h.custom_values, &self.custom_values)
        {
            return AppendableGauge::default();
        }
        let (p_fwd, p_bwd, p_merged) =
            expand_spans_both_ways(&self.p_spans, &h.positive_spans);
        let (n_fwd, n_bwd, n_merged) =
            expand_spans_both_ways(&self.n_spans, &h.negative_spans);
        AppendableGauge {
            p_fwd,
            n_fwd,
            p_bwd,
            n_bwd,
            p_merged,
            n_merged,
            ok_to_append: true,
        }
    }
}

/// Appender for [`FloatHistogramChunk`].
pub struct FloatHistogramAppender<'a> {
    chunk: &'a mut FloatHistogramChunk,
    state: FloatHistogramAppenderState,
}

impl<'a> FloatHistogramAppender<'a> {
    #[inline]
    pub fn state(&self) -> &FloatHistogramAppenderState {
        &self.state
    }

    #[inline]
    pub fn into_state(self) -> FloatHistogramAppenderState {
        self.state
    }

    #[inline]
    pub fn num_samples(&self) -> usize {
        self.chunk.num_samples()
    }

    #[doc(hidden)]
    #[allow(clippy::type_complexity)]
    pub fn appendable(
        &self,
        h: &FloatHistogram,
    ) -> (Vec<Insert>, Vec<Insert>, Vec<Insert>, Vec<Insert>, bool, bool) {
        self.state.appendable(h)
    }

    #[doc(hidden)]
    pub fn appendable_gauge(&self, h: &FloatHistogram) -> AppendableGauge {
        self.state.appendable_gauge(h)
    }

    #[doc(hidden)]
    pub fn recode_chunk(
        &mut self,
        positive_inserts: &[Insert],
        negative_inserts: &[Insert],
        positive_spans: &[HistogramSpan],
        negative_spans: &[HistogramSpan],
    ) -> Result<(), ChunkError> {
        self.recode_in_place(positive_inserts, negative_inserts, positive_spans, negative_spans)
    }

    #[inline]
    pub fn set_counter_reset_header(&mut self, cr: CounterResetHeader) {
        self.state.counter_reset_header = cr;
        let bytes = self.chunk.b.bytes_mut();
        bytes[HISTOGRAM_FLAG_POS] =
            (bytes[HISTOGRAM_FLAG_POS] & !CounterResetHeader::MASK) | (cr as u8);
    }

    fn set_num_samples(&mut self, num: usize) {
        self.state.num_samples = num;
        let be = (num as u16).to_be_bytes();
        let bytes = self.chunk.b.bytes_mut();
        bytes[0] = be[0];
        bytes[1] = be[1];
    }

    fn write_xor_value(b: &mut BitStream, old: &mut XorValue, v: f64) {
        xor_write(b, v, old.value, &mut old.leading, &mut old.trailing);
        old.value = v;
    }

    fn append_raw(&mut self, num: usize, t: i64, h: &FloatHistogram) -> Result<usize, ChunkError> {
        Self::append_raw_to_stream(&mut self.chunk.b, &mut self.state, num, t, h)
    }

    pub(super) fn append_raw_to_stream(
        b: &mut BitStream,
        state: &mut FloatHistogramAppenderState,
        num: usize,
        t: i64,
        h: &FloatHistogram,
    ) -> Result<usize, ChunkError> {
        let mut stale_holder;
        let h = if is_stale_nan(h.sum) {
            stale_holder = FloatHistogram::default();
            stale_holder.sum = h.sum;
            &stale_holder
        } else {
            h
        };

        let mut t_delta = 0i64;
        if num == 0 {
            let num_p = checked_count_spans(&h.positive_spans).ok_or(ChunkError::InvalidChunk)?;
            let num_n = checked_count_spans(&h.negative_spans).ok_or(ChunkError::InvalidChunk)?;
            if num_p != h.positive_buckets.len() || num_n != h.negative_buckets.len() {
                return Err(ChunkError::InvalidChunk);
            }

            write_histogram_chunk_layout(
                b,
                h.schema,
                h.zero_threshold,
                &h.positive_spans,
                &h.negative_spans,
                &h.custom_values,
            );
            state.schema = h.schema;
            state.z_threshold = h.zero_threshold;
            state.p_spans = h.positive_spans.clone();
            state.n_spans = h.negative_spans.clone();
            state.custom_values = h.custom_values.clone();

            state.p_buckets = (0..num_p)
                .map(|i| XorValue {
                    value: h.positive_buckets[i],
                    leading: 0xff,
                    trailing: 0,
                })
                .collect();
            state.n_buckets = (0..num_n)
                .map(|i| XorValue {
                    value: h.negative_buckets[i],
                    leading: 0xff,
                    trailing: 0,
                })
                .collect();

            put_varbit_int(b, t);
            b.write_bits(h.count.to_bits(), 64);
            b.write_bits(h.zero_count.to_bits(), 64);
            b.write_bits(h.sum.to_bits(), 64);
            state.cnt.value = h.count;
            state.z_cnt.value = h.zero_count;
            state.sum.value = h.sum;
            for &bkt in &h.positive_buckets {
                b.write_bits(bkt.to_bits(), 64);
            }
            for &bkt in &h.negative_buckets {
                b.write_bits(bkt.to_bits(), 64);
            }
        } else {
            if !is_stale_nan(h.sum)
                && (h.positive_buckets.len() != state.p_buckets.len()
                    || h.negative_buckets.len() != state.n_buckets.len())
            {
                return Err(ChunkError::InvalidChunk);
            }
            t_delta = t.wrapping_sub(state.t);
            let t_dod = t_delta.wrapping_sub(state.t_delta);
            put_varbit_int(b, t_dod);

            Self::write_xor_value(b, &mut state.cnt, h.count);
            Self::write_xor_value(b, &mut state.z_cnt, h.zero_count);
            Self::write_xor_value(b, &mut state.sum, h.sum);

            for (i, &bkt) in h.positive_buckets.iter().enumerate() {
                Self::write_xor_value(b, &mut state.p_buckets[i], bkt);
            }
            for (i, &bkt) in h.negative_buckets.iter().enumerate() {
                Self::write_xor_value(b, &mut state.n_buckets[i], bkt);
            }
        }

        state.t = t;
        state.t_delta = t_delta;
        Ok(num + 1)
    }

    fn recode_in_place(
        &mut self,
        positive_inserts: &[Insert],
        negative_inserts: &[Insert],
        positive_spans: &[HistogramSpan],
        negative_spans: &[HistogramSpan],
    ) -> Result<(), ChunkError> {
        let crh = self.chunk.counter_reset_header();
        let num_p = checked_count_spans(positive_spans).ok_or(ChunkError::InvalidChunk)?;
        let num_n = checked_count_spans(negative_spans).ok_or(ChunkError::InvalidChunk)?;

        let mut new_chunk = FloatHistogramChunk::new();
        {
            let mut new_app = new_chunk.appender()?;
            let mut it = self.chunk.iter();
            let mut num = 0usize;
            while let Some((t_old, mut h_old)) = it.next_float_histogram()? {
                h_old.positive_spans = positive_spans.to_vec();
                h_old.negative_spans = negative_spans.to_vec();
                if !positive_inserts.is_empty() {
                    h_old.positive_buckets =
                        insert_buckets(&h_old.positive_buckets, num_p, positive_inserts, false);
                }
                if !negative_inserts.is_empty() {
                    h_old.negative_buckets =
                        insert_buckets(&h_old.negative_buckets, num_n, negative_inserts, false);
                }
                num = new_app.append_raw(num, t_old, &h_old)?;
            }
            new_app.set_num_samples(num);
            new_app.set_counter_reset_header(crh);
            self.state = new_app.state.clone();
        }
        *self.chunk = new_chunk;
        Ok(())
    }

    pub(super) fn recode_histogram(
        fh: &mut FloatHistogram,
        p_backward_inserts: &[Insert],
        n_backward_inserts: &[Insert],
    ) {
        if !p_backward_inserts.is_empty() {
            let num_p = count_spans(&fh.positive_spans);
            fh.positive_buckets =
                insert_buckets(&fh.positive_buckets, num_p, p_backward_inserts, false);
        }
        if !n_backward_inserts.is_empty() {
            let num_n = count_spans(&fh.negative_spans);
            fh.negative_buckets =
                insert_buckets(&fh.negative_buckets, num_n, n_backward_inserts, false);
        }
    }

    pub fn append_float_histogram(
        &mut self,
        prev: Option<&FloatHistogramAppenderState>,
        st: i64,
        t: i64,
        h: &FloatHistogram,
        append_only: bool,
    ) -> Result<AppendFloatHistogramOutcome, ChunkError> {
        let (out, _) = self.append_float_histogram_inner(prev, st, t, h, append_only, false)?;
        Ok(out)
    }

    fn append_float_histogram_with_state(
        mut self,
        prev: Option<&FloatHistogramAppenderState>,
        st: i64,
        t: i64,
        h: &FloatHistogram,
        append_only: bool,
    ) -> Result<(AppendFloatHistogramOutcome, FloatHistogramAppenderState), ChunkError> {
        let (out, next_state) = self.append_float_histogram_inner(prev, st, t, h, append_only, true)?;
        let st_out = next_state.unwrap_or(self.state);
        Ok((out, st_out))
    }

    fn append_float_histogram_inner(
        &mut self,
        prev: Option<&FloatHistogramAppenderState>,
        _st: i64,
        t: i64,
        h: &FloatHistogram,
        append_only: bool,
        return_new_state: bool,
    ) -> Result<(AppendFloatHistogramOutcome, Option<FloatHistogramAppenderState>), ChunkError> {
        let num_samples = self.chunk.num_samples();
        if num_samples == u16::MAX as usize {
            return Err(ChunkError::CapacityExceeded);
        }

        if num_samples == 0 {
            let n = self.append_raw(0, t, h)?;
            self.set_num_samples(n);
            if h.counter_reset_hint == CounterResetHint::GaugeType {
                self.set_counter_reset_header(CounterResetHeader::GaugeType);
                return Ok((AppendFloatHistogramOutcome::Appended { recoded: false }, None));
            }
            if h.counter_reset_hint == CounterResetHint::CounterReset {
                self.set_counter_reset_header(CounterResetHeader::CounterReset);
            } else if let Some(p) = prev {
                let (_, _, _, _, _, cr) = p.appendable(h);
                if cr {
                    self.set_counter_reset_header(CounterResetHeader::CounterReset);
                } else {
                    self.set_counter_reset_header(CounterResetHeader::NotCounterReset);
                }
            }
            return Ok((AppendFloatHistogramOutcome::Appended { recoded: false }, None));
        }

        let mut h_owned;
        let h_ref;

        if h.counter_reset_hint != CounterResetHint::GaugeType {
            let (p_fwd, n_fwd, p_bwd, n_bwd, ok_to_append, counter_reset) =
                self.state.appendable(h);
            if !ok_to_append || counter_reset {
                if append_only {
                    return if counter_reset {
                        Err(ChunkError::FloatHistogramCounterReset)
                    } else {
                        Err(ChunkError::FloatHistogramSchemaChange)
                    };
                }
                let mut new_chunk = FloatHistogramChunk::new();
                let new_state = {
                    let mut app = new_chunk.appender()?;
                    if counter_reset {
                        app.set_counter_reset_header(CounterResetHeader::CounterReset);
                    }
                    let n = app.append_raw(0, t, h)?;
                    app.set_num_samples(n);
                    return_new_state.then_some(app.state)
                };
                return Ok((AppendFloatHistogramOutcome::NewChunk(new_chunk), new_state));
            }

            if !p_bwd.is_empty() || !n_bwd.is_empty() {
                h_owned = h.clone();
                if p_fwd.is_empty() && n_fwd.is_empty() {
                    h_owned.positive_spans = self.state.p_spans.clone();
                    h_owned.negative_spans = self.state.n_spans.clone();
                } else {
                    h_owned.positive_spans =
                        adjust_for_inserts(&h_owned.positive_spans, &p_bwd);
                    h_owned.negative_spans =
                        adjust_for_inserts(&h_owned.negative_spans, &n_bwd);
                }
                Self::recode_histogram(&mut h_owned, &p_bwd, &n_bwd);
                h_ref = &h_owned;
            } else {
                h_ref = h;
            }

            if !p_fwd.is_empty() || !n_fwd.is_empty() {
                if append_only {
                    return Err(ChunkError::HistogramLayoutChange);
                }
                self.recode_in_place(&p_fwd, &n_fwd, &h_ref.positive_spans, &h_ref.negative_spans)?;
                let n = self.append_raw(self.chunk.num_samples(), t, h_ref)?;
                self.set_num_samples(n);
                return Ok((AppendFloatHistogramOutcome::Appended { recoded: true }, None));
            }

            let n = self.append_raw(num_samples, t, h_ref)?;
            self.set_num_samples(n);
            return Ok((AppendFloatHistogramOutcome::Appended { recoded: false }, None));
        }

        // Gauge float histogram
        let AppendableGauge {
            p_fwd,
            n_fwd,
            p_bwd,
            n_bwd,
            p_merged,
            n_merged,
            ok_to_append,
        } = self.state.appendable_gauge(h);
        if !ok_to_append {
            if append_only {
                return Err(ChunkError::FloatHistogramSchemaChange);
            }
            let mut new_chunk = FloatHistogramChunk::new();
            let new_state = {
                let mut app = new_chunk.appender()?;
                app.set_counter_reset_header(CounterResetHeader::GaugeType);
                let n = app.append_raw(0, t, h)?;
                app.set_num_samples(n);
                return_new_state.then_some(app.state)
            };
            return Ok((AppendFloatHistogramOutcome::NewChunk(new_chunk), new_state));
        }

        if !p_bwd.is_empty() || !n_bwd.is_empty() {
            if append_only {
                return Err(ChunkError::HistogramLayoutChange);
            }
            h_owned = h.clone();
            h_owned.positive_spans = p_merged;
            h_owned.negative_spans = n_merged;
            Self::recode_histogram(&mut h_owned, &p_bwd, &n_bwd);
            h_ref = &h_owned;
        } else {
            h_ref = h;
        }

        if !p_fwd.is_empty() || !n_fwd.is_empty() {
            if append_only {
                return Err(ChunkError::HistogramLayoutChange);
            }
            self.recode_in_place(&p_fwd, &n_fwd, &h_ref.positive_spans, &h_ref.negative_spans)?;
            let n = self.append_raw(self.chunk.num_samples(), t, h_ref)?;
            self.set_num_samples(n);
            return Ok((AppendFloatHistogramOutcome::Appended { recoded: true }, None));
        }

        let n = self.append_raw(num_samples, t, h_ref)?;
        self.set_num_samples(n);
        Ok((AppendFloatHistogramOutcome::Appended { recoded: false }, None))
    }
}

#[doc(hidden)]
pub fn expand_float_spans_and_buckets(
    a: &[HistogramSpan],
    b: &[HistogramSpan],
    a_buckets: &[XorValue],
    b_buckets: &[f64],
) -> Option<(Vec<Insert>, Vec<Insert>)> {
    if a == b && a_buckets.len() == b_buckets.len() {
        let total_len: usize = a.iter().map(|s| s.length as usize).sum();
        if total_len == a_buckets.len() {
            for (av, &bv) in a_buckets.iter().zip(b_buckets.iter()) {
                if av.value > bv {
                    return None;
                }
            }
            return Some((Vec::new(), Vec::new()));
        }
    }
    let mut ai = BucketIterator::new(a);
    let mut bi = BucketIterator::new(b);

    let mut a_inserts = Vec::new();
    let mut b_inserts = Vec::new();
    let mut a_inter = Insert::default();
    let mut b_inter = Insert::default();

    let mut a_idx = ai.next_idx();
    let mut b_idx = bi.next_idx();

    let mut a_count = 0.0f64;
    let mut b_count = 0.0f64;
    let mut a_count_idx = 0usize;
    let mut b_count_idx = 0usize;

    if a_idx.is_some() {
        a_count = a_buckets.get(a_count_idx)?.value;
    }
    if b_idx.is_some() {
        b_count = *b_buckets.get(b_count_idx)?;
    }

    let add_insert = |inserts: &mut Vec<Insert>, inter: &mut Insert, other_idx: i64| {
        if inter.num == 0 {
            inter.bucket_idx = other_idx;
        } else if inter.bucket_idx.wrapping_add(inter.num as i64) != other_idx {
            inserts.push(*inter);
            inter.num = 0;
            inter.bucket_idx = other_idx;
        }
        inter.num += 1;
    };

    loop {
        match (a_idx, b_idx) {
            (Some(av), Some(bv)) => {
                if av == bv {
                    if a_count > b_count {
                        return None;
                    }
                    if a_inter.num > 0 {
                        a_inserts.push(a_inter);
                        a_inter.num = 0;
                    }
                    a_idx = ai.next_idx();
                    a_inter.pos += 1;
                    a_count_idx += 1;
                    if a_idx.is_some() {
                        a_count = a_buckets.get(a_count_idx)?.value;
                    }

                    if b_inter.num > 0 {
                        b_inserts.push(b_inter);
                        b_inter.num = 0;
                    }
                    b_idx = bi.next_idx();
                    b_inter.pos += 1;
                    b_count_idx += 1;
                    if b_idx.is_some() {
                        b_count = *b_buckets.get(b_count_idx)?;
                    }
                } else if av < bv {
                    if a_count == 0.0 {
                        add_insert(&mut b_inserts, &mut b_inter, av);
                        if a_inter.num > 0 {
                            a_inserts.push(a_inter);
                            a_inter.num = 0;
                        }
                        a_idx = ai.next_idx();
                        a_inter.pos += 1;
                        a_count_idx += 1;
                        if a_idx.is_some() {
                            a_count = a_buckets.get(a_count_idx)?.value;
                        }
                        continue;
                    }
                    return None;
                } else {
                    add_insert(&mut a_inserts, &mut a_inter, bv);
                    if b_inter.num > 0 {
                        b_inserts.push(b_inter);
                        b_inter.num = 0;
                    }
                    b_idx = bi.next_idx();
                    b_inter.pos += 1;
                    b_count_idx += 1;
                    if b_idx.is_some() {
                        b_count = *b_buckets.get(b_count_idx)?;
                    }
                }
            }
            (Some(av), None) => {
                if a_count == 0.0 {
                    add_insert(&mut b_inserts, &mut b_inter, av);
                    if a_inter.num > 0 {
                        a_inserts.push(a_inter);
                        a_inter.num = 0;
                    }
                    a_idx = ai.next_idx();
                    a_inter.pos += 1;
                    a_count_idx += 1;
                    if a_idx.is_some() {
                        a_count = a_buckets.get(a_count_idx)?.value;
                    }
                    continue;
                }
                return None;
            }
            (None, Some(bv)) => {
                add_insert(&mut a_inserts, &mut a_inter, bv);
                if b_inter.num > 0 {
                    b_inserts.push(b_inter);
                    b_inter.num = 0;
                }
                b_idx = bi.next_idx();
                b_inter.pos += 1;
                b_count_idx += 1;
                if b_idx.is_some() {
                    b_count = *b_buckets.get(b_count_idx)?;
                }
            }
            (None, None) => {
                if a_inter.num > 0 {
                    a_inserts.push(a_inter);
                }
                if b_inter.num > 0 {
                    b_inserts.push(b_inter);
                }
                break;
            }
        }
    }

    Some((a_inserts, b_inserts))
}

/// Iterator over float histograms in a [`FloatHistogramChunk`].
#[derive(Clone, Debug)]
pub struct FloatHistogramIterator<'a> {
    pub(super) br: BitReader<'a>,
    pub(super) num_total: u16,
    pub(super) num_read: u16,
    pub(super) counter_reset_header: CounterResetHeader,
    pub(super) schema: i32,
    pub(super) z_threshold: f64,
    pub(super) p_spans: Vec<HistogramSpan>,
    pub(super) n_spans: Vec<HistogramSpan>,
    pub(super) custom_values: Vec<f64>,
    pub(super) t: i64,
    pub(super) t_delta: i64,
    pub(super) sum: XorValue,
    pub(super) cnt: XorValue,
    pub(super) z_cnt: XorValue,
    pub(super) p_buckets: Vec<f64>,
    pub(super) n_buckets: Vec<f64>,
    pub(super) p_buckets_leading: Vec<u8>,
    pub(super) n_buckets_leading: Vec<u8>,
    pub(super) p_buckets_trailing: Vec<u8>,
    pub(super) n_buckets_trailing: Vec<u8>,
}

impl<'a> FloatHistogramIterator<'a> {
    pub(super) fn new_with_header(
        payload: &'a [u8],
        num_total: u16,
        counter_reset_header: CounterResetHeader,
    ) -> Self {
        Self {
            br: BitReader::new(payload),
            num_total,
            num_read: 0,
            counter_reset_header,
            schema: 0,
            z_threshold: 0.0,
            p_spans: Vec::new(),
            n_spans: Vec::new(),
            custom_values: Vec::new(),
            t: i64::MIN,
            t_delta: 0,
            sum: XorValue::default(),
            cnt: XorValue::default(),
            z_cnt: XorValue::default(),
            p_buckets: Vec::new(),
            n_buckets: Vec::new(),
            p_buckets_leading: Vec::new(),
            n_buckets_leading: Vec::new(),
            p_buckets_trailing: Vec::new(),
            n_buckets_trailing: Vec::new(),
        }
    }

    pub fn new(b: &'a [u8]) -> Self {
        if b.len() < HISTOGRAM_HEADER_SIZE {
            return Self {
                br: BitReader::new(&[]),
                num_total: 0,
                num_read: 0,
                counter_reset_header: CounterResetHeader::UnknownCounterReset,
                schema: 0,
                z_threshold: 0.0,
                p_spans: Vec::new(),
                n_spans: Vec::new(),
                custom_values: Vec::new(),
                t: i64::MIN,
                t_delta: 0,
                sum: XorValue::default(),
                cnt: XorValue::default(),
                z_cnt: XorValue::default(),
                p_buckets: Vec::new(),
                n_buckets: Vec::new(),
                p_buckets_leading: Vec::new(),
                n_buckets_leading: Vec::new(),
                p_buckets_trailing: Vec::new(),
                n_buckets_trailing: Vec::new(),
            };
        }
        let num_total = u16::from_be_bytes([b[0], b[1]]);
        let counter_reset_header = CounterResetHeader::from_header_byte(b[HISTOGRAM_FLAG_POS]);
        Self {
            br: BitReader::new(&b[HISTOGRAM_HEADER_SIZE..]),
            num_total,
            num_read: 0,
            counter_reset_header,
            schema: 0,
            z_threshold: 0.0,
            p_spans: Vec::new(),
            n_spans: Vec::new(),
            custom_values: Vec::new(),
            t: i64::MIN,
            t_delta: 0,
            sum: XorValue::default(),
            cnt: XorValue::default(),
            z_cnt: XorValue::default(),
            p_buckets: Vec::new(),
            n_buckets: Vec::new(),
            p_buckets_leading: Vec::new(),
            n_buckets_leading: Vec::new(),
            p_buckets_trailing: Vec::new(),
            n_buckets_trailing: Vec::new(),
        }
    }

    #[inline]
    pub fn reset(&mut self, b: &'a [u8]) {
        *self = Self::new(b);
    }

    pub fn at_float_histogram(&self) -> (i64, FloatHistogram) {
        if is_stale_nan(self.sum.value) {
            return (
                self.t,
                FloatHistogram {
                    sum: stale_nan(),
                    ..Default::default()
                },
            );
        }
        let mut fh = FloatHistogram {
            counter_reset_hint: counter_reset_hint_from_header(
                self.counter_reset_header,
                self.num_read,
            ),
            schema: self.schema,
            zero_threshold: self.z_threshold,
            zero_count: self.z_cnt.value,
            count: self.cnt.value,
            sum: self.sum.value,
            positive_spans: self.p_spans.clone(),
            negative_spans: self.n_spans.clone(),
            positive_buckets: self.p_buckets.clone(),
            negative_buckets: self.n_buckets.clone(),
            custom_values: self.custom_values.clone(),
        };
        if fh.schema > EXPONENTIAL_SCHEMA_MAX && fh.schema <= EXPONENTIAL_SCHEMA_MAX_RESERVED {
            let _ = reduce_float_resolution(&mut fh, EXPONENTIAL_SCHEMA_MAX);
        }
        (self.t, fh)
    }

    pub fn next_sample(&mut self) -> Result<Option<i64>, ChunkError> {
        if self.num_read == self.num_total {
            return Ok(None);
        }

        if self.num_read == 0 {
            let layout = read_histogram_chunk_layout(&mut self.br)?;
            if !is_known_schema(layout.schema) {
                return Err(ChunkError::UnknownSchema(layout.schema));
            }
            self.schema = layout.schema;
            self.z_threshold = layout.zero_threshold;
            self.p_spans = layout.positive_spans;
            self.n_spans = layout.negative_spans;
            self.custom_values = layout.custom_values;

            let (Some(num_p), Some(num_n)) = (
                checked_count_spans(&self.p_spans),
                checked_count_spans(&self.n_spans),
            ) else {
                return Err(ChunkError::InvalidChunk);
            };
            let Some(total_buckets) = num_p.checked_add(num_n) else {
                return Err(ChunkError::InvalidChunk);
            };
            if total_buckets > self.br.remaining_bits() / 64 || total_buckets > 1_000_000 {
                return Err(ChunkError::InvalidChunk);
            }
            self.p_buckets = vec![0.0; num_p];
            self.p_buckets_leading = vec![0; num_p];
            self.p_buckets_trailing = vec![0; num_p];
            self.n_buckets = vec![0.0; num_n];
            self.n_buckets_leading = vec![0; num_n];
            self.n_buckets_trailing = vec![0; num_n];

            self.t = read_varbit_int(&mut self.br)?;
            self.cnt.value = f64::from_bits(self.br.read_bits(64)?);
            self.z_cnt.value = f64::from_bits(self.br.read_bits(64)?);
            self.sum.value = f64::from_bits(self.br.read_bits(64)?);

            for i in 0..num_p {
                self.p_buckets[i] = f64::from_bits(self.br.read_bits(64)?);
            }
            for i in 0..num_n {
                self.n_buckets[i] = f64::from_bits(self.br.read_bits(64)?);
            }
            if self.schema > EXPONENTIAL_SCHEMA_MAX
                && self.schema <= EXPONENTIAL_SCHEMA_MAX_RESERVED
            {
                super::histogram::validate_spans_for_reduce(&self.p_spans, self.p_buckets.len())?;
                super::histogram::validate_spans_for_reduce(&self.n_spans, self.n_buckets.len())?;
            }

            self.num_read += 1;
            return Ok(Some(self.t));
        }

        let t_dod = read_varbit_int(&mut self.br)?;
        self.t_delta = self.t_delta.wrapping_add(t_dod);
        self.t = self.t.wrapping_add(self.t_delta);

        xor_read(
            &mut self.br,
            &mut self.cnt.value,
            &mut self.cnt.leading,
            &mut self.cnt.trailing,
        )?;
        xor_read(
            &mut self.br,
            &mut self.z_cnt.value,
            &mut self.z_cnt.leading,
            &mut self.z_cnt.trailing,
        )?;
        xor_read(
            &mut self.br,
            &mut self.sum.value,
            &mut self.sum.leading,
            &mut self.sum.trailing,
        )?;

        if is_stale_nan(self.sum.value) {
            self.num_read += 1;
            return Ok(Some(self.t));
        }

        for i in 0..self.p_buckets.len() {
            xor_read(
                &mut self.br,
                &mut self.p_buckets[i],
                &mut self.p_buckets_leading[i],
                &mut self.p_buckets_trailing[i],
            )?;
        }
        for i in 0..self.n_buckets.len() {
            xor_read(
                &mut self.br,
                &mut self.n_buckets[i],
                &mut self.n_buckets_leading[i],
                &mut self.n_buckets_trailing[i],
            )?;
        }

        self.num_read += 1;
        Ok(Some(self.t))
    }

    pub fn next_float_histogram(&mut self) -> Result<Option<(i64, FloatHistogram)>, ChunkError> {
        if self.next_sample()?.is_none() {
            return Ok(None);
        }
        Ok(Some(self.at_float_histogram()))
    }

    pub fn seek(&mut self, t: i64) -> Result<Option<(i64, FloatHistogram)>, ChunkError> {
        while t > self.t || self.num_read == 0 {
            if self.next_sample()?.is_none() {
                return Ok(None);
            }
        }
        Ok(Some(self.at_float_histogram()))
    }
}

pub(crate) fn reduce_float_resolution(
    fh: &mut FloatHistogram,
    target_schema: i32,
) -> Result<(), ChunkError> {
    if fh.schema <= target_schema || fh.schema == super::CUSTOM_BUCKETS_SCHEMA {
        return Ok(());
    }
    let shift = (fh.schema - target_schema) as u32;
    (fh.positive_spans, fh.positive_buckets) =
        reduce_float_side(&fh.positive_spans, &fh.positive_buckets, shift)?;
    (fh.negative_spans, fh.negative_buckets) =
        reduce_float_side(&fh.negative_spans, &fh.negative_buckets, shift)?;
    fh.schema = target_schema;
    Ok(())
}

fn reduce_float_side(
    spans: &[HistogramSpan],
    buckets: &[f64],
    shift: u32,
) -> Result<(Vec<HistogramSpan>, Vec<f64>), ChunkError> {
    super::histogram::validate_spans_for_reduce(spans, buckets.len())?;
    if spans.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let mut it = BucketIterator::new(spans);
    let mut new_spans: Vec<HistogramSpan> = Vec::new();
    let mut new_counts: Vec<f64> = Vec::new();

    let mut prev_new_idx: Option<i64> = None;
    let mut acc_idx: Option<i64> = None;
    let mut acc_count = 0.0f64;

    for &val in buckets {
        let Some(orig_idx) = it.next_idx() else {
            return Err(ChunkError::InvalidChunk);
        };
        let shifted = if shift >= 64 {
            if orig_idx <= 0 { -1 } else { 0 }
        } else {
            (orig_idx - 1) >> shift
        };
        let target_idx = shifted + 1;
        match acc_idx {
            Some(idx) if idx == target_idx => {
                acc_count += val;
            }
            Some(idx) => {
                flush_float_bucket(idx, acc_count, &mut prev_new_idx, &mut new_spans, &mut new_counts);
                acc_idx = Some(target_idx);
                acc_count = val;
            }
            None => {
                acc_idx = Some(target_idx);
                acc_count = val;
            }
        }
    }
    if let Some(idx) = acc_idx {
        flush_float_bucket(idx, acc_count, &mut prev_new_idx, &mut new_spans, &mut new_counts);
    }
    Ok((new_spans, new_counts))
}

fn flush_float_bucket(
    idx: i64,
    val: f64,
    prev_idx: &mut Option<i64>,
    spans: &mut Vec<HistogramSpan>,
    counts: &mut Vec<f64>,
) {
    match *prev_idx {
        None => {
            spans.push(HistogramSpan {
                offset: idx as i32,
                length: 1,
            });
        }
        Some(p) => {
            let gap = idx.wrapping_sub(p).wrapping_sub(1);
            if gap == 0 {
                spans.last_mut().unwrap().length += 1;
            } else {
                spans.push(HistogramSpan {
                    offset: gap as i32,
                    length: 1,
                });
            }
        }
    }
    counts.push(val);
    *prev_idx = Some(idx);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunkenc::varbit::{put_varbit_int, put_varbit_uint};

    #[test]
    fn corrupt_float_histogram_u32_max_span_returns_error_not_oom() {
        let mut bs = BitStream::new();
        bs.write_byte(0);
        bs.write_byte(1);
        bs.write_byte(0); // num_samples=1, flags=0
        bs.write_byte(0); // zero_threshold = 0
        put_varbit_int(&mut bs, 0); // schema = 0
        put_varbit_uint(&mut bs, 1); // 1 positive span
        put_varbit_uint(&mut bs, u64::from(u32::MAX)); // length = u32::MAX
        put_varbit_int(&mut bs, 0); // offset = 0
        put_varbit_uint(&mut bs, 0); // 0 negative spans
        put_varbit_int(&mut bs, 100); // t
        bs.write_bits(1.0f64.to_bits(), 64); // cnt
        bs.write_bits(0.0f64.to_bits(), 64); // z_cnt
        bs.write_bits(1.0f64.to_bits(), 64); // sum

        let chunk = FloatHistogramChunk::from_bytes(bs.into_bytes());
        let mut it = chunk.iter();
        let err = it.next_float_histogram().unwrap_err();
        assert!(
            matches!(err, ChunkError::InvalidChunk | ChunkError::UnexpectedEof),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn float_histogram_appender_rejects_short_header_padded_bytes_and_mismatched_raw_c1_c3_c6() {
        let mut short = FloatHistogramChunk::from_bytes(vec![0u8; HISTOGRAM_HEADER_SIZE - 1]);
        assert!(matches!(
            short.appender(),
            Err(ChunkError::InvalidHeader { len: 2, min: 3 })
        ));

        let fh1 = FloatHistogram {
            schema: 0,
            count: 2.0,
            sum: 3.0,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![2.0],
            ..Default::default()
        };
        let fh2 = FloatHistogram {
            schema: 0,
            count: 5.0,
            sum: 7.0,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![5.0],
            ..Default::default()
        };

        let mut chunk = FloatHistogramChunk::new();
        {
            let mut app = chunk.appender().unwrap();
            app.append_float_histogram(None, 0, 1_000, &fh1, false).unwrap();
        }
        let mut bytes = chunk.as_bytes().to_vec();
        bytes.extend_from_slice(&[0xff, 0xaa, 0x55]);
        let mut padded = FloatHistogramChunk::from_bytes(bytes);
        {
            let mut app = padded.appender().unwrap();
            app.append_float_histogram(None, 0, 2_000, &fh2, false).unwrap();
        }
        let mut it = padded.iter();
        assert_eq!(it.next_float_histogram().unwrap().unwrap().1.count, 2.0);
        assert_eq!(it.next_float_histogram().unwrap().unwrap().1.count, 5.0);
        assert!(it.next_float_histogram().unwrap().is_none());

        // C-1: Direct append_raw_to_stream with u32::MAX span or mismatched buckets returns Err without panic/OOM.
        let mut bs = BitStream::with_header(HISTOGRAM_HEADER_SIZE, 32);
        let mut st = FloatHistogramAppenderState::empty();
        let corrupt_huge = FloatHistogram {
            positive_spans: vec![HistogramSpan { offset: 0, length: u32::MAX }],
            positive_buckets: vec![],
            ..Default::default()
        };
        assert!(matches!(
            FloatHistogramAppender::append_raw_to_stream(&mut bs, &mut st, 0, 100, &corrupt_huge),
            Err(ChunkError::InvalidChunk)
        ));
        let corrupt_mismatch = FloatHistogram {
            positive_spans: vec![HistogramSpan { offset: 0, length: 5 }],
            positive_buckets: vec![1.0, 2.0],
            ..Default::default()
        };
        assert!(matches!(
            FloatHistogramAppender::append_raw_to_stream(&mut bs, &mut st, 0, 100, &corrupt_mismatch),
            Err(ChunkError::InvalidChunk)
        ));

        // GateA-2: 0-sample chunk with trailing padding bytes truncates to header before sample 0.
        let mut zero_padded = FloatHistogramChunk::from_bytes(vec![0, 0, 0, 0xff, 0xaa]);
        {
            let _ = zero_padded.appender().unwrap();
            assert_eq!(zero_padded.as_bytes(), &[0, 0, 0]);
            let mut app = zero_padded.appender().unwrap();
            app.append_float_histogram(None, 0, 1_000, &fh1, false)
                .unwrap();
        }
        let mut it = zero_padded.iter();
        assert_eq!(it.next_float_histogram().unwrap().unwrap().1.count, 2.0);
        assert!(it.next_float_histogram().unwrap().is_none());
    }
}
