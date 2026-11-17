//! Prometheus `EncHistogram` integer native histogram chunk codec.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/histogram.go.

use super::bstream::{BitReader, BitStream};
use super::histogram_meta::{
    adjust_for_inserts, checked_count_spans, count_spans, counter_reset_hint_from_header,
    insert_buckets, read_histogram_chunk_layout, write_histogram_chunk_layout, BucketIterator,
    Insert,
};
use super::histogram_model::{
    custom_bucket_bounds_match, is_custom_buckets_schema, is_known_schema, CounterResetHeader,
    CounterResetHint, FloatHistogram, Histogram, HistogramSpan, EXPONENTIAL_SCHEMA_MAX,
    EXPONENTIAL_SCHEMA_MAX_RESERVED,
};
use super::varbit::{put_varbit_int, put_varbit_uint, read_varbit_int, read_varbit_uint};
use super::xor::{xor_read, xor_write};
use super::{is_stale_nan, stale_nan, ChunkError, Encoding};

const HISTOGRAM_FLAG_POS: usize = 2;
const HISTOGRAM_HEADER_SIZE: usize = 3;
const CHUNK_ALLOCATION_SIZE: usize = 128;
const CHUNK_COMPACT_CAPACITY_THRESHOLD: usize = 32;

/// Outcome of appending a histogram sample to a [`HistogramChunk`].
#[derive(Debug)]
pub enum AppendHistogramOutcome {
    /// Sample appended to the current chunk (which may have been recoded in-place).
    Appended { recoded: bool },
    /// Sample required starting a new chunk (due to counter reset or schema/threshold change).
    /// The existing chunk was not modified with the new sample.
    NewChunk(HistogramChunk),
}

/// Prometheus `EncHistogram` (`2`) encoded integer native histogram chunk.
#[derive(Clone, Debug)]
pub struct HistogramChunk {
    b: BitStream,
}

impl PartialEq for HistogramChunk {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for HistogramChunk {}

impl Default for HistogramChunk {
    fn default() -> Self {
        Self::new()
    }
}

impl HistogramChunk {
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
    pub const fn encoding(&self) -> Encoding {
        Encoding::Histogram
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
    pub fn iter(&self) -> HistogramIterator<'_> {
        HistogramIterator::new(self.b.bytes())
    }

    pub fn appender(&mut self) -> Result<HistogramAppender<'_>, ChunkError> {
        if self.b.bytes().len() == HISTOGRAM_HEADER_SIZE {
            return Ok(HistogramAppender {
                chunk: self,
                state: HistogramAppenderState::empty(),
            });
        }

        let (state, valid_bits) = {
            let mut it = self.iter();
            while it.next_sample()?.is_some() {}
            (
                HistogramAppenderState {
                    num_samples: self.num_samples(),
                    counter_reset_header: self.counter_reset_header(),
                    schema: it.schema,
                    z_threshold: it.z_threshold,
                    p_spans: it.p_spans,
                    n_spans: it.n_spans,
                    custom_values: it.custom_values,
                    t: it.t,
                    cnt: it.cnt,
                    z_cnt: it.z_cnt,
                    t_delta: it.t_delta,
                    cnt_delta: it.cnt_delta,
                    z_cnt_delta: it.z_cnt_delta,
                    p_buckets: it.p_buckets,
                    n_buckets: it.n_buckets,
                    p_buckets_delta: it.p_buckets_delta,
                    n_buckets_delta: it.n_buckets_delta,
                    sum: it.sum,
                    leading: it.leading,
                    trailing: it.trailing,
                },
                it.br.valid_bits(),
            )
        };

        self.b.count = valid_bits;
        Ok(HistogramAppender { chunk: self, state })
    }

    /// Appends `(st, t, h)` in O(1) using a cached [`HistogramAppenderState`],
    /// recovering state once via [`Self::appender`] only if `*state` is `None`.
    pub fn append_with_state(
        &mut self,
        state: &mut Option<HistogramAppenderState>,
        st: i64,
        t: i64,
        h: &Histogram,
    ) -> Result<AppendHistogramOutcome, ChunkError> {
        self.append_with_state_prev(state, None, st, t, h)
    }

    /// Appends `(st, t, h)` in O(1) using a cached [`HistogramAppenderState`] and
    /// optional previous-chunk state for computing `CounterResetHeader` on chunk rollover.
    pub fn append_with_state_prev(
        &mut self,
        state: &mut Option<HistogramAppenderState>,
        prev: Option<&HistogramAppenderState>,
        st: i64,
        t: i64,
        h: &Histogram,
    ) -> Result<AppendHistogramOutcome, ChunkError> {
        let mut app = match state.take() {
            Some(s) => HistogramAppender { chunk: self, state: s },
            None => self.appender()?,
        };
        let out = app.append_histogram(prev, st, t, h, false)?;
        if matches!(out, AppendHistogramOutcome::Appended { .. }) {
            *state = Some(app.state);
        }
        Ok(out)
    }
}

/// Snapshot of [`HistogramAppender`] state used to detect counter resets across chunks.
#[derive(Clone, Debug)]
pub struct HistogramAppenderState {
    pub(super) num_samples: usize,
    pub(super) counter_reset_header: CounterResetHeader,
    pub(super) schema: i32,
    pub(super) z_threshold: f64,
    pub(super) p_spans: Vec<HistogramSpan>,
    pub(super) n_spans: Vec<HistogramSpan>,
    pub(super) custom_values: Vec<f64>,
    pub(super) t: i64,
    pub(super) cnt: u64,
    pub(super) z_cnt: u64,
    pub(super) t_delta: i64,
    pub(super) cnt_delta: i64,
    pub(super) z_cnt_delta: i64,
    pub(super) p_buckets: Vec<i64>,
    pub(super) n_buckets: Vec<i64>,
    pub(super) p_buckets_delta: Vec<i64>,
    pub(super) n_buckets_delta: Vec<i64>,
    pub(super) sum: f64,
    pub(super) leading: u8,
    pub(super) trailing: u8,
}

#[derive(Default)]
pub(super) struct AppendableGauge {
    pub(super) p_fwd: Vec<Insert>,
    pub(super) n_fwd: Vec<Insert>,
    pub(super) p_bwd: Vec<Insert>,
    pub(super) n_bwd: Vec<Insert>,
    pub(super) p_merged: Vec<HistogramSpan>,
    pub(super) n_merged: Vec<HistogramSpan>,
    pub(super) ok_to_append: bool,
}

impl HistogramAppenderState {
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
            cnt: 0,
            z_cnt: 0,
            t_delta: 0,
            cnt_delta: 0,
            z_cnt_delta: 0,
            p_buckets: Vec::new(),
            n_buckets: Vec::new(),
            p_buckets_delta: Vec::new(),
            n_buckets_delta: Vec::new(),
            sum: 0.0,
            leading: 0xff,
            trailing: 0,
        }
    }

    pub(crate) fn appendable(
        &self,
        h: &Histogram,
    ) -> (
        Vec<Insert>,
        Vec<Insert>,
        Vec<Insert>,
        Vec<Insert>,
        bool,
        CounterResetHeader,
    ) {
        if self.num_samples > 0 && self.counter_reset_header == CounterResetHeader::GaugeType {
            return (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                false,
                CounterResetHeader::NotCounterReset,
            );
        }
        if h.counter_reset_hint == CounterResetHint::CounterReset {
            return (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                false,
                CounterResetHeader::CounterReset,
            );
        }
        if is_stale_nan(h.sum) {
            return (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                true,
                CounterResetHeader::NotCounterReset,
            );
        }
        if is_stale_nan(self.sum) {
            return (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                false,
                CounterResetHeader::UnknownCounterReset,
            );
        }
        if h.count < self.cnt {
            return (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                false,
                CounterResetHeader::CounterReset,
            );
        }
        if h.schema != self.schema || h.zero_threshold != self.z_threshold {
            return (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                false,
                CounterResetHeader::UnknownCounterReset,
            );
        }
        if is_custom_buckets_schema(h.schema)
            && !custom_bucket_bounds_match(&h.custom_values, &self.custom_values)
        {
            return (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                false,
                CounterResetHeader::CounterReset,
            );
        }
        if h.zero_count < self.z_cnt {
            return (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                false,
                CounterResetHeader::CounterReset,
            );
        }

        let Some((p_fwd, p_bwd)) = expand_int_spans_and_buckets(
            &self.p_spans,
            &h.positive_spans,
            &self.p_buckets,
            &h.positive_buckets,
        ) else {
            return (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                false,
                CounterResetHeader::CounterReset,
            );
        };
        let Some((n_fwd, n_bwd)) = expand_int_spans_and_buckets(
            &self.n_spans,
            &h.negative_spans,
            &self.n_buckets,
            &h.negative_buckets,
        ) else {
            return (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                false,
                CounterResetHeader::CounterReset,
            );
        };

        (
            p_fwd,
            n_fwd,
            p_bwd,
            n_bwd,
            true,
            CounterResetHeader::NotCounterReset,
        )
    }

    pub(super) fn appendable_gauge(&self, h: &Histogram) -> AppendableGauge {
        if self.num_samples > 0 && self.counter_reset_header != CounterResetHeader::GaugeType {
            return AppendableGauge::default();
        }
        if is_stale_nan(h.sum) {
            return AppendableGauge {
                ok_to_append: true,
                ..Default::default()
            };
        }
        if is_stale_nan(self.sum) {
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
            super::histogram_meta::expand_spans_both_ways(&self.p_spans, &h.positive_spans);
        let (n_fwd, n_bwd, n_merged) =
            super::histogram_meta::expand_spans_both_ways(&self.n_spans, &h.negative_spans);
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

/// Appender for [`HistogramChunk`].
pub struct HistogramAppender<'a> {
    chunk: &'a mut HistogramChunk,
    state: HistogramAppenderState,
}

impl<'a> HistogramAppender<'a> {
    #[inline]
    pub fn state(&self) -> &HistogramAppenderState {
        &self.state
    }

    #[inline]
    pub fn into_state(self) -> HistogramAppenderState {
        self.state
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

    fn append_raw(&mut self, num: usize, t: i64, h: &Histogram) -> usize {
        Self::append_raw_to_stream(&mut self.chunk.b, &mut self.state, num, t, h)
    }

    pub(super) fn append_raw_to_stream(
        b: &mut BitStream,
        state: &mut HistogramAppenderState,
        num: usize,
        t: i64,
        h: &Histogram,
    ) -> usize {
        let mut stale_holder;
        let h = if is_stale_nan(h.sum) {
            stale_holder = Histogram::default();
            stale_holder.sum = h.sum;
            &stale_holder
        } else {
            h
        };

        let mut t_delta = 0i64;
        let mut cnt_delta = 0i64;
        let mut z_cnt_delta = 0i64;

        if num == 0 {
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

            let num_p = count_spans(&h.positive_spans);
            let num_n = count_spans(&h.negative_spans);
            state.p_buckets = vec![0; num_p];
            state.p_buckets_delta = vec![0; num_p];
            state.n_buckets = vec![0; num_n];
            state.n_buckets_delta = vec![0; num_n];

            put_varbit_int(b, t);
            put_varbit_uint(b, h.count);
            put_varbit_uint(b, h.zero_count);
            b.write_bits(h.sum.to_bits(), 64);
            for &bkt in &h.positive_buckets {
                put_varbit_int(b, bkt);
            }
            for &bkt in &h.negative_buckets {
                put_varbit_int(b, bkt);
            }
        } else {
            t_delta = t.wrapping_sub(state.t);
            cnt_delta = (h.count as i64).wrapping_sub(state.cnt as i64);
            z_cnt_delta = (h.zero_count as i64).wrapping_sub(state.z_cnt as i64);

            let t_dod = t_delta.wrapping_sub(state.t_delta);
            let mut cnt_dod = cnt_delta.wrapping_sub(state.cnt_delta);
            let mut z_cnt_dod = z_cnt_delta.wrapping_sub(state.z_cnt_delta);

            if is_stale_nan(h.sum) {
                cnt_dod = 0;
                z_cnt_dod = 0;
            }

            put_varbit_int(b, t_dod);
            put_varbit_int(b, cnt_dod);
            put_varbit_int(b, z_cnt_dod);

            xor_write(
                b,
                h.sum,
                state.sum,
                &mut state.leading,
                &mut state.trailing,
            );

            for (i, &bkt) in h.positive_buckets.iter().enumerate() {
                let delta = bkt.wrapping_sub(state.p_buckets[i]);
                let dod = delta.wrapping_sub(state.p_buckets_delta[i]);
                put_varbit_int(b, dod);
                state.p_buckets_delta[i] = delta;
            }
            for (i, &bkt) in h.negative_buckets.iter().enumerate() {
                let delta = bkt.wrapping_sub(state.n_buckets[i]);
                let dod = delta.wrapping_sub(state.n_buckets_delta[i]);
                put_varbit_int(b, dod);
                state.n_buckets_delta[i] = delta;
            }
        }

        state.t = t;
        state.cnt = h.count;
        state.z_cnt = h.zero_count;
        state.t_delta = t_delta;
        state.cnt_delta = cnt_delta;
        state.z_cnt_delta = z_cnt_delta;
        if !is_stale_nan(h.sum) {
            state.p_buckets.copy_from_slice(&h.positive_buckets);
            state.n_buckets.copy_from_slice(&h.negative_buckets);
        }
        state.sum = h.sum;

        num + 1
    }

    fn recode_in_place(
        &mut self,
        positive_inserts: &[Insert],
        negative_inserts: &[Insert],
        positive_spans: &[HistogramSpan],
        negative_spans: &[HistogramSpan],
    ) -> Result<(), ChunkError> {
        let crh = self.chunk.counter_reset_header();
        let num_p = count_spans(positive_spans);
        let num_n = count_spans(negative_spans);

        let mut new_chunk = HistogramChunk::new();
        {
            let mut new_app = new_chunk.appender()?;
            let mut it = self.chunk.iter();
            let mut num = 0usize;
            while let Some((t_old, mut h_old)) = it.next_histogram()? {
                h_old.positive_spans = positive_spans.to_vec();
                h_old.negative_spans = negative_spans.to_vec();
                if !positive_inserts.is_empty() {
                    h_old.positive_buckets =
                        insert_buckets(&h_old.positive_buckets, num_p, positive_inserts, true);
                }
                if !negative_inserts.is_empty() {
                    h_old.negative_buckets =
                        insert_buckets(&h_old.negative_buckets, num_n, negative_inserts, true);
                }
                num = new_app.append_raw(num, t_old, &h_old);
            }
            new_app.set_num_samples(num);
            new_app.set_counter_reset_header(crh);
            self.state = new_app.state.clone();
        }
        *self.chunk = new_chunk;
        Ok(())
    }

    pub(super) fn recode_histogram(
        h: &mut Histogram,
        p_backward_inserts: &[Insert],
        n_backward_inserts: &[Insert],
    ) {
        if !p_backward_inserts.is_empty() {
            let num_p = count_spans(&h.positive_spans);
            h.positive_buckets =
                insert_buckets(&h.positive_buckets, num_p, p_backward_inserts, true);
        }
        if !n_backward_inserts.is_empty() {
            let num_n = count_spans(&h.negative_spans);
            h.negative_buckets =
                insert_buckets(&h.negative_buckets, num_n, n_backward_inserts, true);
        }
    }

    /// Appends a [`Histogram`] sample at timestamp `t`, recoding or cutting a new chunk if required.
    pub fn append_histogram(
        &mut self,
        prev: Option<&HistogramAppenderState>,
        _st: i64,
        t: i64,
        h: &Histogram,
        append_only: bool,
    ) -> Result<AppendHistogramOutcome, ChunkError> {
        let num_samples = self.chunk.num_samples();
        if num_samples == u16::MAX as usize {
            return Err(ChunkError::CapacityExceeded);
        }

        if num_samples == 0 {
            let new_num = self.append_raw(0, t, h);
            self.set_num_samples(new_num);
            if h.counter_reset_hint == CounterResetHint::GaugeType {
                self.set_counter_reset_header(CounterResetHeader::GaugeType);
                return Ok(AppendHistogramOutcome::Appended { recoded: false });
            }
            if h.counter_reset_hint == CounterResetHint::CounterReset {
                self.set_counter_reset_header(CounterResetHeader::CounterReset);
            } else if let Some(p) = prev {
                let (_, _, _, _, _, cr) = p.appendable(h);
                self.set_counter_reset_header(cr);
            }
            return Ok(AppendHistogramOutcome::Appended { recoded: false });
        }

        let mut h_owned;
        let h_ref;

        if h.counter_reset_hint != CounterResetHint::GaugeType {
            let (p_fwd, n_fwd, p_bwd, n_bwd, ok_to_append, cr_hint) = self.state.appendable(h);
            if !ok_to_append || cr_hint != CounterResetHeader::NotCounterReset {
                if append_only {
                    return if cr_hint == CounterResetHeader::CounterReset {
                        Err(ChunkError::HistogramCounterReset)
                    } else {
                        Err(ChunkError::HistogramSchemaChange)
                    };
                }
                let mut new_chunk = HistogramChunk::new();
                {
                    let mut app = new_chunk.appender()?;
                    app.set_counter_reset_header(cr_hint);
                    let n = app.append_raw(0, t, h);
                    app.set_num_samples(n);
                }
                return Ok(AppendHistogramOutcome::NewChunk(new_chunk));
            }

            if !p_bwd.is_empty() || !n_bwd.is_empty() {
                h_owned = h.clone();
                if p_fwd.is_empty() && n_fwd.is_empty() {
                    h_owned.positive_spans = self.state.p_spans.clone();
                    h_owned.negative_spans = self.state.n_spans.clone();
                } else {
                    h_owned.positive_spans = adjust_for_inserts(&h_owned.positive_spans, &p_bwd);
                    h_owned.negative_spans = adjust_for_inserts(&h_owned.negative_spans, &n_bwd);
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
                let n = self.append_raw(self.chunk.num_samples(), t, h_ref);
                self.set_num_samples(n);
                return Ok(AppendHistogramOutcome::Appended { recoded: true });
            }

            let n = self.append_raw(num_samples, t, h_ref);
            self.set_num_samples(n);
            return Ok(AppendHistogramOutcome::Appended { recoded: false });
        }

        // Gauge histogram
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
                return Err(ChunkError::HistogramSchemaChange);
            }
            let mut new_chunk = HistogramChunk::new();
            {
                let mut app = new_chunk.appender()?;
                app.set_counter_reset_header(CounterResetHeader::GaugeType);
                let n = app.append_raw(0, t, h);
                app.set_num_samples(n);
            }
            return Ok(AppendHistogramOutcome::NewChunk(new_chunk));
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
            let n = self.append_raw(self.chunk.num_samples(), t, h_ref);
            self.set_num_samples(n);
            return Ok(AppendHistogramOutcome::Appended { recoded: true });
        }

        let n = self.append_raw(num_samples, t, h_ref);
        self.set_num_samples(n);
        Ok(AppendHistogramOutcome::Appended { recoded: false })
    }
}

fn expand_int_spans_and_buckets(
    a: &[HistogramSpan],
    b: &[HistogramSpan],
    a_buckets: &[i64],
    b_buckets: &[i64],
) -> Option<(Vec<Insert>, Vec<Insert>)> {
    let mut ai = BucketIterator::new(a);
    let mut bi = BucketIterator::new(b);

    let mut a_inserts = Vec::new();
    let mut b_inserts = Vec::new();
    let mut a_inter = Insert::default();
    let mut b_inter = Insert::default();

    let mut a_idx = ai.next_idx();
    let mut b_idx = bi.next_idx();

    let mut a_count = 0i64;
    let mut b_count = 0i64;
    let mut a_count_idx = 0usize;
    let mut b_count_idx = 0usize;

    if a_idx.is_some() {
        a_count = *a_buckets.get(a_count_idx)?;
    }
    if b_idx.is_some() {
        b_count = *b_buckets.get(b_count_idx)?;
    }

    let add_insert = |inserts: &mut Vec<Insert>, inter: &mut Insert, other_idx: i32| {
        if inter.num == 0 {
            inter.bucket_idx = other_idx;
        } else if inter.bucket_idx + (inter.num as i32) != other_idx {
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
                        a_count += *a_buckets.get(a_count_idx)?;
                    }

                    if b_inter.num > 0 {
                        b_inserts.push(b_inter);
                        b_inter.num = 0;
                    }
                    b_idx = bi.next_idx();
                    b_inter.pos += 1;
                    b_count_idx += 1;
                    if b_idx.is_some() {
                        b_count += *b_buckets.get(b_count_idx)?;
                    }
                } else if av < bv {
                    if a_count == 0 {
                        add_insert(&mut b_inserts, &mut b_inter, av);
                        if a_inter.num > 0 {
                            a_inserts.push(a_inter);
                            a_inter.num = 0;
                        }
                        a_idx = ai.next_idx();
                        a_inter.pos += 1;
                        a_count_idx += 1;
                        if a_idx.is_some() {
                            a_count += *a_buckets.get(a_count_idx)?;
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
                        b_count += *b_buckets.get(b_count_idx)?;
                    }
                }
            }
            (Some(av), None) => {
                if a_count == 0 {
                    add_insert(&mut b_inserts, &mut b_inter, av);
                    if a_inter.num > 0 {
                        a_inserts.push(a_inter);
                        a_inter.num = 0;
                    }
                    a_idx = ai.next_idx();
                    a_inter.pos += 1;
                    a_count_idx += 1;
                    if a_idx.is_some() {
                        a_count += *a_buckets.get(a_count_idx)?;
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
                    b_count += *b_buckets.get(b_count_idx)?;
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

/// Iterator over integer native histograms in a [`HistogramChunk`].
#[derive(Clone, Debug)]
pub struct HistogramIterator<'a> {
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
    pub(super) cnt: u64,
    pub(super) z_cnt: u64,
    pub(super) t_delta: i64,
    pub(super) cnt_delta: i64,
    pub(super) z_cnt_delta: i64,
    pub(super) p_buckets: Vec<i64>,
    pub(super) n_buckets: Vec<i64>,
    pub(super) p_buckets_delta: Vec<i64>,
    pub(super) n_buckets_delta: Vec<i64>,
    pub(super) sum: f64,
    pub(super) leading: u8,
    pub(super) trailing: u8,
}

impl<'a> HistogramIterator<'a> {
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
            cnt: 0,
            z_cnt: 0,
            t_delta: 0,
            cnt_delta: 0,
            z_cnt_delta: 0,
            p_buckets: Vec::new(),
            n_buckets: Vec::new(),
            p_buckets_delta: Vec::new(),
            n_buckets_delta: Vec::new(),
            sum: 0.0,
            leading: 0,
            trailing: 0,
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
                cnt: 0,
                z_cnt: 0,
                t_delta: 0,
                cnt_delta: 0,
                z_cnt_delta: 0,
                p_buckets: Vec::new(),
                n_buckets: Vec::new(),
                p_buckets_delta: Vec::new(),
                n_buckets_delta: Vec::new(),
                sum: 0.0,
                leading: 0,
                trailing: 0,
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
            cnt: 0,
            z_cnt: 0,
            t_delta: 0,
            cnt_delta: 0,
            z_cnt_delta: 0,
            p_buckets: Vec::new(),
            n_buckets: Vec::new(),
            p_buckets_delta: Vec::new(),
            n_buckets_delta: Vec::new(),
            sum: 0.0,
            leading: 0,
            trailing: 0,
        }
    }

    pub fn at_histogram(&self) -> (i64, Histogram) {
        if is_stale_nan(self.sum) {
            return (
                self.t,
                Histogram {
                    sum: stale_nan(),
                    ..Default::default()
                },
            );
        }
        let mut h = Histogram {
            counter_reset_hint: counter_reset_hint_from_header(
                self.counter_reset_header,
                self.num_read,
            ),
            schema: self.schema,
            zero_threshold: self.z_threshold,
            zero_count: self.z_cnt,
            count: self.cnt,
            sum: self.sum,
            positive_spans: self.p_spans.clone(),
            negative_spans: self.n_spans.clone(),
            positive_buckets: self.p_buckets.clone(),
            negative_buckets: self.n_buckets.clone(),
            custom_values: self.custom_values.clone(),
        };
        if h.schema > EXPONENTIAL_SCHEMA_MAX && h.schema <= EXPONENTIAL_SCHEMA_MAX_RESERVED {
            reduce_int_resolution(&mut h, EXPONENTIAL_SCHEMA_MAX);
        }
        (self.t, h)
    }

    pub fn at_float_histogram(&self) -> (i64, FloatHistogram) {
        let (t, h) = self.at_histogram();
        if is_stale_nan(h.sum) {
            return (
                t,
                FloatHistogram {
                    sum: stale_nan(),
                    ..Default::default()
                },
            );
        }
        (t, h.to_float())
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
            if total_buckets > self.br.remaining_bits() || total_buckets > 1_000_000 {
                return Err(ChunkError::InvalidChunk);
            }
            self.p_buckets = vec![0; num_p];
            self.p_buckets_delta = vec![0; num_p];
            self.n_buckets = vec![0; num_n];
            self.n_buckets_delta = vec![0; num_n];

            self.t = read_varbit_int(&mut self.br)?;
            self.cnt = read_varbit_uint(&mut self.br)?;
            self.z_cnt = read_varbit_uint(&mut self.br)?;
            self.sum = f64::from_bits(self.br.read_bits(64)?);

            for i in 0..num_p {
                self.p_buckets[i] = read_varbit_int(&mut self.br)?;
            }
            for i in 0..num_n {
                self.n_buckets[i] = read_varbit_int(&mut self.br)?;
            }

            self.num_read += 1;
            return Ok(Some(self.t));
        }

        let t_dod = read_varbit_int(&mut self.br)?;
        self.t_delta = self.t_delta.wrapping_add(t_dod);
        self.t = self.t.wrapping_add(self.t_delta);

        let cnt_dod = read_varbit_int(&mut self.br)?;
        self.cnt_delta = self.cnt_delta.wrapping_add(cnt_dod);
        self.cnt = (self.cnt as i64).wrapping_add(self.cnt_delta) as u64;

        let z_cnt_dod = read_varbit_int(&mut self.br)?;
        self.z_cnt_delta = self.z_cnt_delta.wrapping_add(z_cnt_dod);
        self.z_cnt = (self.z_cnt as i64).wrapping_add(self.z_cnt_delta) as u64;

        xor_read(
            &mut self.br,
            &mut self.sum,
            &mut self.leading,
            &mut self.trailing,
        )?;

        if is_stale_nan(self.sum) {
            self.num_read += 1;
            return Ok(Some(self.t));
        }

        for i in 0..self.p_buckets.len() {
            let dod = read_varbit_int(&mut self.br)?;
            self.p_buckets_delta[i] = self.p_buckets_delta[i].wrapping_add(dod);
            self.p_buckets[i] = self.p_buckets[i].wrapping_add(self.p_buckets_delta[i]);
        }
        for i in 0..self.n_buckets.len() {
            let dod = read_varbit_int(&mut self.br)?;
            self.n_buckets_delta[i] = self.n_buckets_delta[i].wrapping_add(dod);
            self.n_buckets[i] = self.n_buckets[i].wrapping_add(self.n_buckets_delta[i]);
        }

        self.num_read += 1;
        Ok(Some(self.t))
    }

    pub fn next_histogram(&mut self) -> Result<Option<(i64, Histogram)>, ChunkError> {
        if self.next_sample()?.is_none() {
            return Ok(None);
        }
        Ok(Some(self.at_histogram()))
    }

    pub fn seek(&mut self, t: i64) -> Result<Option<(i64, Histogram)>, ChunkError> {
        while t > self.t || self.num_read == 0 {
            if self.next_sample()?.is_none() {
                return Ok(None);
            }
        }
        Ok(Some(self.at_histogram()))
    }
}

pub(crate) fn reduce_int_resolution(h: &mut Histogram, target_schema: i32) {
    if h.schema <= target_schema || h.schema == super::CUSTOM_BUCKETS_SCHEMA {
        return;
    }
    let shift = (h.schema - target_schema) as u32;
    (h.positive_spans, h.positive_buckets) =
        reduce_int_side(&h.positive_spans, &h.positive_buckets, shift);
    (h.negative_spans, h.negative_buckets) =
        reduce_int_side(&h.negative_spans, &h.negative_buckets, shift);
    h.schema = target_schema;
}

fn reduce_int_side(
    spans: &[HistogramSpan],
    buckets: &[i64],
    shift: u32,
) -> (Vec<HistogramSpan>, Vec<i64>) {
    if spans.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let mut it = BucketIterator::new(spans);
    let mut new_spans: Vec<HistogramSpan> = Vec::new();
    let mut new_deltas: Vec<i64> = Vec::new();

    let mut cur_abs = 0i64;
    let mut prev_new_idx: Option<i32> = None;
    let mut prev_new_abs = 0i64;

    let mut acc_idx: Option<i32> = None;
    let mut acc_count = 0i64;

    for &d in buckets {
        let Some(orig_idx) = it.next_idx() else {
            break;
        };
        cur_abs += d;
        // Arithmetic right shift toward -inf: floor(orig_idx / 2^shift)
        let target_idx = orig_idx.div_euclid(1 << shift);
        match acc_idx {
            Some(idx) if idx == target_idx => {
                acc_count += cur_abs;
            }
            Some(idx) => {
                flush_int_bucket(
                    idx,
                    acc_count,
                    &mut prev_new_idx,
                    &mut prev_new_abs,
                    &mut new_spans,
                    &mut new_deltas,
                );
                acc_idx = Some(target_idx);
                acc_count = cur_abs;
            }
            None => {
                acc_idx = Some(target_idx);
                acc_count = cur_abs;
            }
        }
    }
    if let Some(idx) = acc_idx {
        flush_int_bucket(
            idx,
            acc_count,
            &mut prev_new_idx,
            &mut prev_new_abs,
            &mut new_spans,
            &mut new_deltas,
        );
    }
    (new_spans, new_deltas)
}

fn flush_int_bucket(
    idx: i32,
    abs_val: i64,
    prev_idx: &mut Option<i32>,
    prev_abs: &mut i64,
    spans: &mut Vec<HistogramSpan>,
    deltas: &mut Vec<i64>,
) {
    match *prev_idx {
        None => {
            spans.push(HistogramSpan {
                offset: idx,
                length: 1,
            });
        }
        Some(p) => {
            let gap = idx - p - 1;
            if gap == 0 {
                spans.last_mut().unwrap().length += 1;
            } else {
                spans.push(HistogramSpan {
                    offset: gap,
                    length: 1,
                });
            }
        }
    }
    deltas.push(abs_val - *prev_abs);
    *prev_idx = Some(idx);
    *prev_abs = abs_val;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunkenc::varbit::put_varbit_int;

    #[test]
    fn corrupt_histogram_u32_max_span_returns_error_not_oom() {
        // Craft a ~20-byte HistogramChunk with 1 sample and a positive span of length u32::MAX.
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
        put_varbit_uint(&mut bs, 1); // cnt
        put_varbit_uint(&mut bs, 0); // z_cnt
        bs.write_bits(1.0f64.to_bits(), 64); // sum

        let chunk = HistogramChunk::from_bytes(bs.into_bytes());
        let mut it = chunk.iter();
        let err = it.next_histogram().unwrap_err();
        assert!(
            matches!(err, ChunkError::InvalidChunk | ChunkError::UnexpectedEof),
            "unexpected error: {err:?}"
        );
    }
}
