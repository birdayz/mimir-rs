//! Prometheus `EncHistogramST` integer native histogram chunk codec with start timestamp (ST) support.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st.go.

use super::bstream::BitStream;
use super::histogram::{
    AppendableGauge, HistogramAppender, HistogramAppenderState, HistogramIterator,
};
use super::histogram_meta::{adjust_for_inserts, count_spans, insert_buckets, Insert};
use super::histogram_model::{
    CounterResetHeader, CounterResetHint, FloatHistogram, Histogram, HistogramSpan,
};
use super::st::{read_st_header, StDecoder, StEncoder, HISTOGRAM_ST_SAMPLE_COUNT_MASK};
use super::{ChunkError, Encoding};

const HISTOGRAM_HEADER_SIZE: usize = 3;
const HISTOGRAM_ST_HDR_POS: usize = 2;
const CHUNK_ALLOCATION_SIZE: usize = 128;
const CHUNK_COMPACT_CAPACITY_THRESHOLD: usize = 32;

/// Outcome of appending a histogram sample to a [`HistogramStChunk`].
#[derive(Debug)]
pub enum AppendHistogramStOutcome {
    Appended { recoded: bool },
    NewChunk(HistogramStChunk),
}

/// Prometheus `EncHistogramST` (`5`) encoded integer native histogram chunk with ST support.
#[derive(Clone, Debug)]
pub struct HistogramStChunk {
    b: BitStream,
}

impl PartialEq for HistogramStChunk {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for HistogramStChunk {}

impl Default for HistogramStChunk {
    fn default() -> Self {
        Self::new()
    }
}

impl HistogramStChunk {
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
        Encoding::HistogramSt
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
        (u16::from_be_bytes([bytes[0], bytes[1]]) & HISTOGRAM_ST_SAMPLE_COUNT_MASK) as usize
    }

    #[inline]
    pub fn counter_reset_header(&self) -> CounterResetHeader {
        let bytes = self.b.bytes();
        if bytes.len() < HISTOGRAM_HEADER_SIZE {
            return CounterResetHeader::UnknownCounterReset;
        }
        CounterResetHeader::from_header_byte(bytes[0])
    }

    #[inline]
    pub fn compact(&mut self) {
        self.b.compact(CHUNK_COMPACT_CAPACITY_THRESHOLD);
    }

    #[inline]
    pub fn iter(&self) -> HistogramStIterator<'_> {
        HistogramStIterator::new(self.b.bytes())
    }

    pub fn appender(&mut self) -> Result<HistogramStAppender<'_>, ChunkError> {
        if self.b.bytes().len() == HISTOGRAM_HEADER_SIZE {
            return Ok(HistogramStAppender {
                chunk: self,
                state: HistogramAppenderState::empty(),
                st_enc: StEncoder::default(),
            });
        }

        let (state, st_enc, valid_bits) = {
            let mut it = self.iter();
            while it.next_sample()?.is_some() {}
            (
                HistogramAppenderState {
                    num_samples: self.num_samples(),
                    counter_reset_header: self.counter_reset_header(),
                    schema: it.inner.schema,
                    z_threshold: it.inner.z_threshold,
                    p_spans: it.inner.p_spans,
                    n_spans: it.inner.n_spans,
                    custom_values: it.inner.custom_values,
                    t: it.inner.t,
                    cnt: it.inner.cnt,
                    z_cnt: it.inner.z_cnt,
                    t_delta: it.inner.t_delta,
                    cnt_delta: it.inner.cnt_delta,
                    z_cnt_delta: it.inner.z_cnt_delta,
                    p_buckets: it.inner.p_buckets,
                    n_buckets: it.inner.n_buckets,
                    p_buckets_delta: it.inner.p_buckets_delta,
                    n_buckets_delta: it.inner.n_buckets_delta,
                    sum: it.inner.sum,
                    leading: it.inner.leading,
                    trailing: it.inner.trailing,
                },
                StEncoder {
                    st: it.st_dec.st,
                    st_diff: it.st_dec.st_diff,
                    first_st_change_on: it.st_dec.first_st_change_on,
                    first_st_known: it.st_dec.first_st_known,
                },
                it.inner.br.valid_bits(),
            )
        };

        self.b.count = valid_bits;
        Ok(HistogramStAppender {
            chunk: self,
            state,
            st_enc,
        })
    }

    /// Appends `(st, t, h)` in O(1) using cached `(HistogramAppenderState, StEncoder)`.
    pub fn append_with_state(
        &mut self,
        cached: &mut Option<(HistogramAppenderState, StEncoder)>,
        st: i64,
        t: i64,
        h: &Histogram,
    ) -> Result<AppendHistogramStOutcome, ChunkError> {
        self.append_with_state_prev(cached, None, st, t, h)
    }

    /// Appends `(st, t, h)` in O(1) using cached `(HistogramAppenderState, StEncoder)` and
    /// optional previous-chunk state for computing `CounterResetHeader` on chunk rollover.
    pub fn append_with_state_prev(
        &mut self,
        cached: &mut Option<(HistogramAppenderState, StEncoder)>,
        prev: Option<&HistogramAppenderState>,
        st: i64,
        t: i64,
        h: &Histogram,
    ) -> Result<AppendHistogramStOutcome, ChunkError> {
        let mut app = match cached.take() {
            Some((state, st_enc)) => HistogramStAppender {
                chunk: self,
                state,
                st_enc,
            },
            None => self.appender()?,
        };
        let (out, next_cached) = app.append_histogram_with_state(prev, st, t, h, false)?;
        *cached = Some(next_cached);
        Ok(out)
    }
}

/// Appender for [`HistogramStChunk`].
pub struct HistogramStAppender<'a> {
    chunk: &'a mut HistogramStChunk,
    state: HistogramAppenderState,
    st_enc: StEncoder,
}

impl<'a> HistogramStAppender<'a> {
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
        bytes[0] = (bytes[0] & !CounterResetHeader::MASK) | (cr as u8);
    }

    fn set_num_samples(&mut self, num: usize) {
        self.state.num_samples = num;
        let bytes = self.chunk.b.bytes_mut();
        let cr_bits = bytes[0] & CounterResetHeader::MASK;
        let packed = (u16::from(cr_bits) << 8) | ((num as u16) & HISTOGRAM_ST_SAMPLE_COUNT_MASK);
        let be = packed.to_be_bytes();
        bytes[0] = be[0];
        bytes[1] = be[1];
    }

    fn append_histogram_st(&mut self, num: usize, st: i64, t: i64, h: &Histogram) -> usize {
        let prev_t = self.state.t;
        let new_num =
            HistogramAppender::append_raw_to_stream(&mut self.chunk.b, &mut self.state, num, t, h);
        self.st_enc.encode(
            &mut self.chunk.b,
            HISTOGRAM_ST_HDR_POS,
            new_num as u16,
            self.state.t,
            prev_t,
            st,
        );
        new_num
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

        let mut new_chunk = HistogramStChunk::new();
        {
            let mut new_app = new_chunk.appender()?;
            let mut it = self.chunk.iter();
            let mut num = 0usize;
            while let Some((t_old, mut h_old)) = it.next_histogram()? {
                let st_old = it.at_st();
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
                num = new_app.append_histogram_st(num, st_old, t_old, &h_old);
            }
            new_app.set_num_samples(num);
            new_app.set_counter_reset_header(crh);
            self.state = new_app.state.clone();
            self.st_enc = new_app.st_enc.clone();
        }
        *self.chunk = new_chunk;
        Ok(())
    }

    pub fn append_histogram(
        &mut self,
        prev: Option<&HistogramAppenderState>,
        st: i64,
        t: i64,
        h: &Histogram,
        append_only: bool,
    ) -> Result<AppendHistogramStOutcome, ChunkError> {
        let (out, _) = self.append_histogram_inner(prev, st, t, h, append_only, false)?;
        Ok(out)
    }

    fn append_histogram_with_state(
        &mut self,
        prev: Option<&HistogramAppenderState>,
        st: i64,
        t: i64,
        h: &Histogram,
        append_only: bool,
    ) -> Result<(AppendHistogramStOutcome, (HistogramAppenderState, StEncoder)), ChunkError> {
        let (out, next_cached) = self.append_histogram_inner(prev, st, t, h, append_only, true)?;
        let cached = match next_cached {
            Some(c) => c,
            None => (self.state.clone(), self.st_enc.clone()),
        };
        Ok((out, cached))
    }

    #[allow(clippy::type_complexity)]
    fn append_histogram_inner(
        &mut self,
        prev: Option<&HistogramAppenderState>,
        st: i64,
        t: i64,
        h: &Histogram,
        append_only: bool,
        return_new_state: bool,
    ) -> Result<(AppendHistogramStOutcome, Option<(HistogramAppenderState, StEncoder)>), ChunkError> {
        let num_samples = self.chunk.num_samples();
        if num_samples == HISTOGRAM_ST_SAMPLE_COUNT_MASK as usize {
            return Err(ChunkError::CapacityExceeded);
        }

        if num_samples == 0 {
            let new_num = self.append_histogram_st(0, st, t, h);
            self.set_num_samples(new_num);
            if h.counter_reset_hint == CounterResetHint::GaugeType {
                self.set_counter_reset_header(CounterResetHeader::GaugeType);
                return Ok((AppendHistogramStOutcome::Appended { recoded: false }, None));
            }
            if h.counter_reset_hint == CounterResetHint::CounterReset {
                self.set_counter_reset_header(CounterResetHeader::CounterReset);
            } else if let Some(p) = prev {
                let (_, _, _, _, _, cr) = p.appendable(h);
                self.set_counter_reset_header(cr);
            }
            return Ok((AppendHistogramStOutcome::Appended { recoded: false }, None));
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
                let mut new_chunk = HistogramStChunk::new();
                let new_cached = {
                    let mut app = new_chunk.appender()?;
                    app.set_counter_reset_header(cr_hint);
                    let n = app.append_histogram_st(0, st, t, h);
                    app.set_num_samples(n);
                    return_new_state.then_some((app.state, app.st_enc))
                };
                return Ok((AppendHistogramStOutcome::NewChunk(new_chunk), new_cached));
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
                HistogramAppender::recode_histogram(&mut h_owned, &p_bwd, &n_bwd);
                h_ref = &h_owned;
            } else {
                h_ref = h;
            }

            if !p_fwd.is_empty() || !n_fwd.is_empty() {
                if append_only {
                    return Err(ChunkError::HistogramLayoutChange);
                }
                self.recode_in_place(&p_fwd, &n_fwd, &h_ref.positive_spans, &h_ref.negative_spans)?;
                let n = self.append_histogram_st(self.chunk.num_samples(), st, t, h_ref);
                self.set_num_samples(n);
                return Ok((AppendHistogramStOutcome::Appended { recoded: true }, None));
            }

            let n = self.append_histogram_st(num_samples, st, t, h_ref);
            self.set_num_samples(n);
            return Ok((AppendHistogramStOutcome::Appended { recoded: false }, None));
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
            let mut new_chunk = HistogramStChunk::new();
            let new_cached = {
                let mut app = new_chunk.appender()?;
                app.set_counter_reset_header(CounterResetHeader::GaugeType);
                let n = app.append_histogram_st(0, st, t, h);
                app.set_num_samples(n);
                return_new_state.then_some((app.state, app.st_enc))
            };
            return Ok((AppendHistogramStOutcome::NewChunk(new_chunk), new_cached));
        }

        if !p_bwd.is_empty() || !n_bwd.is_empty() {
            if append_only {
                return Err(ChunkError::HistogramLayoutChange);
            }
            h_owned = h.clone();
            h_owned.positive_spans = p_merged;
            h_owned.negative_spans = n_merged;
            HistogramAppender::recode_histogram(&mut h_owned, &p_bwd, &n_bwd);
            h_ref = &h_owned;
        } else {
            h_ref = h;
        }

        if !p_fwd.is_empty() || !n_fwd.is_empty() {
            if append_only {
                return Err(ChunkError::HistogramLayoutChange);
            }
            self.recode_in_place(&p_fwd, &n_fwd, &h_ref.positive_spans, &h_ref.negative_spans)?;
            let n = self.append_histogram_st(self.chunk.num_samples(), st, t, h_ref);
            self.set_num_samples(n);
            return Ok((AppendHistogramStOutcome::Appended { recoded: true }, None));
        }

        let n = self.append_histogram_st(num_samples, st, t, h_ref);
        self.set_num_samples(n);
        Ok((AppendHistogramStOutcome::Appended { recoded: false }, None))
    }
}

/// Iterator over integer native histograms in a [`HistogramStChunk`].
#[derive(Clone, Debug)]
pub struct HistogramStIterator<'a> {
    inner: HistogramIterator<'a>,
    st_dec: StDecoder,
}

impl<'a> HistogramStIterator<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        if b.len() < HISTOGRAM_HEADER_SIZE {
            return Self {
                inner: HistogramIterator::new_with_header(
                    &[],
                    0,
                    CounterResetHeader::UnknownCounterReset,
                ),
                st_dec: StDecoder::default(),
            };
        }
        let num_total = u16::from_be_bytes([b[0], b[1]]) & HISTOGRAM_ST_SAMPLE_COUNT_MASK;
        let cr = CounterResetHeader::from_header_byte(b[0]);
        let (first_st_known, first_st_change_on) = read_st_header(&b[HISTOGRAM_ST_HDR_POS..]);
        Self {
            inner: HistogramIterator::new_with_header(&b[HISTOGRAM_HEADER_SIZE..], num_total, cr),
            st_dec: StDecoder {
                st: 0,
                st_diff: 0,
                first_st_change_on,
                first_st_known,
            },
        }
    }

    #[inline]
    pub fn at_st(&self) -> i64 {
        self.st_dec.st
    }

    #[inline]
    pub fn at_histogram(&self) -> (i64, Histogram) {
        self.inner.at_histogram()
    }

    #[inline]
    pub fn at_float_histogram(&self) -> (i64, FloatHistogram) {
        self.inner.at_float_histogram()
    }

    pub fn next_sample(&mut self) -> Result<Option<i64>, ChunkError> {
        let prev_t = self.inner.t;
        let Some(t) = self.inner.next_sample()? else {
            return Ok(None);
        };
        self.st_dec
            .decode(&mut self.inner.br, self.inner.num_read, t, prev_t)?;
        Ok(Some(t))
    }

    pub fn next_histogram(&mut self) -> Result<Option<(i64, Histogram)>, ChunkError> {
        if self.next_sample()?.is_none() {
            return Ok(None);
        }
        Ok(Some(self.at_histogram()))
    }

    pub fn next_float_histogram(&mut self) -> Result<Option<(i64, FloatHistogram)>, ChunkError> {
        if self.next_sample()?.is_none() {
            return Ok(None);
        }
        Ok(Some(self.at_float_histogram()))
    }

    pub fn seek(&mut self, t: i64) -> Result<Option<(i64, Histogram)>, ChunkError> {
        while t > self.inner.t || self.inner.num_read == 0 {
            if self.next_sample()?.is_none() {
                return Ok(None);
            }
        }
        Ok(Some(self.at_histogram()))
    }
}
