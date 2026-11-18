//! In-memory out-of-order (OOO) sample buffer that sorts, deduplicates, and cuts standard chunks.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/ooo_head.go.

use super::float_histogram::{
    AppendFloatHistogramOutcome, FloatHistogramAppenderState, FloatHistogramChunk,
};
use super::float_histogram_st::{AppendFloatHistogramStOutcome, FloatHistogramStChunk};
use super::histogram::{AppendHistogramOutcome, HistogramAppenderState, HistogramChunk};
use super::histogram_model::{FloatHistogram, Histogram};
use super::histogram_st::{AppendHistogramStOutcome, HistogramStChunk};
use super::st::StEncoder;
use super::xor::{XorAppendState, XorChunk};
use super::xor2::{Xor2AppendState, Xor2Chunk};
use super::{ChunkError, EncodedChunk, Encoding, ValueType};

/// Value stored in an [`OooSample`].
#[derive(Clone, Debug, PartialEq)]
pub enum OooSampleValue {
    Float(f64),
    Histogram(Histogram),
    FloatHistogram(FloatHistogram),
}

impl OooSampleValue {
    #[inline]
    pub fn value_type(&self) -> ValueType {
        match self {
            Self::Float(_) => ValueType::Float,
            Self::Histogram(_) => ValueType::Histogram,
            Self::FloatHistogram(_) => ValueType::FloatHistogram,
        }
    }

    #[inline]
    pub fn prom_equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Float(a), Self::Float(b)) => a.to_bits() == b.to_bits(),
            (Self::Histogram(a), Self::Histogram(b)) => a.prom_equals(b),
            (Self::FloatHistogram(a), Self::FloatHistogram(b)) => a.prom_equals(b),
            _ => false,
        }
    }
}

/// A single timestamped sample stored in an [`OooChunk`].
#[derive(Clone, Debug, PartialEq)]
pub struct OooSample {
    pub st: i64,
    pub t: i64,
    pub value: OooSampleValue,
}

/// Compact 16-byte representation for plain `st == 0` float OOO samples.
#[derive(Clone, Copy, Debug, PartialEq)]
struct PlainFloatOooSample {
    t: i64,
    v: f64,
}

#[derive(Clone, Debug, PartialEq)]
enum CompactOooValue {
    Float(f64),
    Histogram(Box<Histogram>),
    FloatHistogram(Box<FloatHistogram>),
}

impl CompactOooValue {
    #[inline]
    fn value_type(&self) -> ValueType {
        match self {
            Self::Float(_) => ValueType::Float,
            Self::Histogram(_) => ValueType::Histogram,
            Self::FloatHistogram(_) => ValueType::FloatHistogram,
        }
    }

    #[inline]
    fn prom_equals(&self, other: &OooSampleValue) -> bool {
        match (self, other) {
            (Self::Float(a), OooSampleValue::Float(b)) => a.to_bits() == b.to_bits(),
            (Self::Histogram(a), OooSampleValue::Histogram(b)) => a.prom_equals(b),
            (Self::FloatHistogram(a), OooSampleValue::FloatHistogram(b)) => a.prom_equals(b),
            _ => false,
        }
    }

    #[inline]
    fn to_sample_value(&self) -> OooSampleValue {
        match self {
            Self::Float(v) => OooSampleValue::Float(*v),
            Self::Histogram(h) => OooSampleValue::Histogram((**h).clone()),
            Self::FloatHistogram(fh) => OooSampleValue::FloatHistogram((**fh).clone()),
        }
    }
}

impl From<OooSampleValue> for CompactOooValue {
    #[inline]
    fn from(value: OooSampleValue) -> Self {
        match value {
            OooSampleValue::Float(v) => Self::Float(v),
            OooSampleValue::Histogram(h) => Self::Histogram(Box::new(h)),
            OooSampleValue::FloatHistogram(fh) => Self::FloatHistogram(Box::new(fh)),
        }
    }
}

/// Compact OOO sample storing `Histogram` / `FloatHistogram` out-of-line via `Box`.
#[derive(Clone, Debug, PartialEq)]
struct CompactOooSample {
    st: i64,
    t: i64,
    value: CompactOooValue,
}

/// Cut encoded chunk with its `[min_time, max_time]` bounds produced by [`OooChunk::to_encoded_chunks`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CutChunk {
    pub min_time: i64,
    pub max_time: i64,
    pub chunk: EncodedChunk,
}

#[derive(Clone, Debug)]
enum OooSamples {
    Floats(Vec<PlainFloatOooSample>),
    Mixed(Vec<CompactOooSample>),
}

impl Default for OooSamples {
    #[inline]
    fn default() -> Self {
        Self::Floats(Vec::new())
    }
}

#[inline]
const fn next_ooo_cap(cur_cap: usize) -> usize {
    match cur_cap {
        0 => 1,
        1 => 2,
        2..=3 => 4,
        4..=7 => 8,
        8..=11 => 12,
        12..=15 => 16,
        16..=23 => 24,
        24..=31 => 32,
        n => { let step = n / 2; n + if step > 8 { step } else { 8 } },
    }
}

#[inline]
const fn ooo_cap_for_len(len: usize) -> usize {
    match len {
        0 => 0,
        1 => 1,
        2 => 2,
        3..=4 => 4,
        5..=8 => 8,
        9..=12 => 12,
        13..=16 => 16,
        17..=24 => 24,
        25..=32 => 32,
        n => n,
    }
}

#[inline]
fn ensure_ooo_vec_cap<T>(v: &mut Vec<T>, cap_max: Option<usize>) {
    if v.len() == v.capacity() {
        let mut target = next_ooo_cap(v.capacity());
        if let Some(max) = cap_max {
            target = target.min(max.max(v.len() + 1));
        }
        v.reserve_exact(target - v.len());
    }
}

/// Out-of-order sample buffer maintaining samples in ascending timestamp order.
///
/// Inserts for timestamps already present are dropped (returning `false`), matching `tsdb.OOOChunk`.
#[derive(Clone, Debug, Default)]
pub struct OooChunk {
    samples: OooSamples,
}

impl OooChunk {
    #[inline]
    pub fn new() -> Self {
        Self {
            samples: OooSamples::Floats(Vec::new()),
        }
    }

    /// Inserts a sample maintaining ascending timestamp order.
    /// Returns `false` if a sample with timestamp `t` already exists.
    #[inline]
    pub fn insert(&mut self, st: i64, t: i64, value: OooSampleValue) -> bool {
        self.insert_inner(st, t, value, None)
    }

    /// Inserts a sample with an optional `cap_max` ceiling for exact initial/stepped capacity growth.
    #[inline]
    pub fn insert_capped(
        &mut self,
        st: i64,
        t: i64,
        value: OooSampleValue,
        cap_max: usize,
    ) -> bool {
        self.insert_inner(st, t, value, Some(cap_max))
    }

    fn insert_inner(
        &mut self,
        st: i64,
        t: i64,
        value: OooSampleValue,
        cap_max: Option<usize>,
    ) -> bool {
        match &mut self.samples {
            OooSamples::Floats(floats) => {
                if st == 0 {
                    if let OooSampleValue::Float(v) = value {
                        if floats.last().is_none_or(|last| t > last.t) {
                            ensure_ooo_vec_cap(floats, cap_max);
                            floats.push(PlainFloatOooSample { t, v });
                            return true;
                        }
                        let idx = floats.partition_point(|s| s.t < t);
                        if idx < floats.len() && floats[idx].t == t {
                            return false;
                        }
                        ensure_ooo_vec_cap(floats, cap_max);
                        floats.insert(idx, PlainFloatOooSample { t, v });
                        return true;
                    }
                }

                // Upgrade to Mixed representation only after confirming `t` is not a duplicate.
                let idx = if floats.last().is_none_or(|last| t > last.t) {
                    floats.len()
                } else {
                    let pos = floats.partition_point(|s| s.t < t);
                    if pos < floats.len() && floats[pos].t == t {
                        return false;
                    }
                    pos
                };

                let new_len = floats.len() + 1;
                let mut target_cap = ooo_cap_for_len(new_len);
                if let Some(max) = cap_max {
                    target_cap = target_cap.min(max.max(new_len));
                }
                let mut mixed = Vec::new();
                mixed.reserve_exact(target_cap);
                for (i, s) in floats.iter().enumerate() {
                    if i == idx {
                        mixed.push(CompactOooSample {
                            st,
                            t,
                            value: CompactOooValue::from(value.clone()),
                        });
                    }
                    mixed.push(CompactOooSample {
                        st: 0,
                        t: s.t,
                        value: CompactOooValue::Float(s.v),
                    });
                }
                if idx == floats.len() {
                    mixed.push(CompactOooSample {
                        st,
                        t,
                        value: CompactOooValue::from(value),
                    });
                }
                self.samples = OooSamples::Mixed(mixed);
                true
            }
            OooSamples::Mixed(mixed) => {
                if mixed.last().is_none_or(|last| t > last.t) {
                    ensure_ooo_vec_cap(mixed, cap_max);
                    mixed.push(CompactOooSample {
                        st,
                        t,
                        value: CompactOooValue::from(value),
                    });
                    return true;
                }

                let idx = mixed.partition_point(|s| s.t < t);
                if idx < mixed.len() && mixed[idx].t == t {
                    return false;
                }

                ensure_ooo_vec_cap(mixed, cap_max);
                mixed.insert(
                    idx,
                    CompactOooSample {
                        st,
                        t,
                        value: CompactOooValue::from(value),
                    },
                );
                true
            }
        }
    }

    #[inline]
    pub fn insert_float(&mut self, st: i64, t: i64, v: f64) -> bool {
        self.insert(st, t, OooSampleValue::Float(v))
    }

    #[inline]
    pub fn insert_histogram(&mut self, st: i64, t: i64, h: Histogram) -> bool {
        self.insert(st, t, OooSampleValue::Histogram(h))
    }

    #[inline]
    pub fn insert_float_histogram(&mut self, st: i64, t: i64, fh: FloatHistogram) -> bool {
        self.insert(st, t, OooSampleValue::FloatHistogram(fh))
    }

    /// Returns `true` if a sample at `t` exists whose value equals `value`.
    pub fn has_equal_sample(&self, t: i64, value: &OooSampleValue) -> bool {
        match &self.samples {
            OooSamples::Floats(floats) => {
                let idx = floats.partition_point(|s| s.t < t);
                if idx >= floats.len() || floats[idx].t != t {
                    return false;
                }
                match value {
                    OooSampleValue::Float(v) => floats[idx].v.to_bits() == v.to_bits(),
                    _ => false,
                }
            }
            OooSamples::Mixed(mixed) => {
                let idx = mixed.partition_point(|s| s.t < t);
                if idx >= mixed.len() || mixed[idx].t != t {
                    return false;
                }
                mixed[idx].value.prom_equals(value)
            }
        }
    }

    #[inline]
    pub fn num_samples(&self) -> usize {
        match &self.samples {
            OooSamples::Floats(v) => v.len(),
            OooSamples::Mixed(v) => v.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_samples() == 0
    }

    /// Materializes a copy of all stored samples in ascending timestamp order.
    pub fn samples(&self) -> Vec<OooSample> {
        match &self.samples {
            OooSamples::Floats(floats) => floats
                .iter()
                .map(|s| OooSample {
                    st: 0,
                    t: s.t,
                    value: OooSampleValue::Float(s.v),
                })
                .collect(),
            OooSamples::Mixed(mixed) => mixed
                .iter()
                .map(|s| OooSample {
                    st: s.st,
                    t: s.t,
                    value: s.value.to_sample_value(),
                })
                .collect(),
        }
    }

    /// Retains only samples with timestamp `> maxt_inclusive`, returning `(min_time, max_time)`
    /// if any samples remain.
    pub fn retain_after(&mut self, maxt_inclusive: i64) -> Option<(i64, i64)> {
        match &mut self.samples {
            OooSamples::Floats(v) => {
                v.retain(|s| s.t > maxt_inclusive);
                if v.is_empty() {
                    if v.capacity() > 0 {
                        *v = Vec::new();
                    }
                    return None;
                }
                let target = ooo_cap_for_len(v.len());
                if v.capacity() > target * 2 {
                    v.shrink_to(target);
                }
                v.first().zip(v.last()).map(|(a, b)| (a.t, b.t))
            }
            OooSamples::Mixed(v) => {
                v.retain(|s| s.t > maxt_inclusive);
                if v.is_empty() {
                    if v.capacity() > 0 {
                        *v = Vec::new();
                    }
                    return None;
                }
                let target = ooo_cap_for_len(v.len());
                if v.capacity() > target * 2 {
                    v.shrink_to(target);
                }
                v.first().zip(v.last()).map(|(a, b)| (a.t, b.t))
            }
        }
    }

    /// Cuts samples in `[mint, maxt]` into standard encoded chunks (`XorChunk` / `Xor2Chunk`,
    /// `HistogramChunk` / `HistogramStChunk`, `FloatHistogramChunk` / `FloatHistogramStChunk`),
    /// matching `tsdb.OOOChunk.ToEncodedChunks`.
    pub fn to_encoded_chunks(
        &self,
        mint: i64,
        maxt: i64,
        use_xor2: bool,
        use_histogram_st: bool,
    ) -> Result<Vec<CutChunk>, ChunkError> {
        match &self.samples {
            OooSamples::Floats(floats) => {
                if floats.is_empty() {
                    return Ok(Vec::new());
                }
                let start = floats.partition_point(|s| s.t < mint);
                let end = floats.partition_point(|s| s.t <= maxt);
                if start >= end {
                    return Ok(Vec::new());
                }
                let slice = &floats[start..end];
                let cmint = slice[0].t;
                let cmaxt = slice[slice.len() - 1].t;
                let chunk = if use_xor2 {
                    let mut c = Xor2Chunk::new();
                    let mut st = Xor2AppendState::new_empty();
                    for s in slice {
                        c.append_with_state(&mut st, 0, s.t, s.v)?;
                    }
                    EncodedChunk::Xor2(c)
                } else {
                    let mut c = XorChunk::new();
                    let mut st = XorAppendState::new_empty();
                    let mut prev_t = i64::MIN;
                    let mut prev_v = 0.0;
                    for s in slice {
                        c.append_with_state(&mut st, prev_t, prev_v, s.t, s.v)?;
                        prev_t = s.t;
                        prev_v = s.v;
                    }
                    EncodedChunk::Xor(c)
                };
                Ok(vec![CutChunk {
                    min_time: cmint,
                    max_time: cmaxt,
                    chunk,
                }])
            }
            OooSamples::Mixed(mixed) => {
                Self::mixed_to_encoded_chunks(mixed, mint, maxt, use_xor2, use_histogram_st)
            }
        }
    }

    fn mixed_to_encoded_chunks(
        samples: &[CompactOooSample],
        mint: i64,
        maxt: i64,
        use_xor2: bool,
        use_histogram_st: bool,
    ) -> Result<Vec<CutChunk>, ChunkError> {
        if samples.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(1);
        enum Active {
            None,
            Xor {
                cmint: i64,
                cmaxt: i64,
                chunk: XorChunk,
                state: XorAppendState,
                prev_t: i64,
                prev_v: f64,
            },
            Xor2 {
                cmint: i64,
                cmaxt: i64,
                chunk: Xor2Chunk,
                state: Xor2AppendState,
            },
            Hist {
                cmint: i64,
                cmaxt: i64,
                chunk: HistogramChunk,
                state: Option<HistogramAppenderState>,
                prev_state: Option<HistogramAppenderState>,
            },
            HistSt {
                cmint: i64,
                cmaxt: i64,
                chunk: HistogramStChunk,
                state: Option<(HistogramAppenderState, StEncoder)>,
                prev_state: Option<HistogramAppenderState>,
            },
            FloatHist {
                cmint: i64,
                cmaxt: i64,
                chunk: FloatHistogramChunk,
                state: Option<FloatHistogramAppenderState>,
                prev_state: Option<FloatHistogramAppenderState>,
            },
            FloatHistSt {
                cmint: i64,
                cmaxt: i64,
                chunk: FloatHistogramStChunk,
                state: Option<(FloatHistogramAppenderState, StEncoder)>,
                prev_state: Option<FloatHistogramAppenderState>,
            },
        }

        let flush_active = |active: &mut Active, out: &mut Vec<CutChunk>| {
            match std::mem::replace(active, Active::None) {
                Active::None => {}
                Active::Xor {
                    cmint,
                    cmaxt,
                    chunk,
                    ..
                } => {
                    out.push(CutChunk {
                        min_time: cmint,
                        max_time: cmaxt,
                        chunk: EncodedChunk::Xor(chunk),
                    });
                }
                Active::Xor2 {
                    cmint,
                    cmaxt,
                    chunk,
                    ..
                } => {
                    out.push(CutChunk {
                        min_time: cmint,
                        max_time: cmaxt,
                        chunk: EncodedChunk::Xor2(chunk),
                    });
                }
                Active::Hist {
                    cmint,
                    cmaxt,
                    chunk,
                    ..
                } => {
                    out.push(CutChunk {
                        min_time: cmint,
                        max_time: cmaxt,
                        chunk: EncodedChunk::Histogram(chunk),
                    });
                }
                Active::HistSt {
                    cmint,
                    cmaxt,
                    chunk,
                    ..
                } => {
                    out.push(CutChunk {
                        min_time: cmint,
                        max_time: cmaxt,
                        chunk: EncodedChunk::HistogramSt(chunk),
                    });
                }
                Active::FloatHist {
                    cmint,
                    cmaxt,
                    chunk,
                    ..
                } => {
                    out.push(CutChunk {
                        min_time: cmint,
                        max_time: cmaxt,
                        chunk: EncodedChunk::FloatHistogram(chunk),
                    });
                }
                Active::FloatHistSt {
                    cmint,
                    cmaxt,
                    chunk,
                    ..
                } => {
                    out.push(CutChunk {
                        min_time: cmint,
                        max_time: cmaxt,
                        chunk: EncodedChunk::FloatHistogramSt(chunk),
                    });
                }
            }
        };

        let mut active = Active::None;

        for s in samples {
            if s.t < mint {
                continue;
            }
            if s.t > maxt {
                break;
            }

            let enc = match s.value.value_type() {
                ValueType::Float => {
                    if use_xor2 {
                        Encoding::Xor2
                    } else {
                        Encoding::Xor
                    }
                }
                ValueType::Histogram => {
                    if use_histogram_st {
                        Encoding::HistogramSt
                    } else {
                        Encoding::Histogram
                    }
                }
                ValueType::FloatHistogram => {
                    if use_histogram_st {
                        Encoding::FloatHistogramSt
                    } else {
                        Encoding::FloatHistogram
                    }
                }
                ValueType::None => unreachable!(),
            };

            let cur_enc = match &active {
                Active::None => Encoding::None,
                Active::Xor { .. } => Encoding::Xor,
                Active::Xor2 { .. } => Encoding::Xor2,
                Active::Hist { .. } => Encoding::Histogram,
                Active::HistSt { .. } => Encoding::HistogramSt,
                Active::FloatHist { .. } => Encoding::FloatHistogram,
                Active::FloatHistSt { .. } => Encoding::FloatHistogramSt,
            };

            if cur_enc != enc {
                flush_active(&mut active, &mut out);
                active = match enc {
                    Encoding::Xor => Active::Xor {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: XorChunk::new(),
                        state: XorAppendState::new_empty(),
                        prev_t: i64::MIN,
                        prev_v: 0.0,
                    },
                    Encoding::Xor2 => Active::Xor2 {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: Xor2Chunk::new(),
                        state: Xor2AppendState::new_empty(),
                    },
                    Encoding::Histogram => Active::Hist {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: HistogramChunk::new(),
                        state: None,
                        prev_state: None,
                    },
                    Encoding::HistogramSt => Active::HistSt {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: HistogramStChunk::new(),
                        state: None,
                        prev_state: None,
                    },
                    Encoding::FloatHistogram => Active::FloatHist {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: FloatHistogramChunk::new(),
                        state: None,
                        prev_state: None,
                    },
                    Encoding::FloatHistogramSt => Active::FloatHistSt {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: FloatHistogramStChunk::new(),
                        state: None,
                        prev_state: None,
                    },
                    Encoding::None => unreachable!(),
                };
            }

            match (&mut active, &s.value) {
                (
                    Active::Xor {
                        cmaxt,
                        chunk,
                        state,
                        prev_t,
                        prev_v,
                        ..
                    },
                    CompactOooValue::Float(v),
                ) => {
                    chunk.append_with_state(state, *prev_t, *prev_v, s.t, *v)?;
                    *prev_t = s.t;
                    *prev_v = *v;
                    *cmaxt = s.t;
                }
                (
                    Active::Xor2 {
                        cmaxt, chunk, state, ..
                    },
                    CompactOooValue::Float(v),
                ) => {
                    chunk.append_with_state(state, s.st, s.t, *v)?;
                    *cmaxt = s.t;
                }
                (
                    Active::Hist {
                        cmint,
                        cmaxt,
                        chunk,
                        state,
                        prev_state,
                    },
                    CompactOooValue::Histogram(h),
                ) => {
                    let outcome =
                        chunk.append_with_state_prev(state, prev_state.as_ref(), s.st, s.t, h)?;
                    match outcome {
                        AppendHistogramOutcome::Appended { .. } => {
                            *cmaxt = s.t;
                        }
                        AppendHistogramOutcome::NewChunk(new_chunk) => {
                            let old = std::mem::replace(chunk, new_chunk);
                            out.push(CutChunk {
                                min_time: *cmint,
                                max_time: *cmaxt,
                                chunk: EncodedChunk::Histogram(old),
                            });
                            *cmint = s.t;
                            *cmaxt = s.t;
                            *prev_state = None;
                        }
                    }
                }
                (
                    Active::HistSt {
                        cmint,
                        cmaxt,
                        chunk,
                        state,
                        prev_state,
                    },
                    CompactOooValue::Histogram(h),
                ) => {
                    let outcome =
                        chunk.append_with_state_prev(state, prev_state.as_ref(), s.st, s.t, h)?;
                    match outcome {
                        AppendHistogramStOutcome::Appended { .. } => {
                            *cmaxt = s.t;
                        }
                        AppendHistogramStOutcome::NewChunk(new_chunk) => {
                            let old = std::mem::replace(chunk, new_chunk);
                            out.push(CutChunk {
                                min_time: *cmint,
                                max_time: *cmaxt,
                                chunk: EncodedChunk::HistogramSt(old),
                            });
                            *cmint = s.t;
                            *cmaxt = s.t;
                            *prev_state = None;
                        }
                    }
                }
                (
                    Active::FloatHist {
                        cmint,
                        cmaxt,
                        chunk,
                        state,
                        prev_state,
                    },
                    CompactOooValue::FloatHistogram(fh),
                ) => {
                    let outcome = chunk
                        .append_with_state_prev(state, prev_state.as_ref(), s.st, s.t, fh)?;
                    match outcome {
                        AppendFloatHistogramOutcome::Appended { .. } => {
                            *cmaxt = s.t;
                        }
                        AppendFloatHistogramOutcome::NewChunk(new_chunk) => {
                            let old = std::mem::replace(chunk, new_chunk);
                            out.push(CutChunk {
                                min_time: *cmint,
                                max_time: *cmaxt,
                                chunk: EncodedChunk::FloatHistogram(old),
                            });
                            *cmint = s.t;
                            *cmaxt = s.t;
                            *prev_state = None;
                        }
                    }
                }
                (
                    Active::FloatHistSt {
                        cmint,
                        cmaxt,
                        chunk,
                        state,
                        prev_state,
                    },
                    CompactOooValue::FloatHistogram(fh),
                ) => {
                    let outcome = chunk
                        .append_with_state_prev(state, prev_state.as_ref(), s.st, s.t, fh)?;
                    match outcome {
                        AppendFloatHistogramStOutcome::Appended { .. } => {
                            *cmaxt = s.t;
                        }
                        AppendFloatHistogramStOutcome::NewChunk(new_chunk) => {
                            let old = std::mem::replace(chunk, new_chunk);
                            out.push(CutChunk {
                                min_time: *cmint,
                                max_time: *cmaxt,
                                chunk: EncodedChunk::FloatHistogramSt(old),
                            });
                            *cmint = s.t;
                            *cmaxt = s.t;
                            *prev_state = None;
                        }
                    }
                }
                _ => unreachable!(),
            }
        }

        flush_active(&mut active, &mut out);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ooo_sample_sizes_are_compact() {
        assert_eq!(std::mem::size_of::<PlainFloatOooSample>(), 16);
        assert!(
            std::mem::size_of::<CompactOooSample>() <= 32,
            "CompactOooSample was {} bytes",
            std::mem::size_of::<CompactOooSample>()
        );
        assert!(std::mem::size_of::<OooChunk>() <= 32);
    }

    #[test]
    fn ooo_plain_float_stepped_capacity_and_upgrade_to_mixed() {
        let mut ooo = OooChunk::new();
        assert_eq!(
            match &ooo.samples {
                OooSamples::Floats(v) => v.capacity(),
                OooSamples::Mixed(_) => panic!("expected Floats"),
            },
            0
        );

        let mut observed_caps = Vec::new();
        for i in 0..32i64 {
            assert!(ooo.insert_capped(0, (32 - i) * 100, OooSampleValue::Float(i as f64), 32));
            let cap = match &ooo.samples {
                OooSamples::Floats(v) => v.capacity(),
                OooSamples::Mixed(_) => panic!("expected Floats"),
            };
            if observed_caps.last() != Some(&cap) {
                observed_caps.push(cap);
            }
        }
        assert_eq!(observed_caps, vec![1, 2, 4, 8, 12, 16, 24, 32]);

        // Duplicate timestamp does not trigger Mixed upgrade even when st != 0.
        assert!(!ooo.insert(99, 100, OooSampleValue::Float(123.0)));
        assert!(matches!(ooo.samples, OooSamples::Floats(_)));

        // Non-zero st upgrades seamlessly to Mixed while preserving all prior samples.
        assert!(ooo.insert(50, 150, OooSampleValue::Float(42.0)));
        assert!(matches!(ooo.samples, OooSamples::Mixed(_)));
        assert_eq!(ooo.num_samples(), 33);
        assert!(ooo.has_equal_sample(150, &OooSampleValue::Float(42.0)));
        assert!(ooo.has_equal_sample(100, &OooSampleValue::Float(31.0)));

        let retained = ooo.retain_after(3100);
        assert_eq!(retained, Some((3200, 3200)));
        assert_eq!(ooo.num_samples(), 1);
    }

    #[test]
    fn ooo_insert_sorts_dedups_and_cuts_xor() {
        let mut ooo = OooChunk::new();
        assert!(ooo.insert_float(0, 3000, 3.0));
        assert!(ooo.insert_float(0, 1000, 1.0));
        assert!(ooo.insert_float(0, 2000, 2.0));
        // Duplicate timestamp rejected
        assert!(!ooo.insert_float(0, 2000, 99.0));
        assert_eq!(ooo.num_samples(), 3);
        assert!(ooo.has_equal_sample(2000, &OooSampleValue::Float(2.0)));
        assert!(!ooo.has_equal_sample(2000, &OooSampleValue::Float(99.0)));

        let cut = ooo.to_encoded_chunks(i64::MIN, i64::MAX, false, false).unwrap();
        assert_eq!(cut.len(), 1);
        assert_eq!(cut[0].min_time, 1000);
        assert_eq!(cut[0].max_time, 3000);
        if let EncodedChunk::Xor(xc) = &cut[0].chunk {
            let mut it = xc.iter();
            let mut got = Vec::new();
            while let Some(s) = it.next_sample().unwrap() {
                got.push(s);
            }
            assert_eq!(got, vec![(1000, 1.0), (2000, 2.0), (3000, 3.0)]);
        } else {
            panic!("expected Xor chunk");
        }
    }

    #[test]
    fn ooo_cuts_xor2_and_histogram_st_when_enabled() {
        use super::super::histogram_model::HistogramSpan;

        let mut ooo_f = OooChunk::new();
        assert!(ooo_f.insert_float(900, 2000, 2.5));
        assert!(ooo_f.insert_float(900, 1000, 1.5));
        let cut_f = ooo_f.to_encoded_chunks(i64::MIN, i64::MAX, true, true).unwrap();
        assert_eq!(cut_f.len(), 1);
        assert_eq!(cut_f[0].chunk.encoding(), Encoding::Xor2);
        let EncodedChunk::Xor2(ref x2) = cut_f[0].chunk else {
            panic!("expected Xor2 chunk");
        };
        let mut it = x2.iter();
        assert_eq!(it.next_sample().unwrap(), Some((1000, 1.5)));
        assert_eq!(it.at_st(), 900);
        assert_eq!(it.next_sample().unwrap(), Some((2000, 2.5)));
        assert_eq!(it.at_st(), 900);
        assert_eq!(it.next_sample().unwrap(), None);

        let mut ooo_h = OooChunk::new();
        let h1 = Histogram {
            schema: 0,
            count: 2,
            sum: 3.0,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![2],
            ..Histogram::default()
        };
        let mut h2 = h1.clone();
        h2.count = 4;
        h2.sum = 6.0;
        h2.positive_buckets = vec![4];
        assert!(ooo_h.insert_histogram(500, 2000, h2.clone()));
        assert!(ooo_h.insert_histogram(500, 1000, h1.clone()));
        let cut_h = ooo_h.to_encoded_chunks(i64::MIN, i64::MAX, true, true).unwrap();
        assert_eq!(cut_h.len(), 1);
        assert_eq!(cut_h[0].chunk.encoding(), Encoding::HistogramSt);

        let mut ooo_fh = OooChunk::new();
        let fh1 = FloatHistogram {
            schema: 0,
            count: 2.0,
            sum: 3.0,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![2.0],
            ..FloatHistogram::default()
        };
        assert!(ooo_fh.insert_float_histogram(500, 1000, fh1));
        let cut_fh = ooo_fh
            .to_encoded_chunks(i64::MIN, i64::MAX, true, true)
            .unwrap();
        assert_eq!(cut_fh.len(), 1);
        assert_eq!(cut_fh[0].chunk.encoding(), Encoding::FloatHistogramSt);
    }
}
