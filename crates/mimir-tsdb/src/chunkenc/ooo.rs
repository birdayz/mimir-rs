//! In-memory out-of-order (OOO) sample buffer that sorts, deduplicates, and cuts standard chunks.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/ooo_head.go.

use super::float_histogram::{AppendFloatHistogramOutcome, FloatHistogramChunk};
use super::float_histogram_st::{AppendFloatHistogramStOutcome, FloatHistogramStChunk};
use super::histogram::{AppendHistogramOutcome, HistogramChunk};
use super::histogram_model::{FloatHistogram, Histogram};
use super::histogram_st::{AppendHistogramStOutcome, HistogramStChunk};
use super::xor::XorChunk;
use super::xor2::Xor2Chunk;
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

/// Cut encoded chunk with its `[min_time, max_time]` bounds produced by [`OooChunk::to_encoded_chunks`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CutChunk {
    pub min_time: i64,
    pub max_time: i64,
    pub chunk: EncodedChunk,
}

/// Out-of-order sample buffer maintaining samples in ascending timestamp order.
///
/// Inserts for timestamps already present are dropped (returning `false`), matching `tsdb.OOOChunk`.
#[derive(Clone, Debug, Default)]
pub struct OooChunk {
    samples: Vec<OooSample>,
}

impl OooChunk {
    #[inline]
    pub fn new() -> Self {
        Self {
            samples: Vec::with_capacity(4),
        }
    }

    /// Inserts a sample maintaining ascending timestamp order.
    /// Returns `false` if a sample with timestamp `t` already exists.
    pub fn insert(&mut self, st: i64, t: i64, value: OooSampleValue) -> bool {
        if self.samples.last().is_none_or(|last| t > last.t) {
            self.samples.push(OooSample { st, t, value });
            return true;
        }

        let idx = self.samples.partition_point(|s| s.t < t);
        if idx < self.samples.len() && self.samples[idx].t == t {
            return false;
        }

        self.samples.insert(idx, OooSample { st, t, value });
        true
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
        let idx = self.samples.partition_point(|s| s.t < t);
        if idx >= self.samples.len() || self.samples[idx].t != t {
            return false;
        }
        self.samples[idx].value.prom_equals(value)
    }

    #[inline]
    pub fn num_samples(&self) -> usize {
        self.samples.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    #[inline]
    pub fn samples(&self) -> &[OooSample] {
        &self.samples
    }

    /// Retains only samples with timestamp `> maxt_inclusive`, returning `(min_time, max_time)`
    /// if any samples remain.
    pub fn retain_after(&mut self, maxt_inclusive: i64) -> Option<(i64, i64)> {
        self.samples.retain(|s| s.t > maxt_inclusive);
        self.samples
            .first()
            .zip(self.samples.last())
            .map(|(a, b)| (a.t, b.t))
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
        if self.samples.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(1);
        enum Active {
            None,
            Xor {
                cmint: i64,
                cmaxt: i64,
                chunk: XorChunk,
            },
            Xor2 {
                cmint: i64,
                cmaxt: i64,
                chunk: Xor2Chunk,
            },
            Hist {
                cmint: i64,
                cmaxt: i64,
                chunk: HistogramChunk,
                prev_state: Option<super::histogram::HistogramAppenderState>,
            },
            HistSt {
                cmint: i64,
                cmaxt: i64,
                chunk: HistogramStChunk,
                prev_state: Option<super::histogram::HistogramAppenderState>,
            },
            FloatHist {
                cmint: i64,
                cmaxt: i64,
                chunk: FloatHistogramChunk,
                prev_state: Option<super::float_histogram::FloatHistogramAppenderState>,
            },
            FloatHistSt {
                cmint: i64,
                cmaxt: i64,
                chunk: FloatHistogramStChunk,
                prev_state: Option<super::float_histogram::FloatHistogramAppenderState>,
            },
        }

        let flush_active = |active: &mut Active, out: &mut Vec<CutChunk>| {
            match std::mem::replace(active, Active::None) {
                Active::None => {}
                Active::Xor {
                    cmint,
                    cmaxt,
                    chunk,
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

        for s in &self.samples {
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
                    },
                    Encoding::Xor2 => Active::Xor2 {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: Xor2Chunk::new(),
                    },
                    Encoding::Histogram => Active::Hist {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: HistogramChunk::new(),
                        prev_state: None,
                    },
                    Encoding::HistogramSt => Active::HistSt {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: HistogramStChunk::new(),
                        prev_state: None,
                    },
                    Encoding::FloatHistogram => Active::FloatHist {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: FloatHistogramChunk::new(),
                        prev_state: None,
                    },
                    Encoding::FloatHistogramSt => Active::FloatHistSt {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: FloatHistogramStChunk::new(),
                        prev_state: None,
                    },
                    Encoding::None => unreachable!(),
                };
            }

            match (&mut active, &s.value) {
                (
                    Active::Xor {
                        cmaxt, chunk, ..
                    },
                    OooSampleValue::Float(v),
                ) => {
                    let mut app = chunk.appender()?;
                    app.append(s.st, s.t, *v)?;
                    *cmaxt = s.t;
                }
                (
                    Active::Xor2 {
                        cmaxt, chunk, ..
                    },
                    OooSampleValue::Float(v),
                ) => {
                    let mut app = chunk.appender()?;
                    app.append(s.st, s.t, *v)?;
                    *cmaxt = s.t;
                }
                (
                    Active::Hist {
                        cmint,
                        cmaxt,
                        chunk,
                        prev_state,
                    },
                    OooSampleValue::Histogram(h),
                ) => {
                    let mut app = chunk.appender()?;
                    let outcome =
                        app.append_histogram(prev_state.as_ref(), s.st, s.t, h, false)?;
                    let next_prev = app.state().clone();
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
                            *prev_state = Some(next_prev);
                        }
                    }
                }
                (
                    Active::HistSt {
                        cmint,
                        cmaxt,
                        chunk,
                        prev_state,
                    },
                    OooSampleValue::Histogram(h),
                ) => {
                    let mut app = chunk.appender()?;
                    let outcome =
                        app.append_histogram(prev_state.as_ref(), s.st, s.t, h, false)?;
                    let next_prev = app.state().clone();
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
                            *prev_state = Some(next_prev);
                        }
                    }
                }
                (
                    Active::FloatHist {
                        cmint,
                        cmaxt,
                        chunk,
                        prev_state,
                    },
                    OooSampleValue::FloatHistogram(fh),
                ) => {
                    let mut app = chunk.appender()?;
                    let outcome =
                        app.append_float_histogram(prev_state.as_ref(), s.st, s.t, fh, false)?;
                    let next_prev = app.state().clone();
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
                            *prev_state = Some(next_prev);
                        }
                    }
                }
                (
                    Active::FloatHistSt {
                        cmint,
                        cmaxt,
                        chunk,
                        prev_state,
                    },
                    OooSampleValue::FloatHistogram(fh),
                ) => {
                    let mut app = chunk.appender()?;
                    let outcome =
                        app.append_float_histogram(prev_state.as_ref(), s.st, s.t, fh, false)?;
                    let next_prev = app.state().clone();
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
                            *prev_state = Some(next_prev);
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
