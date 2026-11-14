//! In-memory out-of-order (OOO) sample buffer that sorts, deduplicates, and cuts standard chunks.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/ooo_head.go.

use super::float_histogram::{AppendFloatHistogramOutcome, FloatHistogramChunk};
use super::histogram::{AppendHistogramOutcome, HistogramChunk};
use super::histogram_model::{FloatHistogram, Histogram};
use super::xor::XorChunk;
use super::{ChunkError, EncodedChunk, ValueType};

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

    /// Cuts samples in `[mint, maxt]` into standard encoded chunks (`XorChunk`, `HistogramChunk`,
    /// `FloatHistogramChunk`), matching `tsdb.OOOChunk.ToEncodedChunks`.
    pub fn to_encoded_chunks(&self, mint: i64, maxt: i64) -> Result<Vec<CutChunk>, ChunkError> {
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
            Hist {
                cmint: i64,
                cmaxt: i64,
                chunk: HistogramChunk,
                prev_state: Option<super::histogram::HistogramAppenderState>,
            },
            FloatHist {
                cmint: i64,
                cmaxt: i64,
                chunk: FloatHistogramChunk,
                prev_state: Option<super::float_histogram::FloatHistogramAppenderState>,
            },
        }

        let mut active = Active::None;

        for s in &self.samples {
            if s.t < mint {
                continue;
            }
            if s.t > maxt {
                break;
            }

            let vt = s.value.value_type();
            let same_kind = matches!(
                (&active, vt),
                (Active::Xor { .. }, ValueType::Float)
                    | (Active::Hist { .. }, ValueType::Histogram)
                    | (Active::FloatHist { .. }, ValueType::FloatHistogram)
            );

            if !same_kind {
                match std::mem::replace(&mut active, Active::None) {
                    Active::None => {}
                    Active::Xor { cmint, cmaxt, chunk } => {
                        out.push(CutChunk {
                            min_time: cmint,
                            max_time: cmaxt,
                            chunk: EncodedChunk::Xor(chunk),
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
                }

                active = match vt {
                    ValueType::Float => Active::Xor {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: XorChunk::new(),
                    },
                    ValueType::Histogram => Active::Hist {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: HistogramChunk::new(),
                        prev_state: None,
                    },
                    ValueType::FloatHistogram => Active::FloatHist {
                        cmint: s.t,
                        cmaxt: s.t,
                        chunk: FloatHistogramChunk::new(),
                        prev_state: None,
                    },
                    ValueType::None => unreachable!(),
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
                _ => unreachable!(),
            }
        }

        match active {
            Active::None => {}
            Active::Xor { cmint, cmaxt, chunk } => {
                out.push(CutChunk {
                    min_time: cmint,
                    max_time: cmaxt,
                    chunk: EncodedChunk::Xor(chunk),
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
        }

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

        let cut = ooo.to_encoded_chunks(i64::MIN, i64::MAX).unwrap();
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
}
