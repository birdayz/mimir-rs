//! Bit-exact Prometheus TSDB chunk encodings (`EncXOR`, `EncHistogram`, `EncFloatHistogram`, `OOOChunk`).
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/*
//! and github.com/prometheus/prometheus/tsdb/ooo_head.go.

pub mod bstream;
pub mod float_histogram;
pub mod float_histogram_st;
pub mod histogram;
pub mod histogram_meta;
pub mod histogram_model;
pub mod histogram_st;
pub mod ooo;
pub mod st;
pub mod varbit;
pub mod xor;
pub mod xor2;

use thiserror::Error;

pub use float_histogram::{
    FloatHistogramAppender, FloatHistogramAppenderState, FloatHistogramChunk,
    FloatHistogramIterator,
};
pub use float_histogram_st::{
    FloatHistogramStAppender, FloatHistogramStChunk, FloatHistogramStIterator,
};
pub use histogram::{
    HistogramAppender, HistogramAppenderState, HistogramChunk, HistogramIterator,
};
pub use histogram_model::{
    CounterResetHeader, CounterResetHint, FloatHistogram, Histogram, HistogramErrorKind,
    HistogramSpan, HistogramValidationError,
    CUSTOM_BUCKETS_SCHEMA, EXPONENTIAL_SCHEMA_MAX, EXPONENTIAL_SCHEMA_MAX_RESERVED,
    EXPONENTIAL_SCHEMA_MIN, EXPONENTIAL_SCHEMA_MIN_RESERVED,
};
pub use histogram_st::{HistogramStAppender, HistogramStChunk, HistogramStIterator};
pub use ooo::{OooChunk, OooSample, OooSampleValue};
pub use st::StEncoder;
pub use xor::{XorAppendState, XorAppender, XorChunk, XorIterator};
pub use xor2::{Xor2AppendState, Xor2Appender, Xor2Chunk, Xor2Iterator};

/// Maximum size in bytes of an XOR chunk.
pub const MAX_BYTES_PER_XOR_CHUNK: usize = 1024;
/// Maximum size before cutting a new XOR chunk.
pub const MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND: usize = MAX_BYTES_PER_XOR_CHUNK - 19;
/// Target byte size per histogram chunk.
pub const TARGET_BYTES_PER_HISTOGRAM_CHUNK: usize = 1024;
/// Minimum samples per histogram chunk before size-based cutting.
pub const MIN_SAMPLES_PER_HISTOGRAM_CHUNK: usize = 10;

/// IEEE-754 bit pattern for Prometheus `StaleNaN`.
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;
/// IEEE-754 bit pattern for Prometheus `NormalNaN`.
pub const NORMAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0001;
/// IEEE-754 bit pattern for Prometheus `QuietZeroNaN`.
pub const QUIET_ZERO_NAN_BITS: u64 = 0x7ff0_0000_0000_0003;

/// Returns `true` if `v` is the exact Prometheus `StaleNaN` sentinel.
#[inline]
pub fn is_stale_nan(v: f64) -> bool {
    v.to_bits() == STALE_NAN_BITS
}

/// Returns a `f64` with the exact Prometheus `StaleNaN` bit pattern.
#[inline]
pub const fn stale_nan() -> f64 {
    f64::from_bits(STALE_NAN_BITS)
}

#[derive(Debug, Error)]
pub enum ChunkError {
    #[error("unexpected end of chunk bitstream")]
    UnexpectedEof,
    #[error("invalid bit pattern in varbit stream: {0:#b}")]
    InvalidVarbitPattern(u8),
    #[error("invalid chunk encoding: {0}")]
    InvalidEncoding(u8),
    #[error("invalid chunk header (len={len}, expected at least {min})")]
    InvalidHeader { len: usize, min: usize },
    #[error("invalid chunk data")]
    InvalidChunk,
    #[error("unknown histogram schema: {0}")]
    UnknownSchema(i32),
    #[error("histogram counter reset")]
    HistogramCounterReset,
    #[error("histogram schema change")]
    HistogramSchemaChange,
    #[error("float histogram counter reset")]
    FloatHistogramCounterReset,
    #[error("float histogram schema change")]
    FloatHistogramSchemaChange,
    #[error("histogram layout change")]
    HistogramLayoutChange,
    #[error("chunk capacity exceeded")]
    CapacityExceeded,
    #[error("failed to reduce histogram resolution: {0}")]
    ResolutionReduce(String),
}

/// Prometheus chunk encoding identifier (`tsdb/chunkenc/chunk.go`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Encoding {
    None = 0,
    Xor = 1,
    Histogram = 2,
    FloatHistogram = 3,
    Xor2 = 4,
    HistogramSt = 5,
    FloatHistogramSt = 6,
}

impl Encoding {
    #[inline]
    pub fn from_u8(v: u8) -> Result<Self, ChunkError> {
        match v {
            0 => Ok(Self::None),
            1 => Ok(Self::Xor),
            2 => Ok(Self::Histogram),
            3 => Ok(Self::FloatHistogram),
            4 => Ok(Self::Xor2),
            5 => Ok(Self::HistogramSt),
            6 => Ok(Self::FloatHistogramSt),
            other => Err(ChunkError::InvalidEncoding(other)),
        }
    }

    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    #[inline]
    pub const fn is_valid(self) -> bool {
        matches!(
            self,
            Self::Xor
                | Self::Histogram
                | Self::FloatHistogram
                | Self::Xor2
                | Self::HistogramSt
                | Self::FloatHistogramSt
        )
    }

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Xor => "XOR",
            Self::Histogram => "histogram",
            Self::FloatHistogram => "floathistogram",
            Self::Xor2 => "XOR2",
            Self::HistogramSt => "histogramST",
            Self::FloatHistogramSt => "floathistogramST",
        }
    }

    /// Reports whether two encodings are mutually compatible with respect to
    /// sample-value encoding (`Xor` and `Xor2`), matching Go `chunkenc.CompatibleValues`.
    #[inline]
    pub const fn compatible_values(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::Xor | Self::Xor2, Self::Xor | Self::Xor2)
        )
    }
}

impl std::fmt::Display for Encoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Sample value type yielded by chunk iterators.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ValueType {
    None = 0,
    Float = 1,
    Histogram = 2,
    FloatHistogram = 3,
}

/// Unified encoded chunk wrapper over concrete Prometheus chunk types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EncodedChunk {
    Xor(XorChunk),
    Xor2(Xor2Chunk),
    Histogram(HistogramChunk),
    HistogramSt(HistogramStChunk),
    FloatHistogram(FloatHistogramChunk),
    FloatHistogramSt(FloatHistogramStChunk),
}

impl EncodedChunk {
    pub fn from_bytes(encoding: Encoding, data: Vec<u8>) -> Result<Self, ChunkError> {
        match encoding {
            Encoding::Xor => {
                if data.len() < 2 {
                    return Err(ChunkError::InvalidHeader {
                        len: data.len(),
                        min: 2,
                    });
                }
                Ok(Self::Xor(XorChunk::from_bytes(data)))
            }
            Encoding::Xor2 => {
                if data.len() < 3 {
                    return Err(ChunkError::InvalidHeader {
                        len: data.len(),
                        min: 3,
                    });
                }
                Ok(Self::Xor2(Xor2Chunk::from_bytes(data)))
            }
            Encoding::Histogram => {
                if data.len() < 3 {
                    return Err(ChunkError::InvalidHeader {
                        len: data.len(),
                        min: 3,
                    });
                }
                Ok(Self::Histogram(HistogramChunk::from_bytes(data)))
            }
            Encoding::HistogramSt => {
                if data.len() < 3 {
                    return Err(ChunkError::InvalidHeader {
                        len: data.len(),
                        min: 3,
                    });
                }
                Ok(Self::HistogramSt(HistogramStChunk::from_bytes(data)))
            }
            Encoding::FloatHistogram => {
                if data.len() < 3 {
                    return Err(ChunkError::InvalidHeader {
                        len: data.len(),
                        min: 3,
                    });
                }
                Ok(Self::FloatHistogram(FloatHistogramChunk::from_bytes(data)))
            }
            Encoding::FloatHistogramSt => {
                if data.len() < 3 {
                    return Err(ChunkError::InvalidHeader {
                        len: data.len(),
                        min: 3,
                    });
                }
                Ok(Self::FloatHistogramSt(FloatHistogramStChunk::from_bytes(
                    data,
                )))
            }
            other => Err(ChunkError::InvalidEncoding(other.as_u8())),
        }
    }

    #[inline]
    pub fn encoding(&self) -> Encoding {
        match self {
            Self::Xor(_) => Encoding::Xor,
            Self::Xor2(_) => Encoding::Xor2,
            Self::Histogram(_) => Encoding::Histogram,
            Self::HistogramSt(_) => Encoding::HistogramSt,
            Self::FloatHistogram(_) => Encoding::FloatHistogram,
            Self::FloatHistogramSt(_) => Encoding::FloatHistogramSt,
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Xor(c) => c.as_bytes(),
            Self::Xor2(c) => c.as_bytes(),
            Self::Histogram(c) => c.as_bytes(),
            Self::HistogramSt(c) => c.as_bytes(),
            Self::FloatHistogram(c) => c.as_bytes(),
            Self::FloatHistogramSt(c) => c.as_bytes(),
        }
    }

    #[inline]
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Self::Xor(c) => c.into_bytes(),
            Self::Xor2(c) => c.into_bytes(),
            Self::Histogram(c) => c.into_bytes(),
            Self::HistogramSt(c) => c.into_bytes(),
            Self::FloatHistogram(c) => c.into_bytes(),
            Self::FloatHistogramSt(c) => c.into_bytes(),
        }
    }

    #[inline]
    pub fn num_samples(&self) -> usize {
        match self {
            Self::Xor(c) => c.num_samples(),
            Self::Xor2(c) => c.num_samples(),
            Self::Histogram(c) => c.num_samples(),
            Self::HistogramSt(c) => c.num_samples(),
            Self::FloatHistogram(c) => c.num_samples(),
            Self::FloatHistogramSt(c) => c.num_samples(),
        }
    }

    #[inline]
    pub(crate) fn into_bstream(self) -> (Encoding, bstream::BitStream) {
        match self {
            Self::Xor(c) => (Encoding::Xor, c.b),
            Self::Xor2(c) => (Encoding::Xor2, c.b),
            Self::Histogram(c) => (Encoding::Histogram, c.b),
            Self::HistogramSt(c) => (Encoding::HistogramSt, c.b),
            Self::FloatHistogram(c) => (Encoding::FloatHistogram, c.b),
            Self::FloatHistogramSt(c) => (Encoding::FloatHistogramSt, c.b),
        }
    }

    #[inline]
    pub fn reset(&mut self, stream: Vec<u8>) {
        match self {
            Self::Xor(c) => c.reset(stream),
            Self::Xor2(c) => c.reset(stream),
            Self::Histogram(c) => c.reset(stream),
            Self::HistogramSt(c) => c.reset(stream),
            Self::FloatHistogram(c) => c.reset(stream),
            Self::FloatHistogramSt(c) => c.reset(stream),
        }
    }

    #[doc(hidden)]
    #[inline]
    pub fn bstream_parts(&self) -> (&[u8], u8) {
        match self {
            Self::Xor(c) => (c.b.bytes(), c.b.count()),
            Self::Xor2(c) => (c.b.bytes(), c.b.count()),
            Self::Histogram(c) => (c.b.bytes(), c.b.count()),
            Self::HistogramSt(c) => (c.b.bytes(), c.b.count()),
            Self::FloatHistogram(c) => (c.b.bytes(), c.b.count()),
            Self::FloatHistogramSt(c) => (c.b.bytes(), c.b.count()),
        }
    }

    #[doc(hidden)]
    #[inline]
    pub fn set_bstream_for_test(&mut self, stream: Vec<u8>, count: u8) {
        let bs = bstream::BitStream::from_bytes(stream, count);
        match self {
            Self::Xor(c) => c.b = bs,
            Self::Xor2(c) => c.b = bs,
            Self::Histogram(c) => c.b = bs,
            Self::HistogramSt(c) => c.b = bs,
            Self::FloatHistogram(c) => c.b = bs,
            Self::FloatHistogramSt(c) => c.b = bs,
        }
    }

    #[inline]
    pub(crate) fn from_bstream(enc: Encoding, b: bstream::BitStream) -> Self {
        match enc {
            Encoding::Xor => Self::Xor(XorChunk { b }),
            Encoding::Xor2 => Self::Xor2(Xor2Chunk { b }),
            Encoding::Histogram => Self::Histogram(HistogramChunk { b }),
            Encoding::HistogramSt => Self::HistogramSt(HistogramStChunk { b }),
            Encoding::FloatHistogram => Self::FloatHistogram(FloatHistogramChunk { b }),
            Encoding::FloatHistogramSt => Self::FloatHistogramSt(FloatHistogramStChunk { b }),
            Encoding::None => unreachable!("Encoding::None cannot form an EncodedChunk"),
        }
    }
}

const MAX_POOLED_CHUNKS: usize = 64;

fn push_capped<T>(pool: &parking_lot::Mutex<Vec<T>>, item: T) {
    let mut guard = pool.lock();
    if guard.len() < MAX_POOLED_CHUNKS {
        guard.push(item);
    }
}

/// Chunk object pool matching Prometheus `tsdb/chunkenc.Pool`.
#[derive(Default)]
pub struct Pool {
    xor: parking_lot::Mutex<Vec<XorChunk>>,
    xor2: parking_lot::Mutex<Vec<Xor2Chunk>>,
    histogram: parking_lot::Mutex<Vec<HistogramChunk>>,
    histogram_st: parking_lot::Mutex<Vec<HistogramStChunk>>,
    float_histogram: parking_lot::Mutex<Vec<FloatHistogramChunk>>,
    float_histogram_st: parking_lot::Mutex<Vec<FloatHistogramStChunk>>,
}

impl Pool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, encoding: Encoding, b: Vec<u8>) -> Result<EncodedChunk, ChunkError> {
        match encoding {
            Encoding::Xor => {
                let mut c = self.xor.lock().pop().unwrap_or_else(|| XorChunk::from_bytes(Vec::new()));
                c.reset(b);
                Ok(EncodedChunk::Xor(c))
            }
            Encoding::Xor2 => {
                let mut c = self.xor2.lock().pop().unwrap_or_else(|| Xor2Chunk::from_bytes(Vec::new()));
                c.reset(b);
                Ok(EncodedChunk::Xor2(c))
            }
            Encoding::Histogram => {
                let mut c = self.histogram.lock().pop().unwrap_or_else(|| HistogramChunk::from_bytes(Vec::new()));
                c.reset(b);
                Ok(EncodedChunk::Histogram(c))
            }
            Encoding::HistogramSt => {
                let mut c = self.histogram_st.lock().pop().unwrap_or_else(|| HistogramStChunk::from_bytes(Vec::new()));
                c.reset(b);
                Ok(EncodedChunk::HistogramSt(c))
            }
            Encoding::FloatHistogram => {
                let mut c = self.float_histogram.lock().pop().unwrap_or_else(|| FloatHistogramChunk::from_bytes(Vec::new()));
                c.reset(b);
                Ok(EncodedChunk::FloatHistogram(c))
            }
            Encoding::FloatHistogramSt => {
                let mut c = self.float_histogram_st.lock().pop().unwrap_or_else(|| FloatHistogramStChunk::from_bytes(Vec::new()));
                c.reset(b);
                Ok(EncodedChunk::FloatHistogramSt(c))
            }
            Encoding::None => Err(ChunkError::InvalidEncoding(Encoding::None.as_u8())),
        }
    }

    pub fn put(&self, c: &mut EncodedChunk) -> Result<(), ChunkError> {
        c.reset(Vec::new());
        match c {
            EncodedChunk::Xor(_) => push_capped(&self.xor, XorChunk::from_bytes(Vec::new())),
            EncodedChunk::Xor2(_) => push_capped(&self.xor2, Xor2Chunk::from_bytes(Vec::new())),
            EncodedChunk::Histogram(_) => push_capped(&self.histogram, HistogramChunk::from_bytes(Vec::new())),
            EncodedChunk::HistogramSt(_) => push_capped(&self.histogram_st, HistogramStChunk::from_bytes(Vec::new())),
            EncodedChunk::FloatHistogram(_) => push_capped(&self.float_histogram, FloatHistogramChunk::from_bytes(Vec::new())),
            EncodedChunk::FloatHistogramSt(_) => push_capped(&self.float_histogram_st, FloatHistogramStChunk::from_bytes(Vec::new())),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_put_caps_capacity_at_64() {
        let pool = Pool::new();
        for _ in 0..128 {
            let mut c = pool.get(Encoding::Xor, Vec::new()).unwrap();
            pool.put(&mut c).unwrap();
        }
        assert!(pool.xor.lock().len() <= MAX_POOLED_CHUNKS);
    }
}
