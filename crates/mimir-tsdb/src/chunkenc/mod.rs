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
}
