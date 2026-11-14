//! Ported from github.com/grafana/mimir/pkg/ingester/client/custom.go and chunkcompat.go

use bytes::Bytes;

use crate::ingesterpb;

/// Mimir wire chunk encodings (from `pkg/storage/chunk/factory.go`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum MimirChunkEncoding {
    PrometheusXorChunk = 4,
    PrometheusHistogramChunk = 5,
    PrometheusFloatHistogramChunk = 6,
    PrometheusXor2Chunk = 7,
    PrometheusHistogramSTChunk = 8,
    PrometheusFloatHistogramSTChunk = 9,
}

/// Prometheus `chunkenc.Encoding` values (from `tsdb/chunkenc/chunk.go`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TsdbChunkEncoding {
    Xor,
    Xor2,
    Histogram,
    FloatHistogram,
    HistogramSt,
    FloatHistogramSt,
    Other(u8),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkMeta {
    pub min_time: i64,
    pub max_time: i64,
    pub encoding: TsdbChunkEncoding,
    pub data: Vec<u8>,
}

/// Converts a TSDB `ChunkMeta` into a wire `ingesterpb::Chunk`.
/// Ported from `ChunkFromMeta` in `pkg/ingester/client/custom.go`.
pub fn chunk_from_meta(meta: &ChunkMeta) -> Result<ingesterpb::Chunk, String> {
    let encoding = match meta.encoding {
        TsdbChunkEncoding::Xor => MimirChunkEncoding::PrometheusXorChunk as i32,
        TsdbChunkEncoding::Xor2 => MimirChunkEncoding::PrometheusXor2Chunk as i32,
        TsdbChunkEncoding::Histogram => MimirChunkEncoding::PrometheusHistogramChunk as i32,
        TsdbChunkEncoding::FloatHistogram => {
            MimirChunkEncoding::PrometheusFloatHistogramChunk as i32
        }
        TsdbChunkEncoding::HistogramSt => MimirChunkEncoding::PrometheusHistogramSTChunk as i32,
        TsdbChunkEncoding::FloatHistogramSt => {
            MimirChunkEncoding::PrometheusFloatHistogramSTChunk as i32
        }
        TsdbChunkEncoding::Other(v) => {
            return Err(format!(
                "unknown chunk encoding from TSDB chunk querier: {v}"
            ));
        }
    };

    Ok(ingesterpb::Chunk {
        start_timestamp_ms: meta.min_time,
        end_timestamp_ms: meta.max_time,
        encoding,
        data: Bytes::copy_from_slice(&meta.data),
    })
}

/// Ported from `DefaultMetricsMetadataRequest` in `pkg/ingester/client/custom.go`.
pub fn default_metrics_metadata_request() -> ingesterpb::MetricsMetadataRequest {
    ingesterpb::MetricsMetadataRequest {
        limit: -1,
        limit_per_metric: -1,
        metric: String::new(),
        metric_names: Vec::new(),
    }
}
