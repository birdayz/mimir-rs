//! Ported from github.com/grafana/mimir/pkg/ingester/client/mimir_util.go
//! and github.com/grafana/mimir/pkg/util/http.go (for TestMarshall).

use std::future::Future;

use tokio::sync::mpsc;

use crate::ingesterpb;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompressionType {
    NoCompression,
    RawSnappy,
}

/// Checks if the request is cancelled before and after sending on a stream channel.
/// Ported from `sendWithContextErrChecking` / `SendQueryStream` in `pkg/ingester/client/mimir_util.go`.
#[allow(clippy::result_large_err)]
pub async fn send_with_context_err_checking<F, Fut>(
    is_cancelled: F,
    send: Fut,
) -> Result<(), tonic::Status>
where
    F: Fn() -> bool,
    Fut: Future<Output = Result<(), tonic::Status>>,
{
    if is_cancelled() {
        return Err(tonic::Status::cancelled("context canceled"));
    }
    if let Err(err) = send.await {
        if is_cancelled() {
            return Err(tonic::Status::cancelled("context canceled"));
        }
        return Err(err);
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
pub async fn send_query_stream(
    tx: &mpsc::Sender<Result<ingesterpb::QueryStreamResponse, tonic::Status>>,
    resp: ingesterpb::QueryStreamResponse,
    cancelled: &tokio::sync::watch::Receiver<bool>,
) -> Result<(), tonic::Status> {
    send_with_context_err_checking(
        || *cancelled.borrow(),
        async {
            tx.send(Ok(resp))
                .await
                .map_err(|_| tonic::Status::unavailable("stream receiver dropped"))
        },
    )
    .await
}

#[allow(clippy::result_large_err)]
pub async fn send_label_names_and_values_response(
    tx: &mpsc::Sender<Result<ingesterpb::LabelNamesAndValuesResponse, tonic::Status>>,
    resp: ingesterpb::LabelNamesAndValuesResponse,
    cancelled: &tokio::sync::watch::Receiver<bool>,
) -> Result<(), tonic::Status> {
    send_with_context_err_checking(
        || *cancelled.borrow(),
        async {
            tx.send(Ok(resp))
                .await
                .map_err(|_| tonic::Status::unavailable("stream receiver dropped"))
        },
    )
    .await
}

#[allow(clippy::result_large_err)]
pub async fn send_label_values_cardinality_response(
    tx: &mpsc::Sender<Result<ingesterpb::LabelValuesCardinalityResponse, tonic::Status>>,
    resp: ingesterpb::LabelValuesCardinalityResponse,
    cancelled: &tokio::sync::watch::Receiver<bool>,
) -> Result<(), tonic::Status> {
    send_with_context_err_checking(
        || *cancelled.borrow(),
        async {
            tx.send(Ok(resp))
                .await
                .map_err(|_| tonic::Status::unavailable("stream receiver dropped"))
        },
    )
    .await
}

#[allow(clippy::result_large_err)]
pub async fn send_active_series_response(
    tx: &mpsc::Sender<Result<ingesterpb::ActiveSeriesResponse, tonic::Status>>,
    resp: ingesterpb::ActiveSeriesResponse,
    cancelled: &tokio::sync::watch::Receiver<bool>,
) -> Result<(), tonic::Status> {
    send_with_context_err_checking(
        || *cancelled.borrow(),
        async {
            tx.send(Ok(resp))
                .await
                .map_err(|_| tonic::Status::unavailable("stream receiver dropped"))
        },
    )
    .await
}

/// Ported from `AccumulateChunks` in `pkg/ingester/client/mimir_util.go`.
pub fn accumulate_chunks(mut a: Vec<ingesterpb::Chunk>, b: &[ingesterpb::Chunk]) -> Vec<ingesterpb::Chunk> {
    if a.is_empty() {
        return b.to_vec();
    }
    for ch in b {
        if !a.iter().any(|existing| existing == ch) {
            a.push(ch.clone());
        }
    }
    a
}

/// Ported from `util.SerializeProtoResponse` in `pkg/util/http.go` (used by `client_test.go::TestMarshall`).
pub fn serialize_proto_response<M: prost::Message>(
    msg: &M,
    compression: CompressionType,
) -> Result<Vec<u8>, String> {
    let data = msg.encode_to_vec();
    match compression {
        CompressionType::NoCompression => Ok(data),
        CompressionType::RawSnappy => snap::raw::Encoder::new()
            .compress_vec(&data)
            .map_err(|e| format!("snappy encoding: {e}")),
    }
}

/// Ported from `util.ParseProtoReader` in `pkg/util/http.go` (used by `client_test.go::TestMarshall`).
pub fn parse_proto_reader<M: prost::Message + Default>(
    bytes: &[u8],
    max_size: usize,
    compression: CompressionType,
) -> Result<(usize, M), String> {
    if bytes.len() > max_size {
        return Err(format!(
            "compressed message size {} exceeds limit {}",
            bytes.len(),
            max_size
        ));
    }
    let body = match compression {
        CompressionType::NoCompression => bytes.to_vec(),
        CompressionType::RawSnappy => {
            let dec_len = snap::raw::decompress_len(bytes)
                .map_err(|e| format!("snappy decoded len: {e}"))?;
            if dec_len > max_size {
                return Err(format!(
                    "uncompressed message size {dec_len} exceeds limit {max_size}"
                ));
            }
            snap::raw::Decoder::new()
                .decompress_vec(bytes)
                .map_err(|e| format!("snappy decode: {e}"))?
        }
    };
    if body.len() > max_size {
        return Err(format!(
            "message size {} exceeds limit {}",
            body.len(),
            max_size
        ));
    }
    let decoded = M::decode(body.as_slice()).map_err(|e| format!("proto decode: {e}"))?;
    Ok((body.len(), decoded))
}
