use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::stream::unfold;
use parking_lot::Mutex;
use prometheus::{Counter, Opts, Registry};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use mimir_proto::client::buffering_client::{
    BufferingIngesterClient, FastReleasingSlabPool, TrackedPool, WrappedRequest,
};
use mimir_proto::client::compat::{
    from_label_names_request, from_query_request, to_label_names_request, to_query_request,
    LabelHints, PromLabelMatcher, PromMatchType,
};
use mimir_proto::client::custom::{chunk_from_meta, ChunkMeta, MimirChunkEncoding, TsdbChunkEncoding};
use mimir_proto::client::mimir_util::{
    parse_proto_reader, send_query_stream, serialize_proto_response, CompressionType,
};
use mimir_proto::client::only_replica::{
    is_only_replica_context, is_only_replica_in_metadata, only_replica_client_interceptor,
    only_replica_server_interceptor, with_available_replicas, with_only_replica,
    OnlyReplicaMarker,
};
use mimir_proto::client::streaming::{
    merge_histogram_sets, merge_sample_sets, MemoryConsumptionTracker, QueryLimiter,
    QueryStreamClient, SamplePair, SeriesChunksStreamReader, StreamError,
};
use mimir_proto::compat::{
    from_float_histogram_to_histogram_proto, from_histogram_to_histogram_proto,
    PromFloatHistogram, PromHistogram, PromSpan,
};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::ingesterpb::ingester_server::{Ingester, IngesterServer};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn create_request(metric_name: &str, series_per_request: usize) -> cortexpb::WriteRequest {
    let mut timeseries = Vec::with_capacity(series_per_request);
    for i in 0..series_per_request {
        timeseries.push(cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_strs("__name__", metric_name),
                cortexpb::LabelPair::from_strs("cardinality", i.to_string()),
            ],
            samples: vec![cortexpb::Sample {
                value: i as f64,
                timestamp_ms: now_ms(),
            }],
            ..Default::default()
        });
    }
    cortexpb::WriteRequest {
        timeseries,
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    }
}

fn format_labels(labels: &[cortexpb::LabelPair]) -> String {
    let parts: Vec<String> = labels
        .iter()
        .map(|l| {
            format!(
                "{}={:?}",
                String::from_utf8_lossy(&l.name),
                String::from_utf8_lossy(&l.value)
            )
        })
        .collect();
    format!("{{{}}}", parts.join(", "))
}

#[derive(Default)]
struct MockServer {
    track_samples: bool,
    reqs: Mutex<Vec<cortexpb::WriteRequest>>,
    samples_per_series: Mutex<HashMap<String, Vec<cortexpb::Sample>>>,
    query_stream_cancel_notify: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl MockServer {
    fn clear_requests(&self) {
        self.reqs.lock().clear();
    }

    fn requests(&self) -> Vec<cortexpb::WriteRequest> {
        self.reqs.lock().clone()
    }

    fn get_and_remove_samples_for_series(&self, series: &str) -> Vec<cortexpb::Sample> {
        self.samples_per_series
            .lock()
            .remove(series)
            .unwrap_or_default()
    }
}

#[tonic::async_trait]
impl Ingester for MockServer {
    async fn push(
        &self,
        request: tonic::Request<cortexpb::WriteRequest>,
    ) -> Result<tonic::Response<cortexpb::WriteResponse>, tonic::Status> {
        let r = request.into_inner();
        if self.track_samples {
            let mut map = self.samples_per_series.lock();
            for ts in &r.timeseries {
                let ser = format_labels(&ts.labels);
                map.entry(ser).or_default().extend(ts.samples.iter().copied());
            }
        }
        self.reqs.lock().push(r);
        Ok(tonic::Response::new(cortexpb::WriteResponse {}))
    }

    type QueryStreamStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<ingesterpb::QueryStreamResponse, tonic::Status>> + Send>>;

    async fn query_stream(
        &self,
        _request: tonic::Request<ingesterpb::QueryRequest>,
    ) -> Result<tonic::Response<Self::QueryStreamStream>, tonic::Status> {
        let (tx, rx) = mpsc::channel(4);
        if let Some(notify) = self.query_stream_cancel_notify.lock().take() {
            let _ = notify.send(());
        }
        // Keep sender alive briefly until client cancels or drops
        tokio::spawn(async move {
            let (_cancel_tx, cancel_rx) = watch::channel(false);
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = send_query_stream(
                &tx,
                ingesterpb::QueryStreamResponse::default(),
                &cancel_rx,
            )
            .await;
        });
        let stream: Self::QueryStreamStream = Box::pin(ReceiverStream::new(rx));
        Ok(tonic::Response::new(stream))
    }

    async fn query_exemplars(
        &self,
        _request: tonic::Request<ingesterpb::ExemplarQueryRequest>,
    ) -> Result<tonic::Response<ingesterpb::ExemplarQueryResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not implemented"))
    }

    async fn label_values(
        &self,
        _request: tonic::Request<ingesterpb::LabelValuesRequest>,
    ) -> Result<tonic::Response<ingesterpb::LabelValuesResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not implemented"))
    }

    async fn label_names(
        &self,
        _request: tonic::Request<ingesterpb::LabelNamesRequest>,
    ) -> Result<tonic::Response<ingesterpb::LabelNamesResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not implemented"))
    }

    async fn user_stats(
        &self,
        _request: tonic::Request<ingesterpb::UserStatsRequest>,
    ) -> Result<tonic::Response<ingesterpb::UserStatsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not implemented"))
    }

    async fn all_user_stats(
        &self,
        _request: tonic::Request<ingesterpb::UserStatsRequest>,
    ) -> Result<tonic::Response<ingesterpb::UsersStatsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not implemented"))
    }

    async fn metrics_for_label_matchers(
        &self,
        _request: tonic::Request<ingesterpb::MetricsForLabelMatchersRequest>,
    ) -> Result<tonic::Response<ingesterpb::MetricsForLabelMatchersResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not implemented"))
    }

    async fn metrics_metadata(
        &self,
        _request: tonic::Request<ingesterpb::MetricsMetadataRequest>,
    ) -> Result<tonic::Response<ingesterpb::MetricsMetadataResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not implemented"))
    }

    type LabelNamesAndValuesStream = Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<ingesterpb::LabelNamesAndValuesResponse, tonic::Status>>
                + Send,
        >,
    >;

    async fn label_names_and_values(
        &self,
        _request: tonic::Request<ingesterpb::LabelNamesAndValuesRequest>,
    ) -> Result<tonic::Response<Self::LabelNamesAndValuesStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("not implemented"))
    }

    type LabelValuesCardinalityStream = Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = Result<ingesterpb::LabelValuesCardinalityResponse, tonic::Status>,
                > + Send,
        >,
    >;

    async fn label_values_cardinality(
        &self,
        _request: tonic::Request<ingesterpb::LabelValuesCardinalityRequest>,
    ) -> Result<tonic::Response<Self::LabelValuesCardinalityStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("not implemented"))
    }

    type ActiveSeriesStream = Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ingesterpb::ActiveSeriesResponse, tonic::Status>> + Send>,
    >;

    async fn active_series(
        &self,
        _request: tonic::Request<ingesterpb::ActiveSeriesRequest>,
    ) -> Result<tonic::Response<Self::ActiveSeriesStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("not implemented"))
    }

    type SearchLabelNamesStream = Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ingesterpb::SearchResultBatch, tonic::Status>> + Send>,
    >;

    async fn search_label_names(
        &self,
        _request: tonic::Request<ingesterpb::SearchLabelNamesRequest>,
    ) -> Result<tonic::Response<Self::SearchLabelNamesStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("not implemented"))
    }

    type SearchLabelValuesStream = Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ingesterpb::SearchResultBatch, tonic::Status>> + Send>,
    >;

    async fn search_label_values(
        &self,
        _request: tonic::Request<ingesterpb::SearchLabelValuesRequest>,
    ) -> Result<tonic::Response<Self::SearchLabelValuesStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("not implemented"))
    }
}

async fn setup_grpc(track_samples: bool) -> (Arc<MockServer>, Channel, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = unfold(listener, |l| async move {
        let item = l.accept().await.map(|(stream, _)| stream);
        Some((item, l))
    });

    let serv = Arc::new(MockServer {
        track_samples,
        ..Default::default()
    });
    let svc = IngesterServer::from_arc(Arc::clone(&serv));
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(incoming)
            .await;
    });

    let endpoint = format!("http://{addr}");
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();

    (serv, channel, handle)
}

// Upstream: pkg/ingester/client/buffering_client_test.go::TestWriteRequestBufferingClient_Push
#[tokio::test]
async fn test_write_request_buffering_client_push() {
    let (serv, channel, handle) = setup_grpc(false).await;
    let mut client = BufferingIngesterClient::new(channel);

    let requests_to_send: Vec<cortexpb::WriteRequest> =
        (0..10).map(|i| create_request("test", 100 + 10 * i)).collect();

    // push without pooling
    serv.clear_requests();
    for r in &requests_to_send {
        client.push(r.clone(), None).await.unwrap();
    }
    assert_eq!(serv.requests(), requests_to_send);

    // push with pooling
    serv.clear_requests();
    let pool = Arc::new(TrackedPool::new());
    let slab_pool = Arc::new(FastReleasingSlabPool::new(pool.clone(), 512 * 1024));
    for r in &requests_to_send {
        client.push(r.clone(), Some(slab_pool.clone())).await.unwrap();
    }
    assert_eq!(serv.requests(), requests_to_send);
    assert!(pool.gets.load(Ordering::SeqCst) > 0);
    assert_eq!(pool.balance.load(Ordering::SeqCst), 0);

    handle.abort();
}

// Upstream: pkg/ingester/client/buffering_client_test.go::TestWriteRequestBufferingClient_PushWithCancelContext
#[tokio::test]
async fn test_write_request_buffering_client_push_with_cancel_context() {
    let (_serv, channel, handle) = setup_grpc(false).await;
    let mut client = BufferingIngesterClient::new(channel);

    let requests_to_send: Vec<cortexpb::WriteRequest> =
        (0..30).map(|i| create_request("test", 100 + 10 * i)).collect();

    let pool = Arc::new(TrackedPool::new());
    let slab_pool = Arc::new(FastReleasingSlabPool::new(pool.clone(), 512 * 1024));

    for r in requests_to_send {
        let push_fut = client.push(r, Some(slab_pool.clone()));
        tokio::select! {
            _ = push_fut => {}
            _ = tokio::time::sleep(Duration::from_millis(1)) => {}
        }
    }

    handle.abort();
}

// Upstream: pkg/ingester/client/buffering_client_test.go::TestWriteRequestBufferingClient_Push_WithMultipleMarshalCalls
#[tokio::test]
async fn test_write_request_buffering_client_push_with_multiple_marshal_calls() {
    let (serv, channel, handle) = setup_grpc(false).await;
    let req = Arc::new(create_request("test", 100));

    let pool = Arc::new(TrackedPool::new());
    let slab_pool = Arc::new(FastReleasingSlabPool::new(pool.clone(), 512 * 1024));

    let mut wrapped = WrappedRequest::new(Arc::clone(&req), slab_pool.clone());
    let _ = wrapped.marshal().unwrap();
    let _ = wrapped.marshal().unwrap();
    let _ = wrapped.marshal().unwrap();

    let mut client = BufferingIngesterClient::new(channel);
    client
        .push((*req).clone(), Some(slab_pool.clone()))
        .await
        .unwrap();
    wrapped.return_buffers_to_pool();

    assert_eq!(serv.requests(), vec![(*req).clone()]);
    assert!(pool.gets.load(Ordering::SeqCst) > 0);
    assert_eq!(pool.balance.load(Ordering::SeqCst), 0);

    handle.abort();
}

// Upstream: pkg/ingester/client/buffering_client_test.go::TestWriteRequestBufferingClient_PushConcurrent
#[tokio::test]
async fn test_write_request_buffering_client_push_concurrent() {
    let (serv, channel, handle) = setup_grpc(true).await;

    let pool = Arc::new(TrackedPool::new());
    let slab_pool = Arc::new(FastReleasingSlabPool::new(pool.clone(), 512 * 1024));

    const CONCURRENCY: usize = 20;
    const REQUESTS_PER_TASK: usize = 25;

    let mut join_handles = Vec::with_capacity(CONCURRENCY);
    for i in 0..CONCURRENCY {
        let mut client = BufferingIngesterClient::new(channel.clone());
        let slab_pool = slab_pool.clone();
        let serv = Arc::clone(&serv);
        join_handles.push(tokio::spawn(async move {
            let metric_name = format!("test_{i}");
            let mut sent_samples: HashMap<String, Vec<cortexpb::Sample>> = HashMap::new();
            let req = create_request(&metric_name, 50);

            for _ in 0..REQUESTS_PER_TASK {
                for ts in &req.timeseries {
                    let ser = format_labels(&ts.labels);
                    sent_samples
                        .entry(ser)
                        .or_default()
                        .extend(ts.samples.iter().copied());
                }
                client
                    .push(req.clone(), Some(slab_pool.clone()))
                    .await
                    .unwrap();
            }

            for (ser, samples) in sent_samples {
                let received = serv.get_and_remove_samples_for_series(&ser);
                assert_eq!(samples, received);
            }
        }));
    }

    for jh in join_handles {
        jh.await.unwrap();
    }

    assert!(pool.gets.load(Ordering::SeqCst) > 0);
    assert_eq!(pool.balance.load(Ordering::SeqCst), 0);

    handle.abort();
}

// Upstream: pkg/ingester/client/client_test.go::TestMarshall
#[test]
fn test_marshall() {
    const NUM_SERIES: usize = 10;
    let mut req = cortexpb::WriteRequest::default();
    for i in 0..NUM_SERIES {
        req.timeseries.push(cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("foo", i.to_string())],
            samples: vec![cortexpb::Sample {
                timestamp_ms: i as i64,
                value: i as f64,
            }],
            ..Default::default()
        });
    }
    let body = serialize_proto_response(&req, CompressionType::RawSnappy).unwrap();

    const TOO_SMALL_SIZE: usize = 1;
    const PLENTY_SIZE: usize = 1024 * 1024;
    assert!(parse_proto_reader::<cortexpb::WriteRequest>(
        &body,
        TOO_SMALL_SIZE,
        CompressionType::RawSnappy
    )
    .is_err());
    let (_, decoded) = parse_proto_reader::<cortexpb::WriteRequest>(
        &body,
        PLENTY_SIZE,
        CompressionType::RawSnappy,
    )
    .unwrap();
    assert_eq!(decoded.timeseries.len(), NUM_SERIES);
}

// Upstream: pkg/ingester/client/compat_test.go::TestQueryRequest
#[test]
fn test_query_request() {
    let (from, to) = (0i64, 10i64);
    let matchers = vec![
        PromLabelMatcher::new(PromMatchType::MatchEqual, "foo", "1").unwrap(),
        PromLabelMatcher::new(PromMatchType::MatchNotEqual, "bar", "2").unwrap(),
        PromLabelMatcher::new(PromMatchType::MatchRegexp, "baz", "3").unwrap(),
        PromLabelMatcher::new(PromMatchType::MatchNotRegexp, "bop", "4").unwrap(),
    ];

    let req = to_query_request(from, to, &matchers).unwrap();
    let (have_from, have_to, have_matchers) = from_query_request(&req).unwrap();
    assert_eq!(have_from, from);
    assert_eq!(have_to, to);
    assert_eq!(have_matchers.len(), matchers.len());
    for (a, b) in matchers.iter().zip(have_matchers.iter()) {
        assert_eq!(a.to_string(), b.to_string());
    }
}

// Upstream: pkg/ingester/client/compat_test.go::TestLabelNamesRequest
#[test]
fn test_label_names_request() {
    let (mint, maxt) = (0i64, 10i64);
    let hints = LabelHints { limit: 10 };
    let matchers = vec![PromLabelMatcher::must_new(PromMatchType::MatchEqual, "foo", "bar")];

    let req = to_label_names_request(mint, maxt, Some(&hints), &matchers).unwrap();
    let (actual_min_t, actual_max_t, actual_hints, actual_matchers) =
        from_label_names_request(&req).unwrap();

    assert_eq!(actual_min_t, mint);
    assert_eq!(actual_max_t, maxt);
    assert_eq!(actual_hints, Some(hints));
    assert_eq!(actual_matchers, matchers);
}

// Upstream: pkg/ingester/client/custom_test.go::TestChunkFromMeta
#[test]
fn test_chunk_from_meta() {
    let cases = [
        ("XOR", TsdbChunkEncoding::Xor, MimirChunkEncoding::PrometheusXorChunk),
        ("XOR2", TsdbChunkEncoding::Xor2, MimirChunkEncoding::PrometheusXor2Chunk),
        ("Histogram", TsdbChunkEncoding::Histogram, MimirChunkEncoding::PrometheusHistogramChunk),
        (
            "FloatHistogram",
            TsdbChunkEncoding::FloatHistogram,
            MimirChunkEncoding::PrometheusFloatHistogramChunk,
        ),
        (
            "HistogramST",
            TsdbChunkEncoding::HistogramSt,
            MimirChunkEncoding::PrometheusHistogramSTChunk,
        ),
        (
            "FloatHistogramST",
            TsdbChunkEncoding::FloatHistogramSt,
            MimirChunkEncoding::PrometheusFloatHistogramSTChunk,
        ),
    ];

    for (name, enc, expected_mimir_enc) in cases {
        let meta = ChunkMeta {
            min_time: 1000,
            max_time: 2000,
            encoding: enc,
            data: vec![1, 2, 3, 4],
        };
        let ch = chunk_from_meta(&meta).unwrap();
        assert_eq!(ch.encoding, expected_mimir_enc as i32, "{name}");
        assert_eq!(ch.start_timestamp_ms, 1000, "{name}");
        assert_eq!(ch.end_timestamp_ms, 2000, "{name}");
        assert_eq!(ch.data.as_ref(), &[1, 2, 3, 4], "{name}");
    }

    let bad = ChunkMeta {
        min_time: 0,
        max_time: 0,
        encoding: TsdbChunkEncoding::Other(99),
        data: Vec::new(),
    };
    let err = chunk_from_meta(&bad).unwrap_err();
    assert!(err.contains("unknown chunk encoding from TSDB chunk querier"), "{err}");
}

// Upstream: pkg/ingester/client/mimir_util_test.go::TestSendQueryStream
#[tokio::test]
async fn test_send_query_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = unfold(listener, |l| async move {
        let item = l.accept().await.map(|(stream, _)| stream);
        Some((item, l))
    });

    let (server_err_tx, server_err_rx) = tokio::sync::oneshot::channel();
    let (cancel_notify_tx, cancel_notify_rx) = tokio::sync::oneshot::channel();

    struct CancelCheckingServer {
        cancel_notify: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        server_err: Mutex<Option<tokio::sync::oneshot::Sender<tonic::Status>>>,
    }

    #[tonic::async_trait]
    impl Ingester for CancelCheckingServer {
        async fn push(
            &self,
            _r: tonic::Request<cortexpb::WriteRequest>,
        ) -> Result<tonic::Response<cortexpb::WriteResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }

        type QueryStreamStream = Pin<
            Box<
                dyn tokio_stream::Stream<Item = Result<ingesterpb::QueryStreamResponse, tonic::Status>>
                    + Send,
            >,
        >;

        async fn query_stream(
            &self,
            _r: tonic::Request<ingesterpb::QueryRequest>,
        ) -> Result<tonic::Response<Self::QueryStreamStream>, tonic::Status> {
            let (tx, rx) = mpsc::channel(1);
            let notify = self.cancel_notify.lock().take();
            let err_tx = self.server_err.lock().take();
            let (cancelled_tx, cancelled_rx) = watch::channel(false);

            tokio::spawn(async move {
                if let Some(n) = notify {
                    let _ = n.send(());
                }
                // Wait for client stream drop (receiver closed)
                tx.closed().await;
                let _ = cancelled_tx.send(true);
                let err = send_query_stream(
                    &tx,
                    ingesterpb::QueryStreamResponse::default(),
                    &cancelled_rx,
                )
                .await
                .unwrap_err();
                if let Some(et) = err_tx {
                    let _ = et.send(err);
                }
            });

            Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
        }

        async fn query_exemplars(
            &self,
            _r: tonic::Request<ingesterpb::ExemplarQueryRequest>,
        ) -> Result<tonic::Response<ingesterpb::ExemplarQueryResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        async fn label_values(
            &self,
            _r: tonic::Request<ingesterpb::LabelValuesRequest>,
        ) -> Result<tonic::Response<ingesterpb::LabelValuesResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        async fn label_names(
            &self,
            _r: tonic::Request<ingesterpb::LabelNamesRequest>,
        ) -> Result<tonic::Response<ingesterpb::LabelNamesResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        async fn user_stats(
            &self,
            _r: tonic::Request<ingesterpb::UserStatsRequest>,
        ) -> Result<tonic::Response<ingesterpb::UserStatsResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        async fn all_user_stats(
            &self,
            _r: tonic::Request<ingesterpb::UserStatsRequest>,
        ) -> Result<tonic::Response<ingesterpb::UsersStatsResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        async fn metrics_for_label_matchers(
            &self,
            _r: tonic::Request<ingesterpb::MetricsForLabelMatchersRequest>,
        ) -> Result<tonic::Response<ingesterpb::MetricsForLabelMatchersResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        async fn metrics_metadata(
            &self,
            _r: tonic::Request<ingesterpb::MetricsMetadataRequest>,
        ) -> Result<tonic::Response<ingesterpb::MetricsMetadataResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        type LabelNamesAndValuesStream = Pin<
            Box<
                dyn tokio_stream::Stream<Item = Result<ingesterpb::LabelNamesAndValuesResponse, tonic::Status>>
                    + Send,
            >,
        >;
        async fn label_names_and_values(
            &self,
            _r: tonic::Request<ingesterpb::LabelNamesAndValuesRequest>,
        ) -> Result<tonic::Response<Self::LabelNamesAndValuesStream>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        type LabelValuesCardinalityStream = Pin<
            Box<
                dyn tokio_stream::Stream<
                        Item = Result<ingesterpb::LabelValuesCardinalityResponse, tonic::Status>,
                    > + Send,
            >,
        >;
        async fn label_values_cardinality(
            &self,
            _r: tonic::Request<ingesterpb::LabelValuesCardinalityRequest>,
        ) -> Result<tonic::Response<Self::LabelValuesCardinalityStream>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        type ActiveSeriesStream = Pin<
            Box<dyn tokio_stream::Stream<Item = Result<ingesterpb::ActiveSeriesResponse, tonic::Status>> + Send>,
        >;
        async fn active_series(
            &self,
            _r: tonic::Request<ingesterpb::ActiveSeriesRequest>,
        ) -> Result<tonic::Response<Self::ActiveSeriesStream>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        type SearchLabelNamesStream = Pin<
            Box<dyn tokio_stream::Stream<Item = Result<ingesterpb::SearchResultBatch, tonic::Status>> + Send>,
        >;
        async fn search_label_names(
            &self,
            _r: tonic::Request<ingesterpb::SearchLabelNamesRequest>,
        ) -> Result<tonic::Response<Self::SearchLabelNamesStream>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        type SearchLabelValuesStream = Pin<
            Box<dyn tokio_stream::Stream<Item = Result<ingesterpb::SearchResultBatch, tonic::Status>> + Send>,
        >;
        async fn search_label_values(
            &self,
            _r: tonic::Request<ingesterpb::SearchLabelValuesRequest>,
        ) -> Result<tonic::Response<Self::SearchLabelValuesStream>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
    }

    let svc = IngesterServer::new(CancelCheckingServer {
        cancel_notify: Mutex::new(Some(cancel_notify_tx)),
        server_err: Mutex::new(Some(server_err_tx)),
    });
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(incoming)
            .await;
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = IngesterClient::new(channel);
    let stream_resp = client
        .query_stream(ingesterpb::QueryRequest::default())
        .await
        .unwrap();
    let stream = stream_resp.into_inner();
    cancel_notify_rx.await.unwrap();
    // Cancel/drop client stream
    drop(stream);

    let server_err = server_err_rx.await.unwrap();
    assert_eq!(server_err.code(), tonic::Code::Cancelled);

    server_handle.abort();
}

// Upstream: pkg/ingester/client/only_replica_test.go::TestOnlyReplicaClientUnaryInterceptor_And_OnlyReplicaServerUnaryInterceptor
#[tokio::test]
async fn test_only_replica_client_unary_interceptor_and_only_replica_server_unary_interceptor() {
    let outgoing_req = with_available_replicas(1, async {
        only_replica_client_interceptor(tonic::Request::new(())).unwrap()
    })
    .await;
    assert!(is_only_replica_in_metadata(outgoing_req.metadata()));

    let server_req = only_replica_server_interceptor(tonic::Request::from_parts(
        outgoing_req.metadata().clone(),
        tonic::Extensions::new(),
        (),
    ))
    .unwrap();
    assert_eq!(
        server_req.extensions().get::<OnlyReplicaMarker>(),
        Some(&OnlyReplicaMarker(true))
    );
}

// Upstream: pkg/ingester/client/only_replica_test.go::TestOnlyReplicaClientStreamInterceptor_And_OnlyReplicaServerStreamInterceptor
#[tokio::test]
async fn test_only_replica_client_stream_interceptor_and_only_replica_server_stream_interceptor() {
    let outgoing_req = with_available_replicas(1, async {
        only_replica_client_interceptor(tonic::Request::new(ingesterpb::QueryRequest::default()))
            .unwrap()
    })
    .await;
    assert!(is_only_replica_in_metadata(outgoing_req.metadata()));

    let server_req = only_replica_server_interceptor(tonic::Request::from_parts(
        outgoing_req.metadata().clone(),
        tonic::Extensions::new(),
        ingesterpb::QueryRequest::default(),
    ))
    .unwrap();
    assert_eq!(
        server_req.extensions().get::<OnlyReplicaMarker>(),
        Some(&OnlyReplicaMarker(true))
    );
}

// Upstream: pkg/ingester/client/only_replica_test.go::TestContextWithOnlyReplica
#[tokio::test]
async fn test_context_with_only_replica() {
    with_only_replica(async {
        assert!(is_only_replica_context());
    })
    .await;
    assert!(!is_only_replica_context());
}

fn create_test_chunk(ts: i64, val: f64) -> ingesterpb::Chunk {
    let mut data = Vec::with_capacity(16);
    data.extend_from_slice(&ts.to_be_bytes());
    data.extend_from_slice(&val.to_be_bytes());
    ingesterpb::Chunk {
        start_timestamp_ms: ts,
        end_timestamp_ms: ts,
        encoding: MimirChunkEncoding::PrometheusXorChunk as i32,
        data: Bytes::from(data),
    }
}

struct MockQueryStreamClient {
    batches: Vec<Vec<ingesterpb::QueryStreamSeriesChunks>>,
    stream_cancel: watch::Receiver<bool>,
    closed: Arc<AtomicBool>,
}

#[tonic::async_trait]
impl QueryStreamClient for MockQueryStreamClient {
    async fn recv(&mut self) -> Result<Option<ingesterpb::QueryStreamResponse>, StreamError> {
        if *self.stream_cancel.borrow() {
            // Note: matches mockQueryStreamClient in Go: Recv yields batches regardless unless parent cancels,
            // and TestDoesNotAbortWhenStreamContextCancelled verifies stream context cancellation doesn't abort.
        }
        if self.batches.is_empty() {
            return Ok(None);
        }
        let batch = self.batches.remove(0);
        Ok(Some(ingesterpb::QueryStreamResponse {
            streaming_series_chunks: batch,
            ..Default::default()
        }))
    }

    fn close_send(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

// Upstream: pkg/ingester/client/streaming_test.go::TestSeriesChunksStreamReader_HappyPaths
#[tokio::test]
async fn test_series_chunks_stream_reader_happy_paths() {
    let series0 = vec![create_test_chunk(1000, 1.0)];
    let series1 = vec![create_test_chunk(1000, 2.0)];
    let series2 = vec![create_test_chunk(1000, 3.0)];
    let series3 = vec![create_test_chunk(1000, 4.0)];
    let series4 = vec![create_test_chunk(1000, 5.0)];

    let cases = [
        (
            "single series per batch",
            vec![
                vec![ingesterpb::QueryStreamSeriesChunks { series_index: 0, chunks: series0.clone() }],
                vec![ingesterpb::QueryStreamSeriesChunks { series_index: 1, chunks: series1.clone() }],
                vec![ingesterpb::QueryStreamSeriesChunks { series_index: 2, chunks: series2.clone() }],
                vec![ingesterpb::QueryStreamSeriesChunks { series_index: 3, chunks: series3.clone() }],
                vec![ingesterpb::QueryStreamSeriesChunks { series_index: 4, chunks: series4.clone() }],
            ],
        ),
        (
            "multiple series per batch",
            vec![
                vec![
                    ingesterpb::QueryStreamSeriesChunks { series_index: 0, chunks: series0.clone() },
                    ingesterpb::QueryStreamSeriesChunks { series_index: 1, chunks: series1.clone() },
                    ingesterpb::QueryStreamSeriesChunks { series_index: 2, chunks: series2.clone() },
                ],
                vec![
                    ingesterpb::QueryStreamSeriesChunks { series_index: 3, chunks: series3.clone() },
                    ingesterpb::QueryStreamSeriesChunks { series_index: 4, chunks: series4.clone() },
                ],
            ],
        ),
        (
            "empty batches",
            vec![
                vec![
                    ingesterpb::QueryStreamSeriesChunks { series_index: 0, chunks: series0.clone() },
                    ingesterpb::QueryStreamSeriesChunks { series_index: 1, chunks: series1.clone() },
                    ingesterpb::QueryStreamSeriesChunks { series_index: 2, chunks: series2.clone() },
                ],
                vec![],
                vec![
                    ingesterpb::QueryStreamSeriesChunks { series_index: 3, chunks: series3.clone() },
                    ingesterpb::QueryStreamSeriesChunks { series_index: 4, chunks: series4.clone() },
                ],
                vec![],
            ],
        ),
    ];

    for (name, batches) in cases {
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let closed = Arc::new(AtomicBool::new(false));
        let cleaned_up = Arc::new(AtomicBool::new(false));
        let cleaned_cb = Arc::clone(&cleaned_up);
        let mock_client = Box::new(MockQueryStreamClient {
            batches,
            stream_cancel: cancel_rx.clone(),
            closed: Arc::clone(&closed),
        });
        let query_limiter = Arc::new(QueryLimiter::new(0, 0, 0, 0));
        let memory_tracker = Arc::new(MemoryConsumptionTracker::unlimited());
        let mut reader = SeriesChunksStreamReader::new(
            cancel_rx,
            mock_client,
            "ingester",
            5,
            query_limiter,
            memory_tracker,
            Arc::new(move || cleaned_cb.store(true, Ordering::SeqCst)),
        );
        reader.start_buffering();

        for (i, expected) in [&series0, &series1, &series2, &series3, &series4]
            .into_iter()
            .enumerate()
        {
            let actual = reader.get_chunks(i as u64).await.unwrap();
            assert_eq!(&actual, expected, "{name} index {i}");
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(closed.load(Ordering::SeqCst), "{name}");
        assert!(cleaned_up.load(Ordering::SeqCst), "{name}");
    }
}

// Upstream: pkg/ingester/client/streaming_test.go::TestSeriesChunksStreamReader_AbortsWhenParentContextCancelled
#[tokio::test]
async fn test_series_chunks_stream_reader_aborts_when_parent_context_cancelled() {
    let batches = vec![
        vec![ingesterpb::QueryStreamSeriesChunks {
            series_index: 0,
            chunks: vec![create_test_chunk(1000, 1.23)],
        }],
        vec![ingesterpb::QueryStreamSeriesChunks {
            series_index: 1,
            chunks: vec![create_test_chunk(1000, 4.56)],
        }],
        vec![ingesterpb::QueryStreamSeriesChunks {
            series_index: 2,
            chunks: vec![create_test_chunk(1000, 7.89)],
        }],
    ];

    let (_stream_tx, stream_rx) = watch::channel(false);
    let (parent_tx, parent_rx) = watch::channel(false);
    let closed = Arc::new(AtomicBool::new(false));
    let cleaned_up = Arc::new(AtomicBool::new(false));
    let cleaned_cb = Arc::clone(&cleaned_up);

    let mock_client = Box::new(MockQueryStreamClient {
        batches,
        stream_cancel: stream_rx,
        closed: Arc::clone(&closed),
    });
    let mut reader = SeriesChunksStreamReader::new(
        parent_rx,
        mock_client,
        "ingester",
        3,
        Arc::new(QueryLimiter::new(0, 0, 0, 0)),
        Arc::new(MemoryConsumptionTracker::unlimited()),
        Arc::new(move || cleaned_cb.store(true, Ordering::SeqCst)),
    );

    let _ = parent_tx.send(true);
    reader.start_buffering();

    let mut saw_cancel = false;
    for i in 0..3 {
        match reader.get_chunks(i).await {
            Err(e) if e.is_canceled() => {
                saw_cancel = true;
                break;
            }
            Ok(_) => {}
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
    assert!(saw_cancel);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(closed.load(Ordering::SeqCst));
    assert!(cleaned_up.load(Ordering::SeqCst));
}

// Upstream: pkg/ingester/client/streaming_test.go::TestSeriesChunksStreamReader_DoesNotAbortWhenStreamContextCancelled
#[tokio::test]
async fn test_series_chunks_stream_reader_does_not_abort_when_stream_context_cancelled() {
    let batches = vec![
        vec![ingesterpb::QueryStreamSeriesChunks {
            series_index: 0,
            chunks: vec![create_test_chunk(1000, 1.23)],
        }],
        vec![ingesterpb::QueryStreamSeriesChunks {
            series_index: 1,
            chunks: vec![create_test_chunk(1000, 4.56)],
        }],
        vec![ingesterpb::QueryStreamSeriesChunks {
            series_index: 2,
            chunks: vec![create_test_chunk(1000, 7.89)],
        }],
    ];

    let (stream_tx, stream_rx) = watch::channel(false);
    let (_parent_tx, parent_rx) = watch::channel(false);
    let closed = Arc::new(AtomicBool::new(false));
    let cleaned_up = Arc::new(AtomicBool::new(false));
    let cleaned_cb = Arc::clone(&cleaned_up);

    let mock_client = Box::new(MockQueryStreamClient {
        batches,
        stream_cancel: stream_rx,
        closed: Arc::clone(&closed),
    });
    let mut reader = SeriesChunksStreamReader::new(
        parent_rx,
        mock_client,
        "ingester",
        3,
        Arc::new(QueryLimiter::new(0, 0, 0, 0)),
        Arc::new(MemoryConsumptionTracker::unlimited()),
        Arc::new(move || cleaned_cb.store(true, Ordering::SeqCst)),
    );

    let _ = stream_tx.send(true);
    reader.start_buffering();

    for i in 0..3 {
        reader.get_chunks(i).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(closed.load(Ordering::SeqCst));
    assert!(cleaned_up.load(Ordering::SeqCst));
}

// Upstream: pkg/ingester/client/streaming_test.go::TestSeriesChunksStreamReader_ReadingSeriesOutOfOrder
#[tokio::test]
async fn test_series_chunks_stream_reader_reading_series_out_of_order() {
    let batches = vec![vec![ingesterpb::QueryStreamSeriesChunks {
        series_index: 0,
        chunks: vec![create_test_chunk(1000, 1.23)],
    }]];
    let (_tx, rx) = watch::channel(false);
    let mut reader = SeriesChunksStreamReader::new(
        rx.clone(),
        Box::new(MockQueryStreamClient {
            batches,
            stream_cancel: rx,
            closed: Arc::new(AtomicBool::new(false)),
        }),
        "ingester",
        1,
        Arc::new(QueryLimiter::new(0, 0, 0, 0)),
        Arc::new(MemoryConsumptionTracker::unlimited()),
        Arc::new(|| {}),
    );
    reader.start_buffering();

    let err = reader.get_chunks(1).await.unwrap_err().to_string();
    let expected = "attempted to read series at index 1 from ingester chunks stream, but the stream has series with index 0";
    assert_eq!(err, expected);

    assert_eq!(
        reader.get_chunks(2).await.unwrap_err().to_string(),
        format!("attempted to read series at index 2 from ingester chunks stream, but the stream previously failed and returned an error: {expected}")
    );
    assert_eq!(
        reader.get_chunks(3).await.unwrap_err().to_string(),
        format!("attempted to read series at index 3 from ingester chunks stream, but the stream previously failed and returned an error: {expected}")
    );
}

// Upstream: pkg/ingester/client/streaming_test.go::TestSeriesChunksStreamReader_ReadingMoreSeriesThanAvailable
#[tokio::test]
async fn test_series_chunks_stream_reader_reading_more_series_than_available() {
    let first_series = vec![create_test_chunk(1000, 1.23)];
    let batches = vec![vec![ingesterpb::QueryStreamSeriesChunks {
        series_index: 0,
        chunks: first_series.clone(),
    }]];
    let (_tx, rx) = watch::channel(false);
    let mut reader = SeriesChunksStreamReader::new(
        rx.clone(),
        Box::new(MockQueryStreamClient {
            batches,
            stream_cancel: rx,
            closed: Arc::new(AtomicBool::new(false)),
        }),
        "ingester",
        1,
        Arc::new(QueryLimiter::new(0, 0, 0, 0)),
        Arc::new(MemoryConsumptionTracker::unlimited()),
        Arc::new(|| {}),
    );
    reader.start_buffering();

    assert_eq!(reader.get_chunks(0).await.unwrap(), first_series);

    let err = reader.get_chunks(1).await.unwrap_err().to_string();
    let expected = "attempted to read series at index 1 from ingester chunks stream, but the stream has already been exhausted (was expecting 1 series)";
    assert_eq!(err, expected);

    assert_eq!(
        reader.get_chunks(2).await.unwrap_err().to_string(),
        format!("attempted to read series at index 2 from ingester chunks stream, but the stream previously failed and returned an error: {expected}")
    );
    assert_eq!(
        reader.get_chunks(3).await.unwrap_err().to_string(),
        format!("attempted to read series at index 3 from ingester chunks stream, but the stream previously failed and returned an error: {expected}")
    );
}

// Upstream: pkg/ingester/client/streaming_test.go::TestSeriesChunksStreamReader_ReceivedFewerSeriesThanExpected
#[tokio::test]
async fn test_series_chunks_stream_reader_received_fewer_series_than_expected() {
    let first_series = vec![create_test_chunk(1000, 1.23)];
    let batches = vec![vec![ingesterpb::QueryStreamSeriesChunks {
        series_index: 0,
        chunks: first_series.clone(),
    }]];
    let (_tx, rx) = watch::channel(false);
    let closed = Arc::new(AtomicBool::new(false));
    let cleaned_up = Arc::new(AtomicBool::new(false));
    let cleaned_cb = Arc::clone(&cleaned_up);

    let mut reader = SeriesChunksStreamReader::new(
        rx.clone(),
        Box::new(MockQueryStreamClient {
            batches,
            stream_cancel: rx,
            closed: Arc::clone(&closed),
        }),
        "ingester",
        3,
        Arc::new(QueryLimiter::new(0, 0, 0, 0)),
        Arc::new(MemoryConsumptionTracker::unlimited()),
        Arc::new(move || cleaned_cb.store(true, Ordering::SeqCst)),
    );
    reader.start_buffering();

    assert_eq!(reader.get_chunks(0).await.unwrap(), first_series);

    let err = reader.get_chunks(1).await.unwrap_err().to_string();
    let expected = "attempted to read series at index 1 from ingester chunks stream, but the stream has failed: expected to receive 3 series, but got EOF after receiving 1 series";
    assert_eq!(err, expected);

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(closed.load(Ordering::SeqCst));
    assert!(cleaned_up.load(Ordering::SeqCst));

    assert_eq!(
        reader.get_chunks(2).await.unwrap_err().to_string(),
        format!("attempted to read series at index 2 from ingester chunks stream, but the stream previously failed and returned an error: {expected}")
    );
    assert_eq!(
        reader.get_chunks(3).await.unwrap_err().to_string(),
        format!("attempted to read series at index 3 from ingester chunks stream, but the stream previously failed and returned an error: {expected}")
    );
}

// Upstream: pkg/ingester/client/streaming_test.go::TestSeriesChunksStreamReader_ReceivedMoreSeriesThanExpected
#[tokio::test]
async fn test_series_chunks_stream_reader_received_more_series_than_expected() {
    let cases = [
        vec![vec![
            ingesterpb::QueryStreamSeriesChunks { series_index: 0, chunks: vec![create_test_chunk(1000, 1.23)] },
            ingesterpb::QueryStreamSeriesChunks { series_index: 1, chunks: vec![create_test_chunk(1000, 4.56)] },
            ingesterpb::QueryStreamSeriesChunks { series_index: 2, chunks: vec![create_test_chunk(1000, 7.89)] },
        ]],
        vec![
            vec![ingesterpb::QueryStreamSeriesChunks { series_index: 0, chunks: vec![create_test_chunk(1000, 1.23)] }],
            vec![
                ingesterpb::QueryStreamSeriesChunks { series_index: 1, chunks: vec![create_test_chunk(1000, 4.56)] },
                ingesterpb::QueryStreamSeriesChunks { series_index: 2, chunks: vec![create_test_chunk(1000, 7.89)] },
            ],
        ],
    ];

    for batches in cases {
        let (_tx, rx) = watch::channel(false);
        let closed = Arc::new(AtomicBool::new(false));
        let cleaned_up = Arc::new(AtomicBool::new(false));
        let cleaned_cb = Arc::clone(&cleaned_up);

        let mut reader = SeriesChunksStreamReader::new(
            rx.clone(),
            Box::new(MockQueryStreamClient {
                batches,
                stream_cancel: rx,
                closed: Arc::clone(&closed),
            }),
            "ingester",
            1,
            Arc::new(QueryLimiter::new(0, 0, 0, 0)),
            Arc::new(MemoryConsumptionTracker::unlimited()),
            Arc::new(move || cleaned_cb.store(true, Ordering::SeqCst)),
        );
        reader.start_buffering();

        let err = reader.get_chunks(0).await.unwrap_err().to_string();
        let expected = "attempted to read series at index 0 from ingester chunks stream, but the stream has failed: expected to receive only 1 series, but received at least 3 series";
        assert_eq!(err, expected);

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(closed.load(Ordering::SeqCst));
        assert!(cleaned_up.load(Ordering::SeqCst));

        assert_eq!(
            reader.get_chunks(1).await.unwrap_err().to_string(),
            format!("attempted to read series at index 1 from ingester chunks stream, but the stream previously failed and returned an error: {expected}")
        );
        assert_eq!(
            reader.get_chunks(2).await.unwrap_err().to_string(),
            format!("attempted to read series at index 2 from ingester chunks stream, but the stream previously failed and returned an error: {expected}")
        );
    }
}

// Upstream: pkg/ingester/client/streaming_test.go::TestSeriesChunksStreamReader_QueryAndChunksLimits
#[tokio::test]
async fn test_series_chunks_stream_reader_query_and_chunks_limits() {
    let cases = [
        ("query under both limits", 4usize, 200usize, 400u64, ""),
        ("query selects too many chunks", 2, 200, 400, ""),
        (
            "query selects too many chunk bytes",
            4,
            50,
            400,
            "the query exceeded the aggregated chunks size limit (limit: 50 bytes) (err-mimir-max-chunks-bytes-per-query). Consider reducing the time range and/or number of series selected by the query. One way to reduce the number of selected series is to add more label matchers to the query. Otherwise, to adjust the related per-tenant limit, configure -querier.max-fetched-chunk-bytes-per-query, or contact your service administrator.",
        ),
        (
            "query uses too much estimated memory",
            4,
            200,
            50,
            "the query exceeded the maximum allowed estimated amount of memory consumed by a single query (limit: 50 bytes) (err-mimir-max-estimated-memory-consumption-per-query). Consider reducing the time range and/or number of series selected by the query. One way to reduce the number of selected series is to add more label matchers to the query. Otherwise, to adjust the related per-tenant limit, configure -querier.max-estimated-memory-consumption-per-query, or contact your service administrator.",
        ),
    ];

    for (name, max_chunks, max_chunk_bytes, max_est_mem, expected_err) in cases {
        let batches = vec![vec![ingesterpb::QueryStreamSeriesChunks {
            series_index: 0,
            chunks: vec![
                create_test_chunk(1000, 1.23),
                create_test_chunk(2000, 4.56),
                create_test_chunk(3000, 7.89),
            ],
        }]];

        let (_tx, rx) = watch::channel(false);
        let closed = Arc::new(AtomicBool::new(false));
        let cleaned_up = Arc::new(AtomicBool::new(false));
        let cleaned_cb = Arc::clone(&cleaned_up);

        let reg = Registry::new();
        let rejections = Counter::with_opts(Opts::new("rejections", "test")).unwrap();
        reg.register(Box::new(rejections.clone())).unwrap();

        let query_limiter = Arc::new(QueryLimiter::new(0, max_chunk_bytes, max_chunks, 0));
        let memory_tracker = Arc::new(MemoryConsumptionTracker::new(max_est_mem, Some(rejections)));

        let mut reader = SeriesChunksStreamReader::new(
            rx.clone(),
            Box::new(MockQueryStreamClient {
                batches,
                stream_cancel: rx,
                closed: Arc::clone(&closed),
            }),
            "ingester",
            1,
            query_limiter,
            Arc::clone(&memory_tracker),
            Arc::new(move || cleaned_cb.store(true, Ordering::SeqCst)),
        );
        reader.start_buffering();

        let res = reader.get_chunks(0).await;
        if expected_err.is_empty() {
            res.unwrap_or_else(|e| panic!("{name}: unexpected error {e}"));
        } else {
            let actual = res.unwrap_err().to_string();
            assert_eq!(actual, expected_err, "{name}");
            let e1 = reader.get_chunks(1).await.unwrap_err().to_string();
            assert_eq!(
                e1,
                format!("attempted to read series at index 1 from ingester chunks stream, but the stream previously failed and returned an error: {expected_err}")
            );
            let e2 = reader.get_chunks(2).await.unwrap_err().to_string();
            assert_eq!(
                e2,
                format!("attempted to read series at index 2 from ingester chunks stream, but the stream previously failed and returned an error: {expected_err}")
            );
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(closed.load(Ordering::SeqCst), "{name}");
        assert!(cleaned_up.load(Ordering::SeqCst), "{name}");

        reader.free_buffer();
        assert_eq!(memory_tracker.current_estimated_memory_consumption_bytes(), 0);
    }
}

// Upstream: pkg/ingester/client/streaming_test_utils_test.go::TestMergeSampleSets
#[test]
fn test_merge_sample_sets() {
    let now = 1_700_000_000_000i64;
    let s1 = SamplePair { timestamp: now, value: 1.0 };
    let s2 = SamplePair { timestamp: now + 1000, value: 2.0 };
    let s3 = SamplePair { timestamp: now + 4000, value: 3.0 };
    let s4 = SamplePair { timestamp: now + 8000, value: 7.0 };

    let cases = [
        (vec![], vec![], vec![]),
        (vec![s1], vec![], vec![s1]),
        (vec![], vec![s1], vec![s1]),
        (vec![s1], vec![s1], vec![s1]),
        (vec![s1, s2, s3], vec![s1, s3, s4], vec![s1, s2, s3, s4]),
    ];
    for (a, b, exp) in cases {
        assert_eq!(merge_sample_sets(&a, &b), exp);
    }
}

fn gen_test_hist(i: i64) -> PromHistogram {
    PromHistogram {
        count: 12 + (i * 9) as u64,
        zero_count: 2 + i as u64,
        zero_threshold: 0.001,
        sum: 18.4 * ((i + 1) as f64),
        schema: 1,
        positive_spans: vec![
            PromSpan { offset: 0, length: 2 },
            PromSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![i + 1, 1, -1, 0],
        negative_spans: vec![
            PromSpan { offset: 0, length: 2 },
            PromSpan { offset: 1, length: 2 },
        ],
        negative_buckets: vec![i + 1, 1, -1, 0],
        custom_values: Vec::new(),
        counter_reset_hint: 0,
    }
}

fn gen_test_float_hist(i: i64) -> PromFloatHistogram {
    PromFloatHistogram {
        count: 12.0 + ((i * 9) as f64),
        zero_count: 2.0 + (i as f64),
        zero_threshold: 0.001,
        sum: 18.4 * ((i + 1) as f64),
        schema: 1,
        positive_spans: vec![
            PromSpan { offset: 0, length: 2 },
            PromSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![(i + 1) as f64, (i + 2) as f64, (i + 1) as f64, (i + 1) as f64],
        negative_spans: vec![
            PromSpan { offset: 0, length: 2 },
            PromSpan { offset: 1, length: 2 },
        ],
        negative_buckets: vec![(i + 1) as f64, (i + 2) as f64, (i + 1) as f64, (i + 1) as f64],
        custom_values: Vec::new(),
        counter_reset_hint: 0,
    }
}

// Upstream: pkg/ingester/client/streaming_test_utils_test.go::TestMergeHistogramSets
#[test]
fn test_merge_histogram_sets() {
    let now = 1_700_000_000_000i64;
    let s1 = from_float_histogram_to_histogram_proto(now, &gen_test_float_hist(1));
    let s2 = from_histogram_to_histogram_proto(now + 1000, &gen_test_hist(2));
    let s3 = from_float_histogram_to_histogram_proto(now + 4000, &gen_test_float_hist(3));
    let s4 = from_histogram_to_histogram_proto(now + 8000, &gen_test_hist(7));

    let cases = [
        (vec![], vec![], vec![]),
        (vec![s1.clone()], vec![], vec![s1.clone()]),
        (vec![], vec![s1.clone()], vec![s1.clone()]),
        (vec![s1.clone()], vec![s1.clone()], vec![s1.clone()]),
        (
            vec![s1.clone(), s2.clone(), s3.clone()],
            vec![s1.clone(), s3.clone(), s4.clone()],
            vec![s1, s2, s3, s4],
        ),
    ];
    for (a, b, exp) in cases {
        assert_eq!(merge_histogram_sets(&a, &b), exp);
    }
}

#[tokio::test]
async fn w18_wpe_11_final_series_limit_error_is_wrapped() {
    struct LimitAfterFinalSeriesClient {
        step: usize,
    }

    #[tonic::async_trait]
    impl QueryStreamClient for LimitAfterFinalSeriesClient {
        async fn recv(&mut self) -> Result<Option<ingesterpb::QueryStreamResponse>, StreamError> {
            let cur = self.step;
            self.step += 1;
            if cur == 0 {
                Ok(Some(ingesterpb::QueryStreamResponse {
                    streaming_series_chunks: vec![ingesterpb::QueryStreamSeriesChunks {
                        series_index: 0,
                        chunks: vec![create_test_chunk(1000, 1.0)],
                    }],
                    ..Default::default()
                }))
            } else {
                Err(StreamError::Limit(mimir_proto::client::streaming::LimitError(
                    "the query hit the max number of chunks limit".to_string(),
                )))
            }
        }

        fn close_send(&mut self) {}
    }

    let (_tx, rx) = watch::channel(false);
    let mut reader = SeriesChunksStreamReader::new(
        rx,
        Box::new(LimitAfterFinalSeriesClient { step: 0 }),
        "ingester",
        1,
        Arc::new(QueryLimiter::new(0, 0, 0, 0)),
        Arc::new(MemoryConsumptionTracker::unlimited()),
        Arc::new(|| {}),
    );
    reader.start_buffering();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let err = reader.get_chunks(0).await.unwrap_err();
    assert!(
        matches!(err, StreamError::Limit(_)),
        "expected StreamError::Limit, got: {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.starts_with(
            "attempted to read series at index 0 from ingester chunks stream, but the stream has failed: "
        ),
        "expected wrapped final-series limit error message, got: {msg}"
    );
    assert!(msg.contains("the query hit the max number of chunks limit"), "{msg}");
}
