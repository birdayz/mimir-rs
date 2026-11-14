#[path = "upstream_storage_part2_helpers.rs"]
mod helpers;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use helpers::{create_test_kafka_config, FakeKafkaCluster, HookAction};
use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::{CreateTopicsResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use mimir_storage::ingest::{
    create_topics, ingester_partition_id, sasl_mechanism, KafkaMskIamAuth, KafkaOauthbearerAuth,
    KafkaSaslSecretConfig, ResolvedSaslMechanism, ResultPromise, SampledOnlyTracer, SpanRecorder,
    TraceSpanContext, TracedKafkaRecord,
};
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

fn write_secret_to_file<T: Serialize>(dir: &tempfile::TempDir, secret: &T) -> String {
    let path = dir.path().join("secret.json");
    let json = serde_json::to_vec(secret).unwrap();
    std::fs::write(&path, json).unwrap();
    path.to_string_lossy().into_owned()
}

async fn serve_secret_from_socket<T: Serialize>(
    dir: &tempfile::TempDir,
    secret: &T,
) -> String {
    let json = serde_json::to_vec(secret).unwrap();
    let sock_path = dir.path().join("secret.sock");
    let listener = UnixListener::bind(&sock_path).unwrap();

    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let body = json.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.write_all(&body).await;
            });
        }
    });

    sock_path.to_string_lossy().into_owned()
}

// Upstream: pkg/storage/ingest/util_test.go::TestMSKIAMCredentials
#[tokio::test]
async fn test_msk_iam_credentials() {
    let secret = KafkaMskIamAuth {
        access_key: "AKIDEXAMPLE".to_string(),
        secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
        session_token: "AQoDYXdzEJr//some/session/token".to_string(),
        user_agent: String::new(),
    };

    // "file-based"
    {
        let dir = tempfile::tempdir().unwrap();
        let file_path = write_secret_to_file(&dir, &secret);
        let cfg = KafkaSaslSecretConfig::<KafkaMskIamAuth> {
            secret: None,
            file_path,
            http_socket_path: String::new(),
            http_socket_timeout: Duration::from_secs(10),
        };
        let resolved = sasl_mechanism(
            cfg,
            ResolvedSaslMechanism::Callback,
            ResolvedSaslMechanism::Static,
        )
        .unwrap();
        let ResolvedSaslMechanism::Callback(cb) = resolved else {
            panic!("expected callback mechanism");
        };
        let got = cb().await.unwrap();
        assert_eq!(got, secret);
    }

    // "socket-based"
    {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = serve_secret_from_socket(&dir, &secret).await;
        let cfg = KafkaSaslSecretConfig::<KafkaMskIamAuth> {
            secret: None,
            file_path: String::new(),
            http_socket_path: sock_path,
            http_socket_timeout: Duration::from_secs(10),
        };
        let resolved = sasl_mechanism(
            cfg,
            ResolvedSaslMechanism::Callback,
            ResolvedSaslMechanism::Static,
        )
        .unwrap();
        let ResolvedSaslMechanism::Callback(cb) = resolved else {
            panic!("expected callback mechanism");
        };
        let got = cb().await.unwrap();
        assert_eq!(got, secret);
    }
}

// Upstream: pkg/storage/ingest/util_test.go::TestMSKIAMStaticCredentials
#[test]
fn test_msk_iam_static_credentials() {
    let secret = KafkaMskIamAuth {
        access_key: "AKIDEXAMPLE".to_string(),
        secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
        session_token: "AQoDYXdzEJr//some/session/token".to_string(),
        user_agent: String::new(),
    };
    let cfg = KafkaSaslSecretConfig {
        secret: Some(secret.clone()),
        file_path: String::new(),
        http_socket_path: String::new(),
        http_socket_timeout: Duration::from_secs(10),
    };
    let resolved = sasl_mechanism(
        cfg,
        |_| -> KafkaMskIamAuth { panic!("should have not been called") },
        |s| s,
    )
    .unwrap();
    assert_eq!(resolved, secret);
}

// Upstream: pkg/storage/ingest/util_test.go::TestOauthbearerCredentials
#[tokio::test]
async fn test_oauthbearer_credentials() {
    let secret = KafkaOauthbearerAuth {
        token: "some-oauth-token".to_string(),
        zid: "some-zid".to_string(),
        extensions: Default::default(),
    };

    // "file-based"
    {
        let dir = tempfile::tempdir().unwrap();
        let file_path = write_secret_to_file(&dir, &secret);
        let cfg = KafkaSaslSecretConfig::<KafkaOauthbearerAuth> {
            secret: None,
            file_path,
            http_socket_path: String::new(),
            http_socket_timeout: Duration::from_secs(10),
        };
        let resolved = sasl_mechanism(
            cfg,
            ResolvedSaslMechanism::Callback,
            ResolvedSaslMechanism::Static,
        )
        .unwrap();
        let ResolvedSaslMechanism::Callback(cb) = resolved else {
            panic!("expected callback mechanism");
        };
        let got = cb().await.unwrap();
        assert_eq!(got, secret);
    }

    // "socket-based"
    {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = serve_secret_from_socket(&dir, &secret).await;
        let cfg = KafkaSaslSecretConfig::<KafkaOauthbearerAuth> {
            secret: None,
            file_path: String::new(),
            http_socket_path: sock_path,
            http_socket_timeout: Duration::from_secs(10),
        };
        let resolved = sasl_mechanism(
            cfg,
            ResolvedSaslMechanism::Callback,
            ResolvedSaslMechanism::Static,
        )
        .unwrap();
        let ResolvedSaslMechanism::Callback(cb) = resolved else {
            panic!("expected callback mechanism");
        };
        let got = cb().await.unwrap();
        assert_eq!(got, secret);
    }
}

// Upstream: pkg/storage/ingest/util_test.go::TestOauthbearerStaticCredentials
#[test]
fn test_oauthbearer_static_credentials() {
    let secret = KafkaOauthbearerAuth {
        token: "some-oauth-token".to_string(),
        zid: "some-zid".to_string(),
        extensions: Default::default(),
    };
    let cfg = KafkaSaslSecretConfig {
        secret: Some(secret.clone()),
        file_path: String::new(),
        http_socket_path: String::new(),
        http_socket_timeout: Duration::from_secs(10),
    };
    let resolved = sasl_mechanism(
        cfg,
        |_| -> KafkaOauthbearerAuth { panic!("should have not been called") },
        |s| s,
    )
    .unwrap();
    assert_eq!(resolved, secret);
}

// Upstream: pkg/storage/ingest/util_test.go::TestIngesterPartitionID
#[test]
fn test_ingester_partition_id() {
    // "with zones"
    assert_eq!(ingester_partition_id("ingester-zone-a-0").unwrap(), 0);
    assert_eq!(ingester_partition_id("ingester-zone-b-0").unwrap(), 0);
    assert_eq!(ingester_partition_id("ingester-zone-a-1").unwrap(), 1);
    assert_eq!(ingester_partition_id("ingester-zone-b-1").unwrap(), 1);
    assert_eq!(ingester_partition_id("mimir-write-zone-c-2").unwrap(), 2);

    // "without zones"
    assert_eq!(ingester_partition_id("ingester-0").unwrap(), 0);
    assert_eq!(ingester_partition_id("ingester-1").unwrap(), 1);
    assert_eq!(ingester_partition_id("mimir-write-2").unwrap(), 2);

    // "should return error if the ingester ID has a non supported format"
    assert!(ingester_partition_id("unknown").is_err());
    assert!(ingester_partition_id("ingester-zone-a-").is_err());
    assert!(ingester_partition_id("ingester-zone-a").is_err());
}

// Upstream: pkg/storage/ingest/util_test.go::TestResultPromise
#[tokio::test]
async fn test_result_promise() {
    // "wait() should block until a result has been notified"
    {
        let rw = Arc::new(ResultPromise::<i32>::new());
        let mut handles = Vec::new();
        for _ in 0..3 {
            let rw_c = Arc::clone(&rw);
            handles.push(tokio::spawn(async move {
                let actual = rw_c.wait(None).await.unwrap();
                assert_eq!(actual, 12345);
            }));
        }
        rw.notify(Ok(12345));
        for h in handles {
            h.await.unwrap();
        }
    }

    // "wait() should block until an error has been notified"
    {
        let rw = Arc::new(ResultPromise::<i32>::new());
        let mut handles = Vec::new();
        for _ in 0..3 {
            let rw_c = Arc::clone(&rw);
            handles.push(tokio::spawn(async move {
                let err = rw_c.wait(None).await.unwrap_err();
                assert_eq!(err, "test error");
            }));
        }
        rw.notify(Err("test error".to_string()));
        for h in handles {
            h.await.unwrap();
        }
    }

    // "wait() should return when the input context timeout expires"
    {
        let rw = ResultPromise::<i32>::new();
        let err = rw.wait(Some(Duration::from_millis(50))).await.unwrap_err();
        assert!(err.contains("deadline exceeded"), "unexpected error: {err}");
    }
}

// Upstream: pkg/storage/ingest/util_test.go::TestCreateTopics
#[tokio::test]
async fn test_create_topics() {
    // "should create the partitions"
    {
        let cluster = FakeKafkaCluster::start(0, "").await;
        let mut cfg = create_test_kafka_config(&cluster.addr, "topic-name");
        cfg.auto_create_topic_default_partitions = 100;

        *cluster.hooks.create_topics.lock() = Some(Arc::new(|req| {
            Box::pin(async move {
                assert_eq!(req.topics.len(), 1);
                let res = &req.topics[0];
                assert_eq!(res.name.0.as_str(), "topic-name");
                assert_eq!(res.num_partitions, 100);
                assert_eq!(res.replication_factor, -1);
                assert!(res.configs.is_empty());
                assert!(res.assignments.is_empty());

                let mut resp = CreateTopicsResponse::default();
                let mut tr = CreatableTopicResult::default();
                tr.name = res.name.clone();
                tr.num_partitions = res.num_partitions;
                tr.replication_factor = 3;
                resp.topics.push(tr);
                HookAction::Respond(resp)
            })
        }));

        create_topics(&cfg, &[&cfg.topic]).await.unwrap();
        cluster.close();
    }

    // "should create all the requested topics"
    {
        let cluster = FakeKafkaCluster::start(0, "").await;
        let mut cfg = create_test_kafka_config(&cluster.addr, "");
        cfg.auto_create_topic_default_partitions = 100;
        let topics = ["topic-a", "topic-b"];

        *cluster.hooks.create_topics.lock() = Some(Arc::new(|req| {
            Box::pin(async move {
                assert_eq!(req.topics.len(), 2);
                let mut resp = CreateTopicsResponse::default();
                for res in &req.topics {
                    assert_eq!(res.num_partitions, 100);
                    assert_eq!(res.replication_factor, -1);
                    let mut tr = CreatableTopicResult::default();
                    tr.name = res.name.clone();
                    tr.num_partitions = res.num_partitions;
                    tr.replication_factor = 3;
                    resp.topics.push(tr);
                }
                HookAction::Respond(resp)
            })
        }));

        create_topics(&cfg, &topics).await.unwrap();
        cluster.close();
    }

    // "should return an error if one of multiple topics fails to be created"
    {
        let cluster = FakeKafkaCluster::start(0, "").await;
        let mut cfg = create_test_kafka_config(&cluster.addr, "");
        cfg.auto_create_topic_default_partitions = 100;

        *cluster.hooks.create_topics.lock() = Some(Arc::new(|_req| {
            Box::pin(async move {
                let mut resp = CreateTopicsResponse::default();
                let mut ta = CreatableTopicResult::default();
                ta.name = TopicName(StrBytes::from_static_str("topic-a"));
                ta.error_code = ResponseError::TopicAlreadyExists.code();
                let mut tb = CreatableTopicResult::default();
                tb.name = TopicName(StrBytes::from_static_str("topic-b"));
                tb.error_code = ResponseError::NotLeaderOrFollower.code();
                resp.topics = vec![ta, tb];
                HookAction::Respond(resp)
            })
        }));

        let err = create_topics(&cfg, &["topic-a", "topic-b"])
            .await
            .unwrap_err();
        assert!(err.contains("topic-b"), "expected topic-b in {err}");
        cluster.close();
    }

    // "should return an error if a requested topic is missing from the response"
    {
        let cluster = FakeKafkaCluster::start(0, "").await;
        let mut cfg = create_test_kafka_config(&cluster.addr, "");
        cfg.auto_create_topic_default_partitions = 100;

        *cluster.hooks.create_topics.lock() = Some(Arc::new(|_req| {
            Box::pin(async move {
                let mut resp = CreateTopicsResponse::default();
                let mut ta = CreatableTopicResult::default();
                ta.name = TopicName(StrBytes::from_static_str("topic-a"));
                ta.num_partitions = 100;
                ta.replication_factor = 3;
                resp.topics = vec![ta];
                HookAction::Respond(resp)
            })
        }));

        let err = create_topics(&cfg, &["topic-a", "topic-b"])
            .await
            .unwrap_err();
        assert!(err.contains("topic-b"), "expected topic-b in {err}");
        cluster.close();
    }

    // "should return an error if the request fails"
    {
        let cluster = FakeKafkaCluster::start(0, "").await;
        let mut cfg = create_test_kafka_config(&cluster.addr, "topic-name");
        cfg.auto_create_topic_default_partitions = 100;

        *cluster.hooks.create_topics.lock() = Some(Arc::new(|_req| {
            Box::pin(async move {
                let mut resp = CreateTopicsResponse::default();
                let mut t = CreatableTopicResult::default();
                t.name = TopicName(StrBytes::from_static_str("topic-name"));
                t.error_code = ResponseError::NotLeaderOrFollower.code();
                resp.topics = vec![t];
                HookAction::Respond(resp)
            })
        }));

        assert!(create_topics(&cfg, &[&cfg.topic]).await.is_err());
        cluster.close();
    }

    // "should not return error when topic already exists"
    {
        let cluster = FakeKafkaCluster::start(0, "").await;
        let mut cfg = create_test_kafka_config(&cluster.addr, "topic-name");
        cfg.auto_create_topic_default_partitions = 100;

        create_topics(&cfg, &[&cfg.topic]).await.unwrap();
        create_topics(&cfg, &[&cfg.topic]).await.unwrap();
        cluster.close();
    }
}

fn fetch_record_with_traceparent(trace_id: &str, sampled: bool) -> TracedKafkaRecord {
    let flags = if sampled { "01" } else { "00" };
    TracedKafkaRecord {
        topic: "test-topic".to_string(),
        headers: vec![(
            "traceparent".to_string(),
            Bytes::from(format!("00-{trace_id}-00f067aa0ba902b7-{flags}")),
        )],
        ..Default::default()
    }
}

// Upstream: pkg/storage/ingest/util_test.go::TestSampledOnlyTracer_FetchHooks
#[test]
fn test_sampled_only_tracer_fetch_hooks() {
    const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

    // "unsampled record: context carries the remote span context, no span is created"
    {
        let recorder = SpanRecorder::new();
        let tracer = SampledOnlyTracer::new(Arc::clone(&recorder));
        let mut rec = fetch_record_with_traceparent(TRACE_ID, false);
        tracer.on_fetch_record_buffered(&mut rec);

        let sc = rec.span_ctx.expect("span context set");
        assert_eq!(sc.trace_id_string(), TRACE_ID);
        assert!(!sc.is_sampled());
        assert!(sc.is_remote());
        assert!(recorder.started().is_empty());

        tracer.on_fetch_record_unbuffered(&mut rec, true);
        assert!(recorder.ended().is_empty());
    }

    // "sampled record: a receive span is created and ended"
    {
        let recorder = SpanRecorder::new();
        let tracer = SampledOnlyTracer::new(Arc::clone(&recorder));
        let mut rec = fetch_record_with_traceparent(TRACE_ID, true);
        tracer.on_fetch_record_buffered(&mut rec);

        let started = recorder.started();
        assert_eq!(started.len(), 1);
        assert_eq!(started[0].name, "test-topic receive");
        assert_eq!(
            TraceSpanContext {
                trace_id: started[0].parent_trace_id.unwrap(),
                span_id: [1; 8],
                sampled: true,
                remote: true,
            }
            .trace_id_string(),
            TRACE_ID
        );
        assert!(rec.span_ctx.unwrap().is_sampled());

        tracer.on_fetch_record_unbuffered(&mut rec, true);
        assert_eq!(recorder.ended().len(), 1);
    }

    // "record without trace headers: context is set, no span is created"
    {
        let recorder = SpanRecorder::new();
        let tracer = SampledOnlyTracer::new(Arc::clone(&recorder));
        let mut rec = TracedKafkaRecord {
            topic: "test-topic".to_string(),
            ..Default::default()
        };
        tracer.on_fetch_record_buffered(&mut rec);

        let sc = rec.span_ctx.expect("context set");
        assert!(!sc.is_valid());
        assert!(recorder.started().is_empty());

        tracer.on_fetch_record_unbuffered(&mut rec, true);
        assert!(recorder.ended().is_empty());
    }
}

// Upstream: pkg/storage/ingest/util_test.go::TestSampledOnlyTracer_ProduceHooks
#[test]
fn test_sampled_only_tracer_produce_hooks() {
    // "unsampled context: no span is created"
    {
        let recorder = SpanRecorder::new();
        let tracer = SampledOnlyTracer::new(Arc::clone(&recorder));
        let mut rec = TracedKafkaRecord {
            topic: "test-topic".to_string(),
            span_ctx: Some(TraceSpanContext {
                trace_id: [1; 16],
                span_id: [1; 8],
                sampled: false,
                remote: true,
            }),
            ..Default::default()
        };
        tracer.on_produce_record_buffered(&mut rec);
        assert!(recorder.started().is_empty());
        assert!(rec.headers.is_empty());

        tracer.on_produce_record_unbuffered(&mut rec);
        assert!(recorder.ended().is_empty());
    }

    // "sampled context: a publish span is created and ended"
    {
        let recorder = SpanRecorder::new();
        let tracer = SampledOnlyTracer::new(Arc::clone(&recorder));
        let mut rec = TracedKafkaRecord {
            topic: "test-topic".to_string(),
            span_ctx: Some(TraceSpanContext {
                trace_id: [2; 16],
                span_id: [3; 8],
                sampled: true,
                remote: false,
            }),
            ..Default::default()
        };
        tracer.on_produce_record_buffered(&mut rec);
        let started = recorder.started();
        assert_eq!(started.len(), 1);
        assert_eq!(started[0].name, "test-topic publish");
        assert!(rec.headers.iter().any(|(k, _)| k == "traceparent"));

        tracer.on_produce_record_unbuffered(&mut rec);
        assert_eq!(recorder.ended().len(), 1);
    }
}
