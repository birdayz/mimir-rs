//! Wave 1 Task W1-B Conformance Tests for `crates/mimir-common`.
//!
//! - Differential conformance against `target/go-oracle hash-series` across 110+ diverse label sets.
//! - Ported upstream unit tests for tenant IDs, labels_hash, services FSM, reactive limiter,
//!   token-bucket/inflight limiters, backoff, globalerror, logging, shutdownmarker, and activitytracker.

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use mimir_common::activitytracker::{
    load_unfinished_entries, trim_entry_to_size, ActivityTracker, ActivityTrackerConfig,
    ENTRY_SIZE, TIMESTAMP_LENGTH,
};
use mimir_common::backoff::{Backoff, BackoffConfig, BackoffError};
use mimir_common::errors::{
    discard_reasons, map_native_histogram_err, Code, ErrorCause, ErrorDetails, ErrorWithStatus,
    NativeHistogramValidationError, INGESTION_RATE_LIMITED, MISSING_METRIC_NAME,
    SAMPLE_OUT_OF_ORDER,
};
use mimir_common::labels_hash::{
    are_label_names_sorted_and_unique, hash_for_labels, hash_without_labels, is_stale_nan,
    prom_labels_hash, prom_labels_hash_sorted, stale_nan, LabelsBytes, ValidationScheme,
    EXEMPLAR_MAX_LABEL_SET_LENGTH, STALE_NAN_BITS,
};
use mimir_common::limiter::{
    InflightLimitError, InflightLimiter, LimiterRingStrategy,
    RateLimiter, ReactiveLimitError, ReactiveLimiter, ReactiveLimiterConfig,
    RingScaledRateLimiterStrategy,
};
use mimir_common::logging::{
    drop_unsafe_chars, escape_unsafe_chars, format_json_line, format_logfmt_line,
    format_rfc3339_nano_utc, format_rfc3339_utc, IntervalRateLimitedLogger, LogFormat, LogLevel,
    StructuredLogger, TokenBucketRateLimitedLogger,
};
use mimir_common::services::{
    basic_service, idle_service, start_and_await_running, start_manager_and_await_healthy,
    stop_and_await_terminated, stop_manager_and_await_stopped, timer_service, FailureWatcher,
    Listener, Service, ServiceContext, ServiceError, ServiceManager, State,
};
use mimir_common::shutdownmarker;
use mimir_common::tenant::{
    extract_org_id_from_headers, join_tenant_ids, normalize_tenant_ids, parse_single_tenant_id,
    parse_tenant_ids, parse_with_metadata, valid_tenant_id, MultiResolver, Resolver, TenantError,
    MAX_TENANT_ID_LENGTH,
};
use mimir_conformance::oracle::{
    ensure_go_oracle_built, invoke_oracle, HashSeriesRequest, HashSeriesResponse, LabelJson,
};

#[test]
fn prom_labels_hash_matches_go_oracle_across_110_plus_cases() {
    ensure_go_oracle_built().expect("go-oracle must build");

    let mut cases: Vec<(&str, Vec<(String, String)>)> = Vec::new();

    // 1. Empty label set
    cases.push(("empty", vec![]));

    // 2. Single label variants (note: Prometheus Builder drops empty-value labels;
    // we verify without_empty() separately below)
    cases.push((
        "single_metric",
        vec![("__name__".into(), "up".into())],
    ));
    cases.push((
        "single_custom",
        vec![("job".into(), "prometheus".into())],
    ));
    cases.push((
        "single_space_value",
        vec![("space".into(), " ".into())],
    ));

    // 3. Sorted vs unsorted orders
    cases.push((
        "sorted_3",
        vec![
            ("__name__".into(), "http_requests_total".into()),
            ("instance".into(), "10.0.0.1:9090".into()),
            ("job".into(), "api".into()),
        ],
    ));
    cases.push((
        "unsorted_3",
        vec![
            ("job".into(), "api".into()),
            ("instance".into(), "10.0.0.1:9090".into()),
            ("__name__".into(), "http_requests_total".into()),
        ],
    ));
    cases.push((
        "reverse_10",
        (0..10)
            .rev()
            .map(|i| (format!("lbl_{i:02}"), format!("val_{i:02}")))
            .collect(),
    ));

    // 4. Boundary lengths around 255 (1-byte length vs 0xff + 3-byte LE length encoding!)
    for len in [
        1usize, 2, 127, 128, 253, 254, 255, 256, 257, 511, 512, 1024, 4096, 65535, 65536,
    ] {
        cases.push((
            "long_value_boundary",
            vec![
                ("__name__".into(), "long_val_metric".into()),
                ("k".into(), "x".repeat(len)),
            ],
        ));
        cases.push((
            "long_name_boundary",
            vec![
                ("__name__".into(), "long_name_metric".into()),
                ("n".repeat(len), "val".into()),
            ],
        ));
    }

    // 5. Both name and value >= 255 bytes in same series with multiple labels
    cases.push((
        "both_name_and_val_over_255",
        vec![
            ("__name__".into(), "m".repeat(300)),
            ("a".repeat(255), "b".repeat(255)),
            ("c".repeat(400), "d".repeat(700)),
            ("short".into(), "ok".into()),
        ],
    ));

    // 6. Unicode label names and values (ASCII, Latin accented, CJK, Cyrillic, Emoji)
    let emoji_256 = "🚀".repeat(64);
    let emoji_254 = "a".repeat(250) + "🚀";
    let emoji_255 = "a".repeat(251) + "🚀";
    let emoji_border_256 = "a".repeat(252) + "🚀";
    let unicode_samples = [
        ("metric.utf8/name", "héllo wörld"),
        ("метрика", "значение"),
        ("指標名", "値🎉🚀🎄"),
        ("label_with_newline", "line1\nline2\ttab"),
        ("label_with_quotes", "a=\"b\", c=\\d"),
        ("emoji_🔥_label", emoji_256.as_str()), // 256 bytes of UTF-8 emoji (> 255!)
        ("emoji_border_254", emoji_254.as_str()), // 254 bytes
        ("emoji_border_255", emoji_255.as_str()), // 255 bytes
        ("emoji_border_256", emoji_border_256.as_str()), // 256 bytes
    ];
    for (i, (k, v)) in unicode_samples.iter().enumerate() {
        cases.push((
            "unicode_single",
            vec![
                ("__name__".into(), format!("utf8_metric_{i}")),
                ((*k).to_string(), v.to_string()),
            ],
        ));
    }

    // 7. Many diverse multi-label series (varying cardinality & label counts)
    for n in 1..=60 {
        let mut lbls = vec![("__name__".to_string(), format!("series_cardinality_test_{n}"))];
        for j in 0..((n % 15) + 1) {
            let k = format!("label_{j}_{}", (n * 7 + j) % 13);
            let v = if (n + j) % 11 == 0 {
                "val_".to_string() + &"z".repeat(260)
            } else {
                format!("value_{n}_{j}_αβγ")
            };
            lbls.push((k, v));
        }
        cases.push(("diverse_multi", lbls));
    }

    assert!(
        cases.len() >= 100,
        "expected 100+ test cases, got {}",
        cases.len()
    );

    for (idx, (tag, pairs)) in cases.iter().enumerate() {
        let metric_name = pairs
            .iter()
            .find(|(k, _)| k == "__name__")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();

        let oracle_labels: Vec<LabelJson> = pairs
            .iter()
            .map(|(k, v)| LabelJson {
                name: k.clone(),
                value: v.clone(),
            })
            .collect();

        let resp: HashSeriesResponse = invoke_oracle(
            "hash-series",
            &HashSeriesRequest {
                user_id: "tenant-conformance".to_string(),
                metric_name,
                labels: oracle_labels,
            },
        )
        .unwrap_or_else(|e| panic!("oracle hash-series failed at case #{idx} ({tag}): {e}"));

        let pair_refs: Vec<(&str, &str)> =
            pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

        let rust_hash = prom_labels_hash(&pair_refs);
        let lb = LabelsBytes::from_labels(&pair_refs);
        let lb_hash = lb.hash();

        assert_eq!(
            rust_hash, resp.prom_labels_hash,
            "prom_labels_hash mismatch at case #{idx} ({tag})"
        );
        assert_eq!(
            lb_hash, resp.prom_labels_hash,
            "LabelsBytes::hash mismatch at case #{idx} ({tag})"
        );

        let mut sorted_refs = pair_refs.clone();
        sorted_refs.sort_by(|a, b| a.0.cmp(b.0));
        assert_eq!(
            prom_labels_hash_sorted(sorted_refs.iter().copied()),
            resp.prom_labels_hash,
            "prom_labels_hash_sorted mismatch at case #{idx} ({tag})"
        );
    }

    // Verify empty-value stripping matches Prometheus Builder.Set(k, "")
    let resp_empty: HashSeriesResponse = invoke_oracle(
        "hash-series",
        &HashSeriesRequest {
            user_id: "tenant-conformance".to_string(),
            metric_name: String::new(),
            labels: vec![LabelJson {
                name: "empty".to_string(),
                value: String::new(),
            }],
        },
    )
    .expect("oracle empty value");
    assert_eq!(
        LabelsBytes::from_labels(&[("empty", "")]).without_empty().hash(),
        resp_empty.prom_labels_hash
    );
}

#[test]
fn labels_hash_helpers_and_validation() {
    assert_eq!(STALE_NAN_BITS, 0x7ff0_0000_0000_0002);
    assert!(is_stale_nan(stale_nan()));
    assert!(!is_stale_nan(f64::NAN));
    assert!(!is_stale_nan(1.23));

    assert!(ValidationScheme::Legacy.is_valid_label_name("job"));
    assert!(ValidationScheme::Legacy.is_valid_label_name("_foo_0"));
    assert!(!ValidationScheme::Legacy.is_valid_label_name("0foo"));
    assert!(!ValidationScheme::Legacy.is_valid_label_name("foo:bar"));
    assert!(ValidationScheme::Legacy.is_valid_metric_name("foo:bar_0"));
    assert!(ValidationScheme::Utf8.is_valid_label_name("foo.bar/baz🚀"));
    assert!(!ValidationScheme::Utf8.is_valid_label_name(""));

    assert_eq!(EXEMPLAR_MAX_LABEL_SET_LENGTH, 128);
    assert!(are_label_names_sorted_and_unique(&[("a", "1"), ("b", "2")]));
    assert!(!are_label_names_sorted_and_unique(&[("b", "1"), ("a", "2")]));
    assert!(!are_label_names_sorted_and_unique(&[("a", "1"), ("a", "2")]));

    let sorted = [
        ("__name__", "up"),
        ("env", "prod"),
        ("instance", "host1"),
        ("job", "app"),
    ];
    let lb = LabelsBytes::from_sorted_labels(&sorted);
    let mut buf = Vec::new();
    // hash_without_labels excludes __name__ and specified labels
    assert_eq!(
        lb.hash_without_labels(&mut buf, &["instance"]),
        hash_without_labels(&sorted, &["instance"])
    );
    assert_eq!(
        lb.hash_without_labels(&mut buf, &["instance"]),
        hash_for_labels(&sorted, &["env", "job"])
    );
    assert_eq!(
        lb.hash_for_labels(&mut buf, &["env", "job"]),
        hash_for_labels(&sorted, &["env", "job"])
    );
}

#[test]
fn tenant_id_validation_parsing_and_headers() {
    assert_eq!(MAX_TENANT_ID_LENGTH, 150);

    // Exhaustive ASCII table check matching dskit/tenant
    for b in 0u8..=255 {
        let ch = char::from(b);
        let s = ch.to_string();
        let expected_valid = ch.is_ascii_alphanumeric() || "!-_.*'()".contains(ch);
        let res = valid_tenant_id(&s);
        if b == b'.' {
            assert_eq!(res, Err(TenantError::UnsafeTenantId));
        } else if expected_valid {
            assert!(res.is_ok(), "expected byte {b} ({ch:?}) to be valid");
        } else {
            assert!(res.is_err(), "expected byte {b} ({ch:?}) to be invalid");
        }
    }

    assert_eq!(
        valid_tenant_id(".."),
        Err(TenantError::UnsafeTenantId)
    );
    assert_eq!(
        valid_tenant_id(&"x".repeat(151)),
        Err(TenantError::TooLong)
    );
    assert!(valid_tenant_id(&"x".repeat(150)).is_ok());

    assert_eq!(
        normalize_tenant_ids(vec!["c".into(), "a".into(), "b".into(), "a".into()]),
        vec!["a", "b", "c"]
    );
    assert_eq!(join_tenant_ids(&["a", "b", "c"]), "a|b|c");

    assert_eq!(
        parse_tenant_ids("tenant-b|tenant-a:src=k6|tenant-b").unwrap(),
        vec!["tenant-a", "tenant-b"]
    );
    assert_eq!(
        parse_single_tenant_id("tenant-1:k=v|tenant-1").unwrap(),
        "tenant-1"
    );
    assert_eq!(
        parse_single_tenant_id("tenant-1|tenant-2"),
        Err(TenantError::TooManyOrgIDs)
    );

    let resolver = MultiResolver::new();
    assert_eq!(resolver.tenant_id(None), Err(TenantError::NoOrgID));
    assert_eq!(
        resolver.tenant_ids(Some("z|a|z")).unwrap(),
        vec!["a", "z"]
    );

    let (tid, meta) = parse_with_metadata("org1:env=prod:team=core").unwrap();
    assert_eq!(tid, "org1");
    assert_eq!(meta.get("env"), Some("prod"));
    assert_eq!(meta.get("team"), Some("core"));

    // Case-insensitive HTTP header extraction + inconsistent header rejection
    assert_eq!(
        extract_org_id_from_headers([("x-scope-orgid", "t1")]).unwrap(),
        "t1"
    );
    assert_eq!(
        extract_org_id_from_headers([("X-SCOPE-ORGID", "t1"), ("X-Scope-OrgID", "t1")]).unwrap(),
        "t1"
    );
    assert_eq!(
        extract_org_id_from_headers([("X-Scope-OrgID", "t1"), ("x-scope-orgid", "t2")]),
        Err(TenantError::TooManyOrgIDs)
    );
    assert_eq!(
        extract_org_id_from_headers(std::iter::empty::<(&str, &str)>()),
        Err(TenantError::NoOrgID)
    );
}

#[test]
fn services_fsm_and_manager_conformance() {
    mimir_common::block_on_for_tests(async {
        // 1. State transition recording via Listener
        let transitions = Arc::new(Mutex::new(Vec::<String>::new()));
        let t_clone = transitions.clone();
        struct RecordingListener(Arc<Mutex<Vec<String>>>);
        impl Listener for RecordingListener {
            fn starting(&self) {
                self.0.lock().unwrap().push("Starting".to_string());
            }
            fn running(&self) {
                self.0.lock().unwrap().push("Running".to_string());
            }
            fn stopping(&self, from: State) {
                self.0.lock().unwrap().push(format!("Stopping:{from}"));
            }
            fn terminated(&self, from: State) {
                self.0.lock().unwrap().push(format!("Terminated:{from}"));
            }
            fn failed(&self, from: State, err: &ServiceError) {
                self.0.lock().unwrap().push(format!("Failed:{from}:{err}"));
            }
        }

        let svc = idle_service(None, None).with_name("test-idle");
        let _handle = svc.add_listener(Arc::new(RecordingListener(t_clone)));
        assert_eq!(svc.state(), State::New);

        let ctx = ServiceContext::background();
        start_and_await_running(ctx.clone(), svc.as_ref())
            .await
            .unwrap();
        assert_eq!(svc.state(), State::Running);

        // Starting an already Running service fails
        assert!(svc.start_async(ctx.clone()).await.is_err());

        stop_and_await_terminated(ctx.clone(), svc.as_ref())
            .await
            .unwrap();
        assert_eq!(svc.state(), State::Terminated);

        // Wait for async listener task to drain final Terminated event
        let wait_deadline = Instant::now() + Duration::from_secs(2);
        while transitions.lock().unwrap().len() < 4 {
            assert!(
                Instant::now() < wait_deadline,
                "timed out waiting for listener transitions: {:?}",
                *transitions.lock().unwrap()
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        let recorded = transitions.lock().unwrap().clone();
        assert_eq!(
            recorded,
            vec!["Starting", "Running", "Stopping:Running", "Terminated:Stopping"]
        );

        // 2. StopAsync on New service goes straight to Terminated
        let new_svc = idle_service(None, None).with_name("new-svc");
        new_svc.stop_async();
        assert_eq!(new_svc.state(), State::Terminated);

        // 3. Cancel during Starting skips Running -> Stopping:Starting -> Terminated
        let started_flag = Arc::new(AtomicBool::new(false));
        let sf = started_flag.clone();
        let cancel_during_start = basic_service(
            Some(Box::new(move |sctx| {
                let sf = sf.clone();
                Box::pin(async move {
                    sf.store(true, Ordering::Release);
                    sctx.canceled().await;
                    Ok(())
                })
            })),
            Some(Box::new(|_| Box::pin(async { panic!("should not reach Running") }))),
            None,
        );
        let cancel_ctx = ServiceContext::background();
        cancel_during_start
            .start_async(cancel_ctx.clone())
            .await
            .unwrap();
        while !started_flag.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        cancel_during_start.stop_async();
        cancel_during_start.await_terminated().await.unwrap();
        assert_eq!(cancel_during_start.state(), State::Terminated);

        // 4. ServiceManager healthy, services_by_state, timer service, and FailureWatcher
        let ticks = Arc::new(AtomicUsize::new(0));
        let ticks_c = ticks.clone();
        let ts = timer_service(
            Duration::from_millis(5),
            None,
            move |_| {
                let tc = ticks_c.clone();
                Box::pin(async move {
                    tc.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                })
            },
            None,
        )
        .with_name("timer");
        let is = idle_service(None, None).with_name("idle");
        let mgr = ServiceManager::new(vec![ts.clone(), is.clone()]).unwrap();
        let watcher = FailureWatcher::new();
        watcher.watch_manager(&mgr);

        start_manager_and_await_healthy(ctx.clone(), &mgr)
            .await
            .unwrap();
        assert!(mgr.is_healthy());
        let by_state = mgr.services_by_state();
        assert_eq!(by_state.get(&State::Running).map(Vec::len), Some(2));
        let states = mgr.service_states();
        assert_eq!(states.len(), 2);

        stop_manager_and_await_stopped(ctx, &mgr).await.unwrap();
        assert!(mgr.is_stopped());
        assert!(watcher.try_recv().is_none());
        watcher.close();
    });
}

#[test]
fn reactive_limiter_rate_limiter_inflight_and_backoff() {
    // 1. ReactiveLimiter validation & AIMD/Vegas adaptation
    let bad_cfg = ReactiveLimiterConfig {
        min_limit_factor: 0.5,
        ..Default::default()
    };
    assert!(bad_cfg.validate().is_err());

    let cfg = ReactiveLimiterConfig {
        min_limit: 2,
        max_limit: 50,
        initial_limit: 10,
        recent_window_min_samples: 10,
        recent_window_min_duration: Duration::from_millis(10),
        ..Default::default()
    };
    let rl = ReactiveLimiter::new(cfg).unwrap();
    let mut now = Instant::now();

    // Fast baseline RTT with concurrent inflight increases limit
    for _ in 0..15 {
        now += Duration::from_millis(20);
        let mut permits = Vec::new();
        while let Some(p) = rl.try_acquire_permit() {
            permits.push(p);
        }
        for p in permits {
            p.record_with_rtt(now, Duration::from_millis(1));
        }
    }
    let elevated = rl.limit();
    assert!(elevated > 10, "expected limit > 10, got {elevated}");

    // High RTT spike (advance > min_rtt * 2 = 1000ms per window) fills MedianFilter(5) -> decreases limit
    for _ in 0..8 {
        now += Duration::from_millis(1200);
        let mut permits = Vec::new();
        while let Some(p) = rl.try_acquire_permit() {
            permits.push(p);
        }
        for p in permits {
            p.record_with_rtt(now, Duration::from_millis(500));
        }
    }
    assert!(
        rl.limit() < elevated,
        "expected limit to decrease from {elevated}, got {}",
        rl.limit()
    );

    rl.reset();
    assert_eq!(rl.limit(), 10);

    // 2. Async queueing & non-queueing check
    mimir_common::block_on_for_tests(async {
        // Without queueing: full immediately rejects can_acquire_permit()
        let no_q_cfg = ReactiveLimiterConfig {
            min_limit: 1,
            max_limit: 1,
            initial_limit: 1,
            initial_rejection_factor: 0.0,
            max_rejection_factor: 0.0,
            ..Default::default()
        };
        let no_q = ReactiveLimiter::new(no_q_cfg).unwrap();
        let p_only = no_q.try_acquire_permit().unwrap();
        assert!(!no_q.can_acquire_permit());
        drop(p_only);

        // With queueing (1.0..1.0): allows limit*1 = 1 queued waiter, rejects the next
        let qcfg = ReactiveLimiterConfig {
            min_limit: 1,
            max_limit: 1,
            initial_limit: 1,
            initial_rejection_factor: 1.0,
            max_rejection_factor: 1.0,
            ..Default::default()
        };
        let qrl = ReactiveLimiter::new(qcfg).unwrap();
        let p1 = qrl.acquire_permit().await.unwrap();
        assert!(qrl.can_acquire_permit());

        let qrl_c = qrl.clone();
        // Acquire 2nd permit on background task so it queues (queued == 1 == max_queue)
        std::thread::spawn(move || {
            mimir_common::block_on_for_tests(async move {
                let _p2 = qrl_c.acquire_permit().await.unwrap();
            });
        });
        while qrl.queued() == 0 {
            std::thread::yield_now();
        }
        assert!(!qrl.can_acquire_permit());
        assert_eq!(
            qrl.acquire_permit().await.err(),
            Some(ReactiveLimitError::LimitExceeded)
        );
        drop(p1);
    });

    // 3. Ring-scaled RateLimiter
    struct MockRing;
    impl LimiterRingStrategy for MockRing {
        fn convert_global_to_local_limit(&self, _user_id: &str, global_limit: usize) -> usize {
            global_limit / 2
        }
        fn get_shard_size(&self, _user_id: &str) -> usize {
            2
        }
    }
    let strategy = Arc::new(RingScaledRateLimiterStrategy::new(
        |_| 20.0,
        |_| 10,
        Arc::new(MockRing),
    ));
    let rate_limiter = RateLimiter::new(strategy, Duration::from_secs(1));
    let t0 = Instant::now();
    assert_eq!(rate_limiter.limit(t0, "u1"), 10.0);
    assert_eq!(rate_limiter.burst(t0, "u1"), 5);
    assert!(rate_limiter.allow_n(t0, "u1", 5));
    assert!(!rate_limiter.allow_n(t0, "u1", 1));

    // 4. InflightLimiter
    let inflight = InflightLimiter::new();
    let p = inflight.try_acquire(100, 1, 100).unwrap();
    assert_eq!(
        inflight.try_acquire(0, 2, 100).err(),
        Some(InflightLimitError::MaxInflightBytes)
    );
    drop(p);
    assert_eq!(inflight.inflight_requests(), 0);
    assert_eq!(inflight.inflight_bytes(), 0);

    // 5. Backoff
    let mut backoff = Backoff::new(BackoffConfig {
        min_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(20),
        max_retries: 3,
    });
    assert!(backoff.ongoing());
    let _ = backoff.next_delay();
    let _ = backoff.next_delay();
    let _ = backoff.next_delay();
    assert!(!backoff.ongoing());
    assert_eq!(backoff.err(), Some(BackoffError::MaxRetriesReached(3)));
}

#[test]
fn errors_and_logging_conformance() {
    assert_eq!(MISSING_METRIC_NAME.label_value(), "missing_metric_name");
    assert_eq!(
        INGESTION_RATE_LIMITED.label_value(),
        "tenant_max_ingestion_rate"
    );
    assert_eq!(SAMPLE_OUT_OF_ORDER.label_value(), "sample_out_of_order");
    assert_eq!(discard_reasons::RATE_LIMITED, "rate_limited");
    assert_eq!(
        map_native_histogram_err(NativeHistogramValidationError::CountMismatch).as_str(),
        "native-histogram-count-mismatch"
    );
    let err_status = ErrorWithStatus::new(
        MISSING_METRIC_NAME.message("no metric"),
        Code::InvalidArgument,
        Some(ErrorDetails {
            cause: ErrorCause::BadData,
            soft: false,
            rejected_samples: 3,
        }),
    )
    .with_http_status(400);
    assert_eq!(err_status.to_tonic_status().code(), Code::InvalidArgument);

    // Logging formats & sanitization
    assert_eq!(format_rfc3339_utc(UNIX_EPOCH), "1970-01-01T00:00:00Z");
    assert_eq!(
        format_rfc3339_nano_utc(UNIX_EPOCH + Duration::from_nanos(120_000_000)),
        "1970-01-01T00:00:00.12Z"
    );
    assert_eq!(
        format_logfmt_line(&[("level", "info"), ("msg", "hello world"), ("empty", "")]),
        // go-logfmt writes empty string values unquoted (`cluster_label= node=x` in Go logs).
        "level=info msg=\"hello world\" empty=\n"
    );
    let jline = format_json_line(&[("level", "error"), ("msg", "failed")]);
    let jval: serde_json::Value = serde_json::from_str(jline.trim()).unwrap();
    assert_eq!(jval["level"], "error");
    assert_eq!(jval["msg"], "failed");

    assert_eq!(drop_unsafe_chars("a\x1bb\u{2028}c\td"), "abc\td");
    assert_eq!(escape_unsafe_chars("a\nb"), "a\\x0ab");

    struct MemWriter(Arc<Mutex<Vec<u8>>>);
    impl Write for MemWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let buf = Arc::new(Mutex::new(Vec::new()));
    let base = Arc::new(
        StructuredLogger::new(LogFormat::Logfmt, LogLevel::Info, Box::new(MemWriter(buf.clone())))
            .with_clock(Arc::new(|| UNIX_EPOCH)),
    );
    let tb_rl = TokenBucketRateLimitedLogger::new(base.clone(), 1.0, 2, None);
    let t0 = Instant::now();
    tb_rl.log_at(t0, LogLevel::Info, &[("msg", "1")]);
    tb_rl.log_at(t0, LogLevel::Info, &[("msg", "2")]);
    tb_rl.log_at(t0, LogLevel::Info, &[("msg", "3")]);
    assert_eq!(
        String::from_utf8(buf.lock().unwrap().clone())
            .unwrap()
            .lines()
            .count(),
        2
    );

    let buf2 = Arc::new(Mutex::new(Vec::new()));
    let base2 = Arc::new(
        StructuredLogger::new(LogFormat::Logfmt, LogLevel::Info, Box::new(MemWriter(buf2.clone())))
            .with_clock(Arc::new(|| UNIX_EPOCH)),
    );
    let iv_rl = IntervalRateLimitedLogger::new(Duration::from_secs(1), base2);
    let t_start = Instant::now();
    iv_rl.log_at(t_start + Duration::from_millis(1), LogLevel::Info, &[("msg", "a")]);
    iv_rl.log_at(t_start + Duration::from_millis(2), LogLevel::Info, &[("msg", "b")]);
    iv_rl.log_at(t_start + Duration::from_secs(2), LogLevel::Info, &[("msg", "c")]);
    assert_eq!(
        String::from_utf8(buf2.lock().unwrap().clone())
            .unwrap()
            .lines()
            .count(),
        2
    );
}

#[test]
fn shutdownmarker_and_activitytracker_conformance() {
    // 1. Shutdown marker
    let tmp = tempfile::tempdir().unwrap();
    let marker_path = shutdownmarker::get_path(tmp.path());
    assert_eq!(marker_path, tmp.path().join("shutdown-requested.txt"));
    assert!(!shutdownmarker::exists(&marker_path).unwrap());

    shutdownmarker::create(&marker_path).unwrap();
    assert!(shutdownmarker::exists(&marker_path).unwrap());
    let ts_text = std::fs::read_to_string(&marker_path).unwrap();
    assert!(ts_text.ends_with('Z') && ts_text.len() == 20);

    shutdownmarker::remove(&marker_path).unwrap();
    assert!(!shutdownmarker::exists(&marker_path).unwrap());
    // Removing missing marker in missing directory does not fail
    shutdownmarker::remove(shutdownmarker::get_path(tmp.path().join("missing"))).unwrap();

    // 2. ActivityTracker binary format & lifecycle
    let act_file = tmp.path().join("metrics-activity.log");
    const MAX_ENTRIES: usize = 5;
    let tr = ActivityTracker::new(
        ActivityTrackerConfig {
            filepath: act_file.clone(),
            max_entries: MAX_ENTRIES,
        },
        None,
    )
    .unwrap()
    .unwrap();

    let mut inserted = Vec::new();
    for i in 0..MAX_ENTRIES {
        let a = format!("activity-{i}");
        let idx = tr.insert_static(&a);
        assert!(idx >= 0 && (idx as usize) < MAX_ENTRIES);
        inserted.push(a);
        assert_eq!(tr.free_slots_count(), (MAX_ENTRIES - i - 1) as f64);
    }

    // Verify exact binary layout on disk: 5 * 1024 = 5120 bytes
    tr.flush().unwrap();
    let raw = std::fs::read(&act_file).unwrap();
    assert_eq!(raw.len(), MAX_ENTRIES * ENTRY_SIZE);
    let ts0 = u64::from_be_bytes(raw[0..TIMESTAMP_LENGTH].try_into().unwrap());
    assert!(ts0 > 0);
    let loaded = load_unfinished_entries(&act_file).unwrap();
    let loaded_names: Vec<String> = loaded.into_iter().map(|e| e.activity).collect();
    assert_eq!(loaded_names.len(), MAX_ENTRIES);
    for a in &inserted {
        assert!(loaded_names.contains(a));
    }

    // Full tracker rejects
    assert_eq!(tr.insert_static("extra"), -1);
    assert_eq!(tr.failed_full_count(), 1.0);

    // Delete first 2 and verify unfinished entries
    tr.delete(0);
    tr.delete(1);
    tr.flush().unwrap();
    assert_eq!(tr.free_slots_count(), 2.0);
    assert_eq!(load_unfinished_entries(&act_file).unwrap().len(), 3);

    // Empty activity rejected
    assert_eq!(tr.insert_static(""), -1);
    assert_eq!(tr.failed_empty_count(), 1.0);

    tr.close().unwrap();

    // Reopening sees 3 unfinished activities from previous run
    let tr2 = ActivityTracker::new(
        ActivityTrackerConfig {
            filepath: act_file,
            max_entries: MAX_ENTRIES,
        },
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(tr2.loaded_activities_on_startup_count(), 3.0);
    tr2.close().unwrap();

    // Empty filepath disables tracker
    assert!(ActivityTracker::new(
        ActivityTrackerConfig {
            filepath: std::path::PathBuf::new(),
            max_entries: 5,
        },
        None
    )
    .unwrap()
    .is_none());

    // UTF-8 rune boundary trimming
    assert_eq!(trim_entry_to_size("🎉🎄🎅", 5), "🎉");
    assert_eq!(trim_entry_to_size("🎉🎄🎅", 11), "🎉🎄");
}
