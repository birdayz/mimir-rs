//! Wave 16 regressions for PartitionRing, PartitionInstanceLifecycler,
//! PartitionRingEditor, and ConsulKv.
//!
//! Covers:
//! - W16-APC-01: Reject unknown PartitionDesc.token_scheme instead of falling back to Stored
//! - W16-APC-08: Lifecycler startup running race, state-change error context, derived-token error
//!   text, and zero delete_inactive_partition_after reconciliation
//! - W16-APC-10: ConsulKv records consul_request_duration_seconds{status_code="500"} on canceled
//!   Get/List long-polls

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use mimir_common::services::{Service, ServiceContext};
use mimir_config::ingester::ConsulConfig;
use mimir_proto::ringpb::{PartitionDesc, PartitionRingDesc, PartitionState};
use mimir_ring::kv::consul::ConsulKv;
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::{
    registerer_with_kv_name, BoxFuture, CasFn, KvClient, KvError, Registerer, StringCodec,
    WatchKeyFn, WatchPrefixFn,
};
use mimir_ring::partition_ring::http::{
    handle_partition_ring_get, handle_partition_ring_post, PartitionRingPageState,
};
use mimir_ring::partition_ring::model::clean_token_scheme_raw_name;
use mimir_ring::partition_ring::{
    change_partition_state_in_desc, partition_ring_desc_codec, resolve_partition_tokens,
    PartitionInstanceLifecycler, PartitionInstanceLifecyclerConfig, PartitionRing,
    PartitionRingDescExt, PartitionRingEditor, PartitionRingError, PartitionRingOptions,
    PartitionRingReader,
};
use prometheus::Registry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

struct StaticReader(Arc<PartitionRing>);

impl PartitionRingReader for StaticReader {
    fn partition_ring(&self) -> Arc<PartitionRing> {
        Arc::clone(&self.0)
    }
}

struct FailingKv {
    cas_calls: AtomicUsize,
}

impl KvClient for FailingKv {
    fn list(&self, _prefix: &str) -> BoxFuture<'_, Result<Vec<String>, KvError>> {
        Box::pin(async { Err(KvError::Other("kv list failed".to_string())) })
    }

    fn get(&self, _key: &str) -> BoxFuture<'_, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        Box::pin(async { Err(KvError::Other("kv get failed".to_string())) })
    }

    fn delete(&self, _key: &str) -> BoxFuture<'_, Result<(), KvError>> {
        Box::pin(async { Err(KvError::Other("kv delete failed".to_string())) })
    }

    fn cas(&self, _key: &str, _f: CasFn) -> BoxFuture<'_, Result<(), KvError>> {
        self.cas_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(KvError::Other("kv cas get failed".to_string())) })
    }

    fn watch_key(
        &self,
        _key: &str,
        _ctx: ServiceContext,
        _f: WatchKeyFn,
    ) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn watch_prefix(
        &self,
        _prefix: &str,
        _ctx: ServiceContext,
        _f: WatchPrefixFn,
    ) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

#[test]
fn w16_apc_01_unknown_token_scheme_is_rejected_and_rendered_raw() {
    for raw_scheme in [2, -1, 99] {
        let mut desc = PartitionRingDesc::default();
        desc.partitions.insert(
            3,
            PartitionDesc {
                id: 3,
                tokens: vec![],
                state: PartitionState::PartitionActive as i32,
                state_timestamp: 100,
                state_change_locked: false,
                state_change_locked_timestamp: 0,
                token_scheme: raw_scheme,
            },
        );

        let expected = format!(
            "cannot resolve tokens for partition 3: unknown token scheme {raw_scheme}"
        );

        let err = resolve_partition_tokens(&desc, 3, &PartitionRingOptions::default())
            .expect_err("unknown token scheme must be rejected");
        assert_eq!(err.to_string(), expected);

        let ring_err = match PartitionRing::new(desc) {
            Ok(_) => panic!("PartitionRing::new must reject unknown token scheme {raw_scheme}"),
            Err(e) => e,
        };
        assert_eq!(ring_err.to_string(), expected);

        assert_eq!(clean_token_scheme_raw_name(raw_scheme), raw_scheme.to_string());
    }
    assert_eq!(clean_token_scheme_raw_name(0), "Stored");
    assert_eq!(clean_token_scheme_raw_name(1), "Smt512");
}

#[tokio::test]
async fn w16_apc_08_lifecycler_startup_has_no_running_race_and_errors_match_go() {
    // 1. Derived-token validation error matches Go verbatim.
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    let err = match PartitionInstanceLifecycler::new(
        PartitionInstanceLifecyclerConfig {
            partition_id: 4,
            create_partitions_with_derived_tokens: true,
            max_derived_token_partitions: 4,
            instance_id: "ing-4".to_string(),
            ..Default::default()
        },
        "ingester-partitions",
        "ingester-partitions",
        Arc::clone(&kv),
        None,
    ) {
        Ok(_) => panic!("partition_id >= max_derived_token_partitions must fail"),
        Err(e) => e,
    };
    assert_eq!(
        err.to_string(),
        "invalid partition ID 4: must be below the maximum derived token partitions (4) when creating partitions with derived tokens"
    );

    // 2. Immediately after await_running(), change_partition_state must NOT fail with
    //    "lifecycler not running". Repeat several times to catch any scheduling race.
    for iter in 0..20 {
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
        let plc = PartitionInstanceLifecycler::new(
            PartitionInstanceLifecyclerConfig {
                partition_id: 0,
                instance_id: format!("ing-{iter}"),
                wait_owners_duration_on_pending: Duration::from_secs(60),
                polling_interval: Duration::from_secs(60),
                ..Default::default()
            },
            "ingester-partitions",
            "ingester-partitions",
            Arc::clone(&kv),
            None,
        )
        .unwrap();

        let svc = Arc::clone(&plc).into_service();
        svc.start_async(ServiceContext::background()).await.unwrap();
        svc.await_running().await.unwrap();

        plc.change_partition_state(PartitionState::PartitionActive)
            .await
            .unwrap_or_else(|e| panic!("iter {iter}: immediate change_partition_state failed: {e}"));

        // Disallowed transition Active -> Pending must include from/to CleanName().
        let disallowed = plc
            .change_partition_state(PartitionState::PartitionPending)
            .await
            .unwrap_err();
        assert_eq!(
            disallowed.to_string(),
            "change partition state from Active to Pending: partition state change not allowed"
        );

        svc.stop_async();
        svc.await_terminated().await.unwrap();
        let after_stop = plc
            .change_partition_state(PartitionState::PartitionInactive)
            .await
            .unwrap_err();
        assert_eq!(after_stop.to_string(), "lifecycler not running");
    }
}

#[tokio::test]
async fn w16_apc_08_disallowed_state_change_context_and_http_400() {
    let mut desc = PartitionRingDesc::default();
    desc.add_partition(
        0,
        PartitionState::PartitionActive,
        std::time::SystemTime::now(),
    );
    desc.add_partition(
        1,
        PartitionState::PartitionInactive,
        std::time::SystemTime::now(),
    );
    desc.add_partition(
        2,
        PartitionState::PartitionDeleted,
        std::time::SystemTime::now(),
    );

    let err_active_pending =
        change_partition_state_in_desc(&mut desc, 0, PartitionState::PartitionPending).unwrap_err();
    assert!(matches!(
        err_active_pending,
        PartitionRingError::PartitionStateChangeNotAllowed {
            from: PartitionState::PartitionActive,
            to: PartitionState::PartitionPending,
        }
    ));
    assert_eq!(
        err_active_pending.to_string(),
        "change partition state from Active to Pending: partition state change not allowed"
    );

    let err_inactive_pending =
        change_partition_state_in_desc(&mut desc, 1, PartitionState::PartitionPending).unwrap_err();
    assert_eq!(
        err_inactive_pending.to_string(),
        "change partition state from Inactive to Pending: partition state change not allowed"
    );

    let err_deleted_active =
        change_partition_state_in_desc(&mut desc, 2, PartitionState::PartitionActive).unwrap_err();
    assert_eq!(
        err_deleted_active.to_string(),
        "change partition state from Deleted to Active: partition state change not allowed"
    );

    // Verify PartitionRingEditor and HTTP POST surface the full error string.
    let kv = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    kv.put_any("ingester-partitions", &desc).await.unwrap();
    let editor = Arc::new(PartitionRingEditor::new(
        "ingester-partitions",
        Arc::clone(&kv) as Arc<dyn KvClient>,
    ));
    let editor_err = editor
        .change_partition_state(0, PartitionState::PartitionPending)
        .await
        .unwrap_err();
    assert_eq!(
        editor_err.to_string(),
        "change partition state from Active to Pending: partition state change not allowed"
    );

    let ring = PartitionRing::new(desc).unwrap();
    let page_state = PartitionRingPageState {
        reader: Arc::new(StaticReader(ring)),
        editor,
    };
    let resp = handle_partition_ring_post(
        State(page_state.clone()),
        Request::builder()
            .method("POST")
            .uri("/ingester/partition-ring?action=change_state&partition_id=0&partition_state=PartitionPending")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8(body.to_vec()).unwrap(),
        "failed to change partition state: change partition state from Active to Pending: partition state change not allowed\n"
    );

    // Sanity: GET /ingester/partition-ring works on valid ring.
    let get_resp = handle_partition_ring_get(
        State(page_state),
        axum::http::HeaderMap::new(),
        axum::extract::RawQuery(None),
    )
    .await;
    assert_eq!(get_resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn w16_apc_08_zero_delete_after_still_calls_update_ring_and_records_kv_failures() {
    let reg = Registry::new();
    let failing_kv = Arc::new(FailingKv {
        cas_calls: AtomicUsize::new(0),
    });
    let plc = PartitionInstanceLifecycler::new(
        PartitionInstanceLifecyclerConfig {
            partition_id: 0,
            instance_id: "ing-0".to_string(),
            delete_inactive_partition_after: Duration::ZERO,
            ..Default::default()
        },
        "ingester-partitions",
        "ingester-partitions",
        Arc::clone(&failing_kv) as Arc<dyn KvClient>,
        Some(&Registerer::new(&reg)),
    )
    .unwrap();

    plc.reconcile_other_partitions(std::time::SystemTime::now())
        .await;
    assert_eq!(failing_kv.cas_calls.load(Ordering::SeqCst), 1);

    let mfs = reg.gather();
    let get_val = |metric_name: &str| -> f64 {
        mfs.iter()
            .find(|mf| mf.get_name() == metric_name)
            .and_then(|mf| {
                mf.get_metric().iter().find(|m| {
                    m.get_label()
                        .iter()
                        .any(|lp| lp.get_name() == "type" && lp.get_value() == "other-partitions")
                })
            })
            .map(|m| m.get_counter().get_value())
            .unwrap_or(0.0)
    };
    assert_eq!(get_val("partition_ring_lifecycler_reconciles_total"), 1.0);
    assert_eq!(
        get_val("partition_ring_lifecycler_reconciles_failed_total"),
        1.0
    );

    // Also verify with working KV that delete_after=0 does NOT delete inactive ownerless partitions.
    let kv = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    let mut desc = PartitionRingDesc::default();
    desc.add_partition(
        1,
        PartitionState::PartitionInactive,
        std::time::UNIX_EPOCH + Duration::from_secs(1),
    );
    kv.put_any("ingester-partitions", &desc).await.unwrap();

    let reg_ok = Registry::new();
    let plc_ok = PartitionInstanceLifecycler::new(
        PartitionInstanceLifecyclerConfig {
            partition_id: 0,
            instance_id: "ing-0".to_string(),
            delete_inactive_partition_after: Duration::ZERO,
            ..Default::default()
        },
        "ingester-partitions",
        "ingester-partitions",
        Arc::clone(&kv) as Arc<dyn KvClient>,
        Some(&Registerer::new(&reg_ok)),
    )
    .unwrap();
    plc_ok
        .reconcile_other_partitions(std::time::SystemTime::now())
        .await;
    let stored = kv
        .get("ingester-partitions")
        .await
        .unwrap()
        .unwrap()
        .downcast::<PartitionRingDesc>()
        .unwrap();
    assert!(stored.has_partition(1));
}

#[tokio::test]
async fn w16_apc_10_consul_canceled_watch_key_and_prefix_record_500_duration() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen_paths: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_paths_srv = Arc::clone(&seen_paths);

    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let seen = Arc::clone(&seen_paths_srv);
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                if let Ok(n) = sock.read(&mut buf).await {
                    let head = String::from_utf8_lossy(&buf[..n]);
                    let first_line = head.lines().next().unwrap_or("").to_string();
                    seen.lock().unwrap().push(first_line);
                }
                // Hold the long-poll open until client cancels.
                tokio::time::sleep(Duration::from_secs(30)).await;
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK
Content-Length: 2

[]")
                    .await;
            });
        }
    });

    let reg = Registry::new();
    let kv_reg = registerer_with_kv_name(Some(&Registerer::new(&reg)), "w16-consul");
    let client = Arc::new(
        ConsulKv::with_registerer(
            ConsulConfig {
                host: addr.to_string(),
                watch_rate_limit: 0.0,
                ..Default::default()
            },
            Arc::new(StringCodec),
            kv_reg.as_ref(),
        )
        .unwrap(),
    );

    // 1. Canceled watch_key -> operation="Get", status_code="500"
    let ctx_key = ServiceContext::background();
    let ctx_key_w = ctx_key.clone();
    let c1 = Arc::clone(&client);
    let h_key = tokio::spawn(async move {
        c1.watch_key("ring", ctx_key_w, Box::new(|_| true)).await;
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    ctx_key.cancel();
    h_key.await.unwrap();

    // 2. Canceled watch_prefix -> operation="List", status_code="500"
    let ctx_prefix = ServiceContext::background();
    let ctx_prefix_w = ctx_prefix.clone();
    let c2 = Arc::clone(&client);
    let h_prefix = tokio::spawn(async move {
        c2.watch_prefix("tenants/", ctx_prefix_w, Box::new(|_, _| true))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    ctx_prefix.cancel();
    h_prefix.await.unwrap();

    srv.abort();

    let count_for = |op: &str, status: &str| -> u64 {
        let mut total = 0u64;
        for mf in reg.gather() {
            if mf.get_name() != "consul_request_duration_seconds" {
                continue;
            }
            for m in mf.get_metric() {
                let mut has_op = false;
                let mut has_status = false;
                for lp in m.get_label() {
                    if lp.get_name() == "operation" && lp.get_value() == op {
                        has_op = true;
                    }
                    if lp.get_name() == "status_code" && lp.get_value() == status {
                        has_status = true;
                    }
                }
                if has_op && has_status {
                    total += m.get_histogram().get_sample_count();
                }
            }
        }
        total
    };

    assert_eq!(count_for("Get", "500"), 1);
    assert_eq!(count_for("List", "500"), 1);

    let _unused_map: HashMap<String, String> = HashMap::new();
}
