//! Wave 17 regression tests for ConsulKv/MultiKv observability & auth parity
//! (BUG-W17-KV-01..02) and Head compaction / idle TSDB close / flush lifecycle
//! parity (BUG-W17-CMP-01..03).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use axum::body::Bytes;
use axum::http::{Request, StatusCode};
use mimir_common::services::State;
use mimir_config::limits::LimitsConfig;
use mimir_config::{
    BlocksStorageConfig, ConsulConfig, GoDuration, IngestStorageConfig, IngesterConfig,
    ModelDuration, MultiKVConfig, Overrides,
};
use mimir_ingester::http::ingester_router;
use mimir_ingester::ingester::{Ingester, TriggerRequest};
use mimir_ingester::user_tsdb::TsdbCloseCheckResult;
use mimir_ring::kv::consul::{ConsulEnv, ConsulKv};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::metrics::{new_metrics_client, Registerer};
use mimir_ring::kv::multi::{MultiKv, NamedKvClient};
use mimir_ring::kv::{CasOutcome, KvClient, KvError, StringCodec};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::head::HeadOptions;
use mimir_tsdb::wlog::WriteFaultAction;
use prometheus::Registry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tower::ServiceExt;

fn test_ingester_with_blocks(
    tmp: &tempfile::TempDir,
    mut blocks_cfg: BlocksStorageConfig,
) -> Arc<Ingester> {
    blocks_cfg.tsdb.dir = tmp.path().join("tsdb").to_string_lossy().to_string();
    blocks_cfg.bucket.backend_cfg.backend = "filesystem".to_string();
    blocks_cfg.bucket.backend_cfg.filesystem.dir =
        tmp.path().join("bucket").to_string_lossy().to_string();
    Ingester::new(
        IngesterConfig::default(),
        blocks_cfg,
        IngestStorageConfig::default(),
        Overrides::new(LimitsConfig::default()),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("create test ingester")
}

fn b64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        out.push(TABLE[b0 >> 2] as char);
        out.push(TABLE[((b0 & 0x03) << 4) | (b1 >> 4)] as char);
        out.push(if chunk.len() > 1 {
            TABLE[((b1 & 0x0f) << 2) | (b2 >> 6)] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[b2 & 0x3f] as char
        } else {
            '='
        });
    }
    out
}

#[derive(Clone, Debug)]
struct MockHttpReq {
    request_line: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

async fn read_http_req<S: AsyncReadExt + Unpin>(sock: &mut S) -> Option<MockHttpReq> {
    let mut buf = Vec::new();
    let head_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        let mut tmp = [0u8; 4096];
        match sock.read(&mut tmp).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default().to_string();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    let content_len: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    buf.drain(..head_end + 4);
    while buf.len() < content_len {
        let mut tmp = [0u8; 4096];
        match sock.read(&mut tmp).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
    }
    let body = buf[..content_len].to_vec();
    Some(MockHttpReq {
        request_line,
        headers,
        body,
    })
}

fn histogram_count(reg: &Registry, name: &str, expected_labels: &[(&str, &str)]) -> u64 {
    for mf in reg.gather() {
        if mf.get_name() != name {
            continue;
        }
        for m in mf.get_metric() {
            let labels: Vec<(&str, &str)> = m
                .get_label()
                .iter()
                .map(|l| (l.get_name(), l.get_value()))
                .collect();
            if expected_labels.iter().all(|pair| labels.contains(pair)) {
                return m.get_histogram().get_sample_count();
            }
        }
    }
    0
}

fn counter_value(reg: &Registry, name: &str, expected_labels: &[(&str, &str)]) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() != name {
            continue;
        }
        for m in mf.get_metric() {
            let labels: Vec<(&str, &str)> = m
                .get_label()
                .iter()
                .map(|l| (l.get_name(), l.get_value()))
                .collect();
            if expected_labels.iter().all(|pair| labels.contains(pair)) {
                return m.get_counter().get_value();
            }
        }
    }
    0.0
}

#[tokio::test]
async fn bug_w17_kv_01_multikv_mirror_timeout_records_secondary_500_metrics() {
    // Secondary Consul server that stalls on GET so MultiKv mirror_timeout drops the CAS future mid-await.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let _ = read_http_req(&mut sock).await;
                tokio::time::sleep(Duration::from_secs(5)).await;
            });
        }
    });

    let reg = Registry::new();
    let r_wrap = Registerer::new(&reg);
    let r_pri = r_wrap.wrap_with([("role", "primary")]);
    let r_sec = r_wrap.wrap_with([("role", "secondary")]);

    let primary_raw: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(Arc::new(StringCodec)));
    let primary = new_metrics_client("inmemory", primary_raw, &r_pri).expect("primary metrics");

    let sec_consul = Arc::new(
        ConsulKv::with_registerer_and_env(
            ConsulConfig {
                host: format!("http://{addr}"),
                http_client_timeout: GoDuration::from_secs(10),
                ..Default::default()
            },
            Arc::new(StringCodec),
            Some(&r_sec),
            &ConsulEnv::default(),
        )
        .expect("consul kv"),
    );
    let secondary = new_metrics_client("consul", sec_consul, &r_sec).expect("sec metrics");

    let multi = MultiKv::new_with_options(
        MultiKVConfig {
            primary: "inmemory".to_string(),
            secondary: "consul".to_string(),
            mirror_enabled: true,
            mirror_timeout: GoDuration::from_millis(60),
        },
        vec![
            NamedKvClient {
                name: "inmemory".to_string(),
                client: primary,
            },
            NamedKvClient {
                name: "consul".to_string(),
                client: secondary,
            },
        ],
        Some(&r_wrap),
        Some(Arc::new(StringCodec)),
        None,
    );

    multi
        .cas(
            "collectors/ring",
            Box::new(|_| Ok(CasOutcome::Update(Box::new("val-1".to_string())))),
        )
        .await
        .expect("primary CAS must succeed even when secondary times out");

    assert_eq!(
        histogram_count(
            &reg,
            "kv_request_duration_seconds",
            &[("role", "primary"), ("operation", "CAS"), ("status_code", "200")]
        ),
        1
    );
    assert_eq!(
        histogram_count(
            &reg,
            "kv_request_duration_seconds",
            &[
                ("role", "secondary"),
                ("operation", "CAS"),
                ("status_code", "500")
            ]
        ),
        1,
        "dropped MetricsKvClient::cas future must record status_code=500"
    );
    assert_eq!(
        histogram_count(
            &reg,
            "consul_request_duration_seconds",
            &[
                ("role", "secondary"),
                ("operation", "CAS loop"),
                ("status_code", "500")
            ]
        ),
        1,
        "dropped ConsulKv::cas future must record CAS loop status_code=500"
    );
    assert!(
        histogram_count(
            &reg,
            "consul_request_duration_seconds",
            &[
                ("role", "secondary"),
                ("operation", "Get"),
                ("status_code", "500")
            ]
        ) >= 1,
        "dropped ConsulKv::get_raw future must record Get status_code=500"
    );

    server.abort();
}

#[tokio::test]
async fn bug_w17_kv_02_consul_whitespace_token_file_fallback_and_unix_socket() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // 1. Whitespace-only CONSUL_HTTP_TOKEN_FILE must fall back to CONSUL_HTTP_TOKEN.
    let tok_file = tmp.path().join("empty.token");
    std::fs::write(&tok_file, "   \n\t  \n").expect("write token file");

    let seen_tokens = Arc::new(Mutex::new(Vec::<Option<String>>::new()));
    let seen_tokens_c = Arc::clone(&seen_tokens);
    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let tcp_addr = tcp_listener.local_addr().expect("addr");
    let tcp_srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = tcp_listener.accept().await else {
                return;
            };
            let seen = Arc::clone(&seen_tokens_c);
            tokio::spawn(async move {
                while let Some(req) = read_http_req(&mut sock).await {
                    let tok = req
                        .headers
                        .iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case("x-consul-token"))
                        .map(|(_, v)| v.clone());
                    seen.lock().unwrap().push(tok);
                    let resp = "HTTP/1.1 404 Not Found\r\nX-Consul-Index: 1\r\nContent-Length: 0\r\n\r\n";
                    let _ = sock.write_all(resp.as_bytes()).await;
                }
            });
        }
    });

    let env = ConsulEnv {
        token: "fallback-secret-token".to_string(),
        token_file: tok_file.to_string_lossy().to_string(),
        ..Default::default()
    };
    let consul = ConsulKv::with_registerer_and_env(
        ConsulConfig {
            host: format!("http://{tcp_addr}"),
            ..Default::default()
        },
        Arc::new(StringCodec),
        None,
        &env,
    )
    .expect("consul with whitespace token file");
    let got = consul.get("k1").await.expect("get");
    assert!(got.is_none());
    assert_eq!(
        seen_tokens.lock().unwrap().as_slice(),
        &[Some("fallback-secret-token".to_string())]
    );
    tcp_srv.abort();

    // 2. Empty unix:// path must be rejected, and unix:///path/to/sock must communicate over Unix socket.
    let err = match ConsulKv::with_registerer_and_env(
        ConsulConfig {
            host: "unix://".to_string(),
            ..Default::default()
        },
        Arc::new(StringCodec),
        None,
        &ConsulEnv::default(),
    ) {
        Ok(_) => panic!("empty unix:// path must fail"),
        Err(e) => e,
    };
    assert!(matches!(err, KvError::InvalidConfig(_)), "unexpected err: {err}");

    let sock_path = tmp.path().join("consul.sock");
    let unix_listener = tokio::net::UnixListener::bind(&sock_path).expect("bind unix");
    let store: Arc<Mutex<HashMap<String, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
    let store_c = Arc::clone(&store);
    let unix_srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = unix_listener.accept().await else {
                return;
            };
            let store = Arc::clone(&store_c);
            tokio::spawn(async move {
                while let Some(req) = read_http_req(&mut sock).await {
                    let mut parts = req.request_line.split_whitespace();
                    let method = parts.next().unwrap_or("");
                    let path_q = parts.next().unwrap_or("");
                    let path = path_q.split('?').next().unwrap_or("");
                    let key = path.strip_prefix("/v1/kv/").unwrap_or(path).to_string();
                    let resp = match method {
                        "PUT" => {
                            store.lock().unwrap().insert(key, req.body);
                            "HTTP/1.1 200 OK\r\nX-Consul-Index: 2\r\nContent-Length: 4\r\n\r\ntrue".to_string()
                        }
                        "GET" => {
                            if let Some(val) = store.lock().unwrap().get(&key).cloned() {
                                let b64 = b64_encode(&val);
                                let json = format!(
                                    r#"[{{"Key":"{key}","CreateIndex":1,"ModifyIndex":2,"Value":"{b64}"}}]"#
                                );
                                format!(
                                    "HTTP/1.1 200 OK\r\nX-Consul-Index: 2\r\nContent-Length: {}\r\n\r\n{json}",
                                    json.len()
                                )
                            } else {
                                "HTTP/1.1 404 Not Found\r\nX-Consul-Index: 1\r\nContent-Length: 0\r\n\r\n".to_string()
                            }
                        }
                        _ => "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n".to_string(),
                    };
                    let _ = sock.write_all(resp.as_bytes()).await;
                }
            });
        }
    });

    let unix_consul = ConsulKv::with_registerer_and_env(
        ConsulConfig {
            host: format!("unix://{}", sock_path.display()),
            ..Default::default()
        },
        Arc::new(StringCodec),
        None,
        &ConsulEnv::default(),
    )
    .expect("unix consul");
    unix_consul
        .cas(
            "ring",
            Box::new(|_| Ok(CasOutcome::Update(Box::new("unix-ok".to_string())))),
        )
        .await
        .expect("unix cas");
    let got = unix_consul.get("ring").await.expect("unix get");
    let got_str = got
        .and_then(|v| v.downcast::<String>().ok())
        .map(|s| *s);
    assert_eq!(got_str.as_deref(), Some("unix-ok"));
    unix_srv.abort();
}

#[tokio::test]
async fn bug_w17_cmp_01_non_owned_ooo_only_series_evicted_after_compact_ooo_head() {
    // 1. Direct TSDB check: OOO-only series has 0 in-order chunks after compact_ooo_head();
    // compact_selected_series must still snapshot its fingerprint and evict it from Head.
    let tmp = tempfile::tempdir().expect("tempdir");
    let db = Tsdb::open(
        tmp.path(),
        TsdbOptions {
            head: HeadOptions {
                chunk_range_ms: 7_200_000,
                ooo_time_window_ms: 5_400_000,
                ..Default::default()
            },
            no_lockfile: true,
            ..Default::default()
        },
    )
    .expect("open tsdb");

    let s_in = [("__name__", "in_order_owned")];
    let s_ooo = [("__name__", "ooo_only_non_owned")];
    {
        let mut app = db.appender();
        app.append_float(None, &s_in, 10_000_000, 1.0)
            .expect("append in-order");
        app.commit().expect("commit in-order");
    }
    // New appender sees min_valid_time = 10_000_000 - 3_600_000 = 6_400_000,
    // so t = 5_000_000 is strictly OOO within ooo_time_window_ms = 5_400_000.
    let ooo_ref = {
        let mut app = db.appender();
        let r = app
            .append_float(None, &s_ooo, 5_000_000, 7.0)
            .expect("append ooo-only");
        app.commit().expect("commit ooo-only");
        r
    };

    assert_eq!(db.head().num_series(), 2);
    let ooo_blocks = db.compact_ooo_head().expect("compact_ooo_head");
    assert!(
        !ooo_blocks.is_empty(),
        "compact_ooo_head must persist OOO sample into an OOO block"
    );

    db.compact_selected_series(&[ooo_ref])
        .expect("compact_selected_series must evict OOO-only series");
    assert_eq!(
        db.head().num_series(),
        1,
        "non-owned OOO-only series must be evicted from Head"
    );
    db.close().expect("close");

    // 2. Full Ingester orchestrator check with explicit OOO-only series.
    let tmp2 = tempfile::tempdir().expect("tempdir2");
    let cfg = IngesterConfig {
        early_compaction_non_owned_series_enabled: true,
        early_compaction_non_owned_series_min_grace_period: GoDuration::from_millis(0),
        early_compaction_non_owned_series_max_grace_period: GoDuration::from_millis(1),
        ..Default::default()
    };
    let limits = LimitsConfig {
        out_of_order_time_window: ModelDuration(5400 * 1_000_000_000),
        early_head_compaction_owned_series_threshold: 1,
        ..Default::default()
    };

    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = tmp2.path().join("tsdb").to_string_lossy().to_string();
    blocks_cfg.bucket.backend_cfg.backend = "filesystem".to_string();
    blocks_cfg.bucket.backend_cfg.filesystem.dir =
        tmp2.path().join("bucket").to_string_lossy().to_string();

    let reg = Registry::new();
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        IngestStorageConfig::default(),
        Overrides::new(limits),
        None,
        None,
        None,
        None,
        None,
        Some(&reg),
    )
    .expect("ingester");

    let udb = ing.get_or_create_tsdb("t1").expect("user tsdb");
    {
        let mut app = udb.db.appender();
        app.append_float(None, &s_in, 10_000_000, 1.0)
            .expect("append in");
        app.commit().expect("commit in");
    }
    let ooo_ref2 = {
        let mut app = udb.db.appender();
        let r = app
            .append_float(None, &s_ooo, 5_000_000, 3.0)
            .expect("append ooo");
        app.commit().expect("commit ooo");
        r
    };

    let mut non_owned = HashSet::new();
    non_owned.insert(ooo_ref2);
    udb.add_pending_non_owned_refs(&non_owned);
    udb.backdate_pending_non_owned_refs(UNIX_EPOCH + Duration::from_secs(1));

    ing.compact_blocks_due_to_non_owned_series(Duration::ZERO)
        .await;

    assert_eq!(
        udb.db.head().num_series(),
        1,
        "Ingester non-owned compaction must evict OOO-only series"
    );
    assert_eq!(
        counter_value(
            &reg,
            "cortex_ingester_tsdb_early_compaction_non_owned_series_triggered_total",
            &[("user", "t1")]
        ),
        1.0
    );
}

#[tokio::test]
async fn bug_w17_cmp_02_close_idle_tsdb_serializes_with_compaction_and_preserves_series_count_on_error()
{
    // Part A: close_and_delete_user_tsdb_if_idle must preserve series_count when UserTsdb::close() fails.
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.close_idle_tsdb_when_shipping_disabled = true;
    let ing = test_ingester_with_blocks(&tmp, blocks_cfg);

    let udb = ing.get_or_create_tsdb("u-fail").expect("create u-fail");
    {
        let mut app = udb.db.appender();
        // Append across 2 block ranges so head_chunks_len() > 1 and Head::close() calls mmap_head_chunks().
        app.append_float(None, &[("__name__", "m1")], 1_000, 1.0)
            .expect("s1");
        app.append_float(None, &[("__name__", "m1")], 8_000_000, 2.0)
            .expect("s2");
        app.append_float(None, &[("__name__", "m2")], 8_000_000, 3.0)
            .expect("s3");
        app.commit().expect("commit");
    }
    assert_eq!(ing.series_count.load(Ordering::Relaxed), 2);
    udb.deletion_mark_found.store(true, Ordering::Release);

    // Inject a WAL write/flush fault so Head::close() -> wal.close() fails deterministically.
    udb.db.head().set_wal_write_fault_hook_for_test(Some(Arc::new(|_| {
        WriteFaultAction::Error(std::io::ErrorKind::Other)
    })));
    // Write one more sample so the WAL page buffer has unflushed bytes for wal.close() to flush.
    {
        let mut app = udb.db.appender();
        app.append_float(None, &[("__name__", "m1")], 8_001_000, 4.0)
            .expect("s4");
        // Ignore potential commit error if fault fires on commit; if commit succeeds, wal.close() flushes.
        let _ = app.commit();
    }

    let res = ing
        .close_and_delete_user_tsdb_if_idle("u-fail", Duration::from_secs(60))
        .await;
    assert_eq!(res, TsdbCloseCheckResult::CloseFailed);
    assert_eq!(
        ing.series_count.load(Ordering::Relaxed),
        2,
        "series_count must never underflow on close() error"
    );

    // Part B: Concurrent compaction + close_and_delete_user_tsdb_if_idle must serialize cleanly via compact_mtx
    // and remove the tenant directory without ENOTEMPTY / DataRemovalFailed.
    let udb2 = ing.get_or_create_tsdb("u-race").expect("create u-race");
    let u2_dir = udb2.db.dir().to_path_buf();
    {
        let mut app = udb2.db.appender();
        for i in 0..20 {
            let name = format!("race_{i}");
            let lbls = [("__name__", name.as_str())];
            app.append_float(None, &lbls, 1_000, i as f64).expect("r1");
            app.append_float(None, &lbls, 8_000_000, i as f64)
                .expect("r2");
        }
        app.commit().expect("commit");
    }
    assert_eq!(ing.series_count.load(Ordering::Relaxed), 22);
    udb2.deletion_mark_found.store(true, Ordering::Release);

    let db_compact = Arc::clone(&udb2.db);
    let compact_task = tokio::task::spawn_blocking(move || db_compact.compact());
    let close_res = ing
        .close_and_delete_user_tsdb_if_idle("u-race", Duration::from_secs(60))
        .await;
    let _ = compact_task.await.expect("compact join");

    assert_eq!(
        close_res,
        TsdbCloseCheckResult::TenantMarkedForDeletion,
        "close must serialize with compaction and remove dir cleanly"
    );
    assert!(!u2_dir.exists(), "closed TSDB dir must be completely removed");
    assert_eq!(ing.series_count.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn bug_w17_cmp_03_flush_re_reads_ship_tx_and_user_tsdb_sets_block_ranges() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.block_ranges_period = vec![
        GoDuration::from_secs(2 * 3600),
        GoDuration::from_secs(6 * 3600),
        GoDuration::from_secs(18 * 3600),
    ];
    let ing = test_ingester_with_blocks(&tmp, blocks_cfg);

    // 1. UserTsdb::open must propagate all block_ranges_period entries to Tsdb::set_block_ranges.
    let udb = ing.get_or_create_tsdb("u-ranges").expect("user tsdb");
    assert_eq!(
        udb.db.block_ranges(),
        vec![7_200_000, 21_600_000, 64_800_000],
        "UserTsdb::open must preserve multi-range block_ranges_period"
    );

    // 2. POST /ingester/flush?wait=true while State::Starting must re-read ship_tx AFTER compaction completes.
    ing.set_state_for_test(State::Starting);
    ing.set_compaction_service_state(State::Running);
    let (fc_tx, mut fc_rx) = mpsc::channel::<TriggerRequest>(4);
    *ing.force_compact_tx.write() = Some(fc_tx);
    assert!(ing.ship_tx.read().is_none());

    let (ship_tx, mut ship_rx) = mpsc::channel::<TriggerRequest>(4);
    let ship_called = Arc::new(AtomicBool::new(false));
    let ship_called_c = Arc::clone(&ship_called);

    let ing_c = Arc::clone(&ing);
    let compact_actor = tokio::spawn(async move {
        if let Some(req) = fc_rx.recv().await {
            // Simulate ingester finishing startup and populating ship_tx during compaction!
            *ing_c.ship_tx.write() = Some(ship_tx);
            let _ = req.callback.send(());
        }
    });
    let ship_actor = tokio::spawn(async move {
        if let Some(req) = ship_rx.recv().await {
            ship_called_c.store(true, Ordering::Release);
            let _ = req.callback.send(());
        }
    });

    let app = ingester_router(Arc::clone(&ing));
    let req = Request::builder()
        .method("POST")
        .uri("/ingester/flush?wait=true")
        .body(axum::body::Body::from(Bytes::new()))
        .expect("req");
    let resp = app.oneshot(req).await.expect("flush resp");
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    compact_actor.await.expect("compact actor");
    ship_actor.await.expect("ship actor");
    assert!(
        ship_called.load(Ordering::Acquire),
        "flush_handler must re-read ship_tx after compaction and trigger shipping"
    );
}
