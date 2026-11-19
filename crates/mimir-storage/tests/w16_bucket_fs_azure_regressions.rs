//! Wave 16 regressions for BucketClient (filesystem, azure, metrics, get_range).
//!
//! Covers:
//! - W16-APC-03: Filesystem bucket cleans empty parent directories on delete and skips empty
//!   directories in iter_dir
//! - W16-APC-04: Azure IsObjNotFoundErr maps 400 InvalidUri (header & XML) to NotFound
//! - W16-APC-05: Azure multipart PutBlockList emits <Latest> instead of <Uncommitted>
//! - W16-APC-09: BucketClient with_expected_not_found() suppresses failure metrics on attributes()
//!   and delete_strict(); get_range(name, off, 0) checks existence and returns Ok(Bytes::new())

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use mimir_config::AzureConfig;
use mimir_storage::bucket::{azure, metrics, BucketClient, BucketError};
use prometheus::Registry;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn metric_counter_value(reg: &Registry, name: &str, op: &str) -> u64 {
    for mf in reg.gather() {
        if mf.get_name() != name {
            continue;
        }
        for m in mf.get_metric() {
            if m.get_label()
                .iter()
                .any(|lp| lp.get_name() == "operation" && lp.get_value() == op)
            {
                return m.get_counter().get_value() as u64;
            }
        }
    }
    0
}

#[tokio::test]
async fn w16_apc_03_filesystem_cleans_empty_dirs_and_skips_empty_dirs_in_iter() {
    let dir = TempDir::new().unwrap();
    let root = dir.path();
    let bkt = BucketClient::new_filesystem(root).unwrap();

    bkt.upload("tenant-a/01JBLOCK/chunks/000001", Bytes::from_static(b"chunk"))
        .await
        .unwrap();
    bkt.upload("tenant-a/01JBLOCK/meta.json", Bytes::from_static(b"{}"))
        .await
        .unwrap();
    bkt.upload("tenant-b/keep.txt", Bytes::from_static(b"keep"))
        .await
        .unwrap();

    assert_eq!(bkt.iter_dir("tenant-a").await.unwrap(), vec!["tenant-a/01JBLOCK/"]);

    // Delete all objects under tenant-a/01JBLOCK -> automatic cleanup must remove empty parents.
    let deleted = bkt.delete_prefix("tenant-a/01JBLOCK").await.unwrap();
    assert_eq!(deleted, 2);

    assert!(
        !root.join("tenant-a/01JBLOCK").exists(),
        "empty block dir must be removed on delete"
    );
    assert!(
        !root.join("tenant-a").exists(),
        "empty tenant dir must be removed on delete"
    );
    assert_eq!(bkt.iter_dir("tenant-a").await.unwrap(), Vec::<String>::new());
    assert_eq!(bkt.iter_dir("").await.unwrap(), vec!["tenant-b/"]);

    // Pre-existing empty directories on disk must also be skipped by iter_dir.
    std::fs::create_dir_all(root.join("orphan-empty-dir")).unwrap();
    std::fs::create_dir_all(root.join("tenant-b/empty-sub")).unwrap();
    assert_eq!(bkt.iter_dir("").await.unwrap(), vec!["tenant-b/"]);
    assert_eq!(
        bkt.iter_dir("tenant-b").await.unwrap(),
        vec!["tenant-b/keep.txt"]
    );

    // Deleting the last file in tenant-b removes tenant-b when empty (non-empty empty-sub remains
    // until deleted; delete empty-sub then verify).
    std::fs::remove_dir(root.join("tenant-b/empty-sub")).unwrap();
    bkt.delete("tenant-b/keep.txt").await.unwrap();
    assert!(!root.join("tenant-b").exists());
    assert_eq!(bkt.iter_dir("").await.unwrap(), Vec::<String>::new());
}

struct MockHttpRequest {
    method: String,
    target: String,
    body: String,
}

async fn read_one_http_request(sock: &mut tokio::net::TcpStream) -> Option<MockHttpRequest> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let hdr_end = loop {
        let n = sock.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
    };
    let head = String::from_utf8_lossy(&buf[..hdr_end]).to_string();
    let req_line = head.lines().next().unwrap_or("");
    let mut parts = req_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();

    let mut content_len = 0usize;
    for l in head.lines().skip(1) {
        if let Some((k, v)) = l.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_len = v.trim().parse().unwrap_or(0);
            }
        }
    }
    let body_start = hdr_end + 4;
    while buf.len() < body_start + content_len {
        let n = sock.read(&mut tmp).await.ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let body = String::from_utf8_lossy(&buf[body_start..(body_start + content_len).min(buf.len())])
        .to_string();
    Some(MockHttpRequest {
        method,
        target,
        body,
    })
}

#[tokio::test]
async fn w16_apc_04_azure_400_invalid_uri_and_blob_not_found_map_to_not_found() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                while let Some(req) = read_one_http_request(&mut sock).await {
                    if req.target.contains("restype=container") {
                        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                        let _ = sock.write_all(resp.as_bytes()).await;
                        continue;
                    }
                    if req.target.contains("xml-invalid-uri") {
                        let body = "<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>InvalidUri</Code><Message>The requested URI does not represent any resource on the server.</Message></Error>";
                        let resp = format!(
                            "HTTP/1.1 400 Bad Request\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                        continue;
                    }
                    if req.target.contains("blob-not-found") {
                        let resp = "HTTP/1.1 400 Bad Request\r\nx-ms-error-code: BlobNotFound\r\nContent-Length: 0\r\n\r\n";
                        let _ = sock.write_all(resp.as_bytes()).await;
                        continue;
                    }
                    // Header-only 400 InvalidUri (matches HEAD / GET on invalid URI)
                    let resp = "HTTP/1.1 400 Bad Request\r\nx-ms-error-code: InvalidUri\r\nContent-Length: 0\r\n\r\n";
                    let _ = sock.write_all(resp.as_bytes()).await;
                }
            });
        }
    });

    let conn_str = format!(
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{addr}/devstoreaccount1;"
    );
    let store = tokio::task::spawn_blocking(move || {
        azure::new_store(&AzureConfig {
            account_name: "devstoreaccount1".into(),
            container_name: "blocks".into(),
            connection_string: conn_str,
            ..Default::default()
        })
    })
    .await
    .unwrap()
    .unwrap();

    let reg = Registry::new();
    let bkt = BucketClient::from_object_store(store, "blocks", "AZURE");
    // Wrap via BucketConfig so metrics are attached on a second client too, or test metrics via
    // BucketClient::new_client.
    drop(bkt);

    let cfg = mimir_config::BucketConfig {
        backend_cfg: mimir_config::StorageBackendConfig {
            backend: "azure".to_string(),
            azure: AzureConfig {
                account_name: "devstoreaccount1".into(),
                container_name: "blocks".into(),
                connection_string: format!(
                    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{addr}/devstoreaccount1;"
                ),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let reg_cloned = reg.clone();
    let bkt = tokio::task::spawn_blocking(move || {
        BucketClient::new_client(&cfg, "ingester", Some(&reg_cloned))
    })
    .await
    .unwrap()
    .unwrap();

    // 1. exists() -> Ok(false) on 400 x-ms-error-code: InvalidUri
    assert!(!bkt.exists("bad/key").await.unwrap());
    assert!(!bkt.exists("blob-not-found").await.unwrap());

    // 2. download() & attributes() -> NotFound on header and XML fallback
    let dl_err = bkt.download("bad/key").await.unwrap_err();
    assert!(dl_err.is_not_found(), "expected NotFound, got {dl_err:?}");

    let xml_err = bkt.download("xml-invalid-uri").await.unwrap_err();
    assert!(xml_err.is_not_found(), "expected NotFound, got {xml_err:?}");

    let attr_err = bkt.attributes("bad/key").await.unwrap_err();
    assert!(attr_err.is_not_found(), "expected NotFound, got {attr_err:?}");

    // 3. with_expected_not_found() suppresses failure metrics on 400 InvalidUri
    let before_get_failures =
        metric_counter_value(&reg, "thanos_objstore_bucket_operation_failures_total", metrics::OP_GET);
    let before_attr_failures = metric_counter_value(
        &reg,
        "thanos_objstore_bucket_operation_failures_total",
        metrics::OP_ATTRIBUTES,
    );
    let expected = bkt.with_expected_not_found();
    assert!(expected.download("bad/key").await.unwrap_err().is_not_found());
    assert!(expected.attributes("bad/key").await.unwrap_err().is_not_found());
    assert_eq!(
        metric_counter_value(&reg, "thanos_objstore_bucket_operation_failures_total", metrics::OP_GET),
        before_get_failures
    );
    assert_eq!(
        metric_counter_value(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            metrics::OP_ATTRIBUTES,
        ),
        before_attr_failures
    );

    srv.abort();
}

#[tokio::test]
async fn w16_apc_05_azure_multipart_put_block_list_uses_latest_and_survives_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let blocklist_bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let blocklist_calls = Arc::new(AtomicUsize::new(0));
    let block_calls = Arc::new(AtomicUsize::new(0));

    let blocklist_bodies_srv = Arc::clone(&blocklist_bodies);
    let blocklist_calls_srv = Arc::clone(&blocklist_calls);
    let block_calls_srv = Arc::clone(&block_calls);

    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let bl_bodies = Arc::clone(&blocklist_bodies_srv);
            let bl_calls = Arc::clone(&blocklist_calls_srv);
            let b_calls = Arc::clone(&block_calls_srv);
            tokio::spawn(async move {
                while let Some(req) = read_one_http_request(&mut sock).await {
                    if req.target.contains("restype=container") {
                        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                        let _ = sock.write_all(resp.as_bytes()).await;
                    } else if req.method == "PUT" && req.target.contains("comp=blocklist") {
                        let n = bl_calls.fetch_add(1, Ordering::SeqCst);
                        bl_bodies.lock().unwrap().push(req.body);
                        if n == 0 {
                            // Simulate transient 503 after server processed PutBlockList.
                            let resp = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
                            let _ = sock.write_all(resp.as_bytes()).await;
                        } else {
                            let resp = "HTTP/1.1 201 Created\r\nETag: \"etag-1\"\r\nContent-Length: 0\r\n\r\n";
                            let _ = sock.write_all(resp.as_bytes()).await;
                        }
                    } else if req.method == "PUT" && req.target.contains("comp=block") {
                        b_calls.fetch_add(1, Ordering::SeqCst);
                        let resp = "HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n";
                        let _ = sock.write_all(resp.as_bytes()).await;
                    } else {
                        let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
                        let _ = sock.write_all(resp.as_bytes()).await;
                    }
                }
            });
        }
    });

    let conn_str = format!(
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{addr}/devstoreaccount1;"
    );
    let store = tokio::task::spawn_blocking(move || {
        azure::new_store(&AzureConfig {
            account_name: "devstoreaccount1".into(),
            container_name: "blocks".into(),
            connection_string: conn_str,
            ..Default::default()
        })
    })
    .await
    .unwrap()
    .unwrap();

    // Upload > 16 MiB file so BucketClient::upload_file triggers store_upload multipart path.
    let tmp = TempDir::new().unwrap();
    let file_path = tmp.path().join("big-chunk");
    std::fs::write(&file_path, vec![b'x'; 17 * 1024 * 1024]).unwrap();

    let bkt = BucketClient::from_object_store(store, "blocks", "AZURE");
    bkt.upload_file("01JBLOCK/chunks/000001", &file_path)
        .await
        .unwrap();

    srv.abort();

    assert!(block_calls.load(Ordering::SeqCst) >= 3);
    assert_eq!(blocklist_calls.load(Ordering::SeqCst), 2);
    let bodies = blocklist_bodies.lock().unwrap();
    assert_eq!(bodies.len(), 2);
    for xml in bodies.iter() {
        assert!(
            xml.contains("<Latest>") && xml.contains("</Latest>"),
            "PutBlockList XML must use <Latest>, got: {xml}"
        );
        assert!(
            !xml.contains("<Uncommitted>"),
            "PutBlockList XML must not emit <Uncommitted>, got: {xml}"
        );
    }
}

#[tokio::test]
async fn w16_apc_09_expected_not_found_and_zero_length_get_range() {
    let dir = TempDir::new().unwrap();
    let cfg = mimir_config::BucketConfig {
        backend_cfg: mimir_config::StorageBackendConfig {
            backend: "filesystem".to_string(),
            filesystem: mimir_config::FilesystemConfig {
                dir: dir.path().to_string_lossy().into_owned(),
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let reg = Registry::new();
    let fs_bkt = BucketClient::new_client(&cfg, "ingester", Some(&reg)).unwrap();
    let mem_bkt = BucketClient::new_in_memory("mem");

    // 1. with_expected_not_found() suppresses failure metrics on attributes() and delete_strict().
    let expected = fs_bkt.with_expected_not_found();
    assert!(matches!(
        expected.attributes("missing.bin").await,
        Err(BucketError::NotFound(_))
    ));
    assert!(matches!(
        expected.delete_strict("missing.bin").await,
        Err(BucketError::NotFound(_))
    ));
    assert_eq!(
        metric_counter_value(&reg, "thanos_objstore_bucket_operations_total", metrics::OP_ATTRIBUTES),
        1
    );
    assert_eq!(
        metric_counter_value(&reg, "thanos_objstore_bucket_operations_total", metrics::OP_DELETE),
        1
    );
    assert_eq!(
        metric_counter_value(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            metrics::OP_ATTRIBUTES,
        ),
        0
    );
    assert_eq!(
        metric_counter_value(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            metrics::OP_DELETE,
        ),
        0
    );

    // Without with_expected_not_found(), NotFound DOES increment failure counters.
    assert!(matches!(
        fs_bkt.attributes("missing.bin").await,
        Err(BucketError::NotFound(_))
    ));
    assert!(matches!(
        fs_bkt.delete_strict("missing.bin").await,
        Err(BucketError::NotFound(_))
    ));
    assert_eq!(
        metric_counter_value(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            metrics::OP_ATTRIBUTES,
        ),
        1
    );
    assert_eq!(
        metric_counter_value(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            metrics::OP_DELETE,
        ),
        1
    );

    // 2. get_range(name, off, 0) returns Ok(Bytes::new()) on existing object and NotFound on missing.
    for bkt in [&fs_bkt, &mem_bkt] {
        bkt.upload("obj.txt", Bytes::from_static(b"hello world"))
            .await
            .unwrap();
        assert_eq!(bkt.get_range("obj.txt", 0, 0).await.unwrap(), Bytes::new());
        assert_eq!(bkt.get_range("obj.txt", 5, 0).await.unwrap(), Bytes::new());
        assert_eq!(bkt.get_range("obj.txt", 100, 0).await.unwrap(), Bytes::new());
        assert!(matches!(
            bkt.get_range("missing.txt", 0, 0).await,
            Err(BucketError::NotFound(_))
        ));
    }

    assert_eq!(
        metric_counter_value(&reg, "thanos_objstore_bucket_operations_total", metrics::OP_GET_RANGE),
        4
    );
    assert_eq!(
        metric_counter_value(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            metrics::OP_GET_RANGE,
        ),
        1
    );
}
