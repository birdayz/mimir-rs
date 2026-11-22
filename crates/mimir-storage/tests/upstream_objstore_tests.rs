//! 1:1 upstream test ports for `github.com/thanos-io/objstore` (9 files, 53 tests).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use futures::stream::BoxStream;
use mimir_config::{
    parse_cli_and_load_config_test_mode, BucketConfig, BucketHTTPConfig, GoDuration, S3Config,
    S3SSEConfig,
};
use mimir_storage::bucket::metrics::{
    OP_ATTRIBUTES, OP_DELETE, OP_EXISTS, OP_GET, OP_GET_RANGE, OP_ITER, OP_UPLOAD,
};
use mimir_storage::bucket::s3::{
    build_thanos_sse, S3Bucket, ServerSideEncryption, SSE_KMS, SSE_S3,
};
use mimir_storage::bucket::{
    azure, exthttp, gcs, BucketClient, DownloadDirOptions, IterOptions, UploadDirOptions,
};
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use prometheus::Registry;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_LENGTH, LAST_MODIFIED};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn load_blocks_bucket_from_yaml(
    yaml_body: &str,
) -> Result<(BucketConfig, Option<String>), mimir_config::ConfigError> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("config.yaml");
    let full = format!("target: ingester\nblocks_storage:\n{yaml_body}");
    std::fs::write(&path, full).unwrap();
    let loaded = parse_cli_and_load_config_test_mode(
        "mimir",
        &[format!("-config.file={}", path.display())],
    )?;
    Ok((loaded.config.blocks_storage.bucket, loaded.validation_error))
}

fn indent_yaml(raw: &str) -> String {
    raw.lines()
        .map(|l| {
            if l.trim().is_empty() {
                String::new()
            } else {
                format!("  {l}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn metric_counter(reg: &Registry, name: &str, op: &str) -> f64 {
    reg.gather()
        .iter()
        .filter(|mf| mf.get_name() == name)
        .flat_map(|mf| mf.get_metric())
        .filter(|m| {
            m.get_label()
                .iter()
                .any(|lp| lp.get_name() == "operation" && lp.get_value() == op)
        })
        .map(|m| m.get_counter().get_value())
        .sum()
}

fn count_series(reg: &Registry, name: &str) -> usize {
    reg.gather()
        .iter()
        .filter(|mf| mf.get_name() == name)
        .map(|mf| mf.get_metric().len())
        .sum()
}

fn metric_gauge(reg: &Registry, name: &str) -> f64 {
    reg.gather()
        .iter()
        .filter(|mf| mf.get_name() == name)
        .flat_map(|mf| mf.get_metric())
        .map(|m| m.get_gauge().get_value())
        .sum()
}

fn metric_histogram_count_and_sum(reg: &Registry, name: &str, op: &str) -> (u64, f64, Vec<(f64, u64)>) {
    for mf in reg.gather() {
        if mf.get_name() != name {
            continue;
        }
        for m in mf.get_metric() {
            if m.get_label()
                .iter()
                .any(|lp| lp.get_name() == "operation" && lp.get_value() == op)
            {
                let h = m.get_histogram();
                let buckets = h
                    .get_bucket()
                    .iter()
                    .map(|b| (b.get_upper_bound(), b.get_cumulative_count()))
                    .collect();
                return (h.get_sample_count(), h.get_sample_sum(), buckets);
            }
        }
    }
    (0, 0.0, Vec::new())
}

#[derive(Default)]
struct AcceptanceStats {
    attributes_count: usize,
    get_count: usize,
    upload_count: usize,
    delete_count: usize,
    failed_upload_count: usize,
}

async fn acceptance_test(bkt: &BucketClient, large_payload_bytes: usize) -> AcceptanceStats {
    let err = bkt.download("").await.unwrap_err();
    assert!(!err.is_not_found(), "expected user error got not found {err}");

    let err = bkt.download("id1/obj_1.some").await.unwrap_err();
    assert!(err.is_not_found(), "expected not found error got {err}");

    assert!(!bkt.exists("id1/obj_1.some").await.unwrap());

    let err = bkt.attributes("id1/obj_1.some").await.unwrap_err();
    assert!(err.is_not_found(), "expected not found error got {err}");

    // Upload first object.
    bkt.upload("id1/obj_1.some", Bytes::from_static(b"@test-data@"))
        .await
        .unwrap();

    let got = bkt.download("id1/obj_1.some").await.unwrap();
    assert_eq!(got.len(), 11);
    assert_eq!(got.as_ref(), b"@test-data@");

    let attrs = bkt.attributes("id1/obj_1.some").await.unwrap();
    assert_eq!(attrs.size, 11);

    let r2 = bkt.get_range("id1/obj_1.some", 1, 3).await.unwrap();
    assert_eq!(r2.len(), 3);
    assert_eq!(r2.as_ref(), b"tes");

    let r_unspec = bkt.get_range_from("id1/obj_1.some", 1).await.unwrap();
    assert_eq!(r_unspec.len(), 10);
    assert_eq!(r_unspec.as_ref(), b"test-data@");

    let r_len = bkt.get_range("id1/obj_1.some", 3, 9999).await.unwrap();
    assert_eq!(r_len.len(), 8);
    assert_eq!(r_len.as_ref(), b"st-data@");

    assert!(bkt.exists("id1/obj_1.some").await.unwrap());

    // Upload other objects.
    bkt.upload("id1/obj_2.some", Bytes::from_static(b"@test-data2@"))
        .await
        .unwrap();
    bkt.upload("id1/obj_2.some", Bytes::from_static(b"@test-data2@"))
        .await
        .unwrap();
    bkt.upload("id1/obj_3.some", Bytes::from_static(b"@test-data3@"))
        .await
        .unwrap();
    bkt.upload(
        "id1/sub/subobj_1.some",
        Bytes::from_static(b"@test-data4@"),
    )
    .await
    .unwrap();
    bkt.upload(
        "id1/sub/subobj_2.some",
        Bytes::from_static(b"@test-data5@"),
    )
    .await
    .unwrap();
    bkt.upload("id2/obj_4.some", Bytes::from_static(b"@test-data6@"))
        .await
        .unwrap();
    bkt.upload("obj_5.some", Bytes::from_static(b"@test-data7@"))
        .await
        .unwrap();

    // Iter top dir
    let mut seen = bkt.iter_dir("").await.unwrap();
    seen.sort();
    let mut expected = vec!["obj_5.some", "id1/", "id2/"];
    expected.sort();
    assert_eq!(seen, expected);

    // Iter top dir recursive
    let mut seen = bkt.list_all("").await.unwrap();
    seen.sort();
    let mut expected = vec![
        "id1/obj_1.some",
        "id1/obj_2.some",
        "id1/obj_3.some",
        "id1/sub/subobj_1.some",
        "id1/sub/subobj_2.some",
        "id2/obj_4.some",
        "obj_5.some",
    ];
    expected.sort();
    assert_eq!(seen, expected);

    // Iter id1/
    assert_eq!(
        bkt.iter_dir("id1/").await.unwrap(),
        vec![
            "id1/obj_1.some",
            "id1/obj_2.some",
            "id1/obj_3.some",
            "id1/sub/",
        ]
    );

    // Iter id1/ recursive
    assert_eq!(
        bkt.list_all("id1/").await.unwrap(),
        vec![
            "id1/obj_1.some",
            "id1/obj_2.some",
            "id1/obj_3.some",
            "id1/sub/subobj_1.some",
            "id1/sub/subobj_2.some",
        ]
    );

    // Iter id1
    assert_eq!(
        bkt.iter_dir("id1").await.unwrap(),
        vec![
            "id1/obj_1.some",
            "id1/obj_2.some",
            "id1/obj_3.some",
            "id1/sub/",
        ]
    );

    // Iter id1 recursive
    assert_eq!(
        bkt.list_all("id1").await.unwrap(),
        vec![
            "id1/obj_1.some",
            "id1/obj_2.some",
            "id1/obj_3.some",
            "id1/sub/subobj_1.some",
            "id1/sub/subobj_2.some",
        ]
    );

    // Non-existing dir
    assert!(bkt.iter_dir("id0").await.unwrap().is_empty());

    bkt.delete("id1/obj_2.some").await.unwrap();
    assert_eq!(
        bkt.iter_dir("id1/").await.unwrap(),
        vec!["id1/obj_1.some", "id1/obj_3.some", "id1/sub/"]
    );

    bkt.delete("id2/obj_4.some").await.unwrap();
    let mut seen = bkt.iter_dir("").await.unwrap();
    seen.sort();
    let mut expected = vec!["obj_5.some", "id1/"];
    expected.sort();
    assert_eq!(seen, expected);

    bkt.upload("obj_6.som", Bytes::from(vec![0u8; large_payload_bytes]))
        .await
        .unwrap();
    bkt.delete("obj_6.som").await.unwrap();

    AcceptanceStats {
        attributes_count: 2,
        get_count: 3,
        upload_count: 9,
        delete_count: 3,
        failed_upload_count: 0,
    }
}

// ===========================================================================
// clientutil/parse_test.go (2)
// ===========================================================================

// Upstream: github.com/thanos-io/objstore/clientutil/parse_test.go::TestParseLastModified
#[test]
fn test_clientutil_parse_last_modified() {
    let cases = [
        ("no header", "", "", true, None),
        (
            "empty format string to default RFC3339 format",
            "2015-11-06T10:07:11.000Z",
            "",
            false,
            Some("2015-11-06T10:07:11Z"),
        ),
        (
            "valid RFC3339 header value",
            "2015-11-06T10:07:11.000Z",
            "2006-01-02T15:04:05Z07:00",
            false,
            Some("2015-11-06T10:07:11Z"),
        ),
        (
            "invalid RFC3339 header value",
            "invalid",
            "2006-01-02T15:04:05Z07:00",
            true,
            None,
        ),
        (
            "valid RFC1123 header value",
            "Fri, 24 Feb 2012 06:07:48 GMT",
            "Mon, 02 Jan 2006 15:04:05 MST",
            false,
            Some("2012-02-24T06:07:48Z"),
        ),
        (
            "invalid RFC1123 header value",
            "invalid",
            "Mon, 02 Jan 2006 15:04:05 MST",
            true,
            None,
        ),
    ];

    for (name, hv, fmt, want_err, expected_rfc3339) in cases {
        let mut h = HeaderMap::new();
        if !hv.is_empty() {
            h.insert(LAST_MODIFIED, HeaderValue::from_str(hv).unwrap());
        }
        let res = exthttp::parse_last_modified(&h, fmt);
        if want_err {
            let err = res.unwrap_err();
            if hv.is_empty() {
                assert_eq!(err, "Last-Modified header not found", "{name}");
            } else {
                assert!(err.starts_with("parse Last-Modified:"), "{name}: {err}");
            }
        } else {
            let got = res.unwrap().with_timezone(&chrono::Utc);
            let want = chrono::DateTime::parse_from_rfc3339(expected_rfc3339.unwrap())
                .unwrap()
                .with_timezone(&chrono::Utc);
            assert_eq!(got, want, "{name}");
        }
    }
}

// Upstream: github.com/thanos-io/objstore/clientutil/parse_test.go::TestParseContentLength
#[test]
fn test_clientutil_parse_content_length() {
    let mut empty = HeaderMap::new();
    assert_eq!(
        exthttp::parse_content_length(&empty).unwrap_err(),
        "Content-Length header not found"
    );

    empty.insert(CONTENT_LENGTH, HeaderValue::from_static("invalid"));
    let err = exthttp::parse_content_length(&empty).unwrap_err();
    assert!(err.starts_with("convert Content-Length:"), "{err}");

    let mut valid = HeaderMap::new();
    valid.insert(CONTENT_LENGTH, HeaderValue::from_static("12345"));
    assert_eq!(exthttp::parse_content_length(&valid).unwrap(), 12345);
}

// ===========================================================================
// exthttp/parse_test.go (2)
// ===========================================================================

// Upstream: github.com/thanos-io/objstore/exthttp/parse_test.go::TestParseLastModified
#[test]
fn test_exthttp_parse_last_modified() {
    let mut h = HeaderMap::new();
    assert_eq!(
        exthttp::parse_last_modified(&h, "").unwrap_err(),
        "Last-Modified header not found"
    );

    h.insert(
        LAST_MODIFIED,
        HeaderValue::from_static("2015-11-06T10:07:11.000Z"),
    );
    let parsed = exthttp::parse_last_modified(&h, "").unwrap();
    assert_eq!(
        parsed.with_timezone(&chrono::Utc).to_rfc3339(),
        "2015-11-06T10:07:11+00:00"
    );

    h.insert(
        LAST_MODIFIED,
        HeaderValue::from_static("Fri, 24 Feb 2012 06:07:48 GMT"),
    );
    let parsed = exthttp::parse_last_modified(&h, "Mon, 02 Jan 2006 15:04:05 MST").unwrap();
    assert_eq!(
        parsed.with_timezone(&chrono::Utc).to_rfc3339(),
        "2012-02-24T06:07:48+00:00"
    );

    h.insert(LAST_MODIFIED, HeaderValue::from_static("invalid"));
    assert!(exthttp::parse_last_modified(&h, "2006-01-02T15:04:05Z07:00")
        .unwrap_err()
        .starts_with("parse Last-Modified:"));
    assert!(exthttp::parse_last_modified(&h, "Mon, 02 Jan 2006 15:04:05 MST")
        .unwrap_err()
        .starts_with("parse Last-Modified:"));
}

// Upstream: github.com/thanos-io/objstore/exthttp/parse_test.go::TestParseContentLength
#[test]
fn test_exthttp_parse_content_length() {
    let mut h = HeaderMap::new();
    assert_eq!(
        exthttp::parse_content_length(&h).unwrap_err(),
        "Content-Length header not found"
    );
    h.insert(CONTENT_LENGTH, HeaderValue::from_static("invalid"));
    assert!(exthttp::parse_content_length(&h)
        .unwrap_err()
        .starts_with("convert Content-Length:"));
    h.insert(CONTENT_LENGTH, HeaderValue::from_static("12345"));
    assert_eq!(exthttp::parse_content_length(&h).unwrap(), 12345);
}

// ===========================================================================
// inmem_test.go (1)
// ===========================================================================

// Upstream: github.com/thanos-io/objstore/inmem_test.go::TestInMem_ReturnsModifiedInIterAttributes
#[tokio::test]
async fn test_inmem_returns_modified_in_iter_attributes() {
    let b = BucketClient::new_in_memory("test");
    b.upload("test/file1.txt", Bytes::from_static(b"test-data1"))
        .await
        .unwrap();

    let mut items_iterated = 0usize;

    let non_rec = b
        .iter_with_attributes("", IterOptions::updated_at())
        .await
        .unwrap();
    assert_eq!(non_rec.len(), 1);
    assert_eq!(non_rec[0].name, "test/");
    assert!(non_rec[0].last_modified.is_some());
    assert!(non_rec[0].last_modified.unwrap() > SystemTime::UNIX_EPOCH);
    items_iterated += 1;

    let rec = b
        .iter_with_attributes("", IterOptions::updated_at().with_recursive(true))
        .await
        .unwrap();
    assert_eq!(rec.len(), 1);
    assert_eq!(rec[0].name, "test/file1.txt");
    assert!(rec[0].last_modified.is_some());
    assert!(rec[0].last_modified.unwrap() > SystemTime::UNIX_EPOCH);
    items_iterated += 1;

    assert_eq!(items_iterated, 2);
}

// ===========================================================================
// objstore_test.go (13)
// ===========================================================================

// Upstream: github.com/thanos-io/objstore/objstore_test.go::TestMetricBucket_Close
#[tokio::test]
async fn test_metric_bucket_close() {
    let reg = Registry::new();
    let bkt = BucketClient::new_in_memory("abc")
        .with_metrics(&reg, "test")
        .unwrap();

    assert_eq!(
        count_series(&reg, "thanos_objstore_bucket_operations_total"),
        7
    );
    assert_eq!(
        count_series(&reg, "thanos_objstore_bucket_operation_failures_total"),
        7
    );
    assert_eq!(
        count_series(&reg, "thanos_objstore_bucket_operation_duration_seconds"),
        7
    );

    let stats = acceptance_test(&bkt.with_expected_not_found(), 17 * 1024 * 1024).await;
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_ITER),
        9.0
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_ATTRIBUTES),
        stats.attributes_count as f64
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_GET),
        stats.get_count as f64
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_GET_RANGE),
        3.0
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_EXISTS),
        2.0
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_UPLOAD),
        stats.upload_count as f64
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_DELETE),
        stats.delete_count as f64
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operation_failures_total", OP_ITER),
        0.0
    );
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            OP_ATTRIBUTES
        ),
        0.0
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operation_failures_total", OP_GET),
        1.0
    );
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            OP_GET_RANGE
        ),
        0.0
    );
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            OP_EXISTS
        ),
        0.0
    );
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            OP_UPLOAD
        ),
        stats.failed_upload_count as f64
    );
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            OP_DELETE
        ),
        0.0
    );

    let last_upload = metric_gauge(&reg, "thanos_objstore_bucket_last_successful_upload_time");
    assert!(last_upload > 0.0, "last upload not > 0: {last_upload}");

    // Clear bucket, but don't clear metrics to ensure we reuse the same metrics.
    tokio::time::sleep(Duration::from_millis(2)).await;
    let bkt2 = BucketClient::new_in_memory("abc").with_shared_metrics_from(&bkt);
    let stats2 = acceptance_test(&bkt2, 17 * 1024 * 1024).await;
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_ITER),
        18.0
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_ATTRIBUTES),
        (stats.attributes_count + stats2.attributes_count) as f64
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_GET),
        (stats.get_count + stats2.get_count) as f64
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_GET_RANGE),
        6.0
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_EXISTS),
        4.0
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_UPLOAD),
        (stats.upload_count + stats2.upload_count) as f64
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_DELETE),
        (stats.delete_count + stats2.delete_count) as f64
    );
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            OP_ATTRIBUTES
        ),
        1.0
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operation_failures_total", OP_GET),
        3.0
    );
    assert!(
        metric_gauge(&reg, "thanos_objstore_bucket_last_successful_upload_time") > last_upload
    );
}

// Upstream: github.com/thanos-io/objstore/objstore_test.go::TestMetricBucket_Multiple_Clients
#[test]
fn test_metric_bucket_multiple_clients() {
    let reg = Registry::new();
    let _b1 = BucketClient::new_in_memory("abc")
        .with_metrics(&reg, "test")
        .unwrap();
    let _b2 = BucketClient::new_in_memory("def")
        .with_metrics(&reg, "test")
        .unwrap();
    assert_eq!(
        count_series(&reg, "thanos_objstore_bucket_operations_total"),
        14
    );
}

// Upstream: github.com/thanos-io/objstore/objstore_test.go::TestMetricBucket_UploadShouldPreserveReaderFeatures
#[tokio::test]
async fn test_metric_bucket_upload_should_preserve_reader_features() {
    // Rust BucketClient uses Bytes (in-memory) and upload_file (seekable file on disk) rather
    // than Go's io.Reader/Seeker/ReaderAt interface wrappers; verify both preserve content and
    // metric accounting through BucketMetrics.
    let reg = Registry::new();
    let bkt = BucketClient::new_in_memory("test")
        .with_metrics(&reg, "test")
        .unwrap();

    bkt.upload("dir/bytes", Bytes::from_static(b"1"))
        .await
        .unwrap();
    assert_eq!(bkt.download("dir/bytes").await.unwrap().as_ref(), b"1");

    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("test");
    std::fs::write(&file_path, b"test").unwrap();
    bkt.upload_file("dir/file", &file_path).await.unwrap();
    assert_eq!(bkt.download("dir/file").await.unwrap().as_ref(), b"test");

    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_UPLOAD),
        2.0
    );
    let (count, sum, _) = metric_histogram_count_and_sum(
        &reg,
        "thanos_objstore_bucket_operation_transferred_bytes",
        OP_UPLOAD,
    );
    assert_eq!(count, 2);
    assert_eq!(sum, 5.0);
}

// Upstream: github.com/thanos-io/objstore/objstore_test.go::TestMetricBucket_ReaderClose
#[tokio::test]
async fn test_metric_bucket_reader_close() {
    let reg = Registry::new();
    let bkt = BucketClient::new_in_memory("test")
        .with_metrics(&reg, "test")
        .unwrap();

    bkt.upload("dir/obj1", Bytes::from_static(b"test"))
        .await
        .unwrap();
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_UPLOAD),
        1.0
    );
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            OP_UPLOAD
        ),
        0.0
    );

    assert_eq!(bkt.download("dir/obj1").await.unwrap().as_ref(), b"test");
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_GET),
        1.0
    );
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            OP_GET
        ),
        0.0
    );

    assert_eq!(
        bkt.get_range("dir/obj1", 0, 1).await.unwrap().as_ref(),
        b"t"
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_GET_RANGE),
        1.0
    );
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            OP_GET_RANGE
        ),
        0.0
    );
}

#[derive(Debug)]
struct FailingStore {
    inner: Arc<dyn ObjectStore>,
    fail_put: bool,
    fail_get: bool,
}

impl std::fmt::Display for FailingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailingStore")
    }
}

#[async_trait]
impl ObjectStore for FailingStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.fail_put {
            return Err(object_store::Error::Generic {
                store: "FailingStore",
                source: "mocked error".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if self.fail_get {
            return Err(object_store::Error::Generic {
                store: "FailingStore",
                source: "mocked error".into(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &ObjPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjPath, to: &ObjPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &ObjPath, to: &ObjPath) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

// Upstream: github.com/thanos-io/objstore/objstore_test.go::TestMetricBucket_ReaderCloseError
#[tokio::test]
async fn test_metric_bucket_reader_close_error() {
    // Upload failure tracks ops & opsFailures once.
    {
        let reg = Registry::new();
        let store = Arc::new(FailingStore {
            inner: Arc::new(InMemory::new()),
            fail_put: true,
            fail_get: false,
        });
        let bkt = BucketClient::from_object_store(store, "test", "INMEMORY")
            .with_metrics(&reg, "test")
            .unwrap();
        assert!(bkt.upload("test", Bytes::from_static(b"test")).await.is_err());
        assert_eq!(
            metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_UPLOAD),
            1.0
        );
        assert_eq!(
            metric_counter(
                &reg,
                "thanos_objstore_bucket_operation_failures_total",
                OP_UPLOAD
            ),
            1.0
        );
    }

    // Get and GetRange failures track ops & opsFailures once per call.
    {
        let reg = Registry::new();
        let store = Arc::new(FailingStore {
            inner: Arc::new(InMemory::new()),
            fail_put: false,
            fail_get: true,
        });
        let bkt = BucketClient::from_object_store(store, "test", "INMEMORY")
            .with_metrics(&reg, "test")
            .unwrap();
        assert!(bkt.download("test").await.is_err());
        assert_eq!(
            metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_GET),
            1.0
        );
        assert_eq!(
            metric_counter(
                &reg,
                "thanos_objstore_bucket_operation_failures_total",
                OP_GET
            ),
            1.0
        );

        assert!(bkt.get_range("test", 0, 1).await.is_err());
        assert_eq!(
            metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_GET_RANGE),
            1.0
        );
        assert_eq!(
            metric_counter(
                &reg,
                "thanos_objstore_bucket_operation_failures_total",
                OP_GET_RANGE
            ),
            1.0
        );
    }
}

// Upstream: github.com/thanos-io/objstore/objstore_test.go::TestDownloadUploadDirConcurrency
#[tokio::test]
async fn test_download_upload_dir_concurrency() {
    let reg = Registry::new();
    let m = BucketClient::new_in_memory("test")
        .with_metrics(&reg, "test")
        .unwrap();
    let temp_dir = TempDir::new().unwrap();

    m.upload("dir/obj1", Bytes::from_static(b"1"))
        .await
        .unwrap();
    m.upload("dir/obj2", Bytes::from_static(b"2"))
        .await
        .unwrap();
    m.upload("dir/obj3", Bytes::from(vec![b'3'; 1024 * 1024]))
        .await
        .unwrap();

    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_UPLOAD),
        3.0
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_GET),
        0.0
    );

    m.download_dir(
        "dir/",
        "dir/",
        temp_dir.path(),
        DownloadDirOptions {
            concurrency: 10,
            ignored_paths: Vec::new(),
        },
    )
    .await
    .unwrap();

    let downloaded_count = std::fs::read_dir(temp_dir.path()).unwrap().count();
    assert_eq!(downloaded_count, 3);

    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_GET),
        3.0
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_ITER),
        1.0
    );
    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_UPLOAD),
        3.0
    );
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_fetched_bytes_total",
            OP_GET
        ),
        1_048_578.0
    );

    let (get_cnt, get_sum, get_buckets) = metric_histogram_count_and_sum(
        &reg,
        "thanos_objstore_bucket_operation_transferred_bytes",
        OP_GET,
    );
    assert_eq!(get_cnt, 3);
    assert_eq!(get_sum, 1_048_578.0);
    if !get_buckets.is_empty() {
        let le_32k = get_buckets
            .iter()
            .find(|(b, _)| (*b - 32768.0).abs() < 0.1)
            .unwrap()
            .1;
        let le_1m = get_buckets
            .iter()
            .find(|(b, _)| (*b - 1_048_576.0).abs() < 0.1)
            .unwrap()
            .1;
        assert_eq!(le_32k, 2);
        assert_eq!(le_1m, 3);
    }

    m.upload_dir(
        temp_dir.path(),
        "/dir-copy",
        UploadDirOptions { concurrency: 10 },
    )
    .await
    .unwrap();

    assert_eq!(
        metric_counter(&reg, "thanos_objstore_bucket_operations_total", OP_UPLOAD),
        6.0
    );
    assert!(m.exists("dir-copy/obj1").await.unwrap());
    assert!(m.exists("dir-copy/obj2").await.unwrap());
    assert!(m.exists("dir-copy/obj3").await.unwrap());
}

// Upstream: github.com/thanos-io/objstore/objstore_test.go::TestTimingReader
#[tokio::test]
async fn test_timing_reader() {
    let reg = Registry::new();
    let m = BucketClient::new_in_memory("test")
        .with_metrics(&reg, "test")
        .unwrap();
    m.upload("hello", Bytes::from_static(b"hello world"))
        .await
        .unwrap();

    let got = m.download("hello").await.unwrap();
    assert_eq!(got.len(), 11);
    let partial = m.get_range("hello", 0, 4).await.unwrap();
    assert_eq!(partial.as_ref(), b"hell");
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            OP_GET
        ),
        0.0
    );
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_fetched_bytes_total",
            OP_GET
        ),
        11.0
    );
}

// Upstream: github.com/thanos-io/objstore/objstore_test.go::TestTimingReader_ExpectedError
#[tokio::test]
async fn test_timing_reader_expected_error() {
    let reg = Registry::new();
    let m = BucketClient::new_in_memory("test")
        .with_metrics(&reg, "test")
        .unwrap()
        .with_expected_not_found();

    let err = m.download("missing").await.unwrap_err();
    assert!(err.is_not_found());
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            OP_GET
        ),
        0.0
    );
}

// Upstream: github.com/thanos-io/objstore/objstore_test.go::TestTimingReader_UnexpectedError
#[tokio::test]
async fn test_timing_reader_unexpected_error() {
    let reg = Registry::new();
    let m = BucketClient::new_in_memory("test")
        .with_metrics(&reg, "test")
        .unwrap();

    let err = m.download("missing").await.unwrap_err();
    assert!(err.is_not_found());
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            OP_GET
        ),
        1.0
    );
}

// Upstream: github.com/thanos-io/objstore/objstore_test.go::TestTimingReader_ContextCancellation
#[tokio::test]
async fn test_timing_reader_context_cancellation() {
    #[derive(Debug)]
    struct SlowStore(Arc<InMemory>);
    impl std::fmt::Display for SlowStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SlowStore")
        }
    }
    #[async_trait]
    impl ObjectStore for SlowStore {
        async fn put_opts(
            &self,
            l: &ObjPath,
            p: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.0.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &ObjPath,
            o: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.0.put_multipart_opts(l, o).await
        }
        async fn get_opts(
            &self,
            _l: &ObjPath,
            _o: GetOptions,
        ) -> object_store::Result<GetResult> {
            tokio::time::sleep(Duration::from_secs(10)).await;
            unreachable!()
        }
        async fn delete(&self, l: &ObjPath) -> object_store::Result<()> {
            self.0.delete(l).await
        }
        fn list(&self, p: Option<&ObjPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.0.list(p)
        }
        async fn list_with_delimiter(
            &self,
            p: Option<&ObjPath>,
        ) -> object_store::Result<ListResult> {
            self.0.list_with_delimiter(p).await
        }
        async fn copy(&self, from: &ObjPath, to: &ObjPath) -> object_store::Result<()> {
            self.0.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &ObjPath, to: &ObjPath) -> object_store::Result<()> {
            self.0.copy_if_not_exists(from, to).await
        }
    }

    let reg = Registry::new();
    let m = BucketClient::from_object_store(Arc::new(SlowStore(Arc::new(InMemory::new()))), "test", "INMEMORY")
        .with_metrics(&reg, "test")
        .unwrap();

    let cancelled = tokio::select! {
        biased;
        _ = std::future::ready(()) => true,
        _ = m.download("obj") => false,
    };
    assert!(cancelled);
    assert_eq!(
        metric_counter(
            &reg,
            "thanos_objstore_bucket_operation_failures_total",
            OP_GET
        ),
        0.0
    );
}

// Upstream: github.com/thanos-io/objstore/objstore_test.go::TestTimingReader_ShouldCorrectlyWrapFile
#[tokio::test]
async fn test_timing_reader_should_correctly_wrap_file() {
    let dir = TempDir::new().unwrap();
    let test_file = dir.path().join("test");
    std::fs::write(&test_file, b"test").unwrap();

    let reg = Registry::new();
    let m = BucketClient::new_in_memory("test")
        .with_metrics(&reg, "test")
        .unwrap();
    m.upload_file("test", &test_file).await.unwrap();
    assert_eq!(m.download("test").await.unwrap().as_ref(), b"test");
    let (_, sum, _) = metric_histogram_count_and_sum(
        &reg,
        "thanos_objstore_bucket_operation_transferred_bytes",
        OP_UPLOAD,
    );
    assert_eq!(sum, 4.0);
}

// Upstream: github.com/thanos-io/objstore/objstore_test.go::TestDownloadDir_CleanUp
#[tokio::test]
async fn test_download_dir_clean_up() {
    #[derive(Debug)]
    struct UnreliableGetStore {
        inner: Arc<InMemory>,
        n: i32,
        current: AtomicI32,
    }
    impl std::fmt::Display for UnreliableGetStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "UnreliableGetStore")
        }
    }
    #[async_trait]
    impl ObjectStore for UnreliableGetStore {
        async fn put_opts(
            &self,
            l: &ObjPath,
            p: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &ObjPath,
            o: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(
            &self,
            l: &ObjPath,
            o: GetOptions,
        ) -> object_store::Result<GetResult> {
            let c = self.current.fetch_add(1, Ordering::SeqCst) + 1;
            if c % self.n == 0 {
                return Err(object_store::Error::Generic {
                    store: "UnreliableGetStore",
                    source: "some error message".into(),
                });
            }
            self.inner.get_opts(l, o).await
        }
        async fn delete(&self, l: &ObjPath) -> object_store::Result<()> {
            self.inner.delete(l).await
        }
        fn list(&self, p: Option<&ObjPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(
            &self,
            p: Option<&ObjPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy(&self, from: &ObjPath, to: &ObjPath) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &ObjPath, to: &ObjPath) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    let b = BucketClient::from_object_store(
        Arc::new(UnreliableGetStore {
            inner: Arc::new(InMemory::new()),
            n: 3,
            current: AtomicI32::new(0),
        }),
        "test",
        "INMEMORY",
    );
    let temp_dir = TempDir::new().unwrap();
    let dst = temp_dir.path().join("dl");

    b.upload("dir/obj1", Bytes::from_static(b"1"))
        .await
        .unwrap();
    b.upload("dir/obj2", Bytes::from_static(b"2"))
        .await
        .unwrap();
    b.upload("dir/obj3", Bytes::from_static(b"3"))
        .await
        .unwrap();

    assert!(b
        .download_dir("dir/", "dir/", &dst, DownloadDirOptions::default())
        .await
        .is_err());
    assert!(!dst.exists(), "dst dir must be cleaned up on download error");
}

// Upstream: github.com/thanos-io/objstore/objstore_test.go::Test_TryToGetSizeLimitedReader
#[tokio::test]
async fn test_try_to_get_size_limited_reader() {
    use std::io::Read;
    let data = vec![b'a'; 2048];
    let limited = std::io::Read::take(std::io::Cursor::new(&data), 1024);
    assert_eq!(limited.limit(), 1024);

    let reg = Registry::new();
    let bkt = BucketClient::new_in_memory("test")
        .with_metrics(&reg, "test")
        .unwrap();
    let mut buf = Vec::new();
    let mut limited = limited;
    limited.read_to_end(&mut buf).unwrap();
    assert_eq!(buf.len(), 1024);
    bkt.upload("limited", Bytes::from(buf)).await.unwrap();
    let (_, sum, _) = metric_histogram_count_and_sum(
        &reg,
        "thanos_objstore_bucket_operation_transferred_bytes",
        OP_UPLOAD,
    );
    assert_eq!(sum, 1024.0);
}

// ===========================================================================
// prefixed_bucket_test.go (1)
// ===========================================================================

// Upstream: github.com/thanos-io/objstore/prefixed_bucket_test.go::TestPrefixedBucket_Acceptance
#[tokio::test]
async fn test_prefixed_bucket_acceptance() {
    let prefixes = [
        "/someprefix/anotherprefix/",
        "someprefix/anotherprefix/",
        "someprefix/anotherprefix",
        "someprefix/",
        "someprefix",
    ];

    for prefix in prefixes {
        let base1 = BucketClient::new_in_memory("test");
        let pb1 = base1.with_prefix(prefix);
        acceptance_test(&pb1, 17 * 1024 * 1024).await;

        // UsesPrefixTest
        let base2 = BucketClient::new_in_memory("test");
        let trimmed = prefix.trim_matches('/');
        base2
            .upload(
                &format!("{trimmed}/file1.jpg"),
                Bytes::from_static(b"test-data1"),
            )
            .await
            .unwrap();

        let pb2 = base2.with_prefix(prefix);
        assert_eq!(
            pb2.download("file1.jpg").await.unwrap().as_ref(),
            b"test-data1"
        );

        pb2.upload("file2.jpg", Bytes::from_static(b"test-data2"))
            .await
            .unwrap();
        assert_eq!(
            base2
                .download(&format!("{trimmed}/file2.jpg"))
                .await
                .unwrap()
                .as_ref(),
            b"test-data2"
        );

        pb2.delete("file2.jpg").await.unwrap();
        let err = base2
            .download(&format!("{trimmed}/file2.jpg"))
            .await
            .unwrap_err();
        assert!(err.is_not_found());

        assert_eq!(
            pb2.get_range("file1.jpg", 1, 3).await.unwrap().as_ref(),
            b"est"
        );
        assert!(pb2.exists("file1.jpg").await.unwrap());
        assert_eq!(pb2.attributes("file1.jpg").await.unwrap().size, 10);

        base2
            .upload(
                &format!("{trimmed}/dir/file1.jpg"),
                Bytes::from_static(b"test-data1"),
            )
            .await
            .unwrap();

        let mut rec = pb2.list_all("").await.unwrap();
        rec.sort();
        assert_eq!(rec, vec!["dir/file1.jpg", "file1.jpg"]);

        let mut non_rec = pb2.iter_dir("").await.unwrap();
        non_rec.sort();
        assert_eq!(non_rec, vec!["dir/", "file1.jpg"]);
    }
}

// ===========================================================================
// providers/azure/azure_test.go (5)
// ===========================================================================

// Upstream: github.com/thanos-io/objstore/providers/azure/azure_test.go::TestConfig_validate
#[test]
fn test_azure_config_validate() {
    let base = || azure::ThanosAzureConfig {
        storage_account_name: "myStorageAccount".into(),
        storage_account_key: "bXlTdXBlclNlY3JldEtleTEyMyFAIw==".into(),
        storage_connection_string: String::new(),
        storage_create_container: true,
        container_name: "MyContainer".into(),
        endpoint: "blob.core.windows.net".into(),
        user_assigned_id: String::new(),
        az_tenant_id: String::new(),
        client_id: String::new(),
        client_secret: String::new(),
        max_retries: 0,
        pipeline_max_tries: 0,
        reader_max_retry_requests: 100,
        http: BucketHTTPConfig::default(),
    };

    // validConfig
    assert!(base().validate().is_ok());

    // Missing storage account
    let mut c = base();
    c.storage_account_name = String::new();
    assert!(c.validate().is_err());

    // Negative max_tries
    let mut c = base();
    c.pipeline_max_tries = -1;
    assert!(c.validate().is_err());

    // Negative max_retry_requests
    let mut c = base();
    c.reader_max_retry_requests = -1;
    assert!(c.validate().is_err());

    // Valid MSI Resource
    let mut c = base();
    c.storage_account_key = String::new();
    assert!(c.validate().is_ok());

    // Valid User Assigned Identity Config
    let mut c = base();
    c.storage_account_key = String::new();
    c.user_assigned_id = "1234-56578678-655".into();
    assert!(c.validate().is_ok());

    // Valid User Assigned and Connection String set -> fail
    let mut c = base();
    c.storage_account_key = String::new();
    c.user_assigned_id = "1234-56578678-655".into();
    c.storage_connection_string = "myConnectionString".into();
    assert!(c.validate().is_err());

    // Valid AzTenantID, ClientID, ClientSecret
    let mut c = base();
    c.storage_account_key = String::new();
    c.az_tenant_id = "1234-56578678-655".into();
    c.client_id = "1234-56578678-655".into();
    c.client_secret = "1234-56578678-655".into();
    assert!(c.validate().is_ok());

    // Valid ClientID and ClientSecret but missing AzTenantID -> fail
    let mut c = base();
    c.storage_account_key = String::new();
    c.client_id = "1234-56578678-655".into();
    c.client_secret = "1234-56578678-655".into();
    assert!(c.validate().is_err());
}

// Upstream: github.com/thanos-io/objstore/providers/azure/azure_test.go::TestParseConfig_DefaultHTTPConfig
#[test]
fn test_azure_parse_config_default_http_config() {
    let yaml_cfg = r#"
backend: azure
azure:
  account_name: myStorageAccount
  account_key: bXlTdXBlclNlY3JldEtleTEyMyFAIw==
  container_name: MyContainer
  endpoint_suffix: blob.core.windows.net
"#;
    let (cfg, val_err) = load_blocks_bucket_from_yaml(&indent_yaml(yaml_cfg)).unwrap();
    assert!(val_err.is_none());
    assert_eq!(
        cfg.backend_cfg.azure.http.idle_conn_timeout,
        GoDuration::from_secs(90)
    );
    assert_eq!(
        cfg.backend_cfg.azure.http.response_header_timeout,
        GoDuration::from_secs(120)
    );
    assert!(!cfg.backend_cfg.azure.http.insecure_skip_verify);
    assert!(!cfg.backend_cfg.azure.http.force_attempt_http2);
}

// Upstream: github.com/thanos-io/objstore/providers/azure/azure_test.go::TestParseConfig_CustomHTTPConfigWithTLS
#[test]
fn test_azure_parse_config_custom_http_config_with_tls() {
    let yaml_cfg = r#"
backend: azure
azure:
  account_name: myStorageAccount
  account_key: abc123
  container_name: MyContainer
  endpoint_suffix: blob.core.windows.net
  http:
    tls_ca_path: /certs/ca.crt
    tls_cert_path: /certs/cert.crt
    tls_key_path: /certs/key.key
    tls_server_name: server
    insecure_skip_verify: false
    force_attempt_http2: true
"#;
    let (cfg, _) = load_blocks_bucket_from_yaml(&indent_yaml(yaml_cfg)).unwrap();
    assert_eq!(cfg.backend_cfg.azure.http.tls_ca_path, "/certs/ca.crt");
    assert_eq!(cfg.backend_cfg.azure.http.tls_cert_path, "/certs/cert.crt");
    assert_eq!(cfg.backend_cfg.azure.http.tls_key_path, "/certs/key.key");
    assert_eq!(cfg.backend_cfg.azure.http.tls_server_name, "server");
    assert!(!cfg.backend_cfg.azure.http.insecure_skip_verify);
    assert!(cfg.backend_cfg.azure.http.force_attempt_http2);
}

// Upstream: github.com/thanos-io/objstore/providers/azure/azure_test.go::TestParseConfig_CustomLegacyInsecureSkipVerify
#[test]
fn test_azure_parse_config_custom_legacy_insecure_skip_verify() {
    let yaml_cfg = r#"
backend: azure
azure:
  account_name: myStorageAccount
  account_key: abc123
  container_name: MyContainer
  endpoint_suffix: blob.core.windows.net
  http:
    insecure_skip_verify: true
"#;
    let (cfg, _) = load_blocks_bucket_from_yaml(&indent_yaml(yaml_cfg)).unwrap();
    assert!(cfg.backend_cfg.azure.http.insecure_skip_verify);
    assert!(exthttp::client_options(&cfg.backend_cfg.azure.http, None).is_ok());
}

// Upstream: github.com/thanos-io/objstore/providers/azure/azure_test.go::TestNewBucketWithErrorRoundTripper
#[tokio::test]
async fn test_azure_new_bucket_with_error_round_tripper() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let resp = "HTTP/1.1 400 Bad Request\r\nContent-Length: 12\r\nConnection: close\r\n\r\nmocked error";
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });

    let cfg = mimir_config::AzureConfig {
        account_name: "devstoreaccount1".into(),
        container_name: "MyContainer".into(),
        connection_string: format!(
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{addr}/devstoreaccount1;"
        ),
        max_retries: 0,
        ..Default::default()
    };

    let err = azure::new_store_async(&cfg, Some(Duration::from_secs(5)))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("400") || err.to_string().contains("mocked error"),
        "expected HTTP error from Azure container check, got: {err}"
    );
    srv.abort();
}

// ===========================================================================
// providers/filesystem/filesystem_test.go (9)
// ===========================================================================

// Upstream: github.com/thanos-io/objstore/providers/filesystem/filesystem_test.go::TestDelete_EmptyDirDeletionRaceCondition
#[tokio::test]
async fn test_filesystem_delete_empty_dir_deletion_race_condition() {
    const RUNS: usize = 200;
    for _ in 0..RUNS {
        let dir = TempDir::new().unwrap();
        let b = BucketClient::new_filesystem(dir.path()).unwrap();
        b.upload("subfolder/first", Bytes::from_static(b"first"))
            .await
            .unwrap();
        b.upload("subfolder/second", Bytes::from_static(b"second"))
            .await
            .unwrap();

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let b1 = b.clone();
        let bar1 = Arc::clone(&barrier);
        let t1 = tokio::spawn(async move {
            bar1.wait().await;
            b1.delete("subfolder/first").await.unwrap();
        });
        let b2 = b.clone();
        let bar2 = Arc::clone(&barrier);
        let t2 = tokio::spawn(async move {
            bar2.wait().await;
            b2.delete("subfolder/second").await.unwrap();
        });
        t1.await.unwrap();
        t2.await.unwrap();
    }
}

// Upstream: github.com/thanos-io/objstore/providers/filesystem/filesystem_test.go::TestIter_CancelledContext
#[tokio::test]
async fn test_filesystem_iter_cancelled_context() {
    let dir = TempDir::new().unwrap();
    let b = BucketClient::new_filesystem(dir.path()).unwrap();
    let cancelled = tokio::select! {
        biased;
        _ = std::future::ready(()) => true,
        _ = b.iter_dir("") => false,
    };
    assert!(cancelled);
}

// Upstream: github.com/thanos-io/objstore/providers/filesystem/filesystem_test.go::TestIterWithAttributes
#[tokio::test]
async fn test_filesystem_iter_with_attributes() {
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("testfile");
    std::fs::write(&file_path, b"hello").unwrap();
    let expected_updated_at = std::fs::metadata(&file_path).unwrap().modified().unwrap();

    let b = BucketClient::new_filesystem(dir.path()).unwrap();

    // no options
    let entries = b
        .iter_with_attributes("", IterOptions::default())
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].last_modified, None);

    // with updated at
    let entries = b
        .iter_with_attributes("", IterOptions::updated_at())
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].last_modified, Some(expected_updated_at));
}

// Upstream: github.com/thanos-io/objstore/providers/filesystem/filesystem_test.go::TestGet_CancelledContext
#[tokio::test]
async fn test_filesystem_get_cancelled_context() {
    let dir = TempDir::new().unwrap();
    let b = BucketClient::new_filesystem(dir.path()).unwrap();
    let cancelled = tokio::select! {
        biased;
        _ = std::future::ready(()) => true,
        _ = b.download("some-file") => false,
    };
    assert!(cancelled);
}

// Upstream: github.com/thanos-io/objstore/providers/filesystem/filesystem_test.go::TestAttributes_CancelledContext
#[tokio::test]
async fn test_filesystem_attributes_cancelled_context() {
    let dir = TempDir::new().unwrap();
    let b = BucketClient::new_filesystem(dir.path()).unwrap();
    let cancelled = tokio::select! {
        biased;
        _ = std::future::ready(()) => true,
        _ = b.attributes("some-file") => false,
    };
    assert!(cancelled);
}

// Upstream: github.com/thanos-io/objstore/providers/filesystem/filesystem_test.go::TestGetRange_CancelledContext
#[tokio::test]
async fn test_filesystem_get_range_cancelled_context() {
    let dir = TempDir::new().unwrap();
    let b = BucketClient::new_filesystem(dir.path()).unwrap();
    let cancelled = tokio::select! {
        biased;
        _ = std::future::ready(()) => true,
        _ = b.get_range("some-file", 0, 100) => false,
    };
    assert!(cancelled);
}

// Upstream: github.com/thanos-io/objstore/providers/filesystem/filesystem_test.go::TestExists_CancelledContext
#[tokio::test]
async fn test_filesystem_exists_cancelled_context() {
    let dir = TempDir::new().unwrap();
    let b = BucketClient::new_filesystem(dir.path()).unwrap();
    let cancelled = tokio::select! {
        biased;
        _ = std::future::ready(()) => true,
        _ = b.exists("some-file") => false,
    };
    assert!(cancelled);
}

// Upstream: github.com/thanos-io/objstore/providers/filesystem/filesystem_test.go::TestUpload_CancelledContext
#[tokio::test]
async fn test_filesystem_upload_cancelled_context() {
    let dir = TempDir::new().unwrap();
    let b = BucketClient::new_filesystem(dir.path()).unwrap();
    let cancelled = tokio::select! {
        biased;
        _ = std::future::ready(()) => true,
        _ = b.upload("some-file", Bytes::from_static(b"file content")) => false,
    };
    assert!(cancelled);
}

// Upstream: github.com/thanos-io/objstore/providers/filesystem/filesystem_test.go::TestDelete_CancelledContext
#[tokio::test]
async fn test_filesystem_delete_cancelled_context() {
    let dir = TempDir::new().unwrap();
    let b = BucketClient::new_filesystem(dir.path()).unwrap();
    let cancelled = tokio::select! {
        biased;
        _ = std::future::ready(()) => true,
        _ = b.delete("some-file") => false,
    };
    assert!(cancelled);
}

// ===========================================================================
// providers/gcs/gcs_test.go (5)
// ===========================================================================

// Upstream: github.com/thanos-io/objstore/providers/gcs/gcs_test.go::TestBucket_Get_ShouldReturnErrorIfServerTruncateResponse
#[tokio::test]
async fn test_gcs_bucket_get_should_return_error_if_server_truncate_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let resp = "HTTP/1.1 200 OK\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nContent-Length: 100\r\nConnection: close\r\n\r\n12345";
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });

    let mut cfg = BucketConfig::default();
    cfg.backend_cfg.backend = "gcs".into();
    cfg.backend_cfg.gcs.bucket_name = "test-bucket".into();
    cfg.backend_cfg.gcs.max_retries = 1;
    cfg.backend_cfg.gcs.service_account = format!(
        r#"{{"type":"service_account","private_key":"unused","private_key_id":"x","client_email":"x@y","gcs_base_url":"http://{addr}","disable_oauth":true}}"#
    );
    let bkt = BucketClient::new_client(&cfg, "test", None).unwrap();
    let err = bkt.download("test").await.unwrap_err();
    assert!(!err.is_not_found(), "expected truncation error, got {err}");
    srv.abort();
}

// Upstream: github.com/thanos-io/objstore/providers/gcs/gcs_test.go::TestNewBucketWithConfig_ShouldCreateGRPC
#[tokio::test]
async fn test_gcs_new_bucket_with_config_should_create_client() {
    // Mimir's gcs.Config does not expose Thanos's gRPC transport toggle (all GCS operations use
    // HTTP/REST against gcs_base_url or STORAGE_EMULATOR_HOST). Verify GCS bucket creation &
    // operation against a local mock GCS server.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });

    let mut cfg = BucketConfig::default();
    cfg.backend_cfg.backend = "gcs".into();
    cfg.backend_cfg.gcs.bucket_name = "test-bucket".into();
    cfg.backend_cfg.gcs.service_account = format!(
        r#"{{"type":"service_account","private_key":"unused","private_key_id":"x","client_email":"x@y","gcs_base_url":"http://{addr}","disable_oauth":true}}"#
    );
    let bkt = BucketClient::new_client(&cfg, "test-bucket", None).unwrap();
    assert_eq!(bkt.download("obj").await.unwrap().as_ref(), b"ok");
    srv.abort();
}

// Upstream: github.com/thanos-io/objstore/providers/gcs/gcs_test.go::TestParseConfig_ChunkSize
#[test]
fn test_gcs_parse_config_chunk_size() {
    // Mimir's gcs.Config exposes max_retries & enable_upload_retries rather than Thanos's
    // chunk_size_bytes; verify defaults and custom YAML parsing on Mimir GCSConfig.
    let (default_cfg, val_err) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: gcs
gcs:
  bucket_name: abcd
"#,
    ))
    .unwrap();
    assert!(val_err.is_none());
    assert_eq!(default_cfg.backend_cfg.gcs.bucket_name, "abcd");
    assert_eq!(default_cfg.backend_cfg.gcs.max_retries, 20);
    assert!(default_cfg.backend_cfg.gcs.enable_upload_retries);

    let (custom_cfg, val_err) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: gcs
gcs:
  bucket_name: abcd
  max_retries: 5
  enable_upload_retries: false
"#,
    ))
    .unwrap();
    assert!(val_err.is_none());
    assert_eq!(custom_cfg.backend_cfg.gcs.max_retries, 5);
    assert!(!custom_cfg.backend_cfg.gcs.enable_upload_retries);
}

// Upstream: github.com/thanos-io/objstore/providers/gcs/gcs_test.go::TestParseConfig_HTTPConfig
#[test]
fn test_gcs_parse_config_http_config() {
    let (def_cfg, _) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: gcs
gcs:
  bucket_name: abcd
"#,
    ))
    .unwrap();
    assert_eq!(
        def_cfg.backend_cfg.gcs.http.idle_conn_timeout,
        GoDuration::from_secs(90)
    );
    assert_eq!(
        def_cfg.backend_cfg.gcs.http.response_header_timeout,
        GoDuration::from_secs(120)
    );
    assert!(!def_cfg.backend_cfg.gcs.http.insecure_skip_verify);
    assert!(!def_cfg.backend_cfg.gcs.http.force_attempt_http2);

    let (cust_cfg, _) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: gcs
gcs:
  bucket_name: abcd
  http:
    insecure_skip_verify: true
    idle_conn_timeout: 50s
    response_header_timeout: 1m
    force_attempt_http2: true
"#,
    ))
    .unwrap();
    assert_eq!(
        cust_cfg.backend_cfg.gcs.http.idle_conn_timeout,
        GoDuration::from_secs(50)
    );
    assert_eq!(
        cust_cfg.backend_cfg.gcs.http.response_header_timeout,
        GoDuration::from_secs(60)
    );
    assert!(cust_cfg.backend_cfg.gcs.http.insecure_skip_verify);
    assert!(cust_cfg.backend_cfg.gcs.http.force_attempt_http2);

    let (tls_cfg, _) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: gcs
gcs:
  bucket_name: abcd
  http:
    tls_ca_path: /certs/ca.crt
    tls_cert_path: /certs/cert.crt
    tls_key_path: /certs/key.key
    tls_server_name: server
    insecure_skip_verify: false
"#,
    ))
    .unwrap();
    assert_eq!(tls_cfg.backend_cfg.gcs.http.tls_ca_path, "/certs/ca.crt");
    assert_eq!(tls_cfg.backend_cfg.gcs.http.tls_cert_path, "/certs/cert.crt");
    assert_eq!(tls_cfg.backend_cfg.gcs.http.tls_key_path, "/certs/key.key");
    assert_eq!(tls_cfg.backend_cfg.gcs.http.tls_server_name, "server");
    assert!(!tls_cfg.backend_cfg.gcs.http.insecure_skip_verify);
}

// Upstream: github.com/thanos-io/objstore/providers/gcs/gcs_test.go::TestNewBucketWithErrorRoundTripper
#[tokio::test]
async fn test_gcs_new_bucket_with_error_round_tripper() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let resp = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 12\r\n\r\nmocked error";
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });

    let cfg = mimir_config::GCSConfig {
        bucket_name: "test-bucket".into(),
        max_retries: 1,
        service_account: format!(
            r#"{{"type":"service_account","private_key":"unused","private_key_id":"x","client_email":"x@y","gcs_base_url":"http://{addr}","disable_oauth":true}}"#
        ),
        ..Default::default()
    };
    let store = gcs::new_store(&cfg).unwrap();
    let bkt = BucketClient::from_object_store(store, "test-bucket", "GCS");
    let err = bkt.download("test-bucket").await.unwrap_err();
    assert!(
        err.to_string().contains("500") || err.to_string().contains("mocked error"),
        "expected error from HTTP transport, got: {err}"
    );
    srv.abort();
}

// ===========================================================================
// providers/s3/s3_test.go (15)
// ===========================================================================

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestParseConfig
#[test]
fn test_s3_parse_config() {
    let (cfg, val_err) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: s3
s3:
  bucket_name: abcd
  insecure: false
"#,
    ))
    .unwrap();
    assert!(val_err.is_none());
    assert_eq!(cfg.backend_cfg.s3.bucket_name, "abcd");
    assert!(!cfg.backend_cfg.s3.insecure);
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestParseConfig_SSEConfig
#[test]
fn test_s3_parse_config_sse_config() {
    let run = |yaml: &str| {
        let (cfg, _) = load_blocks_bucket_from_yaml(&indent_yaml(yaml)).unwrap();
        S3Bucket::new(&cfg.backend_cfg.s3, "test")
    };

    assert!(run(
        r#"
backend: s3
s3:
  bucket_name: abdd
  endpoint: s3-endpoint
  sse:
    type: SSE-S3
"#,
    )
    .is_ok());

    assert!(run(
        r#"
backend: s3
s3:
  bucket_name: abdd
  endpoint: s3-endpoint
  sse:
    type: SSE-C
"#,
    )
    .is_err());

    assert!(run(
        r#"
backend: s3
s3:
  bucket_name: abdd
  endpoint: s3-endpoint
  sse:
    type: SSE-KMS
"#,
    )
    .is_err());

    assert!(run(
        r#"
backend: s3
s3:
  bucket_name: abdd
  endpoint: s3-endpoint
  sse:
    type: SSE-KMS
    kms_key_id: abcd1234-ab12-cd34-1234567890ab
"#,
    )
    .is_ok());

    assert!(run(
        r#"
backend: s3
s3:
  bucket_name: abdd
  endpoint: s3-endpoint
  sse:
    type: SSE-KMS
    kms_key_id: abcd1234-ab12-cd34-1234567890ab
    kms_encryption_context: '{"key":"value","something":"else","a":"b"}'
"#,
    )
    .is_ok());

    assert!(run(
        r#"
backend: s3
s3:
  bucket_name: abdd
  endpoint: s3-endpoint
  sse:
    type: SSE-MagicKey
    kms_key_id: abcd1234-ab12-cd34-1234567890ab
"#,
    )
    .is_err());
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestParseConfig_DefaultHTTPConfig
#[test]
fn test_s3_parse_config_default_http_config() {
    let (cfg, _) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: s3
s3:
  bucket_name: abcd
  insecure: false
"#,
    ))
    .unwrap();
    assert_eq!(
        cfg.backend_cfg.s3.http.idle_conn_timeout,
        GoDuration::from_secs(90)
    );
    assert_eq!(
        cfg.backend_cfg.s3.http.response_header_timeout,
        GoDuration::from_secs(120)
    );
    assert!(!cfg.backend_cfg.s3.http.insecure_skip_verify);
    assert!(!cfg.backend_cfg.s3.http.force_attempt_http2);
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestParseConfig_CustomHTTPConfig
#[test]
fn test_s3_parse_config_custom_http_config() {
    let (cfg, _) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: s3
s3:
  bucket_name: abcd
  insecure: false
  http:
    insecure_skip_verify: true
    idle_conn_timeout: 50s
    response_header_timeout: 1m
    force_attempt_http2: true
"#,
    ))
    .unwrap();
    assert_eq!(
        cfg.backend_cfg.s3.http.idle_conn_timeout,
        GoDuration::from_secs(50)
    );
    assert_eq!(
        cfg.backend_cfg.s3.http.response_header_timeout,
        GoDuration::from_secs(60)
    );
    assert!(cfg.backend_cfg.s3.http.insecure_skip_verify);
    assert!(cfg.backend_cfg.s3.http.force_attempt_http2);
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestParseConfig_CustomHTTPConfigWithTLS
#[test]
fn test_s3_parse_config_custom_http_config_with_tls() {
    let (cfg, _) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: s3
s3:
  bucket_name: abcd
  insecure: false
  http:
    tls_ca_path: /certs/ca.crt
    tls_cert_path: /certs/cert.crt
    tls_key_path: /certs/key.key
    tls_server_name: server
    insecure_skip_verify: false
"#,
    ))
    .unwrap();
    assert_eq!(cfg.backend_cfg.s3.http.tls_ca_path, "/certs/ca.crt");
    assert_eq!(cfg.backend_cfg.s3.http.tls_cert_path, "/certs/cert.crt");
    assert_eq!(cfg.backend_cfg.s3.http.tls_key_path, "/certs/key.key");
    assert_eq!(cfg.backend_cfg.s3.http.tls_server_name, "server");
    assert!(!cfg.backend_cfg.s3.http.insecure_skip_verify);
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestParseConfig_CustomLegacyInsecureSkipVerify
#[test]
fn test_s3_parse_config_custom_legacy_insecure_skip_verify() {
    let (cfg, _) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: s3
s3:
  bucket_name: abcd
  insecure: false
  http:
    insecure_skip_verify: true
"#,
    ))
    .unwrap();
    assert!(cfg.backend_cfg.s3.http.insecure_skip_verify);
    assert!(exthttp::client_options(&cfg.backend_cfg.s3.http, None).is_ok());
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestValidate_OK
#[test]
fn test_s3_validate_ok() {
    let (cfg1, val1) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: s3
s3:
  bucket_name: bucket-name
  endpoint: s3-endpoint
  access_key_id: access_key
  insecure: false
  signature_version: v4
  secret_access_key: secret_key
  http:
    insecure_skip_verify: false
    idle_conn_timeout: 50s
"#,
    ))
    .unwrap();
    assert!(val1.is_none());
    assert!(S3Bucket::new(&cfg1.backend_cfg.s3, "test").is_ok());

    let (cfg2, val2) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: s3
s3:
  bucket_name: bucket-name
  endpoint: s3-endpoint
  access_key_id: access_key
  insecure: false
  signature_version: v4
  secret_access_key: secret_key
  storage_class: STANDARD_IA
  http:
    idle_conn_timeout: 0s
"#,
    ))
    .unwrap();
    assert!(val2.is_none());
    let bkt2 = S3Bucket::new(&cfg2.backend_cfg.s3, "test").unwrap();
    assert_eq!(bkt2.storage_class(), "STANDARD_IA");

    let (cfg3, val3) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: s3
s3:
  bucket_name: bucket-name
  endpoint: s3-endpoint
  access_key_id: access_key
  insecure: false
  signature_version: v4
  secret_access_key: secret_key
  session_token: session_token
  http:
    idle_conn_timeout: 0s
"#,
    ))
    .unwrap();
    assert!(val3.is_none());
    assert_eq!(cfg3.backend_cfg.s3.session_token, "session_token");
    assert!(S3Bucket::new(&cfg3.backend_cfg.s3, "test").is_ok());
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestParseConfig_PartSize
#[test]
fn test_s3_parse_config_part_size() {
    let (cfg, _) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: s3
s3:
  bucket_name: bucket-name
  endpoint: s3-endpoint
"#,
    ))
    .unwrap();
    assert_eq!(cfg.backend_cfg.s3.part_size, 0);

    let (cfg2, _) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: s3
s3:
  bucket_name: bucket-name
  endpoint: s3-endpoint
  part_size: 104857600
"#,
    ))
    .unwrap();
    assert_eq!(cfg2.backend_cfg.s3.part_size, 1024 * 1024 * 100);
    let bkt2 = S3Bucket::new(&cfg2.backend_cfg.s3, "test").unwrap();
    assert_eq!(bkt2.part_size(), 1024 * 1024 * 100);
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestParseConfig_OldSEEncryptionFieldShouldFail
#[test]
fn test_s3_parse_config_old_se_encryption_field_should_fail() {
    let res = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: s3
s3:
  bucket_name: bucket-name
  endpoint: s3-endpoint
  see_encryption: true
"#,
    ));
    assert!(res.is_err(), "unknown field see_encryption must fail");
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestParseConfig_ListObjectsV1
#[test]
fn test_s3_parse_config_list_objects_v1() {
    let (cfg1, _) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: s3
s3:
  bucket_name: bucket-name
  endpoint: s3-endpoint
"#,
    ))
    .unwrap();
    assert_eq!(cfg1.backend_cfg.s3.list_objects_version, "");
    assert!(!S3Bucket::new(&cfg1.backend_cfg.s3, "test")
        .unwrap()
        .list_objects_v1());

    let (cfg2, _) = load_blocks_bucket_from_yaml(&indent_yaml(
        r#"
backend: s3
s3:
  bucket_name: bucket-name
  endpoint: s3-endpoint
  list_objects_version: v1
"#,
    ))
    .unwrap();
    assert_eq!(cfg2.backend_cfg.s3.list_objects_version, "v1");
    assert!(S3Bucket::new(&cfg2.backend_cfg.s3, "test")
        .unwrap()
        .list_objects_v1());
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestBucket_getServerSideEncryption
#[test]
fn test_s3_bucket_get_server_side_encryption() {
    let base = || S3Config {
        endpoint: "localhost:80".into(),
        bucket_name: "test-bucket".into(),
        ..Default::default()
    };

    // Default config returns None
    let bkt = S3Bucket::new(&base(), "test").unwrap();
    assert_eq!(bkt.effective_sse(None), None);

    // SSE-S3
    let mut cfg = base();
    cfg.sse = S3SSEConfig {
        sse_type: SSE_S3.into(),
        ..Default::default()
    };
    let bkt = S3Bucket::new(&cfg, "test").unwrap();
    assert_eq!(bkt.effective_sse(None), Some(&ServerSideEncryption::S3));

    // SSE-KMS without context -> empty map JSON "{}"
    let mut cfg = base();
    cfg.sse = S3SSEConfig {
        sse_type: SSE_KMS.into(),
        kms_key_id: "key".into(),
        kms_encryption_context: String::new(),
    };
    let bkt = S3Bucket::new(&cfg, "test").unwrap();
    let mut headers = HeaderMap::new();
    bkt.effective_sse(None).unwrap().marshal(&mut headers).unwrap();
    assert_eq!(
        headers
            .get("x-amz-server-side-encryption-context")
            .unwrap()
            .to_str()
            .unwrap(),
        base64::engine::general_purpose::STANDARD.encode("{}")
    );

    // SSE-KMS with encryption context
    let mut cfg = base();
    cfg.sse = S3SSEConfig {
        sse_type: SSE_KMS.into(),
        kms_key_id: "key".into(),
        kms_encryption_context: r#"{"foo":"bar"}"#.into(),
    };
    let bkt = S3Bucket::new(&cfg, "test").unwrap();
    let mut headers = HeaderMap::new();
    bkt.effective_sse(None).unwrap().marshal(&mut headers).unwrap();
    let want_json = serde_json::to_vec(&BTreeMap::from([("foo", "bar")])).unwrap();
    assert_eq!(
        headers
            .get("x-amz-server-side-encryption-context")
            .unwrap()
            .to_str()
            .unwrap(),
        base64::engine::general_purpose::STANDARD.encode(want_json)
    );

    // Override wins over bucket config
    let mut cfg = base();
    cfg.sse = S3SSEConfig {
        sse_type: SSE_S3.into(),
        ..Default::default()
    };
    let bkt = S3Bucket::new(&cfg, "test").unwrap();
    let override_sse = build_thanos_sse(&S3SSEConfig {
        sse_type: SSE_KMS.into(),
        kms_key_id: "test".into(),
        kms_encryption_context: String::new(),
    })
    .unwrap()
    .unwrap();
    assert!(matches!(
        bkt.effective_sse(Some(&override_sse)),
        Some(ServerSideEncryption::Kms { key_id, .. }) if key_id == "test"
    ));
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestBucket_Get_ShouldReturnErrorIfServerTruncateResponse
#[tokio::test]
async fn test_s3_bucket_get_should_return_error_if_server_truncate_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let resp = "HTTP/1.1 200 OK\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nContent-Length: 100\r\nConnection: close\r\n\r\n12345";
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });

    let cfg = S3Config {
        bucket_name: "test-bucket".into(),
        endpoint: addr.to_string(),
        insecure: true,
        region: "test".into(),
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        ..Default::default()
    };
    let bkt = S3Bucket::new(&cfg, "test").unwrap();
    let err = bkt.get("test").await.unwrap_err();
    assert!(!err.is_obj_not_found(), "expected EOF/truncation error, got {err}");
    srv.abort();
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestParseConfig_CustomStorageClass
#[test]
fn test_s3_parse_config_custom_storage_class() {
    let cfg = S3Config {
        endpoint: "localhost:80".into(),
        bucket_name: "test-bucket".into(),
        storage_class: "STANDARD_IA".into(),
        ..Default::default()
    };
    assert!(cfg.validate().is_ok());
    let bkt = S3Bucket::new(&cfg, "test").unwrap();
    assert_eq!(bkt.storage_class(), "STANDARD_IA");
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestParseConfig_DefaultStorageClassIsZero
#[test]
fn test_s3_parse_config_default_storage_class_is_zero() {
    let cfg = S3Config {
        endpoint: "localhost:80".into(),
        bucket_name: "test-bucket".into(),
        ..Default::default()
    };
    let bkt = S3Bucket::new(&cfg, "test").unwrap();
    assert_eq!(bkt.storage_class(), "");
}

// Upstream: github.com/thanos-io/objstore/providers/s3/s3_test.go::TestNewBucketWithErrorRoundTripper
#[tokio::test]
async fn test_s3_new_bucket_with_error_round_tripper() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let body = "<Error><Code>MockedError</Code><Message>mocked roundtripper error</Message></Error>";
                let resp = format!(
                    "HTTP/1.1 400 Bad Request\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });

    let cfg = S3Config {
        endpoint: addr.to_string(),
        bucket_name: "test".into(),
        region: "us-east-1".into(),
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        insecure: true,
        ..Default::default()
    };
    let bkt = S3Bucket::new(&cfg, "test").unwrap();
    let err = bkt.get("test").await.unwrap_err();
    assert!(
        err.to_string().contains("mocked roundtripper error"),
        "expected mocked error, got: {err}"
    );
    srv.abort();
}
