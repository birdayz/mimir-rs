//! Wave 15 Track 3 regression tests:
//! - P1-1: GCS ServiceAccountJwtProvider OAuth2 token exchange, external_account, impersonated_service_account
//! - P1-3: KafkaProducer::produce_sync contended callback completion race
//! - P2-1: stop_rx.changed() Err handling (no 100%-CPU spin on stop_tx drop) & deferred HeapMerger spawn
//! - P2-2: KafkaWriterClient & KafkaProducer Weak-Arc background tasks + 250ms metrics poll interval

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use bytes::Bytes;
use mimir_config::{GCSConfig, KafkaConfig, OrderedConsumptionConfig, StringSliceCSV};
use mimir_storage::bucket::gcs;
use mimir_storage::ingest::heap_merger::MergedBatchConsumer;
use mimir_storage::ingest::{
    produce_results_err, FetchedRecord, GenericOffsetReader, HeapMerger, HeapMergerMetrics,
    KafkaProducer, KafkaProducerClient, KafkaWriterClient, KafkaWriterClientOptions,
    MultiClusterPartitionReader, ProduceContext, ProducerError, RecordBatchConsumer, WriterError,
    WriterRecord,
};
use object_store::path::Path as ObjectPath;
use object_store::PutPayload;
use prometheus::Registry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const TEST_RSA_PRIVATE_KEY_PEM_ESCAPED: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDitZM3/V3vVfqZ\nOl9Se568nQg5UejB/BNdESxtJn/JJtwGu+Ob9f3PAfJSjmRAw0C5rUcQ/aCTTOgX\nUaCM+ij5ltuCObF8Ql4VgkLFrIldm/ZP9bAatZW0JD8m2AzjceTC5Ya0pfWslPY2\nZcwnnFh4ZWPiCcoV+ebn0AcrezmFxLdisTJ+wWSOAzdeEon+s5S0r4RBKYzhVJnU\n60C1ju2W8ovtOXOqe9sWSSJzmPjT166DcLbJmY/IdYywM0SQYs8eaD1J8OmYz9FH\nFT7+B/H9u5s7gqgNTfOYR243N1foc9PMzZmE+qUNLpkODEXNdpVsMjSc3WJXhJzC\nu3K966E/AgMBAAECggEATLET/krNrhstzQv8FxEOn8uXbpyQ2p6FhmQ63Sm67Lci\nk3yYALh0hcSoztiwfWAP00AU3GCQI4ROtyS75TTzC4vG8gTGT7y7yxbns6ssROhv\nqJj6W+61l9zzKBbKNV7f/jZbHV1PAHTb3YcOSxAOtZvG8D5TiJ+jRyQOaN0klmVU\njQJ05hnKKCvctQlOwsg+iFj0lTk/YcRc64OzAPsZFFgbWNSI4VJc4y3C2yiGvAhf\nOpUSfuuCs3q3XKg7l6u4GxAJkwag9sECQfvbomof/2SUdvV1f5VFUIuB2m1OIXZD\nX0F67aEXza7bmKTtNdksWvKoJSd80i623xsNFvZrwQKBgQDyZMicjNH4Qz4nPK0y\nI1mdYCtqLaibY702gAQG5OqX9vAnlknpu0NMZpV78OJDN6IdCQD4OOPhVVHRV66r\nHDQEocZohSX5h0Kqfs4hAXmZqikeAGBmgPTtXVXsiTPKbbyEkm/oVlURcxlbdbrB\n4UCqZPmm9Pe8fHAOvu7wmYov0QKBgQDvb2e+bEH1ft7HpncDPQdR1GsCv7g5WSbL\npl9nBpbHBjvmdwnBD+h2yf+nPS8Z4QpeDzTRhpgl84EXnVf0j9WmT+uEgPxQPYAF\nsysVyPFwEquvyihTFn0iJ0HwdrZ7CpDTJeUFvZmeyyx7VOxdNS8gQlSflEVri+si\ncZzukc6UDwKBgB8vuF/r/lfizuxdcisxQSZuaSeQGFFl08JfViVM7qH+s4dkXr4j\nydTangEETRg44DK8JdPJ/gfP4mb80QxS5J5z8gMgnnIsZPdquTql5e9PZVrm0YBe\ntySTDasRPAifddHYxW0vLmLAXFLYIt8vDBLgSv6gJf+qQwCisDtkhdMBAoGAOm3s\nWjre6cLZKM2Fs4OcGqsEpjoVAeGwpvflxiWEe/MYgQhBmByCtSJcQfqbWOxi9Esv\nr4Xx8Sj4MsQR57uF/rz4E3DKoeYwLLPparqLuLbFsPNbXdNaHBCKddzycRpF91ED\nD6CSMX8RTtONjr1gz7WAIyUyv6j4a4JgMpPgXp8CgYEAlgvHMcs1FZNVCrwJBh6K\nyCboCDUef66G31ySdz9Og2v1UFMrpam+7tvnSBOIkIXPopaCXbVQm9ZgTFcawnx1\nlquyk/neOO5Y4+aoBlFVq1DET7z7UCgswctY6DP4Mi0ORAlzBQoOs2vfIon7hg5W\n9LdIcQjEjBSY2JSoc4phr1s=\n-----END PRIVATE KEY-----";

struct HttpReq {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

async fn read_http_req(s: &mut tokio::net::TcpStream) -> Option<HttpReq> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let header_end;
    loop {
        let n = s.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            header_end = pos;
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let req_line = lines.next().unwrap_or("");
    let mut parts = req_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();

    let mut headers = HashMap::new();
    let mut content_len = 0usize;
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            let kl = k.trim().to_ascii_lowercase();
            let vl = v.trim().to_string();
            if kl == "content-length" {
                content_len = vl.parse().unwrap_or(0);
            }
            headers.insert(kl, vl);
        }
    }

    let body_start = header_end + 4;
    while buf.len() < body_start + content_len {
        let n = s.read(&mut tmp).await.ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let body = buf[body_start..(body_start + content_len).min(buf.len())].to_vec();
    Some(HttpReq {
        method,
        path,
        headers,
        body,
    })
}

async fn write_http_resp(
    s: &mut tokio::net::TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) {
    let resp = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let _ = s.write_all(resp.as_bytes()).await;
    let _ = s.write_all(body).await;
    let _ = s.shutdown().await;
}

fn form_decode(body: &[u8]) -> HashMap<String, String> {
    let s = String::from_utf8_lossy(body);
    let url = reqwest::Url::parse(&format!("http://localhost/?{s}")).expect("valid query");
    url.query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

async fn spawn_mock_gcs_server(expected_bearer: &'static str) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_c = Arc::clone(&hits);
    let objects = Arc::new(parking_lot::Mutex::new(HashMap::<String, Vec<u8>>::new()));

    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                break;
            };
            let hits_c = Arc::clone(&hits_c);
            let objects = Arc::clone(&objects);
            tokio::spawn(async move {
                let Some(req) = read_http_req(&mut s).await else {
                    return;
                };
                hits_c.fetch_add(1, Ordering::SeqCst);
                let auth = req.headers.get("authorization").cloned().unwrap_or_default();
                if auth != format!("Bearer {expected_bearer}") {
                    write_http_resp(
                        &mut s,
                        "401 Unauthorized",
                        "application/xml",
                        format!("<Error><Code>AuthenticationRequired</Code><Message>got {auth}</Message></Error>").as_bytes(),
                    )
                    .await;
                    return;
                }
                match req.method.as_str() {
                    "PUT" => {
                        objects.lock().insert(req.path, req.body);
                        write_http_resp(&mut s, "200 OK", "text/plain", b"").await;
                    }
                    "GET" => {
                        let data = objects.lock().get(&req.path).cloned();
                        match data {
                            Some(d) => write_http_resp(&mut s, "200 OK", "application/octet-stream", &d).await,
                            None => write_http_resp(&mut s, "404 Not Found", "application/xml", b"<Error/>").await,
                        }
                    }
                    _ => write_http_resp(&mut s, "405 Method Not Allowed", "text/plain", b"").await,
                }
            });
        }
    });

    (format!("http://{addr}"), hits)
}

#[tokio::test]
async fn gcs_service_account_exchanges_jwt_for_oauth2_access_token_and_caches() {
    let token_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let token_addr = token_listener.local_addr().unwrap();
    let token_url = format!("http://{token_addr}/token");
    let token_hits = Arc::new(AtomicUsize::new(0));
    let token_hits_c = Arc::clone(&token_hits);
    let expected_aud = token_url.clone();

    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = token_listener.accept().await else {
                break;
            };
            let token_hits_c = Arc::clone(&token_hits_c);
            let expected_aud = expected_aud.clone();
            tokio::spawn(async move {
                let Some(req) = read_http_req(&mut s).await else {
                    return;
                };
                token_hits_c.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.method, "POST");
                assert_eq!(req.path, "/token");
                assert!(
                    req.headers
                        .get("content-type")
                        .is_some_and(|ct| ct.starts_with("application/x-www-form-urlencoded")),
                    "unexpected content-type: {:?}",
                    req.headers
                );
                let form = form_decode(&req.body);
                assert_eq!(
                    form.get("grant_type").map(String::as_str),
                    Some("urn:ietf:params:oauth:grant-type:jwt-bearer")
                );
                let assertion = form.get("assertion").expect("assertion present");
                let jwt_parts: Vec<&str> = assertion.split('.').collect();
                assert_eq!(jwt_parts.len(), 3, "expected 3-part JWS");
                let hdr: serde_json::Value =
                    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(jwt_parts[0]).unwrap()).unwrap();
                assert_eq!(hdr["alg"], "RS256");
                assert_eq!(hdr["kid"], "kid-1");
                let claims: serde_json::Value =
                    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(jwt_parts[1]).unwrap()).unwrap();
                assert_eq!(claims["iss"], "sa@proj.iam.gserviceaccount.com");
                assert_eq!(claims["aud"], expected_aud);
                let scope = claims["scope"].as_str().unwrap();
                assert!(scope.contains("devstorage.full_control"), "{scope}");
                assert!(scope.contains("cloud-platform"), "{scope}");
                assert!(claims["exp"].as_u64().unwrap() > claims["iat"].as_u64().unwrap());

                let resp_body =
                    br#"{"access_token":"ya29.exchanged_sa_tok","expires_in":3600,"token_type":"Bearer"}"#;
                write_http_resp(&mut s, "200 OK", "application/json", resp_body).await;
            });
        }
    });

    let (gcs_url, gcs_hits) = spawn_mock_gcs_server("ya29.exchanged_sa_tok").await;
    let sa_json = serde_json::json!({
        "type": "service_account",
        "client_email": "sa@proj.iam.gserviceaccount.com",
        "private_key_id": "kid-1",
        "private_key": TEST_RSA_PRIVATE_KEY_PEM_ESCAPED,
        "token_uri": token_url,
        "gcs_base_url": gcs_url,
    })
    .to_string();
    let cfg = GCSConfig {
        bucket_name: "bkt".into(),
        service_account: sa_json,
        max_retries: 1,
        ..Default::default()
    };
    let store = gcs::new_store(&cfg).unwrap();
    let p = ObjectPath::from("dir/block.bin");
    store
        .put(&p, PutPayload::from_static(b"hello-gcs"))
        .await
        .unwrap();
    let got1 = store.get(&p).await.unwrap().bytes().await.unwrap();
    let got2 = store.get(&p).await.unwrap().bytes().await.unwrap();
    assert_eq!(got1.as_ref(), b"hello-gcs");
    assert_eq!(got2.as_ref(), b"hello-gcs");
    assert_eq!(gcs_hits.load(Ordering::SeqCst), 3);
    assert_eq!(token_hits.load(Ordering::SeqCst), 1, "OAuth2 access token must be cached");
}

#[tokio::test]
async fn gcs_external_account_file_sts_and_impersonation_flow() {
    let dir = tempfile::tempdir().unwrap();
    let token_path = dir.path().join("oidc-token");
    std::fs::write(&token_path, "  eyJ.file.oidc.token\n").unwrap();

    let sts_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sts_addr = sts_listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = sts_listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let Some(req) = read_http_req(&mut s).await else {
                    return;
                };
                assert_eq!(req.method, "POST");
                assert_eq!(req.path, "/v1/token");
                let form = form_decode(&req.body);
                assert_eq!(
                    form.get("grant_type").map(String::as_str),
                    Some("urn:ietf:params:oauth:grant-type:token-exchange")
                );
                assert_eq!(
                    form.get("requested_token_type").map(String::as_str),
                    Some("urn:ietf:params:oauth:token-type:access_token")
                );
                assert_eq!(
                    form.get("subject_token_type").map(String::as_str),
                    Some("urn:ietf:params:oauth:token-type:jwt")
                );
                assert_eq!(
                    form.get("subject_token").map(String::as_str),
                    Some("eyJ.file.oidc.token")
                );
                assert_eq!(
                    form.get("audience").map(String::as_str),
                    Some("//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/p/providers/k8s")
                );
                write_http_resp(
                    &mut s,
                    "200 OK",
                    "application/json",
                    br#"{"access_token":"sts_tok_1","expires_in":3600,"token_type":"Bearer"}"#,
                )
                .await;
            });
        }
    });

    let iam_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let iam_addr = iam_listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = iam_listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let Some(req) = read_http_req(&mut s).await else {
                    return;
                };
                assert_eq!(req.method, "POST");
                assert_eq!(
                    req.headers.get("authorization").map(String::as_str),
                    Some("Bearer sts_tok_1")
                );
                let body_json: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let scopes = body_json["scope"].as_array().unwrap();
                assert!(scopes.iter().any(|v| v == "https://www.googleapis.com/auth/devstorage.full_control"));
                write_http_resp(
                    &mut s,
                    "200 OK",
                    "application/json",
                    br#"{"accessToken":"imp_tok_2","expireTime":"2099-01-01T00:00:00Z"}"#,
                )
                .await;
            });
        }
    });

    let (gcs_url, _) = spawn_mock_gcs_server("imp_tok_2").await;
    let ext_json = serde_json::json!({
        "type": "external_account",
        "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/p/providers/k8s",
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "token_url": format!("http://{sts_addr}/v1/token"),
        "service_account_impersonation_url": format!("http://{iam_addr}/v1/projects/-/serviceAccounts/target@proj.iam.gserviceaccount.com:generateAccessToken"),
        "credential_source": {
            "file": token_path.to_string_lossy()
        },
        "gcs_base_url": gcs_url
    })
    .to_string();

    let store = gcs::new_store(&GCSConfig {
        bucket_name: "bkt".into(),
        service_account: ext_json,
        max_retries: 1,
        ..Default::default()
    })
    .unwrap();
    let p = ObjectPath::from("wif/obj");
    store.put(&p, PutPayload::from_static(b"wif-ok")).await.unwrap();
    let got = store.get(&p).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"wif-ok");
}

#[tokio::test]
async fn gcs_external_account_url_source_direct_sts_and_impersonated_sa_flows() {
    // 1. URL-sourced external_account without impersonation (direct STS token)
    let oidc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let oidc_addr = oidc_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = oidc_listener.accept().await.unwrap();
        let req = read_http_req(&mut s).await.unwrap();
        assert_eq!(req.method, "GET");
        assert_eq!(req.headers.get("metadata").map(String::as_str), Some("true"));
        write_http_resp(
            &mut s,
            "200 OK",
            "application/json",
            br#"{"access_token":"url_subj_tok"}"#,
        )
        .await;
    });

    let sts_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sts_addr = sts_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = sts_listener.accept().await.unwrap();
        let req = read_http_req(&mut s).await.unwrap();
        let form = form_decode(&req.body);
        assert_eq!(form.get("subject_token").map(String::as_str), Some("url_subj_tok"));
        assert!(
            req.headers
                .get("authorization")
                .is_some_and(|a| a.starts_with("Basic ")),
            "expected Basic client auth header"
        );
        write_http_resp(
            &mut s,
            "200 OK",
            "application/json",
            br#"{"access_token":"sts_direct_tok","expires_in":3600}"#,
        )
        .await;
    });

    let (gcs_url_1, _) = spawn_mock_gcs_server("sts_direct_tok").await;
    let ext_url_json = serde_json::json!({
        "type": "external_account",
        "audience": "//iam.googleapis.com/locations/global/workforcePools/wp/providers/p",
        "subject_token_type": "urn:ietf:params:oauth:token-type:id_token",
        "token_url": format!("http://{sts_addr}/v1/token"),
        "client_id": "cid",
        "client_secret": "csec",
        "credential_source": {
            "url": format!("http://{oidc_addr}/oidc"),
            "headers": { "Metadata": "true" },
            "format": { "type": "json", "subject_token_field_name": "access_token" }
        },
        "gcs_base_url": gcs_url_1
    })
    .to_string();

    let store1 = gcs::new_store(&GCSConfig {
        bucket_name: "bkt".into(),
        service_account: ext_url_json,
        max_retries: 1,
        ..Default::default()
    })
    .unwrap();
    let p1 = ObjectPath::from("url/obj");
    store1.put(&p1, PutPayload::from_static(b"url-ok")).await.unwrap();

    // 2. impersonated_service_account wrapping authorized_user source_credentials
    let au_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let au_addr = au_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = au_listener.accept().await.unwrap();
        let _req = read_http_req(&mut s).await.unwrap();
        write_http_resp(
            &mut s,
            "200 OK",
            "application/json",
            br#"{"access_token":"src_au_tok","expires_in":3600}"#,
        )
        .await;
    });

    let iam_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let iam_addr = iam_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = iam_listener.accept().await.unwrap();
        let req = read_http_req(&mut s).await.unwrap();
        assert_eq!(
            req.headers.get("authorization").map(String::as_str),
            Some("Bearer src_au_tok")
        );
        write_http_resp(
            &mut s,
            "200 OK",
            "application/json",
            br#"{"accessToken":"imp_sa_tok","expireTime":"2099-01-01T00:00:00Z"}"#,
        )
        .await;
    });

    let (gcs_url_2, _) = spawn_mock_gcs_server("imp_sa_tok").await;
    let imp_json = serde_json::json!({
        "type": "impersonated_service_account",
        "service_account_impersonation_url": format!("http://{iam_addr}/v1/projects/-/serviceAccounts/imp@proj.iam.gserviceaccount.com:generateAccessToken"),
        "delegates": ["projects/-/serviceAccounts/del@proj.iam.gserviceaccount.com"],
        "source_credentials": {
            "type": "authorized_user",
            "client_id": "cid",
            "client_secret": "csec",
            "refresh_token": "rt",
            "token_uri": format!("http://{au_addr}/token")
        },
        "gcs_base_url": gcs_url_2
    })
    .to_string();

    let store2 = gcs::new_store(&GCSConfig {
        bucket_name: "bkt".into(),
        service_account: imp_json,
        max_retries: 1,
        ..Default::default()
    })
    .unwrap();
    let p2 = ObjectPath::from("imp/obj");
    store2.put(&p2, PutPayload::from_static(b"imp-ok")).await.unwrap();
    assert_eq!(store2.get(&p2).await.unwrap().bytes().await.unwrap().as_ref(), b"imp-ok");
}

#[tokio::test]
async fn kafka_producer_produce_sync_never_drops_contended_errors() {
    struct ContendedMockClient;
    impl KafkaProducerClient for ContendedMockClient {
        fn produce(
            &self,
            record: WriterRecord,
            on_done: Box<dyn FnOnce(WriterRecord, Result<i64, ProducerError>) + Send>,
        ) {
            let p = record.partition;
            std::thread::spawn(move || {
                let res = if p == 37 {
                    Err(ProducerError::Other("injected broker failure".into()))
                } else {
                    Ok(p as i64)
                };
                on_done(record, res);
            });
        }
        fn buffered_produce_bytes(&self) -> i64 {
            0
        }
        fn buffered_produce_records(&self) -> i64 {
            0
        }
        fn close(&self) {}
    }

    // Even stronger contention: barrier-synchronized batch dispatch across 16 OS threads
    type ProduceDoneCb = Box<dyn FnOnce(WriterRecord, Result<i64, ProducerError>) + Send>;
    struct BarrierMockClient {
        pending: parking_lot::Mutex<Vec<(WriterRecord, ProduceDoneCb)>>,
        batch_size: usize,
    }
    impl KafkaProducerClient for BarrierMockClient {
        fn produce(
            &self,
            record: WriterRecord,
            on_done: Box<dyn FnOnce(WriterRecord, Result<i64, ProducerError>) + Send>,
        ) {
            let mut guard = self.pending.lock();
            guard.push((record, on_done));
            if guard.len() == self.batch_size {
                let items: Vec<_> = guard.drain(..).collect();
                let barrier = Arc::new(Barrier::new(items.len()));
                for (rec, cb) in items {
                    let b = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        b.wait();
                        let res = if rec.partition == 37 {
                            Err(ProducerError::Other("contended broker error".into()))
                        } else {
                            Ok(rec.partition as i64)
                        };
                        cb(rec, res);
                    });
                }
            }
        }
        fn buffered_produce_bytes(&self) -> i64 {
            0
        }
        fn buffered_produce_records(&self) -> i64 {
            0
        }
        fn close(&self) {}
    }

    let _ = ContendedMockClient;
    let batch_size = 64usize;
    let mock = Arc::new(BarrierMockClient {
        pending: parking_lot::Mutex::new(Vec::with_capacity(batch_size)),
        batch_size,
    });
    let producer = KafkaProducer::new(mock, 0, None);
    let ctx = ProduceContext::background();

    for iter in 0..25 {
        let records: Vec<WriterRecord> = (0..batch_size)
            .map(|i| WriterRecord {
                topic: "t".into(),
                partition: i as i32,
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                ..Default::default()
            })
            .collect();
        let results = producer.produce_sync(&ctx, records).await;
        assert_eq!(results.len(), batch_size, "iter {iter}: truncated results");
        let err = produce_results_err(&results).expect_err("iter {iter}: error was dropped");
        match err {
            WriterError::FailedPartitions { partitions, .. } => {
                assert_eq!(partitions, vec![37]);
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
    producer.close();
}

#[tokio::test]
async fn stop_tx_drop_exits_heap_merger_and_offset_reader_without_spinning() {
    // 1. Dropping HeapMerger without calling stop() or shutdown_and_drain() must drop the
    //    background task (and its captured consumer Arc) immediately instead of spinning.
    let sentinel = Arc::new(AtomicBool::new(false));
    let sentinel_weak = Arc::downgrade(&sentinel);
    let consumer: MergedBatchConsumer = Arc::new(move |_recs| {
        let _keep = Arc::clone(&sentinel);
        Box::pin(async { Ok(()) })
    });
    let cfg = OrderedConsumptionConfig {
        enabled: true,
        max_batch_records: 4,
        max_batch_wait: mimir_config::GoDuration::from_millis(50),
        ..Default::default()
    };
    let merger = HeapMerger::start(&cfg, consumer, HeapMergerMetrics::new(None));
    drop(merger);

    let dl = Instant::now() + Duration::from_secs(1);
    while sentinel_weak.strong_count() > 0 {
        assert!(
            Instant::now() < dl,
            "HeapMerger task did not exit after drop (stop_rx spin)"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // 2. Dropping GenericOffsetReader after start() without calling stop() drops stop_tx;
    //    its background poll loop must exit immediately instead of spinning at 100% CPU.
    let poll_sentinel = Arc::new(AtomicBool::new(false));
    let poll_sentinel_weak = Arc::downgrade(&poll_sentinel);
    let fetch_fn = Arc::new(move || {
        let _keep = Arc::clone(&poll_sentinel);
        Box::pin(async { Ok::<i64, String>(42) })
            as Pin<Box<dyn Future<Output = Result<i64, String>> + Send>>
    });
    let reader = GenericOffsetReader::new(fetch_fn, Duration::from_secs(60));
    reader.start().await;
    drop(reader);
    let dl = Instant::now() + Duration::from_secs(1);
    while poll_sentinel_weak.strong_count() > 0 {
        assert!(
            Instant::now() < dl,
            "GenericOffsetReader task did not exit after drop (stop_rx spin)"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[test]
fn multi_cluster_partition_reader_new_outside_tokio_runtime_does_not_panic() {
    struct NoopConsumer;
    impl RecordBatchConsumer for NoopConsumer {
        fn consume(
            &self,
            _records: Vec<FetchedRecord>,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    let cfg = KafkaConfig {
        address: StringSliceCSV(vec!["127.0.0.1:9092".to_string()]),
        topic: "ingest-test".to_string(),
        auto_create_topic_enabled: false,
        consume_from_position_at_startup: "start".to_string(),
        ..Default::default()
    };
    let ordered_cfg = OrderedConsumptionConfig {
        enabled: true,
        max_batch_records: 8,
        max_batch_wait: mimir_config::GoDuration::from_millis(10),
        ..Default::default()
    };

    // Run on a plain std::thread with NO Tokio runtime!
    let reader = std::thread::spawn(move || {
        MultiClusterPartitionReader::new(
            &[cfg.clone(), cfg],
            &ordered_cfg,
            0,
            "ing-0",
            vec![None, None],
            Arc::new(NoopConsumer),
            None,
        )
    })
    .join()
    .expect("thread panicked")
    .expect("MultiClusterPartitionReader::new failed");

    drop(reader);
}

#[tokio::test]
async fn kafka_producer_and_writer_client_drop_without_close_do_not_leak_and_tick_at_250ms() {
    let cfg = KafkaConfig {
        address: StringSliceCSV(vec!["127.0.0.1:9092".to_string()]),
        topic: "ingest-test".to_string(),
        auto_create_topic_enabled: false,
        ..Default::default()
    };

    let reg = Registry::new();
    let client = KafkaWriterClient::new(&cfg, 2, None, KafkaWriterClientOptions::default()).unwrap();
    let client_weak = Arc::downgrade(&client);
    let producer = KafkaProducer::new_with_prefix(client, 0, "test_", Some(&reg));
    let producer_weak = Arc::downgrade(&producer);

    // Wait 350ms to verify 250ms tick cadence (expected 2 observations: t=0 and t=250ms, not ~14 at 25ms)
    tokio::time::sleep(Duration::from_millis(350)).await;
    let mfs = reg.gather();
    let dist = mfs
        .iter()
        .find(|m| m.get_name() == "test_buffered_produce_bytes_distribution")
        .expect("metric registered");
    let count = dist.get_metric()[0].get_summary().get_sample_count();
    assert!(
        (1..=3).contains(&count),
        "expected ~2 samples over 350ms at 250ms interval, got {count}"
    );

    // Drop producer WITHOUT calling .close(); both producer and underlying client must be freed!
    drop(producer);
    let dl = Instant::now() + Duration::from_secs(1);
    while producer_weak.strong_count() > 0 || client_weak.strong_count() > 0 {
        assert!(
            Instant::now() < dl,
            "background tasks leaked Arc references after drop (producer={}, client={})",
            producer_weak.strong_count(),
            client_weak.strong_count()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
