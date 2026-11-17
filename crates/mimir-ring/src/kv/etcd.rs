//! Etcd v3 HTTP/JSON gateway KV client.
//!
//! Ported from github.com/grafana/dskit/kv/etcd/etcd.go (TLS: see [`super::etcd_tls`]).

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use mimir_common::backoff::{Backoff, BackoffConfig};
use mimir_common::services::ServiceContext;
use mimir_config::ingester::EtcdConfig;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

use super::{BoxFuture, CasFn, CasOutcome, Codec, KvClient, KvError};

/// reqwest's error text plus its source chain (the TLS / connect cause is only in the sources).
fn format_reqwest_error(err: &reqwest::Error) -> String {
    let mut msg = err.to_string();
    let mut source = std::error::Error::source(err);
    while let Some(cause) = source {
        msg.push_str(": ");
        msg.push_str(&cause.to_string());
        source = cause.source();
    }
    msg
}

fn b64_encode(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn b64_decode(s: &str) -> Result<Vec<u8>, KvError> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| KvError::Codec(format!("base64 decode: {e}")))
}

fn get_prefix_range_end(prefix: &str) -> Vec<u8> {
    let mut end = prefix.as_bytes().to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xff {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    vec![0]
}

#[derive(Serialize)]
struct RangeRequest {
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    range_end: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    keys_only: bool,
}

#[derive(Deserialize)]
struct EtcdKeyValue {
    key: String,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    version: Option<serde_json::Value>,
}

impl EtcdKeyValue {
    fn version_i64(&self) -> i64 {
        match &self.version {
            Some(serde_json::Value::Number(n)) => n.as_i64().unwrap_or(0),
            Some(serde_json::Value::String(s)) => s.parse::<i64>().unwrap_or(0),
            _ => 0,
        }
    }
}

#[derive(Deserialize)]
struct RangeResponse {
    #[serde(default)]
    kvs: Vec<EtcdKeyValue>,
}

#[derive(Serialize)]
struct DeleteRangeRequest {
    key: String,
}

#[derive(Serialize)]
struct TxnCompare {
    key: String,
    result: &'static str,
    target: &'static str,
    version: i64,
}

#[derive(Serialize)]
struct RequestPut {
    key: String,
    value: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RequestOp {
    request_put: RequestPut,
}

#[derive(Serialize)]
struct TxnRequest {
    compare: Vec<TxnCompare>,
    success: Vec<RequestOp>,
}

#[derive(Deserialize)]
struct TxnResponse {
    #[serde(default)]
    succeeded: bool,
}

#[derive(Serialize)]
struct WatchCreateRequest {
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    range_end: Option<String>,
}

#[derive(Serialize)]
struct WatchRequest {
    create_request: WatchCreateRequest,
}

#[derive(Deserialize)]
struct WatchEvent {
    #[serde(default)]
    kv: Option<EtcdKeyValue>,
    #[serde(default, rename = "type")]
    event_type: Option<String>,
}

#[derive(Deserialize)]
struct WatchResultWrapper {
    #[serde(default)]
    result: Option<WatchResponse>,
}

#[derive(Deserialize)]
struct WatchResponse {
    #[serde(default)]
    events: Vec<WatchEvent>,
    #[serde(default)]
    canceled: bool,
}

pub struct EtcdKv {
    http: reqwest::Client,
    endpoints: Vec<String>,
    cfg: EtcdConfig,
    codec: Arc<dyn Codec>,
    auth_token: RwLock<Option<String>>,
}

impl EtcdKv {
    pub fn new(cfg: EtcdConfig, codec: Arc<dyn Codec>) -> Result<Self, KvError> {
        if cfg.endpoints.is_empty() {
            return Err(KvError::InvalidConfig(
                "etcd endpoints cannot be empty".to_string(),
            ));
        }

        // dskit `Config.GetTLS`: TLS settings only apply with `tls_enabled`; otherwise `https://`
        // endpoints still get TLS with the host roots (clientv3 `CredsRequire`).
        let tls = if cfg.tls_enabled {
            super::etcd_tls::client_config(&cfg)
        } else {
            super::etcd_tls::default_client_config()
        }
        .map_err(|e| KvError::Other(format!("{}: {e}", super::etcd_tls::TLS_INIT_ERROR_PREFIX)))?;

        let timeout: Duration = cfg.dial_timeout.as_std().unwrap_or(Duration::ZERO);
        let mut builder = reqwest::Client::builder().use_preconfigured_tls(tls);
        if !timeout.is_zero() {
            builder = builder.connect_timeout(timeout);
        }
        let http = builder
            .build()
            .map_err(|e| KvError::Http(format!("build etcd http client: {e}")))?;

        let endpoints = cfg
            .endpoints
            .iter()
            .map(|ep| {
                let ep = ep.trim_end_matches('/');
                if ep.starts_with("http://") || ep.starts_with("https://") {
                    ep.to_string()
                } else if cfg.tls_enabled {
                    format!("https://{ep}")
                } else {
                    format!("http://{ep}")
                }
            })
            .collect();

        Ok(Self {
            http,
            endpoints,
            cfg,
            codec,
            auth_token: RwLock::new(None),
        })
    }

    fn endpoint(&self, idx: usize) -> &str {
        &self.endpoints[idx % self.endpoints.len()]
    }

    async fn ensure_auth(&self) -> Result<Option<String>, KvError> {
        if self.cfg.username.is_empty() {
            return Ok(None);
        }
        if let Some(tok) = self.auth_token.read().await.as_ref() {
            return Ok(Some(tok.clone()));
        }
        #[derive(Serialize)]
        struct AuthReq<'a> {
            name: &'a str,
            password: &'a str,
        }
        #[derive(Deserialize)]
        struct AuthResp {
            token: String,
        }

        let url = format!("{}/v3/auth/authenticate", self.endpoint(0));
        let resp = self
            .http
            .post(&url)
            .json(&AuthReq {
                name: &self.cfg.username,
                password: &self.cfg.password,
            })
            .send()
            .await
            .map_err(|e| KvError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(KvError::Http(format!("etcd auth failed: {}", resp.status())));
        }
        let parsed: AuthResp = resp
            .json()
            .await
            .map_err(|e| KvError::Http(format!("decode etcd auth: {e}")))?;
        *self.auth_token.write().await = Some(parsed.token.clone());
        Ok(Some(parsed.token))
    }

    async fn post_json<Req: Serialize, Resp: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &Req,
    ) -> Result<Resp, KvError> {
        let mut headers = HeaderMap::new();
        if let Some(tok) = self.ensure_auth().await? {
            if let Ok(val) = HeaderValue::from_str(&tok) {
                headers.insert("Authorization", val);
            }
        }
        // etcd clientv3 issues every RPC with `grpc.WaitForReady(true)`: while no endpoint can be
        // connected to (refused, TLS handshake failure, ...) the call blocks and reconnects with
        // gRPC's connection backoff (1s base, x1.6, max 120s) until the caller gives up (the
        // caller's context; here: the future is dropped). HTTP-level errors fail immediately.
        let mut reconnect = Backoff::new(BackoffConfig {
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(120),
            max_retries: 0,
        });
        loop {
            let mut last_err = None;
            let mut responded = false;
            for idx in 0..self.endpoints.len() {
                let url = format!("{}{path}", self.endpoint(idx));
                match self
                    .http
                    .post(&url)
                    .headers(headers.clone())
                    .json(body)
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        return resp
                            .json::<Resp>()
                            .await
                            .map_err(|e| KvError::Http(format!("decode etcd {path}: {e}")));
                    }
                    Ok(resp) => {
                        responded = true;
                        last_err = Some(KvError::Http(format!(
                            "etcd {path} returned {}",
                            resp.status()
                        )));
                    }
                    Err(e) => {
                        last_err = Some(KvError::Http(format_reqwest_error(&e)));
                    }
                }
            }
            let Some(err) = last_err else {
                return Err(KvError::Http("no etcd endpoints".to_string()));
            };
            if responded {
                return Err(err);
            }
            warn!(path, err = %err, "etcd endpoints unavailable, waiting for a connection");
            reconnect.wait().await;
        }
    }
}

impl KvClient for EtcdKv {
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
        Box::pin(async move {
            let req = RangeRequest {
                key: b64_encode(prefix.as_bytes()),
                range_end: Some(b64_encode(&get_prefix_range_end(prefix))),
                keys_only: true,
            };
            let resp: RangeResponse = self.post_json("/v3/kv/range", &req).await?;
            let mut keys = Vec::with_capacity(resp.kvs.len());
            for kv in resp.kvs {
                let raw = b64_decode(&kv.key)?;
                let s = String::from_utf8(raw)
                    .map_err(|e| KvError::Codec(format!("etcd key utf8: {e}")))?;
                keys.push(s);
            }
            Ok(keys)
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        Box::pin(async move {
            let req = RangeRequest {
                key: b64_encode(key.as_bytes()),
                range_end: None,
                keys_only: false,
            };
            let resp: RangeResponse = self.post_json("/v3/kv/range", &req).await?;
            if resp.kvs.is_empty() {
                return Ok(None);
            }
            if resp.kvs.len() != 1 {
                return Err(KvError::Other(format!(
                    "got {} kvs, expected 1 or 0",
                    resp.kvs.len()
                )));
            }
            let val_bytes = match resp.kvs[0].value.as_deref() {
                Some(s) if !s.is_empty() => b64_decode(s)?,
                _ => Vec::new(),
            };
            Ok(Some(self.codec.decode(&val_bytes)?))
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let req = DeleteRangeRequest {
                key: b64_encode(key.as_bytes()),
            };
            let _: serde_json::Value = self.post_json("/v3/kv/deleterange", &req).await?;
            Ok(())
        })
    }

    fn cas<'a>(&'a self, key: &'a str, mut f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let max_retries = self.cfg.max_retries.max(1) as usize;
            let mut last_err: Option<KvError> = None;
            let key_b64 = b64_encode(key.as_bytes());

            for _ in 0..max_retries {
                let range_req = RangeRequest {
                    key: key_b64.clone(),
                    range_end: None,
                    keys_only: false,
                };
                let resp: RangeResponse = match self.post_json("/v3/kv/range", &range_req).await {
                    Ok(r) => r,
                    Err(e) => {
                        error!(key, err = %e, "error getting key from etcd");
                        last_err = Some(e);
                        continue;
                    }
                };

                let mut revision = 0i64;
                let decoded = if let Some(first) = resp.kvs.first() {
                    revision = first.version_i64();
                    let val_bytes = match first.value.as_deref() {
                        Some(s) if !s.is_empty() => match b64_decode(s) {
                            Ok(b) => b,
                            Err(e) => {
                                last_err = Some(e);
                                continue;
                            }
                        },
                        _ => Vec::new(),
                    };
                    match self.codec.decode(&val_bytes) {
                        Ok(d) => Some(d),
                        Err(e) => {
                            error!(key, err = %e, "error decoding etcd key");
                            last_err = Some(e);
                            continue;
                        }
                    }
                } else {
                    None
                };

                let outcome = match f(decoded.as_deref()) {
                    Ok(o) => o,
                    Err(cb_err) => {
                        if !cb_err.retry {
                            return Err(cb_err.err);
                        }
                        last_err = Some(cb_err.err);
                        continue;
                    }
                };

                let new_val = match outcome {
                    CasOutcome::NoChange => return Ok(()),
                    CasOutcome::Update(v) => v,
                };

                let encoded = match self.codec.encode(new_val.as_ref()) {
                    Ok(b) => b,
                    Err(e) => {
                        last_err = Some(e);
                        continue;
                    }
                };

                let txn_req = TxnRequest {
                    compare: vec![TxnCompare {
                        key: key_b64.clone(),
                        result: "EQUAL",
                        target: "VERSION",
                        version: revision,
                    }],
                    success: vec![RequestOp {
                        request_put: RequestPut {
                            key: key_b64.clone(),
                            value: b64_encode(&encoded),
                        },
                    }],
                };
                let txn_resp: TxnResponse = match self.post_json("/v3/kv/txn", &txn_req).await {
                    Ok(r) => r,
                    Err(e) => {
                        error!(key, err = %e, "error CASing etcd key");
                        last_err = Some(e);
                        continue;
                    }
                };
                if !txn_resp.succeeded {
                    debug!(key, revision, "failed to CAS, version mismatch in etcd");
                    continue;
                }
                return Ok(());
            }

            Err(last_err.unwrap_or_else(|| KvError::CasFailed {
                key: key.to_string(),
                reason: "failed to CAS".to_string(),
            }))
        })
    }

    fn watch_key<'a>(
        &'a self,
        key: &'a str,
        cancel: ServiceContext,
        mut f: Box<dyn FnMut(Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.watch_impl(
                key,
                None,
                cancel,
                move |_, val| f(val),
            )
            .await
        })
    }

    fn watch_prefix<'a>(
        &'a self,
        prefix: &'a str,
        cancel: ServiceContext,
        mut f: Box<dyn FnMut(&str, Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let range_end = get_prefix_range_end(prefix);
            self.watch_impl(
                prefix,
                Some(range_end),
                cancel,
                move |k, val| f(k, val),
            )
            .await
        })
    }
}

impl EtcdKv {
    async fn watch_impl<F>(
        &self,
        key: &str,
        range_end: Option<Vec<u8>>,
        cancel: ServiceContext,
        mut on_event: F,
    ) where
        F: FnMut(&str, Box<dyn Any + Send + Sync>) -> bool + Send,
    {
        let mut backoff = Backoff::new(BackoffConfig {
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            max_retries: 0,
        });

        let req_body = WatchRequest {
            create_request: WatchCreateRequest {
                key: b64_encode(key.as_bytes()),
                range_end: range_end.as_deref().map(b64_encode),
            },
        };

        while backoff.ongoing() && !cancel.is_canceled() {
            let url = format!("{}/v3/watch", self.endpoint(0));
            let resp_res = tokio::select! {
                _ = cancel.canceled() => return,
                r = self.http.post(&url).json(&req_body).send() => r,
            };

            let mut resp = match resp_res {
                Ok(r) if r.status().is_success() => {
                    backoff.reset();
                    r
                }
                Ok(r) => {
                    error!(key, status = %r.status(), "etcd watch HTTP error");
                    tokio::select! {
                        _ = cancel.canceled() => return,
                        _ = backoff.wait() => continue,
                    }
                }
                Err(e) => {
                    error!(key, err = %e, "etcd watch error");
                    tokio::select! {
                        _ = cancel.canceled() => return,
                        _ = backoff.wait() => continue,
                    }
                }
            };

            let mut buf = Vec::new();
            loop {
                let chunk_res = tokio::select! {
                    _ = cancel.canceled() => return,
                    c = resp.chunk() => c,
                };
                match chunk_res {
                    Ok(Some(chunk)) => {
                        buf.extend_from_slice(&chunk);
                        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                            let line = buf.drain(..=pos).collect::<Vec<u8>>();
                            let trimmed = line.trim_ascii();
                            if trimmed.is_empty() {
                                continue;
                            }
                            if let Ok(wrapper) = serde_json::from_slice::<WatchResultWrapper>(trimmed) {
                                if let Some(wr) = wrapper.result {
                                    if wr.canceled {
                                        break;
                                    }
                                    for ev in wr.events {
                                        if ev.event_type.as_deref() == Some("DELETE") {
                                            continue;
                                        }
                                        let Some(kv) = ev.kv else { continue };
                                        if kv.version_i64() == 0 && kv.value.is_none() {
                                            continue;
                                        }
                                        let Ok(raw_key) = b64_decode(&kv.key) else { continue };
                                        let Ok(key_str) = String::from_utf8(raw_key) else { continue };
                                        let val_bytes = match kv.value.as_deref() {
                                            Some(s) if !s.is_empty() => match b64_decode(s) {
                                                Ok(b) => b,
                                                Err(_) => continue,
                                            },
                                            _ => Vec::new(),
                                        };
                                        match self.codec.decode(&val_bytes) {
                                            Ok(decoded) => {
                                                if !on_event(&key_str, decoded) {
                                                    return;
                                                }
                                            }
                                            Err(e) => {
                                                error!(key = %key_str, err = %e, "error decoding etcd watch event");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        error!(key, err = %e, "etcd watch stream error");
                        break;
                    }
                }
            }

            tokio::select! {
                _ = cancel.canceled() => return,
                _ = backoff.wait() => {}
            }
        }
    }
}
