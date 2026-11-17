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
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

use super::{BoxFuture, CasFn, CasOutcome, Codec, KeyDeleted, KvClient, KvError};

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

/// Returns `(key, range_end)` matching `clientv3.WithPrefix()` (`op.go:424-433`):
/// when `prefix` is empty, both `key` and `range_end` are `[0]` (`"\0"`).
fn prefix_range(prefix: &str) -> (Vec<u8>, Vec<u8>) {
    if prefix.is_empty() {
        (vec![0], vec![0])
    } else {
        (prefix.as_bytes().to_vec(), get_prefix_range_end(prefix))
    }
}

fn parse_i64_opt(v: &Option<serde_json::Value>) -> i64 {
    match v {
        Some(serde_json::Value::Number(n)) => n.as_i64().unwrap_or(0),
        Some(serde_json::Value::String(s)) => s.parse::<i64>().unwrap_or(0),
        _ => 0,
    }
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
struct EtcdResponseHeader {
    #[serde(default)]
    revision: Option<serde_json::Value>,
}

impl EtcdResponseHeader {
    fn revision_i64(&self) -> i64 {
        parse_i64_opt(&self.revision)
    }
}

#[derive(Deserialize)]
struct EtcdKeyValue {
    key: String,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    version: Option<serde_json::Value>,
    #[serde(default)]
    mod_revision: Option<serde_json::Value>,
}

impl EtcdKeyValue {
    fn version_i64(&self) -> i64 {
        parse_i64_opt(&self.version)
    }

    fn mod_revision_i64(&self) -> i64 {
        parse_i64_opt(&self.mod_revision)
    }
}

#[derive(Deserialize)]
struct RangeResponse {
    #[serde(default)]
    header: Option<EtcdResponseHeader>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    start_revision: Option<i64>,
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
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct WatchResponse {
    #[serde(default)]
    header: Option<EtcdResponseHeader>,
    #[serde(default)]
    events: Vec<WatchEvent>,
    #[serde(default)]
    canceled: bool,
    #[serde(default)]
    compact_revision: Option<serde_json::Value>,
    #[serde(default)]
    cancel_reason: Option<String>,
}

impl WatchResponse {
    fn header_revision(&self) -> i64 {
        self.header
            .as_ref()
            .map(EtcdResponseHeader::revision_i64)
            .unwrap_or(0)
    }

    fn compact_revision_i64(&self) -> i64 {
        parse_i64_opt(&self.compact_revision)
    }
}

enum LineAction {
    Continue,
    BreakStream,
    Compacted(i64),
    StopWatching,
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

    async fn invalidate_auth(&self) {
        *self.auth_token.write().await = None;
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

        let mut last_err = None;
        for idx in 0..self.endpoints.len() {
            let url = format!("{}/v3/auth/authenticate", self.endpoint(idx));
            match self
                .http
                .post(&url)
                .json(&AuthReq {
                    name: &self.cfg.username,
                    password: &self.cfg.password,
                })
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    let parsed: AuthResp = resp
                        .json()
                        .await
                        .map_err(|e| KvError::Http(format!("decode etcd auth: {e}")))?;
                    *self.auth_token.write().await = Some(parsed.token.clone());
                    return Ok(Some(parsed.token));
                }
                Ok(resp) => {
                    last_err = Some(KvError::Http(format!("etcd auth failed: {}", resp.status())));
                }
                Err(e) => {
                    last_err = Some(KvError::Http(format_reqwest_error(&e)));
                }
            }
        }
        Err(last_err.unwrap_or_else(|| KvError::Http("no etcd endpoints".to_string())))
    }

    async fn auth_headers(&self) -> Result<HeaderMap, KvError> {
        let mut headers = HeaderMap::new();
        if let Some(tok) = self.ensure_auth().await? {
            if let Ok(val) = HeaderValue::from_str(&tok) {
                headers.insert("Authorization", val);
            }
        }
        Ok(headers)
    }

    async fn post_json<Req: Serialize, Resp: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &Req,
    ) -> Result<Resp, KvError> {
        // etcd clientv3 issues every RPC with `grpc.WaitForReady(true)`: while no endpoint can be
        // connected to (refused, TLS handshake failure, ...) the call blocks and reconnects with
        // gRPC's connection backoff (1s base, x1.6, max 120s) until the caller gives up (the
        // caller's context; here: the future is dropped). HTTP-level errors fail immediately
        // (except 401 when auth is enabled, which refreshes the cached token once).
        let mut reconnect = Backoff::new(BackoffConfig {
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(120),
            max_retries: 0,
        });
        let mut refreshed_auth = false;
        loop {
            let headers = match self.auth_headers().await {
                Ok(h) => h,
                Err(e) => {
                    warn!(path, err = %e, "etcd auth failed while connecting, retrying");
                    reconnect.wait().await;
                    continue;
                }
            };
            let mut last_err = None;
            let mut responded = false;
            let mut need_auth_retry = false;
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
                    Ok(resp)
                        if resp.status() == StatusCode::UNAUTHORIZED
                            && !self.cfg.username.is_empty()
                            && !refreshed_auth =>
                    {
                        refreshed_auth = true;
                        need_auth_retry = true;
                        self.invalidate_auth().await;
                        break;
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
            if need_auth_retry {
                continue;
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
            let (key, range_end) = prefix_range(prefix);
            let req = RangeRequest {
                key: b64_encode(&key),
                range_end: Some(b64_encode(&range_end)),
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
                key.as_bytes(),
                None,
                true,
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
            let (req_key, range_end) = prefix_range(prefix);
            self.watch_impl(
                prefix,
                &req_key,
                Some(range_end),
                false,
                cancel,
                move |k, val| f(k, val),
            )
            .await
        })
    }
}

impl EtcdKv {
    fn process_watch_line<F>(
        &self,
        key_label: &str,
        trimmed: &[u8],
        notify_on_delete: bool,
        start_revision: &mut i64,
        backoff: &mut Backoff,
        on_event: &mut F,
    ) -> LineAction
    where
        F: FnMut(&str, Box<dyn Any + Send + Sync>) -> bool + Send,
    {
        let wrapper = match serde_json::from_slice::<WatchResultWrapper>(trimmed) {
            Ok(w) => w,
            Err(e) => {
                warn!(key = key_label, err = %e, "failed to parse etcd watch response line");
                return LineAction::Continue;
            }
        };

        if let Some(err_val) = wrapper.error {
            error!(key = key_label, err = %err_val, "etcd watch stream returned error");
            return LineAction::BreakStream;
        }

        let Some(wr) = wrapper.result else {
            return LineAction::Continue;
        };

        if wr.canceled {
            let compact_rev = wr.compact_revision_i64();
            if compact_rev > 0 {
                warn!(
                    key = key_label,
                    compact_revision = compact_rev,
                    reason = wr.cancel_reason.as_deref().unwrap_or(""),
                    "etcd watch canceled due to compaction; recovering via range GET"
                );
                return LineAction::Compacted(compact_rev);
            }
            warn!(
                key = key_label,
                reason = wr.cancel_reason.as_deref().unwrap_or(""),
                "etcd watch canceled"
            );
            return LineAction::BreakStream;
        }

        backoff.reset();
        let hdr_rev = wr.header_revision();
        if hdr_rev > 0 {
            *start_revision = (*start_revision).max(hdr_rev + 1);
        }

        for ev in wr.events {
            let is_delete = ev.event_type.as_deref() == Some("DELETE")
                || ev
                    .kv
                    .as_ref()
                    .is_some_and(|kv| kv.version_i64() == 0 && kv.value.is_none());
            let Some(kv) = ev.kv else { continue };
            let mod_rev = kv.mod_revision_i64();
            if mod_rev > 0 {
                *start_revision = (*start_revision).max(mod_rev + 1);
            }
            let Ok(raw_key) = b64_decode(&kv.key) else {
                continue;
            };
            let Ok(key_str) = String::from_utf8(raw_key) else {
                continue;
            };

            if is_delete {
                if notify_on_delete && !on_event(&key_str, Box::new(KeyDeleted)) {
                    return LineAction::StopWatching;
                }
                continue;
            }

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
                        return LineAction::StopWatching;
                    }
                }
                Err(e) => {
                    error!(key = %key_str, err = %e, "error decoding etcd watch event");
                }
            }
        }

        LineAction::Continue
    }

    async fn watch_impl<F>(
        &self,
        key_label: &str,
        req_key: &[u8],
        range_end: Option<Vec<u8>>,
        notify_on_delete: bool,
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

        let mut start_revision: i64 = 0;
        let mut ep_idx: usize = 0;

        while backoff.ongoing() && !cancel.is_canceled() {
            let headers = match tokio::select! {
                _ = cancel.canceled() => return,
                r = self.auth_headers() => r,
            } {
                Ok(h) => h,
                Err(e) => {
                    error!(key = key_label, err = %e, "etcd watch auth error");
                    tokio::select! {
                        _ = cancel.canceled() => return,
                        _ = backoff.wait() => continue,
                    }
                }
            };

            let req_body = WatchRequest {
                create_request: WatchCreateRequest {
                    key: b64_encode(req_key),
                    range_end: range_end.as_deref().map(b64_encode),
                    start_revision: (start_revision > 0).then_some(start_revision),
                },
            };

            let mut connected_resp = None;
            let mut unauthorized = false;
            for attempt in 0..self.endpoints.len() {
                let idx = (ep_idx + attempt) % self.endpoints.len();
                let url = format!("{}/v3/watch", self.endpoint(idx));
                let resp_res = tokio::select! {
                    _ = cancel.canceled() => return,
                    r = self.http.post(&url).headers(headers.clone()).json(&req_body).send() => r,
                };
                match resp_res {
                    Ok(r) if r.status().is_success() => {
                        ep_idx = (idx + 1) % self.endpoints.len();
                        connected_resp = Some(r);
                        break;
                    }
                    Ok(r)
                        if r.status() == StatusCode::UNAUTHORIZED
                            && !self.cfg.username.is_empty() =>
                    {
                        warn!(key = key_label, endpoint = %self.endpoint(idx), "etcd watch unauthorized, refreshing auth token");
                        self.invalidate_auth().await;
                        unauthorized = true;
                        break;
                    }
                    Ok(r) => {
                        error!(key = key_label, endpoint = %self.endpoint(idx), status = %r.status(), "etcd watch HTTP error");
                    }
                    Err(e) => {
                        error!(key = key_label, endpoint = %self.endpoint(idx), err = %e, "etcd watch error");
                    }
                }
            }

            if unauthorized {
                continue;
            }

            let Some(mut resp) = connected_resp else {
                ep_idx = (ep_idx + 1) % self.endpoints.len();
                tokio::select! {
                    _ = cancel.canceled() => return,
                    _ = backoff.wait() => continue,
                }
            };

            let mut buf = Vec::new();
            let mut compacted_rev: Option<i64> = None;
            'stream: loop {
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
                            match self.process_watch_line(
                                key_label,
                                trimmed,
                                notify_on_delete,
                                &mut start_revision,
                                &mut backoff,
                                &mut on_event,
                            ) {
                                LineAction::Continue => {}
                                LineAction::BreakStream => break 'stream,
                                LineAction::Compacted(rev) => {
                                    compacted_rev = Some(rev);
                                    break 'stream;
                                }
                                LineAction::StopWatching => return,
                            }
                        }
                    }
                    Ok(None) => {
                        let trimmed = buf.trim_ascii();
                        if !trimmed.is_empty() {
                            match self.process_watch_line(
                                key_label,
                                trimmed,
                                notify_on_delete,
                                &mut start_revision,
                                &mut backoff,
                                &mut on_event,
                            ) {
                                LineAction::Continue | LineAction::BreakStream => {}
                                LineAction::Compacted(rev) => compacted_rev = Some(rev),
                                LineAction::StopWatching => return,
                            }
                        }
                        break;
                    }
                    Err(e) => {
                        error!(key = key_label, err = %e, "etcd watch stream error");
                        break;
                    }
                }
            }

            if let Some(c_rev) = compacted_rev {
                let range_req = RangeRequest {
                    key: b64_encode(req_key),
                    range_end: range_end.as_deref().map(b64_encode),
                    keys_only: false,
                };
                let range_res = tokio::select! {
                    _ = cancel.canceled() => return,
                    r = self.post_json::<_, RangeResponse>("/v3/kv/range", &range_req) => r,
                };
                match range_res {
                    Ok(range_resp) => {
                        for kv in range_resp.kvs {
                            let Ok(raw_key) = b64_decode(&kv.key) else {
                                continue;
                            };
                            let Ok(key_str) = String::from_utf8(raw_key) else {
                                continue;
                            };
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
                                    error!(key = %key_str, err = %e, "error decoding etcd compaction recovery value");
                                }
                            }
                        }
                        let hdr_rev = range_resp
                            .header
                            .as_ref()
                            .map(EtcdResponseHeader::revision_i64)
                            .unwrap_or(0);
                        start_revision = if hdr_rev > 0 { hdr_rev + 1 } else { c_rev };
                        backoff.reset();
                        continue;
                    }
                    Err(e) => {
                        error!(key = key_label, err = %e, "etcd compaction recovery GET failed");
                        start_revision = c_rev;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_range_empty_matches_clientv3() {
        assert_eq!(prefix_range(""), (vec![0], vec![0]));
        assert_eq!(prefix_range("abc"), (b"abc".to_vec(), b"abd".to_vec()));
    }
}
