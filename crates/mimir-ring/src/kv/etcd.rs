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

enum AuthError {
    /// Server responded at HTTP 4xx level (or returned empty/invalid token or non-transport decode error).
    Rejected(KvError),
    /// Server responded with HTTP 5xx (e.g. 500/502/503/504 during leader election):
    /// unary RPCs fail fast like regular HTTP 5xx, while watch_loop retries with backoff.
    Server(KvError),
    /// Transport / connect / timeout / mid-body failure while reaching auth endpoint (retryable under WaitForReady).
    Transport(KvError),
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
        let mut builder = reqwest::Client::builder()
            .use_preconfigured_tls(tls)
            .tcp_keepalive(Some(Duration::from_secs(10)))
            .tcp_keepalive_interval(Some(Duration::from_secs(5)))
            .tcp_keepalive_retries(Some(4));
        #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
        {
            builder = builder.tcp_user_timeout(Some(Duration::from_secs(30)));
        }
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

    fn unary_request_timeout(&self) -> Duration {
        let dial = self.cfg.dial_timeout.as_std().unwrap_or(Duration::ZERO);
        if !dial.is_zero() {
            dial.saturating_mul(2)
        } else {
            Duration::from_secs(30)
        }
    }

    async fn invalidate_auth(&self) {
        *self.auth_token.write().await = None;
    }

    async fn ensure_auth(&self) -> Result<Option<String>, AuthError> {
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
            #[serde(default)]
            token: String,
        }

        let mut last_err = None;
        let mut responded_server = false;
        let req_timeout = self.unary_request_timeout();
        for idx in 0..self.endpoints.len() {
            let url = format!("{}/v3/auth/authenticate", self.endpoint(idx));
            match self
                .http
                .post(&url)
                .timeout(req_timeout)
                .json(&AuthReq {
                    name: &self.cfg.username,
                    password: &self.cfg.password,
                })
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    let body_bytes = match resp.bytes().await {
                        Ok(b) => b,
                        Err(e) => {
                            // Mid-body read / timeout / connection reset is a transport failure.
                            let _ = e.is_timeout() || e.is_connect() || e.is_body() || e.is_request();
                            last_err = Some(KvError::Http(format_reqwest_error(&e)));
                            continue;
                        }
                    };
                    let parsed: AuthResp = serde_json::from_slice(&body_bytes).map_err(|e| {
                        AuthError::Rejected(KvError::Http(format!("decode etcd auth: {e}")))
                    })?;
                    if parsed.token.is_empty() {
                        return Err(AuthError::Rejected(KvError::Http(
                            "etcd auth failed: missing token in response".to_string(),
                        )));
                    }
                    *self.auth_token.write().await = Some(parsed.token.clone());
                    return Ok(Some(parsed.token));
                }
                Ok(resp) => {
                    let status = resp.status();
                    let err = KvError::Http(format!("etcd auth failed: {status}"));
                    if status.is_client_error() {
                        return Err(AuthError::Rejected(err));
                    }
                    if status.is_server_error() {
                        responded_server = true;
                        last_err = Some(err);
                    } else {
                        return Err(AuthError::Rejected(err));
                    }
                }
                Err(e) => {
                    last_err = Some(KvError::Http(format_reqwest_error(&e)));
                }
            }
        }
        let err = last_err.unwrap_or_else(|| KvError::Http("no etcd endpoints".to_string()));
        if responded_server {
            Err(AuthError::Server(err))
        } else {
            Err(AuthError::Transport(err))
        }
    }

    async fn auth_headers(&self) -> Result<HeaderMap, AuthError> {
        let mut headers = HeaderMap::new();
        if let Some(tok) = self.ensure_auth().await? {
            let val = HeaderValue::from_str(&tok).map_err(|e| {
                AuthError::Rejected(KvError::Http(format!("invalid etcd auth token header: {e}")))
            })?;
            headers.insert("Authorization", val);
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
                Err(AuthError::Rejected(e) | AuthError::Server(e)) => return Err(e),
                Err(AuthError::Transport(e)) => {
                    warn!(path, err = %e, "etcd auth failed while connecting, retrying");
                    reconnect.wait().await;
                    continue;
                }
            };
            let mut last_err = None;
            let mut responded = false;
            let mut need_auth_retry = false;
            let req_timeout = self.unary_request_timeout();
            for idx in 0..self.endpoints.len() {
                let url = format!("{}{path}", self.endpoint(idx));
                match self
                    .http
                    .post(&url)
                    .timeout(req_timeout)
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
            let max_retries = self.cfg.max_retries.max(0) as usize;
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
                    CasOutcome::Update(v) | CasOutcome::UpdateOnce(v) => v,
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
                false,
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
                true,
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
        ignore_delete: bool,
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

            if ignore_delete && is_delete {
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
                    error!(key = %key_str, err = %e, "error decoding key");
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
        ignore_delete: bool,
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
        let mut refreshed_auth = false;

        while backoff.ongoing() && !cancel.is_canceled() {
            let headers = match tokio::select! {
                _ = cancel.canceled() => return,
                r = self.auth_headers() => r,
            } {
                Ok(h) => h,
                Err(AuthError::Rejected(e)) => {
                    error!(key = key_label, err = %e, "etcd watch auth rejected");
                    return;
                }
                Err(AuthError::Server(e) | AuthError::Transport(e)) => {
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
                let watch_open_timeout = self.unary_request_timeout();
                let resp_res = tokio::select! {
                    _ = cancel.canceled() => return,
                    r = tokio::time::timeout(
                        watch_open_timeout,
                        self.http.post(&url).headers(headers.clone()).json(&req_body).send(),
                    ) => match r {
                        Ok(inner) => inner,
                        Err(_) => {
                            error!(key = key_label, endpoint = %self.endpoint(idx), "etcd watch open timed out");
                            continue;
                        }
                    },
                };
                match resp_res {
                    Ok(r) if r.status().is_success() => {
                        refreshed_auth = false;
                        ep_idx = (idx + 1) % self.endpoints.len();
                        connected_resp = Some(r);
                        break;
                    }
                    Ok(r)
                        if r.status() == StatusCode::UNAUTHORIZED
                            && !self.cfg.username.is_empty()
                            && !refreshed_auth =>
                    {
                        warn!(key = key_label, endpoint = %self.endpoint(idx), "etcd watch unauthorized, refreshing auth token");
                        refreshed_auth = true;
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
                refreshed_auth = false;
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
                                ignore_delete,
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
                                ignore_delete,
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
                        if hdr_rev > 0 {
                            start_revision = hdr_rev + 1;
                            backoff.reset();
                            continue;
                        }
                        start_revision = c_rev;
                    }
                    Err(e) => {
                        error!(key = key_label, err = %e, "etcd compaction recovery GET failed");
                        start_revision = c_rev;
                    }
                }
            }

            refreshed_auth = false;
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

    #[tokio::test]
    async fn w22_deep_07_watch_key_and_watch_prefix_delete_parity_with_dskit() {
        use crate::kv::{ProtoSnappyCodec, StringCodec};
        use mimir_proto::ringpb::Desc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        async fn run_mock_watch(lines: Vec<String>) -> String {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let body = lines.join("\n") + "\n";
                let hdr = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = sock.write_all(hdr.as_bytes()).await;
                let _ = sock.write_all(body.as_bytes()).await;
                tokio::time::sleep(Duration::from_millis(200)).await;
            });
            format!("http://{addr}")
        }

        let put_event = |k: &str, v: &[u8], mod_rev: i64| {
            format!(
                r#"{{"result":{{"header":{{"revision":"{mod_rev}"}},"events":[{{"kv":{{"key":"{}","value":"{}","version":"1","mod_revision":"{mod_rev}"}}}}]}}}}"#,
                b64_encode(k.as_bytes()),
                b64_encode(v)
            )
        };
        let delete_event = |k: &str, mod_rev: i64| {
            format!(
                r#"{{"result":{{"header":{{"revision":"{mod_rev}"}},"events":[{{"type":"DELETE","kv":{{"key":"{}","version":"0","mod_revision":"{mod_rev}"}}}}]}}}}"#,
                b64_encode(k.as_bytes())
            )
        };

        // 1. WatchKey with StringCodec: DELETE passes empty bytes through StringCodec -> "".
        let ep_str = run_mock_watch(vec![
            put_event("k1", b"hello", 1),
            delete_event("k1", 2),
        ])
        .await;
        let cfg_str = EtcdConfig {
            endpoints: vec![ep_str],
            ..Default::default()
        };
        let kv_str = EtcdKv::new(cfg_str, Arc::new(StringCodec)).unwrap();
        let ctx_str = ServiceContext::background();
        let got_str = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let got_str_c = Arc::clone(&got_str);
        let ctx_str_c = ctx_str.clone();
        kv_str
            .watch_key(
                "k1",
                ctx_str,
                Box::new(move |any| {
                    let s = *any.downcast::<String>().expect("StringCodec must yield String on DELETE");
                    let stop = s.is_empty();
                    got_str_c.lock().push(s);
                    if stop {
                        ctx_str_c.cancel();
                        false
                    } else {
                        true
                    }
                }),
            )
            .await;
        assert_eq!(*got_str.lock(), vec!["hello".to_string(), String::new()]);

        // 2. WatchKey with ProtoSnappyCodec: DELETE passes &[] through codec.decode(&[]) which errors
        // (logged and skipped) rather than emitting KeyDeleted, followed by next valid PUT.
        let codec_proto = Arc::new(ProtoSnappyCodec::<Desc>::new("ringDesc"));
        let encoded_desc = codec_proto.encode(&Desc::default()).unwrap();
        let ep_proto = run_mock_watch(vec![
            delete_event("ring", 1),
            put_event("ring", &encoded_desc, 2),
        ])
        .await;
        let cfg_proto = EtcdConfig {
            endpoints: vec![ep_proto],
            ..Default::default()
        };
        let kv_proto = EtcdKv::new(cfg_proto, codec_proto).unwrap();
        let ctx_proto = ServiceContext::background();
        let got_proto = Arc::new(parking_lot::Mutex::new(0usize));
        let got_proto_c = Arc::clone(&got_proto);
        let ctx_proto_c = ctx_proto.clone();
        kv_proto
            .watch_key(
                "ring",
                ctx_proto,
                Box::new(move |any| {
                    assert!(any.is::<Desc>(), "expected Desc, never KeyDeleted");
                    *got_proto_c.lock() += 1;
                    ctx_proto_c.cancel();
                    false
                }),
            )
            .await;
        assert_eq!(*got_proto.lock(), 1);

        // 3. WatchPrefix: DELETE is ignored (matching dskit WatchPrefix), PUT is delivered.
        let ep_pref = run_mock_watch(vec![
            delete_event("p/1", 1),
            put_event("p/2", b"val2", 2),
        ])
        .await;
        let cfg_pref = EtcdConfig {
            endpoints: vec![ep_pref],
            ..Default::default()
        };
        let kv_pref = EtcdKv::new(cfg_pref, Arc::new(StringCodec)).unwrap();
        let ctx_pref = ServiceContext::background();
        let got_pref = Arc::new(parking_lot::Mutex::new(Vec::<(String, String)>::new()));
        let got_pref_c = Arc::clone(&got_pref);
        let ctx_pref_c = ctx_pref.clone();
        kv_pref
            .watch_prefix(
                "p/",
                ctx_pref,
                Box::new(move |k, any| {
                    let s = *any.downcast::<String>().unwrap();
                    got_pref_c.lock().push((k.to_string(), s));
                    ctx_pref_c.cancel();
                    false
                }),
            )
            .await;
        assert_eq!(*got_pref.lock(), vec![("p/2".to_string(), "val2".to_string())]);
    }

    #[tokio::test]
    async fn w24_b03_auth_400_and_401_fail_fast_instead_of_looping() {
        use crate::kv::StringCodec;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        async fn spawn_auth_reject_server(status_line: &'static str, body: &'static str) -> String {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                while let Ok((mut sock, _)) = listener.accept().await {
                    tokio::spawn(async move {
                        let mut buf = [0u8; 4096];
                        let _ = sock.read(&mut buf).await;
                        let resp = format!(
                            "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                        let _ = sock.shutdown().await;
                    });
                }
            });
            format!("http://{addr}")
        }

        for (status_line, body, expected_status) in [
            (
                "400 Bad Request",
                r#"{"error":"etcdserver: authentication is not enabled","code":9}"#,
                "400",
            ),
            (
                "401 Unauthorized",
                r#"{"error":"etcdserver: authentication failed, invalid user ID or password","code":16}"#,
                "401",
            ),
        ] {
            let ep = spawn_auth_reject_server(status_line, body).await;
            let cfg = EtcdConfig {
                endpoints: vec![ep],
                username: "root".to_string(),
                password: "wrong".to_string(),
                max_retries: 3,
                ..Default::default()
            };
            let kv = EtcdKv::new(cfg, Arc::new(StringCodec)).unwrap();

            let err_get = tokio::time::timeout(Duration::from_secs(1), kv.get("k"))
                .await
                .expect("get must fail fast on auth rejection")
                .unwrap_err();
            assert!(
                err_get.to_string().contains(expected_status),
                "unexpected get error: {err_get}"
            );

            let err_list = tokio::time::timeout(Duration::from_secs(1), kv.list("p/"))
                .await
                .expect("list must fail fast on auth rejection")
                .unwrap_err();
            assert!(
                err_list.to_string().contains(expected_status),
                "unexpected list error: {err_list}"
            );

            let err_cas = tokio::time::timeout(
                Duration::from_secs(1),
                kv.cas("k", Box::new(|_| Ok(CasOutcome::Update(Box::new("v".to_string()))))),
            )
            .await
            .expect("cas must fail fast on auth rejection")
            .unwrap_err();
            assert!(
                err_cas.to_string().contains(expected_status),
                "unexpected cas error: {err_cas}"
            );

            let ctx = ServiceContext::background();
            tokio::time::timeout(
                Duration::from_secs(1),
                kv.watch_key("k", ctx, Box::new(|_| true)),
            )
            .await
            .expect("watch_key must return immediately on auth rejection");
        }
    }

    #[tokio::test]
    async fn w24_b03_expired_token_reauths_once_and_fails_if_reauth_rejected() {
        use crate::kv::StringCodec;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let auth_calls = Arc::new(AtomicUsize::new(0));
        let range_calls = Arc::new(AtomicUsize::new(0));
        let ac = Arc::clone(&auth_calls);
        let rc = Arc::clone(&range_calls);

        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let ac = Arc::clone(&ac);
                let rc = Arc::clone(&rc);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req_str = String::from_utf8_lossy(&buf[..n]);
                    let first_line = req_str.lines().next().unwrap_or("");
                    let (status_line, body) = if first_line.contains("/v3/auth/authenticate") {
                        let c = ac.fetch_add(1, Ordering::SeqCst);
                        if c == 0 {
                            ("200 OK", r#"{"token":"tok-1"}"#.to_string())
                        } else if c == 1 {
                            ("200 OK", r#"{"token":"tok-2"}"#.to_string())
                        } else {
                            ("401 Unauthorized", r#"{"error":"invalid credentials"}"#.to_string())
                        }
                    } else {
                        let c = rc.fetch_add(1, Ordering::SeqCst);
                        if c == 0 {
                            ("401 Unauthorized", r#"{"error":"invalid auth token"}"#.to_string())
                        } else if c == 1 {
                            ("200 OK", r#"{"kvs":[]}"#.to_string())
                        } else {
                            ("401 Unauthorized", r#"{"error":"invalid auth token"}"#.to_string())
                        }
                    };
                    let resp = format!(
                        "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });

        let cfg = EtcdConfig {
            endpoints: vec![format!("http://{addr}")],
            username: "root".to_string(),
            password: "secret".to_string(),
            ..Default::default()
        };
        let kv = EtcdKv::new(cfg, Arc::new(StringCodec)).unwrap();

        // First get: tok-1 gets 401 -> re-auths tok-2 -> succeeds.
        let res = tokio::time::timeout(Duration::from_secs(1), kv.get("k"))
            .await
            .expect("re-auth must succeed")
            .unwrap();
        assert!(res.is_none());
        assert_eq!(auth_calls.load(Ordering::SeqCst), 2);

        // Second get: tok-2 gets 401 -> re-auth gets 401 -> fails fast.
        let err = tokio::time::timeout(Duration::from_secs(1), kv.get("k"))
            .await
            .expect("rejected re-auth must fail fast")
            .unwrap_err();
        assert!(err.to_string().contains("401"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn w24_watch_key_survives_transient_auth_503_and_reconnects() {
        use crate::kv::StringCodec;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let auth_calls = Arc::new(AtomicUsize::new(0));
        let ac = Arc::clone(&auth_calls);

        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let ac = Arc::clone(&ac);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req_str = String::from_utf8_lossy(&buf[..n]);
                    let first_line = req_str.lines().next().unwrap_or("");
                    if first_line.contains("/v3/auth/authenticate") {
                        let c = ac.fetch_add(1, Ordering::SeqCst);
                        let (status_line, body) = if c == 0 {
                            (
                                "503 Service Unavailable",
                                r#"{"error":"etcdserver: no leader"}"#.to_string(),
                            )
                        } else {
                            ("200 OK", r#"{"token":"tok-recovered"}"#.to_string())
                        };
                        let resp = format!(
                            "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                        let _ = sock.shutdown().await;
                    } else if first_line.contains("/v3/watch") {
                        let val_b64 = b64_encode(b"recovered-val");
                        let key_b64 = b64_encode(b"k");
                        let event_line = format!(
                            r#"{{"result":{{"header":{{"revision":"10"}},"events":[{{"type":"PUT","kv":{{"key":"{key_b64}","value":"{val_b64}","mod_revision":"10"}}}}]}}}}"#
                        );
                        let chunk = format!("{:x}\r\n{event_line}\n\r\n", event_line.len() + 1);
                        let headers = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n";
                        let _ = sock.write_all(headers.as_bytes()).await;
                        let _ = sock.write_all(chunk.as_bytes()).await;
                        let _ = sock.write_all(b"0\r\n\r\n").await;
                        let _ = sock.shutdown().await;
                    }
                });
            }
        });

        let cfg = EtcdConfig {
            endpoints: vec![format!("http://{addr}")],
            username: "root".to_string(),
            password: "secret".to_string(),
            ..Default::default()
        };
        let kv = EtcdKv::new(cfg, Arc::new(StringCodec)).unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        let tx = Arc::new(std::sync::Mutex::new(Some(tx)));
        let ctx = ServiceContext::background();
        let cancel = ctx.clone();

        let handle = tokio::spawn(async move {
            kv.watch_key(
                "k",
                ctx,
                Box::new(move |val| {
                    if let Some(s) = val.downcast_ref::<String>() {
                        if let Some(sender) = tx.lock().unwrap().take() {
                            let _ = sender.send(s.clone());
                        }
                    }
                    false
                }),
            )
            .await;
        });

        let got = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("watch_key must survive transient 503 on /v3/auth/authenticate and deliver event")
            .expect("watch callback must send value");
        assert_eq!(got, "recovered-val");
        assert!(
            auth_calls.load(Ordering::SeqCst) >= 2,
            "expected at least 2 auth calls (initial 503 + retry 200)"
        );
        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    async fn w24_unary_rpc_fails_fast_on_auth_503_and_retries_mid_body_eof() {
        use crate::kv::StringCodec;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // 1. Unary get fails fast on 503 from /v3/auth/authenticate.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    let body = r#"{"error":"etcdserver: no leader"}"#;
                    let resp = format!(
                        "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        let kv = EtcdKv::new(
            EtcdConfig {
                endpoints: vec![format!("http://{addr}")],
                username: "root".to_string(),
                password: "secret".to_string(),
                ..Default::default()
            },
            Arc::new(StringCodec),
        )
        .unwrap();
        let err = tokio::time::timeout(Duration::from_secs(1), kv.get("k"))
            .await
            .expect("unary get must fail fast on auth 503")
            .unwrap_err();
        assert!(err.to_string().contains("503"), "unexpected error: {err}");

        // 2. Truncated 200 OK body on endpoint 0 fails over to endpoint 1 instead of rejecting.
        let l_bad = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_bad = l_bad.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = l_bad.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req_str = String::from_utf8_lossy(&buf[..n]);
                    let first_line = req_str.lines().next().unwrap_or("");
                    if first_line.contains("/v3/auth/authenticate") {
                        // Claim Content-Length 100 but close immediately after partial body.
                        let resp = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{\"tok";
                        let _ = sock.write_all(resp.as_bytes()).await;
                    } else {
                        let body = r#"{"kvs":[]}"#;
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                    }
                    let _ = sock.shutdown().await;
                });
            }
        });
        let l_good = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_good = l_good.local_addr().unwrap();
        let good_auth_calls = Arc::new(AtomicUsize::new(0));
        let gac = Arc::clone(&good_auth_calls);
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = l_good.accept().await {
                let gac = Arc::clone(&gac);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req_str = String::from_utf8_lossy(&buf[..n]);
                    let first_line = req_str.lines().next().unwrap_or("");
                    let body = if first_line.contains("/v3/auth/authenticate") {
                        gac.fetch_add(1, Ordering::SeqCst);
                        r#"{"token":"tok-good"}"#
                    } else {
                        r#"{"kvs":[]}"#
                    };
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        let kv2 = EtcdKv::new(
            EtcdConfig {
                endpoints: vec![format!("http://{addr_bad}"), format!("http://{addr_good}")],
                username: "root".to_string(),
                password: "secret".to_string(),
                ..Default::default()
            },
            Arc::new(StringCodec),
        )
        .unwrap();
        let got = tokio::time::timeout(Duration::from_secs(2), kv2.get("k"))
            .await
            .expect("mid-body auth transport error must fail over to healthy endpoint")
            .unwrap();
        assert!(got.is_none());
        assert_eq!(good_auth_calls.load(Ordering::SeqCst), 1);
    }
}