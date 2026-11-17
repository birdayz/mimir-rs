//! Consul HTTP KV client.
//!
//! Ported from github.com/grafana/dskit/kv/consul/client.go

use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use mimir_common::backoff::{Backoff, BackoffConfig};
use mimir_common::services::ServiceContext;
use mimir_config::ingester::ConsulConfig;
use prometheus::{HistogramOpts, HistogramVec, Registry};
use rand::Rng;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, error};

use super::{BoxFuture, CasFn, CasOutcome, Codec, KvClient, KvError, Registerer};

const LONG_POLL_DURATION_SECS: u64 = 10;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ConsulKvPair {
    key: String,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    create_index: u64,
    #[serde(default)]
    modify_index: u64,
}

struct SimpleRateLimiter {
    rate_per_sec: f64,
    burst: f64,
    state: Mutex<(f64, Instant)>,
}

impl SimpleRateLimiter {
    fn new(rate_per_sec: f64, burst: i64) -> Option<Self> {
        if rate_per_sec <= 0.0 {
            return None;
        }
        let b = (burst.max(1)) as f64;
        Some(Self {
            rate_per_sec,
            burst: b,
            state: Mutex::new((b, Instant::now())),
        })
    }

    async fn wait(&self, cancel: &ServiceContext) -> bool {
        loop {
            if cancel.is_canceled() {
                return false;
            }
            let sleep_dur = {
                let mut st = self.state.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(st.1).as_secs_f64();
                st.0 = (st.0 + elapsed * self.rate_per_sec).min(self.burst);
                st.1 = now;
                if st.0 >= 1.0 {
                    st.0 -= 1.0;
                    None
                } else {
                    let needed = (1.0 - st.0) / self.rate_per_sec;
                    Some(Duration::from_secs_f64(needed))
                }
            };
            match sleep_dur {
                None => return true,
                Some(d) => {
                    tokio::select! {
                        _ = cancel.canceled() => return false,
                        _ = tokio::time::sleep(d) => {}
                    }
                }
            }
        }
    }
}

pub struct ConsulKv {
    http: reqwest::Client,
    base_url: String,
    cfg: ConsulConfig,
    codec: Arc<dyn Codec>,
    limiter: Option<SimpleRateLimiter>,
    request_duration: HistogramVec,
}

impl ConsulKv {
    pub fn new(
        cfg: ConsulConfig,
        codec: Arc<dyn Codec>,
        reg: Option<&Registry>,
    ) -> Result<Self, KvError> {
        Self::with_registerer(cfg, codec, reg.map(Registerer::new).as_ref())
    }

    /// Go `consul.NewClient(cfg, codec, logger, registerer)` with a (possibly
    /// wrapped) Go-style registerer.
    pub fn with_registerer(
        cfg: ConsulConfig,
        codec: Arc<dyn Codec>,
        reg: Option<&Registerer>,
    ) -> Result<Self, KvError> {
        let mut headers = HeaderMap::new();
        if !cfg.acl_token.is_empty() {
            let val = HeaderValue::from_str(&cfg.acl_token)
                .map_err(|e| KvError::InvalidConfig(format!("invalid consul acl token: {e}")))?;
            headers.insert("X-Consul-Token", val);
        }

        let timeout: Duration = cfg.http_client_timeout.as_std().unwrap_or(Duration::ZERO);
        let mut builder = reqwest::Client::builder().default_headers(headers);
        if !timeout.is_zero() {
            builder = builder.timeout(timeout);
        }
        let http = builder
            .build()
            .map_err(|e| KvError::Http(format!("build consul http client: {e}")))?;

        let host = cfg.host.trim_end_matches('/');
        let base_url = if host.starts_with("http://") || host.starts_with("https://") {
            host.to_string()
        } else {
            format!("http://{host}")
        };

        let request_duration = HistogramVec::new(
            HistogramOpts::new(
                "consul_request_duration_seconds",
                "Time spent on consul requests.",
            ),
            &["operation", "status_code"],
        )
        .expect("valid metric");
        if let Some(r) = reg {
            let _ = r.register(Box::new(request_duration.clone()));
        }

        let limiter = SimpleRateLimiter::new(cfg.watch_rate_limit, cfg.watch_burst_size);
        Ok(Self {
            http,
            base_url,
            cfg,
            codec,
            limiter,
            request_duration,
        })
    }

    fn kv_url(&self, key: &str) -> String {
        let trimmed = key.trim_start_matches('/');
        format!("{}/v1/kv/{trimmed}", self.base_url)
    }

    fn append_read_consistency(&self, url: &mut String, has_query: bool) {
        let sep = if has_query { '&' } else { '?' };
        if self.cfg.consistent_reads {
            url.push(sep);
            url.push_str("consistent");
        } else {
            url.push(sep);
            url.push_str("stale");
        }
    }

    async fn get_raw(
        &self,
        key: &str,
        wait_index: Option<u64>,
        allow_consistency: bool,
    ) -> Result<(Option<ConsulKvPair>, u64), KvError> {
        let mut url = self.kv_url(key);
        let mut has_q = false;
        if let Some(idx) = wait_index {
            url.push_str(&format!("?index={idx}&wait={LONG_POLL_DURATION_SECS}s"));
            has_q = true;
        }
        if allow_consistency {
            self.append_read_consistency(&mut url, has_q);
        }

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| KvError::Http(e.to_string()))?;

        let last_index = resp
            .headers()
            .get("X-Consul-Index")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        if resp.status().as_u16() == 404 {
            return Ok((None, last_index));
        }
        if !resp.status().is_success() {
            return Err(KvError::Http(format!("consul get status {}", resp.status())));
        }

        let mut pairs: Vec<ConsulKvPair> = resp
            .json()
            .await
            .map_err(|e| KvError::Http(format!("decode consul get response: {e}")))?;
        Ok((pairs.pop(), last_index))
    }

    async fn list_raw(
        &self,
        prefix: &str,
        wait_index: Option<u64>,
    ) -> Result<(Vec<ConsulKvPair>, u64), KvError> {
        let mut url = format!("{}?recurse=true", self.kv_url(prefix));
        if let Some(idx) = wait_index {
            url.push_str(&format!("&index={idx}&wait={LONG_POLL_DURATION_SECS}s"));
        }
        self.append_read_consistency(&mut url, true);

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| KvError::Http(e.to_string()))?;

        let last_index = resp
            .headers()
            .get("X-Consul-Index")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        if resp.status().as_u16() == 404 {
            return Ok((Vec::new(), last_index));
        }
        if !resp.status().is_success() {
            return Err(KvError::Http(format!("consul list status {}", resp.status())));
        }

        let pairs: Vec<ConsulKvPair> = resp
            .json()
            .await
            .map_err(|e| KvError::Http(format!("decode consul list response: {e}")))?;
        Ok((pairs, last_index))
    }
}

fn check_last_index(index: u64, meta_last_index: u64) -> (u64, bool) {
    if meta_last_index == 0 {
        return (1, false);
    }
    if meta_last_index < index {
        return (0, false);
    }
    if index == meta_last_index {
        return (meta_last_index, true);
    }
    (meta_last_index, false)
}

fn decode_consul_value(
    codec: &dyn Codec,
    val_b64: Option<&str>,
) -> Result<Box<dyn Any + Send + Sync>, KvError> {
    let raw = match val_b64 {
        Some(s) if !s.is_empty() => base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|e| KvError::Codec(format!("base64 decode: {e}")))?,
        _ => Vec::new(),
    };
    codec.decode(&raw)
}

impl KvClient for ConsulKv {
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
        Box::pin(async move {
            let mut url = format!("{}?keys=true", self.kv_url(prefix));
            self.append_read_consistency(&mut url, true);
            let resp = self
                .http
                .get(&url)
                .send()
                .await
                .map_err(|e| KvError::Http(e.to_string()))?;
            if resp.status().as_u16() == 404 {
                return Ok(Vec::new());
            }
            if !resp.status().is_success() {
                return Err(KvError::Http(format!("consul list status {}", resp.status())));
            }
            let keys: Vec<String> = resp
                .json()
                .await
                .map_err(|e| KvError::Http(format!("decode consul keys: {e}")))?;
            Ok(keys)
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        Box::pin(async move {
            let start = Instant::now();
            let res = self.get_raw(key, None, true).await;
            let status = if res.is_ok() { "200" } else { "500" };
            self.request_duration
                .with_label_values(&["Get", status])
                .observe(start.elapsed().as_secs_f64());

            let (pair, _) = res?;
            match pair {
                None => Ok(None),
                Some(p) => Ok(Some(decode_consul_value(
                    self.codec.as_ref(),
                    p.value.as_deref(),
                )?)),
            }
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let resp = self
                .http
                .delete(self.kv_url(key))
                .send()
                .await
                .map_err(|e| KvError::Http(e.to_string()))?;
            if !resp.status().is_success() && resp.status().as_u16() != 404 {
                return Err(KvError::Http(format!("consul delete status {}", resp.status())));
            }
            Ok(())
        })
    }

    fn cas<'a>(&'a self, key: &'a str, mut f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let retries = 10;
            let cas_delay: Duration = self.cfg.cas_retry_delay.as_std().unwrap_or(Duration::ZERO);
            let sleep_before_retry = if !cas_delay.is_zero() {
                let max_ns = cas_delay.as_nanos().min(u64::MAX as u128) as u64;
                Duration::from_nanos(rand::thread_rng().gen_range(0..max_ns.max(1)))
            } else {
                Duration::ZERO
            };

            let mut index;
            for i in 0..retries {
                if i > 0 && !sleep_before_retry.is_zero() {
                    tokio::time::sleep(sleep_before_retry).await;
                }

                let (pair, _) = match self.get_raw(key, None, false).await {
                    Ok(v) => v,
                    Err(err) => {
                        error!(key, %err, "error getting key from consul");
                        continue;
                    }
                };

                let decoded = match pair {
                    Some(p) => {
                        index = p.modify_index;
                        match decode_consul_value(self.codec.as_ref(), p.value.as_deref()) {
                            Ok(v) => Some(v),
                            Err(err) => {
                                error!(key, %err, "error decoding consul key");
                                continue;
                            }
                        }
                    }
                    None => {
                        index = 0;
                        None
                    }
                };

                let outcome = match f(decoded.as_deref()) {
                    Ok(o) => o,
                    Err(cb_err) => {
                        if !cb_err.retry {
                            return Err(cb_err.err);
                        }
                        continue;
                    }
                };

                let new_val = match outcome {
                    CasOutcome::NoChange => return Ok(()),
                    CasOutcome::Update(v) => v,
                };

                let bytes = match self.codec.encode(new_val.as_ref()) {
                    Ok(b) => b,
                    Err(err) => {
                        error!(key, %err, "error serializing value");
                        continue;
                    }
                };

                let url = format!("{}?cas={index}", self.kv_url(key));
                let resp = match self.http.put(&url).body(bytes).send().await {
                    Ok(r) => r,
                    Err(err) => {
                        error!(key, %err, "error CASing consul key");
                        continue;
                    }
                };
                if !resp.status().is_success() {
                    error!(key, status = %resp.status(), "consul CAS failed status");
                    continue;
                }
                let ok: bool = resp.json().await.unwrap_or(false);
                if !ok {
                    debug!(key, index, "error CASing, trying again");
                    continue;
                }
                return Ok(());
            }

            Err(KvError::CasFailed {
                key: key.to_string(),
                reason: "failed to CAS".to_string(),
            })
        })
    }

    fn watch_key<'a>(
        &'a self,
        key: &'a str,
        cancel: ServiceContext,
        mut f: Box<dyn FnMut(Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut backoff = Backoff::new(BackoffConfig {
                min_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(60),
                max_retries: 0,
            });
            let mut index = 0u64;

            while backoff.ongoing() && !cancel.is_canceled() {
                if let Some(lim) = &self.limiter {
                    if !lim.wait(&cancel).await {
                        return;
                    }
                }

                let res = tokio::select! {
                    _ = cancel.canceled() => return,
                    r = self.get_raw(key, Some(index), true) => r,
                };

                let (pair, meta_last_index) = match res {
                    Ok(v) => {
                        backoff.reset();
                        v
                    }
                    Err(err) => {
                        error!(key, %err, "error getting path from consul");
                        tokio::select! {
                            _ = cancel.canceled() => return,
                            _ = backoff.wait() => continue,
                        }
                    }
                };

                let (new_index, skip) = check_last_index(index, meta_last_index);
                index = new_index;
                if skip {
                    continue;
                }

                let Some(p) = pair else {
                    continue;
                };

                match decode_consul_value(self.codec.as_ref(), p.value.as_deref()) {
                    Ok(val) => {
                        if !f(val) {
                            return;
                        }
                    }
                    Err(err) => {
                        error!(key, %err, "error decoding consul key");
                    }
                }
            }
        })
    }

    fn watch_prefix<'a>(
        &'a self,
        prefix: &'a str,
        cancel: ServiceContext,
        mut f: Box<dyn FnMut(&str, Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut backoff = Backoff::new(BackoffConfig {
                min_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(60),
                max_retries: 0,
            });
            let mut index = 0u64;

            while backoff.ongoing() && !cancel.is_canceled() {
                if let Some(lim) = &self.limiter {
                    if !lim.wait(&cancel).await {
                        return;
                    }
                }

                let res = tokio::select! {
                    _ = cancel.canceled() => return,
                    r = self.list_raw(prefix, Some(index)) => r,
                };

                let (pairs, meta_last_index) = match res {
                    Ok(v) => {
                        backoff.reset();
                        v
                    }
                    Err(err) => {
                        error!(prefix, %err, "error listing prefix from consul");
                        tokio::select! {
                            _ = cancel.canceled() => return,
                            _ = backoff.wait() => continue,
                        }
                    }
                };

                let (new_index, skip) = check_last_index(index, meta_last_index);
                if skip {
                    continue;
                }

                for kvp in pairs {
                    if index > 0 && kvp.modify_index <= index && kvp.create_index <= index {
                        continue;
                    }
                    match decode_consul_value(self.codec.as_ref(), kvp.value.as_deref()) {
                        Ok(val) => {
                            if !f(&kvp.key, val) {
                                return;
                            }
                        }
                        Err(err) => {
                            error!(prefix, key = %kvp.key, %err, "error decoding value for prefix");
                        }
                    }
                }

                index = new_index;
            }
        })
    }
}
