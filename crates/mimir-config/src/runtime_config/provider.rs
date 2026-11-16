//! Runtime config source providers (local file and HTTP/HTTPS URL).
//!
//! Ported from:
//! - github.com/grafana/dskit/runtimeconfig/provider.go
//! - github.com/grafana/dskit/runtimeconfig/manager.go (`httpTransport`, HTTP client construction)
//! - github.com/grafana/dskit/middleware/http_cluster.go (`ClusterValidationRoundTripper`)
//! - github.com/grafana/dskit/clusterutil/clusterutil.go (`ClusterValidationLabelHeader`)

use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::error::ConfigError;
use crate::server::RuntimeConfigConfig;

/// `clusterutil.ClusterValidationLabelHeader`.
pub const CLUSTER_VALIDATION_LABEL_HEADER: &str = "X-Cluster";

/// `User-Agent` sent by dskit's `httpProvider.Read`.
pub const RUNTIME_CONFIG_USER_AGENT: &str = "dskit-runtimeconfig";

/// Observer of runtime-config HTTP fetches, so the caller can export dskit's
/// `runtime_config_http_request_duration_seconds{url,status_code}` and
/// `client_invalid_cluster_validation_label_requests_total{client,protocol,method}` series and
/// log rejected cluster-validation requests. The manager itself registers no metrics.
pub trait RuntimeConfigHttpObserver: Send + Sync {
    /// Called once per request. `status_code` is the decimal HTTP status, or `"error"` when the
    /// request or body read failed (`provider.go:httpProvider.Read`).
    fn observe_request(&self, url_for_metrics: &str, status_code: &str, elapsed: Duration);

    /// Called when the server rejected the request because of the cluster validation label
    /// (`middleware.ClusterValidationRoundTripper` reporter). `msg` is the full error message and
    /// `method` the route reported by the server.
    fn invalid_cluster_validation(&self, msg: &str, method: &str);
}

/// `isURL` from `dskit/runtimeconfig/provider.go`.
pub fn is_url(path: &str) -> bool {
    path.starts_with("http://") || path.starts_with("https://")
}

/// `sanitizeURLForMetrics`: drops userinfo, query and fragment.
pub fn sanitize_url_for_metrics(raw: &str) -> String {
    match reqwest::Url::parse(raw) {
        Ok(mut u) => {
            let _ = u.set_username("");
            let _ = u.set_password(None);
            u.set_query(None);
            u.set_fragment(None);
            u.to_string()
        }
        // Go's url.Parse accepts far more than the WHATWG parser; the request itself reports
        // the problem, so keep the raw path as identifier.
        Err(_) => raw.to_string(),
    }
}

/// Formats an `io::Error` the way Go's `os.ReadFile` reports it: `open <path>: <errno text>`.
pub(crate) fn go_file_error(path: &str, err: &std::io::Error) -> String {
    let mut msg = err.to_string();
    if let Some(idx) = msg.find(" (os error ") {
        msg.truncate(idx);
    }
    let mut chars = msg.chars();
    let msg = match chars.next() {
        Some(c) => c.to_lowercase().collect::<String>() + chars.as_str(),
        None => msg,
    };
    format!("open {path}: {msg}")
}

/// Where a config source is read from (`provider` interface).
#[derive(Clone)]
pub(crate) enum Provider {
    File,
    Http(Arc<HttpFetcher>),
}

impl Provider {
    pub(crate) fn read(&self, path: &str, url_for_metrics: &str) -> Result<Vec<u8>, String> {
        match self {
            Provider::File => std::fs::read(path).map_err(|e| go_file_error(path, &e)),
            Provider::Http(f) => f.get(path, url_for_metrics),
        }
    }
}

struct FetchRequest {
    url: String,
    url_for_metrics: String,
    reply: std_mpsc::Sender<Result<Vec<u8>, String>>,
}

/// Shared HTTP client for all URL sources of one manager (dskit builds a single `http.Client`).
///
/// The client runs on a dedicated thread with its own single-threaded Tokio runtime, so reads
/// can be issued from the synchronous reloader thread and from inside an async context (the
/// initial load in `RuntimeConfigManager::new`) alike, and connections can be kept alive across
/// reloads when `http_client_disable_keep_alives` is false.
pub(crate) struct HttpFetcher {
    tx: parking_lot::Mutex<Option<std_mpsc::Sender<FetchRequest>>>,
}

impl HttpFetcher {
    pub(crate) fn new(
        cfg: &RuntimeConfigConfig,
        observer: Arc<parking_lot::RwLock<Option<Arc<dyn RuntimeConfigHttpObserver>>>>,
    ) -> Result<Arc<Self>, ConfigError> {
        // dskit: a zero timeout means 30s.
        let timeout = cfg
            .http_client_timeout
            .as_std()
            .filter(|d| !d.is_zero())
            .unwrap_or(Duration::from_secs(30));
        let disable_keep_alives = cfg.http_client_disable_keep_alives;
        let cluster_label = cfg.http_client_cluster_validation.label.clone();

        let mut builder = reqwest::Client::builder()
            .timeout(timeout)
            .user_agent(RUNTIME_CONFIG_USER_AGENT);
        if disable_keep_alives {
            builder = builder.pool_max_idle_per_host(0);
        }
        let client = builder.build().map_err(|e| {
            ConfigError::RuntimeConfig(format!("creating runtime config HTTP client: {e}"))
        })?;

        let (tx, rx) = std_mpsc::channel::<FetchRequest>();
        let obs = observer;
        thread::Builder::new()
            .name("runtime-config-http".to_string())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        for req in rx {
                            let _ = req
                                .reply
                                .send(Err(format!("runtime config HTTP runtime: {e}")));
                        }
                        return;
                    }
                };
                for req in rx {
                    let res = rt.block_on(fetch(
                        &client,
                        &req.url,
                        &req.url_for_metrics,
                        disable_keep_alives,
                        &cluster_label,
                        &obs,
                    ));
                    let _ = req.reply.send(res);
                }
            })
            .map_err(|e| {
                ConfigError::RuntimeConfig(format!("spawning runtime config HTTP thread: {e}"))
            })?;

        Ok(Arc::new(Self {
            tx: parking_lot::Mutex::new(Some(tx)),
        }))
    }

    fn get(&self, url: &str, url_for_metrics: &str) -> Result<Vec<u8>, String> {
        let (reply_tx, reply_rx) = std_mpsc::channel();
        {
            let guard = self.tx.lock();
            let Some(tx) = guard.as_ref() else {
                return Err(format!("Get {url:?}: runtime config HTTP client stopped"));
            };
            tx.send(FetchRequest {
                url: url.to_string(),
                url_for_metrics: url_for_metrics.to_string(),
                reply: reply_tx,
            })
            .map_err(|_| format!("Get {url:?}: runtime config HTTP client stopped"))?;
        }
        reply_rx
            .recv()
            .unwrap_or_else(|_| Err(format!("Get {url:?}: runtime config HTTP client stopped")))
    }
}

impl Drop for HttpFetcher {
    fn drop(&mut self) {
        // Closing the channel ends the fetch thread.
        self.tx.lock().take();
    }
}

/// Innermost error in a `source()` chain, which carries the useful text
/// (e.g. "Connection refused (os error 111)").
fn innermost(err: &(dyn std::error::Error + 'static)) -> String {
    let mut cur = err;
    while let Some(next) = cur.source() {
        cur = next;
    }
    cur.to_string()
}

fn reqwest_error_text(err: &reqwest::Error) -> String {
    if err.is_timeout() {
        "context deadline exceeded (Client.Timeout exceeded while awaiting headers)".to_string()
    } else {
        innermost(err)
    }
}

#[derive(serde::Deserialize)]
struct ClusterValidationError {
    #[serde(default)]
    cluster_validation_error_message: String,
    #[serde(default)]
    route: String,
}

async fn fetch(
    client: &reqwest::Client,
    url: &str,
    url_for_metrics: &str,
    disable_keep_alives: bool,
    cluster_label: &str,
    observer: &parking_lot::RwLock<Option<Arc<dyn RuntimeConfigHttpObserver>>>,
) -> Result<Vec<u8>, String> {
    let start = Instant::now();
    let observe = |status: &str| {
        if let Some(o) = observer.read().as_ref() {
            o.observe_request(url_for_metrics, status, start.elapsed());
        }
    };

    let mut req = client.get(url);
    if disable_keep_alives {
        // Go's Transport.DisableKeepAlives sends "Connection: close".
        req = req.header(reqwest::header::CONNECTION, "close");
    }
    if !cluster_label.is_empty() {
        req = req.header(CLUSTER_VALIDATION_LABEL_HEADER, cluster_label);
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            observe("error");
            return Err(format!("Get {url:?}: {}", reqwest_error_text(&e)));
        }
    };

    let status = resp.status().as_u16();

    // middleware.ClusterValidationRoundTripper: a 511 with an "application/json" body describing
    // a cluster validation failure turns into a transport error.
    if !cluster_label.is_empty()
        && status == 511
        && resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            == Some("application/json")
    {
        let body = match resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                observe("error");
                return Err(format!("Get {url:?}: {}", reqwest_error_text(&e)));
            }
        };
        if let Ok(cv) = serde_json::from_slice::<ClusterValidationError>(&body) {
            let msg = format!(
                "request rejected by the server: {}",
                cv.cluster_validation_error_message
            );
            if let Some(o) = observer.read().as_ref() {
                o.invalid_cluster_validation(&msg, &cv.route);
            }
            observe("error");
            return Err(format!("Get {url:?}: {msg}"));
        }
        observe(&status.to_string());
        return Err(format!("HTTP {status} from {url}"));
    }

    if status / 100 != 2 {
        let _ = resp.bytes().await;
        observe(&status.to_string());
        return Err(format!("HTTP {status} from {url}"));
    }

    match resp.bytes().await {
        Ok(b) => {
            observe(&status.to_string());
            Ok(b.to_vec())
        }
        Err(e) => {
            observe("error");
            Err(format!("read response body: {}", reqwest_error_text(&e)))
        }
    }
}
