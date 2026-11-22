//! Ported from github.com/grafana/dskit/grpcclient/{grpcclient,ratelimit}.go.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

pub const DEFAULT_INITIAL_WINDOW_SIZE: u64 = 65535;
pub const DEFAULT_KEEPALIVE_TIME: Duration = Duration::from_secs(20);
pub const DEFAULT_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlagSpec {
    pub name: String,
    pub usage: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeepaliveClientParameters {
    pub time: Duration,
    pub timeout: Duration,
    pub permit_without_stream: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallOption {
    MaxCallRecvMsgSize(usize),
    MaxCallSendMsgSize(usize),
    UseCompressor(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterceptorKind {
    RateLimiter,
    RateLimitRetrier,
    Input(String),
    Middleware(String),
    ClusterValidation { label: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DialOption {
    TlsEnabled(bool),
    ConnectParams {
        base_delay: Duration,
        max_delay: Duration,
        min_connect_timeout: Duration,
    },
    InitialWindowSize(i32),
    InitialConnWindowSize(i32),
    DefaultCallOptions(Vec<CallOption>),
    ChainUnaryInterceptors(Vec<InterceptorKind>),
    ChainStreamInterceptors(Vec<String>),
    KeepaliveParams(KeepaliveClientParameters),
}

/// Configuration for a gRPC client (`dskit/grpcclient.Config`).
#[derive(Debug, Clone)]
pub struct Config {
    pub max_recv_msg_size: usize,
    pub max_send_msg_size: usize,
    pub grpc_compression: String,
    pub rate_limit: f64,
    pub rate_limit_burst: usize,
    pub backoff_on_ratelimits: bool,
    pub backoff_min_period: Duration,
    pub backoff_max_period: Duration,
    pub backoff_max_retries: usize,
    pub initial_stream_window_size: u64,
    pub initial_connection_window_size: u64,
    pub tls_enabled: bool,
    pub connect_timeout: Duration,
    pub connect_backoff_base_delay: Duration,
    pub connect_backoff_max_delay: Duration,
    pub keepalive_time: Duration,
    pub keepalive_timeout: Duration,
    pub middleware: Vec<String>,
    pub stream_middleware: Vec<String>,
    pub custom_compressors: Vec<String>,
    pub cluster_validation_label: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_recv_msg_size: 100 << 20,
            max_send_msg_size: 100 << 20,
            grpc_compression: String::new(),
            rate_limit: 0.0,
            rate_limit_burst: 0,
            backoff_on_ratelimits: false,
            backoff_min_period: Duration::from_millis(100),
            backoff_max_period: Duration::from_secs(10),
            backoff_max_retries: 10,
            initial_stream_window_size: DEFAULT_INITIAL_WINDOW_SIZE,
            initial_connection_window_size: DEFAULT_INITIAL_WINDOW_SIZE,
            tls_enabled: false,
            connect_timeout: Duration::from_secs(5),
            connect_backoff_base_delay: Duration::from_secs(1),
            connect_backoff_max_delay: Duration::from_secs(5),
            keepalive_time: DEFAULT_KEEPALIVE_TIME,
            keepalive_timeout: DEFAULT_KEEPALIVE_TIMEOUT,
            middleware: Vec::new(),
            stream_middleware: Vec::new(),
            custom_compressors: Vec::new(),
            cluster_validation_label: String::new(),
        }
    }
}

impl Config {
    /// Applies defaults and returns flag descriptions matching `RegisterFlagsWithPrefix`.
    pub fn register_flags_with_prefix(&mut self, prefix: &str) -> Vec<FlagSpec> {
        self.initial_stream_window_size = DEFAULT_INITIAL_WINDOW_SIZE;
        self.initial_connection_window_size = DEFAULT_INITIAL_WINDOW_SIZE;
        self.max_recv_msg_size = 100 << 20;
        self.max_send_msg_size = 100 << 20;
        self.connect_timeout = Duration::from_secs(5);
        self.connect_backoff_base_delay = Duration::from_secs(1);
        self.connect_backoff_max_delay = Duration::from_secs(5);
        self.keepalive_time = DEFAULT_KEEPALIVE_TIME;
        self.keepalive_timeout = DEFAULT_KEEPALIVE_TIMEOUT;

        let mut supported = String::from(
            "Use compression when sending messages. Supported values are: 'gzip', 'snappy'",
        );
        for cmp in &self.custom_compressors {
            supported.push_str(", '");
            supported.push_str(cmp);
            supported.push('\'');
        }
        supported.push_str(" and '' (disable compression)");

        vec![
            FlagSpec {
                name: format!("{prefix}.grpc-max-recv-msg-size"),
                usage: "gRPC client max receive message size (bytes).".to_string(),
            },
            FlagSpec {
                name: format!("{prefix}.grpc-max-send-msg-size"),
                usage: "gRPC client max send message size (bytes).".to_string(),
            },
            FlagSpec {
                name: format!("{prefix}.grpc-compression"),
                usage: supported,
            },
        ]
    }

    /// Validates the compression configuration.
    pub fn validate(&self) -> Result<(), String> {
        let mut supported = vec!["gzip", "snappy", ""];
        for c in &self.custom_compressors {
            supported.push(c.as_str());
        }
        if !supported.contains(&self.grpc_compression.as_str()) {
            return Err(format!(
                "unsupported compression type: {:?}",
                self.grpc_compression
            ));
        }
        Ok(())
    }

    /// Returns call options implied by the config.
    pub fn call_options(&self) -> Vec<CallOption> {
        let mut opts = vec![
            CallOption::MaxCallRecvMsgSize(self.max_recv_msg_size),
            CallOption::MaxCallSendMsgSize(self.max_send_msg_size),
        ];
        if !self.grpc_compression.is_empty() {
            opts.push(CallOption::UseCompressor(self.grpc_compression.clone()));
        }
        opts
    }

    /// Returns keepalive parameters, falling back to defaults if zero.
    pub fn keepalive_params(&self) -> KeepaliveClientParameters {
        let time = if self.keepalive_time.is_zero() {
            DEFAULT_KEEPALIVE_TIME
        } else {
            self.keepalive_time
        };
        let timeout = if self.keepalive_timeout.is_zero() {
            DEFAULT_KEEPALIVE_TIMEOUT
        } else {
            self.keepalive_timeout
        };
        KeepaliveClientParameters {
            time,
            timeout,
            permit_without_stream: true,
        }
    }

    /// Builds dial options including unary/stream interceptor ordering.
    pub fn dial_option(
        &self,
        input_unary: Vec<String>,
        input_stream: Vec<String>,
    ) -> Result<Vec<DialOption>, String> {
        let mut opts = vec![DialOption::TlsEnabled(self.tls_enabled)];

        let mut unary: Vec<InterceptorKind> =
            input_unary.into_iter().map(InterceptorKind::Input).collect();
        for m in &self.middleware {
            unary.push(InterceptorKind::Middleware(m.clone()));
        }
        let mut stream = input_stream;
        stream.extend(self.stream_middleware.iter().cloned());

        if self.backoff_on_ratelimits {
            unary.insert(0, InterceptorKind::RateLimitRetrier);
        }
        if self.rate_limit > 0.0 {
            unary.insert(0, InterceptorKind::RateLimiter);
        }
        if !self.cluster_validation_label.is_empty() {
            unary.push(InterceptorKind::ClusterValidation {
                label: self.cluster_validation_label.clone(),
            });
        }

        if !self.connect_timeout.is_zero() {
            let base_delay = if self.connect_backoff_base_delay.is_zero() {
                Duration::from_secs(1)
            } else {
                self.connect_backoff_base_delay
            };
            let max_delay = if self.connect_backoff_max_delay.is_zero() {
                Duration::from_secs(120)
            } else {
                self.connect_backoff_max_delay
            };
            opts.push(DialOption::ConnectParams {
                base_delay,
                max_delay,
                min_connect_timeout: self.connect_timeout,
            });
        }

        if self.initial_stream_window_size > DEFAULT_INITIAL_WINDOW_SIZE {
            opts.push(DialOption::InitialWindowSize(
                self.initial_stream_window_size as i32,
            ));
        }
        if self.initial_connection_window_size > DEFAULT_INITIAL_WINDOW_SIZE {
            opts.push(DialOption::InitialConnWindowSize(
                self.initial_connection_window_size as i32,
            ));
        }

        opts.push(DialOption::DefaultCallOptions(self.call_options()));
        opts.push(DialOption::ChainUnaryInterceptors(unary));
        opts.push(DialOption::ChainStreamInterceptors(stream));
        opts.push(DialOption::KeepaliveParams(self.keepalive_params()));

        Ok(opts)
    }
}

/// Client-side rate limiter matching `dskit/grpcclient.NewRateLimiter`.
#[derive(Debug, Clone)]
pub struct GrpcRateLimiter {
    inner: Arc<Mutex<TokenBucketState>>,
}

#[derive(Debug)]
struct TokenBucketState {
    rate: f64,
    burst: usize,
    tokens: f64,
    last: Instant,
}

impl GrpcRateLimiter {
    pub fn new(cfg: &Config) -> Self {
        let burst = if cfg.rate_limit_burst == 0 {
            cfg.rate_limit.max(0.0) as usize
        } else {
            cfg.rate_limit_burst
        };
        Self {
            inner: Arc::new(Mutex::new(TokenBucketState {
                rate: cfg.rate_limit,
                burst,
                tokens: burst as f64,
                last: Instant::now(),
            })),
        }
    }

    /// Acquires 1 token or returns `tonic::Status::resource_exhausted` matching Go's `x/time/rate`.
    #[allow(clippy::result_large_err)]
    pub async fn wait(&self) -> Result<(), tonic::Status> {
        let wait_dur = {
            let mut st = self.inner.lock();
            if st.burst == 0 {
                return Err(tonic::Status::resource_exhausted(
                    "rate: Wait(n=1) exceeds limiter's burst 0",
                ));
            }
            let now = Instant::now();
            let elapsed = now.duration_since(st.last).as_secs_f64();
            st.last = now;
            st.tokens = (st.tokens + elapsed * st.rate).min(st.burst as f64);
            if st.tokens >= 1.0 {
                st.tokens -= 1.0;
                Duration::ZERO
            } else if st.rate <= 0.0 {
                return Err(tonic::Status::resource_exhausted(
                    "rate: Wait(n=1) would exceed context deadline",
                ));
            } else {
                let needed = 1.0 - st.tokens;
                st.tokens -= 1.0;
                Duration::from_secs_f64(needed / st.rate)
            }
        };
        if !wait_dur.is_zero() {
            tokio::time::sleep(wait_dur).await;
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub async fn invoke<F, R>(&self, invoker: F) -> Result<R, tonic::Status>
    where
        F: FnOnce() -> Result<R, tonic::Status>,
    {
        self.wait().await?;
        invoker()
    }
}
