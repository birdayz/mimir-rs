//! Kafka client request retry policy (the franz-go `kgo` semantics Mimir's ingest storage relies on).
//!
//! Mimir talks to Kafka through franz-go's `kgo.Client`, whose retry behaviour is externally
//! observable: it decides which Kafka outages make an ingester fail its startup immediately (a
//! single bad seed broker: connection refused, DNS NXDOMAIN) and which keep it alive, retrying,
//! with `/ready` reporting 503 until the broker comes back (dial timeouts, hung connections,
//! paused brokers). This module ports that policy so every Rust Kafka request path makes the same
//! decisions.
//!
//! Ported from:
//! - github.com/twmb/franz-go/pkg/kgo/errors.go (`isRetryableBrokerErr`, `isDialNonTimeoutErr`,
//!   `isAnyDialErr`, `isPermanentDialErr`, `isSkippableBrokerErr`, `ErrFirstReadEOF`,
//!   `errChosenBrokerDead`, `errUnknownController`)
//! - github.com/twmb/franz-go/pkg/kgo/client.go (`retryable.Request`, `shouldRetry`,
//!   `shouldRetryNext`, `failDial.handleDialErr`, `broker()` any-broker rotation,
//!   `handleShardedReq` retry loop)
//! - github.com/twmb/franz-go/pkg/kgo/broker.go (`handleReq`: retry once on a new connection)
//! - github.com/twmb/franz-go/pkg/kgo/config.go (default `retryBackoff`, `RequestRetries`,
//!   `RequestTimeoutOverhead`)
//! - github.com/twmb/franz-go/pkg/kerr/kerr.go (`Retriable` flag of every error code)
//! - github.com/twmb/franz-go/pkg/kadm/kadm.go (default admin request `TimeoutMillis`)
//! - github.com/grafana/mimir/pkg/storage/ingest/util.go (`commonKafkaClientOptions`:
//!   `kgo.RetryTimeoutFn`)

use std::fmt;
use std::future::Future;
use std::io;
use std::time::Duration;

use tokio::time::Instant;

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::ApiKey;
use rand::Rng;

use super::kafka_client::KafkaClientError;

/// `kgo` default `RequestRetries` (config.go `retries: 20`).
pub const DEFAULT_REQUEST_RETRIES: u32 = 20;

/// `kgo` default `RequestTimeoutOverhead` (config.go `requestTimeoutOverhead: 10 * time.Second`).
/// Mimir's `commonKafkaClientOptions` does not override it, so every reader / admin client uses it
/// as the read deadline of requests without a timeout field (`ApiVersions`, `Metadata`,
/// `ListOffsets`, ...), including the connection handshake.
pub const DEFAULT_REQUEST_TIMEOUT_OVERHEAD: Duration = Duration::from_secs(10);

/// Default retry timeout returned by Mimir's `kgo.RetryTimeoutFn` for every request but
/// `ListOffsets` ("30s is the default timeout in the Kafka client").
pub const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_secs(30);

/// `fetchMetadata` limits retries for internal metadata loads ("we use a small count of 3 retries").
pub const METADATA_LIMIT_RETRIES: u32 = 3;

/// `kadm.NewClient` default request `TimeoutMillis` ("15s timeout default, matching kmsg").
pub const KADM_DEFAULT_TIMEOUT_MILLIS: i32 = 15_000;

/// Mimir's `kgo.RetryTimeoutFn` (`commonKafkaClientOptions`, pkg/storage/ingest/util.go):
/// `ListOffsets` retries for `-ingest-storage.kafka.last-produced-offset-retry-timeout`, every
/// other request for 30s. A zero duration means "no time limit" (`retryTimeout == 0` in kgo).
pub fn mimir_retry_timeout(api_key: ApiKey, last_produced_offset_retry_timeout: Duration) -> Duration {
    if api_key == ApiKey::ListOffsets {
        return last_produced_offset_retry_timeout;
    }
    DEFAULT_RETRY_TIMEOUT
}

/// `kgo` default `RetryBackoffFn` with an explicit jitter factor in `[0.8, 1.2)`.
pub fn retry_backoff_with_jitter(fails: u32, jitter: f64) -> Duration {
    const MIN: Duration = Duration::from_millis(250);
    const MAX: Duration = Duration::from_secs(5);
    if fails == 0 {
        return MIN;
    }
    if fails > 10 {
        return MAX;
    }
    let backoff = MIN * (1u32 << (fails - 1));
    let backoff = backoff.mul_f64(jitter);
    backoff.min(MAX)
}

/// `kgo` default `RetryBackoffFn`: `250ms * 2^(fails-1)` with a `[0.8, 1.2)` jitter, capped at 5s.
pub fn retry_backoff(fails: u32) -> Duration {
    let jitter = 0.8 + 0.4 * rand::thread_rng().gen::<f64>();
    retry_backoff_with_jitter(fails, jitter)
}

// ---------------------------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------------------------

/// Classification of a failed (non deadline-exceeded) TCP dial, mirroring the Go error the
/// `net.Dialer` would return (`*net.OpError{Op: "dial"}` wrapping a syscall or DNS error).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialErrorKind {
    /// `ECONNREFUSED`.
    Refused,
    /// `EHOSTUNREACH`, `ENETUNREACH`, `ENETDOWN`, `EADDRNOTAVAIL`, ...
    Unreachable,
    /// `ETIMEDOUT` returned by the kernel (a timeout error in Go).
    TimedOut,
    /// `EACCES` / `EPERM`.
    PermissionDenied,
    /// DNS NXDOMAIN (`*net.DNSError{IsNotFound: true}`).
    DnsNotFound,
    /// Temporary DNS failure (`*net.DNSError{IsTemporary: true}`).
    DnsTemporary,
    /// Malformed address (`*net.AddrError`, e.g. a missing port).
    BadAddress,
    /// Any other dial syscall failure.
    Other,
}

/// A failed dial to a broker (`unable to dial: dial tcp <addr>: ...`).
#[derive(Debug)]
pub struct DialError {
    pub addr: String,
    pub kind: DialErrorKind,
    pub source: io::Error,
}

impl fmt::Display for DialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let host = self
            .addr
            .rsplit_once(':')
            .map_or(self.addr.as_str(), |(h, _)| h)
            .trim_start_matches('[')
            .trim_end_matches(']');
        match self.kind {
            DialErrorKind::Refused => write!(f, "dial tcp {}: connect: connection refused", self.addr),
            DialErrorKind::DnsNotFound => write!(f, "dial tcp: lookup {host}: no such host"),
            DialErrorKind::DnsTemporary => write!(f, "dial tcp: lookup {host}: {}", self.source),
            DialErrorKind::BadAddress => write!(f, "dial tcp: address {}: {}", self.addr, self.source),
            _ => write!(f, "dial tcp {}: connect: {}", self.addr, self.source),
        }
    }
}

/// Which handshake step saw the broker close the connection immediately (`ErrFirstReadEOF`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirstReadEofKind {
    Dial,
    Tls,
    Sasl,
}

impl fmt::Display for FirstReadEofKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Dial => "broker closed the connection immediately after a dial, which often happens if the client is using TLS when the broker is not expecting it: is TLS misconfigured on the client or the broker?",
            Self::Tls => "broker closed the connection immediately during api versions negotiation, which often happens when the broker requires TLS but the client is using plaintext: is TLS missing?",
            Self::Sasl => "broker closed the connection immediately after a request was issued, which often happens when SASL is required but not provided: is SASL missing?",
        })
    }
}

/// Maps an error returned while establishing a new broker connection (dial + `ApiVersions` +
/// SASL handshake) onto the error shapes `kgo` distinguishes, without changing how the connection
/// itself is established:
/// - socket errors of the dial itself become [`KafkaClientError::Dial`];
/// - the broker closing the connection during the handshake becomes
///   [`KafkaClientError::FirstReadEof`] (never retried, like `ErrFirstReadEOF`);
/// - everything else (dial timeouts, handshake read timeouts, broker error codes) is unchanged.
pub fn classify_connect_error(addr: &str, err: KafkaClientError) -> KafkaClientError {
    match err {
        KafkaClientError::Io(e) => {
            if let Some(kind) = dial_error_kind(&e) {
                return KafkaClientError::Dial(DialError {
                    addr: addr.to_string(),
                    kind,
                    source: e,
                });
            }
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return KafkaClientError::FirstReadEof(e.to_string());
            }
            KafkaClientError::Io(e)
        }
        KafkaClientError::ConnectionClosed => {
            KafkaClientError::FirstReadEof(FirstReadEofKind::Tls.to_string())
        }
        other => other,
    }
}

fn dial_error_kind(e: &io::Error) -> Option<DialErrorKind> {
    use io::ErrorKind as K;
    let msg = e.to_string();
    // std / tokio report getaddrinfo failures as "failed to lookup address information: <gai_strerror>".
    if msg.contains("failed to lookup address information") {
        let temporary = msg.contains("Temporary failure") || msg.contains("try again");
        return Some(if temporary {
            DialErrorKind::DnsTemporary
        } else {
            DialErrorKind::DnsNotFound
        });
    }
    Some(match e.kind() {
        K::ConnectionRefused => DialErrorKind::Refused,
        K::HostUnreachable | K::NetworkUnreachable | K::NetworkDown | K::AddrNotAvailable | K::AddrInUse => {
            DialErrorKind::Unreachable
        }
        K::TimedOut => DialErrorKind::TimedOut,
        K::PermissionDenied => DialErrorKind::PermissionDenied,
        K::InvalidInput => DialErrorKind::BadAddress,
        _ => return None,
    })
}

// ---------------------------------------------------------------------------------------------
// Classification (kgo/errors.go)
// ---------------------------------------------------------------------------------------------

/// `kerr.Error.Retriable` for every Kafka error code flagged retriable in franz-go's `kerr`.
pub fn is_retriable_error_code(code: i16) -> bool {
    matches!(
        code,
        2 | 3 | 5 | 6 | 7 | 8 | 9 | 13 | 14 | 15 | 16 | 19 | 20 | 41 | 56 | 70 | 71 | 72 | 74 | 75
            | 78 | 80 | 83 | 84 | 88 | 89 | 100 | 103 | 106 | 133
    )
}

/// `kerr.IsRetriable(err)`.
pub fn is_kerr_retriable(err: &KafkaClientError) -> bool {
    match err {
        KafkaClientError::Broker(e) => is_retriable_response_error(*e),
        _ => false,
    }
}

pub fn is_retriable_response_error(e: ResponseError) -> bool {
    is_retriable_error_code(e.code())
}

/// `isRetryableBrokerErr`: whether the client can retry what it was just doing with a broker.
pub fn is_retryable_broker_err(err: &KafkaClientError) -> bool {
    use io::ErrorKind as K;
    match err {
        // A dial deadline exceeded is a `net.Error` timeout, which is `Temporary()`.
        KafkaClientError::ConnectTimeout { .. } => true,
        // Non-timeout dial errors are deliberately not retryable (callers opt in, e.g. failDial);
        // only timeouts and temporary DNS failures are.
        KafkaClientError::Dial(d) => matches!(d.kind, DialErrorKind::TimedOut | DialErrorKind::DnsTemporary),
        // Read / write deadline exceeded on an established connection.
        KafkaClientError::RequestTimeout(_) => true,
        // io.EOF / net.ErrClosed: the broker killed the connection.
        KafkaClientError::ConnectionClosed => true,
        // ...unless it was the very first read (bad TLS / SASL), which is not retried.
        KafkaClientError::FirstReadEof(_) => false,
        KafkaClientError::ChosenBrokerDead => true,
        KafkaClientError::UnknownController(_) => true,
        // Socket errors on an established connection are os.SyscallErrors: retried.
        KafkaClientError::Io(e) => matches!(
            e.kind(),
            K::ConnectionReset
                | K::ConnectionAborted
                | K::BrokenPipe
                | K::NotConnected
                | K::TimedOut
                | K::Interrupted
                | K::WouldBlock
                | K::UnexpectedEof
        ),
        _ => false,
    }
}

/// `isAnyDialErr`.
pub fn is_any_dial_err(err: &KafkaClientError) -> bool {
    matches!(err, KafkaClientError::Dial(_) | KafkaClientError::ConnectTimeout { .. })
}

/// `isDialNonTimeoutErr`.
pub fn is_dial_non_timeout_err(err: &KafkaClientError) -> bool {
    matches!(err, KafkaClientError::Dial(d) if d.kind != DialErrorKind::TimedOut)
}

/// `isPermanentDialErr`: DNS NXDOMAIN and local permission errors never heal by retrying.
pub fn is_permanent_dial_err(err: &KafkaClientError) -> bool {
    matches!(
        err,
        KafkaClientError::Dial(d) if matches!(d.kind, DialErrorKind::DnsNotFound | DialErrorKind::PermissionDenied)
    )
}

/// `isSkippableBrokerErr`: not retryable on this broker, but the request can move to the next one
/// (any `*net.OpError`, i.e. a dial / read / write socket error, or an unknown broker).
pub fn is_skippable_broker_err(err: &KafkaClientError) -> bool {
    matches!(
        err,
        KafkaClientError::Dial(_)
            | KafkaClientError::ConnectTimeout { .. }
            | KafkaClientError::RequestTimeout(_)
            | KafkaClientError::Io(_)
            | KafkaClientError::UnknownBroker
    )
}

/// `cl.shouldRetry(tries, err)`.
pub fn should_retry(tries: u32, err: &KafkaClientError) -> bool {
    (is_kerr_retriable(err) || is_retryable_broker_err(err)) && tries <= DEFAULT_REQUEST_RETRIES
}

/// `cl.shouldRetryNext(tries, err)`.
pub fn should_retry_next(tries: u32, err: &KafkaClientError) -> bool {
    is_skippable_broker_err(err) && tries <= DEFAULT_REQUEST_RETRIES
}

/// `failDial.handleDialErr`: transient dial errors to a pinned broker (controller, coordinator)
/// become `errChosenBrokerDead` so the retry loop retries them; permanent ones are returned as-is.
/// `clear` drops the cached broker the request was pinned to.
pub fn handle_dial_err(err: &KafkaClientError, clear: impl FnOnce()) -> Option<KafkaClientError> {
    if !is_any_dial_err(err) {
        return None;
    }
    clear();
    if is_permanent_dial_err(err) {
        return None;
    }
    Some(KafkaClientError::ChosenBrokerDead)
}

// ---------------------------------------------------------------------------------------------
// Broker selection & the retryable request loop (kgo/client.go)
// ---------------------------------------------------------------------------------------------

/// Identity of the broker a request was issued to (kgo compares `*broker` pointers: seed brokers
/// and discovered brokers are distinct even when they share an address).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BrokerKey {
    Seed(usize),
    Node(i32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerChoice {
    pub key: BrokerKey,
    pub addr: String,
}

/// `cl.broker()`: returns brokers in a shuffled order of all discovered brokers, then the next
/// seed (round robin), then reshuffles. If no broker was ever discovered this loops over seeds.
#[derive(Debug, Default)]
pub struct AnyBrokerRotation {
    order: Vec<i32>,
    seed_idx: usize,
}

impl AnyBrokerRotation {
    /// `discovered` is the sorted list of known `(node id, addr)` brokers.
    pub fn next(&mut self, seeds: &[String], discovered: &[(i32, String)]) -> Option<BrokerChoice> {
        while let Some(id) = (!self.order.is_empty()).then(|| self.order.remove(0)) {
            if let Some((_, addr)) = discovered.iter().find(|(n, _)| *n == id) {
                return Some(BrokerChoice {
                    key: BrokerKey::Node(id),
                    addr: addr.clone(),
                });
            }
        }
        if seeds.is_empty() {
            return None;
        }
        self.seed_idx %= seeds.len();
        let choice = BrokerChoice {
            key: BrokerKey::Seed(self.seed_idx),
            addr: seeds[self.seed_idx].clone(),
        };
        self.seed_idx += 1;
        // reinitAnyBrokerOrd
        self.order = discovered.iter().map(|(n, _)| *n).collect();
        let mut rng = rand::thread_rng();
        for i in (1..self.order.len()).rev() {
            let j = rng.gen_range(0..=i);
            self.order.swap(i, j);
        }
        Some(choice)
    }
}

/// `retryable.Request`: issues a request to the broker returned by `br`, retrying with the kgo
/// backoff while the error is retryable (`shouldRetry`) or skippable to a *different* broker
/// (`shouldRetryNext`), bounded by `limit_retries` (0 = `RequestRetries`) and `retry_timeout`
/// (0 = unlimited; measured from before the first broker load, including it).
///
/// `parse_retry_err` mirrors `retryable.parseRetryErr`: it can turn a response (or an error) into a
/// retry reason without changing what is returned to the caller.
pub async fn retryable_request<T, BrFut, IssueFut>(
    retry_timeout: Duration,
    limit_retries: u32,
    mut br: impl FnMut() -> BrFut,
    mut issue: impl FnMut(BrokerChoice) -> IssueFut,
    mut parse_retry_err: impl FnMut(&BrokerChoice, &Result<T, KafkaClientError>) -> Option<KafkaClientError>,
) -> Result<T, KafkaClientError>
where
    BrFut: Future<Output = Result<BrokerChoice, KafkaClientError>>,
    IssueFut: Future<Output = Result<T, KafkaClientError>>,
{
    let try_start = Instant::now();
    let mut tries: u32 = 0;
    let mut next = br().await;
    loop {
        tries += 1;
        let (res, retry_err, last_key) = match next {
            Ok(b) => {
                let res = issue(b.clone()).await;
                let retry_err = parse_retry_err(&b, &res);
                (res, retry_err, Some(b.key))
            }
            Err(e) => (Err(e), None, None),
        };

        let failed = res.is_err() || retry_err.is_some();
        if failed && (limit_retries == 0 || tries <= limit_retries) {
            let backoff = retry_backoff(tries);
            if retry_timeout.is_zero() || try_start.elapsed() + backoff <= retry_timeout {
                let err = res.as_ref().err();
                let retry = err.is_some_and(|e| should_retry(tries, e))
                    || retry_err.as_ref().is_some_and(|e| should_retry(tries, e));
                if retry {
                    tracing::debug!(tries, ?backoff, err = ?err, response_error = ?retry_err, "retrying request");
                    tokio::time::sleep(backoff).await;
                    next = br().await;
                    continue;
                } else if err.is_some_and(|e| should_retry_next(tries, e)) {
                    next = br().await;
                    let next_key = next.as_ref().ok().map(|b| b.key);
                    if next_key != last_key {
                        tracing::debug!(tries, ?backoff, err = ?err, "retrying request on next broker");
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                }
            }
        }
        return res;
    }
}

/// The per-request retry decision of `handleShardedReq` (sharded requests such as `ListOffsets` and
/// `OffsetFetch`, issued to a known broker; a failure to shard / load metadata is not retried).
/// Returns the backoff to sleep before re-sharding and re-issuing, or `None` to give up.
pub fn sharded_retry_backoff(
    start: Instant,
    retry_timeout: Duration,
    tries: u32,
    err: &KafkaClientError,
) -> Option<Duration> {
    let backoff = retry_backoff(tries);
    let not_timed_out = retry_timeout.is_zero() || start.elapsed() + backoff <= retry_timeout;
    (not_timed_out && should_retry(tries, err)).then_some(backoff)
}

fn is_transient_sasl_auth_failed(err: &KafkaClientError) -> bool {
    matches!(err, KafkaClientError::Sasl(msg) if msg.contains("SASL_AUTHENTICATION_FAILED"))
}

/// `broker.handleReq` connection loading: retries once on retryable broker errors or a
/// transient `SASL_AUTHENTICATION_FAILED` (AWS MSK IAM principal/STS race, franz-go #249).
pub async fn load_connection_retrying_once<C, Fut>(
    mut connect: impl FnMut() -> Fut,
) -> Result<C, KafkaClientError>
where
    Fut: Future<Output = Result<C, KafkaClientError>>,
{
    match connect().await {
        Err(e) if is_retryable_broker_err(&e) || is_transient_sasl_auth_failed(&e) => {
            connect().await
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    fn dial(kind: DialErrorKind) -> KafkaClientError {
        KafkaClientError::Dial(DialError {
            addr: "kafka:9092".into(),
            kind,
            source: io::Error::other("x"),
        })
    }

    #[test]
    fn retry_backoff_matches_kgo_default() {
        assert_eq!(retry_backoff_with_jitter(0, 1.0), Duration::from_millis(250));
        assert_eq!(retry_backoff_with_jitter(1, 1.0), Duration::from_millis(250));
        assert_eq!(retry_backoff_with_jitter(2, 1.0), Duration::from_millis(500));
        assert_eq!(retry_backoff_with_jitter(3, 1.0), Duration::from_secs(1));
        assert_eq!(retry_backoff_with_jitter(4, 1.0), Duration::from_secs(2));
        assert_eq!(retry_backoff_with_jitter(5, 1.0), Duration::from_secs(4));
        assert_eq!(retry_backoff_with_jitter(5, 1.2), Duration::from_millis(4800));
        assert_eq!(retry_backoff_with_jitter(6, 1.0), Duration::from_secs(5));
        assert_eq!(retry_backoff_with_jitter(6, 0.8), Duration::from_secs(5));
        assert_eq!(retry_backoff_with_jitter(11, 0.8), Duration::from_secs(5));
        assert_eq!(retry_backoff_with_jitter(2, 0.8), Duration::from_millis(400));
        for fails in 1..=12 {
            let b = retry_backoff(fails);
            assert!(b >= Duration::from_millis(200) && b <= Duration::from_secs(5), "{fails}: {b:?}");
        }
    }

    #[test]
    fn mimir_retry_timeout_fn() {
        let lp = Duration::from_secs(10);
        assert_eq!(mimir_retry_timeout(ApiKey::ListOffsets, lp), lp);
        assert_eq!(mimir_retry_timeout(ApiKey::Metadata, lp), Duration::from_secs(30));
        assert_eq!(mimir_retry_timeout(ApiKey::CreateTopics, lp), Duration::from_secs(30));
        assert_eq!(mimir_retry_timeout(ApiKey::OffsetFetch, lp), Duration::from_secs(30));
    }

    #[test]
    fn classify_connect_errors() {
        let c = |e: io::Error| classify_connect_error("kafka.invalid:9092", KafkaClientError::Io(e));
        let refused = c(io::Error::from(io::ErrorKind::ConnectionRefused));
        assert!(matches!(&refused, KafkaClientError::Dial(d) if d.kind == DialErrorKind::Refused));
        assert_eq!(
            refused.to_string(),
            "unable to dial: dial tcp kafka.invalid:9092: connect: connection refused"
        );
        let nx = c(io::Error::other(
            "failed to lookup address information: Name or service not known",
        ));
        assert!(matches!(&nx, KafkaClientError::Dial(d) if d.kind == DialErrorKind::DnsNotFound));
        assert_eq!(nx.to_string(), "unable to dial: dial tcp: lookup kafka.invalid: no such host");
        let tmp = c(io::Error::other(
            "failed to lookup address information: Temporary failure in name resolution",
        ));
        assert!(matches!(&tmp, KafkaClientError::Dial(d) if d.kind == DialErrorKind::DnsTemporary));
        assert!(matches!(
            c(io::Error::from(io::ErrorKind::HostUnreachable)),
            KafkaClientError::Dial(DialError { kind: DialErrorKind::Unreachable, .. })
        ));
        assert!(matches!(
            c(io::Error::new(io::ErrorKind::UnexpectedEof, "broker closed the connection")),
            KafkaClientError::FirstReadEof(m) if m == "broker closed the connection"
        ));
        assert!(matches!(
            c(io::Error::from(io::ErrorKind::ConnectionReset)),
            KafkaClientError::Io(_)
        ));
        assert!(matches!(
            classify_connect_error("a:1", KafkaClientError::ConnectionClosed),
            KafkaClientError::FirstReadEof(m) if m == FirstReadEofKind::Tls.to_string()
        ));
        assert!(matches!(
            classify_connect_error("a:1", KafkaClientError::RequestTimeout(Duration::from_secs(10))),
            KafkaClientError::RequestTimeout(_)
        ));
    }

    // Mirrors the decision table of franz-go's errors.go for the failure classes Mimir hits when
    // Kafka is unavailable at startup.
    #[test]
    fn retry_classification() {
        let dial_timeout = KafkaClientError::ConnectTimeout { addr: "192.0.2.1:9092".into() };
        let read_timeout = KafkaClientError::RequestTimeout(Duration::from_secs(10));

        // Connection refused: fail fast on a single seed (not retryable, skippable, transient).
        let refused = dial(DialErrorKind::Refused);
        assert!(!is_retryable_broker_err(&refused));
        assert!(is_dial_non_timeout_err(&refused));
        assert!(is_any_dial_err(&refused));
        assert!(!is_permanent_dial_err(&refused));
        assert!(is_skippable_broker_err(&refused));

        // NXDOMAIN: fail fast and permanent.
        let nx = dial(DialErrorKind::DnsNotFound);
        assert!(!is_retryable_broker_err(&nx));
        assert!(is_permanent_dial_err(&nx));
        assert!(is_skippable_broker_err(&nx));
        assert!(is_permanent_dial_err(&dial(DialErrorKind::PermissionDenied)));
        assert!(is_retryable_broker_err(&dial(DialErrorKind::DnsTemporary)));
        assert!(!is_permanent_dial_err(&dial(DialErrorKind::DnsTemporary)));

        // SYN dropped: dial timeout is retried.
        assert!(is_retryable_broker_err(&dial_timeout));
        assert!(is_any_dial_err(&dial_timeout));
        assert!(!is_dial_non_timeout_err(&dial_timeout));
        assert!(!is_permanent_dial_err(&dial_timeout));
        assert!(!is_dial_non_timeout_err(&dial(DialErrorKind::TimedOut)));
        assert!(is_retryable_broker_err(&dial(DialErrorKind::TimedOut)));

        // Accepted but silent (or paused) broker: read deadline exceeded is retried.
        assert!(is_retryable_broker_err(&read_timeout));
        assert!(!is_any_dial_err(&read_timeout));
        assert!(is_skippable_broker_err(&read_timeout));

        // EOF mid-session is retried; EOF on the first read (TLS / SASL mismatch) is not.
        assert!(is_retryable_broker_err(&KafkaClientError::ConnectionClosed));
        assert!(!is_skippable_broker_err(&KafkaClientError::ConnectionClosed));
        for k in [FirstReadEofKind::Dial, FirstReadEofKind::Tls, FirstReadEofKind::Sasl] {
            assert!(!is_retryable_broker_err(&KafkaClientError::FirstReadEof(k.to_string())));
        }
        assert!(is_retryable_broker_err(&KafkaClientError::Io(io::Error::from(
            io::ErrorKind::ConnectionReset
        ))));
        assert!(!is_retryable_broker_err(&KafkaClientError::Io(io::Error::from(
            io::ErrorKind::InvalidData
        ))));
        assert!(is_retryable_broker_err(&KafkaClientError::ChosenBrokerDead));
        assert!(is_retryable_broker_err(&KafkaClientError::UnknownController(-1)));
        assert!(is_skippable_broker_err(&KafkaClientError::UnknownBroker));

        // Kafka error codes: kerr.Retriable.
        let b = KafkaClientError::Broker;
        assert!(should_retry(1, &b(ResponseError::NotController)));
        assert!(should_retry(1, &b(ResponseError::LeaderNotAvailable)));
        assert!(should_retry(1, &b(ResponseError::NotLeaderOrFollower)));
        assert!(should_retry(1, &b(ResponseError::CoordinatorLoadInProgress)));
        assert!(should_retry(1, &b(ResponseError::UnknownTopicOrPartition)));
        assert!(!should_retry(1, &b(ResponseError::TopicAlreadyExists)));
        assert!(!should_retry(1, &b(ResponseError::SaslAuthenticationFailed)));
        assert!(!should_retry(1, &b(ResponseError::GroupIdNotFound)));
        assert!(!should_retry(1, &b(ResponseError::UnknownServerError)));

        // RequestRetries bounds both decisions.
        assert!(should_retry(20, &read_timeout));
        assert!(!should_retry(21, &read_timeout));
        assert!(should_retry_next(20, &refused));
        assert!(!should_retry_next(21, &refused));
        assert!(!should_retry_next(1, &KafkaClientError::ConnectionClosed));
    }

    #[test]
    fn fail_dial_handling() {
        let cleared = AtomicU32::new(0);
        let clear = || {
            cleared.fetch_add(1, Ordering::SeqCst);
        };
        assert!(matches!(
            handle_dial_err(&dial(DialErrorKind::Refused), clear),
            Some(KafkaClientError::ChosenBrokerDead)
        ));
        assert!(handle_dial_err(&dial(DialErrorKind::DnsNotFound), clear).is_none());
        assert!(matches!(
            handle_dial_err(&KafkaClientError::ConnectTimeout { addr: "a:1".into() }, clear),
            Some(KafkaClientError::ChosenBrokerDead)
        ));
        assert!(handle_dial_err(&KafkaClientError::RequestTimeout(Duration::ZERO), clear).is_none());
        assert_eq!(cleared.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn any_broker_rotation_loops_seeds_then_discovered() {
        let seeds = vec!["s0:1".to_string(), "s1:1".to_string()];
        let mut r = AnyBrokerRotation::default();
        assert_eq!(r.next(&seeds, &[]).unwrap().key, BrokerKey::Seed(0));
        assert_eq!(r.next(&seeds, &[]).unwrap().key, BrokerKey::Seed(1));
        assert_eq!(r.next(&seeds, &[]).unwrap().key, BrokerKey::Seed(0));
        let discovered = vec![(1, "b1:1".to_string()), (2, "b2:1".to_string())];
        // The seed is returned first (order was empty), then both discovered brokers, then a seed.
        assert_eq!(r.next(&seeds, &discovered).unwrap().key, BrokerKey::Seed(1));
        let mut ids = vec![r.next(&seeds, &discovered).unwrap().key, r.next(&seeds, &discovered).unwrap().key];
        ids.sort_by_key(|k| format!("{k:?}"));
        assert_eq!(ids, vec![BrokerKey::Node(1), BrokerKey::Node(2)]);
        assert_eq!(r.next(&seeds, &discovered).unwrap().key, BrokerKey::Seed(0));
        assert!(AnyBrokerRotation::default().next(&[], &[]).is_none());
    }

    fn seed() -> BrokerChoice {
        BrokerChoice {
            key: BrokerKey::Seed(0),
            addr: "s:1".into(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_single_seed_refused_fails_fast() {
        let issued = Arc::new(AtomicU32::new(0));
        let i2 = Arc::clone(&issued);
        let start = tokio::time::Instant::now();
        let res: Result<(), _> = retryable_request(
            DEFAULT_RETRY_TIMEOUT,
            0,
            || async { Ok(seed()) },
            |_| async {
                i2.fetch_add(1, Ordering::SeqCst);
                Err(dial(DialErrorKind::Refused))
            },
            |_, _| None,
        )
        .await;
        assert!(matches!(res, Err(KafkaClientError::Dial(_))));
        assert_eq!(issued.load(Ordering::SeqCst), 1);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_refused_rotates_to_next_seed() {
        let n = AtomicU32::new(0);
        let issued = AtomicU32::new(0);
        let res = retryable_request(
            DEFAULT_RETRY_TIMEOUT,
            0,
            || async {
                let i = n.fetch_add(1, Ordering::SeqCst) as usize % 2;
                Ok(BrokerChoice {
                    key: BrokerKey::Seed(i),
                    addr: format!("s{i}:1"),
                })
            },
            |b: BrokerChoice| {
                let issued = &issued;
                async move {
                issued.fetch_add(1, Ordering::SeqCst);
                if b.key == BrokerKey::Seed(0) {
                    Err(dial(DialErrorKind::Refused))
                } else {
                    Ok(7)
                }
                }
            },
            |_, _| None,
        )
        .await;
        assert_eq!(res.unwrap(), 7);
        assert_eq!(issued.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_timeouts_retry_until_retry_timeout() {
        let issued = AtomicU32::new(0);
        let start = tokio::time::Instant::now();
        let res: Result<(), _> = retryable_request(
            DEFAULT_RETRY_TIMEOUT,
            0,
            || async { Ok(seed()) },
            |_| async {
                issued.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(10)).await;
                Err(KafkaClientError::RequestTimeout(Duration::from_secs(10)))
            },
            |_, _| None,
        )
        .await;
        assert!(matches!(res, Err(KafkaClientError::RequestTimeout(_))));
        // 10s, backoff ~250ms, 10s, then 20.x + backoff(2) <= 30 -> third try, then > 30s.
        assert_eq!(issued.load(Ordering::SeqCst), 3);
        assert!(start.elapsed() >= Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_limit_retries_bounds_tries() {
        let issued = AtomicU32::new(0);
        let res: Result<(), _> = retryable_request(
            DEFAULT_RETRY_TIMEOUT,
            METADATA_LIMIT_RETRIES,
            || async { Ok(seed()) },
            |_| async {
                issued.fetch_add(1, Ordering::SeqCst);
                Err(KafkaClientError::ConnectTimeout { addr: "s:1".into() })
            },
            |_, _| None,
        )
        .await;
        assert!(res.is_err());
        assert_eq!(issued.load(Ordering::SeqCst), METADATA_LIMIT_RETRIES + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_recovers_when_broker_comes_back() {
        let issued = AtomicU32::new(0);
        let res = retryable_request(
            DEFAULT_RETRY_TIMEOUT,
            0,
            || async { Ok(seed()) },
            |_| async {
                if issued.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(KafkaClientError::ConnectTimeout { addr: "s:1".into() })
                } else {
                    Ok("ok")
                }
            },
            |_, _| None,
        )
        .await;
        assert_eq!(res.unwrap(), "ok");
        assert_eq!(issued.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_parse_retry_err_retries_successful_responses() {
        // e.g. CreateTopics answered with NOT_CONTROLLER: retried, the response is returned at the end.
        let issued = AtomicU32::new(0);
        let res = retryable_request(
            Duration::from_secs(2),
            0,
            || async { Ok(seed()) },
            |_| async { Ok(issued.fetch_add(1, Ordering::SeqCst)) },
            |_, r| (*r.as_ref().unwrap() < 2).then_some(KafkaClientError::Broker(ResponseError::NotController)),
        )
        .await;
        assert_eq!(res.unwrap(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_broker_load_error_is_retried_like_a_request_error() {
        // handleAdminReq: the controller load (a metadata request) fails with a retryable error.
        let loads = AtomicU32::new(0);
        let res: Result<(), _> = retryable_request(
            DEFAULT_RETRY_TIMEOUT,
            0,
            || async {
                loads.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(18)).await;
                Err(KafkaClientError::ConnectTimeout { addr: "s:1".into() })
            },
            |_| async { Ok(()) },
            |_, _| None,
        )
        .await;
        assert!(res.is_err());
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn load_connection_retries_once_on_retryable_errors() {
        let n = AtomicU32::new(0);
        let r: Result<(), _> = load_connection_retrying_once(|| async {
            n.fetch_add(1, Ordering::SeqCst);
            Err(KafkaClientError::ConnectTimeout { addr: "a:1".into() })
        })
        .await;
        assert!(r.is_err());
        assert_eq!(n.load(Ordering::SeqCst), 2);
        let n = AtomicU32::new(0);
        let r: Result<(), _> = load_connection_retrying_once(|| async {
            n.fetch_add(1, Ordering::SeqCst);
            Err(dial(DialErrorKind::Refused))
        })
        .await;
        assert!(r.is_err());
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn sharded_retry_decision() {
        let start = Instant::now();
        let rt = Duration::from_secs(10);
        assert!(sharded_retry_backoff(start, rt, 1, &KafkaClientError::RequestTimeout(rt)).is_some());
        assert!(sharded_retry_backoff(start, rt, 1, &dial(DialErrorKind::Refused)).is_none());
        assert!(sharded_retry_backoff(start, Duration::from_millis(100), 1, &KafkaClientError::ConnectionClosed).is_none());
        assert!(sharded_retry_backoff(start, Duration::ZERO, 5, &KafkaClientError::ConnectionClosed).is_some());
        assert!(sharded_retry_backoff(start, rt, 21, &KafkaClientError::ConnectionClosed).is_none());
    }
}
