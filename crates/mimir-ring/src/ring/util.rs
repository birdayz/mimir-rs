//! Instance address/port resolution for ring registration.
//!
//! Ported from github.com/grafana/dskit/ring/util.go (`GetInstanceAddr`, `GetInstancePort`)
//! and Go's `net.JoinHostPort`.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use mimir_common::backoff::{Backoff, BackoffConfig, BackoffError};
use mimir_common::netutil::{self, NetutilError};
use mimir_common::services::ServiceError;
use mimir_proto::ringpb::InstanceState;

use super::replication_set::{
    has_replication_set_changed, has_replication_set_changed_without_state,
};
use super::replication_set_tracker::{ContextError, RingContext};
use super::replication_strategy::{Operation, ReplicationSet};
use super::ReadRing;

/// `GetInstanceAddr` returns the address to use to register the instance in the ring.
pub fn get_instance_addr(
    config_addr: &str,
    net_interfaces: &[String],
    enable_inet6: bool,
) -> Result<String, NetutilError> {
    get_instance_addr_with(config_addr, enable_inet6, |inet6| {
        netutil::get_first_address_of(net_interfaces, inet6)
    })
}

/// `GetInstanceAddr` with the `netutil.GetFirstAddressOf` call injectable for tests.
pub fn get_instance_addr_with(
    config_addr: &str,
    enable_inet6: bool,
    get_first_address_of: impl FnOnce(bool) -> Result<String, NetutilError>,
) -> Result<String, NetutilError> {
    if !config_addr.is_empty() {
        return Ok(config_addr.to_string());
    }
    get_first_address_of(enable_inet6)
}

/// `GetInstancePort` returns the port to use to register the instance in the ring.
pub fn get_instance_port(config_port: i64, listen_port: i64) -> i64 {
    if config_port > 0 {
        return config_port;
    }
    listen_port
}

/// Go `net.JoinHostPort`: brackets the host when it contains a colon.
pub fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Waits until `instance_id` is registered in the ring matching `state`, or `ctx` expires/cancels.
pub async fn wait_instance_state(
    ctx: &RingContext,
    r: &dyn ReadRing,
    instance_id: &str,
    state: InstanceState,
) -> Result<(), BackoffError> {
    let mut backoff = Backoff::new(BackoffConfig {
        min_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(1),
        max_retries: 0,
    });

    while !ctx.is_canceled() && backoff.ongoing() {
        if let Ok(actual_state) = r.get_instance_state(instance_id) {
            if actual_state == state {
                return Ok(());
            }
        }
        let delay = backoff.next_delay();
        if !ctx.is_canceled() && backoff.ongoing() && !delay.is_zero() {
            tokio::select! {
                biased;
                _ = ctx.done() => break,
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

    match ctx.err() {
        Some(ContextError::DeadlineExceeded) => Err(BackoffError::DeadlineExceeded),
        Some(ContextError::Canceled) => Err(BackoffError::Canceled),
        None => Err(backoff.err().unwrap_or(BackoffError::Canceled)),
    }
}

/// Monitors ring topology for `op` and waits until it stays stable for at least `min_stability`.
pub async fn wait_ring_stability(
    ctx: &RingContext,
    r: &dyn ReadRing,
    op: Operation,
    min_stability: Duration,
    max_waiting: Duration,
) -> Result<(), ServiceError> {
    wait_stability(ctx, r, op, min_stability, max_waiting, has_replication_set_changed).await
}

/// Waits until the ring topology (ignoring instance state transitions) stays stable for `min_stability`.
pub async fn wait_ring_tokens_stability(
    ctx: &RingContext,
    r: &dyn ReadRing,
    op: Operation,
    min_stability: Duration,
    max_waiting: Duration,
) -> Result<(), ServiceError> {
    wait_stability(
        ctx,
        r,
        op,
        min_stability,
        max_waiting,
        has_replication_set_changed_without_state,
    )
    .await
}

async fn wait_stability(
    ctx: &RingContext,
    r: &dyn ReadRing,
    op: Operation,
    min_stability: Duration,
    max_waiting: Duration,
    is_changed: fn(&ReplicationSet, &ReplicationSet) -> bool,
) -> Result<(), ServiceError> {
    let (timeout_ctx, _cancel) = ctx.with_timeout(max_waiting);

    let mut ring_last_state = r.get_all_healthy(op).unwrap_or_default();
    let mut ring_last_state_ts = Instant::now();

    let polling_frequency = Duration::from_secs(1);
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + polling_frequency,
        polling_frequency,
    );

    loop {
        tokio::select! {
            biased;
            _ = timeout_ctx.done() => {
                return match timeout_ctx.err() {
                    Some(ContextError::DeadlineExceeded) => Err(ServiceError::DeadlineExceeded),
                    _ => Err(ServiceError::Canceled),
                };
            }
            _ = ticker.tick() => {
                let curr_ring_state = r.get_all_healthy(op).unwrap_or_default();
                if is_changed(&ring_last_state, &curr_ring_state) {
                    ring_last_state = curr_ring_state;
                    ring_last_state_ts = Instant::now();
                } else if ring_last_state_ts.elapsed() >= min_stability {
                    return Ok(());
                }
            }
        }
    }
}

/// Returns sorted zone keys from a per-zone token map.
pub fn get_zones(tokens: &HashMap<String, Vec<u32>>) -> Vec<String> {
    let mut zones: Vec<String> = tokens.keys().cloned().collect();
    zones.sort();
    zones
}

/// String set optimized for small sizes (slice up to initial capacity, switching to a HashSet
/// when capacity is exceeded), matching `dskit/ring.stringSet`.
#[derive(Debug, Clone)]
pub struct StringSet {
    set_slice: Vec<String>,
    slice_cap: usize,
    set_map: Option<HashSet<String>>,
    count: usize,
}

impl StringSet {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            set_slice: Vec::with_capacity(cap),
            slice_cap: cap,
            set_map: None,
            count: 0,
        }
    }

    pub fn contains(&self, s: &str) -> bool {
        if let Some(ref map) = self.set_map {
            map.contains(s)
        } else {
            self.set_slice.iter().any(|x| x == s)
        }
    }

    pub fn add(&mut self, s: String) {
        self.count += 1;
        if let Some(ref mut map) = self.set_map {
            map.insert(s);
        } else if self.set_slice.len() < self.slice_cap {
            self.set_slice.push(s);
        } else {
            let mut map = HashSet::with_capacity((self.set_slice.len() * 2).max(4));
            for h in self.set_slice.drain(..) {
                map.insert(h);
            }
            map.insert(s);
            self.set_map = Some(map);
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn is_map_mode(&self) -> bool {
        self.set_map.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_instance_addr_prefers_configured_addr() {
        let got = get_instance_addr_with("1.2.3.4", false, |_| panic!("must not auto-detect"));
        assert_eq!(got, Ok("1.2.3.4".to_string()));
        // Configured addresses are returned verbatim (no validation, like Go).
        let got = get_instance_addr_with("ingester-0.svc", true, |_| panic!("must not auto-detect"));
        assert_eq!(got, Ok("ingester-0.svc".to_string()));
    }

    #[test]
    fn get_instance_addr_auto_detects_and_propagates_errors() {
        let got = get_instance_addr_with("", true, |inet6| {
            assert!(inet6);
            Ok("fc99::9a".to_string())
        });
        assert_eq!(got, Ok("fc99::9a".to_string()));
        let err = NetutilError("no useable address found for interfaces [nope0]".into());
        let got = get_instance_addr_with("", false, |_| Err(err.clone()));
        assert_eq!(got, Err(err));
    }

    #[test]
    fn get_instance_addr_unknown_interface_errors() {
        let got = get_instance_addr("", &["mimir-no-such-if0".to_string()], false);
        assert_eq!(
            got,
            Err(NetutilError(
                "no useable address found for interfaces [mimir-no-such-if0]".into()
            ))
        );
    }

    #[test]
    fn get_instance_port_cases() {
        assert_eq!(get_instance_port(0, 9095), 9095);
        assert_eq!(get_instance_port(-1, 9095), 9095);
        assert_eq!(get_instance_port(1111, 9095), 1111);
    }

    #[test]
    fn join_host_port_cases() {
        assert_eq!(join_host_port("10.0.0.1", "9095"), "10.0.0.1:9095");
        assert_eq!(join_host_port("fc99::9a", "9095"), "[fc99::9a]:9095");
        assert_eq!(join_host_port("localhost", "0"), "localhost:0");
    }
}
