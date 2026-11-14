//! Ingester, ring, partition ring, lifecycler, and gRPC client configurations.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_ring.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_partition_ring.go
//! - github.com/grafana/mimir/pkg/ingester/instance_limits.go
//! - github.com/grafana/mimir/pkg/ingester/circuitbreaker.go
//! - github.com/grafana/mimir/pkg/ingester/activeseries/active_series.go
//! - github.com/grafana/mimir/pkg/ingester/label_names_and_values.go
//! - github.com/grafana/mimir/pkg/ingester/client/client.go
//! - github.com/grafana/mimir/pkg/util/reactivelimiter/*
//! - github.com/grafana/dskit/kv/*
//! - github.com/grafana/dskit/grpcclient/grpcclient.go

use serde::{Deserialize, Serialize};

use crate::duration::GoDuration;
use crate::error::ConfigError;
use crate::flags::FlagSet;
use crate::types::{HumanBytes, StringSliceCSV};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterValidationConfig {
    pub label: String,
}

impl ClusterValidationConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}label"), &self.label, "Primary cluster validation label.", move |t| &get(t).label, move |t| &mut get_mut(t).label);
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConsulConfig {
    pub host: String,
    pub acl_token: String,
    pub http_client_timeout: GoDuration,
    pub consistent_reads: bool,
    pub watch_rate_limit: f64,
    pub watch_burst_size: i64,
    pub cas_retry_delay: GoDuration,
}

impl Default for ConsulConfig {
    fn default() -> Self {
        Self {
            host: "localhost:8500".to_string(),
            acl_token: String::new(),
            http_client_timeout: GoDuration::from_secs(20),
            consistent_reads: false,
            watch_rate_limit: 1.0,
            watch_burst_size: 1,
            cas_retry_delay: GoDuration::from_secs(1),
        }
    }
}

impl ConsulConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}consul.hostname"), &self.host, "Hostname and port of Consul.", move |t| &get(t).host, move |t| &mut get_mut(t).host);
        f.string_var(&format!("{prefix}consul.acl-token"), &self.acl_token, "ACL Token used to interact with Consul.", move |t| &get(t).acl_token, move |t| &mut get_mut(t).acl_token);
        f.go_duration_var(&format!("{prefix}consul.client-timeout"), self.http_client_timeout, "HTTP timeout when talking to Consul", move |t| &get(t).http_client_timeout, move |t| &mut get_mut(t).http_client_timeout);
        f.bool_var(&format!("{prefix}consul.consistent-reads"), self.consistent_reads, "Enable consistent reads to Consul.", move |t| &get(t).consistent_reads, move |t| &mut get_mut(t).consistent_reads);
        f.f64_var(&format!("{prefix}consul.watch-rate-limit"), self.watch_rate_limit, "Rate limit when watching key or prefix in Consul, in requests per second.", move |t| &get(t).watch_rate_limit, move |t| &mut get_mut(t).watch_rate_limit);
        f.i64_var(&format!("{prefix}consul.watch-burst-size"), self.watch_burst_size, "Burst size used in rate limit.", move |t| &get(t).watch_burst_size, move |t| &mut get_mut(t).watch_burst_size);
        f.go_duration_var(&format!("{prefix}consul.cas-retry-delay"), self.cas_retry_delay, "Maximum duration to wait before retrying a Compare And Swap (CAS) operation.", move |t| &get(t).cas_retry_delay, move |t| &mut get_mut(t).cas_retry_delay);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EtcdConfig {
    pub endpoints: Vec<String>,
    pub dial_timeout: GoDuration,
    pub max_retries: i64,
    pub tls_enabled: bool,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub tls_ca_path: String,
    pub tls_server_name: String,
    pub tls_insecure_skip_verify: bool,
    pub tls_cipher_suites: String,
    pub tls_min_version: String,
    pub username: String,
    pub password: String,
}

impl Default for EtcdConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            dial_timeout: GoDuration::from_secs(10),
            max_retries: 10,
            tls_enabled: false,
            tls_cert_path: String::new(),
            tls_key_path: String::new(),
            tls_ca_path: String::new(),
            tls_server_name: String::new(),
            tls_insecure_skip_verify: false,
            tls_cipher_suites: String::new(),
            tls_min_version: String::new(),
            username: String::new(),
            password: String::new(),
        }
    }
}

impl EtcdConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_slice_var(&format!("{prefix}etcd.endpoints"), &self.endpoints, "The etcd endpoints to connect to.", move |t| &get(t).endpoints, move |t| &mut get_mut(t).endpoints);
        f.go_duration_var(&format!("{prefix}etcd.dial-timeout"), self.dial_timeout, "The dial timeout for the etcd connection.", move |t| &get(t).dial_timeout, move |t| &mut get_mut(t).dial_timeout);
        f.i64_var(&format!("{prefix}etcd.max-retries"), self.max_retries, "The maximum number of retries to do for failed ops.", move |t| &get(t).max_retries, move |t| &mut get_mut(t).max_retries);
        f.bool_var(&format!("{prefix}etcd.tls-enabled"), self.tls_enabled, "Enable TLS for the etcd client connection.", move |t| &get(t).tls_enabled, move |t| &mut get_mut(t).tls_enabled);
        f.string_var(&format!("{prefix}etcd.tls-cert-path"), &self.tls_cert_path, "Path to the client certificate.", move |t| &get(t).tls_cert_path, move |t| &mut get_mut(t).tls_cert_path);
        f.string_var(&format!("{prefix}etcd.tls-key-path"), &self.tls_key_path, "Path to the client key.", move |t| &get(t).tls_key_path, move |t| &mut get_mut(t).tls_key_path);
        f.string_var(&format!("{prefix}etcd.tls-ca-path"), &self.tls_ca_path, "Path to the CA certificate.", move |t| &get(t).tls_ca_path, move |t| &mut get_mut(t).tls_ca_path);
        f.string_var(&format!("{prefix}etcd.tls-server-name"), &self.tls_server_name, "Override the expected server name.", move |t| &get(t).tls_server_name, move |t| &mut get_mut(t).tls_server_name);
        f.bool_var(&format!("{prefix}etcd.tls-insecure-skip-verify"), self.tls_insecure_skip_verify, "Skip TLS certificate verification.", move |t| &get(t).tls_insecure_skip_verify, move |t| &mut get_mut(t).tls_insecure_skip_verify);
        f.string_var(&format!("{prefix}etcd.tls-cipher-suites"), &self.tls_cipher_suites, "TLS cipher suites.", move |t| &get(t).tls_cipher_suites, move |t| &mut get_mut(t).tls_cipher_suites);
        f.string_var(&format!("{prefix}etcd.tls-min-version"), &self.tls_min_version, "Minimum TLS version.", move |t| &get(t).tls_min_version, move |t| &mut get_mut(t).tls_min_version);
        f.string_var(&format!("{prefix}etcd.username"), &self.username, "Etcd username.", move |t| &get(t).username, move |t| &mut get_mut(t).username);
        f.string_var(&format!("{prefix}etcd.password"), &self.password, "Etcd password.", move |t| &get(t).password, move |t| &mut get_mut(t).password);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MultiKVConfig {
    pub primary: String,
    pub secondary: String,
    pub mirror_enabled: bool,
    pub mirror_timeout: GoDuration,
}

impl Default for MultiKVConfig {
    fn default() -> Self {
        Self {
            primary: String::new(),
            secondary: String::new(),
            mirror_enabled: false,
            mirror_timeout: GoDuration::from_secs(2),
        }
    }
}

impl MultiKVConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}multi.primary"), &self.primary, "Primary backend storage used by multi-client.", move |t| &get(t).primary, move |t| &mut get_mut(t).primary);
        f.string_var(&format!("{prefix}multi.secondary"), &self.secondary, "Secondary backend storage used by multi-client.", move |t| &get(t).secondary, move |t| &mut get_mut(t).secondary);
        f.bool_var(&format!("{prefix}multi.mirror-enabled"), self.mirror_enabled, "Mirror writes to the secondary store.", move |t| &get(t).mirror_enabled, move |t| &mut get_mut(t).mirror_enabled);
        f.go_duration_var(&format!("{prefix}multi.mirror-timeout"), self.mirror_timeout, "Timeout for storing a value to the secondary store.", move |t| &get(t).mirror_timeout, move |t| &mut get_mut(t).mirror_timeout);
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KVStoreConfig {
    pub store: String,
    pub prefix: String,
    pub consul: ConsulConfig,
    pub etcd: EtcdConfig,
    pub multi: MultiKVConfig,
}

impl Default for KVStoreConfig {
    fn default() -> Self {
        Self {
            store: "memberlist".to_string(),
            prefix: "collectors/".to_string(),
            consul: ConsulConfig::default(),
            etcd: EtcdConfig::default(),
            multi: MultiKVConfig::default(),
        }
    }
}

impl KVStoreConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        flags_prefix: &str,
        default_prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        self.consul.register_flags_into(flags_prefix, f, move |t| &get(t).consul, move |t| &mut get_mut(t).consul);
        self.etcd.register_flags_into(flags_prefix, f, move |t| &get(t).etcd, move |t| &mut get_mut(t).etcd);
        self.multi.register_flags_into(flags_prefix, f, move |t| &get(t).multi, move |t| &mut get_mut(t).multi);

        let effective = if flags_prefix.is_empty() {
            "ring."
        } else {
            flags_prefix
        };
        f.string_var(&format!("{effective}prefix"), default_prefix, "The prefix for the keys in the store. Should end with a /.", move |t| &get(t).prefix, move |t| &mut get_mut(t).prefix);
        f.string_var(&format!("{effective}store"), &self.store, "Backend storage to use for the ring. Supported values are: consul, etcd, inmemory, memberlist, multi.", move |t| &get(t).store, move |t| &mut get_mut(t).store);
    }
}

fn default_hostname() -> String {
    #[cfg(target_os = "linux")]
    if let Ok(s) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
        let trimmed = s.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RingConfig {
    pub kvstore: KVStoreConfig,
    pub heartbeat_period: GoDuration,
    pub heartbeat_timeout: GoDuration,
    pub replication_factor: i64,
    pub zone_awareness_enabled: bool,
    pub excluded_zones: StringSliceCSV,
    pub tokens_file_path: String,
    pub num_tokens: i64,
    pub instance_id: String,
    pub instance_interface_names: Vec<String>,
    pub instance_port: i64,
    pub instance_addr: String,
    pub instance_enable_ipv6: bool,
    pub instance_availability_zone: String,
    pub unregister_on_shutdown: bool,
    pub observe_period: GoDuration,
    pub min_ready_duration: GoDuration,
    pub final_sleep: GoDuration,
    pub token_generation_strategy: String,
    pub spread_minimizing_join_ring_in_order: bool,
    pub spread_minimizing_zones: StringSliceCSV,
    pub status_page_ingester_link_template: String,
}

impl Default for RingConfig {
    fn default() -> Self {
        Self {
            kvstore: KVStoreConfig::default(),
            heartbeat_period: GoDuration::from_secs(15),
            heartbeat_timeout: GoDuration::from_mins(1),
            replication_factor: 3,
            zone_awareness_enabled: false,
            excluded_zones: StringSliceCSV::default(),
            tokens_file_path: String::new(),
            num_tokens: 128,
            instance_id: default_hostname(),
            instance_interface_names: vec!["eth0".to_string(), "en0".to_string()],
            instance_port: 0,
            instance_addr: String::new(),
            instance_enable_ipv6: false,
            instance_availability_zone: String::new(),
            unregister_on_shutdown: true,
            observe_period: GoDuration::ZERO,
            min_ready_duration: GoDuration::from_secs(15),
            final_sleep: GoDuration::ZERO,
            token_generation_strategy: "random".to_string(),
            spread_minimizing_join_ring_in_order: false,
            spread_minimizing_zones: StringSliceCSV::default(),
            status_page_ingester_link_template: "http://{host}:8080/ingester/tenants".to_string(),
        }
    }
}

impl RingConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        let prefix = "ingester.ring.";
        self.kvstore
            .register_flags_into(prefix, "collectors/", f, move |t| &get(t).kvstore, move |t| &mut get_mut(t).kvstore);
        f.go_duration_var(&format!("{prefix}heartbeat-period"), self.heartbeat_period, "Period at which to heartbeat to the ring.", move |t| &get(t).heartbeat_period, move |t| &mut get_mut(t).heartbeat_period);
        f.go_duration_var(&format!("{prefix}heartbeat-timeout"), self.heartbeat_timeout, "The heartbeat timeout after which ingesters are skipped for reads/writes.", move |t| &get(t).heartbeat_timeout, move |t| &mut get_mut(t).heartbeat_timeout);
        f.i64_var(&format!("{prefix}replication-factor"), self.replication_factor, "Number of ingesters that each time series is replicated to.", move |t| &get(t).replication_factor, move |t| &mut get_mut(t).replication_factor);
        f.bool_var(&format!("{prefix}zone-awareness-enabled"), self.zone_awareness_enabled, "True to enable the zone-awareness and replicate ingested samples across different availability zones.", move |t| &get(t).zone_awareness_enabled, move |t| &mut get_mut(t).zone_awareness_enabled);
        f.string_slice_csv_var(&format!("{prefix}excluded-zones"), &self.excluded_zones, "Comma-separated list of zones to exclude from the ring.", move |t| &get(t).excluded_zones, move |t| &mut get_mut(t).excluded_zones);
        f.string_var(&format!("{prefix}tokens-file-path"), &self.tokens_file_path, "File path where tokens are stored.", move |t| &get(t).tokens_file_path, move |t| &mut get_mut(t).tokens_file_path);
        f.i64_var(&format!("{prefix}num-tokens"), self.num_tokens, "Number of tokens for each ingester.", move |t| &get(t).num_tokens, move |t| &mut get_mut(t).num_tokens);
        f.string_var(&format!("{prefix}instance-id"), &self.instance_id, "Instance ID to register in the ring.", move |t| &get(t).instance_id, move |t| &mut get_mut(t).instance_id);
        f.string_slice_var(&format!("{prefix}instance-interface-names"), &self.instance_interface_names, "List of network interface names to look up when finding the instance IP address.", move |t| &get(t).instance_interface_names, move |t| &mut get_mut(t).instance_interface_names);
        f.i64_var(&format!("{prefix}instance-port"), self.instance_port, "Port to advertise in the ring (defaults to -server.grpc-listen-port).", move |t| &get(t).instance_port, move |t| &mut get_mut(t).instance_port);
        f.string_var(&format!("{prefix}instance-addr"), &self.instance_addr, "IP address to advertise in the ring. Default is auto-detected.", move |t| &get(t).instance_addr, move |t| &mut get_mut(t).instance_addr);
        f.bool_var(&format!("{prefix}instance-enable-ipv6"), self.instance_enable_ipv6, "Enable using an IPv6 instance address.", move |t| &get(t).instance_enable_ipv6, move |t| &mut get_mut(t).instance_enable_ipv6);
        f.string_var(&format!("{prefix}instance-availability-zone"), &self.instance_availability_zone, "The availability zone where this instance is running.", move |t| &get(t).instance_availability_zone, move |t| &mut get_mut(t).instance_availability_zone);
        f.bool_var(&format!("{prefix}unregister-on-shutdown"), self.unregister_on_shutdown, "Unregister from the ring upon clean shutdown.", move |t| &get(t).unregister_on_shutdown, move |t| &mut get_mut(t).unregister_on_shutdown);
        f.go_duration_var(&format!("{prefix}observe-period"), self.observe_period, "Observe tokens after generating to resolve collisions.", move |t| &get(t).observe_period, move |t| &mut get_mut(t).observe_period);
        f.go_duration_var(&format!("{prefix}min-ready-duration"), self.min_ready_duration, "Minimum duration to wait after the internal readiness checks have passed.", move |t| &get(t).min_ready_duration, move |t| &mut get_mut(t).min_ready_duration);
        f.go_duration_var(&format!("{prefix}final-sleep"), self.final_sleep, "Duration to sleep for before exiting, to ensure metrics are scraped.", move |t| &get(t).final_sleep, move |t| &mut get_mut(t).final_sleep);
        f.string_var(&format!("{prefix}token-generation-strategy"), &self.token_generation_strategy, "Specifies the strategy used for generating tokens for ingesters. Supported values are: random, spread-minimizing.", move |t| &get(t).token_generation_strategy, move |t| &mut get_mut(t).token_generation_strategy);
        f.bool_var(&format!("{prefix}spread-minimizing-join-ring-in-order"), self.spread_minimizing_join_ring_in_order, "True to allow this ingester registering tokens in the ring only after all previous ingesters.", move |t| &get(t).spread_minimizing_join_ring_in_order, move |t| &mut get_mut(t).spread_minimizing_join_ring_in_order);
        f.string_slice_csv_var(&format!("{prefix}spread-minimizing-zones"), &self.spread_minimizing_zones, "Comma-separated list of zones in which spread minimizing strategy is used.", move |t| &get(t).spread_minimizing_zones, move |t| &mut get_mut(t).spread_minimizing_zones);
        f.string_var(&format!("{prefix}status-page-ingester-link-template"), &self.status_page_ingester_link_template, "Template of the link from each ingester on the ingester ring status page.", move |t| &get(t).status_page_ingester_link_template, move |t| &mut get_mut(t).status_page_ingester_link_template);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.token_generation_strategy != "random"
            && self.token_generation_strategy != "spread-minimizing"
        {
            return Err(ConfigError::Validation(format!(
                "unsupported token generation strategy ({:?}) has been chosen for token-generation-strategy",
                self.token_generation_strategy
            )));
        }

        if self.num_tokens == 0 {
            if !self.tokens_file_path.is_empty() {
                return Err(ConfigError::Validation(
                    "tokens file path must be empty when ring tokens are disabled".to_string(),
                ));
            }
            if self.token_generation_strategy == "spread-minimizing" {
                return Err(ConfigError::Validation(
                    "spread minimizing token generation strategy is not supported when ring tokens are disabled".to_string(),
                ));
            }
            return Ok(());
        }

        if self.token_generation_strategy == "spread-minimizing" {
            if !self.tokens_file_path.is_empty() {
                return Err(ConfigError::Validation(
                    "\"spread-minimizing\" token generation strategy is misconfigured: strategy requires \"tokens-file-path\" to be empty".to_string(),
                ));
            }
            if self.instance_id.is_empty() {
                return Err(ConfigError::Validation(
                    "\"spread-minimizing\" token generation strategy is misconfigured: instance id cannot be empty".to_string(),
                ));
            }
            if self.spread_minimizing_zones.0.is_empty() || self.spread_minimizing_zones.0.len() > 8
            {
                return Err(ConfigError::Validation(format!(
                    "\"spread-minimizing\" token generation strategy is misconfigured: number of zones {} is not correct: it must be greater than 0 and less or equal than 8",
                    self.spread_minimizing_zones.0.len()
                )));
            }
            if !self
                .spread_minimizing_zones
                .0
                .contains(&self.instance_availability_zone)
            {
                return Err(ConfigError::Validation(format!(
                    "\"spread-minimizing\" token generation strategy is misconfigured: zone {} is not valid",
                    self.instance_availability_zone
                )));
            }
            let valid_suffix = self
                .instance_id
                .rfind('-')
                .map(|pos| &self.instance_id[pos + 1..])
                .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()));
            if !valid_suffix {
                return Err(ConfigError::Validation(format!(
                    "\"spread-minimizing\" token generation strategy is misconfigured: unable to extract instance id from {:?}",
                    self.instance_id
                )));
            }
            return Ok(());
        }

        if self.spread_minimizing_join_ring_in_order {
            return Err(ConfigError::Validation(format!(
                "\"spread-minimizing-join-ring-in-order\" must be false when using {:?} token generation strategy",
                self.token_generation_strategy
            )));
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PartitionRingConfig {
    pub kvstore: KVStoreConfig,
    pub min_partition_owners_count: i64,
    pub min_partition_owners_duration: GoDuration,
    pub delete_inactive_partition_after: GoDuration,
    pub create_partitions_with_derived_tokens: bool,
    pub max_derived_token_partitions: i64,
}

impl Default for PartitionRingConfig {
    fn default() -> Self {
        Self {
            kvstore: KVStoreConfig::default(),
            min_partition_owners_count: 1,
            min_partition_owners_duration: GoDuration::from_secs(10),
            delete_inactive_partition_after: GoDuration::from_hours(13),
            create_partitions_with_derived_tokens: false,
            max_derived_token_partitions: 0,
        }
    }
}

impl PartitionRingConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        self.kvstore
            .register_flags_into("ingester.partition-ring.", "collectors/", f, move |t| &get(t).kvstore, move |t| &mut get_mut(t).kvstore);
        f.i64_var("ingester.partition-ring.min-partition-owners-count", self.min_partition_owners_count, "Minimum number of owners to wait before a PENDING partition gets switched to ACTIVE.", move |t| &get(t).min_partition_owners_count, move |t| &mut get_mut(t).min_partition_owners_count);
        f.go_duration_var("ingester.partition-ring.min-partition-owners-duration", self.min_partition_owners_duration, "How long the minimum number of owners are enforced before a PENDING partition gets switched to ACTIVE.", move |t| &get(t).min_partition_owners_duration, move |t| &mut get_mut(t).min_partition_owners_duration);
        f.go_duration_var("ingester.partition-ring.delete-inactive-partition-after", self.delete_inactive_partition_after, "How long to wait before an INACTIVE partition is eligible for deletion.", move |t| &get(t).delete_inactive_partition_after, move |t| &mut get_mut(t).delete_inactive_partition_after);
        f.bool_var("ingester.partition-ring.create-partitions-with-derived-tokens", self.create_partitions_with_derived_tokens, "Create partition with tokens derived from the partition ID.", move |t| &get(t).create_partitions_with_derived_tokens, move |t| &mut get_mut(t).create_partitions_with_derived_tokens);
        f.i64_var("ingester.partition-ring.max-derived-token-partitions", self.max_derived_token_partitions, "Partitions with an ID lower than this value can use derived tokens.", move |t| &get(t).max_derived_token_partitions, move |t| &mut get_mut(t).max_derived_token_partitions);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_derived_token_partitions < 0
            || self.max_derived_token_partitions > i32::MAX as i64
        {
            return Err(ConfigError::Validation(format!(
                "-ingester.partition-ring.max-derived-token-partitions must be between 0 and {}, got {}",
                i32::MAX,
                self.max_derived_token_partitions
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InstanceLimits {
    pub max_ingestion_rate: f64,
    pub max_tenants: i64,
    pub max_series: i64,
    pub max_inflight_push_requests: i64,
    pub max_inflight_push_requests_bytes: i64,
    pub early_head_compaction_min_in_memory_series: i64,
}

impl Default for InstanceLimits {
    fn default() -> Self {
        Self {
            max_ingestion_rate: 0.0,
            max_tenants: 0,
            max_series: 0,
            max_inflight_push_requests: 30000,
            max_inflight_push_requests_bytes: 0,
            early_head_compaction_min_in_memory_series: 0,
        }
    }
}

impl InstanceLimits {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.f64_var("ingester.instance-limits.max-ingestion-rate", self.max_ingestion_rate, "Max ingestion rate (samples/sec) that ingester will accept.", move |t| &get(t).max_ingestion_rate, move |t| &mut get_mut(t).max_ingestion_rate);
        f.i64_var("ingester.instance-limits.max-tenants", self.max_tenants, "Max tenants that this ingester can hold.", move |t| &get(t).max_tenants, move |t| &mut get_mut(t).max_tenants);
        f.i64_var("ingester.instance-limits.max-series", self.max_series, "Max series that this ingester can hold (across all tenants).", move |t| &get(t).max_series, move |t| &mut get_mut(t).max_series);
        f.i64_var("ingester.instance-limits.max-inflight-push-requests", self.max_inflight_push_requests, "Max inflight push requests that this ingester can handle.", move |t| &get(t).max_inflight_push_requests, move |t| &mut get_mut(t).max_inflight_push_requests);
        f.i64_var("ingester.instance-limits.max-inflight-push-requests-bytes", self.max_inflight_push_requests_bytes, "Max total size in bytes of inflight push requests that this ingester can handle.", move |t| &get(t).max_inflight_push_requests_bytes, move |t| &mut get_mut(t).max_inflight_push_requests_bytes);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ActiveSeriesConfig {
    pub active_series_metrics_enabled: bool,
    pub active_series_metrics_update_period: GoDuration,
    pub active_series_metrics_idle_timeout: GoDuration,
}

impl Default for ActiveSeriesConfig {
    fn default() -> Self {
        Self {
            active_series_metrics_enabled: true,
            active_series_metrics_update_period: GoDuration::from_mins(1),
            active_series_metrics_idle_timeout: GoDuration::from_mins(20),
        }
    }
}

impl ActiveSeriesConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.bool_var("ingester.active-series-metrics-enabled", self.active_series_metrics_enabled, "Enable tracking of active series and export them as metrics.", move |t| &get(t).active_series_metrics_enabled, move |t| &mut get_mut(t).active_series_metrics_enabled);
        f.go_duration_var("ingester.active-series-metrics-update-period", self.active_series_metrics_update_period, "How often to update active series metrics.", move |t| &get(t).active_series_metrics_update_period, move |t| &mut get_mut(t).active_series_metrics_update_period);
        f.go_duration_var("ingester.active-series-metrics-idle-timeout", self.active_series_metrics_idle_timeout, "After what time a series is considered to be inactive.", move |t| &get(t).active_series_metrics_idle_timeout, move |t| &mut get_mut(t).active_series_metrics_idle_timeout);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    pub enabled: bool,
    pub failure_threshold_percentage: u64,
    pub failure_execution_threshold: u64,
    pub thresholding_period: GoDuration,
    pub cooldown_period: GoDuration,
    pub initial_delay: GoDuration,
    pub request_timeout: GoDuration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self::with_default_timeout(GoDuration::from_secs(2))
    }
}

impl CircuitBreakerConfig {
    pub fn with_default_timeout(request_timeout: GoDuration) -> Self {
        Self {
            enabled: false,
            failure_threshold_percentage: 10,
            failure_execution_threshold: 100,
            thresholding_period: GoDuration::from_mins(1),
            cooldown_period: GoDuration::from_secs(10),
            initial_delay: GoDuration::ZERO,
            request_timeout,
        }
    }

    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.bool_var(&format!("{prefix}enabled"), self.enabled, "Enable circuit breaking when making requests to ingesters", move |t| &get(t).enabled, move |t| &mut get_mut(t).enabled);
        f.u64_var(&format!("{prefix}failure-threshold-percentage"), self.failure_threshold_percentage, "Max percentage of requests that can fail over period before the circuit breaker opens", move |t| &get(t).failure_threshold_percentage, move |t| &mut get_mut(t).failure_threshold_percentage);
        f.u64_var(&format!("{prefix}failure-execution-threshold"), self.failure_execution_threshold, "How many requests must have been executed in period for the circuit breaker to be eligible to open", move |t| &get(t).failure_execution_threshold, move |t| &mut get_mut(t).failure_execution_threshold);
        f.go_duration_var(&format!("{prefix}thresholding-period"), self.thresholding_period, "Moving window of time that the percentage of failed requests is computed over", move |t| &get(t).thresholding_period, move |t| &mut get_mut(t).thresholding_period);
        f.go_duration_var(&format!("{prefix}cooldown-period"), self.cooldown_period, "How long the circuit breaker will stay in the open state before allowing some requests", move |t| &get(t).cooldown_period, move |t| &mut get_mut(t).cooldown_period);
        f.go_duration_var(&format!("{prefix}initial-delay"), self.initial_delay, "Duration after an initial request that an activated circuit breaker should wait before becoming active.", move |t| &get(t).initial_delay, move |t| &mut get_mut(t).initial_delay);
        f.go_duration_var(&format!("{prefix}request-timeout"), self.request_timeout, "The maximum duration of an ingester's request before it triggers a timeout.", move |t| &get(t).request_timeout, move |t| &mut get_mut(t).request_timeout);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PrioritizerConfig {
    pub calibration_interval: GoDuration,
}

impl Default for PrioritizerConfig {
    fn default() -> Self {
        Self {
            calibration_interval: GoDuration::from_secs(1),
        }
    }
}

impl PrioritizerConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.go_duration_var(&format!("{prefix}calibration-interval"), self.calibration_interval, "How frequently to recalibrate rejection thresholds.", move |t| &get(t).calibration_interval, move |t| &mut get_mut(t).calibration_interval);
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReactiveLimiterConfig {
    pub enabled: bool,
    pub min_limit: u64,
    pub max_limit: u64,
    pub initial_limit: u64,
    pub max_limit_factor: f64,
    pub max_limit_factor_decay: f64,
    pub min_limit_factor: f64,
    pub recent_window_min_duration: GoDuration,
    pub recent_window_max_duration: GoDuration,
    pub recent_window_min_samples: u64,
    pub recent_quantile: f64,
    pub baseline_window_age: u64,
    pub correlation_window: u64,
    pub initial_rejection_factor: f64,
    pub max_rejection_factor: f64,
}

impl Default for ReactiveLimiterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_limit: 2,
            max_limit: 200,
            initial_limit: 20,
            max_limit_factor: 5.0,
            max_limit_factor_decay: 1.0,
            min_limit_factor: 1.2,
            recent_window_min_duration: GoDuration::from_secs(1),
            recent_window_max_duration: GoDuration::from_secs(30),
            recent_window_min_samples: 50,
            recent_quantile: 0.9,
            baseline_window_age: 10,
            correlation_window: 50,
            initial_rejection_factor: 2.0,
            max_rejection_factor: 3.0,
        }
    }
}

impl ReactiveLimiterConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.bool_var(&format!("{prefix}enabled"), self.enabled, "Enable reactive limiting when making requests to a service", move |t| &get(t).enabled, move |t| &mut get_mut(t).enabled);
        f.u64_var(&format!("{prefix}min-limit"), self.min_limit, "Minimum inflight requests limit", move |t| &get(t).min_limit, move |t| &mut get_mut(t).min_limit);
        f.u64_var(&format!("{prefix}max-limit"), self.max_limit, "Maximum inflight requests limit", move |t| &get(t).max_limit, move |t| &mut get_mut(t).max_limit);
        f.u64_var(&format!("{prefix}initial-limit"), self.initial_limit, "Initial inflight requests limit", move |t| &get(t).initial_limit, move |t| &mut get_mut(t).initial_limit);
        f.f64_var(&format!("{prefix}max-limit-factor"), self.max_limit_factor, "The maximum inflight limit as a multiple of current inflight requests", move |t| &get(t).max_limit_factor, move |t| &mut get_mut(t).max_limit_factor);
        f.f64_var(&format!("{prefix}max-limit-factor-decay"), self.max_limit_factor_decay, "Logarithmic decay applied to the maxLimitFactor", move |t| &get(t).max_limit_factor_decay, move |t| &mut get_mut(t).max_limit_factor_decay);
        f.f64_var(&format!("{prefix}min-limit-factor"), self.min_limit_factor, "Minimum limit factor when max-limit-factor-decay is applied", move |t| &get(t).min_limit_factor, move |t| &mut get_mut(t).min_limit_factor);
        f.go_duration_var(&format!("{prefix}recent-window-min-duration"), self.recent_window_min_duration, "Minimum duration of the window used to collect recent response time samples", move |t| &get(t).recent_window_min_duration, move |t| &mut get_mut(t).recent_window_min_duration);
        f.go_duration_var(&format!("{prefix}recent-window-max-duration"), self.recent_window_max_duration, "Maximum duration of the window used to collect recent response time samples", move |t| &get(t).recent_window_max_duration, move |t| &mut get_mut(t).recent_window_max_duration);
        f.u64_var(&format!("{prefix}recent-window-min-samples"), self.recent_window_min_samples, "Minimum number of samples that must be recorded in the recent window", move |t| &get(t).recent_window_min_samples, move |t| &mut get_mut(t).recent_window_min_samples);
        f.f64_var(&format!("{prefix}recent-quantile"), self.recent_quantile, "The quantile of recent recorded response times", move |t| &get(t).recent_quantile, move |t| &mut get_mut(t).recent_quantile);
        f.u64_var(&format!("{prefix}baseline-window-age"), self.baseline_window_age, "The average age of baseline samples", move |t| &get(t).baseline_window_age, move |t| &mut get_mut(t).baseline_window_age);
        f.u64_var(&format!("{prefix}correlation-window"), self.correlation_window, "How many recent limit and inflight time measurements are stored", move |t| &get(t).correlation_window, move |t| &mut get_mut(t).correlation_window);
        f.f64_var(&format!("{prefix}initial-rejection-factor"), self.initial_rejection_factor, "The number of allowed queued requests after which rejections start", move |t| &get(t).initial_rejection_factor, move |t| &mut get_mut(t).initial_rejection_factor);
        f.f64_var(&format!("{prefix}max-rejection-factor"), self.max_rejection_factor, "The number of allowed queued requests after which all requests are rejected", move |t| &get(t).max_rejection_factor, move |t| &mut get_mut(t).max_rejection_factor);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.min_limit_factor < 1.0 {
            return Err(ConfigError::Validation(
                "min_limit_factor must be >= 1".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LabelValuesCountConfig {
    pub chunk_size: i64,
}

impl Default for LabelValuesCountConfig {
    fn default() -> Self {
        Self { chunk_size: 32 }
    }
}

impl LabelValuesCountConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.i64_var("ingester.label-values-count-chunk-size", self.chunk_size, "Number of label values processed per work unit submitted to the ingester compute worker pool.", move |t| &get(t).chunk_size, move |t| &mut get_mut(t).chunk_size);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.chunk_size < 1 {
            return Err(ConfigError::Validation(
                "-ingester.label-values-count-chunk-size must be >= 1".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct IngesterConfig {
    pub ring: RingConfig,
    pub partition_ring: PartitionRingConfig,
    pub metadata_retain_period: GoDuration,
    pub rate_update_period: GoDuration,
    #[serde(flatten)]
    pub active_series: ActiveSeriesConfig,
    pub tsdb_config_update_period: GoDuration,
    pub instance_limits: InstanceLimits,
    pub ignore_series_limit_for_metric_names: String,
    pub read_path_cpu_utilization_limit: f64,
    pub read_path_memory_utilization_limit: u64,
    pub error_sample_rate: i64,
    pub use_ingester_owned_series_for_limits: bool,
    pub track_ingester_owned_series: bool,
    pub owned_series_update_interval: GoDuration,
    pub early_compaction_non_owned_series_enabled: bool,
    pub early_compaction_non_owned_series_min_grace_period: GoDuration,
    pub early_compaction_non_owned_series_max_grace_period: GoDuration,
    pub push_circuit_breaker: CircuitBreakerConfig,
    pub read_circuit_breaker: CircuitBreakerConfig,
    pub rejection_prioritizer: PrioritizerConfig,
    pub push_reactive_limiter: ReactiveLimiterConfig,
    pub read_reactive_limiter: ReactiveLimiterConfig,
    pub compute_workers: i64,
    pub label_values_count: LabelValuesCountConfig,
    pub label_churn_enabled: bool,
    pub push_grpc_method_enabled: bool,
    pub read_compartment_id: i64,
}

impl Default for IngesterConfig {
    fn default() -> Self {
        Self {
            ring: RingConfig::default(),
            partition_ring: PartitionRingConfig::default(),
            metadata_retain_period: GoDuration::from_mins(10),
            rate_update_period: GoDuration::from_secs(15),
            active_series: ActiveSeriesConfig::default(),
            tsdb_config_update_period: GoDuration::from_secs(15),
            instance_limits: InstanceLimits::default(),
            ignore_series_limit_for_metric_names: String::new(),
            read_path_cpu_utilization_limit: 0.0,
            read_path_memory_utilization_limit: 0,
            error_sample_rate: 10,
            use_ingester_owned_series_for_limits: false,
            track_ingester_owned_series: false,
            owned_series_update_interval: GoDuration::from_secs(15),
            early_compaction_non_owned_series_enabled: false,
            early_compaction_non_owned_series_min_grace_period: GoDuration::from_secs(30),
            early_compaction_non_owned_series_max_grace_period: GoDuration::from_mins(5),
            push_circuit_breaker: CircuitBreakerConfig::with_default_timeout(GoDuration::from_secs(2)),
            read_circuit_breaker: CircuitBreakerConfig::with_default_timeout(GoDuration::from_secs(30)),
            rejection_prioritizer: PrioritizerConfig::default(),
            push_reactive_limiter: ReactiveLimiterConfig::default(),
            read_reactive_limiter: ReactiveLimiterConfig::default(),
            compute_workers: 0,
            label_values_count: LabelValuesCountConfig::default(),
            label_churn_enabled: false,
            push_grpc_method_enabled: true,
            read_compartment_id: 0,
        }
    }
}

impl IngesterConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        self.ring.register_flags_into(f, move |t| &get(t).ring, move |t| &mut get_mut(t).ring);
        self.partition_ring.register_flags_into(f, move |t| &get(t).partition_ring, move |t| &mut get_mut(t).partition_ring);
        self.instance_limits.register_flags_into(f, move |t| &get(t).instance_limits, move |t| &mut get_mut(t).instance_limits);
        self.active_series.register_flags_into(f, move |t| &get(t).active_series, move |t| &mut get_mut(t).active_series);
        self.push_circuit_breaker
            .register_flags_into("ingester.push-circuit-breaker.", f, move |t| &get(t).push_circuit_breaker, move |t| &mut get_mut(t).push_circuit_breaker);
        self.read_circuit_breaker
            .register_flags_into("ingester.read-circuit-breaker.", f, move |t| &get(t).read_circuit_breaker, move |t| &mut get_mut(t).read_circuit_breaker);
        self.rejection_prioritizer
            .register_flags_into("ingester.rejection-prioritizer.", f, move |t| &get(t).rejection_prioritizer, move |t| &mut get_mut(t).rejection_prioritizer);
        self.push_reactive_limiter
            .register_flags_into("ingester.push-reactive-limiter.", f, move |t| &get(t).push_reactive_limiter, move |t| &mut get_mut(t).push_reactive_limiter);
        self.read_reactive_limiter
            .register_flags_into("ingester.read-reactive-limiter.", f, move |t| &get(t).read_reactive_limiter, move |t| &mut get_mut(t).read_reactive_limiter);

        f.go_duration_var("ingester.metadata-retain-period", self.metadata_retain_period, "Period at which metadata we have not seen will remain in memory before being deleted.", move |t| &get(t).metadata_retain_period, move |t| &mut get_mut(t).metadata_retain_period);
        f.go_duration_var("ingester.rate-update-period", self.rate_update_period, "Period with which to update the per-tenant ingestion rates.", move |t| &get(t).rate_update_period, move |t| &mut get_mut(t).rate_update_period);
        f.go_duration_var("ingester.tsdb-config-update-period", self.tsdb_config_update_period, "Period with which to update the per-tenant TSDB configuration.", move |t| &get(t).tsdb_config_update_period, move |t| &mut get_mut(t).tsdb_config_update_period);
        f.string_var("ingester.ignore-series-limit-for-metric-names", &self.ignore_series_limit_for_metric_names, "Comma-separated list of metric names for which the per-metric series limit will be ignored.", move |t| &get(t).ignore_series_limit_for_metric_names, move |t| &mut get_mut(t).ignore_series_limit_for_metric_names);
        f.f64_var("ingester.read-path-cpu-utilization-limit", self.read_path_cpu_utilization_limit, "CPU utilization limit, as CPU cores, for CPU/memory utilization based read request limiting.", move |t| &get(t).read_path_cpu_utilization_limit, move |t| &mut get_mut(t).read_path_cpu_utilization_limit);
        f.u64_var("ingester.read-path-memory-utilization-limit", self.read_path_memory_utilization_limit, "Memory limit, in bytes, for CPU/memory utilization based read request limiting.", move |t| &get(t).read_path_memory_utilization_limit, move |t| &mut get_mut(t).read_path_memory_utilization_limit);
        f.i64_var("ingester.error-sample-rate", self.error_sample_rate, "Each error will be logged once in this many times. Use 0 to log all of them.", move |t| &get(t).error_sample_rate, move |t| &mut get_mut(t).error_sample_rate);
        f.bool_var("ingester.use-ingester-owned-series-for-limits", self.use_ingester_owned_series_for_limits, "When enabled, only series currently owned by ingester according to the ring are used when checking limits.", move |t| &get(t).use_ingester_owned_series_for_limits, move |t| &mut get_mut(t).use_ingester_owned_series_for_limits);
        f.bool_var("ingester.track-ingester-owned-series", self.track_ingester_owned_series, "This option enables tracking of ingester-owned series based on ring state.", move |t| &get(t).track_ingester_owned_series, move |t| &mut get_mut(t).track_ingester_owned_series);
        f.go_duration_var("ingester.owned-series-update-interval", self.owned_series_update_interval, "How often to check for ring changes and possibly recompute owned series.", move |t| &get(t).owned_series_update_interval, move |t| &mut get_mut(t).owned_series_update_interval);
        f.bool_var("ingester.early-compaction-non-owned-series-enabled", self.early_compaction_non_owned_series_enabled, "When enabled, the ingester triggers an early TSDB head compaction for non-owned series.", move |t| &get(t).early_compaction_non_owned_series_enabled, move |t| &mut get_mut(t).early_compaction_non_owned_series_enabled);
        f.go_duration_var("ingester.early-compaction-non-owned-series-min-grace-period", self.early_compaction_non_owned_series_min_grace_period, "Minimum time a series must remain non-owned before it can be evicted.", move |t| &get(t).early_compaction_non_owned_series_min_grace_period, move |t| &mut get_mut(t).early_compaction_non_owned_series_min_grace_period);
        f.go_duration_var("ingester.early-compaction-non-owned-series-max-grace-period", self.early_compaction_non_owned_series_max_grace_period, "Maximum time a series may remain non-owned before it is evicted.", move |t| &get(t).early_compaction_non_owned_series_max_grace_period, move |t| &mut get_mut(t).early_compaction_non_owned_series_max_grace_period);
        f.i64_var("ingester.compute-workers", self.compute_workers, "Number of worker goroutines in the ingester's shared tenant-fair compute worker pool.", move |t| &get(t).compute_workers, move |t| &mut get_mut(t).compute_workers);
        self.label_values_count.register_flags_into(f, move |t| &get(t).label_values_count, move |t| &mut get_mut(t).label_values_count);
        f.bool_var("ingester.label-churn-enabled", self.label_churn_enabled, "Enable label churn detection.", move |t| &get(t).label_churn_enabled, move |t| &mut get_mut(t).label_churn_enabled);
        f.bool_var("ingester.push-grpc-method-enabled", self.push_grpc_method_enabled, "Enables Push gRPC method on ingester.", move |t| &get(t).push_grpc_method_enabled, move |t| &mut get_mut(t).push_grpc_method_enabled);
        f.i64_var("ingester.read-compartment-id", self.read_compartment_id, "The read compartment this ingester serves.", move |t| &get(t).read_compartment_id, move |t| &mut get_mut(t).read_compartment_id);
    }

    pub fn validate(
        &self,
        compartments_enabled: bool,
        num_read_compartments: i64,
    ) -> Result<(), ConfigError> {
        if self.error_sample_rate < 0 {
            return Err(ConfigError::Validation(
                "error sample rate cannot be a negative number".to_string(),
            ));
        }

        if compartments_enabled {
            if self.read_compartment_id < 0 || self.read_compartment_id >= num_read_compartments {
                return Err(ConfigError::Validation(format!(
                    "ingester read compartment ID {} is out of range [0, {})",
                    self.read_compartment_id, num_read_compartments
                )));
            }
        } else if self.read_compartment_id != 0 {
            return Err(ConfigError::Validation(
                "ingester read compartment ID must be 0 when compartments are disabled".to_string(),
            ));
        }

        if self.ring.num_tokens == 0 && self.push_grpc_method_enabled {
            return Err(ConfigError::Validation(
                "ring tokens can only be disabled when gRPC push is disabled".to_string(),
            ));
        }

        if self.early_compaction_non_owned_series_enabled
            && !self.use_ingester_owned_series_for_limits
            && !self.track_ingester_owned_series
        {
            return Err(ConfigError::Validation(
                "early compaction of non-owned series requires -ingester.track-ingester-owned-series or -ingester.use-ingester-owned-series-for-limits to be enabled".to_string(),
            ));
        }

        if self.early_compaction_non_owned_series_enabled
            && self.early_compaction_non_owned_series_max_grace_period.0 > 0
            && self.early_compaction_non_owned_series_max_grace_period
                <= self.early_compaction_non_owned_series_min_grace_period
        {
            return Err(ConfigError::Validation(
                "-ingester.early-compaction-non-owned-series-max-grace-period must be greater than -ingester.early-compaction-non-owned-series-min-grace-period when set to a non-zero value".to_string(),
            ));
        }

        if self.compute_workers < 0 {
            return Err(ConfigError::Validation(
                "-ingester.compute-workers must be >= 0".to_string(),
            ));
        }

        self.label_values_count.validate()?;
        self.partition_ring.validate()?;
        self.push_reactive_limiter.validate()?;
        self.read_reactive_limiter.validate()?;
        self.ring.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackoffConfig {
    pub min_period: GoDuration,
    pub max_period: GoDuration,
    pub max_retries: i64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            min_period: GoDuration::from_millis(100),
            max_period: GoDuration::from_secs(10),
            max_retries: 10,
        }
    }
}

impl BackoffConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.go_duration_var(&format!("{prefix}.backoff-min-period"), self.min_period, "Minimum delay when backing off.", move |t| &get(t).min_period, move |t| &mut get_mut(t).min_period);
        f.go_duration_var(&format!("{prefix}.backoff-max-period"), self.max_period, "Maximum delay when backing off.", move |t| &get(t).max_period, move |t| &mut get_mut(t).max_period);
        f.i64_var(&format!("{prefix}.backoff-retries"), self.max_retries, "Number of times to backoff and retry before failing.", move |t| &get(t).max_retries, move |t| &mut get_mut(t).max_retries);
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GRPCClientConfig {
    pub max_recv_msg_size: i64,
    pub max_send_msg_size: i64,
    pub grpc_compression: String,
    pub rate_limit: f64,
    pub rate_limit_burst: i64,
    pub backoff_on_ratelimits: bool,
    pub backoff_config: BackoffConfig,
    pub initial_stream_window_size: HumanBytes,
    pub initial_connection_window_size: HumanBytes,
    pub tls_enabled: bool,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub tls_ca_path: String,
    pub tls_server_name: String,
    pub tls_insecure_skip_verify: bool,
    pub tls_cipher_suites: String,
    pub tls_min_version: String,
    pub connect_timeout: GoDuration,
    pub connect_backoff_base_delay: GoDuration,
    pub connect_backoff_max_delay: GoDuration,
    pub keepalive_time: GoDuration,
    pub keepalive_timeout: GoDuration,
    pub cluster_validation: ClusterValidationConfig,
}

impl Default for GRPCClientConfig {
    fn default() -> Self {
        Self {
            max_recv_msg_size: 100 * 1024 * 1024,
            max_send_msg_size: 100 * 1024 * 1024,
            grpc_compression: String::new(),
            rate_limit: 0.0,
            rate_limit_burst: 0,
            backoff_on_ratelimits: false,
            backoff_config: BackoffConfig::default(),
            initial_stream_window_size: HumanBytes(65535),
            initial_connection_window_size: HumanBytes(65535),
            tls_enabled: false,
            tls_cert_path: String::new(),
            tls_key_path: String::new(),
            tls_ca_path: String::new(),
            tls_server_name: String::new(),
            tls_insecure_skip_verify: false,
            tls_cipher_suites: String::new(),
            tls_min_version: String::new(),
            connect_timeout: GoDuration::from_secs(5),
            connect_backoff_base_delay: GoDuration::from_secs(1),
            connect_backoff_max_delay: GoDuration::from_secs(5),
            keepalive_time: GoDuration::from_secs(20),
            keepalive_timeout: GoDuration::from_secs(10),
            cluster_validation: ClusterValidationConfig::default(),
        }
    }
}

impl GRPCClientConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.i64_var(&format!("{prefix}.grpc-max-recv-msg-size"), self.max_recv_msg_size, "gRPC client max receive message size (bytes).", move |t| &get(t).max_recv_msg_size, move |t| &mut get_mut(t).max_recv_msg_size);
        f.i64_var(&format!("{prefix}.grpc-max-send-msg-size"), self.max_send_msg_size, "gRPC client max send message size (bytes).", move |t| &get(t).max_send_msg_size, move |t| &mut get_mut(t).max_send_msg_size);
        f.string_var(&format!("{prefix}.grpc-compression"), &self.grpc_compression, "Use compression when sending messages. Supported values are: 'gzip', 'snappy', 's2' and '' (disable compression)", move |t| &get(t).grpc_compression, move |t| &mut get_mut(t).grpc_compression);
        f.f64_var(&format!("{prefix}.grpc-client-rate-limit"), self.rate_limit, "Rate limit for gRPC client; 0 means disabled.", move |t| &get(t).rate_limit, move |t| &mut get_mut(t).rate_limit);
        f.i64_var(&format!("{prefix}.grpc-client-rate-limit-burst"), self.rate_limit_burst, "Rate limit burst for gRPC client.", move |t| &get(t).rate_limit_burst, move |t| &mut get_mut(t).rate_limit_burst);
        f.bool_var(&format!("{prefix}.backoff-on-ratelimits"), self.backoff_on_ratelimits, "Enable backoff and retry when we hit rate limits.", move |t| &get(t).backoff_on_ratelimits, move |t| &mut get_mut(t).backoff_on_ratelimits);
        f.human_bytes_var(&format!("{prefix}.initial-stream-window-size"), self.initial_stream_window_size, "Initial stream window size.", move |t| &get(t).initial_stream_window_size, move |t| &mut get_mut(t).initial_stream_window_size);
        f.human_bytes_var(&format!("{prefix}.initial-connection-window-size"), self.initial_connection_window_size, "Initial connection window size.", move |t| &get(t).initial_connection_window_size, move |t| &mut get_mut(t).initial_connection_window_size);
        f.bool_var(&format!("{prefix}.tls-enabled"), self.tls_enabled, "Enable TLS in the gRPC client.", move |t| &get(t).tls_enabled, move |t| &mut get_mut(t).tls_enabled);
        f.string_var(&format!("{prefix}.tls-cert-path"), &self.tls_cert_path, "Path to the client certificate.", move |t| &get(t).tls_cert_path, move |t| &mut get_mut(t).tls_cert_path);
        f.string_var(&format!("{prefix}.tls-key-path"), &self.tls_key_path, "Path to the client key.", move |t| &get(t).tls_key_path, move |t| &mut get_mut(t).tls_key_path);
        f.string_var(&format!("{prefix}.tls-ca-path"), &self.tls_ca_path, "Path to the CA certificate.", move |t| &get(t).tls_ca_path, move |t| &mut get_mut(t).tls_ca_path);
        f.string_var(&format!("{prefix}.tls-server-name"), &self.tls_server_name, "Override the expected server name.", move |t| &get(t).tls_server_name, move |t| &mut get_mut(t).tls_server_name);
        f.bool_var(&format!("{prefix}.tls-insecure-skip-verify"), self.tls_insecure_skip_verify, "Skip TLS certificate verification.", move |t| &get(t).tls_insecure_skip_verify, move |t| &mut get_mut(t).tls_insecure_skip_verify);
        f.string_var(&format!("{prefix}.tls-cipher-suites"), &self.tls_cipher_suites, "TLS cipher suites.", move |t| &get(t).tls_cipher_suites, move |t| &mut get_mut(t).tls_cipher_suites);
        f.string_var(&format!("{prefix}.tls-min-version"), &self.tls_min_version, "Minimum TLS version.", move |t| &get(t).tls_min_version, move |t| &mut get_mut(t).tls_min_version);
        f.go_duration_var(&format!("{prefix}.connect-timeout"), self.connect_timeout, "The maximum amount of time to establish a connection.", move |t| &get(t).connect_timeout, move |t| &mut get_mut(t).connect_timeout);
        f.go_duration_var(&format!("{prefix}.connect-backoff-base-delay"), self.connect_backoff_base_delay, "Initial backoff delay after first connection failure.", move |t| &get(t).connect_backoff_base_delay, move |t| &mut get_mut(t).connect_backoff_base_delay);
        f.go_duration_var(&format!("{prefix}.connect-backoff-max-delay"), self.connect_backoff_max_delay, "Maximum backoff delay when establishing a connection.", move |t| &get(t).connect_backoff_max_delay, move |t| &mut get_mut(t).connect_backoff_max_delay);
        f.go_duration_var(&format!("{prefix}.keepalive-time"), self.keepalive_time, "Keepalive ping period.", move |t| &get(t).keepalive_time, move |t| &mut get_mut(t).keepalive_time);
        f.go_duration_var(&format!("{prefix}.keepalive-timeout"), self.keepalive_timeout, "Keepalive ping timeout.", move |t| &get(t).keepalive_timeout, move |t| &mut get_mut(t).keepalive_timeout);
        self.backoff_config.register_flags_into(prefix, f, move |t| &get(t).backoff_config, move |t| &mut get_mut(t).backoff_config);
        self.cluster_validation
            .register_flags_into(&format!("{prefix}.cluster-validation."), f, move |t| &get(t).cluster_validation, move |t| &mut get_mut(t).cluster_validation);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !matches!(self.grpc_compression.as_str(), "" | "gzip" | "snappy" | "s2") {
            return Err(ConfigError::Validation(format!(
                "unsupported compression type: {:?}",
                self.grpc_compression
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngesterClientConfig {
    pub grpc_client_config: GRPCClientConfig,
}

impl IngesterClientConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        self.grpc_client_config
            .register_flags_into("ingester.client", f, move |t| &get(t).grpc_client_config, move |t| &mut get_mut(t).grpc_client_config);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        self.grpc_client_config.validate()
    }
}
