//! Kafka client option validation and inspection matching franz-go `kgo.Config`.
//!
//! Ported from github.com/twmb/franz-go/pkg/kgo/config.go

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetSpec {
    pub at: i64,
    pub relative: i64,
}

impl OffsetSpec {
    pub fn at_start() -> Self {
        Self { at: -2, relative: 0 }
    }
    pub fn at_end() -> Self {
        Self { at: -1, relative: 0 }
    }
    pub fn at(offset: i64) -> Self {
        Self { at: offset, relative: 0 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaxVersions(pub BTreeMap<i16, i16>);

impl MaxVersions {
    pub fn has_key(&self, key: i16) -> bool {
        self.0.contains_key(&key)
    }
    pub fn lookup_max_key_version(&self, key: i16) -> Option<i16> {
        self.0.get(&key).copied()
    }
}

#[derive(Debug, Clone)]
pub struct KgoConfig {
    pub seed_brokers: Vec<String>,
    pub consumer_group: String,
    pub share_group: String,
    pub share_max_records: Option<i32>,
    pub share_max_records_strict: bool,
    pub has_share_ack_callback: bool,
    pub instance_id: Option<String>,
    pub transactional_id: Option<String>,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub produce_request_timeout: Duration,
    pub transaction_timeout: Duration,
    pub consume_regex: bool,
    pub consume_topics: Vec<String>,
    pub consume_exclude_topics: Vec<String>,
    pub consume_partitions: HashMap<String, HashMap<i32, OffsetSpec>>,
    pub max_buffered_records: i64,
    pub manual_flushing: bool,
    pub max_versions: Option<MaxVersions>,
}

impl Default for KgoConfig {
    fn default() -> Self {
        Self {
            seed_brokers: Vec::new(),
            consumer_group: String::new(),
            share_group: String::new(),
            share_max_records: None,
            share_max_records_strict: false,
            has_share_ack_callback: false,
            instance_id: None,
            transactional_id: None,
            session_timeout: Duration::from_secs(45),
            rebalance_timeout: Duration::from_secs(60),
            heartbeat_interval: Duration::from_secs(3),
            produce_request_timeout: Duration::from_secs(10),
            transaction_timeout: Duration::from_secs(60),
            consume_regex: false,
            consume_topics: Vec::new(),
            consume_exclude_topics: Vec::new(),
            consume_partitions: HashMap::new(),
            max_buffered_records: 10_000,
            manual_flushing: false,
            max_versions: None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum KgoOpt {
    SeedBrokers(Vec<String>),
    ConsumerGroup(String),
    ShareGroup(String),
    ShareMaxRecords(i32),
    ShareMaxRecordsStrict,
    ShareAckCallback,
    InstanceID(String),
    TransactionalID(String),
    SessionTimeout(Duration),
    RebalanceTimeout(Duration),
    HeartbeatInterval(Duration),
    ProduceRequestTimeout(Duration),
    TransactionTimeout(Duration),
    ConsumeRegex,
    ConsumeTopics(Vec<String>),
    ConsumeExcludeTopics(Vec<String>),
    ConsumePartitions(HashMap<String, HashMap<i32, OffsetSpec>>),
    MaxBufferedRecords(i64),
    ManualFlushing,
    MaxVersions(MaxVersions),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KgoOptKey {
    SeedBrokers,
    ConsumerGroup,
    ShareGroup,
    ShareMaxRecords,
    ShareMaxRecordsStrict,
    ShareAckCallback,
    InstanceID,
    TransactionalID,
    SessionTimeout,
    RebalanceTimeout,
    HeartbeatInterval,
    ProduceRequestTimeout,
    TransactionTimeout,
    ConsumeRegex,
    ConsumeTopics,
    ConsumeExcludeTopics,
    ConsumePartitions,
    MaxBufferedRecords,
    ManualFlushing,
    MaxVersions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptValue {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Str(String),
    StrList(Vec<String>),
    Duration(Duration),
}

impl KgoConfig {
    pub fn apply(opts: &[KgoOpt]) -> Result<Self, String> {
        let mut cfg = Self::default();
        for o in opts {
            match o {
                KgoOpt::SeedBrokers(v) => cfg.seed_brokers.extend(v.iter().cloned()),
                KgoOpt::ConsumerGroup(v) => cfg.consumer_group = v.clone(),
                KgoOpt::ShareGroup(v) => cfg.share_group = v.clone(),
                KgoOpt::ShareMaxRecords(v) => cfg.share_max_records = Some(*v),
                KgoOpt::ShareMaxRecordsStrict => cfg.share_max_records_strict = true,
                KgoOpt::ShareAckCallback => cfg.has_share_ack_callback = true,
                KgoOpt::InstanceID(v) => cfg.instance_id = Some(v.clone()),
                KgoOpt::TransactionalID(v) => cfg.transactional_id = Some(v.clone()),
                KgoOpt::SessionTimeout(v) => cfg.session_timeout = *v,
                KgoOpt::RebalanceTimeout(v) => cfg.rebalance_timeout = *v,
                KgoOpt::HeartbeatInterval(v) => cfg.heartbeat_interval = *v,
                KgoOpt::ProduceRequestTimeout(v) => cfg.produce_request_timeout = *v,
                KgoOpt::TransactionTimeout(v) => cfg.transaction_timeout = *v,
                KgoOpt::ConsumeRegex => cfg.consume_regex = true,
                KgoOpt::ConsumeTopics(v) => cfg.consume_topics.extend(v.iter().cloned()),
                KgoOpt::ConsumeExcludeTopics(v) => cfg.consume_exclude_topics.extend(v.iter().cloned()),
                KgoOpt::ConsumePartitions(v) => {
                    for (t, ps) in v {
                        cfg.consume_partitions
                            .entry(t.clone())
                            .or_default()
                            .extend(ps.iter().map(|(&k, &val)| (k, val)));
                    }
                }
                KgoOpt::MaxBufferedRecords(v) => cfg.max_buffered_records = *v,
                KgoOpt::ManualFlushing => cfg.manual_flushing = true,
                KgoOpt::MaxVersions(v) => cfg.max_versions = Some(v.clone()),
            }
        }
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), String> {
        let max_wire_timeout = Duration::from_millis(i32::MAX as u64);

        if self.session_timeout < Duration::from_millis(100) {
            return Err(format!(
                "session timeout {:?} is less than allowed 100ms",
                self.session_timeout
            ));
        }
        if self.session_timeout > max_wire_timeout {
            return Err(format!(
                "session timeout {:?} is larger than allowed {:?}",
                self.session_timeout, max_wire_timeout
            ));
        }
        if self.rebalance_timeout < Duration::from_millis(100) {
            return Err(format!(
                "rebalance timeout {:?} is less than allowed 100ms",
                self.rebalance_timeout
            ));
        }
        if self.rebalance_timeout > max_wire_timeout {
            return Err(format!(
                "rebalance timeout {:?} is larger than allowed {:?}",
                self.rebalance_timeout, max_wire_timeout
            ));
        }
        if self.heartbeat_interval > self.session_timeout {
            return Err(format!(
                "heartbeat interval {:?} is erroneously larger than the session timeout {:?}",
                self.heartbeat_interval, self.session_timeout
            ));
        }
        if self.produce_request_timeout > max_wire_timeout {
            return Err(format!(
                "produce request timeout {:?} is larger than allowed {:?}",
                self.produce_request_timeout, max_wire_timeout
            ));
        }
        if self.transaction_timeout > max_wire_timeout {
            return Err(format!(
                "transaction timeout {:?} is larger than allowed {:?}",
                self.transaction_timeout, max_wire_timeout
            ));
        }
        if !self.consume_regex {
            for t in &self.consume_topics {
                if t.is_empty() {
                    return Err("invalid empty topic name in ConsumeTopics".to_string());
                }
            }
        }
        for (topic, parts) in &self.consume_partitions {
            if topic.is_empty() {
                return Err("invalid empty topic name in ConsumePartitions".to_string());
            }
            for &p in parts.keys() {
                if p < 0 {
                    return Err(format!(
                        "invalid negative partition {p} for topic {topic:?} in ConsumePartitions"
                    ));
                }
            }
        }
        if let Some(ref tx) = self.transactional_id {
            if tx.is_empty() {
                return Err("invalid empty TransactionalID".to_string());
            }
        }
        if let Some(ref inst) = self.instance_id {
            if inst.is_empty() {
                return Err("invalid empty InstanceID".to_string());
            }
        }
        Ok(())
    }

    /// Ported from `Client.OptValue`.
    pub fn opt_value(&self, key: KgoOptKey) -> Option<OptValue> {
        self.opt_values(key).into_iter().next()
    }

    /// Ported from `Client.OptValues`.
    pub fn opt_values(&self, key: KgoOptKey) -> Vec<OptValue> {
        match key {
            KgoOptKey::TransactionalID => match &self.transactional_id {
                Some(s) => vec![OptValue::Str(s.clone()), OptValue::Bool(true)],
                None => Vec::new(),
            },
            KgoOptKey::InstanceID => match &self.instance_id {
                Some(s) => vec![OptValue::Str(s.clone())],
                None => Vec::new(),
            },
            KgoOptKey::ConsumerGroup => vec![OptValue::Str(self.consumer_group.clone())],
            KgoOptKey::ShareGroup => vec![OptValue::Str(self.share_group.clone())],
            KgoOptKey::ShareMaxRecords => self
                .share_max_records
                .map(|v| vec![OptValue::Int32(v)])
                .unwrap_or_default(),
            KgoOptKey::ShareMaxRecordsStrict => vec![OptValue::Bool(self.share_max_records_strict)],
            KgoOptKey::ShareAckCallback => {
                vec![OptValue::Bool(self.has_share_ack_callback)]
            }
            KgoOptKey::SeedBrokers => vec![OptValue::StrList(self.seed_brokers.clone())],
            KgoOptKey::SessionTimeout => vec![OptValue::Duration(self.session_timeout)],
            KgoOptKey::RebalanceTimeout => vec![OptValue::Duration(self.rebalance_timeout)],
            KgoOptKey::HeartbeatInterval => vec![OptValue::Duration(self.heartbeat_interval)],
            KgoOptKey::ProduceRequestTimeout => {
                vec![OptValue::Duration(self.produce_request_timeout)]
            }
            KgoOptKey::TransactionTimeout => vec![OptValue::Duration(self.transaction_timeout)],
            KgoOptKey::ConsumeRegex => vec![OptValue::Bool(self.consume_regex)],
            KgoOptKey::ConsumeTopics => vec![OptValue::StrList(self.consume_topics.clone())],
            KgoOptKey::ConsumeExcludeTopics => {
                vec![OptValue::StrList(self.consume_exclude_topics.clone())]
            }
            KgoOptKey::ConsumePartitions => Vec::new(),
            KgoOptKey::MaxBufferedRecords => vec![OptValue::Int64(self.max_buffered_records)],
            KgoOptKey::ManualFlushing => vec![OptValue::Bool(self.manual_flushing)],
            KgoOptKey::MaxVersions => Vec::new(),
        }
    }
}
