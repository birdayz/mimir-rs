//! Classic hash ring model, CAS/gossip merge, conflict resolution, and token counting.
//!
//! Ported from github.com/grafana/dskit/ring/model.go

use std::any::Any;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};

use crate::kv::memberlist::gofmt::{
    go_sharp_v_str, go_sharp_v_u32_slice, go_sharp_v_u64_map, GoJson,
};
use crate::kv::memberlist::Mergeable;
use crate::kv::{Codec, ProtoSnappyCodec};
use crate::token_gen::token_distance;

/// Maximum clock skew ahead of local time accepted by `Merge`.
pub const MAX_FUTURE_TIMESTAMP_SKEW: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompareResult {
    Equal,
    EqualButStatesAndTimestamps,
    Different,
}

#[derive(Clone, Debug)]
pub(crate) struct InstanceInfo {
    pub instance_id: String,
    pub zone: String,
}

/// Returns the `Codec` used by classic hash rings (`codec_id = "ringDesc"`).
pub fn ring_desc_codec() -> Arc<dyn Codec> {
    Arc::new(
        ProtoSnappyCodec::<Desc>::new("ringDesc").with_mergeable(
            |d| Box::new(d),
            |m| m.as_any().downcast_ref::<Desc>(),
        ),
    )
}

pub fn future_timestamp_limit(now: SystemTime) -> i64 {
    (now + MAX_FUTURE_TIMESTAMP_SKEW)
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(i64::MAX)
}

pub fn local_merge_timestamp(ts: i64, limit: i64) -> i64 {
    if ts > limit {
        -1
    } else {
        ts
    }
}

pub fn system_time_to_unix_secs(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub trait InstanceDescExt {
    fn instance_state(&self) -> InstanceState;
    fn try_instance_state(&self) -> Option<InstanceState>;
    fn state_display(&self) -> Cow<'static, str>;
    fn is_heartbeat_healthy(&self, heartbeat_timeout: Duration, now: SystemTime) -> bool;
    fn is_healthy(
        &self,
        op: super::replication_strategy::Operation,
        heartbeat_timeout: Duration,
        now: SystemTime,
    ) -> bool;
    fn is_ready(&self, now: SystemTime, heartbeat_timeout: Duration) -> Result<(), String>;
    fn get_registered_at(&self) -> Option<SystemTime>;
    fn get_read_only_state(&self) -> (bool, Option<SystemTime>);
}

impl InstanceDescExt for InstanceDesc {
    fn instance_state(&self) -> InstanceState {
        InstanceState::try_from(self.state).unwrap_or(InstanceState::Active)
    }

    fn try_instance_state(&self) -> Option<InstanceState> {
        InstanceState::try_from(self.state).ok()
    }

    fn state_display(&self) -> Cow<'static, str> {
        match InstanceState::try_from(self.state) {
            Ok(s) => Cow::Borrowed(s.as_str_name()),
            Err(_) => Cow::Owned(self.state.to_string()),
        }
    }

    fn is_heartbeat_healthy(&self, heartbeat_timeout: Duration, now: SystemTime) -> bool {
        let now_secs = system_time_to_unix_secs(now);
        // Match Go: now.Sub(time.Unix(i.Timestamp, 0)) <= heartbeatTimeout
        let ts_time = if self.timestamp >= 0 {
            UNIX_EPOCH + Duration::from_secs(self.timestamp as u64)
        } else {
            return false;
        };
        let _ = now_secs;
        match now.duration_since(ts_time) {
            Ok(elapsed) => elapsed <= heartbeat_timeout,
            Err(_) => true, // timestamp in the future -> elapsed <= 0 <= heartbeat_timeout
        }
    }

    fn is_healthy(
        &self,
        op: super::replication_strategy::Operation,
        heartbeat_timeout: Duration,
        now: SystemTime,
    ) -> bool {
        op.is_raw_state_healthy(self.state)
            && self.is_heartbeat_healthy(heartbeat_timeout, now)
    }

    fn is_ready(&self, now: SystemTime, heartbeat_timeout: Duration) -> Result<(), String> {
        if !self.is_heartbeat_healthy(heartbeat_timeout, now) {
            return Err(format!("instance {} past heartbeat timeout", self.addr));
        }
        if self.try_instance_state() != Some(InstanceState::Active) {
            return Err(format!(
                "instance {} in state {}",
                self.addr,
                self.state_display()
            ));
        }
        Ok(())
    }

    fn get_registered_at(&self) -> Option<SystemTime> {
        if self.registered_timestamp <= 0 {
            None
        } else {
            Some(UNIX_EPOCH + Duration::from_secs(self.registered_timestamp as u64))
        }
    }

    fn get_read_only_state(&self) -> (bool, Option<SystemTime>) {
        let ts = if self.read_only_updated_timestamp > 0 {
            Some(UNIX_EPOCH + Duration::from_secs(self.read_only_updated_timestamp as u64))
        } else {
            None
        };
        (self.read_only, ts)
    }
}

pub trait DescExt {
    #[allow(clippy::too_many_arguments)]
    fn add_ingester(
        &mut self,
        id: &str,
        addr: &str,
        zone: &str,
        tokens: Vec<u32>,
        state: InstanceState,
        registered_at: Option<SystemTime>,
        read_only: bool,
        read_only_updated: Option<SystemTime>,
        versions: HashMap<u64, u64>,
    ) -> InstanceDesc;
    fn remove_ingester(&mut self, id: &str);
    fn claim_tokens(&mut self, from: &str, to: &str) -> Vec<u32>;
    fn is_ready(&self, now: SystemTime, heartbeat_timeout: Duration) -> Result<(), String>;
    fn tokens_for(&self, id: &str) -> (Vec<u32>, Vec<u32>);
    fn get_tokens(&self) -> Vec<u32>;
    fn get_tokens_by_zone(&self) -> HashMap<String, Vec<u32>>;
    fn count_tokens(&self) -> HashMap<String, i64>;
    fn ring_compare(&self, other: &Desc) -> CompareResult;
    fn set_instance_ids(&mut self);
    fn merge_with_time(&mut self, other: &mut Desc, local_cas: bool, now: SystemTime)
        -> Option<Desc>;
}

impl DescExt for Desc {
    fn add_ingester(
        &mut self,
        id: &str,
        addr: &str,
        zone: &str,
        tokens: Vec<u32>,
        state: InstanceState,
        registered_at: Option<SystemTime>,
        read_only: bool,
        read_only_updated: Option<SystemTime>,
        versions: HashMap<u64, u64>,
    ) -> InstanceDesc {
        let ingester = InstanceDesc {
            id: id.to_string(),
            addr: addr.to_string(),
            timestamp: system_time_to_unix_secs(SystemTime::now()),
            state: state as i32,
            tokens,
            zone: zone.to_string(),
            registered_timestamp: registered_at.map(system_time_to_unix_secs).unwrap_or(0),
            read_only,
            read_only_updated_timestamp: read_only_updated
                .map(system_time_to_unix_secs)
                .unwrap_or(0),
            versions,
        };
        self.ingesters.insert(id.to_string(), ingester.clone());
        ingester
    }

    fn remove_ingester(&mut self, id: &str) {
        self.ingesters.remove(id);
    }

    fn claim_tokens(&mut self, from: &str, to: &str) -> Vec<u32> {
        let mut result = Vec::new();
        if let Some(from_desc) = self.ingesters.get_mut(from) {
            result = std::mem::take(&mut from_desc.tokens);
        }
        let entry = self.ingesters.entry(to.to_string()).or_default();
        entry.tokens = result.clone();
        result
    }

    fn is_ready(&self, now: SystemTime, heartbeat_timeout: Duration) -> Result<(), String> {
        let mut num_tokens = 0usize;
        for instance in self.ingesters.values() {
            instance.is_ready(now, heartbeat_timeout)?;
            num_tokens += instance.tokens.len();
        }
        if num_tokens == 0 {
            return Err("no tokens in ring".to_string());
        }
        Ok(())
    }

    fn tokens_for(&self, id: &str) -> (Vec<u32>, Vec<u32>) {
        let all_tokens = self.get_tokens();
        let my_tokens = self
            .ingesters
            .get(id)
            .map(|i| i.tokens.clone())
            .unwrap_or_default();
        (my_tokens, all_tokens)
    }

    fn get_tokens(&self) -> Vec<u32> {
        let mut instances: Vec<Vec<u32>> = Vec::with_capacity(self.ingesters.len());
        for instance in self.ingesters.values() {
            let mut toks = instance.tokens.clone();
            if !toks.windows(2).all(|w| w[0] <= w[1]) {
                toks.sort_unstable();
            }
            instances.push(toks);
        }
        merge_tokens(&instances)
    }

    fn get_tokens_by_zone(&self) -> HashMap<String, Vec<u32>> {
        let mut zones: HashMap<String, Vec<Vec<u32>>> = HashMap::new();
        for instance in self.ingesters.values() {
            let mut toks = instance.tokens.clone();
            if !toks.windows(2).all(|w| w[0] <= w[1]) {
                toks.sort_unstable();
            }
            zones.entry(instance.zone.clone()).or_default().push(toks);
        }
        let mut out = HashMap::with_capacity(zones.len());
        for (zone, lists) in zones {
            out.insert(zone, merge_tokens(&lists));
        }
        out
    }

    fn count_tokens(&self) -> HashMap<String, i64> {
        let mut owned: HashMap<String, i64> = HashMap::with_capacity(self.ingesters.len());
        let ring_tokens_by_zone = self.get_tokens_by_zone();
        let ring_instance_by_token = get_tokens_info(self);

        for ring_tokens in ring_tokens_by_zone.values() {
            for (i, &token) in ring_tokens.iter().enumerate() {
                let prev_token = if i == 0 {
                    ring_tokens[ring_tokens.len() - 1]
                } else {
                    ring_tokens[i - 1]
                };
                let diff = token_distance(prev_token, token);
                if let Some(info) = ring_instance_by_token.get(&token) {
                    *owned.entry(info.instance_id.clone()).or_insert(0) += diff;
                }
            }
        }
        for id in self.ingesters.keys() {
            owned.entry(id.clone()).or_insert(0);
        }
        owned
    }

    fn ring_compare(&self, other: &Desc) -> CompareResult {
        if self.ingesters.len() != other.ingesters.len() {
            return CompareResult::Different;
        }

        let mut equal_states_and_timestamps = true;
        for (name, ing) in &self.ingesters {
            let Some(oing) = other.ingesters.get(name) else {
                return CompareResult::Different;
            };
            if ing.addr != oing.addr
                || ing.zone != oing.zone
                || ing.registered_timestamp != oing.registered_timestamp
                || ing.read_only != oing.read_only
                || ing.read_only_updated_timestamp != oing.read_only_updated_timestamp
                || ing.tokens != oing.tokens
            {
                return CompareResult::Different;
            }
            if ing.timestamp != oing.timestamp || ing.state != oing.state {
                equal_states_and_timestamps = false;
            }
        }

        if equal_states_and_timestamps {
            CompareResult::Equal
        } else {
            CompareResult::EqualButStatesAndTimestamps
        }
    }

    fn set_instance_ids(&mut self) {
        for (id, inst) in &mut self.ingesters {
            inst.id = id.clone();
        }
    }

    fn merge_with_time(
        &mut self,
        other: &mut Desc,
        local_cas: bool,
        now: SystemTime,
    ) -> Option<Desc> {
        normalize_ingesters_map(other);

        let mut updated: Vec<String> = Vec::new();
        let mut tokens_changed = false;
        let timestamp_limit = future_timestamp_limit(now);
        let now_unix = system_time_to_unix_secs(now);

        for (name, oing) in &other.ingesters {
            if oing.timestamp > timestamp_limit {
                continue;
            }

            let ting = self.ingesters.get(name);
            let ting_ts = ting.map(|t| t.timestamp).unwrap_or(0);
            let ting_left = ting
                .and_then(|t| t.try_instance_state())
                == Some(InstanceState::Left);
            let this_timestamp = local_merge_timestamp(ting_ts, timestamp_limit);

            if oing.timestamp > this_timestamp {
                let ting_tokens = ting.map(|t| t.tokens.as_slice()).unwrap_or(&[]);
                if ting_tokens != oing.tokens.as_slice() {
                    tokens_changed = true;
                }
                self.ingesters.insert(name.clone(), oing.clone());
                updated.push(name.clone());
            } else if oing.timestamp == this_timestamp
                && !ting_left
                && oing.try_instance_state() == Some(InstanceState::Left)
            {
                self.ingesters.insert(name.clone(), oing.clone());
                updated.push(name.clone());
            }
        }

        if local_cas {
            for (name, ting) in &mut self.ingesters {
                if !other.ingesters.contains_key(name)
                    && ting.try_instance_state() != Some(InstanceState::Left)
                {
                    ting.state = InstanceState::Left as i32;
                    ting.tokens.clear();
                    ting.timestamp = now_unix;
                    updated.push(name.clone());
                }
            }
        }

        if updated.is_empty() {
            return None;
        }

        if tokens_changed && conflicting_tokens_exist(&self.ingesters) {
            resolve_conflicts(&mut self.ingesters);
        }

        let mut out = Desc::default();
        for u in updated {
            if let Some(ing) = self.ingesters.get(&u) {
                out.ingesters.insert(u, ing.clone());
            }
        }
        Some(out)
    }
}

pub fn normalize_ingesters_map(input: &mut Desc) {
    for ing in input.ingesters.values_mut() {
        if ing.try_instance_state() == Some(InstanceState::Left) {
            ing.tokens.clear();
            continue;
        }
        if ing.tokens.is_empty() {
            continue;
        }
        if !ing.tokens.windows(2).all(|w| w[0] <= w[1]) {
            ing.tokens.sort_unstable();
        }
        ing.tokens.dedup();
    }
}

fn conflicting_tokens_exist(ingesters: &HashMap<String, InstanceDesc>) -> bool {
    let mut seen = HashSet::new();
    for ing in ingesters.values() {
        for &t in &ing.tokens {
            if !seen.insert(t) {
                return true;
            }
        }
    }
    false
}

/// Resolves token conflicts identically to `dskit/ring/model.go`:
/// 1. `LEAVING` (and `LEFT`) loses to non-`LEAVING`.
/// 2. Otherwise the lexicographically smaller instance ID wins.
pub fn resolve_conflicts(ingesters: &mut HashMap<String, InstanceDesc>) {
    let total_tokens: usize = ingesters.values().map(|i| i.tokens.len()).sum();
    let mut token_to_ingester: BTreeMap<u32, String> = BTreeMap::new();

    for (ing_key, ing) in ingesters.iter() {
        if ing.try_instance_state() == Some(InstanceState::Left) {
            continue;
        }
        let ing_leaving = ing.try_instance_state() == Some(InstanceState::Leaving);
        for &token in &ing.tokens {
            match token_to_ingester.get(&token) {
                None => {
                    token_to_ingester.insert(token, ing_key.clone());
                }
                Some(prev_key) => {
                    let prev_leaving =
                        ingesters[prev_key].try_instance_state() == Some(InstanceState::Leaving);
                    let winner = if ing_leaving && !prev_leaving {
                        prev_key.clone()
                    } else if (prev_leaving && !ing_leaving) || (ing_key < prev_key) {
                        ing_key.clone()
                    } else {
                        prev_key.clone()
                    };
                    token_to_ingester.insert(token, winner);
                }
            }
        }
    }

    let _ = total_tokens;
    let mut new_token_lists: HashMap<String, Vec<u32>> =
        ingesters.keys().map(|k| (k.clone(), Vec::new())).collect();

    for (token, owner) in token_to_ingester {
        if let Some(list) = new_token_lists.get_mut(&owner) {
            list.push(token);
        }
    }

    for (key, toks) in new_token_lists {
        if let Some(ing) = ingesters.get_mut(&key) {
            ing.tokens = toks;
        }
    }
}

pub(crate) fn get_tokens_info(desc: &Desc) -> HashMap<u32, InstanceInfo> {
    let mut out = HashMap::new();
    for (instance_id, instance) in &desc.ingesters {
        for &token in &instance.tokens {
            out.insert(
                token,
                InstanceInfo {
                    instance_id: instance_id.clone(),
                    zone: instance.zone.clone(),
                },
            );
        }
    }
    out
}

pub(crate) fn merge_tokens(instances: &[Vec<u32>]) -> Vec<u32> {
    let total: usize = instances.iter().map(|v| v.len()).sum();
    let mut out = Vec::with_capacity(total);
    for v in instances {
        out.extend_from_slice(v);
    }
    out.sort_unstable();
    out
}

pub(crate) fn get_oldest_registered_timestamp(desc: &Desc) -> i64 {
    let mut result = 0i64;
    for inst in desc.ingesters.values() {
        if inst.registered_timestamp == 0 {
            return 0;
        }
        if result == 0 || inst.registered_timestamp < result {
            result = inst.registered_timestamp;
        }
    }
    result
}

pub(crate) fn read_only_instances_and_oldest_updated(desc: &Desc) -> (usize, i64) {
    let mut count = 0usize;
    let mut oldest = 0i64;
    let mut first = true;
    for inst in desc.ingesters.values() {
        if !inst.read_only {
            continue;
        }
        count += 1;
        if first {
            oldest = inst.read_only_updated_timestamp;
            first = false;
        } else {
            oldest = oldest.min(inst.read_only_updated_timestamp);
        }
    }
    (count, oldest)
}

/// Go `encoding/json` of `ring.Desc` (gogo `json:"ingesters"`; `InstanceDesc` fields `omitempty`).
pub fn desc_to_go_json(d: &Desc) -> GoJson {
    let ingesters = if d.ingesters.is_empty() {
        GoJson::Null
    } else {
        GoJson::sorted_map(
            d.ingesters
                .iter()
                .map(|(k, v)| (k.clone(), instance_desc_to_go_json(v)))
                .collect(),
        )
    };
    GoJson::Object(vec![("ingesters".into(), ingesters)])
}

fn instance_desc_to_go_json(i: &InstanceDesc) -> GoJson {
    let mut f: Vec<(String, GoJson)> = Vec::new();
    if !i.addr.is_empty() {
        f.push(("addr".into(), GoJson::str(i.addr.as_str())));
    }
    if i.timestamp != 0 {
        f.push(("timestamp".into(), GoJson::int(i.timestamp)));
    }
    if i.state != 0 {
        f.push(("state".into(), GoJson::int(i.state)));
    }
    if !i.tokens.is_empty() {
        f.push((
            "tokens".into(),
            GoJson::Array(i.tokens.iter().map(|t| GoJson::int(*t)).collect()),
        ));
    }
    if !i.zone.is_empty() {
        f.push(("zone".into(), GoJson::str(i.zone.as_str())));
    }
    if i.registered_timestamp != 0 {
        f.push(("registered_timestamp".into(), GoJson::int(i.registered_timestamp)));
    }
    if !i.id.is_empty() {
        f.push(("id".into(), GoJson::str(i.id.as_str())));
    }
    if i.read_only_updated_timestamp != 0 {
        f.push((
            "read_only_updated_timestamp".into(),
            GoJson::int(i.read_only_updated_timestamp),
        ));
    }
    if i.read_only {
        f.push(("read_only".into(), GoJson::Bool(true)));
    }
    if !i.versions.is_empty() {
        f.push((
            "versions".into(),
            GoJson::sorted_map(
                i.versions
                    .iter()
                    .map(|(k, v)| (k.to_string(), GoJson::int(*v)))
                    .collect(),
            ),
        ));
    }
    GoJson::Object(f)
}

/// Go `fmt.Sprintf("%#v", *ring.Desc)` (gogo `Desc.GoString`; map values use Go's default `%#v`).
pub fn desc_go_string(d: &Desc) -> String {
    let mut s = String::from("&ring.Desc{");
    if !d.ingesters.is_empty() {
        let mut keys: Vec<&String> = d.ingesters.keys().collect();
        keys.sort();
        s.push_str("Ingesters: map[string]InstanceDesc{");
        for k in keys {
            s.push_str(&go_sharp_v_str(k));
            s.push_str(": ");
            s.push_str(&instance_desc_sharp_v(&d.ingesters[k]));
            s.push(',');
        }
        s.push_str("},\n");
    }
    s.push('}');
    s
}

fn instance_desc_sharp_v(i: &InstanceDesc) -> String {
    format!(
        "ring.InstanceDesc{{Addr:{}, Timestamp:{}, State:{}, Tokens:{}, Zone:{}, RegisteredTimestamp:{}, Id:{}, ReadOnlyUpdatedTimestamp:{}, ReadOnly:{}, Versions:{}}}",
        go_sharp_v_str(&i.addr),
        i.timestamp,
        i.state,
        go_sharp_v_u32_slice(&i.tokens),
        go_sharp_v_str(&i.zone),
        i.registered_timestamp,
        go_sharp_v_str(&i.id),
        i.read_only_updated_timestamp,
        i.read_only,
        go_sharp_v_u64_map(&i.versions),
    )
}

impl Mergeable for Desc {
    fn merge(
        &mut self,
        other: &mut dyn Mergeable,
        local_cas: bool,
    ) -> Result<Option<Box<dyn Mergeable>>, String> {
        let other_desc = other
            .as_any_mut()
            .downcast_mut::<Desc>()
            .ok_or_else(|| "expected ringpb::Desc".to_string())?;
        Ok(self
            .merge_with_time(other_desc, local_cas, SystemTime::now())
            .map(|c| Box::new(c) as Box<dyn Mergeable>))
    }

    fn merge_content(&self) -> Vec<String> {
        self.ingesters.keys().cloned().collect()
    }

    fn remove_tombstones(&mut self, limit: Option<SystemTime>) -> (usize, usize) {
        let mut total = 0usize;
        let mut removed = 0usize;

        self.ingesters.retain(|_, ing| {
            if ing.try_instance_state() == Some(InstanceState::Left) {
                let expired = match limit {
                    None => true,
                    Some(lim) => unix_secs_before(ing.timestamp, lim),
                };
                if expired {
                    removed += 1;
                    return false;
                } else {
                    total += 1;
                }
            }
            true
        });

        (total, removed)
    }

    fn clone_mergeable(&self) -> Box<dyn Mergeable> {
        Box::new(self.clone())
    }

    fn to_go_json(&self) -> GoJson {
        desc_to_go_json(self)
    }

    fn go_string(&self) -> String {
        desc_go_string(self)
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        self
    }

    fn as_any_mut(&mut self) -> &mut (dyn Any + Send + Sync) {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync> {
        self
    }
}

pub(crate) fn unix_secs_before(ts_secs: i64, limit: SystemTime) -> bool {
    if ts_secs >= 0 {
        match UNIX_EPOCH.checked_add(Duration::from_secs(ts_secs as u64)) {
            Some(t) => t < limit,
            None => false,
        }
    } else {
        match UNIX_EPOCH.checked_sub(Duration::from_secs(ts_secs.unsigned_abs())) {
            Some(t) => t < limit,
            None => true,
        }
    }
}
