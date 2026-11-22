//! Kafka consumer group balancers (range, roundrobin, sticky, cooperative-sticky) and
//! KIP-881 rack-aware assignment ported from github.com/twmb/franz-go/pkg/kgo/group_balancer.go.

use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OwnedPartition {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConsumerMemberMetadata {
    pub version: i16,
    pub topics: Vec<String>,
    pub user_data: Vec<u8>,
    pub owned_partitions: Vec<OwnedPartition>,
    pub generation: i32,
    pub rack: Option<String>,
}

impl ConsumerMemberMetadata {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.version.to_be_bytes());
        out.extend_from_slice(&(self.topics.len() as i32).to_be_bytes());
        for t in &self.topics {
            out.extend_from_slice(&(t.len() as i16).to_be_bytes());
            out.extend_from_slice(t.as_bytes());
        }
        if self.user_data.is_empty() {
            out.extend_from_slice(&(-1i32).to_be_bytes());
        } else {
            out.extend_from_slice(&(self.user_data.len() as i32).to_be_bytes());
            out.extend_from_slice(&self.user_data);
        }
        if self.version >= 1 {
            out.extend_from_slice(&(self.owned_partitions.len() as i32).to_be_bytes());
            for op in &self.owned_partitions {
                out.extend_from_slice(&(op.topic.len() as i16).to_be_bytes());
                out.extend_from_slice(op.topic.as_bytes());
                out.extend_from_slice(&(op.partitions.len() as i32).to_be_bytes());
                for &p in &op.partitions {
                    out.extend_from_slice(&p.to_be_bytes());
                }
            }
        }
        if self.version >= 2 {
            out.extend_from_slice(&self.generation.to_be_bytes());
        }
        if self.version >= 3 {
            match &self.rack {
                None => out.extend_from_slice(&(-1i16).to_be_bytes()),
                Some(r) => {
                    out.extend_from_slice(&(r.len() as i16).to_be_bytes());
                    out.extend_from_slice(r.as_bytes());
                }
            }
        }
        out
    }

    pub fn decode(mut src: &[u8]) -> Result<Self, String> {
        fn read_i16(s: &mut &[u8]) -> Result<i16, String> {
            if s.len() < 2 {
                return Err("truncated i16".into());
            }
            let v = i16::from_be_bytes([s[0], s[1]]);
            *s = &s[2..];
            Ok(v)
        }
        fn read_i32(s: &mut &[u8]) -> Result<i32, String> {
            if s.len() < 4 {
                return Err("truncated i32".into());
            }
            let v = i32::from_be_bytes([s[0], s[1], s[2], s[3]]);
            *s = &s[4..];
            Ok(v)
        }
        fn read_str(s: &mut &[u8]) -> Result<String, String> {
            let len = read_i16(s)?;
            if len < 0 {
                return Err("unexpected null string".into());
            }
            let len = len as usize;
            if s.len() < len {
                return Err("truncated string".into());
            }
            let st = std::str::from_utf8(&s[..len])
                .map_err(|e| e.to_string())?
                .to_string();
            *s = &s[len..];
            Ok(st)
        }
        fn read_nullable_str(s: &mut &[u8]) -> Result<Option<String>, String> {
            let len = read_i16(s)?;
            if len < 0 {
                return Ok(None);
            }
            let len = len as usize;
            if s.len() < len {
                return Err("truncated nullable string".into());
            }
            let st = std::str::from_utf8(&s[..len])
                .map_err(|e| e.to_string())?
                .to_string();
            *s = &s[len..];
            Ok(Some(st))
        }

        let version = read_i16(&mut src)?;
        let n_topics = read_i32(&mut src)?;
        if n_topics < 0 {
            return Err("negative topics count".into());
        }
        let mut topics = Vec::with_capacity((n_topics as usize).min(src.len() / 2));
        for _ in 0..n_topics {
            topics.push(read_str(&mut src)?);
        }
        let ud_len = read_i32(&mut src)?;
        let user_data = if ud_len < 0 {
            Vec::new()
        } else {
            let u = ud_len as usize;
            if src.len() < u {
                return Err("truncated user_data".into());
            }
            let d = src[..u].to_vec();
            src = &src[u..];
            d
        };
        let mut owned_partitions = Vec::new();
        if version >= 1 {
            let n_owned = read_i32(&mut src)?;
            if n_owned < 0 {
                return Err("negative owned count".into());
            }
            for _ in 0..n_owned {
                let topic = read_str(&mut src)?;
                let np = read_i32(&mut src)?;
                if np < 0 {
                    return Err("negative owned partitions count".into());
                }
                let mut parts = Vec::with_capacity((np as usize).min(src.len() / 4));
                for _ in 0..np {
                    parts.push(read_i32(&mut src)?);
                }
                owned_partitions.push(OwnedPartition {
                    topic,
                    partitions: parts,
                });
            }
        }
        let generation = if version >= 2 && !src.is_empty() {
            read_i32(&mut src)?
        } else {
            -1
        };
        let rack = if version >= 3 && !src.is_empty() {
            read_nullable_str(&mut src)?
        } else {
            None
        };
        Ok(Self {
            version,
            topics,
            user_data,
            owned_partitions,
            generation,
            rack,
        })
    }
}

pub fn simple_member_metadata(interests: &[&str], generation: i32) -> Vec<u8> {
    let mut topics: Vec<String> = interests.iter().map(|s| (*s).to_string()).collect();
    topics.sort();
    ConsumerMemberMetadata {
        version: 3,
        topics,
        generation,
        ..Default::default()
    }
    .encode()
}

#[derive(Debug, Clone, Default)]
pub struct JoinGroupMemberInput {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub protocol_metadata: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct BalancePlan {
    pub plan: HashMap<String, HashMap<String, Vec<i32>>>,
}

impl BalancePlan {
    pub fn new(members: &[JoinGroupMemberInput]) -> Self {
        let mut plan = HashMap::with_capacity(members.len());
        for m in members {
            plan.insert(m.member_id.clone(), HashMap::new());
        }
        Self { plan }
    }

    pub fn add_partition(&mut self, member_id: &str, topic: &str, partition: i32) {
        self.plan
            .entry(member_id.to_string())
            .or_default()
            .entry(topic.to_string())
            .or_default()
            .push(partition);
    }

    /// Ported from BalancePlan.AdjustCooperative (including KIP-792 max-claim generation guard).
    pub fn adjust_cooperative(&mut self, b: &ConsumerBalancer) {
        let mut all_added: HashMap<String, HashMap<i32, String>> = HashMap::new();
        let mut all_revoked: HashMap<String, HashSet<i32>> = HashMap::new();

        let mut max_claim: HashMap<String, HashMap<i32, i32>> = HashMap::new();
        for meta in &b.metadatas {
            for ot in &meta.owned_partitions {
                let tc = max_claim.entry(ot.topic.clone()).or_default();
                for &p in &ot.partitions {
                    tc.entry(p)
                        .and_modify(|g| {
                            if meta.generation > *g {
                                *g = meta.generation;
                            }
                        })
                        .or_insert(meta.generation);
                }
            }
        }

        for (member, meta) in b.members.iter().zip(b.metadatas.iter()) {
            let planned = self.plan.get(&member.member_id).cloned().unwrap_or_default();
            let mut new_topics: HashSet<String> = planned.keys().cloned().collect();

            for ot in &meta.owned_partitions {
                let topic = &ot.topic;
                new_topics.remove(topic);
                let Some(ppartitions) = planned.get(topic) else {
                    let rev = all_revoked.entry(topic.clone()).or_default();
                    for &p in &ot.partitions {
                        rev.insert(p);
                    }
                    continue;
                };
                let mut pmap: HashSet<i32> = ppartitions.iter().copied().collect();
                for &op in &ot.partitions {
                    let max_g = max_claim
                        .get(topic)
                        .and_then(|m| m.get(&op))
                        .copied()
                        .unwrap_or(i32::MIN);
                    if meta.generation >= max_g {
                        pmap.remove(&op);
                    }
                }
                if !pmap.is_empty() {
                    let add_t = all_added.entry(topic.clone()).or_default();
                    for p in pmap {
                        add_t.insert(p, member.member_id.clone());
                    }
                }
                let planned_set: HashSet<i32> = ppartitions.iter().copied().collect();
                for &op in &ot.partitions {
                    if !planned_set.contains(&op) {
                        all_revoked.entry(topic.clone()).or_default().insert(op);
                    }
                }
            }
            for pt in new_topics {
                if let Some(parts) = planned.get(&pt) {
                    let add_t = all_added.entry(pt).or_default();
                    for &p in parts {
                        add_t.insert(p, member.member_id.clone());
                    }
                }
            }
        }

        for (topic, rparts) in all_revoked {
            let Some(atopic) = all_added.get(&topic) else {
                continue;
            };
            for rpart in rparts {
                let Some(amember) = atopic.get(&rpart) else {
                    continue;
                };
                if let Some(ptopics) = self.plan.get_mut(amember) {
                    if let Some(pparts) = ptopics.get_mut(&topic) {
                        if let Some(pos) = pparts.iter().position(|&p| p == rpart) {
                            pparts.swap_remove(pos);
                        }
                        if pparts.is_empty() {
                            ptopics.remove(&topic);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerBalancer {
    pub members: Vec<JoinGroupMemberInput>,
    pub metadatas: Vec<ConsumerMemberMetadata>,
    pub topics: HashSet<String>,
    pub partition_racks: Option<HashMap<String, Vec<String>>>,
}

impl ConsumerBalancer {
    pub fn from_members(members: Vec<JoinGroupMemberInput>) -> Result<Self, String> {
        let mut seen = HashSet::with_capacity(members.len());
        let mut dedup = Vec::with_capacity(members.len());
        for m in members {
            if seen.insert(m.member_id.clone()) {
                dedup.push(m);
            }
        }
        let mut metadatas = Vec::with_capacity(dedup.len());
        let mut topics = HashSet::new();
        for m in &dedup {
            let mut meta = match ConsumerMemberMetadata::decode(&m.protocol_metadata) {
                Ok(md) => md,
                Err(e) => {
                    if m.protocol_metadata.starts_with(&[0, 1]) {
                        let mut patched = m.protocol_metadata.clone();
                        patched[0] = 0;
                        patched[1] = 0;
                        ConsumerMemberMetadata::decode(&patched)
                            .map_err(|e2| format!("unable to read member metadata: {e2}"))?
                    } else {
                        return Err(format!("unable to read member metadata: {e}"));
                    }
                }
            };
            for t in &meta.topics {
                topics.insert(t.clone());
            }
            meta.topics.sort();
            metadatas.push(meta);
        }
        Ok(Self {
            members: dedup,
            metadatas,
            topics,
            partition_racks: None,
        })
    }

    pub fn any_member_rack(&self) -> bool {
        self.metadatas.iter().any(|m| m.rack.is_some())
    }
}

pub fn sort_join_members(members: &mut [JoinGroupMemberInput]) {
    members.sort_by(|l, r| match (&l.instance_id, &r.instance_id) {
        (Some(li), Some(ri)) => li.cmp(ri),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => l.member_id.cmp(&r.member_id),
    });
}

/// Ported from rangeBalancer.Balance (with KIP-881 rack-aware phase 1 + range phase 2).
pub fn balance_range(b: &ConsumerBalancer, topics: &HashMap<String, i32>) -> BalancePlan {
    let mut member_rack: HashMap<&str, &str> = HashMap::new();
    for (m, meta) in b.members.iter().zip(b.metadatas.iter()) {
        if let Some(ref r) = meta.rack {
            member_rack.insert(&m.member_id, r.as_str());
        }
    }

    let mut topics_to_consumers: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (idx, meta) in b.metadatas.iter().enumerate() {
        for t in &meta.topics {
            topics_to_consumers.entry(t.clone()).or_default().push(idx);
        }
    }

    let mut plan = BalancePlan::new(&b.members);
    for (topic, mut consumers) in topics_to_consumers {
        consumers.sort_by(|&li, &ri| {
            let l = &b.members[li];
            let r = &b.members[ri];
            match (&l.instance_id, &r.instance_id) {
                (Some(a), Some(b)) => a.cmp(b),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => l.member_id.cmp(&r.member_id),
            }
        });
        let num_partitions = *topics.get(&topic).unwrap_or(&0) as usize;
        let n_consumers = consumers.len();
        if n_consumers == 0 || num_partitions == 0 {
            continue;
        }
        let div = num_partitions / n_consumers;
        let rem = num_partitions % n_consumers;
        let mut assigned = vec![false; num_partitions];
        let mut assign_count = vec![0usize; n_consumers];

        if let Some(topic_racks) = b.partition_racks.as_ref().and_then(|pr| pr.get(&topic)) {
            for (ci, &mi) in consumers.iter().enumerate() {
                let rack = member_rack.get(b.members[mi].member_id.as_str()).copied().unwrap_or("");
                if rack.is_empty() {
                    continue;
                }
                let max_for_consumer = div + usize::from(ci < rem);
                for p in 0..num_partitions {
                    if assign_count[ci] >= max_for_consumer {
                        break;
                    }
                    if assigned[p] {
                        continue;
                    }
                    if p < topic_racks.len() && topic_racks[p] == rack {
                        plan.add_partition(&b.members[mi].member_id, &topic, p as i32);
                        assigned[p] = true;
                        assign_count[ci] += 1;
                    }
                }
            }
        }

        let mut p_idx = 0usize;
        for (ci, &mi) in consumers.iter().enumerate() {
            let max_for_consumer = div + usize::from(ci < rem);
            let mut quota = max_for_consumer.saturating_sub(assign_count[ci]);
            while p_idx < num_partitions && quota > 0 {
                if !assigned[p_idx] {
                    plan.add_partition(&b.members[mi].member_id, &topic, p_idx as i32);
                    assigned[p_idx] = true;
                    quota -= 1;
                }
                p_idx += 1;
            }
        }
    }
    plan
}

/// Sticky + cooperative-sticky balancer preserving highest-generation claims and balancing loads.
pub fn balance_sticky(
    b: &ConsumerBalancer,
    topics: &HashMap<String, i32>,
    cooperative: bool,
) -> BalancePlan {
    let mut plan = BalancePlan::new(&b.members);
    if b.members.is_empty() {
        return plan;
    }

    let mut best_owner: HashMap<(String, i32), (i32, usize)> = HashMap::new();
    for (mi, meta) in b.metadatas.iter().enumerate() {
        let sub_set: HashSet<&str> = meta.topics.iter().map(String::as_str).collect();
        for ot in &meta.owned_partitions {
            if !sub_set.contains(ot.topic.as_str()) {
                continue;
            }
            let max_p = *topics.get(&ot.topic).unwrap_or(&0);
            for &p in &ot.partitions {
                if p < 0 || p >= max_p {
                    continue;
                }
                best_owner
                    .entry((ot.topic.clone(), p))
                    .and_modify(|cur| {
                        if meta.generation > cur.0 {
                            *cur = (meta.generation, mi);
                        }
                    })
                    .or_insert((meta.generation, mi));
            }
        }
    }

    let mut member_loads = vec![0usize; b.members.len()];
    let mut unassigned = Vec::new();
    let mut sorted_topics: Vec<&String> = topics.keys().collect();
    sorted_topics.sort();
    for t in sorted_topics {
        let np = topics[t];
        for p in 0..np {
            if let Some(&(_, mi)) = best_owner.get(&(t.clone(), p)) {
                plan.add_partition(&b.members[mi].member_id, t, p);
                member_loads[mi] += 1;
            } else {
                unassigned.push((t.clone(), p));
            }
        }
    }

    loop {
        let mut max_load = 0usize;
        let mut max_mi = 0usize;
        for (mi, &l) in member_loads.iter().enumerate() {
            if l >= max_load {
                max_load = l;
                max_mi = mi;
            }
        }
        let mut min_load = usize::MAX;
        let mut min_mi = 0usize;
        for (mi, &l) in member_loads.iter().enumerate() {
            if l < min_load {
                min_load = l;
                min_mi = mi;
            }
        }
        if max_load <= min_load + 1 {
            break;
        }
        let min_subs: HashSet<&str> = b.metadatas[min_mi].topics.iter().map(String::as_str).collect();
        let min_stale: HashSet<(String, i32)> = b.metadatas[min_mi]
            .owned_partitions
            .iter()
            .flat_map(|ot| ot.partitions.iter().map(move |&p| (ot.topic.clone(), p)))
            .collect();
        let mut moved = false;
        if let Some(mplan) = plan.plan.get_mut(&b.members[max_mi].member_id) {
            // Prefer moving a partition that min_mi previously claimed (stale resticky).
            let stale_pick = mplan
                .iter()
                .filter(|(t, _)| min_subs.contains(t.as_str()))
                .find_map(|(t, ps)| {
                    ps.iter()
                        .position(|&p| min_stale.contains(&(t.clone(), p)))
                        .map(|idx| (t.clone(), idx))
                });
            let pick = stale_pick.or_else(|| {
                mplan
                    .iter()
                    .find(|(t, ps)| !ps.is_empty() && min_subs.contains(t.as_str()))
                    .map(|(t, ps)| (t.clone(), ps.len() - 1))
            });
            if let Some((ct, idx)) = pick {
                if let Some(ps) = mplan.get_mut(&ct) {
                    let p = ps.remove(idx);
                    if ps.is_empty() {
                        mplan.remove(&ct);
                    }
                    plan.add_partition(&b.members[min_mi].member_id, &ct, p);
                    member_loads[max_mi] -= 1;
                    member_loads[min_mi] += 1;
                    moved = true;
                }
            }
        }
        if !moved {
            break;
        }
    }

    for (t, p) in unassigned {
        let best_mi = b
            .metadatas
            .iter()
            .enumerate()
            .filter(|(_, meta)| meta.topics.iter().any(|mt| mt == &t))
            .min_by_key(|(mi, _)| member_loads[*mi])
            .map(|(mi, _)| mi);
        if let Some(mi) = best_mi {
            plan.add_partition(&b.members[mi].member_id, &t, p);
            member_loads[mi] += 1;
        }
    }

    if cooperative {
        plan.adjust_cooperative(b);
    }
    plan
}

/// Ported from groupConsumer.needRackMeta.
pub fn need_rack_meta(
    b: &ConsumerBalancer,
    my_topics: &HashSet<String>,
    cached_topics: &HashSet<String>,
    topics: &HashSet<String>,
) -> bool {
    if !b.any_member_rack() {
        return false;
    }
    for t in topics {
        if my_topics.contains(t) {
            continue;
        }
        if !cached_topics.contains(t) {
            return true;
        }
    }
    false
}

/// Ported from groupConsumer.buildPartitionRacks.
pub fn build_partition_racks(
    b: &ConsumerBalancer,
    broker_racks: &HashMap<i32, String>,
    cached_topic_partitions: &HashMap<String, Vec<(i32, i32)>>,
    own_topic_leaders: &HashMap<String, Vec<i32>>,
    topic_partition_count: &HashMap<String, i32>,
) -> Option<HashMap<String, Vec<String>>> {
    if !b.any_member_rack() || broker_racks.is_empty() {
        return None;
    }
    let mut out = HashMap::with_capacity(topic_partition_count.len());
    let mut any_rack = false;
    for (topic, &num_parts) in topic_partition_count {
        let mut racks = vec![String::new(); num_parts.max(0) as usize];
        if let Some(ps) = cached_topic_partitions.get(topic) {
            for &(p, leader) in ps {
                if p >= 0 && (p as usize) < racks.len() {
                    if let Some(r) = broker_racks.get(&leader) {
                        racks[p as usize] = r.clone();
                        if !r.is_empty() {
                            any_rack = true;
                        }
                    }
                }
            }
        } else if let Some(leaders) = own_topic_leaders.get(topic) {
            for (i, &leader) in leaders.iter().enumerate() {
                if i < racks.len() {
                    if let Some(r) = broker_racks.get(&leader) {
                        racks[i] = r.clone();
                        if !r.is_empty() {
                            any_rack = true;
                        }
                    }
                }
            }
        }
        out.insert(topic.clone(), racks);
    }
    if any_rack { Some(out) } else { None }
}
