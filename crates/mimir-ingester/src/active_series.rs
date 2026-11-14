//! Striped time-windowed active series & native histogram bucket tracker.
//!
//! Ported from github.com/grafana/mimir/pkg/ingester/activeseries/active_series.go.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use parking_lot::RwLock;

const NUM_STRIPES: usize = 128;

const FLAG_IS_DELETED: u8 = 1 << 0;
const FLAG_IS_OTLP: u8 = 1 << 1;

#[derive(Clone, Debug)]
pub struct CustomTrackerMatcher {
    pub name: String,
    pub matchers: Vec<LabelMatcher>,
}

/// Parses `LimitsConfig.active_series_custom_trackers` (`map[name]promql_matchers`) into `CustomTrackerMatcher`s.
pub fn parse_custom_tracker_matchers(
    raw: &std::collections::BTreeMap<String, String>,
) -> Vec<CustomTrackerMatcher> {
    let mut out = Vec::new();
    for (name, expr) in raw {
        if let Some(ms) = parse_matcher_list_expr(expr) {
            out.push(CustomTrackerMatcher {
                name: name.clone(),
                matchers: ms,
            });
        }
    }
    out
}

fn parse_matcher_list_expr(expr: &str) -> Option<Vec<LabelMatcher>> {
    let trimmed = expr.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(trimmed);
    if inner.trim().is_empty() {
        return Some(Vec::new());
    }
    let mut matchers = Vec::new();
    for part in inner.split(',') {
        let p = part.trim();
        if p.is_empty() {
            continue;
        }
        let (op, name, rest) = if let Some((n, v)) = p.split_once("!~") {
            (MatchType::RegexNotMatch, n.trim(), v.trim())
        } else if let Some((n, v)) = p.split_once("=~") {
            (MatchType::RegexMatch, n.trim(), v.trim())
        } else if let Some((n, v)) = p.split_once("!=") {
            (MatchType::NotEqual, n.trim(), v.trim())
        } else if let Some((n, v)) = p.split_once('=') {
            (MatchType::Equal, n.trim(), v.trim())
        } else {
            return None;
        };
        let val = rest
            .strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))
            .or_else(|| rest.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
            .unwrap_or(rest);
        matchers.push(LabelMatcher::new(op, name, val).ok()?);
    }
    Some(matchers)
}

#[derive(Clone)]
struct SeriesEntry {
    nanos: Arc<AtomicI64>,
    matches: Vec<usize>,
    num_native_histogram_buckets: i64, // -1 if not a native histogram
    flags: u8,
}

impl SeriesEntry {
    fn is_deleted(&self) -> bool {
        (self.flags & FLAG_IS_DELETED) != 0
    }

    fn mark_deleted(&mut self) {
        self.flags |= FLAG_IS_DELETED;
    }

    fn is_otlp(&self) -> bool {
        (self.flags & FLAG_IS_OTLP) != 0
    }
}

struct DeletedEntry {
    ref_id: u64,
    _labels: Labels,
}

#[derive(Default)]
struct DeletedSeries {
    by_hash: parking_lot::Mutex<HashMap<u64, DeletedEntry>>,
}

impl DeletedSeries {
    fn add(&self, ref_id: u64, labels: Labels) {
        let h = labels.hash();
        self.by_hash.lock().insert(
            h,
            DeletedEntry {
                ref_id,
                _labels: labels,
            },
        );
    }

    fn find_and_remove(&self, hash: u64) -> Option<u64> {
        self.by_hash.lock().remove(&hash).map(|d| d.ref_id)
    }

    fn clear(&self) {
        self.by_hash.lock().clear();
    }
}

struct SeriesStripeInner {
    refs: HashMap<u64, SeriesEntry>,
    active: u32,
    active_otlp: u32,
    active_matching: Vec<u32>,
    active_native_histograms: u32,
    active_matching_native_histograms: Vec<u32>,
    active_native_histogram_buckets: u32,
    active_matching_native_histogram_buckets: Vec<u32>,
}

struct SeriesStripe {
    oldest_entry_ns: AtomicI64,
    inner: RwLock<SeriesStripeInner>,
}

impl SeriesStripe {
    fn new(num_matchers: usize) -> Self {
        Self {
            oldest_entry_ns: AtomicI64::new(0),
            inner: RwLock::new(SeriesStripeInner {
                refs: HashMap::new(),
                active: 0,
                active_otlp: 0,
                active_matching: vec![0; num_matchers],
                active_native_histograms: 0,
                active_matching_native_histograms: vec![0; num_matchers],
                active_native_histogram_buckets: 0,
                active_matching_native_histogram_buckets: vec![0; num_matchers],
            }),
        }
    }

    fn update_series_timestamp(
        &self,
        now_ns: i64,
        pairs: &[(&str, &str)],
        ref_id: u64,
        num_native_histogram_buckets: i64,
        is_otlp: bool,
        matchers: &[CustomTrackerMatcher],
    ) -> bool {
        // Fast path: check under read lock
        {
            let guard = self.inner.read();
            if let Some(entry) = guard.refs.get(&ref_id) {
                let prev = entry.nanos.load(Ordering::Relaxed);
                if now_ns > prev {
                    entry.nanos.store(now_ns, Ordering::Relaxed);
                }
                let need_bucket_update = entry.num_native_histogram_buckets
                    != num_native_histogram_buckets
                    && (entry.num_native_histogram_buckets >= 0 || num_native_histogram_buckets >= 0);
                if !need_bucket_update && !(is_otlp && !entry.is_otlp()) {
                    return false;
                }
            }
        }

        let mut guard = self.inner.write();
        if guard.refs.contains_key(&ref_id) {
            let (old_buckets, was_otlp, matches) = {
                let entry = guard.refs.get_mut(&ref_id).expect("checked contains_key");
                let prev = entry.nanos.load(Ordering::Relaxed);
                if now_ns > prev {
                    entry.nanos.store(now_ns, Ordering::Relaxed);
                }
                let was_otlp = entry.is_otlp();
                if is_otlp && !was_otlp {
                    entry.flags |= FLAG_IS_OTLP;
                }
                let old_b = entry.num_native_histogram_buckets;
                if old_b != num_native_histogram_buckets {
                    entry.num_native_histogram_buckets = num_native_histogram_buckets;
                }
                (old_b, was_otlp, entry.matches.clone())
            };
            if is_otlp && !was_otlp {
                guard.active_otlp += 1;
            }
            if old_buckets != num_native_histogram_buckets {
                if old_buckets >= 0 && num_native_histogram_buckets < 0 {
                    guard.active_native_histograms =
                        guard.active_native_histograms.saturating_sub(1);
                    guard.active_native_histogram_buckets = guard
                        .active_native_histogram_buckets
                        .saturating_sub(old_buckets as u32);
                    for idx in matches {
                        if let Some(v) = guard.active_matching_native_histograms.get_mut(idx) {
                            *v = v.saturating_sub(1);
                        }
                        if let Some(v) = guard.active_matching_native_histogram_buckets.get_mut(idx)
                        {
                            *v = v.saturating_sub(old_buckets as u32);
                        }
                    }
                } else if old_buckets < 0 && num_native_histogram_buckets >= 0 {
                    guard.active_native_histograms += 1;
                    guard.active_native_histogram_buckets += num_native_histogram_buckets as u32;
                    for idx in matches {
                        if let Some(v) = guard.active_matching_native_histograms.get_mut(idx) {
                            *v += 1;
                        }
                        if let Some(v) = guard.active_matching_native_histogram_buckets.get_mut(idx)
                        {
                            *v += num_native_histogram_buckets as u32;
                        }
                    }
                } else if old_buckets >= 0 && num_native_histogram_buckets >= 0 {
                    let delta = num_native_histogram_buckets - old_buckets;
                    if delta >= 0 {
                        guard.active_native_histogram_buckets += delta as u32;
                        for idx in matches {
                            if let Some(v) =
                                guard.active_matching_native_histogram_buckets.get_mut(idx)
                            {
                                *v += delta as u32;
                            }
                        }
                    } else {
                        let sub = (-delta) as u32;
                        guard.active_native_histogram_buckets =
                            guard.active_native_histogram_buckets.saturating_sub(sub);
                        for idx in matches {
                            if let Some(v) =
                                guard.active_matching_native_histogram_buckets.get_mut(idx)
                            {
                                *v = v.saturating_sub(sub);
                            }
                        }
                    }
                }
            }
            return false;
        }

        let mut matches = Vec::new();
        for (idx, cm) in matchers.iter().enumerate() {
            if cm.matchers.iter().all(|m| {
                let val = pairs
                    .iter()
                    .find(|(k, _)| *k == m.name)
                    .map(|(_, v)| *v)
                    .unwrap_or("");
                m.matches(val)
            }) {
                matches.push(idx);
            }
        }

        let mut flags = 0u8;
        if is_otlp {
            flags |= FLAG_IS_OTLP;
            guard.active_otlp += 1;
        }
        guard.active += 1;
        for &idx in &matches {
            if let Some(v) = guard.active_matching.get_mut(idx) {
                *v += 1;
            }
        }
        if num_native_histogram_buckets >= 0 {
            guard.active_native_histograms += 1;
            guard.active_native_histogram_buckets += num_native_histogram_buckets as u32;
            for &idx in &matches {
                if let Some(v) = guard.active_matching_native_histograms.get_mut(idx) {
                    *v += 1;
                }
                if let Some(v) = guard.active_matching_native_histogram_buckets.get_mut(idx) {
                    *v += num_native_histogram_buckets as u32;
                }
            }
        }

        guard.refs.insert(
            ref_id,
            SeriesEntry {
                nanos: Arc::new(AtomicI64::new(now_ns)),
                matches,
                num_native_histogram_buckets,
                flags,
            },
        );
        let prev_oldest = self.oldest_entry_ns.load(Ordering::Relaxed);
        if prev_oldest == 0 || now_ns < prev_oldest {
            self.oldest_entry_ns.store(now_ns, Ordering::Relaxed);
        }
        true
    }

    fn contains_ref(&self, ref_id: u64) -> bool {
        self.inner.read().refs.contains_key(&ref_id)
    }

    fn native_histogram_buckets(&self, ref_id: u64) -> Option<u64> {
        let guard = self.inner.read();
        let e = guard.refs.get(&ref_id)?;
        if e.num_native_histogram_buckets >= 0 {
            Some(e.num_native_histogram_buckets as u64)
        } else {
            None
        }
    }

    fn mark_deleted(&self, ref_id: u64, labels: Labels, deleted: &DeletedSeries) {
        let mut guard = self.inner.write();
        if let Some(entry) = guard.refs.get_mut(&ref_id) {
            entry.mark_deleted();
            deleted.add(ref_id, labels);
        }
    }

    fn remove(&self, ref_id: u64) {
        let mut guard = self.inner.write();
        let Some(entry) = guard.refs.remove(&ref_id) else {
            return;
        };
        guard.active = guard.active.saturating_sub(1);
        if entry.is_otlp() {
            guard.active_otlp = guard.active_otlp.saturating_sub(1);
        }
        for &idx in &entry.matches {
            if let Some(v) = guard.active_matching.get_mut(idx) {
                *v = v.saturating_sub(1);
            }
        }
        if entry.num_native_histogram_buckets >= 0 {
            let b = entry.num_native_histogram_buckets as u32;
            guard.active_native_histograms = guard.active_native_histograms.saturating_sub(1);
            guard.active_native_histogram_buckets =
                guard.active_native_histogram_buckets.saturating_sub(b);
            for &idx in &entry.matches {
                if let Some(v) = guard.active_matching_native_histograms.get_mut(idx) {
                    *v = v.saturating_sub(1);
                }
                if let Some(v) = guard.active_matching_native_histogram_buckets.get_mut(idx) {
                    *v = v.saturating_sub(b);
                }
            }
        }
    }

    fn purge(&self, keep_until_ns: i64) {
        let oldest = self.oldest_entry_ns.load(Ordering::Relaxed);
        if oldest != 0 && oldest >= keep_until_ns {
            return;
        }
        let mut guard = self.inner.write();
        let mut new_oldest = 0i64;
        let mut to_remove = Vec::new();
        for (&ref_id, entry) in &guard.refs {
            let ts = entry.nanos.load(Ordering::Relaxed);
            if ts < keep_until_ns {
                to_remove.push(ref_id);
            } else if new_oldest == 0 || ts < new_oldest {
                new_oldest = ts;
            }
        }
        self.oldest_entry_ns.store(new_oldest, Ordering::Relaxed);
        for ref_id in to_remove {
            if let Some(entry) = guard.refs.remove(&ref_id) {
                let _ = entry.is_deleted();
                guard.active = guard.active.saturating_sub(1);
                if entry.is_otlp() {
                    guard.active_otlp = guard.active_otlp.saturating_sub(1);
                }
                for &idx in &entry.matches {
                    if let Some(v) = guard.active_matching.get_mut(idx) {
                        *v = v.saturating_sub(1);
                    }
                }
                if entry.num_native_histogram_buckets >= 0 {
                    let b = entry.num_native_histogram_buckets as u32;
                    guard.active_native_histograms =
                        guard.active_native_histograms.saturating_sub(1);
                    guard.active_native_histogram_buckets =
                        guard.active_native_histogram_buckets.saturating_sub(b);
                    for &idx in &entry.matches {
                        if let Some(v) = guard.active_matching_native_histograms.get_mut(idx) {
                            *v = v.saturating_sub(1);
                        }
                        if let Some(v) = guard.active_matching_native_histogram_buckets.get_mut(idx)
                        {
                            *v = v.saturating_sub(b);
                        }
                    }
                }
            }
        }
    }

    fn clear(&self) {
        let mut guard = self.inner.write();
        guard.refs.clear();
        guard.active = 0;
        guard.active_otlp = 0;
        guard.active_matching.fill(0);
        guard.active_native_histograms = 0;
        guard.active_matching_native_histograms.fill(0);
        guard.active_native_histogram_buckets = 0;
        guard.active_matching_native_histogram_buckets.fill(0);
        self.oldest_entry_ns.store(0, Ordering::Relaxed);
    }
}

/// 128-stripe active series & native histogram bucket tracker for a single tenant.
pub struct ActiveSeries {
    stripes: Vec<SeriesStripe>,
    deleted: DeletedSeries,
    matchers: RwLock<Vec<CustomTrackerMatcher>>,
    timeout: Duration,
}

impl ActiveSeries {
    pub fn new(matchers: Vec<CustomTrackerMatcher>, timeout: Duration) -> Self {
        let mut stripes = Vec::with_capacity(NUM_STRIPES);
        for _ in 0..NUM_STRIPES {
            stripes.push(SeriesStripe::new(matchers.len()));
        }
        Self {
            stripes,
            deleted: DeletedSeries::default(),
            matchers: RwLock::new(matchers),
            timeout,
        }
    }

    pub fn current_matcher_names(&self) -> Vec<String> {
        self.matchers
            .read()
            .iter()
            .map(|m| m.name.clone())
            .collect()
    }

    pub fn reload_matchers(&self, new_matchers: Vec<CustomTrackerMatcher>, head: &mimir_tsdb::head::Head) {
        let mut m_guard = self.matchers.write();
        *m_guard = new_matchers;
        let num_m = m_guard.len();
        for stripe in &self.stripes {
            let mut inner = stripe.inner.write();
            inner.active_matching = vec![0; num_m];
            inner.active_matching_native_histograms = vec![0; num_m];
            inner.active_matching_native_histogram_buckets = vec![0; num_m];

            let mut recomputed = Vec::new();
            for (&ref_id, entry) in &inner.refs {
                let mut matches = Vec::new();
                if let Some(lbls) = head.series_labels(ref_id) {
                    for (idx, cm) in m_guard.iter().enumerate() {
                        if cm.matchers.iter().all(|m| m.matches(lbls.get(&m.name))) {
                            matches.push(idx);
                        }
                    }
                }
                recomputed.push((ref_id, matches, entry.num_native_histogram_buckets));
            }
            for (ref_id, matches, buckets) in recomputed {
                for &idx in &matches {
                    inner.active_matching[idx] += 1;
                    if buckets >= 0 {
                        inner.active_matching_native_histograms[idx] += 1;
                        inner.active_matching_native_histogram_buckets[idx] += buckets as u32;
                    }
                }
                if let Some(e) = inner.refs.get_mut(&ref_id) {
                    e.matches = matches;
                }
            }
        }
    }

    pub fn update_series(
        &self,
        pairs: &[(&str, &str)],
        hash: u64,
        ref_id: u64,
        now: SystemTime,
        num_native_histogram_buckets: i64,
        is_otlp: bool,
    ) {
        let now_ns = now
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        let matchers = self.matchers.read();
        let stripe_id = (ref_id as usize) % NUM_STRIPES;
        let created = self.stripes[stripe_id].update_series_timestamp(
            now_ns,
            pairs,
            ref_id,
            num_native_histogram_buckets,
            is_otlp,
            &matchers,
        );
        if created {
            if let Some(deleted_ref) = self.deleted.find_and_remove(hash) {
                self.stripes[(deleted_ref as usize) % NUM_STRIPES].remove(deleted_ref);
            }
        }
    }

    pub fn post_deletion(&self, deleted: &[(u64, Labels)]) {
        for (ref_id, lbls) in deleted {
            let stripe_id = (*ref_id as usize) % NUM_STRIPES;
            if self.stripes[stripe_id].contains_ref(*ref_id) {
                self.stripes[stripe_id].mark_deleted(*ref_id, lbls.clone(), &self.deleted);
            }
        }
    }

    pub fn purge(&self, now: SystemTime) {
        let keep_until = now.checked_sub(self.timeout).unwrap_or(UNIX_EPOCH);
        let keep_until_ns = keep_until
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        for stripe in &self.stripes {
            stripe.purge(keep_until_ns);
        }
    }

    pub fn contains_ref(&self, ref_id: u64) -> bool {
        self.stripes[(ref_id as usize) % NUM_STRIPES].contains_ref(ref_id)
    }

    pub fn native_histogram_buckets(&self, ref_id: u64) -> Option<u64> {
        self.stripes[(ref_id as usize) % NUM_STRIPES].native_histogram_buckets(ref_id)
    }

    pub fn active(&self) -> (usize, usize, usize, usize) {
        let mut total = 0usize;
        let mut otlp = 0usize;
        let mut histograms = 0usize;
        let mut buckets = 0usize;
        for stripe in &self.stripes {
            let g = stripe.inner.read();
            total += g.active as usize;
            otlp += g.active_otlp as usize;
            histograms += g.active_native_histograms as usize;
            buckets += g.active_native_histogram_buckets as usize;
        }
        (total, otlp, histograms, buckets)
    }

    pub fn active_with_matchers(
        &self,
    ) -> (
        usize,
        Vec<usize>,
        usize,
        usize,
        Vec<usize>,
        usize,
        Vec<usize>,
    ) {
        let n = self.matchers.read().len();
        let mut total = 0usize;
        let mut matching = vec![0usize; n];
        let mut otlp = 0usize;
        let mut histograms = 0usize;
        let mut matching_histograms = vec![0usize; n];
        let mut buckets = 0usize;
        let mut matching_buckets = vec![0usize; n];
        for stripe in &self.stripes {
            let g = stripe.inner.read();
            total += g.active as usize;
            otlp += g.active_otlp as usize;
            histograms += g.active_native_histograms as usize;
            buckets += g.active_native_histogram_buckets as usize;
            for (i, v) in g.active_matching.iter().enumerate() {
                if i < n {
                    matching[i] += *v as usize;
                }
            }
            for (i, v) in g.active_matching_native_histograms.iter().enumerate() {
                if i < n {
                    matching_histograms[i] += *v as usize;
                }
            }
            for (i, v) in g.active_matching_native_histogram_buckets.iter().enumerate() {
                if i < n {
                    matching_buckets[i] += *v as usize;
                }
            }
        }
        (
            total,
            matching,
            otlp,
            histograms,
            matching_histograms,
            buckets,
            matching_buckets,
        )
    }

    pub fn delete(&self, ref_id: u64) {
        self.stripes[(ref_id as usize) % NUM_STRIPES].remove(ref_id);
    }

    pub fn clear(&self) {
        for stripe in &self.stripes {
            stripe.clear();
        }
        self.deleted.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_series_tracks_otlp_histograms_and_purges() {
        let mut map = std::collections::BTreeMap::new();
        map.insert("api_only".to_string(), "{job=\"api\"}".to_string());
        let matchers = parse_custom_tracker_matchers(&map);
        let tracker = ActiveSeries::new(matchers, Duration::from_secs(60));

        let t0 = UNIX_EPOCH + Duration::from_secs(1000);
        tracker.update_series(&[("__name__", "m1"), ("job", "api")], 101, 1, t0, -1, false);
        tracker.update_series(&[("__name__", "m2"), ("job", "api")], 102, 2, t0, 12, true);
        tracker.update_series(&[("__name__", "m3"), ("job", "db")], 103, 3, t0, -1, true);

        let (total, matching, otlp, hists, m_hists, buckets, m_buckets) =
            tracker.active_with_matchers();
        assert_eq!(total, 3);
        assert_eq!(matching, vec![2]);
        assert_eq!(otlp, 2);
        assert_eq!(hists, 1);
        assert_eq!(m_hists, vec![1]);
        assert_eq!(buckets, 12);
        assert_eq!(m_buckets, vec![12]);
        assert_eq!(tracker.native_histogram_buckets(2), Some(12));
        assert_eq!(tracker.native_histogram_buckets(1), None);

        // Purge after timeout removes all
        tracker.purge(t0 + Duration::from_secs(120));
        assert_eq!(tracker.active(), (0, 0, 0, 0));
    }
}
