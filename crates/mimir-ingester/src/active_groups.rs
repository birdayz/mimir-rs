//! Active metric groups tracking for `-validation.separate-metrics-group-label`.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/util/active_groups.go
//! - github.com/grafana/mimir/pkg/util/validation/separate_metrics.go

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};

fn unix_nanos(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i64,
        Err(e) => -(e.duration().as_nanos() as i64),
    }
}

/// Returns the group label value for a write request: the value of the tenant's
/// `separate_metrics_group_label` in the first series of the request, or "" (Go: `validation.GroupLabel`).
pub fn group_label<'a>(
    group_label_name: &str,
    first_series_labels: Option<impl IntoIterator<Item = (&'a str, &'a str)>>,
) -> String {
    if group_label_name.is_empty() {
        return String::new();
    }
    let Some(labels) = first_series_labels else {
        return String::new();
    };
    for (n, v) in labels {
        if n == group_label_name {
            return v.to_string();
        }
    }
    String::new()
}

/// Ported from `util.ActiveGroups`.
pub struct ActiveGroups {
    timestamps_per_user: RwLock<HashMap<String, HashMap<String, Arc<AtomicI64>>>>,
    max_groups_per_user: usize,
}

impl ActiveGroups {
    pub fn new(max_groups_per_user: usize) -> Self {
        Self {
            timestamps_per_user: RwLock::new(HashMap::new()),
            max_groups_per_user,
        }
    }

    pub fn update_group_timestamp_for_user(&self, user_id: &str, group: &str, now: SystemTime) {
        let ts = unix_nanos(now);
        if let Some(g) = self
            .timestamps_per_user
            .read()
            .get(user_id)
            .and_then(|m| m.get(group))
        {
            g.store(ts, Ordering::Relaxed);
            return;
        }
        let mut w = self.timestamps_per_user.write();
        let groups = w.entry(user_id.to_string()).or_default();
        match groups.get(group) {
            Some(g) => g.store(ts, Ordering::Relaxed),
            None => {
                groups.insert(group.to_string(), Arc::new(AtomicI64::new(ts)));
            }
        }
    }

    pub fn purge_inactive_groups_for_user(&self, user_id: &str, deadline: i64) -> Vec<String> {
        let inactive: Vec<String> = match self.timestamps_per_user.read().get(user_id) {
            Some(groups) => groups
                .iter()
                .filter(|(_, ts)| ts.load(Ordering::Relaxed) <= deadline)
                .map(|(g, _)| g.clone())
                .collect(),
            None => return Vec::new(),
        };
        if inactive.is_empty() {
            return Vec::new();
        }
        let mut w = self.timestamps_per_user.write();
        let Some(groups) = w.get_mut(user_id) else {
            return Vec::new();
        };
        let mut removed = Vec::with_capacity(inactive.len());
        for g in inactive {
            if groups
                .get(&g)
                .is_some_and(|ts| ts.load(Ordering::Relaxed) <= deadline)
            {
                groups.remove(&g);
                removed.push(g);
            }
        }
        removed
    }

    pub fn purge_inactive_groups(
        &self,
        inactive_timeout: Duration,
        now: SystemTime,
        cleanup: &dyn Fn(&str, &str),
    ) {
        let users: Vec<String> = self.timestamps_per_user.read().keys().cloned().collect();
        let deadline = unix_nanos(now) - inactive_timeout.as_nanos() as i64;
        for user in users {
            for group in self.purge_inactive_groups_for_user(&user, deadline) {
                cleanup(&user, &group);
            }
        }
    }

    pub fn active_group_limit_exceeded(&self, user_id: &str, group: &str) -> bool {
        let r = self.timestamps_per_user.read();
        let groups = r.get(user_id);
        let contains = groups.is_some_and(|g| g.contains_key(group));
        !contains && groups.map_or(0, HashMap::len) >= self.max_groups_per_user
    }
}

type CleanupFn = Box<dyn Fn(&str, &str) + Send + Sync>;

/// Ported from `util.ActiveGroupsCleanupService` (timer service running every `cleanup_interval`).
pub struct ActiveGroupsCleanupService {
    active_groups: ActiveGroups,
    cleanup_fns: Mutex<Vec<CleanupFn>>,
    inactive_timeout: Duration,
    cleanup_interval: Duration,
}

impl ActiveGroupsCleanupService {
    pub fn new(
        cleanup_interval: Duration,
        inactive_timeout: Duration,
        max_groups_per_user: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            active_groups: ActiveGroups::new(max_groups_per_user),
            cleanup_fns: Mutex::new(Vec::new()),
            inactive_timeout,
            cleanup_interval,
        })
    }

    pub fn cleanup_interval(&self) -> Duration {
        self.cleanup_interval
    }

    /// Registers a cleanup function called with `(user, group)` for each purged group.
    pub fn register(&self, f: CleanupFn) {
        self.cleanup_fns.lock().push(f);
    }

    /// Records activity for `group` and returns the group to use in metrics: `"other"` once the
    /// per-tenant number of groups is exceeded. Empty groups are not tracked.
    pub fn update_active_group_timestamp(
        &self,
        user: &str,
        group: String,
        now: SystemTime,
    ) -> String {
        if group.is_empty() {
            return group;
        }
        let group = if self.active_groups.active_group_limit_exceeded(user, &group) {
            "other".to_string()
        } else {
            group
        };
        self.active_groups
            .update_group_timestamp_for_user(user, &group, now);
        group
    }

    /// One cleanup iteration.
    pub fn iteration(&self, now: SystemTime) {
        let fns = self.cleanup_fns.lock();
        self.active_groups
            .purge_inactive_groups(self.inactive_timeout, now, &|u, g| {
                for f in fns.iter() {
                    f(u, g);
                }
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_GROUPS_PER_USER: usize = 10;

    fn ts(nanos: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_nanos(nanos)
    }

    fn sorted(mut v: Vec<String>) -> Vec<String> {
        v.sort();
        v
    }

    // Upstream: pkg/util/active_groups_test.go::TestActiveGroups
    #[test]
    fn active_groups() {
        let ag = ActiveGroups::new(MAX_GROUPS_PER_USER);
        ag.update_group_timestamp_for_user("user1", "group1", ts(10));
        ag.update_group_timestamp_for_user("user1", "group2", ts(15));
        ag.update_group_timestamp_for_user("user1", "group3", ts(20));
        ag.update_group_timestamp_for_user("user2", "group4", ts(10));
        ag.update_group_timestamp_for_user("user2", "group5", ts(15));
        ag.update_group_timestamp_for_user("user2", "group6", ts(20));

        assert!(ag.purge_inactive_groups_for_user("user1", 5).is_empty());
        assert_eq!(
            sorted(ag.purge_inactive_groups_for_user("user1", 16)),
            vec!["group1".to_string(), "group2".to_string()]
        );
        assert!(ag.purge_inactive_groups_for_user("user2", 5).is_empty());
        assert_eq!(
            ag.purge_inactive_groups_for_user("user2", 11),
            vec!["group4".to_string()]
        );

        ag.update_group_timestamp_for_user("user1", "group1", ts(25));
        assert_eq!(
            ag.purge_inactive_groups_for_user("user1", 21),
            vec!["group3".to_string()]
        );
    }

    // Upstream: pkg/util/active_groups_test.go::TestActiveGroupLimitExceeded
    #[test]
    fn active_group_limit_exceeded() {
        let ag = ActiveGroups::new(MAX_GROUPS_PER_USER);
        for i in 0..MAX_GROUPS_PER_USER {
            ag.update_group_timestamp_for_user("user1", &i.to_string(), ts(i as u64));
        }
        assert!(ag.active_group_limit_exceeded("user1", "new-group"));
        assert!(!ag.active_group_limit_exceeded("user1", "0"));
    }

    // Upstream: pkg/util/active_groups_test.go::TestUpdateActiveGroups
    #[test]
    fn update_active_groups() {
        let s = ActiveGroupsCleanupService::new(
            Duration::from_secs(180),
            Duration::from_secs(1200),
            MAX_GROUPS_PER_USER,
        );
        for i in 0..MAX_GROUPS_PER_USER {
            s.update_active_group_timestamp(
                "user1",
                i.to_string(),
                UNIX_EPOCH + Duration::from_secs(1),
            );
        }
        let final_ts = SystemTime::now() - Duration::from_secs(10);
        s.update_active_group_timestamp(
            "user1",
            "new_group_1".into(),
            UNIX_EPOCH + Duration::from_secs(123),
        );
        let group = s.update_active_group_timestamp("user1", "new_group_2".into(), final_ts);
        assert_eq!(group, "other");
        let r = s.active_groups.timestamps_per_user.read();
        let other = r
            .get("user1")
            .and_then(|m| m.get("other"))
            .expect("other tracked");
        assert_eq!(other.load(Ordering::Relaxed), unix_nanos(final_ts));
    }

    #[test]
    fn empty_group_not_tracked_and_cleanup_calls_registered_fns() {
        let svc =
            ActiveGroupsCleanupService::new(Duration::from_secs(180), Duration::from_secs(60), 10);
        assert_eq!(
            svc.update_active_group_timestamp("u", String::new(), SystemTime::now()),
            ""
        );
        let removed = Arc::new(Mutex::new(Vec::new()));
        let r2 = Arc::clone(&removed);
        svc.register(Box::new(move |u, g| {
            r2.lock().push((u.to_string(), g.to_string()))
        }));
        svc.update_active_group_timestamp(
            "u",
            "old".into(),
            SystemTime::now() - Duration::from_secs(120),
        );
        svc.update_active_group_timestamp("u", "new".into(), SystemTime::now());
        svc.iteration(SystemTime::now());
        assert_eq!(*removed.lock(), vec![("u".to_string(), "old".to_string())]);
    }

    // Go: pkg/util/validation/separate_metrics.go::GroupLabel
    #[test]
    fn group_label_from_first_series() {
        let ls = [("__name__", "m"), ("grp", "g1")];
        assert_eq!(group_label("grp", Some(ls.iter().copied())), "g1");
        assert_eq!(group_label("", Some(ls.iter().copied())), "");
        assert_eq!(group_label("missing", Some(ls.iter().copied())), "");
        assert_eq!(group_label("grp", None::<Vec<(&str, &str)>>), "");
    }
}
