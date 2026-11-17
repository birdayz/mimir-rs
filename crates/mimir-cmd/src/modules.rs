//! Module service states as seen by the `dskit/services.Manager` that Mimir builds from its
//! module graph: used by `/ready`, `/services`, the gRPC health check and failure propagation.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/mimir/mimir.go (`Run`, `readyHandler`, `servicesHandler`)
//! - github.com/grafana/dskit/services/manager.go (`IsHealthy`)
//! - github.com/grafana/dskit/grpcutil/health_check.go (`WithManager`)

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use mimir_common::services::State;
use std::sync::RwLock;

pub type StateFn = Arc<dyn Fn() -> State + Send + Sync>;

/// A module whose state is driven explicitly by the application (e.g. `server`,
/// `sanity-check`), mirroring the dskit service wrapping it in Go.
#[derive(Default)]
pub struct StateCell(AtomicU8);

fn state_to_u8(s: State) -> u8 {
    match s {
        State::New => 0,
        State::Starting => 1,
        State::Running => 2,
        State::Stopping => 3,
        State::Terminated => 4,
        State::Failed => 5,
    }
}

fn u8_to_state(v: u8) -> State {
    match v {
        1 => State::Starting,
        2 => State::Running,
        3 => State::Stopping,
        4 => State::Terminated,
        5 => State::Failed,
        _ => State::New,
    }
}

impl StateCell {
    pub fn new(s: State) -> Arc<Self> {
        Arc::new(Self(AtomicU8::new(state_to_u8(s))))
    }

    pub fn set(&self, s: State) {
        self.0.store(state_to_u8(s), Ordering::Release);
    }

    pub fn get(&self) -> State {
        u8_to_state(self.0.load(Ordering::Acquire))
    }

    pub fn state_fn(self: &Arc<Self>) -> StateFn {
        let me = Arc::clone(self);
        Arc::new(move || me.get())
    }
}

/// The set of module services started for the configured targets (`Mimir.ServiceMap`).
#[derive(Default)]
pub struct ModuleStates {
    modules: RwLock<Vec<(String, StateFn)>>,
}

impl ModuleStates {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Registers (or replaces) the module `name`.
    pub fn add(&self, name: &str, state: StateFn) {
        let mut m = self.modules.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(e) = m.iter_mut().find(|(n, _)| n == name) {
            e.1 = state;
        } else {
            m.push((name.to_string(), state));
        }
    }

    /// Current `(module, state)` pairs sorted by module name.
    pub fn snapshot(&self) -> Vec<(String, State)> {
        let mut out: Vec<(String, State)> = self
            .modules
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(n, f)| (n.clone(), f()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// `services.Manager.IsHealthy`: all services are Running.
    pub fn is_healthy(&self) -> bool {
        self.snapshot().iter().all(|(_, s)| *s == State::Running)
    }

    /// `grpcutil.WithManager`: healthy once every service has been started (none New,
    /// Starting or Failed) and until all services are terminated (at least one Running or
    /// Stopping).
    pub fn is_serving(&self) -> bool {
        let snap = self.snapshot();
        if snap
            .iter()
            .any(|(_, s)| matches!(s, State::New | State::Starting | State::Failed))
        {
            return false;
        }
        snap.iter()
            .any(|(_, s)| matches!(s, State::Running | State::Stopping))
    }

    /// Modules that are not Running, as listed by `readyHandler`. Every module service is
    /// started by the service manager at once, so a not yet started one is reported as
    /// Starting.
    pub fn not_running(&self) -> Vec<String> {
        self.snapshot()
            .into_iter()
            .filter_map(|(n, s)| {
                let s = if s == State::New { State::Starting } else { s };
                (s != State::Running).then(|| format!("{n}: {s}"))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_aggregation_matches_dskit() {
        let m = ModuleStates::new();
        let server = StateCell::new(State::Running);
        let ingester = StateCell::new(State::Starting);
        m.add("server", server.state_fn());
        m.add("ingester-service", ingester.state_fn());
        assert!(!m.is_healthy());
        assert!(!m.is_serving());
        assert_eq!(m.not_running(), vec!["ingester-service: Starting".to_string()]);

        ingester.set(State::Running);
        assert!(m.is_healthy());
        assert!(m.is_serving());

        // `/ingester/shutdown` terminates only the ingester service: not ready, but the gRPC
        // health check keeps reporting SERVING (other modules are still running).
        ingester.set(State::Terminated);
        assert!(!m.is_healthy());
        assert!(m.is_serving());

        ingester.set(State::Failed);
        assert!(!m.is_serving());

        ingester.set(State::Terminated);
        server.set(State::Terminated);
        assert!(!m.is_serving());
    }
}
