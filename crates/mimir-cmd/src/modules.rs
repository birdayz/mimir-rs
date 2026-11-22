//! Module service states as seen by the `dskit/services.Manager` that Mimir builds from its
//! module graph: used by `/ready`, `/services`, the gRPC health check and failure propagation.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/mimir/mimir.go (`Run`, `readyHandler`, `servicesHandler`)
//! - github.com/grafana/dskit/services/manager.go (`IsHealthy`)
//! - github.com/grafana/dskit/grpcutil/health_check.go (`WithManager`)

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};

use mimir_common::logging::{LogLevel, Logger};
use mimir_common::services::{
    stop_and_await_terminated, ManagedBasicService, RunningFn, Service, ServiceContext,
    ServiceError, StartingFn, State, StoppingFn,
};

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

// ---------------------------------------------------------------------------------------------
// dskit/modules: ModuleManager & ModuleService (modules.go, module_service.go)
// ---------------------------------------------------------------------------------------------

pub const ERR_STOP_PROCESS_MSG: &str = "stop process";

pub type ModuleInitFn =
    Arc<dyn Fn() -> Result<Option<Arc<dyn Service>>, anyhow::Error> + Send + Sync>;

pub type DepsMapFn = Arc<dyn Fn(&str) -> HashMap<String, Arc<dyn Service>> + Send + Sync>;

/// Metadata for a registered module (`dskit/modules/modules.go:module`).
#[derive(Clone)]
pub struct ModuleDef {
    pub deps: Vec<String>,
    pub init_fn: Option<ModuleInitFn>,
    pub user_visible: bool,
    pub targetable: bool,
}

/// Functional option marking a module invisible and non-targetable (`UserInvisibleModule`).
pub fn user_invisible_module(m: &mut ModuleDef) {
    m.user_visible = false;
    m.targetable = false;
}

/// Functional option marking a module invisible but still targetable (`UserInvisibleTargetableModule`).
pub fn user_invisible_targetable_module(m: &mut ModuleDef) {
    m.user_visible = false;
    m.targetable = true;
}

/// Dependency-ordered module registry and service wrapper (`dskit/modules.Manager`).
pub struct ModuleManager {
    modules: HashMap<String, ModuleDef>,
    logger: Arc<dyn Logger>,
}

impl ModuleManager {
    pub fn new(logger: Arc<dyn Logger>) -> Self {
        Self {
            modules: HashMap::new(),
            logger,
        }
    }

    pub fn register_module(
        &mut self,
        name: &str,
        init_fn: Option<ModuleInitFn>,
        options: &[fn(&mut ModuleDef)],
    ) {
        let mut def = ModuleDef {
            deps: Vec::new(),
            init_fn,
            user_visible: true,
            targetable: true,
        };
        for opt in options {
            opt(&mut def);
        }
        self.modules.insert(name.to_string(), def);
    }

    pub fn get_module(&self, name: &str) -> Option<&ModuleDef> {
        self.modules.get(name)
    }

    pub fn add_dependency(&mut self, name: &str, depends_on: &[&str]) -> Result<(), String> {
        if !self.modules.contains_key(name) {
            return Err(format!("no such module: {name}"));
        }
        for &new_dep in depends_on {
            if !self.modules.contains_key(new_dep) {
                return Err(format!("no such module: {new_dep}"));
            }
            if new_dep == name || self.dependencies_for_module(new_dep).iter().any(|d| d == name) {
                return Err(format!(
                    "found a circular dependency: {new_dep} depends on {name}"
                ));
            }
        }
        let m = self.modules.get_mut(name).expect("checked above");
        for &dep in depends_on {
            m.deps.push(dep.to_string());
        }
        Ok(())
    }

    pub fn init_module_services(
        &self,
        modules: &[&str],
    ) -> Result<ModuleServicesResult, String> {
        let services_map: Arc<RwLock<HashMap<String, Arc<dyn Service>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let mut init_map: HashMap<String, bool> = HashMap::new();
        let mut stop_deps_map: HashMap<String, Vec<String>> = HashMap::new();

        for &module in modules {
            self.init_module(module, &mut init_map, &services_map, &mut stop_deps_map)?;
        }

        let map = services_map
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Ok(ModuleServicesResult {
            services: map,
            stop_deps_by_module: stop_deps_map,
        })
    }

    fn init_module(
        &self,
        name: &str,
        init_map: &mut HashMap<String, bool>,
        services_map: &Arc<RwLock<HashMap<String, Arc<dyn Service>>>>,
        stop_deps_map: &mut HashMap<String, Vec<String>>,
    ) -> Result<(), String> {
        if !self.modules.contains_key(name) {
            return Err(format!("unrecognised module name: {name}"));
        }

        let mut deps = self.ordered_deps(name);
        deps.push(name.to_string());

        for n in deps {
            if init_map.get(&n).copied().unwrap_or(false) {
                continue;
            }
            let mod_def = &self.modules[&n];
            if let Some(ref init_fn) = mod_def.init_fn {
                let s = init_fn().map_err(|e| format!("error initialising module: {n}: {e}"))?;
                if let Some(svc) = s {
                    let start_dep_names = self.dependencies_for_module(&n);
                    let stop_dep_names = self.inverse_dependencies_for_module(&n);
                    let sm_start = Arc::clone(services_map);
                    let start_deps_fn: DepsMapFn = Arc::new(move |_| {
                        let guard = sm_start
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        start_dep_names
                            .iter()
                            .filter_map(|d| guard.get(d).map(|v| (d.clone(), Arc::clone(v))))
                            .collect()
                    });
                    let sm_stop = Arc::clone(services_map);
                    let stop_deps_fn: DepsMapFn = Arc::new(move |_| {
                        let guard = sm_stop
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        stop_dep_names
                            .iter()
                            .filter_map(|d| guard.get(d).map(|v| (d.clone(), Arc::clone(v))))
                            .collect()
                    });
                    let wrapped = new_module_service(
                        &n,
                        Arc::clone(&self.logger),
                        svc,
                        start_deps_fn,
                        stop_deps_fn,
                    );
                    services_map
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(n.clone(), wrapped);
                }
            }
            init_map.insert(n, true);
        }

        // Snapshot stop dependencies present in services_map for each initialized service.
        let guard = services_map
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for k in guard.keys() {
            let mut inv: Vec<String> = self
                .inverse_dependencies_for_module(k)
                .into_iter()
                .filter(|d| guard.contains_key(d))
                .collect();
            inv.sort();
            stop_deps_map.insert(k.clone(), inv);
        }

        Ok(())
    }

    pub fn user_visible_module_names(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .modules
            .iter()
            .filter(|&(_, v)| v.user_visible)
            .map(|(k, _)| k.clone())
            .collect();
        out.sort();
        out
    }

    pub fn is_user_visible_module(&self, mod_name: &str) -> bool {
        self.modules.get(mod_name).is_some_and(|m| m.user_visible)
    }

    pub fn is_targetable_module(&self, mod_name: &str) -> bool {
        self.modules.get(mod_name).is_some_and(|m| m.targetable)
    }

    pub fn is_module_registered(&self, mod_name: &str) -> bool {
        self.modules.contains_key(mod_name)
    }

    fn list_deps(&self, mod_name: &str) -> Vec<String> {
        let Some(m) = self.modules.get(mod_name) else {
            return Vec::new();
        };
        let mut deps = m.deps.clone();
        for d in &m.deps {
            deps.extend(self.list_deps(d));
        }
        deps
    }

    fn ordered_deps(&self, mod_name: &str) -> Vec<String> {
        let deps = self.list_deps(mod_name);
        let mut uniq: BTreeMap<String, bool> = deps.into_iter().map(|d| (d, false)).collect();
        let mut result = Vec::with_capacity(uniq.len());

        while result.len() < uniq.len() {
            let names: Vec<String> = uniq.keys().cloned().collect();
            for name in names {
                if uniq[&name] {
                    continue;
                }
                let all_added = self.modules[&name]
                    .deps
                    .iter()
                    .all(|dep| uniq.get(dep).copied().unwrap_or(false));
                if all_added {
                    uniq.insert(name.clone(), true);
                    result.push(name);
                }
            }
        }
        result
    }

    pub fn inverse_dependencies_for_module(&self, mod_name: &str) -> Vec<String> {
        let mut result = Vec::new();
        for n in self.modules.keys() {
            if self.dependencies_for_module(n).iter().any(|d| d == mod_name) {
                result.push(n.clone());
            }
        }
        result.sort();
        result
    }

    pub fn dependencies_for_module(&self, module: &str) -> Vec<String> {
        let dedup: BTreeSet<String> = self.list_deps(module).into_iter().collect();
        dedup.into_iter().collect()
    }
}

pub struct ModuleServicesResult {
    pub services: HashMap<String, Arc<dyn Service>>,
    stop_deps_by_module: HashMap<String, Vec<String>>,
}

impl ModuleServicesResult {
    pub fn stop_dependencies_for_module(&self, module: &str) -> Vec<String> {
        self.stop_deps_by_module
            .get(module)
            .cloned()
            .unwrap_or_default()
    }
}

/// Wraps a module `Service` so it awaits start dependencies before starting and stop
/// dependencies before stopping (`dskit/modules.NewModuleService`).
pub fn new_module_service(
    name: &str,
    logger: Arc<dyn Logger>,
    service: Arc<dyn Service>,
    start_deps: DepsMapFn,
    stop_deps: DepsMapFn,
) -> Arc<dyn Service> {
    let mod_name = name.to_string();
    let svc_name = service.service_name();

    let svc_start = Arc::clone(&service);
    let log_start = Arc::clone(&logger);
    let name_start = mod_name.clone();
    let start_fn: StartingFn = Box::new(move |service_ctx: ServiceContext| {
        let svc = Arc::clone(&svc_start);
        let logger = Arc::clone(&log_start);
        let name = name_start.clone();
        let start_deps = Arc::clone(&start_deps);
        Box::pin(async move {
            let deps = start_deps(&name);
            for (m, s) in deps {
                logger.log(
                    LogLevel::Debug,
                    &[
                        ("msg", "module waiting for initialization"),
                        ("module", &name),
                        ("waiting_for", &m),
                    ],
                );
                if let Err(err) = s.await_running_ctx(service_ctx.clone()).await {
                    return Err(ServiceError::failure(format!(
                        "failed to start {name}, because it depends on module {m}, which has failed: {err}"
                    )));
                }
            }

            logger.log(
                LogLevel::Info,
                &[("msg", "starting"), ("module", &name)],
            );
            if let Err(err) = svc.start_async(ServiceContext::background()).await {
                return Err(ServiceError::failure(format!(
                    "error starting module: {name}: {err}"
                )));
            }
            if let Err(err) = svc.await_running_ctx(service_ctx.clone()).await {
                let _ = stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await;
                return Err(ServiceError::failure(format!(
                    "starting module {name}: {err}"
                )));
            }
            if service_ctx.is_canceled() {
                let _ = stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await;
                return Err(ServiceError::failure(format!(
                    "starting module {name}: context canceled"
                )));
            }
            Ok(())
        })
    });

    let svc_run = Arc::clone(&service);
    let run_fn: RunningFn = Box::new(move |service_ctx: ServiceContext| {
        let svc = Arc::clone(&svc_run);
        Box::pin(async move {
            let _ = svc.await_terminated_ctx(service_ctx).await;
            match svc.failure_case() {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    });

    let svc_stop = Arc::clone(&service);
    let log_stop = logger;
    let name_stop = mod_name;
    let stop_fn: StoppingFn = Box::new(move |_err| {
        let svc = Arc::clone(&svc_stop);
        let logger = Arc::clone(&log_stop);
        let name = name_stop.clone();
        let stop_deps = Arc::clone(&stop_deps);
        Box::pin(async move {
            let res = if svc.state() == State::Running {
                for (n, s) in stop_deps(&name) {
                    logger.log(
                        LogLevel::Debug,
                        &[
                            ("msg", "module waiting for"),
                            ("module", &name),
                            ("waiting_for", &n),
                        ],
                    );
                    let _ = s.await_terminated().await;
                }
                logger.log(
                    LogLevel::Debug,
                    &[("msg", "stopping"), ("module", &name)],
                );
                stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await
            } else {
                match svc.failure_case() {
                    Some(e) => Err(e),
                    None => Ok(()),
                }
            };

            match &res {
                Err(e) if e.to_string() != ERR_STOP_PROCESS_MSG => {
                    let err_s = e.to_string();
                    logger.log(
                        LogLevel::Warn,
                        &[
                            ("msg", "module failed with error"),
                            ("module", &name),
                            ("err", &err_s),
                        ],
                    );
                }
                _ => {
                    logger.log(
                        LogLevel::Info,
                        &[("msg", "module stopped"), ("module", &name)],
                    );
                }
            }
            res
        })
    });

    let wrapped = ManagedBasicService::new(Some(start_fn), Some(run_fn), Some(stop_fn));
    if !svc_name.is_empty() {
        wrapped.with_name(svc_name)
    } else {
        wrapped
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
