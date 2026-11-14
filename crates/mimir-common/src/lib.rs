//! Shared primitives: multi-tenancy, label hashing, service lifecycle, logging, and limiters.
//!
//! Ported from:
//! - github.com/grafana/dskit/services
//! - github.com/grafana/dskit/tenant
//! - github.com/grafana/dskit/user
//! - github.com/grafana/dskit/backoff
//! - github.com/grafana/dskit/limiter
//! - github.com/grafana/mimir/pkg/util/log
//! - github.com/grafana/mimir/pkg/util/globalerror
//! - github.com/grafana/mimir/pkg/util/activitytracker
//! - github.com/grafana/mimir/pkg/util/shutdownmarker
//! - github.com/grafana/mimir/pkg/util/reactivelimiter
//! - github.com/grafana/mimir/pkg/distributor/validate.go
//! - github.com/prometheus/prometheus/model/labels

pub mod activitytracker;
pub mod backoff;
pub mod errors;
pub mod labels_hash;
pub mod limiter;
pub mod logging;
pub mod services;
pub mod shutdownmarker;
pub mod tenant;

#[doc(hidden)]
pub fn block_on_for_tests<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime for tests")
        .block_on(future)
}
