//! Shared primitives: multi-tenancy, label hashing, service lifecycle, logging, and limiters.
//!
//! Ported from:
//! - github.com/grafana/dskit/services
//! - github.com/grafana/dskit/tenant
//! - github.com/grafana/dskit/backoff
//! - github.com/grafana/dskit/limiter
//! - github.com/grafana/dskit/concurrency
//! - github.com/grafana/mimir/pkg/util/log
//! - github.com/grafana/mimir/pkg/util/globalerror
//! - github.com/grafana/mimir/pkg/util/validation
//! - github.com/grafana/mimir/pkg/util/activitytracker
//! - github.com/grafana/mimir/pkg/util/shutdownmarker
//! - github.com/grafana/mimir/pkg/util/workerpool
//! - github.com/grafana/mimir/pkg/util/reactivelimiter
//! - github.com/prometheus/prometheus/model/labels
