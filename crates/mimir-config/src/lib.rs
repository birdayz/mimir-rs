//! CLI flag parsing, strict YAML config loading, env expansion, common inheritance, and tenant overrides.
//!
//! Ported from:
//! - github.com/grafana/mimir/cmd/mimir/main.go
//! - github.com/grafana/mimir/pkg/mimir/mimir.go
//! - github.com/grafana/mimir/pkg/ingester/ingester.go
//! - github.com/grafana/mimir/pkg/storage/tsdb/config.go
//! - github.com/grafana/mimir/pkg/storage/bucket/config.go
//! - github.com/grafana/mimir/pkg/storage/ingest/config.go
//! - github.com/grafana/mimir/pkg/util/validation/limits.go
//! - github.com/grafana/mimir/pkg/util/runtimeconfig
