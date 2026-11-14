//! Mimir binary entrypoint, module DAG manager, HTTP + gRPC server, and admin endpoints.
//!
//! Ported from:
//! - github.com/grafana/mimir/cmd/mimir/main.go
//! - github.com/grafana/mimir/pkg/mimir/mimir.go
//! - github.com/grafana/mimir/pkg/mimir/modules.go
//! - github.com/grafana/mimir/pkg/api/*
//! - github.com/grafana/dskit/server/*
