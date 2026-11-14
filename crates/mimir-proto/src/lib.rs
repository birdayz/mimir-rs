//! Wire protocols, protobuf codecs, RW2 symbol tables, and FNV-1 sharding.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/mimirpb/*
//! - github.com/grafana/mimir/pkg/ingester/client/ingester.proto
//! - github.com/grafana/dskit/ring/ring.proto
//! - github.com/grafana/dskit/ring/partition_ring_desc.proto
//! - github.com/grafana/dskit/kv/memberlist/kv.proto
//! - github.com/grafana/dskit/httpgrpc/httpgrpc.proto
//! - github.com/grafana/dskit/grpcutil/error_details.proto
