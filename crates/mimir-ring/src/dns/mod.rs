//! DNS-based service discovery used by the memberlist KV (`-memberlist.join`)
//! and other address lists that accept `dns+`, `dnssrv+` and `dnssrvnoa+`
//! prefixes.
//!
//! Ported from:
//! - github.com/grafana/dskit/dns/provider.go
//! - github.com/grafana/dskit/dns/resolver.go
//! - github.com/grafana/dskit/dns/godns/resolver.go
//! - github.com/grafana/dskit/multierror/multierror.go (error joining semantics)
//! - net.SplitHostPort / net.JoinHostPort / net.(*Resolver).LookupSRV ordering
//!   from the Go standard library (behaviour relied upon by dskit)
//!
//! The `miekgdns` resolver type (github.com/grafana/dskit/dns/miekgdns) is not
//! ported: selecting it returns an explicit error. Mimir itself only uses
//! `dns.GolangResolverType` (pkg/mimir/modules.go `initMemberlistKV`).

mod error;
pub mod godns;
pub(crate) mod net;
mod provider;
mod resolver;

pub use error::{go_quote, DnsError};
pub use net::{go_ip_string, join_host_port, split_host_port};
pub use provider::{
    get_qtype_name, is_dynamic_node, DnsProvider, Provider, ResolverType, GOLANG_RESOLVER_TYPE,
    MIEKGDNS_RESOLVER_TYPE,
};
pub use resolver::{DnsSd, IpLookupResolver, LookupError, QType, Resolver, Srv, A, SRV, SRV_NO_A};
