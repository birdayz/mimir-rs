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
//! - github.com/grafana/dskit/dns/miekgdns/resolver.go

mod error;
pub mod godns;
pub mod miekgdns;
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
