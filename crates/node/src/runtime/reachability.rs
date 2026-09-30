//! The node's bring-up reachability report (ADR 001 § Node Discovery).
//!
//! A node on a host with a public IP (a dedicated or cloud server) takes direct
//! inbound connections; a node behind NAT (a home connection) takes inbound
//! through the relay and hole-punching, which is expected. The report states
//! which case applies, sets `decdn_node_public_address`, and warns only when a
//! public node's registry `multiaddrs` do not carry its public address, since
//! peers dial the registered address first.

use std::net::{IpAddr, SocketAddr};

use decdn_common::net::{quic_multiaddr, route_public_ips};

use crate::metrics::Metrics;

/// What the node found about its own reachability.
#[derive(Debug, PartialEq, Eq)]
enum Reachability {
    /// No public address on the default route.
    BehindNat,
    /// Every public address is registered.
    PublicRegistered,
    /// Public, but these multiaddrs are missing from the registry record.
    PublicUnregistered { missing: Vec<String> },
}

fn classify(public: &[IpAddr], bind_port: u16, registered: &[SocketAddr]) -> Reachability {
    if public.is_empty() {
        return Reachability::BehindNat;
    }
    let missing: Vec<String> = public
        .iter()
        .filter(|ip| !registered.contains(&SocketAddr::new(**ip, bind_port)))
        .map(|ip| quic_multiaddr(*ip, bind_port))
        .collect();
    if missing.is_empty() {
        Reachability::PublicRegistered
    } else {
        Reachability::PublicUnregistered { missing }
    }
}

/// Log the node's reachability once and set `decdn_node_public_address`.
/// `registered` is this node's own decoded registry `multiaddrs`.
pub(super) fn report(metrics: &Metrics, bind_port: u16, registered: &[SocketAddr]) {
    let public = route_public_ips();
    metrics.node_public_address(!public.is_empty());
    match classify(&public, bind_port, registered) {
        Reachability::BehindNat => tracing::info!(
            bind_port,
            "no public address on the default route: the node is behind NAT, and inbound \
             peers reach it through the relay and hole-punching. To take direct inbound, \
             forward UDP {bind_port} to this host and register the forwarded address with \
             `decdn node update-multiaddrs`"
        ),
        Reachability::PublicRegistered => tracing::info!(
            public = ?public,
            bind_port,
            "public address registered: peers dial this node directly"
        ),
        Reachability::PublicUnregistered { missing } => tracing::warn!(
            missing = ?missing,
            "the registry multiaddrs do not carry this node's public address, so peers that \
             dial from the registry may go through the relay. Register it with \
             `decdn node update-multiaddrs --multiaddr <addr>` for each missing address"
        ),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn sock(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn no_public_address_is_behind_nat() {
        assert_eq!(classify(&[], 4433, &[]), Reachability::BehindNat);
    }

    #[test]
    fn public_and_registered_on_the_bind_port() {
        assert_eq!(
            classify(&[ip("8.8.8.8")], 4433, &[sock("8.8.8.8:4433")]),
            Reachability::PublicRegistered
        );
    }

    #[test]
    fn public_but_unregistered_or_on_another_port_names_the_missing_multiaddr() {
        let want = Reachability::PublicUnregistered {
            missing: vec!["/ip4/8.8.8.8/udp/4433/quic-v1".to_string()],
        };
        assert_eq!(classify(&[ip("8.8.8.8")], 4433, &[]), want);
        assert_eq!(
            classify(&[ip("8.8.8.8")], 4433, &[sock("8.8.8.8:5000")]),
            want
        );
    }
}
