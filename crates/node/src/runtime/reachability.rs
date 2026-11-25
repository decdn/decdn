//! The node's bring-up reachability report (ADR 001 § Node Discovery).
//!
//! A node on a host with a public IP (a dedicated or cloud server), or with a
//! configured `network.external_addrs` address (a port forward or a load
//! balancer), takes direct inbound connections. A node with neither (a home
//! connection behind NAT) takes inbound through the relay and hole-punching,
//! which is expected. The report states which case applies, sets
//! `decdn_node_public_address`, and warns only when a public node's registry
//! `multiaddrs` do not carry one of its public addresses, since peers dial the
//! registered address first. When `network.external_addrs` is set, its
//! addresses are the public ones and the host's own addresses do not count.
//! When the node has a public address, the report names its source (`route`
//! or `configured`); the node trusts a configured address from config and
//! does not probe it.

use std::net::SocketAddr;

use decdn_common::net::{public_quic_addrs, quic_multiaddr, route_public_ips};

use crate::metrics::Metrics;

/// What the node found about its own reachability.
#[derive(Debug, PartialEq, Eq)]
enum Reachability {
    /// No public address on the default route and no configured address.
    BehindNat,
    /// Every public address is registered.
    PublicRegistered,
    /// Public, but these multiaddrs are missing from the registry record.
    PublicUnregistered { missing: Vec<String> },
}

/// Classify `public` (the QUIC addresses a remote peer can dial, each with its
/// own port) against `registered` (the node's registry `multiaddrs`).
fn classify(public: &[SocketAddr], registered: &[SocketAddr]) -> Reachability {
    if public.is_empty() {
        return Reachability::BehindNat;
    }
    let missing: Vec<String> = public
        .iter()
        .filter(|a| !registered.contains(a))
        .map(|a| quic_multiaddr(a.ip(), a.port()))
        .collect();
    if missing.is_empty() {
        Reachability::PublicRegistered
    } else {
        Reachability::PublicUnregistered { missing }
    }
}

/// Where the public addresses come from: `"configured"` when
/// `network.external_addrs` is set (the node does not probe these), else
/// `"route"` (the host's route-public IPs). This matches
/// [`public_quic_addrs`], which uses exactly the configured list whenever it is
/// non-empty.
const fn addr_source(external_addrs: &[SocketAddr]) -> &'static str {
    if external_addrs.is_empty() {
        "route"
    } else {
        "configured"
    }
}

/// Log the node's reachability once and set `decdn_node_public_address`.
/// The public addresses are each of `external_addrs` (`network.external_addrs`)
/// on its own port when there are any, else the host's route-public IPs on
/// `bind_port`.
/// `registered` is this node's own decoded registry `multiaddrs`.
pub(super) fn report(
    metrics: &Metrics,
    bind_port: u16,
    external_addrs: &[SocketAddr],
    registered: &[SocketAddr],
) {
    let public = public_quic_addrs(&route_public_ips(), bind_port, external_addrs);
    metrics.node_public_address(!public.is_empty());
    let source = addr_source(external_addrs);
    match classify(&public, registered) {
        Reachability::BehindNat => tracing::info!(
            bind_port,
            "no public address on the default route and no network.external_addrs: the node \
             is behind NAT, and inbound peers reach it through the relay and hole-punching. \
             To take direct inbound, forward UDP {bind_port} to this host, set the forwarded \
             address in network.external_addrs, restart, and register it with \
             `decdn node update-multiaddrs`"
        ),
        Reachability::PublicRegistered => tracing::info!(
            public = ?public,
            source,
            "public address registered: peers dial this node directly"
        ),
        Reachability::PublicUnregistered { missing } => tracing::warn!(
            missing = ?missing,
            source,
            "the registry multiaddrs do not carry this node's public address, so peers that \
             dial from the registry may go through the relay. Register it with \
             `decdn node update-multiaddrs --multiaddr <addr>` for each missing address"
        ),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
