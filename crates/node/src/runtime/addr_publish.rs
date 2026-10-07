//! Which of the node's own addresses its pkarr record carries (ADR 001 § Node
//! Discovery).
//!
//! iroh's `PkarrPublisher` publishes only the relay URL by default. A node's
//! publisher uses [`node_publish_filter`] instead: the relay URL plus every
//! globally routable IP address the endpoint knows for itself, so a peer that
//! dials by bare `NodeId` resolves a direct path. Private, loopback,
//! link-local, and shared (CGNAT) addresses stay out of the record: no remote
//! dialer can reach them, and they describe the operator's internal network.

use std::borrow::Cow;

use decdn_common::net::is_publishable;
use iroh::TransportAddr;
use iroh::address_lookup::AddrFilter;

/// The node's pkarr publish filter: relay URLs and globally routable IPs.
pub(super) fn node_publish_filter() -> AddrFilter {
    AddrFilter::new(|addrs| {
        Cow::Owned(
            addrs
                .iter()
                .filter(|a| match a {
                    TransportAddr::Relay(_) => true,
                    TransportAddr::Ip(sock) => is_publishable(sock.ip()),
                    _ => false,
                })
                .cloned()
                .collect(),
        )
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
