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
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn ip(s: &str) -> TransportAddr {
        TransportAddr::Ip(s.parse::<SocketAddr>().unwrap())
    }

    #[test]
    fn keeps_relay_and_public_ips_drops_the_rest() {
        let relay = TransportAddr::Relay("https://relay.example./".parse().unwrap());
        let input = vec![
            relay.clone(),
            ip("8.8.8.8:4433"),
            ip("[2606:4700::1111]:4433"),
            ip("10.0.0.5:4433"),
            ip("192.168.1.2:4433"),
            ip("127.0.0.1:4433"),
            ip("100.64.0.1:4433"),
            ip("[fd00::1]:4433"),
            ip("[fe80::1]:4433"),
            ip("[::ffff:10.0.0.1]:4433"),
        ];

        let kept = node_publish_filter().apply(&input).into_owned();

        assert_eq!(
            kept,
            vec![relay, ip("8.8.8.8:4433"), ip("[2606:4700::1111]:4433")]
        );
    }
}
