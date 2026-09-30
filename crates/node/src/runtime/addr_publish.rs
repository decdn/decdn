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
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

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

/// Whether `ip` is reachable from the public internet. A stable stand-in for
/// the unstable `IpAddr::is_global`.
fn is_publishable(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_publishable_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => is_publishable_v4(v4),
            None => is_publishable_v6(v6),
        },
    }
}

const fn is_publishable_v4(ip: Ipv4Addr) -> bool {
    let [a, b, ..] = ip.octets();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        // 100.64.0.0/10: shared address space (CGNAT).
        || (a == 100 && (b & 0xC0) == 64)
        // 198.18.0.0/15: benchmarking.
        || (a == 198 && (b & 0xFE) == 18)
        // 0.0.0.0/8 and 240.0.0.0/4: "this network" and reserved.
        || a == 0
        || a >= 240)
}

fn is_publishable_v6(ip: Ipv6Addr) -> bool {
    let first = ip.segments().first().copied().unwrap_or(0);
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        // fc00::/7: unique local.
        || (first & 0xFE00) == 0xFC00
        // fe80::/10: link-local unicast.
        || (first & 0xFFC0) == 0xFE80
        // 2001:db8::/32: documentation.
        || (first == 0x2001 && ip.segments().get(1).copied() == Some(0x0DB8)))
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
            ip("172.16.0.1:4433"),
            ip("127.0.0.1:4433"),
            ip("169.254.1.1:4433"),
            ip("100.64.0.1:4433"),
            ip("[::1]:4433"),
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

    #[test]
    fn classifies_edge_ranges() {
        for public in ["1.1.1.1", "100.128.0.1", "198.20.0.1", "2a01:4f8::1"] {
            assert!(is_publishable(public.parse().unwrap()), "{public}");
        }
        for internal in [
            "0.1.2.3",
            "100.127.255.255",
            "198.19.0.1",
            "203.0.113.10",
            "240.0.0.1",
            "255.255.255.255",
            "2001:db8::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(!is_publishable(internal.parse().unwrap()), "{internal}");
        }
    }
}
