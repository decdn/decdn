//! Host address classification shared by the daemon and the CLI (ADR 001 §
//! Node Discovery).
//!
//! [`is_publishable`] decides whether an IP address is reachable from the
//! public internet. [`route_public_ips`] reports the host's own public
//! addresses: the source address the kernel picks for its default route, per
//! address family, kept only when it is publishable. A host with a public IP on
//! its interface (a typical dedicated or cloud server) reports it; a host
//! behind NAT (a home connection) reports none, because its route source is a
//! private address.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};

/// Whether `ip` is reachable from the public internet. A stable stand-in for
/// the unstable `IpAddr::is_global`. An IPv4-mapped IPv6 address is judged as
/// its IPv4 address.
#[must_use]
pub fn is_publishable(ip: IpAddr) -> bool {
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

/// The host's public IP addresses: for IPv4 and IPv6, the source address of
/// the default route, kept when [`is_publishable`]. Empty on a host behind NAT
/// or with no default route.
///
/// The probe `connect`s an unbound UDP socket, which only asks the kernel to
/// pick a route and sends no packet. The targets are documentation addresses,
/// which follow the default route like any other remote address.
#[must_use]
pub fn route_public_ips() -> Vec<IpAddr> {
    let probes: [(SocketAddr, SocketAddr); 2] = [
        (
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 9)),
        ),
        (
            SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
            SocketAddr::from((Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1), 9)),
        ),
    ];
    probes
        .into_iter()
        .filter_map(|(bind, target)| route_source(bind, target))
        .filter(|ip| is_publishable(*ip))
        .collect()
}

/// The source address the kernel picks to reach `target`, or `None` when the
/// family has no route.
fn route_source(bind: SocketAddr, target: SocketAddr) -> Option<IpAddr> {
    let socket = UdpSocket::bind(bind).ok()?;
    socket.connect(target).ok()?;
    socket.local_addr().ok().map(|a| a.ip())
}

/// The QUIC multiaddr a node at `ip` listening on UDP `port` registers, e.g.
/// `/ip4/203.0.113.10/udp/4433/quic-v1`.
#[must_use]
pub fn quic_multiaddr(ip: IpAddr, port: u16) -> String {
    match ip {
        IpAddr::V4(v4) => format!("/ip4/{v4}/udp/{port}/quic-v1"),
        IpAddr::V6(v6) => format!("/ip6/{v6}/udp/{port}/quic-v1"),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
