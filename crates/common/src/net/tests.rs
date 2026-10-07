use super::*;

#[test]
fn classifies_public_and_internal_ranges() {
    for public in [
        "8.8.8.8",
        "1.1.1.1",
        "100.128.0.1",
        "198.20.0.1",
        "2606:4700::1111",
        "2a01:4f8::1",
    ] {
        assert!(is_publishable(public.parse().unwrap()), "{public}");
    }
    for internal in [
        "10.0.0.5",
        "172.16.0.1",
        "192.168.1.2",
        "127.0.0.1",
        "169.254.1.1",
        "100.64.0.1",
        "100.127.255.255",
        "198.19.0.1",
        "203.0.113.10",
        "0.1.2.3",
        "240.0.0.1",
        "255.255.255.255",
        "::1",
        "fd00::1",
        "fe80::1",
        "2001:db8::1",
        "::ffff:10.0.0.1",
        "::ffff:127.0.0.1",
    ] {
        assert!(!is_publishable(internal.parse().unwrap()), "{internal}");
    }
}

#[test]
fn route_public_ips_returns_only_publishable_addresses() {
    assert!(route_public_ips().into_iter().all(is_publishable));
}

#[test]
fn quic_multiaddr_formats_both_families() {
    assert_eq!(
        quic_multiaddr("203.0.113.10".parse().unwrap(), 4433),
        "/ip4/203.0.113.10/udp/4433/quic-v1"
    );
    assert_eq!(
        quic_multiaddr("2a01:4f8::1".parse().unwrap(), 4433),
        "/ip6/2a01:4f8::1/udp/4433/quic-v1"
    );
}
