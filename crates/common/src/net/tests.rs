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
        "2002:0808:0808::1",
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
        "::10.0.0.1",
        "::8.8.8.8",
        "100::1",
        "2002:0a00:0001::1",
        "224.0.0.1",
        "ff02::1",
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

#[test]
fn parse_external_addr_accepts_public_v4_and_v6() {
    assert_eq!(
        parse_external_addr("8.8.8.8:30443").unwrap(),
        "8.8.8.8:30443".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(
        parse_external_addr("[2606:4700::1111]:4433").unwrap(),
        "[2606:4700::1111]:4433".parse::<SocketAddr>().unwrap()
    );
}

#[test]
fn parse_external_addr_refuses_hostnames_bare_ips_and_port_zero() {
    for raw in ["node.example:4433", "8.8.8.8", "8.8.8.8:0", ""] {
        let err = parse_external_addr(raw).unwrap_err().to_string();
        assert!(err.contains(&format!("{raw:?}")), "{raw}: {err}");
    }
    for raw in ["8.8.8.8", "2606:4700::1111", "[2606:4700::1111]"] {
        let err = parse_external_addr(raw).unwrap_err().to_string();
        assert!(err.contains("missing port"), "{raw}: {err}");
    }
    let err = parse_external_addr("node.example:4433")
        .unwrap_err()
        .to_string();
    assert!(err.contains("a hostname is not accepted"), "{err}");
    let err = parse_external_addr("/ip4/8.8.8.8/udp/4433/quic-v1")
        .unwrap_err()
        .to_string();
    assert!(err.contains("not a multiaddr"), "{err}");
    let err = parse_external_addr("8.8.8.8:70000")
        .unwrap_err()
        .to_string();
    assert!(err.contains("65535"), "{err}");
}

#[test]
fn parse_external_addr_refuses_non_public_ips() {
    for (raw, ip) in [
        ("10.0.0.5:4433", "10.0.0.5"),
        ("100.64.0.1:4433", "100.64.0.1"),
        ("203.0.113.10:4433", "203.0.113.10"),
        ("[fd00::1]:4433", "fd00::1"),
        ("[::ffff:10.0.0.1]:4433", "10.0.0.1"),
        ("[::10.0.0.1]:4433", "::a00:1"),
        ("224.0.0.1:4433", "224.0.0.1"),
        ("[ff02::1]:4433", "ff02::1"),
    ] {
        let err = parse_external_addr(raw).unwrap_err().to_string();
        assert!(err.contains(ip), "{raw}: {err}");
        assert!(err.contains("not a public address"), "{raw}: {err}");
    }
}

#[test]
fn parse_external_addr_drops_the_scope_id() {
    let addr = parse_external_addr("[2606:4700::1111%3]:4433").unwrap();
    assert_eq!(
        addr,
        "[2606:4700::1111]:4433".parse::<SocketAddr>().unwrap()
    );
    assert!(
        matches!(addr, SocketAddr::V6(v6) if v6.scope_id() == 0),
        "{addr:?}"
    );
}

#[test]
fn parse_external_addr_canonicalises_v4_mapped() {
    assert_eq!(
        parse_external_addr("[::ffff:8.8.8.8]:4433").unwrap(),
        "8.8.8.8:4433".parse::<SocketAddr>().unwrap()
    );
}

#[test]
fn public_quic_addrs_prefers_external_without_repeats() {
    let route: Vec<IpAddr> = vec!["8.8.8.8".parse().unwrap()];
    let external: Vec<SocketAddr> = vec![
        "1.1.1.1:30443".parse().unwrap(),
        "1.1.1.1:30443".parse().unwrap(),
    ];
    assert_eq!(
        public_quic_addrs(&route, 4433, &external),
        vec!["1.1.1.1:30443".parse::<SocketAddr>().unwrap()],
        "configured addresses replace the host's own"
    );
    assert_eq!(
        public_quic_addrs(&route, 4433, &[]),
        vec!["8.8.8.8:4433".parse::<SocketAddr>().unwrap()],
        "with none configured, the host's public IPs on bind_port"
    );
}
