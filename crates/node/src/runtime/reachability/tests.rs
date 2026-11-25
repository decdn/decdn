use super::*;

fn sock(s: &str) -> SocketAddr {
    s.parse().unwrap()
}

#[test]
fn no_public_address_is_behind_nat() {
    assert_eq!(classify(&[], &[]), Reachability::BehindNat);
}

#[test]
fn public_and_registered_on_the_bind_port() {
    assert_eq!(
        classify(&[sock("8.8.8.8:4433")], &[sock("8.8.8.8:4433")]),
        Reachability::PublicRegistered
    );
}

#[test]
fn public_but_unregistered_or_on_another_port_names_the_missing_multiaddr() {
    let want = Reachability::PublicUnregistered {
        missing: vec!["/ip4/8.8.8.8/udp/4433/quic-v1".to_string()],
    };
    assert_eq!(classify(&[sock("8.8.8.8:4433")], &[]), want);
    assert_eq!(
        classify(&[sock("8.8.8.8:4433")], &[sock("8.8.8.8:5000")]),
        want
    );
}

#[test]
fn external_addr_on_its_own_port_registered_is_public_registered() {
    let public = public_quic_addrs(&[], 4433, &[sock("1.1.1.1:30443")]);
    assert_eq!(
        classify(&public, &[sock("1.1.1.1:30443")]),
        Reachability::PublicRegistered
    );
}

#[test]
fn unregistered_external_addr_names_its_own_port() {
    let public = public_quic_addrs(&[], 4433, &[sock("1.1.1.1:30443")]);
    assert_eq!(
        classify(&public, &[sock("1.1.1.1:4433")]),
        Reachability::PublicUnregistered {
            missing: vec!["/ip4/1.1.1.1/udp/30443/quic-v1".to_string()],
        }
    );
}

#[test]
fn addr_source_is_route_without_configured_addrs_and_configured_with() {
    assert_eq!(addr_source(&[]), "route");
    assert_eq!(addr_source(&[sock("1.1.1.1:30443")]), "configured");
}

#[test]
fn report_marks_a_configured_external_addr_public() {
    // The configured address replaces the host's own, so the gauge reads 1
    // on any host, behind NAT included.
    let metrics = Metrics::new();
    report(&metrics, 4433, &[sock("1.1.1.1:30443")], &[]);
    let text = metrics.encode().unwrap();
    assert!(
        text.lines().any(|l| l == "decdn_node_public_address 1"),
        "{text}"
    );
}
