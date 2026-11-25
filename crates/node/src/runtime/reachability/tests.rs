use super::*;
use decdn_common::config::ResolvedDiscovery;

fn sock(s: &str) -> SocketAddr {
    s.parse().unwrap()
}

/// A resolved `[network]` section on `bind_port` with `external_addrs`.
fn network(bind_port: u16, external_addrs: Vec<SocketAddr>) -> ResolvedNetwork {
    ResolvedNetwork {
        bind_port,
        relay_urls: vec![],
        external_addrs,
        discovery: ResolvedDiscovery::default(),
    }
}

/// Whether `metrics` reads `decdn_node_public_address 1`.
fn marked_public(metrics: &Metrics) -> bool {
    let text = metrics.encode().unwrap();
    text.lines().any(|l| l == "decdn_node_public_address 1")
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
fn report_marks_a_configured_external_addr_public() {
    // No route-public IP: only the configured address makes the node public.
    let metrics = Metrics::new();
    report_with(
        &[],
        &metrics,
        &network(4433, vec![sock("1.1.1.1:30443")]),
        &[],
    );
    assert!(marked_public(&metrics));
}

#[test]
fn report_marks_a_route_public_ip_public() {
    let metrics = Metrics::new();
    report_with(
        &["8.8.8.8".parse().unwrap()],
        &metrics,
        &network(4433, vec![]),
        &[],
    );
    assert!(marked_public(&metrics));
}

#[test]
fn report_without_a_public_address_is_not_public() {
    let metrics = Metrics::new();
    report_with(&[], &metrics, &network(4433, vec![]), &[]);
    assert!(!marked_public(&metrics));
}
