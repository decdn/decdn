use super::*;
use decdn_common::config::ResolvedDiscovery;
use std::io::ErrorKind;
use std::net::SocketAddr;

/// A resolved `[network]` section on `bind_port` with `external_addrs`.
fn network(bind_port: u16, external_addrs: Vec<SocketAddr>) -> ResolvedNetwork {
    ResolvedNetwork {
        bind_port,
        relay_urls: vec![],
        external_addrs,
        discovery: ResolvedDiscovery::default(),
    }
}

#[test]
fn reachability_passes_both_ways_and_guides_the_nat_case() {
    let public = classify_reachability(&["8.8.8.8".parse().unwrap()], &network(4433, vec![]));
    assert_eq!(public.severity, Severity::Pass);
    assert!(public.title.contains("/ip4/8.8.8.8/udp/4433/quic-v1"));
    assert!(public.remediation.is_none());
    assert_eq!(public.detail.as_deref(), Some("source=route probed=false"));
    assert!(!public.title.contains("load balancer"), "{}", public.title);

    let nat = classify_reachability(&[], &network(4433, vec![]));
    assert_eq!(nat.severity, Severity::Pass, "behind NAT is supported");
    let remediation = nat.remediation.unwrap();
    assert!(remediation.contains("UDP 4433"));
    assert!(remediation.contains("network.external_addrs"));
}

#[test]
fn reachability_counts_a_configured_external_addr() {
    // The configured address replaces the host's own public IP.
    let finding = classify_reachability(
        &["8.8.8.8".parse().unwrap()],
        &network(4433, vec!["1.1.1.1:30443".parse().unwrap()]),
    );
    assert_eq!(finding.severity, Severity::Pass);
    assert!(
        finding.title.contains("/ip4/1.1.1.1/udp/30443/quic-v1"),
        "{}",
        finding.title
    );
    assert!(!finding.title.contains("8.8.8.8"), "{}", finding.title);
    assert!(finding.remediation.is_none());
    assert!(finding.title.contains("load balancer"), "{}", finding.title);
    assert_eq!(
        finding.detail.as_deref(),
        Some("source=configured probed=false")
    );
}

#[test]
fn free_port_passes() {
    assert_eq!(
        classify_bind("admin", 9191, Ok(()), false).severity,
        Severity::Pass
    );
}

#[test]
fn in_use_with_daemon_is_pass_without_is_fail() {
    assert_eq!(
        classify_bind("admin", 9191, Err(ErrorKind::AddrInUse), true).severity,
        Severity::Pass
    );
    assert_eq!(
        classify_bind("admin", 9191, Err(ErrorKind::AddrInUse), false).severity,
        Severity::Fail
    );
}

#[test]
fn other_error_warns() {
    assert_eq!(
        classify_bind("admin", 9191, Err(ErrorKind::PermissionDenied), false).severity,
        Severity::Warn
    );
}
