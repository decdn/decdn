use super::*;
use std::io::ErrorKind;

#[test]
fn reachability_passes_both_ways_and_guides_the_nat_case() {
    let public = classify_reachability(&["8.8.8.8".parse().unwrap()], 4433);
    assert_eq!(public.severity, Severity::Pass);
    assert!(public.title.contains("/ip4/8.8.8.8/udp/4433/quic-v1"));
    assert!(public.remediation.is_none());

    let nat = classify_reachability(&[], 4433);
    assert_eq!(nat.severity, Severity::Pass, "behind NAT is supported");
    assert!(nat.remediation.unwrap().contains("UDP 4433"));
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
