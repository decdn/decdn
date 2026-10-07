use super::*;
use crate::dht::routing::NodeId;
use std::net::{IpAddr, Ipv4Addr};

fn metrics() -> Arc<Metrics> {
    Arc::new(Metrics::new())
}

fn peer(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}

fn ip(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(a, b, c, d))
}

/// ADR 005 §Probe rate limiting defaults: per-peer 5/5, per-IP 50/200,
/// global 1000/2000. The shared engine logic is covered exhaustively by
/// the DHT wrapper's suite; here we pin the probe-specific behaviour — that
/// the per-peer burst of 5 fires on the 6th same-peer probe — to catch a
/// regression in how the probe defaults are wired through this newtype.
///
/// Routed through `ResolvedProbe::default()` and the real `From` mapping
/// rather than restating the eight literals, so these behavioural tests
/// exercise the same path production takes. A default that drifted out of
/// ADR 005 shows up as a burst assertion failing here, not as a fixture
/// that quietly disagrees with the resolver.
fn adr005_default_cfg() -> ProbeRateLimitConfig {
    ProbeRateLimitConfig::from(&decdn_common::config::ResolvedProbe::default())
}

#[test]
fn per_peer_burst_of_five_admits_five_then_rejects() {
    let lim = ProbeRateLimiter::new(&adr005_default_cfg(), metrics());
    let p = peer(1);
    let i = Some(ip(10, 0, 0, 1));
    // Five probes fit the per-peer burst of 5.
    for n in 0..5 {
        assert_eq!(lim.check(&p, i), Ok(()), "probe {n} within burst");
    }
    // The sixth from the same NodeId exhausts the per-peer bucket. Per-IP
    // (burst 200) and global (burst 2000) still have headroom, so per-peer
    // is unambiguously the layer that fires.
    assert_eq!(lim.check(&p, i), Err(ProbeRejectLayer::PerPeer));
}

/// The probe sink must drive the `decdn_probe_rate_limit_*` counters — a
/// regression that wired the probe newtype to the DHT sink would bump
/// `decdn_dht_rate_limit_*` instead and leave the probe scrape silent.
#[test]
fn rejection_increments_probe_scrape_counter_not_dht() {
    let mut cfg = adr005_default_cfg();
    // Strict per-peer so the second same-peer probe rejects; pin refill to
    // ~zero so a slow scheduler can't refill mid-test.
    cfg.per_peer_burst = 1;
    cfg.per_peer_rate_per_sec = 1e-9;
    let metrics = metrics();
    let lim = ProbeRateLimiter::new(&cfg, Arc::clone(&metrics));
    let p = peer(1);
    let i = Some(ip(10, 0, 0, 1));
    assert_eq!(lim.check(&p, i), Ok(()));
    assert_eq!(lim.check(&p, i), Err(ProbeRejectLayer::PerPeer));
    let text = metrics.encode().unwrap();
    assert!(
        text.contains("decdn_probe_rate_limit_rejected_per_peer_total 1"),
        "probe per-peer rejection must appear in /metrics; got:\n{text}"
    );
    assert!(
        text.contains("decdn_dht_rate_limit_rejected_per_peer_total 0"),
        "probe rejection must NOT bump the DHT counter; got:\n{text}"
    );
}

/// Pins the per-IP scrape counter: the `ProbeRateLimitMetrics::rejected`
/// match is hand-written, so a copy-paste swap of its `PerIp` arm would
/// otherwise go uncaught (the per-peer arm is pinned by the test above,
/// the global arm by the test below).
#[test]
fn per_ip_rejection_drives_probe_per_ip_counter() {
    let mut cfg = adr005_default_cfg();
    // Per-IP burst=1 with ~zero refill: the second distinct peer from the
    // same IP can only fail the per-IP layer.
    cfg.per_ip_burst = 1;
    cfg.per_ip_rate_per_sec = 1e-9;
    let metrics = metrics();
    let lim = ProbeRateLimiter::new(&cfg, Arc::clone(&metrics));
    let i = Some(ip(10, 0, 0, 2));
    assert_eq!(lim.check(&peer(3), i), Ok(()));
    assert_eq!(lim.check(&peer(4), i), Err(ProbeRejectLayer::PerIp));
    let text = metrics.encode().unwrap();
    assert!(
        text.contains("decdn_probe_rate_limit_rejected_per_ip_total 1"),
        "probe per-IP rejection must drive the per-IP counter; got:\n{text}"
    );
}

/// A global-cap rejection drives the probe global counter — the third arm
/// of the hand-written `ProbeRateLimitMetrics::rejected` match. With global
/// burst=1 (no refill) and the other layers loose, the second probe from a
/// distinct peer+IP can only fail the global layer.
#[test]
fn global_rejection_drives_probe_global_counter() {
    let mut cfg = adr005_default_cfg();
    cfg.global_burst = 1;
    cfg.global_rate_per_sec = 1e-9;
    let metrics = metrics();
    let lim = ProbeRateLimiter::new(&cfg, Arc::clone(&metrics));
    assert_eq!(lim.check(&peer(1), Some(ip(10, 0, 0, 1))), Ok(()));
    assert_eq!(
        lim.check(&peer(2), Some(ip(10, 0, 0, 2))),
        Err(ProbeRejectLayer::Global)
    );
    let text = metrics.encode().unwrap();
    assert!(
        text.contains("decdn_probe_rate_limit_rejected_global_total 1"),
        "probe global rejection must drive the global counter; got:\n{text}"
    );
}
