use super::*;
use crate::dht::routing::NodeId;

fn metrics() -> Arc<Metrics> {
    Arc::new(Metrics::new())
}

fn peer(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}

/// ADR 005 §Probe rate limiting defaults: per-peer 5/5, global 1000/2000.
/// The shared engine logic is covered exhaustively by the DHT wrapper's
/// suite; here we pin the probe-specific behaviour — that the per-peer burst
/// of 5 fires on the 6th same-peer probe — to catch a regression in how the
/// probe defaults are wired through this newtype.
///
/// Routed through `ResolvedProbe::default()` and the real `From` mapping
/// rather than restating the literals, so these behavioural tests exercise
/// the same path production takes. A default that drifted out of ADR 005
/// shows up as a burst assertion failing here, not as a fixture that quietly
/// disagrees with the resolver.
fn adr005_default_cfg() -> ProbeRateLimitConfig {
    ProbeRateLimitConfig::from(&decdn_common::config::ResolvedProbe::default())
}

#[test]
fn per_peer_burst_of_five_admits_five_then_rejects() {
    let lim = ProbeRateLimiter::new(&adr005_default_cfg(), metrics());
    let p = peer(1);
    // Five probes fit the per-peer burst of 5.
    for n in 0..5 {
        assert_eq!(lim.check(&p), Ok(()), "probe {n} within burst");
    }
    // The sixth from the same NodeId exhausts the per-peer bucket. Global
    // (burst 2000) still has headroom, so per-peer is unambiguously the
    // layer that fires.
    assert_eq!(lim.check(&p), Err(ProbeRejectLayer::PerPeer));
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
    assert_eq!(lim.check(&p), Ok(()));
    assert_eq!(lim.check(&p), Err(ProbeRejectLayer::PerPeer));
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

/// A global-cap rejection drives the probe global counter — the second arm
/// of the hand-written `ProbeRateLimitMetrics::rejected` match. With global
/// burst=1 (no refill) and per-peer loose, the second probe from a distinct
/// peer can only fail the global layer.
#[test]
fn global_rejection_drives_probe_global_counter() {
    let mut cfg = adr005_default_cfg();
    cfg.global_burst = 1;
    cfg.global_rate_per_sec = 1e-9;
    let metrics = metrics();
    let lim = ProbeRateLimiter::new(&cfg, Arc::clone(&metrics));
    assert_eq!(lim.check(&peer(1)), Ok(()));
    assert_eq!(lim.check(&peer(2)), Err(ProbeRejectLayer::Global));
    let text = metrics.encode().unwrap();
    assert!(
        text.contains("decdn_probe_rate_limit_rejected_global_total 1"),
        "probe global rejection must drive the global counter; got:\n{text}"
    );
}
