use super::{
    Address, Bytes, NodeCandidate, ProxyWarmingParams, PublicKey, ResolvedTargets, discovery,
};
use crate::ProvenSize;

/// A node key derived from `seed`.
pub(crate) fn node_key(seed: u8) -> PublicKey {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

/// A verified holder of the whole blob at `rtt_ms`, quoting 1 per MiB, paid at the address
/// `seed` repeated.
pub(crate) fn holder(seed: u8, rtt_ms: f64) -> discovery::DiscoveredHolder {
    discovery::DiscoveredHolder {
        candidate: NodeCandidate {
            node_id: node_key(seed),
            eth_address: Address::repeat_byte(seed),
            region_hint: None,
            multiaddrs: Bytes::new(),
        },
        rtt_ms,
        rate_per_mb: 1,
        total_bytes: Some(ProvenSize::assume_for_tests(128 * 1024 * 1024)),
        coverage: decdn_protocol::Coverage::empty(),
        payment_pools: None,
    }
}

/// Two verified holders resolved the way discovery resolves them, with
/// different measured coverage: the targets, then each holder's node.
pub(crate) fn two_holder_targets() -> (ResolvedTargets, NodeCandidate, NodeCandidate) {
    let mut a = holder(1, 10.0);
    a.coverage = decdn_protocol::Coverage::from_block_indices(4, [0, 1].into_iter());
    let mut b = holder(2, 20.0);
    b.coverage = decdn_protocol::Coverage::from_block_indices(4, [2, 3].into_iter());
    let (node_a, node_b) = (a.candidate.clone(), b.candidate.clone());
    // Distinct rates, so a test that keys a rate to the wrong node fails.
    let samples = vec![(node_a.node_id, a.rtt_ms, 3), (node_b.node_id, b.rtt_ms, 9)];
    let ordered = super::failover_order(
        vec![b, a],
        &[],
        ProxyWarmingParams {
            enabled: false,
            rtt_threshold_ms: 150.0,
            margin_ms: 30.0,
        },
    );
    let targets = ResolvedTargets {
        candidates: ordered.order,
        coverage_by_node: ordered.coverage_by_node,
        samples,
        pinned: false,
        size_hint: Some(ProvenSize::assume_for_tests(128 * 1024 * 1024)),
        late: super::LateSlot::default(),
        opened: super::OpenedSlot::default(),
        pool_hints: std::collections::HashMap::new(),
    };
    (targets, node_a, node_b)
}
