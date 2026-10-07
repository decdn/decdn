use super::{
    Address, Bytes, NodeCandidate, ProxyWarmingParams, PublicKey, ResolvedTargets, discovery,
};

/// A node key derived from `seed`.
pub(crate) fn node_key(seed: u8) -> PublicKey {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

/// A probed holder of the whole blob at `rtt_ms`, paid at the address
/// `seed` repeated.
pub(crate) fn holder(seed: u8, rtt_ms: f64) -> discovery::Probed {
    discovery::Probed {
        candidate: NodeCandidate {
            node_id: node_key(seed),
            eth_address: Address::repeat_byte(seed),
            region_hint: None,
            multiaddrs: Bytes::new(),
        },
        rtt_ms,
        total_bytes: Some(128 * 1024 * 1024),
        coverage: decdn_protocol::Coverage::empty(),
    }
}

/// Two probed holders resolved the way discovery resolves them, with
/// different measured coverage: the targets, then each holder's node.
pub(crate) fn two_holder_targets() -> (ResolvedTargets, NodeCandidate, NodeCandidate) {
    let mut a = holder(1, 10.0);
    a.coverage = decdn_protocol::Coverage::from_block_indices(4, [0, 1].into_iter());
    let mut b = holder(2, 20.0);
    b.coverage = decdn_protocol::Coverage::from_block_indices(4, [2, 3].into_iter());
    let (node_a, node_b) = (a.candidate.clone(), b.candidate.clone());
    let probed_samples = vec![(node_a.node_id, a.rtt_ms, 1), (node_b.node_id, b.rtt_ms, 1)];
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
        probed_samples,
        pinned: false,
        size_hint: Some(128 * 1024 * 1024),
        late: super::LateSlot::default(),
    };
    (targets, node_a, node_b)
}
