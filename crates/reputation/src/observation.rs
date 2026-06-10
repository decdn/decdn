//! Outbound-report observation buffer (ADR 008 §Gossip Protocol — "after
//! interacting with a node, a node broadcasts a signed reputation report").
//!
//! The serving / probe hot path records the most recent metrics it observed
//! per peer via [`ObservationBuffer::observe`]; the gossip publisher drains the
//! buffer once per publish tick via [`ObservationBuffer::drain`]. Coalescing to
//! the latest observation per peer naturally bounds output to ≤1 report per
//! (reporter, node) per tick, honouring the ADR rate limit when the tick equals
//! the 1-hour window.

use decdn_protocol::ReportMetrics;
use iroh::PublicKey as NodeId;
use std::collections::HashMap;
use std::sync::Mutex;

/// In-memory, coalescing buffer of pending outbound reports keyed by peer.
#[derive(Debug, Default)]
pub struct ObservationBuffer {
    inner: Mutex<HashMap<NodeId, ReportMetrics>>,
}

impl ObservationBuffer {
    /// Create an empty buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the latest observed metrics for `peer`, overwriting any pending
    /// observation for the same peer since the last drain.
    pub fn observe(&self, peer: NodeId, metrics: ReportMetrics) {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.insert(peer, metrics);
    }

    /// Take and clear all pending observations.
    pub fn drain(&self) -> Vec<(NodeId, ReportMetrics)> {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.drain().collect()
    }

    /// Number of pending observations.
    pub fn len(&self) -> usize {
        let guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.len()
    }

    /// Whether there are no pending observations.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;

    fn peer() -> NodeId {
        SecretKey::generate().public()
    }

    fn metrics(speed: u32) -> ReportMetrics {
        ReportMetrics {
            delivery_speed: Some(speed),
            uptime_observed: Some(true),
            data_correct: Some(true),
        }
    }

    #[test]
    fn observe_coalesces_per_peer() {
        let buf = ObservationBuffer::new();
        let p = peer();
        buf.observe(p, metrics(1));
        buf.observe(p, metrics(2));
        assert_eq!(buf.len(), 1);
        let drained = buf.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(
            drained.first().map(|(_, m)| m.delivery_speed),
            Some(Some(2))
        );
    }

    #[test]
    fn drain_empties_buffer() {
        let buf = ObservationBuffer::new();
        buf.observe(peer(), metrics(1));
        buf.observe(peer(), metrics(1));
        assert_eq!(buf.drain().len(), 2);
        assert!(buf.is_empty());
        assert!(buf.drain().is_empty());
    }
}
