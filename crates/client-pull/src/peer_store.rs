//! Persisted per-peer knowledge base: registry-fed identity plus interaction-fed
//! latency and price, keyed by iroh [`iroh::PublicKey`], one JSON file per peer.

use crate::discovery::NodeCandidate;
use alloy::primitives::Address;
use decdn_protocol::Region;
use iroh::PublicKey;
use serde::{Deserialize, Serialize};

/// EWMA weight applied to a new latency sample.
pub const EWMA_ALPHA: f64 = 0.3;
/// Age past which a latency sample is no longer trusted for the probe-less path.
pub const LATENCY_TTL_SECS: u64 = 600;
/// Age past which registry-fed identity is refreshed on the next discovery.
pub const IDENTITY_REFRESH_SECS: u64 = 86_400;
/// Age past which identity unseen in the registry is pruned (node likely left the bond set).
pub const IDENTITY_PRUNE_SECS: u64 = 604_800;
/// Duration a just-failed peer is suppressed from selection.
pub const FAILURE_SUPPRESS_SECS: u64 = 300;
/// Fresh, distinct candidates required to take the probe-less fast path.
pub const MIN_FRESH_CANDIDATES: usize = 3;
/// Maximum stats-bearing records retained before eviction.
pub const LRU_CAP: usize = 4_096;

/// Tunables governing staleness, suppression, and eviction.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// See [`LATENCY_TTL_SECS`].
    pub latency_ttl_secs: u64,
    /// See [`IDENTITY_REFRESH_SECS`].
    pub identity_refresh_secs: u64,
    /// See [`IDENTITY_PRUNE_SECS`].
    pub identity_prune_secs: u64,
    /// See [`FAILURE_SUPPRESS_SECS`].
    pub failure_suppress_secs: u64,
    /// See [`MIN_FRESH_CANDIDATES`].
    pub min_fresh_candidates: usize,
    /// See [`LRU_CAP`].
    pub lru_cap: usize,
    /// See [`EWMA_ALPHA`].
    pub ewma_alpha: f64,
}

impl Default for StoreConfig {
    /// Default tunables: all constants at their nominal values.
    fn default() -> Self {
        Self {
            latency_ttl_secs: LATENCY_TTL_SECS,
            identity_refresh_secs: IDENTITY_REFRESH_SECS,
            identity_prune_secs: IDENTITY_PRUNE_SECS,
            failure_suppress_secs: FAILURE_SUPPRESS_SECS,
            min_fresh_candidates: MIN_FRESH_CANDIDATES,
            lru_cap: LRU_CAP,
            ewma_alpha: EWMA_ALPHA,
        }
    }
}

/// Everything the client knows about one peer: identity (registry-fed) and
/// stats (interaction-fed), aging on separate clocks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRecord {
    /// iroh endpoint id — the record's key and filename.
    pub node_id: PublicKey,
    /// The node's Ethereum address, used to open/verify its payment lane.
    pub eth_address: Address,
    /// The node's self-attested region (ADR 030), or `None` when unset/invalid.
    pub region_hint: Option<Region>,
    /// Seconds since the Unix epoch when identity was last confirmed against the registry.
    pub identity_seen_at_secs: u64,
    /// EWMA-smoothed observed latency in milliseconds; `None` until the first sample.
    pub latency_ms: Option<f64>,
    /// Seconds since the Unix epoch of the most recent latency/price sample.
    pub last_sampled_at_secs: Option<u64>,
    /// Last observed price quote; a ranking hint only, never authoritative.
    pub rate_per_mb: Option<u64>,
    /// Number of latency samples folded so far (gates EWMA warm-up).
    pub sample_count: u32,
    /// Seconds since the Unix epoch of the most recent failure, if any.
    pub last_failure_at_secs: Option<u64>,
}

impl PeerRecord {
    /// A latency sample exists and is younger than the TTL.
    #[must_use]
    pub const fn latency_fresh(&self, now_secs: u64, cfg: &StoreConfig) -> bool {
        match self.last_sampled_at_secs {
            Some(t) => now_secs.saturating_sub(t) <= cfg.latency_ttl_secs,
            None => false,
        }
    }

    /// A recent failure still suppresses this peer.
    #[must_use]
    pub const fn failure_suppressed(&self, now_secs: u64, cfg: &StoreConfig) -> bool {
        match self.last_failure_at_secs {
            Some(t) => now_secs.saturating_sub(t) < cfg.failure_suppress_secs,
            None => false,
        }
    }

    /// Identity has not been seen in the registry for longer than the prune horizon.
    #[must_use]
    pub const fn identity_prunable(&self, now_secs: u64, cfg: &StoreConfig) -> bool {
        now_secs.saturating_sub(self.identity_seen_at_secs) > cfg.identity_prune_secs
    }

    /// Eligible for the probe-less fast path: has a fresh latency sample and is not suppressed.
    #[must_use]
    pub const fn selectable(&self, now_secs: u64, cfg: &StoreConfig) -> bool {
        self.latency_ms.is_some()
            && self.latency_fresh(now_secs, cfg)
            && !self.failure_suppressed(now_secs, cfg)
    }

    /// Project the identity half back into a [`NodeCandidate`] for selection/fallback.
    #[must_use]
    pub const fn as_candidate(&self) -> NodeCandidate {
        NodeCandidate {
            node_id: self.node_id,
            eth_address: self.eth_address,
            region_hint: self.region_hint,
        }
    }

    /// Fold a new latency sample into the EWMA (first sample sets the value directly).
    pub fn fold_latency(&mut self, sample_ms: f64, alpha: f64) {
        self.latency_ms = Some(match self.latency_ms {
            Some(prev) => (1.0 - alpha) * prev + alpha * sample_ms,
            None => sample_ms,
        });
        self.sample_count = self.sample_count.saturating_add(1);
    }
}

#[allow(dead_code)]
fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record() -> PeerRecord {
        // A deterministic PublicKey/Address for tests: generate from a seed.
        let secret = iroh::SecretKey::from_bytes(&[7u8; 32]);
        let node_id = secret.public();
        PeerRecord {
            node_id,
            eth_address: Address::repeat_byte(0xAB),
            region_hint: Region::parse("US"),
            identity_seen_at_secs: 1_000,
            latency_ms: None,
            last_sampled_at_secs: None,
            rate_per_mb: None,
            sample_count: 0,
            last_failure_at_secs: None,
        }
    }

    #[test]
    fn first_sample_sets_value_then_ewma_folds() {
        let cfg = StoreConfig::default();
        let mut r = sample_record();
        r.fold_latency(40.0, cfg.ewma_alpha);
        assert_eq!(r.latency_ms, Some(40.0));
        assert_eq!(r.sample_count, 1);
        r.fold_latency(140.0, cfg.ewma_alpha);
        // 0.7*40 + 0.3*140 = 70
        assert!((r.latency_ms.unwrap_or_default() - 70.0).abs() < 1e-9);
        assert_eq!(r.sample_count, 2);
    }

    #[test]
    fn latency_freshness_respects_ttl() {
        let cfg = StoreConfig::default();
        let mut r = sample_record();
        r.last_sampled_at_secs = Some(1_000);
        assert!(r.latency_fresh(1_000 + cfg.latency_ttl_secs, &cfg));
        assert!(!r.latency_fresh(1_000 + cfg.latency_ttl_secs + 1, &cfg));
    }

    #[test]
    fn failure_suppresses_then_expires() {
        let cfg = StoreConfig::default();
        let mut r = sample_record();
        r.latency_ms = Some(20.0);
        r.last_sampled_at_secs = Some(2_000);
        r.last_failure_at_secs = Some(2_000);
        assert!(r.failure_suppressed(2_000, &cfg));
        assert!(!r.selectable(2_000, &cfg));
        let after = 2_000 + cfg.failure_suppress_secs;
        assert!(!r.failure_suppressed(after, &cfg));
        assert!(r.selectable(after, &cfg));
    }

    #[test]
    fn identity_prunable_past_horizon() {
        let cfg = StoreConfig::default();
        let r = sample_record();
        assert!(!r.identity_prunable(1_000 + cfg.identity_prune_secs, &cfg));
        assert!(r.identity_prunable(1_000 + cfg.identity_prune_secs + 1, &cfg));
    }
}
