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

use std::path::{Path, PathBuf};

/// Directory-backed peer knowledge base: one JSON file per peer under `<data_dir>/peers`.
#[derive(Debug, Clone)]
pub struct PeerStore {
    dir: PathBuf,
}

impl PeerStore {
    /// Open (do not create) the store rooted at `<data_dir>/peers`.
    #[must_use]
    pub fn open(data_dir: &Path) -> Self {
        Self {
            dir: data_dir.join("peers"),
        }
    }

    fn path_for(&self, node_id: &PublicKey) -> PathBuf {
        self.dir.join(format!("{node_id}.json"))
    }

    /// Read every valid record, skipping files that do not parse.
    #[must_use]
    pub fn load_all(&self) -> Vec<PeerRecord> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            if let Ok(bytes) = std::fs::read(&path)
                && let Ok(rec) = serde_json::from_slice::<PeerRecord>(&bytes)
            {
                out.push(rec);
            }
        }
        out
    }

    /// Read one record by id.
    #[must_use]
    pub fn get(&self, node_id: &PublicKey) -> Option<PeerRecord> {
        let bytes = std::fs::read(self.path_for(node_id)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    fn write(&self, rec: &PeerRecord) -> anyhow::Result<()> {
        use std::io::Write as _;
        std::fs::create_dir_all(&self.dir)?;
        let bytes = serde_json::to_vec_pretty(rec)?;
        let tmp = tempfile::NamedTempFile::new_in(&self.dir)?;
        // Write through the same handle that is synced below, so the durable
        // bytes are the ones this write produced.
        tmp.as_file().write_all(&bytes)?;
        tmp.as_file().sync_all()?;
        tmp.persist(self.path_for(&rec.node_id))
            .map_err(|e| anyhow::anyhow!("persist peer record: {e}"))?;
        Ok(())
    }

    /// Refresh identity, preserving existing stats.
    pub fn upsert_identity(&self, cand: &NodeCandidate, now_secs: u64) -> anyhow::Result<()> {
        let mut rec = self.get(&cand.node_id).unwrap_or(PeerRecord {
            node_id: cand.node_id,
            eth_address: cand.eth_address,
            region_hint: cand.region_hint,
            identity_seen_at_secs: now_secs,
            latency_ms: None,
            last_sampled_at_secs: None,
            rate_per_mb: None,
            sample_count: 0,
            last_failure_at_secs: None,
        });
        rec.eth_address = cand.eth_address;
        rec.region_hint = cand.region_hint;
        rec.identity_seen_at_secs = now_secs;
        self.write(&rec)
    }

    /// Fold a latency sample, set price, stamp freshness, and clear any failure.
    pub fn record_sample(
        &self,
        node_id: &PublicKey,
        latency_ms: f64,
        rate_per_mb: u64,
        now_secs: u64,
        cfg: &StoreConfig,
    ) -> anyhow::Result<()> {
        let mut rec = self.get(node_id).unwrap_or(PeerRecord {
            node_id: *node_id,
            eth_address: Address::ZERO,
            region_hint: None,
            identity_seen_at_secs: 0,
            latency_ms: None,
            last_sampled_at_secs: None,
            rate_per_mb: None,
            sample_count: 0,
            last_failure_at_secs: None,
        });
        rec.fold_latency(latency_ms, cfg.ewma_alpha);
        rec.rate_per_mb = Some(rate_per_mb);
        rec.last_sampled_at_secs = Some(now_secs);
        rec.last_failure_at_secs = None;
        self.write(&rec)
    }

    /// Stamp a failure so the peer is suppressed from selection.
    pub fn record_failure(&self, node_id: &PublicKey, now_secs: u64) -> anyhow::Result<()> {
        let Some(mut rec) = self.get(node_id) else {
            return Ok(());
        };
        rec.last_failure_at_secs = Some(now_secs);
        self.write(&rec)
    }

    fn delete(&self, node_id: &PublicKey) -> anyhow::Result<()> {
        match std::fs::remove_file(self.path_for(node_id)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Prune very-stale identities, then LRU-evict down to `lru_cap`.
    pub fn prune_and_cap(&self, now_secs: u64, cfg: &StoreConfig) -> anyhow::Result<()> {
        let mut records = self.load_all();
        records.retain(|r| {
            if r.identity_prunable(now_secs, cfg) {
                let _ = self.delete(&r.node_id);
                false
            } else {
                true
            }
        });
        if records.len() <= cfg.lru_cap {
            return Ok(());
        }
        // Least-recently-useful first: oldest last sample, then oldest identity.
        records.sort_by(|a, b| {
            a.last_sampled_at_secs
                .unwrap_or(0)
                .cmp(&b.last_sampled_at_secs.unwrap_or(0))
                .then(a.identity_seen_at_secs.cmp(&b.identity_seen_at_secs))
        });
        let evict = records.len().saturating_sub(cfg.lru_cap);
        for r in records.into_iter().take(evict) {
            self.delete(&r.node_id)?;
        }
        Ok(())
    }
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

    use tempfile::tempdir;

    fn key(b: u8) -> PublicKey {
        iroh::SecretKey::from_bytes(&[b; 32]).public()
    }

    fn candidate(b: u8) -> NodeCandidate {
        NodeCandidate {
            node_id: key(b),
            eth_address: Address::repeat_byte(b),
            region_hint: Region::parse("US"),
        }
    }

    #[test]
    fn upsert_then_get_roundtrips_identity() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let store = PeerStore::open(dir.path());
        store.upsert_identity(&candidate(1), 5_000)?;
        let got = store
            .get(&key(1))
            .ok_or_else(|| anyhow::anyhow!("missing"))?;
        assert_eq!(got.eth_address, Address::repeat_byte(1));
        assert_eq!(got.identity_seen_at_secs, 5_000);
        assert_eq!(got.latency_ms, None);
        Ok(())
    }

    #[test]
    fn sample_preserves_identity_and_folds_latency() -> anyhow::Result<()> {
        let cfg = StoreConfig::default();
        let dir = tempdir()?;
        let store = PeerStore::open(dir.path());
        store.upsert_identity(&candidate(2), 5_000)?;
        store.record_sample(&key(2), 30.0, 7, 5_100, &cfg)?;
        let got = store
            .get(&key(2))
            .ok_or_else(|| anyhow::anyhow!("missing"))?;
        assert_eq!(got.latency_ms, Some(30.0));
        assert_eq!(got.rate_per_mb, Some(7));
        assert_eq!(got.last_sampled_at_secs, Some(5_100));
        assert_eq!(got.eth_address, Address::repeat_byte(2)); // identity intact
        Ok(())
    }

    #[test]
    fn failure_then_success_clears_stamp() -> anyhow::Result<()> {
        let cfg = StoreConfig::default();
        let dir = tempdir()?;
        let store = PeerStore::open(dir.path());
        store.upsert_identity(&candidate(3), 5_000)?;
        store.record_failure(&key(3), 6_000)?;
        assert!(
            store
                .get(&key(3))
                .and_then(|r| r.last_failure_at_secs)
                .is_some()
        );
        store.record_sample(&key(3), 25.0, 3, 6_100, &cfg)?;
        assert!(
            store
                .get(&key(3))
                .and_then(|r| r.last_failure_at_secs)
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn load_all_skips_corrupt_files() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let store = PeerStore::open(dir.path());
        store.upsert_identity(&candidate(4), 5_000)?;
        std::fs::create_dir_all(dir.path().join("peers"))?;
        std::fs::write(dir.path().join("peers").join("garbage.json"), b"{not json")?;
        let all = store.load_all();
        assert_eq!(all.len(), 1);
        Ok(())
    }

    #[test]
    fn prune_drops_very_stale_identity_keeps_fresh() -> anyhow::Result<()> {
        let cfg = StoreConfig::default();
        let dir = tempdir()?;
        let store = PeerStore::open(dir.path());
        store.upsert_identity(&candidate(1), 0)?; // ancient identity
        store.upsert_identity(&candidate(2), 1_000_000)?; // fresh identity
        let now = 1_000_000 + cfg.identity_prune_secs; // key(1) is prunable, key(2) is not
        store.prune_and_cap(now, &cfg)?;
        assert!(store.get(&key(1)).is_none());
        assert!(store.get(&key(2)).is_some());
        Ok(())
    }

    #[test]
    fn stream_sample_supersedes_probe_sample() -> anyhow::Result<()> {
        let cfg = StoreConfig::default();
        let dir = tempdir()?;
        let store = PeerStore::open(dir.path());
        store.upsert_identity(&candidate(1), 1_000)?;
        store.record_sample(&key(1), 200.0, 5, 1_000, &cfg)?; // probe-derived
        store.record_sample(&key(1), 20.0, 5, 1_100, &cfg)?; // stream-derived TTFB
        let r = store
            .get(&key(1))
            .ok_or_else(|| anyhow::anyhow!("missing"))?;
        // EWMA: 0.7*200 + 0.3*20 = 146; the fresh stream pulls latency down toward TTFB.
        assert!(r.latency_ms.unwrap_or_default() < 200.0);
        Ok(())
    }

    #[test]
    fn cap_evicts_least_recently_sampled() -> anyhow::Result<()> {
        let cfg = StoreConfig {
            lru_cap: 2,
            ..Default::default()
        };
        let dir = tempdir()?;
        let store = PeerStore::open(dir.path());
        for b in 1u8..=3 {
            store.upsert_identity(&candidate(b), 1_000_000)?;
            store.record_sample(&key(b), 10.0, 1, 1_000_000 + u64::from(b), &cfg)?;
        }
        store.prune_and_cap(1_000_100, &cfg)?;
        assert_eq!(store.load_all().len(), 2);
        assert!(store.get(&key(1)).is_none()); // oldest sample evicted
        Ok(())
    }
}
