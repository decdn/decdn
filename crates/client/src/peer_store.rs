//! Persisted per-peer knowledge base: registry-fed identity, probe-fed latency,
//! and price from probes and stream opens, keyed by iroh [`iroh::PublicKey`], one JSON file per peer.

use crate::discovery::NodeCandidate;
use alloy::primitives::{Address, Bytes};
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
/// stats (probe- and stream-fed), aging on separate clocks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRecord {
    /// iroh endpoint id — the record's key and filename.
    pub node_id: PublicKey,
    /// The node's Ethereum address, used to open/verify its payment lane.
    pub eth_address: Address,
    /// The node's self-attested region (ADR 030), or `None` when unset/invalid.
    pub region_hint: Option<Region>,
    /// The node's registry-published, packed `multiaddrs` field, refreshed on
    /// every identity upsert. Persisting it lets a returning client dial a
    /// known-good peer directly when it can reach neither the chain (RPC outage)
    /// nor iroh discovery — the fully-decentralized fallback (ADR 001 § Node
    /// Discovery, ADR 012 § Bootstrap step 5). Additive and self-attested: a
    /// stale cached address loses the iroh path race but never fails a dial that
    /// live infrastructure would have served, since on the outage path there is
    /// no discovery to fall back to anyway.
    ///
    /// `#[serde(default)]` so a record written before this field existed still
    /// loads — with empty addresses — instead of failing to deserialize and
    /// taking its latency and identity stats down with it. The field then
    /// repopulates on the next registry read.
    #[serde(default)]
    pub multiaddrs: Bytes,
    /// Seconds since the Unix epoch when identity was last confirmed against the registry.
    pub identity_seen_at_secs: u64,
    /// EWMA-smoothed probe round-trip time in milliseconds (dial to the signed
    /// `cdn/probe/v1` response); `None` until the first sample. Only probes feed
    /// it: on a cache miss a stream open's latency includes the node's own
    /// upstream work, so it measures the content's cache state, not the node's
    /// distance.
    pub latency_ms: Option<f64>,
    /// Seconds since the Unix epoch of the most recent latency sample.
    pub last_sampled_at_secs: Option<u64>,
    /// Most recent price quote from a probe or stream response; a diagnostic
    /// hint only, never authoritative.
    pub rate_per_mb: Option<u64>,
    /// Number of latency samples folded so far (gates EWMA warm-up).
    pub sample_count: u32,
    /// Seconds since the Unix epoch of the most recent failure, if any.
    pub last_failure_at_secs: Option<u64>,
}

impl PeerRecord {
    /// A latency sample exists and is younger than the TTL.
    #[must_use]
    pub(crate) const fn latency_fresh(&self, now_secs: u64, cfg: &StoreConfig) -> bool {
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

    /// Identity was confirmed against the registry within the refresh horizon, so
    /// the cached membership is fresh enough to build a candidate set from without
    /// re-reading the registry. Distinct from `latency_fresh`: identity and
    /// latency age on separate clocks, so a peer can be identity-fresh yet
    /// latency-stale — the case the registry-read-skip path serves by re-probing.
    #[must_use]
    pub const fn identity_fresh(&self, now_secs: u64, cfg: &StoreConfig) -> bool {
        now_secs.saturating_sub(self.identity_seen_at_secs) <= cfg.identity_refresh_secs
    }

    /// Eligible for the probe-less fast path: has a fresh latency sample and is not suppressed.
    #[must_use]
    pub const fn selectable(&self, now_secs: u64, cfg: &StoreConfig) -> bool {
        self.latency_ms.is_some()
            && self.latency_fresh(now_secs, cfg)
            && !self.failure_suppressed(now_secs, cfg)
    }

    /// Project the identity half back into a [`NodeCandidate`] for
    /// selection/fallback, carrying the cached `multiaddrs` so a registry-outage
    /// fallback dial can reach a reachable peer directly, without iroh discovery
    /// (ADR 012 § Bootstrap step 5).
    #[must_use]
    pub fn as_candidate(&self) -> NodeCandidate {
        NodeCandidate {
            node_id: self.node_id,
            eth_address: self.eth_address,
            region_hint: self.region_hint,
            multiaddrs: self.multiaddrs.clone(),
        }
    }

    /// Fold a new latency sample into the EWMA (first sample sets the value directly).
    pub(crate) fn fold_latency(&mut self, sample_ms: f64, alpha: f64) {
        self.latency_ms = Some(match self.latency_ms {
            Some(prev) => (1.0 - alpha) * prev + alpha * sample_ms,
            None => sample_ms,
        });
        self.sample_count = self.sample_count.saturating_add(1);
    }
}

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

/// Serializes every mutation of the store within this process. Each mutation
/// reads a whole record, changes it, and writes the whole record back, so two
/// unserialized writers lose one update: a stream open that races the
/// off-path probe harvest writes back a record read before the probe sample
/// landed. The lock is process-wide rather than per store, because each
/// [`PeerStore::open`] of one directory is a separate value. Concurrent
/// `decdn` processes are not serialized: the atomic rename keeps each record
/// whole, and the last writer wins.
static MUTATION_LOCK: Mutex<()> = Mutex::new(());

/// Take [`MUTATION_LOCK`]. The guarded value is `()`, so a writer that
/// panicked leaves nothing inconsistent behind and the poison is ignored.
fn mutation_guard() -> MutexGuard<'static, ()> {
    MUTATION_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

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
    ///
    /// A per-file read or decode error is logged via `tracing::warn!` and the
    /// file is skipped — one torn record must not take down the whole store,
    /// but a wholesale-unreadable store still shows up in logs instead of
    /// silently returning an empty peer set.
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
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "peer store: ignoring unreadable record"
                    );
                    continue;
                }
            };
            match serde_json::from_slice::<PeerRecord>(&bytes) {
                Ok(rec) => out.push(rec),
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "peer store: ignoring unreadable record"
                    );
                }
            }
        }
        out
    }

    /// Read one record by id.
    #[must_use]
    pub fn get(&self, node_id: &PublicKey) -> Option<PeerRecord> {
        let path = self.path_for(node_id);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "peer store: ignoring unreadable record"
                );
                return None;
            }
        };
        match serde_json::from_slice(&bytes) {
            Ok(rec) => Some(rec),
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "peer store: ignoring unreadable record"
                );
                None
            }
        }
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
        let _guard = mutation_guard();
        let mut rec = self.get(&cand.node_id).unwrap_or(PeerRecord {
            node_id: cand.node_id,
            eth_address: cand.eth_address,
            region_hint: cand.region_hint,
            multiaddrs: cand.multiaddrs.clone(),
            identity_seen_at_secs: now_secs,
            latency_ms: None,
            last_sampled_at_secs: None,
            rate_per_mb: None,
            sample_count: 0,
            last_failure_at_secs: None,
        });
        rec.eth_address = cand.eth_address;
        rec.region_hint = cand.region_hint;
        // Addresses ride the identity clock: refreshed on every registry read so
        // the cache tracks the latest published `multiaddrs`.
        rec.multiaddrs = cand.multiaddrs.clone();
        rec.identity_seen_at_secs = now_secs;
        self.write(&rec)
    }

    /// Fold a probe RTT sample, set price, stamp freshness, and clear any failure.
    pub fn record_sample(
        &self,
        node_id: &PublicKey,
        latency_ms: f64,
        rate_per_mb: u64,
        now_secs: u64,
        cfg: &StoreConfig,
    ) -> anyhow::Result<()> {
        let _guard = mutation_guard();
        let mut rec = self.get(node_id).unwrap_or(PeerRecord {
            node_id: *node_id,
            eth_address: Address::ZERO,
            region_hint: None,
            multiaddrs: Bytes::new(),
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

    /// File a successful stream open: set the quoted price and clear any
    /// failure. Folds no latency and leaves freshness unstamped, so neither the
    /// fast-path TTL nor the LRU order sees the open: on a cache miss the open's
    /// latency includes the node's own upstream work, not just its distance. A
    /// peer with no record is left alone: a stats-only placeholder is never
    /// selectable.
    pub fn record_open(&self, node_id: &PublicKey, rate_per_mb: u64) -> anyhow::Result<()> {
        let _guard = mutation_guard();
        let Some(mut rec) = self.get(node_id) else {
            return Ok(());
        };
        rec.rate_per_mb = Some(rate_per_mb);
        rec.last_failure_at_secs = None;
        self.write(&rec)
    }

    /// Stamp a failure so the peer is suppressed from selection.
    pub fn record_failure(&self, node_id: &PublicKey, now_secs: u64) -> anyhow::Result<()> {
        let _guard = mutation_guard();
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
        let _guard = mutation_guard();
        let mut records = self.load_all();
        records.retain(|r| {
            if r.identity_prunable(now_secs, cfg) {
                if let Err(e) = self.delete(&r.node_id) {
                    tracing::warn!(
                        peer = %r.node_id,
                        error = %e,
                        "peer store: failed to prune a stale record"
                    );
                }
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
mod tests;
