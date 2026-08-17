//! Per-region bandwidth accounting (#750).
//!
//! Aggregates bytes served (and, via a documented seam, pulled) keyed by the
//! counterparty peer's operator-attested region (ADR 030), resolved from the
//! on-chain `CapacityBond` registry projection rather than gossip. Region is
//! resolved through [`RegionResolver`] — the production impl reads the
//! registry's `NodeId → regionHint` map; tests inject a stub. Totals are
//! cumulative since process start (Prometheus-counter semantics) and live
//! only in memory.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use decdn_common::admin::RegionBytes;

use crate::dht::routing::NodeId;

/// Region-bucket key for traffic whose counterparty has no known region — a
/// non-peer end-client, or a peer not currently in the registry's region
/// projection. Re-exported from [`decdn_common::admin`] so this crate and the
/// wire DTOs share a single sentinel.
pub use decdn_common::admin::UNKNOWN_REGION;

/// Resolve an iroh node id (32 raw bytes) to its operator-attested region.
///
/// Async because the trait is boxed via `async-trait` so the accountant can
/// hold an `Arc<dyn RegionResolver>` and tests can inject a stub; the
/// production impl's body is a synchronous map read.
#[async_trait]
pub trait RegionResolver: Send + Sync {
    /// The peer's region code, or `None` if the peer is unknown.
    async fn region_of(&self, node_id: &[u8; 32]) -> Option<String>;
}

/// Production resolver: reads the `NodeId → regionHint` projection kept current
/// by the `CapacityBond` registry watcher (the same enumeration + event tail that
/// maintains the active set). Region is operator-attested on-chain via
/// `registerNode`; the ADR-030 RTT penalty guards against a mismatched claim.
pub struct RegistryRegionResolver(Arc<RwLock<HashMap<NodeId, String>>>);

impl RegistryRegionResolver {
    #[must_use]
    pub const fn new(regions: Arc<RwLock<HashMap<NodeId, String>>>) -> Self {
        Self(regions)
    }
}

impl std::fmt::Debug for RegistryRegionResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryRegionResolver")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl RegionResolver for RegistryRegionResolver {
    async fn region_of(&self, node_id: &[u8; 32]) -> Option<String> {
        // A poisoned lock (a writer panicked) resolves to `None` — an unknown
        // region, folded into the UNKNOWN bucket by `region_for`. The registry
        // watcher's writes are short infallible map swaps, so poisoning is not
        // expected in practice.
        let guard = self.0.read().ok()?;
        guard.get(&NodeId::from_bytes(*node_id)).cloned()
    }
}

/// Cumulative byte counters for one region.
#[derive(Debug, Default, Clone, Copy)]
struct RegionTotals {
    bytes_in: u64,
    bytes_out: u64,
}

/// Which counter a recording advances.
#[derive(Clone, Copy)]
enum Direction {
    In,
    Out,
}

/// In-memory per-region byte accumulator.
pub struct RegionAccountant {
    resolver: Arc<dyn RegionResolver>,
    totals: Mutex<HashMap<String, RegionTotals>>,
}

impl std::fmt::Debug for RegionAccountant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The trait-object resolver carries no `Debug` bound; name the struct
        // without formatting it, mirroring `ChannelStatusHandles` in admin.rs.
        f.debug_struct("RegionAccountant").finish_non_exhaustive()
    }
}

impl RegionAccountant {
    #[must_use]
    pub fn new(resolver: Arc<dyn RegionResolver>) -> Self {
        Self {
            resolver,
            totals: Mutex::new(HashMap::new()),
        }
    }

    /// Record `bytes` served OUT to `peer`, bucketed by `peer`'s region (or
    /// [`UNKNOWN_REGION`]).
    pub async fn record_served(&self, peer: &[u8; 32], bytes: u64) {
        let region = self.region_for(peer).await;
        self.bump(region, Direction::Out, bytes);
    }

    /// Record `bytes` pulled IN from `peer`, bucketed by `peer`'s region.
    ///
    /// The inbound counterpart of [`record_served`](Self::record_served): the
    /// node-to-node pull orchestrator (#831) calls this on each delivered
    /// cache-miss pull-through, so `bytes_in` reconciles pull spend by upstream
    /// region (#858). Cache pull-through against opaque S3/R2 origins has no
    /// resolvable region and is not recorded here.
    pub async fn record_pulled(&self, peer: &[u8; 32], bytes: u64) {
        let region = self.region_for(peer).await;
        self.bump(region, Direction::In, bytes);
    }

    /// Region-sorted snapshot of all buckets.
    #[must_use]
    pub fn snapshot(&self) -> Vec<RegionBytes> {
        // Poison is recovered (counters are monotonic) — see `bump`.
        let totals = match self.totals.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!(
                    "region accountant totals lock poisoned (holder panicked); recovering"
                );
                poisoned.into_inner()
            }
        };
        let mut out: Vec<RegionBytes> = totals
            .iter()
            .map(|(region, t)| RegionBytes {
                region: region.clone(),
                bytes_in: t.bytes_in,
                bytes_out: t.bytes_out,
            })
            .collect();
        out.sort_unstable_by(|a, b| a.region.cmp(&b.region));
        out
    }

    /// The peer's operator-attested region, or `None` if it is not in the
    /// registry's region projection. Exposes the shared [`RegionResolver`] so
    /// the pull path can read a candidate's region (ADR 030) without wiring a
    /// second resolver — unlike the private `region_for`, it does not fold an
    /// unknown peer into the [`UNKNOWN_REGION`] bucket.
    pub async fn region_of(&self, peer: &[u8; 32]) -> Option<String> {
        self.resolver.region_of(peer).await
    }

    async fn region_for(&self, peer: &[u8; 32]) -> String {
        self.resolver
            .region_of(peer)
            .await
            .unwrap_or_else(|| UNKNOWN_REGION.to_string())
    }

    fn bump(&self, region: String, dir: Direction, bytes: u64) {
        let mut totals = match self.totals.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                // A poisoned std::sync::Mutex means a holder panicked. The counters are
                // monotonic, so recover the guard and continue rather than dropping the
                // update — a poisoned lock persists for the process lifetime, so dropping
                // would permanently flatline accounting. error! matches the crate's
                // poisoned-lock convention (see handlers/dht.rs).
                tracing::error!(
                    %region,
                    "region accountant totals lock poisoned (holder panicked); recovering"
                );
                poisoned.into_inner()
            }
        };
        let entry = totals.entry(region).or_default();
        match dir {
            Direction::In => entry.bytes_in = entry.bytes_in.saturating_add(bytes),
            Direction::Out => entry.bytes_out = entry.bytes_out.saturating_add(bytes),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Stub resolver backed by a fixed node-id → region map.
    struct StubResolver(HashMap<[u8; 32], String>);

    #[async_trait]
    impl RegionResolver for StubResolver {
        async fn region_of(&self, node_id: &[u8; 32]) -> Option<String> {
            self.0.get(node_id).cloned()
        }
    }

    fn accountant_with(map: HashMap<[u8; 32], String>) -> RegionAccountant {
        RegionAccountant::new(Arc::new(StubResolver(map)))
    }

    #[tokio::test]
    async fn served_bytes_bucket_by_resolved_region() {
        let peer = [1u8; 32];
        let mut map = HashMap::new();
        map.insert(peer, "DE".to_string());
        let acc = accountant_with(map);

        acc.record_served(&peer, 1000).await;
        acc.record_served(&peer, 500).await;

        let snap = acc.snapshot();
        assert_eq!(snap.len(), 1);
        let de = snap.first().unwrap();
        assert_eq!(de.region, "DE");
        assert_eq!(de.bytes_out, 1500);
        assert_eq!(de.bytes_in, 0);
    }

    #[tokio::test]
    async fn region_of_returns_claim_or_none() {
        let peer = [5u8; 32];
        let mut map = HashMap::new();
        map.insert(peer, "DE".to_string());
        let acc = accountant_with(map);
        // Known peer → its self-attested region; unlike `region_for`, an unknown
        // peer resolves to `None` rather than the UNKNOWN_REGION bucket.
        assert_eq!(acc.region_of(&peer).await, Some("DE".to_string()));
        assert_eq!(acc.region_of(&[0u8; 32]).await, None);
    }

    #[tokio::test]
    async fn unknown_peer_lands_in_unknown_bucket() {
        let acc = accountant_with(HashMap::new());
        acc.record_served(&[9u8; 32], 42).await;

        let snap = acc.snapshot();
        let only = snap.first().unwrap();
        assert_eq!(only.region, UNKNOWN_REGION);
        assert_eq!(only.bytes_out, 42);
    }

    #[tokio::test]
    async fn pulled_bytes_accumulate_into_bytes_in() {
        let peer = [2u8; 32];
        let mut map = HashMap::new();
        map.insert(peer, "FR".to_string());
        let acc = accountant_with(map);

        acc.record_pulled(&peer, 700).await;
        acc.record_served(&peer, 300).await;

        let only = acc.snapshot().into_iter().next().unwrap();
        assert_eq!(only.region, "FR");
        assert_eq!(only.bytes_in, 700);
        assert_eq!(only.bytes_out, 300);
    }

    #[tokio::test]
    async fn snapshot_is_region_sorted() {
        let (a, b, c) = ([1u8; 32], [2u8; 32], [3u8; 32]);
        let mut map = HashMap::new();
        map.insert(a, "US".to_string());
        map.insert(b, "DE".to_string());
        map.insert(c, "FR".to_string());
        let acc = accountant_with(map);
        acc.record_served(&a, 1).await;
        acc.record_served(&b, 1).await;
        acc.record_served(&c, 1).await;

        let regions: Vec<String> = acc.snapshot().into_iter().map(|r| r.region).collect();
        assert_eq!(regions, vec!["DE", "FR", "US"]);
    }

    #[tokio::test]
    async fn served_bytes_saturate_at_u64_max() {
        let peer = [7u8; 32];
        let mut map = HashMap::new();
        map.insert(peer, "DE".to_string());
        let acc = accountant_with(map);

        acc.record_served(&peer, u64::MAX).await;
        acc.record_served(&peer, 1).await; // would overflow → must saturate

        assert_eq!(acc.snapshot().first().unwrap().bytes_out, u64::MAX);
    }

    /// 100 concurrent `record_served` calls against one shared accountant must
    /// all land — guards the accumulator under contention (no lost updates).
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_record_served_sums_without_loss() {
        let peer = [3u8; 32];
        let mut map = HashMap::new();
        map.insert(peer, "DE".to_string());
        let acc = Arc::new(accountant_with(map));

        let mut handles = Vec::new();
        for _ in 0..100 {
            let acc = Arc::clone(&acc);
            handles.push(tokio::spawn(async move {
                acc.record_served(&peer, 1000).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let de = acc.snapshot().into_iter().next().unwrap();
        assert_eq!(de.region, "DE");
        assert_eq!(de.bytes_out, 100_000);
    }

    /// The production [`RegistryRegionResolver`] resolves a node id to the region
    /// captured from the on-chain registry, and reports `None` for an unknown id.
    #[tokio::test]
    async fn registry_region_resolver_reads_the_region_map() {
        use std::sync::RwLock as StdRwLock;

        use crate::dht::routing::NodeId;

        let node_id = [4u8; 32];
        let mut map = HashMap::new();
        map.insert(NodeId::from_bytes(node_id), "FR".to_string());

        let resolver = RegistryRegionResolver::new(Arc::new(StdRwLock::new(map)));
        assert_eq!(resolver.region_of(&node_id).await, Some("FR".to_string()));
        assert_eq!(resolver.region_of(&[0u8; 32]).await, None);
    }
}
