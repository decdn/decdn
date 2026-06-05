//! Per-region bandwidth accounting (#750).
//!
//! Aggregates bytes served (and, via a documented seam, pulled) keyed by the
//! counterparty peer's self-attested `NodeAnnounceBody.region` (ADR 030).
//! Region is resolved through [`RegionResolver`] — the production impl reads
//! the gossip peer table; tests inject a stub. Totals are cumulative since
//! process start (Prometheus-counter semantics) and live only in memory.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use decdn_common::admin::RegionBytes;
use decdn_gossip::PeerTable;
use tokio::sync::RwLock;

/// Region-bucket key for traffic whose counterparty has no known region — a
/// non-peer end-client, or a peer not currently in the gossip peer table.
/// Re-exported from [`decdn_common::admin`] so this crate and the wire DTOs
/// share a single sentinel.
pub use decdn_common::admin::UNKNOWN_REGION;

/// Resolve an iroh node id (32 raw bytes) to its self-attested region.
///
/// Async because the production impl reads the `tokio::sync::RwLock`-guarded
/// peer table. Boxed via `async-trait` so the accountant can hold an
/// `Arc<dyn RegionResolver>` and tests can inject a stub.
#[async_trait]
pub trait RegionResolver: Send + Sync {
    /// The peer's region code, or `None` if the peer is unknown.
    async fn region_of(&self, node_id: &[u8; 32]) -> Option<String>;
}

/// Production resolver: looks the node id up in the shared gossip peer table.
pub struct PeerTableResolver(Arc<RwLock<PeerTable>>);

impl PeerTableResolver {
    #[must_use]
    pub const fn new(peer_table: Arc<RwLock<PeerTable>>) -> Self {
        Self(peer_table)
    }
}

impl std::fmt::Debug for PeerTableResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `PeerTable` carries no `Debug` bound; name the struct without
        // formatting its lock-guarded interior.
        f.debug_struct("PeerTableResolver").finish_non_exhaustive()
    }
}

#[async_trait]
impl RegionResolver for PeerTableResolver {
    async fn region_of(&self, node_id: &[u8; 32]) -> Option<String> {
        let guard = self.0.read().await;
        guard.get(node_id).map(|e| e.announce.body.region.clone())
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
    /// **Forward-compatible seam (#750):** node-to-node paid pull-through is
    /// not orchestrated in production yet (the serving handler returns
    /// `NotFound` on a local miss; cache pull-through targets opaque S3/R2
    /// origins with no region). The future pull orchestrator calls this; until
    /// then `bytes_in` reads `0` in production.
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
        out.sort_by(|a, b| a.region.cmp(&b.region));
        out
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

    /// The production [`PeerTableResolver`] resolves a node id to the region
    /// carried on its `NodeAnnounce`, and reports `None` for an unknown id.
    #[tokio::test]
    async fn peer_table_resolver_reads_announce_region() {
        use decdn_gossip::PeerTable;
        use decdn_protocol::{NodeAnnounce, NodeAnnounceBody};

        let node_id = [4u8; 32];
        let announce = NodeAnnounce {
            body: NodeAnnounceBody {
                node_id,
                region: "FR".to_string(),
                timestamp_us: 1,
            },
            signature: vec![0u8; 64],
        };

        let mut table = PeerTable::new(0, 0);
        table.insert_or_refresh(announce, 100).unwrap();

        let resolver = PeerTableResolver::new(Arc::new(RwLock::new(table)));
        assert_eq!(resolver.region_of(&node_id).await, Some("FR".to_string()));
        assert_eq!(resolver.region_of(&[0u8; 32]).await, None);
    }
}
