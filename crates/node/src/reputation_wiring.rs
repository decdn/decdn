//! Node-side wiring that connects the `decdn-reputation` engine and the
//! `decdn-gossip` reputation transport (issue #326, ADR 008).
//!
//! The leaf crates define trait seams; this module implements them over the
//! node's concrete subsystems:
//!
//! - [`NodeStakedReporterSet`] gates inbound reports to staked nodes via the
//!   chain [`StakerSet`].
//! - [`NodeSettlementSource`] supplies reporter-credibility settlement history.
//! - [`NodeReputationSink`] folds validated reports into the network score and
//!   the regional-coverage map.
//! - [`NodeReportDrain`] feeds the publisher from the observation buffer.
//!
//! # Known limitations
//!
//! 1. **Reporter weights are fed by a bounded, in-memory indexer.**
//!    [`crate::reputation_indexer::SettlementIndexer`] now feeds
//!    [`NodeSettlementSource`] from network-wide `ChannelSettled` events, so
//!    `compute_reporter_weight` returns real weights. Because the source is
//!    in-memory it is rebuilt each boot from a bounded recent block window;
//!    settlements older than that window (and during resubscribe gaps) are not
//!    counted, and `staked_counterparty` uses *current* membership. Durable,
//!    full-52-week indexing is a refinement.
//! 2. **Outbound capture is not wired (tracked by #831).** The publisher drains
//!    [`NodeReportDrain`], but the delivery/probe hot paths do not yet record
//!    outcomes into the [`ObservationBuffer`] (the ADR 008 §Local Score wiring),
//!    so the node receives but does not emit reports. This is gated on the
//!    node-to-node cache-miss pull-through orchestration not yet existing.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use decdn_gossip::{PeerTable, ReportDrain, ReputationSink, StakedReporterSet, ValidatedReport};
use decdn_protocol::{NodeId as ProtocolNodeId, ReportMetrics};
use decdn_reputation::{
    NetworkReputation, ObservationBuffer, RegionalCoverage, ReportInput, SettlementRecord,
    SettlementSource, compute_reporter_weight, effective_settled_value,
};
use iroh::PublicKey;

use crate::dht::staker_set::StakerSet;

/// Wall-clock seconds since the Unix epoch. A clock before the epoch yields
/// `0` (the reputation math saturates rather than panicking).
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Adapts the chain [`StakerSet`] to the gossip [`StakedReporterSet`] gate:
/// only staked nodes may submit reputation reports (ADR 008 §Gossip Protocol).
#[derive(Debug)]
pub struct NodeStakedReporterSet {
    staker_set: Arc<dyn StakerSet>,
}

impl NodeStakedReporterSet {
    /// Wrap the runtime's staker set.
    pub fn new(staker_set: Arc<dyn StakerSet>) -> Self {
        Self { staker_set }
    }
}

impl StakedReporterSet for NodeStakedReporterSet {
    fn contains(&self, reporter: &[u8; 32]) -> bool {
        // `NodeId` is a freely-constructible 32-byte newtype; the bytes were
        // already signature-verified in the gossip validator.
        self.staker_set
            .is_active(&ProtocolNodeId::from_bytes(*reporter))
    }
}

/// One settlement attributed to a reporter, stored with its absolute
/// settlement time so [`SettlementSource::settlements`] can recompute age at
/// query time.
#[derive(Debug, Clone, Copy)]
struct StoredSettlement {
    amount_usdc: u128,
    settled_at_secs: u64,
    /// Counterparty address if it was a staked node at settlement time.
    staked_counterparty: Option<[u8; 20]>,
}

/// Settlements older than this contribute `0` to the weight (ADR 008: 52
/// weeks). Records past this age are pruned on insert so the in-memory vec
/// stays bounded — the engine already ignores them, this just caps memory.
const SETTLEMENT_MAX_AGE_SECS: u64 = 52 * 7 * 24 * 3600;

/// Node-side [`SettlementSource`]: a reporter's settlement history keyed by
/// `NodeId`, fed by [`crate::reputation_indexer::SettlementIndexer`] via
/// [`Self::record_settlement`]. Reporters with no recorded history yield an
/// empty `Vec` (fail-closed → weight 0).
#[derive(Debug)]
pub struct NodeSettlementSource {
    by_reporter: RwLock<HashMap<PublicKey, Vec<StoredSettlement>>>,
    min_counterparties: u32,
}

impl NodeSettlementSource {
    /// Create an empty source using `min_counterparties` for the diversity
    /// discount (ADR 008 §Distinct-Counterparty Discount).
    pub fn new(min_counterparties: u32) -> Self {
        Self {
            by_reporter: RwLock::new(HashMap::new()),
            min_counterparties,
        }
    }

    /// Record a settlement attributed to `reporter` (called by the settlement
    /// indexer). Prunes the reporter's records older than the 52-week window on
    /// insert to keep the in-memory vec bounded.
    pub fn record_settlement(
        &self,
        reporter: PublicKey,
        amount_usdc: u128,
        settled_at_secs: u64,
        staked_counterparty: Option<[u8; 20]>,
    ) {
        let cutoff = now_secs().saturating_sub(SETTLEMENT_MAX_AGE_SECS);
        let mut guard = self
            .by_reporter
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let records = guard.entry(reporter).or_default();
        records.push(StoredSettlement {
            amount_usdc,
            settled_at_secs,
            staked_counterparty,
        });
        records.retain(|s| s.settled_at_secs >= cutoff);
    }

    fn records_for(stored: &[StoredSettlement], now: u64) -> Vec<SettlementRecord> {
        stored
            .iter()
            .map(|s| SettlementRecord {
                amount_usdc: s.amount_usdc,
                age_secs: now.saturating_sub(s.settled_at_secs),
                staked_counterparty: s.staked_counterparty,
            })
            .collect()
    }
}

impl SettlementSource for NodeSettlementSource {
    fn settlements(&self, reporter: PublicKey) -> Vec<SettlementRecord> {
        let now = now_secs();
        let guard = self
            .by_reporter
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .get(&reporter)
            .map(|stored| Self::records_for(stored, now))
            .unwrap_or_default()
    }

    fn max_effective_settled_value(&self) -> f64 {
        let now = now_secs();
        let guard = self
            .by_reporter
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .values()
            .map(|stored| {
                effective_settled_value(&Self::records_for(stored, now), self.min_counterparties)
            })
            .fold(0.0_f64, f64::max)
    }
}

/// Adapts the [`ObservationBuffer`] to the gossip [`ReportDrain`], converting
/// `NodeId` keys to their raw bytes for the wire.
#[derive(Debug)]
pub struct NodeReportDrain {
    buffer: Arc<ObservationBuffer>,
}

impl NodeReportDrain {
    /// Wrap the shared observation buffer.
    pub const fn new(buffer: Arc<ObservationBuffer>) -> Self {
        Self { buffer }
    }
}

impl ReportDrain for NodeReportDrain {
    fn drain(&self) -> Vec<([u8; 32], ReportMetrics)> {
        self.buffer
            .drain()
            .into_iter()
            .map(|(peer, metrics)| (*peer.as_bytes(), metrics))
            .collect()
    }
}

/// Folds validated inbound reports into the network score and the
/// regional-coverage map (ADR 008 §Network Score Aggregation, §Update rule).
pub struct NodeReputationSink {
    network: Arc<NetworkReputation>,
    coverage: Arc<RegionalCoverage>,
    settlement: Arc<NodeSettlementSource>,
    peer_table: Arc<tokio::sync::RwLock<PeerTable>>,
    min_counterparties: u32,
}

impl NodeReputationSink {
    /// Build the sink from the aggregation state and the peer table (used to
    /// resolve a reporter's attested region for regional coverage).
    pub const fn new(
        network: Arc<NetworkReputation>,
        coverage: Arc<RegionalCoverage>,
        settlement: Arc<NodeSettlementSource>,
        peer_table: Arc<tokio::sync::RwLock<PeerTable>>,
        min_counterparties: u32,
    ) -> Self {
        Self {
            network,
            coverage,
            settlement,
            peer_table,
            min_counterparties,
        }
    }
}

impl std::fmt::Debug for NodeReputationSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeReputationSink")
            .field("min_counterparties", &self.min_counterparties)
            .finish_non_exhaustive()
    }
}

impl ReputationSink for NodeReputationSink {
    fn accept(&self, report: ValidatedReport) {
        // Both keys must be valid curve points to address the engine maps. The
        // reporter is already signature-verified upstream; a malformed
        // `provider` field just means we drop this report.
        let (Ok(reporter), Ok(provider)) = (
            PublicKey::from_bytes(&report.reporter),
            PublicKey::from_bytes(&report.provider),
        ) else {
            return;
        };
        let now = now_secs();
        let weight =
            compute_reporter_weight(self.settlement.as_ref(), reporter, self.min_counterparties);

        self.network.record(
            &ReportInput {
                provider,
                reporter,
                delivery_speed: report.delivery_speed,
                uptime_observed: report.uptime_observed,
                data_correct: report.data_correct,
                now_secs: now,
            },
            weight,
        );

        // Regional coverage is best-effort: it needs the reporter's attested
        // region from the peer table (ADR 008 §Update rule step 1). A missing
        // region drops the report from regional aggregation only — the network
        // score above still updated. `try_read` keeps `accept` non-blocking on
        // the gossip hot path.
        let region = self.peer_table.try_read().ok().and_then(|table| {
            table
                .get(&report.reporter)
                .and_then(|entry| <[u8; 2]>::try_from(entry.announce.body.region.as_bytes()).ok())
        });
        if let Some(region) = region {
            let interaction = self.network.config().interaction_score(
                report.delivery_speed,
                report.uptime_observed,
                report.data_correct,
            );
            self.coverage
                .record(provider, region, interaction, weight, now);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use decdn_reputation::NetworkReputationConfig;
    use iroh::SecretKey;

    fn pk() -> PublicKey {
        SecretKey::generate().public()
    }

    #[test]
    fn settlement_source_unknown_reporter_is_empty_and_zero() {
        let src = NodeSettlementSource::new(5);
        assert!(src.settlements(pk()).is_empty());
        assert!(src.max_effective_settled_value().abs() < 1e-12);
    }

    #[test]
    fn settlement_source_records_feed_weight() {
        let src = NodeSettlementSource::new(5);
        let reporter = pk();
        let now = now_secs();
        // Five distinct staked counterparties → full diversity.
        for i in 0..5u8 {
            src.record_settlement(reporter, 1_000_000, now, Some([i; 20]));
        }
        assert!(!src.settlements(reporter).is_empty());
        assert!(src.max_effective_settled_value() > 0.0);
        let w = compute_reporter_weight(&src, reporter, 5);
        assert!(w > 0.0, "weight should be positive, got {w}");
    }

    #[test]
    fn record_settlement_prunes_stale_records() {
        let src = NodeSettlementSource::new(5);
        let r = pk();
        let now = now_secs();
        // 53 weeks old → outside the 52-week window; pruned on the next insert.
        src.record_settlement(r, 1_000_000, now - 53 * 7 * 24 * 3600, Some([1u8; 20]));
        src.record_settlement(r, 1_000_000, now, Some([2u8; 20]));
        assert_eq!(
            src.settlements(r).len(),
            1,
            "the stale record must be pruned on insert"
        );
    }

    #[tokio::test]
    async fn sink_drives_regional_coverage_from_reporter_region() {
        use decdn_protocol::{NodeAnnounce, NodeAnnounceBody};

        let network = Arc::new(NetworkReputation::new(NetworkReputationConfig::default()).unwrap());
        let coverage = Arc::new(RegionalCoverage::new(NetworkReputationConfig::default()).unwrap());
        let settlement = Arc::new(NodeSettlementSource::new(5));
        let peer_table = Arc::new(tokio::sync::RwLock::new(PeerTable::new(60_000_000, 128)));
        let provider = pk();
        let now = now_secs();

        // Three weighted reporters, each with a peer-table entry attesting "DE",
        // so the sink resolves the region and folds into regional coverage.
        let mut reporters = Vec::new();
        for _ in 0..3 {
            let r = pk();
            for i in 0..5u8 {
                settlement.record_settlement(r, 10_000_000, now, Some([i; 20]));
            }
            let announce = NodeAnnounce {
                body: NodeAnnounceBody {
                    node_id: *r.as_bytes(),
                    region: "DE".to_string(),
                    timestamp_us: 1,
                },
                signature: vec![0u8; 64],
            };
            peer_table
                .write()
                .await
                .insert_or_refresh(announce, 1)
                .expect("seed peer entry");
            reporters.push(r);
        }

        let sink = NodeReputationSink::new(
            Arc::clone(&network),
            Arc::clone(&coverage),
            settlement,
            Arc::clone(&peer_table),
            5,
        );
        for r in reporters {
            sink.accept(ValidatedReport {
                provider: *provider.as_bytes(),
                reporter: *r.as_bytes(),
                delivery_speed: Some(10 * 1024 * 1024),
                uptime_observed: Some(true),
                data_correct: Some(true),
                timestamp_secs: now,
            });
        }
        let de = coverage
            .coverage(provider, *b"DE", now)
            .expect("DE coverage present");
        assert!(
            de > 0.5,
            "positive DE reports should raise coverage, got {de}"
        );
        // A region the operator never served has no signal.
        assert!(coverage.coverage(provider, *b"US", now).is_none());
    }

    #[tokio::test]
    async fn sink_with_weighted_reporter_moves_network_score() {
        let network = Arc::new(NetworkReputation::new(NetworkReputationConfig::default()).unwrap());
        let coverage = Arc::new(RegionalCoverage::new(NetworkReputationConfig::default()).unwrap());
        let settlement = Arc::new(NodeSettlementSource::new(5));
        let peer_table = Arc::new(tokio::sync::RwLock::new(PeerTable::new(60_000_000, 128)));

        let provider = pk();
        // Three distinct reporters, each with real settled value → non-zero
        // weight, so the threshold is met and the score departs from neutral.
        let now = now_secs();
        let mut reporters = Vec::new();
        for _ in 0..3 {
            let r = pk();
            for i in 0..5u8 {
                settlement.record_settlement(r, 10_000_000, now, Some([i; 20]));
            }
            reporters.push(r);
        }
        let sink =
            NodeReputationSink::new(Arc::clone(&network), coverage, settlement, peer_table, 5);
        for r in reporters {
            sink.accept(ValidatedReport {
                provider: *provider.as_bytes(),
                reporter: *r.as_bytes(),
                delivery_speed: Some(10 * 1024 * 1024),
                uptime_observed: Some(true),
                data_correct: Some(true),
                timestamp_secs: now,
            });
        }
        assert!(network.is_scored(provider), "threshold should be met");
        assert!(
            network.score(provider, now) > 0.5,
            "positive reports should raise the score above neutral"
        );
    }
}
