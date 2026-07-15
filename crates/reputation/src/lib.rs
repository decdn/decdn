//! Reputation scoring for deCDN nodes (ADR 008).
//!
//! - [`local`] — per-peer EWMA folded from direct delivery outcomes (ADR 008
//!   §Local Score Calculation). In-memory; persistence deferred per §14a.3.
//! - [`network`] — gossip-aggregated, reporter-weighted network score (ADR 008
//!   §Network Score Aggregation, §Combined Score, §Score Decay), with the
//!   credibility weighting in [`settlement`].
//! - [`coverage`] — the derived per-operator regional-coverage signal (ADR 008
//!   §Regional-Coverage Reputation Signal).
//! - [`observation`] — the outbound-report buffer feeding the gossip publisher.
//!
//! `settlement` defines the [`SettlementSource`] trait seam implemented in the
//! `node` crate, keeping this crate free of any chain / incentive dependency.
//! (The staked-reporter gate lives at the gossip layer, not here.)

pub mod coverage;
pub mod interaction;
pub mod local;
pub mod network;
pub mod observation;
pub mod settlement;

pub use coverage::{CoverageBucket, RegionalCoverage};
pub use local::{
    ConfigError, LOCAL_SCORE_MAX_DELTA_PER_REPORT, LocalReputation, LocalReputationConfig, Outcome,
};
pub use network::{NetworkReputation, NetworkReputationConfig, ReportInput, combined_score};
pub use observation::ObservationBuffer;
pub use settlement::{
    SettlementRecord, SettlementSource, compute_reporter_weight, effective_settled_value,
};
