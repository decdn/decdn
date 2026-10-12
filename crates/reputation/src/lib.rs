//! Local reputation scoring for deCDN nodes: each node scores the peers it
//! deals with from its own delivery outcomes ([ADR 008]).
//!
//! [`LocalReputation`] keeps one EWMA score in `[0, 1]` per peer. Feed it each
//! observed delivery with [`LocalReputation::record`], and read a peer's score
//! with [`LocalReputation::score`]. An idle score decays back toward neutral
//! ([ADR 008 § Score Decay]). [`LocalReputationConfig`] sets the smoothing
//! factor, the neutral score, the decay half-life, and the outcome weights
//! ([ADR 008 § Local Score Calculation]).
//!
//! Every score comes from direct, first-party outcomes. Nodes do not share or
//! aggregate scores. Scores live in memory only, so a restart starts every peer
//! at neutral.
//!
//! [ADR 008]: https://github.com/decdn/decdn/blob/main/adr/008-reputation.md
//! [ADR 008 § Score Decay]: https://github.com/decdn/decdn/blob/main/adr/008-reputation.md#score-decay
//! [ADR 008 § Local Score Calculation]: https://github.com/decdn/decdn/blob/main/adr/008-reputation.md#local-score-calculation

mod interaction;
pub mod local;

pub use local::{
    ConfigError, LOCAL_SCORE_MAX_DELTA_PER_REPORT, LocalReputation, LocalReputationConfig, Outcome,
};
