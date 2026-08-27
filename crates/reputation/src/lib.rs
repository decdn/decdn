//! Reputation scoring for deCDN nodes (ADR 008).
//!
//! - [`local`] — per-peer EWMA folded from direct delivery outcomes (ADR 008
//!   §Local Score Calculation). In-memory; persistence deferred per §14a.3.
//! - `interaction` (crate-private) — the per-outcome interaction-quality
//!   helpers that map an observed delivery (speed, correctness) to a `[0,1]`
//!   score fed into the local EWMA.
//!
//! This crate scores only from direct, first-party delivery outcomes.

mod interaction;
pub mod local;

pub use local::{
    ConfigError, LOCAL_SCORE_MAX_DELTA_PER_REPORT, LocalReputation, LocalReputationConfig, Outcome,
};
