//! Reputation scoring for deCDN nodes (ADR 008).
//!
//! Local `PoC` scope (§14a): per-peer EWMA folded from delivery outcomes,
//! held in memory. Persistence is intentionally not implemented per
//! ADR 008 §14a.3 — this supersedes the `data_dir` requirement in issue #321.
//!
//! Network gossip aggregation over `cdn/reputation/v1` is deferred to
//! issue #326.

pub mod local;

pub use local::{ConfigError, LocalReputation, LocalReputationConfig, Outcome};
