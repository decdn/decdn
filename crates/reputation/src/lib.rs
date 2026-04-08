//! Reputation system for deCDN.
//!
//! Collects and aggregates `ReputationReport` messages received via
//! iroh-gossip on the `cdn/reputation/v1` topic to produce per-node
//! reputation scores used in the unified selection formula.
