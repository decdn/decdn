//! Provider selection algorithm (ADR 001 §Node Selection Algorithm + ADR 008 §9).
//!
//! `rank_candidates` returns the input list ordered best-first (lowest score
//! first) with the four-tier tie-breaker applied. The caller iterates the
//! result in order and stops after `MAX_PROVIDER_ATTEMPTS` failed providers.

use decdn_protocol::gossip::LoadHint;

/// Maximum providers to attempt before reporting a fetch failure to the
/// caller (issue #322 — "max 3 provider attempts before returning error").
pub const MAX_PROVIDER_ATTEMPTS: usize = 3;

/// Reputation floor in the score denominator (ADR 001).
#[allow(dead_code)]
const REPUTATION_FLOOR: f64 = 0.1;

/// Score-equivalence threshold for tie-break activation (ADR 001 — "scores
/// within 1% of each other").
#[allow(dead_code)]
const TIE_THRESHOLD: f64 = 0.01;

/// A candidate provider produced by content discovery, ready to be ranked.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// Iroh `NodeId` (Ed25519 public key) of the candidate.
    pub node_id: [u8; 32],
    /// Quoted price in token base units per MB, from the most recent
    /// `ProbeResponse` (`decdn_protocol::message::ProbeResponse::rate_per_mb`).
    pub rate_per_mb: u64,
    /// Round-trip latency observed during probing.
    pub rtt_ms: u32,
    /// Local reputation in `[0.0, 1.0]` from the reputation engine.
    pub reputation: f32,
    /// Most recent advertised load from `NodeAnnounce`. Used by the
    /// load tie-break tier.
    pub load: LoadHint,
    /// ISO 3166-1 alpha-2 region from `NodeAnnounce`. Used by the
    /// geo-diversity tie-break tier.
    pub region: String,
    /// On-chain stake in TOKEN base units. `None` until on-chain stake
    /// lookup is wired (out of scope for issue #322); when populated,
    /// higher stake wins the stake tie-break tier.
    pub stake: Option<u64>,
}

/// A candidate paired with its computed selection score. Lower score is better.
#[derive(Debug, Clone)]
pub struct RankedCandidate {
    pub candidate: Candidate,
    pub score: f64,
}
