//! Pluggable cache admission and eviction policy traits (ADR 040).
//!
//! The engine owns the store and every safety exemption (pins, probe-holds,
//! deny/takedown). A policy is a pure decision function fed observations: it
//! ranks eligible candidates or classifies an admission, and never reaches
//! into the store.
use crate::{EvictionCandidates, Hash};
use std::collections::HashMap;

pub mod lru;
pub mod sketch;
pub mod tinylfu;

/// Where a freshly admitted blob is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Segment {
    /// One-hit-wonder holding area, capped and evicted first.
    Probation,
    /// Promoted, full-citizen cache.
    Main,
}

/// A policy's admission verdict for a miss about to be served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDecision {
    Store {
        segment: Segment,
    },
    /// Reserved: serve without storing. No shipped impl returns it; the engine
    /// treats it as `Store { Probation }` until a pass-through leg exists.
    PassThrough,
}

/// Inputs available to an admission decision.
#[derive(Debug, Clone)]
pub struct AdmissionContext {
    pub hash: Hash,
    /// Total blob size when known ahead of the fill, else `None`.
    pub known_size: Option<u64>,
}

pub trait AdmissionPolicy: Send + Sync + std::fmt::Debug {
    fn admit(&self, ctx: &AdmissionContext) -> AdmissionDecision;
}

/// Everything a policy needs to plan one eviction sweep. The engine assembles
/// it and never interprets the policy's semantics — `candidates` is already
/// stripped of pins/holds/deny, and `segments` is the generic `hash -> Segment`
/// membership the engine tracks with no meaning attached.
#[derive(Debug)]
pub struct EvictionContext<'a> {
    /// Eligible hashes with their last-access `Instant` (pin/hold/deny-filtered).
    pub candidates: &'a EvictionCandidates,
    /// Whole-store per-hash byte size.
    pub sizes: &'a HashMap<Hash, u64>,
    /// Generic segment membership the engine tracks (absent = `Segment::Main`).
    pub segments: &'a HashMap<Hash, Segment>,
    /// Effective footprint the sweep is deciding against.
    pub total_bytes: u64,
    /// Evict down to this many bytes.
    pub target_bytes: u64,
    /// Maximum releases this sweep.
    pub budget: u64,
    /// Configured cache size; a policy sizes its own caps from it (LRU ignores).
    pub cache_bytes: u64,
}

/// The single sweep-time decision: what leaves, and what graduates to a new
/// segment. `LruEviction` returns `promote` empty.
#[derive(Debug, Default)]
pub struct EvictionPlan {
    /// Release these hashes, in order.
    pub evict: Vec<Hash>,
    /// Move these hashes to a new segment (e.g. `Probation -> Main`).
    pub promote: Vec<(Hash, Segment)>,
}

pub trait EvictionPolicy: Send + Sync + std::fmt::Debug {
    /// The one sweep-time decision: what leaves, what graduates. A policy that
    /// ranks by frequency reads the shared [`FrequencyEstimator`] directly — the
    /// engine feeds that estimator on every serve. `candidates` is already
    /// stripped of pins/holds/deny by the engine.
    fn plan(&self, ctx: &EvictionContext<'_>) -> EvictionPlan;
}

/// Decayed frequency estimate shared by admission and eviction.
pub trait FrequencyEstimator: Send + Sync + std::fmt::Debug {
    fn observe(&self, hash: Hash);
    fn estimate(&self, hash: Hash) -> u32;
}

pub use lru::{AlwaysAdmit, LruEviction};
pub use tinylfu::{ProbationAdmission, TinyLfuEstimator, TinyLfuEviction};
