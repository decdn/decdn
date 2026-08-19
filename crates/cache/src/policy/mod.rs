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

pub trait EvictionPolicy: Send + Sync + std::fmt::Debug {
    /// Return the eligible candidates in eviction order (first = evict first).
    /// `candidates` is already stripped of pins/holds/deny by the engine.
    fn select_victims(
        &self,
        candidates: &EvictionCandidates,
        sizes: &HashMap<Hash, u64>,
    ) -> Vec<Hash>;

    /// Optional per-access hook. Default no-op; `TinyLfu` forwards to its estimator.
    fn on_access(&self, _hash: Hash) {}
}

/// Decayed frequency estimate shared by admission and eviction.
pub trait FrequencyEstimator: Send + Sync + std::fmt::Debug {
    fn observe(&self, hash: Hash);
    fn estimate(&self, hash: Hash) -> u32;
}

pub use lru::{AlwaysAdmit, LruEviction};
