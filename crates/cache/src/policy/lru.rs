//! Behavior-preserving default policies: recency eviction, unconditional admit.
use super::{
    AdmissionContext, AdmissionDecision, AdmissionPolicy, EvictionContext, EvictionPlan,
    EvictionPolicy, Segment,
};
use crate::Hash;

/// Evicts by last access, oldest first, until the sweep reaches
/// `target_bytes` or spends its budget. Promotes nothing.
///
/// Equal recencies — every blob that carries the open-time recency seed shares
/// one instant — release largest first, so the sweep reaches its target in
/// fewer releases.
#[derive(Debug, Default, Clone, Copy)]
pub struct LruEviction;

impl EvictionPolicy for LruEviction {
    fn plan(&self, ctx: &EvictionContext<'_>) -> EvictionPlan {
        let size = |h: &Hash| ctx.sizes.get(h).copied().unwrap_or(0);
        let mut ordered: Vec<(Hash, std::time::Instant)> =
            ctx.candidates.iter().map(|(h, t)| (*h, *t)).collect();
        // Oldest access first; equal recencies largest first.
        ordered.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| size(&b.0).cmp(&size(&a.0))));
        let mut evict = Vec::new();
        let mut freed = 0u64;
        for (h, _) in ordered {
            if ctx.total_bytes.saturating_sub(freed) <= ctx.target_bytes {
                break;
            }
            if evict.len() as u64 >= ctx.budget {
                break;
            }
            freed = freed.saturating_add(ctx.sizes.get(&h).copied().unwrap_or(0));
            evict.push(h);
        }
        EvictionPlan {
            evict,
            promote: Vec::new(),
        }
    }
}

/// Stores every miss, straight into [`Segment::Main`]. The default admission
/// policy, and the one that makes the segment map inert.
#[derive(Debug, Default, Clone, Copy)]
pub struct AlwaysAdmit;

impl AdmissionPolicy for AlwaysAdmit {
    fn admit(&self, _ctx: &AdmissionContext) -> AdmissionDecision {
        AdmissionDecision::Store {
            segment: Segment::Main,
        }
    }
}

#[cfg(test)]
mod tests;
