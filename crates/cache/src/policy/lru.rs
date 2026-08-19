//! Behavior-preserving default policies: recency eviction, unconditional admit.
use super::{AdmissionContext, AdmissionDecision, AdmissionPolicy, EvictionPolicy, Segment};
use crate::{EvictionCandidates, Hash};
use std::collections::HashMap;

#[derive(Debug, Default, Clone, Copy)]
pub struct LruEviction;

impl EvictionPolicy for LruEviction {
    fn select_victims(
        &self,
        candidates: &EvictionCandidates,
        _sizes: &HashMap<Hash, u64>,
    ) -> Vec<Hash> {
        let mut ordered: Vec<(Hash, std::time::Instant)> =
            candidates.iter().map(|(h, t)| (*h, *t)).collect();
        ordered.sort_by_key(|(_, last)| *last); // oldest access first
        ordered.into_iter().map(|(h, _)| h).collect()
    }
}

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
mod tests {
    use super::*;
    use crate::policy::{
        AdmissionContext, AdmissionDecision, AdmissionPolicy, EvictionPolicy, Segment,
    };
    use crate::{EvictionCandidates, Hash};
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    fn h(b: u8) -> Hash {
        Hash::from([b; 32])
    }

    #[test]
    fn lru_orders_oldest_access_first() {
        let now = Instant::now();
        let mut map = HashMap::new();
        map.insert(h(1), now); // newest
        map.insert(h(2), now.checked_sub(Duration::from_mins(1)).unwrap_or(now)); // oldest
        map.insert(
            h(3),
            now.checked_sub(Duration::from_secs(30)).unwrap_or(now),
        );
        let candidates = EvictionCandidates::from_map_for_test(map);
        let sizes = HashMap::new();
        let victims = LruEviction.select_victims(&candidates, &sizes);
        assert_eq!(victims, vec![h(2), h(3), h(1)]);
    }

    #[test]
    #[allow(clippy::panic)]
    fn always_admit_stores_to_main() {
        let ctx = AdmissionContext {
            hash: h(1),
            known_size: None,
        };
        match AlwaysAdmit.admit(&ctx) {
            AdmissionDecision::Store {
                segment: Segment::Main,
            } => {}
            other => panic!("expected Store/Main, got {other:?}"),
        }
    }
}
