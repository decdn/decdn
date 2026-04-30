//! In-memory peer table keyed by node ID.
//!
//! Stores the most recent validated [`NodeAnnounce`] per peer. Monotonic
//! timestamp enforcement and TTL eviction live here so the subscriber loop
//! can treat inserts as a single operation.

use std::collections::HashMap;

use decdn_protocol::NodeAnnounce;

/// One entry in the peer table.
#[derive(Debug, Clone)]
pub struct PeerEntry {
    /// Most recent signature-valid announce for this peer.
    pub announce: NodeAnnounce,
    /// Wall-clock microseconds when the first entry for this peer was inserted.
    pub first_seen_us: u64,
    /// Wall-clock microseconds when the entry was last refreshed.
    pub last_seen_us: u64,
}

/// Result of an `insert_or_refresh` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// No prior entry for this node ID.
    Inserted,
    /// Existing entry refreshed with a newer announce.
    Refreshed,
}

/// Error returned by [`PeerTable::insert_or_refresh`] when the incoming
/// announce's timestamp is not strictly greater than the stored entry's.
///
/// A named struct (rather than a bare `u64`) so a future second failure mode
/// can be added without silently mislabeling metrics that match this variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaleTimestamp {
    /// Microseconds-since-epoch timestamp of the entry already stored.
    pub existing_us: u64,
}

/// In-memory, TTL-bounded peer table.
#[derive(Debug)]
pub struct PeerTable {
    entries: HashMap<[u8; 32], PeerEntry>,
    ttl_us: u64,
}

impl PeerTable {
    /// Create an empty table. `ttl_us` is how long an unrefreshed entry may
    /// live — pass `0` to disable TTL (useful in tests).
    pub fn new(ttl_us: u64) -> Self {
        Self {
            entries: HashMap::new(),
            ttl_us,
        }
    }

    /// Number of live entries. Does not trigger eviction.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Borrow the existing entry for a node, if any.
    pub fn get(&self, node_id: &[u8; 32]) -> Option<&PeerEntry> {
        self.entries.get(node_id)
    }

    /// Insert or refresh an entry. Caller has already validated the announce
    /// (signature, timestamp skew, etc.) — this function only enforces
    /// monotonicity against the existing entry.
    ///
    /// # Errors
    /// Returns [`StaleTimestamp`] if `announce.body.timestamp_us` is not
    /// strictly greater than the stored entry's timestamp. The caller maps
    /// this into [`crate::AnnounceReject::StaleTimestamp`] so metrics and
    /// logging stay uniform with other rejection reasons.
    pub fn insert_or_refresh(
        &mut self,
        announce: NodeAnnounce,
        now_us: u64,
    ) -> Result<InsertOutcome, StaleTimestamp> {
        let node_id = announce.body.node_id;
        let ts = announce.body.timestamp_us;
        if let Some(existing) = self.entries.get_mut(&node_id) {
            if ts <= existing.announce.body.timestamp_us {
                return Err(StaleTimestamp {
                    existing_us: existing.announce.body.timestamp_us,
                });
            }
            existing.announce = announce;
            existing.last_seen_us = now_us;
            Ok(InsertOutcome::Refreshed)
        } else {
            self.entries.insert(
                node_id,
                PeerEntry {
                    announce,
                    first_seen_us: now_us,
                    last_seen_us: now_us,
                },
            );
            Ok(InsertOutcome::Inserted)
        }
    }

    /// Evict entries whose `last_seen_us` is older than `now_us - ttl_us`.
    /// No-op if `ttl_us == 0`. Returns the number of entries removed.
    pub fn evict_expired(&mut self, now_us: u64) -> usize {
        if self.ttl_us == 0 {
            return 0;
        }
        let cutoff = now_us.saturating_sub(self.ttl_us);
        let before = self.entries.len();
        self.entries.retain(|_, e| e.last_seen_us >= cutoff);
        before - self.entries.len()
    }

    /// Iterate over all current entries. Order is unspecified.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8; 32], &PeerEntry)> {
        self.entries.iter()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use decdn_protocol::{LoadHint, NodeAnnounceBody};

    fn mk_announce(node_id: [u8; 32], ts_us: u64) -> NodeAnnounce {
        NodeAnnounce {
            body: NodeAnnounceBody {
                node_id,
                region: "US".to_string(),
                load: LoadHint {
                    active_streams: 0,
                    bandwidth_utilization: 0,
                },
                popular_hashes: vec![],
                timestamp_us: ts_us,
            },
            signature: vec![0u8; 64],
        }
    }

    #[test]
    fn insert_then_refresh() -> Result<(), StaleTimestamp> {
        let mut t = PeerTable::new(0);
        let id = [1u8; 32];
        assert_eq!(
            t.insert_or_refresh(mk_announce(id, 10), 100)?,
            InsertOutcome::Inserted
        );
        assert_eq!(
            t.insert_or_refresh(mk_announce(id, 11), 200)?,
            InsertOutcome::Refreshed
        );
        let entry = t.get(&id).ok_or(StaleTimestamp { existing_us: 0 })?;
        assert_eq!(entry.announce.body.timestamp_us, 11);
        assert_eq!(entry.first_seen_us, 100);
        assert_eq!(entry.last_seen_us, 200);
        Ok(())
    }

    #[test]
    fn monotonic_rejects_regression() {
        let mut t = PeerTable::new(0);
        let id = [2u8; 32];
        assert_eq!(
            t.insert_or_refresh(mk_announce(id, 10), 100).unwrap(),
            InsertOutcome::Inserted
        );
        let err = t.insert_or_refresh(mk_announce(id, 9), 200);
        assert_eq!(err, Err(StaleTimestamp { existing_us: 10 }));
        let err_eq = t.insert_or_refresh(mk_announce(id, 10), 200);
        assert_eq!(err_eq, Err(StaleTimestamp { existing_us: 10 }));
    }

    #[test]
    fn ttl_evicts_stale_entries() {
        let mut t = PeerTable::new(1_000); // 1000 µs TTL
        let a = [3u8; 32];
        let b = [4u8; 32];
        assert_eq!(
            t.insert_or_refresh(mk_announce(a, 1), 100).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(
            t.insert_or_refresh(mk_announce(b, 1), 500).unwrap(),
            InsertOutcome::Inserted
        );
        // now_us = 1200 → cutoff = 200, so `a` (last_seen=100) gets evicted, `b` stays.
        let evicted = t.evict_expired(1_200);
        assert_eq!(evicted, 1);
        assert!(t.get(&a).is_none());
        assert!(t.get(&b).is_some());
    }

    #[test]
    fn ttl_zero_is_noop() {
        let mut t = PeerTable::new(0);
        let id = [5u8; 32];
        assert_eq!(
            t.insert_or_refresh(mk_announce(id, 1), 10).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(t.evict_expired(u64::MAX), 0);
        assert!(t.get(&id).is_some());
    }

    #[test]
    fn evict_at_exact_cutoff_keeps_entry() {
        // ttl=100, last_seen=100, now=200 → cutoff=100, 100 >= 100 so entry stays.
        let mut t = PeerTable::new(100);
        let id = [6u8; 32];
        assert_eq!(
            t.insert_or_refresh(mk_announce(id, 1), 100).unwrap(),
            InsertOutcome::Inserted
        );
        let evicted = t.evict_expired(200);
        assert_eq!(evicted, 0);
        assert!(t.get(&id).is_some());
    }

    #[test]
    fn evict_one_microsecond_past_cutoff_removes_entry() {
        // ttl=100, last_seen=100, now=201 → cutoff=101, 100 < 101 so entry is evicted.
        let mut t = PeerTable::new(100);
        let id = [7u8; 32];
        assert_eq!(
            t.insert_or_refresh(mk_announce(id, 1), 100).unwrap(),
            InsertOutcome::Inserted
        );
        let evicted = t.evict_expired(201);
        assert_eq!(evicted, 1);
        assert!(t.get(&id).is_none());
    }

    #[test]
    fn saturating_sub_underflow_keeps_all_entries() {
        // ttl=1000, last_seen=10, now=50 → saturating_sub clamps cutoff to 0,
        // so 10 >= 0 and the entry stays. Guards the saturating_sub path.
        let mut t = PeerTable::new(1_000);
        let id = [8u8; 32];
        assert_eq!(
            t.insert_or_refresh(mk_announce(id, 1), 10).unwrap(),
            InsertOutcome::Inserted
        );
        let evicted = t.evict_expired(50);
        assert_eq!(evicted, 0);
        assert!(t.get(&id).is_some());
    }

    #[test]
    fn refresh_at_boundary_keeps_entry() {
        // Insert at last_seen=100, refresh to last_seen=200. Then evict at now=200
        // with ttl=100 → cutoff=100, refreshed last_seen=200 >= 100 so entry stays.
        let mut t = PeerTable::new(100);
        let id = [9u8; 32];
        assert_eq!(
            t.insert_or_refresh(mk_announce(id, 1), 100).unwrap(),
            InsertOutcome::Inserted
        );
        // Bump announce timestamp so the refresh is accepted (monotonicity).
        assert_eq!(
            t.insert_or_refresh(mk_announce(id, 2), 200).unwrap(),
            InsertOutcome::Refreshed
        );
        let evicted = t.evict_expired(200);
        assert_eq!(evicted, 0);
        let entry = t.get(&id).expect("entry should still be present");
        assert_eq!(entry.last_seen_us, 200);
    }

    #[test]
    fn refresh_resets_eviction_clock() {
        // Without a refresh, evict_expired(300) with ttl=100 would cut off at 200
        // and remove an entry whose last_seen=100. Refreshing to last_seen=250
        // should keep it alive (250 >= 200).
        let mut t = PeerTable::new(100);
        let id = [10u8; 32];
        assert_eq!(
            t.insert_or_refresh(mk_announce(id, 1), 100).unwrap(),
            InsertOutcome::Inserted
        );
        // Bump announce timestamp so the refresh is accepted.
        assert_eq!(
            t.insert_or_refresh(mk_announce(id, 2), 250).unwrap(),
            InsertOutcome::Refreshed
        );
        let evicted = t.evict_expired(300);
        assert_eq!(evicted, 0);
        let entry = t.get(&id).expect("entry should still be present");
        assert_eq!(entry.last_seen_us, 250);
    }
}
