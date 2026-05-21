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
    /// New entry rejected because the table is at its hard cap (#577 H3).
    /// Returned only for previously-unseen node IDs after a one-shot
    /// inline TTL sweep failed to free a slot; existing entries are
    /// always refreshed regardless of the cap so legitimate peers
    /// don't lose their slot under a fresh-keypair flood. Subscriber-
    /// loop callers should bump a `peer_table_full` rejection metric.
    RejectedFull,
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

/// Minimum spacing between inline TTL sweeps fired from
/// [`PeerTable::insert_or_refresh`]'s cap-overflow path (#577 H3 review).
///
/// The cap path triggers `evict_expired`, which is an O(N) `HashMap::retain`
/// scan. Without throttling, a fresh-keypair flood at cap forces a full
/// 100k-entry scan per signature-valid announce — the write lock is held
/// the whole time, which would block legitimate refreshes and the 30 s
/// background sweeper. With a 1 s floor, the worst-case sustained CPU
/// cost of the inline sweep is bounded at ~one scan/second regardless of
/// attacker rate; rejected inserts in between are O(1).
///
/// The floor isn't an upper bound on slot reclamation: the 30 s background
/// sweeper (`service::ttl_sweeper_task`) still runs in parallel and
/// reclaims expired entries on its own cadence. The inline sweep
/// exists only to shorten the time between a slot expiring and the
/// next admitted insert when the table is under cap pressure.
pub const MIN_INLINE_SWEEP_INTERVAL_US: u64 = 1_000_000;

/// In-memory, TTL-bounded peer table.
#[derive(Debug)]
pub struct PeerTable {
    entries: HashMap<[u8; 32], PeerEntry>,
    ttl_us: u64,
    /// Hard cap on entry count (#577 H3). `0` disables the cap; the
    /// resolved-config validator rejects `0` so production paths never
    /// hit the unbounded branch, but in-process tests use it.
    max_entries: usize,
    /// Wall-clock microseconds of the last inline TTL sweep fired from
    /// [`Self::insert_or_refresh`]'s cap path. `0` is the never-swept
    /// sentinel that lets the first cap-overflow always trigger a
    /// sweep. See [`MIN_INLINE_SWEEP_INTERVAL_US`] for the rate-limit
    /// rationale.
    last_inline_sweep_us: u64,
}

impl PeerTable {
    /// Create an empty table.
    ///
    /// * `ttl_us` — how long an unrefreshed entry may live; `0` disables TTL.
    /// * `max_entries` — hard cap on entry count enforced by
    ///   [`Self::insert_or_refresh`]; `0` disables the cap (test-only,
    ///   the config resolver rejects `0` on the production path).
    pub fn new(ttl_us: u64, max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            ttl_us,
            max_entries,
            last_inline_sweep_us: 0,
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
            return Ok(InsertOutcome::Refreshed);
        }

        // New entry. Enforce the hard cap (#577 H3) with a rate-limited
        // inline TTL sweep: under a fresh-keypair flood the table is full
        // of fresh entries the 30 s background sweeper hasn't reached, so
        // the inline sweep gives expired slots a chance to be reclaimed
        // before the request is rejected. The sweep is throttled to at
        // most once per `MIN_INLINE_SWEEP_INTERVAL_US` so the attacker
        // can't force an O(N) `HashMap::retain` per signature-valid
        // announce — rejected inserts in between are O(1). `max_entries
        // == 0` disables the cap (test-only escape hatch).
        if self.max_entries > 0 && self.entries.len() >= self.max_entries {
            // `0` is the never-swept sentinel; otherwise gate on elapsed.
            // `saturating_sub` keeps this safe under a clock reversal.
            let due = self.last_inline_sweep_us == 0
                || now_us.saturating_sub(self.last_inline_sweep_us) >= MIN_INLINE_SWEEP_INTERVAL_US;
            if due {
                self.evict_expired(now_us);
                self.last_inline_sweep_us = now_us;
            }
            if self.entries.len() >= self.max_entries {
                return Ok(InsertOutcome::RejectedFull);
            }
        }

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
    use decdn_protocol::NodeAnnounceBody;

    fn mk_announce(node_id: [u8; 32], ts_us: u64) -> NodeAnnounce {
        NodeAnnounce {
            body: NodeAnnounceBody {
                node_id,
                region: "US".to_string(),
                timestamp_us: ts_us,
            },
            signature: vec![0u8; 64],
        }
    }

    #[test]
    fn insert_then_refresh() -> Result<(), StaleTimestamp> {
        let mut t = PeerTable::new(0, 0);
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
        let mut t = PeerTable::new(0, 0);
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
        let mut t = PeerTable::new(1_000, 0); // 1000 µs TTL, unbounded
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
        let mut t = PeerTable::new(0, 0);
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
        let mut t = PeerTable::new(100, 0);
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
        let mut t = PeerTable::new(100, 0);
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
        let mut t = PeerTable::new(1_000, 0);
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
        let mut t = PeerTable::new(100, 0);
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
        let mut t = PeerTable::new(100, 0);
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

    // #577 H3 — cap behavior.

    #[test]
    fn insert_rejected_when_table_full() {
        // ttl=0 so the inline sweep on the cap path is a no-op; this isolates
        // the "cap reached, no eviction possible" branch from the "sweep
        // freed a slot" branch (covered by the next test).
        let mut t = PeerTable::new(0, 2);
        let a = [11u8; 32];
        let b = [12u8; 32];
        let c = [13u8; 32];
        assert_eq!(
            t.insert_or_refresh(mk_announce(a, 1), 100).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(
            t.insert_or_refresh(mk_announce(b, 1), 100).unwrap(),
            InsertOutcome::Inserted
        );
        // Third distinct id must be rejected — the table is at cap and the
        // inline sweep finds nothing to evict (ttl=0).
        assert_eq!(
            t.insert_or_refresh(mk_announce(c, 1), 100).unwrap(),
            InsertOutcome::RejectedFull
        );
        assert_eq!(t.len(), 2);
        assert!(t.get(&a).is_some());
        assert!(t.get(&b).is_some());
        assert!(t.get(&c).is_none());
    }

    #[test]
    fn refresh_allowed_when_table_full() {
        // The cap is on *new* node IDs only — under a fresh-keypair flood,
        // legitimate peers must still be able to refresh their slot.
        let mut t = PeerTable::new(0, 1);
        let id = [14u8; 32];
        assert_eq!(
            t.insert_or_refresh(mk_announce(id, 1), 100).unwrap(),
            InsertOutcome::Inserted
        );
        // Refresh of the existing id at the cap succeeds.
        assert_eq!(
            t.insert_or_refresh(mk_announce(id, 2), 200).unwrap(),
            InsertOutcome::Refreshed
        );
        let entry = t.get(&id).expect("entry should still be present");
        assert_eq!(entry.last_seen_us, 200);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn inline_sweep_on_full_admits_new_after_expiry() {
        // ttl=100, cap=1. Insert at t=100; advance the clock past the TTL
        // before the next insert. The new insert finds the table at cap,
        // triggers the inline sweep, the original entry expires, and the
        // new one is admitted.
        let mut t = PeerTable::new(100, 1);
        let a = [15u8; 32];
        let b = [16u8; 32];
        assert_eq!(
            t.insert_or_refresh(mk_announce(a, 1), 100).unwrap(),
            InsertOutcome::Inserted
        );
        // now_us = 300, ttl = 100, so cutoff = 200 and a (last_seen=100) is
        // expired. The inline sweep on the cap-full path reclaims it.
        assert_eq!(
            t.insert_or_refresh(mk_announce(b, 1), 300).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(t.len(), 1);
        assert!(t.get(&a).is_none());
        assert!(t.get(&b).is_some());
    }

    #[test]
    #[allow(clippy::many_single_char_names)] // ids labelled to match the timeline comment
    fn inline_sweep_is_throttled_under_sustained_flood() {
        // Defends against the review finding (#656): without throttling, a
        // fresh-keypair flood at cap forces a full O(N) HashMap::retain
        // scan per signature-valid announce — moves the original
        // memory-DoS into a CPU + lock-contention DoS. The throttle is
        // observed indirectly: insert N=2 entries at the cap, then
        // expire them, then drive two rejected inserts in the same
        // throttle window — the second one must remain RejectedFull
        // because the throttle suppresses the re-sweep that would
        // otherwise reclaim the expired slots.
        //
        // Layout (ttl=100µs, cap=2, all times in µs):
        //   t=100:        insert A → Inserted
        //   t=100:        insert B → Inserted (table at cap)
        //   t=400:        insert C → cap-hit, sweep fires (sentinel 0),
        //                            A+B both expired, C admitted.
        //                            last_inline_sweep_us is now 400.
        //   t=500:        insert D → cap-hit, but 500-400=100 µs <
        //                            MIN_INLINE_SWEEP_INTERVAL_US (1 s),
        //                            so the sweep is THROTTLED. C is the
        //                            only entry and its last_seen=400 is
        //                            fresh, so even an unthrottled sweep
        //                            would leave the table at cap.
        //                            Result: RejectedFull, as expected.
        //   t=400 + 1s:   insert E → throttle elapsed, sweep fires; C
        //                            (last_seen=400, ttl=100) is expired
        //                            at t=1_000_400 (cutoff=1_000_300),
        //                            reclaimed; E admitted.
        let mut t = PeerTable::new(100, 2);
        let a = [20u8; 32];
        let b = [21u8; 32];
        let c = [22u8; 32];
        let d = [23u8; 32];
        let e = [24u8; 32];

        assert_eq!(
            t.insert_or_refresh(mk_announce(a, 1), 100).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(
            t.insert_or_refresh(mk_announce(b, 1), 100).unwrap(),
            InsertOutcome::Inserted
        );
        // First cap-overflow at t=400: sweep fires (sentinel 0), reclaims
        // both expired entries, C is admitted.
        assert_eq!(
            t.insert_or_refresh(mk_announce(c, 1), 400).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(t.len(), 1);

        // Fill the second slot so the table is at cap again, then prove
        // the throttle binds: at t=500 (< 1 s after the previous sweep),
        // a fresh-id insert at cap is rejected without a re-sweep — even
        // though we just expired nothing, the assertion is that the
        // throttle path runs and returns RejectedFull immediately.
        // We need the table at cap to exercise the cap branch, so insert
        // one more first.
        let filler = [25u8; 32];
        assert_eq!(
            t.insert_or_refresh(mk_announce(filler, 1), 400).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(t.len(), 2);
        // t=500: cap-overflow, throttle binds (500-400=100µs < 1s).
        assert_eq!(
            t.insert_or_refresh(mk_announce(d, 1), 500).unwrap(),
            InsertOutcome::RejectedFull
        );

        // Pass the throttle window: at t=1_000_400, the elapsed since
        // the last sweep is exactly MIN_INLINE_SWEEP_INTERVAL_US.
        // Now C is expired (last_seen=400, ttl=100, cutoff=1_000_300)
        // and the sweep reclaims it. E is admitted.
        let post_throttle = 400 + MIN_INLINE_SWEEP_INTERVAL_US;
        assert_eq!(
            t.insert_or_refresh(mk_announce(e, 1), post_throttle)
                .unwrap(),
            InsertOutcome::Inserted
        );
    }

    #[test]
    fn max_entries_zero_is_unbounded() {
        // The `0 = unbounded` escape hatch is used by every existing
        // single-arg call site (tests + admin fixtures). A regression that
        // treated `0` as the cap would reject every insert.
        let mut t = PeerTable::new(0, 0);
        for i in 0u8..50 {
            let id = [i; 32];
            assert_eq!(
                t.insert_or_refresh(mk_announce(id, 1), 100).unwrap(),
                InsertOutcome::Inserted
            );
        }
        assert_eq!(t.len(), 50);
    }
}
