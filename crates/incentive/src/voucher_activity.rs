//! In-memory per-lane voucher-activity clock (issue #749).
//!
//! [`crate::LaneState`] persists *what* the latest accepted voucher was
//! (amount, bytes), but not *when* it was accepted: the redb schema carries
//! no last-voucher wall-clock, and adding one would be a schema migration
//! touching every `LaneState` call site. The operator-facing "time since last
//! voucher" signal `decdn node pools` reports (issue #749) does not need
//! durability — a stale-lane diagnostic is about *current* liveness, so an
//! in-process clock that resets on restart is the correct shape.
//!
//! [`VoucherActivity`] is that clock: a `Mutex<HashMap<LaneKey, Instant>>` the
//! voucher-accept path stamps on each acceptance ([`VoucherActivity::touch`])
//! and the admin snapshot reads ([`VoucherActivity::seconds_since`]). It is
//! deliberately *not* wired into [`crate::LaneState::apply_voucher`] (which is
//! a sync, store-coupled leaf method) — the `decdn-node` client handler
//! stamps it after a successful apply, the same place it fires the redeem
//! hint.
//!
//! A lane hydrated from `pools.redb` at boot has no entry until its next
//! voucher, so [`VoucherActivity::seconds_since`] returns `None` — surfaced to
//! the operator as "never (since restart)" rather than a misleading "0s ago".

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use crate::lane::LaneKey;

/// In-memory clock recording the last time this process accepted a voucher
/// on each lane. Thread-safe and cheap to clone the `Arc` around: the client
/// handler holds one reference (writer) and the admin surface holds another
/// (reader). See the module docs for why this is intentionally non-durable.
#[derive(Debug, Default)]
pub struct VoucherActivity {
    /// `Instant` of the most recent `touch` per lane. A poisoned mutex (a
    /// panic while holding it) degrades reads to "no record" and drops
    /// writes rather than propagating the panic — the workspace anti-panic
    /// policy forbids `unwrap`/`expect`, and a lost activity stamp only
    /// makes a lane look staler than it is (a safe direction for a
    /// diagnostic).
    last_voucher_at: Mutex<HashMap<LaneKey, Instant>>,
}

impl VoucherActivity {
    /// Construct an empty activity clock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a voucher was just accepted on `lane`, stamping the
    /// current [`Instant`]. Called from the voucher-accept success path
    /// after `apply_voucher` returns `Ok`. A poisoned lock silently drops
    /// the stamp (the lane then looks staler than it is — a safe direction
    /// for a diagnostic).
    pub fn touch(&self, lane: LaneKey) {
        if let Ok(mut map) = self.last_voucher_at.lock() {
            map.insert(lane, Instant::now());
        }
    }

    /// Drop the activity entry for `lane`, if any. Called when a lane leaves
    /// the live set (its pool settled/closed on-chain) so the map can't grow
    /// without bound on a long-running, high-churn node — the per-`LaneKey`
    /// `Instant` would otherwise linger for the whole process lifetime.
    /// Idempotent: forgetting an unknown (or already forgotten) lane is a
    /// no-op. A poisoned lock silently skips the removal, consistent with
    /// [`touch`](Self::touch)/[`seconds_since`](Self::seconds_since) — the
    /// entry then lingers, the same safe direction as a dropped stamp.
    pub fn forget(&self, lane: LaneKey) {
        if let Ok(mut map) = self.last_voucher_at.lock() {
            map.remove(&lane);
        }
    }

    /// Whole seconds since the last voucher was accepted on `lane`, or
    /// `None` when this process has accepted none since startup (no entry —
    /// e.g. a lane hydrated from disk that hasn't been touched, or a
    /// poisoned lock). `Instant`-based, so wall-clock skew can't make the
    /// age go backwards.
    #[must_use]
    pub fn seconds_since(&self, lane: LaneKey) -> Option<u64> {
        let map = self.last_voucher_at.lock().ok()?;
        map.get(&lane).map(|at| at.elapsed().as_secs())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use alloy::primitives::{address, b256};

    const LANE_A: LaneKey = LaneKey {
        pool_id: b256!("1111111111111111111111111111111111111111111111111111111111111111"),
        signer: address!("00000000000000000000000000000000000000a1"),
        provider: address!("00000000000000000000000000000000000000b1"),
    };
    const LANE_B: LaneKey = LaneKey {
        pool_id: b256!("2222222222222222222222222222222222222222222222222222222222222222"),
        signer: address!("00000000000000000000000000000000000000a2"),
        provider: address!("00000000000000000000000000000000000000b2"),
    };

    #[test]
    fn untouched_lane_reports_none() {
        let activity = VoucherActivity::new();
        assert_eq!(activity.seconds_since(LANE_A), None);
    }

    #[test]
    fn touched_lane_reports_some_small_age() {
        let activity = VoucherActivity::new();
        activity.touch(LANE_A);
        // Immediately after touch the elapsed whole-seconds is 0, but the
        // entry exists, so the value is `Some(_)` — distinguishing "just saw
        // a voucher" from "never saw one".
        let secs = activity.seconds_since(LANE_A);
        assert!(secs.is_some(), "touched lane must report Some, got None");
        assert!(
            secs.unwrap() < 5,
            "age should be near-zero right after touch, got {secs:?}"
        );
        // An unrelated lane is unaffected.
        assert_eq!(activity.seconds_since(LANE_B), None);
    }

    #[test]
    fn forget_removes_a_stamped_lane() {
        let activity = VoucherActivity::new();
        activity.touch(LANE_A);
        activity.touch(LANE_B);
        assert!(activity.seconds_since(LANE_A).is_some());
        // Forgetting LANE_A drops only its entry; LANE_B is untouched.
        activity.forget(LANE_A);
        assert_eq!(
            activity.seconds_since(LANE_A),
            None,
            "forgotten lane must report None (entry removed)"
        );
        assert!(
            activity.seconds_since(LANE_B).is_some(),
            "an unrelated lane must survive forget"
        );
    }

    #[test]
    fn forget_unknown_lane_is_a_noop() {
        let activity = VoucherActivity::new();
        // Forgetting a never-stamped lane does not panic and leaves the
        // (empty) map consistent.
        activity.forget(LANE_A);
        assert_eq!(activity.seconds_since(LANE_A), None);
    }

    #[test]
    fn touch_is_idempotent_per_lane_key() {
        let activity = VoucherActivity::new();
        activity.touch(LANE_A);
        activity.touch(LANE_A);
        // Two touches on the same lane keep a single entry (HashMap
        // overwrite) — still `Some`, no accumulation.
        assert!(activity.seconds_since(LANE_A).is_some());
    }
}
