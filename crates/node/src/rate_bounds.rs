//! Live per-MB delivery-rate floor (#1172, ADR 019 §3.1 / ADR 003).
//!
//! The seller raises its advertised `rate_per_mb` to a governance-set floor
//! before signing a `ProbeResponse` / `StreamResponse` and enforces the same
//! floor again at voucher settlement. The floor lives on-chain in
//! `PaymentChannel.getRateBounds()`; the node reads it once at startup and then
//! tracks `RateBoundsUpdated` events so a governance retune reaches a running
//! node without a restart.
//!
//! There is no governance ceiling. A seller self-clamping its own advertised
//! rate *downward* buys no on-chain safety — a seller never wants to charge
//! less — and buyer protection is the buyer seeing the signed rate in
//! `StreamResponse` before it pays. The absolute upper bound is the wire
//! constant [`decdn_protocol::MAX_RATE_PER_MB`], enforced at message
//! validation, not a tunable band.
//!
//! [`RateBounds`] is the shared handle both the probe/client handlers and the
//! rate-bounds watcher hold. A single value needs no cross-field consistency
//! machinery: one `AtomicU64` is read and written whole.
//!
//! Do **not** pin a quote-time floor across a stream and reuse it to accept
//! that stream's later vouchers. The hard per-byte floor enforced at voucher
//! acceptance (`handlers/client/voucher.rs`) mirrors the on-chain
//! `PaymentChannel._advanceClaimWatermark` check, which reads the **live**
//! `deliveryFloor` at settlement — there is no per-channel floor snapshot on
//! chain. A voucher priced below the live floor is unredeemable
//! (`RateFloorViolation`), so the acceptance check must read the live floor too;
//! pinning the quote-time floor would make the node countersign vouchers it
//! cannot redeem (see #1382 / #1388). Honouring the buyer across a governance
//! floor raise is a separate concern — it needs a graceful re-quote signal, not
//! a stale floor.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Cloneable live delivery-rate floor. All clones share one atomic cell.
#[derive(Clone, Debug)]
pub struct RateBounds {
    floor: Arc<AtomicU64>,
}

impl RateBounds {
    /// Seed the floor, typically from the startup `getRateBounds()` read.
    #[must_use]
    pub fn new(floor: u64) -> Self {
        Self {
            floor: Arc::new(AtomicU64::new(floor)),
        }
    }

    /// Current floor (minimum per-MB rate a voucher may pay).
    #[must_use]
    pub fn floor(&self) -> u64 {
        self.floor.load(Ordering::Relaxed)
    }

    /// Raise `rate` to the current floor. Total by construction — there is no
    /// upper bound to invert against, so no panic path (`u64::clamp` is
    /// `assert!(min <= max)`, which the workspace anti-panic policy forbids on
    /// the request-serving path).
    #[must_use]
    pub fn clamp(&self, rate: u64) -> u64 {
        rate.max(self.floor())
    }

    /// Publish a new floor — called by the rate-bounds watcher on a
    /// `RateBoundsUpdated` event and by the periodic authoritative re-read.
    pub fn store(&self, floor: u64) {
        self.floor.store(floor, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_raises_rate_to_the_floor() {
        let b = RateBounds::new(10);
        assert_eq!(b.clamp(5), 10, "below floor clamps up");
        assert_eq!(b.clamp(50), 50, "at or above floor unchanged");
        assert_eq!(b.clamp(u64::MAX), u64::MAX, "no upper bound to clamp to");
    }

    #[test]
    fn store_updates_are_visible_through_all_clones() {
        let b = RateBounds::new(0);
        let clone = b.clone();
        b.store(25);
        // A clone shares the same cell, so the update through `b` is visible.
        assert_eq!(clone.floor(), 25);
        assert_eq!(clone.clamp(10), 25);
        assert_eq!(clone.clamp(90), 90);
    }
}
