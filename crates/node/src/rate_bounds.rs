//! Live per-MB delivery-rate bounds (#1172, ADR 019 §3.1 / ADR 003).
//!
//! The seller clamps its advertised `rate_per_mb` to a governance-set
//! `[floor, ceiling]` before signing a `ProbeResponse` / `StreamResponse` and
//! enforces the floor again at voucher settlement. Those bounds live on-chain
//! in `PaymentChannel.getRateBounds()`; the node reads them once at startup and
//! then tracks `RateBoundsUpdated` events so a governance retune reaches a
//! running node without a restart.
//!
//! [`RateBounds`] is the shared handle both the probe/client handlers and the
//! rate-bounds watcher hold. The pair is published **atomically** through an
//! [`ArcSwap`], which is load-bearing rather than a convenience: an earlier
//! two-independent-`AtomicU64` design let a reader observe a new floor beside
//! the old ceiling, and `u64::clamp` is `assert!(min <= max)` — so a routine
//! governance retune that raised the band (e.g. `[10, 100] → [200, 500]`) could
//! panic the probe/client signing task. Publishing `(floor, ceiling)` as one
//! value makes that state unrepresentable.
//!
//! Two further guards, defence in depth:
//! - [`RateBounds::clamp`] uses `max`-then-`min` rather than `u64::clamp`, so
//!   even a degenerate `floor > ceiling` pair yields a value instead of a panic.
//! - [`Bounds::new`] normalises `ceiling` up to `floor`, so a degenerate pair is
//!   never stored in the first place.
//!
//! Readers that need a *consistent* pair across several decisions (quote now,
//! verify the matching voucher later) should take one [`RateBounds::snapshot`]
//! and thread the returned [`Bounds`] through, rather than re-reading — a
//! governance move between a signed quote and its voucher would otherwise make
//! the node reject a voucher paying the rate it itself quoted.

use std::sync::Arc;

use arc_swap::ArcSwap;

/// An immutable, internally-consistent `[floor, ceiling]` pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    /// Minimum per-MB rate a voucher may pay.
    pub floor: u64,
    /// Maximum per-MB rate. Always `>= floor` (see [`Bounds::new`]).
    pub ceiling: u64,
}

impl Bounds {
    /// Build a pair, normalising `ceiling` up to `floor` so the band is never
    /// inverted. The on-chain source already guarantees `ceiling > floor`
    /// (`PaymentChannel.setRateBounds`), so this only fires on a malformed
    /// input — and when it does, widening the ceiling is the conservative
    /// choice: it can never clamp a quote *below* the enforced floor, which is
    /// the value settlement checks against.
    #[must_use]
    pub const fn new(floor: u64, ceiling: u64) -> Self {
        Self {
            floor,
            ceiling: if ceiling < floor { floor } else { ceiling },
        }
    }

    /// Clamp `rate` into this band.
    ///
    /// Deliberately `max`-then-`min` rather than `u64::clamp`: the latter is
    /// `assert!(min <= max)` and would panic on a degenerate pair, which the
    /// workspace anti-panic policy forbids on the request-serving path.
    #[must_use]
    pub const fn clamp(&self, rate: u64) -> u64 {
        let raised = if rate < self.floor { self.floor } else { rate };
        if raised > self.ceiling {
            self.ceiling
        } else {
            raised
        }
    }
}

/// Cloneable live delivery-rate bounds. All clones share one atomically-published
/// [`Bounds`] value.
#[derive(Clone, Debug)]
pub struct RateBounds {
    inner: Arc<ArcSwap<Bounds>>,
}

impl RateBounds {
    /// Seed the bounds, typically from the startup `getRateBounds()` read.
    #[must_use]
    pub fn new(floor: u64, ceiling: u64) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(Bounds::new(floor, ceiling))),
        }
    }

    /// Take a consistent snapshot of the current pair. Prefer this over separate
    /// [`Self::floor`] / [`Self::ceiling`] reads whenever both are used for one
    /// decision, or when a later step must agree with an earlier one.
    #[must_use]
    pub fn snapshot(&self) -> Bounds {
        **self.inner.load()
    }

    /// Current floor (minimum per-MB rate a voucher may pay).
    #[must_use]
    pub fn floor(&self) -> u64 {
        self.snapshot().floor
    }

    /// Current ceiling (maximum per-MB rate).
    #[must_use]
    pub fn ceiling(&self) -> u64 {
        self.snapshot().ceiling
    }

    /// Clamp `rate` into the current band, reading the pair atomically.
    #[must_use]
    pub fn clamp(&self, rate: u64) -> u64 {
        self.snapshot().clamp(rate)
    }

    /// Publish a new `[floor, ceiling]` — called by the rate-bounds watcher on a
    /// `RateBoundsUpdated` event and by the periodic authoritative re-read. The
    /// pair is swapped as one value, so no reader can observe a half-applied
    /// update.
    pub fn store(&self, floor: u64, ceiling: u64) {
        self.inner.store(Arc::new(Bounds::new(floor, ceiling)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_holds_rate_within_bounds() {
        let b = RateBounds::new(10, 100);
        assert_eq!(b.clamp(5), 10, "below floor clamps up");
        assert_eq!(b.clamp(50), 50, "in range unchanged");
        assert_eq!(b.clamp(500), 100, "above ceiling clamps down");
    }

    #[test]
    fn store_updates_are_visible_through_all_clones() {
        let b = RateBounds::new(0, u64::MAX);
        let clone = b.clone();
        b.store(25, 75);
        // A clone shares the same cell, so the update through `b` is visible.
        assert_eq!(clone.floor(), 25);
        assert_eq!(clone.ceiling(), 75);
        assert_eq!(clone.clamp(10), 25);
        assert_eq!(clone.clamp(90), 75);
    }

    /// The regression this type's design exists to prevent: raising the band
    /// above the previous ceiling must never expose `floor > ceiling`, and must
    /// never panic. Under the old two-`AtomicU64` design a reader could observe
    /// `(200, 100)` and `u64::clamp` would `assert!` — on the signing path.
    #[test]
    fn raising_the_band_never_exposes_an_inverted_pair() {
        let b = RateBounds::new(10, 100);
        b.store(200, 500);
        let snap = b.snapshot();
        assert!(snap.floor <= snap.ceiling);
        assert_eq!(b.clamp(1), 200);
        assert_eq!(b.clamp(10_000), 500);
    }

    /// A degenerate pair must be normalised, not stored inverted, and must not
    /// panic if one somehow reaches `clamp`.
    #[test]
    fn inverted_input_is_normalised_and_never_panics() {
        let b = RateBounds::new(200, 100);
        let snap = b.snapshot();
        assert_eq!(snap.floor, 200);
        assert_eq!(snap.ceiling, 200, "ceiling normalised up to floor");
        assert_eq!(b.clamp(50), 200);

        // And the raw `Bounds::clamp` is total even if handed an inverted pair
        // constructed by field literal rather than `new`.
        let raw = Bounds {
            floor: 200,
            ceiling: 100,
        };
        assert_eq!(
            raw.clamp(50),
            100,
            "max-then-min yields the ceiling, no panic"
        );
    }

    #[test]
    fn snapshot_is_internally_consistent() {
        let b = RateBounds::new(10, 100);
        let snap = b.snapshot();
        b.store(1_000, 2_000);
        // The previously-taken snapshot is unaffected by the later store —
        // that is what makes quote-then-verify agree.
        assert_eq!(snap.floor, 10);
        assert_eq!(snap.ceiling, 100);
    }
}
