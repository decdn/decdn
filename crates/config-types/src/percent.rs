//! A percentage config value with fixed-point scale (#894).
//!
//! A transparent newtype around `u64` for the seed-leech share ratio
//! (`100` == 1.0×). Distinct from [`Bytes`](crate::Bytes) so the percent
//! cannot be transposed with a byte quantity threaded alongside it — the
//! bytes↔percent confusion seam #894 closes.
//!
//! This type *owns* the fixed-point denominator ([`SHARE_RATIO_SCALE`]) and the
//! one operation that consumes it ([`Percent::scale_saturating`]), so the
//! `/ 100` can never be applied to the wrong operand or with the wrong scale at
//! a call site.
//!
//! `#[serde(transparent)]` keeps the TOML/JSON wire form an unchanged bare
//! integer.

use serde::{Deserialize, Serialize};

/// Fixed-point denominator for [`Percent`] (so `100` == 1.0×).
const SHARE_RATIO_SCALE: u64 = 100;

/// A percentage with a fixed-point scale of 100 (`100` == 1.0×). Wraps `u64`;
/// the wire form is a bare integer.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Percent(u64);

impl Percent {
    /// Wrap a raw percentage (`100` == 1.0×).
    #[must_use]
    pub const fn new(percent: u64) -> Self {
        Self(percent)
    }

    /// The raw percentage value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Apply this percentage to `bytes`, saturating on overflow:
    /// `bytes * percent / 100`. Owns the `SHARE_RATIO_SCALE` division so the
    /// scale is never mismatched at a call site.
    ///
    /// The product is computed in `u128` so a `bytes * percent` that overflows
    /// `u64` is divided *before* it is clamped — clamping the raw product first
    /// (e.g. `u64::saturating_mul`) would divide `u64::MAX` by 100 and return a
    /// value ~100× too small rather than a true saturated `u64::MAX`.
    #[must_use]
    pub fn scale_saturating(self, bytes: u64) -> u64 {
        let scaled = u128::from(bytes) * u128::from(self.0) / u128::from(SHARE_RATIO_SCALE);
        u64::try_from(scaled).unwrap_or(u64::MAX)
    }
}

impl std::fmt::Display for Percent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;

    #[test]
    fn new_and_get_round_trip() {
        assert_eq!(Percent::new(400).get(), 400);
        assert_eq!(Percent::default().get(), 0);
    }

    #[test]
    fn serde_is_a_bare_integer() {
        let json = serde_json::to_string(&Percent::new(400)).expect("serialize");
        assert_eq!(json, "400");
        let back: Percent = serde_json::from_str("400").expect("deserialize");
        assert_eq!(back, Percent::new(400));
    }

    #[test]
    fn scale_saturating_applies_the_ratio() {
        // 1.0× returns the input.
        assert_eq!(Percent::new(100).scale_saturating(1_000), 1_000);
        // 4.0× quadruples.
        assert_eq!(Percent::new(400).scale_saturating(1_000), 4_000);
        // Fractional (50%) halves, truncating.
        assert_eq!(Percent::new(50).scale_saturating(101), 50);
        // 0% pins to nothing.
        assert_eq!(Percent::new(0).scale_saturating(1_000), 0);
    }

    #[test]
    fn scale_saturating_saturates_instead_of_overflowing() {
        // The product is computed in u128, so an overflowing `bytes * percent`
        // is divided before it is clamped and the result saturates at u64::MAX —
        // not u64::MAX/100, which clamping the raw product first would yield.
        assert_eq!(Percent::new(u64::MAX).scale_saturating(u64::MAX), u64::MAX);
        // Regression for the mid-range overflow: 2.0× of just over half of
        // u64::MAX exceeds u64::MAX, so it must saturate. A u64 saturating_mul
        // before the divide returned ~u64::MAX/100 here.
        assert_eq!(
            Percent::new(200).scale_saturating(u64::MAX / 2 + 1),
            u64::MAX
        );
    }
}
