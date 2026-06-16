//! A byte-quantity config value (#894).
//!
//! A transparent newtype around `u64` so a byte quantity cannot be silently
//! transposed with a same-width value carrying a *different* unit — most
//! notably the seed-leech [`Percent`](crate::Percent) share ratio threaded
//! alongside it through the same structs. The two are distinct types, so a
//! bytes↔percent swap is a compile error rather than a saturating-arithmetic
//! mis-pricing that compiles and runs (the seam #894 closes).
//!
//! `#[serde(transparent)]` keeps the TOML/JSON wire form an unchanged bare
//! integer, so wrapping an existing `u64` config field is a non-breaking
//! representation change.

use serde::{Deserialize, Serialize};

/// A quantity of bytes. Wraps `u64`; the wire form is a bare integer.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Bytes(u64);

impl Bytes {
    /// Wrap a raw byte count.
    #[must_use]
    pub const fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    /// The raw byte count.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Add two byte quantities, saturating at `u64::MAX`. Typed on both sides so
    /// a byte count can only be added to another byte count, never to a value
    /// carrying a different unit.
    #[must_use]
    pub const fn saturating_add(self, rhs: Bytes) -> Bytes {
        Bytes(self.0.saturating_add(rhs.0))
    }

    /// Subtract `rhs` from this quantity, saturating at `0`.
    #[must_use]
    pub const fn saturating_sub(self, rhs: Bytes) -> Bytes {
        Bytes(self.0.saturating_sub(rhs.0))
    }
}

impl std::fmt::Display for Bytes {
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
        assert_eq!(Bytes::new(1_048_576).get(), 1_048_576);
        assert_eq!(Bytes::default().get(), 0);
    }

    #[test]
    fn serde_is_a_bare_integer() {
        // Transparent: serializes to a bare integer, not a wrapper object.
        let json = serde_json::to_string(&Bytes::new(2_097_152)).expect("serialize");
        assert_eq!(json, "2097152");
        let back: Bytes = serde_json::from_str("2097152").expect("deserialize");
        assert_eq!(back, Bytes::new(2_097_152));
    }

    #[test]
    fn display_delegates_to_inner() {
        assert_eq!(Bytes::new(256).to_string(), "256");
    }

    #[test]
    fn saturating_add_saturates_at_max() {
        assert_eq!(Bytes::new(2).saturating_add(Bytes::new(3)), Bytes::new(5));
        assert_eq!(
            Bytes::new(u64::MAX).saturating_add(Bytes::new(1)),
            Bytes::new(u64::MAX)
        );
    }

    #[test]
    fn saturating_sub_saturates_at_zero() {
        assert_eq!(Bytes::new(5).saturating_sub(Bytes::new(3)), Bytes::new(2));
        assert_eq!(Bytes::new(3).saturating_sub(Bytes::new(5)), Bytes::new(0));
    }
}
