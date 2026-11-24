//! A byte-quantity config value (#894).
//!
//! A transparent newtype around `u64` so a byte quantity cannot be silently
//! transposed with a same-width value carrying a *different* unit threaded
//! alongside it through the same structs. Distinct types make such a swap a
//! compile error rather than a saturating-arithmetic mis-pricing that compiles
//! and runs (the seam #894 closes).
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
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
