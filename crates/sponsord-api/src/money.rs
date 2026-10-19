//! USDC amounts.

use serde::{Deserialize, Serialize};

/// An amount of USDC in its base unit (6 decimals): `1_000_000` is $1. On the
/// wire it is a bare integer.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(transparent)]
pub struct MicroUsdc(pub u64);

impl MicroUsdc {
    /// No USDC.
    pub const ZERO: MicroUsdc = MicroUsdc(0);

    /// `self + other`, capped at `u64::MAX`.
    #[must_use]
    pub const fn saturating_add(self, other: MicroUsdc) -> MicroUsdc {
        MicroUsdc(self.0.saturating_add(other.0))
    }

    /// `self - other`, floored at zero.
    #[must_use]
    pub const fn saturating_sub(self, other: MicroUsdc) -> MicroUsdc {
        MicroUsdc(self.0.saturating_sub(other.0))
    }
}

impl std::fmt::Display for MicroUsdc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for MicroUsdc {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(MicroUsdc)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
