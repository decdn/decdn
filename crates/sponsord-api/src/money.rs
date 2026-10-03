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
    pub const ZERO: MicroUsdc = MicroUsdc(0);

    #[must_use]
    pub fn saturating_add(self, other: MicroUsdc) -> MicroUsdc {
        MicroUsdc(self.0.saturating_add(other.0))
    }

    #[must_use]
    pub fn saturating_sub(self, other: MicroUsdc) -> MicroUsdc {
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
mod tests {
    use super::*;

    #[test]
    fn arithmetic_saturates() {
        assert_eq!(
            MicroUsdc(u64::MAX).saturating_add(MicroUsdc(1)),
            MicroUsdc(u64::MAX)
        );
        assert_eq!(MicroUsdc(1).saturating_sub(MicroUsdc(2)), MicroUsdc::ZERO);
    }

    #[test]
    fn is_a_bare_integer_on_the_wire() {
        assert_eq!(serde_json::to_string(&MicroUsdc(5)).unwrap(), "5");
        assert_eq!(
            serde_json::from_str::<MicroUsdc>("7").unwrap(),
            MicroUsdc(7)
        );
    }
}
