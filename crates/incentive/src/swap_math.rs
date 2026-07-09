//! Pure math for the USDC→TOKEN bond swap (#991). No I/O — all inputs are
//! on-chain reads passed in by the caller, so every branch is unit-testable.

use alloy::primitives::U256;

/// TOKEN to acquire via swap: the bond shortfall minus TOKEN already held.
/// Saturating, so an operator who already holds enough swaps nothing — this is
/// what makes a `setup` re-run after a failed bond converge instead of
/// re-swapping.
pub const fn swap_top_up(shortfall: U256, token_balance: U256) -> U256 {
    shortfall.saturating_sub(token_balance)
}

/// `expected_in × (1 + slippage_bps/10_000)`, rounded down. Bounds
/// `amountInMaximum` on the exact-out swap.
pub fn max_in_with_slippage(expected_in: U256, slippage_bps: u16) -> U256 {
    let num = U256::from(10_000u32) + U256::from(slippage_bps);
    expected_in * num / U256::from(10_000u32)
}

/// Execution price impact vs spot, in bps: `(expected_in − spot_in)/spot_in`.
/// Returns 0 when execution is at or better than spot, or when `spot_in` is 0.
pub fn price_impact_bps(spot_in: U256, expected_in: U256) -> u32 {
    if spot_in.is_zero() || expected_in <= spot_in {
        return 0;
    }
    let bps = (expected_in - spot_in) * U256::from(10_000u32) / spot_in;
    u32::try_from(bps).unwrap_or(u32::MAX)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use alloy::primitives::U256;

    #[test]
    fn top_up_is_shortfall_minus_holdings_saturating() {
        assert_eq!(
            swap_top_up(U256::from(100u64), U256::from(30u64)),
            U256::from(70u64)
        );
        // holds enough → nothing to swap (resumable re-run)
        assert_eq!(
            swap_top_up(U256::from(100u64), U256::from(100u64)),
            U256::ZERO
        );
        assert_eq!(
            swap_top_up(U256::from(100u64), U256::from(250u64)),
            U256::ZERO
        );
    }

    #[test]
    fn max_in_adds_slippage() {
        // 1_000_000 + 3% = 1_030_000
        assert_eq!(
            max_in_with_slippage(U256::from(1_000_000u64), 300),
            U256::from(1_030_000u64)
        );
        // zero slippage is identity
        assert_eq!(
            max_in_with_slippage(U256::from(1_000_000u64), 0),
            U256::from(1_000_000u64)
        );
    }

    #[test]
    fn price_impact_is_execution_over_spot() {
        // exec 103 vs spot 100 → 300 bps
        assert_eq!(
            price_impact_bps(U256::from(100u64), U256::from(103u64)),
            300
        );
        // exec at/below spot → 0
        assert_eq!(price_impact_bps(U256::from(100u64), U256::from(100u64)), 0);
        assert_eq!(price_impact_bps(U256::ZERO, U256::from(5u64)), 0); // guard div-by-zero
    }
}
