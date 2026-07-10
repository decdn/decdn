//! Pure math for the USDC→TOKEN bond swap (#991). No I/O — all inputs are
//! on-chain reads passed in by the caller, so every branch is unit-testable.

use alloy::primitives::{U256, U512};

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

/// Gross a fee-exclusive spot amount-in up by the pool's swap fee, so the
/// mandatory fee tier is not double-counted as price impact.
///
/// `spot_in` (from the pool's instantaneous mid-price) excludes the swap fee,
/// while the quoter's `expected_in` includes it — comparing them directly makes
/// the fee tier read as phantom impact (~30 bps on a 0.3% pool). Scaling
/// `spot_in` by `1/(1 − fee)` gives the fee-inclusive fair cost at mid-price, so
/// [`price_impact_bps`] against it measures depth impact alone.
///
/// `fee_pips` is the Uniswap V3 fee in millionths (e.g. `3000` = 0.3%). A `0`
/// fee is the identity; a `fee_pips ≥ 1_000_000` (≥100%, unreachable for a real
/// tier) can't be grossed up, so `spot_in` is returned unchanged. The multiply
/// runs in `U512` and saturates on narrowing — `spot_in` is advisory only, so a
/// saturated estimate never bounds real spend.
pub fn fee_inclusive_spot_in(spot_in: U256, fee_pips: u32) -> U256 {
    const SCALE: u32 = 1_000_000;
    if fee_pips == 0 || fee_pips >= SCALE {
        return spot_in;
    }
    let denom = SCALE - fee_pips;
    let grossed = U512::from(spot_in).saturating_mul(U512::from(SCALE)) / U512::from(denom);
    grossed.saturating_to::<U256>()
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

    #[test]
    fn fee_grossup_removes_fee_from_impact() {
        // A 0.3% pool with a fee-inclusive quote exactly at mid-price + fee:
        // fee-exclusive spot 1_000_000, expected_in = 1_000_000 / (1 − 0.003)
        // ≈ 1_003_009. Raw impact reads ~30 bps (the fee); grossing spot up by
        // the fee collapses it to ~0.
        let spot = U256::from(1_000_000u64);
        let expected = U256::from(1_003_009u64);
        assert_eq!(price_impact_bps(spot, expected), 30);
        let grossed = fee_inclusive_spot_in(spot, 3000);
        assert!(price_impact_bps(grossed, expected) <= 1);
        // zero fee and out-of-range fee are the identity
        assert_eq!(fee_inclusive_spot_in(spot, 0), spot);
        assert_eq!(fee_inclusive_spot_in(spot, 1_000_000), spot);
    }
}
