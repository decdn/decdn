// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title BondMath
/// @notice Pure arithmetic for tier-based capacity-bond bond reduction,
///         extracted from `CapacityBond` so the defensive over-100%-tier clip
///         and the active-then-unbonding ordering can be unit-tested directly
///         (without deploying a near-EIP-170-ceiling test harness of the full
///         `CapacityBond`). `CapacityBond._reduceBondAtTier` reads the
///         operator's balances, calls `reduceAtTier`, and writes the results
///         back — behaviorally equivalent to the prior inlined form.
library BondMath {
    /// @dev Basis-point denominator (mirrors `CapacityBond.BPS_DENOMINATOR`).
    uint256 internal constant BPS_DENOMINATOR = 10_000;

    /// @dev Reduce active + unbonding bond at `tierBps`. Active first, then
    ///      unbonding (prevents slash-then-run per ADR 003). The defensive
    ///      `min(remainder, unbonding)` cap (C2 fix) guards against a future
    ///      tier above 100% of at-risk bond (`tierBps > BPS_DENOMINATOR`)
    ///      silently underflowing the unbonding subtraction; the current ladder
    ///      maxes at 50% so the clip is dead defensive code today. When the cap
    ///      clips, `slashAmount` is reduced to the amount actually removed so
    ///      the caller's distribution math does not over-transfer / over-burn.
    /// @dev The `active` / `unbonding` argument order is load-bearing (active is
    ///      consumed first); the two are interchangeable at the type level, so
    ///      transposing them at a call site would silently invert ADR 003's
    ///      ordering. Single trusted caller today (`_reduceBondAtTier`).
    /// @return slashAmount  the amount actually removed across both balances
    /// @return newActive    the operator's active bond after reduction
    /// @return newUnbonding the operator's unbonding amount after reduction
    function reduceAtTier(uint256 active, uint256 unbonding, uint256 tierBps)
        internal
        pure
        returns (uint256 slashAmount, uint256 newActive, uint256 newUnbonding)
    {
        uint256 totalAtRisk = active + unbonding;
        slashAmount = (totalAtRisk * tierBps) / BPS_DENOMINATOR;
        if (slashAmount <= active) {
            newActive = active - slashAmount;
            newUnbonding = unbonding;
        } else {
            uint256 remainder = slashAmount - active;
            if (remainder > unbonding) {
                remainder = unbonding;
                slashAmount = active + remainder;
            }
            newActive = 0;
            newUnbonding = unbonding - remainder;
        }
    }
}
