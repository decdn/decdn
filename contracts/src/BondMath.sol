// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { FixedPointMathLib } from "solady/utils/FixedPointMathLib.sol";

/// @title BondMath
/// @notice Pure arithmetic for the capacity bond: tier-based slash reduction
///         (`reduceAtTier`) and the ADR 026 § Capacity-bond curve evaluation
///         (`bondRequired`). Extracted from `CapacityBond` so the defensive
///         over-100%-tier clip and the active-then-unbonding ordering can be
///         unit-tested directly (without deploying a near-EIP-170-ceiling test
///         harness of the full `CapacityBond`). `CapacityBond._reduceBondAtTier`
///         reads the operator's balances, calls `reduceAtTier`, and writes the
///         results back — behaviorally equivalent to the prior inlined form.
/// @dev    `bondRequired` is `public` (not `internal`): it compiles into this
///         library's own deployed bytecode and is reached from `CapacityBond`
///         via a linked DELEGATECALL, keeping the fixed-point `pow` math out of
///         `CapacityBond`'s near-EIP-170-ceiling runtime size. `reduceAtTier`
///         stays `internal` (it is tiny and inlines into the caller).
library BondMath {
    /// @dev Basis-point denominator (mirrors `CapacityBond.BPS_DENOMINATOR`).
    uint256 internal constant BPS_DENOMINATOR = 10_000;

    /// @notice Capacity-bond curve `bond_required(Mbps) = k × Mbps^α`
    ///         (ADR 026 § Capacity-bond curve). Evaluated in 1e18 fixed point.
    /// @param mbps     Declared serving capacity in Mbps (plain integer, the
    ///                 unit of `CapacityBond.declaredMbps`). `mbps == 0` ⇒ 0.
    /// @param k        Bond constant in TOKEN-wei (e.g. `12.6e18`); the linear
    ///                 multiplier on the curve.
    /// @param alphaWad Curve exponent α in 1e18 fixed point (e.g. `1.2e18`),
    ///                 governance-bounded to [1.0, 1.8] by the caller.
    /// @return The required active bond in TOKEN-wei.
    /// @dev `mbps ≥ 1` ⇒ `mbps * 1e18 ≥ 1e18 > 0`, so `lnWad` (inside `powWad`)
    ///      always receives a positive argument. Over the governable domain
    ///      (`mbps ≤ 1_000_000`, `α ≤ 1.8`) the `powWad` exponent stays well
    ///      under `expWad`'s overflow bound, so the cast to `uint256` is safe.
    function bondRequired(uint256 mbps, uint256 k, uint256 alphaWad) public pure returns (uint256) {
        if (mbps == 0) return 0;
        // Mbps^α in 1e18 fixed point: powWad(x, y) = expWad(lnWad(x) · y).
        uint256 mbpsPow = uint256(FixedPointMathLib.powWad(int256(mbps * 1e18), int256(alphaWad)));
        // bond = k · Mbps^α; mulWad divides the 1e18 scale back out so the
        // TOKEN-wei magnitude of `k` is preserved.
        return FixedPointMathLib.mulWad(k, mbpsPow);
    }

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
