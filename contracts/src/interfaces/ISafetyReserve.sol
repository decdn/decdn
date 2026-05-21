// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ISafetyReserve — slash-redirect inflow surface
/// @notice Minimal interface used by `StakingRegistry.slash` to notify the
///         `SafetyReserve` contract of the 30% slashed-TOKEN redirect leg
///         (ADR 026 § Slashing and burn; ADR 016 line 373).
/// @dev    The full `SafetyReserve` interface (payouts, appeal extensions,
///         keeper-triggered swap) lands with its own contract — see
///         [ADR 028 § Contract surface](https://github.com/decdn/decdn/blob/main/adr/028-slashing-appeals.md)
///         and [ADR 033](https://github.com/decdn/decdn/blob/main/adr/033-safety-insurance-reserve.md).
///         This file declares only what `StakingRegistry` needs to know about.
///
///         Authorization: `recordSlashInflow` carries `SLASH_INFLOW_REPORTER_ROLE`
///         on the implementing contract, granted to `StakingRegistry` in the
///         post-deployment role grant (ADR 016 § Post-Deployment Initialization,
///         step 3).
interface ISafetyReserve {
    /// @notice Called by `StakingRegistry.slash` immediately after a TOKEN
    ///         `safeTransfer` of `amount` to this contract, so the reserve
    ///         can attribute the inflow to a specific operator's slash event
    ///         (used by appeal-pinning per ADR 028).
    /// @param operator The operator whose stake was slashed.
    /// @param amount   The amount of TOKEN transferred to this contract for
    ///                 this slash (30% of the total slash amount per
    ///                 ADR 026 § Slashing and burn).
    function recordSlashInflow(address operator, uint256 amount) external;
}
