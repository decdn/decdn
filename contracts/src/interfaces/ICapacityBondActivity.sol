// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ICapacityBondActivity
/// @notice Consumer-side view of `CapacityBond.isActive` (ADR 003
///         § NodeId-to-Ethereum Binding, ADR 016 § Off-Chain Read API).
///         `PaymentPool.openPool` reads it to refuse pools against an
///         operator that is not a currently-bonded, active node; `OriginAssignment`
///         reads the same predicate for origin authorization. Kept focused —
///         mirroring `ICapacityBondEpoch` / `ICapacityBondEjector` — so
///         consumers link only the read they need.
/// @dev    Returns `true` iff `operator` is registered with an active
///         (non-unbonding) bond at or above the tier minimum; `false` for
///         unregistered, sub-minimum, unbonding, or auto-ejected operators.
///         Operator-level blacklist status (ADR 011) is intentionally NOT
///         consulted here — callers needing the combined "authorized origin"
///         predicate filter against `ContentBlacklist.isOriginBlacklisted`.
interface ICapacityBondActivity {
    function isActive(address operator) external view returns (bool);
}
