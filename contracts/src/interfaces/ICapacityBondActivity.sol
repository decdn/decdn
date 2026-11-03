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
/// @dev    Returns `true` iff `operator` is registered with an active bond
///         at or above its floor; `false` for unregistered, sub-floor, or
///         auto-ejected operators. Bond queued for unbonding does not count.
///         Operator-level blacklist status (ADR 011) is intentionally NOT
///         consulted here — callers needing the combined "authorized origin"
///         predicate filter against `ContentBlacklist.isOriginBlacklisted`.
interface ICapacityBondActivity {
    function isActive(address operator) external view returns (bool);

    /// @notice The operator's bound NodeId (`bytes32(0)` when it holds no
    ///         binding) and its `isActive` status, in one read.
    ///         `PaymentPool.redeemMany` uses it to pay only bound operators
    ///         and to count bytes only for active ones.
    function nodeIdOf(address operator) external view returns (bytes32 nodeId, bool active);
}
