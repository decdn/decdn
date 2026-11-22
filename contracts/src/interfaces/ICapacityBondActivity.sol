// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ICapacityBondActivity
/// @notice Consumer-side view of `CapacityBond.isActive` (ADR 003
///         § NodeId-to-Ethereum Binding, ADR 016 § Off-Chain Read API).
///         `OriginAssignment` reads it for origin authorization. Kept
///         focused — mirroring `ICapacityBondEpoch` / `ICapacityBondEjector` —
///         so consumers link only the read they need.
/// @dev    Returns `true` iff `operator` is registered with an active bond
///         at or above its floor; `false` for unregistered, sub-floor, or
///         ejected operators. Bond that `unbond` released does not count.
///         Operator-level blacklist status (ADR 011) is intentionally NOT
///         consulted here — callers needing the combined "authorized origin"
///         predicate filter against `ContentBlacklist.isOriginBlacklisted`.
interface ICapacityBondActivity {
    function isActive(address operator) external view returns (bool);

    /// @notice The operator's bound NodeId (`bytes32(0)` when it holds no
    ///         binding) and its `isActive` status, in one read. No contract
    ///         calls it; off-chain readers use it.
    function nodeIdOf(address operator) external view returns (bytes32 nodeId, bool active);
}
