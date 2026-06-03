// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @notice Node-registry record (ADR 019; NodeId binding per
///         ADR 003). Declared at file level — like `SlashRecord` in
///         `SlashEscrowLib` — so both `CapacityBond` and its read-surface
///         interface reference one definition rather than duplicating it.
struct NodeInfo {
    bytes32 nodeId;
    // `ethAddress` (20B) + `active` (1B) + `lastMultiaddrUpdate` (8B) =
    // 29 bytes — pack into one storage slot. Don't separate or widen
    // any of these three without re-checking the packing or every
    // `_writeNodeInfo` pays an extra SSTORE.
    address ethAddress;
    bool active;
    uint64 lastMultiaddrUpdate;
    bytes multiaddrs;
    string regionHint;
}

/// @title ICapacityBondRegionView
/// @notice ADR 030 region-eligibility read surface consumed by
///         `ContentBlacklist` for the blacklist-scope ripening predicate
///         (ADR 030 § Region-stability window) and the ADR 011 § Standing path-2 check.
///         Declared as a standalone interface — like `ICapacityBondEjector` —
///         so the consumer types its handle narrowly and slither's
///         `missing-inheritance` detector can verify `CapacityBond` provides
///         the surface it implements.
/// @dev    Exposes only getters `CapacityBond` already has (plus the new
///         `regionGateActivatedAt` immutable), so the reader computes the ADR
///         030 § Region-stability window `effective` value itself —
///         `effective = regionLastChanged != 0 ? regionLastChanged
///         : max(firstBondedAt, regionGateActivatedAt)` — adding zero bytecode
///         to the size-constrained `CapacityBond` (issue #770, EIP-170 ceiling).
interface ICapacityBondRegionView {
    /// @notice Full node-registry record for `ethAddress` (current
    ///         `regionHint` is the only field the predicate reads).
    function getNodeByAddress(address ethAddress) external view returns (NodeInfo memory);

    /// @notice Region in effect before the last `updateRegion` ("" if never changed).
    function regionPrev(address operator) external view returns (string memory);

    /// @notice Timestamp of the last `updateRegion`; 0 if the region has never changed.
    function regionLastChanged(address operator) external view returns (uint64);

    /// @notice First-bond timestamp (ADR 036); lower input to the `effective` fallback.
    function firstBondedAt(address operator) external view returns (uint64);

    /// @notice One-time contract-global region-gate activation timestamp (ADR 030).
    function regionGateActivatedAt() external view returns (uint64);

    /// @notice ADR 030 § Region-stability window (seconds). Governable [3d, 30d].
    function regionStabilityWindow() external view returns (uint256);
}
