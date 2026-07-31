// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ICapacityBondRegionView
/// @notice Aggregate read surface that `SlashJudge` and `ContentBlacklist` call
///         into to evaluate the ADR 030 § Region-stability window ripening
///         predicate (global ∪ current-region ∪ ripening-prev-region). Bundles
///         every input the predicate needs into a single view so the consumers
///         make one cross-contract call instead of five, and so they never have
///         to ABI-decode the full `NodeInfo` (with its `bytes multiaddrs`) just
///         to read `regionHint`.
/// @dev    Declared as a standalone interface (like `ICapacityBondEjector` /
///         `ICapacityBondSlasher`) so consumers can type their `capacityBond`
///         reference narrowly and slither's `missing-inheritance` detector can
///         verify `CapacityBond` provides the surface it implements.
interface ICapacityBondRegionView {
    /// @notice The ripening-predicate inputs for `operator` (ADR 030).
    /// @param operator The node operator (EOA) being scope-tested.
    /// @return regionHint            Current attested region (≤16 bytes; "" if none).
    /// @return regionPrev            Region in effect before the most recent
    ///                               `updateRegion` ("" if never changed).
    /// @return regionLastChanged     Timestamp the current region took effect —
    ///                               set at `registerNode`, restamped on each
    ///                               `updateRegion`. The ripening window runs from
    ///                               this stamp (callers use it as `effective`).
    /// @return regionStabilityWindow Current ripening window in seconds (= REGION_STABILITY_WINDOW).
    function regionScopeData(address operator)
        external
        view
        returns (
            string memory regionHint,
            string memory regionPrev,
            uint64 regionLastChanged,
            uint256 regionStabilityWindow
        );
}
