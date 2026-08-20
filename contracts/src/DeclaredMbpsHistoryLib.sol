// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Checkpoints } from "@openzeppelin/contracts/utils/structs/Checkpoints.sol";
import { Time } from "@openzeppelin/contracts/utils/types/Time.sol";
import { SafeCast } from "@openzeppelin/contracts/utils/math/SafeCast.sol";

/// @title DeclaredMbpsHistoryLib
/// @notice Holds the per-operator `declaredMbps` checkpoint history for
///         `CapacityBond`, so the tier an operator held at the close of any
///         epoch can be read back (`atEpoch`) and fed into the ADR 036
///         served-bytes vote-weight cap.
/// @dev    Separate from `CapacityBond` to keep that contract under the
///         EIP-170 runtime-size ceiling. `record` and `atEpoch` are `public`,
///         so they compile into this library's own deployed bytecode and are
///         reached from `CapacityBond` via a linked DELEGATECALL — executing
///         in `CapacityBond`'s storage context, so the `storage` mapping
///         argument resolves to `CapacityBond`'s slots.
library DeclaredMbpsHistoryLib {
    using Checkpoints for Checkpoints.Trace208;
    using SafeCast for uint256;

    /// @notice Appends a checkpoint of `mbps` at the current block timestamp
    ///         to `operator`'s history in `history`. Called on every
    ///         `declareMbps` and on tier release in `deregisterNode` (with
    ///         `mbps == 0`).
    function record(mapping(address => Checkpoints.Trace208) storage history, address operator, uint256 mbps) public {
        // slither-disable-next-line unused-return
        history[operator].push(Time.timestamp(), mbps.toUint208());
    }

    /// @notice Returns the `declaredMbps` tier `operator` held at the close
    ///         of `epoch`, i.e. the newest checkpoint at or before that
    ///         instant. Returns 0 when `operator` has no checkpoint yet
    ///         (OZ `Checkpoints` default).
    function atEpoch(
        mapping(address => Checkpoints.Trace208) storage history,
        address operator,
        uint64 epoch,
        uint64 epochLength
    ) public view returns (uint256) {
        // End of `epoch` in seconds. `upperLookupRecent` returns the newest
        // checkpoint at or before this instant. `Time.timestamp()` is
        // uint48, so the boundary fits uint48 for any epoch the Governor
        // passes (bounded by real time).
        uint48 epochEnd = ((uint256(epoch) + 1) * epochLength - 1).toUint48();
        return history[operator].upperLookupRecent(epochEnd);
    }
}
