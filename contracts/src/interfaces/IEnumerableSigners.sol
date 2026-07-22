// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IEnumerableSigners
/// @notice Minimal Safe-shaped signer enumeration, used by
///         `ContentBlacklist.registerRegionalBody` for the ADR 011 § Signer
///         non-overlap check between a candidate regional body and the
///         emergency multisig.
/// @dev    Deliberately NOT a conformance requirement. A body that does not
///         implement this is still registrable — ADR 011 puts the disjointness
///         obligation on the governance proposal off-chain in that case, and the
///         `RegionalBodyRegistered` event records which regime applied. Callers
///         must therefore treat a revert as "not enumerable", never as failure,
///         and must assume the callee is untrusted.
interface IEnumerableSigners {
    function getOwners() external view returns (address[] memory);
}
