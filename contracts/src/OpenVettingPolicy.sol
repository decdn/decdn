// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IVettingPolicy } from "./interfaces/IVettingPolicy.sol";

/// @title OpenVettingPolicy — no-gate vetting (ADR 011)
/// @notice Vets every publisher. For local and dev testnet runs where approving
///         publishers is only friction.
///
/// @dev    TESTNET ONLY. This policy must never be installed on a production
///         deployment — it disables the publisher-vetting gate entirely.
///         Deploy tooling and documentation do not offer it for mainnet.
///         Stateless and immutable: no roles, no owner, nothing to govern. To
///         re-gate, governance swaps in another policy via
///         `OriginAssignment.setVettingPolicy`. "Deny everyone" needs no
///         dedicated contract — it is any policy whose `isVetted` returns false.
contract OpenVettingPolicy is IVettingPolicy {
    /// @inheritdoc IVettingPolicy
    function isVetted(address) external pure returns (bool) {
        return true;
    }
}
