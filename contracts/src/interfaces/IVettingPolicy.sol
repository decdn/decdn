// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IVettingPolicy
/// @notice The publisher-vetting seam `OriginAssignment` depends on (ADR 011
///         § Origin Assignment Authority). A policy answers one question: may
///         `publisher` currently seat origins? How a publisher becomes vetted —
///         manual approval, a timelocked governance grant, an on-chain
///         attestation, or no gate at all — is entirely internal to each policy
///         implementation. `OriginAssignment` holds one governance-settable
///         policy address and never depends on more than this view.
interface IVettingPolicy {
    /// @return True iff `publisher` may currently seat origins.
    function isVetted(address publisher) external view returns (bool);
}
