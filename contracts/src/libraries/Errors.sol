// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title Errors
/// @notice Shared custom errors used across deCDN contracts.
/// @dev Contract-specific errors live in their own contracts. Only errors shared
///      across two or more contracts are declared here to avoid accidental
///      drift in the emitted ABI.
library Errors {
    /// @dev Thrown when a zero address is passed to a function that requires a
    /// non-zero address (constructor args, setters).
    error ZeroAddress();

    /// @dev Thrown when an amount parameter is zero and a positive amount is
    /// required.
    error ZeroAmount();

    /// @dev Thrown when a value falls outside an immutable safety bound set at
    /// deployment (ADR 009 governable-with-bounds model).
    error OutOfBounds();

    /// @dev Thrown when a signature fails EIP-712 / ERC-1271 verification via
    /// OpenZeppelin SignatureChecker.
    error InvalidSignature();

    /// @dev Thrown when a deadline has passed.
    error Expired();

    /// @dev Thrown when a state invariant is violated. Such a branch should
    /// be unreachable under correct execution; surfacing it loudly beats
    /// silently masking storage corruption or a future refactor bug.
    error InvariantViolated();

    /// @dev Thrown when a governance setter is called with the currently-
    /// stored value (would emit a misleading *Updated event with old == new).
    error NoOp();
}
