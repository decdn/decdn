// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IEd25519Verifier
/// @notice Stable surface for verifying an ed25519 signature against a 32-byte
///         message digest. Used by `StakingRegistry.registerNode` (and by
///         `reclaimNodeId` in a later PR) to prove ownership of the iroh NodeId's
///         private key without taking an audit-time dependency on the concrete
///         ed25519 library — see ADR 003 § NodeId Ownership Verification.
/// @dev    The implementation is a separate audit boundary; swap it out if
///         RIP-7212 ever lands on Arbitrum and a precompile becomes available.
///         Until then the production implementation wraps a vetted Solidity
///         library (e.g., ed25519-sol). Tests use a stub from
///         `test/mocks/MockEd25519Verifier.sol`.
interface IEd25519Verifier {
    /// @param publicKey    32-byte ed25519 public key (the iroh NodeId).
    /// @param messageHash  32-byte digest the signature was produced over.
    /// @param signature    64-byte ed25519 signature.
    /// @return ok          true iff `signature` is a valid ed25519 signature
    ///                     for `messageHash` under `publicKey`.
    /// @dev    `view`: ed25519 verification is a pure computation. Declaring
    ///         it `view` means callers reach it via `STATICCALL`, which
    ///         cannot reenter — so `StakingRegistry.registerNode` is free of
    ///         the external-call-before-state-write reentrancy class even
    ///         though it commits the binding after verifying. Test mocks
    ///         assert call arguments via `vm.expectCall` rather than storing
    ///         them, so they remain `view`-compatible.
    function verify(bytes32 publicKey, bytes32 messageHash, bytes calldata signature) external view returns (bool ok);
}
