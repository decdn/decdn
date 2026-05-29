// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IEd25519Verifier
/// @notice Stable surface for verifying an ed25519 signature against a 32-byte
///         message digest. Used by `CapacityBond.registerNode` /
///         `bindNodeId` / `reclaimNodeId` to prove ownership of the iroh
///         NodeId's private key without taking an audit-time dependency on
///         the concrete ed25519 library (ADR 019 § Node Onboarding —
///         operator self-attestation flow).
/// @dev    The implementation is a separate audit boundary; swap it out if
///         RIP-6565 / a ed25519 precompile ever lands on Arbitrum. Until then
///         the production implementation (`Ed25519Verifier`) wraps the audited
///         Smoo.th Crypto Lib EIP-6565 verifier, pinned as a git submodule
///         at `lib/crypto-lib`. Tests use a stub from
///         `test/mocks/MockEd25519Verifier.sol`.
interface IEd25519Verifier {
    /// @param publicKey    32-byte ed25519 public key (the iroh NodeId).
    /// @param messageHash  32-byte digest the signature was produced over.
    /// @param signature    64-byte ed25519 signature.
    /// @return ok          true iff `signature` is a valid ed25519 signature
    ///                     for `messageHash` under `publicKey`.
    /// @dev    `view`: ed25519 verification is a pure computation. Declaring
    ///         it `view` means callers reach it via `STATICCALL`, which
    ///         cannot reenter — so `CapacityBond.registerNode` is free of
    ///         the external-call-before-state-write reentrancy class even
    ///         though it commits the binding after verifying. Test mocks
    ///         assert call arguments via `vm.expectCall` rather than storing
    ///         them, so they remain `view`-compatible.
    function verify(bytes32 publicKey, bytes32 messageHash, bytes calldata signature) external view returns (bool ok);
}
