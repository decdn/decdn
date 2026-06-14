// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ISlashJudge
/// @notice Canonical external surface of `SlashJudge` (ADR 014 § SlashJudge
///         Contract). Adjudicates the three signature-dependent offenses —
///         phantom announcement, rate manipulation, blacklist violation — via a
///         two-phase commit–reveal flow (ADR 014 § Challenge front-running
///         mitigation, #854): `commitChallenge` registers an opaque commitment,
///         then a `submit*Challenge` reveal resolves once the commitment matures
///         (no counter-evidence window). Each reveal emits the canonical `Slashed`
///         record consumed by `SlashAppeal.openSlashAppeal(slashId,
///         evidenceBundleHash)` (ADR 028).
/// @dev    The `*ResponseData` arguments are ABI-encoded structs (not postcard
///         wire bytes) whose field order matches the EIP-712 typed data; the
///         contract decodes them, reconstructs the EIP-712 struct hash, and
///         verifies with `SignatureChecker.isValidSignatureNow` (EOA + ERC-1271).
///         The `salt` argument reconstructs the caller's commitment
///         `keccak256(abi.encode(evidenceHash, salt, msg.sender))`.
interface ISlashJudge {
    /// @dev Ordering is contract-canonical (ADR 014 § Consequences): a reorder
    ///      requires a coordinated migration of `SlashAppeal`.
    enum OffenseType {
        Phantom,
        RateManipulation,
        Blacklist
    }

    /// @notice Emitted on every slash that reduces operator bond. `slashId` is
    ///         the globally-monotonic id minted by `CapacityBond.slash`;
    ///         `evidenceHash` is `keccak256` over the per-offense canonical
    ///         preimage (prefixed by `uint8(offenseType)`). Consumed by
    ///         `SlashAppeal.openSlashAppeal(slashId, evidenceBundleHash)`.
    event Slashed(
        uint256 indexed slashId, address indexed operator, OffenseType offenseType, uint256 amount, bytes32 evidenceHash
    );

    /// @notice Phase 1 of every challenge: register an opaque commitment
    ///         `keccak256(abi.encode(evidenceHash, salt, msg.sender))`. The reveal
    ///         (`submit*Challenge`) is valid only after `MIN_REVEAL_DELAY` and
    ///         before `REVEAL_WINDOW` have elapsed. Binding the challenger means a
    ///         mempool copy of the reveal cannot steal the 50% reward (#854).
    function commitChallenge(bytes32 commitment) external;

    /// @notice Phantom announcement: a signed `ProbeResponse{hasBlob:true}` and a
    ///         signed `StreamResponse{ok:false}` for the same hash within 30s.
    ///         Reveals a prior `commitChallenge`; `salt` reconstructs it.
    function submitPhantomChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes calldata probeResponseData,
        bytes calldata probeSlashSig,
        bytes calldata streamResponseData,
        bytes calldata streamSlashSig,
        bytes32 salt
    ) external;

    /// @notice Rate manipulation: a signed pair where the stream rate exceeds the
    ///         probe rate for the same hash within 30s. Reveals a prior
    ///         `commitChallenge`; `salt` reconstructs it.
    function submitRateChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes calldata probeResponseData,
        bytes calldata probeSlashSig,
        bytes calldata streamResponseData,
        bytes calldata streamSlashSig,
        bytes32 salt
    ) external;

    /// @notice Blacklist violation: a signed response (probe `hasBlob:true` or
    ///         stream `ok:true`) for a hash blacklisted before the response time.
    ///         Reveals a prior `commitChallenge`; `salt` reconstructs it.
    function submitBlacklistChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes32 blobHash,
        bytes calldata responseData,
        bytes calldata slashSig,
        bool isStreamResponse,
        bytes32 salt
    ) external;
}
