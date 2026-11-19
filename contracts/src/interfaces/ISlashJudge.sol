// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ISlashJudge
/// @notice Canonical external surface of `SlashJudge` (ADR 014 § SlashJudge
///         Contract). Adjudicates the signature-dependent offense — blacklist
///         violation — via a two-phase commit–reveal flow (ADR 014 § Challenge
///         front-running mitigation, #854): `commitChallenge` registers an opaque
///         commitment, then a `submitBlacklistChallenge` reveal resolves once the
///         commitment matures (no counter-evidence window). Each reveal emits the
///         canonical `Slashed` record consumed by `SlashAppeal.openSlashAppeal(slashId,
///         evidenceBundleHash)` (ADR 028).
/// @dev    The `responseData` argument is an ABI-encoded struct (not postcard
///         wire bytes) whose field order matches the EIP-712 typed data; the
///         contract decodes it, reconstructs the EIP-712 struct hash, and
///         verifies the signature by ECDSA recovery only (no ERC-1271).
///         The `salt` argument reconstructs the caller's commitment
///         `keccak256(abi.encode(evidenceHash, salt, msg.sender))`.
interface ISlashJudge {
    /// @dev Ordering is contract-canonical (ADR 014 § Consequences). The
    ///      ordinal is durable and load-bearing in four places, so a reorder
    ///      after deployment is a migration, not an edit:
    ///      - `SlashEscrowLib.SlashRecord.offenseType` — persisted storage;
    ///      - the `Slashed` events here and on `CapacityBond` — permanent logs,
    ///        non-indexed, so topic0 does not change and old logs silently
    ///        re-decode with the new meanings;
    ///      - the `evidenceHash` preimage (`submitBlacklistChallenge`), which
    ///        keys `usedEvidenceHash`;
    ///      - `commitments`, transitively — an outstanding pre-reorder commit
    ///        reveals into `NoCommitment()`.
    ///      `SlashAppeal` is *not* among them: it is offense-agnostic and reads
    ///      only the operator from `slashRecords`. Selectors alone cannot catch
    ///      a reorder.
    enum OffenseType {
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
    ///         (`submitBlacklistChallenge`) is valid only after `MIN_REVEAL_DELAY`
    ///         and before `REVEAL_WINDOW` have elapsed. Binding the challenger
    ///         means a mempool copy of the reveal cannot steal the 50% reward (#854).
    function commitChallenge(bytes32 commitment) external;

    /// @notice Blacklist violation: a signed stream response with `ok:true` for
    ///         a hash blacklisted before the response time. A signed refusal
    ///         (`ok:false`) is inert as evidence. Reveals a prior
    ///         `commitChallenge`; `salt` reconstructs it.
    function submitBlacklistChallenge(
        address challengedNode,
        bytes32 nodeId,
        bytes32 blobHash,
        bytes calldata responseData,
        bytes calldata slashSig,
        bytes32 salt
    ) external;
}
