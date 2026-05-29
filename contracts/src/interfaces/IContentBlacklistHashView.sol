// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IContentBlacklistHashView
/// @notice Consumer-side view of `ContentBlacklist.getHashEntry` used by
///         `SlashJudge.submitBlacklistChallenge` to confirm a hash was
///         blacklisted before the challenged response time (ADR 014 § Blacklist
///         violation, step 6).
/// @dev    `ContentBlacklist.getHashEntry` returns a static `HashEntry` struct
///         `{uint64 addedAt; bool suspended}`; a static struct return is
///         ABI-identical to the flattened tuple, so this tuple-returning
///         declaration shares the same selector and decodes correctly.
///         `addedAt == 0` means the (region, hash) pair is not blacklisted.
interface IContentBlacklistHashView {
    function getHashEntry(bytes32 region, bytes32 hash) external view returns (uint64 addedAt, bool suspended);
}
