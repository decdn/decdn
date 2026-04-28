// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IContentBlacklist
/// @notice External interface exposed to SlashJudge for blacklist verification.
/// @dev See ADR 011 (content takedown) and ADR 014 (blacklist-violation challenges).
interface IContentBlacklist {
    struct Entry {
        uint64 addedAt;
        uint64 removedAt; // zero while still active
        bool exists;
    }

    /// @notice True if `hash` is currently blacklisted globally.
    function isBlacklisted(
        bytes32 hash
    ) external view returns (bool);

    /// @notice Returns the current entry for `hash`. `entry.exists` is false
    /// when the hash has never been listed.
    function getEntry(
        bytes32 hash
    ) external view returns (Entry memory entry);

    /// @notice Returns true if `hash` was listed at `timestamp` under any
    /// historical interval, accounting for emergency expiry. Used by
    /// `SlashJudge.submitBlacklistChallenge` to verify that an offense
    /// evidence actually corresponds to a blacklisted moment, even if the
    /// hash has since been removed and/or re-added.
    function wasBlacklistedAt(
        bytes32 hash,
        uint64 timestamp
    ) external view returns (bool);

    /// @notice Monotonically-increasing version counter. Bumped on every
    /// blacklist mutation so gossip consumers can detect staleness (ADR 011).
    function blacklistVersion() external view returns (uint256);
}
