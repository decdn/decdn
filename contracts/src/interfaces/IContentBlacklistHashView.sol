// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IContentBlacklistHashView
/// @notice Consumer-side view of `ContentBlacklist.getHashEntry` used by
///         `SlashJudge.submitBlacklistChallenge` to confirm a hash was
///         enforceable before the challenged response time (ADR 014 § Blacklist
///         violation, step 6).
/// @dev    `ContentBlacklist.getHashEntry` returns a static `HashEntry` struct
///         `{uint64 addedAt; uint64 effectiveAt; bool emergency; uint8
///         category}`; a static struct return is ABI-identical to the
///         flattened tuple, so this tuple-returning declaration decodes
///         correctly. **The selector is derived from the INPUTS only, so it is
///         unaffected by the return shape — a mismatch here does not revert, it
///         silently mis-decodes, shifting every word after the dropped field.**
///         That is why the field order must track the struct exactly, and why
///         a matching selector is not evidence that it does. `addedAt == 0`
///         means the (region, hash) pair is not blacklisted.
/// @dev    Slash eligibility is anchored to `effectiveAt`, NOT `addedAt`: ADR
///         011 § Compliance Window gives a node a grace period after an add in
///         which it cannot yet be expected to know the entry exists. `addedAt`
///         stays on the surface because the emergency auto-expiry deadline is
///         derived from it, and `emergency`/`category` are what make that
///         deadline computable by a consumer.
interface IContentBlacklistHashView {
    function getHashEntry(bytes32 region, bytes32 hash)
        external
        view
        returns (uint64 addedAt, uint64 effectiveAt, bool emergency, uint8 category);
}
