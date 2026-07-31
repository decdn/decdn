// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title RegionScopeLib
/// @notice Pure helpers for the ADR 030 § Region-stability window ripening
///         predicate: region string→bytes32 packing and the three-leg scope
///         test (global ∪ current-region ∪ ripening-prev-region). The window
///         runs from the operator's `regionLastChanged` stamp, which callers
///         pass in directly as `effective`.
/// @dev    Every function is `internal`, so the library inlines into its
///         consumers (`SlashJudge`, `ContentBlacklist`) rather than deploying
///         separately. This keeps the predicate logic — and the string-packing
///         assembly — entirely out of `CapacityBond`'s near-EIP-170-ceiling
///         runtime bytecode (`CapacityBond` never imports this library). Mirrors
///         the `BondMath.reduceAtTier` extraction rationale.
library RegionScopeLib {
    /// @notice Pack a region string into a left-aligned, zero-padded `bytes32`,
    ///         matching Solidity's `bytes32("literal")` packing so the result
    ///         compares equal to `ContentBlacklist`'s `bytes32` region keys.
    /// @dev    `CapacityBond.MAX_REGION_HINT_BYTES == 16` guarantees `len < 32` for
    ///         every real caller, so the masked path is what actually runs. Memory
    ///         bytes beyond `len` are not guaranteed zero, hence the mask. The
    ///         `len >= 32` branch is a defensive guard: the mask shift `sub(32, len)`
    ///         would underflow for such an input, so longer strings are truncated to
    ///         their first 32 bytes (already a full word) rather than mis-masked.
    function pack(string memory s) internal pure returns (bytes32 out) {
        bytes memory b = bytes(s);
        uint256 len = b.length;
        if (len == 0) return bytes32(0);
        if (len >= 32) {
            // The first 32 bytes are a full word — no masking needed (and the
            // mask shift below would underflow). Defensive: not reached by real
            // callers (regions are <= 16 bytes).
            // slither-disable-next-line assembly
            assembly ("memory-safe") {
                out := mload(add(b, 32))
            }
            return out;
        }
        // slither-disable-next-line assembly
        assembly ("memory-safe") {
            // Load the first word of the string data (left-aligned: first char
            // in the most-significant byte), then keep only the top `len` bytes
            // by AND-ing with a mask whose high `len` bytes are 1 and the low
            // `32 - len` bytes are 0 (`not(0) << ((32 - len) * 8)`).
            out := and(mload(add(b, 32)), shl(mul(sub(32, len), 8), not(0)))
        }
    }

    /// @notice The single source of truth for the ADR 030 § Region-stability
    ///         window scope predicate's NON-global legs: the region keys whose
    ///         blacklist entries currently apply to a node. An entry is in scope
    ///         iff it is global (handled by the caller via its own GLOBAL lookup),
    ///         OR its region == `currentKey`, OR (the change has not ripened) its
    ///         region == `prevKey` while `prevApplies`.
    /// @param globalRegion The global-scope sentinel (`bytes32("GLOBAL")`).
    /// @param regionHint   Operator's current attested region (string).
    /// @param regionPrev   Operator's previous region (string; "" if never changed).
    /// @param nowTs        The instant the ripening window is measured at, in
    ///                     **seconds** (same unit as `block.timestamp` and
    ///                     `effective`/`window`). The read-path caller
    ///                     (`ContentBlacklist`) passes `block.timestamp` ("what
    ///                     should I serve now"); the slash-path caller
    ///                     (`SlashJudge`) passes the served `responseTs` floored
    ///                     to seconds (`uint64(responseTsUs / 1_000_000)`, "was
    ///                     this past serve in scope") — ADR 030 § Region-stability
    ///                     window item 2 (#801). Callers MUST NOT pass μs here:
    ///                     a μs `nowTs` makes `elapsed` ~1e6× too large and
    ///                     silently mis-scopes the window.
    /// @param effective    The `effectiveSince` stamp for the operator.
    /// @param window       REGION_STABILITY_WINDOW (seconds).
    /// @return currentKey  Packed current region, or `bytes32(0)` when it is empty
    ///                     or GLOBAL (those are not regional-leg matches — GLOBAL
    ///                     is the caller's separate concern). Callers must still
    ///                     skip a `bytes32(0)` key.
    /// @return prevKey     Packed previous region (only meaningful when `prevApplies`).
    /// @return prevApplies True iff the ripening window is still open AND `prevKey`
    ///                     is a distinct, non-empty, non-GLOBAL region.
    /// @dev Centralizes the GLOBAL/empty exclusions, the window check, and the
    ///      `prev != current` dedup so `SlashJudge` and `ContentBlacklist` cannot
    ///      drift. Each caller layers its own liveness predicate (response-anchored
    ///      `_liveBefore` vs. point-in-time `_isLive`) over the returned keys.
    ///      For the read-path (`block.timestamp`) caller, `effective` is always a
    ///      past stamp (timestamps increase monotonically per block), so `elapsed`
    ///      equals `nowTs - effective`. For the slash-path caller `nowTs` is the
    ///      served `responseTs` in seconds (`responseTsUs / 1_000_000`), which CAN
    ///      precede `effective` whenever the operator
    ///      changed region *after* serving (the common evasion case): the saturating
    ///      guard for `nowTs < effective` then yields `elapsed == 0`, conservatively
    ///      treating the serve as "before the change ripened" so the prev (serve-time)
    ///      region stays in scope — exactly the intended slash semantics, not merely
    ///      a defensive underflow guard. `regionPrev == ""` packs to
    ///      `bytes32(0)` (never a real key, since `addHashRegional` rejects it), so
    ///      an empty prev never applies.
    function scopedRegions(
        bytes32 globalRegion,
        string memory regionHint,
        string memory regionPrev,
        uint64 nowTs,
        uint64 effective,
        uint256 window
    ) internal pure returns (bytes32 currentKey, bytes32 prevKey, bool prevApplies) {
        currentKey = pack(regionHint);
        if (currentKey == globalRegion) currentKey = bytes32(0);

        uint256 elapsed = nowTs > effective ? uint256(nowTs) - uint256(effective) : 0;
        if (elapsed < window) {
            bytes32 prev = pack(regionPrev);
            if (prev != bytes32(0) && prev != globalRegion && prev != currentKey) {
                prevKey = prev;
                prevApplies = true;
            }
        }
    }
}
