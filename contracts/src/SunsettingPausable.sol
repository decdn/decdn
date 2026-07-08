// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";

/// @title  SunsettingPausable — a Pausable whose pause capability expires.
/// @notice ADR 009 § Emergency Multisig: the protocol-wide pause is the most
///         centralization-sensitive emergency power, so it sunsets hard at a
///         deploy-time deadline. After the deadline `pause()` reverts for
///         every caller — including governance — leaving the protocol with no
///         standing freeze switch (progressive immutability). The narrow,
///         permanent unlawful-content-removal powers live on the non-pausable
///         `ContentBlacklist` and are unaffected by this sunset.
/// @dev    Each deploying contract anchors its own deadline at construction
///         (`block.timestamp + PAUSE_SUNSET_PERIOD`), so contracts in one
///         deploy batch expire within minutes of each other. The deadline is
///         `immutable`, so governance cannot extend it — the only post-sunset
///         recourse is a governed redeploy with a fresh deadline.
abstract contract SunsettingPausable is Pausable {
    /// @notice Lifetime of the pause capability, measured from deployment.
    uint256 internal constant PAUSE_SUNSET_PERIOD = 365 days;

    /// @notice Timestamp after which `pause()` reverts for all callers.
    uint256 public immutable pauseDeadline;

    /// @notice Thrown when `pause()` is called after `pauseDeadline`.
    error PauseExpired();

    constructor() {
        pauseDeadline = block.timestamp + PAUSE_SUNSET_PERIOD;
    }

    /// @dev Reverts `PauseExpired` once the sunset has passed. Call at the top
    ///      of each `pause()` entrypoint, before `_pause()`.
    function _requirePauseWindowOpen() internal view {
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp > pauseDeadline) revert PauseExpired();
    }
}
