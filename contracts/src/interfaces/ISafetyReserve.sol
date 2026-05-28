// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ISafetyReserve
/// @notice Full external surface of `SafetyReserve` — the 5% FeeRouter bucket
///         + 30% slash-redirect leg custodian, payout authorizer, keeper-
///         driven TOKEN→USDC swap, and slash-appeal venue.
/// @dev    Composed of three independent capability groups:
///           1. Core payout (ADR 033 § Decision): `payout`, `incidents`,
///              `disbursePending`.
///           2. Slash-inflow accounting (ADR 026 § Slashing and burn):
///              `recordSlashInflow`, `swapAccumulatedTokens`.
///           3. Slash-appeals (ADR 028 § Contract surface, ADR 032):
///              `openSlashAppeal`, `fastTrackAppeal`, `rejectAppeal`,
///              `ratifyAppeal`, `reverseAppeal`, `cleanupExpiredAppeal`.
interface ISafetyReserve {
    // ─── Core payout ──────────────────────────────────────────────────

    struct Incident {
        bytes32 bundle;
        address recipient;
        uint256 usdcAmount;
        uint64 paidAt;
        bytes32 reason;
    }

    /// @notice Authorize a USDC payout from the reserve. Restricted to
    ///         `GOVERNANCE_ROLE`. Appends to the immutable `incidents` ledger.
    /// @return incidentId Index of the newly-appended incident in `incidents`.
    function payout(bytes32 bundle, address recipient, uint256 usdcAmount)
        external
        returns (uint256 incidentId, bool queued, uint256 claimId);

    /// @notice Permissionless head-of-queue disbursement of `PendingClaim`s in
    ///         epoch-FIFO order. Returns 0 if the queue is empty.
    function disbursePending() external returns (uint256 incidentId);

    function incidents(uint256 id) external view returns (Incident memory);

    // ─── Slash-inflow accounting ──────────────────────────────────────

    /// @notice Called by `CapacityBond.slash` immediately after the 30% TOKEN
    ///         transfer to this contract. Carries `SLASH_INFLOW_REPORTER_ROLE`.
    function recordSlashInflow(address operator, uint256 amount) external;

    /// @notice Keeper-triggered TOKEN→USDC swap via the Balancer V3 80/20 pool
    ///         (ADR 018). `minOut` is the slippage floor.
    function swapAccumulatedTokens(uint256 amountIn, uint256 minOut) external;

    // ─── Slash-appeals (ADR 028 § Contract surface, ADR 032) ──────────

    enum AppealStatus {
        Open,
        FastTracked,
        Rejected,
        Ratified,
        Reversed,
        Lapsed
    }

    /// @notice Permissionless filing of a slash-appeal. The appellant posts
    ///         `APPEAL_BOND` (TOKEN). Operator is read from
    ///         `CapacityBond.slashRecords(slashId)` — the appellant cannot
    ///         spoof a different operator. Reverts if the 30-day filing
    ///         window since the underlying slash has elapsed, or if the
    ///         underlying operator has already had a successful appeal in
    ///         the trailing 365 days.
    function openSlashAppeal(uint256 slashId, bytes32 evidenceBundleHash) external returns (uint256 appealId);

    /// @notice Emergency-multisig provisional approval (`EMERGENCY_MULTISIG_ROLE`).
    ///         Records a USDC escrow lien at `maxAppealRestitution` (the
    ///         governance-set ceiling — bond entry-tier TWAP from ADR 028
    ///         deferred to a future ADR 018 oracle integration). Atomically
    ///         enforces the solvency invariant.
    function fastTrackAppeal(uint256 appealId) external;

    /// @notice Multisig rejection — burns 100% of the appeal bond.
    function rejectAppeal(uint256 appealId) external;

    /// @notice Governor-only ratification — releases escrow lien, records a
    ///         `slash-appeal-ratify` incident, transfers restitution USDC
    ///         directly to the operator (capped by the escrowed amount which
    ///         is itself capped at `maxAppealRestitution`), and refunds the
    ///         bond to the appellant. Does NOT clear `slashedAtEpoch` on
    ///         `CapacityBond` (per ADR 036 — `reverseAppeal` is the path
    ///         that clears it).
    function ratifyAppeal(uint256 appealId) external;

    /// @notice Governor-only reversal — calls `CapacityBond.clearSlashedAtEpoch`
    ///         (requires `APPEAL_REVERSAL_ROLE` on `CapacityBond`), burns 50%
    ///         bond, routes 50% to challenger-incentive pool.
    function reverseAppeal(uint256 appealId) external;

    /// @notice Permissionless lapse handler for appeals that timed out at the
    ///         multisig or Governor stage (14-day windows).
    function cleanupExpiredAppeal(uint256 appealId) external;
}
