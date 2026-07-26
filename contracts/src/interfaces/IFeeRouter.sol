// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IFeeRouter
/// @notice External surface of `FeeRouter` — the three-bucket per-byte settlement
///         distributor (ADR 026 § FeeRouter split) and the canonical served-bytes
///         accountant that `DecdnGovernor` reads for voting weight per ADR 036.
/// @dev    Settlement: `PaymentChannel.settleChannel` forwards the operator's
///         full USDC balance to `routeSettlement`, which performs the
///         default 60% operator / 30% buyback / 10% treasury split inline
///         (governance-mutable within per-bucket bounds via `setShares`),
///         and updates per-operator + global byte counters.
///
///         Vote weight: `DecdnGovernor._getVotes` calls `bytesInWindow` and
///         `totalBytesInWindow` over the trailing `windowEpochs`-long window
///         (per ADR 036 § Formula). The window length is governance-mutable
///         within [4, 26]; `EPOCH_LENGTH` is constructor-immutable at 1 week
///         (ADR 026 § FeeRouter split).
interface IFeeRouter {
    // ─── Settlement entrypoint ────────────────────────────────────────

    /// @notice Distribute `amount` USDC across the three buckets and stamp the
    ///         served `bytesDelivered` into the current epoch. Called by
    ///         `PaymentChannel.settleChannel` under `ROUTER_CALLER_ROLE`.
    function routeSettlement(address operator, uint256 bytesDelivered, uint256 amount) external;

    // ─── Per-epoch byte accounting (ADR 036) ──────────────────────────

    /// @notice Per-operator served bytes in `epoch`. Populated inline by
    ///         `routeSettlement`. The governance vote-weight source per ADR 036.
    function bytesPerEpoch(address operator, uint64 epoch) external view returns (uint256);

    /// @notice Network-wide served bytes in `epoch`. Populated inline alongside
    ///         the per-operator counter. Used by `DecdnGovernor` for the per-
    ///         operator vote cap and quorum / threshold denominators per ADR 036.
    function totalBytesPerEpoch(uint64 epoch) external view returns (uint256);

    /// @notice Trailing-window sum of operator's served bytes ending inclusively
    ///         at `endEpoch` over `n` epochs. O(n) cold SLOADs.
    function bytesInWindow(address operator, uint64 endEpoch, uint64 n) external view returns (uint256);

    /// @notice Trailing-window sum of network served bytes ending inclusively
    ///         at `endEpoch` over `n` epochs. O(n) cold SLOADs.
    function totalBytesInWindow(uint64 endEpoch, uint64 n) external view returns (uint256);

    /// @notice Current trailing-window length (default 13, bounded [4, 26]).
    function windowEpochs() external view returns (uint64);

    /// @notice `windowEpochs` value as of `timepoint` (ERC-6372 timestamp
    ///         mode). Consumed by `DecdnGovernor` so a governance change to
    ///         the window cannot shift quorum / weights for proposals whose
    ///         snapshot is earlier than the change (closes the in-flight
    ///         weight-drift seam left open by `windowEpochs()` reads).
    function windowEpochsAt(uint48 timepoint) external view returns (uint64);

    /// @notice Constructor-immutable epoch length. Must equal
    ///         `CapacityBond.EPOCH_LENGTH` (7 days) at deploy time —
    ///         otherwise `slashedAtEpoch` (CapacityBond) and `bytesPerEpoch`
    ///         (FeeRouter) index different epochs and the ADR 036 slash-
    ///         zero-out misaligns. Deployment scripts MUST assert equality.
    function epochLength() external view returns (uint64);
}
