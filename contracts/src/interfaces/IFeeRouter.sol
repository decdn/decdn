// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IFeeRouter
/// @notice External surface of `FeeRouter`: the governance-chain receiver of
///         payment-chain sweeps (ADR 016 § Contract: FeeRouter, ADR 026 §
///         FeeRouter split) and the canonical served-bytes accountant that
///         `DecdnGovernor` reads for voting weight (ADR 036).
/// @dev    `deposit` is the one bytes path. It knows no bridge: a bridge
///         adapter (`CctpSweepReceiver` for CCTP V2) receives a sweep and
///         calls it with the USDC that arrived (ADR 016 § Bridge adapters).
///         The weight credit is priced on USDC that stays with the protocol,
///         so the caller needs no authorization.
interface IFeeRouter {
    // ─── Deposit entrypoint ───────────────────────────────────────────

    /// @notice Pull `amount` USDC from the caller, split it into rebate,
    ///         buyback and treasury legs, and credit up to `servedBytes` to
    ///         `operator` in the arrival epoch. Permissionless.
    function deposit(address operator, uint256 servedBytes, uint256 amount) external;

    // ─── Per-epoch byte accounting (ADR 036) ──────────────────────────

    /// @notice Per-operator credited bytes in `epoch`. Written by `deposit`.
    ///         The governance vote-weight source per ADR 036.
    function bytesPerEpoch(address operator, uint64 epoch) external view returns (uint256);

    /// @notice Network-wide credited bytes in `epoch`. Written by `deposit`
    ///         alongside the per-operator counter. Used by `DecdnGovernor` for
    ///         the per-operator vote cap and the quorum and threshold
    ///         denominators per ADR 036.
    function totalBytesPerEpoch(uint64 epoch) external view returns (uint256);

    /// @notice Trailing-window sum of an operator's credited bytes ending
    ///         inclusively at `endEpoch` over `n` epochs. O(n) cold SLOADs.
    function bytesInWindow(address operator, uint64 endEpoch, uint64 n) external view returns (uint256);

    /// @notice Trailing-window sum of network credited bytes ending
    ///         inclusively at `endEpoch` over `n` epochs. O(n) cold SLOADs.
    function totalBytesInWindow(uint64 endEpoch, uint64 n) external view returns (uint256);

    /// @notice Current trailing-window length (default 13, bounded [4, 26]).
    function windowEpochs() external view returns (uint64);

    /// @notice `windowEpochs` value as of `timepoint` (ERC-6372 timestamp
    ///         mode). `DecdnGovernor` reads it so a governance change to the
    ///         window cannot shift quorum or weights for proposals whose
    ///         snapshot predates the change.
    function windowEpochsAt(uint48 timepoint) external view returns (uint64);

    /// @notice Constructor-immutable epoch length. Equals
    ///         `CapacityBond.epochLength()` (7 days), so `declaredMbpsAtEpoch`
    ///         and `bytesPerEpoch` index the same epochs.
    function epochLength() external view returns (uint64);

    // ─── Split and pricing (ADR 016 § Tunable Economics) ──────────────

    /// @notice Share of each deposit returned to an active operator, in bps.
    function rebateBps() external view returns (uint256);

    /// @notice Share of each deposit sent to `buybackBurner`, in bps.
    function buybackBps() external view returns (uint256);

    /// @notice Destination of the buyback leg. Zero while `buybackBps` is 0.
    function buybackBurner() external view returns (address);

    /// @notice Destination of the remainder (the `TimelockController`).
    function treasury() external view returns (address);

    /// @notice Net USDC base units per MiB of weight credit (bounded [1, 1000]).
    function deliveryFloor() external view returns (uint256);
}
