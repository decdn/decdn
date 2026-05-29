// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IFeeRouterSettlement
/// @notice Consumer-side view of the single `FeeRouter` entrypoint that
///         `PaymentChannel` invokes (ADR 003 § FeeRouter Integration,
///         ADR 016 § Cross-Contract Call Graph). Kept minimal — and separate
///         from the full `IFeeRouter` — so `PaymentChannel` links only the
///         settlement selector it calls, mirroring the focused
///         `ICapacityBondReporter` consumer interface.
/// @dev    `PaymentChannel.settleChannel` / `withdraw` `approve` the router for
///         the routed delta, then call `routeSettlement` in the same
///         transaction. The router pulls the USDC via `safeTransferFrom`,
///         performs the three-bucket split, and reverts on a zero `amount`, so
///         the caller must only route strictly-positive deltas.
interface IFeeRouterSettlement {
    /// @notice Distribute `amount` USDC across the three buckets and stamp
    ///         `bytesDelivered` into the current epoch. Caller must hold
    ///         `ROUTER_CALLER_ROLE` on the router and have approved `amount`.
    function routeSettlement(address operator, uint256 bytesDelivered, uint256 amount) external;
}
