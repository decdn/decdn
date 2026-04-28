// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IStablePaymentChannel
/// @notice Narrow view surface of the PoC payment-channel contract, exposed
///         so SlashJudge can authorize corruption-challenge submissions
///         against the actual channel client (ADR 014).
/// @dev Only the minimum `channelClient` lookup is included to avoid coupling
///      SlashJudge to the full Channel struct. When the multi-token
///      `PaymentChannel` replaces StablePaymentChannel in production, it
///      need only implement this same view to preserve the authorization
///      contract.
interface IStablePaymentChannel {
    /// @notice Returns the client address that opened `channelId`, or
    /// `address(0)` if the channel was never opened.
    function channelClient(
        bytes32 channelId
    ) external view returns (address);
}
