// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ITokenMessengerV2
/// @notice The Circle CCTP V2 `TokenMessengerV2` calls a `PaymentPool` makes
///         to sweep (circlefin/evm-cctp-contracts `src/v2/TokenMessengerV2.sol`).
interface ITokenMessengerV2 {
    /// @notice Burn `amount` of `burnToken` and send it to `mintRecipient` on
    ///         `destinationDomain`, with `hookData` appended to the message.
    function depositForBurnWithHook(
        uint256 amount,
        uint32 destinationDomain,
        bytes32 mintRecipient,
        address burnToken,
        bytes32 destinationCaller,
        uint256 maxFee,
        uint32 minFinalityThreshold,
        bytes calldata hookData
    ) external;

    /// @notice The minimum fee for a burn of `amount`. Absent on messenger
    ///         versions without a minimum fee; callers probe it with a
    ///         staticcall.
    function getMinFeeAmount(uint256 amount) external view returns (uint256);
}

/// @title IMessageTransmitterV2
/// @notice The Circle CCTP V2 `MessageTransmitterV2` call a bridge adapter
///         makes to receive a sweep.
interface IMessageTransmitterV2 {
    /// @notice Verify `attestation` over `message`, mark its nonce used, and
    ///         hand the body to the message's `recipient`. Only the message's
    ///         `destinationCaller` may call it when that field is non-zero.
    function receiveMessage(bytes calldata message, bytes calldata attestation) external returns (bool);
}
