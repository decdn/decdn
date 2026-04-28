// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IStablePaymentChannel } from "../../src/interfaces/IStablePaymentChannel.sol";

/// @notice Minimal test double for SlashJudge unit tests. Lets the test set
/// an arbitrary (channelId → client) mapping without standing up the full
/// StablePaymentChannel machinery.
contract MockPaymentChannel is IStablePaymentChannel {
    mapping(bytes32 => address) private _clients;

    function setChannelClient(
        bytes32 channelId,
        address client
    ) external {
        _clients[channelId] = client;
    }

    function channelClient(
        bytes32 channelId
    ) external view returns (address) {
        return _clients[channelId];
    }
}
