// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

import { ITokenMessengerV2, IMessageTransmitterV2 } from "../src/interfaces/ICctpV2.sol";

/// @title LoopbackCctp
/// @notice TESTNET / LOCAL ONLY. Both CCTP V2 sides on one chain, for a local
///         deployment where the governance chain and the payment chain are the
///         same chain. `depositForBurnWithHook` escrows the USDC and emits
///         `MessageSent` with a message in Circle's `MessageV2` /
///         `BurnMessageV2` layout; `receiveMessage` checks `destinationCaller`
///         and the nonce, accepts any non-empty attestation, and releases
///         `amount` to `mintRecipient`. It plays the `TokenMessengerV2` role
///         too, so the message's `sender` and `recipient` are this contract.
/// @dev    It verifies no attestation. Never deploy it where real funds move.
contract LoopbackCctp is ITokenMessengerV2, IMessageTransmitterV2 {
    using SafeERC20 for IERC20;

    /// @notice The CCTP domain this loopback stands in for on both sides.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint32 public immutable localDomain;

    /// @notice Nonces already received.
    mapping(bytes32 nonce => bool) public usedNonces;

    uint256 internal _nextNonce;

    /// @notice Circle's `MessageTransmitterV2` event, carrying the message to
    ///         pass to `receiveMessage`.
    event MessageSent(bytes message);

    /// @param localDomain_ The domain both sides report.
    constructor(uint32 localDomain_) {
        localDomain = localDomain_;
    }

    /// @inheritdoc ITokenMessengerV2
    function depositForBurnWithHook(
        uint256 amount,
        uint32 destinationDomain,
        bytes32 mintRecipient,
        address burnToken,
        bytes32 destinationCaller,
        uint256 maxFee,
        uint32 minFinalityThreshold,
        bytes calldata hookData
    ) external {
        require(destinationDomain == localDomain, "LoopbackCctp: other domain");
        require(amount != 0 && maxFee < amount, "LoopbackCctp: bad amount");
        IERC20(burnToken).safeTransferFrom(msg.sender, address(this), amount);
        bytes32 self = bytes32(uint256(uint160(address(this))));
        bytes memory body = abi.encodePacked(
            uint32(1),
            bytes32(uint256(uint160(burnToken))),
            mintRecipient,
            amount,
            bytes32(uint256(uint160(msg.sender))),
            maxFee,
            uint256(0),
            uint256(0),
            hookData
        );
        emit MessageSent(
            abi.encodePacked(
                uint32(1),
                localDomain,
                destinationDomain,
                keccak256(abi.encode(address(this), _nextNonce++)),
                self,
                self,
                destinationCaller,
                minFinalityThreshold,
                minFinalityThreshold,
                body
            )
        );
    }

    /// @notice Deployed V2 messengers have no minimum fee; neither does this.
    function getMinFeeAmount(uint256) external pure returns (uint256) {
        revert("LoopbackCctp: no minFee");
    }

    /// @inheritdoc IMessageTransmitterV2
    function receiveMessage(bytes calldata message, bytes calldata attestation) external returns (bool) {
        require(attestation.length != 0, "LoopbackCctp: empty attestation");
        bytes32 caller = bytes32(message[108:140]);
        require(caller == bytes32(0) || caller == bytes32(uint256(uint160(msg.sender))), "Invalid caller for message");
        bytes32 nonce = bytes32(message[12:44]);
        require(!usedNonces[nonce], "Nonce already used");
        usedNonces[nonce] = true;
        require(bytes32(message[76:108]) == bytes32(uint256(uint160(address(this)))), "LoopbackCctp: recipient");
        bytes calldata body = message[148:];
        address token = address(uint160(uint256(bytes32(body[4:36]))));
        address to = address(uint160(uint256(bytes32(body[36:68]))));
        uint256 amount = uint256(bytes32(body[68:100]));
        IERC20(token).safeTransfer(to, amount);
        return true;
    }
}
