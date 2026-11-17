// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { ITokenMessengerV2, IMessageTransmitterV2 } from "../../src/interfaces/ICctpV2.sol";
import { MintableUSDC } from "./MintableUSDC.sol";

/// @notice CCTP V2 message encoding, byte for byte as Circle's `MessageV2` and
///         `BurnMessageV2` lay it out (header: sourceDomain 4, sender 44,
///         recipient 76, destinationCaller 108, body 148; burn body:
///         mintRecipient 36, amount 68, messageSender 100, feeExecuted 164,
///         hookData 228).
library CctpMessage {
    struct Burn {
        uint32 sourceDomain;
        uint32 destinationDomain;
        bytes32 nonce;
        bytes32 sender;
        bytes32 recipient;
        bytes32 destinationCaller;
        bytes32 burnToken;
        bytes32 mintRecipient;
        uint256 amount;
        bytes32 messageSender;
        uint256 maxFee;
        uint256 feeExecuted;
        bytes hookData;
    }

    function encode(Burn memory b) internal pure returns (bytes memory) {
        bytes memory body = abi.encodePacked(
            uint32(1),
            b.burnToken,
            b.mintRecipient,
            b.amount,
            b.messageSender,
            b.maxFee,
            b.feeExecuted,
            uint256(0),
            b.hookData
        );
        return abi.encodePacked(
            uint32(1),
            b.sourceDomain,
            b.destinationDomain,
            b.nonce,
            b.sender,
            b.recipient,
            b.destinationCaller,
            uint32(2000),
            uint32(2000),
            body
        );
    }

    function toBytes32(address a) internal pure returns (bytes32) {
        return bytes32(uint256(uint160(a)));
    }
}

/// @notice Destination-side CCTP: checks `destinationCaller` and the nonce,
///         then hands the body to the recipient `MockTokenMessengerV2`.
contract MockMessageTransmitterV2 is IMessageTransmitterV2 {
    uint32 public immutable localDomain;
    mapping(bytes32 => bool) public usedNonces;

    constructor(uint32 localDomain_) {
        localDomain = localDomain_;
    }

    function receiveMessage(bytes calldata message, bytes calldata attestation) external returns (bool) {
        require(attestation.length != 0, "Invalid attestation");
        require(uint32(bytes4(message[8:12])) == localDomain, "Invalid destination domain");
        bytes32 caller = bytes32(message[108:140]);
        require(caller == bytes32(0) || caller == bytes32(uint256(uint160(msg.sender))), "Invalid caller for message");
        bytes32 nonce = bytes32(message[12:44]);
        require(!usedNonces[nonce], "Nonce already used");
        usedNonces[nonce] = true;
        address recipient = address(uint160(uint256(bytes32(message[76:108]))));
        return MockTokenMessengerV2(recipient)
            .handleReceiveFinalizedMessage(uint32(bytes4(message[4:8])), bytes32(message[44:76]), message[148:]);
    }
}

/// @notice Both sides of `TokenMessengerV2`. Source side: burns and records
///         the message it would emit. Destination side: checks the remote
///         messenger and mints `amount - feeExecuted` of the local token.
contract MockTokenMessengerV2 is ITokenMessengerV2 {
    uint32 public immutable localDomain;
    address public transmitter;
    mapping(uint32 => bytes32) public remoteTokenMessengers;
    mapping(bytes32 => MintableUSDC) public localTokenFor;

    /// @dev Zero leaves `getMinFeeAmount` reverting, like deployed V2.
    uint256 public minFeeBps;
    bool public hasMinFee;

    bytes public lastMessage;
    uint256 internal _nonce;

    constructor(uint32 localDomain_) {
        localDomain = localDomain_;
    }

    function setTransmitter(address t) external {
        transmitter = t;
    }

    function setRemote(uint32 domain, address messenger) external {
        remoteTokenMessengers[domain] = CctpMessage.toBytes32(messenger);
    }

    function setLocalToken(bytes32 remoteToken, MintableUSDC local) external {
        localTokenFor[remoteToken] = local;
    }

    function setMinFee(uint256 bps) external {
        hasMinFee = true;
        minFeeBps = bps;
    }

    function getMinFeeAmount(uint256 amount) external view returns (uint256) {
        require(hasMinFee, "no minFee");
        uint256 fee = (amount * minFeeBps) / 10_000;
        return fee == 0 && minFeeBps != 0 ? 1 : fee;
    }

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
        require(amount > 0, "Amount must be nonzero");
        require(maxFee < amount, "Max fee must be less than amount");
        require(minFinalityThreshold == 2000, "test expects standard finality");
        if (hasMinFee) require(maxFee >= this.getMinFeeAmount(amount), "Insufficient max fee");
        require(hookData.length > 0, "Hook data is empty");
        IERC20(burnToken).transferFrom(msg.sender, address(0xdead), amount);
        lastMessage = CctpMessage.encode(
            CctpMessage.Burn({
                sourceDomain: localDomain,
                destinationDomain: destinationDomain,
                nonce: keccak256(abi.encode(localDomain, _nonce++)),
                sender: CctpMessage.toBytes32(address(this)),
                recipient: remoteTokenMessengers[destinationDomain],
                destinationCaller: destinationCaller,
                burnToken: CctpMessage.toBytes32(burnToken),
                mintRecipient: mintRecipient,
                amount: amount,
                messageSender: CctpMessage.toBytes32(msg.sender),
                maxFee: maxFee,
                feeExecuted: 0,
                hookData: hookData
            })
        );
    }

    function handleReceiveFinalizedMessage(uint32 remoteDomain, bytes32 sender, bytes calldata body)
        external
        returns (bool)
    {
        require(msg.sender == transmitter, "Invalid message transmitter");
        require(remoteTokenMessengers[remoteDomain] == sender, "Remote TokenMessenger unsupported");
        MintableUSDC token = localTokenFor[bytes32(body[4:36])];
        require(address(token) != address(0), "Mint token not supported");
        address mintRecipient = address(uint160(uint256(bytes32(body[36:68]))));
        uint256 amount = uint256(bytes32(body[68:100]));
        uint256 fee = uint256(bytes32(body[164:196]));
        token.mint(mintRecipient, amount - fee);
        return true;
    }
}
