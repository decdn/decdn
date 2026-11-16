// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { IFeeRouter } from "./interfaces/IFeeRouter.sol";
import { IMessageTransmitterV2 } from "./interfaces/ICctpV2.sol";

/// @title CctpSweepReceiver
/// @notice The CCTP V2 bridge adapter (ADR 016 § Bridge adapters). Receives one
///         sweep from a payment-chain `PaymentPool` and deposits the USDC that
///         arrived into `FeeRouter` for the operator named in the hook data.
/// @dev    Stateless, no roles, no setters. Each sweep names this contract as
///         `mintRecipient` and `destinationCaller`, so only this contract can
///         receive it, and anyone can make it do so. Message layout follows
///         Circle's `MessageV2` / `BurnMessageV2`.
contract CctpSweepReceiver is ReentrancyGuard {
    using SafeERC20 for IERC20;

    // Circle CCTP V2 message offsets (circlefin/evm-cctp-contracts).
    uint256 internal constant SOURCE_DOMAIN_INDEX = 4;
    uint256 internal constant RECIPIENT_INDEX = 76;
    uint256 internal constant BODY_INDEX = 148;
    uint256 internal constant MINT_RECIPIENT_INDEX = 36;
    uint256 internal constant MESSAGE_SENDER_INDEX = 100;
    uint256 internal constant HOOK_DATA_INDEX = 228;
    /// @dev `abi.encode(address operator, uint256 servedBytes)`.
    uint256 internal constant HOOK_DATA_LENGTH = 64;

    /// @notice The governance-chain USDC that CCTP mints.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IERC20 public immutable usdc;
    /// @notice The local CCTP `MessageTransmitterV2`.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IMessageTransmitterV2 public immutable messageTransmitter;
    /// @notice The local CCTP `TokenMessengerV2`, the only accepted recipient.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    address public immutable tokenMessenger;
    /// @notice Where every sweep is deposited.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IFeeRouter public immutable feeRouter;

    /// @notice One sweep landed. `pool` is the burn's `messageSender` and
    ///         `amount` the USDC deposited into `FeeRouter`.
    event SweepReceived(uint32 indexed sourceDomain, address indexed pool, address indexed operator, uint256 amount);

    /// @notice A constructor address is zero.
    error ZeroAddress();
    /// @notice The message is shorter than a burn message with sweep hook data.
    error MalformedMessage();
    /// @notice The message's recipient is not the local `TokenMessengerV2`.
    error NotTokenMessenger();
    /// @notice The burn's `mintRecipient` is not this contract.
    error NotMintRecipient();
    /// @notice `receiveMessage` returned false.
    error ReceiveFailed();
    /// @notice The message minted no USDC to this contract.
    error NothingMinted();

    /// @param usdc_               Governance-chain USDC.
    /// @param messageTransmitter_ Local CCTP `MessageTransmitterV2`.
    /// @param tokenMessenger_     Local CCTP `TokenMessengerV2`.
    /// @param feeRouter_          The `FeeRouter` to deposit into.
    constructor(
        IERC20 usdc_,
        IMessageTransmitterV2 messageTransmitter_,
        address tokenMessenger_,
        IFeeRouter feeRouter_
    ) {
        if (
            address(usdc_) == address(0) || address(messageTransmitter_) == address(0) || tokenMessenger_ == address(0)
                || address(feeRouter_) == address(0)
        ) revert ZeroAddress();
        usdc = usdc_;
        messageTransmitter = messageTransmitter_;
        tokenMessenger = tokenMessenger_;
        feeRouter = feeRouter_;
    }

    /// @notice Receive one attested sweep and deposit it. Permissionless.
    /// @dev    `U` is this contract's USDC balance rise across
    ///         `receiveMessage`, so a burn of any other token deposits nothing.
    ///         A paused `FeeRouter` reverts the whole call, and the message
    ///         stays valid for a later attempt.
    function receiveSweep(bytes calldata message, bytes calldata attestation) external nonReentrant {
        if (message.length < BODY_INDEX + HOOK_DATA_INDEX + HOOK_DATA_LENGTH) revert MalformedMessage();
        if (bytes32(message[RECIPIENT_INDEX:RECIPIENT_INDEX + 32]) != _toBytes32(tokenMessenger)) {
            revert NotTokenMessenger();
        }
        bytes calldata body = message[BODY_INDEX:];
        if (bytes32(body[MINT_RECIPIENT_INDEX:MINT_RECIPIENT_INDEX + 32]) != _toBytes32(address(this))) {
            revert NotMintRecipient();
        }
        (address operator, uint256 servedBytes) = abi.decode(body[HOOK_DATA_INDEX:], (address, uint256));

        // The balance read across `receiveMessage` is the measurement itself:
        // `minted` is what this message minted, under `nonReentrant`.
        // slither-disable-start reentrancy-balance
        uint256 balanceBefore = usdc.balanceOf(address(this));
        if (!messageTransmitter.receiveMessage(message, attestation)) revert ReceiveFailed();
        uint256 minted = usdc.balanceOf(address(this)) - balanceBefore;
        // slither-disable-next-line incorrect-equality
        if (minted == 0) revert NothingMinted();
        // slither-disable-end reentrancy-balance

        usdc.forceApprove(address(feeRouter), minted);
        feeRouter.deposit(operator, servedBytes, minted);

        emit SweepReceived(
            uint32(bytes4(message[SOURCE_DOMAIN_INDEX:SOURCE_DOMAIN_INDEX + 4])),
            address(uint160(uint256(bytes32(body[MESSAGE_SENDER_INDEX:MESSAGE_SENDER_INDEX + 32])))),
            operator,
            minted
        );
    }

    function _toBytes32(address a) internal pure returns (bytes32) {
        return bytes32(uint256(uint160(a)));
    }
}
