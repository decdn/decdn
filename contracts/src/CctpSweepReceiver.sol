// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { Math } from "@openzeppelin/contracts/utils/math/Math.sol";

import { IFeeRouter } from "./interfaces/IFeeRouter.sol";
import { IMessageTransmitterV2 } from "./interfaces/ICctpV2.sol";
import { PaymentPoolBase, SWEEP_BATCH_LIMIT } from "./PaymentPoolBase.sol";

/// @title CctpSweepReceiver
/// @notice The CCTP V2 bridge adapter (ADR 016 § Bridge adapters). Receives one
///         sweep from a payment-chain `PaymentPool` and deposits the USDC that
///         arrived into `FeeRouter`, one deposit per operator in the hook
///         data's `SweepShare[]`.
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
    /// @dev The shortest `abi.encode(SweepShare[])` hook data: the array
    ///      offset, its length and one 96-byte entry.
    uint256 internal constant MIN_HOOK_DATA_LENGTH = 160;

    /// @notice Most entries one sweep message carries (`SWEEP_BATCH_LIMIT`),
    ///         so a full batch receives in a small fraction of a block.
    uint256 public constant MAX_SWEEP_BATCH = SWEEP_BATCH_LIMIT;

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

    /// @notice One operator's share of a sweep landed. `pool` is the burn's
    ///         `messageSender` and `amount` the USDC deposited into
    ///         `FeeRouter` for `operator`. Emitted once per deposited entry.
    event SweepReceived(uint32 indexed sourceDomain, address indexed pool, address indexed operator, uint256 amount);

    /// @notice A constructor address is zero.
    error ZeroAddress();
    /// @notice The message is shorter than a burn message with sweep hook
    ///         data, or its `SweepShare[]` is empty, longer than
    ///         `MAX_SWEEP_BATCH`, or sums to zero.
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

    /// @notice Receive one attested sweep and deposit each operator's share.
    ///         Permissionless.
    /// @dev    `U` is this contract's USDC balance rise across
    ///         `receiveMessage`, so a burn of any other token deposits nothing.
    ///         `U` splits pro rata to the entry amounts, so a bridge fee comes
    ///         out of every share alike. A paused `FeeRouter` reverts the whole
    ///         call, and the message stays valid for a later attempt.
    function receiveSweep(bytes calldata message, bytes calldata attestation) external nonReentrant {
        if (message.length < BODY_INDEX + HOOK_DATA_INDEX + MIN_HOOK_DATA_LENGTH) revert MalformedMessage();
        if (bytes32(message[RECIPIENT_INDEX:RECIPIENT_INDEX + 32]) != _toBytes32(tokenMessenger)) {
            revert NotTokenMessenger();
        }
        bytes calldata body = message[BODY_INDEX:];
        if (bytes32(body[MINT_RECIPIENT_INDEX:MINT_RECIPIENT_INDEX + 32]) != _toBytes32(address(this))) {
            revert NotMintRecipient();
        }
        PaymentPoolBase.SweepShare[] memory shares = abi.decode(body[HOOK_DATA_INDEX:], (PaymentPoolBase.SweepShare[]));
        uint256 n = shares.length;
        if (n == 0 || n > MAX_SWEEP_BATCH) revert MalformedMessage();
        uint256 sum = 0;
        for (uint256 i = 0; i < n; i++) {
            sum += shares[i].amount;
        }
        if (sum == 0) revert MalformedMessage();

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
        _depositShares(
            shares,
            sum,
            minted,
            uint32(bytes4(message[SOURCE_DOMAIN_INDEX:SOURCE_DOMAIN_INDEX + 4])),
            address(uint160(uint256(bytes32(body[MESSAGE_SENDER_INDEX:MESSAGE_SENDER_INDEX + 32]))))
        );
    }

    /// @dev Deposit `minted` across `shares` pro rata to each `amount`. The
    ///      last entry takes the rounding remainder, so the deposits sum to
    ///      `minted`. An entry whose share is 0 is skipped: `FeeRouter`
    ///      rejects a zero deposit, and one dust entry must not block the
    ///      message.
    function _depositShares(
        PaymentPoolBase.SweepShare[] memory shares,
        uint256 sum,
        uint256 minted,
        uint32 sourceDomain,
        address pool
    ) internal {
        uint256 n = shares.length;
        uint256 left = minted;
        for (uint256 i = 0; i < n; i++) {
            PaymentPoolBase.SweepShare memory s = shares[i];
            uint256 share = i + 1 == n ? left : Math.mulDiv(minted, s.amount, sum);
            left -= share;
            // A zero test on a measured amount, not a balance comparison.
            // slither-disable-next-line incorrect-equality
            if (share == 0) continue;
            emit SweepReceived(sourceDomain, pool, s.operator, share);
            // Each deposit is one operator's credit: a batch is a loop of them.
            // slither-disable-next-line calls-loop
            feeRouter.deposit(s.operator, s.servedBytes, share);
        }
    }

    function _toBytes32(address a) internal pure returns (bytes32) {
        return bytes32(uint256(uint160(a)));
    }
}
