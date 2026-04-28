// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Ownable } from "@openzeppelin/contracts/access/Ownable.sol";
import {
    ReentrancyGuardTransient
} from "@openzeppelin/contracts/utils/ReentrancyGuardTransient.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { EIP712 } from "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import { SignatureChecker } from "@openzeppelin/contracts/utils/cryptography/SignatureChecker.sol";
import { SafeCast } from "@openzeppelin/contracts/utils/math/SafeCast.sol";

import { IStakingRegistry } from "./interfaces/IStakingRegistry.sol";
import { Errors } from "./libraries/Errors.sol";

/// @title StablePaymentChannel
/// @notice USDC payment channels with EIP-712 vouchers and a dispute window.
/// @dev See ADR 003 (payment model), ADR 016 (interaction spec), ADR 024
///      (SignatureChecker over ECDSA for ERC-4337 compatibility). PoC is
///      single-token (USDC only). Vouchers are signed by the CLIENT;
///      the provider submits them at close/dispute/settle time.
///
///      Fees are computed once at settlement time on the final claimed
///      amount, applying the discounted rate if the provider holds
///      ≥ discountThreshold × stakingRegistry.minStake on the registry.
contract StablePaymentChannel is Ownable, ReentrancyGuardTransient, Pausable, EIP712 {
    using SafeERC20 for IERC20;
    using SafeCast for uint256;

    // ---------------------------------------------------------------------
    //  Safety bounds (immutable per ADR 009)
    // ---------------------------------------------------------------------

    uint256 public constant FEE_CEILING_BPS = 2000; // 20% hard cap
    uint256 public constant BPS_DENOMINATOR = 10_000;
    uint64 public constant DISPUTE_WINDOW_FLOOR = 12 hours;
    uint64 public constant DISPUTE_WINDOW_CEILING = 72 hours;
    uint64 public constant CHANNEL_DURATION_FLOOR = 7 days;
    uint64 public constant CHANNEL_DURATION_CEILING = 365 days;

    /// @dev Hardcoded safety envelope for the governable rate bounds (ADR
    ///      003 §Rate Bounds). Both values are USDC base units (6 decimals)
    ///      per MB. The ceiling envelope (10000 ≈ $0.01/MB) sits 10× above
    ///      the PoC initial ceiling and well below any rate that could
    ///      cause overflow when multiplied by realistic byte counts.
    uint256 public constant RATE_FLOOR_MIN = 1; // 1 base unit (~$0.000001/MB)
    uint256 public constant RATE_CEILING_MAX = 10_000; // ~$0.01/MB envelope

    /// @dev EIP-712 typehash for a cumulative payment voucher. Frozen —
    ///      changing any field invalidates every voucher clients have
    ///      already signed. Schema changes require a new typehash name.
    bytes32 public constant VOUCHER_TYPEHASH =
        keccak256("Voucher(bytes32 channelId,uint256 amount,uint256 nonce,address token)");

    // ---------------------------------------------------------------------
    //  Types
    // ---------------------------------------------------------------------

    enum Status {
        None,
        Open,
        Closing,
        Closed
    }

    struct Channel {
        address client;
        address provider;
        uint256 deposit;
        uint256 claimedAmount;
        uint256 claimedNonce;
        uint64 openedAt;
        uint64 expiresAt;
        uint64 disputeDeadline;
        Status status;
    }

    // ---------------------------------------------------------------------
    //  Storage
    // ---------------------------------------------------------------------

    IERC20 public immutable USDC;
    IStakingRegistry public immutable STAKING_REGISTRY;

    /// @notice Protocol treasury receiving the fee portion of each settlement.
    address public treasury;
    uint256 public feeBps; // base fee in basis points (default 300)
    uint256 public discountedFeeBps; // discounted fee (default 150)
    uint256 public discountStakeMultiple; // default 10; provider needs this many
    uint64 public disputeWindow; // seconds
    uint64 public maxChannelDuration; // seconds

    /// @notice Governable rate floor, USDC base units per MB (ADR 003).
    ///         Off-chain coordination only — not enforced at settlement;
    ///         compliant nodes refuse to advertise outside [floor, ceiling].
    uint256 public deliveryFloor;
    /// @notice Governable rate ceiling, USDC base units per MB (ADR 003).
    uint256 public deliveryCeiling;

    mapping(bytes32 channelId => Channel) internal _channels;
    mapping(address client => uint256) internal _clientChannelNonce;

    // ---------------------------------------------------------------------
    //  Events
    // ---------------------------------------------------------------------

    event ChannelOpened(
        bytes32 indexed channelId,
        address indexed client,
        address indexed provider,
        uint256 deposit,
        uint64 expiresAt
    );
    event ChannelToppedUp(bytes32 indexed channelId, uint256 added, uint256 newDeposit);
    event ChannelCloseInitiated(
        bytes32 indexed channelId,
        address indexed initiator,
        uint256 amount,
        uint256 nonce,
        uint64 disputeDeadline
    );
    event ChannelDisputed(
        bytes32 indexed channelId, address indexed disputor, uint256 newAmount, uint256 newNonce
    );
    event ChannelSettled(
        bytes32 indexed channelId, uint256 providerPayout, uint256 clientRefund, uint256 protocolFee
    );
    event ChannelExpiredReclaimed(
        bytes32 indexed channelId, address indexed client, uint256 refund
    );
    event FeeParamsUpdated(uint256 baseBps, uint256 discountedBps, uint256 stakeMultiple);
    event DisputeWindowUpdated(uint64 oldValue, uint64 newValue);
    event MaxChannelDurationUpdated(uint64 oldValue, uint64 newValue);
    event TreasuryUpdated(address indexed oldTreasury, address indexed newTreasury);
    /// @dev Emitted by `setRateBounds` so off-chain nodes refresh their
    ///      cached coordination bounds without polling (ADR 003).
    event RateBoundsUpdated(uint256 newDeliveryFloor, uint256 newDeliveryCeiling);

    // ---------------------------------------------------------------------
    //  Errors
    // ---------------------------------------------------------------------

    error ChannelNotOpen();
    error ChannelNotClosing();
    error ChannelExists();
    error UnknownChannel();
    error NotParticipant();
    error ExpiredChannel();
    error NotYetExpired();
    error DisputeWindowOpen();
    error DisputeWindowClosed();
    error NonceNotIncreasing();
    error AmountNotIncreasing();
    error AmountExceedsDeposit();
    error Overflow();

    // ---------------------------------------------------------------------
    //  Constructor
    // ---------------------------------------------------------------------

    constructor(
        IERC20 usdc,
        IStakingRegistry stakingRegistry,
        address treasury_,
        address admin,
        uint256 feeBps_,
        uint256 discountedFeeBps_,
        uint256 discountStakeMultiple_,
        uint64 disputeWindow_,
        uint64 maxChannelDuration_,
        uint256 deliveryFloor_,
        uint256 deliveryCeiling_
    ) Ownable(admin) EIP712("StablePaymentChannel", "1") {
        if (
            address(usdc) == address(0) || address(stakingRegistry) == address(0)
                || treasury_ == address(0) || admin == address(0)
        ) revert Errors.ZeroAddress();
        if (feeBps_ > FEE_CEILING_BPS || discountedFeeBps_ > feeBps_) revert Errors.OutOfBounds();
        if (discountStakeMultiple_ == 0) revert Errors.OutOfBounds();
        if (disputeWindow_ < DISPUTE_WINDOW_FLOOR || disputeWindow_ > DISPUTE_WINDOW_CEILING) {
            revert Errors.OutOfBounds();
        }
        if (
            maxChannelDuration_ < CHANNEL_DURATION_FLOOR
                || maxChannelDuration_ > CHANNEL_DURATION_CEILING
        ) {
            revert Errors.OutOfBounds();
        }
        if (
            deliveryFloor_ < RATE_FLOOR_MIN || deliveryCeiling_ > RATE_CEILING_MAX
                || deliveryFloor_ > deliveryCeiling_
        ) {
            revert Errors.OutOfBounds();
        }

        USDC = usdc;
        STAKING_REGISTRY = stakingRegistry;
        treasury = treasury_;
        feeBps = feeBps_;
        discountedFeeBps = discountedFeeBps_;
        discountStakeMultiple = discountStakeMultiple_;
        disputeWindow = disputeWindow_;
        maxChannelDuration = maxChannelDuration_;
        deliveryFloor = deliveryFloor_;
        deliveryCeiling = deliveryCeiling_;
    }

    // ---------------------------------------------------------------------
    //  Channel lifecycle
    // ---------------------------------------------------------------------

    function openChannel(
        address provider,
        uint256 deposit
    ) external nonReentrant whenNotPaused returns (bytes32 channelId) {
        if (provider == address(0)) revert Errors.ZeroAddress();
        if (deposit == 0) revert Errors.ZeroAmount();

        uint256 nonce = _clientChannelNonce[msg.sender];
        channelId = keccak256(abi.encodePacked(msg.sender, provider, nonce));
        if (_channels[channelId].status != Status.None) revert ChannelExists();

        unchecked {
            _clientChannelNonce[msg.sender] = nonce + 1;
        }

        uint64 expiresAt = block.timestamp.toUint64() + maxChannelDuration;
        _channels[channelId] = Channel({
            client: msg.sender,
            provider: provider,
            deposit: deposit,
            claimedAmount: 0,
            claimedNonce: 0,
            openedAt: block.timestamp.toUint64(),
            expiresAt: expiresAt,
            disputeDeadline: 0,
            status: Status.Open
        });

        emit ChannelOpened(channelId, msg.sender, provider, deposit, expiresAt);
        USDC.safeTransferFrom(msg.sender, address(this), deposit);
    }

    function topUp(
        bytes32 channelId,
        uint256 amount
    ) external nonReentrant whenNotPaused {
        Channel storage c = _channels[channelId];
        if (c.status != Status.Open) revert ChannelNotOpen();
        if (msg.sender != c.client) revert NotParticipant();
        if (block.timestamp >= c.expiresAt) revert ExpiredChannel();
        if (amount == 0) revert Errors.ZeroAmount();

        c.deposit += amount;
        emit ChannelToppedUp(channelId, amount, c.deposit);
        USDC.safeTransferFrom(msg.sender, address(this), amount);
    }

    /// @notice Start the dispute window with an initial voucher (or zero from
    /// the provider if no bytes were delivered yet).
    function closeChannel(
        bytes32 channelId,
        uint256 amount,
        uint256 nonce,
        bytes calldata signature
    ) external nonReentrant whenNotPaused {
        Channel storage c = _channels[channelId];
        if (c.status != Status.Open) revert ChannelNotOpen();
        if (msg.sender != c.client && msg.sender != c.provider) revert NotParticipant();

        _applyVoucher(
            c,
            channelId,
            amount,
            nonce,
            signature,
            /* allowZero */
            msg.sender == c.provider
        );

        c.status = Status.Closing;
        uint64 deadline = block.timestamp.toUint64() + disputeWindow;
        c.disputeDeadline = deadline;
        emit ChannelCloseInitiated(channelId, msg.sender, c.claimedAmount, c.claimedNonce, deadline);
    }

    /// @notice Supply a higher-nonce voucher during the dispute window.
    function disputeChannel(
        bytes32 channelId,
        uint256 amount,
        uint256 nonce,
        bytes calldata signature
    ) external nonReentrant whenNotPaused {
        Channel storage c = _channels[channelId];
        if (c.status != Status.Closing) revert ChannelNotClosing();
        if (block.timestamp >= c.disputeDeadline) revert DisputeWindowClosed();

        _applyVoucher(
            c,
            channelId,
            amount,
            nonce,
            signature,
            /* allowZero */
            false
        );
        emit ChannelDisputed(channelId, msg.sender, c.claimedAmount, c.claimedNonce);
    }

    /// @notice After the dispute window, distribute funds per the final voucher.
    function settleChannel(
        bytes32 channelId
    ) external nonReentrant whenNotPaused {
        Channel storage c = _channels[channelId];
        if (c.status != Status.Closing) revert ChannelNotClosing();
        if (block.timestamp < c.disputeDeadline) revert DisputeWindowOpen();

        uint256 claimed = c.claimedAmount;
        uint256 deposit = c.deposit;
        address provider = c.provider;
        uint256 fee = (claimed * _effectiveFeeBps(provider)) / BPS_DENOMINATOR;
        uint256 providerPayout = claimed - fee;
        uint256 clientRefund = deposit - claimed;

        c.status = Status.Closed;

        emit ChannelSettled(channelId, providerPayout, clientRefund, fee);

        // Stamp the registry so off-chain clients can rank cold-start
        // candidates by recent delivery activity (ADR 016 §3 Off-Chain Read
        // API). Only on settlements that actually paid the provider —
        // zero-claim channels carry no liveness signal. The registry must
        // hold `SETTLEMENT_REPORTER_ROLE` for this caller; the call reverts
        // if the deploy script never granted it (fail-fast).
        if (providerPayout > 0) STAKING_REGISTRY.recordSettlement(provider);

        if (providerPayout > 0) USDC.safeTransfer(provider, providerPayout);
        if (fee > 0) USDC.safeTransfer(treasury, fee);
        if (clientRefund > 0) USDC.safeTransfer(c.client, clientRefund);
    }

    /// @notice After a channel's `expiresAt` passes without a close, either
    /// party can refund the full deposit to the client.
    function reclaimExpired(
        bytes32 channelId
    ) external nonReentrant whenNotPaused {
        Channel storage c = _channels[channelId];
        if (c.status != Status.Open) revert ChannelNotOpen();
        if (block.timestamp < c.expiresAt) revert NotYetExpired();
        if (msg.sender != c.client && msg.sender != c.provider) revert NotParticipant();

        uint256 refund = c.deposit;
        c.status = Status.Closed;

        emit ChannelExpiredReclaimed(channelId, c.client, refund);
        if (refund > 0) USDC.safeTransfer(c.client, refund);
    }

    // ---------------------------------------------------------------------
    //  Governance
    // ---------------------------------------------------------------------

    function setFeeParams(
        uint256 newFeeBps,
        uint256 newDiscountedFeeBps,
        uint256 newDiscountStakeMultiple
    ) external onlyOwner {
        if (newFeeBps > FEE_CEILING_BPS || newDiscountedFeeBps > newFeeBps) {
            revert Errors.OutOfBounds();
        }
        if (newDiscountStakeMultiple == 0) revert Errors.OutOfBounds();
        feeBps = newFeeBps;
        discountedFeeBps = newDiscountedFeeBps;
        discountStakeMultiple = newDiscountStakeMultiple;
        emit FeeParamsUpdated(newFeeBps, newDiscountedFeeBps, newDiscountStakeMultiple);
    }

    function setDisputeWindow(
        uint64 newWindow
    ) external onlyOwner {
        if (newWindow < DISPUTE_WINDOW_FLOOR || newWindow > DISPUTE_WINDOW_CEILING) {
            revert Errors.OutOfBounds();
        }
        emit DisputeWindowUpdated(disputeWindow, newWindow);
        disputeWindow = newWindow;
    }

    function setMaxChannelDuration(
        uint64 newDuration
    ) external onlyOwner {
        if (newDuration < CHANNEL_DURATION_FLOOR || newDuration > CHANNEL_DURATION_CEILING) {
            revert Errors.OutOfBounds();
        }
        emit MaxChannelDurationUpdated(maxChannelDuration, newDuration);
        maxChannelDuration = newDuration;
    }

    function setTreasury(
        address newTreasury
    ) external onlyOwner {
        if (newTreasury == address(0)) revert Errors.ZeroAddress();
        emit TreasuryUpdated(treasury, newTreasury);
        treasury = newTreasury;
    }

    /// @notice Update the rate floor/ceiling. Coordination-only — nodes
    /// that follow the spec refuse to advertise outside the bounds, but
    /// the contract does not enforce rate compliance during settlement
    /// or slashing (ADR 003 §Rate Bounds).
    function setRateBounds(
        uint256 newDeliveryFloor,
        uint256 newDeliveryCeiling
    ) external onlyOwner {
        if (
            newDeliveryFloor < RATE_FLOOR_MIN || newDeliveryCeiling > RATE_CEILING_MAX
                || newDeliveryFloor > newDeliveryCeiling
        ) {
            revert Errors.OutOfBounds();
        }
        deliveryFloor = newDeliveryFloor;
        deliveryCeiling = newDeliveryCeiling;
        emit RateBoundsUpdated(newDeliveryFloor, newDeliveryCeiling);
    }

    /// @notice Read both rate bounds in a single call so off-chain nodes
    /// can refresh their cache atomically (ADR 003 §Rate Bounds Refresh).
    function getRateBounds() external view returns (uint256, uint256) {
        return (deliveryFloor, deliveryCeiling);
    }

    function pause() external onlyOwner {
        _pause();
    }

    function unpause() external onlyOwner {
        _unpause();
    }

    // ---------------------------------------------------------------------
    //  Views
    // ---------------------------------------------------------------------

    function getChannel(
        bytes32 channelId
    ) external view returns (Channel memory) {
        return _channels[channelId];
    }

    /// @notice Narrow view for `IStablePaymentChannel` — returns the client
    /// address bound to `channelId`, or `address(0)` if the channel was
    /// never opened. Used by SlashJudge to authorize corruption challenges.
    function channelClient(
        bytes32 channelId
    ) external view returns (address) {
        return _channels[channelId].client;
    }

    function nextChannelId(
        address client,
        address provider
    ) external view returns (bytes32) {
        return keccak256(abi.encodePacked(client, provider, _clientChannelNonce[client]));
    }

    function clientChannelNonce(
        address client
    ) external view returns (uint256) {
        return _clientChannelNonce[client];
    }

    function effectiveFeeBps(
        address provider
    ) external view returns (uint256) {
        return _effectiveFeeBps(provider);
    }

    function domainSeparator() external view returns (bytes32) {
        return _domainSeparatorV4();
    }

    // ---------------------------------------------------------------------
    //  Internals
    // ---------------------------------------------------------------------

    function _applyVoucher(
        Channel storage c,
        bytes32 channelId,
        uint256 amount,
        uint256 nonce,
        bytes calldata signature,
        bool allowZero
    ) internal {
        // Provider may open the close flow with a zero voucher when the
        // channel never transferred any bytes.
        if (amount == 0 && nonce == 0 && signature.length == 0) {
            if (!allowZero) revert Errors.InvalidSignature();
            if (c.claimedNonce != 0) revert NonceNotIncreasing();
            return;
        }

        if (nonce <= c.claimedNonce) revert NonceNotIncreasing();
        if (amount < c.claimedAmount) revert AmountNotIncreasing();
        if (amount > c.deposit) revert AmountExceedsDeposit();

        bytes32 structHash =
            keccak256(abi.encode(VOUCHER_TYPEHASH, channelId, amount, nonce, address(USDC)));
        bytes32 digest = _hashTypedDataV4(structHash);
        if (!SignatureChecker.isValidSignatureNow(c.client, digest, signature)) {
            revert Errors.InvalidSignature();
        }

        c.claimedAmount = amount;
        c.claimedNonce = nonce;
    }

    function _effectiveFeeBps(
        address provider
    ) internal view returns (uint256) {
        if (STAKING_REGISTRY.getStakeMultiple(provider) >= discountStakeMultiple) {
            return discountedFeeBps;
        }
        return feeBps;
    }
}
