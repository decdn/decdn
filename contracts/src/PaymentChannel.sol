// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { EIP712 } from "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import { SignatureChecker } from "@openzeppelin/contracts/utils/cryptography/SignatureChecker.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { IFeeRouterSettlement } from "./interfaces/IFeeRouterSettlement.sol";
import { ICapacityBondActivity } from "./interfaces/ICapacityBondActivity.sol";

/// @title PaymentChannel
/// @notice Unidirectional, USDC-denominated off-chain payment channels settled
///         on-chain (ADR 003 § Payment Model). A client deposits USDC, signs
///         cumulative EIP-712 vouchers off-chain as bytes are delivered, and the
///         provider either redeems incrementally via `withdraw` while the channel
///         stays open or runs the `closeChannel` → dispute-window → `settleChannel`
///         lifecycle. Settlement never skims a fee inline: the routed delta is
///         forwarded to `FeeRouter.routeSettlement` in the same transaction, which
///         performs the three-bucket split and stamps served bytes for governance
///         vote weight (ADR 026 / ADR 036).
/// @dev    OZ bases per ADR 016 § OpenZeppelin Framework Usage:
///         `AccessControl` (governance-gated setters + emergency pause role,
///         handed to the `TimelockController` post-deploy — the deploy script's
///         uniform `GOVERNANCE_ROLE`/`DEFAULT_ADMIN_ROLE` handoff applies to this
///         contract like every other), `ReentrancyGuard` + `Pausable` (fund
///         custody), `EIP712` + `SignatureChecker` (EOA + ERC-1271/4337 voucher
///         signers per ADR 024). The USDC address is immutable; the `FeeRouter`
///         target is governance-re-pointable per ADR 016 § No proxy deployment
///         patterns (the voucher domain separator hashes this contract's address,
///         never the router, so re-pointing invalidates no signatures).
contract PaymentChannel is AccessControl, ReentrancyGuard, Pausable, EIP712 {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    // -----------------------------------------------------------------
    // Channel status
    // -----------------------------------------------------------------

    /// @dev Lifecycle: `Open → Closing → Closed`, plus the `Open → Closed`
    ///      shortcut via `reclaimExpired`. `Closed` is terminal. Matches the
    ///      `enum`-typed state machines in `CapacityBond`/`SlashAppeal`; the
    ///      zero default (`Open`) is harmless since a never-opened channel has
    ///      `client == address(0)` and every entry point gates on the caller
    ///      being a recorded party.
    enum Status {
        Open,
        Closing,
        Closed
    }

    // -----------------------------------------------------------------
    // Safety bounds (ADR 003 § Safety bounds, ADR 009)
    // -----------------------------------------------------------------

    uint256 internal constant DISPUTE_WINDOW_FLOOR = 12 hours;
    uint256 internal constant DISPUTE_WINDOW_CEILING = 72 hours;
    uint256 internal constant MAX_CHANNEL_DURATION_FLOOR = 7 days;
    uint256 internal constant MAX_CHANNEL_DURATION_CEILING = 365 days;
    uint256 internal constant MAX_VOUCHER_INTERVAL_FLOOR = 1;
    uint256 internal constant MAX_VOUCHER_INTERVAL_CEILING = 1024;
    uint256 internal constant MIN_DEPOSIT_FLOOR = 1;

    /// @dev Deployment defaults for the governable params ADR 016 § step 8 does
    ///      not pass as constructor args. `minDeposit` = 1 USDC (6 decimals) —
    ///      the dust floor of ADR 003 § Deposit Economics; `maxVoucherIntervalMb`
    ///      = 1 MB cadence default.
    uint256 internal constant DEFAULT_MIN_DEPOSIT = 1_000_000;
    uint256 internal constant DEFAULT_MAX_VOUCHER_INTERVAL_MB = 1;

    /// @dev Dispute time guaranteed from the moment a forced-inclusion
    ///      `disputeChannel` lands (ADR 003 § L2 sequencer censorship).
    uint256 internal constant FORCED_INCLUSION_GUARANTEE = 24 hours;

    // -----------------------------------------------------------------
    // EIP-712 voucher typing (ADR 003 § EIP-712 Voucher Signature)
    // -----------------------------------------------------------------

    bytes32 public constant VOUCHER_TYPEHASH =
        keccak256("Voucher(bytes32 channelId,uint256 amount,uint256 nonce,uint256 bytesDelivered,address token)");

    // -----------------------------------------------------------------
    // Immutables + governable state
    // -----------------------------------------------------------------

    /// @notice The settlement token (USDC), fixed at deployment (6 decimals).
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IERC20 public immutable usdc;

    /// @notice Operator registry read for the `openChannel` active-provider gate.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ICapacityBondActivity public immutable capacityBond;

    /// @notice Settlement router target; governance-re-pointable via `setFeeRouter`.
    address public feeRouter;

    /// @notice Dispute window in seconds (default 48h; bounded [12h, 72h]).
    uint256 public disputeWindow;

    /// @notice Channel lifetime in seconds (default 90d; bounded [7d, 365d]).
    uint256 public maxChannelDuration;

    /// @notice Minimum opening deposit in USDC base units (default 1 USDC).
    uint256 public minDeposit;

    /// @notice Advisory max negotiable voucher interval in MB (bounded [1, 1024]).
    uint256 public maxVoucherIntervalMb;

    /// @dev Advisory rate bounds in USDC base units; not enforced at settlement
    ///      (ADR 003 § Rate Bounds Refresh), exposed via `getRateBounds`.
    uint256 internal deliveryFloor;
    uint256 internal deliveryCeiling;

    /// @notice Per-client monotonic channel counter used in `channelId` derivation.
    mapping(address client => uint256) public clientChannelNonce;

    struct Channel {
        address client;
        address provider;
        address token;
        uint256 deposit;
        uint256 claimedAmount;
        uint256 claimedNonce;
        uint256 claimedBytes;
        uint256 withdrawnAmount;
        uint256 withdrawnBytes;
        // Packed into one slot (8+8+8+1+1 = 26 bytes): timestamps fit `uint64`
        // for ~584 billion years, matching the `SlashRecord`/`Appeal` convention.
        uint64 openedAt;
        uint64 expiresAt;
        uint64 disputeDeadline;
        Status status;
        bool extended;
    }

    mapping(bytes32 channelId => Channel) internal channels;

    // -----------------------------------------------------------------
    // Events (ADR 003 § Events)
    // -----------------------------------------------------------------

    event ChannelOpened(
        bytes32 indexed channelId, address indexed client, address indexed provider, uint256 deposit, uint256 expiresAt
    );
    event ChannelToppedUp(bytes32 indexed channelId, uint256 additionalDeposit, uint256 newDeposit);
    event ChannelWithdrawn(
        bytes32 indexed channelId,
        address indexed provider,
        uint256 withdrawnDelta,
        uint256 bytesDelta,
        uint256 newWithdrawnAmount
    );
    event ChannelCloseInitiated(
        bytes32 indexed channelId,
        address indexed initiator,
        uint256 amount,
        uint256 nonce,
        uint256 bytesDelivered,
        uint256 disputeDeadline
    );
    event ChannelDisputed(
        bytes32 indexed channelId, address indexed disputor, uint256 newAmount, uint256 newNonce, uint256 newBytes
    );
    event ChannelSettled(
        bytes32 indexed channelId,
        address indexed provider,
        uint256 routedAmount,
        uint256 bytesDelivered,
        uint256 clientRefund
    );
    event ChannelExpiredReclaimed(bytes32 indexed channelId, address indexed client, uint256 clientRefund);
    event FeeRouterUpdated(address indexed oldRouter, address indexed newRouter);
    event MinDepositUpdated(uint256 oldValue, uint256 newValue);
    event DisputeWindowUpdated(uint256 oldValue, uint256 newValue);
    event MaxVoucherIntervalUpdated(uint256 oldValue, uint256 newValue);
    event RateBoundsUpdated(uint256 newDeliveryFloor, uint256 newDeliveryCeiling);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error FeeRouterHasNoCode(address feeRouter);
    error DepositBelowMinimum(uint256 deposit, uint256 minDeposit);
    error ProviderNotActive(address provider);
    error NotChannelParty();
    error ChannelNotOpen();
    error ChannelNotClosing();
    error ChannelExpired();
    error ChannelNotExpired();
    error DisputeWindowClosed();
    error DisputeWindowActive();
    error InvalidVoucherSignature();
    error NonMonotonicNonce(uint256 nonce, uint256 claimedNonce);
    error AmountRegression(uint256 amount, uint256 claimedAmount);
    error BytesRegression(uint256 bytesDelivered, uint256 claimedBytes);
    error AmountExceedsDeposit(uint256 amount, uint256 deposit);
    error NothingToWithdraw();
    error ByteAdvanceWithoutPayment(uint256 byteDelta);
    error ZeroAmount();
    error RouterUnchanged();
    error RateBoundsInvalid(uint256 deliveryFloor, uint256 deliveryCeiling);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param usdc_               Settlement token (USDC), fixed for the contract's life.
    /// @param capacityBond_       Operator registry for the `openChannel` active gate.
    /// @param feeRouter_          Initial settlement router; must be a deployed contract.
    /// @param disputeWindow_      Initial dispute window (seconds; bounded [12h, 72h]).
    /// @param maxChannelDuration_ Initial channel lifetime (seconds; bounded [7d, 365d]).
    /// @param deliveryFloor_      Advisory rate floor (USDC base units; >= 1).
    /// @param deliveryCeiling_    Advisory rate ceiling (USDC base units; > floor).
    /// @param admin               `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE` holder
    ///                            (the deployer; handed to the Timelock post-deploy).
    constructor(
        IERC20 usdc_,
        ICapacityBondActivity capacityBond_,
        address feeRouter_,
        uint256 disputeWindow_,
        uint256 maxChannelDuration_,
        uint256 deliveryFloor_,
        uint256 deliveryCeiling_,
        address admin
    ) EIP712("PaymentChannel", "1") {
        if (
            address(usdc_) == address(0) || address(capacityBond_) == address(0) || feeRouter_ == address(0)
                || admin == address(0)
        ) {
            revert ZeroAddress();
        }
        if (feeRouter_.code.length == 0) revert FeeRouterHasNoCode(feeRouter_);
        if (disputeWindow_ < DISPUTE_WINDOW_FLOOR || disputeWindow_ > DISPUTE_WINDOW_CEILING) {
            revert ParamOutOfBounds(disputeWindow_, DISPUTE_WINDOW_FLOOR, DISPUTE_WINDOW_CEILING);
        }
        if (maxChannelDuration_ < MAX_CHANNEL_DURATION_FLOOR || maxChannelDuration_ > MAX_CHANNEL_DURATION_CEILING) {
            revert ParamOutOfBounds(maxChannelDuration_, MAX_CHANNEL_DURATION_FLOOR, MAX_CHANNEL_DURATION_CEILING);
        }
        if (deliveryFloor_ < MIN_DEPOSIT_FLOOR || deliveryCeiling_ <= deliveryFloor_) {
            revert RateBoundsInvalid(deliveryFloor_, deliveryCeiling_);
        }

        usdc = usdc_;
        capacityBond = capacityBond_;
        feeRouter = feeRouter_;
        disputeWindow = disputeWindow_;
        maxChannelDuration = maxChannelDuration_;
        deliveryFloor = deliveryFloor_;
        deliveryCeiling = deliveryCeiling_;
        minDeposit = DEFAULT_MIN_DEPOSIT;
        maxVoucherIntervalMb = DEFAULT_MAX_VOUCHER_INTERVAL_MB;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    // -----------------------------------------------------------------
    // Channel lifecycle
    // -----------------------------------------------------------------

    /// @notice Open a USDC channel against an active provider; transfers `deposit`
    ///         in and derives `channelId = keccak256(client, provider, channelNonce)`.
    /// @dev The `isActive` read precedes the channel-state writes; safe under
    ///      `nonReentrant` against the immutable, trusted `CapacityBond`.
    // slither-disable-next-line reentrancy-no-eth
    function openChannel(address provider, uint256 deposit)
        external
        nonReentrant
        whenNotPaused
        returns (bytes32 channelId)
    {
        if (provider == address(0)) revert ZeroAddress();
        if (deposit < minDeposit) revert DepositBelowMinimum(deposit, minDeposit);
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (!capacityBond.isActive(provider)) revert ProviderNotActive(provider);

        uint256 channelNonce = clientChannelNonce[msg.sender];
        channelId = keccak256(abi.encodePacked(msg.sender, provider, channelNonce));
        clientChannelNonce[msg.sender] = channelNonce + 1;

        Channel storage ch = channels[channelId];
        ch.client = msg.sender;
        ch.provider = provider;
        ch.token = address(usdc);
        ch.openedAt = uint64(block.timestamp);
        ch.expiresAt = uint64(block.timestamp + maxChannelDuration);
        ch.status = Status.Open;

        // Credit the balance actually received, not the requested amount, so a
        // future fee-on-transfer USDC proxy upgrade cannot over-state this
        // channel's share of the shared pool and brick later settlements.
        uint256 balanceBefore = usdc.balanceOf(address(this));
        usdc.safeTransferFrom(msg.sender, address(this), deposit);
        uint256 received = usdc.balanceOf(address(this)) - balanceBefore;
        if (received < minDeposit) revert DepositBelowMinimum(received, minDeposit);
        ch.deposit = received;

        emit ChannelOpened(channelId, msg.sender, provider, received, ch.expiresAt);
    }

    /// @notice Client-only: add funds to an open channel; does not extend `expiresAt`.
    /// @dev `whenNotPaused`: pause refuses new inflows during an incident, while the
    ///      existing-channel exit paths stay open (funds are never trapped).
    function topUp(bytes32 channelId, uint256 additionalDeposit) external nonReentrant whenNotPaused {
        Channel storage ch = channels[channelId];
        _requireOpenAndUnexpired(ch);
        if (msg.sender != ch.client) revert NotChannelParty();
        if (additionalDeposit == 0) revert ZeroAmount();

        // Credit the measured delta (see `openChannel`) so fee-on-transfer
        // behavior cannot over-credit the channel's deposit.
        uint256 balanceBefore = usdc.balanceOf(address(this));
        usdc.safeTransferFrom(msg.sender, address(this), additionalDeposit);
        uint256 received = usdc.balanceOf(address(this)) - balanceBefore;
        if (received == 0) revert ZeroAmount();
        ch.deposit += received;

        emit ChannelToppedUp(channelId, received, ch.deposit);
    }

    /// @notice Provider-only: redeem the accrued delta of a client-signed voucher
    ///         while the channel stays open. Routes the delta through `FeeRouter`
    ///         in the same transaction; no dispute window — a signed, monotonic
    ///         voucher has nothing to dispute (ADR 003 § Operator early withdrawal).
    /// @dev `_verifyVoucher` may staticcall an ERC-1271 client before the watermark
    ///      writes; safe under `nonReentrant` + checks-effects-interactions.
    // slither-disable-next-line reentrancy-no-eth
    function withdraw(
        bytes32 channelId,
        uint256 amount,
        uint256 nonce,
        uint256 bytesDelivered,
        bytes calldata signature
    ) external nonReentrant {
        Channel storage ch = channels[channelId];
        _requireOpenAndUnexpired(ch);
        if (msg.sender != ch.provider) revert NotChannelParty();

        _verifyVoucher(channelId, amount, nonce, bytesDelivered, ch.client, signature);
        // Strict monotonicity against the shared claim watermark (invariant 4).
        _advanceClaimWatermark(ch, amount, nonce, bytesDelivered, true);

        // Capture the routed delta against the PRE-call withdrawal watermark.
        uint256 delta = amount - ch.withdrawnAmount;
        uint256 bytesDelta = bytesDelivered - ch.withdrawnBytes;
        if (delta == 0) revert NothingToWithdraw();

        // Effects before the FeeRouter interaction (checks-effects-interactions).
        ch.withdrawnAmount = amount;
        ch.withdrawnBytes = bytesDelivered;

        _route(ch.provider, bytesDelta, delta);

        emit ChannelWithdrawn(channelId, ch.provider, delta, bytesDelta, amount);
    }

    /// @notice Client or provider: initiate close with the latest voucher; starts
    ///         the dispute window. A zero-voucher close (all-zero args + empty
    ///         signature, only while `claimedNonce == 0`) skips signature checks.
    /// @dev `_verifyVoucher` may staticcall an ERC-1271 client before the close-state
    ///      writes; safe under `nonReentrant` + checks-effects-interactions.
    // slither-disable-next-line reentrancy-no-eth
    function closeChannel(
        bytes32 channelId,
        uint256 amount,
        uint256 nonce,
        uint256 bytesDelivered,
        bytes calldata signature
    ) external nonReentrant {
        Channel storage ch = channels[channelId];
        // Must close before `expiresAt` (matches `topUp`/`withdraw`); after expiry
        // the only path is `reclaimExpired`, which forfeits the un-withdrawn claim
        // to the client per ADR 003 (close-before-expiry obligation).
        _requireOpenAndUnexpired(ch);
        if (msg.sender != ch.client && msg.sender != ch.provider) revert NotChannelParty();

        bool zeroVoucher =
            amount == 0 && nonce == 0 && bytesDelivered == 0 && signature.length == 0 && ch.claimedNonce == 0;

        if (!zeroVoucher) {
            _verifyVoucher(channelId, amount, nonce, bytesDelivered, ch.client, signature);
            // `strictNonce = false`: a party can always close at the current
            // watermark (`nonce ==`), unlike `withdraw`/`disputeChannel`.
            _advanceClaimWatermark(ch, amount, nonce, bytesDelivered, false);
            _requireBytesTrackPayment(ch);
        }

        ch.status = Status.Closing;
        ch.extended = false;
        ch.disputeDeadline = uint64(block.timestamp + disputeWindow);

        emit ChannelCloseInitiated(
            channelId, msg.sender, ch.claimedAmount, ch.claimedNonce, ch.claimedBytes, ch.disputeDeadline
        );
    }

    /// @notice Any address: submit a strictly-higher-nonce voucher during the
    ///         dispute window. A forced-inclusion submission with under
    ///         `FORCED_INCLUSION_GUARANTEE` left (once per close) extends the
    ///         deadline (ADR 003 § L2 sequencer censorship).
    /// @dev `_verifyVoucher` may staticcall an ERC-1271 client before the watermark
    ///      writes; safe under `nonReentrant` + checks-effects-interactions.
    // slither-disable-next-line reentrancy-no-eth
    function disputeChannel(
        bytes32 channelId,
        uint256 amount,
        uint256 nonce,
        uint256 bytesDelivered,
        bytes calldata signature
    ) external nonReentrant {
        Channel storage ch = channels[channelId];
        if (ch.status != Status.Closing) revert ChannelNotClosing();
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp >= ch.disputeDeadline) revert DisputeWindowClosed();

        _verifyVoucher(channelId, amount, nonce, bytesDelivered, ch.client, signature);
        _advanceClaimWatermark(ch, amount, nonce, bytesDelivered, true);
        _requireBytesTrackPayment(ch);

        if (_arrivedViaForcedInclusion() && !ch.extended) {
            // forge-lint: disable-next-line(block-timestamp)
            uint256 remaining = ch.disputeDeadline > block.timestamp ? ch.disputeDeadline - block.timestamp : 0;
            if (remaining < FORCED_INCLUSION_GUARANTEE) {
                ch.disputeDeadline = uint64(block.timestamp + FORCED_INCLUSION_GUARANTEE);
                ch.extended = true;
            }
        }

        emit ChannelDisputed(channelId, msg.sender, amount, nonce, bytesDelivered);
    }

    /// @notice Any address: after the dispute window, refund the client and route
    ///         the un-withdrawn remainder through `FeeRouter` (skipped when fully
    ///         drawn via `withdraw`).
    function settleChannel(bytes32 channelId) external nonReentrant {
        Channel storage ch = channels[channelId];
        if (ch.status != Status.Closing) revert ChannelNotClosing();
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < ch.disputeDeadline) revert DisputeWindowActive();

        uint256 settleAmount = ch.claimedAmount - ch.withdrawnAmount;
        uint256 settleBytes = ch.claimedBytes - ch.withdrawnBytes;
        uint256 clientRefund = ch.deposit - ch.claimedAmount;

        ch.status = Status.Closed;

        if (clientRefund != 0) usdc.safeTransfer(ch.client, clientRefund);
        // `settleBytes != 0` always implies `settleAmount != 0`: `closeChannel`/
        // `disputeChannel` reject a byte-only watermark advance via
        // `_requireBytesTrackPayment`, so routing only on a positive amount delta
        // never drops served-byte accounting (ADR 036 vote weight).
        if (settleAmount != 0) _route(ch.provider, settleBytes, settleAmount);

        emit ChannelSettled(channelId, ch.provider, settleAmount, settleBytes, clientRefund);
    }

    /// @notice Client or provider: refund `deposit - withdrawnAmount` to the client
    ///         on an expired channel that was never closed. No router call — any
    ///         withdrawn bytes were already counted at `withdraw` time.
    function reclaimExpired(bytes32 channelId) external nonReentrant {
        Channel storage ch = channels[channelId];
        if (ch.status != Status.Open) revert ChannelNotOpen();
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < ch.expiresAt) revert ChannelNotExpired();
        if (msg.sender != ch.client && msg.sender != ch.provider) revert NotChannelParty();

        uint256 clientRefund = ch.deposit - ch.withdrawnAmount;

        ch.status = Status.Closed;

        if (clientRefund != 0) usdc.safeTransfer(ch.client, clientRefund);

        emit ChannelExpiredReclaimed(channelId, ch.client, clientRefund);
    }

    // -----------------------------------------------------------------
    // Views
    // -----------------------------------------------------------------

    function getChannel(bytes32 channelId) external view returns (Channel memory) {
        return channels[channelId];
    }

    function getRateBounds() external view returns (uint256 floor, uint256 ceiling) {
        return (deliveryFloor, deliveryCeiling);
    }

    // -----------------------------------------------------------------
    // Governance setters (GOVERNANCE_ROLE — Timelock post-deploy)
    // -----------------------------------------------------------------

    /// @notice Re-point the settlement router. Open channels keep their vouchers
    ///         valid — the EIP-712 domain hashes this contract, not the router
    ///         (ADR 003 § Governance setter: setFeeRouter).
    function setFeeRouter(address newRouter) external onlyRole(GOVERNANCE_ROLE) {
        if (newRouter == address(0)) revert ZeroAddress();
        // Same invariant the constructor enforces: routing to an EOA would let
        // `_route` advance channel state while `routeSettlement` no-ops, desyncing
        // settlement accounting and stranding claimed USDC in the contract.
        if (newRouter.code.length == 0) revert FeeRouterHasNoCode(newRouter);
        if (newRouter == feeRouter) revert RouterUnchanged();
        address old = feeRouter;
        // Drop any standing allowance to the outgoing router so a re-point can
        // never leave it able to pull this contract's USDC after replacement.
        usdc.forceApprove(old, 0);
        feeRouter = newRouter;
        emit FeeRouterUpdated(old, newRouter);
    }

    function setMinDeposit(uint256 newMinDeposit) external onlyRole(GOVERNANCE_ROLE) {
        if (newMinDeposit < MIN_DEPOSIT_FLOOR) {
            revert ParamOutOfBounds(newMinDeposit, MIN_DEPOSIT_FLOOR, type(uint256).max);
        }
        uint256 old = minDeposit;
        minDeposit = newMinDeposit;
        emit MinDepositUpdated(old, newMinDeposit);
    }

    function setDisputeWindow(uint256 newWindow) external onlyRole(GOVERNANCE_ROLE) {
        if (newWindow < DISPUTE_WINDOW_FLOOR || newWindow > DISPUTE_WINDOW_CEILING) {
            revert ParamOutOfBounds(newWindow, DISPUTE_WINDOW_FLOOR, DISPUTE_WINDOW_CEILING);
        }
        uint256 old = disputeWindow;
        disputeWindow = newWindow;
        emit DisputeWindowUpdated(old, newWindow);
    }

    function setRateBounds(uint256 newFloor, uint256 newCeiling) external onlyRole(GOVERNANCE_ROLE) {
        if (newFloor < MIN_DEPOSIT_FLOOR || newCeiling <= newFloor) revert RateBoundsInvalid(newFloor, newCeiling);
        deliveryFloor = newFloor;
        deliveryCeiling = newCeiling;
        emit RateBoundsUpdated(newFloor, newCeiling);
    }

    function setMaxVoucherIntervalMb(uint256 newMaxMb) external onlyRole(GOVERNANCE_ROLE) {
        if (newMaxMb < MAX_VOUCHER_INTERVAL_FLOOR || newMaxMb > MAX_VOUCHER_INTERVAL_CEILING) {
            revert ParamOutOfBounds(newMaxMb, MAX_VOUCHER_INTERVAL_FLOOR, MAX_VOUCHER_INTERVAL_CEILING);
        }
        uint256 old = maxVoucherIntervalMb;
        maxVoucherIntervalMb = newMaxMb;
        emit MaxVoucherIntervalUpdated(old, newMaxMb);
    }

    // -----------------------------------------------------------------
    // Pause control (PAUSER_ROLE — emergency multisig). Pause blocks new
    // channels only; every existing-channel exit path stays callable so funds
    // are never trapped.
    // -----------------------------------------------------------------

    function pause() external onlyRole(PAUSER_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }

    // -----------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------

    function _requireOpenAndUnexpired(Channel storage ch) internal view {
        if (ch.status != Status.Open) revert ChannelNotOpen();
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp >= ch.expiresAt) revert ChannelExpired();
    }

    /// @dev Validate a voucher against the shared claim watermark and advance the
    ///      `claimed*` fields (invariants 1, 2, 4 — ADR 003 § Settlement-path).
    ///      `strictNonce` requires `nonce >` for `withdraw`/`disputeChannel`;
    ///      `closeChannel` passes `false` so a party can always close at the
    ///      current watermark (`nonce ==`), otherwise a channel with no newer
    ///      voucher would be unclosable until `expiresAt`.
    function _advanceClaimWatermark(
        Channel storage ch,
        uint256 amount,
        uint256 nonce,
        uint256 bytesDelivered,
        bool strictNonce
    ) internal {
        if (strictNonce ? nonce <= ch.claimedNonce : nonce < ch.claimedNonce) {
            revert NonMonotonicNonce(nonce, ch.claimedNonce);
        }
        if (amount < ch.claimedAmount) revert AmountRegression(amount, ch.claimedAmount);
        if (bytesDelivered < ch.claimedBytes) revert BytesRegression(bytesDelivered, ch.claimedBytes);
        if (amount > ch.deposit) revert AmountExceedsDeposit(amount, ch.deposit);

        ch.claimedAmount = amount;
        ch.claimedNonce = nonce;
        ch.claimedBytes = bytesDelivered;
    }

    /// @dev Reject a recorded close/dispute voucher that advances served bytes
    ///      without advancing the routable amount past the withdrawal watermark.
    ///      `settleChannel` forwards bytes only alongside a positive amount delta
    ///      (FeeRouter reverts on a zero-amount stamp), so such a voucher would
    ///      silently drop the served-byte accounting ADR 036 uses for governance
    ///      vote weight. `withdraw` is exempt by construction — it reverts
    ///      `NothingToWithdraw` on a zero amount delta.
    function _requireBytesTrackPayment(Channel storage ch) internal view {
        uint256 byteDelta = ch.claimedBytes - ch.withdrawnBytes;
        if (byteDelta != 0 && ch.claimedAmount == ch.withdrawnAmount) {
            revert ByteAdvanceWithoutPayment(byteDelta);
        }
    }

    /// @dev Verify a client EIP-712 voucher signature (EOA or ERC-1271) over the
    ///      canonical typed data. `token` is pinned to `usdc` — vouchers for a
    ///      different token never validate.
    function _verifyVoucher(
        bytes32 channelId,
        uint256 amount,
        uint256 nonce,
        uint256 bytesDelivered,
        address client,
        bytes calldata signature
    ) internal view {
        bytes32 structHash = keccak256(
            abi.encode(VOUCHER_TYPEHASH, channelId, amount, nonce, bytesDelivered, address(usdc))
        );
        bytes32 digest = _hashTypedDataV4(structHash);
        if (!SignatureChecker.isValidSignatureNow(client, digest, signature)) revert InvalidVoucherSignature();
    }

    /// @dev Approve then route a strictly-positive delta to `FeeRouter` in the
    ///      same transaction (the router pulls via `safeTransferFrom`). Caller
    ///      MUST have committed the watermark first (checks-effects-interactions).
    function _route(address operator, uint256 bytesDelta, uint256 amountDelta) internal {
        usdc.forceApprove(feeRouter, amountDelta);
        IFeeRouterSettlement(feeRouter).routeSettlement(operator, bytesDelta, amountDelta);
        // Zero any residue: an honest router pulls exactly `amountDelta`, but a
        // re-pointed or buggy one pulling less would otherwise leave a standing
        // allowance over this contract's USDC. Reset closes that surface.
        usdc.forceApprove(feeRouter, 0);
    }

    /// @dev L2-specific forced-inclusion detection seam. Returns `false` in this
    ///      base contract — the deadline extension is disabled and the 48h base
    ///      window is the censorship protection. The concrete L2 adapter overrides
    ///      this once the deployment L2 is finalized (ADR 003 § L2 sequencer
    ///      censorship — "Exact detection logic is finalized at L2 selection").
    ///      `virtual` keeps solc from folding the `disputeChannel` extension
    ///      branch to dead code.
    function _arrivedViaForcedInclusion() internal view virtual returns (bool) {
        return false;
    }
}
