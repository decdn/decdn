// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { SunsettingPausable } from "./SunsettingPausable.sol";
import { EIP712 } from "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import { SignatureChecker } from "@openzeppelin/contracts/utils/cryptography/SignatureChecker.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { Math } from "@openzeppelin/contracts/utils/math/Math.sol";
import { EnumerableSet } from "@openzeppelin/contracts/utils/structs/EnumerableSet.sol";

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
contract PaymentChannel is AccessControl, ReentrancyGuard, SunsettingPausable, EIP712 {
    using SafeERC20 for IERC20;
    using EnumerableSet for EnumerableSet.Bytes32Set;

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

    uint256 internal constant DISPUTE_WINDOW_FLOOR = 48 hours;
    uint256 internal constant DISPUTE_WINDOW_CEILING = 72 hours;
    uint256 internal constant MAX_CHANNEL_DURATION_FLOOR = 7 days;
    uint256 internal constant MAX_CHANNEL_DURATION_CEILING = 365 days;
    uint256 internal constant MIN_DEPOSIT_FLOOR = 1;

    /// @dev 1 MB in bytes (binary MB, ADR 005 / `rate::BYTES_PER_MB`). Used to
    ///      convert the MB-denominated `deliveryFloor` into the per-byte price
    ///      floor enforced at settlement (`_advanceClaimWatermark`).
    uint256 internal constant BYTES_PER_MB = 1_048_576;

    /// @dev Deployment default for the governable param ADR 016 § step 8 does
    ///      not pass as a constructor arg. `minDeposit` = 1 USDC (6 decimals) —
    ///      the dust floor of ADR 003 § Deposit Economics.
    uint256 internal constant DEFAULT_MIN_DEPOSIT = 1_000_000;

    // -----------------------------------------------------------------
    // EIP-712 voucher typing (ADR 003 § EIP-712 Voucher Signature)
    // -----------------------------------------------------------------

    /// @dev The typehash fixes only the voucher struct shape; cross-chain and
    ///      cross-contract replay protection comes from the EIP-712 domain
    ///      separator (chainId + this contract's address) bound in at
    ///      sign/verify time by the inherited `EIP712` base, not from the
    ///      typehash itself (ADR 003 § EIP-712 Voucher Signature).
    bytes32 public constant VOUCHER_TYPEHASH =
        keccak256("Voucher(bytes32 channelId,uint256 amount,uint256 nonce,uint256 bytesDelivered,address token)");

    /// @dev The provider's cooperative-close waiver typehash. Same field shape as
    ///      a voucher, signed by the PROVIDER (not the client) to attest the
    ///      final state and waive the dispute window (ADR 003 § Cooperative
    ///      close). Shares the domain separator with the voucher, so it is
    ///      likewise bound to this chain + contract; `channelId` (unique per
    ///      client/provider/channelNonce) pins it to one channel.
    bytes32 public constant COOPERATIVE_CLOSE_TYPEHASH = keccak256(
        "CooperativeClose(bytes32 channelId,uint256 amount,uint256 nonce,uint256 bytesDelivered,address token)"
    );

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

    /// @notice Dispute window in seconds (default 48h; bounded [48h, 72h]).
    uint256 public disputeWindow;

    /// @notice Channel lifetime in seconds (default 90d; bounded [7d, 365d]).
    uint256 public maxChannelDuration;

    /// @notice Minimum opening deposit in USDC base units (default 1 USDC).
    uint256 public minDeposit;

    /// @dev Rate bounds in USDC base units per MB, exposed via `getRateBounds`.
    ///      `deliveryFloor` is the per-byte price floor ENFORCED at settlement
    ///      (`_advanceClaimWatermark` requires `amount * BYTES_PER_MB >=
    ///      bytesDelivered * deliveryFloor`), closing the served-byte
    ///      vote-weight inflation of ADR 036 (#846). `deliveryCeiling` stays
    ///      advisory — a coordination ceiling nodes self-apply, not enforced
    ///      on-chain (ADR 003 § Rate Bounds Refresh).
    uint256 internal deliveryFloor;
    uint256 internal deliveryCeiling;

    /// @notice Per-client monotonic channel counter used in `channelId` derivation.
    mapping(address client => uint256) public clientChannelNonce;

    struct Channel {
        // Each `address` (20 bytes) shares its slot with a `uint64` timestamp (8
        // bytes) — and `client`'s slot also carries the 1-byte `Status` enum — so
        // the three addresses, three timestamps, and status pack into 3 slots
        // instead of 4. `uint64` holds timestamps for ~584 billion years, matching
        // the `SlashRecord`/`Appeal` convention.
        address client;
        uint64 openedAt;
        Status status;
        address provider;
        uint64 expiresAt;
        address token;
        uint64 disputeDeadline;
        uint256 deposit;
        uint256 claimedAmount;
        uint256 claimedNonce;
        uint256 claimedBytes;
        uint256 withdrawnAmount;
        uint256 withdrawnBytes;
    }

    mapping(bytes32 channelId => Channel) internal channels;

    /// @notice Channels whose provider settle leg was deferred because `FeeRouter`
    ///         was paused when `settleChannel` ran. The client refund and close
    ///         already happened; the un-withdrawn provider share stays in this
    ///         contract until anyone calls `flushDeferredSettlement` post-unpause.
    ///         The amount/bytes are recomputed from the (now `Closed`) `Channel`.
    /// @dev    Enumerable on-chain: a keeper can list every pending id via
    ///         `deferredSettlementCount` + `deferredSettlements(offset, limit)` and
    ///         drain them without replaying the `SettlementDeferred` event log.
    ///         Membership is also readable per-id via the `settlementDeferred` view.
    ///         Ids are added in the `settleChannel` defer branch and removed in
    ///         `flushDeferredSettlement`; the funds stay safe and permissionlessly
    ///         flushable while parked.
    EnumerableSet.Bytes32Set private _deferredSettlements;

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
    event ChannelCooperativelyClosed(
        bytes32 indexed channelId,
        address indexed provider,
        uint256 routedAmount,
        uint256 bytesDelivered,
        uint256 clientRefund
    );
    event SettlementDeferred(
        bytes32 indexed channelId, address indexed provider, uint256 settleAmount, uint256 settleBytes
    );
    event DeferredSettlementFlushed(
        bytes32 indexed channelId, address indexed provider, uint256 settleAmount, uint256 settleBytes
    );
    event FeeRouterUpdated(address indexed oldRouter, address indexed newRouter);
    event MinDepositUpdated(uint256 oldValue, uint256 newValue);
    event DisputeWindowUpdated(uint256 oldValue, uint256 newValue);
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
    error NoDeferredSettlement();
    error FeeRouterMissingPausedView(address feeRouter);
    error ChannelExpired();
    error ChannelNotExpired();
    error DisputeWindowClosed();
    error DisputeWindowActive();
    error InvalidVoucherSignature();
    error InvalidCooperativeCloseSignature();
    error NonMonotonicNonce(uint256 nonce, uint256 claimedNonce);
    error AmountRegression(uint256 amount, uint256 claimedAmount);
    error BytesRegression(uint256 bytesDelivered, uint256 claimedBytes);
    error AmountExceedsDeposit(uint256 amount, uint256 deposit);
    error RateFloorViolation(uint256 amount, uint256 bytesDelivered, uint256 deliveryFloor);
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
    /// @param disputeWindow_      Initial dispute window (seconds; bounded [48h, 72h]).
    /// @param maxChannelDuration_ Initial channel lifetime (seconds; bounded [7d, 365d]).
    /// @param deliveryFloor_      Per-byte price floor enforced at settlement
    ///                            (USDC base units per MB; >= 1).
    /// @param deliveryCeiling_    Advisory rate ceiling (USDC base units per MB; > floor).
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
        _requireRouterExposesPausedView(feeRouter_);
        if (disputeWindow_ < DISPUTE_WINDOW_FLOOR || disputeWindow_ > DISPUTE_WINDOW_CEILING) {
            revert ParamOutOfBounds(disputeWindow_, DISPUTE_WINDOW_FLOOR, DISPUTE_WINDOW_CEILING);
        }
        if (maxChannelDuration_ < MAX_CHANNEL_DURATION_FLOOR || maxChannelDuration_ > MAX_CHANNEL_DURATION_CEILING) {
            revert ParamOutOfBounds(maxChannelDuration_, MAX_CHANNEL_DURATION_FLOOR, MAX_CHANNEL_DURATION_CEILING);
        }
        if (
            deliveryFloor_ < MIN_DEPOSIT_FLOOR || deliveryCeiling_ <= deliveryFloor_
                || deliveryFloor_ > type(uint64).max || deliveryCeiling_ > type(uint64).max
        ) {
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
        // Balance reads are staticcalls to the trusted USDC under `nonReentrant`.
        // aderyn-ignore-next-line(reentrancy-state-change)
        uint256 balanceBefore = usdc.balanceOf(address(this));
        usdc.safeTransferFrom(msg.sender, address(this), deposit);
        // aderyn-ignore-next-line(reentrancy-state-change)
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
        // behavior cannot over-credit the channel's deposit. Balance reads are
        // staticcalls to the trusted USDC under `nonReentrant`.
        // aderyn-ignore-next-line(reentrancy-state-change)
        uint256 balanceBefore = usdc.balanceOf(address(this));
        usdc.safeTransferFrom(msg.sender, address(this), additionalDeposit);
        // aderyn-ignore-next-line(reentrancy-state-change)
        uint256 received = usdc.balanceOf(address(this)) - balanceBefore;
        // `received` is a derived balance delta; the `== 0` is a presence check
        // (a top-up that delivered nothing), not a dangerous balance equality.
        // slither-disable-next-line incorrect-equality
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
    /// @dev Unlike `settleChannel`, `withdraw` deliberately does NOT defer under a
    ///      paused `FeeRouter` (#890). It strands no client funds — the channel stays
    ///      `Open` — and a revert here rolls back the claim- and withdrawal-watermark
    ///      writes above, so nothing is trapped: the provider re-submits the same voucher once
    ///      the router is unpaused. The deferred-settlement tolerance exists only for
    ///      `settleChannel`, where a paused router would otherwise freeze a client
    ///      refund mid-exit (#849/#889).
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
        ch.disputeDeadline = uint64(block.timestamp + disputeWindow);

        emit ChannelCloseInitiated(
            channelId, msg.sender, ch.claimedAmount, ch.claimedNonce, ch.claimedBytes, ch.disputeDeadline
        );
    }

    /// @notice Any address: submit a strictly-higher-nonce voucher during the
    ///         dispute window. Censorship resistance comes from the baseline
    ///         window sitting above the L2 force-inclusion delay (ADR 003 § L2
    ///         sequencer censorship), not from any on-chain deadline extension.
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
        //
        // A paused `FeeRouter` must NOT freeze the exit: the client refund above
        // already left, and reverting here would trap it — the revert rolls back
        // the `Status.Closed` write above to `Closing`, where `reclaimExpired`
        // (which needs `Open`) cannot save it. Defer the provider leg — its USDC
        // stays in this contract and `flushDeferredSettlement` routes it (and the
        // served bytes) once the router is unpaused. Branch on the explicit
        // `paused()` view, not a `try/catch`, so unexpected router reverts still
        // propagate.
        if (settleAmount != 0) {
            // `paused()` is a staticcall to the trusted governance-set router under
            // `nonReentrant`; the only state set afterward is a bookkeeping bool, no
            // funds move (aderyn reentrancy-state-change FP).
            // aderyn-ignore-next-line(reentrancy-state-change)
            if (IFeeRouterSettlement(feeRouter).paused()) {
                // `add` always returns true here — a channel settles exactly once
                // (status is now `Closed`), so the id cannot already be present.
                // slither-disable-next-line unused-return
                _deferredSettlements.add(channelId);
                emit SettlementDeferred(channelId, ch.provider, settleAmount, settleBytes);
            } else {
                _route(ch.provider, settleBytes, settleAmount);
            }
        }

        emit ChannelSettled(channelId, ch.provider, settleAmount, settleBytes, clientRefund);
    }

    /// @notice Client or provider: settle immediately at a final state BOTH
    ///         parties signed, skipping the dispute window. The client's
    ///         cumulative voucher (`clientVoucherSig`) caps the amount the
    ///         provider may claim; the provider's matching `CooperativeClose`
    ///         waiver (`providerCloseSig`) attests it holds no higher voucher and
    ///         waives the window. With both signatures over the same final tuple
    ///         there is nothing left to dispute (ADR 003 § Cooperative close), so
    ///         on success the client's `deposit - amount` refund and the
    ///         provider's `amount - withdrawnAmount` settle leg both land in this
    ///         one transaction — no funds wait behind the window.
    /// @dev Open-only, mirroring `closeChannel`'s pre-expiry gate: a channel
    ///      already in the dispute window settles through `settleChannel`. The
    ///      shared `claimed*` watermark is advanced with `strictNonce = false`,
    ///      so the agreed state MAY equal the current watermark (e.g. the
    ///      provider already `withdraw`-drained to it and both parties now sign
    ///      that nonce to release the refund). A waiver below the on-chain
    ///      watermark reverts `AmountRegression`/`NonMonotonicNonce`, so the
    ///      on-chain watermark — not the off-chain signature — is the finality
    ///      anchor and a stale waiver can never under-settle. `_verifyVoucher`
    ///      and `_verifyCooperativeClose` may staticcall ERC-1271 signers before
    ///      the state writes; safe under `nonReentrant` + checks-effects-
    ///      interactions.
    /// @dev Unlike `settleChannel`, this does NOT defer under a paused
    ///      `FeeRouter` — it follows `withdraw`'s posture (#890). `settleChannel`
    ///      must defer because it acts on a channel already in `Closing`, where a
    ///      revert would strand the refund (the channel cannot return to `Open`
    ///      for `reclaimExpired`). Here the channel is `Open` and the transition
    ///      is atomic `Open → Closed`: if `_route` reverts under a paused router
    ///      the whole call rolls back, the channel stays `Open`, and nothing is
    ///      stranded — the caller retries post-unpause or falls back to the
    ///      `closeChannel` path. So no defer branch (and no deferred-settlement
    ///      bookkeeping) is warranted.
    // slither-disable-next-line reentrancy-no-eth
    function cooperativeClose(
        bytes32 channelId,
        uint256 amount,
        uint256 nonce,
        uint256 bytesDelivered,
        bytes calldata clientVoucherSig,
        bytes calldata providerCloseSig
    ) external nonReentrant {
        Channel storage ch = channels[channelId];
        _requireOpenAndUnexpired(ch);
        address clientAddr = ch.client;
        address providerAddr = ch.provider;
        if (msg.sender != clientAddr && msg.sender != providerAddr) revert NotChannelParty();

        _verifyVoucher(channelId, amount, nonce, bytesDelivered, clientAddr, clientVoucherSig);
        _verifyCooperativeClose(channelId, amount, nonce, bytesDelivered, providerAddr, providerCloseSig);
        // Non-strict nonce: the agreed final state may equal the current
        // watermark; a lower one reverts (the watermark is the finality anchor).
        _advanceClaimWatermark(ch, amount, nonce, bytesDelivered, false);
        _requireBytesTrackPayment(ch);

        // `_advanceClaimWatermark` set the watermark to the agreed tuple, so
        // `claimed*` now equal `amount`/`bytesDelivered`; reuse the stack vars
        // instead of re-reading them from storage.
        uint256 settleAmount = amount - ch.withdrawnAmount;
        uint256 settleBytes = bytesDelivered - ch.withdrawnBytes;
        uint256 clientRefund = ch.deposit - amount;

        ch.status = Status.Closed;

        if (clientRefund != 0) usdc.safeTransfer(clientAddr, clientRefund);
        // No paused-router defer (unlike `settleChannel`): a paused `_route`
        // reverts the whole atomic `Open → Closed` call, leaving the channel
        // `Open` with nothing stranded. The caller retries post-unpause or falls
        // back to `closeChannel`.
        if (settleAmount != 0) _route(providerAddr, settleBytes, settleAmount);

        emit ChannelCooperativelyClosed(channelId, providerAddr, settleAmount, settleBytes, clientRefund);
    }

    /// @notice Any address: route the provider settle leg deferred by
    ///         `settleChannel` when `FeeRouter` was paused. Safe to retry — a
    ///         still-paused router reverts the whole call and the channel stays in
    ///         the deferred set; a successful flush removes it, so any later call
    ///         reverts `NoDeferredSettlement`. The amount/bytes are recomputed from
    ///         the closed channel, which `settleChannel` froze (no `withdraw` is
    ///         possible once `Closed`), so they equal the provider share still held
    ///         here.
    function flushDeferredSettlement(bytes32 channelId) external nonReentrant {
        // `remove` clears the id and reports presence in one step (checks-effects):
        // false means it was never deferred (or already flushed). Removing before
        // the external route means a paused router reverts the whole tx and
        // restores the set entry for a later retry.
        if (!_deferredSettlements.remove(channelId)) revert NoDeferredSettlement();
        Channel storage ch = channels[channelId];

        uint256 settleAmount = ch.claimedAmount - ch.withdrawnAmount;
        uint256 settleBytes = ch.claimedBytes - ch.withdrawnBytes;

        _route(ch.provider, settleBytes, settleAmount);

        emit DeferredSettlementFlushed(channelId, ch.provider, settleAmount, settleBytes);
    }

    /// @notice True if `channelId` has a provider settle leg awaiting
    ///         `flushDeferredSettlement`. Preserves the legacy per-id read.
    function settlementDeferred(bytes32 channelId) external view returns (bool) {
        return _deferredSettlements.contains(channelId);
    }

    /// @notice Number of channels with a provider settle leg awaiting flush.
    function deferredSettlementCount() external view returns (uint256) {
        return _deferredSettlements.length();
    }

    /// @notice Paginated view of channel ids awaiting `flushDeferredSettlement`,
    ///         so a keeper can drain pending settlements without replaying the
    ///         `SettlementDeferred` event log.
    /// @dev    Order is unstable across flushes — `flushDeferredSettlement` removes
    ///         via swap-and-pop, relocating the tail element into the freed slot —
    ///         so indices are not stable across mutations. A robust keeper drain
    ///         re-reads `deferredSettlements(0, n)` and flushes the head each round
    ///         until `deferredSettlementCount()` is 0; a loop that pages forward
    ///         while flushing can skip an id the swap-and-pop moved behind the
    ///         cursor.
    function deferredSettlements(uint256 offset, uint256 limit) external view returns (bytes32[] memory page) {
        uint256 len = _deferredSettlements.length();
        if (offset >= len || limit == 0) {
            return new bytes32[](0);
        }
        // `remaining > 0` given the `offset >= len` guard above. Take `size` as the
        // smaller of `limit` and `remaining` directly — never forming `offset +
        // limit`, so a defensive `limit == type(uint256).max` clamps instead of
        // reverting on overflow. `offset + i < offset + size <= len`, so every
        // `at(offset + i)` is in bounds.
        uint256 remaining = len - offset;
        uint256 size = limit < remaining ? limit : remaining;
        page = new bytes32[](size);
        for (uint256 i = 0; i < size; i++) {
            page[i] = _deferredSettlements.at(offset + i);
        }
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
    /// @dev    Any settlement deferred before this re-point (see `settlementDeferred`)
    ///         routes through the NEW router on `flushDeferredSettlement` — its
    ///         `_route` reads `feeRouter` live — crediting the new router's epoch
    ///         for ADR-036 weight. Intentional: a re-point is the recovery path out
    ///         of a paused/broken router, and the new one is conformance-probed here.
    function setFeeRouter(address newRouter) external onlyRole(GOVERNANCE_ROLE) {
        if (newRouter == address(0)) revert ZeroAddress();
        // Same invariant the constructor enforces: routing to an EOA would let
        // `_route` advance channel state while `routeSettlement` no-ops, desyncing
        // settlement accounting and stranding claimed USDC in the contract.
        if (newRouter.code.length == 0) revert FeeRouterHasNoCode(newRouter);
        _requireRouterExposesPausedView(newRouter);
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
        // Cap both bounds at `type(uint64).max`: the daemon's rate clamp decodes
        // these as `u64` (`crates/node/src/rate_bounds.rs`), so a band the chain
        // can express but the node cannot enforce would silently strand every
        // voucher below the on-chain floor (#1383).
        if (
            newFloor < MIN_DEPOSIT_FLOOR || newCeiling <= newFloor || newFloor > type(uint64).max
                || newCeiling > type(uint64).max
        ) {
            revert RateBoundsInvalid(newFloor, newCeiling);
        }
        deliveryFloor = newFloor;
        deliveryCeiling = newCeiling;
        emit RateBoundsUpdated(newFloor, newCeiling);
    }

    // -----------------------------------------------------------------
    // Pause control (PAUSER_ROLE — emergency multisig). Pause blocks new
    // channels only; every existing-channel exit path stays callable so funds
    // are never trapped. `withdraw` is the one exit that may transiently revert
    // under a paused router by design (#890) — it strands no funds, so the
    // provider just retries post-unpause rather than deferring like
    // `settleChannel`.
    // -----------------------------------------------------------------

    function pause() external onlyRole(PAUSER_ROLE) {
        _requirePauseWindowOpen();
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
    ///
    ///      Enforces the per-byte price floor `deliveryFloor` on the cumulative
    ///      `amount / bytesDelivered` ratio (#846): without it a voucher could
    ///      stamp arbitrary served bytes for ~zero USDC, and since ADR 036
    ///      sources governance vote weight from those bytes, an operator
    ///      self-paying could mint near-free voting power. The check is the
    ///      single chokepoint for `withdraw`, `closeChannel`, and
    ///      `disputeChannel`, so all settlement entry points are bound.
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

        // Per-byte price floor: require `amount * BYTES_PER_MB >= bytesDelivered
        // * deliveryFloor`. Evaluated as a bytes ceiling via `Math.mulDiv` so a
        // malicious `bytesDelivered` near `type(uint256).max` reverts with a
        // clean `RateFloorViolation` rather than an arithmetic panic.
        // `deliveryFloor >= MIN_DEPOSIT_FLOOR (1)` makes the divisor non-zero;
        // `amount == 0` yields `maxBytes == 0`, so the zero-voucher close
        // (`amount == 0 && bytesDelivered == 0`) still passes.
        uint256 maxBytes = Math.mulDiv(amount, BYTES_PER_MB, deliveryFloor);
        if (bytesDelivered > maxBytes) revert RateFloorViolation(amount, bytesDelivered, deliveryFloor);

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

    /// @dev Verify the provider's EIP-712 cooperative-close waiver (EOA or
    ///      ERC-1271) over the agreed final tuple. Same field shape and `token`
    ///      pin as `_verifyVoucher`, but typed as `CooperativeClose` and recovered
    ///      against the provider — so a client voucher can never stand in for the
    ///      provider's waiver, nor vice versa.
    function _verifyCooperativeClose(
        bytes32 channelId,
        uint256 amount,
        uint256 nonce,
        uint256 bytesDelivered,
        address provider,
        bytes calldata signature
    ) internal view {
        bytes32 structHash = keccak256(
            abi.encode(COOPERATIVE_CLOSE_TYPEHASH, channelId, amount, nonce, bytesDelivered, address(usdc))
        );
        bytes32 digest = _hashTypedDataV4(structHash);
        if (!SignatureChecker.isValidSignatureNow(provider, digest, signature)) {
            revert InvalidCooperativeCloseSignature();
        }
    }

    /// @dev `settleChannel` relies on `FeeRouter.paused()` to defer (not revert)
    ///      the provider leg under pause, keeping the client refund unblocked.
    ///      Probe the view once at config time so a router that does not expose it
    ///      is rejected loudly here rather than bricking exits on every later
    ///      settle. This is fail-fast (it re-reverts with a clear error) — not a
    ///      hot-path silent catch, which would re-introduce the #849 fund-freeze.
    function _requireRouterExposesPausedView(address router) internal view {
        // Static-probe the selector: a router missing `paused()`, or one whose
        // fallback returns no bool-sized value, fails the success/length check.
        (bool ok, bytes memory ret) = router.staticcall(abi.encodeCall(IFeeRouterSettlement.paused, ()));
        if (!ok || ret.length < 32) revert FeeRouterMissingPausedView(router);
        // The high-level paused() call in settleChannel strict-decodes a bool,
        // which reverts on a word > 1; reject such a router here so the probe
        // truly mirrors it (excess returndata is tolerated, matching that decode).
        if (abi.decode(ret, (uint256)) > 1) revert FeeRouterMissingPausedView(router);
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
}
