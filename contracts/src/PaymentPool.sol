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

import { IFeeRouterSettlement } from "./interfaces/IFeeRouterSettlement.sol";
import { ICapacityBondActivity } from "./interfaces/ICapacityBondActivity.sol";

/// @title PaymentPool
/// @notice One funded USDC pool backs payments from many independent capped
///         signers to many nodes (ADR 003 § PaymentPool). An owner opens a
///         single pool and pays every node from it, never opening a pool per
///         node or per client. Signers are authorized off-chain by an
///         owner-signed capability, capped by a spending limit and an
///         expiry, and registered lazily on first redemption into a
///         two-dimensional sharded register keyed by `(signer, provider)`.
///         Redemption never skims a fee inline: the routed amount is
///         forwarded to `FeeRouter.routeSettlement` in the same transaction,
///         which performs the three-bucket split and stamps paid-proportional
///         served bytes for governance vote weight (ADR 026 / ADR 036).
/// @dev    OZ bases per ADR 016 § OpenZeppelin Framework Usage:
///         `AccessControl` (governance-gated setters + emergency pause role,
///         handed to the `TimelockController` post-deploy), `ReentrancyGuard`
///         + `Pausable` (fund custody), `EIP712` (capability and voucher
///         signing domain per ADR 024). The
///         USDC address is immutable; the `FeeRouter` target is
///         governance-re-pointable per ADR 016 § No proxy deployment
///         patterns (the capability/voucher domain separator hashes this
///         contract's address, never the router, so re-pointing invalidates
///         no signatures).
contract PaymentPool is AccessControl, ReentrancyGuard, SunsettingPausable, EIP712 {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    // -----------------------------------------------------------------
    // Pool status
    // -----------------------------------------------------------------

    /// @dev Lifecycle: `Open → Closing → Closed`. `Closed` is terminal. The
    ///      zero default (`Open`) is harmless since a never-opened pool has
    ///      `owner == address(0)` and every entry point gates on the caller
    ///      being the recorded owner.
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

    /// @dev Mirrors `decdn_protocol::MAX_RATE_PER_MB` (ADR 005 §Wire protocol),
    ///      the largest `rate_per_mb` the wire schema will decode. A floor
    ///      above it is network-isolating: no advertised delivery rate could
    ///      ever clear it, so redemption would always revert.
    uint256 internal constant MAX_RATE_PER_MB = 1_000_000_000_000;

    /// @dev Lower bound of the governable per-MB delivery-rate floor.
    uint256 internal constant MIN_RATE_FLOOR = 1;

    /// @dev 1 MB in bytes (binary MB, ADR 005 / `rate::BYTES_PER_MB`).
    uint256 internal constant BYTES_PER_MB = 1_048_576;

    // -----------------------------------------------------------------
    // EIP-712 typing (ADR 003 § EIP-712 Voucher Signature)
    // -----------------------------------------------------------------

    /// @dev Owner-signed, verified once per signer on first redemption. Names
    ///      no provider — a capability is node-agnostic, valid at every node.
    bytes32 public constant CAPABILITY_TYPEHASH =
        keccak256("Capability(address signer,uint256 spendingCap,bytes32 poolId,uint64 expiry)");

    /// @dev Signer-signed, node-addressed. `signer` binds the voucher to the
    ///      authorized key `redeem` validates against; `provider` binds it to
    ///      a single payee, so one node cannot redeem another node's voucher.
    bytes32 public constant VOUCHER_TYPEHASH =
        keccak256("Voucher(bytes32 poolId,address signer,address provider,uint256 amount,uint256 bytesDelivered)");

    /// @notice The EIP-712 domain separator this contract's capabilities and
    ///         vouchers are signed against (ADR 003 § EIP-712 Voucher
    ///         Signature). Captured once, after the inherited `EIP712` base
    ///         has set its own name/version hashes.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    bytes32 public immutable DOMAIN_SEPARATOR;

    // -----------------------------------------------------------------
    // Immutables + governable state
    // -----------------------------------------------------------------

    /// @notice The settlement token (USDC), fixed at deployment (6 decimals).
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IERC20 public immutable usdc;

    /// @notice Operator registry. A pool names no provider and runs no
    ///         `isActive` gate at `openPool` (ADR 016 line 670); kept as a
    ///         validated non-zero immutable for the governance surface later
    ///         tasks build on the same registry.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ICapacityBondActivity public immutable capacityBond;

    /// @notice Settlement router target; governance-re-pointable via `setFeeRouter`.
    address public feeRouter;

    /// @notice Grace window in seconds (default 48h; bounded [48h, 72h]).
    uint256 public disputeWindow;

    /// @dev Per-MB delivery-rate floor in USDC base units. There is no
    ///      governance ceiling: a seller self-clamping its own advertised
    ///      rate downward buys no on-chain safety. The floor is itself
    ///      capped at `MAX_RATE_PER_MB` so it can never exceed what the wire
    ///      schema will carry.
    uint256 internal deliveryFloor;

    /// @notice Per-owner monotonic pool counter used in `poolId` derivation.
    mapping(address owner => uint256) public ownerPoolNonce;

    struct Pool {
        address owner;
        uint64 openedAt;
        Status status;
        address token;
        uint64 disputeDeadline;
        uint256 deposit;
        uint256 totalRedeemed;
    }

    struct Authorization {
        uint256 cap;
        uint64 expiry;
        uint256 spent;
    }

    struct Lane {
        uint256 amount;
        uint256 bytesDelivered;
    }

    mapping(bytes32 poolId => Pool) internal pools;

    /// @notice Per-signer authorization, set once on first redemption for
    ///         that signer (the owner's capability is verified there).
    mapping(bytes32 poolId => mapping(address signer => Authorization)) public authorized;

    /// @notice Per-`(pool, signer, provider)` redemption lane.
    mapping(bytes32 poolId => mapping(address signer => mapping(address provider => Lane))) public watermark;

    // -----------------------------------------------------------------
    // Events (ADR 003 § Events)
    // -----------------------------------------------------------------

    event PoolOpened(bytes32 indexed poolId, address indexed owner, uint256 deposit);
    event PoolToppedUp(bytes32 indexed poolId, uint256 additionalDeposit, uint256 newDeposit);

    /// @notice A node cashed a voucher against its lane. `paid` is the USDC
    ///         routed to `FeeRouter` this call, `bytesPaid` the paid-proportional
    ///         served bytes stamped into the operator's epoch, and
    ///         `newPaidCumulative` the lane's cumulative paid amount after the
    ///         advance. A node follows this event (filtered on its own
    ///         `provider`) as the single write path for the paid side.
    event PoolRedeemed(
        bytes32 indexed poolId,
        address indexed signer,
        address indexed provider,
        uint256 paid,
        uint256 bytesPaid,
        uint256 newPaidCumulative
    );

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error FeeRouterHasNoCode(address feeRouter);
    error FeeRouterMissingPausedView(address feeRouter);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);
    error RateBoundsInvalid(uint256 deliveryFloor);
    error ZeroAmount();
    error PoolNotOpen();
    error NotPoolOwner();
    error NothingToRedeem();
    error NotProvider();
    error InvalidVoucherSignature();
    error InvalidCapabilitySignature();
    error RateFloorViolation(uint256 amount, uint256 bytesDelivered, uint256 deliveryFloor);
    error PoolClosed();

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param usdc_          Settlement token (USDC), fixed for the contract's life.
    /// @param capacityBond_  Operator registry (no gate is run against it at `openPool`).
    /// @param feeRouter_     Initial settlement router; must be a deployed contract.
    /// @param disputeWindow_ Initial grace window (seconds; bounded [48h, 72h]).
    /// @param deliveryFloor_ Per-byte price floor enforced at redemption
    ///                       (USDC base units per MB; >= 1).
    /// @param admin          `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE` holder
    ///                       (the deployer; handed to the Timelock post-deploy).
    constructor(
        IERC20 usdc_,
        ICapacityBondActivity capacityBond_,
        address feeRouter_,
        uint256 disputeWindow_,
        uint256 deliveryFloor_,
        address admin
    ) EIP712("PaymentPool", "1") {
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
        if (deliveryFloor_ < MIN_RATE_FLOOR || deliveryFloor_ > MAX_RATE_PER_MB) {
            revert RateBoundsInvalid(deliveryFloor_);
        }

        usdc = usdc_;
        capacityBond = capacityBond_;
        feeRouter = feeRouter_;
        disputeWindow = disputeWindow_;
        deliveryFloor = deliveryFloor_;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);

        DOMAIN_SEPARATOR = _domainSeparatorV4();
    }

    // -----------------------------------------------------------------
    // Pool lifecycle
    // -----------------------------------------------------------------

    /// @notice Open a USDC pool; transfers `deposit` in and derives
    ///         `poolId = keccak256(owner, ownerPoolNonce[owner])`. Names no
    ///         provider and no signer — a pool is bound to no payee at open.
    // slither-disable-next-line reentrancy-no-eth
    function openPool(uint256 deposit) external nonReentrant whenNotPaused returns (bytes32 poolId) {
        if (deposit == 0) revert ZeroAmount();

        uint256 nonce = ownerPoolNonce[msg.sender];
        poolId = keccak256(abi.encodePacked(msg.sender, nonce));
        ownerPoolNonce[msg.sender] = nonce + 1;

        Pool storage p = pools[poolId];
        p.owner = msg.sender;
        p.openedAt = uint64(block.timestamp);
        p.status = Status.Open;
        p.token = address(usdc);

        // Credit the balance actually received, not the requested amount, so
        // a fee-on-transfer USDC proxy cannot over-state this pool's share of
        // the shared pool and brick later redemptions. Balance reads are
        // staticcalls to the trusted USDC under `nonReentrant`.
        // aderyn-ignore-next-line(reentrancy-state-change)
        uint256 balanceBefore = usdc.balanceOf(address(this));
        usdc.safeTransferFrom(msg.sender, address(this), deposit);
        // aderyn-ignore-next-line(reentrancy-state-change)
        uint256 received = usdc.balanceOf(address(this)) - balanceBefore;
        // `received` is a derived balance delta; the `== 0` is a presence
        // check (a fee-on-transfer proxy that shaved the deposit to
        // nothing), not a dangerous balance equality.
        // slither-disable-next-line incorrect-equality
        if (received == 0) revert ZeroAmount();
        p.deposit = received;

        emit PoolOpened(poolId, msg.sender, received);
    }

    /// @notice Owner-only: add funds to an open pool.
    /// @dev `whenNotPaused`: pause refuses new inflows during an incident.
    // slither-disable-next-line reentrancy-no-eth
    function topUp(bytes32 poolId, uint256 additionalDeposit) external nonReentrant whenNotPaused {
        Pool storage p = pools[poolId];
        if (p.status != Status.Open) revert PoolNotOpen();
        if (msg.sender != p.owner) revert NotPoolOwner();
        if (additionalDeposit == 0) revert ZeroAmount();

        // Credit the measured delta (see `openPool`) so fee-on-transfer
        // behavior cannot over-credit the pool's deposit. Balance reads are
        // staticcalls to the trusted USDC under `nonReentrant`.
        // aderyn-ignore-next-line(reentrancy-state-change)
        uint256 balanceBefore = usdc.balanceOf(address(this));
        usdc.safeTransferFrom(msg.sender, address(this), additionalDeposit);
        // aderyn-ignore-next-line(reentrancy-state-change)
        uint256 received = usdc.balanceOf(address(this)) - balanceBefore;
        // `received` is a derived balance delta; the `== 0` is a presence
        // check (a top-up that delivered nothing), not a dangerous balance
        // equality.
        // slither-disable-next-line incorrect-equality
        if (received == 0) revert ZeroAmount();
        p.deposit += received;

        emit PoolToppedUp(poolId, received, p.deposit);
    }

    // -----------------------------------------------------------------
    // Redemption
    // -----------------------------------------------------------------

    /// @notice Pay a node against a monotone cumulative voucher while the pool
    ///         is `Open` or inside the grace window (ADR 003 § `redeem`
    ///         behavior). The payee (`provider == msg.sender`) presents the
    ///         highest voucher it holds; redemption pays the increment over the
    ///         lane watermark, bounded by the signer's remaining cap and the
    ///         pool's remaining deposit, then routes the paid USDC through
    ///         `FeeRouter.routeSettlement` in the same transaction.
    /// @param  voucherSig The signer's EIP-712 `Voucher` signature over
    ///         `{poolId, signer, provider, cumulative, bytesDelivered}`.
    /// @param  capability On a signer's first redemption, the ABI-encoded tuple
    ///         `(uint256 spendingCap, uint64 expiry, bytes ownerSig)` carrying
    ///         the owner's EIP-712 `Capability` signature and the limits it
    ///         authorizes; empty bytes for every later redemption of an
    ///         already-registered signer.
    // slither-disable-next-line reentrancy-no-eth
    function redeem(
        bytes32 poolId,
        address signer,
        address provider,
        uint256 cumulative,
        uint256 bytesDelivered,
        bytes calldata voucherSig,
        bytes calldata capability
    ) external nonReentrant {
        if (capability.length != 0) {
            (uint256 spendingCap, uint64 expiry, bytes memory ownerSig) =
                abi.decode(capability, (uint256, uint64, bytes));
            _registerCapability(poolId, signer, spendingCap, expiry, ownerSig);
        }
        uint256 paid = _redeemVoucher(poolId, signer, provider, cumulative, bytesDelivered, voucherSig);
        if (paid == 0) revert NothingToRedeem();
    }

    /// @notice One `capabilities` entry to register, mirroring `redeem`'s
    ///         decoded `(spendingCap, expiry, ownerSig)` capability tuple plus
    ///         the `poolId`/`signer` it names.
    struct CapabilityReg {
        bytes32 poolId;
        address signer;
        uint256 spendingCap;
        uint64 expiry;
        bytes ownerSig;
    }

    /// @notice One `vouchers` entry to redeem, mirroring `redeem`'s voucher
    ///         parameters plus the `poolId` it names.
    struct RedeemVoucher {
        bytes32 poolId;
        address signer;
        address provider;
        uint256 cumulative;
        uint256 bytesDelivered;
        bytes voucherSig;
    }

    /// @notice Register every capability, then redeem every voucher, in one
    ///         transaction (ADR 003 § Batch redemption). A node registers the
    ///         signers it needs and redeems all its lanes at once. The two
    ///         loops are decoupled: registration never depends on whether any
    ///         voucher in the batch pays. `_registerCapability` is idempotent
    ///         (a duplicate or already-registered signer is a no-op) and
    ///         reverts on a bad owner signature; `_redeemVoucher` returns 0 on
    ///         every transient-empty voucher (including one whose signer is
    ///         covered by neither this call's `capabilities` nor a prior
    ///         registration), which this loop simply skips, and reverts on a
    ///         structural error (bad voucher signature, wrong provider,
    ///         closed pool, sub-floor rate) that rolls back the whole batch.
    function redeemMany(CapabilityReg[] calldata capabilities, RedeemVoucher[] calldata vouchers)
        external
        nonReentrant
        returns (uint256 totalPaid)
    {
        for (uint256 i = 0; i < capabilities.length; i++) {
            CapabilityReg calldata c = capabilities[i];
            _registerCapability(c.poolId, c.signer, c.spendingCap, c.expiry, c.ownerSig);
        }

        for (uint256 i = 0; i < vouchers.length; i++) {
            RedeemVoucher calldata v = vouchers[i];
            totalPaid += _redeemVoucher(v.poolId, v.signer, v.provider, v.cumulative, v.bytesDelivered, v.voucherSig);
        }
    }

    // -----------------------------------------------------------------
    // Views
    // -----------------------------------------------------------------

    function getPool(bytes32 poolId) external view returns (Pool memory) {
        return pools[poolId];
    }

    // -----------------------------------------------------------------
    // Pause control (PAUSER_ROLE — emergency multisig). Pause blocks new
    // pools/top-ups only.
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

    /// @dev Redemption relies on `FeeRouter.paused()` being callable;
    ///      probe the view once at config time so a router that does not
    ///      expose it is rejected loudly here rather than bricking a later
    ///      redemption on the missing pause view.
    function _requireRouterExposesPausedView(address router) internal view {
        (bool ok, bytes memory ret) = router.staticcall(abi.encodeCall(IFeeRouterSettlement.paused, ()));
        if (!ok || ret.length < 32) revert FeeRouterMissingPausedView(router);
        // The high-level paused() call strict-decodes a bool, which reverts
        // on a word > 1; reject such a router here so the probe truly
        // mirrors that decode.
        if (abi.decode(ret, (uint256)) > 1) revert FeeRouterMissingPausedView(router);
    }

    /// @dev Register a signer once, on its first redemption, by verifying the
    ///      owner's EIP-712 `Capability` over `{signer, spendingCap, poolId,
    ///      expiry}` against `pools[poolId].owner`. Idempotent: an
    ///      already-registered signer (`cap != 0 || expiry != 0`) returns
    ///      without re-verifying or overwriting, so a stray capability on a
    ///      later redeem cannot raise the cap or extend the expiry. Factored so
    ///      the batch `redeemMany` registers each capability before applying its
    ///      vouchers.
    function _registerCapability(
        bytes32 poolId,
        address signer,
        uint256 spendingCap,
        uint64 expiry,
        bytes memory ownerSig
    ) internal {
        Authorization storage a = authorized[poolId][signer];
        if (a.cap != 0 || a.expiry != 0) return;

        bytes32 structHash = keccak256(abi.encode(CAPABILITY_TYPEHASH, signer, spendingCap, poolId, expiry));
        bytes32 digest = _hashTypedDataV4(structHash);
        if (!SignatureChecker.isValidSignatureNow(pools[poolId].owner, digest, ownerSig)) {
            revert InvalidCapabilitySignature();
        }
        a.cap = spendingCap;
        a.expiry = expiry;
    }

    /// @dev The cumulative-`min` redemption core (ADR 003 § `redeem` behavior).
    ///      Returns the routed `paid`, or 0 on a transient-empty voucher that
    ///      writes no state: an unregistered signer, an expired capability, a
    ///      cumulative at or below the lane watermark, or a fully drained /
    ///      cap-reached lane. Reverts only on structural errors: a closed or
    ///      past-deadline pool (`PoolClosed`), a non-payee caller
    ///      (`NotProvider`), a bad voucher signature (`InvalidVoucherSignature`),
    ///      or a sub-floor delivery rate (`RateFloorViolation`). The
    ///      return-0-vs-revert split is what lets the batch `redeemMany` skip an
    ///      empty voucher without reverting the whole batch. Follows
    ///      checks-effects-interactions: the lane, `spent`, and `totalRedeemed`
    ///      advance before the `_route` external call.
    // slither-disable-next-line reentrancy-no-eth
    function _redeemVoucher(
        bytes32 poolId,
        address signer,
        address provider,
        uint256 cumulative,
        uint256 bytesDelivered,
        bytes memory voucherSig
    ) internal returns (uint256 paid) {
        Pool storage p = pools[poolId];

        // Status gate: Open, or Closing before the grace-window deadline.
        if (p.status == Status.Closed) revert PoolClosed();
        // forge-lint: disable-next-line(block-timestamp)
        if (p.status == Status.Closing && block.timestamp >= p.disputeDeadline) revert PoolClosed();

        if (provider != msg.sender) revert NotProvider();

        Authorization storage a = authorized[poolId][signer];
        // Unregistered signer: transient-empty. The single `redeem` path has
        // registered via `_registerCapability` first; the batch path skips it.
        if (a.cap == 0 && a.expiry == 0) return 0;

        _verifyVoucher(poolId, signer, provider, cumulative, bytesDelivered, voucherSig);

        // Expired capability is transient-empty (skippable in a batch); the
        // single path surfaces it as `NothingToRedeem`.
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp >= a.expiry) return 0;

        // Per-MB price floor on the cumulative claim, evaluated as a bytes
        // ceiling via `Math.mulDiv` so a `bytesDelivered` near
        // `type(uint256).max` reverts cleanly instead of arithmetic-panicking.
        // `deliveryFloor >= MIN_RATE_FLOOR (1)` keeps the divisor non-zero.
        if (bytesDelivered > Math.mulDiv(cumulative, BYTES_PER_MB, deliveryFloor)) {
            revert RateFloorViolation(cumulative, bytesDelivered, deliveryFloor);
        }

        Lane storage w = watermark[poolId][signer][provider];
        // Regression / already-paid cumulative: transient-empty.
        if (cumulative <= w.amount) return 0;

        uint256 desired = cumulative - w.amount;
        paid = Math.min(desired, Math.min(a.cap - a.spent, p.deposit - p.totalRedeemed));
        // Drained pool or cap reached: transient-empty, retriable after a top-up.
        if (paid == 0) return 0;

        // A voucher whose bytesDelivered has not advanced settles its money
        // with zero bytes credited; the byte watermark holds and recovers
        // when a later voucher advances it.
        uint256 bytesDelta = bytesDelivered > w.bytesDelivered ? bytesDelivered - w.bytesDelivered : 0;
        uint256 bytesPaid = Math.mulDiv(bytesDelta, paid, desired);

        // Effects before interactions (checks-effects-interactions).
        w.amount += paid;
        w.bytesDelivered += bytesPaid;
        a.spent += paid;
        p.totalRedeemed += paid;

        _route(provider, bytesPaid, paid);

        emit PoolRedeemed(poolId, signer, provider, paid, bytesPaid, w.amount);
    }

    /// @dev Verify an EIP-712 `Voucher` signature (EOA or ERC-1271) against
    ///      `signer` over the canonical typed data. Factored out of
    ///      `_redeemVoucher` to keep that frame within the stack limit.
    function _verifyVoucher(
        bytes32 poolId,
        address signer,
        address provider,
        uint256 cumulative,
        uint256 bytesDelivered,
        bytes memory voucherSig
    ) internal view {
        bytes32 structHash = keccak256(
            abi.encode(VOUCHER_TYPEHASH, poolId, signer, provider, cumulative, bytesDelivered)
        );
        bytes32 digest = _hashTypedDataV4(structHash);
        if (!SignatureChecker.isValidSignatureNow(signer, digest, voucherSig)) revert InvalidVoucherSignature();
    }

    /// @dev Approve then route a strictly-positive delta to `FeeRouter` in the
    ///      same transaction; the router pulls the USDC via `safeTransferFrom`,
    ///      performs the three-bucket split, and stamps `bytesDelta` into the
    ///      operator's epoch. Reads `feeRouter` live so a governance re-point
    ///      credits the new router. Resets the allowance to zero afterward: an
    ///      honest router pulls exactly `amountDelta`, but a re-pointed or buggy
    ///      one pulling less would otherwise leave a standing allowance over
    ///      this contract's USDC. Callers MUST have committed the watermark
    ///      first.
    function _route(address operator, uint256 bytesDelta, uint256 amountDelta) internal {
        usdc.forceApprove(feeRouter, amountDelta);
        IFeeRouterSettlement(feeRouter).routeSettlement(operator, bytesDelta, amountDelta);
        usdc.forceApprove(feeRouter, 0);
    }
}
