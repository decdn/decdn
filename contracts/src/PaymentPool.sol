// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { SunsettingPausable } from "./SunsettingPausable.sol";
import { EIP712 } from "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

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
}
