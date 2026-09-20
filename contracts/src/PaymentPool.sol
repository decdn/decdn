// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { SunsettingPausable } from "./SunsettingPausable.sol";
import { EIP712 } from "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import { ECDSA } from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";
import { SignatureChecker } from "@openzeppelin/contracts/utils/cryptography/SignatureChecker.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { Math } from "@openzeppelin/contracts/utils/math/Math.sol";
import { SafeCast } from "@openzeppelin/contracts/utils/math/SafeCast.sol";

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
///         signing domain per ADR 024). The USDC address and the `FeeRouter`
///         target are both immutable, fixed at deployment per ADR 016 § No
///         proxy deployment patterns.
contract PaymentPool is AccessControl, ReentrancyGuard, SunsettingPausable, EIP712 {
    using SafeERC20 for IERC20;
    using SafeCast for uint256;

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
    ///      the largest `rate_per_mb` the wire schema will decode: 1000
    ///      µUSDC/MB (~$1/GB), a realistic ceiling ~100× market price. It caps
    ///      the governance `deliveryFloor` so the floor can never exceed the
    ///      wire price. A floor above the market clears no advertised rate, so
    ///      every voucher settles below it; `_applyVoucher` does not revert but
    ///      clamps, crediting only `claimed * BYTES_PER_MB / deliveryFloor`
    ///      bytes toward ADR-036 vote weight. A cap far above market would let
    ///      a floor near the ceiling drive that byte credit toward zero and
    ///      suppress vote-weight accrual, so the cap stays close to real prices.
    uint256 internal constant MAX_RATE_PER_MB = 1000;

    /// @dev Lower bound of the governable per-MB delivery-rate floor.
    uint256 internal constant MIN_RATE_FLOOR = 1;

    /// @dev 1 MB in bytes (binary MB, ADR 005 / `rate::BYTES_PER_MB`).
    uint256 internal constant BYTES_PER_MB = 1_048_576;

    /// @dev Ceiling of the governable minimum `openPool` deposit, in USDC
    ///      base units ($100 at 6 decimals). The ceiling keeps governance
    ///      from pricing small honest buyers out of opening a pool at all;
    ///      there is no floor — `0` keeps the knob dormant and any non-zero
    ///      deposit opens a pool.
    uint64 internal constant MIN_DEPOSIT_CEILING = 100e6;

    /// @dev The payment quantum: one chunk of delivery, in bytes. A `PayWord`
    ///      hash-chain tick pays for exactly this much (ADR 003 §Chunk
    ///      Cadence). Equal to `BYTES_PER_MB` **by identity**, which is what
    ///      makes a chunk cost exactly the advertised per-MB rate with no
    ///      rounding at any rate — so the chain introduces no second price
    ///      unit, and its payability floor is the `deliveryFloor` this
    ///      contract already enforces. Named separately from `BYTES_PER_MB`
    ///      because the two mean different things: one is a price denominator,
    ///      the other a meter resolution. A change here invalidates every
    ///      signature made against a live chain, so it is a protocol-version
    ///      change and not a knob.
    uint256 internal constant CHUNK_BYTES = BYTES_PER_MB;

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
    bytes32 public constant VOUCHER_TYPEHASH = keccak256(
        "Voucher(bytes32 poolId,address signer,address provider,uint256 amount,"
        "uint256 bytesDelivered,bytes32 chainRoot,uint256 chunkPrice)"
    );

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

    /// @notice Settlement router target, fixed at deployment. It records the
    ///         `bytesPerEpoch` vote-weight feed that `DecdnGovernor` reads, and
    ///         the Governor binds its `feeRouter` immutable too; binding both
    ///         ends at construction keeps the feed and its consumer pinned to
    ///         one router for the contract's life.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    address public immutable feeRouter;

    /// @notice Grace window in seconds (default 48h; bounded [48h, 72h]).
    uint256 public disputeWindow;

    /// @notice Minimum credited `openPool` deposit in USDC base units
    ///         (bounded [0, $100]; dormant at `0`, where any non-zero
    ///         deposit opens a pool). A Sybil-economics floor: holding N
    ///         concurrent pools locks at least `N × minDeposit`, which is
    ///         what makes a pool identity meaningfully less cheap than a
    ///         bare client identity. The deposit stays fully refundable at
    ///         close, so the floor taxes locked capital for simultaneous
    ///         pools, not a per-pool sunk cost. `topUp` is deliberately
    ///         exempt — the cost is about minting a new identity, not
    ///         adding to an existing pool.
    uint64 public minDeposit;

    /// @dev Per-MB delivery-rate floor in USDC base units. There is no
    ///      governance ceiling: a seller self-clamping its own advertised
    ///      rate downward buys no on-chain safety. The floor is itself
    ///      capped at `MAX_RATE_PER_MB` so it can never exceed what the wire
    ///      schema will carry.
    uint256 internal deliveryFloor;

    /// @notice Per-owner monotonic pool counter used in `poolId` derivation.
    mapping(address owner => uint256) public ownerPoolNonce;

    /// @dev Two slots. `owner + status + disputeDeadline` is 29 bytes, and
    ///      the two USDC counters are 16 more. Every USDC field on this
    ///      contract is `uint64` — 1.845e19 base units is ~$18.4 trillion at
    ///      USDC's 6 decimals, some four hundred times the token's entire
    ///      supply — so the width bounds nothing a real pool can reach while
    ///      keeping each struct inside one slot.
    struct Pool {
        address owner;
        Status status;
        uint64 disputeDeadline;
        uint64 deposit;
        uint64 totalRedeemed;
    }

    /// @dev One slot (192 of 256 bits). `spent <= cap` by construction, so
    ///      the accumulator cannot outgrow the width the cap was accepted at.
    struct Authorization {
        uint64 cap;
        uint64 expiry;
        uint64 spent;
    }

    /// @dev One slot (128 of 256 bits). Both fields are watermarks that only
    ///      ever advance toward a presented `uint64` cumulative, so neither
    ///      can exceed the width a voucher can carry. `bytesDelivered` tops
    ///      out at 18.4 exabytes on a single lane.
    struct Lane {
        uint64 amount;
        uint64 bytesDelivered;
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

    /// @notice One lane's outcome inside a `PoolRedeemed`.
    ///         `newPaidCumulative` is the lane's cumulative paid amount after
    ///         the advance — the value a node writes to its paid watermark,
    ///         cumulative rather than a delta so the stream is idempotent and
    ///         survives a gap. `bytesPaid` is the paid-proportional served
    ///         byte count, carried for per-lane accounting; nothing normative
    ///         reads it, since governance vote weight comes from
    ///         `FeeRouter.bytesPerEpoch`.
    struct LaneSettled {
        address signer;
        uint64 newPaidCumulative;
        uint64 bytesPaid;
    }

    /// @notice A node cashed vouchers against one pool. Emitted once per pool
    ///         group with an entry per lane that actually paid, rather than
    ///         once per lane: the log base and its topics are then paid once
    ///         for the group instead of once for every lane, which is what
    ///         keeps a lane cheap enough to redeem at a small balance.
    ///         A node follows this event, filtered on its own `provider`, as
    ///         the single write path for the paid side.
    /// @dev    The amount paid per lane is deliberately absent: a node writes
    ///         `newPaidCumulative` rather than summing deltas, so a delta is
    ///         never read and would only be a word of log data per lane. The
    ///         per-call USDC total is on `FeeRouter.Settled`.
    event PoolRedeemed(bytes32 indexed poolId, address indexed provider, LaneSettled[] lanes);

    /// @notice The owner started the grace-window close on `poolId`.
    ///         `redeem` stays callable until `disputeDeadline`.
    event PoolCloseInitiated(bytes32 indexed poolId, address indexed owner, uint256 disputeDeadline);

    /// @notice `reclaim` refunded `ownerRefund` (`deposit − totalRedeemed`)
    ///         to `owner` and the pool is now `Closed`.
    event PoolReclaimed(bytes32 indexed poolId, address indexed owner, uint256 ownerRefund);

    event DisputeWindowUpdated(uint256 oldValue, uint256 newValue);
    event RateBoundsUpdated(uint256 newDeliveryFloor);
    event MinDepositUpdated(uint64 oldValue, uint64 newValue);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error FeeRouterHasNoCode(address feeRouter);
    error FeeRouterMissingPausedView(address feeRouter);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);
    error RateBoundsInvalid(uint256 deliveryFloor);
    error ZeroAmount();
    /// @dev The credited `openPool` deposit — the received balance delta, not
    ///      the requested amount — is below the governed `minDeposit`.
    error BelowMinDeposit(uint256 received, uint256 minDeposit);
    error PoolNotOpen();
    error NotPoolOwner();
    error InvalidVoucherSignature();
    error InvalidCapabilitySignature();
    error PoolClosed();
    error PoolNotClosing();
    error GraceWindowActive();
    error LengthMismatch();
    /// @dev A submitted hash-chain preimage does not reach `chainRoot` in
    ///      `chainIndex` steps. Caller error, not transient pool state, so it
    ///      reverts the whole call rather than being skipped.
    error BadPreimage();
    /// @dev The packed `chainMeter` word has a non-zero byte in its reserved
    ///      upper span. Rejected rather than masked away, so the span stays
    ///      claimable by a later field without any voucher signed today
    ///      becoming reinterpretable.
    error ChainMeterReservedNonZero();
    /// @dev The chain-extended claim (`cumulative + chainIndex * chunkPrice`,
    ///      or its byte twin) does not fit `uint64`. Unlike every other value
    ///      here, these two are *derived* from calldata rather than read from
    ///      a signed `uint64`, so they are bounded by this check rather than
    ///      by construction (ADR 003 §PaymentPool).
    error ClaimOverflow();

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param usdc_          Settlement token (USDC), fixed for the contract's life.
    /// @param capacityBond_  Operator registry (no gate is run against it at `openPool`).
    /// @param feeRouter_     Initial settlement router; must be a deployed contract.
    /// @param disputeWindow_ Initial grace window (seconds; bounded [48h, 72h]).
    /// @param deliveryFloor_ Per-byte price floor enforced at redemption
    ///                       (USDC base units per MB; >= 1).
    /// @param minDeposit_    Minimum credited `openPool` deposit (USDC base
    ///                       units; bounded [0, $100] — `0` keeps the knob
    ///                       dormant).
    /// @param admin          `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE` holder
    ///                       (the deployer; handed to the Timelock post-deploy).
    constructor(
        IERC20 usdc_,
        ICapacityBondActivity capacityBond_,
        address feeRouter_,
        uint256 disputeWindow_,
        uint256 deliveryFloor_,
        uint64 minDeposit_,
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
        if (minDeposit_ > MIN_DEPOSIT_CEILING) {
            revert ParamOutOfBounds(minDeposit_, 0, MIN_DEPOSIT_CEILING);
        }

        usdc = usdc_;
        capacityBond = capacityBond_;
        feeRouter = feeRouter_;
        disputeWindow = disputeWindow_;
        deliveryFloor = deliveryFloor_;
        minDeposit = minDeposit_;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
        // Deploy renounces DEFAULT_ADMIN_ROLE, freezing the role table (#2028).
        // PAUSER_ROLE stays rotatable: its admin is GOVERNANCE_ROLE, so a
        // timelocked governance proposal can evict a compromised emergency
        // pauser without a redeploy. GOVERNANCE_ROLE itself keeps the frozen
        // DEFAULT_ADMIN admin, so no new governance key can ever be minted.
        _setRoleAdmin(PAUSER_ROLE, GOVERNANCE_ROLE);

        DOMAIN_SEPARATOR = _domainSeparatorV4();
    }

    // -----------------------------------------------------------------
    // Pool lifecycle
    // -----------------------------------------------------------------

    /// @notice Open a USDC pool; transfers `deposit` in and derives
    ///         `poolId = keccak256(owner, ownerPoolNonce[owner])`. Names no
    ///         provider and no signer — a pool is bound to no payee at open.
    ///         The credited amount must meet the governed `minDeposit`.
    // slither-disable-next-line reentrancy-no-eth
    function openPool(uint64 deposit) external nonReentrant whenNotPaused returns (bytes32 poolId) {
        if (deposit == 0) revert ZeroAmount();

        uint256 nonce = ownerPoolNonce[msg.sender];
        poolId = keccak256(abi.encodePacked(msg.sender, nonce));
        ownerPoolNonce[msg.sender] = nonce + 1;

        Pool storage p = pools[poolId];
        p.owner = msg.sender;
        p.status = Status.Open;

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
        // The Sybil floor is enforced on the credited amount, not the
        // requested `deposit`, so a fee-on-transfer proxy cannot open a pool
        // below it (same reason the deposit itself credits the delta).
        if (received < minDeposit) revert BelowMinDeposit(received, minDeposit);
        // `received <= deposit` for a well-behaved or fee-on-transfer token,
        // but a token that credits more than it was asked for must not silently
        // truncate the pool's deposit.
        p.deposit = received.toUint64();

        emit PoolOpened(poolId, msg.sender, received);
    }

    /// @notice Owner-only: add funds to an open pool.
    /// @dev `whenNotPaused`: pause refuses new inflows during an incident.
    // slither-disable-next-line reentrancy-no-eth
    function topUp(bytes32 poolId, uint64 additionalDeposit) external nonReentrant whenNotPaused {
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
        p.deposit += received.toUint64();

        emit PoolToppedUp(poolId, received, p.deposit);
    }

    // -----------------------------------------------------------------
    // Close and reclaim (ADR 003 § Close and reclaim lifecycle)
    // -----------------------------------------------------------------

    /// @notice Owner-only: start the grace-window close on `poolId`. Moves no
    ///         funds — `redeem` stays callable until `disputeDeadline`, so
    ///         this cannot understate a lane. Only `reclaim`, after the
    ///         window elapses, moves the residual.
    function closePool(bytes32 poolId) external {
        Pool storage p = pools[poolId];
        if (p.status != Status.Open) revert PoolNotOpen();
        if (msg.sender != p.owner) revert NotPoolOwner();

        p.status = Status.Closing;
        uint64 deadline = uint64(block.timestamp + disputeWindow);
        p.disputeDeadline = deadline;

        emit PoolCloseInitiated(poolId, p.owner, deadline);
    }

    /// @notice Callable by anyone once the grace window has elapsed:
    ///         transfers `deposit − totalRedeemed` to the owner and closes
    ///         the pool. The router is not called — every payout already
    ///         happened at each `redeem`.
    // slither-disable-next-line reentrancy-no-eth
    function reclaim(bytes32 poolId) external nonReentrant {
        Pool storage p = pools[poolId];
        if (p.status != Status.Closing) revert PoolNotClosing();
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < p.disputeDeadline) revert GraceWindowActive();

        uint64 ownerRefund = p.deposit - p.totalRedeemed;
        p.status = Status.Closed;

        if (ownerRefund != 0) usdc.safeTransfer(p.owner, ownerRefund);

        emit PoolReclaimed(poolId, p.owner, ownerRefund);
    }

    // -----------------------------------------------------------------
    // Redemption
    // -----------------------------------------------------------------

    /// @notice One signer registration inside a pool's batch: the owner's
    ///         EIP-712 `Capability` signature and the limits it authorizes.
    ///         The pool is the enclosing [`PoolBatch`].
    struct CapabilityReg {
        address signer;
        uint64 spendingCap;
        uint64 expiry;
        bytes ownerSig;
    }

    /// @notice One lane's voucher inside a pool's batch. Names neither its
    ///         pool (the enclosing [`PoolBatch`] does) nor its payee: the
    ///         voucher is redeemed for `msg.sender`, which is also what the
    ///         EIP-712 hash is rebuilt with, so a voucher signed for a
    ///         different node fails as `InvalidVoucherSignature`.
    /// @dev    The signature is the EIP-2098 compact pair `(r, vs)` rather
    ///         than a `bytes` blob, which is what makes this struct *static*:
    ///         an array of it carries no per-element offset, no length word,
    ///         and no padding. That width is the binding constraint on how small a lane
    ///         balance a node can still afford to redeem, so it is worth the
    ///         one capability it gives up — a voucher signer must be an EOA
    ///         (see `_verifyVoucher`).
    ///
    ///         The three chain words bring a lane to **8 words / 256 calldata
    ///         bytes** (ADR 003 §Voucher signatures are compact). `chainRoot`
    ///         is the chain head the signer committed, `preimage` is the
    ///         released value being redeemed, and `chainMeter` packs
    ///         `chunkPrice` and `chainIndex` into one word so the struct stays
    ///         at 8 rather than 9:
    ///
    ///         ```text
    ///          byte  0                     22 23            30 31
    ///               +------------------------+----------------+--+
    ///               |  reserved — MUST be 0  |   chunkPrice   |ci|
    ///               +------------------------+----------------+--+
    ///                  23 bytes                 8 bytes (u64)  1 byte (u8)
    ///         ```
    ///
    ///         The packing is invisible to signers: `_verifyVoucher` decodes
    ///         `chunkPrice`, widens it back to `uint256`, and rebuilds the same
    ///         EIP-712 digest, while `chainIndex` is a redemption parameter and
    ///         is not signed at all. `chainIndex` also needs no range check —
    ///         extracting it as a `uint8` caps it at 255 by construction, the
    ///         same bound the one-byte wire index carries. On the cooperative
    ///         path all three words are nearly all zero bytes (a closing
    ///         voucher carries a zero root, a zero preimage and a zero meter),
    ///         so they compress to almost nothing on an L2.
    struct LaneVoucher {
        address signer;
        uint64 cumulative;
        uint64 bytesDelivered;
        bytes32 r;
        bytes32 vs;
        bytes32 chainRoot;
        bytes32 preimage;
        uint256 chainMeter;
    }

    /// @notice Everything a node redeems against one pool. Naming the pool
    ///         once per group rather than once per entry is both the calldata
    ///         saving and what lets the pool's status gate and its
    ///         `totalRedeemed` write happen once for the whole group.
    struct PoolBatch {
        bytes32 poolId;
        CapabilityReg[] capabilities;
        LaneVoucher[] vouchers;
    }

    /// @notice The sole redemption entry point (ADR 003 § Batch redemption):
    ///         per pool, register every capability, then redeem every
    ///         voucher, all in one transaction. A node redeems every lane it
    ///         holds at once; a single lane is a one-pool batch of one.
    ///         Within a pool the two loops are decoupled: registration never
    ///         depends on whether any voucher pays. `_registerCapability` is
    ///         idempotent (a duplicate or already-registered signer is a
    ///         no-op) and reverts on a bad owner signature; `_applyVoucher`
    ///         returns `(0, 0)` on every transient-empty voucher (including
    ///         one whose signer is covered by neither this pool's
    ///         `capabilities` nor a prior registration), which the loop simply
    ///         skips, and reverts on a structural error (bad voucher
    ///         signature or closed pool) that rolls back everything. A sub-floor
    ///         delivery rate is not a structural error: the voucher settles its
    ///         `cumulative` and credits only the floor-justified byte ceiling.
    /// @dev    Grouping by pool is what makes the per-pool work per-pool: the
    ///         status gate is read once per group, and `totalRedeemed`
    ///         advances in one write at the end of it, so a pool carrying
    ///         many lanes pays for neither again per lane. `_applyVoucher`
    ///         touches no pool storage at all — it takes the group's
    ///         `remaining` and returns what it drew, so the running total is
    ///         threaded in a local rather than re-read.
    ///
    ///         The whole call settles once. Every voucher is redeemed for
    ///         `msg.sender`, so the payout across every pool collapses into a
    ///         single `_route(msg.sender, totalBytes, totalPaid)`, and every
    ///         lane watermark that a per-voucher route would have interleaved
    ///         with is committed before it. Per-lane detail stays on the wire
    ///         in each group's `PoolRedeemed`, which carries one entry per
    ///         lane that paid. Inlining the helpers
    ///         into these loops measures *slower* under the repo's legacy
    ///         codegen — the call is a jump, the flattened frame is stack
    ///         pressure — so they stay factored.
    function redeemMany(PoolBatch[] calldata batches) external nonReentrant returns (uint256 totalPaid) {
        uint256 totalBytes = 0;

        for (uint256 b = 0; b < batches.length; b++) {
            PoolBatch calldata batch = batches[b];
            bytes32 poolId = batch.poolId;
            Pool storage p = pools[poolId];

            // Status gate, once for the group: `Open`, or `Closing` before the
            // grace-window deadline. Nothing inside the group can change it —
            // the only external call in this function happens after every loop.
            if (p.status == Status.Closed) revert PoolClosed();
            // forge-lint: disable-next-line(block-timestamp)
            if (p.status == Status.Closing && block.timestamp >= p.disputeDeadline) revert PoolClosed();

            for (uint256 i = 0; i < batch.capabilities.length; i++) {
                CapabilityReg calldata c = batch.capabilities[i];
                _registerCapability(poolId, c.signer, c.spendingCap, c.expiry, c.ownerSig);
            }

            // The group's solvency bound, read once and drawn down in a local.
            // Passing `remaining` in is what keeps `_applyVoucher` free of pool
            // storage, and it stays exact: every draw this group has already
            // made is subtracted before the next one is bounded.
            uint64 remaining = p.deposit - p.totalRedeemed;
            uint64 poolPaid = 0;
            // Sized for every voucher and truncated to the ones that paid, so
            // the group allocates once and the log carries no empty entries.
            LaneSettled[] memory settled = new LaneSettled[](batch.vouchers.length);
            uint256 settledCount = 0;

            for (uint256 i = 0; i < batch.vouchers.length; i++) {
                LaneVoucher calldata v = batch.vouchers[i];
                (uint64 paid, uint64 bytesPaid, uint64 newCumulative) = _applyVoucher(poolId, v, remaining - poolPaid);
                if (paid != 0) {
                    settled[settledCount] =
                        LaneSettled({ signer: v.signer, newPaidCumulative: newCumulative, bytesPaid: bytesPaid });
                    settledCount++;
                    poolPaid += paid;
                    totalBytes += bytesPaid;
                }
            }

            if (poolPaid != 0) {
                p.totalRedeemed += poolPaid;
                totalPaid += poolPaid;
                // Shorten the array to the lanes that paid. Rewriting the
                // length word in place is the only way to hand `emit` a
                // right-sized array without copying it; `settledCount` is
                // bounded by the allocated length just above.
                // solhint-disable-next-line no-inline-assembly
                assembly ("memory-safe") {
                    mstore(settled, settledCount)
                }
                emit PoolRedeemed(poolId, msg.sender, settled);
            }
        }

        // A call that paid nothing moves nothing: the router rejects a zero
        // amount, and there is nothing to settle.
        if (totalPaid != 0) _route(msg.sender, totalBytes, totalPaid);
    }

    // -----------------------------------------------------------------
    // Views
    // -----------------------------------------------------------------

    function getPool(bytes32 poolId) external view returns (Pool memory) {
        return pools[poolId];
    }

    function getAuthorization(bytes32 poolId, address signer) external view returns (Authorization memory) {
        return authorized[poolId][signer];
    }

    /// @notice Batch companion to `getAuthorization`: one `authorized[poolId][signer]`
    ///         read per `(poolIds[i], signers[i])` pair, in input order. The redeemer
    ///         reads every lane whose registration it does not already know in one
    ///         call instead of one `eth_call` per lane.
    /// @dev    Reverts `LengthMismatch` when the two arrays differ in length. A pure
    ///         loop over the existing mapping — no storage is written and no pair is
    ///         deduplicated (the caller's lanes are already distinct).
    function getAuthorizations(bytes32[] calldata poolIds, address[] calldata signers)
        external
        view
        returns (Authorization[] memory auths)
    {
        if (poolIds.length != signers.length) revert LengthMismatch();
        auths = new Authorization[](poolIds.length);
        for (uint256 i = 0; i < poolIds.length; i++) {
            auths[i] = authorized[poolIds[i]][signers[i]];
        }
    }

    function getWatermark(bytes32 poolId, address signer, address provider) external view returns (Lane memory) {
        return watermark[poolId][signer][provider];
    }

    /// @notice Batch companion to `getWatermark`: one
    ///         `watermark[poolId][signer][provider]` read per
    ///         `(poolIds[i], signers[i], providers[i])` triple, in input order.
    ///         A redeemer reconciles a batch of planned lanes in one call
    ///         instead of one `eth_call` per lane.
    /// @dev    Reverts `LengthMismatch` when the three arrays differ in length. A pure
    ///         loop over the existing mapping — no storage is written and no triple is
    ///         deduplicated.
    function getWatermarks(bytes32[] calldata poolIds, address[] calldata signers, address[] calldata providers)
        external
        view
        returns (Lane[] memory lanes)
    {
        if (poolIds.length != signers.length || poolIds.length != providers.length) revert LengthMismatch();
        lanes = new Lane[](poolIds.length);
        for (uint256 i = 0; i < poolIds.length; i++) {
            lanes[i] = watermark[poolIds[i]][signers[i]][providers[i]];
        }
    }

    function getRateBounds() external view returns (uint256 floor) {
        return deliveryFloor;
    }

    /// @notice A page of the pool ids `owner` has opened, oldest first.
    /// @dev    Reconstructed rather than stored: a pool names no provider, so
    ///         `poolId = keccak256(owner, nonce)` is recomputed for each
    ///         `nonce` in `[offset, min(offset + limit, ownerPoolNonce[owner]))`
    ///         rather than read from a stored id array. `ownerPoolNonce`
    ///         (already public) is the count — there is no separate counter
    ///         to drift from it. Never forms `offset + limit`, so
    ///         `limit == type(uint256).max` clamps instead of overflowing.
    function getPools(address owner, uint256 offset, uint256 limit) external view returns (bytes32[] memory page) {
        uint256 len = ownerPoolNonce[owner];
        if (offset >= len || limit == 0) {
            return new bytes32[](0);
        }
        uint256 remaining = len - offset;
        uint256 size = limit < remaining ? limit : remaining;
        page = new bytes32[](size);
        for (uint256 i = 0; i < size; i++) {
            uint256 nonce = offset + i;
            page[i] = keccak256(abi.encodePacked(owner, nonce));
        }
    }

    // -----------------------------------------------------------------
    // Governance setters (GOVERNANCE_ROLE — Timelock post-deploy)
    // -----------------------------------------------------------------

    function setDisputeWindow(uint256 newWindow) external onlyRole(GOVERNANCE_ROLE) {
        if (newWindow < DISPUTE_WINDOW_FLOOR || newWindow > DISPUTE_WINDOW_CEILING) {
            revert ParamOutOfBounds(newWindow, DISPUTE_WINDOW_FLOOR, DISPUTE_WINDOW_CEILING);
        }
        uint256 old = disputeWindow;
        disputeWindow = newWindow;
        emit DisputeWindowUpdated(old, newWindow);
    }

    function setRateBounds(uint256 newFloor) external onlyRole(GOVERNANCE_ROLE) {
        // Cap at MAX_RATE_PER_MB, not `type(uint64).max`: the daemon decodes
        // the floor as `u64` (#1383), but the wire cap is far below `u64::MAX`,
        // and every value in that gap is above market and would clamp credited
        // bytes toward zero (see the constant).
        if (newFloor < MIN_RATE_FLOOR || newFloor > MAX_RATE_PER_MB) {
            revert RateBoundsInvalid(newFloor);
        }
        deliveryFloor = newFloor;
        emit RateBoundsUpdated(newFloor);
    }

    /// @notice Set the minimum credited `openPool` deposit (bounded
    ///         [0, `MIN_DEPOSIT_CEILING`]; `0` returns the knob to dormant).
    ///         Applies to subsequent `openPool` calls only; open pools and
    ///         `topUp` are unaffected.
    function setMinDeposit(uint64 newMin) external onlyRole(GOVERNANCE_ROLE) {
        if (newMin > MIN_DEPOSIT_CEILING) {
            revert ParamOutOfBounds(newMin, 0, MIN_DEPOSIT_CEILING);
        }
        uint64 old = minDeposit;
        minDeposit = newMin;
        emit MinDepositUpdated(old, newMin);
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
    ///      `redeemMany` registers each capability before applying its
    ///      vouchers.
    /// @dev  `spendingCap` is `uint64` in calldata but hashes as a full word,
    ///       which is the `uint256 spendingCap` the `Capability` typehash
    ///       names — narrowing the field changes no signature a client
    ///       produces.
    function _registerCapability(
        bytes32 poolId,
        address signer,
        uint64 spendingCap,
        uint64 expiry,
        bytes calldata ownerSig
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

    /// @dev The cumulative-`min` redemption core (ADR 003 § Redemption
    ///      behavior), applied for the payee `msg.sender`. Returns the `paid`
    ///      USDC and the paid-proportional `bytesPaid` owed to that payee, or
    ///      `(0, 0)` on a transient-empty voucher that writes no state: an
    ///      unregistered signer, an expired capability, a cumulative at or
    ///      below the lane watermark, or a fully drained / cap-reached lane.
    ///      Reverts only on a structural error: a bad voucher signature
    ///      (`InvalidVoucherSignature` — which is also how a voucher signed
    ///      for a different payee surfaces). A sub-floor delivery rate is not an
    ///      error — the voucher settles its `cumulative` and credits only the
    ///      floor-justified byte ceiling (the clamp below). The
    ///      return-0-vs-revert split is what lets `redeemMany` skip an empty
    ///      voucher without reverting the batch.
    ///
    ///      Touches no pool storage. The caller has already gated the pool's
    ///      status for the whole group and read its `remaining` (`deposit -
    ///      totalRedeemed`, less whatever earlier vouchers in the group have
    ///      already drawn), so this reads and writes only the signer's
    ///      authorization and the lane.
    ///
    ///      Pure state advance: it moves no money. The caller owes the returned
    ///      pair to `_route`, once for the whole call. Keeping the settlement
    ///      out of here is what makes checks-effects-interactions hold across a
    ///      batch: every lane, `spent`, and `totalRedeemed` advance lands
    ///      before the single external call.
    /// @param remaining The pool's still-payable balance for this voucher.
    /// @return paid The USDC drawn for this lane.
    /// @return bytesPaid The paid-proportional served bytes for this lane.
    /// @return newCumulative The lane's cumulative paid amount after the
    ///         advance, which the caller reports in `PoolRedeemed`. Returned
    ///         rather than re-read, since the caller would otherwise pay a
    ///         second load of a slot this call has just written.
    function _applyVoucher(bytes32 poolId, LaneVoucher calldata v, uint64 remaining)
        internal
        returns (uint64 paid, uint64 bytesPaid, uint64 newCumulative)
    {
        address signer = v.signer;

        Authorization storage a = authorized[poolId][signer];
        // Unregistered signer: transient-empty. The single `redeem` path has
        // registered via `_registerCapability` first; the batch path skips it.
        if (a.cap == 0 && a.expiry == 0) return (0, 0, 0);

        // Resolve the chain BEFORE the signature check, because the signature
        // is taken over the `chunkPrice` this unpacks. Both of its reverts are
        // caller error, exactly like a bad signature, so ordering them together
        // costs nothing and keeps the skip-vs-revert split clean below.
        (uint64 claimed, uint64 claimedBytes, uint256 chunkPrice) = _resolveClaim(v);

        _verifyVoucher(poolId, v, chunkPrice);

        // Expired capability is transient-empty (skippable in a batch); the
        // single path surfaces it as `NothingToRedeem`.
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp >= a.expiry) return (0, 0, 0);

        Lane storage w = watermark[poolId][signer][msg.sender];
        // Regression / already-paid claim: transient-empty. This is where a
        // superseded chain lands — a rollover voucher already folded its
        // frontier into a higher `cumulative`, so re-presenting the retired
        // one resolves at or below what the lane has been paid and is simply
        // skipped. Cumulative accounting is what makes that safe with no chain
        // state stored anywhere.
        if (claimed <= w.amount) return (0, 0, 0);

        uint64 desired = claimed - w.amount;
        paid = uint64(Math.min(desired, Math.min(a.cap - a.spent, remaining)));
        // Drained pool or cap reached: transient-empty, retriable after a top-up.
        if (paid == 0) return (0, 0, 0);

        // Soft per-MB price floor, evaluated as a bytes ceiling. The claim
        // justifies at most `claimed * BYTES_PER_MB / deliveryFloor` bytes of
        // delivery credit; a voucher priced below the floor still settles its
        // USDC, but only that many bytes are credited toward the served-bytes
        // governance weight (ADR 003 §Rate-floor enforcement; ADR 036), so
        // cheap bytes cannot inflate vote weight. `deliveryFloor >=
        // MIN_RATE_FLOOR (1)` keeps the divisor non-zero; the `min` runs in
        // `uint256` and is bounded above by the `uint64` `claimedBytes`, so
        // narrowing the ceiling back to `uint64` cannot overflow.
        //
        // The clamp reads the CHAIN-EXTENDED pair, not the signed `cumulative`
        // and `bytesDelivered`: bytes proved by preimage carry the same per-byte
        // price obligation as bytes proved by signature, and pricing only the
        // signed half would let a chain credit its 255 chunks of bytes against
        // whatever the anchor happened to cost.
        uint64 creditedBytes = uint64(Math.min(claimedBytes, Math.mulDiv(claimed, BYTES_PER_MB, deliveryFloor)));

        // A voucher whose credited bytes have not advanced settles its money
        // with zero bytes credited; the byte watermark holds and recovers
        // when a later voucher advances it (including after a floor raise, which
        // lowers the ceiling but never claws back already-credited bytes).
        uint64 bytesDelta = creditedBytes > w.bytesDelivered ? creditedBytes - w.bytesDelivered : 0;
        // `bytesPaid <= bytesDelta` (paid <= desired), so the result is a
        // `uint64` by construction.
        bytesPaid = uint64(Math.mulDiv(bytesDelta, paid, desired));

        // Effects only; the caller settles.
        w.amount += paid;
        w.bytesDelivered += bytesPaid;
        a.spent += paid;
        newCumulative = w.amount;
    }

    /// @dev Resolve a voucher's chain into the claim it authorizes, before any
    ///      of it is paid (ADR 003 §Hash-chain metering (`PayWord`)).
    ///
    ///      Three things happen here, in order:
    ///
    ///      1. **Unpack** `chainMeter` into `chunkPrice` (bytes 23..=30) and
    ///         `chainIndex` (the low byte), rejecting a non-zero reserved span.
    ///         That span is rejected rather than masked so a later field can
    ///         claim those bits without any voucher signed today becoming
    ///         reinterpretable — one comparison for a permanent option. The
    ///         index needs no bound of its own: extracting it as a `uint8` caps
    ///         it at 255 by construction, which is a stronger guarantee than a
    ///         runtime check and matches the one-byte wire index exactly.
    ///      2. **Walk** `preimage` forward `chainIndex` times and require the
    ///         result to equal `chainRoot`. The walk always starts from the
    ///         SUBMITTED value and never from a stored intermediate, which is
    ///         what lets this contract keep no chain state at all and need no
    ///         root-matching branch. Its cost is `chainIndex` keccaks — zero on
    ///         the cooperative path, and bounded at 255 in the mid-chain
    ///         abandonment case the chain exists to cover.
    ///      3. **Extend** the anchor: `claimed = cumulative + chainIndex *
    ///         chunkPrice` over `claimedBytes = bytesDelivered + chainIndex *
    ///         CHUNK_BYTES`.
    ///
    ///      There is no branch for the sealed voucher. `chainRoot = 0` at
    ///      `chainIndex = 0` with a zero preimage satisfies the same equality
    ///      with zeros and resolves to exactly `cumulative`; at any higher
    ///      index the same check would need a value that hashes to `0`, which
    ///      keccak preimage resistance makes infeasible. A real-root voucher
    ///      settles its own `cumulative` the same way, by passing the root as
    ///      its own preimage — so nothing anywhere tests `chainRoot == 0`.
    ///
    ///      Factored out of `_applyVoucher` for the same reason
    ///      `_verifyVoucher` is: this contract compiles without the IR pipeline,
    ///      and that frame is already at the stack limit.
    /// @return claimed The chain-extended cumulative USDC this voucher claims.
    /// @return claimedBytes The chain-extended cumulative bytes it claims.
    /// @return chunkPrice The decoded price, widened for the EIP-712 rebuild.
    function _resolveClaim(LaneVoucher calldata v)
        internal
        pure
        returns (uint64 claimed, uint64 claimedBytes, uint256 chunkPrice)
    {
        uint256 meter = v.chainMeter;
        if (meter >> 72 != 0) revert ChainMeterReservedNonZero();
        chunkPrice = uint256(uint64(meter >> 8));
        uint256 chainIndex = uint256(uint8(meter));

        bytes32 walked = v.preimage;
        for (uint256 i = 0; i < chainIndex; i++) {
            walked = keccak256(abi.encodePacked(walked));
        }
        if (walked != v.chainRoot) revert BadPreimage();

        // Resolved at full width and bounded by an explicit check: unlike every
        // other value here these two are DERIVED from calldata rather than read
        // from a signed `uint64`, so neither is bounded by its field width
        // (ADR 003 §PaymentPool). Everything downstream — the watermark, the
        // fee-router totals — is `uint64`, so the check has to happen before
        // the narrowing, not after it.
        uint256 wideClaimed = uint256(v.cumulative) + chainIndex * chunkPrice;
        uint256 wideBytes = uint256(v.bytesDelivered) + chainIndex * CHUNK_BYTES;
        if (wideClaimed > type(uint64).max || wideBytes > type(uint64).max) revert ClaimOverflow();

        claimed = uint64(wideClaimed);
        claimedBytes = uint64(wideBytes);
    }

    /// @dev Verify an EIP-712 `Voucher` signature against `v.signer` over the
    ///      canonical typed data, with `msg.sender` as the `provider` the
    ///      voucher must name. Factored out of `_applyVoucher` to keep that
    ///      frame within the stack limit. `cumulative` and `bytesDelivered`
    ///      are `uint64` in calldata but hash as full words, which is exactly
    ///      the `uint256 amount` / `uint256 bytesDelivered` the `Voucher`
    ///      typehash names — narrowing the fields changes no signature a
    ///      client produces.
    ///
    ///      `chunkPrice` arrives already decoded out of the packed `chainMeter`
    ///      word and is re-widened to `uint256` here, exactly as the typehash
    ///      declares, so the packing is invisible to every signer and changes
    ///      no digest. `chainIndex` is deliberately absent: it is a redemption
    ///      parameter the node chooses per submission, not something the payer
    ///      signed — which is precisely what lets one signature settle at any
    ///      depth the node can prove.
    ///
    ///      **A voucher signer is an EOA.** Recovery is a plain `ecrecover`
    ///      over the EIP-2098 compact pair, not an ERC-1271 check, so a
    ///      contract account cannot sign vouchers. That buys the static
    ///      calldata layout (see `LaneVoucher`) and skips a cold
    ///      `EXTCODESIZE` on every lane — both of which lower the smallest
    ///      lane balance a node can profitably redeem. A pool *owner* is
    ///      unaffected: `_registerCapability` still verifies through
    ///      `SignatureChecker`, so a Safe or other smart account can own a
    ///      pool and delegate to EOA voucher signers.
    function _verifyVoucher(bytes32 poolId, LaneVoucher calldata v, uint256 chunkPrice) internal view {
        bytes32 structHash = keccak256(
            abi.encode(
                VOUCHER_TYPEHASH, poolId, v.signer, msg.sender, v.cumulative, v.bytesDelivered, v.chainRoot, chunkPrice
            )
        );
        bytes32 digest = _hashTypedDataV4(structHash);
        // The dropped third return is `errorArg`, the offending value behind a
        // recovery failure. `err` alone decides the outcome here — there is no
        // per-reason branch to take, and the revert carries no detail.
        // slither-disable-next-line unused-return
        (address recovered, ECDSA.RecoverError err,) = ECDSA.tryRecover(digest, v.r, v.vs);
        // Check the error explicitly rather than trusting the recovered
        // address: a failed recovery returns `address(0)`, which would match a
        // signer registered at `address(0)`.
        if (err != ECDSA.RecoverError.NoError || recovered != v.signer) revert InvalidVoucherSignature();
    }

    /// @dev Approve then route a strictly-positive delta to `FeeRouter` in the
    ///      same transaction; the router pulls the USDC via `safeTransferFrom`,
    ///      performs the three-bucket split, and stamps `bytesDelta` into the
    ///      operator's epoch. Resets the allowance to zero afterward: an
    ///      honest router pulls exactly `amountDelta`, but a buggy one pulling
    ///      less would otherwise leave a standing allowance over this
    ///      contract's USDC. Callers MUST have committed the watermark first.
    function _route(address operator, uint256 bytesDelta, uint256 amountDelta) internal {
        usdc.forceApprove(feeRouter, amountDelta);
        IFeeRouterSettlement(feeRouter).routeSettlement(operator, bytesDelta, amountDelta);
        usdc.forceApprove(feeRouter, 0);
    }
}
