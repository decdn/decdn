// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { SunsettingPausable } from "./SunsettingPausable.sol";
import { EIP712 } from "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import { ECDSA } from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";
import { SignatureChecker } from "@openzeppelin/contracts/utils/cryptography/SignatureChecker.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { Math } from "@openzeppelin/contracts/utils/math/Math.sol";
import { SafeCast } from "@openzeppelin/contracts/utils/math/SafeCast.sol";

/// @dev Most operators one sweep carries, shared by the pool and the bridge
///      adapters. A full batch's CCTP burn body is 3364 bytes, under the
///      8192-byte `maxMessageBodySize` of the deployed V2 transmitters, and its
///      governance-chain receive stays a small fraction of a block (ADR 016 §
///      Bridge adapters).
uint256 constant SWEEP_BATCH_LIMIT = 32;

/// @title PaymentPoolBase
/// @notice The payment-chain pool shared by every bridge variant (ADR 003 §
///         PaymentPool, ADR 016 § Bridge adapters). One funded USDC pool backs
///         payments from many capped signers to many nodes. A redemption pays
///         `operatorBps` to the redeemer in the same transaction and accrues
///         the rest, with the paid-proportional served bytes, in `pending`.
///         `sweep` sends a batch of operators' `pending` to the governance
///         chain in one bridge transfer through the variant's bridge.
/// @dev    No governance, no roles, no setters: every parameter is a
///         constructor immutable. The pool reads no `CapacityBond` state. The
///         immutable `pauser` can block `openPool` and `topUp` until the
///         sunset; `redeemMany`, `sweep`, `closePool` and `reclaim` never
///         pause. A variant implements `_sweepFee` and `_sendSweep`.
abstract contract PaymentPoolBase is ReentrancyGuard, SunsettingPausable, EIP712 {
    using SafeERC20 for IERC20;
    using SafeCast for uint256;

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
    // Constants
    // -----------------------------------------------------------------

    uint256 internal constant BPS_DENOMINATOR = 10_000;

    /// @dev The payment quantum: one chunk of delivery, in bytes (1 MiB). A
    ///      `PayWord` hash-chain tick pays for exactly this much (ADR 003 §
    ///      Chunk Cadence). A change here invalidates every signature made
    ///      against a live chain, so it is a protocol-version change.
    uint256 internal constant CHUNK_BYTES = 1_048_576;

    /// @notice Most operators one `sweep` carries (`SWEEP_BATCH_LIMIT`).
    uint256 public constant MAX_SWEEP_BATCH = SWEEP_BATCH_LIMIT;

    // -----------------------------------------------------------------
    // EIP-712 typing (ADR 003 § EIP-712 Voucher Signature)
    // -----------------------------------------------------------------

    /// @dev Owner-signed, verified once per signer on first redemption. Names
    ///      no provider: a capability is valid at every node.
    bytes32 public constant CAPABILITY_TYPEHASH =
        keccak256("Capability(address signer,uint256 spendingCap,bytes32 poolId,uint64 expiry)");

    /// @dev Signer-signed, node-addressed. `signer` binds the voucher to the
    ///      authorized key; `provider` binds it to a single payee, so one
    ///      node cannot redeem another node's voucher.
    bytes32 public constant VOUCHER_TYPEHASH = keccak256(
        "Voucher(bytes32 poolId,address signer,address provider,uint256 amount,"
        "uint256 bytesDelivered,bytes32 chainRoot,uint256 chunkPrice)"
    );

    /// @notice The EIP-712 domain separator capabilities and vouchers are
    ///         signed against. It binds the chain id and this pool's address.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    bytes32 public immutable DOMAIN_SEPARATOR;

    // -----------------------------------------------------------------
    // Immutables
    // -----------------------------------------------------------------

    /// @notice The settlement token (native 6-decimal USDC).
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IERC20 public immutable usdc;

    /// @notice Grace window in seconds between `closePool` and `reclaim`. It
    ///         exceeds the chain's censorship window (ADR 003 § L2 sequencer
    ///         censorship). The `uint32` width keeps `closePool`'s deadline
    ///         from wrapping.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint32 public immutable disputeWindow;

    /// @notice Share of each redemption paid to the redeemer in the same
    ///         transaction, in bps (6000 in the reference deployment).
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint256 public immutable operatorBps;

    /// @notice Smallest batch total `sweep` sends, in USDC base units. Each
    ///         sweep costs one receive on the governance chain, so the batch
    ///         total, not each operator's share, must cover it.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint64 public immutable minSweep;

    /// @notice Minimum credited `openPool` deposit in USDC base units. `0`
    ///         leaves the Sybil floor dormant. `topUp` is exempt.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint64 public immutable minDeposit;

    /// @notice The one address that may pause `openPool` and `topUp`.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    address public immutable pauser;

    // -----------------------------------------------------------------
    // Storage
    // -----------------------------------------------------------------

    /// @notice Per-owner monotonic pool counter used in `poolId` derivation.
    mapping(address owner => uint256) public ownerPoolNonce;

    /// @dev Two slots. Every USDC field on this contract is `uint64`: 1.845e19
    ///      base units is about $18.4 trillion, far above USDC's supply.
    struct Pool {
        address owner;
        Status status;
        uint64 disputeDeadline;
        uint64 deposit;
        uint64 totalRedeemed;
    }

    /// @dev One slot. `spent <= cap` by construction.
    struct Authorization {
        uint64 cap;
        uint64 expiry;
        uint64 spent;
    }

    /// @dev One slot. Both fields are watermarks that only advance toward a
    ///      presented `uint64` cumulative.
    struct Lane {
        uint64 amount;
        uint64 bytesDelivered;
    }

    /// @dev One slot. The unswept protocol share of one operator and the
    ///      paid-proportional bytes that ride with it.
    struct Pending {
        uint192 servedBytes;
        uint64 usdc;
    }

    /// @notice One operator's entry in a sweep. The bridge payload is
    ///         `abi.encode(SweepShare[])`, in `sweep` input order.
    struct SweepShare {
        address operator;
        uint256 servedBytes;
        uint256 amount;
    }

    mapping(bytes32 poolId => Pool) internal pools;

    /// @notice Per-signer authorization, set once on the signer's first
    ///         redemption.
    mapping(bytes32 poolId => mapping(address signer => Authorization)) public authorized;

    /// @notice Per-`(pool, signer, provider)` redemption lane.
    mapping(bytes32 poolId => mapping(address signer => mapping(address provider => Lane))) public watermark;

    mapping(address operator => Pending) internal _pending;

    // -----------------------------------------------------------------
    // Events (ADR 003 § Events)
    // -----------------------------------------------------------------

    /// @notice A pool opened with `deposit` credited.
    event PoolOpened(bytes32 indexed poolId, address indexed owner, uint256 deposit);
    /// @notice The owner added `additionalDeposit` to an open pool.
    event PoolToppedUp(bytes32 indexed poolId, uint256 additionalDeposit, uint256 newDeposit);

    /// @notice One lane's outcome inside a `PoolRedeemed`. `newPaidCumulative`
    ///         is the lane's cumulative paid amount after the advance, the
    ///         value a node writes to its paid watermark. `bytesPaid` is the
    ///         lane's paid-proportional served bytes.
    struct LaneSettled {
        address signer;
        uint64 newPaidCumulative;
        uint64 bytesPaid;
    }

    /// @notice A node cashed vouchers against one pool. Emitted once per pool
    ///         group with an entry per lane that paid. A node follows this
    ///         event, filtered on its own `provider`, as the single write path
    ///         for the paid side.
    event PoolRedeemed(bytes32 indexed poolId, address indexed provider, LaneSettled[] lanes);

    /// @notice The owner started the grace-window close on `poolId`.
    event PoolCloseInitiated(bytes32 indexed poolId, address indexed owner, uint256 disputeDeadline);

    /// @notice `reclaim` refunded `ownerRefund` and closed the pool.
    event PoolReclaimed(bytes32 indexed poolId, address indexed owner, uint256 ownerRefund);

    /// @notice `sweep` sent `amount` and `servedBytes` for `operator` to the
    ///         governance chain. Emitted once per operator in the batch.
    event Swept(address indexed operator, uint256 servedBytes, uint256 amount);

    /// @notice `sweep` handed one batch total `amount` to the bridge. `maxFee`
    ///         is the bridge fee the sweep allowed.
    event SweepSent(uint256 amount, uint256 maxFee);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    /// @notice A required constructor address is zero.
    error ZeroAddress();
    /// @notice `operatorBps` exceeds 10000.
    error OperatorBpsAboveWhole(uint256 operatorBps);
    /// @notice A deposit or top-up moved no USDC.
    error ZeroAmount();
    /// @dev The credited `openPool` deposit is below `minDeposit`.
    error BelowMinDeposit(uint256 received, uint256 minDeposit);
    /// @notice The pool is not `Open`.
    error PoolNotOpen();
    /// @notice The caller is not the pool owner.
    error NotPoolOwner();
    /// @notice A voucher signature does not recover to its signer for this payee.
    error InvalidVoucherSignature();
    /// @notice A capability signature does not verify against the pool owner.
    error InvalidCapabilitySignature();
    /// @notice The pool is closed, or past its grace window.
    error PoolClosed();
    /// @notice `reclaim` on a pool that is not `Closing`.
    error PoolNotClosing();
    /// @notice `reclaim` before the grace window elapsed.
    error GraceWindowActive();
    /// @notice Batch view arrays differ in length.
    error LengthMismatch();
    /// @dev A submitted hash-chain preimage does not reach `chainRoot` in
    ///      `chainIndex` steps.
    error BadPreimage();
    /// @dev The packed `chainMeter` word has a non-zero byte in its reserved
    ///      upper span.
    error ChainMeterReservedNonZero();
    /// @dev The chain-extended claim does not fit `uint64`.
    error ClaimOverflow();
    /// @notice The batch total is below `minSweep`.
    error BelowMinSweep(uint256 total, uint256 minSweep);
    /// @notice A sweep batch is empty or longer than `MAX_SWEEP_BATCH`.
    error BadSweepBatch(uint256 length);
    /// @notice `pause` or `unpause` from an address other than `pauser`.
    error NotPauser();

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param usdc_          Native USDC on this chain.
    /// @param disputeWindow_ Grace window in seconds.
    /// @param operatorBps_   Same-transaction operator share (at most 10000).
    /// @param minSweep_      Smallest sweep batch total in USDC base units.
    /// @param minDeposit_    Minimum credited `openPool` deposit (`0` is dormant).
    /// @param pauser_        The address that may pause inflows.
    constructor(
        IERC20 usdc_,
        uint32 disputeWindow_,
        uint256 operatorBps_,
        uint64 minSweep_,
        uint64 minDeposit_,
        address pauser_
    ) EIP712("PaymentPool", "1") {
        if (address(usdc_) == address(0) || pauser_ == address(0)) revert ZeroAddress();
        if (operatorBps_ > BPS_DENOMINATOR) revert OperatorBpsAboveWhole(operatorBps_);
        usdc = usdc_;
        disputeWindow = disputeWindow_;
        operatorBps = operatorBps_;
        minSweep = minSweep_;
        minDeposit = minDeposit_;
        pauser = pauser_;
        DOMAIN_SEPARATOR = _domainSeparatorV4();
    }

    // -----------------------------------------------------------------
    // Pool lifecycle
    // -----------------------------------------------------------------

    /// @notice Open a USDC pool; transfers `deposit` in and derives
    ///         `poolId = keccak256(owner, ownerPoolNonce[owner])`. Names no
    ///         provider and no signer.
    // slither-disable-next-line reentrancy-no-eth
    function openPool(uint64 deposit) external nonReentrant whenNotPaused returns (bytes32 poolId) {
        if (deposit == 0) revert ZeroAmount();

        uint256 nonce = ownerPoolNonce[msg.sender];
        poolId = keccak256(abi.encodePacked(msg.sender, nonce));
        ownerPoolNonce[msg.sender] = nonce + 1;

        Pool storage p = pools[poolId];
        p.owner = msg.sender;
        p.status = Status.Open;

        // Credit the balance actually received, so a fee-on-transfer USDC
        // proxy cannot over-state this pool's share of the shared balance.
        // aderyn-ignore-next-line(reentrancy-state-change)
        uint256 balanceBefore = usdc.balanceOf(address(this));
        usdc.safeTransferFrom(msg.sender, address(this), deposit);
        // aderyn-ignore-next-line(reentrancy-state-change)
        uint256 received = usdc.balanceOf(address(this)) - balanceBefore;
        // slither-disable-next-line incorrect-equality
        if (received == 0) revert ZeroAmount();
        if (received < minDeposit) revert BelowMinDeposit(received, minDeposit);
        p.deposit = received.toUint64();

        emit PoolOpened(poolId, msg.sender, received);
    }

    /// @notice Owner-only: add funds to an open pool.
    // slither-disable-next-line reentrancy-no-eth
    function topUp(bytes32 poolId, uint64 additionalDeposit) external nonReentrant whenNotPaused {
        Pool storage p = pools[poolId];
        if (p.status != Status.Open) revert PoolNotOpen();
        if (msg.sender != p.owner) revert NotPoolOwner();
        if (additionalDeposit == 0) revert ZeroAmount();

        // aderyn-ignore-next-line(reentrancy-state-change)
        uint256 balanceBefore = usdc.balanceOf(address(this));
        usdc.safeTransferFrom(msg.sender, address(this), additionalDeposit);
        // aderyn-ignore-next-line(reentrancy-state-change)
        uint256 received = usdc.balanceOf(address(this)) - balanceBefore;
        // slither-disable-next-line incorrect-equality
        if (received == 0) revert ZeroAmount();
        p.deposit += received.toUint64();

        emit PoolToppedUp(poolId, received, p.deposit);
    }

    /// @notice Owner-only: start the grace-window close on `poolId`. Moves no
    ///         funds; `redeemMany` stays callable until `disputeDeadline`.
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
    ///         the pool.
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

    /// @notice One signer registration inside a pool's batch.
    struct CapabilityReg {
        address signer;
        uint64 spendingCap;
        uint64 expiry;
        bytes ownerSig;
    }

    /// @notice One lane's voucher inside a pool's batch. Names neither its
    ///         pool (the enclosing `PoolBatch` does) nor its payee: the voucher
    ///         is redeemed for `msg.sender`, which is also what the EIP-712
    ///         hash is rebuilt with.
    /// @dev    The signature is the EIP-2098 compact pair `(r, vs)`, which keeps
    ///         the struct static: 8 words, 256 calldata bytes per lane.
    ///         `chainMeter` packs `chunkPrice` (bytes 23..=30) and `chainIndex`
    ///         (the low byte); bytes 0..=22 are reserved and MUST be zero.
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

    /// @notice Everything a node redeems against one pool.
    struct PoolBatch {
        bytes32 poolId;
        CapabilityReg[] capabilities;
        LaneVoucher[] vouchers;
    }

    /// @notice The sole redemption entry point (ADR 003 § Batch redemption):
    ///         per pool, register every capability, then redeem every voucher.
    ///         Pays `operatorBps` of the total paid to `msg.sender` in the same
    ///         transaction and accrues the rest, with the paid-proportional
    ///         served bytes, in `pending[msg.sender]`. The pool clamps no bytes.
    /// @dev    `_registerCapability` keeps the larger value of each field, is a
    ///         no-op for a capability that raises neither, and reverts on a bad
    ///         owner signature. `_applyVoucher` returns zeros on a transient-empty
    ///         voucher, which the loop skips, and reverts on a bad voucher
    ///         signature. Every lane advance lands before the single transfer.
    function redeemMany(PoolBatch[] calldata batches) external nonReentrant returns (uint256 totalPaid) {
        uint256 totalBytes = 0;

        for (uint256 b = 0; b < batches.length; b++) {
            PoolBatch calldata batch = batches[b];
            bytes32 poolId = batch.poolId;
            Pool storage p = pools[poolId];

            if (p.status == Status.Closed) revert PoolClosed();
            // forge-lint: disable-next-line(block-timestamp)
            if (p.status == Status.Closing && block.timestamp >= p.disputeDeadline) revert PoolClosed();

            for (uint256 i = 0; i < batch.capabilities.length; i++) {
                CapabilityReg calldata c = batch.capabilities[i];
                _registerCapability(poolId, c.signer, c.spendingCap, c.expiry, c.ownerSig);
            }

            uint64 remaining = p.deposit - p.totalRedeemed;
            uint64 poolPaid = 0;
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
                // Shorten the array to the lanes that paid; `settledCount` is
                // bounded by the allocated length.
                // solhint-disable-next-line no-inline-assembly
                assembly ("memory-safe") {
                    mstore(settled, settledCount)
                }
                emit PoolRedeemed(poolId, msg.sender, settled);
            }
        }

        if (totalPaid != 0) {
            uint256 operatorShare = (totalPaid * operatorBps) / BPS_DENOMINATOR;
            Pending storage pd = _pending[msg.sender];
            pd.usdc += (totalPaid - operatorShare).toUint64();
            pd.servedBytes += totalBytes.toUint192();
            if (operatorShare != 0) usdc.safeTransfer(msg.sender, operatorShare);
        }
    }

    // -----------------------------------------------------------------
    // Sweep
    // -----------------------------------------------------------------

    /// @notice Send each listed operator's pending protocol share and served
    ///         bytes to the governance chain in one bridge transfer.
    ///         Permissionless. Takes 1 to `MAX_SWEEP_BATCH` operators. Skips an
    ///         entry with no pending share, so a repeated entry or an operator
    ///         swept first by someone else never reverts the batch. Needs the
    ///         batch total to be non-zero and at least `minSweep`. Resets every
    ///         swept `pending` before the bridge call.
    function sweep(address[] calldata operators) external nonReentrant {
        uint256 n = operators.length;
        if (n == 0 || n > MAX_SWEEP_BATCH) revert BadSweepBatch(n);
        SweepShare[] memory found = new SweepShare[](n);
        uint256 k = 0;
        uint256 total = 0;
        for (uint256 i = 0; i < n; i++) {
            address operator = operators[i];
            Pending memory p = _pending[operator];
            // slither-disable-next-line incorrect-equality
            if (p.usdc == 0) continue;
            delete _pending[operator];
            found[k++] = SweepShare({ operator: operator, servedBytes: p.servedBytes, amount: p.usdc });
            total += p.usdc;
            emit Swept(operator, p.servedBytes, p.usdc);
        }
        if (total == 0 || total < minSweep) revert BelowMinSweep(total, minSweep);
        SweepShare[] memory shares = new SweepShare[](k);
        for (uint256 i = 0; i < k; i++) {
            shares[i] = found[i];
        }
        uint256 fee = _sweepFee(total);
        emit SweepSent(total, fee);
        _sendSweep(shares, total, fee);
    }

    /// @notice An operator's unswept protocol share and the served bytes that
    ///         ride with it.
    function pending(address operator) external view returns (uint256 servedBytes, uint256 usdcAmount) {
        Pending memory p = _pending[operator];
        return (p.servedBytes, p.usdc);
    }

    /// @dev The bridge fee a sweep of `amount` allows.
    function _sweepFee(uint256 amount) internal view virtual returns (uint256);

    /// @dev Hand `amount` USDC, the sum of `shares`, and the shares themselves
    ///      to the bridge.
    function _sendSweep(SweepShare[] memory shares, uint256 amount, uint256 fee) internal virtual;

    // -----------------------------------------------------------------
    // Views
    // -----------------------------------------------------------------

    /// @notice The pool record for `poolId`.
    function getPool(bytes32 poolId) external view returns (Pool memory) {
        return pools[poolId];
    }

    /// @notice The registration of `signer` on `poolId`.
    function getAuthorization(bytes32 poolId, address signer) external view returns (Authorization memory) {
        return authorized[poolId][signer];
    }

    /// @notice One `authorized[poolIds[i]][signers[i]]` read per pair, in input
    ///         order. Reverts `LengthMismatch` when the arrays differ in length.
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

    /// @notice The lane `(poolId, signer, provider)`.
    function getWatermark(bytes32 poolId, address signer, address provider) external view returns (Lane memory) {
        return watermark[poolId][signer][provider];
    }

    /// @notice One `watermark[poolIds[i]][signers[i]][providers[i]]` read per
    ///         triple, in input order. Reverts `LengthMismatch` when the arrays
    ///         differ in length.
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

    /// @notice A page of the pool ids `owner` has opened, oldest first.
    /// @dev    Recomputed from `ownerPoolNonce`, never stored. Never forms
    ///         `offset + limit`, so `limit == type(uint256).max` clamps.
    function getPools(address owner, uint256 offset, uint256 limit) external view returns (bytes32[] memory page) {
        uint256 len = ownerPoolNonce[owner];
        if (offset >= len || limit == 0) {
            return new bytes32[](0);
        }
        uint256 left = len - offset;
        uint256 size = limit < left ? limit : left;
        page = new bytes32[](size);
        for (uint256 i = 0; i < size; i++) {
            page[i] = keccak256(abi.encodePacked(owner, offset + i));
        }
    }

    // -----------------------------------------------------------------
    // Pause control (immutable pauser)
    // -----------------------------------------------------------------

    /// @notice Block `openPool` and `topUp`. Reverts after the sunset.
    function pause() external {
        if (msg.sender != pauser) revert NotPauser();
        _requirePauseWindowOpen();
        _pause();
    }

    /// @notice Lift the pause.
    function unpause() external {
        if (msg.sender != pauser) revert NotPauser();
        _unpause();
    }

    // -----------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------

    /// @dev Register a signer's terms by verifying the owner's EIP-712
    ///      `Capability`. The registration holds the larger value of each field
    ///      over every capability the owner signed for the signer and anyone
    ///      landed: the owner is bound by the most generous terms it signed, so
    ///      a provider that holds a capability always redeems under at least its
    ///      terms. A capability that raises neither field returns without
    ///      re-verifying, and one that can pay nothing (a zero cap or an
    ///      expiry already past) is never recorded.
    function _registerCapability(
        bytes32 poolId,
        address signer,
        uint64 spendingCap,
        uint64 expiry,
        bytes calldata ownerSig
    ) internal {
        Authorization storage a = authorized[poolId][signer];
        if (spendingCap <= a.cap && expiry <= a.expiry) return;
        // forge-lint: disable-next-line(block-timestamp)
        if (spendingCap == 0 || expiry <= block.timestamp) return;

        bytes32 structHash = keccak256(abi.encode(CAPABILITY_TYPEHASH, signer, spendingCap, poolId, expiry));
        bytes32 digest = _hashTypedDataV4(structHash);
        // ECDSA first, so a capability the owner's key signed stays valid if
        // the owner later attaches code to its address. A smart-account owner
        // falls through to ERC-1271.
        address owner = pools[poolId].owner;
        // slither-disable-next-line unused-return
        (address recovered, ECDSA.RecoverError err,) = ECDSA.tryRecover(digest, ownerSig);
        if (
            !(err == ECDSA.RecoverError.NoError && recovered == owner)
                && !SignatureChecker.isValidERC1271SignatureNow(owner, digest, ownerSig)
        ) revert InvalidCapabilitySignature();
        if (spendingCap > a.cap) a.cap = spendingCap;
        if (expiry > a.expiry) a.expiry = expiry;
    }

    /// @dev The cumulative-`min` redemption core (ADR 003 § Redemption
    ///      behavior) for the payee `msg.sender`. Returns the USDC `paid`, the
    ///      paid-proportional `bytesPaid`, and the lane's new cumulative, or
    ///      zeros on a transient-empty voucher that writes no state (an
    ///      unregistered signer, an expired capability, a cumulative at or
    ///      below the watermark, or a drained pool or spent cap). Reverts only
    ///      on a bad voucher signature or a malformed chain. Touches no pool
    ///      storage and moves no money.
    function _applyVoucher(bytes32 poolId, LaneVoucher calldata v, uint64 remaining)
        internal
        returns (uint64 paid, uint64 bytesPaid, uint64 newCumulative)
    {
        address signer = v.signer;

        Authorization storage a = authorized[poolId][signer];
        if (a.cap == 0 && a.expiry == 0) return (0, 0, 0);

        (uint64 claimed, uint64 claimedBytes, uint256 chunkPrice) = _resolveClaim(v);

        _verifyVoucher(poolId, v, chunkPrice);

        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp >= a.expiry) return (0, 0, 0);

        Lane storage w = watermark[poolId][signer][msg.sender];
        if (claimed <= w.amount) return (0, 0, 0);

        uint64 desired = claimed - w.amount;
        paid = uint64(Math.min(desired, Math.min(a.cap - a.spent, remaining)));
        if (paid == 0) return (0, 0, 0);

        // Bytes advance in proportion to the payment that covers them, so a
        // partial payment credits only its share of the new bytes.
        // `bytesPaid <= bytesDelta` because `paid <= desired`.
        uint64 bytesDelta = claimedBytes > w.bytesDelivered ? claimedBytes - w.bytesDelivered : 0;
        bytesPaid = uint64(Math.mulDiv(bytesDelta, paid, desired));

        w.amount += paid;
        w.bytesDelivered += bytesPaid;
        a.spent += paid;
        newCumulative = w.amount;
    }

    /// @dev Resolve a voucher's chain into the claim it authorizes (ADR 003 §
    ///      Hash-chain metering (`PayWord`)): unpack `chainMeter`, walk
    ///      `preimage` forward `chainIndex` times to `chainRoot`, and extend
    ///      the anchor by `chainIndex` chunks. A sealed voucher (zero root,
    ///      zero preimage, zero meter) resolves to exactly `cumulative`.
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

        // Derived from calldata rather than read from a signed `uint64`, so
        // both are bounded here before narrowing.
        uint256 wideClaimed = uint256(v.cumulative) + chainIndex * chunkPrice;
        uint256 wideBytes = uint256(v.bytesDelivered) + chainIndex * CHUNK_BYTES;
        if (wideClaimed > type(uint64).max || wideBytes > type(uint64).max) revert ClaimOverflow();

        claimed = uint64(wideClaimed);
        claimedBytes = uint64(wideBytes);
    }

    /// @dev Verify an EIP-712 `Voucher` signature against `v.signer` with
    ///      `msg.sender` as the `provider`. A voucher signer is an EOA: the
    ///      check is a plain `ecrecover` over the compact pair, which keeps
    ///      lane calldata static and skips an `EXTCODESIZE` per lane.
    function _verifyVoucher(bytes32 poolId, LaneVoucher calldata v, uint256 chunkPrice) internal view {
        bytes32 structHash = keccak256(
            abi.encode(
                VOUCHER_TYPEHASH, poolId, v.signer, msg.sender, v.cumulative, v.bytesDelivered, v.chainRoot, chunkPrice
            )
        );
        bytes32 digest = _hashTypedDataV4(structHash);
        // slither-disable-next-line unused-return
        (address recovered, ECDSA.RecoverError err,) = ECDSA.tryRecover(digest, v.r, v.vs);
        if (err != ECDSA.RecoverError.NoError || recovered != v.signer) revert InvalidVoucherSignature();
    }
}
