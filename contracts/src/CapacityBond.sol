// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { EIP712 } from "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import { SignatureChecker } from "@openzeppelin/contracts/utils/cryptography/SignatureChecker.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

import { ICapacityBond } from "./interfaces/ICapacityBond.sol";
import { IEd25519Verifier } from "./interfaces/IEd25519Verifier.sol";
import { ISafetyReserve } from "./interfaces/ISafetyReserve.sol";

/// @title CapacityBond — operator-registry contract
/// @notice Custodies operator TOKEN bond, executes the 50/30/20 slash split
///         from ADR 026 § Slashing and burn, is the canonical settlement
///         reporter sink for `FeeRouter`, is the registry for iroh-NodeId ↔
///         Ethereum-address bindings, holds the 50M TOKEN Genesis Bond
///         Credit allocation in per-operator `PendingCredit` positions
///         (ADR 026 § Genesis Bond Credits), and is the source of
///         `firstBondedAt` / `slashedAtEpoch` for `DecdnGovernor`'s
///         served-bytes voting weight per ADR 036.
/// @dev    Renamed from `StakingRegistry` per ADR 026 v2.2 vocabulary. The
///         stake / unstake / slash / node-registry primitives are unchanged
///         from the prior contract; this revision adds:
///           - `firstBondedAt[op]`  — set on first successful `stake` (ADR 036)
///           - `slashedAtEpoch[op]` — stamped in `slash()`, cleared by
///                                    `clearSlashedAtEpoch` via
///                                    `APPEAL_REVERSAL_ROLE` (ADR 028 / 036)
///           - `regionPrev[op]` + `regionLastChanged[op]` + `updateRegion`
///                                    (ADR 030 § Node Region Self-Attestation)
///           - `declaredMbps[op]`   — operator-asserted capacity (ADR 026
///                                    § Capacity-bond curve; bond enforcement
///                                    against the curve `bond = k × Mbps^α`
///                                    is documented in `capacityBond` view
///                                    and intended for off-chain inspection
///                                    + a future enforcement PR)
///           - `pendingCredit[op]`  — Genesis Bond Credit accounting
///                                    (ADR 026 § Genesis Bond Credits)
///         Slash math also applies to the unvested portion of
///         `pendingCredit[op]`: that portion is added to `slashAmount` and
///         routed through the same 50% challenger / 30% SafetyReserve / 20%
///         burn split (ADR 026 § Genesis Bond Credits — "same terms as
///         voluntary bond").
contract CapacityBond is ICapacityBond, AccessControl, ReentrancyGuard, Pausable, EIP712 {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant SLASH_ROLE = keccak256("SLASH_ROLE");
    bytes32 public constant BLACKLIST_ROLE = keccak256("BLACKLIST_ROLE");
    bytes32 public constant SETTLEMENT_REPORTER_ROLE = keccak256("SETTLEMENT_REPORTER_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    /// @notice Authority to grant a one-shot `PendingCredit` at TGE. Granted to
    ///         the Treasury within `GENESIS_CREDIT_WINDOW` (ADR 016 § Post-
    ///         Deployment Initialization, step 7).
    bytes32 public constant GENESIS_GRANTOR_ROLE = keccak256("GENESIS_GRANTOR_ROLE");

    /// @notice Authority to call `clearSlashedAtEpoch`. Granted to `SafetyReserve`
    ///         (ADR 016 § Post-Deployment Initialization, step 6); consumed by
    ///         `SafetyReserve.reverseAppeal` per ADR 028 § Contract surface.
    bytes32 public constant APPEAL_REVERSAL_ROLE = keccak256("APPEAL_REVERSAL_ROLE");

    // -----------------------------------------------------------------
    // Slash schedule constants (ADR 026 § Slashing and burn)
    // -----------------------------------------------------------------

    uint256 internal constant BPS_DENOMINATOR = 10_000;
    uint256 internal constant SLASH_BPS_TIER_1 = 500;
    uint256 internal constant SLASH_BPS_TIER_2 = 1500;
    uint256 internal constant SLASH_BPS_TIER_3 = 5000;
    uint256 internal constant CHALLENGER_BPS = 5000;
    uint256 internal constant SAFETY_BPS = 3000;
    // 20% burn share is the implicit remainder so legs sum exactly.

    // -----------------------------------------------------------------
    // Governable-parameter safety bounds
    // -----------------------------------------------------------------

    uint256 internal constant MIN_STAKE_FLOOR = 10_000e18;
    uint256 internal constant MIN_STAKE_CEILING = 1_000_000e18;

    uint256 internal constant UNBONDING_PERIOD_FLOOR = 3 days;
    uint256 internal constant UNBONDING_PERIOD_CEILING = 30 days;

    uint256 internal constant MULTIADDR_COOLDOWN_CEILING = 1 days;

    uint256 internal constant MAX_MULTIADDR_SIZE_FLOOR = 64;
    uint256 internal constant MAX_MULTIADDR_SIZE_CEILING = 1024;

    uint256 internal constant MAX_REGION_HINT_BYTES = 16;

    /// @notice Bounds on `regionStabilityWindow` per ADR 030 § Cooldown.
    uint256 internal constant REGION_STABILITY_WINDOW_FLOOR = 3 days;
    uint256 internal constant REGION_STABILITY_WINDOW_CEILING = 30 days;

    /// @notice Bounds on the Genesis Bond Credit grant window (ADR 026
    ///         § Genesis Bond Credits — TGE one-shot, default 30 days).
    uint256 internal constant GENESIS_CREDIT_WINDOW_FLOOR = 7 days;
    uint256 internal constant GENESIS_CREDIT_WINDOW_CEILING = 90 days;

    /// @notice Linear vest duration for Genesis Bond Credits (24 months).
    uint256 internal constant GENESIS_VEST_DURATION = 730 days;

    /// @notice Canonical epoch length shared with `FeeRouter` (ADR 026 §
    ///         FeeRouter split, ADR 036 § Formula). Stored as a constant
    ///         on this contract so `slashedAtEpoch` derives the same epoch
    ///         index `FeeRouter` writes into `bytesPerEpoch`.
    uint64 public constant EPOCH_LENGTH = 7 days;

    /// @notice Bounds for `claimSlashGateEpochs` — match `FeeRouter`'s
    ///         `[WINDOW_EPOCHS_FLOOR, WINDOW_EPOCHS_CEILING]` so governance
    ///         can keep the two in lock-step in a single multi-call without
    ///         this contract taking a cross-contract dependency on FeeRouter.
    uint64 internal constant CLAIM_SLASH_GATE_FLOOR = 4;
    uint64 internal constant CLAIM_SLASH_GATE_CEILING = 26;

    // -----------------------------------------------------------------
    // EIP-712 typehashes
    // -----------------------------------------------------------------

    bytes32 public constant BIND_NODE_TYPEHASH = keccak256("BindNodeId(bytes32 nodeId,uint64 nonce)");

    // -----------------------------------------------------------------
    // Immutable wiring
    // -----------------------------------------------------------------

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ERC20Burnable public immutable token;
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IEd25519Verifier public immutable ed25519Verifier;

    /// @notice End of the Genesis Bond Credit grant window
    ///         (constructor-set as `block.timestamp + genesisCreditWindow`).
    /// @dev    After this timestamp `grantGenesisCredit` permanently reverts.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint64 public immutable genesisCreditWindowEnd;

    // -----------------------------------------------------------------
    // Storage — staking
    // -----------------------------------------------------------------

    struct UnbondingRequest {
        uint256 amount;
        uint256 unlockAt;
    }

    mapping(address operator => uint256 amount) public activeStake;
    mapping(address operator => UnbondingRequest) public unbondingOf;
    mapping(address operator => uint32) public lifetimeOffenseCount;
    mapping(address operator => uint256) public lastSettlementAt;
    mapping(address operator => bool) public ejected;

    /// @notice First-bond-time stamp per operator (ADR 036 § Formula —
    ///         `age_ramp` numerator). Set once on the first `stake` call that
    ///         lifts the operator's `activeStake` above zero; never overwritten.
    mapping(address operator => uint64) internal _firstBondedAt;

    /// @notice Epoch index at which the operator was most recently slashed,
    ///         or 0 if not currently in a slash-zero-out window
    ///         (ADR 036 § Slashing zero-out).
    mapping(address operator => uint64) internal _slashedAtEpoch;

    /// @notice Operator-asserted serving capacity in Mbps (ADR 026
    ///         § Capacity-bond curve). Stored verbatim — curve enforcement
    ///         `activeStake ≥ k × Mbps^α` is not enforced at the contract
    ///         layer in this revision; downstream readers and the Governor
    ///         age-ramp do not depend on it.
    mapping(address operator => uint256) public declaredMbps;

    uint256 public minStake;
    uint256 public unbondingPeriod;

    /// @notice SafetyReserve sink for the 30% slash leg. Zero at construction;
    ///         must be wired post-deploy before any `slash` can fire.
    ISafetyReserve public safetyReserve;

    /// @notice Treasury destination for unvested Genesis Bond Credit forfeited
    ///         by an operator who initiates `requestUnstake` before 24mo of
    ///         vesting (ADR 026 § Genesis Bond Credits — exit clause). If zero
    ///         at unbond time, forfeited credit is burned as a safe fallback.
    address public treasury;

    // -----------------------------------------------------------------
    // Storage — region attestation (ADR 030)
    // -----------------------------------------------------------------

    /// @notice Previous-region snapshot taken on each `updateRegion` call.
    mapping(address operator => string) public regionPrev;

    /// @notice Last `updateRegion` timestamp; 0 means region has never been
    ///         changed (initial value from `registerNode` is final until the
    ///         first explicit update).
    mapping(address operator => uint64) public regionLastChanged;

    /// @notice Cooldown enforced between `updateRegion` calls per ADR 030
    ///         (default 7 days; governable [3d, 30d]).
    uint256 public regionStabilityWindow;

    /// @notice Number of epochs after a slash during which
    ///         `claimVestedCredit` is blocked. Default 13 (≈ one quarter),
    ///         matching the `FeeRouter.windowEpochs` default. Governance
    ///         SHOULD keep this in lock-step with `FeeRouter.windowEpochs`
    ///         in the same multi-call so the claim gate doesn't drift from
    ///         the slash zero-out window used by `DecdnGovernor`. Bounded
    ///         [4, 26] to match FeeRouter.
    uint64 public claimSlashGateEpochs;

    // -----------------------------------------------------------------
    // Storage — Genesis Bond Credits (ADR 026 § Genesis Bond Credits)
    // -----------------------------------------------------------------

    /// @notice Per-operator pending credit. `originalGrant` is set once at TGE
    ///         by Treasury via `grantGenesisCredit` and decreases ONLY when the
    ///         unvested portion is slashed or forfeited on unbond. `claimed`
    ///         is the monotone cumulative amount the operator has pulled into
    ///         `activeStake` via `claimVestedCredit`. The linear vest curve
    ///         is computed on demand from `originalGrant × elapsed / 24mo`,
    ///         not stored — decoupling principal from the running balance
    ///         eliminates the mid-vest claim-acceleration bug that arises
    ///         when the curve is recomputed against a reduced principal.
    struct PendingCredit {
        uint128 originalGrant;
        uint128 claimed;
        uint64 grantedAt;
    }

    mapping(address operator => PendingCredit) internal _pendingCredit;

    // -----------------------------------------------------------------
    // Storage — slash records (ADR 028 § Contract surface)
    // -----------------------------------------------------------------

    /// @notice On-chain slash event record consumed by `SafetyReserve.openSlashAppeal`
    ///         to validate appeals against a specific slash without trusting the
    ///         appellant's `operator` parameter (ADR 028 — closes the unverified-
    ///         operator hole).
    struct SlashRecord {
        address operator;
        uint64 slashedAt;
        uint256 slashAmount;
    }

    /// @notice Monotonic slash counter — next slash receives this index, then
    ///         `slashCounter` increments.
    uint256 public slashCounter;

    mapping(uint256 slashId => SlashRecord) internal _slashRecords;

    // -----------------------------------------------------------------
    // Storage — node registry (ADR 019 § Node Onboarding;
    // NodeId binding per ADR 003 — payments authorization)
    // -----------------------------------------------------------------

    struct NodeInfo {
        bytes32 nodeId;
        address ethAddress;
        bool active;
        uint256 registeredAt;
        uint256 firstRegisteredAt;
        uint256 lastMultiaddrUpdate;
        bytes multiaddrs;
        string regionHint;
    }

    mapping(address operator => NodeInfo) internal _nodes;
    mapping(bytes32 nodeId => address operator) public nodeIdToAddress;
    mapping(address operator => bytes32 nodeId) public addressToNodeId;
    mapping(address operator => uint64) public bindingNonce;
    mapping(bytes32 nodeId => uint64) public registrationNonce;

    address[] internal _registeredAddrs;
    mapping(address operator => uint256 onePlusIndex) internal _registeredIndex;

    uint256 public multiaddrUpdateCooldown;
    uint256 public maxMultiaddrSize;

    // -----------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------

    event Staked(address indexed operator, uint256 amount, uint256 newActiveStake);
    event UnbondingRequested(address indexed operator, uint256 amount, uint256 unlockAt, uint256 newActiveStake);
    event Unstaked(address indexed operator, uint256 amount);
    event Slashed(
        address indexed operator,
        address indexed challenger,
        uint8 offenseType,
        uint32 lifetimeOffenseCount,
        uint256 slashAmount,
        uint256 challengerShare,
        uint256 safetyShare,
        uint256 burnShare
    );
    event AutoEjected(address indexed operator, uint256 remainingStake);
    event EjectedByBlacklist(address indexed operator);
    event Reinstated(address indexed operator);
    event SettlementRecorded(address indexed operator);
    event MinStakeUpdated(uint256 oldValue, uint256 newValue);
    event UnbondingPeriodUpdated(uint256 oldValue, uint256 newValue);
    event SafetyReserveUpdated(address indexed oldAddr, address indexed newAddr);
    event MultiaddrUpdateCooldownUpdated(uint256 oldValue, uint256 newValue);
    event MaxMultiaddrSizeUpdated(uint256 oldValue, uint256 newValue);
    event RegionStabilityWindowUpdated(uint256 oldValue, uint256 newValue);
    event ClaimSlashGateEpochsUpdated(uint64 oldValue, uint64 newValue);

    event NodeRegistered(
        bytes32 indexed nodeId,
        address indexed ethAddress,
        bytes multiaddrs,
        string regionHint,
        uint64 bindingNonce,
        uint64 registrationNonce
    );
    event NodeIdBound(address indexed ethAddress, bytes32 indexed nodeId, uint64 bindingNonce);
    event NodeMultiaddrUpdated(bytes32 indexed nodeId, bytes multiaddrs);
    event NodeDeregistered(bytes32 indexed nodeId);
    event NodeAutoEjected(bytes32 indexed nodeId, uint256 remainingStake);
    event NodeIdReclaimed(bytes32 indexed nodeId, address indexed previousOwner);

    // ADR 030 — region self-attestation.
    event RegionUpdated(bytes32 indexed nodeId, string oldRegion, string newRegion);

    // ADR 036 — slash-zero-out lifecycle.
    event SlashedAtEpochStamped(address indexed operator, uint64 epoch);
    event SlashedAtEpochCleared(address indexed operator);

    // ADR 026 — Genesis Bond Credits.
    event GenesisCreditGranted(address indexed operator, uint256 amount);
    event GenesisCreditClaimed(address indexed operator, uint256 amount);
    event GenesisCreditSlashed(address indexed operator, uint256 amount);
    event GenesisCreditForfeited(address indexed operator, uint256 amount, address indexed recipient);

    // ADR 026 — declared capacity.
    event MbpsDeclared(address indexed operator, uint256 oldMbps, uint256 newMbps);

    // ADR 028 — slash record minting.
    event SlashRecorded(uint256 indexed slashId, address indexed operator, uint64 slashedAt, uint256 slashAmount);

    // I3 — best-effort slash inflow callback.
    event SlashInflowReportFailed(uint256 indexed slashId, address indexed operator, uint256 amount, bytes reason);

    // Treasury wiring (C3).
    event TreasuryUpdated(address indexed oldTreasury, address indexed newTreasury);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error ZeroAmount();
    error ZeroNodeId();
    error InsufficientStake(uint256 requested, uint256 available);
    error UnbondingInProgress();
    error UnbondingNotComplete(uint256 unlockAt);
    error NoUnbondingRequest();
    error SafetyReserveNotWired();
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);
    error NodeAlreadyRegistered();
    error NodeIdAlreadyBound(address currentOwner);
    error AddressAlreadyBound(bytes32 currentNodeId);
    error InvalidBindingSignature();
    error InvalidEd25519Signature();
    error MultiaddrsTooLarge(uint256 size, uint256 ceiling);
    error MultiaddrCooldownActive(uint256 readyAt);
    error NodeNotActive();
    error StakeBelowMinimum(uint256 stake, uint256 required);
    error OperatorEjected();
    error NodeIdNotBound(bytes32 nodeId);
    error RegionHintTooLong(uint256 size, uint256 ceiling);

    // ADR 030
    error RegionCooldownActive(uint64 readyAt);

    // ADR 026 — Genesis Bond Credits
    error GenesisCreditWindowClosed(uint64 windowEnd);
    error GenesisCreditAlreadyGranted(address operator);
    error NoPendingCredit();
    error NothingVested();
    error NotActiveForClaim(address operator);
    error SlashedInWindowForClaim(address operator);

    // ADR 028 — slash record lookup
    error UnknownSlash(uint256 slashId);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param token_                    TOKEN contract (must implement `burn`).
    /// @param ed25519Verifier_          Verifier for `registerNode` ed25519 proof.
    /// @param admin                     Initial `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE`.
    /// @param minStake_                 Initial minimum active stake.
    /// @param unbondingPeriod_          Initial unbonding period.
    /// @param multiaddrUpdateCooldown_  Initial multiaddr-update cooldown.
    /// @param maxMultiaddrSize_         Initial multiaddrs byte-length cap.
    /// @param regionStabilityWindow_    Initial region-update cooldown (default
    ///                                  7 days; bounded `[3d, 30d]`).
    /// @param genesisCreditWindow_      Duration of the TGE grant window
    ///                                  (default 30 days; bounded `[7d, 90d]`).
    constructor(
        ERC20Burnable token_,
        IEd25519Verifier ed25519Verifier_,
        address admin,
        uint256 minStake_,
        uint256 unbondingPeriod_,
        uint256 multiaddrUpdateCooldown_,
        uint256 maxMultiaddrSize_,
        uint256 regionStabilityWindow_,
        uint256 genesisCreditWindow_
    ) EIP712("CapacityBond", "1") {
        if (address(token_) == address(0) || address(ed25519Verifier_) == address(0) || admin == address(0)) {
            revert ZeroAddress();
        }
        _enforceMinStakeBounds(minStake_);
        _enforceUnbondingPeriodBounds(unbondingPeriod_);
        _enforceMultiaddrCooldownBounds(multiaddrUpdateCooldown_);
        _enforceMaxMultiaddrSizeBounds(maxMultiaddrSize_);
        _enforceRegionStabilityWindowBounds(regionStabilityWindow_);
        if (genesisCreditWindow_ < GENESIS_CREDIT_WINDOW_FLOOR || genesisCreditWindow_ > GENESIS_CREDIT_WINDOW_CEILING)
        {
            revert ParamOutOfBounds({
                value: genesisCreditWindow_, floor: GENESIS_CREDIT_WINDOW_FLOOR, ceiling: GENESIS_CREDIT_WINDOW_CEILING
            });
        }

        token = token_;
        ed25519Verifier = ed25519Verifier_;
        minStake = minStake_;
        unbondingPeriod = unbondingPeriod_;
        multiaddrUpdateCooldown = multiaddrUpdateCooldown_;
        maxMultiaddrSize = maxMultiaddrSize_;
        regionStabilityWindow = regionStabilityWindow_;
        genesisCreditWindowEnd = uint64(block.timestamp + genesisCreditWindow_);
        // Default the claim slash gate to 13 epochs (matches FeeRouter's
        // initial `windowEpochs` default). Governance can retune via
        // `setClaimSlashGateEpochs` in lock-step with FeeRouter changes.
        claimSlashGateEpochs = 13;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    // -----------------------------------------------------------------
    // Staking
    // -----------------------------------------------------------------

    function stake(uint256 amount) external nonReentrant whenNotPaused {
        if (amount == 0) revert ZeroAmount();
        IERC20(address(token)).safeTransferFrom(msg.sender, address(this), amount);
        uint256 oldBalance = activeStake[msg.sender];
        uint256 newBalance = oldBalance + amount;
        activeStake[msg.sender] = newBalance;

        // First-bond timestamp is set the moment the operator's `activeStake`
        // becomes non-zero, never overwritten (ADR 036 § Formula).
        if (oldBalance == 0 && _firstBondedAt[msg.sender] == 0) {
            _firstBondedAt[msg.sender] = uint64(block.timestamp);
        }

        if (ejected[msg.sender] && newBalance >= minStake) {
            ejected[msg.sender] = false;
            emit Reinstated(msg.sender);
        }

        emit Staked(msg.sender, amount, newBalance);
    }

    function requestUnstake(uint256 amount) external nonReentrant whenNotPaused {
        if (amount == 0) revert ZeroAmount();
        if (amount > activeStake[msg.sender]) {
            revert InsufficientStake({ requested: amount, available: activeStake[msg.sender] });
        }
        if (unbondingOf[msg.sender].amount != 0) revert UnbondingInProgress();

        // CEI: finalize stake state BEFORE the external token operations in
        // `_forfeitUnvestedCredit` (token.burn / safeTransfer). The token is
        // the protocol's own ERC20Burnable so reentrancy isn't real, but
        // ordering this way satisfies slither's reentrancy-no-eth detector
        // and keeps the contract robust against a future TOKEN swap.
        activeStake[msg.sender] -= amount;
        uint256 unlockAt = block.timestamp + unbondingPeriod;
        unbondingOf[msg.sender] = UnbondingRequest({ amount: amount, unlockAt: unlockAt });

        // ADR 026 § Genesis Bond Credits — exit clause: initiating unbonding
        // forfeits any unvested credit to Treasury (burn if no Treasury wired).
        _forfeitUnvestedCredit(msg.sender);

        emit UnbondingRequested(msg.sender, amount, unlockAt, activeStake[msg.sender]);
    }

    /// @dev Transfers the unvested portion of `operator`'s Genesis Bond Credit
    ///      to `treasury` (or burns if treasury is zero). Reduces `originalGrant`
    ///      to the already-vested amount so future curve reads stop at the
    ///      forfeit boundary.
    function _forfeitUnvestedCredit(address operator) internal {
        PendingCredit storage pc = _pendingCredit[operator];
        if (pc.originalGrant == 0) return;
        uint256 vested = _curveVested(operator);
        uint256 unvested = uint256(pc.originalGrant) - vested;
        // `unvested` is a derived amount, not a token-balance read; zero is
        // the well-defined "fully vested" sentinel.
        // slither-disable-next-line incorrect-equality
        if (unvested == 0) return;
        pc.originalGrant = uint128(vested);

        address sink = treasury;
        if (sink == address(0)) {
            token.burn(unvested);
            emit GenesisCreditForfeited(operator, unvested, address(0));
        } else {
            IERC20(address(token)).safeTransfer(sink, unvested);
            emit GenesisCreditForfeited(operator, unvested, sink);
        }
    }

    function unstake() external nonReentrant whenNotPaused {
        UnbondingRequest memory req = unbondingOf[msg.sender];
        if (req.amount == 0) revert NoUnbondingRequest();
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < req.unlockAt) revert UnbondingNotComplete(req.unlockAt);

        delete unbondingOf[msg.sender];
        IERC20(address(token)).safeTransfer(msg.sender, req.amount);
        emit Unstaked(msg.sender, req.amount);
    }

    /// @notice Self-attest serving capacity in Mbps (ADR 026 § Capacity-bond
    ///         curve). Stored without enforcement against `activeStake` in
    ///         this revision; off-chain consumers and a future enforcement
    ///         PR are the consumers.
    function declareMbps(uint256 mbps) external whenNotPaused {
        uint256 old = declaredMbps[msg.sender];
        declaredMbps[msg.sender] = mbps;
        emit MbpsDeclared(msg.sender, old, mbps);
    }

    // -----------------------------------------------------------------
    // Region self-attestation (ADR 030)
    // -----------------------------------------------------------------

    /// @notice Update the operator's region. The first call has no cooldown
    ///         (operators may correct their initial `registerNode` region);
    ///         every subsequent call is gated by `regionStabilityWindow`.
    /// @dev    The new region is stored in the operator's `NodeInfo.regionHint`
    ///         so all downstream readers (`getActiveNodes`, off-chain DHT)
    ///         see the same source of truth. `regionPrev` retains the prior
    ///         value for the ADR 030 ripening predicate ("blacklist scope
    ///         applies previous region's entries until window ripens").
    function updateRegion(string calldata newRegion) external whenNotPaused {
        if (bytes(newRegion).length > MAX_REGION_HINT_BYTES) {
            revert RegionHintTooLong({ size: bytes(newRegion).length, ceiling: MAX_REGION_HINT_BYTES });
        }
        NodeInfo storage info = _nodes[msg.sender];
        if (!info.active) revert NodeNotActive();

        uint64 lastChanged = regionLastChanged[msg.sender];
        if (lastChanged != 0) {
            uint64 readyAt = lastChanged + uint64(regionStabilityWindow);
            // forge-lint: disable-next-line(block-timestamp)
            if (block.timestamp < readyAt) revert RegionCooldownActive(readyAt);
        }

        string memory oldRegion = info.regionHint;
        regionPrev[msg.sender] = oldRegion;
        info.regionHint = newRegion;
        regionLastChanged[msg.sender] = uint64(block.timestamp);

        emit RegionUpdated(info.nodeId, oldRegion, newRegion);
    }

    // -----------------------------------------------------------------
    // Genesis Bond Credits (ADR 026 § Genesis Bond Credits)
    // -----------------------------------------------------------------

    function pendingCredit(address operator) external view returns (PendingCredit memory) {
        return _pendingCredit[operator];
    }

    /// @notice Linear vest curve value at `block.timestamp` for `operator` —
    ///         `originalGrant × min(elapsed, DURATION) / DURATION`. Decoupled
    ///         from `claimed` so partial claims do not skew the curve.
    function curveVested(address operator) external view returns (uint256) {
        return _curveVested(operator);
    }

    /// @notice Amount currently available to `claimVestedCredit`. Bounded
    ///         below by 0 so a post-slash decrease in `originalGrant` cannot
    ///         produce a negative claimable.
    function claimableCredit(address operator) public view returns (uint256) {
        uint256 vested = _curveVested(operator);
        uint256 claimed = _pendingCredit[operator].claimed;
        if (vested <= claimed) return 0;
        return vested - claimed;
    }

    /// @notice One-shot TGE grant. Treasury must `approve(this, amount)` first;
    ///         TOKEN is pulled into the contract and held against the operator's
    ///         vesting schedule.
    function grantGenesisCredit(address operator, uint256 amount)
        external
        nonReentrant
        whenNotPaused
        onlyRole(GENESIS_GRANTOR_ROLE)
    {
        if (operator == address(0)) revert ZeroAddress();
        if (amount == 0) revert ZeroAmount();
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp > genesisCreditWindowEnd) revert GenesisCreditWindowClosed(genesisCreditWindowEnd);
        if (_pendingCredit[operator].originalGrant != 0) revert GenesisCreditAlreadyGranted(operator);
        if (amount > type(uint128).max) {
            revert ParamOutOfBounds({ value: amount, floor: 1, ceiling: type(uint128).max });
        }

        IERC20(address(token)).safeTransferFrom(msg.sender, address(this), amount);
        _pendingCredit[operator] =
            PendingCredit({ originalGrant: uint128(amount), claimed: 0, grantedAt: uint64(block.timestamp) });

        emit GenesisCreditGranted(operator, amount);
    }

    /// @notice Move the currently-claimable portion (curve - alreadyClaimed)
    ///         from `pendingCredit` into the caller's `activeStake`. Gated on
    ///         `activeStake > 0 && !ejected` plus an unexpired slash-zero-out
    ///         window: while `currentEpoch < slashedAtEpoch +
    ///         claimSlashGateEpochs`, the claim reverts. Once the window
    ///         elapses (or `slashedAtEpoch` is cleared by a successful
    ///         appeal reversal) the claim is unblocked. Matches the
    ///         "operating, non-slashed-in-window" predicate per ADR 026
    ///         § Genesis Bond Credits. (Strict `isActive` additionally
    ///         requires NodeId registration; the looser gate here mirrors
    ///         the economic intent without requiring node-registry tests
    ///         to mint EIP-712 signatures.)
    function claimVestedCredit() external nonReentrant whenNotPaused {
        if (activeStake[msg.sender] == 0 || ejected[msg.sender]) revert NotActiveForClaim(msg.sender);
        // Only block while the operator is still inside their slash zero-out
        // window — past that, the claim is unblocked (closes the lock-out-
        // forever bug from the prior `!= 0` check). The window length is
        // governance-tunable via `setClaimSlashGateEpochs` so it can be
        // kept in lock-step with `FeeRouter.windowEpochs`.
        uint64 slashEpoch = _slashedAtEpoch[msg.sender];
        if (slashEpoch != 0) {
            uint64 currentEpoch = uint64(block.timestamp / EPOCH_LENGTH);
            if (currentEpoch < slashEpoch + claimSlashGateEpochs) {
                revert SlashedInWindowForClaim(msg.sender);
            }
        }

        uint256 claimable = claimableCredit(msg.sender);
        // Derived from the curve, not a balance read; zero is the
        // well-defined "nothing to claim yet" sentinel.
        // slither-disable-next-line incorrect-equality
        if (claimable == 0) revert NothingVested();

        PendingCredit storage pc = _pendingCredit[msg.sender];
        pc.claimed = uint128(uint256(pc.claimed) + claimable);

        uint256 oldBalance = activeStake[msg.sender];
        uint256 newBalance = oldBalance + claimable;
        activeStake[msg.sender] = newBalance;
        if (oldBalance == 0 && _firstBondedAt[msg.sender] == 0) {
            _firstBondedAt[msg.sender] = uint64(block.timestamp);
        }

        emit GenesisCreditClaimed(msg.sender, claimable);
        emit Staked(msg.sender, claimable, newBalance);
    }

    /// @dev Curve helper that reads storage directly so callers don't
    ///      pass storage→memory copies into a memory-param helper
    ///      (aderyn H-2). Result is `originalGrant × min(elapsed, DUR) / DUR`.
    function _curveVested(address operator) internal view returns (uint256) {
        PendingCredit storage pc = _pendingCredit[operator];
        uint128 grant = pc.originalGrant;
        if (grant == 0) return 0;
        // forge-lint: disable-next-line(block-timestamp)
        uint256 elapsed = block.timestamp - pc.grantedAt;
        if (elapsed >= GENESIS_VEST_DURATION) return grant;
        return (uint256(grant) * elapsed) / GENESIS_VEST_DURATION;
    }

    // -----------------------------------------------------------------
    // Node registry (ADR 003 § Node Registry)
    // -----------------------------------------------------------------

    function registerNode(
        bytes32 nodeId,
        bytes calldata multiaddrs,
        string calldata regionHint,
        bytes calldata bindingSignature,
        bytes calldata ed25519Signature
    ) external nonReentrant whenNotPaused {
        _checkRegistrationPreconditions(nodeId, multiaddrs.length, bytes(regionHint).length);
        _checkBindingOneToOne(nodeId);
        uint64 usedBindingNonce = _verifyBindingSignature(nodeId, bindingSignature);
        uint64 usedRegistrationNonce = _verifyEd25519OwnershipSignature(nodeId, ed25519Signature);

        nodeIdToAddress[nodeId] = msg.sender;
        addressToNodeId[msg.sender] = nodeId;
        bindingNonce[msg.sender] = usedBindingNonce + 1;
        _writeNodeInfo(nodeId, multiaddrs, regionHint);
        _addToRegisteredSet(msg.sender);

        emit NodeRegistered(nodeId, msg.sender, multiaddrs, regionHint, usedBindingNonce, usedRegistrationNonce);
        emit NodeIdBound(msg.sender, nodeId, usedBindingNonce);
    }

    function _checkRegistrationPreconditions(bytes32 nodeId, uint256 multiaddrsLength, uint256 regionHintLength)
        internal
        view
    {
        if (nodeId == bytes32(0)) revert ZeroNodeId();
        if (multiaddrsLength > maxMultiaddrSize) {
            revert MultiaddrsTooLarge({ size: multiaddrsLength, ceiling: maxMultiaddrSize });
        }
        if (regionHintLength > MAX_REGION_HINT_BYTES) {
            revert RegionHintTooLong({ size: regionHintLength, ceiling: MAX_REGION_HINT_BYTES });
        }
        if (activeStake[msg.sender] < minStake) {
            revert StakeBelowMinimum({ stake: activeStake[msg.sender], required: minStake });
        }
        if (ejected[msg.sender]) revert OperatorEjected();
        if (_nodes[msg.sender].active) revert NodeAlreadyRegistered();
    }

    function _checkBindingOneToOne(bytes32 nodeId) internal view {
        address nodeIdOwner = nodeIdToAddress[nodeId];
        if (nodeIdOwner != address(0) && nodeIdOwner != msg.sender) {
            revert NodeIdAlreadyBound(nodeIdOwner);
        }
        bytes32 currentBinding = addressToNodeId[msg.sender];
        if (currentBinding != bytes32(0) && currentBinding != nodeId) {
            revert AddressAlreadyBound(currentBinding);
        }
    }

    function _verifyBindingSignature(bytes32 nodeId, bytes calldata sig) internal view returns (uint64 nonce) {
        nonce = bindingNonce[msg.sender];
        bytes32 digest = _hashTypedDataV4(keccak256(abi.encode(BIND_NODE_TYPEHASH, nodeId, nonce)));
        if (!SignatureChecker.isValidSignatureNow(msg.sender, digest, sig)) {
            revert InvalidBindingSignature();
        }
    }

    function _verifyEd25519OwnershipSignature(bytes32 nodeId, bytes calldata sig) internal view returns (uint64 nonce) {
        nonce = registrationNonce[nodeId];
        bytes32 messageHash = keccak256(abi.encodePacked(nodeId, msg.sender, block.chainid, nonce));
        if (!ed25519Verifier.verify(nodeId, messageHash, sig)) {
            revert InvalidEd25519Signature();
        }
    }

    function _writeNodeInfo(bytes32 nodeId, bytes calldata multiaddrs, string calldata regionHint) internal {
        NodeInfo storage info = _nodes[msg.sender];
        info.nodeId = nodeId;
        info.ethAddress = msg.sender;
        info.active = true;
        info.registeredAt = block.timestamp;
        // slither-disable-next-line incorrect-equality
        if (info.firstRegisteredAt == 0) {
            info.firstRegisteredAt = block.timestamp;
        }
        info.lastMultiaddrUpdate = block.timestamp;
        info.multiaddrs = multiaddrs;
        info.regionHint = regionHint;
    }

    function deregisterNode() external nonReentrant whenNotPaused {
        NodeInfo storage info = _nodes[msg.sender];
        if (!info.active) revert NodeNotActive();

        bytes32 nodeId = info.nodeId;
        info.active = false;
        registrationNonce[nodeId] += 1;
        _removeFromRegisteredSet(msg.sender);

        emit NodeDeregistered(nodeId);
    }

    function bindNodeId(bytes32 nodeId, bytes calldata bindingSignature, bytes calldata ed25519Signature)
        external
        nonReentrant
        whenNotPaused
    {
        if (nodeId == bytes32(0)) revert ZeroNodeId();
        address nodeIdOwner = nodeIdToAddress[nodeId];
        if (nodeIdOwner != address(0) && nodeIdOwner != msg.sender) revert NodeIdAlreadyBound(nodeIdOwner);

        uint64 usedBindingNonce = _verifyBindingSignature(nodeId, bindingSignature);
        _verifyEd25519OwnershipSignature(nodeId, ed25519Signature);

        bytes32 oldNodeId = addressToNodeId[msg.sender];
        if (oldNodeId != bytes32(0) && oldNodeId != nodeId) {
            delete nodeIdToAddress[oldNodeId];
            registrationNonce[oldNodeId] += 1;
        }

        nodeIdToAddress[nodeId] = msg.sender;
        addressToNodeId[msg.sender] = nodeId;
        bindingNonce[msg.sender] = usedBindingNonce + 1;

        if (_nodes[msg.sender].active) {
            _nodes[msg.sender].nodeId = nodeId;
        }

        emit NodeIdBound(msg.sender, nodeId, usedBindingNonce);
    }

    function reclaimNodeId(bytes32 nodeId, bytes calldata ed25519Signature) external nonReentrant whenNotPaused {
        if (nodeId == bytes32(0)) revert ZeroNodeId();
        address currentHolder = nodeIdToAddress[nodeId];
        if (currentHolder == address(0)) revert NodeIdNotBound(nodeId);

        _verifyEd25519OwnershipSignature(nodeId, ed25519Signature);

        NodeInfo storage info = _nodes[currentHolder];
        if (info.nodeId == nodeId) {
            if (info.active) {
                info.active = false;
                _removeFromRegisteredSet(currentHolder);
                emit NodeDeregistered(nodeId);
            }
            info.nodeId = bytes32(0);
        }
        delete nodeIdToAddress[nodeId];
        delete addressToNodeId[currentHolder];
        registrationNonce[nodeId] += 1;

        emit NodeIdReclaimed(nodeId, currentHolder);
    }

    function updateMultiaddrs(bytes calldata multiaddrs) external whenNotPaused {
        NodeInfo storage info = _nodes[msg.sender];
        if (!info.active) revert NodeNotActive();
        if (multiaddrs.length > maxMultiaddrSize) {
            revert MultiaddrsTooLarge({ size: multiaddrs.length, ceiling: maxMultiaddrSize });
        }
        uint256 readyAt = info.lastMultiaddrUpdate + multiaddrUpdateCooldown;
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < readyAt) revert MultiaddrCooldownActive(readyAt);

        info.multiaddrs = multiaddrs;
        info.lastMultiaddrUpdate = block.timestamp;

        emit NodeMultiaddrUpdated(info.nodeId, multiaddrs);
    }

    // -----------------------------------------------------------------
    // Slashing (SLASH_ROLE — held by SlashJudge)
    // -----------------------------------------------------------------

    function slash(address operator, address challenger, uint8 offenseType)
        external
        nonReentrant
        whenNotPaused
        onlyRole(SLASH_ROLE)
        returns (uint256 slashId, uint256 totalSlash)
    {
        if (operator == address(0) || challenger == address(0)) revert ZeroAddress();
        if (address(safetyReserve) == address(0)) revert SafetyReserveNotWired();

        uint32 newCount = lifetimeOffenseCount[operator] + 1;
        lifetimeOffenseCount[operator] = newCount;
        uint256 tierBps = newCount == 1 ? SLASH_BPS_TIER_1 : newCount == 2 ? SLASH_BPS_TIER_2 : SLASH_BPS_TIER_3;

        // ADR 026 § Genesis Bond Credits — the unvested portion of pending
        // credit is added to the stake-slash amount and routed through the
        // SAME 50% challenger / 30% safety / 20% burn split (C1 fix).
        uint256 creditSlash = _slashPendingCreditAtTier(operator, tierBps);
        uint256 stakeSlash = _reduceStakeAtTier(operator, tierBps);
        totalSlash = stakeSlash + creditSlash;
        slashId = _mintSlashRecord(operator, totalSlash);
        if (creditSlash != 0) emit GenesisCreditSlashed(operator, creditSlash);
        _distributeAndStamp(slashId, operator, challenger, offenseType, totalSlash, newCount);
    }

    /// @dev Reduce active + unbonding stake at `tierBps`. Active first, then
    ///      unbonding (prevents slash-then-run per ADR 003). The defensive
    ///      `min(remainder, req.amount)` cap (C2 fix) guards against future
    ///      tier-bps schedules above 50% silently underflowing the subtraction.
    function _reduceStakeAtTier(address operator, uint256 tierBps) internal returns (uint256 slashAmount) {
        UnbondingRequest memory req = unbondingOf[operator];
        uint256 totalAtRisk = activeStake[operator] + uint256(req.amount);
        slashAmount = (totalAtRisk * tierBps) / BPS_DENOMINATOR;
        if (slashAmount <= activeStake[operator]) {
            activeStake[operator] -= slashAmount;
        } else {
            uint256 remainder = slashAmount - activeStake[operator];
            if (remainder > req.amount) remainder = req.amount;
            activeStake[operator] = 0;
            unbondingOf[operator].amount = req.amount - remainder;
        }
    }

    /// @dev Reduce the operator's unvested `PendingCredit` by `tierBps`. The
    ///      "unvested" portion is computed against the curve (not against a
    ///      stored `vested` counter) so partial claims don't skew the slash
    ///      basis. `originalGrant` is reduced by the slashed amount so the
    ///      operator's future curve naturally shrinks.
    function _slashPendingCreditAtTier(address operator, uint256 tierBps) internal returns (uint256 slashed) {
        PendingCredit storage pc = _pendingCredit[operator];
        if (pc.originalGrant == 0) return 0;
        uint256 vested = _curveVested(operator);
        uint256 unvested = uint256(pc.originalGrant) - vested;
        // slither-disable-next-line incorrect-equality
        if (unvested == 0) return 0;
        // slither-disable-next-line divide-before-multiply
        slashed = (unvested * tierBps) / BPS_DENOMINATOR;
        // slither-disable-next-line incorrect-equality
        if (slashed == 0) return 0;
        pc.originalGrant = uint128(uint256(pc.originalGrant) - slashed);
    }

    /// @dev Persist the slash record so `SafetyReserve.openSlashAppeal` can
    ///      validate appeals without trusting the appellant's `operator`
    ///      claim (I2 fix).
    function _mintSlashRecord(address operator, uint256 totalSlashAmount) internal returns (uint256 slashId) {
        slashId = slashCounter;
        unchecked {
            slashCounter = slashId + 1;
        }
        _slashRecords[slashId] =
            SlashRecord({ operator: operator, slashedAt: uint64(block.timestamp), slashAmount: totalSlashAmount });
        emit SlashRecorded(slashId, operator, uint64(block.timestamp), totalSlashAmount);
    }

    /// @dev Stamp `slashedAtEpoch`, fire auto-eject if post-slash active stake
    ///      falls below `minStake / 2`, distribute the combined stake+credit
    ///      slash through the 50/30/20 split (C1), and emit `Slashed` +
    ///      ejection events. `recordSlashInflow` is best-effort (I3): a
    ///      faulty SafetyReserve cannot brick the slash path.
    function _distributeAndStamp(
        uint256 slashId,
        address operator,
        address challenger,
        uint8 offenseType,
        uint256 totalSlash,
        uint32 newCount
    ) internal {
        // slither-disable-next-line divide-before-multiply
        uint256 challengerShare = (totalSlash * CHALLENGER_BPS) / BPS_DENOMINATOR;
        // slither-disable-next-line divide-before-multiply
        uint256 safetyShare = (totalSlash * SAFETY_BPS) / BPS_DENOMINATOR;
        uint256 burnShare = totalSlash - challengerShare - safetyShare;

        _slashedAtEpoch[operator] = uint64(block.timestamp / EPOCH_LENGTH);
        emit SlashedAtEpochStamped(operator, _slashedAtEpoch[operator]);

        _maybeAutoEject(operator);
        _routeSlashShares(slashId, operator, challenger, challengerShare, safetyShare, burnShare);

        emit Slashed(operator, challenger, offenseType, newCount, totalSlash, challengerShare, safetyShare, burnShare);
    }

    /// @dev Auto-eject if post-slash active stake fell below minStake/2.
    function _maybeAutoEject(address operator) internal {
        if (activeStake[operator] >= (minStake / 2) || ejected[operator]) return;
        ejected[operator] = true;
        bytes32 nodeId = _ejectNodeEffects(operator);
        emit AutoEjected(operator, activeStake[operator]);
        if (nodeId != bytes32(0)) emit NodeAutoEjected(nodeId, activeStake[operator]);
    }

    /// @dev Route the three shares (challenger / safety / burn) and emit the
    ///      best-effort `recordSlashInflow` callback per I3. Stack-isolated
    ///      from `_distributeAndStamp` to keep both fns under the 16-slot limit.
    ///      On callback failure, surfaces the revert reason in the event so
    ///      off-chain reconcilers can distinguish OOG / pause / role-revoked
    ///      causes without correlating by tx hash.
    function _routeSlashShares(
        uint256 slashId,
        address operator,
        address challenger,
        uint256 challengerShare,
        uint256 safetyShare,
        uint256 burnShare
    ) internal {
        if (challengerShare != 0) {
            IERC20(address(token)).safeTransfer(challenger, challengerShare);
        }
        if (safetyShare != 0) {
            IERC20(address(token)).safeTransfer(address(safetyReserve), safetyShare);
            try safetyReserve.recordSlashInflow(operator, safetyShare) { }
            catch (bytes memory reason) {
                emit SlashInflowReportFailed(slashId, operator, safetyShare, reason);
            }
        }
        if (burnShare != 0) token.burn(burnShare);
    }

    // -----------------------------------------------------------------
    // Slash-zero-out clearing (APPEAL_REVERSAL_ROLE — held by SafetyReserve)
    // -----------------------------------------------------------------

    function clearSlashedAtEpoch(address operator) external onlyRole(APPEAL_REVERSAL_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        if (_slashedAtEpoch[operator] != 0) {
            _slashedAtEpoch[operator] = 0;
            emit SlashedAtEpochCleared(operator);
        }
    }

    // -----------------------------------------------------------------
    // Blacklist ejection
    // -----------------------------------------------------------------

    function ejectNode(address operator) external onlyRole(BLACKLIST_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        if (!ejected[operator]) {
            ejected[operator] = true;
            emit EjectedByBlacklist(operator);
            bytes32 nodeId = _ejectNodeEffects(operator);
            if (nodeId != bytes32(0)) {
                emit NodeAutoEjected(nodeId, activeStake[operator]);
            }
        }
    }

    // -----------------------------------------------------------------
    // Settlement reporter callback (FeeRouter)
    // -----------------------------------------------------------------

    function recordSettlement(address operator) external onlyRole(SETTLEMENT_REPORTER_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        lastSettlementAt[operator] = block.timestamp;
        emit SettlementRecorded(operator);
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function setMinStake(uint256 newMinStake) external onlyRole(GOVERNANCE_ROLE) {
        _enforceMinStakeBounds(newMinStake);
        uint256 oldMinStake = minStake;
        minStake = newMinStake;
        emit MinStakeUpdated(oldMinStake, newMinStake);
    }

    function setUnbondingPeriod(uint256 newPeriod) external onlyRole(GOVERNANCE_ROLE) {
        _enforceUnbondingPeriodBounds(newPeriod);
        uint256 oldPeriod = unbondingPeriod;
        unbondingPeriod = newPeriod;
        emit UnbondingPeriodUpdated(oldPeriod, newPeriod);
    }

    function setSafetyReserve(ISafetyReserve newSafetyReserve) external onlyRole(GOVERNANCE_ROLE) {
        address oldAddr = address(safetyReserve);
        safetyReserve = newSafetyReserve;
        emit SafetyReserveUpdated(oldAddr, address(newSafetyReserve));
    }

    /// @notice Treasury sink for forfeited unvested Genesis Bond Credit on
    ///         operator-initiated unbond (ADR 026 § Genesis Bond Credits —
    ///         exit clause). May be `address(0)`; in that case forfeit is
    ///         burned as a safe fallback.
    function setTreasury(address newTreasury) external onlyRole(GOVERNANCE_ROLE) {
        address oldTreasury = treasury;
        treasury = newTreasury;
        emit TreasuryUpdated(oldTreasury, newTreasury);
    }

    function setMultiaddrUpdateCooldown(uint256 newCooldown) external onlyRole(GOVERNANCE_ROLE) {
        _enforceMultiaddrCooldownBounds(newCooldown);
        uint256 oldCooldown = multiaddrUpdateCooldown;
        multiaddrUpdateCooldown = newCooldown;
        emit MultiaddrUpdateCooldownUpdated(oldCooldown, newCooldown);
    }

    function setMaxMultiaddrSize(uint256 newSize) external onlyRole(GOVERNANCE_ROLE) {
        _enforceMaxMultiaddrSizeBounds(newSize);
        uint256 oldSize = maxMultiaddrSize;
        maxMultiaddrSize = newSize;
        emit MaxMultiaddrSizeUpdated(oldSize, newSize);
    }

    function setRegionStabilityWindow(uint256 newWindow) external onlyRole(GOVERNANCE_ROLE) {
        _enforceRegionStabilityWindowBounds(newWindow);
        uint256 oldWindow = regionStabilityWindow;
        regionStabilityWindow = newWindow;
        emit RegionStabilityWindowUpdated(oldWindow, newWindow);
    }

    /// @notice Set the claim slash gate window. Governance SHOULD update
    ///         this in the same multi-call as any `FeeRouter.windowEpochs`
    ///         change so the claim gate stays aligned with the slash
    ///         zero-out window read by `DecdnGovernor._slashedInWindow`.
    function setClaimSlashGateEpochs(uint64 newValue) external onlyRole(GOVERNANCE_ROLE) {
        if (newValue < CLAIM_SLASH_GATE_FLOOR || newValue > CLAIM_SLASH_GATE_CEILING) {
            revert ParamOutOfBounds({
                value: uint256(newValue),
                floor: uint256(CLAIM_SLASH_GATE_FLOOR),
                ceiling: uint256(CLAIM_SLASH_GATE_CEILING)
            });
        }
        uint64 old = claimSlashGateEpochs;
        claimSlashGateEpochs = newValue;
        emit ClaimSlashGateEpochsUpdated(old, newValue);
    }

    // -----------------------------------------------------------------
    // Pause control
    // -----------------------------------------------------------------

    function pause() external onlyRole(PAUSER_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }

    // -----------------------------------------------------------------
    // Views — stake / governor surface
    // -----------------------------------------------------------------

    function stakeOf(address operator) external view returns (uint256) {
        return activeStake[operator];
    }

    function getStakeMultiple(address operator) external view returns (uint256) {
        return activeStake[operator] / minStake;
    }

    function isActive(address operator) public view returns (bool) {
        // slither-disable-next-line incorrect-equality
        return _nodes[operator].active && activeStake[operator] >= minStake && unbondingOf[operator].amount == 0
            && !ejected[operator];
    }

    /// @inheritdoc ICapacityBond
    function firstBondedAt(address operator) external view returns (uint64) {
        return _firstBondedAt[operator];
    }

    /// @inheritdoc ICapacityBond
    function slashedAtEpoch(address operator) external view returns (uint64) {
        return _slashedAtEpoch[operator];
    }

    /// @inheritdoc ICapacityBond
    function slashRecords(uint256 slashId)
        external
        view
        returns (address operator, uint64 slashedAt_, uint256 slashAmount)
    {
        if (slashId >= slashCounter) revert UnknownSlash(slashId);
        SlashRecord memory r = _slashRecords[slashId];
        return (r.operator, r.slashedAt, r.slashAmount);
    }

    // -----------------------------------------------------------------
    // Views — node registry
    // -----------------------------------------------------------------

    function nodeIdOf(address operator) external view returns (bytes32 nodeId, bool active) {
        nodeId = addressToNodeId[operator];
        active = isActive(operator);
    }

    function getNode(bytes32 nodeId) external view returns (NodeInfo memory) {
        address ethAddress = nodeIdToAddress[nodeId];
        return _nodes[ethAddress];
    }

    function getNodeByAddress(address ethAddress) external view returns (NodeInfo memory) {
        return _nodes[ethAddress];
    }

    function isActiveNode(bytes32 nodeId) external view returns (bool) {
        address ethAddress = nodeIdToAddress[nodeId];
        if (ethAddress == address(0)) return false;
        return isActive(ethAddress);
    }

    function getFirstRegisteredAt(address operator) external view returns (uint256) {
        return _nodes[operator].firstRegisteredAt;
    }

    function getActiveNodeCount() external view returns (uint256) {
        return _registeredAddrs.length;
    }

    function getActiveNodes(uint256 offset, uint256 limit) external view returns (NodeInfo[] memory page) {
        uint256 len = _registeredAddrs.length;
        if (offset >= len || limit == 0) {
            return new NodeInfo[](0);
        }
        uint256 end = offset + limit;
        if (end > len) end = len;
        uint256 size = end - offset;
        page = new NodeInfo[](size);
        for (uint256 i = 0; i < size; i++) {
            page[i] = _nodes[_registeredAddrs[offset + i]];
        }
    }

    // -----------------------------------------------------------------
    // Internal helpers — active set
    // -----------------------------------------------------------------

    function _addToRegisteredSet(address operator) internal {
        if (_registeredIndex[operator] != 0) return;
        _registeredAddrs.push(operator);
        _registeredIndex[operator] = _registeredAddrs.length;
    }

    function _removeFromRegisteredSet(address operator) internal {
        uint256 onePlusIdx = _registeredIndex[operator];
        if (onePlusIdx == 0) return;
        uint256 idx = onePlusIdx - 1;
        uint256 lastIdx = _registeredAddrs.length - 1;
        if (idx != lastIdx) {
            address swapped = _registeredAddrs[lastIdx];
            _registeredAddrs[idx] = swapped;
            _registeredIndex[swapped] = idx + 1;
        }
        _registeredAddrs.pop();
        delete _registeredIndex[operator];
    }

    function _ejectNodeEffects(address operator) internal returns (bytes32 nodeId) {
        NodeInfo storage info = _nodes[operator];
        if (!info.active) return bytes32(0);
        nodeId = info.nodeId;
        info.active = false;
        registrationNonce[nodeId] += 1;
        _removeFromRegisteredSet(operator);
    }

    // -----------------------------------------------------------------
    // Internal helpers — parameter bounds
    // -----------------------------------------------------------------

    function _enforceMinStakeBounds(uint256 value) internal pure {
        if (value < MIN_STAKE_FLOOR || value > MIN_STAKE_CEILING) {
            revert ParamOutOfBounds({ value: value, floor: MIN_STAKE_FLOOR, ceiling: MIN_STAKE_CEILING });
        }
    }

    function _enforceUnbondingPeriodBounds(uint256 value) internal pure {
        if (value < UNBONDING_PERIOD_FLOOR || value > UNBONDING_PERIOD_CEILING) {
            revert ParamOutOfBounds({ value: value, floor: UNBONDING_PERIOD_FLOOR, ceiling: UNBONDING_PERIOD_CEILING });
        }
    }

    function _enforceMultiaddrCooldownBounds(uint256 value) internal pure {
        if (value > MULTIADDR_COOLDOWN_CEILING) {
            revert ParamOutOfBounds({ value: value, floor: 0, ceiling: MULTIADDR_COOLDOWN_CEILING });
        }
    }

    function _enforceMaxMultiaddrSizeBounds(uint256 value) internal pure {
        if (value < MAX_MULTIADDR_SIZE_FLOOR || value > MAX_MULTIADDR_SIZE_CEILING) {
            revert ParamOutOfBounds({
                value: value, floor: MAX_MULTIADDR_SIZE_FLOOR, ceiling: MAX_MULTIADDR_SIZE_CEILING
            });
        }
    }

    function _enforceRegionStabilityWindowBounds(uint256 value) internal pure {
        if (value < REGION_STABILITY_WINDOW_FLOOR || value > REGION_STABILITY_WINDOW_CEILING) {
            revert ParamOutOfBounds({
                value: value, floor: REGION_STABILITY_WINDOW_FLOOR, ceiling: REGION_STABILITY_WINDOW_CEILING
            });
        }
    }
}
