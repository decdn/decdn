// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { SunsettingPausable } from "./SunsettingPausable.sol";
import { EIP712 } from "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import { SignatureChecker } from "@openzeppelin/contracts/utils/cryptography/SignatureChecker.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { Checkpoints } from "@openzeppelin/contracts/utils/structs/Checkpoints.sol";

import { ICapacityBond } from "./interfaces/ICapacityBond.sol";
import { ICapacityBondEjector } from "./interfaces/ICapacityBondEjector.sol";
import { ICapacityBondEpoch } from "./interfaces/ICapacityBondEpoch.sol";
import { ICapacityBondRegionView } from "./interfaces/ICapacityBondRegionView.sol";
import { IEd25519Verifier } from "./interfaces/IEd25519Verifier.sol";
import { BondMath } from "./BondMath.sol";
import { DeclaredMbpsHistoryLib } from "./DeclaredMbpsHistoryLib.sol";

/// @title CapacityBond — operator-registry contract
/// @notice Custodies operator TOKEN bond, exposes `epochLength()` so
///         `FeeRouter` can prove at deploy time that both contracts agree on
///         epoch length, is the registry for iroh-NodeId ↔ Ethereum-address
///         bindings, and is the source of `firstBondedAt` / `declaredMbpsAtEpoch`
///         for `DecdnGovernor`'s served-bytes voting weight per ADR 036.
///         An operator withdraws bond in one `unbond` call, which pays the
///         TOKEN out at once; the bond backs the declared capacity tier
///         (ADR 026 § Capacity-bond curve) and the entry floor.
/// @dev    The bond / unbond / node-registry primitives, plus:
///           - `firstBondedAt[op]`  — tenure start, stamped at entry and reset by
///                                    an `unbond` below the floor (ADR 036)
///           - `regionPrev[op]` + `regionLastChanged[op]` + `updateRegion`
///                                    (ADR 030 § Node Region Self-Attestation)
///           - `declaredMbps[op]`   — operator-asserted capacity (ADR 026
///                                    § Capacity-bond curve), set via
///                                    `declareMbps` within the governable
///                                    `[minCapacityMbps, maxCapacityMbps]`
///                                    band; the bond-curve coupling
///                                    `activeBond ≥ k × Mbps^α` is enforced at
///                                    `declareMbps` / `unbond` /
///                                    `registerNode`, with `k`/`α`
///                                    governance-tunable via `setK` / `setAlpha`
contract CapacityBond is
    ICapacityBond,
    ICapacityBondEjector,
    ICapacityBondEpoch,
    ICapacityBondRegionView,
    AccessControl,
    ReentrancyGuard,
    SunsettingPausable,
    EIP712
{
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant BLACKLIST_ROLE = keccak256("BLACKLIST_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    // -----------------------------------------------------------------
    // Governable-parameter safety bounds
    // -----------------------------------------------------------------

    uint256 internal constant MIN_BOND_FLOOR = 10_000e18;
    uint256 internal constant MIN_BOND_CEILING = 1_000_000e18;

    /// @notice Bounds on the governable declared-capacity band per ADR 026
    ///         § Capacity-bond curve, expressed in Mbps. `minCapacityMbps`
    ///         ∈ [10, 1000] (default 10 Mbps) bars sub-floor dust
    ///         declarations; `maxCapacityMbps` ∈ [50_000, 1_000_000] (default
    ///         200 Gbps) caps per-operator declared capacity. The two ranges
    ///         are disjoint — `MIN_CAPACITY_CEILING_MBPS (1000) <
    ///         MAX_CAPACITY_FLOOR_MBPS (50_000)` — so the floor is always
    ///         strictly below the ceiling without a cross-parameter check.
    ///         INVARIANT: keep the ranges disjoint; relaxing either into
    ///         overlap silently breaks that guarantee. Guarded by
    ///         `test_capacityBand_floorAlwaysBelowCeiling`.
    uint256 internal constant MIN_CAPACITY_FLOOR_MBPS = 10;
    uint256 internal constant MIN_CAPACITY_CEILING_MBPS = 1000;
    uint256 internal constant MAX_CAPACITY_FLOOR_MBPS = 50_000;
    uint256 internal constant MAX_CAPACITY_CEILING_MBPS = 1_000_000;

    /// @notice Bounds on the governable capacity-bond curve (ADR 026
    ///         § Capacity-bond curve). α is hard-bounded to [1.0, 1.8] in 1e18
    ///         fixed point: 1.0 = linear (no decentralization pressure), 1.8 =
    ///         strong concentration penalty. `k` is bounded indirectly — the
    ///         setters require the resulting 1-Gbps-tier bond
    ///         `bondRequired(1000)` to stay within [10K, 200K TOKEN], which
    ///         co-bounds `k` against the live α (default k = 12.6 TOKEN gives
    ///         ~50K TOKEN at 1 Gbps).
    uint256 internal constant ALPHA_FLOOR = 1e18;
    uint256 internal constant ALPHA_CEILING = 1.8e18;
    uint256 internal constant ONE_GBPS_MBPS = 1000;
    uint256 internal constant ONE_GBPS_BOND_FLOOR = 10_000e18;
    uint256 internal constant ONE_GBPS_BOND_CEILING = 200_000e18;

    uint256 internal constant MULTIADDR_COOLDOWN_CEILING = 1 days;

    uint256 internal constant MAX_MULTIADDR_SIZE_FLOOR = 64;
    uint256 internal constant MAX_MULTIADDR_SIZE_CEILING = 1024;

    uint256 internal constant MAX_REGION_HINT_BYTES = 16;

    /// @notice Bounds on `regionStabilityWindow` per ADR 030 § Cooldown.
    uint256 internal constant REGION_STABILITY_WINDOW_FLOOR = 3 days;
    uint256 internal constant REGION_STABILITY_WINDOW_CEILING = 30 days;

    /// @notice Canonical epoch length shared with `FeeRouter` (ADR 026 §
    ///         FeeRouter split, ADR 036 § Formula). Stored as a constant
    ///         on this contract so `declaredMbpsAtEpoch` derives the same epoch
    ///         index `FeeRouter` writes into `bytesPerEpoch`. Exposed via the
    ///         lowercase `epochLength()` accessor so `FeeRouter`'s
    ///         constructor can assert equality against the value the
    ///         deployer passes for `epochLength_` (closes the silent
    ///         epoch-mis-anchor footgun where `DecdnGovernor` would cap one
    ///         epoch's bytes with another epoch's declared capacity).
    uint64 public constant EPOCH_LENGTH = 7 days;

    /// @notice Lowercase accessor for `EPOCH_LENGTH` — exists so
    ///         `ICapacityBondEpoch` can declare it without tripping
    ///         solhint `func-name-mixedcase` on the SCREAMING_SNAKE auto-getter.
    function epochLength() external pure override returns (uint64) {
        return EPOCH_LENGTH;
    }

    // -----------------------------------------------------------------
    // EIP-712 typehashes
    // -----------------------------------------------------------------

    bytes32 public constant BIND_NODE_TYPEHASH = keccak256("BindNodeId(bytes32 nodeId,uint64 nonce)");

    // ADR 019 § Terms Acceptance — `registerNode` carries the operator's
    // acceptance of the current operator terms inside the binding signature.
    // Registration signs this distinct payload (not `BIND_NODE_TYPEHASH`): the
    // acceptance is bound to the exact `termsHash`, and rebinding via
    // `bindNodeId` (key rotation) stays on `BIND_NODE_TYPEHASH` with no terms
    // re-acceptance, per ADR 019 § "enforcement at registration only".
    bytes32 public constant REGISTER_NODE_TYPEHASH =
        keccak256("RegisterNode(bytes32 nodeId,uint64 nonce,bytes32 termsHash)");

    // -----------------------------------------------------------------
    // Immutable wiring
    // -----------------------------------------------------------------

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ERC20Burnable public immutable token;
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IEd25519Verifier public immutable ed25519Verifier;

    // -----------------------------------------------------------------
    // Storage — bonding
    // -----------------------------------------------------------------

    mapping(address operator => uint256 amount) public activeBond;
    mapping(address operator => bool) public ejected;

    /// @notice Governance-blacklist ejection latch (ADR 011 § Hash Evasion and
    ///         Origin Blacklisting). Set by the `BLACKLIST_ROLE` `ejectNode`
    ///         path together with `ejected`: while set, `bond()` MUST NOT clear
    ///         `ejected`, so a blacklisted operator cannot self-reinstate by
    ///         re-bonding. Cleared only by `unEjectNode` when governance lifts
    ///         the blacklist.
    mapping(address operator => bool) public blacklistEjected;

    /// @notice Tenure start per operator (ADR 036 § Formula — `age_ramp`
    ///         numerator): the timestamp of the entry transition that began the
    ///         operator's current run of bonding at or above its floor. An entry
    ///         is the `bond` call that lifts `activeBond` to the live `minBond`,
    ///         or `registerNode`. A running tenure keeps its stamp through later
    ///         bonds, partial unbonds that hold the floor, and `deregisterNode`.
    ///         An `unbond` that leaves `activeBond` below
    ///         `bondFloorAtActivation` resets it to 0. Zero while the operator
    ///         holds no tenure.
    /// @dev    Invariants: `_firstBondedAt[op] != 0` iff
    ///         `bondFloorAtActivation[op] != 0`, and then
    ///         `activeBond[op] >= bondFloorAtActivation[op]`.
    mapping(address operator => uint64) internal _firstBondedAt;

    /// @notice Operator-asserted serving capacity in Mbps (ADR 026
    ///         § Capacity-bond curve). `declareMbps` enforces the governable
    ///         `[minCapacityMbps, maxCapacityMbps]` band, except that an
    ///         INACTIVE operator may declare 0 to release the tier (see the
    ///         implementation note below); the bond-curve
    ///         coupling `activeBond ≥ bondRequired(declaredMbps)` is enforced
    ///         at every site that changes the bond or the tier (`declareMbps`,
    ///         `unbond`, `registerNode`).
    /// @dev   Invariant: every write to this mapping emits `MbpsDeclared`.
    ///         There are exactly two routes back to 0, split by whether the
    ///         operator is active: `deregisterNode` (active — it reverts
    ///         `NodeNotActive` otherwise) and `declareMbps(0)` (inactive — the
    ///         one sub-band value it accepts, and only from an inactive
    ///         caller). Both are needed: a tier declared without registering,
    ///         or left standing by an ejection, has no `deregisterNode` to
    ///         reach, and would otherwise keep pinning `bondRequired(mbps)` of
    ///         the bond forever. See ADR 026 § Capacity-bond curve and issue
    ///         #1361.
    mapping(address operator => uint256) public declaredMbps;

    /// @notice Per-operator history of `declaredMbps`, keyed by declaration
    ///         timestamp. `declaredMbpsAtEpoch` reads it at each epoch boundary
    ///         so a later re-declaration cannot retroactively raise the vote
    ///         cap for epochs already elapsed (ADR 036 § Formula). The live
    ///         `declaredMbps` mapping remains the value the curve gates read.
    mapping(address operator => Checkpoints.Trace208 history) internal _declaredMbpsHistory;

    /// @notice Global minimum active bond that gates ENTRY into the active set
    ///         (ADR 026 § Operator bond). `minBond` is non-retroactive: it is
    ///         read live only at the entry transitions (`bond` entry and
    ///         reinstatement, `registerNode`), never for ongoing active status.
    ///         An operator whose `unbond` ends its tenure re-enters at the live
    ///         value. Governance may tighten it as the network matures without
    ///         purging the electorate — a raise applies to new entrants only.
    ///         See `bondFloorAtActivation`.
    uint256 public minBond;

    /// @notice Per-operator entry floor: the live `minBond` captured at each
    ///         entry transition (`bond` up to the live `minBond`, or
    ///         `registerNode`; ADR 026 § Minimum bond is non-retroactive).
    ///         Ongoing active status (`isActive`) tests the active bond against
    ///         THIS floor, not the live `minBond`, so a later `minBond` raise
    ///         never de-activates an already-active operator. A re-entry
    ///         re-grandfathers at the then-current `minBond`. Zero while the
    ///         operator holds no tenure: before its first entry, and after an
    ///         `unbond` below the floor resets it together with `firstBondedAt`.
    mapping(address operator => uint256) public bondFloorAtActivation;

    /// @notice Governable declared-capacity band (Mbps) enforced on
    ///         `declareMbps`. Defaults: 10 Mbps floor, 200 Gbps (200_000 Mbps)
    ///         ceiling (ADR 026 § Capacity-bond curve).
    uint256 public minCapacityMbps;
    uint256 public maxCapacityMbps;

    /// @notice Capacity-bond curve coefficients (ADR 026 § Capacity-bond curve),
    ///         `bond_required(Mbps) = kConstant × Mbps^alphaWad`. `kConstant` is
    ///         in TOKEN-wei; `alphaWad` is the exponent α in 1e18 fixed point.
    ///         Governance-tunable via `setK` / `setAlpha`; the curve itself is
    ///         evaluated in `BondMath.bondRequired` (linked library, to keep the
    ///         fixed-point `pow` math out of this contract's runtime size).
    uint256 public kConstant;
    uint256 public alphaWad;

    // -----------------------------------------------------------------
    // Storage — region attestation (ADR 030)
    // -----------------------------------------------------------------

    /// @notice Previous-region snapshot taken on each region change, by
    ///         `updateRegion` or by a re-registration into a new region.
    ///         Consulted by the ADR 030 § Region-stability window blacklist-scope
    ///         ripening predicate (*"a node is in scope iff the entry is global,
    ///         OR entry.region == regionHint, OR (block.timestamp -
    ///         regionLastChanged < REGION_STABILITY_WINDOW AND entry.region ==
    ///         regionPrev)"*) at scope-test time. The predicate is read by
    ///         `ContentBlacklist.isHashBlacklistedForOperator`, which reads this
    ///         slot (with `regionLastChanged`) via `RegionScopeLib` off the
    ///         aggregate `regionScopeData` getter.
    mapping(address operator => string) public regionPrev;

    /// @notice Timestamp the operator's current region took effect: set to
    ///         `block.timestamp` at `registerNode` and restamped on each
    ///         `updateRegion`. Never 0 for an active node. The ripening window
    ///         (and the `updateRegion` cooldown) both run from this stamp.
    mapping(address operator => uint64) public regionLastChanged;

    /// @notice Cooldown enforced between `updateRegion` calls per ADR 030
    ///         (default 7 days; governable [3d, 30d]).
    uint256 public regionStabilityWindow;

    // -----------------------------------------------------------------
    // Storage — node registry (ADR 019 § Node Onboarding;
    // NodeId binding per ADR 003 — payments authorization)
    // -----------------------------------------------------------------

    struct NodeInfo {
        bytes32 nodeId;
        // `ethAddress` (20B) + `active` (1B) + `lastMultiaddrUpdate` (8B) =
        // 29 bytes — pack into one storage slot. Don't separate or widen
        // any of these three without re-checking the packing or every
        // `_writeNodeInfo` pays an extra SSTORE.
        address ethAddress;
        bool active;
        uint64 lastMultiaddrUpdate;
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

    // ADR 019 § Governance-canonical terms version — the operator-terms hash
    // new registrants must accept at `registerNode`. Governance-swappable via
    // `setCurrentTermsHash` (no `[floor, ceiling]` rail: a hash has no
    // monotonic direction). Already-registered operators keep their recorded
    // acceptance; a bump binds only new registrants.
    bytes32 public currentTermsHash;

    // -----------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------

    event Bonded(address indexed operator, uint256 amount, uint256 newActiveBond);
    /// @notice `amount` of TOKEN left the operator's active bond and went to
    ///         the operator in the same `unbond` call.
    event Unbonded(address indexed operator, uint256 amount, uint256 newActiveBond);
    event EjectedByBlacklist(address indexed operator);
    event BlacklistEjectionCleared(address indexed operator);
    event Reinstated(address indexed operator);
    event MinBondUpdated(uint256 oldValue, uint256 newValue);
    event KUpdated(uint256 oldValue, uint256 newValue);
    event AlphaUpdated(uint256 oldValue, uint256 newValue);
    event MultiaddrUpdateCooldownUpdated(uint256 oldValue, uint256 newValue);
    event MaxMultiaddrSizeUpdated(uint256 oldValue, uint256 newValue);
    event RegionStabilityWindowUpdated(uint256 oldValue, uint256 newValue);
    event MinCapacityMbpsUpdated(uint256 oldValue, uint256 newValue);
    event MaxCapacityMbpsUpdated(uint256 oldValue, uint256 newValue);

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

    // ADR 019 § Terms Acceptance — operator recorded acceptance of the current
    // operator terms (`termsHash`) at registration; evidence of notice + assent.
    event TermsAccepted(bytes32 indexed nodeId, bytes32 indexed termsHash, uint256 timestamp);
    // ADR 019 § Governance-canonical terms version — governor swapped the
    // canonical operator-terms hash new registrants must accept.
    event CurrentTermsHashUpdated(bytes32 oldHash, bytes32 newHash);
    event NodeDeregistered(bytes32 indexed nodeId);
    event NodeAutoEjected(bytes32 indexed nodeId, uint256 remainingBond);
    event NodeIdReclaimed(bytes32 indexed nodeId, address indexed previousOwner);

    // ADR 030 — region self-attestation.
    event RegionUpdated(bytes32 indexed nodeId, string oldRegion, string newRegion);

    // ADR 026 — declared capacity.
    event MbpsDeclared(address indexed operator, uint256 oldMbps, uint256 newMbps);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error ZeroAmount();
    error ZeroNodeId();
    error InsufficientBond(uint256 requested, uint256 available);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);
    error DeclaredCapacityOutOfBand(uint256 mbps, uint256 floor, uint256 ceiling);
    error BondBelowCurve(uint256 bond, uint256 required);
    error NodeAlreadyRegistered();
    error NodeIdAlreadyBound(address currentOwner);
    error AddressAlreadyBound(bytes32 currentNodeId);
    error InvalidBindingSignature();
    error InvalidEd25519Signature();
    // ADR 019 § Governance-canonical terms version — submitted `termsHash` did not match the
    // governance-canonical `currentTermsHash` (stale terms / un-upgraded CLI).
    error TermsHashMismatch(bytes32 provided, bytes32 expected);
    // ADR 019 § Terms Acceptance — `currentTermsHash` must never be the zero
    // sentinel: genesis commits to a real (possibly draft) terms version and
    // governance may only swap to another non-zero version. Forbidding zero
    // keeps registration from silently degrading to a no-terms bootstrap.
    error ZeroTermsHash();
    error MultiaddrsTooLarge(uint256 size, uint256 ceiling);
    error MultiaddrCooldownActive(uint256 readyAt);
    error NodeNotActive();
    error BondBelowMinimum(uint256 bond, uint256 required);
    error OperatorEjected();
    error NodeIdNotBound(bytes32 nodeId);
    error RegionHintTooLong(uint256 size, uint256 ceiling);

    // ADR 030
    error RegionCooldownActive(uint64 readyAt);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param token_                    TOKEN contract.
    /// @param ed25519Verifier_          Verifier for `registerNode` ed25519 proof.
    /// @param admin                     Initial `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE`.
    /// @param minBond_                  Initial minimum active bond.
    /// @param multiaddrUpdateCooldown_ Initial multiaddr-update cooldown.
    /// @param maxMultiaddrSize_         Initial multiaddrs byte-length cap.
    /// @param regionStabilityWindow_    Initial region-update cooldown (default
    ///                                  7 days; bounded `[3d, 30d]`).
    /// @param currentTermsHash_         Genesis operator-terms hash new
    ///                                  registrants must accept (ADR 019 §
    ///                                  Terms Acceptance). Governance-swappable
    ///                                  via `setCurrentTermsHash`; unbounded
    ///                                  (a hash has no monotonic direction).
    constructor(
        ERC20Burnable token_,
        IEd25519Verifier ed25519Verifier_,
        address admin,
        uint256 minBond_,
        uint256 multiaddrUpdateCooldown_,
        uint256 maxMultiaddrSize_,
        uint256 regionStabilityWindow_,
        bytes32 currentTermsHash_
    ) EIP712("CapacityBond", "1") {
        if (address(token_) == address(0) || address(ed25519Verifier_) == address(0) || admin == address(0)) {
            revert ZeroAddress();
        }
        _enforceMinBondBounds(minBond_);
        _enforceMultiaddrCooldownBounds(multiaddrUpdateCooldown_);
        _enforceMaxMultiaddrSizeBounds(maxMultiaddrSize_);
        _enforceRegionStabilityWindowBounds(regionStabilityWindow_);

        token = token_;
        ed25519Verifier = ed25519Verifier_;
        minBond = minBond_;
        multiaddrUpdateCooldown = multiaddrUpdateCooldown_;
        maxMultiaddrSize = maxMultiaddrSize_;
        regionStabilityWindow = regionStabilityWindow_;
        if (currentTermsHash_ == bytes32(0)) revert ZeroTermsHash();
        currentTermsHash = currentTermsHash_;

        // Declared-capacity band defaults (ADR 026 § Capacity-bond curve):
        // 10 Mbps floor (bars sub-floor dust declarations), 200 Gbps ceiling.
        // Governable post-deploy via setMinCapacityMbps / setMaxCapacityMbps.
        minCapacityMbps = 10;
        maxCapacityMbps = 200_000;

        // Capacity-bond curve defaults (ADR 026 § Capacity-bond curve):
        // α = 1.2, k = 12.6 TOKEN ⇒ bondRequired(1000) ≈ 50K TOKEN at 1 Gbps.
        // Governable post-deploy via setAlpha / setK.
        kConstant = 12.6e18;
        alphaWad = 1.2e18;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
        // Deploy renounces DEFAULT_ADMIN_ROLE, freezing the role table (#2028).
        // PAUSER_ROLE stays rotatable under GOVERNANCE_ROLE so a timelocked
        // proposal can evict a compromised emergency pauser without a redeploy.
        _setRoleAdmin(PAUSER_ROLE, GOVERNANCE_ROLE);
    }

    // -----------------------------------------------------------------
    // Bonding
    // -----------------------------------------------------------------

    /// @notice Add `amount` to the caller's active bond. The bond that lifts the
    ///         caller to the live `minBond` is an entry transition when the
    ///         caller holds no tenure or sits below that `minBond`: it records
    ///         the live `minBond` as `bondFloorAtActivation` and, when no tenure
    ///         runs, stamps `firstBondedAt` with the block timestamp. A running
    ///         tenure keeps its stamp.
    function bond(uint256 amount) external nonReentrant whenNotPaused {
        if (amount == 0) revert ZeroAmount();
        IERC20(address(token)).safeTransferFrom(msg.sender, address(this), amount);
        uint256 oldBalance = activeBond[msg.sender];
        uint256 newBalance = oldBalance + amount;
        activeBond[msg.sender] = newBalance;

        // Entry transition (ADR 026 § Minimum bond is non-retroactive). Ongoing
        // active status (`isActive`) reads the recorded floor, never the live
        // `minBond`, so a later `minBond` raise is non-retroactive. A dust bond
        // below `minBond` records nothing, so it cannot pre-age an address. An
        // operator with no tenure enters with the first bond that holds the
        // live `minBond`, also when its balance already sat above a lowered
        // `minBond`. A tenured operator that tops up through a raised `minBond`
        // re-grandfathers at it and keeps its tenure.
        if (newBalance >= minBond && (oldBalance < minBond || _firstBondedAt[msg.sender] == 0)) {
            _recordEntry();
        }

        // A governance blacklist latch (ADR 011) blocks re-entry by re-bonding:
        // re-entry after a blacklist goes through `unEjectNode` first, and the
        // next bond that holds `minBond` then clears `ejected`.
        if (ejected[msg.sender] && !blacklistEjected[msg.sender] && newBalance >= minBond) {
            ejected[msg.sender] = false;
            emit Reinstated(msg.sender);
        }

        emit Bonded(msg.sender, amount, newBalance);
    }

    /// @dev Entry transition shared by `bond` and `registerNode`. Records the
    ///      live `minBond` as the caller's floor and starts its tenure when none
    ///      runs. Both callers hold `activeBond >= minBond` here, so the tenure
    ///      invariant `firstBondedAt != 0 ⇒ activeBond >= bondFloorAtActivation
    ///      > 0` holds after it.
    function _recordEntry() private {
        bondFloorAtActivation[msg.sender] = minBond;
        // Zero is the "no tenure" sentinel, not a timestamp that could be
        // missed by a strict comparison.
        // slither-disable-next-line incorrect-equality
        if (_firstBondedAt[msg.sender] == 0) _firstBondedAt[msg.sender] = uint64(block.timestamp);
    }

    /// @notice Withdraw `amount` of the active bond to the caller in this call.
    ///         The remaining active bond must still cover the declared capacity
    ///         tier (`bondRequired(declaredMbps)`); an operator that reduces its
    ///         bond below the tier declares a lower tier first, and a full exit
    ///         clears the tier with `deregisterNode` (registered) or
    ///         `declareMbps(0)` (inactive). A remainder below
    ///         `bondFloorAtActivation` ends the operator's tenure: it resets
    ///         `firstBondedAt` and `bondFloorAtActivation` to 0, which takes the
    ///         operator out of the active set (`isActive`). The next entry
    ///         (`bond` up to the live `minBond`, or `registerNode`) records the
    ///         live `minBond` as the floor and stamps a fresh `firstBondedAt`.
    ///         Tenure thus measures continuous bonding at or above the floor,
    ///         and bonding `minBond` and withdrawing it again cannot pre-age an
    ///         address. A remainder at or above the floor keeps both.
    ///
    ///         This is a principal-exit path and is never pausable: pause halts
    ///         intake and serving, never an operator's exit (the client-side
    ///         twin `PaymentPool.reclaim` is un-pausable for the same reason), so
    ///         a captured PAUSER cannot trap an operator's bond.
    function unbond(uint256 amount) external nonReentrant {
        if (amount == 0) revert ZeroAmount();
        uint256 available = activeBond[msg.sender];
        if (amount > available) {
            revert InsufficientBond({ requested: amount, available: available });
        }

        uint256 remaining = available - amount;

        // ADR 026 § Capacity-bond curve: the remaining active bond must still
        // cover the operator's declared capacity. (No-op when no Mbps is
        // declared, since `bondRequired(0) == 0`.)
        uint256 required = bondRequired(declaredMbps[msg.sender]);
        if (remaining < required) {
            revert BondBelowCurve({ bond: remaining, required: required });
        }

        activeBond[msg.sender] = remaining;
        if (remaining < bondFloorAtActivation[msg.sender]) {
            delete bondFloorAtActivation[msg.sender];
            delete _firstBondedAt[msg.sender];
        }
        IERC20(address(token)).safeTransfer(msg.sender, amount);
        emit Unbonded(msg.sender, amount, remaining);
    }

    /// @notice Self-attest serving capacity in Mbps (ADR 026 § Capacity-bond
    ///         curve). The declaration must fall within the governable
    ///         `[minCapacityMbps, maxCapacityMbps]` band — out-of-band values
    ///         revert (the band is validated, not silently coerced to a
    ///         bound). The bond-curve coupling is enforced here:
    ///         `activeBond ≥ bondRequired(mbps)`, so an operator cannot declare
    ///         a capacity tier it has not bonded for.
    ///
    ///         `mbps == 0` is the one value outside that band an operator may
    ///         declare, and only while INACTIVE: it is the tier-release path
    ///         (issue #1361). Active operators still get `DeclaredCapacityOutOfBand`
    ///         for it, so the release is not a back door around the band —
    ///         a registered node leaves via `deregisterNode`, which clears the
    ///         tier as part of the exit.
    /// @dev    Without this branch the tier is unclearable from every state
    ///         `deregisterNode` cannot be called in — bonded-and-declared but
    ///         never registered, blacklist-ejected, or displaced by
    ///         `reclaimNodeId` — so `unbond`'s `bondRequired(declaredMbps)`
    ///         floor outlives the registration it was sized for.
    ///
    ///         This function does not enforce monotonicity, so such an operator
    ///         can also declare DOWN to `minCapacityMbps` and unbond everything
    ///         above `bondRequired(minCapacityMbps)`. That two-step tier-down
    ///         strands the floor-tier cost; this branch is a one-step release
    ///         that strands nothing.
    ///
    ///         The exception is a blacklist-ejected operator: `registerNode`
    ///         reverts `OperatorEjected` no matter how they re-bond, so for them
    ///         this is the only full exit.
    function declareMbps(uint256 mbps) external {
        // Checked before the band so a zero declaration from an inactive
        // operator skips BOTH gates: the band floor (bounded to
        // `[MIN_CAPACITY_FLOOR_MBPS, MIN_CAPACITY_CEILING_MBPS]` by
        // `_enforceMinCapacityBounds`, so governance can never lower it to 0 and
        // collapse this branch into the normal one) and the curve check, which
        // an operator whose tier outgrew its bond (a `setK` / `setAlpha` raise)
        // can no longer meet. The release is an exit step, so like `unbond` and
        // `deregisterNode` it is not pausable.
        bool release = mbps == 0 && !_nodes[msg.sender].active;
        if (!release) {
            _requireNotPaused();
            if (mbps < minCapacityMbps || mbps > maxCapacityMbps) {
                revert DeclaredCapacityOutOfBand({ mbps: mbps, floor: minCapacityMbps, ceiling: maxCapacityMbps });
            }
            uint256 required = bondRequired(mbps);
            if (activeBond[msg.sender] < required) {
                revert BondBelowCurve({ bond: activeBond[msg.sender], required: required });
            }
        }
        uint256 old = declaredMbps[msg.sender];
        declaredMbps[msg.sender] = mbps;
        DeclaredMbpsHistoryLib.record(_declaredMbpsHistory, msg.sender, mbps);
        emit MbpsDeclared(msg.sender, old, mbps);
    }

    /// @notice Active bond required to declare `mbps` of capacity under the
    ///         current curve (ADR 026 § Capacity-bond curve),
    ///         `bondRequired(Mbps) = kConstant × Mbps^alphaWad`. Off-chain
    ///         operators use this to size their bond before `declareMbps`; the
    ///         contract enforces `activeBond ≥ bondRequired(declaredMbps)` at
    ///         `declareMbps` / `unbond` / `registerNode`.
    function bondRequired(uint256 mbps) public view returns (uint256) {
        return BondMath.bondRequired(mbps, kConstant, alphaWad);
    }

    // -----------------------------------------------------------------
    // Region self-attestation (ADR 030)
    // -----------------------------------------------------------------

    /// @notice Update the operator's region. Every call is gated by
    ///         `regionStabilityWindow`, counted from the registration or the
    ///         change that set the current region.
    /// @dev    The new region is stored in the operator's `NodeInfo.regionHint`
    ///         so all downstream readers (`getRegisteredNodes`, off-chain DHT)
    ///         see the same source of truth. `regionPrev` retains the prior
    ///         value for the ADR 030 § Region-stability window blacklist-scope
    ///         ripening predicate
    ///         ("the previous region's entries keep applying until the change
    ///         ripens"), read on-chain by `ContentBlacklist` via the
    ///         `regionScopeData` view (#800).
    /// @dev    Only the length ceiling is enforced here; `"GLOBAL"` and `""` are
    ///         accepted as region strings. `RegionScopeLib` is the canonical guard
    ///         that excludes them from the scope predicate (a GLOBAL/empty current
    ///         or prev region is never a regional-leg match), and `addHashRegional`
    ///         independently rejects them as entry keys — so neither can collide
    ///         with the global blacklist. Rejecting them here too would only add
    ///         bytecode to this near-EIP-170-ceiling contract for no new guarantee.
    function updateRegion(string calldata newRegion) external whenNotPaused {
        if (bytes(newRegion).length > MAX_REGION_HINT_BYTES) {
            revert RegionHintTooLong({ size: bytes(newRegion).length, ceiling: MAX_REGION_HINT_BYTES });
        }
        NodeInfo storage info = _nodes[msg.sender];
        if (!info.active) revert NodeNotActive();

        // `regionLastChanged` is stamped at registration and on every change, so
        // the stability-window cooldown applies uniformly from the moment the
        // current region took effect — including an operator's first change.
        _requireRegionCooldownElapsed();

        emit RegionUpdated(info.nodeId, _setRegion(info, newRegion), newRegion);
    }

    /// @dev ADR 030 § Cooldown: reverts `RegionCooldownActive` until
    ///      `regionStabilityWindow` has elapsed since `regionLastChanged`.
    function _requireRegionCooldownElapsed() internal view {
        uint64 readyAt = regionLastChanged[msg.sender] + uint64(regionStabilityWindow);
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < readyAt) revert RegionCooldownActive(readyAt);
    }

    /// @dev ADR 030 § Region-stability window: a region change snapshots the
    ///      old region into `regionPrev`, so its entries keep applying until the
    ///      change ripens; an unchanged region keeps the existing snapshot.
    ///      A change is subject to the `updateRegion` cooldown on every path, so
    ///      a deregister/re-register cycle cannot flip the region twice inside
    ///      one window and push the serve-time region out of both slots. A
    ///      first registration (`regionLastChanged == 0`) has no prior region
    ///      and skips the cooldown. `regionLastChanged` is stamped on every
    ///      call, so it is never 0 for an active node, and both the ripening
    ///      window and the cooldown run from it. A re-registration restarts the
    ///      window.
    function _setRegion(NodeInfo storage info, string calldata newRegion) internal returns (string memory oldRegion) {
        oldRegion = info.regionHint;
        if (keccak256(bytes(oldRegion)) != keccak256(bytes(newRegion))) {
            if (regionLastChanged[msg.sender] != 0) _requireRegionCooldownElapsed();
            regionPrev[msg.sender] = oldRegion;
            info.regionHint = newRegion;
        }
        // forge-lint: disable-next-line(block-timestamp)
        regionLastChanged[msg.sender] = uint64(block.timestamp);
    }

    // -----------------------------------------------------------------
    // Node registry (ADR 003 § Node Registry)
    // -----------------------------------------------------------------

    /// @param termsHash Operator-terms hash the caller accepts. MUST equal the
    ///        governance-canonical `currentTermsHash` (ADR 019 § Terms
    ///        Acceptance) and is covered by `bindingSignature` (over
    ///        `REGISTER_NODE_TYPEHASH`), so assent binds to the exact bytes.
    function registerNode(
        bytes32 nodeId,
        bytes calldata multiaddrs,
        string calldata regionHint,
        bytes32 termsHash,
        bytes calldata bindingSignature,
        bytes calldata ed25519Signature
    ) external nonReentrant whenNotPaused {
        if (termsHash != currentTermsHash) {
            revert TermsHashMismatch(termsHash, currentTermsHash);
        }
        _checkRegistrationPreconditions(nodeId, multiaddrs.length, bytes(regionHint).length);
        _checkBindingOneToOne(nodeId);
        uint64 usedBindingNonce = _verifyRegistrationSignature(nodeId, termsHash, bindingSignature);
        uint64 usedRegistrationNonce = _verifyEd25519OwnershipSignature(nodeId, ed25519Signature);

        nodeIdToAddress[nodeId] = msg.sender;
        addressToNodeId[msg.sender] = nodeId;
        bindingNonce[msg.sender] = usedBindingNonce + 1;
        // registerNode is an entry transition gated on `activeBond >= minBond`,
        // so the live `minBond` is the entry floor. A floor left by an earlier
        // `bond()` entry may sit above or below it after a `minBond` change;
        // registration always records the live value. It also starts the tenure
        // of an operator that holds none, which happens when a lowered `minBond`
        // let its standing balance pass this gate without a `bond()` entry.
        _recordEntry();
        _writeNodeInfo(nodeId, multiaddrs, regionHint);
        _addToRegisteredSet(msg.sender);

        emit NodeRegistered(nodeId, msg.sender, multiaddrs, regionHint, usedBindingNonce, usedRegistrationNonce);
        emit NodeIdBound(msg.sender, nodeId, usedBindingNonce);
        // forge-lint: disable-next-line(block-timestamp)
        emit TermsAccepted(nodeId, termsHash, block.timestamp);
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
        if (activeBond[msg.sender] < minBond) {
            revert BondBelowMinimum({ bond: activeBond[msg.sender], required: minBond });
        }
        // ADR 026 § Capacity-bond curve: the bond must also cover the declared
        // capacity tier. `bondRequired(0) == 0`, so an operator that has not
        // declared Mbps is gated only by `minBond` above — `registerNode` does
        // not itself require a prior `declareMbps`. The capacity *band* floor
        // (`minCapacityMbps`) is enforced in `declareMbps`, not here.
        uint256 required = bondRequired(declaredMbps[msg.sender]);
        if (activeBond[msg.sender] < required) {
            revert BondBelowCurve({ bond: activeBond[msg.sender], required: required });
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

    /// @dev Registration binding signature covers `termsHash` (ADR 019 § Terms
    ///      Acceptance) over `REGISTER_NODE_TYPEHASH`, sharing the per-address
    ///      `bindingNonce` with `bindNodeId` for cross-path replay protection.
    function _verifyRegistrationSignature(bytes32 nodeId, bytes32 termsHash, bytes calldata sig)
        internal
        view
        returns (uint64 nonce)
    {
        nonce = bindingNonce[msg.sender];
        bytes32 digest = _hashTypedDataV4(keccak256(abi.encode(REGISTER_NODE_TYPEHASH, nodeId, nonce, termsHash)));
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
        info.lastMultiaddrUpdate = uint64(block.timestamp);
        info.multiaddrs = multiaddrs;
        _setRegion(info, regionHint);
    }

    /// @notice Leave the active set. Deactivates the node without touching the
    ///         bond — deactivation and bond exit stay separate operations
    ///         (ADR 003 § Node Registry).
    /// @dev    Clears `declaredMbps`, which is what makes the full-exit path
    ///         ADR 003 § Node Registry describes reachable: the `unbond`
    ///         floor is `bondRequired(declaredMbps)`, so an operator that left
    ///         the tier standing would retain that much bond forever. This is
    ///         the ONLY route to 0 for a registered node — `declareMbps(0)` is
    ///         accepted only from an inactive caller (see `declaredMbps`), which
    ///         a registered node by definition is not. The bond itself stays
    ///         put until the operator calls `unbond`; re-registering needs only
    ///         `minBond` again, but the tier must be re-declared.
    ///
    ///         Not pausable: clearing `declaredMbps` is the step a registered
    ///         operator takes to drop the `bondRequired(declaredMbps)` floor and
    ///         fully unbond, so gating it on pause would trap that floor of
    ///         principal. Leaving the active set is an exit and a de-escalation of
    ///         serving; pause halts intake and serving, never an operator's exit.
    function deregisterNode() external nonReentrant {
        NodeInfo storage info = _nodes[msg.sender];
        if (!info.active) revert NodeNotActive();

        bytes32 nodeId = info.nodeId;
        info.active = false;
        registrationNonce[nodeId] += 1;
        _removeFromRegisteredSet(msg.sender);

        // Emitted, not written silently: `MbpsDeclared` is the only log-based
        // signal for the tier, so a second writer that stayed quiet would
        // desync every indexer that follows it rather than polling
        // `declaredMbps`. Skipped when no tier was ever declared.
        uint256 oldMbps = declaredMbps[msg.sender];
        if (oldMbps != 0) {
            delete declaredMbps[msg.sender];
            DeclaredMbpsHistoryLib.record(_declaredMbpsHistory, msg.sender, 0);
            emit MbpsDeclared(msg.sender, oldMbps, 0);
        }

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
        info.lastMultiaddrUpdate = uint64(block.timestamp);

        emit NodeMultiaddrUpdated(info.nodeId, multiaddrs);
    }

    // -----------------------------------------------------------------
    // Blacklist ejection
    // -----------------------------------------------------------------

    function ejectNode(address operator) external override onlyRole(BLACKLIST_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        // Set the governance latch UNCONDITIONALLY — an operator still
        // `ejected` from an earlier blacklist (latch cleared by `unEjectNode`,
        // not yet reinstated by `bond`) would otherwise skip the
        // one-time-effects block below and never get latched, leaving the
        // self-reinstatement hole open.
        blacklistEjected[operator] = true;
        if (!ejected[operator]) {
            ejected[operator] = true;
            emit EjectedByBlacklist(operator);
            bytes32 nodeId = _ejectNodeEffects(operator);
            if (nodeId != bytes32(0)) {
                emit NodeAutoEjected(nodeId, activeBond[operator]);
            }
        }
    }

    /// @notice Release the governance-blacklist ejection latch when governance
    ///         lifts the blacklist (ADR 011). Clears only `blacklistEjected`;
    ///         the operator re-enters the active set through the normal re-bond
    ///         path (`bond()` reinstatement then `registerNode`), so an
    ///         operator below `minBond` stays ejected. Idempotent.
    function unEjectNode(address operator) external override onlyRole(BLACKLIST_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        if (blacklistEjected[operator]) {
            blacklistEjected[operator] = false;
            emit BlacklistEjectionCleared(operator);
        }
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    /// @notice Set the global entry minimum bond, within the fixed
    ///         `[MIN_BOND_FLOOR, MIN_BOND_CEILING]` (10K–1M TOKEN) rail
    ///         (ADR 026 § Operator bond). Non-retroactive in both directions:
    ///         `minBond` gates only entry (`bond` entry and reinstatement,
    ///         `registerNode`), so a change applies to new entrants and
    ///         re-entrants alone. Active operators keep the
    ///         `bondFloorAtActivation` they entered under — governance may
    ///         tighten the entry bar as the network matures without purging the
    ///         electorate, and a raise alone never de-activates an active
    ///         operator.
    function setMinBond(uint256 newMinBond) external onlyRole(GOVERNANCE_ROLE) {
        _enforceMinBondBounds(newMinBond);
        uint256 oldMinBond = minBond;
        minBond = newMinBond;
        emit MinBondUpdated(oldMinBond, newMinBond);
    }

    /// @notice Swap the governance-canonical operator-terms hash (ADR 019 §
    ///         Governance-canonical terms version). New registrants must accept
    ///         `newHash`; already-registered operators keep their recorded
    ///         acceptance. Deliberately has no `[floor, ceiling]` rail — a hash
    ///         has no monotonic direction; the guard rail is instead the
    ///         governance norm that every proposal setting `currentTermsHash`
    ///         references the terms text and its review record.
    function setCurrentTermsHash(bytes32 newHash) external onlyRole(GOVERNANCE_ROLE) {
        if (newHash == bytes32(0)) revert ZeroTermsHash();
        bytes32 oldHash = currentTermsHash;
        currentTermsHash = newHash;
        emit CurrentTermsHashUpdated(oldHash, newHash);
    }

    /// @notice Set the capacity-bond curve constant `k` in TOKEN-wei (ADR 026
    ///         § Capacity-bond curve). Bounded indirectly: the resulting
    ///         1-Gbps-tier bond `bondRequired(1000)` must stay within
    ///         [10K, 200K TOKEN] against the live α. Update `α` and `k` in the
    ///         order that keeps the 1-Gbps tier in range at each step.
    function setK(uint256 newK) external onlyRole(GOVERNANCE_ROLE) {
        uint256 oldK = kConstant;
        kConstant = newK;
        _enforceOneGbpsTierBond();
        emit KUpdated(oldK, newK);
    }

    /// @notice Set the capacity-bond curve exponent α in 1e18 fixed point
    ///         (ADR 026 § Capacity-bond curve). Hard-bounded to [1.0, 1.8]; the
    ///         resulting 1-Gbps-tier bond must also stay within [10K, 200K
    ///         TOKEN] against the live `k` (see `setK` for the ordering note).
    function setAlpha(uint256 newAlphaWad) external onlyRole(GOVERNANCE_ROLE) {
        if (newAlphaWad < ALPHA_FLOOR || newAlphaWad > ALPHA_CEILING) {
            revert ParamOutOfBounds({ value: newAlphaWad, floor: ALPHA_FLOOR, ceiling: ALPHA_CEILING });
        }
        uint256 oldAlpha = alphaWad;
        alphaWad = newAlphaWad;
        _enforceOneGbpsTierBond();
        emit AlphaUpdated(oldAlpha, newAlphaWad);
    }

    /// @dev Require the 1-Gbps-tier bond under the *current* stored `(k, α)` to
    ///      fall within [10K, 200K TOKEN] (ADR 026 § Capacity-bond curve
    ///      "bounded by 1G tier"). The setters write the new value first and
    ///      call this after; a revert here rolls the tentative write back, so
    ///      this reuses the storage-reading `bondRequired` view rather than a
    ///      second ABI-encoded library call path.
    function _enforceOneGbpsTierBond() internal view {
        uint256 bondAt1G = bondRequired(ONE_GBPS_MBPS);
        if (bondAt1G < ONE_GBPS_BOND_FLOOR || bondAt1G > ONE_GBPS_BOND_CEILING) {
            revert ParamOutOfBounds({ value: bondAt1G, floor: ONE_GBPS_BOND_FLOOR, ceiling: ONE_GBPS_BOND_CEILING });
        }
    }

    /// @notice Set the floor of the declared-capacity band (ADR 026
    ///         § Capacity-bond curve). Bounded [10, 1000] Mbps; the lower
    ///         bound equals the launch default so governance can only raise
    ///         the floor, never reopen the sub-floor dust case.
    function setMinCapacityMbps(uint256 newMin) external onlyRole(GOVERNANCE_ROLE) {
        _enforceMinCapacityBounds(newMin);
        uint256 oldMin = minCapacityMbps;
        minCapacityMbps = newMin;
        emit MinCapacityMbpsUpdated(oldMin, newMin);
    }

    /// @notice Set the ceiling of the declared-capacity band (ADR 026
    ///         § Capacity-bond curve). Bounded [50_000, 1_000_000] Mbps
    ///         (50–1000 Gbps), always strictly above the floor's range.
    function setMaxCapacityMbps(uint256 newMax) external onlyRole(GOVERNANCE_ROLE) {
        _enforceMaxCapacityBounds(newMax);
        uint256 oldMax = maxCapacityMbps;
        maxCapacityMbps = newMax;
        emit MaxCapacityMbpsUpdated(oldMax, newMax);
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

    // -----------------------------------------------------------------
    // Pause control
    // -----------------------------------------------------------------

    function pause() external onlyRole(PAUSER_ROLE) {
        _requirePauseWindowOpen();
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }

    // -----------------------------------------------------------------
    // Views — bond / governor surface
    // -----------------------------------------------------------------

    function bondOf(address operator) external view returns (uint256) {
        return activeBond[operator];
    }

    function isActive(address operator) public view returns (bool) {
        // Ongoing active status tests the grandfathered `bondFloorAtActivation`,
        // not the live `minBond`: a `minBond` raise never de-activates an
        // already-active operator (ADR 026 § Operator bond). `unbond` moves
        // the released amount out of `activeBond` at once, so a partial
        // unbond that retains the floor keeps the operator active, and one that
        // drops below the floor ends its tenure and resets the floor to 0. A
        // zero floor marks an operator that holds no tenure, so it is inactive
        // until its next entry, even while registered.
        uint256 entryFloor = bondFloorAtActivation[operator];
        return _nodes[operator].active && entryFloor != 0 && activeBond[operator] >= entryFloor && !ejected[operator];
    }

    /// @inheritdoc ICapacityBond
    function firstBondedAt(address operator) external view override returns (uint64) {
        return _firstBondedAt[operator];
    }

    /// @inheritdoc ICapacityBond
    function declaredMbpsAtEpoch(address operator, uint64 epoch) external view override returns (uint256) {
        return DeclaredMbpsHistoryLib.atEpoch(_declaredMbpsHistory, operator, epoch, EPOCH_LENGTH);
    }

    // -----------------------------------------------------------------
    // Views — node registry
    // -----------------------------------------------------------------

    function nodeIdOf(address operator) external view returns (bytes32 nodeId, bool active) {
        nodeId = addressToNodeId[operator];
        active = isActive(operator);
    }

    function getNodeByAddress(address ethAddress) external view returns (NodeInfo memory) {
        return _nodes[ethAddress];
    }

    /// @inheritdoc ICapacityBondRegionView
    function regionScopeData(address operator)
        external
        view
        returns (
            string memory regionHint,
            string memory regionPrev_,
            uint64 regionLastChanged_,
            uint256 regionStabilityWindow_
        )
    {
        return (_nodes[operator].regionHint, regionPrev[operator], regionLastChanged[operator], regionStabilityWindow);
    }

    function isActiveNode(bytes32 nodeId) external view returns (bool) {
        address ethAddress = nodeIdToAddress[nodeId];
        if (ethAddress == address(0)) return false;
        return isActive(ethAddress);
    }

    function getRegisteredNodeCount() external view returns (uint256) {
        return _registeredAddrs.length;
    }

    /// @notice Paginated read of the registered-operator set, with per-entry
    ///         activeness computed on-chain.
    /// @dev    Returns the raw `_registeredAddrs` page (every operator whose node
    ///         record is registered, regardless of bond or ejection) plus
    ///         a parallel `active[]` where `active[i] == isActive(page[i].ethAddress)`.
    ///         The two projections diverge on purpose: an operator whose bond sits
    ///         below its floor is `active[i] == false` but still present in `page` because it remains
    ///         payable, so the off-chain node→address bindings must survive. Computing
    ///         `active` here lets callers derive the true active-membership set from a
    ///         single call, with no per-entry `isActive` round-trip (issue #1565).
    function getRegisteredNodes(uint256 offset, uint256 limit)
        external
        view
        returns (NodeInfo[] memory page, bool[] memory active)
    {
        uint256 len = _registeredAddrs.length;
        if (offset >= len || limit == 0) {
            return (new NodeInfo[](0), new bool[](0));
        }
        uint256 end = offset + limit;
        if (end > len) end = len;
        uint256 size = end - offset;
        page = new NodeInfo[](size);
        active = new bool[](size);
        for (uint256 i = 0; i < size; i++) {
            address operator = _registeredAddrs[offset + i];
            page[i] = _nodes[operator];
            active[i] = isActive(operator);
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

    function _enforceMinBondBounds(uint256 value) internal pure {
        if (value < MIN_BOND_FLOOR || value > MIN_BOND_CEILING) {
            revert ParamOutOfBounds({ value: value, floor: MIN_BOND_FLOOR, ceiling: MIN_BOND_CEILING });
        }
    }

    function _enforceMinCapacityBounds(uint256 value) internal pure {
        if (value < MIN_CAPACITY_FLOOR_MBPS || value > MIN_CAPACITY_CEILING_MBPS) {
            revert ParamOutOfBounds({
                value: value, floor: MIN_CAPACITY_FLOOR_MBPS, ceiling: MIN_CAPACITY_CEILING_MBPS
            });
        }
    }

    function _enforceMaxCapacityBounds(uint256 value) internal pure {
        if (value < MAX_CAPACITY_FLOOR_MBPS || value > MAX_CAPACITY_CEILING_MBPS) {
            revert ParamOutOfBounds({
                value: value, floor: MAX_CAPACITY_FLOOR_MBPS, ceiling: MAX_CAPACITY_CEILING_MBPS
            });
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
