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

import { ICapacityBond } from "./interfaces/ICapacityBond.sol";
import { ICapacityBondSlashEscrow } from "./interfaces/ICapacityBondSlashEscrow.sol";
import { ICapacityBondEjector } from "./interfaces/ICapacityBondEjector.sol";
import { ICapacityBondReporter } from "./interfaces/ICapacityBondReporter.sol";
import { ICapacityBondRegionView } from "./interfaces/ICapacityBondRegionView.sol";
import { ISlashJudgeEvidenceView } from "./interfaces/ISlashJudgeEvidenceView.sol";
import { IEd25519Verifier } from "./interfaces/IEd25519Verifier.sol";
import { BondMath } from "./BondMath.sol";
import { SlashEscrowLib, SlashRecord } from "./SlashEscrowLib.sol";

/// @title CapacityBond — operator-registry contract
/// @notice Custodies operator TOKEN bond, executes the escrow-on-slash flow
///         from ADR 026 § Slashing and burn (the slashed TOKEN is held in
///         per-slashId escrow until the appeal window resolves, then either
///         distributed 50% challenger / 50% burn or refunded to the operator
///         on a successful appeal), is the canonical settlement reporter sink
///         for `FeeRouter`, is the registry for iroh-NodeId ↔ Ethereum-address
///         bindings, and is the source of `firstBondedAt` / `slashedAtEpoch`
///         for `DecdnGovernor`'s served-bytes voting weight per ADR 036.
/// @dev    Renamed from `StakingRegistry` per ADR 026 v2.2 vocabulary. The
///         bond / unbond / node-registry primitives are unchanged from the
///         prior contract; this revision adds:
///           - `firstBondedAt[op]`  — set on first successful `bond` (ADR 036)
///           - `slashedAtEpoch[op]` — stamped in `slash()`, cleared by the
///                                    `settleAppealGranted` escrow hook on a
///                                    successful appeal (ADR 028 / 036)
///           - escrow-on-slash: the slashed TOKEN is parked in `_slashRecords`
///                                    (`escrowedTotal` accounting) until
///                                    `finalizeUnappealedSlash` (no appeal) or
///                                    one of the `SLASH_APPEAL_ROLE` settle
///                                    hooks resolves it (ADR 028)
///           - `regionPrev[op]` + `regionLastChanged[op]` + `updateRegion`
///                                    (ADR 030 § Node Region Self-Attestation)
///           - `declaredMbps[op]`   — operator-asserted capacity (ADR 026
///                                    § Capacity-bond curve), set via
///                                    `declareMbps` within the governable
///                                    `[minCapacityMbps, maxCapacityMbps]`
///                                    band; the bond-curve coupling
///                                    `activeBond ≥ k × Mbps^α` is enforced at
///                                    `declareMbps` / `requestUnbond` /
///                                    `registerNode`, with `k`/`α`
///                                    governance-tunable via `setK` / `setAlpha`
contract CapacityBond is
    ICapacityBond,
    ICapacityBondSlashEscrow,
    ICapacityBondEjector,
    ICapacityBondReporter,
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
    bytes32 public constant SLASH_ROLE = keccak256("SLASH_ROLE");
    bytes32 public constant BLACKLIST_ROLE = keccak256("BLACKLIST_ROLE");
    bytes32 public constant SETTLEMENT_REPORTER_ROLE = keccak256("SETTLEMENT_REPORTER_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    /// @notice Authority to drive the escrow-on-slash appeal hooks
    ///         (`markAppealOpen` / `settleAppealUpheld` / `settleAppealGranted`).
    ///         Granted to the `SlashAppeal` contract (ADR 016 § Post-Deployment
    ///         Initialization); consumed by the `SlashAppeal` state machine per
    ///         ADR 028 § Contract surface.
    bytes32 public constant SLASH_APPEAL_ROLE = keccak256("SLASH_APPEAL_ROLE");

    // -----------------------------------------------------------------
    // Slash schedule constants (ADR 026 § Slashing and burn)
    // -----------------------------------------------------------------

    uint256 internal constant BPS_DENOMINATOR = 10_000;
    uint256 internal constant SLASH_BPS_TIER_1 = 500;
    uint256 internal constant SLASH_BPS_TIER_2 = 1500;
    uint256 internal constant SLASH_BPS_TIER_3 = 5000;
    // The challenger/burn split at finality (ADR 026 § Slashing and burn — 50%
    // challenger / 50% burn) now lives in `SlashEscrowLib`, alongside the
    // distribution logic that consumes it.

    /// @notice Window after a slash during which the operator may file an
    ///         appeal (ADR 028 § Hard caps and frequency limits). Also gates
    ///         the permissionless `finalizeUnappealedSlash` path: escrow can
    ///         only be distributed as upheld once this window lapses with no
    ///         appeal. Shared with `SlashAppeal` via `markAppealOpen`, which is
    ///         the single on-chain enforcer of the filing deadline.
    uint64 internal constant APPEAL_FILING_WINDOW = 30 days;

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

    // Governance bounds on `unbondingPeriod` per ADR 009 § CapacityBond curve
    // and governance parameters (mirrored by ADR 016 and ADR 014).
    uint256 internal constant UNBONDING_PERIOD_FLOOR = 7 days;
    uint256 internal constant UNBONDING_PERIOD_CEILING = 60 days;

    uint256 internal constant MULTIADDR_COOLDOWN_CEILING = 1 days;

    uint256 internal constant MAX_MULTIADDR_SIZE_FLOOR = 64;
    uint256 internal constant MAX_MULTIADDR_SIZE_CEILING = 1024;

    uint256 internal constant MAX_REGION_HINT_BYTES = 16;

    /// @notice Bounds on `regionStabilityWindow` per ADR 030 § Cooldown.
    uint256 internal constant REGION_STABILITY_WINDOW_FLOOR = 3 days;
    uint256 internal constant REGION_STABILITY_WINDOW_CEILING = 30 days;

    /// @notice Canonical epoch length shared with `FeeRouter` (ADR 026 §
    ///         FeeRouter split, ADR 036 § Formula). Stored as a constant
    ///         on this contract so `slashedAtEpoch` derives the same epoch
    ///         index `FeeRouter` writes into `bytesPerEpoch`. Exposed via the
    ///         lowercase `epochLength()` accessor so `FeeRouter`'s
    ///         constructor can assert equality against the value the
    ///         deployer passes for `epochLength_` (closes the silent
    ///         epoch-mis-anchor footgun where `DecdnGovernor._slashedInWindow`
    ///         would compare incompatible epoch indices).
    uint64 public constant EPOCH_LENGTH = 7 days;

    /// @notice Lowercase accessor for `EPOCH_LENGTH` — exists so
    ///         `ICapacityBondReporter` can declare it without tripping
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

    struct UnbondingRequest {
        uint256 amount;
        uint256 unlockAt;
    }

    mapping(address operator => uint256 amount) public activeBond;
    mapping(address operator => UnbondingRequest) public unbondingOf;
    mapping(address operator => uint32) public lifetimeOffenseCount;
    mapping(address operator => bool) public ejected;

    /// @notice Governance-blacklist ejection latch (ADR 011 § Hash Evasion and
    ///         Origin Blacklisting). Set by the `BLACKLIST_ROLE` `ejectNode`
    ///         path and is independent of the recoverable slash auto-ejection
    ///         (ADR 026): while set, `bond()` MUST NOT clear `ejected`, so a
    ///         blacklisted operator cannot self-reinstate by re-bonding. Cleared
    ///         only by `unEjectNode` when governance lifts the blacklist.
    mapping(address operator => bool) public blacklistEjected;

    /// @notice First-bond-time stamp per operator (ADR 036 § Formula —
    ///         `age_ramp` numerator). Set once on the first `bond` call that
    ///         lifts the operator's `activeBond` above zero; never overwritten.
    mapping(address operator => uint64) internal _firstBondedAt;

    /// @notice Encoded slash-epoch stamp: `0` means "never slashed in the
    ///         current window"; any non-zero value is `actualEpoch + 1`.
    ///         The +1 offset exists so a genuine slash in epoch 0 (the first
    ///         `EPOCH_LENGTH` after deploy) is not collapsed with the
    ///         "unslashed" sentinel. Consumers MUST decode (`slashed - 1`)
    ///         before doing epoch arithmetic. (ADR 036 § Slashing zero-out.)
    mapping(address operator => uint64) internal _slashedAtEpoch;

    /// @notice Operator-asserted serving capacity in Mbps (ADR 026
    ///         § Capacity-bond curve). `declareMbps` enforces the governable
    ///         `[minCapacityMbps, maxCapacityMbps]` band; the bond-curve
    ///         coupling `activeBond ≥ bondRequired(declaredMbps)` is enforced
    ///         at the operator-initiated mutation sites (`declareMbps`,
    ///         `requestUnbond`, `registerNode`). Slash paths intentionally do
    ///         not re-enforce it — a penalized operator may fall under the
    ///         curve and is auto-ejected below `minBond / 2`.
    mapping(address operator => uint256) public declaredMbps;

    uint256 public minBond;
    uint256 public unbondingPeriod;

    /// @notice SlashJudge view used to enforce the paired
    ///         `maxEvidenceAgeUs < unbondingPeriod * 1e6` invariant on
    ///         `setUnbondingPeriod` (ADR 014 § Interaction with unbonding period).
    ///         Wired post-deploy via `setSlashJudge` because SlashJudge is deployed
    ///         after CapacityBond (deploy-order circular dependency). While unset
    ///         (`address(0)`), `setUnbondingPeriod` applies only the live
    ///         `[UNBONDING_PERIOD_FLOOR, UNBONDING_PERIOD_CEILING]` bound — the
    ///         `[7d,60d]` window from ADR 014/009 (see the bounds' declaration
    ///         note).
    ISlashJudgeEvidenceView public slashJudge;

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

    /// @notice Previous-region snapshot taken on each `updateRegion` call.
    ///         Consulted by the ADR 030 § Region-stability window blacklist-scope
    ///         ripening predicate (*"a node is in scope iff the entry is global,
    ///         OR entry.region == regionHint, OR (block.timestamp - effective <
    ///         REGION_STABILITY_WINDOW AND entry.region == regionPrev)"*) at
    ///         scope-test / slash-eligibility time. The predicate is enforced by
    ///         `SlashJudge._checkBlacklistedBefore` and read by
    ///         `ContentBlacklist.isHashBlacklistedForOperator`; both read this
    ///         slot (with `regionLastChanged` / `firstBondedAt` /
    ///         `regionGateActivatedAt`) via `RegionScopeLib` off the aggregate
    ///         `regionScopeData` getter.
    mapping(address operator => string) public regionPrev;

    /// @notice Last `updateRegion` timestamp; 0 means region has never been
    ///         changed (initial value from `registerNode` is final until the
    ///         first explicit update).
    mapping(address operator => uint64) public regionLastChanged;

    /// @notice Cooldown enforced between `updateRegion` calls per ADR 030
    ///         (default 7 days; governable [3d, 30d]).
    uint256 public regionStabilityWindow;

    /// @notice ADR 030 § Region-stability window gate-activation stamp — the
    ///         floor the ripening window runs from for nodes that have never
    ///         changed region (`effective = max(firstBondedAt, this)`). ADR 030
    ///         specifies an "upgrade initializer" for this value, but
    ///         `CapacityBond` is constructor-deployed and non-upgradeable, so a
    ///         fresh deploy *is* gate activation (no pre-upgrade cohort) — it is
    ///         set to `block.timestamp` in the constructor.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint64 public immutable regionGateActivatedAt;

    // -----------------------------------------------------------------
    // Storage — slash records (ADR 028 § Contract surface)
    // -----------------------------------------------------------------

    /// @notice The slash escrow lifecycle (`SlashStatus`) and the `SlashRecord`
    ///         struct live in [`SlashEscrowLib`](SlashEscrowLib.sol) (file-level
    ///         types) alongside the finality state machine extracted there to
    ///         keep this contract under the EIP-170 size ceiling (issue #770).
    ///         `Escrowed` → either `Upheld` (distributed 50/50) or `Reversed`
    ///         (refunded). `AppealOpen` locks the escrow while the `SlashAppeal`
    ///         state machine runs, so `finalizeUnappealedSlash` cannot race it.

    /// @notice Monotonic slash counter — next slash receives this index, then
    ///         `slashCounter` increments.
    uint256 public slashCounter;

    mapping(uint256 slashId => SlashRecord) internal _slashRecords;

    /// @notice Per-operator append-only list of that operator's slashIds. Used
    ///         to recompute the `slashedAtEpoch` watermark to the max epoch
    ///         among still-standing (non-`Reversed`) slashes when a granted
    ///         appeal reverses one — so reversing the most-recent slash falls
    ///         the watermark back to an older slash that still stands rather
    ///         than wrongly clearing it (ADR 036 § Slashing zero-out —
    ///         multi-slash). Expected to stay small in practice — a slash drops
    ///         active bond (auto-ejecting below `minBond / 2`), so re-slashing
    ///         costs the operator a fresh re-bond each cycle — but this is an
    ///         economic deterrent, not a hard cap; the recompute scan reads from
    ///         the tail and breaks at the first standing slash, so it is cheap
    ///         even if the list is long.
    mapping(address operator => uint256[]) internal _operatorSlashIds;

    /// @notice Sum of all TOKEN currently held in slash escrow (status
    ///         `Escrowed` or `AppealOpen`). Invariant anchor: the contract's
    ///         TOKEN balance must cover `escrowedTotal` plus active bond and
    ///         unbonding bond. Incremented in
    ///         `slash()`, decremented at every terminal escrow transition.
    uint256 public escrowedTotal;

    /// @notice Combined slash-path paused-seconds accumulator (the single source
    ///         of truth). Folds in this contract's own pauses plus, via
    ///         `creditPauseTime`, `SlashAppeal`'s pauses, so every appeal window
    ///         (filing here, review/ratification on `SlashAppeal`) extends by the
    ///         same total and a pause on either contract never silently consumes
    ///         a window (ADR 028 §5). Conservative: a pause that predates a slash
    ///         still extends that slash's window, which only ever favors the
    ///         operator.
    uint64 public override pausedTotal;

    /// @notice `block.timestamp` at which the current pause began; 0 when not
    ///         paused.
    uint64 internal _pausedAt;

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
    event UnbondingRequested(address indexed operator, uint256 amount, uint256 unlockAt, uint256 newActiveBond);
    event Unbonded(address indexed operator, uint256 amount);
    event Slashed(
        address indexed operator,
        address indexed challenger,
        uint8 offenseType,
        uint32 lifetimeOffenseCount,
        uint256 slashAmount
    );
    event AutoEjected(address indexed operator, uint256 remainingBond);
    event EjectedByBlacklist(address indexed operator);
    event BlacklistEjectionCleared(address indexed operator);
    event Reinstated(address indexed operator);
    event SettlementRecorded(address indexed operator);
    event MinBondUpdated(uint256 oldValue, uint256 newValue);
    event KUpdated(uint256 oldValue, uint256 newValue);
    event AlphaUpdated(uint256 oldValue, uint256 newValue);
    event UnbondingPeriodUpdated(uint256 oldValue, uint256 newValue);
    event SlashJudgeUpdated(address indexed oldJudge, address indexed newJudge);
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

    // ADR 036 — slash-zero-out lifecycle. `epoch == 0` signals the cleared /
    // unslashed sentinel (the watermark recomputed to "no standing slash").
    event SlashedAtEpochStamped(address indexed operator, uint64 epoch);

    // ADR 026 — declared capacity.
    event MbpsDeclared(address indexed operator, uint256 oldMbps, uint256 newMbps);

    // ADR 028 — slash record minting + escrow lifecycle.
    event SlashRecorded(uint256 indexed slashId, address indexed operator, uint64 slashedAt, uint256 slashAmount);
    event SlashEscrowed(uint256 indexed slashId, address indexed operator, uint256 amount, uint64 appealWindowClose);
    event SlashAppealOpened(uint256 indexed slashId, address indexed operator);
    event SlashUpheld(uint256 indexed slashId, bool viaAppeal, uint256 challengerShare, uint256 burnShare);
    event SlashReversed(uint256 indexed slashId, address indexed operator, uint256 refund);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error ZeroAmount();
    error ZeroNodeId();
    error InsufficientBond(uint256 requested, uint256 available);
    error UnbondingInProgress();
    error UnbondingNotComplete(uint256 unlockAt);
    error NoUnbondingRequest();
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

    /// @dev `setUnbondingPeriod` would drop `unbondingPeriod * 1e6` to or below the
    ///      live evidence-age ceiling, violating ADR 014's slash-before-withdraw
    ///      invariant.
    ///      Paired with `SlashJudge.EvidenceAgeExceedsUnbonding` (the other half).
    error UnbondingBelowEvidenceAge(uint256 unbondingUs, uint256 maxEvidenceAgeUs);

    // ADR 030
    error RegionCooldownActive(uint64 readyAt);

    // ADR 028 — slash record lookup + escrow lifecycle
    error UnknownSlash(uint256 slashId);
    error SlashNotEscrowed(uint256 slashId);
    error SlashAppealNotOpen(uint256 slashId);
    error FilingWindowStillOpen(uint64 readyAt);
    error FilingWindowClosed(uint64 closedAt);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param token_                    TOKEN contract (must implement `burn`).
    /// @param ed25519Verifier_          Verifier for `registerNode` ed25519 proof.
    /// @param admin                     Initial `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE`.
    /// @param minBond_                  Initial minimum active bond.
    /// @param unbondingPeriod_          Initial unbonding period.
    /// @param multiaddrUpdateCooldown_  Initial multiaddr-update cooldown.
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
        uint256 unbondingPeriod_,
        uint256 multiaddrUpdateCooldown_,
        uint256 maxMultiaddrSize_,
        uint256 regionStabilityWindow_,
        bytes32 currentTermsHash_
    ) EIP712("CapacityBond", "1") {
        if (address(token_) == address(0) || address(ed25519Verifier_) == address(0) || admin == address(0)) {
            revert ZeroAddress();
        }
        _enforceMinBondBounds(minBond_);
        _enforceUnbondingPeriodBounds(unbondingPeriod_);
        _enforceMultiaddrCooldownBounds(multiaddrUpdateCooldown_);
        _enforceMaxMultiaddrSizeBounds(maxMultiaddrSize_);
        _enforceRegionStabilityWindowBounds(regionStabilityWindow_);

        token = token_;
        ed25519Verifier = ed25519Verifier_;
        minBond = minBond_;
        unbondingPeriod = unbondingPeriod_;
        multiaddrUpdateCooldown = multiaddrUpdateCooldown_;
        maxMultiaddrSize = maxMultiaddrSize_;
        regionStabilityWindow = regionStabilityWindow_;
        if (currentTermsHash_ == bytes32(0)) revert ZeroTermsHash();
        currentTermsHash = currentTermsHash_;
        // ADR 030 § Region-stability window: fresh deploy == gate activation
        // (non-upgradeable, so no migration cohort to stay conservative for).
        // forge-lint: disable-next-line(block-timestamp)
        regionGateActivatedAt = uint64(block.timestamp);

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
    }

    // -----------------------------------------------------------------
    // Bonding
    // -----------------------------------------------------------------

    function bond(uint256 amount) external nonReentrant whenNotPaused {
        if (amount == 0) revert ZeroAmount();
        IERC20(address(token)).safeTransferFrom(msg.sender, address(this), amount);
        uint256 oldBalance = activeBond[msg.sender];
        uint256 newBalance = oldBalance + amount;
        activeBond[msg.sender] = newBalance;

        // First-bond timestamp is set the moment the operator's `activeBond`
        // becomes non-zero, never overwritten (ADR 036 § Formula).
        if (oldBalance == 0 && _firstBondedAt[msg.sender] == 0) {
            _firstBondedAt[msg.sender] = uint64(block.timestamp);
        }

        // Slash auto-ejection (ADR 026) is recoverable by re-bonding, but a
        // governance blacklist latch (ADR 011) is not — re-entry after a
        // blacklist must go through `unEjectNode` first.
        if (ejected[msg.sender] && !blacklistEjected[msg.sender] && newBalance >= minBond) {
            ejected[msg.sender] = false;
            emit Reinstated(msg.sender);
        }

        emit Bonded(msg.sender, amount, newBalance);
    }

    function requestUnbond(uint256 amount) external nonReentrant whenNotPaused {
        if (amount == 0) revert ZeroAmount();
        if (amount > activeBond[msg.sender]) {
            revert InsufficientBond({ requested: amount, available: activeBond[msg.sender] });
        }
        if (unbondingOf[msg.sender].amount != 0) revert UnbondingInProgress();

        activeBond[msg.sender] -= amount;

        // ADR 026 § Capacity-bond curve: the remaining active bond must still
        // cover the operator's declared capacity. Operators reducing capacity
        // must `declareMbps` down first. (No-op when no Mbps is declared, since
        // `bondRequired(0) == 0`.)
        uint256 required = bondRequired(declaredMbps[msg.sender]);
        if (activeBond[msg.sender] < required) {
            revert BondBelowCurve({ bond: activeBond[msg.sender], required: required });
        }

        uint256 unlockAt = block.timestamp + unbondingPeriod;
        unbondingOf[msg.sender] = UnbondingRequest({ amount: amount, unlockAt: unlockAt });

        emit UnbondingRequested(msg.sender, amount, unlockAt, activeBond[msg.sender]);
    }

    function unbond() external nonReentrant whenNotPaused {
        UnbondingRequest memory req = unbondingOf[msg.sender];
        if (req.amount == 0) revert NoUnbondingRequest();
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < req.unlockAt) revert UnbondingNotComplete(req.unlockAt);

        delete unbondingOf[msg.sender];
        IERC20(address(token)).safeTransfer(msg.sender, req.amount);
        emit Unbonded(msg.sender, req.amount);
    }

    /// @notice Self-attest serving capacity in Mbps (ADR 026 § Capacity-bond
    ///         curve). The declaration must fall within the governable
    ///         `[minCapacityMbps, maxCapacityMbps]` band — out-of-band values
    ///         revert (the band is validated, not silently coerced to a
    ///         bound). The bond-curve coupling is enforced here:
    ///         `activeBond ≥ bondRequired(mbps)`, so an operator cannot declare
    ///         a capacity tier it has not bonded for.
    function declareMbps(uint256 mbps) external whenNotPaused {
        if (mbps < minCapacityMbps || mbps > maxCapacityMbps) {
            revert DeclaredCapacityOutOfBand({ mbps: mbps, floor: minCapacityMbps, ceiling: maxCapacityMbps });
        }
        uint256 required = bondRequired(mbps);
        if (activeBond[msg.sender] < required) {
            revert BondBelowCurve({ bond: activeBond[msg.sender], required: required });
        }
        uint256 old = declaredMbps[msg.sender];
        declaredMbps[msg.sender] = mbps;
        emit MbpsDeclared(msg.sender, old, mbps);
    }

    /// @notice Active bond required to declare `mbps` of capacity under the
    ///         current curve (ADR 026 § Capacity-bond curve),
    ///         `bondRequired(Mbps) = kConstant × Mbps^alphaWad`. Off-chain
    ///         operators use this to size their bond before `declareMbps`; the
    ///         contract enforces `activeBond ≥ bondRequired(declaredMbps)` at
    ///         `declareMbps` / `requestUnbond` / `registerNode`.
    function bondRequired(uint256 mbps) public view returns (uint256) {
        return BondMath.bondRequired(mbps, kConstant, alphaWad);
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
    ///         value for the ADR 030 § Region-stability window blacklist-scope
    ///         ripening predicate
    ///         ("the previous region's entries keep applying until the change
    ///         ripens"), now enforced on-chain by `SlashJudge` and
    ///         `ContentBlacklist` via the `regionScopeData` view (#800).
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
        info.lastMultiaddrUpdate = uint64(block.timestamp);

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

        uint32 newCount = lifetimeOffenseCount[operator] + 1;
        lifetimeOffenseCount[operator] = newCount;
        uint256 tierBps = newCount == 1 ? SLASH_BPS_TIER_1 : newCount == 2 ? SLASH_BPS_TIER_2 : SLASH_BPS_TIER_3;

        totalSlash = _reduceBondAtTier(operator, tierBps);
        // Escrow-on-slash (ADR 028): nothing is transferred or burned here.
        // The slashed TOKEN stays in this contract under `_slashRecords` until
        // `finalizeUnappealedSlash` (no appeal) or a `SLASH_APPEAL_ROLE` settle
        // hook resolves it. The challenger is recorded for the 50% leg paid at
        // finality.
        slashId = _mintSlashRecord(operator, challenger, totalSlash);
        _stampSlash(operator, challenger, offenseType, totalSlash, newCount);
    }

    /// @dev Reduce active + unbonding bond at `tierBps` (active first, then
    ///      unbonding, per ADR 003), writing back the balances `BondMath`
    ///      derives. The arithmetic — including the defensive clip that caps a
    ///      >100%-of-at-risk tier (C2 fix; the current ladder maxes at 50%) —
    ///      lives in [`BondMath.reduceAtTier`](BondMath.sol) so it can be
    ///      unit-tested without a full-contract harness.
    function _reduceBondAtTier(address operator, uint256 tierBps) internal returns (uint256 slashAmount) {
        uint256 newActive;
        uint256 newUnbonding;
        (slashAmount, newActive, newUnbonding) =
            BondMath.reduceAtTier(activeBond[operator], unbondingOf[operator].amount, tierBps);
        activeBond[operator] = newActive;
        unbondingOf[operator].amount = newUnbonding;
    }

    /// @dev Allocate the next `slashId`, book the slashed TOKEN into the
    ///      value-typed `escrowedTotal`, and persist the record + operator-list
    ///      append + events via [`SlashEscrowLib.mint`](SlashEscrowLib.sol).
    ///      The record lets `SlashAppeal.openSlashAppeal` validate appeals
    ///      without trusting the appellant's `operator` claim (I2 fix), and pins
    ///      the challenger + filing-window deadline for finality.
    function _mintSlashRecord(address operator, address challenger, uint256 totalSlashAmount)
        internal
        returns (uint256 slashId)
    {
        slashId = slashCounter;
        unchecked {
            slashCounter = slashId + 1;
        }
        escrowedTotal += totalSlashAmount;
        SlashEscrowLib.mint(
            _slashRecords,
            _operatorSlashIds,
            slashId,
            operator,
            challenger,
            totalSlashAmount,
            uint64(APPEAL_FILING_WINDOW)
        );
    }

    /// @dev Stamp `slashedAtEpoch`, fire auto-eject if post-slash active bond
    ///      falls below `minBond / 2`, and emit `Slashed`. Under escrow-on-
    ///      slash no TOKEN moves here — distribution happens at finality.
    function _stampSlash(address operator, address challenger, uint8 offenseType, uint256 totalSlash, uint32 newCount)
        internal
    {
        // Store `actualEpoch + 1` so an epoch-0 slash is not confused with
        // the "unslashed" sentinel. Decoders subtract 1.
        _slashedAtEpoch[operator] = uint64(block.timestamp / EPOCH_LENGTH) + 1;
        emit SlashedAtEpochStamped(operator, _slashedAtEpoch[operator]);

        _maybeAutoEject(operator);

        emit Slashed(operator, challenger, offenseType, newCount, totalSlash);
    }

    /// @dev Auto-eject if post-slash active bond fell below minBond/2.
    function _maybeAutoEject(address operator) internal {
        if (activeBond[operator] >= (minBond / 2) || ejected[operator]) return;
        ejected[operator] = true;
        bytes32 nodeId = _ejectNodeEffects(operator);
        emit AutoEjected(operator, activeBond[operator]);
        if (nodeId != bytes32(0)) emit NodeAutoEjected(nodeId, activeBond[operator]);
    }

    // -----------------------------------------------------------------
    // Escrow finality + appeal settle hooks (ADR 028 escrow-on-slash)
    // -----------------------------------------------------------------

    /// @notice Permissionless: distribute a slash whose filing window lapsed
    ///         with no appeal. 50% to the recorded challenger, 50% burned.
    ///         Reverts if the slash is not `Escrowed` or the window is still
    ///         open (an opened appeal flips the status to `AppealOpen`, so this
    ///         path can never race a live appeal). State machine in
    ///         [`SlashEscrowLib`](SlashEscrowLib.sol); this wrapper holds the
    ///         reentrancy guard / pause and books the `escrowedTotal` release.
    function finalizeUnappealedSlash(uint256 slashId) external nonReentrant whenNotPaused {
        escrowedTotal -= SlashEscrowLib.finalizeUnappealed(_slashRecords, token, slashId, slashCounter, pausedTotal);
    }

    /// @inheritdoc ICapacityBondSlashEscrow
    function markAppealOpen(uint256 slashId) external override whenNotPaused onlyRole(SLASH_APPEAL_ROLE) {
        SlashEscrowLib.markAppealOpen(_slashRecords, slashId, slashCounter, pausedTotal);
    }

    /// @inheritdoc ICapacityBondSlashEscrow
    /// @dev Not `whenNotPaused`: `SlashAppeal` may unpause (and thus credit its
    ///      pause time) independently of this contract's pause state, and the
    ///      effect is purely additive to `pausedTotal` (only ever extends a
    ///      window in the operator's favor), so it is safe to accept while
    ///      paused.
    function creditPauseTime(uint64 delta) external override onlyRole(SLASH_APPEAL_ROLE) {
        pausedTotal += delta;
    }

    /// @inheritdoc ICapacityBondSlashEscrow
    function settleAppealUpheld(uint256 slashId)
        external
        override
        nonReentrant
        whenNotPaused
        onlyRole(SLASH_APPEAL_ROLE)
    {
        escrowedTotal -= SlashEscrowLib.settleUpheld(_slashRecords, token, slashId, slashCounter);
    }

    /// @inheritdoc ICapacityBondSlashEscrow
    function settleAppealGranted(uint256 slashId)
        external
        override
        nonReentrant
        whenNotPaused
        onlyRole(SLASH_APPEAL_ROLE)
    {
        // The library marks the record `Reversed`, recomputes the multi-slash
        // zero-out watermark (ADR 036), and emits `SlashReversed`; the caller
        // applies the value-typed escrow refund below.
        (address operator, uint256 refund) = SlashEscrowLib.settleGranted(
            _slashRecords, _operatorSlashIds, _slashedAtEpoch, slashId, slashCounter, EPOCH_LENGTH
        );
        escrowedTotal -= refund;

        // A wrongly-slashed operator is made whole: the full escrowed bond is
        // refunded liquid. The TOKEN backing it never left the contract.
        if (refund != 0) IERC20(address(token)).safeTransfer(operator, refund);
    }

    /// @notice Emergency: force-resolve an `AppealOpen` slash on the upheld path
    ///         (50% challenger / 50% burn) when the normal `SlashAppeal` flow
    ///         cannot — e.g. `SLASH_APPEAL_ROLE` was revoked mid-migration,
    ///         wedging the record's escrow with no caller able to drive the
    ///         settle hooks. Governance-gated escape hatch (ADR 028); resolves a
    ///         stuck record exactly as `finalizeUnappealedSlash` would have.
    /// @dev Reuses `SlashEscrowLib.settleUpheld`, so it can only act on an
    ///      `AppealOpen` record and produces an identical 50/50 distribution.
    function forceResolveStuckAppeal(uint256 slashId) external nonReentrant onlyRole(GOVERNANCE_ROLE) {
        escrowedTotal -= SlashEscrowLib.settleUpheld(_slashRecords, token, slashId, slashCounter);
    }

    // The multi-slash zero-out recompute lives in
    // [`SlashEscrowLib._recomputeSlashedAtEpoch`](SlashEscrowLib.sol), invoked
    // by `settleAppealGranted` via `SlashEscrowLib.settleGranted`.

    // -----------------------------------------------------------------
    // Blacklist ejection
    // -----------------------------------------------------------------

    function ejectNode(address operator) external override onlyRole(BLACKLIST_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        // Set the governance latch UNCONDITIONALLY — an operator already
        // slash-auto-ejected (`ejected == true`) would otherwise skip the
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
    ///         path (`bond()` reinstatement then `registerNode`), so a still
    ///         slash-deficient operator stays ejected. Idempotent.
    function unEjectNode(address operator) external override onlyRole(BLACKLIST_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        if (blacklistEjected[operator]) {
            blacklistEjected[operator] = false;
            emit BlacklistEjectionCleared(operator);
        }
    }

    // -----------------------------------------------------------------
    // Settlement reporter callback (FeeRouter)
    // -----------------------------------------------------------------

    function recordSettlement(address operator) external override onlyRole(SETTLEMENT_REPORTER_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        emit SettlementRecorded(operator);
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

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

    /// @notice Wire the SlashJudge whose `maxEvidenceAgeUs` bounds how low
    ///         `unbondingPeriod` may be set (ADR 014 § Interaction with unbonding
    ///         period). Set post-deploy (SlashJudge is deployed after this
    ///         contract). Re-settable by governance so a redeployed SlashJudge can
    ///         be repointed; the zero address is rejected so the invariant cannot
    ///         be silently disabled once wired.
    function setSlashJudge(ISlashJudgeEvidenceView newSlashJudge) external onlyRole(GOVERNANCE_ROLE) {
        if (address(newSlashJudge) == address(0)) revert ZeroAddress();
        // Reject a judge that would violate the paired invariant against the CURRENT
        // unbonding period (ADR 014): maxEvidenceAgeUs MUST stay strictly below
        // unbondingPeriod * 1e6, so the invariant holds the instant the judge is
        // wired, not only after the next setUnbondingPeriod. `unbondingPeriod` is
        // bounded [7d,60d] so the multiply cannot overflow.
        uint256 unbondingUs = unbondingPeriod * 1_000_000;
        // `maxEvidenceAgeUs()` is a view on the about-to-be-wired `slashJudge`; this
        // setter is GOVERNANCE_ROLE-gated, so no reentrancy vector (aderyn FP).
        // aderyn-ignore-next-line(reentrancy-state-change)
        uint256 maxAgeUs = newSlashJudge.maxEvidenceAgeUs();
        if (unbondingUs <= maxAgeUs) revert UnbondingBelowEvidenceAge(unbondingUs, maxAgeUs);
        address old = address(slashJudge);
        slashJudge = newSlashJudge;
        emit SlashJudgeUpdated(old, address(newSlashJudge));
    }

    function setUnbondingPeriod(uint256 newPeriod) external onlyRole(GOVERNANCE_ROLE) {
        _enforceUnbondingPeriodBounds(newPeriod);
        // Paired cross-parameter invariant (ADR 014 § Interaction with unbonding
        // period): the unbonding period (in microseconds) MUST stay strictly above
        // the evidence-age ceiling, else a node could offend, unbond, and withdraw
        // before evidence can be submitted. Mirrors SlashJudge._enforceUnbondingInvariant.
        // Skipped until `slashJudge` is wired (deploy window); `newPeriod` is bounded
        // [7d,60d] so `newPeriod * 1_000_000` cannot overflow.
        ISlashJudgeEvidenceView judge = slashJudge;
        if (address(judge) != address(0)) {
            uint256 newPeriodUs = newPeriod * 1_000_000;
            // `maxEvidenceAgeUs()` is a view on the governance-set `slashJudge`; this
            // setter is GOVERNANCE_ROLE-gated, so the following state write is not a
            // reentrancy vector (aderyn reentrancy-state-change FP).
            // aderyn-ignore-next-line(reentrancy-state-change)
            uint256 maxAgeUs = judge.maxEvidenceAgeUs();
            if (newPeriodUs <= maxAgeUs) revert UnbondingBelowEvidenceAge(newPeriodUs, maxAgeUs);
        }
        uint256 oldPeriod = unbondingPeriod;
        unbondingPeriod = newPeriod;
        emit UnbondingPeriodUpdated(oldPeriod, newPeriod);
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

    /// @dev Stamp the pause start so `_unpause` can accumulate the duration
    ///      into `pausedTotal` (ADR 028 §5 window extension).
    function _pause() internal override {
        // forge-lint: disable-next-line(block-timestamp)
        _pausedAt = uint64(block.timestamp);
        super._pause();
    }

    /// @dev Accumulate the just-ended pause interval into `pausedTotal`.
    function _unpause() internal override {
        // forge-lint: disable-next-line(block-timestamp)
        pausedTotal += uint64(block.timestamp) - _pausedAt;
        _pausedAt = 0;
        super._unpause();
    }

    // -----------------------------------------------------------------
    // Views — bond / governor surface
    // -----------------------------------------------------------------

    function bondOf(address operator) external view returns (uint256) {
        return activeBond[operator];
    }

    function getBondMultiple(address operator) external view returns (uint256) {
        return activeBond[operator] / minBond;
    }

    function isActive(address operator) public view returns (bool) {
        // slither-disable-next-line incorrect-equality
        return _nodes[operator].active && activeBond[operator] >= minBond && unbondingOf[operator].amount == 0
            && !ejected[operator];
    }

    /// @inheritdoc ICapacityBond
    function firstBondedAt(address operator) external view override returns (uint64) {
        return _firstBondedAt[operator];
    }

    /// @inheritdoc ICapacityBond
    function slashedAtEpoch(address operator) external view override returns (uint64) {
        return _slashedAtEpoch[operator];
    }

    /// @inheritdoc ICapacityBond
    function slashRecords(uint256 slashId)
        external
        view
        override
        returns (address operator, uint64 slashedAt_, uint256 slashAmount)
    {
        if (slashId >= slashCounter) revert UnknownSlash(slashId);
        SlashRecord memory r = _slashRecords[slashId];
        return (r.operator, r.slashedAt, r.slashAmount);
    }

    /// @notice Full slash record including escrow status and the recorded
    ///         challenger / filing-window deadline. Convenience view for
    ///         indexers, keepers, and `SlashAppeal`.
    function getSlashRecord(uint256 slashId) external view returns (SlashRecord memory) {
        if (slashId >= slashCounter) revert UnknownSlash(slashId);
        return _slashRecords[slashId];
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

    /// @inheritdoc ICapacityBondRegionView
    function regionScopeData(address operator)
        external
        view
        returns (
            string memory regionHint,
            string memory regionPrev_,
            uint64 regionLastChanged_,
            uint64 firstBondedAt_,
            uint64 regionGateActivatedAt_,
            uint256 regionStabilityWindow_
        )
    {
        return (
            _nodes[operator].regionHint,
            regionPrev[operator],
            regionLastChanged[operator],
            _firstBondedAt[operator],
            regionGateActivatedAt,
            regionStabilityWindow
        );
    }

    function isActiveNode(bytes32 nodeId) external view returns (bool) {
        address ethAddress = nodeIdToAddress[nodeId];
        if (ethAddress == address(0)) return false;
        return isActive(ethAddress);
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
