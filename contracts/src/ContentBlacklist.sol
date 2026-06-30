// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

import { ICapacityBondEjector } from "./interfaces/ICapacityBondEjector.sol";
import { ICapacityBondRegionView } from "./interfaces/ICapacityBondRegionView.sol";
import { RegionScopeLib } from "./RegionScopeLib.sol";

/// @title ContentBlacklist
/// @notice Global + regional hash blacklist, operator-level blacklist, and
///         origin blacklist (ADR 011 § Content Takedown). Layered with the
///         ADR 031 blacklist-entry appeal surface — operators / publishers /
///         token holders can post a TOKEN bond to challenge an entry; the
///         appeal flows through the same {open → fast-track → ratify/reverse}
///         lifecycle as `SlashAppeal` slashing appeals.
/// @dev    Simplifications vs. ADR 031 carried for this revision:
///           - Standing enforcement is partial: `StandingPath.Operator`
///             filings require a current-region match (ADR 011 § Standing
///             path 2, via ADR 030), but the `StandingPath.TokenHolder`
///             synthetic-standing clawback (balance check at T and T+24h)
///             is deferred — `Publisher`/`TokenHolder` are enum-range
///             validated only at filing time.
///         The interim-relief concurrent fast-track cap is two-tier: a
///         per-region ceiling (`REGION_CONCURRENT_RELIEF_CAP`) plus a
///         per-(filer, region) sub-cap (`FILER_CONCURRENT_RELIEF_CAP`) so no
///         single filer can monopolize a region's relief slots. The rejection
///         cooldown (`filerRejections`, three rejections inside a rolling,
///         governance-tunable window → a full-window lockout) and the perjury
///         denylist (`perjuryDenylistUntilAt`, a 365-day lockout set via
///         `rejectAppealAsPerjury`) are both implemented.
contract ContentBlacklist is AccessControl, ReentrancyGuard {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant EMERGENCY_MULTISIG_ROLE = keccak256("EMERGENCY_MULTISIG_ROLE");
    bytes32 public constant REGIONAL_BODY_ROLE = keccak256("REGIONAL_BODY_ROLE");

    // -----------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------

    uint256 internal constant APPEAL_FILING_WINDOW = 14 days;
    uint256 internal constant APPEAL_REVIEW_WINDOW = 14 days;
    uint256 internal constant APPEAL_RATIFICATION_WINDOW = 14 days;
    uint256 internal constant APPEAL_FREQUENCY_WINDOW = 90 days;
    // Interim-relief concurrent fast-track cap, two-tier (ADR 031):
    //   - `REGION_CONCURRENT_RELIEF_CAP` is the global per-region ceiling on
    //     simultaneously-suspended entries (formerly `BODY_CONCURRENT_APPEAL_CAP`
    //     — renamed because enforcement is per region, not per requesting body).
    //   - `FILER_CONCURRENT_RELIEF_CAP` is a per-(filer, region) sub-cap so a
    //     single adversarial filer cannot monopolize a region's relief slots.
    //     Strictly below the region ceiling, so at least one slot is always
    //     reachable by other filers. Both are fixed constants (no setter).
    uint256 internal constant REGION_CONCURRENT_RELIEF_CAP = 3;
    uint256 internal constant FILER_CONCURRENT_RELIEF_CAP = 2;
    // Deviation from ADR 011 § Bond and frequency caps (spec is
    // [100e18, 10_000e18]). Bounds halved to [50e18, 5000e18] for the
    // testnet phase so appeal-bond economics scale with the smaller TGE
    // float; widen on production redeploy.
    uint256 internal constant APPEAL_BOND_FLOOR = 50e18;
    uint256 internal constant APPEAL_BOND_CEILING = 5000e18;

    // ADR 031 § APPEAL_FILER_REJECTION_COOLDOWN — three rejections inside a
    // rolling window lock the filer out of `openBlacklistAppeal` for another
    // full window (audit finding H-4). The threshold is fixed at three by the
    // `RejectionWindow` fixed-size ring; the window length is governance-tunable
    // via `setRejectionCooldownWindow`, bounded for the same reason `appealBond`
    // is. Default matches the ADR 011 90-day rolling-cooldown spec.
    uint64 internal constant REJECTION_COOLDOWN_WINDOW_DEFAULT = 90 days;
    uint64 internal constant REJECTION_COOLDOWN_WINDOW_FLOOR = 1 days;
    uint64 internal constant REJECTION_COOLDOWN_WINDOW_CEILING = 365 days;

    // ADR 031 § Evidence (perjury denylist) — a single adjudicated bad-faith /
    // false-sworn-declaration appeal costs the filer the right to open new
    // appeals for a fixed term. Distinct from the rejection cooldown, which
    // throttles *volume* abuse (three rejections in a rolling window); this
    // penalizes one egregious act. Multisig-gated via `rejectAppealAsPerjury`
    // and tied to a concrete finalized appeal (auditable), so the duration is a
    // fixed constant with no governance setter.
    uint64 internal constant PERJURY_DENYLIST_DURATION = 365 days;

    bytes32 internal constant GLOBAL_REGION = bytes32("GLOBAL");

    // -----------------------------------------------------------------
    // Standing paths (ADR 031)
    // -----------------------------------------------------------------

    enum StandingPath {
        Publisher,
        Operator,
        TokenHolder
    }

    enum AppealStatus {
        Open,
        FastTracked,
        Rejected,
        Ratified,
        Reversed,
        Lapsed
    }

    // -----------------------------------------------------------------
    // Immutables / governance-mutable
    // -----------------------------------------------------------------

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ICapacityBondEjector public immutable capacityBond;

    /// @dev Same deployed `CapacityBond` as `capacityBond`, typed for the ADR 030
    ///      region-scope read surface (current/prev region + ripening inputs).
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ICapacityBondRegionView public immutable capacityBondRegion;

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ERC20Burnable public immutable token;

    uint256 public appealBond;

    /// @notice Governance-tunable rolling window for the ADR 031 rejection
    ///         cooldown — serves as both the lookback over a filer's recent
    ///         rejections and the lockout duration once three accumulate.
    ///         Bounded to [REJECTION_COOLDOWN_WINDOW_FLOOR,
    ///         REJECTION_COOLDOWN_WINDOW_CEILING]. `uint64` like every other
    ///         appeal-machinery time field, so no widen/narrow casts are needed.
    uint64 public rejectionCooldownWindow;

    // -----------------------------------------------------------------
    // Storage — blacklist entries
    // -----------------------------------------------------------------

    /// @notice Per-(region, hash) entry. `region = GLOBAL_REGION` is the
    ///         global scope. `addedAt == 0` means "not blacklisted".
    struct HashEntry {
        uint64 addedAt;
        bool suspended;
    }

    mapping(bytes32 region => mapping(bytes32 hash => HashEntry)) internal _hashEntries;

    /// @notice Operator-level blacklist (ADR 011 § Decision — operator-level
    ///         blacklist evicts the operator from `CapacityBond` via
    ///         `ejectNode`).
    mapping(address operator => bool) public isOperatorBlacklisted;

    /// @notice Origin-level blacklist (ADR 011 § Hash Evasion and Origin
    ///         Blacklisting).
    mapping(address origin => bool) public isOriginBlacklisted;

    // -----------------------------------------------------------------
    // Storage — appeals
    // -----------------------------------------------------------------

    struct BlacklistAppeal {
        bytes32 hash;
        bytes32 region;
        bytes32 evidenceBundleHash;
        address filer;
        uint256 bond;
        uint64 openedAt;
        uint64 fastTrackedAt;
        StandingPath standingPath;
        AppealStatus status;
    }

    BlacklistAppeal[] internal _appeals;

    mapping(address filer => uint64 lastSuccessAt) public lastRatifiedSuccessAt;
    mapping(bytes32 region => uint256 active) public regionActiveReliefCount;

    /// @notice Per-(region, filer) count of active interim-relief slots — the
    ///         per-filer tier of the two-tier concurrent fast-track cap
    ///         (`FILER_CONCURRENT_RELIEF_CAP`). Incremented alongside
    ///         `regionActiveReliefCount` in `fastTrackBlacklistAppeal` and
    ///         decremented at every exit from `FastTracked`. Public auto-getter
    ///         `filerRegionActiveRelief(region, filer)`.
    mapping(bytes32 region => mapping(address filer => uint256 active)) public filerRegionActiveRelief;

    /// @notice `true` when there is an Open or FastTracked appeal for the
    ///         given `(region, hash)`. Prevents concurrent appeals on the
    ///         same entry — without this guard, multiple fast-tracked
    ///         appeals could share a single `suspended` flag, and the first
    ///         to terminate (reject/lapse/reverse) would clear `suspended`
    ///         out from under any remaining live appeals (state-override
    ///         bug across concurrent appeals).
    mapping(bytes32 region => mapping(bytes32 hash => bool)) public hasActiveAppeal;

    /// @notice ADR 031 § APPEAL_FILER_REJECTION_COOLDOWN rolling-window state.
    ///         `rejections` holds the three most recent rejection timestamps
    ///         (chronological; 0 marks an empty slot); `cooldownUntilAt` is
    ///         non-zero while the filer is locked out. Packs into one slot.
    struct RejectionWindow {
        uint64[3] rejections;
        uint64 cooldownUntilAt;
    }

    /// @notice Per-filer rejection cooldown state. Throttles abusive filers
    ///         whose appeals are repeatedly rejected (audit finding H-4) — the
    ///         success-path `lastRatifiedSuccessAt` cap does not cover them, so
    ///         without this the only brake on rejected-appeal spam is the bond.
    mapping(address filer => RejectionWindow) public filerRejections;

    /// @notice ADR 031 § Evidence perjury denylist. Non-zero `until` while the
    ///         filer is locked out of `openBlacklistAppeal` for an adjudicated
    ///         bad-faith appeal; set by `rejectAppealAsPerjury` to
    ///         `block.timestamp + PERJURY_DENYLIST_DURATION`. Public auto-getter.
    mapping(address filer => uint64 until) public perjuryDenylistUntilAt;

    // -----------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------

    event HashBlacklisted(bytes32 indexed region, bytes32 indexed hash);
    event HashRemoved(bytes32 indexed region, bytes32 indexed hash);
    event OperatorBlacklisted(address indexed operator);
    event OperatorBlacklistCleared(address indexed operator);
    event OriginBlacklistUpdated(address indexed origin, bool blacklisted);

    event BlacklistAppealOpened(
        uint256 indexed appealId,
        bytes32 indexed hash,
        bytes32 indexed region,
        address filer,
        StandingPath standingPath,
        bytes32 evidenceBundleHash,
        uint256 bond
    );
    event BlacklistAppealFastTracked(uint256 indexed appealId);
    event BlacklistAppealRejected(uint256 indexed appealId, uint256 bondBurned);
    /// @notice Emitted (in addition to `BlacklistAppealRejected`) when an appeal
    ///         is rejected as perjury, recording the filer's denylist expiry.
    event BlacklistAppealRejectedAsPerjury(uint256 indexed appealId, address indexed filer, uint64 until);
    event BlacklistAppealRatified(uint256 indexed appealId);
    event BlacklistAppealReversed(uint256 indexed appealId, uint256 bondBurned);
    event BlacklistAppealLapsed(uint256 indexed appealId, uint8 reason);

    event AppealBondUpdated(uint256 oldValue, uint256 newValue);
    event RejectionCooldownWindowUpdated(uint64 oldValue, uint64 newValue);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error ZeroHash();
    error MissingRegion();
    error EntryNotBlacklisted(bytes32 region, bytes32 hash);
    /// @notice Raised by `openBlacklistAppeal` when the (region, hash) entry
    ///         exists but its 14-day appeal filing window has elapsed.
    ///         Distinct from `EntryNotBlacklisted` so off-chain callers can
    ///         tell "never blacklisted" from "too late to appeal."
    error AppealFilingWindowClosed(bytes32 region, bytes32 hash);
    error AppealNotOpen(uint256 appealId);
    error AppealNotFastTracked(uint256 appealId);
    error ReviewWindowOpen(uint64 readyAt);
    error RatificationWindowOpen(uint64 readyAt);
    /// @notice The per-region interim-relief ceiling (`REGION_CONCURRENT_RELIEF_CAP`) is full.
    error RegionalCapHit(bytes32 region, uint256 cap);
    /// @notice The filer's per-(filer, region) interim-relief sub-cap (`FILER_CONCURRENT_RELIEF_CAP`) is full.
    error FilerReliefCapHit(address filer, uint256 cap);
    error AppealAlreadyActive(bytes32 region, bytes32 hash);
    error HashHasActiveAppeal(bytes32 region, bytes32 hash);
    error FrequencyCapHit(uint64 nextAvailableAt);
    error FilerInRejectionCooldown(uint64 cooldownUntilAt);
    error FilerPerjuryDenylisted(uint64 until);
    error UnauthorizedStanding(StandingPath path);
    error OperatorRegionMismatch(bytes32 appealRegion, bytes32 filerRegion);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    constructor(ICapacityBondEjector capacityBond_, ERC20Burnable token_, address admin, uint256 appealBond_) {
        if (address(capacityBond_) == address(0) || address(token_) == address(0) || admin == address(0)) {
            revert ZeroAddress();
        }
        _enforceAppealBondBounds(appealBond_);
        capacityBond = capacityBond_;
        capacityBondRegion = ICapacityBondRegionView(address(capacityBond_));
        token = token_;
        appealBond = appealBond_;
        rejectionCooldownWindow = REJECTION_COOLDOWN_WINDOW_DEFAULT;
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    // -----------------------------------------------------------------
    // Core blacklist surface (ADR 011)
    // -----------------------------------------------------------------

    /// @notice Add `hash` to the global blacklist. `GOVERNANCE_ROLE` only.
    function addHashGlobal(bytes32 hash) external onlyRole(GOVERNANCE_ROLE) {
        _addHash(GLOBAL_REGION, hash);
    }

    /// @notice Add `hash` to the regional blacklist for `region`. Carries
    ///         `REGIONAL_BODY_ROLE` — granted per-region in
    ///         `registerRegionalBody` per ADR 016 § Post-Deployment, step 8.
    function addHashRegional(bytes32 region, bytes32 hash) external onlyRole(REGIONAL_BODY_ROLE) {
        if (region == bytes32(0) || region == GLOBAL_REGION) revert MissingRegion();
        _addHash(region, hash);
    }

    function removeHashGlobal(bytes32 hash) external onlyRole(GOVERNANCE_ROLE) {
        _removeHashRegional(GLOBAL_REGION, hash);
    }

    /// @dev `REGIONAL_BODY_ROLE` must NOT be able to remove `GLOBAL_REGION`
    ///      entries — that would let any regional body bypass governance and
    ///      delete a global blacklist. Mirrors the guard on `addHashRegional`.
    function removeHashRegional(bytes32 region, bytes32 hash) external onlyRole(REGIONAL_BODY_ROLE) {
        if (region == bytes32(0) || region == GLOBAL_REGION) revert MissingRegion();
        _removeHashRegional(region, hash);
    }

    /// @dev `nonReentrant` for consistency with the appeal mutators: this is the
    ///      one path that makes an external state-changing call
    ///      (`capacityBond.ejectNode`) after a state write (M-4). `ejectNode` is
    ///      a trusted contract, but the guard hardens against a future hook.
    function addOperator(address operator) external nonReentrant onlyRole(GOVERNANCE_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        if (!isOperatorBlacklisted[operator]) {
            isOperatorBlacklisted[operator] = true;
            emit OperatorBlacklisted(operator);
            capacityBond.ejectNode(operator);
        }
    }

    /// @dev `nonReentrant` for the same reason as `addOperator`: this makes an
    ///      external state-changing call (`capacityBond.unEjectNode`) after a
    ///      state write (M-4). `unEjectNode` is called UNCONDITIONALLY — outside
    ///      the local-flag guard — and is idempotent: this re-syncs the two
    ///      contracts even when `CapacityBond.blacklistEjected` was latched
    ///      without a matching local entry (e.g. a `BLACKLIST_ROLE` holder that
    ///      called `ejectNode` directly). The guarded form could never repair
    ///      that drift, leaving the operator permanently latched and unable to
    ///      re-bond. The local-flag guard still scopes the `isOperatorBlacklisted`
    ///      clear + `OperatorBlacklistCleared` event to a genuine state change.
    function removeOperator(address operator) external nonReentrant onlyRole(GOVERNANCE_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        if (isOperatorBlacklisted[operator]) {
            isOperatorBlacklisted[operator] = false;
            emit OperatorBlacklistCleared(operator);
        }
        capacityBond.unEjectNode(operator);
    }

    function setOriginBlacklist(address origin, bool blacklisted) external onlyRole(GOVERNANCE_ROLE) {
        if (origin == address(0)) revert ZeroAddress();
        isOriginBlacklisted[origin] = blacklisted;
        emit OriginBlacklistUpdated(origin, blacklisted);
    }

    function isHashBlacklisted(bytes32 hash) external view returns (bool) {
        return _isLive(GLOBAL_REGION, hash);
    }

    function isHashBlacklistedInRegion(bytes32 hash, bytes32 region) external view returns (bool) {
        if (_isLive(GLOBAL_REGION, hash)) return true;
        return _isLive(region, hash);
    }

    /// @notice True iff `hash` is a live blacklist entry IN SCOPE for `operator`
    ///         under the ADR 030 § Region-stability window ripening predicate:
    ///         global ∪ current-region ∪ (within ripening window) prev-region.
    ///         Read-only companion to the `SlashJudge` slash-eligibility gate
    ///         (which additionally anchors each leg to the served-response time);
    ///         this answers "in scope right now".
    /// @dev    Reads the operator's region inputs from `CapacityBond` via
    ///         `regionScopeData` and evaluates the predicate with `RegionScopeLib`.
    // slither-disable-next-line unused-return
    function isHashBlacklistedForOperator(bytes32 hash, address operator) external view returns (bool) {
        if (_isLive(GLOBAL_REGION, hash)) return true;
        (
            string memory regionHint,
            string memory regionPrev,
            uint64 regionLastChanged,
            uint64 firstBondedAt,
            uint64 gateActivatedAt,
            uint256 window
        ) = capacityBondRegion.regionScopeData(operator);

        uint64 effective = RegionScopeLib.effectiveSince(regionLastChanged, firstBondedAt, gateActivatedAt);
        // forge-lint: disable-next-line(block-timestamp)
        (bytes32 cur, bytes32 prev, bool prevApplies) = RegionScopeLib.scopedRegions(
            GLOBAL_REGION, regionHint, regionPrev, uint64(block.timestamp), effective, window
        );

        if (cur != bytes32(0) && _isLive(cur, hash)) return true;
        if (prevApplies && _isLive(prev, hash)) return true;
        return false;
    }

    function getHashEntry(bytes32 region, bytes32 hash) external view returns (HashEntry memory) {
        return _hashEntries[region][hash];
    }

    // -----------------------------------------------------------------
    // Appeals (ADR 031)
    // -----------------------------------------------------------------

    // slither attributes the `regionScopeData` tuple-destructuring unused-return
    // (path-2 standing check) to the enclosing function, so the directive sits here.
    // slither-disable-next-line unused-return
    function openBlacklistAppeal(bytes32 hash, bytes32 region, bytes32 evidenceBundleHash, StandingPath standingPath)
        external
        nonReentrant
        returns (uint256 appealId)
    {
        if (hash == bytes32(0)) revert ZeroHash();
        // SF-M1 fix: callers must pass GLOBAL_REGION explicitly. Silently
        // rewriting `bytes32(0)` to global would burn the bond + 90-day
        // cooldown on the wrong scope for a caller who forgot the arg.
        if (region == bytes32(0)) revert MissingRegion();
        HashEntry memory entry = _hashEntries[region][hash];
        if (entry.addedAt == 0) revert EntryNotBlacklisted(region, hash);
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp > uint256(entry.addedAt) + APPEAL_FILING_WINDOW) {
            revert AppealFilingWindowClosed(region, hash);
        }
        // Single live appeal per (region, hash) — see `hasActiveAppeal`
        // declaration for the override bug this prevents. The per-region
        // concurrent cap is NOT enforced here: an Open appeal grants no interim
        // relief, so un-acted-upon filings must not crowd out others. The cap
        // is charged only once an appeal is fast-tracked (M-1).
        if (hasActiveAppeal[region][hash]) revert AppealAlreadyActive(region, hash);
        uint64 last = lastRatifiedSuccessAt[msg.sender];
        if (last != 0) {
            uint64 nextAvailable = last + uint64(APPEAL_FREQUENCY_WINDOW);
            // forge-lint: disable-next-line(block-timestamp)
            if (block.timestamp < nextAvailable) revert FrequencyCapHit(nextAvailable);
        }

        // ADR 031 § Evidence perjury denylist: a filer adjudicated to have filed
        // a bad-faith / false-sworn-declaration appeal (via `rejectAppealAsPerjury`)
        // is locked out for `PERJURY_DENYLIST_DURATION`. Checked BEFORE the
        // volume-based cooldown so a filer who is both perjury-denylisted and in
        // a rejection cooldown sees the more specific, more severe perjury ban.
        uint64 perjuryUntil = perjuryDenylistUntilAt[msg.sender];
        // forge-lint: disable-next-line(block-timestamp)
        if (perjuryUntil > block.timestamp) revert FilerPerjuryDenylisted(perjuryUntil);

        // ADR 031 § APPEAL_FILER_REJECTION_COOLDOWN (audit H-4): an abusive
        // filer whose appeals keep getting rejected is locked out for the
        // rolling window, independent of the success-path frequency cap above.
        // Both gates are cheap single SLOADs preceding the standing / external-call checks.
        uint64 cooldownUntilAt = filerRejections[msg.sender].cooldownUntilAt;
        // forge-lint: disable-next-line(block-timestamp)
        if (cooldownUntilAt > block.timestamp) revert FilerInRejectionCooldown(cooldownUntilAt);

        // Enum-range check first. Operator standing additionally gets the
        // current-region match below (ADR 011 § Standing path 2); the
        // TokenHolder synthetic-standing clawback (admissibility condition (c)
        // per ADR 031 § 216 — `status == Open && standingPath == TokenHolder &&
        // ...`) is still deferred per the contract header note. The field is
        // persisted so the future on-chain clawback check can read it from
        // the appeal record without re-deriving it from event history.
        if (uint8(standingPath) > uint8(StandingPath.TokenHolder)) revert UnauthorizedStanding(standingPath);

        // ADR 011 § Standing path 2 (Operator): an operator only has standing on
        // a regional entry whose region matches their current attested region.
        // The ADR 030 ripening window is a SOFT norm here — in-window filings are
        // "not auto-rejected … multisig discretion" — so we enforce only the hard
        // current-region match, never a window revert. Global entries are appealable
        // by an in-scope operator regardless of region.
        if (standingPath == StandingPath.Operator && region != GLOBAL_REGION) {
            // `regionScopeData` is a view on the trusted immutable `capacityBond`
            // and this function is `nonReentrant`, so the later appeal-state writes
            // are not a reentrancy vector (aderyn reentrancy-state-change FP).
            // aderyn-ignore-next-line(reentrancy-state-change)
            (string memory filerRegion,,,,,) = capacityBondRegion.regionScopeData(msg.sender);
            bytes32 filerKey = RegionScopeLib.pack(filerRegion);
            if (filerKey != region) revert OperatorRegionMismatch(region, filerKey);
        }

        IERC20(address(token)).safeTransferFrom(msg.sender, address(this), appealBond);

        appealId = _appeals.length;
        _appeals.push(
            BlacklistAppeal({
                hash: hash,
                region: region,
                evidenceBundleHash: evidenceBundleHash,
                filer: msg.sender,
                bond: appealBond,
                openedAt: uint64(block.timestamp),
                fastTrackedAt: 0,
                standingPath: standingPath,
                status: AppealStatus.Open
            })
        );
        hasActiveAppeal[region][hash] = true;

        emit BlacklistAppealOpened(appealId, hash, region, msg.sender, standingPath, evidenceBundleHash, appealBond);
    }

    function fastTrackBlacklistAppeal(uint256 appealId) external nonReentrant onlyRole(EMERGENCY_MULTISIG_ROLE) {
        BlacklistAppeal storage a = _appeals[appealId];
        if (a.status != AppealStatus.Open) revert AppealNotOpen(appealId);
        // Both caps count only fast-tracked appeals — the ones actually holding
        // interim relief (a suspended entry) — so they bound simultaneous
        // suspensions without letting un-acted Open filings consume the budget
        // (M-1). The per-region ceiling is the global backstop; the per-(filer,
        // region) sub-cap stops one filer monopolizing a region's slots.
        if (regionActiveReliefCount[a.region] >= REGION_CONCURRENT_RELIEF_CAP) {
            revert RegionalCapHit(a.region, REGION_CONCURRENT_RELIEF_CAP);
        }
        if (filerRegionActiveRelief[a.region][a.filer] >= FILER_CONCURRENT_RELIEF_CAP) {
            revert FilerReliefCapHit(a.filer, FILER_CONCURRENT_RELIEF_CAP);
        }
        a.fastTrackedAt = uint64(block.timestamp);
        a.status = AppealStatus.FastTracked;
        regionActiveReliefCount[a.region] += 1;
        filerRegionActiveRelief[a.region][a.filer] += 1;
        _hashEntries[a.region][a.hash].suspended = true;
        emit BlacklistAppealFastTracked(appealId);
    }

    function rejectBlacklistAppeal(uint256 appealId) external nonReentrant onlyRole(EMERGENCY_MULTISIG_ROLE) {
        BlacklistAppeal storage a = _appeals[appealId];
        if (a.status != AppealStatus.Open && a.status != AppealStatus.FastTracked) revert AppealNotOpen(appealId);
        if (a.status == AppealStatus.FastTracked) {
            _hashEntries[a.region][a.hash].suspended = false;
            // Only fast-tracked appeals charge the relief caps (M-1); release both tiers.
            if (regionActiveReliefCount[a.region] != 0) regionActiveReliefCount[a.region] -= 1;
            if (filerRegionActiveRelief[a.region][a.filer] != 0) filerRegionActiveRelief[a.region][a.filer] -= 1;
        }
        uint256 bondBurned = a.bond;
        a.bond = 0;
        a.status = AppealStatus.Rejected;
        hasActiveAppeal[a.region][a.hash] = false;
        _recordRejection(a.filer);
        if (bondBurned != 0) token.burn(bondBurned);
        emit BlacklistAppealRejected(appealId, bondBurned);
    }

    /// @notice Reject an appeal as perjury (ADR 031 § Evidence): everything
    ///         `rejectBlacklistAppeal` does — burn the bond, record the rolling-
    ///         window rejection, release any fast-track suspension + relief slot —
    ///         plus deny the filer the appeal path for `PERJURY_DENYLIST_DURATION`.
    ///         Tied to a concrete finalized appeal so the denylist entry is
    ///         auditable; gated to the same `EMERGENCY_MULTISIG_ROLE` as
    ///         `fastTrackBlacklistAppeal` (ADR 009 capability 3 sub-mode, no new
    ///         role). Off-chain adjudication criteria for "egregious bad faith /
    ///         false sworn declaration" live in ADR prose, not contract logic.
    function rejectAppealAsPerjury(uint256 appealId) external nonReentrant onlyRole(EMERGENCY_MULTISIG_ROLE) {
        BlacklistAppeal storage a = _appeals[appealId];
        if (a.status != AppealStatus.Open && a.status != AppealStatus.FastTracked) revert AppealNotOpen(appealId);

        uint64 until = uint64(block.timestamp) + PERJURY_DENYLIST_DURATION;
        perjuryDenylistUntilAt[a.filer] = until;

        if (a.status == AppealStatus.FastTracked) {
            _hashEntries[a.region][a.hash].suspended = false;
            // Only fast-tracked appeals charge the relief caps (M-1); release both tiers.
            if (regionActiveReliefCount[a.region] != 0) regionActiveReliefCount[a.region] -= 1;
            if (filerRegionActiveRelief[a.region][a.filer] != 0) filerRegionActiveRelief[a.region][a.filer] -= 1;
        }
        uint256 bondBurned = a.bond;
        a.bond = 0;
        a.status = AppealStatus.Rejected;
        hasActiveAppeal[a.region][a.hash] = false;
        _recordRejection(a.filer);
        emit BlacklistAppealRejectedAsPerjury(appealId, a.filer, until);
        if (bondBurned != 0) token.burn(bondBurned);
        emit BlacklistAppealRejected(appealId, bondBurned);
    }

    function ratifyBlacklistAppealRemoval(uint256 appealId) external nonReentrant onlyRole(GOVERNANCE_ROLE) {
        BlacklistAppeal storage a = _appeals[appealId];
        if (a.status != AppealStatus.FastTracked) revert AppealNotFastTracked(appealId);

        address filer = a.filer;
        uint256 bondRefund = a.bond;
        bytes32 hash = a.hash;
        bytes32 region = a.region;

        a.bond = 0;
        a.status = AppealStatus.Ratified;
        if (regionActiveReliefCount[region] != 0) regionActiveReliefCount[region] -= 1;
        if (filerRegionActiveRelief[region][filer] != 0) filerRegionActiveRelief[region][filer] -= 1;
        hasActiveAppeal[region][hash] = false;
        lastRatifiedSuccessAt[filer] = uint64(block.timestamp);

        _removeHashRegional(region, hash);
        IERC20(address(token)).safeTransfer(filer, bondRefund);
        emit BlacklistAppealRatified(appealId);
    }

    function reverseBlacklistAppeal(uint256 appealId) external nonReentrant onlyRole(GOVERNANCE_ROLE) {
        BlacklistAppeal storage a = _appeals[appealId];
        if (a.status != AppealStatus.FastTracked) revert AppealNotFastTracked(appealId);

        uint256 bondBurned = a.bond;
        a.bond = 0;
        a.status = AppealStatus.Reversed;
        _hashEntries[a.region][a.hash].suspended = false;
        if (regionActiveReliefCount[a.region] != 0) regionActiveReliefCount[a.region] -= 1;
        if (filerRegionActiveRelief[a.region][a.filer] != 0) filerRegionActiveRelief[a.region][a.filer] -= 1;
        hasActiveAppeal[a.region][a.hash] = false;
        if (bondBurned != 0) token.burn(bondBurned);
        emit BlacklistAppealReversed(appealId, bondBurned);
    }

    function cleanupExpiredBlacklistAppeal(uint256 appealId) external nonReentrant {
        BlacklistAppeal storage a = _appeals[appealId];
        if (a.status == AppealStatus.Open) {
            uint64 readyAt = a.openedAt + uint64(APPEAL_REVIEW_WINDOW);
            // forge-lint: disable-next-line(block-timestamp)
            if (block.timestamp < readyAt) revert ReviewWindowOpen(readyAt);
            uint256 bondBurned = a.bond;
            a.bond = 0;
            a.status = AppealStatus.Lapsed;
            // Open appeals never charged the relief cap (M-1) — nothing to release.
            hasActiveAppeal[a.region][a.hash] = false;
            if (bondBurned != 0) token.burn(bondBurned);
            emit BlacklistAppealLapsed(appealId, 1);
            return;
        }
        if (a.status == AppealStatus.FastTracked) {
            uint64 readyAt = a.fastTrackedAt + uint64(APPEAL_RATIFICATION_WINDOW);
            // forge-lint: disable-next-line(block-timestamp)
            if (block.timestamp < readyAt) revert RatificationWindowOpen(readyAt);
            uint256 bondBurned = a.bond;
            a.bond = 0;
            a.status = AppealStatus.Lapsed;
            _hashEntries[a.region][a.hash].suspended = false;
            if (regionActiveReliefCount[a.region] != 0) regionActiveReliefCount[a.region] -= 1;
            if (filerRegionActiveRelief[a.region][a.filer] != 0) filerRegionActiveRelief[a.region][a.filer] -= 1;
            hasActiveAppeal[a.region][a.hash] = false;
            if (bondBurned != 0) token.burn(bondBurned);
            emit BlacklistAppealLapsed(appealId, 2);
            return;
        }
        revert AppealNotOpen(appealId);
    }

    function getAppeal(uint256 appealId) external view returns (BlacklistAppeal memory) {
        return _appeals[appealId];
    }

    function appealCount() external view returns (uint256) {
        return _appeals.length;
    }

    /// @notice Full ADR 031 rejection-window record for `filer`. The public
    ///         `filerRejections` auto-getter omits the fixed-size `rejections`
    ///         array, so this explicit accessor exposes it for off-chain audit.
    function getFilerRejectionWindow(address filer) external view returns (RejectionWindow memory) {
        return filerRejections[filer];
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function setAppealBond(uint256 newBond) external onlyRole(GOVERNANCE_ROLE) {
        _enforceAppealBondBounds(newBond);
        uint256 old = appealBond;
        appealBond = newBond;
        emit AppealBondUpdated(old, newBond);
    }

    /// @notice Governance-tunable ADR 031 rejection cooldown window (rolling
    ///         lookback == lockout duration). Bounded for the same reason as
    ///         `appealBond` — keeps the abuse throttle inside sane limits.
    function setRejectionCooldownWindow(uint64 newWindow) external onlyRole(GOVERNANCE_ROLE) {
        _enforceRejectionCooldownWindowBounds(newWindow);
        uint64 old = rejectionCooldownWindow;
        rejectionCooldownWindow = newWindow;
        emit RejectionCooldownWindowUpdated(old, newWindow);
    }

    /// @notice Convenience for the post-deployment role grant
    ///         "registerRegionalBody" (ADR 016 § Post-Deployment, step 8).
    function registerRegionalBody(address body) external onlyRole(GOVERNANCE_ROLE) {
        _grantRole(REGIONAL_BODY_ROLE, body);
    }

    // -----------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------

    function _addHash(bytes32 region, bytes32 hash) internal {
        if (hash == bytes32(0)) revert ZeroHash();
        // A re-add while an appeal is live would clear `suspended` and refresh
        // `addedAt`, orphaning the in-flight appeal and resetting the slash-
        // eligibility boundary (H-2). Force governance to terminate the appeal
        // first (reject / reverse / ratify) before re-adding.
        if (hasActiveAppeal[region][hash]) revert HashHasActiveAppeal(region, hash);
        HashEntry storage e = _hashEntries[region][hash];
        // Re-adding refreshes `addedAt` (resets the filing window).
        e.addedAt = uint64(block.timestamp);
        e.suspended = false;
        emit HashBlacklisted(region, hash);
    }

    function _removeHashRegional(bytes32 region, bytes32 hash) internal {
        HashEntry storage e = _hashEntries[region][hash];
        if (e.addedAt == 0) revert EntryNotBlacklisted(region, hash);
        delete _hashEntries[region][hash];
        emit HashRemoved(region, hash);
    }

    /// @dev Reads storage directly to avoid the `storage → memory` flagged
    ///      by aderyn H-2. An entry is live iff it was added (`addedAt != 0`)
    ///      and not currently fast-track-suspended.
    function _isLive(bytes32 region, bytes32 hash) internal view returns (bool) {
        HashEntry storage e = _hashEntries[region][hash];
        return e.addedAt != 0 && !e.suspended;
    }

    function _enforceAppealBondBounds(uint256 value) internal pure {
        if (value < APPEAL_BOND_FLOOR || value > APPEAL_BOND_CEILING) {
            revert ParamOutOfBounds({ value: value, floor: APPEAL_BOND_FLOOR, ceiling: APPEAL_BOND_CEILING });
        }
    }

    function _enforceRejectionCooldownWindowBounds(uint64 value) internal pure {
        if (value < REJECTION_COOLDOWN_WINDOW_FLOOR || value > REJECTION_COOLDOWN_WINDOW_CEILING) {
            revert ParamOutOfBounds(value, REJECTION_COOLDOWN_WINDOW_FLOOR, REJECTION_COOLDOWN_WINDOW_CEILING);
        }
    }

    /// @dev ADR 031 § APPEAL_FILER_REJECTION_COOLDOWN. Appends the current time
    ///      to the filer's three-slot ring (dropping the oldest) and, if all
    ///      three rejections fall inside `rejectionCooldownWindow`, locks the
    ///      filer out for a fresh full window. The ring is cleared on lockout so
    ///      the filer starts from zero once the cooldown elapses. Rejections that
    ///      age out of the rolling window simply never accumulate to three.
    function _recordRejection(address filer) internal {
        RejectionWindow storage w = filerRejections[filer];
        uint64 nowTs = uint64(block.timestamp);
        w.rejections[0] = w.rejections[1];
        w.rejections[1] = w.rejections[2];
        w.rejections[2] = nowTs;
        uint64 oldest = w.rejections[0];
        uint64 window = rejectionCooldownWindow;
        // `oldest != 0` means the ring is full (three rejections recorded); if
        // the oldest of those three is still inside the window, all three are.
        if (oldest != 0 && nowTs - oldest <= window) {
            w.cooldownUntilAt = nowTs + window;
            w.rejections[0] = 0;
            w.rejections[1] = 0;
            w.rejections[2] = 0;
        }
    }
}
