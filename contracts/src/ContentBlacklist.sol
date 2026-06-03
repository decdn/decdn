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
///           - Synthetic-standing clawback (`StandingPath.TokenHolder`
///             balance check) is deferred. Standing path is recorded but
///             not enforced beyond an enum-range check at filing time.
///           - Per-region concurrent-appeal cap (`BODY_CONCURRENT_APPEAL_CAP`)
///             is enforced as a single hard ceiling per region, not by
///             requesting body identity.
///           - Perjury denylist + 365-day rejection cooldown are deferred.
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
    uint256 internal constant BODY_CONCURRENT_APPEAL_CAP = 3;
    // Deviation from ADR 011 § Bond and frequency caps (spec is
    // [100e18, 10_000e18]). Bounds halved to [50e18, 5000e18] for the
    // testnet phase so appeal-bond economics scale with the smaller TGE
    // float; widen on production redeploy.
    uint256 internal constant APPEAL_BOND_FLOOR = 50e18;
    uint256 internal constant APPEAL_BOND_CEILING = 5000e18;

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

    /// @notice `true` when there is an Open or FastTracked appeal for the
    ///         given `(region, hash)`. Prevents concurrent appeals on the
    ///         same entry — without this guard, multiple fast-tracked
    ///         appeals could share a single `suspended` flag, and the first
    ///         to terminate (reject/lapse/reverse) would clear `suspended`
    ///         out from under any remaining live appeals (state-override
    ///         bug across concurrent appeals).
    mapping(bytes32 region => mapping(bytes32 hash => bool)) public hasActiveAppeal;

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
    event BlacklistAppealRatified(uint256 indexed appealId);
    event BlacklistAppealReversed(uint256 indexed appealId, uint256 bondBurned);
    event BlacklistAppealLapsed(uint256 indexed appealId, uint8 reason);

    event AppealBondUpdated(uint256 oldValue, uint256 newValue);

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
    error RegionalCapHit(bytes32 region, uint256 cap);
    error AppealAlreadyActive(bytes32 region, bytes32 hash);
    error HashHasActiveAppeal(bytes32 region, bytes32 hash);
    error FrequencyCapHit(uint64 nextAvailableAt);
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

    function removeOperator(address operator) external onlyRole(GOVERNANCE_ROLE) {
        if (isOperatorBlacklisted[operator]) {
            isOperatorBlacklisted[operator] = false;
            emit OperatorBlacklistCleared(operator);
        }
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

        // StandingPath is recorded but only enum-range validated; the
        // synthetic-standing clawback (admissibility condition (c) per
        // ADR 031 § 216 — `status == Open && standingPath == TokenHolder &&
        // ...`) is deferred per the contract header note. The field is
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
        // The per-region concurrent cap counts only fast-tracked appeals — the
        // ones actually holding interim relief (a suspended entry) — so it
        // bounds simultaneous suspensions per region without letting un-acted
        // Open filings consume the budget (M-1).
        if (regionActiveReliefCount[a.region] >= BODY_CONCURRENT_APPEAL_CAP) {
            revert RegionalCapHit(a.region, BODY_CONCURRENT_APPEAL_CAP);
        }
        a.fastTrackedAt = uint64(block.timestamp);
        a.status = AppealStatus.FastTracked;
        regionActiveReliefCount[a.region] += 1;
        _hashEntries[a.region][a.hash].suspended = true;
        emit BlacklistAppealFastTracked(appealId);
    }

    function rejectBlacklistAppeal(uint256 appealId) external nonReentrant onlyRole(EMERGENCY_MULTISIG_ROLE) {
        BlacklistAppeal storage a = _appeals[appealId];
        if (a.status != AppealStatus.Open && a.status != AppealStatus.FastTracked) revert AppealNotOpen(appealId);
        if (a.status == AppealStatus.FastTracked) {
            _hashEntries[a.region][a.hash].suspended = false;
            // Only fast-tracked appeals charge the relief cap (M-1).
            if (regionActiveReliefCount[a.region] != 0) regionActiveReliefCount[a.region] -= 1;
        }
        uint256 bondBurned = a.bond;
        a.bond = 0;
        a.status = AppealStatus.Rejected;
        hasActiveAppeal[a.region][a.hash] = false;
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

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function setAppealBond(uint256 newBond) external onlyRole(GOVERNANCE_ROLE) {
        _enforceAppealBondBounds(newBond);
        uint256 old = appealBond;
        appealBond = newBond;
        emit AppealBondUpdated(old, newBond);
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
}
