// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { EnumerableSet } from "@openzeppelin/contracts/utils/structs/EnumerableSet.sol";

import { ICapacityBondEjector } from "./interfaces/ICapacityBondEjector.sol";
import { ICapacityBondRegionView } from "./interfaces/ICapacityBondRegionView.sol";
import { IEnumerableSigners } from "./interfaces/IEnumerableSigners.sol";
import { RegionScopeLib } from "./RegionScopeLib.sol";

/// @title ContentBlacklist
/// @notice Global + regional hash blacklist, operator-level blacklist, and
///         origin blacklist (ADR 011 § Content Takedown). Enforcement only:
///         entries are added by governance, by a registered regional body, or
///         by the emergency multisig, and are removed by the global-override
///         slow path (`removeHashGlobal` / `removeHashRegional`). An operator
///         slashed under an entry later found wrongful is made whole through
///         `SlashAppeal` (ADR 028), a separate contract.
contract ContentBlacklist is AccessControl, ReentrancyGuard {
    using EnumerableSet for EnumerableSet.Bytes32Set;
    using EnumerableSet for EnumerableSet.AddressSet;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant EMERGENCY_MULTISIG_ROLE = keccak256("EMERGENCY_MULTISIG_ROLE");
    bytes32 public constant REGIONAL_BODY_ROLE = keccak256("REGIONAL_BODY_ROLE");

    // -----------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------

    // ADR 011 § Compliance Window — an entry does NOT become slashable at
    // `addedAt`; it becomes slashable at `effectiveAt = addedAt + window`. The
    // grace exists because nodes learn of an entry by re-enumerating the
    // deny-set on an interval (10 minutes by default, ADR 011 § Node Behavior):
    // without it, a node serving a request microseconds before the add lands is
    // slashable for a delivery it could not have known was prohibited. Both
    // windows are governance-tunable inside the hardcoded [1 hour, 7 days]
    // bounds the ADR fixes.
    uint64 internal constant COMPLIANCE_WINDOW_DEFAULT = 24 hours;
    uint64 internal constant EMERGENCY_COMPLIANCE_WINDOW_DEFAULT = 2 hours;
    uint64 internal constant COMPLIANCE_WINDOW_FLOOR = 1 hours;
    uint64 internal constant COMPLIANCE_WINDOW_CEILING = 7 days;

    // ADR 011 § Contract: ContentBlacklist — an emergency entry is a 3-of-5
    // multisig act taken without a governance vote, so it self-limits: it stops
    // being enforceable at `addedAt + expiry` unless governance ratifies it with
    // `addHashGlobal`. Severe categories get the longer term because re-exposing
    // that content through governance latency is the worse failure.
    uint64 internal constant EMERGENCY_EXPIRY_GENERAL = 14 days;
    uint64 internal constant EMERGENCY_EXPIRY_SEVERE = 90 days;

    /// @dev ADR 011 § Regional Governance Bodies — a multisig suspension of a
    ///      regional body "must be ratified or reversed by governance vote
    ///      within 14 days (same ratification window as emergency blacklist
    ///      entries)". Silence past the window therefore lapses the suspension
    ///      rather than sustaining it, matching how an unratified emergency
    ///      entry expires: neither is a standing act of governance.
    uint64 internal constant BODY_SUSPENSION_RATIFICATION_WINDOW = 14 days;

    /// @dev Upper bound on the signer set read during the `registerRegionalBody`
    ///      disjointness probe. A candidate body is an untrusted contract at
    ///      probe time, so an unbounded `getOwners()` return is a gas-griefing
    ///      vector; a body with more signers than this is treated as
    ///      not-enumerable and falls back to the off-chain attestation path.
    uint256 internal constant MAX_PROBED_SIGNERS = 32;

    bytes32 internal constant GLOBAL_REGION = bytes32("GLOBAL");

    // -----------------------------------------------------------------
    // Enums
    // -----------------------------------------------------------------

    /// @notice Severity of an emergency entry (ADR 011 § Contract:
    ///         ContentBlacklist). Determines only the auto-expiry term — it
    ///         carries no enforcement weight of its own, so a node need not
    ///         understand the category to comply. Order is ABI-frozen: the
    ///         external surface takes `uint8` and the value is persisted on the
    ///         entry, so reordering would silently reclassify live entries.
    enum Category {
        GENERAL,
        CSAM,
        TERRORIST
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

    /// @notice Grace between a standard or regional add and the moment the entry
    ///         becomes slashable (ADR 011 § Compliance Window). Stamped onto
    ///         `HashEntry.effectiveAt` at add time; changing it never moves the
    ///         boundary for entries already added. Bounded to
    ///         [COMPLIANCE_WINDOW_FLOOR, COMPLIANCE_WINDOW_CEILING].
    uint64 public complianceWindow;

    /// @notice The same grace for emergency-multisig adds — deliberately much
    ///         tighter, because an emergency entry is the unlawful-content path
    ///         and the network cannot wait a day. Same bounds as
    ///         `complianceWindow`, so governance cannot set an emergency window
    ///         under an hour and slash nodes that had no poll cycle to see it.
    uint64 public emergencyComplianceWindow;

    // -----------------------------------------------------------------
    // Storage — blacklist entries
    // -----------------------------------------------------------------

    /// @notice Per-(region, hash) entry. `region = GLOBAL_REGION` is the
    ///         global scope. `addedAt == 0` means "not blacklisted".
    /// @dev    Every field is statically sized: `getHashEntry` returns this
    ///         struct and `IContentBlacklistHashView` / `SlashJudge` decode it
    ///         as the ABI-identical flattened tuple on the hot slash-eligibility
    ///         path. Keep it that way — the audit-trail `reason` string (ADR 011
    ///         § Reason field) is stored out-of-band in `hashReason` precisely so
    ///         it cannot perturb that tuple's shape, and any new field must be
    ///         mirrored into `IContentBlacklistHashView` in the same order.
    ///         Packs into one slot (8 + 8 + 1 + 1 = 18 bytes).
    /// @dev    `effectiveAt` is STAMPED at add time from the window then in
    ///         force, never computed live from the current `complianceWindow`:
    ///         a live computation would let a governance window change
    ///         retroactively move the slash boundary under deliveries already
    ///         served.
    struct HashEntry {
        uint64 addedAt;
        uint64 effectiveAt;
        bool emergency;
        uint8 category;
    }

    mapping(bytes32 region => mapping(bytes32 hash => HashEntry)) internal _hashEntries;

    /// @notice Free-form audit-trail reason per (region, hash) — legal notice
    ///         identifiers (DMCA case numbers, DSA notice IDs) or short category
    ///         labels (ADR 011 § Reason field). Persisted on-chain so takedowns
    ///         carry a DMCA/DSA-defensible provenance record. Kept out of
    ///         `HashEntry` to preserve that struct's static ABI shape (see its
    ///         doc). Public auto-getter `hashReason(region, hash)`; cleared on
    ///         `removeHash*`. Set to `""` when an entry carries no reason.
    mapping(bytes32 region => mapping(bytes32 hash => string)) public hashReason;

    /// @notice Operator-level blacklist (ADR 011 § Decision — operator-level
    ///         blacklist evicts the operator from `CapacityBond` via
    ///         `ejectNode`).
    mapping(address operator => bool) public isOperatorBlacklisted;

    /// @notice Origin-level blacklist (ADR 011 § Hash Evasion and Origin
    ///         Blacklisting). Internal because an emergency-added origin expires
    ///         and a raw mapping getter cannot express that — read through
    ///         `isOriginBlacklisted(address)`, which keeps the same selector the
    ///         auto-getter had, so off-chain consumers binding it are unaffected.
    mapping(address origin => bool) internal _isOriginBlacklisted;

    /// @notice Auto-expiry metadata for origins added via `emergencyAddOrigin`.
    ///         `addedAt == 0` marks a governance origin entry, which never
    ///         expires. Kept out of `_isOriginBlacklisted` so the hot
    ///         `OriginAssignment` read stays a single bool load in the common
    ///         (non-emergency) case.
    struct EmergencyOrigin {
        uint64 addedAt;
        uint8 category;
    }

    mapping(address origin => EmergencyOrigin) internal _emergencyOrigins;

    /// @notice Region-scoped regional-body registry (ADR 011 § Regional
    ///         Governance Bodies). `REGIONAL_BODY_ROLE` alone is not sufficient
    ///         authority to write an entry: the caller must also be THE body
    ///         registered for the region it names, and not suspended.
    /// @dev    `suspendedAt == 0` means not suspended. An unratified suspension
    ///         lapses on its own once `BODY_SUSPENSION_RATIFICATION_WINDOW`
    ///         elapses (see `_bodySuspended`); a ratified one stands until
    ///         `unsuspendRegionalBody`.
    struct RegionalBody {
        address body;
        uint64 suspendedAt;
        bool suspensionRatified;
    }

    mapping(bytes32 region => RegionalBody) internal _regionalBodies;

    /// @notice Reverse index: the single region a body is registered for, or
    ///         `bytes32(0)`. Enforces one-region-per-body so a body cannot be
    ///         suspended in one jurisdiction and keep writing in another.
    ///         Public auto-getter `regionOfBody(address)`.
    mapping(address body => bytes32 region) public regionOfBody;

    /// @notice Membership index for `_hashEntries[region]`, so the entry set of a
    ///         region can be READ rather than reconstructed from the event log.
    /// @dev    Maintained in the two internal choke points `_addHash` and
    ///         `_removeHashRegional`, which every add/remove funnels through, so
    ///         no call site can add an entry without indexing it. Holds RAW
    ///         membership — see `blacklistedHashes` for why it is not
    ///         expiry-filtered.
    mapping(bytes32 region => EnumerableSet.Bytes32Set) internal _regionHashes;

    /// @notice Membership index for the union of the two address-level deny
    ///         lists: `_isOriginBlacklisted` ∪ `isOperatorBlacklisted`.
    /// @dev    A union rather than two sets because that is the only question
    ///         any consumer asks — a node's delivery gate refuses on either, and
    ///         its routing filter skips an origin blacklisted through either.
    ///         Reconciled through `_syncAddr`, which
    ///         re-derives membership from both sources so clearing one list while
    ///         the other still holds cannot drop the address from the index.
    EnumerableSet.AddressSet internal _blacklistedAddrs;

    // -----------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------

    /// @notice A hash entered the enforced deny-set. A node treats this as a
    ///         low-latency signal to re-read the in-scope predicate for the
    ///         hashes it holds; the authoritative deny-set is the full
    ///         enumeration (ADR 011 § Node Behavior), so a missed event is
    ///         recovered by the next re-enumeration rather than by any counter.
    event HashBlacklisted(bytes32 indexed region, bytes32 indexed hash, string reason);
    /// @notice A hash left the enforced deny-set. Signal semantics as in
    ///         `HashBlacklisted`.
    event HashRemoved(bytes32 indexed region, bytes32 indexed hash);
    event OperatorBlacklisted(address indexed operator);
    event OperatorBlacklistCleared(address indexed operator);
    event OriginBlacklistUpdated(address indexed origin, bool blacklisted);

    event ComplianceWindowUpdated(uint64 oldValue, uint64 newValue);
    /// @notice Emitted alongside `OriginBlacklistUpdated` when the entry came in
    ///         via the emergency path, carrying the audit trail
    ///         (`OriginBlacklistUpdated` has no room for it and its shape is
    ///         consumed by nodes). `category` determines the auto-expiry term.
    event EmergencyOriginAdded(address indexed origin, uint8 category, string reason);
    event EmergencyComplianceWindowUpdated(uint64 oldValue, uint64 newValue);

    /// @notice A regional body was bound to `region`. `signersVerified` is false
    ///         when the candidate did not expose an enumerable signer view, in
    ///         which case ADR 011 § Signer non-overlap puts the disjointness
    ///         obligation on the governance proposal off-chain — the event is
    ///         the on-chain record of which of the two regimes applied.
    event RegionalBodyRegistered(bytes32 indexed region, address indexed body, bool signersVerified);
    event RegionalBodyDeregistered(bytes32 indexed region, address indexed body);
    event RegionalBodySuspended(bytes32 indexed region, address indexed body);
    event RegionalBodySuspensionRatified(bytes32 indexed region, address indexed body);
    event RegionalBodyUnsuspended(bytes32 indexed region, address indexed body);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error ZeroHash();
    error MissingRegion();
    error EntryNotBlacklisted(bytes32 region, bytes32 hash);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);
    /// @notice The `uint8` category argument is outside `Category`. Raised
    ///         explicitly rather than letting the enum cast panic (0x21), so the
    ///         emergency path fails with a named, decodable reason.
    error InvalidCategory(uint8 category);
    /// @notice `expireEmergencyEntry` / `expireEmergencyOrigin` was called on an
    ///         entry that is not an emergency entry, or has not reached its
    ///         category deadline yet.
    error NotExpired();
    /// @notice The caller holds `REGIONAL_BODY_ROLE` but is not the body
    ///         registered for the region it named (ADR 011 § Regional Governance
    ///         Bodies — a body's authority is scoped to its own jurisdiction).
    error NotRegionalBodyFor(bytes32 region, address caller);
    error RegionalBodyAlreadyRegistered(bytes32 region, address body);
    /// @notice The candidate body already serves another region. One region per
    ///         body, so suspension in one jurisdiction cannot be routed around.
    error BodyAlreadyServingRegion(address body, bytes32 region);
    error RegionalBodyNotRegistered(bytes32 region);
    error RegionalBodySuspensionNotActive(bytes32 region);
    error RegionalBodyAlreadySuspended(bytes32 region);
    /// @notice `registerRegionalBody` found a signer common to the candidate body
    ///         and the emergency multisig. The multisig can suspend this body,
    ///         so an overlapping signer would grade their own homework
    ///         (ADR 011 § Signer non-overlap).
    error SignerOverlap(address signer);
    /// @notice The address supplied to `registerRegionalBody` as the comparison
    ///         side does not hold `EMERGENCY_MULTISIG_ROLE`, so a probe against
    ///         it would prove nothing.
    error NotEmergencyMultisig(address account);
    /// @notice `emergencyAdd` was aimed at a hash that governance has already
    ///         blacklisted permanently. Re-adding it through the emergency path
    ///         would arm auto-expiry on a standing governance decision, handing
    ///         the multisig a delayed removal it has no authority to perform.
    error EmergencyCannotOverrideGovernance(bytes32 hash);
    /// @notice The `emergencyAddOrigin` counterpart of
    ///         `EmergencyCannotOverrideGovernance`.
    error EmergencyCannotOverrideGovernanceOrigin(address origin);
    /// @notice The suspension's 14-day ratification window has elapsed, so
    ///         governance can no longer ratify it — the suspension has already
    ///         lapsed and the body is writing again. Re-suspend to restart.
    error SuspensionWindowClosed(uint64 closedAt);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    constructor(ICapacityBondEjector capacityBond_, address admin) {
        if (address(capacityBond_) == address(0) || admin == address(0)) {
            revert ZeroAddress();
        }
        capacityBond = capacityBond_;
        capacityBondRegion = ICapacityBondRegionView(address(capacityBond_));
        complianceWindow = COMPLIANCE_WINDOW_DEFAULT;
        emergencyComplianceWindow = EMERGENCY_COMPLIANCE_WINDOW_DEFAULT;
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    // -----------------------------------------------------------------
    // Core blacklist surface (ADR 011)
    // -----------------------------------------------------------------

    /// @notice Add `hash` to the global blacklist. `GOVERNANCE_ROLE` only.
    /// @param  reason Free-form audit-trail note (DMCA/DSA notice id or label);
    ///         persisted on the entry per ADR 011 § Reason field.
    /// @dev Also the ratification path for an emergency entry: re-adding under
    ///      governance rewrites `emergency` to false, so the auto-expiry in
    ///      `_isLive` stops applying and the entry becomes permanent.
    function addHashGlobal(bytes32 hash, string calldata reason) external onlyRole(GOVERNANCE_ROLE) {
        _addHash(GLOBAL_REGION, hash, reason, false, Category.GENERAL);
    }

    /// @notice Add `hash` to the regional blacklist for `region`. Carries
    ///         `REGIONAL_BODY_ROLE` AND requires the caller to be the body
    ///         registered for `region` per ADR 011 § Regional Governance Bodies:
    ///         a body's authority is its own jurisdiction, so the role alone
    ///         cannot reach another region's entries.
    /// @param  reason Free-form audit-trail note (DMCA/DSA notice id or label);
    ///         persisted on the entry per ADR 011 § Reason field.
    function addHashRegional(bytes32 region, bytes32 hash, string calldata reason)
        external
        onlyRole(REGIONAL_BODY_ROLE)
    {
        if (region == bytes32(0) || region == GLOBAL_REGION) revert MissingRegion();
        _requireActiveBodyFor(region);
        _addHash(region, hash, reason, false, Category.GENERAL);
    }

    function removeHashGlobal(bytes32 hash) external onlyRole(GOVERNANCE_ROLE) {
        _removeHashRegional(GLOBAL_REGION, hash);
    }

    /// @dev `REGIONAL_BODY_ROLE` must NOT be able to remove `GLOBAL_REGION`
    ///      entries — that would let any regional body bypass governance and
    ///      delete a global blacklist. Mirrors the guard on `addHashRegional`,
    ///      including the same-body scoping.
    function removeHashRegional(bytes32 region, bytes32 hash) external onlyRole(REGIONAL_BODY_ROLE) {
        if (region == bytes32(0) || region == GLOBAL_REGION) revert MissingRegion();
        _requireActiveBodyFor(region);
        _removeHashRegional(region, hash);
    }

    // -----------------------------------------------------------------
    // Emergency multisig path (ADR 011 § Contract: ContentBlacklist)
    // -----------------------------------------------------------------

    /// @notice Blacklist `hash` globally with no governance vote and no timelock.
    ///         3-of-5 `EMERGENCY_MULTISIG_ROLE`. This is the CSAM / statutory
    ///         one-hour-order path and is retained in perpetuity — under the ADR
    ///         009 capability-split sunset only the protocol-wide pause expires
    ///         at 12 months, not this.
    /// @dev    Emergency adds are global by construction (ADR 011 § Scope), so
    ///         they take no region. They carry the tighter
    ///         `emergencyComplianceWindow` and self-expire per `category` unless
    ///         governance ratifies with `addHashGlobal`.
    function emergencyAdd(bytes32 hash, uint8 category, string calldata reason)
        external
        onlyRole(EMERGENCY_MULTISIG_ROLE)
    {
        // The emergency path may only ADD enforcement, never weaken it. Without
        // this guard, re-adding a governance entry would flip it to
        // `emergency = true` and arm auto-expiry — giving the multisig a delayed
        // `removeHashGlobal`, which is `GOVERNANCE_ROLE`-only and reachable by no
        // other multisig route. Re-adding over the multisig's OWN entry stays
        // allowed: escalating the category, or re-arming one that lapsed,
        // sustains a takedown rather than undoing one.
        HashEntry storage existing = _hashEntries[GLOBAL_REGION][hash];
        if (existing.addedAt != 0 && !existing.emergency) {
            revert EmergencyCannotOverrideGovernance(hash);
        }
        _addHash(GLOBAL_REGION, hash, reason, true, _toCategory(category));
    }

    /// @notice Blacklist an origin operator address with no governance vote.
    ///         Same role and expiry semantics as `emergencyAdd`.
    /// @dev    Deliberately writes only the origin blacklist and does NOT eject
    ///         from `CapacityBond`. Ejection is permanent and forces the
    ///         operator's bond into unbonding (ADR 011 § Hash Evasion); pairing
    ///         an irreversible consequence with a self-expiring entry would be
    ///         incoherent. Permanent removal stays `addOperator`, governance-only.
    function emergencyAddOrigin(address operator, uint8 category, string calldata reason)
        external
        onlyRole(EMERGENCY_MULTISIG_ROLE)
    {
        if (operator == address(0)) revert ZeroAddress();
        // Same one-way rule as `emergencyAdd`. A governance origin entry is
        // marked by `_isOriginBlacklisted && _emergencyOrigins.addedAt == 0`
        // (`setOriginBlacklist` clears the expiry record precisely so it reads
        // as permanent); re-stamping it here would convert it to an expiring one.
        if (_isOriginBlacklisted[operator] && _emergencyOrigins[operator].addedAt == 0) {
            revert EmergencyCannotOverrideGovernanceOrigin(operator);
        }
        Category cat = _toCategory(category);
        _isOriginBlacklisted[operator] = true;
        _emergencyOrigins[operator] = EmergencyOrigin({ addedAt: uint64(block.timestamp), category: uint8(cat) });
        _syncAddr(operator);
        emit OriginBlacklistUpdated(operator, true);
        emit EmergencyOriginAdded(operator, uint8(cat), reason);
    }

    /// @notice Permissionlessly retire an emergency hash entry that has passed
    ///         its category deadline.
    /// @dev    `_isLive` already reports an expired entry as unenforceable, so
    ///         this changes no view answer. It exists to materialize the expiry:
    ///         it removes the entry from the enumerable `_regionHashes` index and
    ///         emits `HashRemoved`, so the raw `blacklistedHashes` membership a
    ///         node enumerates (ADR 011 § Node Behavior) no longer carries the
    ///         lapsed entry. Mirrors the permissionless-cleanup model of
    ///         `OriginAssignment.pruneInactiveOrigin`.
    function expireEmergencyEntry(bytes32 region, bytes32 hash) external {
        HashEntry storage e = _hashEntries[region][hash];
        if (e.addedAt == 0) revert EntryNotBlacklisted(region, hash);
        if (!_emergencyExpired(e.emergency, e.addedAt, e.category)) revert NotExpired();
        _removeHashRegional(region, hash);
    }

    /// @notice Permissionless counterpart of `expireEmergencyEntry` for origins.
    /// @dev    Nodes track the origin deny-set by enumerating
    ///         `blacklistedAddresses` and following the event tail (ADR 011
    ///         § Node Behavior). `OriginBlacklistUpdated(origin, false)` emitted
    ///         here is the low-latency signal an expiry happened; the entry also
    ///         leaves the enumerated membership on removal.
    function expireEmergencyOrigin(address origin) external {
        EmergencyOrigin storage eo = _emergencyOrigins[origin];
        if (!_emergencyExpired(eo.addedAt != 0, eo.addedAt, eo.category)) revert NotExpired();
        delete _emergencyOrigins[origin];
        _isOriginBlacklisted[origin] = false;
        _syncAddr(origin);
        emit OriginBlacklistUpdated(origin, false);
    }

    /// @dev `nonReentrant` because this is the one path that makes an external
    ///      state-changing call
    ///      (`capacityBond.ejectNode`) after a state write (M-4). `ejectNode` is
    ///      a trusted contract, but the guard hardens against a future hook.
    function addOperator(address operator) external nonReentrant onlyRole(GOVERNANCE_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        if (!isOperatorBlacklisted[operator]) {
            isOperatorBlacklisted[operator] = true;
            _syncAddr(operator);
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
            _syncAddr(operator);
            emit OperatorBlacklistCleared(operator);
        }
        capacityBond.unEjectNode(operator);
    }

    /// @dev Also the ratification path for an emergency origin entry: clearing
    ///      the `_emergencyOrigins` record is what makes a governance-set origin
    ///      permanent. Cleared on both polarities — setting `false` must not
    ///      leave a stale expiry record behind that a later `true` would inherit.
    function setOriginBlacklist(address origin, bool blacklisted) external onlyRole(GOVERNANCE_ROLE) {
        if (origin == address(0)) revert ZeroAddress();
        _isOriginBlacklisted[origin] = blacklisted;
        delete _emergencyOrigins[origin];
        _syncAddr(origin);
        emit OriginBlacklistUpdated(origin, blacklisted);
    }

    /// @notice True while `origin` is blacklisted at the origin level. Same
    ///         selector as the former public-mapping auto-getter, so off-chain
    ///         consumers binding it are unaffected — but this form also honours
    ///         emergency auto-expiry, which a raw mapping read could not.
    function isOriginBlacklisted(address origin) public view returns (bool) {
        if (!_isOriginBlacklisted[origin]) return false;
        EmergencyOrigin storage eo = _emergencyOrigins[origin];
        return !_emergencyExpired(eo.addedAt != 0, eo.addedAt, eo.category);
    }

    /// @notice Auto-expiry record for an emergency-added origin.
    ///         `addedAt == 0` means the entry is a permanent governance one.
    function getEmergencyOrigin(address origin) external view returns (EmergencyOrigin memory) {
        return _emergencyOrigins[origin];
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
    function isHashBlacklistedForOperator(bytes32 hash, address operator) external view returns (bool) {
        if (_isLive(GLOBAL_REGION, hash)) return true;
        (bytes32 cur, bytes32 prev, bool prevApplies) = _scopeRegions(operator);
        if (cur != bytes32(0) && _isLive(cur, hash)) return true;
        if (prevApplies && _isLive(prev, hash)) return true;
        return false;
    }

    /// @notice Every region key whose entries are in scope for `operator` right
    ///         now: `GLOBAL_REGION` (always, at index 0), the current region, and
    ///         — while the ADR 030 ripening window is open — the previous one.
    ///         Zero keys are omitted, so the result has 1 to 3 elements.
    /// @dev    The enumeration counterpart of `isHashBlacklistedForOperator`. A
    ///         consumer pages `blacklistedHashes` for each key returned here and
    ///         enforces the union, instead of replaying `HashBlacklisted` from
    ///         the deploy block and retaining out-of-scope entries against a
    ///         future region change. Both this and the point query resolve their
    ///         keys through `_scopeRegions`, so the region packing and the
    ///         ripening arithmetic cannot drift between the two paths.
    function getScopeRegions(address operator) external view returns (bytes32[] memory regions) {
        (bytes32 cur, bytes32 prev, bool prevApplies) = _scopeRegions(operator);
        uint256 n = 1;
        if (cur != bytes32(0)) ++n;
        if (prevApplies) ++n;

        regions = new bytes32[](n);
        regions[0] = GLOBAL_REGION;
        uint256 i = 1;
        if (cur != bytes32(0)) {
            regions[i] = cur;
            ++i;
        }
        if (prevApplies) regions[i] = prev;
    }

    function getHashEntry(bytes32 region, bytes32 hash) external view returns (HashEntry memory) {
        return _hashEntries[region][hash];
    }

    /// @notice How many entries `region` holds. Companion to `blacklistedHashes`.
    /// @dev    RAW membership, matching that view — see its `@dev` for why.
    function blacklistedHashCount(bytes32 region) external view returns (uint256) {
        return _regionHashes[region].length();
    }

    /// @notice A page of `region`'s entry set, starting at `offset` and at most
    ///         `limit` long. A page shorter than `limit` means the end of the set.
    /// @dev    Returns RAW membership — every hash with a stored entry —
    ///         deliberately NOT filtered through `_isLive`. Filtering would put
    ///         holes in a paginated view, so a short page would stop meaning "end
    ///         of set"; and the only entries it would drop are emergency ones past
    ///         their category deadline. Omitting those is the FAIL-OPEN direction
    ///         for a compliance gate, whereas returning them merely over-enforces
    ///         — which is already what a consumer does today, since it learns of
    ///         an expiry only from the `HashRemoved` that `expireEmergencyEntry`
    ///         emits. Callers needing liveness read `getHashEntry` or
    ///         `isHashBlacklistedForOperator` per hash.
    /// @dev    Order is NOT stable across mutations: removal is swap-and-pop, so a
    ///         removal between two page reads can move an unread element into an
    ///         already-read slot and skip it. Page every offset at ONE pinned
    ///         block height and re-check `blacklistedHashCount` at that same
    ///         height. The same swap-and-pop pagination hazard applies to any
    ///         other enumerable set this contract or its peers expose.
    function blacklistedHashes(bytes32 region, uint256 offset, uint256 limit)
        external
        view
        returns (bytes32[] memory page)
    {
        EnumerableSet.Bytes32Set storage set = _regionHashes[region];
        uint256 len = set.length();
        if (offset >= len) return new bytes32[](0);
        // `len - offset` rather than `offset + limit`, which can overflow.
        uint256 n = len - offset;
        if (n > limit) n = limit;
        page = new bytes32[](n);
        for (uint256 i = 0; i < n; ++i) {
            page[i] = set.at(offset + i);
        }
    }

    /// @notice How many addresses are denied at the origin or operator level.
    function blacklistedAddressCount() external view returns (uint256) {
        return _blacklistedAddrs.length();
    }

    /// @notice A page of the union of the origin and operator deny lists — the
    ///         same disjunction `OriginAssignment` evaluates per address.
    /// @dev    RAW membership, with the same pagination caveats as
    ///         `blacklistedHashes`: an emergency origin whose term lapsed but
    ///         which nobody has run `expireEmergencyOrigin` against is still
    ///         returned, and the order is unstable across mutations. The live
    ///         per-address predicates remain `isOriginBlacklisted` and
    ///         `isOperatorBlacklisted`.
    /// @dev    This is the only readable source for the origin deny-set. Like
    ///         the hash set, it carries no version counter: a consumer reconciles
    ///         a dropped `OriginBlacklistUpdated` event against this full
    ///         enumeration (ADR 011 § Node Behavior).
    function blacklistedAddresses(uint256 offset, uint256 limit) external view returns (address[] memory page) {
        uint256 len = _blacklistedAddrs.length();
        if (offset >= len) return new address[](0);
        uint256 n = len - offset;
        if (n > limit) n = limit;
        page = new address[](n);
        for (uint256 i = 0; i < n; ++i) {
            page[i] = _blacklistedAddrs.at(offset + i);
        }
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function setComplianceWindow(uint64 newWindow) external onlyRole(GOVERNANCE_ROLE) {
        _enforceComplianceWindowBounds(newWindow);
        uint64 old = complianceWindow;
        complianceWindow = newWindow;
        emit ComplianceWindowUpdated(old, newWindow);
    }

    function setEmergencyComplianceWindow(uint64 newWindow) external onlyRole(GOVERNANCE_ROLE) {
        _enforceComplianceWindowBounds(newWindow);
        uint64 old = emergencyComplianceWindow;
        emergencyComplianceWindow = newWindow;
        emit EmergencyComplianceWindowUpdated(old, newWindow);
    }

    // -----------------------------------------------------------------
    // Regional-body lifecycle (ADR 011 § Regional Governance Bodies)
    // -----------------------------------------------------------------

    /// @notice Bind `body` to `region` and grant it `REGIONAL_BODY_ROLE`
    ///         (ADR 016 § Post-Deployment, step 6).
    /// @dev    Attempts the ADR 011 § Signer non-overlap check on-chain: the
    ///         emergency multisig can suspend this body's
    ///         entries, so a shared signer would grade their own homework. Where
    ///         both sides expose a Safe-shaped `getOwners()` the disjointness
    ///         is enforced here and `signersVerified` is emitted true; where
    ///         either does not, the ADR puts the obligation on the governance
    ///         proposal off-chain and the event records that this registration
    ///         took the unverified path.
    /// @param  emergencyMultisig The `EMERGENCY_MULTISIG_ROLE` holder to check
    ///         disjointness against. Validated to actually hold the role, so it
    ///         cannot be pointed at a decoy to fake a clean probe.
    function registerRegionalBody(bytes32 region, address body, address emergencyMultisig)
        external
        onlyRole(GOVERNANCE_ROLE)
    {
        if (region == bytes32(0) || region == GLOBAL_REGION) revert MissingRegion();
        if (body == address(0)) revert ZeroAddress();
        if (!hasRole(EMERGENCY_MULTISIG_ROLE, emergencyMultisig)) {
            revert NotEmergencyMultisig(emergencyMultisig);
        }
        if (body == emergencyMultisig) revert SignerOverlap(body);

        RegionalBody storage rb = _regionalBodies[region];
        if (rb.body != address(0)) revert RegionalBodyAlreadyRegistered(region, rb.body);

        bytes32 existing = regionOfBody[body];
        if (existing != bytes32(0)) revert BodyAlreadyServingRegion(body, existing);

        bool signersVerified = _probeSignerDisjointness(body, emergencyMultisig);

        rb.body = body;
        regionOfBody[body] = region;
        _grantRole(REGIONAL_BODY_ROLE, body);
        emit RegionalBodyRegistered(region, body, signersVerified);
    }

    function deregisterRegionalBody(bytes32 region) external onlyRole(GOVERNANCE_ROLE) {
        address body = _requireRegisteredBody(region);
        delete _regionalBodies[region];
        delete regionOfBody[body];
        _revokeRole(REGIONAL_BODY_ROLE, body);
        emit RegionalBodyDeregistered(region, body);
    }

    /// @notice Immediately bar `region`'s body from issuing or removing entries.
    ///         Existing entries stay live (ADR 011 § Regional Governance
    ///         Bodies) — suspension is about the body's future authority, not a
    ///         mass retraction of the jurisdiction's takedowns.
    /// @dev    Must be ratified or reversed by governance within
    ///         `BODY_SUSPENSION_RATIFICATION_WINDOW`; an unratified suspension
    ///         lapses on its own, see `_bodySuspended`.
    function suspendRegionalBody(bytes32 region) external onlyRole(EMERGENCY_MULTISIG_ROLE) {
        address body = _requireRegisteredBody(region);
        RegionalBody storage rb = _regionalBodies[region];
        if (_bodySuspended(rb)) revert RegionalBodyAlreadySuspended(region);
        rb.suspendedAt = uint64(block.timestamp);
        rb.suspensionRatified = false;
        emit RegionalBodySuspended(region, body);
    }

    /// @notice Governance confirms the multisig's suspension, making it stand
    ///         until an explicit `unsuspendRegionalBody`.
    function ratifyRegionalBodySuspension(bytes32 region) external onlyRole(GOVERNANCE_ROLE) {
        address body = _requireRegisteredBody(region);
        RegionalBody storage rb = _regionalBodies[region];
        if (rb.suspendedAt == 0) revert RegionalBodySuspensionNotActive(region);
        if (!rb.suspensionRatified) {
            uint64 closesAt = rb.suspendedAt + BODY_SUSPENSION_RATIFICATION_WINDOW;
            // forge-lint: disable-next-line(block-timestamp)
            if (block.timestamp > closesAt) revert SuspensionWindowClosed(closesAt);
        }
        rb.suspensionRatified = true;
        emit RegionalBodySuspensionRatified(region, body);
    }

    /// @notice Governance reverses a suspension (ratified or not), restoring the
    ///         body's authority.
    function unsuspendRegionalBody(bytes32 region) external onlyRole(GOVERNANCE_ROLE) {
        address body = _requireRegisteredBody(region);
        RegionalBody storage rb = _regionalBodies[region];
        if (rb.suspendedAt == 0) revert RegionalBodySuspensionNotActive(region);
        rb.suspendedAt = 0;
        rb.suspensionRatified = false;
        emit RegionalBodyUnsuspended(region, body);
    }

    /// @notice The body registered for `region`, plus its suspension state.
    function getRegionalBody(bytes32 region) external view returns (RegionalBody memory) {
        return _regionalBodies[region];
    }

    /// @notice True while `region`'s body is barred from writing — i.e. suspended
    ///         and either ratified or still inside the 14-day window.
    function isRegionalBodySuspended(bytes32 region) external view returns (bool) {
        return _bodySuspended(_regionalBodies[region]);
    }

    // -----------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------

    function _addHash(bytes32 region, bytes32 hash, string calldata reason, bool emergency, Category category)
        internal
    {
        if (hash == bytes32(0)) revert ZeroHash();
        HashEntry storage e = _hashEntries[region][hash];
        // Re-adding refreshes `addedAt` (resets the filing window) and overwrites
        // the audit-trail reason with the current notice's.
        e.addedAt = uint64(block.timestamp);
        // ADR 011 § Compliance Window. Stamped, not derived — see the `HashEntry`
        // doc. A re-add restamps, which is correct: a fresh notice restarts the
        // grace, and re-adding under governance is exactly how an emergency
        // entry is ratified into a permanent one (`emergency` back to false).
        e.effectiveAt = uint64(block.timestamp) + (emergency ? emergencyComplianceWindow : complianceWindow);
        e.emergency = emergency;
        e.category = uint8(category);
        hashReason[region][hash] = reason;
        // Idempotent: a re-add restamps the entry and leaves the index alone.
        // slither-disable-next-line unused-return
        _regionHashes[region].add(hash);
        emit HashBlacklisted(region, hash, reason);
    }

    function _removeHashRegional(bytes32 region, bytes32 hash) internal {
        HashEntry storage e = _hashEntries[region][hash];
        if (e.addedAt == 0) revert EntryNotBlacklisted(region, hash);
        delete _hashEntries[region][hash];
        delete hashReason[region][hash];
        // slither-disable-next-line unused-return
        _regionHashes[region].remove(hash);
        emit HashRemoved(region, hash);
    }

    /// @dev Re-derive `a`'s membership in the union index from BOTH source
    ///      mappings. Add-if-either / remove-if-neither, rather than mirroring
    ///      whichever list the caller just touched: the two are set independently
    ///      (`setOriginBlacklist` never touches `isOperatorBlacklisted`, and
    ///      `addOperator` never touches `_isOriginBlacklisted`), so a caller
    ///      clearing one while the other still holds must NOT drop the address.
    ///      Reads `isOriginBlacklisted` — the expiry-honouring view, not the raw
    ///      mapping — so an emergency origin that lapsed leaves the index on the
    ///      next write that touches it.
    function _syncAddr(address a) internal {
        if (isOriginBlacklisted(a) || isOperatorBlacklisted[a]) {
            // slither-disable-next-line unused-return
            _blacklistedAddrs.add(a);
        } else {
            // slither-disable-next-line unused-return
            _blacklistedAddrs.remove(a);
        }
    }

    /// @dev Reads storage directly to avoid the `storage → memory` flagged
    ///      by aderyn H-2. An entry is live iff it was added (`addedAt != 0`)
    ///      and — for emergency entries — has not passed its category deadline. The expiry check lives HERE
    ///      rather than only in `expireEmergencyEntry` so an entry nobody has
    ///      bothered to clean up still stops being enforceable the instant it
    ///      lapses; the permissionless call materializes that, it does not cause
    ///      it.
    /// @dev The ADR 030 region-scope predicate, resolved once for both the point
    ///      query (`isHashBlacklistedForOperator`) and the enumeration
    ///      (`getScopeRegions`). `cur` is `bytes32(0)` when the operator declares
    ///      no region or declares GLOBAL; `prevApplies` is true only while the
    ///      ripening window is still open over a distinct previous region.
    // slither-disable-next-line unused-return
    function _scopeRegions(address operator) internal view returns (bytes32 cur, bytes32 prev, bool prevApplies) {
        (string memory regionHint, string memory regionPrev, uint64 regionLastChanged, uint256 window) =
            capacityBondRegion.regionScopeData(operator);

        // The ripening window runs from `regionLastChanged` (set at registration,
        // restamped on each `updateRegion`), used directly as `effective`.
        // forge-lint: disable-next-line(block-timestamp)
        return RegionScopeLib.scopedRegions(
            GLOBAL_REGION, regionHint, regionPrev, uint64(block.timestamp), regionLastChanged, window
        );
    }

    function _isLive(bytes32 region, bytes32 hash) internal view returns (bool) {
        HashEntry storage e = _hashEntries[region][hash];
        if (e.addedAt == 0) return false;
        return !_emergencyExpired(e.emergency, e.addedAt, e.category);
    }

    /// @dev True iff this is an emergency entry past its category deadline.
    ///      Non-emergency entries never expire, so the `emergency` guard short-
    ///      circuits before any arithmetic.
    function _emergencyExpired(bool emergency, uint64 addedAt, uint8 category) internal view returns (bool) {
        if (!emergency || addedAt == 0) return false;
        // forge-lint: disable-next-line(block-timestamp)
        return block.timestamp > uint256(addedAt) + _expiryFor(category);
    }

    /// @dev ADR 011 § Contract: ContentBlacklist — GENERAL entries lapse in 14
    ///      days, severe ones (CSAM / TERRORIST) in 90. An out-of-range value is
    ///      unreachable (`_toCategory` validates at every entry point) but maps
    ///      to the severe term rather than reverting: a view must never revert
    ///      on stored state, and over-enforcing is the safe direction here.
    function _expiryFor(uint8 category) internal pure returns (uint64) {
        return category == uint8(Category.GENERAL) ? EMERGENCY_EXPIRY_GENERAL : EMERGENCY_EXPIRY_SEVERE;
    }

    function _toCategory(uint8 category) internal pure returns (Category) {
        if (category > uint8(Category.TERRORIST)) revert InvalidCategory(category);
        return Category(category);
    }

    /// @dev A body is barred while a ratified suspension stands, or while an
    ///      unratified one is still inside its 14-day governance window. Past
    ///      that window an unratified suspension has lapsed — ADR 011 requires
    ///      the suspension to be ratified or reversed within it, so governance
    ///      silence is not ratification.
    function _bodySuspended(RegionalBody storage rb) internal view returns (bool) {
        if (rb.suspendedAt == 0) return false;
        if (rb.suspensionRatified) return true;
        // forge-lint: disable-next-line(block-timestamp)
        return block.timestamp <= uint256(rb.suspendedAt) + BODY_SUSPENSION_RATIFICATION_WINDOW;
    }

    function _requireRegisteredBody(bytes32 region) internal view returns (address) {
        address body = _regionalBodies[region].body;
        if (body == address(0)) revert RegionalBodyNotRegistered(region);
        return body;
    }

    /// @dev The authority check `REGIONAL_BODY_ROLE` alone cannot express: the
    ///      caller must be THE body bound to `region`, and not suspended.
    function _requireActiveBodyFor(bytes32 region) internal view {
        RegionalBody storage rb = _regionalBodies[region];
        if (rb.body != msg.sender) revert NotRegionalBodyFor(region, msg.sender);
        if (_bodySuspended(rb)) revert NotRegionalBodyFor(region, msg.sender);
    }

    /// @dev Best-effort on-chain half of the ADR 011 § Signer non-overlap rule.
    ///      Returns true when BOTH sides exposed an enumerable signer set and
    ///      they were verified disjoint; false when either is not enumerable,
    ///      which routes the obligation to the off-chain path the ADR specifies.
    ///      Reverts with `SignerOverlap` on an actual overlap.
    /// @dev  The multisig side is passed in rather than read from state because
    ///       `EMERGENCY_MULTISIG_ROLE` lives in plain `AccessControl`, which
    ///       cannot enumerate its holders. Caching the address instead would
    ///       introduce a second source of truth that silently drifts the first
    ///       time the role is regranted; validating the caller-supplied address
    ///       against the live role cannot drift.
    /// @dev  Both sides are untrusted contracts here, so each call is
    ///       `try`-guarded and each returned set is length-capped — the nested
    ///       loop is bounded at MAX_PROBED_SIGNERS², cheap for a call that only
    ///       ever runs behind a governance timelock.
    function _probeSignerDisjointness(address body, address emergencyMultisig) internal view returns (bool) {
        address[] memory bodyOwners = _tryGetOwners(body);
        if (bodyOwners.length == 0) return false;

        // A signer holding the role directly is an overlap regardless of whether
        // the multisig side turns out to be enumerable, so check it first.
        for (uint256 i = 0; i < bodyOwners.length; ++i) {
            if (hasRole(EMERGENCY_MULTISIG_ROLE, bodyOwners[i])) revert SignerOverlap(bodyOwners[i]);
        }

        address[] memory msigOwners = _tryGetOwners(emergencyMultisig);
        if (msigOwners.length == 0) return false;

        for (uint256 i = 0; i < bodyOwners.length; ++i) {
            for (uint256 j = 0; j < msigOwners.length; ++j) {
                if (bodyOwners[i] == msigOwners[j]) revert SignerOverlap(bodyOwners[i]);
            }
        }
        return true;
    }

    /// @dev Safe-shaped `getOwners()` probe. Returns an empty array for a
    ///      non-enumerable, reverting, or implausibly large signer set — every
    ///      one of which means "cannot verify on-chain", not "verified empty".
    function _tryGetOwners(address account) internal view returns (address[] memory) {
        // An EOA is the common case for a bootstrap-phase multisig placeholder.
        // The check is not redundant with `try`: Solidity's `extcodesize` guard
        // for a value-returning call is emitted in THIS frame, so it reverts
        // past the `catch` rather than into it.
        if (account.code.length == 0) return new address[](0);
        try IEnumerableSigners(account).getOwners() returns (address[] memory owners) {
            if (owners.length > MAX_PROBED_SIGNERS) return new address[](0);
            return owners;
        } catch {
            return new address[](0);
        }
    }

    function _enforceComplianceWindowBounds(uint64 value) internal pure {
        if (value < COMPLIANCE_WINDOW_FLOOR || value > COMPLIANCE_WINDOW_CEILING) {
            revert ParamOutOfBounds(value, COMPLIANCE_WINDOW_FLOOR, COMPLIANCE_WINDOW_CEILING);
        }
    }
}
