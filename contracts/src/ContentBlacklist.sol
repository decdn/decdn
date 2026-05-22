// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { SafeCast } from "@openzeppelin/contracts/utils/math/SafeCast.sol";

import { IStakingRegistryEject } from "./interfaces/IStakingRegistryEject.sol";

/// @title ContentBlacklist (core)
/// @notice Governance-controlled on-chain hash and origin blacklist (ADR 011 §
///         Contract: ContentBlacklist). Carries the global + regional hash
///         paths, origin blacklisting (which force-ejects the operator from
///         `StakingRegistry`), the emergency-multisig fast path with a 12-month
///         sunset, the regional-body registry, the governable compliance
///         window, and a monotonic version counter that nodes poll for deltas.
/// @dev    Entries are keyed by `(blake3Hash, region)` with `region == bytes2(0)`
///         as the global sentinel, so a hash may be globally blacklisted and/or
///         independently blacklisted by multiple regional bodies (consistent
///         with ADR 031's `(hash, region)` keying). `region` is stored as
///         `bytes2` (ISO 3166-1 alpha-2, upper-cased); external functions take
///         `string` and canonicalize internally.
///
///         `effectiveAt` is the slashability threshold (`addedAt + grace`):
///         `grace` is the governable `complianceWindow` for governance entries
///         and a fixed 2h for emergency entries. `isBlacklisted` does NOT gate
///         on `effectiveAt` — eviction is required immediately; `effectiveAt`
///         only bounds when serving becomes a slashable offense (consumed by
///         `SlashJudge`).
///
///         The per-entry appeal surface (ADR 031) — `openBlacklistAppeal` and
///         the multisig/Governor lifecycle that flips `suspended` — lands in a
///         follow-up PR. The `suspended` / `suspendedAtUs` fields exist here so
///         the `isBlacklisted` views are forward-compatible; nothing in this
///         contract sets them yet.
contract ContentBlacklist is AccessControl, ReentrancyGuard {
    using SafeCast for uint256;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    /// @notice ve-Governor (via TimelockController): all standard add/remove,
    ///         regional-body registry, unsuspend, and parameter changes.
    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    /// @notice 3-of-5 emergency multisig: emergency add paths and regional-body
    ///         suspension (ADR 009 § Emergency Multisig).
    bytes32 public constant EMERGENCY_ROLE = keccak256("EMERGENCY_ROLE");

    // -----------------------------------------------------------------
    // Categories + fixed windows
    // -----------------------------------------------------------------

    /// @notice Emergency entry category, governing auto-expiry.
    enum Category {
        GENERAL, // 0 — 14-day auto-expiry
        CSAM, // 1 — 90-day auto-expiry
        TERRORIST // 2 — 90-day auto-expiry
    }

    /// @notice Grace before an emergency entry becomes slashable (fixed).
    uint64 public constant EMERGENCY_COMPLIANCE_WINDOW = 2 hours;
    /// @notice Auto-expiry for `GENERAL` emergency entries.
    uint64 public constant GENERAL_EXPIRY = 14 days;
    /// @notice Auto-expiry for `CSAM` / `TERRORIST` emergency entries.
    uint64 public constant SEVERE_EXPIRY = 90 days;
    /// @notice Emergency-path sunset horizon from deployment (ADR 009).
    uint64 public constant BLACKLIST_SUNSET = 365 days;

    /// @notice Governable `complianceWindow` safety bounds (ADR 011 / ADR 009).
    uint64 public constant COMPLIANCE_WINDOW_FLOOR = 1 hours;
    uint64 public constant COMPLIANCE_WINDOW_CEILING = 7 days;

    // -----------------------------------------------------------------
    // Immutables + governable parameters
    // -----------------------------------------------------------------

    /// @notice Staking registry that origin blacklisting ejects from. Must have
    ///         granted this contract `BLACKLIST_ROLE`.
    IStakingRegistryEject public immutable stakingRegistry;

    /// @notice After this timestamp the emergency add paths revert (ADR 009
    ///         sunset). Immutable; extension requires a new deployment.
    uint64 public immutable blacklistDeadline;

    /// @notice Grace (seconds) between a governance entry's `addedAt` and its
    ///         `effectiveAt` slashability threshold. Default 24h.
    uint64 public complianceWindow;

    // -----------------------------------------------------------------
    // Storage
    // -----------------------------------------------------------------

    /// @notice Per-entry record. `addedAt == 0` means no entry.
    struct BlacklistEntry {
        bytes32 blake3Hash; // the disputed hash (mirrors the key for getEntry returns)
        uint64 addedAt; // block timestamp when added
        uint64 effectiveAt; // addedAt + grace — serving after this is slashable
        uint64 expiresAt; // emergency: addedAt + categoryExpiry; 0 = never (governance entries)
        uint64 suspendedAtUs; // microsecond ts when `suspended` last flipped true; 0 if never (appeals, PR 2)
        bytes2 region; // bytes2(0) = global; ISO 3166-1 alpha-2 otherwise
        bool emergency; // added via the emergency multisig path
        bool suspended; // appeal interim-relief (appeals, PR 2)
        string reason; // free-form audit string (legal notice id, category label, ...)
    }

    /// @notice Per-origin record. Origins are global-only (no region).
    struct OriginEntry {
        uint64 addedAt;
        uint64 expiresAt; // emergency: addedAt + categoryExpiry; 0 = never
        bool emergency;
        string reason;
    }

    /// @notice `blake3Hash => region => entry`. Global entries key on `bytes2(0)`.
    mapping(bytes32 blake3Hash => mapping(bytes2 region => BlacklistEntry)) internal _entries;

    /// @notice `operator => origin entry`.
    mapping(address operator => OriginEntry) internal _origins;

    /// @notice `region => registered body`. `address(0)` means none.
    mapping(bytes2 region => address body) public regionalBodyOf;

    /// @notice `region => whether its body is suspended` (set by the multisig).
    mapping(bytes2 region => bool suspended) public isRegionalBodySuspended;

    /// @notice Monotonic counter bumped on every add/remove across all paths;
    ///         nodes poll it to fetch deltas (ADR 011 § Blacklist version).
    uint256 public version;

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error ZeroHash();
    error EmptyRegion();
    error InvalidRegion();
    error InvalidCategory();
    error EntryNotFound();
    error OriginNotFound();
    error NotRegionalBody();
    error RegionalBodyIsSuspended();
    error RegionalBodyNotRegistered();
    error RegionAlreadyHasBody();
    error EmergencySunsetReached();
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);

    // -----------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------

    event HashBlacklisted(
        bytes32 indexed blake3Hash,
        uint256 indexed version,
        uint256 effectiveAt,
        string region,
        string reason,
        bool emergency
    );
    event HashRemoved(bytes32 indexed blake3Hash, uint256 indexed version, string region);
    event OriginBlacklisted(address indexed operatorAddress, uint256 indexed version, string reason, bool emergency);
    event OriginRemoved(address indexed operatorAddress, uint256 indexed version);
    event RegionalBodyRegistered(bytes2 indexed region, address indexed body);
    event RegionalBodyDeregistered(bytes2 indexed region, address indexed body);
    event RegionalBodySuspended(bytes2 indexed region, address indexed body);
    event RegionalBodyUnsuspended(bytes2 indexed region, address indexed body);
    event ComplianceWindowUpdated(uint64 oldValue, uint64 newValue);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param stakingRegistry_ Registry to eject blacklisted origins from.
    /// @param admin            Initial `DEFAULT_ADMIN_ROLE` (deployer pre-handover).
    /// @param governance       `GOVERNANCE_ROLE` holder (TimelockController).
    /// @param emergencyMultisig `EMERGENCY_ROLE` holder (3-of-5 multisig).
    constructor(IStakingRegistryEject stakingRegistry_, address admin, address governance, address emergencyMultisig) {
        if (
            address(stakingRegistry_) == address(0) || admin == address(0) || governance == address(0)
                || emergencyMultisig == address(0)
        ) {
            revert ZeroAddress();
        }
        stakingRegistry = stakingRegistry_;
        blacklistDeadline = _now() + BLACKLIST_SUNSET;
        complianceWindow = 24 hours;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, governance);
        _grantRole(EMERGENCY_ROLE, emergencyMultisig);
    }

    // -----------------------------------------------------------------
    // Hash blacklist — global governance path
    // -----------------------------------------------------------------

    /// @notice Blacklist `blake3Hash` network-wide. Overwrites any existing
    ///         entry for the global key (e.g. ratifying an expired emergency
    ///         entry into a permanent one).
    function addHash(bytes32 blake3Hash, string calldata reason) external onlyRole(GOVERNANCE_ROLE) {
        _writeHashEntry(blake3Hash, bytes2(0), reason, false, Category.GENERAL);
    }

    /// @notice Remove the global entry for `blake3Hash`.
    function removeHash(bytes32 blake3Hash) external onlyRole(GOVERNANCE_ROLE) {
        _removeHashEntry(blake3Hash, bytes2(0), "");
    }

    // -----------------------------------------------------------------
    // Hash blacklist — regional path (registered body for its own region)
    // -----------------------------------------------------------------

    /// @notice Blacklist `blake3Hash` for `region`. Callable only by the active
    ///         registered body for that region.
    function addHashRegional(bytes32 blake3Hash, string calldata region, string calldata reason) external {
        bytes2 r = _toRegion(region);
        _requireActiveRegionalBody(r);
        _writeHashEntry(blake3Hash, r, reason, false, Category.GENERAL);
    }

    /// @notice Remove a regional entry. Governance-only (the slow-path override
    ///         and the appeal ratification path both route here; ADR 011 §
    ///         Global Override).
    function removeHashRegional(bytes32 blake3Hash, string calldata region) external onlyRole(GOVERNANCE_ROLE) {
        bytes2 r = _toRegion(region);
        _removeHashEntry(blake3Hash, r, region);
    }

    // -----------------------------------------------------------------
    // Emergency multisig path (sunset-bounded)
    // -----------------------------------------------------------------

    /// @notice Emergency global blacklist with category-specific auto-expiry.
    ///         Reverts once the sunset horizon is reached.
    function emergencyAdd(bytes32 blake3Hash, uint8 category, string calldata reason)
        external
        onlyRole(EMERGENCY_ROLE)
    {
        _requirePreSunset();
        _writeHashEntry(blake3Hash, bytes2(0), reason, true, _toCategory(category));
    }

    /// @notice Emergency origin blacklist with category-specific auto-expiry.
    function emergencyAddOrigin(address operatorAddress, uint8 category, string calldata reason)
        external
        onlyRole(EMERGENCY_ROLE)
        nonReentrant
    {
        _requirePreSunset();
        _writeOriginEntry(operatorAddress, reason, true, _toCategory(category));
    }

    // -----------------------------------------------------------------
    // Origin blacklist — global governance path
    // -----------------------------------------------------------------

    /// @notice Blacklist an operator address and force-eject it from
    ///         `StakingRegistry`. Re-entry requires fresh stake under a new
    ///         identity unless `removeOrigin` is called (ADR 011 § Hash Evasion
    ///         and Origin Blacklisting).
    function addOrigin(address operatorAddress, string calldata reason)
        external
        onlyRole(GOVERNANCE_ROLE)
        nonReentrant
    {
        _writeOriginEntry(operatorAddress, reason, false, Category.GENERAL);
    }

    /// @notice Remove an origin entry. Does not un-eject the operator on
    ///         `StakingRegistry`; the operator restakes to re-activate.
    function removeOrigin(address operatorAddress) external onlyRole(GOVERNANCE_ROLE) {
        // `addedAt == 0` is the presence sentinel (no entry), not a value-bearing equality.
        // slither-disable-next-line incorrect-equality
        if (_origins[operatorAddress].addedAt == 0) revert OriginNotFound();
        delete _origins[operatorAddress];
        _bumpVersion();
        emit OriginRemoved(operatorAddress, version);
    }

    // -----------------------------------------------------------------
    // Regional body registry
    // -----------------------------------------------------------------

    /// @notice Register a regional governance body for `region`. Signer
    ///         non-overlap with the emergency multisig is a governance
    ///         obligation verified off-chain (ADR 011 § Regional Governance
    ///         Bodies).
    function registerRegionalBody(string calldata region, address body) external onlyRole(GOVERNANCE_ROLE) {
        if (body == address(0)) revert ZeroAddress();
        bytes2 r = _toRegion(region);
        if (regionalBodyOf[r] != address(0)) revert RegionAlreadyHasBody();
        regionalBodyOf[r] = body;
        emit RegionalBodyRegistered(r, body);
    }

    /// @notice Deregister the body for `region`. Existing entries it issued
    ///         remain active.
    function deregisterRegionalBody(string calldata region) external onlyRole(GOVERNANCE_ROLE) {
        bytes2 r = _toRegion(region);
        address body = regionalBodyOf[r];
        if (body == address(0)) revert RegionalBodyNotRegistered();
        delete regionalBodyOf[r];
        delete isRegionalBodySuspended[r];
        emit RegionalBodyDeregistered(r, body);
    }

    /// @notice Suspend a regional body (emergency multisig). Must be ratified or
    ///         reversed by governance per ADR 011 § Regional Governance Bodies.
    function suspendRegionalBody(string calldata region) external onlyRole(EMERGENCY_ROLE) {
        bytes2 r = _toRegion(region);
        address body = regionalBodyOf[r];
        if (body == address(0)) revert RegionalBodyNotRegistered();
        isRegionalBodySuspended[r] = true;
        emit RegionalBodySuspended(r, body);
    }

    /// @notice Lift a regional-body suspension (governance ratification path).
    function unsuspendRegionalBody(string calldata region) external onlyRole(GOVERNANCE_ROLE) {
        bytes2 r = _toRegion(region);
        address body = regionalBodyOf[r];
        if (body == address(0)) revert RegionalBodyNotRegistered();
        isRegionalBodySuspended[r] = false;
        emit RegionalBodyUnsuspended(r, body);
    }

    // -----------------------------------------------------------------
    // Governable parameter
    // -----------------------------------------------------------------

    /// @notice Set the governance-entry compliance window (bounded [1h, 7d]).
    function setComplianceWindow(uint64 newWindow) external onlyRole(GOVERNANCE_ROLE) {
        if (newWindow < COMPLIANCE_WINDOW_FLOOR || newWindow > COMPLIANCE_WINDOW_CEILING) {
            revert ParamOutOfBounds({
                value: newWindow, floor: COMPLIANCE_WINDOW_FLOOR, ceiling: COMPLIANCE_WINDOW_CEILING
            });
        }
        uint64 old = complianceWindow;
        complianceWindow = newWindow;
        emit ComplianceWindowUpdated(old, newWindow);
    }

    // -----------------------------------------------------------------
    // Views
    // -----------------------------------------------------------------

    /// @notice True iff `blake3Hash` carries an active global entry.
    function isBlacklisted(bytes32 blake3Hash) public view returns (bool) {
        return _entryActive(_entries[blake3Hash][bytes2(0)]);
    }

    /// @notice True iff `blake3Hash` is globally blacklisted or blacklisted in
    ///         `region`. An empty `region` checks the global entry only.
    function isBlacklistedInRegion(bytes32 blake3Hash, string calldata region) external view returns (bool) {
        if (_entryActive(_entries[blake3Hash][bytes2(0)])) return true;
        if (bytes(region).length == 0) return false;
        return _entryActive(_entries[blake3Hash][_toRegion(region)]);
    }

    /// @notice True iff `operatorAddress` carries an active origin entry.
    function isOriginBlacklisted(address operatorAddress) external view returns (bool) {
        return _originActive(_origins[operatorAddress]);
    }

    /// @notice The global entry for `blake3Hash` (zero struct if none).
    function getEntry(bytes32 blake3Hash) external view returns (BlacklistEntry memory) {
        return _entries[blake3Hash][bytes2(0)];
    }

    /// @notice The `(blake3Hash, region)` entry (zero struct if none).
    function getEntryInRegion(bytes32 blake3Hash, string calldata region)
        external
        view
        returns (BlacklistEntry memory)
    {
        return _entries[blake3Hash][_toRegion(region)];
    }

    /// @notice Current blacklist version (poll for deltas).
    function getBlacklistVersion() external view returns (uint256) {
        return version;
    }

    // -----------------------------------------------------------------
    // Internal — writes
    // -----------------------------------------------------------------

    function _writeHashEntry(bytes32 blake3Hash, bytes2 region, string calldata reason, bool emergency, Category cat)
        internal
    {
        if (blake3Hash == bytes32(0)) revert ZeroHash();
        uint64 nowTs = _now();
        uint64 grace = emergency ? EMERGENCY_COMPLIANCE_WINDOW : complianceWindow;

        BlacklistEntry storage entry = _entries[blake3Hash][region];
        entry.blake3Hash = blake3Hash;
        entry.addedAt = nowTs;
        entry.effectiveAt = nowTs + grace;
        entry.expiresAt = emergency ? nowTs + _expiryForCategory(cat) : 0;
        entry.region = region;
        entry.emergency = emergency;
        // Overwriting an entry clears any prior interim-relief suspension.
        entry.suspended = false;
        entry.suspendedAtUs = 0;
        entry.reason = reason;

        _bumpVersion();
        string memory regionStr = region == bytes2(0) ? "" : _regionToString(region);
        emit HashBlacklisted(blake3Hash, version, entry.effectiveAt, regionStr, reason, emergency);
    }

    function _removeHashEntry(bytes32 blake3Hash, bytes2 region, string memory regionStr) internal {
        // `addedAt == 0` is the presence sentinel (no entry), not a value-bearing
        // equality — the strict comparison is the intended semantics.
        // slither-disable-next-line incorrect-equality
        if (_entries[blake3Hash][region].addedAt == 0) revert EntryNotFound();
        delete _entries[blake3Hash][region];
        _bumpVersion();
        emit HashRemoved(blake3Hash, version, regionStr);
    }

    function _writeOriginEntry(address operatorAddress, string calldata reason, bool emergency, Category cat) internal {
        if (operatorAddress == address(0)) revert ZeroAddress();
        uint64 nowTs = _now();

        OriginEntry storage entry = _origins[operatorAddress];
        entry.addedAt = nowTs;
        entry.expiresAt = emergency ? nowTs + _expiryForCategory(cat) : 0;
        entry.emergency = emergency;
        entry.reason = reason;

        _bumpVersion();
        emit OriginBlacklisted(operatorAddress, version, reason, emergency);

        // Effects above, interaction last. Idempotent on the registry side.
        stakingRegistry.ejectNode(operatorAddress);
    }

    function _bumpVersion() internal {
        unchecked {
            ++version;
        }
    }

    // -----------------------------------------------------------------
    // Internal — predicates + helpers
    // -----------------------------------------------------------------

    /// @dev An entry is active iff present, not suspended, and (for emergency
    ///      entries) not past its auto-expiry. Governance entries never expire
    ///      (`expiresAt == 0`).
    function _entryActive(BlacklistEntry storage entry) internal view returns (bool) {
        if (entry.addedAt == 0 || entry.suspended) return false;
        // `expiresAt == 0` is the never-expires sentinel for governance entries;
        // emergency entries lapse once the wall clock passes their expiry. The
        // coarse (hours/days) windows make second-level validator skew immaterial.
        // forge-lint: disable-next-line(block-timestamp)
        return entry.expiresAt == 0 || block.timestamp < entry.expiresAt;
    }

    function _originActive(OriginEntry storage entry) internal view returns (bool) {
        if (entry.addedAt == 0) return false;
        // forge-lint: disable-next-line(block-timestamp)
        return entry.expiresAt == 0 || block.timestamp < entry.expiresAt;
    }

    function _requireActiveRegionalBody(bytes2 region) internal view {
        if (regionalBodyOf[region] != msg.sender) revert NotRegionalBody();
        if (isRegionalBodySuspended[region]) revert RegionalBodyIsSuspended();
    }

    function _requirePreSunset() internal view {
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp >= blacklistDeadline) revert EmergencySunsetReached();
    }

    function _expiryForCategory(Category cat) internal pure returns (uint64) {
        return cat == Category.GENERAL ? GENERAL_EXPIRY : SEVERE_EXPIRY;
    }

    function _toCategory(uint8 category) internal pure returns (Category) {
        if (category > uint8(Category.TERRORIST)) revert InvalidCategory();
        return Category(category);
    }

    function _now() private view returns (uint64) {
        // forge-lint: disable-next-line(block-timestamp)
        return block.timestamp.toUint64();
    }

    /// @dev Canonicalize a 2-character ISO 3166-1 alpha-2 region to upper-cased
    ///      `bytes2`. Reverts on the global sentinel (empty) and on any non
    ///      length-2 / non-ASCII-alpha input.
    function _toRegion(string calldata region) internal pure returns (bytes2) {
        bytes memory b = bytes(region);
        if (b.length == 0) revert EmptyRegion();
        if (b.length != 2) revert InvalidRegion();
        bytes1 c0 = _upperAlpha(b[0]);
        bytes1 c1 = _upperAlpha(b[1]);
        return bytes2(c0) | (bytes2(c1) >> 8);
    }

    function _upperAlpha(bytes1 c) private pure returns (bytes1) {
        uint8 u = uint8(c);
        if (u >= 0x61 && u <= 0x7A) u -= 0x20; // a-z -> A-Z
        if (u < 0x41 || u > 0x5A) revert InvalidRegion(); // require A-Z
        return bytes1(u);
    }

    function _regionToString(bytes2 region) private pure returns (string memory) {
        bytes memory out = new bytes(2);
        out[0] = region[0];
        out[1] = region[1];
        return string(out);
    }
}
