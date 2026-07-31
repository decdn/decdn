// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { EnumerableSet } from "@openzeppelin/contracts/utils/structs/EnumerableSet.sol";

import { ICapacityBondActivity } from "./interfaces/ICapacityBondActivity.sol";
import { IPublisherRegistryOwnership } from "./interfaces/IPublisherRegistryOwnership.sol";
import { IContentBlacklistOriginView } from "./interfaces/IContentBlacklistOriginView.sol";

/// @title OriginAssignment
/// @notice The DAO's positive origin authority (ADR 011 § Origin Assignment
///         Authority), split into two planes:
///         - **Vetting (cold).** Governance decides once, per publisher wallet,
///           who is a network-trusted publisher. A publisher requests vetting,
///           waits `vettingTimelock`, and governance grants it — or governance
///           grants/revokes instantly with `setPublisherVetted`.
///         - **Origins (hot).** A vetted publisher seats and unseats origins for
///           its OWN namespaces instantly, one operator at a time, choosing
///           freely among bonded operators.
///         Namespace 0 (`namespaceId == 0`) has no publisher, so no operator is
///         ever seated for it — `getOrigins(0)` is empty and
///         `isAuthorizedOrigin(0, op)` is always false. Authorized sets are
///         `EnumerableSet`s.
/// @dev    OZ bases per ADR 016 § Contract Inventory: `AccessControl` (governance
///         gating) + `ReentrancyGuard` (all cross-contract reads are views, but
///         the guard matches the inventory and the repo's external-call-then-write
///         convention). Holds no funds. The `ContentBlacklist` binding is wired
///         once post-deploy via `setContentBlacklist`; until then `addOrigin`
///         validates against `CapacityBond.isActive` only and `prune` reverts.
contract OriginAssignment is AccessControl, ReentrancyGuard {
    using EnumerableSet for EnumerableSet.AddressSet;
    using EnumerableSet for EnumerableSet.UintSet;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");

    // -----------------------------------------------------------------
    // Constants (ADR 009 § Governable parameters with safety bounds)
    // -----------------------------------------------------------------

    uint256 internal constant VETTING_TIMELOCK_FLOOR = 24 hours;
    uint256 internal constant VETTING_TIMELOCK_CEILING = 14 days;
    uint256 internal constant MAX_ORIGINS_FLOOR = 1;
    uint256 internal constant MAX_ORIGINS_CEILING = 50;

    uint256 internal constant DEFAULT_VETTING_TIMELOCK = 3 days;
    uint256 internal constant DEFAULT_MAX_ORIGINS = 10;

    // -----------------------------------------------------------------
    // Immutables + governable state
    // -----------------------------------------------------------------

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ICapacityBondActivity public immutable capacityBond;

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IPublisherRegistryOwnership public immutable publisherRegistry;

    /// @notice Read-direction binding for `pruneBlacklistedOrigin` and the
    ///         `addOrigin` blacklist guard. `address(0)` until
    ///         `setContentBlacklist`.
    address public contentBlacklist;

    uint256 public maxOriginsPerNamespace;
    uint256 public vettingTimelock;

    // -----------------------------------------------------------------
    // Storage — publisher vetting + authorized sets
    // -----------------------------------------------------------------

    /// @notice Publishers governance trusts to seat origins for their own
    ///         namespaces. The wallet is vetted, not any operator set, so a
    ///         vetted publisher adds and removes origins with no further
    ///         governance action.
    mapping(address publisher => bool) public isVettedPublisher;

    /// @notice Unix time each pending vetting request ripens. `0` means no
    ///         request is pending for that publisher.
    mapping(address publisher => uint64) internal _vettingReadyAt;

    mapping(uint256 namespaceId => EnumerableSet.AddressSet) internal _origins;

    /// @notice Every namespace whose authorized set is currently non-empty.
    /// @dev    Membership within a namespace was always readable via `getOrigins`;
    ///         the KEY SET was not, which is the sole reason a consumer had to
    ///         replay seating events from the deploy block just to learn which
    ///         namespaces exist. Maintained alongside every `_origins` mutation:
    ///         seated on the 0→1 transition in `addOrigin`, withdrawn once the
    ///         last operator is removed or pruned, so `contains(ns)` matches
    ///         `getOrigins(ns).length > 0` exactly.
    EnumerableSet.UintSet internal _assignedNamespaces;

    // -----------------------------------------------------------------
    // Events (ADR 011 § Contract: OriginAssignment)
    // -----------------------------------------------------------------

    event VettingRequested(address indexed publisher, uint256 readyAt);
    event VettingRequestCancelled(address indexed publisher);
    event PublisherVetted(address indexed publisher, bool vetted, address indexed by);
    event OriginAdded(uint256 indexed namespaceId, address indexed operator, address indexed by);
    event OriginRemoved(uint256 indexed namespaceId, address indexed operator, address indexed by);
    event BlacklistedOriginPruned(uint256 indexed namespaceId, address indexed operator, address indexed pruner);
    event ContentBlacklistUpdated(address indexed oldAddr, address indexed newAddr);
    event MaxOriginsPerNamespaceUpdated(uint256 oldValue, uint256 newValue);
    event VettingTimelockUpdated(uint256 oldValue, uint256 newValue);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error NotNamespaceOwner(uint256 namespaceId, address caller);
    error TooManyOrigins(uint256 count, uint256 cap);
    error DuplicateOperator(address operator);
    error OperatorNotActive(address operator);
    error OperatorBlacklisted(address operator);
    error NotAuthorizedOrigin(uint256 namespaceId, address operator);
    error ContentBlacklistNotSet();
    error OperatorNotBlacklisted(address operator);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);
    error PublisherNotVetted(address publisher);
    error NoNamespaceOwned(address publisher);
    error AlreadyVetted(address publisher);
    error VettingRequestPending(address publisher);
    error NoVettingRequest(address publisher);
    error VettingTimelockNotElapsed(uint256 readyAt);

    // -----------------------------------------------------------------
    // Constructor (ADR 016 § Deployment Order, step 10)
    // -----------------------------------------------------------------

    /// @param capacityBond_       Operator activity source.
    /// @param publisherRegistry_  Namespace ownership source.
    /// @param contentBlacklist_   May be `address(0)` at deploy; bound later via
    ///                            `setContentBlacklist` (ADR 016 post-deploy step 2).
    /// @param admin               `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE` holder.
    constructor(
        ICapacityBondActivity capacityBond_,
        IPublisherRegistryOwnership publisherRegistry_,
        address contentBlacklist_,
        address admin
    ) {
        if (address(capacityBond_) == address(0) || address(publisherRegistry_) == address(0) || admin == address(0)) {
            revert ZeroAddress();
        }
        capacityBond = capacityBond_;
        publisherRegistry = publisherRegistry_;
        contentBlacklist = contentBlacklist_; // may be zero at deploy

        maxOriginsPerNamespace = DEFAULT_MAX_ORIGINS;
        vettingTimelock = DEFAULT_VETTING_TIMELOCK;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    // -----------------------------------------------------------------
    // Publisher vetting (cold path: publisher requests / governance grants)
    // -----------------------------------------------------------------

    /// @notice A publisher asks governance to vet its wallet. The request
    ///         ripens after `vettingTimelock`, which is the window governance
    ///         reviews it in.
    /// @dev The `namespaceCount` external view read precedes the pending-state
    ///      write; safe under `nonReentrant` (the read is a view).
    // slither-disable-next-line reentrancy-no-eth
    function requestVetting() external nonReentrant {
        if (isVettedPublisher[msg.sender]) revert AlreadyVetted(msg.sender);
        if (_vettingReadyAt[msg.sender] != 0) revert VettingRequestPending(msg.sender);
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (publisherRegistry.namespaceCount(msg.sender) == 0) revert NoNamespaceOwned(msg.sender);

        uint64 readyAt = uint64(block.timestamp + vettingTimelock);
        _vettingReadyAt[msg.sender] = readyAt;

        emit VettingRequested(msg.sender, readyAt);
    }

    /// @notice A publisher withdraws its own pending request.
    function cancelVettingRequest() external {
        // The field is a timestamp, so slither's strict-equality detector reads
        // this as a dangerous compare; the `== 0` sentinel is a presence check.
        // slither-disable-next-line incorrect-equality
        if (_vettingReadyAt[msg.sender] == 0) revert NoVettingRequest(msg.sender);
        delete _vettingReadyAt[msg.sender];
        emit VettingRequestCancelled(msg.sender);
    }

    /// @notice Governance grants a ripened request. This is the only place the
    ///         timelock is enforced; `setPublisherVetted` is the instant
    ///         override for both directions.
    function grantVetting(address publisher) external onlyRole(GOVERNANCE_ROLE) {
        uint64 readyAt = _vettingReadyAt[publisher];
        if (readyAt == 0) revert NoVettingRequest(publisher);
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < readyAt) revert VettingTimelockNotElapsed(readyAt);

        delete _vettingReadyAt[publisher];
        isVettedPublisher[publisher] = true;

        emit PublisherVetted(publisher, true, msg.sender);
    }

    /// @notice Governance override: vet a publisher with no wait, or un-vet a
    ///         rogue one. Un-vetting stops NEW origins immediately; origins the
    ///         publisher already seated stay until `removeOrigin` (governance may
    ///         call it on any namespace) or a blacklist prune takes them out —
    ///         evicting them here would be unbounded in the publisher's
    ///         namespace count.
    function setPublisherVetted(address publisher, bool vetted) external onlyRole(GOVERNANCE_ROLE) {
        if (publisher == address(0)) revert ZeroAddress();
        // A grant consumes any pending request; a revocation clears it too, so a
        // ripened request cannot be used to walk straight back in.
        delete _vettingReadyAt[publisher];
        isVettedPublisher[publisher] = vetted;
        emit PublisherVetted(publisher, vetted, msg.sender);
    }

    // -----------------------------------------------------------------
    // Origin seating (hot path: vetted publisher, instant, one operator)
    // -----------------------------------------------------------------

    /// @notice A vetted publisher seats one more operator as an origin for its
    ///         own namespace. Effective immediately.
    /// @dev Validation covers ONLY `operator`. Operators already in the set are
    ///      never re-checked, so a transient failure on a live origin cannot
    ///      block seating a new one (issue #1107). External `ownerOf` /
    ///      `isActive` / blacklist reads precede the set write; safe under
    ///      `nonReentrant` (the reads are views).
    // slither-disable-next-line reentrancy-no-eth
    function addOrigin(uint256 namespaceId, address operator) external nonReentrant {
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (publisherRegistry.ownerOf(namespaceId) != msg.sender) revert NotNamespaceOwner(namespaceId, msg.sender);
        if (!isVettedPublisher[msg.sender]) revert PublisherNotVetted(msg.sender);
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (!capacityBond.isActive(operator)) revert OperatorNotActive(operator);

        address blacklist = contentBlacklist;
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (blacklist != address(0) && _isBlacklisted(blacklist, operator)) {
            revert OperatorBlacklisted(operator);
        }

        EnumerableSet.AddressSet storage set = _origins[namespaceId];
        uint256 seated = set.length();
        if (seated >= maxOriginsPerNamespace) revert TooManyOrigins(seated + 1, maxOriginsPerNamespace);
        if (!set.add(operator)) revert DuplicateOperator(operator);
        // Idempotent: the namespace enters the key set on its 0→1 transition and
        // the later adds are no-ops.
        // slither-disable-next-line unused-return
        _assignedNamespaces.add(namespaceId);

        emit OriginAdded(namespaceId, operator, msg.sender);
    }

    /// @notice Publisher (own namespace) or governance (any) unseats one operator.
    // slither-disable-next-line reentrancy-no-eth
    function removeOrigin(uint256 namespaceId, address operator) external nonReentrant {
        if (!hasRole(GOVERNANCE_ROLE, msg.sender)) {
            // aderyn-ignore-next-line(reentrancy-state-change)
            if (publisherRegistry.ownerOf(namespaceId) != msg.sender) {
                revert NotNamespaceOwner(namespaceId, msg.sender);
            }
        }
        if (!_origins[namespaceId].remove(operator)) revert NotAuthorizedOrigin(namespaceId, operator);
        _pruneEmptyNamespace(namespaceId);
        emit OriginRemoved(namespaceId, operator, msg.sender);
    }

    /// @notice Permissionless: remove a blacklisted operator from a namespace's set.
    ///         The contract checks the blacklist itself, so a caller cannot grief by
    ///         naming a non-blacklisted operator.
    // slither-disable-next-line reentrancy-no-eth
    function pruneBlacklistedOrigin(uint256 namespaceId, address operator) external nonReentrant {
        address blacklist = contentBlacklist;
        if (blacklist == address(0)) revert ContentBlacklistNotSet();
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (!_isBlacklisted(blacklist, operator)) {
            revert OperatorNotBlacklisted(operator);
        }
        if (!_origins[namespaceId].remove(operator)) revert NotAuthorizedOrigin(namespaceId, operator);
        _pruneEmptyNamespace(namespaceId);
        emit BlacklistedOriginPruned(namespaceId, operator, msg.sender);
    }

    /// @dev True if `operator` is blacklisted via EITHER the origin or the
    ///      operator mapping on `ContentBlacklist` (M-2). An operator can be
    ///      ejected via the operator mapping (`addOperator`) without the origin
    ///      mapping being set, and vice versa; authorization must reject — and
    ///      pruning must succeed on — either.
    function _isBlacklisted(address blacklist, address operator) internal view returns (bool) {
        IContentBlacklistOriginView bl = IContentBlacklistOriginView(blacklist);
        return bl.isOriginBlacklisted(operator) || bl.isOperatorBlacklisted(operator);
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    /// @notice Wire (or re-point) the ContentBlacklist read direction. Rejecting
    ///         `address(0)` prevents regressing into the deployment-window state
    ///         where `addOrigin` skips the blacklist check (ADR 011 § Edge cases).
    function setContentBlacklist(address newContentBlacklist) external onlyRole(GOVERNANCE_ROLE) {
        if (newContentBlacklist == address(0)) revert ZeroAddress();
        address old = contentBlacklist;
        contentBlacklist = newContentBlacklist;
        emit ContentBlacklistUpdated(old, newContentBlacklist);
    }

    function setMaxOriginsPerNamespace(uint256 cap) external onlyRole(GOVERNANCE_ROLE) {
        if (cap < MAX_ORIGINS_FLOOR || cap > MAX_ORIGINS_CEILING) {
            revert ParamOutOfBounds(cap, MAX_ORIGINS_FLOOR, MAX_ORIGINS_CEILING);
        }
        uint256 old = maxOriginsPerNamespace;
        maxOriginsPerNamespace = cap;
        emit MaxOriginsPerNamespaceUpdated(old, cap);
    }

    function setVettingTimelock(uint256 secondsDelay) external onlyRole(GOVERNANCE_ROLE) {
        if (secondsDelay < VETTING_TIMELOCK_FLOOR || secondsDelay > VETTING_TIMELOCK_CEILING) {
            revert ParamOutOfBounds(secondsDelay, VETTING_TIMELOCK_FLOOR, VETTING_TIMELOCK_CEILING);
        }
        uint256 old = vettingTimelock;
        vettingTimelock = secondsDelay;
        emit VettingTimelockUpdated(old, secondsDelay);
    }

    // -----------------------------------------------------------------
    // Views
    // -----------------------------------------------------------------

    /// @notice Strict set membership. `namespaceId == 0` has no set, so this is
    ///         always false. Operator-level blacklist status is NOT consulted here —
    ///         off-chain consumers filter against `ContentBlacklist`.
    function isAuthorizedOrigin(uint256 namespaceId, address operator) external view returns (bool) {
        return _origins[namespaceId].contains(operator);
    }

    function getOrigins(uint256 namespaceId) external view returns (address[] memory) {
        return _origins[namespaceId].values();
    }

    /// @notice How many namespaces currently have a non-empty authorized set.
    function assignedNamespaceCount() external view returns (uint256) {
        return _assignedNamespaces.length();
    }

    /// @notice A page of the namespaces that currently have origins, so a
    ///         consumer can bootstrap by paging this and calling `getOrigins` per
    ///         id, instead of replaying seating events from the deploy block to
    ///         discover which ids exist.
    /// @dev    Order is NOT stable across mutations (swap-and-pop on removal), so
    ///         page every offset at ONE pinned block height and re-check
    ///         `assignedNamespaceCount` there. `limit` is clamped against the
    ///         remaining length rather than compared as `offset + limit`, which
    ///         can overflow.
    function assignedNamespaces(uint256 offset, uint256 limit) external view returns (uint256[] memory page) {
        uint256 len = _assignedNamespaces.length();
        if (offset >= len) return new uint256[](0);
        uint256 n = len - offset;
        if (n > limit) n = limit;
        page = new uint256[](n);
        for (uint256 i = 0; i < n; ++i) {
            page[i] = _assignedNamespaces.at(offset + i);
        }
    }

    /// @notice Unix time `publisher`'s pending vetting request ripens, or `0`
    ///         when no request is pending.
    function getPendingVetting(address publisher) external view returns (uint256 readyAt) {
        return _vettingReadyAt[publisher];
    }

    // -----------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------

    /// @dev Withdraw `namespaceId` from the key set once its last authorized
    ///      operator is gone, keeping `_assignedNamespaces` equal to
    ///      "namespaces with a non-empty set" rather than "namespaces ever
    ///      assigned". Called after every single-operator removal.
    function _pruneEmptyNamespace(uint256 namespaceId) internal {
        if (_origins[namespaceId].length() == 0) {
            // slither-disable-next-line unused-return
            _assignedNamespaces.remove(namespaceId);
        }
    }
}
