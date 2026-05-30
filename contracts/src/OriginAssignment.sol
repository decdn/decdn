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
///         Authority): which operators may act as origin backers for which
///         namespace. Registered namespaces follow a publisher-propose /
///         governance-ratify flow gated by an assignment timelock; the
///         default-open namespace (`namespaceId == 0`) is a single
///         governance-maintained global allow-list. Authorized sets are
///         `EnumerableSet`s — `isAuthorizedOrigin(0, op)` is the same view used
///         for registered namespaces, no downstream special case.
/// @dev    OZ bases per ADR 016 § Contract Inventory: `AccessControl` (governance
///         gating) + `ReentrancyGuard` (all cross-contract reads are views, but
///         the guard matches the inventory and the repo's external-call-then-write
///         convention). Holds no funds. The `ContentBlacklist` binding is wired
///         once post-deploy via `setContentBlacklist`; until then activation
///         validates against `CapacityBond.isActive` only and `prune` reverts.
contract OriginAssignment is AccessControl, ReentrancyGuard {
    using EnumerableSet for EnumerableSet.AddressSet;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");

    // -----------------------------------------------------------------
    // Constants (ADR 009 § Governable parameters with safety bounds)
    // -----------------------------------------------------------------

    uint256 internal constant ASSIGNMENT_TIMELOCK_FLOOR = 24 hours;
    uint256 internal constant ASSIGNMENT_TIMELOCK_CEILING = 14 days;
    uint256 internal constant MAX_ORIGINS_FLOOR = 1;
    uint256 internal constant MAX_ORIGINS_CEILING = 50;
    uint256 internal constant DEFAULT_OPEN_MAX_FLOOR = 20;
    uint256 internal constant DEFAULT_OPEN_MAX_CEILING = 500;

    uint256 internal constant DEFAULT_ASSIGNMENT_TIMELOCK = 3 days;
    uint256 internal constant DEFAULT_MAX_ORIGINS = 10;
    uint256 internal constant DEFAULT_OPEN_MAX_ORIGINS = 100;

    /// @dev The default-open allow-list lives under this namespace id.
    uint256 internal constant DEFAULT_OPEN_NAMESPACE = 0;

    // -----------------------------------------------------------------
    // Immutables + governable state
    // -----------------------------------------------------------------

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ICapacityBondActivity public immutable capacityBond;

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IPublisherRegistryOwnership public immutable publisherRegistry;

    /// @notice Read-direction binding for `pruneBlacklistedAssignment` /
    ///         activation re-validation. `address(0)` until `setContentBlacklist`.
    address public contentBlacklist;

    uint256 public maxOriginsPerNamespace;
    uint256 public assignmentTimelock;
    uint256 public defaultOpenMaxOrigins;

    /// @dev Monotonic index stamped into `DefaultOpenAllowlistUpdated`.
    uint256 internal _defaultOpenUpdateIndex;

    // -----------------------------------------------------------------
    // Storage — authorized sets + pending proposals
    // -----------------------------------------------------------------

    mapping(uint256 namespaceId => EnumerableSet.AddressSet) internal _origins;

    struct PendingAssignment {
        address[] operators;
        uint64 readyAt;
    }

    mapping(uint256 namespaceId => PendingAssignment) internal _pending;

    // -----------------------------------------------------------------
    // Events (ADR 011 § Contract: OriginAssignment)
    // -----------------------------------------------------------------

    event AssignmentProposed(
        uint256 indexed namespaceId, address indexed proposer, address[] operators, uint256 readyAt
    );
    event AssignmentProposalCancelled(uint256 indexed namespaceId, address indexed proposer, bool autoCleared);
    event AssignmentActivated(uint256 indexed namespaceId, address[] operators);
    event AssignmentRevoked(uint256 indexed namespaceId, address indexed operator, address indexed by);
    event BlacklistedAssignmentPruned(uint256 indexed namespaceId, address indexed operator, address indexed pruner);
    event DefaultOpenAllowlistUpdated(address[] operators, uint256 indexed updateIndex);
    event DefaultOpenOperatorAdded(address indexed operator);
    event DefaultOpenOperatorRemoved(address indexed operator);
    event ContentBlacklistUpdated(address indexed oldAddr, address indexed newAddr);
    event MaxOriginsPerNamespaceUpdated(uint256 oldValue, uint256 newValue);
    event AssignmentTimelockUpdated(uint256 oldValue, uint256 newValue);
    event DefaultOpenMaxOriginsUpdated(uint256 oldValue, uint256 newValue);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error NotNamespaceOwner(uint256 namespaceId, address caller);
    error EmptyOperatorSet();
    error TooManyOrigins(uint256 count, uint256 cap);
    error DuplicateOperator(address operator);
    error OperatorNotActive(address operator);
    error OperatorBlacklisted(address operator);
    error NoPendingProposal(uint256 namespaceId);
    error TimelockNotElapsed(uint256 readyAt);
    error NotAuthorizedOrigin(uint256 namespaceId, address operator);
    error ContentBlacklistNotSet();
    error OperatorNotBlacklisted(address operator);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);

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
        assignmentTimelock = DEFAULT_ASSIGNMENT_TIMELOCK;
        defaultOpenMaxOrigins = DEFAULT_OPEN_MAX_ORIGINS;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    // -----------------------------------------------------------------
    // Registered-namespace assignment (publisher propose / DAO ratify)
    // -----------------------------------------------------------------

    /// @notice Publisher proposes a candidate origin set for their namespace.
    /// @dev External `ownerOf` / `isActive` reads precede the pending-state write;
    ///      safe under `nonReentrant` (the reads are views).
    // slither-disable-next-line reentrancy-no-eth
    function proposeAssignment(uint256 namespaceId, address[] calldata operators) external nonReentrant {
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (publisherRegistry.ownerOf(namespaceId) != msg.sender) revert NotNamespaceOwner(namespaceId, msg.sender);
        if (operators.length == 0) revert EmptyOperatorSet();
        if (operators.length > maxOriginsPerNamespace) revert TooManyOrigins(operators.length, maxOriginsPerNamespace);

        for (uint256 i = 0; i < operators.length; i++) {
            // aderyn-ignore-next-line(reentrancy-state-change)
            if (!capacityBond.isActive(operators[i])) revert OperatorNotActive(operators[i]);
            for (uint256 j = 0; j < i; j++) {
                if (operators[i] == operators[j]) revert DuplicateOperator(operators[i]);
            }
        }

        // Overwrite any existing pending proposal (ADR 011 § Edge cases).
        if (_pending[namespaceId].readyAt != 0) {
            emit AssignmentProposalCancelled(namespaceId, msg.sender, true);
        }

        uint64 readyAt = uint64(block.timestamp + assignmentTimelock);
        _pending[namespaceId] = PendingAssignment({ operators: operators, readyAt: readyAt });

        emit AssignmentProposed(namespaceId, msg.sender, operators, readyAt);
    }

    /// @notice Governance ratifies a pending proposal after the timelock, after
    ///         re-validating each operator is still active and not blacklisted.
    /// @dev Reverts if an operator went stale during the window (ADR 011 § Lifecycle
    ///      step 2); the publisher then re-proposes — `proposeAssignment` overwrites
    ///      the stale pending set (the documented "auto-clear" effect; an in-revert
    ///      state clear is impossible, so the overwrite path realizes it).
    // slither-disable-next-line reentrancy-no-eth
    function activateAssignment(uint256 namespaceId) external nonReentrant onlyRole(GOVERNANCE_ROLE) {
        PendingAssignment storage pending = _pending[namespaceId];
        if (pending.readyAt == 0) revert NoPendingProposal(namespaceId);
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < pending.readyAt) revert TimelockNotElapsed(pending.readyAt);

        address[] memory operators = pending.operators;
        address blacklist = contentBlacklist;
        for (uint256 i = 0; i < operators.length; i++) {
            // aderyn-ignore-next-line(reentrancy-state-change)
            if (!capacityBond.isActive(operators[i])) revert OperatorNotActive(operators[i]);
            // aderyn-ignore-next-line(reentrancy-state-change)
            if (blacklist != address(0) && IContentBlacklistOriginView(blacklist).isOriginBlacklisted(operators[i])) {
                revert OperatorBlacklisted(operators[i]);
            }
        }

        _replaceSet(namespaceId, operators);
        delete _pending[namespaceId];

        emit AssignmentActivated(namespaceId, operators);
    }

    /// @notice Publisher cancels their own pending proposal before activation.
    function cancelAssignmentProposal(uint256 namespaceId) external nonReentrant {
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (publisherRegistry.ownerOf(namespaceId) != msg.sender) revert NotNamespaceOwner(namespaceId, msg.sender);
        // The `ownerOf` external read taints the struct field for slither's
        // strict-equality detector; the `== 0` sentinel is a presence check.
        // slither-disable-next-line incorrect-equality
        if (_pending[namespaceId].readyAt == 0) revert NoPendingProposal(namespaceId);
        delete _pending[namespaceId];
        emit AssignmentProposalCancelled(namespaceId, msg.sender, false);
    }

    /// @notice Publisher (own namespace) or governance (any) removes one operator.
    // slither-disable-next-line reentrancy-no-eth
    function revokeAssignment(uint256 namespaceId, address operator) external nonReentrant {
        if (!hasRole(GOVERNANCE_ROLE, msg.sender)) {
            // aderyn-ignore-next-line(reentrancy-state-change)
            if (publisherRegistry.ownerOf(namespaceId) != msg.sender) {
                revert NotNamespaceOwner(namespaceId, msg.sender);
            }
        }
        if (!_origins[namespaceId].remove(operator)) revert NotAuthorizedOrigin(namespaceId, operator);
        emit AssignmentRevoked(namespaceId, operator, msg.sender);
    }

    /// @notice Permissionless: remove a blacklisted operator from a namespace's set
    ///         (works for `namespaceId == 0`). The contract checks the blacklist
    ///         itself, so a caller cannot grief by naming a non-blacklisted operator.
    // slither-disable-next-line reentrancy-no-eth
    function pruneBlacklistedAssignment(uint256 namespaceId, address operator) external nonReentrant {
        address blacklist = contentBlacklist;
        if (blacklist == address(0)) revert ContentBlacklistNotSet();
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (!IContentBlacklistOriginView(blacklist).isOriginBlacklisted(operator)) {
            revert OperatorNotBlacklisted(operator);
        }
        if (!_origins[namespaceId].remove(operator)) revert NotAuthorizedOrigin(namespaceId, operator);
        emit BlacklistedAssignmentPruned(namespaceId, operator, msg.sender);
    }

    // -----------------------------------------------------------------
    // Default-open allow-list (namespaceId == 0; GOVERNANCE_ROLE)
    // -----------------------------------------------------------------

    /// @notice Replace the default-open allow-list atomically.
    // slither-disable-next-line reentrancy-no-eth
    function setDefaultOpenAllowlist(address[] calldata operators) external nonReentrant onlyRole(GOVERNANCE_ROLE) {
        if (operators.length > defaultOpenMaxOrigins) revert TooManyOrigins(operators.length, defaultOpenMaxOrigins);
        for (uint256 i = 0; i < operators.length; i++) {
            // aderyn-ignore-next-line(reentrancy-state-change)
            if (!capacityBond.isActive(operators[i])) revert OperatorNotActive(operators[i]);
        }
        _replaceSet(DEFAULT_OPEN_NAMESPACE, operators);
        emit DefaultOpenAllowlistUpdated(operators, ++_defaultOpenUpdateIndex);
    }

    /// @notice Add one operator to the default-open allow-list.
    // slither-disable-next-line reentrancy-no-eth
    function addDefaultOpenOperator(address operator) external nonReentrant onlyRole(GOVERNANCE_ROLE) {
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (!capacityBond.isActive(operator)) revert OperatorNotActive(operator);
        EnumerableSet.AddressSet storage set = _origins[DEFAULT_OPEN_NAMESPACE];
        if (set.length() + 1 > defaultOpenMaxOrigins) revert TooManyOrigins(set.length() + 1, defaultOpenMaxOrigins);
        if (!set.add(operator)) revert DuplicateOperator(operator);
        emit DefaultOpenOperatorAdded(operator);
    }

    /// @notice Remove one operator from the default-open allow-list.
    function removeDefaultOpenOperator(address operator) external onlyRole(GOVERNANCE_ROLE) {
        if (!_origins[DEFAULT_OPEN_NAMESPACE].remove(operator)) {
            revert NotAuthorizedOrigin(DEFAULT_OPEN_NAMESPACE, operator);
        }
        emit DefaultOpenOperatorRemoved(operator);
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    /// @notice Wire (or re-point) the ContentBlacklist read direction. Rejecting
    ///         `address(0)` prevents regressing into the deployment-window state
    ///         where activation skips the blacklist check (ADR 011 § Edge cases).
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

    function setAssignmentTimelock(uint256 secondsDelay) external onlyRole(GOVERNANCE_ROLE) {
        if (secondsDelay < ASSIGNMENT_TIMELOCK_FLOOR || secondsDelay > ASSIGNMENT_TIMELOCK_CEILING) {
            revert ParamOutOfBounds(secondsDelay, ASSIGNMENT_TIMELOCK_FLOOR, ASSIGNMENT_TIMELOCK_CEILING);
        }
        uint256 old = assignmentTimelock;
        assignmentTimelock = secondsDelay;
        emit AssignmentTimelockUpdated(old, secondsDelay);
    }

    function setDefaultOpenMaxOrigins(uint256 cap) external onlyRole(GOVERNANCE_ROLE) {
        if (cap < DEFAULT_OPEN_MAX_FLOOR || cap > DEFAULT_OPEN_MAX_CEILING) {
            revert ParamOutOfBounds(cap, DEFAULT_OPEN_MAX_FLOOR, DEFAULT_OPEN_MAX_CEILING);
        }
        uint256 old = defaultOpenMaxOrigins;
        defaultOpenMaxOrigins = cap;
        emit DefaultOpenMaxOriginsUpdated(old, cap);
    }

    // -----------------------------------------------------------------
    // Views
    // -----------------------------------------------------------------

    /// @notice Strict set membership; `namespaceId == 0` reads the default-open
    ///         allow-list. Operator-level blacklist status is NOT consulted here —
    ///         off-chain consumers filter against `ContentBlacklist`.
    function isAuthorizedOrigin(uint256 namespaceId, address operator) external view returns (bool) {
        return _origins[namespaceId].contains(operator);
    }

    function getOrigins(uint256 namespaceId) external view returns (address[] memory) {
        return _origins[namespaceId].values();
    }

    function getPendingAssignment(uint256 namespaceId)
        external
        view
        returns (address[] memory operators, uint256 readyAt)
    {
        PendingAssignment storage pending = _pending[namespaceId];
        return (pending.operators, pending.readyAt);
    }

    // -----------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------

    /// @dev Replace a namespace's authorized set with `operators` (assumed
    ///      pre-validated for activity). Reverts `DuplicateOperator` if the input
    ///      contains a repeat (the second `add` returns false).
    function _replaceSet(uint256 namespaceId, address[] memory operators) internal {
        EnumerableSet.AddressSet storage set = _origins[namespaceId];
        address[] memory current = set.values();
        for (uint256 i = 0; i < current.length; i++) {
            // Clearing the set; the bool return (was-present) is irrelevant here.
            // slither-disable-next-line unused-return
            set.remove(current[i]);
        }
        for (uint256 i = 0; i < operators.length; i++) {
            if (!set.add(operators[i])) revert DuplicateOperator(operators[i]);
        }
    }
}
