// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { EnumerableSet } from "@openzeppelin/contracts/utils/structs/EnumerableSet.sol";

import { ICapacityBondActivity } from "./interfaces/ICapacityBondActivity.sol";
import { IPublisherRegistryOwnership } from "./interfaces/IPublisherRegistryOwnership.sol";
import { IContentBlacklistOriginView } from "./interfaces/IContentBlacklistOriginView.sol";
import { IVettingPolicy } from "./interfaces/IVettingPolicy.sol";

/// @title OriginAssignment
/// @notice The DAO's positive origin authority (ADR 011 § Origin Assignment
///         Authority), split into two planes:
///         - **Vetting (cold).** Whether a publisher wallet may seat origins is
///           decided by a swappable `IVettingPolicy`. Governance re-points it
///           with `setVettingPolicy` to move between vetting procedures (manual
///           approval, a timelocked governance grant, an on-chain attestation,
///           or no gate) without changing this contract. `addOrigin` asks the
///           installed policy `isVetted(msg.sender)` and nothing more.
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

    uint256 internal constant MAX_ORIGINS_FLOOR = 1;
    uint256 internal constant MAX_ORIGINS_CEILING = 50;

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

    /// @notice The swappable vetting authority. `addOrigin` seats an origin only
    ///         when `vettingPolicy.isVetted(msg.sender)` is true. Governance
    ///         re-points it with `setVettingPolicy` to change the vetting
    ///         procedure without changing this contract. Always a concrete
    ///         contract — never `address(0)` (rejected in the constructor and the
    ///         setter), so `addOrigin` needs no zero branch and fails closed.
    IVettingPolicy public vettingPolicy;

    // -----------------------------------------------------------------
    // Storage — authorized sets
    // -----------------------------------------------------------------

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

    event OriginAdded(uint256 indexed namespaceId, address indexed operator, address indexed by);
    event OriginRemoved(uint256 indexed namespaceId, address indexed operator, address indexed by);
    event BlacklistedOriginPruned(uint256 indexed namespaceId, address indexed operator, address indexed pruner);
    event ContentBlacklistUpdated(address indexed oldAddr, address indexed newAddr);
    event MaxOriginsPerNamespaceUpdated(uint256 oldValue, uint256 newValue);
    event VettingPolicyUpdated(address indexed oldPolicy, address indexed newPolicy);

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
    /// @notice The vetting policy address holds no code. `addOrigin` would revert
    ///         on the `isVetted` ABI decode, bricking seating with an opaque
    ///         low-level failure — so the constructor and setter reject it loudly.
    error VettingPolicyNotAContract(address policy);

    // -----------------------------------------------------------------
    // Constructor (ADR 016 § Deployment Order, step 10)
    // -----------------------------------------------------------------

    /// @param capacityBond_       Operator activity source.
    /// @param publisherRegistry_  Namespace ownership source.
    /// @param contentBlacklist_   May be `address(0)` at deploy; bound later via
    ///                            `setContentBlacklist` (ADR 016 post-deploy step 2).
    /// @param vettingPolicy_      Installed vetting authority. A concrete policy
    ///                            exists from genesis — no `address(0)` phase.
    /// @param admin               `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE` holder.
    constructor(
        ICapacityBondActivity capacityBond_,
        IPublisherRegistryOwnership publisherRegistry_,
        address contentBlacklist_,
        IVettingPolicy vettingPolicy_,
        address admin
    ) {
        if (
            address(capacityBond_) == address(0) || address(publisherRegistry_) == address(0)
                || address(vettingPolicy_) == address(0) || admin == address(0)
        ) {
            revert ZeroAddress();
        }
        // The policy must be a contract: `addOrigin` calls `isVetted` on it, and a
        // non-contract address would revert on the ABI decode instead of failing
        // here. `capacityBond_` / `publisherRegistry_` are likewise called, but
        // their own constructors already ran, so only the policy needs the guard.
        if (address(vettingPolicy_).code.length == 0) revert VettingPolicyNotAContract(address(vettingPolicy_));
        capacityBond = capacityBond_;
        publisherRegistry = publisherRegistry_;
        contentBlacklist = contentBlacklist_; // may be zero at deploy
        vettingPolicy = vettingPolicy_;

        maxOriginsPerNamespace = DEFAULT_MAX_ORIGINS;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
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
        // Vetting first: it is the blocking prerequisite an unvetted caller must
        // act on, so checking it before the `ownerOf` read reports the actionable
        // error. The policy read is a view STATICCALL; a reverting (misconfigured)
        // policy fails the call closed, which is the intended posture.
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (!vettingPolicy.isVetted(msg.sender)) revert PublisherNotVetted(msg.sender);
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (publisherRegistry.ownerOf(namespaceId) != msg.sender) revert NotNamespaceOwner(namespaceId, msg.sender);
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (!capacityBond.isActive(operator)) revert OperatorNotActive(operator);

        address blacklist = contentBlacklist;
        // aderyn-ignore-next-line(reentrancy-state-change)
        if (blacklist != address(0) && _isBlacklisted(blacklist, operator)) {
            revert OperatorBlacklisted(operator);
        }

        // Duplicate BEFORE cap: re-adding an operator that is already seated is a
        // caller error whatever the set size, and reporting `TooManyOrigins` for
        // it would name a count the set never reaches.
        // Both guards read before writing. Reacting to `add`'s return value
        // instead would make a doomed at-cap call pay for two SSTOREs it then
        // throws away, because a revert refunds only the gas left, not the gas
        // already spent.
        EnumerableSet.AddressSet storage set = _origins[namespaceId];
        if (set.contains(operator)) revert DuplicateOperator(operator);
        // The cap binds ADDS ONLY, so it is not a set-wide invariant: lowering
        // `maxOriginsPerNamespace` leaves larger existing sets in place rather
        // than evicting from them.
        uint256 seated = set.length();
        if (seated >= maxOriginsPerNamespace) revert TooManyOrigins(seated + 1, maxOriginsPerNamespace);
        // Necessarily true: the `contains` guard above already rejected a repeat.
        // slither-disable-next-line unused-return
        set.add(operator);
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

    /// @notice Install a different vetting policy, changing the vetting procedure
    ///         without touching this contract. Rejecting `address(0)` keeps
    ///         `addOrigin` branch-free and fail-closed — "deny everyone" is a
    ///         policy whose `isVetted` returns false, not a null policy. The
    ///         policy must be a contract: an EOA (or not-yet-deployed address)
    ///         would make `addOrigin`'s `isVetted` call revert on ABI decode, so
    ///         a misconfiguration fails loudly here instead of bricking seating.
    function setVettingPolicy(IVettingPolicy newPolicy) external onlyRole(GOVERNANCE_ROLE) {
        if (address(newPolicy) == address(0)) revert ZeroAddress();
        if (address(newPolicy).code.length == 0) revert VettingPolicyNotAContract(address(newPolicy));
        address old = address(vettingPolicy);
        vettingPolicy = newPolicy;
        emit VettingPolicyUpdated(old, address(newPolicy));
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

    /// @notice Whether `publisher` may currently seat origins, per the installed
    ///         policy. A passthrough so callers reading vetting status need not
    ///         know the policy address.
    function isVettedPublisher(address publisher) external view returns (bool) {
        return vettingPolicy.isVetted(publisher);
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
