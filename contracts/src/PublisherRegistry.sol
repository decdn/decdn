// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { SafeCast } from "@openzeppelin/contracts/utils/math/SafeCast.sol";

/// @title PublisherRegistry — namespace ownership and lifecycle (ADR 002)
/// @notice Permissionless namespace creation (publisher identity is implicit
///         on first `createNamespace`) and a 2-step namespace ownership
///         transfer (offer, then accept). The registry records who owns each namespace and
///         stores no content hashes — the hash→namespace association is supplied
///         off-chain by the requester at fetch time (ADR 002 §
///         Hash-to-namespace association). Spec: ADR 002 § Contract:
///         PublisherRegistry.
///
/// @dev    OZ composition: `AccessControl` only — the one governable
///         parameter (`maxNamespacesPerPublisher`) carries `GOVERNANCE_ROLE`,
///         held by the Timelock post-deploy.
///
///         No `ReentrancyGuard`: the contract makes no external calls and
///         holds no funds — every function is pure storage bookkeeping, so a
///         reentrancy guard would be dead weight. (ADR 016's inventory row is
///         updated to match.) No `Pausable`: namespace creation is
///         permissionless by design.
///
///         `namespaceId == 0` is reserved for content published without a
///         namespace (ADR 002 § Namespace 0) and is never assigned — the
///         counter starts at 1.
contract PublisherRegistry is AccessControl {
    using SafeCast for uint256;

    /// @notice Setter authority for the governable namespace cap. Held by
    ///         `TimelockController` post-deploy.
    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");

    // Governable-parameter defaults + bounds (ADR 002 / ADR 009).
    uint64 internal constant DEFAULT_MAX_NAMESPACES = 100;
    uint256 internal constant MAX_NAMESPACES_FLOOR = 1;
    uint256 internal constant MAX_NAMESPACES_CEILING = 1000;

    /// @notice Owner of each namespace. `address(0)` for unassigned ids
    ///         (including the reserved no-namespace id 0).
    mapping(uint256 namespaceId => address) public ownerOf;

    /// @notice Number of namespaces a publisher currently owns.
    mapping(address publisher => uint256) public namespaceCount;

    /// @notice Address a namespace is offered to, which takes it with
    ///         `acceptNamespace`. `address(0)` if no offer is open.
    mapping(uint256 namespaceId => address) public pendingOwnerOf;

    // The two fields below are both uint64 and declared consecutively so they
    // pack into a single storage slot. `createNamespace` reads the cap and
    // increments `_nextNamespaceId`, so each call warms one slot.

    /// @notice Per-publisher namespace cap (anti-squatting). Default 100,
    ///         bounded `[1, 1000]` (well within uint64).
    uint64 public maxNamespacesPerPublisher;

    /// @dev Monotonic namespace id allocator. Pre-incremented so the first
    ///      assigned id is 1, leaving 0 as the reserved no-namespace id.
    uint64 internal _nextNamespaceId;

    event NamespaceCreated(uint256 indexed namespaceId, address indexed owner);
    /// @dev `to == address(0)` withdraws the open offer.
    event NamespaceTransferStarted(uint256 indexed namespaceId, address indexed from, address indexed to);
    event NamespaceTransferred(uint256 indexed namespaceId, address indexed from, address indexed to);
    event MaxNamespacesPerPublisherUpdated(uint256 oldValue, uint256 newValue);

    error ZeroAddress();
    error NamespaceCapReached(uint256 cap);
    error NotNamespaceOwner(uint256 namespaceId, address caller);
    error NotPendingOwner(uint256 namespaceId, address caller);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);

    /// @param admin Initial `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE` holder
    ///              (Timelock at mainnet per ADR 016 § Deployment Order).
    ///              The governable namespace cap starts at its ADR 002
    ///              default; no economic constructor args (ADR 016 step 8).
    constructor(address admin) {
        if (admin == address(0)) revert ZeroAddress();
        maxNamespacesPerPublisher = DEFAULT_MAX_NAMESPACES;
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    // -----------------------------------------------------------------
    // Namespaces
    // -----------------------------------------------------------------

    /// @notice Create a new namespace owned by the caller. Permissionless;
    ///         the first call from an address implicitly makes it a publisher.
    /// @return namespaceId The newly assigned id (>= 1).
    function createNamespace() external returns (uint256 namespaceId) {
        if (namespaceCount[msg.sender] >= maxNamespacesPerPublisher) {
            revert NamespaceCapReached(maxNamespacesPerPublisher);
        }
        namespaceId = ++_nextNamespaceId; // uint64, widened to the uint256 return
        ownerOf[namespaceId] = msg.sender;
        namespaceCount[msg.sender] += 1;
        emit NamespaceCreated(namespaceId, msg.sender);
    }

    /// @notice Offer `namespaceId` to `newOwner`, who takes it with
    ///         `acceptNamespace`. A new offer replaces the pending one, and
    ///         `newOwner == address(0)` withdraws it. Ownership moves the
    ///         moment the offer is accepted (ADR 002 § Namespace transfer).
    function transferNamespace(uint256 namespaceId, address newOwner) external {
        _requireOwner(namespaceId);
        pendingOwnerOf[namespaceId] = newOwner;
        emit NamespaceTransferStarted(namespaceId, msg.sender, newOwner);
    }

    /// @notice Take ownership of `namespaceId`. Callable only by its pending
    ///         owner, so an offer to a mistyped address never completes.
    function acceptNamespace(uint256 namespaceId) external {
        // An unset pending owner is address(0), which no caller matches.
        if (msg.sender != pendingOwnerOf[namespaceId]) revert NotPendingOwner(namespaceId, msg.sender);

        address from = ownerOf[namespaceId];
        // Enforce the anti-squatting cap on the receiving side too — otherwise
        // it could be bypassed by creating namespaces under throwaway addresses
        // and transferring them to one publisher. Self-transfers (from ==
        // newOwner) leave the count unchanged, so they're exempt.
        if (from != msg.sender && namespaceCount[msg.sender] >= maxNamespacesPerPublisher) {
            revert NamespaceCapReached(maxNamespacesPerPublisher);
        }
        ownerOf[namespaceId] = msg.sender;
        namespaceCount[from] -= 1;
        namespaceCount[msg.sender] += 1;
        delete pendingOwnerOf[namespaceId];

        emit NamespaceTransferred(namespaceId, from, msg.sender);
    }

    // -----------------------------------------------------------------
    // Governable setters
    // -----------------------------------------------------------------

    function setMaxNamespacesPerPublisher(uint256 newMax) external onlyRole(GOVERNANCE_ROLE) {
        if (newMax < MAX_NAMESPACES_FLOOR || newMax > MAX_NAMESPACES_CEILING) {
            revert ParamOutOfBounds(newMax, MAX_NAMESPACES_FLOOR, MAX_NAMESPACES_CEILING);
        }
        uint256 old = maxNamespacesPerPublisher;
        // newMax <= 1000 after the bound check, so the cast never reverts.
        maxNamespacesPerPublisher = newMax.toUint64();
        emit MaxNamespacesPerPublisherUpdated(old, newMax);
    }

    // -----------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------

    function _requireOwner(uint256 namespaceId) internal view {
        if (ownerOf[namespaceId] != msg.sender) revert NotNamespaceOwner(namespaceId, msg.sender);
    }
}
