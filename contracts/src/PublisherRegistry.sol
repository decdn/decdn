// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { SafeCast } from "@openzeppelin/contracts/utils/math/SafeCast.sol";

/// @title PublisherRegistry — namespaces and content claims (ADR 002)
/// @notice Permissionless namespace creation (publisher identity is implicit
///         on first `createNamespace`), append-only content claims binding
///         `(namespaceId, blake3Hash)`, and a timelocked 2-step namespace
///         ownership transfer. Spec: ADR 002 § Contract: PublisherRegistry.
///
/// @dev    OZ composition: `AccessControl` only — the two governable
///         parameters (`maxNamespacesPerPublisher`, `namespaceTransferTimelock`)
///         carry `GOVERNANCE_ROLE`, held by the Timelock post-deploy.
///
///         No `ReentrancyGuard`: the contract makes no external calls and
///         holds no funds — every function is pure storage bookkeeping, so a
///         reentrancy guard would be dead weight. (ADR 016's inventory row is
///         updated to match.) No `Pausable`: namespace creation is
///         permissionless by design (ADR 002 § Default-open namespace).
///
///         `namespaceId == 0` is reserved as the default-open namespace
///         (ADR 002 § Default-open namespace) and is never assigned — the
///         counter starts at 1. Claims are content-immutable and append-only:
///         once `(namespaceId, hash)` is recorded it is never moved or
///         revoked, and multiple non-zero namespaces may claim the same hash
///         independently (ADR 002 § Multi-claim semantics).
contract PublisherRegistry is AccessControl {
    using SafeCast for uint256;

    /// @notice Setter authority for the two governable parameters. Held by
    ///         `TimelockController` post-deploy.
    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");

    // Governable-parameter defaults + bounds (ADR 002 / ADR 009).
    uint64 internal constant DEFAULT_MAX_NAMESPACES = 100;
    uint256 internal constant MAX_NAMESPACES_FLOOR = 1;
    uint256 internal constant MAX_NAMESPACES_CEILING = 1000;

    uint64 internal constant DEFAULT_TRANSFER_TIMELOCK = 7 days;
    uint64 internal constant TRANSFER_TIMELOCK_FLOOR = 1 days;
    uint64 internal constant TRANSFER_TIMELOCK_CEILING = 30 days;

    /// @dev Packs into a single slot: address (20 bytes) + uint64 (8 bytes).
    ///      A uint64 unix timestamp is good past the year 500-billion.
    struct PendingTransfer {
        address newOwner;
        uint64 readyAt;
    }

    /// @notice Owner of each namespace. `address(0)` for unassigned ids
    ///         (including the reserved default-open id 0).
    mapping(uint256 namespaceId => address) public ownerOf;

    /// @notice Number of namespaces a publisher currently owns.
    mapping(address publisher => uint256) public namespaceCount;

    /// @notice In-flight ownership transfer per namespace (`(0,0)` if none).
    mapping(uint256 namespaceId => PendingTransfer) public pendingTransfer;

    // The three fields below are all uint64 and declared consecutively so
    // they pack into a single storage slot (3 * 8 = 24 bytes). `_nextNamespaceId`
    // is incremented on every createNamespace and the cap is read there too, so
    // co-locating them also warms one slot per call.

    /// @notice Per-publisher namespace cap (anti-squatting). Default 100,
    ///         bounded `[1, 1000]` (well within uint64).
    uint64 public maxNamespacesPerPublisher;

    /// @notice Delay between initiating and finalizing a namespace transfer
    ///         (key-compromise mitigation). Default 7 days, bounded `[1d, 30d]`.
    uint64 public namespaceTransferTimelock;

    /// @dev Monotonic namespace id allocator. Pre-incremented so the first
    ///      assigned id is 1, leaving 0 as the reserved default-open id.
    uint64 internal _nextNamespaceId;

    /// @dev hash => set of claiming namespace ids (append-only).
    mapping(bytes32 blake3Hash => uint256[]) internal _claimingNamespaces;

    /// @dev (namespaceId, hash) => claimed, for O(1) idempotency checks.
    mapping(uint256 namespaceId => mapping(bytes32 blake3Hash => bool)) internal _hasClaimed;

    event NamespaceCreated(uint256 indexed namespaceId, address indexed owner);
    event NamespaceTransferInitiated(
        uint256 indexed namespaceId, address indexed from, address indexed to, uint256 readyAt
    );
    event NamespaceTransferred(uint256 indexed namespaceId, address indexed from, address indexed to);
    /// @dev Not in the ADR 002 interface block, but cancellation should be
    ///      observable on-chain alongside initiate/complete.
    event NamespaceTransferCancelled(uint256 indexed namespaceId, address indexed owner);
    event ContentClaimed(uint256 indexed namespaceId, bytes32 indexed blake3Hash, address indexed claimant);
    event MaxNamespacesPerPublisherUpdated(uint256 oldValue, uint256 newValue);
    event NamespaceTransferTimelockUpdated(uint64 oldValue, uint64 newValue);

    error ZeroAddress();
    error NamespaceCapReached(uint256 cap);
    error NotNamespaceOwner(uint256 namespaceId, address caller);
    error TransferToZeroAddress();
    error NoPendingTransfer(uint256 namespaceId);
    error TransferNotReady(uint256 readyAt);
    error NotPendingOwner(uint256 namespaceId, address caller);
    error AlreadyClaimed(uint256 namespaceId, bytes32 blake3Hash);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);

    /// @param admin Initial `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE` holder
    ///              (Timelock at mainnet per ADR 016 § Deployment Order).
    ///              The two governable parameters start at their ADR 002
    ///              defaults; no economic constructor args (ADR 016 step 11).
    constructor(address admin) {
        if (admin == address(0)) revert ZeroAddress();
        maxNamespacesPerPublisher = DEFAULT_MAX_NAMESPACES;
        namespaceTransferTimelock = DEFAULT_TRANSFER_TIMELOCK;
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

    /// @notice Queue a transfer of `namespaceId` to `newOwner`. Completes only
    ///         after `namespaceTransferTimelock` via `finalizeNamespaceTransfer`;
    ///         the current owner can `cancelNamespaceTransfer` during the window.
    function initiateNamespaceTransfer(uint256 namespaceId, address newOwner) external {
        _requireOwner(namespaceId);
        if (newOwner == address(0)) revert TransferToZeroAddress();
        // Fits uint64 comfortably (block.timestamp + <= 30 days); SafeCast
        // reverts on the (unreachable) overflow.
        uint64 readyAt = (block.timestamp + namespaceTransferTimelock).toUint64();
        pendingTransfer[namespaceId] = PendingTransfer({ newOwner: newOwner, readyAt: readyAt });
        emit NamespaceTransferInitiated(namespaceId, msg.sender, newOwner, readyAt);
    }

    /// @notice Complete a queued transfer once the timelock has elapsed.
    ///         Callable only by the pending recipient (explicit acceptance).
    function finalizeNamespaceTransfer(uint256 namespaceId) external {
        PendingTransfer memory pending = pendingTransfer[namespaceId];
        if (pending.newOwner == address(0)) revert NoPendingTransfer(namespaceId);
        if (msg.sender != pending.newOwner) revert NotPendingOwner(namespaceId, msg.sender);
        // Transfer timelock is days-scale; validator timestamp skew (seconds)
        // is immaterial here.
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < pending.readyAt) revert TransferNotReady(pending.readyAt);

        address from = ownerOf[namespaceId];
        // Enforce the anti-squatting cap on the receiving side too — otherwise
        // it could be bypassed by creating namespaces under throwaway addresses
        // and transferring them to one publisher. Self-transfers (from ==
        // newOwner) leave the count unchanged, so they're exempt.
        if (from != pending.newOwner && namespaceCount[pending.newOwner] >= maxNamespacesPerPublisher) {
            revert NamespaceCapReached(maxNamespacesPerPublisher);
        }
        ownerOf[namespaceId] = pending.newOwner;
        namespaceCount[from] -= 1;
        namespaceCount[pending.newOwner] += 1;
        delete pendingTransfer[namespaceId];

        emit NamespaceTransferred(namespaceId, from, pending.newOwner);
    }

    /// @notice Abort a queued transfer. Callable only by the current owner.
    function cancelNamespaceTransfer(uint256 namespaceId) external {
        _requireOwner(namespaceId);
        if (pendingTransfer[namespaceId].newOwner == address(0)) revert NoPendingTransfer(namespaceId);
        delete pendingTransfer[namespaceId];
        emit NamespaceTransferCancelled(namespaceId, msg.sender);
    }

    // -----------------------------------------------------------------
    // Content claims
    // -----------------------------------------------------------------

    /// @notice Claim `blake3Hash` under `namespaceId` (owner only). Append-only:
    ///         reverts only if THIS namespace already claimed THIS hash; other
    ///         namespaces' claims never block (ADR 002 § Multi-claim semantics).
    function claimContent(uint256 namespaceId, bytes32 blake3Hash) external {
        _requireOwner(namespaceId);
        if (_hasClaimed[namespaceId][blake3Hash]) revert AlreadyClaimed(namespaceId, blake3Hash);
        _hasClaimed[namespaceId][blake3Hash] = true;
        _claimingNamespaces[blake3Hash].push(namespaceId);
        emit ContentClaimed(namespaceId, blake3Hash, msg.sender);
    }

    // -----------------------------------------------------------------
    // Views
    // -----------------------------------------------------------------

    /// @notice Namespaces that have claimed `blake3Hash`. Empty array if none
    ///         (default-open semantics); never reverts.
    function namespaceOf(bytes32 blake3Hash) external view returns (uint256[] memory) {
        return _claimingNamespaces[blake3Hash];
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

    function setNamespaceTransferTimelock(uint64 newTimelock) external onlyRole(GOVERNANCE_ROLE) {
        if (newTimelock < TRANSFER_TIMELOCK_FLOOR || newTimelock > TRANSFER_TIMELOCK_CEILING) {
            revert ParamOutOfBounds(newTimelock, TRANSFER_TIMELOCK_FLOOR, TRANSFER_TIMELOCK_CEILING);
        }
        uint64 old = namespaceTransferTimelock;
        namespaceTransferTimelock = newTimelock;
        emit NamespaceTransferTimelockUpdated(old, newTimelock);
    }

    // -----------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------

    function _requireOwner(uint256 namespaceId) internal view {
        if (ownerOf[namespaceId] != msg.sender) revert NotNamespaceOwner(namespaceId, msg.sender);
    }
}
