// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";

import { IVettingPolicy } from "./interfaces/IVettingPolicy.sol";
import { IVettingRequestable } from "./interfaces/IVettingRequestable.sol";
import { IPublisherRegistryOwnership } from "./interfaces/IPublisherRegistryOwnership.sol";

/// @title TimelockedGovernanceVettingPolicy (ADR 011)
/// @notice A publisher requests vetting, waits `vettingTimelock` (the window
///         governance reviews it in), and governance grants it — or governance
///         grants/revokes instantly with `setPublisherVetted`. This is the
///         production-shaped alternative to `ManualVettingPolicy`: the same
///         request → wait → grant cold plane the network shipped originally, now
///         a self-contained policy behind the `IVettingPolicy` seam. Governance
///         installs it via `OriginAssignment.setVettingPolicy`.
///
/// @dev    `requestVetting` requires the caller to own a namespace, so the
///         policy reads `PublisherRegistry.namespaceCount`. Holds no funds; no
///         `ReentrancyGuard` because the one external read is a view and precedes
///         no state it could be re-entered against.
contract TimelockedGovernanceVettingPolicy is AccessControl, IVettingPolicy, IVettingRequestable {
    /// @notice Grants ripened requests and sets the timelock. Timelock post-handoff.
    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");

    // Governable-parameter default + bounds (ADR 009 § Governable parameters).
    uint256 internal constant VETTING_TIMELOCK_FLOOR = 24 hours;
    uint256 internal constant VETTING_TIMELOCK_CEILING = 14 days;
    uint256 internal constant DEFAULT_VETTING_TIMELOCK = 3 days;

    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IPublisherRegistryOwnership public immutable publisherRegistry;

    /// @notice Delay between a publisher's request and the earliest grant.
    ///         Default 3 days, bounded `[24h, 14d]`.
    uint256 public vettingTimelock;

    /// @inheritdoc IVettingPolicy
    /// @dev Public mapping getter satisfies `IVettingPolicy.isVetted(address)`.
    mapping(address publisher => bool) public isVetted;

    /// @notice Unix time each pending request ripens. `0` means none is pending.
    mapping(address publisher => uint64) internal _vettingReadyAt;

    event VettingRequested(address indexed publisher, uint256 readyAt);
    /// @notice The publisher withdrew its OWN pending request. Governance never
    ///         emits this: `grantVetting` and `setPublisherVetted` both clear any
    ///         pending request and announce it with `PublisherVetted`, so a
    ///         consumer closes a pending entry on EITHER event.
    event VettingRequestCancelled(address indexed publisher);
    event PublisherVetted(address indexed publisher, bool vetted, address indexed by);
    event VettingTimelockUpdated(uint256 oldValue, uint256 newValue);

    error ZeroAddress();
    error NoNamespaceOwned(address publisher);
    error AlreadyVetted(address publisher);
    error VettingRequestPending(address publisher);
    error NoVettingRequest(address publisher);
    error VettingTimelockNotElapsed(uint256 readyAt);
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);

    /// @param registry_ Namespace ownership source (entry condition for a request).
    /// @param admin     `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE` holder (Timelock
    ///                  at mainnet per ADR 016 § Deployment Order).
    constructor(IPublisherRegistryOwnership registry_, address admin) {
        if (address(registry_) == address(0) || admin == address(0)) revert ZeroAddress();
        publisherRegistry = registry_;
        vettingTimelock = DEFAULT_VETTING_TIMELOCK;
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    /// @inheritdoc IVettingRequestable
    /// @notice A publisher asks governance to vet its wallet. The request ripens
    ///         after `vettingTimelock`.
    function requestVetting() external {
        if (isVetted[msg.sender]) revert AlreadyVetted(msg.sender);
        if (_vettingReadyAt[msg.sender] != 0) revert VettingRequestPending(msg.sender);
        if (publisherRegistry.namespaceCount(msg.sender) == 0) revert NoNamespaceOwned(msg.sender);

        // `vettingTimelock` is bounded to [24h, 14d], so the sum cannot approach
        // 2^64; the floor also makes `readyAt` strictly positive, which is what
        // lets `0` serve as the "no request pending" sentinel.
        uint64 readyAt = uint64(block.timestamp + vettingTimelock);
        _vettingReadyAt[msg.sender] = readyAt;

        emit VettingRequested(msg.sender, readyAt);
    }

    /// @notice A publisher withdraws its own pending request.
    function cancelVettingRequest() external {
        // slither-disable-next-line incorrect-equality
        if (_vettingReadyAt[msg.sender] == 0) revert NoVettingRequest(msg.sender);
        delete _vettingReadyAt[msg.sender];
        emit VettingRequestCancelled(msg.sender);
    }

    /// @notice Governance grants a ripened request. The only place the timelock
    ///         is enforced; `setPublisherVetted` is the instant override.
    function grantVetting(address publisher) external onlyRole(GOVERNANCE_ROLE) {
        uint64 readyAt = _vettingReadyAt[publisher];
        if (readyAt == 0) revert NoVettingRequest(publisher);
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < readyAt) revert VettingTimelockNotElapsed(readyAt);

        delete _vettingReadyAt[publisher];
        isVetted[publisher] = true;

        emit PublisherVetted(publisher, true, msg.sender);
    }

    /// @notice Governance override: vet a publisher with no wait, or un-vet a
    ///         rogue one. A grant consumes any pending request; a revocation
    ///         clears it too, so a ripened request cannot walk straight back in.
    function setPublisherVetted(address publisher, bool vetted) external onlyRole(GOVERNANCE_ROLE) {
        if (publisher == address(0)) revert ZeroAddress();
        delete _vettingReadyAt[publisher];
        isVetted[publisher] = vetted;
        emit PublisherVetted(publisher, vetted, msg.sender);
    }

    function setVettingTimelock(uint256 secondsDelay) external onlyRole(GOVERNANCE_ROLE) {
        if (secondsDelay < VETTING_TIMELOCK_FLOOR || secondsDelay > VETTING_TIMELOCK_CEILING) {
            revert ParamOutOfBounds(secondsDelay, VETTING_TIMELOCK_FLOOR, VETTING_TIMELOCK_CEILING);
        }
        uint256 old = vettingTimelock;
        vettingTimelock = secondsDelay;
        emit VettingTimelockUpdated(old, secondsDelay);
    }

    /// @notice Unix time `publisher`'s pending request ripens, or `0` when none.
    function getPendingVetting(address publisher) external view returns (uint256 readyAt) {
        return _vettingReadyAt[publisher];
    }
}
