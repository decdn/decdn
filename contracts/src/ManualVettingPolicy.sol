// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";

import { IVettingPolicy } from "./interfaces/IVettingPolicy.sol";
import { IVettingRequestable } from "./interfaces/IVettingRequestable.sol";

/// @title ManualVettingPolicy — the genesis vetting policy (ADR 011)
/// @notice A `VETTER_ROLE` holder approves publishers directly. Two roles keep
///         two decisions orthogonal:
///         - `GOVERNANCE_ROLE` is the admin of `VETTER_ROLE`, so it decides
///           *who* may vet. It hands off to the Timelock like every other
///           governed target.
///         - `VETTER_ROLE` performs the vetting. It is granted to whoever should
///           hold that authority — an operator EOA on the initial testnet (fast
///           approvals, no 48-hour Timelock delay), and later a multisig, Safe,
///           or `DecdnGovernor` — reassigned by governance with no contract swap.
///
/// @dev    `VETTER_ROLE` is not a back door: its admin is `GOVERNANCE_ROLE`
///         (the Timelock post-handoff), so governance revokes or re-points it at
///         will, and it confers only publisher-vetting — no funds, no
///         parameters. The vetting *procedure* is changed instead by swapping
///         the whole policy via `OriginAssignment.setVettingPolicy`; this
///         contract only varies *who* vets under the manual procedure.
///
///         Holds no funds and makes no external calls, so no `ReentrancyGuard`.
contract ManualVettingPolicy is AccessControl, IVettingPolicy, IVettingRequestable {
    /// @notice Admin of `VETTER_ROLE` — decides who may vet. Timelock post-handoff.
    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");

    /// @notice May call `setVetted`. Held by whatever governance appoints.
    bytes32 public constant VETTER_ROLE = keccak256("VETTER_ROLE");

    mapping(address publisher => bool) private _vetted;

    event PublisherVetted(address indexed publisher, bool vetted, address indexed by);

    error ZeroAddress();

    /// @param admin         `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE` holder
    ///                      (Timelock at mainnet per ADR 016 § Deployment Order).
    /// @param initialVetter Seeded `VETTER_ROLE` holder, or `address(0)` to leave
    ///                      the role unfilled until governance grants it.
    constructor(address admin, address initialVetter) {
        if (admin == address(0)) revert ZeroAddress();
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
        // Governance (not the default admin) manages the vetter set.
        _setRoleAdmin(VETTER_ROLE, GOVERNANCE_ROLE);
        if (initialVetter != address(0)) _grantRole(VETTER_ROLE, initialVetter);
    }

    /// @notice Vet or un-vet `publisher`. Un-vetting stops NEW origins; origins
    ///         the publisher already seated stay until removed on
    ///         `OriginAssignment` (same bounded-work reasoning as the origin set).
    function setVetted(address publisher, bool vetted) external onlyRole(VETTER_ROLE) {
        _vetted[publisher] = vetted;
        emit PublisherVetted(publisher, vetted, msg.sender);
    }

    /// @inheritdoc IVettingPolicy
    function isVetted(address publisher) external view returns (bool) {
        return _vetted[publisher];
    }

    /// @inheritdoc IVettingRequestable
    function requestVetting() external pure {
        revert VettingRequestUnsupported("vetting is granted by a VETTER_ROLE holder, not self-service");
    }
}
