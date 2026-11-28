// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ECDSA } from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";
import { EIP712 } from "@openzeppelin/contracts/utils/cryptography/EIP712.sol";

import { IVettingPolicy } from "./interfaces/IVettingPolicy.sol";

/// @title ManualVettingPolicy — the genesis vetting policy (ADR 011)
/// @notice A `VETTER_ROLE` holder approves publishers. It approves a publisher
///         in one of two ways:
///         - It calls `setVetted` and pays the gas.
///         - It signs an EIP-712 `Vetting` message off-chain and gives it to
///           the publisher. Anyone submits it with `claimVetting`, so the
///           publisher pays the gas.
///         Two roles keep two decisions orthogonal:
///         - `GOVERNANCE_ROLE` is the admin of `VETTER_ROLE`, so it decides
///           *who* may vet. It hands off to the Timelock like every other
///           governed target.
///         - `VETTER_ROLE` performs the vetting. It is granted to whoever should
///           hold that authority — an operator EOA on the initial testnet (fast
///           approvals, no 48-hour Timelock delay), the signing key of a
///           publisher sign-up service, a multisig, a Safe, or `DecdnGovernor`
///           — reassigned by governance with no contract swap. Only an EOA
///           can sign a `Vetting` message.
///
/// @dev    `VETTER_ROLE` is not a back door: its admin is `GOVERNANCE_ROLE`
///         (the Timelock post-handoff), so governance revokes or re-points it at
///         will, and it confers only publisher-vetting — no funds, no
///         parameters. The vetting *procedure* is changed instead by swapping
///         the whole policy via `OriginAssignment.setVettingPolicy`; this
///         contract only varies *who* vets under the manual procedure.
///
///         `nonces` exists only to void outstanding signatures on an un-vet.
///         A `Vetting` signature has no expiry: revoking the signer's role or
///         un-vetting the publisher voids it.
///
///         Holds no funds and makes no external calls, so no `ReentrancyGuard`.
contract ManualVettingPolicy is AccessControl, EIP712, IVettingPolicy {
    /// @notice Admin of `VETTER_ROLE` — decides who may vet. Timelock post-handoff.
    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");

    /// @notice May call `setVetted` and sign `Vetting` approvals. Held by
    ///         whatever governance appoints.
    bytes32 public constant VETTER_ROLE = keccak256("VETTER_ROLE");

    /// @notice EIP-712 type of the off-chain approval `claimVetting` accepts.
    ///         `nonce` is the publisher's current `nonces` value.
    bytes32 public constant VETTING_TYPEHASH = keccak256("Vetting(address publisher,uint256 nonce)");

    mapping(address publisher => bool) private _vetted;

    /// @notice Per-publisher signature nonce. An un-vet increments it, which
    ///         voids every `Vetting` signature issued for that publisher
    ///         before the un-vet.
    mapping(address publisher => uint256) public nonces;

    event PublisherVetted(address indexed publisher, bool vetted, address indexed by);

    error ZeroAddress();
    error InvalidVettingSignature();

    /// @param admin         `DEFAULT_ADMIN_ROLE` + `GOVERNANCE_ROLE` holder
    ///                      (Timelock at mainnet per ADR 016 § Deployment Order).
    /// @param initialVetter Seeded `VETTER_ROLE` holder, or `address(0)` to leave
    ///                      the role unfilled until governance grants it.
    constructor(address admin, address initialVetter) EIP712("ManualVettingPolicy", "1") {
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
    ///         Un-vetting also increments `nonces[publisher]`, so a `Vetting`
    ///         signature issued before the un-vet cannot vet the publisher again.
    function setVetted(address publisher, bool vetted) external onlyRole(VETTER_ROLE) {
        _vetted[publisher] = vetted;
        if (!vetted) ++nonces[publisher];
        emit PublisherVetted(publisher, vetted, msg.sender);
    }

    /// @notice Vet `publisher` with a `Vetting(publisher, nonces[publisher])`
    ///         signature from a current `VETTER_ROLE` holder. Permissionless:
    ///         the signature binds the publisher, so the caller only pays the
    ///         gas. A signature dies when its signer loses `VETTER_ROLE` or the
    ///         publisher is un-vetted. Resubmitting a live signature only emits
    ///         the event again.
    function claimVetting(address publisher, bytes calldata signature) external {
        bytes32 digest = _hashTypedDataV4(keccak256(abi.encode(VETTING_TYPEHASH, publisher, nonces[publisher])));
        // slither-disable-next-line unused-return
        (address signer, ECDSA.RecoverError err,) = ECDSA.tryRecover(digest, signature);
        if (err != ECDSA.RecoverError.NoError || !hasRole(VETTER_ROLE, signer)) {
            revert InvalidVettingSignature();
        }
        _vetted[publisher] = true;
        emit PublisherVetted(publisher, true, signer);
    }

    /// @inheritdoc IVettingPolicy
    function isVetted(address publisher) external view returns (bool) {
        return _vetted[publisher];
    }
}
