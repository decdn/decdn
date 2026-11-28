// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { ManualVettingPolicy } from "../src/ManualVettingPolicy.sol";

contract ManualVettingPolicyTest is Test {
    ManualVettingPolicy internal policy;

    address internal admin = address(0xA11CE);
    uint256 internal vetterKey = 0xF00D;
    address internal vetter;
    address internal publisher = address(0xBEEF);
    address internal stranger = address(0x5747A);

    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 internal constant VETTER_ROLE = keccak256("VETTER_ROLE");

    function setUp() public {
        vetter = vm.addr(vetterKey);
        policy = new ManualVettingPolicy(admin, vetter);
    }

    function _vettingSig(uint256 key, address who, uint256 nonce) internal view returns (bytes memory) {
        bytes32 structHash = keccak256(abi.encode(policy.VETTING_TYPEHASH(), who, nonce));
        bytes32 domainSeparator = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256("ManualVettingPolicy"),
                keccak256("1"),
                block.chainid,
                address(policy)
            )
        );
        (uint8 v, bytes32 r, bytes32 s) =
            vm.sign(key, keccak256(abi.encodePacked("\x19\x01", domainSeparator, structHash)));
        return abi.encodePacked(r, s, v);
    }

    /// The publisher (not the vetter) submits the vetter's signature and pays
    /// the gas; the event names the signer as the vetter.
    function test_claimVetting_vetsAndEmitsSigner() public {
        bytes memory sig = _vettingSig(vetterKey, publisher, 0);
        vm.expectEmit(true, true, true, true);
        emit ManualVettingPolicy.PublisherVetted(publisher, true, vetter);
        vm.prank(publisher);
        policy.claimVetting(publisher, sig);
        assertTrue(policy.isVetted(publisher));
    }

    /// The signature binds the publisher: anyone may relay it, but it vets
    /// only the address it names.
    function test_claimVetting_bindsPublisher() public {
        bytes memory sig = _vettingSig(vetterKey, publisher, 0);
        vm.prank(stranger);
        vm.expectRevert(ManualVettingPolicy.InvalidVettingSignature.selector);
        policy.claimVetting(stranger, sig);

        vm.prank(stranger);
        policy.claimVetting(publisher, sig);
        assertTrue(policy.isVetted(publisher));
        assertFalse(policy.isVetted(stranger));
    }

    function test_claimVetting_rejectsNonVetterSigner() public {
        bytes memory sig = _vettingSig(0xBAD, publisher, 0);
        vm.expectRevert(ManualVettingPolicy.InvalidVettingSignature.selector);
        policy.claimVetting(publisher, sig);
    }

    function test_claimVetting_rejectsMalformedSignature() public {
        vm.expectRevert(ManualVettingPolicy.InvalidVettingSignature.selector);
        policy.claimVetting(publisher, hex"1234");
    }

    /// An un-vet voids every signature issued before it, so a revoked
    /// publisher cannot replay its old signature. A fresh signature over the
    /// new nonce vets it again.
    function test_claimVetting_unvetVoidsOutstandingSignature() public {
        bytes memory sig = _vettingSig(vetterKey, publisher, 0);
        policy.claimVetting(publisher, sig);

        vm.prank(vetter);
        policy.setVetted(publisher, false);
        assertEq(policy.nonces(publisher), 1);

        vm.expectRevert(ManualVettingPolicy.InvalidVettingSignature.selector);
        policy.claimVetting(publisher, sig);
        assertFalse(policy.isVetted(publisher));

        policy.claimVetting(publisher, _vettingSig(vetterKey, publisher, 1));
        assertTrue(policy.isVetted(publisher));
    }

    /// Revoking the signer's VETTER_ROLE voids its unclaimed signatures.
    function test_claimVetting_rejectsSignerWithRevokedRole() public {
        bytes memory sig = _vettingSig(vetterKey, publisher, 0);
        vm.prank(admin);
        policy.revokeRole(VETTER_ROLE, vetter);
        vm.expectRevert(ManualVettingPolicy.InvalidVettingSignature.selector);
        policy.claimVetting(publisher, sig);
    }

    function test_isVetted_falseByDefault() public view {
        assertFalse(policy.isVetted(publisher));
    }

    function test_setVetted_grantsAndEmits() public {
        vm.expectEmit(true, true, true, true);
        emit ManualVettingPolicy.PublisherVetted(publisher, true, vetter);
        vm.prank(vetter);
        policy.setVetted(publisher, true);
        assertTrue(policy.isVetted(publisher));
    }

    function test_setVetted_revokes() public {
        vm.prank(vetter);
        policy.setVetted(publisher, true);
        vm.prank(vetter);
        policy.setVetted(publisher, false);
        assertFalse(policy.isVetted(publisher));
    }

    function test_setVetted_onlyVetter() public {
        vm.prank(stranger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, VETTER_ROLE)
        );
        policy.setVetted(publisher, true);
    }

    /// GOVERNANCE_ROLE decides WHO may vet: it grants VETTER_ROLE to a new
    /// address, that address can then vet, and a revoke removes the power. This
    /// is the "move who does the vetting without swapping the contract" lever.
    function test_governanceGrantsAndRevokesVetter() public {
        address newVetter = address(0xC0FFEE);

        vm.prank(admin);
        policy.grantRole(VETTER_ROLE, newVetter);
        vm.prank(newVetter);
        policy.setVetted(publisher, true);
        assertTrue(policy.isVetted(publisher));

        vm.prank(admin);
        policy.revokeRole(VETTER_ROLE, newVetter);
        vm.prank(newVetter);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, newVetter, VETTER_ROLE)
        );
        policy.setVetted(stranger, true);
    }

    /// VETTER_ROLE's admin is GOVERNANCE_ROLE, not the default admin — so
    /// governance (the Timelock post-handoff) manages the vetter set.
    function test_vetterRoleAdminIsGovernance() public view {
        assertEq(policy.getRoleAdmin(VETTER_ROLE), GOVERNANCE_ROLE);
    }

    function test_constructor_seedsInitialVetter() public view {
        assertTrue(policy.hasRole(VETTER_ROLE, vetter));
        assertTrue(policy.hasRole(GOVERNANCE_ROLE, admin));
        assertTrue(policy.hasRole(policy.DEFAULT_ADMIN_ROLE(), admin));
    }

    function test_constructor_zeroInitialVetter_leavesRoleUnfilled() public {
        ManualVettingPolicy p = new ManualVettingPolicy(admin, address(0));
        assertFalse(p.hasRole(VETTER_ROLE, admin));
        // Governance can still seat a vetter afterwards.
        vm.prank(admin);
        p.grantRole(VETTER_ROLE, vetter);
        assertTrue(p.hasRole(VETTER_ROLE, vetter));
    }

    function test_constructor_rejectsZeroAdmin() public {
        vm.expectRevert(ManualVettingPolicy.ZeroAddress.selector);
        new ManualVettingPolicy(address(0), vetter);
    }
}
