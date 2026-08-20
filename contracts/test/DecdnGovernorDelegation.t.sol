// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IGovernor } from "@openzeppelin/contracts/governance/IGovernor.sol";
import { IERC1271 } from "@openzeppelin/contracts/interfaces/IERC1271.sol";
import { ECDSA } from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";

import { DecdnGovernor } from "../src/DecdnGovernor.sol";
import { IFeeRouter } from "../src/interfaces/IFeeRouter.sol";
import { ICapacityBond } from "../src/interfaces/ICapacityBond.sol";

import { MockFeeRouter } from "./mocks/MockFeeRouter.sol";
import { MockCapacityBond } from "./mocks/MockCapacityBond.sol";

/// @notice Minimal EIP-1271 smart-contract wallet: approves a signature iff it
///         is a valid ECDSA signature from `owner`. Used to prove
///         `delegateBySig` accepts contract signers via `SignatureChecker`.
contract MockERC1271Wallet is IERC1271 {
    address public owner;

    constructor(address owner_) {
        owner = owner_;
    }

    function isValidSignature(bytes32 hash, bytes calldata signature) external view override returns (bytes4) {
        (address recovered,,) = ECDSA.tryRecover(hash, signature);
        return recovered == owner && recovered != address(0) ? this.isValidSignature.selector : bytes4(0xffffffff);
    }
}

/// @title DecdnGovernor vote-delegation tests
/// @notice Exercises the EIP-712 delegation registry (ADR 009 / ADR 026,
///         Governor Bravo pattern): a delegatee casts an operator's vote while
///         the served-bytes weight, bond, and NodeId binding stay on the
///         operator. Covers direct + signed delegation, revocation, expiry,
///         nonce replay, the `NotDelegatee` guard, double-vote prevention
///         (operator-keyed `hasVoted`), the per-operator cap, batch casting,
///         EIP-1271 contract signers, and a full delegated pass to `Succeeded`.
/// @dev    Uses mock `FeeRouter` + `CapacityBond` (as `DecdnGovernor.t.sol`
///         does) so vote weight is injected directly; delegation mechanics are
///         independent of the weight source.
contract DecdnGovernorDelegationTest is Test {
    MockFeeRouter internal feeRouter;
    MockCapacityBond internal bond;
    TimelockController internal timelock;
    DecdnGovernor internal gov;

    uint64 internal constant EPOCH = 7 days;
    uint64 internal constant WINDOW = 13;

    // A base time with many elapsed epochs so the trailing window is populated.
    uint256 internal constant T = 200 * uint256(EPOCH);
    uint256 internal constant PER_TOTAL = 1_000_000;

    // Operators with known private keys so we can sign EIP-712 delegations.
    address internal proposer;
    uint256 internal proposerKey;
    address internal opA;
    uint256 internal opAKey;
    address internal opB;
    uint256 internal opBKey;

    address internal hot = address(0x40E); // a hot voting key (delegatee)
    address internal stranger = address(0x5747);

    uint8 internal constant AGAINST = 0;
    uint8 internal constant FOR = 1;

    // Makes each proposal description unique so identical no-op calls don't
    // collide on `proposalId`.
    uint256 internal _propCounter;

    function setUp() public {
        feeRouter = new MockFeeRouter(WINDOW, EPOCH);
        bond = new MockCapacityBond();

        address[] memory empty = new address[](0);
        address[] memory exec = new address[](1);
        exec[0] = address(0);
        timelock = new TimelockController(2 days, empty, exec, address(this));

        gov = new DecdnGovernor(IFeeRouter(address(feeRouter)), ICapacityBond(address(bond)), timelock);

        (proposer, proposerKey) = makeAddrAndKey("proposer");
        (opA, opAKey) = makeAddrAndKey("opA");
        (opB, opBKey) = makeAddrAndKey("opB");

        vm.warp(T);

        // Each operator gets 10% raw byte share (capped to the 5% default) at
        // full age ramp, so all three carry identical, non-zero vote weight.
        _seedOperator(proposer, 100_000);
        _seedOperator(opA, 100_000);
        _seedOperator(opB, 100_000);
    }

    // -----------------------------------------------------------------
    // Seeding + proposal helpers
    // -----------------------------------------------------------------

    /// @dev Seed `op` with `perOp` bytes/epoch across a band that covers every
    ///      trailing window read during propose (`clock()-1`) and voting
    ///      (`proposalSnapshot`). Total bytes/epoch is set once per epoch.
    function _seedOperator(address op, uint256 perOp) internal {
        bond.setFirstBondedAt(op, uint64(T - 400 days)); // past the 6-month ramp
        for (uint64 e = 180; e <= 205; e++) {
            feeRouter.setBytes(op, e, perOp);
            feeRouter.setTotalBytes(e, PER_TOTAL);
            // Ample declared capacity so the ADR 036 per-epoch cap does not
            // bind for these fixtures; cap-binding is covered explicitly in
            // DecdnGovernor.t.sol's declared-capacity tests.
            bond.setDeclaredMbpsAtEpoch(op, e, 1000);
        }
    }

    /// @dev Propose a no-op action and warp into the active voting window.
    function _activeProposal() internal returns (uint256 proposalId, uint256 snapshot) {
        address[] memory targets = new address[](1);
        targets[0] = address(gov);
        uint256[] memory values = new uint256[](1);
        bytes[] memory calldatas = new bytes[](1);
        calldatas[0] = "";
        _propCounter++;
        string memory desc = string(abi.encodePacked("delegation test proposal #", vm.toString(_propCounter)));

        vm.prank(proposer);
        proposalId = gov.propose(targets, values, calldatas, desc);
        snapshot = gov.proposalSnapshot(proposalId);

        vm.warp(block.timestamp + gov.votingDelay() + 1);
        assertEq(uint256(gov.state(proposalId)), uint256(IGovernor.ProposalState.Active), "proposal not active");
    }

    function _domainSeparator() internal view returns (bytes32) {
        (, string memory name, string memory version, uint256 chainId, address verifyingContract,,) = gov.eip712Domain();
        return keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes(name)),
                keccak256(bytes(version)),
                chainId,
                verifyingContract
            )
        );
    }

    function _delegationDigest(address delegator, address delegatee, uint256 nonce, uint256 expiry)
        internal
        view
        returns (bytes32)
    {
        bytes32 structHash = keccak256(abi.encode(gov.DELEGATION_TYPEHASH(), delegator, delegatee, nonce, expiry));
        return keccak256(abi.encodePacked("\x19\x01", _domainSeparator(), structHash));
    }

    function _sign(uint256 key, bytes32 digest) internal pure returns (bytes memory) {
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(key, digest);
        return abi.encodePacked(r, s, v);
    }

    // -----------------------------------------------------------------
    // Direct delegation
    // -----------------------------------------------------------------

    function test_delegate_setsMappingAndEmits() public {
        vm.expectEmit(true, true, true, true, address(gov));
        emit DecdnGovernor.DelegateChanged(opA, address(0), hot);
        vm.prank(opA);
        gov.delegate(hot);
        assertEq(gov.delegates(opA), hot);
    }

    function test_delegate_revokeClearsMapping() public {
        vm.prank(opA);
        gov.delegate(hot);
        assertEq(gov.delegates(opA), hot);

        vm.prank(opA);
        gov.delegate(address(0));
        assertEq(gov.delegates(opA), address(0));

        // With delegation cleared, the former hot key can no longer cast.
        (uint256 proposalId,) = _activeProposal();
        vm.prank(hot);
        vm.expectRevert(abi.encodeWithSelector(DecdnGovernor.NotDelegatee.selector, opA, hot));
        gov.castVoteByDelegate(proposalId, opA, FOR);
    }

    // -----------------------------------------------------------------
    // Casting via a delegatee
    // -----------------------------------------------------------------

    function test_castByDelegate_attributesWeightToOperator() public {
        (uint256 proposalId, uint256 snapshot) = _activeProposal();
        uint256 opWeight = gov.getVotes(opA, snapshot);
        assertGt(opWeight, 0, "operator has no weight");

        vm.prank(opA);
        gov.delegate(hot);

        vm.prank(hot);
        uint256 cast = gov.castVoteByDelegate(proposalId, opA, FOR);

        assertEq(cast, opWeight, "cast weight != operator weight");
        // Tally + hasVoted are keyed on the operator, not the delegatee.
        assertTrue(gov.hasVoted(proposalId, opA), "operator not marked voted");
        assertFalse(gov.hasVoted(proposalId, hot), "delegatee wrongly marked voted");
        (, uint256 forVotes,) = gov.proposalVotes(proposalId);
        assertEq(forVotes, opWeight, "tally != operator weight");
    }

    function test_castByDelegate_capPreservedVsSelfVote() public {
        // The delegated vote must equal what the operator would cast directly —
        // i.e. the per-operator cap is applied to the operator's weight, not
        // moved or aggregated onto the delegatee.
        (uint256 p1, uint256 snap1) = _activeProposal();
        uint256 selfWeight = gov.getVotes(opA, snap1);
        vm.prank(opA);
        uint256 selfCast = gov.castVote(p1, FOR);
        assertEq(selfCast, selfWeight);

        // Fresh proposal; same operator delegates and votes via the hot key.
        (uint256 p2, uint256 snap2) = _activeProposal();
        vm.prank(opA);
        gov.delegate(hot);
        vm.prank(hot);
        uint256 delegatedCast = gov.castVoteByDelegate(p2, opA, FOR);

        assertEq(delegatedCast, gov.getVotes(opA, snap2));
        assertEq(delegatedCast, selfCast, "delegated weight differs from self-vote");
    }

    function test_castByDelegate_revertsForNonDelegatee() public {
        (uint256 proposalId,) = _activeProposal();
        // opA never delegated to `stranger`.
        vm.prank(stranger);
        vm.expectRevert(abi.encodeWithSelector(DecdnGovernor.NotDelegatee.selector, opA, stranger));
        gov.castVoteByDelegate(proposalId, opA, FOR);
    }

    function test_castByDelegate_withReason() public {
        (uint256 proposalId, uint256 snapshot) = _activeProposal();
        vm.prank(opA);
        gov.delegate(hot);
        vm.prank(hot);
        uint256 cast = gov.castVoteByDelegateWithReason(proposalId, opA, FOR, "delegated aye");
        assertEq(cast, gov.getVotes(opA, snapshot));
        assertTrue(gov.hasVoted(proposalId, opA));
    }

    // -----------------------------------------------------------------
    // Double-vote prevention (operator-keyed hasVoted)
    // -----------------------------------------------------------------

    function test_doubleVote_operatorThenDelegateReverts() public {
        (uint256 proposalId,) = _activeProposal();
        vm.prank(opA);
        gov.delegate(hot);

        // Operator front-runs their own delegate and votes directly.
        vm.prank(opA);
        gov.castVote(proposalId, FOR);

        // The delegate can no longer cast opA's already-consumed weight.
        vm.prank(hot);
        vm.expectRevert(abi.encodeWithSelector(IGovernor.GovernorAlreadyCastVote.selector, opA));
        gov.castVoteByDelegate(proposalId, opA, AGAINST);
    }

    function test_doubleVote_reDelegateAfterCastReverts() public {
        (uint256 proposalId,) = _activeProposal();
        address hot2 = address(0x40E2);

        vm.prank(opA);
        gov.delegate(hot);
        vm.prank(hot);
        gov.castVoteByDelegate(proposalId, opA, FOR);

        // Re-delegating mid-vote must not let a second key re-cast the same
        // operator's weight — `hasVoted[opA]` already guards it.
        vm.prank(opA);
        gov.delegate(hot2);
        vm.prank(hot2);
        vm.expectRevert(abi.encodeWithSelector(IGovernor.GovernorAlreadyCastVote.selector, opA));
        gov.castVoteByDelegate(proposalId, opA, AGAINST);
    }

    // -----------------------------------------------------------------
    // Batch casting
    // -----------------------------------------------------------------

    function test_batchCastByDelegate_countsEachOperator() public {
        (uint256 proposalId, uint256 snapshot) = _activeProposal();
        vm.prank(opA);
        gov.delegate(hot);
        vm.prank(opB);
        gov.delegate(hot);

        address[] memory delegators = new address[](2);
        delegators[0] = opA;
        delegators[1] = opB;

        vm.prank(hot);
        uint256 total = gov.castVotesByDelegate(proposalId, delegators, FOR);

        uint256 expected = gov.getVotes(opA, snapshot) + gov.getVotes(opB, snapshot);
        assertEq(total, expected, "batch total mismatch");
        assertTrue(gov.hasVoted(proposalId, opA));
        assertTrue(gov.hasVoted(proposalId, opB));
        (, uint256 forVotes,) = gov.proposalVotes(proposalId);
        assertEq(forVotes, expected);
    }

    function test_batchCastByDelegate_revertsWholeIfAnyUnauthorized() public {
        (uint256 proposalId,) = _activeProposal();
        vm.prank(opA);
        gov.delegate(hot);
        // opB did NOT delegate to hot.

        address[] memory delegators = new address[](2);
        delegators[0] = opA;
        delegators[1] = opB;

        vm.prank(hot);
        vm.expectRevert(abi.encodeWithSelector(DecdnGovernor.NotDelegatee.selector, opB, hot));
        gov.castVotesByDelegate(proposalId, delegators, FOR);

        // The whole tx reverted — opA's vote was NOT recorded either.
        assertFalse(gov.hasVoted(proposalId, opA), "partial batch leaked a vote");
    }

    // -----------------------------------------------------------------
    // EIP-712 signed delegation
    // -----------------------------------------------------------------

    function test_delegateBySig_valid() public {
        uint256 nonce = gov.delegationNonces(opA);
        uint256 expiry = block.timestamp + 1 hours;
        bytes memory sig = _sign(opAKey, _delegationDigest(opA, hot, nonce, expiry));

        vm.expectEmit(true, true, true, true, address(gov));
        emit DecdnGovernor.DelegateChanged(opA, address(0), hot);
        // A relayer (not opA) submits it.
        vm.prank(stranger);
        gov.delegateBySig(opA, hot, nonce, expiry, sig);

        assertEq(gov.delegates(opA), hot);
        assertEq(gov.delegationNonces(opA), nonce + 1, "nonce not consumed");
    }

    function test_delegateBySig_expiredReverts() public {
        uint256 nonce = gov.delegationNonces(opA);
        uint256 expiry = block.timestamp - 1; // already elapsed
        bytes memory sig = _sign(opAKey, _delegationDigest(opA, hot, nonce, expiry));

        vm.expectRevert(abi.encodeWithSelector(DecdnGovernor.DelegationSignatureExpired.selector, expiry));
        gov.delegateBySig(opA, hot, nonce, expiry, sig);
    }

    function test_delegateBySig_wrongNonceReverts() public {
        uint256 badNonce = gov.delegationNonces(opA) + 7;
        uint256 expiry = block.timestamp + 1 hours;
        bytes memory sig = _sign(opAKey, _delegationDigest(opA, hot, badNonce, expiry));

        vm.expectRevert(abi.encodeWithSelector(DecdnGovernor.InvalidDelegationNonce.selector, opA, 0, badNonce));
        gov.delegateBySig(opA, hot, badNonce, expiry, sig);
    }

    function test_delegateBySig_replayReverts() public {
        uint256 nonce = gov.delegationNonces(opA);
        uint256 expiry = block.timestamp + 1 hours;
        bytes memory sig = _sign(opAKey, _delegationDigest(opA, hot, nonce, expiry));

        gov.delegateBySig(opA, hot, nonce, expiry, sig);
        // Same signature again: nonce was consumed → now expects nonce+1.
        vm.expectRevert(abi.encodeWithSelector(DecdnGovernor.InvalidDelegationNonce.selector, opA, nonce + 1, nonce));
        gov.delegateBySig(opA, hot, nonce, expiry, sig);
    }

    function test_delegateBySig_tamperedSignatureReverts() public {
        uint256 nonce = gov.delegationNonces(opA);
        uint256 expiry = block.timestamp + 1 hours;
        // Signature is over delegatee=hot, but we submit delegatee=stranger.
        bytes memory sig = _sign(opAKey, _delegationDigest(opA, hot, nonce, expiry));

        vm.expectRevert(abi.encodeWithSelector(DecdnGovernor.InvalidDelegationSignature.selector, opA));
        gov.delegateBySig(opA, stranger, nonce, expiry, sig);
    }

    function test_delegateBySig_wrongSignerReverts() public {
        uint256 nonce = gov.delegationNonces(opA);
        uint256 expiry = block.timestamp + 1 hours;
        // opB signs a delegation claiming to be opA.
        bytes memory sig = _sign(opBKey, _delegationDigest(opA, hot, nonce, expiry));

        vm.expectRevert(abi.encodeWithSelector(DecdnGovernor.InvalidDelegationSignature.selector, opA));
        gov.delegateBySig(opA, hot, nonce, expiry, sig);
    }

    function test_delegateBySig_thenCast() public {
        uint256 nonce = gov.delegationNonces(opA);
        uint256 expiry = block.timestamp + 1 hours;
        bytes memory sig = _sign(opAKey, _delegationDigest(opA, hot, nonce, expiry));
        gov.delegateBySig(opA, hot, nonce, expiry, sig);

        (uint256 proposalId, uint256 snapshot) = _activeProposal();
        vm.prank(hot);
        uint256 cast = gov.castVoteByDelegate(proposalId, opA, FOR);
        assertEq(cast, gov.getVotes(opA, snapshot));
        assertTrue(gov.hasVoted(proposalId, opA));
    }

    function test_delegateBySig_eip1271ContractSigner() public {
        // opA's cold key is a smart-contract wallet controlled by opAKey.
        MockERC1271Wallet wallet = new MockERC1271Wallet(opA);
        // Give the wallet the same weight/binding an operator would have.
        _seedOperator(address(wallet), 100_000);

        uint256 nonce = gov.delegationNonces(address(wallet));
        uint256 expiry = block.timestamp + 1 hours;
        bytes memory sig = _sign(opAKey, _delegationDigest(address(wallet), hot, nonce, expiry));

        vm.prank(stranger);
        gov.delegateBySig(address(wallet), hot, nonce, expiry, sig);
        assertEq(gov.delegates(address(wallet)), hot);

        (uint256 proposalId, uint256 snapshot) = _activeProposal();
        vm.prank(hot);
        uint256 cast = gov.castVoteByDelegate(proposalId, address(wallet), FOR);
        assertEq(cast, gov.getVotes(address(wallet), snapshot));
    }

    function test_delegationNonces_startsZero() public view {
        assertEq(gov.delegationNonces(opA), 0);
        assertEq(gov.delegationNonces(stranger), 0);
    }

    // -----------------------------------------------------------------
    // Full delegated pass — delegated votes reach quorum + Succeeded
    // -----------------------------------------------------------------

    function test_delegatedVotes_driveProposalToSucceeded() public {
        (uint256 proposalId, uint256 snapshot) = _activeProposal();

        // proposer votes directly; opA + opB delegate to one hot key that
        // batch-casts. All three For votes must clear the 4% quorum.
        vm.prank(proposer);
        gov.castVote(proposalId, FOR);

        vm.prank(opA);
        gov.delegate(hot);
        vm.prank(opB);
        gov.delegate(hot);
        address[] memory delegators = new address[](2);
        delegators[0] = opA;
        delegators[1] = opB;
        vm.prank(hot);
        gov.castVotesByDelegate(proposalId, delegators, FOR);

        (, uint256 forVotes,) = gov.proposalVotes(proposalId);
        uint256 expected = gov.getVotes(proposer, snapshot) + gov.getVotes(opA, snapshot) + gov.getVotes(opB, snapshot);
        assertEq(forVotes, expected);
        assertGt(forVotes, gov.quorum(snapshot), "did not clear quorum");

        vm.warp(block.timestamp + gov.votingPeriod() + 1);
        assertEq(
            uint256(gov.state(proposalId)),
            uint256(IGovernor.ProposalState.Succeeded),
            "delegated votes did not carry the proposal"
        );
    }
}
