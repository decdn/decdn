// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Vm } from "forge-std/Vm.sol";
import { Token } from "../src/Token.sol";
import { StakingRegistry } from "../src/StakingRegistry.sol";
import { ISafetyReserve } from "../src/interfaces/ISafetyReserve.sol";
import { IEd25519Verifier } from "../src/interfaces/IEd25519Verifier.sol";
import { MockSafetyReserve } from "./mocks/MockSafetyReserve.sol";
import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";

contract StakingRegistryTest is Test {
    Token internal token;
    StakingRegistry internal reg;
    MockSafetyReserve internal safetyReserve;
    MockEd25519Verifier internal ed25519Verifier;

    address internal admin = makeAddr("admin");
    address internal slashJudge = makeAddr("slashJudge");
    address internal blacklist = makeAddr("blacklist");
    address internal feeRouter = makeAddr("feeRouter");
    address internal pauser = makeAddr("pauser");
    address internal operator = makeAddr("operator");
    address internal challenger = makeAddr("challenger");

    uint256 internal constant MIN_STAKE = 50_000e18;
    uint256 internal constant UNBONDING_PERIOD = 7 days;
    uint256 internal constant MULTIADDR_COOLDOWN = 0;
    uint256 internal constant MAX_MULTIADDR_SIZE = 1024;

    function setUp() public {
        token = new Token(admin);
        safetyReserve = new MockSafetyReserve();
        ed25519Verifier = new MockEd25519Verifier();
        reg = new StakingRegistry(
            token,
            IEd25519Verifier(address(ed25519Verifier)),
            admin,
            MIN_STAKE,
            UNBONDING_PERIOD,
            MULTIADDR_COOLDOWN,
            MAX_MULTIADDR_SIZE
        );

        vm.startPrank(admin);
        reg.grantRole(reg.SLASH_ROLE(), slashJudge);
        reg.grantRole(reg.BLACKLIST_ROLE(), blacklist);
        reg.grantRole(reg.SETTLEMENT_REPORTER_ROLE(), feeRouter);
        reg.grantRole(reg.PAUSER_ROLE(), pauser);
        reg.setSafetyReserve(ISafetyReserve(address(safetyReserve)));
        // Seed the operator with stake-worthy TOKEN. Sized above the fuzz
        // upper bound (1M TOKEN) so fuzz tests don't run out.
        assertTrue(token.transfer(operator, 100 * MIN_STAKE));
        vm.stopPrank();
    }

    function _stake(address who, uint256 amount) internal {
        vm.startPrank(who);
        token.approve(address(reg), amount);
        reg.stake(amount);
        vm.stopPrank();
    }

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    function test_constructor_setsImmutableAndState() public view {
        assertEq(address(reg.token()), address(token));
        assertEq(address(reg.ed25519Verifier()), address(ed25519Verifier));
        assertEq(reg.minStake(), MIN_STAKE);
        assertEq(reg.unbondingPeriod(), UNBONDING_PERIOD);
        assertEq(reg.multiaddrUpdateCooldown(), MULTIADDR_COOLDOWN);
        assertEq(reg.maxMultiaddrSize(), MAX_MULTIADDR_SIZE);
        assertTrue(reg.hasRole(reg.DEFAULT_ADMIN_ROLE(), admin));
        assertTrue(reg.hasRole(reg.GOVERNANCE_ROLE(), admin));
    }

    function test_constructor_revertsOnZeroToken() public {
        vm.expectRevert(StakingRegistry.ZeroAddress.selector);
        new StakingRegistry(
            ERC20Burnable(address(0)),
            IEd25519Verifier(address(ed25519Verifier)),
            admin,
            MIN_STAKE,
            UNBONDING_PERIOD,
            MULTIADDR_COOLDOWN,
            MAX_MULTIADDR_SIZE
        );
    }

    function test_constructor_revertsOnZeroVerifier() public {
        vm.expectRevert(StakingRegistry.ZeroAddress.selector);
        new StakingRegistry(
            token,
            IEd25519Verifier(address(0)),
            admin,
            MIN_STAKE,
            UNBONDING_PERIOD,
            MULTIADDR_COOLDOWN,
            MAX_MULTIADDR_SIZE
        );
    }

    function test_constructor_revertsOnZeroAdmin() public {
        vm.expectRevert(StakingRegistry.ZeroAddress.selector);
        new StakingRegistry(
            token,
            IEd25519Verifier(address(ed25519Verifier)),
            address(0),
            MIN_STAKE,
            UNBONDING_PERIOD,
            MULTIADDR_COOLDOWN,
            MAX_MULTIADDR_SIZE
        );
    }

    function test_constructor_revertsOnOutOfBoundsMinStake() public {
        vm.expectRevert();
        _deployWith(1e18, UNBONDING_PERIOD, MULTIADDR_COOLDOWN, MAX_MULTIADDR_SIZE);
        vm.expectRevert();
        _deployWith(10_000_000e18, UNBONDING_PERIOD, MULTIADDR_COOLDOWN, MAX_MULTIADDR_SIZE);
    }

    function test_constructor_revertsOnOutOfBoundsUnbondingPeriod() public {
        vm.expectRevert();
        _deployWith(MIN_STAKE, 1 days, MULTIADDR_COOLDOWN, MAX_MULTIADDR_SIZE);
        vm.expectRevert();
        _deployWith(MIN_STAKE, 60 days, MULTIADDR_COOLDOWN, MAX_MULTIADDR_SIZE);
    }

    function test_constructor_revertsOnOutOfBoundsMultiaddrCooldown() public {
        vm.expectRevert();
        _deployWith(MIN_STAKE, UNBONDING_PERIOD, 2 days, MAX_MULTIADDR_SIZE);
    }

    function test_constructor_revertsOnOutOfBoundsMaxMultiaddrSize() public {
        vm.expectRevert();
        _deployWith(MIN_STAKE, UNBONDING_PERIOD, MULTIADDR_COOLDOWN, 16);
        vm.expectRevert();
        _deployWith(MIN_STAKE, UNBONDING_PERIOD, MULTIADDR_COOLDOWN, 4096);
    }

    function _deployWith(uint256 minStake_, uint256 unbondingPeriod_, uint256 cooldown_, uint256 maxSize_) internal {
        new StakingRegistry(
            token, IEd25519Verifier(address(ed25519Verifier)), admin, minStake_, unbondingPeriod_, cooldown_, maxSize_
        );
    }

    // -----------------------------------------------------------------
    // Staking
    // -----------------------------------------------------------------

    function test_stake_addsToActiveStake() public {
        _stake(operator, MIN_STAKE);
        assertEq(reg.activeStake(operator), MIN_STAKE);
        assertEq(reg.stakeOf(operator), MIN_STAKE);
        assertEq(token.balanceOf(address(reg)), MIN_STAKE);
    }

    function test_stake_revertsOnZeroAmount() public {
        vm.prank(operator);
        vm.expectRevert(StakingRegistry.ZeroAmount.selector);
        reg.stake(0);
    }

    function test_stake_revertsWhenPaused() public {
        vm.prank(pauser);
        reg.pause();
        vm.prank(operator);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        reg.stake(MIN_STAKE);
    }

    function test_stake_clearsEjectedFlagWhenRestakedToMinimum() public {
        _stake(operator, MIN_STAKE);
        vm.prank(blacklist);
        reg.ejectNode(operator);
        assertTrue(reg.ejected(operator));

        // Stake already at min; one more wei triggers the path.
        _stake(operator, 1);
        assertFalse(reg.ejected(operator));
    }

    function test_isActive_requiresStakeAndRegistration() public {
        Vm.Wallet memory wallet = vm.createWallet("active-test");
        address op = wallet.addr;

        // No stake, not registered → false
        assertFalse(reg.isActive(op));

        // Staked but unregistered → still false (ADR 003 isActive needs registration)
        _fundAndStake(op, MIN_STAKE);
        assertFalse(reg.isActive(op));

        // Register → true
        bytes32 nodeId = bytes32(uint256(0xABC));
        bytes memory sig = _signBindNode(wallet, nodeId, 0);
        vm.prank(op);
        reg.registerNode(nodeId, hex"", "", sig, hex"");
        assertTrue(reg.isActive(op));

        // Drop stake below minimum (request unstake) → false despite registration
        vm.prank(op);
        reg.requestUnstake(1);
        assertFalse(reg.isActive(op));
    }

    function test_isActive_falseWhenAnyStakeInUnbonding() public {
        Vm.Wallet memory wallet = vm.createWallet("unbond-test");
        address op = wallet.addr;

        // Stake 2x minimum and register → active
        _fundAndStake(op, 2 * MIN_STAKE);
        bytes32 nodeId = bytes32(uint256(0xBEEF));
        bytes memory sig = _signBindNode(wallet, nodeId, 0);
        vm.prank(op);
        reg.registerNode(nodeId, hex"", "", sig, hex"");
        assertTrue(reg.isActive(op));

        // Begin unbonding a portion; remaining activeStake (MIN_STAKE) is still
        // >= minStake, but ADR 003 treats "partially in unbonding" as inactive.
        vm.prank(op);
        reg.requestUnstake(MIN_STAKE);
        assertEq(reg.activeStake(op), MIN_STAKE);
        assertFalse(reg.isActive(op), "any in-flight unbonding makes operator inactive");
    }

    // -----------------------------------------------------------------
    // Unbonding + unstake
    // -----------------------------------------------------------------

    function test_requestUnstake_movesAmountToUnbonding() public {
        _stake(operator, 2 * MIN_STAKE);
        vm.prank(operator);
        reg.requestUnstake(MIN_STAKE);

        assertEq(reg.activeStake(operator), MIN_STAKE);
        (uint256 amount, uint256 unlockAt) = reg.unbondingOf(operator);
        assertEq(amount, MIN_STAKE);
        assertEq(unlockAt, block.timestamp + UNBONDING_PERIOD);
    }

    function test_requestUnstake_revertsOnInsufficientStake() public {
        _stake(operator, MIN_STAKE);
        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(StakingRegistry.InsufficientStake.selector, MIN_STAKE + 1, MIN_STAKE));
        reg.requestUnstake(MIN_STAKE + 1);
    }

    function test_requestUnstake_revertsOnInflightRequest() public {
        _stake(operator, 2 * MIN_STAKE);
        vm.prank(operator);
        reg.requestUnstake(MIN_STAKE);
        vm.prank(operator);
        vm.expectRevert(StakingRegistry.UnbondingInProgress.selector);
        reg.requestUnstake(MIN_STAKE);
    }

    function test_unstake_revertsBeforeUnbondingComplete() public {
        _stake(operator, MIN_STAKE);
        vm.prank(operator);
        reg.requestUnstake(MIN_STAKE);

        vm.warp(block.timestamp + UNBONDING_PERIOD - 1);
        vm.prank(operator);
        vm.expectRevert();
        reg.unstake();
    }

    function test_unstake_returnsTokensAfterUnbonding() public {
        _stake(operator, MIN_STAKE);
        uint256 operatorBalanceBefore = token.balanceOf(operator);

        vm.prank(operator);
        reg.requestUnstake(MIN_STAKE);

        vm.warp(block.timestamp + UNBONDING_PERIOD);
        vm.prank(operator);
        reg.unstake();

        assertEq(token.balanceOf(operator), operatorBalanceBefore + MIN_STAKE);
        (uint256 amount,) = reg.unbondingOf(operator);
        assertEq(amount, 0);
    }

    function test_unstake_revertsWithNoRequest() public {
        vm.prank(operator);
        vm.expectRevert(StakingRegistry.NoUnbondingRequest.selector);
        reg.unstake();
    }

    // -----------------------------------------------------------------
    // Node registry — registerNode
    // -----------------------------------------------------------------

    function test_registerNode_happyPath() public {
        (Vm.Wallet memory wallet, bytes32 nodeId, bytes memory bindingSig) = _prepareRegistration(0);
        _fundAndStake(wallet.addr, MIN_STAKE);

        bytes memory multiaddrs = hex"010203";

        // Assert StakingRegistry calls the verifier with the canonical
        // (publicKey, messageHash, signature) tuple from ADR 003 § Signed
        // message. vm.expectCall replaces mock-side storage tracking now
        // that IEd25519Verifier.verify is `view`.
        bytes32 expectedHash = keccak256(abi.encodePacked(nodeId, wallet.addr, block.chainid, uint64(0)));
        vm.expectCall(
            address(ed25519Verifier), abi.encodeCall(IEd25519Verifier.verify, (nodeId, expectedHash, hex"deadbeef"))
        );

        vm.prank(wallet.addr);
        reg.registerNode(nodeId, multiaddrs, "us", bindingSig, hex"deadbeef");

        assertEq(reg.nodeIdToAddress(nodeId), wallet.addr);
        assertEq(reg.addressToNodeId(wallet.addr), nodeId);
        assertEq(reg.bindingNonce(wallet.addr), 1);
        assertEq(reg.getActiveNodeCount(), 1);

        StakingRegistry.NodeInfo memory info = reg.getNode(nodeId);
        assertEq(info.nodeId, nodeId);
        assertEq(info.ethAddress, wallet.addr);
        assertTrue(info.active);
        assertEq(info.registeredAt, block.timestamp);
        assertEq(info.firstRegisteredAt, block.timestamp);
        assertEq(info.regionHint, "us");
        assertEq(keccak256(info.multiaddrs), keccak256(multiaddrs));
    }

    function test_registerNode_revertsBelowMinStake() public {
        (Vm.Wallet memory wallet, bytes32 nodeId, bytes memory bindingSig) = _prepareRegistration(0);
        _fundAndStake(wallet.addr, MIN_STAKE - 1);

        vm.prank(wallet.addr);
        vm.expectRevert();
        reg.registerNode(nodeId, hex"", "", bindingSig, hex"");
    }

    function test_registerNode_revertsOnZeroNodeId() public {
        (Vm.Wallet memory wallet,, bytes memory bindingSig) = _prepareRegistration(0);
        _fundAndStake(wallet.addr, MIN_STAKE);

        vm.prank(wallet.addr);
        vm.expectRevert(StakingRegistry.ZeroNodeId.selector);
        reg.registerNode(bytes32(0), hex"", "", bindingSig, hex"");
    }

    function test_registerNode_revertsWhenMultiaddrsTooLarge() public {
        (Vm.Wallet memory wallet, bytes32 nodeId, bytes memory bindingSig) = _prepareRegistration(0);
        _fundAndStake(wallet.addr, MIN_STAKE);

        bytes memory multiaddrs = new bytes(MAX_MULTIADDR_SIZE + 1);
        vm.prank(wallet.addr);
        vm.expectRevert();
        reg.registerNode(nodeId, multiaddrs, "", bindingSig, hex"");
    }

    function test_registerNode_revertsOnBadBindingSignature() public {
        (Vm.Wallet memory wallet, bytes32 nodeId,) = _prepareRegistration(0);
        _fundAndStake(wallet.addr, MIN_STAKE);

        bytes memory badSig = new bytes(65); // all zeros
        vm.prank(wallet.addr);
        vm.expectRevert(StakingRegistry.InvalidBindingSignature.selector);
        reg.registerNode(nodeId, hex"", "", badSig, hex"");
    }

    function test_registerNode_revertsOnRejectedEd25519Signature() public {
        ed25519Verifier.setAccept(false);

        (Vm.Wallet memory wallet, bytes32 nodeId, bytes memory bindingSig) = _prepareRegistration(0);
        _fundAndStake(wallet.addr, MIN_STAKE);

        vm.prank(wallet.addr);
        vm.expectRevert(StakingRegistry.InvalidEd25519Signature.selector);
        reg.registerNode(nodeId, hex"", "", bindingSig, hex"");
    }

    function test_registerNode_revertsWhenEjected() public {
        (Vm.Wallet memory wallet, bytes32 nodeId, bytes memory bindingSig) = _prepareRegistration(0);
        _fundAndStake(wallet.addr, MIN_STAKE);

        // Blacklist-eject before registering (stake stays >= minStake).
        vm.prank(blacklist);
        reg.ejectNode(wallet.addr);
        assertTrue(reg.ejected(wallet.addr));

        // Even with sufficient stake, an ejected operator cannot register —
        // they must be reinstated (re-stake to >= minStake clears the flag).
        vm.prank(wallet.addr);
        vm.expectRevert(StakingRegistry.OperatorEjected.selector);
        reg.registerNode(nodeId, hex"", "", bindingSig, hex"");
    }

    function test_registerNode_revertsWhenRegionHintTooLong() public {
        (Vm.Wallet memory wallet, bytes32 nodeId, bytes memory bindingSig) = _prepareRegistration(0);
        _fundAndStake(wallet.addr, MIN_STAKE);

        string memory longHint = "this-is-way-too-long-for-a-region-code";
        vm.prank(wallet.addr);
        vm.expectRevert(abi.encodeWithSelector(StakingRegistry.RegionHintTooLong.selector, bytes(longHint).length, 16));
        reg.registerNode(nodeId, hex"", longHint, bindingSig, hex"");
    }

    function test_registerNode_revertsOnAlreadyRegistered() public {
        (Vm.Wallet memory wallet, bytes32 nodeId, bytes memory bindingSig) = _prepareRegistration(0);
        _fundAndStake(wallet.addr, MIN_STAKE);
        vm.prank(wallet.addr);
        reg.registerNode(nodeId, hex"", "", bindingSig, hex"");

        // Second registration without deregister must revert.
        (,, bytes memory bindingSig2) = _prepareRegistrationFor(wallet, nodeId, 1);
        vm.prank(wallet.addr);
        vm.expectRevert(StakingRegistry.NodeAlreadyRegistered.selector);
        reg.registerNode(nodeId, hex"", "", bindingSig2, hex"");
    }

    function test_registerNode_revertsOnNodeIdBoundToOther() public {
        // First operator registers nodeId X
        (Vm.Wallet memory wallet1, bytes32 nodeId, bytes memory bindingSig1) = _prepareRegistration(0);
        _fundAndStake(wallet1.addr, MIN_STAKE);
        vm.prank(wallet1.addr);
        reg.registerNode(nodeId, hex"", "", bindingSig1, hex"");

        // Second operator tries the same nodeId
        Vm.Wallet memory wallet2 = vm.createWallet("op2");
        _fundAndStake(wallet2.addr, MIN_STAKE);
        bytes memory bindingSig2 = _signBindNode(wallet2, nodeId, 0);
        vm.prank(wallet2.addr);
        vm.expectRevert(abi.encodeWithSelector(StakingRegistry.NodeIdAlreadyBound.selector, wallet1.addr));
        reg.registerNode(nodeId, hex"", "", bindingSig2, hex"");
    }

    // -----------------------------------------------------------------
    // Node registry — deregisterNode
    // -----------------------------------------------------------------

    function test_deregisterNode_flipsFlagAndRemovesFromActiveSet() public {
        (Vm.Wallet memory wallet, bytes32 nodeId,) = _registerAt(0);

        assertEq(reg.getActiveNodeCount(), 1);
        assertTrue(reg.isActive(wallet.addr));

        vm.prank(wallet.addr);
        reg.deregisterNode();

        assertFalse(reg.isActive(wallet.addr));
        assertEq(reg.getActiveNodeCount(), 0);
        // Binding persists; registrationNonce bumped to invalidate stale sigs.
        assertEq(reg.nodeIdToAddress(nodeId), wallet.addr);
        assertEq(reg.registrationNonce(nodeId), 1);
    }

    function test_deregisterNode_revertsWhenNotActive() public {
        vm.prank(operator);
        vm.expectRevert(StakingRegistry.NodeNotActive.selector);
        reg.deregisterNode();
    }

    function test_reRegisterSameNodeId_afterDeregister_works() public {
        (Vm.Wallet memory wallet, bytes32 nodeId,) = _registerAt(0);
        vm.prank(wallet.addr);
        reg.deregisterNode();

        // Sign with the post-deregister nonces
        (,, bytes memory bindingSig) = _prepareRegistrationFor(wallet, nodeId, 1);
        vm.prank(wallet.addr);
        reg.registerNode(nodeId, hex"", "", bindingSig, hex"");

        StakingRegistry.NodeInfo memory info = reg.getNode(nodeId);
        assertTrue(info.active);
        // firstRegisteredAt is write-once.
        assertTrue(info.firstRegisteredAt <= info.registeredAt);
        assertEq(reg.registrationNonce(nodeId), 1, "registrationNonce stays at 1 between deregister and re-register");
    }

    // -----------------------------------------------------------------
    // Node registry — bindNodeId (key rotation)
    // -----------------------------------------------------------------

    function test_bindNodeId_rotatesToNewNodeId() public {
        (Vm.Wallet memory wallet, bytes32 oldNodeId,) = _registerAt(0);
        bytes32 newNodeId = bytes32(uint256(0xC0FFEE));

        // bindingNonce is 1 after registerNode.
        bytes memory bindSig = _signBindNode(wallet, newNodeId, 1);
        bytes32 expectedHash = keccak256(abi.encodePacked(newNodeId, wallet.addr, block.chainid, uint64(0)));
        vm.expectCall(
            address(ed25519Verifier), abi.encodeCall(IEd25519Verifier.verify, (newNodeId, expectedHash, hex"deadbeef"))
        );

        vm.prank(wallet.addr);
        reg.bindNodeId(newNodeId, bindSig, hex"deadbeef");

        assertEq(reg.addressToNodeId(wallet.addr), newNodeId);
        assertEq(reg.nodeIdToAddress(newNodeId), wallet.addr);
        assertEq(reg.nodeIdToAddress(oldNodeId), address(0), "old binding released");
        assertEq(reg.bindingNonce(wallet.addr), 2);

        // NodeInfo stays consistent for the active node.
        StakingRegistry.NodeInfo memory info = reg.getNodeByAddress(wallet.addr);
        assertEq(info.nodeId, newNodeId);
        assertTrue(reg.isActiveNode(newNodeId));
        assertFalse(reg.isActiveNode(oldNodeId));
    }

    function test_bindNodeId_revertsOnRejectedEd25519() public {
        (Vm.Wallet memory wallet,,) = _registerAt(0);
        bytes32 newNodeId = bytes32(uint256(0xC0FFEE));
        bytes memory bindSig = _signBindNode(wallet, newNodeId, 1);

        ed25519Verifier.setAccept(false);
        vm.prank(wallet.addr);
        vm.expectRevert(StakingRegistry.InvalidEd25519Signature.selector);
        reg.bindNodeId(newNodeId, bindSig, hex"");
    }

    function test_bindNodeId_revertsOnBadBindingSig() public {
        (Vm.Wallet memory wallet,,) = _registerAt(0);
        bytes32 newNodeId = bytes32(uint256(0xC0FFEE));
        bytes memory badSig = new bytes(65);
        vm.prank(wallet.addr);
        vm.expectRevert(StakingRegistry.InvalidBindingSignature.selector);
        reg.bindNodeId(newNodeId, badSig, hex"");
    }

    function test_bindNodeId_revertsIfBoundToOther() public {
        (Vm.Wallet memory wallet1, bytes32 nodeId,) = _registerAt(0);
        Vm.Wallet memory wallet2 = vm.createWallet("op2");
        // wallet2 tries to bind wallet1's nodeId.
        bytes memory bindSig = _signBindNode(wallet2, nodeId, 0);
        vm.prank(wallet2.addr);
        vm.expectRevert(abi.encodeWithSelector(StakingRegistry.NodeIdAlreadyBound.selector, wallet1.addr));
        reg.bindNodeId(nodeId, bindSig, hex"deadbeef");
    }

    function test_bindNodeId_revertsOnZeroNodeId() public {
        (Vm.Wallet memory wallet,,) = _registerAt(0);
        bytes memory bindSig = _signBindNode(wallet, bytes32(0), 1);
        vm.prank(wallet.addr);
        vm.expectRevert(StakingRegistry.ZeroNodeId.selector);
        reg.bindNodeId(bytes32(0), bindSig, hex"deadbeef");
    }

    function test_bindNodeId_preRegistrationBindsWithoutActiveNode() public {
        Vm.Wallet memory wallet = vm.createWallet("unregistered");
        bytes32 nodeId = bytes32(uint256(0xAB));
        bytes memory bindSig = _signBindNode(wallet, nodeId, 0);

        vm.prank(wallet.addr);
        reg.bindNodeId(nodeId, bindSig, hex"deadbeef");

        assertEq(reg.nodeIdToAddress(nodeId), wallet.addr);
        // No active node — harmless binding (ADR 003).
        assertFalse(reg.isActiveNode(nodeId));
        assertFalse(reg.getNodeByAddress(wallet.addr).active);
    }

    // -----------------------------------------------------------------
    // Node registry — reclaimNodeId (squat recovery backstop)
    // -----------------------------------------------------------------

    function test_reclaimNodeId_tearsDownHolderBinding() public {
        (Vm.Wallet memory holder, bytes32 nodeId,) = _registerAt(0);
        assertEq(reg.getActiveNodeCount(), 1);

        address reclaimer = makeAddr("reclaimer");
        vm.prank(reclaimer);
        reg.reclaimNodeId(nodeId, hex"deadbeef"); // mock ed25519 accepts

        assertEq(reg.nodeIdToAddress(nodeId), address(0));
        assertEq(reg.addressToNodeId(holder.addr), bytes32(0));
        assertEq(reg.getActiveNodeCount(), 0, "holder's node deactivated");
        assertFalse(reg.getNodeByAddress(holder.addr).active);
        assertEq(reg.registrationNonce(nodeId), 1, "nonce bumped");
    }

    function test_reclaimNodeId_revertsIfNotBound() public {
        address reclaimer = makeAddr("reclaimer");
        bytes32 nodeId = bytes32(uint256(0xDEAD));
        vm.prank(reclaimer);
        vm.expectRevert(abi.encodeWithSelector(StakingRegistry.NodeIdNotBound.selector, nodeId));
        reg.reclaimNodeId(nodeId, hex"deadbeef");
    }

    function test_reclaimNodeId_revertsOnRejectedEd25519() public {
        (, bytes32 nodeId,) = _registerAt(0);
        ed25519Verifier.setAccept(false);
        address reclaimer = makeAddr("reclaimer");
        vm.prank(reclaimer);
        vm.expectRevert(StakingRegistry.InvalidEd25519Signature.selector);
        reg.reclaimNodeId(nodeId, hex"");
    }

    function test_reclaimThenReRegister_recoversNodeId() public {
        (, bytes32 nodeId,) = _registerAt(0);

        Vm.Wallet memory reclaimer = vm.createWallet("reclaimer");
        vm.prank(reclaimer.addr);
        reg.reclaimNodeId(nodeId, hex"deadbeef");

        // Reclaimer now registers the freed NodeId under their own address.
        _fundAndStake(reclaimer.addr, MIN_STAKE);
        bytes memory bindSig = _signBindNode(reclaimer, nodeId, 0);
        vm.prank(reclaimer.addr);
        reg.registerNode(nodeId, hex"", "", bindSig, hex"deadbeef");

        assertEq(reg.nodeIdToAddress(nodeId), reclaimer.addr);
        assertTrue(reg.isActiveNode(nodeId));
    }

    // -----------------------------------------------------------------
    // Node registry — updateMultiaddrs
    // -----------------------------------------------------------------

    function test_updateMultiaddrs_replacesAndStampsTimestamp() public {
        (Vm.Wallet memory wallet, bytes32 nodeId,) = _registerAt(0);

        bytes memory newAddrs = hex"abcdef";
        vm.warp(block.timestamp + 100);
        vm.prank(wallet.addr);
        reg.updateMultiaddrs(newAddrs);

        StakingRegistry.NodeInfo memory info = reg.getNode(nodeId);
        assertEq(keccak256(info.multiaddrs), keccak256(newAddrs));
        assertEq(info.lastMultiaddrUpdate, block.timestamp);
    }

    function test_updateMultiaddrs_respectsCooldown() public {
        // Set a non-zero cooldown via governance
        vm.prank(admin);
        reg.setMultiaddrUpdateCooldown(1 hours);

        (Vm.Wallet memory wallet,,) = _registerAt(0);

        vm.warp(block.timestamp + 30 minutes);
        vm.prank(wallet.addr);
        vm.expectRevert();
        reg.updateMultiaddrs(hex"aa");

        vm.warp(block.timestamp + 31 minutes); // past cooldown
        vm.prank(wallet.addr);
        reg.updateMultiaddrs(hex"aa");
    }

    function test_updateMultiaddrs_revertsWhenNotActive() public {
        vm.prank(operator);
        vm.expectRevert(StakingRegistry.NodeNotActive.selector);
        reg.updateMultiaddrs(hex"");
    }

    // -----------------------------------------------------------------
    // Slash: tier escalation
    // -----------------------------------------------------------------

    function test_slash_tier1_is5Percent() public {
        _stake(operator, 100_000e18);
        uint256 amount = _slash(operator);
        assertEq(amount, 5000e18, "1st offense = 5%");
        assertEq(reg.lifetimeOffenseCount(operator), 1);
    }

    function test_slash_tier2_is15Percent() public {
        _stake(operator, 100_000e18);
        _slash(operator);
        uint256 amount = _slash(operator);
        assertEq(amount, 14_250e18, "2nd offense = 15% of remaining 95_000");
    }

    function test_slash_tier3_is50Percent() public {
        _stake(operator, 100_000e18);
        _slash(operator);
        _slash(operator);
        uint256 amount = _slash(operator);
        assertEq(amount, 40_375e18, "3rd offense = 50% of remaining 80_750");
    }

    function test_slash_tier3Stays50PercentBeyondThird() public {
        _stake(operator, 1_000_000e18);
        for (uint256 i = 0; i < 5; i++) {
            _slash(operator);
        }
        assertEq(reg.lifetimeOffenseCount(operator), 5);
    }

    // -----------------------------------------------------------------
    // Slash: 50/30/20 split
    // -----------------------------------------------------------------

    function test_slash_distributes50_30_20() public {
        _stake(operator, 100_000e18);
        uint256 supplyBefore = token.totalSupply();
        uint256 challengerBefore = token.balanceOf(challenger);
        uint256 safetyBefore = token.balanceOf(address(safetyReserve));

        vm.prank(slashJudge);
        uint256 slashed = reg.slash(operator, challenger, 0);

        assertEq(slashed, 5000e18);
        assertEq(token.balanceOf(challenger), challengerBefore + 2500e18);
        assertEq(token.balanceOf(address(safetyReserve)), safetyBefore + 1500e18);
        assertEq(token.totalSupply(), supplyBefore - 1000e18);
    }

    function test_slash_notifiesSafetyReserve() public {
        _stake(operator, 100_000e18);
        vm.prank(slashJudge);
        reg.slash(operator, challenger, 0);

        assertEq(safetyReserve.inflowCount(), 1);
        (address inflowOp, uint256 inflowAmount) = safetyReserve.inflows(0);
        assertEq(inflowOp, operator);
        assertEq(inflowAmount, 1500e18);
    }

    // -----------------------------------------------------------------
    // Slash: applies to active + unbonding
    // -----------------------------------------------------------------

    function test_slash_appliesToActivePlusUnbonding() public {
        _stake(operator, 100_000e18);
        vm.prank(operator);
        reg.requestUnstake(40_000e18);

        vm.prank(slashJudge);
        uint256 slashed = reg.slash(operator, challenger, 0);

        assertEq(slashed, 5000e18);
        assertEq(reg.activeStake(operator), 55_000e18);
        (uint256 unbondingAmount,) = reg.unbondingOf(operator);
        assertEq(unbondingAmount, 40_000e18);
    }

    function test_slash_drawsFromUnbondingWhenActiveInsufficient() public {
        _stake(operator, 100_000e18);
        vm.prank(operator);
        reg.requestUnstake(99_000e18);

        vm.prank(slashJudge);
        uint256 slashed = reg.slash(operator, challenger, 0);

        assertEq(slashed, 5000e18);
        assertEq(reg.activeStake(operator), 0);
        (uint256 unbondingAmount,) = reg.unbondingOf(operator);
        assertEq(unbondingAmount, 95_000e18);
    }

    // -----------------------------------------------------------------
    // Slash: auto-ejection (now also flips NodeInfo.active)
    // -----------------------------------------------------------------

    function test_slash_autoEjectAlsoFlipsNodeActive() public {
        // Register + over-slash to trigger auto-eject
        (Vm.Wallet memory wallet,,) = _registerAt(0);
        // Operator from _registerAt is wallet.addr; stake them up significantly
        vm.prank(admin);
        assertTrue(token.transfer(wallet.addr, 10 * MIN_STAKE));
        _stake(wallet.addr, 9 * MIN_STAKE); // total 10x MIN_STAKE staked

        assertEq(reg.getActiveNodeCount(), 1);
        assertTrue(reg.isActive(wallet.addr));

        // Three slashes against 10x MIN_STAKE (500k):
        //   1st: 5% → -25k → 475k
        //   2nd: 15% → -71.25k → 403.75k
        //   3rd: 50% → -201.875k → 201.875k (still above MIN_STAKE/2 = 25k)
        // Need many slashes. Use a higher minStake floor instead: bump
        // minStake far above current stake.
        vm.prank(admin);
        reg.setMinStake(1_000_000e18); // raise min so we're already below 50%

        // Trigger a slash to fire the auto-eject path
        vm.prank(slashJudge);
        reg.slash(wallet.addr, challenger, 0);

        assertTrue(reg.ejected(wallet.addr));
        assertEq(reg.getActiveNodeCount(), 0, "auto-eject removes from active set");
        StakingRegistry.NodeInfo memory info = reg.getNodeByAddress(wallet.addr);
        assertFalse(info.active);
    }

    // -----------------------------------------------------------------
    // Slash: access control + wiring
    // -----------------------------------------------------------------

    function test_slash_revertsWithoutSlashRole() public {
        _stake(operator, MIN_STAKE);
        vm.expectRevert();
        reg.slash(operator, challenger, 0);
    }

    function test_slash_revertsWhenSafetyReserveNotWired() public {
        vm.prank(admin);
        reg.setSafetyReserve(ISafetyReserve(address(0)));
        _stake(operator, MIN_STAKE);
        vm.prank(slashJudge);
        vm.expectRevert(StakingRegistry.SafetyReserveNotWired.selector);
        reg.slash(operator, challenger, 0);
    }

    function test_slash_revertsOnZeroChallenger() public {
        _stake(operator, MIN_STAKE);
        vm.prank(slashJudge);
        vm.expectRevert(StakingRegistry.ZeroAddress.selector);
        reg.slash(operator, address(0), 0);
    }

    // -----------------------------------------------------------------
    // Blacklist ejection — also flips NodeInfo.active when registered
    // -----------------------------------------------------------------

    function test_ejectNode_flipsBothFlagsWhenRegistered() public {
        (Vm.Wallet memory wallet, bytes32 nodeId,) = _registerAt(0);
        assertEq(reg.getActiveNodeCount(), 1);
        assertEq(reg.registrationNonce(nodeId), 0);

        vm.prank(blacklist);
        reg.ejectNode(wallet.addr);

        assertTrue(reg.ejected(wallet.addr));
        assertEq(reg.getActiveNodeCount(), 0);
        StakingRegistry.NodeInfo memory info = reg.getNodeByAddress(wallet.addr);
        assertFalse(info.active);
        // Eject must bump registrationNonce, same as deregisterNode, so a
        // stale ed25519 registration signature can't be replayed on re-register.
        assertEq(reg.registrationNonce(nodeId), 1, "eject bumps registrationNonce");
    }

    function test_slashAutoEject_bumpsRegistrationNonce() public {
        (Vm.Wallet memory wallet, bytes32 nodeId,) = _registerAt(0);
        assertEq(reg.registrationNonce(nodeId), 0);

        // Raise minStake far above current stake so the next slash auto-ejects.
        vm.prank(admin);
        reg.setMinStake(1_000_000e18);
        vm.prank(slashJudge);
        reg.slash(wallet.addr, challenger, 0);

        assertTrue(reg.ejected(wallet.addr));
        assertEq(reg.registrationNonce(nodeId), 1, "slash auto-eject bumps registrationNonce");
    }

    function test_ejectNode_unregisteredOperator_onlySetsFlag() public {
        _stake(operator, MIN_STAKE);
        vm.prank(blacklist);
        reg.ejectNode(operator);

        assertTrue(reg.ejected(operator));
        assertEq(reg.getActiveNodeCount(), 0);
    }

    function test_ejectNode_revertsWithoutBlacklistRole() public {
        vm.expectRevert();
        reg.ejectNode(operator);
    }

    // -----------------------------------------------------------------
    // Settlement recording
    // -----------------------------------------------------------------

    function test_recordSettlement_updatesTimestamp() public {
        vm.prank(feeRouter);
        reg.recordSettlement(operator);
        assertEq(reg.lastSettlementAt(operator), block.timestamp);
    }

    function test_recordSettlement_revertsWithoutReporterRole() public {
        vm.expectRevert();
        reg.recordSettlement(operator);
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function test_setMinStake_revertsOutOfBounds() public {
        vm.prank(admin);
        vm.expectRevert();
        reg.setMinStake(1e18);
        vm.prank(admin);
        vm.expectRevert();
        reg.setMinStake(2_000_000e18);
    }

    function test_setUnbondingPeriod_revertsOutOfBounds() public {
        vm.prank(admin);
        vm.expectRevert();
        reg.setUnbondingPeriod(2 days);
        vm.prank(admin);
        vm.expectRevert();
        reg.setUnbondingPeriod(60 days);
    }

    function test_setMinStake_withinBoundsSucceeds() public {
        vm.prank(admin);
        reg.setMinStake(100_000e18);
        assertEq(reg.minStake(), 100_000e18);
    }

    function test_setMaxMultiaddrSize_withinBounds() public {
        vm.prank(admin);
        reg.setMaxMultiaddrSize(256);
        assertEq(reg.maxMultiaddrSize(), 256);
    }

    // -----------------------------------------------------------------
    // Pagination view
    // -----------------------------------------------------------------

    function test_getActiveNodes_paginationBounds() public {
        // Register 3 operators
        bytes32 n1 = bytes32(uint256(0x1));
        bytes32 n2 = bytes32(uint256(0x2));
        bytes32 n3 = bytes32(uint256(0x3));
        _registerWalletAndNodeId("a", n1);
        _registerWalletAndNodeId("b", n2);
        _registerWalletAndNodeId("c", n3);

        assertEq(reg.getActiveNodeCount(), 3);
        StakingRegistry.NodeInfo[] memory page = reg.getActiveNodes(0, 10);
        assertEq(page.length, 3);

        StakingRegistry.NodeInfo[] memory page2 = reg.getActiveNodes(1, 1);
        assertEq(page2.length, 1);

        // offset >= len → empty
        StakingRegistry.NodeInfo[] memory empty = reg.getActiveNodes(5, 10);
        assertEq(empty.length, 0);
    }

    // -----------------------------------------------------------------
    // Fuzz: slash math always balances
    // -----------------------------------------------------------------

    function testFuzz_slash_threeLegsSumToSlashAmount(uint256 stakeAmount) public {
        stakeAmount = bound(stakeAmount, MIN_STAKE, 1_000_000e18);
        _stake(operator, stakeAmount);

        uint256 supplyBefore = token.totalSupply();
        uint256 challengerBefore = token.balanceOf(challenger);
        uint256 safetyBefore = token.balanceOf(address(safetyReserve));

        vm.prank(slashJudge);
        uint256 slashed = reg.slash(operator, challenger, 0);

        uint256 challengerDelta = token.balanceOf(challenger) - challengerBefore;
        uint256 safetyDelta = token.balanceOf(address(safetyReserve)) - safetyBefore;
        uint256 burned = supplyBefore - token.totalSupply();

        assertEq(challengerDelta + safetyDelta + burned, slashed);
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    function _slash(address op) internal returns (uint256) {
        vm.prank(slashJudge);
        return reg.slash(op, challenger, 0);
    }

    function _fundAndStake(address who, uint256 amount) internal {
        vm.prank(admin);
        assertTrue(token.transfer(who, amount));
        vm.startPrank(who);
        token.approve(address(reg), amount);
        reg.stake(amount);
        vm.stopPrank();
    }

    /// @notice Build a fresh wallet + nodeId + a valid EIP-712 BindNodeId signature.
    function _prepareRegistration(uint256 salt)
        internal
        returns (Vm.Wallet memory wallet, bytes32 nodeId, bytes memory bindingSig)
    {
        wallet = vm.createWallet(string(abi.encodePacked("op-", vm.toString(salt))));
        nodeId = bytes32(uint256(keccak256(abi.encodePacked("nodeId", salt))));
        bindingSig = _signBindNode(wallet, nodeId, 0);
    }

    function _prepareRegistrationFor(Vm.Wallet memory wallet, bytes32 nodeId, uint64 nonce)
        internal
        view
        returns (Vm.Wallet memory, bytes32, bytes memory bindingSig)
    {
        bindingSig = _signBindNode(wallet, nodeId, nonce);
        return (wallet, nodeId, bindingSig);
    }

    function _signBindNode(Vm.Wallet memory wallet, bytes32 nodeId, uint64 nonce) internal view returns (bytes memory) {
        bytes32 structHash = keccak256(abi.encode(reg.BIND_NODE_TYPEHASH(), nodeId, nonce));
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", _domainSeparator(), structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(wallet, digest);
        return abi.encodePacked(r, s, v);
    }

    function _domainSeparator() internal view returns (bytes32) {
        return keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes("StakingRegistry")),
                keccak256(bytes("1")),
                block.chainid,
                address(reg)
            )
        );
    }

    /// @notice Register a fresh operator under the given seed name + nodeId.
    function _registerWalletAndNodeId(string memory seed, bytes32 nodeId) internal returns (Vm.Wallet memory wallet) {
        wallet = vm.createWallet(seed);
        _fundAndStake(wallet.addr, MIN_STAKE);
        bytes memory sig = _signBindNode(wallet, nodeId, 0);
        vm.prank(wallet.addr);
        reg.registerNode(nodeId, hex"", "", sig, hex"");
    }

    /// @notice Convenience for tests that need a single registered operator.
    function _registerAt(uint256 salt)
        internal
        returns (Vm.Wallet memory wallet, bytes32 nodeId, bytes memory bindingSig)
    {
        (wallet, nodeId, bindingSig) = _prepareRegistration(salt);
        _fundAndStake(wallet.addr, MIN_STAKE);
        vm.prank(wallet.addr);
        reg.registerNode(nodeId, hex"", "", bindingSig, hex"");
    }
}
