// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Ownable } from "@openzeppelin/contracts/access/Ownable.sol";

import { TOKEN } from "../src/TOKEN.sol";
import { StakingRegistry } from "../src/StakingRegistry.sol";
import { StablePaymentChannel } from "../src/StablePaymentChannel.sol";
import { Errors } from "../src/libraries/Errors.sol";
import { Roles } from "../src/libraries/Roles.sol";

import { MockUSDC } from "./mocks/MockUSDC.sol";

contract StablePaymentChannelTest is Test {
    TOKEN internal token;
    MockUSDC internal usdc;
    StakingRegistry internal reg;
    StablePaymentChannel internal ch;

    address internal admin = makeAddr("admin");
    address internal treasury = makeAddr("treasury");
    uint256 internal clientPk = 0xC117;
    address internal client;
    address internal provider = makeAddr("provider");
    address internal randomUser = makeAddr("randomUser");

    uint256 internal constant MIN_STAKE = 1000e18;
    uint64 internal constant DISPUTE_WINDOW = 48 hours;
    uint64 internal constant MAX_DURATION = 90 days;

    function setUp() public {
        client = vm.addr(clientPk);

        token = new TOKEN(address(this), 10_000_000e18, address(this));
        usdc = new MockUSDC();
        reg = new StakingRegistry(token, MIN_STAKE, 7 days, 90 days, admin);
        ch = new StablePaymentChannel(
            usdc, reg, treasury, admin, 300, 150, 10, DISPUTE_WINDOW, MAX_DURATION, 1, 1000
        );

        vm.startPrank(admin);
        // The deploy script grants this in production (ADR 016 §2 + Deploy.s.sol).
        // Tests must mirror that wiring or settleChannel reverts on the
        // recordSettlement callback for any non-zero claim.
        reg.grantRole(Roles.SETTLEMENT_REPORTER_ROLE, address(ch));
        reg.unpause();
        vm.stopPrank();

        usdc.mint(client, 1_000_000e6);
        vm.prank(client);
        usdc.approve(address(ch), type(uint256).max);

        // Fund the provider enough to reach multi-stake tier for discount tests.
        token.transfer(provider, 100_000e18);
        vm.prank(provider);
        token.approve(address(reg), type(uint256).max);
    }

    // ---------------- constructor ----------------

    function test_Constructor_Storage() public view {
        assertEq(address(ch.USDC()), address(usdc));
        assertEq(address(ch.STAKING_REGISTRY()), address(reg));
        assertEq(ch.treasury(), treasury);
        assertEq(ch.feeBps(), 300);
        assertEq(ch.discountedFeeBps(), 150);
        assertEq(ch.discountStakeMultiple(), 10);
        assertEq(ch.disputeWindow(), DISPUTE_WINDOW);
        assertEq(ch.maxChannelDuration(), MAX_DURATION);
        assertEq(ch.owner(), admin);
    }

    function test_Constructor_RevertsOnBadParams() public {
        vm.expectRevert(Errors.ZeroAddress.selector);
        new StablePaymentChannel(
            MockUSDC(address(0)),
            reg,
            treasury,
            admin,
            300,
            150,
            10,
            DISPUTE_WINDOW,
            MAX_DURATION,
            1,
            1000
        );
        vm.expectRevert(Errors.OutOfBounds.selector);
        new StablePaymentChannel(
            usdc, reg, treasury, admin, 2001, 150, 10, DISPUTE_WINDOW, MAX_DURATION, 1, 1000
        );
        vm.expectRevert(Errors.OutOfBounds.selector);
        new StablePaymentChannel(
            usdc, reg, treasury, admin, 300, 400, 10, DISPUTE_WINDOW, MAX_DURATION, 1, 1000
        );
        vm.expectRevert(Errors.OutOfBounds.selector);
        new StablePaymentChannel(
            usdc, reg, treasury, admin, 300, 150, 0, DISPUTE_WINDOW, MAX_DURATION, 1, 1000
        );
        vm.expectRevert(Errors.OutOfBounds.selector);
        new StablePaymentChannel(
            usdc, reg, treasury, admin, 300, 150, 10, 1 hours, MAX_DURATION, 1, 1000
        );
        vm.expectRevert(Errors.OutOfBounds.selector);
        new StablePaymentChannel(
            usdc, reg, treasury, admin, 300, 150, 10, DISPUTE_WINDOW, 1 days, 1, 1000
        );
        // Floor below safety minimum (RATE_FLOOR_MIN = 1).
        vm.expectRevert(Errors.OutOfBounds.selector);
        new StablePaymentChannel(
            usdc, reg, treasury, admin, 300, 150, 10, DISPUTE_WINDOW, MAX_DURATION, 0, 1000
        );
        // Ceiling above safety maximum (RATE_CEILING_MAX = 10_000).
        vm.expectRevert(Errors.OutOfBounds.selector);
        new StablePaymentChannel(
            usdc, reg, treasury, admin, 300, 150, 10, DISPUTE_WINDOW, MAX_DURATION, 1, 10_001
        );
        // Floor > ceiling.
        vm.expectRevert(Errors.OutOfBounds.selector);
        new StablePaymentChannel(
            usdc, reg, treasury, admin, 300, 150, 10, DISPUTE_WINDOW, MAX_DURATION, 500, 100
        );
    }

    // ---------------- open / topUp ----------------

    function test_OpenChannel_HappyPath() public {
        bytes32 expected = ch.nextChannelId(client, provider);
        vm.prank(client);
        bytes32 id = ch.openChannel(provider, 1000e6);
        assertEq(id, expected);
        StablePaymentChannel.Channel memory c = ch.getChannel(id);
        assertEq(uint256(c.status), uint256(StablePaymentChannel.Status.Open));
        assertEq(c.client, client);
        assertEq(c.provider, provider);
        assertEq(c.deposit, 1000e6);
        assertEq(usdc.balanceOf(address(ch)), 1000e6);
    }

    function test_OpenChannel_IncrementsNonce() public {
        vm.prank(client);
        bytes32 a = ch.openChannel(provider, 100e6);
        vm.prank(client);
        bytes32 b = ch.openChannel(provider, 100e6);
        assertTrue(a != b);
        assertEq(ch.clientChannelNonce(client), 2);
    }

    function test_OpenChannel_RevertsBadArgs() public {
        vm.expectRevert(Errors.ZeroAddress.selector);
        vm.prank(client);
        ch.openChannel(address(0), 100e6);
        vm.expectRevert(Errors.ZeroAmount.selector);
        vm.prank(client);
        ch.openChannel(provider, 0);
    }

    function test_TopUp_IncreasesDeposit() public {
        vm.prank(client);
        bytes32 id = ch.openChannel(provider, 100e6);
        vm.prank(client);
        ch.topUp(id, 50e6);
        assertEq(ch.getChannel(id).deposit, 150e6);
    }

    function test_TopUp_NotClientReverts() public {
        vm.prank(client);
        bytes32 id = ch.openChannel(provider, 100e6);
        vm.expectRevert(StablePaymentChannel.NotParticipant.selector);
        vm.prank(provider);
        ch.topUp(id, 50e6);
    }

    // ---------------- close / dispute / settle ----------------

    function test_Close_AcceptsSignedVoucher() public {
        bytes32 id = _openChannel(1000e6);

        (uint256 amt, uint256 n) = (600e6, 1);
        bytes memory sig = _signVoucher(clientPk, id, amt, n);

        vm.prank(provider);
        ch.closeChannel(id, amt, n, sig);

        StablePaymentChannel.Channel memory c = ch.getChannel(id);
        assertEq(uint256(c.status), uint256(StablePaymentChannel.Status.Closing));
        assertEq(c.claimedAmount, amt);
        assertEq(c.claimedNonce, n);
        assertEq(c.disputeDeadline, block.timestamp + DISPUTE_WINDOW);
    }

    function test_Close_ProviderZeroVoucherAllowed() public {
        bytes32 id = _openChannel(500e6);
        vm.prank(provider);
        ch.closeChannel(id, 0, 0, "");
        StablePaymentChannel.Channel memory c = ch.getChannel(id);
        assertEq(uint256(c.status), uint256(StablePaymentChannel.Status.Closing));
        assertEq(c.claimedAmount, 0);
    }

    function test_Close_ClientCannotZeroVoucher() public {
        bytes32 id = _openChannel(500e6);
        vm.expectRevert(Errors.InvalidSignature.selector);
        vm.prank(client);
        ch.closeChannel(id, 0, 0, "");
    }

    function test_Close_RejectsBadSignature() public {
        bytes32 id = _openChannel(1000e6);
        bytes memory sig = _signVoucher(0xDEADBEEF, id, 500e6, 1);
        vm.expectRevert(Errors.InvalidSignature.selector);
        vm.prank(provider);
        ch.closeChannel(id, 500e6, 1, sig);
    }

    function test_Close_RejectsAmountExceedsDeposit() public {
        bytes32 id = _openChannel(500e6);
        bytes memory sig = _signVoucher(clientPk, id, 501e6, 1);
        vm.expectRevert(StablePaymentChannel.AmountExceedsDeposit.selector);
        vm.prank(provider);
        ch.closeChannel(id, 501e6, 1, sig);
    }

    function test_Close_NotParticipantReverts() public {
        bytes32 id = _openChannel(500e6);
        bytes memory sig = _signVoucher(clientPk, id, 100e6, 1);
        vm.expectRevert(StablePaymentChannel.NotParticipant.selector);
        vm.prank(randomUser);
        ch.closeChannel(id, 100e6, 1, sig);
    }

    function test_Dispute_HigherNonceUpdates() public {
        bytes32 id = _openChannel(1000e6);
        // close with lower voucher
        bytes memory sigLow = _signVoucher(clientPk, id, 200e6, 1);
        vm.prank(provider);
        ch.closeChannel(id, 200e6, 1, sigLow);

        bytes memory sigHigh = _signVoucher(clientPk, id, 500e6, 2);
        // watchtower (random address) disputes
        vm.prank(randomUser);
        ch.disputeChannel(id, 500e6, 2, sigHigh);

        assertEq(ch.getChannel(id).claimedAmount, 500e6);
        assertEq(ch.getChannel(id).claimedNonce, 2);
    }

    function test_Dispute_LowerNonceReverts() public {
        bytes32 id = _openChannel(1000e6);
        bytes memory s2 = _signVoucher(clientPk, id, 500e6, 2);
        vm.prank(provider);
        ch.closeChannel(id, 500e6, 2, s2);

        bytes memory s1 = _signVoucher(clientPk, id, 200e6, 1);
        vm.expectRevert(StablePaymentChannel.NonceNotIncreasing.selector);
        vm.prank(randomUser);
        ch.disputeChannel(id, 200e6, 1, s1);
    }

    function test_Dispute_AfterDeadlineReverts() public {
        bytes32 id = _openChannel(1000e6);
        bytes memory s1 = _signVoucher(clientPk, id, 100e6, 1);
        vm.prank(provider);
        ch.closeChannel(id, 100e6, 1, s1);
        vm.warp(block.timestamp + DISPUTE_WINDOW + 1);

        bytes memory s2 = _signVoucher(clientPk, id, 200e6, 2);
        vm.expectRevert(StablePaymentChannel.DisputeWindowClosed.selector);
        ch.disputeChannel(id, 200e6, 2, s2);
    }

    function test_Settle_DistributesWithBaseFee() public {
        bytes32 id = _openChannel(1000e6);
        uint256 claimed = 800e6;
        bytes memory sig = _signVoucher(clientPk, id, claimed, 1);
        vm.prank(provider);
        ch.closeChannel(id, claimed, 1, sig);
        vm.warp(block.timestamp + DISPUTE_WINDOW + 1);

        uint256 pBefore = usdc.balanceOf(provider);
        uint256 tBefore = usdc.balanceOf(treasury);
        uint256 cBefore = usdc.balanceOf(client);

        ch.settleChannel(id);

        uint256 fee = (claimed * 300) / 10_000; // 3%
        assertEq(usdc.balanceOf(provider) - pBefore, claimed - fee);
        assertEq(usdc.balanceOf(treasury) - tBefore, fee);
        assertEq(usdc.balanceOf(client) - cBefore, 1000e6 - claimed);
        assertEq(uint256(ch.getChannel(id).status), uint256(StablePaymentChannel.Status.Closed));
    }

    function test_Settle_UsesDiscountedFeeWhenProviderStaked() public {
        // Provider stakes 10× MIN_STAKE (threshold).
        vm.prank(provider);
        reg.stake(10 * MIN_STAKE);

        bytes32 id = _openChannel(1000e6);
        uint256 claimed = 500e6;
        bytes memory sig = _signVoucher(clientPk, id, claimed, 1);
        vm.prank(provider);
        ch.closeChannel(id, claimed, 1, sig);
        vm.warp(block.timestamp + DISPUTE_WINDOW + 1);

        uint256 tBefore = usdc.balanceOf(treasury);
        ch.settleChannel(id);

        uint256 fee = (claimed * 150) / 10_000; // 1.5%
        assertEq(usdc.balanceOf(treasury) - tBefore, fee);
    }

    function test_Settle_BeforeDeadlineReverts() public {
        bytes32 id = _openChannel(1000e6);
        bytes memory sig = _signVoucher(clientPk, id, 100e6, 1);
        vm.prank(provider);
        ch.closeChannel(id, 100e6, 1, sig);
        vm.expectRevert(StablePaymentChannel.DisputeWindowOpen.selector);
        ch.settleChannel(id);
    }

    function test_Settle_StampsLastSettlementAt() public {
        // Settlement of a paying channel stamps the registry so off-chain
        // clients can rank cold-start candidates by recent delivery
        // (ADR 016 §3 Off-Chain Read API).
        bytes32 id = _openChannel(1000e6);
        bytes memory sig = _signVoucher(clientPk, id, 800e6, 1);
        vm.prank(provider);
        ch.closeChannel(id, 800e6, 1, sig);
        vm.warp(block.timestamp + DISPUTE_WINDOW + 1);

        assertEq(reg.lastSettlementAt(provider), 0);
        ch.settleChannel(id);
        assertEq(reg.lastSettlementAt(provider), uint64(block.timestamp));
    }

    function test_Settle_DoesNotStampOnZeroClaim() public {
        // A zero-claim settlement carries no liveness signal, so we don't
        // stamp the registry. Avoids polluting the bootstrap signal with
        // channels that opened and closed without delivering anything.
        // Provider closes with the zero-voucher shortcut (claimedAmount=0,
        // nonce=0 path) which channelClose explicitly allows.
        bytes32 id = _openChannel(1000e6);
        vm.prank(provider);
        ch.closeChannel(id, 0, 0, "");
        vm.warp(block.timestamp + DISPUTE_WINDOW + 1);

        ch.settleChannel(id);
        assertEq(reg.lastSettlementAt(provider), 0);
    }

    // ---------------- reclaimExpired ----------------

    function test_ReclaimExpired_RefundsFullDeposit() public {
        bytes32 id = _openChannel(500e6);
        vm.warp(block.timestamp + MAX_DURATION + 1);
        uint256 before = usdc.balanceOf(client);
        vm.prank(client);
        ch.reclaimExpired(id);
        assertEq(usdc.balanceOf(client) - before, 500e6);
        assertEq(uint256(ch.getChannel(id).status), uint256(StablePaymentChannel.Status.Closed));
    }

    function test_ReclaimExpired_BeforeExpiryReverts() public {
        bytes32 id = _openChannel(500e6);
        vm.expectRevert(StablePaymentChannel.NotYetExpired.selector);
        vm.prank(client);
        ch.reclaimExpired(id);
    }

    // ---------------- pausable sweep ----------------

    function test_Pause_BlocksAllMutators() public {
        bytes32 id = _openChannel(500e6);

        vm.prank(admin);
        ch.pause();

        bytes4 paused = bytes4(keccak256("EnforcedPause()"));

        vm.expectRevert(paused);
        vm.prank(client);
        ch.openChannel(provider, 100e6);

        vm.expectRevert(paused);
        vm.prank(client);
        ch.topUp(id, 100e6);

        vm.expectRevert(paused);
        vm.prank(provider);
        ch.closeChannel(id, 0, 0, "");

        vm.expectRevert(paused);
        vm.prank(provider);
        ch.disputeChannel(id, 0, 0, "");

        vm.expectRevert(paused);
        ch.settleChannel(id);

        vm.expectRevert(paused);
        vm.prank(client);
        ch.reclaimExpired(id);
    }

    // ---------------- max active challenges (not in this file) ----------------

    // ---------------- governance ----------------

    function test_SetFeeParams_Bounds() public {
        vm.prank(admin);
        ch.setFeeParams(500, 250, 20);
        assertEq(ch.feeBps(), 500);
        assertEq(ch.discountedFeeBps(), 250);
        assertEq(ch.discountStakeMultiple(), 20);

        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        ch.setFeeParams(2001, 250, 20);
    }

    function test_OnlyOwnerGuards() public {
        vm.expectRevert(
            abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, randomUser)
        );
        vm.prank(randomUser);
        ch.setFeeParams(500, 250, 20);
    }

    function test_SetDisputeWindow_Updates() public {
        vm.prank(admin);
        ch.setDisputeWindow(36 hours);
        assertEq(ch.disputeWindow(), 36 hours);
    }

    function test_SetDisputeWindow_RejectsBelowFloor() public {
        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        ch.setDisputeWindow(11 hours);
    }

    function test_SetDisputeWindow_RejectsAboveCeiling() public {
        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        ch.setDisputeWindow(73 hours);
    }

    function test_SetDisputeWindow_OnlyOwner() public {
        vm.expectRevert(
            abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, randomUser)
        );
        vm.prank(randomUser);
        ch.setDisputeWindow(36 hours);
    }

    function test_SetMaxChannelDuration_Updates() public {
        vm.prank(admin);
        ch.setMaxChannelDuration(60 days);
        assertEq(ch.maxChannelDuration(), 60 days);
    }

    function test_SetMaxChannelDuration_RejectsBelowFloor() public {
        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        ch.setMaxChannelDuration(6 days);
    }

    function test_SetMaxChannelDuration_RejectsAboveCeiling() public {
        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        ch.setMaxChannelDuration(366 days);
    }

    function test_SetTreasury_Updates() public {
        address newTreasury = makeAddr("newTreasury");
        vm.prank(admin);
        ch.setTreasury(newTreasury);
        assertEq(ch.treasury(), newTreasury);
    }

    function test_SetTreasury_RejectsZero() public {
        vm.expectRevert(Errors.ZeroAddress.selector);
        vm.prank(admin);
        ch.setTreasury(address(0));
    }

    function test_Unpause_RestoresMutators() public {
        vm.startPrank(admin);
        ch.pause();
        ch.unpause();
        vm.stopPrank();
        // openChannel works again — proves the unpause path executed.
        vm.prank(client);
        ch.openChannel(provider, 100e6);
    }

    function test_GetChannel_ReturnsZeroForUnknown() public view {
        StablePaymentChannel.Channel memory c = ch.getChannel(bytes32(uint256(0x1234)));
        assertEq(c.client, address(0));
        assertEq(uint256(c.status), uint256(StablePaymentChannel.Status.None));
    }

    function test_ChannelClient_ReturnsClient() public {
        bytes32 id = _openChannel(100e6);
        assertEq(ch.channelClient(id), client);
    }

    function test_ChannelClient_ReturnsZeroForUnknown() public view {
        assertEq(ch.channelClient(bytes32(uint256(0x9999))), address(0));
    }

    function test_GetRateBounds_ReturnsConstructorValues() public view {
        (uint256 floor, uint256 ceiling) = ch.getRateBounds();
        assertEq(floor, 1);
        assertEq(ceiling, 1000);
        assertEq(ch.deliveryFloor(), 1);
        assertEq(ch.deliveryCeiling(), 1000);
    }

    function test_SetRateBounds_Updates() public {
        vm.expectEmit(true, true, true, true);
        emit StablePaymentChannel.RateBoundsUpdated(5, 2000);
        vm.prank(admin);
        ch.setRateBounds(5, 2000);
        (uint256 floor, uint256 ceiling) = ch.getRateBounds();
        assertEq(floor, 5);
        assertEq(ceiling, 2000);
    }

    function test_SetRateBounds_RejectsBelowFloorMin() public {
        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        ch.setRateBounds(0, 1000);
    }

    function test_SetRateBounds_RejectsAboveCeilingMax() public {
        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        ch.setRateBounds(1, 10_001);
    }

    function test_SetRateBounds_RejectsInvertedBounds() public {
        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        ch.setRateBounds(500, 100);
    }

    function test_SetRateBounds_OnlyOwner() public {
        vm.expectRevert(
            abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, randomUser)
        );
        vm.prank(randomUser);
        ch.setRateBounds(5, 2000);
    }

    function test_NextChannelId_AdvancesAfterOpen() public {
        bytes32 idBefore = ch.nextChannelId(client, provider);
        bytes32 actual = _openChannel(100e6);
        assertEq(actual, idBefore);
        bytes32 idAfter = ch.nextChannelId(client, provider);
        assertTrue(idBefore != idAfter, "nonce should advance after open");
    }

    // ---------------- fuzz ----------------

    function testFuzz_Settle_FeeMath(
        uint256 deposit,
        uint256 claimed,
        uint256 feeBps_
    ) public {
        deposit = bound(deposit, 1e6, 1_000_000e6);
        claimed = bound(claimed, 1, deposit);
        feeBps_ = bound(feeBps_, 0, 2000);

        usdc.mint(client, deposit);
        vm.prank(admin);
        ch.setFeeParams(feeBps_, feeBps_ > 0 ? feeBps_ / 2 : 0, 10);

        vm.prank(client);
        bytes32 id = ch.openChannel(provider, deposit);
        bytes memory sig = _signVoucher(clientPk, id, claimed, 1);
        vm.prank(provider);
        ch.closeChannel(id, claimed, 1, sig);
        vm.warp(block.timestamp + DISPUTE_WINDOW + 1);

        uint256 pBefore = usdc.balanceOf(provider);
        uint256 tBefore = usdc.balanceOf(treasury);
        uint256 cBefore = usdc.balanceOf(client);
        ch.settleChannel(id);
        uint256 fee = (claimed * feeBps_) / 10_000;
        assertEq(usdc.balanceOf(provider) - pBefore, claimed - fee);
        assertEq(usdc.balanceOf(treasury) - tBefore, fee);
        assertEq(usdc.balanceOf(client) - cBefore, deposit - claimed);
    }

    // ---------------- helpers ----------------

    function _openChannel(
        uint256 deposit
    ) internal returns (bytes32 id) {
        vm.prank(client);
        id = ch.openChannel(provider, deposit);
    }

    function _signVoucher(
        uint256 pk,
        bytes32 channelId,
        uint256 amount,
        uint256 nonce
    ) internal view returns (bytes memory) {
        bytes32 structHash =
            keccak256(abi.encode(ch.VOUCHER_TYPEHASH(), channelId, amount, nonce, address(usdc)));
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", ch.domainSeparator(), structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(pk, digest);
        return abi.encodePacked(r, s, v);
    }
}
