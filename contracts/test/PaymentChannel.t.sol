// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";

import { PaymentChannel } from "../src/PaymentChannel.sol";
import { IFeeRouterSettlement } from "../src/interfaces/IFeeRouterSettlement.sol";
import { ICapacityBondActivity } from "../src/interfaces/ICapacityBondActivity.sol";

contract MockUSDC is ERC20 {
    constructor() ERC20("USDC", "USDC") {
        _mint(msg.sender, 1_000_000_000e6);
    }

    function decimals() public pure override returns (uint8) {
        return 6;
    }
}

/// @notice Configurable `isActive` registry stand-in.
contract MockActiveBond is ICapacityBondActivity {
    mapping(address => bool) internal _active;

    function setActive(address operator, bool a) external {
        _active[operator] = a;
    }

    function isActive(address operator) external view override returns (bool) {
        return _active[operator];
    }
}

/// @notice Records `routeSettlement` calls and pulls USDC exactly like the real
///         `FeeRouter` (so the channel's `forceApprove` + delta accounting is
///         exercised), and reverts on a zero amount as the real router does.
contract MockSettlementRouter is IFeeRouterSettlement {
    IERC20 public immutable usdc;

    struct RouteCall {
        address operator;
        uint256 bytesDelivered;
        uint256 amount;
    }

    RouteCall[] public calls;

    constructor(IERC20 usdc_) {
        usdc = usdc_;
    }

    function routeSettlement(address operator, uint256 bytesDelivered, uint256 amount) external override {
        require(amount != 0, "MockSettlementRouter: zero amount");
        usdc.transferFrom(msg.sender, address(this), amount);
        calls.push(RouteCall(operator, bytesDelivered, amount));
    }

    function callCount() external view returns (uint256) {
        return calls.length;
    }

    function totalRouted() external view returns (uint256 sum) {
        for (uint256 i = 0; i < calls.length; i++) {
            sum += calls[i].amount;
        }
    }

    function totalBytes() external view returns (uint256 sum) {
        for (uint256 i = 0; i < calls.length; i++) {
            sum += calls[i].bytesDelivered;
        }
    }
}

/// @notice Subclass exposing the forced-inclusion seam so the deadline-extension
///         branch (ADR 003 § L2 sequencer censorship) is testable.
contract ForcedInclusionHarness is PaymentChannel {
    bool public forced;

    constructor(
        IERC20 usdc_,
        ICapacityBondActivity capacityBond_,
        address feeRouter_,
        uint256 disputeWindow_,
        uint256 maxChannelDuration_,
        uint256 deliveryFloor_,
        uint256 deliveryCeiling_,
        address admin
    )
        PaymentChannel(
            usdc_,
            capacityBond_,
            feeRouter_,
            disputeWindow_,
            maxChannelDuration_,
            deliveryFloor_,
            deliveryCeiling_,
            admin
        )
    { }

    function setForced(bool f) external {
        forced = f;
    }

    function _arrivedViaForcedInclusion() internal view override returns (bool) {
        return forced;
    }
}

contract PaymentChannelTest is Test {
    MockUSDC internal usdc;
    MockActiveBond internal bond;
    MockSettlementRouter internal router;
    PaymentChannel internal channel;

    uint256 internal constant CLIENT_PK = 0xC11E27;
    address internal client;
    address internal provider = address(0xB0B);
    address internal admin = address(0xA11CE);
    address internal pauser = address(0xDEAD);
    address internal stranger = address(0x5747A);

    uint256 internal constant DISPUTE_WINDOW = 48 hours;
    uint256 internal constant MAX_DURATION = 90 days;
    uint256 internal constant DELIVERY_FLOOR = 1;
    uint256 internal constant DELIVERY_CEILING = 1000;
    uint256 internal constant DEPOSIT = 1000e6;

    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 internal constant PAUSER_ROLE = keccak256("PAUSER_ROLE");
    bytes32 internal constant VOUCHER_TYPEHASH =
        keccak256("Voucher(bytes32 channelId,uint256 amount,uint256 nonce,uint256 bytesDelivered,address token)");

    function setUp() public {
        client = vm.addr(CLIENT_PK);

        usdc = new MockUSDC();
        bond = new MockActiveBond();
        router = new MockSettlementRouter(usdc);
        bond.setActive(provider, true);

        channel = new PaymentChannel({
            usdc_: usdc,
            capacityBond_: bond,
            feeRouter_: address(router),
            disputeWindow_: DISPUTE_WINDOW,
            maxChannelDuration_: MAX_DURATION,
            deliveryFloor_: DELIVERY_FLOOR,
            deliveryCeiling_: DELIVERY_CEILING,
            admin: admin
        });

        vm.prank(admin);
        channel.grantRole(PAUSER_ROLE, pauser);

        // Fund the client and approve the channel.
        usdc.transfer(client, 100_000e6);
        vm.prank(client);
        usdc.approve(address(channel), type(uint256).max);
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    function _open() internal returns (bytes32 channelId) {
        vm.prank(client);
        channelId = channel.openChannel(provider, DEPOSIT);
    }

    function _signFor(
        address verifyingContract,
        bytes32 channelId,
        uint256 amount,
        uint256 nonce,
        uint256 bytesDelivered
    ) internal view returns (bytes memory) {
        bytes32 domainSeparator = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes("PaymentChannel")),
                keccak256(bytes("1")),
                block.chainid,
                verifyingContract
            )
        );
        bytes32 structHash =
            keccak256(abi.encode(VOUCHER_TYPEHASH, channelId, amount, nonce, bytesDelivered, address(usdc)));
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", domainSeparator, structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(CLIENT_PK, digest);
        return abi.encodePacked(r, s, v);
    }

    function _sign(bytes32 channelId, uint256 amount, uint256 nonce, uint256 bytesDelivered)
        internal
        view
        returns (bytes memory)
    {
        return _signFor(address(channel), channelId, amount, nonce, bytesDelivered);
    }

    // -----------------------------------------------------------------
    // openChannel
    // -----------------------------------------------------------------

    function test_openChannel_derivesIdAndIncrementsNonce() public {
        bytes32 expected = keccak256(abi.encodePacked(client, provider, uint256(0)));
        bytes32 channelId = _open();
        assertEq(channelId, expected);
        assertEq(channel.clientChannelNonce(client), 1);

        PaymentChannel.Channel memory ch = channel.getChannel(channelId);
        assertEq(ch.client, client);
        assertEq(ch.provider, provider);
        assertEq(ch.token, address(usdc));
        assertEq(ch.deposit, DEPOSIT);
        assertEq(ch.expiresAt, block.timestamp + MAX_DURATION);
        assertEq(usdc.balanceOf(address(channel)), DEPOSIT);
    }

    function test_openChannel_revertsOnInactiveProvider() public {
        bond.setActive(provider, false);
        vm.prank(client);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.ProviderNotActive.selector, provider));
        channel.openChannel(provider, DEPOSIT);
    }

    function test_openChannel_revertsBelowMinDeposit() public {
        vm.prank(client);
        vm.expectRevert(
            abi.encodeWithSelector(PaymentChannel.DepositBelowMinimum.selector, uint256(1), uint256(1_000_000))
        );
        channel.openChannel(provider, 1);
    }

    function test_openChannel_revertsWhenPaused() public {
        vm.prank(pauser);
        channel.pause();
        vm.prank(client);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        channel.openChannel(provider, DEPOSIT);
    }

    function test_openChannel_distinctIdsAcrossProviders() public {
        address provider2 = address(0xB0B2);
        bond.setActive(provider2, true);
        vm.prank(client);
        bytes32 id0 = channel.openChannel(provider, DEPOSIT);
        vm.prank(client);
        bytes32 id1 = channel.openChannel(provider2, DEPOSIT);
        assertTrue(id0 != id1);
        assertEq(channel.clientChannelNonce(client), 2);
    }

    // -----------------------------------------------------------------
    // topUp
    // -----------------------------------------------------------------

    function test_topUp_increasesDeposit() public {
        bytes32 id = _open();
        vm.prank(client);
        channel.topUp(id, 500e6);
        assertEq(channel.getChannel(id).deposit, DEPOSIT + 500e6);
    }

    function test_topUp_onlyClient() public {
        bytes32 id = _open();
        vm.prank(provider);
        vm.expectRevert(PaymentChannel.NotChannelParty.selector);
        channel.topUp(id, 500e6);
    }

    // -----------------------------------------------------------------
    // withdraw
    // -----------------------------------------------------------------

    function test_withdraw_routesDeltaAndAdvancesWatermark() public {
        bytes32 id = _open();
        uint256 amount = 400e6;
        uint256 bytesDelivered = 40_000_000;
        bytes memory sig = _sign(id, amount, 1, bytesDelivered);

        vm.prank(provider);
        channel.withdraw(id, amount, 1, bytesDelivered, sig);

        assertEq(router.callCount(), 1);
        (address op, uint256 b, uint256 amt) = router.calls(0);
        assertEq(op, provider);
        assertEq(b, bytesDelivered);
        assertEq(amt, amount);

        PaymentChannel.Channel memory ch = channel.getChannel(id);
        assertEq(ch.claimedAmount, amount);
        assertEq(ch.claimedNonce, 1);
        assertEq(ch.claimedBytes, bytesDelivered);
        assertEq(ch.withdrawnAmount, amount);
        assertEq(ch.withdrawnBytes, bytesDelivered);
    }

    function test_withdraw_secondWithdrawRoutesOnlyIncrement() public {
        bytes32 id = _open();
        vm.prank(provider);
        channel.withdraw(id, 300e6, 1, 30_000_000, _sign(id, 300e6, 1, 30_000_000));
        vm.prank(provider);
        channel.withdraw(id, 500e6, 2, 50_000_000, _sign(id, 500e6, 2, 50_000_000));

        assertEq(router.callCount(), 2);
        (, uint256 b2, uint256 amt2) = router.calls(1);
        assertEq(amt2, 200e6); // 500 - 300
        assertEq(b2, 20_000_000); // 50M - 30M
        assertEq(router.totalRouted(), 500e6);
    }

    function test_withdraw_onlyProvider() public {
        bytes32 id = _open();
        bytes memory sig = _sign(id, 400e6, 1, 40_000_000);
        vm.prank(client);
        vm.expectRevert(PaymentChannel.NotChannelParty.selector);
        channel.withdraw(id, 400e6, 1, 40_000_000, sig);
    }

    function test_withdraw_revertsOnNonMonotonicNonce() public {
        bytes32 id = _open();
        vm.prank(provider);
        channel.withdraw(id, 300e6, 2, 30_000_000, _sign(id, 300e6, 2, 30_000_000));
        // nonce 2 again — not strictly higher
        vm.prank(provider);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.NonMonotonicNonce.selector, uint256(2), uint256(2)));
        channel.withdraw(id, 400e6, 2, 40_000_000, _sign(id, 400e6, 2, 40_000_000));
    }

    function test_withdraw_revertsOnAmountExceedingDeposit() public {
        bytes32 id = _open();
        uint256 over = DEPOSIT + 1;
        vm.prank(provider);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.AmountExceedsDeposit.selector, over, DEPOSIT));
        channel.withdraw(id, over, 1, 10, _sign(id, over, 1, 10));
    }

    function test_withdraw_revertsOnBadSignature() public {
        bytes32 id = _open();
        bytes memory sigForOtherAmount = _sign(id, 400e6, 1, 40_000_000);
        // Submit a different amount than was signed → recovery mismatch.
        vm.prank(provider);
        vm.expectRevert(PaymentChannel.InvalidVoucherSignature.selector);
        channel.withdraw(id, 401e6, 1, 40_000_000, sigForOtherAmount);
    }

    // -----------------------------------------------------------------
    // closeChannel + settleChannel
    // -----------------------------------------------------------------

    function test_close_then_settle_routesAndRefunds() public {
        bytes32 id = _open();
        uint256 amount = 700e6;
        uint256 bytesDelivered = 70_000_000;
        bytes memory sig = _sign(id, amount, 1, bytesDelivered);

        vm.prank(provider);
        channel.closeChannel(id, amount, 1, bytesDelivered, sig);

        PaymentChannel.Channel memory ch = channel.getChannel(id);
        assertEq(ch.status, 1); // Closing
        assertEq(ch.disputeDeadline, block.timestamp + DISPUTE_WINDOW);

        vm.warp(block.timestamp + DISPUTE_WINDOW);
        uint256 clientBefore = usdc.balanceOf(client);
        channel.settleChannel(id);

        assertEq(router.totalRouted(), amount);
        assertEq(router.totalBytes(), bytesDelivered);
        assertEq(usdc.balanceOf(client) - clientBefore, DEPOSIT - amount);
        assertEq(channel.getChannel(id).status, 2); // Closed
    }

    function test_settle_revertsBeforeDisputeDeadline() public {
        bytes32 id = _open();
        bytes memory sig = _sign(id, 100e6, 1, 1_000_000);
        vm.prank(provider);
        channel.closeChannel(id, 100e6, 1, 1_000_000, sig);
        vm.expectRevert(PaymentChannel.DisputeWindowActive.selector);
        channel.settleChannel(id);
    }

    function test_close_thirdPartyCannotClose() public {
        bytes32 id = _open();
        bytes memory sig = _sign(id, 100e6, 1, 1_000_000);
        vm.prank(stranger);
        vm.expectRevert(PaymentChannel.NotChannelParty.selector);
        channel.closeChannel(id, 100e6, 1, 1_000_000, sig);
    }

    function test_zeroVoucherClose_refundsFullDeposit() public {
        bytes32 id = _open();
        vm.prank(client);
        channel.closeChannel(id, 0, 0, 0, "");
        vm.warp(block.timestamp + DISPUTE_WINDOW);
        uint256 clientBefore = usdc.balanceOf(client);
        channel.settleChannel(id);
        assertEq(usdc.balanceOf(client) - clientBefore, DEPOSIT);
        assertEq(router.callCount(), 0); // zero amount → no router call
    }

    function test_zeroVoucherClose_disabledAfterWithdraw() public {
        bytes32 id = _open();
        vm.prank(provider);
        channel.withdraw(id, 200e6, 1, 20_000_000, _sign(id, 200e6, 1, 20_000_000));
        // claimedNonce now 1 → zero-voucher path disabled; empty sig fails verification.
        vm.prank(client);
        vm.expectRevert(PaymentChannel.InvalidVoucherSignature.selector);
        channel.closeChannel(id, 0, 0, 0, "");
    }

    function test_close_afterWithdraw_settleRoutesRemainderOnly() public {
        bytes32 id = _open();
        vm.prank(provider);
        channel.withdraw(id, 300e6, 1, 30_000_000, _sign(id, 300e6, 1, 30_000_000));
        // Close at a higher voucher.
        vm.prank(provider);
        channel.closeChannel(id, 800e6, 2, 80_000_000, _sign(id, 800e6, 2, 80_000_000));
        vm.warp(block.timestamp + DISPUTE_WINDOW);
        channel.settleChannel(id);

        // withdraw routed 300, settle routes the 500 remainder → 800 total.
        assertEq(router.totalRouted(), 800e6);
        assertEq(router.totalBytes(), 80_000_000);
    }

    function test_close_revertsAfterExpiry() public {
        bytes32 id = _open();
        vm.warp(block.timestamp + MAX_DURATION);
        // After expiry the provider must forfeit via `reclaimExpired`; closing
        // (then settling) here would bypass the close-before-expiry obligation.
        bytes memory sig = _sign(id, 800e6, 1, 80_000_000);
        vm.prank(provider);
        vm.expectRevert(PaymentChannel.ChannelExpired.selector);
        channel.closeChannel(id, 800e6, 1, 80_000_000, sig);
    }

    // -----------------------------------------------------------------
    // disputeChannel
    // -----------------------------------------------------------------

    function test_dispute_higherNonceWins() public {
        bytes32 id = _open();
        vm.prank(client);
        channel.closeChannel(id, 100e6, 1, 1_000_000, _sign(id, 100e6, 1, 1_000_000));
        vm.prank(stranger);
        channel.disputeChannel(id, 500e6, 2, 50_000_000, _sign(id, 500e6, 2, 50_000_000));

        PaymentChannel.Channel memory ch = channel.getChannel(id);
        assertEq(ch.claimedAmount, 500e6);
        assertEq(ch.claimedNonce, 2);
        assertEq(ch.claimedBytes, 50_000_000);
    }

    function test_dispute_revertsOnStaleNonce() public {
        bytes32 id = _open();
        vm.prank(client);
        channel.closeChannel(id, 300e6, 3, 30_000_000, _sign(id, 300e6, 3, 30_000_000));
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.NonMonotonicNonce.selector, uint256(2), uint256(3)));
        channel.disputeChannel(id, 400e6, 2, 40_000_000, _sign(id, 400e6, 2, 40_000_000));
    }

    function test_dispute_revertsAfterDeadline() public {
        bytes32 id = _open();
        vm.prank(client);
        channel.closeChannel(id, 100e6, 1, 1_000_000, _sign(id, 100e6, 1, 1_000_000));
        vm.warp(block.timestamp + DISPUTE_WINDOW + 1);
        vm.expectRevert(PaymentChannel.DisputeWindowClosed.selector);
        channel.disputeChannel(id, 200e6, 2, 2_000_000, _sign(id, 200e6, 2, 2_000_000));
    }

    // -----------------------------------------------------------------
    // reclaimExpired
    // -----------------------------------------------------------------

    function test_reclaimExpired_refundsRemainderToClient() public {
        bytes32 id = _open();
        vm.prank(provider);
        channel.withdraw(id, 250e6, 1, 25_000_000, _sign(id, 250e6, 1, 25_000_000));
        vm.warp(block.timestamp + MAX_DURATION);
        uint256 clientBefore = usdc.balanceOf(client);
        vm.prank(provider); // provider may trigger; refund still goes to client
        channel.reclaimExpired(id);
        assertEq(usdc.balanceOf(client) - clientBefore, DEPOSIT - 250e6);
        assertEq(channel.getChannel(id).status, 2);
    }

    function test_reclaimExpired_revertsBeforeExpiry() public {
        bytes32 id = _open();
        vm.prank(client);
        vm.expectRevert(PaymentChannel.ChannelNotExpired.selector);
        channel.reclaimExpired(id);
    }

    // -----------------------------------------------------------------
    // Forced-inclusion deadline extension
    // -----------------------------------------------------------------

    function test_forcedInclusion_extendsDeadlineOnce() public {
        ForcedInclusionHarness h = new ForcedInclusionHarness(
            usdc, bond, address(router), DISPUTE_WINDOW, MAX_DURATION, DELIVERY_FLOOR, DELIVERY_CEILING, admin
        );
        usdc.transfer(client, 10_000e6);
        vm.prank(client);
        usdc.approve(address(h), type(uint256).max);

        vm.prank(client);
        bytes32 id = h.openChannel(provider, DEPOSIT);

        bytes memory sig1 = _signFor(address(h), id, 100e6, 1, 1_000_000);
        vm.prank(client);
        h.closeChannel(id, 100e6, 1, 1_000_000, sig1);

        // Warp to within 24h of the deadline, then a forced-inclusion dispute.
        vm.warp(block.timestamp + DISPUTE_WINDOW - 1 hours);
        h.setForced(true);
        bytes memory sig2 = _signFor(address(h), id, 200e6, 2, 2_000_000);
        vm.prank(stranger);
        h.disputeChannel(id, 200e6, 2, 2_000_000, sig2);

        PaymentChannel.Channel memory ch = h.getChannel(id);
        assertTrue(ch.extended);
        assertEq(ch.disputeDeadline, block.timestamp + 24 hours);
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function test_setFeeRouter_updatesTarget() public {
        MockSettlementRouter router2 = new MockSettlementRouter(usdc);
        vm.prank(admin);
        channel.setFeeRouter(address(router2));
        assertEq(channel.feeRouter(), address(router2));
    }

    function test_setFeeRouter_revertsOnZeroAndUnchanged() public {
        vm.prank(admin);
        vm.expectRevert(PaymentChannel.ZeroAddress.selector);
        channel.setFeeRouter(address(0));
        vm.prank(admin);
        vm.expectRevert(PaymentChannel.RouterUnchanged.selector);
        channel.setFeeRouter(address(router));
    }

    function test_setFeeRouter_revertsOnEoaRouter() public {
        // `stranger` is an EOA (no code) — routing settlement there would no-op
        // `routeSettlement` while channel state advances, stranding claimed USDC.
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.FeeRouterHasNoCode.selector, stranger));
        channel.setFeeRouter(stranger);
    }

    function test_setDisputeWindow_enforcesBounds() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                PaymentChannel.ParamOutOfBounds.selector, uint256(1 hours), uint256(12 hours), uint256(72 hours)
            )
        );
        channel.setDisputeWindow(1 hours);
        vm.prank(admin);
        channel.setDisputeWindow(24 hours);
        assertEq(channel.disputeWindow(), 24 hours);
    }

    function test_setters_onlyGovernance() public {
        vm.prank(stranger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        channel.setMinDeposit(5);
    }

    function test_getRateBounds_returnsConfigured() public view {
        (uint256 floor, uint256 ceiling) = channel.getRateBounds();
        assertEq(floor, DELIVERY_FLOOR);
        assertEq(ceiling, DELIVERY_CEILING);
    }

    // -----------------------------------------------------------------
    // Fuzz — settlement conservation
    // -----------------------------------------------------------------

    /// @dev A withdraw delta + the settle remainder must partition the claim
    ///      exactly, and the client refund must equal `deposit - claimed`.
    function testFuzz_withdrawThenSettle_partitionsClaim(
        uint256 wAmount,
        uint256 cAmount,
        uint256 wBytes,
        uint256 cBytes
    ) public {
        wAmount = bound(wAmount, 1, DEPOSIT - 1);
        cAmount = bound(cAmount, wAmount + 1, DEPOSIT);
        wBytes = bound(wBytes, 1, 1_000_000_000);
        cBytes = bound(cBytes, wBytes, 2_000_000_000);

        bytes32 id = _open();
        vm.prank(provider);
        channel.withdraw(id, wAmount, 1, wBytes, _sign(id, wAmount, 1, wBytes));
        vm.prank(provider);
        channel.closeChannel(id, cAmount, 2, cBytes, _sign(id, cAmount, 2, cBytes));
        vm.warp(block.timestamp + DISPUTE_WINDOW);
        uint256 clientBefore = usdc.balanceOf(client);
        channel.settleChannel(id);

        assertEq(router.totalRouted(), cAmount);
        assertEq(router.totalBytes(), cBytes);
        assertEq(usdc.balanceOf(client) - clientBefore, DEPOSIT - cAmount);
        // Conservation: provider-routed + client-refund == deposit.
        assertEq(router.totalRouted() + (DEPOSIT - cAmount), DEPOSIT);
    }

    /// @dev Settling a plain close refunds `deposit - claimed` and routes `claimed`.
    function testFuzz_settle_refundEqualsDepositMinusClaimed(uint256 amount, uint256 bytesDelivered) public {
        amount = bound(amount, 1, DEPOSIT);
        bytesDelivered = bound(bytesDelivered, 1, 1_000_000_000);
        bytes32 id = _open();
        vm.prank(provider);
        channel.closeChannel(id, amount, 1, bytesDelivered, _sign(id, amount, 1, bytesDelivered));
        vm.warp(block.timestamp + DISPUTE_WINDOW);
        uint256 clientBefore = usdc.balanceOf(client);
        channel.settleChannel(id);
        assertEq(router.totalRouted(), amount);
        assertEq(usdc.balanceOf(client) - clientBefore, DEPOSIT - amount);
    }
}
