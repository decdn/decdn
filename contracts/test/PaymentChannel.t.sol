// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { ECDSA } from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";

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

    /// @dev Mirrors the real `FeeRouter`'s `whenNotPaused` guard on
    ///      `routeSettlement` so the channel's pause-deferral path is exercised.
    bool internal _paused;

    constructor(IERC20 usdc_) {
        usdc = usdc_;
    }

    function setPaused(bool p) external {
        _paused = p;
    }

    function paused() external view override returns (bool) {
        return _paused;
    }

    function routeSettlement(address operator, uint256 bytesDelivered, uint256 amount) external override {
        require(!_paused, "MockSettlementRouter: paused");
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

/// @notice Router that pulls one wei LESS than approved, leaving a residual
///         allowance. Used to prove `_route` zeroes the allowance after the
///         router call (M-3): without the reset, the residue would persist as a
///         standing allowance over the channel's USDC.
contract UnderPullRouter is IFeeRouterSettlement {
    IERC20 public immutable usdc;

    constructor(IERC20 usdc_) {
        usdc = usdc_;
    }

    function routeSettlement(address, uint256, uint256 amount) external override {
        require(amount != 0, "UnderPullRouter: zero amount");
        usdc.transferFrom(msg.sender, address(this), amount - 1);
    }

    function paused() external pure override returns (bool) {
        return false;
    }
}

/// @notice Router that implements `routeSettlement` but NOT `paused()`. Used to
///         prove the config-time conformance probe rejects a non-conforming
///         router loudly (constructor + setFeeRouter), rather than letting
///         `settleChannel` brick on the missing pause view (#849 follow-up).
contract NoPauseRouter {
    function routeSettlement(address, uint256, uint256) external { }
}

/// @notice Router whose `paused()` shares the selector but returns a non-canonical
///         bool word (2). Proves the conformance probe rejects a return the
///         high-level `paused()` call would strict-decode-revert on.
contract NonBoolPauseRouter {
    function routeSettlement(address, uint256, uint256) external { }

    function paused() external pure returns (uint256) {
        return 2;
    }
}

/// @notice Minimal ERC-1271 smart-account wallet: validates a signature by
///         recovering it to a fixed owner EOA. Exercises the SignatureChecker
///         ERC-1271 branch of voucher verification (ADR 024 smart-account signers).
contract MockERC1271Wallet {
    bytes4 internal constant MAGIC = 0x1626ba7e;
    address public immutable owner;

    constructor(address owner_) {
        owner = owner_;
    }

    function isValidSignature(bytes32 hash, bytes calldata signature) external view returns (bytes4) {
        (address recovered, ECDSA.RecoverError err,) = ECDSA.tryRecover(hash, signature);
        return (err == ECDSA.RecoverError.NoError && recovered == owner) ? MAGIC : bytes4(0xffffffff);
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

    /// @dev Deploy a forced-inclusion harness and fund/approve the client against it.
    function _newForcedHarness() internal returns (ForcedInclusionHarness h) {
        h = new ForcedInclusionHarness(
            usdc, bond, address(router), DISPUTE_WINDOW, MAX_DURATION, DELIVERY_FLOOR, DELIVERY_CEILING, admin
        );
        usdc.transfer(client, 10_000e6);
        vm.prank(client);
        usdc.approve(address(h), type(uint256).max);
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
        assertEq(uint256(ch.expiresAt), block.timestamp + MAX_DURATION);
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

    function test_topUp_revertsWhenPaused() public {
        bytes32 id = _open();
        vm.prank(pauser);
        channel.pause();
        vm.prank(client);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        channel.topUp(id, 500e6);
    }

    // -----------------------------------------------------------------
    // _route allowance hygiene (M-3): no standing allowance survives a
    // settlement, even if the router pulls less than approved.
    // -----------------------------------------------------------------

    function test_route_zeroesResidualAllowanceAfterUnderPull() public {
        UnderPullRouter under = new UnderPullRouter(usdc);
        vm.prank(admin);
        channel.setFeeRouter(address(under));

        bytes32 id = _open();
        uint256 amount = 400e6;
        uint256 bytesDelivered = 40_000_000;
        bytes memory sig = _sign(id, amount, 1, bytesDelivered);
        vm.prank(provider);
        channel.withdraw(id, amount, 1, bytesDelivered, sig);

        // Router pulled amount-1, leaving 1 wei of would-be residue; the reset
        // in `_route` must bring the standing allowance back to zero.
        assertEq(usdc.allowance(address(channel), address(under)), 0);
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
        assertEq(uint8(ch.status), uint8(PaymentChannel.Status.Closing));
        assertEq(uint256(ch.disputeDeadline), block.timestamp + DISPUTE_WINDOW);

        vm.warp(block.timestamp + DISPUTE_WINDOW);
        uint256 clientBefore = usdc.balanceOf(client);
        channel.settleChannel(id);

        assertEq(router.totalRouted(), amount);
        assertEq(router.totalBytes(), bytesDelivered);
        assertEq(usdc.balanceOf(client) - clientBefore, DEPOSIT - amount);
        assertEq(uint8(channel.getChannel(id).status), uint8(PaymentChannel.Status.Closed));
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
        assertEq(uint8(channel.getChannel(id).status), uint8(PaymentChannel.Status.Closed));
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
        assertEq(uint256(ch.disputeDeadline), block.timestamp + 24 hours);
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

    /// @dev A router with code that implements `routeSettlement` but not
    ///      `paused()` is rejected at set time — settleChannel relies on that view.
    function test_setFeeRouter_revertsOnRouterMissingPausedView() public {
        NoPauseRouter bad = new NoPauseRouter();
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.FeeRouterMissingPausedView.selector, address(bad)));
        channel.setFeeRouter(address(bad));
    }

    /// @dev A router whose `paused()` returns a non-canonical bool word (> 1) is
    ///      rejected — the high-level call in `settleChannel` would otherwise
    ///      strict-decode-revert and re-trap the refund.
    function test_setFeeRouter_revertsOnNonBooleanPausedReturn() public {
        NonBoolPauseRouter bad = new NonBoolPauseRouter();
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.FeeRouterMissingPausedView.selector, address(bad)));
        channel.setFeeRouter(address(bad));
    }

    /// @dev The same conformance probe guards construction.
    function test_constructor_revertsOnRouterMissingPausedView() public {
        NoPauseRouter bad = new NoPauseRouter();
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.FeeRouterMissingPausedView.selector, address(bad)));
        new PaymentChannel(
            usdc, bond, address(bad), DISPUTE_WINDOW, MAX_DURATION, DELIVERY_FLOOR, DELIVERY_CEILING, admin
        );
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

    // -----------------------------------------------------------------
    // Settlement-path edges (zero-remainder, watermark close, deposit boundary)
    // -----------------------------------------------------------------

    /// @dev A channel fully drawn via `withdraw` then closed at the same watermark
    ///      settles with NO router call (zero remainder) — the real `FeeRouter`
    ///      reverts on a zero amount, so a stray route here would brick settlement.
    function test_settle_fullyWithdrawn_noRouterCall() public {
        bytes32 id = _open();
        vm.prank(provider);
        channel.withdraw(id, 600e6, 1, 60_000_000, _sign(id, 600e6, 1, 60_000_000));
        uint256 callsAfterWithdraw = router.callCount();

        // Close at the same nonce-1 watermark voucher (non-strict nonce).
        vm.prank(provider);
        channel.closeChannel(id, 600e6, 1, 60_000_000, _sign(id, 600e6, 1, 60_000_000));
        vm.warp(block.timestamp + DISPUTE_WINDOW);
        uint256 clientBefore = usdc.balanceOf(client);
        channel.settleChannel(id);

        assertEq(router.callCount(), callsAfterWithdraw); // no extra route
        assertEq(usdc.balanceOf(client) - clientBefore, DEPOSIT - 600e6);
        assertEq(uint8(channel.getChannel(id).status), uint8(PaymentChannel.Status.Closed));
    }

    /// @dev `closeChannel` accepts a voucher at the current watermark (`nonce ==`),
    ///      the deliberate non-strict asymmetry vs. `withdraw`/`disputeChannel`.
    function test_close_atWatermarkNonce_succeeds() public {
        bytes32 id = _open();
        vm.prank(provider);
        channel.withdraw(id, 300e6, 1, 30_000_000, _sign(id, 300e6, 1, 30_000_000));
        vm.prank(provider);
        channel.closeChannel(id, 300e6, 1, 30_000_000, _sign(id, 300e6, 1, 30_000_000));

        PaymentChannel.Channel memory ch = channel.getChannel(id);
        assertEq(uint8(ch.status), uint8(PaymentChannel.Status.Closing));
        assertEq(ch.claimedNonce, 1);
        assertEq(ch.claimedAmount, 300e6);
        assertEq(ch.claimedBytes, 30_000_000);
    }

    function test_close_revertsOnAmountExceedingDeposit() public {
        bytes32 id = _open();
        uint256 over = DEPOSIT + 1;
        bytes memory sig = _sign(id, over, 1, 1_000_000);
        vm.prank(provider);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.AmountExceedsDeposit.selector, over, DEPOSIT));
        channel.closeChannel(id, over, 1, 1_000_000, sig);
    }

    function test_dispute_revertsOnAmountExceedingDeposit() public {
        bytes32 id = _open();
        vm.prank(client);
        channel.closeChannel(id, 100e6, 1, 1_000_000, _sign(id, 100e6, 1, 1_000_000));
        uint256 over = DEPOSIT + 1;
        vm.prank(stranger);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.AmountExceedsDeposit.selector, over, DEPOSIT));
        channel.disputeChannel(id, over, 2, 2_000_000, _sign(id, over, 2, 2_000_000));
    }

    /// @dev `amount == deposit` is the legal boundary: settle routes the full
    ///      deposit and refunds the client zero.
    function test_settle_amountEqualsDeposit_zeroRefund() public {
        bytes32 id = _open();
        vm.prank(provider);
        channel.closeChannel(id, DEPOSIT, 1, 100_000_000, _sign(id, DEPOSIT, 1, 100_000_000));
        vm.warp(block.timestamp + DISPUTE_WINDOW);
        uint256 clientBefore = usdc.balanceOf(client);
        channel.settleChannel(id);
        assertEq(router.totalRouted(), DEPOSIT);
        assertEq(usdc.balanceOf(client) - clientBefore, 0);
    }

    // -----------------------------------------------------------------
    // settleChannel pause deferral (#849 — exits always open)
    // -----------------------------------------------------------------

    /// @dev Helper: open, close at `amount`/`bytesDelivered`, warp past the
    ///      dispute window. Returns the channel id ready to settle.
    function _openCloseWarp(uint256 amount, uint256 bytesDelivered) internal returns (bytes32 id) {
        id = _open();
        vm.prank(provider);
        channel.closeChannel(id, amount, 1, bytesDelivered, _sign(id, amount, 1, bytesDelivered));
        vm.warp(block.timestamp + DISPUTE_WINDOW);
    }

    /// @dev A paused router must NOT freeze the exit: the client refund still
    ///      lands and the channel closes; only the provider leg is deferred.
    function test_settle_routerPaused_refundsClientAndDefersProviderLeg() public {
        uint256 amount = 700e6;
        uint256 bytesDelivered = 70_000_000;
        bytes32 id = _openCloseWarp(amount, bytesDelivered);

        router.setPaused(true);
        uint256 clientBefore = usdc.balanceOf(client);

        vm.expectEmit(true, true, false, true, address(channel));
        emit PaymentChannel.SettlementDeferred(id, provider, amount, bytesDelivered);
        channel.settleChannel(id);

        // Client refunded and channel closed despite the paused router.
        assertEq(usdc.balanceOf(client) - clientBefore, DEPOSIT - amount);
        assertEq(uint8(channel.getChannel(id).status), uint8(PaymentChannel.Status.Closed));
        // Provider leg deferred: nothing routed, flag set, USDC held by the channel.
        assertTrue(channel.settlementDeferred(id));
        assertEq(router.callCount(), 0);
        assertEq(usdc.balanceOf(address(channel)), amount);
    }

    /// @dev After unpause, anyone can flush the deferred provider leg — routing
    ///      the exact share and bytes once — and a second flush reverts.
    function test_flushDeferredSettlement_routesAfterUnpause() public {
        uint256 amount = 700e6;
        uint256 bytesDelivered = 70_000_000;
        bytes32 id = _openCloseWarp(amount, bytesDelivered);

        router.setPaused(true);
        channel.settleChannel(id);

        router.setPaused(false);
        vm.expectEmit(true, true, false, true, address(channel));
        emit PaymentChannel.DeferredSettlementFlushed(id, provider, amount, bytesDelivered);
        vm.prank(stranger); // permissionless
        channel.flushDeferredSettlement(id);

        assertEq(router.totalRouted(), amount);
        assertEq(router.totalBytes(), bytesDelivered);
        assertEq(usdc.balanceOf(address(channel)), 0);
        assertFalse(channel.settlementDeferred(id));

        // Idempotent: a second flush has nothing left to route.
        vm.expectRevert(PaymentChannel.NoDeferredSettlement.selector);
        channel.flushDeferredSettlement(id);
    }

    /// @dev Flushing while the router is still paused reverts and leaves the
    ///      deferral flag set, so it stays retryable after a later unpause.
    function test_flushDeferredSettlement_stillPaused_revertsAndStaysDeferred() public {
        bytes32 id = _openCloseWarp(700e6, 70_000_000);
        router.setPaused(true);
        channel.settleChannel(id);

        vm.expectRevert(); // router's whenNotPaused guard
        channel.flushDeferredSettlement(id);
        assertTrue(channel.settlementDeferred(id));

        // Unpause and retry succeeds.
        router.setPaused(false);
        channel.flushDeferredSettlement(id);
        assertEq(router.totalRouted(), 700e6);
        assertFalse(channel.settlementDeferred(id));
    }

    /// @dev A channel that never deferred cannot be flushed.
    function test_flushDeferredSettlement_notDeferred_reverts() public {
        bytes32 id = _openCloseWarp(700e6, 70_000_000);
        channel.settleChannel(id); // router not paused → routed inline

        assertFalse(channel.settlementDeferred(id));
        vm.expectRevert(PaymentChannel.NoDeferredSettlement.selector);
        channel.flushDeferredSettlement(id);
    }

    /// @dev A fully-refunded channel (zero provider amount) needs no router call,
    ///      so a paused router never triggers a deferral.
    function test_settle_routerPaused_zeroProviderAmount_noDeferral() public {
        bytes32 id = _open();
        vm.prank(client);
        channel.closeChannel(id, 0, 0, 0, "");
        vm.warp(block.timestamp + DISPUTE_WINDOW);

        router.setPaused(true);
        uint256 clientBefore = usdc.balanceOf(client);
        channel.settleChannel(id);

        assertEq(usdc.balanceOf(client) - clientBefore, DEPOSIT);
        assertFalse(channel.settlementDeferred(id));
        assertEq(router.callCount(), 0);
    }

    // -----------------------------------------------------------------
    // Watermark regression reverts (provider/client fund protection)
    // -----------------------------------------------------------------

    function test_dispute_revertsOnAmountRegression() public {
        bytes32 id = _open();
        vm.prank(client);
        channel.closeChannel(id, 500e6, 1, 50_000_000, _sign(id, 500e6, 1, 50_000_000));
        // Higher nonce but a lower amount must not reduce the provider's payout.
        vm.prank(stranger);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.AmountRegression.selector, 400e6, 500e6));
        channel.disputeChannel(id, 400e6, 2, 60_000_000, _sign(id, 400e6, 2, 60_000_000));
    }

    function test_dispute_revertsOnBytesRegression() public {
        bytes32 id = _open();
        vm.prank(client);
        channel.closeChannel(id, 500e6, 1, 50_000_000, _sign(id, 500e6, 1, 50_000_000));
        // Higher nonce + higher amount but lower bytes must not cut served-byte weight.
        vm.prank(stranger);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.BytesRegression.selector, 40_000_000, 50_000_000));
        channel.disputeChannel(id, 600e6, 2, 40_000_000, _sign(id, 600e6, 2, 40_000_000));
    }

    function test_withdraw_revertsWhenDeltaZero() public {
        bytes32 id = _open();
        vm.prank(provider);
        channel.withdraw(id, 300e6, 1, 30_000_000, _sign(id, 300e6, 1, 30_000_000));
        // Strictly-higher nonce but identical amount AND bytes → nothing to route.
        vm.prank(provider);
        vm.expectRevert(PaymentChannel.NothingToWithdraw.selector);
        channel.withdraw(id, 300e6, 2, 30_000_000, _sign(id, 300e6, 2, 30_000_000));
    }

    function test_withdraw_revertsOnBytesRegression() public {
        bytes32 id = _open();
        vm.prank(provider);
        channel.withdraw(id, 300e6, 1, 30_000_000, _sign(id, 300e6, 1, 30_000_000));
        vm.prank(provider);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.BytesRegression.selector, 20_000_000, 30_000_000));
        channel.withdraw(id, 400e6, 2, 20_000_000, _sign(id, 400e6, 2, 20_000_000));
    }

    /// @dev A close voucher advancing bytes without advancing amount past the
    ///      withdrawal watermark is rejected — `settleChannel` could not route
    ///      those bytes (zero amount delta) without silently dropping them.
    function test_close_revertsOnByteOnlyAdvance() public {
        bytes32 id = _open();
        vm.prank(provider);
        channel.withdraw(id, 300e6, 1, 30_000_000, _sign(id, 300e6, 1, 30_000_000));
        vm.prank(provider);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.ByteAdvanceWithoutPayment.selector, 20_000_000));
        channel.closeChannel(id, 300e6, 2, 50_000_000, _sign(id, 300e6, 2, 50_000_000));
    }

    function test_dispute_revertsOnByteOnlyAdvance() public {
        bytes32 id = _open();
        vm.prank(provider);
        channel.withdraw(id, 300e6, 1, 30_000_000, _sign(id, 300e6, 1, 30_000_000));
        vm.prank(provider);
        channel.closeChannel(id, 300e6, 1, 30_000_000, _sign(id, 300e6, 1, 30_000_000));
        // Dispute advances bytes only (amount stays at the withdrawal watermark).
        vm.prank(stranger);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.ByteAdvanceWithoutPayment.selector, 10_000_000));
        channel.disputeChannel(id, 300e6, 2, 40_000_000, _sign(id, 300e6, 2, 40_000_000));
    }

    // -----------------------------------------------------------------
    // Voucher verification: ERC-1271 + replay
    // -----------------------------------------------------------------

    /// @dev A smart-account (ERC-1271) client validates vouchers via its
    ///      `isValidSignature`, not EOA ecrecover.
    function test_withdraw_acceptsErc1271Signature() public {
        MockERC1271Wallet wallet = new MockERC1271Wallet(client); // owner = CLIENT_PK signer
        usdc.transfer(address(wallet), 10_000e6);
        vm.prank(address(wallet));
        usdc.approve(address(channel), type(uint256).max);

        vm.prank(address(wallet));
        bytes32 id = channel.openChannel(provider, DEPOSIT);
        assertEq(channel.getChannel(id).client, address(wallet));

        // Voucher signed by the wallet's owner key; verified through ERC-1271.
        bytes memory sig = _sign(id, 200e6, 1, 20_000_000);
        vm.prank(provider);
        channel.withdraw(id, 200e6, 1, 20_000_000, sig);

        assertEq(router.totalRouted(), 200e6);
        assertEq(channel.getChannel(id).withdrawnAmount, 200e6);
    }

    /// @dev A voucher validly signed for channel A must not verify on channel B —
    ///      the `channelId` is bound into the EIP-712 digest.
    function test_voucher_rejectedOnWrongChannelId() public {
        bytes32 idA = _open();
        bytes32 idB = _open();
        bytes memory sigA = _sign(idA, 100e6, 1, 1_000_000);
        vm.prank(provider);
        vm.expectRevert(PaymentChannel.InvalidVoucherSignature.selector);
        channel.withdraw(idB, 100e6, 1, 1_000_000, sigA);
    }

    // -----------------------------------------------------------------
    // Constructor validation
    // -----------------------------------------------------------------

    function test_constructor_revertsOnZeroAdmin() public {
        vm.expectRevert(PaymentChannel.ZeroAddress.selector);
        new PaymentChannel(
            usdc, bond, address(router), DISPUTE_WINDOW, MAX_DURATION, DELIVERY_FLOOR, DELIVERY_CEILING, address(0)
        );
    }

    function test_constructor_revertsOnEoaFeeRouter() public {
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.FeeRouterHasNoCode.selector, stranger));
        new PaymentChannel(usdc, bond, stranger, DISPUTE_WINDOW, MAX_DURATION, DELIVERY_FLOOR, DELIVERY_CEILING, admin);
    }

    function test_constructor_revertsOnDisputeWindowOutOfBounds() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                PaymentChannel.ParamOutOfBounds.selector, uint256(1 hours), uint256(12 hours), uint256(72 hours)
            )
        );
        new PaymentChannel(usdc, bond, address(router), 1 hours, MAX_DURATION, DELIVERY_FLOOR, DELIVERY_CEILING, admin);
    }

    // -----------------------------------------------------------------
    // Pause: opens blocked, exits stay callable
    // -----------------------------------------------------------------

    /// @dev While paused, `openChannel` reverts but every existing-channel exit
    ///      path (`withdraw`/`closeChannel`/`settleChannel`) stays callable so
    ///      funds are never trapped (PaymentChannel pause semantics).
    function test_paused_exitsStillCallable() public {
        bytes32 id = _open();
        vm.prank(provider);
        channel.withdraw(id, 200e6, 1, 20_000_000, _sign(id, 200e6, 1, 20_000_000));

        vm.prank(pauser);
        channel.pause();

        // Exits still work.
        vm.prank(provider);
        channel.withdraw(id, 400e6, 2, 40_000_000, _sign(id, 400e6, 2, 40_000_000));
        vm.prank(provider);
        channel.closeChannel(id, 500e6, 3, 50_000_000, _sign(id, 500e6, 3, 50_000_000));
        vm.warp(block.timestamp + DISPUTE_WINDOW);
        channel.settleChannel(id);
        assertEq(uint8(channel.getChannel(id).status), uint8(PaymentChannel.Status.Closed));

        // But a fresh open is blocked.
        vm.prank(client);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        channel.openChannel(provider, DEPOSIT);
    }

    // -----------------------------------------------------------------
    // Forced-inclusion extension: negative branches
    // -----------------------------------------------------------------

    function test_forcedInclusion_doesNotExtendTwice() public {
        ForcedInclusionHarness h = _newForcedHarness();
        vm.prank(client);
        bytes32 id = h.openChannel(provider, DEPOSIT);
        vm.prank(client);
        h.closeChannel(id, 100e6, 1, 1_000_000, _signFor(address(h), id, 100e6, 1, 1_000_000));

        // First forced-inclusion dispute near the deadline → extends once.
        vm.warp(block.timestamp + DISPUTE_WINDOW - 1 hours);
        h.setForced(true);
        vm.prank(stranger);
        h.disputeChannel(id, 200e6, 2, 2_000_000, _signFor(address(h), id, 200e6, 2, 2_000_000));
        assertTrue(h.getChannel(id).extended);
        uint64 deadlineAfterFirst = h.getChannel(id).disputeDeadline;

        // Second forced-inclusion dispute must NOT extend again.
        vm.warp(block.timestamp + 1 hours);
        vm.prank(stranger);
        h.disputeChannel(id, 300e6, 3, 3_000_000, _signFor(address(h), id, 300e6, 3, 3_000_000));
        assertEq(uint256(h.getChannel(id).disputeDeadline), uint256(deadlineAfterFirst));
    }

    function test_forcedInclusion_noExtendWhenAmpleTimeRemains() public {
        ForcedInclusionHarness h = _newForcedHarness();
        vm.prank(client);
        bytes32 id = h.openChannel(provider, DEPOSIT);
        vm.prank(client);
        h.closeChannel(id, 100e6, 1, 1_000_000, _signFor(address(h), id, 100e6, 1, 1_000_000));
        uint64 originalDeadline = h.getChannel(id).disputeDeadline;

        // Forced inclusion but with > FORCED_INCLUSION_GUARANTEE (24h) remaining.
        vm.warp(block.timestamp + 1 hours);
        h.setForced(true);
        vm.prank(stranger);
        h.disputeChannel(id, 200e6, 2, 2_000_000, _signFor(address(h), id, 200e6, 2, 2_000_000));
        assertFalse(h.getChannel(id).extended);
        assertEq(uint256(h.getChannel(id).disputeDeadline), uint256(originalDeadline));
    }

    function test_normalDispute_doesNotExtend() public {
        bytes32 id = _open();
        vm.prank(client);
        channel.closeChannel(id, 100e6, 1, 1_000_000, _sign(id, 100e6, 1, 1_000_000));
        uint64 originalDeadline = channel.getChannel(id).disputeDeadline;

        // Near the deadline, but a normal (non-forced) dispute never extends.
        vm.warp(block.timestamp + DISPUTE_WINDOW - 1 hours);
        vm.prank(stranger);
        channel.disputeChannel(id, 200e6, 2, 2_000_000, _sign(id, 200e6, 2, 2_000_000));
        assertEq(uint256(channel.getChannel(id).disputeDeadline), uint256(originalDeadline));
        assertFalse(channel.getChannel(id).extended);
    }
}
