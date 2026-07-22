// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { ECDSA } from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";

import { PaymentChannel } from "../src/PaymentChannel.sol";
import { SunsettingPausable } from "../src/SunsettingPausable.sol";
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
    uint256 internal constant BYTES_PER_MB = 1_048_576;

    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 internal constant PAUSER_ROLE = keccak256("PAUSER_ROLE");
    bytes32 internal constant VOUCHER_TYPEHASH =
        keccak256("Voucher(bytes32 channelId,uint256 amount,uint256 nonce,uint256 bytesDelivered,address token)");
    bytes32 internal constant COOPERATIVE_CLOSE_TYPEHASH = keccak256(
        "CooperativeClose(bytes32 channelId,uint256 amount,uint256 nonce,uint256 bytesDelivered,address token)"
    );

    // Committed EIP-712 parity vector for the cooperative-close waiver, shared with
    // the off-chain Rust signer (`crates/incentive/src/cooperative_close.rs`,
    // `EXPECTED_COOP_CLOSE_DIGEST`). These are synthetic fixed fixtures, not live
    // deployment config — together with the typehash (read live) and the contract's
    // domain name/version (read live), they pin the cross-language EIP-712 digest so
    // a drift would not silently surface as unsettleable waivers on-chain.
    bytes32 internal constant VEC_COOP_CHANNEL_ID = 0x11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff;
    uint256 internal constant VEC_COOP_AMOUNT = 10_000_000;
    uint256 internal constant VEC_COOP_NONCE = 3;
    uint256 internal constant VEC_COOP_BYTES = 1_048_576;
    // Arbitrary pinned token (Ethereum-mainnet USDC literal); a fixture value, not
    // the deployed channel's `usdc`. The vector asserts encoding parity, not a
    // settleable waiver against any real deployment.
    address internal constant VEC_COOP_TOKEN = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48;
    uint256 internal constant VEC_COOP_CHAIN_ID = 421_614;
    address internal constant VEC_COOP_VERIFYING_CONTRACT = 0x0000000000000000000000000000000000001234;
    bytes32 internal constant VEC_COOP_EXPECTED_DIGEST =
        0x680080f3ddc99e1f65d2a03b8608b84c01e4e6b97885f2ddbca2f17020d8d627;

    // A provider with a known key, needed to sign cooperative-close waivers
    // (the default `provider` is a bare address with no key).
    uint256 internal constant PROVIDER_PK = 0xB0B0B0;
    address internal keyedProvider;

    function setUp() public {
        client = vm.addr(CLIENT_PK);
        keyedProvider = vm.addr(PROVIDER_PK);

        usdc = new MockUSDC();
        bond = new MockActiveBond();
        router = new MockSettlementRouter(usdc);
        bond.setActive(provider, true);
        bond.setActive(keyedProvider, true);

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

    /// @dev EIP-712 sign over `channel`'s domain with an arbitrary key and
    ///      typehash — used for the provider's cooperative-close waiver
    ///      (`COOPERATIVE_CLOSE_TYPEHASH`, signed with `PROVIDER_PK`).
    function _signTypedAs(
        uint256 pk,
        bytes32 typehash,
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
                address(channel)
            )
        );
        bytes32 structHash = keccak256(abi.encode(typehash, channelId, amount, nonce, bytesDelivered, address(usdc)));
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", domainSeparator, structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(pk, digest);
        return abi.encodePacked(r, s, v);
    }

    /// @dev Client voucher signed for the keyed-provider channel (reuses CLIENT_PK).
    function _signClient(bytes32 channelId, uint256 amount, uint256 nonce, uint256 bytesDelivered)
        internal
        view
        returns (bytes memory)
    {
        return _signTypedAs(CLIENT_PK, VOUCHER_TYPEHASH, channelId, amount, nonce, bytesDelivered);
    }

    /// @dev Provider's cooperative-close waiver over the same final tuple.
    function _signWaiver(bytes32 channelId, uint256 amount, uint256 nonce, uint256 bytesDelivered)
        internal
        view
        returns (bytes memory)
    {
        return _signTypedAs(PROVIDER_PK, COOPERATIVE_CLOSE_TYPEHASH, channelId, amount, nonce, bytesDelivered);
    }

    function _openKeyed() internal returns (bytes32 channelId) {
        vm.prank(client);
        channelId = channel.openChannel(keyedProvider, DEPOSIT);
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

    // ADR 009 § Emergency Multisig — the protocol-wide pause sunsets hard at
    // each contract's own construction time + 365 days; afterwards `pause()` reverts for everyone.
    function test_pause_revertsAfterSunset() public {
        vm.warp(block.timestamp + 366 days);
        vm.prank(pauser);
        vm.expectRevert(SunsettingPausable.PauseExpired.selector);
        channel.pause();
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

    /// @dev #890: a paused `FeeRouter` makes `withdraw` revert (unlike `settleChannel`,
    ///      it does not defer). The revert rolls back the watermark advance, so nothing
    ///      is trapped — the provider re-submits the identical voucher after unpause.
    function test_withdraw_routerPaused_revertsThenSucceedsAfterUnpause() public {
        bytes32 id = _open();
        uint256 amount = 400e6;
        uint256 bytesDelivered = 40_000_000;
        bytes memory sig = _sign(id, amount, 1, bytesDelivered);

        router.setPaused(true);
        vm.prank(provider);
        vm.expectRevert(bytes("MockSettlementRouter: paused"));
        channel.withdraw(id, amount, 1, bytesDelivered, sig);

        // The whole tx rolled back: no routing happened and the watermark never moved.
        assertEq(router.callCount(), 0);
        PaymentChannel.Channel memory chBefore = channel.getChannel(id);
        assertEq(chBefore.claimedAmount, 0);
        // The claim-watermark nonce must roll back too: a stuck `claimedNonce`
        // would brick the honest retry below with `NonMonotonicNonce` — the exact
        // "nothing is trapped" property #890 protects.
        assertEq(chBefore.claimedNonce, 0);
        assertEq(chBefore.claimedBytes, 0);
        assertEq(chBefore.withdrawnAmount, 0);
        assertEq(chBefore.withdrawnBytes, 0);

        // After unpause the same voucher withdraws cleanly — no lost or duplicated accounting.
        router.setPaused(false);
        vm.prank(provider);
        channel.withdraw(id, amount, 1, bytesDelivered, sig);

        assertEq(router.callCount(), 1);
        (address op, uint256 b, uint256 amt) = router.calls(0);
        assertEq(op, provider);
        assertEq(b, bytesDelivered);
        assertEq(amt, amount);

        PaymentChannel.Channel memory chAfter = channel.getChannel(id);
        assertEq(chAfter.withdrawnAmount, amount);
        assertEq(chAfter.withdrawnBytes, bytesDelivered);
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
                PaymentChannel.ParamOutOfBounds.selector, uint256(1 hours), uint256(48 hours), uint256(72 hours)
            )
        );
        channel.setDisputeWindow(1 hours);
        vm.prank(admin);
        channel.setDisputeWindow(60 hours);
        assertEq(channel.disputeWindow(), 60 hours);
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
    // Rate bounds uint64 upper cap (#1383): the daemon's rate clamp is `u64`,
    // so a ratified band above `type(uint64).max` fails to decode and silently
    // strands vouchers below the enforced on-chain floor. The chain must not be
    // able to express a band the node cannot enforce. Each governance-gated case
    // pranks `admin` (which holds `GOVERNANCE_ROLE`) so the revert is the bounds
    // guard, not an access-control failure.
    // -----------------------------------------------------------------

    /// @dev A ceiling one above `type(uint64).max` reverts `RateBoundsInvalid`.
    function test_setRateBounds_revertsWhenCeilingExceedsUint64Cap() public {
        uint256 badCeiling = uint256(type(uint64).max) + 1;
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.RateBoundsInvalid.selector, DELIVERY_FLOOR, badCeiling));
        channel.setRateBounds(DELIVERY_FLOOR, badCeiling);
    }

    /// @dev A floor above `type(uint64).max` reverts `RateBoundsInvalid` (the
    ///      ceiling is set just above it so the failure is the u64 cap, not the
    ///      `newCeiling <= newFloor` ordering check).
    function test_setRateBounds_revertsWhenFloorExceedsUint64Cap() public {
        uint256 badFloor = uint256(type(uint64).max) + 1;
        uint256 badCeiling = uint256(type(uint64).max) + 2;
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.RateBoundsInvalid.selector, badFloor, badCeiling));
        channel.setRateBounds(badFloor, badCeiling);
    }

    /// @dev In-bounds control: a ceiling at exactly `type(uint64).max` is the
    ///      widest band the guard admits and succeeds, proving the cap is
    ///      inclusive (`>` , not `>=`).
    function test_setRateBounds_succeedsAtUint64Cap() public {
        uint256 maxCeiling = uint256(type(uint64).max);
        vm.prank(admin);
        channel.setRateBounds(DELIVERY_FLOOR, maxCeiling);
        (uint256 floor, uint256 ceiling) = channel.getRateBounds();
        assertEq(floor, DELIVERY_FLOOR);
        assertEq(ceiling, maxCeiling);
    }

    /// @dev The constructor shares the guard: deploying with a ceiling above
    ///      `type(uint64).max` reverts `RateBoundsInvalid`.
    function test_constructor_revertsOnRateCeilingAboveUint64Cap() public {
        uint256 badCeiling = uint256(type(uint64).max) + 1;
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.RateBoundsInvalid.selector, DELIVERY_FLOOR, badCeiling));
        new PaymentChannel(usdc, bond, address(router), DISPUTE_WINDOW, MAX_DURATION, DELIVERY_FLOOR, badCeiling, admin);
    }

    // -----------------------------------------------------------------
    // Rate floor enforcement (#846): `amount * BYTES_PER_MB >= bytes * floor`
    // -----------------------------------------------------------------

    /// @dev The headline attack: `amount = 1`, `bytesDelivered = 2^256-1`. Must
    ///      revert with a clean `RateFloorViolation`, never an arithmetic panic
    ///      (the `Math.mulDiv` bytes-ceiling makes the comparison overflow-safe).
    function test_rateFloor_revertsOnMaxBytesAttack() public {
        bytes32 id = _open();
        uint256 hugeBytes = type(uint256).max;
        bytes memory sig = _sign(id, 1, 1, hugeBytes);
        vm.prank(provider);
        vm.expectRevert(
            abi.encodeWithSelector(PaymentChannel.RateFloorViolation.selector, 1, hugeBytes, DELIVERY_FLOOR)
        );
        channel.closeChannel(id, 1, 1, hugeBytes, sig);
    }

    /// @dev Same near-free inflation via the absurd `2^200` byte count from the
    ///      issue, exercised on the `withdraw` entry point.
    function test_rateFloor_withdraw_revertsOnInflatedBytes() public {
        bytes32 id = _open();
        uint256 inflated = 2 ** 200;
        bytes memory sig = _sign(id, 1, 1, inflated);
        vm.prank(provider);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.RateFloorViolation.selector, 1, inflated, DELIVERY_FLOOR));
        channel.withdraw(id, 1, 1, inflated, sig);
    }

    /// @dev The floor binds `disputeChannel` too (shared `_advanceClaimWatermark`
    ///      chokepoint). Close honestly, then dispute with a sub-floor voucher.
    function test_rateFloor_dispute_revertsOnInflatedBytes() public {
        bytes32 id = _open();
        // Honest close first (nonce 1): 10 base units permits up to 10*BYTES_PER_MB
        // bytes; 5M is comfortably above the floor.
        channelCloseHonest(id, 10, 1, 5_000_000);
        // Dispute keeps the amount (allowed: `amount >= claimedAmount`) and a
        // higher nonce, but inflates bytes past `10 * BYTES_PER_MB` (~10.49M).
        uint256 inflated = 20_000_000;
        bytes memory sig = _sign(id, 10, 2, inflated);
        vm.prank(client);
        vm.expectRevert(
            abi.encodeWithSelector(PaymentChannel.RateFloorViolation.selector, 10, inflated, DELIVERY_FLOOR)
        );
        channel.disputeChannel(id, 10, 2, inflated, sig);
    }

    /// @dev Boundary: `bytes == amount * BYTES_PER_MB / floor` passes; `+1` reverts.
    function test_rateFloor_boundaryExact() public {
        uint256 amount = 100;
        uint256 maxBytes = amount * BYTES_PER_MB / DELIVERY_FLOOR;

        bytes32 idOk = _open();
        vm.prank(provider);
        channel.closeChannel(idOk, amount, 1, maxBytes, _sign(idOk, amount, 1, maxBytes));
        assertEq(channel.getChannel(idOk).claimedBytes, maxBytes);

        bytes32 idBad = _open();
        bytes memory sig = _sign(idBad, amount, 1, maxBytes + 1);
        vm.prank(provider);
        vm.expectRevert(
            abi.encodeWithSelector(PaymentChannel.RateFloorViolation.selector, amount, maxBytes + 1, DELIVERY_FLOOR)
        );
        channel.closeChannel(idBad, amount, 1, maxBytes + 1, sig);
    }

    /// @dev An honest voucher at the expected market rate (10 base units/MB, 10×
    ///      the floor) settles untouched, and the zero-voucher close still works.
    function test_rateFloor_honestPathUnaffected() public {
        bytes32 id = _open();
        // 40_000_000 bytes ≈ 38.15 MB → 390 base units at the $0.01/GB market rate.
        channelCloseHonest(id, 390, 1, 40_000_000);
        assertEq(channel.getChannel(id).claimedBytes, 40_000_000);

        bytes32 idZero = _open();
        vm.prank(provider);
        channel.closeChannel(idZero, 0, 0, 0, "");
        assertEq(channel.getChannel(idZero).claimedBytes, 0);
    }

    /// @dev With a non-unit floor the bytes ceiling `mulDiv(amount, BYTES_PER_MB,
    ///      floor)` truncates DOWN, so the protocol never over-admits a fractional
    ///      byte: `maxBytes` passes, `maxBytes + 1` reverts even though the exact
    ///      rational boundary lies between them.
    function test_rateFloor_truncatesDownAtNonUnitFloor() public {
        vm.prank(admin);
        channel.setRateBounds(3, DELIVERY_CEILING);

        uint256 amount = 100;
        // 100 * 1_048_576 / 3 = 34_952_533 (rounded down from 34_952_533.33).
        uint256 maxBytes = amount * BYTES_PER_MB / 3;

        bytes32 idOk = _open();
        vm.prank(provider);
        channel.closeChannel(idOk, amount, 1, maxBytes, _sign(idOk, amount, 1, maxBytes));
        assertEq(channel.getChannel(idOk).claimedBytes, maxBytes);

        bytes32 idBad = _open();
        bytes memory sig = _sign(idBad, amount, 1, maxBytes + 1);
        vm.prank(provider);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.RateFloorViolation.selector, amount, maxBytes + 1, 3));
        channel.closeChannel(idBad, amount, 1, maxBytes + 1, sig);
    }

    /// @dev Governance raising `deliveryFloor` rejects a voucher that was valid at
    ///      the old floor — the floor is a live, tunable settlement constraint.
    function test_rateFloor_governanceRaisingFloorRejects() public {
        // At floor 1, a 1-base-unit voucher may claim up to BYTES_PER_MB bytes.
        bytes32 idOld = _open();
        vm.prank(provider);
        channel.closeChannel(idOld, 1, 1, BYTES_PER_MB, _sign(idOld, 1, 1, BYTES_PER_MB));

        // Raise the floor to 2: the same voucher now exceeds the per-byte price.
        vm.prank(admin);
        channel.setRateBounds(2, DELIVERY_CEILING);

        bytes32 idNew = _open();
        bytes memory sig = _sign(idNew, 1, 1, BYTES_PER_MB);
        vm.prank(provider);
        vm.expectRevert(abi.encodeWithSelector(PaymentChannel.RateFloorViolation.selector, 1, BYTES_PER_MB, 2));
        channel.closeChannel(idNew, 1, 1, BYTES_PER_MB, sig);
    }

    /// @dev Routed served bytes can never exceed `amount * BYTES_PER_MB / floor`,
    ///      so vote-weight inflation (ADR 036) costs proportional real USDC. Settle
    ///      at the maximum the floor permits for the paid amount and assert the
    ///      routed bytes equal that ceiling — the bound is load-bearing here, not
    ///      slack: one more byte for the same `amount` would have reverted.
    function test_rateFloor_routedBytesBoundedByPaidAmount() public {
        bytes32 id = _open();
        uint256 amount = 100;
        uint256 maxBytes = amount * BYTES_PER_MB / DELIVERY_FLOOR;
        vm.prank(provider);
        channel.closeChannel(id, amount, 1, maxBytes, _sign(id, amount, 1, maxBytes));
        vm.warp(block.timestamp + DISPUTE_WINDOW);
        channel.settleChannel(id);
        assertEq(router.totalBytes(), maxBytes);
        assertEq(router.totalBytes(), amount * BYTES_PER_MB / DELIVERY_FLOOR);
    }

    /// @dev Fuzz: a close reverts iff `bytesDelivered > amount * BYTES_PER_MB /
    ///      floor`, straddling the boundary so both branches are exercised.
    function testFuzz_rateFloor_revertIffBelowFloor(uint256 amount, uint256 bytesDelivered) public {
        amount = bound(amount, 1, DEPOSIT);
        uint256 maxBytes = amount * BYTES_PER_MB / DELIVERY_FLOOR;
        bytesDelivered = bound(bytesDelivered, 0, 2 * maxBytes);

        bytes32 id = _open();
        bytes memory sig = _sign(id, amount, 1, bytesDelivered);
        vm.prank(provider);
        if (bytesDelivered > maxBytes) {
            vm.expectRevert(
                abi.encodeWithSelector(
                    PaymentChannel.RateFloorViolation.selector, amount, bytesDelivered, DELIVERY_FLOOR
                )
            );
            channel.closeChannel(id, amount, 1, bytesDelivered, sig);
        } else {
            channel.closeChannel(id, amount, 1, bytesDelivered, sig);
            assertEq(channel.getChannel(id).claimedBytes, bytesDelivered);
        }
    }

    /// @dev Close a channel with a provider-signable honest voucher (helper).
    function channelCloseHonest(bytes32 id, uint256 amount, uint256 nonce, uint256 bytesDelivered) internal {
        vm.prank(provider);
        channel.closeChannel(id, amount, nonce, bytesDelivered, _sign(id, amount, nonce, bytesDelivered));
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
        // Keep both watermarks above the enforced per-byte floor (#846): the max
        // bytes claimable at `amount` is `amount * BYTES_PER_MB / deliveryFloor`.
        // `cAmount > wAmount` guarantees `cAmount * BYTES_PER_MB >= wBytes`, so
        // the cumulative-close range stays non-empty.
        wBytes = bound(wBytes, 1, wAmount * BYTES_PER_MB);
        cBytes = bound(cBytes, wBytes, cAmount * BYTES_PER_MB);

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
        // Keep the pair above the enforced per-byte floor (#846): the max bytes
        // claimable for `amount` is `amount * BYTES_PER_MB / deliveryFloor`.
        bytesDelivered = bound(bytesDelivered, 1, amount * BYTES_PER_MB);
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

    /// @dev Membership test over a `deferredSettlements` page (order is unstable).
    function _contains(bytes32[] memory page, bytes32 target) internal pure returns (bool) {
        for (uint256 i = 0; i < page.length; i++) {
            if (page[i] == target) return true;
        }
        return false;
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

    /// @dev The load-bearing case: with a prior `withdraw`, the deferred and
    ///      flushed share must be the remainder (`claimed − withdrawn`), not the
    ///      full claim — proving `flushDeferredSettlement` recomputes the delta and
    ///      cannot double-route already-withdrawn amount/bytes.
    function test_flushDeferredSettlement_partialWithdraw_routesRemainderOnly() public {
        bytes32 id = _open();
        // Provider withdraws part while Open (router unpaused → routes 300e6 now).
        vm.prank(provider);
        channel.withdraw(id, 300e6, 1, 30_000_000, _sign(id, 300e6, 1, 30_000_000));
        // Close at a higher watermark, then warp past the dispute window.
        vm.prank(provider);
        channel.closeChannel(id, 700e6, 2, 70_000_000, _sign(id, 700e6, 2, 70_000_000));
        vm.warp(block.timestamp + DISPUTE_WINDOW);

        router.setPaused(true);
        uint256 clientBefore = usdc.balanceOf(client);

        // Deferred share is the remainder (700−300 / 70M−30M), not the full claim.
        vm.expectEmit(true, true, false, true, address(channel));
        emit PaymentChannel.SettlementDeferred(id, provider, 400e6, 40_000_000);
        channel.settleChannel(id);

        assertEq(usdc.balanceOf(client) - clientBefore, DEPOSIT - 700e6); // 300e6 refund
        assertEq(usdc.balanceOf(address(channel)), 400e6); // only the un-withdrawn share held
        assertTrue(channel.settlementDeferred(id));

        router.setPaused(false);
        vm.expectEmit(true, true, false, true, address(channel));
        emit PaymentChannel.DeferredSettlementFlushed(id, provider, 400e6, 40_000_000);
        channel.flushDeferredSettlement(id);

        // Conservation over the whole run: 300 (withdraw) + 400 (flush) == 700 claimed.
        assertEq(router.totalRouted(), 700e6);
        assertEq(router.totalBytes(), 70_000_000);
        assertEq(usdc.balanceOf(address(channel)), 0);
        assertFalse(channel.settlementDeferred(id));
    }

    /// @dev A deferred settle still closes the channel: the exit is final, so the
    ///      provider leg can only route via flush — never a second settle/dispute.
    function test_settle_deferred_channelIsClosed_cannotResettleOrDispute() public {
        bytes32 id = _openCloseWarp(700e6, 70_000_000);
        router.setPaused(true);
        channel.settleChannel(id); // deferred; status now Closed

        vm.expectRevert(PaymentChannel.ChannelNotClosing.selector);
        channel.settleChannel(id);

        vm.expectRevert(PaymentChannel.ChannelNotClosing.selector);
        channel.disputeChannel(id, 800e6, 2, 80_000_000, _sign(id, 800e6, 2, 80_000_000));
    }

    /// @dev Governance re-pointing the router between defer and flush routes the
    ///      held share through the NEW router (`_route` reads `feeRouter` live) —
    ///      the documented incident-recovery path out of a paused router.
    function test_flushDeferredSettlement_afterRouterRepoint_routesToNewRouter() public {
        bytes32 id = _openCloseWarp(700e6, 70_000_000);
        router.setPaused(true);
        channel.settleChannel(id); // deferred against the paused setUp router

        MockSettlementRouter router2 = new MockSettlementRouter(usdc); // fresh, unpaused, conforming
        vm.prank(admin);
        channel.setFeeRouter(address(router2));

        channel.flushDeferredSettlement(id);

        assertEq(router2.totalRouted(), 700e6); // new router receives the deferred share
        assertEq(router2.totalBytes(), 70_000_000);
        assertEq(router.totalRouted(), 0); // old (paused) router got nothing for the settle
        assertEq(usdc.balanceOf(address(channel)), 0);
        assertFalse(channel.settlementDeferred(id));
    }

    /// @dev Flushing while the router is still paused reverts and leaves the
    ///      deferral flag set, so it stays retryable after a later unpause.
    function test_flushDeferredSettlement_stillPaused_revertsAndStaysDeferred() public {
        bytes32 id = _openCloseWarp(700e6, 70_000_000);
        router.setPaused(true);
        channel.settleChannel(id);

        vm.expectRevert(bytes("MockSettlementRouter: paused")); // mirrors router's whenNotPaused guard
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
    // Deferred-settlement enumeration (#902 — keeper drain without log replay)
    // -----------------------------------------------------------------

    /// @dev A deferral is enumerable on-chain: count + paginated view surface the
    ///      pending id without replaying the `SettlementDeferred` event.
    function test_deferredSettlements_enumeratesPendingId() public {
        bytes32 id = _openCloseWarp(700e6, 70_000_000);
        router.setPaused(true);
        channel.settleChannel(id);

        assertEq(channel.deferredSettlementCount(), 1);
        bytes32[] memory page = channel.deferredSettlements(0, 10);
        assertEq(page.length, 1);
        assertEq(page[0], id);
    }

    /// @dev Flushing removes the id from the enumeration, not just the per-id flag.
    function test_deferredSettlements_clearedAfterFlush() public {
        bytes32 id = _openCloseWarp(700e6, 70_000_000);
        router.setPaused(true);
        channel.settleChannel(id);
        assertEq(channel.deferredSettlementCount(), 1);

        router.setPaused(false);
        channel.flushDeferredSettlement(id);

        assertEq(channel.deferredSettlementCount(), 0);
        assertEq(channel.deferredSettlements(0, 10).length, 0);
    }

    /// @dev Several concurrent deferrals all enumerate; flushing one removes only
    ///      that id (set membership, not position), leaving the rest drainable.
    function test_deferredSettlements_multipleDeferrals_selectiveFlush() public {
        router.setPaused(true);
        bytes32 id1 = _openCloseWarp(700e6, 70_000_000);
        channel.settleChannel(id1);
        bytes32 id2 = _openCloseWarp(500e6, 50_000_000);
        channel.settleChannel(id2);
        bytes32 id3 = _openCloseWarp(300e6, 30_000_000);
        channel.settleChannel(id3);

        assertEq(channel.deferredSettlementCount(), 3);
        bytes32[] memory all = channel.deferredSettlements(0, 10);
        assertTrue(_contains(all, id1));
        assertTrue(_contains(all, id2));
        assertTrue(_contains(all, id3));

        router.setPaused(false); // must be unpaused to route the flushed leg
        channel.flushDeferredSettlement(id2);

        assertEq(channel.deferredSettlementCount(), 2);
        assertFalse(channel.settlementDeferred(id2));
        // Both views agree, in both directions: the page and the per-id view.
        assertTrue(channel.settlementDeferred(id1));
        assertTrue(channel.settlementDeferred(id3));
        bytes32[] memory rest = channel.deferredSettlements(0, 10);
        assertFalse(_contains(rest, id2));
        assertTrue(_contains(rest, id1));
        assertTrue(_contains(rest, id3));
    }

    /// @dev Pages must return the actual ids (proving `at(offset + i)` indexing,
    ///      not just a correctly-sized array): walk the set one id per page across
    ///      every offset and assert the pages partition the set — distinct ids, no
    ///      gaps, each member seen exactly once.
    function test_deferredSettlements_pageContentsPartitionTheSetAcrossOffsets() public {
        router.setPaused(true);
        bytes32 id1 = _openCloseWarp(700e6, 70_000_000);
        channel.settleChannel(id1);
        bytes32 id2 = _openCloseWarp(500e6, 50_000_000);
        channel.settleChannel(id2);
        bytes32 id3 = _openCloseWarp(300e6, 30_000_000);
        channel.settleChannel(id3);

        bytes32 p0 = channel.deferredSettlements(0, 1)[0];
        bytes32 p1 = channel.deferredSettlements(1, 1)[0];
        bytes32 p2 = channel.deferredSettlements(2, 1)[0];

        // Distinct: no offset returned the same element as another (no overlap/gap).
        assertTrue(p0 != p1 && p1 != p2 && p0 != p2);
        // Union equals the set: every deferred id appears in exactly one page.
        bytes32[] memory pages = new bytes32[](3);
        pages[0] = p0;
        pages[1] = p1;
        pages[2] = p2;
        assertTrue(_contains(pages, id1));
        assertTrue(_contains(pages, id2));
        assertTrue(_contains(pages, id3));
    }

    /// @dev `limit == type(uint256).max` from offset 0 is the "drain everything"
    ///      sentinel: it clamps to the set length without overflowing `offset + limit`.
    function test_deferredSettlements_maxLimitFromZeroReturnsFullSet() public {
        router.setPaused(true);
        bytes32 id = _openCloseWarp(700e6, 70_000_000);
        channel.settleChannel(id);

        bytes32[] memory page = channel.deferredSettlements(0, type(uint256).max);
        assertEq(page.length, 1);
        assertEq(page[0], id);
    }

    /// @dev A max `limit` at a non-zero `offset` must clamp to the remaining tail,
    ///      not revert on `offset + limit` overflow — keepers may pass max limit
    ///      defensively from any offset.
    function test_deferredSettlements_maxLimitFromNonZeroOffsetClampsNoOverflow() public {
        router.setPaused(true);
        bytes32 id1 = _openCloseWarp(700e6, 70_000_000);
        channel.settleChannel(id1);
        bytes32 id2 = _openCloseWarp(500e6, 50_000_000);
        channel.settleChannel(id2);
        bytes32 id3 = _openCloseWarp(300e6, 30_000_000);
        channel.settleChannel(id3);

        // offset 1 + max limit → the 2-element tail, no overflow revert.
        bytes32[] memory tail = channel.deferredSettlements(1, type(uint256).max);
        assertEq(tail.length, 2);
        // The two returned ids are the set minus whichever id sits at index 0.
        bytes32 head = channel.deferredSettlements(0, 1)[0];
        assertFalse(_contains(tail, head));
        assertTrue(channel.settlementDeferred(tail[0]));
        assertTrue(channel.settlementDeferred(tail[1]));
        assertTrue(tail[0] != tail[1]);
    }

    /// @dev Pagination guards: out-of-range offset and zero limit yield an empty
    ///      page; a limit past the end clamps to the remaining tail.
    function test_deferredSettlements_paginationBoundaries() public {
        router.setPaused(true);
        bytes32 id1 = _openCloseWarp(700e6, 70_000_000);
        channel.settleChannel(id1);
        bytes32 id2 = _openCloseWarp(500e6, 50_000_000);
        channel.settleChannel(id2);
        bytes32 id3 = _openCloseWarp(300e6, 30_000_000);
        channel.settleChannel(id3);

        assertEq(channel.deferredSettlements(3, 10).length, 0); // offset == len → empty
        assertEq(channel.deferredSettlements(9, 10).length, 0); // offset > len  → empty
        assertEq(channel.deferredSettlements(0, 0).length, 0); // limit 0       → empty
        assertEq(channel.deferredSettlements(2, 10).length, 1); // clamp to tail
        assertEq(channel.deferredSettlements(1, 1).length, 1); // window inside set
        assertEq(channel.deferredSettlements(0, 3).length, 3); // full page
    }

    /// @dev Fuzz the pagination invariant over the full `(offset, limit)` domain
    ///      (including `type(uint256).max` extremes): the returned page must equal
    ///      the matching slice of the canonical full enumeration. This pins, in one
    ///      property, the overflow-safe length clamp (no `offset + limit` revert),
    ///      the `at(offset + i)` indexing, and the absence of duplicates/gaps —
    ///      the page is exactly `full[offset .. offset + size]`.
    function testFuzz_deferredSettlements_pageMatchesFullEnumerationSlice(uint256 offset, uint256 limit) public {
        uint256 count = 6;
        router.setPaused(true);
        for (uint256 i = 0; i < count; i++) {
            bytes32 id = _openCloseWarp(100e6, 10_000_000);
            channel.settleChannel(id);
        }
        assertEq(channel.deferredSettlementCount(), count);

        // Canonical ordering, taken once with no interleaving mutation.
        bytes32[] memory full = channel.deferredSettlements(0, count);
        assertEq(full.length, count);

        // Expected page length, computed with the same overflow-safe clamp the
        // contract uses (never forming `offset + limit`).
        uint256 expectedSize;
        if (offset < count && limit != 0) {
            uint256 remaining = count - offset;
            expectedSize = limit < remaining ? limit : remaining;
        }

        bytes32[] memory page = channel.deferredSettlements(offset, limit);
        assertEq(page.length, expectedSize);
        for (uint256 i = 0; i < expectedSize; i++) {
            // `offset + i < count`, so this reference read cannot overflow.
            assertEq(page[i], full[offset + i]);
        }
    }

    /// @dev A zero provider-amount settle never defers, so the enumeration stays empty.
    function test_deferredSettlementCount_unchangedOnZeroAmountSettle() public {
        bytes32 id = _open();
        vm.prank(client);
        channel.closeChannel(id, 0, 0, 0, "");
        vm.warp(block.timestamp + DISPUTE_WINDOW);

        router.setPaused(true);
        channel.settleChannel(id);

        assertEq(channel.deferredSettlementCount(), 0);
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
                PaymentChannel.ParamOutOfBounds.selector, uint256(1 hours), uint256(48 hours), uint256(72 hours)
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
    // disputeChannel: deadline is fixed for the window
    // -----------------------------------------------------------------

    function test_dispute_doesNotMoveDeadline() public {
        bytes32 id = _open();
        vm.prank(client);
        channel.closeChannel(id, 100e6, 1, 1_000_000, _sign(id, 100e6, 1, 1_000_000));
        uint64 originalDeadline = channel.getChannel(id).disputeDeadline;

        // Even a dispute landing near the deadline leaves it untouched — the
        // baseline window (kept above the L2 force-inclusion delay) is the sole
        // censorship guarantee.
        vm.warp(block.timestamp + DISPUTE_WINDOW - 1 hours);
        vm.prank(stranger);
        channel.disputeChannel(id, 200e6, 2, 2_000_000, _sign(id, 200e6, 2, 2_000_000));
        assertEq(uint256(channel.getChannel(id).disputeDeadline), uint256(originalDeadline));
    }

    // -----------------------------------------------------------------
    // cooperativeClose (ADR 003 § Cooperative close)
    // -----------------------------------------------------------------

    function test_cooperativeClose_settlesImmediatelyNoWindow() public {
        bytes32 id = _openKeyed();
        uint256 amount = 400e6;
        uint256 b = 40_000_000;

        vm.prank(client);
        channel.cooperativeClose(id, amount, 1, b, _signClient(id, amount, 1, b), _signWaiver(id, amount, 1, b));

        // Provider leg routed in the same tx — no time warp, no dispute window.
        assertEq(router.callCount(), 1);
        (address op, uint256 routedBytes, uint256 routedAmt) = router.calls(0);
        assertEq(op, keyedProvider);
        assertEq(routedBytes, b);
        assertEq(routedAmt, amount);

        PaymentChannel.Channel memory ch = channel.getChannel(id);
        assertEq(uint8(ch.status), 2); // Closed
        assertEq(ch.claimedAmount, amount);
        // Client refunded deposit - amount immediately.
        assertEq(usdc.balanceOf(client), 100_000e6 - DEPOSIT + (DEPOSIT - amount));
    }

    /// @dev The "I've been fully paid, just release my refund" case: the provider
    ///      already `withdraw`-drained to the watermark, so both sign that same
    ///      nonce and the client gets an instant refund with no second route.
    function test_cooperativeClose_afterFullWithdraw_refundsClientNoRoute() public {
        bytes32 id = _openKeyed();
        uint256 amount = 400e6;
        uint256 b = 40_000_000;

        vm.prank(keyedProvider);
        channel.withdraw(id, amount, 1, b, _signClient(id, amount, 1, b));
        assertEq(router.callCount(), 1);

        // Cooperative close at the SAME (already-withdrawn) watermark.
        vm.prank(client);
        channel.cooperativeClose(id, amount, 1, b, _signClient(id, amount, 1, b), _signWaiver(id, amount, 1, b));

        // No second route — provider was already fully paid via withdraw.
        assertEq(router.callCount(), 1);
        assertEq(uint8(channel.getChannel(id).status), 2);
        assertEq(usdc.balanceOf(client), 100_000e6 - amount);
    }

    function test_cooperativeClose_eitherPartyMaySubmit() public {
        bytes32 id = _openKeyed();
        uint256 amount = 250e6;
        uint256 b = 25_000_000;

        // Provider submits (symmetric to the client-submits happy path above).
        vm.prank(keyedProvider);
        channel.cooperativeClose(id, amount, 1, b, _signClient(id, amount, 1, b), _signWaiver(id, amount, 1, b));
        assertEq(uint8(channel.getChannel(id).status), 2);
    }

    function test_cooperativeClose_onlyParty() public {
        bytes32 id = _openKeyed();
        uint256 amount = 100e6;
        uint256 b = 10_000_000;
        vm.prank(stranger);
        vm.expectRevert(PaymentChannel.NotChannelParty.selector);
        channel.cooperativeClose(id, amount, 1, b, _signClient(id, amount, 1, b), _signWaiver(id, amount, 1, b));
    }

    function test_cooperativeClose_revertsOnBadClientSig() public {
        bytes32 id = _openKeyed();
        uint256 amount = 100e6;
        uint256 b = 10_000_000;
        // Client voucher signed by the wrong key (the provider's).
        bytes memory badClient = _signTypedAs(PROVIDER_PK, VOUCHER_TYPEHASH, id, amount, 1, b);
        vm.prank(client);
        vm.expectRevert(PaymentChannel.InvalidVoucherSignature.selector);
        channel.cooperativeClose(id, amount, 1, b, badClient, _signWaiver(id, amount, 1, b));
    }

    function test_cooperativeClose_revertsOnBadProviderWaiver() public {
        bytes32 id = _openKeyed();
        uint256 amount = 100e6;
        uint256 b = 10_000_000;
        // Waiver signed by the wrong key (the client's) — a client voucher can
        // never stand in for the provider's waiver.
        bytes memory badWaiver = _signTypedAs(CLIENT_PK, COOPERATIVE_CLOSE_TYPEHASH, id, amount, 1, b);
        vm.prank(client);
        vm.expectRevert(PaymentChannel.InvalidCooperativeCloseSignature.selector);
        channel.cooperativeClose(id, amount, 1, b, _signClient(id, amount, 1, b), badWaiver);
    }

    /// @dev A provider voucher-typed signature (right key, WRONG typehash) is not a
    ///      valid waiver — proves the typehash separation, not just signer identity.
    function test_cooperativeClose_revertsOnWrongTypehashWaiver() public {
        bytes32 id = _openKeyed();
        uint256 amount = 100e6;
        uint256 b = 10_000_000;
        bytes memory wrongType = _signTypedAs(PROVIDER_PK, VOUCHER_TYPEHASH, id, amount, 1, b);
        vm.prank(client);
        vm.expectRevert(PaymentChannel.InvalidCooperativeCloseSignature.selector);
        channel.cooperativeClose(id, amount, 1, b, _signClient(id, amount, 1, b), wrongType);
    }

    /// @dev Cross-language EIP-712 parity: the canonical-vector digest, recomputed
    ///      here with the contract's *live* `COOPERATIVE_CLOSE_TYPEHASH()` and the
    ///      vector's fixed domain, must equal the value the off-chain Rust signer
    ///      pins (`EXPECTED_COOP_CLOSE_DIGEST`). Catches a typehash change (read
    ///      live) and any drift in the Rust signer's struct encoding/framing
    ///      relative to this recomputation. The contract's own domain
    ///      `name`/`version` are pinned separately by
    ///      `test_cooperativeClose_domainMatchesVector`; the contract's runtime
    ///      struct-encode/`_hashTypedDataV4` path is exercised by the happy-path
    ///      round-trip tests (e.g. `test_cooperativeClose_settlesImmediatelyNoWindow`).
    function test_cooperativeClose_digestMatchesVector() public view {
        bytes32 structHash = keccak256(
            abi.encode(
                channel.COOPERATIVE_CLOSE_TYPEHASH(),
                VEC_COOP_CHANNEL_ID,
                VEC_COOP_AMOUNT,
                VEC_COOP_NONCE,
                VEC_COOP_BYTES,
                VEC_COOP_TOKEN
            )
        );
        bytes32 domainSep = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256("PaymentChannel"),
                keccak256("1"),
                VEC_COOP_CHAIN_ID,
                VEC_COOP_VERIFYING_CONTRACT
            )
        );
        bytes32 digest = keccak256(abi.encodePacked(hex"1901", domainSep, structHash));
        assertEq(digest, VEC_COOP_EXPECTED_DIGEST, "coop-close digest drifted from Rust signer vector");
    }

    /// @dev Closes the loop the manual-recompute test cannot: the *deployed*
    ///      contract's EIP-712 domain (ERC-5267 `eip712Domain()`) must use the same
    ///      name/version that went into the pinned digest. A change to
    ///      `EIP712("PaymentChannel", "1")` fails here even though the vector above
    ///      hardcodes the strings.
    function test_cooperativeClose_domainMatchesVector() public view {
        (, string memory name, string memory version,,,,) = channel.eip712Domain();
        assertEq(name, "PaymentChannel", "domain name drifted");
        assertEq(version, "1", "domain version drifted");
    }

    /// @dev Finality anchor: a waiver below the on-chain watermark can never
    ///      under-settle — it reverts against the advanced `claimed*` state.
    function test_cooperativeClose_staleWaiverCannotUnderSettle() public {
        bytes32 id = _openKeyed();
        // Provider withdraws to a high watermark (500e6, nonce 2).
        vm.prank(keyedProvider);
        channel.withdraw(id, 500e6, 2, 50_000_000, _signClient(id, 500e6, 2, 50_000_000));

        // A stale-low cooperative close (400e6) — even at a strictly higher nonce —
        // regresses the amount and reverts.
        vm.prank(client);
        vm.expectRevert(
            abi.encodeWithSelector(PaymentChannel.AmountRegression.selector, uint256(400e6), uint256(500e6))
        );
        channel.cooperativeClose(
            id, 400e6, 3, 60_000_000, _signClient(id, 400e6, 3, 60_000_000), _signWaiver(id, 400e6, 3, 60_000_000)
        );
    }

    function test_cooperativeClose_revertsWhenNotOpen() public {
        bytes32 id = _openKeyed();
        uint256 amount = 300e6;
        uint256 b = 30_000_000;
        // Start a normal close → status Closing; cooperativeClose is Open-only.
        vm.prank(client);
        channel.closeChannel(id, amount, 1, b, _signClient(id, amount, 1, b));
        vm.prank(client);
        vm.expectRevert(PaymentChannel.ChannelNotOpen.selector);
        channel.cooperativeClose(id, amount, 1, b, _signClient(id, amount, 1, b), _signWaiver(id, amount, 1, b));
    }

    /// @dev Unlike `settleChannel`, `cooperativeClose` does NOT defer under a
    ///      paused router (#890 posture): the whole atomic `Open → Closed` call
    ///      reverts, leaving the channel `Open` with nothing stranded — no refund,
    ///      no watermark advance, no deferred-settlement entry. The same call
    ///      succeeds once the router is unpaused.
    function test_cooperativeClose_routerPaused_revertsThenSucceedsAfterUnpause() public {
        bytes32 id = _openKeyed();
        uint256 amount = 400e6;
        uint256 b = 40_000_000;

        router.setPaused(true);
        vm.prank(client);
        vm.expectRevert(bytes("MockSettlementRouter: paused"));
        channel.cooperativeClose(id, amount, 1, b, _signClient(id, amount, 1, b), _signWaiver(id, amount, 1, b));

        // Whole tx rolled back: channel still Open, nothing routed/deferred/refunded.
        PaymentChannel.Channel memory chBefore = channel.getChannel(id);
        assertEq(uint8(chBefore.status), 0); // Open
        assertEq(chBefore.claimedAmount, 0);
        assertEq(router.callCount(), 0);
        assertEq(channel.deferredSettlementCount(), 0);
        assertEq(usdc.balanceOf(client), 100_000e6 - DEPOSIT);

        // After unpause the same signatures settle cleanly in one tx.
        router.setPaused(false);
        vm.prank(client);
        channel.cooperativeClose(id, amount, 1, b, _signClient(id, amount, 1, b), _signWaiver(id, amount, 1, b));
        assertEq(uint8(channel.getChannel(id).status), 2); // Closed
        assertEq(router.callCount(), 1);
        (address op, uint256 routedBytes, uint256 routedAmt) = router.calls(0);
        assertEq(op, keyedProvider);
        assertEq(routedBytes, b);
        assertEq(routedAmt, amount);
        assertEq(usdc.balanceOf(client), 100_000e6 - amount);
    }
}
