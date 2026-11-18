// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { CctpSweepReceiver } from "../src/CctpSweepReceiver.sol";
import { PaymentPoolBase } from "../src/PaymentPoolBase.sol";
import { FeeRouter } from "../src/FeeRouter.sol";
import { IFeeRouter } from "../src/interfaces/IFeeRouter.sol";
import { MintableUSDC } from "./mocks/MintableUSDC.sol";
import { MockCreditBond } from "./mocks/MockCreditBond.sol";
import { CctpMessage, MockMessageTransmitterV2, MockTokenMessengerV2 } from "./mocks/MockCctp.sol";

/// @dev Both CCTP sides live in one EVM: a source messenger on domain 3 (the
///      payment chain) and a destination transmitter + messenger on domain 0.
contract CctpSweepReceiverTest is Test {
    uint32 internal constant SRC = 3;
    uint32 internal constant DST = 0;
    uint64 internal constant EPOCH = 7 days;

    MintableUSDC internal srcUsdc;
    MintableUSDC internal dstUsdc;
    MockTokenMessengerV2 internal srcMessenger;
    MockTokenMessengerV2 internal dstMessenger;
    MockMessageTransmitterV2 internal transmitter;
    MockCreditBond internal bond;
    FeeRouter internal router;
    CctpSweepReceiver internal receiver;

    address internal admin = address(0xA11CE);
    address internal treasury = address(0xD7);
    address internal pool = address(0x9001);
    address internal operator = address(0xB0B);
    address internal relayer = address(0x5157);

    event SweepReceived(uint32 indexed sourceDomain, address indexed pool, address indexed operator, uint256 amount);

    function setUp() public {
        vm.warp(EPOCH + 1);
        srcUsdc = new MintableUSDC();
        dstUsdc = new MintableUSDC();
        srcMessenger = new MockTokenMessengerV2(SRC);
        dstMessenger = new MockTokenMessengerV2(DST);
        transmitter = new MockMessageTransmitterV2(DST);
        dstMessenger.setTransmitter(address(transmitter));
        dstMessenger.setRemote(SRC, address(srcMessenger));
        dstMessenger.setLocalToken(CctpMessage.toBytes32(address(srcUsdc)), dstUsdc);
        srcMessenger.setRemote(DST, address(dstMessenger));

        bond = new MockCreditBond(EPOCH);
        bond.set(operator, true, 1000);
        router = new FeeRouter({
            usdc_: dstUsdc,
            capacityBond_: bond,
            treasury_: treasury,
            epochLength_: EPOCH,
            windowEpochs_: 13,
            deliveryFloor_: 1,
            admin: admin,
            rebateBps_: 7500,
            buybackBps_: 0,
            buybackBurner_: address(0)
        });
        receiver = new CctpSweepReceiver(dstUsdc, transmitter, address(dstMessenger), IFeeRouter(address(router)));
    }

    function _share(address op, uint256 servedBytes, uint256 amount)
        internal
        pure
        returns (PaymentPoolBase.SweepShare[] memory shares)
    {
        shares = new PaymentPoolBase.SweepShare[](1);
        shares[0] = PaymentPoolBase.SweepShare({ operator: op, servedBytes: servedBytes, amount: amount });
    }

    /// @dev Burns `amount` on the source side the way `PaymentPool.sweep` does
    ///      and returns the message Circle would attest.
    function _sweep(address op, uint256 servedBytes, uint256 amount) internal returns (bytes memory) {
        return _sweepShares(_share(op, servedBytes, amount), amount);
    }

    function _sweepShares(PaymentPoolBase.SweepShare[] memory shares, uint256 amount) internal returns (bytes memory) {
        srcUsdc.mint(pool, amount);
        vm.startPrank(pool);
        srcUsdc.approve(address(srcMessenger), amount);
        bytes32 to = CctpMessage.toBytes32(address(receiver));
        srcMessenger.depositForBurnWithHook(amount, DST, to, address(srcUsdc), to, 0, 2000, abi.encode(shares));
        vm.stopPrank();
        return srcMessenger.lastMessage();
    }

    function _burn(bytes32 recipient, bytes32 mintRecipient, bytes32 burnToken, uint256 fee)
        internal
        view
        returns (bytes memory)
    {
        return _burnShares(recipient, mintRecipient, burnToken, fee, abi.encode(_share(operator, 5, 1000e6)));
    }

    /// @dev A message as Circle would attest it, with a 1000 USDC burn.
    function _burnShares(bytes32 recipient, bytes32 mintRecipient, bytes32 burnToken, uint256 fee, bytes memory hook)
        internal
        view
        returns (bytes memory)
    {
        return CctpMessage.encode(
            CctpMessage.Burn({
                sourceDomain: SRC,
                destinationDomain: DST,
                nonce: keccak256("n"),
                sender: CctpMessage.toBytes32(address(srcMessenger)),
                recipient: recipient,
                destinationCaller: CctpMessage.toBytes32(address(receiver)),
                burnToken: burnToken,
                mintRecipient: mintRecipient,
                amount: 1000e6,
                messageSender: CctpMessage.toBytes32(pool),
                maxFee: fee,
                feeExecuted: fee,
                hookData: hook
            })
        );
    }

    function _usdcBurn(uint256 fee, bytes memory hook) internal view returns (bytes memory) {
        return _burnShares(
            CctpMessage.toBytes32(address(dstMessenger)),
            CctpMessage.toBytes32(address(receiver)),
            CctpMessage.toBytes32(address(srcUsdc)),
            fee,
            hook
        );
    }

    function _activeOps(uint256 n) internal returns (address[] memory ops) {
        ops = new address[](n);
        for (uint256 i = 0; i < n; i++) {
            ops[i] = address(uint160(0x20000 + i));
            bond.set(ops[i], true, 1000);
        }
    }

    function _epoch() internal view returns (uint64) {
        return uint64(block.timestamp / EPOCH);
    }

    function test_receiveSweep_depositsMintedUsdc() public {
        bytes memory message = _sweep(operator, 1234, 1000e6);
        vm.prank(relayer);
        receiver.receiveSweep(message, hex"01");

        assertEq(dstUsdc.balanceOf(operator), 750e6, "rebate");
        assertEq(dstUsdc.balanceOf(treasury), 250e6, "treasury");
        assertEq(dstUsdc.balanceOf(address(receiver)), 0, "receiver keeps nothing");
        assertEq(router.bytesPerEpoch(operator, _epoch()), 1234, "credit");
    }

    function test_receiveSweep_emitsSweepReceived() public {
        bytes memory message = _sweep(operator, 1, 2e6);
        vm.expectEmit(true, true, true, true, address(receiver));
        emit SweepReceived(SRC, pool, operator, 2e6);
        receiver.receiveSweep(message, hex"01");
    }

    function test_receiveSweep_creditsAmountNetOfCctpFee() public {
        bytes memory message = _burn(
            CctpMessage.toBytes32(address(dstMessenger)),
            CctpMessage.toBytes32(address(receiver)),
            CctpMessage.toBytes32(address(srcUsdc)),
            1e6
        );
        receiver.receiveSweep(message, hex"01");
        assertEq(dstUsdc.balanceOf(operator) + dstUsdc.balanceOf(treasury), 999e6);
    }

    function test_receiveSweep_rejectsOtherRecipient() public {
        bytes memory message = _burn(
            CctpMessage.toBytes32(address(0xBAD)),
            CctpMessage.toBytes32(address(receiver)),
            CctpMessage.toBytes32(address(srcUsdc)),
            0
        );
        vm.expectRevert(CctpSweepReceiver.NotTokenMessenger.selector);
        receiver.receiveSweep(message, hex"01");
    }

    function test_receiveSweep_rejectsOtherMintRecipient() public {
        bytes memory message = _burn(
            CctpMessage.toBytes32(address(dstMessenger)),
            CctpMessage.toBytes32(address(0xBAD)),
            CctpMessage.toBytes32(address(srcUsdc)),
            0
        );
        vm.expectRevert(CctpSweepReceiver.NotMintRecipient.selector);
        receiver.receiveSweep(message, hex"01");
    }

    /// A burn of a token that mints something other than USDC credits nothing.
    function test_receiveSweep_rejectsNonUsdcMint() public {
        MintableUSDC other = new MintableUSDC();
        bytes32 otherRemote = bytes32(uint256(0xE0C));
        dstMessenger.setLocalToken(otherRemote, other);
        bytes memory message = _burn(
            CctpMessage.toBytes32(address(dstMessenger)), CctpMessage.toBytes32(address(receiver)), otherRemote, 0
        );
        vm.expectRevert(CctpSweepReceiver.NothingMinted.selector);
        receiver.receiveSweep(message, hex"01");
    }

    function test_receiveSweep_rejectsShortMessage() public {
        vm.expectRevert(CctpSweepReceiver.MalformedMessage.selector);
        receiver.receiveSweep(new bytes(400), hex"01");
    }

    /// One message credits every operator in the batch, bytes and rebate.
    function test_receiveSweep_creditsEachOperatorInBatch() public {
        address[] memory ops = _activeOps(3);
        PaymentPoolBase.SweepShare[] memory shares = new PaymentPoolBase.SweepShare[](3);
        for (uint256 i = 0; i < 3; i++) {
            shares[i] = PaymentPoolBase.SweepShare({ operator: ops[i], servedBytes: 100 + i, amount: (i + 1) * 4e6 });
        }
        bytes memory message = _sweepShares(shares, 24e6);
        for (uint256 i = 0; i < 3; i++) {
            vm.expectEmit(true, true, true, true, address(receiver));
            emit SweepReceived(SRC, pool, ops[i], (i + 1) * 4e6);
        }
        receiver.receiveSweep(message, hex"01");

        for (uint256 i = 0; i < 3; i++) {
            assertEq(dstUsdc.balanceOf(ops[i]), (i + 1) * 3e6, "rebate");
            assertEq(router.bytesPerEpoch(ops[i], _epoch()), 100 + i, "credit");
        }
        assertEq(dstUsdc.balanceOf(treasury), 6e6, "treasury");
        assertEq(dstUsdc.balanceOf(address(receiver)), 0, "receiver keeps nothing");
    }

    function test_receiveSweep_maxBatch() public {
        uint256 max = receiver.MAX_SWEEP_BATCH();
        address[] memory ops = _activeOps(max);
        PaymentPoolBase.SweepShare[] memory shares = new PaymentPoolBase.SweepShare[](max);
        for (uint256 i = 0; i < max; i++) {
            shares[i] = PaymentPoolBase.SweepShare({ operator: ops[i], servedBytes: 7, amount: 1e6 });
        }
        receiver.receiveSweep(_sweepShares(shares, max * 1e6), hex"01");
        for (uint256 i = 0; i < max; i++) {
            assertEq(router.bytesPerEpoch(ops[i], _epoch()), 7, "credit");
        }
    }

    /// A bridge fee splits pro rata; the last entry takes the rounding
    /// remainder, so the deposits sum to what was minted.
    function test_receiveSweep_splitsFeeProRataRemainderToLast() public {
        address[] memory ops = _activeOps(3);
        PaymentPoolBase.SweepShare[] memory shares = new PaymentPoolBase.SweepShare[](3);
        shares[0] = PaymentPoolBase.SweepShare({ operator: ops[0], servedBytes: 1, amount: 300e6 });
        shares[1] = PaymentPoolBase.SweepShare({ operator: ops[1], servedBytes: 1, amount: 300e6 });
        shares[2] = PaymentPoolBase.SweepShare({ operator: ops[2], servedBytes: 1, amount: 400e6 });
        uint256 minted = 1000e6 - (1e6 + 1);

        vm.expectEmit(true, true, true, true, address(receiver));
        emit SweepReceived(SRC, pool, ops[0], 299_699_999);
        vm.expectEmit(true, true, true, true, address(receiver));
        emit SweepReceived(SRC, pool, ops[1], 299_699_999);
        vm.expectEmit(true, true, true, true, address(receiver));
        emit SweepReceived(SRC, pool, ops[2], minted - 2 * 299_699_999);
        receiver.receiveSweep(_usdcBurn(1e6 + 1, abi.encode(shares)), hex"01");

        uint256 total = dstUsdc.balanceOf(treasury);
        for (uint256 i = 0; i < 3; i++) {
            total += dstUsdc.balanceOf(ops[i]);
        }
        assertEq(total, minted, "every minted unit lands");
        assertEq(dstUsdc.balanceOf(address(receiver)), 0, "receiver keeps nothing");
    }

    /// An entry whose pro-rata share rounds to zero is skipped; the rest of
    /// the batch still lands.
    function test_receiveSweep_skipsZeroShare() public {
        address[] memory ops = _activeOps(2);
        PaymentPoolBase.SweepShare[] memory shares = new PaymentPoolBase.SweepShare[](2);
        shares[0] = PaymentPoolBase.SweepShare({ operator: ops[0], servedBytes: 11, amount: 1 });
        shares[1] = PaymentPoolBase.SweepShare({ operator: ops[1], servedBytes: 22, amount: 1000e6 - 1 });

        vm.expectEmit(true, true, true, true, address(receiver));
        emit SweepReceived(SRC, pool, ops[1], 999e6);
        receiver.receiveSweep(_usdcBurn(1e6, abi.encode(shares)), hex"01");

        assertEq(router.bytesPerEpoch(ops[0], _epoch()), 0, "zero share credits nothing");
        assertEq(router.bytesPerEpoch(ops[1], _epoch()), 22, "credit");
        assertEq(dstUsdc.balanceOf(address(receiver)), 0, "receiver keeps nothing");
    }

    function test_receiveSweep_rejectsEmptyBatch() public {
        bytes memory message = _usdcBurn(0, abi.encode(new PaymentPoolBase.SweepShare[](0)));
        vm.expectRevert(CctpSweepReceiver.MalformedMessage.selector);
        receiver.receiveSweep(message, hex"01");
    }

    function test_receiveSweep_rejectsOverMaxBatch() public {
        uint256 n = receiver.MAX_SWEEP_BATCH() + 1;
        PaymentPoolBase.SweepShare[] memory shares = new PaymentPoolBase.SweepShare[](n);
        for (uint256 i = 0; i < n; i++) {
            shares[i] = PaymentPoolBase.SweepShare({ operator: operator, servedBytes: 1, amount: 1 });
        }
        bytes memory message = _usdcBurn(0, abi.encode(shares));
        vm.expectRevert(CctpSweepReceiver.MalformedMessage.selector);
        receiver.receiveSweep(message, hex"01");
    }

    function test_receiveSweep_rejectsZeroSum() public {
        bytes memory message = _usdcBurn(0, abi.encode(_share(operator, 1, 0)));
        vm.expectRevert(CctpSweepReceiver.MalformedMessage.selector);
        receiver.receiveSweep(message, hex"01");
    }

    function test_receiveSweep_replayReverts() public {
        bytes memory message = _sweep(operator, 1, 1e6);
        receiver.receiveSweep(message, hex"01");
        vm.expectRevert("Nonce already used");
        receiver.receiveSweep(message, hex"01");
    }

    /// Only the receiver can receive the message: a third party calling the
    /// transmitter directly fails on `destinationCaller`.
    function test_directReceiveByOthersFails() public {
        bytes memory message = _sweep(operator, 1, 1e6);
        vm.prank(relayer);
        vm.expectRevert("Invalid caller for message");
        transmitter.receiveMessage(message, hex"01");
    }

    /// While FeeRouter is paused the receive reverts and the message stays
    /// valid: it lands after unpause.
    function test_pausedRouterLeavesMessageReceivable() public {
        bytes memory message = _sweep(operator, 9, 1e6);
        bytes32 role = router.PAUSER_ROLE();
        vm.startPrank(admin);
        router.grantRole(role, admin);
        router.pause();
        vm.stopPrank();

        vm.expectRevert();
        receiver.receiveSweep(message, hex"01");

        vm.prank(admin);
        router.unpause();
        receiver.receiveSweep(message, hex"01");
        assertEq(router.bytesPerEpoch(operator, _epoch()), 9);
    }
}
