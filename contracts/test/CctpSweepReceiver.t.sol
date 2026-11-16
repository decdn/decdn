// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { CctpSweepReceiver } from "../src/CctpSweepReceiver.sol";
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

    /// @dev Burns `amount` on the source side the way `PaymentPool.sweep` does
    ///      and returns the message Circle would attest.
    function _sweep(address op, uint256 servedBytes, uint256 amount) internal returns (bytes memory) {
        srcUsdc.mint(pool, amount);
        vm.startPrank(pool);
        srcUsdc.approve(address(srcMessenger), amount);
        bytes32 to = CctpMessage.toBytes32(address(receiver));
        srcMessenger.depositForBurnWithHook(amount, DST, to, address(srcUsdc), to, 0, 2000, abi.encode(op, servedBytes));
        vm.stopPrank();
        return srcMessenger.lastMessage();
    }

    function _burn(bytes32 recipient, bytes32 mintRecipient, bytes32 burnToken, uint256 fee)
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
                hookData: abi.encode(operator, uint256(5))
            })
        );
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
