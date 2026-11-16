// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";

import { PaymentPool } from "../src/PaymentPool.sol";
import { PaymentPoolBase } from "../src/PaymentPoolBase.sol";
import { SunsettingPausable } from "../src/SunsettingPausable.sol";
import { CctpSweepReceiver } from "../src/CctpSweepReceiver.sol";
import { FeeRouter } from "../src/FeeRouter.sol";
import { IFeeRouter } from "../src/interfaces/IFeeRouter.sol";
import { ITokenMessengerV2 } from "../src/interfaces/ICctpV2.sol";
import { MintableUSDC } from "./mocks/MintableUSDC.sol";
import { MockCreditBond } from "./mocks/MockCreditBond.sol";
import { CctpMessage, MockMessageTransmitterV2, MockTokenMessengerV2 } from "./mocks/MockCctp.sol";

/// @notice The payment-chain side of the split: the operator share paid at
///         redemption, `pending`, `sweep`, and the immutable pause.
contract PaymentPoolSweepTest is Test {
    uint32 internal constant SRC = 3;
    uint32 internal constant DST = 0;
    uint64 internal constant EPOCH = 7 days;
    uint256 internal constant OPERATOR_BPS = 6000;
    uint64 internal constant MIN_SWEEP = 10e6;
    uint64 internal constant DEPOSIT = 1000e6;

    uint256 internal constant OWNER_PK = 0xC11E27;
    uint256 internal constant SIGNER_PK = 0x519E7;
    bytes32 internal constant CAPABILITY_TYPEHASH =
        keccak256("Capability(address signer,uint256 spendingCap,bytes32 poolId,uint64 expiry)");
    bytes32 internal constant VOUCHER_TYPEHASH = keccak256(
        "Voucher(bytes32 poolId,address signer,address provider,uint256 amount,uint256 bytesDelivered,bytes32 chainRoot,uint256 chunkPrice)"
    );

    MintableUSDC internal srcUsdc;
    MintableUSDC internal dstUsdc;
    MockTokenMessengerV2 internal srcMessenger;
    MockTokenMessengerV2 internal dstMessenger;
    MockMessageTransmitterV2 internal transmitter;
    MockCreditBond internal bond;
    FeeRouter internal router;
    CctpSweepReceiver internal receiver;
    PaymentPool internal pool;

    address internal owner;
    address internal signer;
    address internal operator = address(0xB0B);
    address internal pauser = address(0xDEAD);
    address internal stranger = address(0x5747A);
    address internal treasury = address(0xD7);
    uint64 internal expiry;

    event Swept(address indexed operator, uint256 servedBytes, uint256 amount, uint256 maxFee);

    function setUp() public {
        vm.warp(EPOCH + 1);
        owner = vm.addr(OWNER_PK);
        signer = vm.addr(SIGNER_PK);
        expiry = uint64(block.timestamp + 365 days);

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
            admin: address(this),
            rebateBps_: 7500,
            buybackBps_: 0,
            buybackBurner_: address(0)
        });
        receiver = new CctpSweepReceiver(dstUsdc, transmitter, address(dstMessenger), IFeeRouter(address(router)));
        pool = _deploy(srcMessenger, address(receiver), OPERATOR_BPS, pauser);

        srcUsdc.mint(owner, 100_000e6);
        vm.prank(owner);
        srcUsdc.approve(address(pool), type(uint256).max);
    }

    function _deploy(ITokenMessengerV2 messenger, address recipient, uint256 bps, address pauser_)
        internal
        returns (PaymentPool)
    {
        return new PaymentPool({
            usdc_: srcUsdc,
            tokenMessenger_: messenger,
            sweepRecipient_: recipient,
            sweepDomain_: DST,
            disputeWindow_: 48 hours,
            operatorBps_: bps,
            minSweep_: MIN_SWEEP,
            minDeposit_: 0,
            pauser_: pauser_
        });
    }

    // -----------------------------------------------------------------
    // Signing
    // -----------------------------------------------------------------

    function _digest(bytes32 structHash) internal view returns (bytes32) {
        bytes32 domainSeparator = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes("PaymentPool")),
                keccak256(bytes("1")),
                block.chainid,
                address(pool)
            )
        );
        return keccak256(abi.encodePacked("\x19\x01", domainSeparator, structHash));
    }

    function _open() internal returns (bytes32 poolId) {
        vm.prank(owner);
        poolId = pool.openPool(DEPOSIT);
    }

    /// @dev One-lane redemption by `payee` of a voucher at `amount` /
    ///      `servedBytes` cumulative, registering the signer on first use.
    function _redeem(bytes32 poolId, address payee, uint64 amount, uint64 servedBytes) internal returns (uint256 paid) {
        PaymentPoolBase.CapabilityReg[] memory caps = new PaymentPoolBase.CapabilityReg[](1);
        {
            bytes32 capHash = keccak256(abi.encode(CAPABILITY_TYPEHASH, signer, uint64(500e6), poolId, expiry));
            (uint8 cv, bytes32 cr, bytes32 cs) = vm.sign(OWNER_PK, _digest(capHash));
            caps[0] = PaymentPoolBase.CapabilityReg({
                signer: signer, spendingCap: 500e6, expiry: expiry, ownerSig: abi.encodePacked(cr, cs, cv)
            });
        }
        PaymentPoolBase.LaneVoucher[] memory v = new PaymentPoolBase.LaneVoucher[](1);
        {
            bytes32 vHash = keccak256(
                abi.encode(VOUCHER_TYPEHASH, poolId, signer, payee, amount, servedBytes, bytes32(0), uint256(0))
            );
            (uint8 vv, bytes32 vr, bytes32 vs) = vm.sign(SIGNER_PK, _digest(vHash));
            v[0] = PaymentPoolBase.LaneVoucher({
                signer: signer,
                cumulative: amount,
                bytesDelivered: servedBytes,
                r: vr,
                vs: bytes32(uint256(vs) | (uint256(vv - 27) << 255)),
                chainRoot: bytes32(0),
                preimage: bytes32(0),
                chainMeter: 0
            });
        }
        PaymentPoolBase.PoolBatch[] memory batches = new PaymentPoolBase.PoolBatch[](1);
        batches[0] = PaymentPoolBase.PoolBatch({ poolId: poolId, capabilities: caps, vouchers: v });
        vm.prank(payee);
        return pool.redeemMany(batches);
    }

    function _pending(address op) internal view returns (uint256 servedBytes, uint256 usdc) {
        return pool.pending(op);
    }

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    function test_constructor_setsImmutables() public view {
        assertEq(address(pool.usdc()), address(srcUsdc));
        assertEq(address(pool.tokenMessenger()), address(srcMessenger));
        assertEq(pool.sweepRecipient(), address(receiver));
        assertEq(pool.sweepDomain(), DST);
        assertEq(pool.disputeWindow(), 48 hours);
        assertEq(pool.operatorBps(), OPERATOR_BPS);
        assertEq(pool.minSweep(), MIN_SWEEP);
        assertEq(pool.minDeposit(), 0);
        assertEq(pool.pauser(), pauser);
    }

    function test_constructor_rejectsZeroAddresses() public {
        vm.expectRevert(PaymentPoolBase.ZeroAddress.selector);
        _deploy(ITokenMessengerV2(address(0)), address(receiver), OPERATOR_BPS, pauser);
        vm.expectRevert(PaymentPoolBase.ZeroAddress.selector);
        _deploy(srcMessenger, address(0), OPERATOR_BPS, pauser);
        vm.expectRevert(PaymentPoolBase.ZeroAddress.selector);
        _deploy(srcMessenger, address(receiver), OPERATOR_BPS, address(0));
    }

    function test_constructor_rejectsOperatorBpsAboveWhole() public {
        vm.expectRevert(abi.encodeWithSelector(PaymentPoolBase.OperatorBpsAboveWhole.selector, uint256(10_001)));
        _deploy(srcMessenger, address(receiver), 10_001, pauser);
    }

    // -----------------------------------------------------------------
    // Redemption payout
    // -----------------------------------------------------------------

    function test_redeem_paysOperatorShareAndAccruesTheRest() public {
        bytes32 id = _open();
        uint256 paid = _redeem(id, operator, 100e6, 5_000_000);
        assertEq(paid, 100e6);
        assertEq(srcUsdc.balanceOf(operator), 60e6, "operatorBps same-tx");
        (uint256 b, uint256 u) = _pending(operator);
        assertEq(u, 40e6, "protocol share pending");
        assertEq(b, 5_000_000, "bytes pending, unclamped");
        assertEq(srcUsdc.balanceOf(address(pool)), DEPOSIT - 60e6, "pool keeps deposit minus payout");
    }

    /// Any address can be a payee: the pool reads no bond.
    function test_redeem_needsNoBond() public {
        bytes32 id = _open();
        _redeem(id, stranger, 10e6, 1);
        assertEq(srcUsdc.balanceOf(stranger), 6e6);
    }

    function test_redeem_pendingAccumulates() public {
        bytes32 id = _open();
        _redeem(id, operator, 10e6, 100);
        _redeem(id, operator, 30e6, 300);
        (uint256 b, uint256 u) = _pending(operator);
        assertEq(u, 12e6);
        assertEq(b, 300);
    }

    function test_redeem_operatorBpsWholeLeavesNothingPending() public {
        pool = _deploy(srcMessenger, address(receiver), 10_000, pauser);
        vm.prank(owner);
        srcUsdc.approve(address(pool), type(uint256).max);
        bytes32 id = _open();
        _redeem(id, operator, 10e6, 7);
        (uint256 b, uint256 u) = _pending(operator);
        assertEq(u, 0);
        assertEq(b, 7);
        assertEq(srcUsdc.balanceOf(operator), 10e6);
    }

    // -----------------------------------------------------------------
    // Sweep
    // -----------------------------------------------------------------

    function test_sweep_revertsBelowMinSweep() public {
        bytes32 id = _open();
        _redeem(id, operator, 20e6, 1);
        vm.expectRevert(
            abi.encodeWithSelector(PaymentPoolBase.BelowMinSweep.selector, uint256(8e6), uint256(MIN_SWEEP))
        );
        pool.sweep(operator);
    }

    function test_sweep_burnsPendingToReceiverAndResets() public {
        bytes32 id = _open();
        _redeem(id, operator, 100e6, 5_000_000);

        vm.expectEmit(true, true, true, true, address(pool));
        emit Swept(operator, 5_000_000, 40e6, 0);
        vm.prank(stranger);
        pool.sweep(operator);

        (uint256 b, uint256 u) = _pending(operator);
        assertEq(u, 0, "pending usdc reset");
        assertEq(b, 0, "pending bytes reset");
        assertEq(srcUsdc.balanceOf(address(pool)), DEPOSIT - 100e6, "protocol share left the pool");

        bytes memory m = srcMessenger.lastMessage();
        assertEq(uint32(bytes4(_slice(m, 8, 4))), DST, "destination domain");
        assertEq(bytes32(_slice(m, 108, 32)), CctpMessage.toBytes32(address(receiver)), "destinationCaller");
        assertEq(bytes32(_slice(m, 148 + 36, 32)), CctpMessage.toBytes32(address(receiver)), "mintRecipient");
        assertEq(uint256(bytes32(_slice(m, 148 + 68, 32))), 40e6, "amount");
        assertEq(bytes32(_slice(m, 148 + 100, 32)), CctpMessage.toBytes32(address(pool)), "messageSender");
        assertEq(uint256(bytes32(_slice(m, 148 + 132, 32))), 0, "maxFee without getMinFeeAmount");
        assertEq(_slice(m, 148 + 228, 64), abi.encode(operator, uint256(5_000_000)), "hookData");
    }

    function test_sweep_usesMessengerMinFeeWhenPresent() public {
        srcMessenger.setMinFee(14); // 1.4 bps
        bytes32 id = _open();
        _redeem(id, operator, 500e6, 1);
        pool.sweep(operator);
        bytes memory m = srcMessenger.lastMessage();
        assertEq(uint256(bytes32(_slice(m, 148 + 132, 32))), (200e6 * 14) / 10_000, "maxFee = getMinFeeAmount");
    }

    /// The sweep lands end to end: CCTP mints to the receiver, which deposits
    /// into FeeRouter and credits the operator.
    function test_sweep_landsInFeeRouter() public {
        bytes32 id = _open();
        _redeem(id, operator, 100e6, 5_000_000);
        pool.sweep(operator);
        receiver.receiveSweep(srcMessenger.lastMessage(), hex"01");

        assertEq(dstUsdc.balanceOf(operator), 30e6, "rebate on the governance chain");
        assertEq(dstUsdc.balanceOf(treasury), 10e6, "treasury remainder");
        assertEq(router.bytesPerEpoch(operator, uint64(block.timestamp / EPOCH)), 5_000_000, "credit");
    }

    // -----------------------------------------------------------------
    // Pause
    // -----------------------------------------------------------------

    function test_pause_onlyPauser() public {
        vm.prank(stranger);
        vm.expectRevert(PaymentPoolBase.NotPauser.selector);
        pool.pause();
    }

    function test_pause_blocksInflowsOnly() public {
        bytes32 id = _open();
        _redeem(id, operator, 50e6, 1);
        vm.prank(pauser);
        pool.pause();

        vm.prank(owner);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        pool.openPool(DEPOSIT);
        vm.prank(owner);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        pool.topUp(id, 1e6);

        _redeem(id, operator, 100e6, 2);
        pool.sweep(operator);
        assertEq(srcUsdc.balanceOf(operator), 60e6, "redeem runs while paused");

        vm.prank(pauser);
        pool.unpause();
        vm.prank(owner);
        pool.topUp(id, 1e6);
    }

    function test_pause_sunsets() public {
        vm.warp(block.timestamp + 366 days);
        vm.prank(pauser);
        vm.expectRevert(SunsettingPausable.PauseExpired.selector);
        pool.pause();
    }

    function _slice(bytes memory b, uint256 start, uint256 len) internal pure returns (bytes memory out) {
        out = new bytes(len);
        for (uint256 i = 0; i < len; i++) {
            out[i] = b[start + i];
        }
    }
}
