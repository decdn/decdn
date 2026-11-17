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

    event Swept(address indexed operator, uint256 servedBytes, uint256 amount);
    event SweepSent(uint256 amount, uint256 maxFee);

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

    function _one(address op) internal pure returns (address[] memory ops) {
        ops = new address[](1);
        ops[0] = op;
    }

    /// @dev `n` distinct active operators, each with a redeemed voucher of
    ///      `amount` / `servedBytes` on `poolId`.
    function _operators(bytes32 poolId, uint256 n, uint64 amount, uint64 servedBytes)
        internal
        returns (address[] memory ops)
    {
        ops = new address[](n);
        for (uint256 i = 0; i < n; i++) {
            ops[i] = address(uint160(0x10000 + i));
            bond.set(ops[i], true, 1000);
            _redeem(poolId, ops[i], amount, servedBytes);
        }
    }

    /// @dev The hook data of the last burn, decoded.
    function _lastShares() internal view returns (PaymentPoolBase.SweepShare[] memory) {
        bytes memory m = srcMessenger.lastMessage();
        return abi.decode(_slice(m, 148 + 228, m.length - 148 - 228), (PaymentPoolBase.SweepShare[]));
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
        pool.sweep(_one(operator));
    }

    function test_sweep_burnsPendingToReceiverAndResets() public {
        bytes32 id = _open();
        _redeem(id, operator, 100e6, 5_000_000);

        vm.expectEmit(true, true, true, true, address(pool));
        emit Swept(operator, 5_000_000, 40e6);
        vm.expectEmit(true, true, true, true, address(pool));
        emit SweepSent(40e6, 0);
        vm.prank(stranger);
        pool.sweep(_one(operator));

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
        PaymentPoolBase.SweepShare[] memory want = new PaymentPoolBase.SweepShare[](1);
        want[0] = PaymentPoolBase.SweepShare({ operator: operator, servedBytes: 5_000_000, amount: 40e6 });
        assertEq(_slice(m, 148 + 228, m.length - 148 - 228), abi.encode(want), "hookData");
    }

    function test_sweep_usesMessengerMinFeeOnTheTotal() public {
        srcMessenger.setMinFee(14); // 1.4 bps
        bytes32 id = _open();
        address[] memory ops = _operators(id, 2, 250e6, 1);
        pool.sweep(ops);
        bytes memory m = srcMessenger.lastMessage();
        assertEq(uint256(bytes32(_slice(m, 148 + 132, 32))), (200e6 * 14) / 10_000, "maxFee = getMinFeeAmount");
    }

    /// Each share is below `minSweep`; together they reach it.
    function test_sweep_batchOfTinySharesReachesMinSweep() public {
        bytes32 id = _open();
        address[] memory ops = _operators(id, 5, 5e6, 7);
        (, uint256 u) = _pending(ops[0]);
        assertLt(u, MIN_SWEEP, "each share alone is below minSweep");

        pool.sweep(ops);

        for (uint256 i = 0; i < ops.length; i++) {
            (uint256 b, uint256 left) = _pending(ops[i]);
            assertEq(left, 0, "pending usdc reset");
            assertEq(b, 0, "pending bytes reset");
        }
        bytes memory m = srcMessenger.lastMessage();
        assertEq(uint256(bytes32(_slice(m, 148 + 68, 32))), 10e6, "one burn of the total");
        PaymentPoolBase.SweepShare[] memory shares = _lastShares();
        assertEq(shares.length, 5);
        for (uint256 i = 0; i < shares.length; i++) {
            assertEq(shares[i].operator, ops[i], "input order");
            assertEq(shares[i].servedBytes, 7);
            assertEq(shares[i].amount, 2e6);
        }
    }

    function test_sweep_emitsSweptPerOperator() public {
        bytes32 id = _open();
        address[] memory ops = _operators(id, 3, 10e6, 9);
        for (uint256 i = 0; i < ops.length; i++) {
            vm.expectEmit(true, true, true, true, address(pool));
            emit Swept(ops[i], 9, 4e6);
        }
        vm.expectEmit(true, true, true, true, address(pool));
        emit SweepSent(12e6, 0);
        pool.sweep(ops);
    }

    function test_sweep_maxBatchSucceeds() public {
        bytes32 id = _open();
        uint256 max = pool.MAX_SWEEP_BATCH();
        address[] memory ops = _operators(id, max, 10e6, 3);
        pool.sweep(ops);
        assertEq(_lastShares().length, max);
        // The burn body stays under CCTP V2's 8192-byte `maxMessageBodySize`.
        assertEq(srcMessenger.lastMessage().length - 148, 3364, "message body size");
    }

    function test_sweep_revertsEmpty() public {
        vm.expectRevert(abi.encodeWithSelector(PaymentPoolBase.BadSweepBatch.selector, uint256(0)));
        pool.sweep(new address[](0));
    }

    function test_sweep_revertsAboveMax() public {
        bytes32 id = _open();
        uint256 max = pool.MAX_SWEEP_BATCH();
        address[] memory ops = _operators(id, max + 1, 1e6, 1);
        vm.expectRevert(abi.encodeWithSelector(PaymentPoolBase.BadSweepBatch.selector, max + 1));
        pool.sweep(ops);
    }

    function test_sweep_revertsOnDuplicate() public {
        bytes32 id = _open();
        _redeem(id, operator, 100e6, 1);
        address[] memory ops = new address[](2);
        ops[0] = operator;
        ops[1] = operator;
        vm.expectRevert(abi.encodeWithSelector(PaymentPoolBase.NothingPending.selector, operator));
        pool.sweep(ops);
    }

    function test_sweep_revertsOnEntryWithNothingPending() public {
        bytes32 id = _open();
        _redeem(id, operator, 100e6, 1);
        address[] memory ops = new address[](2);
        ops[0] = operator;
        ops[1] = stranger;
        vm.expectRevert(abi.encodeWithSelector(PaymentPoolBase.NothingPending.selector, stranger));
        pool.sweep(ops);
    }

    /// The sweep lands end to end: CCTP mints to the receiver, which deposits
    /// into FeeRouter and credits each operator.
    function test_sweep_landsInFeeRouter() public {
        bytes32 id = _open();
        address other = address(0xCAFE);
        bond.set(other, true, 1000);
        _redeem(id, operator, 100e6, 5_000_000);
        _redeem(id, other, 50e6, 2_000_000);
        address[] memory ops = new address[](2);
        ops[0] = operator;
        ops[1] = other;
        pool.sweep(ops);
        receiver.receiveSweep(srcMessenger.lastMessage(), hex"01");

        uint64 epoch = uint64(block.timestamp / EPOCH);
        assertEq(dstUsdc.balanceOf(operator), 30e6, "operator rebate on the governance chain");
        assertEq(dstUsdc.balanceOf(other), 15e6, "other rebate on the governance chain");
        assertEq(dstUsdc.balanceOf(treasury), 15e6, "treasury remainder");
        assertEq(router.bytesPerEpoch(operator, epoch), 5_000_000, "operator credit");
        assertEq(router.bytesPerEpoch(other, epoch), 2_000_000, "other credit");
    }

    /// Gas of a full batch on both chains, logged per operator. The receive
    /// runs against cold storage, the way a fresh transaction meets it.
    function test_sweep_gasPerOperator() public {
        bytes32 id = _open();
        uint256 max = pool.MAX_SWEEP_BATCH();
        // Two 6 USDC shares make the smallest batch that reaches `minSweep`.
        address[] memory ops = _operators(id, max, 15e6, 3);
        address[] memory pair = new address[](2);
        pair[0] = ops[0];
        pair[1] = ops[1];
        address[] memory rest = new address[](max - 2);
        for (uint256 i = 2; i < max; i++) {
            rest[i - 2] = ops[i];
        }

        uint256 sendPair = _sweepGas(pair);
        bytes memory messagePair = srcMessenger.lastMessage();
        uint256 sendRest = _sweepGas(rest);
        bytes memory messageRest = srcMessenger.lastMessage();
        uint256 receivePair = _receiveGas(messagePair);
        uint256 receiveRest = _receiveGas(messageRest);

        uint256 sendPerOp = (sendRest - sendPair) / (max - 4);
        uint256 receivePerOp = (receiveRest - receivePair) / (max - 4);
        emit log_named_uint("sweep: gas per operator", sendPerOp);
        emit log_named_uint("sweep: fixed gas", sendPair - 2 * sendPerOp);
        emit log_named_uint("receiveSweep: gas per operator", receivePerOp);
        emit log_named_uint("receiveSweep: fixed gas", receivePair - 2 * receivePerOp);
        emit log_named_uint("receiveSweep: full batch estimate", receivePair + receivePerOp * (max - 2));
        assertLt(receivePair + receivePerOp * (max - 2), 5_000_000, "a full batch receive stays far under a block");
    }

    function _coolAll() internal {
        vm.cool(address(pool));
        vm.cool(address(srcUsdc));
        vm.cool(address(srcMessenger));
        vm.cool(address(dstUsdc));
        vm.cool(address(dstMessenger));
        vm.cool(address(transmitter));
        vm.cool(address(router));
        vm.cool(address(receiver));
        vm.cool(address(bond));
    }

    function _sweepGas(address[] memory ops) internal returns (uint256) {
        _coolAll();
        uint256 g = gasleft();
        pool.sweep(ops);
        return g - gasleft();
    }

    function _receiveGas(bytes memory message) internal returns (uint256) {
        _coolAll();
        uint256 g = gasleft();
        receiver.receiveSweep(message, hex"01");
        return g - gasleft();
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
        pool.sweep(_one(operator));
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
