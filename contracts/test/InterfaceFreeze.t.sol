// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { PaymentPool } from "../src/PaymentPool.sol";

/// @title InterfaceFreezeTest — public-surface stability snapshot for
///        `PaymentPool` (ADR 003). Each assertion pins a function's
///        compiler-derived selector to its canonical signature string; any
///        signature drift on this surface flips the selector and fails the
///        test, forcing a deliberate ABI change + audit re-review.
/// @dev   Selectors are read through an instance's function values: members
///        `PaymentPool` inherits from `PaymentPoolBase`, and every public
///        variable's getter, are visible only that way.
contract PaymentPoolInterfaceFreezeTest is Test {
    function test_paymentPool_abiFrozen() public view {
        PaymentPool p = PaymentPool(address(0));
        assertEq(p.openPool.selector, bytes4(keccak256("openPool(uint64)")), "openPool");
        assertEq(p.topUp.selector, bytes4(keccak256("topUp(bytes32,uint64)")), "topUp");
        assertEq(
            p.redeemMany.selector,
            bytes4(
                keccak256(
                    "redeemMany((bytes32,(address,uint64,uint64,bytes)[],(address,uint64,uint64,bytes32,bytes32,bytes32,bytes32,uint256)[])[])"
                )
            ),
            "redeemMany"
        );
        assertEq(p.closePool.selector, bytes4(keccak256("closePool(bytes32)")), "closePool");
        assertEq(p.reclaim.selector, bytes4(keccak256("reclaim(bytes32)")), "reclaim");
        assertEq(p.sweep.selector, bytes4(keccak256("sweep(address[])")), "sweep");
        assertEq(p.pending.selector, bytes4(keccak256("pending(address)")), "pending");
        assertEq(p.pause.selector, bytes4(keccak256("pause()")), "pause");
        assertEq(p.unpause.selector, bytes4(keccak256("unpause()")), "unpause");

        assertEq(p.getPool.selector, bytes4(keccak256("getPool(bytes32)")), "getPool");
        assertEq(
            p.getAuthorization.selector, bytes4(keccak256("getAuthorization(bytes32,address)")), "getAuthorization"
        );
        assertEq(
            p.getAuthorizations.selector,
            bytes4(keccak256("getAuthorizations(bytes32[],address[])")),
            "getAuthorizations"
        );
        assertEq(p.getWatermark.selector, bytes4(keccak256("getWatermark(bytes32,address,address)")), "getWatermark");
        assertEq(
            p.getWatermarks.selector, bytes4(keccak256("getWatermarks(bytes32[],address[],address[])")), "getWatermarks"
        );
        assertEq(p.getPools.selector, bytes4(keccak256("getPools(address,uint256,uint256)")), "getPools");
        assertEq(p.ownerPoolNonce.selector, bytes4(keccak256("ownerPoolNonce(address)")), "ownerPoolNonce");

        assertEq(p.disputeWindow.selector, bytes4(keccak256("disputeWindow()")), "disputeWindow");
        assertEq(p.operatorBps.selector, bytes4(keccak256("operatorBps()")), "operatorBps");
        assertEq(p.minSweep.selector, bytes4(keccak256("minSweep()")), "minSweep");
        assertEq(p.minDeposit.selector, bytes4(keccak256("minDeposit()")), "minDeposit");
        assertEq(p.pauser.selector, bytes4(keccak256("pauser()")), "pauser");
        assertEq(p.MAX_SWEEP_BATCH.selector, bytes4(keccak256("MAX_SWEEP_BATCH()")), "MAX_SWEEP_BATCH");
        assertEq(p.tokenMessenger.selector, bytes4(keccak256("tokenMessenger()")), "tokenMessenger");
        assertEq(p.sweepRecipient.selector, bytes4(keccak256("sweepRecipient()")), "sweepRecipient");
        assertEq(p.sweepDomain.selector, bytes4(keccak256("sweepDomain()")), "sweepDomain");
    }
}
