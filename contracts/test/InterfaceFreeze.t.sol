// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { PaymentPool } from "../src/PaymentPool.sol";

/// @title InterfaceFreezeTest — public-surface stability snapshot for
///        `PaymentPool` (ADR 003). Each assertion pins a function's
///        compiler-derived selector to its canonical signature string; any
///        signature drift on this surface flips the selector and fails the
///        test, forcing a deliberate ABI change + audit re-review.
contract PaymentPoolInterfaceFreezeTest is Test {
    function test_paymentPool_abiFrozen() public view {
        assertEq(PaymentPool.openPool.selector, bytes4(keccak256("openPool(uint256)")), "openPool");
        assertEq(PaymentPool.topUp.selector, bytes4(keccak256("topUp(bytes32,uint256)")), "topUp");
        assertEq(
            PaymentPool.redeem.selector,
            bytes4(keccak256("redeem(bytes32,address,address,uint256,uint256,bytes,bytes)")),
            "redeem"
        );
        assertEq(
            PaymentPool.redeemMany.selector,
            bytes4(
                keccak256(
                    "redeemMany((bytes32,address,uint256,uint64,bytes)[],(bytes32,address,address,uint256,uint256,bytes)[])"
                )
            ),
            "redeemMany"
        );
        assertEq(PaymentPool.closePool.selector, bytes4(keccak256("closePool(bytes32)")), "closePool");
        assertEq(PaymentPool.reclaim.selector, bytes4(keccak256("reclaim(bytes32)")), "reclaim");
        assertEq(PaymentPool.getPool.selector, bytes4(keccak256("getPool(bytes32)")), "getPool");
        assertEq(
            PaymentPool.getAuthorization.selector,
            bytes4(keccak256("getAuthorization(bytes32,address)")),
            "getAuthorization"
        );
        assertEq(
            PaymentPool.getWatermark.selector,
            bytes4(keccak256("getWatermark(bytes32,address,address)")),
            "getWatermark"
        );
        assertEq(PaymentPool.getPools.selector, bytes4(keccak256("getPools(address,uint256,uint256)")), "getPools");
        // `ownerPoolNonce` is a public mapping — its auto-generated getter
        // selector is exposed only through an instance's function value, not
        // the type itself.
        assertEq(
            PaymentPool(address(0)).ownerPoolNonce.selector,
            bytes4(keccak256("ownerPoolNonce(address)")),
            "ownerPoolNonce"
        );
        assertEq(PaymentPool.getRateBounds.selector, bytes4(keccak256("getRateBounds()")), "getRateBounds");
        assertEq(PaymentPool.setFeeRouter.selector, bytes4(keccak256("setFeeRouter(address)")), "setFeeRouter");
        assertEq(
            PaymentPool.setDisputeWindow.selector, bytes4(keccak256("setDisputeWindow(uint256)")), "setDisputeWindow"
        );
        assertEq(PaymentPool.setRateBounds.selector, bytes4(keccak256("setRateBounds(uint256)")), "setRateBounds");
    }
}
