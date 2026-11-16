// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Vm } from "forge-std/Vm.sol";

import { LoopbackCctp } from "../testnet/LoopbackCctp.sol";
import { CctpSweepReceiver } from "../src/CctpSweepReceiver.sol";
import { FeeRouter } from "../src/FeeRouter.sol";
import { IFeeRouter } from "../src/interfaces/IFeeRouter.sol";
import { MintableUSDC } from "./mocks/MintableUSDC.sol";
import { MockCreditBond } from "./mocks/MockCreditBond.sol";

/// @notice A single-chain deployment sweeps through `LoopbackCctp` into the
///         real receiver and router, the way the local anvil e2e does.
contract LoopbackCctpTest is Test {
    uint64 internal constant EPOCH = 7 days;

    function test_sweepLoopsBackIntoFeeRouter() public {
        vm.warp(EPOCH + 1);
        MintableUSDC usdc = new MintableUSDC();
        LoopbackCctp cctp = new LoopbackCctp(0);
        MockCreditBond bond = new MockCreditBond(EPOCH);
        address operator = address(0xB0B);
        bond.set(operator, true, 1000);
        FeeRouter router = new FeeRouter({
            usdc_: usdc,
            capacityBond_: bond,
            treasury_: address(0xD7),
            epochLength_: EPOCH,
            windowEpochs_: 13,
            deliveryFloor_: 1,
            admin: address(this),
            rebateBps_: 7500,
            buybackBps_: 0,
            buybackBurner_: address(0)
        });
        CctpSweepReceiver receiver = new CctpSweepReceiver(usdc, cctp, address(cctp), IFeeRouter(address(router)));

        // Stand in for PaymentPool.sweep: burn from a pool-like caller.
        address pool = address(0x9001);
        usdc.mint(pool, 40e6);
        vm.startPrank(pool);
        usdc.approve(address(cctp), 40e6);
        bytes32 to = bytes32(uint256(uint160(address(receiver))));
        vm.recordLogs();
        cctp.depositForBurnWithHook(40e6, 0, to, address(usdc), to, 0, 2000, abi.encode(operator, uint256(777)));
        vm.stopPrank();

        Vm.Log[] memory logs = vm.getRecordedLogs();
        bytes memory message = abi.decode(logs[logs.length - 1].data, (bytes));
        receiver.receiveSweep(message, hex"01");

        assertEq(usdc.balanceOf(operator), 30e6, "rebate");
        assertEq(router.bytesPerEpoch(operator, uint64(block.timestamp / EPOCH)), 777, "credit");
        vm.expectRevert("Nonce already used");
        receiver.receiveSweep(message, hex"01");
    }
}
