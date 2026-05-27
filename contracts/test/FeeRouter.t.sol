// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";

import { FeeRouter, ICapacityBondReporter } from "../src/FeeRouter.sol";

contract MockUSDC is ERC20 {
    constructor() ERC20("USDC", "USDC") {
        _mint(msg.sender, 1_000_000_000e6);
    }

    function decimals() public pure override returns (uint8) {
        return 6;
    }
}

contract MockBondReporter is ICapacityBondReporter {
    address[] public reported;

    function recordSettlement(address operator) external override {
        reported.push(operator);
    }

    function reportedCount() external view returns (uint256) {
        return reported.length;
    }
}

contract FeeRouterTest is Test {
    MockUSDC internal usdc;
    MockBondReporter internal bondReporter;
    FeeRouter internal router;

    address internal admin = address(0xA11CE);
    address internal channel = address(0xCAFE);
    address internal operator = address(0xB0B);
    address internal treasury = address(0xD7);
    address internal safety = address(0x5A);
    address internal buyback = address(0xBB);

    uint64 internal constant EPOCH = 7 days;

    function setUp() public {
        usdc = new MockUSDC();
        bondReporter = new MockBondReporter();

        // 6000 / 2500 / 1000 / 500 — ADR 026 steady-state default.
        uint256[4] memory shares = [uint256(6000), uint256(2500), uint256(1000), uint256(500)];

        router = new FeeRouter({
            usdc_: usdc,
            capacityBond_: bondReporter,
            treasury_: treasury,
            epochLength_: EPOCH,
            windowEpochs_: 13,
            admin: admin,
            initialShares: shares,
            safetyReserve_: safety,
            buybackBurner_: buyback
        });

        vm.startPrank(admin);
        router.grantRole(router.ROUTER_CALLER_ROLE(), channel);
        vm.stopPrank();

        // Fund the channel with USDC and approve router.
        usdc.transfer(channel, 1_000_000e6);
        vm.prank(channel);
        usdc.approve(address(router), type(uint256).max);
    }

    function test_routeSettlement_distributesFourLegs() public {
        uint256 amount = 1000e6;
        uint256 bytesDelivered = 100_000_000;

        vm.warp(EPOCH + 1);
        vm.prank(channel);
        router.routeSettlement(operator, bytesDelivered, amount);

        // Shares: 6000/2500/1000/500.
        assertEq(usdc.balanceOf(operator), 600e6);
        assertEq(usdc.balanceOf(buyback), 250e6);
        assertEq(usdc.balanceOf(treasury), 100e6);
        assertEq(usdc.balanceOf(safety), 50e6);

        // Settlement reporter invoked.
        assertEq(bondReporter.reportedCount(), 1);
        assertEq(bondReporter.reported(0), operator);
    }

    function test_routeSettlement_incrementsBytesPerEpoch() public {
        vm.warp(EPOCH + 1);
        vm.prank(channel);
        router.routeSettlement(operator, 100, 1000e6);

        uint64 epoch = uint64((EPOCH + 1) / EPOCH);
        assertEq(router.bytesPerEpoch(operator, epoch), 100);
        assertEq(router.totalBytesPerEpoch(epoch), 100);
    }

    function test_bytesInWindow_sumsTrailingWindow() public {
        // Drive 4 epochs of settlement.
        for (uint64 i = 1; i <= 4; i++) {
            vm.warp(uint256(i) * EPOCH + 1);
            vm.prank(channel);
            router.routeSettlement(operator, i * 1000, 100e6);
        }
        // Window of 4 ending at epoch 4: should sum 1k+2k+3k+4k = 10k.
        assertEq(router.bytesInWindow(operator, 4, 4), 10_000);
        assertEq(router.totalBytesInWindow(4, 4), 10_000);
    }

    function test_bytesInWindow_zeroWindowReturnsZero() public view {
        assertEq(router.bytesInWindow(operator, 100, 0), 0);
        assertEq(router.totalBytesInWindow(100, 0), 0);
    }

    function test_setWindowEpochs_enforcesBounds() public {
        vm.prank(admin);
        vm.expectRevert();
        router.setWindowEpochs(3);

        vm.prank(admin);
        vm.expectRevert();
        router.setWindowEpochs(27);

        vm.prank(admin);
        router.setWindowEpochs(20);
        assertEq(router.windowEpochs(), 20);
    }

    function test_setShares_rejectsSumNot10000() public {
        uint256[4] memory bad = [uint256(5000), uint256(2500), uint256(1000), uint256(500)];
        vm.prank(admin);
        vm.expectRevert();
        router.setShares(bad);
    }

    function test_setShares_rejectsNonZeroShareWithoutDestination() public {
        // Clear safety reserve while non-zero share — should revert.
        vm.startPrank(admin);
        vm.expectRevert();
        router.setSafetyReserve(address(0));
        vm.stopPrank();
    }
}
