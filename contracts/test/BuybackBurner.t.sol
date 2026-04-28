// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { TOKEN } from "../src/TOKEN.sol";
import { BuybackBurner } from "../src/BuybackBurner.sol";
import { Errors } from "../src/libraries/Errors.sol";
import { Roles } from "../src/libraries/Roles.sol";

import { MockUSDC } from "./mocks/MockUSDC.sol";

contract BuybackBurnerTest is Test {
    TOKEN internal token;
    MockUSDC internal usdc;
    BuybackBurner internal bb;

    address internal admin = makeAddr("admin");
    address internal keeper = makeAddr("keeper");
    address internal treasury = makeAddr("treasury");
    address internal router = makeAddr("router");

    function setUp() public {
        token = new TOKEN(address(this), 1_000_000e18, address(this));
        usdc = new MockUSDC();
        bb = new BuybackBurner(IERC20(address(token)), IERC20(address(usdc)), router, admin);

        vm.prank(admin);
        bb.grantRole(Roles.KEEPER_ROLE, keeper);
        usdc.mint(treasury, 1_000_000e6);
        vm.prank(treasury);
        usdc.approve(address(bb), type(uint256).max);
    }

    function test_Constructor_StoresImmutables() public view {
        assertEq(address(bb.TOKEN_CONTRACT()), address(token));
        assertEq(address(bb.USDC()), address(usdc));
        assertEq(bb.BALANCER_V3_ROUTER(), router);
        assertEq(bb.slippageToleranceBps(), 200);
        assertEq(bb.minBuybackAmount(), 1000e6);
        assertEq(bb.maxBuybackAmount(), 100_000e6);
    }

    function test_Constructor_RevertsOnZero() public {
        vm.expectRevert(Errors.ZeroAddress.selector);
        new BuybackBurner(IERC20(address(0)), IERC20(address(usdc)), router, admin);
        vm.expectRevert(Errors.ZeroAddress.selector);
        new BuybackBurner(IERC20(address(token)), IERC20(address(0)), router, admin);
        vm.expectRevert(Errors.ZeroAddress.selector);
        new BuybackBurner(IERC20(address(token)), IERC20(address(usdc)), router, address(0));
    }

    function test_DepositUSDC_AccumulatesBalance() public {
        vm.prank(treasury);
        bb.depositUSDC(10_000e6);
        assertEq(usdc.balanceOf(address(bb)), 10_000e6);
    }

    function test_DepositUSDC_RevertsOnZero() public {
        vm.expectRevert(Errors.ZeroAmount.selector);
        vm.prank(treasury);
        bb.depositUSDC(0);
    }

    function test_ExecuteBuyback_RevertsDisabled_PoC() public {
        vm.expectRevert(BuybackBurner.BuybackDisabled.selector);
        vm.prank(keeper);
        bb.executeBuyback(1000e6, 0);
    }

    function test_ExecuteBuyback_RequiresKeeperRole() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                address(this),
                Roles.KEEPER_ROLE
            )
        );
        bb.executeBuyback(1000e6, 0);
    }

    function test_SetPool() public {
        address p = makeAddr("pool");
        vm.prank(admin);
        bb.setPool(p);
        assertEq(bb.pool(), p);
    }

    function test_SetSlippageTolerance_Bounds() public {
        vm.prank(admin);
        bb.setSlippageToleranceBps(500);
        assertEq(bb.slippageToleranceBps(), 500);

        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        bb.setSlippageToleranceBps(1001);
    }

    function test_SetMinMax_MonotonicityEnforced() public {
        vm.startPrank(admin);
        // min cannot exceed current max
        vm.expectRevert(Errors.OutOfBounds.selector);
        bb.setMinBuybackAmount(200_000e6);

        // max cannot drop below current min
        vm.expectRevert(Errors.OutOfBounds.selector);
        bb.setMaxBuybackAmount(500e6);

        bb.setMaxBuybackAmount(500_000e6);
        bb.setMinBuybackAmount(5000e6);
        assertEq(bb.minBuybackAmount(), 5000e6);
        assertEq(bb.maxBuybackAmount(), 500_000e6);
        vm.stopPrank();
    }

    function test_Pause_BlocksDeposits() public {
        vm.prank(admin);
        bb.pause();
        vm.expectRevert();
        vm.prank(treasury);
        bb.depositUSDC(1000e6);
    }
}
