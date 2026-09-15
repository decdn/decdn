// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { BuybackBurner } from "../src/BuybackBurner.sol";
import { SunsettingPausable } from "../src/SunsettingPausable.sol";
import { Token } from "../src/Token.sol";

import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";

contract MockUSDC is ERC20 {
    constructor() ERC20("USDC", "USDC") {
        _mint(msg.sender, 1_000_000_000e6);
    }

    function decimals() public pure override returns (uint8) {
        return 6;
    }
}

/// @notice Test-only TOKEN faucet that hands a configurable amount to the
///         caller. Lets `TestableBuybackBurner._performSwap` simulate "the
///         venue transferred X TOKEN to the BuybackBurner contract" without
///         requiring a live pool. Pre-funded from the test's TOKEN holder in
///         `setUp`.
contract TokenSource {
    IERC20 internal token;

    constructor(IERC20 token_) {
        token = token_;
    }

    function feed(uint256 amount) external {
        if (amount != 0) {
            // slither-disable-next-line unchecked-transfer
            token.transfer(msg.sender, amount);
        }
    }
}

/// @notice Concrete `BuybackBurner` subclass for tests of the venue-neutral
///         base. `_performSwap` pulls `actualTransfer` TOKEN from the
///         `TokenSource` into this contract (simulating the venue's outbound
///         transfer leg) and returns `reportedReturn` (simulating the venue's
///         reported post-swap amount). Decoupling the two fields lets the test
///         exercise every revert branch in `executeBuyback`:
///           - `actualTransfer == 0` → `SwapNotImplemented`
///           - `reportedReturn != actualTransfer` → `SwapReportMismatch`
///           - both equal and non-zero → happy-path burn + event
///         A plain `wired` flag stands in for the subclass `_requireWired`
///         hook so the base `PoolNotWired` path is exercisable without a venue.
contract TestableBuybackBurner is BuybackBurner {
    TokenSource internal source;
    uint256 public actualTransfer;
    uint256 public reportedReturn;
    bool public wired;

    constructor(IERC20 usdc_, ERC20Burnable token_, address admin, address treasury_, TokenSource source_)
        BuybackBurner(usdc_, token_, admin, treasury_)
    {
        source = source_;
    }

    function setSwap(uint256 actual_, uint256 reported_) external {
        actualTransfer = actual_;
        reportedReturn = reported_;
    }

    function setWired(bool wired_) external {
        wired = wired_;
    }

    function _performSwap(uint256, uint256) internal override returns (uint256) {
        source.feed(actualTransfer);
        return reportedReturn;
    }

    function _requireWired() internal view override {
        if (!wired) revert PoolNotWired();
    }
}

/// @title BuybackBurner smoke tests
/// @notice Coverage for the venue-neutral `BuybackBurner` via a concrete
///         `TestableBuybackBurner`. Covers `executeBuyback` happy + revert
///         paths (including the I1 trust-boundary check on the subclass's
///         reported `tokenOut` and the `_requireWired` gate), `pause`/`unpause`
///         and `rescueUSDC` role guards, and constructor zero-address
///         validation. Venue-specific wiring (pool/vault/router) is covered in
///         the per-venue suites.
contract BuybackBurnerTest is Test {
    MockUSDC internal usdc;
    Token internal token;
    TokenSource internal source;
    TestableBuybackBurner internal bb;

    address internal admin = address(0xA11CE);
    address internal keeper = address(0xCAFE);
    address internal pauser = address(0xBAD);
    address internal treasury = address(0x7EA);

    uint256 internal constant USDC_AMOUNT = 1000e6;
    uint256 internal constant TOKEN_OUT = 500e18;

    function setUp() public {
        usdc = new MockUSDC();
        token = new Token(admin);
        source = new TokenSource(IERC20(address(token)));

        bb = new TestableBuybackBurner(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, source);

        vm.startPrank(admin);
        bb.grantRole(bb.KEEPER_ROLE(), keeper);
        bb.grantRole(bb.PAUSER_ROLE(), pauser);
        bb.setWired(true);
        // Fund the TokenSource with TOKEN so `_performSwap` can hand a
        // configurable amount to the BuybackBurner.
        token.transfer(address(source), 1_000_000e18);
        vm.stopPrank();
        // Fund the BuybackBurner with USDC (would arrive via FeeRouter.routeSettlement).
        usdc.transfer(address(bb), 10_000_000e6);
    }

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    function test_constructor_grantsAdminAndGovernanceRoles() public view {
        assertTrue(bb.hasRole(bb.DEFAULT_ADMIN_ROLE(), admin));
        assertTrue(bb.hasRole(bb.GOVERNANCE_ROLE(), admin));
    }

    function test_constructor_revertsOnZeroUsdc() public {
        vm.expectRevert(BuybackBurner.ZeroAddress.selector);
        new TestableBuybackBurner(IERC20(address(0)), ERC20Burnable(address(token)), admin, treasury, source);
    }

    function test_constructor_revertsOnZeroToken() public {
        vm.expectRevert(BuybackBurner.ZeroAddress.selector);
        new TestableBuybackBurner(IERC20(address(usdc)), ERC20Burnable(address(0)), admin, treasury, source);
    }

    function test_constructor_revertsOnZeroAdmin() public {
        vm.expectRevert(BuybackBurner.ZeroAddress.selector);
        new TestableBuybackBurner(IERC20(address(usdc)), ERC20Burnable(address(token)), address(0), treasury, source);
    }

    function test_constructor_revertsOnZeroTreasury() public {
        vm.expectRevert(BuybackBurner.ZeroAddress.selector);
        new TestableBuybackBurner(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, address(0), source);
    }

    function test_constructor_snapshotsTreasury() public view {
        assertEq(bb.treasury(), treasury);
    }

    // -----------------------------------------------------------------
    // executeBuyback — revert paths
    // -----------------------------------------------------------------

    function test_executeBuyback_revertsZeroAmount() public {
        vm.prank(keeper);
        vm.expectRevert(BuybackBurner.ZeroAmount.selector);
        bb.executeBuyback(0, 0);
    }

    function test_executeBuyback_revertsZeroMinOut() public {
        bb.setSwap({ actual_: TOKEN_OUT, reported_: TOKEN_OUT });
        vm.prank(keeper);
        vm.expectRevert(BuybackBurner.ZeroMinOut.selector);
        bb.executeBuyback(USDC_AMOUNT, 0);
    }

    function test_executeBuyback_revertsWhenAmountExceedsBalance() public {
        // `bb` holds 10_000_000e6 USDC (funded in setUp); request more.
        uint256 balance = usdc.balanceOf(address(bb));
        bb.setSwap({ actual_: TOKEN_OUT, reported_: TOKEN_OUT });
        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(BuybackBurner.AmountExceedsBalance.selector, balance + 1, balance));
        bb.executeBuyback(balance + 1, 1);
    }

    function test_executeBuyback_revertsPoolNotWiredWhenUnwired() public {
        // Fresh deployment left unwired (`wired` defaults false).
        TestableBuybackBurner fresh =
            new TestableBuybackBurner(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, source);
        vm.startPrank(admin);
        fresh.grantRole(fresh.KEEPER_ROLE(), keeper);
        vm.stopPrank();

        vm.prank(keeper);
        vm.expectRevert(BuybackBurner.PoolNotWired.selector);
        fresh.executeBuyback(USDC_AMOUNT, 1);
    }

    function test_executeBuyback_revertsSwapNotImplementedWhenActualDeltaIsZero() public {
        // Subclass reports a positive `tokenOut` but transfers nothing — the
        // I1 trust-boundary check must catch this and revert with
        // `SwapNotImplemented` BEFORE the burn fires (otherwise the contract
        // would emit `BuybackExecuted` with a zero burn, misleading indexers).
        bb.setSwap({ actual_: 0, reported_: TOKEN_OUT });

        vm.prank(keeper);
        vm.expectRevert(BuybackBurner.SwapNotImplemented.selector);
        bb.executeBuyback(USDC_AMOUNT, 1);
    }

    function test_executeBuyback_revertsSwapReportMismatch() public {
        // Subclass reports `TOKEN_OUT` but only transfers half — the
        // residual would otherwise be stranded in the contract on the burn
        // call. Must revert with the exact reported / actual pair.
        bb.setSwap({ actual_: TOKEN_OUT / 2, reported_: TOKEN_OUT });

        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(BuybackBurner.SwapReportMismatch.selector, TOKEN_OUT, TOKEN_OUT / 2));
        bb.executeBuyback(USDC_AMOUNT, 1);
    }

    // ADR 009 § Emergency Multisig — the protocol-wide pause sunsets hard at
    // each contract's own construction time + 365 days; afterwards `pause()` reverts for everyone.
    function test_pause_revertsAfterSunset() public {
        vm.warp(block.timestamp + 366 days);
        vm.prank(pauser);
        vm.expectRevert(SunsettingPausable.PauseExpired.selector);
        bb.pause();
    }

    function test_executeBuyback_revertsWhenPaused() public {
        vm.prank(pauser);
        bb.pause();

        bb.setSwap({ actual_: TOKEN_OUT, reported_: TOKEN_OUT });
        vm.prank(keeper);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        bb.executeBuyback(USDC_AMOUNT, 1);
    }

    function test_executeBuyback_revertsWithoutKeeperRole() public {
        bb.setSwap({ actual_: TOKEN_OUT, reported_: TOKEN_OUT });
        // `address(this)` doesn't hold KEEPER_ROLE.
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector, address(this), bb.KEEPER_ROLE()
            )
        );
        bb.executeBuyback(USDC_AMOUNT, 1);
    }

    // -----------------------------------------------------------------
    // executeBuyback — happy path
    // -----------------------------------------------------------------

    function test_executeBuyback_happyPath_burnsAndEmits() public {
        bb.setSwap({ actual_: TOKEN_OUT, reported_: TOKEN_OUT });
        uint256 supplyBefore = token.totalSupply();
        uint256 bbBalanceBefore = token.balanceOf(address(bb));

        // `BuybackExecuted(uint256 usdcIn, uint256 tokenOut)` has zero indexed
        // params, so all three topic checks are false; only data is checked.
        vm.expectEmit(false, false, false, true, address(bb));
        emit BuybackBurner.BuybackExecuted(USDC_AMOUNT, TOKEN_OUT);
        vm.prank(keeper);
        uint256 tokenOut = bb.executeBuyback(USDC_AMOUNT, 1);

        assertEq(tokenOut, TOKEN_OUT);
        // Supply burned, contract holds no residual TOKEN.
        assertEq(supplyBefore - token.totalSupply(), TOKEN_OUT);
        assertEq(token.balanceOf(address(bb)), bbBalanceBefore);
    }

    // -----------------------------------------------------------------
    // pause / unpause — role guards
    // -----------------------------------------------------------------

    function test_pause_revertsWithoutPauserRole() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector, address(this), bb.PAUSER_ROLE()
            )
        );
        bb.pause();
    }

    function test_unpause_revertsWithoutPauserRole() public {
        vm.prank(pauser);
        bb.pause();
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector, address(this), bb.PAUSER_ROLE()
            )
        );
        bb.unpause();
    }

    // -----------------------------------------------------------------
    // rescueUSDC — recover stranded USDC
    // -----------------------------------------------------------------

    function test_rescueUSDC_transfersToTreasury() public {
        uint256 amount = 1234e6;
        uint256 treasuryBefore = usdc.balanceOf(treasury);
        uint256 bbBefore = usdc.balanceOf(address(bb));

        vm.expectEmit(false, false, false, true, address(bb));
        emit BuybackBurner.UsdcRescued(amount);
        vm.prank(admin);
        bb.rescueUSDC(amount);

        // Funds land at the fixed treasury; there is no caller-chosen sink.
        assertEq(bb.treasury(), treasury);
        assertEq(usdc.balanceOf(treasury), treasuryBefore + amount);
        assertEq(usdc.balanceOf(address(bb)), bbBefore - amount);
    }

    function test_rescueUSDC_revertsWithoutGovernanceRole() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector, address(this), bb.GOVERNANCE_ROLE()
            )
        );
        bb.rescueUSDC(1e6);
    }

    function test_rescueUSDC_revertsOnZeroAmount() public {
        vm.prank(admin);
        vm.expectRevert(BuybackBurner.ZeroAmount.selector);
        bb.rescueUSDC(0);
    }
}
