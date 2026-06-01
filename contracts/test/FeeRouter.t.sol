// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { FeeRouter } from "../src/FeeRouter.sol";
import { ICapacityBondReporter } from "../src/interfaces/ICapacityBondReporter.sol";

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
    uint64 public epochLengthValue;

    constructor(uint64 epochLengthValue_) {
        epochLengthValue = epochLengthValue_;
    }

    function recordSettlement(address operator) external override {
        reported.push(operator);
    }

    function reportedCount() external view returns (uint256) {
        return reported.length;
    }

    function epochLength() external view override returns (uint64) {
        return epochLengthValue;
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
    address internal buyback = address(0xBB);

    uint64 internal constant EPOCH = 7 days;

    function setUp() public {
        usdc = new MockUSDC();
        bondReporter = new MockBondReporter(EPOCH);

        // 6000 / 3000 / 1000 — ADR 026 steady-state default.
        uint256[3] memory shares = [uint256(6000), uint256(3000), uint256(1000)];

        router = new FeeRouter({
            usdc_: usdc,
            capacityBond_: bondReporter,
            treasury_: treasury,
            epochLength_: EPOCH,
            windowEpochs_: 13,
            admin: admin,
            initialShares: shares,
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

    function test_routeSettlement_distributesThreeLegs() public {
        uint256 amount = 1000e6;
        uint256 bytesDelivered = 100_000_000;

        vm.warp(EPOCH + 1);
        vm.prank(channel);
        router.routeSettlement(operator, bytesDelivered, amount);

        // Shares: 6000/3000/1000.
        assertEq(usdc.balanceOf(operator), 600e6);
        assertEq(usdc.balanceOf(buyback), 300e6);
        assertEq(usdc.balanceOf(treasury), 100e6);

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

    function test_bytesInWindow_clampsOversizedWindow() public {
        // Drive 4 epochs of settlement.
        for (uint64 i = 1; i <= 4; i++) {
            vm.warp(uint256(i) * EPOCH + 1);
            vm.prank(channel);
            router.routeSettlement(operator, i * 1000, 100e6);
        }
        // A massive `n` must not loop unbounded: it clamps to the 26-epoch
        // ceiling, which (ending at epoch 4) covers all 4 populated epochs and
        // returns the same sum as an explicit ceiling-length window.
        uint256 clamped = router.bytesInWindow(operator, 4, type(uint64).max);
        assertEq(clamped, 10_000);
        assertEq(clamped, router.bytesInWindow(operator, 4, 26));
        assertEq(router.totalBytesInWindow(4, type(uint64).max), router.totalBytesInWindow(4, 26));
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
        // [5000, 3000, 1000] sums to 9000 → SharesDoNotSum(9000).
        uint256[3] memory bad = [uint256(5000), uint256(3000), uint256(1000)];
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(FeeRouter.SharesDoNotSum.selector, uint256(9000)));
        router.setShares(bad);
    }

    function test_setShares_rejectsNonZeroShareWithoutDestination() public {
        // Clear buyback burner (bucket index 1) while its share is non-zero.
        vm.startPrank(admin);
        vm.expectRevert(abi.encodeWithSelector(FeeRouter.NonZeroShareNeedsDestination.selector, uint256(1)));
        router.setBuybackBurner(address(0));
        vm.stopPrank();
    }

    /// @notice routeSettlement is gated on ROUTER_CALLER_ROLE — the role
    ///         exists specifically to prevent any account from inflating
    ///         their own per-epoch served-bytes (which would translate to
    ///         voting weight via ADR 036). A caller without the role MUST
    ///         revert before the bytesPerEpoch increment.
    function test_routeSettlement_revertsWithoutRouterRole() public {
        // `operator` does not hold ROUTER_CALLER_ROLE. Fund + approve so
        // any failure can only come from the role gate, not the
        // safeTransferFrom or address checks.
        usdc.transfer(operator, 1000e6);
        vm.prank(operator);
        usdc.approve(address(router), type(uint256).max);

        vm.warp(EPOCH + 1);
        vm.prank(operator);
        vm.expectRevert();
        router.routeSettlement(operator, 1_000_000, 1000e6);

        // bytesPerEpoch unaffected — the revert happened before the write.
        uint64 epoch = uint64((EPOCH + 1) / EPOCH);
        assertEq(router.bytesPerEpoch(operator, epoch), 0);
        assertEq(router.totalBytesPerEpoch(epoch), 0);
    }

    // -----------------------------------------------------------------
    // Access-control guards on governance setters
    // -----------------------------------------------------------------

    function test_setShares_revertsWithoutRole() public {
        uint256[3] memory shares = [uint256(6000), uint256(3000), uint256(1000)];
        _expectMissingRole(operator, router.GOVERNANCE_ROLE());
        vm.prank(operator);
        router.setShares(shares);
    }

    function test_setSharesAndDestinations_revertsWithoutRole() public {
        uint256[3] memory shares = [uint256(6000), uint256(3000), uint256(1000)];
        FeeRouter.ShareDestinations memory dests =
            FeeRouter.ShareDestinations({ buybackBurner: buyback, treasury: treasury });
        _expectMissingRole(operator, router.GOVERNANCE_ROLE());
        vm.prank(operator);
        router.setSharesAndDestinations(shares, dests);
    }

    function test_setBuybackBurner_revertsWithoutRole() public {
        _expectMissingRole(operator, router.GOVERNANCE_ROLE());
        vm.prank(operator);
        router.setBuybackBurner(address(0x9999));
    }

    function test_setTreasury_revertsWithoutRole() public {
        _expectMissingRole(operator, router.GOVERNANCE_ROLE());
        vm.prank(operator);
        router.setTreasury(address(0x9999));
    }

    function test_setWindowEpochs_revertsWithoutRole() public {
        _expectMissingRole(operator, router.GOVERNANCE_ROLE());
        vm.prank(operator);
        router.setWindowEpochs(20);
    }

    function test_pause_revertsWithoutRole() public {
        _expectMissingRole(operator, router.PAUSER_ROLE());
        vm.prank(operator);
        router.pause();
    }

    function test_unpause_revertsWithoutRole() public {
        bytes32 pauserRole = router.PAUSER_ROLE();
        vm.startPrank(admin);
        router.grantRole(pauserRole, admin);
        router.pause();
        vm.stopPrank();

        _expectMissingRole(operator, pauserRole);
        vm.prank(operator);
        router.unpause();
    }

    /// @dev See `CapacityBond.t.sol:_expectMissingRole` for the rationale on
    ///      reading the role bytes32 outside the helper (prank-consumption
    ///      avoidance). The helper is local to each test file rather than
    ///      shared via a base contract to keep test deps shallow.
    function _expectMissingRole(address caller, bytes32 role) internal {
        vm.expectRevert(abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, caller, role));
    }

    /// @notice T-1 — `opShare` absorbs the rounding remainder so a 1-wei
    ///         settlement with zero buyback share + zero buybackBurner does
    ///         NOT attempt `safeTransfer(address(0), 1)` and revert.
    ///         Regression test for the dust-to-address(0) bug that would
    ///         brick settlements.
    function test_routeSettlement_dustRoutesToOperatorWhenBuybackUnset() public {
        // Valid bps split with buyback = 0 so buybackBurner can be address(0).
        // Operator floor is 4000, treasury ceiling 3000. Pick 9000/0/1000.
        uint256[3] memory split = [uint256(9000), uint256(0), uint256(1000)];
        FeeRouter.ShareDestinations memory dests =
            FeeRouter.ShareDestinations({ buybackBurner: address(0), treasury: treasury });
        vm.prank(admin);
        router.setSharesAndDestinations(split, dests);

        // Settle 1 wei. With these shares all computed legs round to
        // zero except the operator remainder; opShare = 1 - 0 - 0 = 1.
        uint256 opBefore = usdc.balanceOf(operator);
        vm.warp(EPOCH + 1);
        vm.prank(channel);
        router.routeSettlement(operator, 0, 1);
        assertEq(usdc.balanceOf(operator) - opBefore, 1);
    }
}
