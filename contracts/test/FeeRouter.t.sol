// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";

import { FeeRouter } from "../src/FeeRouter.sol";
import { SunsettingPausable } from "../src/SunsettingPausable.sol";
import { ICapacityBondCredit } from "../src/interfaces/ICapacityBondCredit.sol";
import { MockCreditBond } from "./mocks/MockCreditBond.sol";

/// @dev USDC stand-in with Circle-style address blocking, so a test can make
///      the rebate transfer to an operator fail.
contract BlockingUSDC is ERC20 {
    mapping(address => bool) public blocked;

    constructor() ERC20("USDC", "USDC") {
        _mint(msg.sender, 1_000_000_000e6);
    }

    function decimals() public pure override returns (uint8) {
        return 6;
    }

    function setBlocked(address account, bool isBlocked) external {
        blocked[account] = isBlocked;
    }

    function _update(address from, address to, uint256 value) internal override {
        require(!blocked[to], "blocked");
        super._update(from, to, value);
    }
}

contract FeeRouterTest is Test {
    BlockingUSDC internal usdc;
    MockCreditBond internal bond;
    FeeRouter internal router;

    address internal admin = address(0xA11CE);
    address internal adapter = address(0xCAFE);
    address internal operator = address(0xB0B);
    address internal treasury = address(0xD7);

    uint64 internal constant EPOCH = 7 days;
    uint256 internal constant MIB = 1_048_576;
    /// @dev 1 Gbps declared: the clamp is 1000 * 7 days * 125_000 bytes.
    uint256 internal constant MBPS = 1000;
    uint256 internal constant CLAMP = MBPS * uint256(EPOCH) * 125_000;

    event Deposited(
        address indexed depositor,
        address indexed operator,
        uint256 amount,
        uint256 servedBytes,
        uint256 credit,
        uint64 indexed epoch
    );

    function setUp() public {
        usdc = new BlockingUSDC();
        bond = new MockCreditBond(EPOCH);
        bond.set(operator, true, MBPS);

        // Launch split from ADR 016 § Tunable Economics example: 75% rebate,
        // 25% treasury. Floor 1 (the launch value).
        router = _deployRouter(bond, EPOCH, 7500, 1);

        usdc.transfer(adapter, 1_000_000e6);
        vm.prank(adapter);
        usdc.approve(address(router), type(uint256).max);
        vm.warp(EPOCH + 1);
    }

    function _deployRouter(ICapacityBondCredit bond_, uint64 epochLength_, uint256 rebateBps_, uint256 floor_)
        internal
        returns (FeeRouter)
    {
        return new FeeRouter({
            usdc_: usdc,
            capacityBond_: bond_,
            treasury_: treasury,
            epochLength_: epochLength_,
            windowEpochs_: 13,
            deliveryFloor_: floor_,
            admin: admin,
            rebateBps_: rebateBps_
        });
    }

    function _deposit(uint256 servedBytes, uint256 amount) internal {
        vm.prank(adapter);
        router.deposit(operator, servedBytes, amount);
    }

    function _epoch() internal view returns (uint64) {
        return uint64(block.timestamp / EPOCH);
    }

    // -----------------------------------------------------------------
    // Constructor guards
    // -----------------------------------------------------------------

    function test_constructor_revertsOnEpochLengthMismatch() public {
        MockCreditBond mismatched = new MockCreditBond(EPOCH + 1 days);
        vm.expectRevert(abi.encodeWithSelector(FeeRouter.EpochLengthMismatch.selector, EPOCH + 1 days, EPOCH));
        _deployRouter(mismatched, EPOCH, 7500, 1);
    }

    function test_constructor_revertsOnZeroEpochLengthEvenWhenBondAgrees() public {
        MockCreditBond zeroBond = new MockCreditBond(0);
        vm.expectRevert(FeeRouter.ZeroEpochLength.selector);
        _deployRouter(zeroBond, 0, 7500, 1);
    }

    function test_constructor_revertsOnRebateAboveCeiling() public {
        vm.expectRevert(abi.encodeWithSelector(FeeRouter.RebateAboveCeiling.selector, uint256(9901)));
        _deployRouter(bond, EPOCH, 9901, 1);
    }

    function test_constructor_revertsOnFloorOutOfBounds() public {
        vm.expectRevert(abi.encodeWithSelector(FeeRouter.DeliveryFloorOutOfBounds.selector, uint256(0)));
        _deployRouter(bond, EPOCH, 7500, 0);
        vm.expectRevert(abi.encodeWithSelector(FeeRouter.DeliveryFloorOutOfBounds.selector, uint256(1001)));
        _deployRouter(bond, EPOCH, 7500, 1001);
    }

    // -----------------------------------------------------------------
    // deposit: split
    // -----------------------------------------------------------------

    function test_deposit_activeOperatorGetsRebate() public {
        _deposit(0, 1000e6);
        assertEq(usdc.balanceOf(operator), 750e6, "rebate");
        assertEq(usdc.balanceOf(treasury), 250e6, "treasury remainder");
        assertEq(usdc.balanceOf(address(router)), 0, "router holds nothing");
    }

    function test_deposit_inactiveOperatorRebateGoesToTreasury() public {
        bond.set(operator, false, MBPS);
        _deposit(5_000_000, 1000e6);
        assertEq(usdc.balanceOf(operator), 0, "no rebate");
        assertEq(usdc.balanceOf(treasury), 1000e6, "whole deposit to treasury");
        assertEq(router.bytesPerEpoch(operator, _epoch()), 0, "no credit");
        assertEq(router.totalBytesPerEpoch(_epoch()), 0, "no total");
    }

    function test_deposit_failedRebateFallsBackToTreasury() public {
        usdc.setBlocked(operator, true);
        _deposit(100, 1000e6);
        assertEq(usdc.balanceOf(operator), 0, "blocked operator");
        assertEq(usdc.balanceOf(treasury), 1000e6, "rebate fell back");
        assertEq(router.bytesPerEpoch(operator, _epoch()), 100, "credit still lands");
    }

    function test_deposit_dustRoundsToTreasury() public {
        _deposit(0, 1);
        assertEq(usdc.balanceOf(operator), 0, "rebate rounds down");
        assertEq(usdc.balanceOf(treasury), 1, "remainder");
    }

    function test_deposit_isPermissionless() public {
        address stranger = address(0x5157);
        usdc.transfer(stranger, 10e6);
        vm.startPrank(stranger);
        usdc.approve(address(router), 10e6);
        router.deposit(operator, 7, 10e6);
        vm.stopPrank();
        assertEq(router.bytesPerEpoch(operator, _epoch()), 7);
    }

    function test_deposit_revertsOnZeroOperatorOrAmount() public {
        vm.prank(adapter);
        vm.expectRevert(FeeRouter.ZeroAddress.selector);
        router.deposit(address(0), 1, 1);
        vm.prank(adapter);
        vm.expectRevert(FeeRouter.ZeroAmount.selector);
        router.deposit(operator, 1, 0);
    }

    function test_deposit_revertsWhenPaused() public {
        bytes32 role = router.PAUSER_ROLE();
        vm.startPrank(admin);
        router.grantRole(role, admin);
        router.pause();
        vm.stopPrank();

        vm.prank(adapter);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        router.deposit(operator, 1, 1e6);
    }

    function test_deposit_emitsDeposited() public {
        vm.expectEmit(true, true, true, true, address(router));
        emit Deposited(adapter, operator, 1000e6, 42, 42, _epoch());
        _deposit(42, 1000e6);
    }

    // -----------------------------------------------------------------
    // deposit: credit
    // -----------------------------------------------------------------

    /// The treasury leg is 25% of the deposit; at floor 1 one treasury-leg
    /// base unit buys one MiB of credit, so `bytes` binds here.
    function test_deposit_creditIsBytesWhenTreasuryLegCoversThem() public {
        _deposit(1_000_000, 1000e6);
        assertEq(router.bytesPerEpoch(operator, _epoch()), 1_000_000);
        assertEq(router.totalBytesPerEpoch(_epoch()), 1_000_000);
    }

    /// A 25-base-unit treasury leg at floor 1 backs 25 MiB, below the claimed bytes.
    function test_deposit_creditIsCappedByTreasuryLeg() public {
        _deposit(1000 * MIB, 100);
        assertEq(router.bytesPerEpoch(operator, _epoch()), 25 * MIB);
    }

    /// The rebate does not back credit: at the rebate ceiling only the 1%
    /// treasury leg does.
    function test_deposit_maxRebateCreditsFromTheTreasuryLeg() public {
        vm.prank(admin);
        router.setSplit(9900);
        _deposit(1000 * MIB, 100e6);
        // A 1e6 treasury leg at floor 1 backs 1e6 MiB, above the claimed bytes.
        assertEq(router.bytesPerEpoch(operator, _epoch()), 1000 * MIB);
        assertEq(usdc.balanceOf(operator), 99e6);
    }

    function test_deposit_floorPricesCredit() public {
        vm.prank(admin);
        router.setDeliveryFloor(1000);
        // A 250e6 treasury leg at 1000 base units per MiB backs 250_000 MiB.
        _deposit(type(uint64).max, 1000e6);
        assertEq(router.bytesPerEpoch(operator, _epoch()), 250_000 * MIB > CLAMP ? CLAMP : 250_000 * MIB);
    }

    function test_deposit_capacityClampBindsOperatorAndTotal() public {
        _deposit(CLAMP - 10, 1_000_000e6 / 10);
        _deposit(100, 1e6);
        assertEq(router.bytesPerEpoch(operator, _epoch()), CLAMP, "operator clamped");
        assertEq(router.totalBytesPerEpoch(_epoch()), CLAMP, "total clamped alike");
        _deposit(100, 1e6);
        assertEq(router.bytesPerEpoch(operator, _epoch()), CLAMP, "no room left");
    }

    function test_deposit_zeroDeclaredCapacityCreditsNothing() public {
        bond.set(operator, true, 0);
        _deposit(1000, 1000e6);
        assertEq(router.bytesPerEpoch(operator, _epoch()), 0);
        assertEq(usdc.balanceOf(operator), 750e6, "rebate still paid");
    }

    function test_deposit_stampsArrivalEpoch() public {
        _deposit(10, 1e6);
        uint64 first = _epoch();
        vm.warp(block.timestamp + EPOCH);
        _deposit(20, 1e6);
        assertEq(router.bytesPerEpoch(operator, first), 10);
        assertEq(router.bytesPerEpoch(operator, first + 1), 20);
    }

    // -----------------------------------------------------------------
    // Windows
    // -----------------------------------------------------------------

    function test_bytesInWindow_sumsTrailingWindow() public {
        for (uint64 i = 1; i <= 4; i++) {
            vm.warp(uint256(i) * EPOCH + 1);
            _deposit(i * 1000, 100e6);
        }
        assertEq(router.bytesInWindow(operator, 4, 4), 10_000);
        assertEq(router.totalBytesInWindow(4, 4), 10_000);
    }

    function test_bytesInWindow_clampsOversizedWindow() public {
        for (uint64 i = 1; i <= 4; i++) {
            vm.warp(uint256(i) * EPOCH + 1);
            _deposit(i * 1000, 100e6);
        }
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
        vm.startPrank(admin);
        vm.expectRevert(abi.encodeWithSelector(FeeRouter.WindowOutOfBounds.selector, uint64(3), uint64(4), uint64(26)));
        router.setWindowEpochs(3);
        vm.expectRevert(abi.encodeWithSelector(FeeRouter.WindowOutOfBounds.selector, uint64(27), uint64(4), uint64(26)));
        router.setWindowEpochs(27);
        router.setWindowEpochs(20);
        vm.stopPrank();
        assertEq(router.windowEpochs(), 20);
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    /// A rebate above 99% would leave no treasury leg to back weight, so a
    /// quiet window could empty the electorate for good.
    function test_setSplit_keepsATreasuryShare() public {
        vm.startPrank(admin);
        vm.expectRevert(abi.encodeWithSelector(FeeRouter.RebateAboveCeiling.selector, uint256(9901)));
        router.setSplit(9901);
        vm.expectRevert(abi.encodeWithSelector(FeeRouter.RebateAboveCeiling.selector, uint256(10_000)));
        router.setSplit(10_000);
        router.setSplit(9900);
        vm.stopPrank();
        assertEq(router.rebateBps(), 9900);
    }

    function test_setTreasury_rejectsZero() public {
        vm.prank(admin);
        vm.expectRevert(FeeRouter.ZeroAddress.selector);
        router.setTreasury(address(0));
    }

    function test_setDeliveryFloor_enforcesBounds() public {
        vm.startPrank(admin);
        vm.expectRevert(abi.encodeWithSelector(FeeRouter.DeliveryFloorOutOfBounds.selector, uint256(0)));
        router.setDeliveryFloor(0);
        vm.expectRevert(abi.encodeWithSelector(FeeRouter.DeliveryFloorOutOfBounds.selector, uint256(1001)));
        router.setDeliveryFloor(1001);
        router.setDeliveryFloor(1000);
        vm.stopPrank();
        assertEq(router.deliveryFloor(), 1000);
    }

    function test_setters_revertWithoutRole() public {
        bytes32 gov = router.GOVERNANCE_ROLE();
        vm.startPrank(operator);
        _expectMissingRole(operator, gov);
        router.setSplit(0);
        _expectMissingRole(operator, gov);
        router.setTreasury(address(0x9999));
        _expectMissingRole(operator, gov);
        router.setDeliveryFloor(2);
        _expectMissingRole(operator, gov);
        router.setWindowEpochs(20);
        vm.stopPrank();
    }

    // -----------------------------------------------------------------
    // Pause
    // -----------------------------------------------------------------

    function test_pause_revertsAfterSunset() public {
        bytes32 pauserRole = router.PAUSER_ROLE();
        vm.prank(admin);
        router.grantRole(pauserRole, admin);

        vm.warp(block.timestamp + 366 days);
        vm.prank(admin);
        vm.expectRevert(SunsettingPausable.PauseExpired.selector);
        router.pause();
    }

    function test_pause_revertsWithoutRole() public {
        bytes32 pauserRole = router.PAUSER_ROLE();
        _expectMissingRole(operator, pauserRole);
        vm.prank(operator);
        router.pause();
    }

    function _expectMissingRole(address caller, bytes32 role) internal {
        vm.expectRevert(abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, caller, role));
    }
}
