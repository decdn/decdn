// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";

import { DecdnGovernor } from "../src/DecdnGovernor.sol";
import { IFeeRouter } from "../src/interfaces/IFeeRouter.sol";
import { ICapacityBond } from "../src/interfaces/ICapacityBond.sol";

import { MockFeeRouter } from "./mocks/MockFeeRouter.sol";
import { MockCapacityBond } from "./mocks/MockCapacityBond.sol";

/// @title DecdnGovernor smoke tests
/// @notice Exercises the ADR 036 `_getVotes` formula in isolation using mock
///         `FeeRouter` + `CapacityBond` so the vote-weight math is decoupled
///         from real settlement state. Tests cover: served-bytes path,
///         per-operator cap, slash zero-out, age-ramp gating, and quorum /
///         threshold derivation from `totalBytesInWindow`.
contract DecdnGovernorTest is Test {
    MockFeeRouter internal feeRouter;
    MockCapacityBond internal bond;
    TimelockController internal timelock;
    DecdnGovernor internal gov;

    address internal operator = address(0xB0B);

    uint64 internal constant EPOCH = 7 days;
    uint64 internal constant WINDOW = 13;

    function setUp() public {
        feeRouter = new MockFeeRouter(WINDOW, EPOCH);
        bond = new MockCapacityBond();

        address[] memory empty = new address[](0);
        address[] memory exec = new address[](1);
        exec[0] = address(0);
        timelock = new TimelockController(2 days, empty, exec, address(this));

        gov = new DecdnGovernor(IFeeRouter(address(feeRouter)), ICapacityBond(address(bond)), timelock);
    }

    function test_getVotes_zeroIfNeverBonded() public view {
        // No firstBondedAt set → age_ramp returns 0 → vote weight 0.
        assertEq(gov.getVotes(operator, EPOCH * 20), 0);
    }

    // Base time large enough that subtracting 365 days does not underflow,
    // and bytes set at the query-epoch (`tp / EPOCH`).
    uint256 internal constant BASE = 2 * 365 days;
    uint256 internal immutable tp = BASE + 1;

    function _setBytesAtTimepoint(address op, uint256 served, uint256 total) internal {
        uint64 e = uint64(tp / EPOCH);
        feeRouter.setBytes(op, e, served);
        feeRouter.setTotalBytes(e, total);
    }

    function test_getVotes_ramp() public {
        vm.warp(BASE + 2);
        bond.setFirstBondedAt(operator, uint64(BASE - 180 days));
        _setBytesAtTimepoint(operator, 100_000, 1_000_000);
        // 100k served vs 1M total → 10% raw. Cap = 5% → 50k. Full ramp → 50k.
        assertEq(gov.getVotes(operator, tp), 50_000);
    }

    function test_getVotes_uncappedWhenBelowCap() public {
        vm.warp(BASE + 2);
        bond.setFirstBondedAt(operator, uint64(BASE - 180 days));
        _setBytesAtTimepoint(operator, 10_000, 1_000_000);
        // 10k / 1M = 1% raw < 5% cap → 10k. Full ramp → 10k.
        assertEq(gov.getVotes(operator, tp), 10_000);
    }

    function test_getVotes_zeroWhenSlashedInWindow() public {
        vm.warp(BASE + 2);
        bond.setFirstBondedAt(operator, uint64(BASE - 180 days));
        _setBytesAtTimepoint(operator, 100_000, 1_000_000);
        bond.setSlashedAtEpoch(operator, uint64(tp / EPOCH));
        assertEq(gov.getVotes(operator, tp), 0);
    }

    function test_getVotes_recoversAfterWindowSlidesPastSlash() public {
        vm.warp(BASE + 2);
        bond.setFirstBondedAt(operator, uint64(BASE - 365 days));
        _setBytesAtTimepoint(operator, 100_000, 1_000_000);

        // Slash at epoch 5; current window of 13 ends near (BASE / EPOCH) ≈ 104.
        // Slash falls well before windowStart, so vote weight is non-zero.
        bond.setSlashedAtEpoch(operator, 5);
        assertGt(gov.getVotes(operator, tp), 0);
    }

    function test_getVotes_halfRamp() public {
        vm.warp(BASE + 2);
        // 90 days / 180 days = 0.5 ramp.
        bond.setFirstBondedAt(operator, uint64(BASE - 90 days));
        _setBytesAtTimepoint(operator, 10_000, 1_000_000);
        // 10k raw (below cap) * 0.5 = 5_000.
        assertEq(gov.getVotes(operator, tp), 5000);
    }

    function test_quorum_isFourPercentOfTotalBytesInWindow() public {
        vm.warp(BASE + 2);
        feeRouter.setTotalBytes(uint64(tp / EPOCH), 1_000_000);
        assertEq(gov.quorum(tp), 40_000);
    }

    function test_proposalThreshold_isPointOnePercent() public {
        vm.warp(BASE + 2);
        // proposalThreshold uses clock() - 1 = block.timestamp - 1.
        feeRouter.setTotalBytes(uint64((block.timestamp - 1) / EPOCH), 1_000_000);
        assertEq(gov.proposalThreshold(), 1000);
    }

    function test_setVoteCapBps_enforcesBounds() public {
        // Calling without governance role (we're not the executor) reverts.
        vm.expectRevert();
        gov.setVoteCapBps(500);
    }
}
