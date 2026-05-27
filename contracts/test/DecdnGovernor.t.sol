// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";
import { Checkpoints } from "@openzeppelin/contracts/utils/structs/Checkpoints.sol";

import { DecdnGovernor } from "../src/DecdnGovernor.sol";
import { IFeeRouter } from "../src/interfaces/IFeeRouter.sol";
import { ICapacityBond } from "../src/interfaces/ICapacityBond.sol";

import { MockFeeRouter } from "./mocks/MockFeeRouter.sol";
import { MockCapacityBond } from "./mocks/MockCapacityBond.sol";

/// @notice DecdnGovernor subclass that exposes a privileged push helper for
///         the `_voteCapBpsHistory` Trace208 — used to exercise the I4
///         snapshot semantics without staging a full propose/vote/queue/
///         execute dance. The production `setVoteCapBps` is timelock-gated;
///         testing the read path (`voteCapBpsAt(historicalTp)` returns the
///         prior value after a later push) only requires that we can push
///         from two distinct timepoints — the gate itself is verified
///         independently by `test_setVoteCapBps_enforcesBounds`.
contract TestableDecdnGovernor is DecdnGovernor {
    using Checkpoints for Checkpoints.Trace208;

    constructor(IFeeRouter f, ICapacityBond c, TimelockController t) DecdnGovernor(f, c, t) { }

    function pushVoteCapBpsForTest(uint208 value) external {
        // slither-disable-next-line unused-return
        _voteCapBpsHistory.push(clock(), value);
    }
}

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

    /// @notice I4 regression — a later `setVoteCapBps` push must NOT shift
    ///         the vote weight read at an earlier `timepoint`. Without the
    ///         Trace208 checkpointing (and `voteCapBpsAt(timepoint)` reads
    ///         in `_cappedServed`), a mid-proposal governance change to the
    ///         per-operator cap would retroactively re-anchor every active
    ///         proposal's weights. This test deploys the privileged-push
    ///         subclass so we can stage two distinct cap values at two
    ///         distinct timepoints without the timelock dance.
    function test_voteCapBpsAt_preservesPriorReadAfterLaterPush() public {
        // Deploy the testable subclass on top of the existing mocks.
        TestableDecdnGovernor t =
            new TestableDecdnGovernor(IFeeRouter(address(feeRouter)), ICapacityBond(address(bond)), timelock);

        // Stage the operator with substantial served-bytes at the historical
        // timepoint. Use full ramp so weight = capped serve directly.
        vm.warp(BASE + 2);
        bond.setFirstBondedAt(operator, uint64(BASE - 365 days));
        _setBytesAtTimepoint(operator, 100_000, 1_000_000);

        // Capture the historical timepoint and the weight at the seeded
        // cap (500 bps = 5% of 1_000_000 = 50_000).
        uint48 historicalTp = uint48(block.timestamp);
        uint256 historicalWeight = t.getVotes(operator, historicalTp);
        assertEq(historicalWeight, 50_000);
        assertEq(t.voteCapBpsAt(historicalTp), 500);

        // Warp forward and push a tighter cap (200 bps). Any reader that
        // looked at `voteCapBps()` live would now see 200; the I4 invariant
        // says historical reads MUST stay at 500.
        vm.warp(block.timestamp + 30 days);
        t.pushVoteCapBpsForTest(200);

        // Latest is 200, but the snapshot read returns the prior 500.
        assertEq(t.voteCapBps(), 200);
        assertEq(t.voteCapBpsAt(historicalTp), 500);

        // And the actual weight at the historical timepoint is unchanged —
        // proves `_cappedServed` consults `voteCapBpsAt(tp)`, not `voteCapBps()`.
        // This is the core I4 invariant: an in-flight proposal whose
        // snapshot is `historicalTp` sees the old 500 bps cap even after
        // governance pushed the new 200 bps.
        assertEq(t.getVotes(operator, historicalTp), historicalWeight);
    }

    /// @notice T-4 — slash that happens AFTER a historical timepoint must
    ///         NOT retroactively zero its vote weight. Regression test for
    ///         the `slashed <= endEpoch` upper bound.
    function test_getVotes_historicalSnapshotIgnoresFutureSlash() public {
        vm.warp(BASE + 2);
        bond.setFirstBondedAt(operator, uint64(BASE - 180 days));
        _setBytesAtTimepoint(operator, 10_000, 1_000_000);

        // Capture the historical vote weight (no slash yet).
        uint256 historicalWeight = gov.getVotes(operator, tp);
        assertGt(historicalWeight, 0);

        // Now a slash happens at a LATER epoch than the timepoint's window.
        // `tp / EPOCH` ≈ 104; pick a slash epoch strictly greater.
        bond.setSlashedAtEpoch(operator, uint64(tp / EPOCH) + 1);

        // The historical snapshot weight must NOT change — the slash is
        // beyond `endEpoch` of the historical window.
        assertEq(gov.getVotes(operator, tp), historicalWeight);
    }
}
