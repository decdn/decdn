// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IGovernor } from "@openzeppelin/contracts/governance/IGovernor.sol";
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

    /// @notice Companion to `pushVoteCapBpsForTest` — exposes the
    ///         `_ageRampMonthsHistory` Trace208 so we can stage two distinct
    ///         age-ramp values at two distinct timepoints and verify the
    ///         historical-read invariant without the full propose/queue/
    ///         execute timelock dance.
    function pushAgeRampMonthsForTest(uint208 value) external {
        // slither-disable-next-line unused-return
        _ageRampMonthsHistory.push(clock(), value);
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

    // Base time large enough that subtracting 365 days does not underflow.
    // Bytes are seeded at the last fully-elapsed epoch (`tp / EPOCH - 1`),
    // which is the window endpoint the Governor reads after #847 — settlements
    // in the in-progress epoch (`tp / EPOCH`) are deliberately not counted.
    uint256 internal constant BASE = 2 * 365 days;
    uint256 internal immutable tp = BASE + 1;

    /// @dev Epoch the vote window ends on after #847: the last fully-elapsed
    ///      epoch as of `tp`.
    function _endEpoch() internal view returns (uint64) {
        return uint64(tp / EPOCH) - 1;
    }

    function _setBytesAtTimepoint(address op, uint256 served, uint256 total) internal {
        uint64 e = _endEpoch();
        feeRouter.setBytes(op, e, served);
        feeRouter.setTotalBytes(e, total);
        // Ample declared capacity so the ADR 036 per-epoch cap does not bind for
        // the small byte fixtures these tests use; cap-binding is covered
        // explicitly in the declared-capacity tests below.
        bond.setDeclaredMbpsAtEpoch(op, e, 1000);
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
        // Mock stores the raw encoded value; pass `actualEpoch + 1` to mirror
        // the real CapacityBond's +1 stamp convention. The slash lands on the
        // window endpoint (`_endEpoch()`), so the stamp is `_endEpoch() + 1`.
        bond.setSlashedAtEpoch(operator, _endEpoch() + 1);
        assertEq(gov.getVotes(operator, tp), 0);
    }

    function test_getVotes_recoversAfterWindowSlidesPastSlash() public {
        vm.warp(BASE + 2);
        bond.setFirstBondedAt(operator, uint64(BASE - 365 days));
        _setBytesAtTimepoint(operator, 100_000, 1_000_000);

        // Slash at actual epoch 5; the window of 13 ends at `_endEpoch()` and so
        // starts at `_endEpoch() + 1 - WINDOW` (≈ 91). Slash falls well before
        // windowStart, so vote weight is non-zero. Pass `actualEpoch + 1` per the
        // +1-offset convention.
        bond.setSlashedAtEpoch(operator, 5 + 1);
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
        // Seed the last fully-elapsed epoch the window reads (#847).
        feeRouter.setTotalBytes(_endEpoch(), 1_000_000);
        assertEq(gov.quorum(tp), 40_000);
    }

    function test_proposalThreshold_isPointOnePercent() public {
        vm.warp(BASE + 2);
        // proposalThreshold uses clock() - 1 = block.timestamp - 1, and reads
        // the last fully-elapsed epoch of that snapshot (#847).
        feeRouter.setTotalBytes(uint64((block.timestamp - 1) / EPOCH) - 1, 1_000_000);
        assertEq(gov.proposalThreshold(), 1000);
    }

    /// @notice `setVoteCapBps` is gated on `onlyGovernance` (timelock
    ///         executor). A direct call from the test contract must revert
    ///         with the explicit `GovernorOnlyExecutor` selector. The arg
    ///         `500` is intentionally inside the `[VOTE_CAP_BPS_FLOOR=100,
    ///         VOTE_CAP_BPS_CEILING=2500]` range so the only reachable
    ///         revert path is the role guard; bounds enforcement on the
    ///         executor path is out of scope for this test (would require
    ///         a full timelock propose/queue/execute dance).
    function test_setVoteCapBps_revertsWithoutTimelockCaller() public {
        vm.expectRevert(abi.encodeWithSelector(IGovernor.GovernorOnlyExecutor.selector, address(this)));
        gov.setVoteCapBps(500);
    }

    /// @notice Companion access-control test — `setAgeRampMonths` is gated
    ///         on `onlyGovernance` (timelock executor). A direct call from
    ///         the test contract must revert with the explicit
    ///         `GovernorOnlyExecutor` selector; see the rationale on
    ///         `test_setVoteCapBps_enforcesBounds`.
    function test_setAgeRampMonths_revertsWithoutTimelockCaller() public {
        vm.expectRevert(abi.encodeWithSelector(IGovernor.GovernorOnlyExecutor.selector, address(this)));
        gov.setAgeRampMonths(6);
    }

    /// @notice I4 companion — a later `setAgeRampMonths` push must NOT shift
    ///         the vote weight read at an earlier `timepoint`. Mirrors the
    ///         `voteCapBps` historical-read invariant: in-flight proposals
    ///         must read the age-ramp value that was canonical at their
    ///         snapshot timepoint, not the live value.
    function test_ageRampMonthsAt_preservesPriorReadAfterLaterPush() public {
        TestableDecdnGovernor t =
            new TestableDecdnGovernor(IFeeRouter(address(feeRouter)), ICapacityBond(address(bond)), timelock);

        // Half-age operator: 90 days bonded under the default 6-month
        // (180-day) ramp = 0.5 multiplier on 10k served = 5k weight.
        vm.warp(BASE + 2);
        bond.setFirstBondedAt(operator, uint64(BASE - 90 days));
        _setBytesAtTimepoint(operator, 10_000, 1_000_000);

        uint48 historicalTp = uint48(block.timestamp);
        uint256 historicalWeight = t.getVotes(operator, historicalTp);
        assertEq(historicalWeight, 5000);
        assertEq(t.ageRampMonthsAt(historicalTp), 6);

        // Warp forward and push a tighter age-ramp (12 months — operator
        // would no longer be at half ramp at 90 days, would drop to 0.25).
        vm.warp(block.timestamp + 30 days);
        t.pushAgeRampMonthsForTest(12);

        assertEq(t.ageRampMonths(), 12);
        assertEq(t.ageRampMonthsAt(historicalTp), 6);

        // Vote weight at the historical timepoint must still see the 6-month
        // ramp (half multiplier on 10k = 5k), not the new 12-month ramp.
        assertEq(t.getVotes(operator, historicalTp), historicalWeight);
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

        // Now a slash happens at a LATER actual epoch than the timepoint's
        // window. `tp / EPOCH` ≈ 104; pick actual slash epoch `tp/EPOCH + 1`,
        // then add the +1 stamp offset → store `tp/EPOCH + 2`.
        bond.setSlashedAtEpoch(operator, uint64(tp / EPOCH) + 2);

        // The historical snapshot weight must NOT change — the slash is
        // beyond `endEpoch` of the historical window.
        assertEq(gov.getVotes(operator, tp), historicalWeight);
    }

    /// @notice #847 regression — the core defect. A settlement landing in the
    ///         in-progress epoch (`tp / EPOCH`) after a proposal snapshot must
    ///         NOT change the weight read at that snapshot. Because the window
    ///         now ends at the last fully-elapsed epoch and `routeSettlement`
    ///         only ever writes the current bucket, the snapshot read is
    ///         immutable. This assertion FAILS on the pre-#847 code (which read
    ///         the live current-epoch bucket) and PASSES after the fix.
    function test_getVotes_immuneToCurrentEpochSettlement() public {
        vm.warp(BASE + 2);
        bond.setFirstBondedAt(operator, uint64(BASE - 365 days));
        _setBytesAtTimepoint(operator, 100_000, 1_000_000);

        uint256 w0 = gov.getVotes(operator, tp);
        assertEq(w0, 50_000); // 5% cap of 1M, full ramp.

        // Simulate a mid-vote settlement: the node settles a large byte count
        // into the CURRENT (in-progress) epoch bucket — the mock analogue of
        // `FeeRouter.routeSettlement` writing `block.timestamp / epochLength`.
        uint64 currentEpoch = uint64(tp / EPOCH);
        feeRouter.setBytes(operator, currentEpoch, 1_000_000_000);
        feeRouter.setTotalBytes(currentEpoch, 1_000_000_000);

        // Weight at the snapshot is unchanged — the current epoch is excluded.
        assertEq(gov.getVotes(operator, tp), w0);
        // Quorum (also windowed) is likewise unaffected by the in-progress epoch.
        assertEq(gov.quorum(tp), 40_000);
    }

    /// @notice #847 edge — before any epoch has fully elapsed (snapshot inside
    ///         epoch 0), the window is empty: weight, quorum, and threshold all
    ///         return 0 without underflowing `uint64` in `_endEpoch`.
    function test_getVotes_zeroBeforeFirstElapsedEpoch() public {
        // A timepoint strictly inside epoch 0.
        uint256 early = EPOCH - 1;
        bond.setFirstBondedAt(operator, 0);
        // Even with bytes seeded in epoch 0, nothing is counted yet.
        feeRouter.setBytes(operator, 0, 100_000);
        feeRouter.setTotalBytes(0, 1_000_000);

        assertEq(gov.getVotes(operator, early), 0);
        assertEq(gov.quorum(early), 0);

        // proposalThreshold reads `clock() - 1`; warp into epoch 0 so the
        // snapshot is still pre-first-elapsed-epoch.
        vm.warp(EPOCH - 1);
        assertEq(gov.proposalThreshold(), 0);
    }

    /// @notice #847 accepted trade-off — a slash recorded in the in-progress
    ///         epoch does NOT zero a proposal snapshotted earlier in that same
    ///         epoch; it takes effect once the epoch elapses (the slash window
    ///         tracks the byte window). Encoded so a future reader does not
    ///         "tighten" it back and silently reintroduce the snapshot-mutation
    ///         defect via the slash leg.
    function test_slash_inCurrentEpoch_doesNotZeroEarlierSnapshot() public {
        vm.warp(BASE + 2);
        bond.setFirstBondedAt(operator, uint64(BASE - 365 days));
        _setBytesAtTimepoint(operator, 100_000, 1_000_000);

        uint256 w0 = gov.getVotes(operator, tp);
        assertGt(w0, 0);

        // Slash stamped in the CURRENT (in-progress) epoch: actualEpoch =
        // tp/EPOCH, stamp = tp/EPOCH + 1. That epoch is > `_endEpoch()`, so the
        // historical snapshot at `tp` is unaffected.
        bond.setSlashedAtEpoch(operator, uint64(tp / EPOCH) + 1);
        assertEq(gov.getVotes(operator, tp), w0);
    }

    /// @notice #847 boundary — the slash window's lower edge is inclusive. A
    ///         slash at exactly `windowStart` zeroes the vote; one epoch earlier
    ///         does not. Pins the `>=` lower-edge bound as well as the upper
    ///         `== endEpoch` edge.
    function test_getVotes_slashAtWindowStartBoundary() public {
        vm.warp(BASE + 2);
        bond.setFirstBondedAt(operator, uint64(BASE - 365 days));
        _setBytesAtTimepoint(operator, 100_000, 1_000_000);

        uint64 windowStart = _endEpoch() + 1 - WINDOW;

        // Slash exactly at windowStart (stamp = actualEpoch + 1) → in window → 0.
        bond.setSlashedAtEpoch(operator, windowStart + 1);
        assertEq(gov.getVotes(operator, tp), 0);

        // One epoch before windowStart → outside the window → non-zero.
        bond.setSlashedAtEpoch(operator, windowStart);
        assertGt(gov.getVotes(operator, tp), 0);
    }

    /// @notice #847 — an operator whose bytes are ONLY in the in-progress epoch
    ///         (none in any elapsed epoch) reads exactly 0 weight and contributes
    ///         0 to quorum. This is the standalone "in-progress excluded" case;
    ///         `test_getVotes_immuneToCurrentEpochSettlement` proves the elapsed
    ///         bucket is unaffected, this proves the in-progress bucket alone is
    ///         not counted.
    function test_getVotes_inProgressOnlyBytesReadZero() public {
        vm.warp(BASE + 2);
        bond.setFirstBondedAt(operator, uint64(BASE - 365 days));

        // Bytes exist only in the current (in-progress) epoch — nothing elapsed.
        uint64 currentEpoch = uint64(tp / EPOCH);
        feeRouter.setBytes(operator, currentEpoch, 100_000);
        feeRouter.setTotalBytes(currentEpoch, 1_000_000);

        assertEq(gov.getVotes(operator, tp), 0);
        assertEq(gov.quorum(tp), 0);
    }

    /// @notice #847 edge — exercises the `!hasElapsed` branch of
    ///         `_slashedInWindow` specifically (a non-zero slash stamp that
    ///         passes the `slashStamp == 0` guard but lands in epoch 0). A
    ///         slashed operator still reads 0 weight, but via the empty-window
    ///         byte leg (`_cappedServed → 0`), not the slash leg. Encodes the
    ///         implicit coupling so a future refactor of `_cappedServed`'s
    ///         epoch-0 behavior can't silently turn this into a slash bypass.
    function test_getVotes_epoch0_slashedOperatorStillZero() public {
        uint256 early = EPOCH - 1; // strictly inside epoch 0 → !hasElapsed.
        bond.setFirstBondedAt(operator, 1);
        feeRouter.setBytes(operator, 0, 100_000);
        feeRouter.setTotalBytes(0, 1_000_000);
        // Non-zero slash stamp (actualEpoch 0 → stamp 1): clears the
        // `slashStamp == 0` guard so `_slashedInWindow` reaches `!hasElapsed`.
        bond.setSlashedAtEpoch(operator, 1);

        assertEq(gov.getVotes(operator, early), 0);
    }

    // ADR 036 § Formula — declared-capacity cap. `declaredMbps × epochLength ×
    // 125_000` is the max bytes the declared line could deliver in an epoch.
    // Short-epoch (1s) harness so the cap is hand-sized: 1 Mbps × 1 s × 125_000
    // = 125_000 bytes. firstBondedAt = 1 (non-zero) + tp2 = 400 days ⇒ full ramp.
    function test_getVotes_capsAtDeclaredCapacity_shortEpoch() public {
        MockFeeRouter fr = new MockFeeRouter(WINDOW, 1);
        MockCapacityBond cb = new MockCapacityBond();
        DecdnGovernor g = new DecdnGovernor(IFeeRouter(address(fr)), ICapacityBond(address(cb)), timelock);

        uint256 tp2 = 400 days; // full age ramp (horizon 180 days)
        uint64 e = uint64(tp2) - 1; // epochLength 1s → last fully-elapsed epoch
        cb.setFirstBondedAt(operator, 1); // non-zero sentinel → ramp not auto-zeroed

        fr.setBytes(operator, e, 300_000); // served above the capacity cap
        fr.setTotalBytes(e, 1_000_000_000); // 5% share cap = 50_000_000 ≫ served
        cb.setDeclaredMbpsAtEpoch(operator, e, 1); // cap = 1 × 1 × 125_000 = 125_000

        // min(served 300_000, capacityCap 125_000, shareCap 50_000_000) = 125_000.
        assertEq(g.getVotes(operator, tp2), 125_000);
    }

    function test_getVotes_burstFair_onlyOverLineEpochClipped() public {
        MockFeeRouter fr = new MockFeeRouter(WINDOW, 1);
        MockCapacityBond cb = new MockCapacityBond();
        DecdnGovernor g = new DecdnGovernor(IFeeRouter(address(fr)), ICapacityBond(address(cb)), timelock);

        uint256 tp2 = 400 days;
        uint64 end = uint64(tp2) - 1;
        cb.setFirstBondedAt(operator, 1);

        // Epoch `end`: 300k served, cap 125k → clipped to 125k.
        fr.setBytes(operator, end, 300_000);
        cb.setDeclaredMbpsAtEpoch(operator, end, 1); // 125_000
        // Epoch `end-1`: 50k served, cap 125k → full 50k (under line, untouched).
        fr.setBytes(operator, end - 1, 50_000);
        cb.setDeclaredMbpsAtEpoch(operator, end - 1, 1);
        fr.setTotalBytes(end, 1_000_000_000); // share cap non-binding

        // 125_000 + 50_000 = 175_000; no cross-epoch penalty.
        assertEq(g.getVotes(operator, tp2), 175_000);
    }

    function test_getVotes_zeroDeclaredCapacity_zeroWeight() public {
        MockFeeRouter fr = new MockFeeRouter(WINDOW, 1);
        MockCapacityBond cb = new MockCapacityBond();
        DecdnGovernor g = new DecdnGovernor(IFeeRouter(address(fr)), ICapacityBond(address(cb)), timelock);

        uint256 tp2 = 400 days;
        uint64 e = uint64(tp2) - 1;
        cb.setFirstBondedAt(operator, 1); // full ramp, so the ONLY zeroing cause is the cap
        fr.setBytes(operator, e, 300_000);
        fr.setTotalBytes(e, 1_000_000_000);
        // No declared capacity anywhere in the window → cap 0 → weight 0.
        assertEq(g.getVotes(operator, tp2), 0);
    }
}
