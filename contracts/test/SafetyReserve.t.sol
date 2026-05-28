// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { SafetyReserve } from "../src/SafetyReserve.sol";
import { Token } from "../src/Token.sol";
import { ICapacityBond } from "../src/interfaces/ICapacityBond.sol";
import { ISafetyReserve } from "../src/interfaces/ISafetyReserve.sol";

import { MockCapacityBond } from "./mocks/MockCapacityBond.sol";

contract MockUSDC is ERC20 {
    constructor() ERC20("USDC", "USDC") {
        _mint(msg.sender, 1_000_000_000e6);
    }

    function decimals() public pure override returns (uint8) {
        return 6;
    }
}

/// @title SafetyReserve smoke tests
/// @notice Critical-path coverage for the ADR 028 appeal state machine,
///         payout queue, ADR 026 slash-redirect accounting, and the
///         `clearSlashedAtEpoch` cross-contract call on `CapacityBond`.
contract SafetyReserveTest is Test {
    MockUSDC internal usdc;
    Token internal token;
    MockCapacityBond internal bond;
    SafetyReserve internal reserve;

    address internal admin = address(0xA11CE);
    address internal multisig = address(0xC0DE);
    address internal operator = address(0xB0B);
    address internal appellant = address(0xA9);
    address internal challengerPool = address(0xCC);

    uint256 internal constant APPEAL_BOND = 1000e18;
    uint256 internal constant MAX_RESTITUTION = 100_000e6; // 100k USDC

    function setUp() public {
        usdc = new MockUSDC();
        token = new Token(admin);
        bond = new MockCapacityBond();

        reserve = new SafetyReserve({
            usdc_: usdc,
            token_: token,
            capacityBond_: ICapacityBond(address(bond)),
            admin: admin,
            emergencyMultisig: multisig,
            appealBond_: APPEAL_BOND,
            maxAppealRestitution_: MAX_RESTITUTION
        });

        // Seed reserve with USDC (would arrive via FeeRouter in prod).
        usdc.transfer(address(reserve), 500_000e6);

        // Seed appellant with TOKEN to post bond.
        vm.prank(admin);
        token.transfer(appellant, APPEAL_BOND * 10);
        vm.prank(appellant);
        token.approve(address(reserve), type(uint256).max);

        vm.startPrank(admin);
        reserve.setChallengerIncentivePool(challengerPool);
        vm.stopPrank();
    }

    function test_payout_immediatePay() public {
        vm.prank(admin);
        (uint256 incidentId, bool queued, uint256 claimId) = reserve.payout(bytes32("bundle-1"), operator, 1000e6);
        assertFalse(queued);
        assertEq(claimId, 0);
        assertEq(reserve.incidentCount(), 1);
        assertEq(reserve.incidents(incidentId).recipient, operator);
        assertEq(usdc.balanceOf(operator), 1000e6);
    }

    function test_payout_queueOnInsufficientLiquidity() public {
        uint256 huge = 10_000_000_000e6;
        vm.prank(admin);
        (uint256 incidentId, bool queued, uint256 claimId) = reserve.payout(bytes32("bundle-x"), operator, huge);
        assertEq(incidentId, 0);
        assertTrue(queued);
        assertEq(claimId, 0);
        assertEq(reserve.pendingQueueLength(), 1);
    }

    function test_disbursePending_drainsAfterRefill() public {
        // Drain reserve via a queue.
        uint256 amount = 200_000e6;
        vm.prank(admin);
        reserve.payout(bytes32("q-1"), operator, amount);
        // Reserve only has 500k USDC, payout settled the head immediately.
        assertEq(reserve.pendingQueueLength(), 0);

        // Now queue something larger than balance.
        vm.prank(admin);
        reserve.payout(bytes32("q-2"), operator, 500_000e6);
        assertEq(reserve.pendingQueueLength(), 1);

        // Refill and disburse.
        usdc.transfer(address(reserve), 500_000e6);
        uint256 incidentId = reserve.disbursePending();
        assertGt(incidentId, 0);
        assertEq(reserve.pendingQueueLength(), 0);
    }

    function test_disbursePending_emitsSkippedWhenEmpty() public {
        // Empty queue → returns 0, emits skip event.
        uint256 incidentId = reserve.disbursePending();
        assertEq(incidentId, 0);
    }

    function test_swapAccumulatedTokens_revertsUntilImplemented() public {
        bytes32 keeperRole = reserve.KEEPER_ROLE();
        vm.startPrank(admin);
        reserve.grantRole(keeperRole, admin);
        // Pool unwired → PoolNotWired.
        vm.expectRevert();
        reserve.swapAccumulatedTokens(1e18, 0);
        // Wire pool, expect SwapNotImplemented (I1 fix).
        reserve.setBalancerPool(address(0x1234));
        vm.expectRevert();
        reserve.swapAccumulatedTokens(1e18, 0);
        vm.stopPrank();
    }

    function test_openSlashAppeal_validatesOperatorFromCapacityBond() public {
        // Seed a slash record in mock bond.
        bond.setSlashRecord(0, operator, uint64(block.timestamp), 50_000e18);

        vm.prank(appellant);
        uint256 appealId = reserve.openSlashAppeal(0, bytes32("evidence"));
        SafetyReserve.Appeal memory a = reserve.getAppeal(appealId);
        assertEq(a.operator, operator);
        assertEq(a.appellant, appellant);
        assertEq(uint8(a.status), uint8(ISafetyReserve.AppealStatus.Open));
    }

    function test_openSlashAppeal_revertsUnknownSlash() public {
        vm.prank(appellant);
        vm.expectRevert();
        reserve.openSlashAppeal(999, bytes32("e"));
    }

    function test_fullAppealFlow_ratify() public {
        bond.setSlashRecord(0, operator, uint64(block.timestamp), 50_000e18);

        vm.prank(appellant);
        uint256 appealId = reserve.openSlashAppeal(0, bytes32("evidence"));

        // Multisig fast-tracks.
        vm.prank(multisig);
        reserve.fastTrackAppeal(appealId);
        assertEq(reserve.totalEscrowLien(), MAX_RESTITUTION);

        // Governance ratifies.
        uint256 operatorUsdcBefore = usdc.balanceOf(operator);
        uint256 appellantTokenBefore = token.balanceOf(appellant);
        vm.prank(admin);
        reserve.ratifyAppeal(appealId);

        // Escrow lien released, restitution paid, bond refunded.
        assertEq(reserve.totalEscrowLien(), 0);
        assertEq(usdc.balanceOf(operator) - operatorUsdcBefore, MAX_RESTITUTION);
        assertEq(token.balanceOf(appellant) - appellantTokenBefore, APPEAL_BOND);
        assertEq(uint8(reserve.getAppeal(appealId).status), uint8(ISafetyReserve.AppealStatus.Ratified));
    }

    function test_fullAppealFlow_reverseClearsSlashedAtEpoch() public {
        bond.setSlashRecord(0, operator, uint64(block.timestamp), 50_000e18);
        bond.setSlashedAtEpoch(operator, 42);

        vm.prank(appellant);
        uint256 appealId = reserve.openSlashAppeal(0, bytes32("evidence"));
        vm.prank(multisig);
        reserve.fastTrackAppeal(appealId);

        uint256 challengerPoolBefore = token.balanceOf(challengerPool);
        vm.prank(admin);
        reserve.reverseAppeal(appealId);

        // CapacityBond.slashedAtEpoch cleared.
        assertEq(bond.slashedAtEpoch(operator), 0);
        // 50% bond burn + 50% to challenger pool.
        assertEq(token.balanceOf(challengerPool) - challengerPoolBefore, APPEAL_BOND / 2);
        assertEq(reserve.totalEscrowLien(), 0);
    }

    function test_rejectAppeal_burnsBond() public {
        bond.setSlashRecord(0, operator, uint64(block.timestamp), 50_000e18);

        uint256 supplyBefore = token.totalSupply();
        vm.prank(appellant);
        uint256 appealId = reserve.openSlashAppeal(0, bytes32("evidence"));
        vm.prank(multisig);
        reserve.rejectAppeal(appealId);

        // 100% bond burned.
        assertEq(supplyBefore - token.totalSupply(), APPEAL_BOND);
    }

    function test_cleanupExpiredAppeal_openLapse() public {
        bond.setSlashRecord(0, operator, uint64(block.timestamp), 50_000e18);
        vm.prank(appellant);
        uint256 appealId = reserve.openSlashAppeal(0, bytes32("e"));

        // Warp past the review window.
        vm.warp(block.timestamp + 15 days);
        reserve.cleanupExpiredAppeal(appealId);
        assertEq(uint8(reserve.getAppeal(appealId).status), uint8(ISafetyReserve.AppealStatus.Lapsed));
    }

    function test_recordSlashInflow_attributesToOperator() public {
        bytes32 reporterRole = reserve.SLASH_INFLOW_REPORTER_ROLE();
        vm.prank(admin);
        reserve.grantRole(reporterRole, admin);
        vm.prank(admin);
        reserve.recordSlashInflow(operator, 1500e18);
        assertEq(reserve.slashInflowOf(operator), 1500e18);
    }

    function test_availableUsdc_subtractsEscrowLien() public {
        bond.setSlashRecord(0, operator, uint64(block.timestamp), 50_000e18);
        uint256 before = reserve.availableUsdc();

        vm.prank(appellant);
        uint256 appealId = reserve.openSlashAppeal(0, bytes32("e"));
        vm.prank(multisig);
        reserve.fastTrackAppeal(appealId);

        assertEq(reserve.availableUsdc(), before - MAX_RESTITUTION);
    }

    // -----------------------------------------------------------------
    // Access-control guards on governance setters (none were previously
    // exercised; a refactor that dropped `onlyRole(GOVERNANCE_ROLE)` would
    // otherwise pass CI silently).
    // -----------------------------------------------------------------

    function test_setBalancerPool_revertsWithoutRole() public {
        _expectMissingRole(operator, reserve.GOVERNANCE_ROLE());
        vm.prank(operator);
        reserve.setBalancerPool(address(0x1234));
    }

    function test_setChallengerIncentivePool_revertsWithoutRole() public {
        _expectMissingRole(operator, reserve.GOVERNANCE_ROLE());
        vm.prank(operator);
        reserve.setChallengerIncentivePool(address(0x1234));
    }

    function test_setAppealBond_revertsWithoutRole() public {
        _expectMissingRole(operator, reserve.GOVERNANCE_ROLE());
        vm.prank(operator);
        reserve.setAppealBond(APPEAL_BOND * 2);
    }

    function test_setMaxAppealRestitution_revertsWithoutRole() public {
        _expectMissingRole(operator, reserve.GOVERNANCE_ROLE());
        vm.prank(operator);
        reserve.setMaxAppealRestitution(MAX_RESTITUTION * 2);
    }

    /// @dev See `CapacityBond.t.sol:_expectMissingRole` for rationale.
    function _expectMissingRole(address caller, bytes32 role) internal {
        vm.expectRevert(abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, caller, role));
    }

    // -----------------------------------------------------------------
    // openSlashAppeal — window and frequency-cap guards
    // -----------------------------------------------------------------

    /// @notice ADR 028 § Filing window — appeals filed more than 30 days
    ///         after the slash event must revert. Without this guard,
    ///         stale slashes could be challenged indefinitely.
    function test_openSlashAppeal_revertsWhenFilingWindowClosed() public {
        uint64 slashedAt = uint64(block.timestamp);
        bond.setSlashRecord(0, operator, slashedAt, 50_000e18);

        vm.warp(uint256(slashedAt) + 30 days + 1);
        vm.prank(appellant);
        vm.expectRevert(abi.encodeWithSelector(SafetyReserve.FilingWindowClosed.selector, slashedAt));
        reserve.openSlashAppeal(0, bytes32("e"));
    }

    /// @notice ADR 028 § Frequency cap — once an operator has had a
    ///         successful (ratified) appeal, a follow-up appeal within
    ///         365 days must revert. Rejected/lapsed appeals do NOT consume
    ///         the slot — see `test_hasActiveAppeal_clearedOnReject` etc.
    function test_openSlashAppeal_revertsAtFrequencyCap() public {
        // Slash #0, appeal, fast-track, ratify — fills the frequency slot.
        bond.setSlashRecord(0, operator, uint64(block.timestamp), 50_000e18);
        vm.prank(appellant);
        uint256 appealId = reserve.openSlashAppeal(0, bytes32("e1"));
        vm.prank(multisig);
        reserve.fastTrackAppeal(appealId);
        vm.prank(admin);
        reserve.ratifyAppeal(appealId);

        // Slash #1 for the SAME operator a few days later.
        vm.warp(block.timestamp + 10 days);
        bond.setSlashRecord(1, operator, uint64(block.timestamp), 50_000e18);
        uint64 lastAccepted = reserve.lastAcceptedAppealAt(operator);
        uint64 nextAvailable = lastAccepted + 365 days;

        vm.prank(appellant);
        vm.expectRevert(abi.encodeWithSelector(SafetyReserve.FrequencyCapHit.selector, nextAvailable));
        reserve.openSlashAppeal(1, bytes32("e2"));
    }

    // -----------------------------------------------------------------
    // ADR 036 § Slashing zero-out — reverse clears, ratify does NOT
    // -----------------------------------------------------------------

    /// @notice Companion to `test_fullAppealFlow_reverseClearsSlashedAtEpoch`:
    ///         the ratify path MUST leave `slashedAtEpoch` set. Operator gets
    ///         USDC restitution but keeps the reputation hit — the slash is
    ///         not retroactively wiped from history.
    function test_ratifyAppeal_doesNotClearSlashedAtEpoch() public {
        bond.setSlashRecord(0, operator, uint64(block.timestamp), 50_000e18);
        bond.setSlashedAtEpoch(operator, 42);

        vm.prank(appellant);
        uint256 appealId = reserve.openSlashAppeal(0, bytes32("e"));
        vm.prank(multisig);
        reserve.fastTrackAppeal(appealId);
        vm.prank(admin);
        reserve.ratifyAppeal(appealId);

        assertEq(bond.slashedAtEpoch(operator), 42, "ratify must NOT clear slashedAtEpoch");
    }

    // -----------------------------------------------------------------
    // fastTrackAppeal — insufficient-reserve guard
    // -----------------------------------------------------------------

    /// @notice ADR 032 § Escrow lien invariant — `fastTrackAppeal` must
    ///         revert when reserve liquidity (minus existing liens) cannot
    ///         cover the new restitution. Without this guard, the reserve
    ///         could be over-committed and a subsequent ratify would fail.
    function test_fastTrackAppeal_revertsWhenReserveInsufficient() public {
        // Drain the reserve via a queued payout that pulls almost everything.
        // Reserve balance is 500k; pay out 499k so only 1k remains. The
        // restitution cap is 100k, so fastTrack must revert.
        vm.prank(admin);
        reserve.payout(bytes32("drain"), address(0xDEAD), 499_000e6);

        bond.setSlashRecord(0, operator, uint64(block.timestamp), 50_000e18);
        vm.prank(appellant);
        uint256 appealId = reserve.openSlashAppeal(0, bytes32("e"));

        uint256 available = reserve.availableUsdc();
        vm.prank(multisig);
        vm.expectRevert(abi.encodeWithSelector(SafetyReserve.InsufficientReserve.selector, available, MAX_RESTITUTION));
        reserve.fastTrackAppeal(appealId);
    }

    /// @notice T-2 — Duplicate `openSlashAppeal(slashId)` must revert with
    ///         `SlashAlreadyAppealed`. Without this guard, multiple appeals
    ///         could each be ratified up to `maxAppealRestitution`,
    ///         draining the reserve.
    function test_openSlashAppeal_revertsDuplicateSlashId() public {
        bond.setSlashRecord(0, operator, uint64(block.timestamp), 50_000e18);

        vm.prank(appellant);
        reserve.openSlashAppeal(0, bytes32("first"));

        // Second call from any caller on the same slashId must revert.
        // Fund a second appellant so the failure is the dedup guard, not
        // the bond pull.
        address secondAppellant = address(0xA8);
        vm.prank(admin);
        token.transfer(secondAppellant, APPEAL_BOND);
        vm.prank(secondAppellant);
        token.approve(address(reserve), type(uint256).max);

        vm.prank(secondAppellant);
        vm.expectRevert(abi.encodeWithSelector(SafetyReserve.SlashAlreadyAppealed.selector, uint256(0)));
        reserve.openSlashAppeal(0, bytes32("second"));

        // Confirm the guard flag is set.
        assertTrue(reserve.slashAppealed(0));
    }
}
