// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { CapacityBond } from "../src/CapacityBond.sol";
import { SlashJudge } from "../src/SlashJudge.sol";
import { Token } from "../src/Token.sol";
import { ISlashJudgeEvidenceView } from "../src/interfaces/ISlashJudgeEvidenceView.sol";
import { ICapacityBondSlasher } from "../src/interfaces/ICapacityBondSlasher.sol";

import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";
import { MockBlacklistView } from "./SlashJudge.t.sol";

/// @title Cross-contract CapacityBond/SlashJudge unbonding invariant
/// @notice The CapacityBond unit tests exercise the ADR-014 paired invariant
///         (`maxEvidenceAgeUs < unbondingPeriod * 1e6`) against a *mock* judge
///         (`MockSlashJudgeEvidence`). This integration test deploys the REAL
///         `CapacityBond` and the REAL `SlashJudge` pointed at it, wires them,
///         and asserts the boundary from the CapacityBond side so both real
///         contracts are proven to agree on the strict-inequality boundary.
/// @dev    Values are chosen against the live testnet bounds (which deviate
///         from ADR 014/009's `[7d,60d]` spec — see CapacityBond's
///         `UNBONDING_PERIOD_FLOOR`/`_CEILING` note): CapacityBond
///         `unbondingPeriod ∈ [3 days, 30 days]`; SlashJudge
///         `maxEvidenceAgeUs ∈ [1 day, 30 days]` (microseconds).
contract CapacityBondSlashJudgeInvariantTest is Test {
    Token internal token;
    MockEd25519Verifier internal ed25519;
    MockBlacklistView internal blacklist;
    CapacityBond internal bond;
    SlashJudge internal judge;

    address internal admin = address(0xA11CE);

    uint256 internal constant MIN_BOND = 50_000e18;
    uint256 internal constant CHALLENGE_BOND = 100e18;
    // 20 days in microseconds: valid (< 30d unbonding ceiling, within [1d,30d]).
    uint256 internal constant MAX_EVIDENCE_AGE_US = 20 days * 1_000_000;

    function setUp() public {
        token = new Token(admin);
        ed25519 = new MockEd25519Verifier();
        blacklist = new MockBlacklistView();

        // Construct the real CapacityBond at the unbonding ceiling (30 days).
        bond = new CapacityBond({
            token_: token,
            ed25519Verifier_: ed25519,
            admin: admin,
            minBond_: MIN_BOND,
            unbondingPeriod_: 30 days,
            multiaddrUpdateCooldown_: 0,
            maxMultiaddrSize_: 1024,
            regionStabilityWindow_: 7 days,
            currentTermsHash_: keccak256("decdn operator terms v1")
        });

        // Real SlashJudge pointed at the real bond. 20d < 30d so its constructor
        // invariant (`maxEvidenceAgeUs < capacityBond.unbondingPeriod() * 1e6`)
        // passes.
        judge = new SlashJudge(
            ICapacityBondSlasher(address(bond)),
            IERC20(address(token)),
            blacklist,
            CHALLENGE_BOND,
            MAX_EVIDENCE_AGE_US,
            admin
        );

        // Wire the judge from the governance admin. 30d*1e6 > 20d*1e6 so the
        // setSlashJudge mirror check passes.
        vm.prank(admin);
        bond.setSlashJudge(ISlashJudgeEvidenceView(address(judge)));
    }

    /// @notice Equal case: `20d*1e6 == maxEvidenceAgeUs`. The strict invariant
    ///         (`maxEvidenceAgeUs < unbondingPeriod*1e6`) means equality MUST
    ///         revert. 20 days ∈ [3d,30d] so the bound check passes first and
    ///         the cross-parameter mirror check is the one that fires.
    function test_realSlashJudge_unbondingEqualToEvidenceAge_reverts() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                CapacityBond.UnbondingBelowEvidenceAge.selector,
                uint256(20 days) * 1_000_000,
                uint256(20 days) * 1_000_000
            )
        );
        bond.setUnbondingPeriod(20 days);
    }

    /// @notice One day above the evidence-age ceiling: `21d*1e6 > 20d*1e6`, so the
    ///         strict invariant holds and the update succeeds.
    function test_realSlashJudge_unbondingAboveEvidenceAge_succeeds() public {
        vm.prank(admin);
        bond.setUnbondingPeriod(21 days);
        assertEq(bond.unbondingPeriod(), 21 days);
    }

    function test_realSlashJudge_wiredInSetUp() public view {
        assertEq(address(bond.slashJudge()), address(judge));
        assertEq(judge.maxEvidenceAgeUs(), MAX_EVIDENCE_AGE_US);
    }
}
