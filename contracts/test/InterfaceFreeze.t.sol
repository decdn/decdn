// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { PaymentChannel } from "../src/PaymentChannel.sol";
import { SlashJudge } from "../src/SlashJudge.sol";
import { OriginAssignment } from "../src/OriginAssignment.sol";

/// @title InterfaceFreezeTest — public-surface stability snapshot (issue #452
///        acceptance: "interface ABIs frozen for audit"). Scope is the three
///        contracts ADDED in #452 — PaymentChannel, SlashJudge, OriginAssignment;
///        the pre-existing surface (CapacityBond, FeeRouter, SlashAppeal,
///        ContentBlacklist, DecdnGovernor) is not pinned here. Each assertion pins
///        a function's compiler-derived selector to its canonical signature string;
///        any signature drift on these three flips the selector and fails the
///        test, forcing a deliberate ABI change + audit re-review.
contract InterfaceFreezeTest is Test {
    function test_paymentChannel_abiFrozen() public pure {
        assertEq(
            PaymentChannel.openChannel.selector,
            bytes4(keccak256("openChannel(address,uint256,address)")),
            "openChannel"
        );
        assertEq(PaymentChannel.topUp.selector, bytes4(keccak256("topUp(bytes32,uint256)")), "topUp");
        assertEq(
            PaymentChannel.withdraw.selector,
            bytes4(keccak256("withdraw(bytes32,uint256,uint256,uint256,bytes)")),
            "withdraw"
        );
        assertEq(
            PaymentChannel.closeChannel.selector,
            bytes4(keccak256("closeChannel(bytes32,uint256,uint256,uint256,bytes)")),
            "closeChannel"
        );
        assertEq(
            PaymentChannel.closeChannelWithoutVoucher.selector,
            bytes4(keccak256("closeChannelWithoutVoucher(bytes32)")),
            "closeChannelWithoutVoucher"
        );
        assertEq(
            PaymentChannel.disputeChannel.selector,
            bytes4(keccak256("disputeChannel(bytes32,uint256,uint256,uint256,bytes)")),
            "disputeChannel"
        );
        assertEq(PaymentChannel.settleChannel.selector, bytes4(keccak256("settleChannel(bytes32)")), "settleChannel");
        assertEq(PaymentChannel.reclaimExpired.selector, bytes4(keccak256("reclaimExpired(bytes32)")), "reclaimExpired");
        assertEq(PaymentChannel.getChannel.selector, bytes4(keccak256("getChannel(bytes32)")), "getChannel");
        assertEq(PaymentChannel.getRateBounds.selector, bytes4(keccak256("getRateBounds()")), "getRateBounds");
        assertEq(PaymentChannel.setFeeRouter.selector, bytes4(keccak256("setFeeRouter(address)")), "setFeeRouter");
    }

    function test_slashJudge_abiFrozen() public pure {
        // The trailing `bytes32 salt` on the three reveals + `commitChallenge` are
        // the commit–reveal front-running fix (#854); a deliberate ABI change.
        assertEq(SlashJudge.commitChallenge.selector, bytes4(keccak256("commitChallenge(bytes32)")), "commitChallenge");
        assertEq(
            SlashJudge.submitPhantomChallenge.selector,
            bytes4(keccak256("submitPhantomChallenge(address,bytes32,bytes,bytes,bytes,bytes,bytes32)")),
            "submitPhantomChallenge"
        );
        assertEq(
            SlashJudge.submitRateChallenge.selector,
            bytes4(keccak256("submitRateChallenge(address,bytes32,bytes,bytes,bytes,bytes,bytes32)")),
            "submitRateChallenge"
        );
        assertEq(
            SlashJudge.submitBlacklistChallenge.selector,
            bytes4(keccak256("submitBlacklistChallenge(address,bytes32,bytes32,bytes,bytes,bool,bytes32)")),
            "submitBlacklistChallenge"
        );
        assertEq(
            SlashJudge.setMaxEvidenceAge.selector, bytes4(keccak256("setMaxEvidenceAge(uint256)")), "setMaxEvidenceAge"
        );
        assertEq(
            SlashJudge.setChallengeBond.selector, bytes4(keccak256("setChallengeBond(uint256)")), "setChallengeBond"
        );
    }

    function test_originAssignment_abiFrozen() public pure {
        assertEq(
            OriginAssignment.proposeAssignment.selector,
            bytes4(keccak256("proposeAssignment(uint256,address[])")),
            "proposeAssignment"
        );
        assertEq(
            OriginAssignment.activateAssignment.selector,
            bytes4(keccak256("activateAssignment(uint256)")),
            "activateAssignment"
        );
        assertEq(
            OriginAssignment.revokeAssignment.selector,
            bytes4(keccak256("revokeAssignment(uint256,address)")),
            "revokeAssignment"
        );
        assertEq(
            OriginAssignment.pruneBlacklistedAssignment.selector,
            bytes4(keccak256("pruneBlacklistedAssignment(uint256,address)")),
            "pruneBlacklistedAssignment"
        );
        assertEq(
            OriginAssignment.setContentBlacklist.selector,
            bytes4(keccak256("setContentBlacklist(address)")),
            "setContentBlacklist"
        );
        assertEq(
            OriginAssignment.isAuthorizedOrigin.selector,
            bytes4(keccak256("isAuthorizedOrigin(uint256,address)")),
            "isAuthorizedOrigin"
        );
        assertEq(OriginAssignment.getOrigins.selector, bytes4(keccak256("getOrigins(uint256)")), "getOrigins");
    }
}
