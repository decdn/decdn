// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { PaymentChannel } from "../src/PaymentChannel.sol";
import { SlashJudge } from "../src/SlashJudge.sol";
import { OriginAssignment } from "../src/OriginAssignment.sol";
import { ISlashJudge } from "../src/interfaces/ISlashJudge.sol";

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
        // The trailing `bytes32 salt` on the two reveals + `commitChallenge` are
        // the commit–reveal front-running fix (#854); a deliberate ABI change.
        assertEq(SlashJudge.commitChallenge.selector, bytes4(keccak256("commitChallenge(bytes32)")), "commitChallenge");
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

    /// @notice `OffenseType` ordinals are durable: they land in
    ///         `SlashEscrowLib.SlashRecord.offenseType` storage, in both
    ///         (non-indexed) `Slashed` events, and inside the `evidenceHash`
    ///         preimage that keys `usedEvidenceHash` and `commitments`. Enum
    ///         members ABI-encode as `uint8`, so a reorder leaves every selector
    ///         above unchanged — `test_slashJudge_abiFrozen` is structurally
    ///         blind to it. This is the gate that is not.
    function test_slashJudge_offenseOrdinalsFrozen() public pure {
        assertEq(uint8(ISlashJudge.OffenseType.RateManipulation), 0, "RateManipulation ordinal");
        assertEq(uint8(ISlashJudge.OffenseType.Blacklist), 1, "Blacklist ordinal");
    }

    function test_originAssignment_abiFrozen() public pure {
        assertEq(OriginAssignment.addOrigin.selector, bytes4(keccak256("addOrigin(uint256,address)")), "addOrigin");
        assertEq(
            OriginAssignment.removeOrigin.selector, bytes4(keccak256("removeOrigin(uint256,address)")), "removeOrigin"
        );
        assertEq(
            OriginAssignment.pruneBlacklistedOrigin.selector,
            bytes4(keccak256("pruneBlacklistedOrigin(uint256,address)")),
            "pruneBlacklistedOrigin"
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
        // Reads the off-chain side binds by hand. `assignedNamespaces` is the
        // only way the node discovers which namespaces exist at bootstrap.
        // (`isVettedPublisher` is a passthrough view, and `vettingPolicy` a public
        // variable — both bound by hand off-chain and pinned against the live ABI
        // in `OriginAssignmentTest.test_publicGetters_matchTheFrozenAbi`.)
        assertEq(
            OriginAssignment.assignedNamespaceCount.selector,
            bytes4(keccak256("assignedNamespaceCount()")),
            "assignedNamespaceCount"
        );
        assertEq(
            OriginAssignment.assignedNamespaces.selector,
            bytes4(keccak256("assignedNamespaces(uint256,uint256)")),
            "assignedNamespaces"
        );
        // Governance-proposal targets: a drifted selector silently bricks a vote.
        assertEq(
            OriginAssignment.setVettingPolicy.selector,
            bytes4(keccak256("setVettingPolicy(address)")),
            "setVettingPolicy"
        );
        assertEq(
            OriginAssignment.setMaxOriginsPerNamespace.selector,
            bytes4(keccak256("setMaxOriginsPerNamespace(uint256)")),
            "setMaxOriginsPerNamespace"
        );
    }
}
