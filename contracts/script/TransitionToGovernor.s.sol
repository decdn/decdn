// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Script, console2 } from "forge-std/Script.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

// This is an operator runbook script whose entire purpose is to print the
// governance calldata to schedule through the Timelock — console output is
// intentional here.
// solhint-disable no-console

/// @title TransitionToGovernor — end the ADR 009 bootstrap-multisig phase.
/// @notice ADR 009 § Bootstrap-multisig phase: at launch the operator set is too
///         thin for served-bytes-weighted DAO voting to be safe against a cheap
///         operator-fleet takeover, so `DeployProtocol` (with `BOOTSTRAP_MULTISIG`
///         set) seats the bootstrap multisig as the `TimelockController`'s sole
///         `PROPOSER_ROLE`/`CANCELLER_ROLE` holder and grants `DecdnGovernor`
///         neither. Every parameter change still runs through the Timelock's 48-hour
///         delay; what the phase withholds is the ability to *schedule* one.
///
///         The phase ends when the multisig judges the operator set broad enough and
///         schedules the batch this script prints — a manual decision with no
///         threshold automation (issue #1175). The batch grants the Governor both
///         roles and revokes the multisig's, in that order.
///
/// @dev    This script BROADCASTS NOTHING. It prints `TimelockController.scheduleBatch`
///         and `executeBatch` calldata for the multisig to submit, exactly as
///         `ActivateBuyback` does for buyback activation. Every leg targets the
///         Timelock itself, which self-administers `PROPOSER_ROLE`/`CANCELLER_ROLE`
///         (their role-admin is `DEFAULT_ADMIN_ROLE`, held by the Timelock), so the
///         transition is an ordinary Timelock proposal — no bespoke contract, and no
///         address holds a standing power to perform it out of band.
///
///         **Irreversible in practice.** Executing the batch strips the multisig's
///         `PROPOSER_ROLE`, so it can never schedule anything again — including a
///         proposal to reinstate itself. Only the Governor can propose afterwards,
///         which is what ADR 009 § Transition means by "the bootstrap-multisig
///         cannot be reinstated". Order matters: the Governor is granted before the
///         multisig is revoked, so the batch cannot strand the Timelock with no
///         proposer at all.
///
///         Env vars:
///           - `GOVERNANCE_TIMELOCK` — the deployed TimelockController
///           - `DECDN_GOVERNOR`      — the deployed DecdnGovernor
///           - `BOOTSTRAP_MULTISIG`  — the multisig being retired
///         Both addresses are in `deployments/<chainId>.json`
///         (`contracts.TimelockController`, `contracts.DecdnGovernor`, and
///         `externalDeps.bootstrapMultisig`).
contract TransitionToGovernor is Script {
    /// @notice The Timelock's scheduling roles do not look like a live bootstrap
    ///         phase, so this batch is not the right instrument. Either the deploy
    ///         never entered the phase, the transition already ran, or the two roles
    ///         are seated inconsistently — all cases where the operator should look
    ///         at the chain rather than sign what this script would have printed.
    error NotInBootstrapPhase(address multisig, address governor);

    function run() external view {
        TimelockController timelock = TimelockController(payable(vm.envAddress("GOVERNANCE_TIMELOCK")));
        address governor = vm.envAddress("DECDN_GOVERNOR");
        address multisig = vm.envAddress("BOOTSTRAP_MULTISIG");

        bytes32 proposerRole = timelock.PROPOSER_ROLE();
        bytes32 cancellerRole = timelock.CANCELLER_ROLE();

        // Fail loudly rather than print a plausible-looking batch against a chain
        // that is not mid-bootstrap. The predicate is the phase definition in full —
        // the multisig holds BOTH scheduling roles and the Governor holds neither —
        // not just the proposer half. The batch itself would survive a partial state
        // (OZ `grantRole`/`revokeRole` no-op when the state already matches, so it
        // converges either way); what a loose predicate costs is the signal. Printing
        // against a half-transitioned or never-bootstrapped chain tells the operator
        // that the chain looked as expected when it did not, which is the wrong thing
        // to learn from a script whose output is signed once and is irreversible.
        bool multisigSeated = timelock.hasRole(proposerRole, multisig) && timelock.hasRole(cancellerRole, multisig);
        bool governorUnseated = !timelock.hasRole(proposerRole, governor) && !timelock.hasRole(cancellerRole, governor);
        if (!multisigSeated || !governorUnseated) revert NotInBootstrapPhase(multisig, governor);

        address[] memory targets = new address[](4);
        uint256[] memory values = new uint256[](4);
        bytes[] memory payloads = new bytes[](4);
        for (uint256 i = 0; i < 4; i++) {
            targets[i] = address(timelock);
        }
        // Grant before revoke: the reverse order would, between legs, leave the
        // Timelock with no proposer at all.
        payloads[0] = abi.encodeCall(IAccessControl.grantRole, (proposerRole, governor));
        payloads[1] = abi.encodeCall(IAccessControl.grantRole, (cancellerRole, governor));
        payloads[2] = abi.encodeCall(IAccessControl.revokeRole, (proposerRole, multisig));
        payloads[3] = abi.encodeCall(IAccessControl.revokeRole, (cancellerRole, multisig));

        uint256 delay = timelock.getMinDelay();
        bytes memory scheduleCalldata = abi.encodeCall(
            TimelockController.scheduleBatch, (targets, values, payloads, bytes32(0), bytes32(0), delay)
        );
        bytes memory executeCalldata =
            abi.encodeCall(TimelockController.executeBatch, (targets, values, payloads, bytes32(0), bytes32(0)));

        console2.log("== ADR 009 bootstrap -> DAO governance transition ==");
        console2.log("Timelock:          ", address(timelock));
        console2.log("Governor (gains):  ", governor);
        console2.log("Multisig (loses):  ", multisig);
        console2.log("Timelock delay (s):", delay);
        console2.log("");
        console2.log("1) Submit from the bootstrap multisig, target the Timelock:");
        console2.log("   TimelockController.scheduleBatch(...)");
        console2.logBytes(scheduleCalldata);
        console2.log("");
        console2.log("2) After the delay elapses, anyone may execute (open executor):");
        console2.log("   TimelockController.executeBatch(...)");
        console2.logBytes(executeCalldata);
        console2.log("");
        console2.log("This is one-way: leg 3 strips the multisig's PROPOSER_ROLE, so it");
        console2.log("cannot schedule a proposal to reinstate itself afterwards.");
    }
}
