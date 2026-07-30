// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Script, console2 } from "forge-std/Script.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

/// @dev The one accessor this script needs off `DecdnGovernor` — declared locally
///      rather than importing the Governor so the runbook script does not carry the
///      whole governance stack's bytecode. `DecdnGovernor` inherits it from OZ's
///      `GovernorTimelockControl`.
interface IGovernorTimelock {
    function timelock() external view returns (address);
}

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
///         **Irreversible by the multisig.** Executing the batch strips the
///         multisig's `PROPOSER_ROLE`, so it can never schedule anything again —
///         including a proposal to reinstate itself. That is the precise claim; the
///         DAO that inherits the role could in principle vote to re-grant it, which
///         is what ADR 009 § Transition means by "the bootstrap-multisig cannot be
///         reinstated" — no *standing* party can undo it.
///
///         Env vars:
///           - `GOVERNANCE_TIMELOCK` — the deployed TimelockController
///           - `DECDN_GOVERNOR`      — the deployed DecdnGovernor
///           - `BOOTSTRAP_MULTISIG`  — the multisig being retired
///         All three are in `deployments/<chainId>.json`
///         (`contracts.TimelockController`, `contracts.DecdnGovernor`, and
///         `externalDeps.bootstrapMultisig`).
contract TransitionToGovernor is Script {
    /// @notice The Timelock's scheduling roles do not look like a live bootstrap
    ///         phase, so this batch is not the right instrument. Either the deploy
    ///         never entered the phase, the transition already ran, or the two roles
    ///         are seated inconsistently — all cases where the operator should look
    ///         at the chain rather than sign what this script would have printed.
    error NotInBootstrapPhase(address multisig, address governor);
    /// @notice `DECDN_GOVERNOR` is not a contract, or is a contract that executes
    ///         through a different Timelock. `boundTimelock` is what it actually
    ///         named (`address(0)` when the address holds no code at all). Guards
    ///         the one input whose correctness the role checks cannot establish.
    error GovernorNotBoundToTimelock(address governor, address boundTimelock);

    function run() external view {
        TimelockController timelock = TimelockController(payable(vm.envAddress("GOVERNANCE_TIMELOCK")));
        address governor = vm.envAddress("DECDN_GOVERNOR");
        address multisig = vm.envAddress("BOOTSTRAP_MULTISIG");

        (address[] memory targets, uint256[] memory values, bytes[] memory payloads) =
            transitionBatchChecked(timelock, governor, multisig);

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

    /// @notice The batch, gated on the chain actually being mid-bootstrap-phase and
    ///         on `governor` being the Governor this Timelock executes for. This is
    ///         what `run()` calls; it is `public` so the guards are testable without
    ///         env vars, which is the only reason they had no coverage before.
    function transitionBatchChecked(TimelockController timelock, address governor, address multisig)
        public
        view
        returns (address[] memory targets, uint256[] memory values, bytes[] memory payloads)
    {
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

        // `governorUnseated` above is a NEGATIVE check, and "does not hold these two
        // roles" is the default state of every address on Ethereum — it passes for an
        // EOA, for `address(0)`, for a Governor from a different chain's manifest, for
        // a one-nibble typo. The multisig half cannot fail that way because it
        // requires *holding* both roles, which is what makes the asymmetry easy to
        // miss. So identify the Governor positively: it must be a contract that names
        // THIS Timelock as its executor. Without this, a mistyped `DECDN_GOVERNOR`
        // yields a clean-looking batch that irreversibly grants scheduling power to a
        // dead address while revoking the multisig's — governance bricked, no recovery.
        if (governor.code.length == 0) revert GovernorNotBoundToTimelock(governor, address(0));
        address boundTimelock = IGovernorTimelock(governor).timelock();
        if (boundTimelock != address(timelock)) revert GovernorNotBoundToTimelock(governor, boundTimelock);

        return transitionBatch(timelock, governor, multisig);
    }

    /// @notice The four legs of the transition, in execution order: grant the
    ///         Governor `PROPOSER_ROLE` + `CANCELLER_ROLE`, then revoke both from the
    ///         multisig. Every leg targets the Timelock, which self-administers those
    ///         roles, so the batch needs no privileged caller beyond the scheduler.
    ///
    /// @dev    `public` rather than inlined into `run()` so the test suite can execute
    ///         the *actual* batch this script prints. A test that rebuilt the legs by
    ///         hand would be a second source of truth: reorder or retarget a leg here
    ///         and the hand-written twin still passes, leaving the one artifact a
    ///         human signs unverified. That is precisely the duplication
    ///         `BuybackVenueLib` removed elsewhere in this change.
    function transitionBatch(TimelockController timelock, address governor, address multisig)
        public
        view
        returns (address[] memory targets, uint256[] memory values, bytes[] memory payloads)
    {
        bytes32 proposerRole = timelock.PROPOSER_ROLE();
        bytes32 cancellerRole = timelock.CANCELLER_ROLE();

        targets = new address[](4);
        values = new uint256[](4);
        payloads = new bytes[](4);
        for (uint256 i = 0; i < 4; i++) {
            targets[i] = address(timelock);
        }
        payloads[0] = abi.encodeCall(IAccessControl.grantRole, (proposerRole, governor));
        payloads[1] = abi.encodeCall(IAccessControl.grantRole, (cancellerRole, governor));
        payloads[2] = abi.encodeCall(IAccessControl.revokeRole, (proposerRole, multisig));
        payloads[3] = abi.encodeCall(IAccessControl.revokeRole, (cancellerRole, multisig));
    }
}
