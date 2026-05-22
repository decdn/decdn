// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Governor } from "@openzeppelin/contracts/governance/Governor.sol";
import { GovernorCountingSimple } from "@openzeppelin/contracts/governance/extensions/GovernorCountingSimple.sol";
import { GovernorTimelockControl } from "@openzeppelin/contracts/governance/extensions/GovernorTimelockControl.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";
import { Time } from "@openzeppelin/contracts/utils/types/Time.sol";

import { IVotingEscrow } from "./interfaces/IVotingEscrow.sol";

/// @title DecdnGovernor
/// @notice ve-weighted on-chain governor for the deCDN protocol. Proposals
///         execute through a `TimelockController` (48h delay), so the timelock
///         — not the governor — holds `DEFAULT_ADMIN_ROLE` / `GOVERNANCE_ROLE`
///         on every governed contract and custodies the treasury bucket
///         (ADR 016 § Deployment Order).
/// @dev    Vote source and counting:
///         - Voting weight is `VotingEscrow.balanceOfAt(account, snapshot)` —
///           ve-balance, NOT raw TOKEN holdings (ADR 026 § Voting weight =
///           ve-balance). TOKEN deliberately omits `ERC20Votes`.
///         - Quorum is 4% and the proposal threshold is 0.1% of total
///           ve-supply at the proposal snapshot, read via
///           `VotingEscrow.totalSupplyAt` / `totalSupply` (ADR 009).
///         - The clock is timestamp-based (ERC-6372 `mode=timestamp`) to align
///           with `VotingEscrow`'s timestamp-keyed checkpoints.
///
///         Why a custom vote source instead of OZ `GovernorVotes` /
///         `GovernorVotesQuorumFraction`: those modules require an
///         `IVotes`/IERC-5805 token, but `VotingEscrow` exposes
///         `balanceOfAt` / `totalSupplyAt` (decaying ve-weight) rather than the
///         IVotes surface, so the vote source and quorum are supplied as thin
///         overrides reading `VotingEscrow` directly.
///
///         Delegation is DEFERRED. ADR 009 § Delegation envisions a Governor
///         Bravo delegation pattern over ve-balance; for now each voter votes
///         their own ve-balance (no delegation indirection), matching "voting
///         weight is sourced from `VotingEscrow.balanceOfAt`". Delegating
///         ve-weight is materially harder than ERC20Votes-style delegation
///         because ve-weight decays continuously — a delegate's weight is the
///         time-varying sum of its delegators' decaying balances, which needs
///         per-delegate bias/slope checkpointing in `VotingEscrow` rather than
///         a static vote-unit checkpoint. That work lands separately and does
///         not change this contract's interface.
///
///         Governance-process parameters (voting delay/period, quorum and
///         proposal-threshold fractions) are fixed constants here. They are not
///         part of ADR 009's economic-parameter set (fee shares, stake bounds,
///         timelocks), so they carry no governable setters; if governance
///         tuning is later desired, bounded setters are added then.
contract DecdnGovernor is Governor, GovernorCountingSimple, GovernorTimelockControl {
    /// @notice ve vote source (ADR 026 § Voting weight = ve-balance).
    IVotingEscrow public immutable votingEscrow;

    /// @dev 1-day delay between proposal creation and the vote snapshot, giving
    ///      the electorate notice before weight is measured. Total governance
    ///      latency is ~10 days (1d delay + 7d vote + 48h timelock; ADR 009).
    uint256 private constant VOTING_DELAY = 1 days;
    /// @dev 7-day voting window (ADR 009 / ADR 026).
    uint256 private constant VOTING_PERIOD = 7 days;
    /// @dev Quorum = 4% of total ve-supply at the proposal snapshot (ADR 009).
    uint256 private constant QUORUM_NUMERATOR = 4;
    uint256 private constant QUORUM_DENOMINATOR = 100;
    /// @dev Proposal threshold = 0.1% of total ve-supply (ADR 009).
    uint256 private constant PROPOSAL_THRESHOLD_NUMERATOR = 1;
    uint256 private constant PROPOSAL_THRESHOLD_DENOMINATOR = 1000;

    /// @notice Thrown when the vote source is the zero address.
    error ZeroVotingEscrow();

    /// @param votingEscrow_ ve vote source (must be non-zero).
    /// @param timelock_     Execution timelock; holds privileged roles on
    ///                      governed contracts (ADR 016).
    constructor(IVotingEscrow votingEscrow_, TimelockController timelock_)
        Governor("DecdnGovernor")
        GovernorTimelockControl(timelock_)
    {
        if (address(votingEscrow_) == address(0)) revert ZeroVotingEscrow();
        votingEscrow = votingEscrow_;
    }

    // -----------------------------------------------------------------
    // Clock — ERC-6372 timestamp mode (aligned with VotingEscrow)
    // -----------------------------------------------------------------

    function clock() public view override returns (uint48) {
        return Time.timestamp();
    }

    // solhint-disable-next-line func-name-mixedcase
    function CLOCK_MODE() public pure override returns (string memory) {
        return "mode=timestamp";
    }

    // -----------------------------------------------------------------
    // Fixed voting schedule (ADR 009)
    // -----------------------------------------------------------------

    function votingDelay() public pure override returns (uint256) {
        return VOTING_DELAY;
    }

    function votingPeriod() public pure override returns (uint256) {
        return VOTING_PERIOD;
    }

    // -----------------------------------------------------------------
    // Vote source + quorum/threshold from ve-supply (ADR 009 / ADR 026)
    // -----------------------------------------------------------------

    /// @notice Quorum = 4% of total ve-supply at `timepoint`.
    function quorum(uint256 timepoint) public view override returns (uint256) {
        return (votingEscrow.totalSupplyAt(timepoint) * QUORUM_NUMERATOR) / QUORUM_DENOMINATOR;
    }

    /// @notice Proposal threshold = 0.1% of current total ve-supply. Evaluated
    ///         at proposal time, which is the snapshot the proposer's weight is
    ///         checked against (`clock() - 1`).
    function proposalThreshold() public view override returns (uint256) {
        return (votingEscrow.totalSupply() * PROPOSAL_THRESHOLD_NUMERATOR) / PROPOSAL_THRESHOLD_DENOMINATOR;
    }

    /// @dev Voting weight = ve-balance at the proposal snapshot. `params` is
    ///      unused (no fractional / custom voting).
    function _getVotes(address account, uint256 timepoint, bytes memory) internal view override returns (uint256) {
        return votingEscrow.balanceOfAt(account, timepoint);
    }

    // -----------------------------------------------------------------
    // Timelock plumbing — resolve Governor / GovernorTimelockControl
    // -----------------------------------------------------------------

    function state(uint256 proposalId) public view override(Governor, GovernorTimelockControl) returns (ProposalState) {
        return super.state(proposalId);
    }

    function proposalNeedsQueuing(uint256 proposalId)
        public
        view
        override(Governor, GovernorTimelockControl)
        returns (bool)
    {
        return super.proposalNeedsQueuing(proposalId);
    }

    function _queueOperations(
        uint256 proposalId,
        address[] memory targets,
        uint256[] memory values,
        bytes[] memory calldatas,
        bytes32 descriptionHash
    ) internal override(Governor, GovernorTimelockControl) returns (uint48) {
        return super._queueOperations(proposalId, targets, values, calldatas, descriptionHash);
    }

    function _executeOperations(
        uint256 proposalId,
        address[] memory targets,
        uint256[] memory values,
        bytes[] memory calldatas,
        bytes32 descriptionHash
    ) internal override(Governor, GovernorTimelockControl) {
        super._executeOperations(proposalId, targets, values, calldatas, descriptionHash);
    }

    function _cancel(
        address[] memory targets,
        uint256[] memory values,
        bytes[] memory calldatas,
        bytes32 descriptionHash
    ) internal override(Governor, GovernorTimelockControl) returns (uint256) {
        return super._cancel(targets, values, calldatas, descriptionHash);
    }

    function _executor() internal view override(Governor, GovernorTimelockControl) returns (address) {
        return super._executor();
    }
}
